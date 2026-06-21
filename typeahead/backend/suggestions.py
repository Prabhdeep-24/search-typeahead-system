"""Core suggestion logic: compute, cache, flush.
Write-through invalidation - flush_query immediately recomputes affected prefixes,
so the cache is always accurate after every flush.

V2 (recency-aware ranking): a separate, parallel ranking signal on top of the
same data. Never cached in cache_servers - see compute_top10_recency below for
why - so none of the existing build/flush cache machinery above needed to
change to support it; only the recency_memory/last_tick_memory bookkeeping
is new."""
import bisect
import heapq
import threading
import time

from cache import ring
from database import (
    get_all_queries_with_recency,
    update_counts_and_recency_batch,
    update_counts_batch,
    update_recency_only_batch,
)
from storage import (
    cache_servers,
    cache_topology_lock,
    frequency_memory,
    last_tick_memory,
    recency_memory,
    sorted_queries,
    write_buffer,
)
from wal import clear_wal, replay_wal

MIN_PREFIX_LENGTH = 3
TOP_N = 10
BUFFER_SIZE_THRESHOLD = 100
FLUSH_INTERVAL_SECONDS = 5

# V2 recency tuning. Tick = 30s wall-clock bucket, derived from time.time()
# directly rather than a manual counter, so it's always correct even across
# restarts. DECAY_FACTOR is applied once per elapsed tick since a query was
# last touched - a search's contribution to recency_score halves roughly
# every ~7 ticks (~3.5 minutes), fast enough to watch decay live in a demo.
TICK_SECONDS = 30
DECAY_FACTOR = 0.9
COUNT_WEIGHT = 0.01  # all-time count's weight as a tie-breaker/floor in the hybrid score

_buffer_lock = threading.Lock()


def get_current_tick():
    return int(time.time() // TICK_SECONDS)


def hybrid_score(query):
    """recency_score + a small fraction of all-time count. Recency dominates
    (so genuinely fresh activity wins), but count acts as a floor/tie-breaker
    so historically popular-but-quiet queries don't tie at exactly 0 with
    everything else that's never been searched live."""
    return recency_memory.get(query, 0) + COUNT_WEIGHT * frequency_memory.get(query, 0)

# Character used to build the upper bound of a prefix's range in sorted_queries.
# Any real query string sorts below "<prefix> + this char" since it's higher
# than any character we expect in normal text.
_RANGE_UPPER_BOUND_CHAR = "￿"


def get_prefixes(query):
    """Get all prefixes of length >= MIN_PREFIX_LENGTH"""
    query = query.lower().strip()
    return [query[:i] for i in range(MIN_PREFIX_LENGTH, len(query) + 1)]


def _prefix_range(prefix):
    """Binary-search the [start, end) slice of sorted_queries whose entries
    start with `prefix`. O(log N) instead of scanning all N queries."""
    lo = bisect.bisect_left(sorted_queries, prefix)
    hi = bisect.bisect_left(sorted_queries, prefix + _RANGE_UPPER_BOUND_CHAR)
    return lo, hi


def compute_top10(prefix):
    """V1: rank by count from frequency_memory.
    V2: update to use recency_score [ TO BE FILLED ]

    Implementation note: finds the matching range via binary search on
    sorted_queries (O(log N)), then picks the top 10 by count from just that
    range using a heap (O(range_size log 10)) instead of scanning the entire
    dataset on every call - required once the dataset is in the millions."""
    lo, hi = _prefix_range(prefix)
    if lo == hi:
        return []
    candidates = sorted_queries[lo:hi]
    top = heapq.nlargest(TOP_N, candidates, key=lambda q: frequency_memory.get(q, 0))
    return top


def compute_top10_recency(prefix):
    """V2: rank by hybrid_score (recency_score + a fraction of count).

    Computed LIVE on every call, never cached - unlike count, a recency score
    can change purely because time passed, with no new search happening at
    all. Caching it would mean either re-deriving on every read anyway (no
    benefit) or serving silently stale rankings with no write event to ever
    trigger a refresh. The underlying lookup (binary-search range here) is
    already cheap, so there's nothing to gain from caching it.

    Returns recency-ranked winners first, then "stable fill" (plain count
    order) for any remaining slots - so a prefix nobody has searched live
    yet still returns a full, sensibly-ordered list instead of empty/ties."""
    lo, hi = _prefix_range(prefix)
    if lo == hi:
        return []
    candidates = sorted_queries[lo:hi]

    recency_ranked = heapq.nlargest(TOP_N, candidates, key=hybrid_score)
    if len(recency_ranked) >= TOP_N:
        return recency_ranked

    seen = set(recency_ranked)
    remaining_needed = TOP_N - len(recency_ranked)
    stable_pool = [q for q in candidates if q not in seen]
    stable_fill = heapq.nlargest(
        remaining_needed, stable_pool, key=lambda q: frequency_memory.get(q, 0)
    )
    return recency_ranked + stable_fill


def _register_new_query(query):
    """Insert a query into sorted_queries if it hasn't been seen before.
    Rare in steady state (most searches are for existing queries), so the
    occasional O(N) insort cost is acceptable."""
    idx = bisect.bisect_left(sorted_queries, query)
    if idx == len(sorted_queries) or sorted_queries[idx] != query:
        sorted_queries.insert(idx, query)


def update_cache(prefix):
    """Recompute top 10 for prefix from scratch and store in the cache node it
    hashes to. Used only as a fallback for a prefix the bottom-up build never
    saw (e.g. one that only exists because of a brand-new query).

    Locked: redistribute_cache() replaces cache_servers[server] with a brand
    new dict object when a server is added/removed - a write landing on the
    old dict object right as that swap happens would be silently lost."""
    server = ring.get_server(prefix)
    top10 = compute_top10(prefix)
    with cache_topology_lock:
        if server and server in cache_servers:
            cache_servers[server][prefix] = top10


def _merge_update_cache(prefix, changed_queries):
    """Recompute a prefix's top 10 by merging its EXISTING cached entry with
    just the queries that changed this flush, instead of recomputing from the
    full dataset. Correct because of the write-through invariant: the cache
    is always already-accurate going into a flush, so any query not in the
    old top 10 and not changed this flush still has the same count it had
    before - it couldn't have newly entered the top 10. So the new true top
    10 is guaranteed to be found within (old top 10) union (changed queries).

    Locked for the same reason as update_cache above - see its docstring."""
    server = ring.get_server(prefix)
    if not server:
        return

    with cache_topology_lock:
        if server not in cache_servers:
            return

        existing = cache_servers[server].get(prefix)
        if existing is None:
            # Genuinely new prefix the startup build never saw - full fallback.
            cache_servers[server][prefix] = compute_top10(prefix)
            return

        candidates = {q: frequency_memory.get(q, 0) for q in existing}
        for q in changed_queries:
            candidates[q] = frequency_memory.get(q, 0)

        top10 = heapq.nlargest(TOP_N, candidates.items(), key=lambda kv: kv[1])
        cache_servers[server][prefix] = [q for q, _ in top10]


def _decay_recency(query, current_tick):
    """Apply DECAY_FACTOR once per elapsed tick since this query's
    recency_score was last touched, lazily - only computed when the query is
    actually flushed, not on a global timer. A query nobody has searched in a
    while simply sits at whatever it decayed to the last time it WAS touched;
    its true current value is always derivable from (old score, ticks since),
    so there's no need to eagerly update everything on every tick."""
    ticks_passed = current_tick - last_tick_memory.get(query, 0)
    old_score = recency_memory.get(query, 0)
    return old_score * (DECAY_FACTOR**ticks_passed) if ticks_passed > 0 else old_score


def flush_buffer():
    """
    Swap out the write buffer, apply all deltas to SQLite in ONE transaction,
    update frequency_memory, then patch the cache for every affected prefix
    via the cheap old-top10-plus-deltas merge above, then clear the WAL since
    these deltas are now durably in SQLite.

    Also decays and refreshes each changed query's recency_score (V2) in the
    same pass, using the same batch of deltas - one mechanism driving both
    the count-based cache and the recency signal.
    """
    with _buffer_lock:
        if not write_buffer:
            return
        deltas = dict(write_buffer)
        write_buffer.clear()

    if not deltas:
        return

    current_tick = get_current_tick()
    db_updates = {}  # query -> (count_delta, new_recency_score, tick)
    prefix_to_changed = {}
    for query, delta in deltas.items():
        is_new = query not in frequency_memory
        frequency_memory[query] = frequency_memory.get(query, 0) + delta
        if is_new:
            _register_new_query(query)

        new_recency = _decay_recency(query, current_tick) + delta
        recency_memory[query] = new_recency
        last_tick_memory[query] = current_tick
        db_updates[query] = (delta, new_recency, current_tick)

        for prefix in get_prefixes(query):
            prefix_to_changed.setdefault(prefix, []).append(query)

    update_counts_and_recency_batch(db_updates)

    for prefix, changed_queries in prefix_to_changed.items():
        _merge_update_cache(prefix, changed_queries)

    clear_wal()
    print(
        f"Flushed {len(deltas)} queries ({sum(deltas.values())} total searches) "
        f"-> 1 DB transaction, {len(prefix_to_changed)} prefixes patched"
    )


def apply_global_decay():
    """Manual /decay sweep: unlike the lazy per-query decay above (which only
    runs when a query is actually searched again), this walks every query
    that has a non-zero recency_score and decays it based on elapsed ticks,
    even ones nobody has searched recently. Lazy decay alone never "catches
    up" a quiet query's stored score until someone searches it again; this
    gives an explicit, on-demand way to see the global recency picture
    reflect the current moment, useful for demoing the decay behavior."""
    current_tick = get_current_tick()
    db_updates = {}  # query -> (new_recency_score, tick)
    for query in list(recency_memory.keys()):
        if recency_memory.get(query, 0) == 0:
            continue
        new_score = _decay_recency(query, current_tick)
        recency_memory[query] = new_score
        last_tick_memory[query] = current_tick
        db_updates[query] = (new_score, current_tick)

    update_recency_only_batch(db_updates)
    return len(db_updates)


def background_flush():
    """Background daemon thread - flushes periodically, or sooner if the
    buffer grows past BUFFER_SIZE_THRESHOLD distinct queries."""
    while True:
        time.sleep(FLUSH_INTERVAL_SECONDS)
        flush_buffer()


def maybe_flush_now():
    """Called after every search submission - flush immediately if the buffer
    is large, instead of waiting for the next timer tick."""
    if len(write_buffer) >= BUFFER_SIZE_THRESHOLD:
        flush_buffer()


def build_cache():
    """
    Called on startup.
    1. Recover any unflushed searches from the WAL (crash recovery).
    2. Load all data from SQLite into frequency_memory.
    3. Build the cache bottom-up: start from complete queries (the longest
       "prefix" of anything is the query itself), then walk one character
       shorter at a time, merging each prefix's top 10 from its own count
       (if it's itself a complete query) plus its children's already-known
       top 10 lists - never rescanning the full dataset at any prefix.
    """
    recovered = replay_wal()
    if recovered:
        print(f"Recovered {len(recovered)} queries from WAL after unclean shutdown")
        update_counts_batch(recovered)
        clear_wal()

    print("Loading data from SQLite into memory...")
    for query, count, recency, last_tick in get_all_queries_with_recency():
        frequency_memory[query] = count
        recency_memory[query] = recency
        last_tick_memory[query] = last_tick

    print("Sorting queries for fast prefix lookup...")
    sorted_queries.extend(sorted(frequency_memory.keys()))

    if not frequency_memory:
        print("Cache ready. 0 prefixes (empty dataset).")
        return

    print("Building cache bottom-up (merging children prefixes upward)...")

    # Bucket queries by their own length so each one is injected as "the
    # complete word at this prefix" exactly once, at its own length - O(N)
    # total across the whole build, not re-scanned at every level.
    length_buckets = {}
    max_len = 0
    for query, count in frequency_memory.items():
        length_buckets.setdefault(len(query), []).append((query, count))
        max_len = max(max_len, len(query))

    next_level = {}  # prefix (length L+1) -> top10 [(query, count), ...] from the level below
    total_prefixes = 0

    for length in range(max_len, MIN_PREFIX_LENGTH - 1, -1):
        # Group next_level's entries by their parent (this level's) prefix -
        # this is the "merge from children" step, bounded by however many
        # distinct next-characters actually occur, not by total matches.
        candidates_by_prefix = {}
        for child_prefix, child_top10 in next_level.items():
            parent = child_prefix[:length]
            candidates_by_prefix.setdefault(parent, []).extend(child_top10)

        # A prefix that is itself a complete query also contributes its own
        # count, in addition to whatever children it has (e.g. "iphone" is
        # both a real query AND has children like "iphone 15").
        for query, count in length_buckets.get(length, []):
            candidates_by_prefix.setdefault(query, []).append((query, count))

        current_level = {}
        for prefix, candidates in candidates_by_prefix.items():
            top10 = heapq.nlargest(TOP_N, candidates, key=lambda t: t[1])
            current_level[prefix] = top10
            server = ring.get_server(prefix)
            if server and server in cache_servers:
                cache_servers[server][prefix] = [q for q, _ in top10]
            total_prefixes += 1

        next_level = current_level

    print(f"Cache ready. {total_prefixes} prefixes cached across {len(ring.servers)} servers.")


def redistribute_cache():
    """Called after adding/removing a server. Adding/removing a server only
    changes ROUTING (which server owns a prefix) - it never changes the
    actual top-10 answer for any prefix - so we just re-bucket the existing,
    already-correct cached entries into their new homes instead of
    recomputing anything.

    Caller MUST already hold cache_topology_lock (main.py's add_server/
    remove_server do) - this function does NOT acquire it itself, since it's
    a plain Lock (not reentrant) and its only callers already hold it."""
    old_entries = {}
    for server_dict in cache_servers.values():
        old_entries.update(server_dict)

    for server in cache_servers:
        cache_servers[server] = {}

    for prefix, top10 in old_entries.items():
        server = ring.get_server(prefix)
        if server and server in cache_servers:
            cache_servers[server][prefix] = top10
