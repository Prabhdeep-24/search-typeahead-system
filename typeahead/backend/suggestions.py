"""Core suggestion logic: compute, cache, flush.
Write-through invalidation - flush_query immediately recomputes affected prefixes,
so the cache is always accurate after every flush."""
import bisect
import heapq
import threading
import time

from cache import ring
from database import get_all_queries, update_counts_batch
from storage import cache_servers, frequency_memory, sorted_queries, write_buffer
from wal import clear_wal, replay_wal

MIN_PREFIX_LENGTH = 3
TOP_N = 10
BUFFER_SIZE_THRESHOLD = 100
FLUSH_INTERVAL_SECONDS = 5

_buffer_lock = threading.Lock()

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
    saw (e.g. one that only exists because of a brand-new query)."""
    server = ring.get_server(prefix)
    if server and server in cache_servers:
        cache_servers[server][prefix] = compute_top10(prefix)


def _merge_update_cache(prefix, changed_queries):
    """Recompute a prefix's top 10 by merging its EXISTING cached entry with
    just the queries that changed this flush, instead of recomputing from the
    full dataset. Correct because of the write-through invariant: the cache
    is always already-accurate going into a flush, so any query not in the
    old top 10 and not changed this flush still has the same count it had
    before - it couldn't have newly entered the top 10. So the new true top
    10 is guaranteed to be found within (old top 10) union (changed queries)."""
    server = ring.get_server(prefix)
    if not server or server not in cache_servers:
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


def flush_buffer():
    """
    Swap out the write buffer, apply all deltas to SQLite in ONE transaction,
    update frequency_memory, then patch the cache for every affected prefix
    via the cheap old-top10-plus-deltas merge above, then clear the WAL since
    these deltas are now durably in SQLite.
    """
    with _buffer_lock:
        if not write_buffer:
            return
        deltas = dict(write_buffer)
        write_buffer.clear()

    if not deltas:
        return

    update_counts_batch(deltas)

    prefix_to_changed = {}
    for query, delta in deltas.items():
        is_new = query not in frequency_memory
        frequency_memory[query] = frequency_memory.get(query, 0) + delta
        if is_new:
            _register_new_query(query)
        for prefix in get_prefixes(query):
            prefix_to_changed.setdefault(prefix, []).append(query)

    for prefix, changed_queries in prefix_to_changed.items():
        _merge_update_cache(prefix, changed_queries)

    clear_wal()
    print(
        f"Flushed {len(deltas)} queries ({sum(deltas.values())} total searches) "
        f"-> 1 DB transaction, {len(prefix_to_changed)} prefixes patched"
    )


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
    frequency_memory.update(get_all_queries())

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
    recomputing anything."""
    old_entries = {}
    for server_dict in cache_servers.values():
        old_entries.update(server_dict)

    for server in cache_servers:
        cache_servers[server] = {}

    for prefix, top10 in old_entries.items():
        server = ring.get_server(prefix)
        if server and server in cache_servers:
            cache_servers[server][prefix] = top10
