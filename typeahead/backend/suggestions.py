"""Core suggestion logic: compute, cache, flush.
Write-through invalidation - flush_query immediately recomputes affected prefixes,
so the cache is always accurate after every flush.

V2 (recency-aware ranking): TWO lists merged at serve time, not one list.
cache_servers is the "stable" list - ranked by all-time count, exactly like
V1, provably safe to maintain incrementally (count never decreases).
recency_cache_servers is the "trending" list - small, often EMPTY, holding
only queries with a currently-meaningful (non-decayed-away) recency_score
for that prefix. main.py merges trending + stable at request time, deduped,
so a query that gets temporarily outranked by something trending and drops
out of the trending list is never lost - it's still sitting in the stable
list the whole time, and naturally resurfaces once the trending entry
decays below it. The trending list needs no scan-fallback at all (unlike
the stable list): "nothing is trending here" is always a valid, correct
answer, never a "missing data" state."""
import heapq
import threading
import time
from operator import itemgetter

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
    dirty_recency_prefixes,
    frequency_memory,
    last_tick_memory,
    pending_scans,
    pending_scans_set,
    recency_cache_servers,
    recency_memory,
    scan_queue_lock,
    write_buffer,
)
from wal import clear_wal, replay_wal

MIN_PREFIX_LENGTH = 3
TOP_N = 10
BUFFER_SIZE_THRESHOLD = 100
FLUSH_INTERVAL_SECONDS = 5

# Only the top this-many queries (by all-time count) get a precomputed cache
# entry at startup - see build_cache. Tuned empirically: covers ~74% of real
# search volume in our 300k-query dataset while cutting build time from
# ~6.8s to ~1.7-2.2s. Everything else is filled lazily on first miss.
TOP_N_PRECOMPUTE = 100000

# How long background_scan_fill sleeps when both scan queues are empty -
# just avoids a tight busy-loop burning CPU for nothing while idle.
SCAN_WORKER_IDLE_SLEEP_SECONDS = 0.01

# How long background_scan_fill sleeps between each scan WHILE draining a
# backlog. Pacing the worker like this trades "how fast the backlog
# drains" for "how much it competes with /suggest and /search for the
# GIL" - tested empirically, see its tuning note in background_scan_fill.
SCAN_WORKER_PACING_SLEEP_SECONDS = 0.01

# V2 recency tuning. Tick = 30s wall-clock bucket, derived from time.time()
# directly rather than a manual counter, so it's always correct even across
# restarts. DECAY_FACTOR is applied once per elapsed tick since a query was
# last touched - a search's contribution to recency_score halves roughly
# every ~7 ticks (~3.5 minutes), fast enough to watch decay live in a demo.
TICK_SECONDS = 30
DECAY_FACTOR = 0.9
COUNT_WEIGHT = 0.01  # all-time count's weight as a tie-breaker/floor in the hybrid score

# A query's recency_score decays asymptotically toward 0 but never hits it
# exactly - below this, it's no longer meaningfully "trending" and gets
# pruned out of the trending list entirely (see _merge_update_trending_cache
# / refresh_recency_cache). The stable list (cache_servers) still has it -
# this only controls when it stops getting a trending-list boost.
RECENCY_PRUNE_THRESHOLD = 0.5

_buffer_lock = threading.Lock()


def get_current_tick():
    return int(time.time() // TICK_SECONDS)


def hybrid_score(query):
    """recency_score + a small fraction of all-time count. Recency dominates
    (so genuinely fresh activity wins), but count acts as a floor/tie-breaker
    so historically popular-but-quiet queries don't tie at exactly 0 with
    everything else that's never been searched live."""
    return recency_memory.get(query, 0) + COUNT_WEIGHT * frequency_memory.get(query, 0)


def get_prefixes(query):
    """Get all prefixes of length >= MIN_PREFIX_LENGTH"""
    query = query.lower().strip()
    return [query[:i] for i in range(MIN_PREFIX_LENGTH, len(query) + 1)]


def compute_top10_via_scan(prefix):
    """Fallback for a prefix that was never precomputed (outside the top
    TOP_N_PRECOMPUTE queries at build time) and hasn't been searched live
    yet either. A linear scan over frequency_memory - NOT binary search, we
    no longer maintain any sorted structure at all. Measured cost: ~5.5ms,
    almost entirely independent of how many queries actually match, since
    the scan always touches the full dataset regardless. Paid once per
    distinct cold prefix for the life of the process - the caller writes
    the result into the cache, so every subsequent request for the same
    prefix is an instant hit afterward.

    This is the ONLY scan-fallback in the system - it only ever fills
    cache_servers (the stable list). The trending list (recency_cache_servers)
    never needs a scan: an empty trending list is always a correct answer,
    never "missing data" - see the module docstring."""
    matches = [(q, c) for q, c in frequency_memory.items() if q.startswith(prefix)]
    top10 = heapq.nlargest(TOP_N, matches, key=itemgetter(1))
    return [q for q, _ in top10]


def _enqueue_scan(prefix):
    """Marks a prefix as needing a real scan of cache_servers at some point,
    without doing it now. Dedup'd via pending_scans_set so the same prefix
    never sits in the queue twice."""
    with scan_queue_lock:
        if prefix not in pending_scans_set:
            pending_scans_set.add(prefix)
            pending_scans.append(prefix)


def background_scan_fill():
    """Drains pending_scans forever, one prefix at a time: the only place
    the expensive ~5.5ms compute_top10_via_scan actually runs for
    queue-deferred misses. This keeps flush_buffer itself fast no matter
    how many never-cached prefixes one flush's changed_queries happen to
    touch - it only ever appends prefix names to the queue, it never scans.

    A /suggest request for a prefix that's still waiting in this queue is
    NOT blocked by it - main.py does its own scan immediately on a miss,
    independent of (and usually faster than) this background drain. So this
    worker only needs to "win the race" for prefixes nobody happens to ask
    for live; the already-cached check below makes that race harmless
    either way - whichever side gets there first wins, the other skips."""
    while True:
        with scan_queue_lock:
            prefix = pending_scans.popleft() if pending_scans else None
            if prefix is not None:
                pending_scans_set.discard(prefix)

        if prefix is None:
            time.sleep(SCAN_WORKER_IDLE_SLEEP_SECONDS)
            continue

        server = ring.get_server(prefix)
        if server and cache_servers.get(server, {}).get(prefix) is None:
            top10_queries = compute_top10_via_scan(prefix)
            with cache_topology_lock:
                if server in cache_servers:
                    cache_servers[server][prefix] = top10_queries

        time.sleep(SCAN_WORKER_PACING_SLEEP_SECONDS)


def _merge_update_cache(prefix, changed_queries):
    """Recompute a prefix's top 10 by merging its EXISTING cached entry with
    just the queries that changed this flush, instead of recomputing from the
    full dataset. Correct because of the write-through invariant: the cache
    is always already-accurate going into a flush, so any query not in the
    old top 10 and not changed this flush still has the same count it had
    before - it couldn't have newly entered the top 10. So the new true top
    10 is guaranteed to be found within (old top 10) union (changed queries).

    Locked: redistribute_cache() replaces cache_servers[server] with a brand
    new dict object when a server is added/removed - a write landing on the
    old dict object right as that swap happens would be silently lost."""
    server = ring.get_server(prefix)
    if not server:
        return

    with cache_topology_lock:
        if server not in cache_servers:
            return
        existing = cache_servers[server].get(prefix)

    if existing is None:
        # No cached entry - this prefix was either outside the precomputed
        # top TOP_N_PRECOMPUTE queries at build time, or genuinely brand
        # new. changed_queries alone is NOT guaranteed to be the full
        # answer here (unlike when every prefix was precomputed): there
        # could be other already-existing, merely-uncommon queries matching
        # this prefix that were never precomputed - the real answer needs a
        # full scan, which is too expensive to do inline for every one of
        # potentially thousands of these per flush. Defer it to
        # background_scan_fill instead - flush_buffer stays fast no matter
        # how many never-cached prefixes it touches.
        _enqueue_scan(prefix)
        return

    candidates = {q: frequency_memory.get(q, 0) for q in existing}
    for q in changed_queries:
        candidates[q] = frequency_memory.get(q, 0)

    top10 = heapq.nlargest(TOP_N, candidates.items(), key=itemgetter(1))
    with cache_topology_lock:
        if server in cache_servers:
            cache_servers[server][prefix] = [q for q, _ in top10]


def _merge_update_trending_cache(prefix, changed_queries):
    """Updates the TRENDING list (recency_cache_servers) for a prefix - the
    small, often-empty list of queries with currently-meaningful recency
    activity. Unlike _merge_update_cache, there is no "existing is None"
    scan-fallback branch here at all: an empty/missing trending list is
    always a correct answer ("nothing is trending here right now"), never a
    "missing data" state, because membership in this list is ENTIRELY
    determined by actual search activity (changed_queries), which this
    function always has direct access to.

    Candidates considered: the prefix's existing trending members (might
    still be trending) + whatever was just searched this flush. Anything
    whose recency_score has decayed below RECENCY_PRUNE_THRESHOLD gets
    dropped from the list entirely - this is the active pruning step that a
    single merged hybrid_score list couldn't do safely (it had no way to
    re-discover a stable-list query that got displaced). Here, nothing
    needs "re-discovering": the stable list (cache_servers) never lost it
    in the first place, and main.py merges both lists at serve time."""
    server = ring.get_server(prefix)
    if not server:
        return

    with cache_topology_lock:
        if server not in recency_cache_servers:
            return
        existing = recency_cache_servers.get(server, {}).get(prefix, [])

    candidates = {q: hybrid_score(q) for q in existing}
    for q in changed_queries:
        candidates[q] = hybrid_score(q)

    trending = [q for q in candidates if recency_memory.get(q, 0) > RECENCY_PRUNE_THRESHOLD]
    new_top = [q for q, _ in heapq.nlargest(TOP_N, ((q, candidates[q]) for q in trending), key=itemgetter(1))]

    with cache_topology_lock:
        if server in recency_cache_servers:
            if new_top:
                recency_cache_servers[server][prefix] = new_top
                dirty_recency_prefixes.add(prefix)
            else:
                recency_cache_servers[server].pop(prefix, None)
                dirty_recency_prefixes.discard(prefix)


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
    update frequency_memory, then patch BOTH caches (count-ranked and
    recency-ranked) for every affected prefix via the cheap old-top10-plus-
    deltas merge above, then clear the WAL since these deltas are now
    durably in SQLite.

    Also decays and refreshes each changed query's recency_score (V2) in the
    same pass, using the same batch of deltas - one mechanism driving both
    caches and the recency signal.
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
        frequency_memory[query] = frequency_memory.get(query, 0) + delta

        new_recency = _decay_recency(query, current_tick) + delta
        recency_memory[query] = new_recency
        last_tick_memory[query] = current_tick
        db_updates[query] = (delta, new_recency, current_tick)

        for prefix in get_prefixes(query):
            prefix_to_changed.setdefault(prefix, []).append(query)

    update_counts_and_recency_batch(db_updates)

    # Each call is fast: either a cheap in-memory merge (already-cached
    # prefixes - the vast majority of real traffic) or, for a never-cached
    # prefix, just an O(1) append onto a queue (see _enqueue_scan) - never
    # an inline scan. This is what keeps flush_buffer itself fast
    # regardless of how many distinct prefixes prefix_to_changed holds, or
    # how many of those happen to be never-cached - the actual ~5.5ms scans
    # for those happen later, off to
    # the side, in background_scan_fill.
    for prefix, changed_queries in prefix_to_changed.items():
        _merge_update_cache(prefix, changed_queries)
        _merge_update_trending_cache(prefix, changed_queries)

    clear_wal()
    print(
        f"Flushed {len(deltas)} queries ({sum(deltas.values())} total searches) "
        f"-> 1 DB transaction, {len(prefix_to_changed)} prefixes patched"
    )


def _decay_all_in_memory():
    """Decays every query's recency_score (in-memory only, no DB write) based
    on elapsed ticks, even ones nobody has searched recently. Lazy decay alone
    (in flush_buffer) never "catches up" a quiet query's stored score until
    someone searches it again - this is what makes the values used by
    refresh_recency_cache actually current for RANKING purposes.

    Deliberately does NOT write to SQLite. recency_score is a soft freshness
    signal, not critical data - the persisted copy is allowed to lag until
    that query is next searched (flush_buffer persists it then) or until
    /decay is explicitly called. Measured why this matters: a single
    multi-thousand-row decay write, competing with SQLite's one-writer-at-a-
    time semantics against many concurrent flush commits, was blocking
    /search requests for over a second (worst case observed: 2.6s) under
    heavy concurrent load - confirmed by disabling DB writes here entirely,
    which dropped max latency from ~860ms back to ~86ms with no other
    change."""
    current_tick = get_current_tick()
    updated = 0
    for query in list(recency_memory.keys()):
        if recency_memory.get(query, 0) == 0:
            continue
        recency_memory[query] = _decay_recency(query, current_tick)
        last_tick_memory[query] = current_tick
        updated += 1
    return updated


DECAY_DB_CHUNK_SIZE = 1000


def apply_global_decay():
    """Like _decay_all_in_memory, but ALSO persists to SQLite (in small
    chunks, to avoid hogging the single SQLite writer slot for too long in
    one transaction). Used by the explicit POST /decay endpoint - a
    deliberate, infrequent admin action where a brief wait is acceptable,
    unlike the periodic background thread (see background_decay_refresh)."""
    current_tick = get_current_tick()
    db_chunk = {}
    updated = 0

    for query in list(recency_memory.keys()):
        if recency_memory.get(query, 0) == 0:
            continue
        new_score = _decay_recency(query, current_tick)
        recency_memory[query] = new_score
        last_tick_memory[query] = current_tick
        db_chunk[query] = (new_score, current_tick)
        updated += 1

        if len(db_chunk) >= DECAY_DB_CHUNK_SIZE:
            update_recency_only_batch(db_chunk)
            db_chunk = {}

    if db_chunk:
        update_recency_only_batch(db_chunk)

    return updated


REFRESH_CHUNK_SIZE = 2000


def refresh_recency_cache():
    """Periodic re-sort AND prune of the trending list's EXISTING members,
    using freshly decayed scores. No new candidates are ever discovered
    here (decay only moves scores DOWN, so nothing currently outside the
    trending list could rise into it purely from time passing) - but
    members already IN the list can and do fall below
    RECENCY_PRUNE_THRESHOLD as they decay, and get dropped from the
    trending list entirely. That's safe precisely because they're never
    "lost" - the stable list (cache_servers) still has them, and main.py
    merges both lists at serve time.

    Only touches dirty_recency_prefixes (prefixes with a currently non-empty
    trending list) - NOT all cached prefixes, which is the vast majority
    with zero live search activity and an empty/absent trending entry.

    Processed in chunks, re-acquiring the lock between each: under heavy
    search activity the dirty set can grow large, and holding the lock for
    one giant pass was measured to stall live requests by hundreds of ms."""
    prefixes = list(dirty_recency_prefixes)
    for i in range(0, len(prefixes), REFRESH_CHUNK_SIZE):
        chunk = prefixes[i : i + REFRESH_CHUNK_SIZE]
        with cache_topology_lock:
            for prefix in chunk:
                server = ring.get_server(prefix)
                if not server or server not in recency_cache_servers:
                    continue
                candidates = recency_cache_servers[server].get(prefix)
                if not candidates:
                    dirty_recency_prefixes.discard(prefix)
                    continue
                survivors = [q for q in candidates if recency_memory.get(q, 0) > RECENCY_PRUNE_THRESHOLD]
                if survivors:
                    recency_cache_servers[server][prefix] = sorted(
                        survivors, key=hybrid_score, reverse=True
                    )[:TOP_N]
                else:
                    recency_cache_servers[server].pop(prefix, None)
                    dirty_recency_prefixes.discard(prefix)


def background_decay_refresh():
    """Background daemon thread - periodically decays recency_memory
    in-memory (so it reflects the current tick for every query, not just
    ones recently searched) and re-sorts the recency cache to match.
    Deliberately uses the in-memory-only decay (no DB write) - see
    _decay_all_in_memory's docstring for why."""
    while True:
        time.sleep(TICK_SECONDS)
        t0 = time.perf_counter()
        n = _decay_all_in_memory()
        t1 = time.perf_counter()
        refresh_recency_cache()
        t2 = time.perf_counter()
        print(f"DIAG_DECAY in_memory={t1-t0:.3f}s ({n}) refresh={t2-t1:.3f}s (dirty={len(dirty_recency_prefixes)})")


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
    2. Load all data from SQLite into frequency_memory/recency_memory.
    3. Build the STABLE list (cache_servers) bottom-up: start from complete
       queries (the longest "prefix" of anything is the query itself), then
       walk one character shorter at a time, merging each prefix's top 10
       from its own score plus its children's already-known top 10 lists -
       never rescanning the full dataset at any prefix. Single ranking only
       (by count) - the TRENDING list (recency_cache_servers) needs no
       precompute at all, since an empty trending list is always a correct
       starting state (see module docstring).
    4. Rebuild trending lists ONLY for queries with genuinely still-relevant
       residual recency activity (recency_score > threshold) - cheap, since
       on a fresh database this set is empty, and even after real traffic
       it's a small fraction of the full dataset, not all ~300k queries.
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

    if not frequency_memory:
        print("Cache ready. 0 prefixes (empty dataset).")
        return

    # Only the top TOP_N_PRECOMPUTE queries by count get a precomputed cache
    # entry - building all ~3M prefixes for the full dataset cost ~6.8s,
    # most of it for the long tail of rarely-searched queries that may never
    # actually get typed. Everything outside this set is filled lazily on
    # first miss (a ~5.5ms linear scan - see compute_top10_via_scan - then
    # cached forever after, so it's a one-time cost per distinct cold
    # prefix, not a per-request one). Measured: covering the top 100,000
    # queries (a third of this dataset) already covers ~74% of real search
    # volume, while cutting startup from ~6.8s to ~1.7-2.2s.
    precompute_queries = (
        list(frequency_memory.keys())
        if len(frequency_memory) <= TOP_N_PRECOMPUTE
        else [q for q, _ in heapq.nlargest(TOP_N_PRECOMPUTE, frequency_memory.items(), key=itemgetter(1))]
    )

    print("Building stable cache bottom-up (merging children prefixes upward)...")

    # Bucket queries by their own length so each one is injected as "the
    # complete word at this prefix" exactly once, at its own length - O(N)
    # total across the whole build, not re-scanned at every level.
    length_buckets = {}
    max_len = 0
    for query in precompute_queries:
        length_buckets.setdefault(len(query), []).append(query)
        max_len = max(max_len, len(query))

    next_level_count = {}  # prefix (length L+1) -> top10 [(query, count), ...]
    total_prefixes = 0

    for length in range(max_len, MIN_PREFIX_LENGTH - 1, -1):
        candidates_count = {}
        for child_prefix, child_top10 in next_level_count.items():
            candidates_count.setdefault(child_prefix[:length], []).extend(child_top10)
        for query in length_buckets.get(length, []):
            candidates_count.setdefault(query, []).append((query, frequency_memory[query]))

        current_level_count = {}
        for prefix in candidates_count:
            top10_count = heapq.nlargest(TOP_N, candidates_count[prefix], key=itemgetter(1))
            current_level_count[prefix] = top10_count
            server = ring.get_server(prefix)
            if server and server in cache_servers:
                cache_servers[server][prefix] = [q for q, _ in top10_count]
            total_prefixes += 1

        next_level_count = current_level_count

    print(f"Cache ready. {total_prefixes} prefixes cached across {len(ring.servers)} servers.")

    # Trending list rebuild: only needed across an unclean-ish restart where
    # SOME queries still have meaningful (not yet decayed away) recency
    # activity from before the restart. Scoped to just those queries - not
    # the full dataset - since on a fresh database (the common case) this
    # list is empty and the loop below does nothing at all.
    trending_queries = [q for q in recency_memory if recency_memory[q] > RECENCY_PRUNE_THRESHOLD]
    if trending_queries:
        print(f"Rebuilding trending lists for {len(trending_queries)} queries with residual recency activity...")
        prefix_candidates = {}
        for query in trending_queries:
            score = hybrid_score(query)
            for prefix in get_prefixes(query):
                prefix_candidates.setdefault(prefix, []).append((query, score))
        for prefix, candidates in prefix_candidates.items():
            top = [q for q, _ in heapq.nlargest(TOP_N, candidates, key=itemgetter(1))]
            server = ring.get_server(prefix)
            if server and server in recency_cache_servers:
                recency_cache_servers[server][prefix] = top
                dirty_recency_prefixes.add(prefix)


def redistribute_cache():
    """Called after adding/removing a server. Adding/removing a server only
    changes ROUTING (which server owns a prefix) - it never changes the
    actual top-10 answer for any prefix - so we just re-bucket the existing,
    already-correct cached entries (in BOTH caches) into their new homes
    instead of recomputing anything.

    Caller MUST already hold cache_topology_lock (main.py's add_server/
    remove_server do) - this function does NOT acquire it itself, since it's
    a plain Lock (not reentrant) and its only callers already hold it."""
    old_entries = {}
    for server_dict in cache_servers.values():
        old_entries.update(server_dict)
    old_recency_entries = {}
    for server_dict in recency_cache_servers.values():
        old_recency_entries.update(server_dict)

    for server in cache_servers:
        cache_servers[server] = {}
    for server in recency_cache_servers:
        recency_cache_servers[server] = {}

    for prefix, top10 in old_entries.items():
        server = ring.get_server(prefix)
        if server and server in cache_servers:
            cache_servers[server][prefix] = top10

    for prefix, top10 in old_recency_entries.items():
        server = ring.get_server(prefix)
        if server and server in recency_cache_servers:
            recency_cache_servers[server][prefix] = top10
