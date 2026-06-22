"""Core suggestion logic: compute, cache, flush.
Write-through invalidation - flush_query immediately recomputes affected prefixes,
so the cache is always accurate after every flush.

V2 (recency-aware ranking): a second, parallel cache (recency_cache_servers),
built and patched the SAME way as the basic one - bottom-up merge at startup,
delta-merge on flush - just ranked by hybrid_score instead of raw count. The
one thing per-flush merging alone can't catch is decay: recency_score shrinks
purely from time passing, with no write to react to. background_decay_refresh
covers that case with a periodic re-sort of each prefix's EXISTING candidates
(no search needed - see its docstring for why that's sufficient)."""
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
    recency_cache_servers,
    recency_memory,
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


def get_prefixes(query):
    """Get all prefixes of length >= MIN_PREFIX_LENGTH"""
    query = query.lower().strip()
    return [query[:i] for i in range(MIN_PREFIX_LENGTH, len(query) + 1)]


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
            # This exact prefix has never been a prefix of ANY query before -
            # not in the original build (which covers every prefix of every
            # original query), not in any prior flush (which patches every
            # affected prefix). So the only queries that could possibly match
            # it right now are the ones already in changed_queries - nothing
            # else to search for, no need to fall back to a full lookup.
            candidates = {q: frequency_memory.get(q, 0) for q in changed_queries}
            top10 = heapq.nlargest(TOP_N, candidates.items(), key=itemgetter(1))
            cache_servers[server][prefix] = [q for q, _ in top10]
            return

        candidates = {q: frequency_memory.get(q, 0) for q in existing}
        for q in changed_queries:
            candidates[q] = frequency_memory.get(q, 0)

        top10 = heapq.nlargest(TOP_N, candidates.items(), key=itemgetter(1))
        cache_servers[server][prefix] = [q for q, _ in top10]


def _merge_update_recency_cache(prefix, changed_queries):
    """Same merge logic as _merge_update_cache, ranked by hybrid_score and
    patching recency_cache_servers instead. The same correctness argument
    applies: existing entries' relative count to each other can't change
    without a write, so (old top10) union (changed queries) still covers the
    true new top10 - decay's effect on RANKING (not membership) is handled
    separately by background_decay_refresh."""
    server = ring.get_server(prefix)
    if not server:
        return

    with cache_topology_lock:
        if server not in recency_cache_servers:
            return

        # Break the build-time alias (see build_cache) the moment a real
        # write needs to happen: recency_cache_servers[server] may still be
        # the SAME dict object as cache_servers[server] (never copied, to
        # save ~3M writes when nothing has diverged yet). Mutating it in
        # place here would corrupt cache_servers too - replace it with a
        # real independent copy first. Safe to keep sharing the unmodified
        # entries' list objects, since neither side ever mutates a list in
        # place - both only ever reassign a prefix's value wholesale.
        if recency_cache_servers[server] is cache_servers.get(server):
            recency_cache_servers[server] = dict(recency_cache_servers[server])

        # This prefix now has at least one candidate with non-zero
        # recency_score - the periodic refresh needs to know to re-sort it.
        dirty_recency_prefixes.add(prefix)

        existing = recency_cache_servers[server].get(prefix)
        if existing is None:
            candidates = {q: hybrid_score(q) for q in changed_queries}
            top10 = heapq.nlargest(TOP_N, candidates.items(), key=itemgetter(1))
            recency_cache_servers[server][prefix] = [q for q, _ in top10]
            return

        candidates = {q: hybrid_score(q) for q in existing}
        for q in changed_queries:
            candidates[q] = hybrid_score(q)

        top10 = heapq.nlargest(TOP_N, candidates.items(), key=itemgetter(1))
        recency_cache_servers[server][prefix] = [q for q, _ in top10]


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

    for prefix, changed_queries in prefix_to_changed.items():
        _merge_update_cache(prefix, changed_queries)
        _merge_update_recency_cache(prefix, changed_queries)

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
    """Periodic re-sort of cached recency entries' EXISTING candidates, using
    freshly decayed scores. No search involved, and none needed: decay only
    ever moves a score DOWN (DECAY_FACTOR < 1), so a query that isn't already
    sitting in a prefix's cached pool could never rise into it purely from
    time passing - the only way a new contender enters a top10 is an actual
    search, which the flush-time merge above already catches. This step only
    needs to re-order what's already there.

    Only re-sorts dirty_recency_prefixes (prefixes actually touched by a real
    search at some point) - NOT all ~3M cached prefixes. The vast majority
    have zero live search activity, so every candidate's hybrid_score is just
    0.01*count, which decay never changes the relative order of. Re-sorting
    all of them on every tick was measured to cause a multi-second stall.

    Processed in chunks, re-acquiring the lock between each: under heavy
    search activity the dirty set can grow into the hundreds of thousands,
    and holding the lock (blocking every /suggest and flush) for the whole
    pass in one go was measured to stall live requests by several hundred
    ms. Chunking trades one big stall for several much smaller ones, letting
    other threads interleave in between."""
    prefixes = list(dirty_recency_prefixes)
    for i in range(0, len(prefixes), REFRESH_CHUNK_SIZE):
        chunk = prefixes[i : i + REFRESH_CHUNK_SIZE]
        with cache_topology_lock:
            for prefix in chunk:
                server = ring.get_server(prefix)
                if not server or server not in recency_cache_servers:
                    continue
                candidates = recency_cache_servers[server].get(prefix)
                if candidates:
                    recency_cache_servers[server][prefix] = sorted(
                        candidates, key=hybrid_score, reverse=True
                    )[:TOP_N]


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
    3. Build BOTH caches bottom-up in one pass: start from complete queries
       (the longest "prefix" of anything is the query itself), then walk one
       character shorter at a time, merging each prefix's top 10 from its own
       score (if it's itself a complete query) plus its children's already-
       known top 10 lists - never rescanning the full dataset at any prefix.
       cache_servers is ranked by count; recency_cache_servers by hybrid_score
       - same merge, same pass, two independent rankings.
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

    # Fast path: if no query has EVER been searched live, recency_score is 0
    # for everyone, so hybrid_score = 0 + 0.01*count - a pure linear scaling
    # of count. Sorting by hybrid_score then gives the IDENTICAL order to
    # sorting by count, for every prefix, with no exceptions. So on a fresh
    # database (the common case - first run, or any restart before real
    # traffic), we skip computing a second ranking entirely and just copy
    # the count-ranked lists into the recency cache - cutting build time
    # roughly back to the single-ranking cost. Once real searches happen,
    # this no longer holds (some queries now have non-zero recency_score),
    # so the next restart correctly falls back to the full dual-ranking
    # merge below.
    fresh_start = all(v == 0 for v in recency_memory.values())

    print(
        "Building cache bottom-up (merging children prefixes upward)"
        + (", fresh start - recency mirrors basic..." if fresh_start else "...")
    )

    # Bucket queries by their own length so each one is injected as "the
    # complete word at this prefix" exactly once, at its own length - O(N)
    # total across the whole build, not re-scanned at every level.
    length_buckets = {}
    max_len = 0
    for query in frequency_memory:
        length_buckets.setdefault(len(query), []).append(query)
        max_len = max(max_len, len(query))

    next_level_count = {}    # prefix (length L+1) -> top10 [(query, count), ...]
    next_level_recency = {}  # prefix (length L+1) -> top10 [(query, hybrid_score), ...] - unused if fresh_start
    total_prefixes = 0

    if fresh_start:
        # Lean single-ranking pass - the branch is hoisted OUTSIDE the loop
        # entirely (checked once here, not 2.97M times inside it), and both
        # caches are written from the SAME list object, no second ranking
        # computed at all. This is the same shape as the original V1-only
        # build, plus one extra (cheap) dict write per prefix for the second
        # cache - measured to add ~0.9s for ~3M prefixes, not several
        # seconds. Checking the branch inside the loop instead of hoisting
        # it out here was measured to cost over a second extra by itself.
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

        # recency_cache_servers starts as a direct ALIAS of cache_servers
        # (same dict objects, not copies) - zero extra writes for ~3M
        # prefixes, since the two are byte-for-byte identical right now.
        # The alias is broken (replaced with a real independent copy) the
        # moment real search activity first causes genuine divergence - see
        # _merge_update_recency_cache.
        for server in cache_servers:
            recency_cache_servers[server] = cache_servers[server]
    else:
        # Full dual-ranking pass - real search history exists, so the
        # recency ranking can genuinely differ from the count ranking and
        # must be computed for real.
        for length in range(max_len, MIN_PREFIX_LENGTH - 1, -1):
            candidates_count = {}
            candidates_recency = {}
            for child_prefix, child_top10 in next_level_count.items():
                candidates_count.setdefault(child_prefix[:length], []).extend(child_top10)
            for child_prefix, child_top10 in next_level_recency.items():
                candidates_recency.setdefault(child_prefix[:length], []).extend(child_top10)
            for query in length_buckets.get(length, []):
                candidates_count.setdefault(query, []).append((query, frequency_memory[query]))
                candidates_recency.setdefault(query, []).append((query, hybrid_score(query)))

            current_level_count = {}
            current_level_recency = {}
            for prefix in candidates_count:
                top10_count = heapq.nlargest(TOP_N, candidates_count[prefix], key=itemgetter(1))
                top10_recency = heapq.nlargest(
                    TOP_N, candidates_recency.get(prefix, []), key=itemgetter(1)
                )
                current_level_count[prefix] = top10_count
                current_level_recency[prefix] = top10_recency

                server = ring.get_server(prefix)
                if server and server in cache_servers:
                    cache_servers[server][prefix] = [q for q, _ in top10_count]
                if server and server in recency_cache_servers:
                    recency_cache_servers[server][prefix] = [q for q, _ in top10_recency]
                total_prefixes += 1

            next_level_count = current_level_count
            next_level_recency = current_level_recency

    print(f"Cache ready. {total_prefixes} prefixes cached across {len(ring.servers)} servers.")


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
