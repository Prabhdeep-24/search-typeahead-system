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
    """Recompute top 10 for prefix and store in the cache node it hashes to"""
    server = ring.get_server(prefix)
    if server and server in cache_servers:
        cache_servers[server][prefix] = compute_top10(prefix)


def flush_buffer():
    """
    Swap out the write buffer, apply all deltas to SQLite in ONE transaction,
    update frequency_memory, recompute cache for every affected prefix,
    then clear the WAL since these deltas are now durably in SQLite.
    """
    with _buffer_lock:
        if not write_buffer:
            return
        deltas = dict(write_buffer)
        write_buffer.clear()

    if not deltas:
        return

    update_counts_batch(deltas)

    affected_prefixes = set()
    for query, delta in deltas.items():
        is_new = query not in frequency_memory
        frequency_memory[query] = frequency_memory.get(query, 0) + delta
        if is_new:
            _register_new_query(query)
        affected_prefixes.update(get_prefixes(query))

    for prefix in affected_prefixes:
        update_cache(prefix)

    clear_wal()
    print(
        f"Flushed {len(deltas)} queries ({sum(deltas.values())} total searches) "
        f"-> 1 DB transaction, {len(affected_prefixes)} prefixes recomputed"
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
    3. Precompute ALL prefixes and populate cache servers.
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

    print("Building cache (precomputing all prefixes)...")
    all_prefixes = set()
    for query in frequency_memory:
        for prefix in get_prefixes(query):
            all_prefixes.add(prefix)

    for prefix in all_prefixes:
        update_cache(prefix)

    print(f"Cache ready. {len(all_prefixes)} prefixes cached across {len(ring.servers)} servers.")


def redistribute_cache():
    """Called after adding/removing a server. Recomputes which server each
    prefix belongs to, demonstrating that consistent hashing only moves ~1/N
    of the keys instead of reshuffling everything."""
    for server in cache_servers:
        cache_servers[server] = {}

    all_prefixes = set()
    for query in frequency_memory:
        for prefix in get_prefixes(query):
            all_prefixes.add(prefix)

    for prefix in all_prefixes:
        update_cache(prefix)
