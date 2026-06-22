"""In-memory state. Everything here is derived from SQLite and safe to lose/rebuild on restart,
except write_buffer (protected by the write-ahead log in wal.py)."""
import threading
from collections import deque

# Sharded cache - each server is a Python dictionary
# Key: prefix (str), Value: list of top 10 suggestions, ranked by all-time count
cache_servers = {
    "server1": {},
    "server2": {},
    "server3": {},
}

# Same shape as cache_servers, but ranked by hybrid_score (recency-aware).
# Built the same way (bottom-up merge), patched the same way (delta-merge on
# flush) - PLUS a periodic background re-sort to account for decay, which
# changes scores purely with elapsed time, not just writes (see
# background_decay_refresh in suggestions.py).
recency_cache_servers = {
    "server1": {},
    "server2": {},
    "server3": {},
}

# Guards every direct write into cache_servers/recency_cache_servers (a
# flush's patch, a topology change, or the periodic decay re-sort), AND
# redistribute_cache()'s full rebuild. redistribute_cache() replaces
# cache_servers[server] with a brand-new dict object - a concurrent write
# landing on the OLD dict object right as that swap happens is silently
# lost. Confirmed via a concurrent-admin-op stress test before this lock
# existed.
cache_topology_lock = threading.Lock()

# Write buffer - holds pending counts before flushing to SQLite
# Key: query (str), Value: pending count (int)
write_buffer = {}

# In-memory copy of frequency table for fast reads during cache computation.
# Loaded from SQLite on startup, kept in sync after every flush.
frequency_memory = {}

# Cache hit/miss counters for the performance report
cache_stats = {"hits": 0, "misses": 0}

# V2 (recency-aware ranking): query -> decayed recency score, and the tick
# (30s wall-clock bucket) it was last updated at. Mirrors recency_score/
# last_tick in SQLite.
recency_memory = {}
last_tick_memory = {}

# Prefixes whose cached recency entry contains at least one candidate with a
# non-zero recency_score - i.e. prefixes actually touched by a real search at
# some point. The vast majority of the ~3M cached prefixes never have any
# live search activity, so their hybrid_score is just 0.01*count for every
# candidate - decay never changes that relative order. The periodic refresh
# only needs to re-sort THIS set, not all ~3M entries (see suggestions.py).
dirty_recency_prefixes = set()

# Prefixes that are missing from cache_servers and/or recency_cache_servers
# (outside the precomputed top TOP_N_PRECOMPUTE queries, never searched
# live either) and are waiting for background_scan_fill to compute their
# real top10 via a full scan. A single shared queue, not one per cache:
# computing the count-ranked and hybrid-score-ranked top10 both require the
# same scan over frequency_memory, so the worker does both in one pass
# (compute_top10_both_via_scan) and writes whichever side(s) still need it
# - re-checked at drain time, not assumed from whatever triggered the
# enqueue. A flush only ever appends here - it never scans inline itself
# anymore, so a flush whose changed_queries happen to touch many
# never-cached prefixes stays fast no matter how many there are. A live
# /suggest request for one of these prefixes is NOT blocked by this queue -
# it does its own scan immediately and independently (see main.py); this
# queue only exists to proactively fill in prefixes nobody has happened to
# ask for yet. pending_scans_set is for O(1) "already queued, don't
# enqueue twice" checks; the pair is mutated together under scan_queue_lock.
pending_scans = deque()
pending_scans_set = set()
scan_queue_lock = threading.Lock()
