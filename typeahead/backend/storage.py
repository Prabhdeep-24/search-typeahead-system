"""In-memory state. Everything here is derived from SQLite and safe to lose/rebuild on restart,
except write_buffer (protected by the write-ahead log in wal.py)."""
import threading

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
