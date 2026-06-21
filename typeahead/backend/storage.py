"""In-memory state. Everything here is derived from SQLite and safe to lose/rebuild on restart,
except write_buffer (protected by the write-ahead log in wal.py)."""
import threading

# Sharded cache - each server is a Python dictionary
# Key: prefix (str), Value: list of top 10 suggestions
cache_servers = {
    "server1": {},
    "server2": {},
    "server3": {},
}

# Guards every direct write into cache_servers (a cache-miss fill, a flush's
# patch, or a topology change), AND redistribute_cache()'s full rebuild.
# redistribute_cache() replaces cache_servers[server] with a brand-new dict
# object - a concurrent write landing on the OLD dict object right as that
# swap happens is silently lost (no crash, just a dropped cache entry).
# Confirmed via a concurrent-admin-op stress test before this lock existed.
cache_topology_lock = threading.Lock()

# Write buffer - holds pending counts before flushing to SQLite
# Key: query (str), Value: pending count (int)
write_buffer = {}

# In-memory copy of frequency table for fast reads during cache computation.
# Loaded from SQLite on startup, kept in sync after every flush.
frequency_memory = {}

# Alphabetically sorted list of every query string in frequency_memory.
# Internal-only structure used by compute_top10 to find prefix matches via
# binary search instead of scanning all of frequency_memory on every call -
# necessary once the dataset grows into the millions (see suggestions.py).
sorted_queries = []

# Cache hit/miss counters for the performance report
cache_stats = {"hits": 0, "misses": 0}

# V2 (recency-aware ranking): query -> decayed recency score, and the tick
# (30s wall-clock bucket) it was last updated at. Mirrors recency_score/
# last_tick in SQLite. Never cached in cache_servers - recomputed live on
# every request, since it can change purely from time passing, not just
# from new searches (see compute_top10_recency in suggestions.py).
recency_memory = {}
last_tick_memory = {}
