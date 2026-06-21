"""In-memory state. Everything here is derived from SQLite and safe to lose/rebuild on restart,
except write_buffer (protected by the write-ahead log in wal.py)."""

# Sharded cache - each server is a Python dictionary
# Key: prefix (str), Value: list of top 10 suggestions
cache_servers = {
    "server1": {},
    "server2": {},
    "server3": {},
}

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
