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
