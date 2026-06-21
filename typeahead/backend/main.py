"""FastAPI app - all routes."""
import threading
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from cache import ring
from database import init_db, load_dataset
from storage import cache_servers, cache_stats, frequency_memory, write_buffer
from suggestions import (
    background_flush,
    build_cache,
    compute_top10,
    flush_buffer,
    maybe_flush_now,
    redistribute_cache,
    update_cache,
)
from wal import append_to_wal

app = FastAPI(title="Search Typeahead System")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATASET_PATH = str(Path(__file__).parent / "dataset.csv")


@app.on_event("startup")
def startup():
    init_db()
    load_dataset(DATASET_PATH)
    build_cache()
    thread = threading.Thread(target=background_flush, daemon=True)
    thread.start()


@app.get("/suggest")
def suggest(q: str = ""):
    """
    Returns top 10 suggestions for a prefix.
    - Lowercase + strip input
    - If length < 3, return []
    - Get server from consistent hash ring
    - Check cache -> hit: return; miss: compute, store, return
    - Handles empty, mixed-case, no-match gracefully
    """
    q = q.lower().strip()
    if not q or len(q) < 3:
        return {"suggestions": [], "server": None, "cache_hit": False}

    server = ring.get_server(q)
    cached = cache_servers.get(server, {}).get(q)

    if cached is not None:
        cache_stats["hits"] += 1
        return {"suggestions": cached, "server": server, "cache_hit": True}

    cache_stats["misses"] += 1
    suggestions = compute_top10(q)
    cache_servers[server][q] = suggestions

    return {"suggestions": suggestions, "server": server, "cache_hit": False}


@app.post("/search")
def search(q: str = ""):
    """
    Called when user submits a search.
    - Returns {"message": "Searched"}
    - Appends to WAL (durable), increments write buffer (batched, not an
      immediate DB write)
    - Background thread (or size threshold) flushes the buffer to SQLite
    Failure trade-off: if the app crashes between the WAL append and the next
    flush, the WAL is replayed on the next startup to recover the count.
    """
    q = q.lower().strip()
    if not q or len(q) < 3:
        return {"message": "Searched", "status": "ignored", "reason": "query too short"}

    append_to_wal(q)
    write_buffer[q] = write_buffer.get(q, 0) + 1
    maybe_flush_now()

    return {
        "message": "Searched",
        "query": q,
        "buffer_count": write_buffer.get(q, 0),
    }


@app.get("/cache/debug")
def cache_debug(prefix: str = ""):
    """Debug endpoint - shows which cache node owns this prefix and whether
    it's a hit or miss."""
    prefix = prefix.lower().strip()
    server = ring.get_server(prefix)
    cached = cache_servers.get(server, {}).get(prefix)

    return {
        "prefix": prefix,
        "server": server,
        "cache_hit": cached is not None,
        "suggestions": cached if cached is not None else [],
        "frequency_db_count": frequency_memory.get(prefix, 0),
        "buffer_pending": write_buffer.get(prefix, 0),
    }


@app.get("/trending")
def trending():
    """Top 10 globally trending queries.
    V1: by raw count. V2: by recency_score [ TO BE FILLED ]."""
    sorted_queries = sorted(frequency_memory.items(), key=lambda kv: kv[1], reverse=True)
    return {"trending": [q for q, _ in sorted_queries[:10]]}


@app.get("/servers/status")
def servers_status():
    """Shows all servers and prefix distribution - proves consistent hashing works."""
    distribution = ring.get_distribution()
    total_hits = cache_stats["hits"]
    total_misses = cache_stats["misses"]
    total = total_hits + total_misses
    hit_rate = (total_hits / total * 100) if total else 0.0

    return {
        "servers": list(ring.servers),
        "virtual_nodes_per_server": ring.virtual_nodes,
        "distribution": distribution,
        "total_prefixes": sum(distribution.values()),
        "buffer_pending_queries": len(write_buffer),
        "buffer_pending_total": sum(write_buffer.values()),
        "cache_hit_rate_percent": round(hit_rate, 2),
        "cache_hits": total_hits,
        "cache_misses": total_misses,
    }


@app.post("/servers/add")
def add_server(name: str):
    """Add a new cache server to the ring. Only ~1/N prefixes should move."""
    if name in ring.servers:
        return {"status": "already exists"}

    cache_servers[name] = {}
    ring.add_server(name)
    redistribute_cache()

    return {
        "status": "added",
        "server": name,
        "distribution": ring.get_distribution(),
    }


@app.post("/servers/remove")
def remove_server(name: str):
    """Remove a cache server from the ring. Remaining prefixes redistribute."""
    if name not in ring.servers:
        return {"status": "not found"}

    ring.remove_server(name)
    del cache_servers[name]
    redistribute_cache()

    return {
        "status": "removed",
        "server": name,
        "distribution": ring.get_distribution(),
    }


@app.post("/admin/flush")
def admin_flush():
    """Manually trigger a buffer flush (useful for testing/demo without waiting 5s)."""
    flush_buffer()
    return {"status": "flushed"}
