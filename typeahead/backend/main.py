"""FastAPI app - all routes."""
import gc
import threading
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from cache import ring
from database import init_db, load_dataset
from storage import (
    cache_servers,
    cache_stats,
    cache_topology_lock,
    frequency_memory,
    last_tick_memory,
    recency_cache_servers,
    recency_memory,
    write_buffer,
)
from suggestions import (
    apply_global_decay,
    background_decay_refresh,
    background_flush,
    background_scan_fill,
    build_cache,
    compute_top10_via_scan,
    flush_buffer,
    get_current_tick,
    hybrid_score,
    maybe_flush_now,
    redistribute_cache,
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
    # The cache holds millions of long-lived objects. CPython's cyclic GC
    # periodically does a full sweep over every tracked object while holding
    # the GIL - under concurrent load this stalled every thread in the
    # process at once (measured ~640ms p100 spikes during load testing).
    # gc.freeze() marks everything alive right now as permanent so the
    # collector stops re-scanning it on every cycle; confirmed via load test
    # that this drops max /suggest latency from ~640ms to ~34ms.
    gc.collect()
    gc.freeze()
    threading.Thread(target=background_flush, daemon=True).start()
    threading.Thread(target=background_decay_refresh, daemon=True).start()
    threading.Thread(target=background_scan_fill, daemon=True).start()


@app.get("/suggest")
def suggest(q: str = "", mode: str = "basic"):
    """
    Returns top 10 suggestions for a prefix.
    - Lowercase + strip input
    - If length < 3, return []
    - mode="basic" (default): cached (cache_servers), sorted by all-time
      count - V1 behavior, unchanged.
    - mode="recency": V2 - merges TWO lists: the small "trending" list
      (recency_cache_servers, queries with current recency activity) and
      the "stable" list (cache_servers, by count), deduped, trending first.
      A query that gets temporarily outranked and drops out of trending is
      never lost - it's still sitting in the stable list the whole time,
      and naturally resurfaces in the merge once the trending entry decays
      below it (handled by background_decay_refresh pruning the trending
      side - see suggestions.py).
    - Handles empty, mixed-case, no-match gracefully

    Only the top TOP_N_PRECOMPUTE queries get a stable-list cache entry at
    startup (see build_cache) - a prefix outside that set is a genuine miss
    the first time anyone asks for it. We fall back to a linear scan
    (~5.5ms, independent of dataset size growth in practice since it always
    scans the same ~300k rows) and cache the result, so every subsequent
    request for that same prefix is an instant hit afterward - this is a
    one-time cost per distinct cold prefix, not a per-request one. The
    trending list never needs this fallback - empty is always correct.
    """
    q = q.lower().strip()
    if not q or len(q) < 3:
        return {"suggestions": [], "server": None, "cache_hit": False, "mode": mode}

    server = ring.get_server(q)

    if mode == "recency":
        trending = recency_cache_servers.get(server, {}).get(q, [])
        stable = cache_servers.get(server, {}).get(q)
        cache_hit = stable is not None

        if stable is None:
            stable = compute_top10_via_scan(q)
            with cache_topology_lock:
                if server in cache_servers:
                    cache_servers[server][q] = stable

        seen = set(trending)
        merged = list(trending)
        for cand in stable:
            if len(merged) >= 10:
                break
            if cand not in seen:
                merged.append(cand)
                seen.add(cand)

        return {"suggestions": merged, "server": server, "cache_hit": cache_hit, "mode": "recency"}

    cached = cache_servers.get(server, {}).get(q)

    if cached is not None:
        cache_stats["hits"] += 1
        return {"suggestions": cached, "server": server, "cache_hit": True, "mode": "basic"}

    cache_stats["misses"] += 1
    suggestions = compute_top10_via_scan(q)
    with cache_topology_lock:
        if server in cache_servers:
            cache_servers[server][q] = suggestions

    return {"suggestions": suggestions, "server": server, "cache_hit": False, "mode": "basic"}


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
def trending(mode: str = "basic"):
    """Top 10 globally trending queries.
    mode="basic" (default): by all-time count - V1 behavior, unchanged.
    mode="recency": V2 - by hybrid_score (recency-weighted), computed live."""
    if mode == "recency":
        ranked = sorted(frequency_memory.keys(), key=hybrid_score, reverse=True)
    else:
        ranked = [q for q, _ in sorted(frequency_memory.items(), key=lambda kv: kv[1], reverse=True)]
    return {"trending": ranked[:10], "mode": mode}


@app.post("/decay")
def decay():
    """
    V2: manually trigger a global decay sweep.

    Recency scores normally decay lazily - only recomputed when a query is
    actually searched again (see flush_buffer). A query nobody has searched
    in a while just sits at its last-computed value until touched. This
    endpoint forces every query's stored recency_score to catch up to the
    current tick, useful for demoing/inspecting the decay behavior directly
    rather than waiting for organic search traffic to trigger it.
    """
    updated = apply_global_decay()
    return {"status": "decayed", "queries_updated": updated}


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
    """Add a new cache server to the ring. Only ~1/N prefixes should move.
    Adds the new server to BOTH caches (basic and recency)."""
    with cache_topology_lock:
        if name in ring.servers:
            return {"status": "already exists"}

        cache_servers[name] = {}
        recency_cache_servers[name] = {}
        ring.add_server(name)
        redistribute_cache()

    return {
        "status": "added",
        "server": name,
        "distribution": ring.get_distribution(),
    }


@app.post("/servers/remove")
def remove_server(name: str):
    """Remove a cache server from the ring. Remaining prefixes redistribute
    in BOTH caches.

    Order matters here: redistribute_cache() must run BEFORE the server's
    entry is deleted from cache_servers/recency_cache_servers, since it
    reads every existing server's entries to redistribute them. Deleting
    first would silently drop everything that was cached on this server."""
    with cache_topology_lock:
        if name not in ring.servers:
            return {"status": "not found"}

        ring.remove_server(name)
        redistribute_cache()
        del cache_servers[name]
        del recency_cache_servers[name]

    return {
        "status": "removed",
        "server": name,
        "distribution": ring.get_distribution(),
    }


@app.get("/recency/debug")
def recency_debug(query: str = ""):
    """V2 debug endpoint: shows the raw count, recency_score, and resulting
    hybrid_score for a single query - useful for demoing decay live (search
    something repeatedly, watch recency_score climb; stop, watch it fade)."""
    query = query.lower().strip()
    return {
        "query": query,
        "count": frequency_memory.get(query, 0),
        "recency_score": recency_memory.get(query, 0),
        "hybrid_score": hybrid_score(query),
        "last_tick": last_tick_memory.get(query, 0),
        "current_tick": get_current_tick(),
    }


@app.post("/admin/flush")
def admin_flush():
    """Manually trigger a buffer flush (useful for testing/demo without waiting 5s)."""
    flush_buffer()
    return {"status": "flushed"}
