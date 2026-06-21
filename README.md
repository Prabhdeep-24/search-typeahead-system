# Search Typeahead System

A search typeahead/autocomplete system built around the backend data-system
design: prefix suggestions ranked by popularity, a distributed cache using
consistent hashing, batch writes to reduce database pressure, and a
recency-aware ranking mode for trending searches.

Built for the HLD101 Search Typeahead assignment ([SST-2028]).

## Contents

- [Architecture](#architecture)
- [Setup](#setup)
- [Running V1 vs V2](#running-v1-vs-v2)
- [API Reference](#api-reference)
- [Dataset](#dataset)
- [Design Decisions & Trade-offs](#design-decisions--trade-offs)
- [Performance Report](#performance-report)
- [Known Limitations](#known-limitations)

---

## Architecture

```
                         ┌─────────────┐
   User types "iph" ───▶ │   /suggest   │
                         └──────┬──────┘
                                │ ring.get_server("iph")
                                ▼
                    ┌───────────────────────┐
                    │  Consistent Hash Ring   │   32 virtual nodes/server
                    │  (cache.py)             │   - keeps load balanced
                    └───────────┬─────────────┘   - only ~1/N keys move
                                │                    when a server is added/removed
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                 ▼
        ┌──────────┐      ┌──────────┐      ┌──────────┐
        │ server1  │      │ server2  │      │ server3  │   in-memory dicts
        │ {prefix: │      │ {prefix: │      │ {prefix: │   prefix -> top10
        │  top10}  │      │  top10}  │      │  top10}  │
        └──────────┘      └──────────┘      └──────────┘
              │ cache miss / new prefix
              ▼
        compute_top10(prefix)        binary search on sorted_queries
                                      + heap-pick top 10

   POST /search ──▶ write_buffer (in-memory) ──▶ flush every 5s or 100 queries
                          │                              │
                          ▼ (also, durability)            ▼
                      wal.log                    1 SQLite transaction
                   (crash recovery)          (frequency, recency_score, last_tick)
                                                          │
                                                          ▼
                                          patch cache (merge old top10 + deltas,
                                          NOT a full rescan)
```

**The three layers, what each owns:**

| Layer | Owns | Survives restart? |
|---|---|---|
| SQLite (`typeahead.db`) | The durable source of truth: `query, count, recency_score, last_tick` | Yes |
| In-memory mirrors (`frequency_memory`, `sorted_queries`, `recency_memory`) | A fast, rebuildable view of SQLite, loaded at startup | No (rebuilt from SQLite) |
| Cache (`cache_servers`) | Precomputed top-10 answers per prefix, sharded across logical nodes | No (rebuilt from the mirrors) |

If everything except SQLite disappeared, the system would rebuild itself correctly on the next startup — nothing except SQLite is the real source of truth.

### Suggestion ranking: built bottom-up, not scanned per-request

Building the cache from scratch by scanning the whole dataset for every prefix would be O(prefixes × dataset size) — infeasible once the dataset is large. Instead, `build_cache()` walks from the **longest** prefixes (complete queries) down to the shortest, merging each prefix's top-10 from its own count (if it's itself a complete query) plus its children's *already-computed* top-10 lists:

```
"iphone"   → [iphone: 100k]                      (a leaf - just itself)
"iphon"    → merge(iphone, iphony, iphons, ...)   → top 10
"ipho"     → merge(iphon, iphoo, ...)             → top 10
"iph"      → merge(ipha, iphb, ..., ipho, ...)    → top 10
```

Each merge only ever looks at a bounded number of candidates (however many distinct next-characters actually occur, never the full dataset), so the total build cost is roughly linear in the dataset size, not in (prefixes × dataset size).

### Updating the cache after a search: merge the delta, don't rescan

When a batch of searches flushes, the cache is **never rebuilt from scratch** — for each affected prefix, the existing (already-correct) cached top-10 is merged with just the handful of queries that changed this batch, and the new top-10 is picked from that small combined set. This is provably correct: any query that wasn't in the old top-10 and didn't change this batch still has the same count it had before, so it couldn't have newly entered the top-10.

### Consistent hashing

Each of the 3 logical cache servers is placed on the hash ring **32 times** (virtual nodes), at different hash positions. With only 3-4 real servers, placing each one once would give a badly skewed split; 32 virtual copies per server averages the load out, since each physical server's "territory" becomes the *sum* of many small scattered arcs instead of one large arc. Verified live: adding a 4th server moves almost exactly 1/4 of the keys (not a full reshuffle), and removing it restores the exact original distribution.

### Batch writes + crash recovery

Searches don't hit SQLite individually — they accumulate in an in-memory `write_buffer`, flushed either every 5 seconds or as soon as 100 distinct queries are pending, whichever comes first. All deltas in a batch are written in **one** SQLite transaction.

The failure case is explicit: if the process crashes between a search being buffered and the next flush, that increment would normally be lost. To close that gap, every search is *also* appended to a plain append-only `wal.log` file (cheap — no transaction overhead, just a line appended) before being buffered. On the next startup, if `wal.log` is non-empty, it means the previous flush never happened — those entries are replayed and applied before anything else starts. Verified with an actual hard-kill mid-flush: the buffered search survived and was recovered on restart.

### Recency-aware ranking (V2)

A second, parallel ranking signal: `hybrid_score = recency_score + 0.01 × count`. `recency_score` decays lazily — `× 0.9` per elapsed 30-second "tick" — only recomputed when a query is actually searched again (not on a global timer). This is computed **live**, never cached, because unlike count, it can change purely from time passing with no new search at all; caching it would mean re-deriving it on read anyway, with no benefit. Both `/suggest` and `/trending` accept `?mode=basic|recency` so the two rankings can be demonstrated side by side from one running server.

---

## Setup

Requires Python 3.10+ and Node 18+.

### Backend

```bash
cd typeahead/backend
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --port 8000
```

On first run, this loads `dataset.csv` into a fresh `typeahead.db` SQLite file and builds the cache — takes a few seconds. Subsequent runs skip the dataset load since the data already exists.

### Frontend

```bash
cd typeahead/frontend
npm install
npm run dev
```

Open the URL Vite prints (typically `http://localhost:5173`).

---

## Running V1 vs V2

There's no separate "V1 mode" / "V2 mode" to switch between — V2 (recency-aware ranking) is additive and always available; V1 behavior is simply what you get by default.

- **V1 (default)**: `GET /suggest?q=iphone` and `GET /trending` — ranked by all-time count.
- **V2**: add `mode=recency` — `GET /suggest?q=iphone&mode=recency`, `GET /trending?mode=recency` — ranked by the recency-weighted hybrid score.

In the UI, the **Basic / Recency-aware** toggle at the top switches both the search bar and the trending list between the two modes live.

To see the recency effect: search the same (otherwise low-count) query a few times via the UI or `POST /search`, then switch to recency mode — it should jump above historically-popular-but-stale results. Wait a few minutes without searching it again and it fades back down (half-life ≈ 3.5 minutes).

---

## API Reference

Full interactive docs are auto-generated by FastAPI at `http://localhost:8000/docs` once the backend is running.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/suggest?q=<prefix>&mode=basic\|recency` | Up to 10 suggestions for a prefix |
| `POST` | `/search?q=<query>` | Submit a search; returns `{"message": "Searched"}` |
| `GET` | `/cache/debug?prefix=<prefix>` | Which cache node owns this prefix, and hit/miss |
| `GET` | `/trending?mode=basic\|recency` | Top 10 globally popular/trending queries |
| `GET` | `/servers/status` | Cache distribution, hit rate, buffer status |
| `POST` | `/servers/add?name=<name>` | Add a cache node to the ring |
| `POST` | `/servers/remove?name=<name>` | Remove a cache node from the ring |
| `POST` | `/decay` | Manually force a global recency decay sweep |
| `GET` | `/recency/debug?query=<query>` | Inspect one query's count/recency/hybrid score |
| `POST` | `/admin/flush` | Manually trigger a buffer flush (for testing/demo) |

---

## Dataset

**Source**: [Wikipedia Pageviews](https://dumps.wikimedia.org/other/pageviews/) — an official, public Wikimedia dump (no login required, no PII). Article titles are used as a real-world proxy for "what people search for."

**How it was built**: 6 hourly snapshots from May 1, 2025 (spread across the day: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC) were downloaded, filtered to English-language articles, stripped of non-article namespaces (`Talk:`, `Category:`, `File:`, etc.), and aggregated by summing view counts per title across all 6 hours. This produced 2,270,786 distinct queries, which were then trimmed to the **top 300,000 by count** — still 3x the assignment's 100,000 minimum — to keep startup build time and memory practical (the full 2.27M set took ~90s/3.8GB to build; the trimmed 300k set takes ~6s/750MB).

`dataset.csv` (committed to the repo) has exactly the columns `query,count`, ready to load directly — no separate download step needed to run the project.

---

## Design Decisions & Trade-offs

| Decision | Why | Trade-off accepted |
|---|---|---|
| Flat dict cache, not a Trie | Simpler to implement/explain; bottom-up merge build avoids the Trie's main advantage (avoiding full rescans) anyway | None significant once the bottom-up build was added |
| Write-through cache invalidation, not TTL | Cache is always exactly correct after a flush, simpler mental model | Slightly more recompute work per flush than lazy TTL expiry |
| Recency computed live, never cached | Recency changes purely from time passing, not just writes; caching it would either go stale silently or need re-deriving anyway | None measurable — verified under load that live computation costs the same as the cached basic path |
| Batch threshold = 100 queries or 5s | Keeps flush windows small enough that the "entire dataset changes in one flush" pathological case (which would take ~10s) never organically occurs | None in practice |
| WAL log for the write buffer | Closes the "crash before flush" durability gap cheaply (plain file append, no transaction overhead) | A few milliseconds of crash exposure (between request arrival and the WAL write completing) |
| `gc.freeze()` after cache build | CPython's cyclic GC periodically scans every long-lived object while holding the GIL; with millions of cache objects this stalled every thread in the process simultaneously (~640ms p100 under load) | None — freezing means the GC simply never scans this generation of objects again; verified normal allocation/collection of new (request-scoped) objects is unaffected |
| Persistent per-thread SQLite connections + WAL mode | Avoids reconnect overhead and `fsync`-on-every-commit blocking | None measurable |
| `cache_topology_lock` around all cache writes + redistribution | Concurrent admin operations (add/remove server) raced on the cache dict's structure, causing a crash and silent data loss (found via targeted concurrency testing) | Negligible — lock contention only matters during the rare add/remove admin action, not regular traffic |

### What would change at much larger scale (discussed, not built — out of scope for this assignment's data size)

- **Compress the cache structure**: an FST/DAWG (shares suffixes, not just prefixes) instead of a flat dict — 10-50x smaller for the same data.
- **Index only the "head" of the distribution**: fast-index the most-searched queries; let the long tail fall back to a slower path.
- **Shard the keyspace itself** (not just the cache) by leading-character ranges, sized by actual data volume rather than alphabet position, with consistent hashing deciding which physical machine owns which range — exactly the same principle already used for the cache layer, applied one level up.
- **Disk-backed structures** (e.g. an LSM-tree store) for whatever doesn't fit in RAM even after the above.

---

## Performance Report

All numbers measured live against the running system in this repo (not estimated), 300,000-query dataset, single machine (10 cores).

### Startup cost (one-time)

| Metric | Value |
|---|---|
| Dataset load into SQLite | 0.5-0.6s |
| Bottom-up cache build (2,968,474 prefixes) | ~5.5-5.7s |
| Peak memory after build | ~750 MB |

### `/suggest` latency under concurrent load

Graduated from 1 to 32 concurrent worker pairs (each pair = 1 suggest + 1 search client), 8s per stage:

| Workers (suggest+search pairs) | Combined req/s | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 1 | 3,246 | 0.50ms | 0.66ms | 4.31ms | 13.87ms |
| 2 | 4,035 | 0.81ms | 1.15ms | 5.28ms | 30.67ms |
| 4 | 4,143 | 1.50ms | 3.06ms | 6.39ms | 44.63ms |
| 8 | 4,312 | 3.04ms | 7.19ms | 8.13ms | 63.30ms |
| 16 | 4,224 | 6.51ms | 10.97ms | 13.63ms | 80.41ms |
| 32 | 4,314 | 13.52ms | 18.90ms | 22.50ms | 101.57ms |

Throughput plateaus around ~4,300 req/s (this machine's practical ceiling); latency degrades smoothly and predictably with concurrency, no instability at any tested scale.

**A real regression was found and fixed during this testing**: under sustained heavy load, CPython's garbage collector periodically scanned the millions of long-lived cache objects while holding the GIL, stalling every thread in the process simultaneously — measured at up to **~640ms** for every in-flight request at once. Calling `gc.freeze()` once after the cache finishes building (since the cache's objects are effectively permanent) dropped this to **~34ms max**, with no other code changes. Confirmed via a clean before/after A/B test.

### Basic vs. recency-mode latency (both under concurrent batch-update load)

To confirm the live-computed recency path doesn't cost more than the cache-backed basic path:

| Mode | Requests | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| `basic` (cached) | 18,179 (1,211 req/s) | 5.51ms | 11.61ms | 14.63ms | 33.13ms |
| `recency` (live-computed) | 18,165 (1,210 req/s) | 5.53ms | 11.57ms | 14.63ms | 35.96ms |

Both running simultaneously, with 15,832 concurrent `/search` requests driving flushes throughout. The two modes track each other within ~1-3ms across every percentile — confirming the decision not to cache recency results was sound.

### Cache hit rate

After the bottom-up build, every prefix that exists in the dataset already has a precomputed answer — the cache starts "warm." Measured hit rate in steady-state traffic: **~95-100%** (misses only occur for genuinely new queries never seen before).

### Write reduction via batching

Example from a live run: a single flush handled **5,000 distinct changed queries (256,008 total search events) in one SQLite transaction**, completing in 0.234s. Without batching, that would have been 256,008 separate transactions.

### Extreme-batch stress test (validates the batching threshold's necessity)

| Batch size (distinct queries changed in one flush) | Flush time |
|---|---|
| 5,000 | 0.23s |
| 50,000 | 2.06s |
| 150,000 | 4.58s |
| 300,000 (the entire dataset at once) | 10.44s |

This confirms why `BUFFER_SIZE_THRESHOLD = 100` matters: if the entire dataset changed within one flush window, that flush alone would take longer than the 5-second interval between flushes. The threshold guarantees the system never organically reaches this regime — flushes happen long before the buffer could grow anywhere near this large.

All of the above is reproducible — the load-test scripts used to generate these numbers are committed in `typeahead/backend/`: `loadtest_concurrent.py` (basic-mode latency under concurrent search+suggest traffic), `loadtest_modes.py` (basic vs. recency comparison), and the inline scripts referenced for the extreme-batch and concurrency tests. Run them with the backend already running (`python3 loadtest_concurrent.py` from inside `typeahead/backend` with the venv active).

### Concurrency correctness (found via targeted adversarial testing, not assumed)

Running continuous search/suggest/flush traffic **simultaneously** with repeated `/servers/add`/`/servers/remove` cycles surfaced two real bugs:
1. A crash (`RuntimeError: dictionary changed size during iteration`) when two admin operations raced on the cache's key set, aborting a redistribution mid-way.
2. A silent data-loss race even without a crash — a concurrent flush write landing on a cache dict object at the exact moment it got replaced during redistribution was dropped with no error.

Both fixed with a shared lock around all cache-topology-affecting operations. Re-running the identical adversarial test against the fix: **0 errors, exact prefix count (2,968,474) preserved throughout** 15 seconds of simultaneous chaos traffic, with no measurable latency regression under normal (non-adversarial) concurrent load.

---

## Known Limitations

- Single-process, single-machine — the "distributed" cache nodes are simulated as separate in-memory dicts within one process, not real separate servers. This was a deliberate choice (see Design Decisions) to demonstrate the consistent-hashing and cache-sharding *concepts* without the operational overhead of running real distributed infrastructure for an assignment of this scope.
- The write buffer has a small (sub-millisecond) crash-exposure window between a request arriving and its WAL write completing.
- Recency decay ticks are wall-clock-based (`time.time() // 30`); if the system clock is changed backwards, ticks could (harmlessly) appear to "go backwards" — not handled specially, since it's not a realistic concern for a local demo.
