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

**The three layers, what each owns:**

| Layer | Owns | Survives restart? |
|---|---|---|
| SQLite (`typeahead.db`) | The durable source of truth: `query, count, recency_score, last_tick` | Yes |
| In-memory mirrors (`frequency_memory`, `recency_memory`, `last_tick_memory`) | A fast, rebuildable view of SQLite, loaded at startup | No (rebuilt from SQLite) |
| Two caches (`cache_servers`, `recency_cache_servers`) | Precomputed top-10 answers per prefix, sharded across logical nodes | No (rebuilt from the mirrors) |

If everything except SQLite disappeared, the system would rebuild itself correctly on the next startup — nothing except SQLite is the real source of truth.

### Two caches, not one: "stable" and "trending"

`cache_servers` is the **stable list** — ranked by all-time count, exactly like a pure V1 system. `recency_cache_servers` is the **trending list** — a small, often-*empty* list holding only queries with currently-meaningful live search activity for that prefix. `mode=recency` requests merge both at serve time (trending first, then the stable list fills remaining slots, deduplicated).

This split exists to fix a real correctness gap: a single list ranked purely by a blended "recency + count" score, updated incrementally, has no way to bring back a quietly-popular query once a temporarily-trending one displaces it and then decays away — nothing outside that list's current membership is ever reconsidered. Keeping the stable list completely separate (and never touched by recency logic) guarantees nothing genuinely popular is ever permanently lost — it just temporarily ranks below whatever's actively trending, and resurfaces automatically the moment that trend fades, because the merge is recomputed fresh on every request.

### The primary algorithm: bottom-up merge (this is what populates the stable list)

The stable cache is **not** filled by computing each prefix independently from a full scan. `build_cache()` walks from the **longest** prefixes (complete queries) down to the shortest, merging each prefix's top-10 from its own count (if it's itself a complete query) plus its children's *already-computed* top-10 lists:

```
"iphone"   → [iphone: 100k]                      (a leaf - just itself)
"iphon"    → merge(iphone, iphony, iphons, ...)   → top 10
"ipho"     → merge(iphon, iphoo, ...)             → top 10
"iph"      → merge(ipha, iphb, ..., ipho, ...)    → top 10
```

Each merge only ever looks at a bounded number of candidates, so the total build cost is roughly linear in the dataset size. **Only the top 100,000 queries by count** (a third of the 300,000-query dataset, but ~74% of real search volume) get a precomputed entry this way — building the full dataset's ~3 million prefixes cost ~6.8s for mostly-never-typed long-tail coverage; capping it at the top 100k cuts startup to ~2.6-2.7s.

**Updating the stable list after a search uses the same idea, applied incrementally**: when a batch of searches flushes, the cache is never rebuilt from scratch — for each affected, *already-cached* prefix, the existing top-10 is merged with just the handful of queries that changed this batch. This is provably correct for count-based ranking specifically: a query that wasn't in the old top-10 and didn't change this batch still has the same count it had before, so it couldn't have newly entered the top-10.

### The fallback path: a real linear scan — only for genuinely uncached prefixes

A prefix outside the precomputed top-100k, never searched live either, is a genuine miss. `/suggest` falls back to `compute_top10_via_scan` — a direct linear scan over `frequency_memory` (~5.5ms, since no sorted structure is maintained at all) — and write-through caches the result, so every later request for that exact prefix is an instant hit afterward. This is a one-time cost per distinct cold prefix, not a per-request one.

A flush that discovers many of these in one batch never scans inline itself — it just appends the prefix's name to a queue (`pending_scans`). A dedicated background thread (`background_scan_fill`) drains that queue, one prefix at a time, paced with a small delay between scans so it can't dominate the CPU while catching up on a large backlog. A live `/suggest` request for a prefix still waiting in that queue is unaffected either way — it does its own scan immediately and independently.

The **trending list never needs any of this** — an empty/missing trending entry is always a correct answer ("nothing is trending here right now"), never "missing data."

```
GET /suggest?q=iph&mode=basic
        │
        ▼
  ring.get_server("iph")  ──▶  which of server1/2/3 owns this prefix (consistent hashing)
        │
        ▼
  cache_servers[server]["iph"]
        │
   ┌────┴────┐
   │ HIT     │  the common case - merge-built/merge-patched entry,
   │ (O(1))  │  return directly, no computation
   └─────────┘
        │
   ┌────┴──────────────┐
   │ MISS (rare)        │  outside the precomputed top-100k, never searched
   │ linear scan         │  live yet - scan frequency_memory directly,
   │ (~5.5ms, one-time)  │  cache the result, return it
   └─────────────────────┘
```

```
GET /suggest?q=iph&mode=recency
        │
        ▼
  trending = recency_cache_servers[server].get("iph", [])   ← small, often empty
  stable   = cache_servers[server].get("iph")                ← falls back to the
                                                                same scan above if missing
        │
        ▼
  merge: trending entries first, then stable entries not already
  included, capped at 10 — a displaced-but-popular query is never lost,
  it just temporarily ranks below the trending list
```

```
POST /search ──▶ write_buffer (in-memory) ──▶ flush every 5s or 100 distinct queries
                     │                              │
                     ▼ (durability)                  ▼
                 wal.log                    1 SQLite transaction
              (crash recovery)          (count, recency_score, last_tick)
                                                      │
                                          ┌───────────┴────────────┐
                                          ▼                        ▼
                                patch stable list           patch trending list
                              (merge old top10 +          (merge existing trending
                               this batch's deltas)         + this batch, prune anything
                                                             decayed below threshold)
```

### Consistent hashing

Each of the 3 logical cache servers is placed on the hash ring **32 times** (virtual nodes), using `zlib.crc32` for routing (a fast, non-cryptographic hash — no collision-resistance needed, just a reasonably even split). With only 3-4 real servers, placing each one once would give a badly skewed split; 32 virtual copies per server averages the load out. Verified live: adding a 4th server moves almost exactly 1/4 of the keys, and removing it restores the exact original distribution.

### Batch writes + crash recovery

Searches don't hit SQLite individually — they accumulate in an in-memory `write_buffer`, flushed every 5 seconds unconditionally (or sooner, as soon as 100 distinct queries are pending). All deltas in a batch are written in **one** SQLite transaction.

The failure case is explicit: if the process crashes between a search being buffered and the next flush, that increment would normally be lost. To close that gap, every search is *also* appended to a plain append-only `wal.log` file before being buffered. On the next startup, if `wal.log` is non-empty, those entries are replayed and applied before anything else starts. Verified with an actual hard-kill mid-flush: the buffered search survived and was recovered on restart.

### Recency-aware ranking (V2)

`hybrid_score = recency_score + 0.01 × count` — recency dominates so genuinely fresh activity wins, but the count term acts as a floor so historically popular-but-quiet queries never tie at exactly 0 with everything else. `recency_score` decays lazily: `× 0.9` per elapsed 30-second "tick" (`current_tick = int(time.time() // 30)`, wall-clock derived, never a manually incremented counter), only recomputed when a query is actually flushed — not on a global timer, and never via a cron job or full-table scan.

A periodic background thread (`background_decay_refresh`, every 30s) keeps the trending list current between flushes: it re-sorts and *prunes* (actively removes, not just reorders) any trending entry whose `recency_score` has decayed below `RECENCY_PRUNE_THRESHOLD` — scoped only to prefixes with real recorded activity (`dirty_recency_prefixes`), never the full multi-million-entry cache. `last_tick` is persisted in SQLite alongside the score, so a restart doesn't wipe decay history.

Both `/suggest` and `/trending` accept `?mode=basic|recency` so the two rankings can be demonstrated side by side from one running server.

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
- **V2**: add `mode=recency` — `GET /suggest?q=iphone&mode=recency`, `GET /trending?mode=recency` — ranked by the recency-weighted hybrid score, merged with the stable list.

In the UI, the **Basic / Recency-aware** toggle at the top switches both the search bar and the trending list between the two modes live.

To see the recency effect: search the same (otherwise low-count) query a few times via the UI or `POST /search`, then switch to recency mode — it should jump above historically-popular-but-stale results. Wait a few minutes without searching it again and it fades back down (half-life ≈ 3.5 minutes), and the previously-displaced popular results return automatically.

---

## API Reference

Full interactive docs are auto-generated by FastAPI at `http://localhost:8000/docs` once the backend is running.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/suggest?q=<prefix>&mode=basic\|recency` | Up to 10 suggestions for a prefix |
| `POST` | `/search?q=<query>` | Submit a search; returns `{"message": "Searched"}` |
| `GET` | `/cache/debug?prefix=<prefix>` | Which cache node owns this prefix, and hit/miss (stable list) |
| `GET` | `/trending?mode=basic\|recency` | Top 10 globally popular/trending queries |
| `GET` | `/servers/status` | Cache distribution, hit rate, buffer status |
| `POST` | `/servers/add?name=<name>` | Add a cache node to the ring |
| `POST` | `/servers/remove?name=<name>` | Remove a cache node from the ring |
| `POST` | `/decay` | Manually force a global recency decay sweep |
| `GET` | `/recency/debug?query=<query>` | Inspect one query's count/recency/hybrid score |
| `POST` | `/admin/flush` | Manually trigger a buffer flush (for testing/demo) |

Note: the hit-rate metric on `/servers/status` tracks the **stable list** (`mode=basic`) only — `mode=recency` always merges the (usually tiny) trending list on top of it.

---

## Dataset

**Source**: [Wikipedia Pageviews](https://dumps.wikimedia.org/other/pageviews/) — an official, public Wikimedia dump (no login required, no PII). Article titles are used as a real-world proxy for "what people search for."

**How it was built**: 6 hourly snapshots from May 1, 2025 (spread across the day: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC) were downloaded, filtered to English-language articles, stripped of non-article namespaces (`Talk:`, `Category:`, `File:`, etc.), and aggregated by summing view counts per title across all 6 hours. This produced 2,270,786 distinct queries, which were then trimmed to the **top 300,000 by count** — still 3x the assignment's 100,000 minimum — to keep startup build time and memory practical.

`dataset.csv` (committed to the repo) has exactly the columns `query,count`, ready to load directly — no separate download step needed to run the project.

## Performance Report

All numbers measured live against the running system in this repo (not estimated), 300,000-query dataset, single machine.

### Startup cost (one-time)

| Metric | Value |
|---|---|
| Dataset load into SQLite | ~0.5-0.6s |
| Bottom-up merge cache build (top 100,000 queries → ~995,000 prefixes) | ~2.6-2.7s |
| Peak memory after build | ~307 MB |

### `/suggest` + `/search` concurrent load (8 suggest workers + 8 search workers, mixed basic/recency traffic)

| Metric | Value |
|---|---|
| Combined `/suggest` throughput | ~1,200 req/s |
| `/search` throughput (driving continuous flushes) | ~950 req/s |
| `/suggest` p50 | 3.51ms |
| `/suggest` p95 | 17.87ms |
| `/suggest` p99 | 23.72ms |
| `/suggest` max | 262.59ms |

