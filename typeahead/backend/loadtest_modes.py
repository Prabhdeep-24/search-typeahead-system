"""
Load test comparing /suggest latency for mode=basic (cache-backed) vs
mode=recency (computed live, never cached) - BOTH running simultaneously
alongside continuous /search traffic that drives periodic batch flushes.
This is the same "does latency degrade from background updates" question as
before, but now covering the recency code path too, which is never served
from cache_servers.
"""
import csv
import random
import threading
import time

import httpx

API_BASE = "http://127.0.0.1:8000"
DURATION_SECONDS = 15
WORKERS_PER_GROUP = 8

with open("dataset.csv", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    all_queries = [row["query"] for row in reader]

long_enough = [q for q in all_queries if len(q) >= 6]
prefixes_pool = [q[:random.randint(3, 6)] for q in random.sample(long_enough, 2000)]

basic_latencies = []
recency_latencies = []
lock = threading.Lock()
search_count = [0]
stop_flag = threading.Event()


def suggest_worker(mode, bucket):
    client = httpx.Client(timeout=10.0)
    while not stop_flag.is_set():
        prefix = random.choice(prefixes_pool)
        t0 = time.perf_counter()
        try:
            client.get(f"{API_BASE}/suggest", params={"q": prefix, "mode": mode})
            elapsed_ms = (time.perf_counter() - t0) * 1000
            with lock:
                bucket.append(elapsed_ms)
        except Exception:
            pass


def search_worker():
    client = httpx.Client(timeout=10.0)
    while not stop_flag.is_set():
        query = random.choice(all_queries)
        try:
            client.post(f"{API_BASE}/search", params={"q": query})
            with lock:
                search_count[0] += 1
        except Exception:
            pass


threads = (
    [threading.Thread(target=suggest_worker, args=("basic", basic_latencies), daemon=True) for _ in range(WORKERS_PER_GROUP)]
    + [threading.Thread(target=suggest_worker, args=("recency", recency_latencies), daemon=True) for _ in range(WORKERS_PER_GROUP)]
    + [threading.Thread(target=search_worker, daemon=True) for _ in range(WORKERS_PER_GROUP)]
)

print(
    f"Running {WORKERS_PER_GROUP} basic-mode + {WORKERS_PER_GROUP} recency-mode suggest "
    f"workers + {WORKERS_PER_GROUP} search workers for {DURATION_SECONDS}s..."
)
start = time.time()
for t in threads:
    t.start()
time.sleep(DURATION_SECONDS)
stop_flag.set()
for t in threads:
    t.join(timeout=3)
elapsed = time.time() - start


def report(name, latencies):
    if not latencies:
        print(f"{name}: no data")
        return
    latencies.sort()
    n = len(latencies)

    def pct(p):
        return latencies[min(n - 1, int(n * p))]

    print(
        f"{name}: {n} reqs ({n/elapsed:.0f} req/s)  "
        f"p50={pct(0.50):.2f}ms  p95={pct(0.95):.2f}ms  p99={pct(0.99):.2f}ms  max={latencies[-1]:.2f}ms"
    )


print(f"\n=== Results over {elapsed:.1f}s (with {search_count[0]} concurrent /search requests driving flushes) ===")
report("mode=basic   (cache-backed)", basic_latencies)
report("mode=recency (live-computed)", recency_latencies)
