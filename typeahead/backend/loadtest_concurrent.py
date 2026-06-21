"""
One-off load test: fires continuous /search traffic (to trigger periodic
flushes) and continuous /suggest traffic (the user-facing latency that
matters) AT THE SAME TIME, against the live running server, and reports
whether /suggest latency degrades while a flush is happening.
"""
import csv
import random
import threading
import time

import httpx

API_BASE = "http://127.0.0.1:8000"
DURATION_SECONDS = 15
SUGGEST_WORKERS = 8
SEARCH_WORKERS = 8

with open("dataset.csv", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    all_queries = [row["query"] for row in reader]

long_enough = [q for q in all_queries if len(q) >= 6]
prefixes_pool = [q[:random.randint(3, 6)] for q in random.sample(long_enough, 2000)]

suggest_latencies = []
suggest_lock = threading.Lock()
search_count = [0]
stop_flag = threading.Event()


def suggest_worker():
    client = httpx.Client(timeout=5.0)
    while not stop_flag.is_set():
        prefix = random.choice(prefixes_pool)
        t0 = time.perf_counter()
        try:
            client.get(f"{API_BASE}/suggest", params={"q": prefix})
            elapsed_ms = (time.perf_counter() - t0) * 1000
            with suggest_lock:
                suggest_latencies.append((time.time(), elapsed_ms))
        except Exception:
            pass


def search_worker():
    client = httpx.Client(timeout=5.0)
    while not stop_flag.is_set():
        query = random.choice(all_queries)
        try:
            client.post(f"{API_BASE}/search", params={"q": query})
            with suggest_lock:
                search_count[0] += 1
        except Exception:
            pass


threads = []
for _ in range(SUGGEST_WORKERS):
    t = threading.Thread(target=suggest_worker, daemon=True)
    threads.append(t)
for _ in range(SEARCH_WORKERS):
    t = threading.Thread(target=search_worker, daemon=True)
    threads.append(t)

print(f"Starting {SUGGEST_WORKERS} suggest workers + {SEARCH_WORKERS} search workers for {DURATION_SECONDS}s...")
start = time.time()
for t in threads:
    t.start()

for i in range(DURATION_SECONDS):
    time.sleep(1)
    print(f"  ...heartbeat t={i+1}s, {len(suggest_latencies)} suggest reqs so far", flush=True)
stop_flag.set()
for t in threads:
    t.join(timeout=2)

elapsed = time.time() - start
latencies_only = [lat for _, lat in suggest_latencies]
latencies_only.sort()
n = len(latencies_only)


def pct(p):
    if not latencies_only:
        return 0
    idx = min(n - 1, int(n * p))
    return latencies_only[idx]


print(f"\n=== Results over {elapsed:.1f}s ===")
print(f"Total /suggest requests: {n} ({n/elapsed:.0f} req/s)")
print(f"Total /search requests: {search_count[0]} ({search_count[0]/elapsed:.0f} req/s)")
print(f"/suggest latency  p50: {pct(0.50):.2f}ms  p95: {pct(0.95):.2f}ms  p99: {pct(0.99):.2f}ms  max: {latencies_only[-1]:.2f}ms" if latencies_only else "no data")

# Show the 10 single worst latencies with their wall-clock time, so we can
# cross-reference against the server log's flush timestamps.
worst = sorted(suggest_latencies, key=lambda x: -x[1])[:10]
print("\nWorst 10 individual /suggest latencies (wall-clock time, ms):")
for ts, lat in worst:
    print(f"  {time.strftime('%H:%M:%S', time.localtime(ts))}.{int(ts*1000)%1000:03d}  {lat:.2f}ms")
