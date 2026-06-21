"""Write-ahead log for the search-count write buffer.

The write buffer (storage.write_buffer) lives only in memory between flushes -
if the process crashes before a flush, those increments would normally be lost.
To close that gap, every search is also appended to a plain append-only log file
(cheap: no locking, no indexing, no transaction overhead - just a line appended).

On a clean flush, the log is cleared (those deltas are now safely in SQLite).
On startup, if the log is non-empty, it means the process crashed after logging
a search but before the next flush - we replay the log to recover those counts
before building anything else.
"""
import threading
from collections import defaultdict
from pathlib import Path

WAL_PATH = Path(__file__).parent / "wal.log"
_wal_lock = threading.Lock()


def append_to_wal(query):
    with _wal_lock:
        with open(WAL_PATH, "a", encoding="utf-8") as f:
            f.write(query + "\n")


def clear_wal():
    with _wal_lock:
        if WAL_PATH.exists():
            WAL_PATH.unlink()


def replay_wal():
    """Returns {query: count} aggregated from any leftover WAL entries.
    Empty dict if there's nothing to recover."""
    if not WAL_PATH.exists():
        return {}

    recovered = defaultdict(int)
    with open(WAL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            query = line.strip()
            if query:
                recovered[query] += 1
    return dict(recovered)
