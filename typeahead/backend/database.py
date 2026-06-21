"""SQLite connection and queries. SQLite is the durable source of truth;
everything else (in-memory cache, frequency_memory) is a rebuildable view of this."""
import csv
import sqlite3
from pathlib import Path

DB_PATH = str(Path(__file__).parent / "typeahead.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    """Create tables if they don't exist"""
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS frequency (
                query TEXT PRIMARY KEY,
                count INTEGER DEFAULT 0
            )
        """)
        conn.commit()


def load_dataset(filepath):
    """
    Load CSV into SQLite on first run only.
    Skip if data already exists.
    CSV format: query,count
    """
    with get_connection() as conn:
        existing = conn.execute("SELECT COUNT(*) FROM frequency").fetchone()[0]
        if existing > 0:
            print(f"Dataset already loaded ({existing} queries). Skipping.")
            return

        rows = []
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                query = row["query"].lower().strip()
                count = int(row["count"])
                if len(query) >= 3:
                    rows.append((query, count))

        conn.executemany(
            "INSERT OR IGNORE INTO frequency (query, count) VALUES (?, ?)",
            rows,
        )
        conn.commit()
        print(f"Loaded {len(rows)} queries into SQLite.")


def get_all_queries():
    """Load all queries and counts from SQLite into memory"""
    with get_connection() as conn:
        rows = conn.execute("SELECT query, count FROM frequency").fetchall()
    return {row[0]: row[1] for row in rows}


def update_count(query, increment):
    """Apply a buffered delta to SQLite (called on flush)"""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO frequency (query, count) VALUES (?, ?)
            ON CONFLICT(query) DO UPDATE SET count = count + ?
            """,
            (query, increment, increment),
        )
        conn.commit()


def update_counts_batch(deltas: dict):
    """Apply many buffered deltas to SQLite in a single transaction.
    This is the actual write-reduction mechanism: N searches -> 1 transaction."""
    if not deltas:
        return
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO frequency (query, count) VALUES (?, ?)
            ON CONFLICT(query) DO UPDATE SET count = count + ?
            """,
            [(query, delta, delta) for query, delta in deltas.items()],
        )
        conn.commit()


def get_count(query):
    """Get count for a single query"""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT count FROM frequency WHERE query = ?", (query,)
        ).fetchone()
    return row[0] if row else 0
