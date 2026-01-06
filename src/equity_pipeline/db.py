import sqlite3
from pathlib import Path

DB_PATH = Path("data/pipeline_runs.db")


def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_connection() as conn:
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at REAL,
            finished_at REAL,
            status TEXT
        )
        """
        )

        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS pipeline_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            name TEXT,
            attempts INTEGER,
            duration REAL,
            status TEXT,
            error TEXT,
            FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id)
        )
        """
        )
