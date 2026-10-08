"""Database access for the job tables, through the configured hooks.

Every job module calls :func:`get_db` / :func:`now_iso` here; each call goes to
``config.settings`` so a host can hand over its own connection and clock
(``configure(get_db=..., now_iso=...)``). Without a host, :func:`connect` opens
the package's own SQLite file — ``ORBIT_JOBS_DB``, default
``~/.local/share/orbit-jobs/jobs.db`` — one connection per thread, in WAL mode
with a busy timeout, and creates the job tables on first open.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from orbit_jobs import config

__all__ = ["get_db", "now_iso", "connect", "db_path", "utc_now_iso"]

# Same wait as the automations host's connections: a contended write queues
# behind the WAL writer instead of failing with "database is locked".
BUSY_TIMEOUT_MS = 30_000

_local = threading.local()


def get_db() -> sqlite3.Connection:
    """The configured connection (use as ``with get_db() as conn``)."""
    return config.settings.get_db()


def now_iso() -> str:
    """The configured write-side clock, as an ISO-8601 string."""
    return config.settings.now_iso()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db_path() -> Path:
    return Path(os.environ.get("ORBIT_JOBS_DB")
                or Path.home() / ".local/share/orbit-jobs/jobs.db")


def connect() -> sqlite3.Connection:
    """This thread's connection to :func:`db_path`, opened (with the job
    tables) on first use and reopened when the path changes."""
    path = db_path()
    conn = getattr(_local, "conn", None)
    if conn is not None and getattr(_local, "path", None) == path:
        return conn
    if conn is not None:
        conn.close()
    from orbit_jobs.schema import register_schema

    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    register_schema(conn)
    conn.commit()
    _local.conn, _local.path = conn, path
    return conn
