"""DDL for the jobs + job_steps tables.

The module owns its own schema. ``register_schema(conn)`` is called
from :func:`core.db.get_db` on every connection. It ensures the current
fresh-install baseline exists; older local DBs are not upgraded in-place.

Centralizing the DDL here means writers in ``core.jobs.store`` can
reason about column shape without hunting through ``tasks_db.py`` and
keeps any future migration story for jobs/job_steps in one file.
"""

from __future__ import annotations

import sqlite3


def register_schema(conn: sqlite3.Connection) -> None:
    """Apply jobs + job_steps DDL on ``conn``. Idempotent."""
    # Generic async-job tracking. ``kind`` distinguishes handlers
    # (chat_turn, digest_run, etc); ``subject_type``/``subject_id`` link
    # the job back to whatever domain object spawned it so the UI can
    # query per-subject.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            subject_type TEXT,
            subject_id TEXT,
            payload TEXT NOT NULL DEFAULT '{}',
            result TEXT,
            error TEXT,
            timeout_seconds INTEGER,
            parent_job_id INTEGER,
            required_capability TEXT,
            priority INTEGER NOT NULL DEFAULT 0,
            workflow_id TEXT,
            worker_id TEXT,
            last_heartbeat_at TEXT,
            origin TEXT NOT NULL DEFAULT 'agent',
            retry_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            updated_at TEXT NOT NULL
        )
    """)
    # Additive guard for pre-existing local DBs: ``retry_count`` was
    # added after the baseline shipped, and jobs DBs are long-lived (the
    # "recreate after baseline changes" policy doesn't help a live queue).
    # Mirrors the ``tasks.confirmed_event_uid`` exception in core/db.py.
    _job_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
    }
    if "retry_count" not in _job_cols:
        conn.execute(
            "ALTER TABLE jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0"
        )

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_subject"
        " ON jobs (subject_type, subject_id, status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status)"
    )
    # Covers /api/jobs/classify-aggregate (kind filter + recency sort),
    # /api/jobs?kind=…&limit=…, the dashboard's `recent_ingests` slice,
    # and `tickets/workflows`. The `kind, created_at DESC` shape is
    # exactly what every "latest N of this kind" query wants.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_kind_created"
        " ON jobs (kind, created_at DESC)"
    )
    # Producer/consumer + workflow scaffolding:
    # parent_job_id    : tree linkage (a job that spawned this one)
    # required_capability : queue routing — a worker advertises a
    #                       capability ("ollama", "scraper-messenger",
    #                       "applescript", "db", ...) and only claims
    #                       jobs whose capability matches.
    # priority         : tiebreak among same-capability pending rows;
    #                    higher numbers first.
    # workflow_id      : opt-in grouping for kanban / ticket views —
    #                    "messenger-thread:<thread_id>" puts every job
    #                    that processes one Messenger thread under one
    #                    legend, even when those jobs span multiple
    #                    capabilities.
    # worker_id / last_heartbeat_at : populated by the Phase 1 worker
    #                    runtime so a server restart can tell the
    #                    difference between "in-flight on a still-alive
    #                    worker" and "interrupted, sweep me up".
    # origin records who/what kicked off the job: manual, scheduled, ingest,
    # or agent.

    # Tree walks: "all jobs spawned by N" / "all jobs in workflow W".
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_parent"
        " ON jobs (parent_job_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_workflow"
        " ON jobs (workflow_id)"
    )
    # Hot path for Phase 1 worker.claim_one(capability): scan the
    # pending queue ordered by priority desc, created_at asc, filtered
    # by capability. The composite index covers all three.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_claim"
        " ON jobs (status, required_capability, priority DESC, created_at)"
    )

    # Health-tab KPI/Signal scans (core/health). Without these the Signals
    # endpoint full-scans the jobs table four times per request, and because
    # the large ``result`` JSON is stored inline, each scan reads the whole
    # table off disk (~5 s on a 6k-row / 45 MB db). These are *covering*
    # indexes — they carry every column the health queries read, so SQLite
    # answers from the B-tree and never touches the row blobs (~9x faster,
    # 5.5 s → 0.7 s). Additive: CREATE INDEX IF NOT EXISTS applies to existing
    # local DBs without a recreate.
    #   finished     : every "completed jobs in the last N days" scan — the
    #                  capacity busy/p95/wait rollup, _kpi_jobs failure rate,
    #                  and latency_signals (filters required_capability='ollama'
    #                  within the window). Leading with finished_at lets all
    #                  three *seek* the window instead of scanning the table.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_health_finished"
        " ON jobs (finished_at, required_capability, kind, status, started_at, created_at)"
    )
    #   created      : _kpi_contention sweeps every in-window queue interval.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_health_created"
        " ON jobs (created_at, finished_at, started_at, kind, required_capability, subject_id)"
    )
    #   ingest       : _kpi_ingest per-pipeline freshness/productivity scans.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_health_ingest"
        " ON jobs (kind, subject_id, status, finished_at)"
    )

    # Per-step records for jobs that run a multi-step pipeline (digest
    # skills, graph-driven flows). Rows are inserted on step entry
    # (status=running) and updated on exit. ``llm_calls`` and
    # ``io_records`` are JSON arrays of per-call records — populated
    # automatically by core/llm and the I/O modules via
    # JobHandle.record_llm / record_io.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            started_at TEXT NOT NULL,
            finished_at TEXT,
            ms INTEGER,
            input_text TEXT,
            output_text TEXT,
            title TEXT,
            subtitle TEXT,
            llm_calls TEXT NOT NULL DEFAULT '[]',
            io_records TEXT NOT NULL DEFAULT '[]',
            error TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_job_steps_job"
        " ON job_steps (job_id, id)"
    )
    # Cross-process cancel mailbox. The cancel API route can't reach a
    # handler running in a separate worker process (runner._HANDLES is
    # process-local), so it drops a row here instead; the owning
    # worker's per-job heartbeat loop polls it (~10 s) and cancels the
    # handler task. Rows are deleted by ``mark_terminal`` so the table
    # only ever holds requests for in-flight jobs. A separate table
    # (rather than a jobs column) keeps this additive — CREATE TABLE IF
    # NOT EXISTS applies to existing local DBs without a recreate.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_cancel_requests (
            job_id INTEGER PRIMARY KEY,
            requested_at TEXT NOT NULL
        )
    """)
    # Worker-process heartbeat — independent of any running job. Every
    # worker registers itself on startup and pings ``last_alive_at`` from
    # its main poll loop, regardless of whether it's currently executing
    # a job. This is what catches the "process alive but stuck" case
    # (e.g. import-error retry loop, claim_one wedged) that job-scoped
    # ``jobs.last_heartbeat_at`` can't see, since that column only
    # advances while a handler is running.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS worker_heartbeats (
            capability TEXT NOT NULL,
            pid INTEGER NOT NULL,
            worker_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            last_alive_at TEXT NOT NULL,
            PRIMARY KEY (capability, pid)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_alive"
        " ON worker_heartbeats (capability, last_alive_at)"
    )
    # Orphaned: the object-storage input mirror (``core.jobs.remote``'s former
    # ``mirror_inputs`` / the redirect branch of ``input_location``) was
    # removed; stays in the baseline schema per the no-drop convention
    # (same pattern as ``resource_samples`` in core/db.py).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS remote_input_mirror (
            ref TEXT PRIMARY KEY,
            object_key TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime REAL NOT NULL,
            mirrored_at TEXT NOT NULL
        )
    """)
