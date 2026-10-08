"""SQLite-backed persistence for the jobs table."""

from __future__ import annotations

import json
import socket
from datetime import datetime, timedelta, timezone
from typing import Any

from core.compute_allocation import host_name
from core.db import get_db, now_iso
from core.jobs import wake

ACTIVE_STATUSES = ("pending", "running")
TERMINAL_STATUSES = ("succeeded", "failed", "cancelled")
STEP_STATUSES = ("running", "ok", "failed", "cancelled")

__all__ = [
    "ACTIVE_STATUSES",
    "TERMINAL_STATUSES",
    "STEP_STATUSES",
    "insert",
    "get_workflow_id",
    "mark_running",
    "claim_one",
    "heartbeat",
    "clear_superseded_jobs",
    "recover_orphans",
    "worker_heartbeat",
    "worker_unregister",
    "list_live_workers",
    "compute_hosts",
    "pending_count_by_capability",
    "request_cancel",
    "cancel_requested",
    "mark_terminal",
    "get",
    "delete_job",
    "list_jobs",
    "has_active_for_subject",
    "insert_step",
    "finish_step",
    "list_steps",
    "get_step",
    "cancel_orphans",
]


def _row_to_dict(row) -> dict[str, Any]:
    d = dict(row)
    for key in ("payload", "result"):
        raw = d.get(key)
        if raw:
            try:
                d[key] = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                pass
        elif raw is None and key == "payload":
            d[key] = {}
    return d


def _step_row_to_dict(row) -> dict[str, Any]:
    d = dict(row)
    for key in ("llm_calls", "io_records"):
        raw = d.get(key)
        if raw:
            try:
                d[key] = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                d[key] = []
        else:
            d[key] = []
    return d


def insert(
    kind: str,
    payload: dict[str, Any],
    subject_type: str | None = None,
    subject_id: str | None = None,
    timeout_seconds: int | None = None,
    *,
    parent_job_id: int | None = None,
    required_capability: str | None = None,
    priority: int = 0,
    workflow_id: str | None = None,
    origin: str = "agent",
    retry_count: int = 0,
) -> int:
    """Persist a pending job. Phase 0 fields (parent / capability /
    priority / workflow_id) are optional and default to NULL/0 for
    backwards compatibility — existing call sites that don't yet care
    about producer/consumer routing keep working.

    ``origin`` records provenance (manual / scheduled / ingest /
    agent). Defaults to ``"agent"`` so a missed declaration shows
    up clearly in the dashboard rather than blending in with real
    user clicks. Resolution lives in ``core.jobs.runner.spawn``.
    """
    ts = now_iso()
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO jobs
              (kind, status, subject_type, subject_id, payload,
               timeout_seconds, parent_job_id, required_capability,
               priority, workflow_id, origin, retry_count,
               created_at, updated_at)
            VALUES (?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                kind, subject_type, subject_id, json.dumps(payload or {}),
                timeout_seconds, parent_job_id, required_capability,
                priority, workflow_id, origin, retry_count, ts, ts,
            ),
        )
        job_id = int(cur.lastrowid)
    # After the connection block so the row is committed before workers
    # react (inside an outer transaction they find it on their next poll).
    wake.notify(required_capability)
    return job_id


def get_workflow_id(job_id: int) -> str | None:
    """Return the workflow_id for a job, or None if unset/missing."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT workflow_id FROM jobs WHERE id = ?", (job_id,),
        ).fetchone()
    return row["workflow_id"] if row else None


def mark_running(job_id: int) -> None:
    ts = now_iso()
    with get_db() as conn:
        conn.execute(
            "UPDATE jobs SET status = 'running', started_at = ?, updated_at = ?"
            " WHERE id = ?",
            (ts, ts, job_id),
        )


# ── Phase 1: worker claim / heartbeat / orphan recovery ───────────────

def claim_one(
    capabilities: list[str],
    worker_id: str,
    *,
    job_id: int | None = None,
    code_version: str | None = None,
    allow_unpinned: bool = True,
    kinds: list[str] | None = None,
) -> dict | None:
    """Atomically claim the next pending job matching one of ``capabilities``.

    Highest ``priority`` first, then oldest ``created_at``. Marks the row
    ``running``, sets ``worker_id``, ``started_at``, ``last_heartbeat_at``,
    ``updated_at``, and returns the row as a dict. Returns ``None`` when
    no pending job matches. ``job_id`` restricts a dedicated caller to its
    own queued job without claiming unrelated work on the same capability.
    ``code_version`` (a remote worker's commit) restricts the claim to jobs
    whose payload pins that commit (``payload.code_version``), plus unpinned
    jobs when ``allow_unpinned``. ``kinds`` restricts the claim to those
    kinds (an empty list claims nothing).

    Atomicity: SQLite serializes writes, and the inner SELECT runs inside
    the same UPDATE statement, so two workers polling at the same time
    cannot both claim the same row. ``UPDATE … RETURNING`` (SQLite 3.35+)
    avoids the SELECT-then-UPDATE race entirely.

    Mirrors what an ``rq`` BLPOP would do — same shape so a Redis swap
    is implementation-only on the worker side.
    """
    if not capabilities or kinds == []:
        return None
    placeholders = ",".join("?" for _ in capabilities)
    kind_filter = "" if kinds is None else f"AND kind IN ({','.join('?' for _ in kinds)})"
    ts = now_iso()
    with get_db() as conn:
        cur = conn.execute(
            f"""
            UPDATE jobs
               SET status = 'running',
                   worker_id = ?,
                   started_at = COALESCE(started_at, ?),
                   last_heartbeat_at = ?,
                   updated_at = ?
             WHERE id = (
                 SELECT id FROM jobs
                  WHERE status = 'pending'
                    AND required_capability IN ({placeholders})
                    {kind_filter}
                    AND (? IS NULL OR id = ?)
                    AND (? IS NULL
                         OR json_extract(payload, '$.code_version') = ?
                         OR (json_extract(payload, '$.code_version') IS NULL AND ?))
                  ORDER BY priority DESC, created_at ASC
                  LIMIT 1
             )
            RETURNING *
            """,
            (worker_id, ts, ts, ts, *capabilities, *(kinds or ()), job_id, job_id,
             code_version, code_version, int(allow_unpinned)),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None


def heartbeat(job_id: int, *, worker_id: str | None = None) -> bool:
    """Refresh ``last_heartbeat_at`` for a still-running job.

    Workers call this every few seconds while running a long handler.
    The Phase 1 startup sweep uses this column to distinguish "alive
    worker, leave alone" from "interrupted, mark cancelled".

    With ``worker_id``, only that worker's claim is refreshed. Returns
    False when the row is no longer running (or no longer that worker's).
    """
    ts = now_iso()
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE jobs SET last_heartbeat_at = ?, updated_at = ?"
            " WHERE id = ? AND status = 'running'"
            " AND (? IS NULL OR worker_id = ?)",
            (ts, ts, job_id, worker_id, worker_id),
        )
        return bool(cur.rowcount)


_STUCK_TIMEOUT_MULTIPLIER = 2
# Absolute wall for any running job — catches rows that have no
# timeout_seconds set (legacy or oversight). 6h is well past anything
# we expect a single capability-routed job to take.
_STUCK_ABSOLUTE_SECONDS = 6 * 60 * 60
# A remote job whose worker vanished (a spot machine reclaimed, a node cut off from the
# hub) is infrastructure loss, not a failure: it is queued again, without spending its
# failure retries, up to this many times in a row (a job that keeps killing its worker
# stops). Remote capabilities are ``core.jobs.remote.REMOTE_CAPABILITY_PREFIX``.
_ORPHAN_REQUEUES = 3
_REMOTE_PREFIX = "remote-"
# A remote worker heartbeats every 10 s, but over the network: the hub restarting or a
# blip on the tailnet can silence every remote job for a minute without any machine dying
# (2026-10-06: a lab restart cancelled 16 healthy PC shards). Remote jobs get this long.
_REMOTE_STALE_SECONDS = 10 * 60
_DEAD_REASON = "worker died (no heartbeat)"

# Pending-row TTL derived from the job's own timeout: a healthy claim
# takes ~1 s, so a row still pending after 10x its timeout was routed to
# a capability whose worker never came up. The 1 h floor protects
# short-timeout kinds whose worker is only briefly down. Rows with no
# timeout_seconds fall back to the flat pending_max_age_seconds cutoff.
_PENDING_TTL_TIMEOUT_MULTIPLIER = 10
_PENDING_TTL_FLOOR_SECONDS = 60 * 60


def _expire_stale_pending_jobs(*, ts: str, pending_cutoff: str) -> int:
    """Phase 1 of :func:`clear_superseded_jobs`: flip long-pending rows to
    ``cancelled``. Its own commit so this durable effect survives even if
    a later phase raises."""
    with get_db() as conn:
        cur = conn.execute(
            """
            UPDATE jobs
               SET status = 'cancelled',
                   error = 'expired (pending, never claimed by a worker)',
                   finished_at = ?,
                   updated_at = ?
             WHERE status = 'pending'
               AND (
                 (timeout_seconds IS NOT NULL
                  AND (julianday('now') - julianday(created_at)) * 86400
                      > MAX(?, ? * timeout_seconds))
                 OR
                 (timeout_seconds IS NULL AND created_at < ?)
               )
            """,
            (
                ts, ts,
                _PENDING_TTL_FLOOR_SECONDS,
                _PENDING_TTL_TIMEOUT_MULTIPLIER,
                pending_cutoff,
            ),
        )
        return cur.rowcount or 0


def _superseded_by_kind_counts(
    *, superseded_predicate: str, zombie_predicate: str,
    grace_cutoff: str, age_cutoff: str,
) -> dict[str, int]:
    """Phase 2: read-only tally used only for the return payload."""
    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT kind, COUNT(*) AS n
              FROM jobs
             WHERE {superseded_predicate} OR {zombie_predicate}
             GROUP BY kind
            """,
            {"grace_cutoff": grace_cutoff, "age_cutoff": age_cutoff},
        ).fetchall()
    return {r["kind"]: int(r["n"]) for r in rows}


def _delete_superseded_steps(
    *, superseded_predicate: str, zombie_predicate: str,
    grace_cutoff: str, age_cutoff: str,
) -> None:
    """Phase 3: delete the child ``job_steps`` rows first (FK-shaped
    cleanup) so a crash between phases never leaves ``jobs`` deleted
    with orphaned steps. Own commit."""
    with get_db() as conn:
        conn.execute(
            f"""
            DELETE FROM job_steps
             WHERE job_id IN (
                SELECT id FROM jobs
                 WHERE {superseded_predicate} OR {zombie_predicate}
             )
            """,
            {"grace_cutoff": grace_cutoff, "age_cutoff": age_cutoff},
        )


def _delete_superseded_jobs(
    *, superseded_predicate: str, zombie_predicate: str,
    grace_cutoff: str, age_cutoff: str,
) -> int:
    """Phase 4: delete the superseded/zombie ``jobs`` rows. Own commit."""
    with get_db() as conn:
        cur = conn.execute(
            f"""
            DELETE FROM jobs
             WHERE {superseded_predicate} OR {zombie_predicate}
            """,
            {"grace_cutoff": grace_cutoff, "age_cutoff": age_cutoff},
        )
        return cur.rowcount or 0


def _delete_orphaned_job_steps() -> None:
    """Phase 5: sweep any ``job_steps`` rows left behind by an earlier
    (pre-phase-4-cleanup) sweep whose ``jobs`` DELETE leaked its steps.
    Own commit, independently idempotent of every other phase."""
    with get_db() as conn:
        conn.execute(
            "DELETE FROM job_steps"
            " WHERE job_id NOT IN (SELECT id FROM jobs)"
        )


def clear_superseded_jobs(
    *,
    grace_seconds: int = 24 * 3600,
    max_age_seconds: int = 14 * 24 * 3600,
    pending_max_age_seconds: int = 2 * 24 * 3600,
) -> dict:
    """Delete failed/cancelled jobs that are superseded or just very old.

    Two clauses (OR'd):

    1. **Superseded** — a later ``(kind, subject_id)`` succeeded after
       this row (by id). The newer success is the live state; the old
       failure is noise. Requires the failure to be at least
       ``grace_seconds`` old so recent failures stay visible long
       enough to investigate.

    2. **Zombie** — older than ``max_age_seconds`` regardless of
       supersession. Catches orphan failures from retired skills
       (e.g. *-classify jobs from before per-thread classify_thread
       replaced the batch path) whose subject will never see another
       run. Without this clause those rows sit forever.

    ``cancelled`` is included alongside ``failed``: in practice the
    overwhelming majority of cancelled rows are infrastructure
    interruptions ("interrupted by server restart", "worker died",
    "stuck past timeout"), not deliberate user actions — and either
    way, once a newer same-subject run succeeded (or 14 days passed)
    the row is noise in the Tickets view. Rows with no ``subject_id``
    are eligible for the zombie clause only (no "same subject" to
    compare against).

    Before the delete, a third pass **expires stale pending rows**: a
    job still pending long past a normal claim (~1 s) was routed to a
    capability whose worker never came up. Left alone it sits forever
    AND blocks re-spawns for the same subject via
    ``has_active_for_subject``. Rows with a ``timeout_seconds`` expire
    at ``max(1h, 10x timeout)`` — timeout-proportionate, so a
    5-minute-timeout classify job stops blocking its subject within the
    hour instead of after ``pending_max_age_seconds`` (2 days). Rows
    without a timeout keep the flat ``pending_max_age_seconds`` cutoff.
    Expiry flips
    it to ``cancelled`` (not delete), so it stays visible in the
    Tickets view until the superseded/zombie clauses reap it — in the
    same sweep if a later same-subject success already exists.

    Returns ``{"swept": int, "by_kind": {kind: int},
    "expired_pending": int, ...}``.

    Runs as five independently-committed phases (not one long
    transaction) — each phase is idempotent on its own, and a failure
    partway through leaves every already-run phase's effect durable
    rather than holding the WAL writer lock for the whole sweep and
    rolling everything back.
    """
    ts = now_iso()
    pending_cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=pending_max_age_seconds)
    ).isoformat()
    grace_cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=grace_seconds)
    ).isoformat()
    age_cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)
    ).isoformat()
    superseded_predicate = """(
        status IN ('failed', 'cancelled')
        AND subject_id IS NOT NULL
        AND created_at < :grace_cutoff
        AND EXISTS (
              SELECT 1 FROM jobs s
               WHERE s.kind = jobs.kind
                 AND s.subject_id = jobs.subject_id
                 AND s.status = 'succeeded'
                 AND s.id > jobs.id
        )
    )"""
    zombie_predicate = (
        "(status IN ('failed', 'cancelled') AND created_at < :age_cutoff)"
    )
    predicate_kwargs = dict(
        superseded_predicate=superseded_predicate,
        zombie_predicate=zombie_predicate,
        grace_cutoff=grace_cutoff, age_cutoff=age_cutoff,
    )

    # Phase 1 — same julianday-age pattern as the stuck-job sweep in
    # recover_orphans: timeout-bearing rows expire at
    # max(floor, multiplier * timeout); timeout-less rows use the flat
    # pending_max_age_seconds cutoff.
    expired_pending = _expire_stale_pending_jobs(ts=ts, pending_cutoff=pending_cutoff)
    # Phase 2 — read-only tally for the return payload.
    by_kind = _superseded_by_kind_counts(**predicate_kwargs)
    # Phase 3 — child rows first so a crash before phase 4 never leaves
    # a deleted job with orphaned steps.
    _delete_superseded_steps(**predicate_kwargs)
    # Phase 4 — the superseded/zombie job rows themselves.
    swept = _delete_superseded_jobs(**predicate_kwargs)
    # Phase 5 — orphaned steps from sweeps that pre-date the phase-3
    # cleanup above (an older jobs DELETE used to leak its job_steps rows).
    _delete_orphaned_job_steps()

    return {
        "swept": swept,
        "by_kind": by_kind,
        "expired_pending": expired_pending,
        "grace_seconds": grace_seconds,
        "max_age_seconds": max_age_seconds,
        "pending_max_age_seconds": pending_max_age_seconds,
    }


def recover_orphans(stale_after_seconds: int = 60,
                    remote_stale_after_seconds: int = _REMOTE_STALE_SECONDS) -> int:
    """Sweep `running` rows whose worker is dead or stuck.

    Two failure modes are handled:

    1. **Dead worker** — ``last_heartbeat_at`` is stale (or NULL). The
       worker process is gone; the row is an orphan claim.

    2. **Stuck worker** — heartbeats keep flowing but the job has been
       in ``running`` past ``2 * timeout_seconds`` (or 6h absolute if no
       timeout is set). This is the "alive thread, dead socket" case
       we hit when Ollama got restarted while a classify worker was
       mid-call: the heartbeat thread kept ticking from a separate
       thread while the blocked HTTP read pinned the job for days.
       Liveness ≠ progress, so we sweep on age too.

    Both branches flip the row to ``cancelled`` so a fresh worker can
    pick up retries. A dead *remote* job (``remote-*``) is also queued again
    (``_requeue_orphan``, at most ``_ORPHAN_REQUEUES`` times in a row): its machine
    went away, the work itself didn't fail. The stuck worker's blocked thread is *not* killed
    here — recovering memory/socket state requires restarting the
    worker process out-of-band (or letting it crash on next request).

    Returns the number of rows swept.

    NOTE: jobs without ``required_capability`` (the legacy in-process
    path) still go through ``cancel_orphans()`` since they're tied to
    the FastAPI event loop, not a separate worker.
    """
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)
    ).isoformat()
    remote_cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(seconds=remote_stale_after_seconds)
    ).isoformat()
    absolute_cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(seconds=_STUCK_ABSOLUTE_SECONDS)
    ).isoformat()
    ts = now_iso()
    dead_reason = _DEAD_REASON
    stuck_reason = "stuck (past timeout window, alive but not progressing)"
    with get_db() as conn:
        dead_remote = [dict(r) for r in conn.execute(
            """
            SELECT * FROM jobs
             WHERE status = 'running'
               AND required_capability LIKE ? || '%'
               AND (last_heartbeat_at IS NULL OR last_heartbeat_at < ?)
            """,
            (_REMOTE_PREFIX, remote_cutoff_iso),
        ).fetchall()]
        # Local workers: silent past ``stale_after_seconds``; remote ones: past
        # ``remote_stale_after_seconds``.
        cur_dead = conn.execute(
            """
            UPDATE jobs
               SET status = 'cancelled',
                   error = ?,
                   finished_at = COALESCE(finished_at, ?),
                   updated_at = ?
             WHERE status = 'running'
               AND required_capability IS NOT NULL
               AND (last_heartbeat_at IS NULL OR last_heartbeat_at <
                    CASE WHEN required_capability LIKE ? || '%' THEN ? ELSE ? END)
            """,
            (dead_reason, ts, ts, _REMOTE_PREFIX, remote_cutoff_iso, cutoff_iso),
        )
        # SQLite-side age check using julianday math against `now`. We
        # use 2x the configured timeout when set, else a 6h absolute
        # wall. ``started_at`` is the source of truth here — not
        # heartbeat — because the bug is "heartbeats are fine, work
        # isn't."
        cur_stuck = conn.execute(
            """
            UPDATE jobs
               SET status = 'cancelled',
                   error = ?,
                   finished_at = COALESCE(finished_at, ?),
                   updated_at = ?
             WHERE status = 'running'
               AND required_capability IS NOT NULL
               AND started_at IS NOT NULL
               AND (
                 (timeout_seconds IS NOT NULL
                  AND (julianday('now') - julianday(started_at)) * 86400
                      > ? * timeout_seconds)
                 OR
                 (timeout_seconds IS NULL AND started_at < ?)
               )
            """,
            (stuck_reason, ts, ts, _STUCK_TIMEOUT_MULTIPLIER, absolute_cutoff_iso),
        )
        conn.execute(
            """
            UPDATE job_steps SET status = 'cancelled',
                   finished_at = COALESCE(finished_at, ?),
                   error = COALESCE(error, ?)
             WHERE status = 'running'
               AND job_id IN (
                 SELECT id FROM jobs WHERE status = 'cancelled'
                   AND error IN (?, ?)
               )
            """,
            (ts, dead_reason, dead_reason, stuck_reason),
        )
        swept = (cur_dead.rowcount or 0) + (cur_stuck.rowcount or 0)
    for job in dead_remote:
        _requeue_orphan(job)
    return swept


def _requeue_orphan(job: dict) -> int | None:
    """Queue a fresh copy of a remote job whose worker vanished; None past the cap, or
    when someone had asked to cancel the job (its worker died before seeing that).

    The copy keeps the job's ``retry_count`` (its failure budget is untouched) and
    points ``parent_job_id`` at the orphan, so the lineage shows the requeues."""
    if cancel_requested(int(job["id"])):
        return None
    lost, parent = 1, job.get("parent_job_id")
    with get_db() as conn:
        while parent is not None and lost <= _ORPHAN_REQUEUES:
            row = conn.execute("SELECT parent_job_id, error FROM jobs WHERE id = ?",
                               (parent,)).fetchone()
            if row is None or row["error"] != _DEAD_REASON:
                break
            lost, parent = lost + 1, row["parent_job_id"]
    if lost > _ORPHAN_REQUEUES:
        return None
    return insert(
        job["kind"], json.loads(job["payload"] or "{}"),
        subject_type=job.get("subject_type"), subject_id=job.get("subject_id"),
        timeout_seconds=job.get("timeout_seconds"), parent_job_id=int(job["id"]),
        required_capability=job.get("required_capability"),
        priority=int(job.get("priority") or 0), workflow_id=job.get("workflow_id"),
        origin=job.get("origin") or "agent", retry_count=int(job.get("retry_count") or 0),
    )


# ── Worker-process heartbeats (distinct from per-job heartbeats) ─────

def worker_heartbeat(worker_id: str, capability: str, pid: int) -> None:
    """Upsert this worker's liveness ping. Called by the worker's main
    poll loop every iteration, regardless of whether it's currently
    running a job. This is the signal a dead/stuck worker can't produce
    even when its OS process is technically alive (e.g. wedged in a
    failing-import retry loop).

    Keyed on ``(capability, pid)`` so a code-drift exec into the same
    PID refreshes the row rather than leaking a new one. The
    ``worker_id`` column is informational only.
    """
    ts = now_iso()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO worker_heartbeats
                (capability, pid, worker_id, started_at, last_alive_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(capability, pid) DO UPDATE
                SET last_alive_at = excluded.last_alive_at,
                    worker_id = excluded.worker_id
            """,
            (capability, pid, worker_id, ts, ts),
        )


def worker_unregister(worker_id: str) -> None:
    """Best-effort cleanup on graceful shutdown."""
    with get_db() as conn:
        conn.execute(
            "DELETE FROM worker_heartbeats WHERE worker_id = ?",
            (worker_id,),
        )


def list_live_workers(stale_after_seconds: int = 60) -> list[dict]:
    """Return worker_heartbeats rows whose ``last_alive_at`` is within
    the freshness window. The caller can compare against the
    capability-keyed ``_managed`` dict to find capabilities where every
    managed PID has gone dark."""
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)
    ).isoformat()
    with get_db() as conn:
        rows = conn.execute(
            "SELECT worker_id, capability, pid, started_at, last_alive_at"
            " FROM worker_heartbeats WHERE last_alive_at >= ?",
            (cutoff_iso,),
        ).fetchall()
    return [dict(r) for r in rows]


def pending_count_by_capability() -> dict[str, int]:
    """Per-capability pending-job depth. Used by the sweep to decide
    whether a dead worker's capability is actually causing harm right
    now (queue == 0 means no respawn pressure)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT required_capability, COUNT(*) AS n"
            " FROM jobs WHERE status = 'pending'"
            "   AND required_capability IS NOT NULL"
            " GROUP BY required_capability",
        ).fetchall()
    return {r["required_capability"]: r["n"] for r in rows}


def request_cancel(job_id: int) -> str | None:
    """Request cancellation of a job from outside its owning process.

    Two cases:

    - **pending** — no worker has claimed it yet, so flip the row to
      ``cancelled`` directly. Returns ``"cancelled"``; the caller is
      responsible for emitting the ``job.finished`` event.
    - **running** — the handler lives in some worker process we can't
      reach, so drop a row in ``job_cancel_requests``. The worker's
      per-job heartbeat loop polls it every ~10 s and cancels the
      handler task. Returns ``"requested"``.

    Returns ``None`` when the job is missing or already terminal.
    """
    ts = now_iso()
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE jobs SET status = 'cancelled', error = 'cancelled',"
            " finished_at = ?, updated_at = ?"
            " WHERE id = ? AND status = 'pending'",
            (ts, ts, job_id),
        )
        if cur.rowcount:
            return "cancelled"
        row = conn.execute(
            "SELECT status FROM jobs WHERE id = ?", (job_id,),
        ).fetchone()
        if row is None or row["status"] != "running":
            return None
        conn.execute(
            "INSERT OR IGNORE INTO job_cancel_requests (job_id, requested_at)"
            " VALUES (?, ?)",
            (job_id, ts),
        )
        return "requested"


def cancel_requested(job_id: int) -> bool:
    """True when a cross-process cancel request is pending for this job.
    Polled by the worker's per-job heartbeat loop."""
    with get_db() as conn:
        return conn.execute(
            "SELECT 1 FROM job_cancel_requests WHERE job_id = ?", (job_id,),
        ).fetchone() is not None


def mark_terminal(
    job_id: int,
    status: str,
    *,
    result: dict | None = None,
    error: str | None = None,
    worker_id: str | None = None,
) -> bool:
    """Record a job's final status. With ``worker_id``, only a row still
    running under that worker's claim is finished — a lease the orphan
    sweep already released is left alone. Returns whether a row changed."""
    assert status in TERMINAL_STATUSES
    ts = now_iso()
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE jobs SET status = ?, result = ?, error = ?,"
            " finished_at = ?, updated_at = ? WHERE id = ?"
            " AND (? IS NULL OR (worker_id = ? AND status = 'running'))",
            (
                status,
                json.dumps(result) if result is not None else None,
                error,
                ts,
                ts,
                job_id,
                worker_id,
                worker_id,
            ),
        )
        if not cur.rowcount:
            return False
        # Consume any outstanding cross-process cancel request — the job
        # is terminal either way, and a retry gets a fresh job id.
        conn.execute(
            "DELETE FROM job_cancel_requests WHERE job_id = ?", (job_id,),
        )
        return True


def get(job_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _row_to_dict(row) if row else None


def delete_job(job_id: int) -> str:
    """Hard-delete a terminal job and its steps.

    Returns one of:
      - ``"deleted"``: job row and steps were removed.
      - ``"missing"``: no such job.
      - ``"active"``: job is pending/running and must be cancelled first.
    """
    with get_db() as conn:
        row = conn.execute(
            "SELECT status FROM jobs WHERE id = ?", (job_id,),
        ).fetchone()
        if row is None:
            return "missing"
        if row["status"] in ACTIVE_STATUSES:
            return "active"
        conn.execute("DELETE FROM job_steps WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    return "deleted"


def list_jobs(
    *,
    subject_type: str | None = None,
    subject_id: str | None = None,
    status: str | None = None,
    active: bool = False,
    kind: str | None = None,
    limit: int = 100,
) -> list[dict]:
    sql = "SELECT * FROM jobs WHERE 1=1"
    params: list[Any] = []
    if subject_type is not None:
        sql += " AND subject_type = ?"
        params.append(subject_type)
    if subject_id is not None:
        sql += " AND subject_id = ?"
        params.append(subject_id)
    if kind is not None:
        sql += " AND kind = ?"
        params.append(kind)
    if status is not None:
        sql += " AND status = ?"
        params.append(status)
    elif active:
        sql += " AND status IN ('pending', 'running')"
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def has_active_for_subject(
    subject_type: str, subject_id: str, kind: str | None = None
) -> bool:
    sql = (
        "SELECT 1 FROM jobs WHERE subject_type = ? AND subject_id = ?"
        " AND status IN ('pending', 'running')"
    )
    params: list[Any] = [subject_type, subject_id]
    if kind is not None:
        sql += " AND kind = ?"
        params.append(kind)
    sql += " LIMIT 1"
    with get_db() as conn:
        return conn.execute(sql, params).fetchone() is not None


def annotate_step(step_id: int, *, title: str | None = None, subtitle: str | None = None) -> None:
    """Live-update a running step's presentational fields so pollers (job
    drawer, arena cards) see sub-step progress before the step finishes."""
    sets, params = [], []
    if title is not None:
        sets.append("title = ?")
        params.append(title)
    if subtitle is not None:
        sets.append("subtitle = ?")
        params.append(subtitle)
    if not sets:
        return
    params.append(step_id)
    with get_db() as conn:
        conn.execute(f"UPDATE job_steps SET {', '.join(sets)} WHERE id = ?", params)


def insert_step(
    job_id: int,
    name: str,
    *,
    title: str | None = None,
    subtitle: str | None = None,
    input_text: str | None = None,
) -> int:
    """Insert a step row in 'running' state. Returns the new step id."""
    ts = now_iso()
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO job_steps
              (job_id, name, status, started_at, input_text, title, subtitle)
            VALUES (?, ?, 'running', ?, ?, ?, ?)
            """,
            (job_id, name, ts, input_text, title, subtitle),
        )
        return int(cur.lastrowid)


def finish_step(
    step_id: int,
    status: str,
    *,
    ms: int,
    output_text: str | None = None,
    title: str | None = None,
    subtitle: str | None = None,
    llm_calls: list[dict] | None = None,
    io_records: list[dict] | None = None,
    error: str | None = None,
) -> None:
    """Update a step row to terminal status (ok / failed / cancelled)."""
    assert status in ("ok", "failed", "cancelled"), status
    ts = now_iso()
    with get_db() as conn:
        conn.execute(
            """
            UPDATE job_steps SET
              status = ?, finished_at = ?, ms = ?,
              output_text = COALESCE(?, output_text),
              title = COALESCE(?, title),
              subtitle = COALESCE(?, subtitle),
              llm_calls = ?, io_records = ?,
              error = ?
            WHERE id = ?
            """,
            (
                status, ts, ms,
                output_text, title, subtitle,
                json.dumps(llm_calls or []),
                json.dumps(io_records or []),
                error,
                step_id,
            ),
        )


def list_steps(job_id: int) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM job_steps WHERE job_id = ? ORDER BY id ASC",
            (job_id,),
        ).fetchall()
    return [_step_row_to_dict(r) for r in rows]


def get_step(step_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM job_steps WHERE id = ?", (step_id,),
        ).fetchone()
    return _step_row_to_dict(row) if row else None




def cancel_orphans() -> int:
    """Mark in-process orphans as ``cancelled`` on FastAPI startup.

    Sweeps only rows with ``required_capability IS NULL`` — those run
    on the FastAPI event loop, which just died with the previous
    process, so their work cannot continue. Capability-routed rows
    belong to separate worker processes that might still be alive;
    those are handled by :func:`recover_orphans`, which uses heartbeat
    freshness to distinguish "alive worker" from "dead worker".

    We use ``cancelled`` (not ``failed``) to communicate "interrupted
    by infrastructure, not by a bug in the work itself" so the UI
    doesn't look alarming after a normal restart. Step rows still in
    ``running`` get flipped to ``cancelled`` for the same reason. The
    full job + step history (input, output, prompts, IO records)
    remains intact and queryable via /api/jobs.
    """
    ts = now_iso()
    reason = "interrupted by server restart"
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE jobs SET status = 'cancelled', error = ?,"
            " finished_at = COALESCE(finished_at, ?), updated_at = ?"
            " WHERE status IN ('pending', 'running')"
            "   AND required_capability IS NULL",
            (reason, ts, ts),
        )
        conn.execute(
            "UPDATE job_steps SET status = 'cancelled',"
            " finished_at = COALESCE(finished_at, ?),"
            " error = COALESCE(error, ?)"
            " WHERE status = 'running'"
            "   AND job_id IN ("
            "     SELECT id FROM jobs WHERE status = 'cancelled' AND error = ?"
            "   )",
            (ts, reason, reason),
        )
        return cur.rowcount or 0


def compute_hosts() -> dict[str, int]:
    """Known machines and current self-play shards, using worker-id host prefixes."""
    hosts = {host_name(socket.gethostname()): 0, "VictorHyte": 0}
    with get_db() as conn:
        rows = conn.execute("""SELECT worker_id, 0 AS running FROM worker_heartbeats
            WHERE capability LIKE 'remote-%'
            UNION ALL SELECT worker_id, CASE WHEN status='running' THEN 1 ELSE 0 END
            FROM jobs WHERE kind='selfplay_shard' AND worker_id IS NOT NULL""")
        for row in rows:
            if row["worker_id"].count(":") < 3:
                continue  # local worker IDs start with capability, not hostname
            try:
                host = host_name(row["worker_id"].split(":")[0])
            except ValueError:
                continue
            key = next((h for h in hosts if h.lower() == host.lower()), host)
            hosts[key] = hosts.get(key, 0) + row["running"]
    return hosts
