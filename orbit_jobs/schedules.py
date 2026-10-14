"""Declarative schedules: what an app's tick runs and when, with failure backoff and
one record per run (which doubles as heartbeat data).

An :class:`Entry` is ``(name, when, action)``. ``when`` is one of:

- :class:`Daily` — once per day at or after a time (local by default, UTC on
  request), optionally on one weekday only (a weekly entry);
- :class:`Every` — once per N-minute bucket of the clock;
- :data:`TICK` — every tick (the action keeps its own gate, if any).

``action`` is a callable ``action(now)`` (sync or async, run on the event loop) or
a :class:`Job`, which spawns an orbit_jobs job. A job-backed run takes its outcome
from the job (it stays ``running`` until the job ends), and an entry whose job is
still pending or running is not started again.

The app's tick calls ``await run_due(entries)``. Each entry runs in its own ``try``:
one failure never skips the rest. A timed entry's period is claimed (in the job
database, before the action runs), so a restart mid-run doesn't repeat it at once;
the interrupted run is closed as ``skipped`` and the entry runs again next period.
An action returns :data:`RETRY` to leave its period unclaimed (try again next tick,
nothing recorded) or :data:`SKIP` to record a ``skipped`` run.

A failed run releases its period and backs off (:class:`Backoff`): the next try
waits ``first_minutes`` doubling per consecutive failure up to ``max_minutes``,
and never less than the entry's normal interval (itself capped at
``max_minutes``, so a monthly entry still retries daily). A success resets it.
TICK entries don't back off unless given a ``backoff``.

:func:`health` reports per-entry heartbeat data (``docs/plans/HEARTBEATS.md`` in
the automations repository). Needs only the standard library and the job tables
(``orbit_jobs.schema``); no worker is required unless a :class:`Job` entry's kind
is capability-routed.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from orbit_jobs.db import get_db, now_iso
from orbit_jobs.runner import JobConflictError, spawn
from orbit_jobs.store import TERMINAL_STATUSES, has_active_for_subject
from orbit_jobs.store import get as get_job

__all__ = [
    "Daily", "Every", "TICK", "Entry", "Job", "Backoff", "DEFAULT_BACKOFF", "NO_BACKOFF",
    "RETRY", "SKIP", "MONDAY", "SATURDAY", "SUNDAY", "KEEP_DAYS", "ERROR_CHARS",
    "run_due", "health", "prune", "import_periods",
]

logger = logging.getLogger(__name__)

#: An action returns this to leave its period unclaimed (retry next tick, no record).
RETRY = object()
#: An action returns this to record a ``skipped`` run (its period stays claimed).
SKIP = object()

MONDAY, SATURDAY, SUNDAY = 0, 5, 6
KEEP_DAYS = 7
ERROR_CHARS = 300
_PRUNE_EVERY = timedelta(hours=1)
_last_prune: datetime | None = None


@dataclass(frozen=True)
class Daily:
    """Once per day at or after ``at`` (a ``(hour, minute)`` or a callable
    returning one, read every tick). ``weekday`` (0 = Monday) makes it
    weekly. The period is the date, ``YYYY-MM-DD``, on the chosen clock."""

    at: tuple[int, int] | Callable[[], tuple[int, int]] = (0, 0)
    weekday: int | None = None
    utc: bool = False
    stateful = True

    def period(self, now: datetime) -> str | None:
        clock = now.astimezone(timezone.utc) if self.utc else now.astimezone()
        if self.weekday is not None and clock.weekday() != self.weekday:
            return None
        at = self.at() if callable(self.at) else self.at
        if (clock.hour, clock.minute) < tuple(at):
            return None
        return clock.strftime("%Y-%m-%d")

    @property
    def interval_minutes(self) -> float:
        return 7 * 1440 if self.weekday is not None else 1440


@dataclass(frozen=True)
class Every:
    """Once per ``minutes``-long bucket of the clock (``Every(60)`` = once
    per clock hour)."""

    minutes: float
    stateful = True

    def period(self, now: datetime) -> str | None:
        return str(int(now.timestamp() // (60 * self.minutes)))

    @property
    def interval_minutes(self) -> float:
        return self.minutes


@dataclass(frozen=True)
class _Tick:
    stateful = False
    interval_minutes = None

    def period(self, now: datetime) -> str | None:
        return None


TICK = _Tick()


@dataclass(frozen=True)
class Backoff:
    """After ``n`` consecutive failures, wait ``first_minutes * 2**(n-1)`` capped at
    ``max_minutes``, and at least the entry's interval capped at ``max_minutes``."""

    first_minutes: float = 60
    max_minutes: float = 24 * 60

    def delay_minutes(self, failures: int, interval_minutes: float | None = None) -> float:
        grown = min(self.max_minutes, self.first_minutes * 2 ** min(max(failures, 1) - 1, 30))
        floor = min(interval_minutes or 0, self.max_minutes)
        return max(grown, floor)


DEFAULT_BACKOFF = Backoff()
#: A failed run is retried as soon as it is next due.
NO_BACKOFF = Backoff(first_minutes=0, max_minutes=0)


@dataclass(frozen=True)
class Job:
    """Run an entry as an orbit_jobs job: ``spawn(kind, payload, ...)`` with
    ``origin="scheduled"``. ``payload`` is a dict or ``payload(now) -> dict``. The
    subject defaults to ``("schedule", <entry name>)``; an active job of this kind for
    the subject (scheduled or manual) keeps the entry from starting another."""

    kind: str
    payload: dict | Callable[[datetime], dict] | None = None
    subject_type: str = "schedule"
    subject_id: str | None = None
    timeout_seconds: int | None = None
    priority: int = 0


@dataclass(frozen=True)
class Entry:
    """One scheduled thing.

    ``enabled`` is checked when the entry is due; while it says no, nothing is
    claimed or recorded. ``backoff`` defaults to :data:`DEFAULT_BACKOFF` for timed
    entries and :data:`NO_BACKOFF` for TICK. ``title``, ``kind`` and ``where`` are
    descriptive only (heartbeat ``name`` / ``kind`` / ``where``).
    """

    name: str
    when: Daily | Every | _Tick
    action: Callable[[datetime], Any] | Job
    enabled: Callable[[], Any] | None = None
    backoff: Backoff | None = None
    title: str | None = None
    kind: str | None = None
    where: str | None = None

    def _backoff(self) -> Backoff:
        if self.backoff is not None:
            return self.backoff
        return DEFAULT_BACKOFF if self.when.stateful else NO_BACKOFF


# ── time helpers ──────────────────────────────────────────────────────


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _now() -> datetime:
    return _parse(now_iso()) or datetime.now(timezone.utc)


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:ERROR_CHARS]


# ── state + run records ───────────────────────────────────────────────


def _claim(entry: Entry, at: datetime, tick: datetime) -> int | None:
    """Claim the entry's period and insert its ``running`` row; the run id, or None
    when it isn't due (or another process claimed it first)."""
    period = entry.when.period(tick)
    if entry.when.stateful and period is None:
        return None
    ts = _iso(at)
    with get_db() as conn:
        row = conn.execute(
            "SELECT period, retry_at FROM schedule_state WHERE name = ?", (entry.name,),
        ).fetchone()
    if row is not None:
        if entry.when.stateful and row["period"] == period:
            return None
        if row["retry_at"] and row["retry_at"] > _iso(tick):
            return None
    if entry.enabled is not None and not entry.enabled():
        return None
    with get_db() as conn:
        if entry.when.stateful:
            conn.execute("INSERT OR IGNORE INTO schedule_state (name, updated_at) VALUES (?, ?)",
                         (entry.name, ts))
            # Compare-and-set, so two processes never both claim one period.
            claimed = conn.execute(
                "UPDATE schedule_state SET period = ?, updated_at = ?"
                " WHERE name = ? AND period IS NOT ?"
                " AND (retry_at IS NULL OR retry_at <= ?)",
                (period, ts, entry.name, period, _iso(tick)),
            ).rowcount
            if not claimed:
                return None
        # A run still marked running here was cut off (the process stopped): this
        # process runs an entry's inline actions one at a time.
        conn.execute(
            "UPDATE schedule_runs SET status = 'skipped', finished_at = ?,"
            " error = 'interrupted (the process stopped mid-run)'"
            " WHERE name = ? AND status = 'running' AND job_id IS NULL",
            (ts, entry.name),
        )
        cur = conn.execute(
            "INSERT INTO schedule_runs (name, period, status, started_at) VALUES (?, ?, 'running', ?)",
            (entry.name, period, ts),
        )
        return int(cur.lastrowid)


def _discard(entry: Entry, run_id: int, period: str | None) -> None:
    """Undo a claim: no record, the period is free again."""
    with get_db() as conn:
        conn.execute("DELETE FROM schedule_runs WHERE id = ?", (run_id,))
        if period is not None:
            conn.execute("UPDATE schedule_state SET period = NULL WHERE name = ? AND period = ?",
                         (entry.name, period))


def _finish(entry: Entry, run_id: int, status: str, *, finished: datetime,
            duration_ms: int | None, error: str | None) -> None:
    """Close a run and update the entry's backoff: a failure releases the period and
    sets ``retry_at``; a success resets the failure count; a skip changes neither."""
    error = error[:ERROR_CHARS] if error else None
    ts = _iso(finished)
    with get_db() as conn:
        run = conn.execute("SELECT period FROM schedule_runs WHERE id = ?", (run_id,)).fetchone()
        conn.execute(
            "UPDATE schedule_runs SET status = ?, finished_at = ?, duration_ms = ?, error = ?"
            " WHERE id = ?",
            (status, ts, duration_ms, error, run_id),
        )
        if status == "skipped":
            return
        state = conn.execute(
            "SELECT failures FROM schedule_state WHERE name = ?", (entry.name,),
        ).fetchone()
        if status == "ok":
            if state is not None:
                conn.execute(
                    "UPDATE schedule_state SET failures = 0, retry_at = NULL, updated_at = ?"
                    " WHERE name = ?", (ts, entry.name))
            return
        failures = (state["failures"] if state is not None else 0) + 1
        delay = entry._backoff().delay_minutes(failures, entry.when.interval_minutes)
        retry_at = _iso(finished + timedelta(minutes=delay)) if delay > 0 else None
        conn.execute("INSERT OR IGNORE INTO schedule_state (name, updated_at) VALUES (?, ?)",
                     (entry.name, ts))
        conn.execute(
            "UPDATE schedule_state SET failures = ?, retry_at = ?, updated_at = ?,"
            " period = CASE WHEN period IS ? THEN NULL ELSE period END WHERE name = ?",
            (failures, retry_at, ts, run["period"] if run else None, entry.name),
        )


def _attach_job(run_id: int, job_id: int) -> None:
    with get_db() as conn:
        conn.execute("UPDATE schedule_runs SET job_id = ? WHERE id = ?", (job_id, run_id))


def _settle(entry: Entry, tick: datetime) -> list[str]:
    """Close the entry's job-backed runs whose job has ended; their statuses."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, started_at, job_id FROM schedule_runs"
            " WHERE name = ? AND status = 'running' AND job_id IS NOT NULL ORDER BY id",
            (entry.name,),
        ).fetchall()
    settled = []
    for row in rows:
        job = get_job(row["job_id"])
        if job is not None and job["status"] not in TERMINAL_STATUSES:
            continue
        if job is None:
            status, error, finished = "failed", f"job {row['job_id']} no longer exists", tick
        else:
            finished = _parse(job.get("finished_at")) or tick
            status = {"succeeded": "ok", "failed": "failed"}.get(job["status"], "skipped")
            error = None if status == "ok" else (job.get("error") or f"job {job['status']}")
        started = _parse(row["started_at"]) or finished
        duration = max(0, int((finished - started).total_seconds() * 1000))
        _finish(entry, row["id"], status, finished=finished, duration_ms=duration, error=error)
        settled.append(status)
    return settled


# ── the tick ──────────────────────────────────────────────────────────


async def _call(action: Callable[[datetime], Any], now: datetime) -> Any:
    if inspect.iscoroutinefunction(action):
        return await action(now)
    result = action(now)
    return await result if inspect.isawaitable(result) else result


async def _run_entry(entry: Entry, tick: datetime, at: datetime) -> str | None:
    job = entry.action if isinstance(entry.action, Job) else None
    settled: str | None = None
    if job is not None:
        done = await asyncio.to_thread(_settle, entry, tick)
        settled = done[-1] if done else None
        subject_id = job.subject_id or entry.name
        if await asyncio.to_thread(has_active_for_subject, job.subject_type, subject_id, job.kind):
            return settled
    run_id = await asyncio.to_thread(_claim, entry, at, tick)
    if run_id is None:
        return settled
    period = entry.when.period(tick)
    started = time.monotonic()

    def elapsed_ms() -> int:
        return int((time.monotonic() - started) * 1000)

    if job is not None:
        try:
            payload = job.payload(tick) if callable(job.payload) else dict(job.payload or {})
            job_id = spawn(job.kind, payload, subject_type=job.subject_type,
                           subject_id=job.subject_id or entry.name,
                           timeout_seconds=job.timeout_seconds, priority=job.priority,
                           origin="scheduled")
        except JobConflictError:  # started elsewhere since the check
            await asyncio.to_thread(_discard, entry, run_id, period)
            return settled
        except Exception as exc:
            logger.exception("scheduled entry %s could not spawn its job", entry.name)
            await asyncio.to_thread(_finish, entry, run_id, "failed", duration_ms=elapsed_ms(),
                                    finished=at, error=_error_text(exc))
            return "failed"
        await asyncio.to_thread(_attach_job, run_id, job_id)
        return "running"

    try:
        result = await _call(entry.action, tick)
    except Exception as exc:
        logger.exception("scheduled entry %s failed; continuing", entry.name)
        ms = elapsed_ms()
        await asyncio.to_thread(_finish, entry, run_id, "failed", duration_ms=ms,
                                finished=at + timedelta(milliseconds=ms), error=_error_text(exc))
        return "failed"
    if result is RETRY:
        await asyncio.to_thread(_discard, entry, run_id, period)
        return None
    status = "skipped" if result is SKIP else "ok"
    ms = elapsed_ms()
    await asyncio.to_thread(_finish, entry, run_id, status, duration_ms=ms,
                            finished=at + timedelta(milliseconds=ms), error=None)
    return status


async def run_due(entries: tuple[Entry, ...] | list[Entry],
                  now: datetime | None = None) -> dict[str, str]:
    """Run every due entry in order, each isolated from the others, and prune old
    run records (at most hourly). ``now`` defaults to the configured clock.
    Returns ``{name: status}`` for runs started or settled this tick."""
    tick = now or _now()
    started = time.monotonic()
    out: dict[str, str] = {}
    for entry in entries:
        at = tick + timedelta(seconds=time.monotonic() - started)
        try:
            status = await _run_entry(entry, tick, at)
        except Exception:
            logger.exception("scheduled entry %s failed; continuing", entry.name)
            continue
        if status is not None:
            out[entry.name] = status
    global _last_prune
    if _last_prune is None or not timedelta(0) <= tick - _last_prune < _PRUNE_EVERY:
        try:
            await asyncio.to_thread(prune, tick)
            _last_prune = tick
        except Exception:
            logger.exception("pruning schedule runs failed")
    return out


# ── maintenance + read side ──────────────────────────────────────────


def prune(now: datetime | None = None, keep_days: int = KEEP_DAYS) -> int:
    """Delete run records started more than ``keep_days`` ago; the count."""
    cutoff = _iso((now or _now()) - timedelta(days=keep_days))
    with get_db() as conn:
        return conn.execute("DELETE FROM schedule_runs WHERE started_at < ?", (cutoff,)).rowcount


def import_periods(periods: dict[str, str]) -> int:
    """One-time import of an app's existing ``{entry name: period}`` (same formats as
    :meth:`Daily.period` / :meth:`Every.period`), so nothing that already ran this
    period runs again after the switch. Entries that already have state are left
    alone. Returns how many were imported."""
    ts = _iso(_now())
    with get_db() as conn:
        return sum(
            conn.execute(
                "INSERT OR IGNORE INTO schedule_state (name, period, updated_at) VALUES (?, ?, ?)",
                (name, str(period), ts),
            ).rowcount
            for name, period in periods.items() if period
        )


def health(entries: tuple[Entry, ...] | list[Entry], now: datetime | None = None, *,
           tick_minutes: float = 1) -> list[dict[str, Any]]:
    """Per-entry heartbeat data: ``id``, ``name``, ``expected_every_minutes`` (TICK
    entries report ``tick_minutes``), ``last_run_at``, ``last_ok_at``, ``last_status``
    (ok | failed | skipped | running | unknown), ``last_error``, ``last_duration_ms``,
    ``runs_24h``, ``failures_24h``, plus ``kind`` / ``where`` when the entry declares
    them. Never includes payloads."""
    since = _iso((now or _now()) - timedelta(hours=24))
    out = []
    with get_db() as conn:
        for entry in entries:
            def q(sql: str, *args: Any, name: str = entry.name) -> Any:
                return conn.execute(sql, (name, *args)).fetchone()

            last = q("SELECT started_at, status FROM schedule_runs WHERE name = ?"
                     " ORDER BY id DESC LIMIT 1")
            ok = q("SELECT MAX(finished_at) AS at FROM schedule_runs WHERE name = ? AND status = 'ok'")
            failed = q("SELECT error FROM schedule_runs WHERE name = ? AND status = 'failed'"
                       " ORDER BY id DESC LIMIT 1")
            done = q("SELECT duration_ms FROM schedule_runs WHERE name = ?"
                     " AND status != 'running' ORDER BY id DESC LIMIT 1")
            counts = q("SELECT COUNT(*) AS runs, COALESCE(SUM(status = 'failed'), 0) AS failures"
                       " FROM schedule_runs WHERE name = ? AND started_at >= ?", since)
            item = {
                "id": entry.name,
                "name": entry.title or entry.name,
                "expected_every_minutes": entry.when.interval_minutes or tick_minutes,
                "last_run_at": last["started_at"] if last else None,
                "last_ok_at": ok["at"] if ok else None,
                "last_status": last["status"] if last else "unknown",
                "last_error": failed["error"] if failed else None,
                "last_duration_ms": done["duration_ms"] if done else None,
                "runs_24h": counts["runs"],
                "failures_24h": counts["failures"],
            }
            if entry.kind is not None:
                item["kind"] = entry.kind
            if entry.where is not None:
                item["where"] = entry.where
            out.append(item)
    return out
