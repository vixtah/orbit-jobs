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
waits ``first_minutes`` (60) doubling per consecutive failure up to ``max_minutes``
(a day), whatever the entry's interval: a 6-hourly entry retries after 1 h, a
monthly one within a day. A success resets it. TICK entries don't back off unless
given a ``backoff``.

A hand-written loop that doesn't use the registry (``while True: ...; sleep``)
declares a :class:`Service` and wraps each iteration in :func:`record_run`, which
writes the same ``schedule_runs`` records (no periods, no backoff). Work that runs
only when asked (a cache refresh on a request) is a ``Service`` with
``expected_every_minutes=None``: on demand, never late, only failures matter.

Run errors are redacted before they are stored and when reported (the
``redact_error`` hook, :mod:`orbit_jobs.redact`); raise :class:`RunFailed` to record a
plain message without the exception type. :func:`import_runs` brings an app's past
run records over when it switches from its own.

:func:`health` reports per-entry (and per-service) heartbeat data
(``docs/plans/HEARTBEATS.md`` in the automations repository). Needs only the standard library and the job tables
(``orbit_jobs.schema``); no worker is required unless a :class:`Job` entry's kind
is capability-routed.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from orbit_jobs.config import settings
from orbit_jobs.db import get_db, now_iso
from orbit_jobs.runner import JobConflictError, spawn
from orbit_jobs.store import TERMINAL_STATUSES, has_active_for_subject
from orbit_jobs.store import get as get_job

__all__ = [
    "Daily", "Every", "TICK", "Entry", "Service", "Job", "Backoff", "DEFAULT_BACKOFF", "NO_BACKOFF",
    "RETRY", "SKIP", "RunFailed", "FROM_WHEN", "MONDAY", "SATURDAY", "SUNDAY", "KEEP_DAYS", "ERROR_CHARS",
    "run_due", "record_run", "health", "prune", "import_periods", "import_runs",
]

logger = logging.getLogger(__name__)

#: An action returns this to leave its period unclaimed (retry next tick, no record).
RETRY = object()
#: An action returns this to record a ``skipped`` run (its period stays claimed).
SKIP = object()



class RunFailed(Exception):
    """Raise it (or a subclass) from an action or a :func:`record_run` block to record
    a failed run whose error is exactly its message, without the exception type in
    front (``RunFailed("3 of 40 venues failed")``). Other exceptions are recorded as
    ``Type: message``. Either way the text is redacted (the ``redact_error`` hook)."""


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
    ``max_minutes``, regardless of the entry's interval. ``at_least_interval=True``
    restores the v0.2.1 floor: never sooner than the interval (capped at
    ``max_minutes``)."""

    first_minutes: float = 60
    max_minutes: float = 24 * 60
    at_least_interval: bool = False

    def delay_minutes(self, failures: int, interval_minutes: float | None = None) -> float:
        grown = min(self.max_minutes, self.first_minutes * 2 ** min(max(failures, 1) - 1, 30))
        if not self.at_least_interval:
            return grown
        return max(grown, min(interval_minutes or 0, self.max_minutes))


DEFAULT_BACKOFF = Backoff()
#: A failed run is retried as soon as it is next due.
NO_BACKOFF = Backoff(first_minutes=0, max_minutes=0)


class _FromWhen:
    """The default of :attr:`Entry.expected_every_minutes`: take it from ``when``."""

    def __repr__(self) -> str:
        return "FROM_WHEN"


FROM_WHEN = _FromWhen()


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
    ``expected_every_minutes`` overrides the cadence :func:`health` reports (by
    default the ``when``'s interval, or the tick for TICK); ``None`` means on demand
    (e.g. a TICK entry whose own gate rarely opens): never late, only failures matter.
    """

    name: str
    when: Daily | Every | _Tick
    action: Callable[[datetime], Any] | Job
    enabled: Callable[[], Any] | None = None
    backoff: Backoff | None = None
    title: str | None = None
    kind: str | None = None
    where: str | None = None
    expected_every_minutes: float | None | _FromWhen = FROM_WHEN

    def _backoff(self) -> Backoff:
        if self.backoff is not None:
            return self.backoff
        return DEFAULT_BACKOFF if self.when.stateful else NO_BACKOFF


@dataclass(frozen=True)
class Service:
    """A hand-written loop or worker that records its own runs with
    :func:`record_run` instead of being run by :func:`run_due`. ``name`` is its id in
    ``schedule_runs``; ``expected_every_minutes`` is how often a run should happen, or
    ``None`` for work that runs only when asked (on demand: never late, only failures
    matter). ``title``, ``kind`` and ``where`` are descriptive only, as on
    :class:`Entry`."""

    name: str
    expected_every_minutes: float | None
    title: str | None = None
    kind: str | None = None
    where: str | None = None


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
    if isinstance(exc, RunFailed):
        return _clean(str(exc)) or type(exc).__name__
    return _clean(f"{type(exc).__name__}: {exc}")


def _clean(error: str | None) -> str | None:
    """Error text as stored and reported: redacted, then cut to ``ERROR_CHARS``."""
    if not error:
        return None
    try:
        error = settings.redact_error(str(error))
    except Exception:  # a broken hook never leaks the raw text
        logger.warning("the redact_error hook failed; withholding the error text", exc_info=True)
        return "[error text withheld: redaction failed]"
    return error[:ERROR_CHARS] if error else None


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
    error = _clean(error)
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
        if isinstance(exc, RunFailed):
            logger.warning("scheduled entry %s failed: %s", entry.name, _error_text(exc))
        else:
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


# ── hand-written loops ────────────────────────────────────────────────


def _open_run(name: str) -> int:
    """Insert a ``running`` row (closing one left by a stopped process); its id."""
    ts = _iso(_now())
    with get_db() as conn:
        conn.execute(
            "UPDATE schedule_runs SET status = 'skipped', finished_at = ?,"
            " error = 'interrupted (the process stopped mid-run)'"
            " WHERE name = ? AND status = 'running' AND job_id IS NULL",
            (ts, name),
        )
        cur = conn.execute(
            "INSERT INTO schedule_runs (name, status, started_at) VALUES (?, 'running', ?)",
            (name, ts),
        )
        return int(cur.lastrowid)


def _close_run(run_id: int, started: float, exc: BaseException | None) -> None:
    if exc is None:
        status, error = "ok", None
    elif isinstance(exc, Exception):
        status, error = "failed", _error_text(exc)
    else:  # cancelled or interrupted: not the loop's failure
        status, error = "skipped", _error_text(exc) if str(exc) else type(exc).__name__
    error = _clean(error)
    ms = int((time.monotonic() - started) * 1000)
    finished = _now()
    with get_db() as conn:
        conn.execute(
            "UPDATE schedule_runs SET status = ?, finished_at = ?, duration_ms = ?, error = ?"
            " WHERE id = ?",
            (status, _iso(finished), ms, error, run_id),
        )
    global _last_prune
    if _last_prune is None or not timedelta(0) <= finished - _last_prune < _PRUNE_EVERY:
        try:
            prune(finished)
            _last_prune = finished
        except Exception:
            logger.exception("pruning schedule runs failed")


class record_run:  # lower-case: used like a function, ``with record_run(name):``
    """Record one run of a hand-written loop in ``schedule_runs``, so it shows in
    :func:`health` like an entry: ``running`` on entry, then ``ok``, or ``failed``
    with the error (cut to 300 characters) when the block raises; the exception still
    propagates. A cancellation (``CancelledError``, ``KeyboardInterrupt``) is recorded
    ``skipped``. Recording never breaks the work: if a database write fails (opening
    or closing the run) a warning is logged, the block still runs and its own result
    or exception reaches the caller unchanged. Works as ``with`` and ``async with`` (the async form does its
    database writes off the event loop). ``service`` is a :class:`Service` or its
    name. One name runs one iteration at a time: a row still ``running`` when the
    next starts is closed as ``skipped`` (interrupted). Old records are pruned at most
    hourly, as :func:`run_due` does."""

    def __init__(self, service: Service | str):
        self.name = service.name if isinstance(service, Service) else service
        self._run_id: int | None = None
        self._started = 0.0

    # Telemetry never breaks the work: a failed write (database locked, missing,
    # read-only) is logged as a warning, the block still runs, and its own outcome
    # (return or exception) is what the caller sees.

    def __enter__(self) -> record_run:
        try:
            self._run_id = _open_run(self.name)
        except Exception:
            self._run_id = None
            logger.warning("recording the start of %s failed; running it unrecorded",
                           self.name, exc_info=True)
        self._started = time.monotonic()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._run_id is None:
            return
        try:
            _close_run(self._run_id, self._started, exc)
        except Exception:
            logger.warning("recording the end of %s failed", self.name, exc_info=True)

    async def __aenter__(self) -> record_run:
        try:
            self._run_id = await asyncio.to_thread(_open_run, self.name)
        except Exception:
            self._run_id = None
            logger.warning("recording the start of %s failed; running it unrecorded",
                           self.name, exc_info=True)
        self._started = time.monotonic()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._run_id is None:
            return
        try:
            await asyncio.to_thread(_close_run, self._run_id, self._started, exc)
        except Exception:
            logger.warning("recording the end of %s failed", self.name, exc_info=True)


# ── maintenance + read side ──────────────────────────────────────────


def prune(now: datetime | None = None, keep_days: int = KEEP_DAYS) -> int:
    """Delete run records started more than ``keep_days`` ago, except each name's
    latest run and latest success (so a monthly entry keeps its ``last_ok_at``); the
    count."""
    with get_db() as conn:
        return _prune(conn, now, keep_days)


def _prune(conn: Any, now: datetime | None = None, keep_days: int = KEEP_DAYS) -> int:
    cutoff = _iso((now or _now()) - timedelta(days=keep_days))
    return conn.execute(
        "DELETE FROM schedule_runs WHERE started_at < ? AND id NOT IN ("
        " SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY name"
        "  ORDER BY started_at DESC, id DESC) AS n FROM schedule_runs) WHERE n = 1"
        " UNION"
        " SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY name"
        "  ORDER BY finished_at DESC, id DESC) AS n FROM schedule_runs"
        "  WHERE status = 'ok') WHERE n = 1)",
        (cutoff,),
    ).rowcount


def import_periods(periods: dict[str, str], *, failures: dict[str, int] | None = None,
                   retry_at: dict[str, datetime | str] | None = None) -> int:
    """One-time import of an app's existing schedule state, so the switch from its own
    scheduler neither repeats a run nor forgets a failure:

    - ``periods``: ``{entry name: period}`` (same formats as :meth:`Daily.period` /
      :meth:`Every.period`); nothing that already ran this period runs again.
    - ``failures``: ``{entry name: consecutive failures}``; the next failure's backoff
      grows from there.
    - ``retry_at``: ``{entry name: datetime or ISO time}``; the entry doesn't run
      before then (a naive time is UTC). Leave a retrying entry's period out (a failed
      run releases its period), so it runs once ``retry_at`` passes.

    A name may appear in any of the three. Entries that already have state are left
    alone (existing state wins). Returns how many were imported."""
    failures, retry_at = failures or {}, retry_at or {}
    retries: dict[str, str] = {}
    for name, when in retry_at.items():
        if when is None or when == "":
            continue
        parsed = when if isinstance(when, datetime) else _parse(str(when))
        if parsed is None:
            raise ValueError(f"import_periods: unreadable retry_at for {name!r}: {when!r}")
        retries[name] = _iso(parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc))
    names = dict.fromkeys([*periods, *failures, *retries])
    ts = _iso(_now())
    imported = 0
    with get_db() as conn:
        for name in names:
            period = str(periods[name]) if periods.get(name) else None
            count = int(failures.get(name) or 0)
            retry = retries.get(name)
            if period is None and not count and retry is None:
                continue
            imported += conn.execute(
                "INSERT OR IGNORE INTO schedule_state (name, period, failures, retry_at,"
                " updated_at) VALUES (?, ?, ?, ?, ?)",
                (name, period, count, retry, ts),
            ).rowcount
    return imported


_IMPORT_STATUSES = ("ok", "failed", "skipped")


def _when(value: datetime | str | float | int | None, what: str) -> datetime | None:
    """A run time given as a datetime (naive = UTC), ISO text or epoch seconds."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value, timezone.utc)
    parsed = _parse(str(value))
    if parsed is None:
        raise ValueError(f"import_runs: unreadable {what}: {value!r}")
    return parsed


def import_runs(runs: Iterable[Mapping[str, Any]], *, now: datetime | None = None) -> int:
    """One-time import of an app's past run records into ``schedule_runs``, so an app
    switching from its own records keeps its history: :func:`health` has ``last_ok_at``,
    the last error and the 24-hour counts from the first moment, instead of a running
    first run with no success yet.

    Each run is a mapping with ``name``, ``started_at`` and ``status`` (``ok``,
    ``failed`` or ``skipped``; ``running``, a run its process never finished, is
    recorded ``skipped`` as interrupted) and optionally ``finished_at``, ``error`` and
    ``duration_ms`` (computed from the two times when missing). Times are datetimes (a
    naive one is UTC), ISO text or epoch seconds. Errors are redacted and cut like
    any other run's.

    Idempotent: a run whose name and start time (to the second) are already recorded
    is skipped, so it is safe to call on every start. Records older than ``KEEP_DAYS``
    are pruned as usual, except each name's latest run and latest success. Returns how
    many runs were added."""
    rows = []
    for run in runs:
        name = run.get("name")
        if not name:
            raise ValueError(f"import_runs: a run without a name: {dict(run)!r}")
        status = str(run.get("status") or "")
        error = run.get("error")
        if status == "running":
            status, error = "skipped", error or "interrupted (the process stopped mid-run)"
        if status not in _IMPORT_STATUSES:
            raise ValueError(f"import_runs: unknown status {status!r} for {name!r}")
        started = _when(run.get("started_at"), "started_at")
        if started is None:
            raise ValueError(f"import_runs: a run of {name!r} without started_at")
        finished = _when(run.get("finished_at"), "finished_at")
        duration = run.get("duration_ms")
        if duration is None and finished is not None:
            duration = max(0, int((finished - started).total_seconds() * 1000))
        if finished is None:  # every imported run is over: it ended when it started
            finished = started + timedelta(milliseconds=int(duration or 0))
        rows.append((str(name), status, _iso(started),
                     _iso(finished), int(duration) if duration is not None else None, _clean(error)))
    added: list[int] = []
    with get_db() as conn:
        for name, status, started, finished, duration, error in rows:
            if conn.execute("SELECT 1 FROM schedule_runs WHERE name = ? AND started_at = ?",
                            (name, started)).fetchone():
                continue
            added.append(int(conn.execute(
                "INSERT INTO schedule_runs (name, status, started_at, finished_at,"
                " duration_ms, error) VALUES (?, ?, ?, ?, ?, ?)",
                (name, status, started, finished, duration, error),
            ).lastrowid))
        if not added:
            return 0
        # What prune would delete goes now, so a repeat import adds nothing again.
        _prune(conn, now)
        marks = ",".join("?" * len(added))
        return conn.execute(f"SELECT COUNT(*) FROM schedule_runs WHERE id IN ({marks})",
                            added).fetchone()[0]


def _expected_every(entry: Entry | Service, tick_minutes: float) -> float | None:
    if isinstance(entry, Service) or entry.expected_every_minutes is not FROM_WHEN:
        return entry.expected_every_minutes
    return entry.when.interval_minutes or tick_minutes


def health(entries: tuple[Entry | Service, ...] | list[Entry | Service],
           now: datetime | None = None, *, tick_minutes: float = 1) -> list[dict[str, Any]]:
    """Per-entry heartbeat data for :class:`Entry` and :class:`Service` items, in
    order: ``id``, ``name``, ``expected_every_minutes`` (TICK entries report
    ``tick_minutes``; a service or an entry that declares one its own, ``None`` meaning
    on demand), ``last_run_at``, ``last_ok_at``, ``last_status``
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
                     " ORDER BY started_at DESC, id DESC LIMIT 1")
            ok = q("SELECT MAX(finished_at) AS at FROM schedule_runs WHERE name = ? AND status = 'ok'")
            failed = q("SELECT error FROM schedule_runs WHERE name = ? AND status = 'failed'"
                       " ORDER BY started_at DESC, id DESC LIMIT 1")
            done = q("SELECT duration_ms FROM schedule_runs WHERE name = ?"
                     " AND status != 'running' ORDER BY started_at DESC, id DESC LIMIT 1")
            counts = q("SELECT COUNT(*) AS runs, COALESCE(SUM(status = 'failed'), 0) AS failures"
                       " FROM schedule_runs WHERE name = ? AND started_at >= ?", since)
            item = {
                "id": entry.name,
                "name": entry.title or entry.name,
                "expected_every_minutes": _expected_every(entry, tick_minutes),
                "last_run_at": last["started_at"] if last else None,
                "last_ok_at": ok["at"] if ok else None,
                "last_status": last["status"] if last else "unknown",
                "last_error": _clean(failed["error"]) if failed else None,
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
