"""orbit_jobs: a SQLite-backed job queue with capability workers and remote workers.

A **job** is a unit of detached background work tracked in the ``jobs``
table. Each job has a ``kind`` (looked up against a handler registry),
optional ``subject`` (`(type, id)` pointing at the domain object that
spawned it), a JSON payload, and lifecycle status columns.

Public surface:

- ``configure(**hooks)`` — wire the host (database, clock, event bus,
  transport, callbacks); see :mod:`orbit_jobs.config`. Defaults need only
  the standard library.
- ``register_kind(KindDescriptor(...))`` — register an ``async`` handler.
  The composition root registers kinds; the engine never imports them.
- ``spawn(kind, payload, subject_type=..., subject_id=..., ...)`` — persist
  a pending row and return its id. Capability-routed kinds wait for a worker
  (``orbit_jobs.worker``); the rest run on the current event loop.
- ``get(job_id)`` / ``list_jobs(...)`` — read-only queries for the API layer.
- ``current()`` / ``begin_step`` / ``end_step`` / ``JobHandle`` — in-handler
  telemetry and cancellation (``orbit_jobs.runctx`` holds the ContextVar).
- ``cancel_orphans()`` / ``recover_orphans()`` — startup and periodic sweeps.

Worker entry points are separate modules, imported on demand:
``orbit_jobs.worker`` (local capability worker), ``orbit_jobs.remote`` /
``orbit_jobs.remote_worker`` (jobs run on other machines over HTTP) and
``orbit_jobs.routes`` (the FastAPI router for them).

A handler signature is ``async (job_id: int, payload: dict) -> dict``.
The returned dict is persisted as JSON in ``result``; raising produces a
``failed`` status with the exception text in ``error``.
"""

from orbit_jobs import events
from orbit_jobs.capture import JobHandle, begin_step, current, end_step
from orbit_jobs.config import configure, settings
from orbit_jobs.history import has_succeeded_for_subject
from orbit_jobs.registry import (
    KindDescriptor,
    get_descriptor,
    list_descriptors,
    register_kind,
)
from orbit_jobs.runner import (
    JobConflictError,
    cancel,
    emit_activity_for_job_finish,
    register_handle,
    spawn,
    spawn_child,
    unregister_handle,
)
from orbit_jobs.store import (
    cancel_orphans,
    cancel_requested,
    claim_one,
    clear_superseded_jobs,
    delete_job,
    get,
    has_active_for_subject,
    heartbeat,
    list_jobs,
    list_live_workers,
    list_steps,
    pending_count_by_capability,
    recover_orphans,
    request_cancel,
)

# Per-job event stream for SSE. ``events.subscribe(job_id)`` yields dicts;
# ``events.emit`` is for internal use (called by capture + runner).
subscribe = events.subscribe
subscribe_global = events.subscribe_global

__all__ = [
    # Wiring
    "configure",
    "settings",
    # Lifecycle / control
    "spawn",
    "spawn_child",
    "cancel",
    "request_cancel",
    "cancel_requested",
    "register_kind",
    "KindDescriptor",
    "get_descriptor",
    "list_descriptors",
    # Read
    "get",
    "list_jobs",
    "list_steps",
    "has_active_for_subject",
    "has_succeeded_for_subject",
    "clear_superseded_jobs",
    "delete_job",
    "list_live_workers",
    "pending_count_by_capability",
    # In-handler
    "current",
    "JobHandle",
    "begin_step",
    "end_step",
    "register_handle",
    "unregister_handle",
    "emit_activity_for_job_finish",
    # Live observation
    "subscribe",
    "subscribe_global",
    # Errors
    "JobConflictError",
    # Maintenance
    "cancel_orphans",
    "recover_orphans",
    # Worker runtime
    "claim_one",
    "heartbeat",
    "run_claimed_job",
]


async def run_claimed_job(job: dict, worker_id: str) -> bool:
    """Execute a claimed dedicated-worker job with its normal lease and cancellation."""
    from orbit_jobs.worker import run_claimed_job as execute

    return await execute(job, worker_id)
