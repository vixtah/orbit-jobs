"""Async runner that turns a registered kind into a background task.

``spawn`` is the only entry point. It persists the job row, launches the
handler on the running event loop, and wires status transitions + error
capture around it. The asyncio Task is held in a process-local dict so
we can keep the handler alive across the request that spawned it — the
task is detached from request lifetime.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from orbit_jobs import config, events
from orbit_jobs.capture import JobHandle, clear_handle, current, start_handle
from orbit_jobs.db import get_db
from orbit_jobs.registry import get_descriptor
from orbit_jobs.store import (
    get_workflow_id,
    has_active_for_subject,
    insert,
    mark_running,
    mark_terminal,
)

log = logging.getLogger(__name__)

_RUNNING: dict[int, asyncio.Task] = {}
_HANDLES: dict[int, JobHandle] = {}


class JobConflictError(RuntimeError):
    """Raised when an identical active job already exists for the subject."""


def register_handle(job_id: int, handle: JobHandle) -> None:
    """Expose a running job handle for cancellation in this process."""
    _HANDLES[job_id] = handle


def unregister_handle(job_id: int) -> None:
    """Remove a previously registered in-process job handle."""
    _HANDLES.pop(job_id, None)


def spawn(
    kind: str,
    payload: dict[str, Any],
    *,
    subject_type: str | None = None,
    subject_id: str | None = None,
    allow_concurrent: bool = False,
    timeout_seconds: int | None = None,
    parent_job_id: int | None = None,
    required_capability: str | None = None,
    priority: int = 0,
    workflow_id: str | None = None,
    origin: str | None = None,
) -> int:
    """Persist a pending job and (in the current legacy path) kick off
    its execution on the running event loop.

    Phase 0 forwards ``parent_job_id``, ``required_capability``,
    ``priority``, and ``workflow_id`` straight into the row. They have
    no behavioural effect yet — the Phase 1 worker runtime will start
    consulting ``required_capability`` for queue routing. Until then,
    the asyncio task is created exactly as before so the dashboard,
    iOS app, and CLI keep working unchanged.

    ``origin`` records who/what kicked off the job — see
    :mod:`orbit_jobs.schema` for the value vocabulary. Resolution rules:

    1. Caller passed an explicit ``origin`` → use it.
    2. Else if ``parent_job_id`` is set → inherit the parent's origin.
    3. Else default to ``"agent"`` so spawns that bypass an HTTP route
       (curl, scripts, an LLM coding agent) don't masquerade as user
       clicks in the dashboard.
    """
    descriptor = get_descriptor(kind)
    if descriptor is None:
        raise ValueError(f"Unknown job kind: {kind!r}")
    handler = descriptor.handler

    # Fall back to the descriptor's default timeout when the caller
    # didn't pin one. Bounds the worker's asyncio.wait_for() so a wedged
    # handler can't pin a slot forever.
    if timeout_seconds is None:
        timeout_seconds = descriptor.default_timeout_seconds

    # Optional spawn-time payload validation. Lets each kind catch
    # malformed payloads at the API boundary instead of inside the
    # worker (where the failure hides in a step trace).
    schema = descriptor.payload_schema
    if schema is not None:
        try:
            schema.model_validate(payload)
        except Exception as err:  # noqa: BLE001
            raise ValueError(f"invalid payload for {kind!r}: {err}") from err

    # Capability resolution: descriptor is the source of truth. Callers
    # may still pass ``required_capability`` explicitly for the few
    # cross-cutting cases (re-routing a kind onto a different worker
    # pool in tests, etc.) but the default is "ask the descriptor".
    resolved_capability = descriptor.resolve_capability(payload)
    if required_capability is None:
        required_capability = resolved_capability
    elif (
        resolved_capability is not None
        and required_capability != resolved_capability
    ):
        raise ValueError(
            f"required_capability override {required_capability!r} "
            f"contradicts descriptor for {kind!r}"
            f" (expected {resolved_capability!r})"
        )

    if (
        not allow_concurrent
        and subject_type is not None
        and subject_id is not None
        and has_active_for_subject(subject_type, subject_id, kind)
    ):
        raise JobConflictError(
            f"{kind} already running for {subject_type}:{subject_id}"
        )

    if origin is None and parent_job_id is not None:
        origin = _origin_of(parent_job_id)
    if origin is None:
        origin = "agent"

    job_id = insert(
        kind, payload, subject_type, subject_id, timeout_seconds,
        parent_job_id=parent_job_id,
        required_capability=required_capability,
        priority=priority,
        workflow_id=workflow_id,
        origin=origin,
    )

    # Capability-routed jobs sit pending until a worker claims them
    # (Phase 1+). Legacy jobs without capability run inline on the
    # current event loop, exactly as before — that's the path the
    # FastAPI server still uses for digest_run, chat_turn, etc.
    if required_capability is not None:
        return job_id

    loop = asyncio.get_running_loop()
    task = loop.create_task(
        _execute(job_id, handler, payload, timeout_seconds, kind),
        name=f"job-{job_id}",
    )
    _RUNNING[job_id] = task
    task.add_done_callback(lambda t, jid=job_id: _RUNNING.pop(jid, None))
    return job_id


def spawn_child(
    kind: str,
    payload: dict[str, Any],
    *,
    capability: str | None = None,
    priority: int = 0,
    workflow_id: str | None = None,
    parent_job_id: int | None = None,
    subject_type: str | None = None,
    subject_id: str | None = None,
    timeout_seconds: int | None = None,
    allow_concurrent: bool = True,
    origin: str | None = None,
) -> int:
    """Enqueue follow-up work from inside a running handler.

    Convenience wrapper around :func:`spawn` for the producer/consumer
    model. Two ergonomic shortcuts the bare ``spawn`` doesn't have:

    - ``parent_job_id`` defaults to the currently-running job's id
      (resolved via the ``JobHandle`` ContextVar). Pass explicitly only
      when spawning from outside a handler.
    - ``workflow_id`` defaults to the parent's so a whole spawn-tree
      shares one grouping. Override to start a new workflow mid-tree.

    ``allow_concurrent`` defaults to True for child jobs — the parent
    is the natural concurrency gate. We don't want a downstream
    classify_thread blocked because messenger-ingest is already running.

    Surface mirrors what an ``rq.Queue.enqueue`` would expose, so the
    swap to a Redis-backed queue (if/when) doesn't change handler-side
    code — only :func:`spawn`'s implementation.
    """
    if parent_job_id is None:
        handle = current()
        parent_job_id = handle.job_id if handle is not None else None

    if workflow_id is None and parent_job_id is not None:
        workflow_id = get_workflow_id(parent_job_id)

    return spawn(
        kind,
        payload,
        subject_type=subject_type,
        subject_id=subject_id,
        allow_concurrent=allow_concurrent,
        timeout_seconds=timeout_seconds,
        parent_job_id=parent_job_id,
        required_capability=capability,
        priority=priority,
        workflow_id=workflow_id,
        origin=origin,  # None → spawn() inherits from parent_job_id
    )


def _origin_of(job_id: int) -> str | None:
    """Look up a job's origin (used by spawn() to inherit). ``None`` if
    the row doesn't exist (shouldn't happen in practice — ``spawn``
    always inserts before caller can reference)."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT origin FROM jobs WHERE id = ?", (job_id,),
        ).fetchone()
    return row["origin"] if row else None


async def _execute(
    job_id: int,
    handler,
    payload: dict,
    timeout_seconds: int | None,
    kind: str,
) -> None:
    mark_running(job_id)
    handle = start_handle(job_id, kind)
    register_handle(job_id, handle)
    final_status: str | None = None
    final_error: str | None = None
    final_result: dict | None = None
    try:
        if timeout_seconds and timeout_seconds > 0:
            result = await asyncio.wait_for(
                handler(job_id, payload), timeout=timeout_seconds,
            )
        else:
            result = await handler(job_id, payload)
        final_status = "succeeded"
        final_result = result or {}
    except asyncio.TimeoutError:
        log.warning("job %s timed out after %ss", job_id, timeout_seconds)
        final_status = "failed"
        final_error = f"timeout after {timeout_seconds}s"
        mark_terminal(job_id, "failed", error=final_error)
    except asyncio.CancelledError:
        final_status = "cancelled"
        final_error = "cancelled"
        mark_terminal(job_id, "cancelled", error="cancelled")
        events.emit(job_id, {
            "type": "job.finished", "status": "cancelled", "error": "cancelled",
        })
        unregister_handle(job_id)
        clear_handle()
        raise
    except Exception as exc:
        log.exception("job %s failed", job_id)
        final_status = "failed"
        final_error = f"{type(exc).__name__}: {exc}"
        mark_terminal(job_id, "failed", error=final_error)
    finally:
        unregister_handle(job_id)
        clear_handle()
    if final_status == "succeeded":
        mark_terminal(job_id, "succeeded", result=final_result)
    events.emit(job_id, {
        "type": "job.finished",
        "status": final_status,
        "error": final_error,
        "result": final_result,
    })
    # The on_job_finished hook (the host's activity feed). Best-effort —
    # never raise.
    emit_activity_for_job_finish(
        job_id=job_id,
        kind=kind,
        status=final_status,
        error=final_error,
    )


def emit_activity_for_job_finish(
    *,
    job_id: int,
    kind: str,
    status: str | None,
    error: str | None,
) -> None:
    """Tell the configured ``on_job_finished`` hook a job reached a terminal
    status (the host mirrors some kinds into its activity feed). Best-effort —
    never raises."""
    try:
        config.settings.on_job_finished(job_id=job_id, kind=kind, status=status, error=error)
    except Exception:  # noqa: BLE001
        # Activity emission is best-effort.
        pass


def cancel(job_id: int) -> bool:
    """Cancel an in-flight job. Fires registered cancel callbacks
    (close httpx clients, kill subprocesses) and cancels the asyncio
    Task so awaiting code surfaces ``CancelledError`` immediately."""
    handle = _HANDLES.get(job_id)
    if handle is not None:
        handle.cancel()
    task = _RUNNING.get(job_id)
    if task is None or task.done():
        return handle is not None
    task.cancel()
    return True
