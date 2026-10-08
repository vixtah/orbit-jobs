"""Per-job event stream for live observation (SSE).

Transport is the configured bus (``config.settings.make_bus``; one topic per
job_id), built on first use. This module owns only the job-specific shape on top
of it:

- ``step.started``  — a node just entered (id, name, started_at, input_text)
- ``step.finished`` — terminal status, ms, output_text, error?
- ``step.llm``      — one LLM call recorded mid-step
- ``step.io``       — one IO record recorded mid-step
- ``job.finished``  — terminal status for the job (closes the per-job stream)

``subscribe(job_id)`` yields that job's events and closes itself when
``job.finished`` arrives. ``subscribe_global()`` rides the bus firehose:
for every job event it emits a lightweight summary (enriched with the
job's kind/subject) so the dashboard can invalidate only the affected
React Query keys without a global refetch.

Queue size: ``JOBS_EVENT_QUEUE_SIZE`` (default 256). A slow subscriber
loses events and recovers from the job row (``store.get``).
"""

from __future__ import annotations

import os
import threading
from typing import AsyncIterator

from orbit_jobs import config
from orbit_jobs.db import get_db

_QUEUE_SIZE = int(os.environ.get("JOBS_EVENT_QUEUE_SIZE", "256"))
_bus = None
_bus_factory = None
_bus_lock = threading.Lock()


def _the_bus():
    """The ``jobs`` bus, rebuilt if a different ``make_bus`` hook is configured."""
    global _bus, _bus_factory
    factory = config.settings.make_bus
    with _bus_lock:
        if _bus is None or _bus_factory is not factory:
            _bus, _bus_factory = factory("jobs", queue_size=_QUEUE_SIZE), factory
        return _bus


def emit(job_id: int, event: dict) -> None:
    """Publish an event for a job to its per-job subscribers (and the
    firehose feeding ``subscribe_global``). Non-blocking."""
    _the_bus().emit(event, topic=job_id)


async def subscribe(
    job_id: int, *, initial: dict | None = None,
) -> AsyncIterator[dict]:
    """Yield events for a job until the stream closes naturally.

    Closes when a ``job.finished`` event arrives, or when the caller
    cancels the iterator (e.g. SSE client disconnect). An ``initial``
    snapshot, if given, is yielded first so clients don't race the gap
    between "fetch current state" and "subscribe to deltas".
    """
    async for event in _the_bus().subscribe(job_id, initial=initial):
        yield event
        if event.get("type") == "job.finished":
            break


async def subscribe_global() -> AsyncIterator[dict]:
    """Yield a lightweight summary for every job lifecycle event.

    Used by the dashboard for scoped query invalidation: the client looks
    at ``job_id`` / ``kind`` / ``subject_*`` to decide which React Query
    keys to invalidate, so only affected components refetch. A ``ping``
    every 30s lets clients detect dead streams.
    """
    async for topic, event in _the_bus().firehose(
        idle_timeout=30.0, idle_event=(None, {"type": "ping"}),
    ):
        if event.get("type") == "ping":
            yield {"type": "ping"}
            continue
        meta = _job_meta(topic)
        yield {
            "job_id": topic,
            "type": event.get("type"),
            "status": event.get("status"),
            "kind": meta.get("kind"),
            "subject_type": meta.get("subject_type"),
            "subject_id": meta.get("subject_id"),
        }


def has_subscribers(job_id: int) -> bool:
    return _the_bus().has_subscribers(job_id)


# Cached (kind, subject_type, subject_id) per job_id so the global stream
# doesn't hit the DB on every step event. Jobs are append-only once
# kind/subject are set, so this is safe.
_meta_cache: dict[int, dict] = {}


def _job_meta(job_id: int) -> dict:
    cached = _meta_cache.get(job_id)
    if cached is not None:
        return cached
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT kind, subject_type, subject_id FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            return {}
        meta = {
            "kind": row["kind"],
            "subject_type": row["subject_type"],
            "subject_id": row["subject_id"],
        }
    except Exception:  # noqa: BLE001
        return {}
    _meta_cache[job_id] = meta
    # Don't let this map grow forever — drop the oldest entries when it
    # crosses a soft cap. 4096 covers any reasonable burst.
    if len(_meta_cache) > 4096:
        for k in list(_meta_cache.keys())[: len(_meta_cache) - 4096]:
            _meta_cache.pop(k, None)
    return meta
