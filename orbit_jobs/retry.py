"""Opt-in retry-respawn for failed jobs.

A kind opts in via ``KindDescriptor.max_retries``; the worker calls
``maybe_retry`` from its failed (never cancelled) finish path to re-enqueue
a fresh pending row. Lives beside ``worker``/``store`` in the engine layer —
imports nothing above ``core.jobs``.
"""

from __future__ import annotations

from core.jobs.registry import get_descriptor
from core.jobs.store import insert

__all__ = ["maybe_retry"]


def maybe_retry(job: dict, payload: dict) -> int | None:
    """Re-enqueue a failed job when its descriptor opts into retries.

    Called only from the failed (never cancelled) finish path. The
    fresh pending row carries ``retry_count + 1`` — the chain-counted
    budget — and ``parent_job_id`` pointing at the failed row, so
    persistent failures stop at ``max_retries`` and the lineage stays
    visible in the Tickets tree. The failed row itself stays failed.

    Inserts via ``store.insert`` directly (not ``runner.spawn``): the
    original row is already terminal, so ``has_active_for_subject``
    can't conflict, and re-running spawn's descriptor/payload
    validation on a payload that already passed it once is redundant.

    Returns the new job id, or None when no retry applies.
    """
    descriptor = get_descriptor(job["kind"])
    if descriptor is None or descriptor.max_retries <= 0:
        return None
    retry_count = int(job.get("retry_count") or 0)
    if retry_count >= descriptor.max_retries:
        return None
    return insert(
        job["kind"],
        payload,
        subject_type=job.get("subject_type"),
        subject_id=job.get("subject_id"),
        timeout_seconds=job.get("timeout_seconds"),
        parent_job_id=int(job["id"]),
        required_capability=job.get("required_capability"),
        priority=int(job.get("priority") or 0),
        workflow_id=job.get("workflow_id"),
        origin=job.get("origin") or "agent",
        retry_count=retry_count + 1,
    )
