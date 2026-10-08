"""Per-job and per-step capture context.

Public API (re-exported from ``core.jobs``):

- :class:`JobHandle` — methods callers use during a job:
  ``is_cancelled``, ``record_io``, ``record_llm``, ``annotate``,
  ``register_cancel_callback``.
- :func:`current` — recover the active handle from any task or thread.

Framework-only helpers (do NOT call from node code):

- :func:`begin_step` / :func:`end_step` — paired around node execution;
  they own the ``job_steps`` row lifecycle and the per-step ``StepBucket``.
- :class:`StepBucket` — internal accumulator for one step's records.
- :func:`start_handle` / :func:`clear_handle` — runner-only lifecycle
  publishers.

Why a ContextVar:

A ``JobHandle`` is created when a job starts running and published via
a ``ContextVar`` so any code running underneath the handler —
synchronous or async — can find it via :func:`current`. Inside a job,
individual steps (pipeline nodes wrapped by ``timed()``) push a
``StepBucket`` onto a separate step ``ContextVar`` so per-step LLM and
I/O records bucket correctly even with parallel async fan-out.

Threading notes:

- ``asyncio.to_thread`` / ``loop.run_in_executor`` copy the current
  Context, so the handle and step ContextVars propagate into worker
  threads. Mutations to bucket lists are visible to the parent task
  because the lists are shared by reference.
- Raw ``threading.Thread`` does NOT copy context. Code that spawns raw
  threads inside a node must use ``contextvars.copy_context().run(...)``
  or records land nowhere; ``record_llm`` / ``record_io`` log a debug
  message when there's no active bucket.
"""

from __future__ import annotations

import logging
import threading
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Callable

from core import runctx
from core.jobs import events, store
from core.runctx import current  # noqa: F401 — re-exported as the public accessor

log = logging.getLogger(__name__)

_MAX_TEXT = 12000


def _clip(text: str | None, limit: int = _MAX_TEXT) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


@dataclass
class StepBucket:
    """Per-step accumulator. Drained at step end into the job_steps row."""

    step_id: int
    title: str | None = None
    subtitle: str | None = None
    output_text: str | None = None
    llm_calls: list[dict] = field(default_factory=list)
    io_records: list[dict] = field(default_factory=list)


@dataclass
class JobHandle:
    """Public handle for in-flight job state.

    Held in a ContextVar; recovered via :func:`current`. Methods are
    safe to call from any task or thread that inherits the context.
    """

    job_id: int
    kind: str
    cancel_event: threading.Event = field(default_factory=threading.Event)
    cancel_callbacks: list[Callable[[], None]] = field(default_factory=list)

    # ── cancellation ──────────────────────────────────────────

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def cancel(self) -> None:
        """Signal cancellation through three complementary mechanisms.

        Idempotent. Callbacks run synchronously on the caller's thread —
        keep them quick.

        Why three mechanisms (do NOT collapse them):

        1. ``cancel_event.set()`` — the cooperative signal. Picked up by
           ``timed()`` between sync nodes (no await point to inject
           ``CancelledError`` into) so a long sequence of fast sync
           steps still stops promptly.
        2. ``cancel_callbacks`` — for leaf I/O that can't be reached by
           ``Task.cancel()``: subprocesses (AppleScript, scrapers) and
           httpx clients held by ``core/llm.ainvoke``. Closing the
           client mid-stream surfaces a connection error at the await
           point.
        3. ``Task.cancel()`` — happens in :func:`core.jobs.runner.cancel`
           on the asyncio Task. This is what makes awaiting LLM calls
           raise ``CancelledError`` immediately rather than running to
           completion.

        Each layer covers a path the others can't reach. Sync code
        between awaits → mechanism 1. Long-running native handles →
        mechanism 2. Awaiting Python code → mechanism 3.
        """
        if self.cancel_event.is_set():
            return
        self.cancel_event.set()
        for cb in self.cancel_callbacks:
            try:
                cb()
            except Exception:
                log.exception("cancel callback failed for job %s", self.job_id)

    def register_cancel_callback(self, cb: Callable[[], None]) -> None:
        """Register a callback fired on cancel — used by leaf I/O code
        (LLM HTTP clients, subprocesses) to abort in flight."""
        self.cancel_callbacks.append(cb)

    # ── per-step records (called by I/O modules, not nodes) ───

    def record_llm(
        self,
        *,
        prompt: str,
        response: str,
        ms: int,
        model: str | None = None,
        label: str | None = None,
        prompt_variant: str | None = None,
    ) -> None:
        record = {
            "label": label,
            "prompt": _clip(prompt),
            "response": _clip(response),
            "ms": ms,
            "model": model,
            "ts": time.time(),
            "prompt_variant": prompt_variant,
        }
        bucket = _current_step.get()
        if bucket is not None:
            bucket.llm_calls.append(record)
            events.emit(self.job_id, {
                "type": "step.llm",
                "step_id": bucket.step_id,
                "call": record,
            })
        else:
            log.debug(
                "record_llm with no active step (job=%s, label=%s)",
                self.job_id, label,
            )

    def record_io(
        self,
        resource: str,
        mode: str,
        *,
        op: str | None = None,
        rows: int | None = None,
        ms: int | None = None,
        ok: bool = True,
        error: str | None = None,
        **extra: object,
    ) -> None:
        """Append an IO record to the active step's bucket.

        Beyond the standard fields, accepts arbitrary ``**extra`` kwargs
        that get merged into the record dict — used for resource-specific
        annotations (e.g. ``path=...`` for screenshots, ``url=...`` for
        outbound HTTP). The dashboard surfaces unknown keys as a small
        labelled detail row in the IO records section.
        """
        record = {
            "resource": resource,
            "mode": mode,
            "op": op,
            "rows": rows,
            "ms": ms,
            "ok": ok,
            "error": error,
            "ts": time.time(),
        }
        # Drop None-valued extras so we don't fill the JSON with nulls
        # that break the drawer's "if value" rendering checks.
        for k, v in extra.items():
            if v is not None:
                record[k] = v

        bucket = _current_step.get()
        if bucket is not None:
            bucket.io_records.append(record)
            events.emit(self.job_id, {
                "type": "step.io",
                "step_id": bucket.step_id,
                "record": record,
            })
        else:
            log.debug(
                "record_io with no active step (job=%s, resource=%s)",
                self.job_id, resource,
            )

    def record_screenshot(
        self,
        path: str,
        *,
        label: str | None = None,
    ) -> None:
        """Convenience wrapper: emit a ``screenshot`` IO record pointing
        at a file on disk. Path is stored as-is — the dashboard's
        ``GET /api/screenshots/...`` endpoint validates that it's
        rooted under the project's allowed screenshot directory before
        serving."""
        self.record_io(
            "screenshot",
            "write",
            op=label,
            path=path,
        )

    def annotate(
        self,
        *,
        title: str | None = None,
        subtitle: str | None = None,
        output_text: str | None = None,
    ) -> None:
        """Set presentational fields on the active step. No-op if no step."""
        bucket = _current_step.get()
        if bucket is None:
            return
        if title is not None:
            bucket.title = title
        if subtitle is not None:
            bucket.subtitle = subtitle
        if output_text is not None:
            bucket.output_text = output_text
        if title is not None or subtitle is not None:
            # Flush presentational fields live (not just at end_step) so
            # pollers and the SSE stream can show sub-step progress from
            # long-running steps. Telemetry must never break the job.
            try:
                store.annotate_step(bucket.step_id, title=title, subtitle=subtitle)
                events.emit(self.job_id, {
                    "type": "step.annotated",
                    "step_id": bucket.step_id,
                    "title": bucket.title,
                    "subtitle": bucket.subtitle,
                })
            except Exception:  # noqa: BLE001
                pass


# ── module-level ContextVars ──────────────────────────────────
# The active-handle ContextVar lives in the dependency-free leaf
# ``core.runctx`` so domain modules (llm, tasks, calendar, scrapers) can
# read ``current()`` without importing ``core.jobs`` (which would cycle).
# ``current`` is re-exported here so jobs-internal callers and the
# ``core.jobs`` facade keep their existing import.
_current_step: ContextVar[StepBucket | None] = ContextVar(
    "_current_step_bucket", default=None,
)


# ── lifecycle ─────────────────────────────────────────────────

def start_handle(job_id: int, kind: str) -> JobHandle:
    """Publish a fresh JobHandle on the current context.

    Called once by the runner around a handler execution. Returns the
    handle so the runner can call ``cancel()`` from elsewhere.
    """
    handle = JobHandle(job_id=job_id, kind=kind)
    runctx.set_current(handle)
    return handle


def clear_handle() -> None:
    runctx.clear()


# ── per-step lifecycle (used by timed() wrappers) ─────────────

def begin_step(
    name: str,
    *,
    input_text: str | None = None,
    title: str | None = None,
    subtitle: str | None = None,
) -> StepBucket | None:
    """Insert a step row in 'running' state and publish a bucket.

    Returns ``None`` when there's no active job — the wrapper can fall
    through to the legacy in-memory timing path in that case (CLI runs,
    tests).
    """
    handle = current()
    if handle is None:
        return None
    clipped_input = _clip(input_text)
    step_id = store.insert_step(
        handle.job_id, name,
        title=title, subtitle=subtitle,
        input_text=clipped_input,
    )
    bucket = StepBucket(
        step_id=step_id, title=title, subtitle=subtitle,
    )
    _current_step.set(bucket)
    events.emit(handle.job_id, {
        "type": "step.started",
        "step_id": step_id,
        "name": name,
        "title": title,
        "subtitle": subtitle,
        "input_text": clipped_input,
    })
    return bucket


def end_step(
    bucket: StepBucket,
    status: str,
    *,
    ms: int,
    output_text: str | None = None,
    error: str | None = None,
) -> None:
    """Persist a step's final state and clear the step ContextVar."""
    clipped_output = _clip(output_text or bucket.output_text)
    store.finish_step(
        bucket.step_id, status, ms=ms,
        output_text=clipped_output,
        title=bucket.title,
        subtitle=bucket.subtitle,
        llm_calls=bucket.llm_calls,
        io_records=bucket.io_records,
        error=error,
    )
    _current_step.set(None)
    handle = current()
    if handle is not None:
        events.emit(handle.job_id, {
            "type": "step.finished",
            "step_id": bucket.step_id,
            "status": status,
            "ms": ms,
            "output_text": clipped_output,
            "title": bucket.title,
            "subtitle": bucket.subtitle,
            "error": error,
        })
