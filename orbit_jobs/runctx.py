"""Active run context — the ambient telemetry + cancellation handle.

A dependency-free leaf. Domain modules (``llm``, ``tasks``, ``calendar``,
the chat scrapers) record I/O / LLM calls and check for cancellation
against the currently-running job via :func:`current`, WITHOUT importing
``core.jobs`` — which would create a ``domain -> jobs`` import cycle, since
``jobs`` already depends on the domains its kinds orchestrate.

The jobs runtime publishes its ``JobHandle`` here via :func:`set_current`
when a job starts; :func:`current` returns it, typed as the minimal
:class:`RunContext` protocol. Outside a job (CLI, tests, the voice
pipeline) ``current()`` is ``None`` and callers no-op.

Implementation note: ``core.jobs.capture.JobHandle`` is the only concrete
``RunContext``; it satisfies this protocol structurally.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Callable, Protocol, runtime_checkable


@runtime_checkable
class RunContext(Protocol):
    """The surface domain code may call on the active run handle."""

    job_id: int
    kind: str

    def is_cancelled(self) -> bool: ...

    def register_cancel_callback(self, cb: Callable[[], None]) -> None: ...

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
    ) -> None: ...

    def record_llm(
        self,
        *,
        prompt: str,
        response: str,
        ms: int,
        model: str | None = None,
        label: str | None = None,
    ) -> None: ...

    def record_screenshot(self, path: str, *, label: str | None = None) -> None: ...


_current: ContextVar[RunContext | None] = ContextVar("_run_context", default=None)


def current() -> RunContext | None:
    """The active run context for this task/thread, or ``None``."""
    return _current.get()


def set_current(ctx: RunContext | None) -> None:
    """Publish the active run context. Called by the jobs runtime only."""
    _current.set(ctx)


def clear() -> None:
    """Drop the active run context (jobs runtime only)."""
    _current.set(None)
