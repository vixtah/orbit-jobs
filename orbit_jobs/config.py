"""The hooks a composition root sets once, with self-contained defaults.

orbit_jobs never imports its host. Everything host-specific reaches it through
:data:`settings`, filled by :func:`configure` (the automations host does this in
``core.jobs.host``). Unset hooks fall back to defaults that need nothing but the
standard library: a SQLite file of the package's own (``orbit_jobs.db``), an
in-process event bus, no cross-process transport and no-op callbacks.

Hooks:

- ``get_db`` / ``now_iso`` — the connection (used as ``with get_db() as conn``)
  and the write-side clock.
- ``make_bus`` — ``make_bus(name, queue_size=n)`` builds the per-job event bus
  (``emit(event, topic=)``, ``subscribe``, ``firehose``, ``has_subscribers``).
- ``transport`` — cross-process hints: an object with ``subject(*tokens)``,
  ``publish``, ``subscribe``, ``unsubscribe``, ``connected()`` and async
  ``start()`` / ``stop()`` (the host passes ``core.mesh``). Default: none.
- ``on_job_finished(job_id=, kind=, status=, error=)`` — after every terminal
  job (local, in-process or remote), e.g. an activity feed. Errors are swallowed.
- ``reap_children()`` — before a worker execs itself and when it exits.
- ``pause_claiming(capabilities) -> bool`` — a worker skips claiming while True.
- ``on_worker_start(capabilities)`` — in ``worker.main`` after logging is set up.
- ``host_name(hostname) -> str`` / ``excluded_cpus(host) -> list[int]`` — the
  remote worker's machine name and CPUs it must not run on.
- ``on_lease(job, worker_id)`` / ``on_lease_end(job, worker_id)`` — a remote
  job was claimed or renewed (``job`` may be None: nothing claimed) / finished.
- ``code_root`` / ``code_dirs`` — the checkout whose commit (plus an mtime
  digest of ``code_dirs``) is the code version; None means the working directory.
  Set before importing ``orbit_jobs.worker``, which records its startup version.
- ``redact_error(text) -> str`` — applied to every schedule run error before it is
  stored or reported (:mod:`orbit_jobs.schedules`). Default:
  :func:`orbit_jobs.redact.redact` (URL query strings, credentials and token-like
  values removed); a host may pass a stricter one, or ``lambda text: text``.
- ``import_probe`` — module a fresh interpreter must import before a worker
  execs itself (None: no probe). ``worker_module`` — what the exec re-runs
  (``AUTOMATIONS_WORKER_MODULE`` overrides).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

__all__ = ["Settings", "NullTransport", "settings", "configure", "snapshot", "restore"]


class NullTransport:
    """No cross-process transport: workers simply poll."""

    @staticmethod
    def subject(*tokens: Any) -> str:
        return ".".join(str(t) for t in tokens)

    @staticmethod
    def connected() -> bool:
        return False

    @staticmethod
    def publish(subject: str, data: dict) -> None:
        return None

    @staticmethod
    def subscribe(subject: str, handler: Callable, *, key: str | None = None,
                  include_own: bool = False) -> None:
        return None

    @staticmethod
    def unsubscribe(key: str) -> None:
        return None

    @staticmethod
    async def start() -> None:
        return None

    @staticmethod
    async def stop() -> None:
        return None


def _default_get_db():
    from orbit_jobs.db import connect

    return connect()


def _default_now_iso() -> str:
    from orbit_jobs.db import utc_now_iso

    return utc_now_iso()


def _default_make_bus(name: str, *, queue_size: int = 256):
    from orbit_jobs.bus import Bus

    return Bus(name, queue_size=queue_size)


def _default_redact_error(text: str) -> str:
    from orbit_jobs.redact import redact

    return redact(text)


def _short_host_name(host: str) -> str:
    return host.strip().split(".")[0]


def _nothing(*_args: Any, **_kwargs: Any) -> None:
    return None


@dataclass
class Settings:
    get_db: Callable[[], Any] = _default_get_db
    now_iso: Callable[[], str] = _default_now_iso
    make_bus: Callable[..., Any] = _default_make_bus
    transport: Any = NullTransport
    on_job_finished: Callable[..., None] = _nothing
    reap_children: Callable[[], None] = _nothing
    pause_claiming: Callable[[list[str]], bool] = lambda capabilities: False
    on_worker_start: Callable[[list[str]], None] = _nothing
    host_name: Callable[[str], str] = _short_host_name
    excluded_cpus: Callable[[str], list[int]] = lambda host: []
    on_lease: Callable[[dict | None, str], None] = _nothing
    on_lease_end: Callable[[dict, str], None] = _nothing
    code_root: Path | None = None
    code_dirs: tuple[str, ...] = ()
    import_probe: str | None = None
    redact_error: Callable[[str], str] = _default_redact_error
    worker_module: str = "orbit_jobs.worker"


settings = Settings()

_NAMES = frozenset(f.name for f in fields(Settings))


def configure(**hooks: Any) -> Settings:
    """Set hooks by name (unknown names raise TypeError). Composition roots call
    this once at startup; later calls replace only the hooks they name."""
    unknown = set(hooks) - _NAMES
    if unknown:
        raise TypeError(f"unknown orbit_jobs hooks: {sorted(unknown)}")
    for name, value in hooks.items():
        setattr(settings, name, value)
    return settings


def snapshot() -> Settings:
    """A copy of the current hooks, for :func:`restore` (tests)."""
    return replace(settings)


def restore(saved: Settings) -> None:
    for name in _NAMES:
        setattr(settings, name, getattr(saved, name))
