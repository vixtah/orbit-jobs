"""Descriptor-based registry for job kinds.

Every async unit of work in this codebase must be a ``KindDescriptor``
registered here. The descriptor declares everything the rest of the
system needs to know about the kind without poking at handler-private
state:

- ``label`` — short human name shown in Tickets cards (e.g. ``"Sync"``).
- ``description`` — one-line help string (rendered in /api/jobs/kinds).
- ``handler`` — the async coroutine that executes the work.
- ``capability`` — worker pool that handles this kind. Static string
  (``"ollama"``, ``"db"``, ``"applescript"``) OR a template like
  ``"scraper-{source}"`` resolved per-payload.
- ``describe_payload`` — kind-specific summary used by the Tickets UI to
  populate the workflow head with concrete numbers (count, target, etc).
- ``payload_schema`` — optional pydantic model for spawn-time validation.
- ``protocol`` — version of the kind's contract with a remote worker (payload
  in, result out). Remote workers advertise theirs and only claim jobs whose
  kind matches the server's, so they keep working across unrelated commits.

This is the choke point that makes "how does this kind look in the UI"
a registration-time question, not a per-spawn-call question. See
the host's CLAUDE.md → "Async work goes through core.jobs" for the full policy.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

JobHandler = Callable[[int, dict[str, Any]], Awaitable[dict[str, Any] | None]]
PayloadDescriber = Callable[[dict[str, Any]], dict[str, Any]]


def _empty_describer(_payload: dict[str, Any]) -> dict[str, Any]:
    return {}


@dataclass(frozen=True)
class KindDescriptor:
    """Self-describing record for one job kind."""

    kind: str
    label: str
    description: str
    handler: JobHandler
    capability: str | None = None
    describe_payload: PayloadDescriber = field(default=_empty_describer)
    payload_schema: Any = None  # optional pydantic BaseModel subclass
    # Fallback timeout for ``spawn()`` when the caller doesn't pass one.
    # Without this, an unbounded handler can wedge a worker indefinitely
    # (e.g. a dead-socketed ollama call). ``None`` = no timeout, matching
    # prior behavior. Set per-kind to bound the longest reasonable run.
    default_timeout_seconds: int | None = None
    # Opt-in automatic retry on failure (never on user cancel). When a
    # job fails and its row's ``retry_count`` is below this, the worker
    # re-enqueues a fresh pending row (retry_count + 1, parent linked to
    # the failed job). 0 = no retries, matching prior behavior. Only set
    # this on idempotent kinds — never anything that sends/creates.
    max_retries: int = 0
    # Bump when the payload, the result or anything the server does with the
    # result changes, so remote workers on older code stop claiming the kind
    # (they log it and restart once their checkout moves). Only remote-*
    # kinds use it today.
    protocol: int = 1

    def resolve_capability(self, payload: dict[str, Any]) -> str | None:
        """Expand a capability template against the spawn payload.

        ``"scraper-{source}"`` + ``{"source": "imsg"}`` → ``"scraper-imsg"``.
        Static strings pass through. ``None`` means "run in-process".
        """
        if self.capability is None:
            return None
        if "{" not in self.capability:
            return self.capability
        try:
            return self.capability.format(**payload)
        except (KeyError, IndexError, ValueError) as err:
            raise ValueError(
                f"capability template {self.capability!r} for kind"
                f" {self.kind!r} can't be resolved against payload: {err}"
            ) from err


_REGISTRY: dict[str, KindDescriptor] = {}


def register_kind(descriptor: KindDescriptor) -> None:
    """Register a kind. Re-registering the same kind replaces the prior
    descriptor (idempotent across worker module reloads)."""
    if not isinstance(descriptor, KindDescriptor):
        raise TypeError(
            "register_kind requires a KindDescriptor; the (kind, handler) "
            "form is no longer supported. See orbit_jobs/registry.py."
        )
    _REGISTRY[descriptor.kind] = descriptor


def get_descriptor(kind: str) -> KindDescriptor | None:
    return _REGISTRY.get(kind)


def get_handler(kind: str) -> JobHandler | None:
    """Backward-compat shim for callers that just want the coroutine."""
    desc = _REGISTRY.get(kind)
    return desc.handler if desc is not None else None


def list_descriptors() -> list[KindDescriptor]:
    return [_REGISTRY[k] for k in sorted(_REGISTRY)]


def list_kinds() -> list[str]:
    return sorted(_REGISTRY.keys())
