"""Instant "work is waiting" hints, so idle workers claim new jobs at once.

Every pending row is created by ``store.insert``, which calls ``notify`` with
the row's capability. Workers ``listen`` on their capabilities and ``idle``
until a hint arrives, ``stop_event`` is set, or their usual poll interval
passes. The hint carries no job: the worker still claims through the atomic
``claim_one``, so a lost or duplicate hint only changes how soon a job starts.

Transport is ``core.mesh`` (subjects ``<prefix>.work.<capability>``); without
``NATS_URLS`` nothing is sent and workers simply poll as before.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from core import mesh

__all__ = ["notify", "listen", "unlisten", "idle"]


def notify(capability: str | None) -> None:
    """Tell workers of ``capability`` that a job is waiting."""
    if capability:
        mesh.publish(mesh.subject("work", capability), {"capability": capability})


def listen(capabilities: Iterable[str], event: asyncio.Event, *, key: str) -> None:
    """Set ``event`` whenever a hint for one of ``capabilities`` arrives.

    ``include_own``: a server or test process may spawn and run in one process.
    """
    for capability in capabilities:
        mesh.subscribe(mesh.subject("work", capability), lambda _data, _origin: event.set(),
                       key=f"{key}:{capability}", include_own=True)


def unlisten(capabilities: Iterable[str], *, key: str) -> None:
    for capability in capabilities:
        mesh.unsubscribe(f"{key}:{capability}")


async def idle(stop_event: asyncio.Event, wake_event: asyncio.Event, timeout: float) -> None:
    """Return when either event is set or ``timeout`` seconds pass."""
    waiters = [asyncio.ensure_future(stop_event.wait()), asyncio.ensure_future(wake_event.wait())]
    try:
        await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()
