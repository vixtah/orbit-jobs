"""Default in-process pub/sub behind the per-job event stream (``orbit_jobs.events``).

Fans plain ``dict`` events out to bounded subscriber queues keyed by a topic (a
job id), plus a firehose of ``(topic, event)`` pairs. A slow subscriber whose
queue fills loses events (warned once); callers recover from their snapshot.
Process-local: a host that wants events to cross processes configures its own
bus with the same interface (``configure(make_bus=...)``).
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, AsyncIterator

log = logging.getLogger(__name__)

__all__ = ["Bus"]


class Bus:
    def __init__(self, name: str, *, queue_size: int = 256) -> None:
        self.name = name
        self._queue_size = max(16, queue_size)
        self._topics: dict[Any, set[asyncio.Queue]] = defaultdict(set)
        self._firehose: set[asyncio.Queue] = set()
        self._warned: set[int] = set()

    def emit(self, event: dict, *, topic: Any) -> None:
        """Fan ``event`` out to ``topic`` subscribers and the firehose. Non-blocking."""
        for q in list(self._topics.get(topic, ())):
            self._offer(q, event, topic, warn=True)
        for q in list(self._firehose):
            self._offer(q, (topic, event), topic, warn=False)

    def _offer(self, q: asyncio.Queue, item: Any, topic: Any, *, warn: bool) -> None:
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            if warn and id(q) not in self._warned:
                self._warned.add(id(q))
                ev = item[1] if isinstance(item, tuple) else item
                log.warning("%s bus: subscriber queue full for topic %r (dropping %s)",
                            self.name, topic, ev.get("type"))

    async def subscribe(self, topic: Any, *, initial: dict | None = None,
                        idle_timeout: float | None = None,
                        idle_event: dict | None = None) -> AsyncIterator[dict]:
        """Yield events emitted to ``topic`` until the caller cancels. The queue is
        registered before ``initial`` is yielded, so nothing emitted in response
        to the snapshot is missed."""
        q: asyncio.Queue = asyncio.Queue(maxsize=self._queue_size)
        self._topics[topic].add(q)
        try:
            if initial is not None:
                yield initial
            async for ev in self._drain(q, idle_timeout, idle_event):
                yield ev
        finally:
            self._topics[topic].discard(q)
            if not self._topics[topic]:
                self._topics.pop(topic, None)
            self._warned.discard(id(q))

    async def firehose(self, *, idle_timeout: float | None = None,
                       idle_event: tuple | None = None) -> AsyncIterator[tuple[Any, dict]]:
        """Yield ``(topic, event)`` for every emit, across all topics."""
        q: asyncio.Queue = asyncio.Queue(maxsize=self._queue_size)
        self._firehose.add(q)
        try:
            async for ev in self._drain(q, idle_timeout, idle_event):
                yield ev
        finally:
            self._firehose.discard(q)
            self._warned.discard(id(q))

    async def _drain(self, q: asyncio.Queue, idle_timeout: float | None,
                     idle_event: Any) -> AsyncIterator[Any]:
        while True:
            if idle_timeout is None:
                yield await q.get()
            else:
                try:
                    yield await asyncio.wait_for(q.get(), timeout=idle_timeout)
                except asyncio.TimeoutError:
                    yield idle_event

    def has_subscribers(self, topic: Any) -> bool:
        return bool(self._topics.get(topic))
