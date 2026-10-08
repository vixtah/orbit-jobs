"""Optional NATS link between processes and machines: the transport for job wake-ups
(``orbit_jobs.configure(transport=orbit_jobs.mesh)``), and for anything else a host
bridges over it (an event bus, request/reply).

A leaf: it imports nothing from orbit_jobs or a host, so the event bus, the job
store and the workers can all use it. Off unless ``NATS_URLS`` is set; every
function is then a cheap no-op and the process behaves exactly as without it.
Needs ``nats-py`` (extra ``mesh``) only when ``NATS_URLS`` is set.

NATS here is an accelerator, never a source of truth. Messages are
fire-and-forget hints (a live event, "work is waiting"); anything that must
not be lost lives in a database, and every consumer keeps its polling path.
So a dropped message, a NATS restart or a machine with no NATS at all only
makes things slower, never wrong.

Interface:

* ``start()`` / ``stop()`` — once per process event loop (the server
  lifespan, a worker's ``run_worker``, a remote worker's ``run``). ``start``
  never blocks: it connects in the background and keeps retrying.
* ``publish(subject, data)`` — sync, thread-safe, non-blocking. Drops the
  message when not connected.
* ``subscribe(subject, handler, key=...)`` — declarative: recorded now,
  applied whenever a connection comes up. ``handler(data, origin)`` may be
  sync or async. Messages this process published are skipped unless
  ``include_own``.
* ``unsubscribe(key)`` — drops a recorded subscription.
* ``serve(subject, handler, queue=..., workers=N)`` — answer requests: one member of the
  queue group gets each request; ``handler(data) -> dict`` runs on a pool of ``workers``
  threads (CPU-bound work never blocks the connection) and its dict is the reply.
* ``request_sync(subject, data, timeout)`` — ask and wait, from any thread but the mesh's
  own loop. None when not connected, nobody answers, or the wait times out: callers
  always have a local way to do the work.
* ``subject(*tokens)`` — builds ``<prefix>.<token>…`` with tokens made safe.

Config: ``NATS_URLS`` (comma-separated, e.g. ``nats://100.64.0.1:4222``),
``NATS_USER`` / ``NATS_PASSWORD`` (optional), ``NATS_SUBJECT_PREFIX``
(default ``automations``; separate deployments or tests use their own).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import socket
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

__all__ = [
    "enabled",
    "origin",
    "subject",
    "publish",
    "subscribe",
    "unsubscribe",
    "serve",
    "request_sync",
    "start",
    "stop",
    "connected",
]

Handler = Callable[[dict, str], Any]

RECONNECT_SECONDS = 2.0
OUTBOX_SIZE = 4096

_ORIGIN = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"


@dataclass
class _Sub:
    subject: str
    handler: Handler
    include_own: bool
    live: Any = None  # the nats Subscription while connected
    queue: str = ""  # queue group: one member gets each message
    pool: Any = None  # ThreadPoolExecutor for a responder (serve); None for plain handlers
    inflight: set = field(default_factory=set)  # a responder's requests being answered


@dataclass
class _State:
    loop: asyncio.AbstractEventLoop | None = None
    nc: Any = None
    outbox: asyncio.Queue | None = None
    tasks: list[asyncio.Task] = field(default_factory=list)
    oversize_warned: set[str] = field(default_factory=set)
    pending: int = 0  # messages queued or in flight; stop() waits for them


_subs: dict[str, _Sub] = {}
_state = _State()


def _urls() -> list[str]:
    return [u.strip() for u in os.environ.get("NATS_URLS", "").split(",") if u.strip()]


def enabled() -> bool:
    return bool(_urls())


def origin() -> str:
    """This process's id, stamped on every message it publishes."""
    return _ORIGIN


def subject(*tokens: Any) -> str:
    """``subject("work", "remote-cpu")`` → ``automations.work.remote-cpu``.

    Dots, spaces and wildcards inside a token become ``_`` so a capability or
    bus name can never change the subject's shape.
    """
    prefix = os.environ.get("NATS_SUBJECT_PREFIX", "automations")
    safe = ["".join("_" if ch in ". *>\t\r\n" else ch for ch in str(t)) or "_" for t in tokens]
    return ".".join([prefix, *safe])


def connected() -> bool:
    return _state.nc is not None and bool(getattr(_state.nc, "is_connected", False))


# -- publish ---------------------------------------------------------------

def publish(subject: str, data: dict) -> None:
    """Queue ``data`` for ``subject``. Never blocks, never raises."""
    loop, outbox = _state.loop, _state.outbox
    if loop is None or outbox is None or loop.is_closed():
        return
    try:
        payload = json.dumps({"origin": _ORIGIN, "data": data}, default=str).encode()
    except (TypeError, ValueError):
        log.debug("mesh: unserialisable message for %s dropped", subject, exc_info=True)
        return
    try:
        loop.call_soon_threadsafe(_offer, outbox, (subject, payload))
    except RuntimeError:  # loop closed between the check and the call
        pass


def _offer(outbox: asyncio.Queue, item: tuple[str, bytes]) -> None:
    try:
        outbox.put_nowait(item)
        _state.pending += 1
    except asyncio.QueueFull:
        log.debug("mesh: outbox full, dropping a message for %s", item[0])


async def _pump(outbox: asyncio.Queue) -> None:
    while True:
        item = await outbox.get()
        try:
            await _send(*item)
        finally:
            _state.pending -= 1


async def _send(subj: str, payload: bytes) -> None:
    nc = _state.nc
    if nc is None or not nc.is_connected:
        return  # fire-and-forget: the database is the record
    limit = getattr(nc, "max_payload", 0) or 0
    if limit and len(payload) > limit:
        if subj not in _state.oversize_warned:
            _state.oversize_warned.add(subj)
            log.warning("mesh: %d-byte message for %s exceeds the server's %d-byte "
                        "limit; dropping (warned once per subject)", len(payload), subj, limit)
        return
    try:
        await nc.publish(subj, payload)
    except Exception:  # noqa: BLE001 — a publish failure must never surface to callers
        log.debug("mesh: publish to %s failed", subj, exc_info=True)


# -- subscribe -------------------------------------------------------------

def subscribe(subject: str, handler: Handler, *, key: str | None = None,
              include_own: bool = False) -> None:
    """Record a subscription; it goes live on every (re)connect.

    ``key`` names the subscription so registering again replaces it (a bus
    or worker re-created in tests doesn't stack handlers). Defaults to the
    subject.
    """
    key = key or subject
    old = _subs.get(key)
    sub = _Sub(subject, handler, include_own)
    _subs[key] = sub
    _schedule(_resubscribe(old, sub))


def unsubscribe(key: str) -> None:
    """Forget the subscription recorded under ``key`` (no-op if unknown)."""
    old = _subs.pop(key, None)
    if old is not None:
        if old.pool is not None:
            old.pool.shutdown(wait=False, cancel_futures=True)
        _schedule(_resubscribe(old, None))


def _schedule(coro) -> None:
    loop = _state.loop
    if loop is None or loop.is_closed() or not connected():
        coro.close()
        return

    def create() -> None:
        _state.tasks.append(loop.create_task(coro))

    loop.call_soon_threadsafe(create)


async def _resubscribe(old: _Sub | None, new: _Sub | None) -> None:
    if old is not None and old.live is not None and old.live is not _ATTACHING:
        try:
            await old.live.unsubscribe()
        except Exception:  # noqa: BLE001
            pass
        old.live = None
    if new is not None:
        await _attach(new)


_ATTACHING = object()  # placeholder while a subscribe call is in flight


async def _attach(sub: _Sub) -> None:
    nc = _state.nc
    if nc is None or sub.live is not None:
        return  # not connected, or already attached / attaching (no double delivery)
    sub.live = _ATTACHING

    async def on_msg(msg) -> None:
        try:
            envelope = json.loads(msg.data)
        except (ValueError, TypeError):
            return
        sender = envelope.get("origin", "")
        if sub.pool is not None:
            # The client runs a subscription's callbacks one at a time: answering here would
            # serialise requests whatever the pool size. Answer on a task (stop() waits).
            task = asyncio.ensure_future(_respond(sub, msg, envelope.get("data") or {}))
            sub.inflight.add(task)
            task.add_done_callback(sub.inflight.discard)
            return
        if sender == _ORIGIN and not sub.include_own:
            return
        try:
            out = sub.handler(envelope.get("data") or {}, sender)
            if inspect.isawaitable(out):
                await out
        except Exception:  # noqa: BLE001
            log.exception("mesh: handler for %s failed", sub.subject)

    try:
        sub.live = await nc.subscribe(sub.subject, queue=sub.queue, cb=on_msg)
    except Exception:  # noqa: BLE001
        sub.live = None
        log.warning("mesh: subscribe to %s failed", sub.subject, exc_info=True)


async def _respond(sub: _Sub, msg, data: dict) -> None:
    """Run a responder's handler off the loop and send its dict back."""
    if not msg.reply:
        return
    loop = asyncio.get_running_loop()
    try:
        reply = await loop.run_in_executor(sub.pool, sub.handler, data)
    except Exception as exc:  # noqa: BLE001 — the asker falls back to doing it itself
        log.exception("mesh: responder for %s failed", sub.subject)
        reply = {"error": f"{type(exc).__name__}: {exc}"}
    try:
        await msg.respond(json.dumps(reply, default=str).encode())
    except Exception:  # noqa: BLE001
        log.debug("mesh: reply on %s failed", sub.subject, exc_info=True)


def serve(subject: str, handler: Callable[[dict], dict], *, queue: str,
          key: str | None = None, workers: int = 1) -> None:
    """Answer requests on ``subject`` as one member of ``queue`` (see the module doc)."""
    from concurrent.futures import ThreadPoolExecutor

    key = key or f"serve:{subject}"
    old = _subs.get(key)
    if old is not None and old.pool is not None:
        old.pool.shutdown(wait=False, cancel_futures=True)
    sub = _Sub(subject, handler, include_own=True, queue=queue,
               pool=ThreadPoolExecutor(max_workers=max(1, workers),
                                       thread_name_prefix=f"mesh-serve-{queue}"))
    _subs[key] = sub
    _schedule(_resubscribe(old, sub))


def request_sync(subject: str, data: dict, timeout: float) -> dict | None:
    """Ask once and wait up to ``timeout`` seconds for one answer (see the module doc)."""
    loop, nc = _state.loop, _state.nc
    if loop is None or nc is None or loop.is_closed() or not connected():
        return None
    try:
        if asyncio.get_running_loop() is loop:
            log.warning("mesh: request_sync called on the mesh loop; use a thread")
            return None
    except RuntimeError:
        pass  # no loop in this thread: the expected case
    payload = json.dumps({"origin": _ORIGIN, "data": data}, default=str).encode()
    future = asyncio.run_coroutine_threadsafe(nc.request(subject, payload, timeout=timeout),
                                              loop)
    try:
        msg = future.result(timeout + 1.0)
        return json.loads(msg.data)
    except Exception as exc:  # noqa: BLE001 — no responders, timeout, bad reply: do it locally
        future.cancel()
        log.debug("mesh: request on %s got no answer (%s)", subject, type(exc).__name__)
        return None


# -- lifecycle -------------------------------------------------------------

async def start() -> None:
    """Begin connecting on the running loop (no-op when disabled)."""
    if not enabled():
        return
    loop = asyncio.get_running_loop()
    if _state.loop is loop and _state.tasks:
        return
    if _state.loop is not None and _state.loop is not loop:
        _reset()  # a previous loop (an earlier asyncio.run) is gone
    _state.loop = loop
    _state.outbox = asyncio.Queue(maxsize=OUTBOX_SIZE)
    # Transport tasks, not work (allowlisted in tests/test_async_through_jobs.py).
    _state.tasks = [
        loop.create_task(_pump(_state.outbox), name="mesh-pump"),
        loop.create_task(_connect_forever(), name="mesh-connect"),
    ]


async def _connect_forever() -> None:
    import nats  # imported here so a process without NATS_URLS never loads it

    async def on_disconnect() -> None:
        log.warning("mesh: disconnected from NATS; retrying")

    async def on_reconnect() -> None:
        log.info("mesh: reconnected to NATS")

    async def on_error(exc: Exception) -> None:
        log.debug("mesh: NATS error: %s", exc)

    options: dict[str, Any] = {
        "servers": _urls(),
        "name": _ORIGIN,
        "allow_reconnect": True,
        "max_reconnect_attempts": -1,
        "reconnect_time_wait": RECONNECT_SECONDS,
        "connect_timeout": 5,
        "disconnected_cb": on_disconnect,
        "reconnected_cb": on_reconnect,
        "error_cb": on_error,
    }
    if os.environ.get("NATS_USER"):
        options["user"] = os.environ["NATS_USER"]
        options["password"] = os.environ.get("NATS_PASSWORD", "")
    warned = False
    while True:
        try:
            nc = await nats.connect(**options)
            break
        except Exception as exc:  # noqa: BLE001 — keep the app running without NATS
            if not warned:
                log.warning("mesh: NATS unreachable at %s (%s); running without it and "
                            "retrying every %.0fs", ",".join(_urls()), exc, RECONNECT_SECONDS)
                warned = True
            await asyncio.sleep(RECONNECT_SECONDS)
    _state.nc = nc
    log.info("mesh: connected to NATS at %s as %s", nc.connected_url.netloc, _ORIGIN)
    # nats-py restores live subscriptions after a reconnect by itself.
    for sub in list(_subs.values()):
        await _attach(sub)


RESPOND_GRACE = 60.0  # seconds stop() waits for requests being answered


async def stop() -> None:
    """Stop taking requests, answer the ones in flight (up to ``RESPOND_GRACE`` s), send
    what's queued (up to ~1 s) and close the connection."""
    tasks, nc = _state.tasks, _state.nc
    responders = [sub for sub in _subs.values() if sub.pool is not None]
    for sub in responders:
        if sub.live is not None and sub.live is not _ATTACHING:
            try:  # stop delivery; requests already delivered still start (inflight)
                await sub.live.drain()
            except Exception:  # noqa: BLE001
                pass
            sub.live = None
    inflight = [t for sub in responders for t in sub.inflight]
    if inflight:
        await asyncio.wait(inflight, timeout=RESPOND_GRACE)
    if nc is not None:
        await asyncio.sleep(0)  # let publishes scheduled just before stop() enqueue
        for _ in range(100):
            if _state.pending <= 0:
                break
            await asyncio.sleep(0.01)
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    if nc is not None:
        try:
            await nc.drain()
        except Exception:  # noqa: BLE001
            pass
    _reset()


def _reset() -> None:
    for sub in _subs.values():
        sub.live = None
    _state.loop = _state.nc = _state.outbox = None
    _state.tasks = []
    _state.pending = 0
    _state.oversize_warned.clear()
