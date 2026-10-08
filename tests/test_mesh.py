"""orbit_jobs.mesh: the optional NATS link (worker wake-ups, request/reply).

Tests that need a server start a throwaway ``nats-server`` on a free local
port (``brew install nats-server``); everything else runs without one.
"""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time

import pytest

import orbit_jobs
from orbit_jobs import KindDescriptor, config, mesh
from orbit_jobs.store import get, insert

NATS_SERVER = shutil.which("nats-server")
needs_server = pytest.mark.skipif(NATS_SERVER is None, reason="nats-server is not installed")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def nats_url(monkeypatch):
    port = _free_port()
    proc = subprocess.Popen([NATS_SERVER, "-a", "127.0.0.1", "-p", str(port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.05)
    else:
        proc.kill()
        pytest.fail("nats-server did not start")
    url = f"nats://127.0.0.1:{port}"
    monkeypatch.setenv("NATS_URLS", url)
    yield url
    proc.terminate()
    proc.wait(timeout=10)


async def _until(pred, timeout: float = 5.0) -> bool:
    for _ in range(int(timeout / 0.02)):
        if pred():
            return True
        await asyncio.sleep(0.02)
    return pred()


async def _connected() -> None:
    assert await _until(mesh.connected), "mesh never connected"
    await asyncio.sleep(0.2)  # let the subscriptions made on connect reach the server


@pytest.fixture(autouse=True)
def mesh_transport():
    """These tests run orbit_jobs with the mesh as its transport, as a host configures it."""
    config.configure(transport=mesh)
    yield
    mesh._reset()


def test_off_without_nats_urls(monkeypatch):
    monkeypatch.delenv("NATS_URLS", raising=False)

    async def go():
        await mesh.start()
        mesh.publish(mesh.subject("anything"), {"x": 1})   # a silent no-op
        await mesh.stop()

    assert not mesh.enabled()
    asyncio.run(go())
    assert not mesh.connected()


def test_subject_tokens_cannot_change_the_subject_shape(monkeypatch):
    monkeypatch.setenv("NATS_SUBJECT_PREFIX", "test")
    assert mesh.subject("work", "remote-cpu") == "test.work.remote-cpu"
    assert mesh.subject("bus", "a.b *>") == "test.bus.a_b___"


def test_an_unreachable_server_never_blocks_startup(monkeypatch):
    monkeypatch.setenv("NATS_URLS", f"nats://127.0.0.1:{_free_port()}")  # nothing listens

    async def go():
        started = time.monotonic()
        await mesh.start()
        took = time.monotonic() - started
        mesh.publish(mesh.subject("x"), {"dropped": True})
        await asyncio.sleep(0.1)
        connected = mesh.connected()
        await mesh.stop()
        return took, connected

    took, connected = asyncio.run(go())
    assert took < 0.5 and not connected


@pytest.mark.integration
@needs_server
def test_a_request_is_answered_by_one_responder_and_falls_back_without_one(nats_url):
    seen: list[dict] = []

    def double(data: dict) -> dict:
        seen.append(data)
        if data.get("boom"):
            raise RuntimeError("bad input")
        return {"twice": data["n"] * 2}

    async def go():
        await mesh.start()
        await _connected()
        subj = mesh.subject("test", "double")
        t0 = time.monotonic()
        nobody = await asyncio.to_thread(mesh.request_sync, subj, {"n": 1}, 5.0)
        fast_fallback = time.monotonic() - t0
        for key in ("a", "b"):   # two members of one queue group: each request goes to one
            mesh.serve(subj, double, queue="doublers", key=f"serve-test-{key}", workers=2)
        await asyncio.sleep(0.2)
        answers = [await asyncio.to_thread(mesh.request_sync, subj, {"n": n}, 5.0)
                   for n in range(3)]
        failed = await asyncio.to_thread(mesh.request_sync, subj, {"boom": 1}, 5.0)
        on_loop = mesh.request_sync(subj, {"n": 9}, 5.0)
        for key in ("a", "b"):
            mesh.unsubscribe(f"serve-test-{key}")
        await mesh.stop()
        return nobody, fast_fallback, answers, failed, on_loop

    nobody, fast_fallback, answers, failed, on_loop = asyncio.run(go())
    assert nobody is None and fast_fallback < 2.0   # "no responders" comes back at once
    assert answers == [{"twice": 0}, {"twice": 2}, {"twice": 4}]
    assert len(seen) == 4   # once per request, not once per queue member
    assert failed == {"error": "RuntimeError: bad input"}
    assert on_loop is None   # asking from the mesh's own loop would deadlock it
    assert mesh.request_sync("automations.test.double", {"n": 1}, 1.0) is None  # disconnected


@pytest.mark.integration
@needs_server
def test_a_responder_answers_requests_at_once_and_finishes_them_on_stop(nats_url):
    def slow(data: dict) -> dict:
        time.sleep(0.6)
        return {"n": data["n"]}

    async def go():
        await mesh.start()
        await _connected()
        subj = mesh.subject("test", "slow")
        mesh.serve(subj, slow, queue="slowers", key="serve-test-slow", workers=3)
        await asyncio.sleep(0.2)
        t0 = time.monotonic()
        asks = [asyncio.create_task(asyncio.to_thread(mesh.request_sync, subj, {"n": n}, 5.0))
                for n in range(3)]
        await asyncio.sleep(0.2)   # all three in flight
        await mesh.stop()          # drains: they're answered before the connection closes
        answers = await asyncio.gather(*asks)
        return answers, time.monotonic() - t0

    answers, took = asyncio.run(go())
    assert sorted(a["n"] for a in answers) == [0, 1, 2]
    assert took < 1.5   # side by side, not 3 x 0.6 s one after another


@pytest.mark.integration
@needs_server
def test_a_spawned_job_wakes_an_idle_worker_at_once(nats_url, monkeypatch):
    from orbit_jobs import worker as worker_module

    monkeypatch.setattr(worker_module.os, "execv", lambda *a, **k: None)

    async def handler(job_id, payload):
        return {"ok": True}

    orbit_jobs.register_kind(KindDescriptor(kind="test_mesh_wake", label="t", description="t",
                                            handler=handler, capability="test-mesh-cap"))

    async def go():
        stop = asyncio.Event()
        # A 60 s poll interval: only a wake-up can get this job started quickly.
        task = asyncio.create_task(worker_module.run_worker(
            ["test-mesh-cap"], poll_interval=60, stop_event=stop))
        await _connected()
        started = time.monotonic()
        job_id = insert("test_mesh_wake", {}, required_capability="test-mesh-cap")
        done = await _until(lambda: get(job_id)["status"] == "succeeded", timeout=10)
        took = time.monotonic() - started
        stop.set()
        await asyncio.wait_for(task, 10)
        return done, took

    done, took = asyncio.run(go())
    assert done and took < 5
