"""A worker stays visibly alive while it runs one long job."""

from __future__ import annotations

import asyncio

import orbit_jobs as jobs
import orbit_jobs.worker as worker_module
from orbit_jobs.registry import KindDescriptor
from orbit_jobs.store import insert
from orbit_jobs.worker import run_worker


def test_worker_heartbeat_refreshes_during_a_long_job(monkeypatch):
    # The respawn sweep keys on worker_heartbeats; a silent busy worker gets a twin.
    running = asyncio.Event()
    pings_while_running: list[str] = []
    real = worker_module.worker_heartbeat

    def spy(worker_id, capability, pid):
        if running.is_set():
            pings_while_running.append(capability)
        return real(worker_id, capability, pid)

    monkeypatch.setattr(worker_module, "worker_heartbeat", spy)
    monkeypatch.setattr(worker_module, "HEARTBEAT_INTERVAL_SECONDS", 0.05)

    async def handler(job_id: int, payload: dict):
        running.set()
        await asyncio.sleep(0.4)
        running.clear()
        return {"ok": True}

    jobs.register_kind(KindDescriptor(
        kind="test_worker_liveness", label="t", description="t", handler=handler,
    ))
    insert("test_worker_liveness", {}, required_capability="test-liveness-cap")

    async def drive():
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_worker(["test-liveness-cap"], poll_interval=0.02, stop_event=stop)
        )
        for _ in range(100):
            if len(pings_while_running) >= 2:
                break
            await asyncio.sleep(0.02)
        stop.set()
        await asyncio.wait_for(task, timeout=3.0)

    asyncio.run(drive())
    assert len(pings_while_running) >= 2
    assert set(pings_while_running) == {"test-liveness-cap"}
