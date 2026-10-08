"""The worker's orphan sweep runs on its own timer, so a worker busy with one long job
still releases the jobs of vanished machines (2026-10-06: a reclaimed spot node's shards
sat as "running" for 3 h while the lab worker ran a training generation)."""

from __future__ import annotations

import asyncio

from orbit_jobs import worker


def test_the_sweep_keeps_running_while_the_worker_is_busy(monkeypatch):
    # Stop once the sweep has fired 3 times — a call count, not a wall-clock
    # window — so this can't flake under machine load (a short sleep might
    # not cover enough intervals; a long one just wastes time either way).
    calls = []
    stop = asyncio.Event()
    monkeypatch.setattr(worker, "_ORPHAN_SWEEP_INTERVAL_SECONDS", 0.01)

    def fake_recover_orphans():
        calls.append(1)
        if len(calls) >= 3:
            stop.set()
        return 0

    monkeypatch.setattr(worker, "recover_orphans", fake_recover_orphans)

    async def main():
        sweeper = asyncio.create_task(worker._sweep_orphans_periodically(stop))
        await asyncio.wait_for(sweeper, timeout=5)

    asyncio.run(main())
    assert len(calls) == 3


def test_the_sweep_survives_a_failing_sweep(monkeypatch):
    calls = []
    stop = asyncio.Event()

    def boom():
        calls.append(1)
        if len(calls) >= 2:
            stop.set()
        raise RuntimeError("db locked")

    monkeypatch.setattr(worker, "_ORPHAN_SWEEP_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(worker, "recover_orphans", boom)

    async def main():
        sweeper = asyncio.create_task(worker._sweep_orphans_periodically(stop))
        await asyncio.wait_for(sweeper, timeout=5)

    asyncio.run(main())
    assert len(calls) == 2
