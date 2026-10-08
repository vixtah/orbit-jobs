"""Tests for two job-system behaviors added together:

1. Timeout-derived pending TTL — ``clear_superseded_jobs`` expires a
   never-claimed pending row at ``max(1h, 10x timeout_seconds)`` instead
   of the flat 2-day cutoff, so a down worker stops blocking
   same-subject re-spawns within the hour. Rows without a timeout keep
   the flat ``pending_max_age_seconds`` fallback.

2. Opt-in retry — a kind registered with ``max_retries=N`` gets a fresh
   pending row (retry_count + 1, parent-linked) when a run fails, never
   when the user cancels, and the chain stops at N.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from core import jobs
from core.db import get_db
from core.jobs import store as _store
from core.jobs.registry import KindDescriptor
from core.jobs.store import insert, request_cancel
from core.jobs.worker import run_worker


def _age_job(job_id: int, *, seconds: float) -> None:
    """Backdate a job's created_at by ``seconds``."""
    ts = (
        datetime.now(timezone.utc) - timedelta(seconds=seconds)
    ).isoformat()
    with get_db() as conn:
        conn.execute(
            "UPDATE jobs SET created_at = ? WHERE id = ?", (ts, job_id),
        )


def _rows_for_kind(kind: str) -> list[dict]:
    with get_db() as conn:
        return [
            dict(r) for r in conn.execute(
                "SELECT * FROM jobs WHERE kind = ? ORDER BY id", (kind,),
            )
        ]


# ── 1. timeout-derived pending TTL ────────────────────────────────────


def test_pending_with_short_timeout_expires_after_floor():
    """timeout=60s → clamp = max(1h, 10x60s) = 1h. A 2h-old pending row
    expires long before the flat 2-day fallback would have caught it."""
    job_id = insert(
        "classify_thread", {}, subject_type="thread", subject_id="imsg:ttl1",
        timeout_seconds=60, required_capability="ghost-cap",
    )
    _age_job(job_id, seconds=2 * 3600)
    assert _store.has_active_for_subject("thread", "imsg:ttl1")

    result = _store.clear_superseded_jobs()

    assert result["expired_pending"] == 1
    job = _store.get(job_id)
    assert job["status"] == "cancelled"
    assert "expired" in job["error"]
    assert not _store.has_active_for_subject("thread", "imsg:ttl1")


def test_pending_with_short_timeout_protected_by_floor():
    """timeout=60s means 10x timeout is only 10 min, but the 1h floor
    keeps a 30-min-old row alive (worker might be briefly down)."""
    job_id = insert(
        "classify_thread", {}, subject_type="thread", subject_id="imsg:ttl2",
        timeout_seconds=60, required_capability="ghost-cap",
    )
    _age_job(job_id, seconds=30 * 60)

    result = _store.clear_superseded_jobs()

    assert result["expired_pending"] == 0
    assert _store.get(job_id)["status"] == "pending"


def test_pending_with_long_timeout_uses_multiplier():
    """timeout=3600s → clamp = 10h. Kept at 2h, expired at 11h."""
    job_id = insert(
        "classify_thread", {}, subject_type="thread", subject_id="imsg:ttl3",
        timeout_seconds=3600, required_capability="ghost-cap",
    )
    _age_job(job_id, seconds=2 * 3600)
    assert _store.clear_superseded_jobs()["expired_pending"] == 0
    assert _store.get(job_id)["status"] == "pending"

    _age_job(job_id, seconds=11 * 3600)
    assert _store.clear_superseded_jobs()["expired_pending"] == 1
    assert _store.get(job_id)["status"] == "cancelled"


def test_pending_without_timeout_keeps_flat_fallback():
    """No timeout_seconds → the flat 2-day cutoff applies, not the 1h
    floor: a 1.5-day-old row survives, a 3-day-old row expires."""
    young = insert(
        "digest_run", {}, subject_type="skill", subject_id="ttl4",
        required_capability="ghost-cap",
    )
    old = insert(
        "digest_run", {}, subject_type="skill", subject_id="ttl5",
        required_capability="ghost-cap",
    )
    _age_job(young, seconds=int(1.5 * 24 * 3600))
    _age_job(old, seconds=3 * 24 * 3600)

    result = _store.clear_superseded_jobs()

    assert result["expired_pending"] == 1
    assert _store.get(young)["status"] == "pending"
    assert _store.get(old)["status"] == "cancelled"


# ── 2. opt-in retry on failure ────────────────────────────────────────


def test_failed_job_retries_once_then_stops():
    """A max_retries=1 kind that always fails: the first failure spawns
    exactly one linked pending retry; the retry's failure spawns
    nothing (chain-counted budget)."""
    attempts: list[int] = []

    async def failing_handler(job_id: int, payload: dict):
        attempts.append(job_id)
        raise RuntimeError("boom")

    jobs.register_kind(KindDescriptor(
        kind="test_retry_fail", label="t", description="t",
        handler=failing_handler, max_retries=1,
    ))

    first_id = insert(
        "test_retry_fail", {"thread": "x"},
        subject_type="thread", subject_id="imsg:retry1",
        timeout_seconds=120, required_capability="test-retry-cap",
    )

    async def drive():
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_worker(["test-retry-cap"], poll_interval=0.05, stop_event=stop)
        )
        # Wait for both the original and its retry to land failed, failing
        # immediately (not after a guessed wall-clock window) if a third
        # row ever appears.
        for _ in range(100):
            rows = _rows_for_kind("test_retry_fail")
            assert len(rows) <= 2, rows
            if len(rows) == 2 and all(r["status"] == "failed" for r in rows):
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("worker never finished the retry chain")
        # A few extra idle polls, asserted on each tick rather than slept
        # through blind, so an (incorrect) second retry would be caught the
        # moment it's claimed instead of only at the final assertion below.
        for _ in range(6):
            await asyncio.sleep(0.05)
            assert len(_rows_for_kind("test_retry_fail")) <= 2
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

    asyncio.run(drive())

    rows = _rows_for_kind("test_retry_fail")
    assert len(rows) == 2, rows
    original, retry = rows
    assert original["id"] == first_id
    assert original["status"] == "failed"
    assert original["retry_count"] == 0

    assert retry["status"] == "failed"
    assert retry["retry_count"] == 1
    assert retry["parent_job_id"] == first_id
    # The re-insert copies identity/routing and skips subject-dedup —
    # same subject as the (terminal) failed row, same payload/capability.
    assert retry["subject_type"] == "thread"
    assert retry["subject_id"] == "imsg:retry1"
    assert retry["payload"] == '{"thread": "x"}'
    assert retry["required_capability"] == "test-retry-cap"
    assert retry["timeout_seconds"] == 120
    assert len(attempts) == 2


def test_cancelled_job_never_retries(monkeypatch):
    """A user-cancelled job spawns no retry row even when its kind opts
    into retries."""
    monkeypatch.setattr("core.jobs.worker.HEARTBEAT_INTERVAL_SECONDS", 0.05)

    started = asyncio.Event()

    async def slow_handler(job_id: int, payload: dict):
        started.set()
        await asyncio.sleep(60)
        return {"ok": True}

    jobs.register_kind(KindDescriptor(
        kind="test_retry_cancel", label="t", description="t",
        handler=slow_handler, max_retries=1,
    ))

    job_id = insert(
        "test_retry_cancel", {}, subject_type="thread",
        subject_id="imsg:retry2", required_capability="test-retry-cancel-cap",
    )

    async def drive():
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_worker(
                ["test-retry-cancel-cap"], poll_interval=0.05,
                stop_event=stop,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=5.0)
        assert request_cancel(job_id) == "requested"
        for _ in range(100):
            rows = _rows_for_kind("test_retry_cancel")
            assert len(rows) == 1, rows  # a retry would be a second row
            if rows[0]["status"] == "cancelled":
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("job never landed cancelled")
        # A few extra idle polls, asserted on each tick instead of slept
        # through blind, so a wrong retry would be caught as it's claimed.
        for _ in range(6):
            await asyncio.sleep(0.05)
            assert len(_rows_for_kind("test_retry_cancel")) == 1
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

    asyncio.run(drive())

    rows = _rows_for_kind("test_retry_cancel")
    assert len(rows) == 1, rows
    assert rows[0]["status"] == "cancelled"
