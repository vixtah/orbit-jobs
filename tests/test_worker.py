"""Tests for the Phase 1 capability worker runtime."""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone

import orbit_jobs as jobs
import orbit_jobs.worker as worker_module
import pytest
from orbit_jobs.db import get_db
from orbit_jobs.store import (
    cancel_requested,
    claim_one,
    delete_job,
    heartbeat,
    insert,
    mark_terminal,
    recover_orphans,
    request_cancel,
)
from orbit_jobs.worker import run_worker


@pytest.fixture(autouse=True)
def _never_exec_the_test_process(monkeypatch):
    """Several tests here drive ``run_worker`` in-process, and its idle loop
    can decide the code changed and call ``os.execv`` — which replaces the
    *pytest* process, killing the run with exit 2, a truncated log and no
    failure summary (it looks like a catastrophic regression and is not).

    Neutralise the process-replacing call for every test in this file.
    Tests that assert exec behaviour stub it again themselves, which wins
    because their monkeypatch runs after this fixture.
    """
    monkeypatch.setattr(worker_module.os, "execv", lambda *a, **k: None)
    # Each test starts with the code check due (it's throttled per process).
    monkeypatch.setattr(worker_module, "_LAST_CODE_CHECK", float("-inf"))


def _row_status(job_id: int) -> dict:
    with get_db() as conn:
        return dict(
            conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        )


def _delete(*ids: int) -> None:
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    with get_db() as conn:
        conn.execute(f"DELETE FROM jobs WHERE id IN ({placeholders})", ids)


# ── 1. claim_one is atomic and capability-scoped ──────────────────────

def test_claim_one_picks_priority_then_age_for_matching_capability():
    """Two pending rows: one is high-priority but newer; the other is
    low-priority but older. Claim should pick the high-priority one
    first; a second claim should pick the older low-priority row."""
    older_low = insert(
        "noop_kind", {"x": 1},
        required_capability="db", priority=0,
    )
    newer_high = insert(
        "noop_kind", {"x": 2},
        required_capability="db", priority=10,
    )
    other_cap = insert(
        "noop_kind", {"x": 3},
        required_capability="ollama", priority=99,
    )

    try:
        first = claim_one(["db"], "worker-A")
        assert first is not None
        assert first["id"] == newer_high
        assert first["status"] == "running"
        assert first["worker_id"] == "worker-A"
        assert first["last_heartbeat_at"] is not None

        second = claim_one(["db"], "worker-A")
        assert second is not None
        assert second["id"] == older_low

        # Empty queue for db now — should return None.
        third = claim_one(["db"], "worker-A")
        assert third is None

        # The ollama-capability row should NOT have been touched.
        assert _row_status(other_cap)["status"] == "pending"
    finally:
        _delete(older_low, newer_high, other_cap)


def test_claim_one_skips_capabilities_not_in_list():
    job_id = insert(
        "noop_kind", {}, required_capability="ollama",
    )
    try:
        # A worker advertising only 'db' should not see the ollama row.
        assert claim_one(["db"], "worker-A") is None
        # A worker with both should claim it.
        claimed = claim_one(["db", "ollama"], "worker-A")
        assert claimed is not None
        assert claimed["id"] == job_id
    finally:
        _delete(job_id)


def test_claim_one_two_workers_cannot_grab_same_row():
    """Atomic claim — two workers calling claim_one back-to-back never
    both see the same pending row."""
    job_id = insert("noop_kind", {}, required_capability="db")
    try:
        a = claim_one(["db"], "worker-A")
        b = claim_one(["db"], "worker-B")
        assert a is not None and a["id"] == job_id
        assert b is None, "second claim must not see the same row"
    finally:
        _delete(job_id)


# ── 2. heartbeat refreshes last_heartbeat_at ──────────────────────────

def test_heartbeat_only_updates_running_rows():
    job_id = insert("noop_kind", {}, required_capability="db")
    try:
        # Pending row: heartbeat is a no-op (the row is not running yet).
        heartbeat(job_id)
        assert _row_status(job_id)["last_heartbeat_at"] is None

        # Claim → row goes running with an initial heartbeat.
        claim_one(["db"], "worker-A")
        first_hb = _row_status(job_id)["last_heartbeat_at"]
        assert first_hb is not None

        # Wait a tick so the timestamp changes, then call heartbeat.
        import time as _time
        _time.sleep(0.01)
        heartbeat(job_id)
        second_hb = _row_status(job_id)["last_heartbeat_at"]
        assert second_hb >= first_hb  # never goes backwards
    finally:
        _delete(job_id)


# ── 3. recover_orphans uses heartbeat freshness ───────────────────────

def test_recover_orphans_sweeps_stale_workers_only():
    """Two running rows: one with a fresh heartbeat (alive worker), one
    with a stale heartbeat (dead worker). Recovery should sweep only
    the stale one."""
    fresh = insert("noop_kind", {}, required_capability="db")
    stale = insert("noop_kind", {}, required_capability="db")
    try:
        claim_one(["db"], "fresh-worker")
        # Move the second row into a 'running' state with an old heartbeat.
        ancient = (
            datetime.now(timezone.utc) - timedelta(seconds=300)
        ).isoformat()
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET status = 'running', worker_id = 'dead-worker',"
                " started_at = ?, last_heartbeat_at = ?, updated_at = ?"
                " WHERE id = ?",
                (ancient, ancient, ancient, stale),
            )

        # claim_one above only ran for `fresh`; we need to re-check
        # which id ended up claimed (the ordering may be either way
        # since both are pending+priority 0 — we set `stale` to running
        # via raw SQL above so claim_one targeted `fresh` first).
        # But to keep the test deterministic, we'll explicitly check
        # both rows by id.

        swept = recover_orphans(stale_after_seconds=60)
        assert swept == 1, f"expected 1 sweep, got {swept}"
        assert _row_status(fresh)["status"] == "running"
        assert _row_status(stale)["status"] == "cancelled"
        assert "worker died" in (_row_status(stale)["error"] or "")
    finally:
        _delete(fresh, stale)


def test_recover_orphans_ignores_legacy_capability_null_rows():
    """Capability-NULL rows belong to the legacy in-process path —
    cancel_orphans handles those separately. recover_orphans must
    leave them alone even when running with no heartbeat."""
    legacy = insert("noop_kind", {})  # no capability
    try:
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET status = 'running' WHERE id = ?", (legacy,),
            )
        swept = recover_orphans()
        assert swept == 0
        assert _row_status(legacy)["status"] == "running"
    finally:
        _delete(legacy)


# ── 4. run_worker drains the queue end-to-end ─────────────────────────

def test_run_worker_drains_and_dispatches():
    """A worker advertising 'test-cap' should claim and execute a job
    whose required_capability matches, leaving the row succeeded."""
    received: list[tuple[int, dict]] = []

    async def handler(job_id: int, payload: dict):
        received.append((job_id, payload))
        return {"ok": True, "got": payload}

    from orbit_jobs.registry import KindDescriptor
    jobs.register_kind(KindDescriptor(
        kind="test_worker_kind", label="t", description="t", handler=handler,
    ))

    job_id = insert(
        "test_worker_kind",
        {"hello": "world"},
        required_capability="test-cap",
    )

    try:
        async def drain():
            stop = asyncio.Event()
            # Run the worker for a short window — long enough to claim
            # + execute one quick job, then signal stop.
            task = asyncio.create_task(
                run_worker(["test-cap"], poll_interval=0.05, stop_event=stop)
            )
            # Poll for the job to finish.
            for _ in range(50):
                row = _row_status(job_id)
                if row["status"] in ("succeeded", "failed"):
                    break
                await asyncio.sleep(0.05)
            stop.set()
            await asyncio.wait_for(task, timeout=2.0)

        asyncio.run(drain())

        row = _row_status(job_id)
        assert row["status"] == "succeeded", row
        assert json.loads(row["result"]) == {"ok": True, "got": {"hello": "world"}}
        assert received == [(job_id, {"hello": "world"})]
    finally:
        _delete(job_id)


# ── 5. cross-process cancel ───────────────────────────────────────────

def test_request_cancel_pending_flips_directly():
    """A pending capability-routed job has no owning worker, so the
    cancel request flips the row to cancelled in place."""
    job_id = insert("noop_kind", {}, required_capability="db")
    try:
        assert request_cancel(job_id) == "cancelled"
        row = _row_status(job_id)
        assert row["status"] == "cancelled"
        assert row["error"] == "cancelled"
        # No mailbox row for a directly-flipped job.
        assert not cancel_requested(job_id)
        # Idempotent: a second request on the now-terminal job is a no-op.
        assert request_cancel(job_id) is None
    finally:
        _delete(job_id)


def test_delete_job_refuses_active_and_deletes_terminal_with_steps():
    pending = insert("noop_kind", {}, required_capability="db")
    done = insert("noop_kind", {}, required_capability="db")
    try:
        assert delete_job(999999) == "missing"
        assert delete_job(pending) == "active"

        mark_terminal(done, "succeeded", result={"ok": True})
        with get_db() as conn:
            conn.execute(
                "INSERT INTO job_steps (job_id, name, status, started_at)"
                " VALUES (?, 'step', 'ok', ?)",
                (done, datetime.now(timezone.utc).isoformat()),
            )

        assert delete_job(done) == "deleted"
        with get_db() as conn:
            assert conn.execute(
                "SELECT 1 FROM jobs WHERE id = ?", (done,),
            ).fetchone() is None
            assert conn.execute(
                "SELECT 1 FROM job_steps WHERE job_id = ?", (done,),
            ).fetchone() is None
    finally:
        _delete(pending, done)


def test_request_cancel_running_records_request_and_terminal_consumes_it():
    job_id = insert("noop_kind", {}, required_capability="db")
    try:
        claim_one(["db"], "worker-A")  # row goes running
        assert request_cancel(job_id) == "requested"
        assert cancel_requested(job_id)
        # The worker finishing the job (any terminal status) consumes
        # the request so the mailbox never accumulates stale rows.
        mark_terminal(job_id, "cancelled", error="cancelled")
        assert not cancel_requested(job_id)
        assert request_cancel(job_id) is None
    finally:
        _delete(job_id)


def test_run_worker_cancels_handler_on_cross_process_request(monkeypatch):
    """The worker's heartbeat loop polls job_cancel_requests and cancels
    the in-flight handler task; the job lands cancelled and the worker
    itself stays alive to drain the next job."""
    monkeypatch.setattr(
        "orbit_jobs.worker.HEARTBEAT_INTERVAL_SECONDS", 0.05,
    )

    started = asyncio.Event()
    second_ran: list[int] = []

    async def slow_handler(job_id: int, payload: dict):
        started.set()
        await asyncio.sleep(60)  # cancelled long before this completes
        return {"ok": True}

    async def quick_handler(job_id: int, payload: dict):
        second_ran.append(job_id)
        return {"ok": True}

    from orbit_jobs.registry import KindDescriptor
    jobs.register_kind(KindDescriptor(
        kind="test_worker_slow", label="t", description="t",
        handler=slow_handler,
    ))
    jobs.register_kind(KindDescriptor(
        kind="test_worker_quick", label="t", description="t",
        handler=quick_handler,
    ))

    slow_id = insert(
        "test_worker_slow", {}, required_capability="test-cancel-cap",
    )
    quick_id = None

    try:
        async def drive():
            nonlocal quick_id
            stop = asyncio.Event()
            task = asyncio.create_task(
                run_worker(
                    ["test-cancel-cap"], poll_interval=0.05, stop_event=stop,
                )
            )
            await asyncio.wait_for(started.wait(), timeout=5.0)
            assert request_cancel(slow_id) == "requested"
            for _ in range(100):
                if _row_status(slow_id)["status"] == "cancelled":
                    break
                await asyncio.sleep(0.05)
            # Worker must survive a user cancel: it should claim and
            # finish a fresh job afterwards.
            quick_id = insert(
                "test_worker_quick", {},
                required_capability="test-cancel-cap",
            )
            for _ in range(100):
                if _row_status(quick_id)["status"] == "succeeded":
                    break
                await asyncio.sleep(0.05)
            stop.set()
            await asyncio.wait_for(task, timeout=2.0)

        asyncio.run(drive())

        slow_row = _row_status(slow_id)
        assert slow_row["status"] == "cancelled", slow_row
        assert slow_row["error"] == "cancelled"
        assert not cancel_requested(slow_id)  # mailbox consumed
        assert _row_status(quick_id)["status"] == "succeeded"
        assert second_ran == [quick_id]
    finally:
        ids = [slow_id] + ([quick_id] if quick_id else [])
        _delete(*ids)


# ── 6. import health check gates every exec/restart trigger ──────────

def test_exec_self_execs_when_import_health_check_passes(monkeypatch):
    execd: list[list[str]] = []
    monkeypatch.setattr(worker_module, "_import_health_ok", lambda: True)
    monkeypatch.setattr(worker_module.config.settings, "reap_children", lambda: None)
    monkeypatch.setattr(worker_module.os, "execv", lambda *a: execd.append(list(a)))

    worker_module._exec_self(reason="test")

    assert execd, "expected execv to run when the import health check passes"


def test_exec_self_defers_when_import_health_check_fails(monkeypatch):
    """The shared exec funnel (_exec_self) must never exec when the
    checkout can't import, regardless of which caller triggered it."""
    execd: list[list[str]] = []
    monkeypatch.setattr(worker_module, "_import_health_ok", lambda: False)
    monkeypatch.setattr(worker_module.os, "execv", lambda *a: execd.append(list(a)))

    worker_module._exec_self(reason="test")

    assert execd == [], "os.execv must not run when the import health check fails"


def test_maybe_exec_for_new_code_defers_when_import_broken(monkeypatch):
    """Drift-detection path: a version change with a broken checkout
    must defer through the shared gate rather than exec."""
    monkeypatch.setattr(worker_module, "_STARTUP_TS", 0.0)
    monkeypatch.setattr(worker_module, "current_code_version", lambda: "deadbeef")
    monkeypatch.setattr(worker_module, "_import_health_ok", lambda: False)
    execd: list[list[str]] = []
    monkeypatch.setattr(worker_module.os, "execv", lambda *a: execd.append(list(a)))

    worker_module._maybe_exec_for_new_code()

    assert execd == []


def test_git_failure_does_not_read_as_code_drift(monkeypatch):
    """Regression (found 2026-09-22): a transient ``git rev-parse`` failure
    used to bounce every worker, and aborted a full pytest run by exec'ing
    the test process away.

    The fallback returned a bare digest while ``_STARTUP_VERSION`` held
    ``<head>+<digest>``, so the two could never be equal and the idle loop
    read a git hiccup as new code. The fallback is now tagged, and versions
    of different shapes are treated as not comparable.
    """
    import subprocess as sp

    def timeout(*_args, **_kwargs):
        raise sp.TimeoutExpired("git rev-parse HEAD", 2)

    startup = worker_module.current_code_version()
    assert "+" in startup and not startup.startswith("nogit+")

    monkeypatch.setattr(worker_module.subprocess, "check_output", timeout)
    fallback = worker_module.current_code_version()
    assert fallback.startswith("nogit+"), fallback
    assert not worker_module._comparable(startup, fallback)

    monkeypatch.setattr(worker_module, "_STARTUP_TS", 0.0)
    monkeypatch.setattr(worker_module, "_STARTUP_VERSION", startup)
    monkeypatch.setattr(worker_module, "_import_health_ok", lambda: True)
    execd: list[list[str]] = []
    monkeypatch.setattr(worker_module.os, "execv", lambda *a: execd.append(list(a)))

    worker_module._maybe_exec_for_new_code()

    assert execd == [], "a git hiccup must not restart the worker"


def test_real_code_drift_still_restarts(monkeypatch):
    """The guard above must not disable drift detection itself."""
    monkeypatch.setattr(worker_module, "_STARTUP_TS", 0.0)
    monkeypatch.setattr(worker_module, "_STARTUP_VERSION", "aaaaaaaaaaaa+1111")
    monkeypatch.setattr(worker_module, "current_code_version", lambda: "bbbbbbbbbbbb+2222")
    monkeypatch.setattr(worker_module, "_import_health_ok", lambda: True)
    execd: list[list[str]] = []
    monkeypatch.setattr(worker_module.os, "execv", lambda *a: execd.append(list(a)))

    worker_module._maybe_exec_for_new_code()

    assert execd, "a genuine commit change must still restart the worker"


def test_dirty_job_exec_path_blocked_by_import_health_check(monkeypatch):
    """Regression for the 2026-08-31 outage: the post-dirty-job restart
    trigger (``EXEC_AFTER_DIRTY_JOB``) called ``os.execv`` directly with
    no import check of its own — only ``_maybe_exec_for_new_code``
    (the drift-detection path) checked. That let a drift restart which
    deferred at 00:57:52 get forced through moments later at 00:59:04 via
    a dirty job hitting this second, unchecked path. A job that ends
    dirty (here: times out) with a broken checkout must NOT exec — the
    worker keeps running the current in-memory code and stays alive to
    claim more work."""
    monkeypatch.setattr(worker_module, "EXEC_AFTER_DIRTY_JOB", True)
    monkeypatch.setattr(worker_module, "_import_health_ok", lambda: False)
    execd: list[list[str]] = []
    monkeypatch.setattr(worker_module.os, "execv", lambda *a: execd.append(list(a)))

    async def hanging_handler(job_id: int, payload: dict):
        await asyncio.sleep(60)

    from orbit_jobs.registry import KindDescriptor
    jobs.register_kind(KindDescriptor(
        kind="test_worker_dirty_gate", label="t", description="t",
        handler=hanging_handler,
    ))
    job_id = insert(
        "test_worker_dirty_gate", {}, None, None, 0.05,
        required_capability="test-dirty-gate-cap",
    )

    try:
        async def drive():
            stop = asyncio.Event()
            task = asyncio.create_task(
                run_worker(
                    ["test-dirty-gate-cap"], poll_interval=0.05, stop_event=stop,
                )
            )
            for _ in range(100):
                if _row_status(job_id)["status"] == "failed":
                    break
                await asyncio.sleep(0.05)
            # Give the post-dirty-job exec trigger a beat to (not) fire.
            await asyncio.sleep(0.2)
            stop.set()
            await asyncio.wait_for(task, timeout=2.0)

        asyncio.run(drive())

        assert execd == [], "os.execv must not run when the import health check fails"
        row = _row_status(job_id)
        assert row["status"] == "failed"
        assert "timeout" in (row["error"] or "")
    finally:
        _delete(job_id)


def test_run_worker_marks_failed_on_handler_exception():
    async def handler(job_id: int, payload: dict):
        raise RuntimeError("boom")

    from orbit_jobs.registry import KindDescriptor
    jobs.register_kind(KindDescriptor(
        kind="test_worker_failing", label="t", description="t", handler=handler,
    ))
    job_id = insert(
        "test_worker_failing", {}, required_capability="test-fail-cap",
    )
    try:
        async def drain():
            stop = asyncio.Event()
            task = asyncio.create_task(
                run_worker(["test-fail-cap"], poll_interval=0.05, stop_event=stop)
            )
            for _ in range(50):
                row = _row_status(job_id)
                if row["status"] in ("succeeded", "failed", "cancelled"):
                    break
                await asyncio.sleep(0.05)
            stop.set()
            await asyncio.wait_for(task, timeout=2.0)

        asyncio.run(drain())

        row = _row_status(job_id)
        assert row["status"] == "failed", row
        assert "boom" in (row["error"] or "")
    finally:
        _delete(job_id)


# ── control-plane sqlite off the event loop (story control-plane-hardening) ──


def test_worker_heartbeat_and_claim_one_run_off_event_loop_thread(monkeypatch):
    """``run_worker``'s poll tick must call ``worker_heartbeat`` and
    ``claim_one`` via ``asyncio.to_thread`` — not directly on the loop —
    so a contended sqlite write can't block HTTP/SSE for up to the
    busy_timeout. Monkeypatch both store calls to record which thread
    ran them and assert it's never the loop's own thread."""
    main_thread = threading.current_thread()
    seen_threads: list[threading.Thread] = []

    def fake_worker_heartbeat(worker_id, capability, pid):
        seen_threads.append(threading.current_thread())

    def fake_claim_one(capabilities, worker_id):
        seen_threads.append(threading.current_thread())
        return None

    monkeypatch.setattr(worker_module, "worker_heartbeat", fake_worker_heartbeat)
    monkeypatch.setattr(worker_module, "claim_one", fake_claim_one)
    # Avoid touching the real orphan-recovery path for this thread-identity
    # assertion — it's covered separately, and stubbing it keeps this test
    # focused on the two named call sites.
    monkeypatch.setattr(worker_module, "recover_orphans", lambda *a, **k: 0)

    async def drive():
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_worker(["test-thread-cap"], poll_interval=0.02, stop_event=stop)
        )
        for _ in range(50):
            if len(seen_threads) >= 2:
                break
            await asyncio.sleep(0.02)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

    asyncio.run(drive())

    assert len(seen_threads) >= 2, "expected both worker_heartbeat and claim_one to run"
    for t in seen_threads:
        assert t is not main_thread, (
            "worker_heartbeat/claim_one ran on the event-loop thread — "
            "expected asyncio.to_thread to offload them"
        )


def test_heartbeat_and_cancel_requested_run_off_event_loop_thread(monkeypatch):
    """The per-job heartbeat ticker's ``heartbeat`` and ``cancel_requested``
    calls must likewise run via ``asyncio.to_thread``."""
    main_thread = threading.current_thread()
    seen_threads: list[threading.Thread] = []

    real_heartbeat = worker_module.heartbeat
    real_cancel_requested = worker_module.cancel_requested

    def spying_heartbeat(job_id):
        seen_threads.append(threading.current_thread())
        return real_heartbeat(job_id)

    def spying_cancel_requested(job_id):
        seen_threads.append(threading.current_thread())
        return real_cancel_requested(job_id)

    monkeypatch.setattr(worker_module, "heartbeat", spying_heartbeat)
    monkeypatch.setattr(worker_module, "cancel_requested", spying_cancel_requested)

    async def handler(job_id: int, payload: dict):
        # Live long enough for at least one heartbeat tick to fire —
        # ``_heartbeat_loop`` ticks every HEARTBEAT_INTERVAL_SECONDS, so
        # shrink that window for the test.
        await asyncio.sleep(0.3)
        return {"ok": True}

    from orbit_jobs.registry import KindDescriptor
    jobs.register_kind(KindDescriptor(
        kind="test_worker_hb_thread", label="t", description="t", handler=handler,
    ))
    job_id = insert(
        "test_worker_hb_thread", {}, required_capability="test-hb-thread-cap",
    )

    monkeypatch.setattr(worker_module, "HEARTBEAT_INTERVAL_SECONDS", 0.05)

    try:
        async def drive():
            stop = asyncio.Event()
            task = asyncio.create_task(
                run_worker(
                    ["test-hb-thread-cap"], poll_interval=0.02, stop_event=stop,
                )
            )
            for _ in range(100):
                if len(seen_threads) >= 2:
                    break
                await asyncio.sleep(0.02)
            stop.set()
            await asyncio.wait_for(task, timeout=3.0)

        asyncio.run(drive())

        assert len(seen_threads) >= 2, "expected heartbeat + cancel_requested to fire"
        for t in seen_threads:
            assert t is not main_thread, (
                "heartbeat/cancel_requested ran on the event-loop thread — "
                "expected asyncio.to_thread to offload them"
            )
    finally:
        _delete(job_id)


def test_code_drift_check_runs_at_most_every_interval(monkeypatch):
    """The idle loop calls this every tick; the git + file-stat check itself
    runs at most once per CODE_CHECK_INTERVAL_SECONDS."""
    calls = []
    monkeypatch.setattr(worker_module, "_STARTUP_TS", 0.0)
    monkeypatch.setattr(worker_module, "_STARTUP_VERSION", "aaaaaaaaaaaa+1111")
    monkeypatch.setattr(worker_module, "current_code_version",
                        lambda: calls.append(1) or "aaaaaaaaaaaa+1111")
    clock = [1000.0]
    monkeypatch.setattr(worker_module.time, "monotonic", lambda: clock[0])
    for _ in range(5):
        worker_module._maybe_exec_for_new_code()
        clock[0] += 1.0
    assert len(calls) == 1
    clock[0] += worker_module.CODE_CHECK_INTERVAL_SECONDS
    worker_module._maybe_exec_for_new_code()
    assert len(calls) == 2
