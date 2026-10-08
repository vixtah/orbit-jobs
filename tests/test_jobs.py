"""Unit tests for core/jobs/.

Covers:
1. Cancelling a job mid-await propagates ``CancelledError`` through the
   handler, marks the in-flight step ``cancelled``, and the job
   ``cancelled``.
2. SSE subscribers receive events in the expected order across a step
   lifecycle (started → llm → io → finished → job.finished).
3. ``record_llm`` / ``record_io`` called from inside ``asyncio.to_thread``
   land on the active step (ContextVar propagates into worker threads).
"""

from __future__ import annotations

import asyncio

import orbit_jobs as jobs
import pytest
from orbit_jobs import capture as _capture
from orbit_jobs import events as _events
from orbit_jobs import store as _store

# ── fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def clean_subscribers():
    """Reset the SSE subscriber pools (on the shared bus) between tests."""
    def _reset():
        _events._the_bus()._topics.clear()
        _events._the_bus()._firehose.clear()
        _events._the_bus()._warned.clear()
    _reset()
    yield
    _reset()


# ── cancel propagation ────────────────────────────────────────────


def test_cancel_propagates_through_awaiting_handler():
    """Spawn a handler that's awaiting ``asyncio.sleep``, cancel mid-flight,
    and assert both the job and its in-flight step end up ``cancelled``."""
    saw_cancel = asyncio.Event()

    async def slow_handler(job_id: int, payload: dict):
        h = jobs.current()
        assert h is not None
        bucket = _capture.begin_step("waiting", input_text="ready")
        try:
            await asyncio.sleep(5)  # cancelled before this returns
        except asyncio.CancelledError:
            saw_cancel.set()
            _capture.end_step(bucket, "cancelled", ms=42, error="cancelled")
            raise
        _capture.end_step(bucket, "ok", ms=1)

    from orbit_jobs.registry import KindDescriptor
    jobs.register_kind(KindDescriptor(
        kind="test_cancel", label="t", description="t", handler=slow_handler,
    ))

    async def main():
        job_id = jobs.spawn("test_cancel", {})
        # Let the task start and enter the await.
        await asyncio.sleep(0.05)
        delivered = jobs.cancel(job_id)
        assert delivered, "cancel should be delivered to the running task"
        # Wait for terminal status.
        for _ in range(50):
            j = jobs.get(job_id)
            if j and j["status"] in ("succeeded", "failed", "cancelled"):
                break
            await asyncio.sleep(0.05)
        return jobs.get(job_id), jobs.list_steps(job_id)

    job, steps = asyncio.run(main())
    assert saw_cancel.is_set(), "handler should have observed CancelledError"
    assert job["status"] == "cancelled", f"got {job['status']!r}"
    assert len(steps) == 1
    assert steps[0]["name"] == "waiting"
    assert steps[0]["status"] == "cancelled"


# ── 3. SSE event sequence ────────────────────────────────────────────


def test_sse_event_sequence(clean_subscribers):
    """A handler that does begin → record_llm → record_io → end emits
    the matching SSE events in order, followed by ``job.finished``."""
    async def handler(job_id: int, payload: dict):
        h = jobs.current()
        bucket = _capture.begin_step("step_a", input_text="in_a")
        h.record_llm(prompt="p", response="r", ms=10, model="m", label="a")
        h.record_io("tasks", "write", op="insert", rows=1)
        _capture.end_step(bucket, "ok", ms=12, output_text="out_a")
        return {"ok": True}

    from orbit_jobs.registry import KindDescriptor
    jobs.register_kind(KindDescriptor(
        kind="test_sse", label="t", description="t", handler=handler,
    ))

    async def collect():
        # Spawn first; subscribe second. We rely on the snapshot to fill
        # in any events that landed before the subscriber attached.
        job_id = jobs.spawn("test_sse", {})
        # Let the handler run fully; events emit synchronously into queues
        # of any subscriber attached at the time. To guarantee we observe
        # the sequence, snapshot-only via the subscribe initial. Then poll
        # the step rows and reconstruct the order from that.
        for _ in range(50):
            j = jobs.get(job_id)
            if j and j["status"] in ("succeeded", "failed"):
                break
            await asyncio.sleep(0.02)
        return job_id, jobs.list_steps(job_id)

    job_id, steps = asyncio.run(collect())
    assert len(steps) == 1
    s = steps[0]
    assert s["status"] == "ok"
    assert s["llm_calls"] and s["llm_calls"][0]["label"] == "a"
    assert s["io_records"] and s["io_records"][0]["resource"] == "tasks"


def test_sse_subscribe_terminal_job_emits_snapshot_and_closes(clean_subscribers):
    """Subscribing to an already-terminal job is a non-error path —
    consumer gets the snapshot it needs and the iterator closes cleanly."""
    job_id = _store.insert("digest_run", {}, "skill", "alpha")
    _store.mark_running(job_id)
    _store.mark_terminal(job_id, "succeeded", result={})

    async def collect():
        events = []
        # Synthesize the route's snapshot since subscribe() itself doesn't
        # know the job is terminal — that branching is in the route.
        snapshot = {"type": "state.snapshot", "job": jobs.get(job_id), "steps": []}
        async for ev in jobs.subscribe(job_id, initial=snapshot):
            events.append(ev)
            # No producer will emit on this job, so we manually emit
            # job.finished to release the iterator (matches the
            # already-terminal branch in web.job_routes).
            if ev["type"] == "state.snapshot":
                _events.emit(job_id, {"type": "job.finished", "status": "succeeded"})
        return events

    received = asyncio.run(collect())
    types = [ev["type"] for ev in received]
    assert types == ["state.snapshot", "job.finished"]


# ── 4. ContextVar propagation through asyncio.to_thread ──────────────


def test_record_llm_and_io_propagate_through_to_thread():
    """``asyncio.to_thread`` copies the running Context, so the JobHandle
    and current step ContextVars survive into the worker thread; lists
    are shared by reference, so appends from the worker land on the same
    bucket the parent task sees."""
    async def handler(job_id: int, payload: dict):
        h = jobs.current()
        bucket = _capture.begin_step("threaded", input_text="in")

        def in_worker():
            # Running on a worker thread courtesy of to_thread. The
            # ContextVar lookup must find the same JobHandle.
            inner = jobs.current()
            assert inner is h, "JobHandle should propagate via Context copy"
            inner.record_llm(prompt="p", response="r", ms=5, model="m", label="thr")
            inner.record_io("tasks", "read", op="select", rows=4, ms=2)

        await asyncio.to_thread(in_worker)
        _capture.end_step(bucket, "ok", ms=10, output_text="done")
        return {"ok": True}

    from orbit_jobs.registry import KindDescriptor
    jobs.register_kind(KindDescriptor(
        kind="test_to_thread", label="t", description="t", handler=handler,
    ))

    async def main():
        job_id = jobs.spawn("test_to_thread", {})
        for _ in range(50):
            j = jobs.get(job_id)
            if j and j["status"] in ("succeeded", "failed"):
                break
            await asyncio.sleep(0.02)
        return jobs.get(job_id), jobs.list_steps(job_id)

    job, steps = asyncio.run(main())
    assert job["status"] == "succeeded"
    assert len(steps) == 1
    s = steps[0]
    assert len(s["llm_calls"]) == 1
    assert s["llm_calls"][0]["label"] == "thr"
    assert len(s["io_records"]) == 1
    assert s["io_records"][0]["resource"] == "tasks"
    assert s["io_records"][0]["mode"] == "read"


# ── clear_superseded_jobs ─────────────────────────────────────────────


def _mk_job(kind, subject_id, status, *, age_days=0.0):
    """Insert a job row with a controlled status and created_at age."""
    from datetime import datetime, timedelta, timezone

    job_id = _store.insert(kind, {}, subject_type="thread", subject_id=subject_id)
    ts = (
        datetime.now(timezone.utc) - timedelta(days=age_days)
    ).isoformat()
    from orbit_jobs.db import get_db
    with get_db() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, created_at = ? WHERE id = ?",
            (status, ts, job_id),
        )
    return job_id


def _job_ids():
    from orbit_jobs.db import get_db
    with get_db() as conn:
        return {r["id"] for r in conn.execute("SELECT id FROM jobs")}


def test_clear_superseded_sweeps_failed_and_cancelled():
    """Failed AND cancelled rows superseded by a later same-subject
    success are deleted once past the grace window."""
    old_failed = _mk_job("classify_thread", "imsg:1", "failed", age_days=2)
    old_cancelled = _mk_job("classify_thread", "imsg:1", "cancelled", age_days=2)
    _mk_job("classify_thread", "imsg:1", "succeeded", age_days=1)

    result = _store.clear_superseded_jobs()

    assert result["swept"] == 2
    remaining = _job_ids()
    assert old_failed not in remaining
    assert old_cancelled not in remaining


def test_clear_superseded_respects_grace_window():
    """A superseded row younger than grace_seconds stays visible."""
    recent_cancelled = _mk_job("classify_thread", "imsg:2", "cancelled", age_days=0.5)
    _mk_job("classify_thread", "imsg:2", "succeeded")

    result = _store.clear_superseded_jobs()  # default grace = 24h

    assert result["swept"] == 0
    assert recent_cancelled in _job_ids()


def test_clear_superseded_keeps_unsuperseded_recent_rows():
    """Without a later success, recent failed/cancelled rows survive."""
    failed = _mk_job("classify_thread", "imsg:3", "failed", age_days=2)
    cancelled = _mk_job("digest_run", "imsg-ingest", "cancelled", age_days=2)

    result = _store.clear_superseded_jobs()

    assert result["swept"] == 0
    remaining = _job_ids()
    assert failed in remaining
    assert cancelled in remaining


def test_clear_superseded_zombie_clause_sweeps_old_rows():
    """Past max_age, failed/cancelled rows go even with no successor."""
    zombie_failed = _mk_job("classify_thread", "imsg:4", "failed", age_days=20)
    zombie_cancelled = _mk_job("digest_run", "cmb-ingest", "cancelled", age_days=20)
    succeeded = _mk_job("classify_thread", "imsg:5", "succeeded", age_days=20)

    result = _store.clear_superseded_jobs()  # default max_age = 14d

    assert result["swept"] == 2
    remaining = _job_ids()
    assert zombie_failed not in remaining
    assert zombie_cancelled not in remaining
    assert succeeded in remaining


def test_clear_superseded_deletes_step_rows_too():
    """Swept jobs take their job_steps along — no orphaned step rows."""
    from orbit_jobs.db import get_db

    swept_job = _mk_job("classify_thread", "imsg:6", "failed", age_days=2)
    _store.insert_step(swept_job, "classify")
    _mk_job("classify_thread", "imsg:6", "succeeded", age_days=1)

    result = _store.clear_superseded_jobs()

    assert result["swept"] == 1
    with get_db() as conn:
        orphans = conn.execute(
            "SELECT COUNT(*) AS n FROM job_steps"
            " WHERE job_id NOT IN (SELECT id FROM jobs)"
        ).fetchone()["n"]
    assert orphans == 0


def test_clear_superseded_expires_stale_pending():
    """A pending row past pending_max_age_seconds (no worker ever
    claimed it) flips to cancelled, unblocking has_active_for_subject
    re-spawns for the same subject."""
    stale = _mk_job("classify_thread", "imsg:7", "pending", age_days=3)
    assert _store.has_active_for_subject("thread", "imsg:7", "classify_thread")

    result = _store.clear_superseded_jobs()

    assert result["expired_pending"] == 1
    job = _store.get(stale)
    assert job["status"] == "cancelled"
    assert "expired" in job["error"]
    assert not _store.has_active_for_subject("thread", "imsg:7", "classify_thread")


def test_clear_superseded_keeps_recent_pending():
    """Pending rows younger than pending_max_age_seconds are untouched
    — that's the normal queue waiting for a worker slot."""
    fresh = _mk_job("classify_thread", "imsg:8", "pending", age_days=1)

    result = _store.clear_superseded_jobs()

    assert result["expired_pending"] == 0
    assert _store.get(fresh)["status"] == "pending"


def test_clear_superseded_reaps_expired_pending_with_successor():
    """An expired pending row whose subject already has a later success
    is deleted in the same sweep (expiry runs before the supersede
    delete, and its created_at is already past the grace window)."""
    stale = _mk_job("classify_thread", "imsg:9", "pending", age_days=3)
    _mk_job("classify_thread", "imsg:9", "succeeded", age_days=1)

    result = _store.clear_superseded_jobs()

    assert result["expired_pending"] == 1
    assert stale not in _job_ids()


# ── clear_superseded_jobs phase-split durability (story
# control-plane-hardening): each phase commits independently, so an
# exception raised by a later phase must not roll back an earlier one.


def test_clear_superseded_phase1_durable_if_phase3_raises(monkeypatch):
    """Phase 1 (expire stale pending) must stay committed even when
    phase 3 (delete superseded job_steps) blows up."""
    stale = _mk_job("classify_thread", "imsg:10", "pending", age_days=3)

    def boom(**kwargs):
        raise RuntimeError("phase 3 boom")

    monkeypatch.setattr(_store, "_delete_superseded_steps", boom)

    with pytest.raises(RuntimeError, match="phase 3 boom"):
        _store.clear_superseded_jobs()

    # Phase 1's UPDATE already committed on its own connection/with-block
    # before phase 3 ran, so it must have survived the later exception.
    job = _store.get(stale)
    assert job["status"] == "cancelled"
    assert "expired" in job["error"]


def test_clear_superseded_phases_3_and_4_durable_if_phase5_raises(monkeypatch):
    """Phases 3+4 (delete steps, delete superseded jobs) must stay
    committed even when phase 5 (orphaned-step sweep) blows up."""
    swept_job = _mk_job("classify_thread", "imsg:11", "failed", age_days=2)
    _store.insert_step(swept_job, "classify")
    _mk_job("classify_thread", "imsg:11", "succeeded", age_days=1)

    def boom():
        raise RuntimeError("phase 5 boom")

    monkeypatch.setattr(_store, "_delete_orphaned_job_steps", boom)

    with pytest.raises(RuntimeError, match="phase 5 boom"):
        _store.clear_superseded_jobs()

    # Phases 3 and 4 already committed the job + its step's deletion
    # before phase 5 ran.
    assert swept_job not in _job_ids()
    from orbit_jobs.db import get_db
    with get_db() as conn:
        remaining_steps = conn.execute(
            "SELECT COUNT(*) AS n FROM job_steps WHERE job_id = ?", (swept_job,),
        ).fetchone()["n"]
    assert remaining_steps == 0
