"""Remote workers claim, run and finish jobs over HTTP (core.jobs.remote*)."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI

import core.jobs.remote as remote
import core.jobs.remote_worker as remote_worker
import server.worker_routes as wr
from core import jobs
from core.jobs.registry import KindDescriptor
from core.jobs.store import get, insert, mark_terminal, request_cancel
from server.remote_worker_routes import router

TOKEN = "s3cret"
HEAD = "a" * 40


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    monkeypatch.setenv("REMOTE_WORKER_TOKEN", TOKEN)
    monkeypatch.setattr(remote, "git_head", lambda: HEAD)
    monkeypatch.setattr(remote_worker, "HEARTBEAT_INTERVAL_SECONDS", 0.05)


def _client(token: str | None = TOKEN) -> httpx.AsyncClient:
    app = FastAPI()
    app.include_router(router)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://server", headers=headers)


def _register(kind: str, handler, **kw) -> None:
    jobs.register_kind(KindDescriptor(kind=kind, label=kind, description=kind,
                                      handler=handler, **kw))


async def _run_until(pred, capabilities=("remote-test",), timeout=5.0) -> None:
    """Drive the real worker loop until ``pred()`` holds, then stop it."""
    stop = asyncio.Event()
    async with _client() as client:
        task = asyncio.create_task(remote_worker.run_remote_worker(
            client, list(capabilities), stop_event=stop, code_version=HEAD, poll_interval=0.02))
        for _ in range(int(timeout / 0.02)):
            if pred():
                break
            await asyncio.sleep(0.02)
        stop.set()
        await asyncio.wait_for(task, timeout=timeout)
    assert pred()


def _claim(client, **overrides):
    body = {"worker_id": "w", "capabilities": ["remote-test"], "pid": 1, "code_version": HEAD}
    return client.post("/api/remote-workers/claim", json={**body, **overrides})


def test_routes_are_off_without_a_token(monkeypatch):
    monkeypatch.delenv("REMOTE_WORKER_TOKEN")

    async def go():
        async with _client() as c:
            return (await _claim(c)).status_code

    assert asyncio.run(go()) == 404


def test_wrong_token_is_rejected():
    async def go():
        async with _client("nope") as c:
            return (await _claim(c)).status_code

    assert asyncio.run(go()) == 401


def test_claims_refuse_local_capabilities_and_other_code():
    job_id = insert("classify_thread", {}, required_capability="ollama")

    async def go():
        async with _client() as c:
            local = await _claim(c, capabilities=["ollama"])
            skewed = await _claim(c, code_version="b" * 40)
            return local.status_code, skewed.status_code, skewed.json()["detail"]

    local, skewed, detail = asyncio.run(go())
    assert local == 400
    assert skewed == 409 and detail["server"] == HEAD
    assert get(job_id)["status"] == "pending"   # a remote box never took local work


def test_a_pinned_job_goes_to_workers_on_its_commit_even_after_the_server_moves():
    old = "b" * 40
    pinned = insert("pinned", {"code_version": old}, required_capability="remote-test")
    unpinned = insert("unpinned", {}, required_capability="remote-test")

    async def go():
        async with _client() as c:
            at_head = (await _claim(c)).json()["job"]["id"]
            on_old = (await _claim(c, code_version=old)).json()["job"]["id"]
            nothing_left = await _claim(c, code_version=old)
            return at_head, on_old, nothing_left.status_code

    at_head, on_old, nothing_left = asyncio.run(go())
    assert at_head == unpinned   # the server's commit never takes another commit's job
    assert on_old == pinned      # the old commit keeps working through its pinned run
    assert nothing_left == 409   # and is told it's behind once that run is drained


def test_remote_worker_runs_a_job_end_to_end():
    async def handler(job_id, payload):
        return {"doubled": payload["n"] * 2}

    _register("test_remote_double", handler)
    job_id = insert("test_remote_double", {"n": 21}, required_capability="remote-test")

    asyncio.run(_run_until(lambda: get(job_id)["status"] == "succeeded"))

    job = get(job_id)
    assert job["result"] == {"doubled": 42}
    assert job["worker_id"].split(":")[1] == "remote-test"


def test_cancel_request_reaches_the_remote_handler():
    started = asyncio.Event()

    async def handler(job_id, payload):
        started.set()
        await asyncio.sleep(30)

    _register("test_remote_cancel", handler)
    job_id = insert("test_remote_cancel", {}, required_capability="remote-test")

    def cancelled():
        if started.is_set() and get(job_id)["status"] == "running":
            request_cancel(job_id)
        return get(job_id)["status"] == "cancelled"

    asyncio.run(_run_until(cancelled))
    assert get(job_id)["error"] == "cancelled"


def test_lost_lease_stops_the_handler_and_keeps_the_sweeps_verdict():
    started = asyncio.Event()
    stopped = []

    async def handler(job_id, payload):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            stopped.append(job_id)
            raise

    _register("test_remote_lease", handler)
    job_id = insert("test_remote_lease", {}, required_capability="remote-test")

    def swept():
        if started.is_set() and get(job_id)["status"] == "running":
            # What recover_orphans does to a job whose heartbeats went stale.
            mark_terminal(job_id, "cancelled", error="worker died (no heartbeat)")
        return bool(stopped)

    asyncio.run(_run_until(swept))
    assert get(job_id)["error"] == "worker died (no heartbeat)"


def _orphan(job_id: int) -> None:
    """Make ``job_id`` a running job whose worker stopped heartbeating long ago."""
    from core.db import get_db

    with get_db() as conn:
        conn.execute("UPDATE jobs SET status = 'running', started_at = ?, last_heartbeat_at = ? "
                     "WHERE id = ?", ("2026-01-01T00:00:00+00:00",) * 2 + (job_id,))


def _active(kind: str) -> list[dict]:
    return [j for j in jobs.list_jobs(kind=kind) if j["status"] in ("pending", "running")]


def test_a_remote_job_whose_worker_vanished_is_queued_again_up_to_three_times():
    from core.jobs.store import recover_orphans

    job_id = insert("test_remote_orphan", {"shard": 7}, subject_type="t", subject_id="run/7",
                    required_capability="remote-test", retry_count=1)
    chain = [job_id]
    for _ in range(3):
        _orphan(chain[-1])
        recover_orphans()
        assert get(chain[-1])["status"] == "cancelled"
        (fresh,) = _active("test_remote_orphan")
        assert fresh["parent_job_id"] == chain[-1] and fresh["payload"] == {"shard": 7}
        assert fresh["subject_id"] == "run/7" and fresh["retry_count"] == 1  # budget kept
        chain.append(fresh["id"])
    _orphan(chain[-1])
    recover_orphans()
    assert _active("test_remote_orphan") == []  # the fourth loss in a row is final


def test_a_remote_job_cancelled_before_its_worker_vanished_is_not_queued_again():
    from core.jobs.store import recover_orphans

    job_id = insert("test_remote_orphan", {}, subject_type="t", subject_id="run/8",
                    required_capability="remote-test")
    _orphan(job_id)
    assert jobs.request_cancel(job_id) == "requested"  # its worker never sees it
    recover_orphans()
    assert get(job_id)["status"] == "cancelled" and _active("test_remote_orphan") == []


def test_a_local_job_whose_worker_vanished_is_only_cancelled():
    from core.jobs.store import recover_orphans

    job_id = insert("test_local_orphan", {}, required_capability="ollama")
    _orphan(job_id)
    recover_orphans()
    assert get(job_id)["status"] == "cancelled" and _active("test_local_orphan") == []


def _silent_for(job_id: int, seconds: int) -> None:
    """Make ``job_id`` a running job whose last heartbeat was ``seconds`` ago."""
    from datetime import UTC, datetime, timedelta

    from core.db import get_db

    then = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
    with get_db() as conn:
        conn.execute("UPDATE jobs SET status = 'running', started_at = ?, last_heartbeat_at = ? "
                     "WHERE id = ?", (then, then, job_id))


def test_a_remote_job_survives_a_few_silent_minutes_but_a_local_one_does_not():
    """A hub restart can silence remote heartbeats for a minute (2026-10-06): remote jobs
    get 10 minutes before they count as dead, local ones keep 60 s."""
    from core.jobs.store import recover_orphans

    remote = insert("test_remote_orphan", {}, subject_type="t", subject_id="run/9",
                    required_capability="remote-test")
    local = insert("test_local_orphan", {}, required_capability="ollama")
    _silent_for(remote, 3 * 60)
    _silent_for(local, 3 * 60)
    recover_orphans()
    assert get(remote)["status"] == "running"
    assert get(local)["status"] == "cancelled"
    _silent_for(remote, 11 * 60)
    recover_orphans()
    assert get(remote)["status"] == "cancelled"
    assert [j["parent_job_id"] for j in _active("test_remote_orphan")] == [remote]


def test_failed_remote_job_uses_the_kinds_retry_budget():
    async def handler(job_id, payload):
        raise RuntimeError("boom")

    _register("test_remote_retry", handler, capability="remote-test", max_retries=1)
    job_id = insert("test_remote_retry", {}, required_capability="remote-test")

    def retried_and_failed():
        rows = jobs.list_jobs(kind="test_remote_retry")
        return len(rows) == 2 and all(r["status"] == "failed" for r in rows)

    asyncio.run(_run_until(retried_and_failed))
    assert get(job_id)["error"] == "RuntimeError: boom"


def _drive_until(pred, settle: float = 0.3, timeout: float = 5.0) -> None:
    """Run the worker until ``pred()`` holds, keep it running ``settle`` s more, stop it."""
    async def go():
        stop = asyncio.Event()
        async with _client() as client:
            task = asyncio.create_task(remote_worker.run_remote_worker(
                client, ["remote-test"], stop_event=stop, code_version=HEAD, poll_interval=0.02))
            for _ in range(int(timeout / 0.02)):
                if pred():
                    break
                await asyncio.sleep(0.02)
            await asyncio.sleep(settle)
            stop.set()  # also ends a fast-failure pause
            await asyncio.wait_for(task, timeout=timeout)

    asyncio.run(go())


def test_a_worker_failing_every_job_at_once_stops_claiming():
    async def broken(job_id, payload):
        raise RuntimeError("model missing on this machine")

    _register("test_remote_broken", broken, capability="remote-test", max_retries=0)
    ids = [insert("test_remote_broken", {"n": n}, required_capability="remote-test")
           for n in range(6)]

    _drive_until(lambda: sum(get(i)["status"] == "failed" for i in ids) >= 3)

    statuses = [get(i)["status"] for i in ids]
    assert statuses.count("failed") == remote_worker.MAX_FAST_FAILURES
    assert statuses.count("pending") == 6 - remote_worker.MAX_FAST_FAILURES


def test_a_success_between_failures_keeps_the_worker_claiming():
    async def flaky(job_id, payload):
        if payload["fail"]:
            raise RuntimeError("bad shard")
        return {}

    _register("test_remote_flaky", flaky, capability="remote-test", max_retries=0)
    ids = [insert("test_remote_flaky", {"fail": fail}, required_capability="remote-test")
           for fail in (True, True, False, True, True)]

    _drive_until(lambda: all(get(i)["status"] in ("failed", "succeeded") for i in ids), settle=0)

    assert [get(i)["status"] for i in ids] == ["failed", "failed", "succeeded", "failed", "failed"]


def test_local_sweep_never_spawns_a_remote_capability(monkeypatch):
    spawned = []
    monkeypatch.setattr(wr, "_spawn", lambda cap: spawned.append(cap))
    insert("test_remote_double", {}, required_capability="remote-test")

    wr.sweep_dead_workers()

    assert spawned == []


def test_artifacts_upload_to_the_server_and_leave_the_worker(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    monkeypatch.setenv("REMOTE_ARTIFACTS_DIR", str(root))
    local = tmp_path / "remote-box" / "out.bin"
    local.parent.mkdir()

    async def handler(job_id, payload):
        local.write_bytes(b"shard bytes")
        return {"positions": 3, "artifacts": {"run-1/shard-00007.npz": str(local)}}

    _register("test_remote_artifact", handler)
    job_id = insert("test_remote_artifact", {}, required_capability="remote-test")

    asyncio.run(_run_until(lambda: get(job_id)["status"] == "succeeded"))

    stored = root / "run-1" / "shard-00007.npz"
    assert stored.read_bytes() == b"shard bytes"
    assert get(job_id)["result"] == {"positions": 3,
                                     "artifacts": {"run-1/shard-00007.npz": str(stored)}}
    assert not local.exists()


def test_artifact_names_cannot_escape_and_need_the_lease(tmp_path, monkeypatch):
    monkeypatch.setenv("REMOTE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    job_id = insert("test_remote_double", {}, required_capability="remote-test")

    async def go():
        async with _client() as c:
            claimed = (await _claim(c)).json()["job"]
            assert claimed["id"] == job_id

            def put(name, worker_id="w"):
                return c.put(f"/api/remote-workers/jobs/{job_id}/artifacts",
                             params={"name": name, "worker_id": worker_id}, content=b"x")

            return [(await put(n)).status_code for n in ("../x", "/etc/x", "a/../../x")] + [
                (await put("ok.bin", worker_id="someone-else")).status_code,
                (await put("ok.bin")).status_code]

    assert asyncio.run(go()) == [400, 400, 400, 409, 200]
    assert not (tmp_path / "x").exists()


def test_a_handler_raising_a_base_exception_fails_the_job_not_the_worker():
    class Panic(BaseException):  # like pyo3_runtime.PanicException from a Rust panic
        pass

    async def handler(job_id, payload):
        raise Panic("unknown card")

    _register("test_remote_panic", handler)
    first = insert("test_remote_panic", {}, required_capability="remote-test")
    second = insert("test_remote_double", {"n": 2}, required_capability="remote-test")

    asyncio.run(_run_until(lambda: get(second)["status"] == "succeeded"))
    assert get(first)["status"] == "failed"
    assert get(first)["error"] == "Panic: unknown card"


def test_worker_exits_to_restart_when_its_checkout_moves_to_the_servers_commit(monkeypatch):
    monkeypatch.setattr(remote, "git_head", lambda: "new-server-head")
    monkeypatch.setattr(remote_worker, "git_head", lambda: "new-server-head")  # pushed here too

    async def go():
        async with _client() as client:
            await asyncio.wait_for(remote_worker.run_remote_worker(
                client, ["remote-test"], stop_event=asyncio.Event(), code_version=HEAD,
                poll_interval=0.02), timeout=5)

    asyncio.run(go())  # returns instead of waiting on the version mismatch


async def _noop(job_id, payload):
    return {}


def test_protocol_versions_let_a_worker_on_another_commit_take_matching_kinds():
    _register("proto_ok", _noop, capability="remote-test")
    _register("proto_new", _noop, capability="remote-test", protocol=2)
    ok = insert("proto_ok", {}, required_capability="remote-test")
    newer = insert("proto_new", {}, required_capability="remote-test")

    async def go():
        async with _client() as c:
            r = await _claim(c, code_version="b" * 40,
                             protocols={"proto_ok": 1, "proto_new": 1, "proto_gone": 1})
            return r.status_code, r.json()

    status, body = asyncio.run(go())
    assert status == 200
    assert body["job"]["id"] == ok                         # another commit, same protocol
    assert body["stale_kinds"] == ["proto_gone", "proto_new"]
    assert get(newer)["status"] == "pending"               # an older protocol never runs it


def test_with_protocols_a_pinned_job_still_needs_its_exact_commit():
    _register("proto_pinned", _noop, capability="remote-test")
    pinned = insert("proto_pinned", {"code_version": "c" * 40}, required_capability="remote-test")

    async def go():
        async with _client() as c:
            elsewhere = await _claim(c, protocols={"proto_pinned": 1})
            on_commit = await _claim(c, code_version="c" * 40, protocols={"proto_pinned": 1})
            return elsewhere.json()["job"], on_commit.json()["job"]["id"]

    elsewhere, on_commit = asyncio.run(go())
    assert elsewhere is None and on_commit == pinned


def test_a_worker_with_protocols_keeps_running_across_server_commits(monkeypatch):
    monkeypatch.setattr(remote, "git_head", lambda: "server-moved-on")   # no 409 any more

    async def handler(job_id, payload):
        return {"ran": True}

    _register("proto_e2e", handler, capability="remote-test")
    job_id = insert("proto_e2e", {}, required_capability="remote-test")
    asyncio.run(_run_until(lambda: get(job_id)["status"] == "succeeded"))
    assert get(job_id)["result"] == {"ran": True}


def test_a_training_job_holds_its_machines_shared_cpus_while_it_runs():
    """A remote-gpu job claimed by a machine takes that machine's self-play/training pool
    (its self-play drains) until it finishes."""
    from core import compute_allocation as ca

    ca.report("VictorHyte", 28, 16, 16)
    ca.set_allocation("VictorHyte", 16, 0)
    _register("remote-train-test", lambda *_: None, capability="remote-gpu")
    job_id = insert("remote-train-test", {}, required_capability="remote-gpu")

    async def go():
        async with _client() as c:
            claimed = await c.post("/api/remote-workers/claim", json={
                "worker_id": "VictorHyte:remote-gpu:7:ab", "capabilities": ["remote-gpu"],
                "pid": 7, "code_version": HEAD})
            assert claimed.json()["job"]["id"] == job_id
            held = ca.get_allocation("VictorHyte")
            await c.post(f"/api/remote-workers/jobs/{job_id}/heartbeat",
                         json={"worker_id": "VictorHyte:remote-gpu:7:ab", "pid": 7})
            renewed = ca.get_allocation("VictorHyte")
            await c.post(f"/api/remote-workers/jobs/{job_id}/finish", json={
                "worker_id": "VictorHyte:remote-gpu:7:ab", "status": "succeeded"})
            return held, renewed

    held, renewed = asyncio.run(go())
    assert (held["selfplay_now"], held["training_now"]) == (0, 16)
    assert held["training_claimed_by"] == f"job {job_id}"
    assert renewed["training_claimed_until"] >= held["training_claimed_until"]
    after = ca.get_allocation("VictorHyte")
    assert (after["selfplay_now"], after["training_now"], after["training_active"]) == (
        16, 0, False)


def test_a_running_job_publishes_its_progress_file_before_it_finishes(tmp_path, monkeypatch):
    """A training run's metrics reach the server after each change, while it still trains."""
    root = tmp_path / "artifacts"
    monkeypatch.setenv("REMOTE_ARTIFACTS_DIR", str(root))
    local = tmp_path / "remote-box" / "metrics.json"
    local.parent.mkdir()
    stored = root / "riftbound-train" / "v9" / "warm-s0.json"

    async def handler(job_id, payload):
        remote_worker.publish_while_running(job_id, "riftbound-train/v9/warm-s0.json", local)
        local.write_text('{"epochs": [{"epoch": 1}]}')
        for _ in range(200):  # until the heartbeat has carried it to the server
            if stored.exists():
                break
            await asyncio.sleep(0.02)
        seen_mid_run = stored.read_text()
        return {"seen": seen_mid_run}

    _register("test_remote_progress", handler)
    job_id = insert("test_remote_progress", {}, required_capability="remote-test")
    asyncio.run(_run_until(lambda: get(job_id)["status"] == "succeeded"))
    assert get(job_id)["result"]["seen"] == '{"epochs": [{"epoch": 1}]}'
    assert job_id not in remote_worker._LIVE_ARTIFACTS  # forgotten once the job ends
