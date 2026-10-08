"""Remote job inputs: files a remote worker downloads before its handler runs
(orbit_jobs.remote input roots, the input route, the worker's cache)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import orbit_jobs as jobs
import orbit_jobs.remote as remote
import orbit_jobs.remote_worker as remote_worker
import pytest
from fastapi import FastAPI
from orbit_jobs.registry import KindDescriptor
from orbit_jobs.routes import remote_worker_router
from orbit_jobs.store import get, insert

TOKEN = "s3cret"
HEAD = "a" * 40


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("REMOTE_WORKER_TOKEN", TOKEN)
    monkeypatch.setenv("REMOTE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("REMOTE_INPUT_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("REMOTE_WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setattr(remote, "git_head", lambda: HEAD)
    monkeypatch.setattr(remote_worker, "HEARTBEAT_INTERVAL_SECONDS", 0.05)


def _artifact(rel: str, data: bytes) -> Path:
    path = remote.artifacts_root() / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(remote_worker_router())
    return app


async def _drain(app: FastAPI, pred, timeout: float = 5.0) -> None:
    stop = asyncio.Event()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://server",
        headers={"Authorization": f"Bearer {TOKEN}"},
    ) as client:
        task = asyncio.create_task(
            remote_worker.run_remote_worker(
                client, ["remote-test"], stop_event=stop, code_version=HEAD, poll_interval=0.02
            )
        )
        for _ in range(int(timeout / 0.02)):
            if pred():
                break
            await asyncio.sleep(0.02)
        stop.set()
        await asyncio.wait_for(task, timeout)


def _register_reader(kind: str, seen: list) -> None:
    async def handler(job_id, payload):
        root = Path(payload["inputs_dir"])
        seen.append(
            {
                p.relative_to(root).as_posix(): p.read_bytes()
                for p in sorted(root.rglob("*"))
                if p.is_file()
            }
        )
        return {"ok": True}

    jobs.register_kind(
        KindDescriptor(
            kind=kind, label=kind, description=kind, handler=handler, capability="remote-test"
        )
    )


def test_inputs_are_downloaded_before_the_handler_and_cached_across_jobs(tmp_path, monkeypatch):
    _artifact("data-v9/shard-00001.npz", b"shard one")
    _artifact("data-v9/shard-00002.npz", b"shard two!")
    seen: list = []
    _register_reader("test_inputs_read", seen)
    inputs = remote.describe_inputs(
        {
            "window/a.npz": "artifacts:data-v9/shard-00001.npz",
            "window/b.npz": "artifacts:data-v9/shard-00002.npz",
        }
    )
    first = insert("test_inputs_read", {"inputs": inputs}, required_capability="remote-test")
    second = insert("test_inputs_read", {"inputs": inputs}, required_capability="remote-test")

    served = []
    real = remote.input_location
    monkeypatch.setattr(remote, "input_location", lambda *a: served.append(a[2]) or real(*a))
    asyncio.run(
        _drain(_app(), lambda: all(get(j)["status"] == "succeeded" for j in (first, second)))
    )

    assert seen == [{"window/a.npz": b"shard one", "window/b.npz": b"shard two!"}] * 2
    assert sorted(served) == ["window/a.npz", "window/b.npz"]  # the second job hit the cache
    assert not (tmp_path / "work" / "jobs").exists() or not any(
        (tmp_path / "work" / "jobs").iterdir()
    )
    assert (
        tmp_path / "cache" / "artifacts" / "data-v9" / "shard-00001.npz"
    ).read_bytes() == b"shard one"


def test_input_refs_and_names_cannot_escape_their_roots():
    for bad in ("artifacts:../secrets", "artifacts:/etc/passwd", "nope:file", "artifacts:a\\b"):
        with pytest.raises(ValueError):
            remote.resolve_input(bad)
    _artifact("ok.npz", b"1")
    with pytest.raises(ValueError):
        remote.describe_inputs({"../outside": "artifacts:ok.npz"})
    with pytest.raises(FileNotFoundError):
        remote.describe_inputs({"x": "artifacts:missing.npz"})
    remote.register_input_root("test-root", lambda: remote.artifacts_root())
    assert remote.resolve_input("test-root:ok.npz") == remote.artifacts_root() / "ok.npz"


def test_an_input_that_changed_size_fails_the_job_instead_of_running_on_bad_data(monkeypatch):
    monkeypatch.setattr(remote_worker, "DOWNLOAD_ATTEMPTS", 1)
    path = _artifact("data-v9/shard-00001.npz", b"original")
    seen: list = []
    _register_reader("test_inputs_changed", seen)
    job = insert(
        "test_inputs_changed",
        {"inputs": remote.describe_inputs({"s.npz": "artifacts:data-v9/shard-00001.npz"})},
        required_capability="remote-test",
    )
    path.write_bytes(b"rewritten with another length")
    asyncio.run(_drain(_app(), lambda: get(job)["status"] == "failed"))

    assert seen == []
    assert "expected 8" in get(job)["error"]
