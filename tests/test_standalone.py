"""orbit_jobs runs without its host: only the package on the path, no site-packages."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]

SCRIPT = r'''
import asyncio
import sys

import orbit_jobs
from orbit_jobs import KindDescriptor, begin_step, end_step, get, list_steps, register_kind, spawn
from orbit_jobs.worker import run_worker

for host in ("core", "app", "server"):
    try:
        __import__(host)
    except ModuleNotFoundError:
        pass
    else:
        raise SystemExit(f"{host} is importable; this test must run without the host")


async def double(job_id, payload):
    bucket = begin_step("double", input_text=str(payload["n"]))
    orbit_jobs.current().record_io("memory", "read", rows=1)
    end_step(bucket, "ok", ms=1, output_text="done")
    return {"doubled": payload["n"] * 2}


register_kind(KindDescriptor(kind="double", label="Double", description="d", handler=double,
                             capability="cpu"))
register_kind(KindDescriptor(kind="inline", label="Inline", description="i", handler=double))


async def main():
    routed = spawn("double", {"n": 21})
    inline = spawn("inline", {"n": 2})
    stop = asyncio.Event()
    worker = asyncio.ensure_future(run_worker(["cpu"], poll_interval=0.05, stop_event=stop))
    for _ in range(200):
        if all(get(j)["status"] in ("succeeded", "failed") for j in (routed, inline)):
            break
        await asyncio.sleep(0.05)
    stop.set()
    await worker
    for job_id in (routed, inline):
        job = get(job_id)
        steps = list_steps(job_id)
        print(job["kind"], job["status"], job["result"], steps[0]["status"],
              steps[0]["io_records"][0]["resource"])
    print(sorted(m for m in sys.modules if m.split(".")[0] in ("core", "app", "server")))


asyncio.run(main())
'''


def test_runs_a_job_end_to_end_with_only_orbit_jobs_on_the_path(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PYTHON", "VIRTUAL_ENV"))}
    env["PYTHONPATH"] = str(PACKAGE_ROOT)
    env["ORBIT_JOBS_DB"] = str(tmp_path / "jobs.db")
    out = subprocess.run([sys.executable, "-S", "-c", SCRIPT], cwd=tmp_path, env=env,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    assert out.stdout.splitlines() == [
        "double succeeded {'doubled': 42} ok memory",
        "inline succeeded {'doubled': 4} ok memory",
        "[]",
    ]
    assert (tmp_path / "jobs.db").exists()
