"""Job worker for another machine: claims ``remote-*`` jobs over HTTP.

Runs on a box that has a checkout of this repo at the server's git HEAD
but no access to ``tasks.db``. It polls the server's
``/api/remote-workers`` routes, runs the registered handler locally, keeps
the job's lease alive with heartbeats (which also carry cancel requests)
and posts the result back. See ``core.jobs.remote`` for the server side
and the pure-compute contract remote kinds must follow.

Run it through the app composition root, which registers the kinds:

    REMOTE_WORKER_TOKEN=... uv run python -m app.remote_worker \\
        --server http://victors-macbook-pro-2:8765 --capability remote-cpu
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath

import httpx

from core import mesh
from core.compute_allocation import allowed_cpus, default_excluded, host_name
from core.jobs import wake
from core.jobs.registry import get_handler, list_descriptors
from core.jobs.remote import git_head, is_remote_capability

log = logging.getLogger("jobs.remote_worker")

HEARTBEAT_INTERVAL_SECONDS = 10
IDLE_SLEEP_SECONDS = 2.0
# An empty queue doubles the poll interval up to this, so a pool of idle
# worker processes doesn't hammer the server.
MAX_IDLE_SLEEP_SECONDS = 30.0
MAX_BACKOFF_SECONDS = 60.0
# The server refuses claims while our git HEAD differs from its checkout.
VERSION_MISMATCH_SLEEP_SECONDS = 60.0
# Job inputs: parallel downloads per job, and the per-machine cache they land in
# (shared across jobs, so a shard used by several training runs downloads once;
# oldest-used files are evicted past REMOTE_INPUT_CACHE_GB).
DOWNLOAD_CONCURRENCY = 4
DOWNLOAD_ATTEMPTS = 5
# A worker whose jobs keep failing right after the claim is probably broken
# itself (a missing file, a bad install), not the jobs: after this many in a row
# it stops claiming for a while instead of failing the whole queue. The pause
# doubles while it keeps happening and resets on a success. On a rented node the
# pause also stops its heartbeats, so the fleet sweep destroys it.
FAST_FAILURE_SECONDS = 60.0
MAX_FAST_FAILURES = 3
FAST_FAILURE_PAUSE_SECONDS = 15 * 60.0
MAX_FAST_FAILURE_PAUSE_SECONDS = 2 * 3600.0


def _work_root() -> Path:
    return Path(os.environ.get("REMOTE_WORK_DIR") or Path.home() / ".cache/automations/remote-work")


def _cache_root() -> Path:
    return Path(os.environ.get("REMOTE_INPUT_CACHE")
                or Path.home() / ".cache/automations/remote-inputs")


def _safe(name: str) -> Path:
    rel = PurePosixPath(name)
    if (not name or rel.is_absolute() or "\\" in name
            or any(p in ("", ".", "..") for p in rel.parts)):
        raise ValueError(f"bad input name {name!r}")
    return Path(*rel.parts)


class _LeaseLost(Exception):
    pass


async def _download(client: httpx.AsyncClient, job_id: int, worker_id: str, name: str,
                    dest: Path, size: int) -> None:
    """Fetch one input to ``dest`` (via a temp file), following a redirect to
    object storage. Retries transient failures; raises on a size mismatch."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(f".{dest.name}.{os.getpid()}.part")
    delay = 1.0
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            async with client.stream("GET", f"/api/remote-workers/jobs/{job_id}/inputs",
                                     params={"name": name, "worker_id": worker_id},
                                     follow_redirects=True, timeout=None) as r:
                if r.status_code == 409:
                    raise _LeaseLost(f"job {job_id} lease lost while fetching {name}")
                r.raise_for_status()
                with part.open("wb") as f:
                    async for chunk in r.aiter_bytes(1 << 20):
                        f.write(chunk)
            got = part.stat().st_size
            if got != size:
                raise ValueError(f"input {name}: got {got} bytes, expected {size}")
            part.replace(dest)
            return
        except (httpx.HTTPError, ValueError) as exc:
            part.unlink(missing_ok=True)
            if attempt == DOWNLOAD_ATTEMPTS or (isinstance(exc, httpx.HTTPStatusError)
                                                and exc.response.status_code < 500):
                raise
            log.warning("job %s: fetching %s failed (%s); retry %d in %.0fs",
                        job_id, name, exc, attempt, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_BACKOFF_SECONDS)


def _link(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.unlink(missing_ok=True)
    try:
        os.link(src, dest)
    except OSError:  # another volume, or a filesystem without hard links
        shutil.copy2(src, dest)


async def _fetch_inputs(client: httpx.AsyncClient, job_id: int, worker_id: str,
                        inputs: dict[str, dict]) -> Path:
    """Make every input available under one directory; returns it.

    Files come from the machine-wide cache when present at the right size,
    else from the server. Logs progress every 25 files.
    """
    job_dir = _work_root() / "jobs" / str(job_id) / "inputs"
    cache = _cache_root()
    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    total, done, fetched, fetched_bytes = len(inputs), 0, 0, 0

    async def one(name: str, spec: dict) -> None:
        nonlocal done, fetched, fetched_bytes
        root, _, rel = spec["ref"].partition(":")
        cached = cache / _safe(root) / _safe(rel)
        size = int(spec["size"])
        if cached.exists() and cached.stat().st_size == size:
            os.utime(cached)  # recently used: evicted last
        else:
            async with sem:
                await _download(client, job_id, worker_id, name, cached, size)
            fetched, fetched_bytes = fetched + 1, fetched_bytes + size
        _link(cached, job_dir / _safe(name))
        done += 1
        if done % 25 == 0 or done == total:
            log.info("job %s: inputs %d/%d ready (%d fetched, %.0f MB)",
                     job_id, done, total, fetched, fetched_bytes / 1e6)

    await asyncio.gather(*(one(name, spec) for name, spec in inputs.items()))
    return job_dir


def _evict_cache(limit_bytes: int) -> None:
    """Delete least recently used cached inputs until the cache fits."""
    files = [p for p in _cache_root().rglob("*") if p.is_file() and not p.name.endswith(".part")]
    stats = sorted(((p.stat().st_mtime, p.stat().st_size, p) for p in files))
    total = sum(size for _, size, _ in stats)
    for _, size, path in stats:
        if total <= limit_bytes:
            break
        path.unlink(missing_ok=True)
        total -= size


# Files a running job wants on the server before it finishes (``publish_while_running``):
# {job id: {artifact name: local path}}, uploaded with the heartbeat whenever they change.
_LIVE_ARTIFACTS: dict[int, dict[str, str]] = {}


def publish_while_running(job_id: int, name: str, path: str | Path) -> None:
    """Upload ``path`` as the job's artifact ``name`` each time it changes while the job
    runs (a training run's per-epoch metrics), so the server sees its progress before it
    finishes. Best effort; the job's final artifacts still come with its result."""
    _LIVE_ARTIFACTS.setdefault(job_id, {})[name] = str(path)


async def _push_live_artifacts(client: httpx.AsyncClient, job_id: int, worker_id: str,
                               sent: dict[str, float]) -> None:
    for name, local in list(_LIVE_ARTIFACTS.get(job_id, {}).items()):
        try:
            mtime = os.stat(local).st_mtime
            if sent.get(name) == mtime:
                continue
            data = await asyncio.to_thread(Path(local).read_bytes)
            r = await client.put(f"/api/remote-workers/jobs/{job_id}/artifacts",
                                 params={"name": name, "worker_id": worker_id}, content=data)
            if r.is_success:
                sent[name] = mtime
        except (OSError, httpx.HTTPError):
            log.debug("job %s: live artifact %s not sent this time", job_id, name,
                      exc_info=True)


async def _run_job(client: httpx.AsyncClient, job: dict, worker_id: str) -> str | None:
    """Run one claimed job; its final status, or None when the lease was lost."""
    job_id = int(job["id"])
    handler = get_handler(job["kind"])
    if handler is None:
        await _finish(client, job_id, worker_id, "failed",
                      error=f"no handler for kind {job['kind']!r} on {socket.gethostname()}")
        return "failed"
    async def run_handler():
        payload = dict(job.get("payload") or {})
        if payload.get("inputs"):
            payload["inputs_dir"] = str(await _fetch_inputs(client, job_id, worker_id,
                                                            payload["inputs"]))
        return await handler(job_id, payload)

    task = asyncio.create_task(run_handler(), name=f"job-{job_id}")
    reason: str | None = None  # why we cancelled the handler, if we did

    async def keep_lease() -> None:
        nonlocal reason
        sent: dict[str, float] = {}  # live artifacts' mtimes as last uploaded
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            try:
                r = await client.post(f"/api/remote-workers/jobs/{job_id}/heartbeat",
                                      json={"worker_id": worker_id, "pid": os.getpid()})
            except httpx.HTTPError:
                # A blip is fine: the server's orphan sweep waits 60 s before
                # releasing the lease, and a later heartbeat will tell us if it did.
                log.warning("heartbeat for job %s failed; retrying", job_id, exc_info=True)
                continue
            if r.status_code == 409:
                reason = "lease lost"
            elif r.is_success and r.json().get("cancel"):
                reason = "cancelled"
            if reason:
                log.info("job %s: %s — stopping handler", job_id, reason)
                task.cancel()
                return
            await _push_live_artifacts(client, job_id, worker_id, sent)

    lease = asyncio.create_task(keep_lease())
    timeout = job.get("timeout_seconds")
    try:  # the lease stays alive until any artifacts are uploaded
        try:
            result = await asyncio.wait_for(
                task, timeout=timeout if timeout and timeout > 0 else None)
            status, error = "succeeded", None
        except asyncio.TimeoutError:
            result, status, error = None, "failed", f"timeout after {timeout}s"
        except asyncio.CancelledError:
            if reason is None:  # the worker itself is shutting down
                raise
            result, status, error = None, "cancelled", "cancelled"
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:  # noqa: BLE001 — pyo3 panics are BaseException
            log.exception("job %s failed", job_id)
            result, status, error = None, "failed", f"{type(exc).__name__}: {exc}"
        if reason == "lease lost":
            return None  # the server already finished the row; nothing to report
        if status == "succeeded" and isinstance(result, dict) and result.get("artifacts"):
            try:
                uploaded = await _upload_artifacts(client, job_id, worker_id, result["artifacts"])
            except Exception as exc:  # noqa: BLE001
                log.exception("job %s: artifact upload failed", job_id)
                result, status, error = None, "failed", f"artifact upload failed: {exc}"
            else:
                if uploaded is None:
                    return None  # lease lost mid-upload
                result = {**result, "artifacts": uploaded}
    finally:
        lease.cancel()
        _LIVE_ARTIFACTS.pop(job_id, None)
        if (job.get("payload") or {}).get("inputs"):
            shutil.rmtree(_work_root() / "jobs" / str(job_id), ignore_errors=True)
            limit = float(os.environ.get("REMOTE_INPUT_CACHE_GB", "100")) * 1e9
            await asyncio.to_thread(_evict_cache, int(limit))
    await _finish(client, job_id, worker_id, status, result=result, error=error)
    return status


async def _send(client: httpx.AsyncClient, what: str, method: str, url: str,
                **kwargs) -> httpx.Response | None:
    """Send until the server answers (connection errors and 5xx are retried
    with backoff). Returns None on 409 — the lease is gone. Other errors raise."""
    delay = 1.0
    while True:
        try:
            r = await client.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            log.warning("%s failed (%s); retrying in %.0fs", what, exc, delay)
        else:
            if r.status_code == 409:
                return None
            if r.status_code < 500:
                return r.raise_for_status()
            log.warning("%s got %s; retrying in %.0fs", what, r.status_code, delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_BACKOFF_SECONDS)


async def _upload_artifacts(client: httpx.AsyncClient, job_id: int, worker_id: str,
                            artifacts: dict[str, str]) -> dict[str, str] | None:
    """Upload each ``{name: local_path}`` and delete the local copy. Returns
    ``{name: server_path}``, or None when the lease was lost."""
    uploaded = {}
    for name, local in artifacts.items():
        data = await asyncio.to_thread(Path(local).read_bytes)
        r = await _send(client, f"upload {name} for job {job_id}", "PUT",
                        f"/api/remote-workers/jobs/{job_id}/artifacts",
                        params={"name": name, "worker_id": worker_id}, content=data)
        if r is None:
            return None
        uploaded[name] = r.json()["path"]
        Path(local).unlink(missing_ok=True)
    return uploaded


async def _finish(client: httpx.AsyncClient, job_id: int, worker_id: str, status: str,
                  *, result: dict | None = None, error: str | None = None) -> None:
    body = {"worker_id": worker_id, "status": status, "result": result, "error": error}
    # The result is the whole point of the job, so keep trying until the server answers.
    r = await _send(client, f"finish for job {job_id}", "POST",
                    f"/api/remote-workers/jobs/{job_id}/finish", json=body)
    if r is None:
        log.warning("job %s finished after its lease was released; result dropped", job_id)
    else:
        log.info("job %s %s", job_id, status)


async def run_remote_worker(
    client: httpx.AsyncClient,
    capabilities: list[str],
    *,
    stop_event: asyncio.Event,
    code_version: str | None = None,
    poll_interval: float = IDLE_SLEEP_SECONDS,
    stop_file: Path | None = None,
) -> None:
    """Claim and run jobs until ``stop_event`` is set. ``client`` carries the
    server base URL and the bearer token."""
    bad = [c for c in capabilities if not is_remote_capability(c)]
    if bad:
        raise ValueError(f"not remote capabilities: {bad}")
    # A node started from a source snapshot (no .git) names its commit instead.
    code_version = code_version or os.environ.get("AUTOMATIONS_CODE_VERSION") or git_head()
    if code_version is None:
        raise RuntimeError("remote worker needs a git checkout or AUTOMATIONS_CODE_VERSION "
                           "(the code version is checked on claim)")
    worker_id = (f"{host_name(socket.gethostname())}:{','.join(capabilities)}:"
                 f"{os.getpid()}:{uuid.uuid4().hex[:6]}")
    log.info("remote worker %s at %s", worker_id, code_version[:12])
    # Protocol version of every kind this process can run: the server hands it
    # any unpinned job (of its capabilities) whose kind version matches,
    # whatever commit either side is on.
    protocols = {d.kind: d.protocol for d in list_descriptors()}
    claim_body = {"worker_id": worker_id, "capabilities": capabilities,
                  "pid": os.getpid(), "code_version": code_version, "protocols": protocols}
    reported_stale: list[str] = []
    backoff = idle = poll_interval
    fast_failures, pause = 0, FAST_FAILURE_PAUSE_SECONDS
    # With NATS_URLS set, a spawned job wakes this worker at once instead of
    # waiting out the idle backoff (up to MAX_IDLE_SLEEP_SECONDS).
    wake_event = asyncio.Event()
    await mesh.start()
    wake.listen(capabilities, wake_event, key=f"remote:{worker_id}")
    try:
        while not stop_event.is_set() and not (stop_file and stop_file.exists()):
            wait = idle
            wake_event.clear()
            try:
                r = await client.post("/api/remote-workers/claim", json=claim_body)
                if r.status_code == 409:
                    if git_head() not in (None, code_version):
                        # New code was pushed to this checkout: exit so the
                        # --processes supervisor restarts us on it.
                        log.info("checkout moved to a new commit; exiting to restart on it")
                        return
                    log.error("server refuses claims: %s — check out the server's commit",
                              r.json().get("detail"))
                    wait = VERSION_MISMATCH_SLEEP_SECONDS
                else:
                    r.raise_for_status()
                    backoff = poll_interval
                    body = r.json()
                    stale = body.get("stale_kinds") or []
                    if stale != reported_stale:
                        reported_stale = stale
                        if stale:
                            log.warning("server won't hand this worker %s (another protocol "
                                        "version there); update this checkout", ", ".join(stale))
                    job = body["job"]
                    if job is not None:
                        log.info("claimed job %s (%s)", job["id"], job["kind"])
                        started = time.monotonic()
                        status = await _run_job(client, job, worker_id)
                        quick = time.monotonic() - started < FAST_FAILURE_SECONDS
                        idle = poll_interval
                        if status == "succeeded":
                            fast_failures, pause = 0, FAST_FAILURE_PAUSE_SECONDS
                        elif status == "failed" and quick:
                            fast_failures += 1
                        else:
                            fast_failures = 0
                        if fast_failures >= MAX_FAST_FAILURES:
                            log.error("%d jobs in a row failed within %.0fs of the claim; this "
                                      "worker looks broken, so it stops claiming for %.0f min",
                                      fast_failures, FAST_FAILURE_SECONDS, pause / 60)
                            # Only a stop ends the pause early, not a wake-up for new jobs.
                            await wake.idle(stop_event, asyncio.Event(), pause)
                            fast_failures, pause = 0, min(pause * 2, MAX_FAST_FAILURE_PAUSE_SECONDS)
                        continue
                    if git_head() not in (None, code_version):
                        # Idle on a checkout that has moved: restart on the new
                        # code (the --processes supervisor brings us back). Not
                        # while busy, so a run pinned to this commit finishes.
                        log.info("checkout moved to a new commit; exiting to restart on it")
                        return
                    idle = min(idle * 2, MAX_IDLE_SLEEP_SECONDS)
            except httpx.HTTPError as exc:
                log.warning("claim failed (%s); retrying in %.0fs", exc, backoff)
                wait, backoff = backoff, min(backoff * 2, MAX_BACKOFF_SECONDS)
            await wake.idle(stop_event, wake_event, wait)
    finally:
        wake.unlisten(capabilities, key=f"remote:{worker_id}")
        await mesh.stop()
        try:
            await client.post("/api/remote-workers/unregister",
                              json={"worker_id": worker_id, "pid": os.getpid()})
        except httpx.HTTPError:
            pass


def allocation_target(current: int, allocation: dict | None) -> int:
    """A failed/malformed poll preserves the last target, including a deliberate zero."""
    if not isinstance(allocation, dict):
        return current
    target = allocation.get("selfplay_processes")
    usable = allocation.get("usable_cpus")
    training = allocation.get("training_threads")
    excluded = allocation.get("excluded_cpus")
    if (any(type(n) is not int or n < 0 for n in (target, usable, training))
            or not isinstance(excluded, list)
            or any(type(n) is not int or n < 0 for n in excluded)):
        return current
    # If hardware capacity shrinks, leave the training reservation intact.
    return min(target, max(0, usable - training))


def _cpus_of(mask: int) -> list[int]:
    return [i for i in range(mask.bit_length()) if mask >> i & 1]


def _mask_of(cpus: list[int]) -> int:
    return sum(1 << c for c in set(cpus))


def _win_affinity(pid: int, cpus: list[int] | None) -> list[int]:
    """Windows: Get/SetProcessAffinityMask through kernel32 (no third-party packages,
    so a worker that restarts on new code before `uv sync` still imports)."""
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.GetCurrentProcess.restype = wintypes.HANDLE
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    k32.GetProcessAffinityMask.argtypes = (
        wintypes.HANDLE, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t))
    k32.SetProcessAffinityMask.argtypes = (wintypes.HANDLE, ctypes.c_size_t)
    k32.CloseHandle.argtypes = (wintypes.HANDLE,)
    own = pid in (0, os.getpid())
    # PROCESS_SET_INFORMATION | PROCESS_QUERY_INFORMATION
    handle = k32.GetCurrentProcess() if own else k32.OpenProcess(0x0200 | 0x0400, False, pid)
    if not handle:
        err = ctypes.get_last_error()
        if err == 87:  # ERROR_INVALID_PARAMETER: no such process
            raise ProcessLookupError(pid)
        raise ctypes.WinError(err)
    try:
        if cpus is not None:
            if not k32.SetProcessAffinityMask(handle, _mask_of(cpus)):
                raise ctypes.WinError(ctypes.get_last_error())
            return cpus
        mine, system = ctypes.c_size_t(), ctypes.c_size_t()
        if not k32.GetProcessAffinityMask(handle, ctypes.byref(mine), ctypes.byref(system)):
            raise ctypes.WinError(ctypes.get_last_error())
        return _cpus_of(mine.value)
    finally:
        if not own:
            k32.CloseHandle(handle)


def _win_descendants(pid: int) -> list[int]:
    """Windows: every process started (directly or not) by ``pid``, from a Toolhelp
    snapshot. A venv's ``Scripts\\python.exe`` is a launcher that runs the real interpreter
    as its child, so pinning a worker means pinning that child too."""
    import ctypes
    from ctypes import wintypes

    class Entry(ctypes.Structure):
        _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD),
                    ("th32ProcessID", wintypes.DWORD), ("th32DefaultHeapID", ctypes.c_size_t),
                    ("th32ModuleID", wintypes.DWORD), ("cntThreads", wintypes.DWORD),
                    ("th32ParentProcessID", wintypes.DWORD), ("pcPriClassBase", wintypes.LONG),
                    ("dwFlags", wintypes.DWORD), ("szExeFile", ctypes.c_wchar * 260)]

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    k32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
    k32.Process32FirstW.argtypes = k32.Process32NextW.argtypes = (
        wintypes.HANDLE, ctypes.POINTER(Entry))
    k32.CloseHandle.argtypes = (wintypes.HANDLE,)
    snap = k32.CreateToolhelp32Snapshot(0x2, 0)  # TH32CS_SNAPPROCESS
    if snap in (None, wintypes.HANDLE(-1).value):
        raise ctypes.WinError(ctypes.get_last_error())
    children: dict[int, list[int]] = {}
    try:
        entry = Entry()
        entry.dwSize = ctypes.sizeof(Entry)
        ok = k32.Process32FirstW(snap, ctypes.byref(entry))
        while ok:
            children.setdefault(entry.th32ParentProcessID, []).append(entry.th32ProcessID)
            ok = k32.Process32NextW(snap, ctypes.byref(entry))
    finally:
        k32.CloseHandle(snap)
    return _tree(pid, children)


def _tree(pid: int, children: dict[int, list[int]]) -> list[int]:
    """``pid``'s descendants in ``children`` (parent -> child pids), each once."""
    out, todo = [], list(children.get(pid, ()))
    while todo:
        child = todo.pop()
        if child not in out and child != pid:
            out.append(child)
            todo.extend(children.get(child, ()))
    return out


def get_affinity(pid: int) -> list[int]:
    """The CPUs ``pid`` (0: this process) may run on. Linux and Windows."""
    if sys.platform == "win32":
        return _win_affinity(pid, None)
    return sorted(os.sched_getaffinity(pid))


def set_affinity(pid: int, cpus: list[int]) -> None:
    if sys.platform == "win32":
        _win_affinity(pid, cpus)
    else:
        os.sched_setaffinity(pid, cpus)


def apply_affinity(pid: int, excluded: list[int], logical: int) -> None:
    """Fail closed on supported platforms. Children inherit affinity at creation."""
    if sys.platform == "darwin":
        return
    cpus = allowed_cpus(logical, excluded)
    if not cpus:
        raise ValueError("no usable CPUs remain")
    set_affinity(pid, cpus)


class WorkerPool:
    """Drain via stop files on every OS; never terminate a shard during resizing."""

    def __init__(self, cmd: list[str], directory: Path, logical: int):
        self.cmd, self.directory, self.logical = cmd, directory, logical
        self.children: list[tuple[subprocess.Popen, Path]] = []
        self._pinned: list[int] | None = None  # the exclusions the supervisor is pinned for

    @property
    def running(self) -> int:
        return sum(proc.poll() is None for proc, _ in self.children)

    def reconcile(self, target: int, excluded: list[int]) -> None:
        # The supervisor itself stays on the allowed CPUs too (VictorHyte's faulty cores
        # must run nothing), and its children inherit that mask when they are spawned.
        if sys.platform != "darwin" and self._pinned != sorted(excluded):
            apply_affinity(0, excluded, self.logical)
            self._pinned = sorted(excluded)
        live = []
        for proc, stop in self.children:
            if proc.poll() is None:
                live.append((proc, stop))
            else:
                stop.unlink(missing_ok=True)
        self.children = live
        active = [(proc, stop) for proc, stop in live if not stop.exists()]
        for _, stop in active[target:]:
            stop.touch()
        # Also update existing processes when exclusions change: on Windows the worker
        # behind each venv launcher too (it inherited the old mask when it started).
        for proc, _ in live:
            pids = [proc.pid]
            if sys.platform == "win32":
                try:
                    pids += _win_descendants(proc.pid)
                except OSError:
                    log.warning("could not list worker %d's processes", proc.pid, exc_info=True)
            for pid in pids:
                try:
                    apply_affinity(pid, excluded, self.logical)
                except ProcessLookupError:
                    pass  # it exited meanwhile; reap on the next pass
        # Draining processes occupy their slots until they really exit.
        for _ in range(max(0, target - len(live))):
            stop = self.directory / uuid.uuid4().hex
            # Linux/Windows children inherit the supervisor's (already pinned) mask, so
            # even their imports and first instructions run on healthy CPUs.
            proc = subprocess.Popen([*self.cmd, "--stop-file", str(stop)])
            self.children.append((proc, stop))

    def drain(self) -> None:
        for _, stop in self.children:
            stop.touch()


def _supervise(cmd: list[str], n: int, *, server: str, token: str,
               follow_allocation: bool = True) -> int:
    """Keep the worker pool at its target. With ``follow_allocation`` (self-play's
    ``remote-cpu`` pool) the target is this host's admin Compute allocation, polled and
    reported every ~30 s; otherwise (e.g. one ``remote-gpu`` worker beside it) it stays
    at ``n`` and the allocation, which describes self-play, is only read for the host's
    excluded CPUs (faulty cores must run nothing, whatever the pool)."""
    stopping = False

    def on_stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, on_stop)
    signal.signal(signal.SIGTERM, on_stop)
    host = host_name(socket.gethostname())
    logical = os.cpu_count() or 1
    excluded = default_excluded(host)
    target = min(n, len(allowed_cpus(logical, excluded)))
    if sys.platform == "darwin":
        log.info("macOS has no CPU affinity API; exclusions only reduce the worker budget")
    with tempfile.TemporaryDirectory(prefix="remote-workers-") as directory, httpx.Client(
        base_url=server, timeout=10, headers={"Authorization": f"Bearer {token}"}
    ) as client:
        pool = WorkerPool(cmd, Path(directory), logical)
        next_poll = 0.0
        while True:
            if stopping:
                pool.drain()
                if pool.running == 0:
                    return 0
                time.sleep(1)
                continue
            if time.monotonic() >= next_poll:
                try:
                    if follow_allocation:
                        response = client.post("/api/remote-workers/report", json={
                            "host": host, "logical_cpus": logical,
                            "running_processes": pool.running, "starting_processes": n,
                        })
                    else:
                        response = client.get("/api/remote-workers/allocation",
                                              params={"host": host})
                    response.raise_for_status()
                    allocation = response.json()
                    # Validate the entire response before applying any of it.
                    if (not isinstance(allocation, dict)
                            or not isinstance(allocation.get("excluded_cpus"), list)
                            or any(type(c) is not int or c < 0
                                   for c in allocation["excluded_cpus"])
                            or follow_allocation and any(
                                type(allocation.get(k)) is not int or allocation[k] < 0
                                for k in ("selfplay_processes", "training_threads",
                                          "usable_cpus"))):
                        raise ValueError("invalid allocation response")
                    if follow_allocation:
                        target = allocation_target(target, allocation)
                    excluded = sorted(
                        set(allocation["excluded_cpus"]) | set(default_excluded(host)))
                    target = min(target, len(allowed_cpus(logical, excluded)))
                except (httpx.HTTPError, ValueError):
                    log.warning("allocation poll failed; keeping target %d", target, exc_info=True)
                next_poll = time.monotonic() + random.uniform(25, 35)
            try:
                pool.reconcile(target, excluded)
            except (OSError, ValueError, RuntimeError):
                log.exception("could not reconcile worker pool; retrying")
            time.sleep(1)


def main(argv: list[str] | None = None, *, module: str = "core.jobs.remote_worker") -> int:
    """CLI entry. ``module`` is what ``--processes`` children run (the app
    composition root passes itself so the children register the kinds too)."""
    ap = argparse.ArgumentParser(description="Run jobs for a server on another machine.")
    ap.add_argument("--server", required=True, help="e.g. http://victors-macbook-pro-2:8765")
    ap.add_argument("--capability", required=True, help="comma-separated remote-* capabilities")
    ap.add_argument("--processes", type=int, default=1,
                    help="worker processes to run (each runs one job at a time)")
    ap.add_argument("--stop-file", type=Path, help=argparse.SUPPRESS)
    ap.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    argv = sys.argv[1:] if argv is None else argv
    args = ap.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per poll otherwise
    token = os.environ.get("REMOTE_WORKER_TOKEN")
    if not token:
        print("REMOTE_WORKER_TOKEN must be set (the same value as on the server)", file=sys.stderr)
        return 2
    if args.processes < 0:
        ap.error("--processes must be nonnegative")
    capabilities = [c.strip() for c in args.capability.split(",") if c.strip()]
    if args.stop_file is None:
        # Only the self-play pool follows the host's Compute allocation; a GPU pool
        # keeps --processes (one worker per GPU), however many self-play slots exist.
        return _supervise([sys.executable, "-m", module, *argv], args.processes,
                          server=args.server, token=token,
                          follow_allocation="remote-cpu" in capabilities)

    async def run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):  # signal.signal also works on Windows
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
        async with httpx.AsyncClient(base_url=args.server, timeout=30,
                                     headers={"Authorization": f"Bearer {token}"}) as client:
            await run_remote_worker(client, capabilities, stop_event=stop, stop_file=args.stop_file)

    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
