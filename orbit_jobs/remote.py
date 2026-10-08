"""Server side of remote workers: other machines run jobs over HTTP.

A remote worker (``orbit_jobs.remote_worker``, e.g. a spare box on the
tailnet) can't open the server's database, so the server claims, heartbeats and
finishes jobs on its behalf with the same store calls a local worker
makes. The orphan sweep, retry and cancel paths therefore apply unchanged.

Only ``remote-*`` capabilities may be claimed remotely, and local workers
are never spawned for them, so routing a kind to another machine is a
declared choice: ``KindDescriptor(capability="remote-cpu", ...)``.

Remote kinds must be pure compute (payload in, result dict out). Their
handler runs without a ``JobHandle``, so it records no steps, and any
``get_db()`` call would hit the remote machine's own database. CPU-bound
work belongs in ``asyncio.to_thread`` so the lease heartbeat keeps running.

Files come back as **artifacts**: a handler lists them in its result as
``{"artifacts": {name: local_path}}``; the worker uploads each one (and
deletes its local copy) before finishing, and the stored result maps each
name to its path under :func:`artifacts_root` on the server.

Files go out as **inputs**: a job's payload may carry
``{"inputs": {local_name: {"ref": "<root>:<relative path>", "size": n}}}``
(build it with :func:`describe_inputs`). Roots are named directories on this
machine: ``artifacts`` (:func:`artifacts_root`) plus any a product registers
(:func:`register_input_root`). The worker downloads each input before the
handler starts, through ``GET /api/remote-workers/jobs/{id}/inputs``, which
serves the file directly.

A claimed or renewed lease calls the ``on_lease(job, worker_id)`` hook and a
finished one ``on_lease_end(job, worker_id)`` (the host holds a machine's shared
CPUs for its training jobs with them, ``core.compute_allocation``).
"""

from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Callable
from pathlib import Path, PurePosixPath

from orbit_jobs import config, events
from orbit_jobs.registry import get_descriptor
from orbit_jobs.retry import maybe_retry
from orbit_jobs.runner import emit_activity_for_job_finish
from orbit_jobs.store import (
    TERMINAL_STATUSES,
    cancel_requested,
    claim_one,
    get,
    heartbeat,
    mark_terminal,
    worker_heartbeat,
    worker_unregister,
)

log = logging.getLogger(__name__)

REMOTE_CAPABILITY_PREFIX = "remote-"

__all__ = [
    "REMOTE_CAPABILITY_PREFIX",
    "is_remote_capability",
    "git_head",
    "claim",
    "claim_compatible",
    "job_heartbeat",
    "finish",
    "unregister",
    "artifacts_root",
    "store_artifact",
    "register_input_root",
    "resolve_input",
    "describe_inputs",
    "input_location",
]


def is_remote_capability(capability: str) -> bool:
    return capability.startswith(REMOTE_CAPABILITY_PREFIX)


def git_head() -> str | None:
    """Committed code version of this checkout, or None without git."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=config.settings.code_root or Path.cwd(),
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return out.stdout.strip() or None


def claim(
    worker_id: str,
    capabilities: list[str],
    pid: int,
    code_version: str | None = None,
    server_head: str | None = None,
) -> dict | None:
    """Record the worker as alive and claim its next job, if any.

    With ``code_version`` (the worker's commit), the worker only gets jobs
    pinned to that commit (``payload.code_version``, e.g. every shard of a
    training run), plus unpinned jobs when it equals ``server_head``. A
    pinned run therefore keeps going while the server's checkout moves on.
    """
    if not capabilities or not all(is_remote_capability(c) for c in capabilities):
        raise ValueError(f"remote workers only claim {REMOTE_CAPABILITY_PREFIX}* capabilities")
    worker_heartbeat(worker_id, capabilities[0], pid)
    if code_version is None:
        job = claim_one(capabilities, worker_id)
    else:
        job = claim_one(capabilities, worker_id, code_version=code_version,
                        allow_unpinned=server_head is not None and code_version == server_head)
    config.settings.on_lease(job, worker_id)
    return job


def claim_compatible(
    worker_id: str,
    capabilities: list[str],
    pid: int,
    code_version: str,
    protocols: dict[str, int],
) -> tuple[dict | None, list[str]]:
    """Claim the next job of a kind whose protocol matches this server's.

    ``protocols`` maps each kind the worker can run to its ``protocol``
    version. Unpinned jobs of matching kinds are claimable at any commit;
    jobs pinned to a commit (``payload.code_version``) still need exactly
    that commit. Returns the job (or None) and the kinds the worker offered
    that this server won't hand it (unknown here, or another protocol).
    """
    if not capabilities or not all(is_remote_capability(c) for c in capabilities):
        raise ValueError(f"remote workers only claim {REMOTE_CAPABILITY_PREFIX}* capabilities")
    worker_heartbeat(worker_id, capabilities[0], pid)
    compatible, stale = [], []
    for kind, version in sorted(protocols.items()):
        desc = get_descriptor(kind)
        if desc is not None and desc.protocol == version:
            compatible.append(kind)
        else:
            stale.append(kind)
    job = claim_one(capabilities, worker_id, code_version=code_version, allow_unpinned=True,
                    kinds=compatible)
    config.settings.on_lease(job, worker_id)
    return job, stale


def job_heartbeat(job_id: int, worker_id: str, pid: int) -> dict:
    """Refresh a running job's lease. ``owned`` False means the lease is
    gone (swept or cancelled) and the worker should drop the job."""
    owned = heartbeat(job_id, worker_id=worker_id)
    if owned:
        job = get(job_id)
        worker_heartbeat(worker_id, job["required_capability"], pid)
        config.settings.on_lease(job, worker_id)
    return {"owned": owned, "cancel": owned and cancel_requested(job_id)}


def finish(
    job_id: int,
    worker_id: str,
    status: str,
    *,
    result: dict | None = None,
    error: str | None = None,
) -> bool:
    """Finish a job the worker still holds. Mirrors the local worker's
    finish path: terminal row, opt-in retry, job event, activity feed."""
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"status must be one of {TERMINAL_STATUSES}")
    job = get(job_id)
    if job is None:
        return False
    if status == "succeeded":
        result, error = result or {}, None
    if not mark_terminal(job_id, status, result=result, error=error, worker_id=worker_id):
        return False
    config.settings.on_lease_end(job, worker_id)
    if status == "failed":
        try:
            retry_id = maybe_retry(job, job.get("payload") or {})
            if retry_id is not None:
                log.info("remote job %s failed — spawned retry job %s", job_id, retry_id)
        except Exception:  # noqa: BLE001
            log.exception("retry spawn failed for remote job %s", job_id)
    events.emit(job_id, {
        "type": "job.finished", "status": status, "error": error, "result": result,
    })
    try:
        emit_activity_for_job_finish(job_id=job_id, kind=job["kind"], status=status, error=error)
    except Exception:  # noqa: BLE001
        log.debug("activity-feed emit failed for job %s", job_id, exc_info=True)
    return True


def unregister(worker_id: str) -> None:
    worker_unregister(worker_id)


def artifacts_root() -> Path:
    """Where uploaded artifacts land (``REMOTE_ARTIFACTS_DIR`` overrides)."""
    return Path(os.environ.get("REMOTE_ARTIFACTS_DIR")
                or Path.home() / ".local/share/automations/remote-artifacts")


def store_artifact(job_id: int, worker_id: str, name: str, data: bytes) -> Path | None:
    """Save a file a remote job produced at ``artifacts_root()/name``.

    ``name`` is a relative POSIX path (``run-7/shard-00012.npz``); anything
    that could escape the root raises ValueError. Written via a temp file and
    rename, so readers only ever see complete files. Returns None when the
    worker no longer holds the job.
    """
    parts = _relative_parts(name, "artifact name")
    job = get(job_id)
    if job is None or job["status"] != "running" or job.get("worker_id") != worker_id:
        return None
    dest = artifacts_root().joinpath(*parts)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{job_id}.tmp")
    tmp.write_bytes(data)
    tmp.replace(dest)
    return dest


def _relative_parts(name: str, what: str) -> tuple[str, ...]:
    """``name`` as path parts, refusing anything that could leave its root."""
    rel = PurePosixPath(name)
    if not name or rel.is_absolute() or any(part in ("", ".", "..") for part in rel.parts) \
            or "\\" in name or ":" in rel.parts[0]:
        raise ValueError(f"bad {what} {name!r}")
    return rel.parts


# -- inputs -------------------------------------------------------------------

_INPUT_ROOTS: dict[str, Callable[[], Path]] = {"artifacts": artifacts_root}


def register_input_root(name: str, path: Callable[[], Path]) -> None:
    """Let jobs reference files under ``path()`` as ``"<name>:<relative path>"``."""
    if not name or ":" in name or "/" in name:
        raise ValueError(f"bad input root name {name!r}")
    _INPUT_ROOTS[name] = path


def resolve_input(ref: str) -> Path:
    """The local file an input ref names (it need not exist)."""
    root, sep, rel = ref.partition(":")
    if not sep or root not in _INPUT_ROOTS:
        raise ValueError(f"unknown input root in {ref!r}")
    return _INPUT_ROOTS[root]().joinpath(*_relative_parts(rel, "input path"))


def describe_inputs(files: dict[str, str]) -> dict[str, dict]:
    """``{local_name: ref}`` → the payload's ``inputs`` (sizes filled in).

    Raises FileNotFoundError for a missing file, so a job is never queued
    with an input no worker could fetch.
    """
    out = {}
    for local, ref in files.items():
        _relative_parts(local, "input name")
        out[local] = {"ref": ref, "size": resolve_input(ref).stat().st_size}
    return out


def input_location(job_id: int, worker_id: str, name: str) -> tuple[str, str | Path] | None:
    """Where the worker holding ``job_id`` fetches its input ``name``:
    always ``("file", path)``. None when the worker no longer holds the job;
    KeyError for a name the job doesn't list.
    """
    job = get(job_id)
    if job is None or job["status"] != "running" or job.get("worker_id") != worker_id:
        return None
    spec = ((job.get("payload") or {}).get("inputs") or {})[name]
    path = resolve_input(spec["ref"])
    path.stat()  # FileNotFoundError when the file is gone
    return "file", path
