"""HTTP routes remote workers use to claim and finish jobs (FastAPI; optional).

:func:`remote_worker_router` builds the router a server mounts. Off unless the
token env var (``REMOTE_WORKER_TOKEN``) is set; every call must then send it as
``Authorization: Bearer <token>``. The check is a router-level dependency, so
routes a host adds to the returned router are protected too. A worker that
sends ``protocols`` (its kinds' ``protocol`` versions) gets unpinned jobs of
every kind whose version matches this server's, whatever commit either side is
on, plus jobs pinned to its exact git HEAD (``payload.code_version``, e.g. a
training run's shards). The response lists the kinds it offered but can't have
(``stale_kinds``). An older worker that sends only its HEAD gets pinned jobs for
that commit, or unpinned jobs when its HEAD matches this checkout's; with
nothing to run and a different HEAD, its claim is refused (409).
Logic lives in ``orbit_jobs.remote``; ``/source`` serves ``orbit_jobs.source``.
"""

from __future__ import annotations

import asyncio
import hmac
import os

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel

from orbit_jobs import remote, source

__all__ = ["remote_worker_router"]


class ClaimRequest(BaseModel):
    worker_id: str
    capabilities: list[str]
    pid: int
    code_version: str
    protocols: dict[str, int] | None = None


class HeartbeatRequest(BaseModel):
    worker_id: str
    pid: int


class FinishRequest(BaseModel):
    worker_id: str
    status: str
    result: dict | None = None
    error: str | None = None


def remote_worker_router(*, prefix: str = "/api/remote-workers",
                         token_env: str = "REMOTE_WORKER_TOKEN") -> APIRouter:
    """A router with the claim / heartbeat / finish / artifact / input /
    unregister / source routes, each behind the bearer token in ``token_env``."""

    def require_token(authorization: str | None = Header(default=None)) -> None:
        token = os.environ.get(token_env, "")
        if not token:
            raise HTTPException(404, f"remote workers are disabled ({token_env} unset)")
        if not hmac.compare_digest(authorization or "", f"Bearer {token}"):
            raise HTTPException(401, "bad remote worker token")

    router = APIRouter(prefix=prefix, dependencies=[Depends(require_token)])

    @router.post("/claim")
    async def claim(body: ClaimRequest):
        keys = ("id", "kind", "payload", "timeout_seconds", "required_capability", "retry_count")
        if body.protocols is not None:
            try:
                job, stale = await asyncio.to_thread(
                    remote.claim_compatible, body.worker_id, body.capabilities, body.pid,
                    body.code_version, body.protocols)
            except ValueError as exc:
                raise HTTPException(400, str(exc)) from exc
            return {"job": None if job is None else {k: job.get(k) for k in keys},
                    "stale_kinds": stale}
        head = await asyncio.to_thread(remote.git_head)
        try:
            job = await asyncio.to_thread(remote.claim, body.worker_id, body.capabilities,
                                          body.pid, body.code_version, head)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        if job is None and body.code_version != head:
            raise HTTPException(409, {
                "error": "code version mismatch", "server": head, "worker": body.code_version,
            })
        if job is None:
            return {"job": None}
        return {"job": {k: job.get(k) for k in keys}}

    @router.post("/jobs/{job_id}/heartbeat")
    async def job_heartbeat(job_id: int, body: HeartbeatRequest):
        state = await asyncio.to_thread(remote.job_heartbeat, job_id, body.worker_id, body.pid)
        if not state["owned"]:
            raise HTTPException(409, "job is no longer held by this worker")
        return {"cancel": state["cancel"]}

    @router.post("/jobs/{job_id}/finish")
    async def finish(job_id: int, body: FinishRequest):
        try:
            done = await asyncio.to_thread(
                remote.finish, job_id, body.worker_id, body.status,
                result=body.result, error=body.error,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        if not done:
            raise HTTPException(409, "job is no longer held by this worker")
        return {"ok": True}

    @router.put("/jobs/{job_id}/artifacts")
    async def upload_artifact(job_id: int, name: str, worker_id: str, request: Request):
        """Raw request body = the file. Saved under ``remote.artifacts_root()``."""
        data = await request.body()
        try:
            path = await asyncio.to_thread(remote.store_artifact, job_id, worker_id, name, data)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        if path is None:
            raise HTTPException(409, "job is no longer held by this worker")
        return {"path": str(path)}

    @router.get("/jobs/{job_id}/inputs")
    async def job_input(job_id: int, name: str, worker_id: str):
        """One input file of a held job."""
        try:
            where = await asyncio.to_thread(remote.input_location, job_id, worker_id, name)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except (KeyError, FileNotFoundError) as exc:
            raise HTTPException(404, f"no input {name!r} for job {job_id}") from exc
        if where is None:
            raise HTTPException(409, "job is no longer held by this worker")
        _kind, target = where
        return FileResponse(target)

    @router.post("/unregister")
    async def unregister(body: HeartbeatRequest):
        await asyncio.to_thread(remote.unregister, body.worker_id)
        return {"ok": True}

    @router.get("/source")
    async def source_archive(commit: str):
        """Gzipped ``git archive`` of ``commit``, for workers without git (fleet nodes)."""
        try:
            data = await asyncio.to_thread(source.archive, commit)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        return Response(data, media_type="application/gzip",
                        headers={"Cache-Control": "no-store"})

    return router
