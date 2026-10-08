# orbit-jobs

A SQLite-backed job queue with capability-scoped workers, per-step telemetry and
remote workers over HTTP. Import package: `orbit_jobs`. The queue, local worker and
telemetry need only the standard library (Python 3.12+); the remote worker needs
`httpx` (extra `remote`), the server routes need FastAPI (extra `server`) and the
optional NATS transport needs `nats-py` (extra `mesh`).

It was extracted from the automations monorepo (`core/jobs`, `core/mesh.py`) so the
automations assistant and Riftbound, now separate repositories, share one bounded job
package. It imports nothing from either. Both depend on it by git tag:

```toml
[project]
dependencies = ["orbit-jobs[remote,server,mesh]"]

[tool.uv.sources]
orbit-jobs = { git = "https://github.com/vixtah/orbit-jobs", tag = "v0.1.0" }
```

## Public API

```python
import orbit_jobs
from orbit_jobs import KindDescriptor, register_kind, spawn

async def double(job_id: int, payload: dict) -> dict:
    return {"doubled": payload["n"] * 2}

register_kind(KindDescriptor(kind="double", label="Double", description="x2",
                             handler=double, capability="cpu"))
job_id = spawn("double", {"n": 21})        # pending until a "cpu" worker claims it
```

- `orbit_jobs` — `configure`, `register_kind` / `KindDescriptor`, `spawn` /
  `spawn_child`, `cancel` / `request_cancel`, `get` / `list_jobs` / `list_steps`,
  `current` / `begin_step` / `end_step` / `JobHandle`, `subscribe` /
  `subscribe_global` (per-job SSE events), `cancel_orphans` / `recover_orphans`,
  `claim_one`, `run_claimed_job`.
- `orbit_jobs.worker` — the capability worker: `run_worker(capabilities, ...)`,
  `main()` (`--capability a,b`). Heartbeats, timeouts, cross-process cancel, opt-in
  retries, orphan sweeps, exec-on-code-drift and exec-after-dirty-job.
- `orbit_jobs.remote` / `orbit_jobs.remote_worker` — `remote-*` capabilities run on
  other machines over HTTP: lease heartbeats, artifacts (uploads), inputs
  (downloads, cached), protocol versions, a CPU-pinning process supervisor.
- `orbit_jobs.routes.remote_worker_router()` — the FastAPI router a server mounts
  for them (bearer token from `REMOTE_WORKER_TOKEN`; routes the caller adds to the
  returned router get the same check).
- `orbit_jobs.source` — `git archive` of a commit, for workers without git.
- `orbit_jobs.runctx` — the active-job ContextVar domain code reads (`current()`)
  without importing the job system.
- `orbit_jobs.mesh` — the optional NATS link (`NATS_URLS`; off and free without it):
  `publish` / `subscribe` hints, `serve` / `request_sync` request-reply, `start` /
  `stop`. Pass it as the `transport` hook for instant worker wake-ups. Subjects are
  `<NATS_SUBJECT_PREFIX>.<tokens>` (default prefix `automations`, kept so processes
  of both repositories on one NATS server keep talking to each other).
- `orbit_jobs.schema.register_schema(conn)` — the `jobs`, `job_steps`,
  `job_cancel_requests`, `worker_heartbeats` (and orphaned `remote_input_mirror`)
  tables.

## Hooks (`orbit_jobs.configure`)

A composition root sets these once, before importing `orbit_jobs.worker` (which
records its code version on import). Every hook has a self-contained default.

| Hook | Default | The automations host (`core/jobs/host.py`) |
|------|---------|--------------------------------------------|
| `get_db`, `now_iso` | own SQLite file (`ORBIT_JOBS_DB`, default `~/.local/share/orbit-jobs/jobs.db`; WAL, 30 s busy timeout, tables created on open), UTC clock | `core.db.get_db` / `now_iso` (tasks.db) |
| `make_bus(name, queue_size=)` | in-process bus (`orbit_jobs.bus`) | `core.events.bus.Bus` (bridged over NATS) |
| `transport` | none: workers poll | `orbit_jobs.mesh` (instant wake-ups) |
| `on_job_finished(job_id=, kind=, status=, error=)` | nothing | activity feed (`core.events.feed`) |
| `reap_children()` | nothing | `kill_child_process_tree` (Playwright) |
| `pause_claiming(capabilities)` | never | ollama workers pause while voice is busy |
| `on_worker_start(capabilities)` | nothing | `core.logsink.install` |
| `host_name(hostname)`, `excluded_cpus(host)` | short hostname, none | `core.compute_allocation` |
| `on_lease(job, worker_id)`, `on_lease_end(job, worker_id)` | nothing | hold/release a machine's CPUs for `remote-gpu` jobs |
| `code_root`, `code_dirs` | working directory, no dirs | repo root, `core`, `app` and `packages/orbit-jobs/orbit_jobs` |
| `import_probe`, `worker_module` | none, `orbit_jobs.worker` | `app.kinds`, `core.jobs.worker` |

Environment variables are unchanged from the host: `JOBS_EVENT_QUEUE_SIZE`,
`AUTOMATIONS_WORKER_MODULE`, `REMOTE_WORKER_TOKEN`, `REMOTE_ARTIFACTS_DIR`,
`REMOTE_WORK_DIR`, `REMOTE_INPUT_CACHE`, `REMOTE_INPUT_CACHE_GB`,
`AUTOMATIONS_CODE_VERSION`, `LOG_LEVEL`; plus `ORBIT_JOBS_DB` for the default
connector.

## Not included

- The training fleet, compute allocation and Riftbound's remote-worker routes live in
  the Riftbound repository and plug in through the hooks.
- Host read models (Tickets views, the host's worker list, spawn kill switches).
- Job kinds. Composition roots register them.

## Composition roots

- **automations**: `core.jobs` is the host facade. Importing it calls
  `core.jobs.host.install()`, and each moved module path (`core.jobs.store`,
  `core.jobs.worker`, ..., `core.runctx`, `core.mesh`) is a stub that replaces itself in
  `sys.modules` with the `orbit_jobs` module, so both names are the same module object
  and `monkeypatch.setattr` works on either.
- **riftbound**: `riftbound.jobs_host.install()` (its own job database, the mesh,
  compute allocation), called by every Riftbound entry point.

## Tests

```bash
uv sync && uv run pytest        # standalone, no host
```

`tests/test_standalone.py` runs a job end to end in a subprocess with only this
package on the path and no site-packages. `tests/test_mesh.py` starts a throwaway
`nats-server` when one is installed (`brew install nats-server`) and skips otherwise.

## Releasing

Tag a version (`git tag v0.1.1 && git push origin v0.1.1`), then bump the `tag` in each
consumer's `[tool.uv.sources]` and `uv lock --upgrade-package orbit-jobs` there.
Installed environments that don't `uv sync` (the Mac's `.venv-voice`) need
`uv pip install "orbit-jobs @ git+https://github.com/vixtah/orbit-jobs@v0.1.1"`.
