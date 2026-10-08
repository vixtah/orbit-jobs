# orbit-jobs

A SQLite-backed job queue with capability-scoped workers, per-step telemetry and
remote workers over HTTP. Import package: `orbit_jobs`. The queue, local worker and
telemetry need only the standard library (Python 3.12+); the remote worker needs
`httpx` (extra `remote`) and the server routes need FastAPI (extra `server`).

It was extracted from the automations host (`core/jobs`) so the host and the
Riftbound product can share one bounded job package. It imports nothing from
either; an import-linter contract in the root `pyproject.toml` enforces that.

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
| `transport` | none: workers poll | `core.mesh` (instant wake-ups) |
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

- The fleet (`core/fleet`), compute allocation and the NATS mesh (`core/mesh.py`)
  stay in the host and will move with Riftbound; they plug in through the hooks.
- Host read models: Tickets views over chats (`core.jobs.views`), the fleet's
  per-machine activity and `compute_hosts` (`core.jobs.activity`), the host's
  worker list (`core.jobs.capabilities`) and spawn kill switches
  (`core.jobs.spawn_config`).
- Job kinds. Composition roots register them.

## In the automations host

`core.jobs` is the host facade. Importing it calls `core.jobs.host.install()`, and
each moved module path (`core.jobs.store`, `core.jobs.worker`, ...) and
`core.runctx` is a stub that replaces itself in `sys.modules` with the
`orbit_jobs` module, so both names are the same module object and
`monkeypatch.setattr` works on either.

## Tests

```bash
cd packages/orbit-jobs && python -m pytest      # standalone, no host plugin
```

`tests/test_standalone.py` runs a job end to end in a subprocess with only this
package on the path and no site-packages. The root suite also collects these tests.

## Deploying the split (once)

Every interpreter that imports `core` needs `orbit_jobs` importable; without it
`core.jobs` and `core.runctx` fail to import. Don't `uv sync` the Mac's main
checkout (its `.venv` carries torch from the `train` group).

- **Mac, main checkout** — merge and install in one go, then restart:

  ```bash
  cd ~/Projects/automations
  git merge jobs-package \
    && uv pip install --python .venv/bin/python -e packages/orbit-jobs --no-deps \
    && uv pip install --python .venv-voice/bin/python -e packages/orbit-jobs --no-deps
  .venv/bin/python -c "import core.jobs, core.runctx, orbit_jobs.runctx as r; assert core.runctx is r"
  .venv-voice/bin/python -c "import core.runctx"
  ```

  Then restart the web server, the workers and the voice sidecar. Workers that
  notice the new commit on their own exec only after `import app.kinds` succeeds in
  a fresh interpreter, so they wait for the install instead of crashing.
- **Riftbound thinker checkouts** (`rb-think`, their own `.venv`): `uv sync` as usual
  when moved to the new commit.
- **VPS**: `deploy/vps/deploy.sh` runs `uv sync --frozen --no-dev --package riftbound`,
  which installs `orbit-jobs` (a riftbound dependency) and import-checks.
- **Remote worker machines** (`victorhyte`): `uv sync` (with the extras that machine
  uses, e.g. `--all-packages --extra train`) or
  `uv pip install -e packages/orbit-jobs --no-deps` right after pushing the commit:
  workers exit when the checkout moves and fail on `import orbit_jobs` until then.
  Fleet nodes run `uv sync --frozen` at boot and need nothing.
