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
orbit-jobs = { git = "https://github.com/vixtah/orbit-jobs", tag = "v0.2.4" }
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
- `orbit_jobs.schedules` — declarative schedules for an app's tick, with failure
  backoff and per-run records that double as heartbeats (below).
- `orbit_jobs.schema.register_schema(conn)` — the `jobs`, `job_steps`,
  `job_cancel_requests`, `worker_heartbeats`, `schedule_runs`, `schedule_state` (and
  orphaned `remote_input_mirror`) tables.

## Schedules and health (`orbit_jobs.schedules`)

"Run X every N minutes" for any app, standard library only; workers are optional.
An `Entry(name, when, action)` runs on `Every(minutes)` (once per clock bucket),
`Daily(at=(h, m), weekday=None, utc=False)` (once a day, or a week with `weekday`)
or `TICK` (every tick). The action is `action(now)` (sync or async, on the event
loop) or `Job(kind, payload)`, which spawns an orbit_jobs job and takes the run's
outcome from the job; an entry whose job (scheduled or manual, same kind and
subject) is still pending or running is not started again.

```python
from orbit_jobs.schedules import Backoff, Daily, Entry, Every, Job, health, run_due

ENTRIES = (
    Entry("refresh-planner", Every(5), plan_refreshes, title="Venue refresh planner",
          kind="loop", where="VPS"),
    Entry("news-ingest", Every(360), Job("pipeline_run", {"pipeline": "news-ingest"},
          subject_type="pipeline", subject_id="news-ingest")),
    Entry("morning-brief", Daily((9, 30)), spawn_brief, backoff=Backoff(15, 60)),
)

async def scheduler_loop():
    while True:
        await run_due(ENTRIES)          # each entry isolated; prunes old runs hourly
        await asyncio.sleep(60)

def heartbeats():                       # the services list of GET /heartbeats
    return health(ENTRIES)
```

- **Claim before run.** A timed entry's period is written to `schedule_state` (a
  compare-and-set, so two processes never both claim it) and a `running` row to
  `schedule_runs` before the action starts. A restart mid-run doesn't repeat it;
  the cut-off run is closed as `skipped` and the entry runs next period. An action
  returns `RETRY` to leave the period unclaimed (no record) or `SKIP` to record a
  `skipped` run. `enabled=` gates an entry without claiming anything.
- **Backoff.** A failed run frees its period and waits `first_minutes` (60),
  doubling per consecutive failure up to `max_minutes` (1440): 1, 2, 4, 8, 16, then
  24 h, whatever the entry's interval (a 6-hourly entry retries after 1 h, a daily
  or monthly one within the day). A success resets it; the state is in the
  database, so it survives restarts. Override per entry with
  `backoff=Backoff(first, max)`; `Backoff(at_least_interval=True)` keeps the v0.2.1
  rule (never sooner than the interval, capped at `max_minutes`); `NO_BACKOFF`
  retries when next due. TICK
  entries don't back off unless given one. A job that ends `cancelled` (e.g. a
  restart's `cancel_orphans`) is recorded `skipped` and doesn't count as a failure.
- **Run records.** `schedule_runs`: one row per run (`name`, `period`, `status`
  running | ok | failed | skipped, `started_at`, `finished_at`, `duration_ms`,
  `error` cut to 300 characters, `job_id`), kept 7 days (`run_due` prunes hourly;
  `prune()` on demand), except each name's latest run and latest success, so a
  monthly entry keeps its `last_ok_at`.
- **Error text.** A failure is recorded as `Type: message`; raise
  `RunFailed("3 of 40 venues failed")` (or a subclass) from an action or a
  `record_run` block to record just the message. Every error is redacted before it
  is stored and again when `health()` reports it (`orbit_jobs.redact.redact`): URL
  query strings, fragments and `user:password@` are removed, and `Bearer` / `Basic`
  credentials, the value after a secret-looking name (`token`, `x-app-token`,
  `api_key`, `client_secret`, `password`, `authorization`, `cookie`, `session`...)
  and long opaque strings (32+ characters mixing letters and digits) become
  `[redacted]`. The `redact_error` hook replaces the rule (a stricter one, or
  `lambda text: text`); a hook that raises withholds the text.
- **Health.** `health(entries, tick_minutes=1)` (entries and/or `Service`s, below)
  returns per item `id`, `name` (`title` or the name), `expected_every_minutes`
  (TICK reports `tick_minutes`, a service its declared value; an entry may declare
  one with `expected_every_minutes=`, and `None` means on demand), `last_run_at`,
  `last_ok_at`, `last_status` (`unknown` before any run), `last_error` (the latest
  failure's), `last_duration_ms`, `runs_24h`, `failures_24h`, plus `kind` / `where` when the entry declares them: the service
  shape of the `/heartbeats` contract (automations `docs/plans/HEARTBEATS.md`).
  No payloads or results are included.

### Hand-written loops (`Service`, `record_run`)

A loop that doesn't go through `run_due` (a `while True` sweeper, an always-on
worker) declares a `Service` and wraps each iteration in `record_run`. Its runs
land in the same `schedule_runs` table and `health()` takes entries and services
together:

```python
from orbit_jobs.schedules import Service, health, record_run

SWEEPER = Service("fleet-sweeper", expected_every_minutes=2, title="Fleet sweeper",
                  kind="loop", where="VPS")

async def sweeper_loop():
    while True:
        try:
            async with record_run(SWEEPER):     # or `with record_run("fleet-sweeper"):`
                await sweep_fleet()
        except Exception:
            log.exception("sweep failed")       # already recorded as failed
        await asyncio.sleep(120)

def heartbeats():
    return health([*ENTRIES, SWEEPER])
```

`record_run` writes a `running` row on entry, then `ok`, or `failed` with the
error cut to 300 characters (the exception still propagates); a cancellation is
recorded `skipped`. There are no periods or backoff: the loop keeps its own timing.
Recording never breaks the work: if the job database can't be written when a run
opens or closes, a warning is logged, the block still runs, and its own result or
exception reaches the caller unchanged.

Work that runs only when something asks for it (a cache rebuilt on a request) is a
service with no cadence: `Service("price-history", expected_every_minutes=None)`.
`health()` reports `expected_every_minutes: null`, which the heartbeat contract
reads as "on demand": never late, only a failed last run matters. Wrap only the
real refresh in `record_run`, not the cache hits.
One name runs one iteration at a time; a row still `running` when the next starts
(the process stopped) is closed as `skipped`. Old records are pruned hourly, as
with `run_due`.

### Migrating an existing scheduler

Keep entry names and period formats: `Every.period` is
`str(int(timestamp) // (60 * minutes))` and `Daily.period` is `YYYY-MM-DD`, the
same as the automations registry (`server/schedule_registry.py`). Before the first
`run_due`, import the periods once so nothing that already ran this period runs
again: the automations host keeps them in the `scheduler_entry_periods`
preference, so

```python
from orbit_jobs.schedules import import_periods
import_periods(preferences.get("scheduler_entry_periods") or {})   # once; existing state wins
```

after which the preference is no longer read. An app that also tracked failures
keeps its pending retry and failure count with
`import_periods(periods, failures={name: n}, retry_at={name: datetime})` (a naive
time is UTC; leave a retrying entry's period out, as a failed run releases it). Entries with `legacy_pref` should be
folded into that dict first (as its `load_state` does). Note the behaviour change:
a raising timed action used to be retried every tick; it now backs off (1 h, then
doubling; give the morning brief `backoff=Backoff(15, 60)` to retry sooner). Apps with their own run tables (Reserve's `runs`, CardScanner's
`schedule:<name>` meta) can seed `schedule_state.period` the same way, or simply
let each entry run once on the first tick.

Bring the old run history too, or `health()` starts empty: the first run shows as
`running` with no success yet, which a heartbeat collector reads as late.
`import_runs(runs)` takes the past runs as mappings with `name`, `started_at`,
`status` (ok | failed | skipped; `running`, a run whose process stopped, becomes
`skipped`) and optionally `finished_at`, `error` and `duration_ms`; times are
datetimes (naive = UTC), ISO text or epoch seconds:

```python
from orbit_jobs.schedules import import_runs
import_runs({"name": r["service"], "started_at": r["started_at"], "finished_at": r["finished_at"],
             "status": r["status"], "error": r["error"]} for r in old_rows)   # every start is fine
```

It is idempotent (a run with the same name and start second is skipped), redacts
errors like any run, and keeps only what pruning would keep (the last 7 days, plus
each name's latest run and latest success), so `last_ok_at`, `last_error` and the
24-hour counts are right from the first request. `health()` orders runs by start
time, so history imported after new runs doesn't hide them.

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
| `redact_error(text)` | `orbit_jobs.redact.redact` | (default) |
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
