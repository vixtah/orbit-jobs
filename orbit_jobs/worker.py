"""Capability-scoped job worker.

A worker is a long-running process (started by launchd / systemd /
manually) that polls the ``jobs`` table for pending rows whose
``required_capability`` matches what the worker advertises, then runs
the registered handler. Survives FastAPI restart — that's the whole
point of being a separate process.

Lifecycle:

1. ``--capability ollama`` (or comma-separated list) — what queues to drain.
2. Loop: ``claim_one`` → if claimed, run handler with timeout + heartbeat;
   else idle 1s and check again.
3. SIGTERM / SIGINT — finish current job if any, then exit cleanly.

Handlers are the same ``async (job_id, payload) -> dict`` shape as the
FastAPI in-process runner uses, so a kind can move between in-process
and worker-routed by toggling ``required_capability`` at spawn time.
No handler code change required.

Run a worker through the app composition root:

    uv run python -m app.worker --capability ollama

Or, with multiple capabilities multiplexed onto one process (fine when
they don't compete for the same scarce resource):

    uv run python -m app.worker --capability db,scraper-imsg

The Phase 1 default is one process per capability, started by launchd.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from core import mesh
from core.jobs import events, wake
from core.jobs.capture import clear_handle, start_handle
from core.jobs.registry import get_descriptor, get_handler, list_kinds
from core.jobs.retry import maybe_retry
from core.jobs.runner import (
    emit_activity_for_job_finish,
    register_handle,
    unregister_handle,
)
from core.jobs.store import (
    cancel_requested,
    claim_one,
    heartbeat,
    mark_terminal,
    recover_orphans,
    worker_heartbeat,
    worker_unregister,
)
from core.sources.browsers import kill_child_process_tree

log = logging.getLogger("jobs.worker")

# How often a busy worker refreshes ``last_heartbeat_at``. The orphan
# sweep on FastAPI startup uses 60 s as the cutoff, so anything <30 s
# gives plenty of margin.
HEARTBEAT_INTERVAL_SECONDS = 10

# How long to sleep between claim attempts when the queue is idle. Low
# enough that perceived pickup latency is invisible (~1 s), high enough
# that an idle worker is essentially free.
IDLE_SLEEP_SECONDS = 1.0

# Don't auto-restart during the first MIN_UPTIME_BEFORE_RESTART seconds
# after startup — guards against an edit-time race where the code mtime
# bumps right after import and triggers an immediate exec loop.
MIN_UPTIME_BEFORE_RESTART = 30.0

# Marks a code version derived without a git HEAD.
_NO_GIT = "nogit"


def current_code_version() -> str:
    """Return a short hash representing the running code.

    Prefers the current git HEAD (cheap, monotonic with commits). Falls
    back to a hash of mtime+size for every .py under ``core/`` and
    ``app/`` so non-git installs and uncommitted edits still trigger
    auto-restart.
    """
    repo_root = Path(__file__).resolve().parents[2]
    # Fast path: git HEAD. ``git rev-parse HEAD`` is microseconds.
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        head = out.decode("ascii", "ignore").strip()
        if head:
            # Mix in a digest of any uncommitted .py edits so workers
            # restart on a save even if the user hasn't committed.
            return f"{head[:12]}+{_dirty_digest(repo_root)}"
    except Exception:
        log.debug("git rev-parse failed; falling back to dirty digest",
                  exc_info=True)
    # Tagged so the drift check can tell "no git HEAD available" apart from a
    # real commit change. A bare digest here is indistinguishable from nothing
    # and never equals the ``<head>+<digest>`` shape, so a single slow
    # ``git rev-parse`` used to read as drift and bounce the worker.
    return f"{_NO_GIT}+{_dirty_digest(repo_root)}"


def _dirty_digest(repo_root: Path) -> str:
    """Hash mtime+size of every .py file under core/ and app/."""
    h = hashlib.md5(usedforsecurity=False)
    for sub in ("core", "app"):
        base = repo_root / sub
        if not base.exists():
            continue
        for p in sorted(base.rglob("*.py")):
            try:
                st = p.stat()
            except OSError:
                continue
            h.update(p.relative_to(repo_root).as_posix().encode())
            h.update(f"|{st.st_mtime_ns}|{st.st_size}".encode())
    return h.hexdigest()[:12]


_STARTUP_VERSION = current_code_version()
_STARTUP_TS = time.monotonic()

# When True (set by ``main()``, i.e. a real subprocess worker), a job
# that ends user-cancelled or timed-out triggers an os.execv into a
# fresh process — the only way to reap a zombie ``to_thread`` handler
# thread. Left False for in-process callers (tests).
EXEC_AFTER_DIRTY_JOB = False

# When True (set by ``main()``), the idle loop execs into a fresh
# interpreter when the on-disk code version drifts from this process's
# startup version. Left False for in-process callers (tests) — a test
# driving ``run_worker`` through many idle-loop ticks (liveness/TTL/retry
# tests) would otherwise risk os.execv'ing away the pytest process itself
# once MIN_UPTIME_BEFORE_RESTART has elapsed since this module was first
# imported in the session.
EXEC_ON_CODE_DRIFT = False


def _comparable(before: str, after: str) -> bool:
    """True when two code versions were produced the same way.

    ``current_code_version`` has two shapes: ``<head>+<digest>`` when git
    answers and ``nogit+<digest>`` when it does not. Comparing across the two
    says nothing about whether the code changed.
    """
    return (before.startswith(f"{_NO_GIT}+")) == (after.startswith(f"{_NO_GIT}+"))


def _maybe_exec_for_new_code() -> None:
    """If the on-disk code version changed since this worker started,
    replace the process with a fresh interpreter so the next jobs run
    the latest code. Called at the top of the idle loop — never
    interrupts a job in flight.

    Uses ``os.execv`` so the PID is preserved (no supervisor required).
    """
    if time.monotonic() - _STARTUP_TS < MIN_UPTIME_BEFORE_RESTART:
        return
    try:
        cur = current_code_version()
    except Exception:
        return
    if cur == _STARTUP_VERSION:
        return
    if not _comparable(_STARTUP_VERSION, cur):
        # One side lost its git component (a transient rev-parse failure).
        # The versions cannot be compared, so drift is unknown — and unknown
        # must not mean restart.
        log.debug("code version not comparable (%s vs %s) — not restarting",
                  _STARTUP_VERSION, cur)
        return
    log.info(
        "code version drift detected (%s -> %s) — restarting worker",
        _STARTUP_VERSION, cur,
    )
    _exec_self(reason=f"code version drift ({_STARTUP_VERSION} -> {cur})")


def _import_health_ok() -> bool:
    """Probe that ``app.kinds`` still imports cleanly in a fresh
    interpreter. The checkout can be transiently broken — e.g. conflict
    markers while the merge coordinator resolves a feature in the live
    tree — and exec'ing into a SyntaxError kills the worker permanently
    (``execv`` has no way back). Every exec/restart trigger must call
    this immediately before exec'ing, not earlier: checking well before
    the exec leaves a TOCTOU window where the tree changes in between.
    """
    try:
        probe = subprocess.run(
            [sys.executable, "-c", "import app.kinds"],
            capture_output=True, text=True, timeout=60,
        )
    except Exception:
        return False
    if probe.returncode != 0:
        log.warning(
            "import health check failed — new code fails to import: %s",
            (probe.stderr or "")[-300:].strip(),
        )
        return False
    return True


def _exec_self(reason: str = "restart requested") -> None:
    """Replace this process with a fresh worker module invocation.

    Uses ``os.execv`` so the PID is preserved (no supervisor required)
    and every thread in the old image — including zombie scrape threads
    left behind by a cancelled/timed-out ``asyncio.to_thread`` node —
    dies with it.

    This is the single point every restart/exec trigger funnels through,
    so the import health check below is the last gate before every exec —
    not just the drift-detection path. (The 2026-08-31 outage: a drift
    restart at 00:59:04 landed workers in an import-broken tree because
    the post-dirty-job exec path called ``os.execv`` directly with no
    import check at all — only the drift path had one, and only for
    itself.) If the check fails, keep running the current in-memory code
    and let the caller retry on a later tick.

    We MUST re-invoke as `python -m <module>` rather than
    `python <argv...>` — when launched with `-m`, sys.argv[0] is the
    path to worker.py, so a naïve execv with argv would re-run it as a
    plain script with sys.path[0] = .../core/jobs/. From there
    `from core.jobs import events` fails (no `core` package above it),
    the worker dies, and a managed parent has to spawn another. We hit
    this on every `git pull` / save: log files filled with
    `ModuleNotFoundError: No module named 'core'` while ingest jobs
    silently produced 0 results.
    """
    if not _import_health_ok():
        log.warning(
            "%s but new code fails to import — deferring restart; will "
            "retry on a later tick",
            reason,
        )
        return
    # execv wipes threads but not child processes — a cancelled/timed-out
    # sync scrape (e.g. Playwright) can leave its node/chromium orphaned,
    # holding the browser profile's SingletonLock. Reap them first.
    kill_child_process_tree()
    # Flush any pending log output before the exec wipes state.
    for handler in logging.getLogger().handlers:
        try:
            handler.flush()
        except Exception:  # noqa: BLE001
            pass
    args = sys.argv[1:]  # everything after the script path — flags like --capability
    module = os.environ.get("AUTOMATIONS_WORKER_MODULE", "core.jobs.worker")
    os.execv(sys.executable, [sys.executable, "-m", module, *args])


def _make_worker_id(capabilities: list[str]) -> str:
    """Stable-ish per-process identifier. PID + a short uuid suffix so
    multiple workers on the same host don't collide if PIDs ever do."""
    return f"{','.join(capabilities)}:{os.getpid()}:{uuid.uuid4().hex[:6]}"


async def run_claimed_job(job: dict, worker_id: str) -> bool:
    """Run one dedicated worker lease, with normal heartbeat/cancellation semantics.

    Unlike the queue-draining worker, this returns to its composition root
    without replacing the process when unrelated code changes on disk.
    """
    if job.get("status") != "running" or job.get("worker_id") != worker_id:
        raise ValueError("Job must be claimed by this worker before execution")
    stop = asyncio.Event()

    async def alive():
        while not stop.is_set():
            try:
                await asyncio.to_thread(
                    worker_heartbeat, worker_id, job["required_capability"], os.getpid(),
                )
            except Exception:
                log.exception("dedicated worker heartbeat failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass

    ticker = asyncio.create_task(alive())
    try:
        return await _run_one(job, worker_id)
    finally:
        stop.set()
        try:
            await ticker
        finally:
            try:
                await asyncio.to_thread(worker_unregister, worker_id)
            except Exception:
                log.exception("dedicated worker heartbeat cleanup failed")


async def _run_one(job: dict, worker_id: str) -> bool:
    """Run a single claimed job to completion. Mirrors runner._execute
    but adds a heartbeat task that refreshes last_heartbeat_at while
    the handler runs.

    Returns True when the job ended "dirty" — user-cancelled or timed
    out. Either way the handler task was cancelled at an await point,
    but any sync node running inside ``asyncio.to_thread`` keeps its
    thread alive in the background, still holding the platform resource
    (emulator, Playwright session, Ollama socket). The caller uses this
    to decide whether to exec into a fresh process.
    """
    job_id = int(job["id"])
    kind = job["kind"]
    payload = _decode_payload(job)
    timeout_seconds = job.get("timeout_seconds")

    handler = get_handler(kind)
    if handler is None:
        mark_terminal(job_id, "failed", error=f"no handler for kind {kind!r}")
        events.emit(job_id, {
            "type": "job.finished", "status": "failed",
            "error": f"no handler for kind {kind!r}",
        })
        return False

    handle = start_handle(job_id, kind)
    register_handle(job_id, handle)

    # The handler runs as its own task so the heartbeat loop can cancel
    # it on a cross-process cancel request (the API route can't reach
    # this process's handles — it drops a row in job_cancel_requests
    # instead, and we poll it on every heartbeat tick).
    handler_task = asyncio.create_task(
        handler(job_id, payload), name=f"job-{job_id}",
    )
    user_cancelled = False

    # Heartbeat ticker — keeps last_heartbeat_at fresh so the orphan
    # sweep on FastAPI startup leaves this row alone.
    stop = asyncio.Event()

    async def _heartbeat_loop():
        nonlocal user_cancelled
        while not stop.is_set():
            try:
                await asyncio.to_thread(heartbeat, job_id)
            except Exception:
                log.exception("heartbeat write failed for job %s", job_id)
            # The poll loop's liveness ping pauses while a job runs; without this
            # the respawn sweep reads any job longer than 60 s (with work queued
            # behind it) as a dead worker and starts a duplicate.
            try:
                await asyncio.to_thread(
                    worker_heartbeat, worker_id, job["required_capability"], os.getpid(),
                )
            except Exception:
                log.exception("worker heartbeat failed during job %s", job_id)
            if not user_cancelled:
                try:
                    if await asyncio.to_thread(cancel_requested, job_id):
                        user_cancelled = True
                        log.info(
                            "cancel requested for job %s — cancelling handler",
                            job_id,
                        )
                        handle.cancel()
                        handler_task.cancel()
                except Exception:
                    log.exception("cancel check failed for job %s", job_id)
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS,
                )
            except asyncio.TimeoutError:
                pass

    hb_task = asyncio.create_task(_heartbeat_loop(), name=f"hb-{job_id}")

    final_status: str | None = None
    final_error: str | None = None
    final_result: dict | None = None
    timed_out = False
    try:
        if timeout_seconds and timeout_seconds > 0:
            result = await asyncio.wait_for(
                handler_task, timeout=timeout_seconds,
            )
        else:
            result = await handler_task
        final_status = "succeeded"
        final_result = result or {}
    except asyncio.TimeoutError:
        log.warning("job %s timed out after %ss", job_id, timeout_seconds)
        timed_out = True
        final_status = "failed"
        final_error = f"timeout after {timeout_seconds}s"
        mark_terminal(job_id, "failed", error=final_error)
    except asyncio.CancelledError:
        final_status = "cancelled"
        final_error = "cancelled" if user_cancelled else "cancelled (worker shutdown)"
        mark_terminal(job_id, "cancelled", error=final_error)
        if not user_cancelled:
            # Worker itself is being torn down — cancellation hit the
            # awaiter, not the handler task, so cancel that too, then
            # emit and propagate so run_worker's shutdown path sees the
            # CancelledError.
            handler_task.cancel()
            events.emit(job_id, {
                "type": "job.finished", "status": "cancelled",
                "error": final_error,
            })
            raise
        # User-requested cancel: fall through to the shared finish path
        # below (event emit + activity mirror); the worker stays alive.
    except Exception as exc:
        log.exception("job %s failed", job_id)
        final_status = "failed"
        final_error = f"{type(exc).__name__}: {exc}"
        mark_terminal(job_id, "failed", error=final_error)
    finally:
        stop.set()
        try:
            await hb_task
        except Exception:
            pass
        unregister_handle(job_id)
        clear_handle()

    if final_status == "succeeded":
        mark_terminal(job_id, "succeeded", result=final_result)
    elif final_status == "failed" and not user_cancelled:
        # Opt-in retry (descriptor.max_retries) — failed rows only,
        # never user cancels. Best-effort: a retry-insert error must
        # not mask the job's own terminal bookkeeping.
        try:
            retry_id = maybe_retry(job, payload)
            if retry_id is not None:
                log.info(
                    "job %s failed — spawned retry job %s (attempt %s/%s)",
                    job_id, retry_id,
                    int(job.get("retry_count") or 0) + 1,
                    get_descriptor(kind).max_retries,
                )
        except Exception:  # noqa: BLE001
            log.exception("retry spawn failed for job %s", job_id)
    events.emit(job_id, {
        "type": "job.finished",
        "status": final_status,
        "error": final_error,
        "result": final_result,
    })
    # Mirror to the activity feed — same shape as runner._execute does
    # for in-process jobs. Best-effort.
    try:
        emit_activity_for_job_finish(
            job_id=job_id,
            kind=kind,
            status=final_status,
            error=final_error,
        )
    except Exception:  # noqa: BLE001
        log.debug("activity-feed emit failed for job %s", job_id, exc_info=True)
    return user_cancelled or timed_out


def _decode_payload(job: dict) -> dict:
    """``jobs.payload`` is stored as a JSON string. claim_one returns
    the row mostly-as-is, so payload arrives as a string and needs decoding."""
    import json as _json
    raw = job.get("payload")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            return _json.loads(raw)
        except _json.JSONDecodeError:
            return {}
    return {}


async def run_worker(
    capabilities: list[str],
    *,
    poll_interval: float = IDLE_SLEEP_SECONDS,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Drain pending jobs for the given capabilities until ``stop_event``
    is set. Tested via in-process invocation; production workers use
    :func:`main` which wires SIGTERM into ``stop_event``."""
    worker_id = _make_worker_id(capabilities)
    stop_event = stop_event or asyncio.Event()
    log.info(
        "worker %s started, capabilities=%s, registered_kinds=%s",
        worker_id, capabilities, list_kinds(),
    )
    # Guard: the engine (core.jobs.worker) registers no kinds on its own —
    # the app composition root (app.worker) must import app.kinds first. If
    # the registry is empty, every claimed job fails with "no handler for
    # kind", which reads as a stuck pipeline rather than a launch mistake.
    # Surface it once, loudly, instead of spraying per-job errors. (This is
    # the silent failure mode behind the iOS-sim audit finding #2.)
    if not list_kinds():
        log.error(
            "worker %s started with ZERO registered job kinds — it will fail "
            "every job with 'no handler for kind'. Launch via 'python -m "
            "app.worker' (which imports app.kinds), not 'python -m "
            "core.jobs.worker'. AUTOMATIONS_WORKER_MODULE=%r",
            worker_id, os.environ.get("AUTOMATIONS_WORKER_MODULE"),
        )

    # Worker-level heartbeat — written from every poll tick regardless
    # of whether we're running a job. A process that's alive but wedged
    # (failing-import retry loop, claim_one exception storm) will stop
    # producing these even though ``ps`` still shows it. The sweep in
    # web/worker_routes uses this to decide when to respawn.
    pid = os.getpid()
    primary_cap = capabilities[0] if capabilities else ""

    async def _ping_worker_alive() -> None:
        try:
            await asyncio.to_thread(worker_heartbeat, worker_id, primary_cap, pid)
        except Exception:
            log.exception("worker_heartbeat write failed")

    await _ping_worker_alive()  # immediate so the row exists before first claim

    # Instant wake-ups over NATS when NATS_URLS is set; polling otherwise.
    wake_event = asyncio.Event()
    await mesh.start()
    wake.listen(capabilities, wake_event, key=f"worker:{worker_id}")
    try:
        await _drain_queue(capabilities, worker_id, stop_event, wake_event, poll_interval,
                           _ping_worker_alive)
    finally:
        wake.unlisten(capabilities, key=f"worker:{worker_id}")
        await mesh.stop()

    # Best-effort liveness cleanup. If we exit hard the row will age out
    # of the freshness window naturally; this just speeds it up.
    try:
        worker_unregister(worker_id)
    except Exception:
        log.exception("worker_unregister failed for %s", worker_id)
    log.info("worker %s stopped", worker_id)


async def _drain_queue(capabilities, worker_id, stop_event, wake_event, poll_interval,
                       _ping_worker_alive) -> None:
    """The claim loop of :func:`run_worker`."""

    # Sweep any rows orphaned by a previous worker crash. Cheap to do
    # once at startup; safe to run even if another worker is alive
    # because the cutoff window is generous.
    try:
        swept = await asyncio.to_thread(recover_orphans)
        if swept:
            log.info("recovered %d orphaned jobs on startup", swept)
    except Exception:
        log.exception("orphan recovery failed at startup; continuing")

    # Periodic sweep so "stuck but heartbeating" jobs (e.g. a worker
    # blocked on a dead Ollama socket) and the jobs of vanished remote
    # machines get released without waiting on a server restart. Cheap
    # SQL UPDATE; safe to run from every worker. It runs on its own timer,
    # not between jobs: a worker busy for hours with one long job (the
    # Riftbound training lab's generation runner) swept nothing while a
    # reclaimed spot node's shards sat as "running" (2026-10-06, 3 h).
    sweeper = asyncio.create_task(_sweep_orphans_periodically(stop_event))
    try:
        await _claim_loop(capabilities, worker_id, stop_event, wake_event, poll_interval,
                          _ping_worker_alive)
    finally:
        sweeper.cancel()


_ORPHAN_SWEEP_INTERVAL_SECONDS = 300


async def _sweep_orphans_periodically(stop_event) -> None:
    """``recover_orphans`` every ``_ORPHAN_SWEEP_INTERVAL_SECONDS`` until stopped."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=_ORPHAN_SWEEP_INTERVAL_SECONDS)
            return
        except TimeoutError:
            pass
        try:
            swept = await asyncio.to_thread(recover_orphans)
            if swept:
                log.info("periodic sweep released %d orphaned jobs", swept)
        except Exception:
            log.exception("periodic orphan sweep failed; continuing")


async def _claim_loop(capabilities, worker_id, stop_event, wake_event, poll_interval,
                      _ping_worker_alive) -> None:
    """Claim and run jobs until stopped."""
    while not stop_event.is_set():
        # Worker-level heartbeat — refresh every tick. This is the
        # signal a wedged worker can't fake.
        await _ping_worker_alive()

        # Auto-restart hook: replaces the process if the on-disk code
        # changed since startup. Runs only between jobs so an in-flight
        # job is never interrupted. Disabled outside the real worker
        # entry point (`main` sets the flag), same reasoning as
        # EXEC_AFTER_DIRTY_JOB below.
        if EXEC_ON_CODE_DRIFT:
            _maybe_exec_for_new_code()

        # Yield ollama to the voice pipeline. While a voice session is
        # active, ollama-capable workers stop claiming new jobs so the
        # model isn't contending with voice generation. In-flight jobs
        # are unaffected; only the *next* claim is paused. Non-ollama
        # workers (db, applescript, etc.) keep draining their queues
        # since they don't touch the model.
        ollama_paused = False
        if "ollama" in capabilities:
            try:
                from core.voice import busy as _voice_busy
                if _voice_busy.is_busy():
                    ollama_paused = True
            except Exception:
                # core.voice not available in this worker venv — fine.
                pass

        # Clear before claiming: a hint that lands after this point (even
        # during the claim) wakes the idle wait below, so none is lost.
        wake_event.clear()
        if ollama_paused:
            job = None
        else:
            try:
                job = await asyncio.to_thread(claim_one, capabilities, worker_id)
            except Exception:
                log.exception("claim_one failed; backing off")
                job = None

        if job is None:
            await wake.idle(stop_event, wake_event, poll_interval)
            continue

        try:
            dirty = await _run_one(job, worker_id)
        except asyncio.CancelledError:
            # Worker is shutting down mid-job — stop_event will be set.
            break
        except Exception:
            log.exception("unhandled error running job %s", job.get("id"))
            dirty = False

        # A user-cancelled or timed-out job may have left a zombie
        # thread behind (a sync node in asyncio.to_thread can't be
        # cancelled — it keeps driving the emulator / Playwright /
        # socket and poisons the next job on this worker). exec into a
        # fresh process so the slate is actually clean. Disabled outside
        # the real worker entry point (`main` sets the flag) so tests
        # that drive run_worker in-process don't exec away the test
        # runner.
        if dirty and EXEC_AFTER_DIRTY_JOB:
            log.warning(
                "job %s ended dirty (cancelled/timeout) — restarting "
                "worker process to clear any zombie handler threads",
                job.get("id"),
            )
            _exec_self(reason=f"dirty job {job.get('id')} cleanup restart")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a capability-scoped job worker.",
    )
    parser.add_argument(
        "--capability",
        required=True,
        help="Comma-separated capabilities to drain "
             "(e.g. 'ollama' or 'db,applescript').",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=IDLE_SLEEP_SECONDS,
        help=f"Seconds between idle polls (default {IDLE_SLEEP_SECONDS}).",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Python log level (default INFO).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    capabilities = [c.strip() for c in args.capability.split(",") if c.strip()]
    if not capabilities:
        print("--capability cannot be empty", file=sys.stderr)
        return 2

    # Mirror this worker's logging into tasks.db (see core.logsink).
    from core import logsink
    logsink.install(f"worker-{'+'.join(capabilities)}")

    global EXEC_AFTER_DIRTY_JOB, EXEC_ON_CODE_DRIFT
    EXEC_AFTER_DIRTY_JOB = True
    EXEC_ON_CODE_DRIFT = True

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()

    def _signal_stop(signum, _frame):
        log.info("received signal %s, stopping after current job", signum)
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGTERM, _signal_stop)
    signal.signal(signal.SIGINT, _signal_stop)

    started = time.time()
    try:
        loop.run_until_complete(
            run_worker(
                capabilities,
                poll_interval=args.poll_interval,
                stop_event=stop_event,
            )
        )
    finally:
        loop.close()
        # SIGTERM shutdown reaps children too — a job in flight when the
        # signal landed can leave a Playwright node/chromium behind.
        kill_child_process_tree()
    log.info("uptime: %.1fs", time.time() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
