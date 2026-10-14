"""orbit_jobs.schedules: due/period logic, claim-before-run, backoff, job-backed
entries, isolation, health and pruning. Fake clock throughout: every tick passes
``now`` explicitly and the configured ``now_iso`` hook follows it."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
from orbit_jobs import KindDescriptor, config, register_kind, schedules, store
from orbit_jobs.db import get_db
from orbit_jobs.schedules import (
    RETRY,
    SKIP,
    SUNDAY,
    TICK,
    Backoff,
    Daily,
    Entry,
    Every,
    Job,
    Service,
    health,
    import_periods,
    prune,
    record_run,
    run_due,
)

T0 = datetime(2026, 10, 13, 10, 0, 30, tzinfo=timezone.utc)  # a Tuesday


class Clock:
    def __init__(self, now: datetime = T0):
        self.now = now

    def advance(self, **delta) -> datetime:
        self.now += timedelta(**delta)
        return self.now


@pytest.fixture
def clock():
    c = Clock()
    config.configure(now_iso=lambda: c.now.isoformat())
    schedules._last_prune = None
    yield c
    schedules._last_prune = None


def tick(entries, now):
    return asyncio.run(run_due(entries, now))


def runs(name):
    with get_db() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM schedule_runs WHERE name = ? ORDER BY id", (name,))]


def state(name):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM schedule_state WHERE name = ?", (name,)).fetchone()
    return dict(row) if row else None


class Counter:
    def __init__(self, fail: bool = False, result=None):
        self.calls, self.fail, self.result = 0, fail, result

    def __call__(self, now):
        self.calls += 1
        if self.fail:
            raise RuntimeError("upstream 504 " + "x" * 400)
        return self.result


# ── period logic ──────────────────────────────────────────────────────


def test_every_runs_once_per_clock_bucket(clock):
    action = Counter()
    entries = [Entry("five", Every(5), action)]
    assert tick(entries, T0) == {"five": "ok"}
    assert tick(entries, T0 + timedelta(minutes=4)) == {}  # same 10:00-10:05 bucket
    assert tick(entries, T0 + timedelta(minutes=5)) == {"five": "ok"}
    assert action.calls == 2
    assert [r["status"] for r in runs("five")] == ["ok", "ok"]


def test_every_period_matches_the_tasks_registry_format():
    assert Every(60).period(T0) == str(int(T0.timestamp()) // 3600)


def test_daily_waits_for_its_time_then_runs_once_a_day(clock):
    action = Counter()
    entries = [Entry("brief", Daily((9, 30), utc=True), action)]
    early = T0.replace(hour=9, minute=29)
    assert tick(entries, early) == {}
    assert tick(entries, early + timedelta(minutes=1)) == {"brief": "ok"}
    assert tick(entries, early + timedelta(hours=5)) == {}
    assert tick(entries, early + timedelta(days=1)) == {}  # 09:29 next day
    assert tick(entries, early + timedelta(days=1, minutes=1)) == {"brief": "ok"}
    assert action.calls == 2


def test_daily_weekday_and_callable_time():
    weekly = Daily(lambda: (18, 0), weekday=SUNDAY, utc=True)
    sunday = datetime(2026, 10, 18, 18, 0, tzinfo=timezone.utc)
    assert weekly.period(sunday) == "2026-10-18"
    assert weekly.period(sunday - timedelta(minutes=1)) is None
    assert weekly.period(sunday + timedelta(days=1)) is None
    assert weekly.interval_minutes == 7 * 1440 and Daily().interval_minutes == 1440


def test_daily_uses_local_time_unless_utc():
    now = datetime(2026, 10, 13, 23, 30, tzinfo=timezone.utc)
    local = now.astimezone()
    assert Daily((0, 0)).period(now) == local.strftime("%Y-%m-%d")
    assert Daily((0, 0), utc=True).period(now) == "2026-10-13"


def test_tick_runs_every_call(clock):
    action = Counter()
    entries = [Entry("t", TICK, action)]
    for i in range(3):
        tick(entries, T0 + timedelta(seconds=i))
    assert action.calls == 3 and len(runs("t")) == 3


def test_enabled_false_claims_nothing(clock):
    on = {"v": False}
    action = Counter()
    entries = [Entry("gated", Every(60), action, enabled=lambda: on["v"])]
    assert tick(entries, T0) == {}
    assert state("gated") is None and runs("gated") == []
    on["v"] = True
    assert tick(entries, T0 + timedelta(minutes=1)) == {"gated": "ok"}


def test_retry_releases_the_claim_and_skip_records(clock):
    retry, skip = Counter(result=RETRY), Counter(result=SKIP)
    entries = [Entry("r", Every(60), retry), Entry("s", Every(60), skip)]
    assert tick(entries, T0) == {"s": "skipped"}
    assert tick(entries, T0 + timedelta(minutes=1)) == {}
    assert retry.calls == 2 and skip.calls == 1  # RETRY: due again; SKIP: period kept
    assert runs("r") == [] and [r["status"] for r in runs("s")] == ["skipped"]


# ── claim before run ─────────────────────────────────────────────────


def test_period_is_claimed_and_recorded_before_the_action_runs(clock):
    seen = {}

    def look(now):
        seen["state"] = state("c")
        seen["runs"] = runs("c")

    tick([Entry("c", Every(60), look)], T0)
    assert seen["state"]["period"] == Every(60).period(T0)
    assert [r["status"] for r in seen["runs"]] == ["running"]


def test_a_run_cut_off_mid_way_is_not_repeated_at_once(clock):
    async def dies(now):
        raise asyncio.CancelledError  # the process going away mid-run

    with pytest.raises(asyncio.CancelledError):
        tick([Entry("crash", Every(60), dies)], T0)
    action = Counter()
    entries = [Entry("crash", Every(60), action)]
    assert tick(entries, T0 + timedelta(minutes=10)) == {}  # same hour: already claimed
    assert tick(entries, T0 + timedelta(hours=1)) == {"crash": "ok"}  # picked up later
    first, second = runs("crash")
    assert first["status"] == "skipped" and "interrupted" in first["error"]
    assert second["status"] == "ok"


def test_import_periods_keeps_an_already_run_period(clock):
    assert import_periods({"daily": "2026-10-13", "none": ""}) == 1
    assert import_periods({"daily": "2026-10-12"}) == 0  # existing state wins
    action = Counter()
    entries = [Entry("daily", Daily(utc=True), action)]
    assert tick(entries, T0) == {}
    assert tick(entries, T0 + timedelta(days=1)) == {"daily": "ok"}


def test_import_periods_seeds_failures_and_a_pending_retry(clock):
    retry = T0 + timedelta(hours=2)
    assert import_periods({}, failures={"news": 3}, retry_at={"news": retry}) == 1
    assert state("news")["failures"] == 3
    assert state("news")["retry_at"] == schedules._iso(retry)
    action = Counter(fail=True)
    entries = [Entry("news", Every(360), action)]
    assert tick(entries, T0 + timedelta(minutes=119)) == {}       # the retry is kept
    assert tick(entries, retry + timedelta(seconds=1)) == {"news": "failed"}
    assert state("news")["failures"] == 4                           # grows from 3: 8 h
    retry_at = schedules._parse(state("news")["retry_at"])
    finished = schedules._parse(runs("news")[-1]["finished_at"])
    assert retry_at - finished == timedelta(hours=8)


def test_import_periods_retry_forms_and_existing_state_wins(clock):
    naive = (T0 + timedelta(hours=1)).replace(tzinfo=None)
    pacific = (T0 + timedelta(hours=1)).astimezone(timezone(timedelta(hours=-7)))
    assert import_periods({"a": "2026-10-13"}, failures={"b": 1},
                          retry_at={"c": naive.isoformat(), "d": pacific, "e": None}) == 4
    assert state("c")["retry_at"] == state("d")["retry_at"] == schedules._iso(
        T0 + timedelta(hours=1))
    assert state("a")["failures"] == 0 and state("a")["retry_at"] is None
    assert state("e") is None
    assert import_periods({"a": "2026-10-12"}, failures={"a": 5, "b": 9},
                          retry_at={"c": T0}) == 0
    assert (state("a")["period"], state("a")["failures"]) == ("2026-10-13", 0)
    assert state("b")["failures"] == 1
    with pytest.raises(ValueError):
        import_periods({}, retry_at={"f": "not a time"})


# ── backoff ──────────────────────────────────────────────────────────


def test_backoff_delay_grows_and_caps_regardless_of_the_interval():
    b = Backoff()
    schedule = [60, 120, 240, 480, 960, 1440, 1440]
    for interval in (5, 6 * 60, 1440, 30 * 1440, None):
        assert [b.delay_minutes(n, interval) for n in range(1, 8)] == schedule
    assert b.delay_minutes(10_000, None) == 1440
    assert Backoff(first_minutes=1, max_minutes=30).delay_minutes(1, 5) == 1
    assert schedules.NO_BACKOFF.delay_minutes(3, 60) == 0


def test_backoff_at_least_interval_keeps_the_old_floor():
    b = Backoff(at_least_interval=True)
    assert [b.delay_minutes(n, 6 * 60) for n in range(1, 6)] == [360, 360, 360, 480, 960]
    assert b.delay_minutes(1, 30 * 1440) == 1440        # capped: a monthly retries daily
    assert Backoff(1, 30, at_least_interval=True).delay_minutes(1, 5) == 5


@pytest.mark.parametrize(("when", "first_retry"), [
    (Every(360), timedelta(hours=1)),        # a 6-hourly entry retries after 1 h
    (Daily(utc=True), timedelta(hours=1)),   # a daily one the same day
    (Every(30 * 1440), timedelta(hours=1)),  # a monthly one well within a day
])
def test_a_failed_long_interval_entry_retries_on_the_backoff_schedule(clock, when, first_retry):
    action = Counter(fail=True)
    entries = [Entry("slow", when, action)]
    assert tick(entries, T0) == {"slow": "failed"}
    assert tick(entries, T0 + first_retry - timedelta(minutes=1)) == {}
    assert tick(entries, T0 + first_retry + timedelta(minutes=1)) == {"slow": "failed"}
    retry_at = schedules._parse(state("slow")["retry_at"])
    finished = schedules._parse(runs("slow")[-1]["finished_at"])
    assert retry_at - finished == timedelta(hours=2)
    action.fail = False
    assert tick(entries, retry_at + timedelta(seconds=1)) == {"slow": "ok"}
    assert tick(entries, retry_at + timedelta(minutes=5)) == {}  # back to its cadence
    assert action.calls == 3


def test_failures_back_off_then_a_success_resets(clock):
    action = Counter(fail=True)
    entries = [Entry("flaky", Every(5), action)]
    now = T0
    assert tick(entries, now) == {"flaky": "failed"}
    assert state("flaky")["failures"] == 1
    assert tick(entries, now + timedelta(minutes=59)) == {}          # backing off
    now += timedelta(minutes=61)
    assert tick(entries, now) == {"flaky": "failed"}                 # 1 h later
    assert tick(entries, now + timedelta(minutes=119)) == {}
    now += timedelta(minutes=121)
    assert tick(entries, now) == {"flaky": "failed"}                 # 2 h later
    assert state("flaky")["failures"] == 3
    assert action.calls == 3
    error = runs("flaky")[0]["error"]
    assert error.startswith("RuntimeError: upstream 504") and len(error) == 300

    action.fail = False
    now += timedelta(minutes=241)
    assert tick(entries, now) == {"flaky": "ok"}
    assert state("flaky")["failures"] == 0 and state("flaky")["retry_at"] is None
    assert tick(entries, now + timedelta(minutes=5)) == {"flaky": "ok"}  # normal cadence


def test_backoff_caps_at_a_day(clock):
    action = Counter(fail=True)
    entries = [Entry("down", Every(5), action)]
    now = T0
    for _ in range(8):
        tick(entries, now)
        now = schedules._parse(state("down")["retry_at"]) + timedelta(seconds=1)
    retry_at = schedules._parse(state("down")["retry_at"])
    finished = schedules._parse(runs("down")[-1]["finished_at"])
    assert state("down")["failures"] == 8
    assert retry_at - finished == timedelta(hours=24)


def test_backoff_survives_a_restart(clock, orbit_jobs_db):
    import sqlite3

    tick([Entry("p", Every(5), Counter(fail=True))], T0)
    fresh = sqlite3.connect(orbit_jobs_db)  # what a new process would read
    failures, retry_at = fresh.execute(
        "SELECT failures, retry_at FROM schedule_state WHERE name = 'p'").fetchone()
    fresh.close()
    assert failures == 1 and retry_at == schedules._iso(T0 + timedelta(hours=1))
    action = Counter()  # the module keeps no in-memory schedule state
    assert tick([Entry("p", Every(5), action)], T0 + timedelta(minutes=30)) == {}
    assert action.calls == 0


def test_tick_entries_do_not_back_off_by_default(clock):
    action = Counter(fail=True)
    entries = [Entry("gate", TICK, action)]
    tick(entries, T0)
    tick(entries, T0 + timedelta(minutes=1))
    assert action.calls == 2


# ── isolation ────────────────────────────────────────────────────────


def test_one_failing_entry_does_not_stop_the_others(clock):
    before, after = Counter(), Counter()
    entries = [Entry("a", Every(5), before), Entry("bad", Every(5), Counter(fail=True)),
               Entry("b", TICK, after)]
    assert tick(entries, T0) == {"a": "ok", "bad": "failed", "b": "ok"}
    assert before.calls == after.calls == 1


def test_async_actions_run(clock):
    async def work(now):
        await asyncio.sleep(0)

    assert tick([Entry("async", Every(5), work)], T0) == {"async": "ok"}


# ── job-backed entries ───────────────────────────────────────────────


async def _noop(job_id, payload):
    return {"ok": True}


def _routed_kind():
    register_kind(KindDescriptor(kind="sched_routed", label="R", description="r",
                                 handler=_noop, capability="sched-test"))


def test_job_entry_takes_the_jobs_outcome_and_never_double_starts(clock):
    _routed_kind()
    entries = [Entry("refresh", Every(5), Job("sched_routed", lambda now: {"n": 1}))]
    assert tick(entries, T0) == {"refresh": "running"}
    [run] = runs("refresh")
    job = store.get(run["job_id"])
    assert job["status"] == "pending" and job["origin"] == "scheduled"
    assert (job["subject_type"], job["subject_id"]) == ("schedule", "refresh")
    assert job["payload"] == {"n": 1}

    # Still pending at the next period: not started again, still running.
    assert tick(entries, T0 + timedelta(minutes=5)) == {}
    assert len(runs("refresh")) == 1
    assert health(entries, T0)[0]["last_status"] == "running"

    clock.advance(minutes=7)
    store.mark_terminal(run["job_id"], "failed", error="boom")
    now = clock.advance(minutes=1)
    assert tick(entries, now) == {"refresh": "failed"}  # settled; backing off, no new start
    [run] = runs("refresh")
    assert (run["status"], run["error"], run["duration_ms"]) == ("failed", "boom", 7 * 60_000)
    assert state("refresh")["failures"] == 1

    now = clock.advance(minutes=61)
    assert tick(entries, now) == {"refresh": "running"}
    second = runs("refresh")[-1]
    store.mark_terminal(second["job_id"], "succeeded", result={"ok": True})
    now = clock.advance(minutes=5)
    assert tick(entries, now) == {"refresh": "running"}  # settled ok, then next period starts
    assert [r["status"] for r in runs("refresh")] == ["failed", "ok", "running"]
    assert state("refresh")["failures"] == 0


def test_job_entry_waits_for_a_manual_run_of_the_same_subject(clock):
    _routed_kind()
    from orbit_jobs import spawn

    async def manual():
        return spawn("sched_routed", {}, subject_type="pipeline", subject_id="news")

    asyncio.run(manual())
    entries = [Entry("news", Every(60), Job("sched_routed", subject_type="pipeline"))]
    assert tick(entries, T0) == {}
    assert runs("news") == [] and state("news") is None


def test_a_cancelled_job_is_skipped_without_backoff(clock):
    _routed_kind()
    entries = [Entry("c", Every(5), Job("sched_routed"))]
    tick(entries, T0)
    store.mark_terminal(runs("c")[0]["job_id"], "cancelled", error="interrupted by server restart")
    assert tick(entries, T0 + timedelta(minutes=5)) == {"c": "running"}
    assert runs("c")[0]["status"] == "skipped"
    assert state("c")["failures"] == 0


def test_inline_job_kind_runs_on_the_loop(clock):
    register_kind(KindDescriptor(kind="sched_inline", label="I", description="i",
                                 handler=_noop))
    entries = [Entry("inline", Every(5), Job("sched_inline"))]

    async def two_ticks():
        first = await run_due(entries, T0)
        for _ in range(100):
            if store.get(runs("inline")[0]["job_id"])["status"] == "succeeded":
                break
            await asyncio.sleep(0.01)
        return first, await run_due(entries, T0 + timedelta(minutes=1))

    assert asyncio.run(two_ticks()) == ({"inline": "running"}, {"inline": "ok"})


def test_spawn_failure_is_a_failed_run(clock):
    entries = [Entry("ghost", Every(5), Job("no_such_kind"))]
    assert tick(entries, T0) == {"ghost": "failed"}
    assert "Unknown job kind" in runs("ghost")[0]["error"]


# ── health + pruning ─────────────────────────────────────────────────


def test_health_shape_and_24h_counts(clock):
    flaky = Counter()
    entries = [
        Entry("planner", Every(5), flaky, title="Venue refresh planner", kind="loop",
              where="VPS", backoff=schedules.NO_BACKOFF),
        Entry("never", Daily((9, 0)), Counter(), enabled=lambda: False),
        Entry("sweep", TICK, Counter()),
    ]
    old = T0 - timedelta(hours=30)
    tick(entries[:1], old)  # outside the 24 h window
    for i in range(4):
        flaky.fail = i == 2
        tick(entries, T0 + timedelta(minutes=5 * i))
    flaky.fail = False
    now = T0 + timedelta(minutes=20)
    planner, never, sweep = health(entries, now)

    assert planner == {
        "id": "planner", "name": "Venue refresh planner", "expected_every_minutes": 5,
        "last_run_at": schedules._iso(T0 + timedelta(minutes=15)),
        "last_ok_at": schedules._iso(T0 + timedelta(minutes=15)),
        "last_status": "ok", "last_error": planner["last_error"],
        "last_duration_ms": planner["last_duration_ms"], "runs_24h": 4, "failures_24h": 1,
        "kind": "loop", "where": "VPS",
    }
    assert isinstance(planner["last_duration_ms"], int)
    assert planner["last_error"].startswith("RuntimeError: upstream 504")
    assert never == {
        "id": "never", "name": "never", "expected_every_minutes": 1440, "last_run_at": None,
        "last_ok_at": None, "last_status": "unknown", "last_error": None,
        "last_duration_ms": None, "runs_24h": 0, "failures_24h": 0,
    }
    assert sweep["expected_every_minutes"] == 1 and sweep["runs_24h"] == 4
    assert health(entries[2:], now, tick_minutes=0.5)[0]["expected_every_minutes"] == 0.5


def test_on_demand_and_overridden_cadence_in_health(clock):
    items = [
        Entry("queue", TICK, Counter(), expected_every_minutes=None),
        Entry("hourly-ish", Every(5), Counter(), expected_every_minutes=60),
        Entry("default", Every(5), Counter()),
        Service("art-index", expected_every_minutes=None, kind="refresh"),
    ]
    with record_run(items[3]):
        pass
    queue, hourly, default, art = health(items, T0)
    assert queue["expected_every_minutes"] is None
    assert hourly["expected_every_minutes"] == 60
    assert default["expected_every_minutes"] == 5
    assert art["expected_every_minutes"] is None and art["last_status"] == "ok"
    assert art["kind"] == "refresh"


# ── hand-written loops (Service + record_run) ─────────────────────────


def test_record_run_records_ok_and_failed_runs(clock):
    sweeper = Service("fleet-sweeper", 2, title="Fleet sweeper", kind="loop", where="VPS")
    with record_run(sweeper):
        assert runs("fleet-sweeper")[-1]["status"] == "running"
        clock.advance(seconds=3)
    clock.advance(minutes=2)
    with pytest.raises(RuntimeError), record_run("fleet-sweeper"):
        raise RuntimeError("droplet API 503 " + "x" * 400)

    ok, failed = runs("fleet-sweeper")
    assert ok["status"] == "ok" and ok["error"] is None and ok["period"] is None
    assert ok["started_at"] == schedules._iso(T0)
    assert ok["finished_at"] == schedules._iso(T0 + timedelta(seconds=3))
    assert failed["status"] == "failed" and len(failed["error"]) == 300
    assert failed["error"].startswith("RuntimeError: droplet API 503")
    assert state("fleet-sweeper") is None  # no period, no backoff


def test_record_run_async_and_cancellation(clock):
    async def main():
        async with record_run("lab"):
            await asyncio.sleep(0)
        with pytest.raises(asyncio.CancelledError):
            async with record_run("lab"):
                raise asyncio.CancelledError()
        with pytest.raises(ValueError):
            async with record_run("lab"):
                raise ValueError("bad shard")

    asyncio.run(main())
    assert [(r["status"], r["error"]) for r in runs("lab")] == [
        ("ok", None), ("skipped", "CancelledError"), ("failed", "ValueError: bad shard")]


def test_record_run_closes_a_run_cut_off_by_a_stopped_process(clock):
    record_run("sweeper").__enter__()  # the process died inside the block
    clock.advance(minutes=1)
    with record_run("sweeper"):
        pass
    assert [(r["status"], r["error"]) for r in runs("sweeper")] == [
        ("skipped", "interrupted (the process stopped mid-run)"), ("ok", None)]


def _broken_db(monkeypatch, *, on_open: bool, on_close: bool):
    real_open, real_close = schedules._open_run, schedules._close_run

    def fail(*_a, **_k):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(schedules, "_open_run", fail if on_open else real_open)
    monkeypatch.setattr(schedules, "_close_run", fail if on_close else real_close)


@pytest.mark.parametrize("on_open,on_close", [(True, False), (False, True), (True, True)])
def test_record_run_database_failure_never_breaks_the_work(clock, monkeypatch, caplog,
                                                             on_open, on_close):
    _broken_db(monkeypatch, on_open=on_open, on_close=on_close)
    done = []
    with caplog.at_level("WARNING", logger="orbit_jobs.schedules"):
        with record_run("sweeper"):
            done.append("sync")
        with pytest.raises(ValueError, match="bad shard") as raised:
            with record_run("sweeper"):
                raise ValueError("bad shard")
        assert type(raised.value) is ValueError

        async def main():
            async with record_run("sweeper"):
                done.append("async")
            with pytest.raises(KeyError):
                async with record_run("sweeper"):
                    raise KeyError("k")
            with pytest.raises(asyncio.CancelledError):
                async with record_run("sweeper"):
                    raise asyncio.CancelledError()

        asyncio.run(main())
    assert done == ["sync", "async"]
    warned = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warned) == 5
    assert all("sweeper" in r.getMessage() for r in warned)
    if on_open:
        assert runs("sweeper") == []


def test_health_mixes_entries_and_services(clock):
    entries = [Entry("planner", Every(5), Counter())]
    sweeper = Service("fleet-sweeper", 2, title="Fleet sweeper", kind="loop", where="VPS")
    tick(entries, T0)
    with pytest.raises(RuntimeError), record_run(sweeper):
        raise RuntimeError("boom")
    planner, svc, idle = health([*entries, sweeper, Service("idle", 60)], T0)

    assert planner["id"] == "planner" and planner["last_status"] == "ok"
    assert svc == {
        "id": "fleet-sweeper", "name": "Fleet sweeper", "expected_every_minutes": 2,
        "last_run_at": schedules._iso(T0), "last_ok_at": None, "last_status": "failed",
        "last_error": "RuntimeError: boom", "last_duration_ms": svc["last_duration_ms"],
        "runs_24h": 1, "failures_24h": 1, "kind": "loop", "where": "VPS",
    }
    assert idle["last_status"] == "unknown" and idle["expected_every_minutes"] == 60
    assert "kind" not in idle


def test_record_run_prunes_old_records(clock):
    with record_run("x"):
        pass
    clock.advance(days=8)
    with record_run("x"):
        pass
    assert [r["started_at"][:10] for r in runs("x")] == ["2026-10-21"]


def test_old_runs_are_pruned_automatically(clock):
    entries = [Entry("x", Every(60), Counter())]
    tick(entries, T0 - timedelta(days=8))
    tick(entries, T0 - timedelta(days=2))
    assert len(runs("x")) == 2
    tick(entries, T0)  # more than an hour since the last prune
    assert [r["started_at"][:10] for r in runs("x")] == ["2026-10-11", "2026-10-13"]
    assert prune(T0 + timedelta(days=6)) == 1
    assert len(runs("x")) == 1


def test_works_with_only_the_standard_library(tmp_path):
    """An app that uses only this module (no workers, no extras, no site-packages)."""
    import os
    import subprocess
    import sys
    from pathlib import Path

    script = (
        "import asyncio\n"
        "from orbit_jobs.schedules import Entry, Every, health, run_due\n"
        "entries = [Entry('ping', Every(5), lambda now: None)]\n"
        "print(asyncio.run(run_due(entries)), health(entries)[0]['last_status'])\n"
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PYTHON", "VIRTUAL_ENV"))}
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    env["ORBIT_JOBS_DB"] = str(tmp_path / "jobs.db")
    out = subprocess.run([sys.executable, "-S", "-c", script], cwd=tmp_path, env=env,
                         capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "{'ping': 'ok'} ok"
