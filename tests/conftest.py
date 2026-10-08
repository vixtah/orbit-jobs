"""Standalone fixtures: every test runs orbit_jobs with its default hooks against a
fresh SQLite file of its own. Needs nothing from a host, and restores whatever hooks
were configured before (the root suite runs these next to the host's tests)."""

from __future__ import annotations

import pytest
from orbit_jobs import config


@pytest.fixture(autouse=True)
def orbit_jobs_db(tmp_path, monkeypatch):
    saved = config.snapshot()
    config.restore(config.Settings())
    db_file = tmp_path / "jobs.db"
    monkeypatch.setenv("ORBIT_JOBS_DB", str(db_file))
    # Remote-worker file roots, so no test writes under the home directory.
    monkeypatch.setenv("REMOTE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("REMOTE_INPUT_CACHE", str(tmp_path / "input-cache"))
    monkeypatch.setenv("REMOTE_WORK_DIR", str(tmp_path / "remote-work"))
    yield db_file
    config.restore(saved)
