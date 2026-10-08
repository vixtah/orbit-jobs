"""Narrow durable-history reads for job-producing orchestration."""

from __future__ import annotations

from typing import Any

from core.db import get_db


def has_succeeded_for_subject(
    subject_type: str, subject_id: str, kind: str | None = None
) -> bool:
    """Whether a job for this exact subject has ever succeeded.

    This is the durable idempotency read for producers whose successful
    terminal outcomes suppress unchanged work across process restarts.
    Failed and cancelled attempts deliberately do not count.
    """
    sql = (
        "SELECT 1 FROM jobs WHERE subject_type = ? AND subject_id = ?"
        " AND status = 'succeeded'"
    )
    params: list[Any] = [subject_type, subject_id]
    if kind is not None:
        sql += " AND kind = ?"
        params.append(kind)
    sql += " LIMIT 1"
    with get_db() as conn:
        return conn.execute(sql, params).fetchone() is not None
