"""Source snapshots of this checkout's commits, for remote workers without git.

A fleet node (docs/FLEET.md) boots from an image with a baseline checkout and
fetches the exact commit its lease is pinned to as a gzipped ``git archive``.
Only commits that exist in this repository are served.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_COMMIT = re.compile(r"[0-9a-fA-F]{7,40}")
_REPO = Path(__file__).resolve().parents[2]


def resolve(commit: str) -> str:
    """The full 40-character id of ``commit`` in this repository.

    Raises ValueError for a malformed id and LookupError when the repository has
    no such commit (or a short id is ambiguous)."""
    if not isinstance(commit, str) or not _COMMIT.fullmatch(commit):
        raise ValueError("commit must be a 7-40 character hex id")
    out = subprocess.run(["git", "rev-parse", "--verify", "--quiet",
                          f"{commit.lower()}^{{commit}}"],
                         cwd=_REPO, capture_output=True, text=True, timeout=30)
    if out.returncode != 0:
        raise LookupError(f"no commit {commit} in this repository")
    return out.stdout.strip()


def archive(commit: str) -> bytes:
    """``git archive --format=tar.gz`` of ``commit``.

    Raises ValueError when ``commit`` isn't a 7–40 character hex id and
    LookupError when this repository has no such commit (or the short id is
    ambiguous).
    """
    if not isinstance(commit, str) or not _COMMIT.fullmatch(commit):
        raise ValueError("commit must be a 7-40 character hex id")
    commit = commit.lower()
    exists = subprocess.run(["git", "cat-file", "-e", f"{commit}^{{commit}}"],
                            cwd=_REPO, capture_output=True, timeout=30)
    if exists.returncode != 0:
        raise LookupError(f"no commit {commit} in this repository")
    out = subprocess.run(["git", "archive", "--format=tar.gz", commit],
                         cwd=_REPO, capture_output=True, timeout=300)
    if out.returncode != 0:
        raise RuntimeError(f"git archive {commit} failed: "
                           f"{out.stderr.decode(errors='replace').strip()}")
    return out.stdout
