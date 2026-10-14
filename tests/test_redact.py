"""orbit_jobs.redact: what reaches run records of an error's text."""

import pytest
from orbit_jobs.redact import REDACTED, redact


@pytest.mark.parametrize("text, expected", [
    ("GET https://maps.googleapis.com/maps/api/place?key=AIzaXYZ&q=golf failed",
     "GET https://maps.googleapis.com/maps/api/place failed"),
    ("https://h.test/a#access_token=abc", "https://h.test/a"),
    ("wss://u:p4ss@h.test/socket", f"wss://{REDACTED}@h.test/socket"),
    ("https://u:p4ss@h.test/x?y=1", "https://h.test/x"),
    ("Authorization: Bearer eyJhbGciOi.abc.def", f"Authorization: Bearer {REDACTED}"),
    ("x-app-token: s3cret here", f"x-app-token: {REDACTED} here"),
    ("{'X-Rift-Token': 'abc123'}", f"{{'X-Rift-Token': '{REDACTED}'}}"),
    ('{"api_key": "k-1", "n": 2}', f'{{"api_key": "{REDACTED}", "n": 2}}'),
    ("password=hunter2&user=me", f"password={REDACTED}&user=me"),
    ("client_secret=abc, other", f"client_secret={REDACTED}, other"),
    ("leaked q8Zr2Lm0Xv7Tn4Pk9Wc1Hd5Gf3Js6Ab0Yt here", f"leaked {REDACTED} here"),
])
def test_redacts(text, expected):
    assert redact(text) == expected
    assert redact(redact(text)) == expected  # idempotent


@pytest.mark.parametrize("text", [
    "RuntimeError: upstream 504",
    "KeyError: 'reserve-warm'",
    "monkey=banana",
    "timeout after 30s talking to https://h.test/path/to/thing",
    "3 of 40 venues failed",
    "",
])
def test_leaves_ordinary_text_alone(text):
    assert redact(text) == text
