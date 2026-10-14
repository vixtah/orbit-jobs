"""Keep secrets out of recorded error text (standard library only).

:func:`redact` is what :mod:`orbit_jobs.schedules` applies to every run error before
it is stored or reported (the ``redact_error`` hook, :mod:`orbit_jobs.config`). It
follows the rules of ``app_server_basics.auth.redact`` (token query parameters and
token headers), widened for free-form error text:

- URLs lose their query string and fragment (``https://host/path?key=...`` becomes
  ``https://host/path``) and any ``user:password@``;
- ``Bearer`` / ``Basic`` credentials;
- the value after a secret-looking name (``token``, ``x-app-token``, ``api_key``,
  ``client_secret``, ``password``, ``authorization``, ``cookie``, ``session``...),
  written ``name=value``, ``name: value`` or ``"name": "value"``;
- long opaque strings (32 or more letters, digits, ``-`` or ``_`` with both letters
  and digits), which is what keys and tokens look like.

Each becomes ``[redacted]``. Redacting twice changes nothing.
"""

from __future__ import annotations

import re

__all__ = ["REDACTED", "redact"]

REDACTED = "[redacted]"
_DONE = re.escape(REDACTED)

# scheme://[user[:password]@]host/path[?query][#fragment]
_URL = re.compile(r"(?i)\b((?:https?|wss?|ftp)://)(?:[^\s/@?#]*@)?([^\s?#\"'<>]*)[?#][^\s\"'<>]*")
_USERINFO = re.compile(r"(?i)\b((?:https?|wss?|ftp)://)[^\s/@?#]+@")
_SCHEME_AUTH = re.compile(rf"(?i)\b(bearer|basic)\s+(?!{_DONE})[A-Za-z0-9._~+/=-]+")
_NAMES = (
    r"(?:[\w-]*[_-])?(?:token|apikey|api[_-]?key|key|secret|password|passwd|pwd|"
    r"signature|sig|auth|authorization|credentials?|cookie|session|sessionid)"
)
_NAMED = re.compile(
    rf"(?i)((?<![\w-]){_NAMES}[\"']?\s*[:=]\s*[\"']?)"
    rf"(?!{_DONE}|bearer\b|basic\b)[^\s\"'&,;)}}\]]+"
)
_OPAQUE = re.compile(r"(?<![\w-])(?=[\w-]*\d)(?=[\w-]*[A-Za-z])[\w-]{32,}(?![\w-])")


def redact(text: str) -> str:
    """``text`` with URL query strings, credentials and token-like values replaced by
    ``[redacted]`` (see the module docstring)."""
    if not text:
        return text
    text = _URL.sub(r"\1\2", text)
    text = _USERINFO.sub(rf"\1{REDACTED}@", text)
    text = _SCHEME_AUTH.sub(rf"\1 {REDACTED}", text)
    text = _NAMED.sub(rf"\1{REDACTED}", text)
    return _OPAQUE.sub(REDACTED, text)
