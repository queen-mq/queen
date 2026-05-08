"""
Shared helpers for window operators. Mirror of _windowCommon.js.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timezone
from typing import Any, Callable, Optional


MS_PER_SEC = 1000


async def extract_event_time(msg: Any, event_time_fn: Optional[Callable[[Any], Any]]) -> Optional[int]:
    """
    Extract the timestamp (epoch ms) for an envelope's message.

    Processing-time mode (no extractor): uses ``msg["createdAt"]``
    (Queen-stamped, ISO string).
    Event-time mode: calls the extractor; the return must be a
    datetime, a number (ms epoch), or an ISO string.

    Returns None when the timestamp can't be determined.
    """
    if event_time_fn is not None and callable(event_time_fn):
        try:
            raw = event_time_fn(msg)
            if inspect.isawaitable(raw):
                raw = await raw
        except Exception as err:
            raise RuntimeError(f"eventTime extractor threw: {err}") from err
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return int(raw.timestamp() * 1000)
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return int(raw)
        if isinstance(raw, str):
            try:
                return int(_parse_iso(raw) * 1000)
            except Exception:
                return None
        return None
    # Processing-time fallback.
    if isinstance(msg, dict) and msg.get("createdAt"):
        try:
            return int(_parse_iso(msg["createdAt"]) * 1000)
        except Exception:
            return None
    return None


def _parse_iso(s: str) -> float:
    """Return epoch seconds (float) for an ISO 8601 timestamp."""
    # Python's fromisoformat handles a subset; use a more tolerant approach.
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def normalise_late_opts(on_late: Any) -> str:
    """Validate / normalise the on_late policy: 'drop' (default) or 'include'."""
    if on_late is None or on_late == "drop":
        return "drop"
    if on_late == "include":
        return "include"
    raise ValueError(f"on_late must be 'drop' or 'include' (got {on_late!r})")


def every_to_ms(every: str) -> int:
    """Parse 'second'|'minute'|'hour'|'day'|'week' shorthand into ms."""
    mapping = {
        "second": 1000,
        "minute": 60 * 1000,
        "hour": 3600 * 1000,
        "day": 86_400 * 1000,
        "week": 7 * 86_400 * 1000,
    }
    if every not in mapping:
        raise ValueError(
            f"window_cron: every must be one of second|minute|hour|day|week (got {every!r})"
        )
    return mapping[every]


def to_iso(ms: int) -> str:
    """
    Convert epoch-ms to a JS-compatible ISO 8601 'Z' string.

    Always includes millisecond precision ('.000Z') to match
    JavaScript's Date.prototype.toISOString() output. This guarantees
    cross-language identity of state keys (which embed the windowKey ISO
    string).
    """
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    # isoformat() with timespec='milliseconds' always produces .NNN.
    s = dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return s
