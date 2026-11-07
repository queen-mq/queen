"""
Shared helpers for window operators. Mirror of _windowCommon.js.
"""

from __future__ import annotations

import inspect
import math
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


# ---------------------------------------------------------------------------
# Feed streams (Stream.from_feed): the event-time options, as the JS SDK reads
# them (_windowCommon.js). Durations are milliseconds; a number is seconds.
# ---------------------------------------------------------------------------

def _js():
    from ..util import js as _jsmod  # local: util imports nothing from operators

    return _jsmod


def ms_value(x: Any) -> Any:
    """A millisecond count as JS keeps it: an int when whole, else the float."""
    if isinstance(x, float) and x.is_integer():
        return int(x)
    return x


def duration_ms(value: Any, name: str, what: str) -> Optional[Any]:
    """A duration option: a number of seconds, or a string with a unit ('30s',
    '5m', '1h30m'). Milliseconds, or None when the option is not given."""
    js = _js()
    if value is None or value is js.UNDEFINED:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not js.js_is_finite(value) or value < 0:
            raise ValueError(f"{what}: {name} is a number of seconds from 0, or a duration string such as '30s'")
        return math.floor(value * MS_PER_SEC)
    if isinstance(value, str):
        return js.parse_duration_ms(value)
    raise ValueError(f"{what}: {name} is a number of seconds, or a duration string such as '30s'")


def window_lateness_ms(value: Any, what: str) -> Any:
    """A window's ``allowedLateness``: seconds (as before), or a duration string
    like a pattern's ('5m'). Milliseconds, never negative."""
    js = _js()
    if value is None or value is js.UNDEFINED:
        return 0
    if isinstance(value, str):
        return js.parse_duration_ms(value)
    if isinstance(value, bool):
        n: Any = 1 if value else 0
    elif isinstance(value, (int, float)):
        n = value
    else:
        raise ValueError(f"{what}: allowedLateness is a number of seconds, or a duration string such as '5m'")
    if not js.js_is_finite(n):
        raise ValueError(f"{what}: allowedLateness is a number of seconds, or a duration string such as '5m'")
    return max(0, math.floor(n * MS_PER_SEC))


def feed_time_options(op: Any, event_time: Any, idle: Any, what: str) -> None:
    """The feed's event-time options of a window: ``event_time=False`` (the
    feed's clock, even after a step on event time) and ``idle`` (a quiet feed
    moves the watermark on). ``idle`` goes in the config when given;
    ``event_time=False`` does not (the chain hash says which clock each step
    runs on)."""
    op.event_time_off = event_time is False
    op.idle_ms = duration_ms(idle, "idle", what)
    if op.idle_ms is not None and op.idle_ms <= 0:
        raise ValueError(f"{what}: idle is a duration above zero")
    if op.idle_ms is not None:
        op.config["idle"] = op.idle_ms


def step_time_options(op: Any, opts: dict, what: str) -> None:
    """The event-time options of a pattern, a join or a gather (feed sources):
    ``eventTime`` (msg) -> time, or False for the feed's clock; ``allowedLateness``;
    ``onLate`` ('drop' | 'include'); ``idle``. Added to the config only when given."""
    js = _js()
    event_time = _first(opts, "eventTime", "event_time")
    allowed_lateness = _first(opts, "allowedLateness", "allowed_lateness")
    on_late = _first(opts, "onLate", "on_late")
    idle = _first(opts, "idle")
    if event_time is not None and event_time is not js.UNDEFINED and event_time is not False and not callable(event_time):
        raise ValueError(f"{what}: eventTime is a function (msg) => time, or false for the feed's clock")
    op.event_time_fn = event_time if callable(event_time) else None
    op.event_time_off = event_time is False
    lat = duration_ms(allowed_lateness, "allowedLateness", what)
    op.allowed_lateness_ms = 0 if lat is None else lat
    op.lateness_given = allowed_lateness is not None and allowed_lateness is not js.UNDEFINED
    op.on_late_policy = normalise_late_opts(None if on_late is js.UNDEFINED else on_late)
    op.on_late_given = on_late is not None and on_late is not js.UNDEFINED
    op.idle_ms = duration_ms(idle, "idle", what)
    if op.idle_ms is not None and op.idle_ms <= 0:
        raise ValueError(f"{what}: idle is a duration above zero")
    if op.event_time_fn:
        op.config["eventTime"] = True
    if op.lateness_given:
        op.config["allowedLateness"] = op.allowed_lateness_ms
    if op.on_late_given:
        op.config["onLate"] = op.on_late_policy
    if op.idle_ms is not None:
        op.config["idle"] = op.idle_ms


def _first(opts: dict, *names: str) -> Any:
    js = _js()
    for n in names:
        if n in opts:
            return opts[n]
    return js.UNDEFINED


def datetime_ms(dt: datetime) -> Any:
    """A datetime as JS epoch milliseconds (whole, as a JS Date keeps them; naive = local time)."""
    if dt.tzinfo is None:
        t = dt.timestamp() * 1000
        return int(t)
    delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
    us = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return int(us / 1000) if us < 0 else us // 1000


async def extract_event_time_js(msg: Any, event_time_fn: Optional[Callable[[Any], Any]]) -> Any:
    """``extractEventTime`` of the JS SDK: the epoch ms of a message, or None.

    With a function: a datetime, a finite number (epoch ms, kept as given) or a
    string Date.parse reads; anything else is no time. A function that raises
    is reported as 'eventTime extractor threw: ...'. Without one: the
    message's ``createdAt``.
    """
    js = _js()
    if event_time_fn is not None:
        try:
            raw = await js.call_user(event_time_fn, msg)
        except Exception as err:  # noqa: BLE001 - bubbles up, as in JS
            raise RuntimeError(f"eventTime extractor threw: {err}") from err
        if raw is None or raw is js.UNDEFINED:
            return None
        if isinstance(raw, datetime):
            return datetime_ms(raw)
        if js.is_number(raw):
            return raw if js.js_is_finite(raw) else None
        if isinstance(raw, str):
            return js.js_date_parse(raw)
        return None
    created = js.js_get(msg, "createdAt") if isinstance(msg, dict) else js.UNDEFINED
    if js.js_truthy(created):
        return js.js_date_parse(created if isinstance(created, str) else js.js_str(created))
    return None
