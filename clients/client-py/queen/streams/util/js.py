"""
What JavaScript does, in Python: the few rules the feed streams depend on to
be byte-compatible with the JS SDK (a stream moves between SDKs, a live read
asks a runner of any SDK).

  * ``UNDEFINED`` is JavaScript's ``undefined`` (a field that is not there);
    ``None`` is JSON ``null``. A user function that answers ``UNDEFINED`` is
    one that answered ``undefined`` in JS.
  * ``js_json_dumps`` is ``JSON.stringify``: insertion order (integer-like
    keys first, as JS objects keep them), JS number text (``60``, never
    ``60.0``; ``1e-7``; ``1e+21``), ``undefined`` left out, ``NaN`` as null,
    no ASCII escaping.
  * ``js_normalize`` is ``JSON.parse(JSON.stringify(x))``: what JS would have
    written, as Python data (an integral float is an int).
  * ``js_str`` is ``String(x)``; ``js_truthy`` is JS truthiness (``[]`` and
    ``{}`` are true); ``js_iso`` is ``Date.prototype.toISOString``;
    ``js_date_parse`` is ``Date.parse`` (``None`` for NaN); ``js_sort_key``
    orders strings by UTF-16 code units, as JS compares them.
  * ``parse_duration_ms`` is the JS SDK's ``parseDurationMs`` ('30s', '1h30m').
"""

from __future__ import annotations

import inspect
import json
import math
import re
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, List, Optional

__all__ = [
    "UNDEFINED",
    "is_undefined",
    "nullish",
    "js_truthy",
    "is_number",
    "js_is_finite",
    "js_is_integer",
    "js_number_str",
    "js_str",
    "js_json_dumps",
    "js_normalize",
    "js_clone",
    "js_same",
    "js_sort_key",
    "js_iso",
    "js_date_parse",
    "parse_duration_ms",
    "now_ms",
    "utf16_len",
    "call_user",
    "js_get",
]


class _Undefined:
    """JavaScript's ``undefined``: falsy, and left out of JSON objects."""

    _instance: Optional["_Undefined"] = None

    def __new__(cls) -> "_Undefined":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "undefined"

    def __bool__(self) -> bool:
        return False

    def __reduce__(self) -> str:
        return "UNDEFINED"


UNDEFINED = _Undefined()


def is_undefined(v: Any) -> bool:
    return v is UNDEFINED


def nullish(v: Any) -> bool:
    """``v == null`` in JS: null or undefined."""
    return v is None or v is UNDEFINED


def is_number(v: Any) -> bool:
    """``typeof v === 'number'`` (a bool is not one)."""
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def js_is_finite(v: Any) -> bool:
    """``Number.isFinite(v)``."""
    if not is_number(v):
        return False
    if isinstance(v, int):
        return True
    return math.isfinite(v)


def js_is_integer(v: Any) -> bool:
    """``Number.isInteger(v)`` (``5.0`` is one)."""
    if not is_number(v):
        return False
    if isinstance(v, int):
        return True
    return math.isfinite(v) and v.is_integer()


def js_truthy(v: Any) -> bool:
    """JS truthiness: false, 0, NaN, '', null and undefined are false; every object is true."""
    if v is None or v is UNDEFINED or v is False:
        return False
    if v is True:
        return True
    if isinstance(v, (int, float)):
        return v == v and v != 0
    if isinstance(v, str):
        return len(v) > 0
    return True


# ---------------------------------------------------------------- numbers

_MAX_SAFE = 2 ** 53


def js_number_str(x: Any) -> str:
    """``Number.prototype.toString()`` for a number."""
    if isinstance(x, bool):
        return "true" if x else "false"
    if isinstance(x, int):
        if -_MAX_SAFE <= x <= _MAX_SAFE:
            return str(x)
        try:
            x = float(x)
        except OverflowError:
            return "Infinity" if x > 0 else "-Infinity"
    if x != x:
        return "NaN"
    if x == math.inf:
        return "Infinity"
    if x == -math.inf:
        return "-Infinity"
    if x == 0:
        return "0"
    if x < 0:
        return "-" + js_number_str(-x)
    r = repr(float(x))
    if "e" in r:
        mant, exp_s = r.split("e")
        exp = int(exp_s)
    else:
        mant, exp = r, 0
    if "." in mant:
        ip, fp = mant.split(".")
    else:
        ip, fp = mant, ""
    digits = ip + fp
    stripped = digits.lstrip("0")
    lz = len(digits) - len(stripped)
    s = stripped.rstrip("0") or "0"
    n = len(ip) - lz + exp
    k = len(s)
    if k <= n <= 21:
        return s + "0" * (n - k)
    if 0 < n <= 21:
        return s[:n] + "." + s[n:]
    if -6 < n <= 0:
        return "0." + "0" * (-n) + s
    e = n - 1
    sign = "+" if e >= 0 else "-"
    if k == 1:
        return f"{s}e{sign}{abs(e)}"
    return f"{s[0]}.{s[1:]}e{sign}{abs(e)}"


def js_str(v: Any) -> str:
    """``String(v)``."""
    if isinstance(v, str):
        return v
    if v is None:
        return "null"
    if v is UNDEFINED:
        return "undefined"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return js_number_str(v)
    if isinstance(v, (list, tuple)):
        return ",".join("" if nullish(x) else js_str(x) for x in v)
    if isinstance(v, datetime):
        return js_iso(v.timestamp() * 1000)
    if isinstance(v, dict):
        return "[object Object]"
    return str(v)


# ---------------------------------------------------------------- JSON

_INDEX_MAX = 2 ** 32 - 2


def _is_index_key(k: str) -> bool:
    # A canonical array index: '0', or digits without a leading zero, below 2^32 - 1.
    if not k or not k.isdigit() or not k.isascii():
        return False
    if len(k) > 1 and k[0] == "0":
        return False
    return int(k) <= _INDEX_MAX


def _key_str(k: Any) -> str:
    if isinstance(k, str):
        return k
    return js_str(k)


def _ordered_items(d: Dict[Any, Any]) -> List[Any]:
    """A JS object's own keys in the order JS keeps them: integer-like first, ascending."""
    items = [(_key_str(k), v) for k, v in d.items()]
    if not any(k[:1].isdigit() for k, _ in items):
        return items
    idx = [(k, v) for k, v in items if _is_index_key(k)]
    if not idx:
        return items
    rest = [(k, v) for k, v in items if not _is_index_key(k)]
    idx.sort(key=lambda kv: int(kv[0]))
    return idx + rest


try:  # the C escaper (ensure_ascii=False flavour) when available
    from json.encoder import c_encode_basestring as _c_quote  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    _c_quote = None

_SURROGATE = re.compile("[\ud800-\udfff]")


def _quote(s: str) -> str:
    q = _c_quote(s) if _c_quote is not None else json.encoder.py_encode_basestring(s)  # type: ignore[attr-defined]
    if not s.isascii() and _SURROGATE.search(q):
        q = _SURROGATE.sub(lambda m: "\\u%04x" % ord(m.group()), q)
    return q


def _skippable(v: Any) -> bool:
    return v is UNDEFINED or callable(v) and not isinstance(v, (dict, list, tuple, str))


def _dumps(v: Any, out: List[str]) -> None:
    if v is None:
        out.append("null")
    elif v is True:
        out.append("true")
    elif v is False:
        out.append("false")
    elif isinstance(v, str):
        out.append(_quote(v))
    elif isinstance(v, int):
        out.append(js_number_str(v))
    elif isinstance(v, float):
        out.append(js_number_str(v) if math.isfinite(v) else "null")
    elif isinstance(v, dict):
        out.append("{")
        first = True
        for k, x in _ordered_items(v):
            if _skippable(x):
                continue
            if not first:
                out.append(",")
            first = False
            out.append(_quote(k))
            out.append(":")
            _dumps(x, out)
        out.append("}")
    elif isinstance(v, (list, tuple)):
        out.append("[")
        for i, x in enumerate(v):
            if i:
                out.append(",")
            if _skippable(x):
                out.append("null")
            else:
                _dumps(x, out)
        out.append("]")
    elif isinstance(v, datetime):
        out.append(_quote(js_iso(v.timestamp() * 1000)))
    elif v is UNDEFINED:
        out.append("null")
    elif hasattr(v, "to_json") and callable(v.to_json):
        _dumps(v.to_json(), out)
    else:
        raise TypeError(f"JSON: {type(v).__name__} is not serializable")


def js_json_dumps(v: Any) -> Any:
    """``JSON.stringify(v)``: a string, or ``UNDEFINED`` for ``undefined``."""
    if _skippable(v):
        return UNDEFINED
    out: List[str] = []
    _dumps(v, out)
    return "".join(out)


def js_normalize(v: Any) -> Any:
    """``JSON.parse(JSON.stringify(v))``, as Python data: ``UNDEFINED`` for undefined."""
    if v is None or v is True or v is False or isinstance(v, str):
        return v
    if isinstance(v, int):
        if -_MAX_SAFE <= v <= _MAX_SAFE:
            return v
        return _normalize_float(float(v)) if abs(v) < 1e308 else None
    if isinstance(v, float):
        return _normalize_float(v)
    if isinstance(v, dict):
        out: Dict[str, Any] = {}
        for k, x in _ordered_items(v):
            if _skippable(x):
                continue
            out[k] = js_normalize(x)
        return out
    if isinstance(v, (list, tuple)):
        return [None if _skippable(x) else js_normalize(x) for x in v]
    if isinstance(v, datetime):
        return js_iso(v.timestamp() * 1000)
    if v is UNDEFINED or callable(v):
        return UNDEFINED
    if hasattr(v, "to_json") and callable(v.to_json):
        return js_normalize(v.to_json())
    raise TypeError(f"JSON: {type(v).__name__} is not serializable")


def _normalize_float(v: float) -> Any:
    if not math.isfinite(v):
        return None
    if v.is_integer() and abs(v) < 1e21:
        return int(v)
    return v


def js_clone(v: Any) -> Any:
    """``structuredClone`` of JSON-like data (undefined and NaN kept)."""
    if isinstance(v, dict):
        return {k: js_clone(x) for k, x in v.items()}
    if isinstance(v, list):
        return [js_clone(x) for x in v]
    if isinstance(v, tuple):
        return tuple(js_clone(x) for x in v)
    return v


def js_same(a: Any, b: Any) -> bool:
    """``a === b``: identity for objects, value for primitives (``1 === 1.0``, ``NaN !== NaN``)."""
    if a is b:
        return not (isinstance(a, float) and a != a)
    if isinstance(a, bool) or isinstance(b, bool):
        return False
    if is_number(a) and is_number(b):
        return a == b
    if isinstance(a, str) and isinstance(b, str):
        return a == b
    return False


def js_get(obj: Any, key: str) -> Any:
    """``obj[key]`` for a JSON-like value: ``UNDEFINED`` when not an object or not there."""
    if isinstance(obj, dict):
        return obj.get(key, UNDEFINED)
    return UNDEFINED


def utf16_len(s: str) -> int:
    """``s.length`` in JS (UTF-16 code units)."""
    if s.isascii():
        return len(s)
    return len(s) + sum(1 for ch in s if ord(ch) > 0xFFFF)


_ASTRAL = re.compile("[\U00010000-\U0010ffff]")


def _as_surrogates(m: "re.Match[str]") -> str:
    c = ord(m.group()) - 0x10000
    return chr(0xD800 + (c >> 10)) + chr(0xDC00 + (c & 0x3FF))


def js_sort_key(s: str) -> str:
    """A key that sorts as JS compares strings (by UTF-16 code units)."""
    if s.isascii() or not _ASTRAL.search(s):
        return s
    return _ASTRAL.sub(_as_surrogates, s)


# ---------------------------------------------------------------- time

def now_ms() -> int:
    """``Date.now()``."""
    return time.time_ns() // 1_000_000


def _civil_from_days(z: int) -> Any:
    # Howard Hinnant's algorithm: days since 1970-01-01 -> (y, m, d), any year.
    z += 719468
    era = z // 146097  # floor division: the C version's adjustment is built in
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return (y + 1 if m <= 2 else y), m, d


def _days_from_civil(y: int, m: int, d: int) -> int:
    y -= 1 if m <= 2 else 0
    era = y // 400
    yoe = y - era * 400
    mp = m - 3 if m > 2 else m + 9
    doy = (153 * mp + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def js_iso(t: Any) -> str:
    """``new Date(t).toISOString()``; raises ValueError where JS throws RangeError."""
    if not js_is_finite(t) or abs(t) > 8.64e15:
        raise ValueError("Invalid time value")
    ms = int(t)  # TimeClip truncates toward zero
    days, rem = divmod(ms, 86_400_000)
    y, mo, d = _civil_from_days(days)
    hh, rem = divmod(rem, 3_600_000)
    mi, rem = divmod(rem, 60_000)
    ss, mss = divmod(rem, 1000)
    if 0 <= y <= 9999:
        ys = f"{y:04d}"
    else:
        ys = f"{'+' if y > 0 else '-'}{abs(y):06d}"
    return f"{ys}-{mo:02d}-{d:02d}T{hh:02d}:{mi:02d}:{ss:02d}.{mss:03d}Z"


_ISO_RE = re.compile(
    r"^(?P<y>[+-]\d{6}|\d{4})"
    r"(?:-(?P<mo>\d{2})(?:-(?P<d>\d{2}))?)?"
    r"(?:[Tt ](?P<h>\d{2}):(?P<mi>\d{2})(?::(?P<s>\d{2})(?:\.(?P<f>\d+))?)?"
    r"(?P<z>[Zz]|[+-]\d{2}:?\d{2})?)?$"
)
_DAYS_IN_MONTH_MAX = 31


def js_date_parse(s: Any) -> Optional[int]:
    """``Date.parse(s)`` (epoch ms), ``None`` where JS answers NaN.

    The ISO forms V8 reads: a date alone is UTC, a date and time without an
    offset is local time; any number of fraction digits (cut to the
    millisecond); a day up to 31 in any month (it runs into the next one, as
    V8 does). RFC 2822 too ('Thu, 06 Nov 2026 09:00:00 GMT').
    """
    if not isinstance(s, str):
        s = js_str(s)
    m = _ISO_RE.match(s)
    if m:
        y = int(m.group("y"))
        if m.group("y") == "-000000":
            return None
        mo = int(m.group("mo") or 1)
        d = int(m.group("d") or 1)
        h = int(m.group("h") or 0)
        mi = int(m.group("mi") or 0)
        sec = int(m.group("s") or 0)
        frac = m.group("f") or ""
        ms = int((frac + "000")[:3]) if frac else 0
        if not (1 <= mo <= 12) or not (1 <= d <= _DAYS_IN_MONTH_MAX):
            return None
        if h > 24 or mi > 59 or sec > 59 or (h == 24 and (mi or sec or ms)):
            return None
        days = _days_from_civil(y, mo, 1) + d - 1
        t = days * 86_400_000 + h * 3_600_000 + mi * 60_000 + sec * 1000 + ms
        z = m.group("z")
        if z is None:
            if m.group("h") is not None:
                # A date and time without an offset is local time.
                try:
                    wall = datetime(1970, 1, 1) + timedelta(milliseconds=t)
                    t = int(round(wall.timestamp() * 1000))
                except (OverflowError, ValueError, OSError):
                    pass
        elif z not in ("Z", "z"):
            sign = 1 if z[0] == "+" else -1
            digits = z[1:].replace(":", "")
            off = int(digits[:2]) * 60 + int(digits[2:])
            t -= sign * off * 60_000
        if abs(t) > 8.64e15:
            return None
        return t
    try:
        dt = parsedate_to_datetime(s)
    except (TypeError, ValueError, IndexError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return int(dt.timestamp() * 1000)


# ---------------------------------------------------------------- durations

_UNIT_MS = {"ms": 1, "s": 1000, "m": 60 * 1000, "h": 60 * 60 * 1000, "d": 24 * 60 * 60 * 1000}
_DURATION_PARTS = re.compile(r"\d+(?:\.\d+)?[a-z]+")
_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|h|d)$")


def parse_duration_ms(value: Any) -> Any:
    """A duration STRING ('250ms', '30s', '1h30m') in milliseconds; numbers are refused."""
    if not isinstance(value, str) or value.strip() == "":
        raise ValueError(
            f"duration must be a string with a unit, e.g. '250ms', '30s', '15m', '24h', '7d' — got {js_json_dumps(value) if not callable(value) else 'a function'}. "
            "For a plain number use ttlSeconds (KV) or delayMs (timers), which carry their unit in the name."
        )
    low = value.strip().lower()
    parts = _DURATION_PARTS.findall(low)
    if not parts or "".join(parts) != low:
        raise ValueError(f"invalid duration '{value}': expected <number><unit> parts, e.g. '30s' or '1h30m'")
    ms: Any = 0
    for part in parts:
        m = _DURATION_RE.match(part)
        if not m:
            raise ValueError(f"invalid duration '{value}': unknown unit in '{part}' (use ms, s, m, h, d)")
        num = float(m.group(1)) if "." in m.group(1) else int(m.group(1))
        ms = ms + num * _UNIT_MS[m.group(2)]
    if not (ms > 0):
        raise ValueError(f"invalid duration '{value}': must be greater than zero")
    if isinstance(ms, float) and ms.is_integer():
        ms = int(ms)
    return ms


# ---------------------------------------------------------------- user functions

_ARITY: Dict[Any, Optional[int]] = {}


def _arity(fn: Callable[..., Any]) -> Optional[int]:
    """How many positional arguments `fn` takes (None: any number)."""
    try:
        return _ARITY[fn]
    except (KeyError, TypeError):
        pass
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        n: Optional[int] = None
    else:
        n = 0
        for p in sig.parameters.values():
            if p.kind == inspect.Parameter.VAR_POSITIONAL:
                n = None
                break
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                n += 1
    try:
        if len(_ARITY) < 100_000:
            _ARITY[fn] = n
    except TypeError:
        pass
    return n


async def call_user(fn: Callable[..., Any], *args: Any) -> Any:
    """Call a user function as JS does: extra arguments are dropped, a coroutine is awaited."""
    n = _arity(fn)
    out = fn(*args) if n is None or n >= len(args) else fn(*args[:n])
    if inspect.isawaitable(out):
        out = await out
    return out


def date_of(value: Any) -> Optional[float]:
    """A Python date/datetime as epoch ms (a naive datetime is local time, as in JS)."""
    if isinstance(value, datetime):
        return value.timestamp() * 1000
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp() * 1000
    return None

