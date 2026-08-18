"""
Op builders for the key/value surface -- the ONE place a KV op is shaped.

Every caller goes through here: the standalone ``POST /api/v1/kv`` surface, the
transaction rider (``tx.kv.*``), and whatever comes next. That is not tidiness,
it is the only way the two wires cannot drift, and it is what makes
``tests/kv_unit/test_kv_wire.py`` a contract rather than a sample.

WHAT THIS MODULE VALIDATES, AND WHAT IT DELIBERATELY DOES NOT.

The semantic rules live in ``queen.kv_apply_v1`` so that all seven clients and
the embedded broker inherit them without a line of their own
(PLAN_KV_TIMERS.md §5.1). What is checked here is only what the SDK has to
decide anyway in order to build the body at all, plus the two cases §5.3 calls
client-side bugs:

  * the expiry, because the SDK must choose between ``ttlSeconds`` and
    ``forever`` -- there is no third thing to send, and no ``expiresAt`` input
    field exists;
  * ``expect`` explicitly None, which is a bug in the caller's code and NEVER a
    silent downgrade to an unconditional upsert. If the word `expect` was
    written, an intention to fence was declared.

Everything else -- the charset of a namespace, the key ceiling, the taxonomy of
`reason` -- is the procedure's, and duplicating it here would give the product
two places to disagree about what a KV operation is.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

__all__ = [
    "UNSET",
    "is_unset",
    "get",
    "get_many",
    "get_prefix",
    "put",
    "put_if_absent",
    "delete",
    "incr",
    "expiry_fields",
]


class _Unset:
    """Sentinel for "the caller said nothing", which `None` cannot express here
    because `expect=None` has to be an ERROR and not a default."""

    _instance: Optional["_Unset"] = None

    def __new__(cls) -> "_Unset":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "<unset>"

    def __bool__(self) -> bool:
        return False


UNSET = _Unset()


def is_unset(value: Any) -> bool:
    """True when the caller said nothing about this argument."""
    return isinstance(value, _Unset)


def _name(field: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def expiry_fields(
    ttl_seconds: Any = UNSET,
    forever: Any = UNSET,
    ttl: Any = UNSET,
    until: Any = UNSET,
) -> Dict[str, Any]:
    """Resolve the four SDK spellings of "when does this die" into the ONE pair
    the wire accepts.

    §5.1: every put, putIfAbsent and incr carries EXACTLY ONE of ``ttlSeconds``
    (a positive integer) and ``forever: true``. Zero or two declarations is
    ``kv_expiry_not_specified``. A put does NOT inherit the previous expiry --
    it is not expressible, and a put that inherited one in silence is the
    fastest way to make a marker immortal.

    ``ttl`` (a ``timedelta``) and ``until`` (a ``datetime``) are SDK sugar and
    are converted to a delta HERE, at send time. There is no ``expiresAt``
    input field on the wire and there will not be one: an absolute instant
    would put a second clock into a product that has exactly one.
    """
    declared = [n for n, v in (("ttl_seconds", ttl_seconds), ("forever", forever), ("ttl", ttl), ("until", until)) if not isinstance(v, _Unset)]
    if not declared:
        raise ValueError(
            "kv_expiry_not_specified: pass exactly one of ttl_seconds=<int>, "
            "ttl=<timedelta>, until=<datetime> or forever=True -- an expiry is "
            "mandatory on every KV write, and a put never inherits the previous one"
        )
    if len(declared) > 1:
        raise ValueError(f"exactly one expiry may be given, got {declared}")

    if not isinstance(forever, _Unset):
        if forever is not True:
            raise ValueError("forever must be True when given; omit it otherwise")
        return {"forever": True}

    if not isinstance(until, _Unset):
        if not isinstance(until, datetime):
            raise TypeError("until must be a datetime")
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        seconds = math.ceil((until - datetime.now(timezone.utc)).total_seconds())
        return _ttl_field(seconds)

    if not isinstance(ttl, _Unset):
        if isinstance(ttl, timedelta):
            return _ttl_field(math.ceil(ttl.total_seconds()))
        return _ttl_field(ttl)

    return _ttl_field(ttl_seconds)


def _ttl_field(seconds: Any) -> Dict[str, Any]:
    if isinstance(seconds, bool) or not isinstance(seconds, int):
        raise TypeError("ttl_seconds must be an int number of seconds")
    if seconds <= 0:
        raise ValueError("ttl_seconds must be greater than zero")
    return {"ttlSeconds": seconds}


def _expect_field(expect: Any) -> Dict[str, Any]:
    """§5.3 / S14: `expect` present but empty is a client-side bug."""
    if isinstance(expect, _Unset):
        return {}
    if expect is None:
        raise ValueError(
            "expect=None is not an unconditional write: if you wrote `expect` you "
            "declared an intention to fence, and an absent value is a bug in the "
            "caller. Omit the argument entirely for an upsert, or pass "
            "expect=0 for \"must not exist\""
        )
    if isinstance(expect, bool) or not isinstance(expect, int):
        raise TypeError("expect must be an int version (0 means \"must not exist\")")
    if expect < 0:
        raise ValueError("expect must be >= 0")
    return {"expect": expect}


def _required_field(required: Any) -> Dict[str, Any]:
    """`required:true` escalates a lost precondition into a rolled-back
    transaction (§6.1 point 5), and it is opt-in PER OPERATION. Absent by
    default, and absent from the body when False, so a bundle that does not use
    it is byte-identical to one written before the field existed."""
    if not required:
        return {}
    return {"required": True}


def _int_field(name: str, value: Any) -> Any:
    """`bool` is a subclass of `int` in Python, so `delta=True` would go out as
    1 and a rate limiter would count somebody's typo as a request. This is the
    Python-specific trap of §10.4 and it is closed here, once."""
    if isinstance(value, bool):
        raise TypeError(f"{name} must be a number, not a bool")
    if not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    return value


# ---------------------------------------------------------------------------
# Reads.
# ---------------------------------------------------------------------------


def get(ns: str, key: str) -> Dict[str, Any]:
    return {"op": "get", "ns": _name("ns", ns), "key": _name("key", key)}


def get_many(ns: str, keys: Iterable[str]) -> Dict[str, Any]:
    key_list: List[str] = list(keys)
    if not key_list:
        raise ValueError("get_many needs at least one key")
    for k in key_list:
        _name("key", k)
    return {"op": "getMany", "ns": _name("ns", ns), "keys": key_list}


def get_prefix(
    ns: str,
    prefix: str,
    *,
    after: Optional[str] = None,
    limit: Optional[int] = None,
    keys_only: bool = False,
) -> Dict[str, Any]:
    """§5.5. `limit` is CLAMPED by the server and never rejected, so it is not
    validated here beyond being an int -- a 400 on a too-high limit is an error
    the user cannot fix without reading the server's configuration.

    An empty prefix is refused, because it is the declared boundary: a
    namespace is not a table to enumerate.
    """
    if not isinstance(prefix, str) or not prefix:
        raise ValueError("kv_prefix_required: getPrefix needs a non-empty prefix -- a namespace is not a table to enumerate")
    op: Dict[str, Any] = {"op": "getPrefix", "ns": _name("ns", ns), "prefix": prefix}
    if after is not None:
        op["after"] = _name("after", after)
    if limit is not None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an int")
        op["limit"] = limit
    if keys_only:
        op["keysOnly"] = True
    return op


# ---------------------------------------------------------------------------
# Writes.
# ---------------------------------------------------------------------------


def put(
    ns: str,
    key: str,
    value: Any,
    *,
    ttl_seconds: Any = UNSET,
    forever: Any = UNSET,
    ttl: Any = UNSET,
    until: Any = UNSET,
    expect: Any = UNSET,
    required: bool = False,
) -> Dict[str, Any]:
    op: Dict[str, Any] = {"op": "put", "ns": _name("ns", ns), "key": _name("key", key), "value": value}
    op.update(expiry_fields(ttl_seconds, forever, ttl, until))
    op.update(_expect_field(expect))
    op.update(_required_field(required))
    return op


def put_if_absent(
    ns: str,
    key: str,
    value: Any,
    *,
    ttl_seconds: Any = UNSET,
    forever: Any = UNSET,
    ttl: Any = UNSET,
    until: Any = UNSET,
    required: bool = False,
) -> Dict[str, Any]:
    """The alias of §5.3, kept under its own name on the wire because it is the
    name of the thing and because `applied`, answering "did I win?", is the most
    frequent question asked of this API.

    It takes NO `expect`: it desugars to put+expect:0 inside the procedure, and
    a different expect alongside it is a 22023. Not offering the parameter is
    cheaper than explaining it.
    """
    op: Dict[str, Any] = {"op": "putIfAbsent", "ns": _name("ns", ns), "key": _name("key", key), "value": value}
    op.update(expiry_fields(ttl_seconds, forever, ttl, until))
    op.update(_required_field(required))
    return op


def delete(
    ns: str,
    key: str,
    *,
    expect: Any = UNSET,
    required: bool = False,
) -> Dict[str, Any]:
    op: Dict[str, Any] = {"op": "delete", "ns": _name("ns", ns), "key": _name("key", key)}
    op.update(_expect_field(expect))
    op.update(_required_field(required))
    return op


def incr(
    ns: str,
    key: str,
    *,
    delta: Any = 1,
    min: Any = None,
    max: Any = None,
    ttl_seconds: Any = UNSET,
    forever: Any = UNSET,
    ttl: Any = UNSET,
    until: Any = UNSET,
    required: bool = False,
) -> Dict[str, Any]:
    """§5.4. No `expect`, ever: incr is the way OUT of the CAS loop, and a
    precondition here would reintroduce the cycle it exists to remove.

    Two things worth knowing at the call site rather than in a bug report:

      * with `max`, `applied` IS the admission decision -- the write does not
        saturate and does not truncate, it refuses, so the request that would
        have blown the ceiling has not spent any budget;
      * the TTL is CREATE-ONLY. A live row keeps its expiry, or a fixed-window
        limiter on an always-busy client would never close its window, i.e.
        would stop limiting exactly under load. An expired row counts as zero
        and starts a new window, which is what makes the limiter one call.
    """
    op: Dict[str, Any] = {
        "op": "incr",
        "ns": _name("ns", ns),
        "key": _name("key", key),
        "delta": _int_field("delta", delta),
    }
    if min is not None:
        op["min"] = _int_field("min", min)
    if max is not None:
        op["max"] = _int_field("max", max)
    op.update(expiry_fields(ttl_seconds, forever, ttl, until))
    op.update(_required_field(required))
    return op
