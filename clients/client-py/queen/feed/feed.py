"""
The tenant feed: every push to a tenant's queues, in the order the broker's
log committed them, read by position (``POST /api/v1/feed``). Port of the JS
SDK's feed/Feed.js, with the same options and the same rules.

    # What happened to one booking today, then stop.
    async for e in client.feed(partitions=["booking-9137"], since="1d", follow=False):
        print(e["position"], e["queue"], e["partition"], e["data"])

    # Every Italian order from now on, as it is committed.
    async for e in client.feed(queues=["orders"], where=[["data.country", "=", "IT"]]):
        ...

A position is the broker log entry that wrote a message. One number orders
every queue of the tenant, and the pushes of one transaction share theirs.
Nothing is leased, acked or moved: the feed is a read, and every consumer
group still gets its messages.

Followers serve it. A leader answers 421 with the followers to ask, and the
reader then reads from those (and goes back to the configured URL if none
answers). A single broker serves it itself.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Dict, List, Mapping, Optional

import httpx

from ..errors import QueenError
from ..streams.util.js import (
    UNDEFINED,
    is_number,
    js_date_parse,
    js_get,
    js_iso,
    js_is_finite,
    js_is_integer,
    js_json_dumps,
    js_normalize,
    js_same,
    js_sort_key,
    js_truthy,
    now_ms,
    nullish,
)

__all__ = [
    "FeedReader",
    "FeedAbortedError",
    "FeedUnsupportedError",
    "where_condition",
    "normalize_where",
    "condition_holds",
    "since_to_iso",
    "FEED_ON_LEADER",
]

FEED_ON_LEADER = "feed_on_leader"
OPS = ("=", "==", "!=", "<", "<=", ">", ">=", "in", "exists", "prefix", "!prefix")
DEFAULT_WAIT_MILLIS = 5000
# A long poll may hold the request this long; the HTTP timeout must be longer.
MAX_WAIT_MILLIS = 30000
_FEED_PATH = "/api/v1/feed"


class FeedAbortedError(QueenError):
    """A read its caller cancelled (``signal``): the reader stays where it was."""

    aborted = True
    status = None
    code = "aborted"


class FeedUnsupportedError(QueenError):
    """A 404 from the feed route: the broker predates the tenant feed."""

    status = 404
    code = "no_feed_route"
    no_feed_route = True


class FeedError(QueenError):
    """Every node asked was the leader or did not answer."""


def _jstr(v: Any) -> str:
    out = js_json_dumps(v)
    return "undefined" if out is UNDEFINED else out


def where_condition(field: Any, op: Any, value: Any = UNDEFINED) -> Dict[str, Any]:
    """One ``where`` condition, checked: ``{field, op, value}``.

        where_condition("data.country", "=", "IT")
        where_condition("data.total", ">", 100)
        where_condition("partition", "prefix", "booking-")
        where_condition("transactionId", "!prefix", "fd-")
        where_condition("data.vip", "exists")

    Fields: queue, partition, transactionId, offset, position, data, data.<path>.
    Ops: = != < <= > >= in exists prefix !prefix. A field the message does not
    have holds for no op, ``!=`` and ``!prefix`` included.
    """
    if not isinstance(field, str) or len(field) == 0:
        raise ValueError(f"where: field must be a non-empty string, got {_jstr(field)}")
    if op not in OPS:
        raise ValueError(f"where: unknown op {_jstr(op)} (= != < <= > >= in exists prefix !prefix)")
    if op == "in" and not isinstance(value, (list, tuple)):
        raise ValueError(f"where: in on {field} needs an array")
    if op in ("prefix", "!prefix") and not isinstance(value, str):
        raise ValueError(f"where: {op} on {field} needs a string")
    cond: Dict[str, Any] = {"field": field, "op": op}
    if op != "exists":
        cond["value"] = None if value is UNDEFINED else (list(value) if isinstance(value, tuple) else value)
    return cond


def normalize_where(where: Any) -> List[Dict[str, Any]]:
    """``where`` as the caller wrote it: a condition, ``[field, op, value]``, or a list of either."""
    if where is None or where is UNDEFINED:
        return []
    if isinstance(where, (list, tuple)) and (len(where) == 0 or not isinstance(where[0], str)):
        items = list(where)
    else:
        items = [where]
    out = []
    for c in items:
        if isinstance(c, (list, tuple)):
            c = list(c)
            out.append(where_condition(c[0] if len(c) > 0 else UNDEFINED, c[1] if len(c) > 1 else UNDEFINED, c[2] if len(c) > 2 else UNDEFINED))
        elif isinstance(c, Mapping):
            out.append(where_condition(c.get("field", UNDEFINED), c.get("op", UNDEFINED), c.get("value", UNDEFINED)))
        else:
            raise ValueError(f"where: a condition is {{ field, op, value }} or [field, op, value], got {_jstr(c)}")
    return out


def _json_eq(a: Any, b: Any) -> bool:
    if is_number(a) and is_number(b):
        return a == b
    if a is None or b is None or not isinstance(a, (dict, list)) or not isinstance(b, (dict, list)):
        return js_same(a, b)
    return js_json_dumps(a) == js_json_dumps(b)


def _compare(a: Any, b: Any) -> Optional[int]:
    if is_number(a) and is_number(b):
        return -1 if a < b else 1 if a > b else 0
    if isinstance(a, str) and isinstance(b, str):
        ka, kb = js_sort_key(a), js_sort_key(b)
        return -1 if ka < kb else 1 if ka > kb else 0
    return None


_JS_NUM = re.compile(r"^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*$")


def _js_index(key: str) -> Optional[int]:
    """``Number(key)`` as an array index, or None."""
    if key.strip() == "":
        return 0
    if not _JS_NUM.match(key):
        k = key.strip().lower()
        try:
            if k.startswith(("0x", "0o", "0b")):
                return int(k, 0)
        except ValueError:
            return None
        return None
    n = float(key)
    if n.is_integer() and n >= 0:
        return int(n)
    return None


def _field_value(field: str, msg: Any) -> Any:
    if field == "queue":
        v = js_get(msg, "queue")
        return js_get(msg, "queueName") if nullish(v) else v
    if field == "partition":
        v = js_get(msg, "partition")
        return js_get(msg, "partitionName") if nullish(v) else v
    if field in ("transactionId", "offset", "position", "data"):
        return js_get(msg, field)
    if not field.startswith("data."):
        return UNDEFINED
    cur = js_get(msg, "data")
    for key in field[5:].split("."):
        if not isinstance(cur, (dict, list)):
            return UNDEFINED
        if isinstance(cur, list):
            i = _js_index(key)
            cur = cur[i] if i is not None and i < len(cur) else UNDEFINED
        else:
            cur = cur.get(key, UNDEFINED)
        if cur is UNDEFINED:
            return UNDEFINED
    return cur


def condition_holds(cond: Mapping[str, Any], msg: Any) -> bool:
    """Whether ``cond`` holds for a message, evaluated here: the same rules the
    broker applies to a feed read, for a stream whose source is a queue."""
    v = _field_value(cond["field"], msg)
    if v is UNDEFINED:
        return False
    op = cond["op"]
    value = cond.get("value", UNDEFINED)
    if op == "exists":
        return True
    if op in ("=", "=="):
        return _json_eq(v, value)
    if op == "!=":
        return not _json_eq(v, value)
    if op == "in":
        return isinstance(value, list) and any(_json_eq(v, x) for x in value)
    if op == "prefix":
        return isinstance(v, str) and isinstance(value, str) and v.startswith(value)
    if op == "!prefix":
        return isinstance(v, str) and isinstance(value, str) and not v.startswith(value)
    c = _compare(v, value)
    if c is None:
        return False
    if op == "<":
        return c < 0
    if op == "<=":
        return c <= 0
    if op == ">":
        return c > 0
    if op == ">=":
        return c >= 0
    return False


_UNITS = {"ms": 1, "s": 1000, "m": 60000, "h": 3600000, "d": 86400000, "w": 604800000}
_SINCE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)\s*$")


def since_to_iso(since: Any, now: Optional[float] = None) -> str:
    """A start time as the broker reads it: a datetime, epoch milliseconds, an
    ISO string, or a duration back from now ('30s', '15m', '2h', '1d', '1w')."""
    if now is None:
        now = now_ms()
    if isinstance(since, datetime):
        return js_iso(since.timestamp() * 1000)
    if is_number(since) and js_is_finite(since):
        return js_iso(since)
    if isinstance(since, str):
        m = _SINCE_RE.match(since)
        if m:
            n = float(m.group(1)) if "." in m.group(1) else int(m.group(1))
            return js_iso(now - n * _UNITS[m.group(2)])
        t = js_date_parse(since)
        if t is not None:
            return js_iso(t)
    raise ValueError(
        f"feed: since must be a datetime, epoch ms, an ISO time or a duration like '15m', got {_jstr(since)}"
    )


def _names(v: Any, what: str) -> Optional[List[str]]:
    if v is None or v is UNDEFINED:
        return None
    out = list(v) if isinstance(v, (list, tuple)) else [v]
    for x in out:
        if not isinstance(x, str) or len(x) == 0:
            raise ValueError(f"feed: {what} must be names (strings), got {_jstr(x)}")
    return out if out else None


def _shard_option(shard: Any) -> Optional[Dict[str, Any]]:
    """``shard`` as the broker reads it, checked: ``{count, index, queues?}``, or None."""
    if shard is None or shard is UNDEFINED:
        return None
    count = shard.get("count") if isinstance(shard, Mapping) else None
    index = shard.get("index") if isinstance(shard, Mapping) else None
    queues = shard.get("queues") if isinstance(shard, Mapping) else None
    if not js_is_integer(count) or count < 1 or count > 4096:
        raise ValueError(f"feed: shard.count is a whole number from 1 to 4096, got {_jstr(count)}")
    if not js_is_integer(index) or index < 0 or index >= count:
        raise ValueError(f"feed: shard.index is from 0 to {int(count) - 1}, got {_jstr(index)}")
    out: Dict[str, Any] = {"count": int(count), "index": int(index)}
    q = _names(queues, "shard.queues")
    if q:
        out["queues"] = q
    return out


_ENTRY_FIELDS = (
    ("position", "position"),
    ("kind", "kind"),
    ("queue", "queue"),
    ("partition", "partition"),
    ("partitionId", "partitionId"),
    ("offset", "offset"),
    ("id", "id"),
    ("transactionId", "transactionId"),
    ("data", "payload"),
    ("ts", "ts"),
    ("createdAt", "ts"),
)


def to_entry(e: Mapping[str, Any]) -> Dict[str, Any]:
    """A feed entry as the reader hands it out: the message in Queen's shape, plus its position."""
    out: Dict[str, Any] = {}
    for name, src in _ENTRY_FIELDS:
        if src in e:
            out[name] = e[src]
    return out


def status_of(err: BaseException) -> Optional[int]:
    """The HTTP status an error carries, if any."""
    s = getattr(err, "status", None)
    if isinstance(s, int) and not isinstance(s, bool):
        return s
    if isinstance(err, httpx.HTTPStatusError):
        return err.response.status_code
    return None


def body_of(err: BaseException) -> Any:
    """The parsed answer an HTTP error carries, if any."""
    body = getattr(err, "body", None)
    if isinstance(body, dict):
        return body
    if isinstance(err, httpx.HTTPStatusError):
        try:
            parsed = err.response.json()
        except Exception:  # noqa: BLE001
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _opt(o: Mapping[str, Any], *names: str) -> Any:
    for n in names:
        if n in o and o[n] is not None and o[n] is not UNDEFINED:
            return o[n]
    return UNDEFINED


class FeedReader:
    """Reads the tenant feed by position. Made by ``client.feed(...)``.

    Options (snake_case, or the JS camelCase names in a mapping):
        queues        names or patterns (``orders*``); default every queue
        partitions    partition names
        where         conditions sent to the broker ([field, op, value] or {field, op, value})
        from_         first position, 'now' or 'earliest'; default 'now' ('earliest' with since)
        since         only messages created at or after it ('1d', ISO, datetime, epoch ms)
        follow        keep waiting for new entries (default True); False stops once caught up
        wait_millis   long poll of each read while caught up (default 5000, max 30000)
        max_records   messages per read (broker default 1000)
        max_bytes     payload bytes per read (broker default 1 MiB)
        follower_urls nodes to read from first, e.g. one follower's URL
        shard         {count, index, queues?}: only the partitions whose name hashes
                      (FNV-1a, 32 bit, UTF-8) to index of count; with queues, only
                      those queues are split and the others are read whole
    """

    def __init__(
        self,
        http_client: Any,
        client_for: Callable[[str], Any],
        scheme: str = "http",
        options: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        o: Dict[str, Any] = dict(options or {})
        o.update({k: v for k, v in kwargs.items()})
        self._http = http_client
        self._client_for = client_for
        self._scheme = scheme or "http"
        since = _opt(o, "since")
        wait = _opt(o, "wait_millis", "waitMillis")
        self._opts: Dict[str, Any] = {
            "queues": _names(_opt(o, "queues", "queue"), "queues"),
            "partitions": _names(_opt(o, "partitions", "partition"), "partitions"),
            "where": normalize_where(_opt(o, "where")),
            "kinds": _opt(o, "kinds"),
            "since": since_to_iso(since) if since is not UNDEFINED else UNDEFINED,
            "follow": _opt(o, "follow") is not False,
            "waitMillis": min(max(DEFAULT_WAIT_MILLIS if wait is UNDEFINED else wait, 0), MAX_WAIT_MILLIS),
            "maxRecords": _opt(o, "max_records", "maxRecords"),
            "maxBytes": _opt(o, "max_bytes", "maxBytes"),
            "shard": _shard_option(_opt(o, "shard")),
        }
        self._followers: List[Dict[str, Any]] = []
        self._follower_at = 0
        self._fallback_until = 0
        self._stopped = False
        for url in _opt(o, "follower_urls", "followerUrls") or []:
            self._followers.append({"url": url, "client": client_for(url)})
        start = _opt(o, "from_", "from", "start")
        if start is UNDEFINED:
            start = "earliest" if since is not UNDEFINED else "now"
        self._position: Any = start
        if not (start in ("now", "earliest") or (js_is_integer(start) and start >= 0)):
            raise ValueError(f"feed: from must be a position, 'now' or 'earliest', got {_jstr(start)}")
        if is_number(start):
            self._position = int(start)

    @property
    def position(self) -> Any:
        """Where the next read starts: a position once a read has answered."""
        return self._position

    def seek(self, position: Any) -> None:
        """Move the reader (a runner whose commit lost goes back to the committed position)."""
        self._position = position

    async def read(self, *, wait_millis: Optional[float] = None, signal: Optional[asyncio.Event] = None) -> Dict[str, Any]:
        """One read: ``{entries, next, resolvedTs, caughtUp, applied}``, and the
        reader moves to ``next``. Waits up to ``wait_millis`` for an entry while
        there is nothing new. ``signal`` (an asyncio.Event) cancels the read: it
        raises FeedAbortedError, and the reader stays where it was."""
        wait = min(max(self._opts["waitMillis"] if wait_millis is None else wait_millis, 0), MAX_WAIT_MILLIS)
        body: Dict[str, Any] = {"from": self._position, "maxWaitMs": wait}
        o = self._opts
        if o["queues"]:
            body["queues"] = o["queues"]
        if o["partitions"]:
            body["partitions"] = o["partitions"]
        if o["where"]:
            body["where"] = o["where"]
        if o["kinds"] is not UNDEFINED and js_truthy(o["kinds"]):
            body["kinds"] = o["kinds"]
        if o["since"] is not UNDEFINED:
            body["since"] = o["since"]
        if o["maxRecords"] is not UNDEFINED:
            body["maxRecords"] = o["maxRecords"]
        if o["maxBytes"] is not UNDEFINED:
            body["maxBytes"] = o["maxBytes"]
        if o["shard"]:
            body["shard"] = o["shard"]
        answer = await self._post(body, wait + 10000, signal)
        answer = answer if isinstance(answer, dict) else {}
        self._position = answer.get("next", UNDEFINED)
        return {
            "entries": [to_entry(e) for e in (answer.get("entries") or [])],
            "next": answer.get("next", UNDEFINED),
            "resolvedTs": answer.get("resolvedTs", UNDEFINED),
            "caughtUp": answer.get("caughtUp") is True,
            "applied": answer.get("applied", UNDEFINED),
        }

    async def probe(self) -> Dict[str, Any]:
        """One call that reads nothing (from 'now') and leaves the reader where it
        is: it raises what a read would, a broker without the feed included."""
        answer = await self._post({"from": "now", "maxWaitMs": 0, "maxRecords": 1}, 10000)
        return answer if isinstance(answer, dict) else {}

    def __aiter__(self) -> AsyncIterator[Dict[str, Any]]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Dict[str, Any]]:
        """Every entry, in log order. Stops once caught up when ``follow`` is False, or at stop()."""
        while not self._stopped:
            page = await self.read(wait_millis=self._opts["waitMillis"] if self._opts["follow"] else 0)
            for e in page["entries"]:
                yield e
                if self._stopped:
                    return
            if not self._opts["follow"] and page["caughtUp"]:
                return

    def stop(self) -> None:
        """Ends the iteration after the entry in hand."""
        self._stopped = True

    async def close(self) -> None:
        """Closes the clients this reader opened to followers."""
        self._stopped = True
        followers, self._followers = self._followers, []
        for f in followers:
            client = f["client"]
            closer = getattr(client, "close", None) or getattr(client, "destroy", None)
            if callable(closer):
                try:
                    out = closer()
                    if asyncio.iscoroutine(out) or isinstance(out, asyncio.Future):
                        await out
                except Exception:  # noqa: BLE001 - best effort
                    pass

    def _learn(self, followers: Any) -> None:
        seen = {f["url"] for f in self._followers}
        for f in followers or []:
            addr = f.get("httpAddr").strip() if isinstance(f, dict) and isinstance(f.get("httpAddr"), str) else ""
            if not addr:
                continue
            url = addr if re.match(r"^https?://", addr) else f"{self._scheme}://{addr}"
            if url in seen:
                continue
            seen.add(url)
            self._followers.append({"url": url, "client": self._client_for(url)})

    async def _send(self, client: Any, body: Dict[str, Any], timeout_millis: float, signal: Optional[asyncio.Event]) -> Any:
        wire = js_normalize(body)
        if signal is None:
            return await client.post(_FEED_PATH, wire, timeout_millis)
        if signal.is_set():
            raise FeedAbortedError("feed read aborted by the caller")
        task = asyncio.ensure_future(client.post(_FEED_PATH, wire, timeout_millis))
        waiter = asyncio.ensure_future(signal.wait())
        try:
            done, _ = await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            task.cancel()
            waiter.cancel()
            raise
        if task in done:
            waiter.cancel()
            return task.result()
        task.cancel()
        try:
            await task
        except BaseException:  # noqa: BLE001 - it was cancelled on purpose
            pass
        raise FeedAbortedError("feed read aborted by the caller")

    async def _post(self, body: Dict[str, Any], timeout_millis: float, signal: Optional[asyncio.Event] = None) -> Any:
        last_error: Optional[BaseException] = None
        # Each follower once, then the configured URL; a leader's 421 adds followers.
        attempt = 0
        while attempt < len(self._followers) + 3:
            attempt += 1
            use_follower = len(self._followers) > 0 and now_ms() >= self._fallback_until
            target = self._followers[self._follower_at % len(self._followers)] if use_follower else None
            client = target["client"] if target else self._http
            try:
                return await self._send(client, body, timeout_millis, signal)
            except FeedAbortedError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - judged below
                if getattr(err, "aborted", False):
                    raise
                last_error = err
                status = status_of(err)
                if status == 421 and getattr(err, "code", None) == FEED_ON_LEADER:
                    # A leader: read from the followers it names (this node may lead now).
                    b = body_of(err)
                    self._learn(b.get("followers") if isinstance(b, dict) else None)
                    if target:
                        self._follower_at += 1
                    self._fallback_until = 0
                    continue
                if target and (not status or status >= 500):
                    # That follower is gone or busy: the next one, then the configured URL.
                    self._follower_at += 1
                    if self._follower_at % len(self._followers) == 0:
                        self._fallback_until = now_ms() + 5000
                    continue
                if status == 404:
                    e = FeedUnsupportedError(
                        "feed: this broker has no POST /api/v1/feed (it answered 404): it is older than 2.8.0, "
                        "the first broker with the tenant feed."
                    )
                    raise e from err
                raise
        e2 = FeedError(
            "feed: every node asked is the leader or did not answer; the feed is read from followers "
            "(pass follower_urls, or run the broker with QUEEN_FEED_ON_LEADER=true on a test cluster)"
        )
        raise e2 from last_error
