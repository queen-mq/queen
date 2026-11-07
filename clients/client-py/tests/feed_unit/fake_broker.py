"""
A broker in memory that speaks the wires a feed stream uses --
``POST /api/v1/feed``, ``/api/v1/kv``, ``/api/v1/transaction``,
``/api/v1/locks``, the ephemeral push/pop -- so the tests run the client's own
KV, Lock, TransactionBuilder, FeedReader and Ephemeral against it. Port of the
JS SDK's test-v2/feed-unit/fakeBroker.js, same rules.

Every write is one log position, as on the broker: a transaction (its pushes
share the position), a lock call. Only pushes are feed entries. The clock is
virtual: ``broker.now_ms`` is the wall clock, which the test moves; a write is
stamped ``max(now_ms, the last write's time)``, and a caught-up read resolves to
the last write's time, so the clock moves only with writes, as on the broker.
Locks expire on the real clock (the Lock handle keeps its deadline on it).
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, unquote

import httpx

from queen.builders.transaction_builder import TransactionBuilder
from queen.ephemeral.ephemeral import Ephemeral
from queen.feed.feed import FeedReader, condition_holds
from queen.kv.kv import KV
from queen.locks.locks import Lock, Locks
from queen.streams.runtime.feed.state import shard_of
from queen.streams.util.js import js_date_parse, js_iso

T_START = js_date_parse("2026-11-06T09:00:00.000Z")


def _glob(pattern: str) -> "re.Pattern[str]":
    return re.compile("^" + "".join(".*" if c == "*" else "." if c == "?" else re.escape(c) for c in pattern) + r"\Z")


def _copy(v: Any) -> Any:
    return json.loads(json.dumps(v))


class _Q:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeBroker:
    def __init__(self, start: Optional[int] = None) -> None:
        start = T_START if start is None else start
        self.now_ms = start
        self.last_now_ms = start
        self.applied = 0
        self.log: List[Dict[str, Any]] = []
        self.kv: Dict[str, Dict[str, Any]] = {}
        self.kv_version = 100
        self.ids: set = set()
        self.offsets: Dict[str, int] = {}
        self.pids: Dict[str, str] = {}
        self.locks: Dict[str, Dict[str, Any]] = {}
        self.tokens = 0
        self.waiters: List[asyncio.Event] = []
        self.txns: List[Dict[str, Any]] = []
        self.feed_reads: List[Dict[str, Any]] = []
        self.injected: List[Dict[str, Any]] = []
        self.clients: List["FakeHttp"] = []
        self.ephemeral: Dict[str, List[Dict[str, Any]]] = {}
        self.ephemeral_waiters: List[asyncio.Event] = []
        self.ephemeral_seq = 0

    # ---- the log ------------------------------------------------------------

    def _stamp(self) -> Dict[str, int]:
        self.applied += 1
        self.last_now_ms = max(self.now_ms, self.last_now_ms)
        return {"position": self.applied, "t": self.last_now_ms}

    def _wake(self) -> None:
        w, self.waiters = self.waiters, []
        for ev in w:
            ev.set()

    @staticmethod
    def _kv_key(ns: str, key: str) -> str:
        return f"{ns}\u0000{key}"

    def _append_pushes(self, items: List[Dict[str, Any]], position: int, t: int) -> List[Dict[str, Any]]:
        results = []
        for it in items:
            partition = it.get("partition") if it.get("partition") is not None else "Default"
            pk = f"{it['queue']}\u0000{partition}"
            if pk not in self.pids:
                self.pids[pk] = f"pid-{len(self.pids) + 1}"
            offset = self.offsets.get(pk, 0) + 1
            self.offsets[pk] = offset
            self.ids.add(f"{pk}\u0000{it['transactionId']}")
            self.log.append({
                "position": position, "kind": "push", "queue": it["queue"], "partition": partition,
                "partitionId": self.pids[pk], "offset": offset, "id": f"m-{position}-{offset}",
                "transactionId": it["transactionId"], "payload": it.get("payload"), "ts": js_iso(t),
            })
            results.append({"type": "push", "status": "queued", "transactionId": it["transactionId"]})
        return results

    def push(self, items: Any) -> int:
        """A producer's push: one position for all of ``items`` ({queue, partition, data, transactionId?})."""
        lst = items if isinstance(items, list) else [items]
        st = self._stamp()
        position, t = st["position"], st["t"]
        self._append_pushes([
            {
                "queue": x["queue"], "partition": x.get("partition"), "payload": _copy(x.get("data")),
                "transactionId": x.get("transactionId") or f"in-{position}-{i}",
            }
            for i, x in enumerate(lst)
        ], position, t)
        self._wake()
        return position

    def messages(self, queue: str) -> List[Dict[str, Any]]:
        """Entries of a queue, as stored."""
        return [e for e in self.log if e["queue"] == queue]

    def payloads(self, queue: str) -> List[Any]:
        return [e["payload"] for e in self.messages(queue)]

    def kv_get(self, ns: str, key: str) -> Optional[Dict[str, Any]]:
        return self.kv.get(self._kv_key(ns, key))

    def kv_value(self, ns: str, key: str) -> Any:
        row = self.kv_get(ns, key)
        return row["value"] if row else None

    def kv_put(self, ns: str, key: str, value: Any) -> int:
        """Write a KV row as another process's commit would (one position)."""
        self._stamp()
        self.kv_version += 1
        self.kv[self._kv_key(ns, key)] = {"value": _copy(value), "version": self.kv_version}
        return self.kv_version

    def kv_keys(self, ns: str, prefix: str = "") -> List[str]:
        """The KV keys of a namespace that start with ``prefix``."""
        head = f"{ns}\u0000{prefix}"
        return [k[len(ns) + 1:] for k in self.kv if k.startswith(head)]

    # ---- the feed -------------------------------------------------------------

    async def read(self, body: Dict[str, Any], again: bool = False) -> Dict[str, Any]:
        if not again:
            self.feed_reads.append(_copy(body))
        frm = body.get("from")
        if frm is None or frm == "now":
            frm = self.applied + 1
        elif frm == "earliest":
            frm = 1
        frm = int(frm)
        queues = [_glob(q) for q in (body["queues"] if isinstance(body["queues"], list) else [body["queues"]])] if body.get("queues") else None
        parts = (body["partitions"] if isinstance(body["partitions"], list) else [body["partitions"]]) if body.get("partitions") else None
        where = body.get("where") or []
        since = js_date_parse(body["since"]) if body.get("since") else None
        mx = body.get("maxRecords", 1000) if body.get("maxRecords") is not None else 1000
        shard = body.get("shard") or None
        shard_queues = [_glob(q) for q in shard["queues"]] if shard and shard.get("queues") else None
        out: List[Dict[str, Any]] = []
        nxt = self.applied + 1
        for e in self.log:
            if e["position"] < frm:
                continue
            if len(out) >= mx and e["position"] != out[-1]["position"]:
                nxt = e["position"]
                break
            if queues and not any(r.match(e["queue"]) for r in queues):
                continue
            if parts and e["partition"] not in parts:
                continue
            if since is not None and js_date_parse(e["ts"]) < since:
                continue
            if shard and (not shard_queues or any(r.match(e["queue"]) for r in shard_queues)) and shard_of(e["partition"], shard["count"]) != shard["index"]:
                continue
            msg = {"queue": e["queue"], "partition": e["partition"], "transactionId": e["transactionId"], "offset": e["offset"], "position": e["position"], "data": e["payload"]}
            if not all(condition_holds(c, msg) for c in where):
                continue
            out.append(e)
        caught_up = nxt > self.applied
        if not out and caught_up and (body.get("maxWaitMs") or 0) > 0 and not again:
            ev = asyncio.Event()
            self.waiters.append(ev)
            try:
                await asyncio.wait_for(ev.wait(), body["maxWaitMs"] / 1000)
            except asyncio.TimeoutError:
                pass
            finally:
                if ev in self.waiters:
                    self.waiters.remove(ev)
            return await self.read({**body, "from": frm}, True)
        last = None
        for e in self.log:
            if e["position"] < nxt:
                last = e
        resolved = self.last_now_ms if caught_up else js_date_parse(last["ts"] if last else js_iso(self.last_now_ms))
        return {
            "applied": self.applied,
            "caughtUp": caught_up,
            "entries": [dict(e) for e in out],
            "next": nxt,
            "resolvedTs": js_iso(resolved),
        }

    # ---- kv -------------------------------------------------------------------

    def kv_call(self, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = []
        for index, op in enumerate(operations):
            if op["op"] == "get":
                row = self.kv.get(self._kv_key(op["ns"], op["key"]))
                results.append(
                    {"index": index, "op": "get", "key": op["key"], "found": True, "value": row["value"], "version": row["version"]}
                    if row else {"index": index, "op": "get", "key": op["key"], "found": False}
                )
            elif op["op"] == "getMany":
                rows, missing = [], []
                for k in op["keys"]:
                    row = self.kv.get(self._kv_key(op["ns"], k))
                    if row:
                        rows.append({"key": k, "value": row["value"], "version": row["version"]})
                    else:
                        missing.append(k)
                results.append({"index": index, "op": "getMany", "rows": rows, "missing": missing, "truncated": False})
            elif op["op"] == "getPrefix":
                keys = sorted(self.kv_keys(op["ns"], op["prefix"]))
                after = op.get("after")
                keys = [k for k in keys if after is None or k > after]
                limit = min(op.get("limit") if op.get("limit") is not None else 100, 1000)
                page = keys[:limit]
                rows = []
                for k in page:
                    row = self.kv[self._kv_key(op["ns"], k)]
                    rows.append({"key": k, "version": row["version"]} if op.get("keysOnly") else {"key": k, "value": row["value"], "version": row["version"]})
                truncated = len(keys) > limit
                results.append({"index": index, "op": "getPrefix", "rows": rows, "truncated": truncated, "nextAfter": page[-1] if truncated else None})
            else:
                raise RuntimeError(f"fake broker: kv op {op['op']} not served")
        return {"results": results}

    # ---- locks ----------------------------------------------------------------

    def _lock_row(self, name: str) -> Optional[Dict[str, Any]]:
        row = self.locks.get(name)
        return row if row and row["expiresAt"] > time.monotonic() else None

    def locks_call(self, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = []
        for op in operations:
            row = self._lock_row(op["name"])
            if op["op"] == "acquire":
                if row and row["owner"] != op.get("owner"):
                    results.append({"acquired": False, "reason": "held", "holders": [{"owner": row["owner"], "token": row["token"]}]})
                    continue
                self._stamp()
                self.tokens += 1
                token = self.tokens
                self.locks[op["name"]] = {"owner": op.get("owner"), "token": token, "expiresAt": time.monotonic() + op["ttlSeconds"]}
                results.append({"acquired": True, "slot": 0, "token": token, "owner": op.get("owner"),
                                "guard": {"op": "check", "ns": "queen-locks", "key": op["name"], "expect": token, "required": True}})
            elif op["op"] == "renew":
                if not row or row["token"] != op["token"]:
                    results.append({"renewed": False, "reason": "lost", "holders": []})
                    continue
                self._stamp()
                self.tokens += 1
                token = self.tokens
                self.locks[op["name"]] = {"owner": row["owner"], "token": token, "expiresAt": time.monotonic() + op["ttlSeconds"]}
                results.append({"renewed": True, "slot": 0, "token": token,
                                "guard": {"op": "check", "ns": "queen-locks", "key": op["name"], "expect": token, "required": True}})
            elif op["op"] == "release":
                if not row or row["token"] != op["token"]:
                    results.append({"released": False, "reason": "lost"})
                    continue
                self._stamp()
                del self.locks[op["name"]]
                results.append({"released": True})
            elif op["op"] == "get":
                results.append({"held": bool(row), "holders": [{"owner": row["owner"], "token": row["token"]}] if row else []})
            else:
                raise RuntimeError(f"fake broker: lock op {op['op']} not served")
        return {"results": results}

    def steal_lock(self, name: str, owner: str = "someone-else", token: int = 9999, seconds: float = 60.0) -> None:
        self.locks[name] = {"owner": owner, "token": token, "expiresAt": time.monotonic() + seconds}

    # ---- transactions -----------------------------------------------------------

    def inject_next(self, spec: Dict[str, Any]) -> None:
        """Something for the next transaction: ``before(broker)`` runs first; ``error``
        is raised instead of the answer, before the transaction applies
        (``when: 'before'``) or after it (a lost answer)."""
        self.injected.append(spec)

    def transaction(self, body: Dict[str, Any]) -> Dict[str, Any]:
        items: List[Dict[str, Any]] = []
        for op in body.get("operations") or []:
            if op.get("type") == "push":
                items.extend(op["items"])
        kv = body.get("kv") or []
        injected = self.injected.pop(0) if self.injected else None
        if injected and injected.get("before"):
            injected["before"](self)
        if injected and injected.get("error") and injected.get("when") != "after":
            raise injected["error"]
        # Duplicates first, as the broker answers them.
        seen = set()
        for it in items:
            k = f"{it['queue']}\u0000{it.get('partition') if it.get('partition') is not None else 'Default'}\u0000{it['transactionId']}"
            if k in self.ids or k in seen:
                return {"success": False, "reason": "duplicate", "error": "QDUP a pushed message is a duplicate; the transaction rolled back", "results": []}
            seen.add(k)
        if len(kv) > 64:
            return {"success": False, "reason": "bad_request", "error": f"kv_too_many_ops: {len(kv)} ops in one call, the ceiling is 64"}
        for i, op in enumerate(kv):
            def fail(kv_reason: str, row: Optional[Dict[str, Any]], i: int = i) -> Dict[str, Any]:
                return {
                    "success": False, "reason": "kv_precondition", "failedIndex": len(items) + i, "kvReason": kv_reason,
                    "version": row["version"] if row else None, "value": row["value"] if row else None,
                    "error": "QKV a required KV precondition failed; the transaction rolled back", "results": [],
                }
            if op["op"] == "check" and op["ns"] == "queen-locks":
                lrow = self._lock_row(op["key"])
                if not lrow:
                    return fail("missing", None)
                if lrow["token"] != op.get("expect"):
                    return fail("version", {"version": lrow["token"], "value": {"owner": lrow["owner"]}})
                continue
            row = self.kv.get(self._kv_key(op["ns"], op["key"]))
            if op["op"] == "check":
                if op.get("expect") == 0 and row:
                    return fail("exists", row)
                if op.get("expect") != 0 and (not row or row["version"] != op.get("expect")):
                    return fail("version" if row else "missing", row)
                continue
            if op["op"] == "putIfAbsent" and row:
                return fail("exists", row)
            if op["op"] == "put" and "expect" in op:
                if not row:
                    return fail("missing", None)
                if row["version"] != op["expect"]:
                    return fail("version", row)
            if op["op"] == "put" and not op.get("forever") and not op.get("ttlSeconds"):
                return {"success": False, "reason": "bad_request", "error": "kv_expiry_not_specified"}
        st = self._stamp()
        position, t = st["position"], st["t"]
        results = self._append_pushes([{**it, "partition": it.get("partition") if it.get("partition") is not None else "Default"} for it in items], position, t)
        written: Dict[str, int] = {}
        for i, op in enumerate(kv):
            if op["op"] == "check":
                results.append({"index": len(items) + i, "type": "kv", "op": "check", "key": op["key"]})
                continue
            k = self._kv_key(op["ns"], op["key"])
            if op["op"] == "delete":
                self.kv.pop(k, None)
                results.append({"index": len(items) + i, "type": "kv", "op": "delete", "key": op["key"], "applied": True})
                continue
            self.kv_version += 1
            version = self.kv_version
            self.kv[k] = {"value": _copy(op.get("value")), "version": version}
            written[op["key"]] = version
            results.append({"index": len(items) + i, "type": "kv", "op": op["op"], "key": op["key"], "version": version, "applied": True})
        self.txns.append({"position": position, "items": items, "kv": kv, "written": written})
        self._wake()
        if injected and injected.get("error") and injected.get("when") == "after":
            raise injected["error"]
        return {"success": True, "transactionId": f"txn-{position}", "results": results}

    # ---- ephemeral ----------------------------------------------------------------

    def _eph_wake(self) -> None:
        w, self.ephemeral_waiters = self.ephemeral_waiters, []
        for ev in w:
            ev.set()

    def ephemeral_push(self, body: Dict[str, Any]) -> Dict[str, Any]:
        q = self.ephemeral.setdefault(body["queue"], [])
        for m in body.get("messages") or []:
            self.ephemeral_seq += 1
            q.append({"id": f"e-{self.ephemeral_seq}", "partition": body.get("partition") or "Default", "payload": _copy(m.get("payload")), "attempts": 0})
        self._eph_wake()
        return {"pushed": len(body.get("messages") or [])}

    async def ephemeral_pop(self, params: Dict[str, List[str]]) -> Dict[str, Any]:
        queue = params.get("queue", [""])[0]
        batch = int((params.get("batch") or ["1"])[0] or 1)
        wait = (params.get("wait") or [""])[0] == "true"
        timeout = int((params.get("timeout") or ["0"])[0] or 0)

        def take() -> Dict[str, Any]:
            q = self.ephemeral.get(queue, [])
            out = q[:batch]
            del q[:batch]
            return {"queue": queue, "messages": out}

        got = take()
        if got["messages"] or not wait:
            return got
        ev = asyncio.Event()
        self.ephemeral_waiters.append(ev)
        try:
            await asyncio.wait_for(ev.wait(), timeout / 1000)
        except asyncio.TimeoutError:
            pass
        finally:
            if ev in self.ephemeral_waiters:
                self.ephemeral_waiters.remove(ev)
        return take()

    # ---- clients --------------------------------------------------------------------

    def client(self, name: str = "client") -> "FakeHttp":
        c = FakeHttp(self, name)
        self.clients.append(c)
        return c


class FakeHttp:
    """An HTTP client on the broker, as one process would have; ``cut`` makes it unreachable."""

    def __init__(self, broker: FakeBroker, name: str) -> None:
        self.broker = broker
        self.name = name
        self.cut = False
        self.post_hook: Optional[Callable[..., Any]] = None

    def _refuse(self) -> None:
        if self.cut:
            raise httpx.ConnectError("connect ECONNREFUSED")

    async def post(self, path: str, body: Any = None, request_timeout_millis: Any = None, affinity_key: Any = None, *, retry_kind: Any = None) -> Any:
        if self.post_hook is not None:
            await self.post_hook(path, body)
        self._refuse()
        await asyncio.sleep(0)
        copy = _copy(body)
        b = self.broker
        if path == "/api/v1/feed":
            return await b.read(copy)
        if path == "/api/v1/kv":
            return b.kv_call(copy["operations"])
        if path == "/api/v1/transaction":
            return b.transaction(copy)
        if path == "/api/v1/locks":
            return b.locks_call(copy["operations"])
        if path == "/api/v1/ephemeral/push":
            return b.ephemeral_push(copy)
        raise RuntimeError(f"fake broker: no {path}")

    async def get(self, path: str, request_timeout_millis: Any = None, affinity_key: Any = None, *, retry_kind: Any = None) -> Any:
        self._refuse()
        await asyncio.sleep(0)
        route, _, query = path.partition("?")
        if route == "/api/v1/ephemeral/pop":
            return await self.broker.ephemeral_pop(parse_qs(query))
        raise RuntimeError(f"fake broker: no GET {route}")

    async def delete(self, path: str, request_timeout_millis: Any = None, affinity_key: Any = None, *, retry_kind: Any = None) -> Any:
        m = re.match(r"^/api/v1/ephemeral/queue/(.+)$", path)
        if m:
            self.broker.ephemeral.pop(unquote(m.group(1)), None)
            return {"deleted": True}
        raise RuntimeError(f"fake broker: no DELETE {path}")

    async def close(self) -> None:
        pass


class FakeQueen:
    """A Queen client stand-in made of the client's own parts, on one fake HTTP
    client. ``ephemeral=True`` gives it the ephemeral queues too (live-state
    reads); without, a runner serves no state."""

    def __init__(self, broker: FakeBroker, name: str = "queen", *, ephemeral: bool = False) -> None:
        self.http = broker.client(name)
        self._locks = Locks(self.http)
        self.kv = KV(self.http)
        self.ephemeral = Ephemeral(self.http) if ephemeral else None

    def transaction(self) -> TransactionBuilder:
        return TransactionBuilder(self.http)

    def lock(self, name: str, **opts: Any) -> Lock:
        return Lock(self._locks, name, **opts)

    def feed(self, options: Any = None, **kwargs: Any) -> FeedReader:
        return FeedReader(self.http, lambda url: self.http, "http", options, **kwargs)

    def queue(self, name: str) -> _Q:
        return _Q(name)


def fake_queen(broker: FakeBroker, name: str = "queen", *, ephemeral: bool = False) -> FakeQueen:
    return FakeQueen(broker, name, ephemeral=ephemeral)


async def until(cond: Callable[[], Any], ms: float = 3000, what: str = "condition") -> None:
    end = time.monotonic() + ms / 1000
    while True:
        v = cond()
        if asyncio.iscoroutine(v):
            v = await v
        if v:
            return
        if time.monotonic() > end:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.005)


async def wait(ms: float) -> None:
    await asyncio.sleep(ms / 1000)


# ---- helpers the JS tests share -------------------------------------------------

T0 = T_START


def at(b: FakeBroker, ms: int) -> None:
    b.now_ms = T0 + ms


def fast(**o: Any) -> Dict[str, Any]:
    return {"wait_millis": 20, "commit_every_millis": 0, "retry_delay_millis": 1, **o}


def quiet(**o: Any) -> Dict[str, Any]:
    return {"on_error": lambda err, ctx: None, **o}


def crash(q: FakeQueen) -> None:
    """A process that dies: its client stops answering, so it can neither commit nor keep its lock."""
    q.http.cut = True


def payloads(b: FakeBroker, queue: str) -> List[Any]:
    return [e["payload"] for e in b.messages(queue)]
