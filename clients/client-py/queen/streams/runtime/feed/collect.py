"""
``.join(sides=...)`` and ``.gather()`` on the tenant feed: collect the parts of
one entity until every part is in, or its deadline passes. Port of the JS
SDK's runtime/feed/collect.js.

join (sides): one message per side, any number of sides, in any order, within
``within`` of the first. When the last side arrives, ``select(got, key)`` is
sent; a new message for the key then starts a new set.

gather (steps or count): the parts of one run. With ``steps``, one message per
named step, as a join; with ``count(msg, value)``, distinct messages (by queue,
partition and transaction id) until as many as a part says there are. When all
are in, ``onComplete(got, key)`` is sent, and the run is remembered until its
deadline, so a late duplicate does not open a new one that would then time out.

Either way, a deadline that passes first sends ``onTimeout(got, missing, key)``.

State key ``c\\u001f<key>``; a value is ``{key, first, due, parts | list+ids,
expected}``, or ``{key, due, done: true}`` for a finished gather.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any, Dict, Iterator, List

from ...util.js import UNDEFINED, call_user, js_get, js_is_finite, js_is_integer, js_iso, js_str, js_truthy, nullish
from .patterns import clone, kept_of, message_of, snapshot_of
from .state import REBUILD_BATCH, DueHeap, FeedState, is_buffer_key

SEP = "\u001f"


def _or_empty(v: Any) -> str:
    # `${x ?? ''}`
    return "" if nullish(v) else js_str(v)


def _part_id(msg: Any) -> str:
    if js_truthy(js_get(msg, "transactionId")):
        return f"{js_str(js_get(msg, 'queue'))}{SEP}{js_str(js_get(msg, 'partition'))}{SEP}{js_str(js_get(msg, 'transactionId'))}"
    # An earlier step's result: one per window, or per deadline, of its key.
    if js_truthy(js_get(msg, "derived")):
        c = js_get(msg, "ctx")
        if not js_truthy(c):
            c = {}
        ws = js_get(c, "windowStart")
        if nullish(ws):
            ws = js_get(c, "deadline")
        return f"d{SEP}{js_str(js_get(msg, 'key'))}{SEP}{_or_empty(js_get(c, 'kind'))}{SEP}{_or_empty(ws)}{SEP}{_or_empty(js_get(msg, 'ts'))}"
    k = js_get(msg, "key")
    if nullish(k):
        k = js_get(msg, "partition")
    return f"{js_str(js_get(msg, 'position'))}{SEP}{_or_empty(js_get(msg, 'offset'))}{SEP}{_or_empty(k)}"


def _classify(k: str, v: Any) -> Any:
    if is_buffer_key(k):
        return "held"
    return "open" if isinstance(v, dict) and not js_truthy(v.get("done", UNDEFINED)) else None


class CollectEngine:
    def __init__(self, *, op: Any, state: FeedState) -> None:
        self._op = op
        self._state = state
        self._heap = DueHeap()
        self.kind = op.collect_kind
        state.classify_with(_classify)

    async def _passes(self, part: Dict[str, Any], msg: Any, value: Any) -> bool:
        if part["queue"] is not None:
            return js_get(msg, "queue") == part["queue"]
        return js_truthy(await call_user(part["pred"], msg, value))

    async def relevant(self, env: Dict[str, Any], t: Any) -> bool:
        """Whether a message is a part (with steps or sides: one of their tests passes; by count: every message is)."""
        op = self._op
        if not op.parts:
            return True
        msg = message_of(env, t)
        value = env.get("value", UNDEFINED)
        for p in op.parts:
            if await self._passes(p, msg, value):
                return True
        return False

    def rebuild(self) -> None:
        self._heap.clear()
        for k, v in self._state.items_view():
            self._index(k, v)

    async def rebuild_async(self) -> None:
        self._heap.clear()
        n = 0
        for k, v in self._state.entries():
            self._index(k, v)
            n += 1
            if n % REBUILD_BATCH == 0:
                await asyncio.sleep(0)

    def _index(self, k: str, v: Any) -> None:
        if not is_buffer_key(k) and isinstance(v, dict) and js_is_finite(v.get("due")):
            self._heap.push(v["due"], k)

    def next_due(self) -> Any:
        self._prune()
        top = self._heap.peek()
        return top[0] if top is not None else None

    @property
    def open(self) -> int:
        return self._state.count("open")

    def advance(self, t: Any, emits: List[Dict[str, Any]], limit: Any = math.inf) -> None:
        n = 0
        while n < limit:
            n += 1
            self._prune()
            top = self._heap.peek()
            if top is None or top[0] > t:
                return
            _, sk = self._heap.pop()  # type: ignore[misc]
            v = self._state.get(sk)
            self._state.delete(sk)
            if js_truthy(v.get("done", UNDEFINED)) or not self._op.on_timeout:
                continue
            got = self._got(v)
            missing = self._missing(v)
            emits.append({
                "key": v["key"],
                "ctx": {"kind": "timeout", "key": v["key"], "partition": v["key"], "deadline": v["due"] - 1, "missing": missing},
                "state": v,
                "time": v["due"],
                "eventTime": v["due"] - 1,
                "compute": _timeout_of(self._op.on_timeout, got, missing, v["key"]),
            })

    async def process(self, env: Dict[str, Any], t: Any, emits: List[Dict[str, Any]]) -> str:
        op = self._op
        msg = message_of(env, t)
        value = env.get("value", UNDEFINED)
        part = None
        if op.parts:
            for p in op.parts:
                if await self._passes(p, msg, value):
                    part = p["name"]
                    break
            if part is None:
                return "ok"
        key = js_str(await call_user(op.key_fn, msg, value)) if op.key_fn else js_str(env.get("key", UNDEFINED))
        sk = f"c{SEP}{key}"
        run = self._state.get(sk)
        # A gather that finished: remembered until its deadline, and quiet.
        if run is not UNDEFINED and js_truthy(run.get("done", UNDEFINED)):
            return "ok"
        snap = snapshot_of(msg, await kept_of(op, msg, value))
        if op.parts:
            if run is not UNDEFINED and run["parts"].get(part, UNDEFINED) is not UNDEFINED and op.on_repeat == "keep":
                return "ok"
            if run is not UNDEFINED:
                nxt = {**run, "parts": {**run["parts"], part: snap}}
            else:
                nxt = {"key": key, "first": t, "due": t + op.within_ms + 1, "parts": {part: snap}}
        else:
            pid = _part_id(msg)
            if run is not UNDEFINED and pid in run["ids"]:
                return "ok"
            expected = run["expected"] if run is not UNDEFINED else None
            if op.count_fn:
                n = await call_user(op.count_fn, msg, value)
                if js_is_integer(n) and n > 0:
                    expected = n if expected is None else max(expected, n)
            if run is not UNDEFINED:
                nxt = {**run, "list": [*run["list"], snap], "ids": [*run["ids"], pid], "expected": expected}
            else:
                nxt = {"key": key, "first": t, "due": t + op.within_ms + 1, "list": [snap], "ids": [pid], "expected": expected}
        if self._complete(nxt):
            got = self._got(nxt)
            emits.append({
                "key": key,
                "ctx": {"kind": "complete", "key": key, "partition": key, "deadline": nxt["due"] - 1},
                "state": nxt,
                "eventTime": t,
                "compute": _complete_of(op, got, key),
            })
            if op.remember:
                self._state.set(sk, {"key": key, "due": nxt["due"], "done": True})
                if run is UNDEFINED:
                    self._heap.push(nxt["due"], sk)
            else:
                self._state.delete(sk)
            return "ok"
        self._state.set(sk, nxt)
        if run is UNDEFINED:
            self._heap.push(nxt["due"], sk)
        return "ok"

    def inspect(self, key: Any) -> Any:
        """The run of ``key``, for a live read."""
        v = self._state.get(f"c{SEP}{js_str(key)}")
        return self._view(v) if v is not UNDEFINED else None

    def list(self) -> Iterator[Dict[str, Any]]:
        for k, v in self._state.items_view():
            if not is_buffer_key(k):
                yield self._view(v)

    def _view(self, v: Dict[str, Any]) -> Dict[str, Any]:
        if js_truthy(v.get("done", UNDEFINED)):
            return {"key": v["key"], "done": True, "until": js_iso(v["due"] - 1)}
        out: Dict[str, Any] = {"key": v["key"], "got": clone(self._got(v)), "missing": self._missing(v)}
        if "list" in v:
            out["expected"] = v.get("expected", UNDEFINED)
        out["deadline"] = js_iso(v["due"] - 1)
        return out

    def _complete(self, v: Dict[str, Any]) -> bool:
        if self._op.parts:
            return all(v["parts"].get(p["name"], UNDEFINED) is not UNDEFINED for p in self._op.parts)
        return v["expected"] is not None and len(v["list"]) >= v["expected"]

    def _got(self, v: Dict[str, Any]) -> Any:
        return v["parts"] if self._op.parts else v["list"]

    def _missing(self, v: Dict[str, Any]) -> Any:
        if self._op.parts:
            return [p["name"] for p in self._op.parts if v["parts"].get(p["name"], UNDEFINED) is UNDEFINED]
        return None if v["expected"] is None else max(0, v["expected"] - len(v["list"]))

    def _prune(self) -> None:
        heap = self._heap
        state = self._state
        while heap.size:
            due, key = heap.peek()  # type: ignore[misc]
            v = state.get(key)
            if isinstance(v, dict) and v.get("due", UNDEFINED) == due:
                return
            heap.pop()


def _timeout_of(on_timeout: Any, got: Any, missing: Any, key: Any) -> Any:
    async def compute() -> Any:
        return await call_user(on_timeout, clone(got), clone(missing), key)

    return compute


def _complete_of(op: Any, got: Any, key: str) -> Any:
    async def compute() -> Any:
        if op.on_complete:
            return await call_user(op.on_complete, clone(got), key)
        return {"key": key, "parts": clone(got)}

    return compute
