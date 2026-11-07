"""
``.pattern()`` and ``.join()`` (two sides) on the tenant feed: two kinds of
message for one entity, in time, on the clock every queue shares (when the log
committed each message), or on event time (clock.py hands the engine its
messages in event-time order, each with its event time). Port of the JS SDK's
runtime/feed/patterns.js.

State keys: ``p\\u001f<key>`` for the ``start`` a pattern waits on, and
``j\\u001fL\\u001f<key>`` / ``j\\u001fR\\u001f<key>`` for the side a join waits on.
A value is ``{key, msg, side?, due}``: the waiting message (or what ``keep``
kept of it), and ``due``, the first millisecond past its deadline. A message
exactly ``within`` after the one waiting is still in time.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any, Dict, Iterator, List, Optional

from ...util.js import UNDEFINED, call_user, js_clone, js_get, js_is_finite, js_iso, js_str, js_truthy, nullish
from .state import REBUILD_BATCH, DueHeap, FeedState, is_buffer_key

SEP = "\u001f"
clone = js_clone


def message_of(env: Dict[str, Any], t: Any) -> Any:
    """The message a pattern, join or gather sees for an envelope: the feed
    message itself, or, after an earlier stateful step, that step's result in
    the feed's shape (``data`` is the result, ``queue`` is None, ``partition``
    the key, ``ctx`` what the step said about it)."""
    if not env.get("derived"):
        return env.get("msg", UNDEFINED)
    pos = env.get("position", UNDEFINED)
    ctx = env.get("ctx", UNDEFINED)
    return {
        "position": None if nullish(pos) else pos,
        "queue": None,
        "partition": js_str(env.get("key", UNDEFINED)),
        "key": env.get("key", UNDEFINED),
        "data": env.get("value", UNDEFINED),
        "ctx": None if nullish(ctx) else ctx,
        "ts": js_iso(t) if js_is_finite(t) else None,
        "derived": True,
    }


def snapshot_of(msg: Any, kept: Any = UNDEFINED) -> Dict[str, Any]:
    """The parts of a message a waiting state keeps; ``data`` is ``keep(msg, value)`` when the step has one."""
    created = js_get(msg, "createdAt")
    snap: Dict[str, Any] = {
        "position": js_get(msg, "position"),
        "queue": js_get(msg, "queue"),
        "partition": js_get(msg, "partition"),
        "partitionId": js_get(msg, "partitionId"),
        "offset": js_get(msg, "offset"),
        "id": js_get(msg, "id"),
        "transactionId": js_get(msg, "transactionId"),
        "ts": js_get(msg, "ts"),
        "createdAt": js_get(msg, "ts") if nullish(created) else created,
        "data": js_get(msg, "data") if kept is UNDEFINED else kept["value"],
    }
    if js_truthy(js_get(msg, "derived")):
        snap["derived"] = True
        snap["key"] = js_get(msg, "key")
        ctx = js_get(msg, "ctx")
        if js_truthy(ctx):
            snap["ctx"] = ctx
    return snap


async def kept_of(op: Any, msg: Any, value: Any) -> Any:
    """What a step keeps of a message: ``{"value": ...}`` from its ``keep``, or UNDEFINED for all of it."""
    if not op.keep_fn:
        return UNDEFINED
    return {"value": await call_user(op.keep_fn, msg, value)}


def _classify(k: str, v: Any) -> Any:
    return "held" if is_buffer_key(k) else "open"


class PatternEngine:
    def __init__(self, *, op: Any, state: FeedState) -> None:
        self._op = op
        self._state = state
        self._heap = DueHeap()
        self.kind = op.pattern_kind
        state.classify_with(_classify)

    async def relevant(self, env: Dict[str, Any], t: Any) -> bool:
        """Whether a message concerns the step (a start or then, a side): on event time only these are held."""
        msg = message_of(env, t)
        op = self._op
        if op.pattern_kind == "pattern":
            return js_truthy(await call_user(op.then, msg)) or js_truthy(await call_user(op.start, msg))
        return js_truthy(await call_user(op.left, msg)) or js_truthy(await call_user(op.right, msg))

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
        """Pass every deadline at or before ``t``: one timeout emit each when ``onTimeout`` is set."""
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
            if not self._op.on_timeout:
                continue
            raw_side = v.get("side", UNDEFINED)
            side = "right" if raw_side == "R" else "left" if raw_side == "L" else UNDEFINED
            ctx: Dict[str, Any] = {"kind": "timeout", "key": v["key"], "partition": v["key"]}
            if side is not UNDEFINED:
                ctx["side"] = side
            ctx["deadline"] = v["due"] - 1
            ctx["waitedFor"] = v.get("msg", UNDEFINED)
            emits.append({
                "key": v["key"],
                "ctx": ctx,
                "state": v,
                "time": v["due"],
                "eventTime": v["due"] - 1,
                "compute": self._timeout_of(v, side),
            })

    def _timeout_of(self, v: Dict[str, Any], side: Any) -> Any:
        on_timeout = self._op.on_timeout

        async def compute() -> Any:
            if side is not UNDEFINED:
                return await call_user(on_timeout, clone(v.get("msg", UNDEFINED)), side)
            return await call_user(on_timeout, clone(v.get("msg", UNDEFINED)))

        return compute

    async def process(self, env: Dict[str, Any], t: Any, emits: List[Dict[str, Any]]) -> str:
        """One envelope at time ``t``; a match is appended to ``emits``."""
        msg = message_of(env, t)
        op = self._op
        value = env.get("value", UNDEFINED)

        # The entity: key(msg, value), so a .flatMap() before can key each item
        # of a message on its own id. Asked only of a message that starts or
        # ends a wait: the others need not have the field.
        async def key_of() -> str:
            if op.key_fn:
                return js_str(await call_user(op.key_fn, msg, value))
            return js_str(env.get("key", UNDEFINED))

        if op.pattern_kind == "pattern":
            is_then = js_truthy(await call_user(op.then, msg))
            is_start = js_truthy(await call_user(op.start, msg))
            if not is_then and not is_start:
                return "ok"
            key = await key_of()
            sk = f"p{SEP}{key}"
            waiting = self._state.get(sk)
            if is_then and waiting is not UNDEFINED:
                self._state.delete(sk)
                snap = snapshot_of(msg, await kept_of(op, msg, value))
                emits.append({
                    "key": key,
                    "ctx": {"kind": "match", "key": key, "partition": key, "deadline": waiting["due"] - 1},
                    "state": waiting,
                    "eventTime": t,
                    "compute": _match_of(op, key, waiting, snap),
                })
                if is_start:
                    await self._wait(sk, key, msg, value, t)
            elif is_start and (waiting is UNDEFINED or op.on_repeat == "restart"):
                await self._wait(sk, key, msg, value, t)
            return "ok"
        # join: a message that is both left and right counts as left.
        side: Optional[str] = None
        if js_truthy(await call_user(op.left, msg)):
            side = "L"
        elif js_truthy(await call_user(op.right, msg)):
            side = "R"
        if side is None:
            return "ok"
        key = await key_of()
        other = "R" if side == "L" else "L"
        other_key = f"j{SEP}{other}{SEP}{key}"
        waiting = self._state.get(other_key)
        if waiting is not UNDEFINED:
            self._state.delete(other_key)
            snap = snapshot_of(msg, await kept_of(op, msg, value))
            left = snap if side == "L" else waiting.get("msg", UNDEFINED)
            right = snap if side == "R" else waiting.get("msg", UNDEFINED)
            emits.append({
                "key": key,
                "ctx": {"kind": "match", "key": key, "partition": key, "deadline": waiting["due"] - 1},
                "state": waiting,
                "eventTime": t,
                "compute": _select_of(op, key, left, right),
            })
            return "ok"
        my_key = f"j{SEP}{side}{SEP}{key}"
        if self._state.get(my_key) is UNDEFINED or op.on_repeat == "restart":
            await self._wait(my_key, key, msg, value, t, side)
        return "ok"

    def inspect(self, key: Any) -> Any:
        """What waits for ``key``, for a live read: ``{key, waiting: [{side?, msg, deadline}]}``, or None."""
        k = js_str(key)
        out = []
        keys = [f"p{SEP}{k}"] if self.kind == "pattern" else [f"j{SEP}L{SEP}{k}", f"j{SEP}R{SEP}{k}"]
        for sk in keys:
            v = self._state.get(sk)
            if v is not UNDEFINED:
                out.append(self._view(v))
        return {"key": k, "waiting": out} if out else None

    def list(self) -> Iterator[Dict[str, Any]]:
        """Every waiting entity, for a live read."""
        for k, v in self._state.items_view():
            if not is_buffer_key(k):
                yield {"key": v.get("key", UNDEFINED), "waiting": [self._view(v)]}

    def _view(self, v: Dict[str, Any]) -> Dict[str, Any]:
        w: Dict[str, Any] = {"msg": clone(v.get("msg", UNDEFINED)), "deadline": js_iso(v["due"] - 1)}
        if js_truthy(v.get("side", UNDEFINED)):
            w["side"] = "left" if v["side"] == "L" else "right"
        return w

    async def _wait(self, sk: str, key: str, msg: Any, value: Any, t: Any, side: Optional[str] = None) -> None:
        due = t + self._op.within_ms + 1
        snap = snapshot_of(msg, await kept_of(self._op, msg, value))
        self._state.set(sk, {"key": key, "side": side, "msg": snap, "due": due} if side else {"key": key, "msg": snap, "due": due})
        self._heap.push(due, sk)

    def _prune(self) -> None:
        heap = self._heap
        state = self._state
        while heap.size:
            due, key = heap.peek()  # type: ignore[misc]
            v = state.get(key)
            if isinstance(v, dict) and v.get("due", UNDEFINED) == due:
                return
            heap.pop()


def _match_of(op: Any, key: str, waiting: Dict[str, Any], snap: Dict[str, Any]) -> Any:
    async def compute() -> Any:
        if op.on_match:
            return await call_user(op.on_match, clone(waiting.get("msg", UNDEFINED)), clone(snap))
        return {"key": key, "start": clone(waiting.get("msg", UNDEFINED)), "then": snap}

    return compute


def _select_of(op: Any, key: str, left: Any, right: Any) -> Any:
    async def compute() -> Any:
        if op.select:
            return await call_user(op.select, clone(left), clone(right))
        return {"key": key, "left": clone(left), "right": clone(right)}

    return compute
