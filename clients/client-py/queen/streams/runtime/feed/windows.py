"""
Windows on the tenant feed: tumbling, sliding, cron and session windows with
reduce/aggregate, per key, kept in the stream's FeedState. Port of the JS
SDK's runtime/feed/windows.js.

Without ``eventTime`` a message's time is when the log committed it, and every
queue shares that clock: a window closes once the clock is past its end (plus
``gracePeriod``), and nothing that belongs to it can come after. With
``eventTime`` (or after a step on event time) a message's time is the
function's answer and windows close on the step's watermark (clock.py).

A result goes on with its event time: the window's last millisecond.

State keys: ``w\\u001f<start>\\u001f<key>`` for a window of the fixed kinds,
``s\\u001f<key>`` for a key's open session. A value is
``{key, start, end, due, acc}`` (a session also keeps ``last``), where ``due``
is when it closes.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any, Dict, Iterator, List

from ...util.js import UNDEFINED, js_clone, js_is_finite, js_iso, js_sort_key, js_str
from .state import REBUILD_BATCH, DueHeap, FeedState, is_buffer_key

SEP = "\u001f"
_W = "w" + SEP
_S = "s" + SEP


def _final_acc(acc: Any, is_aggregate: bool) -> Any:
    """An aggregate's result, without the running sums it keeps for ``avg``."""
    if not is_aggregate or not isinstance(acc, (dict, list)):
        return js_clone(acc)
    if isinstance(acc, list):
        return {str(i): v for i, v in enumerate(acc)}
    return {k: v for k, v in acc.items() if not (isinstance(k, str) and k.startswith("__avg_"))}


def _classify(k: str, v: Any) -> Any:
    if is_buffer_key(k):
        return "held"
    if k.startswith(_W) or k.startswith(_S):
        return "open"
    return None


class WindowEngine:
    def __init__(self, *, window: Any, reducer: Any, state: FeedState) -> None:
        if reducer is None:
            raise ValueError("a window on a feed needs .reduce() or .aggregate() after it")
        self._op = window
        self._reducer = reducer
        self._state = state
        self._heap = DueHeap()
        self._is_aggregate = reducer.kind == "aggregate"
        self.kind = window.window_kind
        # Counted as they change (FeedState), so metrics cost the same at any size.
        state.classify_with(_classify)

    async def relevant(self, env: Dict[str, Any], t: Any) -> bool:
        """Every message concerns a window: a session on event time holds them all, in order."""
        return True

    def rebuild(self) -> None:
        """Index what the state holds, after a load."""
        self._heap.clear()
        for k, v in self._state.items_view():
            self._index(k, v)

    async def rebuild_async(self) -> None:
        """The same, a batch at a time, so a process loading millions of windows keeps its lock and answers."""
        self._heap.clear()
        n = 0
        for k, v in self._state.entries():
            self._index(k, v)
            n += 1
            if n % REBUILD_BATCH == 0:
                await asyncio.sleep(0)

    def _index(self, k: str, v: Any) -> None:
        if (k.startswith(_W) or k.startswith(_S)) and isinstance(v, dict) and js_is_finite(v.get("due")):
            self._heap.push(v["due"], k)

    def next_due(self) -> Any:
        """When the next window closes, on the clock this engine closes on; None if none is open."""
        self._prune()
        top = self._heap.peek()
        return top[0] if top is not None else None

    @property
    def open(self) -> int:
        return self._state.count("open")

    def advance(self, t: Any, emits: List[Dict[str, Any]], limit: Any = math.inf) -> None:
        """Close every window due at or before ``t``. Pure: it removes them from
        the state and appends one emit each, ``{key, value, ctx}``."""
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
            # The result's time is when the window closed, not when this
            # process saw it close: one time on every run and every replay.
            em = self._emit_of(v)
            em["time"] = v["due"]
            emits.append(em)

    async def process(self, env: Dict[str, Any], t: Any, emits: List[Dict[str, Any]]) -> str:
        """Fold one envelope in at time ``t`` (the step's clock). A session that
        the gap closed is appended to ``emits``."""
        key = js_str(env.get("key", UNDEFINED))
        value = env.get("value", UNDEFINED)
        if value is UNDEFINED:
            value = env.get("msg", UNDEFINED)
        if self.kind == "session":
            await self._session(key, t, value, emits)
        else:
            for start, end in self._windows_of(t):
                await self._fold(key, start, end, value)
        return "ok"

    # ---- internals ---------------------------------------------------------

    def _prune(self) -> None:
        # A heap item whose window was closed, or moved, since it was pushed.
        heap = self._heap
        state = self._state
        while heap.size:
            due, key = heap.peek()  # type: ignore[misc]
            v = state.get(key)
            if isinstance(v, dict) and v.get("due", UNDEFINED) == due:
                return
            heap.pop()

    def _windows_of(self, ts: Any) -> List[List[Any]]:
        op = self._op
        if self.kind == "tumbling":
            start = math.floor(ts / op.window_ms) * op.window_ms
            return [[start, start + op.window_ms]]
        if self.kind == "cron":
            start = op.floor_to_boundary_js(ts)
            return [[start, start + op.window_ms]]
        if self.kind == "sliding":
            latest = math.floor(ts / op.slide_ms) * op.slide_ms
            out = []
            i = 0
            while i < op.windows_per_event:
                start = latest - i * op.slide_ms
                if start <= ts < start + op.size_ms:
                    out.append([start, start + op.size_ms])
                i += 1
            return out
        raise ValueError(f"a {self.kind} window does not run on a feed")

    async def _fold(self, key: str, start: Any, end: Any, value: Any) -> None:
        sk = f"{_W}{js_str(start)}{SEP}{key}"
        cur = self._state.get(sk)
        acc = await self._reducer.feed_fold(
            js_clone(cur.get("acc", UNDEFINED)) if isinstance(cur, dict) else self._reducer.feed_initial(),
            value,
        )
        due = end + self._op.grace_period_ms
        self._state.set(sk, {"key": key, "start": start, "end": end, "due": due, "acc": acc})
        if cur is UNDEFINED:
            self._heap.push(due, sk)

    async def _session(self, key: str, ts: Any, value: Any, emits: List[Dict[str, Any]]) -> None:
        sk = f"{_S}{key}"
        cur = self._state.get(sk)
        gap = self._op.gap_ms
        if cur is not UNDEFINED and ts > cur["last"] + gap:
            # Past the gap: that session is over, and this message starts the next.
            self._state.delete(sk)
            emits.append(self._emit_of(cur))
            cur = UNDEFINED
        has = cur is not UNDEFINED
        acc = await self._reducer.feed_fold(
            js_clone(cur.get("acc", UNDEFINED)) if has else self._reducer.feed_initial(),
            value,
        )
        start = min(cur["start"], ts) if has else ts
        last = max(cur["last"], ts) if has else ts
        # A message exactly `gap` after the last one is still in the session (as
        # a pattern's `within` is in time): it closes the millisecond after.
        due = last + gap + 1 + self._op.grace_period_ms
        self._state.set(sk, {"key": key, "start": start, "last": last, "end": last + gap, "due": due, "acc": acc})
        if not has or cur["due"] != due:
            self._heap.push(due, sk)

    def inspect(self, key: Any) -> Any:
        """The open windows of ``key``, for a live read: ``{key, windows: [{start, end, closesAt, value}]}``, or None."""
        k = js_str(key)
        windows = []
        for _, v in self._state.items_view():
            if isinstance(v, dict) and v.get("key", UNDEFINED) == k:
                windows.append(self._view(v))
        if not windows:
            return None
        windows.sort(key=lambda w: js_sort_key(w["start"]))
        return {"key": k, "windows": windows}

    def list(self) -> Iterator[Dict[str, Any]]:
        """Every open window, for a live read."""
        for _, v in self._state.items_view():
            if isinstance(v, dict) and v.get("key", UNDEFINED) is not UNDEFINED:
                yield {"key": v["key"], "windows": [self._view(v)]}

    def _view(self, v: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "start": js_iso(v["start"]),
            "end": js_iso(v["end"]),
            "closesAt": js_iso(v["due"]),
            "value": _final_acc(v.get("acc", UNDEFINED), self._is_aggregate),
        }

    def _emit_of(self, v: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": v["key"],
            # On to a later step on event time: the last millisecond the window covers.
            "eventTime": v["end"] - 1,
            "value": _final_acc(v.get("acc", UNDEFINED), self._is_aggregate),
            "ctx": {
                "kind": self.kind,
                "key": v["key"],
                "partition": v["key"],
                "windowStart": v["start"],
                "windowEnd": v["end"],
                "windowKey": js_iso(v["start"]),
            },
            "state": v,
        }

