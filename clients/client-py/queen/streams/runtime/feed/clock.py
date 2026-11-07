"""
The clock of one stateful step of a feed stream: the feed's, or event time.
Port of the JS SDK's runtime/feed/clock.js.

The feed's clock (FeedClock) is the time the log committed each message.
Every queue shares it and it only moves forward, so a step on it closes a
window or passes a deadline once the clock is past it.

Event time (EventClock) is a time the messages carry: ``eventTime(msg)``, or,
after another step on event time, the time of that step's result. The step
keeps a WATERMARK: the latest event time seen, minus ``allowedLateness``. A
window closes, and a deadline passes, once the watermark is past it; a message
older than the watermark is late (dropped and counted, or taken anyway with
``onLate: 'include'``). Only the messages that concern a pattern, a join or a
gather count: the others need no time and move nothing.

In order: patterns, joins, gathers and sessions on event time hold each
message until the watermark reaches its time, then take them in event-time
order, ties in the order they came. Idle: once the step has had no message for
``idle`` on the feed's clock, its watermark moves on as the feed's clock does.
A step fed by a step on event time follows that step's FLOOR (the oldest time
that step can still send a result with).

State: the held messages are entries of the step's state, keys
``b\\u001f<n>``; the watermark and its anchors are the step's meta, carried in
the snapshot pointer.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any, Dict, List, Optional

from ...operators._window_common import extract_event_time_js
from ...util.js import UNDEFINED, js_get, js_is_finite, js_is_integer, js_same
from .patterns import message_of
from .state import BUFFER_PREFIX, REBUILD_BATCH, DueHeap, FeedState, is_buffer_key

NEG_INF = -math.inf


def buffer_key(n: int) -> str:
    return f"{BUFFER_PREFIX}{str(n).zfill(12)}"


def pack(env: Dict[str, Any]) -> Dict[str, Any]:
    """An envelope as a held entry: the message, and only what differs from it."""
    msg = env.get("msg", UNDEFINED)
    value = env.get("value", UNDEFINED)
    e: Dict[str, Any] = {"m": msg, "k": env.get("key", UNDEFINED)}
    if js_same(value, msg) or (value is UNDEFINED and msg is UNDEFINED):
        e["vm"] = 1
    elif isinstance(msg, (dict, list)) and (js_same(value, js_get(msg, "data")) or (value is UNDEFINED and js_get(msg, "data") is UNDEFINED)):
        e["vd"] = 1
    else:
        e["v"] = value
    if env.get("derived"):
        e["d"] = 1
    ctx = env.get("ctx", UNDEFINED)
    if ctx is not UNDEFINED:
        e["c"] = ctx
    pos = env.get("position", UNDEFINED)
    if pos is not UNDEFINED and pos is not None:
        e["p"] = pos
    et = env.get("eventTime", UNDEFINED)
    if js_is_finite(et):
        e["et"] = et
    return e


def unpack(e: Dict[str, Any]) -> Dict[str, Any]:
    m = e.get("m", UNDEFINED)
    env: Dict[str, Any] = {"msg": m, "key": e.get("k", UNDEFINED)}
    if e.get("vm"):
        env["value"] = m
    elif e.get("vd"):
        env["value"] = js_get(m, "data") if isinstance(m, (dict, list)) else UNDEFINED
    else:
        env["value"] = e.get("v", UNDEFINED)
    if e.get("d"):
        env["derived"] = True
    if e.get("c", UNDEFINED) is not UNDEFINED:
        env["ctx"] = e["c"]
    if e.get("p", UNDEFINED) is not UNDEFINED:
        env["position"] = e["p"]
    if e.get("et", UNDEFINED) is not UNDEFINED:
        env["eventTime"] = e["et"]
    return env


class FeedClock:
    """The feed's clock: a step's time is the feed time it is given."""

    kind = "feed"

    def __init__(self, *, engine: Any) -> None:
        self.engine = engine

    @property
    def needs_feed_clock(self) -> bool:
        """It closes on the feed's clock: the runner moves that clock on a quiet feed."""
        return True

    async def accept(self, env: Dict[str, Any], t: Any, emits: List[Dict[str, Any]]) -> str:
        await self.engine.process(env, t, emits)
        return "ok"

    def next_at(self) -> Any:
        """The feed time of its next close, or None."""
        return self.engine.next_due()

    def take(self, emits: List[Dict[str, Any]], when: Any, limit: Any = math.inf) -> Dict[str, Any]:
        """Close what is due next: its emits are appended."""
        due = self.engine.next_due()
        self.engine.advance(due, emits, limit)
        return {"at": due}

    def watermark_at(self, t: Any) -> Any:
        return None

    def checkpoint(self) -> Any:
        return None

    def restore(self, cp: Any) -> None:
        pass

    def meta(self) -> Dict[str, Any]:
        return {}

    def load_meta(self, meta: Any) -> None:
        pass

    def rebuild(self) -> None:
        pass

    async def rebuild_async(self) -> None:
        pass

    @property
    def held(self) -> int:
        return 0


class EventClock:
    kind = "event"

    def __init__(
        self,
        *,
        engine: Any,
        state: FeedState,
        fn: Any,
        shape: str = "feed",
        lateness_ms: Any = 0,
        idle_ms: Any = None,
        on_late: str = "drop",
        ordered: bool = False,
        upstream: Optional["EventClock"] = None,
        grace_ms: Any = 0,
    ) -> None:
        if not js_is_finite(lateness_ms) or lateness_ms < 0:
            raise ValueError(f"allowedLateness must be a duration of zero or more, got {lateness_ms}")
        if idle_ms is not None and (not js_is_finite(idle_ms) or idle_ms <= 0):
            raise ValueError(f"idle must be a duration above zero, got {idle_ms}")
        if upstream is not None and idle_ms is not None:
            raise ValueError("a step fed by a step on event time follows its watermark: idle goes on the first step on event time")
        self._engine = engine
        self._state = state
        self._fn = fn if callable(fn) else None
        self._shape = shape
        self._lateness_ms = lateness_ms
        self._idle_ms = idle_ms
        self._on_late = on_late
        self._ordered = ordered
        self._upstream = upstream
        self._grace_ms = grace_ms if js_is_finite(grace_ms) and grace_ms > 0 else 0
        self._heap = DueHeap()
        # From the messages (a fed step: from the results it got): the latest
        # event time seen minus allowedLateness, as of the feed time t_last; None: none yet.
        self.w_last: Any = NEG_INF
        self.t_last: Any = None
        # The feed time of the step's last message or close: nothing it does later happens before it.
        self.since: Any = None
        # The number of the next held message.
        self.seq = 0

    @property
    def needs_feed_clock(self) -> bool:
        """The feed's clock moves it: with ``idle``, or when the step before it moves with that clock."""
        return self._idle_ms is not None or (self._upstream is not None and self._upstream.needs_feed_clock)

    def watermark_at(self, t: Any) -> Any:
        """The watermark at feed time ``t``."""
        if self._upstream is not None:
            f = self._upstream.floor_at(t)
            return max(self.w_last, f - self._lateness_ms) if js_is_finite(f) else self.w_last
        if self.t_last is None or self._idle_ms is None or not js_is_finite(self.w_last):
            return self.w_last
        quiet = t - self.t_last - self._idle_ms
        return self.w_last + quiet if quiet > 0 else self.w_last

    def ready_at(self, x: Any) -> Any:
        """The feed time at which the watermark reaches ``x``; None when only a message, or the step before, can take it there."""
        at = None
        if self.t_last is not None and x <= self.w_last:
            at = self.t_last
        elif self._upstream is not None:
            at = self._upstream.floor_ready_at(x + self._lateness_ms)
        elif self.t_last is not None and self._idle_ms is not None and js_is_finite(self.w_last):
            at = self.t_last + self._idle_ms + (x - self.w_last)
        if at is None:
            return None
        return self.since if self.since is not None and self.since > at else at

    def floor_at(self, t: Any) -> Any:
        """The oldest event time this step can still send a result with, at feed time ``t``."""
        f = self.watermark_at(t)
        self._prune()
        top = self._heap.peek()
        if top is not None:
            f = min(f, top[0])
        d = self._engine.next_due()
        if d is not None:
            f = min(f, d - self._grace_ms - 2)
        return f

    def floor_ready_at(self, x: Any) -> Any:
        """The feed time at which the floor reaches ``x``; None while a held message or an open close could still send something older."""
        self._prune()
        top = self._heap.peek()
        if top is not None and top[0] < x:
            return None
        d = self._engine.next_due()
        if d is not None and d - self._grace_ms - 2 < x:
            return None
        return self.ready_at(x)

    async def _time_of(self, env: Dict[str, Any], t: Any) -> Any:
        if self._fn is not None:
            ts = await extract_event_time_js(message_of(env, t) if self._shape == "feed" else env.get("msg", UNDEFINED), self._fn)
            if ts is None:
                raise ValueError("eventTime gave no time for this message (a Date, epoch milliseconds or an ISO string)")
        else:
            et = env.get("eventTime", UNDEFINED)
            ts = et if js_is_finite(et) else None
            if ts is None:
                raise ValueError("a result reached a step on event time without an event time")
        return ts

    async def accept(self, env: Dict[str, Any], t: Any, emits: List[Dict[str, Any]]) -> str:
        """One envelope at feed time ``t``: ignored, late ('late': dropped, or taken
        at once with onLate 'include'), held until the watermark reaches it (in
        order), or taken now. What the engine sends is appended to ``emits``."""
        if self._ordered:
            et = env.get("eventTime", UNDEFINED)
            if not await self._engine.relevant(env, et if js_is_finite(et) else t):
                return "ok"
        ts = await self._time_of(env, t)
        # A fed step judges by the results it got: its watermark also follows
        # the step before, whose floor moves while that step's results are on their way.
        w = self.w_last if self._upstream is not None else self.watermark_at(t)
        self.w_last = max(w, ts - self._lateness_ms)
        self.t_last = t
        if self.since is None or t > self.since:
            self.since = t
        if ts < w:
            if self._on_late == "include":
                await self._engine.process(env, ts, emits)
            return "late"
        if not self._ordered:
            await self._engine.process(env, ts, emits)
            return "ok"
        key = buffer_key(self.seq)
        self.seq += 1
        self._state.set(key, {"t": ts, "e": pack(env)})
        self._heap.push(ts, key)
        return "ok"

    def _prune(self) -> None:
        # A held message a rollback took back out of the state, or put back
        # under the same key with another time (seq rolls back with the state).
        heap = self._heap
        state = self._state
        while heap.size:
            due, key = heap.peek()  # type: ignore[misc]
            v = state.get(key)
            if isinstance(v, dict) and v.get("t", UNDEFINED) == due:
                return
            heap.pop()

    def _next(self) -> Any:
        """The next thing due in event time: a deadline (first, at a tie) or a held message."""
        self._prune()
        due = self._engine.next_due()
        held = self._heap.peek()
        if held is not None and (due is None or held[0] < due):
            return {"at": held[0], "key": held[1]}
        return None if due is None else {"at": due, "key": None}

    def next_at(self) -> Any:
        """The feed time of the next close or release, or None."""
        n = self._next()
        return self.ready_at(n["at"]) if n else None

    def take(self, emits: List[Dict[str, Any]], when: Any, limit: Any = math.inf) -> Dict[str, Any]:
        """Take what is due next, at feed time ``when``: close the engine's windows
        or deadlines of that time (appended to ``emits``), or take the next held
        message out, for the caller to run through the engine:
        ``{at, release: {key, ts, env}}``."""
        n = self._next()
        if js_is_finite(when) and (self.since is None or when > self.since):
            self.since = when
        if not n["key"]:
            self._engine.advance(n["at"], emits, limit)
            return {"at": n["at"]}
        self._heap.pop()
        v = self._state.get(n["key"])
        self._state.delete(n["key"])
        return {"at": n["at"], "release": {"key": n["key"], "ts": v["t"], "env": unpack(v["e"])}}

    @property
    def held(self) -> int:
        """The messages held until the watermark reaches them."""
        return self._state.count("held")

    def checkpoint(self) -> Dict[str, Any]:
        return {"w": self.w_last, "t": self.t_last, "s": self.since, "n": self.seq}

    def restore(self, cp: Dict[str, Any]) -> None:
        self.w_last = cp["w"]
        self.t_last = cp["t"]
        self.since = cp["s"]
        self.seq = cp["n"]

    def meta(self) -> Dict[str, Any]:
        """The watermark and its anchors, for the snapshot pointer (JSON: no infinities)."""
        m: Dict[str, Any] = {"n": self.seq}
        if js_is_finite(self.w_last):
            m["w"] = self.w_last
        if self.t_last is not None:
            m["t"] = self.t_last
        if self.since is not None:
            m["s"] = self.since
        return m

    def load_meta(self, meta: Any) -> None:
        m = meta if isinstance(meta, dict) else {}
        # `watermark`: a record from before the anchors (the windows' own watermark).
        w = m.get("w", UNDEFINED)
        wm = m.get("watermark", UNDEFINED)
        self.w_last = w if js_is_finite(w) else wm if js_is_finite(wm) else NEG_INF
        t = m.get("t", UNDEFINED)
        self.t_last = t if js_is_finite(t) else None
        s = m.get("s", UNDEFINED)
        self.since = s if js_is_finite(s) else self.t_last
        n = m.get("n", UNDEFINED)
        self.seq = int(n) if js_is_integer(n) and n >= 0 else 0

    def rebuild(self) -> None:
        """Index the held messages of the state, after a load; numbers go on after the highest one held."""
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
        if not is_buffer_key(k) or not isinstance(v, dict) or not js_is_finite(v.get("t", UNDEFINED)):
            return
        self._heap.push(v["t"], k)
        rest = k[len(BUFFER_PREFIX):]
        if rest.isdigit() and rest.isascii():
            n = int(rest)
            if n >= self.seq:
                self.seq = n + 1
