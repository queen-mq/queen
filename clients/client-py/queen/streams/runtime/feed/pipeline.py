"""
The steps of a feed stream, run in chain order: stateless operators, and any
number of stateful steps (a window with reduce/aggregate, a pattern, a join, a
gather, a table join), each with its own state. Port of the JS SDK's
runtime/feed/pipeline.js.

A feed message enters as ``{msg, key: partition, value: data}``. A stateful
step that sends a result hands it on as an envelope of its own -- ``{msg:
result, value: result, key, ctx, eventTime}``, marked derived -- which the
operators and steps after it see as they see a window's result: map/filter/
key_by get the result, a pattern/join/gather gets it in the feed's shape (see
patterns.message_of). The steps after the last one feed the sink.

``advance(t)`` closes what is due by feed time ``t``, across the steps, in the
order it fell due on the feed's clock -- an event-time step's close falls due
when its watermark reaches it -- the later step first at one time, so a result
that reaches a step finds every wait of that step due by then already closed:
one order every run, on every replay.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any, Awaitable, Callable, Dict, List, Optional

from ...util.js import UNDEFINED, call_user, js_get, js_is_finite, js_iso, js_json_dumps, js_str, js_truthy, nullish
from .clock import EventClock, FeedClock
from .collect import CollectEngine
from .patterns import PatternEngine
from .state import CLOSE_BATCH, FeedState
from .tables import TableStep
from .windows import WindowEngine


def _typeof(v: Any) -> str:
    if v is UNDEFINED:
        return "undefined"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    if callable(v):
        return "function"
    return "object"


async def apply_op(op: Any, env: Dict[str, Any]) -> List[Dict[str, Any]]:
    """A stateless operator on one envelope, as the JS operators apply it."""
    kind = op.kind
    msg = env.get("msg", UNDEFINED)
    ctx = env.get("ctx", UNDEFINED)
    if kind == "map":
        value = await call_user(op.fn, msg, ctx)
        out = dict(env)
        out["value"] = value
        return [out]
    if kind == "filter":
        keep = await call_user(op.fn, msg, ctx)
        return [env] if js_truthy(keep) else []
    if kind == "flatMap":
        items = await call_user(op.fn, msg, ctx)
        if not isinstance(items, (list, tuple)):
            raise TypeError(f"flatMap fn must return an array (got {_typeof(items)})")
        res = []
        for v in items:
            e = dict(env)
            e["value"] = v
            res.append(e)
        return res
    if kind == "keyBy":
        key = js_str(await call_user(op.fn, msg))
        out = dict(env)
        out["key"] = key
        return [out]
    raise ValueError(f"unsupported operator: {kind}")


async def apply_all(op: Any, envelopes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for env in envelopes:
        out.extend(await apply_op(op, env))
    return out


def _step_label(s: Dict[str, Any]) -> str:
    if s["kind"] == "window":
        return f"the {s['window'].window_kind} window"
    if s["kind"] == "pattern":
        return f"the {s['op'].pattern_kind}"
    if s["kind"] == "collect":
        return f"the {s['op'].collect_kind}"
    return f"the {s['kind']}"


def clock_of(s: Dict[str, Any], engine: Any, state: FeedState, before: Optional[EventClock], fed: bool) -> Any:
    """The clock of a step: event time when it says ``eventTime``, or when the
    stateful step before it runs on event time (unless it says ``eventTime:
    false``); else the feed's."""
    op = s["window"] if s["kind"] == "window" else s["op"]
    what = f"{_step_label(s)} {js_json_dumps(s['name'])}"
    # Patterns, joins, gathers and sessions depend on the order of their messages.
    ordered = s["kind"] != "window" or s["window"].window_kind == "session"
    on_late = getattr(op, "on_late_policy", None) or "drop"
    lateness_ms = getattr(op, "allowed_lateness_ms", None)
    lateness_ms = 0 if lateness_ms is None else lateness_ms
    idle_ms = getattr(op, "idle_ms", None)
    grace_ms = 0
    if s["kind"] == "window":
        g = getattr(s["window"], "grace_period_ms", None)
        grace_ms = 0 if g is None else g
    if callable(getattr(op, "event_time_fn", None)):
        if fed and idle_ms is not None:
            raise ValueError(
                f"Stream.fromFeed: idle on {what}, whose messages are the results of the step before it: moving on with the feed's "
                "clock it could pass a result that step has not sent yet. Leave out its eventTime (it then takes the times of those "
                "results and follows that step's watermark), and put idle on the first step on event time"
            )
        return EventClock(
            engine=engine, state=state, fn=op.event_time_fn, shape="value" if s["kind"] == "window" else "feed",
            lateness_ms=lateness_ms, idle_ms=idle_ms, on_late=on_late, ordered=ordered, grace_ms=grace_ms,
        )
    if before is not None and not getattr(op, "event_time_off", False):
        if idle_ms is not None:
            raise ValueError(
                f"Stream.fromFeed: {what} runs on the event time of the step before it and follows its watermark: "
                "put idle on the first step on event time"
            )
        return EventClock(
            engine=engine, state=state, fn=None, lateness_ms=lateness_ms, on_late=on_late, ordered=ordered,
            upstream=before, grace_ms=grace_ms,
        )
    if idle_ms is not None:
        raise ValueError(
            f"Stream.fromFeed: idle moves a watermark on: {what} runs on the feed's clock, give it eventTime "
            "(or put it after a step on event time)"
        )
    # A window as a pattern: both default to no lateness and 'drop', so anything else was asked for.
    if lateness_ms > 0 or on_late == "include":
        raise ValueError(
            f"Stream.fromFeed: allowedLateness and onLate are for event time: {what} runs on the feed's clock, give it "
            "eventTime (or put it after a step on event time)"
        )
    return FeedClock(engine=engine)


class Step:
    __slots__ = ("kind", "name", "state", "engine", "table", "clock", "committed", "ops")

    def __init__(self, kind: str, name: Optional[str] = None, ops: Optional[list] = None) -> None:
        self.kind = kind
        self.name = name
        self.state: Optional[FeedState] = None
        self.engine: Any = None
        self.table: Optional[TableStep] = None
        self.clock: Any = None
        self.committed: Any = None
        self.ops = ops


Unit = Callable[[Dict[str, Any], Callable[[Any], Awaitable[None]]], Awaitable[str]]


class Pipeline:
    def __init__(self, plan: List[Dict[str, Any]]) -> None:
        self.steps: List[Step] = []
        self.late = 0
        # The event-time clock of the last step with an engine: a step after it runs on event time too.
        before: Optional[EventClock] = None
        fed = False
        for s in plan:
            if s["kind"] == "ops":
                self.steps.append(Step("ops", ops=s["ops"]))
                continue
            state = FeedState()
            step = Step(s["kind"], s["name"])
            step.state = state
            if s["kind"] == "window":
                step.engine = WindowEngine(window=s["window"], reducer=s["reducer"], state=state)
            elif s["kind"] == "pattern":
                step.engine = PatternEngine(op=s["op"], state=state)
            elif s["kind"] == "collect":
                step.engine = CollectEngine(op=s["op"], state=state)
            elif s["kind"] == "table":
                step.table = TableStep(op=s["op"], state=state)
            else:
                raise ValueError(f"feed pipeline: unknown step {s['kind']}")
            if step.engine is not None:
                step.clock = clock_of(s, step.engine, state, before, fed)
                before = step.clock if step.clock.kind == "event" else None
                fed = True
            self.steps.append(step)
        self._stateful = [s for s in self.steps if s.state is not None]
        self.commit_point()

    @property
    def stateful(self) -> List[Step]:
        """The steps that keep state, in chain order."""
        return self._stateful

    @property
    def has_state(self) -> bool:
        return len(self._stateful) > 0

    @property
    def tables(self) -> List[TableStep]:
        return [s.table for s in self.steps if s.table is not None]

    def step(self, name: Any) -> Optional[Step]:
        """The step named ``name`` (or ``step<n>``)."""
        for s in self._stateful:
            if s.name == name:
                return s
        return None

    # ---- running ------------------------------------------------------------

    async def apply_row(self, entry: Dict[str, Any]) -> None:
        """A table queue's message: every table of that queue takes the row."""
        q = js_get(entry, "queue")
        for s in self.steps:
            if s.table is not None and q in s.table.queues:
                await s.table.apply_row(entry)

    async def entry(self, entry: Dict[str, Any], t: Any, out: Callable[..., Awaitable[None]]) -> None:
        """One feed message at feed time ``t``; what reaches the end goes to ``out(env, entry)``."""
        env = {"msg": entry, "key": js_get(entry, "partition"), "value": js_get(entry, "data")}
        await self._flow(0, [env], t, entry, out, js_get(entry, "position"))

    async def advance(self, t: Any, unit: Unit, *, max_units: Any = math.inf, on_batch: Any = None) -> str:
        """
        Close what is due by feed time ``t``, in the order it fell due, across
        the steps: the earliest first, and at one time the later step first.
        Each result runs on through the steps after it as one unit:
        ``unit(emit, run)``, where the runner calls ``run(out)`` (again, to
        retry) and judges it; a held message is a unit too.

        ``max_units``: stop after so many units (a replay stops where the stored
        record says the stream got to); ``on_batch(done)``: called after each
        batch, where the runner may commit in the middle of a long close
        ('abandon' stops here: the runner loaded its state again).
        """
        done = 0
        steps = self.steps
        while True:
            if done >= max_units:
                return "ok"
            at = -1
            when: Any = math.inf
            for i in range(len(steps) - 1, -1, -1):
                c = steps[i].clock
                if c is None:
                    continue
                w = c.next_at()
                if w is not None and w <= t and w < when:
                    at = i
                    when = w
            if at < 0:
                return "ok"
            s = steps[at]
            emits: List[Dict[str, Any]] = []
            # At most CLOSE_BATCH at once: millions of windows that end at one
            # time close in batches, in the same order, with the event loop
            # turning between them. Never more than may run.
            took = s.clock.take(emits, when, min(CLOSE_BATCH, max_units - done))
            if "release" in took:
                if (await self._release(at, took["release"], when, unit)) == "stop":
                    return "stop"
                done += 1
                if on_batch is not None and (await on_batch(done)) == "abandon":
                    return "abandon"
                continue
            for em in emits:
                em["step"] = at
                # A result happens when its window or deadline closed, whenever
                # this process noticed: the steps after see the same time on every run.
                r = await unit(em, self._emit_run(em, at, when))
                if r == "stop":
                    return "stop"
                done += 1
            if on_batch is not None and emits and (await on_batch(done)) == "abandon":
                return "abandon"
            if len(emits) >= CLOSE_BATCH:
                await asyncio.sleep(0)

    def _emit_run(self, em: Dict[str, Any], at: int, when: Any) -> Callable[[Any], Awaitable[None]]:
        async def run(out: Any) -> None:
            envs = await self._derive([em], None)
            await self._flow(at + 1, envs, when, None, out, None)

        return run

    async def _release(self, at: int, rel: Dict[str, Any], when: Any, unit: Unit) -> str:
        """A message an event-time step held, now in its turn: through the step, and its results on."""
        s = self.steps[at]
        env = rel["env"]
        derived = bool(env.get("derived"))
        ctx = env.get("ctx", UNDEFINED)
        ctx_kind = js_get(ctx, "kind") if isinstance(ctx, dict) else UNDEFINED
        em = {
            "step": at,
            "key": env.get("key", UNDEFINED),
            "time": rel["ts"],
            "id": rel["key"],
            # A feed message: reported and dead-lettered as the message it is; a result, as the result it is.
            "ctx": {
                "kind": ("result" if nullish(ctx_kind) else ctx_kind) if derived else "message",
                "key": env.get("key", UNDEFINED),
                "partition": env.get("key", UNDEFINED),
                "eventTime": rel["ts"],
            },
            "entry": None if derived else env.get("msg", UNDEFINED),
            "value": env.get("value", UNDEFINED) if derived else UNDEFINED,
            "state": {"msg": env.get("msg", UNDEFINED)},
            "release": True,
        }

        async def run(out: Any) -> None:
            emits: List[Dict[str, Any]] = []
            await s.engine.process(env, rel["ts"], emits)
            position = env.get("position", UNDEFINED)
            if nullish(position):
                if derived:
                    position = None
                else:
                    p = js_get(env.get("msg", UNDEFINED), "position")
                    position = None if nullish(p) else p
            envs = await self._derive(emits, position)
            await self._flow(at + 1, envs, when, None, out, position)

        return await unit(em, run)

    def next_due(self) -> Any:
        """When the next window, deadline or held message is due on the feed's clock; None if none waits on it."""
        best = None
        for s in self.steps:
            if s.clock is None or not s.clock.needs_feed_clock:
                continue
            d = s.clock.next_at()
            if d is not None and (best is None or d < best):
                best = d
        return best

    @property
    def needs_feed_clock(self) -> bool:
        """Whether a step closes as the feed's clock moves (the runner then moves it on a quiet feed)."""
        return any(s.clock is not None and s.clock.needs_feed_clock for s in self.steps)

    def watermark_at(self, t: Any) -> Any:
        """The lowest event-time watermark at feed time ``t``, for the metrics; None without event time."""
        w = None
        for s in self.steps:
            if s.clock is None or s.clock.kind != "event":
                continue
            x = s.clock.watermark_at(t)
            if js_is_finite(x):
                w = x if w is None else min(w, x)
        return w

    async def _flow(self, start: int, envs: List[Dict[str, Any]], t: Any, entry: Any, out: Any, position: Any) -> None:
        """Run ``envs`` through the steps from ``start``. ``t`` is the feed time;
        ``entry`` the feed message they came from (None for results of a close);
        ``position`` the position the results of a step carry."""
        cur = envs
        i = start
        steps = self.steps
        while i < len(steps) and cur:
            s = steps[i]
            i += 1
            if s.kind == "ops":
                for op in s.ops or []:
                    cur = await apply_all(op, cur)
                continue
            if s.table is not None:
                nxt: List[Dict[str, Any]] = []
                for env in cur:
                    nxt.extend(await s.table.apply(env, t))
                cur = nxt
                continue
            emits: List[Dict[str, Any]] = []
            for env in cur:
                if (await s.clock.accept(env, t, emits)) == "late":
                    self.late += 1
            cur = await self._derive(emits, position)
        for env in cur:
            await out(env, entry)

    async def _derive(self, emits: List[Dict[str, Any]], position: Any) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for em in emits:
            compute = em.get("compute")
            value = await compute() if compute is not None else em.get("value", UNDEFINED)
            if compute is not None and nullish(value):
                continue
            env = {
                "msg": value,
                "value": value,
                "ctx": em.get("ctx", UNDEFINED),
                "key": em.get("key", UNDEFINED),
                "derived": True,
                "position": None if nullish(position) else position,
            }
            et = em.get("eventTime", UNDEFINED)
            if js_is_finite(et):
                env["eventTime"] = et
            out.append(env)
        return out

    # ---- state --------------------------------------------------------------

    def checkpoint(self) -> List[Any]:
        """A point to come back to: every step's state and clock."""
        return [
            {"mark": s.state.mark(), "clock": s.clock.checkpoint() if s.clock is not None else None} if s.state is not None else None
            for s in self.steps
        ]

    def restore(self, cp: List[Any]) -> None:
        for i, s in enumerate(self.steps):
            if s.state is None:
                continue
            s.state.rollback_to(cp[i]["mark"])
            if s.clock is not None and cp[i]["clock"]:
                s.clock.restore(cp[i]["clock"])

    def commit_point(self) -> None:
        """What is in now is committed: the point a rollback comes back to."""
        for s in self._stateful:
            s.state.committed()  # type: ignore[union-attr]
            s.committed = s.clock.checkpoint() if s.clock is not None else None

    def discard(self) -> None:
        """Back to the last commit point."""
        for s in self._stateful:
            s.state.discard()  # type: ignore[union-attr]
            if s.engine is not None:
                if s.committed:
                    s.clock.restore(s.committed)
                s.engine.rebuild()
                s.clock.rebuild()

    @property
    def dirty(self) -> bool:
        return any(s.state.dirty for s in self._stateful)  # type: ignore[union-attr]

    @property
    def changed_since_snapshot(self) -> bool:
        return any(s.state.changed_since_snapshot for s in self._stateful)  # type: ignore[union-attr]

    def snapshot_taken(self) -> None:
        for s in self._stateful:
            s.state.snapshot_taken()  # type: ignore[union-attr]

    @property
    def empty(self) -> bool:
        """No step holds anything."""
        return all(s.state.size == 0 for s in self._stateful)  # type: ignore[union-attr]

    def dump(self) -> Dict[str, Any]:
        """Every step's state, for a snapshot."""
        return {"steps": [{"name": s.name, "kind": s.kind, "entries": s.state.dump()} for s in self._stateful]}  # type: ignore[union-attr]

    def metas(self) -> List[Dict[str, Any]]:
        """The clocks' own values (event-time watermarks and their anchors), carried in the snapshot pointer."""
        return [s.clock.meta() if s.clock is not None else {} for s in self._stateful]

    def load(self, dump: Any, metas: Any) -> None:
        """Every step from a snapshot (``dump`` None: empty) and the clocks' values."""
        stored = dump.get("steps") if isinstance(dump, dict) and isinstance(dump.get("steps"), list) else []
        for i, s in enumerate(self._stateful):
            d = stored[i] if i < len(stored) else None
            s.state.load(d.get("entries") if isinstance(d, dict) and d.get("kind") == s.kind else [])  # type: ignore[union-attr]
            if s.engine is not None:
                s.clock.load_meta(metas[i] if isinstance(metas, list) and i < len(metas) else None)
                s.engine.rebuild()
                s.clock.rebuild()
        self.commit_point()

    def load_begin(self) -> None:
        """A load in pieces (snapshot.py, format 2): load_begin(), then load_piece(...)
        for each piece of each layer in order, then load_end(metas)."""
        for s in self._stateful:
            s.state.load_begin()  # type: ignore[union-attr]

    def load_piece(self, step: Any, kind: Any, set_: Any, del_: Any) -> None:
        if not isinstance(step, int) or isinstance(step, bool) or step < 0 or step >= len(self._stateful):
            return
        s = self._stateful[step]
        if s.kind != kind:
            return
        st = s.state
        if isinstance(set_, list):
            for kv in set_:
                st.load_set(kv[0], kv[1] if len(kv) > 1 else UNDEFINED)  # type: ignore[union-attr]
        if isinstance(del_, list):
            for k in del_:
                st.load_delete(k)  # type: ignore[union-attr]

    async def load_end(self, metas: Any) -> None:
        for i, s in enumerate(self._stateful):
            s.state.loaded()  # type: ignore[union-attr]
            if s.engine is not None:
                s.clock.load_meta(metas[i] if isinstance(metas, list) and i < len(metas) else None)
                await s.engine.rebuild_async()
                await s.clock.rebuild_async()
        self.commit_point()

    @property
    def touched_since_snapshot(self) -> int:
        """How many keys changed since the last snapshot, over every step (a delta writes them)."""
        return sum(s.state.touched_since_snapshot for s in self._stateful)  # type: ignore[union-attr]

    def wipe(self) -> None:
        """Every step empty (a reset): the next snapshot writes it."""
        for s in self._stateful:
            s.state.wipe()  # type: ignore[union-attr]
            if s.engine is not None:
                s.clock.load_meta(None)
                s.engine.rebuild()
                s.clock.rebuild()
        self.commit_point()

    @property
    def open(self) -> int:
        """Open windows, waits, runs and rows per step, and in all."""
        n = 0
        for s in self._stateful:
            n += s.engine.open if s.engine is not None else s.table.open  # type: ignore[union-attr]
        return n

    @property
    def state_keys(self) -> int:
        return sum(s.state.size for s in self._stateful)  # type: ignore[union-attr]

    def step_metrics(self, t: Any) -> List[Dict[str, Any]]:
        """Per step: what is open, the state's keys, and on event time the watermark and the messages held."""
        out = []
        for s in self._stateful:
            m: Dict[str, Any] = {
                "name": s.name,
                "kind": s.kind,
                "open": s.engine.open if s.engine is not None else s.table.open,  # type: ignore[union-attr]
                "keys": s.state.size,  # type: ignore[union-attr]
            }
            if s.clock is not None:
                m["clock"] = s.clock.kind
                if s.clock.kind == "event":
                    w = s.clock.watermark_at(t)
                    m["watermark"] = js_iso(w) if js_is_finite(w) else None
                    m["held"] = s.clock.held
            out.append(m)
        return out

    def _no_step(self, name: Any) -> str:
        names = ", ".join(js_str(x.name) for x in self._stateful)
        return f"no step named {js_json_dumps(name) if name is not UNDEFINED else 'undefined'} ({names})"

    def inspect(self, name: Any, key: Any) -> Dict[str, Any]:
        """A live read of one step: ``inspect(key)``."""
        s = self.step(name)
        if s is None:
            return {"found": False, "error": self._no_step(name)}
        v = (s.engine or s.table).inspect(key)
        return {"found": False} if v is None else {"found": True, "value": v}

    def list(self, name: Any, limit: Any = 100) -> Dict[str, Any]:
        s = self.step(name)
        if s is None:
            return {"error": self._no_step(name), "items": []}
        items = []
        truncated = False
        for v in (s.engine or s.table).list():
            if len(items) >= limit:
                truncated = True
                break
            items.append(v)
        # The messages an event-time step holds in order are not the step's to list.
        return {"items": items, "truncated": truncated, "total": s.state.size - (s.clock.held if s.clock is not None else 0)}  # type: ignore[union-attr]
