"""
Stream — fluent builder for streaming pipelines. 1:1 port of Stream.js.

A Stream is an immutable chain of Operators that gets compiled and run by a
Runner. Each combinator (.map, .filter, .gate, ...) returns a NEW Stream
with the operator appended; the chain is finalized by ``await stream.run(...)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from .operators.aggregate_op import AggregateOperator
from .operators.filter_op import FilterOperator
from .operators.flat_map_op import FlatMapOperator
from .operators.foreach_op import ForeachOperator
from .operators.gate_op import GateOperator
from .operators.key_by_op import KeyByOperator
from .operators.map_op import MapOperator
from .operators.reduce_op import ReduceOperator
from .operators.sink_op import SinkOperator
from .operators.window_cron import WindowCronOperator
from .operators.window_session import WindowSessionOperator
from .operators.window_sliding import WindowSlidingOperator
from .operators.window_tumbling import WindowTumblingOperator
from .operators.pattern_op import PatternOperator
from .operators.collect_op import CollectOperator
from .operators.table_op import TableOperator
from .operators.name_op import NameOperator
from .operators.reduce_op import NO_INITIAL
from .operators._feed_opts import camel_opts
from .runtime.runner import Runner
from .util.config_hash import config_hash_of, js_config_hash_of
from .util.js import UNDEFINED

# Operators that run on a feed source only.
FEED_ONLY = ("pattern", "collect", "table", "name")

# Source options of Stream.from_feed, Python spelling -> the JS name.
_SOURCE_NAMES = {
    "from_": "from",
    "follower_urls": "followerUrls",
    "max_records": "maxRecords",
    "max_bytes": "maxBytes",
}


class FeedSource:
    """The source of a Stream.from_feed: the client, and the read's options (JS names)."""

    kind = "feed"

    def __init__(self, queen: Any, options: dict):
        self.queen = queen
        self.options = options


@dataclass
class CompiledFeed:
    """A chain on a feed, compiled: its steps, what goes to the broker, the sink, the chain hash."""

    source: Any
    source_options: dict
    steps: list
    pushdown: list
    sink: Any
    operators: list
    chain_hash: str
    config_hash: str


@dataclass
class _CompiledStages:
    pre: list
    key_by: Optional[Any]
    window: Optional[Any]
    reducer: Optional[Any]
    gate: Optional[Any]
    post: list
    sink: Optional[Any]


@dataclass
class _CompiledStream:
    source: Any
    source_options: dict
    stages: _CompiledStages
    operators: list
    config_hash: str


class Stream:
    """Fluent, immutable streaming pipeline builder."""

    @classmethod
    def from_(cls, queue_builder: Any, **options: Any) -> "Stream":
        """
        Build a Stream sourced from a Queen QueueBuilder.

        Args:
            queue_builder: result of ``queen.queue('name')`` — must
                expose ``.pop()``, ``.ack()``, ``.push()`` and the queue name.
        """
        return cls(source=queue_builder, operators=[], source_options=options)

    # JS-compatible alias
    @classmethod
    def from_queue(cls, queue_builder: Any, **options: Any) -> "Stream":
        return cls.from_(queue_builder, **options)

    @classmethod
    def from_feed(cls, queen: Any, options: Optional[dict] = None, **kwargs: Any) -> "Stream":
        """
        Build a Stream sourced from the tenant feed: every push to the tenant's
        queues (or the ones named), in log order, read from followers::

            runner = await (
                Stream.from_feed(client, queues=["orders"])
                .where("data.country", "=", "IT")       # sent to the broker, filtered there
                .filter(lambda m: is_vip(m["data"]))     # your code, runs here
                .to(client.queue("orders-it"))
                .run(query_id="orders-it")
            )

        The runner keeps its position in KV (``queen-feed`` / query_id) and
        commits it with the sink's pushes, and the state of its windows and
        patterns, in one transaction. One process runs a query_id at a time;
        another one with the same query_id waits and takes over when the first
        stops or dies.

        Options: ``queues`` (names or patterns; default every queue),
        ``partitions``, ``from_`` ('now' by default, 'earliest', or a position:
        where a NEW query starts), ``since`` (a new query starts at messages
        created since: '1d', ISO, datetime, epoch ms), ``follower_urls``.
        A mapping with the JS names (``from``, ``followerUrls``) works too.
        """
        if queen is None or not callable(getattr(queen, "feed", None)) or not callable(getattr(queen, "transaction", None)):
            raise ValueError("Stream.from_feed(queen, options): pass the Queen client")
        opts: dict = {}
        for src in (options or {}), kwargs:
            for k, v in src.items():
                if v is None:
                    continue
                opts[_SOURCE_NAMES.get(k, k)] = v
        return cls(source=FeedSource(queen, opts), operators=[], source_options=opts)

    fromFeed = from_feed

    @staticmethod
    def view(queen: Any, query_id: str, *, shards: Optional[int] = None, timeout_millis: Optional[int] = None) -> Any:
        """
        Ask a running feed stream about its live state, from any process::

            view = Stream.view(client, "reply-time")
            await view.get("perTenant", "tenant-42")
            await view.list("perTenant", limit=50)
            await view.status()

        Each active runner answers on an ephemeral queue of its own (see
        runtime/feed/query.py). Steps are named with .name(), else step1, step2...
        """
        from .runtime.feed.query import FeedView

        return FeedView(queen, query_id, shards=shards, timeout_millis=timeout_millis)

    def __init__(self, source: Any, operators: List[Any], source_options: Optional[dict] = None):
        self._source = source
        self._source_options = source_options or {}
        self._operators = operators

    # ---------------------------------------------------------------- Stateless

    def map(self, fn: Callable[..., Any]) -> "Stream":
        return self._extend(MapOperator(fn))

    def filter(self, predicate: Callable[..., Any]) -> "Stream":
        return self._extend(FilterOperator(predicate))

    def where(self, field: str, op: str, value: Any = UNDEFINED) -> "Stream":
        """
        Keep only the source messages where ``field op value`` holds. On a feed
        source the condition is sent to the broker and filtered there (nothing
        that fails it crosses the network); on a queue source it is evaluated
        here. Wherever it sits in the chain, it tests the source message::

            .where("data.country", "=", "IT")
            .where("data.total", ">", 100)
            .where("partition", "prefix", "booking-")

        Fields: queue, partition, transactionId, offset, position, data,
        data.<path>. Ops: = != < <= > >= in exists prefix !prefix.
        """
        from ..feed.feed import condition_holds, where_condition

        cond = where_condition(field, op, value)
        flt = FilterOperator(lambda msg: condition_holds(cond, msg))
        flt.where = cond  # type: ignore[attr-defined]
        flt.config = {"kind": "filter", "where": cond}
        return self._extend(flt)

    def flat_map(self, fn: Callable[..., Any]) -> "Stream":
        return self._extend(FlatMapOperator(fn))

    # JS aliases (camelCase) for portability
    flatMap = flat_map

    # -------------------------------------------------------------------- Keying

    def key_by(self, fn: Callable[[Any], Any]) -> "Stream":
        return self._extend(KeyByOperator(fn))

    keyBy = key_by

    # -------------------------------------------------------------------- Windows

    def window_tumbling(self, **opts: Any) -> "Stream":
        return self._extend(WindowTumblingOperator(**_kwargs_camel_to_snake(opts)))

    def window_sliding(self, **opts: Any) -> "Stream":
        return self._extend(WindowSlidingOperator(**_kwargs_camel_to_snake(opts)))

    def window_session(self, **opts: Any) -> "Stream":
        return self._extend(WindowSessionOperator(**_kwargs_camel_to_snake(opts)))

    def window_cron(self, **opts: Any) -> "Stream":
        return self._extend(WindowCronOperator(**_kwargs_camel_to_snake(opts)))

    # JS aliases
    windowTumbling = window_tumbling
    windowSliding = window_sliding
    windowSession = window_session
    windowCron = window_cron

    # -------------------------------------------------------------------- Reduce

    def reduce(self, fn: Callable, initial: Any = NO_INITIAL) -> "Stream":
        return self._extend(ReduceOperator(fn, initial))

    def aggregate(self, extractors: dict) -> "Stream":
        return self._extend(AggregateOperator(extractors))

    # ------------------------------------------------------------------ Patterns

    def pattern(self, opts: Optional[dict] = None, **kwargs: Any) -> "Stream":
        """
        A ``start`` message, then a ``then`` message for the same entity within
        ``within``, on the feed's clock (the time the log committed each message,
        the same for every queue). Feed sources only::

            Stream.from_feed(client, queues=["orders", "payments"]).pattern(
                start=lambda m: m["queue"] == "orders",
                then=lambda m: m["queue"] == "payments",
                within="15m",
                on_match=lambda order, payment: {"order": order["data"]["id"], "paid": True},
                on_timeout=lambda order: {"order": order["data"]["id"], "remind": True},
            ).to(client.queue("order-followups"))

        The entity is ``key(msg, value)``, else ``.key_by()``, else the partition
        name. ``on_match`` and ``on_timeout`` answer what to send on (None:
        nothing); without ``on_match`` a match sends ``{key, start, then}``.
        ``on_repeat='restart'`` lets a new start replace the one waiting.
        ``event_time``, ``allowed_lateness``, ``on_late`` and ``idle`` put
        ``within`` on the messages' own times (see the JS docs: same rules).
        """
        return self._extend(PatternOperator("pattern", camel_opts(opts, **kwargs)))

    def join(self, opts: Optional[dict] = None, **kwargs: Any) -> "Stream":
        """
        Two sides (``left``, ``right``: a pair in either order within ``within``;
        ``select(left, right)``, ``on_timeout(msg, side)``), or any number of
        ``sides`` (``{name: test or queue name}``, or a list of queue names;
        ``select(got, key)``, ``on_timeout(got, missing, key)``). Feed sources only.
        """
        o = camel_opts(opts, **kwargs)
        if o.get("sides") is not None:
            return self._extend(CollectOperator("join", o))
        return self._extend(PatternOperator("join", o))

    def gather(self, opts: Optional[dict] = None, **kwargs: Any) -> "Stream":
        """
        The parts of one run, collected until all are in or the deadline passes.
        ``steps`` names the parts (queue names, or ``{name: test}``);
        ``count(msg, value)`` instead says how many distinct messages a run has.
        All in: ``on_complete(got, key)``; past the deadline:
        ``on_timeout(got, missing, key)``. Feed sources only.
        """
        return self._extend(CollectOperator("gather", camel_opts(opts, **kwargs)))

    def join_table(self, opts: Optional[dict] = None, **kwargs: Any) -> "Stream":
        """
        Join every message with the latest row of a table queue (one message per
        change; None deletes). A message sees the table as of its own position.
        Feed sources only: ``queue``, ``on(msg, value)``, ``key(row)``,
        ``value(row)``, ``select(value, row, msg)``, ``missing`` ('keep' | 'drop'),
        ``initial`` ('all' | 'now').
        """
        return self._extend(TableOperator(camel_opts(opts, **kwargs)))

    joinTable = join_table

    def name(self, name: str) -> "Stream":
        """Name the stateful step before this call, for live reads
        (``runner.state(name)``, ``Stream.view(...).get(name, key)``)."""
        return self._extend(NameOperator(name))

    # ---------------------------------------------------------------------- Gate

    def gate(self, fn: Callable[..., Any]) -> "Stream":
        """
        Per-message ALLOW/DENY decision with persistent per-key state.

        Used to build rate limiters, throttlers, fairness gates, circuit
        breakers. See queen.streams.helpers.rate_limiter for ready-made
        gate factories.
        """
        return self._extend(GateOperator(fn))

    # ---------------------------------------------------------------------- Sink

    def to(self, target: Any, partition: Optional[Any] = None) -> "Stream":
        """
        Sink to Queen queues. The cycle commits state + push + ack atomically::

            .to(client.queue("orders-it"))                     # one queue
            .to([client.queue("audit"), client.queue("bi")])   # each record to both (feed)
            .to(lambda order, msg: f"orders-{order['country']}")  # a function picks (feed)

        A function gets ``(value, msg)`` and answers a queue (builder or name),
        ``{"queue", "partition"}``, a list of those, or None for nowhere.
        ``partition``: a string, or a function of the value.
        """
        return self._extend(SinkOperator(target, partition=partition))

    def foreach(self, fn: Callable[..., Any]) -> "Stream":
        return self._extend(ForeachOperator(fn))

    # ----------------------------------------------------------------- Compile/run

    async def run(self, query_id: Optional[str] = None, url: Optional[str] = None, **opts: Any) -> Any:
        """
        Start the streaming runner. Returns a Runner exposing ``stop()`` and
        ``metrics()``.

        Args:
            query_id: durable identity of this query (becomes the
                consumer group + state shard).
            url: Queen broker URL (e.g. ``http://localhost:6632``).
            batch_size: messages per cycle (default 200)
            max_partitions: pop up to N partitions per cycle (default 4)
            max_wait_millis: long-poll wait for source pop (default 1000)
            subscription_mode: 'all' (default) | 'new'
            subscription_from: ISO timestamp or 'now'
            reset: True to wipe existing state if config_hash mismatches
            consumer_group: override the default 'streams.<queryId>' group
            on_error: callable(err, ctx) for cycle error hook
            bearer_token: JWT for auth-enabled brokers
        """
        if query_id is None:
            query_id = opts.pop("queryId", None)
        if not query_id:
            raise ValueError("run requires query_id=...")
        if isinstance(self._source, FeedSource):
            return await self._run_feed(query_id, opts)
        feed_only = next((op for op in self._operators if op.kind in FEED_ONLY), None)
        if feed_only is not None:
            raise ValueError(
                ".pattern(), .join(), .gather(), .join_table() and .name() run on Stream.from_feed only: "
                "they need the clock every queue shares"
            )
        if any(op.kind == "window" and getattr(op, "idle_ms", None) is not None for op in self._operators):
            raise ValueError(
                "a window's idle runs on Stream.from_feed only (on a queue source, idle_flush_ms closes windows on quiet partitions)"
            )
        sink_op = next((op for op in self._operators if op.kind == "sink"), None)
        if sink_op is not None and getattr(sink_op, "routes", False):
            raise ValueError(
                ".to() with several queues or a function runs on Stream.from_feed only; a queue source sinks to one queue"
            )
        if not url:
            raise ValueError("run requires url=... on a queue source")
        compiled = self._compile()
        runner_opts: dict = {
            "queryId": query_id,
            "url": url,
            "stream": compiled,
        }
        # Map snake_case kwargs to the runner's camelCase keys.
        for k, v in opts.items():
            runner_opts[_to_camel(k)] = v
        runner = Runner(**runner_opts)
        await runner.start()
        return runner

    async def _run_feed(self, query_id: str, opts: dict) -> Any:
        from .runtime.feed_runner import FeedRunner, runner_options
        from .runtime.feed_shards import FeedShardGroup

        compiled = self._compile_feed()
        options = runner_options(opts)
        shards = options.pop("shards", None)
        if shards is not None and shards != 1:
            group = FeedShardGroup(stream=compiled, query_id=query_id, shards=shards, **options)
            await group.start()
            return group
        runner = FeedRunner(stream=compiled, query_id=query_id, **options)
        await runner.start()
        return runner

    def _compile_feed(self) -> CompiledFeed:
        """
        Compile a chain on a feed into steps: stateless operators, and the
        stateful steps in chain order (a window and its reduce/aggregate are
        one step). ``where`` before the first stateful step goes to the broker.
        ``chain_hash`` is made from the stateful steps and the key_bys around
        them: a different one finds the stored state was kept by another chain.
        """
        ops = self._operators
        sink_idx = next((i for i, op in enumerate(ops) if op.kind in ("sink", "foreach")), -1)
        if sink_idx != -1 and sink_idx != len(ops) - 1:
            raise ValueError(
                "sink operators (.to / .foreach) must be the last in the chain; "
                f"found {ops[sink_idx].kind} at position {sink_idx} of {len(ops)}"
            )
        sink = None if sink_idx == -1 else ops[sink_idx]
        upstream = list(ops) if sink_idx == -1 else list(ops[:sink_idx])
        steps: list = []
        pushdown: list = []
        pending: list = []
        window = None
        stateful = 0

        def flush() -> None:
            nonlocal pending
            if pending:
                steps.append({"kind": "ops", "ops": pending})
            pending = []

        def named(op: Any) -> str:
            return getattr(op, "step_name", None) or f"step{stateful}"

        for op in upstream:
            if window is not None and op.kind not in ("reduce", "aggregate"):
                raise ValueError("Stream.fromFeed: a window needs .reduce() or .aggregate() right after it")
            kind = op.kind
            if kind == "filter":
                # Before any stateful step a where tests the source message: the broker does it.
                if getattr(op, "where", None) is not None and stateful == 0:
                    pushdown.append(op.where)
                else:
                    pending.append(op)
            elif kind in ("map", "flatMap", "keyBy"):
                pending.append(op)
            elif kind == "window":
                flush()
                window = op
            elif kind in ("reduce", "aggregate"):
                if window is None:
                    raise ValueError("reduce/aggregate requires a preceding window operator")
                stateful += 1
                steps.append({"kind": "window", "window": window, "reducer": op, "name": f"step{stateful}"})
                window = None
            elif kind in ("pattern", "collect", "table"):
                flush()
                stateful += 1
                steps.append({"kind": kind, "op": op, "name": named(op)})
            elif kind == "name":
                last = next((st for st in reversed(steps) if st["kind"] != "ops"), None)
                if last is None or pending:
                    raise ValueError(".name() names the stateful step right before it")
                last["name"] = op.name
            elif kind == "gate":
                raise ValueError(
                    "Stream.fromFeed: .gate() runs on queue sources only — it holds a partition's lease, and a feed has none"
                )
            else:
                raise ValueError(f"unsupported operator: {kind}")
        if window is not None:
            raise ValueError("Stream.fromFeed: a window needs .reduce() or .aggregate() after it")
        flush()
        names = [st["name"] for st in steps if st["kind"] != "ops"]
        dup = next((n for i, n in enumerate(names) if names.index(n) != i), None)
        if dup is not None:
            from .util.js import js_json_dumps

            raise ValueError(f"Stream.fromFeed: two stateful steps are named {js_json_dumps(dup)}")
        # The clock each stateful step runs on, as the pipeline resolves it
        # (runtime/feed/pipeline.py): a step that takes the event time of the
        # step before says so in the hash, since its state is kept on that time.
        inheriting: set = set()
        on_event_time = False
        for st in steps:
            if st["kind"] in ("ops", "table"):
                continue
            op = st["window"] if st["kind"] == "window" else st["op"]
            if callable(getattr(op, "event_time_fn", None)):
                on_event_time = True
            elif on_event_time and not getattr(op, "event_time_off", False):
                inheriting.add(id(op))
            else:
                on_event_time = False
        shape = [
            (_Inherited(op) if id(op) in inheriting else op)
            for op in upstream
            if op.kind not in ("map", "filter", "flatMap")
        ]
        return CompiledFeed(
            source=self._source,
            source_options=self._source_options,
            steps=steps,
            pushdown=pushdown,
            sink=sink,
            operators=list(ops),
            chain_hash=js_config_hash_of(shape)[:16],
            config_hash=js_config_hash_of(ops),
        )

    # ===== internals =========================================================

    def _extend(self, operator: Any) -> "Stream":
        return Stream(
            source=self._source,
            operators=[*self._operators, operator],
            source_options=self._source_options,
        )

    def _compile(self) -> _CompiledStream:
        sink = next((op for op in self._operators if op.kind in ("sink", "foreach")), None)
        sink_idx = self._operators.index(sink) if sink else len(self._operators)
        if sink and sink_idx != len(self._operators) - 1:
            raise ValueError(
                "sink operators (.to / .foreach) must be the last in the chain; "
                f"found {sink.kind} at position {sink_idx} of {len(self._operators)}"
            )
        upstream = self._operators[:sink_idx] if sink else list(self._operators)

        stages = _CompiledStages(
            pre=[], key_by=None, window=None, reducer=None, gate=None, post=[], sink=sink
        )
        phase = "pre"
        for op in upstream:
            kind = op.kind
            if kind in ("map", "filter", "flatMap"):
                if phase in ("pre", "keyed", "window"):
                    stages.pre.append(op)
                else:
                    stages.post.append(op)
            elif kind == "keyBy":
                if stages.key_by is not None:
                    raise ValueError("only one .key_by() per stream")
                if phase in ("reducer", "gate"):
                    raise ValueError(".key_by() must come before window/reduce/gate")
                stages.key_by = op
                if phase == "pre":
                    phase = "keyed"
            elif kind == "window":
                if stages.window is not None:
                    raise ValueError("only one window operator per stream")
                if phase == "reducer":
                    raise ValueError("window must come before reduce/aggregate")
                if stages.gate is not None:
                    raise ValueError("window+reduce is incompatible with .gate() in the same stream")
                stages.window = op
                phase = "window"
            elif kind in ("reduce", "aggregate"):
                if stages.reducer is not None:
                    raise ValueError("only one reduce/aggregate per stream")
                if stages.window is None:
                    raise ValueError("reduce/aggregate requires a preceding window operator")
                if stages.gate is not None:
                    raise ValueError("reduce/aggregate is incompatible with .gate() in the same stream")
                stages.reducer = op
                phase = "reducer"
            elif kind == "gate":
                if stages.gate is not None:
                    raise ValueError("only one .gate() per stream")
                if stages.window is not None or stages.reducer is not None:
                    raise ValueError(".gate() is incompatible with windowing/reduce in the same stream")
                stages.gate = op
                phase = "gate"
            else:
                raise ValueError(f"unsupported operator: {kind}")

        config_hash = config_hash_of(self._operators)

        return _CompiledStream(
            source=self._source,
            source_options=self._source_options,
            stages=stages,
            operators=list(self._operators),
            config_hash=config_hash,
        )


class _Inherited:
    """A step that takes the event time of the step before, as the hash describes it."""

    def __init__(self, op: Any):
        self.kind = op.kind
        cfg = getattr(op, "feed_config", None)
        if not isinstance(cfg, dict):
            cfg = getattr(op, "config", None) or {}
        self.config = {**cfg, "clock": "event"}


def _to_camel(s: str) -> str:
    parts = s.split("_")
    return parts[0] + "".join(p.title() for p in parts[1:])


def _kwargs_camel_to_snake(opts: dict) -> dict:
    """
    Accept both camelCase JS-style kwargs and snake_case Python-style for
    window operators. Internally always translate to snake_case so the
    operator constructors can use Python conventions.
    """
    out: dict = {}
    for k, v in opts.items():
        if k == "gracePeriod":
            out["grace_period"] = v
        elif k == "idleFlushMs":
            out["idle_flush_ms"] = v
        elif k == "eventTime":
            out["event_time"] = v
        elif k == "allowedLateness":
            out["allowed_lateness"] = v
        elif k == "onLate":
            out["on_late"] = v
        else:
            out[k] = v
    return out
