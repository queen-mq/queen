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
from .runtime.runner import Runner
from .util.config_hash import config_hash_of


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

    def __init__(self, source: Any, operators: List[Any], source_options: Optional[dict] = None):
        self._source = source
        self._source_options = source_options or {}
        self._operators = operators

    # ---------------------------------------------------------------- Stateless

    def map(self, fn: Callable[..., Any]) -> "Stream":
        return self._extend(MapOperator(fn))

    def filter(self, predicate: Callable[..., Any]) -> "Stream":
        return self._extend(FilterOperator(predicate))

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

    def reduce(self, fn: Callable, initial: Any = None) -> "Stream":
        return self._extend(ReduceOperator(fn, initial))

    def aggregate(self, extractors: dict) -> "Stream":
        return self._extend(AggregateOperator(extractors))

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

    def to(self, sink_queue_builder: Any, partition: Optional[Any] = None) -> "Stream":
        return self._extend(SinkOperator(sink_queue_builder, partition=partition))

    def foreach(self, fn: Callable[..., Any]) -> "Stream":
        return self._extend(ForeachOperator(fn))

    # ----------------------------------------------------------------- Compile/run

    async def run(self, query_id: str, url: str, **opts: Any) -> Runner:
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
        if not query_id:
            raise ValueError("run requires query_id=...")
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
