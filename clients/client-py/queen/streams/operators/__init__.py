"""
queen.streams.operators — operator classes used by the fluent Stream builder.

Most code should not import these directly; use the Stream methods (.map,
.filter, .gate, .window_tumbling, etc.) and pass user callables.
"""

from .map_op import MapOperator
from .filter_op import FilterOperator
from .flat_map_op import FlatMapOperator
from .key_by_op import KeyByOperator
from .window_tumbling import WindowTumblingOperator
from .window_sliding import WindowSlidingOperator
from .window_session import WindowSessionOperator
from .window_cron import WindowCronOperator
from .reduce_op import ReduceOperator, state_key_for, parse_state_key
from .aggregate_op import AggregateOperator, RESERVED_AGGREGATE_FIELDS
from .sink_op import SinkOperator
from .foreach_op import ForeachOperator
from .gate_op import GateOperator

__all__ = [
    "MapOperator",
    "FilterOperator",
    "FlatMapOperator",
    "KeyByOperator",
    "WindowTumblingOperator",
    "WindowSlidingOperator",
    "WindowSessionOperator",
    "WindowCronOperator",
    "ReduceOperator",
    "AggregateOperator",
    "RESERVED_AGGREGATE_FIELDS",
    "SinkOperator",
    "ForeachOperator",
    "GateOperator",
    "state_key_for",
    "parse_state_key",
]
