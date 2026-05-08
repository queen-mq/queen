"""
SinkOperator — terminal ``.to(queue)``. Records flowing into the sink are
pushed onto the destination Queen queue as part of the cycle commit.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union


class SinkOperator:
    kind = "sink"

    def __init__(self, queue_builder: Any, partition: Optional[Union[str, Callable[[Any], Any]]] = None):
        self.queue_builder = queue_builder
        self.queue_name = _extract_queue_name(queue_builder)
        self.partition_resolver = partition
        self.config: dict = {"kind": "sink", "queue": self.queue_name}

    def build_push_items(self, entries: list, source_partition_name: Optional[str]) -> list[dict]:
        items = []
        for entry in entries:
            value = entry.get("value")
            payload = value if isinstance(value, dict) else {"value": value}
            items.append({
                "queue": self.queue_name,
                "partition": self._resolve_partition(value, source_partition_name),
                "payload": payload,
            })
        return items

    def _resolve_partition(self, value: Any, source_partition_name: Optional[str]) -> str:
        if callable(self.partition_resolver):
            return str(self.partition_resolver(value))
        if isinstance(self.partition_resolver, str):
            return self.partition_resolver
        return source_partition_name or "Default"


def _extract_queue_name(qb: Any) -> str:
    """Get the queue name from a QueueBuilder, a fake, or a dict."""
    # QueueBuilder has a private/protected name attribute. Try the public ones first.
    for attr in ("_queue_name", "queue_name"):
        v = getattr(qb, attr, None)
        if isinstance(v, str) and v:
            return v
    # Object with .name property
    name = getattr(qb, "name", None)
    if isinstance(name, str) and name:
        return name
    # Plain dict-like
    if isinstance(qb, dict):
        for key in ("queueName", "queue_name", "name"):
            if isinstance(qb.get(key), str) and qb[key]:
                return qb[key]
    raise ValueError(
        "Could not extract queue name from sink target. "
        "Pass either a queen.queue('name') builder or {'queueName': 'name'}."
    )
