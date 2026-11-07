"""
SinkOperator — terminal ``.to(...)``. Records flowing into the sink are
pushed onto Queen queues as part of the cycle commit.

The target is one of:
  - a queue: ``client.queue('orders-it')`` (or its name)
  - several queues: ``[client.queue('audit'), client.queue('orders-it')]``;
    every record goes to each of them
  - a function ``(value, msg) -> target``, called per record, that answers a
    queue (builder or name), ``{"queue", "partition"}``, a list of those, or
    None to send the record nowhere

Several queues and functions run on a feed source (``Stream.from_feed``),
whose commit can push to any queue; a queue source sinks to one queue.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Union

from ..util.js import call_user, is_number, js_number_str, js_str, nullish


class SinkOperator:
    kind = "sink"

    def __init__(self, target: Any, partition: Optional[Union[str, Callable[[Any], Any]]] = None):
        self.partition_resolver = partition
        if callable(target) and not _is_queue_like(target):
            self.route: Optional[Callable[..., Any]] = target
            self.queue_builder = None
            self.queues: Optional[List[str]] = None
            self.queue_name: Optional[str] = None
            self.config: dict = {"kind": "sink", "route": True}
        elif isinstance(target, (list, tuple)):
            if len(target) == 0:
                raise ValueError(".to([]): name at least one queue")
            self.route = None
            self.queue_builder = None
            names: List[str] = []
            for t in target:
                n = _extract_queue_name(t)
                if n not in names:
                    names.append(n)
            self.queues = names
            self.queue_name = names[0] if len(names) == 1 else None
            self.config = {"kind": "sink", "queues": list(names)}
        else:
            self.route = None
            self.queue_builder = target
            self.queue_name = _extract_queue_name(target)
            self.queues = [self.queue_name]
            self.config = {"kind": "sink", "queue": self.queue_name}

    @property
    def routes(self) -> bool:
        """True when records may go to more than one queue (a function or a list)."""
        return self.route is not None or len(self.queues or []) > 1

    def build_push_items(self, entries: list, source_partition_name: Optional[str]) -> list:
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

    async def targets_for(self, value: Any, msg: Any, fallback: Any) -> List[dict]:
        """Where one record goes: ``[{queue, partition}]``, empty to send it nowhere.
        ``partition`` is the target's own, else the sink's ``partition`` option, else
        ``fallback`` (the source partition, or the key of a window or a pattern)."""
        if self.route is not None:
            raw = await call_user(self.route, value, msg)
            if nullish(raw):
                return []
            if not isinstance(raw, (list, tuple)):
                raw = [raw]
        else:
            raw = self.queues or []
        out = []
        for t in raw:
            if nullish(t):
                continue
            own = isinstance(t, dict) and not isinstance(t.get("name"), str) and isinstance(t.get("queue"), str)
            queue = _require_queue(t["queue"]) if own else _extract_queue_name(t)
            partition = t.get("partition") if own and not nullish(t.get("partition")) else None
            if partition is None:
                partition = await self._resolve_partition_async(value, fallback)
            out.append({"queue": queue, "partition": _require_partition(partition, queue)})
        return out

    def _resolve_partition(self, value: Any, source_partition_name: Optional[str]) -> str:
        if callable(self.partition_resolver):
            return str(self.partition_resolver(value))
        if isinstance(self.partition_resolver, str):
            return self.partition_resolver
        return source_partition_name or "Default"

    async def _resolve_partition_async(self, value: Any, fallback: Any) -> str:
        if callable(self.partition_resolver):
            return js_str(await call_user(self.partition_resolver, value))
        if isinstance(self.partition_resolver, str):
            return self.partition_resolver
        return "Default" if nullish(fallback) or fallback == "" else js_str(fallback)


def payload_of(value: Any) -> Any:
    """A pushed payload is an object (a dict or a list): anything else travels as ``{"value": ...}``."""
    return value if isinstance(value, (dict, list)) else {"value": value}


def _is_queue_like(t: Any) -> bool:
    for attr in ("_queue_name", "queue_name"):
        if isinstance(getattr(t, attr, None), str):
            return True
    return False


def _require_queue(name: Any) -> str:
    if not isinstance(name, str) or len(name) == 0:
        from ..util.js import js_json_dumps

        raise ValueError(f".to(): a queue name is a non-empty string, got {js_json_dumps(name)}")
    return name


def _require_partition(partition: Any, queue: str) -> str:
    p = js_number_str(partition) if is_number(partition) else partition
    if not isinstance(p, str) or len(p) == 0:
        from ..util.js import js_json_dumps

        raise ValueError(f".to(): the partition for {queue} is a non-empty string, got {js_json_dumps(partition)}")
    return p


def _extract_queue_name(qb: Any) -> str:
    """Get the queue name from a QueueBuilder, a fake, a dict, or a string."""
    if isinstance(qb, str):
        return _require_queue(qb)
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
