"""
What a feed stream is, for whoever looks at it from outside (the console): the
queues it reads, its steps in order, where its results go, and what it does
with a message that keeps failing. Port of the JS SDK's runtime/feed/about.js.

The active runner writes it to ``queen-feed`` / ``<queryId>#about`` in its first
commit after it becomes active, under the lock like the rest of the commit
(one record per query: every shard writes the same one). Nothing reads it back
to run the stream: the chain in the code is the stream.

Functions are not described (a ``.map(fn)`` is ``map``), and long values are
cut: a ``where`` with a list of 5,000 names keeps the first 50.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from ...util.js import UNDEFINED, is_number, js_is_finite, js_iso, js_str, utf16_len

FORMAT = 1
MAX_TEXT = 200
MAX_LIST = 50
MAX_DEPTH = 4

_client: Optional[str] = None


def about_key(query_id: str) -> str:
    """The key of a query's description in ``queen-feed``."""
    return f"{query_id}#about"


def _client_name() -> str:
    global _client
    if _client is None:
        try:
            from .... import __version__

            _client = f"client-py {__version__}"
        except Exception:  # noqa: BLE001
            _client = "client-py"
    return _client


def utf16_slice(s: str, n: int) -> str:
    """``s.slice(0, n)`` in JS (UTF-16 code units)."""
    if s.isascii():
        return s[:n]
    data = s.encode("utf-16-le", "surrogatepass")[: 2 * n]
    return data.decode("utf-16-le", "surrogatepass")


def clip_text(s: Any, max_chars: int) -> str:
    t = s if isinstance(s, str) else js_str(s)
    return f"{utf16_slice(t, max_chars)}…" if utf16_len(t) > max_chars else t


def clip(v: Any, depth: int = 0) -> Any:
    """A JSON-safe copy, cut to a size worth storing: long strings, long lists, deep objects, no functions."""
    if v is None or v is UNDEFINED:
        return None
    if isinstance(v, str):
        return f"{utf16_slice(v, MAX_TEXT)}…" if utf16_len(v) > MAX_TEXT else v
    if isinstance(v, bool):
        return v
    if is_number(v):
        return v if js_is_finite(v) else js_str(v)
    if isinstance(v, datetime):
        return js_iso(v.timestamp() * 1000)
    if isinstance(v, (list, tuple)):
        out = [clip(x, depth + 1) for x in list(v)[:MAX_LIST]]
        if len(v) > MAX_LIST:
            out.append(f"… {len(v) - MAX_LIST} more")
        return out
    if isinstance(v, dict):
        if depth >= MAX_DEPTH:
            return "…"
        return {(k if isinstance(k, str) else js_str(k)): clip(x, depth + 1) for k, x in v.items() if x is not UNDEFINED and not callable(x)}
    if callable(v):
        return "fn"
    return js_str(v)


def _as_list(v: Any) -> Any:
    if v is None or v is UNDEFINED:
        return None
    return list(v) if isinstance(v, (list, tuple)) else [v]


def _where_of(c: Any) -> Any:
    if not isinstance(c, dict):
        return None
    return clip({"field": c.get("field", UNDEFINED), "op": c.get("op", UNDEFINED), "value": c.get("value", UNDEFINED)})


def _config_of(op: Any) -> Dict[str, Any]:
    cfg = getattr(op, "feed_config", None)
    if not isinstance(cfg, dict):
        cfg = getattr(op, "config", None)
    return cfg if isinstance(cfg, dict) else {}


def _step_of(s: Dict[str, Any], pipe_step: Any) -> Dict[str, Any]:
    if s["kind"] == "ops":
        ops = s["ops"]
        out: Dict[str, Any] = {"kind": "ops", "ops": ["where" if op.kind == "filter" and getattr(op, "where", None) else op.kind for op in ops]}
        where = [_where_of(op.where) for op in ops if op.kind == "filter" and getattr(op, "where", None)]
        if where:
            out["where"] = where
        return out
    clock = pipe_step.clock.kind if pipe_step is not None and getattr(pipe_step, "clock", None) is not None else None
    if s["kind"] == "window":
        reducer = s["reducer"]
        out = {
            "kind": "window",
            "name": s["name"],
            "window": clip(_config_of(s["window"])),
            "reducer": {"kind": reducer.kind, **clip(_config_of(reducer))},
        }
        if clock:
            out["clock"] = clock
        return out
    config = clip(_config_of(s["op"]))
    # pattern, join (two sides or many), gather, table: the name the chain gave it.
    kind = "table" if s["kind"] == "table" else (config.get("kind") or s["kind"])
    out = {"kind": kind, "name": s["name"], "config": config}
    if clock:
        out["clock"] = clock
    return out


def _sink_of(sink: Any) -> Any:
    if not sink:
        return None
    if sink.kind == "foreach":
        return {"kind": "foreach"}
    if getattr(sink, "route", None):
        return {"kind": "route"}
    queues = getattr(sink, "queues", None) or ([sink.queue_name] if getattr(sink, "queue_name", None) else [])
    return {"kind": "queues", "queues": clip(queues)}


def describe_feed_stream(
    *,
    stream: Any,
    pipeline: Any,
    query_id: str,
    shard: Any,
    shard_by: Any,
    poison: Dict[str, Any],
    exclusive: Any,
    lock_ttl: Any,
    position_commit_millis: Any,
) -> Dict[str, Any]:
    """The description of one stream (the same, in meaning, as the JS SDK's)."""
    source = stream.source.options or {}
    queues = source.get("queues")
    if queues is None or queues is UNDEFINED:
        queues = source.get("queue")
    partitions = source.get("partitions")
    if partitions is None or partitions is UNDEFINED:
        partitions = source.get("partition")
    reads: Dict[str, Any] = {
        "queues": clip(_as_list(queues)),
        "partitions": clip(_as_list(partitions)),
        "where": [w for w in (_where_of(c) for c in (stream.pushdown or [])) if w],
    }
    if source.get("kinds", UNDEFINED) is not UNDEFINED:
        reads["kinds"] = clip(source["kinds"])
    if source.get("since", UNDEFINED) is not UNDEFINED:
        reads["since"] = clip(source["since"])
    tables: List[str] = []
    for t in pipeline.tables:
        for q in t.queues or []:
            if q not in tables:
                tables.append(q)
    if tables:
        reads["tables"] = clip(tables)
    return {
        "v": FORMAT,
        "queryId": query_id,
        "hash": stream.chain_hash or None,
        "client": _client_name(),
        "reads": reads,
        "steps": [_step_of(s, pipeline.steps[i] if i < len(pipeline.steps) else None) for i, s in enumerate(stream.steps)],
        "sink": _sink_of(stream.sink),
        "onPoison": {"queue": poison["queue"]} if poison["kind"] == "deadLetter" else poison["kind"],
        "shards": {"count": shard["count"], "by": "function" if callable(shard_by) else "partition"} if shard else None,
        "exclusive": exclusive is not False,
        "lockTtl": clip(lock_ttl),
        "positionCommitMillis": position_commit_millis if js_is_finite(position_commit_millis) else None,
    }
