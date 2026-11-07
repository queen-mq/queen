"""
FeedShardGroup — ``run(query_id=..., shards=N)``: the N shards of one feed
stream in this process, one FeedRunner each. Port of the JS SDK's
runtime/FeedShards.js.

Each shard has its own lock: the same code started in several processes runs
each shard in the first process that takes its lock, and keeps it as a standby
in the others, which take over when that process stops or dies. To place
shards on purpose, run ``shard={"index": i, "count": n}`` in each process.

The handle answers like one runner: stop(), done, metrics() (the sums, and
every shard's), and state(step).get(key) / list(limit) over the shards this
process runs.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..util.js import js_is_finite, js_is_integer, js_json_dumps
from .feed_runner import FeedRunner

_SUMS = (
    "cycles", "entriesRead", "emitted", "outputs", "effects", "batches", "commits", "conflicts", "errors", "retries",
    "poisoned", "skipped", "deadLettered", "late", "ticks", "takeovers", "snapshots", "replays", "replayEntries",
    "tableRows", "pendingOutputs", "pendingBatch", "open", "stateKeys",
)


class _GroupState:
    def __init__(self, runners: List[FeedRunner], step: Any) -> None:
        self._runners = runners
        self._step = step

    def get(self, key: Any) -> Dict[str, Any]:
        for r in self._runners:
            v = r.state(self._step).get(key)
            if v.get("found") or v.get("error"):
                return {**v, "shard": r.shard["index"] if r.shard else 0}
        return {"found": False}

    def list(self, limit: Any = 100) -> Dict[str, Any]:
        items: List[Any] = []
        truncated = False
        for r in self._runners:
            part = r.state(self._step).list(limit)
            if part.get("error"):
                return part
            items.extend(part.get("items") or [])
            truncated = truncated or bool(part.get("truncated"))
        return {"items": items[:limit], "truncated": truncated or len(items) > limit}


class FeedShardGroup:
    def __init__(self, *, shards: Any, **opts: Any) -> None:
        if not js_is_integer(shards) or shards < 2 or shards > 4096:
            raise ValueError(f"run({{ shards }}): shards is a whole number from 2 to 4096, got {js_json_dumps(shards) if isinstance(shards, (int, float, str)) else repr(shards)}")
        if opts.get("shard") is not None:
            raise ValueError("run(): give shards (all of them here) or shard (one), not both")
        self._count = int(shards)
        self._opts = opts
        self._runners: List[FeedRunner] = []

    @property
    def shards(self) -> int:
        return self._count

    @property
    def runners(self) -> List[FeedRunner]:
        return list(self._runners)

    async def start(self) -> None:
        for i in range(self._count):
            self._runners.append(FeedRunner(**self._opts, shard={"index": i, "count": self._count}))
        # One at a time: the first shard writes the layout the others then find.
        try:
            for r in self._runners:
                await r.start()
        except BaseException:
            await asyncio.gather(*[r.stop() for r in self._runners], return_exceptions=True)
            raise

    async def stop(self) -> None:
        await asyncio.gather(*[r.stop() for r in self._runners])

    @property
    def done(self) -> Any:
        return asyncio.gather(*[r.done for r in self._runners if r.done is not None])

    def metrics(self) -> Dict[str, Any]:
        shards = [r.metrics() for r in self._runners]

        def total(k: str) -> Any:
            return sum(m.get(k) for m in shards if js_is_finite(m.get(k)))

        out: Dict[str, Any] = {"shards": self._count, "running": sum(1 for m in shards if m.get("role") == "active")}
        for k in _SUMS:
            out[k] = total(k)
        out["perShard"] = shards
        return out

    def state(self, step: Any) -> _GroupState:
        """The live state of one step over the shards this process runs."""
        return _GroupState(self._runners, step)
