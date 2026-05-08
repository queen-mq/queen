"""
Shared utilities for streaming integration tests. 1:1 port of test-v2/stream/_helpers.js.

Tests in tests/streams_integration/ run live against a Queen instance.
Each test:
  - generates a unique queue/query name
  - sets up source + sink queues
  - runs a Stream pipeline for a bounded time
  - asserts on the resulting sink-queue contents and/or runner metrics
  - stops the stream
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import time
from typing import Any, Awaitable, Callable, Optional

import httpx


_NAME_COUNTER = [0]
STREAMS_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")


def mk_name(test_name: str, suffix: str = "") -> str:
    """Build a unique queue/query name scoped to a test, prefixed 'test-stream-'."""
    _NAME_COUNTER[0] += 1
    stamp = f"{int(time.time() * 1000):x}"
    slug = re.sub(r"[^a-zA-Z0-9]", "-", str(test_name or "unknown")).lower()
    tail = f"-{suffix}" if suffix else ""
    return f"test-stream-{slug}-{stamp}-{_NAME_COUNTER[0]}{tail}"


async def sleep(ms: float) -> None:
    await asyncio.sleep(ms / 1000.0)


async def wait_for(predicate: Callable[[], Awaitable[Any]], timeout_ms: float = 10_000, interval_ms: float = 100) -> Any:
    start = asyncio.get_event_loop().time()
    last = await predicate()
    while not last and (asyncio.get_event_loop().time() - start) * 1000 < timeout_ms:
        await sleep(interval_ms)
        last = await predicate()
    return last


async def push_spread(client, queue_name: str, items: list) -> None:
    """Push items grouped by partition (one HTTP call per partition)."""
    by_part: dict = {}
    for it in items:
        part = it.get("partition", "Default")
        by_part.setdefault(part, []).append({"data": it["data"]})
    await asyncio.gather(*[
        client.queue(queue_name).partition(part).push(batch) for part, batch in by_part.items()
    ])


async def drain_sink(client, queue_name: str, timeout_ms: float = 5000, group: Optional[str] = None) -> list:
    cg = group or f"drain-{int(time.time()*1000)}-{random.randint(0, 999_999)}"
    out: list = []
    start = asyncio.get_event_loop().time()
    while (asyncio.get_event_loop().time() - start) * 1000 < timeout_ms:
        popped = await client.queue(queue_name).group(cg).batch(1).wait(False).pop()
        if not popped:
            break
        for m in popped:
            out.append(m)
        await client.ack(popped, True, {"group": cg})
    return out


async def drain_until(
    client,
    queue_name: str,
    until: Callable[[list], bool],
    timeout_ms: float = 15_000,
    group: Optional[str] = None,
) -> list:
    cg = group or f"drain-until-{int(time.time()*1000)}-{random.randint(0, 999_999)}"
    out: list = []
    start = asyncio.get_event_loop().time()
    while (asyncio.get_event_loop().time() - start) * 1000 < timeout_ms:
        if until(out):
            break
        popped = await client.queue(queue_name).group(cg).batch(1).wait(True).timeout_millis(500).pop()
        if popped:
            for m in popped:
                out.append(m)
            await client.ack(popped, True, {"group": cg})
    return out


def expect(actual: Any, op: str, expected: Any, what: str) -> dict:
    ok = False
    detail = ""
    if op == "==":
        ok = actual == expected
        detail = f"{what}: expected {expected!r}, got {actual!r}"
    elif op == ">=":
        ok = actual >= expected
        detail = f"{what}: expected >= {expected}, got {actual}"
    elif op == "<=":
        ok = actual <= expected
        detail = f"{what}: expected <= {expected}, got {actual}"
    elif op == ">":
        ok = actual > expected
        detail = f"{what}: expected > {expected}, got {actual}"
    elif op == "<":
        ok = actual < expected
        detail = f"{what}: expected < {expected}, got {actual}"
    elif op == "in":
        ok = actual in expected
        detail = f"{what}: expected one of {expected!r}, got {actual!r}"
    else:
        raise ValueError(f"unknown op: {op}")
    return {"ok": ok, "detail": detail}


def summarise(test_name: str, checks: list) -> dict:
    failed = [c for c in checks if not c["ok"]]
    if not failed:
        return {"success": True, "message": f"{len(checks)} checks passed"}
    return {"success": False, "message": "; ".join(f["detail"] for f in failed)}


async def run_stream_for(stream_factory: Callable[[], Awaitable[Any]], run_ms: float):
    handle = await stream_factory()
    await sleep(run_ms)
    await handle.stop()
    return handle


# ---------------------------------------------------------------------------
# pytest auto-skip if broker not reachable
# ---------------------------------------------------------------------------


async def queen_reachable(url: str = STREAMS_URL) -> bool:
    if url == "skip":
        return False
    try:
        async with httpx.AsyncClient(timeout=2.0) as c:
            r = await c.get(url + "/health")
            return r.is_success
    except Exception:
        return False


def skip_if_no_broker():
    """Decorator that returns a skip-marker for tests when no broker is reachable."""
    import pytest
    return pytest.mark.skipif(
        not asyncio.run(queen_reachable()),
        reason=f"Queen not reachable at {STREAMS_URL} (set QUEEN_URL or start the broker)",
    )
