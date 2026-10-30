"""
Closing windows under a claim (broker 2.7, issue #153), without a broker.

The window operators say when the earliest window still open closes; the
runner tracks that per partition, claims the partition when the time comes
and closes it in a cycle that acks the claim's empty batch. The runner tests
use the fake streams server with the two 2.7 routes added.
"""

from __future__ import annotations

import asyncio
import json
from typing import Optional

import httpx
import pytest

from queen.streams.operators import (
    ReduceOperator,
    WindowCronOperator,
    WindowSessionOperator,
    WindowSlidingOperator,
    WindowTumblingOperator,
    state_key_for,
)
from queen.streams.stream import Stream

from .fake_server import FakeStreamsServer, create_fake_source


def _msg(ms: int) -> dict:
    """A processing-time message created at ms."""
    from queen.streams.operators._window_common import to_iso

    return {"createdAt": to_iso(ms), "data": {"n": 1}}


async def _annotate(window, ms_list, key: str = "k") -> list:
    out = []
    for ms in ms_list:
        out.extend(await window.apply({"msg": _msg(ms), "key": key, "value": {"n": 1}}))
    return out


def _counter() -> ReduceOperator:
    return ReduceOperator(lambda acc, v: acc + 1, 0)


# ---------------------------------------------------------------------------
# When to come back: the operators
# ---------------------------------------------------------------------------


class TestNextClose:
    @pytest.mark.asyncio
    async def test_tumbling_is_the_earliest_open_window_end_plus_grace(self):
        w = WindowTumblingOperator(seconds=10, grace_period=2)
        envs = await _annotate(w, [5_000, 15_000])

        def run(clock):
            return _counter().run(
                envelopes=envs,
                loaded_state={},
                stream_time_ms=clock,
                operator_tag=w.operator_tag,
                grace_period_ms=w.grace_period_ms,
            )

        out = await run(9_000)
        assert out["emits"] == []
        assert out["nextCloseMs"] == 12_000, "first window: end 10 s + 2 s grace"

        # The first closed: the second is next.
        out = await run(12_000)
        assert len(out["emits"]) == 1
        assert out["nextCloseMs"] == 22_000

        # Both closed: nothing to come back for.
        out = await run(22_000)
        assert len(out["emits"]) == 2
        assert out["nextCloseMs"] is None

    @pytest.mark.asyncio
    async def test_a_stored_window_the_batch_did_not_touch_still_counts(self):
        w = WindowTumblingOperator(seconds=10)
        stored = {
            state_key_for(w.operator_tag, "1970-01-01T00:00:10.000Z", "k"): {
                "acc": 1,
                "windowStart": 10_000,
                "windowEnd": 20_000,
            }
        }
        # Only the stored window.
        out = await _counter().run(
            envelopes=[], loaded_state=stored, stream_time_ms=15_000, operator_tag=w.operator_tag
        )
        assert out["emits"] == []
        assert out["nextCloseMs"] == 20_000

        # A batch for a later window of another key: the stored one still
        # closes first.
        envs = await _annotate(w, [35_000], key="other")
        out = await _counter().run(
            envelopes=envs, loaded_state=stored, stream_time_ms=15_000, operator_tag=w.operator_tag
        )
        assert out["nextCloseMs"] == 20_000

    @pytest.mark.asyncio
    async def test_sliding_and_cron_windows_report_it_too(self):
        sliding = WindowSlidingOperator(size=10, slide=5)
        envs = await _annotate(sliding, [7_000])  # windows [0, 10) and [5, 15)
        out = await _counter().run(
            envelopes=envs, loaded_state={}, stream_time_ms=7_000, operator_tag=sliding.operator_tag
        )
        assert out["nextCloseMs"] == 10_000

        cron = WindowCronOperator(every="minute")
        envs = await _annotate(cron, [61_000])
        out = await _counter().run(
            envelopes=envs, loaded_state={}, stream_time_ms=61_000, operator_tag=cron.operator_tag
        )
        assert out["nextCloseMs"] == 120_000

    @pytest.mark.asyncio
    async def test_a_session_closes_at_its_last_event_plus_gap_and_grace(self):
        w = WindowSessionOperator(gap=30, grace_period=1)
        envs = await _annotate(w, [0, 10_000], key="u1")
        out = await w.run_session(
            envelopes=envs,
            loaded_state={},
            reducer_fn=lambda acc, v: acc + 1,
            reducer_initial=lambda: 0,
            now_ms=11_000,
        )
        assert out["emits"] == []
        assert out["nextCloseMs"] == 41_000

        # The idle close of the stored session: nothing left open after it.
        stored = {
            w.open_session_state_key("u1"): {"acc": 2, "sessionStart": 0, "lastEventTime": 10_000}
        }
        out = await w.run_session(
            envelopes=[],
            loaded_state=stored,
            reducer_fn=lambda acc, v: acc + 1,
            reducer_initial=lambda: 0,
            now_ms=40_999,
        )
        assert out["emits"] == []
        assert out["nextCloseMs"] == 41_000
        out = await w.run_session(
            envelopes=[],
            loaded_state=stored,
            reducer_fn=lambda acc, v: acc + 1,
            reducer_initial=lambda: 0,
            now_ms=41_000,
        )
        assert len(out["emits"]) == 1
        assert out["nextCloseMs"] is None


# ---------------------------------------------------------------------------
# The runner, against a fake broker with the 2.7 routes
# ---------------------------------------------------------------------------


class FakeClaimServer(FakeStreamsServer):
    """The fake streams server plus POST /streams/v1/claim and
    /streams/v1/state/partitions."""

    def __init__(self, now_ms: int, statuses: Optional[dict] = None, names: Optional[dict] = None):
        super().__init__()
        self.now_ms = now_ms
        self.statuses = statuses or {}
        self.names = names or {}
        self.recorded["claims"] = []
        self.recorded["sweeps"] = []

    def _handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        if path == "/streams/v1/state/partitions":
            self.recorded["sweeps"].append(body)
            prefix = f"{body.get('query_id')}|"
            pids = sorted(
                {k[len(prefix) :].split("|", 1)[0] for k in self.state if k.startswith(prefix)}
            )
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "partitions": [
                        {"partition_id": p, "partition": self.names.get(p, ""), "queue": "src"}
                        for p in pids
                    ],
                    "next": None,
                },
            )
        if path == "/streams/v1/claim":
            self.recorded["claims"].append(body)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "lease_id": "claim-lease",
                    "lease_seconds": body.get("lease_seconds"),
                    "now_ms": self.now_ms,
                    "partitions": [
                        {
                            "partition_id": p,
                            "partition": self.names.get(p, ""),
                            "status": self.statuses.get(p, "claimed"),
                        }
                        for p in body.get("partition_ids", [])
                    ],
                },
            )
        return super()._handler(request)


class FakeQueueRef:
    def __init__(self, name: str):
        self._queue_name = name
        self.queue_name = name


def _session_stream(source):
    return (
        Stream.from_(source)
        .window_session(gap=2, idle_flush_ms=100)
        .aggregate({"count": lambda m: 1})
        .to(FakeQueueRef("sink"))
    )


async def _start_with_state(server, stream, query: str, state: dict):
    # Register the query up front so its state can be seeded before the run
    # starts: the first sweep goes out at once.
    server.queries[query] = {"id": "q-1", "config_hash": stream._compile().config_hash}
    for k, v in state.items():
        server.state[f"q-1|{k}"] = v
    await server.start()
    return await stream.run(query_id=query, url=server.url)


async def _wait(cond, timeout_s: float = 3.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout_s
    while not cond() and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.02)


class TestRunnerClaims:
    @pytest.mark.asyncio
    async def test_a_session_left_in_state_closes_under_a_claim_at_start(self):
        server = FakeClaimServer(now_ms=10_000, names={"pid-1": "trace-a"})
        stream = _session_stream(create_fake_source("src", []))
        key = "sess:2\u001fopen\u001fpid-1"
        handle = await _start_with_state(
            server,
            stream,
            "claims.restart",
            {
                f"pid-1|{key}": {
                    "acc": {"count": 2},
                    "sessionStart": 1_000,
                    "lastEventTime": 2_000,
                },
            },
        )
        try:
            await _wait(lambda: len(server.recorded["cycles"]) >= 1)
            await asyncio.sleep(0.3)
        finally:
            await handle.stop()
            await server.close()

        assert server.recorded["sweeps"][0] == {
            "query_id": "q-1",
            "limit": 1000,
        }, "first sweep: every partition"
        claim = server.recorded["claims"][0]
        assert claim["partition_ids"] == ["pid-1"]
        assert claim["consumer_group"] == "streams.claims.restart"
        assert claim["lease_seconds"] == 30

        assert len(server.recorded["cycles"]) == 1, "closed once, then no longer tracked"
        cycle = server.recorded["cycles"][0]
        assert cycle["ack"] == {
            "transactionId": "",
            "leaseId": "claim-lease",
            "status": "completed",
            "count": 0,
        }
        assert cycle["release_lease"] is True
        assert cycle["state_ops"] == [{"type": "delete", "key": key}]
        assert [(p["partition"], p["payload"]) for p in cycle["push_items"]] == [
            ("trace-a", {"count": 2})
        ]
        assert len(server.recorded["claims"]) == 1
        assert handle.metrics()["flushCyclesTotal"] == 1

    @pytest.mark.asyncio
    async def test_a_window_not_ripe_on_the_brokers_clock_gives_the_lease_back_and_waits(self):
        # The broker's clock says 3 s: the session closes at 2 + 2 = 4 s.
        server = FakeClaimServer(now_ms=3_000)
        stream = _session_stream(create_fake_source("src", []))
        handle = await _start_with_state(
            server,
            stream,
            "claims.notripe",
            {
                "pid-1|sess:2\u001fopen\u001fpid-1": {
                    "acc": {"count": 1},
                    "sessionStart": 2_000,
                    "lastEventTime": 2_000,
                },
            },
        )
        try:
            await _wait(lambda: len(server.recorded["cycles"]) >= 1)
            await asyncio.sleep(0.3)
        finally:
            await handle.stop()
            await server.close()

        cycle = server.recorded["cycles"][0]
        assert cycle["ack"]["count"] == 0 and cycle["ack"]["leaseId"] == "claim-lease"
        assert cycle["state_ops"] == [] and cycle["push_items"] == []
        # Due again at 4 s (an epoch time long past): claimed again each tick.
        assert handle._due["pid-1"]["atMs"] == 4_000

    @pytest.mark.asyncio
    async def test_busy_waits_and_backlog_is_dropped(self):
        server = FakeClaimServer(now_ms=10_000, statuses={"pid-1": "busy", "pid-2": "backlog"})
        stream = _session_stream(create_fake_source("src", []))
        row = {"acc": {"count": 1}, "sessionStart": 1_000, "lastEventTime": 1_000}
        handle = await _start_with_state(
            server,
            stream,
            "claims.busy",
            {
                "pid-1|sess:2\u001fopen\u001fpid-1": row,
                "pid-2|sess:2\u001fopen\u001fpid-2": row,
            },
        )
        try:
            await _wait(lambda: len(server.recorded["claims"]) >= 1)
            await asyncio.sleep(0.5)
        finally:
            await handle.stop()
            await server.close()

        assert server.recorded["cycles"] == []
        assert sorted(server.recorded["claims"][0]["partition_ids"]) == ["pid-1", "pid-2"]
        # busy: back in a second, not on every 100 ms tick. backlog: the next
        # pop delivers its messages, nothing to track.
        assert len(server.recorded["claims"]) == 1
        assert "pid-1" in handle._due and "pid-2" not in handle._due

    @pytest.mark.asyncio
    async def test_an_old_broker_falls_back_to_the_flush_of_recent_partitions(self):
        # The plain fake answers 404 on the 2.7 routes, like a broker before it.
        server = FakeStreamsServer()
        await server.start()
        source = create_fake_source(
            "src",
            [
                [
                    {
                        "id": "m-1",
                        "transactionId": "tx-1",
                        "partitionId": "pid-1",
                        "partition": "trace-a",
                        "leaseId": "pop-lease",
                        "data": {"n": 1},
                        "createdAt": "2026-01-01T10:00:00.000Z",
                    }
                ]
            ],
        )
        handle = await _session_stream(source).run(query_id="claims.old", url=server.url)
        try:
            await _wait(lambda: any(c["ack"] is None for c in server.recorded["cycles"]))
        finally:
            await handle.stop()
            await server.close()

        assert handle._claims == "no"
        assert handle._due == {}
        flushes = [c for c in server.recorded["cycles"] if c["ack"] is None]
        assert len(flushes) == 1, "the old flush closed the session"
        assert flushes[0]["push_items"][0]["payload"] == {"count": 1}
