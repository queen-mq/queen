"""
Event-time tests against the fake streams server. 1:1 port of
eventTime.test.js (3 test cases).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from queen.streams.operators.window_tumbling import WindowTumblingOperator
from queen.streams.stream import Stream

from .fake_server import (
    create_fake_streams_server,
    create_fake_source,
    fake_message,
)


async def run_until_drained(handle, server, delta_cycles: int, timeout_ms: float = 3000) -> None:
    baseline = len(server.recorded["cycles"])
    start = asyncio.get_event_loop().time()
    while (
        len(server.recorded["cycles"]) - baseline < delta_cycles
        and (asyncio.get_event_loop().time() - start) * 1000 < timeout_ms
    ):
        await asyncio.sleep(0.025)
    await handle.stop()


def date_parse_ms(iso: str) -> int:
    s = iso.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class FakeQueueRef:
    def __init__(self, name: str):
        self._queue_name = name
        self.queue_name = name


class TestEventTimeMode:
    @pytest.mark.asyncio
    async def test_window_tumbling_uses_extractor_instead_of_createdAt(self):
        op = WindowTumblingOperator(
            seconds=60,
            event_time=lambda m: m["data"]["eventTs"],
        )
        out = await op.apply({
            "msg": {
                "createdAt": "2026-01-01T10:30:00Z",
                "data": {"eventTs": "2026-01-01T10:00:30Z"},
            },
            "key": "p1",
            "value": {},
        })
        assert out[0]["windowKey"] == "2026-01-01T10:00:00.000Z"
        assert out[0]["eventTimeMs"] == date_parse_ms("2026-01-01T10:00:30Z")

    @pytest.mark.asyncio
    async def test_drops_late_events_against_the_watermark_onLate_drop(self):
        server = await create_fake_streams_server()
        try:
            part_a = "00000000-0000-0000-0000-000000000111"
            source = create_fake_source("events", [
                [
                    fake_message(part_a, "p1",
                                 {"eventTs": "2026-01-01T10:00:00Z", "amount": 5},
                                 "2026-01-01T10:00:00Z"),
                    fake_message(part_a, "p1",
                                 {"eventTs": "2026-01-01T10:00:30Z", "amount": 7},
                                 "2026-01-01T10:01:00Z"),
                    fake_message(part_a, "p1",
                                 {"eventTs": "2026-01-01T10:01:30Z", "amount": 3},
                                 "2026-01-01T10:01:30Z"),
                ]
            ])

            handle = await (
                Stream.from_(source)
                .window_tumbling(
                    seconds=60,
                    event_time=lambda m: m["data"]["eventTs"],
                    allowed_lateness=30,
                    on_late="drop",
                )
                .reduce(lambda acc, m: acc + m["amount"], 0)
                .to(FakeQueueRef("totals"))
                .run(query_id="streams.test.evt_drop", url=server.url)
            )

            await run_until_drained(handle, server, 1)

            assert len(server.recorded["cycles"]) >= 1
            c = server.recorded["cycles"][0]
            wm_op = next((o for o in c["state_ops"] if o["key"] == "__wm__"), None)
            assert wm_op is not None
            assert wm_op["type"] == "upsert"
            assert wm_op["value"]["eventTimeMs"] > 0
        finally:
            await server.close()

    @pytest.mark.asyncio
    async def test_persists_watermark_across_runs_and_drops_late_events_on_resume(self):
        server = await create_fake_streams_server()
        try:
            part_a = "00000000-0000-0000-0000-000000000222"

            def build_stream(source):
                return (
                    Stream.from_(source)
                    .window_tumbling(
                        seconds=60,
                        event_time=lambda m: m["data"]["eventTs"],
                        allowed_lateness=30,
                        on_late="drop",
                    )
                    .reduce(lambda acc, m: acc + m["amount"], 0)
                    .to(FakeQueueRef("totals"))
                )

            source1 = create_fake_source("events", [
                [fake_message(part_a, "p1",
                              {"eventTs": "2026-01-01T11:00:00Z", "amount": 1},
                              "2026-01-01T11:00:00Z")]
            ])
            handle1 = await build_stream(source1).run(
                query_id="streams.test.evt_persist",
                url=server.url,
                reset=True,
            )
            await run_until_drained(handle1, server, 1)

            wm_keys = [k for k in server.state.keys() if k.endswith("|__wm__")]
            assert len(wm_keys) >= 1, "watermark state row must exist after run 1"

            source2 = create_fake_source("events", [
                [fake_message(part_a, "p1",
                              {"eventTs": "2026-01-01T10:00:00Z", "amount": 999},
                              "2026-01-01T11:01:00Z")]
            ])
            handle2 = await build_stream(source2).run(
                query_id="streams.test.evt_persist",
                url=server.url,
            )
            await run_until_drained(handle2, server, 2, timeout_ms=1500)

            m = handle2.metrics()
            assert m.get("lateEventsTotal", 0) >= 1
        finally:
            await server.close()
