"""
Unit tests for operator semantics. 1:1 port of operators.test.js — exercises
the operator classes directly (no HTTP, no Queen needed).

Total: 29 test cases across 9 describe-style classes.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from queen.streams.operators import (
    AggregateOperator,
    FilterOperator,
    FlatMapOperator,
    KeyByOperator,
    MapOperator,
    ReduceOperator,
    WindowCronOperator,
    WindowSessionOperator,
    WindowSlidingOperator,
    WindowTumblingOperator,
    state_key_for,
)
from queen.streams.util.config_hash import config_hash_of


def env(msg: dict, key: str = "p1") -> dict:
    """Tiny envelope helper mirroring the JS test."""
    return {"msg": msg, "key": key, "value": msg.get("data")}


def date_parse_ms(iso: str) -> int:
    s = iso.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# ===========================================================================
# MapOperator (2 tests)
# ===========================================================================
class TestMapOperator:
    @pytest.mark.asyncio
    async def test_transforms_the_envelope_value(self):
        op = MapOperator(lambda m, ctx=None: {"doubled": m["data"]["x"] * 2})
        out = await op.apply(env({"data": {"x": 3}}))
        assert len(out) == 1
        assert out[0]["value"] == {"doubled": 6}

    @pytest.mark.asyncio
    async def test_supports_async_fns(self):
        async def fn(m, ctx=None):
            return {"x": m["data"]["x"] + 1}
        op = MapOperator(fn)
        out = await op.apply(env({"data": {"x": 4}}))
        assert out[0]["value"] == {"x": 5}


# ===========================================================================
# FilterOperator (1 test)
# ===========================================================================
class TestFilterOperator:
    @pytest.mark.asyncio
    async def test_drops_envelopes_where_predicate_is_false(self):
        op = FilterOperator(lambda m, ctx=None: m["data"]["x"] > 10)
        assert await op.apply(env({"data": {"x": 5}})) == []
        assert len(await op.apply(env({"data": {"x": 20}}))) == 1


# ===========================================================================
# FlatMapOperator (2 tests)
# ===========================================================================
class TestFlatMapOperator:
    @pytest.mark.asyncio
    async def test_emits_zero_or_more_outputs(self):
        op = FlatMapOperator(lambda m, ctx=None: list(range(m["data"]["n"])))
        assert len(await op.apply(env({"data": {"n": 3}}))) == 3
        assert len(await op.apply(env({"data": {"n": 0}}))) == 0

    @pytest.mark.asyncio
    async def test_rejects_non_array_returns(self):
        op = FlatMapOperator(lambda m, ctx=None: "not an array")
        with pytest.raises(TypeError, match=r"must return a list"):
            await op.apply(env({"data": {}}))


# ===========================================================================
# KeyByOperator (2 tests)
# ===========================================================================
class TestKeyByOperator:
    @pytest.mark.asyncio
    async def test_overrides_the_envelope_key(self):
        op = KeyByOperator(lambda m: m["data"]["userId"])
        out = await op.apply(env({"data": {"userId": "u-42"}}, "partition-A"))
        assert out[0]["key"] == "u-42"

    @pytest.mark.asyncio
    async def test_coerces_keys_to_string(self):
        op = KeyByOperator(lambda m: m["data"]["id"])
        out = await op.apply(env({"data": {"id": 7}}))
        assert out[0]["key"] == "7"
        assert isinstance(out[0]["key"], str)


# ===========================================================================
# WindowTumblingOperator (2 tests)
# ===========================================================================
class TestWindowTumblingOperator:
    @pytest.mark.asyncio
    async def test_annotates_envelope_with_window_fields(self):
        op = WindowTumblingOperator(seconds=60)
        out = await op.apply({
            "msg": {"createdAt": "2026-01-01T10:01:30.000Z", "data": {}},
            "key": "p1",
            "value": {},
        })
        e = out[0]
        assert e["windowStart"] == date_parse_ms("2026-01-01T10:01:00.000Z")
        assert e["windowEnd"] == date_parse_ms("2026-01-01T10:02:00.000Z")
        assert e["windowKey"] == "2026-01-01T10:01:00.000Z"

    @pytest.mark.asyncio
    async def test_throws_on_missing_or_invalid_createdAt(self):
        op = WindowTumblingOperator(seconds=60)
        with pytest.raises(RuntimeError, match=r"could not determine timestamp"):
            await op.apply({"msg": {"data": {}}, "key": "p1", "value": {}})
        with pytest.raises(RuntimeError, match=r"could not determine timestamp"):
            await op.apply({"msg": {"createdAt": "nope", "data": {}}, "key": "p1", "value": {}})


# ===========================================================================
# ReduceOperator (5 tests)
# ===========================================================================
TAG = "tumb:60"


def reduce_envelope(key: str, ts_iso: str, value):
    ws = (date_parse_ms(ts_iso) // 60_000) * 60_000
    iso_ws = (
        datetime.fromtimestamp(ws / 1000.0, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    return {
        "key": key,
        "windowStart": ws,
        "windowEnd": ws + 60_000,
        "windowKey": iso_ws,
        "operatorTag": TAG,
        "value": value,
    }


class TestReduceOperator:
    @pytest.mark.asyncio
    async def test_accumulates_within_a_window_and_emits_closed_windows(self):
        op = ReduceOperator(lambda acc, v: acc + v, 0)
        envelopes = [
            reduce_envelope("A", "2026-01-01T10:00:01Z", 1),
            reduce_envelope("A", "2026-01-01T10:00:30Z", 2),
            reduce_envelope("A", "2026-01-01T10:01:05Z", 5),
        ]
        result = await op.run(
            envelopes=envelopes,
            loaded_state={},
            operator_tag=TAG,
            stream_time_ms=date_parse_ms("2026-01-01T10:01:05Z"),
        )
        closed = result["emits"]
        assert len(closed) == 1
        assert closed[0]["key"] == "A"
        assert closed[0]["value"] == 3
        assert closed[0]["windowKey"] == "2026-01-01T10:00:00.000Z"

        upserts = [o for o in result["stateOps"] if o["type"] == "upsert"]
        deletes = [o for o in result["stateOps"] if o["type"] == "delete"]
        assert len(upserts) == 1
        assert len(deletes) == 0
        assert upserts[0]["value"]["acc"] == 5

    @pytest.mark.asyncio
    async def test_emits_a_delete_for_a_seeded_window_when_its_end_has_passed(self):
        op = ReduceOperator(lambda acc, v: acc + v, 0)
        ws00 = date_parse_ms("2026-01-01T10:00:00Z")
        we00 = date_parse_ms("2026-01-01T10:01:00Z")
        loaded = {
            state_key_for(TAG, "2026-01-01T10:00:00.000Z", "A"): {
                "acc": 42, "windowStart": ws00, "windowEnd": we00,
            }
        }
        envelopes = [reduce_envelope("A", "2026-01-01T10:01:05Z", 5)]
        result = await op.run(
            envelopes=envelopes,
            loaded_state=loaded,
            operator_tag=TAG,
            stream_time_ms=date_parse_ms("2026-01-01T10:01:05Z"),
        )
        closed = result["emits"]
        assert len(closed) == 1
        assert closed[0]["value"] == 42
        assert closed[0]["windowKey"] == "2026-01-01T10:00:00.000Z"
        upserts = [o for o in result["stateOps"] if o["type"] == "upsert"]
        deletes = [o for o in result["stateOps"] if o["type"] == "delete"]
        assert len(deletes) == 1
        assert len(upserts) == 1
        assert upserts[0]["value"]["acc"] == 5

    @pytest.mark.asyncio
    async def test_respects_loaded_state_across_cycles(self):
        op = ReduceOperator(lambda acc, v: acc + v, 0)
        envelopes = [reduce_envelope("A", "2026-01-01T10:00:30Z", 5)]
        loaded = {
            state_key_for(TAG, "2026-01-01T10:00:00.000Z", "A"): {
                "acc": 10,
                "windowStart": date_parse_ms("2026-01-01T10:00:00Z"),
                "windowEnd": date_parse_ms("2026-01-01T10:01:00Z"),
            }
        }
        result = await op.run(
            envelopes=envelopes,
            loaded_state=loaded,
            operator_tag=TAG,
            stream_time_ms=date_parse_ms("2026-01-01T10:00:30Z"),
        )
        assert len(result["emits"]) == 0
        upsert = next((o for o in result["stateOps"] if o["type"] == "upsert"), None)
        assert upsert is not None
        assert upsert["value"]["acc"] == 15

    @pytest.mark.asyncio
    async def test_respects_grace_period_keeps_window_open_during_grace(self):
        op = ReduceOperator(lambda acc, v: acc + v, 0)
        ws = date_parse_ms("2026-01-01T10:00:00Z")
        we = date_parse_ms("2026-01-01T10:01:00Z")
        loaded = {
            state_key_for(TAG, "2026-01-01T10:00:00.000Z", "A"): {
                "acc": 42, "windowStart": ws, "windowEnd": we,
            }
        }
        result = await op.run(
            envelopes=[],
            loaded_state=loaded,
            operator_tag=TAG,
            grace_period_ms=30_000,
            stream_time_ms=date_parse_ms("2026-01-01T10:01:05Z"),
        )
        assert len(result["emits"]) == 0
        assert len(result["stateOps"]) == 0

    @pytest.mark.asyncio
    async def test_respects_grace_period_closes_after_grace(self):
        op = ReduceOperator(lambda acc, v: acc + v, 0)
        ws = date_parse_ms("2026-01-01T10:00:00Z")
        we = date_parse_ms("2026-01-01T10:01:00Z")
        loaded = {
            state_key_for(TAG, "2026-01-01T10:00:00.000Z", "A"): {
                "acc": 42, "windowStart": ws, "windowEnd": we,
            }
        }
        result = await op.run(
            envelopes=[],
            loaded_state=loaded,
            operator_tag=TAG,
            grace_period_ms=30_000,
            stream_time_ms=date_parse_ms("2026-01-01T10:01:35Z"),
        )
        assert len(result["emits"]) == 1
        assert result["emits"][0]["value"] == 42

    @pytest.mark.asyncio
    async def test_ignores_state_rows_owned_by_another_operator(self):
        op = ReduceOperator(lambda acc, v: acc + v, 0)
        ws = date_parse_ms("2026-01-01T10:00:00Z")
        we = date_parse_ms("2026-01-01T10:01:00Z")
        loaded = {
            state_key_for("slide:60:10", "2026-01-01T10:00:00.000Z", "A"): {
                "acc": 99, "windowStart": ws, "windowEnd": we,
            },
            "__wm__": {"eventTimeMs": 1234},
        }
        result = await op.run(
            envelopes=[],
            loaded_state=loaded,
            operator_tag=TAG,
            stream_time_ms=date_parse_ms("2026-01-01T10:05:00Z"),
        )
        assert len(result["emits"]) == 0
        assert len(result["stateOps"]) == 0


# ===========================================================================
# AggregateOperator (1 test)
# ===========================================================================
class TestAggregateOperator:
    @pytest.mark.asyncio
    async def test_computes_count_sum_avg_min_max(self):
        op = AggregateOperator({
            "count": lambda m: 1,
            "sum":   lambda m: m["amount"],
            "avg":   lambda m: m["amount"],
            "min":   lambda m: m["amount"],
            "max":   lambda m: m["amount"],
        })
        envs = [
            reduce_envelope("A", "2026-01-01T10:00:01Z", {"amount": 10}),
            reduce_envelope("A", "2026-01-01T10:00:30Z", {"amount": 30}),
            reduce_envelope("A", "2026-01-01T10:00:55Z", {"amount": 20}),
            reduce_envelope("A", "2026-01-01T10:01:05Z", {"amount": 99}),
        ]
        r = await op.run(
            envelopes=envs,
            loaded_state={},
            operator_tag=TAG,
            stream_time_ms=date_parse_ms("2026-01-01T10:01:05Z"),
        )
        assert len(r["emits"]) == 1
        v = r["emits"][0]["value"]
        assert v["count"] == 3
        assert v["sum"] == 60
        assert v["avg"] == 20
        assert v["min"] == 10
        assert v["max"] == 30


# ===========================================================================
# WindowSlidingOperator (2 tests)
# ===========================================================================
class TestWindowSlidingOperator:
    @pytest.mark.asyncio
    async def test_emits_n_envelopes_per_event_for_size_60_slide_10(self):
        op = WindowSlidingOperator(size=60, slide=10)
        out = await op.apply({
            "msg": {"createdAt": "2026-01-01T10:00:35.000Z", "data": {}},
            "key": "p1", "value": {},
        })
        assert len(out) == 6
        for annot in out:
            assert annot["windowStart"] <= date_parse_ms("2026-01-01T10:00:35Z")
            assert annot["windowEnd"] - annot["windowStart"] == 60_000
            assert annot["operatorTag"] == "slide:60:10"
        assert len({o["windowKey"] for o in out}) == 6

    def test_rejects_size_not_a_multiple_of_slide(self):
        with pytest.raises(ValueError, match=r"must be an integer multiple"):
            WindowSlidingOperator(size=60, slide=7)


# ===========================================================================
# WindowCronOperator (5 tests)
# ===========================================================================
class TestWindowCronOperator:
    @pytest.mark.asyncio
    async def test_aligns_minute_boundaries(self):
        op = WindowCronOperator(every="minute")
        out = await op.apply({
            "msg": {"createdAt": "2026-01-01T10:00:35.123Z", "data": {}},
            "key": "p1", "value": {},
        })
        assert len(out) == 1
        assert out[0]["windowKey"] == "2026-01-01T10:00:00.000Z"
        assert out[0]["operatorTag"] == "cron:minute"

    @pytest.mark.asyncio
    async def test_aligns_hour_boundaries(self):
        op = WindowCronOperator(every="hour")
        out = await op.apply({
            "msg": {"createdAt": "2026-01-01T10:30:35.000Z", "data": {}},
            "key": "p1", "value": {},
        })
        assert out[0]["windowKey"] == "2026-01-01T10:00:00.000Z"

    @pytest.mark.asyncio
    async def test_aligns_day_boundaries_to_utc_midnight(self):
        op = WindowCronOperator(every="day")
        out = await op.apply({
            "msg": {"createdAt": "2026-01-01T22:30:35.000Z", "data": {}},
            "key": "p1", "value": {},
        })
        assert out[0]["windowKey"] == "2026-01-01T00:00:00.000Z"

    @pytest.mark.asyncio
    async def test_aligns_week_boundaries_to_monday_00_utc(self):
        op = WindowCronOperator(every="week")
        # 2026-01-07 is a Wednesday; window should start on Mon 2026-01-05.
        out = await op.apply({
            "msg": {"createdAt": "2026-01-07T12:00:00.000Z", "data": {}},
            "key": "p1", "value": {},
        })
        assert out[0]["windowKey"] == "2026-01-05T00:00:00.000Z"

    def test_rejects_unknown_every(self):
        with pytest.raises(ValueError, match=r"every must be"):
            WindowCronOperator(every="fortnight")


# ===========================================================================
# WindowSessionOperator (3 tests)
# ===========================================================================
class TestWindowSessionOperator:
    @pytest.mark.asyncio
    async def test_extends_a_session_within_gap(self):
        op = WindowSessionOperator(gap=30)
        reducer_fn = lambda acc, m: acc + m["x"]
        reducer_initial = lambda: 0

        t1 = date_parse_ms("2026-01-01T10:00:00Z")
        t2 = date_parse_ms("2026-01-01T10:00:10Z")
        envelopes = [
            {"key": "A", "eventTimeMs": t1, "value": {"x": 1}},
            {"key": "A", "eventTimeMs": t2, "value": {"x": 2}},
        ]
        result = await op.run_session(
            envelopes=envelopes,
            loaded_state={},
            reducer_fn=reducer_fn,
            reducer_initial=reducer_initial,
            now_ms=t2,
        )
        assert len(result["emits"]) == 0
        upsert = next((o for o in result["stateOps"] if o["type"] == "upsert"), None)
        assert upsert is not None
        assert upsert["value"]["acc"] == 3
        assert upsert["value"]["sessionStart"] == t1
        assert upsert["value"]["lastEventTime"] == t2

    @pytest.mark.asyncio
    async def test_closes_a_session_when_gap_is_exceeded_by_next_event(self):
        op = WindowSessionOperator(gap=30)
        reducer_fn = lambda acc, m: acc + m["x"]
        reducer_initial = lambda: 0
        t1 = date_parse_ms("2026-01-01T10:00:00Z")
        t2 = date_parse_ms("2026-01-01T10:01:00Z")
        envelopes = [
            {"key": "A", "eventTimeMs": t1, "value": {"x": 1}},
            {"key": "A", "eventTimeMs": t2, "value": {"x": 5}},
        ]
        result = await op.run_session(
            envelopes=envelopes,
            loaded_state={},
            reducer_fn=reducer_fn,
            reducer_initial=reducer_initial,
            now_ms=t2,
        )
        assert len(result["emits"]) == 1
        assert result["emits"][0]["value"] == 1
        upsert = next((o for o in result["stateOps"] if o["type"] == "upsert"), None)
        assert upsert is not None
        assert upsert["value"]["acc"] == 5
        assert upsert["value"]["sessionStart"] == t2

    @pytest.mark.asyncio
    async def test_idle_flushes_a_seeded_session_whose_gap_has_expired(self):
        op = WindowSessionOperator(gap=30)
        reducer_fn = lambda acc, m: acc + m["x"]
        reducer_initial = lambda: 0
        t1 = date_parse_ms("2026-01-01T10:00:00Z")
        flush_time = date_parse_ms("2026-01-01T10:02:00Z")
        sk = op.open_session_state_key("A")
        loaded = {sk: {"acc": 99, "sessionStart": t1, "lastEventTime": t1}}
        result = await op.run_session(
            envelopes=[],
            loaded_state=loaded,
            reducer_fn=reducer_fn,
            reducer_initial=reducer_initial,
            now_ms=flush_time,
        )
        assert len(result["emits"]) == 1
        assert result["emits"][0]["value"] == 99
        delete = next((o for o in result["stateOps"] if o["type"] == "delete"), None)
        assert delete is not None
        assert delete["key"] == sk


# ===========================================================================
# config_hash_of (3 tests)
# ===========================================================================
class TestConfigHashOf:
    def test_is_stable_for_the_same_chain_shape(self):
        ops_a = [
            MapOperator(lambda m: m),
            WindowTumblingOperator(seconds=60),
            AggregateOperator({"count": lambda m: 1}),
        ]
        ops_b = [
            MapOperator(lambda m: None),  # different fn, same kind
            WindowTumblingOperator(seconds=60),
            AggregateOperator({"count": lambda m: 1}),
        ]
        assert config_hash_of(ops_a) == config_hash_of(ops_b)

    def test_differs_when_window_size_changes(self):
        a = config_hash_of([WindowTumblingOperator(seconds=60)])
        b = config_hash_of([WindowTumblingOperator(seconds=30)])
        assert a != b

    def test_differs_when_chain_reorders(self):
        a = config_hash_of([MapOperator(lambda m: None), FilterOperator(lambda m: True)])
        b = config_hash_of([FilterOperator(lambda m: True), MapOperator(lambda m: None)])
        assert a != b
