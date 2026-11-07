"""Port of the JS SDK's test-v2/feed-unit/feedSteps.test.js: two positions and
a replay, many stateful steps, joins of many sides and gathers, table joins,
shards, live reads, and a crash anywhere."""

import math
import time

import pytest

from queen.streams.runtime.feed.snapshot import decode_snapshot, encode_snapshot
from queen.streams.runtime.feed.state import fnv1a32, shard_of
from queen.streams.runtime.feed_runner import POSITION_NAMESPACE as NS
from queen.streams.runtime.feed_runner import output_prefix
from queen.streams.stream import Stream
from queen.streams.util.js import js_iso

from .fake_broker import T0, FakeBroker, at, crash, fake_queen, fast, payloads, quiet, until, wait

MIN0 = math.floor(T0 / 60000) * 60000


def per_minute(q, **source):
    return (Stream.from_feed(q, from_="earliest", queues=["orders"], **source)
            .window_tumbling(seconds=60).aggregate({"count": lambda: 1}).name("perMinute").to(q.queue("counts")))


def position_of(b, key):
    v = b.kv_value(NS, key)
    return (v or {}).get("position", 0)


# ---- two positions and a replay ---------------------------------------------------------

async def test_a_commit_writes_the_position_only_no_state_while_windows_stay_open():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(3):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": i}})
    runner = await per_minute(queen).run(query_id="pos-only", **fast(now=lambda: b.now_ms))
    await until(lambda: position_of(b, "pos-only") > 3)
    stored = b.kv_value(NS, "pos-only")
    assert stored["snapshot"]["chunks"] == 0, "no chunks: the state is not written"
    assert stored["snapshot"]["position"] == 1, "the snapshot is the stream's start, where the state was empty"
    assert [k for k in b.kv_keys(NS, "pos-only#") if k != "pos-only#about"] == [], "nothing but the position record (and the description)"
    assert runner.metrics()["stateKeys"] == 1, "the open window is in memory"


async def test_after_a_crash_the_next_runner_reads_again_from_the_snapshot_nothing_lost_nothing_sent_twice():
    b = FakeBroker()
    qa = fake_queen(b, "a")
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    at(b, 61000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    # No snapshot due in this test: the next runner must read the feed again.
    opts = dict(**fast(now=lambda: b.now_ms, lock_ttl="1s", snapshot_every_millis=3600000), **quiet())
    a = await per_minute(qa).run(query_id="crash", **opts)
    await until(lambda: len(b.messages("counts")) >= 1 and position_of(b, "crash") > 3)
    assert payloads(b, "counts") == [{"count": 2}], "minute 0 closed before the crash"
    crash(qa)
    commits_at_crash = a.metrics()["commits"]
    at(b, 62000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    second = await per_minute(fake_queen(b, "b")).run(query_id="crash", **opts)
    await until(lambda: second.metrics()["role"] == "active", 6000, "the takeover")
    assert second.metrics()["replays"] >= 1, "it read the feed again"
    assert second.metrics()["replayEntries"] >= 3
    at(b, 121000)
    b.push({"queue": "orders", "partition": "other", "data": {}})
    await until(lambda: len(b.messages("counts")) >= 2)
    await second.stop()
    assert payloads(b, "counts") == [{"count": 2}, {"count": 2}], "minute 0 once, minute 1 whole"
    assert a.metrics()["commits"] == commits_at_crash, "the crashed process committed nothing more"


async def test_a_snapshot_is_a_full_layer_then_deltas_only_named_layers_stay_and_a_restart_loads_them_and_the_tail():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(20):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {}})
    runner = await per_minute(queen).run(query_id="snapdue", **fast(now=lambda: b.now_ms, snapshot_max_entries=2))
    await until(lambda: runner.metrics()["snapshots"] >= 1 and runner.metrics()["entriesRead"] >= 20)
    first = b.kv_value(NS, "snapdue")["snapshot"]
    assert len(first["layers"]) == 1
    assert first["layers"][0].get("full"), "the first snapshot is a full layer"
    # Two more messages for c0: one key changed, so the next snapshot is a delta of it on the full layer.
    for _ in range(2):
        b.push({"queue": "orders", "partition": "c0", "data": {}})
    await until(lambda: len(b.kv_value(NS, "snapdue")["snapshot"].get("layers") or []) >= 2)
    second = b.kv_value(NS, "snapdue")["snapshot"]
    assert second["layers"][0]["sid"] == first["layers"][0]["sid"], "the full layer is still the one under it"
    assert not second["layers"][1].get("full"), "a delta"
    assert second["layers"][1]["bytes"] < second["layers"][0]["bytes"] / 5, "the delta holds the changed key only"
    await runner.stop()
    stored = b.kv_value(NS, "snapdue")
    named = {layer["sid"] for layer in stored["snapshot"]["layers"]}
    keys = b.kv_keys(NS, "snapdue#snap#")
    assert keys and all(k[len("snapdue#snap#"):k.rfind("#")] in named for k in keys), "only the chunks of named layers are in KV"
    again = await per_minute(fake_queen(b, "again")).run(query_id="snapdue", **fast(now=lambda: b.now_ms))
    assert again.metrics()["stateKeys"] == 20, "the twenty open windows came back"
    at(b, 61000)
    b.push({"queue": "orders", "partition": "late", "data": {}})
    await until(lambda: len(b.messages("counts")) >= 20)
    await again.stop()
    counts = {e["partition"]: e["payload"]["count"] for e in b.messages("counts")}
    assert counts == {f"c{i}": (3 if i == 0 else 1) for i in range(20)}


async def test_a_state_that_did_not_change_moves_the_snapshot_with_the_position_writing_no_chunk():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders", "noise"]).window_tumbling(seconds=60)
                    .aggregate({"count": lambda: 1}).name("perMinute").to(queen.queue("counts"))
                    .run(query_id="moved", **fast(now=lambda: b.now_ms, snapshot_max_entries=1, position_commit_millis=0)))
    await until(lambda: runner.metrics()["snapshots"] >= 1)
    sid = b.kv_value(NS, "moved")["snapshot"]["id"]
    snapshots = runner.metrics()["snapshots"]
    for _ in range(3):
        b.push({"queue": "other-queue", "partition": "x", "data": {}})
    end = b.applied + 1
    await until(lambda: position_of(b, "moved") >= end)
    stored = b.kv_value(NS, "moved")
    assert stored["snapshot"]["id"] == sid, "the same chunks"
    assert stored["snapshot"]["position"] == stored["position"], "named at the new position"
    assert runner.metrics()["snapshots"] == snapshots, "nothing written"


async def test_an_empty_state_is_a_snapshot_of_no_chunks_and_the_old_chunks_go():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    runner = await per_minute(queen).run(query_id="empty", **fast(now=lambda: b.now_ms, snapshot_max_entries=1))
    await until(lambda: runner.metrics()["snapshots"] >= 1 and len(b.kv_keys(NS, "empty#snap#")) >= 1)
    at(b, 61000)
    await until(lambda: len(b.messages("counts")) >= 1, 3000, "the window to close on the clock")
    await until(lambda: b.kv_value(NS, "empty")["snapshot"]["chunks"] == 0 and len(b.kv_keys(NS, "empty#snap#")) == 0, 3000)
    assert runner.metrics()["stateKeys"] == 0


async def test_a_state_of_thousands_of_keys_in_many_chunks_comes_back_whole_after_a_crash():
    b = FakeBroker()
    qa = fake_queen(b, "a")
    at(b, 1000)
    n = 1500
    for i in range(0, n, 50):
        b.push([{"queue": "orders", "partition": f"k{i + j}", "data": {"pad": "x" * 40}} for j in range(50)])
    a = await per_minute(qa).run(query_id="big", **fast(now=lambda: b.now_ms, lock_ttl="1s", snapshot_max_entries=100, snapshot_chunk_chars=2000), **quiet())
    await until(lambda: a.metrics()["entriesRead"] >= n and a.metrics()["snapshots"] >= 1)
    await until(lambda: b.kv_value(NS, "big")["snapshot"]["chunks"] > 3, 3000, "a snapshot of several chunks")
    crash(qa)
    second = await per_minute(fake_queen(b, "b")).run(query_id="big", **fast(now=lambda: b.now_ms, lock_ttl="1s"), **quiet())
    await until(lambda: second.metrics()["role"] == "active", 6000)
    # The snapshot, the replay after it, and whatever was not committed yet, read live.
    await until(lambda: second.metrics()["stateKeys"] == n, 6000, "every key back")
    at(b, 61000)
    b.push({"queue": "orders", "partition": "flush", "data": {}})
    await until(lambda: len(b.messages("counts")) >= n, 6000)
    await second.stop()
    counts = b.messages("counts")
    assert len(counts) == n, "one result per key, none twice"
    assert len({e["partition"] for e in counts}) == n
    assert all(e["payload"]["count"] == 1 for e in counts)


async def test_a_record_of_the_bucket_format_is_read_once_and_carried_on_as_snapshots():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    pos = b.push({"queue": "orders", "partition": "c1", "data": {}})
    chain_hash = per_minute(queen)._compile_feed().chain_hash
    sk = f"w\u001f{MIN0}\u001fc1"
    b.kv_put(NS, "legacy", {"position": pos + 1, "queryId": "legacy", "epoch": 0, "buckets": 2, "hash": chain_hash, "clock": T0 + 1000})
    b.kv_put(NS, "legacy#0", {"e": {sk: {"key": "c1", "start": MIN0, "end": MIN0 + 60000, "due": MIN0 + 60000, "acc": {"count": 5}}}})
    b.kv_put(NS, "legacy#1", {"e": {}})
    runner = await per_minute(queen).run(query_id="legacy", **fast(now=lambda: b.now_ms))
    at(b, 2000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    await until(lambda: ((b.kv_value(NS, "legacy") or {}).get("snapshot") or {}).get("chunks", 0) >= 1)
    await until(lambda: all(k.startswith("legacy#snap#") or k == "legacy#about" for k in b.kv_keys(NS, "legacy#")), 2000, "the buckets to go")
    at(b, 61000)
    b.push({"queue": "orders", "partition": "c2", "data": {}})
    await until(lambda: len(b.messages("counts")) >= 1)
    await runner.stop()
    assert payloads(b, "counts")[0] == {"count": 6}, "the 5 from the buckets and the new one"


def test_snapshots_encode_and_decode():
    dump = {"steps": [{"name": "s", "kind": "window", "entries": [["a", {"x": "é" * 3000}], ["b", [1, 2, None]]]}]}
    out = encode_snapshot(dump, 1024)
    assert len(out["chunks"]) >= 1
    assert out["bytes"] > 3000
    assert decode_snapshot(out["chunks"])["steps"] == dump["steps"]


# ---- many stateful steps -----------------------------------------------------------------

async def test_a_windows_results_feed_a_second_window():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    b.push({"queue": "orders", "partition": "c2", "data": {}})
    at(b, 61000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders"])
                    .window_tumbling(seconds=60).aggregate({"count": lambda: 1}).name("perKey")
                    .key_by(lambda v: "all")
                    .window_tumbling(seconds=120).aggregate({"total": lambda v: v["count"], "keys": lambda v: 1}).name("total")
                    .to(queen.queue("totals")).run(query_id="two-windows", **fast(now=lambda: b.now_ms)))
    at(b, 121000)
    b.push({"queue": "orders", "partition": "c3", "data": {}})
    await until(lambda: len(b.messages("totals")) >= 1)
    await runner.stop()
    assert payloads(b, "totals") == [{"total": 3, "keys": 2}]
    assert b.messages("totals")[0]["partition"] == "all"
    assert [s["name"] for s in runner.metrics()["steps"]] == ["perKey", "total"]


async def test_a_patterns_matches_feed_a_window_and_a_windows_results_feed_a_pattern():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000); b.push({"queue": "orders", "partition": "o1", "data": {}})
    at(b, 2000); b.push({"queue": "payments", "partition": "o1", "data": {}})
    at(b, 3000); b.push({"queue": "orders", "partition": "o2", "data": {}})
    at(b, 4000); b.push({"queue": "payments", "partition": "o2", "data": {}})
    at(b, 5000); b.push({"queue": "orders", "partition": "o3", "data": {}})
    paid = await (Stream.from_feed(queen, from_="earliest", queues=["orders", "payments"])
                  .pattern(start=lambda m: m["queue"] == "orders", then=lambda m: m["queue"] == "payments", within="15m",
                           on_match=lambda o: {"order": o["partition"]})
                  .key_by(lambda v: "all").window_tumbling(seconds=60).aggregate({"paid": lambda: 1})
                  .to(queen.queue("paid-per-minute")).run(query_id="pattern-window", **fast(now=lambda: b.now_ms)))
    # A burst per tenant, then calm: a window's results are the pattern's messages.
    bursts = await (Stream.from_feed(queen, from_="earliest", queues=["clicks"])
                    .window_tumbling(seconds=60).aggregate({"count": lambda: 1})
                    .pattern(start=lambda m: m["data"]["count"] >= 2, then=lambda m: m["data"]["count"] < 2, within="5m",
                             on_match=lambda start, calm: {"tenant": start["partition"], "from": start["data"]["count"], "to": calm["data"]["count"]})
                    .to(queen.queue("calmed")).run(query_id="window-pattern", **fast(now=lambda: b.now_ms)))
    at(b, 6000); b.push({"queue": "clicks", "partition": "t1", "data": {}})
    at(b, 7000); b.push({"queue": "clicks", "partition": "t1", "data": {}})
    at(b, 61000); b.push({"queue": "clicks", "partition": "t1", "data": {}})
    at(b, 121000); b.push({"queue": "clicks", "partition": "x", "data": {}})
    await until(lambda: len(b.messages("paid-per-minute")) >= 1 and len(b.messages("calmed")) >= 1)
    await paid.stop()
    await bursts.stop()
    assert payloads(b, "paid-per-minute") == [{"paid": 2}]
    assert payloads(b, "calmed") == [{"tenant": "t1", "from": 2, "to": 1}]


async def test_a_chain_of_a_table_join_a_three_sided_join_and_a_window_replays_after_a_crash_to_the_same_results():
    def chain(q):
        return (Stream.from_feed(q, from_="earliest", queues=["orders", "payments", "shipments"])
                .join_table(queue="carriers", on=lambda m: m["data"].get("carrier"),
                            select=lambda v, row: {**v, "speed": (row or {}).get("speed", "unknown") if isinstance(row, dict) else "unknown"})
                .join(sides={"order": "orders", "paid": "payments", "sent": "shipments"}, within="10m",
                      # A join keeps the message's payload; keep the table-joined value instead.
                      keep=lambda m, value: value,
                      select=lambda got, key: {"order": key, "speed": got["sent"]["data"]["speed"]})
                .key_by(lambda v: "all")
                .window_tumbling(seconds=60).aggregate({"done": lambda: 1, "fast": lambda v: 1 if v["speed"] == "fast" else 0})
                .to(q.queue("done-per-minute")))

    def load(b):
        at(b, 500); b.push({"queue": "carriers", "partition": "dhl", "data": {"speed": "fast"}})
        for i in range(6):
            at(b, 1000 + i * 100)
            b.push({"queue": "shipments", "partition": f"o{i}", "data": {"carrier": "dhl" if i % 2 else "ups"}})
            b.push({"queue": "orders", "partition": f"o{i}", "data": {}})
            if i != 5:
                b.push({"queue": "payments", "partition": f"o{i}", "data": {}})

    # Once without a crash, for the expected results.
    ref = FakeBroker()
    load(ref)
    r = await chain(fake_queen(ref)).run(query_id="steps", **fast(now=lambda: ref.now_ms))
    at(ref, 61000); ref.push({"queue": "orders", "partition": "flush", "data": {}})
    await until(lambda: len(ref.messages("done-per-minute")) >= 1)
    await r.stop()
    assert payloads(ref, "done-per-minute") == [{"done": 5, "fast": 2}]
    # Again, with a crash in the middle.
    b = FakeBroker()
    load(b)
    qa = fake_queen(b, "a")
    a = await chain(qa).run(query_id="steps", **fast(now=lambda: b.now_ms, lock_ttl="1s", max_records=4), **quiet())
    await until(lambda: a.metrics()["entriesRead"] >= 8)
    crash(qa)
    second = await chain(fake_queen(b, "b")).run(query_id="steps", **fast(now=lambda: b.now_ms, lock_ttl="1s"), **quiet())
    await until(lambda: second.metrics()["role"] == "active", 6000)
    at(b, 61000); b.push({"queue": "orders", "partition": "flush", "data": {}})
    await until(lambda: len(b.messages("done-per-minute")) >= 1)
    await wait(50)
    await second.stop()
    assert payloads(b, "done-per-minute") == payloads(ref, "done-per-minute")


# ---- stopping ----------------------------------------------------------------------------

async def test_stop_cancels_a_long_poll_in_flight_and_still_commits_and_snapshots_what_was_done():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(3):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {}})
    runner = await per_minute(queen).run(query_id="prompt-stop", **fast(now=lambda: b.now_ms, wait_millis=20000, commit_every_millis=60000))
    await until(lambda: runner.metrics()["entriesRead"] >= 3)
    await wait(50)
    t0 = time.monotonic()
    await runner.stop()
    took = (time.monotonic() - t0) * 1000
    assert took < 1000, f"stopped in {took:.0f} ms, not after the 20 s poll"
    stored = b.kv_value(NS, "prompt-stop")
    assert stored["snapshot"]["chunks"] >= 1, "the open windows went into a snapshot"
    assert stored["position"] > 3


# ---- steps and failures -------------------------------------------------------------------

async def test_a_later_step_whose_function_keeps_failing_on_a_result_skips_that_result_the_rest_goes_on():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for p in ("c1", "c2", "c2"):
        b.push({"queue": "orders", "partition": p, "data": {}})

    def key(v):
        if v["count"] == 1:
            raise RuntimeError("a single order is refused")
        return "all"

    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders"])
                    .window_tumbling(seconds=60).aggregate({"count": lambda: 1})
                    .key_by(key)
                    .window_tumbling(seconds=120).aggregate({"sum": lambda v: v["count"]})
                    .to(queen.queue("out")).run(query_id="poison-step", **fast(now=lambda: b.now_ms, on_poison="skip"), **quiet()))
    # Minute 0 closes at 61 s, into the second window's first two minutes, which close at 121 s.
    at(b, 61000)
    b.push({"queue": "orders", "partition": "x", "data": {}})
    at(b, 121000)
    b.push({"queue": "orders", "partition": "x", "data": {}})
    await until(lambda: len(b.messages("out")) >= 1)
    await runner.stop()
    assert payloads(b, "out") == [{"sum": 2}], "c1 (one order) failed and was skipped; c2 counted"
    assert runner.metrics()["skipped"] >= 1


async def test_a_table_with_initial_now_starts_empty():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "settings", "partition": "p1", "data": {"currency": "EUR"}})
    runner = await (Stream.from_feed(queen, queues=["bookings"])
                    .join_table(queue="settings", initial="now", on=lambda m: m["data"]["property"],
                                select=lambda v, row: {"id": v["id"], "currency": row.get("currency") if isinstance(row, dict) else None})
                    .to(queen.queue("out")).run(query_id="table-now", **fast()))
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 1, "property": "p1"}})
    b.push({"queue": "settings", "partition": "p1", "data": {"currency": "CHF"}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 2, "property": "p1"}})
    await until(lambda: len(b.messages("out")) >= 2)
    await runner.stop()
    assert payloads(b, "out") == [{"id": 1, "currency": None}, {"id": 2, "currency": "CHF"}]


async def test_names_steps_refuses_a_name_in_the_wrong_place_or_twice_and_the_feed_only_steps_on_a_queue_source():
    q = fake_queen(FakeBroker())
    s = Stream.from_feed(q).window_tumbling(seconds=60).aggregate({"count": lambda: 1}).name("a")
    assert next(x for x in s._compile_feed().steps if x["kind"] == "window")["name"] == "a"
    with pytest.raises(ValueError, match="stateful step right before"):
        Stream.from_feed(q).name("a")._compile_feed()
    with pytest.raises(ValueError, match="stateful step right before"):
        s.map(lambda v: v).name("b")._compile_feed()
    with pytest.raises(ValueError, match='named "a"'):
        s.window_tumbling(seconds=60).aggregate({"n": lambda: 1}).name("a").to(q.queue("x"))._compile_feed()
    with pytest.raises(ValueError, match="a step name"):
        Stream.from_feed(q).name("bad name")

    class Src:
        _queue_name = "orders"

    with pytest.raises(ValueError, match="Stream.from_feed only"):
        await Stream.from_(Src()).gather(steps=["a"], within="1m").to(q.queue("x")).run(query_id="qs", url="http://127.0.0.1:1")


# ---- joins of many sides and gathers ---------------------------------------------------------

async def test_a_three_sided_join_pairs_the_sides_in_any_order_and_names_the_missing_side_at_the_deadline():
    b = FakeBroker()
    queen = fake_queen(b)
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .join(sides=["orders", "payments", "shipments"], within="15m",
                          select=lambda got, key: {"order": key, "sides": sorted(got.keys())},
                          on_timeout=lambda got, missing, key: {"order": key, "missing": missing})
                    .to(queen.queue("out")).run(query_id="join3", **fast(now=lambda: b.now_ms)))
    at(b, 1000); b.push({"queue": "shipments", "partition": "o1", "data": {}})
    at(b, 2000); b.push({"queue": "payments", "partition": "o1", "data": {}})
    at(b, 3000); b.push({"queue": "orders", "partition": "o2", "data": {}})
    at(b, 4000); b.push({"queue": "orders", "partition": "o1", "data": {}})
    await until(lambda: len(b.messages("out")) >= 1)
    assert payloads(b, "out") == [{"order": "o1", "sides": ["orders", "payments", "shipments"]}]
    at(b, 3000 + 15 * 60000 + 1)
    await until(lambda: len(b.messages("out")) >= 2, 3000, "the deadline of o2")
    await runner.stop()
    assert payloads(b, "out")[1] == {"order": "o2", "missing": ["payments", "shipments"]}


async def test_a_gather_of_named_steps_completes_once_a_late_duplicate_opens_nothing():
    b = FakeBroker()
    queen = fake_queen(b)
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .gather(steps={"paid": lambda m: m["queue"] == "payments", "stocked": lambda m: m["queue"] == "stock"},
                            key=lambda m: m["data"]["order"], within="10m",
                            on_complete=lambda got, key: {"order": key, "ready": True},
                            on_timeout=lambda got, missing, key: {"order": key, "missing": missing})
                    .name("runs").to(queen.queue("out")).run(query_id="gather-steps", **fast(now=lambda: b.now_ms)))
    at(b, 1000); b.push({"queue": "payments", "partition": "p", "data": {"order": "A"}})
    at(b, 2000); b.push({"queue": "stock", "partition": "s", "data": {"order": "A"}})
    await until(lambda: len(b.messages("out")) >= 1)
    assert runner.state("runs").get("A")["value"]["done"] is True, "remembered until its deadline"
    at(b, 3000); b.push({"queue": "payments", "partition": "p", "data": {"order": "A"}})
    at(b, 1000 + 10 * 60000 + 1000)
    await wait(150)
    b.push({"queue": "stock", "partition": "s", "data": {"order": "B"}})
    at(b, 1000 + 10 * 60000 + 2000 + 10 * 60000)
    await until(lambda: len(b.messages("out")) >= 2, 3000, "the deadline of B")
    await runner.stop()
    assert payloads(b, "out") == [{"order": "A", "ready": True}, {"order": "B", "missing": ["paid"]}]


async def test_a_gather_by_count_waits_for_distinct_parts_and_says_how_many_are_missing():
    b = FakeBroker()
    queen = fake_queen(b)
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["parts"])
                    .gather(count=lambda m: m["data"]["of"], key=lambda m: m["data"]["batch"], within="5m",
                            on_complete=lambda got, key: {"batch": key, "parts": sorted(p["data"]["n"] for p in got)},
                            on_timeout=lambda got, missing, key: {"batch": key, "missing": missing})
                    .to(queen.queue("out")).run(query_id="gather-count", **fast(now=lambda: b.now_ms)))
    at(b, 1000)
    b.push({"queue": "parts", "partition": "x", "data": {"batch": "b1", "of": 3, "n": 1}, "transactionId": "b1-1"})
    b.push({"queue": "parts", "partition": "x", "data": {"batch": "b1", "of": 3, "n": 1}, "transactionId": "b1-1"})
    b.push({"queue": "parts", "partition": "y", "data": {"batch": "b1", "of": 3, "n": 2}, "transactionId": "b1-2"})
    b.push({"queue": "parts", "partition": "x", "data": {"batch": "b2", "of": 2, "n": 1}, "transactionId": "b2-1"})
    b.push({"queue": "parts", "partition": "z", "data": {"batch": "b1", "of": 3, "n": 3}, "transactionId": "b1-3"})
    await until(lambda: len(b.messages("out")) >= 1)
    assert payloads(b, "out")[0] == {"batch": "b1", "parts": [1, 2, 3]}, "the duplicate counted once"
    at(b, 1000 + 5 * 60000 + 1)
    await until(lambda: len(b.messages("out")) >= 2, 3000)
    await runner.stop()
    assert payloads(b, "out")[1] == {"batch": "b2", "missing": 1}


def test_refuses_a_join_of_one_side_and_a_gather_with_both_steps_and_count():
    q = fake_queen(FakeBroker())
    with pytest.raises(ValueError, match="at least two"):
        Stream.from_feed(q).join(sides=["a"], within="1m")
    with pytest.raises(ValueError, match="one of the two"):
        Stream.from_feed(q).gather(steps=["a"], count=lambda m: 1, within="1m")
    with pytest.raises(ValueError, match="one of the two"):
        Stream.from_feed(q).gather(within="1m")


# ---- tables ----------------------------------------------------------------------------

def bookings(q, source=None, table=None):
    return (Stream.from_feed(q, {"from": "earliest", "queues": ["bookings"], **(source or {})})
            .join_table(queue="settings", on=lambda m: m["data"]["property"],
                        select=lambda v, row: {"id": v["id"], "currency": row["currency"] if isinstance(row, dict) else None}, **(table or {}))
            .to(q.queue("out")))


async def test_a_message_sees_the_table_as_of_its_own_position_updates_before_it_none_after():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "settings", "partition": "p1", "data": {"currency": "EUR"}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 1, "property": "p1"}})
    b.push({"queue": "settings", "partition": "p1", "data": {"currency": "CHF"}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 2, "property": "p1"}})
    # A change and a booking in one transaction: the change counts first.
    b.push([
        {"queue": "bookings", "partition": "x", "data": {"id": 3, "property": "p1"}},
        {"queue": "settings", "partition": "p1", "data": {"currency": "GBP"}},
    ])
    b.push({"queue": "settings", "partition": "p1", "data": None})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 4, "property": "p1"}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 5, "property": "p2"}})
    runner = await bookings(queen).run(query_id="table", **fast())
    await until(lambda: len(b.messages("out")) >= 5)
    await runner.stop()
    assert payloads(b, "out") == [
        {"id": 1, "currency": "EUR"}, {"id": 2, "currency": "CHF"}, {"id": 3, "currency": "GBP"},
        {"id": 4, "currency": None}, {"id": 5, "currency": None},
    ]


async def test_a_new_stream_first_reads_the_table_queue_from_its_beginning_and_missing_drop_drops():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "settings", "partition": "p1", "data": {"currency": "EUR"}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 0, "property": "p1"}})
    runner = await bookings(queen, source={"from": "now"}, table={"missing": "drop"}).run(query_id="prelude", **fast())
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 1, "property": "p1"}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 2, "property": "nope"}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 3, "property": "p1"}})
    await until(lambda: len(b.messages("out")) >= 2)
    await wait(50)
    await runner.stop()
    assert payloads(b, "out") == [{"id": 1, "currency": "EUR"}, {"id": 3, "currency": "EUR"}], \
        "a row from before the start; the unknown property dropped; id 0 is before the start"
    assert runner.metrics()["tableRows"] >= 1


async def test_a_where_of_the_stream_does_not_filter_the_tables_rows_and_the_table_survives_a_restart():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "settings", "partition": "p1", "data": {"currency": "EUR"}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 1, "property": "p1", "vip": True}})
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 2, "property": "p1", "vip": False}})

    def chain(q):
        return (Stream.from_feed(q, from_="earliest", queues=["bookings"]).where("data.vip", "=", True)
                .join_table(queue="settings", on=lambda m: m["data"]["property"],
                            select=lambda v, row: {"id": v["id"], "currency": row.get("currency") if isinstance(row, dict) else None})
                .to(q.queue("out")))

    first = await chain(queen).run(query_id="table-where", **fast())
    await until(lambda: len(b.messages("out")) >= 1)
    await first.stop()
    b.push({"queue": "bookings", "partition": "x", "data": {"id": 3, "property": "p1", "vip": True}})
    second = await chain(fake_queen(b, "second")).run(query_id="table-where", **fast())
    await until(lambda: len(b.messages("out")) >= 2)
    await second.stop()
    assert payloads(b, "out") == [{"id": 1, "currency": "EUR"}, {"id": 3, "currency": "EUR"}]
    assert not any(any(c.get("field") == "data.vip" for c in (r.get("where") or [])) for r in b.feed_reads), "the where ran here, not on the broker"


# ---- shards ------------------------------------------------------------------------------

def test_the_same_hash_as_the_broker_fnv_1a_over_utf_8():
    assert fnv1a32("") == 0x811C9DC5
    assert fnv1a32("a") == 0xE40C292C
    assert fnv1a32("foobar") == 0xBF9CF968
    assert fnv1a32("é") == fnv1a32(bytes([0xC3, 0xA9]).decode("utf-8"))


async def test_shards_every_partition_is_read_by_one_shard_each_with_its_own_position_lock_and_filter_on_the_broker():
    b = FakeBroker()
    queen = fake_queen(b)
    for i in range(30):
        for j in range(2):
            b.push({"queue": "orders", "partition": f"c{i}", "data": {"i": i, "j": j}})
    group = await (Stream.from_feed(queen, from_="earliest", queues=["orders"]).map(lambda m: dict(m["data"]))
                   .to(queen.queue("copy")).run(query_id="sharded", shards=3, **fast()))
    await until(lambda: len(b.messages("copy")) >= 60)
    await wait(50)
    await group.stop()
    copy = b.messages("copy")
    assert len(copy) == 60, "each message once"
    assert len({e["transactionId"] for e in copy}) == 60
    assert all(e["transactionId"].startswith(output_prefix("sharded")) for e in copy), "one prefix for every shard"
    for s in range(3):
        assert b.kv_value(NS, f"sharded#shard{s}of3")["position"] > 1, f"shard {s} has a position"
        assert any((r.get("shard") or {}).get("count") == 3 and r["shard"]["index"] == s for r in b.feed_reads), f"shard {s} reads its slice"
    assert b.kv_value(NS, "sharded#layout")["shards"] == 3
    m = group.metrics()
    assert m["shards"] == 3
    assert m["outputs"] == 60
    # Each shard took the partitions the hash gives it.
    for e in copy:
        assert shard_of(e["partition"], 3) == shard_of(f"c{e['payload']['i']}", 3)


async def test_shard_by_a_key_windows_per_tenant_come_out_right_across_shards():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(40):
        b.push({"queue": "clicks", "partition": f"p{i % 7}", "data": {"tenant": f"t{i % 5}"}})
    group = await (Stream.from_feed(queen, from_="earliest", queues=["clicks"]).key_by(lambda m: m["data"]["tenant"])
                   .window_tumbling(seconds=60).aggregate({"count": lambda: 1})
                   .map(lambda v, ctx: {"tenant": ctx["key"], "count": v["count"]})
                   .to(queen.queue("per-tenant"))
                   .run(query_id="by-tenant", shards=4, shard_by=lambda m: m["data"]["tenant"], **fast(now=lambda: b.now_ms)))
    at(b, 61000)
    b.push({"queue": "clicks", "partition": "p0", "data": {"tenant": "flush"}})
    await until(lambda: len(b.messages("per-tenant")) >= 5)
    await group.stop()
    out = sorted(payloads(b, "per-tenant"), key=lambda x: x["tenant"])
    assert out == [{"tenant": f"t{t}", "count": 8} for t in range(5)]
    assert not any(r.get("shard") for r in b.feed_reads), "a key shard is filtered here"


async def test_a_different_shard_count_is_refused_a_reset_starts_the_shards_over():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {}})

    def chain():
        return Stream.from_feed(queen, from_="earliest", queues=["orders"]).to(queen.queue("copy"))

    g = await chain().run(query_id="layout", shards=3, **fast())
    await until(lambda: len(b.messages("copy")) >= 1)
    await g.stop()
    with pytest.raises(Exception) as c1:
        await chain().run(query_id="layout", shards=2, **fast())
    assert getattr(c1.value, "code", None) == "feed_layout_changed"
    with pytest.raises(Exception) as c2:
        await chain().run(query_id="layout", **fast())
    assert getattr(c2.value, "code", None) == "feed_layout_changed"
    again = await chain().run(query_id="layout", shards=2, reset="two", **fast())
    await again.stop()
    assert b.kv_value(NS, "layout#layout")["shards"] == 2


async def test_a_second_process_waits_on_every_shard_and_takes_them_over_when_the_first_stops():
    b = FakeBroker()
    for i in range(12):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {"n": i}})

    def chain(q):
        return Stream.from_feed(q, from_="earliest", queues=["orders"]).to(q.queue("copy"))

    qa, qb = fake_queen(b, "a"), fake_queen(b, "b")
    a = await chain(qa).run(query_id="standby-shards", shards=3, **fast())
    bb = await chain(qb).run(query_id="standby-shards", shards=3, **fast())
    await until(lambda: len(b.messages("copy")) >= 12)
    assert bb.metrics()["running"] == 0, "all standby"
    await a.stop()
    for i in range(12, 24):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {"n": i}})
    await until(lambda: len(b.messages("copy")) >= 24, 6000)
    await bb.stop()
    assert sorted(p["n"] for p in payloads(b, "copy")) == list(range(24))


async def test_every_shard_sees_the_whole_table():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "settings", "partition": "p1", "data": {"currency": "EUR"}})
    for i in range(20):
        b.push({"queue": "bookings", "partition": f"b{i}", "data": {"id": i, "property": "p1"}})
    group = await (Stream.from_feed(queen, from_="earliest", queues=["bookings"])
                   .join_table(queue="settings", on=lambda m: m["data"]["property"],
                               select=lambda v, row: {"id": v["id"], "currency": row.get("currency") if isinstance(row, dict) else None})
                   .to(queen.queue("out")).run(query_id="shard-table", shards=3, **fast()))
    await until(lambda: len(b.messages("out")) >= 20)
    await group.stop()
    assert all(p["currency"] == "EUR" for p in payloads(b, "out"))
    assert any((r.get("shard") or {}).get("queues") == ["bookings"] for r in b.feed_reads), "the shard splits the stream queue only"


# ---- live reads ---------------------------------------------------------------------------

async def test_in_the_process_a_step_by_name_one_key_or_a_list():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for p in ("c1", "c1", "c2"):
        b.push({"queue": "orders", "partition": p, "data": {}})
    runner = await per_minute(queen).run(query_id="live", **fast(now=lambda: b.now_ms))
    await until(lambda: runner.metrics()["entriesRead"] >= 3)
    one = runner.state("perMinute").get("c1")
    assert one["found"] is True
    assert [w["value"] for w in one["value"]["windows"]] == [{"count": 2}]
    assert len(runner.state("perMinute").list()["items"]) == 2
    assert runner.state("perMinute").get("nope")["found"] is False
    assert "no step named" in runner.state("nope").get("c1")["error"]


async def test_from_outside_over_ephemeral_queues_on_one_runner_and_on_shards():
    b = FakeBroker()
    queen = fake_queen(b, "runner", ephemeral=True)
    at(b, 1000)
    for i in range(9):
        b.push({"queue": "orders", "partition": f"c{i % 3}", "data": {}})
    runner = await per_minute(queen).run(query_id="view-one", **fast(now=lambda: b.now_ms, serve_wait_millis=100))
    await until(lambda: runner.metrics()["entriesRead"] >= 9)
    asker = fake_queen(b, "asker", ephemeral=True)
    view = Stream.view(asker, "view-one", timeout_millis=2000)
    got = await view.get("perMinute", "c1")
    assert got["found"] is True
    assert got["value"]["windows"][0]["value"]["count"] == 3
    assert len((await view.list("perMinute"))["items"]) == 3
    status = (await view.status())[0]
    assert status["role"] == "active"
    assert status["metrics"]["entriesRead"] >= 9

    group = await (Stream.from_feed(queen, from_="earliest", queues=["orders"])
                   .window_tumbling(seconds=60).aggregate({"count": lambda: 1}).name("perMinute")
                   .to(queen.queue("counts2")).run(query_id="view-shards", shards=3, **fast(now=lambda: b.now_ms, serve_wait_millis=100)))
    await until(lambda: group.metrics()["entriesRead"] >= 9)
    sharded = Stream.view(asker, "view-shards", timeout_millis=2000)
    for k in ("c0", "c1", "c2"):
        r = await sharded.get("perMinute", k)
        assert r["found"] is True, k
        assert r["shard"] == shard_of(k, 3), f"{k} answered by its shard"
    assert len((await sharded.list("perMinute"))["items"]) == 3
    assert len(await sharded.status()) == 3


async def test_a_view_of_a_stream_nobody_runs_times_out_with_the_reason():
    asker = fake_queen(FakeBroker(), "asker", ephemeral=True)
    with pytest.raises(Exception) as caught:
        await Stream.view(asker, "nobody", timeout_millis=150).get("s", "k")
    assert getattr(caught.value, "code", None) == "feed_view_timeout"


# ---- what the code review found -------------------------------------------------------------

async def test_a_later_steps_deadline_that_passed_on_a_tick_is_closed_before_an_earlier_steps_result_reaches_it_on_a_replay_too():
    def chain(q):
        return (Stream.from_feed(q, from_="earliest", queues=["events"])
                .window_tumbling(seconds=10).aggregate({"count": lambda: 1})
                .key_by(lambda v: "all")
                .join(left=lambda m: m["ctx"]["key"] == "k1", right=lambda m: m["ctx"]["key"] == "k2", within="5s",
                      on_timeout=lambda m, side: {"side": side, "key": m["ctx"]["key"]})
                .to(q.queue("out")))

    async def scenario(crash_it):
        b = FakeBroker()
        q = fake_queen(b, "a")

        def opts():
            return dict(**fast(now=lambda: b.now_ms, lock_ttl="1s", snapshot_every_millis=3600000), **quiet())

        at(b, 1000); b.push({"queue": "events", "partition": "k1", "data": {}})
        at(b, 11000); b.push({"queue": "events", "partition": "k2", "data": {}})
        r = await chain(q).run(query_id="xstep", **opts())
        await until(lambda: r.metrics()["entriesRead"] >= 2 and r.metrics()["steps"][1]["open"] == 1)
        at(b, 16000)  # quiet: a tick, and step 2's deadline (15 s) passes alone
        await until(lambda: len(b.messages("out")) >= 1, 3000, "the left timeout")
        at(b, 21000)  # a tick: k2's window closes at 20 s, its result waits in step 2
        await until(lambda: r.metrics()["steps"][1]["open"] == 1 and r.metrics()["steps"][0]["open"] == 0, 3000)
        await until(lambda: (b.kv_value(NS, "xstep") or {}).get("clock", 0) >= T0 + 21000, 3000)
        if crash_it:
            crash(q)
            q = fake_queen(b, "b")
            r = await chain(q).run(query_id="xstep", **opts())
            await until(lambda: r.metrics()["role"] == "active", 6000)
            assert r.metrics()["replays"] >= 1
        at(b, 26000); b.push({"queue": "events", "partition": "z", "data": {}})
        await until(lambda: len(b.messages("out")) >= 2, 3000, "the right timeout")
        await wait(50)
        return payloads(b, "out")

    live = await scenario(False)
    assert live == [{"side": "left", "key": "k1"}, {"side": "right", "key": "k2"}]
    assert await scenario(True) == live


async def test_a_takeover_after_a_stand_down_keeps_the_state_it_loaded():
    b = FakeBroker()
    at(b, 1000)

    def chain(q):
        return (Stream.from_feed(q, from_="earliest", queues=["orders"]).window_tumbling(seconds=60)
                .aggregate({"count": lambda: 1}).to(q.queue("counts")))

    def opts():
        return dict(**fast(now=lambda: b.now_ms, lock_ttl="1s"), **quiet())

    qa = fake_queen(b, "a")
    feed_calls = {"n": 0}

    async def hook(path, body):
        if path == "/api/v1/feed":
            feed_calls["n"] += 1
            if feed_calls["n"] == 2:
                qa.http.cut = True

    qa.http.post_hook = hook
    a = await chain(qa).run(query_id="arm", **opts())
    await until(lambda: a.metrics()["role"] == "standby", 15000, "a stands down")
    for i in range(3):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {}})
    bb = await chain(fake_queen(b, "b")).run(query_id="arm", **opts())
    await until(lambda: bb.metrics()["role"] == "active" and bb.metrics()["stateKeys"] == 3, 15000)
    await bb.stop()
    qa.http.cut = False
    await until(lambda: a.metrics()["role"] == "active", 20000, "a takes over")
    await until(lambda: a.metrics()["commits"] >= 1, 5000)
    await a.stop()
    c = await chain(fake_queen(b, "c")).run(query_id="arm", **opts())
    assert c.metrics()["stateKeys"] == 3


@pytest.mark.parametrize("queues", [["acme.*"], ["acme.bookings", "acme.settings"]])
async def test_shards_never_split_a_table_queue_the_streams_queues_also_name(queues):
    bk = 0
    while shard_of(f"b{bk}", 2) != 0:
        bk += 1
    pr = 0
    while shard_of(f"p{pr}", 2) != 1:
        pr += 1
    b = FakeBroker()
    b.push({"queue": "acme.settings", "partition": f"p{pr}", "data": {"currency": "EUR"}})
    b.push({"queue": "acme.bookings", "partition": f"b{bk}", "data": {"id": 1, "property": f"p{pr}"}})
    q = fake_queen(b)
    g = await (Stream.from_feed(q, from_="earliest", queues=queues)
               .join_table(queue="acme.settings", on=lambda m: m["data"]["property"],
                           select=lambda v, row: {"id": v["id"], "currency": row["currency"] if isinstance(row, dict) else None})
               .to(q.queue("out")).run(query_id=f"t-{len(queues)}", shards=2, **fast()))
    await until(lambda: len(b.messages("out")) >= 1)
    await wait(50)
    await g.stop()
    assert payloads(b, "out") == [{"id": 1, "currency": "EUR"}]


async def test_a_process_still_on_the_old_layout_commits_nothing_once_a_reset_changed_it_and_stops():
    b = FakeBroker()
    for i in range(6):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {"n": i}})

    def chain(q):
        return Stream.from_feed(q, from_="earliest", queues=["orders"]).to(q.queue("copy"))

    # An idle shard commits its moved position now and then: then it meets the new layout too.
    old = await chain(fake_queen(b, "old")).run(query_id="relayout", shards=2, **fast(position_commit_millis=50), **quiet())
    await until(lambda: len(b.messages("copy")) >= 6)
    fresh = await chain(fake_queen(b, "new")).run(query_id="relayout", shards=3, reset="three", **fast(), **quiet())
    b.push({"queue": "orders", "partition": "c9", "data": {"n": 9}})
    await until(lambda: all(m["role"] == "stopped" for m in old.metrics()["perShard"]), 4000, "the old shards to stop")
    await until(lambda: any(e["payload"]["n"] == 9 for e in b.messages("copy")))
    await wait(100)
    await fresh.stop()
    nines = [p for p in payloads(b, "copy") if p["n"] == 9]
    assert len(nines) == 1, "the new message once: only the new layout committed it"
    assert any(m["stopped"] and "layout" in m["stopped"]["error"] for m in old.metrics()["perShard"])


async def test_a_reset_string_applies_to_a_process_that_took_over_as_a_standby():
    b = FakeBroker()
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    first = await per_minute(fake_queen(b, "v1")).run(query_id="reset-takeover", **fast(now=lambda: b.now_ms, reset="v1"))
    await until(lambda: first.metrics()["stateKeys"] == 1)
    second = await per_minute(fake_queen(b, "v2")).run(query_id="reset-takeover", **fast(now=lambda: b.now_ms, reset="v2"))
    assert second.metrics()["role"] == "standby"
    await first.stop()
    await until(lambda: second.metrics()["role"] == "active", 6000)
    await until(lambda: b.kv_value(NS, "reset-takeover").get("resetToken") == "v2", 3000)
    assert b.kv_value(NS, "reset-takeover")["epoch"] == 1, "started over once"


async def test_a_replay_leaves_out_the_messages_the_run_before_skipped_even_when_they_would_pass_now():
    b = FakeBroker()
    at(b, 1000)
    for i in range(4):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": i}})
    failing = {"on": True}

    def m(msg):
        if failing["on"] and msg["data"]["n"] == 2:
            raise RuntimeError("a flaky service")
        return msg["data"]

    def chain(q):
        return (Stream.from_feed(q, from_="earliest", queues=["orders"]).map(m)
                .window_tumbling(seconds=60).aggregate({"count": lambda: 1}).to(q.queue("counts")))

    def opts():
        return dict(**fast(now=lambda: b.now_ms, lock_ttl="1s", on_poison="skip", snapshot_every_millis=3600000), **quiet())

    qa = fake_queen(b, "a")
    a = await chain(qa).run(query_id="skips", **opts())
    await until(lambda: a.metrics()["skipped"] == 1 and position_of(b, "skips") > 4)
    assert len(b.kv_value(NS, "skips")["skips"]) == 1, "the skip is kept with the position"
    crash(qa)
    failing["on"] = False
    second = await chain(fake_queen(b, "b")).run(query_id="skips", **opts())
    await until(lambda: second.metrics()["role"] == "active", 6000)
    assert second.metrics()["replaySkipped"] >= 1
    at(b, 61000); b.push({"queue": "orders", "partition": "x", "data": {"n": 9}})
    await until(lambda: len(b.messages("counts")) >= 1)
    await second.stop()
    assert payloads(b, "counts")[0] == {"count": 3}, "as the first run counted: the skipped message stays out"


async def test_a_shard_by_that_throws_meets_the_poison_policy_in_shard_0_only():
    b = FakeBroker()
    for i in range(6):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {"n": i}})
    dlq = []

    def shard_by(msg):
        if msg["data"]["n"] == 4:
            raise RuntimeError("no key")
        return f"k{msg['data']['n']}"

    def on_error(e, ctx):
        if ctx.get("action") == "skipped":
            dlq.append((ctx.get("shard") or {}).get("index"))

    g = await (Stream.from_feed(fake_queen(b), from_="earliest", queues=["orders"]).to(fake_queen(b).queue("copy"))
               .run(query_id="bad-shard-key", shards=3, shard_by=shard_by, **fast(on_poison="skip", on_error=on_error)))
    await until(lambda: len(b.messages("copy")) >= 5)
    await wait(100)
    await g.stop()
    assert sorted(p["n"] for p in payloads(b, "copy")) == [0, 1, 2, 3, 5]
    assert dlq == [0], "one skip, in shard 0"


async def test_since_without_from_starts_at_the_first_message_since_then():
    from datetime import datetime, timezone

    b = FakeBroker()
    at(b, 1000); b.push({"queue": "orders", "partition": "c1", "data": {"n": 1}})
    at(b, 5000); b.push({"queue": "orders", "partition": "c1", "data": {"n": 2}})
    since = datetime.fromtimestamp((T0 + 3000) / 1000, tz=timezone.utc)
    r = await Stream.from_feed(fake_queen(b), queues=["orders"], since=since).to(fake_queen(b).queue("copy")).run(query_id="since", **fast())
    await until(lambda: len(b.messages("copy")) >= 1)
    await wait(50)
    await r.stop()
    assert [p["n"] for p in payloads(b, "copy")] == [2]


async def test_a_gather_by_count_of_results_from_a_window_counts_each_window_once():
    b = FakeBroker()
    queen = fake_queen(b)
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders"])
                    .window_tumbling(seconds=60).aggregate({"count": lambda: 1})
                    .gather(count=lambda m: 2, key=lambda m: m["partition"], within="10m",
                            on_complete=lambda got, key: {"key": key, "minutes": [g["data"]["count"] for g in got]})
                    .to(queen.queue("out")).run(query_id="gather-windows", **fast(now=lambda: b.now_ms)))
    at(b, 1000); b.push({"queue": "orders", "partition": "c1", "data": {}})
    at(b, 61000); b.push({"queue": "orders", "partition": "c1", "data": {}})
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    at(b, 121000); b.push({"queue": "orders", "partition": "flush", "data": {}})
    await until(lambda: len(b.messages("out")) >= 1)
    await runner.stop()
    assert payloads(b, "out") == [{"key": "c1", "minutes": [1, 2]}]


# ---- a crash anywhere gives the same results --------------------------------------------------

async def test_a_result_reaches_the_next_step_at_the_time_its_window_closed_however_late_a_replay_sees_it():
    def chain(q):
        return (Stream.from_feed(q, from_="earliest", queues=["orders"])
                .window_tumbling(seconds=60).aggregate({"count": lambda: 1})
                .key_by(lambda v: "all")
                .window_tumbling(seconds=60).aggregate({"windows": lambda: 1})
                .map(lambda v, ctx: {**v, "minute": ctx["windowKey"]})
                .to(q.queue("out")))

    b = FakeBroker()
    qa = fake_queen(b, "a")
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})

    def opts():
        return dict(**fast(now=lambda: b.now_ms, lock_ttl="1s", snapshot_every_millis=3600000), **quiet())

    a = await chain(qa).run(query_id="late-close", **opts())
    await until(lambda: a.metrics()["entriesRead"] >= 1)
    # A quiet feed: this runner closes minute 0 on the clock (a tick), at about 61 s.
    at(b, 61000)
    await until(lambda: a.metrics()["steps"][1]["open"] == 1, 3000, "the first result in the second step")
    at(b, 130000)
    b.push({"queue": "orders", "partition": "x", "data": {}})
    await until(lambda: len(b.messages("out")) >= 1)
    await until(lambda: position_of(b, "late-close") > b.messages("orders")[-1]["position"])
    crash(qa)
    # The next runner replays from the start: it sees minute 0 close only at 130 s.
    second = await chain(fake_queen(b, "b")).run(query_id="late-close", **opts())
    await until(lambda: second.metrics()["role"] == "active", 6000)
    at(b, 250000)
    b.push({"queue": "orders", "partition": "y", "data": {}})
    await until(lambda: len(b.messages("out")) >= 2, 4000)
    await wait(50)
    await second.stop()
    assert [p["minute"] for p in payloads(b, "out")] == [js_iso(MIN0 + 60000), js_iso(MIN0 + 180000)], \
        "each result once, in the minute its window closed"


async def test_two_windows_chained_crashed_at_random_points_send_what_one_uninterrupted_run_sends():
    seed = {"v": 7}

    def rnd():
        seed["v"] = (seed["v"] * 1103515245 + 12345) % 2147483648
        return seed["v"] / 2147483648

    msgs = []
    t = 0
    for _ in range(160):
        t += math.floor(rnd() * 4000)
        msgs.append({"t": t, "partition": f"p{math.floor(rnd() * 9)}", "v": math.floor(rnd() * 10)})

    def chain(q):
        return (Stream.from_feed(q, from_="earliest", queues=["events"])
                .window_tumbling(seconds=60).aggregate({"count": lambda: 1, "sum": lambda v: v["v"]})
                .key_by(lambda v: "all")
                .window_tumbling(seconds=180).aggregate({"windows": lambda: 1, "total": lambda v: v["sum"]})
                .to(q.queue("out")))

    def load(b):
        for m in msgs:
            at(b, m["t"])
            b.push({"queue": "events", "partition": m["partition"], "data": {"v": m["v"]}})

    # Two messages far later close every window of both steps; returns the last position.
    def flush(b):
        at(b, t + 400000); b.push({"queue": "events", "partition": "flush", "data": {"v": 0}})
        at(b, t + 800000)
        return b.push({"queue": "events", "partition": "flush", "data": {"v": 0}})

    ref = FakeBroker()
    load(ref)
    r = await chain(fake_queen(ref)).run(query_id="rand", **fast(now=lambda: ref.now_ms))
    ref_end = flush(ref)
    await until(lambda: position_of(ref, "rand") > ref_end, 6000)
    await wait(100)
    await r.stop()
    expected = payloads(ref, "out")
    assert len(expected) >= 3

    b = FakeBroker()
    load(b)
    q = fake_queen(b, "p0")

    def opts():
        return dict(**fast(now=lambda: b.now_ms, lock_ttl="1s", max_records=7, snapshot_max_entries=25), **quiet())

    runner = await chain(q).run(query_id="rand", **opts())
    for k in range(1, 4):
        target = math.floor(len(msgs) * k / 4)
        await until(lambda: position_of(b, "rand") >= target, 6000)
        crash(q)
        q = fake_queen(b, f"p{k}")
        runner = await chain(q).run(query_id="rand", **opts())
        await until(lambda: runner.metrics()["role"] == "active", 6000)
    end = flush(b)
    await until(lambda: position_of(b, "rand") > end and len(b.messages("out")) >= len(expected), 8000)
    await wait(100)
    await runner.stop()
    assert payloads(b, "out") == expected
