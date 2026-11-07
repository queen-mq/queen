"""Port of the JS SDK's test-v2/feed-unit/feedStreams.test.js: poison
messages, one runner per query_id, routing, windows on the feed's clock,
patterns and joins, commits that go wrong."""

import asyncio
import re

import pytest

from queen.streams.runtime.feed_runner import POSITION_NAMESPACE, output_prefix
from queen.streams.stream import Stream
from queen.streams.util.js import js_iso

from .fake_broker import T0, FakeBroker, at, fake_queen, fast, payloads, quiet, until, wait


class _Err(Exception):
    def __init__(self, message, **attrs):
        super().__init__(message)
        for k, v in attrs.items():
            setattr(self, k, v)


def orders(b, n):
    for i in range(1, n + 1):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {"n": i}})


# ---- poison messages ----------------------------------------------------------------

async def test_stops_at_a_message_that_keeps_failing_by_default_with_what_came_before_committed():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 5)
    calls = {"n": 0}
    errors = []

    def m(msg):
        if msg["data"]["n"] == 3:
            calls["n"] += 1
            raise RuntimeError("bad order")
        return {"n": msg["data"]["n"]}

    runner = await (Stream.from_feed(queen, from_="earliest").map(m).to(queen.queue("out"))
                    .run(query_id="stop", **fast(max_records=2), on_error=lambda err, ctx: errors.append((err, ctx))))
    await runner.done
    assert payloads(b, "out") == [{"n": 1}, {"n": 2}]
    assert calls["n"] == 4, "the first try and 3 retries"
    third = b.messages("orders")[2]
    assert b.kv_value(POSITION_NAMESPACE, "stop")["position"] == third["position"], "the stored position is the message"
    mm = runner.metrics()
    assert mm["role"] == "stopped"
    assert mm["stopped"]["reason"] == "poison"
    assert mm["stopped"]["position"] == third["position"]
    stop = next((e for e in errors if e[1].get("action") == "stopped"), None)
    assert stop, "on_error hears it"
    assert re.search(r"position \d+ \(orders/c3, offset 1\) failed 4 times: bad order", str(stop[0]))
    assert stop[0].code == "feed_poison"
    await runner.stop()


async def test_skip_leaves_the_message_out_and_carries_on():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 5)

    def m(msg):
        if msg["data"]["n"] == 3:
            raise RuntimeError("bad order")
        return {"n": msg["data"]["n"]}

    runner = await Stream.from_feed(queen, from_="earliest").map(m).to(queen.queue("out")).run(query_id="skip", **fast(), **quiet(on_poison="skip"))
    await until(lambda: len(b.messages("out")) >= 4)
    await runner.stop()
    assert [p["n"] for p in payloads(b, "out")] == [1, 2, 4, 5]
    assert runner.metrics()["skipped"] == 1
    assert runner.metrics()["poisoned"] == 1


async def test_a_dead_letter_queue_gets_the_message_and_the_error_in_the_commit_of_the_others():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 4)

    def m(msg):
        if msg["data"]["n"] == 2:
            raise TypeError("no price")
        return {"n": msg["data"]["n"]}

    runner = await (Stream.from_feed(queen, from_="earliest").map(m).to(queen.queue("out"))
                    .run(query_id="dlq", **fast(), **quiet(on_poison=queen.queue("out-dlq"), retries=1)))
    await until(lambda: len(b.messages("out")) >= 3)
    await runner.stop()
    dead = b.messages("out-dlq")[0]
    assert dead["partition"] == "c2", "in the partition it came from"
    assert dead["payload"]["entry"]["data"] == {"n": 2}
    assert dead["payload"]["entry"]["queue"] == "orders"
    assert dead["payload"]["error"] == {"name": "TypeError", "message": "no price"}
    assert dead["payload"]["attempts"] == 2
    assert dead["payload"]["queryId"] == "dlq"
    assert dead["transactionId"].startswith(output_prefix("dlq")), "the query never reads it back"
    assert dead["position"] == b.messages("out")[0]["position"], "one commit with the outputs"
    assert runner.metrics()["deadLettered"] == 1


async def test_a_function_that_fails_then_passes_is_not_poison():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 3)
    tries = {"n": 0}

    def m(msg):
        if msg["data"]["n"] == 2:
            tries["n"] += 1
            if tries["n"] < 3:
                raise RuntimeError("flaky")
        return {"n": msg["data"]["n"]}

    runner = await Stream.from_feed(queen, from_="earliest").map(m).to(queen.queue("out")).run(query_id="flaky", **fast())
    await until(lambda: len(b.messages("out")) >= 3)
    await runner.stop()
    assert [p["n"] for p in payloads(b, "out")] == [1, 2, 3]
    assert runner.metrics()["retries"] == 2
    assert runner.metrics()["poisoned"] == 0


async def test_a_message_tried_again_counts_its_results_once_in_emitted_a_skipped_one_counts_none():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 3)
    failed = {"once": False}
    calls = []

    def effect(v):
        calls.append(v)
        if v == 20 and not failed["once"]:
            failed["once"] = True
            raise RuntimeError("once")
        if v == 30:
            raise RuntimeError("always")

    runner = await (Stream.from_feed(queen, from_="earliest").flat_map(lambda m: [m["data"]["n"], m["data"]["n"] * 10])
                    .foreach(effect).run(query_id="emitted-once", **fast(), **quiet(on_poison="skip")))
    await until(lambda: runner.metrics()["skipped"] >= 1)
    await runner.stop()
    assert calls[:5] == [1, 10, 2, 20, 2], "message 2 runs again from its first result"
    assert runner.metrics()["emitted"] == 4, "the four results of messages 1 and 2, once each"
    assert runner.metrics()["skipped"] == 1


async def test_a_retried_message_starts_again_from_the_state_before_it_even_after_an_in_place_change():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(1, 4):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": i}})
    failed = {"once": False}

    def red(acc, v):
        # JS reads v.n of the 'other' message as undefined; here it is a .get.
        acc["sum"] += v.get("n", 0)
        acc["count"] += 1
        if v.get("n") == 2 and not failed["once"]:
            failed["once"] = True
            raise RuntimeError("once")
        return acc

    runner = await (Stream.from_feed(queen, from_="earliest").window_tumbling(seconds=60).reduce(red, {"sum": 0, "count": 0})
                    .to(queen.queue("sums")).run(query_id="undo", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["entriesRead"] >= 3)
    at(b, 61000)
    b.push({"queue": "other", "partition": "x", "data": {}})
    await until(lambda: len(b.messages("sums")) >= 1)
    await runner.stop()
    assert payloads(b, "sums") == [{"sum": 6, "count": 3}], "the failed try left nothing behind"


async def test_a_window_result_whose_sink_function_keeps_failing_is_skipped_and_the_stream_goes_on():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    b.push({"queue": "orders", "partition": "bad", "data": {"n": 1}})
    b.push({"queue": "orders", "partition": "good", "data": {"n": 2}})
    seen = []

    def effect(value, ctx):
        if ctx["key"] == "bad":
            raise RuntimeError("downstream refused")
        seen.append({"key": ctx["key"], **value})

    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders"]).window_tumbling(seconds=60)
                    .aggregate({"count": lambda: 1}).foreach(effect)
                    .run(query_id="emit-skip", **fast(now=lambda: b.now_ms), **quiet(on_poison="skip")))
    await until(lambda: runner.metrics()["entriesRead"] >= 2)
    at(b, 61000)
    b.push({"queue": "orders", "partition": "good", "data": {"n": 3}})
    await until(lambda: len(seen) >= 1 and runner.metrics()["skipped"] >= 1)
    await runner.stop()
    assert seen == [{"key": "good", "count": 1}]


# ---- one runner per query_id ------------------------------------------------------------

async def test_a_second_process_on_the_query_id_waits_each_message_runs_foreach_once():
    b = FakeBroker()
    qa, qb = fake_queen(b, "a"), fake_queen(b, "b")
    for i in range(1, 41):
        b.push({"queue": "orders", "partition": f"c{i % 5}", "data": {"n": i}})
    calls = []

    def effect(q, who):
        return Stream.from_feed(q, from_="earliest").foreach(lambda v: calls.append({"who": who, "n": v["n"]}))

    ra = await effect(qa, "a").run(query_id="fx", **fast(max_records=7))
    rb = await effect(qb, "b").run(query_id="fx", **fast(max_records=7))
    await until(lambda: len(calls) >= 40)
    await wait(100)
    assert ra.metrics()["role"] == "active"
    assert rb.metrics()["role"] == "standby"
    assert len(calls) == 40
    assert list(dict.fromkeys(c["who"] for c in calls)) == ["a"]
    await ra.stop()
    await rb.stop()


async def test_when_the_active_one_stops_the_standby_takes_over_from_the_stored_position():
    b = FakeBroker()
    qa, qb = fake_queen(b, "a"), fake_queen(b, "b")
    for i in range(1, 6):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": i}})

    def copy(q):
        return Stream.from_feed(q, from_="earliest", queues=["orders"]).to(q.queue("copy"))

    ra = await copy(qa).run(query_id="copy", **fast(lock_ttl="2s"))
    rb = await copy(qb).run(query_id="copy", **fast(lock_ttl="2s"))
    await until(lambda: len(b.messages("copy")) >= 5)
    await ra.stop()
    for i in range(6, 9):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": i}})
    await until(lambda: len(b.messages("copy")) >= 8, 6000, "the standby to copy 6..8")
    await rb.stop()
    assert [p["n"] for p in payloads(b, "copy")] == [1, 2, 3, 4, 5, 6, 7, 8], "each once, in order"
    assert rb.metrics()["takeovers"] == 1


async def test_a_process_that_loses_its_lock_cannot_commit_and_the_other_one_takes_over():
    b = FakeBroker()
    qa, qb = fake_queen(b, "a"), fake_queen(b, "b")
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 1}})

    def copy(q):
        return Stream.from_feed(q, from_="earliest", queues=["orders"]).to(q.queue("copy"))

    ra = await copy(qa).run(query_id="cut", **fast(lock_ttl="1s"), **quiet())
    rb = await copy(qb).run(query_id="cut", **fast(lock_ttl="1s"), **quiet())
    await until(lambda: len(b.messages("copy")) >= 1)
    # a's process is cut off (a partition, or a long pause): its lock expires.
    qa.http.cut = True
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 2}})
    await until(lambda: len(b.messages("copy")) >= 2, 8000, "b to take over")
    qa.http.cut = False
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 3}})
    await until(lambda: len(b.messages("copy")) >= 3, 4000)
    await wait(200)
    assert ra.metrics()["role"] == "standby", "a found its lock gone"
    assert rb.metrics()["role"] == "active"
    assert [p["n"] for p in payloads(b, "copy")] == [1, 2, 3]
    await ra.stop()
    await rb.stop()


async def test_exclusive_false_runs_both_and_the_position_still_decides_who_commits():
    b = FakeBroker()
    qa, qb = fake_queen(b, "a"), fake_queen(b, "b")
    for i in range(1, 31):
        b.push({"queue": "orders", "partition": f"c{i % 3}", "data": {"n": i}})

    def copy(q):
        return Stream.from_feed(q, from_="earliest", queues=["orders"]).to(q.queue("copy"))

    ra = await copy(qa).run(query_id="both", **fast(exclusive=False, max_records=4))
    rb = await copy(qb).run(query_id="both", **fast(exclusive=False, max_records=4))
    await until(lambda: len(b.messages("copy")) >= 30)
    await wait(100)
    await ra.stop()
    await rb.stop()
    ns = [p["n"] for p in payloads(b, "copy")]
    assert len(ns) == 30
    assert len(set(ns)) == 30, "no message twice"
    assert ra.metrics()["role"] == "stopped"


# ---- routing ------------------------------------------------------------------------

async def test_a_function_picks_the_queue_per_message_and_every_output_commits_with_the_position():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "o1", "data": {"id": 1, "country": "IT"}})
    b.push({"queue": "orders", "partition": "o2", "data": {"id": 2, "country": "FR"}})
    b.push({"queue": "orders", "partition": "o3", "data": {"id": 3, "country": "IT"}})
    b.push({"queue": "orders", "partition": "o4", "data": {"id": 4}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders*"])
                    .to(lambda order: f"orders-{order['country']}" if order.get("country") else None)
                    .run(query_id="router", **fast()))
    await until(lambda: len(b.messages("orders-IT")) >= 2 and len(b.messages("orders-FR")) >= 1)
    await wait(100)
    await runner.stop()
    assert [p["id"] for p in payloads(b, "orders-IT")] == [1, 3]
    assert [p["id"] for p in payloads(b, "orders-FR")] == [2]
    commit = next(t for t in b.txns if t["items"])
    assert len(commit["items"]) == 3, "one transaction for both queues"
    assert any(op.get("key") == "router" for op in commit["kv"]), "with the position"
    # orders* matches its own outputs, and it never reads them back.
    assert runner.metrics()["entriesRead"] == 4
    assert len(b.messages("orders-IT")) == 2


async def test_a_list_sends_each_record_to_every_queue_a_function_may_answer_a_partition_or_several_targets():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "o1", "data": {"id": 1, "total": 500}})
    b.push({"queue": "orders", "partition": "o2", "data": {"id": 2, "total": 5}})
    everyone = await (Stream.from_feed(queen, from_="earliest", queues=["orders"])
                      .to([queen.queue("audit"), queen.queue("bi")]).run(query_id="fanout", **fast()))
    picked = await (Stream.from_feed(queen, from_="earliest", queues=["orders"])
                    .to(lambda o: [{"queue": "by-size", "partition": "big" if o["total"] > 100 else "small"},
                                   queen.queue("review") if o["total"] > 100 else None])
                    .run(query_id="picker", **fast()))
    await until(lambda: len(b.messages("bi")) >= 2 and len(b.messages("by-size")) >= 2 and len(b.messages("review")) >= 1)
    await everyone.stop()
    await picked.stop()
    assert [p["id"] for p in payloads(b, "audit")] == [1, 2]
    assert [p["id"] for p in payloads(b, "bi")] == [1, 2]
    assert [[e["payload"]["id"], e["partition"]] for e in b.messages("by-size")] == [[1, "big"], [2, "small"]]
    assert [p["id"] for p in payloads(b, "review")] == [1]
    assert b.messages("review")[0]["partition"] == "o1", "the source partition by default"


async def test_a_queue_source_refuses_several_queues_and_functions_and_so_do_pattern_and_join():
    class Src:
        _queue_name = "src"

    with pytest.raises(ValueError, match="Stream.from_feed only"):
        await Stream.from_(Src()).to(lambda v: "x").run(query_id="r1", url="http://127.0.0.1:1")
    with pytest.raises(ValueError, match="Stream.from_feed only"):
        await Stream.from_(Src()).to([{"name": "a"}, {"name": "b"}]).run(query_id="r2", url="http://127.0.0.1:1")
    with pytest.raises(ValueError, match="Stream.from_feed only"):
        await (Stream.from_(Src()).pattern(start=lambda m: True, then=lambda m: True, within="1m")
               .to({"name": "x"}).run(query_id="r3", url="http://127.0.0.1:1"))


# ---- windows on the feed clock --------------------------------------------------------

async def test_a_tumbling_window_counts_per_entity_across_queues_and_closes_when_the_clock_passes_its_end():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000); b.push({"queue": "orders", "partition": "customer-1", "data": {"total": 10}})
    at(b, 2000); b.push({"queue": "payments", "partition": "customer-1", "data": {"total": 10}})
    at(b, 3000); b.push({"queue": "orders", "partition": "customer-2", "data": {"total": 7}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders", "payments"])
                    .window_tumbling(seconds=60)
                    .aggregate({"count": lambda: 1, "sum": lambda v: v["total"], "avg": lambda v: v["total"]})
                    .map(lambda acc, ctx: {**acc, "key": ctx["key"], "window": ctx["windowKey"]})
                    .to(queen.queue("per-minute"))
                    .run(query_id="per-minute", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["entriesRead"] >= 3)
    assert len(b.messages("per-minute")) == 0, "open until the minute is over"
    at(b, 61000); b.push({"queue": "orders", "partition": "customer-3", "data": {"total": 1}})
    await until(lambda: len(b.messages("per-minute")) >= 2)
    await runner.stop()
    out = b.messages("per-minute")
    assert [e["payload"] for e in out] == [
        {"count": 2, "sum": 20, "avg": 10, "key": "customer-1", "window": "2026-11-06T09:00:00.000Z"},
        {"count": 1, "sum": 7, "avg": 7, "key": "customer-2", "window": "2026-11-06T09:00:00.000Z"},
    ], "no __avg_ running sums in the result"
    assert [e["partition"] for e in out] == ["customer-1", "customer-2"], "a result goes to its key's partition"
    assert out[0]["transactionId"].startswith(output_prefix("per-minute"))
    stored = b.kv_keys(POSITION_NAMESPACE, "per-minute#snap#")
    assert len(stored) >= 1, "a stop writes a snapshot: the open window of customer-3 is in it"
    assert all(len(t["kv"]) <= 64 for t in b.txns), "every commit within the 64 KV ops of a transaction"


async def test_a_window_on_a_quiet_feed_closes_on_time_the_runner_moves_the_broker_clock_with_a_write():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 5000); b.push({"queue": "orders", "partition": "c1", "data": {}})
    at(b, 6000); b.push({"queue": "orders", "partition": "c1", "data": {}})
    runner = await (Stream.from_feed(queen, from_="earliest").window_tumbling(seconds=60).aggregate({"count": lambda: 1})
                    .to(queen.queue("counts")).run(query_id="quiet", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["entriesRead"] >= 2)
    # Nothing more is written: only time passes.
    at(b, 59500)
    await wait(100)
    assert len(b.messages("counts")) == 0, "not before its end"
    at(b, 60000)
    await until(lambda: len(b.messages("counts")) >= 1, 3000, "the window to close with no message after it")
    await runner.stop()
    assert payloads(b, "counts") == [{"count": 2}]
    assert runner.metrics()["ticks"] >= 1


async def test_a_restart_carries_the_open_windows_on_from_kv_without_counting_anything_twice():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(3):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": i}})

    def chain(q):
        return Stream.from_feed(q, from_="earliest").window_tumbling(seconds=60).aggregate({"count": lambda: 1}).to(q.queue("counts"))

    first = await chain(queen).run(query_id="restart", **fast(now=lambda: b.now_ms))
    await until(lambda: first.metrics()["entriesRead"] >= 3)
    await first.stop()
    at(b, 2000)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 3}})
    second = await chain(fake_queen(b, "second")).run(query_id="restart", **fast(now=lambda: b.now_ms))
    await until(lambda: second.metrics()["entriesRead"] >= 1)
    at(b, 61000)
    b.push({"queue": "other", "partition": "x", "data": {}})
    await until(lambda: len(b.messages("counts")) >= 1)
    await second.stop()
    assert payloads(b, "counts") == [{"count": 4}]


async def test_sliding_windows_count_each_message_in_every_window_it_falls_in():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 10000); b.push({"queue": "q", "partition": "k", "data": {}})
    at(b, 40000); b.push({"queue": "q", "partition": "k", "data": {}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["q"]).window_sliding(size=60, slide=30)
                    .aggregate({"count": lambda: 1})
                    .map(lambda acc, ctx: {"start": ctx["windowKey"][11:19], "count": acc["count"]})
                    .to(queen.queue("slides")).run(query_id="slides", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["entriesRead"] >= 2)
    at(b, 200000); b.push({"queue": "other", "partition": "x", "data": {}})
    await until(lambda: len(b.messages("slides")) >= 3)
    await runner.stop()
    assert payloads(b, "slides") == [{"start": "08:59:30", "count": 1}, {"start": "09:00:00", "count": 2}, {"start": "09:00:30", "count": 1}]


async def test_a_session_closes_when_a_message_comes_past_the_gap_and_on_the_clock_when_none_does():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 0); b.push({"queue": "clicks", "partition": "u1", "data": {}})
    at(b, 5000); b.push({"queue": "clicks", "partition": "u1", "data": {}})
    at(b, 30000); b.push({"queue": "clicks", "partition": "u1", "data": {}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["clicks"]).window_session(gap=10)
                    .aggregate({"count": lambda: 1})
                    .map(lambda acc, ctx: {"count": acc["count"], "from": ctx["windowStart"] - T0, "to": ctx["windowEnd"] - T0})
                    .to(queen.queue("sessions")).run(query_id="sessions", **fast(now=lambda: b.now_ms)))
    await until(lambda: len(b.messages("sessions")) >= 1)
    assert payloads(b, "sessions") == [{"count": 2, "from": 0, "to": 15000}]
    at(b, 41000)
    await until(lambda: len(b.messages("sessions")) >= 2, 3000, "the second session to close on the clock")
    await runner.stop()
    assert payloads(b, "sessions")[1] == {"count": 1, "from": 30000, "to": 40000}


async def test_cron_windows_follow_the_wall_clock_and_key_by_groups_by_any_field():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 59000); b.push({"queue": "sales", "partition": "s1", "data": {"country": "IT", "amount": 5}})
    at(b, 61000); b.push({"queue": "sales", "partition": "s2", "data": {"country": "IT", "amount": 7}})
    at(b, 62000); b.push({"queue": "sales", "partition": "s3", "data": {"country": "FR", "amount": 1}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["sales"]).key_by(lambda m: m["data"]["country"])
                    .window_cron(every="minute").aggregate({"sum": lambda v: v["amount"]})
                    .map(lambda acc, ctx: {"country": ctx["key"], "minute": ctx["windowKey"][11:16], "sum": acc["sum"]})
                    .to(queen.queue("per-country")).run(query_id="cron", **fast(now=lambda: b.now_ms)))
    await until(lambda: len(b.messages("per-country")) >= 1)
    at(b, 125000); b.push({"queue": "other", "partition": "x", "data": {}})
    await until(lambda: len(b.messages("per-country")) >= 3)
    await runner.stop()
    assert payloads(b, "per-country") == [
        {"country": "IT", "minute": "09:00", "sum": 5},
        {"country": "FR", "minute": "09:01", "sum": 1},
        {"country": "IT", "minute": "09:01", "sum": 7},
    ], "windows that end together close in key order"


async def test_event_time_windows_close_on_the_watermark_and_drop_what_comes_too_late():
    b = FakeBroker()
    queen = fake_queen(b)

    def ev(sec):
        return js_iso(T0 + sec * 1000)

    for sec, v in ((10, 1), (50, 2), (70, 3), (20, 4), (130, 5)):
        b.push({"queue": "readings", "partition": "sensor", "data": {"at": ev(sec), "v": v}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["readings"])
                    .window_tumbling(seconds=60, eventTime=lambda m: m["data"]["at"], allowedLateness=5)
                    .aggregate({"sum": lambda v: v["v"]})
                    .map(lambda acc, ctx: {"minute": ctx["windowKey"][11:16], "sum": acc["sum"]})
                    .to(queen.queue("sums")).run(query_id="event-time", **fast()))
    await until(lambda: len(b.messages("sums")) >= 2)
    await runner.stop()
    assert payloads(b, "sums") == [{"minute": "09:00", "sum": 3}, {"minute": "09:01", "sum": 3}]
    assert runner.metrics()["late"] == 1, "the reading of 09:00:20 came after the watermark passed it"


async def test_a_chain_that_no_longer_fits_the_stored_state_is_refused_reset_starts_it_over():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    first = await (Stream.from_feed(queen, from_="earliest").window_tumbling(seconds=60).aggregate({"count": lambda: 1})
                   .to(queen.queue("counts")).run(query_id="chain", **fast(now=lambda: b.now_ms)))
    await until(lambda: first.metrics()["stateKeys"] >= 1)
    await first.stop()
    assert len(b.kv_keys(POSITION_NAMESPACE, "chain#snap#")) >= 1, "a stop writes a snapshot"

    def changed():
        return Stream.from_feed(queen, from_="earliest").window_tumbling(seconds=30).aggregate({"count": lambda: 1}).to(queen.queue("counts"))

    with pytest.raises(Exception) as caught:
        await changed().run(query_id="chain", **fast())
    assert getattr(caught.value, "code", None) == "feed_chain_changed"
    again = await changed().run(query_id="chain", **fast(reset="v2", now=lambda: b.now_ms))
    await until(lambda: again.metrics()["entriesRead"] >= 1)
    await again.stop()
    stored = b.kv_value(POSITION_NAMESPACE, "chain")
    assert stored["epoch"] == 1
    assert stored["resetToken"] == "v2"
    # The same reset string again carries on, it does not start over.
    third = await changed().run(query_id="chain", **fast(reset="v2", now=lambda: b.now_ms))
    await wait(80)
    await third.stop()
    assert b.kv_value(POSITION_NAMESPACE, "chain")["epoch"] == 1


# ---- patterns and joins ---------------------------------------------------------------

def pattern(queen, **extra):
    opts = {
        "start": lambda m: m["queue"] == "orders",
        "then": lambda m: m["queue"] == "payments",
        "within": "15m",
        "on_match": lambda order, payment: {"order": order["data"]["id"], "paid": payment["data"]["amount"]},
        "on_timeout": lambda order: {"order": order["data"]["id"], "remind": True},
        **extra,
    }
    return Stream.from_feed(queen, from_="earliest", queues=["orders", "payments"]).pattern(**opts).to(queen.queue("followups"))


async def test_an_order_then_its_payment_within_15_minutes_is_a_match_across_two_queues():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 0); b.push({"queue": "orders", "partition": "order-1", "data": {"id": 1}})
    at(b, 60000); b.push({"queue": "payments", "partition": "order-1", "data": {"amount": 30}})
    at(b, 70000); b.push({"queue": "orders", "partition": "order-3", "data": {"id": 3}})
    at(b, 970000); b.push({"queue": "payments", "partition": "order-3", "data": {"amount": 5}})
    at(b, 971000); b.push({"queue": "payments", "partition": "order-9", "data": {"amount": 1}})
    runner = await pattern(queen).run(query_id="paid", **fast(now=lambda: b.now_ms))
    await until(lambda: len(b.messages("followups")) >= 2)
    await wait(50)
    await runner.stop()
    assert payloads(b, "followups") == [{"order": 1, "paid": 30}, {"order": 3, "paid": 5}], \
        "exactly 15 minutes is still in time; a payment with no order waits for nothing"
    assert b.messages("followups")[0]["partition"] == "order-1"


async def test_an_order_with_no_payment_in_time_sends_the_reminder_at_the_deadline_on_a_quiet_feed():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 0); b.push({"queue": "orders", "partition": "order-2", "data": {"id": 2}})
    runner = await pattern(queen).run(query_id="remind", **fast(now=lambda: b.now_ms))
    await until(lambda: runner.metrics()["entriesRead"] >= 1)
    at(b, 900000)
    await wait(100)
    assert len(b.messages("followups")) == 0, "not at 15 minutes yet"
    at(b, 901000)
    await until(lambda: len(b.messages("followups")) >= 1, 3000, "the reminder")
    # A payment after the deadline is no match any more.
    at(b, 950000); b.push({"queue": "payments", "partition": "order-2", "data": {"amount": 9}})
    await wait(100)
    await runner.stop()
    assert payloads(b, "followups") == [{"order": 2, "remind": True}]


async def test_on_repeat_restart_counts_the_deadline_from_the_latest_start():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 0); b.push({"queue": "orders", "partition": "cart-1", "data": {"id": "a"}})
    at(b, 600000); b.push({"queue": "orders", "partition": "cart-1", "data": {"id": "b"}})
    runner = await pattern(queen, on_repeat="restart").run(query_id="cart", **fast(now=lambda: b.now_ms))
    await until(lambda: runner.metrics()["entriesRead"] >= 2)
    at(b, 1000000)
    await wait(150)
    assert len(b.messages("followups")) == 0, "the second start moved the deadline to 25 minutes"
    at(b, 1501000)
    await until(lambda: len(b.messages("followups")) >= 1)
    await runner.stop()
    assert payloads(b, "followups") == [{"order": "b", "remind": True}]


async def test_a_join_pairs_two_messages_of_an_entity_in_either_order_and_times_out_the_one_left_alone():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 0); b.push({"queue": "shipments", "partition": "order-1", "data": {"carrier": "DHL"}})
    at(b, 1000); b.push({"queue": "orders", "partition": "order-1", "data": {"id": 1}})
    at(b, 2000); b.push({"queue": "orders", "partition": "order-2", "data": {"id": 2}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders", "shipments"])
                    .join(left=lambda m: m["queue"] == "orders", right=lambda m: m["queue"] == "shipments", within="1h",
                          select=lambda order, shipment: {"order": order["data"]["id"], "carrier": shipment["data"]["carrier"]},
                          on_timeout=lambda m, side: {"unshipped": m["data"]["id"], "side": side})
                    .to(queen.queue("joined")).run(query_id="join", **fast(now=lambda: b.now_ms)))
    await until(lambda: len(b.messages("joined")) >= 1)
    at(b, 2000 + 3601000)
    await until(lambda: len(b.messages("joined")) >= 2, 3000, "the timeout of order 2")
    await runner.stop()
    assert payloads(b, "joined") == [{"order": 1, "carrier": "DHL"}, {"unshipped": 2, "side": "left"}]


async def test_after_a_flat_map_the_key_sees_each_item_one_message_can_open_several_waits():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 0); b.push({"queue": "sync", "partition": "conv-1", "data": {"changes": [{"messageId": "m1"}, {"messageId": "m2"}, {"other": 1}]}})
    at(b, 10); b.push({"queue": "delivered", "partition": "x", "data": {"message": {"id": "m2"}}})
    at(b, 15); b.push({"queue": "other", "partition": "x", "data": {}})
    at(b, 20); b.push({"queue": "delivered", "partition": "x", "data": {"message": {"id": "m1"}}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["sync", "delivered", "other"])
                    .flat_map(lambda m: [c for c in m["data"]["changes"] if c.get("messageId")] if m["queue"] == "sync" else [m["data"]])
                    .pattern(start=lambda m: m["queue"] == "sync", then=lambda m: m["queue"] == "delivered",
                             # Asked only of sync and delivered messages: 'other' has no id at all.
                             key=lambda m, v: v["messageId"] if m["queue"] == "sync" else v["message"]["id"],
                             within="1m", on_match=lambda s, d: {"id": d["data"]["message"]["id"]})
                    .to(queen.queue("ok")).run(query_id="flat-key", **fast(now=lambda: b.now_ms)))
    await until(lambda: len(b.messages("ok")) >= 2)
    await runner.stop()
    assert sorted(p["id"] for p in payloads(b, "ok")) == ["m1", "m2"]


async def test_the_entity_can_be_a_field_instead_of_the_partition():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 0); b.push({"queue": "orders", "partition": "p-a", "data": {"id": "X1"}})
    at(b, 1000); b.push({"queue": "payments", "partition": "p-b", "data": {"order": "X1", "amount": 3}})
    runner = await pattern(queen, key=lambda m: m["data"].get("id") or m["data"].get("order")).run(query_id="by-field", **fast(now=lambda: b.now_ms))
    await until(lambda: len(b.messages("followups")) >= 1)
    await runner.stop()
    assert payloads(b, "followups") == [{"order": "X1", "paid": 3}]
    assert b.messages("followups")[0]["partition"] == "X1"


# ---- commits that go wrong ----------------------------------------------------------------

async def test_a_commit_whose_answer_was_lost_is_not_written_twice_stateless():
    b = FakeBroker()
    queen = fake_queen(b)
    for i in range(1, 4):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": i}})
    # The broker applies the commit, and the answer never comes back.
    b.inject_next({"when": "after", "error": _Err("socket hang up", code="ECONNRESET")})
    runner = await Stream.from_feed(queen, from_="earliest", queues=["orders"]).to(queen.queue("copy")).run(query_id="lost-answer", **fast(), **quiet())
    await until(lambda: runner.metrics()["conflicts"] >= 1, 4000, "the retry to find the commit stored")
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 4}})
    await until(lambda: len(b.messages("copy")) >= 4)
    await runner.stop()
    assert [p["n"] for p in payloads(b, "copy")] == [1, 2, 3, 4]


async def test_a_commit_whose_answer_was_lost_does_not_count_twice_windows():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(1, 4):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": i}})
    b.inject_next({"when": "after", "error": _Err("socket hang up", code="ECONNRESET")})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders"]).window_tumbling(seconds=60).aggregate({"count": lambda: 1})
                    .to(queen.queue("counts")).run(query_id="lost-window", **fast(now=lambda: b.now_ms), **quiet()))
    await until(lambda: runner.metrics()["conflicts"] >= 1, 4000)
    at(b, 2000); b.push({"queue": "orders", "partition": "c1", "data": {"n": 4}})
    at(b, 61000); b.push({"queue": "orders", "partition": "c2", "data": {"n": 5}})
    await until(lambda: len(b.messages("counts")) >= 1)
    await runner.stop()
    assert payloads(b, "counts") == [{"count": 4}]


async def test_a_page_with_more_outputs_than_max_pending_outputs_commits_between_positions():
    b = FakeBroker()
    queen = fake_queen(b)
    for i in range(1, 8):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {"n": i}})
    runner = await Stream.from_feed(queen, from_="earliest", queues=["orders"]).to(queen.queue("copy")).run(query_id="split", **fast(max_pending_outputs=2))
    await until(lambda: len(b.messages("copy")) >= 7)
    await runner.stop()
    assert [p["n"] for p in payloads(b, "copy")] == [1, 2, 3, 4, 5, 6, 7]
    commits = [t for t in b.txns if t["items"]]
    assert len(commits) >= 4, f"{len(commits)} commits"
    assert all(len(t["items"]) <= 2 for t in commits)


async def test_a_process_whose_lock_was_taken_can_commit_nothing_more_and_stands_by():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 1}})
    runner = await Stream.from_feed(queen, from_="earliest", queues=["orders"]).to(queen.queue("copy")).run(query_id="stolen", **fast(), **quiet())
    await until(lambda: len(b.messages("copy")) >= 1)
    # The lock expired on the broker and somebody else holds it now.
    b.steal_lock("queen-feed/stolen")
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 2}})
    await until(lambda: runner.metrics()["role"] == "standby", 3000)
    await wait(100)
    assert [p["n"] for p in payloads(b, "copy")] == [1], "n=2 is the new holder's to copy"
    await runner.stop()
