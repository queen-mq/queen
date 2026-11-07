"""Port of the JS SDK's test-v2/feed-unit/feedBatch.test.js:
``.foreach_batch(fn, max_items=..., max_wait_millis=...)`` on a feed stream,
the results in batches, and the position saved only once fn returns. Each
test runs the client's own runner against the broker in memory of
fake_broker.py."""

import asyncio
import inspect
import re

import pytest

from queen.streams.runtime.feed_runner import POSITION_NAMESPACE as NS, feed_output_id, output_prefix
from queen.streams.stream import Stream

from .fake_broker import T0, FakeBroker, at, crash, fake_queen, fast, quiet, until, wait


def stored(b, id_):
    return b.kv_value(NS, id_)


def orders(b, frm, to):
    for i in range(frm, to + 1):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {"n": i}})


def ns(items):
    return [it["value"]["n"] for it in items]


def past(b, id_, e):
    """The record's position is the next one to read: past ``e`` once e is committed.
    (The lock and each commit take positions of their own, so the positions are read from the log.)"""
    return ((stored(b, id_) or {}).get("position") or 0) > e["position"]


def last_order(b):
    return b.messages("orders")[-1]


def to_n(m):
    return {"n": m["data"]["n"]}


def per_key(items):
    return [(it["ctx"]["key"], it["value"]["count"]) for it in items]


class Recorder:
    """A batch function that records each call, and runs ``then(items, call)`` (to raise, wait, crash)."""

    def __init__(self, then=None):
        self.calls = []
        self._then = then

    async def fn(self, items):
        self.calls.append(items)
        if self._then is not None:
            r = self._then(items, len(self.calls))
            if inspect.isawaitable(r):
                await r


# ---- batches and the position -----------------------------------------------------------------

async def test_one_batch_per_read_each_item_has_the_value_the_ctx_foreach_gets_and_the_id_of_the_output():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 5)
    gate = asyncio.Event()
    seen = {"open": "unset"}

    async def then(items, call):
        seen["open"] = stored(b, "per-read")
        await gate.wait()

    rec = Recorder(then)
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(to_n)
                    .foreach_batch(rec.fn)
                    .run(query_id="per-read", **fast()))
    await until(lambda: len(rec.calls) == 1)
    await wait(50)
    assert seen["open"] is None, "nothing committed while fn runs"
    assert stored(b, "per-read") is None, "still nothing committed"
    gate.set()
    await until(lambda: past(b, "per-read", last_order(b)))
    items = rec.calls[0]
    assert ns(items) == [1, 2, 3, 4, 5], "the whole read, in log order"
    msgs = b.messages("orders")
    for i, it in enumerate(items):
        e = msgs[i]
        assert sorted(it.keys()) == ["ctx", "id", "value"]
        assert it["ctx"] == {
            "position": e["position"], "queue": "orders", "partition": e["partition"],
            "partitionId": e["partitionId"], "offset": e["offset"], "transactionId": e["transactionId"],
        }
        assert it["id"] == feed_output_id("per-read", e, 0, 0, "per-read"), "the id a .to() output of it would have"
    m = runner.metrics()
    assert m["batches"] == 1
    assert m["effects"] == 5, "effects count the items delivered"
    assert m["emitted"] == 5
    assert m["pendingBatch"] == 0
    assert len(rec.calls) == 1


async def test_max_items_a_read_of_7_goes_in_calls_of_3_3_and_1_each_followed_by_its_commit():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 7)
    positions = []
    rec = Recorder(lambda items, call: positions.append((stored(b, "max-items") or {}).get("position")))
    await (Stream.from_feed(queen, from_="earliest")
           .map(to_n)
           .foreach_batch(rec.fn, max_items=3)
           .run(query_id="max-items", **fast()))
    await until(lambda: past(b, "max-items", last_order(b)))
    assert [ns(c) for c in rec.calls] == [[1, 2, 3], [4, 5, 6], [7]]
    msgs = b.messages("orders")
    assert positions == [None, msgs[3]["position"], msgs[6]["position"]], "each call sees the commit of the one before, and no more"


async def test_max_items_also_splits_what_one_message_makes_and_the_commit_comes_after_the_last():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 5}})
    positions = []
    rec = Recorder(lambda items, call: positions.append((stored(b, "split-one") or {}).get("position")))
    await (Stream.from_feed(queen, from_="earliest")
           .flat_map(lambda m: [{"n": i} for i in range(m["data"]["n"])])
           .foreach_batch(rec.fn, max_items=2)
           .run(query_id="split-one", **fast()))
    await until(lambda: past(b, "split-one", last_order(b)))
    assert [ns(c) for c in rec.calls] == [[0, 1], [2, 3], [4]]
    assert positions == [None, None, None], "one message is one position: it commits once, after its last item"
    e = b.messages("orders")[0]
    assert [it["id"] for c in rec.calls for it in c] == [feed_output_id("split-one", e, i, 0, "split-one") for i in range(5)]


async def test_max_wait_millis_one_batch_across_reads_nothing_committed_while_it_waits_full_it_goes_at_once():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 0)
    orders(b, 1, 2)
    rec = Recorder()
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(to_n)
                    .foreach_batch(rec.fn, max_items=5, max_wait_millis=1000)
                    .run(query_id="linger", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["pendingBatch"] == 2)
    at(b, 400)
    orders(b, 3, 3)
    await until(lambda: runner.metrics()["pendingBatch"] == 3)
    await wait(80)
    assert len(rec.calls) == 0, "it waits for more"
    assert stored(b, "linger") is None, "and nothing commits past the items that wait"
    at(b, 1000)
    await until(lambda: len(rec.calls) == 1)
    assert ns(rec.calls[0]) == [1, 2, 3], "one batch of the two reads"
    await until(lambda: past(b, "linger", last_order(b)))
    # Five at once fill the batch: it goes without waiting.
    orders(b, 4, 8)
    await until(lambda: len(rec.calls) == 2)
    assert ns(rec.calls[1]) == [4, 5, 6, 7, 8]
    await until(lambda: past(b, "linger", last_order(b)))


# ---- stops and crashes ----------------------------------------------------------------

async def test_stop_delivers_the_batch_that_waits_then_commits_with_the_stop_mark():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 3)
    rec = Recorder()
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(to_n)
                    .foreach_batch(rec.fn, max_wait_millis=3600000)
                    .run(query_id="stop-flush", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["pendingBatch"] == 3)
    assert len(rec.calls) == 0
    await runner.stop()
    assert [ns(c) for c in rec.calls] == [[1, 2, 3]]
    assert past(b, "stop-flush", last_order(b)), "committed past the batch"
    assert stored(b, "stop-flush")["stopped"]["reason"] == "stop"


async def test_a_batch_that_fails_at_stop_is_tried_once_and_left_for_the_next_start_which_gets_the_same_items():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 2)

    def down(items, call):
        if items[0]["value"]["n"] == 3:
            raise RuntimeError("down")

    first = Recorder(down)
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(to_n)
                    .foreach_batch(first.fn, max_wait_millis=3600000, max_items=2)
                    .run(query_id="stop-fail", **fast(now=lambda: b.now_ms), **quiet(retries=3)))
    await until(lambda: past(b, "stop-fail", last_order(b)))
    orders(b, 3, 3)
    await until(lambda: runner.metrics()["pendingBatch"] == 1)
    await runner.stop()
    assert [ns(c) for c in first.calls] == [[1, 2], [3]], "one try at the stop: no retries"
    assert not past(b, "stop-fail", last_order(b)), "the item is not committed"
    assert stored(b, "stop-fail")["stopped"]["reason"] == "stop"
    again = Recorder()
    second = await (Stream.from_feed(fake_queen(b, "again"), from_="earliest")
                    .map(to_n)
                    .foreach_batch(again.fn)
                    .run(query_id="stop-fail", **fast()))
    await until(lambda: len(again.calls) == 1)
    assert ns(again.calls[0]) == [3]
    assert again.calls[0][0]["id"] == first.calls[1][0]["id"], "the same id: an upsert by it writes once"
    await second.stop()


async def test_a_crash_before_fn_returns_delivers_the_batch_again_with_the_same_ids_and_a_replay_delivers_nothing():
    b = FakeBroker()
    qa = fake_queen(b, "a")
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    b.push({"queue": "orders", "partition": "c2", "data": {}})
    at(b, 61000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})

    def per_minute(q, rec):
        return (Stream.from_feed(q, from_="earliest", queues=["orders"])
                .window_tumbling(seconds=60)
                .aggregate({"count": lambda: 1})
                .foreach_batch(rec.fn))

    # A delivers minute 0 and commits it; minute 1 closes in a batch it never commits.
    cut = {"done": False}

    def crash_on_minute_1(items, call):
        if any(it["ctx"]["windowStart"] == T0 + 60000 for it in items):
            crash(qa)
            cut["done"] = True

    a = Recorder(crash_on_minute_1)
    opts = {**fast(now=lambda: b.now_ms, lock_ttl="1s", snapshot_every_millis=3600000), **quiet()}
    await per_minute(qa, a).run(query_id="batch-crash", **opts)
    await until(lambda: len(a.calls) == 1 and past(b, "batch-crash", last_order(b)))
    assert per_key(a.calls[0]) == [("c1", 1), ("c2", 1)], "minute 0, one item per key"
    at(b, 121000)
    b.push({"queue": "orders", "partition": "c3", "data": {}})
    await until(lambda: cut["done"])
    lost = a.calls[1]
    assert per_key(lost) == [("c1", 1)]
    bb = Recorder()
    second = await per_minute(fake_queen(b, "b"), bb).run(query_id="batch-crash", **opts)
    await until(lambda: second.metrics()["role"] == "active", 6000, "the takeover")
    await until(lambda: len(bb.calls) >= 1)
    assert second.metrics()["replays"] >= 1, "it read the feed again from the snapshot"
    assert per_key(bb.calls[0]) == [("c1", 1)], "minute 1 again, and minute 0 not: the replay sends nothing"
    assert [it["id"] for it in bb.calls[0]] == [it["id"] for it in lost], "the same ids as the batch that never committed"
    assert all(it["id"].startswith(output_prefix("batch-crash")) for it in lost)
    await until(lambda: past(b, "batch-crash", last_order(b)))


# ---- failures and the poison policy ----------------------------------------------------------------

async def test_a_batch_that_fails_then_passes_is_tried_again_whole_not_poison():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 3)

    def flaky(items, call):
        if call <= 2:
            raise RuntimeError("flaky")

    rec = Recorder(flaky)
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(to_n)
                    .foreach_batch(rec.fn)
                    .run(query_id="batch-flaky", **fast()))
    await until(lambda: past(b, "batch-flaky", last_order(b)))
    assert [ns(c) for c in rec.calls] == [[1, 2, 3], [1, 2, 3], [1, 2, 3]]
    assert [it["id"] for it in rec.calls[2]] == [it["id"] for it in rec.calls[0]]
    m = runner.metrics()
    assert m["retries"] == 2
    assert m["poisoned"] == 0
    assert m["batches"] == 1
    assert m["effects"] == 3


async def test_skip_tried_whole_then_item_by_item_the_item_that_fails_is_left_out_the_others_are_delivered():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 4)
    errors = []

    def bad_row(items, call):
        if any(it["value"]["n"] == 3 for it in items):
            raise RuntimeError("bad row")

    rec = Recorder(bad_row)
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(to_n)
                    .foreach_batch(rec.fn)
                    .run(query_id="batch-skip", **fast(), on_poison="skip", retries=1,
                         on_error=lambda err, ctx: errors.append((err, ctx))))
    await until(lambda: past(b, "batch-skip", last_order(b)))
    assert [ns(c) for c in rec.calls] == [[1, 2, 3, 4], [1, 2, 3, 4], [1], [2], [3], [3], [4]]
    m = runner.metrics()
    assert m["skipped"] == 1
    assert m["poisoned"] == 1
    assert m["effects"] == 3, "items 1, 2 and 4"
    assert m["batches"] == 3
    third = b.messages("orders")[2]
    skip = next((e for e in errors if e[1].get("action") == "skipped"), None)
    assert skip is not None
    err, ctx = skip
    assert ctx["phase"] == "poison"
    assert ctx["position"] == third["position"]
    assert ctx["partition"] == "c3"
    assert re.search(
        r"an item of the message at position \d+ \(orders/c3, offset 1\) failed 2 times in foreachBatch and was skipped: bad row",
        str(err),
    )


async def test_a_dead_letter_queue_gets_the_item_in_the_commit_that_moves_the_position_past_the_batch():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 3)

    def no_price(items, call):
        if any(it["value"]["n"] == 2 for it in items):
            raise TypeError("no price")

    rec = Recorder(no_price)
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(to_n)
                    .foreach_batch(rec.fn)
                    .run(query_id="batch-dlq", **fast(), **quiet(on_poison=queen.queue("batch-dlq-q"), retries=0)))
    await until(lambda: past(b, "batch-dlq", last_order(b)))
    assert [ns(c) for c in rec.calls] == [[1, 2, 3], [1], [2], [3]]
    dead_letters = b.messages("batch-dlq-q")
    assert dead_letters, "dead-lettered"
    dead = dead_letters[0]
    assert dead["partition"] == "c2", "in the partition it came from"
    second = b.messages("orders")[1]
    assert dead["payload"] == {
        "queryId": "batch-dlq",
        "attempts": 1,
        "error": {"name": "TypeError", "message": "no price"},
        "item": {"value": {"n": 2}, "ctx": rec.calls[0][1]["ctx"], "id": feed_output_id("batch-dlq", second, 0, 0, "batch-dlq")},
    }
    assert dead["transactionId"].startswith(output_prefix("batch-dlq")), "the query never reads it back"
    commit = next(t for t in b.txns if any(it["queue"] == "batch-dlq-q" for it in t["items"]))
    assert any(op.get("key") == "batch-dlq" and ((op.get("value") or {}).get("position") or 0) > last_order(b)["position"]
               for op in commit["kv"]), "with the position past the batch, in one transaction"
    assert runner.metrics()["deadLettered"] == 1


async def test_stop_nothing_of_the_batch_commits_the_record_names_the_item_and_a_restart_gets_the_whole_batch_again():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 2)
    errors = []

    def bad_row(items, call):
        if any(it["value"]["n"] == 4 for it in items):
            raise RuntimeError("bad row")

    rec = Recorder(bad_row)
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(to_n)
                    .foreach_batch(rec.fn)
                    .run(query_id="batch-stop", **fast(), retries=1, on_error=lambda err, ctx: errors.append((err, ctx))))
    await until(lambda: past(b, "batch-stop", last_order(b)))
    before = stored(b, "batch-stop")["position"]
    orders(b, 3, 5)
    await runner.done
    assert [ns(c) for c in rec.calls] == [[1, 2], [3, 4, 5], [3, 4, 5], [3], [4], [4]], "whole twice, then one by one up to the item"
    fourth = b.messages("orders")[3]
    rec2 = stored(b, "batch-stop")
    assert rec2["position"] == before, "the last commit: before the batch"
    assert rec2["stopped"]["reason"] == "poison"
    assert rec2["stopped"]["position"] == fourth["position"], "the item that failed"
    assert rec2["stopped"]["partition"] == "c4"
    m = runner.metrics()
    assert m["role"] == "stopped"
    assert m["stopped"]["reason"] == "poison"
    assert m["stopped"]["position"] == fourth["position"]
    err, _ctx = next(e for e in errors if e[1].get("action") == "stopped")
    assert err.code == "feed_poison"
    assert re.search(
        r"an item of the message at position \d+ \(orders/c4, offset 1\) failed 2 times in foreachBatch: bad row\. "
        r"The stream stopped before its batch",
        str(err),
    )
    again = Recorder()
    await (Stream.from_feed(fake_queen(b, "fixed"), from_="earliest")
           .map(to_n)
           .foreach_batch(again.fn)
           .run(query_id="batch-stop", **fast()))
    await until(lambda: past(b, "batch-stop", last_order(b)))
    assert [ns(c) for c in again.calls] == [[3, 4, 5]]
    assert [it["id"] for it in again.calls[0]] == [it["id"] for it in rec.calls[1]]


async def test_a_transaction_that_stops_the_stream_takes_its_items_out_of_the_batch_again():
    # Not in the JS file: the messages of one transaction go or stay together,
    # so an item of a message before the poisoned one, in its transaction,
    # leaves the batch (the checkpoint's batch length) and comes again later.
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 1}})
    b.push([
        {"queue": "orders", "partition": "c2", "data": {"n": 2}},
        {"queue": "orders", "partition": "c3", "data": {"n": 3}},
    ])
    rec = Recorder()

    def bad_order(m):
        if m["data"]["n"] == 3:
            raise RuntimeError("bad order")
        return {"n": m["data"]["n"]}

    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(bad_order)
                    .foreach_batch(rec.fn)
                    .run(query_id="batch-tx-stop", **fast(), **quiet(retries=0)))
    await runner.done
    assert [ns(c) for c in rec.calls] == [[1]], "the item of n=2 stays with its transaction"
    r = stored(b, "batch-tx-stop")
    assert r["position"] == b.messages("orders")[1]["position"], "stopped at the transaction"
    assert r["stopped"]["partition"] == "c3"
    assert runner.metrics()["pendingBatch"] == 0


async def test_a_message_that_stops_the_stream_comes_after_the_batch_that_waits_the_batch_goes_first():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 4)
    rec = Recorder()

    def bad_order(m):
        if m["data"]["n"] == 3:
            raise RuntimeError("bad order")
        return {"n": m["data"]["n"]}

    runner = await (Stream.from_feed(queen, from_="earliest")
                    .map(bad_order)
                    .foreach_batch(rec.fn)
                    .run(query_id="msg-stop", **fast(), **quiet()))
    await runner.done
    assert [ns(c) for c in rec.calls] == [[1, 2]]
    third = b.messages("orders")[2]
    r = stored(b, "msg-stop")
    assert r["position"] == third["position"], "stopped at the message"
    assert r["stopped"]["reason"] == "poison"
    assert runner.metrics()["effects"] == 2


# ---- windows and closes ----------------------------------------------------------------

async def test_nothing_commits_while_a_window_result_waits_in_the_batch_not_even_a_tick_the_commit_after_fn_moves_the_clock():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    b.push({"queue": "orders", "partition": "c1", "data": {}})
    at(b, 61000)
    b.push({"queue": "orders", "partition": "c2", "data": {}})
    rec = Recorder()
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders"])
                    .window_tumbling(seconds=60)
                    .aggregate({"count": lambda: 1})
                    .foreach_batch(rec.fn, max_wait_millis=100000)
                    .run(query_id="batch-tick", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["pendingBatch"] == 1)
    commits = runner.metrics()["commits"]
    # Minute 1 is due by now on this clock: a stream without a waiting batch would tick.
    at(b, 125000)
    await wait(150)
    assert runner.metrics()["ticks"] == 0, "no tick while an item waits"
    assert runner.metrics()["commits"] == commits, "no commit"
    assert len(rec.calls) == 0
    at(b, 161000)
    await until(lambda: len(rec.calls) == 1)
    assert per_key(rec.calls[0]) == [("c1", 1)]
    # The commit after it moved the broker's clock: minute 1 closes into the next batch.
    await until(lambda: runner.metrics()["pendingBatch"] == 1)
    at(b, 262000)
    await until(lambda: len(rec.calls) == 2)
    assert per_key(rec.calls[1]) == [("c2", 1)]
    prefix = output_prefix("batch-tick")
    assert all(it["id"].startswith(prefix) and len(it["id"]) == len(prefix) + 40 for c in rec.calls for it in c)


async def test_a_close_of_many_windows_at_once_goes_in_batches_of_max_items_with_commits_in_the_middle_of_the_close():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    parts = 2500
    for i in range(parts):
        b.push({"queue": "orders", "partition": f"p{i}", "data": {}})
    rec = Recorder()
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["orders"])
                    .window_tumbling(seconds=60)
                    .aggregate({"count": lambda: 1})
                    .foreach_batch(rec.fn, max_items=1000)
                    .run(query_id="batch-mass", **fast(now=lambda: b.now_ms)))

    def loaded():
        m = runner.metrics()
        return m["entriesRead"] >= parts and m["pendingBatch"] == 0 and m["commits"] >= 1

    await until(loaded, 10000)
    at(b, 61000)
    b.push({"queue": "other", "partition": "x", "data": {}})
    await until(lambda: sum(len(c) for c in rec.calls) >= parts, 10000)
    assert [len(c) for c in rec.calls] == [1000, 1000, 500]
    assert runner.metrics()["closeCommits"] >= 1, "a commit in the middle of the close"
    assert len({it["id"] for c in rec.calls for it in c}) == parts, "every result its own id"


# ---- the chain ----------------------------------------------------------------

async def test_runs_on_stream_from_feed_only():
    class Src:
        _queue_name = "orders"

    async def fn(items):
        return None

    s = Stream.from_(Src()).foreach_batch(fn)
    with pytest.raises(ValueError, match=r"\.foreach_batch\(\) runs on Stream\.from_feed only"):
        await s.run(query_id="q")


async def test_checks_its_function_and_options():
    queen = fake_queen(FakeBroker())
    s = Stream.from_feed(queen, {})
    with pytest.raises(ValueError, match="fn is a function"):
        s.foreach_batch("no")
    with pytest.raises(ValueError, match="max_items is a whole number from 1"):
        s.foreach_batch(lambda items: None, max_items=0)
    with pytest.raises(ValueError, match="max_items is a whole number from 1"):
        s.foreach_batch(lambda items: None, max_items=1.5)
    with pytest.raises(ValueError, match="max_wait_millis is a whole number from 0"):
        s.foreach_batch(lambda items: None, max_wait_millis=-1)
    with pytest.raises(ValueError, match='unknown option "maxWait"'):
        s.foreach_batch(lambda items: None, {"maxWait": 10})
    # The JS names work too, and the error names the option as it was given.
    with pytest.raises(ValueError, match="maxItems is a whole number from 1"):
        s.foreachBatch(lambda items: None, {"maxItems": 0})
    with pytest.raises(ValueError, match="must be the last in the chain"):
        await s.foreach_batch(lambda items: None).map(lambda x: x).run(query_id="x")


async def test_the_description_of_the_stream_names_the_sink_and_its_options():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1, 1)

    async def fn(items):
        return None

    await (Stream.from_feed(queen, from_="earliest")
           .foreach_batch(fn, max_items=50, max_wait_millis=250)
           .run(query_id="batch-about", **fast()))
    await until(lambda: b.kv_get(NS, "batch-about#about"))
    assert b.kv_get(NS, "batch-about#about")["value"]["sink"] == {"kind": "foreachBatch", "maxItems": 50, "maxWaitMillis": 250}
