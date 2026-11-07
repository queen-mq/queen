"""Port of the JS SDK's test-v2/feed-unit/feed.test.js."""

import asyncio
import json
from datetime import datetime, timezone

import pytest

from queen.feed.feed import FeedReader, condition_holds, normalize_where, since_to_iso, where_condition
from queen.streams.runtime.feed_runner import POSITION_NAMESPACE, feed_output_id, output_prefix
from queen.streams.stream import Stream
from queen.streams.util.js import js_date_parse

from .fake_broker import FakeBroker, fake_queen, until


class _Refused(Exception):
    def __init__(self, message, **attrs):
        super().__init__(message)
        for k, v in attrs.items():
            setattr(self, k, v)


class FakeHttp:
    """An HTTP client that answers from a list and records what it was asked."""

    def __init__(self, answers, name="main"):
        self.name = name
        self.calls = []
        self.answers = list(answers)

    async def post(self, path, body=None, timeout=None, *args, **kwargs):
        self.calls.append({"path": path, "body": json.loads(json.dumps(body)), "timeout": timeout})
        if not self.answers:
            raise RuntimeError(f"{self.name}: no answer left")
        nxt = self.answers.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt

    async def close(self):
        pass


def entry(position, n, partition="c1", payload=None, queue="orders"):
    return {
        "id": f"m{n}", "kind": "push", "offset": n, "partition": partition, "partitionId": "pid-1",
        "payload": {"n": n, **(payload or {})}, "position": position, "queue": queue,
        "transactionId": f"t{n}", "ts": "2026-11-06T09:00:00.000000Z",
    }


def page(entries, nxt, caught_up=True, applied=None):
    return {"applied": nxt - 1 if applied is None else applied, "caughtUp": caught_up, "entries": entries, "next": nxt,
            "resolvedTs": "2026-11-06T09:00:01.000000Z"}


# ---- where conditions ---------------------------------------------------------------

def test_builds_and_checks_conditions():
    assert where_condition("data.country", "=", "IT") == {"field": "data.country", "op": "=", "value": "IT"}
    assert where_condition("data.vip", "exists") == {"field": "data.vip", "op": "exists"}
    with pytest.raises(ValueError, match="unknown op"):
        where_condition("x", "~", 1)
    with pytest.raises(ValueError, match="needs an array"):
        where_condition("x", "in", 1)
    assert normalize_where(["data.a", "=", 1]) == [{"field": "data.a", "op": "=", "value": 1}]
    assert len(normalize_where([["data.a", "=", 1], {"field": "queue", "op": "!=", "value": "q"}])) == 2


def test_evaluates_like_the_broker_on_a_message():
    msg = {"queue": "orders", "partition": "booking-9", "data": {"country": "IT", "total": 40, "tags": ["vip"]}}
    assert condition_holds(where_condition("data.country", "=", "IT"), msg) is True
    assert condition_holds(where_condition("data.total", ">", 100), msg) is False
    assert condition_holds(where_condition("data.tags.0", "=", "vip"), msg) is True
    assert condition_holds(where_condition("partition", "prefix", "booking-"), msg) is True
    assert condition_holds(where_condition("data.country", "in", ["FR", "IT"]), msg) is True
    # A missing field holds for no op, `!=` included.
    assert condition_holds(where_condition("data.missing", "!=", "x"), msg) is False


def test_reads_since_as_a_duration_a_date_or_a_time():
    now = js_date_parse("2026-11-06T10:00:00Z")
    assert since_to_iso("1d", now) == "2026-11-05T10:00:00.000Z"
    assert since_to_iso("15m", now) == "2026-11-06T09:45:00.000Z"
    assert since_to_iso(datetime(2026, 11, 6, 10, 0, 0, tzinfo=timezone.utc)) == "2026-11-06T10:00:00.000Z"
    assert since_to_iso("2026-11-01T00:00:00Z") == "2026-11-01T00:00:00.000Z"
    with pytest.raises(ValueError, match="since must be"):
        since_to_iso("yesterday")


# ---- FeedReader ---------------------------------------------------------------------

async def test_sends_the_read_moves_to_next_and_stops_once_caught_up_when_not_following():
    http = FakeHttp([page([entry(5, 1), entry(6, 2)], 7, False, 9), page([entry(9, 3)], 10, True)])

    def no_follower(url):
        raise RuntimeError("no follower")

    reader = FeedReader(http, no_follower, "http", {"queues": "orders*", "where": [["data.country", "=", "IT"]], "from": "earliest", "follow": False})
    seen = [e["data"]["n"] async for e in reader]
    assert seen == [1, 2, 3]
    assert reader.position == 10
    assert http.calls[0]["path"] == "/api/v1/feed"
    assert http.calls[0]["body"]["queues"] == ["orders*"]
    assert http.calls[0]["body"]["where"] == [{"field": "data.country", "op": "=", "value": "IT"}]
    assert http.calls[0]["body"]["from"] == "earliest"
    assert http.calls[0]["body"]["maxWaitMs"] == 0, "follow:false never long-polls"
    assert http.calls[1]["body"]["from"] == 7


async def test_hands_entries_out_in_queen_message_shape():
    http = FakeHttp([page([entry(5, 1)], 6)])
    reader = FeedReader(http, lambda url: None, "http", from_=1)
    e = (await reader.read())["entries"][0]
    assert e["data"] == {"n": 1}
    assert e["createdAt"] == e["ts"]
    assert e["position"] == 5
    assert e["partition"] == "c1"


async def test_a_leader_that_answers_421_sends_the_reader_to_the_followers_it_names():
    refused = _Refused("feed on leader", status=421, code="feed_on_leader",
                       body={"code": "feed_on_leader", "followers": [{"nodeId": 2, "httpAddr": "10.0.0.2:6632"}, {"nodeId": 3, "httpAddr": "10.0.0.3:6632"}]})
    main = FakeHttp([refused])
    opened = []
    followers = {
        "https://10.0.0.2:6632": FakeHttp([page([entry(5, 1)], 6), page([entry(6, 2)], 7)], "f2"),
        "https://10.0.0.3:6632": FakeHttp([], "f3"),
    }

    def client_for(url):
        opened.append(url)
        return followers[url]

    reader = FeedReader(main, client_for, "https", from_=1)
    first = await reader.read()
    assert [e["data"]["n"] for e in first["entries"]] == [1]
    assert opened == ["https://10.0.0.2:6632", "https://10.0.0.3:6632"]
    # The next read goes straight to the follower.
    second = await reader.read()
    assert [e["data"]["n"] for e in second["entries"]] == [2]
    assert len(main.calls) == 1
    assert len(followers["https://10.0.0.2:6632"].calls) == 2


async def test_a_follower_that_does_not_answer_passes_the_read_to_the_next_node():
    down = _Refused("ECONNREFUSED", code="ECONNREFUSED")
    main = FakeHttp([page([entry(5, 1)], 6)])
    f1 = FakeHttp([down], "f1")
    reader = FeedReader(main, lambda url: f1, "http", from_=1, follower_urls=["http://10.0.0.9:6632"])
    p = await reader.read()
    assert [e["data"]["n"] for e in p["entries"]] == [1]
    assert len(f1.calls) == 1
    assert len(main.calls) == 1, "then the configured URL"


async def test_a_read_cancelled_by_its_signal_raises_aborted_and_the_reader_stays():
    class Slow(FakeHttp):
        async def post(self, path, body=None, timeout=None, *a, **k):
            await asyncio.sleep(10)

    reader = FeedReader(Slow([]), lambda url: None, "http", from_=4)
    sig = asyncio.Event()
    task = asyncio.ensure_future(reader.read(signal=sig))
    await asyncio.sleep(0.02)
    sig.set()
    with pytest.raises(Exception) as caught:
        await asyncio.wait_for(task, 1)
    assert getattr(caught.value, "aborted", False) is True
    assert reader.position == 4


# ---- the runner -------------------------------------------------------------------

def fast(**o):
    return {"wait_millis": 20, "commit_every_millis": 0, **o}


def real_reads(b):
    return [r for r in b.feed_reads if not (r.get("from") == "now" and r.get("maxRecords") == 1 and r.get("maxWaitMs") == 0)]


async def test_pushes_the_outputs_and_the_new_position_in_one_transaction_with_ids_made_from_the_entry():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push([
        {"queue": "orders", "partition": "c1", "data": {"n": 1, "country": "IT"}},
        {"queue": "orders", "partition": "c1", "data": {"n": 2, "country": "IT"}},
    ])
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 3, "country": "IT"}})
    b.push({"queue": "orders", "partition": "c2", "data": {"n": 4, "country": "FR"}})
    runner = await (Stream.from_feed(queen, queues=["orders"], from_="earliest")
                    .where("data.country", "=", "IT")
                    .filter(lambda m: m["data"]["n"] != 2)
                    .map(lambda m: {"order": m["data"]["n"]})
                    .to(queen.queue("orders-it"))
                    .run(query_id="orders-it", **fast()))
    await until(lambda: len(b.messages("orders-it")) >= 2)
    await runner.stop()

    out = b.messages("orders-it")
    assert [e["payload"] for e in out] == [{"order": 1}, {"order": 3}]
    assert out[0]["partition"] == "c1", "the source partition by default"
    assert out[0]["position"] == out[1]["position"], "one commit"
    src = b.messages("orders")[0]
    assert out[0]["transactionId"] == feed_output_id("orders-it", src, 0)
    assert out[0]["transactionId"].startswith(output_prefix("orders-it"))
    txn = next(t for t in b.txns if t["items"])
    pos = next(op for op in txn["kv"] if op.get("key") == "orders-it")
    assert pos["op"] == "putIfAbsent", "a new query creates its position"
    assert pos["required"] is True
    assert any(op["op"] == "check" and op["ns"] == "queen-locks" for op in txn["kv"]), "the commit is guarded by the lock"
    assert b.kv_value(POSITION_NAMESPACE, "orders-it")["position"] > 4
    # The broker did the where; the query's own outputs are left out of the read.
    first = real_reads(b)[0]
    assert first["queues"] == ["orders"]
    assert first["where"] == [
        {"field": "data.country", "op": "=", "value": "IT"},
        {"field": "transactionId", "op": "!prefix", "value": output_prefix("orders-it")},
    ]
    assert first["from"] == "earliest"


async def test_a_stored_position_is_where_it_starts_and_later_commits_fence_on_its_version():
    b = FakeBroker()
    queen = fake_queen(b)
    for n in range(1, 4):
        b.push({"queue": "orders", "partition": "c1", "data": {"n": n}})
    v = b.kv_put(POSITION_NAMESPACE, "copy", {"position": 3, "queryId": "copy"})
    runner = await Stream.from_feed(queen).to(queen.queue("copy")).run(query_id="copy", **fast())
    await until(lambda: len(b.messages("copy")) >= 1)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 4}})
    await until(lambda: len(b.messages("copy")) >= 2)
    await runner.stop()
    assert [e["payload"]["n"] for e in b.messages("copy")] == [3, 4], "from position 3 on"
    assert real_reads(b)[0]["from"] == 3
    commits = [t for t in b.txns if t["items"]]
    assert next(op for op in commits[0]["kv"] if op.get("key") == "copy")["expect"] == v
    assert next(op for op in commits[1]["kv"] if op.get("key") == "copy")["expect"] == commits[0]["written"]["copy"], "the version the first commit answered"


async def test_a_lost_commit_carries_on_from_the_position_that_won_writing_nothing_of_its_own():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 1}})
    winner = {}

    def before(broker):
        # Another process committed first: the position moved past n=2.
        broker.push({"queue": "orders", "partition": "c1", "data": {"n": 2}})
        winner["at"] = broker.applied + 1
        broker.kv_put(POSITION_NAMESPACE, "copy", {"position": winner["at"], "queryId": "copy"})

    b.inject_next({"before": before})
    runner = await Stream.from_feed(queen, from_="earliest").to(queen.queue("copy")).run(query_id="copy", **fast(exclusive=False))
    await until(lambda: runner.metrics()["conflicts"] >= 1)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 3}})
    await until(lambda: len(b.messages("copy")) >= 1)
    await runner.stop()
    assert [e["payload"]["n"] for e in b.messages("copy")] == [3], "n=1 lost to the winner, n=2 was the winner's"
    assert any(r.get("from") == winner["at"] for r in real_reads(b)), "read again from the winner"


async def test_a_commit_refused_as_duplicate_reads_on_from_the_stored_position():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 1}})
    src = b.messages("orders")[0]
    winner = {}

    def before(broker):
        broker.ids.add(f"copy\u0000c1\u0000{feed_output_id('race', src, 0)}")
        winner["at"] = broker.applied + 1
        broker.kv_put(POSITION_NAMESPACE, "race", {"position": winner["at"], "queryId": "race"})

    b.inject_next({"before": before})
    runner = await Stream.from_feed(queen, from_="earliest").to(queen.queue("copy")).run(query_id="race", **fast(exclusive=False))
    await until(lambda: runner.metrics()["conflicts"] >= 1)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 2}})
    await until(lambda: len(b.messages("copy")) >= 1)
    await runner.stop()
    assert [e["payload"]["n"] for e in b.messages("copy")] == [2]
    assert any(r.get("from") == winner["at"] for r in real_reads(b)), "read on from the winner, not the same page again"
    assert runner.metrics()["errors"] == 0, "a lost race is not an error"


async def test_a_broker_without_the_feed_fails_run_with_the_reason():
    b = FakeBroker()
    queen = fake_queen(b)

    class NoFeed:
        async def post(self, *a, **k):
            raise _Refused("HTTP 404: Not Found", status=404)

    queen.feed = lambda options=None, **kw: FeedReader(NoFeed(), lambda url: None, "http", options, **kw)
    with pytest.raises(Exception) as caught:
        await Stream.from_feed(queen).to(queen.queue("copy")).run(query_id="old-broker")
    assert getattr(caught.value, "code", None) == "no_feed_route"
    assert "no POST /api/v1/feed" in str(caught.value)


async def test_a_cycle_error_with_no_on_error_hook_is_said_on_stderr(capsys):
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {"n": 1}})
    b.inject_next({"error": _Refused("HTTP 503: Service Unavailable", status=503)})
    runner = await Stream.from_feed(queen, from_="earliest").to(queen.queue("copy")).run(query_id="loud", **fast())
    await until(lambda: len(b.messages("copy")) >= 1, 5000)
    await runner.stop()
    said = capsys.readouterr().err
    assert "[queen feed] loud" in said and "503" in said and "(retrying)" in said, said
    assert len(b.messages("copy")) == 1, "the retry wrote it once"


async def test_refuses_a_chain_it_cannot_run_on_a_feed():
    b = FakeBroker()
    queen = fake_queen(b)
    with pytest.raises(ValueError, match=r"gate\(\) runs on queue sources only"):
        await Stream.from_feed(queen).gate(lambda v, ctx: True).to(queen.queue("x")).run(query_id="g")
    with pytest.raises(ValueError, match="end the chain"):
        await Stream.from_feed(queen).map(lambda m: m).run(query_id="nosink")
    with pytest.raises(ValueError, match="without '#'"):
        await Stream.from_feed(queen).to(queen.queue("x")).run(query_id="a#b")
    with pytest.raises(ValueError, match="onPoison is 'stop', 'skip' or a queue"):
        await Stream.from_feed(queen).to(queen.queue("x")).run(query_id="p", on_poison=42)
