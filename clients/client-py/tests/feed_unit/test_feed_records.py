"""Port of the JS SDK's test-v2/feed-unit/feedRecords.test.js: what a feed
stream leaves in KV for a reader outside its process (the console): a
description of the stream (``<queryId>#about``), and in the position record
the feed time it read up to, whether it was caught up, a heartbeat on a quiet
feed, what each step holds, and why it stopped."""

import re

from queen.streams.runtime.feed_runner import POSITION_NAMESPACE as NS
from queen.streams.stream import Stream
from queen.streams.util.js import js_date_parse, js_iso

from .fake_broker import T0, FakeBroker, at, fake_queen, fast, quiet, until, wait


def record(b, key):
    return b.kv_value(NS, key)


def version(b, key):
    row = b.kv_get(NS, key)
    return row["version"] if row else None


def chain(queen, seconds=60):
    return (Stream.from_feed(queen, from_="earliest", queues=["orders", "pay*"])
            .where("data.country", "=", "IT")
            .map(lambda m: m["data"])
            .window_tumbling(seconds=seconds)
            .aggregate({"count": lambda: 1, "sum": lambda v: v["total"]})
            .name("perCustomer")
            .to(queen.queue("totals")))


# ---- the description ----------------------------------------------------------------------

async def test_says_what_the_stream_reads_its_steps_where_results_go_and_what_a_failing_message_does():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {"country": "IT", "total": 1}})
    await chain(queen).run(query_id="about", **fast(), on_poison=queen.queue("feed-dlq"))
    await until(lambda: record(b, "about#about"), 3000, "the first commit")
    about = record(b, "about#about")
    assert about["v"] == 1
    assert about["queryId"] == "about"
    assert re.match(r"^client-py \d+\.\d+\.\d+", about["client"])
    assert re.match(r"^[0-9a-f]{16}$", about["hash"])
    assert any(any(op.get("key") == "about#about" for op in t["kv"]) and any(op.get("key") == "about" for op in t["kv"]) for t in b.txns), \
        "in a commit of the position"
    assert about["reads"] == {"queues": ["orders", "pay*"], "partitions": None, "where": [{"field": "data.country", "op": "=", "value": "IT"}]}, \
        "a where before the first stateful step is the broker's filter"
    assert [s["kind"] for s in about["steps"]] == ["ops", "window"]
    assert about["steps"][0] == {"kind": "ops", "ops": ["map"]}
    w = about["steps"][1]
    assert w["name"] == "perCustomer"
    assert w["clock"] == "feed"
    assert w["window"]["kind"] == "window-tumbling"
    assert w["window"]["seconds"] == 60
    assert w["reducer"] == {"kind": "aggregate", "fields": ["count", "sum"]}
    assert about["sink"] == {"kind": "queues", "queues": ["totals"]}
    assert about["onPoison"] == {"queue": "feed-dlq"}
    assert about["shards"] is None
    assert about["exclusive"] is True


async def test_is_written_once_per_activation_the_next_deploys_chain_replaces_it():
    b = FakeBroker()
    b.push({"queue": "orders", "partition": "c1", "data": {"country": "IT", "total": 1}})
    first = await chain(fake_queen(b, "a")).run(query_id="again", **fast())
    await until(lambda: record(b, "again#about"), 3000, "the first commit")
    v1 = version(b, "again#about")
    b.push({"queue": "orders", "partition": "c2", "data": {"country": "IT", "total": 1}})
    await until(lambda: record(b, "again")["position"] > b.messages("orders")[1]["position"])
    assert version(b, "again#about") == v1, "not at every commit"
    await first.stop()
    await chain(fake_queen(b, "c"), 10).run(query_id="again", **fast(reset="v2"))
    await until(lambda: version(b, "again#about") > v1, 3000, "the new deploy's first commit")
    assert record(b, "again#about")["steps"][1]["window"]["seconds"] == 10


async def test_names_a_pattern_a_gather_a_table_and_their_clocks_a_function_sink_is_a_route_shards_are_one_record():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "orders", "partition": "c1", "data": {"at": js_iso(T0)}})
    await (Stream.from_feed(queen, from_="earliest", queues=["orders", "payments"])
           .join_table(queue="customers", on=lambda m: m["partition"],
                       select=lambda v, row: {**v, "tier": row.get("tier") if isinstance(row, dict) else None})
           .name("customers")
           .pattern(start=lambda m: m["queue"] == "orders", then=lambda m: m["queue"] == "payments", within="15m",
                    event_time=lambda m: m["data"]["at"], on_timeout=lambda s: {"unpaid": s["partition"]})
           .name("unpaid")
           .gather(steps={"unpaid": lambda m: m["data"].get("unpaid", None) is not None if isinstance(m["data"], dict) else False},
                   key=lambda m: m["data"]["unpaid"], within="1h", event_time=False)
           .name("alerts")
           .to(lambda v: "alerts" if v.get("unpaid") else None)
           .run(query_id="kinds", **fast(), shards=2, on_poison="skip"))
    await until(lambda: record(b, "kinds#about"), 3000, "a shard's first commit")
    about = record(b, "kinds#about")
    assert [[s["kind"], s["name"]] for s in about["steps"]] == [["table", "customers"], ["pattern", "unpaid"], ["gather", "alerts"]]
    assert about["steps"][0]["config"]["queues"] == ["customers"]
    assert "clock" not in about["steps"][0], "a table runs on no clock"
    assert about["steps"][1]["clock"] == "event"
    assert about["steps"][1]["config"]["within"] == 15 * 60000
    assert about["steps"][2]["clock"] == "feed", "eventTime: false, after a step on event time"
    assert about["steps"][2]["config"]["parts"] == ["unpaid"]
    assert about["reads"]["tables"] == ["customers"]
    assert about["sink"] == {"kind": "route"}
    assert about["onPoison"] == "skip"
    assert about["shards"] == {"count": 2, "by": "partition"}
    assert b.kv_keys(NS, "kinds#about") == ["kinds#about"], "one description for every shard"


# ---- the position record -------------------------------------------------------------------

async def test_says_the_feed_time_it_read_up_to_and_that_it_was_caught_up():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000); b.push({"queue": "orders", "partition": "c1", "data": {"n": 1}})
    at(b, 2000); b.push({"queue": "orders", "partition": "c2", "data": {"n": 2}})
    last = b.messages("orders")[1]
    runner = await Stream.from_feed(queen, from_="earliest").map(lambda m: m["data"]).to(queen.queue("out")).run(query_id="read-to", **fast(now=lambda: b.now_ms))
    await until(lambda: len(b.messages("out")) >= 2 and (record(b, "read-to") or {}).get("caughtUp") is True)
    r = record(b, "read-to")
    assert r["readTo"] >= js_date_parse(last["ts"]), "at or past the last message read"
    assert r["caughtUp"] is True
    assert "open" not in r, "a chain with no state counts nothing"
    assert "stopped" not in r
    await runner.stop()


async def test_a_quiet_caught_up_stream_keeps_its_record_fresh():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000); b.push({"queue": "orders", "partition": "c1", "data": {}})
    await Stream.from_feed(queen, from_="earliest").to(queen.queue("out")).run(query_id="fresh", **fast(now=lambda: b.now_ms, position_commit_millis=5000))
    await until(lambda: len(b.messages("out")) >= 1 and (record(b, "fresh") or {}).get("caughtUp") is True)
    v = version(b, "fresh")
    at(b, 3000)
    await wait(100)
    assert version(b, "fresh") == v, "not before positionCommitMillis"
    at(b, 7000)
    await until(lambda: version(b, "fresh") > v, 3000, "the next position commit")
    r = record(b, "fresh")
    assert r["caughtUp"] is True
    assert r["updatedAt"] == js_iso(T0 + 7000)
    # The broker's clock moves only with writes: readTo can trail the wall clock; caughtUp says it is not behind.
    assert T0 + 1000 <= r["readTo"] <= T0 + 7000, f"readTo {r['readTo'] - T0} ms"
    assert record(b, "fresh#about")["positionCommitMillis"] == 5000, "the description says how often to expect it"


async def test_counts_what_each_step_holds_open_windows_and_messages_an_event_time_step_holds():
    b = FakeBroker()
    queen = fake_queen(b)

    def ev(minutes):
        return js_iso(js_date_parse("2025-10-01T10:00:00.000Z") + minutes * 60000)

    at(b, 1000)
    for c in ("c1", "c2", "c3"):
        b.push({"queue": "orders", "partition": c, "data": {"total": 1, "at": ev(0)}})
    b.push({"queue": "orders", "partition": "c1", "data": {"total": 1, "at": ev(30)}})
    runner = await (Stream.from_feed(queen, from_="earliest")
                    .window_tumbling(seconds=60).aggregate({"count": lambda: 1}).name("perMinute")
                    .pattern(start=lambda m: True, then=lambda m: False, within="1h", event_time=lambda: ev(0),
                             allowed_lateness="1h", on_timeout=lambda s: s)
                    .name("later").to(queen.queue("out")).run(query_id="counts", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["entriesRead"] >= 4)
    await until(lambda: ((record(b, "counts") or {}).get("open") or {}).get("perMinute") == 3)
    r = record(b, "counts")
    assert list(r["open"].keys()) == ["perMinute", "later"]
    assert "held" not in r, "nothing reached the event-time step yet: no window closed"


async def test_counts_the_messages_an_event_time_step_holds_for_their_turn():
    b = FakeBroker()
    queen = fake_queen(b)

    def ev(minutes):
        return js_iso(js_date_parse("2025-10-01T10:00:00.000Z") + minutes * 60000)

    at(b, 1000)
    b.push({"queue": "issues", "partition": "r1", "data": {"n": 1, "at": ev(0)}})
    b.push({"queue": "issues", "partition": "r1", "data": {"n": 2, "at": ev(30)}})
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["issues", "comments"])
                    .pattern(start=lambda m: m["queue"] == "issues", then=lambda m: m["queue"] == "comments",
                             key=lambda m: f"{m['partition']}#{m['data']['n']}", within="1h", event_time=lambda m: m["data"]["at"],
                             allowed_lateness="20m", on_timeout=lambda s: {"unanswered": s["data"]["n"]})
                    .name("unanswered").to(queen.queue("out")).run(query_id="held", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["steps"][0]["held"] == 1)
    await until(lambda: ((record(b, "held") or {}).get("held") or {}).get("unanswered") == 1, 3000, "the commit after it")
    r = record(b, "held")
    assert r["open"]["unanswered"] == 1, "issue 1 waits for its answer; issue 2 waits for the watermark"


# ---- why it stopped ---------------------------------------------------------------------------

def orders(b, n):
    for i in range(1, n + 1):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {"n": i}})


async def test_a_message_that_keeps_failing_the_reason_the_attempts_and_the_message_in_the_commit_of_the_others():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 4)

    def m(msg):
        if msg["data"]["n"] == 3:
            raise RuntimeError("bad order")
        return msg["data"]

    runner = await Stream.from_feed(queen, from_="earliest").map(m).to(queen.queue("out")).run(query_id="poison", **fast(max_records=2), **quiet())
    await runner.done
    third = b.messages("orders")[2]
    r = record(b, "poison")
    assert r["position"] == third["position"]
    assert r["stopped"]["reason"] == "poison"
    assert r["stopped"]["error"] == "bad order"
    assert r["stopped"]["attempts"] == 4
    assert r["stopped"]["position"] == third["position"]
    assert r["stopped"]["queue"] == "orders"
    assert r["stopped"]["partition"] == "c3"
    assert r["stopped"]["offset"] == third["offset"]
    assert js_date_parse(r["stopped"]["at"]) > 0


async def test_a_message_that_fails_first_with_nothing_else_to_commit_the_record_still_says_so():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 2)

    def m(msg):
        raise RuntimeError("no parser")

    runner = await Stream.from_feed(queen, from_="earliest").map(m).to(queen.queue("out")).run(query_id="first", **fast(), **quiet())
    await runner.done
    r = record(b, "first")
    assert r["position"] == b.messages("orders")[0]["position"]
    assert r["stopped"]["reason"] == "poison"
    assert r["stopped"]["error"] == "no parser"


async def test_a_long_error_is_cut():
    b = FakeBroker()
    queen = fake_queen(b)
    orders(b, 1)

    def m(msg):
        raise RuntimeError("x" * 5000)

    runner = await Stream.from_feed(queen, from_="earliest").map(m).to(queen.queue("out")).run(query_id="long", **fast(), **quiet())
    await runner.done
    assert len(record(b, "long")["stopped"]["error"]) <= 501


async def test_a_stop_asked_by_its_process_says_so_and_the_next_commit_of_a_new_run_drops_it():
    b = FakeBroker()
    orders(b, 2)
    qa = fake_queen(b, "a")
    first = await Stream.from_feed(qa, from_="earliest").to(qa.queue("out")).run(query_id="asked", **fast())
    await until(lambda: len(b.messages("out")) >= 2)
    await first.stop()
    assert record(b, "asked")["stopped"]["reason"] == "stop"
    qb = fake_queen(b, "b")
    second = await Stream.from_feed(qb, from_="earliest").to(qb.queue("out")).run(query_id="asked", **fast())
    orders(b, 1)
    await until(lambda: len(b.messages("out")) >= 3)
    await until(lambda: "stopped" not in record(b, "asked"), 3000, "the new run's commit")
    await second.stop()


async def test_a_stream_stopped_before_its_first_read_writes_no_record():
    b = FakeBroker()
    queen = fake_queen(b)
    runner = await Stream.from_feed(queen, from_="earliest").to(queen.queue("out")).run(query_id="early", **fast())
    await runner.stop()
    r = record(b, "early")
    assert r is None or isinstance(r.get("position"), int), f"a position or nothing, got {r}"


async def test_a_standby_that_stops_writes_nothing():
    b = FakeBroker()
    orders(b, 1)
    qa = fake_queen(b, "a")
    active = await Stream.from_feed(qa, from_="earliest").to(qa.queue("out")).run(query_id="pair", **fast())
    await until(lambda: len(b.messages("out")) >= 1 and (record(b, "pair") or {}).get("caughtUp") is True)
    qb = fake_queen(b, "b")
    standby = await Stream.from_feed(qb, from_="earliest").to(qb.queue("out")).run(query_id="pair", **fast())
    assert standby.metrics()["role"] == "standby"
    v = version(b, "pair")
    await standby.stop()
    assert version(b, "pair") == v
    assert "stopped" not in record(b, "pair")
    await active.stop()
