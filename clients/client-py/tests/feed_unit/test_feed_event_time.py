"""Port of the JS SDK's test-v2/feed-unit/feedEventTime.test.js: steps whose
clock is the time the messages carry. History pushed all at once, messages
out of order, a quiet feed, steps chained on event time, and a crash anywhere."""

import math
from datetime import datetime, timezone

import pytest

from queen.streams.runtime.feed_runner import POSITION_NAMESPACE as NS
from queen.streams.stream import Stream
from queen.streams.util.js import js_date_parse, js_iso

from .fake_broker import T0, FakeBroker, at, crash, fake_queen, fast, payloads, quiet, until, wait

# The history: a day long before the feed's clock.
H0 = js_date_parse("2025-10-01T10:00:00.000Z")


def ev(minutes, sec=0):
    return js_iso(H0 + minutes * 60000 + sec * 1000)


def position_of(b, key):
    return (b.kv_value(NS, key) or {}).get("position", 0)


# An issue opened, and the first comment on it within the hour.
def issue(b, repo, n, minutes):
    return b.push({"queue": "issues", "partition": repo, "data": {"n": n, "at": ev(minutes)}})


def comment(b, repo, n, minutes):
    return b.push({"queue": "comments", "partition": repo, "data": {"n": n, "at": ev(minutes)}})


def unanswered(q, **opts):
    o = {
        "start": lambda m: m["queue"] == "issues",
        "then": lambda m: m["queue"] == "comments",
        "key": lambda m: f"{m['partition']}#{m['data']['n']}",
        "within": "1h",
        "event_time": lambda m: m["data"]["at"],
        "on_match": lambda s, t: {"issue": f"{s['partition']}#{s['data']['n']}", "answered": t["data"]["at"][11:16]},
        "on_timeout": lambda s: {"issue": f"{s['partition']}#{s['data']['n']}", "unanswered": True},
        **opts,
    }
    return (Stream.from_feed(q, from_="earliest", queues=["issues", "comments"]).pattern(**o)
            .name("unanswered").to(q.queue("out")))


# ---- deadlines on the messages' own times -------------------------------------------------

async def test_history_pushed_all_at_once_a_pattern_times_out_by_the_event_times_not_the_commit_times():
    def load(b):
        issue(b, "r1", 1, 0)
        issue(b, "r1", 2, 10)
        issue(b, "r2", 1, 20)
        comment(b, "r1", 1, 30)
        comment(b, "r2", 1, 85)  # 65 minutes after its issue
        issue(b, "r3", 1, 120)

    b = FakeBroker()
    queen = fake_queen(b)
    load(b)
    runner = await unanswered(queen).run(query_id="history", **fast())
    await until(lambda: len(b.messages("out")) >= 3)
    await wait(30)
    assert payloads(b, "out") == [
        {"issue": "r1#1", "answered": "10:30"},
        {"issue": "r1#2", "unanswered": True},
        {"issue": "r2#1", "unanswered": True},
    ]
    step = runner.metrics()["steps"][0]
    assert step["clock"] == "event"
    assert step["watermark"] == ev(120), "the watermark is the latest event time"
    assert step["open"] == 1, "r3#1 waits: its hour has not passed"
    assert runner.metrics()["watermark"] == ev(120)

    # The same history on the feed's clock: everything is within the hour.
    f = FakeBroker()
    fq = fake_queen(f)
    load(f)
    await (Stream.from_feed(fq, from_="earliest", queues=["issues", "comments"])
           .pattern(start=lambda m: m["queue"] == "issues", then=lambda m: m["queue"] == "comments",
                    key=lambda m: f"{m['partition']}#{m['data']['n']}", within="1h",
                    on_match=lambda s: {"issue": f"{s['partition']}#{s['data']['n']}"})
           .to(fq.queue("out")).run(query_id="history-feed", **fast()))
    await until(lambda: len(f.messages("out")) >= 2)
    assert payloads(f, "out") == [{"issue": "r1#1"}, {"issue": "r2#1"}], "the 65-minute comment matches on commit times"


async def test_a_message_out_of_order_by_less_than_allowed_lateness_takes_its_place_one_later_than_that_is_late():
    def load(b):
        issue(b, "r1", 1, 0)
        issue(b, "r9", 1, 45)
        comment(b, "r1", 1, 30)  # 15 minutes out of order
        issue(b, "r9", 2, 120)

    async def run(opts, query_id):
        b = FakeBroker()
        queen = fake_queen(b)
        load(b)
        runner = await unanswered(queen, **opts).run(query_id=query_id, **fast())
        await until(lambda: runner.metrics()["entriesRead"] >= 4)
        await wait(60)
        return payloads(b, "out"), runner.metrics()

    out, m = await run({"allowed_lateness": "20m"}, "lateness")
    assert out == [{"issue": "r1#1", "answered": "10:30"}], "the comment waited for its place"
    assert m["late"] == 0
    assert m["steps"][0]["held"] == 1, "r9#2 (12:00) waits for the watermark (11:40)"

    out, m = await run({}, "strict")
    assert out == [{"issue": "r1#1", "unanswered": True}, {"issue": "r9#1", "unanswered": True}], \
        "without lateness the comment came after the watermark: dropped, so its issue times out"
    assert m["late"] == 1

    out, m = await run({"on_late": "include"}, "include")
    assert out == [{"issue": "r1#1", "answered": "10:30"}, {"issue": "r9#1", "unanswered": True}], \
        "onLate: 'include' takes the late comment at once"
    assert m["late"] == 1


async def test_a_join_on_event_time_pairs_the_sides_in_either_order_and_its_deadline_counts_on_event_time():
    b = FakeBroker()
    queen = fake_queen(b)
    b.push({"queue": "payments", "partition": "o1", "data": {"at": ev(5)}})
    b.push({"queue": "orders", "partition": "o1", "data": {"at": ev(0)}})
    b.push({"queue": "orders", "partition": "o2", "data": {"at": ev(10)}})
    b.push({"queue": "orders", "partition": "o3", "data": {"at": ev(40)}})
    await (Stream.from_feed(queen, from_="earliest", queues=["orders", "payments"])
           .join(left=lambda m: m["queue"] == "orders", right=lambda m: m["queue"] == "payments", within="15m",
                 event_time=lambda m: m["data"]["at"], allowed_lateness="5m",
                 select=lambda l, r: {"order": l["partition"], "paidAfterSec": (js_date_parse(r["data"]["at"]) - js_date_parse(l["data"]["at"])) / 1000},
                 on_timeout=lambda m, side: {"order": m["partition"], "missing": "payment" if side == "left" else "order"})
           .to(queen.queue("out")).run(query_id="join", **fast()))
    await until(lambda: len(b.messages("out")) >= 2)
    await wait(30)
    assert payloads(b, "out") == [{"order": "o1", "paidAfterSec": 300}, {"order": "o2", "missing": "payment"}]


async def test_a_gather_on_event_time_counts_its_deadline_from_the_earliest_part_whatever_order_the_parts_come_in():
    b = FakeBroker()
    queen = fake_queen(b)

    def part(queue, run, minutes):
        b.push({"queue": queue, "partition": "x", "data": {"run": run, "at": ev(minutes)}})

    part("c", 1, 9)
    part("a", 1, 0)
    part("b", 1, 5)
    part("a", 2, 20)
    part("b", 2, 25)
    part("c", 2, 31)  # 11 minutes after its run's first part
    part("a", "zz", 50)
    await (Stream.from_feed(queen, from_="earliest", queues=["a", "b", "c"])
           .gather(steps=["a", "b", "c"], key=lambda m: str(m["data"]["run"]), within="10m",
                   event_time=lambda m: m["data"]["at"], allowed_lateness="15m",
                   on_complete=lambda got, run: {"run": run, "done": True, "first": got["a"]["data"]["at"][11:16]},
                   on_timeout=lambda got, missing, run: {"run": run, "missing": missing})
           .to(queen.queue("out")).run(query_id="gather", **fast()))
    await until(lambda: len(b.messages("out")) >= 2)
    await wait(30)
    assert payloads(b, "out") == [{"run": "1", "done": True, "first": "10:00"}, {"run": "2", "missing": ["c"]}]


async def test_a_session_on_event_time_takes_its_messages_in_event_time_order():
    b = FakeBroker()
    queen = fake_queen(b)
    for minutes in (0, 20, 10):
        b.push({"queue": "clicks", "partition": "p", "data": {"user": "u", "at": ev(minutes)}})
    b.push({"queue": "clicks", "partition": "p", "data": {"user": "v", "at": ev(120)}})
    await (Stream.from_feed(queen, from_="earliest", queues=["clicks"]).key_by(lambda m: m["data"]["user"])
           .window_session(gap=900, eventTime=lambda m: m["data"]["at"], allowedLateness=1800)
           .aggregate({"clicks": lambda: 1})
           .map(lambda v, ctx: {"user": ctx["key"], "clicks": v["clicks"], "from": js_iso(ctx["windowStart"])[11:16], "to": js_iso(ctx["windowEnd"])[11:16]})
           .to(queen.queue("sessions")).run(query_id="sessions", **fast()))
    await until(lambda: len(b.messages("sessions")) >= 1)
    await wait(30)
    # In the order they came, 10:20 would end the 10:00 session (past the
    # 15-minute gap) before 10:10 joins them.
    assert payloads(b, "sessions") == [{"user": "u", "clicks": 3, "from": "10:00", "to": "10:35"}]


# ---- sessions: the gap ------------------------------------------------------------------

@pytest.mark.parametrize("opts,event_time", [({}, False), ({"gracePeriod": 2}, False), ({}, True), ({"gracePeriod": 2}, True)])
async def test_a_message_exactly_gap_after_the_last_one_is_still_in_the_session(opts, event_time):
    b = FakeBroker()
    queen = fake_queen(b)
    for sec in (0, 10, 21):
        at(b, sec * 1000)
        b.push({"queue": "clicks", "partition": "u1", "data": {"at": ev(0, sec)}})
    at(b, 60000)
    b.push({"queue": "clicks", "partition": "other", "data": {"at": ev(1)}})
    extra = {"eventTime": lambda m: m["data"]["at"]} if event_time else {}
    runner = await (Stream.from_feed(queen, from_="earliest", queues=["clicks"])
                    .window_session(gap=10, **opts, **extra).aggregate({"count": lambda: 1})
                    .map(lambda acc, ctx: {"user": ctx["key"], "count": acc["count"]})
                    .to(queen.queue("sessions")).run(query_id="gap", **fast(now=lambda: b.now_ms)))
    await until(lambda: len(b.messages("sessions")) >= 2)
    await runner.stop()
    # 0 and 10 are 10 s apart: one session; 21 is 11 s after 10: the next.
    assert [p for p in payloads(b, "sessions") if p["user"] == "u1"] == [{"user": "u1", "count": 2}, {"user": "u1", "count": 1}]


# ---- chained steps ----------------------------------------------------------------------

def pushes(b):
    for minutes in (5, 10, 15, 60, 90, 105, 180, 190, 200, 210):
        b.push({"queue": "pushes", "partition": "r1", "data": {"at": ev(minutes)}})
    b.push({"queue": "pushes", "partition": "r2", "data": {"at": ev(310)}})


def busy_hours(q, **pattern_opts):
    o = {
        "start": lambda m: m["data"]["pushes"] >= 3,
        "then": lambda m: m["data"]["pushes"] >= 3,
        "within": "90m",
        "on_match": lambda a, c: {"repo": a["partition"], "busy": [a["data"]["hour"], c["data"]["hour"]]},
        "on_timeout": lambda a: {"repo": a["partition"], "alone": a["data"]["hour"]},
        **pattern_opts,
    }
    return (Stream.from_feed(q, from_="earliest", queues=["pushes"])
            .window_tumbling(seconds=3600, eventTime=lambda m: m["data"]["at"]).aggregate({"pushes": lambda: 1})
            .map(lambda v, ctx: {**v, "hour": ctx["windowKey"][11:13]})
            .pattern(**o).name("busy").to(q.queue("out")))


async def test_a_step_after_a_step_on_event_time_runs_on_the_results_event_times():
    b = FakeBroker()
    queen = fake_queen(b)
    pushes(b)
    runner = await busy_hours(queen).run(query_id="chain", **fast())
    await until(lambda: len(b.messages("out")) >= 2)
    await wait(30)
    # Hours 10 and 11 are within 90 minutes (10:59:59.999 to 11:59:59.999);
    # hour 13 is not, so hour 11's wait times out first.
    assert payloads(b, "out") == [{"repo": "r1", "busy": ["10", "11"]}, {"repo": "r1", "alone": "11"}]
    steps = runner.metrics()["steps"]
    assert [s["clock"] for s in steps] == ["event", "event"]
    # The pattern follows the window step's watermark (15:10: r2's push).
    assert steps[1]["watermark"] == ev(310), "the watermark of the window step: nothing older can come from it"


async def test_event_time_false_keeps_a_later_step_on_the_feeds_clock():
    b = FakeBroker()
    queen = fake_queen(b)
    pushes(b)
    runner = await busy_hours(queen, event_time=False).run(query_id="chain-feed", **fast())
    await until(lambda: len(b.messages("out")) >= 2)
    await wait(30)
    assert payloads(b, "out") == [{"repo": "r1", "busy": ["10", "11"]}, {"repo": "r1", "busy": ["11", "13"]}], \
        "the results came within 90 minutes of each other on the feed's clock"
    assert [s["clock"] for s in runner.metrics()["steps"]] == ["event", "feed"]


# ---- a quiet feed -------------------------------------------------------------------------

async def test_without_messages_the_watermark_stays_with_idle_it_moves_on_with_the_feeds_clock():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    b.push({"queue": "readings", "partition": "s1", "data": {"at": ev(0, 10), "v": 1}})
    b.push({"queue": "readings", "partition": "s1", "data": {"at": ev(0, 50), "v": 2}})

    def sums(idle):
        extra = {"idle": idle} if idle else {}
        return (Stream.from_feed(queen, from_="earliest", queues=["readings"])
                .window_tumbling(seconds=60, eventTime=lambda m: m["data"]["at"], **extra).aggregate({"sum": lambda v: v["v"]})
                .map(lambda acc, ctx: {"minute": ctx["windowKey"][11:16], "sum": acc["sum"]}))

    still = await sums(None).to(queen.queue("still")).run(query_id="no-idle", **fast(now=lambda: b.now_ms))
    runner = await sums("30s").to(queen.queue("sums")).run(query_id="idle", **fast(now=lambda: b.now_ms))
    await until(lambda: runner.metrics()["entriesRead"] >= 2 and still.metrics()["entriesRead"] >= 2)
    # 35 s after the last message: the watermark moved 5 s past 10:00:50.
    at(b, 36000)
    await wait(200)
    assert len(b.messages("sums")) == 0
    # 41 s after: past 10:01:00, the minute closes.
    at(b, 42000)
    await until(lambda: len(b.messages("sums")) >= 1, 3000)
    assert payloads(b, "sums") == [{"minute": "10:00", "sum": 3}]
    assert runner.metrics()["steps"][0]["watermark"] == ev(1, 1), "at 42 s: 10:00:50 + 11 s"
    at(b, 600000)
    await wait(200)
    assert len(b.messages("still")) == 0, "without idle only a message moves the watermark"


async def test_a_finished_import_releases_what_it_held_and_passes_its_last_deadlines_at_the_pace_of_the_feeds_clock():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    b.push({"queue": "issues", "partition": "r1", "data": {"n": 1, "at": ev(0, 0)}})
    b.push({"queue": "issues", "partition": "r1", "data": {"n": 2, "at": ev(0, 5)}})
    b.push({"queue": "comments", "partition": "r1", "data": {"n": 1, "at": ev(0, 12)}})
    runner = await unanswered(queen, within="20s", idle="30s", allowed_lateness="10s").run(query_id="tail", **fast(now=lambda: b.now_ms))
    await until(lambda: runner.metrics()["entriesRead"] >= 3)
    await wait(50)
    # The watermark is at 10:00:02: r1#2 (10:00:05) and the comment (10:00:12) wait for it.
    assert payloads(b, "out") == []
    assert runner.metrics()["steps"][0]["held"] == 2
    # 30 s of quiet, then event time goes on with the feed's clock.
    at(b, 1000 + 45000)
    await until(lambda: len(b.messages("out")) >= 1, 3000)
    await wait(50)
    assert payloads(b, "out") == [{"issue": "r1#1", "answered": "10:00"}]
    # r1#2 is due at 10:00:25.001: 53 s after.
    at(b, 1000 + 54000)
    await until(lambda: len(b.messages("out")) >= 2, 3000)
    assert payloads(b, "out") == [{"issue": "r1#1", "answered": "10:00"}, {"issue": "r1#2", "unanswered": True}]


# ---- restarts ---------------------------------------------------------------------------

async def test_a_stop_and_a_start_keep_the_held_messages_and_the_watermark():
    b = FakeBroker()
    issue(b, "r1", 1, 0)
    issue(b, "r2", 1, 10)
    first = await unanswered(fake_queen(b, "a"), allowed_lateness="30m").run(query_id="restart", **fast())
    await until(lambda: first.metrics()["entriesRead"] >= 2)
    assert first.metrics()["steps"][0]["held"] == 2
    await first.stop()
    comment(b, "r1", 1, 50)
    issue(b, "r3", 1, 200)
    second = await unanswered(fake_queen(b, "b"), allowed_lateness="30m").run(query_id="restart", **fast())
    await until(lambda: len(b.messages("out")) >= 2)
    await wait(30)
    assert payloads(b, "out") == [{"issue": "r1#1", "answered": "10:50"}, {"issue": "r2#1", "unanswered": True}]
    assert second.metrics()["replays"] == 0, "the stop wrote a snapshot: nothing to read again"


async def test_a_replay_closes_again_what_idle_closed_at_the_same_feed_time_and_sends_it_once():
    b = FakeBroker()

    def opts():
        return dict(**fast(now=lambda: b.now_ms, lock_ttl="1s", snapshot_every_millis=3600000), **quiet())

    def chain(q):
        return unanswered(q, within="1m", idle="30s")

    at(b, 1000)
    b.push({"queue": "issues", "partition": "r1", "data": {"n": 1, "at": ev(0, 0)}})
    b.push({"queue": "issues", "partition": "r2", "data": {"n": 1, "at": ev(0, 30)}})
    qa = fake_queen(b, "a")
    a = await chain(qa).run(query_id="idle-replay", **opts())
    await until(lambda: a.metrics()["entriesRead"] >= 2)
    # Quiet: r1#1 (due 10:01:00.001) passes 61.001 s after the last message.
    at(b, 1000 + 62000)
    await until(lambda: len(b.messages("out")) >= 1, 3000)
    # r4#1 keeps the state from going empty (an empty state would move the snapshot here).
    b.push({"queue": "issues", "partition": "r4", "data": {"n": 1, "at": ev(1, 15)}})
    b.push({"queue": "comments", "partition": "r2", "data": {"n": 1, "at": ev(1, 20)}})
    await until(lambda: len(b.messages("out")) >= 2, 3000)
    end = b.messages("comments")[-1]["position"]
    await until(lambda: position_of(b, "idle-replay") > end)
    assert b.kv_value(NS, "idle-replay")["snapshot"]["position"] == 1, "no snapshot since the start"
    crash(qa)
    # The next runner reads it all again: the timeout (idle) and the match come back in the state, and are not sent again.
    second = await chain(fake_queen(b, "b")).run(query_id="idle-replay", **opts())
    await until(lambda: second.metrics()["role"] == "active", 6000)
    assert second.metrics()["replayEntries"] >= 4, f"replayed {second.metrics()['replayEntries']}"
    at(b, 1000 + 70000)
    b.push({"queue": "issues", "partition": "r3", "data": {"n": 1, "at": ev(2, 0)}})
    at(b, 1000 + 200000)
    await until(lambda: len(b.messages("out")) >= 4, 4000)
    await wait(50)
    assert payloads(b, "out") == [
        {"issue": "r1#1", "unanswered": True},
        {"issue": "r2#1", "answered": "10:01"},
        {"issue": "r4#1", "unanswered": True},
        {"issue": "r3#1", "unanswered": True},
    ]


async def test_out_of_order_history_crashed_at_random_points_sends_what_one_uninterrupted_run_sends():
    seed = {"v": 11}

    def rnd():
        # The LCG in 32-bit integers, as JS's Math.imul now computes it: (imul(seed, a) + c) & 0x7fffffff.
        seed["v"] = (seed["v"] * 1103515245 + 12345) & 0x7FFFFFFF
        return seed["v"] / 2147483648

    msgs = []
    e = 0
    for i in range(150):
        e += math.floor(rnd() * 90)
        jitter = math.floor((rnd() - 0.5) * 240)  # up to 2 minutes either way
        msgs.append({"i": i, "repo": f"r{math.floor(rnd() * 6)}", "open": rnd() < 0.5, "sec": max(0, e + jitter)})

    def chain(q):
        return (Stream.from_feed(q, from_="earliest", queues=["issues", "comments"])
                .pattern(start=lambda m: m["queue"] == "issues", then=lambda m: m["queue"] == "comments", within="10m",
                         event_time=lambda m: m["data"]["at"], allowed_lateness="3m", idle="30s",
                         on_match=lambda s, t: {"repo": s["partition"], "s": s["data"]["i"], "t": t["data"]["i"]},
                         on_timeout=lambda s: {"repo": s["partition"], "s": s["data"]["i"], "timeout": True})
                .key_by(lambda v: v["repo"])
                .window_tumbling(seconds=1800).aggregate({"results": lambda: 1})
                .map(lambda v, ctx: {"repo": ctx["key"], "half": ctx["windowKey"][11:16], "results": v["results"]})
                .to(q.queue("out")))

    # Per repo (the sink's partition), in order.
    def by_repo(b):
        m = {}
        for x in b.messages("out"):
            m.setdefault(x["partition"], []).append(x["payload"])
        return m

    def load(b):
        last = 0
        for m in msgs:
            at(b, m["i"] * 1000)
            last = b.push({"queue": "issues" if m["open"] else "comments", "partition": m["repo"],
                           "data": {"i": m["i"], "at": js_iso(H0 + m["sec"] * 1000)}})
        return last

    # The import ends; the feed's clock goes on for 80 minutes, which closes every wait and window (idle).
    async def tail(b, runner):
        for k in range(1, 9):
            at(b, len(msgs) * 1000 + k * 600000)
            await wait(60)

        def settled():
            m = runner.metrics()
            return m["open"] == 0 and all(not st.get("held") for st in m["steps"]) and m["pendingOutputs"] == 0

        await until(settled, 8000, "every wait and window to close")
        await wait(100)

    ref = FakeBroker()
    ref_end = load(ref)
    r = await chain(fake_queen(ref)).run(query_id="rand-et", **fast(now=lambda: ref.now_ms))
    await until(lambda: position_of(ref, "rand-et") > ref_end, 6000)
    await tail(ref, r)
    await r.stop()
    expected = by_repo(ref)
    assert len(expected) == 6 and len(payloads(ref, "out")) >= 12, f"results: {len(payloads(ref, 'out'))}"

    b = FakeBroker()
    end = load(b)

    def opts():
        return dict(**fast(now=lambda: b.now_ms, lock_ttl="1s", max_records=7, snapshot_max_entries=20), **quiet())

    q = fake_queen(b, "p0")
    runner = await chain(q).run(query_id="rand-et", **opts())
    for k in range(1, 4):
        target = math.floor(len(msgs) * k / 4)
        await until(lambda: position_of(b, "rand-et") >= target, 6000)
        crash(q)
        q = fake_queen(b, f"p{k}")
        runner = await chain(q).run(query_id="rand-et", **opts())
        await until(lambda: runner.metrics()["role"] == "active", 6000)
    await until(lambda: position_of(b, "rand-et") > end, 6000)
    await tail(b, runner)
    await runner.stop()
    assert by_repo(b) == expected


# ---- failures and checks ------------------------------------------------------------------

async def test_a_held_message_whose_step_keeps_failing_meets_the_poison_policy_as_the_message_it_is():
    b = FakeBroker()
    queen = fake_queen(b)
    issue(b, "bad", 1, 0)
    issue(b, "good", 1, 0)
    comment(b, "bad", 1, 5)
    comment(b, "good", 1, 6)
    issue(b, "later", 1, 30)

    def on_match(s):
        if s["partition"] == "bad":
            raise RuntimeError("boom")
        return {"issue": f"{s['partition']}#{s['data']['n']}", "answered": True}

    runner = await unanswered(queen, allowed_lateness="10m", on_match=on_match).run(query_id="poison-held", **fast(on_poison=queen.queue("dlq")), **quiet())
    await until(lambda: len(b.messages("out")) >= 1 and len(b.messages("dlq")) >= 1)
    await wait(30)
    assert payloads(b, "out") == [{"issue": "good#1", "answered": True}]
    record = payloads(b, "dlq")[0]
    assert record["entry"]["queue"] == "comments", "the comment that made the failing match"
    assert record["entry"]["partition"] == "bad"
    assert record["error"]["message"] == "boom"
    assert runner.metrics()["deadLettered"] == 1


async def test_a_live_read_lists_what_a_step_keeps_not_the_messages_it_holds_for_order():
    b = FakeBroker()
    queen = fake_queen(b)
    issue(b, "r1", 1, 0)
    issue(b, "r2", 1, 5)
    runner = await unanswered(queen, allowed_lateness="1h").run(query_id="live", **fast())
    await until(lambda: runner.metrics()["entriesRead"] >= 2)
    assert runner.state("unanswered").list() == {"items": [], "truncated": False, "total": 0}
    assert runner.metrics()["steps"][0]["held"] == 2
    issue(b, "r3", 1, 62)
    await until(lambda: runner.metrics()["steps"][0]["open"] == 1)
    lst = runner.state("unanswered").list()
    assert lst["total"] == 1
    assert lst["items"][0]["key"] == "r1#1"
    assert runner.state("unanswered").get("r1#1")["found"] is True


async def test_refuses_idle_allowed_lateness_and_on_late_on_the_feeds_clock_idle_on_a_queue_source_and_a_non_function_event_time():
    b = FakeBroker()
    queen = fake_queen(b)

    def pattern(**extra):
        return (Stream.from_feed(queen, from_="earliest")
                .pattern(start=lambda m: True, then=lambda m: True, within="1m", **extra).to(queen.queue("out")))

    with pytest.raises(ValueError, match="idle moves a watermark on"):
        await pattern(idle="30s").run(query_id="v1", **fast())
    with pytest.raises(ValueError, match="allowedLateness and onLate are for event time"):
        await pattern(allowed_lateness="1m").run(query_id="v2", **fast())
    with pytest.raises(ValueError, match="allowedLateness and onLate are for event time"):
        await pattern(on_late="include").run(query_id="v3", **fast())
    def window(**extra):
        return (Stream.from_feed(queen, from_="earliest").window_tumbling(seconds=60, **extra)
                .aggregate({"n": lambda: 1}).to(queen.queue("out")))

    with pytest.raises(ValueError, match="idle moves a watermark on"):
        await window(idle=30).run(query_id="v4", **fast())
    with pytest.raises(ValueError, match="allowedLateness and onLate are for event time: the tumbling window"):
        await window(allowed_lateness=30).run(query_id="v6", **fast())
    with pytest.raises(ValueError, match="allowedLateness and onLate are for event time: the tumbling window"):
        await window(on_late="include").run(query_id="v7", **fast())
    with pytest.raises(ValueError, match="allowedLateness and onLate are for event time: the session window"):
        await (Stream.from_feed(queen, from_="earliest").window_session(gap=60, allowed_lateness="1m")
               .aggregate({"n": lambda: 1}).to(queen.queue("out")).run(query_id="v8", **fast()))
    # The plain values pass: no lateness, late messages dropped.
    plain = await window(allowed_lateness=0, on_late="drop").run(query_id="v9", **fast())
    await plain.stop()

    class Src:
        _queue_name = "src"

    with pytest.raises(ValueError, match="runs on Stream.from_feed only"):
        await (Stream.from_(Src()).window_tumbling(seconds=60, idle=30).aggregate({"n": lambda: 1})
               .to({"name": "out"}).run(query_id="v5", url="http://127.0.0.1:1"))
    with pytest.raises(ValueError, match="eventTime is a function"):
        pattern(event_time="data.at")
    with pytest.raises(ValueError, match="idle is a duration above zero"):
        pattern(event_time=lambda m: m["data"]["at"], idle=0)


# ---- what the code review found -----------------------------------------------------------

async def test_a_step_after_a_step_on_event_time_follows_its_watermark_a_quiet_feed_sends_nothing_the_step_before_still_has_as_late():
    b = FakeBroker()
    q = fake_queen(b)

    def push(ms, queue, n):
        at(b, ms)
        b.push({"queue": queue, "partition": "r1", "data": {"n": n, "at": js_iso(T0 + ms)}})

    push(0, "issues", 1)
    push(300000, "issues", 2)
    push(400000, "comments", 2)
    runner = await (Stream.from_feed(q, from_="earliest", queues=["issues", "comments"])
                    .pattern(start=lambda m: m["queue"] == "issues", then=lambda m: m["queue"] == "comments",
                             key=lambda m: f"{m['partition']}#{m['data']['n']}", within="10m",
                             event_time=lambda m: m["data"]["at"], idle="1m",
                             on_match=lambda s: {"issue": s["data"]["n"], "answered": True},
                             on_timeout=lambda s: {"issue": s["data"]["n"], "answered": False})
                    .key_by(lambda v: "r1").window_tumbling(seconds=3600).aggregate({"results": lambda: 1})
                    .to(q.queue("hourly")).run(query_id="review-h1", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["entriesRead"] >= 3)
    for ms in (500000, 700000, 1200000, 3000000, 4000000, 5000000, 6000000):
        at(b, ms)
        await wait(60)
    await until(lambda: len(b.messages("hourly")) >= 1, 3000)
    await wait(50)
    assert runner.metrics()["late"] == 0
    assert payloads(b, "hourly") == [{"results": 2}]


async def test_a_window_after_a_window_every_reading_counted_once_with_idle_on_the_first_only():
    b = FakeBroker()
    q = fake_queen(b)
    # A message every 20 s of the feed's clock; event time at a third of that pace.
    for k in range(60):
        at(b, k * 20000)
        b.push({"queue": "r", "partition": "s", "data": {"at": js_iso(H0 + math.floor(k * 20000 / 3))}})
    runner = await (Stream.from_feed(q, from_="earliest", queues=["r"])
                    .window_tumbling(seconds=60, eventTime=lambda m: m["data"]["at"], idle="30s").aggregate({"n": lambda: 1})
                    .window_tumbling(seconds=3600).aggregate({"n": lambda v: v["n"]})
                    .to(q.queue("out")).run(query_id="review-h1b", **fast(now=lambda: b.now_ms)))
    await until(lambda: runner.metrics()["entriesRead"] >= 60)
    for k in range(1, 9):
        at(b, 60 * 20000 + k * 900000)
        await wait(60)
    await until(lambda: len(b.messages("out")) >= 1, 3000)
    await wait(50)
    assert runner.metrics()["late"] == 0
    assert payloads(b, "out") == [{"n": 60}]


async def test_idle_goes_on_the_first_step_on_event_time_only():
    b = FakeBroker()
    q = fake_queen(b)

    def first():
        return (Stream.from_feed(q, from_="earliest")
                .window_tumbling(seconds=60, eventTime=lambda m: m["data"]["at"]).aggregate({"n": lambda: 1}))

    with pytest.raises(ValueError, match="follows its watermark: put idle on the first step"):
        await first().window_tumbling(seconds=3600, idle=30).aggregate({"n": lambda v: v["n"]}).to(q.queue("out")).run(query_id="v-fed-idle", **fast())
    with pytest.raises(ValueError, match="results of the step before it"):
        await (first().pattern(start=lambda m: True, then=lambda m: True, within="1m", event_time=lambda m: m["ts"], idle="30s")
               .to(q.queue("out")).run(query_id="v-fed-own", **fast()))


async def test_a_held_message_whose_unit_failed_leaves_nothing_behind_that_releases_the_next_one_early():
    async def run(with_bad):
        b = FakeBroker()
        q = fake_queen(b)
        # One transaction: an item of k1 is held, then an item without a kind fails the unit
        # (skipped); k2's part a at 10:20 is held next, under the same number.
        e1 = {"queue": "ev", "partition": "p", "data": {"at": ev(0), "items": [{"kind": "a", "id": "k1"}, {"id": "bad"}] if with_bad else []}}
        b.push([e1, {"queue": "ev", "partition": "p", "data": {"at": ev(20), "items": [{"kind": "a", "id": "k2"}]}}])
        b.push({"queue": "ev", "partition": "p", "data": {"at": ev(31), "items": [{"kind": "a", "id": "k3"}]}})
        b.push({"queue": "ev", "partition": "p", "data": {"at": ev(10), "items": [{"kind": "b", "id": "k2"}]}})
        b.push({"queue": "ev", "partition": "p", "data": {"at": ev(200), "items": [{"kind": "a", "id": "k9"}]}})

        def is_b(m, v):
            return v["kind"].startswith("b")  # an item without a kind raises, as JS's v.kind.startsWith does

        runner = await (Stream.from_feed(q, from_="earliest", queues=["ev"]).flat_map(lambda m: m["data"]["items"])
                        .gather(steps={"a": lambda m, v: v.get("kind") == "a", "b": is_b}, key=lambda m, v: v["id"], within="5m",
                                event_time=lambda m: m["data"]["at"], allowed_lateness="30m",
                                on_complete=lambda got, key: {"key": key, "done": True},
                                on_timeout=lambda got, missing, key: {"key": key, "missing": missing})
                        .to(q.queue("out")).run(query_id="review-h2", **fast(on_poison="skip"), **quiet()))
        await until(lambda: runner.metrics()["entriesRead"] >= 5)
        await wait(80)
        return [p for p in payloads(b, "out") if p["key"] == "k2"], runner.metrics()["skipped"]

    want = [{"key": "k2", "missing": ["a"]}, {"key": "k2", "missing": ["b"]}]
    out, _ = await run(False)
    assert out == want
    out, skipped = await run(True)
    assert skipped == 1
    assert out == want


async def test_two_held_items_of_one_message_that_both_fail_go_to_the_dead_letter_queue_each_once():
    b = FakeBroker()
    q = fake_queen(b)
    b.push({"queue": "issues", "partition": "r1", "data": {"ids": ["a", "b"], "bad": True, "at": ev(0)}})
    b.push({"queue": "comments", "partition": "r1", "data": {"ids": ["a", "b"], "at": ev(5)}})
    b.push({"queue": "issues", "partition": "r1", "data": {"ids": ["c"], "at": ev(30)}})

    def on_match(s):
        if s["data"].get("bad"):
            raise RuntimeError("boom")
        return {"ok": True}

    runner = await (Stream.from_feed(q, from_="earliest", queues=["issues", "comments"])
                    .flat_map(lambda m: [{"id": i} for i in m["data"]["ids"]])
                    .pattern(start=lambda m: m["queue"] == "issues", then=lambda m: m["queue"] == "comments",
                             key=lambda m, v: v["id"], within="1h", event_time=lambda m: m["data"]["at"], on_match=on_match)
                    .to(q.queue("out")).run(query_id="review-h3", **fast(on_poison=q.queue("dlq")), **quiet()))
    await until(lambda: len(b.messages("dlq")) >= 2, 3000)
    await wait(80)
    assert len(b.messages("dlq")) == 2
    assert len({e["transactionId"] for e in b.messages("dlq")}) == 2
    assert runner.metrics()["conflicts"] <= 1, f"conflicts: {runner.metrics()['conflicts']}"


async def test_an_invalid_date_meets_the_poison_policy_a_window_takes_allowed_lateness_as_a_duration_too():
    b = FakeBroker()
    q = fake_queen(b)
    b.push({"queue": "issues", "partition": "r1", "data": {"n": 1, "at": ev(0)}})
    b.push({"queue": "issues", "partition": "r1", "data": {"n": 2}})
    b.push({"queue": "comments", "partition": "r1", "data": {"n": 1, "at": ev(5)}})
    for k in range(3, 10):
        b.push({"queue": "issues", "partition": "r1", "data": {"n": k, "at": ev(60 * k)}})

    def as_date(m):
        # new Date(undefined) in JS is an Invalid Date; a datetime of nothing has no time either.
        at_ = m["data"].get("at")
        return datetime.fromtimestamp(js_date_parse(at_) / 1000, tz=timezone.utc) if at_ else None

    runner = await unanswered(q, event_time=as_date).run(query_id="review-m1", **fast(on_poison="skip"), **quiet())
    await until(lambda: runner.metrics()["entriesRead"] >= 10)
    await until(lambda: len(b.messages("out")) >= 3)
    assert runner.metrics()["skipped"] == 1
    assert payloads(b, "out")[:2] == [{"issue": "r1#1", "answered": "10:05"}, {"issue": "r1#3", "unanswered": True}]

    w = FakeBroker()
    wq = fake_queen(w)
    for minutes in (0, 1, 2, 30, 31, 90):
        w.push({"queue": "r", "partition": "s", "data": {"at": ev(minutes)}})
    await (Stream.from_feed(wq, from_="earliest", queues=["r"])
           .window_tumbling(seconds=600, eventTime=lambda m: m["data"]["at"], allowedLateness="5m").aggregate({"n": lambda: 1})
           .to(wq.queue("out")).run(query_id="review-m1b", **fast()))
    await until(lambda: len(w.messages("out")) >= 2)
    assert payloads(w, "out") == [{"n": 3}, {"n": 2}]


async def test_a_message_a_pattern_ignores_needs_no_time_and_moves_no_watermark():
    b = FakeBroker()
    q = fake_queen(b)
    b.push({"queue": "issues", "partition": "r1", "data": {"n": 1, "at": ev(0)}})
    b.push({"queue": "audit", "partition": "x", "data": {"who": "bot"}})  # no time at all
    b.push({"queue": "audit", "partition": "x", "data": {"at": ev(60 * 24 * 365)}})  # a year later
    b.push({"queue": "comments", "partition": "r1", "data": {"n": 1, "at": ev(5)}})
    b.push({"queue": "issues", "partition": "r2", "data": {"n": 1, "at": ev(30)}})
    runner = await (Stream.from_feed(q, from_="earliest")
                    .pattern(start=lambda m: m["queue"] == "issues", then=lambda m: m["queue"] == "comments",
                             key=lambda m: f"{m['partition']}#{m['data']['n']}", within="1h", allowed_lateness="10m",
                             event_time=lambda m: m["data"]["at"],
                             on_match=lambda s: {"answered": s["data"]["n"]}, on_timeout=lambda s: {"unanswered": s["data"]["n"]})
                    .to(q.queue("out")).run(query_id="review-m2", **fast(), **quiet()))
    await until(lambda: runner.metrics()["entriesRead"] >= 5)
    await until(lambda: len(b.messages("out")) >= 1)
    await wait(50)
    assert runner.metrics()["stopped"] is None
    assert runner.metrics()["late"] == 0
    assert payloads(b, "out") == [{"answered": 1}]


def test_the_chain_hash_says_which_steps_take_the_event_time_of_the_step_before():
    q = fake_queen(FakeBroker())

    def hash_of(s):
        return s.to(q.queue("out"))._compile_feed().chain_hash

    def win():
        return Stream.from_feed(q).window_tumbling(seconds=60, eventTime=lambda m: m["data"]["at"]).aggregate({"n": lambda: 1})

    def pat(**o):
        return {"start": lambda m: True, "then": lambda m: True, "within": "1m", **o}

    assert hash_of(win().pattern(**pat())) != hash_of(win().pattern(**pat(event_time=False))), "inherited event time vs the feed clock"

    def plain():
        return Stream.from_feed(q).window_tumbling(seconds=60).aggregate({"n": lambda: 1})

    assert hash_of(plain()) == hash_of(Stream.from_feed(q).window_tumbling(seconds=60, eventTime=False).aggregate({"n": lambda: 1}))
