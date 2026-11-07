"""Byte compatibility with the JS SDK: the vectors in js_vectors.json were made
by the JS SDK itself (the frozen copy the port follows (jsref5), TZ=Europe/Rome), and
Python must give the same bytes: chain hashes, the stream's description
(`#about`), ids, queue names, number and date text, the shard hash, and a
snapshot piece JS wrote."""

import json
import math
import os
import time

import pytest

from queen.streams.runtime.feed.about import clip, describe_feed_stream
from queen.streams.runtime.feed.pipeline import Pipeline
from queen.streams.runtime.feed.query import query_queue_name
from queen.streams.runtime.feed.snapshot import _inflate_raw, _unb64
from queen.streams.runtime.feed.state import fnv1a32, shard_of
from queen.streams.runtime.feed_runner import _sha, feed_output_id, output_prefix, shard_id
from queen.streams.stream import Stream
from queen.streams.util.js import js_date_parse, js_iso, js_json_dumps, js_number_str

V = json.load(open(os.path.join(os.path.dirname(__file__), "js_vectors.json"), encoding="utf-8"))


class _Q:
    def __init__(self, name):
        self.name = name


class _Client:
    def feed(self, *a, **k):
        return None

    def transaction(self):
        return None

    def queue(self, name):
        return _Q(name)


def f(*a):
    return 1


def chains():
    q = _Client()
    return {
        "tumbling": Stream.from_feed(q, queues=["orders"]).window_tumbling(seconds=60).aggregate({"count": f}).name("perMinute").to(q.queue("counts")),
        "slidingKeyed": Stream.from_feed(q).key_by(f).window_sliding(size=60, slide=30, gracePeriod=1.5).aggregate({"count": f, "sum": f, "avg": f}).to(q.queue("x")),
        "session": Stream.from_feed(q).window_session(gap=10, gracePeriod=2, allowedLateness="30s", eventTime=f).reduce(f, {"sum": 0}).to(q.queue("x")),
        "sessionEt": Stream.from_feed(q).window_session(gap=0.5, eventTime=f, allowedLateness=5, idle=30).reduce(f).to(q.queue("x")),
        "reduceNull": Stream.from_feed(q).window_tumbling(seconds=1.5).reduce(f, None).to(q.queue("x")),
        "cron": Stream.from_feed(q).window_cron(every="hour", idle="1m", eventTime=f).aggregate({"n": f}).to([q.queue("a"), q.queue("b")]),
        "pattern": Stream.from_feed(q).where("data.x", "=", 1).pattern(start=f, then=f, within="15m", on_timeout=f, event_time=f,
                                                                       allowed_lateness="5m", idle="1m", on_late="include", name="p1").to(f),
        "joinTwo": Stream.from_feed(q).join(left=f, right=f, within="1h", select=f, on_repeat="restart").foreach(f),
        "joinSides": Stream.from_feed(q).join(sides=["orders", "payments", "shipments"], within="10m", select=f, on_timeout=f).to(q.queue("x")),
        "gatherSteps": Stream.from_feed(q).gather(steps={"a": "qa", "b": f}, key=f, within="5m", on_repeat="latest").to(q.queue("x")),
        "gatherCount": Stream.from_feed(q).gather(count=f, within="1h30m", on_complete=f).to(q.queue("x")),
        "table": Stream.from_feed(q).join_table(queue=["s1", "s2", "s1"], on=f, key=f).name("t").map(f).to(q.queue("x")),
        "inherit": Stream.from_feed(q).window_tumbling(seconds=60, eventTime=f).aggregate({"n": f}).pattern(start=f, then=f, within="1m").to(q.queue("x")),
        "inheritOff": Stream.from_feed(q).window_tumbling(seconds=60, eventTime=f).aggregate({"n": f}).pattern(start=f, then=f, within="1m", event_time=False).to(q.queue("x")),
        "twoWindows": Stream.from_feed(q).window_tumbling(seconds=60).aggregate({"count": f}).key_by(f).window_tumbling(seconds=120)
        .aggregate({"total": f, "keys": f}).name("total").to(q.queue("x")),
    }


@pytest.mark.parametrize("name", sorted(V["hashes"]))
def test_the_chain_hash_is_the_js_one(name):
    c = chains()[name]._compile_feed()
    assert c.chain_hash == V["hashes"][name]
    assert c.config_hash == V["configHashes"][name]


@pytest.mark.parametrize("name", sorted(V["abouts"]))
def test_the_description_is_the_js_one(name):
    c = chains()[name]._compile_feed()
    p = Pipeline(c.steps)
    about = describe_feed_stream(stream=c, pipeline=p, query_id=name, shard=None, shard_by="partition", poison={"kind": "stop"},
                                 exclusive=True, lock_ttl="15s", position_commit_millis=5000)
    del about["client"]
    assert json.loads(js_json_dumps(about)) == V["abouts"][name]


def test_numbers_print_as_js_prints_them():
    for x, want in V["numberStrings"]:
        assert js_number_str(x) == want, x


def test_times_print_and_parse_as_js_does():
    for t, want in V["times"]:
        assert js_iso(t) == want, t
    if time.strftime("%Z") in ("CET", "CEST") or os.environ.get("TZ") == "Europe/Rome":
        for s, want in V["parses"]:
            assert js_date_parse(s) == want, s
    else:  # only the forms with an offset (local time differs elsewhere)
        for s, want in V["parses"]:
            if s.endswith(("Z", "z", "GMT")) or "+" in s[10:] or len(s) == 10:
                assert js_date_parse(s) == want, s


def test_the_shard_hash_is_the_js_one():
    for s, h, shard in V["fnv"]:
        assert fnv1a32(s) == h, s
        assert shard_of(s, 7) == shard, s


def test_ids_and_queue_names_are_the_js_ones():
    ids = V["ids"]
    entry = {"position": 5, "queue": "orders", "partition": "c1", "offset": 3}
    assert output_prefix("orders-it") == ids["prefix"]
    assert feed_output_id("orders-it", entry, 0) == ids["out0"]
    assert feed_output_id("q", entry, 2, 1, shard_id("q", {"index": 0, "count": 2})) == ids["out2"]
    assert shard_id("q", {"index": 1, "count": 3}) == ids["shardId"]
    assert query_queue_name("reply-time", 0) == ids["queue0"]
    assert query_queue_name("reply-time", 3) == ids["queue3"]
    assert f"{output_prefix('q')}{_sha('q' + chr(10) + '0' + chr(10) + 'emit' + chr(10) + '104' + chr(10) + '7')[:40]}" == ids["emit"]


def test_a_snapshot_piece_written_by_js_reads_here_and_the_same_piece_text_is_written_here():
    raw = _inflate_raw(_unb64(V["piece"]["chunk"]))
    assert raw.decode("utf-8") == V["piece"]["text"]
    piece = json.loads(raw)
    set_text = ",".join(js_json_dumps(kv) for kv in piece["set"])
    del_text = ",".join(js_json_dumps(k) for k in piece["del"])
    assert f'{{"v":2,"s":0,"k":{js_json_dumps(piece["k"])},"set":[{set_text}],"del":[{del_text}]}}' == V["piece"]["text"]


def test_clip_cuts_as_js_does():
    got = [clip("x" * 250), clip(list(range(60))), clip({"a": {"b": {"c": {"d": {"e": 1}}}}}), clip(math.nan), clip(f), clip({"f": f, "u": None, "n": None})]
    want = V["clips"]
    assert got[:5] == want[:5]
    # JS drops an undefined field and keeps a null one; None is JSON null here.
    assert got[5] == {"u": None, "n": None} and want[5] == {"n": None}
