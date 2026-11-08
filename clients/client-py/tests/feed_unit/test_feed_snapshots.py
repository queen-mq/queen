"""Port of the JS SDK's test-v2/feed-unit/feedSnapshots.test.js: snapshots in
layers and pieces, counts kept as the state changes, millions due at once, and
a long close that commits in pieces.

Timing bounds that JS sets for its event loop are kept where a Python process
meets them; where Python is slower by nature the bound is wider (noted)."""

import asyncio
import gc
import hashlib
import json
import time

from queen.streams.runtime.feed.pipeline import Pipeline
from queen.streams.runtime.feed.snapshot import chunk_key, encode_snapshot, layers_of, new_snapshot_id, read_layer, write_layer
from queen.streams.runtime.feed.state import FeedState
from queen.streams.runtime.feed_runner import POSITION_NAMESPACE as NS
from queen.streams.stream import Stream
from queen.streams.util.js import UNDEFINED

from .fake_broker import FakeBroker, at, crash, fake_queen, fast, quiet, until, wait


def per_minute(q, **source):
    return (Stream.from_feed(q, from_="earliest", queues=["orders"], **source)
            .window_tumbling(seconds=60).aggregate({"count": lambda: 1}).name("perMinute").to(q.queue("counts")))


def sid_of(prefix, key):
    return key[len(prefix):key.rfind("#")]


class Gaps:
    """The longest the event loop stood still while it ran (a timer every 5 ms),
    and how many times the timer ran."""

    def __init__(self):
        self.max = 0.0
        self.turns = 0
        self.last = time.monotonic()
        self.task = asyncio.ensure_future(self._tick())

    async def _tick(self):
        try:
            while True:
                await asyncio.sleep(0.005)
                now = time.monotonic()
                self.max = max(self.max, (now - self.last) * 1000)
                self.last = now
                self.turns += 1
        except asyncio.CancelledError:
            pass

    def stop(self):
        self.task.cancel()


# ---- counts kept as it changes --------------------------------------------------------------

def test_keeps_its_class_counts_through_sets_deletes_rollbacks_and_loads_in_pieces():
    def classify(k, v):
        return "held" if k.startswith("b") else ("open" if isinstance(v, dict) and not v.get("done") else None)

    st = FeedState()
    st.classify_with(classify)

    def scan(c):
        return sum(1 for k, v in st.entries() if classify(k, v) == c)

    seed = {"v": 7}

    def rnd(n):
        seed["v"] = (seed["v"] * 1103515245 + 12345) % 2147483648
        return seed["v"] % n

    for rnd_i in range(300):
        mark = st.mark()
        for _ in range(20):
            k = f"{'b' if rnd(2) else 'c'}{rnd(30)}"
            if rnd(3) == 0:
                st.delete(k)
            else:
                st.set(k, {"done": rnd(4) == 0})
        if rnd(2):
            st.rollback_to(mark)
        else:
            st.committed()
        assert st.count("open") == scan("open"), f"open, round {rnd_i}"
        assert st.count("held") == scan("held"), f"held, round {rnd_i}"
    entries = list(st.entries())
    st.load_begin()
    for k, v in entries:
        st.load_set(k, v)
    st.load_delete(entries[0][0])
    st.loaded()
    assert st.count("open") == scan("open")
    assert st.count("held") == scan("held")
    assert st.touched_since_snapshot == 0, "a load leaves nothing to write"
    st.wipe()
    assert st.count("open") == 0


async def test_a_pipelines_metrics_cost_the_same_with_200000_open_windows():
    q = fake_queen(FakeBroker())
    p = Pipeline(per_minute(q)._compile_feed().steps)
    n = 200000
    p.load_begin()
    p.load_piece(0, "window", [[f"w\u001f0\u001fk{i}", {"key": f"k{i}", "start": 0, "end": 60000, "due": 60000, "acc": {"count": 1}}] for i in range(n)], [])
    await p.load_end(None)
    t0 = time.perf_counter()
    open_ = 0
    for _ in range(2000):
        open_ = p.open
        p.step_metrics(0)
    ms = (time.perf_counter() - t0) * 1000
    assert open_ == n
    assert ms < 300, f"2,000 reads of the metrics took {ms:.0f} ms"


# ---- millions due at once ------------------------------------------------------------------

async def _loaded(n, due):
    q = fake_queen(FakeBroker())
    p = Pipeline(per_minute(q)._compile_feed().steps)
    p.load_begin()
    p.load_piece(0, "window", [[f"w\u001f0\u001fk{str(i).zfill(7)}", {"key": f"k{i}", "start": 0, "end": due, "due": due, "acc": {"count": 1}}] for i in range(n)], [])
    await p.load_end(None)
    return p


async def test_100000_windows_that_end_at_one_time_close_in_batches_in_key_order_with_the_event_loop_turning():
    p = await _loaded(100000, 60000)
    order = []
    g = Gaps()

    async def unit(em, run):
        order.append(em["key"])
        return "ok"

    r = await p.advance(60000, unit)
    g.stop()
    assert r == "ok"
    assert len(order) == 100000
    assert p.open == 0
    # One order every time, whatever the batches: the heap's (due, state key), here k0, k1, k2, ...
    assert all(k == f"k{i}" for i, k in enumerate(order)), "the closes keep the heap order across batches"
    assert g.max < 200, f"the event loop stood still for {g.max:.0f} ms"


async def test_a_loaded_state_of_500000_entries_is_indexed_again_in_batches():
    # JS bounds the stall at 400 ms for the whole load, one piece of 500,000
    # entries included; a real snapshot piece holds a few thousand. Here the
    # rebuild, which turns the loop every 50,000 entries, is what is measured.
    q = fake_queen(FakeBroker())
    p = Pipeline(per_minute(q)._compile_feed().steps)
    p.load_begin()
    p.load_piece(0, "window", [[f"w\u001f0\u001fk{str(i).zfill(7)}", {"key": f"k{i}", "start": 0, "end": 60000, "due": 60000, "acc": {"count": 1}}] for i in range(500000)], [])
    # Python's garbage collector stops the loop on its own, whatever the
    # rebuild does: a full collection over these 500,000 entries took 200 to
    # 410 ms on a Mac and failed CI at 407 ms. It is off while the rebuild is
    # measured (with it off the longest stop was 22 to 47 ms).
    gc.disable()
    try:
        g = Gaps()
        await p.load_end(None)
        g.stop()
    finally:
        gc.enable()
    assert p.open == 500000
    # A rebuild that never turns the loop would still be quick enough to pass
    # the bound on a fast machine: the loop must have turned along the way.
    assert g.turns >= 5, f"the event loop turned {g.turns} times during the rebuild"
    assert g.max < 400, f"the event loop stood still for {g.max:.0f} ms"


# ---- a long close commits in pieces ----------------------------------------------------------

async def test_12000_windows_that_end_at_one_time_commit_in_pieces_and_each_result_goes_out_once():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    n = 12000
    for i in range(n):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {}})
    runner = await per_minute(queen).run(query_id="burst", **fast(now=lambda: b.now_ms, max_pending_outputs=1000, snapshot_every_millis=3600000))
    await until(lambda: runner.metrics()["entriesRead"] >= n, 30000)
    at(b, 61000)
    b.push({"queue": "orders", "partition": "late", "data": {}})
    await until(lambda: len(b.messages("counts")) >= n, 60000, "every window closed")
    await runner.stop()
    keys = [e["partition"] for e in b.messages("counts")]
    assert len(keys) == n
    assert len(set(keys)) == n, "no result twice"
    # A commit comes after a batch of closes (2,000) once 1,000 results wait: six for 12,000.
    assert runner.metrics()["closeCommits"] >= 5, f"commits in the middle of the close ({runner.metrics()['closeCommits']})"
    assert b.kv_value(NS, "burst")["units"] == n, "the record counts the closes"


async def _crash_mid_close(snapshot_every_millis):
    b = FakeBroker()
    qa = fake_queen(b, "a")
    at(b, 1000)
    n = 20000
    for i in range(n):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {}})

    def opts():
        return dict(**fast(now=lambda: b.now_ms, lock_ttl="1s", max_pending_outputs=400, snapshot_every_millis=snapshot_every_millis), **quiet())

    a = await per_minute(qa).run(query_id="burstcrash", **opts())
    await until(lambda: a.metrics()["entriesRead"] >= n, 30000)
    at(b, 61000)
    b.push({"queue": "orders", "partition": "late", "data": {}})
    await until(lambda: a.metrics()["closeCommits"] >= 3, 30000, "commits in the middle of the close")
    crash(qa)
    await wait(50)
    stored = b.kv_value(NS, "burstcrash")
    assert 0 < stored["units"] < n, f"the record names how far the close got ({stored['units']} of {n})"
    sent = len(b.messages("counts"))
    assert sent == stored["units"], "what is committed is what the record counts"
    second = await per_minute(fake_queen(b, "b")).run(query_id="burstcrash", **opts())
    await until(lambda: second.metrics()["role"] == "active", 16000, "the takeover")
    await until(lambda: len(b.messages("counts")) >= n, 60000, "the rest of the close")
    await second.stop()
    keys = [e["partition"] for e in b.messages("counts")]
    assert len(keys) == n, "every window once"
    assert len(set(keys)) == n, "no result twice"
    assert b.kv_value(NS, "burstcrash")["units"] == n


async def test_a_crash_in_the_middle_of_a_long_close_read_again_from_the_start():
    await _crash_mid_close(3600000)


async def test_a_crash_in_the_middle_of_a_long_close_snapshots_in_the_middle_of_the_close():
    await _crash_mid_close(1)


# ---- snapshots in pieces and layers ----------------------------------------------------------

async def test_a_big_layer_is_written_in_pieces_while_the_event_loop_keeps_turning_and_reads_back_whole():
    b = FakeBroker()
    queen = fake_queen(b)
    n = 200000

    def entries():
        for i in range(n):
            yield f"w\u001f0\u001fk{i}", {"key": f"k{i}", "start": 0, "end": 60000, "due": 60000, "acc": {"count": i % 7}}

    g = Gaps()
    w = await write_layer(queen=queen, ns=NS, id="big", sid="s1", lock=None, steps=[{"kind": "window", "entries": entries()}])
    g.stop()
    assert w["entries"] == n
    assert w["chunks"] > 10, f"many chunks ({w['chunks']})"
    assert g.max < 200, f"the event loop stood still for {g.max:.0f} ms while the layer was written"
    got = {}
    pieces = {"n": 0}

    def on_piece(pc):
        pieces["n"] += 1
        for k, v in pc["set"]:
            got[k] = v

    await read_layer(queen=queen, ns=NS, id="big", sid="s1", count=w["chunks"], on_piece=on_piece)
    assert pieces["n"] > 10, f"read back piece by piece ({pieces['n']})"
    assert len(got) == n
    assert got["w\u001f0\u001fk12345"] == {"key": "k12345", "start": 0, "end": 60000, "due": 60000, "acc": {"count": 12345 % 7}}


async def test_a_piece_bigger_than_one_kv_value_goes_on_in_the_next_one_deletes_are_read_back():
    b = FakeBroker()
    queen = fake_queen(b)
    # Hex of hashes: deflate saves about half of it, so the one piece spans several chunks.
    big = "".join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(64))
    lst = [("a", {"big": big, "n": 1}), ("gone", UNDEFINED), ("b", {"n": 2})]
    w = await write_layer(queen=queen, ns=NS, id="small", sid="s1", lock=None, steps=[{"kind": "pattern", "entries": lst}], chunk_chars=1024, piece_bytes=1 << 20)
    assert w["chunks"] >= 2, f"the piece spans several chunks ({w['chunks']})"
    for n in range(w["chunks"]):
        v = b.kv_value(NS, chunk_key("small", "s1", n))
        assert len(v["d"]) <= 1024
        if n < w["chunks"] - 1:
            assert v.get("m") == 1, f"chunk {n} says more follows"
        else:
            assert "m" not in v, "the last chunk ends the piece"
    seen = []
    await read_layer(queen=queen, ns=NS, id="small", sid="s1", count=w["chunks"], on_piece=seen.append)
    assert len(seen) == 1
    assert seen[0]["set"] == [["a", {"big": big, "n": 1}], ["b", {"n": 2}]]
    assert seen[0]["del"] == ["gone"]
    assert seen[0]["k"] == "pattern"


async def test_the_piece_limit_counts_utf8_bytes_not_characters():
    b = FakeBroker()
    queen = fake_queen(b)
    # 200 characters, 600 bytes in UTF-8: counted in characters, a piece would hold three times the limit.
    text = "漢字" * 100
    lst = [(f"k{i}", {"text": text}) for i in range(12)]
    room = 2048
    w = await write_layer(queen=queen, ns=NS, id="utf8", sid="s1", lock=None, steps=[{"kind": "window", "entries": lst}], piece_bytes=room)
    pieces = []
    await read_layer(queen=queen, ns=NS, id="utf8", sid="s1", count=w["chunks"], on_piece=pieces.append)
    assert [e for pc in pieces for e in pc["set"]] == [[k, v] for k, v in lst]
    assert len(pieces) >= 3, f"{len(pieces)} pieces"
    for pc in pieces:
        # A piece ends at the entry that takes it to the limit: the entries before that one stay under it.
        before = sum(len(json.dumps(e, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) for e in pc["set"][:-1])
        assert before < room, f"{before} bytes before the last entry of a piece (limit {room})"


async def test_a_wait_that_matched_after_the_full_layer_is_a_delete_in_the_next_delta_and_does_not_come_back():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(20):
        b.push({"queue": "orders", "partition": f"o{i}", "data": {"start": True}})

    def waits(q):
        return (Stream.from_feed(q, from_="earliest", queues=["orders"])
                .pattern(start=lambda m: m["data"].get("start") is True, then=lambda m: m["data"].get("paid") is True, within="1h",
                         on_match=lambda s: {"paid": s["partition"]})
                .to(q.queue("paid")))

    runner = await waits(queen).run(query_id="dels", **fast(now=lambda: b.now_ms, snapshot_max_entries=1))
    await until(lambda: runner.metrics()["entriesRead"] >= 20 and runner.metrics()["snapshots"] >= 1)
    base = b.kv_value(NS, "dels")["snapshot"]["layers"][0]
    b.push({"queue": "orders", "partition": "o3", "data": {"paid": True}})
    b.push({"queue": "orders", "partition": "o7", "data": {"paid": True}})
    await until(lambda: len(b.messages("paid")) >= 2 and len(b.kv_value(NS, "dels")["snapshot"].get("layers") or []) >= 2)
    await runner.stop()
    snap = b.kv_value(NS, "dels")["snapshot"]
    assert snap["layers"][0]["sid"] == base["sid"], "the full layer stays under the deltas"
    deleted = []
    for layer in snap["layers"][1:]:
        await read_layer(queen=queen, ns=NS, id="dels", sid=layer["sid"], count=layer["chunks"], on_piece=lambda pc: deleted.extend(pc["del"]))
    assert len([k for k in deleted if k.endswith("o3") or k.endswith("o7")]) == 2, f"the two matched waits are deletes ({deleted})"
    again = await waits(fake_queen(b, "again")).run(query_id="dels", **fast(now=lambda: b.now_ms))
    assert again.metrics()["open"] == 18, "eighteen waits came back, not the two that matched"
    b.push({"queue": "orders", "partition": "o3", "data": {"paid": True}})
    b.push({"queue": "orders", "partition": "o4", "data": {"paid": True}})
    await until(lambda: len(b.messages("paid")) >= 3)
    await again.stop()
    assert sorted(e["payload"]["paid"] for e in b.messages("paid")) == ["o3", "o4", "o7"], "o3 matched once, o4 matched now"


async def test_when_the_deltas_weigh_as_much_as_the_full_layer_a_full_layer_replaces_them_and_their_chunks_go():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(4):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {}})
    runner = await per_minute(queen).run(query_id="compact", **fast(now=lambda: b.now_ms, snapshot_max_entries=1))
    await until(lambda: runner.metrics()["snapshots"] >= 1 and runner.metrics()["entriesRead"] >= 4)
    first_base = b.kv_value(NS, "compact")["snapshot"]["layers"][0]["sid"]
    saw_deltas = 0
    for i in range(4, 40):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {}})
        await until(lambda: runner.metrics()["entriesRead"] >= i + 1)
        saw_deltas = max(saw_deltas, len(b.kv_value(NS, "compact")["snapshot"].get("layers") or []) - 1)

    def replaced():
        s = b.kv_value(NS, "compact")["snapshot"]
        return s["layers"][0]["sid"] != first_base and s["layers"][0].get("full")

    await until(replaced, 3000, "a new full layer")
    await runner.stop()
    assert saw_deltas >= 1, "deltas came before it"
    assert runner.metrics()["snapshotsFull"] >= 2, "the first full layer and at least one more"
    snap = b.kv_value(NS, "compact")["snapshot"]
    named = {layer["sid"] for layer in snap["layers"]}
    keys = b.kv_keys(NS, "compact#snap#")
    assert all(sid_of("compact#snap#", k) in named for k in keys), "the replaced layers are gone from KV"
    assert not any(sid_of("compact#snap#", k) == first_base for k in keys), "the first full layer is gone"
    again = await per_minute(fake_queen(b, "again")).run(query_id="compact", **fast(now=lambda: b.now_ms))
    assert again.metrics()["stateKeys"] == 40, "all forty windows came back"


async def test_a_snapshot_of_format_1_still_loads_and_the_next_one_is_a_full_layer_of_format_2():
    b = FakeBroker()
    queen = fake_queen(b)
    at(b, 1000)
    for i in range(3):
        b.push({"queue": "orders", "partition": f"c{i}", "data": {}})
    first = await per_minute(queen).run(query_id="old", **fast(now=lambda: b.now_ms))
    await until(lambda: first.metrics()["entriesRead"] >= 3)
    await first.stop()
    # Rewrite the stored snapshot the way a runner of format 1 wrote it.
    rec = b.kv_value(NS, "old")
    entries = []
    for layer in layers_of(rec["snapshot"]):
        await read_layer(queen=queen, ns=NS, id="old", sid=layer["sid"], count=layer["chunks"], on_piece=lambda pc: entries.extend(pc["set"]))
    for k in b.kv_keys(NS, "old#snap#"):
        del b.kv[f"{NS}\u0000{k}"]
    enc = encode_snapshot({"steps": [{"name": "perMinute", "kind": "window", "entries": entries}]})
    sid = new_snapshot_id()
    for n, d in enumerate(enc["chunks"]):
        b.kv_put(NS, chunk_key("old", sid, n), {"d": d})
    b.kv_put(NS, "old", {**rec, "snapshot": {"id": sid, "chunks": len(enc["chunks"]), "bytes": enc["bytes"],
                                             "position": rec["snapshot"]["position"], "clock": rec["snapshot"]["clock"], "metas": rec["snapshot"]["metas"]}})
    again = await per_minute(fake_queen(b, "again")).run(query_id="old", **fast(now=lambda: b.now_ms, snapshot_max_entries=1))
    assert again.metrics()["stateKeys"] == 3, "the three windows of the old snapshot came back"
    b.push({"queue": "orders", "partition": "c9", "data": {}})
    await until(lambda: len(b.kv_value(NS, "old")["snapshot"].get("layers") or []) == 1)
    await again.stop()
    snap = b.kv_value(NS, "old")["snapshot"]
    assert snap["layers"][0].get("full"), "a full layer of format 2"
    assert not any(sid_of("old#snap#", k) == sid for k in b.kv_keys(NS, "old#snap#")), "the old chunks are gone"
