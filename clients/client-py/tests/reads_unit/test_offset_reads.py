"""
queue.fetch() and queue.changed_since() against a fake broker that answers
POST /api/v1/fetch and POST /api/v1/partitions/changed the way
server/src/rsm/facade/real/phase2/reads.rs does: entries answered in order,
each entry's byte budget and the whole call's ceiling (the first record of an
entry read whatever it weighs), a record cap per entry, OFFSET_OUT_OF_RANGE
below logStartOffset and above the high watermark, UNKNOWN_TOPIC_OR_PARTITION
for a queue that does not exist, discovery pages with a ``next`` cursor exactly
when a page is full, BAD_CURSOR for a cursor it did not issue.

The fake sits behind the client's transport seam (``Queen(transport=...)``), so
the bodies the client sends are asserted as they would go on the wire.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest

from queen import Queen

from ..kv_unit.plan_server import PlanServer, Recorded

MiB = 1024 * 1024
TS = "2026-10-20T10:00:00.000000Z"


class FakeBroker:
    def __init__(self, records_per_entry: int = 10_000):
        self.queues: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.records_per_entry = records_per_entry
        self.safe_times: List[str] = []
        self.after_fetch = None
        self.created = 0
        self.fetch_override = None
        self.changed_override = None
        self.server = PlanServer(default=self._answer)

    def write(self, queue: str, partition: str, payloads: List[Any], last_write_at: str = TS) -> None:
        q = self.queues.setdefault(queue, {})
        if partition not in q:
            q[partition] = {"records": [], "logStart": 0, "created": self.created, "lastWriteAt": last_write_at}
            self.created += 1
        p = q[partition]
        for payload in payloads:
            offset = p["records"][-1]["offset"] + 1 if p["records"] else p["logStart"]
            p["records"].append({"offset": offset, "transactionId": f"{partition}-{offset}", "payload": payload, "ts": TS})
        p["lastWriteAt"] = last_write_at

    def trim(self, queue: str, partition: str, offset: int) -> None:
        p = self.queues[queue][partition]
        p["records"] = [r for r in p["records"] if r["offset"] >= offset]
        p["logStart"] = offset

    def _answer(self, rec: Recorded) -> Dict[str, Any]:
        if rec.method == "POST" and rec.path == "/api/v1/fetch":
            answer = self.fetch_override(rec.body) if self.fetch_override else self.fetch(rec.body)
            if self.after_fetch:
                self.after_fetch(self)
            return answer
        if rec.method == "POST" and rec.path == "/api/v1/partitions/changed":
            return self.changed_override(rec.body) if self.changed_override else self.changed(rec.body)
        return {"status": 404, "json": {"error": "not found"}}

    def fetch(self, body: Dict[str, Any]) -> Dict[str, Any]:
        if len(body["entries"]) > 1024:
            return {"status": 400, "json": {"error": "too many entries"}}
        ceiling = body.get("maxBytes")
        used = 0
        entries = []
        for e in body["entries"]:
            out = {"queue": e["queue"], "partition": e.get("partition", "Default"), "records": [], "highWatermark": 0, "logStartOffset": 0}
            q = self.queues.get(e["queue"])
            if q is None:
                out["error"] = "UNKNOWN_TOPIC_OR_PARTITION"
                entries.append(out)
                continue
            p = q.get(out["partition"])
            if p is None:
                entries.append(out)
                continue
            last = p["records"][-1]["offset"] if p["records"] else p["logStart"] - 1
            out["highWatermark"] = last + 1
            out["logStartOffset"] = p["logStart"]
            if e["offset"] < p["logStart"] or e["offset"] > out["highWatermark"]:
                out["error"] = "OFFSET_OUT_OF_RANGE"
                entries.append(out)
                continue
            if ceiling is not None and used >= ceiling:
                entries.append(out)
                continue
            budget = min(e.get("maxBytes", MiB), float("inf") if ceiling is None else ceiling - used)
            spent = 0
            for r in p["records"]:
                if r["offset"] < e["offset"]:
                    continue
                if len(out["records"]) >= self.records_per_entry:
                    break
                weight = max(len(json.dumps(r["payload"], separators=(",", ":"))), 1)
                if out["records"] and spent + weight > budget:
                    break
                spent += weight
                out["records"].append(r)
            used += spent
            entries.append(out)
        return {"status": 200, "json": {"entries": entries}}

    def changed(self, body: Dict[str, Any]) -> Dict[str, Any]:
        if len(self.safe_times) > 1:
            safe_time = self.safe_times.pop(0)
        else:
            safe_time = self.safe_times[0] if self.safe_times else "2026-10-20T12:00:00.000000Z"
        entries = []
        for e in body["entries"]:
            q = self.queues.get(e["queue"])
            if q is None:
                entries.append({"queue": e["queue"], "error": "UNKNOWN_TOPIC_OR_PARTITION"})
                continue
            start = 0
            if e.get("after"):
                m = re.fullmatch(r"p\|(\d+)", e["after"])
                if not m:
                    entries.append({"queue": e["queue"], "error": "BAD_CURSOR"})
                    continue
                start = int(m.group(1)) + 1
            limit = min(max(e.get("limit", 1000), 1), 1000)
            partitions = []
            last: Optional[int] = None
            for name, p in sorted(q.items(), key=lambda kv: kv[1]["created"]):
                if p["created"] < start:
                    continue
                if e.get("since") and p["lastWriteAt"] < e["since"]:
                    continue
                last_offset = p["records"][-1]["offset"] if p["records"] else p["logStart"] - 1
                partitions.append({"name": name, "id": f"id-{name}", "lastOffset": last_offset, "logStart": p["logStart"], "lastWriteAt": p["lastWriteAt"]})
                last = p["created"]
                if len(partitions) >= limit:
                    break
            entries.append({"queue": e["queue"], "partitions": partitions, "next": f"p|{last}" if len(partitions) >= limit else None})
        return {"status": 200, "json": {"safeTime": safe_time, "safeTimeDegraded": False, "entries": entries}}

    def client(self) -> Queen:
        return Queen(url="http://fake.local", transport=self.server, retry_attempts=1)

    def fetches(self) -> List[Recorded]:
        return [r for r in self.server.requests if r.path == "/api/v1/fetch"]


def payloads(n: int, start: int = 0) -> List[Dict[str, int]]:
    return [{"n": start + i} for i in range(n)]


def offsets_of(part: Dict[str, Any]) -> List[int]:
    return [r["offset"] for r in part["records"]]


async def run(broker: FakeBroker, test):
    client = broker.client()
    try:
        return await test(client)
    finally:
        await client.close()


# ---------------------------------------------------------------- fetch


@pytest.mark.asyncio
async def test_fetch_reads_every_record_from_offset_zero_in_one_call_when_they_fit():
    broker = FakeBroker()
    broker.write("tf.events", "trace-1", payloads(3))
    broker.write("tf.events", "trace-2", [{"a": 1}])

    async def test(queen):
        read = await queen.queue("tf.events").fetch(["trace-1", "trace-2"])
        assert list(read) == ["trace-1", "trace-2"]
        t1 = read["trace-1"]
        assert [r["payload"] for r in t1["records"]] == payloads(3)
        assert t1["records"][0] == {"offset": 0, "transactionId": "trace-1-0", "payload": {"n": 0}, "ts": TS}
        assert (t1["partition"], t1["highWatermark"], t1["logStartOffset"], t1["nextOffset"]) == ("trace-1", 3, 0, 3)
        assert [r["payload"] for r in read["trace-2"]["records"]] == [{"a": 1}]

    await run(broker, test)
    fetches = broker.fetches()
    assert len(fetches) == 1
    assert fetches[0].body == {
        "entries": [
            {"queue": "tf.events", "partition": "trace-1", "offset": 0, "maxBytes": 8 * MiB},
            {"queue": "tf.events", "partition": "trace-2", "offset": 0, "maxBytes": 8 * MiB},
        ],
        "maxBytes": 8 * MiB,
    }


@pytest.mark.asyncio
async def test_fetch_asks_again_until_the_high_watermark_of_the_first_answer():
    broker = FakeBroker(records_per_entry=3)
    broker.write("q", "p", payloads(10))

    async def test(queen):
        read = await queen.queue("q").fetch("p")
        assert offsets_of(read["p"]) == list(range(10))
        assert read["p"]["nextOffset"] == 10

    await run(broker, test)
    assert [r.body["entries"][0]["offset"] for r in broker.fetches()] == [0, 3, 6, 9]


@pytest.mark.asyncio
async def test_fetch_does_not_chase_a_partition_written_during_the_read():
    broker = FakeBroker(records_per_entry=2)
    broker.write("q", "p", payloads(4))
    broker.after_fetch = lambda b: b.write("q", "p", [{"late": True}])

    async def test(queen):
        read = await queen.queue("q").fetch(["p"])
        assert offsets_of(read["p"]) == [0, 1, 2, 3]
        assert read["p"]["nextOffset"] == 4

    await run(broker, test)
    assert len(broker.fetches()) == 2


@pytest.mark.asyncio
async def test_fetch_asks_for_at_most_1000_partitions_per_call():
    broker = FakeBroker()
    names = [f"p-{i}" for i in range(2500)]
    for name in names:
        broker.write("q", name, [{"name": name}])

    async def test(queen):
        read = await queen.queue("q").fetch(names)
        assert len(read) == 2500
        for name in names:
            assert [r["payload"] for r in read[name]["records"]] == [{"name": name}]

    await run(broker, test)
    assert [len(r.body["entries"]) for r in broker.fetches()] == [1000, 1000, 500]


@pytest.mark.asyncio
async def test_fetch_asks_again_for_the_partitions_the_call_ceiling_left_empty():
    broker = FakeBroker()
    fat = "x" * 100
    for name in ("a", "b", "c"):
        broker.write("q", name, [fat, fat])

    async def test(queen):
        read = await queen.queue("q").fetch(["a", "b", "c"], max_bytes=250)
        for name in ("a", "b", "c"):
            assert offsets_of(read[name]) == [0, 1]

    await run(broker, test)
    assert len(broker.fetches()) > 1
    assert all(r.body["maxBytes"] == 250 for r in broker.fetches())


@pytest.mark.asyncio
async def test_fetch_reads_from_log_start_when_retention_deleted_the_start():
    broker = FakeBroker()
    broker.write("q", "p", payloads(10))
    broker.trim("q", "p", 6)

    async def test(queen):
        read = await queen.queue("q").fetch(["p"])
        assert offsets_of(read["p"]) == [6, 7, 8, 9]
        assert read["p"]["logStartOffset"] == 6

    await run(broker, test)
    assert [r.body["entries"][0]["offset"] for r in broker.fetches()] == [0, 6]


@pytest.mark.asyncio
async def test_fetch_starts_each_partition_at_its_offset():
    broker = FakeBroker()
    broker.write("q", "a", payloads(5))
    broker.write("q", "b", payloads(5))

    async def test(queen):
        read = await queen.queue("q").fetch(["a", "b"], offsets={"a": 3})
        assert offsets_of(read["a"]) == [3, 4]
        assert offsets_of(read["b"]) == [0, 1, 2, 3, 4]
        again = await queen.queue("q").fetch(["a"], offsets={"a": read["a"]["nextOffset"]})
        assert again["a"]["records"] == []
        assert again["a"]["nextOffset"] == 5

    await run(broker, test)


@pytest.mark.asyncio
async def test_fetch_answers_no_records_for_a_missing_queue_an_unwritten_partition_and_an_offset_past_the_end():
    broker = FakeBroker()
    broker.write("q", "p", payloads(2))

    async def test(queen):
        missing = await queen.queue("nope").fetch(["p"])
        assert missing["p"]["records"] == []
        read = await queen.queue("q").fetch(["never", "p"], offsets={"p": 7})
        assert read["never"]["records"] == []
        assert read["p"]["records"] == []
        assert read["p"]["nextOffset"] == 7
        assert read["p"]["highWatermark"] == 2

    await run(broker, test)


@pytest.mark.asyncio
async def test_fetch_raises_on_an_error_marker_it_does_not_know():
    broker = FakeBroker()
    broker.fetch_override = lambda body: {
        "status": 200,
        "json": {"entries": [{"queue": "q", "partition": "p", "records": [], "highWatermark": 1, "logStartOffset": 0, "error": "SOMETHING_NEW"}]},
    }

    async def test(queen):
        with pytest.raises(RuntimeError, match="SOMETHING_NEW"):
            await queen.queue("q").fetch(["p"])

    await run(broker, test)


@pytest.mark.asyncio
async def test_fetch_stops_when_a_call_reads_nothing_although_the_partition_is_not_at_its_end():
    broker = FakeBroker()
    broker.fetch_override = lambda body: {
        "status": 200,
        "json": {"entries": [{"queue": "q", "partition": "p", "records": [], "highWatermark": 5, "logStartOffset": 0}]},
    }

    async def test(queen):
        read = await queen.queue("q").fetch(["p"])
        assert read["p"]["records"] == []

    await run(broker, test)
    assert len(broker.fetches()) == 1


@pytest.mark.asyncio
async def test_fetch_asks_once_for_a_name_given_twice_and_refuses_bad_input_before_any_call():
    broker = FakeBroker()
    broker.write("q", "p", payloads(1))

    async def test(queen):
        read = await queen.queue("q").fetch(["p", "p"])
        assert len(read) == 1
        with pytest.raises(ValueError, match="non-empty string"):
            await queen.queue("q").fetch([""])
        with pytest.raises(ValueError, match="non-negative integer"):
            await queen.queue("q").fetch(["p"], offsets={"p": -1})
        with pytest.raises(ValueError, match="positive integer"):
            await queen.queue("q").fetch(["p"], max_bytes=0)

    await run(broker, test)
    assert len(broker.fetches()) == 1
    assert len(broker.fetches()[0].body["entries"]) == 1


# ---------------------------------------------------------- changed_since


@pytest.mark.asyncio
async def test_changed_since_follows_the_page_cursor_to_the_end_of_the_pass():
    broker = FakeBroker()
    for i in range(2300):
        broker.write("tf.events", f"t-{i}", [{"i": i}])

    async def test(queen):
        changed = await queen.queue("tf.events").changed_since("2026-10-20T09:00:00Z")
        parts = changed["partitions"]
        assert len(parts) == 2300
        assert parts[0] == {"name": "t-0", "id": "id-t-0", "lastOffset": 0, "logStart": 0, "lastWriteAt": TS}
        assert parts[-1]["name"] == "t-2299"
        assert changed["safeTime"] == "2026-10-20T12:00:00.000000Z"

    await run(broker, test)
    bodies = [r.body["entries"][0] for r in broker.server.requests]
    assert [b.get("after") for b in bodies] == [None, "p|999", "p|1999"]
    assert all(b["since"] == "2026-10-20T09:00:00Z" and b["limit"] == 1000 and b["queue"] == "tf.events" for b in bodies)


@pytest.mark.asyncio
async def test_changed_since_lists_only_what_was_written_at_or_after_since():
    broker = FakeBroker()
    broker.write("q", "old", [1], last_write_at="2026-10-20T08:00:00.000000Z")
    broker.write("q", "new", [1], last_write_at="2026-10-20T10:30:00.000000Z")

    async def test(queen):
        aware = datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc)
        changed = await queen.queue("q").changed_since(aware)
        assert [p["name"] for p in changed["partitions"]] == ["new"]
        await queen.queue("q").changed_since(datetime(2026, 10, 20, 10, 0))

    await run(broker, test)
    sent = [r.body["entries"][0]["since"] for r in broker.server.requests]
    assert sent == ["2026-10-20T10:00:00.000000Z", "2026-10-20T10:00:00.000000Z"]


@pytest.mark.asyncio
async def test_changed_since_without_since_lists_every_partition():
    broker = FakeBroker()
    broker.write("q", "a", [1], last_write_at="2020-01-01T00:00:00.000000Z")

    async def test(queen):
        changed = await queen.queue("q").changed_since()
        assert [p["name"] for p in changed["partitions"]] == ["a"]

    await run(broker, test)
    assert "since" not in broker.server.requests[0].body["entries"][0]


@pytest.mark.asyncio
async def test_changed_since_answers_the_smallest_safe_time_of_its_pages():
    broker = FakeBroker()
    for i in range(1500):
        broker.write("q", f"p-{i}", [1])
    broker.safe_times = ["2026-10-20T12:00:05.000000Z", "2026-10-20T12:00:01.000000Z"]

    async def test(queen):
        changed = await queen.queue("q").changed_since(None)
        assert changed["safeTime"] == "2026-10-20T12:00:01.000000Z"

    await run(broker, test)


@pytest.mark.asyncio
async def test_changed_since_answers_nothing_for_a_missing_queue_and_raises_on_bad_cursor():
    broker = FakeBroker()

    async def missing(queen):
        changed = await queen.queue("ghost").changed_since("2026-10-20T00:00:00Z")
        assert changed == {"partitions": [], "safeTime": "2026-10-20T12:00:00.000000Z"}

    await run(broker, missing)

    bad = FakeBroker()
    bad.changed_override = lambda body: {
        "status": 200,
        "json": {"safeTime": "2026-10-20T12:00:00.000000Z", "safeTimeDegraded": False, "entries": [{"queue": "q", "error": "BAD_CURSOR"}]},
    }

    async def cursor(queen):
        with pytest.raises(RuntimeError, match="BAD_CURSOR"):
            await queen.queue("q").changed_since(None)

    await run(bad, cursor)


@pytest.mark.asyncio
async def test_changed_since_refuses_a_since_that_is_not_a_time_before_any_call():
    broker = FakeBroker()

    async def test(queen):
        with pytest.raises(ValueError, match="ISO-8601"):
            await queen.queue("q").changed_since(12345)

    await run(broker, test)
    assert broker.server.requests == []
