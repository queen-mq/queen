"""
Streams auto-link on a queue source (PLAN_ROUTING.md §6): a cycle push item
whose record is the input message unchanged carries ``link: {offset,
transactionId}`` (the cycle's own partition holds that message), beside the
payload a broker without links stores as before. Anything transformed is a
plain push item; ``link=False`` turns it off; a broker that refuses the links
gets the same cycle with the records pushed.
"""

from __future__ import annotations

import asyncio

from queen.streams.stream import Stream

from .fake_server import create_fake_source, create_fake_streams_server, fake_message

PART = "00000000-0000-0000-0000-000000000aaa"


class Sink:
    def __init__(self, name: str):
        self._queue_name = name
        self.queue_name = name


async def run_cycles(stream, server, cycles: int, query_id: str, timeout_ms: float = 3000):
    handle = await stream.run(query_id=query_id, url=server.url)
    start = asyncio.get_event_loop().time()
    while len(server.recorded["cycles"]) < cycles and (asyncio.get_event_loop().time() - start) * 1000 < timeout_ms:
        await asyncio.sleep(0.02)
    await asyncio.sleep(0.05)
    await handle.stop()
    return handle


def messages(*specs):
    return [fake_message(PART, "cust-1", data, f"2026-01-01T10:00:0{i}Z", transaction_id=txn, offset=offset)
            for i, (data, txn, offset) in enumerate(specs)]


async def test_an_unchanged_message_goes_as_a_link_beside_its_payload():
    server = await create_fake_streams_server()
    try:
        source = create_fake_source("orders", [messages(
            ({"type": "order", "country": "IT"}, "o-1", 10),
            ({"type": "heartbeat"}, "hb-1", 11),
            ({"type": "order", "country": "IT"}, "o-2", 12),
        )])
        handle = await run_cycles(
            Stream.from_(source)
            .where("data.type", "=", "order")
            .filter(lambda m: m["data"].get("country") == "IT")
            .to(Sink("orders-it")),
            server, 1, "streams.test.links",
        )
        (cycle,) = server.recorded["cycles"]
        assert cycle["partition_id"] == PART
        assert cycle["push_items"] == [
            {"queue": "orders-it", "partition": "cust-1", "payload": {"type": "order", "country": "IT"},
             "link": {"offset": 10, "transactionId": "o-1"}},
            {"queue": "orders-it", "partition": "cust-1", "payload": {"type": "order", "country": "IT"},
             "link": {"offset": 12, "transactionId": "o-2"}},
        ]
        assert cycle["ack"]["count"] == 3, "every popped message is settled"
        assert handle.metrics()["linkItemsTotal"] == 2
    finally:
        await server.close()


async def test_a_mapped_record_is_a_push_and_link_false_pushes_copies():
    server = await create_fake_streams_server()
    try:
        mapped = create_fake_source("orders", [messages(({"n": 1}, "o-1", 10))])
        await run_cycles(Stream.from_(mapped).map(lambda m: {"n": m["data"]["n"] + 1}).to(Sink("out")),
                         server, 1, "streams.test.mapped")
        copied = create_fake_source("orders", [messages(({"n": 1}, "o-2", 11))])
        await run_cycles(Stream.from_(copied).to(Sink("out"), link=False), server, 2, "streams.test.copied")
        first, second = server.recorded["cycles"]
        assert first["push_items"] == [{"queue": "out", "partition": "cust-1", "payload": {"n": 2}}]
        assert second["push_items"] == [{"queue": "out", "partition": "cust-1", "payload": {"n": 1}}]
    finally:
        await server.close()


async def test_a_gate_and_a_key_choose_messages_and_do_not_change_them():
    server = await create_fake_streams_server()
    try:
        source = create_fake_source("orders", [messages(({"n": 1}, "o-1", 10), ({"n": 2}, "o-2", 11))])
        await run_cycles(
            Stream.from_(source).key_by(lambda m: "k").gate(lambda value, ctx: value["n"] == 1).to(Sink("out")),
            server, 1, "streams.test.gate",
        )
        cycle = server.recorded["cycles"][0]
        assert cycle["push_items"] == [
            {"queue": "out", "partition": "cust-1", "payload": {"n": 1}, "link": {"offset": 10, "transactionId": "o-1"}}
        ]
    finally:
        await server.close()


async def test_a_message_without_an_offset_is_pushed():
    server = await create_fake_streams_server()
    try:
        source = create_fake_source("orders", [[fake_message(PART, "cust-1", {"n": 1}, transaction_id="o-1")]])
        await run_cycles(Stream.from_(source).to(Sink("out")), server, 1, "streams.test.no-offset")
        assert server.recorded["cycles"][0]["push_items"] == [{"queue": "out", "partition": "cust-1", "payload": {"n": 1}}]
    finally:
        await server.close()


async def test_a_cluster_that_refuses_links_gets_the_same_cycle_pushed_and_pushes_from_then_on():
    server = await create_fake_streams_server()
    server.links = "cluster_version"
    try:
        source = create_fake_source("orders", [
            messages(({"n": 1}, "o-1", 10)),
            messages(({"n": 2}, "o-2", 11)),
        ])
        handle = await run_cycles(Stream.from_(source).to(Sink("out")), server, 3, "streams.test.cluster-version")
        refused, again, later = server.recorded["cycles"]
        assert refused["push_items"][0]["link"] == {"offset": 10, "transactionId": "o-1"}
        assert again["push_items"] == [{"queue": "out", "partition": "cust-1", "payload": {"n": 1}}], "the same cycle, pushed"
        assert again["ack"] == refused["ack"]
        assert later["push_items"] == [{"queue": "out", "partition": "cust-1", "payload": {"n": 2}}]
        m = handle.metrics()
        assert (m["linkItemsTotal"], m["errorsTotal"]) == (0, 0)
    finally:
        await server.close()


async def test_a_link_whose_source_is_gone_pushes_that_cycle_only():
    server = await create_fake_streams_server()
    server.gone = {"o-1"}
    try:
        source = create_fake_source("orders", [
            messages(({"n": 1}, "o-1", 10)),
            messages(({"n": 2}, "o-2", 11)),
        ])
        handle = await run_cycles(Stream.from_(source).to(Sink("out")), server, 3, "streams.test.source-gone")
        refused, again, later = server.recorded["cycles"]
        assert "link" in refused["push_items"][0] and "link" not in again["push_items"][0]
        assert later["push_items"][0]["link"] == {"offset": 11, "transactionId": "o-2"}, "the next cycle links again"
        assert handle.metrics()["linkItemsTotal"] == 1
    finally:
        await server.close()
