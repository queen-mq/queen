"""
Headers (PLAN_ROUTING.md §6): a message's headers on every push path the SDK
has -- push, buffered push, transaction push, and the binary bodies -- and back
on every read: pop (JSON and framed), fetch, the feed. No broker: the requests
are asserted as they leave the client.

What is at stake besides the shape: a push WITHOUT headers stays the request it
was, byte for byte, and an item that is its own payload keeps a ``headers`` key
of its as data.
"""

from __future__ import annotations

import json
import struct
from typing import Any, Dict, List, Optional, Tuple

import pytest

from queen import Queen
from queen.feed.feed import condition_holds, to_entry, where_condition
from queen.http import zstd
from queen.http.pop_frames import decode_headers, decode_pop_frames
from queen.http.push_binary import binary_push_body, encode_headers

from ..conflation_unit.pop_transport import PopTransport, message, pop_body
from ..kv_unit.plan_server import PlanServer
from ..wire_unit.test_pop_frames import framed, text, u32
from ..wire_unit.wire_transport import WireTransport, json_answer

ORDER_HEADERS = {"type": "order.paid", "traceparent": "00-4bf92f35a9-00f067aa0b-01"}
TS = "2026-11-12T09:00:00.000000Z"


@pytest.fixture
def plain_zstd(monkeypatch):
    """The groups' frames as they are: these tests read the frame layout, not
    the compression, so they need no zstd."""
    monkeypatch.setattr(zstd, "_codec", (lambda raw: raw, lambda frame: frame))


def make_plan(*plan: Any) -> Tuple[Queen, PlanServer]:
    server = PlanServer(*plan)
    return Queen(url="http://plan.local", transport=server, retry_attempts=1), server


def make_wire(answer: Any, **config: Any) -> Tuple[Queen, WireTransport]:
    transport = WireTransport(answer)
    return Queen(url="http://wire.local", transport=transport, retry_attempts=1, **config), transport


def queued(count: int = 1):
    return lambda _request: json_answer(json.dumps([{"status": "queued"}] * count))


def frames_of(z: bytes) -> List[Dict[str, Any]]:
    """The client frames of one group: ``u32 len | flags | id | u16 len, txn |
    [u16 len, headers, flag 8] | payload``, the headers as (name, value) pairs
    in the order they are written."""
    out = []
    at = 0
    while at < len(z):
        (length,) = struct.unpack_from("<I", z, at)
        body = z[at + 4 : at + 4 + length]
        at += 4 + length
        flags = body[0]
        (txn_length,) = struct.unpack_from("<H", body, 17)
        p = 19 + txn_length
        pairs: Optional[List[Tuple[str, str]]] = None
        if flags & 8:
            (block_length,) = struct.unpack_from("<H", body, p)
            block = body[p + 2 : p + 2 + block_length]
            p += 2 + block_length
            pairs = []
            (count,) = struct.unpack_from("<H", block, 0)
            q = 2
            for _ in range(count):
                name = block[q + 1 : q + 1 + block[q]].decode()
                q += 1 + block[q]
                (value_length,) = struct.unpack_from("<H", block, q)
                pairs.append((name, block[q + 2 : q + 2 + value_length].decode()))
                q += 2 + value_length
            assert q == len(block), "bytes after the last header"
        out.append({"flags": flags, "txn": body[19 : 19 + txn_length].decode(), "headers": pairs, "payload": body[p:].decode()})
    return out


def groups_of(data: bytes) -> List[Dict[str, Any]]:
    """``u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z``."""
    at = 0

    def take(n: int) -> bytes:
        nonlocal at
        at += n
        return data[at - n : at]

    groups = []
    for _ in range(struct.unpack("<I", take(4))[0]):
        queue = take(struct.unpack("<H", take(2))[0]).decode()
        partition = take(struct.unpack("<H", take(2))[0]).decode()
        count = struct.unpack("<I", take(4))[0]
        frames = frames_of(zstd.decompress(take(struct.unpack("<I", take(4))[0])))
        assert len(frames) == count
        groups.append({"queue": queue, "partition": partition, "frames": frames})
    assert at == len(data), "bytes after the last group"
    return groups


# ---- push, JSON ------------------------------------------------------------------


async def test_a_push_carries_its_headers_beside_the_payload_and_only_when_it_has_some():
    client, server = make_plan()
    try:
        await client.queue("orders").partition("order-9137").push([
            {"transactionId": "order-9137-paid", "data": {"orderId": 9137}, "headers": ORDER_HEADERS},
            {"transactionId": "order-9137-noted", "payload": {"orderId": 9137}, "headers": {}},
            {"transactionId": "order-9137-shipped", "data": {"orderId": 9137}},
        ])
    finally:
        await client.close()

    items = server.only.body["items"]
    assert items[0] == {
        "queue": "orders",
        "partition": "order-9137",
        "payload": {"orderId": 9137},
        "transactionId": "order-9137-paid",
        "headers": ORDER_HEADERS,
    }
    assert "headers" not in items[1], "{} is no headers"
    assert list(items[2]) == ["queue", "partition", "payload", "transactionId"], "the request it always was"


async def test_an_item_with_neither_data_nor_payload_is_its_own_payload_headers_key_included():
    client, server = make_plan()
    try:
        await client.queue("http-log").push({"url": "/orders", "headers": {"Accept": "*/*"}})
    finally:
        await client.close()

    item = server.only.body["items"][0]
    assert item["payload"] == {"url": "/orders", "headers": {"Accept": "*/*"}}
    assert "headers" not in item


@pytest.mark.parametrize(
    "headers, match",
    [
        ({"Type": "x"}, "header name 'Type'"),
        ({"queen-trace": "x"}, "the broker's"),
        ({"type": 3}, "must be a string"),
        ({"a" * 65: "x"}, "1 to 64"),
        ({"": "x"}, "1 to 64"),
        ([("type", "x")], "dict of strings"),
    ],
)
async def test_headers_the_broker_refuses_raise_before_any_request(headers, match):
    client, server = make_plan()
    try:
        with pytest.raises(ValueError, match=match):
            client.queue("orders").push({"data": {}, "headers": headers})
        with pytest.raises(ValueError, match=match):
            client.transaction().queue("orders").push({"data": {}, "headers": headers})
    finally:
        await client.close()
    assert server.requests == []


async def test_a_buffered_push_keeps_each_messages_headers():
    client, server = make_plan()
    try:
        await client.queue("orders").partition("p1").buffer({"message_count": 10, "time_millis": 60000}).push(
            [{"data": {"n": 1}, "headers": {"type": "a"}}, {"data": {"n": 2}}]
        )
        await client.flush_all_buffers()
    finally:
        await client.close()

    (push,) = [r for r in server.requests if r.path == "/api/v1/push"]
    assert [it.get("headers") for it in push.body["items"]] == [{"type": "a"}, None]


async def test_a_transaction_push_carries_headers():
    client, server = make_plan()
    try:
        await (
            client.transaction()
            .queue("out").partition("p1")
            .push([{"data": {"n": 1}, "headers": {"type": "a"}}, {"data": {"n": 2}}])
            .commit()
        )
    finally:
        await client.close()

    items = server.only.body["operations"][0]["items"]
    assert items[0]["headers"] == {"type": "a"}
    assert "headers" not in items[1]


async def test_a_timer_gives_its_fired_message_headers_on_every_timer_surface():
    client, server = make_plan()
    try:
        await client.timers.schedule("orders", "order-1:expire", {"orderId": 1}, delay_ms=60_000,
                                     headers={"type": "order.expired"})
        await client.timer("orders").key("order-2:expire").payload({"orderId": 2}).after_ms(1).headers({"type": "x"}).reschedule()
        await client.timers.schedule("orders", "order-3:expire", {"orderId": 3}, delay_ms=1)
        await (
            client.transaction()
            .timer("orders").key("order-4:expire").payload({"orderId": 4}).after_ms(1).headers({"type": "y"}).schedule()
            .commit()
        )
        with pytest.raises(ValueError, match="the broker's"):
            await client.timers.schedule("orders", "k", {}, delay_ms=1, headers={"queen-x": "1"})
    finally:
        await client.close()

    first, second, plain, tx = server.requests
    assert first.body["operations"][0]["headers"] == {"type": "order.expired"}
    assert (second.body["operations"][0]["op"], second.body["operations"][0]["headers"]) == ("reschedule", {"type": "x"})
    assert "headers" not in plain.body["operations"][0]
    assert tx.body["timers"][0]["headers"] == {"type": "y"}


# ---- the binary bodies --------------------------------------------------------------


def test_encode_headers_writes_the_block_the_broker_stores_sorted_by_name():
    block = encode_headers({"type": "x", "a.b": "é"})
    assert block == (
        struct.pack("<H", 2)
        + bytes([3]) + b"a.b" + struct.pack("<H", 2) + "é".encode()
        + bytes([4]) + b"type" + struct.pack("<H", 1) + b"x"
    )
    assert decode_headers(block) == {"a.b": "é", "type": "x"}


async def test_binary_push_sends_a_push_with_headers_as_json_the_plain_body_has_no_place_for_them():
    client, transport = make_wire(queued(), binary_push=True)
    try:
        await client.queue("orders").partition("p1").push({"data": {"a": 1}, "headers": {"type": "x"}})
        await client.queue("orders").partition("p1").push({"data": {"a": 2}})
    finally:
        await client.close()

    with_headers, without = transport.pushes
    assert with_headers.content_type == "application/json"
    assert json.loads(with_headers.raw)["items"][0]["headers"] == {"type": "x"}
    assert without.content_type == "application/x-queen-push" and without.raw[:4] == b"QPB1"
    item = {"queue": "q", "partition": "p", "payload": 1, "headers": {"type": "x"}}
    assert binary_push_body([item], False) is None


async def test_push_compression_carries_headers_in_the_frame_flag_8_sorted_by_name(plain_zstd):
    client, transport = make_wire(queued(2), binary_push=True, push_compression=True)
    try:
        await client.queue("orders").partition("p1").push([
            {"data": {"a": 1}, "transactionId": "t1", "headers": {"type": "order.paid", "source": "web"}},
            {"data": {"a": 2}, "transactionId": "t2"},
        ])
    finally:
        await client.close()

    (sent,) = transport.pushes
    assert sent.content_type == "application/x-queen-push"
    assert sent.raw[:5] == b"QPB1\x08"
    (group,) = groups_of(sent.raw[5:])
    first, second = group["frames"]
    assert (first["flags"], first["txn"], first["payload"]) == (8, "t1", '{"a":1}')
    assert first["headers"] == [("source", "web"), ("type", "order.paid")]
    assert (second["flags"], second["headers"], second["payload"]) == (0, None, '{"a":2}')


async def test_a_framed_transaction_carries_push_headers_in_frames_and_its_links_in_the_head(plain_zstd):
    results = [
        {"index": 0, "type": "ack", "success": True},
        {"index": 1, "type": "link", "success": True},
        {"index": 2, "type": "push", "success": True},
    ]
    client, transport = make_wire(
        lambda _r: json_answer(json.dumps({"success": True, "results": results})),
        binary_push=True,
        push_compression=True,
    )
    popped = {"transactionId": "in-1", "partitionId": "7", "leaseId": "lease-1", "offset": 41, "consumerGroup": "billing"}
    try:
        res = await (
            client.transaction()
            .ack(popped, context={"consumer_group": "billing"})
            .queue("review").partition("order-1").link([popped])
            .queue("out").partition("p1").push([{"data": {"n": 1}, "transactionId": "t1", "headers": {"type": "x"}}])
            .commit()
        )
    finally:
        await client.close()

    assert [r["type"] for r in res["results"]] == ["ack", "link", "push"]
    (sent,) = [r for r in transport.requests if r.path == "/api/v1/transaction"]
    assert sent.raw[:4] == b"QTB1"
    (head_length,) = struct.unpack_from("<I", sent.raw, 5)
    head = json.loads(sent.raw[9 : 9 + head_length])
    assert [op["type"] for op in head["operations"]] == ["ack", "link"]
    (group,) = groups_of(sent.raw[9 + head_length :])
    assert group["frames"][0]["headers"] == [("type", "x")]


# ---- the reads ------------------------------------------------------------------------


async def test_a_popped_message_has_its_headers_and_one_without_has_none():
    transport = PopTransport(pop_body([{**message("txn-1"), "headers": {"type": "x"}}, message("txn-2")]))
    client = Queen(url="http://plan.local", transport=transport, retry_attempts=1)
    try:
        messages = await client.queue("orders").group("workers").pop()
    finally:
        await client.close()

    assert messages[0]["headers"] == {"type": "x"}
    assert messages[1].get("headers") is None


def stored_frame(flags: int, txn: str, payload: str, block: bytes = b"") -> bytes:
    """One stored frame with a producer (flag 2) and, with flag 8, a headers block."""
    body = bytes([flags]) + bytes([1] + [0] * 5 + [0x70, 0, 0x80] + [0] * 7) + text(txn) + text("svc")
    if flags & 8:
        body += struct.pack("<H", len(block)) + block
    return u32(len(body) + len(payload.encode())) + body + payload.encode()


def test_a_framed_pop_reads_the_headers_flag_where_the_json_answer_puts_them():
    block = encode_headers({"type": "order.paid"})
    answer = framed("", 10, 11, False, [stored_frame(2 | 8, "t-10", '{"n":10}', block), stored_frame(2, "t-11", '{"n":11}')])
    got = decode_pop_frames(answer)["messages"]

    assert got[0]["headers"] == {"type": "order.paid"}
    keys = list(got[0])
    assert keys[keys.index("producerSub") + 1] == "headers", "after producerSub, as in the JSON answer"
    assert got[0]["data"] == {"n": 10} and got[0]["producerSub"] == "svc"
    assert "headers" not in got[1]


def test_a_framed_pop_refuses_a_link_frame_as_it_refuses_an_encrypted_one():
    answer = framed("", 10, 10, False, [stored_frame(2 | 16, "t-10", "x" * 24)])
    with pytest.raises(ValueError, match="a frame is a link"):
        decode_pop_frames(answer)


async def test_a_fetched_record_has_its_headers_only_when_it_has_some():
    answer = {
        "entries": [
            {
                "partition": "p",
                "highWatermark": 2,
                "logStartOffset": 0,
                "records": [
                    {"headers": {"type": "x"}, "offset": 0, "transactionId": "t0", "payload": {"n": 0}, "ts": TS},
                    {"offset": 1, "transactionId": "t1", "payload": {"n": 1}, "ts": TS},
                ],
            }
        ]
    }
    client, _ = make_plan({"status": 200, "json": answer})
    try:
        read = await client.queue("orders").fetch(["p"])
    finally:
        await client.close()

    first, second = read["p"]["records"]
    assert first == {"offset": 0, "transactionId": "t0", "payload": {"n": 0}, "ts": TS, "headers": {"type": "x"}}
    assert second == {"offset": 1, "transactionId": "t1", "payload": {"n": 1}, "ts": TS}


def test_a_feed_entry_keeps_its_headers_and_a_where_reads_them():
    raw = {"position": 7, "queue": "orders", "partition": "p", "offset": 3, "id": "m", "transactionId": "t",
           "payload": {"n": 1}, "ts": TS}
    entry = to_entry({**raw, "headers": {"type": "order.paid", "content.type": "json"}})
    assert entry["headers"] == {"type": "order.paid", "content.type": "json"}
    assert "headers" not in to_entry(raw)

    assert condition_holds(where_condition("headers.type", "=", "order.paid"), entry)
    assert condition_holds(where_condition("headers.type", "prefix", "order."), entry)
    assert condition_holds(where_condition("headers.content.type", "in", ["json", "xml"]), entry), "a name with dots"
    assert condition_holds(where_condition("headers.type", "exists"), entry)
    assert not condition_holds(where_condition("headers.source", "!=", "web"), entry), "a header it lacks matches no op"
    assert not condition_holds(where_condition("headers.type", "=", "order.paid"), to_entry(raw))
