"""
The durable sink, pinned to the byte.

EPHEMERAL_QUEUES.md §4.1 buys the ephemeral buffered push by PARAMETRIZING the
drain rather than duplicating it, and §7.3 names the price of that bargain: "a
pin that the DURABLE sink's bodies are byte-identical before and after the sink
refactor". This file is that pin.

It is written against the durable path only. Nothing here mentions an ephemeral
queue, and that is deliberate: the question it answers is not "does the new
feature work" but "did the refactor that made the new feature possible move a
single byte on the path that was already in production". An item whose key ORDER
changed, an envelope that grew a `queue` field because the ephemeral wire has
one, a path that became `/api/v1/push/batch` -- none of those would fail an
equality check on a PARSED body, and every one of them is a broken 1.0.6
producer.

The literal below is the request the buffered durable push made before sinks
existed. It is not derived from the code under test: derive it and the pin pins
nothing.
"""

from __future__ import annotations

import pytest

from queen.buffer.sinks import DURABLE_DESTINATION, DURABLE_SINK, durable_address

from .envelopes import make, until

#: The exact bytes a buffered durable push of two items has always produced.
PINNED_BODY = (
    '{"items":['
    '{"queue":"orders","partition":"Default","payload":{"n":1},"transactionId":"fixed-1"},'
    '{"queue":"orders","partition":"Default","payload":{"n":2},"transactionId":"fixed-2"}'
    "]}"
)


@pytest.mark.asyncio
async def test_drains_the_buffered_durable_push_to_the_same_path_with_the_same_bytes():
    client, server = make()
    await (
        client.queue("orders")
        .partition("Default")
        .buffer({"message_count": 2, "time_millis": 60000})
        .push(
            [
                {"data": {"n": 1}, "transactionId": "fixed-1"},
                {"data": {"n": 2}, "transactionId": "fixed-2"},
            ]
        )
    )

    await until(
        lambda: any(r.path == "/api/v1/push" for r in server.requests),
        "the durable batch to reach the wire",
    )
    hit = next(r for r in server.requests if r.path == "/api/v1/push")
    assert hit.route == "POST /api/v1/push"
    # The serialized request, not a re-encoding of the parsed one: this is the
    # string a broker would have read off the socket.
    assert hit.raw.decode() == PINNED_BODY
    await client.close()


@pytest.mark.asyncio
async def test_an_unbuffered_durable_push_is_untouched_by_sinks():
    """The direct push does not go through a buffer at all, so the refactor must
    not have reached it. Asserted anyway: "did not touch it" is cheap to claim
    and cheap to check."""
    client, server = make()
    await client.queue("orders").partition("Default").push(
        {"data": {"n": 1}, "transactionId": "fixed-1"}
    )

    hit = next(r for r in server.requests if r.path == "/api/v1/push")
    assert hit.body["items"] == [
        {
            "queue": "orders",
            "partition": "Default",
            "payload": {"n": 1},
            "transactionId": "fixed-1",
        }
    ]
    await client.close()


def test_the_default_destination_is_the_durable_push():
    """A buffer created without a destination -- which is every caller that
    existed before ephemeral queues did -- drains where it always did."""
    assert DURABLE_DESTINATION.sink is DURABLE_SINK
    assert DURABLE_SINK.path == "/api/v1/push"
    assert DURABLE_SINK.format(None, None, [{"a": 1}]) == {"items": [{"a": 1}]}
    assert durable_address("orders", "Default") == "orders/Default"


@pytest.mark.asyncio
async def test_a_buffer_built_without_a_destination_still_posts_the_durable_wire():
    """The seam itself, at the level below the builder: MessageBuffer's default
    is the durable destination, so an add that names none is unchanged."""
    from queen.buffer.buffer_manager import BufferManager

    client, server = make()
    manager = BufferManager(client._http_client)
    await manager.add_message(
        durable_address("orders", "Default"),
        {"queue": "orders", "partition": "Default", "payload": {"n": 1}},
        {"message_count": 1, "time_millis": 60000},
    )

    await until(
        lambda: any(r.path == "/api/v1/push" for r in server.requests),
        "the durable batch to reach the wire",
    )
    hit = next(r for r in server.requests if r.path == "/api/v1/push")
    assert hit.body == {
        "items": [{"queue": "orders", "partition": "Default", "payload": {"n": 1}}]
    }
    await manager.cleanup()
    await client.close()
