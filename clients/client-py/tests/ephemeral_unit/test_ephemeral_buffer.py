"""
Buffered ephemeral push: the request it drains into, and the semantics it
inherits (EPHEMERAL_QUEUES.md §4.1, §7.3).

§4.1 buys this feature by PARAMETRIZING the drain rather than duplicating it, so
there are two separable questions and this file answers both:

  * the ephemeral sink formats the right envelope for the right route, and
    addresses its buffers under `eph:` so the two storage classes never share a
    buffer or a drain;
  * every property the 1.0.6 buffer rewrite bought -- the blocking bound, a
    failed batch back at the FRONT and retried in order, nothing dropped -- is
    still there on the ephemeral path, because it is literally the same loop.

The third question, "did the refactor move a byte on the DURABLE path", is
answered separately in test_durable_sink_pin.py.
"""

from __future__ import annotations

import asyncio

import pytest

from queen.buffer.sinks import (
    DURABLE_DESTINATION,
    EPHEMERAL_SINK,
    ephemeral_address,
    ephemeral_destination,
)

from .envelopes import make, pushed, until

QUEUE = "presence"

# Big enough that the time trigger never fires inside a test: every drain in
# this file is caused by the count threshold, the bound, or an explicit flush.
NO_LINGER = 60000


def ephemeral_pushes(server):
    return [r for r in server.requests if r.path == "/api/v1/ephemeral/push"]


# ---------------------------------------------------------------------------
# The request it drains into.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drains_one_batch_to_the_ephemeral_route_with_identity_on_envelope():
    client, server = make()
    result = await client.ephemeral.push(
        QUEUE,
        [{"n": 1}, {"n": 2}],
        partition="room-7",
        buffered={"message_count": 2, "time_millis": NO_LINGER},
    )

    # It resolves when the messages are IN the buffer, not when they are at the
    # broker -- that distinction is the whole trade the option buys.
    assert result == {"buffered": True, "count": 2}

    await until(lambda: ephemeral_pushes(server), "the batch to reach the wire")
    hit = ephemeral_pushes(server)[0]
    assert hit.route == "POST /api/v1/ephemeral/push"
    assert hit.body == {
        "queue": QUEUE,
        "partition": "room-7",
        "messages": [{"payload": {"n": 1}}, {"payload": {"n": 2}}],
    }
    await client.close()


@pytest.mark.asyncio
async def test_omits_partition_on_the_drained_batch_when_the_push_named_none():
    client, server = make()
    await client.ephemeral.push(
        QUEUE, [{"n": 1}], buffered={"message_count": 1, "time_millis": NO_LINGER}
    )

    await until(lambda: ephemeral_pushes(server), "the batch to reach the wire")
    assert ephemeral_pushes(server)[0].body == {
        "queue": QUEUE,
        "messages": [{"payload": {"n": 1}}],
    }
    await client.close()


@pytest.mark.asyncio
async def test_close_drains_an_ephemeral_buffer_that_never_reached_its_threshold():
    """The buffer is drained by the same ``flush_all_buffers`` path
    ``client.close()`` already runs, under the same deadline."""
    client, server = make()
    await client.ephemeral.push(
        QUEUE, [{"n": 1}], buffered={"message_count": 1000, "time_millis": NO_LINGER}
    )
    assert ephemeral_pushes(server) == []

    await client.close()
    assert ephemeral_pushes(server)[0].body["messages"] == [{"payload": {"n": 1}}]


@pytest.mark.asyncio
async def test_never_shares_a_buffer_with_the_durable_queue_of_the_same_name():
    """§10 Q8: same name on both engines is legal and they are unrelated
    objects. A shared buffer would post one family's messages to the other
    family's route."""
    client, server = make()
    await client.ephemeral.push(
        "orders", [{"n": 1}], buffered={"message_count": 1, "time_millis": NO_LINGER}
    )
    await (
        client.queue("orders")
        .partition("Default")
        .buffer({"message_count": 1, "time_millis": NO_LINGER})
        .push({"n": 2})
    )

    await until(
        lambda: len([r for r in server.requests if r.path.endswith("push")]) >= 2,
        "both batches to reach the wire",
    )
    by_path = {r.path: r.body for r in server.requests}
    assert by_path["/api/v1/ephemeral/push"] == {
        "queue": "orders",
        "messages": [{"payload": {"n": 1}}],
    }
    assert list(by_path["/api/v1/push"].keys()) == ["items"]
    assert by_path["/api/v1/push"]["items"][0]["queue"] == "orders"
    await client.close()


def test_addresses_one_buffer_per_queue_partition_namespaced_under_eph():
    # A named partition and no partition are DIFFERENT destinations: the broker
    # picks when none was named, and merging the two would hand it a partition
    # the caller never chose.
    assert ephemeral_address("orders", "Default") == "eph:orders/Default"
    assert ephemeral_address("orders") == "eph:orders"
    assert ephemeral_destination("orders", "p").sink is EPHEMERAL_SINK
    assert DURABLE_DESTINATION.sink.path == "/api/v1/push"


# ---------------------------------------------------------------------------
# The semantics it inherits.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parks_an_add_at_the_bound_and_resumes_it_when_the_flusher_drains():
    """The 1.0.6 contract, unchanged on this path: at ``max_size`` the producer
    BLOCKS. It does not grow the process and it does not report a success the
    buffer never accepted."""
    release = asyncio.Event()

    async def slow_push(*args, **kwargs):
        await release.wait()
        return {"pushed": 1}

    client, server = make()
    client._http_client.post = slow_push  # type: ignore[method-assign]

    # The push adds one message at a time and awaits each add, so with a bound
    # of one the FIRST add schedules the drain (which then parks in the POST
    # above) and the SECOND meets a buffer at its bound with nothing draining
    # it. The push cannot resolve until the flusher frees capacity.
    blocked = asyncio.create_task(
        client.ephemeral.push(
            QUEUE,
            [{"n": 1}, {"n": 2}, {"n": 3}],
            buffered={"message_count": 1, "max_size": 1, "time_millis": NO_LINGER},
        )
    )

    await asyncio.sleep(0.05)
    assert not blocked.done(), "the add must park while the buffer is at its bound"

    release.set()
    assert await asyncio.wait_for(blocked, 2) == {"buffered": True, "count": 3}
    await client.close()


@pytest.mark.asyncio
async def test_puts_a_failed_batch_back_at_the_front_and_retries_it_in_order():
    """Nothing is dropped on a failed drain, and the producer's order survives
    the retry -- which is the property a re-queue at the front exists for."""
    client, server = make(
        [
            {"status": 503, "json": {"error": "ephemeral_unavailable"}},
            pushed(3),
        ]
    )

    await client.ephemeral.push(
        QUEUE,
        [{"n": 1}, {"n": 2}, {"n": 3}],
        buffered={
            "message_count": 3,
            "time_millis": NO_LINGER,
            "retry_delay_millis": 10,
        },
    )

    await until(
        lambda: len(ephemeral_pushes(server)) >= 2, "the failed batch to be retried"
    )
    first, retry = ephemeral_pushes(server)[:2]
    assert first.body["messages"] == retry.body["messages"]
    assert [m["payload"]["n"] for m in retry.body["messages"]] == [1, 2, 3]
    await client.close()


@pytest.mark.asyncio
async def test_an_explicit_flush_sends_what_is_buffered_now():
    client, server = make()
    await client.ephemeral.push(
        QUEUE,
        [{"n": 1}],
        partition="room-7",
        buffered={"message_count": 1000, "time_millis": NO_LINGER},
    )
    assert ephemeral_pushes(server) == []

    await client.ephemeral.flush(QUEUE, partition="room-7")
    assert ephemeral_pushes(server)[0].body["messages"] == [{"payload": {"n": 1}}]

    # A flush of an address with no buffer is a no-op, not a KeyError.
    await client.ephemeral.flush("never-pushed-to")
    await client.close()


@pytest.mark.asyncio
async def test_reports_the_messages_a_stopped_buffer_refused():
    """A push into a closed client raises instead of counting the message as
    accepted -- reported success for a message that only lived in this process
    is the exact lie the bounded buffer was written to remove."""
    client, server = make()
    await client.close()

    with pytest.raises(Exception) as caught:
        await client.ephemeral.push(
            QUEUE, [{"n": 1}], buffered={"message_count": 1000}
        )
    assert "closed" in str(caught.value)


@pytest.mark.asyncio
async def test_translates_interval_millis_into_the_linger_the_buffer_reads():
    """``resolve_buffer_options`` carries unknown keys untouched, so an
    untranslated ``interval_millis`` would be a linger that quietly does
    nothing: a producer batching on count alone, stalled below its threshold."""
    client, server = make()
    await client.ephemeral.push(
        QUEUE, [{"n": 1}], buffered={"interval_millis": 25, "message_count": 1000}
    )

    buffer = client._buffer_manager._buffers[ephemeral_address(QUEUE)]
    assert buffer.options["time_millis"] == 25

    with pytest.raises(ValueError, match="not both"):
        await client.ephemeral.push(
            QUEUE, [{"n": 2}], buffered={"interval_millis": 25, "time_millis": 30}
        )
    await client.close()


@pytest.mark.asyncio
async def test_a_hand_built_ephemeral_says_so_instead_of_dropping_the_option():
    from queen.ephemeral import Ephemeral

    client, server = make()
    bare = Ephemeral(client._http_client)
    with pytest.raises(ValueError, match="buffer manager"):
        await bare.push(QUEUE, [{"n": 1}], buffered=True)
    await client.close()
