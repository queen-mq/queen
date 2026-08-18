"""
The kv/timer riders on the transaction wire, against a real broker.

PLAN_KV_TIMERS.md §6.3 (the graft), §8.2 (the flat index space), §8.3 (the
failure body), §11.4 (the gate belongs FIRST in the bundle).

This file is where the feature's actual claim gets tested: not "a KV write
works" but "the KV write and the push are the same transaction", i.e. that
losing the gate leaves NO message behind.
"""

import asyncio

import pytest

NS = "test-kv-txn-py"


@pytest.mark.asyncio
async def test_the_gate_and_the_push_are_one_transaction(client):
    """The whole product claim, in one test: a bundle whose gate loses does not
    push. Without `required` on the marker the gate would be decoration -- the
    marker would lose its race and the message would go out anyway."""
    queue = "test-kv-txn-gate"
    await client.queue(queue).create()

    first = await (
        client.transaction()
        .queue(queue)
        .push([{"data": {"attempt": 1}}])
        .once(NS, "gate:evt-1", ttl_seconds=300)
        .commit()
    )
    assert first["success"] is True
    assert first

    second = await (
        client.transaction()
        .queue(queue)
        .push([{"data": {"attempt": 2}}])
        .once(NS, "gate:evt-1", ttl_seconds=300)
        .commit()
    )
    # RETURNED, not raised (§8.3): a lost precondition is the expected outcome
    # of every legitimate redelivery and must not reach a retry policy.
    assert second["success"] is False
    assert second["reason"] == "kv_precondition"
    assert second["kvReason"] == "exists"
    assert not second
    # §8.2 point 4: failedIndex is in the FLAT space. One push item occupies
    # index 0, so the kv array starts at 1 and the gate is flat index 1 -- get
    # this wrong and the caller inspects somebody else's operation.
    assert second["failedIndex"] == 1

    messages = await client.queue(queue).batch(10).wait(False).pop()
    assert len(messages) == 1, "the losing bundle pushed a message anyway"
    assert messages[0]["data"] == {"attempt": 1}


@pytest.mark.asyncio
async def test_a_kv_write_shares_the_fate_of_its_ack(client):
    """§5.2's hierarchy, demonstrated: the ack transaction is the PRIMARY fence.
    A bogus ack rolls the bundle back, and the KV write with it -- something a
    CAS could not do, because an `expect` on a still-matching version succeeds
    from a zombie too."""
    queue = "test-kv-txn-fate"
    await client.queue(queue).create()
    await client.queue(queue).push([{"data": {"v": 1}}])
    messages = await client.queue(queue).batch(1).wait(False).pop()
    assert messages

    bogus = dict(messages[0])
    bogus["transactionId"] = "00000000-0000-4000-8000-000000000000"

    with pytest.raises(Exception):
        await (
            client.transaction()
            .ack(bogus)
            .kv.put(NS, "fate:marker", {"written": True}, ttl_seconds=300)
            .commit()
        )

    assert (await client.kv.get(NS, "fate:marker"))["found"] is False


@pytest.mark.asyncio
async def test_a_kv_only_bundle_commits(client):
    """§2.5: a bundle with no push and no ack buys nothing from the wire's lock
    discipline, so the broker routes it straight to the KV procedure. From the
    client it just has to work."""
    res = await (
        client.transaction()
        .kv.put(NS, "solo:a", {"a": 1}, ttl_seconds=300)
        .kv.put(NS, "solo:b", {"b": 2}, ttl_seconds=300)
        .commit()
    )
    assert res["success"] is True
    assert (await client.kv.get(NS, "solo:b"))["value"] == {"b": 2}


@pytest.mark.asyncio
async def test_get_prefix_is_refused_inside_a_bundle(client):
    """§5.5: read work the caller does not bound, under the outermost lock space
    of the product. Refused by the SDK before the round trip, and by the
    procedure if anybody bypasses the SDK."""
    with pytest.raises(ValueError):
        client.transaction().kv.get_prefix(NS, "solo:")


@pytest.mark.asyncio
async def test_a_timer_rides_the_bundle_and_fires(client):
    """The saga shape: close the work and arm the compensation atomically."""
    queue = "test-timer-txn-ride"
    await client.queue(queue).create()

    res = await (
        client.transaction()
        .kv.put(NS, "saga:9f1", {"state": "open"}, ttl_seconds=300)
        .timer(queue)
        .key("saga:9f1:timeout")
        .payload({"sagaId": "9f1"})
        .after_ms(500)
        .schedule()
        .commit()
    )
    assert res["success"] is True

    # See tests/test_timers.py::_DELIVERY_TIMEOUT_S for why this deadline is
    # tens of seconds: a fired timer is not poppable until the hot-list ring is
    # reseeded, while a plain push is poppable immediately.
    deadline = asyncio.get_event_loop().time() + 45
    messages = []
    while asyncio.get_event_loop().time() < deadline:
        messages = await client.queue(queue).batch(10).wait(False).pop()
        if messages:
            break
        await asyncio.sleep(0.2)
    assert messages, "the timer scheduled inside the bundle never fired"
    assert messages[0]["data"] == {"sagaId": "9f1"}

    # §4.4 in practice: the compensation consumer checks the saga's KV state
    # BEFORE compensating, because a cancel that answered `absent` may have
    # arrived 5 ms after the fire. This is the only correct shape, and no
    # example may show the other one.
    state = await client.kv.get(NS, "saga:9f1")
    assert state["found"] is True and state["value"] == {"state": "open"}


@pytest.mark.asyncio
async def test_a_timer_cancel_rides_the_bundle(client):
    queue = "test-timer-txn-cancel"
    await client.queue(queue).create()
    await client.timers.schedule(queue, "txn:cancel", {"v": 1}, delay_ms=60_000)

    res = await (
        client.transaction()
        .kv.put(NS, "saga:closed", {"state": "closed"}, ttl_seconds=300)
        .timer(queue)
        .key("txn:cancel")
        .cancel()
        .commit()
    )
    assert res["success"] is True
    assert (await client.timers.peek(queue, "txn:cancel"))["found"] is False
