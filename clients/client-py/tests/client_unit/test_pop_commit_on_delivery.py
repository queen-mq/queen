"""
commit_on_delivery() is the pop's commit at delivery; auto_ack() is consume()'s.

The broker's `autoAck=true` on a pop moves the consumer group's cursor past the
messages as it hands them out: no lease, nothing to ack, at-most-once. The SDK
sends it only for commit_on_delivery(). auto_ack() means the ack the client
sends after a consume() handler, and never reaches the wire.
"""

from __future__ import annotations

import pytest

from queen import Queen

from ..conflation_unit.pop_transport import PopTransport, message, pop_body


def make(*pop_plan):
    transport = PopTransport(*pop_plan)
    client = Queen(url="http://plan.local", transport=transport, retry_attempts=1)
    return client, transport


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["pop", "pop_result"])
async def test_commit_on_delivery_sends_auto_ack_on_the_pop(terminal):
    client, transport = make(pop_body([message()]))
    try:
        await getattr(client.queue("orders").group("workers").commit_on_delivery(), terminal)()
    finally:
        await client.close()

    assert transport.pops[0].param("autoAck") == "true"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configure",
    [
        lambda b: b,
        lambda b: b.auto_ack(True),
        lambda b: b.auto_ack(False),
        lambda b: b.commit_on_delivery(False),
    ],
    ids=["never-called", "auto_ack(True)", "auto_ack(False)", "commit_on_delivery(False)"],
)
async def test_a_pop_stays_leased_without_commit_on_delivery(configure):
    client, transport = make(pop_body([message()]))
    try:
        await configure(client.queue("orders").group("workers")).pop()
    finally:
        await client.close()

    assert transport.pops[0].param("autoAck") is None


@pytest.mark.asyncio
async def test_consume_refuses_commit_on_delivery_before_any_request():
    client, transport = make(pop_body([message()]))

    async def handler(msg):
        return None

    try:
        with pytest.raises(ValueError, match="commit_on_delivery"):
            await client.queue("orders").group("workers").commit_on_delivery().limit(1).consume(
                handler
            )
    finally:
        await client.close()

    assert transport.requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configure, acked",
    [(lambda b: b, True), (lambda b: b.auto_ack(True), True), (lambda b: b.auto_ack(False), False)],
    ids=["never-called", "auto_ack(True)", "auto_ack(False)"],
)
async def test_consume_keeps_its_client_side_ack(configure, acked):
    """consume() acks after its handler unless told not to, and never puts
    autoAck on the wire: the client owns the ack there."""
    client, transport = make(pop_body([message()]))

    async def handler(msg):
        return None

    try:
        builder = configure(client.queue("orders").group("workers"))
        await builder.batch(1).wait(False).limit(1).consume(handler)
    finally:
        await client.close()

    assert transport.pops[0].param("autoAck") is None
    sent = [r for r in transport.requests if r.path.startswith("/api/v1/ack")]
    assert bool(sent) is acked
