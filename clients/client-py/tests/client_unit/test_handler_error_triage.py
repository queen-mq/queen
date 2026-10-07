"""
A handler error is the handler's, never a pop error.

Under auto_ack(False) the consumer sends no nack for a handler that raises:
that is the design, and the error stops the consumer. The worker's error
triage is for the broker's answers, so a handler error whose text says
"timeout" or "connection" must not be read as a long-poll timeout or a network
fault: that kept the loop polling with the message still leased, and the error
was lost.
"""

from __future__ import annotations

import asyncio

import pytest

from queen import Queen

from ..conflation_unit.pop_transport import PopTransport, message, pop_body

CONSUME_TIMEOUT_S = 5.0


class PaymentsDown(Exception):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "text, wait",
    [
        ("connection to the payments API was refused", False),
        ("timeout waiting for the database", True),
        ("Request timed out upstream", True),
        ("plain failure", False),
    ],
    ids=["says-connection", "says-timeout", "says-timed-out", "plain"],
)
async def test_a_handler_error_under_auto_ack_false_stops_the_consumer(text, wait):
    transport = PopTransport(pop_body([message("txn-1")]))
    client = Queen(url="http://plan.local", transport=transport, retry_attempts=1)

    async def handler(msg):
        raise PaymentsDown(text)

    try:
        with pytest.raises(PaymentsDown, match=text):
            await asyncio.wait_for(
                client.queue("orders")
                .group("workers")
                .batch(1)
                .auto_ack(False)
                .wait(wait)
                .consume(handler),
                CONSUME_TIMEOUT_S,
            )
    finally:
        await client.close()

    assert len(transport.pops) == 1, "the consumer polled again after the handler failed"
    assert not [r for r in transport.requests if r.path.startswith("/api/v1/ack")], "no nack"
