"""
The tests behind the published examples.

Every marked region in this file is rendered on queenmq.com through
webdoc/scripts/gen-snippets.mjs: real queue names, a partition key that means
something, and no test scaffolding inside a marked region. Assertions stay
outside the markers.

After editing a marked region, regenerate the partials with
`pnpm --dir webdoc gen` or the docs CI check fails on drift. The queues used
here (orders, payments) are wiped by the cleanup fixture in conftest.py, which
is what lets the dedup example keep a fixed transactionId across runs.
"""

import pytest


@pytest.mark.asyncio
async def test_docs_examples(client):
    """Push, consume, fan-out pop and dedup, exactly as the docs show them."""
    # docs:start(py-push)
    res = await client.queue("orders").partition("customer-42").push([
        {"data": {"orderId": 9137, "amount": 99.5}}
    ])
    # docs:end
    assert res[0].get("status") == "queued"

    # docs:start(py-consume)
    async def handle(message):
        print(message["data"])

    await (
        client.queue("orders")
        .group("billing")
        .subscription_mode("all")
        .limit(1)
        .each()
        .consume(handle)
    )
    # docs:end

    # The consume loop acks on return, so billing's cursor moved past the order.
    drained = await client.queue("orders").group("billing").batch(1).wait(False).pop()
    assert len(drained) == 0

    # A raw pop on another cursor still sees the message: groups are fan-out.
    # docs:start(py-pop)
    messages = await client.queue("orders").batch(10).wait(True).pop()
    # docs:end
    assert len(messages) == 1
    assert messages[0]["data"]["orderId"] == 9137

    # docs:start(py-push-dedup)
    first = await client.queue("payments").partition("customer-42").push([
        {"transactionId": "order-9137-paid", "data": {"orderId": 9137, "amount": 99.5}}
    ])

    retry = await client.queue("payments").partition("customer-42").push([
        {"transactionId": "order-9137-paid", "data": {"orderId": 9137, "amount": 99.5}}
    ])
    # retry[0]["status"] == "duplicate": the second push wrote nothing
    # and answers with the first message's id.
    # docs:end
    assert first[0].get("status") == "queued"
    assert retry[0].get("status") == "duplicate"
    assert retry[0].get("message_id") == first[0].get("message_id")
