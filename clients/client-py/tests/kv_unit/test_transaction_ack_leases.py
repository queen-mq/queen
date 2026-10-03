"""
Each ack of a transaction bundle carries the lease of its own message.

A Queen 2 broker fences an ACK in ``POST /api/v1/transaction`` with the
``leaseId`` on that operation, and lends it one from ``requiredLeases`` only
when every lease in the bundle is the same lease. With the leases in
``requiredLeases`` alone, a bundle that acks messages of two leases is applied
unfenced: an ack completes its message even after its lease expired and
another consumer took it. The broker-side half of this is
tests/test_transaction_lease_fencing.py.
"""

from __future__ import annotations

import pytest

from queen import Queen

from .plan_server import PlanServer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unleased",
    [
        {"transactionId": "txn-3", "partitionId": "part-3"},
        {"transactionId": "txn-3", "partitionId": "part-3", "leaseId": None},
        {"transactionId": "txn-3", "partitionId": "part-3", "leaseId": ""},
    ],
    ids=["missing", "none", "empty"],
)
async def test_each_ack_names_its_own_lease(unleased):
    server = PlanServer()
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    await (
        client.transaction()
        .ack({"transactionId": "txn-1", "partitionId": "part-1", "leaseId": "lease-1"})
        .ack({"transactionId": "txn-2", "partitionId": "part-2", "leaseId": "lease-2"})
        .ack(unleased)
        .commit()
    )

    assert server.only.route == "POST /api/v1/transaction"
    body = server.only.body
    ops = body["operations"]
    assert [op["transactionId"] for op in ops] == ["txn-1", "txn-2", "txn-3"]
    assert ops[0].get("leaseId") == "lease-1"
    assert ops[1].get("leaseId") == "lease-2"
    # No lease, no key: not null, not "".
    assert "leaseId" not in ops[2]
    # requiredLeases is what it was: each lease once (the set() makes the
    # order arbitrary), and nothing for the message without one.
    assert sorted(body["requiredLeases"]) == ["lease-1", "lease-2"]
    await client.close()
