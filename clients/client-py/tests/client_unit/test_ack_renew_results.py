"""
ack() and renew() report what the broker answered, not the HTTP status.

`POST /api/v1/ack/batch` answers 200 with one item per acknowledgment, and a
refused item carries `success: false` and an `error`
(server/src/rsm/facade/real.rs, render_ack). `POST /api/v1/lease/:id/extend`
answers 200 with `success: false, renewed: 0` when it extended nothing
(real.rs, renew_impl). The bodies below are what a 2.0.1 broker sent.
"""

from __future__ import annotations

import pytest

from queen import Queen

from ..kv_unit.plan_server import PlanServer


def make(*plan):
    server = PlanServer(*plan)
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    return client, server


def msg(txn: str, lease: str = "lease-1"):
    return {"transactionId": txn, "partitionId": "2", "leaseId": lease}


def ack_item(index: int, txn: str, success: bool, error=None, released=False):
    return {
        "index": index,
        "transactionId": txn,
        "success": success,
        "error": error,
        "leaseReleased": released,
        "dlq": False,
        "noop": False,
    }


def renew_body(lease: str, renewed: int):
    expires = "2026-10-06T12:41:34.021Z" if renewed else None
    return {
        "leaseId": lease,
        "success": renewed > 0,
        "renewed": renewed,
        "newExpiresAt": expires,
        "expiresAt": expires,
        "lease_expires_at": expires,
    }


# ---------------------------------------------------------------- batch ack


@pytest.mark.asyncio
async def test_a_refused_batch_ack_reports_failure():
    refused = "invalid or expired lease"
    client, server = make(
        {
            "status": 200,
            "json": [ack_item(0, "ack-1", False, refused), ack_item(1, "ack-2", False, refused)],
        }
    )
    try:
        res = await client.ack([msg("ack-1"), msg("ack-2")], True, {"group": "g1"})
    finally:
        await client.close()

    assert server.only.route == "POST /api/v1/ack/batch"
    assert res["success"] is False
    assert refused in res["error"]
    assert [r["success"] for r in res["results"]] == [False, False]


@pytest.mark.asyncio
async def test_a_partly_refused_batch_ack_reports_failure_and_counts_it():
    client, _ = make(
        {
            "status": 200,
            "json": [
                ack_item(0, "ack-1", True, released=True),
                ack_item(1, "ack-2", False, "invalid or expired lease"),
                ack_item(2, "ack-3", False, "invalid or expired lease"),
            ],
        }
    )
    try:
        res = await client.ack([msg("ack-1"), msg("ack-2"), msg("ack-3")], True, {"group": "g1"})
    finally:
        await client.close()

    assert res["success"] is False
    assert res["error"] == "2 of 3 acknowledgments rejected: invalid or expired lease"
    assert [r["success"] for r in res["results"]] == [True, False, False]


@pytest.mark.asyncio
async def test_an_accepted_batch_ack_reports_success_with_its_results():
    items = [ack_item(0, "ack-1", True, released=True), ack_item(1, "ack-2", True, released=True)]
    client, _ = make({"status": 200, "json": items})
    try:
        res = await client.ack([msg("ack-1"), msg("ack-2")], True, {"group": "g1"})
    finally:
        await client.close()

    assert res["success"] is True
    assert res["results"] == items


# -------------------------------------------------------------------- renew


@pytest.mark.asyncio
async def test_renew_reports_a_lease_that_extended_nothing_as_failed():
    client, server = make({"status": 200, "json": renew_body("lease-1", 0)})
    try:
        res = await client.renew("lease-1")
    finally:
        await client.close()

    assert server.only.route == "POST /api/v1/lease/lease-1/extend"
    assert res["leaseId"] == "lease-1"
    assert res["success"] is False
    assert res["renewed"] == 0
    assert res["newExpiresAt"] is None
    assert res["error"], "says why nothing was renewed"


@pytest.mark.asyncio
async def test_renew_reports_an_extended_lease_as_renewed():
    client, _ = make({"status": 200, "json": renew_body("lease-1", 1)})
    try:
        res = await client.renew({"leaseId": "lease-1"})
    finally:
        await client.close()

    assert res == {
        "leaseId": "lease-1",
        "success": True,
        "renewed": 1,
        "newExpiresAt": "2026-10-06T12:41:34.021Z",
    }


@pytest.mark.asyncio
async def test_renew_of_several_leases_reports_each_one():
    client, _ = make(
        {"status": 200, "json": renew_body("lease-1", 1)},
        {"status": 200, "json": renew_body("lease-2", 0)},
    )
    try:
        res = await client.renew([msg("a", "lease-1"), msg("b", "lease-2")])
    finally:
        await client.close()

    assert [(r["leaseId"], r["success"]) for r in res] == [("lease-1", True), ("lease-2", False)]
