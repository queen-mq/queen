"""
The transaction wire's two rider arrays -- the shape decision this feature is
most likely to get wrong, and the one that fails SILENTLY when it is wrong.

PLAN_KV_TIMERS.md §6.3, §8.2, §10.4:

  * `kv` and `timers` are TOP-LEVEL fields of the request, beside `operations`
    and never inside it. The reason is a Go failure -- two struct fields with
    the same JSON key at the same level are BOTH dropped by encoding/json, with
    no error -- but the SHAPE is one shape for all seven clients, so Python is
    held to it too and a test says so.
  * A bundle carrying neither array must produce the body it produces today,
    byte for byte, or every existing transaction test is testing a new wire.
  * `commit()` RETURNS on `{success:false, reason:'kv_precondition'}` and raises
    on everything else (§8.3, §10.2). A lost precondition is the expected
    outcome of every legitimate redelivery; raising would put the product's
    most frequent outcome into the caller's error path.
"""

from __future__ import annotations

import base64
import json

import pytest

from queen import Queen

from .plan_server import PlanServer


def make(plan=None):
    server = PlanServer(*(plan or []))
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    return client, server


def a_message(txn="txn-a", partition="part-a", lease="lease-a"):
    return {"transactionId": txn, "partitionId": partition, "leaseId": lease}


# ---------------------------------------------------------------------------
# Shape.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kv_and_timers_are_top_level_and_never_inside_operations():
    client, server = make()
    await (
        client.transaction()
        .ack(a_message())
        .kv.put("saga", "order:9f1", {"state": "closed"}, ttl_seconds=3600)
        .timer("orders")
        .key("order:9f1:expire")
        .payload({"orderId": "9f1"})
        .after_ms(60_000)
        .txn("fixed-txn")
        .schedule()
        .commit()
    )
    body = server.only.body
    assert set(body) == {"operations", "requiredLeases", "kv", "timers"}
    assert [o["type"] for o in body["operations"]] == ["ack"]
    assert body["kv"] == [
        {
            "op": "put",
            "ns": "saga",
            "key": "order:9f1",
            "value": {"state": "closed"},
            "ttlSeconds": 3600,
        }
    ]
    assert body["timers"] == [
        {
            "op": "schedule",
            "queue": "orders",
            "timerKey": "order:9f1:expire",
            "payload": base64.b64encode(json.dumps({"orderId": "9f1"}).encode()).decode(),
            "delayMs": 60000,
            "txn": "fixed-txn",
        }
    ]
    # And the negative of the same rule, stated so it cannot regress: no
    # operation carries a kv or timer leg.
    for op in body["operations"]:
        assert "kv" not in op and "timers" not in op
    await client.close()


@pytest.mark.asyncio
async def test_a_bundle_without_riders_is_byte_identical_to_today():
    """§6.3: the wire procedure's return is byte-identical when the arrays are
    absent. The REQUEST has to be too, or the compatibility argument is only
    half made."""
    client, server = make()
    await client.transaction().queue("orders").push([{"data": {"v": 1}, "transactionId": "t1"}]).commit()
    body = server.only.body
    assert list(body) == ["operations", "requiredLeases"]
    assert "kv" not in body and "timers" not in body
    await client.close()


@pytest.mark.asyncio
async def test_a_kv_only_bundle_commits_without_any_operations():
    """§2.5: a KV-only bundle is routed away from the wire by the broker, which
    means the client must be able to build one -- the old "no operations to
    commit" guard would have refused it."""
    client, server = make()
    await client.transaction().kv.put_if_absent("dedup", "evt:1", True, ttl_seconds=86400).commit()
    assert server.only.body["operations"] == []
    assert len(server.only.body["kv"]) == 1
    await client.close()


@pytest.mark.asyncio
async def test_an_empty_transaction_is_still_refused():
    client, _ = make()
    with pytest.raises(Exception, match="no operations"):
        await client.transaction().commit()
    await client.close()


@pytest.mark.asyncio
async def test_get_prefix_is_refused_in_the_wire():
    """§5.5: unbounded read work inside the transaction that holds the
    OUTERMOST lock space of the product. The broker raises 22023; the SDK makes
    it unwritable, which is cheaper for everyone."""
    client, _ = make()
    with pytest.raises(ValueError, match="getPrefix"):
        client.transaction().kv.get_prefix("saga", "order:")
    await client.close()


@pytest.mark.asyncio
async def test_tx_once_is_a_required_put_if_absent_and_goes_first():
    """§11.4 / S13: the gate is the FIRST op of the bundle, and `required` is
    what makes the whole transaction abort when the marker already exists.
    Without `required` the bundle commits and the gate was decoration."""
    client, server = make()
    await (
        client.transaction()
        .once("dedup", "evt:1", ttl_seconds=86400)
        .queue("orders")
        .push([{"data": {"v": 1}, "transactionId": "t1"}])
        .commit()
    )
    assert server.only.body["kv"] == [
        {
            "op": "putIfAbsent",
            "ns": "dedup",
            "key": "evt:1",
            "value": True,
            "ttlSeconds": 86400,
            "required": True,
        }
    ]
    await client.close()


@pytest.mark.asyncio
async def test_an_until_expiry_is_measured_at_send_time_not_at_queue_time():
    """`until=<instant>` is sugar for a delta, and the delta has to be measured
    when the bundle is SENT. A bundle assembled around a pop can spend real time
    between the `tx.kv.put` call and `commit()`, and a TTL frozen at queue time
    would already be stale on arrival."""
    import asyncio
    from datetime import datetime, timedelta, timezone

    client, server = make()
    tx = client.transaction().kv.put(
        "saga", "k", 1, until=datetime.now(timezone.utc) + timedelta(seconds=10)
    )
    await asyncio.sleep(1.1)
    await tx.commit()
    # Ten seconds were asked for, one was spent assembling: what goes out is
    # what is left, not what was asked for.
    assert server.only.body["kv"][0]["ttlSeconds"] <= 9
    await client.close()


@pytest.mark.asyncio
async def test_the_tx_timer_cancel_leg_rides_the_timers_array():
    """Inside a bundle a cancel has no separate route to take -- it is part of
    the transaction, which is the whole point. The always-allowed DELETE route
    is what the STANDALONE cancel uses (§9.6), and that split is deliberate."""
    client, server = make()
    await (
        client.transaction()
        .ack(a_message())
        .timer("orders")
        .key("order:9f1:expire")
        .cancel()
        .commit()
    )
    assert server.only.body["timers"] == [
        {"op": "cancel", "queue": "orders", "timerKey": "order:9f1:expire"}
    ]
    await client.close()


# ---------------------------------------------------------------------------
# §8.3 -- the failure body, and the one branch that returns instead of raising.
# ---------------------------------------------------------------------------


PRECONDITION = {
    "status": 200,
    "json": {
        "transactionId": "txn-1",
        "success": False,
        "ok": False,
        "reason": "kv_precondition",
        "error": "kv_precondition_failed",
        "results": [],
        "failedIndex": 1,
        "kvReason": "exists",
        "version": 90101,
        "value": {"by": "another-worker"},
    },
}


@pytest.mark.asyncio
async def test_commit_returns_on_a_lost_precondition():
    client, _ = make([PRECONDITION])
    res = await (
        client.transaction()
        .once("dedup", "evt:1", ttl_seconds=86400)
        .queue("orders")
        .push([{"data": {"v": 1}}])
        .commit()
    )
    assert res["success"] is False
    assert res["reason"] == "kv_precondition"
    # Everything the loser needs, without a second round trip (§5.3, §10.3).
    assert res["failedIndex"] == 1
    assert res["kvReason"] == "exists"
    assert res["version"] == 90101
    assert res["value"] == {"by": "another-worker"}
    await client.close()


@pytest.mark.asyncio
async def test_a_returned_precondition_is_falsy_and_a_success_is_truthy():
    client, _ = make([PRECONDITION, {"status": 200, "json": {"transactionId": "t", "success": True, "results": []}}])
    lost = await client.transaction().once("dedup", "evt:1", ttl_seconds=60).commit()
    assert not lost
    won = await client.transaction().once("dedup", "evt:2", ttl_seconds=60).commit()
    assert won
    await client.close()


@pytest.mark.asyncio
async def test_commit_still_raises_on_every_other_failure():
    """The precondition branch is a NARROW exemption. A lease that expired, a
    bad request, a database error -- all still raise, or the caller would treat
    a broken transaction as a lost race and carry on."""
    client, _ = make(
        [
            {"status": 200, "json": {"transactionId": "t", "success": False, "reason": "ack_rejected", "error": "QTXN lease expired", "results": []}},
        ]
    )
    with pytest.raises(Exception, match="QTXN"):
        await client.transaction().ack(a_message()).commit()
    await client.close()


@pytest.mark.asyncio
async def test_a_precondition_shaped_400_still_raises():
    """The exemption is keyed on reason AND on the call having succeeded. A
    4xx is a failed call whatever its body says."""
    client, _ = make([{"status": 400, "json": {"transactionId": "t", "success": False, "reason": "kv_precondition", "error": "bad"}}])
    with pytest.raises(Exception):
        await client.transaction().once("dedup", "e", ttl_seconds=60).commit()
    await client.close()
