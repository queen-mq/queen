"""
The timer wire contract, asserted on the EXACT JSON body and the EXACT route.

PLAN_KV_TIMERS.md §4 (the declared contract), §8.1 (routes), §9.6 (cancel is
never blockable, which is a ROUTE decision and therefore an SDK decision).
"""

from __future__ import annotations

import base64
import json
from datetime import timedelta

import pytest

from queen import Queen
from queen.errors import TimerError

from .plan_server import PlanServer, error_body


def make(plan=None):
    server = PlanServer(*(plan or []))
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    return client, server


def ops_of(server: PlanServer):
    return server.last.body["operations"]


def decoded(op) -> object:
    return json.loads(base64.b64decode(op["payload"]).decode())


@pytest.mark.asyncio
async def test_schedule_body_is_exactly_this():
    client, server = make()
    res = await client.timers.schedule(
        "orders", "order:9f1:expire", {"orderId": "9f1"}, delay_ms=30_000, txn="fixed-txn"
    )
    assert server.only.route == "POST /api/v1/timers"
    assert server.only.body == {
        "operations": [
            {
                "op": "schedule",
                "queue": "orders",
                "timerKey": "order:9f1:expire",
                "payload": base64.b64encode(json.dumps({"orderId": "9f1"}).encode()).decode(),
                "delayMs": 30000,
                "txn": "fixed-txn",
            }
        ]
    }
    assert res["status"] == "scheduled"
    await client.close()


@pytest.mark.asyncio
async def test_the_payload_is_base64_and_bytes_go_through_untouched():
    client, server = make()
    await client.timers.schedule("orders", "k", {"a": 1}, delay_ms=1)
    assert decoded(ops_of(server)[0]) == {"a": 1}

    await client.timers.schedule("orders", "k", b"\x00\x01raw", delay_ms=1)
    assert base64.b64decode(ops_of(server)[0]["payload"]) == b"\x00\x01raw"
    await client.close()


@pytest.mark.asyncio
async def test_the_delay_is_relative_and_in_milliseconds():
    """§4.2 and §20.6: only relative durations on this wire, and the rule of the
    product is "durations that can be sub-second are in ms, the ones that cannot
    are in seconds". A 250 ms retry backoff is a central use of timers."""
    client, server = make()
    await client.timers.schedule("orders", "k", {}, delay_ms=250)
    assert ops_of(server)[0]["delayMs"] == 250

    await client.timers.schedule("orders", "k", {}, delay=timedelta(minutes=2))
    assert ops_of(server)[0]["delayMs"] == 120_000

    # An absolute instant is not expressible, and asking for one is an error
    # here rather than a 22023 from the stored procedure.
    with pytest.raises(TypeError):
        await client.timers.schedule("orders", "k", {}, deliver_at="2026-09-01T00:00:00Z")
    with pytest.raises(ValueError, match="delay"):
        await client.timers.schedule("orders", "k", {})
    await client.close()


@pytest.mark.asyncio
async def test_a_delay_in_the_past_is_legal():
    """§4.2: a deliverAt in the past is LEGAL and fires on the first cycle. The
    SDK must not "helpfully" clamp it to zero or refuse it."""
    client, server = make()
    await client.timers.schedule("orders", "k", {}, delay_ms=-5000)
    assert ops_of(server)[0]["delayMs"] == -5000
    await client.close()


@pytest.mark.asyncio
async def test_server_owned_fields_are_refused_locally():
    """§4.2: producerSub, messageId and the tenant are not input fields. The
    procedure raises 22023 on them; the SDK must not offer a way to send them,
    or the first person to try gets an audit finding instead of a test failure."""
    client, _ = make()
    for field in ("producerSub", "messageId", "tenant", "_tenant", "deliverAt", "delaySeconds"):
        with pytest.raises(ValueError, match="server-owned"):
            await client.timers.batch([{"op": "schedule", "queue": "q", "timerKey": "k", "delayMs": 1, "payload": "e30=", field: "x"}])
    await client.close()


@pytest.mark.asyncio
async def test_the_txn_is_minted_client_side_when_absent_and_returned():
    """§4.4: `absent` may mean ALREADY DELIVERED, and the authority is the log.
    A caller can only look for the timer's txn in the destination queue if it
    knows it, so the SDK mints it rather than letting the broker do it."""
    client, server = make()
    res = await client.timers.schedule("orders", "k", {}, delay_ms=1)
    sent = ops_of(server)[0]["txn"]
    assert isinstance(sent, str) and len(sent) >= 32
    assert res["txn"] == sent
    await client.close()


@pytest.mark.asyncio
async def test_reschedule_is_the_same_upsert_under_another_name():
    client, server = make()
    await client.timers.reschedule("orders", "k", {"v": 2}, delay_ms=5_000, txn="t")
    assert ops_of(server)[0]["op"] == "reschedule"
    await client.close()


@pytest.mark.asyncio
async def test_partition_is_omitted_when_not_given():
    """The procedure defaults it to 'Default'. Sending it anyway would put a
    client-side default in front of a server-side one, which is two places for
    the same decision."""
    client, server = make()
    await client.timers.schedule("orders", "k", {}, delay_ms=1)
    assert "partition" not in ops_of(server)[0]
    await client.timers.schedule("orders", "k", {}, delay_ms=1, partition="cust-7")
    assert ops_of(server)[0]["partition"] == "cust-7"
    await client.close()


# ---------------------------------------------------------------------------
# §9.6 -- THE ROUTE DECISION. This is the one place where an SDK can undo a
# server-side guarantee by picking the wrong URL.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_uses_the_delete_route_and_never_the_batch():
    """§9.6: `POST /api/v1/timers` carries cancels in the same array as
    schedules, so a cancel sent there inherits the SCHEDULE's authorization and
    is 403'd on a blocked cluster. `DELETE /api/v1/timers/:queue/*timerKey` is
    its own route with its own class and is never blockable. A tenant that
    cannot cancel keeps producing messages it cannot stop."""
    client, server = make()
    await client.timers.cancel("orders", "order:9f1:expire")
    assert server.only.route == "DELETE /api/v1/timers/orders/order:9f1:expire"
    assert server.only.body is None
    await client.close()


@pytest.mark.asyncio
async def test_cancel_echoes_the_expected_txn_as_the_one_query_parameter():
    """§4.4: the response carries the txn back on `absent` so the "was it
    already delivered?" check needs no second API call."""
    client, server = make()
    await client.timers.cancel("orders", "k", txn="the-txn")
    assert server.only.query == {"txn": ["the-txn"]}
    await client.close()


@pytest.mark.asyncio
async def test_a_timer_key_with_slashes_stays_one_key():
    """The route's `*timerKey` is a catch-all, and `tenant/42` must arrive as
    one key -- not two segments, and not a percent-escape the broker decodes
    into something else."""
    client, server = make()
    await client.timers.cancel("orders", "tenant/42")
    assert server.only.path == "/api/v1/timers/orders/tenant/42"
    await client.close()


@pytest.mark.asyncio
async def test_peek_and_list_routes():
    client, server = make()
    await client.timers.peek("orders", "order:9f1:expire")
    assert server.last.route == "GET /api/v1/timers/orders/order:9f1:expire"

    await client.timers.list("orders", after="k1", limit=50)
    assert server.last.path == "/api/v1/timers/orders"
    assert server.last.query == {"after": ["k1"], "limit": ["50"]}
    await client.close()


@pytest.mark.asyncio
async def test_absent_is_falsy_because_ok_is_false():
    """§4.4: `absent` carries ok:false. The in-house lesson is queue delete,
    where `deleted:false` with a 200 read as success to every client that
    trusted the field."""
    client, _ = make(
        [
            {"status": 200, "json": {"ok": False, "status": "absent", "queue": "orders", "timerKey": "k", "txn": "t"}},
            {"status": 200, "json": {"ok": True, "status": "cancelled", "queue": "orders", "timerKey": "k", "txn": "t"}},
        ]
    )
    absent = await client.timers.cancel("orders", "k", txn="t")
    assert not absent and absent["status"] == "absent"
    assert absent["txn"] == "t"
    assert await client.timers.cancel("orders", "k", txn="t")
    await client.close()


@pytest.mark.asyncio
async def test_too_late_is_a_verdict_with_a_200_not_an_exception():
    """§4.3: the broker holding the claim has already packed that payload and is
    about to commit it. `too_late` is a verdict, and the remedy is a new key."""
    client, _ = make([{"status": 200, "json": {"ok": False, "status": "too_late", "queue": "orders", "timerKey": "k"}}])
    res = await client.timers.cancel("orders", "k")
    assert res["status"] == "too_late" and not res
    await client.close()


# ---------------------------------------------------------------------------
# The builder (§10.2: "builder con terminale schedule()/cancel()").
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_builder_produces_the_same_body_as_the_method():
    client, server = make()
    await (
        client.timer("orders")
        .key("order:9f1:expire")
        .payload({"orderId": "9f1"})
        .after_ms(30_000)
        .txn("fixed-txn")
        .schedule()
    )
    from_builder = server.last.body
    await client.timers.schedule("orders", "order:9f1:expire", {"orderId": "9f1"}, delay_ms=30_000, txn="fixed-txn")
    assert from_builder == server.last.body
    await client.close()


@pytest.mark.asyncio
async def test_the_builder_cancel_terminal_also_uses_the_delete_route():
    client, server = make()
    await client.timer("orders").key("order:9f1:expire").cancel()
    assert server.only.route == "DELETE /api/v1/timers/orders/order:9f1:expire"
    await client.close()


@pytest.mark.asyncio
async def test_the_builder_after_accepts_a_timedelta():
    client, server = make()
    await client.timer("orders").key("k").payload({}).after(timedelta(seconds=45)).schedule()
    assert ops_of(server)[0]["delayMs"] == 45_000
    await client.close()


@pytest.mark.asyncio
async def test_the_operator_kill_switch_pauses_schedules_with_a_503():
    """The timer surface is on every cell, so there is no "not enabled here"
    answer left to handle. The operator's runtime switch remains, and it is
    narrower than the old boot flag was: it pauses SCHEDULING only, because a
    cancel that could be blocked would leave a tenant producing messages it
    cannot stop (§9.6). See `test_a_cancel_is_never_paused` below.
    """
    client, _ = make([{**error_body(503, "timers_disabled", "timers_disabled"), "retry_after": 1}])
    with pytest.raises(TimerError) as ei:
        await client.timers.schedule("orders", "k", {}, delay_ms=1)
    assert ei.value.status == 503
    assert ei.value.code == "timers_disabled"
    assert ei.value.reason == "timers_disabled"
    assert ei.value.retry_after_seconds == 1
    await client.close()


@pytest.mark.asyncio
async def test_a_cancel_is_never_paused():
    """The DELETE route is off the switch on the broker side, so the SDK must
    not add a gate of its own in front of it: a cancel goes out and succeeds
    while schedules are paused."""
    client, server = make([{"status": 200, "json": {"ok": True, "status": "cancelled", "txn": "t-1"}}])
    res = await client.timers.cancel("orders", "k", txn="t-1")
    assert res
    assert server.only.method == "DELETE"
    await client.close()


@pytest.mark.asyncio
async def test_a_horizon_refusal_is_a_403_with_its_own_code():
    """§9.5: the horizon is a plan verdict, not a shape error, and it has its
    own code so an operator can tell it from a quota."""
    client, _ = make([error_body(403, "timer_horizon_exceeded", "timers_horizon")])
    with pytest.raises(TimerError) as ei:
        await client.timers.schedule("orders", "k", {}, delay_ms=10**12)
    assert ei.value.code == "timer_horizon_exceeded"
    await client.close()
