"""
Filtered groups (PLAN_ROUTING.md §4): ``.where()`` on the pop/consume builder
goes on the query string of every pop of the group, from BOTH parameter
builders (pop() and consume() are separate code), and nowhere else; the admin
route changes it.

What is at stake: a where the broker refuses (``where_mismatch``, a bad where)
or ignores (no group, a namespace pop, a broker older than filtered groups)
must never read as "no messages" or as the whole queue -- so those raise here,
and a pop without a where is the request it always was. The broker echoes
``"filtered": true`` on every answer of a pop that sent a where, empty ones
included; an answer without it (or a bodiless 204) is an older broker.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple

import httpx
import pytest

from queen import FilterUnsupportedError, Queen

from ..conflation_unit.pop_transport import PopTransport, message, pop_body
from ..kv_unit.plan_server import PlanServer

CONSUME_TIMEOUT_S = 5.0


def filtered(messages: Optional[List[Dict[str, Any]]] = None, **kw: Any) -> Dict[str, Any]:
    """A pop answer of a filtered group, as the broker sends it: with the echo."""
    return {**pop_body(messages, **kw), "filtered": True}


def make(*pop_plan: Any) -> Tuple[Queen, PopTransport]:
    transport = PopTransport(*pop_plan, default_pop=filtered())
    return Queen(url="http://plan.local", transport=transport, retry_attempts=1), transport


def conditions(raw: str) -> Any:
    return json.loads(raw)


async def test_pop_sends_the_conditions_anded_as_one_json_list():
    client, transport = make()
    try:
        await (
            client.queue("order-events").group("billing")
            .where("data.kind", "in", ["order.paid", "order.refunded"])
            .where("headers.source", "=", "web")
            .where("data.vip", "exists")
            .pop()
        )
    finally:
        await client.close()

    (popped,) = transport.pops
    assert popped.route == "GET /api/v1/pop/queue/order-events"
    assert popped.param("consumerGroup") == "billing"
    assert conditions(popped.param("where")) == [
        {"field": "data.kind", "op": "in", "value": ["order.paid", "order.refunded"]},
        {"field": "headers.source", "op": "=", "value": "web"},
        {"field": "data.vip", "op": "exists"},
    ]


async def test_pop_sends_one_string_as_it_is_and_on_the_partition_route_too():
    client, transport = make()
    try:
        await (
            client.queue("order-events").partition("order-1").group("billing")
            .where("data.kind in ('order.paid', 'order.refunded')")
            .pop()
        )
    finally:
        await client.close()

    (popped,) = transport.pops
    assert popped.path == "/api/v1/pop/queue/order-events/partition/order-1"
    assert popped.param("where") == "data.kind in ('order.paid', 'order.refunded')"


async def test_a_condition_may_be_a_dict_or_a_list_of_triples():
    client, transport = make()
    try:
        await (
            client.queue("q").group("g")
            .where({"field": "data.a", "op": "=", "value": 1})
            .where([("data.b", ">", 2), ["partition", "prefix", "eu-"]])
            .pop()
        )
    finally:
        await client.close()

    assert conditions(transport.pops[0].param("where")) == [
        {"field": "data.a", "op": "=", "value": 1},
        {"field": "data.b", "op": ">", "value": 2},
        {"field": "partition", "op": "prefix", "value": "eu-"},
    ]


async def test_a_pop_without_a_where_is_the_request_it_always_was_and_needs_no_echo():
    client, transport = make(pop_body([message("txn-1")]), filtered())
    try:
        plain_messages = await client.queue("orders").group("workers").batch(5).pop()
        await client.queue("orders").group("workers").batch(5).where("data.a", "=", 1).pop()
    finally:
        await client.close()

    assert [m["transactionId"] for m in plain_messages] == ["txn-1"]
    plain, with_where = transport.pops
    assert plain.param("where") is None
    # The where is the last parameter: everything before it is unchanged.
    assert with_where.raw_query.startswith(plain.raw_query + "&where=")


async def test_consume_sends_the_same_where_on_every_pop():
    handled = []

    async def handler(msg):
        handled.append(msg["transactionId"])

    client, transport = make(
        filtered([message("txn-1")], group="billing"),
        filtered([message("txn-2", lease_id="lease-2")], group="billing"),
    )
    try:
        await asyncio.wait_for(
            client.queue("order-events").group("billing").where("data.kind", "=", "order.paid")
            .batch(1).limit(2).consume(handler),
            CONSUME_TIMEOUT_S,
        )
    finally:
        await client.close()

    assert handled == ["txn-1", "txn-2"]
    assert len(transport.pops) == 2
    sent = {p.param("where") for p in transport.pops}
    assert len(sent) == 1
    assert conditions(sent.pop()) == [{"field": "data.kind", "op": "=", "value": "order.paid"}]


# ---- a broker that does not filter ---------------------------------------------------


async def test_pop_raises_when_the_answer_does_not_say_it_filtered_even_with_messages():
    client, transport = make(pop_body([message("txn-1")], group="billing"))
    try:
        with pytest.raises(FilterUnsupportedError, match="does not filter consumer groups; upgrade it"):
            await client.queue("order-events").group("billing").where("data.kind", "=", "x").pop()
    finally:
        await client.close()
    assert len(transport.pops) == 1


class _NoContent(httpx.AsyncBaseTransport):
    """Answers every pop as an older broker answers an empty one: a bodiless 204."""

    def __init__(self) -> None:
        self.pops = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0)
        self.pops += 1
        return httpx.Response(204)


async def test_a_bodiless_204_is_no_proof_either_from_pop_and_from_consume():
    transport = _NoContent()
    client = Queen(url="http://plan.local", transport=transport, retry_attempts=1)

    async def handler(_msg):
        raise AssertionError("no message is handled")

    try:
        with pytest.raises(FilterUnsupportedError):
            await client.queue("order-events").group("billing").where("data.kind", "=", "x").wait(False).pop()
        with pytest.raises(FilterUnsupportedError):
            await asyncio.wait_for(
                client.queue("order-events").group("billing").where("data.kind", "=", "x").wait(False).consume(handler),
                CONSUME_TIMEOUT_S,
            )
        # Without a where, an empty 204 is just no messages.
        assert await client.queue("order-events").group("billing").wait(False).pop() == []
    finally:
        await client.close()
    assert transport.pops == 3, "consume stopped on its first answer"


async def test_consume_stops_on_the_first_answer_without_the_echo_before_handling_anything():
    handled = []

    async def handler(msg):
        handled.append(msg)

    client, transport = make(pop_body([message("txn-1")], group="billing"))
    try:
        with pytest.raises(FilterUnsupportedError):
            await asyncio.wait_for(
                client.queue("order-events").group("billing").where("data.kind", "=", "x").consume(handler),
                CONSUME_TIMEOUT_S,
            )
    finally:
        await client.close()
    assert handled == [] and len(transport.pops) == 1


# ---- refused before or by the broker -----------------------------------------------------


@pytest.mark.parametrize(
    "build, match",
    [
        (lambda q: q.queue("orders").where("data.a", "=", 1), "name one with group"),
        (lambda q: q.queue().namespace("billing").group("g").where("data.a", "=", 1), "namespace/task pop"),
    ],
    ids=["no group", "a namespace pop"],
)
async def test_a_where_the_broker_would_refuse_or_ignore_raises_from_pop_and_consume(build, match):
    client, transport = make()

    async def handler(_msg):
        pass

    try:
        with pytest.raises(ValueError, match=match):
            await build(client).pop()
        with pytest.raises(ValueError, match=match):
            build(client).consume(handler)
    finally:
        await client.close()
    assert transport.pops == []


async def test_conditions_and_a_string_do_not_mix_and_a_bad_condition_raises_where_it_is_written():
    client, transport = make()
    try:
        with pytest.raises(ValueError, match="not both"):
            client.queue("q").group("g").where("data.a", "=", 1).where("data.b = 2")
        with pytest.raises(ValueError, match="not both"):
            client.queue("q").group("g").where("data.b = 2").where("data.a", "=", 1)
        with pytest.raises(ValueError, match="unknown op"):
            client.queue("q").group("g").where("data.a", "~", 1)
        with pytest.raises(TypeError):
            client.queue("q").group("g").where("data.a", "=", {1, 2})
        # Two strings are ANDed, as two conditions are.
        await client.queue("q").group("g").where("data.a = 1").where("headers.type = 'x'").pop()
    finally:
        await client.close()
    assert transport.pops[0].param("where") == "data.a = 1 and headers.type = 'x'"


class _Mismatch(httpx.AsyncBaseTransport):
    """Answers every pop as the broker answers a group's other where."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "error": "group billing on order-events filters with where \"data.kind = 'x'\"; change it with "
                "POST /api/v1/consumer-groups/billing/subscription, or use another group",
                "code": "where_mismatch",
            },
        )


async def test_a_refused_where_raises_from_pop_instead_of_reading_as_no_messages():
    client = Queen(url="http://plan.local", transport=_Mismatch(), retry_attempts=1)
    try:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await client.queue("order-events").group("billing").where("data.kind", "=", "y").pop()
        assert getattr(caught.value, "code", None) == "where_mismatch"
        # Without a where, a 400 keeps reading as [] (this method's contract).
        assert await client.queue("order-events").group("billing").pop() == []
    finally:
        await client.close()


async def test_admin_changes_a_groups_where_on_the_subscription_route():
    server = PlanServer()
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    try:
        await client.admin.update_consumer_group_where("billing", "order-events", "data.kind = 'order.paid'")
        await client.admin.update_consumer_group_where("billing", "order-events", [("data.total", ">", 100)])
    finally:
        await client.close()

    text, listed = server.requests
    assert text.route == "POST /api/v1/consumer-groups/billing/subscription"
    assert text.body == {"queue": "order-events", "where": "data.kind = 'order.paid'"}
    assert listed.body == {"queue": "order-events", "where": [{"field": "data.total", "op": ">", "value": 100}]}
