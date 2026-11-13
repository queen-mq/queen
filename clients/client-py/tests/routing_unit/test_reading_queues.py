"""
Reading queues (PLAN_ROUTING.md §5): ``config({"from": ...}).create()``.

This SDK sends every option config() was not given with its default
(QUEUE_DEFAULTS), and the broker refuses the options a reading queue takes
from its sources (400 ``reading_queue``). What is at stake: a reading queue's
configure must leave those defaults out -- and only the defaults: one the
caller set travels, for the broker to refuse in its own words -- while every
other configure is the request it always was.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

import pytest

from queen import Queen, ReadingQueueUnsupportedError
from queen.builders.queue_builder import READER_KEEPS
from queen.utils.defaults import QUEUE_DEFAULTS

from ..kv_unit.plan_server import PlanServer, Recorded

# The defaults this SDK sends that a reading queue refuses.
KEEP_DEFAULTS = {"maxSize", "retentionSeconds", "completedRetentionSeconds", "encryptionEnabled"}


def configured(rec: Recorded) -> Dict[str, Any]:
    """The broker's configure answer: a reading queue's carries its route."""
    body = rec.body if isinstance(rec.body, dict) else {}
    options = body.get("options") or {}
    answer: Dict[str, Any] = {"success": True}
    if options.get("from") is not None:
        answer["route"] = {"from": [{"queue": "orders", "where": None, "conditions": []}], "partitionBy": "same",
                           "catchingUp": options.get("start") == "all"}
        answer["catchingUp"] = options.get("start") == "all"
    return {"status": 200, "json": answer}


def make(default: Any = configured) -> Tuple[Queen, PlanServer]:
    server = PlanServer(default=default)
    return Queen(url="http://plan.local", transport=server, retry_attempts=1), server


def test_the_defaults_a_reading_queue_refuses_are_the_ones_named():
    camel = {k.split("_")[0] + "".join(w.capitalize() for w in k.split("_")[1:]) for k in QUEUE_DEFAULTS}
    assert camel & set(READER_KEEPS) == KEEP_DEFAULTS


async def test_a_reading_queue_sends_its_route_options_and_no_retention_default():
    client, server = make()
    try:
        await client.queue("orders-it").config({
            "from": "orders",
            "where": "data.country = 'IT'",
            "partition_by": "same",
            "start": "all",
            "lease_time": 30,
            "retry_limit": 5,
        }).create()
    finally:
        await client.close()

    body = server.only.body
    assert server.only.route == "POST /api/v1/configure"
    assert body["queue"] == "orders-it"
    options = body["options"]
    assert options["from"] == "orders"
    assert options["where"] == "data.country = 'IT'"
    assert options["partitionBy"] == "same"
    assert options["start"] == "all"
    assert (options["leaseTime"], options["retryLimit"]) == (30, 5)
    assert (options["priority"], options["delayedProcessing"], options["windowBuffer"]) == (0, 0, 0), "the other defaults stay"
    assert not KEEP_DEFAULTS & set(options)


async def test_conditions_are_sent_as_the_list_the_broker_reads_and_each_source_may_have_its_own():
    client, server = make()
    try:
        await client.queue("billing-in").config({
            "from_": ["orders", {"queue": "refunds", "where": ("data.total", ">", 100)}],
            "where": [("data.kind", "in", ("order.paid", "order.refunded")), {"field": "headers.source", "op": "=", "value": "web"}],
            "partitionBy": "headers.customer",
        }).create()
    finally:
        await client.close()

    options = server.only.body["options"]
    assert options["from"] == ["orders", {"queue": "refunds", "where": [{"field": "data.total", "op": ">", "value": 100}]}]
    assert options["where"] == [
        {"field": "data.kind", "op": "in", "value": ["order.paid", "order.refunded"]},
        {"field": "headers.source", "op": "=", "value": "web"},
    ]
    assert options["partitionBy"] == "headers.customer"
    assert not KEEP_DEFAULTS & set(options)


async def test_a_keep_option_the_caller_set_travels_for_the_broker_to_refuse():
    client, server = make()
    try:
        await client.queue("orders-it").config({"from": "orders", "retention_seconds": 60}).create()
    finally:
        await client.close()

    options = server.only.body["options"]
    assert options["retentionSeconds"] == 60
    assert not (KEEP_DEFAULTS - {"retentionSeconds"}) & set(options)


async def test_an_ordinary_queue_and_from_none_send_every_default_as_before():
    client, server = make()
    try:
        await client.queue("orders").config({"lease_time": 30}).create()
        await client.queue("orders-it").config({"from": None}).create()
    finally:
        await client.close()

    ordinary, stopped = server.requests
    assert set(ordinary.body["options"]) == {
        k.split("_")[0] + "".join(w.capitalize() for w in k.split("_")[1:]) for k in QUEUE_DEFAULTS
    }
    assert stopped.body["options"]["from"] is None
    assert KEEP_DEFAULTS <= set(stopped.body["options"]), "no longer a reader: an ordinary queue's configure"


async def test_delete_with_force_deletes_a_queue_that_reading_queues_read():
    client, server = make()
    try:
        await client.queue("orders").delete()
        await client.queue("orders").delete(force=True)
    finally:
        await client.close()

    plain, forced = server.requests
    assert (plain.route, plain.query_string) == ("DELETE /api/v1/resources/queues/orders", "")
    assert (forced.route, forced.query_string) == ("DELETE /api/v1/resources/queues/orders", "force=true")


async def test_a_broker_that_answers_a_reading_queue_without_its_route_does_not_read_queues():
    client, server = make(default={"status": 200, "json": {"success": True}})
    heard = []

    async def on_error(err):
        heard.append(err)

    try:
        with pytest.raises(ReadingQueueUnsupportedError, match="does not read other queues; upgrade it"):
            await client.queue("orders-it").config({"from": "orders"}).create()
        answer = await client.queue("orders-it").config({"from": "orders"}).create().on_error(on_error)
        # Nothing to prove for an ordinary queue, or one that stops reading.
        await client.queue("orders").config({"lease_time": 30}).create()
        await client.queue("orders-it").config({"from": None}).create()
    finally:
        await client.close()

    assert answer["success"] is False
    assert len(heard) == 1 and isinstance(heard[0], ReadingQueueUnsupportedError)
    assert len(server.requests) == 4


async def test_a_reading_queues_answer_with_its_route_is_handed_back():
    client, _ = make()
    try:
        answer = await client.queue("orders-it").config({"from": "orders", "start": "all"}).create()
    finally:
        await client.close()
    assert answer["route"]["partitionBy"] == "same" and answer["catchingUp"] is True
