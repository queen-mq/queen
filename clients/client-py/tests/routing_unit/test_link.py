"""
``link`` in transactions (PLAN_ROUTING.md §7): the transaction push step's
sibling. One ``{"type": "link", queue, partition, from, headers}`` operation per
message; ``from`` names a popped message by its partitionId (with the group it
was popped with) and a feed entry, whose partitionId is a UUID, by its queue
and partition.
"""

from __future__ import annotations

from typing import Any, Tuple

import pytest

from queen import Queen
from queen.builders.transaction_builder import link_source

from ..kv_unit.plan_server import PlanServer

POPPED = {
    "id": "0190aaaa-0000-7000-8000-000000000001",
    "transactionId": "order-9137-paid",
    "partitionId": "123",
    "partition": "order-9137",
    "leaseId": "lease-1",
    "consumerGroup": "billing",
    "offset": 42,
    "data": {"orderId": 9137, "total": 120},
}

FEED_ENTRY = {
    "position": 88,
    "queue": "orders",
    "partition": "order-9137",
    "partitionId": "0190bbbb-0000-7000-8000-00000000000a",
    "offset": 42,
    "id": "0190aaaa-0000-7000-8000-000000000001",
    "transactionId": "order-9137-paid",
    "data": {"orderId": 9137},
}


def make(*plan: Any) -> Tuple[Queen, PlanServer]:
    server = PlanServer(*plan)
    return Queen(url="http://plan.local", transport=server, retry_attempts=1), server


async def test_a_link_op_per_message_beside_the_ack_naming_the_message_as_the_pop_returned_it():
    client, server = make()
    try:
        await (
            client.transaction()
            .ack(POPPED, context={"consumer_group": "billing"})
            .queue("manual-review").partition("order-9137")
            .link([POPPED], headers={"review-reason": "amount"})
            .commit()
        )
    finally:
        await client.close()

    ack, link = server.only.body["operations"]
    assert ack["type"] == "ack"
    assert link == {
        "type": "link",
        "queue": "manual-review",
        "partition": "order-9137",
        "from": {"partitionId": "123", "offset": 42, "transactionId": "order-9137-paid", "consumerGroup": "billing"},
        "headers": {"review-reason": "amount"},
    }


async def test_a_feed_entry_is_named_by_queue_and_partition_and_no_partition_is_the_brokers_default():
    client, server = make()
    try:
        await client.transaction().queue("audit").link(FEED_ENTRY).commit()
    finally:
        await client.close()

    (link,) = server.only.body["operations"]
    assert link == {
        "type": "link",
        "queue": "audit",
        "from": {"queue": "orders", "partition": "order-9137", "offset": 42, "transactionId": "order-9137-paid"},
    }


async def test_links_and_pushes_keep_the_order_they_were_added_in():
    client, server = make()
    try:
        await (
            client.transaction()
            .queue("a").partition("p").push([{"data": {"n": 1}}])
            .queue("b").partition("p").link([POPPED, {**POPPED, "transactionId": "t2", "offset": 43}])
            .queue("a").partition("p").push([{"data": {"n": 2}}])
            .commit()
        )
    finally:
        await client.close()

    ops = server.only.body["operations"]
    assert [(op["type"], op.get("queue")) for op in ops] == [("push", None), ("link", "b"), ("link", "b"), ("push", None)]
    assert [op["from"]["offset"] for op in ops if op["type"] == "link"] == [42, 43]


def test_a_queue_mode_message_and_a_numeric_partition_id_are_named_as_they_are():
    assert link_source({"transactionId": "t", "partitionId": 7, "offset": 0}) == {
        "partitionId": "7", "offset": 0, "transactionId": "t",
    }
    assert link_source({**POPPED, "consumerGroup": "__QUEUE_MODE__"})["consumerGroup"] == "__QUEUE_MODE__"


@pytest.mark.parametrize(
    "msg, match",
    [
        ({**POPPED, "offset": None}, "its offset"),
        ({**POPPED, "offset": True}, "its offset"),
        ({**POPPED, "transactionId": ""}, "its transactionId"),
        ({"transactionId": "t", "partitionId": "part-1", "offset": 1}, "partitionId a pop returned"),
        ("order-9137-paid", "the messages a pop or the feed returned"),
    ],
)
async def test_a_message_that_does_not_name_its_source_raises_before_any_request(msg, match):
    client, server = make()
    try:
        with pytest.raises(ValueError, match=match):
            client.transaction().queue("review").link([msg])
        with pytest.raises(ValueError, match="dict of strings"):
            client.transaction().queue("review").link([POPPED], headers=["review-reason"])
    finally:
        await client.close()
    assert server.requests == []


async def test_a_refused_link_raises_with_the_brokers_reason_and_a_linked_duplicate_commits():
    gone = {"status": 200, "json": {"success": False, "reason": "source_gone", "results": [],
                                    "error": "source_gone: offset 42 of orders is not retained"}}
    committed = {"status": 200, "json": {"success": True, "results": [
        {"index": 0, "type": "link", "success": True, "duplicate": True, "transactionId": "order-9137-paid"},
    ]}}
    client, _ = make(gone, committed)
    try:
        with pytest.raises(Exception) as caught:
            await client.transaction().queue("review").link([POPPED]).commit()
        assert getattr(caught.value, "reason", None) == "source_gone"
        res = await client.transaction().queue("review").link([POPPED]).commit()
    finally:
        await client.close()
    assert res and res["results"][0]["duplicate"] is True
