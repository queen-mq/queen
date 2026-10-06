"""
pop() raises the broker's 400 refusal of a conflating pop.

pop() turns a failure into an empty list on purpose (its swallow-to-[] contract,
the same in the JS client), with one exception for conflation: a consumer that
asked for last-value delivery and is not getting it must be told
(PLAN_CONFLATION §4). The broker refuses a conflating pop with 400 when it names
no consumer group, or commits at delivery as well (autoAck) (server/src/handlers/data.rs,
conflation_refusal). Both are configuration faults that no retry can fix, and
the JS client raises them; this one returned [] for every pop.
"""

from __future__ import annotations

import httpx
import pytest

from queen import Queen

from ..kv_unit.plan_server import PlanServer

NO_GROUP = (
    "conflation requires consumerGroup: queue mode is a shared cursor with no group "
    "identity to hang a delivery policy on"
)


def make(*plan):
    server = PlanServer(*plan)
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    return client, server


def refusal(error: str):
    return {"status": 400, "json": {"success": False, "error": error, "messages": []}}


@pytest.mark.asyncio
async def test_a_conflating_pop_refused_with_400_raises():
    client, server = make(refusal(NO_GROUP))
    try:
        with pytest.raises(httpx.HTTPStatusError) as raised:
            await client.queue("orders").conflation().wait(False).pop()
    finally:
        await client.close()

    assert raised.value.response.status_code == 400
    assert "requires consumerGroup" in str(raised.value)
    assert server.only.query["conflation"] == ["true"]


@pytest.mark.asyncio
async def test_a_conflating_pop_that_commits_on_delivery_refused_with_400_raises():
    client, server = make(refusal("conflation cannot be combined with autoAck"))
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await client.queue("orders").group("workers").conflation().commit_on_delivery().wait(
                False
            ).pop()
    finally:
        await client.close()

    assert server.only.query["autoAck"] == ["true"]


@pytest.mark.asyncio
async def test_a_400_on_a_pop_that_does_not_conflate_is_still_an_empty_list():
    """The exception is for conflation only; the rest of the contract stays."""
    client, _ = make(refusal("some other refusal"))
    try:
        assert await client.queue("orders").group("workers").wait(False).pop() == []
    finally:
        await client.close()
