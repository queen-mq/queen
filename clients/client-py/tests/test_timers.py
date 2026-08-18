"""
Timer integration tests -- against a real broker (PLAN_KV_TIMERS.md §4).

Every queue here starts with ``test-`` so ``cleanup_test_data`` purges both the
queue and the pending rows of ``queen.log_timers``; the timer table has no
foreign key to the queue, so it would otherwise survive the queue that gave it
its name and re-fire into the next run.

``deliverAt`` IS "NOT BEFORE": every wait below polls with a deadline rather
than sleeping for the delay and asserting once.
"""

import asyncio
import time

import pytest

from queen.errors import TimerError

Q = "test-timer-py"


#: Why this is 45 s and not 2 s, measured on 1.0.2 with the sweeper's own
#: defaults: a timer that FIRES is not immediately POPPABLE. The fire commits
#: the frame, but nothing tells the hot-list ring about it, so the message is
#: only served after the next reseed -- 30 s at QUEEN_HOTLIST_RESEED_MS's
#: default, and up to QUEEN_HOTLIST_RESEED_WINDOW_MS elsewhere. A plain push to
#: the same queue is poppable in 0 s, so the gap is specific to the fire path.
#: Measured: schedule(delay=500ms) -> `sweeper: swept fired=1` within a second
#: -> first successful pop at +30 s, repeatably.
#:
#: The deadline is generous rather than tight so this suite reports the CLIENT,
#: which is what it is for. If the fire path ever notifies the ring, these tests
#: get faster and none of them changes.
_DELIVERY_TIMEOUT_S = 45.0


async def pop_until(client, queue, *, timeout_s=_DELIVERY_TIMEOUT_S, group=None):
    """Poll a queue until one message shows up or the deadline passes."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        builder = client.queue(queue).batch(10).wait(False)
        if group:
            builder = builder.group(group)
        messages = await builder.pop()
        if messages:
            return messages
        await asyncio.sleep(0.2)
    return []


@pytest.mark.asyncio
async def test_a_scheduled_timer_is_delivered_as_a_message(client):
    queue = f"{Q}-fire"
    await client.queue(queue).create()

    res = await client.timers.schedule(
        queue, "fire:1", {"orderId": "9f1"}, delay_ms=500
    )
    assert res["status"] == "scheduled"
    assert res["ok"] is True
    # The message id is promised AT SCHEDULE TIME so the delivered frame can be
    # correlated without a second API call.
    assert res["messageId"]
    assert res["deliverAt"]

    messages = await pop_until(client, queue)
    assert messages, "the timer never fired"
    assert messages[0]["data"] == {"orderId": "9f1"}
    # The txn the SDK minted is the one in the log, which is what makes the
    # `absent` contract of §4.4 checkable at all.
    assert messages[0]["transactionId"] == res["txn"]


@pytest.mark.asyncio
async def test_cancel_before_the_fire_removes_it(client):
    queue = f"{Q}-cancel"
    await client.queue(queue).create()

    scheduled = await client.timers.schedule(queue, "cancel:1", {"v": 1}, delay_ms=30_000)
    peeked = await client.timers.peek(queue, "cancel:1")
    assert peeked["found"] is True

    cancelled = await client.timers.cancel(queue, "cancel:1", txn=scheduled["txn"])
    assert cancelled["status"] == "cancelled"
    assert cancelled["ok"] is True
    assert cancelled["txn"] == scheduled["txn"]

    assert (await client.timers.peek(queue, "cancel:1"))["found"] is False
    assert await pop_until(client, queue, timeout_s=2.0) == []


@pytest.mark.asyncio
async def test_cancelling_something_that_is_not_pending_is_absent_and_not_ok(client):
    """§4.4, THE PLACE A USER GETS HURT. There is no tombstone: `absent` means
    "no longer pending" and MAY MEAN ALREADY DELIVERED. It carries ok:false --
    the in-house lesson being queue delete, where `deleted:false` with a 200 read
    as success to every client that trusted the field -- and it echoes the txn
    back so the authority (the log) can be consulted with no second API."""
    res = await client.timers.cancel(f"{Q}-absent", "never:scheduled", txn="my-txn")
    assert res["status"] == "absent"
    assert res["ok"] is False
    assert not res
    assert res["txn"] == "my-txn"


@pytest.mark.asyncio
async def test_reschedule_moves_the_delivery_and_keeps_the_key(client):
    queue = f"{Q}-resched"
    await client.queue(queue).create()

    first = await client.timers.schedule(queue, "resched:1", {"v": 1}, delay_ms=60_000)
    assert first["status"] == "scheduled"

    again = await client.timers.reschedule(queue, "resched:1", {"v": 2}, delay_ms=500)
    assert again["status"] == "rescheduled"
    assert again["deliverAt"] < first["deliverAt"]

    messages = await pop_until(client, queue)
    assert messages and messages[0]["data"] == {"v": 2}, "the reschedule delivered the OLD payload"


@pytest.mark.asyncio
async def test_a_delay_in_the_past_fires_on_the_first_cycle(client):
    """§4.2: a delivery time in the past is LEGAL, explicitly.

    Asserted on the timer ROW disappearing rather than on the message arriving:
    there is no tombstone (§4.4), so the row going away IS the fire, and it is
    observable as soon as the fire commits -- without waiting out the reseed
    described above."""
    queue = f"{Q}-past"
    await client.queue(queue).create()
    await client.timers.schedule(queue, "past:1", {"late": True}, delay_ms=-10_000)

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if not (await client.timers.peek(queue, "past:1"))["found"]:
            return
        await asyncio.sleep(0.2)
    pytest.fail("a timer with a delivery time in the past never fired")


@pytest.mark.asyncio
async def test_a_second_schedule_on_the_same_key_is_an_upsert(client):
    """§4.1: schedule and reschedule are the SAME upsert, which is what makes a
    client retry after a crash safe by construction."""
    queue = f"{Q}-upsert"
    await client.queue(queue).create()
    await client.timers.schedule(queue, "upsert:1", {"v": 1}, delay_ms=60_000)
    second = await client.timers.schedule(queue, "upsert:1", {"v": 2}, delay_ms=60_000)
    assert second["status"] == "rescheduled"

    listing = await client.timers.list(queue)
    assert [row["timerKey"] for row in listing["rows"]] == ["upsert:1"]


@pytest.mark.asyncio
async def test_list_is_keyset_paged_within_one_queue(client):
    queue = f"{Q}-list"
    await client.queue(queue).create()
    for i in range(5):
        await client.timers.schedule(queue, f"list:{i}", {"i": i}, delay_ms=120_000)

    page = await client.timers.list(queue, limit=2)
    assert len(page["rows"]) == 2
    assert page["truncated"] is True

    rows = await client.timers.list_all(queue, limit=2)
    assert [r["timerKey"] for r in rows] == [f"list:{i}" for i in range(5)]


@pytest.mark.asyncio
async def test_the_builder_schedules_and_cancels(client):
    queue = f"{Q}-builder"
    await client.queue(queue).create()

    scheduled = await (
        client.timer(queue).key("builder:1").payload({"v": 1}).after_ms(60_000).schedule()
    )
    assert scheduled["ok"] is True

    cancelled = await client.timer(queue).key("builder:1").cancel()
    assert cancelled["status"] == "cancelled"


@pytest.mark.asyncio
async def test_a_batch_is_index_aligned(client):
    queue = f"{Q}-batch"
    await client.queue(queue).create()
    results = await client.timers.batch(
        [
            client.timers.op.schedule(queue, "batch:a", {"a": 1}, delay_ms=60_000),
            client.timers.op.schedule(queue, "batch:b", {"b": 2}, delay_ms=60_000),
        ]
    )
    assert [r["timerKey"] for r in results] == ["batch:a", "batch:b"]


@pytest.mark.asyncio
async def test_one_key_may_appear_at_most_once_per_call(client):
    """The rule that makes the intra-space lock order total (§2.2). The broker
    raises 22023 on it, and the SDK surfaces it as a named 400."""
    queue = f"{Q}-dup"
    with pytest.raises(TimerError) as ei:
        await client.timers.batch(
            [
                client.timers.op.schedule(queue, "dup:1", {}, delay_ms=1000),
                client.timers.op.schedule(queue, "dup:1", {}, delay_ms=2000),
            ]
        )
    assert ei.value.status == 400
    assert ei.value.code == "timers_bad_request"
