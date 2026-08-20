"""
max_size is a BLOCKING bound (queen/buffer/message_buffer.py).

The defect these cover, measured on the Go client on 2026-08-20: with an
unbounded buffer a producer filling at 1.46M msg/s against a 1.0M msg/s flush
pipeline accumulated 20.9M messages (11.7 GB RSS) in 45 seconds, reported
success for all of them, and lost every one at process exit. The fix is that
the add path parks on an asyncio.Condition once the buffer holds max_size
messages, so the producer's throughput degrades to the drain's instead of
lying, and so a producer that is cancelled while parked hears about it.

No broker and no database: the flush pipeline is a FakeSink.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from queen.buffer.buffer_manager import BufferManager
from queen.errors import QueenError

from .fake_sink import FakeSink, item, settle

ADDRESS = "test-buffer/Default"

# message_count 2 / max_size 4 keeps the arithmetic small enough to reason
# about by hand; time_millis is parked far away so nothing under test depends
# on the time-based flush firing.
SMALL = {"message_count": 2, "time_millis": 60000, "max_size": 4, "retry_delay_millis": 10}


async def produce(manager: BufferManager, count: int, options=SMALL, first: int = 0) -> int:
    """Add `count` messages, returning how many were accepted"""
    accepted = 0
    for i in range(first, first + count):
        await manager.add_message(ADDRESS, item(i), options)
        accepted += 1
    return accepted


def test_add_message_is_a_coroutine_function():
    """The blocking contract only exists if callers await it.

    A refactor that made add_message synchronous again would not fail any
    behavioural test -- it would just stop blocking, silently. This asserts the
    shape directly.
    """
    assert inspect.iscoroutinefunction(BufferManager.add_message)


async def test_add_blocks_at_the_bound_and_resumes_when_the_flusher_drains():
    sink = FakeSink()
    sink.close_gate()  # the first flush hangs, so nothing can drain
    manager = BufferManager(sink)

    producer = asyncio.create_task(produce(manager, 20))
    await settle()

    assert not producer.done(), "producer should be parked on the max_size bound"

    buffered = manager.get_stats()["totalBufferedMessages"]
    assert buffered <= SMALL["max_size"], f"occupancy {buffered} exceeded the bound"
    assert sink.in_flight == 1, "exactly one flush should be in flight, holding one batch"

    # The real assertion of the bug: without backpressure all 20 would already
    # be sitting in memory. Buffered + in-flight is capped at bound + one batch.
    assert buffered + len(sink.attempts[0]) <= SMALL["max_size"] + SMALL["message_count"]

    sink.open_gate()
    assert await asyncio.wait_for(producer, timeout=5) == 20

    await manager.flush_all_buffers()
    assert sink.delivered_payloads == list(range(20)), "order must survive the backpressure park"
    assert manager.get_stats()["totalBufferedMessages"] == 0


async def test_cleanup_wakes_parked_adds():
    """Shutdown must never leave a parked producer hanging."""
    sink = FakeSink()
    sink.close_gate()
    manager = BufferManager(sink)

    producer = asyncio.create_task(produce(manager, 50))
    await settle()
    assert not producer.done(), "producer should be parked on the max_size bound"

    await asyncio.wait_for(manager.cleanup(), timeout=5)

    with pytest.raises(QueenError):
        # Woken, and told the truth: the message it was parked on was NOT
        # buffered, so it must not read as a successful push.
        await asyncio.wait_for(producer, timeout=5)

    assert manager.get_stats()["totalBufferedMessages"] == 0


async def fill_to_the_bound(manager: BufferManager, sink: FakeSink) -> int:
    """Drive the buffer to exactly max_size with one batch stuck in the sink.

    `add` never suspends while there is room, so the first max_size adds land
    before the drain task gets a turn. After settle() that drain has taken one
    batch out and is hanging on the closed gate; the capacity it freed is
    PROVISIONAL (the batch can still come back), which is why the wake is tied
    to a successful send and not to take_batch -- so we top the buffer back up
    to the bound by hand and the next add has to park.

    Returns the number of payloads added (0 .. n-1).
    """
    for i in range(SMALL["max_size"]):
        await manager.add_message(ADDRESS, item(i), SMALL)
    await settle()
    assert sink.in_flight == 1

    added = SMALL["max_size"]
    top_up = SMALL["max_size"] - manager.get_stats()["totalBufferedMessages"]
    for i in range(added, added + top_up):
        await manager.add_message(ADDRESS, item(i), SMALL)
    assert manager.get_stats()["totalBufferedMessages"] == SMALL["max_size"]
    return added + top_up


async def test_parked_add_that_times_out_raises_and_does_not_report_success():
    sink = FakeSink()
    sink.close_gate()
    manager = BufferManager(sink)

    added = await fill_to_the_bound(manager, sink)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(manager.add_message(ADDRESS, item("timed-out"), SMALL), timeout=0.05)

    sink.open_gate()
    await manager.flush_all_buffers()

    payloads = sink.delivered_payloads
    assert "timed-out" not in payloads, "a timed-out add must not have buffered its message"
    assert payloads == list(range(added))


async def test_parked_add_that_is_cancelled_raises_and_does_not_report_success():
    sink = FakeSink()
    sink.close_gate()
    manager = BufferManager(sink)

    added = await fill_to_the_bound(manager, sink)

    blocked = asyncio.create_task(manager.add_message(ADDRESS, item("cancelled"), SMALL))
    await settle()
    assert not blocked.done(), "an add at the bound must park"

    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked

    sink.open_gate()
    await manager.flush_all_buffers()

    payloads = sink.delivered_payloads
    assert "cancelled" not in payloads
    assert payloads == list(range(added))
