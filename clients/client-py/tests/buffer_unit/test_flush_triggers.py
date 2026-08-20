"""
The two flush triggers still fire after the rewrite.

Backpressure changed how a drain is REQUESTED (at most one outstanding request
per buffer, so a producer that never yields cannot spawn a drain task per
message), so the count threshold and the time-based timer are worth asserting
directly: the timer in particular is what the buffered e2e test relies on, and
that one needs a broker, so it cannot catch a regression here.
"""

from __future__ import annotations

import asyncio

from queen.buffer.buffer_manager import BufferManager

from .fake_sink import FakeSink, item, settle

ADDRESS = "test-buffer/Default"


async def test_count_threshold_triggers_a_flush():
    sink = FakeSink()
    manager = BufferManager(sink)
    options = {"message_count": 3, "time_millis": 60000, "max_size": 12}

    for i in range(3):
        await manager.add_message(ADDRESS, item(i), options)
    await settle()

    assert sink.delivered_payloads == [0, 1, 2], "crossing message_count must flush"
    assert manager.get_stats()["totalBufferedMessages"] == 0


async def test_timer_triggers_a_flush_below_the_threshold():
    sink = FakeSink()
    manager = BufferManager(sink)
    options = {"message_count": 100, "time_millis": 30, "max_size": 400}

    await manager.add_message(ADDRESS, item("lonely"), options)
    assert sink.delivered_payloads == [], "one message is far below the threshold"

    await asyncio.sleep(0.12)
    assert sink.delivered_payloads == ["lonely"], "the time-based flush must still fire"
    assert manager.get_stats()["flushesPerformed"] == 1


async def test_one_drain_is_requested_per_burst():
    """A tight add loop must not schedule one drain task per message.

    `add` does not suspend while there is room, so every add past the threshold
    would otherwise create a task that immediately no-ops.
    """
    sink = FakeSink()
    manager = BufferManager(sink)
    options = {"message_count": 2, "time_millis": 60000, "max_size": 100}

    for i in range(20):
        await manager.add_message(ADDRESS, item(i), options)

    assert len(manager._pending_flushes) == 1, "one outstanding drain request for the whole burst"

    await manager.flush_all_buffers()
    assert sink.delivered_payloads == list(range(20))
