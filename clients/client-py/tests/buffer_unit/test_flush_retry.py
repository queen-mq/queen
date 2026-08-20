"""
A failed flush batch is re-queued at the FRONT and retried, never dropped
(queen/buffer/buffer_manager.py::_drain).

The defect: the flusher took a batch OUT of the buffer before sending it, and
on an HTTP error logged and moved on -- up to message_count messages vanished
per failed POST, long after the caller had been told the push succeeded.

No broker and no database: the flush pipeline is a FakeSink that can be told
which attempts must fail.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from queen.buffer.buffer_manager import BufferManager

from .fake_sink import FakeSink, item, settle

ADDRESS = "test-buffer/Default"

RETRY_MILLIS = 40
BATCHED = {
    "message_count": 3,
    "time_millis": 60000,
    "max_size": 12,
    "retry_delay_millis": RETRY_MILLIS,
}


async def test_failed_batch_is_requeued_in_order_and_retried_after_the_delay():
    # Attempts 1 and 3 fail; each must be re-offered with the same messages in
    # the same order rather than skipped.
    sink = FakeSink(fail_attempts={1, 3})
    manager = BufferManager(sink)

    for i in range(9):
        await manager.add_message(ADDRESS, item(i), BATCHED)

    started = time.monotonic()
    await manager.flush_all_buffers()
    elapsed = time.monotonic() - started

    assert sink.attempt_payloads(0) == [0, 1, 2], "first batch"
    assert sink.attempt_payloads(1) == [0, 1, 2], "retry must resend the same batch, in order"
    assert sink.attempt_payloads(2) == [3, 4, 5]
    assert sink.attempt_payloads(3) == [3, 4, 5]
    assert sink.attempt_payloads(4) == [6, 7, 8]

    assert sink.delivered_payloads == list(range(9)), "nothing dropped, nothing reordered"
    assert manager.get_stats()["totalBufferedMessages"] == 0

    # Two failures, each paying retry_delay_millis before the batch is retried.
    assert elapsed >= 2 * RETRY_MILLIS / 1000.0 * 0.9, f"retries returned too fast ({elapsed:.3f}s)"


async def test_nothing_is_dropped_across_many_adds_with_intermittent_failures():
    total = 200
    options = {
        "message_count": 10,
        "time_millis": 60000,
        "max_size": 40,
        "retry_delay_millis": 1,
    }
    # Every third POST fails. The producer below is a plain loop, so it really
    # does park on the bound and get woken by the drain, failures included.
    sink = FakeSink(fail_predicate=lambda attempt: attempt % 3 == 0)
    manager = BufferManager(sink)

    for i in range(total):
        await manager.add_message(ADDRESS, item(i), options)

    await manager.flush_all_buffers()

    assert len(sink.delivered) == total, "exact parity: every add must reach the sink exactly once"
    assert sink.delivered_payloads == list(range(total)), "payload order must be the producer's"
    assert len(sink.attempts) > total // options["message_count"], "the failures must have retried"
    assert manager.get_stats()["totalBufferedMessages"] == 0


async def test_a_cancelled_flush_puts_its_batch_back():
    """Cancellation is not delivery.

    White-box on purpose: it reaches into the manager's pending-flush set,
    because the only way to observe the in-flight batch surviving cancellation
    is to cancel the drain while it holds one.
    """
    options = {"message_count": 2, "time_millis": 60000, "max_size": 4, "retry_delay_millis": 10}
    sink = FakeSink()
    sink.close_gate()
    manager = BufferManager(sink)

    for i in range(4):
        await manager.add_message(ADDRESS, item(i), options)
    await settle()

    buffer = manager._buffers[ADDRESS]
    assert sink.in_flight == 1
    assert buffer.message_count == 2, "one batch is out of the buffer, held by the gated sink"

    (drain,) = tuple(manager._pending_flushes)
    drain.cancel()
    with pytest.raises(asyncio.CancelledError):
        await drain

    assert buffer.message_count == 4, "the cancelled batch must be back at the front"

    sink.open_gate()
    await manager.flush_all_buffers()
    assert sink.delivered_payloads == [0, 1, 2, 3]
