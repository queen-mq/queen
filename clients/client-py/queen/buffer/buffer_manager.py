"""
Buffer manager for client-side message buffering across queues

Owns the registry of per-queue buffers and the drain loop that empties them
into the broker. The two properties that matter, both explained at length in
message_buffer.py:

  - producers BLOCK at the buffer's max_size bound instead of growing the
    process without limit, and
  - a batch that fails to POST goes back at the front of its buffer and is
    retried after retry_delay_millis; nothing is dropped on a failed flush.
"""

import asyncio
from typing import Any, Dict, List, Optional, Set

from ..errors import QueenError
from ..utils import logger
from .message_buffer import MessageBuffer
from .sinks import Destination


class BufferManager:
    """Buffer manager for client-side message buffering across queues"""

    def __init__(self, http_client: Any):  # Type: HttpClient but avoiding circular import
        """
        Initialize buffer manager

        Args:
            http_client: HttpClient instance
        """
        self._http_client = http_client
        self._buffers: Dict[str, MessageBuffer] = {}  # queueAddress -> MessageBuffer
        self._pending_flushes: Set["asyncio.Task[None]"] = set()
        self._flush_count = 0
        self._stopped = False
        # Guards MUTATION of the _buffers registry only, and is never held
        # across an await -- not the network, and above all not a buffer's
        # backpressure gate. Holding it across a blocking add() would deadlock
        # the client outright: the add cannot return until a drain frees
        # capacity, and every other producer (and every other queue) would be
        # queued behind the same lock in the meantime.
        self._lock = asyncio.Lock()

    async def add_message(
        self,
        queue_address: str,
        formatted_message: Dict[str, Any],
        buffer_options: Dict[str, Any],
        destination: Optional[Destination] = None,
    ) -> None:
        """
        Add message to buffer, BLOCKING while that buffer is at its max_size.

        Awaitable, and every caller must await it: a dropped coroutine here is a
        silent no-push, which is the same class of lie as the unbounded buffer
        this replaced.

        Args:
            queue_address: Queue address (queue/partition)
            formatted_message: Formatted message dict
            buffer_options: Buffer options (resolved by MessageBuffer; the
                options of the FIRST push to an address configure the buffer for
                its lifetime)
            destination: Where this address drains to (buffer/sinks.py), read
                ONLY when the buffer is created -- like the options above, the
                first push to an address fixes it for the buffer's lifetime.
                None means the durable push, which is what every caller that
                predates ephemeral queues passes.

        Raises:
            QueenError: if the client has been closed
            asyncio.CancelledError: if the caller cancels while blocked
        """
        async with self._lock:
            # A push after cleanup() would otherwise create a fresh buffer that
            # nothing will ever drain: messages accepted into a client that is
            # already closed, which is the same false success as the unbounded
            # buffer. cleanup() only runs from close(), so this cannot fire on a
            # live client.
            if self._stopped:
                raise QueenError(
                    f"client is closed: message not buffered for {queue_address}"
                )

            buffer = self._buffers.get(queue_address)
            if buffer is None:
                buffer = MessageBuffer(
                    queue_address, buffer_options, self._schedule_flush, destination
                )
                self._buffers[queue_address] = buffer
                logger.log(
                    "BufferManager.createBuffer",
                    {
                        "queue_address": queue_address,
                        "options": buffer.options,
                        "sink": buffer.destination.sink.name,
                    },
                )

        # Outside the registry lock, deliberately: this call can park.
        await buffer.add(formatted_message)
        logger.log(
            "BufferManager.addMessage",
            {"queue_address": queue_address, "message_count": buffer.message_count},
        )

    def _schedule_flush(self, queue_address: str) -> Optional["asyncio.Task[None]"]:
        """
        Schedule a drain (called from MessageBuffer's timer, count threshold and
        backpressure paths -- all of which are synchronous, so this must only
        create the task and never await it).

        Args:
            queue_address: Queue address to flush

        Returns:
            The drain task, or None if the address has no buffer
        """
        buffer = self._buffers.get(queue_address)
        if buffer is None:
            return None

        task = asyncio.create_task(self._drain(queue_address, buffer))
        # Keep a strong reference until it finishes. asyncio holds only a weak
        # reference to a running task, so a fire-and-forget flush can be
        # garbage collected mid-POST; _pending_flushes is also what
        # flush_buffer/close wait on.
        self._pending_flushes.add(task)
        task.add_done_callback(self._pending_flushes.discard)
        return task

    async def _drain(self, queue_address: str, buffer: MessageBuffer) -> None:
        """
        Drain a buffer to the broker, batch by batch, until it is empty.

        One drain per buffer at a time (begin_flush is the claim). A batch that
        fails to POST is re-queued at the front and retried after the buffer's
        retry_delay_millis, for as long as the buffer is alive and this task is
        not cancelled: an unreachable broker therefore shows up as blocked
        producers and a bounded buffer, never as messages that quietly
        disappeared.

        Args:
            queue_address: Queue address
            buffer: The buffer to drain
        """
        if not buffer.begin_flush():
            return

        # WHERE a batch goes is the buffer's own destination (buffer/sinks.py),
        # not a constant in this loop: everything below -- the claim, the
        # re-queue at the front, the retry, the wake -- is about ordering,
        # occupancy and loss, and none of it knows or needs to know which route
        # the batch lands on.
        sink, dest_queue, dest_partition = buffer.destination

        try:
            while True:
                batch = buffer.take_batch()
                if not batch:
                    return

                try:
                    await self._http_client.post(
                        sink.path, sink.format(dest_queue, dest_partition, batch)
                    )
                except asyncio.CancelledError:
                    # Cancellation is not delivery. Put the batch back before
                    # unwinding, or a cancelled shutdown loses precisely what
                    # the re-queue path exists to protect.
                    buffer.requeue_front(batch)
                    raise
                except Exception as error:
                    logger.error(
                        "BufferManager.flushBuffer",
                        {
                            "queue_address": queue_address,
                            "count": len(batch),
                            "error": str(error),
                            "action": "requeued",
                        },
                    )
                    buffer.requeue_front(batch)
                    if buffer.is_stopped:
                        return
                    # Sleep OUTSIDE any lock, then retake from the front: the
                    # re-queued batch is at the head, so the retry sends the
                    # same messages in the same order.
                    await asyncio.sleep(buffer.retry_delay_seconds)
                    continue

                self._flush_count += 1
                logger.log(
                    "BufferManager.flushBuffer",
                    {
                        "queue_address": queue_address,
                        "sink": sink.name,
                        "status": "success",
                        "messages_sent": len(batch),
                    },
                )
                # Capacity freed: wake the producers parked on max_size.
                await buffer.notify_drained()
        finally:
            await buffer.end_flush()

    async def flush_buffer(self, queue_address: str) -> None:
        """
        Flush all messages for a queue address

        Args:
            queue_address: Queue address to flush
        """
        logger.log(
            "BufferManager.flushBuffer",
            {
                "queue_address": queue_address,
                "active_buffers": len(self._buffers),
                "pending_flushes": len(self._pending_flushes),
            },
        )

        buffer = self._buffers.get(queue_address)
        if buffer is None:
            logger.log(
                "BufferManager.flushBuffer",
                {"queue_address": queue_address, "status": "not-found"},
            )
            await self._wait_for_pending_flushes()
            return

        buffer.cancel_timer()

        while True:
            # If a drain was already in flight, _drain returns immediately (it
            # cannot claim the buffer) and the wait below is what actually
            # blocks until that drain is done. Re-check afterwards, because
            # messages added while it was finishing are still ours to flush.
            await self._drain(queue_address, buffer)
            await self._wait_for_pending_flushes()
            if buffer.message_count == 0 or buffer.is_stopped:
                return

    async def flush_all_buffers(self) -> None:
        """
        Flush all buffers.

        With an unreachable broker this retries until it is cancelled -- the
        drain never gives up on a batch, since the alternative is the silent
        loss this module was rewritten to remove. Callers that need a bounded
        shutdown wrap it in asyncio.wait_for; Queen.close() does exactly that
        (CLOSE_FLUSH_TIMEOUT_SECONDS) and reports what was left unsent, so a
        dead broker ends a shutdown loudly instead of hanging it.
        """
        queue_addresses = list(self._buffers.keys())

        logger.log(
            "BufferManager.flushAllBuffers",
            {"buffer_count": len(queue_addresses), "pending_flushes": len(self._pending_flushes)},
        )

        for queue_address in queue_addresses:
            await self.flush_buffer(queue_address)

        logger.log("BufferManager.flushAllBuffers", {"status": "completed"})

    async def _wait_for_pending_flushes(self) -> None:
        """Wait for all pending flush tasks"""
        pending = list(self._pending_flushes)
        if not pending:
            return
        await asyncio.gather(*pending, return_exceptions=True)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get buffer statistics

        Returns:
            Dict with buffer stats
        """
        total_buffered_messages = 0
        oldest_buffer_age = 0.0

        for buffer in self._buffers.values():
            total_buffered_messages += buffer.message_count
            age = buffer.first_message_age
            oldest_buffer_age = max(oldest_buffer_age, age)

        stats = {
            "activeBuffers": len(self._buffers),
            "totalBufferedMessages": total_buffered_messages,
            "oldestBufferAge": oldest_buffer_age,
            "flushesPerformed": self._flush_count,
        }

        logger.log("BufferManager.getStats", stats)
        return stats

    async def cleanup(self) -> None:
        """
        Stop and discard every buffer.

        Awaitable because stopping a buffer has to WAKE the producers parked on
        its bound (asyncio.Condition.notify_all needs the lock, so the wake must
        come from a coroutine). Shutdown that left a parked add hanging would
        turn a bounded buffer into a hung client.
        """
        logger.log("BufferManager.cleanup", {"buffer_count": len(self._buffers)})
        self._stopped = True

        buffers: List[MessageBuffer] = list(self._buffers.values())
        self._buffers.clear()
        for buffer in buffers:
            await buffer.cleanup()

        # Stopped buffers make the drain loops unwind at their next check, but a
        # drain sitting in a POST (or in its retry sleep) would outlive the
        # client; cancel them and let the CancelledError handler in _drain put
        # its batch back into a buffer that is being discarded anyway.
        pending = list(self._pending_flushes)
        self._pending_flushes.clear()
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
