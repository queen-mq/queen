"""
Message buffer for a single queue

The buffer is BOUNDED and the add path BLOCKS at the bound. That is the whole
point of this module, so it is worth writing down why: an unbounded client-side
buffer does not degrade under load, it lies. It accepts every message at full
speed, reports success for all of them, grows the process until it dies and
takes everything unflushed with it. Measured on the Go client on 2026-08-20: a
producer filling at 1.46M msg/s against a 1.0M msg/s flush pipeline accumulated
20.9M messages (11.7 GB RSS) in 45 seconds and lost all of them at exit, with
zero client-side errors anywhere. The same shape with the bound in place
sustains 881,148 msg/s with exact send/receive parity and 71 MB RSS: the send
rate degrades to the drain rate, which is the honest behavior.

Two rules follow from that and both live here:

  1. `max_size` is a blocking bound, and "unbounded" is not expressible.
  2. A batch that fails to flush goes back to the FRONT of the buffer and is
     retried; it is never dropped. Ordering is the producer's ordering.

CHOICE OF PRIMITIVE: the blocking gate is an ``asyncio.Condition``. This client
is asyncio, so a parked producer must yield the single event-loop thread -- the
flusher that has to free the capacity runs on that same thread, so a spin loop
or a ``sleep()``-poll would either starve the drain outright or quantize every
producer's latency to the poll interval. ``Condition`` is the direct analogue of
the ``sync.Cond`` in the Go reference (clients/client-go/message_buffer.go): it
releases its lock while parked and re-acquires it on wake, and because a parked
``wait()`` is just an awaited future, ``asyncio.CancelledError`` and
``asyncio.wait_for`` reach the producer for free -- which is how the caller's
cancellation is honored while blocked, the job the Go version needs a ``ctx``
for.
"""

import asyncio
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional

from ..errors import QueenError
from ..utils.defaults import BUFFER_DEFAULTS


def resolve_buffer_options(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Resolve user buffer options against BUFFER_DEFAULTS and clamp them.

    The clamping is not cosmetic:

    - ``max_size`` of 0 (or absent) means the DEFAULT bound, never infinity.
      Unbounded is what lost 20.9M messages in the measurement above, so opting
      out of backpressure is deliberately not expressible in the config.
    - ``max_size`` is floored at ``message_count``. A caller who asks to batch
      1000 messages per POST but bounds the buffer at 10 has asked for a buffer
      that can never reach its own flush threshold; the flush threshold wins.

    Args:
        options: Raw user options (may be None or partial)

    Returns:
        A new dict carrying every buffer key, resolved and clamped
    """
    # The RAW options decide, not the merge: max_size is derived from the
    # caller's message_count when they did not pick one, so buffer({"message_count":
    # 1000}) gets a 4000 bound and not the 400 that BUFFER_DEFAULTS documents
    # for the default message_count of 100. Reading max_size out of the merged
    # dict would make "absent" unobservable and quietly cap every large batch
    # configuration at the default.
    raw: Dict[str, Any] = dict(options or {})

    message_count = int(raw.get("message_count") or 0)
    if message_count <= 0:
        message_count = BUFFER_DEFAULTS["message_count"]

    time_millis = int(raw.get("time_millis") or 0)
    if time_millis <= 0:
        time_millis = BUFFER_DEFAULTS["time_millis"]

    max_size = int(raw.get("max_size") or 0)
    if max_size <= 0:
        max_size = 4 * message_count
    if max_size < message_count:
        max_size = message_count

    retry_delay_millis = int(raw.get("retry_delay_millis") or 0)
    if retry_delay_millis <= 0:
        retry_delay_millis = BUFFER_DEFAULTS["retry_delay_millis"]

    resolved: Dict[str, Any] = {**BUFFER_DEFAULTS, **raw}
    resolved["message_count"] = message_count
    resolved["time_millis"] = time_millis
    resolved["max_size"] = max_size
    resolved["retry_delay_millis"] = retry_delay_millis
    return resolved


class MessageBuffer:
    """Message buffer for a single queue"""

    def __init__(
        self,
        queue_address: str,
        options: Optional[Dict[str, Any]],
        flush_callback: Callable[[str], Any],
    ):
        """
        Initialize message buffer

        Args:
            queue_address: Queue address (queue/partition)
            options: Buffer options (message_count, time_millis, max_size,
                retry_delay_millis) -- resolved and clamped on the way in
            flush_callback: Callback that SCHEDULES a drain for this address.
                It must not block: it is called while the condition's lock is
                held (see `add`), so it may only create the task, never await
                the flush.
        """
        self._queue_address = queue_address
        self._options = resolve_buffer_options(options)
        self._flush_callback = flush_callback

        # Read once: these are fixed for the buffer's lifetime and are on the
        # add path, which runs per message.
        self._flush_threshold: int = self._options["message_count"]
        self._max_size: int = self._options["max_size"]
        self._time_millis: int = self._options["time_millis"]
        self._retry_delay_millis: int = self._options["retry_delay_millis"]

        # deque, not list: the retry path re-queues a failed batch at the FRONT,
        # and popleft/extendleft are O(batch) where list slicing is O(buffer).
        self._messages: Deque[Dict[str, Any]] = deque()

        # The gate producers park on when the buffer is at max_size. Its lock is
        # here for the park/wake PROTOCOL (notify_all requires holding it), not
        # to protect `_messages` from concurrent mutation -- asyncio is
        # single-threaded, so any method below that does not await is already a
        # critical section. That distinction is why take_batch/requeue_front are
        # plain `def`: see their docstrings.
        self._cond = asyncio.Condition()

        self._timer: Optional[asyncio.TimerHandle] = None
        self._first_message_time: Optional[float] = None
        self._flushing = False
        # A drain has been SCHEDULED but has not started claiming yet. Without
        # this, a producer that never yields (a tight `await add(...)` loop does
        # not suspend while there is room) would create one drain task per
        # message between crossing the flush threshold and parking at the bound,
        # and all but one of them would immediately no-op.
        self._flush_requested = False
        self._stopped = False

    # ===========================
    # Producer side
    # ===========================

    async def add(self, formatted_message: Dict[str, Any]) -> None:
        """
        Add a message to the buffer, BLOCKING while the buffer is at max_size.

        The wait is cancel-safe: ``asyncio.Condition.wait()`` re-acquires the
        lock before re-raising ``CancelledError``, so the ``async with`` below
        releases it correctly on the way out and the exception reaches the
        caller instead of being swallowed. A cancelled or timed-out add has NOT
        buffered its message -- it parks before appending, never after -- so a
        caller that sees the exception can be certain nothing was accepted.

        Args:
            formatted_message: Formatted message dict

        Raises:
            QueenError: if the buffer has been stopped (client shutting down)
            asyncio.CancelledError: if the caller cancels while parked
        """
        async with self._cond:
            while len(self._messages) >= self._max_size and not self._stopped:
                # A parked producer means the producers outran the flusher, and
                # the time-based timer may still be most of a second away, so
                # make sure a drain is actually in flight before parking --
                # otherwise the wake that would free this producer has nothing
                # scheduled to produce it. The callback only creates a task,
                # which cannot start until wait() below releases the lock.
                self._request_flush()
                await self._cond.wait()

            if self._stopped:
                # Deviation from the Go reference, which returns nil here: in
                # Python a silent `return` from an awaited add is
                # indistinguishable from success, and "reported success while
                # dropping the message" is the exact family of bug this whole
                # module exists to remove. Adding to a closed client is an
                # error, and says so.
                raise QueenError(
                    f"buffer for {self._queue_address} is stopped (client closed); "
                    "message not buffered"
                )

            if not self._messages:
                self._first_message_time = time.time()
                self._start_timer()

            # The bound check and this append are in the same lock hold with no
            # await between them, so two producers can never both observe room
            # and both append: occupancy stays at or below max_size (the one
            # documented overshoot is a re-queued batch, see requeue_front).
            self._messages.append(formatted_message)
            should_flush = len(self._messages) >= self._flush_threshold

        if should_flush:
            self._request_flush()

    # ===========================
    # Flusher side
    # ===========================

    def _request_flush(self) -> None:
        """Ask for a drain, at most one outstanding at a time"""
        if self._flushing or self._flush_requested or self._stopped:
            return
        self._flush_requested = True
        self._flush_callback(self._queue_address)

    def begin_flush(self) -> bool:
        """
        Claim the exclusive right to drain this buffer.

        Returns:
            True if the caller now owns the drain, False if another drain is
            already running, the buffer is empty, or the buffer is stopped.
        """
        # The scheduled drain has arrived, whether or not it wins the claim.
        self._flush_requested = False
        if self._flushing or self._stopped or not self._messages:
            return False
        self._flushing = True
        # The timer's job is done: this drain loops until the buffer is empty.
        self.cancel_timer()
        return True

    async def end_flush(self) -> None:
        """Release the drain claim and wake anything parked on the bound."""
        self._flushing = False
        if not self._messages:
            self._first_message_time = None
        await self.notify_drained()

    def take_batch(self, batch_size: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Remove up to batch_size messages from the FRONT of the buffer.

        Deliberately synchronous. It contains no await, so it cannot be
        interrupted half-way and cannot lose messages to a cancellation landing
        between the pop and the caller receiving the list -- the flusher owns
        the batch the instant this returns. The returned list is always a fresh
        list object and never a view of the buffer's storage, so re-queueing it
        later cannot corrupt what is in the buffer by then (the Go reference has
        to copy() explicitly for the same reason: a re-queued sub-slice would
        alias the backing array).

        Args:
            batch_size: How many to take (None = the configured message_count)

        Returns:
            The batch, oldest first; empty if the buffer is empty or stopped
        """
        if self._stopped or not self._messages:
            return []
        size = self._flush_threshold if batch_size is None else batch_size
        size = max(1, min(size, len(self._messages)))
        return [self._messages.popleft() for _ in range(size)]

    def requeue_front(self, batch: List[Dict[str, Any]]) -> None:
        """
        Put a batch that failed to send back at the FRONT of the buffer.

        Never dropped, and never re-ordered. Before the fix the flusher took the
        batch out, POSTed it, and on error only logged: up to message_count
        messages vanished per failed POST, with the caller long since told the
        push succeeded.

        Synchronous for the same reason as take_batch, and it matters more here:
        this is called from inside the CancelledError handler of the flush loop,
        where an await could suspend and never come back, so the only way the
        batch is guaranteed to survive cancellation is for the re-queue to
        contain no suspension point at all.

        Note this is the one place occupancy can exceed max_size, by at most one
        batch (the buffer may have refilled to the bound while this batch was in
        flight). That overshoot is documented on the max_size option and is the
        price of never dropping.

        Args:
            batch: The batch previously returned by take_batch
        """
        if not batch:
            return
        # extendleft() inserts its argument in REVERSE, so reverse it first: the
        # entire value of re-queueing at the front is preserving the producer's
        # order.
        self._messages.extendleft(reversed(batch))

    async def notify_drained(self) -> None:
        """
        Wake every producer parked on the max_size bound.

        notify_all rather than notify, mirroring the Go reference's Broadcast:
        a drained batch frees capacity for several producers at once, and a
        broadcast is also immune to the wake landing on a waiter that has just
        been cancelled (with notify(1) that wake would be consumed and lost).
        """
        async with self._cond:
            self._cond.notify_all()

    # ===========================
    # Timer
    # ===========================

    def _start_timer(self) -> None:
        """Start the time-based flush timer (no-op if already running)"""
        if self._timer:
            return

        loop = asyncio.get_running_loop()
        self._timer = loop.call_later(self._time_millis / 1000.0, self._on_timer)

    def _on_timer(self) -> None:
        """Timer callback: schedule a drain.

        Reads `_flushing`/`_messages` without the condition's lock, which is
        safe because there is no await between the reads and the scheduling, and
        because the drain re-checks both under begin_flush(). The reads here are
        a hint that avoids scheduling a task that would immediately no-op.
        """
        self._timer = None
        if not self._messages:
            return
        self._request_flush()

    def cancel_timer(self) -> None:
        """Cancel the timer without triggering a flush"""
        if self._timer:
            self._timer.cancel()
            self._timer = None

    # ===========================
    # Lifecycle
    # ===========================

    async def stop(self) -> None:
        """
        Stop the buffer: no more adds, and the drain loop unwinds.

        Awaitable where the Go reference's Stop() is not, and the reason is the
        primitive: sync.Cond.Broadcast can be called without holding the mutex,
        while asyncio.Condition.notify_all REQUIRES the lock, so the wake has to
        happen from a coroutine. Waking here is not optional -- a producer
        parked on the bound at shutdown would otherwise wait forever for a
        drain that is never going to run again.
        """
        self.cancel_timer()
        async with self._cond:
            self._stopped = True
            self._cond.notify_all()

    async def cleanup(self) -> None:
        """Stop the buffer and discard whatever it still holds"""
        await self.stop()
        self._messages.clear()
        self._first_message_time = None
        self._flushing = False

    # ===========================
    # Introspection
    # ===========================

    @property
    def message_count(self) -> int:
        """Number of messages currently buffered"""
        return len(self._messages)

    @property
    def options(self) -> Dict[str, Any]:
        """Resolved buffer options"""
        return self._options

    @property
    def max_size(self) -> int:
        """Backpressure bound: adds block once the buffer holds this many"""
        return self._max_size

    @property
    def retry_delay_seconds(self) -> float:
        """Delay before retrying a failed flush batch, in seconds"""
        return self._retry_delay_millis / 1000.0

    @property
    def is_flushing(self) -> bool:
        """True while a drain owns this buffer"""
        return self._flushing

    @property
    def is_stopped(self) -> bool:
        """True once stop()/cleanup() has run"""
        return self._stopped

    @property
    def first_message_age(self) -> float:
        """Get age of first message in milliseconds"""
        if self._first_message_time:
            return (time.time() - self._first_message_time) * 1000.0
        return 0.0
