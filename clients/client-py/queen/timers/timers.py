"""
The timer API -- ``client.timers`` and ``client.timer(queue)``.

PLAN_KV_TIMERS.md §4 (contract), §8.1 (routes), §9.6 (why cancel has a route of
its own, which makes it an SDK decision and not only a server one).

THE THREE SENTENCES THAT HAVE TO TRAVEL WITH THIS API:

  1. ``deliverAt`` IS "NOT BEFORE", NEVER "EXACTLY AT". The measured floor on
     this stack is a single hop around 10 ms plus one sweep cycle. A healthy
     timer lands within about ten milliseconds of the sweeper's minimum sleep;
     past its maximum sleep (1 s) there is a wake-up problem, not a load
     problem.
  2. ``absent`` MEANS "NO LONGER PENDING" AND MAY MEAN ALREADY DELIVERED
     (§4.4). There is no tombstone -- a delivered timer has no row. The
     authority is the log: look for the timer's ``txn`` in the destination
     queue, which is why every answer carries it back.
  3. ``too_late`` IS A VERDICT, NOT A FAILURE (§4.3). The broker holding the
     claim has already unpacked and packed that payload and is about to commit
     it. The remedy is a new key, or waiting for delivery and acting on the
     message.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote, urlencode

import httpx

from ..errors import TimerError, wrap_http_error
from ..utils import logger
from . import ops as _ops

_PATH = "/api/v1/timers"


class TimerResult(dict):
    """A timer result element, which is a plain dict plus a truthful ``bool``.

    ``status`` is a CLOSED taxonomy -- ``scheduled | rescheduled | cancelled |
    absent | too_late`` -- closed because a client that has to tell them apart
    writes a match, not a substring search. ``__bool__`` reads ``ok``, so the
    three verdicts that are not successes (``absent``, ``too_late``, and a
    ``cancel`` that found nothing) do not read as one.
    """

    __slots__ = ()

    def __bool__(self) -> bool:
        if "ok" in self:
            return bool(self["ok"])
        if "found" in self:
            return bool(self["found"])
        return len(self) > 0


class Timers:
    """Scheduled messages against one Queen deployment."""

    op = _ops

    def __init__(self, http_client: Any) -> None:
        self._http_client = http_client

    # -----------------------------------------------------------------
    # POST /api/v1/timers -- schedule and reschedule, batch.
    # -----------------------------------------------------------------

    async def batch(self, operations: Iterable[Dict[str, Any]]) -> List[TimerResult]:
        """Apply timer ops in one transaction. Results are INDEX-ALIGNED (§6.4).

        A cancel IS accepted in this array -- it is the same stored procedure --
        but a cancel sent here inherits this ROUTE's authorization class, so on
        a cluster that is over quota a mixed batch is refused whole (§9.6). Use
        :meth:`cancel`, which takes the route that is never blocked.
        """
        op_list = list(operations)
        if not op_list:
            return []
        for op in op_list:
            _ops.check_not_server_owned(op)
        logger.log("Timers.batch", {"count": len(op_list), "ops": [o.get("op") for o in op_list]})
        try:
            response = await self._http_client.post(_PATH, {"operations": op_list})
        except Exception as error:  # noqa: BLE001 - re-raised, possibly re-typed
            raise wrap_http_error(error, TimerError) from None
        results = (response or {}).get("results") or []
        return [TimerResult(r) if isinstance(r, dict) else r for r in results]

    async def _one(self, op: Dict[str, Any]) -> TimerResult:
        results = await self.batch([op])
        if not results:
            raise TimerError(
                "the server returned no result for a single-op call",
                request=httpx.Request("POST", _PATH),
                response=httpx.Response(500),
                code="timers_result_missing",
            )
        return results[0]

    async def schedule(
        self,
        queue: str,
        timer_key: str,
        payload: Any,
        *,
        delay_ms: Optional[Any] = None,
        delay: Optional[Any] = None,
        partition: Optional[str] = None,
        txn: Optional[str] = None,
        payload_zstd: bool = False,
    ) -> TimerResult:
        """Schedule a message for later. Upsert on ``(queue, timer_key)``.

        Returns ``{ok, status, queue, timerKey, txn, messageId, deliverAt}``.
        The ``messageId`` is promised at schedule time so the delivered frame
        can be correlated without a second API call.
        """
        return await self._one(
            _ops.schedule(
                queue, timer_key, payload,
                delay_ms=delay_ms, delay=delay, partition=partition, txn=txn, payload_zstd=payload_zstd,
            )
        )

    async def reschedule(
        self,
        queue: str,
        timer_key: str,
        payload: Any,
        *,
        delay_ms: Optional[Any] = None,
        delay: Optional[Any] = None,
        partition: Optional[str] = None,
        txn: Optional[str] = None,
        payload_zstd: bool = False,
    ) -> TimerResult:
        """The same upsert under the name of the intent. ``attempts`` returns to
        zero: a rescheduled timer is a new timer under an old name."""
        return await self._one(
            _ops.schedule(
                queue, timer_key, payload,
                delay_ms=delay_ms, delay=delay, partition=partition, txn=txn,
                payload_zstd=payload_zstd, reschedule=True,
            )
        )

    # -----------------------------------------------------------------
    # DELETE /api/v1/timers/:queue/*timerKey -- §9.6, never blockable.
    # -----------------------------------------------------------------

    async def cancel(self, queue: str, timer_key: str, *, txn: Optional[str] = None) -> TimerResult:
        """Cancel a pending timer, over the route that cannot be blocked.

        THIS IS NOT A DETAIL OF THE URL. ``POST /api/v1/timers`` carries cancels
        in the same array as schedules, so a cancel sent there inherits the
        schedule's authorization and is refused on a cluster that is over quota
        or has scheduling paused. The fire never switches itself off, so a
        tenant that cannot cancel keeps producing messages it cannot stop until
        the horizon or an operator -- the block would produce the exact opposite
        of its purpose. Hence the separate route, and hence this method.

        ``txn`` is echoed back on ``absent`` so the "was it already delivered?"
        check needs no second API (§4.4). Pass the one you got from
        :meth:`schedule`.

        Cancelling a timer that is in BACKOFF succeeds: during backoff the claim
        token is NULL on purpose.
        """
        path = f"{_PATH}/{quote(queue, safe='')}/{quote(timer_key, safe='/')}"
        if txn:
            path = f"{path}?{urlencode({'txn': txn})}"
        logger.log("Timers.cancel", {"queue": queue, "timer_key": timer_key})
        try:
            response = await self._http_client.delete(path)
        except Exception as error:  # noqa: BLE001
            raise wrap_http_error(error, TimerError) from None
        return TimerResult(response or {})

    # -----------------------------------------------------------------
    # Reads.
    # -----------------------------------------------------------------

    async def peek(self, queue: str, timer_key: str) -> TimerResult:
        """One key, with its payload exactly as stored.

        A miss is ``{"found": false}`` with HTTP 200. ``encrypted`` tells the
        truth about the payload -- peek is an inspection surface and does not
        quietly decrypt what the fire will deliver as an envelope.
        """
        path = f"{_PATH}/{quote(queue, safe='')}/{quote(timer_key, safe='/')}"
        try:
            response = await self._http_client.get(path)
        except Exception as error:  # noqa: BLE001
            raise wrap_http_error(error, TimerError) from None
        return TimerResult(response or {})

    async def list(
        self,
        queue: str,
        *,
        after: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Keyset page of the pending timers of ONE queue.

        The queue is mandatory and is a path segment rather than a filter on
        purpose (§4.1): a tenant-wide list would be a scan that an end user of
        the customer could trigger, on the first endpoint of this product whose
        call rate is decided by somebody else's web traffic.

        ``after`` is an EXCLUSIVE keyset cursor, not an offset. ``limit`` is
        clamped by the server and never rejected, with ``truncated`` telling
        the truth.
        """
        params: Dict[str, Any] = {}
        if after:
            params["after"] = after
        if limit is not None:
            params["limit"] = limit
        path = f"{_PATH}/{quote(queue, safe='')}"
        if params:
            path = f"{path}?{urlencode(params)}"
        try:
            return await self._http_client.get(path) or {}
        except Exception as error:  # noqa: BLE001
            raise wrap_http_error(error, TimerError) from None

    async def list_all(self, queue: str, *, limit: Optional[int] = None, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Walk ``nextAfter`` to the end of a queue's pending timers."""
        rows: List[Dict[str, Any]] = []
        after: Optional[str] = None
        while True:
            page = await self.list(queue, after=after, limit=limit)
            rows.extend(page.get("rows") or [])
            if max_rows is not None and len(rows) >= max_rows:
                return rows[:max_rows]
            if not page.get("truncated"):
                return rows
            after = page.get("nextAfter")
            if not after:
                return rows

    # -----------------------------------------------------------------
    # The builder.
    # -----------------------------------------------------------------

    def timer(self, queue: str) -> "TimerBuilder":
        """Fluent form, with ``schedule()`` / ``reschedule()`` / ``cancel()``
        as the terminals."""
        return TimerBuilder(self, queue)


class TimerBuilder:
    """Fluent timer, mirroring ``QueueBuilder``'s shape in this client.

        await (client.timer("orders")
                     .key("order:9f1:expire")
                     .payload({"orderId": "9f1"})
                     .after(timedelta(minutes=30))
                     .schedule())

    The terminals are coroutines here and are NOT coroutines on the transaction
    variant (``tx.timer(...)``), which returns the transaction for chaining --
    the same split this client already has between ``client.queue(q).push()``
    and ``tx.queue(q).push()``.
    """

    def __init__(self, timers: Timers, queue: str) -> None:
        self._timers = timers
        self._queue = queue
        self._key: Optional[str] = None
        self._payload: Any = None
        self._has_payload = False
        self._delay_ms: Optional[Any] = None
        self._delay: Optional[Any] = None
        self._partition: Optional[str] = None
        self._txn: Optional[str] = None
        self._payload_zstd = False

    def key(self, timer_key: str) -> "TimerBuilder":
        self._key = timer_key
        return self

    def payload(self, payload: Any, *, zstd: bool = False) -> "TimerBuilder":
        self._payload = payload
        self._has_payload = True
        self._payload_zstd = zstd
        return self

    def after_ms(self, delay_ms: Any) -> "TimerBuilder":
        self._delay_ms = delay_ms
        return self

    def after(self, delay: Any) -> "TimerBuilder":
        """A ``timedelta``. There is no string duration parser in this SDK and
        there will not be one: Python already has the type."""
        self._delay = delay
        return self

    def partition(self, partition: str) -> "TimerBuilder":
        self._partition = partition
        return self

    def txn(self, txn: str) -> "TimerBuilder":
        self._txn = txn
        return self

    def _require_key(self) -> str:
        if not self._key:
            raise ValueError("timer needs .key(<timerKey>)")
        return self._key

    def _require_payload(self) -> Any:
        if not self._has_payload:
            raise ValueError("timer needs .payload(<data>)")
        return self._payload

    async def schedule(self) -> TimerResult:
        return await self._timers.schedule(
            self._queue, self._require_key(), self._require_payload(),
            delay_ms=self._delay_ms, delay=self._delay, partition=self._partition,
            txn=self._txn, payload_zstd=self._payload_zstd,
        )

    async def reschedule(self) -> TimerResult:
        return await self._timers.reschedule(
            self._queue, self._require_key(), self._require_payload(),
            delay_ms=self._delay_ms, delay=self._delay, partition=self._partition,
            txn=self._txn, payload_zstd=self._payload_zstd,
        )

    async def cancel(self) -> TimerResult:
        """Terminal cancel -- takes the DELETE route, the one that is never
        blockable (§9.6)."""
        return await self._timers.cancel(self._queue, self._require_key(), txn=self._txn)
