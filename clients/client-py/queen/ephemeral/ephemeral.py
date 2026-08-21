"""
The ephemeral surface -- ``client.ephemeral``.

EPHEMERAL_QUEUES.md §1 (semantics), §3.1 (the wire), §4 / §4.1 (the SDK).

Eight verbs over one route family, ``/api/v1/ephemeral/*``: configure, reset,
delete, push, pop, ack, queues, depth. Flat coroutines, not a builder chain --
the durable ``queue(name).partition(p).push(...)`` fluency exists because a
durable queue has a dozen configured properties that read well as a sentence;
an ephemeral queue has a ring in a broker's RAM and a handful of bounds, and a
chain would only hide how few moving parts there are.

WHAT THIS CLASS IS ABOUT, BEFORE ANY SIGNATURE: contents survive NOTHING
(§1.2). Not a restart, not a crash, not a deploy, not the ownership move a
membership change causes. Treat a failover like a Redis restart. Declared
CONFIGURATION is durable -- it lives in PG and comes back after a restart, as
configured and EMPTY. There is no replay, no history, no subscription mode and
no DLQ, because none of those concepts has a referent when there is no history
to have.

DELIVERY IS NOT "AT MOST ONCE" (§1.3), and the docs must not say it is. The
class picks what can be LOST; the ack mode picks the guarantee. ``auto_ack``
advances the cursor at delivery and is at-most-once. The default -- explicit
ack -- is at-least-once for as long as the owning broker incarnation lives: an
unacked message redelivers when its lease expires, with ``attempts``
incremented, until ``retryLimit``, after which it is DROPPED and counted (no
DLQ, §9). Consumers still need idempotency, exactly as on durable queues.

CONSUMPTION SEMANTICS COME FROM THE GROUP, EXACTLY AS ON THE DURABLE ENGINE
(§1.5). There is no queue-level mode to choose::

    await eph.pop(q, group="workers")   # competing consumers: one cursor
    await eph.pop(q, group="tail-a")    # fan-out: this subscriber's own cursor
    await eph.pop(q)                    # groupless queue mode, as on durable

Every group has its own cursor over the ONE ring, so fan-out subscribers each
see everything and competing consumers of one group share the work.

ORDERING is FIFO per (queue, partition) within one ownership incarnation.
Across an incarnation boundary the question is empty: the contents are gone.

AND THE TWO KINDS OF 404, WHICH MUST NEVER BE CONFUSED FOR EACH OTHER. No SDK
negotiates a version, so against a broker or proxy older than 1.1 the whole
family answers 404 -- the broker because the routes do not exist, the proxy
because an unknown API path is ``route_blocked``. That is a DEPLOYMENT fact and
arrives as an ``EphemeralError`` whose ``.code`` is ``EPHEMERAL_UNSUPPORTED``.

But ``depth`` also answers a real 404, with ``code: 'ephemeral_queue_not_found'``,
when the queue simply is not there -- and it is the only verb that can, since
push and pop create implicitly, ``reset`` answers ``dropped:0`` and ``delete``
answers ``deleted:false``. That is a DATA fact and arrives as
``EphemeralQueueNotFoundError``, whose ``.code`` is
``EPHEMERAL_QUEUE_NOT_FOUND``. Collapsing it into the first would send somebody
chasing a broker version over a queue name typo.

Both keep the broker's own refusal as ``__cause__``. Branch on the code, never
on the prose.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union
from urllib.parse import quote, urlencode

import httpx

from ..buffer.sinks import EPHEMERAL_SINK, ephemeral_address, ephemeral_destination
from ..errors import (
    EphemeralError,
    EphemeralQueueNotFoundError,
    code_of,
    wrap_http_error,
)
from ..utils import logger

#: ``error.code`` on the old-broker error, so callers branch on a code and never
#: on a message.
EPHEMERAL_UNSUPPORTED = "ephemeral_unsupported"

#: The message every SDK fixes for this case (§4). Keep it identical across
#: clients: operators grep it.
EPHEMERAL_UNSUPPORTED_MESSAGE = (
    "broker/proxy does not support ephemeral queues (requires >= 1.1)"
)

#: ``error.code`` when the queue itself does not exist -- the broker's own code
#: string, kept identical across every SDK (Go ``ErrEphemeralQueueNotFound``,
#: ``queen_protocol::EPHEMERAL_QUEUE_NOT_FOUND_CODE``) so a code seen in one
#: language's logs means the same thing in the next.
EPHEMERAL_QUEUE_NOT_FOUND = "ephemeral_queue_not_found"

_CONFIGURE_ROUTE = "/api/v1/ephemeral/configure"
_RESET_ROUTE = "/api/v1/ephemeral/reset"
_ACK_ROUTE = "/api/v1/ephemeral/ack"
_POP_ROUTE = "/api/v1/ephemeral/pop"
_QUEUES_ROUTE = "/api/v1/ephemeral/queues"

# The seven knobs of `configure` (§3.1), wire spelling first and the snake_case
# spelling this SDK's callers write beside it. A CLOSED list: an option this
# client does not know is refused rather than dropped on the floor, because
# every one of these bounds something (bytes, length, age, redelivery) and a
# silently ignored `ttl_second` is a ring that grows until a global budget
# answers 503.
_CONFIGURE_OPTIONS: Tuple[Tuple[str, str], ...] = (
    ("maxBytes", "max_bytes"),
    ("maxLength", "max_length"),
    ("policy", "policy"),
    ("ttlSeconds", "ttl_seconds"),
    ("leaseSeconds", "lease_seconds"),
    ("retryLimit", "retry_limit"),
    ("windowBuffer", "window_buffer"),
)

#: Long-poll default, matching the durable pop's, when ``wait`` is asked for
#: without a timeout.
DEFAULT_WAIT_TIMEOUT_MILLIS = 30000

# The HTTP deadline must outlive the server's own long-poll timeout, or the
# client aborts a request the broker was about to answer. Same 5s slack the
# durable pop uses.
WAIT_TIMEOUT_SLACK_MILLIS = 5000


def _require_queue(queue: Any) -> str:
    if not isinstance(queue, str) or not queue:
        raise ValueError(f"ephemeral: queue must be a non-empty string, got {queue!r}")
    return queue


def _map_error(error: Exception, queue: Optional[str] = None) -> Exception:
    """Two facts arrive on this family as 404, and telling them apart is the
    whole job of this function. THE BODY'S CODE decides, not the status:

    * ``ephemeral_queue_not_found`` -- the routes are there and answered; the
      QUEUE is not. Only ``depth`` can say this (§3.1): push and pop create
      implicitly, ``reset`` answers ``dropped:0``, ``delete`` answers
      ``deleted:false``. It is checked for on every verb anyway, because which
      verbs can say it is the broker's business and this client should not
      re-encode that list.
    * anything else -- an old broker that never registered the routes, or an old
      proxy answering ``route_blocked`` because it fails closed on unknown API
      paths (§4, §8). Both mean "upgrade".

    ``queue`` is passed only so a missing-queue error can name it. The broker's
    own response is kept on both mappings, so nothing the HTTP layer surfaced is
    lost.
    """
    if not (
        isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 404
    ):
        return wrap_http_error(error, EphemeralError)

    if code_of(error.response) == EPHEMERAL_QUEUE_NOT_FOUND:
        return EphemeralQueueNotFoundError(
            f'ephemeral: queue "{queue}" does not exist'
            if queue
            else "ephemeral: that queue does not exist",
            request=error.request,
            response=error.response,
            code=EPHEMERAL_QUEUE_NOT_FOUND,
            queue=queue,
        )

    return EphemeralError(
        EPHEMERAL_UNSUPPORTED_MESSAGE,
        request=error.request,
        response=error.response,
        code=EPHEMERAL_UNSUPPORTED,
    )


def _configure_body(
    options: Optional[Mapping[str, Any]], overrides: Mapping[str, Any]
) -> Dict[str, Any]:
    """Merge the mapping form and the keyword form into the wire's fixed order.

    Both spellings of a knob are accepted -- the wire's ``ttlSeconds`` because
    that is what a config file or a copy-pasted body carries, and the
    ``ttl_seconds`` this SDK writes everywhere else -- and giving the SAME knob
    twice is refused rather than silently resolved, the rule the KV expiry sugar
    already follows.
    """
    # Keywords are applied last and therefore win over the same key in the
    # mapping -- the mapping is the config file, the keyword is the call site.
    given: Dict[str, Any] = {}
    if options is not None:
        if not isinstance(options, Mapping):
            raise ValueError(
                f"ephemeral: configure options must be a mapping, got {options!r}"
            )
        given.update(options)
    given.update(overrides)

    known: Dict[str, str] = {}
    for wire, snake in _CONFIGURE_OPTIONS:
        known[wire] = wire
        known[snake] = wire

    unknown = [key for key in given if key not in known]
    if unknown:
        raise ValueError(
            f"ephemeral: unknown configure option(s) {', '.join(sorted(unknown))} — "
            "an option this client does not know would be silently dropped. "
            f"Known options: {', '.join(snake for _, snake in _CONFIGURE_OPTIONS)}"
        )

    resolved: Dict[str, Any] = {}
    for key, value in given.items():
        wire = known[key]
        if wire in resolved and resolved[wire] != value:
            raise ValueError(
                f"ephemeral: configure got two values for {wire} — "
                "pass either the wire spelling or the snake_case one, not both"
            )
        resolved[wire] = value

    # Fixed order, so the body a caller reads in a log is the body §3.1
    # documents, whichever order they wrote their keyword arguments in.
    body: Dict[str, Any] = {}
    for wire, _snake in _CONFIGURE_OPTIONS:
        if wire in resolved and resolved[wire] is not None:
            body[wire] = resolved[wire]
    return body


def _to_message(item: Any) -> Dict[str, Any]:
    """One message on the ephemeral wire is ``{payload}`` and nothing else.

    No transactionId, because there is no dedup index to hold one, and no queue
    or partition, because the envelope already carries them.

    The ``{"data": ...}`` / ``{"payload": ...}`` sugar is the durable push's,
    deliberately reproduced so one mental model covers both families --
    INCLUDING its trap: a dict that happens to have a ``data`` key is read as
    the sugar, and its other keys do not travel. Wrap it (``{"payload": obj}``)
    when the dict IS the payload.
    """
    if item is None:
        raise ValueError(
            "ephemeral: a message may not be None — "
            'write {"payload": None} to push a null payload'
        )
    if isinstance(item, Mapping):
        if "payload" in item:
            return {"payload": item["payload"]}
        if "data" in item:
            return {"payload": item["data"]}
    return {"payload": item}


def _to_messages(messages: Any) -> List[Dict[str, Any]]:
    if isinstance(messages, (list, tuple)):
        items: Iterable[Any] = messages
    else:
        items = [messages]
    return [_to_message(item) for item in items]


def _normalize_status(status: Any) -> Any:
    """``True``/``False`` are sugar for the two statuses people actually mean."""
    if isinstance(status, bool):
        return "completed" if status else "failed"
    return status


def _to_acks(
    acks: Any, status: Any = None, error: Any = None
) -> List[Dict[str, Any]]:
    """An ack is ``{id, status?, error?}``.

    Accepts a popped message, a bare id string, or the wire object itself; a
    per-entry status wins over the call-wide default, which is how a mixed batch
    (some completed, one retry) is expressed in a single request.
    """
    items = acks if isinstance(acks, (list, tuple)) else [acks]
    out: List[Dict[str, Any]] = []
    for index, entry in enumerate(items):
        is_mapping = isinstance(entry, Mapping)
        entry_id = entry if isinstance(entry, str) else (entry.get("id") if is_mapping else None)
        if not isinstance(entry_id, str) or not entry_id:
            raise ValueError(
                f"ephemeral: ack at index {index} carries no message id — "
                "pass the popped message, or its `id`"
            )

        ack: Dict[str, Any] = {"id": entry_id}

        entry_status = entry.get("status") if is_mapping else None
        effective_status = entry_status if entry_status is not None else status
        if effective_status is not None:
            ack["status"] = _normalize_status(effective_status)

        entry_error = entry.get("error") if is_mapping else None
        effective_error = entry_error if entry_error is not None else error
        if effective_error is not None:
            ack["error"] = effective_error

        out.append(ack)
    return out


def _resolve_timeout(timeout: Optional[int], timeout_millis: Optional[int]) -> int:
    """``timeout`` is the wire's name and milliseconds is the SDK's unit, so both
    spellings are accepted -- and BOTH AT ONCE is refused rather than silently
    resolved, the same rule the KV expiry sugar follows."""
    if timeout is not None and timeout_millis is not None:
        raise ValueError(
            "ephemeral: pass either `timeout` or `timeout_millis`, not both — "
            "they are the same milliseconds"
        )
    if timeout is not None:
        return timeout
    if timeout_millis is not None:
        return timeout_millis
    return DEFAULT_WAIT_TIMEOUT_MILLIS


def _buffer_options_from(buffered: Any) -> Dict[str, Any]:
    """Buffer options are the durable ``buffer()`` call's, unchanged (§4.1).

    The two families share the machinery, so they share its vocabulary --
    ``message_count`` / ``time_millis`` / ``max_size`` / ``retry_delay_millis``.
    ``interval_millis`` is accepted as a spelling of ``time_millis`` because it
    is the name the ephemeral plan's API sketch used; it is TRANSLATED rather
    than passed through, since ``resolve_buffer_options`` carries unknown keys
    untouched and would silently ignore it -- a linger option that quietly does
    nothing is a producer that batches on count alone and stalls below the
    threshold. Both spellings at once is refused.
    """
    if buffered is True:
        return {}
    if not isinstance(buffered, Mapping):
        raise ValueError(
            f"ephemeral: `buffered` must be True or an options mapping, got {buffered!r}"
        )

    options = dict(buffered)
    interval_millis = options.pop("interval_millis", None)
    if interval_millis is not None:
        if options.get("time_millis") is not None:
            raise ValueError(
                "ephemeral: pass either `time_millis` or `interval_millis`, not both — "
                "they are the same linger"
            )
        options["time_millis"] = interval_millis
    return options


class Ephemeral:
    """RAM-class queues, reached through ``client.ephemeral``."""

    def __init__(self, http_client: Any, buffer_manager: Any = None) -> None:
        self._http_client = http_client
        self._buffer_manager = buffer_manager

    # -----------------------------------------------------------------
    # The one place the two 404 rules live.
    # -----------------------------------------------------------------

    async def _call(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        *,
        timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None,
        retry_kind: Optional[str] = None,
        queue: Optional[str] = None,
    ) -> Any:
        try:
            if method == "GET":
                return await self._http_client.get(
                    path, timeout_millis, affinity_key, retry_kind=retry_kind
                )
            if method == "DELETE":
                return await self._http_client.delete(
                    path, timeout_millis, affinity_key, retry_kind=retry_kind
                )
            return await self._http_client.post(
                path, body, timeout_millis, affinity_key, retry_kind=retry_kind
            )
        except Exception as error:  # noqa: BLE001 - re-raised, possibly re-typed
            logger.error(
                "Ephemeral.request",
                {
                    "method": method,
                    "path": path,
                    "status": getattr(error, "status", None),
                    "error": str(error),
                    "code": getattr(error, "code", None),
                },
            )
            mapped = _map_error(error, queue)
            # The original is kept as __cause__ for BOTH 404 mappings -- "the
            # broker answered 404" is the evidence for "upgrade the broker", and
            # "it answered 404 with ephemeral_queue_not_found" is the evidence
            # for "that queue is not there"; dropping either would leave a
            # caller with a claim and no proof. Anything else follows the
            # kv/timers precedent and suppresses the chain, whose traceback says
            # nothing the wrapped error does not.
            if isinstance(mapped, EphemeralError) and mapped.code in (
                EPHEMERAL_UNSUPPORTED,
                EPHEMERAL_QUEUE_NOT_FOUND,
            ):
                raise mapped from error
            raise mapped from None

    # -----------------------------------------------------------------
    # Declaration.
    # -----------------------------------------------------------------

    async def configure(
        self,
        queue: str,
        options: Optional[Mapping[str, Any]] = None,
        **overrides: Any,
    ) -> Any:
        """Declare a queue and its bounds. Persists the OPTIONS in PG (§1.1):
        the configuration survives a restart, the contents never do, and the
        queue comes back declared and empty.

        Optional in every sense -- a push or a pop that names an unknown queue
        creates it implicitly with the tenant defaults (§1.1). Declare when you
        want non-default bounds, or when you want the queue to exist in the
        dashboard before its first message.

        Options, all optional, as keywords or as one mapping:
        ``max_bytes`` / ``max_length`` (the per-queue budget, with ``policy``
        deciding whether breaching it rejects the push with 429 or drops the
        OLDEST message -- feed semantics), ``ttl_seconds`` (drop messages older
        than this; it is NOT the durable ``retention``, which cleans consumed
        history and never touches pending), ``lease_seconds`` and
        ``retry_limit`` (redelivery), ``window_buffer`` (``{"ms":…, "count":…}``
        -- let a waiting pop fatten its batch).

        An option this client does not know is REFUSED, never dropped.
        """
        _require_queue(queue)
        body = {"queue": queue, "options": _configure_body(options, overrides)}
        logger.log("Ephemeral.configure", {"queue": queue, "options": list(body["options"])})
        return await self._call("POST", _CONFIGURE_ROUTE, body, queue=queue)

    async def reset(self, queue: str) -> Any:
        """Drop every message, void every lease, rewind every group cursor.
        Answers ``{dropped}``.

        A verb that would be indefensible on a durable queue and is merely
        honest here: it destroys nothing the class ever promised to keep (§1.2).
        The declared configuration stays.
        """
        _require_queue(queue)
        logger.log("Ephemeral.reset", {"queue": queue})
        return await self._call("POST", _RESET_ROUTE, {"queue": queue}, queue=queue)

    async def delete(self, queue: str) -> Any:
        """Delete the queue: contents, cursors, and the declared configuration
        in PG."""
        _require_queue(queue)
        logger.log("Ephemeral.delete", {"queue": queue})
        return await self._call(
            "DELETE", f"/api/v1/ephemeral/queue/{quote(queue, safe='')}", queue=queue
        )

    # -----------------------------------------------------------------
    # Push.
    # -----------------------------------------------------------------

    async def push(
        self,
        queue: str,
        messages: Any,
        *,
        partition: Optional[str] = None,
        buffered: Any = None,
    ) -> Dict[str, Any]:
        """Push one message or many. All-or-nothing per request; answers
        ``{pushed}``::

            await client.ephemeral.push("presence", [{"user": "a", "typing": True}])
            await client.ephemeral.push("presence", msgs, partition="room-7")

        ``partition`` picks the ring (FIFO is per partition, §1.4); omitted, the
        broker picks and this client does not invent a default.

        ``buffered`` (``True`` or ``{"interval_millis":…, "message_count":…,
        "max_size":…}``) batches client-side through the SAME machinery the
        durable push uses (§4.1) -- blocking backpressure at ``max_size``, a
        failed batch back at the FRONT and retried, ``client.close()`` draining
        it under a deadline. It returns ``{"buffered": True, "count": n}`` once
        the messages are IN the buffer, not once they are at the broker, and a
        buffered message that has not flushed dies with the process. That last
        part is already inside this class's contract, which is exactly why
        buffering is a reasonable default here and a considered decision on a
        durable queue.
        """
        _require_queue(queue)
        items = _to_messages(messages)
        if not items:
            return {"pushed": 0}

        # `is not None and is not False`, not a truth test: `buffered={}` means
        # "buffer with the defaults" and an empty mapping is falsy in Python,
        # so a plain `if buffered:` would silently send it down the direct path.
        if buffered is not None and buffered is not False:
            if self._buffer_manager is None:
                raise ValueError(
                    "ephemeral: buffered push needs the client's buffer manager — "
                    "use client.ephemeral, not a hand-built Ephemeral"
                )
            return await self._push_buffered(queue, partition, items, buffered)

        body: Dict[str, Any] = {"queue": queue}
        if partition is not None:
            body["partition"] = partition
        body["messages"] = items

        logger.log(
            "Ephemeral.push", {"queue": queue, "partition": partition, "count": len(items)}
        )
        return await self._call("POST", EPHEMERAL_SINK.path, body, queue=queue)

    async def _push_buffered(
        self,
        queue: str,
        partition: Optional[str],
        items: List[Dict[str, Any]],
        buffered: Any,
    ) -> Dict[str, Any]:
        """The buffered variant.

        One buffer per ``eph:<queue>/<partition>`` address, so an ephemeral
        queue and a durable queue of the same name never share a buffer or a
        drain (§4.1).
        """
        address = ephemeral_address(queue, partition)
        destination = ephemeral_destination(queue, partition)
        buffer_options = _buffer_options_from(buffered)
        accepted = 0

        # Awaited one at a time: add_message is where the max_size bound blocks,
        # so a buffered push that did not await would report success for
        # messages the buffer never accepted.
        try:
            for item in items:
                await self._buffer_manager.add_message(
                    address, item, buffer_options, destination
                )
                accepted += 1
        except Exception as error:  # noqa: BLE001 - reported, then re-raised
            logger.error(
                "Ephemeral.push",
                {
                    "queue": queue,
                    "partition": partition,
                    "status": "not-buffered",
                    "count": len(items) - accepted,
                    "error": str(error),
                },
            )
            raise

        logger.log(
            "Ephemeral.push",
            {"queue": queue, "partition": partition, "status": "buffered", "count": accepted},
        )
        return {"buffered": True, "count": accepted}

    async def flush(self, queue: str, *, partition: Optional[str] = None) -> None:
        """Send everything buffered for one ephemeral queue/partition, now.

        Unbounded on purpose, like every other flush in this SDK: the drain puts
        a failed batch back and keeps trying, and the only place that bound
        belongs is shutdown, where ``client.close()`` already applies
        ``CLOSE_FLUSH_TIMEOUT_SECONDS`` and reports what never left.
        """
        _require_queue(queue)
        if self._buffer_manager is None:
            return
        address = ephemeral_address(queue, partition)
        logger.log("Ephemeral.flush", {"queue": queue, "partition": partition})
        await self._buffer_manager.flush_buffer(address)

    # -----------------------------------------------------------------
    # Pop.
    # -----------------------------------------------------------------

    async def pop(
        self,
        queue: str,
        *,
        partition: Optional[str] = None,
        batch: Optional[int] = None,
        wait: bool = False,
        timeout: Optional[int] = None,
        timeout_millis: Optional[int] = None,
        group: Optional[str] = None,
        auto_ack: bool = False,
    ) -> Dict[str, Any]:
        """Take up to ``batch`` messages. Answers ``{queue, messages}``, with
        ``messages`` an EMPTY LIST when there was nothing -- never None, so the
        unpack is always safe::

            result = await client.ephemeral.pop("inbox", wait=True)
            for message in result["messages"]:
                ...

        Each message is ``{id, partition, payload, attempts}``. The ``id`` is
        opaque: it encodes the owning broker incarnation, which is what lets an
        ack that arrives after a restart or an ownership move answer ``stale``
        instead of acking somebody else's message.

        ``wait=True`` is a real long poll, parked on a RAM gate with no database
        behind it and no polling interval anywhere (§3.4) -- the structural
        reason an ephemeral inbox answers in transport time. ``timeout`` is
        milliseconds (default 30000 when waiting), and the HTTP deadline is set
        past it so the broker's timeout always fires first.

        ``group`` is the whole of the consumption semantics (§1.5): same group =
        competing consumers, own group = fan-out, no group = queue mode.
        ``auto_ack=True`` commits at delivery and is at-most-once.
        """
        _require_queue(queue)
        wait_millis = _resolve_timeout(timeout, timeout_millis)

        params: List[Tuple[str, str]] = [("queue", queue)]
        if partition is not None:
            params.append(("partition", partition))
        if batch is not None:
            params.append(("batch", str(batch)))
        # Sent only when true, so a plain pop is the shortest query this route
        # can receive and the broker's own defaults own everything else.
        if wait:
            params.append(("wait", "true"))
            params.append(("timeout", str(wait_millis)))
        if group is not None:
            params.append(("group", group))
        if auto_ack:
            params.append(("autoAck", "true"))

        logger.log(
            "Ephemeral.pop",
            {
                "queue": queue,
                "partition": partition,
                "group": group,
                "batch": batch,
                "wait": wait,
            },
        )

        # Affinity so repeated pops of one queue land on one backend when the
        # client holds several URLs: the broker forwards to the rendezvous owner
        # either way, so this saves a hop, it does not create correctness.
        affinity_key = f"{queue}:{partition or '*'}:{group or '__QUEUE_MODE__'}"
        result = await self._call(
            "GET",
            f"{_POP_ROUTE}?{urlencode(params)}",
            timeout_millis=(wait_millis + WAIT_TIMEOUT_SLACK_MILLIS) if wait else None,
            affinity_key=affinity_key,
            # A long poll that meets a 429 should back off and keep waiting
            # rather than give up after a handful of tries.
            retry_kind="pop" if wait else None,
            queue=queue,
        )

        body = result if isinstance(result, dict) else {}
        raw = body.get("messages")
        messages = [m for m in raw if m is not None] if isinstance(raw, list) else []
        logger.log(
            "Ephemeral.pop",
            {
                "queue": queue,
                "status": "success" if messages else "empty",
                "count": len(messages),
            },
        )
        return {"queue": body.get("queue") or queue, "messages": messages}

    # -----------------------------------------------------------------
    # Ack.
    # -----------------------------------------------------------------

    async def ack(
        self,
        queue: str,
        acks: Any,
        *,
        group: Optional[str] = None,
        status: Union[bool, str, None] = None,
        error: Optional[str] = None,
    ) -> Any:
        """Acknowledge popped messages. Answers ``{results:[{id, outcome}]}``
        with ``outcome`` in ``{acked, redelivered, stale, unknown}``::

            await client.ephemeral.ack("inbox", messages, group="workers")
            await client.ephemeral.ack("inbox", [{"id": mid, "status": "retry"}])

        ``stale`` is NOT an error and never arrives as one: it is the answer to
        an ack whose message belonged to a previous incarnation of the ring,
        which is how this class fences a restart or an ownership move without a
        lease protocol. Pass the same ``group`` the pop used -- cursors are per
        group.

        ``status`` is ``completed`` (default), ``failed`` or ``retry``; ``False``
        is sugar for ``failed``. A failed or retried message comes back with
        ``attempts+1`` until ``retryLimit``, then it is dropped and counted.
        There is no DLQ.
        """
        _require_queue(queue)
        entries = _to_acks(acks, status, error)
        if not entries:
            return {"results": []}

        body: Dict[str, Any] = {"queue": queue}
        if group is not None:
            body["group"] = group
        body["acks"] = entries

        logger.log(
            "Ephemeral.ack", {"queue": queue, "group": group, "count": len(entries)}
        )
        return await self._call("POST", _ACK_ROUTE, body, queue=queue)

    # -----------------------------------------------------------------
    # Status.
    # -----------------------------------------------------------------

    async def queues(self) -> Any:
        """Every ephemeral queue this tenant currently has, declared and
        implicit.

        Free to poll: the gauges are read out of the broker's own memory, with
        no database behind them -- unlike the durable meter, whose 1s poll is
        load-bearing on PG.
        """
        logger.log("Ephemeral.queues", {})
        return await self._call("GET", _QUEUES_ROUTE)

    async def depth(self, queue: str) -> Any:
        """Depth gauges for one queue: ring length, bytes, and the per-group
        cursors.

        THE ONLY VERB THAT CAN TELL YOU A QUEUE IS MISSING. Everything else
        either creates the queue (push, pop) or answers a normal body about
        having done nothing (``reset`` -> ``dropped:0``, ``delete`` ->
        ``deleted:false``). Here an unknown queue raises
        ``EphemeralQueueNotFoundError`` -- a different fact from the
        ``EPHEMERAL_UNSUPPORTED`` verdict, which is about the broker's version,
        and worth distinguishing precisely because both are 404s.
        """
        _require_queue(queue)
        logger.log("Ephemeral.depth", {"queue": queue})
        return await self._call(
            "GET", f"{_QUEUES_ROUTE}/{quote(queue, safe='')}/depth", queue=queue
        )
