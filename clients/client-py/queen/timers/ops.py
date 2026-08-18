"""
Op builders for the timer surface -- the ONE place a timer op is shaped.

Shared by ``client.timers.*``, by the ``client.timer(queue)`` builder and by
the transaction rider, so the three cannot drift and one set of body assertions
covers all of them.

PLAN_KV_TIMERS.md §4 (the declared contract), §4.2 (the fields the server owns).
"""

from __future__ import annotations

import base64
import json
import math
from datetime import timedelta
from typing import Any, Dict, Optional

from ..utils.uuid_gen import generate_uuid

__all__ = ["schedule", "cancel", "encode_payload", "delay_ms_of", "SERVER_OWNED"]


#: Fields the SERVER owns, in both spellings a caller might reach for. Present
#: in an op they are a 22023 at the broker and a rejection at the edge, never a
#: silent drop (§4.2): a tenant posting ``{"producerSub": "billing-service"}``
#: would otherwise get, one second later, a frame in the log whose provenance is
#: attested by the broker and forged by the client -- and ``producer_sub`` is the
#: one non-repudiable field of a frame.
#:
#: ``deliverAt`` and ``delaySeconds`` are in the list for a different reason:
#: they are the two things somebody will try to send instead of ``delayMs``, and
#: a named refusal here is better than a 22023 three layers down.
SERVER_OWNED = (
    "producerSub",
    "producer_sub",
    "messageId",
    "message_id",
    "tenant",
    "tenantId",
    "tenant_id",
    "deliverAt",
    "deliver_at",
    "delaySeconds",
    "delay_seconds",
    "attempts",
    "claimToken",
    "claim_token",
    "claimedUntil",
)


def check_not_server_owned(op: Dict[str, Any]) -> None:
    for field in op:
        if field in SERVER_OWNED or field.startswith("_"):
            raise ValueError(
                f"`{field}` is server-owned and cannot be supplied: the producer identity and "
                "the tenant come from the authenticated request, the message id is minted by "
                "the broker, and deliverAt is not expressible -- the wire carries only the "
                "relative delayMs"
            )


def encode_payload(payload: Any) -> str:
    """Payload -> base64, which is what the wire carries.

    ``bytes`` go through untouched, for a caller that has already framed its
    own body. Anything else is JSON-encoded first, because a timer BECOMES a
    message and the message body of this product is JSON.
    """
    if isinstance(payload, (bytes, bytearray, memoryview)):
        raw = bytes(payload)
    elif isinstance(payload, str):
        # A bare string is a JSON document too; encoding it as one keeps the
        # delivered frame parseable by the same consumer as every other push.
        raw = json.dumps(payload).encode("utf-8")
    else:
        raw = json.dumps(payload).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def delay_ms_of(delay_ms: Optional[Any], delay: Optional[Any]) -> int:
    """Resolve the two SDK spellings into the ONE field on the wire.

    §4.2 and the product's declared rule: **durations that can be sub-second are
    in milliseconds, the ones that cannot are in seconds.** A 250 ms retry
    backoff is a real and central use of a timer; a sub-second TTL is not a real
    use for anybody. Which is why this is ``delayMs`` and the KV expiry is
    ``ttlSeconds``.

    ONLY RELATIVE DURATIONS. An absolute instant is not expressible on this
    wire: ``deliver_at`` is computed in Postgres as ``now() + interval``, so
    there is one clock and no inter-broker skew can enter anywhere. A delay in
    the PAST is legal and fires on the first cycle.
    """
    if delay is not None and delay_ms is not None:
        raise ValueError("pass either delay_ms=<int> or delay=<timedelta>, not both")
    if delay is not None:
        if not isinstance(delay, timedelta):
            raise TypeError("delay must be a timedelta; use delay_ms for a plain number of milliseconds")
        return int(math.floor(delay.total_seconds() * 1000))
    if delay_ms is None:
        raise ValueError(
            "a delay is required: pass delay_ms=<int milliseconds> or delay=<timedelta>. "
            "deliverAt is not expressible -- only relative durations travel on this wire"
        )
    if isinstance(delay_ms, bool) or not isinstance(delay_ms, (int, float)):
        raise TypeError("delay_ms must be a number of milliseconds")
    return int(delay_ms)


def schedule(
    queue: str,
    timer_key: str,
    payload: Any,
    *,
    delay_ms: Optional[Any] = None,
    delay: Optional[Any] = None,
    partition: Optional[str] = None,
    txn: Optional[str] = None,
    payload_zstd: bool = False,
    reschedule: bool = False,
) -> Dict[str, Any]:
    """One ``schedule`` (or ``reschedule``) op.

    THEY ARE THE SAME UPSERT (§4.1), which is what makes a client retry after a
    crash safe by construction. ``attempts`` goes back to 0 and ``last_error``
    to NULL: a rescheduled timer is a NEW timer under an OLD name, and a freshly
    corrected payload must not inherit the budget spent by the one that was
    poisoning things.

    ``txn`` is minted here when absent rather than by the broker, and it is the
    reason it can be minted at all: ``absent`` on a later cancel MAY MEAN
    ALREADY DELIVERED (§4.4), the authority is the log, and the caller can only
    look for the timer's txn in the destination queue if it knows it.
    """
    if not isinstance(queue, str) or not queue:
        raise ValueError("queue must be a non-empty string")
    if not isinstance(timer_key, str) or not timer_key:
        raise ValueError("timer_key must be a non-empty string")

    op: Dict[str, Any] = {
        "op": "reschedule" if reschedule else "schedule",
        "queue": queue,
        "timerKey": timer_key,
        "payload": encode_payload(payload),
        "delayMs": delay_ms_of(delay_ms, delay),
        "txn": txn or generate_uuid(),
    }
    if partition is not None:
        if not isinstance(partition, str) or not partition:
            raise ValueError("partition, when given, must be a non-empty string")
        op["partition"] = partition
    if payload_zstd:
        op["payloadZstd"] = True
    return op


def cancel(queue: str, timer_key: str, *, txn: Optional[str] = None) -> Dict[str, Any]:
    """One ``cancel`` op, for the TRANSACTION rider.

    The standalone cancel does NOT use this: it goes to
    ``DELETE /api/v1/timers/:queue/*timerKey``, which is a separate route with a
    separate authorization class that is never blockable (§9.6). Inside a bundle
    there is no separate route to take -- being part of the transaction is the
    entire point of putting it there.
    """
    if not isinstance(queue, str) or not queue:
        raise ValueError("queue must be a non-empty string")
    if not isinstance(timer_key, str) or not timer_key:
        raise ValueError("timer_key must be a non-empty string")
    op: Dict[str, Any] = {"op": "cancel", "queue": queue, "timerKey": timer_key}
    if txn is not None:
        op["txn"] = txn
    return op
