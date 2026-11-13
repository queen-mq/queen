"""
The binary push body: an alternative to the JSON body of POST /api/v1/push.

    "QPB1" | u8 flags | [u32 raw_len, when zstd] | body
    body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
           | n x ([u16 len, transactionId, when flag 4] | u32 len, payload)

All integers little-endian. flags: 1 = the body is one zstd frame of raw_len
bytes, 2 = answer with the counts only, 4 = every item carries its transaction
id (empty = the broker mints it). No trace id, no headers. Each payload is one
JSON document.

With flag 8 (encode_push_frames) a group is the messages as the broker stores
them, compressed here once:

    body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z
    z:     one zstd frame of n frames: u32 body_len | u8 flags | u8[16] message id
           | u16 len, transactionId | [u16 len, headers, flag 8] | payload

A frame's flags are 0, or 8 when the message has headers (catalogue version 8):
the block is ``u16 count | (u8 len, name | u16 len, value) x count``, sorted by
name (encode_headers).

The message id is a version-7 UUID minted by the client; a message without its
own transaction id uses the id's text. The broker checks every frame and keeps
the compressed bytes as they are, so it does not compress them again.

A broker that does not know this body answers 400; a client sends it only when
``binary_push`` is set, so by default a push is the JSON request every broker
knows.

The binary transaction body (encode_transaction_frames), an alternative to the
JSON body of POST /api/v1/transaction, carries the pushes the same way:

    "QTB1" | u8 flags | u32 len, head | u32 groups | the groups, as with flag 8 above

head is the JSON transaction body without its push operations: the acks (and
links) in ``operations``, ``requiredLeases``, ``kv``, ``timers``. flags: 2 = the answer
leaves out the pushes' and the acks' results and counts them (``pushed``,
``duplicate``, ``acked``); the riders' results stay. In the answer the head's
operations come first and the groups' messages after them, in the order
written; the riders follow, as in the JSON body.
"""

import json
import os
import struct
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

from . import zstd

PUSH_BINARY_CONTENT_TYPE = "application/x-queen-push"

_MAGIC = b"QPB1"
_TXN_MAGIC = b"QTB1"
_FLAG_ZSTD = 1
_FLAG_COUNTS = 2
_FLAG_TXN = 4
_FLAG_FRAMES = 8
# A frame's flag: the message's headers follow its transaction id.
_FRAME_HEADERS = 8

_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")


@dataclass
class PushBatch:
    """One partition's messages for encode_push_binary and encode_push_frames."""

    queue: str
    partition: str
    # Each one JSON document, UTF-8.
    payloads: List[bytes]
    # None, or one transaction id per payload ("" = the broker mints it).
    transaction_ids: Optional[List[str]] = None
    # None, or one headers block per payload (encode_headers; None or b"" =
    # none). Frames only: encode_push_frames and encode_transaction_frames.
    headers: Optional[List[Optional[bytes]]] = None


class _TooLong(ValueError):
    """A name or a transaction id does not fit its 16-bit length."""


def encode_headers(headers: Mapping[str, str]) -> bytes:
    """A message's headers as the frame's block: ``u16 count | (u8 len, name |
    u16 len, value) x count``, sorted by name, as the broker stores it. The
    names are checked by the caller (validate_headers): ASCII, at most 64."""
    out = bytearray(_U16.pack(len(headers)))
    for name in sorted(headers):
        key = name.encode("utf-8")
        value = headers[name].encode("utf-8")
        if len(key) > 0xFF:
            raise _TooLong("header name longer than 255 bytes")
        out.append(len(key))
        out += key
        if len(value) > 0xFFFF:
            raise _TooLong("header value longer than 65535 bytes")
        out += _U16.pack(len(value))
        out += value
    if len(out) > 0xFFFF:
        raise _TooLong("headers longer than 65535 bytes")
    return bytes(out)


def _text16(out: bytearray, text: str, what: str) -> None:
    data = text.encode("utf-8")
    if len(data) > 0xFFFF:
        raise _TooLong(f"{what} longer than 65535 bytes")
    out += _U16.pack(len(data))
    out += data


def _check(batch: PushBatch) -> None:
    if batch.transaction_ids is not None and len(batch.transaction_ids) != len(batch.payloads):
        raise ValueError(
            f"{len(batch.transaction_ids)} transaction ids for {len(batch.payloads)} payloads"
        )
    if batch.headers is not None and len(batch.headers) != len(batch.payloads):
        raise ValueError(f"{len(batch.headers)} headers blocks for {len(batch.payloads)} payloads")


def encode_push_binary(
    batches: List[PushBatch], *, compress: bool = False, counts: bool = False
) -> bytes:
    """Encode batches as a binary push body.

    compress: the body is zstd-compressed. counts: the broker answers
    ``{"count", "queued", "duplicate", "error"}`` instead of one object per
    message. This body has no place for headers: encode_push_frames has.
    """
    with_txn = any(batch.transaction_ids is not None for batch in batches)
    flags = (_FLAG_COUNTS if counts else 0) | (_FLAG_TXN if with_txn else 0)
    body = bytearray(_U32.pack(len(batches)))
    for batch in batches:
        _check(batch)
        if batch.headers is not None and any(batch.headers):
            raise ValueError("the plain binary push body carries no headers: use encode_push_frames")
        _text16(body, batch.queue, "queue name")
        _text16(body, batch.partition, "partition name")
        body += _U32.pack(len(batch.payloads))
        for i, payload in enumerate(batch.payloads):
            if with_txn:
                txn = batch.transaction_ids[i] if batch.transaction_ids is not None else ""
                _text16(body, txn, "transaction id")
            body += _U32.pack(len(payload))
            body += payload
    if not compress:
        return _MAGIC + bytes([flags]) + bytes(body)
    return _MAGIC + bytes([flags | _FLAG_ZSTD]) + _U32.pack(len(body)) + zstd.compress(bytes(body))


def encode_push_frames(batches: List[PushBatch], *, counts: bool = False) -> bytes:
    """Encode batches as a binary push body whose groups are the messages
    framed as the broker stores them and compressed once, here. counts: the
    broker answers with the counts instead of one object per message."""
    out = bytearray(_MAGIC)
    out.append(_FLAG_FRAMES | (_FLAG_COUNTS if counts else 0))
    return _append_frame_groups(out, batches)


def encode_transaction_frames(
    head: Optional[bytes], batches: List[PushBatch], *, counts: bool = False
) -> bytes:
    """Encode a binary transaction body: head, the JSON transaction body
    without its push operations (None or empty for none), then batches as
    groups of messages framed as the broker stores them and compressed once,
    here. counts: the broker counts the pushes and the acks in its answer
    instead of rendering one result for each."""
    head = head or b""
    out = bytearray(_TXN_MAGIC)
    out.append(_FLAG_COUNTS if counts else 0)
    out += _U32.pack(len(head))
    out += head
    return _append_frame_groups(out, batches)


def _append_frame_groups(out: bytearray, batches: List[PushBatch]) -> bytes:
    """Appends ``u32 groups | per group: u16 len, queue | u16 len, partition |
    u32 n | u32 z_len | z`` to out: each batch's messages framed as the broker
    stores them, with a version-7 id minted here, as one zstd frame."""
    out += _U32.pack(len(batches))
    for batch in batches:
        _check(batch)
        frames = bytearray()
        # A version-7 UUID per message: the time in milliseconds, then random
        # bits under the version and variant marks.
        stamp = (time.time_ns() // 1_000_000).to_bytes(6, "big")
        noise = os.urandom(10 * len(batch.payloads))
        for i, payload in enumerate(batch.payloads):
            tail = bytearray(noise[10 * i : 10 * i + 10])
            tail[0] = 0x70 | (tail[0] & 0x0F)
            tail[2] = 0x80 | (tail[2] & 0x3F)
            message_id = stamp + bytes(tail)
            txn = batch.transaction_ids[i] if batch.transaction_ids is not None else ""
            txn_bytes = (txn or str(uuid.UUID(bytes=message_id))).encode("utf-8")
            if len(txn_bytes) > 0xFFFF:
                raise _TooLong("transaction id longer than 65535 bytes")
            # A block of no headers is written as none, as the broker does.
            block = batch.headers[i] if batch.headers is not None else None
            block = block if block and len(block) > 2 else None
            extra = 2 + len(block) if block is not None else 0
            frames += _U32.pack(1 + 16 + 2 + len(txn_bytes) + extra + len(payload))
            frames.append(_FRAME_HEADERS if block is not None else 0)
            frames += message_id
            frames += _U16.pack(len(txn_bytes))
            frames += txn_bytes
            if block is not None:
                frames += _U16.pack(len(block))
                frames += block
            frames += payload
        _text16(out, batch.queue, "queue name")
        _text16(out, batch.partition, "partition name")
        out += _U32.pack(len(batch.payloads))
        compressed = zstd.compress(bytes(frames))
        out += _U32.pack(len(compressed))
        out += compressed
    return bytes(out)


def binary_push_body(items: List[Dict[str, Any]], compress: bool) -> Optional[bytes]:
    """The items of a JSON push (``{queue, partition, payload, transactionId,
    headers}``) as a binary push body, or None when one of them cannot travel
    in it: a trace id, a transaction id that is not a string, a payload JSON
    does not carry, a name over 65535 bytes, headers without compress (the
    plain body has no place for them). The caller then sends the JSON body, and
    the broker answers that as it always did. Consecutive items of one
    partition share a group, so the order of the items is the order on the wire.

    compress: the messages framed and compressed here, kept by the broker as
    they are; headers travel in the frames.
    """
    if not items:
        return None
    batches: List[PushBatch] = []
    for item in items:
        if not _add_item(batches, item, frames=compress):
            return None
    try:
        return encode_push_frames(batches) if compress else encode_push_binary(batches)
    except _TooLong:
        # The JSON push says so in the broker's words.
        return None


def _add_item(batches: List[PushBatch], item: Any, *, frames: bool = True) -> bool:
    """Adds one pushed item to batches, in the last one when it is the same
    partition's. False when the item cannot travel in a binary body; headers
    travel only in frames."""
    if not isinstance(item, dict) or not isinstance(item.get("queue"), str):
        return False
    if item.get("traceId") is not None:
        return False
    txn = item.get("transactionId")
    if txn is None:
        txn = ""
    partition = item.get("partition")
    if partition is None:
        partition = "Default"
    if not isinstance(txn, str) or not isinstance(partition, str) or "payload" not in item:
        return False
    headers = item.get("headers")
    block: Optional[bytes] = None
    if headers:
        if not frames or not isinstance(headers, dict):
            return False
        try:
            block = encode_headers(headers)
        except (AttributeError, TypeError, ValueError):
            # Not a dict of strings, or too long: the JSON request says so.
            return False
    try:
        payload = json.dumps(
            item["payload"], ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError):
        return False
    last = batches[-1] if batches else None
    if last is None or last.queue != item["queue"] or last.partition != partition:
        last = PushBatch(item["queue"], partition, [], [], [])
        batches.append(last)
    last.payloads.append(payload)
    assert last.transaction_ids is not None and last.headers is not None
    last.transaction_ids.append(txn)
    last.headers.append(block)
    return True


def transaction_frames(request: Any) -> Optional[Tuple[bytes, Optional[List[int]]]]:
    """A JSON transaction body (``{operations, requiredLeases, kv, timers}``)
    as the binary one, when its pushes can travel in it: there is at least one,
    and none carries a trace id. A push's headers travel in its frame.
    Everything but the pushes (acks, links) stays in the JSON head;
    consecutive pushed items of one partition share a group, so the pushes
    keep their order. None otherwise: the caller then sends the JSON body.

    Returns ``(body, order)``. order is None when the broker's answer lists the
    operations as the caller added them (every other operation before every
    push). Otherwise ``order[k]`` is the place, among the caller's operations,
    of the k-th result the broker sends: it lists the head's operations first,
    then the pushes.
    """
    if not isinstance(request, dict) or not isinstance(request.get("operations"), list):
        return None
    head_operations: List[Any] = []
    batches: List[PushBatch] = []
    head_at: List[int] = []
    push_at: List[int] = []
    flat = 0
    for operation in request["operations"]:
        if not isinstance(operation, dict) or operation.get("type") != "push":
            head_operations.append(operation)
            head_at.append(flat)
            flat += 1
            continue
        # A push names its items, or is one item itself.
        items = operation.get("items")
        for item in items if isinstance(items, list) else [operation]:
            if not _add_item(batches, item):
                return None
            push_at.append(flat)
            flat += 1
    if not batches:
        return None
    head = {**request, "operations": head_operations}
    try:
        head_json = json.dumps(head, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        body = encode_transaction_frames(head_json.encode("utf-8"), batches)
    except (TypeError, ValueError):
        # A name too long for the body, or a head JSON does not carry: the JSON
        # request says so as it always did.
        return None
    order = head_at + push_at
    return body, (None if order == list(range(len(order))) else order)


def restore_result_order(answer: Any, order: Optional[List[int]]) -> None:
    """Puts the results of a binary transaction back in the order the caller
    added its operations in, and gives each its own index again. The riders'
    results, after the operations', are already in place."""
    if order is None or not isinstance(answer, dict):
        return
    results = answer.get("results")
    if not isinstance(results, list) or len(results) < len(order):
        return
    restored = list(results)
    for k, at in enumerate(order):
        result = results[k]
        restored[at] = {**result, "index": at} if isinstance(result, dict) else result
    answer["results"] = restored
