"""
The framed pop answer: what a broker sends for a pop with ``frames=true``.
The messages come as the stored records, each still the zstd frame the
producer (or the broker) made, and are unpacked here.

    "QPF1" | u8 1 | u32 len, the answer's own keys as one JSON object
    u16 len, queue | u16 len, leaseId | u16 len, consumerGroup | u32 claims
    per claim:  u64 partitionId | u16 len, partition | u64 start | u64 end | u32 deliveryAttempt | u32 records
    per record: u64 base_offset | i64 created_at_us | u32 count | u8 codec (0 raw, 1 zstd) | u32 len | bytes

All integers little-endian. A record's bytes are ``count`` frames:

    u32 body_len | u8 flags | u8[16] message id | [u8[16] trace id, flag 1]
    | u16 len, transactionId | [u16 len, producer, flag 2]
    | [u16 len, headers, flag 8] | payload

Message i of a record has offset base_offset+i and belongs to the delivery
when that lies in the claim's start..=end. The JSON object in front holds what
the JSON answer has beside its messages: the conflation echo and the
autopilot's choice.

The broker answers JSON instead whenever a record holds headers or links
(catalogue version 8), as it does for encrypted ones; the headers flag is read
here all the same, and a link (flag 16) is refused like an encrypted frame.
"""

import json
import struct
import time
import uuid
from typing import Any, Dict, List

from . import zstd

POP_FRAMES_CONTENT_TYPE = "application/x-queen-pop"

_MAGIC = b"QPF1"
_FLAG_TRACE = 1
_FLAG_PRODUCER = 2
_FLAG_ENCRYPTED = 4
_FLAG_HEADERS = 8
_FLAG_LINK = 16

_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")
_I64 = struct.Struct("<q")


def _damaged(what: str) -> ValueError:
    return ValueError(f"framed pop answer: {what}")


class _In:
    """Reads little-endian fields off the answer; a read past the end raises."""

    __slots__ = ("data", "at", "short")

    def __init__(self, data: bytes, short: str = "truncated") -> None:
        self.data = data
        self.at = 0
        self.short = short

    def take(self, length: int) -> bytes:
        end = self.at + length
        if end > len(self.data):
            raise _damaged(self.short)
        piece = self.data[self.at : end]
        self.at = end
        return piece

    def _number(self, layout: struct.Struct) -> int:
        at = self.at
        if at + layout.size > len(self.data):
            raise _damaged(self.short)
        self.at = at + layout.size
        return layout.unpack_from(self.data, at)[0]  # type: ignore[no-any-return]

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return self._number(_U16)

    def u32(self) -> int:
        return self._number(_U32)

    def u64(self) -> int:
        return self._number(_U64)

    def i64(self) -> int:
        return self._number(_I64)

    def text(self) -> str:
        return self.take(self.u16()).decode("utf-8")

    def rest(self) -> bytes:
        return self.data[self.at :]


def is_pop_frames(data: bytes) -> bool:
    """Whether a pop answer's bytes are the framed one."""
    return len(data) >= len(_MAGIC) + 1 and data[: len(_MAGIC)] == _MAGIC


def _iso(created_at_us: int) -> str:
    """Microseconds since the epoch as the broker writes ``createdAt``."""
    seconds, micros = divmod(created_at_us, 1_000_000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{micros // 1000:03d}Z"


def decode_headers(block: bytes) -> Dict[str, str]:
    """A frame's headers block (``u16 count | (u8 len, name | u16 len, value) x
    count``) as the dict the JSON answer has."""
    reader = _In(block, "a headers block is malformed")
    headers: Dict[str, str] = {}
    for _ in range(reader.u16()):
        name = reader.take(reader.u8()).decode("utf-8")
        headers[name] = reader.text()
    if reader.rest():
        raise _damaged("a headers block is malformed")
    return headers


def decode_pop_frames(data: bytes) -> Dict[str, Any]:
    """Unpack a framed pop answer into the dict the JSON answer parses to::

        {"success", "queue", "partition", "partitionId", "leaseId", "consumerGroup",
         "messages": [{"id", "transactionId", "traceId", "data", "producerSub",
                       ["headers",] "createdAt", "partitionId", "partition",
                       "leaseId", "consumerGroup", "deliveryAttempt", "offset"}],
         "partitionsClaimed", ...the answer's own keys}

    The messages are in delivery order. Everything that reads a pop answer (the
    conflation check, the autopilot's choice, the consumer loop) reads this
    dict as it reads the JSON one. Raises ValueError on an answer that is not
    whole.
    """
    if not is_pop_frames(data):
        raise _damaged("not a framed pop answer")
    answer = _In(data)
    answer.take(len(_MAGIC))
    version = answer.u8()
    own: Any = None
    if version == 1:
        text = answer.take(answer.u32())
        if text:
            try:
                own = json.loads(text)
            except ValueError as error:
                raise _damaged(f"its own keys: {error}") from None
    elif version != 0:
        raise _damaged(f"unknown version {version}")

    queue = answer.text()
    lease_id = answer.text()
    consumer_group = answer.text()
    claims = answer.u32()
    messages: List[Dict[str, Any]] = []
    first_partition = ""
    first_partition_id = ""

    for claim in range(claims):
        partition_id = str(answer.u64())
        partition = answer.text()
        start = answer.u64()
        end = answer.u64()
        delivery_attempt = answer.u32()
        records = answer.u32()
        if claim == 0:
            first_partition, first_partition_id = partition, partition_id
        for _ in range(records):
            base = answer.u64()
            created_at = _iso(answer.i64())
            count = answer.u32()
            codec = answer.u8()
            raw = answer.take(answer.u32())
            if codec == 1:
                try:
                    raw = zstd.decompress(raw)
                except RuntimeError:
                    raise
                except Exception as error:
                    raise _damaged(f"zstd: {error}") from None
            elif codec != 0:
                raise _damaged(f"unknown codec {codec}")
            frames = _In(raw, f"a record is shorter than its {count} frames")
            for i in range(count):
                frame = _In(frames.take(frames.u32()), "a frame is malformed")
                flags = frame.u8()
                message_id = frame.take(16)
                trace = frame.take(16) if flags & _FLAG_TRACE else None
                transaction_id = frame.text()
                producer_sub = frame.text() if flags & _FLAG_PRODUCER else None
                block = frame.take(frame.u16()) if flags & _FLAG_HEADERS else None
                if flags & _FLAG_ENCRYPTED:
                    raise _damaged("a frame is encrypted")
                if flags & _FLAG_LINK:
                    raise _damaged("a frame is a link")
                offset = base + i
                if offset < start or offset > end:
                    continue
                payload = frame.rest()
                message: Dict[str, Any] = {
                    "id": str(uuid.UUID(bytes=message_id)),
                    "transactionId": transaction_id,
                    "traceId": str(uuid.UUID(bytes=trace)) if trace is not None else None,
                    "data": json.loads(payload) if payload else None,
                    "producerSub": producer_sub,
                }
                # Only when the message has headers, after producerSub: where
                # the JSON answer puts them.
                if block is not None:
                    message["headers"] = decode_headers(block)
                message.update(
                    {
                        "createdAt": created_at,
                        "partitionId": partition_id,
                        "partition": partition,
                        "leaseId": lease_id,
                        "consumerGroup": consumer_group,
                        "deliveryAttempt": delivery_attempt,
                        "offset": offset,
                    }
                )
                messages.append(message)

    result: Dict[str, Any] = {
        "success": True,
        "queue": queue,
        "partition": first_partition,
        "partitionId": first_partition_id,
        "leaseId": lease_id,
        "consumerGroup": consumer_group,
        "messages": messages,
        "partitionsClaimed": claims,
    }
    if isinstance(own, dict):
        # "frames" only says the answer is this one; the JSON answer has no such key.
        result.update((key, value) for key, value in own.items() if key != "frames")
    return result
