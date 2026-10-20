"""
Reads by offset and partition discovery, as two queue methods::

    read = await queen.queue("orders").fetch(["customer-42", "customer-7"])
    read["customer-42"]["records"]   # [{"offset", "transactionId", "payload", "ts"}, ...]

    changed = await queen.queue("orders").changed_since(since)
    changed["partitions"], changed["safeTime"]

``fetch`` is ``POST /api/v1/fetch`` and ``changed_since`` is
``POST /api/v1/partitions/changed``. Neither takes a lease, moves a cursor or
needs an ack: two readers asking the same question get the same answer, and no
consumer group notices them. Each is served from the answering node's own
applied state.

Both do the loops a caller of the raw routes has to write:

- fetch asks for at most 1000 partitions per call, and asks again from one past
  the last record until each partition reaches the high watermark its first
  answer reported. A partition whose start was deleted by retention is read
  from its oldest kept record (an offset below ``logStartOffset`` answers
  OFFSET_OUT_OF_RANGE and no records, so a loop that does not restart there
  silently reads nothing).
- changed_since follows the page cursor (``next``) until the broker says the
  pass is over, and answers the smallest ``safeTime`` of its pages.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

from typing_extensions import TypedDict

# The broker refuses more than 1024 entries in one call; 1000 leaves room.
ENTRIES_PER_CALL = 1000

# Bytes of payload one call may carry: each entry's budget and the ceiling of
# the whole call. 8 MiB is the most the broker reads for one entry.
DEFAULT_MAX_BYTES = 8 * 1024 * 1024

# Partitions per discovery page: the broker's default and its ceiling.
CHANGED_PAGE = 1000

OFFSET_OUT_OF_RANGE = "OFFSET_OUT_OF_RANGE"
UNKNOWN_TOPIC_OR_PARTITION = "UNKNOWN_TOPIC_OR_PARTITION"


class FetchedRecord(TypedDict):
    """One record: its offset, the transactionId it was pushed with, its payload
    as pushed, and the time the push that wrote it was stored."""

    offset: int
    transactionId: str
    payload: Any
    ts: str


class FetchedPartition(TypedDict):
    """What ``fetch`` read of one partition. ``highWatermark`` is the next offset
    the partition will assign, ``logStartOffset`` the oldest one retention kept,
    ``nextOffset`` where to start next time to read only what is new."""

    partition: str
    records: List[FetchedRecord]
    highWatermark: int
    logStartOffset: int
    nextOffset: int


class ChangedPartition(TypedDict):
    """One partition of a discovery answer. ``lastOffset`` is the offset of its
    last record (-1 when none)."""

    name: str
    id: Optional[str]
    lastOffset: int
    logStart: int
    lastWriteAt: str


class ChangedPartitions(TypedDict):
    partitions: List[ChangedPartition]
    safeTime: Optional[str]


def _read_affinity(queue: str) -> str:
    # Every call of one read goes to the same node: a follower answers from
    # what it has applied, so two nodes can disagree about where a partition
    # ends.
    return f"{queue}:*:__READ__"


def _partition_names(partitions: Union[str, Iterable[str]]) -> List[str]:
    items = [partitions] if isinstance(partitions, str) else list(partitions)
    names: List[str] = []
    seen = set()
    for p in items:
        name = str(p) if isinstance(p, int) and not isinstance(p, bool) else p
        if not isinstance(name, str) or not name:
            raise ValueError(f"fetch: a partition name must be a non-empty string, got {p!r}")
        if name not in seen:
            seen.add(name)
            names.append(name)
    return names


def _start_offsets(offsets: Optional[Mapping[str, int]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for name, offset in (offsets or {}).items():
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError(f"fetch: offsets[{name!r}] must be a non-negative integer, got {offset!r}")
        out[str(name)] = offset
    return out


async def fetch_partitions(
    http_client: Any,
    queue: Optional[str],
    partitions: Union[str, Iterable[str]],
    *,
    offsets: Optional[Mapping[str, int]] = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Dict[str, FetchedPartition]:
    """Every record of ``partitions``, read by offset: from each partition's
    offset in ``offsets`` (default 0, the oldest record still kept) up to where
    the partition ended when it was first read.

    Answers a dict of partition name to :class:`FetchedPartition`, in the order
    the partitions were named. A partition nobody has written, or of a queue
    that does not exist, answers no records. So does an offset past the end of
    the partition (``nextOffset`` above ``highWatermark`` says so: the partition
    was deleted and written again).
    """
    if not queue:
        raise ValueError("Queue name is required for fetch")
    names = _partition_names(partitions)
    starts = _start_offsets(offsets)
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
        raise ValueError(f"fetch: max_bytes must be a positive integer, got {max_bytes!r}")

    state: Dict[str, Dict[str, Any]] = {
        name: {
            "partition": name,
            "records": [],
            "highWatermark": 0,
            "logStartOffset": 0,
            "nextOffset": starts.get(name, 0),
            # The high watermark of the first answer: the read ends there.
            "end": None,
        }
        for name in names
    }

    pending = names
    while pending:
        again: List[str] = []
        for i in range(0, len(pending), ENTRIES_PER_CALL):
            chunk = pending[i : i + ENTRIES_PER_CALL]
            body = {
                "entries": [
                    {
                        "queue": queue,
                        "partition": name,
                        "offset": state[name]["nextOffset"],
                        "maxBytes": max_bytes,
                    }
                    for name in chunk
                ],
                "maxBytes": max_bytes,
            }
            answer = await http_client.post("/api/v1/fetch", body, None, _read_affinity(queue))
            entries = answer.get("entries") if isinstance(answer, dict) else None
            if not isinstance(entries, list) or len(entries) != len(chunk):
                got = len(entries) if isinstance(entries, list) else "no"
                raise RuntimeError(f"fetch {queue}: the broker answered {got} entries for {len(chunk)} asked")

            delivered = 0
            # Entries that came back empty while short of their end: the call's
            # byte ceiling was spent by the entries before them.
            starved: List[str] = []
            for name, entry in zip(chunk, entries):
                s = state[name]
                if entry.get("highWatermark") is not None:
                    s["highWatermark"] = entry["highWatermark"]
                if entry.get("logStartOffset") is not None:
                    s["logStartOffset"] = entry["logStartOffset"]
                error = entry.get("error")
                if error:
                    if error == OFFSET_OUT_OF_RANGE and s["nextOffset"] < s["logStartOffset"]:
                        # Retention deleted what was asked for: go on from the
                        # oldest kept.
                        s["nextOffset"] = s["logStartOffset"]
                        if s["end"] is None:
                            s["end"] = s["highWatermark"]
                        if s["nextOffset"] < s["end"]:
                            again.append(name)
                        continue
                    if error in (OFFSET_OUT_OF_RANGE, UNKNOWN_TOPIC_OR_PARTITION):
                        continue
                    raise RuntimeError(f"fetch {queue}/{name}: the broker answered {error}")
                if s["end"] is None:
                    s["end"] = s["highWatermark"]
                records = entry.get("records") or []
                if records:
                    delivered += len(records)
                    for r in records:
                        s["records"].append(
                            {
                                "offset": r.get("offset"),
                                "transactionId": r.get("transactionId"),
                                "payload": r.get("payload"),
                                "ts": r.get("ts"),
                            }
                        )
                    s["nextOffset"] = records[-1]["offset"] + 1
                    if s["nextOffset"] < s["end"]:
                        again.append(name)
                elif s["nextOffset"] < s["end"]:
                    starved.append(name)
            # An empty entry is asked again only when its call read something:
            # a call that read nothing at all would answer the same next time.
            if delivered > 0:
                again.extend(starved)
        pending = again

    out: Dict[str, FetchedPartition] = {}
    for name in names:
        s = state[name]
        out[name] = {
            "partition": s["partition"],
            "records": s["records"],
            "highWatermark": s["highWatermark"],
            "logStartOffset": s["logStartOffset"],
            "nextOffset": s["nextOffset"],
        }
    return out


def _since_iso(since: Union[None, str, datetime]) -> Optional[str]:
    if since is None:
        return None
    if isinstance(since, datetime):
        # A naive datetime is read as UTC, as the broker reads a time with no
        # offset.
        aware = since if since.tzinfo is not None else since.replace(tzinfo=timezone.utc)
        return aware.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(since, str) and since:
        return since
    raise ValueError(f"changed_since: since must be a datetime or an ISO-8601 string, got {since!r}")


async def partitions_changed_since(
    http_client: Any,
    queue: Optional[str],
    since: Union[None, str, datetime] = None,
) -> ChangedPartitions:
    """The partitions of the queue written at or after ``since``, in the order
    they were created, and the ``safeTime`` of the answer.

    ``since`` is a datetime (naive = UTC) or an ISO-8601 string; None lists
    every partition of the queue. A queue that does not exist has none.

    ``safeTime`` is the broker's time up to which every write is counted in this
    answer: pass it as the next ``since`` to see only what changed after this
    call. It comes from the broker's clock, never compare it with the local one.
    """
    if not queue:
        raise ValueError("Queue name is required for changed_since")
    start = _since_iso(since)
    partitions: List[ChangedPartition] = []
    safe_time: Optional[str] = None
    after: Optional[str] = None
    while True:
        entry: Dict[str, Any] = {"queue": queue, "limit": CHANGED_PAGE}
        if start is not None:
            entry["since"] = start
        if after is not None:
            entry["after"] = after
        answer = await http_client.post(
            "/api/v1/partitions/changed", {"entries": [entry]}, None, _read_affinity(queue)
        )
        answer = answer if isinstance(answer, dict) else {}
        # Pages served by different nodes: the smallest is the one that holds
        # for all of them. The broker spells every one the same way, so the
        # strings compare in time order.
        page_safe = answer.get("safeTime")
        if isinstance(page_safe, str) and (safe_time is None or page_safe < safe_time):
            safe_time = page_safe
        results = answer.get("entries") or []
        if not results:
            raise RuntimeError(f"changed_since {queue}: the broker answered no entry")
        result = results[0]
        error = result.get("error")
        if error == UNKNOWN_TOPIC_OR_PARTITION:
            break
        if error:
            raise RuntimeError(f"changed_since {queue}: the broker answered {error}")
        for p in result.get("partitions") or []:
            partitions.append(
                {
                    "name": p.get("name"),
                    "id": p.get("id"),
                    "lastOffset": p.get("lastOffset"),
                    "logStart": p.get("logStart"),
                    "lastWriteAt": p.get("lastWriteAt"),
                }
            )
        after = result.get("next")
        if not after:
            break
    return {"partitions": partitions, "safeTime": safe_time}
