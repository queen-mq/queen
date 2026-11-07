"""
FeedRunner — runs a Stream whose source is the tenant feed
(``Stream.from_feed(client, ...)``). Port of the JS SDK's
streams/runtime/FeedRunner.js, with the same KV formats, ids and rules, so
one stream moves between SDKs.

A cycle reads one page of the feed from the runner's position and runs the
chain on each message, in log order (feed/pipeline.py). What the cycle
produced commits in ONE transaction: the pushes to the sink queues and the new
position, a KV key (``queen-feed`` / query_id) written with ``expect`` on its
version. Exactly once into Queen; ``.foreach`` effects run before the position
moves, so they are at least once.

Two positions, not a copy of the state at every commit: the record holds the
committed position and the position of the last SNAPSHOT (feed/snapshot.py),
named by the commit that moves the position, in the same transaction. A
restart loads the snapshot and reads the feed again from the snapshot's
position to the committed one, sending nothing.

One runner at a time: each query_id has a lock (``queen-feed/<queryId>``); any
other process with the same query_id waits on it and takes over when the first
stops or dies. Each commit is guarded by the lock (``exclusive=False`` turns
this off: then the position's version alone decides which process commits).

A message whose function raises is tried again (``retries``, 3), each time
against the state from before it. Then ``on_poison`` says what happens:
'stop' (the default) commits what came before it and stops; 'skip' leaves it
out; a queue (``client.queue('feed-dlq')``) receives it.

Closes are counted (``units``): a long close (millions of windows that end at
one time) commits in pieces, and the record's ``units`` says how far it got.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import re
import sys
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional

from ...errors import LockNotHeldError, QueenError
from ...feed.feed import condition_holds
from ...utils import logger
from ..operators.sink_op import _extract_queue_name, payload_of
from ..util.tasks import spawn
from ..util.js import (
    UNDEFINED,
    call_user,
    js_date_parse,
    js_is_finite,
    js_is_integer,
    js_iso,
    js_json_dumps,
    js_normalize,
    js_str,
    now_ms,
    nullish,
    parse_duration_ms,
)
from .feed.pipeline import Pipeline
from .feed.patterns import snapshot_of
from .feed.about import about_key, clip, clip_text, describe_feed_stream
from .feed.query import StateServer, query_queue_name
from .feed.snapshot import (
    DEFAULT_CHUNK_CHARS,
    DEFAULT_PIECE_BYTES,
    MAX_LAYERS,
    decode_snapshot,
    delete_keys,
    layer_keys,
    layers_of,
    new_snapshot_id,
    pointer_of,
    read_chunks,
    read_layer,
    stray_chunk_keys,
    write_layer,
)
from .feed.state import shard_of

POSITION_NAMESPACE = "queen-feed"
POSITION_COMMIT_MILLIS = 5000
# The error a `stopped` record keeps, at most.
STOP_ERROR_MAX_CHARS = 500
COMMIT_EVERY_MILLIS = 1000
MAX_PENDING_OUTPUTS = 5000
MAX_BACKOFF_MILLIS = 10000
LOCK_TTL = "15s"
RETRIES = 3
RETRY_DELAY_MILLIS = 100
SNAPSHOT_EVERY_MILLIS = 60000
SNAPSHOT_MAX_ENTRIES = 200000
# Skips (messages the poison policy left out) remembered since the snapshot,
# for a replay to leave out the same ones; past this many, a snapshot is due.
MAX_SKIPS = 1000
# A replay waits this long, at most, for a follower to apply the committed position.
REPLAY_CATCH_UP_MILLIS = 60000
# A quiet feed with a window or deadline due: at most one clock-moving write
# this often, and a short read after it (the long poll would not wake for it).
TICK_MIN_MILLIS = 500
TICK_READ_WAIT_MILLIS = 100
# The broker's clock moves only with writes. When none came for this long and
# a deadline waits, one write tells this process the broker's time.
CLOCK_SYNC_MILLIS = 5000
QUERY_ID_MAX_BYTES = 200
LOCK_NOT_HELD = "LOCK_NOT_HELD"

_INF = math.inf
_CONTROL_OR_HASH = re.compile("[\u0000-\u001f\u007f#]")


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "surrogatepass")).hexdigest()


class FeedRunnerError(QueenError):
    """A feed stream that cannot run (``code``); ``fatal`` ones stop the runner."""

    def __init__(self, message: str, *, code: Optional[str] = None, fatal: bool = False, **attrs: Any) -> None:
        super().__init__(message)
        self.code = code
        self.fatal = fatal
        for k, v in attrs.items():
            setattr(self, k, v)


class _Stopping(Exception):
    stopping = True


def output_prefix(query_id: str) -> str:
    """The prefix of every transaction id a query writes, every shard of it: its reads leave these out."""
    return f"fd-{_sha(f'queen-feed' + chr(10) + query_id)[:8]}-"


def feed_output_id(query_id: str, entry: Dict[str, Any], i: int, epoch: int = 0, scope: Optional[str] = None) -> str:
    """The transaction id of the ``i``-th output of one feed entry: the same on every retry."""
    sc = query_id if scope is None else scope
    text = "\n".join(
        js_str(x)
        for x in (sc, epoch, entry.get("position", UNDEFINED), entry.get("queue", UNDEFINED), entry.get("partition", UNDEFINED), entry.get("offset", UNDEFINED), i)
    )
    return f"{output_prefix(query_id)}{_sha(text)[:40]}"


def shard_id(query_id: str, shard: Optional[Dict[str, int]]) -> str:
    """The KV key and lock name of one shard of a query (or of the query, unsharded)."""
    return f"{query_id}#shard{shard['index']}of{shard['count']}" if shard else query_id


def lock_name(query_id: str, shard: Optional[Dict[str, int]]) -> str:
    return f"queen-feed/{query_id}/shard-{shard['index']}-of-{shard['count']}" if shard else f"queen-feed/{query_id}"


def _require_query_id(query_id: Any) -> str:
    if (
        not isinstance(query_id, str)
        or len(query_id) == 0
        or len(query_id.encode("utf-8", "surrogatepass")) > QUERY_ID_MAX_BYTES
        or _CONTROL_OR_HASH.search(query_id)
    ):
        raise ValueError(
            f"Stream.fromFeed: queryId is a non-empty string of at most {QUERY_ID_MAX_BYTES} bytes, without '#' "
            f"and control characters (it names a KV key and a lock), got {js_json_dumps(query_id) if isinstance(query_id, (str, int, float, type(None))) else repr(query_id)}"
        )
    return query_id


def _require_shard(shard: Any) -> Optional[Dict[str, int]]:
    if shard is None or shard is UNDEFINED:
        return None
    index = shard.get("index") if isinstance(shard, dict) else None
    count = shard.get("count") if isinstance(shard, dict) else None
    if not js_is_integer(count) or count < 1 or count > 4096 or not js_is_integer(index) or index < 0 or index >= count:
        raise ValueError(
            "Stream.fromFeed: shard is { index, count }, count from 1 to 4096 and index from 0 to count - 1, got "
            f"{js_json_dumps(shard) if isinstance(shard, dict) else repr(shard)}"
        )
    return None if count == 1 else {"index": int(index), "count": int(count)}


def _version_of(result: Any, key: str) -> Optional[int]:
    for r in (result.get("results") if isinstance(result, dict) else None) or []:
        if isinstance(r, dict) and r.get("type") == "kv" and r.get("key") == key and js_is_integer(r.get("version")):
            return int(r["version"])
    return None


def _ts_of(entry: Dict[str, Any]) -> Any:
    ts = entry.get("ts")
    if not ts:
        ts = entry.get("createdAt")
    t = js_date_parse(ts if isinstance(ts, str) and ts else "")
    return 0 if t is None else t


async def _sleep(ms: Any, signal: Optional[asyncio.Event]) -> None:
    if signal is not None and signal.is_set():
        return
    seconds = max(0.0, float(ms) / 1000.0)
    if signal is None:
        await asyncio.sleep(seconds)
        return
    try:
        await asyncio.wait_for(signal.wait(), seconds)
    except asyncio.TimeoutError:
        pass


def _glob(pattern: str) -> "re.Pattern[str]":
    out = []
    for ch in pattern:
        if ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return re.compile("^" + "".join(out) + "\\Z")


def _as_list(v: Any) -> Optional[List[Any]]:
    if v is None or v is UNDEFINED:
        return None
    lst = list(v) if isinstance(v, (list, tuple)) else [v]
    return lst if lst else None


def _poison_target(on_poison: Any) -> Dict[str, Any]:
    if on_poison is None or on_poison is UNDEFINED or on_poison == "stop":
        return {"kind": "stop"}
    if on_poison == "skip":
        return {"kind": "skip"}
    name = None
    if isinstance(on_poison, str):
        name = on_poison
    elif not isinstance(on_poison, (bool, int, float, list, tuple)):
        try:
            name = _extract_queue_name(on_poison)
        except ValueError:
            name = None
    if not name:
        raise ValueError(
            "Stream.fromFeed: onPoison is 'stop', 'skip' or a queue for the messages that fail (queen.queue('feed-dlq'))"
        )
    return {"kind": "deadLetter", "queue": name}


def lock_ttl_seconds_of(lock_ttl: Any) -> int:
    """``lockTtl`` as the lock takes it: a duration string ('15s'), seconds, or a timedelta."""
    if isinstance(lock_ttl, str):
        return math.ceil(parse_duration_ms(lock_ttl) / 1000)
    if isinstance(lock_ttl, timedelta):
        return math.ceil(lock_ttl.total_seconds())
    if js_is_integer(lock_ttl) and lock_ttl > 0:
        return int(lock_ttl)
    raise ValueError("Stream.fromFeed: lock_ttl is a duration string such as '15s', a whole number of seconds, or a timedelta")


def _fresh_pending() -> Dict[str, Any]:
    # readTo / caughtUp: the feed time `next` is at, and whether the read was caught up there.
    return {"outputs": [], "next": None, "work": 0, "effects": 0, "emitSeq": 0, "readTo": None, "caughtUp": None}


_CAMEL = re.compile(r"(?<!^)(?=[A-Z])")

# run() options: the JS name -> the Python one.
_OPTION_NAMES = {
    "queryId": "query_id",
    "onPoison": "on_poison",
    "retryDelayMillis": "retry_delay_millis",
    "lockTtl": "lock_ttl",
    "shardBy": "shard_by",
    "snapshotEveryMillis": "snapshot_every_millis",
    "snapshotMaxEntries": "snapshot_max_entries",
    "snapshotChunkChars": "snapshot_chunk_chars",
    "snapshotPieceBytes": "snapshot_piece_bytes",
    "commitEveryMillis": "commit_every_millis",
    "positionCommitMillis": "position_commit_millis",
    "maxPendingOutputs": "max_pending_outputs",
    "waitMillis": "wait_millis",
    "maxRecords": "max_records",
    "maxBytes": "max_bytes",
    "serveState": "serve_state",
    "serveWaitMillis": "serve_wait_millis",
    "onError": "on_error",
    "abortSignal": "abort_signal",
    "from": "from_",
}


def runner_options(opts: Dict[str, Any]) -> Dict[str, Any]:
    """run() options in their Python names (the JS camelCase names work too)."""
    out: Dict[str, Any] = {}
    for k, v in opts.items():
        name = _OPTION_NAMES.get(k)
        if name is None:
            name = k if "_" in k or k.islower() else _CAMEL.sub("_", k).lower()
        out[name] = v
    return out


class _StateReader:
    """``runner.state(step)``: ``get(key)`` and ``list(limit)`` of the live state."""

    def __init__(self, pipeline: Any, step: Any) -> None:
        self._p = pipeline
        self._step = step

    def get(self, key: Any) -> Dict[str, Any]:
        return self._p.inspect(self._step, key)

    def list(self, limit: Any = 100) -> Dict[str, Any]:
        return self._p.list(self._step, limit)


class FeedRunner:
    def __init__(
        self,
        *,
        stream: Any,
        query_id: str,
        from_: Any = UNDEFINED,
        wait_millis: Any = 5000,
        max_records: Any = UNDEFINED,
        max_bytes: Any = UNDEFINED,
        reset: Any = False,
        on_error: Optional[Callable[..., Any]] = None,
        abort_signal: Optional[asyncio.Event] = None,
        position_commit_millis: Any = POSITION_COMMIT_MILLIS,
        commit_every_millis: Any = COMMIT_EVERY_MILLIS,
        max_pending_outputs: Any = MAX_PENDING_OUTPUTS,
        exclusive: bool = True,
        lock_ttl: Any = LOCK_TTL,
        on_poison: Any = "stop",
        retries: Any = RETRIES,
        retry_delay_millis: Any = RETRY_DELAY_MILLIS,
        snapshot_every_millis: Any = SNAPSHOT_EVERY_MILLIS,
        snapshot_max_entries: Any = SNAPSHOT_MAX_ENTRIES,
        snapshot_chunk_chars: Any = DEFAULT_CHUNK_CHARS,
        snapshot_piece_bytes: Any = DEFAULT_PIECE_BYTES,
        shard: Any = None,
        shard_by: Any = "partition",
        serve_state: bool = True,
        serve_wait_millis: Any = None,
        now: Optional[Callable[[], Any]] = None,
        **unknown: Any,
    ) -> None:
        if unknown:
            raise TypeError(f"Stream.fromFeed: unknown run() option(s): {', '.join(sorted(unknown))}")
        self._stream = stream
        self._queen = stream.source.queen
        self._query_id = _require_query_id(query_id)
        self._shard = _require_shard(shard)
        self._id = shard_id(self._query_id, self._shard)
        if shard_by != "partition" and not callable(shard_by):
            raise ValueError("Stream.fromFeed: shardBy is 'partition' (the default) or a function (msg) => key")
        self._shard_by = shard_by
        if not js_is_integer(retries) or retries < 0:
            raise ValueError("Stream.fromFeed: retries is a whole number from 0")
        if reset is not True and reset is not False and (not isinstance(reset, str) or len(reset) == 0):
            raise ValueError("Stream.fromFeed: reset is true (start over at every start) or a string (start over once per string)")
        self._poison = _poison_target(on_poison)
        self._now: Callable[[], Any] = now if callable(now) else now_ms
        self._opts: Dict[str, Any] = {
            "from": from_,
            "waitMillis": wait_millis,
            "maxRecords": max_records,
            "maxBytes": max_bytes,
            "reset": reset,
            "onError": on_error,
            "abortSignal": abort_signal,
            "positionCommitMillis": position_commit_millis,
            "commitEveryMillis": commit_every_millis,
            "maxPendingOutputs": max_pending_outputs,
            "exclusive": exclusive,
            "lockTtl": lock_ttl,
            "retries": int(retries),
            "retryDelayMillis": retry_delay_millis,
            "snapshotEveryMillis": snapshot_every_millis,
            "snapshotMaxEntries": snapshot_max_entries,
            "snapshotChunkChars": snapshot_chunk_chars,
            "snapshotPieceBytes": snapshot_piece_bytes,
            "serveState": serve_state,
            "serveWaitMillis": serve_wait_millis,
        }
        self._reader: Any = None
        self._pipeline: Any = None
        self._sink: Any = None
        self._hash: Optional[str] = None
        # The reads: what the broker filters, what this process filters.
        self._broker_queues: Optional[List[str]] = None
        self._broker_partitions: Optional[List[str]] = None
        self._broker_where: List[Dict[str, Any]] = []
        self._broker_shard: Optional[Dict[str, Any]] = None
        self._local_where: List[Dict[str, Any]] = []
        self._local_partitions: Optional[set] = None
        self._stream_queues: Optional[List["re.Pattern[str]"]] = None
        self._local_shard = False
        self._table_queues: Dict[str, None] = {}
        self._lock: Any = None
        self._lock_lost = False
        self._active = False
        # The last commit.
        self._position: Any = None
        self._version = 0
        self._epoch = 0
        self._reset_token: Optional[str] = None
        self._committed_clock: Any = 0
        # How many closes (windows, deadlines, runs, held messages, each one
        # unit) the stream has made since it started, and at the last commit.
        self._units = 0
        self._committed_units = 0
        # A replay stops its closes at this count (the stored record's), inf: no cap.
        self._replay_cap: Any = _INF
        # The time a close in progress runs up to: a commit in the middle of it records that clock.
        self._burst_target: Any = None
        # The snapshot the stored record names.
        self._snap: Optional[Dict[str, Any]] = None
        self._entries_since_snap = 0
        self._snap_wall_at: Any = 0
        self._old_keys: List[str] = []
        self._had_stray = False
        # The poison policy's skips since the snapshot (committed, and since the last commit).
        self._skips: List[str] = []
        self._pending_skips: List[str] = []
        self._skip_set: set = set()
        # The version of `<queryId>#layout` this process runs under (0: absent); every commit checks it.
        self._layout_version = 0
        # A new stream's state is empty where it starts: that position is its first snapshot.
        self._initial_snap = False
        self._pending = _fresh_pending()
        self._clock: Any = 0
        self._replaying = False
        self._poison_stop: Optional[Dict[str, Any]] = None
        self._last_commit_at: Any = 0
        self._last_position_commit: Any = 0
        # The feed time the committed position is at, and whether it was read caught up.
        self._committed_read_to: Any = 0
        self._committed_caught_up = False
        # Why the stream stops, for the record of its last commit: `{reason, at, ...}`.
        self._stop_mark: Optional[Dict[str, Any]] = None
        # stop() or the abort signal asked for the stop.
        self._stop_asked = False
        # What the stream is (feed/about.py; UNDEFINED: not made yet), and whether
        # the next commit writes it: the first commit of each activation does.
        self._about: Any = UNDEFINED
        self._about_owed = False
        self._last_tick_at: Any = 0
        self._resolved_ms: Any = 0
        self._resolved_at: Any = 0
        self._offset: Any = -_INF
        self._last_now: Any = 0
        self._next_wait: Any = None
        self._loop: Optional["asyncio.Future[None]"] = None
        self._stopping = False
        self._stop_event = asyncio.Event()
        self._last_said: Dict[str, Any] = {}
        self._server: Optional[StateServer] = None
        self._abort_watch: Optional["asyncio.Task[None]"] = None
        self._metrics: Dict[str, Any] = {
            "role": "starting",
            "cycles": 0,
            "entriesRead": 0,
            "emitted": 0,
            "outputs": 0,
            "effects": 0,
            "commits": 0,
            "conflicts": 0,
            "errors": 0,
            "retries": 0,
            "poisoned": 0,
            "skipped": 0,
            "deadLettered": 0,
            "late": 0,
            "ticks": 0,
            "takeovers": 0,
            "snapshots": 0,
            "snapshotBytes": 0,
            "snapshotChunks": 0,
            "snapshotsFull": 0,
            "snapshotLayers": 0,
            "snapshotMillis": 0,
            "units": 0,
            "closeCommits": 0,
            "replays": 0,
            "replayEntries": 0,
            "replayMillis": 0,
            "replaySkipped": 0,
            "tableRows": 0,
            "position": None,
            "applied": None,
            "lag": None,
            "stopped": None,
        }

    # ---- the handle -------------------------------------------------------------

    async def start(self) -> None:
        """Check the chain, take the lock if it is free, load the position and state, start the cycles."""
        steps, sink, chain_hash = self._stream.steps, self._stream.sink, self._stream.chain_hash
        if not sink:
            raise ValueError("Stream.fromFeed: end the chain with .to(queen.queue(...)) or .foreach(fn)")
        self._sink = sink
        self._pipeline = Pipeline(steps)
        self._hash = chain_hash
        self._plan_reads()

        self._reader = self._make_reader(start="now")
        # A broker without the feed (or a feed nobody can serve) fails run() here,
        # with the reason, instead of a stream that silently never runs.
        await self._reader.probe()
        await self._check_layout()

        if self._opts["exclusive"]:
            self._lock = self._queen.lock(
                lock_name(self._query_id, self._shard),
                ttl_seconds=lock_ttl_seconds_of(self._opts["lockTtl"]),
                retry_min=0.25,
                retry_max=2.0,
            )
            self._lock.on_lost(self._on_lock_lost)
        # Active now when the lock is free, so that a chain the stored state was
        # not written by fails run() itself; otherwise a standby, waiting.
        if self._lock is None or await self._lock.acquire():
            try:
                await self._become_active(True)
            except BaseException:
                if self._lock is not None:
                    try:
                        await self._lock.release()
                    except Exception:  # noqa: BLE001
                        pass
                raise
        else:
            self._metrics["role"] = "standby"
        logger.log("FeedRunner.start", {"queryId": self._id, "role": self._metrics["role"], "position": self._position})
        sig = self._opts["abortSignal"]
        if isinstance(sig, asyncio.Event):
            self._abort_watch = spawn(self._watch_abort(sig))  # type: ignore[assignment]
        self._loop = spawn(self._run())

    async def _watch_abort(self, sig: asyncio.Event) -> None:
        await sig.wait()
        self._stop_asked = True
        self._stopping = True
        self._stop_event.set()

    def _on_lock_lost(self, why: Any) -> None:
        self._lock_lost = True
        # It can commit nothing more: a standby from now on.
        if self._active and not self._stopping:
            self._metrics["role"] = "standby"
        if self._server is not None:
            self._server.stop()
        logger.warn("FeedRunner.lockLost", {"queryId": self._id, "why": why})

    async def stop(self) -> None:
        """Stop after the cycle in hand, commit what is pending (and a snapshot), give the lock back."""
        self._stop_asked = True
        self._stopping = True
        self._stop_event.set()
        if self._loop is not None:
            try:
                await asyncio.shield(self._loop)
            except BaseException:  # noqa: BLE001 - reported by on_error
                pass
        if self._reader is not None:
            await self._reader.close()
        if self._server is not None:
            self._server.stop()
        if self._abort_watch is not None:
            self._abort_watch.cancel()

    @property
    def done(self) -> Any:
        """Awaitable: resolves when the runner has stopped (stop(), the abort signal, a poison message, a broker without the feed)."""
        return self._loop

    @property
    def shard(self) -> Optional[Dict[str, int]]:
        return self._shard

    def metrics(self) -> Dict[str, Any]:
        m = dict(self._metrics)
        m["pendingOutputs"] = len(self._pending["outputs"])
        if self._shard:
            m["shard"] = dict(self._shard)
        p = self._pipeline
        if p is not None and p.has_state:
            m["open"] = p.open
            m["stateKeys"] = p.state_keys
            m["steps"] = p.step_metrics(self._clock)
            m["late"] = p.late
            m["clock"] = js_iso(self._clock) if self._clock > 0 else None
            w = p.watermark_at(self._clock)
            if w is not None:
                m["watermark"] = js_iso(w)
            m["snapshotPosition"] = self._snap["position"] if self._snap else None
            m["units"] = self._units
        return m

    def state(self, step: Any) -> _StateReader:
        """The live state of one step, in this process: ``get(key)`` and ``list(limit)``."""
        return _StateReader(self._pipeline, step)

    # ---- reads ------------------------------------------------------------------

    def _plan_reads(self) -> None:
        """What the broker filters and what this process filters, for the source, the tables and the shard."""
        source = self._stream.source.options or {}
        queues = _as_list(source.get("queues", source.get("queue")))
        partitions = _as_list(source.get("partitions", source.get("partition")))
        tables = self._pipeline.tables
        for t in tables:
            for q in t.queues:
                self._table_queues[q] = None
        # Never read back what this query wrote: its outputs carry its prefix.
        me = {"field": "transactionId", "op": "!prefix", "value": output_prefix(self._query_id)}
        where = list(self._stream.pushdown or [])
        if not tables:
            self._broker_queues = queues
            self._broker_partitions = partitions
            self._broker_where = [*where, me]
            if self._shard and self._shard_by == "partition":
                self._broker_shard = {"count": self._shard["count"], "index": self._shard["index"]}
        else:
            # A table queue is read whole: its rows pass no filter of the
            # stream's and no shard, so those filters move here.
            if queues:
                merged: List[str] = []
                for q in [*queues, *self._table_queues]:
                    if q not in merged:
                        merged.append(q)
                self._broker_queues = merged
            else:
                self._broker_queues = None
            self._broker_where = [me]
            self._local_where = where
            self._local_partitions = set(partitions) if partitions else None
            self._stream_queues = [_glob(q) for q in queues] if queues else None
            if self._shard and self._shard_by == "partition":
                # The broker splits only the stream's own queues: never a table
                # queue, which every shard reads whole.
                split = [q for q in queues if q not in self._table_queues] if queues else None
                hits_table = bool(split) and any(
                    re.search(r"[*?]", pat) and any(_glob(pat).match(t) for t in self._table_queues) for pat in split  # type: ignore[union-attr]
                )
                if split and not hits_table:
                    self._broker_shard = {"count": self._shard["count"], "index": self._shard["index"], "queues": split}
                else:
                    self._local_shard = True
        if self._shard and callable(self._shard_by):
            self._local_shard = True

    async def _route(self, entry: Dict[str, Any]) -> str:
        """Where a message goes: a table's rows, the stream, or nowhere (another shard's, filtered out here)."""
        q = entry.get("queue")
        if q in self._table_queues:
            return "table"
        if self._stream_queues is not None and not any(re_.match(js_str(q)) for re_ in self._stream_queues):
            return "skip"
        if self._local_partitions is not None and entry.get("partition") not in self._local_partitions:
            return "skip"
        for c in self._local_where:
            if not condition_holds(c, entry):
                return "skip"
        if self._local_shard and not callable(self._shard_by) and shard_of(entry.get("partition", UNDEFINED), self._shard["count"]) != self._shard["index"]:  # type: ignore[index]
            return "skip"
        return "stream"

    async def _owns(self, entry: Dict[str, Any]) -> bool:
        """Whether this shard takes a stream message, by ``shard_by``: run inside the
        message's unit, so a function that raises meets the poison policy -- in
        shard 0 only, which takes such a message; the other shards leave it."""
        if not self._local_shard or not callable(self._shard_by):
            return True
        try:
            key = await call_user(self._shard_by, entry)
        except Exception:
            if self._shard["index"] == 0:  # type: ignore[index]
                raise
            return False
        return shard_of(js_str(key), self._shard["count"]) == self._shard["index"]  # type: ignore[index]

    def _make_reader(self, *, start: Any, since: Any = UNDEFINED, tables_only: bool = False) -> Any:
        source = self._stream.source.options or {}
        max_records = self._opts["maxRecords"] if not nullish(self._opts["maxRecords"]) else source.get("maxRecords", UNDEFINED)
        max_bytes = self._opts["maxBytes"] if not nullish(self._opts["maxBytes"]) else source.get("maxBytes", UNDEFINED)
        o: Dict[str, Any] = {}
        if tables_only:
            o["queues"] = list(self._table_queues)
            o["followerUrls"] = source.get("followerUrls", UNDEFINED)
            o["where"] = [{"field": "transactionId", "op": "!prefix", "value": output_prefix(self._query_id)}]
            o["from"] = start
            o["waitMillis"] = 0
        else:
            o["queues"] = self._broker_queues if self._broker_queues is not None else UNDEFINED
            o["partitions"] = self._broker_partitions if self._broker_partitions is not None else UNDEFINED
            o["kinds"] = source.get("kinds", UNDEFINED)
            o["since"] = since
            o["followerUrls"] = source.get("followerUrls", UNDEFINED)
            o["where"] = self._broker_where
            o["shard"] = self._broker_shard if self._broker_shard is not None else UNDEFINED
            o["from"] = start
            o["waitMillis"] = self._opts["waitMillis"]
        o["maxRecords"] = max_records
        o["maxBytes"] = max_bytes
        return self._queen.feed({k: v for k, v in o.items() if v is not UNDEFINED and v is not None})

    async def _check_layout(self) -> None:
        """The shard count (and how shards split) is part of the stream, kept in
        ``queen-feed`` / ``<queryId>#layout``. Another one is refused; every
        commit checks the layout's version."""
        ns = POSITION_NAMESPACE
        key = f"{self._query_id}#layout"
        count = self._shard["count"] if self._shard else 1
        by = "function" if callable(self._shard_by) else "partition"
        reset = self._opts["reset"]
        row = await self._queen.kv.get(ns, key)
        value = row.get("value") if isinstance(row, dict) and row.get("found") else None
        stored = value if isinstance(value, dict) and js_is_integer(value.get("shards")) else None
        resetting = reset is True or (isinstance(reset, str) and (stored is None or stored.get("resetToken") != reset))
        if stored is not None and stored["shards"] == count and (count == 1 or (stored.get("shardBy") or "partition") == by):
            self._layout_version = row.get("version") or 0
            return
        if stored is None and count == 1:
            self._layout_version = 0
            return
        if stored is None:
            # First sharded run: refuse when the query ran whole and kept a position.
            whole = await self._queen.kv.get(ns, self._query_id)
            if isinstance(whole, dict) and whole.get("found") and not resetting:
                raise FeedRunnerError(
                    f"{self._query_id} ran as one stream (its position is in KV); running it in {count} shards would move keys "
                    "between shards with their state: run it with { reset: true } (or a reset string) to start the shards over",
                    code="feed_layout_changed",
                )
        elif not resetting:
            plural = "" if stored["shards"] == 1 else "s"
            raise FeedRunnerError(
                f"{self._query_id} runs in {stored['shards']} shard{plural} split by {stored.get('shardBy') or 'partition'}; "
                f"running it in {count} split by {by} would move keys between shards with their state: run it as before, "
                "or with { reset: true } (or a reset string) to start over",
                code="feed_layout_changed",
            )
        new_value: Dict[str, Any] = {"shards": count, "shardBy": by, "updatedAt": js_iso(now_ms())}
        if isinstance(reset, str):
            new_value["resetToken"] = reset
        tx = self._queen.transaction()
        if count == 1:
            tx.kv.delete(ns, key)
        elif isinstance(row, dict) and row.get("found"):
            tx.kv.put(ns, key, new_value, expect=row.get("version"), required=True, forever=True)
        else:
            tx.kv.put_if_absent(ns, key, new_value, required=True, forever=True)
        res = await tx.commit()
        if isinstance(res, dict) and res.get("success") is False:
            # Another process wrote it first: read it again, and judge that one.
            if res.get("reason") == "kv_precondition":
                return await self._check_layout()
            raise FeedRunnerError(f"FeedRunner: the shard layout could not be written ({res.get('reason') or 'unknown'})")
        if count == 1:
            self._layout_version = 0
        else:
            v = _version_of(res, key)
            if v is None:
                again = await self._queen.kv.get(ns, key)
                v = (again.get("version") if isinstance(again, dict) else None) or 0
            self._layout_version = v

    async def _layout_moved(self) -> bool:
        """The layout changed under this process: it stops (a newer deploy runs the stream)."""
        row = await self._queen.kv.get(POSITION_NAMESPACE, f"{self._query_id}#layout")
        found = isinstance(row, dict) and bool(row.get("found"))
        v = (row.get("version") or 0) if found else 0
        if v == self._layout_version:
            return False
        value = row.get("value") if found else None
        now = f"{value.get('shards') if isinstance(value, dict) else 'undefined'} shards" if found else "one stream"
        raise FeedRunnerError(
            f"the shard layout of {self._query_id} changed while this process ran it ({now} now): "
            "another deploy runs it; this process stops",
            code="feed_layout_changed",
            fatal=True,
        )

    # ---- the loop ---------------------------------------------------------------

    async def _run(self) -> None:
        backoff = 500
        while not self._stopping:
            try:
                if not self._active:
                    if self._lock is not None and not self._lock.held:
                        self._metrics["role"] = "standby"
                        got = await self._lock.acquire(wait=3600, signal=self._stop_event)
                        if not got or self._stopping:
                            continue
                        self._metrics["takeovers"] += 1
                    await self._become_active(False)
                    backoff = 500
                if self._lock is not None and (self._lock_lost or not self._lock.held):
                    self._stand_down("lost")
                    continue
                await self._cycle()
                backoff = 500
            except asyncio.CancelledError:
                raise
            except BaseException as err:  # noqa: BLE001 - judged below
                # stop() cancelled the read in flight: what the pages before it
                # did is still pending, and the stop below commits it.
                if self._stopping and getattr(err, "aborted", False):
                    break
                # stop() came while a message was being retried: leave it for later.
                if getattr(err, "stopping", False):
                    if self._active:
                        self._rollback()
                    break
                if getattr(err, "fatal", False):
                    self._report(err, {"phase": "load", "position": self._position})
                    self._metrics["stopped"] = {"reason": "config", "error": str(err)}
                    self._stopping = True
                    break
                self._metrics["errors"] += 1
                if self._active:
                    self._rollback()
                if getattr(err, "code", None) == LOCK_NOT_HELD or isinstance(err, LockNotHeldError):
                    self._stand_down("expired")
                    continue
                self._report(err, {"phase": "cycle" if self._active else "load", "position": self._position})
                # A broker without the feed will not grow one: stop rather than retry.
                if getattr(err, "no_feed_route", False):
                    self._stopping = True
                    break
                await _sleep(backoff, self._stop_event)
                backoff = min(backoff * 2, MAX_BACKOFF_MILLIS)
        # Stopping: what is done and not yet committed goes in now, with a
        # snapshot, so the next start has nothing to read again.
        snapshot_owed = self._pipeline.has_state and self._pipeline.changed_since_snapshot
        # Asked to stop: the record of this last commit says so (`stopped`), so
        # that a stream stopped on purpose is not taken for one that died.
        # (Only at a position: a stream stopped before its first read has none to write.)
        at_position = self._pending["next"] if js_is_integer(self._pending["next"]) else self._position
        asked = self._stop_asked and self._metrics["stopped"] is None and js_is_integer(at_position)
        if (
            self._active
            and (self._has_pending() or snapshot_owed or asked)
            and not (self._lock is not None and not self._lock.held)
            and self._poison_stop is None
        ):
            if asked:
                self._stop_mark = {"reason": "stop", "at": js_iso(self._now())}
            try:
                await self._commit()
            except Exception as err:  # noqa: BLE001
                self._report(err, {"phase": "stop", "position": self._position})
            finally:
                self._stop_mark = None
        self._active = False
        if self._server is not None:
            self._server.stop()
        if self._lock is not None:
            try:
                await self._lock.release()
            except Exception:  # noqa: BLE001
                pass
        self._metrics["role"] = "stopped"

    async def _become_active(self, first: bool) -> None:
        await self._load(first)
        self._lock_lost = False
        self._active = True
        self._metrics["role"] = "active"
        # The first commit of each activation writes what the stream is (_about_value).
        self._about_owed = True
        eph = getattr(self._queen, "ephemeral", None)
        if self._opts["serveState"] and eph is not None:
            if self._server is not None:
                self._server.stop()
            wait = self._opts["serveWaitMillis"]
            self._server = StateServer(
                self._queen,
                query_queue_name(self._query_id, self._shard["index"] if self._shard else 0),
                self._answer,
                wait_millis=wait if wait else None,
            )
            self._server.start()

    def _stand_down(self, why: str) -> None:
        """The lock is gone: drop what was not committed and wait to hold it again."""
        self._rollback()
        self._initial_snap = False
        self._active = False
        if self._server is not None:
            self._server.stop()
        self._metrics["role"] = "standby"
        logger.log("FeedRunner.standby", {"queryId": self._id, "why": why})

    def _about_value(self) -> Any:
        """What this stream reads, its steps and where its results go (feed/about.py),
        for ``queen-feed`` / ``<queryId>#about``: the console reads it. Made once
        per process; None when it cannot be made (the stream runs without it)."""
        if self._about is UNDEFINED:
            try:
                self._about = js_normalize(describe_feed_stream(
                    stream=self._stream,
                    pipeline=self._pipeline,
                    query_id=self._query_id,
                    shard=self._shard,
                    shard_by=self._shard_by,
                    poison=self._poison,
                    exclusive=self._opts["exclusive"],
                    lock_ttl=self._opts["lockTtl"],
                    position_commit_millis=self._opts["positionCommitMillis"],
                ))
            except Exception as err:  # noqa: BLE001
                logger.warn("FeedRunner.about", {"queryId": self._id, "error": str(err)})
                self._about = None
        return self._about

    def _report(self, err: BaseException, ctx: Dict[str, Any]) -> None:
        logger.error("FeedRunner", {"queryId": self._id, "error": str(err), **ctx})
        hook = self._opts["onError"]
        if callable(hook):
            try:
                out = hook(err, {"queryId": self._query_id, "shard": self._shard, **ctx})
                if inspect.isawaitable(out):
                    spawn(out)  # type: ignore[arg-type]
            except Exception:  # noqa: BLE001 - the hook's own
                pass
            return
        # No hook: say it on stderr, once per message every 30 s, so a stream
        # that cannot run never looks like a stream with nothing to do.
        msg = str(err) or type(err).__name__
        now = now_ms()
        last = self._last_said.get(msg)
        if last is None or now - last >= 30000:
            self._last_said[msg] = now
            retrying = ctx.get("phase") in ("cycle", "load") and not getattr(err, "no_feed_route", False)
            print(f"[queen feed] {self._id}: {msg}{' (retrying)' if retrying else ''}", file=sys.stderr)

    def _answer(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """A question from outside (feed/query.py), answered from the live state."""
        base = {
            "shard": self._shard["index"] if self._shard else 0,
            "shards": self._shard["count"] if self._shard else 1,
            "role": self._metrics["role"],
            "position": self._reader.position if self._reader is not None else self._position,
            "committed": self._position,
        }
        op = req.get("op")
        if op == "status":
            return {**base, "metrics": self.metrics()}
        if not self._pipeline.has_state:
            return {**base, "error": "this stream keeps no state"}
        if op == "get":
            return {**base, **self._pipeline.inspect(req.get("step", UNDEFINED), req.get("key", UNDEFINED))}
        if op == "list":
            try:
                n = float(req.get("limit")) if not isinstance(req.get("limit"), bool) else float(req.get("limit"))
            except (TypeError, ValueError):
                n = 0.0
            if n != n or n == 0:
                n = 100
            limit = min(max(n, 1), 1000)
            return {**base, **self._pipeline.list(req.get("step", UNDEFINED), limit)}
        return {**base, "error": f"unknown question {js_json_dumps(op) if op is not UNDEFINED else 'undefined'} (get, list, status)"}

    # ---- one cycle --------------------------------------------------------------

    async def _cycle(self) -> None:
        wait = self._opts["waitMillis"]
        if self._next_wait is not None:
            wait = self._next_wait
            self._next_wait = None
        # stop() cancels a long poll in flight: nothing of it was processed yet.
        page = await self._reader.read(wait_millis=wait, signal=self._stop_event)
        m = self._metrics
        m["cycles"] += 1
        m["entriesRead"] += len(page["entries"])
        m["applied"] = page.get("applied")
        applied, nxt = page.get("applied"), page.get("next")
        m["lag"] = max(0, applied - nxt + 1) if js_is_integer(applied) and js_is_integer(nxt) else None
        # The broker's clock as of the read: a lower bound of how far the
        # broker's clock is from this one; the estimate decides only when to
        # write a tick, never when a window closes.
        ts = page.get("resolvedTs")
        resolved = js_date_parse(ts if isinstance(ts, str) and ts else "")
        if resolved is not None:
            now = self._now()
            # This machine's clock was set back: learn the distance again.
            if now < self._last_now:
                self._offset = -_INF
            self._last_now = now
            if resolved > self._resolved_ms:
                self._resolved_ms = resolved
                self._resolved_at = now
            self._offset = max(self._offset, resolved - now)
        # Lost the lock while the read was out: what it read is the next holder's.
        if self._lock is not None and (self._lock_lost or not self._lock.held):
            self._stand_down("lost")
            return
        if self._initial_snap:
            self._initial_snap = False
            first = page["entries"][0] if page["entries"] else None
            self._snap = {
                "id": None,
                "chunks": 0,
                "bytes": 0,
                "position": first["position"] if first else page.get("next"),
                "clock": _ts_of(first) if first else (0 if resolved is None else resolved),
                "metas": self._pipeline.metas(),
                # Nothing closed yet where the stream starts: a replay from here counts its closes from this.
                "units": self._units,
            }
            self._pipeline.snapshot_taken()

        r = await self._process_page(page, resolved)
        if r == "abandon":
            return
        if r == "stop":
            await self._stop_on_poison()
            return
        await self._settle(page)

    def _groups(self, entries: List[Dict[str, Any]], below: Any = _INF) -> List[Dict[str, Any]]:
        """The entries of a page by position: one position is one transaction, whose messages share a time."""
        groups: List[Dict[str, Any]] = []
        for e in entries:
            if e["position"] >= below:
                break
            if groups and groups[-1]["position"] == e["position"]:
                groups[-1]["entries"].append(e)
            else:
                groups.append({"position": e["position"], "entries": [e]})
        return groups

    async def _process_page(self, page: Dict[str, Any], resolved: Any) -> str:
        """Every message of a page, in order, then the clock to what the read resolved."""
        for g in self._groups(page["entries"]):
            ref = {"cp": self._checkpoint()}
            r = await self._process_group(g, ref)
            if r == "abandon":
                return "abandon"
            if r == "stop":
                # The whole transaction the poisoned message came in stays for
                # later (from the last commit, when one came in the middle of a close).
                self._restore(ref["cp"])
                self._pending["next"] = g["position"]
                return "stop"
            self._pending["next"] = g["position"] + 1
            self._read_to(_ts_of(g["entries"][0]), False)
            # Enough waiting outputs: commit them now, between two positions.
            if len(self._pending["outputs"]) >= self._opts["maxPendingOutputs"]:
                if (await self._commit()) != "ok":
                    return "abandon"
        if self._pipeline.has_state and self._pipeline.needs_feed_clock:
            ref = {"cp": self._checkpoint()}
            t = max(self._clock, 0 if resolved is None else resolved)
            self._clock = t
            r = await self._advance_to(t, ref)
            if r == "abandon":
                return "abandon"
            if r == "stop":
                self._restore(ref["cp"])
                self._pending["next"] = page.get("next")
                self._read_to(0 if resolved is None else resolved, page.get("caughtUp") is True)
                return "stop"
        self._pending["next"] = page.get("next")
        self._read_to(0 if resolved is None else resolved, page.get("caughtUp") is True)
        return "ok"

    def _read_to(self, t: Any, caught_up: bool) -> None:
        """The feed time the pending position is at (it only moves forward), and whether the read was caught up there."""
        p = self._pending
        base = p["readTo"] if p["readTo"] is not None else self._committed_read_to
        p["readTo"] = max(base, t if js_is_finite(t) else 0)
        p["caughtUp"] = caught_up

    async def _process_group(self, g: Dict[str, Any], ref: Optional[Dict[str, Any]] = None) -> str:
        # One position is one transaction of the log: its messages share a time.
        t = max(self._clock, _ts_of(g["entries"][0]))
        if self._pipeline.has_state:
            # Before the group's messages: a commit in the middle of this close names the group's position, not past it.
            r = await self._advance_to(t, ref)
            if r != "ok":
                return r
        self._clock = t
        # A table row of the transaction counts before its other messages.
        routed = []
        for entry in g["entries"]:
            routed.append((entry, await self._route(entry)))
        routed.sort(key=lambda er: 0 if er[1] == "table" else 1)
        for entry, route in routed:
            if route == "skip":
                continue
            if route == "table":
                if (await self._unit({"entry": entry}, self._row_fn(entry))) == "stop":
                    return "stop"
                self._metrics["tableRows"] += 1
                self._pending["work"] += 1
                continue
            r = await self._unit({"entry": entry}, self._entry_fn(entry, t))
            if r == "stop":
                return "stop"
            if self._pipeline.has_state:
                self._pending["work"] += 1
        self._entries_since_snap += len(g["entries"])
        # An event-time step may have moved its watermark. After the group's
        # messages: a commit in the middle of this close names the next position.
        if self._pipeline.has_state:
            self._pending["next"] = g["position"] + 1
            self._read_to(t, False)
            r = await self._advance_to(t, ref)
            if r != "ok":
                return r
        return "ok"

    def _row_fn(self, entry: Dict[str, Any]) -> Callable[[Any], Any]:
        async def run(_ctr: Any) -> None:
            await self._pipeline.apply_row(entry)

        return run

    def _entry_fn(self, entry: Dict[str, Any], t: Any) -> Callable[[Any], Any]:
        async def run(ctr: Dict[str, int]) -> None:
            if not await self._owns(entry):
                return

            async def out(env: Dict[str, Any], ent: Any) -> None:
                await self._out(env, ent, ctr)

            await self._pipeline.entry(entry, t, out)

        return run

    async def _advance_to(self, t: Any, ref: Optional[Dict[str, Any]] = None) -> str:
        """Close what is due at ``t``; each result is delivered as a unit of its own,
        and counted (``units``). When many results wait, they commit in the middle
        of the close, after a batch; the commit records the count, and
        ``ref['cp']`` becomes the point a stop comes back to. A replay stops at
        the count the stored record names."""
        if not js_is_finite(t):
            return "ok"
        live = not self._replaying
        max_units = _INF if live or not js_is_finite(self._replay_cap) else max(0, self._replay_cap - self._units)
        outer = self._burst_target
        self._burst_target = t if outer is None else max(outer, t)

        async def unit(em: Dict[str, Any], run: Callable[[Any], Any]) -> str:
            async def fn(ctr: Dict[str, int]) -> None:
                async def out(env: Dict[str, Any], ent: Any) -> None:
                    await self._out(env, ent, ctr)

                await run(out)

            r = await self._unit({"emit": em}, fn)
            if r != "stop":
                self._units += 1
            return r

        async def on_batch(_done: int) -> str:
            if len(self._pending["outputs"]) < self._opts["maxPendingOutputs"]:
                return "ok"
            self._metrics["closeCommits"] += 1
            if (await self._commit()) != "ok":
                return "abandon"
            if ref is not None:
                ref["cp"] = self._checkpoint()
            return "ok"

        try:
            return await self._pipeline.advance(t, unit, max_units=max_units, on_batch=on_batch if live else None)
        finally:
            self._burst_target = outer

    async def _out(self, env: Dict[str, Any], entry: Any, ctr: Dict[str, int]) -> None:
        """One result at the end of the chain: the queues it goes to (pushed with
        the next commit) or the ``.foreach`` function. A replay sends nothing."""
        if self._replaying:
            return
        self._metrics["emitted"] += 1
        derived = bool(env.get("derived"))
        ctx = env.get("ctx", UNDEFINED) if derived else entry
        if self._sink.kind == "foreach":
            if derived or not entry:
                fctx = env.get("ctx", UNDEFINED)
            else:
                fctx = {
                    "position": entry.get("position", UNDEFINED),
                    "queue": entry.get("queue", UNDEFINED),
                    "partition": entry.get("partition", UNDEFINED),
                    "partitionId": entry.get("partitionId", UNDEFINED),
                    "offset": entry.get("offset", UNDEFINED),
                    "transactionId": entry.get("transactionId", UNDEFINED),
                }
            await call_user(self._sink.fn, env.get("value", UNDEFINED), fctx)
            self._metrics["effects"] += 1
            self._pending["effects"] += 1
            return
        fallback = env.get("key", UNDEFINED) if derived else (entry.get("partition", UNDEFINED) if entry else env.get("key", UNDEFINED))
        targets = await self._sink.targets_for(env.get("value", UNDEFINED), ctx, fallback)
        for target in targets:
            if entry:
                transaction_id = feed_output_id(self._query_id, entry, ctr["n"], self._epoch, self._id)
                ctr["n"] += 1
            else:
                transaction_id = self._emit_id()
            self._pending["outputs"].append({
                "queue": target["queue"],
                "partition": target["partition"],
                "data": payload_of(env.get("value", UNDEFINED)),
                "transactionId": transaction_id,
            })

    def _emit_id(self) -> str:
        seq = self._pending["emitSeq"]
        self._pending["emitSeq"] += 1
        h = _sha(f"{self._id}\n{self._epoch}\nemit\n{self._version}\n{seq}")
        return f"{output_prefix(self._query_id)}{h[:40]}"

    async def _unit(self, subject: Dict[str, Any], fn: Callable[[Dict[str, int]], Any]) -> str:
        """Run ``fn`` (a message, or one result of a step) until it passes, at most
        ``retries`` more times, each time from the state it saw the first time.
        Then the poison policy: 'ok', 'skipped', 'deadLettered' or 'stop'. In a
        replay a failure is left out at once."""
        retries = self._opts["retries"]
        if self._replaying:
            # What the run before left out (its poison policy), a replay leaves out.
            if self._skip_id(subject) in self._skip_set:
                self._metrics["replaySkipped"] += 1
                return "ok"
            for attempt in range(retries + 1):
                cp = self._checkpoint()
                try:
                    await fn({"n": 0})
                    return "ok"
                except asyncio.CancelledError:
                    raise
                except Exception as err:  # noqa: BLE001 - the replay leaves it out
                    self._restore(cp)
                    if attempt == retries:
                        self._metrics["replaySkipped"] += 1
                        entry = subject.get("entry")
                        logger.warn(
                            "FeedRunner.replaySkip",
                            {"queryId": self._id, "error": str(err), "position": entry.get("position") if entry else None},
                        )
                        return "ok"
                    await _sleep(self._opts["retryDelayMillis"] * 2 ** attempt, self._stop_event)
            return "ok"
        last_err: Optional[BaseException] = None
        for attempt in range(retries + 1):
            cp = self._checkpoint()
            try:
                await fn({"n": 0})
                return "ok"
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - retried, then the poison policy
                self._restore(cp)
                last_err = err
                if self._stopping:
                    raise _Stopping("stopping") from err
                if attempt < retries:
                    self._metrics["retries"] += 1
                    await _sleep(self._opts["retryDelayMillis"] * 2 ** attempt, self._stop_event)
        return self._poisoned(subject, last_err, retries + 1)

    def _skip_id(self, subject: Dict[str, Any]) -> str:
        """The identity of a unit, the same on a replay: a message by its coordinates,
        a result by its step, key, time and kind."""
        e = subject.get("entry")
        if e:
            return "\u001f".join(["e", js_str(e.get("position", UNDEFINED)), js_str(e.get("queue", UNDEFINED)), js_str(e.get("partition", UNDEFINED)), js_str(e.get("offset", UNDEFINED))])
        em = subject["emit"]
        step = em.get("step", UNDEFINED)
        time_ = em.get("time", UNDEFINED)
        ctx = em.get("ctx", UNDEFINED)
        kind = ctx.get("kind", UNDEFINED) if isinstance(ctx, dict) else UNDEFINED
        sid = "\u001f".join([
            "r",
            "" if nullish(step) else js_str(step),
            js_str(em.get("key", UNDEFINED)),
            "" if nullish(time_) else js_str(time_),
            "" if nullish(kind) else js_str(kind),
        ])
        # A message an event-time step held: its place in the step's order (the same on a replay).
        eid = em.get("id")
        return f"{sid}\u001f{js_str(eid)}" if eid else sid

    def _poisoned(self, subject: Dict[str, Any], err: Optional[BaseException], attempts: int) -> str:
        self._metrics["poisoned"] += 1
        if self._poison["kind"] != "stop" and self._pipeline.has_state:
            self._pending_skips.append(self._skip_id(subject))
        # A feed message: the unit's own, or one an event-time step held and took in its turn.
        emit = subject.get("emit")
        entry = subject.get("entry") or (emit.get("entry") if isinstance(emit, dict) else None) or None
        if entry:
            what: Dict[str, Any] = {
                "position": entry.get("position", UNDEFINED),
                "queue": entry.get("queue", UNDEFINED),
                "partition": entry.get("partition", UNDEFINED),
                "offset": entry.get("offset", UNDEFINED),
                "transactionId": entry.get("transactionId", UNDEFINED),
            }
            where = (
                f"the message at position {js_str(what['position'])} ({js_str(what['queue'])}/{js_str(what['partition'])}, "
                f"offset {js_str(what['offset'])})"
            )
        else:
            step = emit.get("step", UNDEFINED)
            what = {"step": None if nullish(step) else step, "result": emit["ctx"].get("kind", UNDEFINED), "key": emit.get("key", UNDEFINED)}
            key_text = js_json_dumps(what["key"])
            where = f"the {js_str(what['result'])} result for key {key_text if key_text is not UNDEFINED else 'undefined'}"
        reason = (str(err) if err is not None and str(err) else (type(err).__name__ if err is not None else "undefined"))
        if self._poison["kind"] == "skip":
            self._metrics["skipped"] += 1
            e = FeedRunnerError(f"{where} failed {attempts} times and was skipped: {reason}")
            e.__cause__ = err
            self._report(e, {"phase": "poison", "action": "skipped", "attempts": attempts, **what})
            return "skipped"
        if self._poison["kind"] == "deadLetter":
            record: Dict[str, Any] = {
                "queryId": self._query_id,
                "attempts": attempts,
                "error": {"name": _error_name(err), "message": reason},
            }
            if entry:
                record["entry"] = snapshot_of(entry)
            else:
                record["result"] = {
                    **emit["ctx"],
                    "value": emit.get("value", UNDEFINED),
                    "waiting": emit["state"].get("msg", UNDEFINED) if isinstance(emit.get("state"), dict) else UNDEFINED,
                }
            if self._shard:
                record["shard"] = dict(self._shard)
            # A message an event-time step held is one of maybe several items of
            # it (a .flatMap() before): its place in the step's order tells them apart.
            held = f"\n{js_str(emit.get('step', UNDEFINED))}\n{js_str(emit.get('id', UNDEFINED))}" if isinstance(emit, dict) and emit.get("release") else ""
            if entry:
                text = f"{self._id}\n{self._epoch}\ndlq\n{js_str(what['position'])}\n{js_str(what['queue'])}\n{js_str(what['partition'])}\n{js_str(what['offset'])}{held}"
                tid = f"{output_prefix(self._query_id)}{_sha(text)[:40]}"
            else:
                tid = self._emit_id()
            self._pending["outputs"].append({
                "queue": self._poison["queue"],
                "partition": entry.get("partition", UNDEFINED) if entry else js_str(emit.get("key", UNDEFINED)),
                "data": record,
                "transactionId": tid,
            })
            self._metrics["deadLettered"] += 1
            e = FeedRunnerError(f"{where} failed {attempts} times and went to {self._poison['queue']}: {reason}")
            e.__cause__ = err
            self._report(e, {"phase": "poison", "action": "deadLettered", "attempts": attempts, **what})
            return "deadLettered"
        self._poison_stop = {"where": where, "what": what, "attempts": attempts, "err": err, "reason": reason}
        return "stop"

    async def _stop_on_poison(self) -> None:
        """on_poison 'stop': commit what came before the message, then stop there."""
        p = self._poison_stop
        assert p is not None
        # This commit's record says why the stream stopped, and at which
        # message: it is written even with nothing else to commit.
        self._stop_mark = {
            "reason": "poison",
            "at": js_iso(self._now()),
            "error": clip_text(p["reason"], STOP_ERROR_MAX_CHARS),
            "attempts": p["attempts"],
            **clip(p["what"]),
        }
        try:
            r = await self._commit()
        finally:
            self._stop_mark = None
        if r != "ok":
            # Another commit won: the stream reads on from the stored position.
            self._poison_stop = None
            return
        error = FeedRunnerError(
            f"{p['where']} failed {p['attempts']} times: {p['reason']}. The stream stopped there: fix the function and start it again, "
            "or run it with onPoison: 'skip' or a dead-letter queue",
            code="feed_poison",
            **{k: v for k, v in p["what"].items() if k not in ("code", "fatal")},
        )
        error.__cause__ = p["err"]
        self._metrics["stopped"] = {"reason": "poison", "error": p["reason"], "attempts": p["attempts"], **p["what"]}
        self._report(error, {"phase": "poison", "action": "stopped", "attempts": p["attempts"], **p["what"]})
        self._stopping = True

    def _checkpoint(self) -> Dict[str, Any]:
        return {
            "state": self._pipeline.checkpoint(),
            "outputs": len(self._pending["outputs"]),
            "work": self._pending["work"],
            "effects": self._pending["effects"],
            "emitSeq": self._pending["emitSeq"],
            "clock": self._clock,
            "units": self._units,
            "late": self._pipeline.late,
            "skips": len(self._pending_skips),
            # A unit tried again counts its results once.
            "emitted": self._metrics["emitted"],
        }

    def _restore(self, cp: Dict[str, Any]) -> None:
        self._pipeline.restore(cp["state"])
        del self._pending["outputs"][cp["outputs"]:]
        self._pending["work"] = cp["work"]
        self._pending["effects"] = cp["effects"]
        self._pending["emitSeq"] = cp["emitSeq"]
        self._clock = cp["clock"]
        self._units = cp["units"]
        self._pipeline.late = cp["late"]
        del self._pending_skips[cp["skips"]:]
        self._metrics["emitted"] = cp["emitted"]

    def _rollback(self) -> None:
        """Back to the last commit: what was produced since is dropped, and read again."""
        self._pending = _fresh_pending()
        self._pending_skips = []
        self._clock = self._committed_clock
        self._units = self._committed_units
        self._pipeline.discard()
        if self._reader is not None:
            self._reader.seek(self._position)

    def _has_pending(self) -> bool:
        p = self._pending
        return (
            len(p["outputs"]) > 0
            or p["work"] > 0
            or p["effects"] > 0
            or (self._pipeline.has_state and self._pipeline.dirty)
            or (js_is_integer(p["next"]) and p["next"] != self._position)
        )

    async def _settle(self, page: Dict[str, Any]) -> None:
        """After a page: commit when it is time, else move the broker's clock if a deadline is due."""
        p = self._pending
        now = self._now()
        # Outputs and done effects commit soon (at once when caught up); state
        # alone every commitEveryMillis; a position alone now and then.
        outputs = len(p["outputs"]) > 0 or p["effects"] > 0
        moved = js_is_integer(p["next"]) and p["next"] != self._position
        due = False
        if outputs:
            due = (
                page["caughtUp"]
                or len(p["outputs"]) >= self._opts["maxPendingOutputs"]
                or now - self._last_commit_at >= self._opts["commitEveryMillis"]
            )
        elif self._pipeline.dirty or p["work"] > 0:
            due = now - self._last_commit_at >= self._opts["commitEveryMillis"]
        elif moved:
            due = now - self._last_position_commit >= self._opts["positionCommitMillis"]
        if due:
            await self._commit()
        if not self._pipeline.has_state or not self._pipeline.needs_feed_clock or self._resolved_at == 0:
            return
        nxt = self._pipeline.next_due()
        if nxt is None:
            return
        # A window or a deadline is pending. Right after a write of this
        # process, a short read shows the broker's time (the best sample).
        if due:
            self._next_wait = TICK_READ_WAIT_MILLIS
            return
        if not page["caughtUp"]:
            return
        # Where the broker's clock is now, as far as this process can tell.
        estimate = now + self._offset
        if estimate >= nxt:
            if now - self._last_tick_at >= TICK_MIN_MILLIS:
                await self._tick(now)
            self._next_wait = TICK_READ_WAIT_MILLIS
        elif now - self._resolved_at >= CLOCK_SYNC_MILLIS and now - self._last_tick_at >= CLOCK_SYNC_MILLIS:
            # The last write anyone made is old: the estimate may be far behind.
            await self._tick(now)
            self._next_wait = TICK_READ_WAIT_MILLIS
        else:
            self._next_wait = max(0, min(self._opts["waitMillis"], math.ceil(nxt - estimate)))

    async def _tick(self, now: Any) -> None:
        """Write the position: the write moves the broker's clock, and the next read shows it."""
        self._last_tick_at = now
        self._metrics["ticks"] += 1
        await self._commit()

    # ---- commit -----------------------------------------------------------------

    async def _snapshot_for(self, nxt: Any) -> Dict[str, Any]:
        """The snapshot the commit at ``nxt`` names: the stored one moved here (the
        state did not change since), the stored one as it is (not due yet), an
        empty one, or a new layer on it, whose chunks are written now."""
        p = self._pipeline
        prev = self._snap
        metas = p.metas()
        if prev and not p.changed_since_snapshot:
            return {"pointer": {**prev, "position": nxt, "clock": self._record_clock(), "metas": metas, "units": self._units}, "wrote": False, "moved": True, "replaced": []}
        layers = layers_of(prev)
        if p.empty:
            return {"pointer": pointer_of([], nxt, self._record_clock(), metas, self._units), "wrote": True, "replaced": layers}
        # Due on the wall clock, after so many messages, at a stop, or with many skips.
        due = (
            not prev
            or self._stopping
            or self._now() - self._snap_wall_at >= self._opts["snapshotEveryMillis"]
            or self._entries_since_snap >= self._opts["snapshotMaxEntries"]
            or len(self._skips) + len(self._pending_skips) > MAX_SKIPS
        )
        if not due:
            return {"pointer": prev, "wrote": False, "replaced": []}
        started = now_ms()
        base = layers[0] if layers else None
        delta_bytes = 0
        for layer in layers[1:]:
            delta_bytes += layer["bytes"]
        full = (
            not base
            or base.get("v") == 1
            or len(layers) >= MAX_LAYERS
            or delta_bytes >= base["bytes"]
            or p.touched_since_snapshot >= p.state_keys
        )
        sid = new_snapshot_id()
        w = await write_layer(
            queen=self._queen,
            ns=POSITION_NAMESPACE,
            id=self._id,
            sid=sid,
            lock=self._lock,
            steps=[{"kind": st.kind, "entries": st.state.entries() if full else st.state.touched()} for st in p.stateful],
            chunk_chars=self._opts["snapshotChunkChars"],
            piece_bytes=self._opts["snapshotPieceBytes"],
        )
        layer: Dict[str, Any] = {"sid": sid, "chunks": w["chunks"], "bytes": w["bytes"]}
        if full:
            layer["full"] = True
        named = [layer] if full else ([*layers, layer] if w["chunks"] > 0 else layers)
        return {
            "pointer": pointer_of(named, nxt, self._record_clock(), metas, self._units),
            "wrote": True,
            "full": full,
            "replaced": layers if full else [],
            "keys": layer_keys(self._id, [layer]),
            "millis": now_ms() - started,
        }

    def _record_clock(self) -> Any:
        """The clock a commit records. In the middle of a close it is the time the close runs up to."""
        return self._clock if self._burst_target is None else max(self._clock, self._burst_target)

    async def _commit(self) -> str:
        """One transaction: the outputs, the position (and the snapshot it names). 'ok', 'conflict' or 'lost'."""
        nxt = self._pending["next"] if js_is_integer(self._pending["next"]) else self._position
        stateful = self._pipeline.has_state
        snap = None
        try:
            snap = await self._snapshot_for(nxt) if stateful else None
        except BaseException:
            # Chunks of this attempt may be in KV, named by nothing.
            self._had_stray = True
            raise
        done = False
        try:
            r = await self._commit_with(nxt, stateful, snap)
            done = r == "ok"
            return r
        finally:
            if not done and snap and snap.get("wrote") and snap.get("keys"):
                self._had_stray = True

    async def _commit_with(self, nxt: Any, stateful: bool, snap: Optional[Dict[str, Any]]) -> str:
        tx = self._queen.transaction()
        if self._lock is not None:
            tx.guard(self._lock)
        groups: Dict[Any, Dict[str, Any]] = {}
        for it in self._pending["outputs"]:
            k = (it["queue"], it["partition"])
            g = groups.get(k)
            if g is None:
                g = groups[k] = {"queue": it["queue"], "partition": it["partition"], "items": []}
            g["items"].append({"data": js_normalize(it["data"]), "transactionId": it["transactionId"]})
        for g in groups.values():
            tx.queue(g["queue"]).partition(g["partition"]).push(g["items"])
        value: Dict[str, Any] = {
            "position": nxt,
            "queryId": self._query_id,
            "epoch": self._epoch,
            "updatedAt": js_iso(self._now()),
        }
        if self._shard:
            value["shard"] = dict(self._shard)
        if self._reset_token is not None:
            value["resetToken"] = self._reset_token
        recorded = self._record_clock()
        if stateful:
            value["hash"] = self._hash
            value["clock"] = recorded
            value["units"] = self._units
            value["snapshot"] = snap["pointer"]  # type: ignore[index]
        # The skips since the snapshot this record names (none past a new one).
        skips = [*self._skips, *self._pending_skips] if stateful and not (snap.get("wrote") or snap.get("moved")) else []  # type: ignore[union-attr]
        if skips:
            value["skips"] = skips
        # For a reader of the record (the console): the feed time the position
        # is at, whether the read was caught up there, what each step holds,
        # and why it stopped.
        p = self._pending
        read_to = p["readTo"] if p["readTo"] is not None else self._committed_read_to
        caught_up = p["caughtUp"] if p["caughtUp"] is not None else (nxt == self._position and self._committed_caught_up)
        if read_to > 0:
            value["readTo"] = read_to
        value["caughtUp"] = caught_up is True
        if stateful:
            value["open"] = {}
            for st in self._pipeline.stateful:
                value["open"][st.name] = st.engine.open if st.engine is not None else st.table.open
                if st.clock is not None and st.clock.kind == "event" and st.clock.held > 0:
                    value.setdefault("held", {})[st.name] = st.clock.held
        if self._stop_mark:
            value["stopped"] = self._stop_mark
        # Every shard writes the same description, under the lock like the rest of the commit.
        about = self._about_value() if self._about_owed else None
        if about:
            tx.kv.put(POSITION_NAMESPACE, about_key(self._query_id), about, forever=True)
        # A layout another deploy wrote since this process started stops it here.
        tx.kv.check(POSITION_NAMESPACE, f"{self._query_id}#layout", expect=self._layout_version, required=True)
        wire = js_normalize(value)
        if self._version > 0:
            tx.kv.put(POSITION_NAMESPACE, self._id, wire, expect=self._version, required=True, forever=True)
        else:
            tx.kv.put_if_absent(POSITION_NAMESPACE, self._id, wire, required=True, forever=True)
        try:
            res = await tx.commit()
        except Exception as err:
            # A pushed output is already stored: its id is made from what it
            # comes from, so another process on this queryId committed these
            # messages -- or this one did, and its answer was lost -- and that
            # commit moved the position in the same transaction.
            if getattr(err, "reason", None) == "duplicate":
                await self._reload("duplicate")
                return "conflict"
            raise
        if isinstance(res, dict) and res.get("success") is False:
            if res.get("reason") != "kv_precondition":
                raise FeedRunnerError(f"FeedRunner: commit refused ({res.get('reason') or 'unknown'}): {res.get('error') or ''}")
            # The guard lost (the lock is somebody else's now), or the position
            # moved (another process, or this commit's own earlier attempt whose
            # answer was lost). Nothing of this commit was written.
            if self._lock is not None and not self._lock.held:
                self._stand_down("guard")
                return "lost"
            await self._layout_moved()
            await self._reload("position")
            return "conflict"
        version = _version_of(res, self._id)
        committed_outputs = len(self._pending["outputs"])
        self._position = nxt
        self._committed_clock = recorded
        self._committed_units = self._units
        self._committed_read_to = read_to
        self._committed_caught_up = value["caughtUp"]
        self._about_owed = False
        if stateful:
            self._snap = snap["pointer"]  # type: ignore[index]
            if snap.get("wrote") or snap.get("moved"):  # type: ignore[union-attr]
                self._pipeline.snapshot_taken()
                self._entries_since_snap = 0
                self._snap_wall_at = self._now()
            self._skips = skips
            self._skip_set = set(skips)
            self._pending_skips = []
            if snap.get("wrote"):  # type: ignore[union-attr]
                m = self._metrics
                m["snapshots"] += 1
                if snap.get("full"):  # type: ignore[union-attr]
                    m["snapshotsFull"] += 1
                m["snapshotBytes"] = snap["pointer"]["bytes"]  # type: ignore[index]
                m["snapshotChunks"] = snap["pointer"]["chunks"]  # type: ignore[index]
                m["snapshotLayers"] = len(layers_of(snap["pointer"]))  # type: ignore[index]
                m["snapshotMillis"] = snap.get("millis") or 0  # type: ignore[union-attr]
                # The layers the new pointer does not name: nobody reads them again.
                if snap["replaced"]:  # type: ignore[index]
                    self._old_keys.extend(layer_keys(self._id, snap["replaced"]))  # type: ignore[index]
            self._pipeline.commit_point()
        self._pending = _fresh_pending()
        self._last_commit_at = self._last_position_commit = self._now()
        self._metrics["commits"] += 1
        self._metrics["outputs"] += committed_outputs
        self._metrics["position"] = nxt
        if version is not None:
            self._version = version
        else:
            await self._refresh_version()
        if stateful and snap.get("wrote") and self._had_stray and self._lock is not None:  # type: ignore[union-attr]
            # Holding the lock, this process is the only writer: every chunk not
            # of the snapshot just named is left over.
            self._had_stray = False
            try:
                keep = {layer["sid"] for layer in layers_of(snap["pointer"])}  # type: ignore[index]
                self._old_keys.extend(await stray_chunk_keys(queen=self._queen, ns=POSITION_NAMESPACE, id=self._id, keep=keep))
            except Exception as err:  # noqa: BLE001
                logger.warn("FeedRunner.snapshotStray", {"queryId": self._id, "error": str(err)})
        if self._old_keys:
            await self._drop_old_keys()
        return "ok"

    async def _drop_old_keys(self) -> None:
        """Delete what no record names any more: the chunks of replaced snapshots (best effort)."""
        keys = self._old_keys
        self._old_keys = []
        try:
            await delete_keys(queen=self._queen, ns=POSITION_NAMESPACE, keys=keys, lock=self._lock)
        except Exception as err:  # noqa: BLE001
            logger.warn("FeedRunner.snapshotCleanup", {"queryId": self._id, "keys": len(keys), "error": str(err)})

    async def _refresh_version(self) -> None:
        got = await self._queen.kv.get(POSITION_NAMESPACE, self._id)
        if isinstance(got, dict) and got.get("found"):
            self._version = got.get("version") or self._version

    # ---- load -------------------------------------------------------------------

    async def _reload(self, why: str) -> None:
        """Lost a commit to another one: take the stored position and state, read on from there."""
        self._metrics["conflicts"] += 1
        self._rollback()
        try:
            await self._load(False)
        except BaseException:
            # Half loaded is not loaded: the next cycle loads all of it again.
            self._active = False
            raise
        logger.log("FeedRunner.conflict", {"queryId": self._id, "why": why, "position": self._position, "version": self._version})

    async def _load(self, first: bool) -> None:
        """The position, and the state as of it: the snapshot the record names, then
        the feed read again from the snapshot's position to the committed one."""
        qid = self._id
        # Nothing of an earlier load or start carries over.
        self._initial_snap = False
        self._old_keys = []
        row = await self._queen.kv.get(POSITION_NAMESPACE, qid)
        found = isinstance(row, dict) and bool(row.get("found"))
        meta = row.get("value") if found and isinstance(row.get("value"), dict) else None
        stored = meta if meta is not None and js_is_integer(meta.get("position")) else None
        self._version = (row.get("version") or 0) if found else 0
        reset = self._opts["reset"]
        # `true` starts over when this process starts; a string, once, at
        # whichever load first finds the stored record without it.
        start_over = (first and reset is True) or (isinstance(reset, str) and stored is not None and stored.get("resetToken") != reset)
        self._skips = list(stored["skips"]) if not start_over and stored is not None and isinstance(stored.get("skips"), list) else []
        self._skip_set = set(self._skips)
        self._pending_skips = []
        if isinstance(reset, str):
            self._reset_token = reset
        else:
            self._reset_token = stored["resetToken"] if stored is not None and isinstance(stored.get("resetToken"), str) else None
        source = self._stream.source.options or {}
        p = self._pipeline
        is_new = False
        if stored is None or start_over:
            is_new = True
            if stored is not None:
                self._epoch = (int(stored["epoch"]) if js_is_integer(stored.get("epoch")) else 0) + 1
            else:
                self._epoch = int(meta["epoch"]) + 1 if meta is not None and js_is_integer(meta.get("epoch")) else 0
            start = self._opts["from"]
            if nullish(start):
                start = source.get("from", UNDEFINED)
            if nullish(start):
                start = "earliest" if source.get("since", UNDEFINED) is not UNDEFINED else "now"
            self._position = start
            self._clock = 0
            if p.has_state:
                p.wipe()
            # What the stored record kept is nobody's now.
            if stored is not None and stored.get("snapshot"):
                self._old_keys.extend(layer_keys(qid, layers_of(stored["snapshot"])))
            if stored is not None and js_is_integer(stored.get("buckets")):
                for b in range(int(stored["buckets"])):
                    self._old_keys.append(f"{qid}#{b}")
            self._snap = None
            if start_over:
                logger.log("FeedRunner.reset", {"queryId": qid, "epoch": self._epoch, "from": self._position})
        else:
            self._epoch = int(stored["epoch"]) if js_is_integer(stored.get("epoch")) else 0
            self._position = int(stored["position"])
            self._clock = stored["clock"] if js_is_finite(stored.get("clock")) else 0
            if p.has_state:
                await self._load_state(stored)
        self._committed_clock = self._clock
        self._committed_read_to = stored["readTo"] if not is_new and stored is not None and js_is_finite(stored.get("readTo")) else self._clock
        self._committed_caught_up = False
        if is_new:
            self._units = 0
        self._committed_units = self._units
        if is_new:
            self._entries_since_snap = 0
        self._snap_wall_at = self._now()
        self._pending = _fresh_pending()
        # Snapshot attempts that died between their chunks and their commit
        # leave chunks nobody names: looked for at the next snapshot.
        if p.has_state:
            self._had_stray = True
        if is_new:
            # 'now' is the position when the stream starts, not when its first
            # read comes: a message pushed once run() resolved is the stream's.
            if self._position == "now" and source.get("since", UNDEFINED) is UNDEFINED:
                self._position = (await self._reader.probe()).get("next")
            if any(t.initial == "all" for t in p.tables) and self._position != "earliest":
                await self._prelude(source)
            # Empty where it starts (a table's first rows aside, which need a real snapshot).
            self._initial_snap = p.has_state and p.empty
            if js_is_integer(self._position):
                self._reader.seek(self._position)
            elif source.get("since", UNDEFINED) is not UNDEFINED:
                await self._reader.close()
                self._reader = self._make_reader(start=self._position, since=source["since"])
            else:
                self._reader.seek(self._position)
        else:
            self._reader.seek(self._position)

    async def _load_state(self, stored: Dict[str, Any]) -> None:
        p = self._pipeline
        qid = self._id
        snap = stored.get("snapshot") if isinstance(stored.get("snapshot"), dict) else None
        # State the old chain kept: chunks, or messages to read again since the snapshot.
        has_state = bool(
            (snap is not None and (len(layers_of(snap)) > 0 or (js_is_integer(snap.get("position")) and snap["position"] < stored["position"])))
            or js_is_integer(stored.get("buckets"))
        )
        if has_state and isinstance(stored.get("hash"), str) and stored["hash"] != self._hash:
            raise FeedRunnerError(
                f"the steps of {self._query_id} changed since it last ran, and its state was kept by the old chain: "
                "run it with { reset: true } (or a reset string) to start it over, or give it a new queryId",
                code="feed_chain_changed",
                fatal=True,
            )
        if snap is not None:
            layers = layers_of(snap)
            try:
                if layers and layers[0].get("v") == 1:
                    # Format 1: one JSON of the whole state across the chunks of one id.
                    chunks = await read_chunks(queen=self._queen, ns=POSITION_NAMESPACE, id=qid, sid=layers[0]["sid"], count=layers[0]["chunks"])
                    p.load(decode_snapshot(chunks), snap.get("metas"))
                else:
                    # Format 2: every layer in order, piece by piece.
                    p.load_begin()
                    for layer in layers:
                        await read_layer(
                            queen=self._queen,
                            ns=POSITION_NAMESPACE,
                            id=qid,
                            sid=layer["sid"],
                            count=layer["chunks"],
                            on_piece=lambda pc: p.load_piece(pc.get("s"), pc.get("k"), pc.get("set"), pc.get("del")),
                        )
                    await p.load_end(snap.get("metas"))
            except Exception as err:
                # Another process replaced the snapshot while this one read it
                # (exclusive: false): load again, from the record as it is now.
                now = await self._queen.kv.get(POSITION_NAMESPACE, qid)

                def sids(x: Any) -> str:
                    return ",".join(js_str(layer.get("sid")) for layer in layers_of(x))

                now_value = now.get("value") if isinstance(now, dict) and now.get("found") else None
                if (
                    getattr(err, "code", None) == "feed_snapshot_missing"
                    and isinstance(now, dict)
                    and now.get("found")
                    and sids(now_value.get("snapshot") if isinstance(now_value, dict) else None) != sids(snap)
                ):
                    raise FeedRunnerError(f"the snapshot of {qid} was replaced while it loaded; loading again", code="feed_snapshot_moved") from err
                raise
            # The closes the state holds (the snapshot's count), and how many the
            # stored record says came by its position: the replay stops there.
            self._units = int(snap["units"]) if js_is_integer(snap.get("units")) else 0
            cap = int(stored["units"]) if js_is_integer(snap.get("units")) and js_is_integer(stored.get("units")) else _INF
            snap_clock = snap["clock"] if js_is_finite(snap.get("clock")) else 0
            self._snap = snap
            if (
                (js_is_integer(snap.get("position")) and snap["position"] < stored["position"])
                or (js_is_finite(cap) and cap > self._units)
                or snap_clock < self._clock
            ):
                frm = snap["position"] if js_is_integer(snap.get("position")) else stored["position"]
                self._entries_since_snap = await self._replay(frm, stored["position"], snap_clock, self._clock, cap)
        elif js_is_integer(stored.get("buckets")):
            # A record of the bucket format (before snapshots): the buckets are
            # the state at the position. Loaded once; the next commit writes a snapshot.
            keys = [f"{qid}#{b}" for b in range(int(stored["buckets"]))]
            r = await self._queen.kv.get_many(POSITION_NAMESPACE, keys) if keys else {"rows": []}
            entries = []
            for row in (r.get("rows") if isinstance(r, dict) else None) or []:
                v = row.get("value")
                e = v.get("e") if isinstance(v, dict) and isinstance(v.get("e"), dict) else {}
                for k, x in e.items():
                    entries.append([k, x])
            metas = [{"watermark": stored.get("watermark", UNDEFINED)}]
            first_step = p.stateful[0] if p.stateful else None
            p.load({"steps": [{"name": first_step.name if first_step else UNDEFINED, "kind": first_step.kind if first_step else UNDEFINED, "entries": entries}]}, metas)
            self._snap = None
            self._old_keys.extend(keys)
        else:
            p.load(None, None)
            self._snap = None

    async def _replay(self, frm: int, to: int, snap_clock: Any, committed_clock: Any, cap: Any = _INF) -> int:
        """The feed from ``frm`` (a snapshot's position) to ``to`` (the committed one)
        once more, as it ran: same filters, same clock, every result dropped."""
        started = now_ms()
        reader = self._make_reader(start=frm)
        self._replaying = True
        self._replay_cap = cap
        self._clock = snap_clock
        n = 0
        try:
            deadline = now_ms() + REPLAY_CATCH_UP_MILLIS
            while js_is_finite(reader.position) and reader.position < to:
                page = await reader.read(wait_millis=100)
                for g in self._groups(page["entries"], to):
                    await self._process_group(g)
                    n += len(g["entries"])
                pos = reader.position
                if page["caughtUp"] and js_is_integer(pos) and pos < to and now_ms() > deadline:
                    raise FeedRunnerError(
                        f"FeedRunner: the follower read has not reached the committed position {to} (at {pos}) after {REPLAY_CATCH_UP_MILLIS} ms"
                    )
            self._clock = max(self._clock, committed_clock)
            await self._advance_to(self._clock)
        finally:
            self._replaying = False
            self._replay_cap = _INF
            await reader.close()
        if js_is_finite(cap) and self._units != cap:
            logger.warn("FeedRunner.replayUnits", {"queryId": self._id, "replayed": self._units, "stored": cap})
        self._pipeline.commit_point()
        self._metrics["replays"] += 1
        self._metrics["replayEntries"] += n
        self._metrics["replayMillis"] += now_ms() - started
        logger.log("FeedRunner.replay", {"queryId": self._id, "from": frm, "to": to, "entries": n, "millis": now_ms() - started})
        return n

    async def _prelude(self, source: Dict[str, Any]) -> None:
        """A new stream with a table: the table queue from its beginning up to where
        the stream starts, rows only, so the first message already sees every row."""
        start = self._position
        if not js_is_integer(start):
            if source.get("since", UNDEFINED) is not UNDEFINED:
                probe = self._make_reader(start="earliest", since=source["since"])
                try:
                    page = await probe.read(wait_millis=0)
                    start = page["entries"][0]["position"] if page["entries"] else page.get("next")
                finally:
                    await probe.close()
            else:
                answer = await self._reader.probe()
                start = answer.get("next")
        self._position = start
        reader = self._make_reader(start="earliest", tables_only=True)
        rows = 0
        try:
            while reader.position == "earliest" or (js_is_integer(reader.position) and reader.position < start):
                page = await reader.read(wait_millis=0)
                for e in page["entries"]:
                    if e["position"] >= start:
                        break
                    await self._pipeline.apply_row(e)
                    rows += 1
                if page["caughtUp"]:
                    break
        finally:
            await reader.close()
        self._metrics["tableRows"] += rows
        # The rows are the starting point: a rollback keeps them (the position
        # stays where the stream starts), and the first commit snapshots them.
        self._pipeline.commit_point()
        if rows:
            self._pending["work"] += 1
        logger.log("FeedRunner.tablePrelude", {"queryId": self._id, "rows": rows, "start": start})


def _error_name(err: Optional[BaseException]) -> str:
    if err is None:
        return "Error"
    name = getattr(err, "js_name", None)
    if isinstance(name, str):
        return name
    return type(err).__name__
