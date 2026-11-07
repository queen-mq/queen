"""
Snapshots of a feed stream's state, in KV, in chunks. Port of the JS SDK's
runtime/feed/snapshot.js, byte-compatible with it.

A snapshot is the state of every stateful step at one position, as LAYERS: a
full one (every entry), then deltas (the entries set or deleted since the layer
before), applied in order. When the deltas weigh as much as the full layer, or
there are many, the next snapshot is a full one again and the old layers go.

A layer is written in PIECES: a few thousand entries of one step as JSON
(``{"v":2,"s":<step>,"k":"<kind>","set":[[k,v],...],"del":[k,...]}``, about
256 KB), deflated (raw, RFC 1951, no zlib header), standard base64, cut into
KV values of 48,000 characters (a value with ``m: 1`` goes on in the next).

The values are written first, in transactions of at most 60 KV writes, under a
new id nothing points to yet; then the commit that moves the position names
the layers, in the same transaction; then the chunks of layers no longer
named are deleted. So the position record always names a whole snapshot.

    queen-feed / <id>                    the position record: {snapshot: {layers: [{sid, chunks, bytes, full?}], position, clock, metas, units}}
    queen-feed / <id>#snap#<sid>#<n>     chunk n of layer sid: {d: '<base64>', m?: 1}

Format 1 (one JSON of the whole state across the chunks of one id, no layers)
is still read: the next snapshot after it is a full layer.
"""

from __future__ import annotations

import asyncio
import base64
import json
import math
import re
import secrets
import zlib
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Set, Union

from ...util.js import UNDEFINED, js_is_integer, js_json_dumps, js_normalize, now_ms

SNAPSHOT_FORMAT = 1
LAYER_FORMAT = 2
DEFAULT_CHUNK_CHARS = 48_000
# JSON of one piece before deflate.
DEFAULT_PIECE_BYTES = 256 * 1024
# At most this many layers: a full one and deltas.
MAX_LAYERS = 16
# A transaction carries at most 64 KV operations; the lock's guard is one.
OPS_PER_TX = 60
# Keys asked per read: about 3 MB of chunks, under the 4 MiB read budget.
KEYS_PER_READ = 64
_B64_JUNK = re.compile(r"[^A-Za-z0-9+/]")
# A piece this big is deflated off the event loop (a thread), as JS does off the main thread.
_THREAD_BYTES = 64 * 1024


class SnapshotError(Exception):
    """A snapshot that cannot be read or written; ``fatal`` ones stop the runner."""

    def __init__(self, message: str, *, code: Optional[str] = None, fatal: bool = False, reason: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code
        self.fatal = fatal
        self.reason = reason


def _deflate_raw(data: bytes) -> bytes:
    c = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15, 8, zlib.Z_DEFAULT_STRATEGY)
    return c.compress(data) + c.flush()


def _inflate_raw(data: bytes) -> bytes:
    return zlib.decompress(data, -15)


async def _deflate(data: bytes) -> bytes:
    if len(data) >= _THREAD_BYTES:
        return await asyncio.to_thread(_deflate_raw, data)
    return _deflate_raw(data)


async def _inflate(data: bytes) -> bytes:
    if len(data) >= _THREAD_BYTES:
        return await asyncio.to_thread(_inflate_raw, data)
    return _inflate_raw(data)


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(text: str) -> bytes:
    # Buffer.from(text, 'base64') is lenient: no padding needed, junk skipped.
    t = _B64_JUNK.sub("", text.replace("-", "+").replace("_", "/"))
    t += "=" * (-len(t) % 4)
    return base64.b64decode(t)


def _utf8(text: str) -> bytes:
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError:
        return text.encode("utf-8", "surrogatepass")


def _loads(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8", "replace"))


def layers_of(snap: Any) -> List[Dict[str, Any]]:
    """The layers a snapshot pointer names, oldest first: format 2 lists them;
    format 1 is one full layer of its own (``v: 1``); an empty state names none."""
    if not isinstance(snap, dict):
        return []
    if isinstance(snap.get("layers"), list):
        return snap["layers"]
    chunks = snap.get("chunks")
    if snap.get("id") and isinstance(chunks, (int, float)) and not isinstance(chunks, bool) and chunks > 0:
        return [{"sid": snap["id"], "chunks": chunks, "bytes": snap.get("bytes") or 0, "full": True, "v": 1}]
    return []


def pointer_of(layers: List[Dict[str, Any]], position: Any, clock: Any, metas: Any, units: Any = UNDEFINED) -> Dict[str, Any]:
    """A pointer that names ``layers`` (sums kept for the metrics and for the old
    readers of ``id`` / ``chunks``). ``units``: how many closes the stream had
    made when the state was as the layers hold it."""
    chunks = 0
    nbytes = 0
    for layer in layers:
        chunks += layer["chunks"]
        nbytes += layer["bytes"]
    p: Dict[str, Any] = {
        "id": layers[-1]["sid"] if layers else None,
        "chunks": chunks,
        "bytes": nbytes,
        "layers": layers,
        "position": position,
        "clock": clock,
        "metas": metas,
    }
    if js_is_integer(units):
        p["units"] = units
    return p


def chunk_prefix(qid: str) -> str:
    return f"{qid}#snap#"


def chunk_key(qid: str, sid: str, n: int) -> str:
    return f"{chunk_prefix(qid)}{sid}#{n}"


def layer_keys(qid: str, layers: List[Dict[str, Any]]) -> List[str]:
    """Every chunk key of the layers."""
    out = []
    for layer in layers:
        for n in range(int(layer["chunks"])):
            out.append(chunk_key(qid, layer["sid"], n))
    return out


def _base36(n: int) -> str:
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    if n == 0:
        return "0"
    out = []
    neg = n < 0
    n = abs(n)
    while n:
        n, r = divmod(n, 36)
        out.append(digits[r])
    return ("-" if neg else "") + "".join(reversed(out))


def new_snapshot_id() -> str:
    """A new snapshot id: never one an earlier attempt or another process used."""
    return f"{_base36(now_ms())}{secrets.token_hex(4)}"


def encode_snapshot(dump: Dict[str, Any], chunk_chars: int = DEFAULT_CHUNK_CHARS) -> Dict[str, Any]:
    """``dump`` as chunks of format 1: ``{chunks: [str], bytes}`` (bytes: the JSON before deflate)."""
    text = js_json_dumps({"v": SNAPSHOT_FORMAT, **dump})
    data = _utf8(text)
    b64 = _b64(_deflate_raw(data))
    size = max(1024, math.floor(chunk_chars))
    chunks = [b64[i:i + size] for i in range(0, len(b64), size)]
    return {"chunks": chunks, "bytes": len(data)}


def decode_snapshot(chunks: List[str]) -> Dict[str, Any]:
    dump = _loads(_inflate_raw(_unb64("".join(chunks))))
    if not isinstance(dump, dict) or dump.get("v") != SNAPSHOT_FORMAT:
        v = dump.get("v") if isinstance(dump, dict) else dump
        raise SnapshotError(f"feed snapshot: format {v} is not one this client reads ({SNAPSHOT_FORMAT})")
    return dump


async def _commit_kv(queen: Any, lock: Any, fill: Callable[[Any], None]) -> Any:
    tx = queen.transaction()
    if lock is not None:
        tx.guard(lock)
    fill(tx)
    res = await tx.commit()
    if isinstance(res, dict) and res.get("success") is False:
        err = SnapshotError(
            f"feed snapshot: a KV write was refused ({res.get('reason') or 'unknown'}): {res.get('error') or ''}",
            reason=res.get("reason"),
        )
        raise err
    return res


async def write_layer(
    *,
    queen: Any,
    ns: str,
    id: str,  # noqa: A002 - the JS name
    sid: str,
    lock: Any,
    steps: List[Dict[str, Any]],
    chunk_chars: int = DEFAULT_CHUNK_CHARS,
    piece_bytes: int = DEFAULT_PIECE_BYTES,
) -> Dict[str, int]:
    """Write one layer ``sid``: for each step, its ``entries`` (``(key, value)``;
    a value of UNDEFINED deletes the key), in pieces. Nothing points to the layer
    until the position does. Returns ``{chunks, bytes, entries}``."""
    size = max(1024, math.floor(chunk_chars))
    room = max(1024, math.floor(piece_bytes))
    counters = {"chunks": 0, "bytes": 0, "entries": 0}
    batch: List[Any] = []

    async def send() -> None:
        nonlocal batch
        if not batch:
            return
        part = batch
        batch = []

        def fill(tx: Any) -> None:
            for n, value in part:
                tx.kv.put(ns, chunk_key(id, sid, n), value, forever=True)

        await _commit_kv(queen, lock, fill)

    async def piece(s: int, kind: str, set_: List[str], del_: List[str]) -> None:
        text = f'{{"v":{LAYER_FORMAT},"s":{s},"k":{js_json_dumps(kind)},"set":[{",".join(set_)}],"del":[{",".join(del_)}]}}'
        data = _utf8(text)
        counters["bytes"] += len(data)
        b64 = _b64(await _deflate(data))
        i = 0
        while i < len(b64):
            more = i + size < len(b64)
            n = counters["chunks"]
            counters["chunks"] += 1
            batch.append((n, {"d": b64[i:i + size], "m": 1} if more else {"d": b64[i:i + size]}))
            if len(batch) >= OPS_PER_TX:
                await send()
            i += size

    for s, step in enumerate(steps):
        kind = step["kind"]
        set_: List[str] = []
        del_: List[str] = []
        used = 0
        for k, v in step["entries"]:
            counters["entries"] += 1
            # The limit is in UTF-8 bytes (a string's length counts fewer).
            if v is UNDEFINED:
                j = js_json_dumps(k)
                del_.append(j)
            else:
                j = js_json_dumps([k, v])
                set_.append(j)
            used += len(_utf8(j))
            if used >= room:
                await piece(s, kind, set_, del_)
                set_ = []
                del_ = []
                used = 0
                await asyncio.sleep(0)
        if set_ or del_:
            await piece(s, kind, set_, del_)
    await send()
    return counters


async def read_layer(
    *,
    queen: Any,
    ns: str,
    id: str,  # noqa: A002
    sid: str,
    count: int,
    on_piece: Callable[[Dict[str, Any]], Union[None, Awaitable[None]]],
) -> None:
    """Read layer ``sid`` (``count`` chunks) piece by piece, in order, and hand
    each one to ``on_piece({s, k, set, del})``. Raises when a chunk is missing."""
    parts: List[str] = []
    i = 0
    count = int(count)
    while i < count:
        values = await _read_values(queen=queen, ns=ns, id=id, sid=sid, start=i, end=min(count, i + KEYS_PER_READ), count=count)
        for v in values:
            parts.append(v["d"])
            if v.get("m"):
                continue
            raw = await _inflate(_unb64("".join(parts)))
            parts = []
            p = _loads(raw)
            if not isinstance(p, dict) or p.get("v") != LAYER_FORMAT:
                pv = p.get("v") if isinstance(p, dict) else p
                raise SnapshotError(
                    f"feed snapshot {sid} of {id}: a piece of format {pv} (this client reads {LAYER_FORMAT})",
                    code="feed_snapshot_format",
                    fatal=True,
                )
            out = on_piece(p)
            if asyncio.iscoroutine(out):
                await out
            await asyncio.sleep(0)
        i += KEYS_PER_READ
    if parts:
        raise SnapshotError(f"feed snapshot {sid} of {id}: its last chunk says more follows", code="feed_snapshot_missing", fatal=True)


async def _read_values(*, queen: Any, ns: str, id: str, sid: str, start: int, end: int, count: int) -> List[Dict[str, Any]]:  # noqa: A002
    """Chunks ``start`` to ``end`` (exclusive) of layer ``sid``, as their values, in order."""
    out: List[Any] = [None] * (end - start)
    ask = [chunk_key(id, sid, n) for n in range(start, end)]
    rnd = 0
    while ask:
        r = await queen.kv.get_many(ns, ask)
        got: Set[str] = set()
        for row in (r.get("rows") if isinstance(r, dict) else None) or []:
            key = row.get("key", "")
            n = int(key[key.rfind("#") + 1:])
            value = row.get("value")
            if 0 <= n - start < len(out):
                out[n - start] = value if isinstance(value, dict) and isinstance(value.get("d"), str) else None
            got.add(key)
        missing = (r.get("missing") if isinstance(r, dict) else None) or []
        if missing:
            raise SnapshotError(
                f"feed snapshot {sid} of {id} lacks {len(missing)} of its {count} chunks (first: {missing[0]}): "
                "the state cannot be loaded; start the stream over with { reset: true }",
                code="feed_snapshot_missing",
                fatal=True,
            )
        left = [k for k in ask if k not in got]
        if len(left) == len(ask) or rnd > 64:
            raise SnapshotError(f"feed snapshot: the KV read of {id}'s chunks came back empty and cut")
        ask = left
        rnd += 1
    for n, v in enumerate(out):
        if not v:
            raise SnapshotError(f"feed snapshot {sid} of {id}: chunk {start + n} is not a chunk", code="feed_snapshot_missing", fatal=True)
    return out


async def write_chunks(*, queen: Any, ns: str, id: str, sid: str, chunks: List[str], lock: Any) -> None:  # noqa: A002
    """Write the chunks of snapshot ``sid`` (format 1); nothing points to them until the position does."""
    for i in range(0, len(chunks), OPS_PER_TX):
        end = min(len(chunks), i + OPS_PER_TX)

        def fill(tx: Any, i: int = i, end: int = end) -> None:
            for n in range(i, end):
                tx.kv.put(ns, chunk_key(id, sid, n), {"d": chunks[n]}, forever=True)

        await _commit_kv(queen, lock, fill)


async def read_chunks(*, queen: Any, ns: str, id: str, sid: str, count: int) -> List[str]:  # noqa: A002
    """The chunks of snapshot ``sid``, in order; raises when one is missing."""
    count = int(count)
    out: List[Any] = [None] * count
    for i in range(0, count, KEYS_PER_READ):
        ask = [chunk_key(id, sid, n) for n in range(i, min(count, i + KEYS_PER_READ))]
        rnd = 0
        while ask:
            r = await queen.kv.get_many(ns, ask)
            got: Set[str] = set()
            for row in (r.get("rows") if isinstance(r, dict) else None) or []:
                key = row.get("key", "")
                n = int(key[key.rfind("#") + 1:])
                value = row.get("value")
                if 0 <= n < count:
                    out[n] = value["d"] if isinstance(value, dict) and isinstance(value.get("d"), str) else None
                got.add(key)
            missing = (r.get("missing") if isinstance(r, dict) else None) or []
            if missing:
                raise SnapshotError(
                    f"feed snapshot {sid} of {id} lacks {len(missing)} of its {count} chunks (first: {missing[0]}): "
                    "the state cannot be loaded; start the stream over with { reset: true }",
                    code="feed_snapshot_missing",
                    fatal=True,
                )
            left = [k for k in ask if k not in got]
            if len(left) == len(ask) or rnd > 64:
                raise SnapshotError(f"feed snapshot: the KV read of {id}'s chunks came back empty and cut")
            ask = left
            rnd += 1
    for n in range(count):
        if not isinstance(out[n], str):
            raise SnapshotError(f"feed snapshot {sid} of {id}: chunk {n} is not a chunk", code="feed_snapshot_missing", fatal=True)
    return out


async def delete_keys(*, queen: Any, ns: str, keys: List[str], lock: Any) -> None:
    """Delete keys, in transactions of at most 60 deletes."""
    for i in range(0, len(keys), OPS_PER_TX):
        part = keys[i:i + OPS_PER_TX]

        def fill(tx: Any, part: List[str] = part) -> None:
            for k in part:
                tx.kv.delete(ns, k)

        await _commit_kv(queen, lock, fill)


async def stray_chunk_keys(*, queen: Any, ns: str, id: str, keep: Union[str, Iterable[str], None]) -> List[str]:  # noqa: A002
    """The chunk keys of ``id`` in KV whose layer is not one of ``keep`` (left by an attempt that never committed)."""
    if not callable(getattr(queen.kv, "get_prefix", None)):
        return []
    if keep is None:
        live: Set[str] = set()
    elif isinstance(keep, str):
        live = {keep}
    else:
        live = set(keep)
    prefix = chunk_prefix(id)
    out: List[str] = []
    after: Optional[str] = None
    for _ in range(1000):
        r = await queen.kv.get_prefix(ns, prefix, keys_only=True, limit=1000, after=after)
        for row in (r.get("rows") if isinstance(r, dict) else None) or []:
            key = row if isinstance(row, str) else row.get("key", "")
            sid = key[len(prefix):key.rfind("#")]
            if sid not in live:
                out.append(key)
        if not (isinstance(r, dict) and r.get("truncated") and r.get("nextAfter")):
            break
        after = r["nextAfter"]
    return out


def normalized_value(value: Any) -> Any:
    """A value as JS would write it to KV (JSON.parse(JSON.stringify(v)))."""
    return js_normalize(value)
