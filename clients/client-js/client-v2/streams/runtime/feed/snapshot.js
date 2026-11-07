/**
 * Snapshots of a feed stream's state, in KV, in chunks.
 *
 * A snapshot is the state of every stateful step at one position, as LAYERS:
 * a full one (every entry), then deltas (the entries set or deleted since the
 * layer before), applied in order. Most snapshots are a delta, so writing one
 * costs what changed, not the size of the state; when the deltas weigh as
 * much as the full layer, or there are many, the next snapshot is a full one
 * again and the old layers go.
 *
 * A layer is written in PIECES: a few thousand entries of one step as JSON
 * (`{ v: 2, s, k, set, del }`, about 256 KB), deflated off the main thread,
 * base64, cut into KV values that each fit one KV value
 * (QUEEN_KV_MAX_VALUE_BYTES, 64 KiB by default; a value with `m: 1` goes on
 * in the next). No string ever holds the whole state, and the process keeps
 * answering (its lock, its live reads) while a big layer is written or read.
 *
 * The values are written first, in transactions of at most 60 KV writes,
 * under a new id nothing points to yet; then the commit that moves the
 * position names the layers, in the same transaction; then the chunks of
 * layers no longer named are deleted. So the position record always names a
 * whole snapshot.
 *
 *   queen-feed / <id>                    the position record: { snapshot: { layers: [{ sid, chunks, bytes, full? }], position, clock, metas } }
 *   queen-feed / <id>#snap#<sid>#<n>     chunk n of layer sid: { d: '<base64>', m?: 1 }
 *
 * Format 1 (one JSON of the whole state across the chunks of one id, no
 * layers) is still read: the next snapshot after it is a full layer.
 */
import crypto from 'node:crypto'
import zlib from 'node:zlib'
import { promisify } from 'node:util'
import { setImmediate as nextTurn } from 'node:timers/promises'

export const SNAPSHOT_FORMAT = 1
export const LAYER_FORMAT = 2
export const DEFAULT_CHUNK_CHARS = 48_000
/** JSON of one piece before deflate. */
export const DEFAULT_PIECE_BYTES = 256 * 1024
/** At most this many layers: a full one and deltas. */
export const MAX_LAYERS = 16
// A transaction carries at most 64 KV operations; the lock's guard is one.
const OPS_PER_TX = 60
// Keys asked per read: about 3 MB of chunks, under the 4 MiB read budget.
const KEYS_PER_READ = 64
const deflate = promisify(zlib.deflateRaw)
const inflate = promisify(zlib.inflateRaw)

/**
 * The layers a snapshot pointer names, oldest first: format 2 lists them;
 * format 1 is one full layer of its own (`v: 1`); an empty state names none.
 */
export function layersOf(snap) {
  if (!snap || typeof snap !== 'object') return []
  if (Array.isArray(snap.layers)) return snap.layers
  if (snap.id && snap.chunks > 0) return [{ sid: snap.id, chunks: snap.chunks, bytes: snap.bytes || 0, full: true, v: 1 }]
  return []
}

/**
 * A pointer that names `layers` (sums kept for the metrics and for the old
 * readers of `id` / `chunks`). `units`: how many closes the stream had made
 * when the state was as the layers hold it (the runner counts them).
 */
export function pointerOf(layers, position, clock, metas, units) {
  let chunks = 0
  let bytes = 0
  for (const l of layers) { chunks += l.chunks; bytes += l.bytes }
  const p = { id: layers.length ? layers[layers.length - 1].sid : null, chunks, bytes, layers, position, clock, metas }
  if (Number.isInteger(units)) p.units = units
  return p
}

/** Every chunk key of the layers. */
export function layerKeys(id, layers) {
  const out = []
  for (const l of layers) for (let n = 0; n < l.chunks; n++) out.push(chunkKey(id, l.sid, n))
  return out
}

export function chunkPrefix(id) {
  return `${id}#snap#`
}

export function chunkKey(id, sid, n) {
  return `${chunkPrefix(id)}${sid}#${n}`
}

/** A new snapshot id: never one an earlier attempt or another process used. */
export function newSnapshotId() {
  return `${Date.now().toString(36)}${crypto.randomBytes(4).toString('hex')}`
}

/** `dump` as chunks: `{ chunks: [string], bytes }` (bytes: the JSON before deflate). */
export function encodeSnapshot(dump, chunkChars = DEFAULT_CHUNK_CHARS) {
  const json = JSON.stringify({ v: SNAPSHOT_FORMAT, ...dump })
  const b64 = zlib.deflateRawSync(Buffer.from(json, 'utf8')).toString('base64')
  const size = Math.max(1024, Math.floor(chunkChars))
  const chunks = []
  for (let i = 0; i < b64.length; i += size) chunks.push(b64.slice(i, i + size))
  return { chunks, bytes: Buffer.byteLength(json) }
}

export function decodeSnapshot(chunks) {
  const buf = zlib.inflateRawSync(Buffer.from(chunks.join(''), 'base64'))
  const dump = JSON.parse(buf.toString('utf8'))
  if (!dump || dump.v !== SNAPSHOT_FORMAT) {
    throw new Error(`feed snapshot: format ${dump && dump.v} is not one this client reads (${SNAPSHOT_FORMAT})`)
  }
  return dump
}

async function commitKv(queen, lock, fill) {
  const tx = queen.transaction()
  if (lock) tx.guard(lock)
  fill(tx)
  const res = await tx.commit()
  if (res && res.success === false) {
    const err = new Error(`feed snapshot: a KV write was refused (${res.reason || 'unknown'}): ${res.error || ''}`)
    err.reason = res.reason
    throw err
  }
  return res
}

/**
 * Write one layer `sid`: for each step, its `entries` (`[key, value]`; a value
 * of `undefined` deletes the key), in pieces. Nothing points to the layer
 * until the position does. Returns `{ chunks, bytes, entries }` (`bytes`: the
 * JSON before deflate).
 */
export async function writeLayer({ queen, ns, id, sid, lock, steps, chunkChars = DEFAULT_CHUNK_CHARS, pieceBytes = DEFAULT_PIECE_BYTES }) {
  const size = Math.max(1024, Math.floor(chunkChars))
  const room = Math.max(1024, Math.floor(pieceBytes))
  let chunks = 0
  let bytes = 0
  let entries = 0
  let batch = []
  const send = async () => {
    if (!batch.length) return
    const part = batch
    batch = []
    await commitKv(queen, lock, (tx) => { for (const [n, value] of part) tx.kv.put(ns, chunkKey(id, sid, n), value, { forever: true }) })
  }
  const piece = async (s, kind, set, del) => {
    const json = `{"v":${LAYER_FORMAT},"s":${s},"k":${JSON.stringify(kind)},"set":[${set.join(',')}],"del":[${del.join(',')}]}`
    bytes += Buffer.byteLength(json)
    // Off the main thread (zlib's pool): the event loop runs on meanwhile.
    const b64 = (await deflate(Buffer.from(json, 'utf8'))).toString('base64')
    for (let i = 0; i < b64.length; i += size) {
      const more = i + size < b64.length
      batch.push([chunks++, more ? { d: b64.slice(i, i + size), m: 1 } : { d: b64.slice(i, i + size) }])
      if (batch.length >= OPS_PER_TX) await send()
    }
  }
  for (let s = 0; s < steps.length; s++) {
    const { kind, entries: list } = steps[s]
    let set = []
    let del = []
    let used = 0
    for (const [k, v] of list) {
      entries++
      // The limit is in UTF-8 bytes: a string's length counts UTF-16 units, up to 3 times fewer.
      if (v === undefined) {
        const j = JSON.stringify(k)
        del.push(j); used += Buffer.byteLength(j)
      } else {
        const j = JSON.stringify([k, v])
        set.push(j); used += Buffer.byteLength(j)
      }
      if (used >= room) {
        await piece(s, kind, set, del)
        set = []; del = []; used = 0
        await nextTurn()
      }
    }
    if (set.length || del.length) await piece(s, kind, set, del)
  }
  await send()
  return { chunks, bytes, entries }
}

/**
 * Read layer `sid` (`count` chunks) piece by piece, in order, and hand each
 * one to `onPiece({ s, k, set, del })`. Throws when a chunk is missing.
 */
export async function readLayer({ queen, ns, id, sid, count, onPiece }) {
  let parts = []
  for (let i = 0; i < count; i += KEYS_PER_READ) {
    const values = await readValues({ queen, ns, id, sid, from: i, to: Math.min(count, i + KEYS_PER_READ), count })
    for (const v of values) {
      parts.push(v.d)
      if (v.m) continue
      const raw = await inflate(Buffer.from(parts.join(''), 'base64'))
      parts = []
      const p = JSON.parse(raw.toString('utf8'))
      if (!p || p.v !== LAYER_FORMAT) {
        throw Object.assign(new Error(`feed snapshot ${sid} of ${id}: a piece of format ${p && p.v} (this client reads ${LAYER_FORMAT})`), { fatal: true, code: 'feed_snapshot_format' })
      }
      await onPiece(p)
      await nextTurn()
    }
  }
  if (parts.length) {
    throw Object.assign(new Error(`feed snapshot ${sid} of ${id}: its last chunk says more follows`), { fatal: true, code: 'feed_snapshot_missing' })
  }
}

/** Chunks `from` to `to` (exclusive) of layer `sid`, as their values, in order. */
async function readValues({ queen, ns, id, sid, from, to, count }) {
  const out = new Array(to - from)
  let ask = []
  for (let n = from; n < to; n++) ask.push(chunkKey(id, sid, n))
  for (let round = 0; ask.length > 0; round++) {
    const r = await queen.kv.getMany(ns, ask)
    const got = new Set()
    for (const row of r.rows || []) {
      const n = Number(row.key.slice(row.key.lastIndexOf('#') + 1))
      out[n - from] = row.value && typeof row.value.d === 'string' ? row.value : null
      got.add(row.key)
    }
    if ((r.missing || []).length) {
      throw Object.assign(new Error(
        `feed snapshot ${sid} of ${id} lacks ${r.missing.length} of its ${count} chunks (first: ${r.missing[0]}): ` +
        'the state cannot be loaded; start the stream over with { reset: true }'
      ), { fatal: true, code: 'feed_snapshot_missing' })
    }
    const left = ask.filter((k) => !got.has(k))
    if (left.length === ask.length || round > 64) {
      throw new Error(`feed snapshot: the KV read of ${id}'s chunks came back empty and cut`)
    }
    ask = left
  }
  for (let n = 0; n < out.length; n++) {
    if (!out[n]) {
      throw Object.assign(new Error(`feed snapshot ${sid} of ${id}: chunk ${from + n} is not a chunk`), { fatal: true, code: 'feed_snapshot_missing' })
    }
  }
  return out
}

/** Write the chunks of snapshot `sid` (format 1); nothing points to them until the position does. */
export async function writeChunks({ queen, ns, id, sid, chunks, lock }) {
  for (let i = 0; i < chunks.length; i += OPS_PER_TX) {
    const end = Math.min(chunks.length, i + OPS_PER_TX)
    await commitKv(queen, lock, (tx) => {
      for (let n = i; n < end; n++) tx.kv.put(ns, chunkKey(id, sid, n), { d: chunks[n] }, { forever: true })
    })
  }
}

/** The chunks of snapshot `sid`, in order; throws when one is missing. */
export async function readChunks({ queen, ns, id, sid, count }) {
  const out = new Array(count)
  for (let i = 0; i < count; i += KEYS_PER_READ) {
    let ask = []
    for (let n = i; n < Math.min(count, i + KEYS_PER_READ); n++) ask.push(chunkKey(id, sid, n))
    for (let round = 0; ask.length > 0; round++) {
      const r = await queen.kv.getMany(ns, ask)
      const got = new Set()
      for (const row of r.rows || []) {
        const n = Number(row.key.slice(row.key.lastIndexOf('#') + 1))
        out[n] = row.value && typeof row.value.d === 'string' ? row.value.d : null
        got.add(row.key)
      }
      if ((r.missing || []).length) {
        throw Object.assign(new Error(
          `feed snapshot ${sid} of ${id} lacks ${r.missing.length} of its ${count} chunks (first: ${r.missing[0]}): ` +
          'the state cannot be loaded; start the stream over with { reset: true }'
        ), { fatal: true, code: 'feed_snapshot_missing' })
      }
      const left = ask.filter((k) => !got.has(k))
      if (left.length === ask.length || round > 64) {
        throw new Error(`feed snapshot: the KV read of ${id}'s chunks came back empty and cut`)
      }
      ask = left
    }
  }
  for (let n = 0; n < count; n++) {
    if (typeof out[n] !== 'string') {
      throw Object.assign(new Error(`feed snapshot ${sid} of ${id}: chunk ${n} is not a chunk`), { fatal: true, code: 'feed_snapshot_missing' })
    }
  }
  return out
}

/** Delete keys, in transactions of at most 60 deletes. */
export async function deleteKeys({ queen, ns, keys, lock }) {
  for (let i = 0; i < keys.length; i += OPS_PER_TX) {
    const part = keys.slice(i, i + OPS_PER_TX)
    await commitKv(queen, lock, (tx) => { for (const k of part) tx.kv.delete(ns, k) })
  }
}

/**
 * The chunk keys of `id` in KV whose layer is not one of `keep` (a layer id,
 * or a set of them: left by an attempt that never committed).
 */
export async function strayChunkKeys({ queen, ns, id, keep }) {
  if (typeof queen.kv.getPrefix !== 'function') return []
  const live = keep instanceof Set ? keep : new Set(keep ? [keep] : [])
  const prefix = chunkPrefix(id)
  const out = []
  let after = null
  for (let page = 0; page < 1000; page++) {
    const r = await queen.kv.getPrefix(ns, prefix, { keysOnly: true, limit: 1000, after })
    for (const row of r.rows || []) {
      const key = typeof row === 'string' ? row : row.key
      const sid = key.slice(prefix.length, key.lastIndexOf('#'))
      if (!live.has(sid)) out.push(key)
    }
    if (!r.truncated || !r.nextAfter) break
    after = r.nextAfter
  }
  return out
}
