/**
 * What a feed stream is, for whoever looks at it from outside (the console):
 * the queues it reads, its steps in order, where its results go, and what it
 * does with a message that keeps failing.
 *
 * The active runner writes it to `queen-feed` / `<queryId>#about` in its first
 * commit after it becomes active, under the lock like the rest of the commit
 * (one record per query: every shard writes the same one). A stopped stream
 * keeps it, so it still says what it was. Nothing reads it back to run the
 * stream: the chain in the code is the stream.
 *
 * Functions are not described (a `.map(fn)` is `map`), and long values are
 * cut: a `where` with a list of 5,000 names keeps the first 50.
 */
import { createRequire } from 'node:module'

/** The key of a query's description in `queen-feed`. */
export function aboutKey(queryId) {
  return `${queryId}#about`
}

const FORMAT = 1
const MAX_TEXT = 200
const MAX_LIST = 50
const MAX_DEPTH = 4

let clientVersion = null
function client() {
  if (clientVersion === null) {
    try {
      clientVersion = `client-js ${createRequire(import.meta.url)('../../../../package.json').version}`
    } catch (_) {
      clientVersion = 'client-js'
    }
  }
  return clientVersion
}

/** A JSON-safe copy, cut to a size worth storing: long strings, long lists, deep objects, no functions. */
export function clip(v, depth = 0) {
  if (v === undefined || v === null) return null
  if (typeof v === 'string') return v.length > MAX_TEXT ? `${v.slice(0, MAX_TEXT)}…` : v
  if (typeof v === 'number') return Number.isFinite(v) ? v : String(v)
  if (typeof v === 'boolean') return v
  if (typeof v === 'function') return 'fn'
  if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString()
  if (Array.isArray(v)) {
    const out = v.slice(0, MAX_LIST).map((x) => clip(x, depth + 1))
    if (v.length > MAX_LIST) out.push(`… ${v.length - MAX_LIST} more`)
    return out
  }
  if (typeof v === 'object') {
    if (depth >= MAX_DEPTH) return '…'
    const o = {}
    for (const [k, x] of Object.entries(v)) if (x !== undefined && typeof x !== 'function') o[k] = clip(x, depth + 1)
    return o
  }
  return String(v)
}

const asList = (v) => (v === undefined || v === null ? null : (Array.isArray(v) ? v : [v]))

function whereOf(c) {
  return c && typeof c === 'object' ? clip({ field: c.field, op: c.op, value: c.value }) : null
}

/** One compiled step (Stream._compileFeed) as the description says it; `pipeStep` gives the clock it runs on. */
function stepOf(s, pipeStep) {
  if (s.kind === 'ops') {
    const out = { kind: 'ops', ops: s.ops.map((op) => (op.kind === 'filter' && op.where ? 'where' : op.kind)) }
    const where = s.ops.filter((op) => op.kind === 'filter' && op.where).map((op) => whereOf(op.where))
    if (where.length) out.where = where
    return out
  }
  const clock = pipeStep && pipeStep.clock ? pipeStep.clock.kind : null
  if (s.kind === 'window') {
    const out = { kind: 'window', name: s.name, window: clip(s.window.config), reducer: { kind: s.reducer.kind, ...clip(s.reducer.config || {}) } }
    if (clock) out.clock = clock
    return out
  }
  const config = clip(s.op.config || {})
  // pattern, join (two sides or many), gather, table: the name the chain gave it.
  const kind = s.kind === 'table' ? 'table' : (config.kind || s.kind)
  const out = { kind, name: s.name, config }
  if (clock) out.clock = clock
  return out
}

function sinkOf(sink) {
  if (!sink) return null
  if (sink.kind === 'foreach') return { kind: 'foreach' }
  if (sink.route) return { kind: 'route' }
  return { kind: 'queues', queues: clip(sink.queues || (sink.queueName ? [sink.queueName] : [])) }
}

/**
 * The description of one stream.
 *
 * @param {object} p
 * @param {object} p.stream - the compiled stream (Stream._compileFeed)
 * @param {object} p.pipeline - its Pipeline (the clock each step runs on)
 * @param {string} p.queryId
 * @param {{count:number}|null} p.shard
 * @param {'partition'|Function} p.shardBy
 * @param {{kind:string, queue?:string}} p.poison - the runner's onPoison
 * @param {boolean} p.exclusive
 * @param {string|number} p.lockTtl
 * @param {number} p.positionCommitMillis - how often a running stream writes its position at least, while the log moves
 */
export function describeFeedStream({ stream, pipeline, queryId, shard, shardBy, poison, exclusive, lockTtl, positionCommitMillis }) {
  const source = stream.source.options || {}
  const reads = {
    queues: clip(asList(source.queues ?? source.queue)),
    partitions: clip(asList(source.partitions ?? source.partition)),
    where: (stream.pushdown || []).map(whereOf).filter(Boolean)
  }
  if (source.kinds !== undefined) reads.kinds = clip(source.kinds)
  if (source.since !== undefined) reads.since = clip(source.since)
  const tables = [...new Set(pipeline.tables.flatMap((t) => t.queues || []))]
  if (tables.length) reads.tables = clip(tables)
  return {
    v: FORMAT,
    queryId,
    hash: stream.chainHash || null,
    client: client(),
    reads,
    steps: stream.steps.map((s, i) => stepOf(s, pipeline.steps[i])),
    sink: sinkOf(stream.sink),
    onPoison: poison.kind === 'deadLetter' ? { queue: poison.queue } : poison.kind,
    shards: shard ? { count: shard.count, by: typeof shardBy === 'function' ? 'function' : 'partition' } : null,
    exclusive: exclusive !== false,
    lockTtl: clip(lockTtl),
    positionCommitMillis: Number.isFinite(positionCommitMillis) ? positionCommitMillis : null
  }
}
