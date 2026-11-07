/**
 * FeedRunner — runs a Stream whose source is the tenant feed
 * (`Stream.fromFeed(queen, ...)`).
 *
 * A cycle reads one page of the feed from the runner's position and runs the
 * chain on each message, in log order: stateless operators and any number of
 * stateful steps (windows, patterns, joins, gathers, table joins; see
 * feed/pipeline.js). What the cycle produced commits in ONE transaction: the
 * pushes to the sink queues and the new position, a KV key (`queen-feed` /
 * queryId) written with `expect` on its version. Exactly once into Queen;
 * `.foreach` effects run before the position moves, so they are at least
 * once.
 *
 * Two positions, not a copy of the state at every commit. The record holds
 * the committed position and the position of the last SNAPSHOT: the whole
 * state of every step, written now and then (`snapshotEveryMillis`, and at a
 * stop) in chunks under its own KV keys (feed/snapshot.js), and named by the
 * commit that moves the position, in the same transaction. A restart loads
 * the snapshot and reads the feed again from the snapshot's position to the
 * committed one, sending nothing: the feed replays in one order with the
 * times it had, every step decides from those times, so the state comes back
 * as it was. A commit with no snapshot due writes no state at all; a state
 * that has not changed since the snapshot moves the snapshot's position with
 * the commit, at no cost; an empty state is a snapshot of no chunks.
 *
 * One runner at a time. Each queryId has a lock (`queen-feed/<queryId>`): the
 * process holding it runs, any other one with the same queryId waits on it
 * and takes over when the first stops or dies. Each commit is guarded by the
 * lock (`exclusive: false` turns this off: then the position's version alone
 * decides which process commits).
 *
 * Shards. `shard: { index, count }` runs one slice of the stream: the
 * partitions whose name hashes to `index` (the broker filters them, before
 * decoding a payload), or, with `shardBy: (msg) => key`, the messages whose
 * key does (filtered here). A shard has its own position, snapshots and lock
 * (`queen-feed/<queryId>/shard-<i>-of-<n>`); the outputs of every shard carry
 * the query's prefix, so no shard reads back what any shard wrote. Every
 * stateful step must group by the shard key or by a key made from it.
 *
 * A message whose function throws is tried again (`retries`, 3, with a short
 * pause), each time against the state from before it. Then `onPoison` says
 * what happens: 'stop' (the default) commits what came before it and stops;
 * 'skip' leaves it out; a queue (`queen.queue('feed-dlq')`) receives it.
 *
 * Commits come when the feed is caught up (at once, for low latency), every
 * `commitEveryMillis` while it catches up, and when `maxPendingOutputs`
 * outputs wait; a page the filters emptied moves the position at most every
 * `positionCommitMillis`. Reads go to followers.
 *
 * Windows and deadlines close on the feed's clock: the time the log committed
 * each message, which every queue shares and which only moves forward. On a
 * quiet feed the runner writes its position when a window or a deadline is
 * due: the write moves the broker's clock, and the next read closes it.
 *
 * A step can run on event time instead (`eventTime`, or after a step on
 * event time; feed/clock.js): it closes on its own watermark, takes the
 * messages of a pattern, join, gather or session in event-time order, and
 * with `idle` lets the feed's clock move its watermark when no message comes
 * (the runner then moves the clock on a quiet feed as above). The watermark
 * and the messages held for order are part of the state and its snapshot, so
 * a replay closes the same windows at the same times.
 *
 * Live state. The active runner answers questions about its state on an
 * ephemeral queue of its own (feed/query.js; `serveState`, on by default), and
 * `runner.state(step)` reads it in the same process.
 */

import crypto from 'node:crypto'
import * as logger from '../../utils/logger.js'
import { configHashOf } from '../util/configHash.js'
import { payloadOf } from '../operators/SinkOperator.js'
import { LOCK_NOT_HELD } from '../../locks/Locks.js'
import { conditionHolds } from '../../feed/Feed.js'
import { shardOf } from './feed/state.js'
import { Pipeline } from './feed/pipeline.js'
import { snapshotOf } from './feed/patterns.js'
import {
  decodeSnapshot, newSnapshotId, readChunks, deleteKeys, strayChunkKeys, DEFAULT_CHUNK_CHARS,
  DEFAULT_PIECE_BYTES, MAX_LAYERS, layersOf, pointerOf, layerKeys, writeLayer, readLayer
} from './feed/snapshot.js'
import { StateServer, queryQueueName } from './feed/query.js'
import { describeFeedStream, aboutKey, clip } from './feed/about.js'

export const POSITION_NAMESPACE = 'queen-feed'
const POSITION_COMMIT_MILLIS = 5000
// The error a `stopped` record keeps, at most.
const STOP_ERROR_MAX_CHARS = 500
const COMMIT_EVERY_MILLIS = 1000
const MAX_PENDING_OUTPUTS = 5000
const MAX_BACKOFF_MILLIS = 10000
const LOCK_TTL = '15s'
const RETRIES = 3
const RETRY_DELAY_MILLIS = 100
const SNAPSHOT_EVERY_MILLIS = 60000
const SNAPSHOT_MAX_ENTRIES = 200000
// Skips (messages the poison policy left out) remembered since the snapshot,
// for a replay to leave out the same ones; past this many, a snapshot is due.
const MAX_SKIPS = 1000
// A replay waits this long, at most, for a follower to apply the committed position.
const REPLAY_CATCH_UP_MILLIS = 60000
// A quiet feed with a window or deadline due: at most one clock-moving write
// this often, and a short read after it (the long poll would not wake for it).
const TICK_MIN_MILLIS = 500
const TICK_READ_WAIT_MILLIS = 100
// The broker's clock moves only with writes. When none came for this long
// and a deadline waits, one write tells this process the broker's time.
const CLOCK_SYNC_MILLIS = 5000
const QUERY_ID_MAX_BYTES = 200

const sha = (s) => crypto.createHash('sha256').update(s).digest('hex')

/** The prefix of every transaction id a query writes, every shard of it: its reads leave these out. */
export function outputPrefix(queryId) {
  return `fd-${sha(`queen-feed\n${queryId}`).slice(0, 8)}-`
}

/**
 * The transaction id of the `i`-th output of one feed entry: the same on every
 * retry. It starts with the query's prefix (every shard shares it, so no shard
 * reads back another's outputs); `scope` (a shard's id) makes the rest its own.
 */
export function feedOutputId(queryId, entry, i, epoch = 0, scope = queryId) {
  const h = sha(`${scope}\n${epoch}\n${entry.position}\n${entry.queue}\n${entry.partition}\n${entry.offset}\n${i}`)
  return `${outputPrefix(queryId)}${h.slice(0, 40)}`
}

/** The KV key and lock name of one shard of a query (or of the query, unsharded). */
export function shardId(queryId, shard) {
  return shard ? `${queryId}#shard${shard.index}of${shard.count}` : queryId
}

function lockName(queryId, shard) {
  return shard ? `queen-feed/${queryId}/shard-${shard.index}-of-${shard.count}` : `queen-feed/${queryId}`
}

function requireQueryId(queryId) {
  if (typeof queryId !== 'string' || queryId.length === 0 || Buffer.byteLength(queryId) > QUERY_ID_MAX_BYTES ||
      /[\u0000-\u001f\u007f#]/.test(queryId)) {
    throw new Error(
      `Stream.fromFeed: queryId is a non-empty string of at most ${QUERY_ID_MAX_BYTES} bytes, without '#' ` +
      `and control characters (it names a KV key and a lock), got ${JSON.stringify(queryId)}`
    )
  }
  return queryId
}

function requireShard(shard) {
  if (shard === undefined || shard === null) return null
  const { index, count } = shard
  if (!Number.isInteger(count) || count < 1 || count > 4096 || !Number.isInteger(index) || index < 0 || index >= count) {
    throw new Error(`Stream.fromFeed: shard is { index, count }, count from 1 to 4096 and index from 0 to count - 1, got ${JSON.stringify(shard)}`)
  }
  return count === 1 ? null : { index, count }
}

function versionOf(result, key) {
  for (const r of result?.results || []) {
    if (r && r.type === 'kv' && r.key === key && Number.isInteger(r.version)) return r.version
  }
  return null
}

function tsOf(entry) {
  const t = Date.parse(entry.ts || entry.createdAt || '')
  return Number.isNaN(t) ? 0 : t
}

const sleep = (ms, signal) => new Promise((resolve) => {
  if (signal?.aborted) return resolve()
  const t = setTimeout(done, ms)
  function done() {
    signal?.removeEventListener('abort', done)
    clearTimeout(t)
    resolve()
  }
  signal?.addEventListener('abort', done, { once: true })
})

function glob(pattern) {
  const re = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')
  return new RegExp(`^${re}$`)
}

function asList(v) {
  if (v === undefined || v === null) return null
  const l = Array.isArray(v) ? v : [v]
  return l.length ? l : null
}

function poisonTarget(onPoison) {
  if (onPoison === undefined || onPoison === null || onPoison === 'stop') return { kind: 'stop' }
  if (onPoison === 'skip') return { kind: 'skip' }
  const name = typeof onPoison === 'string' ? onPoison : onPoison && typeof onPoison.name === 'string' ? onPoison.name : null
  if (!name) {
    throw new Error("Stream.fromFeed: onPoison is 'stop', 'skip' or a queue for the messages that fail (queen.queue('feed-dlq'))")
  }
  return { kind: 'deadLetter', queue: name }
}

function freshPending() {
  // readTo / caughtUp: the feed time `next` is at, and whether the read was caught up there.
  return { outputs: [], next: null, work: 0, effects: 0, emitSeq: 0, readTo: null, caughtUp: null }
}

function clipText(s, max) {
  const t = typeof s === 'string' ? s : String(s)
  return t.length > max ? `${t.slice(0, max)}…` : t
}

export class FeedRunner {
  #stream
  #queen
  #queryId
  #id
  #shard
  #shardBy
  #opts
  #now
  #poison
  #reader = null
  #pipeline
  #sink = null
  #hash = null
  // The reads: what the broker filters, what this process filters.
  #brokerQueues = null
  #brokerPartitions = null
  #brokerWhere = []
  #brokerShard = null
  #localWhere = []
  #localPartitions = null
  #streamQueues = null
  #localShard = false
  #tableQueues = new Set()
  #lock = null
  #lockLost = false
  #active = false
  // The last commit.
  #position = null
  #version = 0
  #epoch = 0
  #resetToken = null
  #committedClock = 0
  // How many closes (windows, deadlines, runs, held messages, each one unit)
  // the stream has made since it started, and at the last commit. Closes come
  // in one order on every run, so this count is a cursor: a commit in the
  // middle of a long close records it, and a replay stops at it.
  #units = 0
  #committedUnits = 0
  // A replay stops its closes at this count (the stored record's), Infinity: no cap.
  #replayCap = Infinity
  // The time a close in progress runs up to: a commit in the middle of it records that clock.
  #burstTarget = null
  // The snapshot the stored record names: { id, chunks, bytes, position, clock, metas } (id null: empty).
  #snap = null
  #entriesSinceSnap = 0
  #snapWallAt = 0
  #oldKeys = []
  #hadStray = false
  // The poison policy's skips since the snapshot (committed, and since the last commit).
  #skips = []
  #pendingSkips = []
  #skipSet = new Set()
  // The version of `<queryId>#layout` this process runs under (0: absent); every commit checks it.
  #layoutVersion = 0
  // A new stream's state is empty where it starts: that position is its first
  // snapshot, with no chunks, taken from the first page read.
  #initialSnap = false
  // What the cycles produced since.
  #pending = freshPending()
  #clock = 0
  #replaying = false
  #poisonStop = null
  #lastCommitAt = 0
  #lastPositionCommit = 0
  // The feed time the committed position is at, and whether it was read caught up.
  #committedReadTo = 0
  #committedCaughtUp = false
  // Why the stream stops, for the record of its last commit: `{ reason, at, ... }`.
  #stopMark = null
  // stop() or the abort signal asked for the stop.
  #stopAsked = false
  // What the stream is (feed/about.js; undefined: not made yet), and whether
  // the next commit writes it: the first commit of each activation does.
  #about = undefined
  #aboutOwed = false
  #lastTickAt = 0
  #resolvedMs = 0
  #resolvedAt = 0
  #offset = -Infinity
  #lastNow = 0
  #nextWait = null
  #loop = null
  #stopping = false
  #stopAbort = new AbortController()
  #lastSaid = new Map()
  #server = null
  #metrics = {
    role: 'starting',
    cycles: 0,
    entriesRead: 0,
    emitted: 0,
    outputs: 0,
    effects: 0,
    commits: 0,
    conflicts: 0,
    errors: 0,
    retries: 0,
    poisoned: 0,
    skipped: 0,
    deadLettered: 0,
    late: 0,
    ticks: 0,
    takeovers: 0,
    snapshots: 0,
    snapshotBytes: 0,
    snapshotChunks: 0,
    snapshotsFull: 0,
    snapshotLayers: 0,
    snapshotMillis: 0,
    units: 0,
    closeCommits: 0,
    replays: 0,
    replayEntries: 0,
    replayMillis: 0,
    replaySkipped: 0,
    tableRows: 0,
    position: null,
    applied: null,
    lag: null,
    stopped: null
  }

  constructor({
    stream,
    queryId,
    from,
    waitMillis = 5000,
    maxRecords,
    maxBytes,
    reset = false,
    onError,
    abortSignal,
    positionCommitMillis = POSITION_COMMIT_MILLIS,
    commitEveryMillis = COMMIT_EVERY_MILLIS,
    maxPendingOutputs = MAX_PENDING_OUTPUTS,
    exclusive = true,
    lockTtl = LOCK_TTL,
    onPoison = 'stop',
    retries = RETRIES,
    retryDelayMillis = RETRY_DELAY_MILLIS,
    snapshotEveryMillis = SNAPSHOT_EVERY_MILLIS,
    snapshotMaxEntries = SNAPSHOT_MAX_ENTRIES,
    snapshotChunkChars = DEFAULT_CHUNK_CHARS,
    snapshotPieceBytes = DEFAULT_PIECE_BYTES,
    shard,
    shardBy = 'partition',
    serveState = true,
    serveWaitMillis,
    now
  }) {
    this.#stream = stream
    this.#queen = stream.source.queen
    this.#queryId = requireQueryId(queryId)
    this.#shard = requireShard(shard)
    this.#id = shardId(this.#queryId, this.#shard)
    if (shardBy !== 'partition' && typeof shardBy !== 'function') {
      throw new Error("Stream.fromFeed: shardBy is 'partition' (the default) or a function (msg) => key")
    }
    this.#shardBy = shardBy
    if (!Number.isInteger(retries) || retries < 0) throw new Error('Stream.fromFeed: retries is a whole number from 0')
    if (reset !== true && reset !== false && (typeof reset !== 'string' || reset.length === 0)) {
      throw new Error('Stream.fromFeed: reset is true (start over at every start) or a string (start over once per string)')
    }
    this.#poison = poisonTarget(onPoison)
    this.#now = typeof now === 'function' ? now : Date.now
    this.#opts = {
      from, waitMillis, maxRecords, maxBytes, reset, onError, abortSignal, positionCommitMillis,
      commitEveryMillis, maxPendingOutputs, exclusive, lockTtl, retries, retryDelayMillis,
      snapshotEveryMillis, snapshotMaxEntries, snapshotChunkChars, snapshotPieceBytes, serveState, serveWaitMillis
    }
  }

  /** Check the chain, take the lock if it is free, load the position and state, start the cycles (not awaited). */
  async start() {
    const { steps, sink, chainHash } = this.#stream
    if (!sink) throw new Error('Stream.fromFeed: end the chain with .to(queen.queue(...)) or .foreach(fn)')
    this.#sink = sink
    this.#pipeline = new Pipeline(steps)
    this.#hash = chainHash
    this.#planReads()

    this.#reader = this.#makeReader({ from: 'now' })
    // A broker without the feed (or a feed nobody can serve) fails run() here,
    // with the reason, instead of a stream that silently never runs.
    await this.#reader.probe()
    await this.#checkLayout()

    if (this.#opts.exclusive) {
      this.#lock = this.#queen.lock(lockName(this.#queryId, this.#shard), { ttl: this.#opts.lockTtl, retryMinMs: 250, retryMaxMs: 2000 })
      this.#lock.onLost((why) => {
        this.#lockLost = true
        // It can commit nothing more: a standby from now on.
        if (this.#active && !this.#stopping) this.#metrics.role = 'standby'
        this.#server?.stop()
        logger.warn('FeedRunner.lockLost', { queryId: this.#id, why })
      })
    }
    // Active now when the lock is free, so that a chain the stored state was
    // not written by fails run() itself; otherwise a standby, waiting.
    if (!this.#lock || (await this.#lock.acquire())) {
      try {
        await this.#becomeActive(true)
      } catch (err) {
        await this.#lock?.release().catch(() => {})
        throw err
      }
    } else {
      this.#metrics.role = 'standby'
    }
    logger.log('FeedRunner.start', { queryId: this.#id, role: this.#metrics.role, position: this.#position })
    if (this.#opts.abortSignal) {
      this.#opts.abortSignal.addEventListener('abort', () => {
        this.#stopAsked = true
        this.#stopping = true
        this.#stopAbort.abort()
      }, { once: true })
    }
    this.#loop = this.#run()
  }

  /** Stop after the cycle in hand, commit what is pending (and a snapshot), give the lock back; resolves once done. */
  async stop() {
    this.#stopAsked = true
    this.#stopping = true
    this.#stopAbort.abort()
    try { await this.#loop } catch (_) { /* reported by onError */ }
    await this.#reader?.close()
    this.#server?.stop()
  }

  /** Resolves when the runner has stopped: by stop(), the abort signal, a poison message, or a broker without the feed. */
  get done() {
    return this.#loop
  }

  /** This runner's shard, `{ index, count }`, or null. */
  get shard() {
    return this.#shard
  }

  metrics() {
    const m = { ...this.#metrics }
    m.pendingOutputs = this.#pending.outputs.length
    if (this.#shard) m.shard = { ...this.#shard }
    if (this.#pipeline && this.#pipeline.hasState) {
      m.open = this.#pipeline.open
      m.stateKeys = this.#pipeline.stateKeys
      m.steps = this.#pipeline.stepMetrics(this.#clock)
      m.late = this.#pipeline.late
      m.clock = this.#clock > 0 ? new Date(this.#clock).toISOString() : null
      const w = this.#pipeline.watermarkAt(this.#clock)
      if (w !== null) m.watermark = new Date(w).toISOString()
      m.snapshotPosition = this.#snap ? this.#snap.position : null
      m.units = this.#units
    }
    return m
  }

  /** The live state of one step, in this process: `get(key)` and `list(limit)`. */
  state(step) {
    const p = this.#pipeline
    return {
      get: (key) => p.inspect(step, key),
      list: (limit = 100) => p.list(step, limit)
    }
  }

  // ---- reads ----------------------------------------------------------------

  /** What the broker filters and what this process filters, for the source, the tables and the shard. */
  #planReads() {
    const source = this.#stream.source.options || {}
    const queues = asList(source.queues ?? source.queue)
    const partitions = asList(source.partitions ?? source.partition)
    const tables = this.#pipeline.tables
    for (const t of tables) for (const q of t.queues) this.#tableQueues.add(q)
    // Never read back what this query wrote: its outputs carry its prefix.
    const self = { field: 'transactionId', op: '!prefix', value: outputPrefix(this.#queryId) }
    const where = this.#stream.pushdown || []
    if (tables.length === 0) {
      this.#brokerQueues = queues
      this.#brokerPartitions = partitions
      this.#brokerWhere = [...where, self]
      if (this.#shard && this.#shardBy === 'partition') this.#brokerShard = { count: this.#shard.count, index: this.#shard.index }
    } else {
      // A table queue is read whole: its rows pass no filter of the stream's
      // and no shard, so those filters move here, for the stream's messages.
      this.#brokerQueues = queues ? [...new Set([...queues, ...this.#tableQueues])] : null
      this.#brokerWhere = [self]
      this.#localWhere = where
      this.#localPartitions = partitions ? new Set(partitions) : null
      this.#streamQueues = queues ? queues.map(glob) : null
      if (this.#shard && this.#shardBy === 'partition') {
        // The broker splits only the stream's own queues: never a table queue,
        // which every shard reads whole. A pattern that also matches a table
        // queue cannot say that to the broker: then the split happens here.
        const split = queues ? queues.filter((q) => !this.#tableQueues.has(q)) : null
        const hitsTable = split && split.some((pat) => /[*?]/.test(pat) && [...this.#tableQueues].some((t) => glob(pat).test(t)))
        if (split && split.length && !hitsTable) this.#brokerShard = { count: this.#shard.count, index: this.#shard.index, queues: split }
        else this.#localShard = true
      }
    }
    if (this.#shard && typeof this.#shardBy === 'function') this.#localShard = true
  }

  /** Where a message goes: a table's rows, the stream, or nowhere (another shard's, filtered out here). */
  async #route(entry) {
    if (this.#tableQueues.has(entry.queue)) return 'table'
    if (this.#streamQueues && !this.#streamQueues.some((re) => re.test(entry.queue))) return 'skip'
    if (this.#localPartitions && !this.#localPartitions.has(entry.partition)) return 'skip'
    for (const c of this.#localWhere) if (!conditionHolds(c, entry)) return 'skip'
    if (this.#localShard && typeof this.#shardBy !== 'function' && shardOf(entry.partition, this.#shard.count) !== this.#shard.index) return 'skip'
    return 'stream'
  }

  /**
   * Whether this shard takes a stream message, by `shardBy`: run inside the
   * message's unit, so a function that throws meets the poison policy -- in
   * shard 0 only, which takes such a message; the other shards leave it.
   */
  async #owns(entry) {
    if (!this.#localShard || typeof this.#shardBy !== 'function') return true
    let key
    try {
      key = await this.#shardBy(entry)
    } catch (err) {
      if (this.#shard.index === 0) throw err
      return false
    }
    return shardOf(String(key), this.#shard.count) === this.#shard.index
  }

  #makeReader({ from, since, tablesOnly = false }) {
    const source = this.#stream.source.options || {}
    if (tablesOnly) {
      return this.#queen.feed({
        queues: [...this.#tableQueues],
        followerUrls: source.followerUrls,
        where: [{ field: 'transactionId', op: '!prefix', value: outputPrefix(this.#queryId) }],
        from,
        waitMillis: 0,
        maxRecords: this.#opts.maxRecords ?? source.maxRecords,
        maxBytes: this.#opts.maxBytes ?? source.maxBytes
      })
    }
    return this.#queen.feed({
      queues: this.#brokerQueues ?? undefined,
      partitions: this.#brokerPartitions ?? undefined,
      kinds: source.kinds,
      since,
      followerUrls: source.followerUrls,
      where: this.#brokerWhere,
      shard: this.#brokerShard ?? undefined,
      from,
      waitMillis: this.#opts.waitMillis,
      maxRecords: this.#opts.maxRecords ?? source.maxRecords,
      maxBytes: this.#opts.maxBytes ?? source.maxBytes
    })
  }

  /**
   * The shard count (and how shards split: by partition or by a function) is
   * part of the stream, kept in `queen-feed` / `<queryId>#layout`. Another
   * one, or a sharded run of a query that ran whole (or the other way), is
   * refused: keys would move between shards with their state. Every commit
   * checks the layout's version, so a process still running the old layout
   * after a `reset` changed it commits nothing more, and stops.
   */
  async #checkLayout() {
    const ns = POSITION_NAMESPACE
    const key = `${this.#queryId}#layout`
    const count = this.#shard ? this.#shard.count : 1
    const by = typeof this.#shardBy === 'function' ? 'function' : 'partition'
    const reset = this.#opts.reset
    const row = await this.#queen.kv.get(ns, key)
    const stored = row && row.found && row.value && Number.isInteger(row.value.shards) ? row.value : null
    const resetting = reset === true || (typeof reset === 'string' && (!stored || stored.resetToken !== reset))
    if (stored && stored.shards === count && (count === 1 || (stored.shardBy ?? 'partition') === by)) {
      this.#layoutVersion = row.version || 0
      return
    }
    if (!stored && count === 1) {
      this.#layoutVersion = 0
      return
    }
    if (!stored) {
      // First sharded run: refuse when the query ran whole and kept a position.
      const whole = await this.#queen.kv.get(ns, this.#queryId)
      if (whole && whole.found && !resetting) {
        throw Object.assign(new Error(
          `${this.#queryId} ran as one stream (its position is in KV); running it in ${count} shards would move keys ` +
          'between shards with their state: run it with { reset: true } (or a reset string) to start the shards over'
        ), { code: 'feed_layout_changed' })
      }
    } else if (!resetting) {
      throw Object.assign(new Error(
        `${this.#queryId} runs in ${stored.shards} shard${stored.shards === 1 ? '' : 's'} split by ${stored.shardBy ?? 'partition'}; ` +
        `running it in ${count} split by ${by} would move keys between shards with their state: run it as before, ` +
        'or with { reset: true } (or a reset string) to start over'
      ), { code: 'feed_layout_changed' })
    }
    const value = { shards: count, shardBy: by, updatedAt: new Date().toISOString() }
    if (typeof reset === 'string') value.resetToken = reset
    const tx = this.#queen.transaction()
    if (count === 1) tx.kv.delete(ns, key)
    else if (row && row.found) tx.kv.put(ns, key, value, { expect: row.version, required: true, forever: true })
    else tx.kv.putIfAbsent(ns, key, value, { required: true, forever: true })
    const res = await tx.commit()
    if (res && res.success === false) {
      // Another process wrote it first: read it again, and judge that one.
      if (res.reason === 'kv_precondition') return this.#checkLayout()
      throw new Error(`FeedRunner: the shard layout could not be written (${res.reason || 'unknown'})`)
    }
    if (count === 1) {
      this.#layoutVersion = 0
    } else {
      const v = versionOf(res, key)
      this.#layoutVersion = v !== null ? v : ((await this.#queen.kv.get(ns, key)).version || 0)
    }
  }

  /** The layout changed under this process: it stops (a newer deploy runs the stream). */
  async #layoutMoved() {
    const row = await this.#queen.kv.get(POSITION_NAMESPACE, `${this.#queryId}#layout`)
    const v = row && row.found ? row.version || 0 : 0
    if (v === this.#layoutVersion) return false
    throw Object.assign(new Error(
      `the shard layout of ${this.#queryId} changed while this process ran it (${row && row.found ? `${row.value?.shards} shards` : 'one stream'} now): ` +
      'another deploy runs it; this process stops'
    ), { fatal: true, code: 'feed_layout_changed' })
  }

  // ---- the loop -------------------------------------------------------------

  async #run() {
    let backoff = 500
    while (!this.#stopping) {
      try {
        if (!this.#active) {
          if (this.#lock && !this.#lock.held) {
            this.#metrics.role = 'standby'
            const got = await this.#lock.acquire({ wait: '1h', signal: this.#stopAbort.signal })
            if (!got || this.#stopping) continue
            this.#metrics.takeovers++
          }
          await this.#becomeActive(false)
          backoff = 500
        }
        if (this.#lock && (this.#lockLost || !this.#lock.held)) {
          this.#standDown('lost')
          continue
        }
        await this.#cycle()
        backoff = 500
      } catch (err) {
        // stop() cancelled the read in flight: what the pages before it did
        // is still pending, and the stop below commits it.
        if (this.#stopping && err && err.aborted) break
        // stop() came while a message was being retried: leave it for later.
        if (err && err.stopping) {
          if (this.#active) this.#rollback()
          break
        }
        if (err && err.fatal) {
          this.#report(err, { phase: 'load', position: this.#position })
          this.#metrics.stopped = { reason: 'config', error: err.message }
          this.#stopping = true
          break
        }
        this.#metrics.errors++
        if (this.#active) this.#rollback()
        if (err && err.code === LOCK_NOT_HELD) {
          this.#standDown('expired')
          continue
        }
        this.#report(err, { phase: this.#active ? 'cycle' : 'load', position: this.#position })
        // A broker without the feed will not grow one: stop rather than retry.
        if (err && err.noFeedRoute) {
          this.#stopping = true
          break
        }
        await sleep(backoff, this.#stopAbort.signal)
        backoff = Math.min(backoff * 2, MAX_BACKOFF_MILLIS)
      }
    }
    // Stopping: what is done and not yet committed goes in now, with a
    // snapshot, so the next start has nothing to read again.
    const snapshotOwed = this.#pipeline.hasState && this.#pipeline.changedSinceSnapshot
    // Asked to stop: the record of this last commit says so (`stopped`), so
    // that a stream stopped on purpose is not taken for one that died.
    // (Only at a position: a stream stopped before its first read has none to write.)
    const at = Number.isInteger(this.#pending.next) ? this.#pending.next : this.#position
    const asked = this.#stopAsked && this.#metrics.stopped === null && Number.isInteger(at)
    if (this.#active && (this.#hasPending() || snapshotOwed || asked) && !(this.#lock && !this.#lock.held) && this.#poisonStop === null) {
      if (asked) this.#stopMark = { reason: 'stop', at: new Date(this.#now()).toISOString() }
      try {
        await this.#commit()
      } catch (err) {
        this.#report(err, { phase: 'stop', position: this.#position })
      } finally {
        this.#stopMark = null
      }
    }
    this.#active = false
    this.#server?.stop()
    if (this.#lock) await this.#lock.release().catch(() => {})
    this.#metrics.role = 'stopped'
  }

  async #becomeActive(first) {
    await this.#load(first)
    this.#lockLost = false
    this.#active = true
    this.#metrics.role = 'active'
    // The first commit of each activation writes what the stream is (#aboutValue).
    this.#aboutOwed = true
    if (this.#opts.serveState && this.#queen.ephemeral) {
      this.#server?.stop()
      this.#server = new StateServer(
        this.#queen,
        queryQueueName(this.#queryId, this.#shard ? this.#shard.index : 0),
        (req) => this.#answer(req),
        this.#opts.serveWaitMillis ? { waitMillis: this.#opts.serveWaitMillis } : undefined
      )
      this.#server.start()
    }
  }

  /** The lock is gone: drop what was not committed and wait to hold it again. */
  #standDown(why) {
    this.#rollback()
    this.#initialSnap = false
    this.#active = false
    this.#server?.stop()
    this.#metrics.role = 'standby'
    logger.log('FeedRunner.standby', { queryId: this.#id, why })
  }

  /**
   * What this stream reads, its steps and where its results go (feed/about.js),
   * for `queen-feed` / `<queryId>#about`: the console reads it. Made once per
   * process; null when it cannot be made (the stream runs without it).
   */
  #aboutValue() {
    if (this.#about === undefined) {
      try {
        this.#about = describeFeedStream({
          stream: this.#stream,
          pipeline: this.#pipeline,
          queryId: this.#queryId,
          shard: this.#shard,
          shardBy: this.#shardBy,
          poison: this.#poison,
          exclusive: this.#opts.exclusive,
          lockTtl: this.#opts.lockTtl,
          positionCommitMillis: this.#opts.positionCommitMillis
        })
      } catch (err) {
        logger.warn('FeedRunner.about', { queryId: this.#id, error: err?.message })
        this.#about = null
      }
    }
    return this.#about
  }

  #report(err, ctx) {
    logger.error('FeedRunner', { queryId: this.#id, error: err?.message, ...ctx })
    if (typeof this.#opts.onError === 'function') {
      try { this.#opts.onError(err, { queryId: this.#queryId, shard: this.#shard, ...ctx }) } catch (_) { /* the hook's own */ }
      return
    }
    // No hook: say it on the console, once per message every 30 s, so a
    // stream that cannot run never looks like a stream with nothing to do.
    const msg = err && err.message ? err.message : String(err)
    const now = Date.now()
    if (this.#lastSaid.get(msg) === undefined || now - this.#lastSaid.get(msg) >= 30000) {
      this.#lastSaid.set(msg, now)
      const retrying = (ctx.phase === 'cycle' || ctx.phase === 'load') && !(err && err.noFeedRoute)
      console.error(`[queen feed] ${this.#id}: ${msg}${retrying ? ' (retrying)' : ''}`)
    }
  }

  /** A question from outside (feed/query.js), answered from the live state. */
  #answer(req) {
    const base = {
      shard: this.#shard ? this.#shard.index : 0,
      shards: this.#shard ? this.#shard.count : 1,
      role: this.#metrics.role,
      position: this.#reader ? this.#reader.position : this.#position,
      committed: this.#position
    }
    if (req.op === 'status') return { ...base, metrics: this.metrics() }
    if (!this.#pipeline.hasState) return { ...base, error: 'this stream keeps no state' }
    if (req.op === 'get') return { ...base, ...this.#pipeline.inspect(req.step, req.key) }
    if (req.op === 'list') {
      const limit = Math.min(Math.max(Number(req.limit) || 100, 1), 1000)
      return { ...base, ...this.#pipeline.list(req.step, limit) }
    }
    return { ...base, error: `unknown question ${JSON.stringify(req.op)} (get, list, status)` }
  }

  // ---- one cycle ------------------------------------------------------------

  async #cycle() {
    let wait = this.#opts.waitMillis
    if (this.#nextWait !== null) {
      wait = this.#nextWait
      this.#nextWait = null
    }
    // stop() cancels a long poll in flight: nothing of it was processed yet.
    const page = await this.#reader.read({ waitMillis: wait, signal: this.#stopAbort.signal })
    this.#metrics.cycles++
    this.#metrics.entriesRead += page.entries.length
    this.#metrics.applied = page.applied
    this.#metrics.lag = Number.isInteger(page.applied) && Number.isInteger(page.next)
      ? Math.max(0, page.applied - page.next + 1)
      : null
    // The broker's clock as of the read. It is never ahead of the broker's
    // time when the answer left, so `resolved - now` is a lower bound of how
    // far the broker's clock is from this one; the best recent bound gives
    // the broker's time now, never later than it is.
    // The estimate decides only when to write a tick; windows close on what
    // the broker answers, so a wrong estimate costs a write or some delay,
    // never a window closed early.
    const resolved = Date.parse(page.resolvedTs || '')
    if (!Number.isNaN(resolved)) {
      const now = this.#now()
      // This machine's clock was set back: learn the distance again.
      if (now < this.#lastNow) this.#offset = -Infinity
      this.#lastNow = now
      if (resolved > this.#resolvedMs) {
        this.#resolvedMs = resolved
        this.#resolvedAt = now
      }
      this.#offset = Math.max(this.#offset, resolved - now)
    }
    // Lost the lock while the read was out: what it read is the next holder's.
    if (this.#lock && (this.#lockLost || !this.#lock.held)) {
      this.#standDown('lost')
      return
    }
    if (this.#initialSnap) {
      this.#initialSnap = false
      const first = page.entries.length ? page.entries[0] : null
      this.#snap = {
        id: null,
        chunks: 0,
        bytes: 0,
        position: first ? first.position : page.next,
        clock: first ? tsOf(first) : (Number.isNaN(resolved) ? 0 : resolved),
        metas: this.#pipeline.metas(),
        // Nothing closed yet where the stream starts: a replay from here counts its closes from this.
        units: this.#units
      }
      this.#pipeline.snapshotTaken()
    }

    const r = await this.#processPage(page, Number.isNaN(resolved) ? null : resolved)
    if (r === 'abandon') return
    if (r === 'stop') {
      await this.#stopOnPoison()
      return
    }
    await this.#settle(page)
  }

  /** The entries of a page by position: one position is one transaction, whose messages share a time. */
  #groups(entries, below = Infinity) {
    const groups = []
    for (const e of entries) {
      if (e.position >= below) break
      const last = groups[groups.length - 1]
      if (last && last.position === e.position) last.entries.push(e)
      else groups.push({ position: e.position, entries: [e] })
    }
    return groups
  }

  /** Every message of a page, in order, then the clock to what the read resolved. */
  async #processPage(page, resolved) {
    for (const g of this.#groups(page.entries)) {
      const ref = { cp: this.#checkpoint() }
      const r = await this.#processGroup(g, ref)
      if (r === 'abandon') return 'abandon'
      if (r === 'stop') {
        // The whole transaction the poisoned message came in stays for later
        // (from the last commit, when one came in the middle of a close).
        this.#restore(ref.cp)
        this.#pending.next = g.position
        return 'stop'
      }
      this.#pending.next = g.position + 1
      this.#readTo(tsOf(g.entries[0]), false)
      // Enough waiting outputs: commit them now, between two positions.
      if (this.#pending.outputs.length >= this.#opts.maxPendingOutputs) {
        if ((await this.#commit()) !== 'ok') return 'abandon'
      }
    }
    if (this.#pipeline.hasState && this.#pipeline.needsFeedClock) {
      const ref = { cp: this.#checkpoint() }
      const t = Math.max(this.#clock, resolved ?? 0)
      this.#clock = t
      const r = await this.#advanceTo(t, ref)
      if (r === 'abandon') return 'abandon'
      if (r === 'stop') {
        this.#restore(ref.cp)
        this.#pending.next = page.next
        this.#readTo(resolved ?? 0, page.caughtUp === true)
        return 'stop'
      }
    }
    this.#pending.next = page.next
    this.#readTo(resolved ?? 0, page.caughtUp === true)
    return 'ok'
  }

  /** The feed time the pending position is at (it only moves forward), and whether the read was caught up there. */
  #readTo(t, caughtUp) {
    const p = this.#pending
    p.readTo = Math.max(p.readTo ?? this.#committedReadTo, Number.isFinite(t) ? t : 0)
    p.caughtUp = caughtUp
  }

  async #processGroup(g, ref = null) {
    // One position is one transaction of the log: its messages share a time.
    const t = Math.max(this.#clock, tsOf(g.entries[0]))
    if (this.#pipeline.hasState) {
      // Before the group's messages: a commit in the middle of this close names the group's position, not past it.
      const r = await this.#advanceTo(t, ref)
      if (r !== 'ok') return r
    }
    this.#clock = t
    // A table row of the transaction counts before its other messages.
    const routed = []
    for (const entry of g.entries) routed.push({ entry, route: await this.#route(entry) })
    routed.sort((a, b) => (a.route === 'table' ? 0 : 1) - (b.route === 'table' ? 0 : 1))
    for (const { entry, route } of routed) {
      if (route === 'skip') continue
      if (route === 'table') {
        if ((await this.#unit({ entry }, () => this.#pipeline.applyRow(entry))) === 'stop') return 'stop'
        this.#metrics.tableRows++
        this.#pending.work++
        continue
      }
      const r = await this.#unit({ entry }, async (ctr) => {
        if (!(await this.#owns(entry))) return
        await this.#pipeline.entry(entry, t, (env, ent) => this.#out(env, ent, ctr))
      })
      if (r === 'stop') return 'stop'
      if (this.#pipeline.hasState) this.#pending.work++
    }
    this.#entriesSinceSnap += g.entries.length
    // An event-time step may have moved its watermark. After the group's
    // messages: a commit in the middle of this close names the next position.
    if (this.#pipeline.hasState) {
      this.#pending.next = g.position + 1
      this.#readTo(t, false)
      const r = await this.#advanceTo(t, ref)
      if (r !== 'ok') return r
    }
    return 'ok'
  }

  /**
   * Close what is due at `t`; each result is delivered as a unit of its own,
   * and counted (`#units`). When many results wait (millions of windows that
   * end at one time), they commit in the middle of the close, after a batch;
   * the commit records the count, and `ref.cp` becomes the point a stop comes
   * back to. A replay stops at the count the stored record names.
   */
  async #advanceTo(t, ref = null) {
    if (!Number.isFinite(t)) return 'ok'
    const live = !this.#replaying
    const maxUnits = live || !Number.isFinite(this.#replayCap) ? Infinity : Math.max(0, this.#replayCap - this.#units)
    const outer = this.#burstTarget
    this.#burstTarget = outer === null ? t : Math.max(outer, t)
    try {
      return await this.#pipeline.advance(t, async (em, run) => {
        const r = await this.#unit({ emit: em }, (ctr) => run((env, ent) => this.#out(env, ent, ctr)))
        if (r !== 'stop') this.#units++
        return r
      }, {
        maxUnits,
        onBatch: live
          ? async () => {
            if (this.#pending.outputs.length < this.#opts.maxPendingOutputs) return 'ok'
            this.#metrics.closeCommits++
            if ((await this.#commit()) !== 'ok') return 'abandon'
            if (ref) ref.cp = this.#checkpoint()
            return 'ok'
          }
          : null
      })
    } finally {
      this.#burstTarget = outer
    }
  }

  /**
   * One result at the end of the chain: the queues it goes to (pushed with
   * the next commit) or the `.foreach` function. Ids of outputs that come from
   * a message are made from it; the others' from the commit. A replay sends
   * nothing: it was all sent before.
   */
  async #out(env, entry, ctr) {
    if (this.#replaying) return
    this.#metrics.emitted++
    const ctx = env.derived ? env.ctx : entry
    if (this.#sink.kind === 'foreach') {
      const fctx = env.derived || !entry
        ? env.ctx
        : { position: entry.position, queue: entry.queue, partition: entry.partition, partitionId: entry.partitionId, offset: entry.offset, transactionId: entry.transactionId }
      await this.#sink.fn(env.value, fctx)
      this.#metrics.effects++
      this.#pending.effects++
      return
    }
    const fallback = env.derived ? env.key : (entry ? entry.partition : env.key)
    const targets = await this.#sink.targetsFor(env.value, ctx, fallback)
    for (const target of targets) {
      const transactionId = entry
        ? feedOutputId(this.#queryId, entry, ctr.n++, this.#epoch, this.#id)
        : this.#emitId()
      this.#pending.outputs.push({ queue: target.queue, partition: target.partition, data: payloadOf(env.value), transactionId })
    }
  }

  #emitId() {
    const seq = this.#pending.emitSeq++
    const h = sha(`${this.#id}\n${this.#epoch}\nemit\n${this.#version}\n${seq}`)
    return `${outputPrefix(this.#queryId)}${h.slice(0, 40)}`
  }

  /**
   * Run `fn` (a message, or one result of a step) until it passes, at most
   * `retries` more times, each time from the state it saw the first time.
   * Then the poison policy: 'ok', 'skipped', 'deadLettered' or 'stop'. In a
   * replay a failure is left out at once: the run that sent it skipped it,
   * or it would not have committed past it.
   */
  async #unit(subject, fn) {
    if (this.#replaying) {
      // What the run before left out (its poison policy), a replay leaves out.
      if (this.#skipSet.has(this.#skipId(subject))) {
        this.#metrics.replaySkipped++
        return 'ok'
      }
      for (let attempt = 0; attempt <= this.#opts.retries; attempt++) {
        const cp = this.#checkpoint()
        try {
          await fn({ n: 0 })
          return 'ok'
        } catch (err) {
          this.#restore(cp)
          if (attempt === this.#opts.retries) {
            this.#metrics.replaySkipped++
            logger.warn('FeedRunner.replaySkip', { queryId: this.#id, error: err?.message, position: subject.entry?.position })
            return 'ok'
          }
          await sleep(this.#opts.retryDelayMillis * 2 ** attempt, this.#stopAbort.signal)
        }
      }
      return 'ok'
    }
    let lastErr = null
    for (let attempt = 0; attempt <= this.#opts.retries; attempt++) {
      const cp = this.#checkpoint()
      try {
        await fn({ n: 0 })
        return 'ok'
      } catch (err) {
        this.#restore(cp)
        lastErr = err
        if (this.#stopping) throw Object.assign(new Error('stopping'), { stopping: true })
        if (attempt < this.#opts.retries) {
          this.#metrics.retries++
          await sleep(this.#opts.retryDelayMillis * 2 ** attempt, this.#stopAbort.signal)
        }
      }
    }
    return this.#poisoned(subject, lastErr, this.#opts.retries + 1)
  }

  /** The identity of a unit, the same on a replay: a message by its coordinates, a result by its step, key, time and kind. */
  #skipId(subject) {
    if (subject.entry) {
      const e = subject.entry
      return `e\u001f${e.position}\u001f${e.queue}\u001f${e.partition}\u001f${e.offset}`
    }
    const em = subject.emit
    const id = `r\u001f${em.step ?? ''}\u001f${em.key}\u001f${em.time ?? ''}\u001f${em.ctx?.kind ?? ''}`
    // A message an event-time step held: its place in the step's order (the same on a replay).
    return em.id ? `${id}\u001f${em.id}` : id
  }

  #poisoned(subject, err, attempts) {
    this.#metrics.poisoned++
    if (this.#poison.kind !== 'stop' && this.#pipeline.hasState) this.#pendingSkips.push(this.#skipId(subject))
    // A feed message: the unit's own, or one an event-time step held and took in its turn.
    const entry = subject.entry || subject.emit?.entry || null
    const what = entry
      ? {
          position: entry.position,
          queue: entry.queue,
          partition: entry.partition,
          offset: entry.offset,
          transactionId: entry.transactionId
        }
      : { step: subject.emit.step ?? null, result: subject.emit.ctx.kind, key: subject.emit.key }
    const where = entry
      ? `the message at position ${what.position} (${what.queue}/${what.partition}, offset ${what.offset})`
      : `the ${what.result} result for key ${JSON.stringify(what.key)}`
    const reason = err && err.message ? err.message : String(err)
    if (this.#poison.kind === 'skip') {
      this.#metrics.skipped++
      this.#report(Object.assign(new Error(`${where} failed ${attempts} times and was skipped: ${reason}`), { cause: err }),
        { phase: 'poison', action: 'skipped', attempts, ...what })
      return 'skipped'
    }
    if (this.#poison.kind === 'deadLetter') {
      const record = {
        queryId: this.#queryId,
        attempts,
        error: { name: err?.name ?? 'Error', message: reason },
        ...(entry
          ? { entry: snapshotOf(entry) }
          : { result: { ...subject.emit.ctx, value: subject.emit.value, waiting: subject.emit.state?.msg } })
      }
      if (this.#shard) record.shard = { ...this.#shard }
      // A message an event-time step held is one of maybe several items of it
      // (a .flatMap() before): its place in the step's order tells them apart.
      const held = subject.emit?.release ? `\n${subject.emit.step}\n${subject.emit.id}` : ''
      const id = entry
        ? `${outputPrefix(this.#queryId)}${sha(`${this.#id}\n${this.#epoch}\ndlq\n${what.position}\n${what.queue}\n${what.partition}\n${what.offset}${held}`).slice(0, 40)}`
        : this.#emitId()
      this.#pending.outputs.push({
        queue: this.#poison.queue,
        partition: entry ? entry.partition : String(subject.emit.key),
        data: record,
        transactionId: id
      })
      this.#metrics.deadLettered++
      this.#report(Object.assign(new Error(`${where} failed ${attempts} times and went to ${this.#poison.queue}: ${reason}`), { cause: err }),
        { phase: 'poison', action: 'deadLettered', attempts, ...what })
      return 'deadLettered'
    }
    this.#poisonStop = { where, what, attempts, err, reason }
    return 'stop'
  }

  /** onPoison 'stop': commit what came before the message, then stop there. */
  async #stopOnPoison() {
    const p = this.#poisonStop
    // This commit's record says why the stream stopped, and at which message:
    // it is written even with nothing else to commit.
    this.#stopMark = {
      reason: 'poison',
      at: new Date(this.#now()).toISOString(),
      error: clipText(p.reason, STOP_ERROR_MAX_CHARS),
      attempts: p.attempts,
      ...clip(p.what)
    }
    let r
    try {
      r = await this.#commit()
    } finally {
      this.#stopMark = null
    }
    if (r !== 'ok') {
      // Another commit won: the stream reads on from the stored position.
      this.#poisonStop = null
      return
    }
    const error = Object.assign(
      new Error(`${p.where} failed ${p.attempts} times: ${p.reason}. The stream stopped there: fix the function and start it again, ` +
        "or run it with onPoison: 'skip' or a dead-letter queue"),
      { cause: p.err, code: 'feed_poison', ...p.what }
    )
    this.#metrics.stopped = { reason: 'poison', error: p.reason, attempts: p.attempts, ...p.what }
    this.#report(error, { phase: 'poison', action: 'stopped', attempts: p.attempts, ...p.what })
    this.#stopping = true
  }

  #checkpoint() {
    return {
      state: this.#pipeline.checkpoint(),
      outputs: this.#pending.outputs.length,
      work: this.#pending.work,
      effects: this.#pending.effects,
      emitSeq: this.#pending.emitSeq,
      clock: this.#clock,
      units: this.#units,
      late: this.#pipeline.late,
      skips: this.#pendingSkips.length,
      // A unit tried again counts its results once.
      emitted: this.#metrics.emitted
    }
  }

  #restore(cp) {
    this.#pipeline.restore(cp.state)
    this.#pending.outputs.length = cp.outputs
    this.#pending.work = cp.work
    this.#pending.effects = cp.effects
    this.#pending.emitSeq = cp.emitSeq
    this.#clock = cp.clock
    this.#units = cp.units
    this.#pipeline.late = cp.late
    this.#pendingSkips.length = cp.skips
    this.#metrics.emitted = cp.emitted
  }

  /** Back to the last commit: what was produced since is dropped, and read again. */
  #rollback() {
    this.#pending = freshPending()
    this.#pendingSkips = []
    this.#clock = this.#committedClock
    this.#units = this.#committedUnits
    this.#pipeline.discard()
    this.#reader?.seek(this.#position)
  }

  #hasPending() {
    const p = this.#pending
    return p.outputs.length > 0 || p.work > 0 || p.effects > 0 || (this.#pipeline.hasState && this.#pipeline.dirty) ||
      (Number.isInteger(p.next) && p.next !== this.#position)
  }

  /** After a page: commit when it is time, else move the broker's clock if a deadline is due. */
  async #settle(page) {
    const p = this.#pending
    const now = this.#now()
    // Outputs and done effects commit soon (at once when caught up); state
    // alone every commitEveryMillis; a position alone now and then.
    const outputs = p.outputs.length > 0 || p.effects > 0
    const moved = Number.isInteger(p.next) && p.next !== this.#position
    let due = false
    if (outputs) {
      due = page.caughtUp || p.outputs.length >= this.#opts.maxPendingOutputs || now - this.#lastCommitAt >= this.#opts.commitEveryMillis
    } else if (this.#pipeline.dirty || p.work > 0) {
      due = now - this.#lastCommitAt >= this.#opts.commitEveryMillis
    } else if (moved) {
      due = now - this.#lastPositionCommit >= this.#opts.positionCommitMillis
    }
    if (due) await this.#commit()
    if (!this.#pipeline.hasState || !this.#pipeline.needsFeedClock || this.#resolvedAt === 0) return
    const next = this.#pipeline.nextDue()
    if (next === null) return
    // A window or a deadline is pending. Right after a write of this
    // process, a short read shows the broker's time (the best sample).
    if (due) {
      this.#nextWait = TICK_READ_WAIT_MILLIS
      return
    }
    if (!page.caughtUp) return
    // Where the broker's clock is now, as far as this process can tell.
    const estimate = now + this.#offset
    if (estimate >= next) {
      if (now - this.#lastTickAt >= TICK_MIN_MILLIS) await this.#tick(now)
      this.#nextWait = TICK_READ_WAIT_MILLIS
    } else if (now - this.#resolvedAt >= CLOCK_SYNC_MILLIS && now - this.#lastTickAt >= CLOCK_SYNC_MILLIS) {
      // The last write anyone made is old: the estimate may be far behind.
      await this.#tick(now)
      this.#nextWait = TICK_READ_WAIT_MILLIS
    } else {
      this.#nextWait = Math.max(0, Math.min(this.#opts.waitMillis, Math.ceil(next - estimate)))
    }
  }

  /** Write the position: the write moves the broker's clock, and the next read shows it. */
  async #tick(now) {
    this.#lastTickAt = now
    this.#metrics.ticks++
    await this.#commit()
  }

  // ---- commit -----------------------------------------------------------------

  /**
   * The snapshot the commit at `next` names: the stored one moved here (the
   * state did not change since), the stored one as it is (not due yet), an
   * empty one (nothing is open), or a new layer on it, whose chunks are
   * written now: a delta of the keys changed since the last snapshot, or a
   * full layer when there is none yet, when the deltas weigh as much as the
   * full one, or when there are MAX_LAYERS. `replaced`: the layers the new
   * pointer no longer names (their chunks go once it commits).
   */
  async #snapshotFor(next) {
    const p = this.#pipeline
    const prev = this.#snap
    const metas = p.metas()
    if (prev && !p.changedSinceSnapshot) {
      return { pointer: { ...prev, position: next, clock: this.#recordClock(), metas, units: this.#units }, wrote: false, moved: true, replaced: [] }
    }
    const layers = layersOf(prev)
    if (p.empty) {
      return { pointer: pointerOf([], next, this.#recordClock(), metas, this.#units), wrote: true, replaced: layers }
    }
    // Due on the wall clock (a stream catching up on old messages would
    // otherwise snapshot at every commit), after so many messages (a restart
    // reads them again), at a stop, or with many skips. A delta costs what
    // changed, so the size of the state does not hold it back.
    const due = !prev || this.#stopping ||
      this.#now() - this.#snapWallAt >= this.#opts.snapshotEveryMillis ||
      this.#entriesSinceSnap >= this.#opts.snapshotMaxEntries ||
      this.#skips.length + this.#pendingSkips.length > MAX_SKIPS
    if (!due) return { pointer: prev, wrote: false, replaced: [] }
    const started = Date.now()
    const base = layers[0]
    let deltaBytes = 0
    for (const l of layers.slice(1)) deltaBytes += l.bytes
    const full = !base || base.v === 1 || layers.length >= MAX_LAYERS || deltaBytes >= base.bytes ||
      p.touchedSinceSnapshot >= p.stateKeys
    const sid = newSnapshotId()
    const w = await writeLayer({
      queen: this.#queen,
      ns: POSITION_NAMESPACE,
      id: this.#id,
      sid,
      lock: this.#lock,
      steps: p.stateful.map((st) => ({ kind: st.kind, entries: full ? st.state.entries() : st.state.touched() })),
      chunkChars: this.#opts.snapshotChunkChars,
      pieceBytes: this.#opts.snapshotPieceBytes
    })
    const layer = { sid, chunks: w.chunks, bytes: w.bytes }
    if (full) layer.full = true
    const named = full ? [layer] : w.chunks > 0 ? [...layers, layer] : layers
    return {
      pointer: pointerOf(named, next, this.#recordClock(), metas, this.#units),
      wrote: true,
      full,
      replaced: full ? layers : [],
      keys: layerKeys(this.#id, [layer]),
      millis: Date.now() - started
    }
  }

  /**
   * The clock a commit records. In the middle of a close it is the time the
   * close runs up to: the record's `units` says how far it got, and what is
   * still open by that time is closed by the next run or the next process.
   */
  #recordClock() {
    return this.#burstTarget === null ? this.#clock : Math.max(this.#clock, this.#burstTarget)
  }

  /** One transaction: the outputs, the position (and the snapshot it names). 'ok', 'conflict' or 'lost'. */
  async #commit() {
    const next = Number.isInteger(this.#pending.next) ? this.#pending.next : this.#position
    const stateful = this.#pipeline.hasState
    let snap = null
    try {
      snap = stateful ? await this.#snapshotFor(next) : null
    } catch (err) {
      // Chunks of this attempt may be in KV, named by nothing.
      this.#hadStray = true
      throw err
    }
    let done = false
    try {
      const r = await this.#commitWith(next, stateful, snap)
      done = r === 'ok'
      return r
    } finally {
      if (!done && snap && snap.wrote && snap.keys && snap.keys.length) this.#hadStray = true
    }
  }

  async #commitWith(next, stateful, snap) {
    const tx = this.#queen.transaction()
    if (this.#lock) tx.guard(this.#lock)
    const groups = new Map()
    for (const it of this.#pending.outputs) {
      const k = `${it.queue}\u0000${it.partition}`
      if (!groups.has(k)) groups.set(k, { queue: it.queue, partition: it.partition, items: [] })
      groups.get(k).items.push({ data: it.data, transactionId: it.transactionId })
    }
    for (const g of groups.values()) tx.queue(g.queue).partition(g.partition).push(g.items)
    const value = {
      position: next,
      queryId: this.#queryId,
      epoch: this.#epoch,
      updatedAt: new Date(this.#now()).toISOString()
    }
    if (this.#shard) value.shard = { ...this.#shard }
    if (this.#resetToken !== null) value.resetToken = this.#resetToken
    const recorded = this.#recordClock()
    if (stateful) Object.assign(value, { hash: this.#hash, clock: recorded, units: this.#units, snapshot: snap.pointer })
    // The skips since the snapshot this record names (none past a new one).
    const skips = stateful && !(snap.wrote || snap.moved) ? [...this.#skips, ...this.#pendingSkips] : []
    if (skips.length) value.skips = skips
    // For a reader of the record (the console): the feed time the position is
    // at (every message before it was stamped at or before it), whether the
    // read was caught up there, what each step holds, and why it stopped.
    const readTo = this.#pending.readTo ?? this.#committedReadTo
    const caughtUp = this.#pending.caughtUp ?? (next === this.#position && this.#committedCaughtUp)
    if (readTo > 0) value.readTo = readTo
    value.caughtUp = caughtUp === true
    if (stateful) {
      value.open = {}
      for (const s of this.#pipeline.stateful) {
        value.open[s.name] = s.engine ? s.engine.open : s.table.open
        if (s.clock && s.clock.kind === 'event' && s.clock.held > 0) (value.held ??= {})[s.name] = s.clock.held
      }
    }
    if (this.#stopMark) value.stopped = this.#stopMark
    // Every shard writes the same description, under the lock like the rest of the commit.
    const about = this.#aboutOwed ? this.#aboutValue() : null
    if (about) tx.kv.put(POSITION_NAMESPACE, aboutKey(this.#queryId), about, { forever: true })
    // A layout another deploy wrote since this process started stops it here.
    tx.kv.check(POSITION_NAMESPACE, `${this.#queryId}#layout`, { expect: this.#layoutVersion, required: true })
    if (this.#version > 0) {
      tx.kv.put(POSITION_NAMESPACE, this.#id, value, { expect: this.#version, required: true, forever: true })
    } else {
      tx.kv.putIfAbsent(POSITION_NAMESPACE, this.#id, value, { required: true, forever: true })
    }
    let res
    try {
      res = await tx.commit()
    } catch (err) {
      // A pushed output is already stored: its id is made from what it comes
      // from, so another process on this queryId committed these messages --
      // or this one did, and its answer was lost -- and that commit moved the
      // position in the same transaction. Carry on from the stored position.
      if (err && err.reason === 'duplicate') {
        await this.#reload('duplicate')
        return 'conflict'
      }
      throw err
    }
    if (res && res.success === false) {
      if (res.reason !== 'kv_precondition') {
        throw new Error(`FeedRunner: commit refused (${res.reason || 'unknown'}): ${res.error || ''}`)
      }
      // The guard lost (the lock is somebody else's now), or the position
      // moved (another process, or this commit's own earlier attempt whose
      // answer was lost). Nothing of this commit was written.
      if (this.#lock && !this.#lock.held) {
        this.#standDown('guard')
        return 'lost'
      }
      await this.#layoutMoved()
      await this.#reload('position')
      return 'conflict'
    }
    const version = versionOf(res, this.#id)
    const committedOutputs = this.#pending.outputs.length
    this.#position = next
    this.#committedClock = recorded
    this.#committedUnits = this.#units
    this.#committedReadTo = readTo
    this.#committedCaughtUp = value.caughtUp
    this.#aboutOwed = false
    if (stateful) {
      const prev = this.#snap
      this.#snap = snap.pointer
      if (snap.wrote || snap.moved) {
        this.#pipeline.snapshotTaken()
        this.#entriesSinceSnap = 0
        this.#snapWallAt = this.#now()
      }
      this.#skips = skips
      this.#skipSet = new Set(skips)
      this.#pendingSkips = []
      if (snap.wrote) {
        this.#metrics.snapshots++
        if (snap.full) this.#metrics.snapshotsFull++
        this.#metrics.snapshotBytes = snap.pointer.bytes
        this.#metrics.snapshotChunks = snap.pointer.chunks
        this.#metrics.snapshotLayers = layersOf(snap.pointer).length
        this.#metrics.snapshotMillis = snap.millis ?? 0
        // The layers the new pointer does not name: nobody reads them again.
        if (snap.replaced.length) this.#oldKeys.push(...layerKeys(this.#id, snap.replaced))
      }
      this.#pipeline.commitPoint()
    }
    this.#pending = freshPending()
    this.#lastCommitAt = this.#lastPositionCommit = this.#now()
    this.#metrics.commits++
    this.#metrics.outputs += committedOutputs
    this.#metrics.position = next
    if (version !== null) this.#version = version
    else await this.#refreshVersion()
    if (stateful && snap.wrote && this.#hadStray && this.#lock) {
      // Holding the lock, this process is the only writer: every chunk not of
      // the snapshot just named is left over.
      this.#hadStray = false
      try {
        const keep = new Set(layersOf(snap.pointer).map((l) => l.sid))
        this.#oldKeys.push(...await strayChunkKeys({ queen: this.#queen, ns: POSITION_NAMESPACE, id: this.#id, keep }))
      } catch (err) {
        logger.warn('FeedRunner.snapshotStray', { queryId: this.#id, error: err?.message })
      }
    }
    if (this.#oldKeys.length) await this.#dropOldKeys()
    return 'ok'
  }

  /** Delete what no record names any more: the chunks of replaced snapshots (best effort). */
  async #dropOldKeys() {
    const keys = this.#oldKeys
    this.#oldKeys = []
    try {
      await deleteKeys({ queen: this.#queen, ns: POSITION_NAMESPACE, keys, lock: this.#lock })
    } catch (err) {
      logger.warn('FeedRunner.snapshotCleanup', { queryId: this.#id, keys: keys.length, error: err?.message })
    }
  }

  async #refreshVersion() {
    const got = await this.#queen.kv.get(POSITION_NAMESPACE, this.#id)
    if (got && got.found) this.#version = got.version || this.#version
  }

  // ---- load -----------------------------------------------------------------

  /** Lost a commit to another one: take the stored position and state, read on from there. */
  async #reload(why) {
    this.#metrics.conflicts++
    this.#rollback()
    try {
      await this.#load(false)
    } catch (err) {
      // Half loaded is not loaded: the next cycle loads all of it again.
      this.#active = false
      throw err
    }
    logger.log('FeedRunner.conflict', { queryId: this.#id, why, position: this.#position, version: this.#version })
  }

  /**
   * The position, and the state as of it: the snapshot the record names,
   * then the feed read again from the snapshot's position to the committed
   * one, sending nothing.
   */
  async #load(first) {
    const qid = this.#id
    // Nothing of an earlier load or start carries over.
    this.#initialSnap = false
    this.#oldKeys = []
    const row = await this.#queen.kv.get(POSITION_NAMESPACE, qid)
    const meta = row && row.found && row.value && typeof row.value === 'object' ? row.value : null
    const stored = meta && Number.isInteger(meta.position) ? meta : null
    this.#version = row && row.found ? row.version || 0 : 0
    const reset = this.#opts.reset
    // `true` starts over when this process starts; a string, once, at whichever
    // load first finds the stored record without it (a takeover too).
    const startOver = (first && reset === true) || (typeof reset === 'string' && stored !== null && stored.resetToken !== reset)
    this.#skips = !startOver && stored && Array.isArray(stored.skips) ? stored.skips : []
    this.#skipSet = new Set(this.#skips)
    this.#pendingSkips = []
    this.#resetToken = typeof reset === 'string' ? reset : (stored && typeof stored.resetToken === 'string' ? stored.resetToken : null)
    const source = this.#stream.source.options || {}
    const p = this.#pipeline
    let isNew = false
    if (!stored || startOver) {
      isNew = true
      this.#epoch = stored ? (Number.isInteger(stored.epoch) ? stored.epoch : 0) + 1 : (Number.isInteger(meta?.epoch) ? meta.epoch + 1 : 0)
      this.#position = this.#opts.from ?? source.from ?? (source.since !== undefined ? 'earliest' : 'now')
      this.#clock = 0
      if (p.hasState) p.wipe()
      // What the stored record kept is nobody's now.
      if (stored && stored.snapshot) this.#oldKeys.push(...layerKeys(qid, layersOf(stored.snapshot)))
      if (stored && Number.isInteger(stored.buckets)) for (let b = 0; b < stored.buckets; b++) this.#oldKeys.push(`${qid}#${b}`)
      this.#snap = null
      if (startOver) logger.log('FeedRunner.reset', { queryId: qid, epoch: this.#epoch, from: this.#position })
    } else {
      this.#epoch = Number.isInteger(stored.epoch) ? stored.epoch : 0
      this.#position = stored.position
      this.#clock = Number.isFinite(stored.clock) ? stored.clock : 0
      if (p.hasState) await this.#loadState(stored)
    }
    this.#committedClock = this.#clock
    this.#committedReadTo = !isNew && Number.isFinite(stored.readTo) ? stored.readTo : this.#clock
    this.#committedCaughtUp = false
    if (isNew) this.#units = 0
    this.#committedUnits = this.#units
    if (isNew) this.#entriesSinceSnap = 0
    this.#snapWallAt = this.#now()
    this.#pending = freshPending()
    // Snapshot attempts that died between their chunks and their commit (this
    // process's, or the one it takes over from) leave chunks nobody names:
    // looked for at the next snapshot.
    if (p.hasState) this.#hadStray = true
    if (isNew) {
      // 'now' is the position when the stream starts, not when its first read
      // comes: a message pushed once run() resolved is the stream's.
      if (this.#position === 'now' && source.since === undefined) this.#position = (await this.#reader.probe()).next
      if (p.tables.some((t) => t.initial === 'all') && this.#position !== 'earliest') await this.#prelude(source)
      // Empty where it starts (a table's first rows aside, which need a real snapshot).
      this.#initialSnap = p.hasState && p.empty
      if (Number.isInteger(this.#position)) {
        this.#reader.seek(this.#position)
      } else if (source.since !== undefined) {
        await this.#reader.close()
        this.#reader = this.#makeReader({ from: this.#position, since: source.since })
      } else {
        this.#reader.seek(this.#position)
      }
    } else {
      this.#reader.seek(this.#position)
    }
  }

  async #loadState(stored) {
    const p = this.#pipeline
    const qid = this.#id
    const snap = stored.snapshot && typeof stored.snapshot === 'object' ? stored.snapshot : null
    // State the old chain kept: chunks, or messages to read again since the snapshot.
    const hasState = (snap && (layersOf(snap).length > 0 || (Number.isInteger(snap.position) && snap.position < stored.position))) ||
      Number.isInteger(stored.buckets)
    if (hasState && typeof stored.hash === 'string' && stored.hash !== this.#hash) {
      throw Object.assign(new Error(
        `the steps of ${this.#queryId} changed since it last ran, and its state was kept by the old chain: ` +
        'run it with { reset: true } (or a reset string) to start it over, or give it a new queryId'
      ), { fatal: true, code: 'feed_chain_changed' })
    }
    if (snap) {
      const layers = layersOf(snap)
      try {
        if (layers.length && layers[0].v === 1) {
          // Format 1: one JSON of the whole state across the chunks of one id.
          p.load(decodeSnapshot(await readChunks({ queen: this.#queen, ns: POSITION_NAMESPACE, id: qid, sid: layers[0].sid, count: layers[0].chunks })), snap.metas)
        } else {
          // Format 2: every layer in order, piece by piece; nothing ever holds the whole state at once.
          p.loadBegin()
          for (const l of layers) {
            await readLayer({
              queen: this.#queen,
              ns: POSITION_NAMESPACE,
              id: qid,
              sid: l.sid,
              count: l.chunks,
              onPiece: (pc) => p.loadPiece(pc.s, pc.k, pc.set, pc.del)
            })
          }
          await p.loadEnd(snap.metas)
        }
      } catch (err) {
        // Another process replaced the snapshot while this one read it
        // (exclusive: false): load again, from the record as it is now.
        const now = await this.#queen.kv.get(POSITION_NAMESPACE, qid)
        const sids = (x) => layersOf(x).map((l) => l.sid).join(',')
        if (err.code === 'feed_snapshot_missing' && now && now.found && sids(now.value?.snapshot) !== sids(snap)) {
          throw Object.assign(new Error(`the snapshot of ${qid} was replaced while it loaded; loading again`), { code: 'feed_snapshot_moved' })
        }
        throw err
      }
      // Loading leaves the state "unchanged since the snapshot"; what the
      // replay changes marks it changed, so the next commit cannot move the
      // snapshot's position over a state the chunks do not hold.
      // The closes the state holds (the snapshot's count), and how many the
      // stored record says came by its position: the replay stops there, also
      // in the middle of a close that a commit split.
      this.#units = Number.isInteger(snap.units) ? snap.units : 0
      const cap = Number.isInteger(snap.units) && Number.isInteger(stored.units) ? stored.units : Infinity
      const snapClock = Number.isFinite(snap.clock) ? snap.clock : 0
      this.#snap = snap
      if ((Number.isInteger(snap.position) && snap.position < stored.position) ||
        (Number.isFinite(cap) && cap > this.#units) || snapClock < this.#clock) {
        this.#entriesSinceSnap = await this.#replay(Number.isInteger(snap.position) ? snap.position : stored.position, stored.position, snapClock, this.#clock, cap)
      }
    } else if (Number.isInteger(stored.buckets)) {
      // A record of the bucket format (before snapshots): the buckets are the
      // state at the position. Loaded once; the next commit writes a snapshot.
      const keys = Array.from({ length: stored.buckets }, (_, b) => `${qid}#${b}`)
      const r = keys.length ? await this.#queen.kv.getMany(POSITION_NAMESPACE, keys) : { rows: [] }
      const entries = []
      for (const row of r.rows || []) {
        const e = row.value && typeof row.value === 'object' && row.value.e && typeof row.value.e === 'object' ? row.value.e : {}
        for (const kv of Object.entries(e)) entries.push(kv)
      }
      const metas = [{ watermark: stored.watermark }]
      p.load({ steps: [{ name: p.stateful[0]?.name, kind: p.stateful[0]?.kind, entries }] }, metas)
      this.#snap = null
      this.#oldKeys.push(...keys)
    } else {
      p.load(null, null)
      this.#snap = null
    }
  }

  /**
   * The feed from `from` (a snapshot's position) to `to` (the committed one)
   * once more, as it ran: same filters, same clock (each message's time, then
   * the committed clock), every result dropped.
   */
  async #replay(from, to, snapClock, committedClock, cap = Infinity) {
    const started = Date.now()
    const reader = this.#makeReader({ from })
    this.#replaying = true
    this.#replayCap = cap
    this.#clock = snapClock
    let n = 0
    try {
      const deadline = Date.now() + REPLAY_CATCH_UP_MILLIS
      while (reader.position < to) {
        const page = await reader.read({ waitMillis: 100 })
        for (const g of this.#groups(page.entries, to)) {
          await this.#processGroup(g)
          n += g.entries.length
        }
        if (page.caughtUp && reader.position < to && Date.now() > deadline) {
          throw new Error(`FeedRunner: the follower read has not reached the committed position ${to} (at ${reader.position}) after ${REPLAY_CATCH_UP_MILLIS} ms`)
        }
      }
      this.#clock = Math.max(this.#clock, committedClock)
      await this.#advanceTo(this.#clock)
    } finally {
      this.#replaying = false
      this.#replayCap = Infinity
      await reader.close()
    }
    if (Number.isFinite(cap) && this.#units !== cap) {
      logger.warn('FeedRunner.replayUnits', { queryId: this.#id, replayed: this.#units, stored: cap })
    }
    this.#pipeline.commitPoint()
    this.#metrics.replays++
    this.#metrics.replayEntries += n
    this.#metrics.replayMillis += Date.now() - started
    logger.log('FeedRunner.replay', { queryId: this.#id, from, to, entries: n, millis: Date.now() - started })
    return n
  }

  /**
   * A new stream with a table: the table queue from its beginning up to where
   * the stream starts, rows only, so the first message already sees every row.
   */
  async #prelude(source) {
    let start = this.#position
    if (!Number.isInteger(start)) {
      if (source.since !== undefined) {
        const probe = this.#makeReader({ from: 'earliest', since: source.since })
        try {
          const page = await probe.read({ waitMillis: 0 })
          start = page.entries.length ? page.entries[0].position : page.next
        } finally {
          await probe.close()
        }
      } else {
        const answer = await this.#reader.probe()
        start = answer.next
      }
    }
    this.#position = start
    const reader = this.#makeReader({ from: 'earliest', tablesOnly: true })
    let rows = 0
    try {
      while (reader.position === 'earliest' || reader.position < start) {
        const page = await reader.read({ waitMillis: 0 })
        for (const e of page.entries) {
          if (e.position >= start) break
          await this.#pipeline.applyRow(e)
          rows++
        }
        if (page.caughtUp) break
      }
    } finally {
      await reader.close()
    }
    this.#metrics.tableRows += rows
    // The rows are the starting point: a rollback keeps them (the position
    // stays where the stream starts), and the first commit snapshots them.
    this.#pipeline.commitPoint()
    if (rows) this.#pending.work++
    logger.log('FeedRunner.tablePrelude', { queryId: this.#id, rows, start })
  }
}
