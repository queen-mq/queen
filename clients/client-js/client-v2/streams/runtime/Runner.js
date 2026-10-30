/**
 * Runner — the streaming engine's pop/process/cycle loop, plus the
 * per-window idle-flush timer.
 *
 * Lifecycle
 * ---------
 *   start():
 *     - register the query (idempotent, returns query_id)
 *     - warn if .keyBy() consistently differs from partition key
 *     - kick off the pop loop in the background
 *     - if a window operator has idleFlushMs > 0, also start a flush timer
 *
 *   loop:
 *     pop -> group by partition -> for each partition group:
 *       - apply pre-reducer ops -> envelopes
 *       - if window+reduce: read state (incl. watermark), reduce, build
 *         state ops + emits
 *       - apply post-reducer ops to each emit
 *       - build push_items / run foreach effects
 *       - commit cycle (state ops + push items + ack) atomically
 *
 *   flush timer, processing-time windows with a reducer (broker 2.7):
 *     each cycle notes when the partition's earliest open window closes ->
 *     when that time comes, claim the partition (POST /streams/v1/claim: a
 *     lease with an empty batch) -> close what is ripe on the broker's
 *     clock -> commit a cycle that acks the empty batch (count 0) under
 *     the claim's lease. A sweep (POST /streams/v1/state/partitions) finds
 *     the windows nobody tracks: after a restart, or left by another runner.
 *
 *   flush timer, event-time windows or a broker without the claim route:
 *     periodically wake -> for partitions touched recently, query state
 *     for ripe windows -> apply post-reducer ops + foreach + sink push,
 *     commit no-ack cycle (state deletes + sink push, no source ack).
 *
 *   stop():
 *     - signal abort, wait for in-flight cycle and flush (a flush at most
 *       10 s), return.
 *
 * Retry / fault model unchanged from v0.1: cycle commit failures roll
 * back; Queen redelivers via lease/retry. Stateless / reducer / foreach
 * exceptions abort their cycle.
 */

import { StreamsHttpClient } from './StreamsHttpClient.js'
import { registerQuery } from './register.js'
import { getState } from './state.js'
import { commitCycle } from './cycle.js'
import { stateKeyFor, parseStateKey } from '../operators/ReduceOperator.js'
import { makeLogger } from '../util/logger.js'
import { sleep } from '../util/backoff.js'
import { CONFLATION_UNSUPPORTED } from '../../utils/conflation.js'

const DEFAULT_BATCH_SIZE = 200
const DEFAULT_MAX_PARTITIONS = 4
const DEFAULT_IDLE_SLEEP_MS = 100
// Track partitions we've touched within this rolling window. Drives the
// idle-flush sweep — anything older than this is assumed to be owned by a
// different worker and we don't speculatively flush it. Only for a broker
// without POST /streams/v1/claim (before 2.7), and for event-time windows.
const PARTITION_RECENCY_MS = 5 * 60 * 1000  // 5 minutes

// Closes under a claim (broker 2.7, issue #153).
//
// How often a runner asks the broker for the partitions holding the query's
// state: after a restart, a scale-down, or for partitions another runner
// left, these are the windows nobody tracks.
const SWEEP_EVERY_MS = 30_000
// How old a partition's newest state row must be before a later sweep takes
// it on: a younger one is some runner's live work, which tracks it already.
// The first sweep after a start takes every partition: this runner tracks
// nothing yet.
const SWEEP_UNTOUCHED_MS = 30_000
// Partitions per page of the sweep.
const SWEEP_PAGE = 1000
// Partitions per claim, which is also the most one flush tick closes: a
// sweep that finds thousands of quiet partitions (after an outage) works
// through them a batch per tick instead of all at once.
const CLAIM_BATCH = 100
// The lease a claim asks for. A close is one state read and one commit.
const CLAIM_LEASE_S = 30
// How soon to try again a partition that someone else holds, or whose claim
// or close failed.
const CLAIM_RETRY_MS = 1000
// How long stop() waits for a flush in flight.
const STOP_FLUSH_WAIT_MS = 10_000

// Whether the broker serves POST /streams/v1/claim.
const CLAIMS_UNKNOWN = 'unknown'
const CLAIMS_YES     = 'yes'
const CLAIMS_NO      = 'no'   // an older broker: the unleased flush of recent partitions

const WATERMARK_STATE_KEY = '__wm__'  // reserved internal key for per-partition watermark

export class Runner {
  constructor(opts) {
    this.options          = opts
    this.stream           = opts.stream
    this.queryId          = opts.queryId
    this.batchSize        = opts.batchSize || DEFAULT_BATCH_SIZE
    this.maxPartitions    = opts.maxPartitions || DEFAULT_MAX_PARTITIONS
    this.maxWaitMillis    = opts.maxWaitMillis != null ? opts.maxWaitMillis : 1000
    this.subscriptionMode = opts.subscriptionMode || null
    this.subscriptionFrom = opts.subscriptionFrom || null
    // Last-value delivery on the SOURCE pop (PLAN_CONFLATION §1.1): each cycle
    // sees only the newest visible message per partition. Off by default. The
    // query's own cursor/state path is untouched — this is the ordinary pop
    // parameter, declared on the runner's consumer group like any other
    // consumer's. Requires broker >= 1.1.0; an older one is detected from the
    // missing response echo and stops the loop rather than quietly replaying
    // every stale intermediate through the operators (§4).
    this.conflation       = !!opts.conflation
    this.reset            = !!opts.reset
    this.onError          = opts.onError || null
    this.abortSignal      = opts.abortSignal || null
    this.logger           = makeLogger(opts.logger)
    this.consumerGroup    = opts.consumerGroup || `streams.${this.queryId}`

    if (!opts.url) {
      throw new Error('Stream.run requires { url: "<queen-url>" } so the SDK can call /streams/v1/* endpoints')
    }
    this.http = new StreamsHttpClient({
      url: opts.url,
      bearerToken: opts.bearerToken
    })

    this._stopped              = false
    this._loopPromise          = null
    this._flushTimers          = []          // setInterval handles
    this._flushInFlight        = false
    // Per-partition mutex between the pop/cycle loop and the idle-flush
    // timer. Both paths are read(state) -> compute -> commit against the
    // same (query_id, partition_id) state rows; the server's advisory
    // lock only serialises the COMMITS, so an unsynchronised flush can
    // read a window's acc, have the cycle emit+delete (or re-upsert) it,
    // and then emit the same acc again — a duplicate emit (or, with the
    // opposite interleave, drop a freshly-reduced value). Node is
    // single-threaded but both paths interleave at await points, which is
    // exactly where the read->compute->commit race bites. Serialising the
    // two in-process paths removes the race at its source.
    this._partitionMutexes     = new Map()   // partitionId -> tail promise
    this._recentPartitions     = new Map()   // partitionId -> { partitionName, touchedAt }
    this._partitionWatermarks  = new Map()   // partitionId -> wmMs (cache; the broker is source of truth)
    // Processing-time windows still open, by partition: what the idle flush
    // claims and closes once their time has come. atMs is on the broker's
    // clock (processing time is the messages' createdAt).
    this._due                  = new Map()   // partitionId -> { partitionName, atMs }
    this._claims               = CLAIMS_UNKNOWN
    this._nextSweepAt          = 0           // the first flush tick sweeps
    this._sweepUntouchedMs     = 0           // the first sweep takes every partition
    this._flushPromise         = null        // the flush tick in flight, for stop()
    this._stats = {
      cyclesTotal:      0,
      flushCyclesTotal: 0,
      messagesTotal:    0,
      pushItemsTotal:   0,
      stateOpsTotal:    0,
      lateEventsTotal:  0,
      errorsTotal:      0,
      lastCycleAt:      null,
      lastFlushAt:      null,
      lastError:        null
    }
    this._serverQueryId = null
  }

  // ---------------------------------------------------------------- public API

  async start() {
    const stages = this.stream.stages
    const sourceQueueName = this._extractQueueName(this.stream.source)
    const sinkQueueName = stages.sink && stages.sink.kind === 'sink'
      ? stages.sink.queueName
      : null

    const reg = await registerQuery({
      http: this.http,
      name: this.queryId,
      sourceQueue: sourceQueueName,
      sinkQueue: sinkQueueName,
      configHash: this.stream.config_hash,
      reset: this.reset
    })
    this._serverQueryId = reg.queryId
    this.logger.info('query registered', {
      queryId: this.queryId, server_id: reg.queryId, fresh: reg.fresh, didReset: reg.didReset
    })

    if (stages.keyBy) {
      this.logger.warn(
        '.keyBy() is set — if the derived key consistently differs from ' +
        'the source partition_id, multiple workers may contend on the ' +
        'same (query_id, partition_id, key) state rows. Consider the ' +
        'repartition pattern (push to a co-keyed intermediate queue).'
      )
    }

    // Spawn idle-flush timer(s) for window operators that have one.
    if (stages.window && typeof stages.window.idleFlushMs === 'number' && stages.window.idleFlushMs > 0) {
      const ms = stages.window.idleFlushMs
      const handle = setInterval(() => { this._flushTick().catch(() => {}) }, ms)
      // Don't keep the event loop alive just for the flush timer.
      if (typeof handle.unref === 'function') handle.unref()
      this._flushTimers.push(handle)
      this.logger.info('idle-flush timer armed', { intervalMs: ms, operator: stages.window.operatorTag })
    }

    this._loopPromise = this._loop()
    // The first flush tick runs now rather than one interval later: its sweep
    // finds the windows a previous runner left open.
    if (this._flushTimers.length > 0) this._flushTick().catch(() => {})
    return this
  }

  async stop() {
    this._stopped = true
    for (const t of this._flushTimers) clearInterval(t)
    this._flushTimers = []
    if (this._loopPromise) await this._loopPromise
    // Allow any in-flight flush to drain. A close cut short would keep its
    // claim on the partition until the lease ran out, so it gets up to 10 s;
    // past that stop() returns and the flush ends on its own.
    if (this._flushPromise) {
      let timer
      await Promise.race([
        this._flushPromise.catch(() => {}),
        new Promise(resolve => { timer = setTimeout(resolve, STOP_FLUSH_WAIT_MS) })
      ])
      clearTimeout(timer)
    }
  }

  metrics() {
    return { ...this._stats, queryId: this.queryId, serverQueryId: this._serverQueryId }
  }

  // ----------------------------------------------------------------- main loop

  async _loop() {
    while (!this._isStopped()) {
      try {
        const messages = await this._popMessages()
        if (this._isStopped()) break
        if (!messages || messages.length === 0) {
          await sleep(DEFAULT_IDLE_SLEEP_MS)
          continue
        }
        const groups = this._groupByPartition(messages)
        for (const group of groups) {
          if (this._isStopped()) break
          // The recent map feeds only the unleased flush.
          if (!this._leasedCloses() || this._claims !== CLAIMS_YES) {
            this._touchPartition(group.partitionId, group.partitionName)
          }
          try {
            await this._processPartitionCycle(group)
            this._stats.cyclesTotal++
            this._stats.messagesTotal += group.messages.length
            this._stats.lastCycleAt = new Date().toISOString()
          } catch (err) {
            this._reportError(err, { phase: 'cycle', partition: group.partitionId, messages: group.messages })
          }
        }
      } catch (err) {
        this._reportError(err, { phase: 'pop' })
        // A broker that cannot apply the declared conflation policy is a
        // permanent fault, not a transient one (PLAN_CONFLATION §4): retrying
        // every 500ms would turn "your last-value policy is not in force" into
        // a log line nobody reads while the operators chew the whole backlog.
        if (err && err.code === CONFLATION_UNSUPPORTED) {
          this._stopped = true
          break
        }
        await sleep(500)
      }
    }
    this.logger.info('loop stopped', { queryId: this.queryId, stats: this._stats })
  }

  _isStopped() {
    if (this._stopped) return true
    if (this.abortSignal && this.abortSignal.aborted) {
      this._stopped = true
      return true
    }
    return false
  }

  _reportError(err, ctx) {
    this._stats.errorsTotal++
    this._stats.lastError = err && err.message ? err.message : String(err)
    this.logger.error(ctx.phase + ' failed', { queryId: this.queryId, ...ctx, error: this._stats.lastError })
    if (this.onError) {
      try { this.onError(err, ctx) } catch { /* ignore */ }
    }
  }

  // ----------------------------------------------------------------- pop

  async _popMessages() {
    let qb = this.stream.source
      .batch(this.batchSize)
      .wait(true)
      .timeoutMillis(this.maxWaitMillis)
      .group(this.consumerGroup)

    // Unconditional, and it has to be: the runtime has already defaulted
    // maxPartitions (to 4), so this is always a decision the streams layer
    // made. Skipping the call for maxPartitions === 1 used to be a harmless
    // optimisation -- 1 was what an omitted `partitions` meant on the wire --
    // but with pop autopilot an omitted `partitions` means "broker, you
    // choose", which would widen a query that explicitly asked for one
    // partition per pop.
    qb = qb.partitions(this.maxPartitions)
    if (this.subscriptionMode) qb = qb.subscriptionMode(this.subscriptionMode)
    if (this.subscriptionFrom) qb = qb.subscriptionFrom(this.subscriptionFrom)
    if (this.conflation) qb = qb.conflation(true)
    return qb.pop()
  }

  _groupByPartition(messages) {
    const map = new Map()
    for (const m of messages) {
      const key = m.partitionId || 'unknown'
      let g = map.get(key)
      if (!g) {
        g = {
          partitionId: m.partitionId,
          partitionName: m.partition,
          leaseId: m.leaseId,
          consumerGroup: m.consumerGroup || this.consumerGroup,
          messages: []
        }
        map.set(key, g)
      }
      g.messages.push(m)
    }
    return Array.from(map.values())
  }

  _touchPartition(partitionId, partitionName) {
    if (!partitionId) return
    this._recentPartitions.set(partitionId, {
      partitionName,
      touchedAt: Date.now()
    })
    // Trim entries older than the recency window.
    const cutoff = Date.now() - PARTITION_RECENCY_MS
    for (const [pid, e] of this._recentPartitions.entries()) {
      if (e.touchedAt < cutoff) this._recentPartitions.delete(pid)
    }
  }

  // ------------------------------------------------- per-partition mutex

  /**
   * Run `fn` with the per-partition mutex held (see _partitionMutexes in
   * the constructor). Implemented as a promise chain per partitionId: each
   * entrant waits on the previous tail, so the cycle path and the
   * idle-flush path can never interleave their read->compute->commit
   * sections for the same partition. The returned promise settles like
   * `fn()` (errors propagate to the caller); the stored tail never
   * rejects, so a failed cycle doesn't poison the chain.
   */
  _withPartitionLock(partitionId, fn) {
    const key = partitionId || 'unknown'
    const tail = this._partitionMutexes.get(key) || Promise.resolve()
    const run = tail.then(fn)
    const next = run.then(() => {}, () => {})
    this._partitionMutexes.set(key, next)
    next.then(() => {
      // GC: drop the entry once the chain drains.
      if (this._partitionMutexes.get(key) === next) this._partitionMutexes.delete(key)
    })
    return run
  }

  // ----------------------------------------------------------- cycle

  async _processPartitionCycle(group) {
    // Mutual exclusion with the idle-flush timer (see _partitionMutexes).
    return this._withPartitionLock(group.partitionId, () => this._processPartitionCycleInner(group))
  }

  async _processPartitionCycleInner(group) {
    const stages = this.stream.stages
    const partitionId = group.partitionId
    const partitionName = group.partitionName

    // Partition FIFO order. A 2.x broker answers each message with its offset,
    // which IS that order, and the one the gate's partial ack counts in.
    // (createdAt, id) is only the fallback for brokers that send no offset:
    // createdAt is rendered to the millisecond, and pushes from concurrent
    // producers share one with their ids out of offset order (measured
    // 2026-10-02 on 2.0.0-beta.6: 69 of 400 positions disagreed).
    const orderedMessages = [...group.messages].sort((a, b) => {
      if (Number.isFinite(a.offset) && Number.isFinite(b.offset)) return a.offset - b.offset
      const ta = Date.parse(a.createdAt || '') || 0
      const tb = Date.parse(b.createdAt || '') || 0
      if (ta !== tb) return ta - tb
      return (a.id || '').localeCompare(b.id || '')
    })

    // 1. Run pre-reducer stateless ops on each message.
    let envelopes = orderedMessages.map(msg => ({
      msg,
      key:   msg.partitionId || partitionId,
      value: msg.data
    }))
    for (const op of stages.pre) {
      const next = []
      for (const env of envelopes) {
        const out = await op.apply(env)
        for (const e of out) next.push(e)
      }
      envelopes = next
    }

    // 2. KeyBy override.
    if (stages.keyBy) {
      const next = []
      for (const env of envelopes) {
        const out = await stages.keyBy.apply(env)
        for (const e of out) next.push(e)
      }
      envelopes = next
    }

    // 3. GATE stage takes a different path: serial evaluation, possibly
    //    partial ack, no lease release on partial.
    if (stages.gate) {
      return await this._processGateCycle({
        stages, envelopes, orderedMessages, group, partitionId, partitionName
      })
    }

    // 3. Window operator + reducer (if present).
    let stateOps = []
    let emits = []
    let nextCloseMs = null
    if (stages.window) {
      const result = stages.window.windowKind === 'session'
        ? await this._processSessionWindow(stages, envelopes, partitionId, orderedMessages)
        : await this._processStatelessWindow(stages, envelopes, partitionId, orderedMessages)
      stateOps = result.stateOps
      emits = result.emits
      nextCloseMs = result.nextCloseMs ?? null
    } else {
      // No window: each remaining envelope becomes an emit.
      emits = envelopes.map(env => ({ key: env.key, value: env.value, sourceMsg: env.msg }))
    }

    // 4. Post-reducer stateless ops.
    emits = await this._applyPostStages(stages.post, emits, partitionId, partitionName)

    // 5. Build sink push items / run foreach effects.
    let pushItems = []
    if (stages.sink && stages.sink.kind === 'sink') {
      pushItems = stages.sink.buildPushItems(
        emits.map(e => ({ key: e.key, value: e.value, sourceMsg: e.sourceMsg })),
        partitionName
      )
    } else if (stages.sink && stages.sink.kind === 'foreach') {
      await stages.sink.runEffects(emits.map(e => ({
        value: e.value,
        ctx:   this._buildEmitCtx(e, partitionName, partitionId)
      })))
    }

    // 6. Build the source ack — advance the cursor to the LAST message
    //    and tell the broker how many messages were in this cycle's batch
    //    so its acked count / lease release logic uses the correct count
    //    (the cycle is atomic across the full batch).
    const lastMsg = orderedMessages[orderedMessages.length - 1]
    const ack = lastMsg
      ? {
          transactionId: lastMsg.transactionId,
          leaseId:       lastMsg.leaseId || group.leaseId,
          status:        'completed',
          count:         orderedMessages.length
        }
      : null

    // 7. Commit.
    this._stats.stateOpsTotal += stateOps.length
    this._stats.pushItemsTotal += pushItems.length
    await commitCycle({
      http: this.http,
      queryId: this._serverQueryId,
      partitionId,
      consumerGroup: group.consumerGroup,
      stateOps,
      pushItems,
      ack
    })
    // What stays open closes when its time comes, under a claim.
    // A broker without claims never closes these, so they are not kept.
    if (this._leasedCloses() && this._claims !== CLAIMS_NO) this._setDue(partitionId, partitionName, nextCloseMs)
  }

  // --------------------------------------------------------------- gate path

  /**
   * Gate-cycle path: serial per-message ALLOW/DENY decision with persistent
   * per-key state. On the first DENY, halt and commit a partial ack with
   * release_lease=false so the un-acked tail returns when the lease expires
   * (preserving FIFO per partition without a deferred queue).
   *
   * The gate settles SOURCE MESSAGES, not envelopes. A pre-stage runs before
   * it and breaks the one-to-one correspondence: `.filter()` leaves a message
   * with no envelope, `.flatMap()` leaves it with several. The partial ack is
   * an offset commit -- the broker advances `count` messages from the head of
   * the leased batch -- so counting envelopes acks the wrong prefix: past a
   * denied message (which is then never redelivered) after a flatMap, short of
   * the allowed ones (which are then gated and pushed again) after a filter. A
   * message is settled only when every envelope it produced was allowed, so a
   * redelivery replays the whole message and never half of it.
   *
   * State model: load ALL state rows for the partition once, evaluate the
   * gate in order, mutate in-memory state, then upsert only the keys whose
   * ALLOWED messages mutated them. What a denied message did to the state on
   * its way to being denied is rolled back: a denied message did not happen.
   */
  async _processGateCycle({ stages, envelopes, orderedMessages, group, partitionId, partitionName }) {
    const gate = stages.gate

    // One bucket of envelopes per claimed message, in partition order (the
    // sort in _processPartitionCycleInner). Every pre-stage operator carries
    // the source message through as `env.msg`.
    const indexOf = new Map(orderedMessages.map((m, i) => [m, i]))
    const buckets = orderedMessages.map(() => [])
    for (const env of envelopes) {
      const i = indexOf.get(env.msg)
      if (i === undefined) {
        // Not traceable to a claimed message, so not settleable by an offset
        // commit. Hold the batch rather than ack past it: the lease lapses and
        // the whole batch comes back.
        this._reportError(new Error('.gate(): an envelope does not come from a message of this batch; holding the batch'), {
          phase: 'gate-eval', partition: partitionId
        })
        return
      }
      buckets[i].push(env)
    }

    // Load all state rows for this partition. Cheap: usually one row per
    // (rate-limit key) — for the canonical rate-limiter pattern that's a
    // single row, since partition == limit key.
    const loadedAll = await getState({
      http: this.http,
      queryId: this._serverQueryId,
      partitionId,
      keys: []
    })

    // Working in-memory state, keyed by envelope.key. We only persist
    // entries that actually changed via an ALLOW.
    //   liveState[key]: the current mutable object the user fn sees
    //   touchedKeys: keys whose state was mutated by an allowed eval
    const liveState = new Map()
    const touchedKeys = new Set()

    const ensureState = (key) => {
      if (liveState.has(key)) return liveState.get(key)
      const initial = loadedAll.has(key)
        ? this._cloneState(loadedAll.get(key))
        : {}
      liveState.set(key, initial)
      return initial
    }

    const streamTimeMs = Date.now()
    let settled = 0          // messages whose every envelope was allowed
    let denied = false
    const allowedEnvelopes = []

    messages: for (const bucket of buckets) {
      // What this message does to the state, to undo it if one of its
      // envelopes is denied: key -> its value before this message
      // (undefined: no entry yet), and the keys it touched first.
      const before = new Map()
      const newlyTouched = []
      for (const env of bucket) {
        const key = env.key
        if (!before.has(key)) {
          before.set(key, liveState.has(key) ? this._cloneState(liveState.get(key)) : undefined)
        }
        const stateForKey = ensureState(key)
        const ctx = {
          state:        stateForKey,
          streamTimeMs,
          partitionId,
          partition:    partitionName,
          key
        }
        let decision
        try {
          decision = await gate.evaluate(env, ctx)
        } catch (err) {
          // Gate threw: treat as a transient cycle error. Don't commit
          // anything. The lease will time out and the broker redelivers.
          this._reportError(err, { phase: 'gate-eval', partition: partitionId, message: env.msg })
          return
        }
        if (!decision.allow) {
          for (const [k, value] of before) {
            if (value === undefined) liveState.delete(k)
            else liveState.set(k, value)
          }
          for (const k of newlyTouched) touchedKeys.delete(k)
          denied = true
          break messages
        }
        // Mutated state on this key is committable.
        if (!touchedKeys.has(key)) {
          touchedKeys.add(key)
          newlyTouched.push(key)
        }
      }
      // Settled, including a message the pre-stages dropped entirely: it
      // has nothing to gate, and holding it would hold the whole batch.
      allowedEnvelopes.push(...bucket)
      settled++
    }

    if (settled === 0) {
      // The first message was denied. Nothing to commit: skip the cycle
      // entirely. The lease stays held by us until it naturally expires;
      // the messages are then redelivered (to us or another worker) in
      // their original order.
      this._stats.gateDenialsTotal = (this._stats.gateDenialsTotal || 0) + orderedMessages.length
      return
    }

    // Build state ops for keys that were touched (only by allowed messages).
    const stateOps = []
    for (const key of touchedKeys) {
      stateOps.push({
        type:  'upsert',
        key,
        value: liveState.get(key)
      })
    }

    // Apply post-stages to allowed emits (post-stages here see the original
    // message value, since gate doesn't transform it — same as a no-window
    // pass-through).
    let emits = allowedEnvelopes.map(env => ({
      key: env.key, value: env.value, sourceMsg: env.msg
    }))
    emits = await this._applyPostStages(stages.post, emits, partitionId, partitionName)

    // Build sink push items / run foreach for allowed emits.
    let pushItems = []
    if (stages.sink && stages.sink.kind === 'sink') {
      pushItems = stages.sink.buildPushItems(
        emits.map(e => ({ key: e.key, value: e.value, sourceMsg: e.sourceMsg })),
        partitionName
      )
    } else if (stages.sink && stages.sink.kind === 'foreach') {
      await stages.sink.runEffects(emits.map(e => ({
        value: e.value,
        ctx:   this._buildEmitCtx(e, partitionName, partitionId)
      })))
    }

    // Ack the LAST settled message (cursor advances to it). The count tells
    // the broker how many messages this commit covers, counted from the head
    // of the leased batch: on a partial ack it is the offset the cursor
    // moves to, so it has to be the number of settled MESSAGES.
    const lastSettled = orderedMessages[settled - 1]
    const ack = {
      transactionId: lastSettled.transactionId,
      leaseId:       lastSettled.leaseId || group.leaseId,
      status:        'completed',
      count:         settled
    }

    // Holding the lease on a partial ack is what preserves FIFO: the denied
    // tail is not claimable by another worker, so it cannot be overtaken.
    const releaseLease = !denied

    if (denied) {
      this._stats.gateDenialsTotal = (this._stats.gateDenialsTotal || 0) + (orderedMessages.length - settled)
    }
    this._stats.gateAllowsTotal = (this._stats.gateAllowsTotal || 0) + settled

    this._stats.stateOpsTotal  += stateOps.length
    this._stats.pushItemsTotal += pushItems.length
    await commitCycle({
      http: this.http,
      queryId: this._serverQueryId,
      partitionId,
      consumerGroup: group.consumerGroup,
      stateOps,
      pushItems,
      ack,
      releaseLease
    })
  }

  /**
   * Deep-copy state value before handing it to user code so the loaded
   * cache (loadedAll) isn't mutated in-place by gate evaluations that we
   * later decide to discard.
   */
  _cloneState(v) {
    if (v == null) return {}
    if (typeof v !== 'object') return v
    try { return JSON.parse(JSON.stringify(v)) } catch { return { ...v } }
  }

  // --------------------- stateless window path (tumbling / sliding / cron)

  async _processStatelessWindow(stages, envelopes, partitionId, orderedMessages) {
    const window = stages.window
    const reducer = stages.reducer

    // 3a. Load state for the partition FIRST. The watermark (event-time
    //     mode) lives under reserved __wm__; pull it into the per-partition
    //     cache so the late-event filter below can use it.
    const loadedAll = reducer
      ? await getState({
          http: this.http,
          queryId: this._serverQueryId,
          partitionId,
          keys: []
        })
      : new Map()

    if (window.eventTimeFn) {
      const wmRow = loadedAll.get(WATERMARK_STATE_KEY)
      if (wmRow && typeof wmRow.eventTimeMs === 'number') {
        this._partitionWatermarks.set(partitionId, wmRow.eventTimeMs)
      }
    }

    // 3b. Annotate each envelope with windowing metadata, fanning out for
    //     sliding windows. Late events (event-time mode) are filtered
    //     here against the just-loaded watermark.
    let annotated = []
    const watermarkMs = this._partitionWatermarks.get(partitionId)
    let droppedLate = 0
    let maxObservedEventTime = -Infinity

    for (const env of envelopes) {
      let outs
      try {
        outs = await window.apply(env)
      } catch (err) {
        this._reportError(err, { phase: 'window-apply', partition: partitionId, message: env.msg })
        continue
      }
      for (const annot of outs) {
        if (window.eventTimeFn && typeof annot.eventTimeMs === 'number') {
          if (annot.eventTimeMs > maxObservedEventTime) maxObservedEventTime = annot.eventTimeMs
          // Watermark is stored already-offset by allowedLateness, so the
          // late filter compares directly: anything below the watermark
          // is by definition past our tolerance.
          if (typeof watermarkMs === 'number' && annot.eventTimeMs < watermarkMs) {
            if (window.onLatePolicy === 'drop') {
              droppedLate++
              continue
            }
            // 'include': accumulate anyway (best-effort, may produce
            // duplicate emits if the window has already been flushed).
          }
        }
        annotated.push(annot)
      }
    }
    if (droppedLate > 0) {
      this._stats.lateEventsTotal += droppedLate
    }

    if (!reducer) {
      return {
        stateOps: [],
        emits: annotated.map(env => ({ key: env.key, value: env.value, sourceMsg: env.msg }))
      }
    }

    // 3c. Compute the close-trigger clock:
    //     event-time: use the watermark (advanced by this batch's events)
    //     processing-time: use max(createdAt) of source messages in this batch
    let clockMs
    if (window.eventTimeFn) {
      const advancedWm = Math.max(
        this._partitionWatermarks.get(partitionId) ?? -Infinity,
        maxObservedEventTime > -Infinity ? maxObservedEventTime - window.allowedLatenessMs : -Infinity
      )
      this._partitionWatermarks.set(partitionId, advancedWm)
      clockMs = advancedWm
    } else {
      let pt = -Infinity
      for (const m of orderedMessages) {
        const t = Date.parse(m.createdAt || '') || 0
        if (t > pt) pt = t
      }
      clockMs = pt
    }

    const result = await reducer.run({
      envelopes:    annotated,
      loadedState:  loadedAll,
      streamTimeMs: clockMs,
      operatorTag:  window.operatorTag,
      gracePeriodMs: window.gracePeriodMs
    })

    // 3d. If event-time mode and watermark moved, persist it.
    if (window.eventTimeFn) {
      const wm = this._partitionWatermarks.get(partitionId)
      if (typeof wm === 'number' && wm > -Infinity) {
        result.stateOps.push({
          type: 'upsert',
          key: WATERMARK_STATE_KEY,
          value: { eventTimeMs: wm }
        })
      }
    }

    return result
  }

  // ----------------------------------------------------- session window path

  async _processSessionWindow(stages, envelopes, partitionId, orderedMessages) {
    const window = stages.window
    const reducer = stages.reducer
    if (!reducer) {
      throw new Error('windowSession requires a downstream .reduce() or .aggregate()')
    }

    // Load state FIRST so the watermark (event-time mode) is in cache
    // before we filter late events.
    const loadedAll = await getState({
      http: this.http,
      queryId: this._serverQueryId,
      partitionId,
      keys: []
    })
    const sessionState = new Map()
    for (const [key, value] of loadedAll.entries()) {
      if (key.startsWith(window.operatorTag + '\u001fopen\u001f')) {
        sessionState.set(key, value)
      }
    }
    if (window.eventTimeFn) {
      const wmRow = loadedAll.get(WATERMARK_STATE_KEY)
      if (wmRow && typeof wmRow.eventTimeMs === 'number') {
        this._partitionWatermarks.set(partitionId, wmRow.eventTimeMs)
      }
    }

    // Filter late events in event-time mode against the loaded watermark.
    let droppedLate = 0
    let maxObservedEventTime = -Infinity
    const watermarkMs = this._partitionWatermarks.get(partitionId)
    const enriched = []
    for (const env of envelopes) {
      let annots
      try {
        annots = await window.apply(env)
      } catch (err) {
        this._reportError(err, { phase: 'window-apply', partition: partitionId, message: env.msg })
        continue
      }
      const annot = annots[0]
      if (window.eventTimeFn) {
        if (annot.eventTimeMs > maxObservedEventTime) maxObservedEventTime = annot.eventTimeMs
        // See comment above: watermark already includes the lateness offset.
        if (typeof watermarkMs === 'number' && annot.eventTimeMs < watermarkMs) {
          if (window.onLatePolicy === 'drop') {
            droppedLate++
            continue
          }
        }
      }
      enriched.push(annot)
    }
    if (droppedLate > 0) this._stats.lateEventsTotal += droppedLate

    // Reference time for the idle-close sweep inside runSession.
    let nowMs
    if (window.eventTimeFn) {
      const advancedWm = Math.max(
        this._partitionWatermarks.get(partitionId) ?? -Infinity,
        maxObservedEventTime > -Infinity ? maxObservedEventTime - window.allowedLatenessMs : -Infinity
      )
      this._partitionWatermarks.set(partitionId, advancedWm)
      nowMs = advancedWm
    } else {
      // Processing time: the batch's newest createdAt. That is the broker's
      // clock, the one the claimed closes use too; this host's Date.now()
      // could close a session early when it runs ahead, and would close
      // every session of an old backlog at once. Date.now() only when no
      // message carries a createdAt.
      let newest = -Infinity
      for (const m of orderedMessages || []) {
        const t = Date.parse(m.createdAt || '')
        if (Number.isFinite(t) && t > newest) newest = t
      }
      nowMs = newest > -Infinity ? newest : Date.now()
    }

    const reducerFn = reducer.fn
    const reducerInitial = () => reducer._initial ? reducer._initial() : (
      reducer.initial !== undefined ? JSON.parse(JSON.stringify(reducer.initial)) : 0
    )

    const result = await window.runSession({
      envelopes:     enriched,
      loadedState:   sessionState,
      reducerFn,
      reducerInitial,
      nowMs
    })

    if (window.eventTimeFn) {
      const wm = this._partitionWatermarks.get(partitionId)
      if (typeof wm === 'number' && wm > -Infinity) {
        result.stateOps.push({
          type: 'upsert',
          key: WATERMARK_STATE_KEY,
          value: { eventTimeMs: wm }
        })
      }
    }

    return result
  }

  // -------------------------------------------------------- post-reducer ops

  async _applyPostStages(postOps, emits, partitionId, partitionName) {
    if (postOps.length === 0 || emits.length === 0) return emits
    let working = emits
    for (const op of postOps) {
      const next = []
      for (const e of working) {
        const ctx = this._buildEmitCtx(e, partitionName, partitionId)
        const outs = await op.apply({ msg: e.value, key: e.key, value: e.value, ctx })
        for (const o of outs) {
          next.push({ ...e, value: o.value !== undefined ? o.value : o.msg })
        }
      }
      working = next
    }
    return working
  }

  _buildEmitCtx(e, partitionName, partitionId) {
    return {
      partition:   partitionName,
      partitionId: partitionId,
      key:         e.key,
      windowKey:   e.windowKey,
      windowStart: e.windowStart,
      windowEnd:   e.windowEnd
    }
  }

  // ------------------------------------------------ idle flush (per-window)

  async _flushTick() {
    // A tick that would overlap the one still running is skipped.
    if (this._stopped || this._flushInFlight) return
    const stages = this.stream.stages
    if (!stages.window) return
    this._flushInFlight = true
    const run = this._flushTickInner()
    this._flushPromise = run
    try {
      await run
    } finally {
      this._flushInFlight = false
    }
  }

  async _flushTickInner() {
    if (!this._leasedCloses() || this._claims === CLAIMS_NO) {
      await this._flushRecent()
    } else {
      if (Date.now() >= this._nextSweepAt) {
        await this._sweep(this._sweepUntouchedMs)
        this._nextSweepAt = Date.now() + SWEEP_EVERY_MS
        this._sweepUntouchedMs = SWEEP_UNTOUCHED_MS
      }
      // Not when stopping: a claim we would not close holds its partition
      // until the lease runs out.
      if (this._claims !== CLAIMS_NO && !this._isStopped()) await this._closeDue()
    }
    this._stats.lastFlushAt = new Date().toISOString()
  }

  /**
   * The unleased flush of the partitions this process popped in the last
   * five minutes: event-time windows, and every window on a broker without
   * the claim route.
   */
  async _flushRecent() {
    const partitions = Array.from(this._recentPartitions.entries())
    for (const [partitionId, info] of partitions) {
      if (this._isStopped()) break
      try {
        await this._flushPartition(partitionId, info.partitionName)
      } catch (err) {
        this._reportError(err, { phase: 'flush', partition: partitionId })
      }
    }
  }

  async _flushPartition(partitionId, partitionName) {
    // Mutual exclusion with the pop/cycle loop (see _partitionMutexes).
    return this._withPartitionLock(partitionId, () => this._flushPartitionInner(partitionId, partitionName))
  }

  async _flushPartitionInner(partitionId, partitionName) {
    const stages = this.stream.stages
    const window = stages.window

    // Reference clock for "is this window ripe?":
    //   event-time: cached watermark for this partition (no advancement,
    //               since we have no new events here)
    //   processing-time: Date.now()
    const clockMs = window.eventTimeFn
      ? this._partitionWatermarks.get(partitionId)
      : Date.now()

    if (typeof clockMs !== 'number') return  // no watermark yet, nothing to flush

    if (window.windowKind === 'session') {
      return this._flushSessionPartition(partitionId, partitionName, clockMs)
    }

    // Tumbling / sliding / cron: query for state rows with windowEnd <= ripe_at.
    const ripeAt = clockMs - window.gracePeriodMs
    if (ripeAt < 0) return

    const rows = await this._fetchRipeStateRows(partitionId, window.operatorTag + '\u001f', ripeAt)
    if (rows.size === 0) return

    // Reconstruct emits from each ripe row.
    let emits = []
    const stateOps = []
    for (const [stateKey, value] of rows.entries()) {
      const parts = parseStateKey(stateKey)
      if (!parts || parts.operatorTag !== window.operatorTag) continue
      const acc = value && value.acc !== undefined ? value.acc : value
      emits.push({
        key:         parts.userKey,
        windowStart: value.windowStart ?? Date.parse(parts.windowKey) ?? 0,
        windowEnd:   value.windowEnd   ?? null,
        windowKey:   parts.windowKey,
        value:       acc,
        sourceMsg:   null
      })
      stateOps.push({ type: 'delete', key: stateKey })
    }
    if (emits.length === 0) return

    emits = await this._applyPostStages(stages.post, emits, partitionId, partitionName)

    // Build push items / run foreach.
    let pushItems = []
    if (stages.sink && stages.sink.kind === 'sink') {
      pushItems = stages.sink.buildPushItems(
        emits.map(e => ({ key: e.key, value: e.value, sourceMsg: e.sourceMsg })),
        partitionName
      )
    } else if (stages.sink && stages.sink.kind === 'foreach') {
      await stages.sink.runEffects(emits.map(e => ({
        value: e.value,
        ctx:   this._buildEmitCtx(e, partitionName, partitionId)
      })))
    }

    this._stats.flushCyclesTotal++
    this._stats.stateOpsTotal += stateOps.length
    this._stats.pushItemsTotal += pushItems.length

    await commitCycle({
      http: this.http,
      queryId: this._serverQueryId,
      partitionId,
      consumerGroup: this.consumerGroup,
      stateOps,
      pushItems,
      ack: null
    })
  }

  async _flushSessionPartition(partitionId, partitionName, clockMs) {
    const stages = this.stream.stages
    const window = stages.window

    // Load all open sessions for this operator on this partition.
    const all = await getState({
      http: this.http,
      queryId: this._serverQueryId,
      partitionId,
      keys: []
    })
    const sessionState = new Map()
    for (const [key, value] of all.entries()) {
      if (key.startsWith(window.operatorTag + '\u001fopen\u001f')) {
        sessionState.set(key, value)
      }
    }
    if (sessionState.size === 0) return

    const reducer = stages.reducer
    const reducerInitial = () => reducer._initial ? reducer._initial() : 0
    const result = await window.runSession({
      envelopes: [],
      loadedState: sessionState,
      reducerFn: reducer.fn,
      reducerInitial,
      nowMs: clockMs
    })
    if (result.emits.length === 0) return

    const emits = await this._applyPostStages(stages.post, result.emits, partitionId, partitionName)

    let pushItems = []
    if (stages.sink && stages.sink.kind === 'sink') {
      pushItems = stages.sink.buildPushItems(
        emits.map(e => ({ key: e.key, value: e.value, sourceMsg: null })),
        partitionName
      )
    } else if (stages.sink && stages.sink.kind === 'foreach') {
      await stages.sink.runEffects(emits.map(e => ({
        value: e.value,
        ctx:   this._buildEmitCtx(e, partitionName, partitionId)
      })))
    }

    this._stats.flushCyclesTotal++
    this._stats.stateOpsTotal += result.stateOps.length
    this._stats.pushItemsTotal += pushItems.length

    await commitCycle({
      http: this.http,
      queryId: this._serverQueryId,
      partitionId,
      consumerGroup: this.consumerGroup,
      stateOps: result.stateOps,
      pushItems,
      ack: null
    })
  }

  /**
   * Use the state route's key_prefix + ripe_at_or_before filters to scope
   * the fetch.
   */
  async _fetchRipeStateRows(partitionId, keyPrefix, ripeAtMs) {
    const body = {
      query_id:           this._serverQueryId,
      partition_id:       partitionId,
      keys:               [],
      key_prefix:         keyPrefix,
      ripe_at_or_before:  ripeAtMs
    }
    const res = await this.http.post('/streams/v1/state/get', body)
    const map = new Map()
    if (res && Array.isArray(res.rows)) {
      for (const row of res.rows) {
        if (row && typeof row.key === 'string') map.set(row.key, row.value)
      }
    }
    return map
  }

  // ------------------------------------------------ closes under a claim
  //
  // A processing-time window closes when its time comes even if its
  // partition gets no more messages. The runner keeps, per partition, when
  // the earliest open window closes (from each cycle it commits, and from
  // the sweep), claims the partition when that time has come
  // (POST /streams/v1/claim: a lease with an empty batch, granted only when
  // nobody holds the partition and the group has read all of it), and
  // closes the windows in a cycle that acks the empty batch. The broker
  // refuses that cycle whole when the lease is gone, so two runners never
  // close one window, and a close never deletes a window another runner
  // just extended. The clock is the broker's, the one createdAt comes from.

  /**
   * Whether this stream closes its windows under claims: processing-time
   * windows with a reducer and an idle flush. Event-time windows close on
   * the watermark, which only events move.
   */
  _leasedCloses() {
    const { window, reducer } = this.stream.stages
    return !!(window && !window.eventTimeFn && window.idleFlushMs > 0 && reducer)
  }

  /** Track (or forget, atMs null) when the partition's earliest open window closes. */
  _setDue(partitionId, partitionName, atMs) {
    if (!partitionId) return
    if (typeof atMs === 'number' && Number.isFinite(atMs)) {
      this._due.set(partitionId, { partitionName, atMs })
    } else {
      this._due.delete(partitionId)
    }
  }

  /** Try a tracked partition again no sooner than atMs. */
  _retryDue(partitionId, atMs) {
    const d = this._due.get(partitionId)
    if (d) d.atMs = Math.max(d.atMs, atMs)
  }

  _claimsOff() {
    if (this._claims !== CLAIMS_NO) {
      this.logger.info('broker has no stream claims; idle flush of recent partitions', { queryId: this.queryId })
    }
    this._claims = CLAIMS_NO
    // Nothing left that would never be closed.
    this._due.clear()
  }

  /**
   * The partitions holding the query's state that nobody touched for
   * `untouchedMs` (0: all of them) and this runner does not track yet:
   * tracked as due now, so the next ticks claim them a batch at a time.
   */
  async _sweep(untouchedMs) {
    let after = null
    for (;;) {
      if (this._isStopped()) return
      const body = { query_id: this._serverQueryId, limit: SWEEP_PAGE }
      if (untouchedMs > 0) body.untouched_for_ms = untouchedMs
      if (after) body.after = after
      let res
      try {
        res = await this.http.post('/streams/v1/state/partitions', body)
      } catch (err) {
        // A broker before 2.7: no claims either.
        if (err && err.status === 404) return this._claimsOff()
        this._reportError(err, { phase: 'sweep' })
        return
      }
      if (!res) return
      if (res.success === false) {
        this._reportError(new Error(res.error || 'state/partitions failed'), { phase: 'sweep' })
        return
      }
      const now = Date.now()
      for (const p of res.partitions || []) {
        if (!p || !p.partition_id || this._due.has(p.partition_id)) continue
        this._due.set(p.partition_id, { partitionName: p.partition || '', atMs: now })
      }
      if (!res.next) return
      after = res.next
    }
  }

  /** Claim the partitions whose windows are due, and close them. */
  async _closeDue() {
    const window = this.stream.stages.window
    const now = Date.now()
    const ready = []
    for (const [partitionId, d] of this._due) {
      if (d.atMs <= now) ready.push([partitionId, d.atMs])
    }
    if (ready.length === 0) return
    ready.sort((a, b) => a[1] - b[1])
    const partitionIds = ready.slice(0, CLAIM_BATCH).map(([partitionId]) => partitionId)

    let res
    try {
      res = await this.http.post('/streams/v1/claim', {
        query_id:       this._serverQueryId,
        partition_ids:  partitionIds,
        consumer_group: this.consumerGroup,
        lease_seconds:  CLAIM_LEASE_S
      })
    } catch (err) {
      if (err && err.status === 404) return this._claimsOff()
      this._reportError(err, { phase: 'claim' })
      const retryAt = Date.now() + CLAIM_RETRY_MS
      for (const partitionId of partitionIds) this._retryDue(partitionId, retryAt)
      return
    }
    if (!res) return
    this._claims = CLAIMS_YES
    if (res.success === false) {
      this._reportError(new Error(res.error || 'claim refused'), { phase: 'claim' })
      return
    }

    for (const p of res.partitions || []) {
      const partitionId = p.partition_id
      let partitionName = p.partition
      if (!partitionName) {
        const d = this._due.get(partitionId)
        partitionName = d ? d.partitionName : ''
      }
      if (p.status === 'claimed') {
        // Stopping: the claims not closed yet lapse with their lease,
        // nothing is lost.
        if (this._isStopped()) continue
        const clockMs = Number.isFinite(res.now_ms) ? res.now_ms : Date.now()
        try {
          await this._closeClaimed(partitionId, partitionName, res.lease_id, clockMs)
        } catch (err) {
          // Its claim lapses with the lease; the next try comes after it.
          this._reportError(err, { phase: 'flush', partition: partitionId })
          this._retryDue(partitionId, Date.now() + CLAIM_RETRY_MS)
        }
      } else if (p.status === 'busy') {
        // Someone holds it (a cycle, maybe another runner's): come back
        // after an idle-flush interval.
        this._retryDue(partitionId, Date.now() + Math.max(window.idleFlushMs, CLAIM_RETRY_MS))
      } else {
        // backlog: messages wait, and the cycle that takes them tracks the
        // partition again. missing: nothing to close there.
        this._setDue(partitionId, partitionName, null)
      }
    }
  }

  /**
   * Close what is ripe on a claimed partition at `clockMs` (the broker's
   * time at the claim), in a cycle that acks the claim's empty batch, and
   * track when the windows left open close. The due time is set while the
   * partition lock is held, as a cycle sets it: set after, it could
   * overwrite the newer time of a cycle that ran in between.
   */
  _closeClaimed(partitionId, partitionName, leaseId, clockMs) {
    // Mutual exclusion with the pop/cycle loop (see _partitionMutexes).
    return this._withPartitionLock(partitionId, async () => {
      const stages = this.stream.stages
      const window = stages.window
      const reducer = stages.reducer
      // Every row: the windows left open say when to come back.
      const loadedAll = await getState({
        http: this.http,
        queryId: this._serverQueryId,
        partitionId,
        keys: []
      })
      let result
      if (window.windowKind === 'session') {
        const sessionState = new Map()
        for (const [key, value] of loadedAll.entries()) {
          if (key.startsWith(window.operatorTag + '\u001fopen\u001f')) sessionState.set(key, value)
        }
        result = await window.runSession({
          envelopes:      [],
          loadedState:    sessionState,
          reducerFn:      reducer.fn,
          reducerInitial: () => reducer._initial ? reducer._initial() : 0,
          nowMs:          clockMs
        })
      } else {
        result = await reducer.run({
          envelopes:     [],
          loadedState:   loadedAll,
          streamTimeMs:  clockMs,
          operatorTag:   window.operatorTag,
          gracePeriodMs: window.gracePeriodMs
        })
      }
      const closed = result.emits.length > 0
      const emits = await this._applyPostStages(stages.post, result.emits, partitionId, partitionName)
      const pushItems = await this._terminal(emits, partitionName, partitionId)
      // The empty batch's ack: count 0 keeps the cursor where it is and the
      // lease goes back, also when nothing was ripe (another runner closed
      // it first).
      await commitCycle({
        http: this.http,
        queryId: this._serverQueryId,
        partitionId,
        consumerGroup: this.consumerGroup,
        stateOps: result.stateOps,
        pushItems,
        ack: { transactionId: '', leaseId, status: 'completed', count: 0 },
        releaseLease: true
      })
      if (closed) this._stats.flushCyclesTotal++
      this._stats.stateOpsTotal += result.stateOps.length
      this._stats.pushItemsTotal += pushItems.length
      this._setDue(partitionId, partitionName, result.nextCloseMs ?? null)
    })
  }

  /** Sink push items for the emits, or run foreach on them (then none). */
  async _terminal(emits, partitionName, partitionId) {
    const sink = this.stream.stages.sink
    if (sink && sink.kind === 'sink') {
      return sink.buildPushItems(
        emits.map(e => ({ key: e.key, value: e.value, sourceMsg: e.sourceMsg || null })),
        partitionName
      )
    }
    if (sink && sink.kind === 'foreach') {
      await sink.runEffects(emits.map(e => ({
        value: e.value,
        ctx:   this._buildEmitCtx(e, partitionName, partitionId)
      })))
    }
    return []
  }

  // --------------------------------------------------------------- helpers

  _extractQueueName(qb) {
    if (!qb) return null
    if (qb && typeof qb.name === 'string' && qb.name.length > 0) return qb.name
    if (typeof qb._queueName === 'string') return qb._queueName
    if (typeof qb.queueName === 'string') return qb.queueName
    return ''
  }
}
