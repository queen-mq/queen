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
 *   flush timer:
 *     periodically wake -> for partitions touched recently, query state
 *     for ripe windows -> apply post-reducer ops + foreach + sink push,
 *     commit no-ack cycle (state deletes + sink push, no source ack).
 *
 *   stop():
 *     - signal abort, wait for in-flight cycle and flush, return.
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

const DEFAULT_BATCH_SIZE = 200
const DEFAULT_MAX_PARTITIONS = 4
const DEFAULT_IDLE_SLEEP_MS = 100
// Track partitions we've touched within this rolling window. Drives the
// idle-flush sweep — anything older than this is assumed to be owned by a
// different worker and we don't speculatively flush it.
const PARTITION_RECENCY_MS = 5 * 60 * 1000  // 5 minutes

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
    this._recentPartitions     = new Map()   // partitionId -> { partitionName, touchedAt }
    this._partitionWatermarks  = new Map()   // partitionId -> wmMs (cache; PG is source of truth)
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
    return this
  }

  async stop() {
    this._stopped = true
    for (const t of this._flushTimers) clearInterval(t)
    this._flushTimers = []
    if (this._loopPromise) await this._loopPromise
    // Allow any in-flight flush to drain.
    while (this._flushInFlight) await sleep(20)
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
          this._touchPartition(group.partitionId, group.partitionName)
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

    if (this.maxPartitions > 1) {
      qb = qb.partitions(this.maxPartitions)
    }
    if (this.subscriptionMode) qb = qb.subscriptionMode(this.subscriptionMode)
    if (this.subscriptionFrom) qb = qb.subscriptionFrom(this.subscriptionFrom)
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

  // ----------------------------------------------------------- cycle

  async _processPartitionCycle(group) {
    const stages = this.stream.stages
    const partitionId = group.partitionId
    const partitionName = group.partitionName

    // Sort messages by (createdAt, id) to preserve partition FIFO order.
    const orderedMessages = [...group.messages].sort((a, b) => {
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

    // 3. Window operator + reducer (if present).
    let stateOps = []
    let emits = []
    if (stages.window) {
      const result = stages.window.windowKind === 'session'
        ? await this._processSessionWindow(stages, envelopes, partitionId)
        : await this._processStatelessWindow(stages, envelopes, partitionId, orderedMessages)
      stateOps = result.stateOps
      emits = result.emits
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
    //    and tell the SP how many messages were in this cycle's batch
    //    so partition_consumers.acked_count / lease release logic uses
    //    the correct count (the cycle is atomic across the full batch).
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

  async _processSessionWindow(stages, envelopes, partitionId) {
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
      // Use max event time observed in the batch; idle-flush uses Date.now().
      nowMs = maxObservedEventTime > -Infinity ? maxObservedEventTime : Date.now()
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
    if (this._stopped || this._flushInFlight) return
    const stages = this.stream.stages
    if (!stages.window) return
    this._flushInFlight = true
    try {
      const partitions = Array.from(this._recentPartitions.entries())
      for (const [partitionId, info] of partitions) {
        if (this._isStopped()) break
        try {
          await this._flushPartition(partitionId, info.partitionName)
        } catch (err) {
          this._reportError(err, { phase: 'flush', partition: partitionId })
        }
      }
      this._stats.lastFlushAt = new Date().toISOString()
    } finally {
      this._flushInFlight = false
    }
  }

  async _flushPartition(partitionId, partitionName) {
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
   * Use the streams_state_get_v1 SP's key_prefix + ripe_at_or_before
   * filters to scope the fetch.
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

  // --------------------------------------------------------------- helpers

  _extractQueueName(qb) {
    if (!qb) return null
    if (qb && typeof qb.name === 'string' && qb.name.length > 0) return qb.name
    if (typeof qb._queueName === 'string') return qb._queueName
    if (typeof qb.queueName === 'string') return qb.queueName
    return ''
  }
}
