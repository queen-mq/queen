/**
 * Stream — fluent builder for streaming pipelines.
 *
 * A Stream is an immutable chain of Operators that gets compiled and run
 * by a Runner. Each combinator (`.map`, `.filter`, ...) returns a NEW Stream
 * with the operator appended; the chain is finalized by `.run({ queryId })`.
 *
 * The chain shape (operator types + their stateful config) is fingerprinted
 * via configHash so that re-deploying with a different chain under the same
 * queryId is rejected at registration unless `{ reset: true }` is passed.
 */

import { MapOperator } from './operators/MapOperator.js'
import { FilterOperator } from './operators/FilterOperator.js'
import { FlatMapOperator } from './operators/FlatMapOperator.js'
import { KeyByOperator } from './operators/KeyByOperator.js'
import { WindowTumblingOperator } from './operators/WindowTumblingOperator.js'
import { WindowSlidingOperator } from './operators/WindowSlidingOperator.js'
import { WindowSessionOperator } from './operators/WindowSessionOperator.js'
import { WindowCronOperator } from './operators/WindowCronOperator.js'
import { ReduceOperator } from './operators/ReduceOperator.js'
import { AggregateOperator } from './operators/AggregateOperator.js'
import { GateOperator } from './operators/GateOperator.js'
import { SinkOperator } from './operators/SinkOperator.js'
import { ForeachOperator } from './operators/ForeachOperator.js'
import { ForeachBatchOperator } from './operators/ForeachBatchOperator.js'
import { PatternOperator } from './operators/PatternOperator.js'
import { CollectOperator } from './operators/CollectOperator.js'
import { TableOperator } from './operators/TableOperator.js'
import { NameOperator } from './operators/NameOperator.js'
import { Runner } from './runtime/Runner.js'
import { FeedRunner } from './runtime/FeedRunner.js'
import { FeedShardGroup } from './runtime/FeedShards.js'
import { FeedView } from './runtime/feed/query.js'
import { configHashOf } from './util/configHash.js'
import { whereCondition, conditionHolds } from '../feed/Feed.js'

// Operators that run on a feed source only.
const FEED_ONLY = new Set(['pattern', 'collect', 'table', 'name'])

export class Stream {
  /**
   * Build a Stream sourced from the tenant feed: every push to the tenant's
   * queues (or the ones named), in log order, read from followers.
   *
   *   await Stream
   *     .fromFeed(queen, { queues: ['orders'] })
   *     .where('data.country', '=', 'IT')      // sent to the broker, filtered there
   *     .filter(m => isVip(m.data.customer))   // your code, runs here
   *     .to(queen.queue('orders-it'))
   *     .run({ queryId: 'orders-it' })
   *
   * The runner keeps its position in KV (`queen-feed` / queryId) and commits
   * it with the sink's pushes, and the state of its windows and patterns, in
   * one transaction. One process runs a queryId at a time; another one with
   * the same queryId waits and takes over when the first stops or dies.
   *
   * A chain on a feed: where, filter, map, flatMap, keyBy, and any number of
   * stateful steps in any order -- a window with reduce/aggregate, a pattern,
   * a join (two sides or more), a gather, a table join -- each step's results
   * feeding the next; then .to() (one queue, several, or a function that
   * picks), .foreach() or .foreachBatch(). Gates run on queue sources only.
   *
   * @param {object} queen - the Queen client
   * @param {object} [options]
   * @param {string|string[]} [options.queues] - names or patterns; default every queue
   * @param {string|string[]} [options.partitions] - partition names
   * @param {number|'now'|'earliest'} [options.from='now'] - where a NEW query starts
   * @param {Date|number|string} [options.since] - a new query starts at messages created since ('1d', ISO, Date)
   * @param {string[]} [options.followerUrls] - nodes to read from first
   * @returns {Stream}
   */
  static fromFeed(queen, options = {}) {
    if (!queen || typeof queen.feed !== 'function' || typeof queen.transaction !== 'function') {
      throw new Error('Stream.fromFeed(queen, options): pass the Queen client')
    }
    return new Stream({
      source: { kind: 'feed', queen, options: { ...options } },
      operators: [],
      sourceOptions: options
    })
  }
  /**
   * Build a Stream sourced from a Queen QueueBuilder.
   *
   * @param {object} queueBuilder - the result of queen.queue('name') — must
   *   expose .pop() / .ack() / .push() and the queue name.
   * @param {object} [options]
   * @returns {Stream}
   */
  static from(queueBuilder, options = {}) {
    return Stream._fromQueue(queueBuilder, options)
  }

  /**
   * Ask a running feed stream about its live state, from any process:
   *
   *   const view = Stream.view(queen, 'reply-time')
   *   await view.get('perTenant', 'tenant-42')
   *   await view.list('perTenant', { limit: 50 })
   *   await view.status()
   *
   * Each active runner answers on an ephemeral queue of its own (see
   * runtime/feed/query.js). Steps are named with .name(), else step1, step2...
   *
   * @param {object} queen
   * @param {string} queryId
   * @param {{shards?: number, timeoutMillis?: number}} [opts]
   * @returns {FeedView}
   */
  static view(queen, queryId, opts = {}) {
    return new FeedView(queen, queryId, opts)
  }

  static _fromQueue(queueBuilder, options = {}) {
    return new Stream({
      source: queueBuilder,
      operators: [],
      sourceOptions: options
    })
  }

  /**
   * @param {object} args
   * @param {object} args.source - source QueueBuilder
   * @param {object} [args.sourceOptions]
   * @param {Array} args.operators - operator chain
   */
  constructor({ source, operators, sourceOptions = {} }) {
    this._source = source
    this._sourceOptions = sourceOptions
    this._operators = operators
  }

  // ---------------------------------------------------------------- Stateless

  /** @param {(msg:object)=>any|Promise<any>} fn */
  map(fn) {
    return this._extend(new MapOperator(fn))
  }

  /** @param {(msg:object)=>boolean|Promise<boolean>} predicate */
  filter(predicate) {
    return this._extend(new FilterOperator(predicate))
  }

  /**
   * Keep only the source messages where `field op value` holds. On a feed
   * source the condition is sent to the broker and filtered there (nothing
   * that fails it crosses the network); on a queue source it is evaluated
   * here. Wherever it sits in the chain, it tests the source message.
   *
   *   .where('data.country', '=', 'IT')
   *   .where('data.total', '>', 100)
   *   .where('partition', 'prefix', 'booking-')
   *
   * Fields: queue, partition, transactionId, offset, position, data,
   * data.<path>. Ops: = != < <= > >= in exists prefix.
   */
  where(field, op, value) {
    const cond = whereCondition(field, op, value)
    const filter = new FilterOperator((msg) => conditionHolds(cond, msg))
    filter.where = cond
    filter.config = { kind: 'filter', where: cond }
    return this._extend(filter)
  }

  /** @param {(msg:object)=>(any[]|Promise<any[]>)} fn */
  flatMap(fn) {
    return this._extend(new FlatMapOperator(fn))
  }

  // -------------------------------------------------------------------- Keying

  /**
   * Override the implicit partition-key for downstream stateful operators.
   * If omitted, stateful operators use the source partition_id as the key,
   * which is the natural fit for queues partitioned by entity.
   *
   * Warning: when the key differs from the partition, multiple workers
   * holding different partition leases may write the same logical key,
   * causing cross-worker contention on (query_id, partition_id, key) state
   * rows. The repartition pattern (push to a co-keyed intermediate queue,
   * then process) avoids this.
   *
   * @param {(msg:object)=>string} fn
   */
  keyBy(fn) {
    return this._extend(new KeyByOperator(fn))
  }

  // -------------------------------------------------------------------- Windows

  /**
   * Fixed-size, non-overlapping windows.
   * @param {{
   *   seconds: number,
   *   gracePeriod?: number,    // accept events for windowEnd + grace before closing
   *   idleFlushMs?: number,    // close ripe windows on quiet partitions every N ms (default 5000)
   *   eventTime?: ((msg:any)=>(number|Date|string)) | false,  // event-time mode if set; false: the feed's clock after a step on event time
   *   allowedLateness?: number | string, // event-time only: drop events older than wm - allowedLateness (seconds, or '5m')
   *   onLate?: 'drop' | 'include',
   *   idle?: number | string    // feed, the first step on event time: after this long without a message the watermark moves on with the feed's clock
   * }} opts
   */
  windowTumbling(opts) {
    return this._extend(new WindowTumblingOperator(opts))
  }

  /**
   * Overlapping fixed-size windows that hop every `slide` seconds.
   * Each event creates `size/slide` state rows per key.
   * @param {{size:number, slide:number, gracePeriod?:number, idleFlushMs?:number,
   *          eventTime?:((msg:any)=>any)|false, allowedLateness?:number, onLate?:string, idle?:number|string}} opts
   */
  windowSliding(opts) {
    return this._extend(new WindowSlidingOperator(opts))
  }

  /**
   * Per-key activity-based windows. A session for a key extends as long
   * as events keep arriving within `gap` seconds of each other. On a feed and
   * event time, a session takes its messages in event-time order (each waits
   * `allowedLateness` for the ones before it).
   * @param {{gap:number, gracePeriod?:number, idleFlushMs?:number,
   *          eventTime?:((msg:any)=>any)|false, allowedLateness?:number, onLate?:string, idle?:number|string}} opts
   */
  windowSession(opts) {
    return this._extend(new WindowSessionOperator(opts))
  }

  /**
   * Wall-clock-aligned windows. Currently supports the `every:` shorthand
   * (`'second'|'minute'|'hour'|'day'|'week'`); full cron syntax is reserved
   * for v0.3.
   * @param {{every:string, gracePeriod?:number, idleFlushMs?:number,
   *          eventTime?:((msg:any)=>any)|false, allowedLateness?:number, onLate?:string, idle?:number|string}} opts
   */
  windowCron(opts) {
    return this._extend(new WindowCronOperator(opts))
  }

  // -------------------------------------------------------------------- Reduce

  /**
   * @template T
   * @param {(acc:T, msg:object)=>T|Promise<T>} fn
   * @param {T} initial
   */
  reduce(fn, initial) {
    return this._extend(new ReduceOperator(fn, initial))
  }

  /**
   * Sugar over reduce. Each provided extractor produces one named field on
   * the aggregate value.
   *
   * Example:
   *   .aggregate({ count: () => 1, sum: (v) => v.amount })
   *
   * Output value shape: { count: number, sum: number, ... }
   *
   * @param {Record<string,(msg:object)=>number>} extractors
   */
  aggregate(extractors) {
    return this._extend(new AggregateOperator(extractors))
  }

  // ------------------------------------------------------------------ Patterns

  /**
   * A `start` message, then a `then` message for the same entity within
   * `within`, on the feed's clock (the time the log committed each message,
   * the same for every queue). Feed sources only.
   *
   *   Stream.fromFeed(queen, { queues: ['orders', 'payments'] })
   *     .pattern({
   *       start: (m) => m.queue === 'orders',
   *       then:  (m) => m.queue === 'payments',
   *       within: '15m',
   *       onMatch:   (order, payment) => ({ order: order.data.id, paid: true }),
   *       onTimeout: (order) => ({ order: order.data.id, remind: true })
   *     })
   *     .to(queen.queue('order-followups'))
   *
   * The entity is `key(msg, value)` (value: the item of a `.flatMap()` before),
   * else `.keyBy()`, else the partition name.
   * `onMatch` and `onTimeout` return what to send on (null: nothing); without
   * `onMatch` a match sends `{ key, start, then }`, without `onTimeout` a
   * deadline passes in silence. `onRepeat: 'restart'` lets a new `start`
   * replace the one waiting (the default `'keep'` ignores it).
   *
   * Event time: `eventTime: (m) => m.data.at` puts `within` on the messages'
   * own times (a Date, epoch ms or ISO string), for history pushed long after
   * it happened or producers that send late. The step takes its messages in
   * event-time order: each one that concerns it waits `allowedLateness` (a
   * duration, default 0) for any earlier one still to come; one older than
   * that is late (`onLate: 'drop'`, the default, or `'include'`: taken at
   * once). `idle: '1m'` (on the first step on event time): after a minute
   * without a message, event time moves on with the feed's clock, so
   * deadlines pass on a quiet feed. A step after a step on event time runs on
   * event time too and follows its watermark (`eventTime: false` keeps it on
   * the feed's clock). Messages the tests do not take need no event time.
   *
   * @param {{start:Function, then:Function, within:string, key?:Function,
   *          onMatch?:Function, onTimeout?:Function, onRepeat?:'keep'|'restart',
   *          eventTime?:Function|false, allowedLateness?:string|number,
   *          onLate?:'drop'|'include', idle?:string|number}} opts
   */
  pattern(opts) {
    return this._extend(new PatternOperator('pattern', opts))
  }

  /**
   * A `left` and a `right` message for the same entity, in either order,
   * within `within` of each other: the second one sends `select(left, right)`
   * (default `{ key, left, right }`). One left alone past its deadline sends
   * `onTimeout(msg, 'left' | 'right')` if given. Feed sources only.
   *
   *   .join({
   *     left:  (m) => m.queue === 'orders',
   *     right: (m) => m.queue === 'shipments',
   *     within: '2d',
   *     select: (order, shipment) => ({ order: order.data.id, carrier: shipment.data.carrier })
   *   })
   *
   * `eventTime`, `allowedLateness`, `onLate` and `idle` work as on `.pattern()`.
   *
   * @param {{left:Function, right:Function, within:string, key?:Function,
   *          select?:Function, onTimeout?:Function, onRepeat?:'keep'|'restart',
   *          eventTime?:Function|false, allowedLateness?:string|number,
   *          onLate?:'drop'|'include', idle?:string|number}} opts
   */
  join(opts) {
    if (opts && typeof opts === 'object' && opts.sides !== undefined) {
      return this._extend(new CollectOperator('join', opts))
    }
    return this._extend(new PatternOperator('join', opts))
  }

  /**
   * The parts of one run, collected until all are in or the deadline passes.
   * `steps` names the parts (queue names, or `{ name: (msg, value) => bool }`);
   * `count(msg, value)` instead says how many distinct messages a run has.
   * All in: `onComplete(got, key)`; past the deadline: `onTimeout(got,
   * missing, key)`. A finished run is remembered until its deadline, so a late
   * duplicate opens nothing. Feed sources only.
   *
   *   .gather({
   *     steps: ['payments', 'stock', 'shipping'],
   *     key: (m) => m.data.orderId,
   *     within: '5m',
   *     onComplete: (got, order) => ({ order, ready: true }),
   *     onTimeout: (got, missing, order) => ({ order, missing })
   *   })
   *
   * On event time (`eventTime`, as on `.pattern()`) the deadline counts from
   * the earliest part's event time.
   *
   * @param {{steps?:string[]|object, count?:Function, within:string, key?:Function,
   *          onComplete?:Function, onTimeout?:Function, onRepeat?:'keep'|'latest', keep?:Function,
   *          eventTime?:Function|false, allowedLateness?:string|number,
   *          onLate?:'drop'|'include', idle?:string|number}} opts
   */
  gather(opts) {
    return this._extend(new CollectOperator('gather', opts))
  }

  /**
   * Join every message with the latest row of a table queue (one message per
   * change; null deletes). A message sees the table as of its own position.
   * Feed sources only.
   *
   *   .joinTable({
   *     queue: 'property-settings',
   *     on: (m) => m.data.propertyId,
   *     select: (booking, settings) => ({ ...booking, currency: settings?.currency })
   *   })
   *
   * @param {{queue:string|string[], on:Function, key?:Function, value?:Function,
   *          select?:Function, missing?:'keep'|'drop', initial?:'all'|'now'}} opts
   */
  joinTable(opts) {
    return this._extend(new TableOperator(opts))
  }

  /**
   * Name the stateful step before this call, for live reads
   * (`handle.state(name)`, `Stream.view(...).get(name, key)`).
   * @param {string} name
   */
  name(name) {
    return this._extend(new NameOperator(name))
  }

  // ---------------------------------------------------------------------- Gate

  /**
   * Per-message ALLOW/DENY decision with persistent per-key state.
   *
   * Used to build rate limiters, throttlers, fairness gates, circuit
   * breakers — anything where the semantics is "should this message
   * proceed RIGHT NOW given the recent history of this key?".
   *
   * The user fn receives `(value, ctx)` where:
   *   - value: the message payload (post any pre-stage map/filter)
   *   - ctx.state: mutable per-key state (loaded from the broker,
   *                persisted only if you return ALLOW for this message)
   *   - ctx.streamTimeMs: system clock for the cycle (use for refill math)
   *   - ctx.partitionId: source partition_id (= state shard)
   *
   * Return `true` (or `{allow:true}`) to let the message through.
   * Return `false` (or `{allow:false}`) to halt the batch here. The runner
   * commits an ack for the prefix that was allowed and DOES NOT release the
   * source lease, so the denied message and its successors get redelivered
   * in their original order when the lease expires. FIFO per partition is
   * preserved without any deferred queue. The prefix is counted in source
   * messages: after a `.flatMap()`, a message is allowed only if every value
   * it produced is allowed.
   *
   * Example (token bucket rate limiter):
   *
   *   .gate((req, ctx) => {
   *     const cfg = { capacity: 10, refillPerSec: 5 }
   *     const now = ctx.streamTimeMs
   *     ctx.state.tokens       ??= cfg.capacity
   *     ctx.state.lastRefillAt ??= now
   *     const elapsedSec = (now - ctx.state.lastRefillAt) / 1000
   *     ctx.state.tokens = Math.min(
   *       cfg.capacity,
   *       ctx.state.tokens + elapsedSec * cfg.refillPerSec
   *     )
   *     ctx.state.lastRefillAt = now
   *     if (ctx.state.tokens >= 1) {
   *       ctx.state.tokens -= 1
   *       return true
   *     }
   *     return false   // halt batch, lease expires, redelivered in order
   *   })
   *
   * Constraints
   *  - At most one .gate() per stream (multiple gates would compose
   *    awkwardly with the partial-ack semantics; chain serially via two
   *    streams if needed).
   *  - .gate() is incompatible with windowing/reducing in the same stream.
   *    The window+reducer model assumes the FULL batch is consumed atomically;
   *    gating breaks that. Run them as two separate streams.
   *
   * @param {(value:any, ctx:{state:object, streamTimeMs:number, partitionId:string}) => boolean | {allow:boolean} | Promise<boolean | {allow:boolean}>} fn
   */
  gate(fn) {
    return this._extend(new GateOperator(fn))
  }

  // ---------------------------------------------------------------------- Sink

  /**
   * Sink to Queen queues. The cycle commits state + push + ack atomically.
   *
   *   .to(queen.queue('orders-it'))                    // one queue
   *   .to([queen.queue('audit'), queen.queue('bi')])   // each record to both (feed)
   *   .to((order) => `orders-${order.country}`)       // a function picks (feed)
   *
   * A function gets `(value, msg)` and answers a queue (builder or name),
   * `{ queue, partition }`, a list of those, or null for nowhere. Several
   * queues and functions run on `Stream.fromFeed`; a queue source sinks to one.
   *
   * @param {object|string|Array|Function} target - `queen.queue('sink-name')`, a list, or a function
   * @param {{partition?: string|((value:any)=>string)}} [opts]
   */
  to(target, opts = {}) {
    return this._extend(new SinkOperator(target, opts))
  }

  /**
   * Terminal at-least-once side-effect. The cycle acks the source only after
   * `fn` has resolved successfully, so a crash mid-`fn` will redeliver. If
   * you need exactly-once external effects, write to a sink queue and have
   * a dedicated worker consume it.
   *
   * @param {(value:any)=>void|Promise<void>} fn
   */
  foreach(fn) {
    return this._extend(new ForeachOperator(fn))
  }

  /**
   * Terminal at-least-once side effect, in batches (Stream.fromFeed only).
   * `fn(items)` gets up to `maxItems` results, each `{ value, ctx, id }`: the
   * value and context `.foreach()` would get, and an id that is the same on
   * every delivery of that result (a key for an upsert). A batch goes at the
   * end of each read, or, with `maxWaitMillis`, once it is full or its first
   * item has waited that long. The position moves only after `fn` returns:
   * a crash before that delivers the batch again.
   *
   *   .foreachBatch(async (items) => {
   *     await db.upsert('totals', items.map((it) => ({ id: it.id, ...it.value })))
   *   }, { maxItems: 500, maxWaitMillis: 1000 })
   *
   * A batch that throws is tried again whole (`retries`), then item by item,
   * so the poison policy (`onPoison`) meets only the item that fails.
   *
   * @param {(items:Array<{value:any, ctx:object, id:string}>)=>void|Promise<void>} fn
   * @param {{maxItems?: number, maxWaitMillis?: number}} [opts] - maxItems 1000, maxWaitMillis 0
   */
  foreachBatch(fn, opts = {}) {
    return this._extend(new ForeachBatchOperator(fn, opts))
  }

  // ----------------------------------------------------------------- Compile/run

  /**
   * Start the streaming runner. Returns a handle exposing:
   *   - stop(): graceful drain
   *   - metrics(): cycle/throughput/lag stats
   *
   * @param {object} runOptions
   * @param {string} runOptions.queryId - durable identity of this query
   * @param {number} [runOptions.batchSize=200] - messages per cycle
   * @param {number} [runOptions.maxPartitions=4] - lease up to N partitions/cycle
   * @param {number} [runOptions.maxWaitMillis=1000] - long-poll wait for source pop
   * @param {string} [runOptions.subscriptionMode] - 'new' | 'all'. Unset, the
   *   broker's default applies, which is 'new' (DEFAULT_SUBSCRIPTION_MODE): a
   *   query registered after its source already holds messages starts at the
   *   tail and skips them. Pass 'all' to process the backlog.
   * @param {string} [runOptions.subscriptionFrom] - ISO timestamp or 'now'
   * @param {boolean} [runOptions.conflation=false] - last-value delivery on the
   *   source pop: each cycle sees only the newest visible message per partition
   *   (PLAN_CONFLATION §1.1). Requires broker >= 1.1.0
   * @param {boolean} [runOptions.reset=false] - wipe state on config_hash mismatch
   * @param {(err:Error, ctx:object)=>void} [runOptions.onError] - cycle error hook
   * @param {AbortSignal} [runOptions.abortSignal] - external cancellation
   */
  async run(runOptions) {
    if (!runOptions || !runOptions.queryId) {
      throw new Error('run({ queryId }) is required')
    }
    if (this._source && this._source.kind === 'feed') {
      const compiled = this._compileFeed()
      const shards = runOptions.shards
      if (shards !== undefined && shards !== null && shards !== 1) {
        const group = new FeedShardGroup({ ...runOptions, stream: compiled })
        await group.start()
        return group
      }
      const { shards: _ignored, ...opts } = runOptions
      const feedRunner = new FeedRunner({ ...opts, stream: compiled })
      await feedRunner.start()
      return feedRunner
    }
    if (this._operators.some((op) => op.kind === 'foreachBatch')) {
      throw new Error('.foreachBatch() runs on Stream.fromFeed only; on a queue source use .foreach(), or .to() a queue that a worker pops in batches')
    }
    const feedOnly = this._operators.find((op) => FEED_ONLY.has(op.kind))
    if (feedOnly) {
      throw new Error('.pattern(), .join(), .gather(), .joinTable() and .name() run on Stream.fromFeed only: they need the clock every queue shares')
    }
    if (this._operators.some((op) => op.kind === 'window' && op.idleMs !== null && op.idleMs !== undefined)) {
      throw new Error("a window's idle runs on Stream.fromFeed only (on a queue source, idleFlushMs closes windows on quiet partitions)")
    }
    const compiled = this._compile()
    if (compiled.stages.sink && compiled.stages.sink.kind === 'sink' && compiled.stages.sink.routes) {
      throw new Error('.to() with several queues or a function runs on Stream.fromFeed only; a queue source sinks to one queue')
    }
    const runner = new Runner({
      ...runOptions,
      stream: compiled
    })
    await runner.start()
    return runner
  }

  // ===== internals =========================================================

  /** @returns {Stream} */
  _extend(operator) {
    return new Stream({
      source: this._source,
      sourceOptions: this._sourceOptions,
      operators: [...this._operators, operator]
    })
  }

  /**
   * Compile a chain on a feed into steps: stateless operators, and the
   * stateful steps in chain order (a window and its reduce/aggregate are one
   * step). `where` before the first stateful step goes to the broker.
   * `chainHash` is made from the stateful steps and the keyBys around them:
   * a different one finds the stored state was kept by another chain.
   */
  _compileFeed() {
    const ops = this._operators
    const sinkIdx = ops.findIndex((op) => op.kind === 'sink' || op.kind === 'foreach' || op.kind === 'foreachBatch')
    if (sinkIdx !== -1 && sinkIdx !== ops.length - 1) {
      throw new Error(`sink operators (.to / .foreach / .foreachBatch) must be the last in the chain; found ${ops[sinkIdx].kind} at position ${sinkIdx} of ${ops.length}`)
    }
    const sink = sinkIdx === -1 ? null : ops[sinkIdx]
    const upstream = sinkIdx === -1 ? ops : ops.slice(0, sinkIdx)
    const steps = []
    const pushdown = []
    let pending = []
    let window = null
    let stateful = 0
    const flush = () => {
      if (pending.length) steps.push({ kind: 'ops', ops: pending })
      pending = []
    }
    const named = (op) => op.stepName || `step${stateful}`
    for (const op of upstream) {
      if (window && op.kind !== 'reduce' && op.kind !== 'aggregate') {
        throw new Error('Stream.fromFeed: a window needs .reduce() or .aggregate() right after it')
      }
      switch (op.kind) {
        case 'filter':
          // Before any stateful step a where tests the source message: the broker does it.
          if (op.where && stateful === 0) pushdown.push(op.where)
          else pending.push(op)
          break
        case 'map':
        case 'flatMap':
        case 'keyBy':
          pending.push(op)
          break
        case 'window':
          flush()
          window = op
          break
        case 'reduce':
        case 'aggregate':
          if (!window) throw new Error('reduce/aggregate requires a preceding window operator')
          stateful++
          steps.push({ kind: 'window', window, reducer: op, name: `step${stateful}` })
          window = null
          break
        case 'pattern':
          flush()
          stateful++
          steps.push({ kind: 'pattern', op, name: named(op) })
          break
        case 'collect':
          flush()
          stateful++
          steps.push({ kind: 'collect', op, name: named(op) })
          break
        case 'table':
          flush()
          stateful++
          steps.push({ kind: 'table', op, name: named(op) })
          break
        case 'name': {
          const last = [...steps].reverse().find((st) => st.kind !== 'ops')
          if (!last || pending.length) throw new Error('.name() names the stateful step right before it')
          last.name = op.name
          break
        }
        case 'gate':
          throw new Error('Stream.fromFeed: .gate() runs on queue sources only — it holds a partition\'s lease, and a feed has none')
        default:
          throw new Error(`unsupported operator: ${op.kind}`)
      }
    }
    if (window) throw new Error('Stream.fromFeed: a window needs .reduce() or .aggregate() after it')
    flush()
    const names = steps.filter((st) => st.kind !== 'ops').map((st) => st.name)
    const dup = names.find((n, i) => names.indexOf(n) !== i)
    if (dup) throw new Error(`Stream.fromFeed: two stateful steps are named ${JSON.stringify(dup)}`)
    // The clock each stateful step runs on, as the pipeline resolves it (feed/pipeline.js): a step
    // that takes the event time of the step before says so in the hash, since its state is kept on
    // that time (an eventTime of its own, or false, is in its config or changes nothing).
    const inheriting = new Set()
    let onEventTime = false
    for (const st of steps) {
      if (st.kind === 'ops' || st.kind === 'table') continue
      const op = st.kind === 'window' ? st.window : st.op
      if (typeof op.eventTimeFn === 'function') onEventTime = true
      else if (onEventTime && !op.eventTimeOff) inheriting.add(op)
      else onEventTime = false
    }
    const shape = upstream
      .filter((op) => op.kind !== 'map' && op.kind !== 'filter' && op.kind !== 'flatMap')
      .map((op) => (inheriting.has(op) ? { kind: op.kind, config: { ...op.config, clock: 'event' } } : op))
    return {
      source: this._source,
      sourceOptions: this._sourceOptions,
      steps,
      pushdown,
      sink,
      operators: ops,
      chainHash: configHashOf(shape).slice(0, 16),
      config_hash: configHashOf(ops)
    }
  }

  /**
   * Compile the operator chain into a runnable description: stages, sink,
   * and the configHash. Exposed for the Runner.
   */
  _compile() {
    const sink = this._operators.find(op => op.kind === 'sink' || op.kind === 'foreach')
    const sinkIdx = sink ? this._operators.indexOf(sink) : this._operators.length

    // Verify the sink (if present) is the last operator.
    if (sink && sinkIdx !== this._operators.length - 1) {
      throw new Error(
        'sink operators (.to / .foreach) must be the last in the chain; ' +
        `found ${sink.kind} at position ${sinkIdx} of ${this._operators.length}`
      )
    }

    const upstream = sink ? this._operators.slice(0, sinkIdx) : this._operators

    // Decompose into stages:
    //   pre:    [stateless...] applied to each source record
    //   keyBy:  optional KeyByOperator (operates on source record)
    //   window: optional WindowTumblingOperator (annotates envelopes)
    //   reducer: optional ReduceOperator/AggregateOperator
    //   post:   [stateless...] applied to each emit value (post-reducer)
    //   sink:   optional SinkOperator/ForeachOperator (terminal)
    //
    // "phase" walks from `pre` -> `keyed` -> `window` -> `reducer` -> `post`.
    // Stateless operators belong to whichever side of the reducer they're
    // declared on. A reducer is required to switch to `post`; without one,
    // all stateless ops are pre-stage.
    const stages = {
      pre: [],
      keyBy: null,
      window: null,
      reducer: null,
      gate: null,
      pattern: null,
      post: [],
      sink
    }
    let phase = 'pre'
    for (const op of upstream) {
      if (op.kind === 'map' || op.kind === 'filter' || op.kind === 'flatMap') {
        if (phase === 'pre' || phase === 'keyed') {
          stages.pre.push(op)
        } else if (phase === 'window') {
          // Stateless ops between window and reducer are unusual; treat as pre
          // (they see the windowed envelope but have no reducer state to act on).
          stages.pre.push(op)
        } else {
          // post-reducer, post-gate or post-pattern: operates on each emit value.
          stages.post.push(op)
        }
      } else if (op.kind === 'keyBy') {
        if (stages.keyBy) throw new Error('only one .keyBy() per stream')
        if (phase === 'reducer' || phase === 'gate' || phase === 'pattern') {
          throw new Error('.keyBy() must come before window/reduce/gate/pattern')
        }
        stages.keyBy = op
        if (phase === 'pre') phase = 'keyed'
      } else if (op.kind === 'pattern') {
        if (stages.pattern) throw new Error('only one .pattern() or .join() per stream')
        if (stages.window || stages.reducer || stages.gate) {
          throw new Error('.pattern()/.join() do not combine with a window, reduce or gate in one stream; run two streams')
        }
        stages.pattern = op
        phase = 'pattern'
      } else if (op.kind === 'window') {
        if (stages.window) throw new Error('only one window operator per stream')
        if (stages.pattern) throw new Error('a window does not combine with .pattern()/.join() in one stream')
        if (phase === 'reducer') throw new Error('window must come before reduce/aggregate')
        if (stages.gate) throw new Error('window+reduce is incompatible with .gate() in the same stream')
        stages.window = op
        phase = 'window'
      } else if (op.kind === 'reduce' || op.kind === 'aggregate') {
        if (stages.reducer) throw new Error('only one reduce/aggregate per stream')
        if (!stages.window) {
          throw new Error('reduce/aggregate requires a preceding window operator')
        }
        if (stages.gate) throw new Error('reduce/aggregate is incompatible with .gate() in the same stream')
        stages.reducer = op
        phase = 'reducer'
      } else if (op.kind === 'gate') {
        if (stages.gate) throw new Error('only one .gate() per stream')
        if (stages.window || stages.reducer) {
          throw new Error('.gate() is incompatible with windowing/reduce in the same stream')
        }
        if (stages.pattern) throw new Error('.gate() does not combine with .pattern()/.join() in one stream')
        stages.gate = op
        phase = 'gate'
      } else {
        throw new Error(`unsupported operator: ${op.kind}`)
      }
    }
    // Backwards-compat: keep the old name `stateless` aliasing `pre` so any
    // existing code paths that referenced stages.stateless keep working.
    stages.stateless = stages.pre

    // Compute config_hash from the chain shape (operator kinds + their
    // structural config; user functions are intentionally not hashed since
    // we cannot stably serialise closures).
    const config_hash = configHashOf(this._operators)

    return {
      source: this._source,
      sourceOptions: this._sourceOptions,
      stages,
      operators: this._operators,
      config_hash
    }
  }
}
