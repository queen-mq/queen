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
import { SinkOperator } from './operators/SinkOperator.js'
import { ForeachOperator } from './operators/ForeachOperator.js'
import { Runner } from './runtime/Runner.js'
import { configHashOf } from './util/configHash.js'

export class Stream {
  /**
   * Build a Stream sourced from a Queen QueueBuilder.
   *
   * @param {object} queueBuilder - the result of queen.queue('name') — must
   *   expose .pop() / .ack() / .push() and the queue name.
   * @param {object} [options]
   * @returns {Stream}
   */
  static from(queueBuilder, options = {}) {
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
   *   eventTime?: (msg:any)=>(number|Date|string),  // event-time mode if set
   *   allowedLateness?: number, // event-time only: drop events older than wm - allowedLateness
   *   onLate?: 'drop' | 'include'
   * }} opts
   */
  windowTumbling(opts) {
    return this._extend(new WindowTumblingOperator(opts))
  }

  /**
   * Overlapping fixed-size windows that hop every `slide` seconds.
   * Each event creates `size/slide` state rows per key.
   * @param {{size:number, slide:number, gracePeriod?:number, idleFlushMs?:number,
   *          eventTime?:(msg:any)=>any, allowedLateness?:number, onLate?:string}} opts
   */
  windowSliding(opts) {
    return this._extend(new WindowSlidingOperator(opts))
  }

  /**
   * Per-key activity-based windows. A session for a key extends as long
   * as events keep arriving within `gap` seconds of each other.
   * @param {{gap:number, gracePeriod?:number, idleFlushMs?:number,
   *          eventTime?:(msg:any)=>any, allowedLateness?:number, onLate?:string}} opts
   */
  windowSession(opts) {
    return this._extend(new WindowSessionOperator(opts))
  }

  /**
   * Wall-clock-aligned windows. Currently supports the `every:` shorthand
   * (`'second'|'minute'|'hour'|'day'|'week'`); full cron syntax is reserved
   * for v0.3.
   * @param {{every:string, gracePeriod?:number, idleFlushMs?:number,
   *          eventTime?:(msg:any)=>any, allowedLateness?:number, onLate?:string}} opts
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
   *   .aggregate({ count: () => 1, sum: m => m.data.amount })
   *
   * Output value shape: { count: number, sum: number, ... }
   *
   * @param {Record<string,(msg:object)=>number>} extractors
   */
  aggregate(extractors) {
    return this._extend(new AggregateOperator(extractors))
  }

  // ---------------------------------------------------------------------- Sink

  /**
   * Sink to a Queen queue. The cycle commits state + push + ack atomically.
   *
   * @param {object} sinkQueueBuilder - `queen.queue('sink-name')`
   * @param {{partition?: string|((value:any)=>string)}} [opts]
   */
  to(sinkQueueBuilder, opts = {}) {
    return this._extend(new SinkOperator(sinkQueueBuilder, opts))
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
   * @param {string} [runOptions.subscriptionMode] - 'all' (default) | 'new'
   * @param {string} [runOptions.subscriptionFrom] - ISO timestamp or 'now'
   * @param {boolean} [runOptions.reset=false] - wipe state on config_hash mismatch
   * @param {(err:Error, ctx:object)=>void} [runOptions.onError] - cycle error hook
   * @param {AbortSignal} [runOptions.abortSignal] - external cancellation
   */
  async run(runOptions) {
    if (!runOptions || !runOptions.queryId) {
      throw new Error('run({ queryId }) is required')
    }
    const compiled = this._compile()
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
          // post-reducer: operates on each emit value.
          stages.post.push(op)
        }
      } else if (op.kind === 'keyBy') {
        if (stages.keyBy) throw new Error('only one .keyBy() per stream')
        if (phase === 'reducer') throw new Error('.keyBy() must come before window/reduce')
        stages.keyBy = op
        if (phase === 'pre') phase = 'keyed'
      } else if (op.kind === 'window') {
        if (stages.window) throw new Error('only one window operator per stream')
        if (phase === 'reducer') throw new Error('window must come before reduce/aggregate')
        stages.window = op
        phase = 'window'
      } else if (op.kind === 'reduce' || op.kind === 'aggregate') {
        if (stages.reducer) throw new Error('only one reduce/aggregate per stream')
        if (!stages.window) {
          throw new Error('reduce/aggregate requires a preceding window operator')
        }
        stages.reducer = op
        phase = 'reducer'
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
