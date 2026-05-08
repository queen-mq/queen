/**
 * WindowTumblingOperator — bucket envelopes by floor(time, N).
 *
 * The "time" used for bucketing is either:
 *   - the source message's `createdAt` (Queen-stamped) — processing-time mode
 *     when no `eventTime` extractor is provided
 *   - the value returned by `opts.eventTime(msg)` — event-time mode when
 *     the extractor is provided. Out-of-order events update the
 *     per-partition watermark; events older than `watermark - allowedLateness`
 *     are routed per `onLate` policy.
 *
 * Operator config (all optional except `seconds`):
 *
 *   seconds         (req) window size in seconds
 *   gracePeriod     (sec) accept events for windowEnd + grace before closing (default 0)
 *   idleFlushMs     (ms)  Runner closes ripe windows on quiet partitions every
 *                         this many ms. Default 5000. Pass 0 to disable.
 *   eventTime       (fn)  msg => epoch_ms; if set, switches to event-time mode
 *   allowedLateness (sec) drop events older than wm - allowedLateness (default 0)
 *   onLate          (str) 'drop' (default) | 'include' — what to do with
 *                         events older than the watermark
 *
 * The operator does NOT itself emit records during apply — it only annotates
 * each envelope with `windowStart`, `windowEnd`, `windowKey`, plus the
 * operator's `operatorTag`/`gracePeriodMs` so the downstream reducer can
 * apply the right close trigger.
 *
 * Window-close trigger (handled by ReduceOperator using these annotations):
 *   processing-time: windowEnd + grace <= max(createdAt) seen in batch
 *                                          OR latest streamTime fed by Runner
 *   event-time:      windowEnd + grace <= partition watermark
 *   idle flush:      windowEnd + grace <= Date.now() (per Runner timer)
 */

import { extractEventTime, normaliseLateOpts, MS_PER_SEC } from './_windowCommon.js'

export class WindowTumblingOperator {
  constructor({
    seconds,
    gracePeriod   = 0,
    idleFlushMs   = 5000,
    eventTime,
    allowedLateness = 0,
    onLate
  } = {}) {
    if (typeof seconds !== 'number' || seconds <= 0) {
      throw new Error('windowTumbling requires { seconds: <positive number> }')
    }
    this.kind             = 'window'
    this.windowKind       = 'tumbling'
    this.windowMs         = seconds * MS_PER_SEC
    this.gracePeriodMs    = Math.max(0, Math.floor(gracePeriod * MS_PER_SEC))
    this.idleFlushMs      = Math.max(0, Math.floor(idleFlushMs))
    this.eventTimeFn      = typeof eventTime === 'function' ? eventTime : null
    this.allowedLatenessMs = Math.max(0, Math.floor(allowedLateness * MS_PER_SEC))
    this.onLatePolicy     = normaliseLateOpts(onLate)
    this.operatorTag      = `tumb:${seconds}`
    this.config           = {
      kind:             'window-tumbling',
      seconds,
      gracePeriod,
      idleFlushMs:      this.idleFlushMs,
      eventTime:        !!this.eventTimeFn,
      allowedLateness,
      onLate:           this.onLatePolicy
    }
  }

  /**
   * Annotate each input envelope with the tumbling-window slot it falls into.
   * Returns an array (1 element for tumbling, vs N for sliding).
   *
   * In event-time mode, returns the empty array if the event is "late" and
   * `onLate` is 'drop'. The Runner is responsible for tracking this against
   * the per-partition watermark and incrementing the late-events counter.
   */
  async apply(envelope) {
    const ts = extractEventTime(envelope.msg, this.eventTimeFn)
    if (ts === null) {
      throw new Error('windowTumbling could not determine timestamp for message')
    }
    const windowStart = Math.floor(ts / this.windowMs) * this.windowMs
    const windowEnd   = windowStart + this.windowMs
    return [{
      ...envelope,
      eventTimeMs:    ts,
      windowStart,
      windowEnd,
      windowKey:      new Date(windowStart).toISOString(),
      operatorTag:    this.operatorTag,
      gracePeriodMs:  this.gracePeriodMs
    }]
  }
}
