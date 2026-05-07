/**
 * WindowCronOperator — wall-clock-aligned tumbling windows.
 *
 * Bucket events by absolute UTC boundaries (minute-on-the-minute,
 * hour-on-the-hour, day at 00:00 UTC, week starting Mon 00:00 UTC).
 * Equivalent to WindowTumblingOperator with a fixed size, but the boundary
 * is anchored to wall-clock instead of relative-to-epoch arithmetic.
 *
 * v0.2 ships only the `every:` shorthand; full cron expressions
 * (`'*\/5 * * * *'`) are reserved for v0.3 and would add a dep.
 *
 * Config:
 *   every           (str) 'second'|'minute'|'hour'|'day'|'week' (required)
 *   gracePeriod     (sec) accept events for windowEnd + grace before closing
 *   idleFlushMs     (ms)  ripe-window flush cadence; default 30_000
 *   eventTime, allowedLateness, onLate — same as the other window operators
 */

import { extractEventTime, normaliseLateOpts, everyToMs, MS_PER_SEC } from './_windowCommon.js'

const WEEK_EPOCH_OFFSET_MS = 4 * 86_400 * 1000  // Jan 1 1970 UTC was Thursday;
// shift so weeks align to Monday 00:00 UTC

export class WindowCronOperator {
  constructor({
    every,
    gracePeriod   = 0,
    idleFlushMs   = 30_000,
    eventTime,
    allowedLateness = 0,
    onLate
  } = {}) {
    if (typeof every !== 'string') {
      throw new Error("windowCron requires { every: 'second'|'minute'|'hour'|'day'|'week' }")
    }
    this.kind             = 'window'
    this.windowKind       = 'cron'
    this.every            = every
    this.windowMs         = everyToMs(every)
    this.gracePeriodMs    = Math.max(0, Math.floor(gracePeriod * MS_PER_SEC))
    this.idleFlushMs      = Math.max(0, Math.floor(idleFlushMs))
    this.eventTimeFn      = typeof eventTime === 'function' ? eventTime : null
    this.allowedLatenessMs = Math.max(0, Math.floor(allowedLateness * MS_PER_SEC))
    this.onLatePolicy     = normaliseLateOpts(onLate)
    this.operatorTag      = `cron:${every}`
    this.config           = {
      kind: 'window-cron',
      every,
      gracePeriod,
      idleFlushMs:      this.idleFlushMs,
      eventTime:        !!this.eventTimeFn,
      allowedLateness,
      onLate:           this.onLatePolicy
    }
  }

  async apply(envelope) {
    const ts = extractEventTime(envelope.msg, this.eventTimeFn)
    if (ts === null) {
      throw new Error('windowCron could not determine timestamp for message')
    }
    const windowStart = this._floorToBoundary(ts)
    return [{
      ...envelope,
      eventTimeMs:    ts,
      windowStart,
      windowEnd:      windowStart + this.windowMs,
      windowKey:      new Date(windowStart).toISOString(),
      operatorTag:    this.operatorTag,
      gracePeriodMs:  this.gracePeriodMs
    }]
  }

  _floorToBoundary(ts) {
    if (this.every === 'week') {
      // Align to Monday 00:00 UTC.
      const shifted = ts - WEEK_EPOCH_OFFSET_MS
      const week    = Math.floor(shifted / this.windowMs)
      return week * this.windowMs + WEEK_EPOCH_OFFSET_MS
    }
    return Math.floor(ts / this.windowMs) * this.windowMs
  }
}
