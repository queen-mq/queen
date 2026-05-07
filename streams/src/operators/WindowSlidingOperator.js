/**
 * WindowSlidingOperator — overlapping fixed-size windows that slide every
 * `slide` seconds. An event with timestamp T belongs to all windows whose
 * range covers T:
 *
 *   for each i in 0..(size/slide - 1):
 *     windowStart = floor((T - i*slide) / slide) * slide
 *     ...
 *
 * Equivalent way to enumerate: every window [s, s+size) where
 *   s = floor(T/slide)*slide - i*slide  for i = 0..size/slide-1
 *
 * Storage cost: each event creates `size/slide` state rows (per key).
 * Worth documenting in the README — for a 1h window sliding every 1m,
 * that's 60 state rows per event. Fine in absolute terms; can become
 * expensive at very high cardinalities.
 *
 * Config (all optional except `size` and `slide`):
 *   size            (sec) window length
 *   slide           (sec) hop interval; must divide `size` for clean alignment
 *                          (we enforce size % slide == 0 to keep window count finite)
 *   gracePeriod     (sec) accept events for windowEnd + grace before closing
 *   idleFlushMs     (ms)  ripe-window flush cadence; default 5000; 0 = disable
 *   eventTime, allowedLateness, onLate — same semantics as WindowTumblingOperator
 */

import { extractEventTime, normaliseLateOpts, MS_PER_SEC } from './_windowCommon.js'

export class WindowSlidingOperator {
  constructor({
    size,
    slide,
    gracePeriod   = 0,
    idleFlushMs   = 5000,
    eventTime,
    allowedLateness = 0,
    onLate
  } = {}) {
    if (typeof size !== 'number' || size <= 0) {
      throw new Error('windowSliding requires { size: <positive number> }')
    }
    if (typeof slide !== 'number' || slide <= 0) {
      throw new Error('windowSliding requires { slide: <positive number> }')
    }
    if (size % slide !== 0) {
      throw new Error(
        `windowSliding: size (${size}) must be an integer multiple of slide (${slide}) ` +
        'to keep the per-event window count finite'
      )
    }
    this.kind             = 'window'
    this.windowKind       = 'sliding'
    this.sizeMs           = size * MS_PER_SEC
    this.slideMs          = slide * MS_PER_SEC
    this.windowsPerEvent  = size / slide
    this.gracePeriodMs    = Math.max(0, Math.floor(gracePeriod * MS_PER_SEC))
    this.idleFlushMs      = Math.max(0, Math.floor(idleFlushMs))
    this.eventTimeFn      = typeof eventTime === 'function' ? eventTime : null
    this.allowedLatenessMs = Math.max(0, Math.floor(allowedLateness * MS_PER_SEC))
    this.onLatePolicy     = normaliseLateOpts(onLate)
    this.operatorTag      = `slide:${size}:${slide}`
    this.config           = {
      kind: 'window-sliding',
      size,
      slide,
      gracePeriod,
      idleFlushMs:      this.idleFlushMs,
      eventTime:        !!this.eventTimeFn,
      allowedLateness,
      onLate:           this.onLatePolicy
    }
  }

  /**
   * Annotate the envelope with ALL windows it belongs to. Returns
   * `windowsPerEvent` envelopes (default 6 for size=60, slide=10).
   */
  async apply(envelope) {
    const ts = extractEventTime(envelope.msg, this.eventTimeFn)
    if (ts === null) {
      throw new Error('windowSliding could not determine timestamp for message')
    }
    // Latest slide boundary at or before ts.
    const latestStart = Math.floor(ts / this.slideMs) * this.slideMs
    const out = []
    for (let i = 0; i < this.windowsPerEvent; i++) {
      const windowStart = latestStart - i * this.slideMs
      // Skip windows that don't actually contain ts (only relevant at the
      // very edges, e.g. when slide doesn't evenly tile time but size does).
      if (ts < windowStart || ts >= windowStart + this.sizeMs) continue
      out.push({
        ...envelope,
        eventTimeMs:    ts,
        windowStart,
        windowEnd:      windowStart + this.sizeMs,
        windowKey:      new Date(windowStart).toISOString(),
        operatorTag:    this.operatorTag,
        gracePeriodMs:  this.gracePeriodMs
      })
    }
    return out
  }
}
