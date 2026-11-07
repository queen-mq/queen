/**
 * Shared helpers for window operators.
 */
import { parseDurationMs } from '../../kv/expiry.js'

export const MS_PER_SEC = 1000

/**
 * Extract the timestamp (epoch ms) for an envelope's message.
 *
 * Processing-time mode (no extractor): uses `msg.createdAt` (Queen-stamped).
 * Event-time mode: calls the extractor with the original message; the
 * extractor must return a Date, a number (ms epoch), or an ISO string.
 *
 * Returns null when the timestamp can't be determined; the operator's
 * apply() should treat null as a hard error (no fallback).
 */
export function extractEventTime(msg, eventTimeFn) {
  if (typeof eventTimeFn === 'function') {
    let raw
    try {
      raw = eventTimeFn(msg)
    } catch (err) {
      // Bubble up so the runner can decide policy.
      const e = new Error('eventTime extractor threw: ' + (err && err.message ? err.message : String(err)))
      e.cause = err
      throw e
    }
    if (raw == null) return null
    if (raw instanceof Date) {
      // An Invalid Date (new Date(undefined)) is no time: the message meets the poison policy.
      const t = raw.getTime()
      return Number.isFinite(t) ? t : null
    }
    if (typeof raw === 'number' && Number.isFinite(raw)) return raw
    if (typeof raw === 'string') {
      const t = Date.parse(raw)
      if (!Number.isNaN(t)) return t
    }
    return null
  }
  // Processing-time fallback.
  if (msg && msg.createdAt) {
    const t = Date.parse(msg.createdAt)
    if (!Number.isNaN(t)) return t
  }
  return null
}

/**
 * Validate / normalise the onLate policy for event-time windows.
 *
 *   'drop'    — silently drop the late event
 *   'include' — accumulate it into the (probably-already-closed) window
 *               anyway. Best-effort: if the window has already been
 *               flushed, this still updates the now-recreated state row,
 *               leading to a duplicate emit on next close.
 *
 * Sink-routing for late events ("send to a 'late' queue") is reserved for
 * a future v0.x; if the user passes anything else we throw early.
 */
export function normaliseLateOpts(onLate) {
  if (onLate === undefined || onLate === null || onLate === 'drop') return 'drop'
  if (onLate === 'include') return 'include'
  throw new Error(`onLate must be 'drop' or 'include' (got ${JSON.stringify(onLate)})`)
}

/**
 * Helper: parse an `every: 'minute'|'hour'|'day'|'week'` shorthand into ms.
 * Used by WindowCronOperator. Day/week are at UTC boundaries.
 */
export function everyToMs(every) {
  switch (every) {
    case 'second': return 1000
    case 'minute': return 60 * 1000
    case 'hour':   return 3600 * 1000
    case 'day':    return 86_400 * 1000
    case 'week':   return 7 * 86_400 * 1000
    default:
      throw new Error(`windowCron: every must be one of second|minute|hour|day|week (got ${JSON.stringify(every)})`)
  }
}

/**
 * A duration option: a number of seconds, or a string with a unit ('30s',
 * '5m', '1h30m'). Returns milliseconds, or null when the option is not given.
 */
export function durationMs(value, name, what) {
  if (value === undefined || value === null) return null
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value < 0) throw new Error(`${what}: ${name} is a number of seconds from 0, or a duration string such as '30s'`)
    return Math.floor(value * MS_PER_SEC)
  }
  if (typeof value === 'string') return parseDurationMs(value)
  throw new Error(`${what}: ${name} is a number of seconds, or a duration string such as '30s'`)
}

/**
 * A window's `allowedLateness`: seconds (as before), or a duration string
 * like a pattern's ('5m'). Milliseconds, never negative.
 */
export function windowLatenessMs(value, what) {
  if (value === undefined || value === null) return 0
  if (typeof value === 'string') return parseDurationMs(value)
  const n = Number(value)
  if (!Number.isFinite(n)) throw new Error(`${what}: allowedLateness is a number of seconds, or a duration string such as '5m'`)
  return Math.max(0, Math.floor(n * MS_PER_SEC))
}

/**
 * The feed's event-time options of a window: `eventTime: false` (the feed's
 * clock, even after a step on event time) and `idle` (a quiet feed moves the
 * watermark on). `idle` goes in the config when given; `eventTime: false`
 * does not (the chain hash says which clock each step runs on), so the shape
 * of a window that does not use them, and its hash, stay as they were.
 */
export function feedTimeOptions(op, { eventTime, idle }, what) {
  op.eventTimeOff = eventTime === false
  op.idleMs = durationMs(idle, 'idle', what)
  if (op.idleMs !== null && op.idleMs <= 0) throw new Error(`${what}: idle is a duration above zero`)
  if (op.idleMs !== null) op.config.idle = op.idleMs
}

/**
 * The event-time options of a pattern, a join or a gather (feed sources):
 *
 *   eventTime        (msg) => time: the step runs on event time (a Date, epoch
 *                    milliseconds or an ISO string); false: the feed's clock,
 *                    even after a step on event time
 *   allowedLateness  how far out of order messages may come (seconds, or a
 *                    duration string): each waits that long, then the step
 *                    takes them in event-time order (default 0)
 *   onLate           'drop' (default) | 'include': a message older than the
 *                    watermark is left out, or taken at once, out of order
 *   idle             after this long without a message, the watermark moves
 *                    on with the feed's clock
 *
 * Added to the config only when given.
 */
export function stepTimeOptions(op, { eventTime, allowedLateness, onLate, idle }, what) {
  if (eventTime !== undefined && eventTime !== null && eventTime !== false && typeof eventTime !== 'function') {
    throw new Error(`${what}: eventTime is a function (msg) => time, or false for the feed's clock`)
  }
  op.eventTimeFn = typeof eventTime === 'function' ? eventTime : null
  op.eventTimeOff = eventTime === false
  op.allowedLatenessMs = durationMs(allowedLateness, 'allowedLateness', what) ?? 0
  op.latenessGiven = allowedLateness !== undefined && allowedLateness !== null
  op.onLatePolicy = normaliseLateOpts(onLate)
  op.onLateGiven = onLate !== undefined && onLate !== null
  op.idleMs = durationMs(idle, 'idle', what)
  if (op.idleMs !== null && op.idleMs <= 0) throw new Error(`${what}: idle is a duration above zero`)
  if (op.eventTimeFn) op.config.eventTime = true
  if (op.latenessGiven) op.config.allowedLateness = op.allowedLatenessMs
  if (op.onLateGiven) op.config.onLate = op.onLatePolicy
  if (op.idleMs !== null) op.config.idle = op.idleMs
}
