/**
 * Shared helpers for window operators.
 */

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
    if (raw instanceof Date) return raw.getTime()
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
