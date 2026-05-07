/**
 * WindowSessionOperator — per-key activity-based windows.
 *
 * A session window for a key starts on the first event for that key and
 * extends as long as events keep arriving within `gap` seconds of each
 * other. The session closes after `gap` seconds of inactivity for that key
 * (detected either by a new event arriving past the gap, or by the
 * idle-flush timer at `windowStart + lastEventLag + gap <= now`).
 *
 * Unlike tumbling/sliding/cron, session boundaries are NOT computable
 * stateless from a single message — they depend on what came before.
 * Therefore session windowing has its own code path in the Runner that
 * loads each key's open session, decides whether each event extends or
 * closes-and-restarts, and emits closed sessions.
 *
 * Config:
 *   gap             (sec) silence threshold; required
 *   gracePeriod     (sec) accept events for sessionEnd + grace before closing
 *   idleFlushMs     (ms)  ripe-window flush cadence; default 1000 (sessions
 *                          are inherently more lateness-sensitive than tumbling)
 *   eventTime, allowedLateness, onLate — same as the other window operators
 *
 * State shape (per key, scoped to (query_id, partition_id, operatorTag)):
 *   key:   `${operatorTag}\u001fopen\u001f${userKey}`
 *   value: { acc, sessionStart, sessionEnd, lastEventTime }
 *
 * Where `sessionEnd = lastEventTime + gap` is the wall-clock time at which
 * the session is considered closed if no further events arrive.
 *
 * The "windowKey" of an emitted session is its sessionStart timestamp
 * (ISO string). sessionEnd is included on the emit envelope as windowEnd.
 */

import { extractEventTime, normaliseLateOpts, MS_PER_SEC } from './_windowCommon.js'

export class WindowSessionOperator {
  constructor({
    gap,
    gracePeriod   = 0,
    idleFlushMs   = 1000,
    eventTime,
    allowedLateness = 0,
    onLate
  } = {}) {
    if (typeof gap !== 'number' || gap <= 0) {
      throw new Error('windowSession requires { gap: <positive number> }')
    }
    this.kind             = 'window'
    this.windowKind       = 'session'
    this.gapMs            = gap * MS_PER_SEC
    this.gracePeriodMs    = Math.max(0, Math.floor(gracePeriod * MS_PER_SEC))
    this.idleFlushMs      = Math.max(0, Math.floor(idleFlushMs))
    this.eventTimeFn      = typeof eventTime === 'function' ? eventTime : null
    this.allowedLatenessMs = Math.max(0, Math.floor(allowedLateness * MS_PER_SEC))
    this.onLatePolicy     = normaliseLateOpts(onLate)
    this.operatorTag      = `sess:${gap}`
    this.config           = {
      kind: 'window-session',
      gap,
      gracePeriod,
      idleFlushMs:      this.idleFlushMs,
      eventTime:        !!this.eventTimeFn,
      allowedLateness,
      onLate:           this.onLatePolicy
    }
  }

  /**
   * Build the state key for a per-key open session.
   */
  openSessionStateKey(userKey) {
    return `${this.operatorTag}\u001fopen\u001f${userKey}`
  }

  /**
   * Annotate the envelope with the timestamp; actual session-window
   * decisions are made in the Runner's session path (since they depend
   * on prior state). The annotated envelope still carries the operator's
   * fields so the dispatch is uniform.
   */
  async apply(envelope) {
    const ts = extractEventTime(envelope.msg, this.eventTimeFn)
    if (ts === null) {
      throw new Error('windowSession could not determine timestamp for message')
    }
    return [{
      ...envelope,
      eventTimeMs:   ts,
      operatorTag:   this.operatorTag,
      gracePeriodMs: this.gracePeriodMs
    }]
  }

  /**
   * Apply this batch's events to the per-key open-session state.
   * Returns state ops + emits, just like ReduceOperator.run().
   *
   * @param {{
   *   envelopes: Array<{key:string, eventTimeMs:number, value:any}>,
   *   loadedState: Map<string, any>,    // pre-filtered to this operator's keys
   *   reducerFn: (acc:any, value:any) => any | Promise<any>,
   *   reducerInitial: () => any,
   *   nowMs?: number                     // wall-clock for idle-flush; default Date.now()
   * }} ctx
   */
  async runSession({ envelopes, loadedState, reducerFn, reducerInitial, nowMs }) {
    // Per-userKey session state. We build up the working set from loaded
    // state (pre-filtered to our operatorTag's "open" keys) plus any new
    // events in this batch.
    const sessions = new Map()  // userKey -> { acc, sessionStart, lastEventTime, dirty, seeded }

    // Seed from PG state.
    for (const [stateKey, value] of loadedState.entries()) {
      // Expected shape: `${operatorTag}\u001fopen\u001f${userKey}`
      const parts = stateKey.split('\u001f')
      if (parts.length !== 3 || parts[0] !== this.operatorTag || parts[1] !== 'open') continue
      const userKey = parts[2]
      sessions.set(userKey, {
        acc:           value && value.acc !== undefined ? value.acc : reducerInitial(),
        sessionStart:  typeof value.sessionStart === 'number' ? value.sessionStart : null,
        lastEventTime: typeof value.lastEventTime === 'number' ? value.lastEventTime : null,
        dirty:         false,
        seeded:        true
      })
    }

    const closedEmits = []

    // Process events in order. Each event either extends the open session
    // or closes the prior one (emit + delete) and starts a new session.
    // We rely on the runner to have sorted envelopes by partition FIFO order
    // (which equals createdAt order in Queen). For event-time mode the
    // ordering may be different from event-time — we still process them
    // in arrival order for correctness against the lease.
    for (const env of envelopes) {
      const userKey = env.key
      const ts      = env.eventTimeMs
      let s = sessions.get(userKey)

      if (!s || s.lastEventTime === null) {
        // Brand-new (or empty) session.
        s = {
          acc:           reducerInitial(),
          sessionStart:  ts,
          lastEventTime: ts,
          dirty:         true,
          seeded:        s ? s.seeded : false
        }
        sessions.set(userKey, s)
      } else if (s.lastEventTime + this.gapMs >= ts) {
        // Extends prior session.
        if (ts < s.lastEventTime) {
          // Late event in event-time mode (still arrived in order at Queen
          // but its eventTime is older than what we've already seen).
          // For 'drop' policy in event-time, runner pre-filters; here
          // we accept and only update lastEventTime if newer.
        } else {
          s.lastEventTime = ts
        }
        s.dirty = true
      } else {
        // Gap exceeded: close prior session, start new.
        closedEmits.push(this._buildEmit(userKey, s))
        s = {
          acc:           reducerInitial(),
          sessionStart:  ts,
          lastEventTime: ts,
          dirty:         true,
          seeded:        false  // we're about to overwrite the seeded row
        }
        sessions.set(userKey, s)
      }

      s.acc = await reducerFn(s.acc, env.value !== undefined ? env.value : env.msg)
    }

    // Idle-flush sweep: any session whose lastEventTime + gap + grace is
    // already in the past (against the wall clock or the seen stream
    // time) is also closed.
    const flushReference = typeof nowMs === 'number' ? nowMs : Date.now()
    for (const [userKey, s] of sessions.entries()) {
      if (s.lastEventTime === null) continue
      const sessionEnd = s.lastEventTime + this.gapMs
      if (sessionEnd + this.gracePeriodMs <= flushReference) {
        closedEmits.push(this._buildEmit(userKey, s))
        s.closed = true
      }
    }

    // Build state ops.
    const stateOps = []
    for (const [userKey, s] of sessions.entries()) {
      const stateKey = this.openSessionStateKey(userKey)
      if (s.closed) {
        if (s.seeded) stateOps.push({ type: 'delete', key: stateKey })
        // Newly-opened sessions that closed in the same batch never made
        // it to PG, so no delete needed.
      } else if (s.dirty) {
        stateOps.push({
          type: 'upsert',
          key: stateKey,
          value: {
            acc:           s.acc,
            sessionStart:  s.sessionStart,
            lastEventTime: s.lastEventTime
          }
        })
      }
    }

    return { stateOps, emits: closedEmits }
  }

  _buildEmit(userKey, s) {
    return {
      key:          userKey,
      windowStart:  s.sessionStart,
      windowEnd:    s.lastEventTime + this.gapMs,
      windowKey:    new Date(s.sessionStart).toISOString(),
      value:        s.acc
    }
  }
}
