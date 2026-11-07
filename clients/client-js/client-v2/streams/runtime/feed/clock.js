/**
 * The clock of one stateful step of a feed stream: the feed's, or event time.
 *
 * The feed's clock (FeedClock) is the time the log committed each message.
 * Every queue shares it and it only moves forward, so a step on it closes a
 * window or passes a deadline once the clock is past it, and nothing that
 * belongs before can come after.
 *
 * Event time (EventClock) is a time the messages carry: `eventTime(msg)`, or,
 * after another step on event time, the time of that step's result (a
 * window's last millisecond, a deadline, the message that completed a match).
 * Messages may come out of that order, so the step keeps a WATERMARK: the
 * latest event time seen, minus `allowedLateness`. A window closes, and a
 * deadline passes, once the watermark is past it; a message older than the
 * watermark is late (dropped and counted, or taken anyway with `onLate:
 * 'include'`). Only the messages that concern a pattern, a join or a gather
 * (its tests say so) count: the others need no time and move nothing.
 *
 * In order. Patterns, joins, gathers and sessions depend on the order of
 * their messages ("A then B", which part came first, the gap between two).
 * On event time such a step holds each message that concerns it until the
 * watermark reaches the message's time, then takes them in event-time order,
 * ties in the order they came: the step sees one ordered stream, as on the
 * feed's clock. A message out of order by less than `allowedLateness` takes
 * its place; the cost is that much of the stream in memory. Tumbling, sliding
 * and cron windows only add up, so they take each message as it comes.
 *
 * Idle. The watermark moves only with messages. With `idle`, once the step
 * has had no message for that long on the feed's clock, its watermark moves
 * on as the feed's clock does: a quiet feed closes its windows and passes its
 * deadlines, a finished import too (at the pace of the feed's clock). The
 * rule depends only on the feed's clock and the messages, so a replay closes
 * the same things at the same times.
 *
 * A step fed by a step on event time (it takes that step's results, and
 * their times) follows that step's watermark: its own moves with the results
 * it gets, and up to the FLOOR of the step before -- the oldest time that
 * step can still send a result with (its watermark, the messages it holds,
 * the windows and deadlines it keeps open). Results come out of a step in
 * time order, so nothing the step before still sends is late in the step
 * after, and a quiet feed (`idle` on the first step) closes every step in
 * turn. It has no `idle` of its own: moving on with the feed's clock, it
 * could pass a result the step before has not sent yet.
 *
 * The times a result goes on with: its event time (for a later step on event
 * time), and the feed time it happened at: when the step's watermark reached
 * it -- the time of the message that moved it, the feed time at which `idle`
 * moved it, or the time the step before closed what held it back -- never
 * before the step's own last message or close. The same on every run and
 * every replay, however late a process notices.
 *
 * State: the held messages are entries of the step's state, keys
 * `b\u001f<n>`; the watermark and its anchors are the step's meta, carried in
 * the snapshot pointer.
 */
import { setImmediate as nextTurn } from 'node:timers/promises'
import { DueHeap, BUFFER_PREFIX, isBufferKey, REBUILD_BATCH } from './state.js'
import { extractEventTime } from '../../operators/_windowCommon.js'
import { messageOf } from './patterns.js'

function bufferKey(n) {
  return `${BUFFER_PREFIX}${String(n).padStart(12, '0')}`
}

/** An envelope as a held entry: the message, and only what differs from it. */
function pack(env) {
  const e = { m: env.msg, k: env.key }
  if (env.value === env.msg) e.vm = 1
  else if (env.msg && typeof env.msg === 'object' && env.value === env.msg.data) e.vd = 1
  else e.v = env.value
  if (env.derived) e.d = 1
  if (env.ctx !== undefined) e.c = env.ctx
  if (env.position !== undefined && env.position !== null) e.p = env.position
  if (Number.isFinite(env.eventTime)) e.et = env.eventTime
  return e
}

function unpack(e) {
  const env = { msg: e.m, key: e.k }
  env.value = e.vm ? e.m : e.vd ? (e.m && typeof e.m === 'object' ? e.m.data : undefined) : e.v
  if (e.d) env.derived = true
  if (e.c !== undefined) env.ctx = e.c
  if (e.p !== undefined) env.position = e.p
  if (e.et !== undefined) env.eventTime = e.et
  return env
}

/** The feed's clock: a step's time is the feed time it is given. */
export class FeedClock {
  kind = 'feed'

  constructor({ engine }) {
    this.engine = engine
  }

  /** It closes on the feed's clock: the runner moves that clock on a quiet feed. */
  get needsFeedClock() {
    return true
  }

  async accept(env, t, emits) {
    await this.engine.process(env, t, emits)
    return 'ok'
  }

  /** The feed time of its next close, or null. */
  nextAt() {
    return this.engine.nextDue()
  }

  /** Close what is due next: its emits are appended. */
  take(emits, when, limit = Infinity) {
    const due = this.engine.nextDue()
    this.engine.advance(due, emits, limit)
    return { at: due }
  }

  watermarkAt() {
    return null
  }

  checkpoint() {
    return null
  }

  restore() {}

  meta() {
    return {}
  }

  loadMeta() {}

  rebuild() {}

  async rebuildAsync() {}

  get held() {
    return 0
  }
}

export class EventClock {
  kind = 'event'
  #engine
  #state
  #fn
  #shape
  #latenessMs
  #idleMs
  #onLate
  #ordered
  #upstream
  #graceMs
  #heap = new DueHeap()
  // From the messages (a fed step: from the results it got): the latest event
  // time seen minus allowedLateness, as of the feed time tLast; null: none yet.
  wLast = -Infinity
  tLast = null
  // The feed time of the step's last message or close: nothing it does later happens before it.
  since = null
  // The number of the next held message.
  seq = 0

  /**
   * @param {object} o
   * @param {object} o.engine - the step's engine (windows, patterns, collect)
   * @param {object} o.state - the step's FeedState
   * @param {Function|null} o.fn - eventTime(msg); null: the time a result carries
   * @param {'value'|'feed'} [o.shape] - what `fn` gets: the envelope's msg (windows) or the feed's shape (patterns)
   * @param {number} [o.latenessMs]
   * @param {number|null} [o.idleMs]
   * @param {'drop'|'include'} [o.onLate]
   * @param {boolean} [o.ordered] - hold messages and take them in event-time order
   * @param {EventClock|null} [o.upstream] - the step this one is fed by: its watermark follows that step's floor
   * @param {number} [o.graceMs] - how long after a window's end it closes (the floor counts back from a close)
   */
  constructor({ engine, state, fn, shape = 'feed', latenessMs = 0, idleMs = null, onLate = 'drop', ordered = false, upstream = null, graceMs = 0 }) {
    if (!Number.isFinite(latenessMs) || latenessMs < 0) throw new Error(`allowedLateness must be a duration of zero or more, got ${latenessMs}`)
    if (idleMs !== null && (!Number.isFinite(idleMs) || idleMs <= 0)) throw new Error(`idle must be a duration above zero, got ${idleMs}`)
    if (upstream && idleMs !== null) throw new Error('a step fed by a step on event time follows its watermark: idle goes on the first step on event time')
    this.#engine = engine
    this.#state = state
    this.#fn = typeof fn === 'function' ? fn : null
    this.#shape = shape
    this.#latenessMs = latenessMs
    this.#idleMs = idleMs
    this.#onLate = onLate
    this.#ordered = ordered
    this.#upstream = upstream
    this.#graceMs = Number.isFinite(graceMs) && graceMs > 0 ? graceMs : 0
  }

  /** The feed's clock moves it: with `idle`, or when the step before it moves with that clock. */
  get needsFeedClock() {
    return this.#idleMs !== null || (this.#upstream !== null && this.#upstream.needsFeedClock)
  }

  /** The watermark at feed time `t`. */
  watermarkAt(t) {
    if (this.#upstream) {
      const f = this.#upstream.floorAt(t)
      return Number.isFinite(f) ? Math.max(this.wLast, f - this.#latenessMs) : this.wLast
    }
    if (this.tLast === null || this.#idleMs === null || !Number.isFinite(this.wLast)) return this.wLast
    const quiet = t - this.tLast - this.#idleMs
    return quiet > 0 ? this.wLast + quiet : this.wLast
  }

  /** The feed time at which the watermark reaches `x`; null when only a message, or the step before, can take it there. */
  readyAt(x) {
    let at = null
    if (this.tLast !== null && x <= this.wLast) at = this.tLast
    else if (this.#upstream) at = this.#upstream.floorReadyAt(x + this.#latenessMs)
    else if (this.tLast !== null && this.#idleMs !== null && Number.isFinite(this.wLast)) at = this.tLast + this.#idleMs + (x - this.wLast)
    if (at === null) return null
    return this.since !== null && this.since > at ? this.since : at
  }

  /**
   * The oldest event time this step can still send a result with, at feed
   * time `t`: a message to come is at or after the watermark (an older one is
   * late), a held message's results carry its time or later, and a window or
   * deadline still open sends the time of its end (or deadline): its close,
   * less the grace, less 2 ms at most.
   */
  floorAt(t) {
    let f = this.watermarkAt(t)
    this.#prune()
    if (this.#heap.size) f = Math.min(f, this.#heap.peek().due)
    const d = this.#engine.nextDue()
    if (d !== null) f = Math.min(f, d - this.#graceMs - 2)
    return f
  }

  /** The feed time at which the floor reaches `x`; null while a held message or an open close could still send something older. */
  floorReadyAt(x) {
    this.#prune()
    if (this.#heap.size && this.#heap.peek().due < x) return null
    const d = this.#engine.nextDue()
    if (d !== null && d - this.#graceMs - 2 < x) return null
    return this.readyAt(x)
  }

  async #timeOf(env, t) {
    let ts
    if (this.#fn) {
      ts = extractEventTime(this.#shape === 'feed' ? messageOf(env, t) : env.msg, this.#fn)
      if (ts === null) throw new Error('eventTime gave no time for this message (a Date, epoch milliseconds or an ISO string)')
    } else {
      ts = Number.isFinite(env.eventTime) ? env.eventTime : null
      if (ts === null) throw new Error('a result reached a step on event time without an event time')
    }
    return ts
  }

  /**
   * One envelope at feed time `t`: ignored (a pattern's, a join's or a
   * gather's tests do not take it), late ('late': dropped, or taken at once
   * with onLate 'include'), held until the watermark reaches it (in order),
   * or taken now. What the engine sends is appended to `emits`.
   */
  async accept(env, t, emits) {
    if (this.#ordered && !(await this.#engine.relevant(env, Number.isFinite(env.eventTime) ? env.eventTime : t))) return 'ok'
    const ts = await this.#timeOf(env, t)
    // A fed step judges by the results it got: its watermark also follows the
    // step before, whose floor moves while that step's results are on their way.
    const w = this.#upstream ? this.wLast : this.watermarkAt(t)
    this.wLast = Math.max(w, ts - this.#latenessMs)
    this.tLast = t
    if (this.since === null || t > this.since) this.since = t
    if (ts < w) {
      if (this.#onLate === 'include') await this.#engine.process(env, ts, emits)
      return 'late'
    }
    if (!this.#ordered) {
      await this.#engine.process(env, ts, emits)
      return 'ok'
    }
    const key = bufferKey(this.seq++)
    this.#state.set(key, { t: ts, e: pack(env) })
    this.#heap.push(ts, key)
    return 'ok'
  }

  #prune() {
    // A held message a rollback took back out of the state, or put back under
    // the same key with another time (seq rolls back with the state).
    while (this.#heap.size) {
      const top = this.#heap.peek()
      const v = this.#state.get(top.key)
      if (v && v.t === top.due) return
      this.#heap.pop()
    }
  }

  /** The next thing due in event time: a deadline (first, at a tie: past it, a message at that time is out of time) or a held message. */
  #next() {
    this.#prune()
    const due = this.#engine.nextDue()
    const held = this.#heap.size ? this.#heap.peek() : null
    if (held && (due === null || held.due < due)) return { at: held.due, key: held.key }
    return due === null ? null : { at: due, key: null }
  }

  /** The feed time of the next close or release, or null. */
  nextAt() {
    const n = this.#next()
    return n ? this.readyAt(n.at) : null
  }

  /**
   * Take what is due next, at feed time `when` (the caller checked it is
   * ready): close the engine's windows or deadlines of that time (appended to
   * `emits`), or take the next held message out, for the caller to run
   * through the engine: `{ at, release: { key, ts, env } }`.
   */
  take(emits, when, limit = Infinity) {
    const n = this.#next()
    if (Number.isFinite(when) && (this.since === null || when > this.since)) this.since = when
    if (!n.key) {
      this.#engine.advance(n.at, emits, limit)
      return { at: n.at }
    }
    this.#heap.pop()
    const v = this.#state.get(n.key)
    this.#state.delete(n.key)
    return { at: n.at, release: { key: n.key, ts: v.t, env: unpack(v.e) } }
  }

  /** The messages held until the watermark reaches them. */
  get held() {
    return this.#state.count('held')
  }

  checkpoint() {
    return { w: this.wLast, t: this.tLast, s: this.since, n: this.seq }
  }

  restore(cp) {
    this.wLast = cp.w
    this.tLast = cp.t
    this.since = cp.s
    this.seq = cp.n
  }

  /** The watermark and its anchors, for the snapshot pointer (JSON: no infinities). */
  meta() {
    const m = { n: this.seq }
    if (Number.isFinite(this.wLast)) m.w = this.wLast
    if (this.tLast !== null) m.t = this.tLast
    if (this.since !== null) m.s = this.since
    return m
  }

  loadMeta(meta) {
    const m = meta && typeof meta === 'object' ? meta : {}
    // `watermark`: a record from before the anchors (the windows' own watermark).
    this.wLast = Number.isFinite(m.w) ? m.w : Number.isFinite(m.watermark) ? m.watermark : -Infinity
    this.tLast = Number.isFinite(m.t) ? m.t : null
    this.since = Number.isFinite(m.s) ? m.s : this.tLast
    this.seq = Number.isInteger(m.n) && m.n >= 0 ? m.n : 0
  }

  /** Index the held messages of the state, after a load; numbers go on after the highest one held. */
  rebuild() {
    this.#heap.clear()
    for (const [k, v] of this.#state.entries()) this.#index(k, v)
  }

  /** The same, a batch at a time, so a process loading millions of entries keeps its lock and answers. */
  async rebuildAsync() {
    this.#heap.clear()
    let n = 0
    for (const [k, v] of this.#state.entries()) {
      this.#index(k, v)
      if (++n % REBUILD_BATCH === 0) await nextTurn()
    }
  }

  #index(k, v) {
    if (!isBufferKey(k) || !v || !Number.isFinite(v.t)) return
    this.#heap.push(v.t, k)
    const n = Number(k.slice(BUFFER_PREFIX.length))
    if (Number.isInteger(n) && n >= this.seq) this.seq = n + 1
  }
}
