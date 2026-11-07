/**
 * Windows on the tenant feed: tumbling, sliding, cron and session windows
 * with reduce/aggregate, per key, kept in the stream's FeedState.
 *
 * The clock (clock.js). Without `eventTime` a message's time is when the log
 * committed it, and every queue shares that clock: it only moves forward
 * along the log, and once a read has answered `resolvedTs` no message
 * committed later can be older. So a window closes once the clock is past its
 * end (plus `gracePeriod`), and nothing that belongs to it can come after: no
 * late messages and no guessing. The runner moves the clock to each message's
 * time and to every read's `resolvedTs`; when the feed is quiet it writes
 * the position once, which moves the broker's clock, to close the window on
 * time.
 *
 * With `eventTime` (or after a step on event time) a message's time is the
 * function's answer (the result's own time) and windows close on the step's
 * watermark (the latest event time seen, minus `allowedLateness`; with
 * `idle`, a quiet feed moves it on); an event older than the watermark is
 * late: dropped (`onLate: 'drop'`, the default) or folded in anyway
 * (`'include'`, which opens its window again and sends it again). Sessions
 * on event time get their messages in event-time order (the clock holds them
 * for `allowedLateness`); the other kinds only add up, and take each message
 * as it comes. Either way the engine gets each message with its time.
 *
 * A result goes on with its event time: the window's last millisecond.
 *
 * State keys: `w\u001f<start>\u001f<key>` for a window of the fixed kinds,
 * `s\u001f<key>` for a key's open session. A value is
 * `{ key, start, end, due, acc }` (a session also keeps `last`), where `due`
 * is when it closes.
 */
import { setImmediate as nextTurn } from 'node:timers/promises'
import { DueHeap, isBufferKey, REBUILD_BATCH } from './state.js'

const SEP = '\u001f'

function clone(v) {
  return v === undefined || v === null || typeof v !== 'object' ? v : structuredClone(v)
}

/** An aggregate's result, without the running sums it keeps for `avg`. */
function finalAcc(acc, isAggregate) {
  if (!isAggregate || !acc || typeof acc !== 'object') return clone(acc)
  const out = {}
  for (const [k, v] of Object.entries(acc)) if (!k.startsWith('__avg_')) out[k] = v
  return out
}

export class WindowEngine {
  #op
  #reducer
  #state
  #heap = new DueHeap()
  #isAggregate

  constructor({ window, reducer, state }) {
    if (!reducer) throw new Error('a window on a feed needs .reduce() or .aggregate() after it')
    this.#op = window
    this.#reducer = reducer
    this.#state = state
    this.#isAggregate = reducer.kind === 'aggregate'
    this.kind = window.windowKind
    // Counted as they change (FeedState), so metrics cost the same at any size.
    state.classifyWith((k) => isBufferKey(k) ? 'held' : (k.startsWith(`w${SEP}`) || k.startsWith(`s${SEP}`)) ? 'open' : null)
  }

  /** Every message concerns a window: a session on event time holds them all, in order. */
  relevant() {
    return true
  }

  /** Index what the state holds, after a load. */
  rebuild() {
    this.#heap.clear()
    for (const [k, v] of this.#state.entries()) this.#index(k, v)
  }

  /** The same, a batch at a time, so a process loading millions of windows keeps its lock and answers. */
  async rebuildAsync() {
    this.#heap.clear()
    let n = 0
    for (const [k, v] of this.#state.entries()) {
      this.#index(k, v)
      if (++n % REBUILD_BATCH === 0) await nextTurn()
    }
  }

  #index(k, v) {
    if ((k.startsWith(`w${SEP}`) || k.startsWith(`s${SEP}`)) && v && Number.isFinite(v.due)) this.#heap.push(v.due, k)
  }

  /** When the next window closes, on the clock this engine closes on; null if none is open. */
  nextDue() {
    this.#prune()
    return this.#heap.size ? this.#heap.peek().due : null
  }

  /** The open windows and sessions. */
  get open() {
    return this.#state.count('open')
  }

  /**
   * Close every window due at or before `t`. Pure: it removes them from the
   * state and appends one emit each, `{ key, value, ctx }`.
   */
  advance(t, emits, limit = Infinity) {
    for (let n = 0; n < limit; n++) {
      this.#prune()
      if (!this.#heap.size || this.#heap.peek().due > t) return
      const { key: sk } = this.#heap.pop()
      const v = this.#state.get(sk)
      this.#state.delete(sk)
      // The result's time is when the window closed, not when this process
      // saw it close: one time on every run and every replay.
      emits.push({ ...this.#emitOf(v), time: v.due })
    }
  }

  /**
   * Fold one envelope in at time `t` (the step's clock: the feed time of its
   * message, or its event time). A session that the gap closed is appended
   * to `emits`.
   */
  async process(env, t, emits) {
    const key = String(env.key)
    const value = env.value !== undefined ? env.value : env.msg
    if (this.kind === 'session') {
      await this.#session(key, t, value, emits)
    } else {
      for (const [start, end] of this.#windowsOf(t)) await this.#fold(key, start, end, value)
    }
    return 'ok'
  }

  // ---- internals ---------------------------------------------------------

  #prune() {
    // A heap item whose window was closed, or moved, since it was pushed.
    while (this.#heap.size) {
      const top = this.#heap.peek()
      const v = this.#state.get(top.key)
      if (v && v.due === top.due) return
      this.#heap.pop()
    }
  }

  #windowsOf(ts) {
    const op = this.#op
    if (this.kind === 'tumbling') {
      const start = Math.floor(ts / op.windowMs) * op.windowMs
      return [[start, start + op.windowMs]]
    }
    if (this.kind === 'cron') {
      const start = op._floorToBoundary(ts)
      return [[start, start + op.windowMs]]
    }
    if (this.kind === 'sliding') {
      const latest = Math.floor(ts / op.slideMs) * op.slideMs
      const out = []
      for (let i = 0; i < op.windowsPerEvent; i++) {
        const start = latest - i * op.slideMs
        if (ts >= start && ts < start + op.sizeMs) out.push([start, start + op.sizeMs])
      }
      return out
    }
    throw new Error(`a ${this.kind} window does not run on a feed`)
  }

  async #fold(key, start, end, value) {
    const sk = `w${SEP}${start}${SEP}${key}`
    const cur = this.#state.get(sk)
    const acc = await this.#reducer.fn(cur ? clone(cur.acc) : this.#reducer._initial(), value)
    const due = end + this.#op.gracePeriodMs
    this.#state.set(sk, { key, start, end, due, acc })
    if (!cur) this.#heap.push(due, sk)
  }

  async #session(key, ts, value, emits) {
    const sk = `s${SEP}${key}`
    let cur = this.#state.get(sk)
    const gap = this.#op.gapMs
    if (cur && ts > cur.last + gap) {
      // Past the gap: that session is over, and this message starts the next.
      this.#state.delete(sk)
      emits.push(this.#emitOf(cur))
      cur = undefined
    }
    const acc = await this.#reducer.fn(cur ? clone(cur.acc) : this.#reducer._initial(), value)
    const start = cur ? Math.min(cur.start, ts) : ts
    const last = cur ? Math.max(cur.last, ts) : ts
    // A message exactly `gap` after the last one is still in the session (as
    // a pattern's `within` is in time): it closes the millisecond after.
    const due = last + gap + 1 + this.#op.gracePeriodMs
    this.#state.set(sk, { key, start, last, end: last + gap, due, acc })
    if (!cur || cur.due !== due) this.#heap.push(due, sk)
  }

  /** The open windows of `key`, for a live read: `{ key, windows: [{ start, end, value }] }`, or null. */
  inspect(key) {
    const k = String(key)
    const windows = []
    for (const [, v] of this.#state.entries()) if (v && v.key === k) windows.push(this.#view(v))
    if (!windows.length) return null
    windows.sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0))
    return { key: k, windows }
  }

  /** Every open window, for a live read. */
  * list() {
    for (const [, v] of this.#state.entries()) if (v && v.key !== undefined) yield { key: v.key, windows: [this.#view(v)] }
  }

  #view(v) {
    return {
      start: new Date(v.start).toISOString(),
      end: new Date(v.end).toISOString(),
      closesAt: new Date(v.due).toISOString(),
      value: finalAcc(v.acc, this.#isAggregate)
    }
  }

  #emitOf(v) {
    return {
      key: v.key,
      // On to a later step on event time: the last millisecond the window covers.
      eventTime: v.end - 1,
      value: finalAcc(v.acc, this.#isAggregate),
      ctx: {
        kind: this.kind,
        key: v.key,
        partition: v.key,
        windowStart: v.start,
        windowEnd: v.end,
        windowKey: new Date(v.start).toISOString()
      },
      state: v
    }
  }
}
