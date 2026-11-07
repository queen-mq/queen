/**
 * The steps of a feed stream, run in chain order: stateless operators, and
 * any number of stateful steps (a window with reduce/aggregate, a pattern, a
 * join, a gather, a table join), each with its own state.
 *
 * A feed message enters as `{ msg, key: partition, value: data }`. A stateful
 * step that sends a result (a window closed, a match, a timeout, a gathered
 * run) hands it on as an envelope of its own -- `{ msg: result, value: result,
 * key, ctx, eventTime }`, marked derived -- which the operators and steps
 * after it see as they see a window's result today: map/filter/keyBy get the
 * result, a pattern/join/gather gets it in the feed's shape (see patterns.js
 * messageOf). The steps after the last one feed the sink.
 *
 * Each step with an engine runs on a clock (clock.js): the feed's (the time
 * the log committed each message), or event time -- its own `eventTime`, or,
 * after a step on event time, the time each result carries. `eventTime:
 * false` keeps a step on the feed's clock after a step on event time.
 *
 * `advance(t)` closes what is due by feed time `t`, across the steps, in the
 * order it fell due on the feed's clock -- an event-time step's close falls
 * due when its watermark reaches it -- the later step first at one time, so a
 * result that reaches a step finds every wait of that step due by then
 * already closed: one order every run, on every replay.
 */
import { FeedState, CLOSE_BATCH } from './state.js'
import { WindowEngine } from './windows.js'
import { PatternEngine } from './patterns.js'
import { CollectEngine } from './collect.js'
import { TableStep } from './tables.js'
import { FeedClock, EventClock } from './clock.js'
import { setImmediate as nextTurn } from 'node:timers/promises'

async function applyAll(op, envelopes) {
  const out = []
  for (const env of envelopes) {
    for (const e of await op.apply(env)) out.push(e)
  }
  return out
}

function stepLabel(s) {
  if (s.kind === 'window') return `the ${s.window.windowKind} window`
  if (s.kind === 'pattern') return `the ${s.op.patternKind}`
  if (s.kind === 'collect') return `the ${s.op.collectKind}`
  return `the ${s.kind}`
}

/**
 * The clock of a step: event time when it says `eventTime`, or when the
 * stateful step before it runs on event time (unless it says `eventTime:
 * false`): then it takes that step's results with their times, and its
 * watermark follows that step's (clock.js); else the feed's. `fed`: a
 * stateful step comes before it, so its messages are that step's results.
 */
function clockOf(s, engine, state, before, fed) {
  const op = s.kind === 'window' ? s.window : s.op
  const what = `${stepLabel(s)} ${JSON.stringify(s.name)}`
  // Patterns, joins, gathers and sessions depend on the order of their messages.
  const ordered = s.kind !== 'window' || s.window.windowKind === 'session'
  const onLate = op.onLatePolicy ?? 'drop'
  const latenessMs = op.allowedLatenessMs ?? 0
  const idleMs = op.idleMs ?? null
  const graceMs = s.kind === 'window' ? s.window.gracePeriodMs ?? 0 : 0
  if (typeof op.eventTimeFn === 'function') {
    if (fed && idleMs !== null) {
      throw new Error(
        `Stream.fromFeed: idle on ${what}, whose messages are the results of the step before it: moving on with the feed's ` +
        'clock it could pass a result that step has not sent yet. Leave out its eventTime (it then takes the times of those ' +
        'results and follows that step\'s watermark), and put idle on the first step on event time'
      )
    }
    return new EventClock({
      engine, state, fn: op.eventTimeFn, shape: s.kind === 'window' ? 'value' : 'feed',
      latenessMs, idleMs, onLate, ordered, graceMs
    })
  }
  if (before && !op.eventTimeOff) {
    if (idleMs !== null) {
      throw new Error(`Stream.fromFeed: ${what} runs on the event time of the step before it and follows its watermark: put idle on the first step on event time`)
    }
    return new EventClock({ engine, state, fn: null, latenessMs, onLate, ordered, upstream: before, graceMs })
  }
  if (idleMs !== null) {
    throw new Error(`Stream.fromFeed: idle moves a watermark on: ${what} runs on the feed's clock, give it eventTime (or put it after a step on event time)`)
  }
  // A window as a pattern: both default to no lateness and 'drop', so anything else was asked for.
  if (latenessMs > 0 || onLate === 'include') {
    throw new Error(`Stream.fromFeed: allowedLateness and onLate are for event time: ${what} runs on the feed's clock, give it eventTime (or put it after a step on event time)`)
  }
  return new FeedClock({ engine })
}

export class Pipeline {
  steps = []
  late = 0

  /**
   * @param {Array} plan - the compiled steps: `{ kind: 'ops', ops }`, `{ kind:
   *   'window', window, reducer, name }`, `{ kind: 'pattern'|'collect', op, name }`,
   *   `{ kind: 'table', op, name }`
   */
  constructor(plan) {
    // The event-time clock of the last step with an engine: a step after it runs on event time too.
    let before = null
    let fed = false
    for (const s of plan) {
      if (s.kind === 'ops') {
        this.steps.push({ kind: 'ops', ops: s.ops })
        continue
      }
      const state = new FeedState()
      const step = { kind: s.kind, name: s.name, state, engine: null, table: null, clock: null, committed: null }
      if (s.kind === 'window') step.engine = new WindowEngine({ window: s.window, reducer: s.reducer, state })
      else if (s.kind === 'pattern') step.engine = new PatternEngine({ op: s.op, state })
      else if (s.kind === 'collect') step.engine = new CollectEngine({ op: s.op, state })
      else if (s.kind === 'table') step.table = new TableStep({ op: s.op, state })
      else throw new Error(`feed pipeline: unknown step ${s.kind}`)
      if (step.engine) {
        step.clock = clockOf(s, step.engine, state, before, fed)
        before = step.clock.kind === 'event' ? step.clock : null
        fed = true
      }
      this.steps.push(step)
    }
    this.commitPoint()
  }

  /** The steps that keep state, in chain order. */
  get stateful() {
    return this.steps.filter((s) => s.state)
  }

  get hasState() {
    return this.steps.some((s) => s.state)
  }

  get tables() {
    return this.steps.filter((s) => s.table).map((s) => s.table)
  }

  /** The step named `name` (or `step<n>`). */
  step(name) {
    return this.steps.find((s) => s.state && s.name === name) || null
  }

  // ---- running ------------------------------------------------------------

  /** A table queue's message: every table of that queue takes the row. */
  async applyRow(entry) {
    for (const s of this.steps) if (s.table && s.table.queues.includes(entry.queue)) await s.table.applyRow(entry)
  }

  /** One feed message at feed time `t`; what reaches the end goes to `out(env, entry)`. */
  async entry(entry, t, out) {
    await this.#flow(0, [{ msg: entry, key: entry.partition, value: entry.data }], t, entry, out, entry.position)
  }

  /**
   * Close what is due by feed time `t`, in the order it fell due, across the
   * steps: the earliest first, and at one time the later step first, so a
   * result that reaches a step at time T finds every wait of that step due by
   * T already closed -- as a message at T does, since the runner closes up to
   * T before it. A step on event time falls due when its watermark reaches
   * its next window, deadline or held message; a held message is taken out
   * and run through its step then.
   *
   * Each result runs on through the steps after it as one unit: `unit(emit,
   * run)`, where the runner calls `run(out)` (again, to retry) and judges it;
   * a held message is a unit too, so the poison policy meets the message that
   * fails. So the closes interleave with the messages the same way whenever
   * this is called: on every tick, every read, and every replay.
   */
  async advance(t, unit, opts = {}) {
    // `maxUnits`: stop after so many units (a replay stops where the stored
    // record says the stream got to); `onBatch(done)`: called after each batch,
    // where the runner may commit in the middle of a long close ('abandon'
    // stops here: the runner loaded its state again).
    const maxUnits = opts.maxUnits ?? Infinity
    const onBatch = opts.onBatch ?? null
    let done = 0
    for (;;) {
      if (done >= maxUnits) return 'ok'
      let at = -1
      let when = Infinity
      for (let i = this.steps.length - 1; i >= 0; i--) {
        const c = this.steps[i].clock
        if (!c) continue
        const w = c.nextAt()
        if (w !== null && w <= t && w < when) { at = i; when = w }
      }
      if (at < 0) return 'ok'
      const s = this.steps[at]
      const emits = []
      // At most CLOSE_BATCH at once: millions of windows that end at one time
      // close in batches, in the same order, with the event loop turning
      // between them (the lock is renewed, live reads are answered). Never
      // more than may run: no window leaves the state without its unit.
      const took = s.clock.take(emits, when, Math.min(CLOSE_BATCH, maxUnits - done))
      if (took.release) {
        if ((await this.#release(at, took.release, when, unit)) === 'stop') return 'stop'
        done++
        if (onBatch && (await onBatch(done)) === 'abandon') return 'abandon'
        continue
      }
      for (const em of emits) {
        em.step = at
        // A result happens when its window or deadline closed, whenever this
        // process noticed: the steps after see the same time on every run.
        const r = await unit(em, async (out) => {
          const envs = await this.#derive([em], null)
          await this.#flow(at + 1, envs, when, null, out, null)
        })
        if (r === 'stop') return 'stop'
        done++
      }
      if (onBatch && emits.length && (await onBatch(done)) === 'abandon') return 'abandon'
      if (emits.length >= CLOSE_BATCH) await nextTurn()
    }
  }

  /** A message an event-time step held, now in its turn: through the step, and its results on. */
  async #release(at, rel, when, unit) {
    const s = this.steps[at]
    const env = rel.env
    const em = {
      step: at,
      key: env.key,
      time: rel.ts,
      id: rel.key,
      // A feed message: reported and dead-lettered as the message it is; a result, as the result it is.
      ctx: { kind: env.derived ? (env.ctx?.kind ?? 'result') : 'message', key: env.key, partition: env.key, eventTime: rel.ts },
      entry: env.derived ? null : env.msg,
      value: env.derived ? env.value : undefined,
      state: { msg: env.msg },
      release: true
    }
    return unit(em, async (out) => {
      const emits = []
      await s.engine.process(env, rel.ts, emits)
      const position = env.position ?? (env.derived ? null : env.msg?.position ?? null)
      const envs = await this.#derive(emits, position)
      await this.#flow(at + 1, envs, when, null, out, position)
    })
  }

  /** When the next window, deadline or held message is due on the feed's clock; null if none waits on it. */
  nextDue() {
    let min = null
    for (const s of this.steps) {
      if (!s.clock || !s.clock.needsFeedClock) continue
      const d = s.clock.nextAt()
      if (d !== null && (min === null || d < min)) min = d
    }
    return min
  }

  /** Whether a step closes as the feed's clock moves (the runner then moves it on a quiet feed). */
  get needsFeedClock() {
    return this.steps.some((s) => s.clock && s.clock.needsFeedClock)
  }

  /** The lowest event-time watermark at feed time `t`, for the metrics; null without event time. */
  watermarkAt(t) {
    let w = null
    for (const s of this.steps) {
      if (!s.clock || s.clock.kind !== 'event') continue
      const x = s.clock.watermarkAt(t)
      if (Number.isFinite(x)) w = w === null ? x : Math.min(w, x)
    }
    return w
  }

  /**
   * Run `envs` through the steps from `from`. `t` is the feed time; `entry`
   * the feed message they came from (null for results of a close: then the
   * runner gives outputs ids of its own); `position` the position the results
   * of a step carry.
   */
  async #flow(from, envs, t, entry, out, position) {
    let cur = envs
    for (let i = from; i < this.steps.length && cur.length; i++) {
      const s = this.steps[i]
      if (s.kind === 'ops') {
        for (const op of s.ops) cur = await applyAll(op, cur)
        continue
      }
      if (s.table) {
        const next = []
        for (const env of cur) for (const e of await s.table.apply(env, t)) next.push(e)
        cur = next
        continue
      }
      const emits = []
      for (const env of cur) {
        if ((await s.clock.accept(env, t, emits)) === 'late') this.late++
      }
      cur = await this.#derive(emits, position)
    }
    for (const env of cur) await out(env, entry)
  }

  async #derive(emits, position) {
    const out = []
    for (const em of emits) {
      const value = em.compute ? await em.compute() : em.value
      if (em.compute && (value === null || value === undefined)) continue
      const env = { msg: value, value, ctx: em.ctx, key: em.key, derived: true, position: position ?? null }
      if (Number.isFinite(em.eventTime)) env.eventTime = em.eventTime
      out.push(env)
    }
    return out
  }

  // ---- state --------------------------------------------------------------

  /** A point to come back to: every step's state and clock. */
  checkpoint() {
    return this.steps.map((s) => (s.state ? { mark: s.state.mark(), clock: s.clock ? s.clock.checkpoint() : null } : null))
  }

  restore(cp) {
    this.steps.forEach((s, i) => {
      if (!s.state) return
      s.state.rollbackTo(cp[i].mark)
      if (s.clock && cp[i].clock) s.clock.restore(cp[i].clock)
    })
  }

  /** What is in now is committed: the point a rollback comes back to. */
  commitPoint() {
    for (const s of this.steps) {
      if (!s.state) continue
      s.state.committed()
      s.committed = s.clock ? s.clock.checkpoint() : null
    }
  }

  /** Back to the last commit point. */
  discard() {
    for (const s of this.steps) {
      if (!s.state) continue
      s.state.discard()
      if (s.engine) {
        if (s.committed) s.clock.restore(s.committed)
        s.engine.rebuild()
        s.clock.rebuild()
      }
    }
  }

  get dirty() {
    return this.steps.some((s) => s.state && s.state.dirty)
  }

  get changedSinceSnapshot() {
    return this.steps.some((s) => s.state && s.state.changedSinceSnapshot)
  }

  snapshotTaken() {
    for (const s of this.steps) if (s.state) s.state.snapshotTaken()
  }

  /** No step holds anything. */
  get empty() {
    return this.steps.every((s) => !s.state || s.state.size === 0)
  }

  /** Every step's state, for a snapshot. */
  dump() {
    return { steps: this.stateful.map((s) => ({ name: s.name, kind: s.kind, entries: s.state.dump() })) }
  }

  /** The clocks' own values (event-time watermarks and their anchors), carried in the snapshot pointer. */
  metas() {
    return this.stateful.map((s) => (s.clock ? s.clock.meta() : {}))
  }

  /** Every step from a snapshot (`dump` null: empty) and the clocks' values. */
  load(dump, metas) {
    const steps = this.stateful
    const stored = dump && Array.isArray(dump.steps) ? dump.steps : []
    steps.forEach((s, i) => {
      const d = stored[i]
      s.state.load(d && d.kind === s.kind ? d.entries : [])
      if (s.engine) {
        s.clock.loadMeta(Array.isArray(metas) ? metas[i] : null)
        s.engine.rebuild()
        s.clock.rebuild()
      }
    })
    this.commitPoint()
  }

  /**
   * A load in pieces (snapshot.js, format 2): `loadBegin()`, then
   * `loadPiece(step, kind, set, del)` for each piece of each layer in order,
   * then `loadEnd(metas)`. A piece of another kind of step than the one at
   * that place in the chain is left out, as `load` leaves out a dump of another
   * chain (the chain hash refuses that case before).
   */
  loadBegin() {
    for (const s of this.stateful) s.state.loadBegin()
  }

  loadPiece(step, kind, set, del) {
    const s = this.stateful[step]
    if (!s || s.kind !== kind) return
    if (Array.isArray(set)) for (const kv of set) s.state.loadSet(kv[0], kv[1])
    if (Array.isArray(del)) for (const k of del) s.state.loadDelete(k)
  }

  async loadEnd(metas) {
    const steps = this.stateful
    for (let i = 0; i < steps.length; i++) {
      const s = steps[i]
      s.state.loaded()
      if (s.engine) {
        s.clock.loadMeta(Array.isArray(metas) ? metas[i] : null)
        await s.engine.rebuildAsync()
        await s.clock.rebuildAsync()
      }
    }
    this.commitPoint()
  }

  /** How many keys changed since the last snapshot, over every step (a delta writes them). */
  get touchedSinceSnapshot() {
    let n = 0
    for (const s of this.stateful) n += s.state.touchedSinceSnapshot
    return n
  }

  /** Every step empty (a reset): the next snapshot writes it. */
  wipe() {
    for (const s of this.stateful) {
      s.state.wipe()
      if (s.engine) {
        s.clock.loadMeta(null)
        s.engine.rebuild()
        s.clock.rebuild()
      }
    }
    this.commitPoint()
  }

  /** Open windows, waits, runs and rows per step, and in all. */
  get open() {
    let n = 0
    for (const s of this.stateful) n += s.engine ? s.engine.open : s.table.open
    return n
  }

  get stateKeys() {
    let n = 0
    for (const s of this.stateful) n += s.state.size
    return n
  }

  /** Per step: what is open, the state's keys, and on event time the watermark and the messages held. */
  stepMetrics(t) {
    return this.stateful.map((s) => {
      const m = { name: s.name, kind: s.kind, open: s.engine ? s.engine.open : s.table.open, keys: s.state.size }
      if (s.clock) {
        m.clock = s.clock.kind
        if (s.clock.kind === 'event') {
          const w = s.clock.watermarkAt(t)
          m.watermark = Number.isFinite(w) ? new Date(w).toISOString() : null
          m.held = s.clock.held
        }
      }
      return m
    })
  }

  /** A live read of one step: `inspect(key)` or `list(limit)`. */
  inspect(name, key) {
    const s = this.step(name)
    if (!s) return { found: false, error: `no step named ${JSON.stringify(name)} (${this.stateful.map((x) => x.name).join(', ')})` }
    const v = (s.engine || s.table).inspect(key)
    return v === null ? { found: false } : { found: true, value: v }
  }

  list(name, limit = 100) {
    const s = this.step(name)
    if (!s) return { error: `no step named ${JSON.stringify(name)} (${this.stateful.map((x) => x.name).join(', ')})`, items: [] }
    const items = []
    let truncated = false
    for (const v of (s.engine || s.table).list()) {
      if (items.length >= limit) { truncated = true; break }
      items.push(v)
    }
    // The messages an event-time step holds in order are not the step's to list.
    return { items, truncated, total: s.state.size - (s.clock ? s.clock.held : 0) }
  }
}
