/**
 * `.join({ sides })` and `.gather()` on the tenant feed: collect the parts of
 * one entity until every part is in, or its deadline passes.
 *
 * join (sides): one message per side, any number of sides, in any order,
 * within `within` of the first. When the last side arrives, `select(got, key)`
 * is sent; a new message for the key then starts a new set.
 *
 * gather (steps or count): the parts of one run. With `steps`, one message per
 * named step, as a join; with `count(msg, value)`, distinct messages (by queue,
 * partition and transaction id) until as many as a part says there are. When
 * all are in, `onComplete(got, key)` is sent, and the run is remembered until
 * its deadline, so a late duplicate does not open a new one that would then
 * time out.
 *
 * Either way, a deadline that passes first sends `onTimeout(got, missing, key)`:
 * `missing` is the names not in (steps, sides), or how many parts are missing
 * (count; null when no part said how many).
 *
 * State key `c\u001f<key>`; a value is `{ key, first, due, parts | list+ids,
 * expected }`, or `{ key, due, done: true }` for a finished gather.
 *
 * On event time (clock.js) the parts come in event-time order with their
 * event times: `first` is the earliest part's, the deadline counts from it. A
 * result goes on with its time: the last part's, or the deadline's last
 * millisecond.
 */
import { setImmediate as nextTurn } from 'node:timers/promises'
import { DueHeap, isBufferKey, REBUILD_BATCH } from './state.js'
import { clone, messageOf, snapshotOf, keptOf } from './patterns.js'

const SEP = '\u001f'

function partId(msg) {
  if (msg.transactionId) return `${msg.queue}${SEP}${msg.partition}${SEP}${msg.transactionId}`
  // An earlier step's result: one per window, or per deadline, of its key.
  if (msg.derived) {
    const c = msg.ctx || {}
    return `d${SEP}${msg.key}${SEP}${c.kind ?? ''}${SEP}${c.windowStart ?? c.deadline ?? ''}${SEP}${msg.ts ?? ''}`
  }
  return `${msg.position}${SEP}${msg.offset ?? ''}${SEP}${msg.key ?? msg.partition ?? ''}`
}

export class CollectEngine {
  #op
  #state
  #heap = new DueHeap()

  constructor({ op, state }) {
    this.#op = op
    this.#state = state
    this.kind = op.collectKind
    state.classifyWith((k, v) => isBufferKey(k) ? 'held' : (v && !v.done) ? 'open' : null)
  }

  /** Whether a message is a part (with steps or sides: one of their tests passes; by count: every message is). */
  async relevant(env, t) {
    const op = this.#op
    if (!op.parts) return true
    const msg = messageOf(env, t)
    for (const p of op.parts) if (await p.pred(msg, env.value)) return true
    return false
  }

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
    if (!isBufferKey(k) && v && Number.isFinite(v.due)) this.#heap.push(v.due, k)
  }

  nextDue() {
    this.#prune()
    return this.#heap.size ? this.#heap.peek().due : null
  }

  get open() {
    return this.#state.count('open')
  }

  advance(t, emits, limit = Infinity) {
    for (let n = 0; n < limit; n++) {
      this.#prune()
      if (!this.#heap.size || this.#heap.peek().due > t) return
      const { key: sk } = this.#heap.pop()
      const v = this.#state.get(sk)
      this.#state.delete(sk)
      if (v.done || !this.#op.onTimeout) continue
      const got = this.#got(v)
      const missing = this.#missing(v)
      emits.push({
        key: v.key,
        ctx: { kind: 'timeout', key: v.key, partition: v.key, deadline: v.due - 1, missing },
        state: v,
        time: v.due,
        eventTime: v.due - 1,
        compute: () => this.#op.onTimeout(clone(got), clone(missing), v.key)
      })
    }
  }

  async process(env, t, emits) {
    const op = this.#op
    const msg = messageOf(env, t)
    let part = null
    if (op.parts) {
      for (const p of op.parts) {
        if (await p.pred(msg, env.value)) { part = p.name; break }
      }
      if (part === null) return 'ok'
    }
    const key = String(op.keyFn ? await op.keyFn(msg, env.value) : env.key)
    const sk = `c${SEP}${key}`
    const run = this.#state.get(sk)
    // A gather that finished: remembered until its deadline, and quiet.
    if (run && run.done) return 'ok'
    const snap = snapshotOf(msg, await keptOf(op, msg, env.value))
    let next
    if (op.parts) {
      if (run && run.parts[part] !== undefined && op.onRepeat === 'keep') return 'ok'
      next = run
        ? { ...run, parts: { ...run.parts, [part]: snap } }
        : { key, first: t, due: t + op.withinMs + 1, parts: { [part]: snap } }
    } else {
      const id = partId(msg)
      if (run && run.ids.includes(id)) return 'ok'
      let expected = run ? run.expected : null
      if (op.countFn) {
        const n = await op.countFn(msg, env.value)
        if (Number.isInteger(n) && n > 0) expected = expected === null ? n : Math.max(expected, n)
      }
      next = run
        ? { ...run, list: [...run.list, snap], ids: [...run.ids, id], expected }
        : { key, first: t, due: t + op.withinMs + 1, list: [snap], ids: [id], expected }
    }
    if (this.#complete(next)) {
      const got = this.#got(next)
      emits.push({
        key,
        ctx: { kind: 'complete', key, partition: key, deadline: next.due - 1 },
        state: next,
        eventTime: t,
        compute: op.onComplete
          ? () => op.onComplete(clone(got), key)
          : () => ({ key, parts: clone(got) })
      })
      if (op.remember) {
        this.#state.set(sk, { key, due: next.due, done: true })
        if (!run) this.#heap.push(next.due, sk)
      } else {
        this.#state.delete(sk)
      }
      return 'ok'
    }
    this.#state.set(sk, next)
    if (!run) this.#heap.push(next.due, sk)
    return 'ok'
  }

  /** The run of `key`, for a live read. */
  inspect(key) {
    const v = this.#state.get(`c${SEP}${String(key)}`)
    return v ? this.#view(v) : null
  }

  * list() {
    for (const [k, v] of this.#state.entries()) if (!isBufferKey(k)) yield this.#view(v)
  }

  #view(v) {
    if (v.done) return { key: v.key, done: true, until: new Date(v.due - 1).toISOString() }
    return {
      key: v.key,
      got: clone(this.#got(v)),
      missing: this.#missing(v),
      expected: v.list ? v.expected : undefined,
      deadline: new Date(v.due - 1).toISOString()
    }
  }

  #complete(v) {
    if (this.#op.parts) return this.#op.parts.every((p) => v.parts[p.name] !== undefined)
    return v.expected !== null && v.list.length >= v.expected
  }

  #got(v) {
    return this.#op.parts ? v.parts : v.list
  }

  #missing(v) {
    if (this.#op.parts) return this.#op.parts.filter((p) => v.parts[p.name] === undefined).map((p) => p.name)
    return v.expected === null ? null : Math.max(0, v.expected - v.list.length)
  }

  #prune() {
    while (this.#heap.size) {
      const top = this.#heap.peek()
      const v = this.#state.get(top.key)
      if (v && v.due === top.due) return
      this.#heap.pop()
    }
  }
}
