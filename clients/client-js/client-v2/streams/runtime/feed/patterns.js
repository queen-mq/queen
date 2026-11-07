/**
 * `.pattern()` and `.join()` (two sides) on the tenant feed: two kinds of
 * message for one entity, in time, on the clock every queue shares (when the
 * log committed each message), or on event time (clock.js: the clock then
 * hands the engine its messages in event-time order, each with its event
 * time). See PatternOperator for what they mean.
 *
 * State keys: `p\u001f<key>` for the `start` a pattern waits on, and
 * `j\u001fL\u001f<key>` / `j\u001fR\u001f<key>` for the side a join waits on.
 * A value is `{ key, msg, side?, due }`: the waiting message (or what `keep`
 * kept of it), and `due`, the first millisecond past its deadline. A message
 * exactly `within` after the one waiting is still in time.
 *
 * Matches are found as messages are processed (the runner calls `process`);
 * deadlines pass as the clock moves (`advance`), and a timeout is an emit
 * whose value the runner gets from `onTimeout` -- user code, so it runs in a
 * unit of its own, with its own retries. A result goes on with its time, for
 * a later step on event time: a match's is the time of the message that made
 * it, a timeout's the deadline's last millisecond.
 */
import { setImmediate as nextTurn } from 'node:timers/promises'
import { DueHeap, isBufferKey, REBUILD_BATCH } from './state.js'

const SEP = '\u001f'

export function clone(v) {
  return v === undefined || v === null || typeof v !== 'object' ? v : structuredClone(v)
}

/**
 * The message a pattern, join or gather sees for an envelope: the feed
 * message itself, or, after an earlier stateful step, that step's result in
 * the feed's shape (`data` is the result, `queue` is null, `partition` the
 * key, `ctx` what the step said about it).
 */
export function messageOf(env, t) {
  if (!env.derived) return env.msg
  return {
    position: env.position ?? null,
    queue: null,
    partition: String(env.key),
    key: env.key,
    data: env.value,
    ctx: env.ctx ?? null,
    ts: Number.isFinite(t) ? new Date(t).toISOString() : null,
    derived: true
  }
}

/** The parts of a message a waiting state keeps; `data` is `keep(msg, value)` when the step has one. */
export function snapshotOf(msg, kept) {
  const snap = {
    position: msg.position,
    queue: msg.queue,
    partition: msg.partition,
    partitionId: msg.partitionId,
    offset: msg.offset,
    id: msg.id,
    transactionId: msg.transactionId,
    ts: msg.ts,
    createdAt: msg.createdAt ?? msg.ts,
    data: kept === undefined ? msg.data : kept.value
  }
  if (msg.derived) {
    snap.derived = true
    snap.key = msg.key
    if (msg.ctx) snap.ctx = msg.ctx
  }
  return snap
}

/** What a step keeps of a message: `{ value }` from its `keep`, or undefined for all of it. */
export async function keptOf(op, msg, value) {
  if (!op.keepFn) return undefined
  return { value: await op.keepFn(msg, value) }
}

export class PatternEngine {
  #op
  #state
  #heap = new DueHeap()

  constructor({ op, state }) {
    this.#op = op
    this.#state = state
    this.kind = op.patternKind
    state.classifyWith((k) => isBufferKey(k) ? 'held' : 'open')
  }

  /** Whether a message concerns the step (a start or then, a side): on event time only these are held. */
  async relevant(env, t) {
    const msg = messageOf(env, t)
    const op = this.#op
    if (op.patternKind === 'pattern') return (await op.then(msg)) || (await op.start(msg))
    return (await op.left(msg)) || (await op.right(msg))
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

  /** Pass every deadline at or before `t`: one timeout emit each when `onTimeout` is set. */
  advance(t, emits, limit = Infinity) {
    for (let n = 0; n < limit; n++) {
      this.#prune()
      if (!this.#heap.size || this.#heap.peek().due > t) return
      const { key: sk } = this.#heap.pop()
      const v = this.#state.get(sk)
      this.#state.delete(sk)
      if (!this.#op.onTimeout) continue
      const side = v.side === 'R' ? 'right' : v.side === 'L' ? 'left' : undefined
      emits.push({
        key: v.key,
        ctx: { kind: 'timeout', key: v.key, partition: v.key, side, deadline: v.due - 1, waitedFor: v.msg },
        state: v,
        time: v.due,
        eventTime: v.due - 1,
        compute: () => (side ? this.#op.onTimeout(clone(v.msg), side) : this.#op.onTimeout(clone(v.msg)))
      })
    }
  }

  /** One envelope at feed time `t`; a match is appended to `emits`. */
  async process(env, t, emits) {
    const msg = messageOf(env, t)
    const op = this.#op
    // The entity: key(msg, value), so a .flatMap() before can key each item
    // of a message on its own id. Asked only of a message that starts or
    // ends a wait: the others need not have the field.
    const keyOf = async () => String(op.keyFn ? await op.keyFn(msg, env.value) : env.key)
    if (op.patternKind === 'pattern') {
      const isThen = await op.then(msg)
      const isStart = await op.start(msg)
      if (!isThen && !isStart) return 'ok'
      const key = await keyOf()
      const sk = `p${SEP}${key}`
      const waiting = this.#state.get(sk)
      if (isThen && waiting) {
        this.#state.delete(sk)
        const snap = snapshotOf(msg, await keptOf(op, msg, env.value))
        emits.push({
          key,
          ctx: { kind: 'match', key, partition: key, deadline: waiting.due - 1 },
          state: waiting,
          eventTime: t,
          compute: op.onMatch
            ? () => op.onMatch(clone(waiting.msg), clone(snap))
            : () => ({ key, start: clone(waiting.msg), then: snap })
        })
        if (isStart) await this.#wait(sk, key, msg, env.value, t)
      } else if (isStart && (!waiting || op.onRepeat === 'restart')) {
        await this.#wait(sk, key, msg, env.value, t)
      }
      return 'ok'
    }
    // join: a message that is both left and right counts as left.
    const side = (await op.left(msg)) ? 'L' : (await op.right(msg)) ? 'R' : null
    if (!side) return 'ok'
    const key = await keyOf()
    const other = side === 'L' ? 'R' : 'L'
    const otherKey = `j${SEP}${other}${SEP}${key}`
    const waiting = this.#state.get(otherKey)
    if (waiting) {
      this.#state.delete(otherKey)
      const snap = snapshotOf(msg, await keptOf(op, msg, env.value))
      const left = side === 'L' ? snap : waiting.msg
      const right = side === 'R' ? snap : waiting.msg
      emits.push({
        key,
        ctx: { kind: 'match', key, partition: key, deadline: waiting.due - 1 },
        state: waiting,
        eventTime: t,
        compute: op.select ? () => op.select(clone(left), clone(right)) : () => ({ key, left: clone(left), right: clone(right) })
      })
      return 'ok'
    }
    const myKey = `j${SEP}${side}${SEP}${key}`
    if (!this.#state.get(myKey) || op.onRepeat === 'restart') await this.#wait(myKey, key, msg, env.value, t, side)
    return 'ok'
  }

  /** What waits for `key`, for a live read: `{ waiting: [{ side?, msg, deadline }] }`, or null. */
  inspect(key) {
    const k = String(key)
    const out = []
    for (const sk of this.kind === 'pattern' ? [`p${SEP}${k}`] : [`j${SEP}L${SEP}${k}`, `j${SEP}R${SEP}${k}`]) {
      const v = this.#state.get(sk)
      if (v) out.push(this.#view(v))
    }
    return out.length ? { key: k, waiting: out } : null
  }

  /** Every waiting entity, for a live read. */
  * list() {
    for (const [k, v] of this.#state.entries()) if (!isBufferKey(k)) yield { key: v.key, waiting: [this.#view(v)] }
  }

  #view(v) {
    const w = { msg: clone(v.msg), deadline: new Date(v.due - 1).toISOString() }
    if (v.side) w.side = v.side === 'L' ? 'left' : 'right'
    return w
  }

  async #wait(sk, key, msg, value, t, side) {
    const due = t + this.#op.withinMs + 1
    const snap = snapshotOf(msg, await keptOf(this.#op, msg, value))
    this.#state.set(sk, side ? { key, side, msg: snap, due } : { key, msg: snap, due })
    this.#heap.push(due, sk)
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
