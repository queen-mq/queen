/**
 * `.joinTable()` on the tenant feed: the latest row per key of a table queue,
 * looked up for every message that reaches the step.
 *
 * A table queue holds one message per change (a property's settings, a
 * customer's tier): its key (`key(row)`, the partition by default) and value
 * (`value(row)`, the payload by default); a value of null deletes the row.
 * Rows come from the same feed as the stream's messages, in one order, so a
 * message sees the table exactly as of its own position: every change
 * committed before it, none after (a change in the same transaction counts as
 * before). The table is the step's state, carried in the snapshots; a new
 * stream first reads the table queue from its beginning (`initial: 'all'`).
 *
 * State key `t\u001f<key>`; a value is `{ v, p }`: the row and its position.
 */
import { clone, messageOf } from './patterns.js'

const SEP = '\u001f'

export class TableStep {
  #op
  #state

  constructor({ op, state }) {
    this.#op = op
    this.#state = state
    this.kind = 'table'
  }

  get queues() {
    return this.#op.queues
  }

  get initial() {
    return this.#op.initial
  }

  get open() {
    return this.#state.size
  }

  /** A message of the table's queue: the row it sets, or deletes. */
  async applyRow(entry) {
    const op = this.#op
    const key = op.keyFn ? await op.keyFn(entry) : entry.partition
    if (key === null || key === undefined) return
    const value = op.valueFn ? await op.valueFn(entry) : entry.data
    const sk = `t${SEP}${String(key)}`
    if (value === null || value === undefined) this.#state.delete(sk)
    else this.#state.set(sk, { v: value, p: entry.position })
  }

  /** A stream message: the envelope with the row joined in, or none when the row is missing and missing is 'drop'. */
  async apply(env, t) {
    const op = this.#op
    const msg = messageOf(env, t)
    const key = await op.onFn(msg, env.value)
    const found = key === null || key === undefined ? undefined : this.#state.get(`t${SEP}${String(key)}`)
    const row = found ? clone(found.v) : undefined
    if (row === undefined && op.missing === 'drop') return []
    const value = op.selectFn ? await op.selectFn(env.value, row, msg) : { value: env.value, row }
    if (value === null || value === undefined) return []
    return [{ ...env, value }]
  }

  inspect(key) {
    const found = this.#state.get(`t${SEP}${String(key)}`)
    return found ? { key: String(key), row: clone(found.v), position: found.p } : null
  }

  * list() {
    for (const [k, v] of this.#state.entries()) yield { key: k.slice(2), row: clone(v.v), position: v.p }
  }
}
