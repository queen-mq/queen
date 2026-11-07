/**
 * ForeachBatchOperator — terminal at-least-once side effect, in batches
 * (`Stream.fromFeed` only).
 *
 * The runner keeps the results that reach the end of the chain and calls
 * `fn(items)` with up to `maxItems` of them: at the end of each read (by
 * default), or, with `maxWaitMillis`, once the batch is full or its first
 * item has waited that long. The position moves only after `fn` returns, in
 * the commit right after it: a crash before that delivers the batch again,
 * with the same ids.
 *
 * User-fn signature: `(items) => void|Promise<void>`, each item
 * `{ value, ctx, id }`: the value and context `.foreach()` would get, and an
 * id that is the same on every delivery of that result (a key for an upsert).
 */

const DEFAULT_MAX_ITEMS = 1000
const OPTIONS = new Set(['maxItems', 'maxWaitMillis'])

export class ForeachBatchOperator {
  constructor(fn, opts = {}) {
    if (typeof fn !== 'function') {
      throw new Error('.foreachBatch(fn, options): fn is a function (items) => ..., each item { value, ctx, id }')
    }
    if (opts === null || typeof opts !== 'object' || Array.isArray(opts)) {
      throw new Error('.foreachBatch(fn, options): options is an object { maxItems, maxWaitMillis }')
    }
    for (const k of Object.keys(opts)) {
      if (!OPTIONS.has(k)) throw new Error(`.foreachBatch(fn, options): unknown option ${JSON.stringify(k)} (maxItems, maxWaitMillis)`)
    }
    const maxItems = opts.maxItems ?? DEFAULT_MAX_ITEMS
    const maxWaitMillis = opts.maxWaitMillis ?? 0
    if (!Number.isInteger(maxItems) || maxItems < 1) {
      throw new Error(`.foreachBatch(fn, options): maxItems is a whole number from 1, got ${JSON.stringify(opts.maxItems)}`)
    }
    if (!Number.isInteger(maxWaitMillis) || maxWaitMillis < 0) {
      throw new Error(`.foreachBatch(fn, options): maxWaitMillis is a whole number from 0, got ${JSON.stringify(opts.maxWaitMillis)}`)
    }
    this.kind = 'foreachBatch'
    this.fn = fn
    this.maxItems = maxItems
    this.maxWaitMillis = maxWaitMillis
    this.config = { kind: 'foreachBatch', maxItems, maxWaitMillis }
  }
}
