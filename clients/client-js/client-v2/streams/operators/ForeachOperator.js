/**
 * ForeachOperator — terminal at-least-once side-effect.
 *
 * The runtime invokes the user fn for each emitted record after state has
 * been computed but BEFORE the cycle commits the source ack. If `fn`
 * resolves successfully for all records, the cycle commits as normal. If
 * `fn` throws, the cycle is aborted and the messages will be redelivered
 * by Queen's lease/retry mechanism.
 *
 * User-fn signature: `(value, ctx?) => void|Promise<void>`
 *   value: the post-reducer value (e.g. an aggregate { count, sum, ... })
 *   ctx:   { partition, partitionId, key, windowKey, windowStart, windowEnd }
 *
 * For exactly-once external effects, use `.to(queue)` and have a separate
 * worker drain the sink queue with a transactional ack.
 */
export class ForeachOperator {
  constructor(fn) {
    this.kind = 'foreach'
    this.fn = fn
    this.config = { kind: 'foreach' }
  }

  async runEffects(entries) {
    for (const entry of entries) {
      await this.fn(entry.value, entry.ctx)
    }
  }
}
