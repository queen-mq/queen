/**
 * SinkOperator — terminal `.to(queue)`. Records flowing into the sink are
 * pushed onto the destination Queen queue as part of the cycle commit.
 *
 * The runtime is responsible for shaping each sink push item:
 *   { queue, partition?, payload }
 *
 * If the sink has a partition function (opts.partition), it's called with
 * the value to derive the partition name; otherwise the source partition
 * name is reused (preserves entity-level co-partitioning by default).
 */
export class SinkOperator {
  constructor(queueBuilder, opts = {}) {
    this.kind = 'sink'
    this.queueBuilder = queueBuilder
    this.queueName = extractQueueName(queueBuilder)
    this.partitionResolver = opts.partition
    this.config = { kind: 'sink', queue: this.queueName }
  }

  /**
   * Build sink push items from a list of {key, value, sourceMsg} entries.
   * Called by the Runner after upstream operators have produced final values.
   */
  buildPushItems(entries, sourcePartitionName) {
    return entries.map(({ value, sourceMsg }) => ({
      queue: this.queueName,
      partition: this._resolvePartition(value, sourcePartitionName),
      payload: value === null || typeof value !== 'object' ? { value } : value
    }))
  }

  _resolvePartition(value, sourcePartitionName) {
    if (typeof this.partitionResolver === 'function') {
      return String(this.partitionResolver(value))
    }
    if (typeof this.partitionResolver === 'string') {
      return this.partitionResolver
    }
    return sourcePartitionName || 'Default'
  }
}

function extractQueueName(qb) {
  // QueueBuilder.name (getter) is the canonical accessor since v0.14.x.
  // Test fakes and raw objects fall back to _queueName / queueName fields.
  if (qb && typeof qb.name === 'string' && qb.name.length > 0) return qb.name
  if (qb && typeof qb._queueName === 'string') return qb._queueName
  if (qb && typeof qb.queueName === 'string') return qb.queueName
  throw new Error(
    'Could not extract queue name from sink target. ' +
    'Pass either a queen.queue("name") builder or { queueName: "name" }.'
  )
}
