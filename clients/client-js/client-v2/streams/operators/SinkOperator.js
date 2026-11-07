/**
 * SinkOperator — terminal `.to(...)`. Records flowing into the sink are
 * pushed onto Queen queues as part of the cycle commit.
 *
 * The target is one of:
 *   - a queue: `queen.queue('orders-it')` (or its name)
 *   - several queues: `[queen.queue('audit'), queen.queue('orders-it')]`;
 *     every record goes to each of them
 *   - a function `(value, msg) => target`, called per record, that answers a
 *     queue (builder or name), `{ queue, partition }`, a list of those, or
 *     null to send the record nowhere
 *
 * Several queues and functions run on a feed source (`Stream.fromFeed`),
 * whose commit can push to any queue; a queue source sinks to one queue.
 *
 * The runtime is responsible for shaping each sink push item:
 *   { queue, partition?, payload }
 *
 * If the sink has a partition function (opts.partition), it's called with
 * the value to derive the partition name; otherwise the source partition
 * name is reused (preserves entity-level co-partitioning by default).
 */
export class SinkOperator {
  constructor(target, opts = {}) {
    this.kind = 'sink'
    this.partitionResolver = opts.partition
    if (typeof target === 'function') {
      this.route = target
      this.queues = null
      this.queueName = null
      this.config = { kind: 'sink', route: true }
    } else if (Array.isArray(target)) {
      if (target.length === 0) throw new Error('.to([]): name at least one queue')
      this.route = null
      this.queues = [...new Set(target.map(extractQueueName))]
      this.queueName = this.queues.length === 1 ? this.queues[0] : null
      this.config = { kind: 'sink', queues: this.queues }
    } else {
      this.route = null
      this.queueName = extractQueueName(target)
      this.queues = [this.queueName]
      this.config = { kind: 'sink', queue: this.queueName }
    }
  }

  /** True when records may go to more than one queue (a function or a list). */
  get routes() {
    return this.route !== null || this.queues.length > 1
  }

  /**
   * Build sink push items from a list of {key, value, sourceMsg} entries.
   * Called by the Runner after upstream operators have produced final values.
   */
  buildPushItems(entries, sourcePartitionName) {
    return entries.map(({ value, sourceMsg }) => ({
      queue: this.queueName,
      partition: this._resolvePartition(value, sourcePartitionName),
      payload: payloadOf(value)
    }))
  }

  /**
   * Where one record goes: `[{ queue, partition }]`, empty to send it nowhere.
   * `partition` is the target's own, else the sink's `partition` option, else
   * `fallback` (the source partition, or the key of a window or a pattern).
   */
  async targetsFor(value, msg, fallback) {
    let raw
    if (this.route) {
      raw = await this.route(value, msg)
      if (raw === null || raw === undefined) return []
      if (!Array.isArray(raw)) raw = [raw]
    } else {
      raw = this.queues
    }
    const out = []
    for (const t of raw) {
      if (t === null || t === undefined) continue
      const own = t && typeof t === 'object' && typeof t.name !== 'string' && typeof t.queue === 'string'
      const queue = own ? requireQueue(t.queue) : extractQueueName(t)
      let partition = own && t.partition !== undefined && t.partition !== null ? t.partition : null
      if (partition === null) partition = await this._resolvePartitionAsync(value, fallback)
      out.push({ queue, partition: requirePartition(partition, queue) })
    }
    return out
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

  async _resolvePartitionAsync(value, fallback) {
    if (typeof this.partitionResolver === 'function') {
      return String(await this.partitionResolver(value))
    }
    if (typeof this.partitionResolver === 'string') return this.partitionResolver
    return fallback === undefined || fallback === null || fallback === '' ? 'Default' : String(fallback)
  }
}

/** A pushed payload is an object: anything else travels as `{ value }`. */
export function payloadOf(value) {
  return value === null || typeof value !== 'object' ? { value } : value
}

function requireQueue(name) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error(`.to(): a queue name is a non-empty string, got ${JSON.stringify(name)}`)
  }
  return name
}

function requirePartition(partition, queue) {
  const p = typeof partition === 'number' ? String(partition) : partition
  if (typeof p !== 'string' || p.length === 0) {
    throw new Error(`.to(): the partition for ${queue} is a non-empty string, got ${JSON.stringify(partition)}`)
  }
  return p
}

function extractQueueName(qb) {
  if (typeof qb === 'string') return requireQueue(qb)
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
