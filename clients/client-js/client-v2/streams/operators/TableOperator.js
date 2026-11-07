/**
 * TableOperator — `.joinTable()`: the latest row per key of a table queue,
 * joined into every message. Feed sources only. See runtime/feed/tables.js.
 *
 *   Stream.fromFeed(queen, { queues: ['bookings'] })
 *     .joinTable({
 *       queue: 'property-settings',            // one message per change of a property
 *       on: (m) => m.data.propertyId,          // the row a booking looks up
 *       select: (booking, settings) => ({ ...booking, currency: settings?.currency })
 *     })
 *
 * key(row) is a row's key (default: its partition), value(row) what the table
 * keeps (default: its payload; null deletes the row). missing: 'keep' (the
 * default: select gets undefined) or 'drop'. initial: 'all' (a new stream
 * first reads the table queue from its beginning) or 'now'.
 */
function requireFn(fn, name) {
  if (typeof fn !== 'function') throw new Error(`joinTable: ${name} must be a function`)
  return fn
}

function optionalFn(fn, name) {
  if (fn === undefined || fn === null) return null
  return requireFn(fn, name)
}

export class TableOperator {
  constructor(opts = {}) {
    if (!opts || typeof opts !== 'object') throw new Error('joinTable: pass an options object')
    const queues = Array.isArray(opts.queue) ? opts.queue : [opts.queue]
    if (!queues.length || queues.some((q) => typeof q !== 'string' || q.length === 0 || /[*?]/.test(q))) {
      throw new Error("joinTable: queue is the table queue's name (or a list of names, no patterns)")
    }
    this.kind = 'table'
    this.queues = [...new Set(queues)]
    this.onFn = requireFn(opts.on, 'on')
    this.keyFn = optionalFn(opts.key, 'key')
    this.valueFn = optionalFn(opts.value, 'value')
    this.selectFn = optionalFn(opts.select, 'select')
    this.missing = opts.missing ?? 'keep'
    if (this.missing !== 'keep' && this.missing !== 'drop') {
      throw new Error(`joinTable: missing is 'keep' or 'drop', got ${JSON.stringify(opts.missing)}`)
    }
    this.initial = opts.initial ?? 'all'
    if (this.initial !== 'all' && this.initial !== 'now') {
      throw new Error(`joinTable: initial is 'all' (read the table queue from its beginning) or 'now', got ${JSON.stringify(opts.initial)}`)
    }
    this.stepName = typeof opts.name === 'string' && opts.name.length ? opts.name : null
    this.config = { kind: 'table', queues: this.queues, key: !!this.keyFn, value: !!this.valueFn }
  }
}
