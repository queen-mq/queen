/**
 * CollectOperator — `.join({ sides })` and `.gather()`: the parts of one
 * entity, collected until all are in or the deadline passes. Feed sources
 * only. See runtime/feed/collect.js for the rules.
 *
 *   .join({
 *     sides: { order: (m) => m.queue === 'orders', paid: (m) => m.queue === 'payments', sent: (m) => m.queue === 'shipments' },
 *     within: '2d',
 *     select: (got, key) => ({ order: key, carrier: got.sent.data.carrier })
 *   })
 *
 *   .gather({
 *     steps: ['payments', 'stock', 'shipping'],   // queue names, or { name: (msg, value) => bool }
 *     key: (m) => m.data.orderId,
 *     within: '5m',
 *     onComplete: (got, key) => ({ order: key, ready: true }),
 *     onTimeout: (got, missing, key) => ({ order: key, missing })
 *   })
 *
 *   .gather({ count: (m) => m.data.parts, key: (m) => m.data.batchId, within: '10m', ... })
 *
 * Sides and steps are names with a test each (a queue name stands for
 * `(m) => m.queue === name`); a message is the part of the first test it
 * passes. `keep(msg, value)` says what of a message to keep while it waits.
 * `eventTime`, `allowedLateness`, `onLate` and `idle` put the deadline on the
 * messages' own times (stepTimeOptions in _windowCommon.js).
 */
import { parseDurationMs } from '../../kv/expiry.js'
import { stepTimeOptions } from './_windowCommon.js'

function requireFn(fn, name, what) {
  if (typeof fn !== 'function') throw new Error(`${what}: ${name} must be a function`)
  return fn
}

function optionalFn(fn, name, what) {
  if (fn === undefined || fn === null) return null
  return requireFn(fn, name, what)
}

function partsOf(spec, name, what) {
  const list = []
  const add = (partName, test) => {
    if (typeof partName !== 'string' || partName.length === 0) throw new Error(`${what}: every ${name} has a name`)
    if (list.some((p) => p.name === partName)) throw new Error(`${what}: ${name} ${JSON.stringify(partName)} is named twice`)
    if (typeof test === 'string') {
      const queue = test
      list.push({ name: partName, pred: (m) => m.queue === queue })
    } else {
      list.push({ name: partName, pred: requireFn(test, `${name} ${partName}`, what) })
    }
  }
  if (Array.isArray(spec)) {
    for (const q of spec) add(q, q)
  } else if (spec && typeof spec === 'object') {
    for (const [k, v] of Object.entries(spec)) add(k, v)
  } else {
    throw new Error(`${what}: ${name} is a list of queue names or { name: test }`)
  }
  return list
}

export class CollectOperator {
  /**
   * @param {'join'|'gather'} collectKind
   * @param {object} opts
   */
  constructor(collectKind, opts = {}) {
    const what = collectKind
    if (!opts || typeof opts !== 'object') throw new Error(`${what}: pass an options object`)
    if (typeof opts.within !== 'string') throw new Error(`${what}: within is a duration string, e.g. within: '15m'`)
    this.kind = 'collect'
    this.collectKind = collectKind
    this.withinMs = parseDurationMs(opts.within)
    this.keyFn = optionalFn(opts.key, 'key', what)
    this.keepFn = optionalFn(opts.keep, 'keep', what)
    this.onTimeout = optionalFn(opts.onTimeout, 'onTimeout', what)
    this.onRepeat = opts.onRepeat ?? 'keep'
    if (this.onRepeat !== 'keep' && this.onRepeat !== 'latest') {
      throw new Error(`${what}: onRepeat is 'keep' (the first message of a part stays) or 'latest', got ${JSON.stringify(opts.onRepeat)}`)
    }
    this.stepName = typeof opts.name === 'string' && opts.name.length ? opts.name : null
    if (collectKind === 'join') {
      this.parts = partsOf(opts.sides, 'side', what)
      if (this.parts.length < 2) throw new Error('join: sides names at least two')
      this.countFn = null
      this.onComplete = optionalFn(opts.select, 'select', what)
      this.remember = false
    } else {
      const hasSteps = opts.steps !== undefined
      const hasCount = opts.count !== undefined
      if (hasSteps === hasCount) throw new Error('gather: give steps (the parts by name) or count (how many parts a run has), one of the two')
      this.parts = hasSteps ? partsOf(opts.steps, 'step', what) : null
      if (this.parts && this.parts.length === 0) throw new Error('gather: steps names at least one')
      this.countFn = hasCount ? requireFn(opts.count, 'count', what) : null
      this.onComplete = optionalFn(opts.onComplete, 'onComplete', what)
      this.remember = true
    }
    this.operatorTag = `${what}:${this.withinMs}`
    this.config = {
      kind: what,
      within: this.withinMs,
      parts: this.parts ? this.parts.map((p) => p.name) : null,
      count: !!this.countFn,
      onRepeat: this.onRepeat,
      timeout: !!this.onTimeout
    }
    stepTimeOptions(this, opts, what)
  }
}
