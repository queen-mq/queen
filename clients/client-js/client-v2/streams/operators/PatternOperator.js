/**
 * PatternOperator — `.pattern()` and `.join()`: two kinds of message for one
 * entity, in time. Runs on a feed source only (`Stream.fromFeed`), where every
 * queue shares one clock: the time the log committed each message. With
 * `eventTime` (or after a step on event time) the deadlines run on the
 * messages' own times instead: see stepTimeOptions in _windowCommon.js.
 *
 * pattern: a `start` message, then a `then` message within `within`.
 *   A `then` in time emits `onMatch(start, then)`; no `then` in time emits
 *   `onTimeout(start)` once the clock passes the deadline ("A and no B").
 *
 * join: a `left` and a `right` message, in either order, within `within`.
 *   The second one emits `select(left, right)`; one left alone emits
 *   `onTimeout(message, side)` at its deadline.
 *
 * The entity is `key(msg, value)`, else `.keyBy()`, else the partition name, so
 * the partition `order-9137` of `orders` and of `payments` is one entity.
 * `value` is the item a `.flatMap()` before the pattern made: a message that
 * carries several ids waits once for each. One
 * message waits per entity (per side, for a join): a second `start` while one
 * waits is ignored (`onRepeat: 'keep'`, the default, the deadline counts from
 * the first) or takes its place (`'restart'`, the deadline counts from the
 * latest). The waiting messages are the stream's state, kept in KV with its
 * position.
 *
 * Every function gets messages in the feed's shape (`queue`, `partition`,
 * `data`, `ts`, ...) and may be async. `onMatch`, `select` and `onTimeout`
 * return the value to send on; null or undefined sends nothing. After an
 * earlier stateful step, a message is that step's result: `data` is the
 * result, `partition` its key, `queue` null.
 *
 * `keep(msg, value)` keeps only what it answers (as the waiting message's
 * `data`) instead of the whole payload: less memory, smaller snapshots.
 */
import { parseDurationMs } from '../../kv/expiry.js'
import { stepTimeOptions } from './_windowCommon.js'

const REPEAT = new Set(['keep', 'restart'])

function requireFn(fn, name, what) {
  if (typeof fn !== 'function') throw new Error(`${what}: ${name} must be a function`)
  return fn
}

function optionalFn(fn, name, what) {
  if (fn === undefined || fn === null) return null
  return requireFn(fn, name, what)
}

export class PatternOperator {
  /**
   * @param {'pattern'|'join'} patternKind
   * @param {object} opts
   */
  constructor(patternKind, opts = {}) {
    const what = patternKind === 'join' ? 'join' : 'pattern'
    if (!opts || typeof opts !== 'object') throw new Error(`${what}: pass an options object`)
    if (typeof opts.within !== 'string') {
      throw new Error(`${what}: within is a duration string, e.g. within: '15m'`)
    }
    this.kind = 'pattern'
    this.patternKind = what
    this.withinMs = parseDurationMs(opts.within)
    this.keyFn = optionalFn(opts.key, 'key', what)
    // What a waiting message keeps: keep(msg, value) becomes its data (all of it by default).
    this.keepFn = optionalFn(opts.keep, 'keep', what)
    this.stepName = typeof opts.name === 'string' && opts.name.length ? opts.name : null
    this.onTimeout = optionalFn(opts.onTimeout, 'onTimeout', what)
    this.onRepeat = opts.onRepeat ?? 'keep'
    if (!REPEAT.has(this.onRepeat)) {
      throw new Error(`${what}: onRepeat is 'keep' or 'restart', got ${JSON.stringify(opts.onRepeat)}`)
    }
    if (what === 'pattern') {
      this.start = requireFn(opts.start, 'start', what)
      this.then = requireFn(opts.then, 'then', what)
      this.onMatch = optionalFn(opts.onMatch, 'onMatch', what)
    } else {
      this.left = requireFn(opts.left, 'left', what)
      this.right = requireFn(opts.right, 'right', what)
      this.select = optionalFn(opts.select, 'select', what)
    }
    this.operatorTag = `${what}:${this.withinMs}`
    this.config = {
      kind: what,
      within: this.withinMs,
      onRepeat: this.onRepeat,
      timeout: !!this.onTimeout
    }
    stepTimeOptions(this, opts, what)
  }
}
