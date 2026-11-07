/**
 * The state of one stateful step of a feed stream: its open windows,
 * sessions, waiting messages or table rows, by state key, in memory.
 *
 * It is not written to KV at every commit. A commit writes the position; now
 * and then the runner writes a snapshot of every step's state (snapshot.js),
 * and a restart loads the last snapshot and reads the feed again from the
 * snapshot's position to the committed one, sending nothing it already sent.
 * The feed replays in one order with the times it had, so the state comes
 * back exactly as it was.
 *
 * Every change is recorded so that it can be undone: a function that throws
 * leaves no trace (its message is retried, skipped or dead-lettered against
 * the state from before it), and a commit that did not happen rolls back to
 * the last one that did.
 */

export class FeedState {
  #map = new Map()
  #undo = []
  // Anything set or deleted since the last snapshot: the next one has work.
  #changed = false
  // The keys set or deleted since the last snapshot: what a delta snapshot writes.
  #touched = new Set()
  // How many entries of each class the state holds (`classify`), kept as it
  // changes so a count costs the same at any size.
  #classify = null
  #counts = new Map()

  get size() {
    return this.#map.size
  }

  /** Something changed since the last commit. */
  get dirty() {
    return this.#undo.length > 0
  }

  /** Something changed since the last snapshot. */
  get changedSinceSnapshot() {
    return this.#changed
  }

  /** How many keys were set or deleted since the last snapshot (some may be back as they were). */
  get touchedSinceSnapshot() {
    return this.#touched.size
  }

  snapshotTaken() {
    this.#changed = false
    this.#touched.clear()
  }

  /**
   * Count the entries by class: `classify(key, value)` answers a class name
   * (`'open'`, `'held'`, ...) or null. The engine of the step says what its
   * keys are; `count(name)` then reads a number kept up to date.
   */
  classifyWith(fn) {
    this.#classify = typeof fn === 'function' ? fn : null
    this.#recount()
  }

  /** How many entries of class `name` the state holds. */
  count(name) {
    return this.#counts.get(name) || 0
  }

  get(key) {
    return this.#map.get(key)
  }

  has(key) {
    return this.#map.has(key)
  }

  * entries() {
    yield * this.#map.entries()
  }

  /** Store `value` (a fresh object: stored values are never changed in place). */
  set(key, value) {
    const had = this.#map.has(key)
    const prev = had ? this.#map.get(key) : undefined
    this.#undo.push({ key, had, prev })
    if (had) this.#bump(key, prev, -1)
    this.#map.set(key, value)
    this.#bump(key, value, 1)
    this.#changed = true
    this.#touched.add(key)
  }

  delete(key) {
    if (!this.#map.has(key)) return
    const prev = this.#map.get(key)
    this.#undo.push({ key, had: true, prev })
    this.#bump(key, prev, -1)
    this.#map.delete(key)
    this.#changed = true
    this.#touched.add(key)
  }

  /** A point to roll back to. */
  mark() {
    return this.#undo.length
  }

  rollbackTo(mark) {
    while (this.#undo.length > mark) {
      const u = this.#undo.pop()
      if (this.#map.has(u.key)) this.#bump(u.key, this.#map.get(u.key), -1)
      if (u.had) {
        this.#map.set(u.key, u.prev)
        this.#bump(u.key, u.prev, 1)
      } else {
        this.#map.delete(u.key)
      }
      // The key stays touched: a delta writes it as it is now, which is right
      // whether or not it went back to what the snapshot holds.
    }
  }

  /** Back to the state of the last commit. */
  discard() {
    this.rollbackTo(0)
  }

  /** The changes are committed: they are the new starting point. */
  committed() {
    this.#undo = []
  }

  /** Every entry, for a snapshot: `[[key, value], ...]`. */
  dump() {
    return [...this.#map.entries()]
  }

  /**
   * What a delta snapshot writes: each key touched since the last snapshot,
   * as `[key, value]` when it is there and `[key, undefined]` when it is gone.
   */
  * touched() {
    for (const key of this.#touched) yield [key, this.#map.get(key)]
  }

  /** Replace everything with a snapshot's entries; nothing to undo. */
  load(entries) {
    this.#map = new Map(Array.isArray(entries) ? entries : [])
    this.#undo = []
    this.#changed = false
    this.#touched.clear()
    this.#recount()
  }

  /** A load in pieces: start empty, then `loadSet` / `loadDelete` each piece, then `loaded()`. */
  loadBegin() {
    this.#map = new Map()
    this.#undo = []
    this.#counts = new Map()
  }

  loadSet(key, value) {
    if (this.#map.has(key)) this.#bump(key, this.#map.get(key), -1)
    this.#map.set(key, value)
    this.#bump(key, value, 1)
  }

  loadDelete(key) {
    if (!this.#map.has(key)) return
    this.#bump(key, this.#map.get(key), -1)
    this.#map.delete(key)
  }

  /** The pieces are in: the state is as the snapshot holds it, nothing to undo or to write. */
  loaded() {
    this.#undo = []
    this.#changed = false
    this.#touched.clear()
  }

  /** Start empty (a reset): the next snapshot writes the empty state. */
  wipe() {
    this.#map = new Map()
    this.#undo = []
    this.#changed = true
    this.#touched.clear()
    this.#counts = new Map()
  }

  #bump(key, value, d) {
    if (!this.#classify) return
    const c = this.#classify(key, value)
    if (c === null || c === undefined) return
    this.#counts.set(c, (this.#counts.get(c) || 0) + d)
  }

  #recount() {
    this.#counts = new Map()
    if (!this.#classify) return
    for (const [k, v] of this.#map) this.#bump(k, v, 1)
  }
}

/**
 * The messages an event-time step holds until its watermark reaches them
 * (clock.js) are entries of the step's state under this prefix; the engines
 * leave them alone.
 */
export const BUFFER_PREFIX = 'b\u001f'

/** Entries indexed between two turns of the event loop when a loaded state is indexed again. */
export const REBUILD_BATCH = 50_000
/** Windows, deadlines or runs closed in one go; then the results run, then the next batch. */
export const CLOSE_BATCH = 2_000

export function isBufferKey(key) {
  return typeof key === 'string' && key.startsWith(BUFFER_PREFIX)
}

/** `{ due, key }` before another: the earlier due, then the smaller key (so ties close in one order every time). */
function before(x, y) {
  return x.due < y.due || (x.due === y.due && x.key < y.key)
}

/** A min-heap of `{ due, key }`: the next window or deadline to close. */
export class DueHeap {
  #a = []

  get size() { return this.#a.length }

  clear() { this.#a = [] }

  peek() { return this.#a[0] }

  push(due, key) {
    const a = this.#a
    a.push({ due, key })
    let i = a.length - 1
    while (i > 0) {
      const parent = (i - 1) >> 1
      if (!before(a[i], a[parent])) break
      ;[a[parent], a[i]] = [a[i], a[parent]]
      i = parent
    }
  }

  pop() {
    const a = this.#a
    const top = a[0]
    const last = a.pop()
    if (a.length > 0) {
      a[0] = last
      let i = 0
      for (;;) {
        const l = 2 * i + 1
        const r = l + 1
        let m = i
        if (l < a.length && before(a[l], a[m])) m = l
        if (r < a.length && before(a[r], a[m])) m = r
        if (m === i) break
        ;[a[m], a[i]] = [a[i], a[m]]
        i = m
      }
    }
    return top
  }
}

/**
 * FNV-1a, 32 bit, over the UTF-8 bytes of `text`: the hash the broker picks a
 * shard by (feed.rs `fnv1a32`), so both sides agree on where a partition is.
 */
export function fnv1a32(text) {
  const bytes = Buffer.from(String(text), 'utf8')
  let h = 0x811c9dc5
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i]
    h = Math.imul(h, 0x01000193)
  }
  return h >>> 0
}

/** The shard of `count` that a key (a partition name, by default) belongs to. */
export function shardOf(key, count) {
  return fnv1a32(key) % Math.max(1, count)
}
