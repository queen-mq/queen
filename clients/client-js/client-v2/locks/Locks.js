/**
 * Locks: a lock and a semaphore, as leases with a fencing token.
 *
 *     const lock = queen.lock('daily-report', { ttl: '30s' })
 *     if (!(await lock.acquire())) return          // somebody else has it
 *     try {
 *       await queen.transaction()
 *         .guard(lock)                             // commits only while the lock is ours
 *         .queue('reports').push([{ data: report }])
 *         .commit()
 *     } finally {
 *       await lock.release()
 *     }
 *
 * WHAT IT IS. A permit is one KV row in the namespace `queen-locks`, written
 * with a lifetime: `acquire` is a `putIfAbsent`, `renew` a `put` with `expect`,
 * `release` a `delete` with `expect`. The broker's `POST /api/v1/locks` does
 * that turning, so there is one implementation of it for every client. A lock
 * is the semaphore of one permit; `queen.semaphore(name, n)` is the same thing
 * with n.
 *
 * WHAT IT IS NOT: A MUTEX. A permit EXPIRES, and nobody tells its holder. A
 * process that is paused, partitioned or slow keeps running past its lifetime
 * while somebody else acquires. So the lock alone never makes two holders
 * impossible; what makes their WORK exclusive is the token:
 *
 *   * inside Queen, `.guard(lock)` on a transaction: the acks, pushes, KV
 *     writes and timers of the step commit only if the permit is still this
 *     holder's, in the same log entry. A holder that was replaced commits
 *     nothing.
 *   * outside Queen, `lock.token`: a number that only rises on a lock. A
 *     resource that remembers the highest token it has accepted and refuses a
 *     lower one (`WHERE fence <= $token`) refuses the holder that was
 *     replaced. Accept an EQUAL one: a holder writes many times with one token.
 *
 * Work that goes through neither is protected only by the lifetime being
 * longer than the work, which is a hope, not a guarantee.
 *
 * THE TOKEN CHANGES AT EVERY RENEW. A renew rewrites the row, so the broker
 * answers a new token and the one before stops working. This handle keeps the
 * current one: read `lock.token` and `lock.guard()` when you use them, never
 * hold a copy across an `await`.
 *
 * THE OWNER is the holder's identity, minted here per handle. It is what makes
 * a call safe to send again when its answer was lost: the broker answers the
 * permit the first attempt took. Two handles with one owner are one holder —
 * pass your own only if that is what you mean.
 */

import os from 'node:os'
import { randomBytes } from 'node:crypto'

import * as logger from '../utils/logger.js'
import { parseDurationMs } from '../kv/expiry.js'

/** `.code` of the error a guarded step throws when its lock is not held. */
export const LOCK_NOT_HELD = 'LOCK_NOT_HELD'

const NAME_MAX_BYTES = 256
const OWNER_MAX_BYTES = 256
const LIMIT_MAX = 1024

function requireName(name) {
  // The broker's rule, checked here so the mistake surfaces at the call: no
  // '#', which sits between a name and its slot in the row's key.
  if (typeof name !== 'string' || name.length === 0 || Buffer.byteLength(name) > NAME_MAX_BYTES ||
      /[\u0000-\u001f\u007f-\u009f#]/.test(name)) {
    throw new Error(
      `lock: a name is a non-empty string of at most ${NAME_MAX_BYTES} bytes, without control characters ` +
      `and without '#' — got ${JSON.stringify(name)}`
    )
  }
  return name
}

/**
 * The lifetime, in whole seconds, rounded UP. `ttl` is a duration string
 * ('30s', '5m'), `ttlSeconds` a number. There is no `forever` and no default:
 * a lock that never expires is one nobody can take back from a dead holder.
 */
export function lockTtlSeconds(opts = {}) {
  if (opts.forever !== undefined) {
    throw new Error('lock: there is no `forever`. A lock takes a lifetime, and a holder that needs longer renews')
  }
  if (opts.ttl !== undefined && opts.ttlSeconds !== undefined) {
    throw new Error('lock: lifetime declared twice (ttl and ttlSeconds) — pick one')
  }
  const seconds = opts.ttl !== undefined ? Math.ceil(parseDurationMs(opts.ttl) / 1000) : opts.ttlSeconds
  if (!Number.isInteger(seconds) || seconds <= 0) {
    throw new Error(
      "lock: a lifetime is required — ttl: '30s' or ttlSeconds: 30 (a whole number of seconds above zero)"
    )
  }
  return seconds
}

function mintOwner() {
  const host = os.hostname().slice(0, 128)
  return `${host}:${process.pid}:${randomBytes(6).toString('hex')}`
}

function requireOwner(owner) {
  if (typeof owner !== 'string' || owner.length === 0 || Buffer.byteLength(owner) > OWNER_MAX_BYTES ||
      /[\u0000-\u001f\u007f-\u009f]/.test(owner)) {
    throw new Error(`lock: owner is a non-empty string of at most ${OWNER_MAX_BYTES} bytes, without control characters`)
  }
  return owner
}

const sleep = (ms, signal) => new Promise((resolve) => {
  if (signal?.aborted) return resolve()
  const t = setTimeout(done, ms)
  function done() {
    signal?.removeEventListener('abort', done)
    clearTimeout(t)
    resolve()
  }
  signal?.addEventListener('abort', done, { once: true })
})

// ---------------------------------------------------------------------------
// The wire: POST /api/v1/locks
// ---------------------------------------------------------------------------

/**
 * The four operations as the broker speaks them, with no state kept here.
 * `queen.lock()` is what most code wants; this is for a caller that keeps the
 * token itself, and for `get`.
 *
 * As on the KV routes, the HTTP status says how the CALL went and never what
 * an operation answered: a lock held by somebody else is a 200 with
 * `acquired: false`. Every result is an OBJECT and objects are truthy — read
 * the field.
 */
export class Locks {
  #httpClient
  #held = new Set()

  constructor(httpClient) {
    this.#httpClient = httpClient
  }

  /**
   * Several operations in one call, each on a different lock: one result per
   * operation, in order. They are independent — nothing here is
   * all-or-nothing.
   */
  async batch(operations) {
    if (!Array.isArray(operations) || operations.length === 0) {
      throw new Error('locks: batch needs a non-empty array of operations')
    }
    logger.log('Locks.batch', { count: operations.length, ops: operations.map(o => o.op) })
    let body
    try {
      body = await this.#httpClient.post('/api/v1/locks', { operations })
    } catch (error) {
      // These routes put the code in `error`, which HttpClient maps onto the
      // message: mirror it on `.code` so nobody branches on prose (as Kv does).
      if (error && !error.code && typeof error.message === 'string') error.code = error.message
      logger.error('Locks.batch', { error: error.message, status: error.status, code: error.code })
      throw error
    }
    const results = body && Array.isArray(body.results) ? body.results : null
    if (!results || results.length !== operations.length) {
      throw new Error(
        `locks: expected {"results":[...]} with ${operations.length} element(s), got ` +
        (results ? `${results.length}` : 'another envelope')
      )
    }
    return results
  }

  async #one(operation, flag) {
    const [result] = await this.batch([operation])
    if (flag && typeof result[flag] !== 'boolean') {
      throw new Error(`locks: ${operation.op} result carries no \`${flag}\` field; refusing to guess the verdict`)
    }
    return result
  }

  /**
   * Take a permit: `{acquired, slot, token, owner, guard, already?}`, or
   * `{acquired: false, reason: 'held' | 'contended', holders}`.
   *
   * `limit` above 1 makes it a semaphore of that many permits. Every caller
   * of one name passes the same limit; it is stored nowhere.
   */
  async acquire(name, opts = {}) {
    const op = { op: 'acquire', name: requireName(name), ttlSeconds: lockTtlSeconds(opts) }
    if (opts.owner !== undefined && opts.owner !== null) op.owner = requireOwner(opts.owner)
    if (opts.limit !== undefined) {
      if (!Number.isInteger(opts.limit) || opts.limit < 1 || opts.limit > LIMIT_MAX) {
        throw new Error(`lock: limit is a whole number from 1 (a lock) to ${LIMIT_MAX}`)
      }
      op.limit = opts.limit
    }
    return this.#one(op, 'acquired')
  }

  /**
   * Extend a permit: `{renewed, slot, token, guard}` with a NEW token, or
   * `{renewed: false, reason: 'lost', holders}`.
   */
  async renew(name, opts = {}) {
    const op = { op: 'renew', name: requireName(name), token: opts.token, ttlSeconds: lockTtlSeconds(opts) }
    if (!Number.isSafeInteger(op.token) || op.token <= 0) {
      throw new Error('lock: renew needs the token of the permit (the one the last acquire or renew answered)')
    }
    if (opts.slot !== undefined) op.slot = opts.slot
    if (opts.owner !== undefined && opts.owner !== null) op.owner = requireOwner(opts.owner)
    return this.#one(op, 'renewed')
  }

  /** Give a permit back: `{released}`; `false` with `reason: 'lost'` when the token is no longer the row's. */
  async release(name, opts = {}) {
    const op = { op: 'release', name: requireName(name), token: opts.token }
    if (!Number.isSafeInteger(op.token) || op.token <= 0) {
      throw new Error('lock: release needs the token of the permit (the one the last acquire or renew answered)')
    }
    if (opts.slot !== undefined) op.slot = opts.slot
    return this.#one(op, 'released')
  }

  /** Who holds it: `{held, holders: [{slot, owner, token, since, expiresAt, renewedAt}]}`. */
  async get(name) {
    return this.#one({ op: 'get', name: requireName(name) }, 'held')
  }

  // ---- the handles of this client, for close() -------------------------------

  /** @internal */
  _track(lock, held) {
    if (held) this.#held.add(lock)
    else this.#held.delete(lock)
  }

  /**
   * Give back every permit this client's handles hold, best effort. Called by
   * `queen.close()`: a permit left behind is only a wait of one lifetime for
   * the next holder, never a leak.
   * @internal
   */
  async releaseAll() {
    const held = [...this.#held]
    await Promise.all(held.map(lock => lock.release().catch(() => false)))
    return held.length
  }
}

// ---------------------------------------------------------------------------
// The handle
// ---------------------------------------------------------------------------

/**
 * One holder's hold on one lock (or one permit of a semaphore).
 *
 * It keeps the token current, renews in the background (every third of the
 * lifetime, unless `autoRenew: false`), and says when the permit is gone:
 * `lock.signal` aborts and `onLost` handlers run. "Gone" is the broker saying
 * so, or the lifetime passing on THIS machine's clock with no renew having
 * succeeded — a client that cannot reach the broker must assume the worst.
 */
export class Lock {
  #locks
  #name
  #limit
  #owner
  #ttlSeconds
  #autoRenew
  #renewEveryMs
  #retry

  #token = null
  #slot = null
  // The broker's own `guard` of the current lease period, kept as answered:
  // where a permit's row lives is the broker's rule and is written once, there.
  #guard = null
  #validUntil = 0
  #abort = new AbortController()
  #lostHandlers = []
  #timer = null
  #renewing = null

  constructor(locks, name, opts = {}) {
    this.#locks = locks
    this.#name = requireName(name)
    this.#ttlSeconds = lockTtlSeconds(opts)
    this.#limit = opts.limit ?? 1
    if (!Number.isInteger(this.#limit) || this.#limit < 1 || this.#limit > LIMIT_MAX) {
      throw new Error(`lock: limit is a whole number from 1 (a lock) to ${LIMIT_MAX}`)
    }
    this.#owner = opts.owner !== undefined && opts.owner !== null ? requireOwner(opts.owner) : mintOwner()
    this.#autoRenew = opts.autoRenew !== false
    const every = opts.renewEvery !== undefined ? parseDurationMs(opts.renewEvery) : (this.#ttlSeconds * 1000) / 3
    if (!(every > 0) || every >= this.#ttlSeconds * 1000) {
      throw new Error('lock: renewEvery must be shorter than the lifetime, or the permit expires between renews')
    }
    this.#renewEveryMs = every
    // How a waiting acquire comes back: first after `min`, then 1.5x each time
    // up to `max`, each wait shortened by a random quarter so a crowd spreads.
    this.#retry = { min: opts.retryMinMs ?? 100, max: opts.retryMaxMs ?? 1000 }
  }

  get name() { return this.#name }
  get owner() { return this.#owner }
  get limit() { return this.#limit }
  get ttlSeconds() { return this.#ttlSeconds }

  /**
   * Whether this handle holds a permit, as far as it can know: the broker
   * granted or renewed it, and its lifetime has not run out on this machine's
   * clock. `true` here is a belief with a deadline, not a proof — the proof
   * is the guard on the transaction.
   */
  get held() {
    return this.#token !== null && Date.now() < this.#validUntil
  }

  /** The fencing token of the current lease period; `null` when not held. */
  get token() { return this.held ? this.#token : null }

  /** The semaphore slot this handle holds (0 for a lock); `null` when not held. */
  get slot() { return this.held ? this.#slot : null }

  /**
   * Epoch milliseconds, on this machine's clock, past which the permit must
   * be considered gone unless a renew succeeds first. Counted from when the
   * request was SENT, so it is never later than the broker's own deadline.
   */
  get validUntil() { return this.held ? this.#validUntil : 0 }

  /** Aborts when the permit is lost. A release is not a loss and does not abort it. */
  get signal() { return this.#abort.signal }

  /** Run `fn(reason)` when the permit is lost: 'renew', 'guard', 'expired'. */
  onLost(fn) {
    this.#lostHandlers.push(fn)
    return this
  }

  /**
   * The KV operation that holds while the permit is this handle's: a `check`
   * of the permit's row at the current token, `required`. `.guard(lock)` on a
   * transaction adds it for you and follows a renew; use this one to put it in
   * a KV batch yourself, and take it at the moment you send.
   */
  guard() {
    if (!this.held) {
      const error = new Error(`lock: '${this.#name}' is not held; there is nothing to guard with`)
      error.code = LOCK_NOT_HELD
      throw error
    }
    return { ...this.#guard }
  }

  /**
   * Take the permit. Resolves `true` or `false` — a boolean, on purpose, so
   * `if (await lock.acquire())` means what it reads as.
   *
   * With `wait` (a duration string) it keeps trying until the permit is free
   * or the wait is over, coming back every 100 ms to 1 s with jitter. Without
   * it there is one attempt. `signal` gives the wait up early.
   */
  async acquire(opts = {}) {
    if (this.held) return true
    const waitMs = opts.wait !== undefined ? parseDurationMs(opts.wait) : 0
    const deadline = Date.now() + waitMs
    let pause = this.#retry.min
    for (;;) {
      const sentAt = Date.now()
      const r = await this.#locks.acquire(this.#name, {
        ttlSeconds: this.#ttlSeconds,
        owner: this.#owner,
        ...(this.#limit > 1 ? { limit: this.#limit } : {})
      })
      if (r.acquired) {
        this.#take(r, sentAt)
        logger.log('Lock.acquire', { name: this.#name, slot: r.slot, already: r.already === true })
        return true
      }
      const left = deadline - Date.now()
      if (left <= 0 || opts.signal?.aborted) return false
      const jittered = pause * (0.75 + Math.random() * 0.25)
      await sleep(Math.min(jittered, left), opts.signal)
      pause = Math.min(pause * 1.5, this.#retry.max)
    }
  }

  /**
   * Extend the lease now. `true`, with a new token in place, or `false`: the
   * permit is gone and the handle says so (`signal`, `onLost`). Rejects when
   * the broker could not be asked; the permit is then neither renewed nor
   * known lost, and its deadline stands.
   *
   * The background renewal calls this; call it yourself with
   * `autoRenew: false`, or before a long step.
   */
  async renew() {
    if (this.#renewing) return this.#renewing
    if (this.#token === null) return false
    this.#renewing = this.#renewOnce().finally(() => { this.#renewing = null })
    return this.#renewing
  }

  async #renewOnce() {
    const token = this.#token
    const sentAt = Date.now()
    const r = await this.#locks.renew(this.#name, {
      token,
      slot: this.#slot,
      ttlSeconds: this.#ttlSeconds,
      owner: this.#owner
    })
    // Released, or lost, while the renew was in flight: its answer is about a
    // permit this handle no longer has.
    if (this.#token !== token) return false
    if (!r.renewed) {
      this.#lose('renew')
      return false
    }
    this.#token = r.token
    this.#guard = r.guard
    this.#validUntil = sentAt + this.#ttlSeconds * 1000
    return true
  }

  /**
   * Give the permit back. `true` when the broker removed it; `false` when it
   * was not this handle's any more (expired, or taken over) or was never
   * held. Either way the handle holds nothing afterwards and can acquire again.
   */
  async release() {
    // A renew in flight owns the token until it answers.
    if (this.#renewing) await this.#renewing.catch(() => {})
    if (this.#token === null) return false
    const token = this.#token
    const slot = this.#slot
    this.#drop()
    const r = await this.#locks.release(this.#name, { token, slot })
    logger.log('Lock.release', { name: this.#name, released: r.released })
    return r.released === true
  }

  /**
   * Acquire, run `fn(lock)`, release — whatever `fn` does. Resolves
   * `{acquired: false}` when the permit could not be had (after `wait`, if
   * given), else `{acquired: true, value}` with what `fn` returned.
   *
   * It does not stop `fn` when the permit is lost; nothing can. Pass
   * `lock.signal` to whatever `fn` awaits, and guard what it commits.
   */
  async run(fn, opts = {}) {
    if (!(await this.acquire(opts))) return { acquired: false }
    try {
      return { acquired: true, value: await fn(this) }
    } finally {
      await this.release().catch((error) => {
        logger.error('Lock.run', { name: this.#name, error: error.message, note: 'release failed; the permit expires by itself' })
      })
    }
  }

  // ---- used by TransactionBuilder.guard --------------------------------------

  /** Resolves once no renew is in flight: the token is then the current one. @internal */
  async _settled() {
    if (this.#renewing) await this.#renewing.catch(() => {})
  }

  /** The broker said the permit is not this handle's. @internal */
  _lost(reason) {
    if (this.#token !== null) this.#lose(reason)
  }

  // ---- state ------------------------------------------------------------------

  #take(result, sentAt) {
    this.#token = result.token
    this.#slot = result.slot
    this.#guard = result.guard
    this.#validUntil = sentAt + this.#ttlSeconds * 1000
    // A handle that lost a permit earlier gets a fresh signal with the new one.
    if (this.#abort.signal.aborted) this.#abort = new AbortController()
    this.#locks._track(this, true)
    this.#schedule()
  }

  #drop() {
    this.#token = null
    this.#slot = null
    this.#guard = null
    this.#validUntil = 0
    if (this.#timer) clearTimeout(this.#timer)
    this.#timer = null
    this.#locks._track(this, false)
  }

  #lose(reason) {
    logger.warn('Lock.lost', { name: this.#name, owner: this.#owner, reason })
    this.#drop()
    this.#abort.abort(Object.assign(new Error(`lock '${this.#name}' was lost (${reason})`), { code: LOCK_NOT_HELD }))
    for (const fn of this.#lostHandlers) {
      try { fn(reason) } catch (error) { logger.error('Lock.onLost', { error: error.message }) }
    }
  }

  /**
   * The background renewal, and the deadline. One timer: it fires when the
   * next renew is due, or — with `autoRenew: false`, or after renews that
   * could not reach the broker — when the lifetime runs out.
   */
  #schedule() {
    if (this.#timer) clearTimeout(this.#timer)
    const untilDead = this.#validUntil - Date.now()
    const delay = this.#autoRenew ? Math.min(this.#renewEveryMs, Math.max(untilDead, 0)) : Math.max(untilDead, 0)
    this.#timer = setTimeout(() => this.#tick(), delay)
    // A held lock must not keep a finished process alive.
    this.#timer.unref?.()
  }

  async #tick() {
    if (this.#token === null) return
    if (Date.now() >= this.#validUntil) {
      this.#lose('expired')
      return
    }
    if (this.#autoRenew) {
      try {
        await this.renew()
      } catch (error) {
        // Could not ask. Not a loss yet: try again sooner, until the deadline.
        logger.warn('Lock.renew', { name: this.#name, error: error.message, status: error.status })
        if (this.#token !== null) {
          if (this.#timer) clearTimeout(this.#timer)
          const left = this.#validUntil - Date.now()
          this.#timer = setTimeout(() => this.#tick(), Math.max(Math.min(left / 4, this.#renewEveryMs), 50))
          this.#timer.unref?.()
        }
        return
      }
    }
    if (this.#token !== null) this.#schedule()
  }
}
