/**
 * Timers (PLAN_KV_TIMERS.md §4, §8.1, §9.6).
 *
 * A timer is a message you promise now and the broker delivers later, into a
 * real queue, through the real log. Four terminals, all EXPLICIT -- nothing on
 * this builder does anything until one of them is called:
 *
 *     await queen.timer('orders').key('order-9f1').delay('30s')
 *                .payload({ orderId: '9f1' }).schedule()
 *     await queen.timer('orders').key('order-9f1').cancel()
 *     await queen.timer('orders').key('order-9f1').peek()
 *     await queen.timer('orders').list({ limit: 50 })
 *
 * THE FOUR THINGS THIS FILE EXISTS TO GET RIGHT
 *
 * 1. CANCEL USES ITS OWN ROUTE, AND THAT IS NOT A DETAIL (§9.6).
 *    `DELETE /api/v1/timers/:queue/*timerKey` is the one route a proxy is
 *    forbidden to block; a cancel sent inside `POST /api/v1/timers` inherits
 *    the schedule's authorization instead. Since the FIRE never switches
 *    itself off, a tenant that can no longer cancel keeps producing messages it
 *    cannot stop -- a block there produces the exact opposite of its purpose.
 *    So `cancel()` on this builder always uses the DELETE route. (Inside a
 *    transaction it necessarily rides the bundle's array and inherits the
 *    bundle's fate; see TransactionBuilder, which says so where it happens.)
 *
 * 2. ONLY RELATIVE DURATIONS, IN MILLISECONDS (§4.2, §20.6). `delayMs`, never
 *    `delaySeconds` and never an absolute instant: `deliver_at` is computed in
 *    Postgres, so there is ONE clock and no broker's skew can enter. The rule
 *    of the product is "durations that can be sub-second are in milliseconds,
 *    the ones that cannot are in seconds" -- a 250 ms retry backoff is a real
 *    and central use of timers, which is why this wire is the millisecond one.
 *    A delay in the past is LEGAL and fires on the first cycle.
 *
 * 3. `deliverAt` IS "NOT BEFORE", NEVER "EXACTLY AT". The floor on this stack
 *    is a single hop (p50 ~10 ms, fsync ~4 ms) plus one sweep cycle.
 *
 * 4. THERE IS NO TOMBSTONE (§4.4). Once a timer has fired its row is gone, so a
 *    later cancel answers `absent` -- **which MAY mean it was already
 *    delivered**. `absent` carries `ok:false` for exactly that reason, and the
 *    response echoes the `txn` so the authority (the log, in the destination
 *    queue) can be consulted without a second API. Any saga that cancels a
 *    compensation timer must have the compensating consumer check the saga's
 *    KV state before compensating: otherwise "the timer went out 5 ms before
 *    the cancel" unwinds a booking that was already shipped.
 */

import * as logger from '../utils/logger.js'
import { generateUUID } from './QueueBuilder.js'
import { parseDurationMs } from '../kv/expiry.js'

/** JSON payloads are encoded for the caller; raw bytes are passed through. */
function encodePayload(payload) {
  if (payload === undefined) return undefined
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(payload)) {
    return payload.toString('base64')
  }
  if (payload instanceof Uint8Array) {
    return Buffer.from(payload).toString('base64')
  }
  return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64')
}

export class TimerBuilder {
  #httpClient
  #queue
  #sink
  #timerKey = null
  #partition = null
  #delayMs = null
  #payloadB64 = undefined
  #txn = null

  /**
   * `sink`, when present, receives the finished op instead of the network:
   * that is how the same builder serves `queen.timer(...)` and
   * `tx.timer(...)`, so the wire shape of a timer op is written once.
   */
  constructor(httpClient, queue, sink = null) {
    if (typeof queue !== 'string' || queue.length === 0) {
      throw new Error('timer: a queue name is required — a timer without a queue has nowhere to be delivered')
    }
    this.#httpClient = httpClient
    this.#queue = queue
    this.#sink = sink
  }

  /** The timer's identity inside its queue. `(queue, timerKey)` is the primary key. */
  key(timerKey) {
    if (typeof timerKey !== 'string' || timerKey.length === 0) {
      throw new Error('timer: timerKey must be a non-empty string')
    }
    this.#timerKey = timerKey
    return this
  }

  /** Destination partition of the delivered message. Defaults to 'Default'. */
  partition(name) {
    this.#partition = name
    return this
  }

  /** Fire no earlier than this many milliseconds from now. Negative is legal: it fires on the first cycle. */
  delayMs(ms) {
    if (typeof ms !== 'number' || !Number.isFinite(ms)) {
      throw new Error(`timer: delayMs must be a finite number of milliseconds, got ${JSON.stringify(ms)}`)
    }
    this.#delayMs = Math.round(ms)
    return this
  }

  /** The same delay as a duration string: '250ms', '30s', '2h'. Converted to delayMs here. */
  delay(duration) {
    this.#delayMs = Math.round(parseDurationMs(duration))
    return this
  }

  /** The message body. Any JSON value, or a Buffer/Uint8Array for raw bytes. */
  payload(payload) {
    this.#payloadB64 = encodePayload(payload)
    return this
  }

  /**
   * The transaction id the delivered message will carry. Minted when absent.
   *
   * §20.2: every reschedule overwrites it, because a rescheduled timer is a
   * NEW message -- and the corollary that must travel with it is that there is
   * no dedup net on the fire at all, so rescheduling or republishing a timer
   * that has already gone out produces a second message in the log and nothing
   * stops it.
   */
  txn(transactionId) {
    this.#txn = transactionId
    return this
  }

  // ------------------------------------------------------------ op shapes

  #scheduleOp() {
    if (!this.#timerKey) throw new Error('timer: key(...) is required to schedule')
    if (this.#delayMs === null) {
      throw new Error('timer: delayMs(...) or delay(...) is required — an absolute instant is not expressible on this wire')
    }
    if (this.#payloadB64 === undefined) throw new Error('timer: payload(...) is required to schedule')
    const op = {
      op: 'schedule',
      queue: this.#queue,
      timerKey: this.#timerKey
    }
    if (this.#partition !== null && this.#partition !== undefined) op.partition = this.#partition
    op.delayMs = this.#delayMs
    op.txn = this.#txn || generateUUID()
    op.payload = this.#payloadB64
    return op
  }

  #cancelOp() {
    if (!this.#timerKey) throw new Error('timer: key(...) is required to cancel')
    const op = { op: 'cancel', queue: this.#queue, timerKey: this.#timerKey }
    if (this.#txn) op.txn = this.#txn
    return op
  }

  #path(suffix = '') {
    return `/api/v1/timers/${encodeURIComponent(this.#queue)}${suffix}`
  }

  #keyPath() {
    return this.#path(`/${encodeURIComponent(this.#timerKey)}`)
  }

  #single(body, what) {
    if (!body || typeof body !== 'object' || !Array.isArray(body.results)) {
      throw new Error(`timer: unexpected ${what} response envelope — expected {"results":[...]}`)
    }
    if (body.results.length !== 1) {
      throw new Error(`timer: got ${body.results.length} results for 1 operation`)
    }
    return body.results[0]
  }

  // ------------------------------------------------------------ terminals

  /**
   * Schedule (or reschedule) this timer. An UPSERT on `(queue, timerKey)`, so
   * a retry after a client crash is safe by construction; the answer says
   * which it was, `status: 'scheduled' | 'rescheduled'`.
   *
   * A reschedule resets `attempts` and clears `last_error`: a rescheduled
   * timer is a new timer under an old name, and a freshly corrected payload
   * must not inherit the budget consumed by the one that was poisoning it.
   *
   * `too_late` (with `ok:false`) means the timer is already claimed by a
   * broker that is about to commit it. Bounded by the sweeper lease. The
   * remedy is a new key, or waiting for the delivery and acting on the message.
   */
  // NOT `async`, deliberately: with a sink this terminal is the transaction's
  // own chaining call and must hand back the TransactionBuilder itself, not a
  // promise of it, or `.timer(q)...schedule().commit()` stops being a chain.
  // The network path returns the promise, so `await ...schedule()` is
  // unchanged for the standalone caller.
  schedule() {
    if (this.#sink) return this.#sink(this.#scheduleOp())
    return this.#scheduleRemote()
  }

  // The op is built INSIDE the async function so that a shape error on the
  // network path is a rejected promise like every other failure of this
  // client, and not a synchronous throw that `.catch()` would miss. On the
  // transaction path it necessarily stays synchronous: there is no promise
  // there to reject.
  async #scheduleRemote() {
    const op = this.#scheduleOp()
    logger.log('TimerBuilder.schedule', { queue: this.#queue, timerKey: op.timerKey, delayMs: op.delayMs })
    const body = await this.#httpClient.post('/api/v1/timers', { operations: [op] })
    return this.#single(body, 'schedule')
  }

  /**
   * Cancel this timer, through the route that is never blockable (§9.6).
   *
   * Idempotent: `absent` answers `ok:false` and MAY MEAN ALREADY DELIVERED --
   * there is no tombstone. Pass `.txn(...)` and it comes back in the answer,
   * so the log can settle the question.
   */
  cancel() {
    if (this.#sink) return this.#sink(this.#cancelOp())
    return this.#cancelRemote()
  }

  async #cancelRemote() {
    const op = this.#cancelOp()
    logger.log('TimerBuilder.cancel', { queue: this.#queue, timerKey: op.timerKey })
    const query = op.txn ? `?txn=${encodeURIComponent(op.txn)}` : ''
    return this.#httpClient.delete(`${this.#keyPath()}${query}`)
  }

  /** Read one pending timer, payload included. A miss is `{found:false}` with HTTP 200, never a 404. */
  async peek() {
    if (this.#sink) throw new Error('timer: peek is a read, not a transaction operation — use queen.timer(...).peek()')
    if (!this.#timerKey) throw new Error('timer: key(...) is required to peek')
    logger.log('TimerBuilder.peek', { queue: this.#queue, timerKey: this.#timerKey })
    return this.#httpClient.get(this.#keyPath())
  }

  /**
   * List the pending timers of this queue: `{rows, truncated, nextAfter}`.
   *
   * The queue is mandatory and that is why it is a path segment rather than a
   * filter: a tenant-wide list would be a scan that an end user of the
   * customer could trigger. `after` is an exclusive keyset cursor.
   */
  async list(opts = {}) {
    if (this.#sink) throw new Error('timer: list is a read, not a transaction operation — use queen.timer(...).list()')
    const params = new URLSearchParams()
    if (opts.after !== undefined && opts.after !== null && opts.after !== '') params.append('after', opts.after)
    if (opts.limit !== undefined && opts.limit !== null) params.append('limit', String(opts.limit))
    const query = params.toString()
    logger.log('TimerBuilder.list', { queue: this.#queue, after: opts.after, limit: opts.limit })
    return this.#httpClient.get(this.#path(query ? `?${query}` : ''))
  }
}
