/**
 * Scheduled messages for one queue.
 *
 *     await queen.timer('orders').key(orderId).delay('30m')
 *                .payload({ orderId }).schedule()
 *     await queen.timer('orders').key(orderId).cancel()
 *
 * `deliverAt` is "not before", never "exactly at". A cancel that answers
 * `absent` means "no longer pending" and MAY MEAN ALREADY DELIVERED.
 */

import type { Duration, JsonObject, MessageHeaders, QueenHttpClient } from '../types.js'

/** `schedule()`: an upsert on `(queue, timerKey)`. */
export interface TimerScheduleResult {
  ok: boolean
  /** `scheduled`, `rescheduled`, or `too_late` with `ok: false`. */
  status: 'scheduled' | 'rescheduled' | 'too_late' | (string & {})
  txn?: string
  messageId?: string
  deliverAt?: string
  [key: string]: unknown
}

/** `cancel()`: idempotent; `absent` MAY MEAN ALREADY DELIVERED. */
export interface TimerCancelResult {
  ok: boolean
  status: 'cancelled' | 'absent' | (string & {})
  txn?: string
  [key: string]: unknown
}

/** `peek()`: a miss is `{ found: false }`. */
export interface TimerPeekResult {
  found: boolean
  [key: string]: unknown
}

/** `list()`: the pending timers of the queue. */
export interface TimerListResult {
  rows: JsonObject[]
  truncated: boolean
  nextAfter: string | null
  [key: string]: unknown
}

/** The steps every timer builder has. */
interface TimerSteps {
  /** The timer's identity inside its queue. Required. */
  key(timerKey: string): this
  /** Destination partition of the delivered message. Defaults to 'Default'. */
  partition(name: string): this
  /** Fire no earlier than this many milliseconds from now. */
  delayMs(ms: number): this
  /** The same delay as a duration string: '250ms', '30s', '2h'. */
  delay(duration: Duration): this
  /** The message body. Any JSON value, or a Uint8Array/Buffer for raw bytes. Required to schedule. */
  payload(payload: unknown): this
  /** The delivered message's headers. */
  headers(headers: MessageHeaders): this
  /** The transaction id the delivered message will carry. Minted when absent. */
  txn(transactionId: string): this
}

export declare class TimerBuilder implements TimerSteps {
  constructor(httpClient: QueenHttpClient | null, queue: string, sink?: ((op: JsonObject) => unknown) | null)
  key(timerKey: string): this
  partition(name: string): this
  delayMs(ms: number): this
  delay(duration: Duration): this
  payload(payload: unknown): this
  headers(headers: MessageHeaders): this
  txn(transactionId: string): this
  /** Schedule (or reschedule) this timer. */
  schedule(): Promise<TimerScheduleResult>
  /** Cancel this timer, through the route that is never blockable. */
  cancel(): Promise<TimerCancelResult>
  /** Read one pending timer, payload included. */
  peek(): Promise<TimerPeekResult>
  /** List the pending timers of this queue. `after` is an exclusive keyset cursor. */
  list(opts?: { after?: string | null | undefined; limit?: number | null | undefined }): Promise<TimerListResult>
}

/**
 * A timer inside a transaction (`tx.timer(q)`): `.schedule()` and
 * `.cancel()` add the op and hand the transaction back. `peek` and `list`
 * are reads, not transaction operations.
 */
export interface TransactionTimerBuilder<Tx> extends TimerSteps {
  schedule(): Tx
  cancel(): Tx
}
