/**
 * Transaction builder for atomic operations: acks, pushes, links, KV
 * writes, lock guards and timers that commit together, in one log entry.
 *
 *     await queen.transaction()
 *       .ack(message)
 *       .queue('output').push([{ data: { result: 'processed' } }])
 *       .kv.put('saga', sagaId, { step: 'charged' }, { ttl: '24h' })
 *       .once('dedup', message.transactionId, { ttl: '24h' })
 *       .commit()
 *
 * `commit()` RETURNS on a lost gate (`success: false`, `reason:
 * 'kv_precondition'`) and throws on everything else.
 */

import type {
  KvCheckOptions,
  KvDeleteOptions,
  KvIncrOptions,
  KvOnceOptions,
  KvPutIfAbsentOptions,
  KvPutOptions
} from '../kv/Kv.js'
import type { Lock } from '../locks/Locks.js'
import type {
  AckStatus,
  AckTarget,
  JsonObject,
  LinkableMessage,
  LinkOptions,
  PushItem,
  QueenHttpClient,
  TransactionGroupContext,
  TransactionResult
} from '../types.js'
import type { TransactionTimerBuilder } from './TimerBuilder.js'

/**
 * Where a `link` finds its message: as a pop returned it (partitionId,
 * offset, transactionId), or a feed entry by its queue and partition.
 */
export declare function linkSource(message: LinkableMessage, consumerGroup?: string | null): JsonObject

/** `.queue(name)` of a transaction: push or link into that queue. */
export interface TransactionQueue {
  /** The partition of the pushes and links that follow. */
  partition(partitionKey: string): TransactionQueue
  /** Push items in this transaction. */
  push<T = any>(items: PushItem<T> | PushItem<T>[]): TransactionBuilder
  /**
   * Forward messages this worker holds into this queue, without sending
   * their payload again. Without `.partition(p)` each message keeps its
   * partition's name.
   */
  link(messages: LinkableMessage | LinkableMessage[], opts?: LinkOptions): TransactionBuilder
}

/**
 * KV operations that commit with the transaction. Every method returns the
 * TRANSACTION. `getPrefix` is not available inside a transaction (it throws).
 */
export interface TransactionKv {
  get(ns: string, key: string): TransactionBuilder
  getMany(ns: string, keys: string[]): TransactionBuilder
  put(ns: string, key: string, value: unknown, opts: KvPutOptions): TransactionBuilder
  putIfAbsent(ns: string, key: string, value: unknown, opts: KvPutIfAbsentOptions): TransactionBuilder
  delete(ns: string, key: string, opts?: KvDeleteOptions): TransactionBuilder
  incr(ns: string, key: string, delta: number | undefined, opts: KvIncrOptions): TransactionBuilder
  check(ns: string, key: string, opts: KvCheckOptions): TransactionBuilder
  /** Throws: unbounded read work does not belong in a transaction. */
  getPrefix(...args: unknown[]): never
}

export declare class TransactionBuilder {
  constructor(httpClient: QueenHttpClient)
  /**
   * Ack popped messages as part of this transaction (default status
   * `completed`). The consumer group defaults to the one each message was
   * popped under; pass `{ consumerGroup }` (or `{ group }`) to override it.
   */
  ack(messages: AckTarget | AckTarget[], status?: AckStatus, context?: TransactionGroupContext): this
  /** Push or link into a queue. */
  queue(queueName: string): TransactionQueue
  /** KV operations that commit with this transaction. */
  get kv(): TransactionKv
  /**
   * The gate: do the bundle at most once. A `putIfAbsent` with
   * `required: true`, so a marker that already exists rolls the whole
   * transaction back and `commit()` returns `success: false`.
   */
  once(ns: string, key: string, opts: KvOnceOptions): this
  /**
   * Commit this bundle only while a lock is held. `commit()` throws
   * `.code === 'LOCK_NOT_HELD'` when the handle holds nothing.
   */
  guard(lock: Lock): this
  /** Schedule or cancel a timer as part of this transaction. */
  timer(queueName: string): TransactionTimerBuilder<this>
  /** Send the bundle. Returns on a lost gate (`success: false`), throws on every other failure. */
  commit(): Promise<TransactionResult>
}
