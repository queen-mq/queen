/**
 * Queue builder for the fluent API: `queen.queue(name)`.
 *
 * Type parameters:
 * - `T`: the payload of the queue's messages (default `any`).
 * - `Each`: whether `.each()` was called. With it the consume handler gets
 *   one message per call; without it, the popped array.
 * - `Buffered`: whether `.buffer()` was called. A buffered push resolves to
 *   `{ buffered: true, count }` instead of the broker's per-item results.
 *
 * `.each()` and `.buffer()` change the builder in place, but only the
 * builder they RETURN carries the new type. Chain them, or keep the
 * returned builder:
 *
 *     const q = queen.queue('tasks').each()   // typed for one message per call
 *     q.consume(async (message) => { ... })
 *
 *     const q2 = queen.queue('tasks')
 *     q2.each()                                // q2's type does not change
 *     q2.consume(async (messages) => { ... })  // typed as an array, gets one message
 */

import type {
  BufferedPushResult,
  BufferOptions,
  ChangedSinceResult,
  ConsumeHandler,
  ConsumeInput,
  ConsumeOptions,
  DLQResult,
  FetchedPartition,
  FetchOptions,
  FormattedPushItem,
  Instant,
  Message,
  PopResult,
  PushError,
  PushItem,
  PushItemResult,
  QueenBufferManager,
  QueenHttpClient,
  QueueConfig,
  QueueConfigureResult,
  QueueDeleteResult,
  SubscriptionMode,
  SupervisionConfig,
  WhereConditions,
  WhereOp
} from '../types.js'

/** A UUIDv7, as pushed items get when they carry no transactionId. */
export declare const generateUUID: () => string

/**
 * The partition one pushed item goes to: its own `partition` when the item
 * is an envelope, else the builder's.
 */
export declare function itemPartition(item: object, envelope: boolean, fallback: string | null): string | null

/**
 * The thenable `create()` and `delete()` return: it runs when awaited.
 * With `.onError()` an error resolves `{ success: false, error }` instead
 * of rejecting.
 */
export interface OperationBuilder<R> extends PromiseLike<R> {
  onSuccess(callback: (result: R) => unknown): this
  onError(callback: (error: Error) => unknown): OperationBuilder<R | { success: false; error: string }>
}

/**
 * The thenable `push()` returns: it runs when awaited. Without `.onError()`
 * a rejected item makes it reject with a `PushError`. With it, nothing
 * rejects: items the broker rejected go to the callback and the push still
 * resolves the per-item results (check each `status`), while a send that
 * failed outright (network or HTTP error, or the buffer refusing the
 * messages) resolves `null`.
 */
export interface PushBuilder<R> extends PromiseLike<R> {
  /** The items the broker took (or the buffer accepted). */
  onSuccess(callback: (items: FormattedPushItem[]) => unknown): this
  /** The items that failed, and why. Catching the error: the push no longer rejects. */
  onError(callback: (items: FormattedPushItem[], error: PushError) => unknown): PushBuilder<R | null>
  /** Items whose transactionId the broker already had. */
  onDuplicate(callback: (items: FormattedPushItem[], error: Error) => unknown): this
}

/**
 * The thenable `consume()` returns: the consumer starts when awaited and
 * resolves once it stops (limit, idle timeout, or its signal).
 *
 * With `onSuccess` or `onError`, auto-ack is off: the callbacks settle the
 * messages. `onError` catches the handler's error, so nothing is nacked for
 * you.
 */
export interface ConsumeBuilder<T, Each extends boolean> extends PromiseLike<void> {
  onSuccess(callback: (input: ConsumeInput<T, Each>, result: unknown) => unknown): this
  onError(callback: (input: ConsumeInput<T, Each>, error: unknown) => unknown): this
}

/** `queue.dlq()`: query the dead letter queue. */
export interface DLQBuilder {
  /** Page size (default 100). */
  limit(count: number): this
  offset(count: number): this
  from(timestamp: string): this
  to(timestamp: string): this
  /** Resolves `{ messages: [], total: 0 }` on an error. */
  get(): Promise<DLQResult>
}

export declare class QueueBuilder<T = any, Each extends boolean = false, Buffered extends boolean = false> {
  constructor(queen: unknown, httpClient: QueenHttpClient, bufferManager: QueenBufferManager, queueName?: string | null)

  /** The queue name (null for a namespace/task builder). */
  get name(): string | null

  // ----------------------------------------------------- queue configuration

  namespace(name: string): this
  task(name: string): this
  /**
   * The queue's options, sent by `create()` with `QUEUE_DEFAULTS` for the
   * ones not given. With `from` (or `where`, `partitionBy`, `start`) it is a
   * reading queue: it takes the messages of other queues that match.
   */
  config(options: QueueConfig): this
  /** Create or reconfigure the queue. Throws `.code === READING_QUEUES_UNSUPPORTED` on a broker without reading queues. */
  create(): OperationBuilder<QueueConfigureResult>
  /** Delete the queue. A queue that reading queues read is refused unless `{ force: true }`. */
  delete(options?: { force?: boolean | undefined }): OperationBuilder<QueueDeleteResult>

  // ------------------------------------------------------------------- push

  /** The partition of pushes and pops (default 'Default'). */
  partition(name: string): this
  /** Batch pushes client-side instead of sending each one. */
  buffer(options: BufferOptions): QueueBuilder<T, Each, true>
  /**
   * Push one item or many: envelopes (`{ data, partition?, transactionId?,
   * traceId?, headers? }`) or bare payload objects.
   */
  push(payload: PushItem<T> | PushItem<T>[]): PushBuilder<Buffered extends true ? BufferedPushResult : PushItemResult[]>

  // ---------------------------------------------------------------- consume

  /** The consumer group. */
  group(name: string): this
  /** Opt-in consumer observations in the broker dashboard; false by default. */
  supervision(config?: SupervisionConfig | false): this
  /** Parallel workers (at least 1). */
  concurrency(count: number): this
  /** Pin the message budget of one pop; unset, the broker sizes it. `batch(0)` reads as unset. */
  batch(size: number): this
  /** Pop from up to N partitions in one call (multi-partition pop); unset, the broker chooses. */
  partitions(n: number): this
  /** Broker-side pop sizing for this builder (default on). */
  autopilot(enabled?: boolean): this
  /** Stop consuming after this many messages. */
  limit(count: number | null): this
  /** Stop consuming after this long without a message. */
  idleMillis(millis: number | null): this
  /** consume(): ack each message after the handler returns (default true). No effect on pop(). */
  autoAck(enabled: boolean): this
  /** Renew the lease while the handler runs, every `intervalMillis`. */
  renewLease(enabled: boolean, intervalMillis?: number): this
  subscriptionMode(mode: SubscriptionMode): this
  /** Start the group at an ISO timestamp (or 'now'). */
  subscriptionFrom(from: string): this
  /** Last-value delivery for this consumer group. Requires broker >= 1.1.0. */
  conflation(enabled?: boolean): this
  /**
   * A filtered group: this group gets only the messages its `where` takes.
   * Several calls are ANDed; one string says the same in the broker's words.
   */
  where(field: string, op: WhereOp, value?: unknown): this
  where(whereText: string): this
  where(conditions: WhereConditions): this
  /** Hand the consume handler one message per call instead of the popped array. */
  each(): QueueBuilder<T, true, Buffered>
  /**
   * Consume until stopped. The handler gets one message with `.each()`, the
   * popped array without it. A throw nacks what it was given.
   */
  consume(handler: ConsumeHandler<T, Each>, options?: ConsumeOptions): ConsumeBuilder<T, Each>

  // -------------------------------------------------------------------- pop

  /** Long-poll: wait up to `timeoutMillis` for a message (default true). */
  wait(enabled: boolean): this
  /** The long-poll timeout in milliseconds (default 30000). */
  timeoutMillis(millis: number): this
  /** Commit the messages of pop() at delivery: no lease, nothing to ack (at-most-once). */
  commitOnDelivery(enabled?: boolean): this
  /** Claim messages and report what the broker chose for this pop. */
  popResult(): Promise<PopResult<T>>
  /** Claim messages. Transport errors resolve `[]`; conflation and where refusals reject. */
  pop(): Promise<Message<T>[]>

  // ----------------------------------------------------------------- buffer

  /** Send every partition's buffered messages of this queue, now. */
  flushBuffer(): Promise<void>

  // ---------------------------------------------------------- reads by offset

  /** Every record of these partitions, read by offset: no lease, no cursor, nothing to ack. */
  fetch(partitions: string | number | Array<string | number>, options?: FetchOptions): Promise<Map<string, FetchedPartition<T>>>
  /** The partitions written at or after `since` (null lists them all). */
  changedSince(since?: Instant | null): Promise<ChangedSinceResult>

  // -------------------------------------------------------------------- dlq

  /** Query the dead letter queue, optionally of one consumer group. */
  dlq(consumerGroup?: string | null): DLQBuilder
}
