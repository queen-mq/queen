/**
 * RAM-class queues: `/api/v1/ephemeral/*`.
 *
 *     await queen.ephemeral.push('inbox:7', [{ hello: 'world' }])
 *     const { messages } = await queen.ephemeral.pop('inbox:7', { wait: true })
 *     await queen.ephemeral.ack('inbox:7', messages, { group: 'workers' })
 *
 * CONTENTS SURVIVE NOTHING (restart, crash, deploy, ownership move); a
 * declared queue's options are durable. A queue does not have to exist: the
 * first push or pop that names one creates it. No replay, no
 * subscriptionMode, no DLQ, no transactions.
 *
 * Requires broker/proxy >= 1.1: an older one 404s the whole family, mapped
 * to `.code === EPHEMERAL_UNSUPPORTED`. `depth` on a queue that does not
 * exist raises `.code === EPHEMERAL_QUEUE_NOT_FOUND` instead.
 */

import type { JsonObject, QueenBufferManager, QueenHttpClient } from '../types.js'

/** `.code` of a 404 from a broker or proxy without the ephemeral routes. */
export declare const EPHEMERAL_UNSUPPORTED: 'ephemeral_unsupported'

/** The message of the `EPHEMERAL_UNSUPPORTED` error. */
export declare const EPHEMERAL_UNSUPPORTED_MESSAGE: string

/** `.code` of `depth` on a queue that does not exist. */
export declare const EPHEMERAL_QUEUE_NOT_FOUND: 'ephemeral_queue_not_found'

/** Options of `ephemeral.configure()`. An unknown option throws. */
export interface EphemeralConfigureOptions {
  /** Per-queue byte budget. */
  maxBytes?: number | undefined
  /** Per-queue message budget. */
  maxLength?: number | undefined
  /** On a full queue: `reject` the push (429), or `dropOldest` (feed semantics). */
  policy?: 'reject' | 'dropOldest' | undefined
  /** Drop messages older than this. */
  ttlSeconds?: number | undefined
  leaseSeconds?: number | undefined
  retryLimit?: number | undefined
  /** Let a waiting pop fatten its batch. */
  windowBuffer?: { ms?: number | undefined; count?: number | undefined } | undefined
}

/** `buffered` options of `ephemeral.push()`: `QueueBuilder.buffer()`'s, plus `intervalMillis` for `timeMillis`. */
export interface EphemeralBufferOptions {
  messageCount?: number | undefined
  timeMillis?: number | undefined
  intervalMillis?: number | undefined
  maxSize?: number | undefined
  retryDelayMillis?: number | undefined
}

/** Options of `ephemeral.push()`. */
export interface EphemeralPushOptions {
  /** The ring (FIFO is per partition); omitted, the broker picks. */
  partition?: string | null | undefined
  /** Batch client-side; resolves once the messages are IN the buffer. */
  buffered?: true | EphemeralBufferOptions | false | null | undefined
  /** Gives up a buffered push that waits for capacity. */
  signal?: AbortSignal | undefined
}

/** Options of `ephemeral.pop()`. */
export interface EphemeralPopOptions {
  partition?: string | null | undefined
  batch?: number | null | undefined
  /** A real long poll. */
  wait?: boolean | undefined
  /** Long-poll timeout in milliseconds (default 30000); `timeoutMillis` is the same. */
  timeout?: number | null | undefined
  timeoutMillis?: number | null | undefined
  /** Same group competes, own group fans out, no group is queue mode. */
  group?: string | null | undefined
  /** Commit at delivery (at-most-once). */
  commitOnDelivery?: boolean | null | undefined
  /** @deprecated The old name of `commitOnDelivery`. */
  autoAck?: boolean | null | undefined
}

/** A popped ephemeral message. The `id` is opaque. */
export interface EphemeralMessage<T = any> {
  id: string
  partition: string
  payload: T
  attempts: number
  [key: string]: unknown
}

/** An ephemeral ack status. `true` is `completed` and `false` is `failed`. */
export type EphemeralAckStatus = 'completed' | 'failed' | 'retry'

/** What an ephemeral ack names: a popped message, its id, or `{ id, status?, error? }`. */
export type EphemeralAckTarget =
  | string
  | {
      id: string
      status?: EphemeralAckStatus | boolean | null | undefined
      error?: string | null | undefined
    }

/** Options of `ephemeral.ack()`: the call-wide default status and error, and the pop's group. */
export interface EphemeralAckOptions {
  status?: EphemeralAckStatus | boolean | null | undefined
  error?: string | null | undefined
  group?: string | null | undefined
}

/** One ack's outcome. `stale` is not an error: the message belonged to a previous incarnation. */
export interface EphemeralAckResult {
  id: string
  outcome: 'acked' | 'redelivered' | 'stale' | 'unknown' | (string & {})
}

export declare class Ephemeral {
  constructor(httpClient: QueenHttpClient, bufferManager?: QueenBufferManager | null)
  /** Declare a queue and its bounds. The options persist; the contents never do. */
  configure(queue: string, options?: EphemeralConfigureOptions): Promise<JsonObject>
  /** Drop every message, void every lease, rewind every group cursor. */
  reset(queue: string): Promise<{ dropped: number; [key: string]: unknown }>
  /** Delete the queue: contents, cursors, and the declared configuration. */
  delete(queue: string): Promise<{ deleted: boolean; [key: string]: unknown }>
  /**
   * Push one message or many. All-or-nothing per request. `{ data }` and
   * `{ payload }` are read as the envelope; wrap an object that has a `data`
   * key as `{ payload: obj }`.
   */
  push(
    queue: string,
    messages: unknown,
    opts?: EphemeralPushOptions
  ): Promise<{ pushed: number; [key: string]: unknown } | { buffered: true; count: number }>
  /** Send everything buffered for one ephemeral queue/partition, now. */
  flush(queue: string, opts?: { partition?: string | null | undefined; deadlineMillis?: number | null | undefined }): Promise<void>
  /** Take up to `batch` messages. `messages` is an empty array when there was nothing, never null. */
  pop<T = any>(queue: string, opts?: EphemeralPopOptions): Promise<{ queue: string; messages: EphemeralMessage<T>[] }>
  /** Acknowledge popped messages. Pass the same `group` the pop used. */
  ack(
    queue: string,
    acks: EphemeralAckTarget | EphemeralAckTarget[],
    opts?: EphemeralAckOptions
  ): Promise<{ results: EphemeralAckResult[] }>
  /** Every ephemeral queue this tenant currently has. */
  queues(): Promise<unknown>
  /** Depth gauges for one queue. Raises `EPHEMERAL_QUEUE_NOT_FOUND` for an unknown queue. */
  depth(queue: string): Promise<JsonObject>
}
