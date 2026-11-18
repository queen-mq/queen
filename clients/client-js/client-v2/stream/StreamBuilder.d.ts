/**
 * StreamBuilder - fluent API for defining streams (legacy stream API,
 * `queen.stream(name, namespace)`). For new code, see `Stream`.
 */

import type { QueenHttpClient } from '../types.js'

export declare class StreamBuilder {
  constructor(httpClient: QueenHttpClient, queen: unknown, name: string, namespace?: string)
  /** The definition sent by `define()`. */
  config: {
    name: string
    namespace: string | undefined
    source_queue_names: string[]
    partitioned: boolean
    window_type: string
    window_duration_ms: number
    window_grace_period_ms: number
    window_lease_timeout_ms: number
  }
  /** Set the source queues for this stream. */
  sources(queueNames?: string[]): this
  /** Enable partitioned processing (group by partition_id). */
  partitioned(): this
  /** Configure tumbling time window, in seconds. */
  tumblingTime(seconds: number): this
  /** Grace period for late-arriving messages, in seconds. */
  gracePeriod(seconds: number): this
  /** Lease timeout, in seconds. */
  leaseTimeout(seconds: number): this
  /** Define/create the stream on the server. */
  define(): Promise<unknown>
}
