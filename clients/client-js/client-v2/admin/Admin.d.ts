/**
 * Admin API for Queen: administrative and observability endpoints, used by
 * dashboards and admin tools rather than regular applications.
 *
 * Most answers are the broker's JSON as it is, typed `unknown`: narrow them
 * where you read them. The ones with a documented shape are typed.
 */

import type { QueenHttpClient, WhereInput } from '../types.js'

/** Query parameters of the admin list endpoints; `null` and `undefined` values are left out. */
export type AdminQueryParams = Record<string, string | number | boolean | null | undefined>

/** `getQueueDepth()`. */
export interface QueueDepth {
  queue: string
  group: string | null
  pending: number
  /** How many partitions owe work (broker >= 1.1.0). */
  partitionsPending?: number
  /** The group's stored last-value delivery policy (broker >= 1.1.0). */
  conflation?: boolean
  /** Work depth: handler invocations still owed (broker >= 1.1.0). */
  effectivePending?: number
  /** Leased right now (2.x brokers). */
  processing?: number
  /** Pending and not leased (2.x brokers). */
  ready?: number
  partitionsReady?: number
  effectiveReady?: number
  /** The partition list was cut short. */
  partitionsTruncated?: boolean
  unread?: number
  partitions: Array<{ partition: string; pending: number; processing?: number; ready?: number }>
  [key: string]: unknown
}

/** Options of `seekConsumerGroup()`: an ISO `timestamp`, or the broker's other seek targets. */
export interface SeekOptions {
  timestamp?: string | undefined
  [key: string]: unknown
}

export declare class Admin {
  constructor(httpClient: QueenHttpClient)
  /** System overview with queue counts, message stats, etc. */
  getOverview(): Promise<unknown>
  getNamespaces(): Promise<unknown>
  getTasks(): Promise<unknown>
  /** List queues (limit, offset, search, ...). */
  listQueues(params?: AdminQueryParams): Promise<unknown>
  getQueue(name: string): Promise<unknown>
  /**
   * Per-partition backlog for a queue: watermark arithmetic only. Omitting
   * group gives queue-level pending. Requires broker >= 1.0.4.
   */
  getQueueDepth(name: string, group?: string | null): Promise<QueueDepth>
  /**
   * @deprecated The 2.x broker has no route that clears a queue, so this
   *             always rejects, before any request. Seek each consumer group
   *             to the end instead.
   */
  clearQueue(name: string, partition?: string | null): Promise<never>
  getPartitions(params?: AdminQueryParams): Promise<unknown>
  /** List messages with filters (queue, partition, status, limit, offset, ...). */
  listMessages(params?: AdminQueryParams): Promise<unknown>
  getMessage(partitionId: string, transactionId: string): Promise<unknown>
  deleteMessage(partitionId: string, transactionId: string): Promise<unknown>
  retryMessage(partitionId: string, transactionId: string): Promise<unknown>
  /**
   * @deprecated The 2.x broker has no route that moves a message to the DLQ
   *             by its address, so this always rejects, before any request.
   *             Ack the leased message with the `dlq` status instead.
   */
  moveMessageToDLQ(partitionId: string, transactionId: string): Promise<never>
  getTracesByName(traceName: string, params?: AdminQueryParams): Promise<unknown>
  getTraceNames(params?: AdminQueryParams): Promise<unknown>
  getTracesForMessage(partitionId: string, transactionId: string): Promise<unknown>
  getStatus(params?: AdminQueryParams): Promise<unknown>
  getQueueStats(params?: AdminQueryParams): Promise<unknown>
  getQueueDetail(name: string, params?: AdminQueryParams): Promise<unknown>
  getAnalytics(params?: AdminQueryParams): Promise<unknown>
  listConsumerGroups(): Promise<unknown>
  refreshConsumerStats(): Promise<unknown>
  getConsumerGroup(name: string): Promise<unknown>
  /** Consumer groups lagging at least `minLagSeconds` (default 60). */
  getLaggingConsumers(minLagSeconds?: number): Promise<unknown>
  deleteConsumerGroupForQueue(consumerGroup: string, queueName: string, deleteMetadata?: boolean): Promise<unknown>
  /** Seek a consumer group's cursor on a queue; also releases its live leases. */
  seekConsumerGroup(consumerGroup: string, queueName: string, options?: SeekOptions): Promise<unknown>
  /**
   * Change the `where` of a filtered consumer group; it applies from the next
   * push. Update the consumers' `.where()` to match.
   */
  updateConsumerGroupWhere(
    consumerGroup: string,
    queueName: string,
    where: WhereInput
  ): Promise<{ success: boolean; consumerGroup: string; queueName: string; where: string; [key: string]: unknown }>
  health(): Promise<unknown>
  /** Prometheus metrics, as raw text. */
  metrics(): Promise<string>
  getSystemMetrics(params?: AdminQueryParams): Promise<unknown>
  getWorkerMetrics(params?: AdminQueryParams): Promise<unknown>
}
