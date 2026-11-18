/**
 * Default configuration values for Queen Client.
 * Following the convention:
 * - Properties with "Millis" suffix → milliseconds
 * - Properties with "Seconds" suffix → seconds
 * - Properties without suffix (time-related) → seconds
 */

import type { LoadBalancingStrategy, QueenLogger, Retry429Options } from '../types.js'

export declare const CLIENT_DEFAULTS: {
  timeoutMillis: number
  retryAttempts: number
  retryDelayMillis: number
  loadBalancingStrategy: LoadBalancingStrategy
  affinityHashRing: number
  enableFailover: boolean
  healthRetryAfterMillis: number
  bearerToken: null
  headers: Record<string, string>
  hostHeader: null
  handleSignals: boolean
  logger: QueenLogger | null
  retry429: Retry429Options | undefined
  binaryPush: boolean
  pushCompression: boolean
  framedPop: boolean
  bufferMaxMessages: number
  onBufferError: null
}

export declare const QUEUE_DEFAULTS: {
  leaseTime: number
  retryLimit: number
  priority: number
  delayedProcessing: number
  windowBuffer: number
  maxSize: number
  retentionSeconds: number
  completedRetentionSeconds: number
  encryptionEnabled: boolean
}

/**
 * `batch` and `maxPartitions` are the autopilot-off defaults: with autopilot
 * on (the default) a knob the caller never set is left for the broker to size.
 */
export declare const CONSUME_DEFAULTS: {
  concurrency: number
  batch: number
  autoAck: boolean
  wait: boolean
  timeoutMillis: number
  limit: number | null
  idleMillis: number | null
  renewLease: boolean
  renewLeaseIntervalMillis: number | null
  subscriptionMode: string | null
  subscriptionFrom: string | null
  maxPartitions: number
  conflation: boolean
}

/** As in CONSUME_DEFAULTS, `batch` is the autopilot-off default. */
export declare const POP_DEFAULTS: {
  batch: number
  wait: boolean
  timeoutMillis: number
  autoAck: boolean
  commitOnDelivery: boolean
}

export declare const BUFFER_DEFAULTS: {
  messageCount: number
  timeMillis: number
  maxSize: number
  retryDelayMillis: number
}
