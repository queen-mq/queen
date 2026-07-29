/**
 * Queen Message Queue Client - Version 2
 * Clean, fluent API with smart defaults
 */

import { HttpClient } from './http/HttpClient.js'
import { LoadBalancer } from './http/LoadBalancer.js'
import { BufferManager } from './buffer/BufferManager.js'
import { QueueBuilder } from './builders/QueueBuilder.js'
import { TransactionBuilder } from './builders/TransactionBuilder.js'
import { StreamBuilder } from './stream/StreamBuilder.js'
import { StreamConsumer } from './stream/StreamConsumer.js'
import { Admin } from './admin/Admin.js'
import { CLIENT_DEFAULTS } from './utils/defaults.js'
import { validateUrl, validateUrls } from './utils/validation.js'
import * as logger from './utils/logger.js'

// Both /api/v1/ack and /api/v1/ack/batch respond with a top-level JSON array,
// one item per acknowledgment in request order:
//   [{index, transactionId, success, error, queueName, partitionName, leaseReleased, dlq}]
// (see queen.ack_messages_v2 / routes/ack.cpp). A rejected ack/nack (e.g.
// "Invalid or expired lease") still arrives as HTTP 200 with success=false on
// the item, so the per-item flag is the only signal that the broker accepted it.

function normalizeAckItem(item, index) {
  if (item === null || typeof item !== 'object') {
    throw new Error(`Unexpected ack result item at index ${index}`)
  }
  let success = typeof item.success === 'boolean' ? item.success : true
  let error = typeof item.error === 'string' && item.error.length > 0 ? item.error : null
  if (error) success = false
  if (!success && !error) error = 'Acknowledgment rejected by server'
  return { ...item, success, error }
}

// expected is the number of acknowledgments sent, so a truncated or misaligned
// response fails loudly instead of being misattributed to the wrong message.
function parseAckResults(result, expected) {
  if (Array.isArray(result)) {
    if (result.length !== expected) {
      throw new Error(`Ack response has ${result.length} results, expected ${expected}`)
    }
    return result.map(normalizeAckItem)
  }

  // Top-level error envelope: the whole request was rejected.
  if (result && typeof result === 'object' && typeof result.error === 'string' && result.error.length > 0) {
    return Array.from({ length: expected }, () => ({ success: false, error: result.error }))
  }

  throw new Error('Unexpected ack response format: missing per-item result array')
}

export class Queen {
  #httpClient
  #bufferManager
  #config
  #shutdownHandlers = []
  #admin = null

  constructor(config = {}) {
    // Configure custom logger before anything else.
    // Opt into structured field logging with `structuredLogs: true` (the logger
    // must accept a leading merge object, e.g. pino/bunyan); defaults to the
    // backward-compatible single-string format.
    if (config && typeof config === 'object' && !Array.isArray(config) && config.logger) {
      logger.configure(config.logger, { structured: config.structuredLogs === true })
    }

    logger.log('Queen.constructor', { config: typeof config === 'object' && !Array.isArray(config) ? { ...config, urls: config.urls?.length || 0 } : { type: typeof config } })
    
    // Normalize config
    this.#config = this.#normalizeConfig(config)

    // Create HTTP client
    this.#httpClient = this.#createHttpClient()

    // Create buffer manager
    this.#bufferManager = new BufferManager(this.#httpClient)

    // Setup graceful shutdown (opt-out via handleSignals: false)
    if (this.#config.handleSignals) {
      this.#setupGracefulShutdown()
    }
    
    logger.log('Queen.constructor', { status: 'initialized', urls: this.#config.urls.length, handleSignals: this.#config.handleSignals })
  }

  #normalizeConfig(config) {
    // Handle different input formats
    if (typeof config === 'string') {
      // Single URL string
      return {
        ...CLIENT_DEFAULTS,
        urls: [validateUrl(config)]
      }
    }

    if (Array.isArray(config)) {
      // Array of URLs
      return {
        ...CLIENT_DEFAULTS,
        urls: validateUrls(config)
      }
    }

    // Object config
    const normalized = {
      ...CLIENT_DEFAULTS,
      ...config
    }

    // Ensure URLs are validated
    if (normalized.urls) {
      normalized.urls = validateUrls(normalized.urls)
    } else if (normalized.url) {
      normalized.urls = [validateUrl(normalized.url)]
    } else {
      throw new Error('Must provide urls or url in configuration')
    }

    return normalized
  }

  #createHttpClient() {
    const { urls, timeoutMillis, retryAttempts, retryDelayMillis, loadBalancingStrategy, affinityHashRing, healthRetryAfterMillis, enableFailover, bearerToken, headers, hostHeader, retry429 } = this.#config

    if (urls.length === 1) {
      // Single server
      return new HttpClient({
        baseUrl: urls[0],
        timeoutMillis,
        retryAttempts,
        retryDelayMillis,
        bearerToken,
        headers,
        hostHeader,
        retry429
      })
    }

    // Multiple servers with load balancing
    const loadBalancer = new LoadBalancer(urls, loadBalancingStrategy, {
      affinityHashRing,
      healthRetryAfterMillis
    })
    return new HttpClient({
      loadBalancer,
      timeoutMillis,
      retryAttempts,
      retryDelayMillis,
      enableFailover,
      bearerToken,
      headers,
      hostHeader,
      retry429
    })
  }

  #setupGracefulShutdown() {
    // Skip signal handlers in browser environment
    if (typeof process === 'undefined' || typeof process.on !== 'function') {
      return
    }

    let signalReceivedCount = 0;
    const shutdown = async (signal) => {
      logger.log('Queen.shutdown', { signal })
      try {
        signalReceivedCount++;
        if (signalReceivedCount > 1) {
          logger.warn('Queen.shutdown', 'Received multiple shutdown signals, exiting immediately')
          process.exit(1)
        }
        await this.close()
        process.exit(0)
      } catch (error) {
        logger.error('Queen.shutdown', { error: error.message })
        process.exit(1)
      }
    }

    const sigintHandler = () => shutdown('SIGINT')
    const sigtermHandler = () => shutdown('SIGTERM')

    process.on('SIGINT', sigintHandler)
    process.on('SIGTERM', sigtermHandler)

    // Store handlers for cleanup
    this.#shutdownHandlers.push(
      () => process.off('SIGINT', sigintHandler),
      () => process.off('SIGTERM', sigtermHandler)
    )
  }

  // ===========================
  // Queue Builder Entry Point
  // ===========================

  queue(name = null) {
    return new QueueBuilder(this, this.#httpClient, this.#bufferManager, name)
  }

  // ===========================
  // Admin API Entry Point
  // ===========================

  /**
   * Get the Admin API for administrative and observability operations
   * @returns {Admin} Admin API instance (lazily initialized, singleton)
   */
  get admin() {
    if (!this.#admin) {
      this.#admin = new Admin(this.#httpClient)
    }
    return this.#admin
  }

  // ===========================
  // Transaction API
  // ===========================

  transaction() {
    return new TransactionBuilder(this.#httpClient)
  }

  // ===========================
  // Direct ACK API
  // ===========================

  async ack(message, status = true, context = {}) {
    const isBatch = Array.isArray(message)
    logger.log('Queen.ack', { isBatch, count: isBatch ? message.length : 1, status, context })
    
    // Handle batch acknowledgment
    if (Array.isArray(message)) {
      if (message.length === 0) {
        return { success: true, processed: 0, results: [] }
      }

      // Check if messages have individual status
      const hasIndividualStatus = message.some(msg =>
        typeof msg === 'object' && msg !== null && ('_status' in msg || '_error' in msg)
      )

      let acknowledgments

      if (hasIndividualStatus) {
        // Each message has its own status
        acknowledgments = message.map(msg => {
          const transactionId = typeof msg === 'string' ? msg : (msg.transactionId || msg.id)
          const partitionId = typeof msg === 'object' ? msg.partitionId : null
          const leaseId = typeof msg === 'object' ? msg.leaseId : null

          if (!transactionId) {
            throw new Error('Message must have transactionId or id property')
          }

          // CRITICAL: partitionId is now MANDATORY to prevent acking wrong message
          if (!partitionId) {
            throw new Error('Message must have partitionId property to ensure message uniqueness')
          }

          const msgStatus = msg._status !== undefined ? msg._status : status
          const statusStr = typeof msgStatus === 'boolean'
            ? (msgStatus ? 'completed' : 'failed')
            : msgStatus

          const ack = {
            transactionId,
            partitionId,
            status: statusStr,
            error: msg._error || context.error || null
          }

          if (leaseId) ack.leaseId = leaseId

          return ack
        })
      } else {
        // Same status for all messages
        const statusStr = typeof status === 'boolean'
          ? (status ? 'completed' : 'failed')
          : status

        acknowledgments = message.map(msg => {
          const transactionId = typeof msg === 'string' ? msg : (msg.transactionId || msg.id)
          const partitionId = typeof msg === 'object' ? msg.partitionId : null
          const leaseId = typeof msg === 'object' ? msg.leaseId : null

          if (!transactionId) {
            throw new Error('Message must have transactionId or id property')
          }

          // CRITICAL: partitionId is now MANDATORY to prevent acking wrong message
          if (!partitionId) {
            throw new Error('Message must have partitionId property to ensure message uniqueness')
          }

          const ack = {
            transactionId,
            partitionId,
            status: statusStr,
            error: context.error || null
          }

          if (leaseId) ack.leaseId = leaseId

          return ack
        })
      }

      // Call batch ack endpoint
      try {
        const result = await this.#httpClient.post('/api/v1/ack/batch', {
          acknowledgments,
          consumerGroup: context.group || null
        })

        const results = parseAckResults(result, acknowledgments.length)
        const failed = results.filter(r => !r.success)

        if (failed.length > 0) {
          const error = failed.length === 1
            ? failed[0].error
            : `${failed.length} of ${results.length} acknowledgments rejected: ${failed[0].error}`
          logger.error('Queen.ack', { type: 'batch', error, failed: failed.length, count: results.length })
          return { success: false, error, results }
        }

        logger.log('Queen.ack', { type: 'batch', success: true, count: results.length })
        return { success: true, processed: results.length, results }
      } catch (error) {
        logger.error('Queen.ack', { type: 'batch', error: error.message })
        return { success: false, error: error.message }
      }
    }

    // Handle single message acknowledgment
    const transactionId = typeof message === 'string' ? message : (message.transactionId || message.id)
    const partitionId = typeof message === 'object' ? message.partitionId : null
    const leaseId = typeof message === 'object' ? message.leaseId : null

    if (!transactionId) {
      return { success: false, error: 'Message must have transactionId or id property' }
    }

    // CRITICAL: partitionId is now MANDATORY to prevent acking wrong message
    if (!partitionId) {
      return { success: false, error: 'Message must have partitionId property to ensure message uniqueness' }
    }

    const statusStr = typeof status === 'boolean'
      ? (status ? 'completed' : 'failed')
      : status

    const body = {
      transactionId,
      partitionId,
      status: statusStr,
      error: context.error || null,
      consumerGroup: context.group || null
    }

    if (leaseId) body.leaseId = leaseId

    try {
      const result = await this.#httpClient.post('/api/v1/ack', body)

      const [ackResult] = parseAckResults(result, 1)

      if (!ackResult.success) {
        logger.error('Queen.ack', { type: 'single', transactionId, error: ackResult.error })
      } else {
        logger.log('Queen.ack', { type: 'single', transactionId, success: true })
      }
      return ackResult
    } catch (error) {
      logger.error('Queen.ack', { type: 'single', transactionId, error: error.message })
      return { success: false, error: error.message }
    }
  }

  // ===========================
  // Lease Renewal API
  // ===========================

  async renew(messageOrLeaseId) {
    let leaseIds = []

    if (typeof messageOrLeaseId === 'string') {
      leaseIds = [messageOrLeaseId]
    } else if (Array.isArray(messageOrLeaseId)) {
      leaseIds = messageOrLeaseId.map(item =>
        typeof item === 'string' ? item : item.leaseId
      ).filter(Boolean)
    } else if (messageOrLeaseId && typeof messageOrLeaseId === 'object') {
      if (messageOrLeaseId.leaseId) {
        leaseIds = [messageOrLeaseId.leaseId]
      }
    }

    // Dedupe: with v4 multi-partition pop, all messages in one batch share
    // the same leaseId (one renew_lease_v2 call extends every claimed
    // partition_consumers row). Without this, callers passing the full
    // messages array would issue N redundant identical HTTP calls.
    leaseIds = [...new Set(leaseIds)]

    if (leaseIds.length === 0) {
      logger.warn('Queen.renew', 'No valid lease IDs found for renewal')
      return { success: false, error: 'No valid lease IDs found for renewal' }
    }
    
    logger.log('Queen.renew', { count: leaseIds.length })

    const results = []
    for (const leaseId of leaseIds) {
      try {
        const result = await this.#httpClient.post(`/api/v1/lease/${leaseId}/extend`, {})
        results.push({
          leaseId,
          success: true,
          newExpiresAt: result.leaseId ? result.newExpiresAt : result.lease_expires_at
        })
        logger.log('Queen.renew', { leaseId, success: true })
      } catch (error) {
        results.push({ leaseId, success: false, error: error.message })
        logger.error('Queen.renew', { leaseId, error: error.message })
      }
    }

    logger.log('Queen.renew', { total: results.length, successful: results.filter(r => r.success).length })
    return Array.isArray(messageOrLeaseId) ? results : results[0]
  }

  // ===========================
  // Buffer Management API
  // ===========================

  async flushAllBuffers() {
    logger.log('Queen.flushAllBuffers', 'Starting flush of all buffers')
    await this.#bufferManager.flushAllBuffers()
    logger.log('Queen.flushAllBuffers', 'Completed')
  }

  getBufferStats() {
    const stats = this.#bufferManager.getStats()
    logger.log('Queen.getBufferStats', stats)
    return stats
  }

  // ===========================
  // Consumer Group Management
  // ===========================

  /**
   * Delete a consumer group and optionally its subscription metadata
   * @param {string} consumerGroup - Consumer group name
   * @param {boolean} deleteMetadata - Whether to delete subscription metadata (default: true)
   * @returns {Promise<object>}
   */
  async deleteConsumerGroup(consumerGroup, deleteMetadata = true) {
    logger.log('Queen.deleteConsumerGroup', { consumerGroup, deleteMetadata })
    
    const url = `/api/v1/consumer-groups/${encodeURIComponent(consumerGroup)}?deleteMetadata=${deleteMetadata}`
    const response = await this.#httpClient.delete(url)
    
    logger.log('Queen.deleteConsumerGroup', { success: true, consumerGroup })
    return response
  }

  /**
   * Update subscription timestamp for a consumer group
   * @param {string} consumerGroup - Consumer group name
   * @param {string} timestamp - New subscription timestamp (ISO 8601)
   * @returns {Promise<object>}
   */
  async updateConsumerGroupTimestamp(consumerGroup, timestamp) {
    logger.log('Queen.updateConsumerGroupTimestamp', { consumerGroup, timestamp })
    
    const url = `/api/v1/consumer-groups/${encodeURIComponent(consumerGroup)}/subscription`
    const response = await this.#httpClient.post(url, {
      subscriptionTimestamp: timestamp
    })
    
    logger.log('Queen.updateConsumerGroupTimestamp', { success: true, consumerGroup })
    return response
  }

  // ===========================
  // Streaming API
  // ===========================

  /**
   * Define a stream for windowed processing
   * @param {string} name - Stream name
   * @param {string} namespace - Stream namespace
   * @returns {StreamBuilder}
   */
  stream(name, namespace) {
    logger.log('Queen.stream', { name, namespace })
    return new StreamBuilder(this.#httpClient, this, name, namespace)
  }

  /**
   * Create a consumer for a stream
   * @param {string} streamName - Stream name
   * @param {string} consumerGroup - Consumer group
   * @returns {StreamConsumer}
   */
  consumer(streamName, consumerGroup) {
    logger.log('Queen.consumer', { streamName, consumerGroup })
    return new StreamConsumer(this.#httpClient, this, streamName, consumerGroup)
  }

  // ===========================
  // Graceful Shutdown
  // ===========================

  /**
   * Register SIGINT/SIGTERM handlers so the client flushes buffers before exit.
   * Called automatically unless handleSignals is set to false.
   */
  enableGracefulShutdown() {
    if (this.#shutdownHandlers.length > 0) return this
    this.#setupGracefulShutdown()
    return this
  }

  /**
   * Remove previously registered SIGINT/SIGTERM handlers.
   * Use this when Queen is embedded in a larger application that manages its own lifecycle.
   */
  disableGracefulShutdown() {
    for (const cleanup of this.#shutdownHandlers) {
      cleanup()
    }
    this.#shutdownHandlers = []
    return this
  }

  async close() {
    logger.log('Queen.close', 'Starting shutdown')

    // Flush all buffers
    try {
      await this.#bufferManager.flushAllBuffers()
      logger.log('Queen.close', 'All buffers flushed')
    } catch (error) {
      logger.error('Queen.close', { error: error.message, phase: 'buffer-flush' })
    }

    // Cleanup buffer manager
    this.#bufferManager.cleanup()

    // Remove shutdown handlers
    for (const cleanup of this.#shutdownHandlers) {
      cleanup()
    }
    this.#shutdownHandlers = []

    // Release the HTTP agent's keep-alive sockets — without this the Node
    // event loop stays pinned by open sockets and the process never exits
    // naturally after close().
    try {
      await this.#httpClient.destroy()
    } catch (error) {
      logger.warn('Queen.close', { error: error.message, phase: 'http-destroy' })
    }

    logger.log('Queen.close', 'Client closed successfully')
  }
}

