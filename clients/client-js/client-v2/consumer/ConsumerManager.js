/**
 * Consumer manager for handling concurrent workers
 */

import * as logger from '../utils/logger.js'

export class ConsumerManager {
  #httpClient
  #queen

  constructor(httpClient, queen) {
    this.#httpClient = httpClient
    this.#queen = queen
  }

  /**
   * Generate affinity key for consistent routing
   * Matches server's PollIntention::grouping_key() format
   * Format: queue:partition:consumerGroup or namespace:task:consumerGroup
   */
  #getAffinityKey(queue, partition, namespace, task, group) {
    if (queue) {
      // Queue-based routing: queue:partition:consumerGroup
      return `${queue}:${partition || '*'}:${group || '__QUEUE_MODE__'}`
    } else if (namespace || task) {
      // Namespace/task-based routing: namespace:task:consumerGroup
      return `${namespace || '*'}:${task || '*'}:${group || '__QUEUE_MODE__'}`
    }
    return null
  }

  async start(handler, options) {
    const {
      queue,
      partition,
      namespace,
      task,
      group,
      concurrency,
      batch,
      limit,
      idleMillis,
      autoAck,
      wait,
      timeoutMillis,
      renewLease,
      renewLeaseIntervalMillis,
      subscriptionMode,
      subscriptionFrom,
      each,
      maxPartitions,
      signal
    } = options

    logger.log('ConsumerManager.start', { 
      queue, 
      partition, 
      namespace, 
      task, 
      group, 
      concurrency, 
      batch, 
      limit, 
      autoAck,
      wait,
      each
    })

    // Build the path and params for pop requests
    const path = this.#buildPath(queue, partition, namespace, task)
    const baseParams = this.#buildParams(batch, wait, timeoutMillis, group, subscriptionMode, subscriptionFrom, namespace, task, autoAck, maxPartitions)
    
    // Generate affinity key for consistent routing to same backend
    const affinityKey = this.#getAffinityKey(queue, partition, namespace, task, group)

    // Start workers
    const workers = []
    for (let i = 0; i < concurrency; i++) {
      workers.push(this.#worker(i, handler, path, baseParams, {
        batch,
        limit,
        idleMillis,
        autoAck,
        wait,
        timeoutMillis,
        renewLease,
        renewLeaseIntervalMillis,
        each,
        signal,
        group,  // Pass consumer group to workers
        affinityKey  // Pass affinity key to workers
      }))
    }

    logger.log('ConsumerManager.start', { status: 'workers-started', count: concurrency })

    // Wait for all workers to complete
    await Promise.all(workers)
    
    logger.log('ConsumerManager.start', { status: 'completed' })
  }

  async #worker(workerId, handler, path, baseParams, options) {
    const {
      batch,
      limit,
      idleMillis,
      autoAck,
      wait,
      timeoutMillis,
      renewLease,
      renewLeaseIntervalMillis,
      each,
      signal,
      group,
      affinityKey
    } = options

    logger.log('ConsumerManager.worker', { workerId, status: 'started', limit, idleMillis })
    
    let processedCount = 0
    let lastMessageTime = idleMillis ? Date.now() : null

    while (true) {
      // Check abort signal
      if (signal && signal.aborted) {
        logger.log('ConsumerManager.worker', { workerId, status: 'aborted', processedCount })
        break
      }

      // Check limit
      if (limit && processedCount >= limit) {
        logger.log('ConsumerManager.worker', { workerId, status: 'limit-reached', processedCount, limit })
        break
      }

      // Check idle timeout
      if (idleMillis && lastMessageTime) {
        const idleTime = Date.now() - lastMessageTime
        if (idleTime >= idleMillis) {
          logger.log('ConsumerManager.worker', { workerId, status: 'idle-timeout', processedCount, idleTime })
          break
        }
      }

      try {
        // Pop messages with affinity key for consistent routing. wait=true is
        // a long-poll: mark it 'pop' so a 429 backs off and keeps waiting
        // instead of giving up after the bounded push-like attempt budget.
        const clientTimeout = wait ? timeoutMillis + 5000 : timeoutMillis
        const result = await this.#httpClient.get(`${path}?${baseParams}`, clientTimeout, affinityKey, wait ? 'pop' : null)

        // Handle empty response
        if (!result || !result.messages || result.messages.length === 0) {
          if (wait) {
            continue // Long polling timeout, retry
          } else {
            // Short delay before retry
            await new Promise(resolve => setTimeout(resolve, 100))
            continue
          }
        }

        const messages = result.messages.filter(msg => msg != null)

        if (messages.length === 0) {
          continue
        }

        logger.log('ConsumerManager.worker', { workerId, status: 'messages-received', count: messages.length })

        // Enhance messages with trace() method
        this.#enhanceMessagesWithTrace(messages, group)

        // Update last message time
        if (idleMillis) {
          lastMessageTime = Date.now()
        }

        // Set up lease renewal if enabled
        let renewalTimer = null
        if (renewLease && renewLeaseIntervalMillis) {
          renewalTimer = this.#setupLeaseRenewal(messages, renewLeaseIntervalMillis)
        }

        try {
          // Process messages
          if (each) {
            // Process one at a time
            for (const message of messages) {
              if (signal && signal.aborted) break

              const ok = await this.#processMessage(message, handler, autoAck, group)
              processedCount++

              // A nack releases the lease and clamps the server cursor at the
              // failed message: everything after it in this popped batch WILL
              // be redelivered. Processing it now would only produce duplicates
              // and rejected acks — abandon the rest of the batch.
              if (autoAck && !ok) {
                logger.warn('ConsumerManager.worker', { workerId, status: 'batch-abandoned-after-nack', remaining: messages.length - messages.indexOf(message) - 1 })
                break
              }

              if (limit && processedCount >= limit) break
            }
          } else {
            // Process as batch
            await this.#processBatch(messages, handler, autoAck, group)
            processedCount += messages.length
          }
          
          logger.log('ConsumerManager.worker', { workerId, status: 'messages-processed', count: messages.length, total: processedCount })
        } finally {
          // Clear renewal timer
          if (renewalTimer) {
            clearInterval(renewalTimer)
          }
        }

      } catch (error) {
        // Check if this is a timeout error (expected for long polling)
        const isTimeoutError = error.name === 'AbortError' ||
                              error.message?.includes('timeout')

        if (isTimeoutError && wait) {
          continue // Retry on timeout
        }

        // 429 (rate limited): HttpClient already retries this internally
        // with backoff (unbounded for wait=true pop, per retry429 policy) --
        // this branch is a defensive fallback for the case where an explicit
        // retry429.maxAttempts override got exhausted. Back off and keep
        // polling instead of hot-looping or rethrowing/dying.
        if (error.status === 429) {
          const retryAfterMs = typeof error.retryAfterSeconds === 'number' && error.retryAfterSeconds >= 0
            ? error.retryAfterSeconds * 1000
            : 1000
          logger.warn('ConsumerManager.worker', { workerId, status: 'rate-limited', code: error.code, retryAfterMs })
          await new Promise(resolve => setTimeout(resolve, retryAfterMs))
          continue
        }

        // Check if network error
        const isNetworkError = error.message?.includes('fetch failed') ||
                              error.message?.includes('ECONNREFUSED') ||
                              error.code === 'ECONNREFUSED'

        if (isNetworkError) {
          logger.warn('ConsumerManager.worker', { workerId, error: 'network', message: error.message })
          // Wait before retry
          await new Promise(resolve => setTimeout(resolve, 1000))
          continue
        }

        // 403 (forbidden): terminal. cluster_suspended in particular can
        // never resolve itself, and none of the other proxy codes
        // (storage_quota_exceeded / feature_gated / forbidden) are worth
        // hot-looping either -- stop this worker and surface the error
        // (with .code) to the caller instead of retrying.
        if (error.status === 403) {
          logger.error('ConsumerManager.worker', { workerId, status: 'forbidden', code: error.code, error: error.message })
          throw error
        }

        // Other errors - rethrow
        logger.error('ConsumerManager.worker', { workerId, error: error.message, code: error.code })
        throw error
      }
    }
    
    logger.log('ConsumerManager.worker', { workerId, status: 'stopped', processedCount })
  }

  /**
   * Returns true when the message was handled (and acked) successfully,
   * false when it was nacked — the caller must abandon the rest of the
   * popped batch (the nack released the lease server-side).
   */
  async #processMessage(message, handler, autoAck, group) {
    try {
      await handler(message)

      // Auto-ack on success if enabled
      if (autoAck) {
        const context = group ? { group } : {}
        const res = await this.#queen.ack(message, true, context)
        if (res && res.success === false) {
          logger.error('ConsumerManager.processMessage', { transactionId: message.transactionId, status: 'ack-rejected', error: res.error })
        } else {
          logger.log('ConsumerManager.processMessage', { transactionId: message.transactionId, status: 'acked' })
        }
      }
      return true
    } catch (error) {
      // Auto-nack on error if enabled
      if (autoAck) {
        const context = group ? { group, error: error.message } : { error: error.message }
        const res = await this.#queen.ack(message, false, context)
        if (res && res.success === false) {
          logger.error('ConsumerManager.processMessage', { transactionId: message.transactionId, status: 'nack-rejected', error: res.error })
        }
        logger.error('ConsumerManager.processMessage', { transactionId: message.transactionId, error: error.message, status: 'nacked' })
        // Don't rethrow when autoAck is enabled - NACK was already sent
        // This allows the consumer to continue and retry
        return false
      }
      logger.error('ConsumerManager.processMessage', { transactionId: message.transactionId, error: error.message })
      throw error
    }
  }

  async #processBatch(messages, handler, autoAck, group) {
    try {
      await handler(messages)

      // Auto-ack on success if enabled
      if (autoAck) {
        const context = group ? { group } : {}
        const res = await this.#queen.ack(messages, true, context)
        if (res && res.success === false) {
          logger.error('ConsumerManager.processBatch', { count: messages.length, status: 'ack-rejected', error: res.error })
        } else {
          logger.log('ConsumerManager.processBatch', { count: messages.length, status: 'acked' })
        }
      }
    } catch (error) {
      // Auto-nack on error if enabled
      if (autoAck) {
        const context = group ? { group, error: error.message } : { error: error.message }
        await this.#queen.ack(messages, false, context)
        logger.error('ConsumerManager.processBatch', { count: messages.length, error: error.message, status: 'nacked' })
        // Don't rethrow when autoAck is enabled - NACK was already sent
        // This allows the consumer to continue and retry
        return
      }
      logger.error('ConsumerManager.processBatch', { count: messages.length, error: error.message })
      throw error
    }
  }

  #setupLeaseRenewal(messages, intervalMillis) {
    const leaseIds = messages.map(m => m.leaseId).filter(id => id != null)

    if (leaseIds.length === 0) return null

    return setInterval(async () => {
      try {
        await this.#queen.renew(messages)
      } catch (error) {
        logger.error('ConsumerManager.leaseRenewal', { error: error.message })
      }
    }, intervalMillis)
  }

  #enhanceMessagesWithTrace(messages, group) {
    const httpClient = this.#httpClient
    const consumerGroup = group || '__QUEUE_MODE__'
    
    for (const message of messages) {
      /**
       * Record a trace event for this message
       * @param {object} traceConfig - Configuration object
       * @param {string|string[]} [traceConfig.traceName] - Single name or array of names for categorization
       * @param {string} [traceConfig.eventType='info'] - Event type (info, error, step, processing, etc.)
       * @param {object} traceConfig.data - User data to store with the trace
       * @returns {Promise<object>} Result with success status
       * 
       * IMPORTANT: This method will NEVER crash - errors are logged but don't throw
       * 
       * @example
       * await msg.trace({
       *   traceName: ['tenant-acme', 'chat-room-123'],
       *   eventType: 'info',
       *   data: { text: 'Started processing', orderId: 123 }
       * });
       */
      message.trace = async (traceConfig) => {
        try {
          // Validate required structure
          if (typeof traceConfig !== 'object' || !traceConfig.data) {
            logger.warn('ConsumerManager.trace', { 
              error: 'Invalid trace config: requires { data: {...} }',
              transactionId: message.transactionId 
            })
            return { success: false, error: 'Invalid trace config: requires { data: {...} }' }
          }
          
          // Normalize traceName to array
          let traceNames = null
          if (traceConfig.traceName) {
            if (Array.isArray(traceConfig.traceName)) {
              traceNames = traceConfig.traceName.filter(n => typeof n === 'string' && n.length > 0)
              if (traceNames.length === 0) traceNames = null
            } else if (typeof traceConfig.traceName === 'string') {
              traceNames = [traceConfig.traceName]
            }
          }
          
          const response = await httpClient.post('/api/v1/traces', {
            transactionId: message.transactionId,
            partitionId: message.partitionId,
            consumerGroup: consumerGroup,
            traceNames: traceNames,
            eventType: traceConfig.eventType || 'info',
            data: traceConfig.data
          })
          
          logger.log('ConsumerManager.trace', { 
            transactionId: message.transactionId, 
            success: true,
            traceNames: traceNames 
          })
          return { success: true, ...response }
        } catch (error) {
          // CRITICAL: NEVER CRASH - just log and return gracefully
          logger.error('ConsumerManager.trace', {
            transactionId: message.transactionId,
            error: error.message,
            phase: 'trace-failed'
          })
          logger.warn('ConsumerManager.trace', { transactionId: message.transactionId, error: error.message })
          
          return { success: false, error: error.message }
        }
      }
    }
  }

  #buildPath(queue, partition, namespace, task) {
    if (queue) {
      if (partition) {
        return `/api/v1/pop/queue/${queue}/partition/${partition}`
      }
      return `/api/v1/pop/queue/${queue}`
    }

    if (namespace || task) {
      return '/api/v1/pop'
    }

    throw new Error('Must specify queue, namespace, or task')
  }

  #buildParams(batch, wait, timeoutMillis, group, subscriptionMode, subscriptionFrom, namespace, task, autoAck, maxPartitions) {
    const params = new URLSearchParams({
      batch: batch.toString(),
      wait: wait.toString(),
      timeout: timeoutMillis.toString()  // Server expects 'timeout', not 'timeoutMillis'
    })

    if (group) params.append('consumerGroup', group)
    if (subscriptionMode) params.append('subscriptionMode', subscriptionMode)
    if (subscriptionFrom) params.append('subscriptionFrom', subscriptionFrom)
    if (namespace) params.append('namespace', namespace)
    if (task) params.append('task', task)
    // v4 multi-partition pop: drain up to N sparse partitions per call.
    if (maxPartitions && maxPartitions > 1) params.append('partitions', maxPartitions.toString())
    // NEVER send autoAck for consume - client always manages acking
    // autoAck is only for pop() where server auto-acks immediately

    return params
  }
}

