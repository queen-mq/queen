/**
 * Queue builder for fluent API
 */

import { v7 as uuidv7 } from 'uuid';

export const generateUUID = () => {
  return uuidv7();
};

//import { generateUUID } from '../../utils/uuid.js'
import { isValidUUID } from '../utils/validation.js'
import { QUEUE_DEFAULTS, CONSUME_DEFAULTS, POP_DEFAULTS } from '../utils/defaults.js'
import * as logger from '../utils/logger.js'

export class QueueBuilder {
  #queen
  #httpClient
  #bufferManager
  #queueName = null
  #partition = 'Default'
  #namespace = null
  #task = null
  #group = null
  #config = {}

  // Consume options
  #concurrency = CONSUME_DEFAULTS.concurrency
  #batch = CONSUME_DEFAULTS.batch
  #limit = CONSUME_DEFAULTS.limit
  #idleMillis = CONSUME_DEFAULTS.idleMillis
  #autoAck = CONSUME_DEFAULTS.autoAck
  #wait = CONSUME_DEFAULTS.wait
  #timeoutMillis = CONSUME_DEFAULTS.timeoutMillis
  #renewLease = CONSUME_DEFAULTS.renewLease
  #renewLeaseIntervalMillis = CONSUME_DEFAULTS.renewLeaseIntervalMillis
  #subscriptionMode = CONSUME_DEFAULTS.subscriptionMode
  #subscriptionFrom = CONSUME_DEFAULTS.subscriptionFrom
  #each = false
  #maxPartitions = 1

  // Buffer options
  #bufferOptions = null

  constructor(queen, httpClient, bufferManager, queueName = null) {
    this.#queen = queen
    this.#httpClient = httpClient
    this.#bufferManager = bufferManager
    this.#queueName = queueName
  }

  /**
   * Public read-only accessor for the queue name. Useful for tooling that
   * holds a QueueBuilder reference and needs to know the underlying name —
   * e.g. @queenmq/streams resolves sink-queue names from the QueueBuilder
   * passed to .to(...).
   */
  get name() {
    return this.#queueName
  }

  // ===========================
  // Affinity Key Generation
  // ===========================

  /**
   * Generate affinity key for consistent routing
   * Matches server's PollIntention::grouping_key() format
   * Format: queue:partition:consumerGroup or namespace:task:consumerGroup
   */
  #getAffinityKey() {
    if (this.#queueName) {
      // Queue-based routing: queue:partition:consumerGroup
      return `${this.#queueName}:${this.#partition || '*'}:${this.#group || '__QUEUE_MODE__'}`
    } else if (this.#namespace || this.#task) {
      // Namespace/task-based routing: namespace:task:consumerGroup
      return `${this.#namespace || '*'}:${this.#task || '*'}:${this.#group || '__QUEUE_MODE__'}`
    }
    return null
  }

  // ===========================
  // Queue Configuration Methods
  // ===========================

  namespace(name) {
    this.#namespace = name
    return this
  }

  task(name) {
    this.#task = name
    return this
  }

  config(options) {
    this.#config = { ...QUEUE_DEFAULTS, ...options }
    return this
  }

  create() {
    // Always merge with QUEUE_DEFAULTS to ensure all options are sent
    const fullConfig = Object.keys(this.#config).length > 0 
      ? this.#config 
      : QUEUE_DEFAULTS
    
    const payload = {
      queue: this.#queueName,
      namespace: this.#namespace,
      task: this.#task,
      options: fullConfig
    }

    logger.log('QueueBuilder.create', { queue: this.#queueName, namespace: this.#namespace, task: this.#task })
    return new OperationBuilder(this.#httpClient, 'POST', '/api/v1/configure', payload)
  }

  delete() {
    if (!this.#queueName) {
      throw new Error('Queue name is required for delete operation')
    }

    logger.log('QueueBuilder.delete', { queue: this.#queueName })
    return new OperationBuilder(
      this.#httpClient,
      'DELETE',
      `/api/v1/resources/queues/${encodeURIComponent(this.#queueName)}`,
      null
    )
  }

  // ===========================
  // Push Methods
  // ===========================

  partition(name) {
    this.#partition = name
    return this
  }

  buffer(options) {
    this.#bufferOptions = options
    return this
  }

  push(payload) {
    if (!this.#queueName) {
      throw new Error('Queue name is required for push operation')
    }

    logger.log('QueueBuilder.push', { queue: this.#queueName, partition: this.#partition, count: Array.isArray(payload) ? payload.length : 1, buffered: !!this.#bufferOptions })

    // Format items
    const items = Array.isArray(payload) ? payload : [payload]
    const formattedItems = items.map(item => {
      // Determine the payload - check if property exists, not just truthy
      let payloadValue
      if ('data' in item) {
        payloadValue = item.data
      } else if ('payload' in item) {
        payloadValue = item.payload
      } else {
        payloadValue = item
      }

      const result = {
        queue: this.#queueName,
        partition: this.#partition,
        payload: payloadValue,
        transactionId: item.transactionId || generateUUID()
      }

      // Include traceId if provided and valid UUID
      if (item.traceId && isValidUUID(item.traceId)) {
        result.traceId = item.traceId
      }

      return result
    })

    // Return a PushBuilder for chaining callbacks
    return new PushBuilder(this.#httpClient, this.#bufferManager, this.#queueName, this.#partition, formattedItems, this.#bufferOptions)
  }

  // ===========================
  // Consume Configuration Methods
  // ===========================

  group(name) {
    this.#group = name
    return this
  }

  concurrency(count) {
    this.#concurrency = Math.max(1, count)
    return this
  }

  batch(size) {
    this.#batch = Math.max(1, size)
    return this
  }

  /**
   * Pop messages from up to N partitions in a single call (v4 multi-partition pop).
   *
   * Use this to drain many sparsely-loaded partitions efficiently. With
   * `partitions(N)`, the global `batch(B)` budget is shared across all
   * claimed partitions: at most B total messages, drawn from up to N
   * partitions, in a single network round-trip. All N share one leaseId
   * (renewing once extends them all).
   *
   * Default 1 = legacy single-partition behavior.
   */
  partitions(n) {
    this.#maxPartitions = Math.max(1, n)
    return this
  }

  limit(count) {
    this.#limit = count
    return this
  }

  idleMillis(millis) {
    this.#idleMillis = millis
    return this
  }

  autoAck(enabled) {
    this.#autoAck = enabled
    return this
  }

  renewLease(enabled, intervalMillis) {
    this.#renewLease = enabled
    if (intervalMillis) {
      this.#renewLeaseIntervalMillis = intervalMillis
    }
    return this
  }

  subscriptionMode(mode) {
    this.#subscriptionMode = mode
    return this
  }

  subscriptionFrom(from) {
    this.#subscriptionFrom = from
    return this
  }

  each() {
    this.#each = true
    return this
  }

  // ===========================
  // Consume Method
  // ===========================

  consume(handler, options = {}) {
    const consumeOptions = {
      queue: this.#queueName,
      partition: this.#partition !== 'Default' ? this.#partition : null,
      namespace: this.#namespace,
      task: this.#task,
      group: this.#group,
      concurrency: this.#concurrency,
      batch: this.#batch,
      limit: this.#limit,
      idleMillis: this.#idleMillis,
      autoAck: this.#autoAck,
      wait: this.#wait,
      timeoutMillis: this.#timeoutMillis,
      renewLease: this.#renewLease,
      renewLeaseIntervalMillis: this.#renewLeaseIntervalMillis,
      subscriptionMode: this.#subscriptionMode,
      subscriptionFrom: this.#subscriptionFrom,
      each: this.#each,
      maxPartitions: this.#maxPartitions,
      signal: options.signal
    }

    return new ConsumeBuilder(this.#httpClient, this.#queen, handler, consumeOptions)
  }

  // ===========================
  // Pop Methods
  // ===========================

  wait(enabled) {
    this.#wait = enabled
    return this
  }

  /**
   * Set the long-poll timeout in milliseconds. Only meaningful when wait(true)
   * is also set. Default is CONSUME_DEFAULTS.timeoutMillis (30000ms).
   *
   * Use a short timeout in tight consumer loops where you want the long-poll
   * to release the request frequently (e.g. so a shutdown flag is observed
   * promptly). The HTTP layer adds 5s of slack so the server-side timeout
   * always fires first.
   */
  timeoutMillis(millis) {
    this.#timeoutMillis = Math.max(1, millis)
    return this
  }

  async pop() {
    logger.log('QueueBuilder.pop', { queue: this.#queueName, partition: this.#partition, namespace: this.#namespace, task: this.#task, batch: this.#batch, wait: this.#wait, group: this.#group })
    
    try {
      const path = this.#buildPopPath()
      
      // For pop(), use POP defaults (not CONSUME defaults)
      // Override autoAck to false unless explicitly set
      const effectiveAutoAck = this.#autoAck !== CONSUME_DEFAULTS.autoAck ? this.#autoAck : POP_DEFAULTS.autoAck
      
      // Build params with correct autoAck for pop
      const params = new URLSearchParams({
        batch: this.#batch.toString(),
        wait: this.#wait.toString(),
        timeout: this.#timeoutMillis.toString()
      })

      if (this.#group) params.append('consumerGroup', this.#group)
      if (this.#namespace) params.append('namespace', this.#namespace)
      if (this.#task) params.append('task', this.#task)
      if (effectiveAutoAck) params.append('autoAck', 'true')
      if (this.#subscriptionMode) params.append('subscriptionMode', this.#subscriptionMode)
      if (this.#subscriptionFrom) params.append('subscriptionFrom', this.#subscriptionFrom)
      if (this.#maxPartitions > 1) params.append('partitions', this.#maxPartitions.toString())

      // Generate affinity key for consistent routing to same backend
      const affinityKey = this.#getAffinityKey()
      
      const result = await this.#httpClient.get(`${path}?${params}`, this.#timeoutMillis + 5000, affinityKey)

      if (!result || !result.messages) {
        logger.log('QueueBuilder.pop', { status: 'no-messages' })
        return []
      }

      const messages = result.messages.filter(msg => msg != null)
      logger.log('QueueBuilder.pop', { status: 'success', count: messages.length })
      return messages
    } catch (error) {
      // Return empty array on error instead of throwing
      logger.error('QueueBuilder.pop', { error: error.message })
      return []
    }
  }

  #buildPopPath() {
    if (this.#queueName) {
      if (this.#partition && this.#partition !== 'Default') {
        return `/api/v1/pop/queue/${this.#queueName}/partition/${this.#partition}`
      }
      return `/api/v1/pop/queue/${this.#queueName}`
    }

    if (this.#namespace || this.#task) {
      return '/api/v1/pop'
    }

    throw new Error('Must specify queue, namespace, or task for pop operation')
  }

  #buildPopParams() {
    const params = new URLSearchParams({
      batch: this.#batch.toString(),
      wait: this.#wait.toString(),
      timeout: this.#timeoutMillis.toString()  // Server expects 'timeout', not 'timeoutMillis'
    })

    if (this.#group) params.append('consumerGroup', this.#group)
    if (this.#namespace) params.append('namespace', this.#namespace)
    if (this.#task) params.append('task', this.#task)
    if (this.#autoAck) params.append('autoAck', 'true')
    if (this.#subscriptionMode) params.append('subscriptionMode', this.#subscriptionMode)
    if (this.#subscriptionFrom) params.append('subscriptionFrom', this.#subscriptionFrom)

    return params
  }

  // ===========================
  // Buffer Management Methods
  // ===========================

  async flushBuffer() {
    if (!this.#queueName) {
      throw new Error('Queue name is required for buffer flush')
    }
    const queueAddress = `${this.#queueName}/${this.#partition}`
    logger.log('QueueBuilder.flushBuffer', { queueAddress })
    await this.#bufferManager.flushBuffer(queueAddress)
  }

  // ===========================
  // Dead Letter Queue Methods
  // ===========================

  dlq(consumerGroup = null) {
    if (!this.#queueName) {
      throw new Error('Queue name is required for DLQ operations')
    }
    logger.log('QueueBuilder.dlq', { queue: this.#queueName, consumerGroup, partition: this.#partition })
    return new DLQBuilder(this.#httpClient, this.#queueName, consumerGroup, this.#partition)
  }
}

/**
 * Operation builder for create/delete operations with callbacks
 */
class OperationBuilder {
  #httpClient
  #method
  #path
  #body
  #onSuccessCallback = null
  #onErrorCallback = null
  #executed = false

  constructor(httpClient, method, path, body) {
    this.#httpClient = httpClient
    this.#method = method
    this.#path = path
    this.#body = body
  }

  onSuccess(callback) {
    this.#onSuccessCallback = callback
    return this
  }

  onError(callback) {
    this.#onErrorCallback = callback
    return this
  }

  // Auto-execute when awaited
  then(resolve, reject) {
    if (this.#executed) {
      return Promise.resolve().then(resolve, reject)
    }
    this.#executed = true
    return this.#execute().then(resolve, reject)
  }

  async #execute() {
    logger.log('OperationBuilder.execute', { method: this.#method, path: this.#path })
    
    try {
      let result

      if (this.#method === 'GET') {
        result = await this.#httpClient.get(this.#path)
      } else if (this.#method === 'POST') {
        result = await this.#httpClient.post(this.#path, this.#body)
      } else if (this.#method === 'PUT') {
        result = await this.#httpClient.put(this.#path, this.#body)
      } else if (this.#method === 'DELETE') {
        result = await this.#httpClient.delete(this.#path)
      }

      if (result && result.error) {
        const error = new Error(result.error)
        logger.error('OperationBuilder.execute', { method: this.#method, path: this.#path, error: result.error })
        if (this.#onErrorCallback) {
          await this.#onErrorCallback(error)
          return { success: false, error: result.error }
        }
        throw error
      }

      logger.log('OperationBuilder.execute', { method: this.#method, path: this.#path, status: 'success' })
      if (this.#onSuccessCallback) {
        await this.#onSuccessCallback(result)
      }

      return result

    } catch (error) {
      logger.error('OperationBuilder.execute', { method: this.#method, path: this.#path, error: error.message })
      if (this.#onErrorCallback) {
        await this.#onErrorCallback(error)
        return { success: false, error: error.message }
      }
      throw error
    }
  }
}

/**
 * Consume builder for chaining callbacks
 */
class ConsumeBuilder {
  #httpClient
  #queen
  #handler
  #options
  #onSuccessCallback = null
  #onErrorCallback = null
  #executed = false

  constructor(httpClient, queen, handler, options) {
    this.#httpClient = httpClient
    this.#queen = queen
    this.#handler = handler
    this.#options = options
  }

  onSuccess(callback) {
    this.#onSuccessCallback = callback
    return this
  }

  onError(callback) {
    this.#onErrorCallback = callback
    return this
  }

  // Auto-execute when awaited
  then(resolve, reject) {
    if (this.#executed) {
      return Promise.resolve().then(resolve, reject)
    }
    this.#executed = true
    return this.#execute().then(resolve, reject)
  }

  async #execute() {
    // Import ConsumerManager lazily to avoid circular dependency
    const { ConsumerManager } = await import('../consumer/ConsumerManager.js')
    const consumerManager = new ConsumerManager(this.#httpClient, this.#queen)

    // Wrap the handler to include callback logic
    const wrappedHandler = async (msgOrMsgs) => {
      try {
        const result = await this.#handler(msgOrMsgs)
        
        // Call onSuccess if defined
        if (this.#onSuccessCallback) {
          await this.#onSuccessCallback(msgOrMsgs, result)
        }
        
        return result
      } catch (error) {
        // Call onError if defined
        if (this.#onErrorCallback) {
          await this.#onErrorCallback(msgOrMsgs, error)
          // Don't re-throw if callback is defined
          return
        }
        // Re-throw if no error callback
        throw error
      }
    }

    // IMPORTANT: If callbacks are defined, auto-ack must be disabled
    // to prevent double-acking (auto-ack + manual ack in callback)
    const hasCallbacks = this.#onSuccessCallback || this.#onErrorCallback
    const effectiveAutoAck = hasCallbacks ? false : this.#options.autoAck

    const updatedOptions = {
      ...this.#options,
      autoAck: effectiveAutoAck
    }

    return consumerManager.start(wrappedHandler, updatedOptions)
  }
}

/**
 * Push builder for chaining callbacks
 */
class PushBuilder {
  #httpClient
  #bufferManager
  #queueName
  #partition
  #formattedItems
  #bufferOptions
  #onSuccessCallback = null
  #onErrorCallback = null
  #onDuplicateCallback = null
  #executed = false

  constructor(httpClient, bufferManager, queueName, partition, formattedItems, bufferOptions) {
    this.#httpClient = httpClient
    this.#bufferManager = bufferManager
    this.#queueName = queueName
    this.#partition = partition
    this.#formattedItems = formattedItems
    this.#bufferOptions = bufferOptions
  }

  onSuccess(callback) {
    this.#onSuccessCallback = callback
    return this
  }

  onError(callback) {
    this.#onErrorCallback = callback
    return this
  }

  onDuplicate(callback) {
    this.#onDuplicateCallback = callback
    return this
  }

  // Auto-execute when awaited
  then(resolve, reject) {
    if (this.#executed) {
      return Promise.resolve().then(resolve, reject)
    }
    this.#executed = true
    return this.#execute().then(resolve, reject)
  }

  async #execute() {
    logger.log('PushBuilder.execute', { queue: this.#queueName, partition: this.#partition, count: this.#formattedItems.length, buffered: !!this.#bufferOptions })
    
    // Client-side buffering
    if (this.#bufferOptions) {
      for (const item of this.#formattedItems) {
        const queueAddress = `${this.#queueName}/${this.#partition}`
        this.#bufferManager.addMessage(queueAddress, item, this.#bufferOptions)
      }
      const result = { buffered: true, count: this.#formattedItems.length }
      
      logger.log('PushBuilder.execute', { status: 'buffered', count: this.#formattedItems.length })
      
      if (this.#onSuccessCallback) {
        await this.#onSuccessCallback(this.#formattedItems)
      }
      
      return result
    }

    // Immediate push
    try {
      const results = await this.#httpClient.post('/api/v1/push', { items: this.#formattedItems })

      // Server returns an array of results with status for each item
      if (Array.isArray(results)) {
        // Separate results by status
        const successful = []
        const duplicates = []
        const failed = []

        for (let i = 0; i < results.length; i++) {
          const result = results[i]
          const originalItem = this.#formattedItems[i]

          if (result.status === 'duplicate') {
            duplicates.push({ ...originalItem, result })
          } else if (result.status === 'failed') {
            failed.push({ ...originalItem, result, error: result.error })
          } else if (result.status === 'queued') {
            successful.push({ ...originalItem, result })
          }
        }

        // Call appropriate callbacks
        if (duplicates.length > 0 && this.#onDuplicateCallback) {
          await this.#onDuplicateCallback(duplicates, new Error('Duplicate transaction IDs detected'))
        }

        if (failed.length > 0 && this.#onErrorCallback) {
          const error = new Error(failed[0].error || 'Push failed')
          await this.#onErrorCallback(failed, error)
        }

        if (successful.length > 0 && this.#onSuccessCallback) {
          await this.#onSuccessCallback(successful)
        }

        // Only throw if no error callback is defined
        if (failed.length > 0 && !this.#onErrorCallback) {
          logger.error('PushBuilder.execute', { status: 'failed', count: failed.length })
          throw new Error(failed[0].error || 'Push failed')
        }

        logger.log('PushBuilder.execute', { status: 'success', successful: successful.length, duplicates: duplicates.length, failed: failed.length })
        return results
      }

      // Fallback for non-array responses
      if (results && results.error) {
        const error = new Error(results.error)
        if (this.#onErrorCallback) {
          await this.#onErrorCallback(this.#formattedItems, error)
          return results // Don't throw if callback is defined
        }
        throw error
      }

      if (this.#onSuccessCallback) {
        await this.#onSuccessCallback(this.#formattedItems)
      }

      return results

    } catch (error) {
      // Network or HTTP errors
      if (this.#onErrorCallback) {
        await this.#onErrorCallback(this.#formattedItems, error)
        return null // Don't throw if callback is defined
      }
      throw error
    }
  }
}

/**
 * DLQ (Dead Letter Queue) builder for querying failed messages
 */
class DLQBuilder {
  #httpClient
  #queueName
  #consumerGroup
  #partition
  #limit = 100
  #offset = 0
  #from = null
  #to = null

  constructor(httpClient, queueName, consumerGroup, partition) {
    this.#httpClient = httpClient
    this.#queueName = queueName
    this.#consumerGroup = consumerGroup
    this.#partition = partition !== 'Default' ? partition : null
  }

  limit(count) {
    this.#limit = Math.max(1, count)
    return this
  }

  offset(count) {
    this.#offset = Math.max(0, count)
    return this
  }

  from(timestamp) {
    this.#from = timestamp
    return this
  }

  to(timestamp) {
    this.#to = timestamp
    return this
  }

  async get() {
    const params = new URLSearchParams()
    
    params.append('queue', this.#queueName)
    params.append('limit', this.#limit.toString())
    params.append('offset', this.#offset.toString())
    
    if (this.#consumerGroup) {
      params.append('consumerGroup', this.#consumerGroup)
    }
    
    if (this.#partition) {
      params.append('partition', this.#partition)
    }
    
    if (this.#from) {
      params.append('from', this.#from)
    }
    
    if (this.#to) {
      params.append('to', this.#to)
    }

    logger.log('DLQBuilder.get', { queue: this.#queueName, consumerGroup: this.#consumerGroup, partition: this.#partition, limit: this.#limit, offset: this.#offset })

    try {
      const result = await this.#httpClient.get(`/api/v1/dlq?${params}`)
      logger.log('DLQBuilder.get', { status: 'success', total: result?.total || 0, messages: result?.messages?.length || 0 })
      return result || { messages: [], total: 0 }
    } catch (error) {
      logger.error('DLQBuilder.get', { error: error.message })
      return { messages: [], total: 0 }
    }
  }
}

