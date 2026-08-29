/**
 * Queue builder for fluent API
 */

import { v7 as uuidv7 } from 'uuid';

export const generateUUID = () => {
  return uuidv7();
};

//import { generateUUID } from '../../utils/uuid.js'
import { isValidUUID } from '../utils/validation.js'
import { durableAddress } from '../buffer/sinks.js'
import { QUEUE_DEFAULTS, CONSUME_DEFAULTS, POP_DEFAULTS } from '../utils/defaults.js'
import { checkConflationResponse, CONFLATION_UNSUPPORTED } from '../utils/conflation.js'
import { popSizing, parseAutopilotDecision } from '../utils/autopilot.js'
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
  // batch / maxPartitions hold the USER's value, and null means the setter was
  // never called -- which is the dimension pop autopilot gets to choose. The
  // client-side defaults are applied at emission time (utils/autopilot.js), not
  // here, because filling them in here would erase the difference between
  // "never called batch()" and "called batch(1)".
  #batch = null
  #limit = CONSUME_DEFAULTS.limit
  #idleMillis = CONSUME_DEFAULTS.idleMillis
  #autoAck = CONSUME_DEFAULTS.autoAck
  #wait = CONSUME_DEFAULTS.wait
  #timeoutMillis = CONSUME_DEFAULTS.timeoutMillis
  #renewLease = CONSUME_DEFAULTS.renewLease
  #renewLeaseIntervalMillis = CONSUME_DEFAULTS.renewLeaseIntervalMillis
  #subscriptionMode = CONSUME_DEFAULTS.subscriptionMode
  #subscriptionFrom = CONSUME_DEFAULTS.subscriptionFrom
  #conflation = CONSUME_DEFAULTS.conflation
  #each = false
  #maxPartitions = null
  // Per-call override for pop autopilot: null = the client default (on unless
  // QUEEN_SDK_POP_AUTOPILOT turned it off).
  #autopilot = null

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

  /**
   * Batch pushes client-side instead of sending each one.
   *
   * @param {object} options
   * @param {number} [options.messageCount=100] - flush once this many messages are waiting
   * @param {number} [options.timeMillis=1000] - flush this long after the first message arrives
   * @param {number} [options.maxSize] - backpressure bound: past this many buffered
   *   messages `push()` WAITS for the flusher instead of growing the heap.
   *   Defaults to 4 x messageCount, floored at messageCount. There is no
   *   unbounded setting: unbounded is what lost 20.9M messages in the
   *   2026-08-20 measurement.
   * @param {number} [options.retryDelayMillis=250] - delay before retrying a batch
   *   whose POST failed. Failed batches are re-queued at the front of the
   *   buffer and retried, never dropped.
   */
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

  /**
   * Pin the message budget for one pop. Leave it unset and the broker sizes it
   * (see `autopilot`), where it used to mean the client-side default of 1.
   *
   * `batch(0)` is not "a batch of zero" and never was: it is the absence of an
   * opinion, so it reads as unset.
   */
  batch(size) {
    this.#batch = size > 0 ? size : null
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
   * Leave it unset and the broker chooses the sweep width (see `autopilot`);
   * `partitions(1)` pins the legacy single-partition behaviour, which is a
   * decision the broker is told about and never overrides.
   */
  partitions(n) {
    this.#maxPartitions = n > 0 ? n : null
    return this
  }

  /**
   * Turn broker-side pop sizing on or off for this builder.
   *
   * On (the default) the broker chooses `batch` and `partitions` for the pops
   * of this builder. Even then, a `batch` or `partitions` set explicitly
   * travels on the wire as it always did and is never second-guessed: autopilot
   * only ever fills the knobs left unset.
   *
   * `autopilot(false)` restores this SDK's pre-1.2 behaviour byte for byte: the
   * client-side defaults come back (batch 1, partitions 1) and no autopilot
   * parameter is sent. QUEEN_SDK_POP_AUTOPILOT=off does the same for a whole
   * process; an explicit call here outranks the environment in both directions.
   *
   * Setting BOTH batch and partitions leaves autopilot nothing to decide, so no
   * autopilot parameter is sent in that case either, whatever this flag says.
   */
  autopilot(enabled = true) {
    this.#autopilot = !!enabled
    return this
  }

  /**
   * This builder's resolved autopilot decision: its own flag when set,
   * otherwise the client-wide default settled in the Queen constructor.
   */
  #autopilotEnabled() {
    if (this.#autopilot !== null) return this.#autopilot
    return !this.#queen || !this.#queen.autopilotOff
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

  /**
   * Last-value delivery for this consumer group (PLAN_CONFLATION §1.1).
   *
   * A pop of a partition delivers exactly ONE message — the newest visible one
   * — and commits past everything it skipped. For command-style queues where
   * one partition is one logical task key ("recompute entity X"), a consumer
   * behind a backlog then does the work once with the latest input instead of
   * replaying every stale intermediate.
   *
   * It is a property of the GROUP, not of the call: it is persisted when the
   * group first registers on the queue, and from then on the stored value wins
   * for every consumer of that group. Declaring the opposite later does not
   * flip it — the SDK warns once and keeps working. Default off; a group
   * created without it behaves exactly as before.
   *
   * Requires broker >= 1.1.0. An older broker ignores the parameter and would
   * quietly deliver the whole backlog, so the SDK raises on the first response
   * that does not echo the flag rather than draining it silently.
   *
   * Refused by the broker (400) when combined with queue mode (no consumer
   * group) or with autoAck, which commits at delivery and would turn the
   * "the newest state is definitely processed" guarantee into at-most-once.
   */
  conflation(enabled = true) {
    this.#conflation = !!enabled
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
      conflation: this.#conflation,
      each: this.#each,
      maxPartitions: this.#maxPartitions,
      // Resolved here so ConsumerManager sees a decision and not a null. batch
      // and maxPartitions keep their null when autopilot is on, and that null
      // has to survive all the way to #buildParams: it is the ONLY record that
      // the user said nothing about that dimension.
      autopilot: this.#autopilotEnabled(),
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

  /**
   * Claim messages and report what the broker chose for this pop.
   *
   * Same call as `pop()` — this is the shape that also carries the additive
   * `autopilot` echo, which is null when this pop did not engage autopilot or
   * the broker is older than 1.2.
   *
   *   const { messages, autopilot } = await client.queue('events').group('w').popResult()
   *   if (autopilot) console.log(autopilot.partitions, autopilot.batch, autopilot.waitMillis)
   *
   * @returns {Promise<{messages: object[], autopilot: {partitions: number, batch: number, waitMillis: number}|null}>}
   */
  async popResult() {
    return this.#popWithDecision()
  }

  async pop() {
    return (await this.#popWithDecision()).messages
  }

  async #popWithDecision() {
    logger.log('QueueBuilder.pop', { queue: this.#queueName, partition: this.#partition, namespace: this.#namespace, task: this.#task, batch: this.#batch, wait: this.#wait, group: this.#group })
    
    try {
      const path = this.#buildPopPath()
      
      // For pop(), use POP defaults (not CONSUME defaults)
      // Override autoAck to false unless explicitly set
      const effectiveAutoAck = this.#autoAck !== CONSUME_DEFAULTS.autoAck ? this.#autoAck : POP_DEFAULTS.autoAck
      
      // Batch, partitions and with them the autopilot flag. The RULE for which
      // of the three travel lives in one place (utils/autopilot.js) because
      // consume() builds its query string separately; only the PLACEMENT is
      // here, and it is the pre-autopilot placement so an autopilot-off request
      // is byte-identical to the one this SDK used to send.
      const sizing = popSizing({
        batch: this.#batch,
        maxPartitions: this.#maxPartitions,
        fallbackBatch: POP_DEFAULTS.batch,
        autopilot: this.#autopilotEnabled()
      })

      // Build params with correct autoAck for pop
      const params = new URLSearchParams()
      if (sizing.autopilot) params.append('autopilot', 'true')
      if (sizing.batch !== null) params.append('batch', sizing.batch)
      params.append('wait', this.#wait.toString())
      params.append('timeout', this.#timeoutMillis.toString())

      if (this.#group) params.append('consumerGroup', this.#group)
      if (this.#namespace) params.append('namespace', this.#namespace)
      if (this.#task) params.append('task', this.#task)
      if (effectiveAutoAck) params.append('autoAck', 'true')
      if (this.#subscriptionMode) params.append('subscriptionMode', this.#subscriptionMode)
      if (this.#subscriptionFrom) params.append('subscriptionFrom', this.#subscriptionFrom)
      if (sizing.partitions !== null) params.append('partitions', sizing.partitions)
      // Conflation (PLAN_CONFLATION §3.1): sent ONLY when true, so an
      // undeclared pop is byte-identical to today. NOTE: this is the pop
      // builder; consume() builds its params in ConsumerManager#buildParams —
      // see the comment below #buildPopPath about exactly this hazard.
      if (this.#conflation) params.append('conflation', 'true')

      // Generate affinity key for consistent routing to same backend
      const affinityKey = this.#getAffinityKey()

      // wait=true is a long-poll: on 429 it should back off and keep waiting
      // rather than give up after a handful of tries (retryKind: 'pop').
      const result = await this.#httpClient.get(`${path}?${params}`, this.#timeoutMillis + 5000, affinityKey, this.#wait ? 'pop' : null)

      // Degrade-loudly (PLAN_CONFLATION §4), BEFORE the empty-response return:
      // an old broker's empty pop is a bodiless 204 (result === null), and that
      // is precisely the first thing a consumer on an idle queue sees. Also
      // where a declaration conflict is warned about, exactly once.
      if (this.#conflation) {
        checkConflationResponse(result, {
          queue: this.#queueName,
          namespace: this.#namespace,
          task: this.#task,
          group: this.#group
        })
      }

      // The broker's own account of how it sized this pop, when the request
      // engaged autopilot and the answer had a body to carry it (a bodiless 204
      // cannot, so an empty short pop reports null).
      const autopilot = parseAutopilotDecision(result)

      if (!result || !result.messages) {
        logger.log('QueueBuilder.pop', { status: 'no-messages' })
        return { messages: [], autopilot }
      }

      const messages = result.messages.filter(msg => msg != null)
      logger.log('QueueBuilder.pop', { status: 'success', count: messages.length })
      return { messages, autopilot }
    } catch (error) {
      // Conflation is the one thing this method does NOT swallow. The
      // swallow-to-[] contract exists for transport faults, where [] means "no
      // messages right now"; for a declared conflation it would mean "your
      // last-value policy is not in force and you will never be told", which is
      // the silent failure the feature is not allowed to have (§4). Both the
      // missing-echo error and the broker's 400 refusals (queue mode / autoAck)
      // are permanent config faults, so they raise.
      if (error.code === CONFLATION_UNSUPPORTED || (this.#conflation && error.status === 400)) {
        logger.error('QueueBuilder.pop', { error: error.message, status: error.status, code: error.code, conflation: true })
        throw error
      }
      // Return empty array on error instead of throwing. This also covers a
      // 429 whose retry429 policy was exhausted (bounded pop, or an explicit
      // maxAttempts override) and a terminal 403 (e.g. cluster_suspended) --
      // both are logged with their `.code` rather than raising, matching this
      // method's existing swallow-to-[] contract.
      logger.error('QueueBuilder.pop', { error: error.message, status: error.status, code: error.code })
      return { messages: [], autopilot: null }
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

  // NOTE: a second, DEAD copy of the pop parameter builder lived here and was
  // deleted with the kv/timers work (PLAN_KV_TIMERS.md §10.4). pop() builds its
  // own params inline, above, because it has to override autoAck with the POP
  // defaults; the dead copy did not. Anyone adding a parameter by looking for
  // the method whose name says "build pop params" would have added it to the
  // copy nobody calls: the pop would keep working and the parameter would
  // simply never arrive, which reads as a server-side mystery and not as a
  // client bug.
  //
  // The pair that is still live and MUST be kept in sync is pop()'s inline
  // params above and ConsumerManager#buildParams: every pop query parameter
  // (subscriptionMode, subscriptionFrom, partitions, conflation, ...) has to be
  // appended in BOTH, because pop() and consume() share no builder.

  // ===========================
  // Buffer Management Methods
  // ===========================

  async flushBuffer() {
    if (!this.#queueName) {
      throw new Error('Queue name is required for buffer flush')
    }
    const queueAddress = durableAddress(this.#queueName, this.#partition)
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
    
    // Client-side buffering. Awaited, one message at a time: addMessage is
    // where the maxSize backpressure bound is applied, so a buffered push
    // resolves only once every item is actually IN the buffer. Firing these
    // off without awaiting would report success for messages the buffer never
    // accepted -- the exact failure this bound exists to remove.
    if (this.#bufferOptions) {
      // No destination: the durable push is the default sink, so this address's
      // buffer drains to POST /api/v1/push with a `{items}` body exactly as it
      // did before sinks existed (buffer/sinks.js).
      const queueAddress = durableAddress(this.#queueName, this.#partition)
      const accepted = []

      try {
        for (const item of this.#formattedItems) {
          await this.#bufferManager.addMessage(queueAddress, item, this.#bufferOptions)
          accepted.push(item)
        }
      } catch (error) {
        // The buffer refused this message (the client is closing, or the wait
        // for capacity was aborted). Report the items that did NOT make it, the
        // same way the immediate push below reports rejected items -- counting
        // them as buffered would be the false success this bound removes.
        const unbuffered = this.#formattedItems.slice(accepted.length)
        logger.error('PushBuilder.execute', { status: 'not-buffered', count: unbuffered.length, error: error.message })
        if (this.#onErrorCallback) {
          await this.#onErrorCallback(unbuffered, error)
          return null
        }
        throw error
      }

      const result = { buffered: true, count: accepted.length }

      logger.log('PushBuilder.execute', { status: 'buffered', count: accepted.length })

      if (this.#onSuccessCallback) {
        await this.#onSuccessCallback(accepted)
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

