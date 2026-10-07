/**
 * Consumer manager for handling concurrent workers
 */

import * as logger from '../utils/logger.js'
import { Supervision } from './Supervision.js'
import { checkConflationResponse, CONFLATION_UNSUPPORTED } from '../utils/conflation.js'
import { popSizing, parseAutopilotDecision, emptyPollDelayMillis } from '../utils/autopilot.js'
import { CONSUME_DEFAULTS } from '../utils/defaults.js'

/** Wait `ms`, or less if the consumer is stopped meanwhile: the loop checks the signal next. */
function pause(ms, signal) {
  if (signal?.aborted) return Promise.resolve()
  return new Promise(resolve => {
    const done = () => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    signal?.addEventListener('abort', done, { once: true })
  })
}

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
      conflation,
      each,
      maxPartitions,
      autopilot,
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
    const baseParams = this.#buildParams(batch, wait, timeoutMillis, group, subscriptionMode, subscriptionFrom, namespace, task, autoAck, maxPartitions, conflation, this.#autopilotEnabled(autopilot))
    
    // Generate affinity key for consistent routing to same backend
    const affinityKey = this.#getAffinityKey(queue, partition, namespace, task, group)

    const supervision = options.supervision ? new Supervision(this.#httpClient, options.supervision, options) : null
    if (supervision) handler = supervision.wrap(handler)

    // Start workers
    const workers = []
    for (let i = 0; i < concurrency; i++) {
      if (supervision) supervision.running++
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
        affinityKey,  // Pass affinity key to workers
        // Conflation was REQUESTED by this consumer: the worker has to check
        // every response for the broker's echo (PLAN_CONFLATION §4) and needs
        // the pop target to key the once-per-(queue,group) conflict warning.
        conflation,
        conflationCtx: { queue, namespace, task, group }
      }).finally(async () => {
        if (supervision && --supervision.running === 0) await supervision.stop()
      }))
    }

    supervision?.start()
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
      affinityKey,
      conflation,
      conflationCtx
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
        // The signal closes the poll as well: a broker hands nothing to a poll
        // whose caller is gone, so a stopped consumer is never given a message
        // it would only sit on until the lease expires.
        const result = await this.#httpClient.get(`${path}?${baseParams}`, clientTimeout, affinityKey, wait ? 'pop' : null, signal)

        // Degrade-loudly (PLAN_CONFLATION §4). Checked BEFORE the empty-response
        // branch on purpose: a pre-1.1.0 broker answers an empty pop with a
        // bodiless 204 (result === null), which is the first thing a consumer
        // on an idle queue sees — and the whole point is to raise before a
        // single message of a backlog is processed one-by-one. Throwing here
        // leaves the loop through the catch below, which stops this worker.
        if (conflation) {
          checkConflationResponse(result, conflationCtx)
        }

        // Handle empty response
        if (!result || !result.messages || result.messages.length === 0) {
          if (wait) {
            continue // Long polling timeout, retry
          } else {
            // Short delay before retry -- the broker's advised pacing when this
            // pop engaged autopilot and the broker had an opinion (it knows the
            // arrival rate on this queue and this client does not), otherwise
            // the historical 100ms.
            await pause(emptyPollDelayMillis(parseAutopilotDecision(result)), signal)
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
            // Process one at a time. A nack releases the failed message's
            // partition and clamps that partition's cursor at it: the later
            // messages of THAT partition will be redelivered, so handling them
            // now would only produce duplicates and rejected acks. The other
            // partitions of a multi-partition pop are still leased to this
            // worker, so their messages are handled now, not after the lease.
            const nackedPartitions = new Set()
            for (const [i, message] of messages.entries()) {
              // Stopped, or the limit reached, with messages still in hand:
              // give them back rather than leave them leased. Those of a
              // nacked partition were already given back by the nack.
              if ((signal && signal.aborted) || (limit && processedCount >= limit)) {
                const unstarted = messages.slice(i).filter(m => !nackedPartitions.has(m.partitionId ?? m.partition))
                if (unstarted.length > 0) {
                  await this.#releaseUnstarted(unstarted, group, signal && signal.aborted ? 'aborted' : 'limit-reached')
                }
                break
              }

              const partition = message.partitionId ?? message.partition
              if (nackedPartitions.has(partition)) continue

              const ok = await this.#processMessage(message, handler, autoAck, group)
              processedCount++

              if (!ok) {
                nackedPartitions.add(partition)
                logger.warn('ConsumerManager.worker', { workerId, status: 'partition-abandoned-after-nack', partition })
              }
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
        // Stopped while a poll was open: the poll was closed, nothing was
        // taken, the worker is done. Not an error, whatever the wait mode.
        if (error.aborted || (signal && signal.aborted)) {
          logger.log('ConsumerManager.worker', { workerId, status: 'aborted', processedCount })
          break
        }

        // Conflation faults are terminal and are classified FIRST, ahead of the
        // message-substring heuristics below: a consumer that asked for
        // last-value delivery and is not getting it must stop, not retry
        // (PLAN_CONFLATION §4). The broker's 400 refusals (queue mode /
        // autoAck) are permanent config faults and stop the loop for the same
        // reason — retrying them forever would be the silent version.
        if (error.code === CONFLATION_UNSUPPORTED || (conflation && error.status === 400)) {
          logger.error('ConsumerManager.worker', { workerId, status: 'conflation-unavailable', code: error.code, httpStatus: error.status, error: error.message })
          throw error
        }

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
          await pause(retryAfterMs, signal)
          continue
        }

        // Check if network error
        const isNetworkError = error.message?.includes('fetch failed') ||
                              error.message?.includes('ECONNREFUSED') ||
                              error.code === 'ECONNREFUSED'

        if (isNetworkError) {
          logger.warn('ConsumerManager.worker', { workerId, error: 'network', message: error.message })
          // Wait before retry
          await pause(1000, signal)
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
    } catch (error) {
      await this.#nackFailed(message, error, autoAck, group, 'ConsumerManager.processMessage')
      return false
    }

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
  }

  /**
   * Hand back messages this worker holds but will not process (it was stopped,
   * or reached its limit): a `retry` ack releases their lease, the broker
   * redelivers them first, in order, and charges no retry. Best effort -- a
   * release that fails leaves the message to its lease, as before.
   */
  async #releaseUnstarted(messages, group, reason) {
    const subject = { count: messages.length, transactionIds: messages.map(m => m.transactionId) }
    try {
      const res = await this.#queen.ack(messages, 'retry', group ? { group } : {})
      if (res && res.success === false) {
        logger.warn('ConsumerManager.release', { ...subject, reason, status: 'release-rejected', error: res.error })
      } else {
        logger.log('ConsumerManager.release', { ...subject, reason, status: 'released' })
      }
    } catch (error) {
      logger.warn('ConsumerManager.release', { ...subject, reason, status: 'release-failed', error: error.message })
    }
  }

  async #processBatch(messages, handler, autoAck, group) {
    try {
      await handler(messages)
    } catch (error) {
      await this.#nackFailed(messages, error, autoAck, group, 'ConsumerManager.processBatch')
      return
    }

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
  }

  /**
   * A handler threw: nack what it was given, and let the worker keep
   * consuming. The same with `autoAck(false)`, which hands the SUCCESS path to
   * the handler and not the failure path: a handler that threw did not get to
   * settle its messages, and leaving them leased would hold the partition
   * until the lease expires. Before this, a throw under autoAck(false) left
   * the worker loop and stopped the consumer (the others kept running behind a
   * consume() promise that had already rejected).
   *
   * The nack goes through the broker's retry budget like any other: the
   * message is redelivered, and lands in the DLQ once the queue's retryLimit
   * is spent. A message the handler already acked before throwing is already
   * settled, so the broker refuses its nack and nothing changes for it. A
   * handler that wants to decide for itself (ack, nack, DLQ, or stop)
   * declares `.onError()`, which catches the error before it gets here.
   */
  async #nackFailed(messageOrMessages, error, autoAck, group, where) {
    const batch = Array.isArray(messageOrMessages)
    const subject = batch ? { count: messageOrMessages.length } : { transactionId: messageOrMessages.transactionId }
    // A handler can throw anything, not only an Error.
    const reason = error instanceof Error ? error.message : String(error)
    logger.error(where, { ...subject, error: reason, status: 'handler-failed', autoAck })

    const context = group ? { group, error: reason } : { error: reason }
    try {
      const res = await this.#queen.ack(messageOrMessages, false, context)
      if (res && res.success === false) {
        // Under autoAck(false) this is usually a handler that acked before it
        // threw; with autoAck it means the lease ran out under the handler.
        const log = autoAck ? logger.error : logger.warn
        log(where, { ...subject, status: 'nack-rejected', error: res.error })
      } else {
        logger.log(where, { ...subject, status: 'nacked' })
      }
    } catch (nackError) {
      // A message this client cannot even address (no partitionId) cannot be
      // nacked; its lease expires and the broker redelivers it. Never a reason
      // to stop the consumer.
      logger.error(where, { ...subject, status: 'nack-failed', error: nackError.message })
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

  /**
   * The autopilot decision for one consume: the caller's explicit option if
   * there is one, otherwise the client-wide default settled in the Queen
   * constructor. The builder path has already resolved it; the undefined case
   * is for callers that drive ConsumerManager with options of their own.
   */
  #autopilotEnabled(autopilot) {
    if (typeof autopilot === 'boolean') return autopilot
    return !this.#queen || !this.#queen.autopilotOff
  }

  #buildParams(batch, wait, timeoutMillis, group, subscriptionMode, subscriptionFrom, namespace, task, autoAck, maxPartitions, conflation, autopilot = true) {
    // Batch, partitions and with them the autopilot flag. null/0 means the user
    // set nothing (QueueBuilder leaves it that way on purpose), which is the
    // dimension the broker gets to choose. THE RULE lives in one place
    // (utils/autopilot.js) precisely because this is the SECOND parameter
    // builder; only the placement of the keys is here, and it is the
    // pre-autopilot placement so an autopilot-off request is byte-identical.
    const sizing = popSizing({
      batch,
      maxPartitions,
      fallbackBatch: CONSUME_DEFAULTS.batch,
      autopilot
    })

    const params = new URLSearchParams()
    if (sizing.autopilot) params.append('autopilot', 'true')
    if (sizing.batch !== null) params.append('batch', sizing.batch)
    params.append('wait', wait.toString())
    params.append('timeout', timeoutMillis.toString())  // Server expects 'timeout', not 'timeoutMillis'

    if (group) params.append('consumerGroup', group)
    if (subscriptionMode) params.append('subscriptionMode', subscriptionMode)
    if (subscriptionFrom) params.append('subscriptionFrom', subscriptionFrom)
    if (namespace) params.append('namespace', namespace)
    if (task) params.append('task', task)
    // v4 multi-partition pop: drain up to N sparse partitions per call. Under
    // autopilot a pinned width travels even when it is 1, because 1 is then a
    // decision and not the absence of one.
    if (sizing.partitions !== null) params.append('partitions', sizing.partitions)
    // Conflation (PLAN_CONFLATION §3.1): last-value delivery for this group.
    // Sent ONLY when true, so a consumer that never declares it puts no new
    // bytes on the wire. THIS IS THE SECOND PARAMETER BUILDER — the pop() one
    // lives inline in QueueBuilder.pop and must gain every parameter too.
    if (conflation) params.append('conflation', 'true')
    // NEVER send autoAck for consume - client always manages acking
    // autoAck is only for pop() where server auto-acks immediately

    return params
  }
}

