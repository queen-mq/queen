/**
 * Transaction builder for atomic operations
 *
 * KV AND TIMER RIDERS (PLAN_KV_TIMERS.md §6.3, §8.2, §10.4)
 *
 * `kv` and `timers` are TOP-LEVEL fields of the request body, beside
 * `operations` and NEVER elements of it. That is a wire decision, not a style
 * one, and the reason is a silent failure in another language that this shape
 * has to respect because one broker parses one wire for all seven clients: two
 * Go struct fields carrying the same JSON key at the same level are BOTH
 * DROPPED by encoding/json, with no error and no warning. A bundle would go
 * out with zero kv operations, the broker would commit a transaction with no
 * gate, and the `putIfAbsent` the bundle existed for would never have
 * happened. An op that still arrives as `{"type":"kv"}` inside `operations`
 * gets a named 400 from the broker, which is the best failure available.
 *
 * A bundle that carries neither array produces exactly the body it produces
 * today -- no `kv: []`, no `timers: null` -- so nothing changes for anyone who
 * does not use the feature, including brokers that predate it.
 *
 * WHY THE RIDERS ARE WORTH THE PARAGRAPH: the transaction is the PRIMARY
 * fence, and `expect` only the secondary assertion. A KV write that shares the
 * transaction with the ack is undone when an expired lease makes the ack fail
 * -- something a CAS cannot do, because an `expect` on a still-matching
 * version succeeds even from a zombie worker.
 */

import * as logger from '../utils/logger.js'
import { generateUUID } from './QueueBuilder.js'
import { isValidUUID } from '../utils/validation.js'
import { kvOp, materializeKvOp } from '../kv/Kv.js'
import { TimerBuilder } from './TimerBuilder.js'

export class TransactionBuilder {
  #httpClient
  #operations = []
  #requiredLeases = []
  // Unresolved on purpose: `until` is an instant, and freezing it when the op
  // was queued would ship a TTL already stale by however long the bundle took
  // to assemble. Materialized in commit(), which is send time.
  #kvEntries = []
  #timerOps = []
  #kvApi = null

  constructor(httpClient) {
    this.#httpClient = httpClient
  }

  ack(messages, status = 'completed', context = {}) {
    const msgs = Array.isArray(messages) ? messages : [messages]
    
    logger.log('TransactionBuilder.ack', { count: msgs.length, status, consumerGroup: context.consumerGroup })

    msgs.forEach(msg => {
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

      const operation = {
        type: 'ack',
        transactionId,
        partitionId,
        status
      }

      // Add consumerGroup if provided in context
      if (context.consumerGroup) {
        operation.consumerGroup = context.consumerGroup
      }

      this.#operations.push(operation)

      if (leaseId) {
        this.#requiredLeases.push(leaseId)
      }
    })

    return this
  }

  queue(queueName) {
    // Return a sub-builder for push operations with partition support
    let partition = null
    
    const subBuilder = {
      partition: (partitionKey) => {
        partition = partitionKey
        return subBuilder
      },
      
      push: (items) => {
        const itemArray = Array.isArray(items) ? items : [items]
        
        logger.log('TransactionBuilder.queue.push', { queue: queueName, partition, count: itemArray.length })

        this.#operations.push({
          type: 'push',
          items: itemArray.map(item => {
            // Check if property exists, not just truthy (to support null values)
            let payloadValue
            if ('data' in item) {
              payloadValue = item.data
            } else if ('payload' in item) {
              payloadValue = item.payload
            } else {
              payloadValue = item
            }

            // Same contract as QueueBuilder.push: the caller's transactionId is
            // what makes a retried transaction idempotent inside the dedup
            // window, so it has to reach the wire. Absent, mint one here rather
            // than leaving the broker to do it, so the id is knowable client
            // side either way.
            const result = {
              queue: queueName,
              payload: payloadValue,
              transactionId: item.transactionId || generateUUID()
            }

            // Add partition if set
            if (partition !== null) {
              result.partition = partition
            }

            if (item.traceId && isValidUUID(item.traceId)) {
              result.traceId = item.traceId
            }

            return result
          })
        })

        return this
      }
    }
    
    return subBuilder
  }

  // ===========================
  // KV rider (§5, §8.2)
  // ===========================

  /**
   * KV operations that commit with this transaction.
   *
   *     await queen.transaction()
   *       .ack(msg)
   *       .kv.put('saga', sagaId, { step: 'charged' }, { ttl: '24h' })
   *       .commit()
   *
   * Every method returns the TRANSACTION, so the chain keeps reading like one.
   * Same op shapes as `queen.kv`, built by the same code, so the two surfaces
   * cannot drift.
   *
   * `getPrefix` is deliberately absent from this wire and throws here rather
   * than at the broker: it is unbounded read work inside the transaction that
   * holds the outermost lock space and, downstream, the partition ones. `get`
   * and `getMany` are allowed because the caller fixes their cost -- the
   * boundary is COST, not the kind of operation.
   */
  get kv() {
    if (this.#kvApi) return this.#kvApi
    const add = (entry) => {
      this.#kvEntries.push(entry)
      return this
    }
    this.#kvApi = {
      get: (ns, key) => add(kvOp.get(ns, key)),
      getMany: (ns, keys) => add(kvOp.getMany(ns, keys)),
      put: (ns, key, value, opts = {}) => add(kvOp.put(ns, key, value, opts)),
      putIfAbsent: (ns, key, value, opts = {}) => add(kvOp.putIfAbsent(ns, key, value, opts)),
      delete: (ns, key, opts = {}) => add(kvOp.delete(ns, key, opts)),
      incr: (ns, key, delta = 1, opts = {}) => add(kvOp.incr(ns, key, delta, opts)),
      getPrefix: () => {
        throw new Error(
          'kv: getPrefix is not available inside a transaction — unbounded read work under the outermost ' +
          'lock space. Use POST /api/v1/kv (queen.kv.getPrefix / queen.kv.listAll) outside the bundle.'
        )
      }
    }
    return this.#kvApi
  }

  /**
   * The gate, which is the reason this feature exists: do the bundle at most
   * once.
   *
   *     const res = await queen.transaction()
   *       .ack(msg)
   *       .queue('emails').push([{ data: mail }])
   *       .once('test-idem', orderId, { ttl: '24h' })
   *       .commit()
   *     if (res.success === false) return    // a redelivery: already done
   *
   * `putIfAbsent` with `required:true`, so a marker that already exists ABORTS
   * the transaction: the push and the ack roll back together with it. That is
   * what makes "the email is sent exactly once" a property of the database
   * rather than a hope about redelivery.
   *
   * The verdict is RETURNED by `commit()`, not thrown -- see there.
   */
  once(ns, key, opts = {}) {
    const value = opts.value !== undefined ? opts.value : true
    const required = opts.required === false ? {} : { required: true }
    return this.kv.putIfAbsent(ns, key, value, { ...opts, ...required })
  }

  // ===========================
  // Timer rider (§4, §9.6)
  // ===========================

  /**
   * Schedule or cancel a timer as part of this transaction.
   *
   *     await queen.transaction()
   *       .ack(msg)
   *       .timer('reminders').key(orderId).delay('24h').payload({ orderId }).schedule()
   *       .commit()
   *
   * `.schedule()` and `.cancel()` are the terminals; they add the op and hand
   * the transaction back. `peek` and `list` are reads and are not transaction
   * operations.
   *
   * ONE ASYMMETRY TO KNOW ABOUT (§9.6): a cancel sent this way rides the
   * bundle and shares its fate, including its authorization. The cancel that
   * is guaranteed never to be blocked is the standalone
   * `queen.timer(q).key(k).cancel()`, on its own DELETE route. Cancel inside a
   * bundle when you need atomicity with an ack; cancel outside it when you
   * need the cancel to land no matter what.
   */
  timer(queueName) {
    return new TimerBuilder(null, queueName, (op) => {
      this.#timerOps.push(op)
      return this
    })
  }

  async commit() {
    const riderCount = this.#kvEntries.length + this.#timerOps.length
    if (this.#operations.length === 0 && riderCount === 0) {
      logger.error('TransactionBuilder.commit', 'No operations to commit')
      throw new Error('Transaction has no operations to commit')
    }

    logger.log('TransactionBuilder.commit', {
      operationCount: this.#operations.length,
      requiredLeases: this.#requiredLeases.length,
      kv: this.#kvEntries.length,
      timers: this.#timerOps.length
    })

    // Byte-identity when the riders are absent (§6.3): the keys are added only
    // when there is something in them, so a bundle that uses neither feature
    // produces exactly the body it produced before this feature existed.
    const body = {
      operations: this.#operations,
      requiredLeases: [...new Set(this.#requiredLeases)] // Unique leases
    }
    if (this.#kvEntries.length > 0) {
      const now = Date.now()
      body.kv = this.#kvEntries.map(e => materializeKvOp(e, now))
    }
    if (this.#timerOps.length > 0) {
      body.timers = this.#timerOps
    }

    try {
      const result = await this.#httpClient.post('/api/v1/transaction', body)

      if (!result.success) {
        // THE ONE OUTCOME THAT RETURNS INSTEAD OF THROWING (§8.3).
        //
        // A lost `required` KV precondition is not a failure: it is the
        // EXPECTED outcome of every legitimate redelivery -- the idempotency
        // marker doing its job -- and it arrives as HTTP 200 with
        // `success:false, reason:'kv_precondition'` precisely so that it stays
        // out of retry policies and error metrics. Throwing it would put the
        // single most frequent outcome of this product inside every caller's
        // catch block, where the natural reflex is to retry, which is the one
        // thing that must not happen.
        //
        // The body carries `failedIndex` (in the FLAT index space of
        // `results[]`), `kvReason`, `version` and `value`, so the caller can
        // see who won without a second round trip.
        if (result.reason === 'kv_precondition') {
          logger.log('TransactionBuilder.commit', {
            status: 'kv_precondition',
            failedIndex: result.failedIndex,
            kvReason: result.kvReason
          })
          return result
        }

        logger.error('TransactionBuilder.commit', { error: result.error, reason: result.reason })
        const error = new Error(result.error || 'Transaction failed')
        // The closed-taxonomy code travels ON the error, so no caller ever has
        // to match the message: bad_request | duplicate | ack_rejected |
        // timer_horizon_exceeded | payload_too_large | misaligned | db_error.
        if (result.reason) error.reason = result.reason
        error.result = result
        throw error
      }

      logger.log('TransactionBuilder.commit', { status: 'success' })
      return result
    } catch (error) {
      logger.error('TransactionBuilder.commit', { error: error.message })
      throw error
    }
  }
}

