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
import { consumerGroupOf } from '../utils/consumerGroup.js'

export class TransactionBuilder {
  #httpClient
  #operations = []
  #requiredLeases = []
  // Unresolved on purpose: `until` is an instant, and freezing it when the op
  // was queued would ship a TTL already stale by however long the bundle took
  // to assemble. Materialized in commit(), which is send time.
  #kvEntries = []
  // Lock handles whose permit this bundle commits under (`.guard(lock)`).
  // Kept as handles, not as ops: the token is read at SEND time, because a
  // lock's own renewal changes it.
  #guards = []
  #timerOps = []
  #kvApi = null

  constructor(httpClient) {
    this.#httpClient = httpClient
  }

  /**
   * Ack popped messages as part of this transaction.
   *
   * The consumer group defaults to the one each message was popped under
   * (`message.consumerGroup`, which every pop answers with), because the lease
   * an ack has to match belongs to that group: an ack that names no group is
   * judged in queue mode, and a message popped by a group then fails with
   * `rejected_ack`. Pass `{ consumerGroup }` (or `{ group }`, the key
   * `queen.ack()` takes) to override it.
   */
  ack(messages, status = 'completed', context = {}) {
    const msgs = Array.isArray(messages) ? messages : [messages]
    const explicitGroup = context.consumerGroup || context.group || null

    logger.log('TransactionBuilder.ack', { count: msgs.length, status, consumerGroup: explicitGroup })

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

      const consumerGroup = explicitGroup || consumerGroupOf(msg)
      if (consumerGroup) {
        operation.consumerGroup = consumerGroup
      }

      // Each ack names its own lease. A Queen 2 broker fences an ack with the
      // lease the operation carries, and lends it one from requiredLeases only
      // when the bundle names a single lease: in a bundle spanning two leases,
      // an ack without its own would be applied whoever holds its partition
      // now. 1.x brokers read the operation's lease before requiredLeases too.
      if (leaseId) {
        operation.leaseId = leaseId
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
      // A precondition on a key the bundle does not write. With
      // `required: true` it is the bundle's gate; without, only a look.
      check: (ns, key, opts = {}) => add(kvOp.check(ns, key, opts)),
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
   * what makes "the email is sent exactly once" a property of the broker
   * rather than a hope about redelivery.
   *
   * The verdict is RETURNED by `commit()`, not thrown -- see there.
   */
  once(ns, key, opts = {}) {
    const value = opts.value !== undefined ? opts.value : true
    const required = opts.required === false ? {} : { required: true }
    return this.kv.putIfAbsent(ns, key, value, { ...opts, ...required })
  }

  /**
   * Commit this bundle only while a lock is held.
   *
   *     const lock = queen.lock('daily-report', { ttl: '30s' })
   *     if (!(await lock.acquire())) return
   *     const res = await queen.transaction()
   *       .guard(lock)
   *       .queue('reports').push([{ data: report }])
   *       .commit()
   *     if (res.success === false) return    // the lock is somebody else's now
   *
   * The guard is a `check` of the permit's row at the lock's token, with
   * `required: true`: the broker judges it in the same log entry as the acks,
   * pushes, keys and timers beside it. A holder that was paused past its
   * lifetime and replaced commits NOTHING — which the lock by itself cannot
   * promise, since nobody stops an expired holder from running.
   *
   * The token is read when `commit()` sends, and a lock's own background
   * renewal moves it. A guard that lost to this handle's own renewal is sent
   * again with the new token; one that lost to another holder is the verdict,
   * returned like `once`'s (`success: false, reason: 'kv_precondition'`), and
   * the handle then reports the lock lost.
   *
   * Throws at `commit()`, with `.code === 'LOCK_NOT_HELD'`, when the handle
   * holds nothing: a step that asked for a guard must not go out without one.
   */
  guard(lock) {
    if (!lock || typeof lock.guard !== 'function' || typeof lock._settled !== 'function') {
      throw new Error('transaction: guard() takes a lock from queen.lock() or queen.semaphore()')
    }
    this.#guards.push(lock)
    return this
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

  /**
   * The request body, built at send time. The guards go first in `kv`, so
   * guard `i` is op `i` of the rider.
   */
  #body() {
    // Byte-identity when the riders are absent (§6.3): the keys are added only
    // when there is something in them, so a bundle that uses neither feature
    // produces exactly the body it produced before this feature existed.
    const body = {
      operations: this.#operations,
      requiredLeases: [...new Set(this.#requiredLeases)] // Unique leases
    }
    if (this.#guards.length + this.#kvEntries.length > 0) {
      const now = Date.now()
      body.kv = [
        ...this.#guards.map(lock => lock.guard()),
        ...this.#kvEntries.map(e => materializeKvOp(e, now))
      ]
    }
    if (this.#timerOps.length > 0) {
      body.timers = this.#timerOps
    }
    return body
  }

  /**
   * The lock whose guard is the precondition a rolled-back bundle names, if
   * it is one of this bundle's. `failedIndex` is in the FLAT space of
   * `results[]`: every pushed item and every ack first, then the `kv` rider.
   */
  #failedGuard(result) {
    if (this.#guards.length === 0 || !Number.isInteger(result.failedIndex)) return null
    const flatOperations = this.#operations.reduce(
      (n, op) => n + (op.type === 'push' ? op.items.length : 1), 0
    )
    return this.#guards[result.failedIndex - flatOperations] ?? null
  }

  async commit() {
    const riderCount = this.#guards.length + this.#kvEntries.length + this.#timerOps.length
    if (this.#operations.length === 0 && riderCount === 0) {
      logger.error('TransactionBuilder.commit', 'No operations to commit')
      throw new Error('Transaction has no operations to commit')
    }

    logger.log('TransactionBuilder.commit', {
      operationCount: this.#operations.length,
      requiredLeases: this.#requiredLeases.length,
      kv: this.#kvEntries.length,
      guards: this.#guards.length,
      timers: this.#timerOps.length
    })

    try {
      // A bundle is sent again in ONE case: its guard lost to the lock's own
      // background renewal, which moved the token between the moment the body
      // was built and the moment the broker judged it. The row still names
      // this owner, so the lock is held; nothing committed (a lost required
      // precondition rolls the whole bundle back), so sending it again with
      // the new token is the same step, not a second one.
      let result
      for (let attempt = 0; ; attempt++) {
        await Promise.all(this.#guards.map(lock => lock._settled()))
        result = await this.#httpClient.post('/api/v1/transaction', this.#body())
        if (result.success || result.reason !== 'kv_precondition') break
        const lock = this.#failedGuard(result)
        if (!lock) break
        const ownRenewal = result.kvReason === 'version' && result.value?.owner === lock.owner
        if (!ownRenewal) {
          // Expired, released, or another holder's: the lock is gone.
          lock._lost('guard')
          break
        }
        if (attempt >= 3) break
        await lock._settled()
        if (!lock.held) break
        logger.log('TransactionBuilder.commit', { status: 'guard_renewed', lock: lock.name, attempt })
      }

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
        // The broker's reason code travels ON the error, so no caller ever has
        // to match the message. The ones worth branching on from a 2.x broker:
        // bad_request, duplicate (a pushed transactionId is already stored),
        // rejected_ack (an acked message is no longer leased by this worker, or
        // was popped under a different consumer group than the ack names), and
        // too_large. A 1.x broker spells the ack one ack_rejected, and also
        // sends timer_horizon_exceeded, payload_too_large, misaligned and
        // db_error.
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

