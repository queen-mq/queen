/**
 * Buffer manager for client-side message buffering across queues.
 *
 * One MessageBuffer per address -- a durable QUEUE (since 2026-11-06; each item
 * carries its partition, so one flush carries every partition) or an ephemeral
 * `queue/partition` -- and exactly ONE drain loop per buffer. The drain is the
 * only thing that sends: it takes `messageCount`-sized batches off the front,
 * POSTs them, and wakes producers parked on the buffer's maxSize bound after
 * each batch that is definitively gone.
 *
 * A batch whose POST fails goes straight back to the front of the buffer, in
 * order, and is retried after `retryDelayMillis` -- indefinitely, until it
 * lands or the buffer is stopped. That is the half of the 2026-08-20 fix that
 * removes loss on flush error; MessageBuffer's docs carry the other half
 * (blocking backpressure) and the measurements behind both.
 *
 * Three more guards (2026-11-06), found with a producer that pushed buffered
 * messages to 100 queues x 1,000 partitions in a `while (true)` loop and ran
 * Node out of memory in ten seconds without sending a request:
 *
 *  - A bound for the WHOLE client (`maxMessages`, default 100,000): a buffer's
 *    own bound (4 x messageCount) bounds that buffer, and 100,000 of them bound
 *    nothing. Past it, a buffered push waits for the drains, whatever the
 *    number of buffers.
 *  - A turn for the event loop every 256 buffered messages: an add that finds
 *    room resolves at once, so a loop of them never let Node run the flush
 *    timers or the sockets, and nothing was ever sent.
 *  - Items the broker REFUSES inside a successful push (a per-item status
 *    other than queued, buffered or duplicate) go back to the front and are
 *    retried; after 5 attempts they are dropped and reported (`onError`, else
 *    the console). Before, the drain read only whether the POST threw, so a
 *    refused item vanished without a word.
 *
 * Deadlines: an explicit flush (`flushBuffer`, `flushAllBuffers`) may pass
 * `deadlineMillis` to bound how long it is willing to keep retrying, because
 * "retry forever" is right for a background flusher and wrong for a shutdown
 * path that has a SIGTERM grace period to respect. When the deadline expires
 * the messages are still in the buffer -- the error says how many -- so the
 * failure is loud rather than silent.
 *
 * WHERE a batch goes is the buffer's DESTINATION (buffer/sinks.js), not
 * something this loop knows: durable pushes and ephemeral pushes are two routes
 * with two body shapes and exactly one set of ordering, backpressure and retry
 * semantics, so the drain is parametrized instead of copied. Addresses are
 * namespaced per family (`eph:` prefix), so the two never share a buffer, a
 * drain, or a retry queue. A buffer created without a destination drains to the
 * durable push exactly as before.
 */

import { MessageBuffer } from './MessageBuffer.js'
import { DURABLE_SINK } from './sinks.js'
import * as logger from '../utils/logger.js'

/** Messages the whole client may hold buffered before a push waits. */
export const DEFAULT_MAX_BUFFERED_MESSAGES = 100000
/** A buffered push gives the event loop a turn after this many. */
const YIELD_EVERY = 256
/** Attempts at an item the broker keeps refusing before it is dropped and reported. */
export const MAX_REFUSED_ATTEMPTS = 5
/** Per-item push statuses that mean the item is stored (or already was). */
const LANDED = new Set(['queued', 'buffered', 'duplicate'])

const yieldToEventLoop = () => new Promise((resolve) => {
  if (typeof setImmediate === 'function') setImmediate(resolve)
  else setTimeout(resolve, 0)
})

/**
 * A durable batch with each partition's items together, in the order each
 * partition first appears, and each partition's own order kept. The broker
 * plans a push per partition, and the binary body has one group per run of a
 * partition: grouping makes the fewest groups. Order ACROSS partitions was
 * never promised.
 */
export function groupByPartition(batch) {
  const groups = new Map()
  for (const item of batch) {
    const key = item.partition ?? 'Default'
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key).push(item)
  }
  if (groups.size <= 1) return batch
  return [].concat(...groups.values())
}

/**
 * The items of a sent batch the broker refused, by position: the answer is
 * one result per item, in order (a binary push's answers are put back in item
 * order by the HTTP client). A missing result counts as landed, as it always
 * did. A counts answer (`{count, queued, duplicate, error}`) says how many but
 * not which: then the whole batch is sent again, and what had landed comes
 * back as a duplicate (each item keeps its transaction id).
 */
export function refusedItems(sent, result) {
  if (Array.isArray(result)) {
    const out = []
    for (let i = 0; i < sent.length; i++) {
      const r = result[i]
      if (r === undefined || r === null || typeof r !== 'object') continue
      if (r.status === undefined || LANDED.has(r.status)) continue
      out.push({ item: sent[i], error: typeof r.error === 'string' && r.error ? r.error : `status ${JSON.stringify(r.status)}` })
    }
    return out
  }
  if (result && typeof result === 'object' && Number.isInteger(result.error) && result.error > 0) {
    return sent.map((item) => ({ item, error: `${result.error} of ${result.count ?? sent.length} refused` }))
  }
  return []
}

function abortError(signal) {
  if (signal.reason instanceof Error) return signal.reason
  const error = new Error('Queen buffered push aborted while waiting for buffer capacity')
  error.name = 'AbortError'
  if (signal.reason !== undefined) error.cause = signal.reason
  return error
}

export class BufferManager {
  #httpClient
  #buffers = new Map() // queueAddress -> MessageBuffer
  #drains = new Map()  // queueAddress -> { promise, ctl } for the in-flight drain
  #flushCount = 0
  #stopped = false
  // The client-wide bound: messages in every buffer, in flight included.
  #maxMessages
  #total = 0
  #roomWaiters = []
  #adds = 0
  // Refused items: how many times each was sent, and what was given up on.
  #attempts = new WeakMap()
  #dropped = 0
  #onError
  #lastSaid = new Map()

  /**
   * @param {object} httpClient
   * @param {object} [options]
   * @param {number} [options.maxMessages=100000] - messages the whole client may
   *   hold buffered (every queue, in flight included) before a push waits
   * @param {(items: object[], error: Error) => void} [options.onError] - items
   *   the broker kept refusing, dropped after MAX_REFUSED_ATTEMPTS; without it
   *   they are reported on the console
   */
  constructor(httpClient, { maxMessages, onError } = {}) {
    this.#httpClient = httpClient
    this.#maxMessages = Number.isInteger(maxMessages) && maxMessages > 0 ? maxMessages : DEFAULT_MAX_BUFFERED_MESSAGES
    this.#onError = typeof onError === 'function' ? onError : null
  }

  /**
   * Buffer one message, waiting for room if its buffer, or the client, is at
   * its bound.
   *
   * Returns a promise: the add path is where backpressure is applied, so
   * callers MUST await it. PushBuilder does; anything that forgets would be
   * back to the unbounded behaviour this replaced.
   *
   * @param {string} queueAddress
   * @param {object} formattedMessage
   * @param {object} bufferOptions
   * @param {{ signal?: AbortSignal, destination?: object }} [opts] - `destination`
   *   is `{ sink, queue, partition }` (buffer/sinks.js) and is read ONLY when
   *   this address's buffer is created: an address belongs to one queue of one
   *   storage class, so its route cannot change under an in-flight retry.
   */
  async addMessage(queueAddress, formattedMessage, bufferOptions, { signal, destination = null } = {}) {
    // A push after cleanup() would otherwise create a fresh buffer that nothing
    // will ever flush -- messages accepted into a client that is already shut
    // down, which is the same false success the bound exists to remove.
    if (this.#stopped) {
      throw new Error(`Queen client is closed: message not buffered for ${queueAddress}`)
    }

    // A turn for the event loop now and then: an add that finds room resolves
    // at once, and a loop of them would never let the flush timers or the
    // sockets run.
    if (++this.#adds % YIELD_EVERY === 0) await yieldToEventLoop()

    // The client-wide bound. Every buffer holding messages is made to drain
    // first: the waiting add must not depend on a timer that is far away.
    while (this.#total >= this.#maxMessages && !this.#stopped) {
      this.#drainEverything()
      await this.#waitForRoom(signal)
      if (signal?.aborted) throw abortError(signal)
    }
    if (this.#stopped) {
      throw new Error(`Queen client is closed: message not buffered for ${queueAddress}`)
    }

    if (!this.#buffers.has(queueAddress)) {
      // The raw options go to the buffer, which fills in the defaults itself:
      // maxSize is derived from the messageCount this caller asked for, and
      // merging defaults here first would hide the difference between "not set"
      // and "set to the default".
      const created = new MessageBuffer(queueAddress, bufferOptions, (addr) => { this.#startDrain(addr) }, destination)
      logger.log('BufferManager.createBuffer', { queueAddress, options: created.options, sink: created.destination.sink.name })
      this.#buffers.set(queueAddress, created)
    }

    const buffer = this.#buffers.get(queueAddress)
    await buffer.add(formattedMessage, { signal })
    this.#total++
    logger.log('BufferManager.addMessage', { queueAddress, messageCount: buffer.messageCount })
  }

  #waitForRoom(signal) {
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject, signal, onAbort: null }
      waiter.settle = (fn, arg) => {
        const i = this.#roomWaiters.indexOf(waiter)
        if (i !== -1) this.#roomWaiters.splice(i, 1)
        if (waiter.onAbort) signal.removeEventListener('abort', waiter.onAbort)
        fn(arg)
      }
      if (signal) {
        waiter.onAbort = () => waiter.settle(reject, abortError(signal))
        signal.addEventListener('abort', waiter.onAbort, { once: true })
      }
      this.#roomWaiters.push(waiter)
    })
  }

  /** Wake every add waiting on the client-wide bound; each checks again. */
  #wakeRoom() {
    for (const waiter of this.#roomWaiters.slice()) waiter.settle(waiter.resolve)
  }

  /** Start (or join) the drain of every buffer that holds a message. */
  #drainEverything() {
    for (const [address, buffer] of this.#buffers) {
      if (buffer.messageCount > 0 && !buffer.isFlushing) {
        buffer.cancelTimer()
        this.#startDrain(address)
      }
    }
  }

  /**
   * Start the drain loop for an address, or join the one already running.
   *
   * Joining rather than starting a second loop is what keeps batches in order:
   * two concurrent senders on the same partition would interleave their POSTs.
   * Returns null when there is nothing to send.
   *
   * @param {string} queueAddress
   * @param {number|null} deadlineMillis - how long a caller is willing to keep
   *   retrying a failing batch; null (the default, and what background flushes
   *   use) means "until it lands or the buffer stops".
   */
  #startDrain(queueAddress, deadlineMillis = null) {
    const deadline = deadlineMillis === null || deadlineMillis === undefined
      ? Number.POSITIVE_INFINITY
      : Date.now() + deadlineMillis

    const running = this.#drains.get(queueAddress)
    if (running) {
      // A caller with a deadline joining a background drain tightens it: the
      // shortest patience wins, otherwise a shutdown could be held open by a
      // retry loop that was started with none.
      running.ctl.deadline = Math.min(running.ctl.deadline, deadline)
      return running.promise
    }

    const buffer = this.#buffers.get(queueAddress)
    if (!buffer || !buffer.beginFlush()) return null

    const ctl = { deadline }
    const promise = this.#drain(queueAddress, buffer, ctl)
    this.#drains.set(queueAddress, { promise, ctl })
    // The count-threshold and timer triggers do not await this promise, so give
    // it a handler of its own: a deadline tightened by a concurrent explicit
    // flush would otherwise surface as an unhandled rejection.
    promise.catch(() => {})
    return promise
  }

  async #drain(queueAddress, buffer, ctl) {
    const { messageCount, retryDelayMillis } = buffer.options
    const { sink, queue, partition } = buffer.destination
    const durable = sink === DURABLE_SINK

    try {
      while (buffer.messageCount > 0 && !buffer.isStopped) {
        const batch = buffer.takeBatch(messageCount)
        if (batch.length === 0) break
        // A durable buffer holds every partition of its queue: send each
        // partition's items together (their own order kept).
        const sent = durable ? groupByPartition(batch) : batch

        let result
        try {
          result = await this.#httpClient.post(sink.path, sink.format(queue, partition, sent))
          logger.debug('BufferManager.drain', { queueAddress, sink: sink.name, sent: sent.length, serverResponse: result ? `${result.length || 'N/A'} items` : 'null' })
          this.#flushCount++
        } catch (error) {
          // NOT dropped. The batch goes back at the front, in order, and this
          // loop retries it. Before 2026-08-20 this branch logged and moved on,
          // losing up to messageCount messages per failed POST.
          buffer.restoreBatch(batch)
          logger.error('BufferManager.drain', { queueAddress, error: error.message, requeued: batch.length })

          const remaining = ctl.deadline - Date.now()
          if (remaining <= 0) {
            error.queenUnflushedCount = buffer.messageCount
            error.message = `${error.message} (${buffer.messageCount} message(s) still buffered for ${queueAddress}, not sent)`
            throw error
          }

          await buffer.sleepUnlessStopped(Math.min(retryDelayMillis, remaining))
          continue
        }

        // The POST answered. What the broker refused inside it goes back to
        // the front (in order) for another attempt, until it has had its share.
        const refused = durable ? refusedItems(sent, result) : []
        const retry = []
        const dropped = []
        for (const { item, error } of refused) {
          const tries = (this.#attempts.get(item) || 1) + 1
          if (tries > MAX_REFUSED_ATTEMPTS) {
            dropped.push({ item, error })
          } else {
            this.#attempts.set(item, tries)
            retry.push(item)
          }
        }
        if (retry.length) buffer.restoreBatch(retry)
        this.#total -= sent.length - retry.length
        if (dropped.length) this.#reportDropped(queueAddress, dropped)

        // Capacity freed. Woken here rather than at takeBatch, because a
        // batch that fails to send goes straight back: waking on extraction
        // would let producers refill against room that never freed.
        buffer.wakeWaiters()
        this.#wakeRoom()

        if (retry.length) {
          logger.error('BufferManager.drain', { queueAddress, refused: retry.length, error: refused[0].error })
          const remaining = ctl.deadline - Date.now()
          if (remaining <= 0) break
          await buffer.sleepUnlessStopped(Math.min(retryDelayMillis, remaining))
        }
      }
    } finally {
      buffer.endFlush()
      this.#drains.delete(queueAddress)
      // Drop the entry only when nothing can still be pointed at it: a parked
      // add holds this exact object, and deleting it here would leave that add
      // appending into an orphan no drain would ever visit.
      if (buffer.messageCount === 0 && !buffer.hasParkedAdds && !buffer.isStopped) {
        this.#buffers.delete(queueAddress)
      }
      buffer.wakeWaiters()
      this.#wakeRoom()
    }
  }

  #reportDropped(queueAddress, dropped) {
    this.#dropped += dropped.length
    const error = new Error(
      `${dropped.length} buffered message(s) for ${queueAddress} refused by the broker ${MAX_REFUSED_ATTEMPTS} times and dropped: ${dropped[0].error}`
    )
    error.items = dropped.map((d) => d.item)
    logger.error('BufferManager.drain', { queueAddress, dropped: dropped.length, error: dropped[0].error })
    if (this.#onError) {
      try { this.#onError(error.items, error) } catch (_) { /* the hook's own */ }
      return
    }
    // No hook: say it, once per message every 30 s -- these messages are lost.
    const now = Date.now()
    const last = this.#lastSaid.get(error.message)
    if (last === undefined || now - last >= 30000) {
      this.#lastSaid.set(error.message, now)
      console.error(`[queen buffer] ${error.message}`)
    }
  }

  /**
   * Send everything buffered for one address.
   *
   * @param {string} queueAddress
   * @param {{ deadlineMillis?: number }} [opts] - stop retrying a failing batch
   *   after this long and throw (the messages stay in the buffer). Omit to
   *   retry until the batch lands.
   */
  async flushBuffer(queueAddress, { deadlineMillis = null } = {}) {
    logger.log('BufferManager.flushBuffer', { queueAddress, activeBuffers: this.#buffers.size, pendingFlushes: this.#drains.size })

    const buffer = this.#buffers.get(queueAddress)
    if (!buffer) {
      logger.debug('BufferManager.flushBuffer', { queueAddress, status: 'not-found' })
      await this.#waitForDrains()
      return
    }

    // Cancel the timer to prevent a time-based flush racing this one.
    buffer.cancelTimer()

    const drain = this.#startDrain(queueAddress, deadlineMillis)
    if (drain) await drain

    logger.debug('BufferManager.flushBuffer', { queueAddress, status: 'completed' })
  }

  /**
   * Send everything buffered, for every address.
   *
   * Drains run concurrently across addresses (they are independent buffers, and
   * a shutdown should not pay for them one at a time) and every one is awaited
   * before the first error is rethrown: an unreachable queue must not strand
   * the others' messages.
   */
  async flushAllBuffers({ deadlineMillis = null } = {}) {
    const queueAddresses = new Set([...this.#buffers.keys(), ...this.#drains.keys()])
    logger.log('BufferManager.flushAllBuffers', { bufferCount: queueAddresses.size, pendingFlushes: this.#drains.size })

    const drains = []
    for (const queueAddress of queueAddresses) {
      const buffer = this.#buffers.get(queueAddress)
      if (buffer) buffer.cancelTimer()
      const drain = this.#startDrain(queueAddress, deadlineMillis)
      if (drain) drains.push(drain)
    }

    const outcomes = await Promise.allSettled(drains)
    const failure = outcomes.find(outcome => outcome.status === 'rejected')

    logger.log('BufferManager.flushAllBuffers', { status: failure ? 'failed' : 'completed' })
    if (failure) throw failure.reason
  }

  async #waitForDrains() {
    if (this.#drains.size === 0) return
    logger.debug('BufferManager.waitForDrains', { count: this.#drains.size })
    await Promise.allSettled([...this.#drains.values()].map(entry => entry.promise))
    logger.debug('BufferManager.waitForDrains', { status: 'completed' })
  }

  getStats() {
    let totalBufferedMessages = 0
    let oldestBufferAge = 0

    for (const buffer of this.#buffers.values()) {
      totalBufferedMessages += buffer.messageCount
      const age = buffer.firstMessageAge
      oldestBufferAge = Math.max(oldestBufferAge, age)
    }

    const stats = {
      activeBuffers: this.#buffers.size,
      totalBufferedMessages,
      oldestBufferAge,
      flushesPerformed: this.#flushCount,
      maxBufferedMessages: this.#maxMessages,
      droppedMessages: this.#dropped
    }

    logger.log('BufferManager.getStats', stats)
    return stats
  }

  /**
   * Stop every buffer and discard what is left.
   *
   * Stopping wakes parked adds (they reject: their message was never buffered)
   * and ends any retry loop, so this also unhangs a drain that was waiting out
   * a broker outage. Anything still buffered here is lost -- which is why
   * Queen.close() flushes with a deadline first and logs what remains.
   */
  cleanup() {
    this.#stopped = true
    const unflushed = this.getStats().totalBufferedMessages
    if (unflushed > 0) {
      logger.error('BufferManager.cleanup', { unflushedMessages: unflushed, status: 'discarded' })
    }
    logger.log('BufferManager.cleanup', { bufferCount: this.#buffers.size })
    for (const buffer of this.#buffers.values()) {
      buffer.cleanup()
    }
    this.#buffers.clear()
    this.#total = 0
    for (const waiter of this.#roomWaiters.slice()) {
      waiter.settle(waiter.reject, new Error('Queen client is closed: message not buffered'))
    }
  }
}
