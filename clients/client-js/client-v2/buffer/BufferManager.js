/**
 * Buffer manager for client-side message buffering across queues.
 *
 * One MessageBuffer per `queue/partition` address (the granularity the broker
 * fuses writes on), and exactly ONE drain loop per buffer. The drain is the
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
 * Deadlines: an explicit flush (`flushBuffer`, `flushAllBuffers`) may pass
 * `deadlineMillis` to bound how long it is willing to keep retrying, because
 * "retry forever" is right for a background flusher and wrong for a shutdown
 * path that has a SIGTERM grace period to respect. When the deadline expires
 * the messages are still in the buffer -- the error says how many -- so the
 * failure is loud rather than silent.
 */

import { MessageBuffer } from './MessageBuffer.js'
import * as logger from '../utils/logger.js'

export class BufferManager {
  #httpClient
  #buffers = new Map() // queueAddress -> MessageBuffer
  #drains = new Map()  // queueAddress -> { promise, ctl } for the in-flight drain
  #flushCount = 0
  #stopped = false

  constructor(httpClient) {
    this.#httpClient = httpClient
  }

  /**
   * Buffer one message, waiting for room if the buffer is at its bound.
   *
   * Returns a promise: the add path is where backpressure is applied, so
   * callers MUST await it. PushBuilder does; anything that forgets would be
   * back to the unbounded behaviour this replaced.
   *
   * @param {string} queueAddress
   * @param {object} formattedMessage
   * @param {object} bufferOptions
   * @param {{ signal?: AbortSignal }} [opts]
   */
  async addMessage(queueAddress, formattedMessage, bufferOptions, { signal } = {}) {
    // A push after cleanup() would otherwise create a fresh buffer that nothing
    // will ever flush -- messages accepted into a client that is already shut
    // down, which is the same false success the bound exists to remove.
    if (this.#stopped) {
      throw new Error(`Queen client is closed: message not buffered for ${queueAddress}`)
    }

    if (!this.#buffers.has(queueAddress)) {
      // The raw options go to the buffer, which fills in the defaults itself:
      // maxSize is derived from the messageCount this caller asked for, and
      // merging defaults here first would hide the difference between "not set"
      // and "set to the default".
      const created = new MessageBuffer(queueAddress, bufferOptions, (addr) => { this.#startDrain(addr) })
      logger.log('BufferManager.createBuffer', { queueAddress, options: created.options })
      this.#buffers.set(queueAddress, created)
    }

    const buffer = this.#buffers.get(queueAddress)
    await buffer.add(formattedMessage, { signal })
    logger.log('BufferManager.addMessage', { queueAddress, messageCount: buffer.messageCount })
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

    try {
      while (buffer.messageCount > 0 && !buffer.isStopped) {
        const batch = buffer.takeBatch(messageCount)
        if (batch.length === 0) break

        try {
          const result = await this.#httpClient.post('/api/v1/push', { items: batch })
          logger.debug('BufferManager.drain', { queueAddress, sent: batch.length, serverResponse: result ? `${result.length || 'N/A'} items` : 'null' })

          this.#flushCount++
          // Capacity freed. Woken here rather than at takeBatch, because a
          // batch that fails to send goes straight back: waking on extraction
          // would let producers refill against room that never freed.
          buffer.wakeWaiters()
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
      flushesPerformed: this.#flushCount
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
  }
}
