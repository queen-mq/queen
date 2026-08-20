/**
 * Message buffer for a single queue/partition.
 *
 * This is the client-side linger: single pushes accumulate here and leave as
 * one request once `messageCount` messages are waiting, or `timeMillis` has
 * passed since the first one arrived. Two properties beyond that batching are
 * load-bearing, and neither existed before 2026-08-20:
 *
 *  - `maxSize` is a BLOCKING bound, not a hint. `add()` returns a promise that
 *    does not resolve while the buffer is full, so a producer that outruns the
 *    flush pipeline is paced down to the drain rate instead of growing the
 *    heap. Measured on the Go client, whose buffer had exactly this shape:
 *    filling at 1.46M msg/s against a 1.0M msg/s flush pipeline accumulated
 *    20.9M messages (11.7 GB of RSS) in 45 seconds and lost every one of them
 *    at process exit, with ZERO client-side errors reported anywhere. The
 *    bounded version sustained 881,148 msg/s with exact send/receive parity
 *    (39,655,787 = 39,655,787) and 71 MB of RSS.
 *
 *  - A batch that fails to send is put BACK at the front of the buffer, in
 *    order, and retried after `retryDelayMillis`. It is never dropped. Before
 *    this, the flusher took the batch out before the POST and only logged the
 *    failure, so up to `messageCount` messages vanished per failed request.
 *
 * Together those two turn a broker outage into blocked producers with bounded
 * memory, instead of silent loss.
 *
 * BLOCKING IDIOM: JavaScript is single-threaded, so "block" cannot mean
 * "occupy the thread" -- that would starve the very flush that frees the
 * capacity being waited for. It means an awaitable gate: parked adds hold a
 * promise that the flusher resolves after each drained batch (and `stop()`
 * rejects). No spin loop, no setInterval poll: the event loop is free to run
 * the flush while producers wait.
 *
 * Buffered messages still live only in this process's memory. A crash, or a
 * `process.exit()` that skips `close()`, loses them -- buffering belongs on
 * telemetry-shaped traffic, not on anything that must not be lost.
 */

import { BUFFER_DEFAULTS } from '../utils/defaults.js'

export class MessageBuffer {
  #queueAddress
  #messages = []
  #options
  #flushCallback
  #timer = null
  #firstMessageTime = null
  #flushing = false
  #stopped = false
  // Adds parked on the maxSize bound. Each entry can be woken (capacity freed)
  // or failed (stop, or the caller's AbortSignal).
  #waiters = []
  // Counts adds that have been woken but have not resumed yet. JS resumes an
  // awaiting function on a later microtask, so the waiter list is already empty
  // while those adds are still in flight; without this counter BufferManager
  // could drop the buffer entry out of its map in that window and the resumed
  // add would append to an orphan nobody ever flushes.
  #parked = 0
  #stopWaiters = []

  constructor(queueAddress, options, flushCallback) {
    this.#queueAddress = queueAddress
    this.#options = MessageBuffer.normalizeOptions(options)
    this.#flushCallback = flushCallback
  }

  /**
   * Fill in defaults and enforce the bound's invariants.
   *
   * `maxSize: 0` (or absent) means the DEFAULT bound, never "unbounded":
   * unbounded is the defect this knob exists to close, so opting out of
   * backpressure is deliberately not expressible. The floor keeps the bound
   * sane when a caller sets a `messageCount` larger than their `maxSize` --
   * a buffer that must block before it can even assemble one batch would
   * deadlock against its own flush threshold.
   */
  static normalizeOptions(options) {
    // The caller's raw options, NOT BUFFER_DEFAULTS spread over them: the bound
    // is derived from whatever messageCount this buffer ended up with, so
    // `buffer({ messageCount: 10 })` gets a bound of 40, not the 400 that suits
    // the default batch of 100. Unknown keys are carried through untouched.
    const provided = options || {}

    const messageCount = provided.messageCount > 0 ? provided.messageCount : BUFFER_DEFAULTS.messageCount
    const timeMillis = provided.timeMillis > 0 ? provided.timeMillis : BUFFER_DEFAULTS.timeMillis

    let maxSize = provided.maxSize > 0 ? provided.maxSize : 4 * messageCount
    if (maxSize < messageCount) maxSize = messageCount

    const retryDelayMillis = provided.retryDelayMillis > 0
      ? provided.retryDelayMillis
      : BUFFER_DEFAULTS.retryDelayMillis

    return { ...provided, messageCount, timeMillis, maxSize, retryDelayMillis }
  }

  /**
   * Append one message, waiting for room if the buffer is at its bound.
   *
   * Resolves once the message is in the buffer. Rejects if the buffer is
   * stopped while parked, or if `signal` aborts -- an add that could not be
   * buffered must never look like a successful push.
   *
   * @param {object} formattedMessage
   * @param {{ signal?: AbortSignal }} [opts]
   */
  async add(formattedMessage, { signal } = {}) {
    if (this.#stopped) {
      throw new Error(`Queen buffer ${this.#queueAddress} is stopped: message not buffered`)
    }
    if (signal?.aborted) throw abortReason(signal)

    // BACKPRESSURE. Re-checked in a loop, not once: a broadcast wakes every
    // parked add, and the first ones to resume can fill the room that was
    // freed, so the rest have to park again.
    while (this.#messages.length >= this.#options.maxSize && !this.#stopped) {
      // Being at the bound means producers outran the flusher. Make sure one is
      // actually running before parking -- the time-based flush may be a full
      // `timeMillis` away, and nothing else will start it.
      this.#triggerFlush()
      // The counter is held across the resumption itself, not just the wait:
      // see #parked for why the window between "woken" and "resumed" matters.
      this.#parked++
      try {
        await this.#waitForCapacity(signal)
      } finally {
        this.#parked--
      }
      if (signal?.aborted) throw abortReason(signal)
    }

    if (this.#stopped) {
      throw new Error(`Queen buffer ${this.#queueAddress} stopped while waiting for capacity: message not buffered`)
    }

    if (this.#messages.length === 0) {
      this.#firstMessageTime = Date.now()
      this.#startTimer()
    }

    this.#messages.push(formattedMessage)

    if (this.#messages.length >= this.#options.messageCount) {
      this.#triggerFlush()
    }
  }

  #waitForCapacity(signal) {
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject, signal, onAbort: null }

      waiter.settle = (fn, arg) => {
        const index = this.#waiters.indexOf(waiter)
        if (index !== -1) this.#waiters.splice(index, 1)
        if (waiter.onAbort) signal.removeEventListener('abort', waiter.onAbort)
        fn(arg)
      }

      if (signal) {
        waiter.onAbort = () => waiter.settle(reject, abortReason(signal))
        signal.addEventListener('abort', waiter.onAbort, { once: true })
      }

      this.#waiters.push(waiter)
    })
  }

  /**
   * Wake every parked add. Called by the flusher after a batch is definitively
   * gone (the POST succeeded), not when the batch is merely taken out of the
   * buffer: a batch that fails goes straight back, and waking on extraction
   * would let producers refill against room that never actually freed.
   */
  wakeWaiters() {
    const waiters = this.#waiters.slice()
    for (const waiter of waiters) waiter.settle(waiter.resolve)
  }

  #startTimer() {
    if (this.#timer) return // Timer already running

    // Deliberately NOT unref()'d: a short script that pushes and returns must
    // stay alive long enough for the time-based flush to fire.
    this.#timer = setTimeout(() => {
      this.#timer = null
      this.#triggerFlush()
    }, this.#options.timeMillis)
  }

  #triggerFlush() {
    if (this.#flushing || this.#stopped || this.#messages.length === 0) return
    this.#flushCallback(this.#queueAddress)
  }

  /**
   * Claim the right to flush this buffer. Returns false when a flush is already
   * running: one drain loop per buffer, so a burst of adds past the threshold
   * cannot start a second sender that would interleave batches out of order.
   */
  beginFlush() {
    if (this.#flushing || this.#stopped || this.#messages.length === 0) return false
    this.#flushing = true
    this.cancelTimer()
    return true
  }

  /**
   * Release the flush claim. Anything still buffered (a batch put back by a
   * failed send, or messages added while the drain was stopping) gets a fresh
   * timer, so it cannot sit there unnoticed until the next add.
   */
  endFlush() {
    this.#flushing = false
    if (this.#messages.length > 0 && !this.#stopped) this.#startTimer()
  }

  /**
   * Take up to `batchSize` messages off the front.
   *
   * `splice` returns a fresh array, so the batch does not alias the buffer's
   * storage and putting it back cannot corrupt what is left behind. (The Go
   * reference has to copy explicitly there -- its slices do alias.)
   */
  takeBatch(batchSize) {
    const messages = this.#messages.splice(0, batchSize)
    if (this.#messages.length === 0) this.#firstMessageTime = null
    return messages
  }

  /**
   * Put a failed batch back at the FRONT, preserving order: these messages were
   * queued before everything still in the buffer, and a retry must not reorder
   * a partition's lane. Occupancy can overshoot maxSize by this one batch --
   * documented on BUFFER_DEFAULTS.maxSize.
   */
  restoreBatch(messages) {
    if (messages.length === 0) return
    this.#messages.unshift(...messages)
    if (this.#firstMessageTime === null) this.#firstMessageTime = Date.now()
  }

  /**
   * Sleep, but return early if the buffer is stopped. Used for the retry delay
   * between attempts at a failed batch: a shutdown must not wait out a delay
   * that only exists to pace a broker that is not answering.
   */
  sleepUnlessStopped(millis) {
    if (this.#stopped || millis <= 0) return Promise.resolve()
    return new Promise(resolve => {
      let timer = null
      const wake = () => {
        if (timer) clearTimeout(timer)
        const index = this.#stopWaiters.indexOf(wake)
        if (index !== -1) this.#stopWaiters.splice(index, 1)
        resolve()
      }
      timer = setTimeout(wake, millis)
      this.#stopWaiters.push(wake)
    })
  }

  /**
   * Stop accepting messages and wake everything parked, so shutdown cannot
   * hang. Parked adds REJECT rather than resolve: their message was never
   * buffered, and reporting success for a message that was dropped on the floor
   * is the failure mode this whole change exists to remove.
   */
  stop() {
    if (this.#stopped) return
    this.#stopped = true
    this.cancelTimer()

    const waiters = this.#waiters.slice()
    for (const waiter of waiters) {
      waiter.settle(
        waiter.reject,
        new Error(`Queen buffer ${this.#queueAddress} stopped while waiting for capacity: message not buffered`)
      )
    }

    const sleepers = this.#stopWaiters.splice(0, this.#stopWaiters.length)
    for (const wake of sleepers) wake()
  }

  cancelTimer() {
    // Cancel the timer without triggering a flush
    if (this.#timer) {
      clearTimeout(this.#timer)
      this.#timer = null
    }
  }

  get messageCount() {
    return this.#messages.length
  }

  get options() {
    return this.#options
  }

  get isFlushing() {
    return this.#flushing
  }

  get isStopped() {
    return this.#stopped
  }

  /** True while any add is parked on the bound, or woken but not yet resumed. */
  get hasParkedAdds() {
    return this.#waiters.length > 0 || this.#parked > 0
  }

  get firstMessageAge() {
    return this.#firstMessageTime ? Date.now() - this.#firstMessageTime : 0
  }

  cleanup() {
    this.stop()
    this.#messages = []
    this.#firstMessageTime = null
    this.#flushing = false
  }
}

/**
 * The reason an AbortSignal carries, or a plain AbortError when the runtime
 * (or the caller's controller) did not set one.
 */
function abortReason(signal) {
  if (signal.reason instanceof Error) return signal.reason
  const error = new Error('Queen buffered push aborted while waiting for buffer capacity')
  error.name = 'AbortError'
  if (signal.reason !== undefined) error.cause = signal.reason
  return error
}
