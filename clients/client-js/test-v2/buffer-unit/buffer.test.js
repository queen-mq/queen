/**
 * Client-side push buffer: backpressure and loss-free retry.
 *
 * These pin the 2026-08-20 fix, ported from the Go reference
 * (clients/client-go/message_buffer.go). Two defects were measured there and
 * were present in this SDK too:
 *
 *   1. add() appended to an unbounded array and returned. A producer filling at
 *      1.46M msg/s against a 1.0M msg/s flush pipeline accumulated 20.9M
 *      messages (11.7 GB of RSS) in 45 seconds and lost every one at process
 *      exit, with zero client-side errors reported.
 *   2. The flusher took a batch out of the buffer before the POST and dropped
 *      it on error, losing up to messageCount messages per failed request.
 *
 * No broker and no network: the sink is a fake object with a post() the test
 * drives, which is the whole point -- the buffer's contract is about ordering,
 * occupancy and retries, none of which need a server to observe.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { QueueBuilder } from '../../client-v2/builders/QueueBuilder.js'
import { BufferManager } from '../../client-v2/buffer/BufferManager.js'
import { MessageBuffer } from '../../client-v2/buffer/MessageBuffer.js'
import { BUFFER_DEFAULTS } from '../../client-v2/utils/defaults.js'

const ADDRESS = 'orders/Default'

const item = n => ({ queue: 'orders', partition: 'Default', payload: { n }, transactionId: `t-${n}` })
const payloadNumbers = items => items.map(i => i.payload.n)

/** A sink whose every POST hangs until the test releases or fails it. */
function controlledSink() {
  const sent = []
  const attempts = []
  const inflight = []

  return {
    sent,
    attempts,
    get inflightCount() { return inflight.length },
    post(_path, body) {
      const items = body.items
      attempts.push(items)
      return new Promise((resolve, reject) => {
        inflight.push({ items, resolve, reject })
      })
    },
    /** Complete the oldest in-flight POST successfully. */
    release() {
      const call = inflight.shift()
      assert.ok(call, 'no POST in flight to release')
      sent.push(...call.items)
      call.resolve([])
    },
    /** Fail the oldest in-flight POST. */
    reject(message = 'connect ECONNREFUSED') {
      const call = inflight.shift()
      assert.ok(call, 'no POST in flight to reject')
      call.reject(new Error(message))
    }
  }
}

/**
 * A sink that answers immediately, failing whenever `shouldFail(attemptIndex)`
 * says so. Used for the parity runs, where the interesting question is what
 * comes out the far end after N failures, not when each one happened.
 */
function flakySink(shouldFail) {
  const sent = []
  let attempt = 0
  return {
    sent,
    get attempts() { return attempt },
    async post(_path, body) {
      const index = attempt++
      if (shouldFail(index)) throw new Error(`sink refused attempt ${index}`)
      sent.push(...body.items)
      return []
    }
  }
}

const tick = (ms = 0) => new Promise(resolve => setTimeout(resolve, ms))

/** Poll until `predicate()` holds, or fail the test. Cheaper than a fixed sleep
 *  on an idle machine and less flaky than one on a loaded machine. */
async function until(predicate, what, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await tick(2)
  }
  assert.fail(`timed out waiting for: ${what}`)
}

/** Track whether a promise has settled, without awaiting it. */
function watch(promise) {
  const state = { settled: false, value: undefined, error: undefined }
  promise.then(
    value => { state.settled = true; state.value = value },
    error => { state.settled = true; state.error = error }
  )
  return state
}

describe('buffer options', () => {
  it('resolves an absent or zero maxSize to a BOUND, never to unbounded', () => {
    // Unbounded is the defect this knob closes, so it is deliberately not
    // expressible: 0 means the default bound, not infinity.
    assert.equal(MessageBuffer.normalizeOptions({ messageCount: 10 }).maxSize, 40)
    assert.equal(MessageBuffer.normalizeOptions({ messageCount: 10, maxSize: 0 }).maxSize, 40)
    assert.equal(MessageBuffer.normalizeOptions({ messageCount: 10, maxSize: -1 }).maxSize, 40)
    assert.equal(MessageBuffer.normalizeOptions({}).maxSize, BUFFER_DEFAULTS.maxSize)
  })

  it('floors maxSize at messageCount', () => {
    // A bound below the flush threshold would block the producer before the
    // buffer could assemble the batch that unblocks it.
    assert.equal(MessageBuffer.normalizeOptions({ messageCount: 100, maxSize: 10 }).maxSize, 100)
  })

  it('defaults retryDelayMillis rather than retrying in a hot loop', () => {
    assert.equal(MessageBuffer.normalizeOptions({ messageCount: 10 }).retryDelayMillis, 250)
    assert.equal(MessageBuffer.normalizeOptions({ retryDelayMillis: 0 }).retryDelayMillis, 250)
    assert.equal(MessageBuffer.normalizeOptions({ retryDelayMillis: 5 }).retryDelayMillis, 5)
  })

  it('ships a bounded default', () => {
    assert.equal(BUFFER_DEFAULTS.maxSize, 4 * BUFFER_DEFAULTS.messageCount)
  })
})

describe('backpressure', () => {
  it('parks an add at the bound and resumes it when the flusher drains', async () => {
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 2, timeMillis: 60000, maxSize: 4, retryDelayMillis: 5 }

    // The first two adds trip the count threshold and start the drain, which
    // takes them and then hangs on the sink. The next four pile up behind it.
    for (let n = 0; n < 6; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }
    await until(() => sink.inflightCount === 1, 'the first batch to reach the sink')
    assert.equal(manager.getStats().totalBufferedMessages, 4, 'the buffer should be sitting exactly at its bound')

    // The seventh add has nowhere to go: it must WAIT, not grow the buffer.
    const parked = watch(manager.addMessage(ADDRESS, item(6), options))
    await tick(20)
    assert.equal(parked.settled, false, 'add returned while the buffer was full: that is the unbounded defect')
    assert.equal(manager.getStats().totalBufferedMessages, 4, 'buffer grew past maxSize while an add was parked')

    // Draining a batch frees room, which is what wakes the parked add.
    sink.release()
    await until(() => parked.settled, 'the parked add to resume once capacity freed')
    assert.equal(parked.error, undefined, 'the resumed add must succeed, not error')
    assert.deepEqual(payloadNumbers(sink.sent), [0, 1], 'the drained batch went out in order')

    manager.cleanup()
  })

  it('wakes parked adds on cleanup, and tells them the message was NOT buffered', async () => {
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 2, timeMillis: 60000, maxSize: 2, retryDelayMillis: 5 }

    for (let n = 0; n < 4; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }
    await until(() => manager.getStats().totalBufferedMessages === 2, 'the buffer to reach its bound')

    const parked = watch(manager.addMessage(ADDRESS, item(99), options))
    await tick(20)
    assert.equal(parked.settled, false, 'precondition: the add is parked')

    // Shutdown must not leave a producer waiting forever...
    manager.cleanup()
    await until(() => parked.settled, 'cleanup() to wake the parked add')

    // ...and must not tell it the message went somewhere. It did not.
    assert.ok(parked.error instanceof Error, 'a parked add woken by cleanup must reject, not resolve')
    assert.match(parked.error.message, /not buffered/)
  })

  it('fails a parked add when its AbortSignal fires, instead of reporting success', async () => {
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 2, timeMillis: 60000, maxSize: 2, retryDelayMillis: 5 }

    for (let n = 0; n < 4; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }
    await until(() => manager.getStats().totalBufferedMessages === 2, 'the buffer to reach its bound')

    const controller = new AbortController()
    const parked = watch(manager.addMessage(ADDRESS, item(99), options, { signal: controller.signal }))
    await tick(20)
    assert.equal(parked.settled, false, 'precondition: the add is parked')

    controller.abort()
    await until(() => parked.settled, 'the abort to release the parked add')
    assert.ok(parked.error instanceof Error, 'an aborted add must reject')
    assert.equal(parked.error.name, 'AbortError')
    assert.equal(manager.getStats().totalBufferedMessages, 2, 'an aborted add must not leave its message behind')

    manager.cleanup()
  })
})

describe('failed flushes', () => {
  it('puts a failed batch back at the FRONT, in order, and retries it after retryDelayMillis', async () => {
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 3, timeMillis: 60000, maxSize: 12, retryDelayMillis: 40 }

    for (let n = 0; n < 3; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }
    await until(() => sink.inflightCount === 1, 'the first attempt')

    const failedAt = Date.now()
    sink.reject()

    // The batch is back in the buffer -- not logged and forgotten.
    await until(() => manager.getStats().totalBufferedMessages === 3, 'the failed batch to be re-queued')
    assert.equal(manager.getStats().flushesPerformed, 0, 'a POST that never landed must not count as a flush')

    await until(() => sink.inflightCount === 1, 'the retry')
    assert.ok(Date.now() - failedAt >= 35, 'the retry must wait out retryDelayMillis, not spin')
    assert.deepEqual(payloadNumbers(sink.attempts[1]), [0, 1, 2], 'the retry must resend the same batch, in the same order')

    sink.release()
    await until(() => manager.getStats().totalBufferedMessages === 0, 'the retry to drain the buffer')
    assert.deepEqual(payloadNumbers(sink.sent), [0, 1, 2])
    assert.equal(manager.getStats().flushesPerformed, 1)

    manager.cleanup()
  })

  it('keeps a re-queued batch ahead of messages added while it was failing', async () => {
    // Ordering within a partition is the product's headline promise: a retry
    // that landed behind newer messages would reorder the lane.
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 2, timeMillis: 60000, maxSize: 20, retryDelayMillis: 10 }

    for (let n = 0; n < 2; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }
    await until(() => sink.inflightCount === 1, 'the first attempt')
    sink.reject()
    await until(() => manager.getStats().totalBufferedMessages === 2, 're-queue')

    for (let n = 2; n < 6; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }

    for (let batch = 0; batch < 3; batch++) {
      await until(() => sink.inflightCount === 1, `attempt for batch ${batch}`)
      sink.release()
    }
    await until(() => manager.getStats().totalBufferedMessages === 0, 'the buffer to drain')
    assert.deepEqual(payloadNumbers(sink.sent), [0, 1, 2, 3, 4, 5])

    manager.cleanup()
  })

  it('loses nothing across a run with intermittent failures', async () => {
    // The whole point, stated as count parity: every message a caller was told
    // was buffered comes out of the sink exactly once, in order, however many
    // POSTs failed on the way.
    const total = 500
    const sink = flakySink(attempt => attempt % 3 === 1)
    const manager = new BufferManager(sink)
    const options = { messageCount: 7, timeMillis: 50, maxSize: 21, retryDelayMillis: 1 }

    for (let n = 0; n < total; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }
    await manager.flushAllBuffers()

    assert.equal(sink.sent.length, total, `sent ${sink.sent.length} of ${total}: messages were dropped`)
    assert.deepEqual(payloadNumbers(sink.sent), Array.from({ length: total }, (_, n) => n), 'order was not preserved')
    assert.equal(manager.getStats().totalBufferedMessages, 0)
    assert.ok(sink.attempts > total / options.messageCount, 'precondition: the sink actually failed some attempts')

    manager.cleanup()
  })

  it('bounds an explicit flush by its deadline and says how much is still buffered', async () => {
    // Background flushes retry forever rather than drop; a shutdown cannot.
    const sink = flakySink(() => true)
    const manager = new BufferManager(sink)
    const options = { messageCount: 2, timeMillis: 60000, maxSize: 8, retryDelayMillis: 5 }

    for (let n = 0; n < 4; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }

    await assert.rejects(
      () => manager.flushAllBuffers({ deadlineMillis: 30 }),
      error => {
        assert.equal(error.queenUnflushedCount, 4, 'the error must report what was left unsent')
        assert.match(error.message, /still buffered/)
        return true
      }
    )
    assert.equal(manager.getStats().totalBufferedMessages, 4, 'a flush that gave up must leave the messages in the buffer, not drop them')

    manager.cleanup()
  })
})

describe('drain loop', () => {
  it('runs one drain per buffer, no matter how many adds trip the threshold', async () => {
    // Two senders on one partition would interleave batches and reorder the
    // lane; the flushing flag is what prevents that.
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 2, timeMillis: 60000, maxSize: 100, retryDelayMillis: 5 }

    for (let n = 0; n < 10; n++) {
      await manager.addMessage(ADDRESS, item(n), options)
    }
    await until(() => sink.inflightCount === 1, 'the first batch')
    await tick(20)
    assert.equal(sink.inflightCount, 1, 'a second drain started: batches can now interleave')
    assert.equal(sink.attempts.length, 1)

    manager.cleanup()
  })

  it('keeps separate buffers per queue/partition address', async () => {
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 100, timeMillis: 60000, maxSize: 400, retryDelayMillis: 5 }

    await manager.addMessage('orders/eu', item(1), options)
    await manager.addMessage('orders/us', item(2), options)
    await manager.addMessage('other/eu', item(3), options)

    const stats = manager.getStats()
    assert.equal(stats.activeBuffers, 3)
    assert.equal(stats.totalBufferedMessages, 3)

    manager.cleanup()
  })

  it('keeps the public push API shape now that the add path can wait', async () => {
    // addMessage became awaitable so backpressure could exist at all. The
    // builder is what most callers actually touch, so pin its shape: a buffered
    // push still resolves to { buffered, count } and still lands in the buffer.
    // The builder is driven over the fake sink rather than a Queen, so the
    // test owns the teardown: a real client's pending flush timer would keep
    // the runner alive for the full timeMillis.
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const builder = new QueueBuilder(null, sink, manager, 'orders')

    const result = await builder
      .partition('eu')
      .buffer({ messageCount: 1000, timeMillis: 60000 })
      .push([{ data: { n: 1 } }, { data: { n: 2 } }])

    assert.deepEqual(result, { buffered: true, count: 2 })
    const stats = manager.getStats()
    assert.equal(stats.totalBufferedMessages, 2)
    assert.equal(stats.activeBuffers, 1, 'one buffer per queue/partition, not one per push')

    manager.cleanup()
  })

  it('reports the items a stopped buffer refused instead of counting them as pushed', async () => {
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const builder = new QueueBuilder(null, sink, manager, 'orders')
    manager.cleanup() // client already closing

    // push() returns a thenable builder, not a Promise, so await it inside.
    await assert.rejects(
      async () => { await builder.buffer({ messageCount: 1000, timeMillis: 60000 }).push([{ data: { n: 1 } }]) },
      /not buffered/
    )
    assert.equal(manager.getStats().totalBufferedMessages, 0, 'a closed client must not accumulate messages nothing will flush')
  })

  it('flushes a buffer that never reaches its count, on the timer', async () => {
    // The branch every other test here avoids with a 60s timer, and the one a
    // low-volume producer actually lives on.
    const sink = flakySink(() => false)
    const manager = new BufferManager(sink)
    const options = { messageCount: 1000, timeMillis: 30, maxSize: 4000, retryDelayMillis: 5 }

    await manager.addMessage(ADDRESS, item(1), options)
    await manager.addMessage(ADDRESS, item(2), options)
    assert.equal(manager.getStats().totalBufferedMessages, 2, 'two messages are nowhere near the threshold')

    await until(() => sink.sent.length === 2, 'the time-based flush to fire')
    manager.cleanup()
  })
})
