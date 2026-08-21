/**
 * Buffered ephemeral push: the sink refactor, from both ends
 * (EPHEMERAL_QUEUES.md §4.1).
 *
 * The claim being tested is "the machinery is REUSED, not duplicated". That
 * splits into two questions and this file answers them with two different
 * instruments:
 *
 *   1. does a buffered ephemeral push produce the ephemeral REQUEST? -- asked
 *      against a real socket through a real Queen, because the answer is bytes;
 *   2. does it keep the buffer's ORDERING, RETRY and DEADLINE behaviour? --
 *      asked against a fake http client the test drives by hand, because the
 *      answer is timing, and a real server would only add flakiness to a
 *      question that has nothing to do with the network.
 *
 * The second half deliberately mirrors buffer-unit/buffer.test.js: the same
 * controlled sink, the same `until` polling, the same assertions -- pointed at
 * the ephemeral destination. If the two files ever disagree, the sink parameter
 * has leaked into semantics it was never supposed to touch.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { BufferManager } from '../../client-v2/buffer/BufferManager.js'
import { ephemeralAddress, ephemeralDestination, EPHEMERAL_SINK } from '../../client-v2/buffer/sinks.js'
import { withPlanServer, ok, pushed } from './_planServer.js'

const QUEUE = 'presence'
const ADDRESS = ephemeralAddress(QUEUE, 'room-7')
const DESTINATION = ephemeralDestination(QUEUE, 'room-7')

const message = n => ({ payload: { n } })
const payloadNumbers = items => items.map(i => i.payload.n)

const tick = (ms = 0) => new Promise(resolve => setTimeout(resolve, ms))

async function until(predicate, what, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await tick(2)
  }
  assert.fail(`timed out waiting for: ${what}`)
}

/** A fake http client whose every POST hangs until the test releases or fails it. */
function controlledSink() {
  const sent = []
  const attempts = []
  const inflight = []

  return {
    sent,
    attempts,
    get inflightCount() { return inflight.length },
    post(path, body) {
      attempts.push({ path, body })
      return new Promise((resolve, reject) => {
        inflight.push({ messages: body.messages, resolve, reject })
      })
    },
    release() {
      const call = inflight.shift()
      assert.ok(call, 'no POST in flight to release')
      sent.push(...call.messages)
      call.resolve({ pushed: call.messages.length })
    },
    reject(reason = 'connect ECONNREFUSED') {
      const call = inflight.shift()
      assert.ok(call, 'no POST in flight to reject')
      call.reject(new Error(reason))
    }
  }
}

/** A fake http client that answers immediately, failing whenever asked to. */
function flakySink(shouldFail) {
  const sent = []
  let attempt = 0
  return {
    sent,
    get attempts() { return attempt },
    async post(_path, body) {
      const index = attempt++
      if (shouldFail(index)) throw new Error(`sink refused attempt ${index}`)
      sent.push(...body.messages)
      return { pushed: body.messages.length }
    }
  }
}

describe('buffered ephemeral push — the request it drains into', () => {
  it('drains one batch to the ephemeral route, with the identity on the envelope', async () => {
    await withPlanServer([pushed(3)], ok({ pushed: 0 }), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        const res = await queen.ephemeral.push(
          QUEUE,
          [{ n: 1 }, { n: 2 }, { n: 3 }],
          { partition: 'room-7', buffered: { messageCount: 3, timeMillis: 60000 } }
        )
        // Resolves once the messages are IN the buffer, not once they landed.
        assert.deepEqual(res, { buffered: true, count: 3 })

        await until(() => hits.length === 1, 'the count threshold to drain one batch')
        assert.equal(hits[0].method, 'POST')
        assert.equal(hits[0].url, '/api/v1/ephemeral/push')
        assert.deepEqual(hits[0].body, {
          queue: QUEUE,
          partition: 'room-7',
          messages: [{ payload: { n: 1 } }, { payload: { n: 2 } }, { payload: { n: 3 } }]
        })
      } finally {
        await queen.close()
      }
    })
  })

  it('omits `partition` on the drained batch when the push named none', async () => {
    await withPlanServer([pushed(2)], ok({ pushed: 0 }), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        await queen.ephemeral.push(QUEUE, [{ n: 1 }, { n: 2 }], { buffered: { messageCount: 2, timeMillis: 60000 } })
        await until(() => hits.length === 1, 'the batch')
        assert.deepEqual(hits[0].body, { queue: QUEUE, messages: [{ payload: { n: 1 } }, { payload: { n: 2 } }] })
      } finally {
        await queen.close()
      }
    })
  })

  it('close() drains an ephemeral buffer that never reached its threshold', async () => {
    // §4.1: the same flushAllBuffers deadline path the durable buffers use.
    // Without it, a short-lived producer's last partial batch dies with the
    // process — which the class contract permits and nobody wants.
    await withPlanServer([pushed(1)], ok({ pushed: 0 }), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      await queen.ephemeral.push(QUEUE, [{ n: 1 }], { buffered: { messageCount: 1000, timeMillis: 600000 } })
      assert.equal(hits.length, 0, 'precondition: nothing has been sent yet')

      await queen.close()

      assert.equal(hits.length, 1, 'close() must flush what is buffered')
      assert.equal(hits[0].url, '/api/v1/ephemeral/push')
      assert.deepEqual(hits[0].body.messages, [{ payload: { n: 1 } }])
    })
  })

  it('never shares a buffer with the durable queue of the same name', async () => {
    // The two are unrelated objects (§10 Q8). A shared address would post one
    // family's messages to the other family's route.
    await withPlanServer([], ok({ pushed: 0 }), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        await queen.ephemeral.push(QUEUE, [{ n: 1 }], { partition: 'Default', buffered: { messageCount: 1000, timeMillis: 600000 } })
        await queen.queue(QUEUE).partition('Default').buffer({ messageCount: 1000, timeMillis: 600000 }).push([{ data: { n: 2 } }])

        assert.equal(queen.getBufferStats().activeBuffers, 2, 'one buffer per family, never one shared')
        assert.equal(queen.getBufferStats().totalBufferedMessages, 2)

        await queen.flushAllBuffers()
        assert.equal(hits.length, 2)

        const byPath = Object.fromEntries(hits.map(h => [h.url, h.body]))
        assert.deepEqual(byPath['/api/v1/ephemeral/push'].messages, [{ payload: { n: 1 } }])
        assert.equal(byPath['/api/v1/push'].items.length, 1)
        assert.equal(byPath['/api/v1/push'].items[0].queue, QUEUE, 'the durable item still repeats its own identity')
      } finally {
        await queen.close()
      }
    })
  })

  it('addresses one buffer per queue/partition, namespaced under `eph:`', () => {
    assert.equal(ephemeralAddress('presence', 'room-7'), 'eph:presence/room-7')
    assert.equal(ephemeralAddress('presence'), 'eph:presence', 'no partition is its own destination, not `Default`')
    assert.equal(EPHEMERAL_SINK.path, '/api/v1/ephemeral/push')
  })
})

describe('buffered ephemeral push — the semantics it inherits', () => {
  it('parks an add at the bound and resumes it when the flusher drains', async () => {
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 2, timeMillis: 60000, maxSize: 4, retryDelayMillis: 5 }

    for (let n = 0; n < 6; n++) {
      await manager.addMessage(ADDRESS, message(n), options, { destination: DESTINATION })
    }
    await until(() => sink.inflightCount === 1, 'the first batch to reach the sink')
    assert.equal(manager.getStats().totalBufferedMessages, 4, 'the buffer should be sitting exactly at its bound')

    let settled = false
    const parked = manager.addMessage(ADDRESS, message(6), options, { destination: DESTINATION })
    parked.then(() => { settled = true }, () => { settled = true })
    await tick(20)
    assert.equal(settled, false, 'add returned while the buffer was full: that is the unbounded defect')

    sink.release()
    await until(() => settled, 'the parked add to resume once capacity freed')
    assert.deepEqual(payloadNumbers(sink.sent), [0, 1], 'the drained batch went out in order')
    assert.equal(sink.attempts[0].path, '/api/v1/ephemeral/push')

    manager.cleanup()
  })

  it('puts a failed batch back at the FRONT and retries the same messages, in order', async () => {
    const sink = controlledSink()
    const manager = new BufferManager(sink)
    const options = { messageCount: 3, timeMillis: 60000, maxSize: 12, retryDelayMillis: 40 }

    for (let n = 0; n < 3; n++) {
      await manager.addMessage(ADDRESS, message(n), options, { destination: DESTINATION })
    }
    await until(() => sink.inflightCount === 1, 'the first attempt')

    const failedAt = Date.now()
    sink.reject()

    await until(() => manager.getStats().totalBufferedMessages === 3, 'the failed batch to be re-queued')
    assert.equal(manager.getStats().flushesPerformed, 0, 'a POST that never landed must not count as a flush')

    await until(() => sink.inflightCount === 1, 'the retry')
    assert.ok(Date.now() - failedAt >= 35, 'the retry must wait out retryDelayMillis, not spin')
    assert.deepEqual(payloadNumbers(sink.attempts[1].body.messages), [0, 1, 2], 'the retry resends the same batch, in the same order')
    assert.equal(sink.attempts[1].path, '/api/v1/ephemeral/push', 'and to the same route')

    sink.release()
    await until(() => manager.getStats().totalBufferedMessages === 0, 'the retry to drain the buffer')
    assert.deepEqual(payloadNumbers(sink.sent), [0, 1, 2])

    manager.cleanup()
  })

  it('loses nothing across a run with intermittent failures', async () => {
    const total = 200
    const sink = flakySink(attempt => attempt % 3 === 1)
    const manager = new BufferManager(sink)
    const options = { messageCount: 7, timeMillis: 50, maxSize: 21, retryDelayMillis: 1 }

    for (let n = 0; n < total; n++) {
      await manager.addMessage(ADDRESS, message(n), options, { destination: DESTINATION })
    }
    await manager.flushAllBuffers()

    assert.equal(sink.sent.length, total, `sent ${sink.sent.length} of ${total}: messages were dropped`)
    assert.deepEqual(payloadNumbers(sink.sent), Array.from({ length: total }, (_, n) => n), 'order was not preserved')
    assert.ok(sink.attempts > total / options.messageCount, 'precondition: the sink actually failed some attempts')

    manager.cleanup()
  })

  it('bounds an explicit flush by its deadline and says how much is still buffered', async () => {
    const sink = flakySink(() => true)
    const manager = new BufferManager(sink)
    const options = { messageCount: 2, timeMillis: 60000, maxSize: 8, retryDelayMillis: 5 }

    for (let n = 0; n < 4; n++) {
      await manager.addMessage(ADDRESS, message(n), options, { destination: DESTINATION })
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

  it('reports the messages a stopped buffer refused instead of counting them as pushed', async () => {
    await withPlanServer([], ok({ pushed: 0 }), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      await queen.close() // client already closing

      await assert.rejects(
        () => queen.ephemeral.push(QUEUE, [{ n: 1 }], { buffered: { messageCount: 1000, timeMillis: 60000 } }),
        /not buffered/
      )
      assert.equal(hits.length, 0)
    })
  })

  it('translates `intervalMillis` into the linger the buffer actually reads', async () => {
    // MessageBuffer carries unknown option keys through untouched, so an
    // un-translated `intervalMillis` would leave the buffer on its 1s default
    // and silently ignore what the caller asked for.
    await withPlanServer([pushed(1)], ok({ pushed: 0 }), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        await queen.ephemeral.push(QUEUE, [{ n: 1 }], { buffered: { messageCount: 1000, intervalMillis: 20 } })
        await until(() => hits.length === 1, 'the time-based flush to fire on the interval it was given')
        assert.deepEqual(hits[0].body.messages, [{ payload: { n: 1 } }])
      } finally {
        await queen.close()
      }
    })
  })
})
