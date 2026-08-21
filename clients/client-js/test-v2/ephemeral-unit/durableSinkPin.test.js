/**
 * The durable sink, pinned to the byte.
 *
 * EPHEMERAL_QUEUES.md §4.1 buys the ephemeral buffered push by PARAMETRIZING
 * the drain rather than duplicating it, and §7.3 names the price of that
 * bargain: "a pin that the DURABLE sink's bodies are byte-identical before and
 * after the sink refactor". This file is that pin.
 *
 * It is written against the durable path only. Nothing here mentions an
 * ephemeral queue, and that is deliberate: the question it answers is not
 * "does the new feature work" but "did the refactor that made the new feature
 * possible move a single byte on the path that was already in production". A
 * `{queue, partition, payload, transactionId}` item whose key ORDER changed, an
 * envelope that grew a `queue` field because the ephemeral wire has one, a path
 * that became `/api/v1/push/batch` -- none of those would fail a deepEqual on a
 * parsed body, and all of them are a broken 1.0.6 producer.
 *
 * The literal below is the request the buffered durable push made before sinks
 * existed. It is not derived from the code under test: derive it and the pin
 * pins nothing.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { BufferManager } from '../../client-v2/buffer/BufferManager.js'
import { MessageBuffer } from '../../client-v2/buffer/MessageBuffer.js'
import { DURABLE_SINK, DURABLE_DESTINATION, durableAddress } from '../../client-v2/buffer/sinks.js'
import { withPlanServer, ok } from './_planServer.js'

const tick = (ms = 0) => new Promise(resolve => setTimeout(resolve, ms))

async function until(predicate, what, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await tick(2)
  }
  assert.fail(`timed out waiting for: ${what}`)
}

/** The exact bytes a buffered durable push of two items has always produced. */
const PINNED_BODY =
  '{"items":[' +
  '{"queue":"orders","partition":"Default","payload":{"n":1},"transactionId":"fixed-1"},' +
  '{"queue":"orders","partition":"Default","payload":{"n":2},"transactionId":"fixed-2"}' +
  ']}'

describe('durable sink — byte identity', () => {
  it('drains the buffered durable push to the same path, with the same bytes', async () => {
    await withPlanServer([ok([])], ok([]), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        await queen
          .queue('orders')
          .partition('Default')
          .buffer({ messageCount: 2, timeMillis: 60000 })
          .push([
            { data: { n: 1 }, transactionId: 'fixed-1' },
            { data: { n: 2 }, transactionId: 'fixed-2' }
          ])

        await until(() => hits.length === 1, 'the count threshold to drain the batch')
        assert.equal(hits[0].method, 'POST')
        assert.equal(hits[0].url, '/api/v1/push')
        assert.equal(hits[0].raw, PINNED_BODY, 'the durable buffered push must be byte-identical to 1.0.6')
      } finally {
        await queen.close()
      }
    })
  })

  it('formats the envelope from the batch alone — the queue and partition arguments do not leak into it', () => {
    // The durable wire repeats the identity on every ITEM, so the sink is handed
    // a queue and a partition it must ignore. An implementation that hoisted
    // them to the envelope "for symmetry" with the ephemeral sink would produce
    // a body no broker of any version has ever parsed.
    const batch = [{ queue: 'orders', partition: 'Default', payload: { n: 1 }, transactionId: 'fixed-1' }]
    assert.equal(DURABLE_SINK.path, '/api/v1/push')
    assert.equal(JSON.stringify(DURABLE_SINK.format('ignored', 'ignored', batch)), JSON.stringify({ items: batch }))
    assert.equal(JSON.stringify(DURABLE_SINK.format(null, null, batch)), JSON.stringify({ items: batch }))
  })

  it('is what a buffer gets when nobody names a destination', async () => {
    // Every caller written before sinks existed passes three arguments to
    // addMessage. They must keep draining where they always did, without
    // knowing the parameter exists.
    const posts = []
    const httpClient = { async post(path, body) { posts.push({ path, body }); return [] } }
    const manager = new BufferManager(httpClient)

    const address = durableAddress('orders', 'Default')
    assert.equal(address, 'orders/Default', 'the durable address is unchanged: no namespace prefix')

    await manager.addMessage(address, { queue: 'orders', partition: 'Default', payload: { n: 1 }, transactionId: 'fixed-1' }, { messageCount: 1, timeMillis: 60000 })
    await until(() => posts.length === 1, 'the drain')

    assert.equal(posts[0].path, '/api/v1/push')
    assert.deepEqual(Object.keys(posts[0].body), ['items'], 'the durable envelope is {items} and only {items}')
    assert.deepEqual(posts[0].body.items, [{ queue: 'orders', partition: 'Default', payload: { n: 1 }, transactionId: 'fixed-1' }])

    manager.cleanup()
  })

  it('defaults a MessageBuffer built without one to the durable destination', () => {
    const buffer = new MessageBuffer('orders/Default', { messageCount: 1 }, () => {})
    assert.equal(buffer.destination, DURABLE_DESTINATION)
    assert.equal(buffer.destination.sink.path, '/api/v1/push')
    buffer.cleanup()
  })
})
