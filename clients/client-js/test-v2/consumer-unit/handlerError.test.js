/**
 * consume(): what a handler that throws does to the consumer.
 *
 * The rule, the same with and without autoAck: the messages the handler was
 * given are NACKED (the broker redelivers them, and files them in the DLQ
 * once the queue's retryLimit is spent) and the worker KEEPS CONSUMING.
 * autoAck(false) hands the success path to the handler, not the failure path:
 * a handler that threw did not get to settle its messages.
 *
 * Before: under autoAck(false) the error left the worker loop, the consume()
 * promise rejected after one message, and the messages stayed leased until
 * the lease expired (found 2026-10-02 against 2.0.0-beta.6). With
 * concurrency > 1 the other workers kept running behind a promise that had
 * already rejected.
 *
 * `.onError(fn)` is the way to decide for yourself: the error never reaches
 * the consumer, so nothing is nacked on your behalf.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen } from '../../client-v2/index.js'

const GROUP = 'workers'

const message = (n) => ({
  id: `msg-${n}`,
  transactionId: `tx-${n}`,
  partitionId: '7',
  partition: 'p1',
  leaseId: `lease-${n}`,
  consumerGroup: GROUP,
  data: { n },
  createdAt: '2026-10-02T10:00:00.000Z'
})

/**
 * A fake broker routed by path: each pop answers the next batch of `pops`
 * (an empty 204 once they run out), each ack or batch ack is accepted. Every
 * request is recorded.
 */
async function withBroker(pops, run) {
  const queue = [...pops]
  const requests = []
  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) : null
      const path = req.url.split('?')[0]
      requests.push({ method: req.method, path, body })
      if (req.method === 'GET' && path.startsWith('/api/v1/pop')) {
        const batch = queue.shift()
        if (!batch) { res.writeHead(204); res.end(); return }
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ success: true, consumerGroup: GROUP, messages: batch }))
        return
      }
      if (req.method === 'POST' && (path === '/api/v1/ack' || path === '/api/v1/ack/batch')) {
        const acks = body.acknowledgments || [body]
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(acks.map((a, i) => ({ index: i, transactionId: a.transactionId, success: true, error: null }))))
        return
      }
      res.writeHead(404, { 'Content-Type': 'application/json' })
      res.end('{"error":"not found"}')
    })
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const queen = new Queen({ url: `http://127.0.0.1:${server.address().port}`, handleSignals: false })
  try {
    await run(queen, requests)
  } finally {
    await queen.close()
    await new Promise(resolve => server.close(resolve))
  }
}

const acksIn = (requests) => requests.filter(r => r.method === 'POST' && r.path.startsWith('/api/v1/ack'))
const statusesOf = (ackRequest) => (ackRequest.body.acknowledgments || [ackRequest.body]).map(a => a.status)

describe('consume() — a handler that throws is nacked, and the consumer keeps going', () => {
  it('autoAck(false), one message at a time: nack, redelivery, done', async () => {
    // Delivery 1 of tx-1 fails, delivery 2 succeeds and the handler acks it.
    await withBroker([[message(1)], [message(1)]], async (queen, requests) => {
      const seen = []
      await queen.queue('orders').group(GROUP).wait(false).autoAck(false).each().limit(2)
        .consume(async (msg) => {
          seen.push(msg.transactionId)
          if (seen.length === 1) throw new Error('boom')
          await queen.ack(msg, true)
        })

      assert.deepEqual(seen, ['tx-1', 'tx-1'], 'the consumer survived the throw and got the redelivery')
      const acks = acksIn(requests)
      assert.equal(acks.length, 2)
      assert.deepEqual(statusesOf(acks[0]), ['failed'], 'the throw nacked the message')
      assert.equal(acks[0].body.consumerGroup, GROUP)
      assert.equal(acks[0].body.leaseId, 'lease-1', 'the nack names the lease it releases')
      assert.equal(acks[0].body.error, 'boom', 'the handler error travels with the nack')
      assert.deepEqual(statusesOf(acks[1]), ['completed'], 'the handler\'s own ack of the redelivery')
    })
  })

  it('autoAck(false), batch handler: the whole batch is nacked in one call', async () => {
    await withBroker([[message(1), message(2)], [message(1), message(2)]], async (queen, requests) => {
      let calls = 0
      await queen.queue('orders').group(GROUP).wait(false).autoAck(false).limit(3)
        .consume(async (msgs) => {
          calls++
          if (calls === 1) throw new Error('batch boom')
          await queen.ack(msgs, true)
        })

      assert.equal(calls, 2)
      const acks = acksIn(requests)
      assert.equal(acks[0].path, '/api/v1/ack/batch')
      assert.deepEqual(statusesOf(acks[0]), ['failed', 'failed'])
      assert.equal(acks[0].body.consumerGroup, GROUP)
      assert.deepEqual(statusesOf(acks[1]), ['completed', 'completed'])
    })
  })

  it('autoAck(false), .each(): the rest of the popped batch is abandoned after a nack', async () => {
    // The nack released the lease and clamps the cursor at tx-1, so tx-2 comes
    // back with it: handling it now would only produce a duplicate.
    await withBroker([[message(1), message(2)], [message(1), message(2)]], async (queen) => {
      const seen = []
      await queen.queue('orders').group(GROUP).wait(false).autoAck(false).each().limit(3)
        .consume(async (msg) => {
          seen.push(msg.transactionId)
          if (seen.length === 1) throw new Error('boom')
        })
      assert.deepEqual(seen, ['tx-1', 'tx-1', 'tx-2'])
    })
  })

  it('autoAck(true) behaves the same way: nack and keep going', async () => {
    await withBroker([[message(1)], [message(1)]], async (queen, requests) => {
      let calls = 0
      await queen.queue('orders').group(GROUP).wait(false).each().limit(2)
        .consume(async () => { if (++calls === 1) throw new Error('boom') })
      assert.equal(calls, 2)
      assert.deepEqual(acksIn(requests).map(statusesOf), [['failed'], ['completed']])
    })
  })

  it('with .onError() the handler decides: nothing is nacked on its behalf', async () => {
    await withBroker([[message(1)], [message(2)]], async (queen, requests) => {
      const failures = []
      await queen.queue('orders').group(GROUP).wait(false).autoAck(false).each().limit(2)
        .consume(async (msg) => { if (msg.transactionId === 'tx-1') throw new Error('boom') })
        .onError(async (msg, err) => { failures.push([msg.transactionId, err.message]) })

      assert.deepEqual(failures, [['tx-1', 'boom']])
      assert.equal(acksIn(requests).length, 0, 'autoAck(false) + onError: the consumer sent no ack and no nack')
    })
  })
})
