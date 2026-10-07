/**
 * each(): a nack releases ONE partition. A multi-partition pop claims several
 * partitions under one lease; when the handler fails a message, the nack
 * releases that message's partition and clamps its cursor, so the later
 * messages of THAT partition come back on the next pop. The other partitions
 * are still leased to this worker: their messages must be handled now.
 *
 * Before: the loop abandoned the whole popped batch after a nack. The other
 * partitions' messages stayed leased and came back only when the lease
 * expired (found live 2026-10-06 against 2.0.1: B1 and B2 waited the whole
 * 6 s lease after A1 failed).
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen } from '../../client-v2/index.js'

const GROUP = 'workers'

const message = (partition, n) => ({
  id: `msg-${partition}${n}`,
  transactionId: `tx-${partition}${n}`,
  partitionId: `pid-${partition}`,
  partition,
  leaseId: 'lease-1',
  consumerGroup: GROUP,
  data: { tag: `${partition}${n}` },
  createdAt: '2026-10-06T10:00:00.000Z'
})

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

describe('each(): a nack skips only its own partition', () => {
  it('handles the other partitions of the pop after a failure', async () => {
    const pop = [message('A', 1), message('A', 2), message('B', 1), message('B', 2)]
    await withBroker([pop], async (queen, requests) => {
      const handled = []
      await queen.queue('orders').group(GROUP).each().batch(4).limit(3).idleMillis(500)
        .consume(async (m) => {
          handled.push(m.data.tag)
          if (m.data.tag === 'A1') throw new Error('A1 fails')
        })

      assert.deepEqual(handled, ['A1', 'B1', 'B2'], 'A2 is skipped (it comes back after the nack), B is handled now')
      const settled = requests.filter(r => r.path.startsWith('/api/v1/ack'))
        .flatMap(r => r.body.acknowledgments || [r.body])
        .map(a => `${a.transactionId}:${a.status}`)
      assert.deepEqual(settled, ['tx-A1:failed', 'tx-B1:completed', 'tx-B2:completed'])
    })
  })
})
