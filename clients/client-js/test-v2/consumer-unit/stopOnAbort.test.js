/**
 * consume() — what aborting its signal does.
 *
 * The rule: stopping a consumer never strands a message. The long poll in
 * flight is closed, so the broker stops handing this consumer messages (a
 * broker hands nothing to a poll whose caller is gone, and releases what a
 * forwarded pop claimed for one). A message the client already holds but has
 * not handed to the handler goes back with a `retry` ack: the lease is
 * released, the message is redelivered first, and no retry is charged.
 *
 * Before: the signal was only checked between polls. A poll open at the
 * abort stayed open for up to its timeout, the broker could still hand it a
 * message, and each() then dropped that message without settling it. The
 * partition stayed blocked until the lease expired (measured against 2.0.1:
 * the full lease, 60-180 s on a rolling restart, every restart).
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen } from '../../client-v2/index.js'
import { HttpClient } from '../../client-v2/http/HttpClient.js'
import { LoadBalancer } from '../../client-v2/http/LoadBalancer.js'

const GROUP = 'workers'

const message = (n) => ({
  id: `msg-${n}`,
  transactionId: `tx-${n}`,
  partitionId: '7',
  partition: 'p1',
  leaseId: 'lease-1',
  consumerGroup: GROUP,
  data: { n },
  createdAt: '2026-10-06T10:00:00.000Z'
})

/**
 * A fake broker. A pop answers the next batch of `pops`; once they run out it
 * is held open, like a long poll on an idle queue, and recorded as `closed`
 * when the client goes away. `ackStatus` answers acks (default: accepted).
 */
async function startBroker(pops = [], { ackStatus = 200 } = {}) {
  const queue = [...pops]
  const requests = []
  const held = []
  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) : null
      const path = req.url.split('?')[0]
      const record = { method: req.method, path, body, closed: false }
      requests.push(record)
      if (req.method === 'GET' && path.startsWith('/api/v1/pop')) {
        const batch = queue.shift()
        if (batch) {
          res.writeHead(200, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ success: true, consumerGroup: GROUP, messages: batch }))
          return
        }
        res.on('close', () => { if (!res.writableEnded) record.closed = true })
        held.push(res)
        return
      }
      if (req.method === 'POST' && (path === '/api/v1/ack' || path === '/api/v1/ack/batch')) {
        const acks = body.acknowledgments || [body]
        res.writeHead(ackStatus, { 'Content-Type': 'application/json' })
        res.end(ackStatus === 200
          ? JSON.stringify(acks.map((a, i) => ({ index: i, transactionId: a.transactionId, success: true, error: null, leaseReleased: true })))
          : '{"error":"unavailable"}')
        return
      }
      res.writeHead(404, { 'Content-Type': 'application/json' })
      res.end('{"error":"not found"}')
    })
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    requests,
    pops: () => requests.filter(r => r.method === 'GET' && r.path.startsWith('/api/v1/pop')),
    acks: () => requests.filter(r => r.method === 'POST' && r.path.startsWith('/api/v1/ack')),
    async stop() {
      for (const res of held) if (!res.writableEnded && !res.destroyed) { res.writeHead(204); res.end() }
      server.closeAllConnections()
      await new Promise(resolve => server.close(resolve))
    }
  }
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

/** Resolves with 'resolved' / 'rejected: …', or 'pending' if it took longer than `ms`. */
const settleWithin = (promise, ms) => Promise.race([
  Promise.resolve(promise).then(() => 'resolved', e => `rejected: ${e.message}`),
  sleep(ms).then(() => 'pending')
])

const settledStatuses = (acks) => acks.flatMap(a => (a.body.acknowledgments || [a.body]).map(x => `${x.transactionId}:${x.status}`))

describe('consume() — aborting the signal never strands a message', () => {
  it('closes the long poll in flight, and the consumer ends at once', async () => {
    const broker = await startBroker()
    const queen = new Queen({ url: broker.url, handleSignals: false })
    try {
      const ac = new AbortController()
      const running = queen.queue('orders').group(GROUP).timeoutMillis(30000)
        .consume(async () => {}, { signal: ac.signal })
      const outcome = settleWithin(running, 2000)
      await sleep(150)
      assert.equal(broker.pops().length, 1, 'one long poll is open')

      ac.abort()

      assert.equal(await outcome, 'resolved', 'consume() resolves without waiting out the 30 s poll')
      await sleep(50)
      assert.equal(broker.pops()[0].closed, true, 'the client closed the poll it had open')
      assert.equal(broker.pops().length, 1, 'and opened no other')
    } finally {
      await queen.close()
      await broker.stop()
    }
  })

  it('is not a backend failure: the poll does not fail over to another node', async () => {
    const a = await startBroker()
    const b = await startBroker()
    const queen = new Queen({ urls: [a.url, b.url], enableFailover: true, loadBalancingStrategy: 'affinity', handleSignals: false })
    try {
      const ac = new AbortController()
      const running = queen.queue('orders').group(GROUP).timeoutMillis(30000)
        .consume(async () => {}, { signal: ac.signal })
      const outcome = settleWithin(running, 2000)
      await sleep(150)

      ac.abort()

      assert.equal(await outcome, 'resolved')
      await sleep(50)
      assert.equal(a.pops().length + b.pops().length, 1, 'the abort did not fail over to the other node')
    } finally {
      await queen.close()
      await a.stop()
      await b.stop()
    }
  })

  it('HttpClient: an aborted request rejects as aborted, and leaves every node healthy', async () => {
    const a = await startBroker()
    const b = await startBroker()
    const lb = new LoadBalancer([a.url, b.url], 'affinity')
    const http = new HttpClient({ loadBalancer: lb, enableFailover: true, retryAttempts: 3 })
    try {
      const ac = new AbortController()
      const request = http.get('/api/v1/pop/queue/orders?wait=true&timeout=30000', 35000, 'orders:*:workers', 'pop', ac.signal)
      const settled = request.then(() => null, e => e)
      await sleep(150)

      ac.abort()

      const error = await settled
      assert.equal(error?.aborted, true, 'the rejection says the caller aborted it')
      assert.equal(a.pops().length + b.pops().length, 1, 'no other node was tried')
      for (const [url, status] of lb.getHealthStatus()) {
        assert.equal(status.healthy, true, `${url} is still healthy`)
      }
    } finally {
      await a.stop()
      await b.stop()
    }
  })

  it('wait(false): an abort while a pop is in flight ends the consumer without an error', async () => {
    const broker = await startBroker()
    const queen = new Queen({ url: broker.url, handleSignals: false })
    try {
      const ac = new AbortController()
      const running = queen.queue('orders').group(GROUP).wait(false).timeoutMillis(30000)
        .consume(async () => {}, { signal: ac.signal })
      const outcome = settleWithin(running, 2000)
      await sleep(150)

      ac.abort()

      assert.equal(await outcome, 'resolved', 'consume() resolves, it does not reject')
    } finally {
      await queen.close()
      await broker.stop()
    }
  })

  it('each(): the messages not yet handed to the handler go back with a retry ack', async () => {
    const broker = await startBroker([[message(1), message(2), message(3)]])
    const queen = new Queen({ url: broker.url, handleSignals: false })
    try {
      const ac = new AbortController()
      const seen = []
      const running = queen.queue('orders').group(GROUP).batch(3).each()
        .consume(async (msg) => { seen.push(msg.transactionId); ac.abort() }, { signal: ac.signal })

      assert.equal(await settleWithin(running, 2000), 'resolved')
      assert.deepEqual(seen, ['tx-1'], 'nothing is handed to the handler after the stop')
      assert.deepEqual(settledStatuses(broker.acks()), ['tx-1:completed', 'tx-2:retry', 'tx-3:retry'],
        'the one in the handler finishes; the other two are released, not dropped')
      const release = broker.acks().at(-1).body
      for (const a of release.acknowledgments || [release]) {
        assert.equal(a.leaseId ?? release.leaseId, 'lease-1', 'the release names the lease it gives back')
      }
    } finally {
      await queen.close()
      await broker.stop()
    }
  })

  it('each() with a limit: the messages past the limit go back too, instead of staying leased', async () => {
    const broker = await startBroker([[message(1), message(2), message(3)]])
    const queen = new Queen({ url: broker.url, handleSignals: false })
    try {
      const seen = []
      const running = queen.queue('orders').group(GROUP).batch(3).each().limit(1)
        .consume(async (msg) => { seen.push(msg.transactionId) })

      assert.equal(await settleWithin(running, 2000), 'resolved')
      assert.deepEqual(seen, ['tx-1'])
      assert.deepEqual(settledStatuses(broker.acks()), ['tx-1:completed', 'tx-2:retry', 'tx-3:retry'])
    } finally {
      await queen.close()
      await broker.stop()
    }
  })

  it('a release that cannot be delivered does not fail the consumer: the lease is the fallback', async () => {
    const broker = await startBroker([[message(1), message(2)]], { ackStatus: 503 })
    const queen = new Queen({ url: broker.url, handleSignals: false, retryAttempts: 1 })
    try {
      const ac = new AbortController()
      const running = queen.queue('orders').group(GROUP).batch(2).each().autoAck(false)
        .consume(async () => { ac.abort() }, { signal: ac.signal })

      assert.equal(await settleWithin(running, 3000), 'resolved', 'consume() still resolves')
      assert.ok(settledStatuses(broker.acks()).includes('tx-2:retry'), 'the release was attempted')
    } finally {
      await queen.close()
      await broker.stop()
    }
  })
})

/**
 * A server that sends the headers and half a JSON body, then stalls. Each
 * request is counted; `closeAll` ends the stalled bodies.
 */
async function startStallingServer() {
  let hits = 0
  const open = []
  const server = createServer((req, res) => {
    hits++
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.write('{"value":')
    open.push(res)
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    hits: () => hits,
    async stop() {
      for (const res of open) if (!res.destroyed) res.destroy()
      server.closeAllConnections()
      await new Promise(resolve => server.close(resolve))
    }
  }
}

/** Resolves once fetch has handed back the response headers for a URL with `prefix`. */
function onResponseHeaders(prefix) {
  const original = globalThis.fetch
  let ready
  const promise = new Promise(resolve => { ready = resolve })
  globalThis.fetch = async (...args) => {
    const response = await original(...args)
    if (String(args[0]).startsWith(prefix)) setImmediate(ready)
    return response
  }
  return { ready: promise, restore() { globalThis.fetch = original } }
}

describe('HttpClient — a response whose body is still being read', () => {
  it('an abort during the body read is a caller abort: no node is marked unhealthy', async () => {
    const a = await startStallingServer()
    const b = await startStallingServer()
    const lb = new LoadBalancer([a.url, b.url], 'affinity')
    const http = new HttpClient({ loadBalancer: lb, enableFailover: true, retryAttempts: 1 })
    const headers = onResponseHeaders('http://127.0.0.1:')
    try {
      const ac = new AbortController()
      const settled = http.get('/status', 10000, 'key', null, ac.signal).then(() => null, e => e)
      await headers.ready

      ac.abort()

      const error = await settled
      assert.equal(error?.aborted, true, 'the rejection says the caller aborted it')
      assert.equal(a.hits() + b.hits(), 1, 'no other node was tried')
      for (const [url, status] of lb.getHealthStatus()) {
        assert.equal(status.healthy, true, `${url} is still healthy`)
      }
    } finally {
      headers.restore()
      await http.destroy()
      await a.stop()
      await b.stop()
    }
  })

  it('the request timeout also bounds the body read', async () => {
    const server = await startStallingServer()
    const http = new HttpClient({ baseUrl: server.url, retryAttempts: 1 })
    try {
      const started = Date.now()
      const outcome = await settleWithin(http.get('/status', 300), 3000)
      assert.match(outcome, /^rejected: Request timeout/, 'a stalled body ends as a timeout, not a hang')
      assert.ok(Date.now() - started < 2000)
    } finally {
      await http.destroy()
      await server.stop()
    }
  })
})
