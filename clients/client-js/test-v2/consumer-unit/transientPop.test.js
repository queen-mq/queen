/**
 * consume() — a pop the broker could not answer for a while is polled again.
 *
 * The rule: a consumer stops only for a fault that waiting cannot fix (a 4xx
 * the broker answers the same way next time, a conflation it cannot apply, a
 * stop by its caller). What the broker, or the way to it, answers while it
 * cannot serve (the 503s of a leader election, a 500 from a node shutting
 * down, a gateway's 502 or 504, a timeout, a lost connection) is waited out,
 * paced by Retry-After, and the consumer goes on.
 *
 * Before: only a 429 and a refused connection were retried. On 2026-10-29 the
 * prod cluster lost its leader twice, pops were answered 503 "the request
 * deadline elapsed" for a few minutes, and every consumer of a service
 * stopped. Its process stayed up, so nothing restarted it, and hours of
 * messages piled up.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen } from '../../client-v2/index.js'
import { isTransientPopError, transientDelayMillis } from '../../client-v2/consumer/ConsumerManager.js'
import { configure as configureLogger } from '../../client-v2/utils/logger.js'

const GROUP = 'workers'

const message = (n) => ({
  id: `msg-${n}`,
  transactionId: `tx-${n}`,
  partitionId: '7',
  partition: 'p1',
  leaseId: 'lease-1',
  consumerGroup: GROUP,
  data: { n },
  createdAt: '2026-10-29T17:40:00.000Z'
})

// The broker's own answers while it has no leader (server/src/handlers/raft.rs).
const brokerBusy = (code, error, retryAfter = 0) => ({
  status: 503,
  retryAfter,
  body: JSON.stringify({ error, code })
})
const deadlineElapsed = (retryAfter = 0) => brokerBusy('timeout', 'the request deadline elapsed', retryAfter)
const noLeader = (retryAfter = 0) => brokerBusy('no_leader', 'no leader is known; retry after', retryAfter)

/**
 * A fake broker. Each pop takes the next step of `script`:
 * - `{ status, body, retryAfter }`: answered with that status;
 * - `{ messages }`: answered 200 with those messages;
 * - `'drop-mid-answer'`: the headers go out, the connection is cut halfway
 *   through the body;
 * - `'hold'`: never answered.
 * Once the script runs out, a pop is held open, like a long poll on an idle
 * queue. Acks are accepted.
 */
async function startBroker(script) {
  const steps = [...script]
  const requests = []
  const held = []
  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      const path = req.url.split('?')[0]
      const record = { method: req.method, path, body: raw ? JSON.parse(raw) : null, at: performance.now() }
      requests.push(record)
      if (req.method === 'GET' && path.startsWith('/api/v1/pop')) {
        const step = steps.shift()
        if (step === undefined || step === 'hold') {
          held.push(res)
          return
        }
        if (step === 'drop-mid-answer') {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': '4096' })
          res.write('{"success":true,"messages":[')
          setTimeout(() => res.socket.destroy(), 20)
          return
        }
        if (step.messages) {
          res.writeHead(200, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ success: true, consumerGroup: GROUP, messages: step.messages }))
          return
        }
        const headers = { 'Content-Type': step.contentType || 'application/json' }
        if (step.retryAfter != null) headers['Retry-After'] = String(step.retryAfter)
        res.writeHead(step.status, headers)
        res.end(step.body ?? '')
        return
      }
      if (req.method === 'POST' && (path === '/api/v1/ack' || path === '/api/v1/ack/batch')) {
        const acks = record.body.acknowledgments || [record.body]
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(acks.map((a, i) => ({ index: i, transactionId: a.transactionId, success: true, error: null, leaseReleased: true }))))
        return
      }
      res.writeHead(404, { 'Content-Type': 'application/json' })
      res.end('{"error":"not found"}')
    })
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  return {
    url: `http://127.0.0.1:${server.address().port}`,
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

/** Resolves with 'resolved', the rejection's error, or 'pending' if it took longer than `ms`. */
const settleWithin = (promise, ms) => Promise.race([
  Promise.resolve(promise).then(() => 'resolved', error => error),
  sleep(ms).then(() => 'pending')
])

async function until(condition, ms, what) {
  const deadline = performance.now() + ms
  while (!condition()) {
    if (performance.now() > deadline) assert.fail(`timed out after ${ms} ms waiting for ${what}`)
    await sleep(10)
  }
}

const settledStatuses = (acks) => acks.flatMap(a => (a.body.acknowledgments || [a.body]).map(x => `${x.transactionId}:${x.status}`))

/** A custom logger that keeps every structured line it is given, by level. */
function captureLogs() {
  const lines = { info: [], warn: [], error: [], debug: [] }
  const logger = Object.fromEntries(Object.keys(lines).map(level => [level, (fields) => { lines[level].push(fields) }]))
  return { logger, lines }
}

/**
 * A consumer on `broker` whose handler records the transaction ids it is
 * given. `stop()` aborts it and closes the client; it runs once, so a test's
 * `finally` can call it again to clean up after a failed assertion.
 */
function startConsumer(broker, { configure = q => q, queenOptions = {} } = {}) {
  const queen = new Queen({ url: broker.url, handleSignals: false, retryAttempts: 1, ...queenOptions })
  const handled = []
  const ac = new AbortController()
  const running = Promise.resolve(
    configure(queen.queue('orders').group(GROUP))
      .consume(async (messages) => { handled.push(...[messages].flat().map(m => m.transactionId)) }, { signal: ac.signal })
  )
  running.catch(() => {}) // a test that expects a rejection reads it through settleWithin
  let stopping = null
  return {
    handled,
    running,
    stop() {
      stopping ??= (async () => {
        ac.abort()
        const outcome = await settleWithin(running, 2000)
        await queen.close()
        return outcome
      })()
      return stopping
    }
  }
}

describe('consume() — waits out what the broker could not answer for a while', () => {
  it('a leader election: 503s, then a message — it is handled and the consumer goes on', async () => {
    const broker = await startBroker([deadlineElapsed(), deadlineElapsed(), deadlineElapsed(), { messages: [message(1)] }])
    const logs = captureLogs()
    const consumer = startConsumer(broker, { queenOptions: { logger: logs.logger, structuredLogs: true } })
    try {
      // 3 refused pops, the one that brings the message, then the next long poll.
      await until(() => broker.pops().length === 5, 3000, 'the consumer to poll again after the message')

      assert.deepEqual(consumer.handled, ['tx-1'])
      assert.deepEqual(settledStatuses(broker.acks()), ['tx-1:completed'])
      assert.equal(await settleWithin(consumer.running, 50), 'pending', 'consume() is still running')

      const retries = logs.lines.warn.filter(l => l.status === 'pop-failed-retrying')
      assert.deepEqual(retries.map(l => [l.httpStatus, l.code, l.failedPops]), [[503, 'timeout', 1], [503, 'timeout', 2], [503, 'timeout', 3]])
      const recovered = logs.lines.warn.filter(l => l.status === 'pop-recovered')
      assert.equal(recovered.length, 1, 'one line when the pops are answered again')
      assert.equal(recovered[0].failedPops, 3)
      assert.equal(await consumer.stop(), 'resolved')
    } finally {
      await consumer.stop()
      configureLogger(null)
      await broker.stop()
    }
  })

  it('waits the Retry-After the broker sent before the next pop', async () => {
    const broker = await startBroker([noLeader(1), { messages: [message(1)] }])
    const consumer = startConsumer(broker)
    try {
      await until(() => broker.acks().length === 1, 4000, 'the message to be acked')
      const [refused, served] = broker.pops()
      const gap = served.at - refused.at
      assert.ok(gap >= 750 && gap < 2000, `polled again after ${Math.round(gap)} ms, the Retry-After was 1 s`)
      assert.deepEqual(consumer.handled, ['tx-1'])
      assert.equal(await consumer.stop(), 'resolved')
    } finally {
      await consumer.stop()
      await broker.stop()
    }
  })

  it('a 500 from a node shutting down, and a gateway 502 that is not JSON', async () => {
    const broker = await startBroker([
      { status: 500, retryAfter: 0, body: JSON.stringify({ error: 'planner channel closed', code: 'internal' }) },
      { status: 502, retryAfter: 0, contentType: 'text/html', body: '<html><body>502 Bad Gateway</body></html>' },
      { messages: [message(1)] }
    ])
    const consumer = startConsumer(broker)
    try {
      await until(() => broker.acks().length === 1, 3000, 'the message to be acked')
      assert.deepEqual(consumer.handled, ['tx-1'])
      assert.equal(await consumer.stop(), 'resolved')
    } finally {
      await consumer.stop()
      await broker.stop()
    }
  })

  it('a connection cut while the answer was being read', async () => {
    const broker = await startBroker(['drop-mid-answer', { messages: [message(1)] }])
    const consumer = startConsumer(broker)
    try {
      await until(() => broker.acks().length === 1, 4000, 'the message to be acked')
      assert.deepEqual(consumer.handled, ['tx-1'])
      assert.equal(await consumer.stop(), 'resolved')
    } finally {
      await consumer.stop()
      await broker.stop()
    }
  })

  it('wait(false): a pop the client timed out', async () => {
    const broker = await startBroker(['hold', { messages: [message(1)] }])
    const consumer = startConsumer(broker, { configure: q => q.wait(false).timeoutMillis(200) })
    try {
      await until(() => broker.acks().length === 1, 4000, 'the message to be acked')
      assert.deepEqual(consumer.handled, ['tx-1'])
      assert.equal(await consumer.stop(), 'resolved')
    } finally {
      await consumer.stop()
      await broker.stop()
    }
  })

  it('a stop during the wait ends consume() at once', async () => {
    const broker = await startBroker([noLeader(30)])
    const consumer = startConsumer(broker)
    try {
      await until(() => broker.pops().length === 1, 2000, 'the first pop')
      await sleep(100) // the answer is read and the consumer is waiting
      const started = performance.now()
      const outcome = await consumer.stop()
      const elapsed = performance.now() - started
      assert.equal(outcome, 'resolved')
      assert.ok(elapsed < 250, `stopping took ${Math.round(elapsed)} ms, not the 30 s wait`)
      assert.equal(broker.pops().length, 1, 'no pop after the stop')
    } finally {
      await consumer.stop()
      await broker.stop()
    }
  })
})

describe('consume() — still stops on what waiting cannot fix', () => {
  const refusals = [
    { name: 'a 400 the broker refuses the same way next time', status: 400, body: { error: 'subscriptionFrom is not a timestamp', code: 'rejected' } },
    { name: 'a 503 raft_phase1_unsupported: the route is not served at all', status: 503, body: { error: 'raft phase 1: the message path is routed but not yet wired to the state machine', code: 'raft_phase1_unsupported' } }
  ]
  for (const refusal of refusals) {
    it(refusal.name, async () => {
      const broker = await startBroker([{ status: refusal.status, retryAfter: refusal.status === 503 ? 1 : null, body: JSON.stringify(refusal.body) }])
      const consumer = startConsumer(broker)
      try {
        const outcome = await settleWithin(consumer.running, 2000)
        assert.ok(outcome instanceof Error, `consume() rejects, got ${outcome}`)
        assert.equal(outcome.status, refusal.status)
        assert.equal(outcome.code, refusal.body.code)
        assert.equal(broker.pops().length, 1, 'not polled again')
        assert.deepEqual(consumer.handled, [])
      } finally {
        await consumer.stop()
        await broker.stop()
      }
    })
  }
})

describe('isTransientPopError', () => {
  const http = (status, code) => Object.assign(new Error(`HTTP ${status}`), { status, code })
  const named = (name, text, extra = {}) => Object.assign(new Error(text), { name, ...extra })

  it('is true for what waiting can fix', () => {
    const transient = {
      '503 timeout (the request deadline elapsed)': http(503, 'timeout'),
      '503 no_leader': http(503, 'no_leader'),
      '503 retry (not the leader)': http(503, 'retry'),
      '503 outcome_unknown': http(503, 'outcome_unknown'),
      '503 standby': http(503, 'standby'),
      '500 internal': http(500, 'internal'),
      '502 from a gateway': http(502),
      '504 from a gateway': http(504),
      '507 storage_full': http(507, 'storage_full'),
      '408 from a gateway': http(408),
      '429 the HttpClient stopped retrying': http(429, 'overloaded'),
      "this client's request timeout": named('AbortError', 'Request timeout after 35000ms', { timeout: 35000 }),
      'a refused connection': new TypeError('fetch failed (ECONNREFUSED)'),
      'a connection reset': new TypeError('fetch failed (ECONNRESET)'),
      'a connection cut mid-answer': new TypeError('terminated')
    }
    for (const [what, error] of Object.entries(transient)) assert.equal(isTransientPopError(error), true, what)
  })

  it('is false for what waiting cannot fix', () => {
    const terminal = {
      '400 rejected': http(400, 'rejected'),
      '401': http(401),
      '403 cluster_suspended': http(403, 'cluster_suspended'),
      '404': http(404),
      '413 name_too_long': http(413, 'name_too_long'),
      '503 raft_phase1_unsupported': http(503, 'raft_phase1_unsupported'),
      'a stop by the caller': named('AbortError', 'GET /api/v1/pop/queue/orders aborted by the caller', { aborted: true }),
      'a hostHeader this runtime cannot send': new Error("hostHeader 'acme:6711' cannot be honored here: no undici dispatcher available"),
      'a bug': new TypeError("Cannot read properties of undefined (reading 'messages')"),
      'nothing': null
    }
    for (const [what, error] of Object.entries(terminal)) assert.equal(isTransientPopError(error), false, what)
  })
})

describe('transientDelayMillis', () => {
  const middle = () => 0.5
  const lowest = () => 0
  const highest = () => 0.999999

  it("waits the broker's Retry-After, ±20 %, however many pops failed", () => {
    assert.equal(transientDelayMillis(1, 1, middle), 1000)
    assert.equal(transientDelayMillis(9, 1, middle), 1000)
    assert.equal(transientDelayMillis(1, 1, lowest), 800)
    assert.equal(transientDelayMillis(1, 1, highest), 1200)
  })

  it('without a Retry-After: 1 s, doubling with each failure, up to 30 s', () => {
    assert.deepEqual([1, 2, 3, 4, 5, 6, 7, 60].map(n => transientDelayMillis(n, null, middle)),
      [1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000])
  })

  it('never more than 30 s, whatever the Retry-After', () => {
    assert.equal(transientDelayMillis(1, 3600, highest), 30000)
    assert.equal(transientDelayMillis(1, 3600, lowest), 24000)
  })
})
