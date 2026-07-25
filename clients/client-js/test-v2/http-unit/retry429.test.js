/**
 * Client-side 429/403 handling tests (PLAN_QUEEN_PROXY_CLOUD.md §4/§9,
 * blocker B4 -- "client 429/backoff work ... mandatory pre-enforcement").
 *
 * The proxy error contract under test:
 *   429  Retry-After: <seconds>  { "error": "...", "code": "rate_limited" | "quota_exceeded" }
 *   403                          { "error": "...", "code": "cluster_suspended" | "storage_quota_exceeded"
 *                                                          | "feature_gated" | "forbidden" }
 *
 * These tests run against a real local HTTP server (no broker) that plays
 * back a canned response script, mirroring the style of ack.test.js
 * (withAckServer) and streams-unit/fakeServer.js -- no mocking framework,
 * just node:http.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen } from '../../client-v2/index.js'
import { HttpClient } from '../../client-v2/http/HttpClient.js'

/**
 * Spin up a real HTTP server driven by a canned response `plan` (array of
 * {status, body, retryAfter, headers}, consumed in request order; any
 * request beyond the plan gets `defaultResponse`). Records each hit's
 * arrival time so tests can sanity-check backoff pacing.
 */
function withPlanServer(plan, defaultResponse, run) {
  const hits = []
  const start = Date.now()
  let index = 0

  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      hits.push({ method: req.method, url: req.url, atMs: Date.now() - start })

      const descriptor = index < plan.length ? plan[index] : defaultResponse
      index++

      const status = descriptor.status ?? 200
      const headers = { 'Content-Type': 'application/json', ...(descriptor.headers || {}) }
      if (descriptor.retryAfter != null) headers['Retry-After'] = String(descriptor.retryAfter)

      res.writeHead(status, headers)
      res.end(JSON.stringify(descriptor.body ?? { ok: true }))
    })
  })

  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      const teardown = () => new Promise(r => server.close(r))
      Promise.resolve()
        .then(() => run(`http://127.0.0.1:${port}`, hits))
        .then(
          async (value) => { await teardown(); resolve(value) },
          async (err) => { await teardown(); reject(err) }
        )
    })
  })
}

function rateLimited(retryAfter) {
  const descriptor = { status: 429, body: { error: 'slow down', code: 'rate_limited' } }
  if (retryAfter != null) descriptor.retryAfter = retryAfter
  return descriptor
}

function repeat(descriptor, n) {
  return Array.from({ length: n }, () => ({ ...descriptor }))
}

// ---------------------------------------------------------------------------
// HttpClient (direct) -- the centralized retry429 mechanism itself.
// ---------------------------------------------------------------------------

describe('HttpClient — 429 retry policy', () => {
  it('honors Retry-After (seconds) and succeeds once the header-specified wait elapses', async () => {
    const plan = [rateLimited('0')]
    await withPlanServer(plan, { status: 200, body: { ok: true } }, async (url, hits) => {
      const client = new HttpClient({ baseUrl: url, retry429: { baseMs: 5, capMs: 50 } })
      try {
        const result = await client.get('/x')
        assert.deepEqual(result, { ok: true })
        assert.equal(hits.length, 2, 'one 429 then one success')
      } finally {
        await client.destroy()
      }
    })
  })

  it('falls back to exponential backoff when Retry-After is absent, and the gap grows', async () => {
    const plan = repeat(rateLimited(), 2)
    await withPlanServer(plan, { status: 200, body: { ok: true } }, async (url, hits) => {
      const client = new HttpClient({ baseUrl: url, retry429: { baseMs: 20, capMs: 2000 } })
      try {
        const result = await client.get('/x')
        assert.deepEqual(result, { ok: true })
        assert.equal(hits.length, 3)
        const gap1 = hits[1].atMs - hits[0].atMs
        const gap2 = hits[2].atMs - hits[1].atMs
        // Exponential (base=20ms -> ~20ms then ~40ms), jittered +-20%: allow
        // generous slack for CI timer jitter while still proving growth.
        assert.ok(gap2 > gap1 * 1.2, `expected gap2 (${gap2}ms) to exceed gap1 (${gap1}ms) by backoff growth`)
      } finally {
        await client.destroy()
      }
    })
  })

  it('gives up after retry429.maxAttempts on a default (push-like) call and throws rate_limited', async () => {
    await withPlanServer([], rateLimited(), async (url, hits) => {
      const client = new HttpClient({ baseUrl: url, retry429: { maxAttempts: 3, baseMs: 1, capMs: 5 } })
      try {
        await assert.rejects(
          client.post('/api/v1/push', { items: [] }),
          (err) => {
            assert.equal(err.status, 429)
            assert.equal(err.code, 'rate_limited')
            return true
          }
        )
        assert.equal(hits.length, 3, 'exactly maxAttempts tries, no more')
      } finally {
        await client.destroy()
      }
    })
  })

  it('defaults to 10 attempts for a default (non-pop) call when retry429 is not configured', async () => {
    await withPlanServer([], rateLimited(), async (url, hits) => {
      const client = new HttpClient({ baseUrl: url, retry429: { baseMs: 1, capMs: 2 } })
      try {
        await assert.rejects(client.post('/api/v1/push', { items: [] }))
        assert.equal(hits.length, 10)
      } finally {
        await client.destroy()
      }
    })
  })

  it('retries a long-poll pop (retryKind "pop") past the push default of 10 attempts', async () => {
    const plan = repeat(rateLimited('0'), 14)
    await withPlanServer(plan, { status: 200, body: { messages: [{ id: 'm1' }] } }, async (url, hits) => {
      const client = new HttpClient({ baseUrl: url, retry429: { baseMs: 1, capMs: 5 } })
      try {
        const result = await client.get('/api/v1/pop', null, null, 'pop')
        assert.deepEqual(result, { messages: [{ id: 'm1' }] })
        assert.equal(hits.length, 15, 'pop must not give up at the push default of 10 attempts')
      } finally {
        await client.destroy()
      }
    })
  })

  it('an explicit maxAttempts override applies to pop too (not just push)', async () => {
    await withPlanServer([], rateLimited(), async (url, hits) => {
      const client = new HttpClient({ baseUrl: url, retry429: { maxAttempts: 2, baseMs: 1, capMs: 5 } })
      try {
        await assert.rejects(client.get('/api/v1/pop', null, null, 'pop'))
        assert.equal(hits.length, 2, 'explicit maxAttempts bounds pop as well as push')
      } finally {
        await client.destroy()
      }
    })
  })

  it('never retries a 403 and preserves its code (terminal)', async () => {
    await withPlanServer([], { status: 403, body: { error: 'cluster suspended', code: 'cluster_suspended' } }, async (url, hits) => {
      const client = new HttpClient({ baseUrl: url })
      try {
        await assert.rejects(
          client.post('/api/v1/push', { items: [] }),
          (err) => {
            assert.equal(err.status, 403)
            assert.equal(err.code, 'cluster_suspended')
            return true
          }
        )
        assert.equal(hits.length, 1, '403 must not be retried')
      } finally {
        await client.destroy()
      }
    })
  })

  it('still does not retry an ordinary 4xx like 400 (unaffected by the 429 changes)', async () => {
    await withPlanServer([], { status: 400, body: { error: 'bad request' } }, async (url, hits) => {
      const client = new HttpClient({ baseUrl: url })
      try {
        await assert.rejects(client.get('/x'), (err) => {
          assert.equal(err.status, 400)
          return true
        })
        assert.equal(hits.length, 1)
      } finally {
        await client.destroy()
      }
    })
  })
})

// ---------------------------------------------------------------------------
// Queen public API wiring: config plumbing (retry429) + call-site marking
// (push vs. wait=true pop) actually reach HttpClient.
// ---------------------------------------------------------------------------

describe('Queen — push()/pop() wired to retry429', () => {
  it('push() retries a 429 (Retry-After) then succeeds', async () => {
    const plan = [rateLimited('0')]
    await withPlanServer(plan, { status: 200, body: [{ status: 'queued', transactionId: 'tx-1' }] }, async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false, retry429: { baseMs: 5, capMs: 50 } })
      try {
        const result = await queen.queue('q1').push({ hello: 'world' })
        assert.equal(hits.length, 2)
        assert.equal(result[0].status, 'queued')
      } finally {
        await queen.close()
      }
    })
  })

  it('push() surfaces a terminal 403 without retrying', async () => {
    await withPlanServer([], { status: 403, body: { error: 'over quota', code: 'storage_quota_exceeded' } }, async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        await assert.rejects(
          async () => { await queen.queue('q1').push({ hello: 'world' }) },
          (err) => { assert.equal(err.code, 'storage_quota_exceeded'); return true }
        )
        assert.equal(hits.length, 1)
      } finally {
        await queen.close()
      }
    })
  })

  it('a long-poll pop() rides out more 429s than the push default before returning', async () => {
    const plan = repeat(rateLimited('0'), 12)
    await withPlanServer(plan, { status: 200, body: { messages: [{ transactionId: 'tx-1', data: { x: 1 } }] } }, async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false, retry429: { baseMs: 1, capMs: 5 } })
      try {
        const messages = await queen.queue('q1').wait(true).pop()
        assert.equal(messages.length, 1)
        assert.equal(hits.length, 13)
      } finally {
        await queen.close()
      }
    })
  })

  it('a non-waiting pop() uses the bounded default and swallows an exhausted 429 to []', async () => {
    await withPlanServer([], rateLimited(), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false, retry429: { maxAttempts: 2, baseMs: 1, capMs: 5 } })
      try {
        const messages = await queen.queue('q1').wait(false).pop()
        assert.deepEqual(messages, [], 'pop() keeps its swallow-errors-to-[] contract')
        assert.equal(hits.length, 2)
      } finally {
        await queen.close()
      }
    })
  })
})

// ---------------------------------------------------------------------------
// ConsumerManager worker loop: the actual hot-loop/die bug (B4) this task
// fixes -- consume() must back off through 429s and stop cleanly on a
// terminal 403 instead of spinning or crashing uncontrolled.
// ---------------------------------------------------------------------------

describe('Queen — consume() loop backs off on 429 and stops on terminal 403', () => {
  it('backs off through repeated 429s and still delivers the message', async () => {
    const plan = repeat(rateLimited('0'), 3)
    await withPlanServer(
      plan,
      { status: 200, body: { messages: [{ transactionId: 'tx-1', partitionId: 'p-1', data: { x: 1 } }] } },
      async (url, hits) => {
        const queen = new Queen({ url, handleSignals: false, retry429: { baseMs: 1, capMs: 5 } })
        try {
          const received = []
          // .each() delivers one message per handler call (default batches
          // the whole array) -- simplest shape to assert on here.
          await queen.queue('q1').wait(true).limit(1).autoAck(false).each().consume(async (msg) => {
            received.push(msg)
          })
          assert.equal(received.length, 1)
          assert.equal(received[0].transactionId, 'tx-1')
          assert.ok(hits.length >= 4, 'the 3 rate-limited attempts plus the final success must all have hit the server')
        } finally {
          await queen.close()
        }
      }
    )
  })

  it('stops the worker on a terminal 403 (cluster_suspended) instead of hot-looping or hanging', async () => {
    await withPlanServer([], { status: 403, body: { error: 'cluster suspended', code: 'cluster_suspended' } }, async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        await assert.rejects(
          async () => { await queen.queue('q1').wait(true).consume(async () => {}) },
          (err) => { assert.equal(err.code, 'cluster_suspended'); return true }
        )
        assert.equal(hits.length, 1, 'must stop after the first 403, not hot-loop')
      } finally {
        await queen.close()
      }
    })
  })
})
