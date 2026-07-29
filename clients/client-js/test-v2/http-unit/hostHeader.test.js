/**
 * Host-routed proxy support: selecting a tenant cluster at a queen_proxy
 * deployment (PLAN_QUEEN_PROXY_CLOUD.md §2 -- the proxy resolves the cluster
 * from the Host header's first DNS label, queen_proxy/src/cache.rs
 * slug_from_host).
 *
 * The defect these tests lock down: `fetch()` silently drops a caller-supplied
 * `Host` header (WHATWG forbidden header name), so a JS user pointing at a
 * shared address either got 421 cluster_unknown or -- worse, on a cell with a
 * default cluster -- was routed to somebody ELSE's cluster with no signal.
 * `hostHeader` advertises the cluster host while the socket stays pinned to the
 * configured address, and a `headers: { Host }` is mapped onto it out loud.
 *
 * Same style as retry429.test.js: real node:http servers, no mocking framework.
 * Every assertion here is about what actually arrives on the wire (req.headers.host).
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen } from '../../client-v2/index.js'
import { HttpClient } from '../../client-v2/http/HttpClient.js'
import { LoadBalancer } from '../../client-v2/http/LoadBalancer.js'

/**
 * Spin up a real HTTP server that records the Host header (and the rest) of
 * every request it receives, then answers with `respond(hit)`.
 */
function withHostServer(run, respond = () => ({ status: 200, body: { ok: true } })) {
  const hits = []
  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      const hit = { method: req.method, url: req.url, host: req.headers.host, headers: req.headers }
      hits.push(hit)
      const descriptor = respond(hit)
      res.writeHead(descriptor.status ?? 200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(descriptor.body ?? { ok: true }))
    })
  })

  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      const teardown = () => new Promise(r => server.close(r))
      Promise.resolve()
        .then(() => run({ url: `http://127.0.0.1:${port}`, port, hits }))
        .then(
          async (value) => { await teardown(); resolve(value) },
          async (err) => { await teardown(); reject(err) }
        )
    })
  })
}

/** Capture console.warn for the duration of `fn` (the trap warning is not
 *  routed through the QUEEN_CLIENT_LOG-gated logger on purpose). */
async function captureWarnings(fn) {
  const original = console.warn
  const warnings = []
  console.warn = (...args) => { warnings.push(args.join(' ')) }
  try {
    return { value: await fn(), warnings }
  } finally {
    console.warn = original
  }
}

// ---------------------------------------------------------------------------
// The wire: what Host actually goes out.
// ---------------------------------------------------------------------------

describe('HttpClient — hostHeader on the wire', () => {
  it('sends the address as Host when no hostHeader is configured (unchanged default)', async () => {
    await withHostServer(async ({ url, port, hits }) => {
      const client = new HttpClient({ baseUrl: url })
      try {
        await client.get('/x')
        assert.equal(hits.length, 1)
        assert.equal(hits[0].host, `127.0.0.1:${port}`)
      } finally {
        await client.destroy()
      }
    })
  })

  it('sends the configured hostHeader verbatim while still connecting to baseUrl', async () => {
    await withHostServer(async ({ url, hits }) => {
      const client = new HttpClient({ baseUrl: url, hostHeader: 'acme.eu1.queenmq.cloud' })
      try {
        const result = await client.get('/x')
        assert.deepEqual(result, { ok: true }, 'the request must still reach the configured address')
        assert.equal(hits[0].host, 'acme.eu1.queenmq.cloud')
      } finally {
        await client.destroy()
      }
    })
  })

  it('advertises a bare slug (what queen_proxy turns into a cluster) without appending the real port', async () => {
    await withHostServer(async ({ url, hits }) => {
      const client = new HttpClient({ baseUrl: url, hostHeader: 'sdka' })
      try {
        await client.get('/x')
        assert.equal(hits[0].host, 'sdka', 'slug_from_host takes the first label — no :port noise')
      } finally {
        await client.destroy()
      }
    })
  })

  it('keeps an explicit port in the advertised Host, and never resolves that name in DNS', async () => {
    await withHostServer(async ({ url, hits }) => {
      // `acme.invalid:6711` cannot resolve and 6711 is not our listener: the
      // request can only arrive if the socket is pinned to baseUrl.
      const client = new HttpClient({ baseUrl: url, hostHeader: 'acme.invalid:6711' })
      try {
        await client.get('/x')
        assert.equal(hits[0].host, 'acme.invalid:6711')
      } finally {
        await client.destroy()
      }
    })
  })

  it('applies to every verb and keeps the path/body intact', async () => {
    await withHostServer(async ({ url, hits }) => {
      const client = new HttpClient({ baseUrl: url, hostHeader: 'acme' })
      try {
        await client.post('/api/v1/push', { items: [1] })
        await client.delete('/api/v1/queues/q1')
        assert.deepEqual(hits.map(h => h.host), ['acme', 'acme'])
        assert.deepEqual(hits.map(h => h.url), ['/api/v1/push', '/api/v1/queues/q1'])
      } finally {
        await client.destroy()
      }
    })
  })

  it('shares one pinned agent across concurrent first requests', async () => {
    await withHostServer(async ({ url, hits }) => {
      const client = new HttpClient({ baseUrl: url, hostHeader: 'acme' })
      try {
        // All five race the (async) agent creation; each must still get the
        // advertised Host, and destroy() must be able to release every socket.
        const results = await Promise.all(Array.from({ length: 5 }, () => client.get('/x')))
        assert.equal(results.length, 5)
        assert.equal(hits.length, 5)
        for (const hit of hits) assert.equal(hit.host, 'acme')
      } finally {
        await client.destroy()
      }
    })
  })

  it('leaves other custom headers (and the bearer token) untouched', async () => {
    await withHostServer(async ({ url, hits }) => {
      const client = new HttpClient({
        baseUrl: url,
        bearerToken: 'key-a',
        hostHeader: 'acme',
        headers: { 'X-Trace': 'abc' }
      })
      try {
        await client.get('/x')
        assert.equal(hits[0].host, 'acme')
        assert.equal(hits[0].headers['x-trace'], 'abc')
        assert.equal(hits[0].headers['authorization'], 'Bearer key-a')
      } finally {
        await client.destroy()
      }
    })
  })
})

// ---------------------------------------------------------------------------
// The trap: `headers: { Host }` must never be silently dropped.
// ---------------------------------------------------------------------------

describe('HttpClient — headers: { Host } is mapped, never silently ignored', () => {
  it('maps headers.Host onto hostHeader and warns on the console once', async () => {
    await withHostServer(async ({ url, hits }) => {
      const { warnings } = await captureWarnings(async () => {
        // Distinct value per test: the warn-once dedupe is keyed by value.
        const client = new HttpClient({ baseUrl: url, headers: { Host: 'mapped-a.example.test' } })
        try {
          await client.get('/x')
        } finally {
          await client.destroy()
        }
      })
      assert.equal(hits[0].host, 'mapped-a.example.test', 'the Host the caller asked for must reach the wire')
      assert.equal(warnings.length, 1, 'exactly one warning for the first occurrence')
      assert.match(warnings[0], /forbidden header name/)
      assert.match(warnings[0], /hostHeader/)
    })
  })

  it('warns only once for a repeated identical misconfiguration', async () => {
    await withHostServer(async ({ url }) => {
      const { warnings } = await captureWarnings(async () => {
        for (let i = 0; i < 3; i++) {
          const client = new HttpClient({ baseUrl: url, headers: { Host: 'mapped-b.example.test' } })
          await client.get('/x')
          await client.destroy()
        }
      })
      assert.equal(warnings.length, 1, 'one warning per distinct offending value, not per client')
    })
  })

  it('accepts a lower-case `host` key too (header names are case-insensitive)', async () => {
    await withHostServer(async ({ url, hits }) => {
      await captureWarnings(async () => {
        const client = new HttpClient({ baseUrl: url, headers: { host: 'mapped-c.example.test' } })
        try {
          await client.get('/x')
        } finally {
          await client.destroy()
        }
      })
      assert.equal(hits[0].host, 'mapped-c.example.test')
    })
  })

  it('lets an explicit hostHeader win over a conflicting headers.Host, and says so', async () => {
    await withHostServer(async ({ url, hits }) => {
      const { warnings } = await captureWarnings(async () => {
        const client = new HttpClient({
          baseUrl: url,
          hostHeader: 'explicit-d.example.test',
          headers: { Host: 'conflicting-d.example.test' }
        })
        try {
          await client.get('/x')
        } finally {
          await client.destroy()
        }
      })
      assert.equal(hits[0].host, 'explicit-d.example.test')
      assert.equal(warnings.length, 1)
      assert.match(warnings[0], /is ignored because hostHeader/)
    })
  })

  it('does not warn when headers.Host merely repeats hostHeader', async () => {
    await withHostServer(async ({ url, hits }) => {
      const { warnings } = await captureWarnings(async () => {
        const client = new HttpClient({
          baseUrl: url,
          hostHeader: 'same-e.example.test',
          headers: { Host: 'same-e.example.test' }
        })
        try {
          await client.get('/x')
        } finally {
          await client.destroy()
        }
      })
      assert.equal(hits[0].host, 'same-e.example.test')
      assert.equal(warnings.length, 0)
    })
  })
})

// ---------------------------------------------------------------------------
// Validation + failure modes: a bad or unhonorable Host must fail loudly.
// ---------------------------------------------------------------------------

describe('HttpClient — hostHeader validation', () => {
  it('rejects a URL-shaped hostHeader at construction', () => {
    assert.throws(
      () => new HttpClient({ baseUrl: 'http://127.0.0.1:1', hostHeader: 'http://acme.example.test' }),
      /bare authority/
    )
  })

  it('rejects a hostHeader with a path or whitespace', () => {
    assert.throws(() => new HttpClient({ baseUrl: 'http://127.0.0.1:1', hostHeader: 'acme/foo' }), /bare authority/)
    assert.throws(() => new HttpClient({ baseUrl: 'http://127.0.0.1:1', hostHeader: 'acme host' }), /bare authority/)
  })

  it('rejects an empty hostHeader', () => {
    assert.throws(() => new HttpClient({ baseUrl: 'http://127.0.0.1:1', hostHeader: '   ' }), /non-empty string/)
  })

  it('treats hostHeader: null as "no override" (default behaviour)', async () => {
    await withHostServer(async ({ url, port, hits }) => {
      const client = new HttpClient({ baseUrl: url, hostHeader: null })
      try {
        await client.get('/x')
        assert.equal(hits[0].host, `127.0.0.1:${port}`)
      } finally {
        await client.destroy()
      }
    })
  })

  it('refuses to send (instead of leaking the address as Host) once the client is destroyed', async () => {
    await withHostServer(async ({ url, hits }) => {
      const client = new HttpClient({ baseUrl: url, hostHeader: 'acme', retryAttempts: 1 })
      await client.get('/x')
      await client.destroy()
      await assert.rejects(client.get('/x'), /cannot be honored|Refusing to send/)
      assert.equal(hits.length, 1, 'the post-destroy request must not reach the server with the wrong Host')
    })
  })
})

// ---------------------------------------------------------------------------
// Load balancing: one virtual Host, several real backends.
// ---------------------------------------------------------------------------

describe('HttpClient — hostHeader with multiple backends', () => {
  it('advertises the same Host to every backend while dialing each one', async () => {
    await withHostServer(async ({ url: urlA, hits: hitsA }) => {
      await withHostServer(async ({ url: urlB, hits: hitsB }) => {
        const loadBalancer = new LoadBalancer([urlA, urlB], 'round-robin')
        const client = new HttpClient({ loadBalancer, hostHeader: 'acme.eu1.queenmq.cloud' })
        try {
          for (let i = 0; i < 4; i++) await client.get('/x')
          assert.ok(hitsA.length > 0 && hitsB.length > 0, `both backends must be dialed (${hitsA.length}/${hitsB.length})`)
          for (const hit of [...hitsA, ...hitsB]) {
            assert.equal(hit.host, 'acme.eu1.queenmq.cloud')
          }
        } finally {
          await client.destroy()
        }
      })
    })
  })

  it('keeps failover working: a 500 on one backend still lands on the other with the right Host', async () => {
    await withHostServer(
      async ({ url: urlA, hits: hitsA }) => {
        await withHostServer(async ({ url: urlB, hits: hitsB }) => {
          const loadBalancer = new LoadBalancer([urlA, urlB], 'round-robin')
          const client = new HttpClient({ loadBalancer, hostHeader: 'acme', retryAttempts: 1 })
          try {
            const result = await client.get('/x')
            assert.deepEqual(result, { ok: true })
            assert.ok(hitsA.length > 0, 'the failing backend was tried')
            assert.ok(hitsB.length > 0, 'and the request failed over to the healthy one')
            for (const hit of [...hitsA, ...hitsB]) assert.equal(hit.host, 'acme')
          } finally {
            await client.destroy()
          }
        })
      },
      () => ({ status: 500, body: { error: 'boom' } })
    )
  })
})

// ---------------------------------------------------------------------------
// Queen public API wiring.
// ---------------------------------------------------------------------------

describe('Queen — hostHeader config plumbing', () => {
  it('push() through a Queen configured with hostHeader carries it on the wire', async () => {
    await withHostServer(
      async ({ url, hits }) => {
        const queen = new Queen({ url, hostHeader: 'sdkb', bearerToken: 'key-b', handleSignals: false })
        try {
          const result = await queen.queue('q1').push({ hello: 'world' })
          assert.equal(result[0].status, 'queued')
          assert.equal(hits[0].host, 'sdkb')
          assert.equal(hits[0].headers['authorization'], 'Bearer key-b')
        } finally {
          await queen.close()
        }
      },
      () => ({ status: 200, body: [{ status: 'queued', transactionId: 'tx-1' }] })
    )
  })

  it('a Queen built with headers: { Host } routes to that cluster (Go/Python parity)', async () => {
    await withHostServer(
      async ({ url, hits }) => {
        const { value: result } = await captureWarnings(async () => {
          const queen = new Queen({ url, headers: { Host: 'parity-f.example.test' }, handleSignals: false })
          try {
            return await queen.queue('q1').push({ hello: 'world' })
          } finally {
            await queen.close()
          }
        })
        assert.equal(result[0].status, 'queued')
        assert.equal(hits[0].host, 'parity-f.example.test')
      },
      () => ({ status: 200, body: [{ status: 'queued', transactionId: 'tx-1' }] })
    )
  })

  it('a Queen without hostHeader is byte-for-byte unchanged (Host = the address)', async () => {
    await withHostServer(
      async ({ url, port, hits }) => {
        const queen = new Queen({ url, handleSignals: false })
        try {
          await queen.queue('q1').push({ hello: 'world' })
          assert.equal(hits[0].host, `127.0.0.1:${port}`)
        } finally {
          await queen.close()
        }
      },
      () => ({ status: 200, body: [{ status: 'queued', transactionId: 'tx-1' }] })
    )
  })
})
