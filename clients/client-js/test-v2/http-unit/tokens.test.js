/**
 * Client tokens on the JS client: `bearerToken` as a function (TokenSource),
 * refreshed before a client token expires and after a 401, and
 * `queen.tokens.mint()` (POST /api/v1/tokens).
 *
 * Against a real local HTTP server that plays back a canned plan, the style
 * of retry429.test.js: no broker, no mocking framework.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen, TokenSource, expiresAtMs } from '../../client-v2/index.js'
import { HttpClient } from '../../client-v2/http/HttpClient.js'

/** A client token whose `exp` is `secondsFromNow` away (signature fake). */
function clientToken (secondsFromNow, sub = 'dev1') {
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url')
  const exp = Math.floor(Date.now() / 1000) + secondsFromNow
  return `qt_${b64({ alg: 'HS256' })}.${b64({ sub, exp, typ: 'client' })}.sig`
}

function withPlanServer (plan, run) {
  const hits = []
  let index = 0
  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      hits.push({ method: req.method, url: req.url, auth: req.headers.authorization || null, body: raw })
      const d = index < plan.length ? plan[index] : { status: 200, body: { ok: true } }
      index++
      res.writeHead(d.status ?? 200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(d.body ?? { ok: true }))
    })
  })
  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      const teardown = () => new Promise(r => server.close(r))
      Promise.resolve()
        .then(() => run(`http://127.0.0.1:${port}`, hits))
        .then(
          async (v) => { await teardown(); resolve(v) },
          async (e) => { await teardown(); reject(e) }
        )
    })
  })
}

const expired = { status: 401, body: { error: 'client token expired', code: 'token_expired' } }

describe('TokenSource', () => {
  it('answers a fixed credential as it is, and nothing without one', async () => {
    assert.equal(await new TokenSource('qk_live_x').get(), 'qk_live_x')
    assert.equal(new TokenSource('qk_live_x').refreshable, false)
    assert.equal(await new TokenSource(null).get(), null)
    assert.equal(new TokenSource('').configured, false)
  })

  it('calls the function once, and again only when the token nears its exp', async () => {
    let calls = 0
    const long = new TokenSource(() => { calls++; return clientToken(3600) })
    await long.get(); await long.get(); await long.get()
    assert.equal(calls, 1, 'a token with an hour left is kept')

    calls = 0
    const short = new TokenSource(() => { calls++; return clientToken(10) })
    await short.get(); await short.get()
    assert.equal(calls, 2, 'a token 10 s from its exp is fetched again')
  })

  it('refreshes once for many requests refused with the same token', async () => {
    let calls = 0
    const tokens = new TokenSource(async () => {
      calls++
      await new Promise(r => setTimeout(r, 10))
      return clientToken(3600, `dev${calls}`)
    })
    const first = await tokens.get()
    const again = await Promise.all([1, 2, 3, 4].map(() => tokens.get({ failed: first })))
    assert.equal(calls, 2, 'one refresh for four 401s')
    assert.ok(again.every(t => t === again[0] && t !== first))
    // A request refused with a token already replaced takes the new one.
    assert.equal(await tokens.get({ failed: first }), again[0])
    assert.equal(calls, 2)
  })

  it('refuses a function that answers no token', async () => {
    await assert.rejects(new TokenSource(() => '').get(), /non-empty string/)
  })

  it('reads a client token expiry and nothing else', () => {
    const t = clientToken(100)
    const at = expiresAtMs(t)
    assert.ok(at > Date.now() + 90_000 && at <= Date.now() + 100_000)
    assert.equal(expiresAtMs('qk_live_abc'), null)
    assert.equal(expiresAtMs('qt_not.a.jwt'), null)
    assert.equal(expiresAtMs('qt_garbage'), null)
  })
})

describe('HttpClient — a token function', () => {
  it('fetches a new token after a 401 and sends the request once more', async () => {
    let n = 0
    const tokens = [clientToken(3600, 'old'), clientToken(3600, 'new')]
    await withPlanServer([expired, { status: 200, body: { ok: 1 } }], async (url, hits) => {
      const http = new HttpClient({ baseUrl: url, bearerToken: () => tokens[n++] })
      const out = await http.get('/api/v1/resources/queues')
      assert.deepEqual(out, { ok: 1 })
      assert.equal(hits.length, 2)
      assert.equal(hits[0].auth, `Bearer ${tokens[0]}`)
      assert.equal(hits[1].auth, `Bearer ${tokens[1]}`, 'the retry carries the new token')
    })
  })

  it('gives a second 401 to the caller instead of looping', async () => {
    let n = 0
    await withPlanServer([expired, expired, expired], async (url, hits) => {
      const http = new HttpClient({ baseUrl: url, bearerToken: () => clientToken(3600, `t${n++}`) })
      await assert.rejects(http.get('/api/v1/resources/queues'), (e) => e.status === 401 && e.code === 'token_expired')
      assert.equal(hits.length, 2, 'one refresh, one retry')
    })
  })

  it('does not retry a 401 with a fixed credential', async () => {
    await withPlanServer([expired], async (url, hits) => {
      const http = new HttpClient({ baseUrl: url, bearerToken: 'qk_live_static' })
      await assert.rejects(http.get('/api/v1/resources/queues'), (e) => e.status === 401)
      assert.equal(hits.length, 1)
      assert.equal(hits[0].auth, 'Bearer qk_live_static')
    })
  })
})

describe('queen.tokens.mint', () => {
  it('posts the entity and the options it was given', async () => {
    const answer = { token: clientToken(60), sub: 'dev-8f3a', key: 'k', expiresAt: 'x', expiresIn: 60 }
    await withPlanServer([{ status: 201, body: answer }], async (url, hits) => {
      const queen = new Queen({ url, bearerToken: 'qk_live_role', handleSignals: false })
      const out = await queen.tokens.mint({ sub: 'dev-8f3a', ttlSeconds: 60 })
      assert.deepEqual(out, answer)
      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/tokens')
      assert.deepEqual(JSON.parse(hits[0].body), { sub: 'dev-8f3a', ttlSeconds: 60 })
      assert.equal(hits[0].auth, 'Bearer qk_live_role')
      await assert.rejects(queen.tokens.mint({}), /sub is required/)
      await queen.close()
    })
  })

  it('shares one token function across the client', async () => {
    let calls = 0
    await withPlanServer([], async (url) => {
      const queen = new Queen({ url, bearerToken: () => { calls++; return clientToken(3600) }, handleSignals: false })
      await queen.tokens.mint({ sub: 'a' })
      await queen.tokens.mint({ sub: 'b' })
      await queen.tokens.mint({ sub: 'c' })
      assert.equal(calls, 1)
      await queen.close()
    })
  })
})
