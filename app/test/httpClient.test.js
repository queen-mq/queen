import test from 'node:test'
import assert from 'node:assert/strict'

import { ApiError, describeApiError } from '../src/api/errors.js'
import { createApiClient } from '../src/api/httpClient.js'

const jsonResponse = (body, init = {}) => new Response(JSON.stringify(body), {
  ...init,
  headers: {
    'content-type': 'application/json',
    ...init.headers,
  },
})

test('HTTP client scopes requests, serializes params, and preserves the response facade', async () => {
  let sent
  let successes = 0
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test/',
    getActingClusterHeader: () => ({ name: 'x-queen-act-cluster', value: 'cluster-a' }),
    reportApiSuccess: () => { successes += 1 },
    fetch: async request => {
      sent = request
      return jsonResponse({ queues: ['one'] })
    },
  })

  const response = await client.get('/api/v1/resources/queues', {
    params: { limit: 50, offset: 0 },
  })

  assert.equal(sent.url, 'https://queen.test/api/v1/resources/queues?limit=50&offset=0')
  assert.equal(sent.headers.get('x-queen-act-cluster'), 'cluster-a')
  assert.equal(sent.credentials, 'include')
  assert.deepEqual(response.data, { queues: ['one'] })
  assert.equal(response.status, 200)
  assert.equal(response.config.url, '/api/v1/resources/queues')
  assert.equal(successes, 1)
})

test('HTTP client sends mutation bodies as JSON', async () => {
  let sentBody
  let sentContentType
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    fetch: async request => {
      sentBody = await request.clone().json()
      sentContentType = request.headers.get('content-type')
      return jsonResponse({ success: true }, { status: 201 })
    },
  })

  const response = await client.post('/api/v1/push', { queue: 'jobs', value: 1 })

  assert.deepEqual(sentBody, { queue: 'jobs', value: 1 })
  assert.match(sentContentType, /^application\/json/)
  assert.equal(response.status, 201)
  assert.deepEqual(response.data, { success: true })
})

test('HTTP client does not retry a failed mutation and preserves the API error envelope', async () => {
  let attempts = 0
  let reported
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    reportApiFailure: error => { reported = error },
    fetch: async () => {
      attempts += 1
      return jsonResponse(
        { code: 'overloaded', error: 'Try later' },
        { status: 503, headers: { 'retry-after': '7' } },
      )
    },
  })

  await assert.rejects(
    client.delete('/api/v1/dlq'),
    error => {
      assert.ok(error instanceof ApiError)
      assert.equal(error.message, 'Try later')
      assert.equal(error.status, 503)
      assert.equal(error.code, 'overloaded')
      assert.equal(error.retryAfter, 7)
      assert.equal(error.path, '/api/v1/dlq')
      return true
    },
  )

  assert.equal(attempts, 1)
  assert.equal(reported?.status, 503)
})

test('HTTP client rejects an API route that falls through to HTML', async () => {
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    fetch: async () => new Response('<!doctype html>', {
      status: 200,
      headers: { 'content-type': 'text/html' },
    }),
  })

  await assert.rejects(
    client.get('/api/v1/not-a-route'),
    error => error instanceof ApiError &&
      error.code === 'not_an_api_response' &&
      error.status === 200,
  )
})

test('HTTP client leaves caller aborts silent', async () => {
  let failures = 0
  const controller = new AbortController()
  controller.abort()
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    reportApiFailure: () => { failures += 1 },
    fetch: async request => {
      if (request.signal.aborted) {
        throw new DOMException('The operation was aborted', 'AbortError')
      }
      throw new Error('expected an aborted request')
    },
  })

  await assert.rejects(
    client.get('/api/v1/messages', { signal: controller.signal }),
    error => error.name === 'AbortError',
  )
  assert.equal(failures, 0)
})

test('HTTP client returns the Prometheus surface as text', async () => {
  let sent
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    getActingClusterHeader: () => ({ name: 'x-queen-act-cluster', value: 'cluster-a' }),
    fetch: async request => {
      sent = request
      return new Response('queen_up 1\n', {
        headers: { 'content-type': 'text/plain' },
      })
    },
  })

  const response = await client.get('/metrics/prometheus', { responseType: 'text' })
  assert.equal(response.data, 'queen_up 1\n')
  assert.equal(sent.headers.has('x-queen-act-cluster'), false)
})

test('HTTP client converts network failures into offline API errors', async () => {
  let reported
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    reportApiFailure: error => { reported = error },
    fetch: async () => { throw new TypeError('fetch failed') },
  })

  await assert.rejects(
    client.get('/api/v1/resources/overview'),
    error => error instanceof ApiError &&
      error.status === 0 &&
      error.isOffline &&
      error.path === '/api/v1/resources/overview',
  )
  assert.equal(reported?.isOffline, true)
})

test('HTTP client redirects once on 401 without settling the caller', async () => {
  let redirects = 0
  let failures = 0
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    redirectToLogin: () => { redirects += 1 },
    reportApiFailure: () => { failures += 1 },
    fetch: async () => jsonResponse(
      { code: 'unauthorized', error: 'Sign in' },
      { status: 401 },
    ),
  })

  const request = client.get('/api/v1/resources/overview')
  const outcome = await Promise.race([
    request.then(() => 'settled', () => 'settled'),
    new Promise(resolve => setTimeout(() => resolve('pending'), 10)),
  ])

  assert.equal(outcome, 'pending')
  assert.equal(redirects, 1)
  assert.equal(failures, 0)
})

test('HTTP client keeps a probe\'s missing-route answers off the global surface', async () => {
  let failures = 0
  let status = 404
  let html = false
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    reportApiFailure: () => { failures += 1 },
    fetch: async () => (html
      ? new Response('<!doctype html>', { status: 200, headers: { 'content-type': 'text/html' } })
      : jsonResponse({ error: 'Not Found' }, { status })),
  })

  // The stable "no such route here" answers are a probe's expected outcome.
  await assert.rejects(
    client.get('/api/v1/analytics/workload', { probe: true }),
    error => error instanceof ApiError && error.status === 404,
  )
  html = true
  await assert.rejects(
    client.get('/api/v1/analytics/workload', { probe: true }),
    error => error instanceof ApiError && error.code === 'not_an_api_response',
  )
  html = false
  assert.equal(failures, 0)

  // Anything else on a probe is still a failure worth surfacing.
  status = 503
  await assert.rejects(client.get('/api/v1/analytics/workload', { probe: true }), error => error.status === 503)
  assert.equal(failures, 1)

  // And the same 404 without the flag is reported as it always was.
  status = 404
  await assert.rejects(client.get('/api/v1/analytics/workload'), error => error.status === 404)
  assert.equal(failures, 2)
})

// ---------------------------------------------------------------------------
// What a 403 is actually about
// ---------------------------------------------------------------------------

// Built through the client, from the envelopes the two components really send,
// because the code does not always arrive in the same field: the proxy names it
// in `code` (proxy/src/errors.rs json_error) and the broker's own handlers put
// it in `error` with no `code` at all. A table of hand-made `{code}` objects
// would pass while every broker-minted refusal fell through to the role
// sentence in production.
const refusal = async (body, status) => {
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    fetch: async () => jsonResponse(body, { status }),
  })
  try {
    await client.post('/api/v1/configure', { queue: 'orders' })
  } catch (err) {
    return err
  }
  throw new Error('the call was expected to fail')
}

test('a 403 is worded by its CODE, and only a real one blames the role', async () => {
  // THE OBSERVED BUG. As an ADMIN, a configure over the plan's retention
  // ceiling came back 403 quota_exceeded with the cap in the message, and the
  // toast said the role was wrong — advice that sends the operator to ask for a
  // permission they already hold. The proxy's own sentence names the cap, so it
  // is repeated verbatim after the subject it was missing.
  const ceiling = await refusal({
    code: 'quota_exceeded',
    error: "retentionSeconds of 1209600s exceeds the plan's max_retention_seconds (604800s)",
  }, 403)
  assert.equal(
    describeApiError(ceiling),
    'The plan on this cluster refused it: retentionSeconds of 1209600s exceeds the plan\'s max_retention_seconds (604800s)',
  )
  assert.doesNotMatch(describeApiError(ceiling), /role/)

  // The other two limit codes, same shape: a block nothing about the principal
  // can lift, and the server's sentence saying which cap it is.
  const storage = await refusal({ code: 'storage_quota_exceeded', error: 'storage quota exceeded; pushes blocked' }, 403)
  assert.match(describeApiError(storage), /^The plan on this cluster refused it: storage quota exceeded/)
  assert.doesNotMatch(describeApiError(storage), /role/)

  const hold = await refusal({ code: 'push_blocked', error: 'pushes blocked (billing hold)' }, 403)
  assert.match(describeApiError(hold), /^The plan on this cluster refused it: pushes blocked \(billing hold\)/)

  // A limit code with no sentence to repeat names no cap it was not told about.
  const bare = await refusal({ code: 'quota_exceeded' }, 403)
  assert.equal(describeApiError(bare), 'The plan on this cluster refused it')

  // feature_gated, from BOTH envelopes: the proxy's plan gate, and the broker's
  // own Verdict::NotGranted (no queen.kv_quota row grants the family).
  const gatedByProxy = await refusal({ code: 'feature_gated', error: 'not in your plan' }, 403)
  assert.equal(describeApiError(gatedByProxy), 'Not in this cluster\u2019s plan')
  const gatedByBroker = await refusal({ error: 'feature_gated', reason: 'feature_gated' }, 403)
  assert.equal(gatedByBroker.code, null, 'the broker sends no `code` field')
  assert.equal(describeApiError(gatedByBroker), 'Not in this cluster\u2019s plan')

  const suspended = await refusal({ code: 'cluster_suspended', error: 'cluster suspended' }, 403)
  assert.equal(describeApiError(suspended), 'This cluster is suspended')

  // And the two answers that ARE about the principal keep the role sentence:
  // the proxy's `forbidden` (acting.rs, auth.rs, console.rs) and a 403 that
  // carried no code at all.
  const forbidden = await refusal({ code: 'forbidden', error: 'not permitted' }, 403)
  assert.equal(describeApiError(forbidden), 'Not permitted for your role on this cluster')
  const noCode = await refusal({ error: 'forbidden' }, 403)
  assert.equal(describeApiError(noCode), 'Not permitted for your role on this cluster')
})

test('a 503 that names a gated family is the family, not a broken cell', async () => {
  // The pages with a quiet card for it never reach `describeApiError`; the ones
  // that report inline (views/Ephemeral.vue's banner) would otherwise say
  // "Server error (HTTP 503)" about a cell whose durable queues are healthy.
  const paused = await refusal({ error: 'kv_disabled', reason: 'switch' }, 503)
  assert.equal(describeApiError(paused), 'Not being served on this cell right now')

  // A 503 that names nothing is still what it always was.
  const broken = await refusal({ error: 'boom' }, 503)
  assert.equal(describeApiError(broken), 'Server error (HTTP 503)')
})

test('a caller that renders the gated verdicts itself gets no toast on top', async () => {
  // The KV page with the cell's kv switch off showed its quiet panel AND a red
  // toast: two reports of one fact, the second implying something broke. Same
  // rule as the missing-route answers above — `probe: true` is the caller
  // saying it renders these, and what it renders is a state, not a failure.
  let failures = 0
  let body = { code: 'feature_gated', error: 'not in your plan' }
  let status = 403
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    reportApiFailure: () => { failures += 1 },
    fetch: async () => jsonResponse(body, { status }),
  })
  const ask = (config) => client.get('/api/v1/resources/kv/namespaces', config)

  await assert.rejects(ask({ probe: true }), (err) => err.status === 403)
  assert.equal(failures, 0)

  // The switch codes, from the ladder and from each handler's own
  // `unavailable()` — every one of them a 503 the page answers with one card.
  status = 503
  for (const code of [
    'kv_disabled', 'timers_disabled', 'ephemeral_disabled',
    'kv_unavailable', 'timers_unavailable', 'ephemeral_unavailable',
  ]) {
    body = { error: code, reason: 'switch' }
    await assert.rejects(ask({ probe: true }), (err) => err.status === 503)
  }
  assert.equal(failures, 0, 'no switch code reaches the global surface on a probe')

  // A 503 that is NOT one of them is a broken cell, and must still be reported.
  body = { error: 'boom' }
  await assert.rejects(ask({ probe: true }), (err) => err.status === 503)
  assert.equal(failures, 1)

  // So must a 429 — a plan rate limit is a failure the page cannot explain
  // away, and swallowing it would leave stale rows with nothing saying why.
  status = 429
  body = { code: 'rate_limited', error: 'slow down' }
  await assert.rejects(ask({ probe: true }), (err) => err.status === 429)
  assert.equal(failures, 2)

  // And without the flag the gated answers are reported exactly as before: a
  // call whose caller renders nothing must not go silent.
  status = 403
  body = { code: 'feature_gated', error: 'not in your plan' }
  await assert.rejects(ask(), (err) => err.status === 403)
  assert.equal(failures, 3)
})
