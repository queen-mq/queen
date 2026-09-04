import test from 'node:test'
import assert from 'node:assert/strict'

import { ApiError } from '../src/api/errors.js'
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
