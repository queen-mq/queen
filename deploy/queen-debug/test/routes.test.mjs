import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'

import { createBrokerCall } from '../src/broker.mjs'
import { isAllowed, keyPath, segment, withQuery } from '../src/routes.mjs'

// The broker's own published table of every route and its access level.
const TABLE = readFileSync(new URL('../../../webdoc/src/content/partials/generated/broker-routes.mdx', import.meta.url), 'utf8')
const brokerRoutes = [...TABLE.matchAll(/^\| `([A-Z]+)` \| `([^`]+)` \| ([a-z-]+) \|/gm)].map(([, method, path, level]) => ({
  method,
  path,
  level,
  sample: path.replace(/:[a-zA-Z]+/g, 'x').replace(/\*[a-zA-Z]+/g, 'y/z'),
}))

// GET on these two falls to read-write only because the broker's list has no
// arm for them. They change nothing.
const RAFT_READS = new Set(['/api/v1/raft/status', '/api/v1/raft/members'])

test('the broker table was parsed', () => {
  assert.ok(brokerRoutes.length > 100, `only ${brokerRoutes.length} routes`)
  assert.ok(brokerRoutes.some((r) => r.path === '/api/v1/push'))
})

test('every route the broker does not call read-only is refused', () => {
  for (const r of brokerRoutes) {
    if (r.level === 'read-only' || r.level === 'public') continue
    if (r.method === 'GET' && RAFT_READS.has(r.path)) continue
    assert.equal(isAllowed(r.method, r.sample), false, `${r.method} ${r.path} (${r.level}) must be refused`)
  }
})

test('the calls that change state are refused by name', () => {
  for (const [method, path] of [
    ['POST', '/api/v1/push'],
    ['GET', '/api/v1/pop/queue/orders'],
    ['GET', '/api/v1/pop/queue/orders/partition/p'],
    ['POST', '/api/v1/ack'],
    ['POST', '/api/v1/ack/batch'],
    ['POST', '/api/v1/transaction'],
    ['POST', '/api/v1/lease/abc/extend'],
    ['POST', '/api/v1/configure'],
    ['POST', '/api/v1/kv'],
    ['PUT', '/api/v1/kv/ns/key'],
    ['DELETE', '/api/v1/kv/ns/key'],
    ['POST', '/api/v1/locks'],
    ['POST', '/api/v1/timers'],
    ['DELETE', '/api/v1/timers/q/key'],
    ['DELETE', '/api/v1/resources/queues/orders'],
    ['DELETE', '/api/v1/consumer-groups/billing'],
    ['DELETE', '/api/v1/dlq'],
    ['POST', '/api/v1/messages/p/t/retry'],
    ['POST', '/api/v1/connectors/c/resync'],
    ['PUT', '/api/v1/connectors/c'],
    ['DELETE', '/api/v1/connectors/c'],
    ['GET', '/api/v1/ephemeral/pop'],
    ['POST', '/api/v1/ephemeral/push'],
    ['GET', '/api/v1/system/raft/membership'],
    ['POST', '/api/v1/system/raft/membership/promote'],
    ['DELETE', '/api/v1/system/raft/membership/members/3'],
    ['POST', '/api/v1/system/link/promote'],
    ['POST', '/streams/v1/queries'],
    ['GET', '/internal/anything'],
  ]) {
    assert.equal(isAllowed(method, path), false, `${method} ${path}`)
  }
})

test('every route this server reads is one the broker calls read-only or public', () => {
  const candidates = brokerRoutes.filter((r) => isAllowed(r.method, r.sample))
  assert.ok(candidates.length > 30)
  for (const r of candidates) {
    const fine = r.level === 'read-only' || r.level === 'public' || (r.method === 'GET' && RAFT_READS.has(r.path))
    assert.ok(fine, `${r.method} ${r.path} is ${r.level}`)
  }
})

test('the probe exists only when turned on, and only on its own key', () => {
  assert.equal(isAllowed('PUT', '/api/v1/kv/queen-debug/probe'), false)
  assert.equal(isAllowed('PUT', '/api/v1/kv/queen-debug/probe', { allowProbe: true }), true)
  assert.equal(isAllowed('PUT', '/api/v1/kv/queen-debug/other', { allowProbe: true }), false)
  assert.equal(isAllowed('PUT', '/api/v1/kv/orders/probe', { allowProbe: true }), false)
  assert.equal(isAllowed('DELETE', '/api/v1/kv/queen-debug/probe', { allowProbe: true }), false)
})

test('a path cannot climb out of its route', () => {
  for (const p of ['/api/v1/kv/ns/../../system/x', '/api/v1/consumer-groups/..', '/api/v1/kv/ns/%2e%2e/x', '/api/v1/kv/ns/.%2E/x']) {
    assert.equal(isAllowed('GET', p), false, p)
  }
  for (const bad of ['..', '.', '', 'a\u0000b', 'a\nb']) assert.throws(() => segment(bad), RangeError, JSON.stringify(bad))
  assert.equal(segment('a/b'), 'a%2Fb')
  assert.equal(keyPath('customer/1'), 'customer/1')
  assert.throws(() => keyPath('a/../b'), RangeError)
})

test('the last check holds whatever a tool builds', async () => {
  const sent = []
  const call = createBrokerCall({ fetchImpl: async (u) => { sent.push(String(u)); return new Response('{}') } })
  const node = { name: 'n', url: 'http://127.0.0.1:1' }
  for (const [method, target] of [
    ['POST', '/api/v1/push'],
    ['GET', '/api/v1/consumer-groups/../system/raft/membership'],
    ['GET', '/api/v1/kv/a/%2e%2e/%2e%2e/system/raft/membership'],
    ['GET', '//evil.test/health'],
    ['GET', 'http://evil.test/health'],
    ['PUT', '/api/v1/kv/queen-debug/probe'],
  ]) {
    await assert.rejects(call(node, method, target), (e) => e.kind === 'refused', `${method} ${target}`)
  }
  assert.deepEqual(sent, [])
})

test('a redirect is not followed', async () => {
  let init
  const call = createBrokerCall({ fetchImpl: async (_u, i) => { init = i; return new Response(null, { status: 302, headers: { location: 'http://evil.test/' } }) } })
  await assert.rejects(call({ name: 'n', url: 'http://127.0.0.1:1' }, 'GET', '/health'), (e) => e.status === 302)
  assert.equal(init.redirect, 'manual')
})

test('withQuery leaves out what was not set', () => {
  assert.equal(withQuery('/x', { a: 1, b: undefined, c: null, d: '' }), '/x?a=1')
  assert.equal(withQuery('/x'), '/x')
})
