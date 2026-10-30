import { test } from 'node:test'
import assert from 'node:assert/strict'
import { QUEUE_TABS, consumerLocation, contextQuery, inNamespace, listCarry, namespaceCounts, queueLocation, queueMatches, queueOf, queueTabLocation, queueTabOf, readRouteValue, safeReturnTo, validWindow } from '../src/composables/navigation.js'

const source = { name: 'Queues', fullPath: '/queues?search=orders&namespace=', query: { search: 'orders', namespace: '' } }

test('queue journeys preserve the filtered origin and encode queue identities', () => {
  const detail = queueLocation('orders/#? café', source)
  assert.equal(detail.path, '/queues/orders%2F%23%3F%20caf%C3%A9')
  assert.equal(detail.query.returnTo, source.fullPath)
  const nested = queueLocation('orders/#? café', { ...detail, fullPath: detail.path }, 'failed')
  assert.equal(nested.path, '/dlq')
  assert.equal(nested.query.queue, 'orders/#? café')
  assert.equal(nested.query.returnTo, source.fullPath)
  assert.equal(queueLocation('orders', { name: 'QueueDetail', query: {}, fullPath: '/queues/orders' }, 'messages').query.returnTo, '/queues')
})

test('a consumer result identifies both group and queue, even for shared group names', () => {
  const a = consumerLocation({ name: 'workers', queueName: 'orders' }, source)
  const b = consumerLocation({ name: 'workers', queueName: 'billing' }, source)
  assert.equal(a.query.group, b.query.group)
  assert.notEqual(a.query.queue, b.query.queue)
  assert.equal(a.query.search, undefined)
})

test('only the investigation period and origin cross views; row filters do not leak', () => {
  const route = { ...source, query: { ...source.query, range: '24h', partition: 'p', status: 'failed', group: 'g', page: '8' } }
  assert.deepEqual(contextQuery(route), { range: '24h', returnTo: source.fullPath })
})

test('return links stay inside dashboard routes', () => {
  for (const path of ['https://example.org', '//example.org', '/auth/logout', '/\\example.org', '/unknown']) assert.equal(safeReturnTo(path), '')
  for (const path of ['/', '/queues?namespace=&search=a', '/supervisors?instance=x', '/messages?partitionId=p']) assert.equal(safeReturnTo(path), path)
})

test('query decoding distinguishes the default namespace and rejects malformed pages', () => {
  assert.equal(readRouteValue('', null), '')
  assert.equal(readRouteValue(undefined, null), null)
  for (const raw of ['-2', '1.5', 'NaN', 'Infinity', ['2']]) assert.equal(readRouteValue(raw, 1), 1)
  assert.equal(readRouteValue('2', 1), 2)
  assert.equal(validWindow('bad', '2026-10-09'), null)
  assert.equal(validWindow('2026-10-10', '2026-10-09'), null)
})

test('the Queues section: which page is which tab, with or without a picked queue', () => {
  const r = (name, meta, extra = {}) => ({ name, meta, params: {}, query: {}, ...extra })
  assert.equal(queueTabOf(r('Queues', { queueTab: 'overview' })), 'overview')
  assert.equal(queueTabOf(r('Messages', { queueTab: 'messages' })), 'messages')
  // Consumer groups is a tab only while a queue is picked.
  const groups = { queueTab: 'consumers', queueTabNeedsQueue: true }
  assert.equal(queueTabOf(r('Consumers', groups)), '')
  assert.equal(queueTabOf(r('Consumers', groups, { query: { queue: 'orders' } })), 'consumers')
  assert.equal(queueTabOf(r('Kv', {})), '')
  // The queue is in the path on its own pages, in ?queue= on its tabs.
  assert.equal(queueOf(r('QueueAlerts', {}, { params: { queueName: 'orders' } })), 'orders')
  assert.equal(queueOf(r('DeadLetter', {}, { query: { queue: 'billing' } })), 'billing')
})

test('a tab keeps the investigation window and drops the queue when none is picked', () => {
  const route = { name: 'Messages', meta: {}, params: {}, query: { queue: 'orders', range: '6h', status: 'failed' }, fullPath: '/messages?queue=orders' }
  assert.deepEqual(queueTabLocation('failed', '', route), { path: '/dlq', query: { range: '6h' } })
  // A tab that needs a queue falls back to the queue list's own page.
  assert.equal(queueTabLocation('scheduled', '', route).path, '/queues')
  assert.equal(queueTabLocation('alerts', 'a/b', route).path, '/queues/a%2Fb/alerts')
  assert.deepEqual(queueTabLocation('failed', 'orders', route), { path: '/dlq', query: { range: '6h', queue: 'orders' } })
  assert.deepEqual(QUEUE_TABS.map((t) => t.view), ['overview', 'messages', 'consumers', 'supervisors', 'failed', 'scheduled', 'alerts'])
})

test('the queue list and the Queues table match a term the same way', () => {
  const q = { name: 'orders', namespace: 'shop', task: 'billing' }
  for (const term of ['', '  ', 'ORD', 'sho', 'bill']) assert.equal(queueMatches(q, term), true)
  assert.equal(queueMatches(q, 'zzz'), false)
  assert.equal(queueMatches({ name: 'x' }, 'shop'), false)
})

test('the namespace picked in the queue list: every one, a named one, or the default one', () => {
  const shop = { name: 'orders', namespace: 'shop' }
  const plain = { name: 'jobs', namespace: '' }
  const bare = { name: 'legacy' }
  assert.equal(inNamespace(shop, null), true)
  assert.equal(inNamespace(shop, undefined), true)
  assert.equal(inNamespace(shop, 'shop'), true)
  assert.equal(inNamespace(shop, ''), false)
  // '' is the default namespace, and a queue that reports none is in it.
  assert.equal(inNamespace(plain, ''), true)
  assert.equal(inNamespace(bare, ''), true)
  assert.deepEqual(namespaceCounts([shop, plain, bare, { name: 'carts', namespace: 'shop' }, null]), [
    { name: '', count: 2, sev: null },
    { name: 'shop', count: 2, sev: null },
  ])
  // A namespace carries the worst mark of its queues.
  const sev = { orders: 'warn', carts: 'bad', jobs: null }
  assert.deepEqual(
    namespaceCounts([shop, plain, { name: 'carts', namespace: 'shop' }], (q) => sev[q.name]).map((n) => [n.name, n.sev]),
    [['', null], ['shop', 'bad']],
  )
})

test('the list filters ride along, and an empty namespace is kept as the default one', () => {
  assert.deepEqual(listCarry({ query: { find: 'ord', namespace: 'shop', status: 'x' } }), { find: 'ord', namespace: 'shop' })
  assert.deepEqual(listCarry({ query: { namespace: '' } }), { namespace: '' })
  assert.deepEqual(listCarry({ query: {} }), {})
})
