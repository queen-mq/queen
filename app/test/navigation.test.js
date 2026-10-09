import { test } from 'node:test'
import assert from 'node:assert/strict'
import { consumerLocation, contextQuery, queueLocation, readRouteValue, safeReturnTo, validWindow } from '../src/composables/navigation.js'

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
