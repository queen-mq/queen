import { test } from 'node:test'
import assert from 'node:assert/strict'
import { buildQueueTriage, filterQueueTriage, observePending } from '../src/composables/queueTriage.js'
import { queueAttention } from '../src/composables/useAttention.js'

const q = (name, pending, namespace = 'prod') => ({ name, namespace, messages: { pending } })
const g = (queueName, lag, state = 'Stable', name = `${queueName}-reader`) => ({ queueName, name, maxTimeLag: lag, state })

test('priority is severity then unread age, with the same verdict as the rest of the app', () => {
  const queues = [q('large', 1000000), q('slow', 3), q('stalled', 1), q('unread', 12), q('healthy', 999)]
  const groups = [g('large', 60), g('slow', 301), g('stalled', 900), g('healthy', 2)]
  const rows = buildQueueTriage(queues, groups)
  assert.deepEqual(rows.map(r => r.name), ['stalled', 'slow', 'large', 'unread', 'healthy'])
  for (const i of queueAttention(queues, groups)) assert.equal(rows.find(r => r.name === i.name).sev, i.sev)
  assert.equal(rows.at(-1).sev, null, 'a large pending count alone is not an alert')
})

test('evidence names the lagging groups and excludes groups that never read', () => {
  const rows = buildQueueTriage([q('a', 12), q('b', 7)], [g('a', 4), g('a', 301, 'Stable', 'slow'), g('a', 9000, 'Dead'), g('b', 8000, 'Dead')])
  assert.equal(rows[0].lag, 301)
  assert.deepEqual(rows[0].affected.map(g => g.name), ['slow'])
  assert.equal(rows[0].groupCount, 3)
  assert.equal(rows[1].deadOnly, true)
  assert.equal(rows[1].readerCount, 0)
  assert.equal(rows[1].lag, null)
})

test('zero waiting with in-flight work does not hide unconfirmed consumer lag', () => {
  const rows = buildQueueTriage([{ ...q('migration', 0), messages: { pending: 0, processing: 12 } }], [g('migration', 326)])
  assert.equal(rows[0].pending, 0)
  assert.equal(rows[0].processing, 12)
  assert.equal(rows[0].sev, 'bad')
  assert.equal(rows[0].affected[0].name, 'migration-reader')
  assert.equal(filterQueueTriage(rows).length, 1)
})

test('pending changes compare measured counts, including zero and decreases', () => {
  const first = observePending(null, [q('a', 10), q('b', 8)], 1000)
  assert.equal(first.delta.size, 0)
  const next = observePending(first.current, [q('a', 17), q('b', 0)], 31000)
  assert.deepEqual([...next.delta], [['a', 7], ['b', -8]])
  assert.equal(next.elapsed, 30000)
  const rows = buildQueueTriage([q('a', 17), q('b', 0)], [g('a', 0)], next.delta)
  assert.deepEqual(filterQueueTriage(rows, '', 'growing').map(r => r.name), ['a'])
  assert.equal(rows.find(r => r.name === 'a').sev, null, 'growth does not invent a severity rule')
})

test('missing, invalid, new or discontinuous measurements do not become zero changes', () => {
  const before = observePending(null, [q('missing', null), q('a', 10)], 1000)
  const next = observePending(before.current, [q('missing', 3), q('a', undefined), q('new', 20)], 31000)
  assert.equal(next.delta.size, 0)
  for (const at of [1000, 0, 121001]) assert.equal(observePending(before.current, [q('a', 22)], at).delta.size, 0)
  const rows = buildQueueTriage([q('bad', false), q('unknown', null)], [g('unknown', 90)])
  assert.equal(rows.find(r => r.name === 'unknown').pending, null)
  assert.equal(rows.find(r => r.name === 'bad').pending, null)
})

test('a reset starts a fresh comparison and cannot leak another scope', () => {
  const other = observePending(null, [q('same-name', 1000)], 1000)
  assert.equal(observePending(null, [q('same-name', 10)], 31000).delta.size, 0)
  assert.equal(other.current.counts.get('same-name'), 1000)
})

test('five thousand queues stay searchable by name and namespace, including the last queue', () => {
  const queues = Array.from({ length: 5000 }, (_, i) => q(`queue-${String(i).padStart(4, '0')}`, i, i % 2 ? 'Payments' : 'Events'))
  const rows = buildQueueTriage(queues, queues.map((q, i) => g(q.name, i % 100 === 0 ? 301 : 0)))
  assert.equal(filterQueueTriage(rows, '', 'attention').length, 50)
  assert.equal(filterQueueTriage(rows, ' payments ', 'all').length, 2500)
  assert.deepEqual(filterQueueTriage(rows, 'QUEUE-4999', 'all').map(r => r.name), ['queue-4999'])
  assert.equal(filterQueueTriage(rows, 'missing', 'all').length, 0)
})
