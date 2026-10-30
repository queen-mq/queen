// The Consumer groups tab: which groups it shows for the queue picked in the
// list (else the list's namespace), its search and its "Lagging only" box, the
// order it draws them in, and the words a verdict is said in. Every verdict is
// useAttention's.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  groupCount, groupLabel, isBehind, sortGroups, verdictOf, visibleGroups, visiblePartitions, SORTS, VERDICT,
} from '../src/composables/consumerGroups.js'

// The tenant's default lines: 1 min is behind, 5 min is stuck.
const g = (queueName, name, maxTimeLag, state = 'Stable', more = {}) => ({ name, queueName, maxTimeLag, state, ...more })
const kafka = (queueName, name, maxTimeLag, inactiveSeconds) => ({
  name, queueName, maxTimeLag, kind: 'kafka',
  state: inactiveSeconds === 0 ? 'Stable' : 'Empty',
  liveMembers: inactiveSeconds === 0 ? 3 : 0,
  lastSeenAt: inactiveSeconds === 0 ? null : '2026-10-19T09:00:00.000Z',
  inactiveSeconds,
})
const names = (rows) => rows.map((r) => `${r.queueName}/${r.name}`)

test('a picked queue shows only its groups; no queue shows every group', () => {
  const groups = [g('orders', 'a', 1), g('mail', 'b', 1), g('orders', '__QUEUE_MODE__', 0)]
  assert.deepEqual(names(visibleGroups(groups, { queue: 'orders' })), ['orders/a', 'orders/__QUEUE_MODE__'])
  assert.deepEqual(names(visibleGroups(groups)), ['orders/a', 'mail/b', 'orders/__QUEUE_MODE__'])
})

test('with no queue picked the list namespace scopes the groups; null is every namespace', () => {
  const meta = new Map([['orders', { name: 'orders', namespace: 'shop' }], ['mail', { name: 'mail', namespace: '' }]])
  const metaOf = (q) => meta.get(q)
  const groups = [g('orders', 'a', 1), g('mail', 'b', 1), g('ghost', 'c', 1)]
  assert.deepEqual(names(visibleGroups(groups, { namespace: 'shop', metaOf })), ['orders/a'])
  // '' is the default namespace, a real one; a queue the listing does not know is in none.
  assert.deepEqual(names(visibleGroups(groups, { namespace: '', metaOf })), ['mail/b'])
  assert.equal(visibleGroups(groups, { namespace: null, metaOf }).length, 3)
  // A picked queue wins over the namespace: it is the scope.
  assert.deepEqual(names(visibleGroups(groups, { queue: 'mail', namespace: 'shop', metaOf })), ['mail/b'])
})

test('the search finds a group by its name or its queue, and queue mode by its words', () => {
  const groups = [g('orders', 'billing', 1), g('mail', 'sender', 1), g('audit', '__QUEUE_MODE__', 0)]
  assert.deepEqual(names(visibleGroups(groups, { search: 'BILL' })), ['orders/billing'])
  assert.deepEqual(names(visibleGroups(groups, { search: 'mail' })), ['mail/sender'])
  assert.deepEqual(names(visibleGroups(groups, { search: 'queue mode' })), ['audit/__QUEUE_MODE__'])
  assert.equal(visibleGroups(groups, { search: '   ' }).length, 3)
})

test('"Lagging only" keeps the groups past their lag lines, not every group with a second of lag', () => {
  const groups = [
    g('q', 'fresh', 2),
    g('q', 'late', 120),
    g('q', 'stuck', 900),
    g('q', 'never', 90_000, 'Dead'),
    kafka('q', 'gone', 90_000, 7_200),
  ]
  assert.deepEqual(names(visibleGroups(groups, { laggingOnly: true })), ['q/late', 'q/stuck'])
  assert.equal(isBehind(groups[0]), false)
  assert.equal(isBehind(groups[1]), true)
  // A queue's own lines move the verdict: 3 minutes is fine on a queue whose line is 10.
  const linesFor = () => ({ lagWarnSeconds: 600, lagBadSeconds: 1200, inactiveAfterSeconds: 900 })
  assert.deepEqual(names(visibleGroups(groups, { laggingOnly: true, linesFor })), ['q/stuck'])
})

test('worst first: stuck, lagging, caught up, never consumed, inactive; lag then name inside each', () => {
  const groups = [
    g('q', 'fine-b', 3),
    kafka('q', 'gone', 90_000, 7_200),
    g('q', 'never', 90_000, 'Dead'),
    g('q', 'late', 120),
    g('q', 'stuck', 900),
    g('q', 'fine-a', 3),
    g('q', 'later', 200),
  ]
  assert.deepEqual(sortGroups(groups).map((r) => r.name), ['stuck', 'later', 'late', 'fine-a', 'fine-b', 'never', 'gone'])
  assert.equal(groups[0].name, 'fine-b', 'the input is not reordered')
})

test('by time lag the inactive Kafka groups go last; by partitions and by name as said', () => {
  const groups = [g('q', 'b', 10, 'Stable', { members: 2 }), kafka('q', 'gone', 90_000, 7_200), g('q', 'a', 50, 'Stable', { members: 8 })]
  assert.deepEqual(sortGroups(groups, 'lag').map((r) => r.name), ['a', 'b', 'gone'])
  assert.deepEqual(sortGroups(groups, 'members').map((r) => r.name), ['a', 'b', 'gone'])
  assert.deepEqual(sortGroups(groups, 'name').map((r) => r.name), ['a', 'b', 'gone'])
  assert.deepEqual(SORTS.map((s) => s.value), ['health', 'lag', 'members', 'name'])
})

test('the words: queue mode by name, a verdict per state, a count that says when it is narrowed', () => {
  assert.equal(groupLabel({ name: '__QUEUE_MODE__', queueName: 'q' }), 'Queue mode')
  assert.equal(groupLabel({ name: 'billing' }), 'billing')
  assert.equal(groupLabel(null), '—')
  assert.equal(verdictOf(g('q', 'x', 900)).word, 'Stuck')
  assert.equal(verdictOf(g('q', 'x', 120)).word, 'Lagging')
  assert.equal(verdictOf(g('q', 'x', 0, 'Dead')).word, 'Never consumed')
  assert.equal(verdictOf(kafka('q', 'k', 0, 7_200)).word, 'Inactive')
  assert.equal(verdictOf(g('q', 'x', 2)).glyph, 'ok')
  assert.equal(VERDICT.empty.glyph, 'ok')
  assert.equal(groupCount(12, 12), '12 groups')
  assert.equal(groupCount(1, 1), '1 group')
  assert.equal(groupCount(3, 1200), '3 of 1,200 groups')
})

test('lagging partitions follow the same scope and search as the groups', () => {
  const meta = new Map([['orders', { name: 'orders', namespace: 'shop' }], ['mail', { name: 'mail' }]])
  const metaOf = (q) => meta.get(q)
  const p = (queue_name, consumer_group, partition_name) => ({ queue_name, consumer_group, partition_name })
  const rows = [p('orders', 'billing', 'a'), p('mail', '__QUEUE_MODE__', 'b'), p('orders', 'audit', 'c')]
  const parts = (out) => out.map((r) => r.partition_name)
  assert.deepEqual(parts(visiblePartitions(rows, { queue: 'orders' })), ['a', 'c'])
  assert.deepEqual(parts(visiblePartitions(rows, { namespace: 'shop', metaOf })), ['a', 'c'])
  // A queue with no namespace is in the default one, as in the queue list.
  assert.deepEqual(parts(visiblePartitions(rows, { namespace: '', metaOf })), ['b'])
  assert.deepEqual(parts(visiblePartitions(rows, { search: 'queue mode' })), ['b'])
  assert.deepEqual(parts(visiblePartitions(rows, { queue: 'orders', search: 'AUD' })), ['c'])
})

test('nothing in, nothing out', () => {
  assert.deepEqual(visibleGroups(null), [])
  assert.deepEqual(visibleGroups([null, undefined]), [])
  assert.deepEqual(sortGroups(null), [])
  assert.deepEqual(visiblePartitions(null), [])
})
