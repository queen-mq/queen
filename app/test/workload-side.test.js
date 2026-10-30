// The Workload list: the tenant, then every namespace (or task) by name, one
// line each with the worst queueAttention mark of its queues and their
// pending. Queues are not listed: a click is the page's drill-down, on the
// right.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { GROUP_BY, groupName, rootName, workloadList } from '../src/composables/workloadSide.js'

const q = (name, namespace, task, pending = 0, more = {}) => ({ name, namespace, task, messages: { pending }, ...more })
const g = (queueName, maxTimeLag) => ({ name: `${queueName}-g`, queueName, maxTimeLag, state: 'Stable' })
const queues = [
  q('shop.orders', 'shop', 'billing', 5),
  q('shop.mail', 'shop', 'notify', 0),
  q('ops.audit', 'ops', 'billing', 7),
  q('loose', '', '', 3),
]

test('the tenant, then every namespace by name, the empty one last, each with its pending', () => {
  const t = workloadList({ queues, groups: [] })
  assert.deepEqual(t.root, { label: 'All namespaces', on: true, pending: 15 })
  assert.deepEqual(t.rows.map((n) => [n.key, n.label, n.count, n.pending, n.on]), [
    ['ops', 'ops', 1, 7, false],
    ['shop', 'shop', 2, 5, false],
    ['', '(no namespace)', 1, 3, false],
  ])
  assert.equal(t.groups, 3)
  assert.equal(t.queues, 4)
  // One line each: no queue rows under a group.
  assert.ok(t.rows.every((n) => !('queues' in n)))
})

test('a group carries the worst mark of its queues, and says how many need you', () => {
  const t = workloadList({ queues, groups: [g('shop.mail', 400), g('shop.orders', 2)], focus: 'shop' })
  const shop = t.rows.find((n) => n.key === 'shop')
  assert.equal(shop.on, true)
  assert.equal(t.root.on, false)
  assert.equal(shop.sev, 'bad')
  assert.equal(shop.why, '1 queue needs attention')
  // ops.audit has pending and no group reading it.
  assert.equal(t.rows.find((n) => n.key === 'ops').sev, 'warn')
})

test('the order is by name whatever the marks, so a refresh never moves a row', () => {
  const calm = workloadList({ queues, groups: [g('shop.mail', 1), g('shop.orders', 1), g('ops.audit', 1), g('loose', 1)] })
  const loud = workloadList({ queues, groups: [g('shop.mail', 900)] })
  assert.deepEqual(calm.rows.map((n) => n.key), loud.rows.map((n) => n.key))
})

test('by task: the tasks are the groups', () => {
  const t = workloadList({ queues, groups: [], groupBy: 'task', focus: 'billing' })
  assert.equal(t.root.label, 'All tasks')
  assert.deepEqual(t.rows.map((n) => [n.label, n.count, n.on]), [['billing', 2, true], ['notify', 1, false], ['(no task)', 1, false]])
})

test('no consumer groups, no marks: pending alone is not "unread"', () => {
  const t = workloadList({ queues, groups: null })
  assert.ok(t.rows.every((n) => n.sev === null && n.why === ''))
  // With the groups read and none attached, messages nobody reads are amber.
  const read = workloadList({ queues, groups: [] })
  assert.equal(read.rows.find((n) => n.key === 'shop').sev, 'warn')
})

test('keys the list does not know, and the one drilled into, are still groups', () => {
  const t = workloadList({ queues: [], keys: ['quiet'], focus: 'gone' })
  assert.deepEqual(t.rows.map((n) => [n.key, n.count, n.pending]), [['gone', 0, null], ['quiet', 0, null]])
  assert.equal(t.root.pending, null)
})

test('the search keeps a group it names, or one holding a queue it names', () => {
  assert.deepEqual(workloadList({ queues, groups: [], search: 'AUDIT' }).rows.map((n) => n.key), ['ops'])
  assert.deepEqual(workloadList({ queues, groups: [], search: 'shop' }).rows.map((n) => n.key), ['shop'])
  assert.deepEqual(workloadList({ queues, groups: [], search: 'no namespace' }).rows.map((n) => n.key), [''])
  // The counts stay the whole tenant's.
  const none = workloadList({ queues, groups: [], search: 'nothing-like-it' })
  assert.deepEqual([none.rows.length, none.groups, none.queues], [0, 3, 4])
})

test('names for the empty key, the "all" row and the Group by choices', () => {
  assert.equal(groupName('', 'namespace'), '(no namespace)')
  assert.equal(groupName('', 'task'), '(no task)')
  assert.equal(groupName('x', 'task'), 'x')
  assert.equal(rootName('task'), 'All tasks')
  assert.equal(rootName('namespace'), 'All namespaces')
  assert.deepEqual(GROUP_BY.map((o) => o.value), ['namespace', 'task'])
})
