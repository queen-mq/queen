import { test } from 'node:test'
import assert from 'node:assert/strict'
import { groupLabel, queueAlerts } from '../src/composables/queueAlerts.js'

const queue = (pending, extra = {}) => ({ name: 'orders', messages: { pending }, ...extra })
const group = (name, maxTimeLag, extra = {}) => ({ name, queueName: 'orders', state: 'Stable', maxTimeLag, ...extra })

test('a queue holding messages with no reader has one alert, worked on in Consumer groups', () => {
  const [a, ...rest] = queueAlerts(queue(1200), [])
  assert.equal(rest.length, 0)
  assert.equal(a.sev, 'warn')
  assert.equal(a.tab, 'consumers')
  assert.match(a.detail, /1,200 pending/)
  // Nothing pending, nothing to say; and Settings can say no reader is expected.
  assert.deepEqual(queueAlerts(queue(0), []), [])
  assert.deepEqual(queueAlerts(queue(5), [], () => ({ noReaderOk: true })), [])
})

test('groups behind are listed worst first; other queues and healthy groups are not', () => {
  const groups = [
    group('fast', 2),
    group('slow', 120),
    group('stuck', 3600),
    { ...group('elsewhere', 9999), queueName: 'billing' },
  ]
  const alerts = queueAlerts(queue(50), groups)
  assert.deepEqual(alerts.map((a) => [a.sev, a.title]), [
    ['bad', 'stuck is falling behind'],
    ['warn', 'slow is behind'],
  ])
  assert.match(alerts[0].detail, /1h old/)
})

test('queue mode is named for a person, and a nameless row says nothing', () => {
  assert.equal(groupLabel({ name: '__QUEUE_MODE__' }), 'Queue mode')
  assert.equal(groupLabel({ name: 'workers' }), 'workers')
  assert.deepEqual(queueAlerts(null, [group('x', 9999)]), [])
})
