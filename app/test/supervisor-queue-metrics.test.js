import { test } from 'node:test'
import assert from 'node:assert/strict'
import { supervisorQueueMetrics } from '../src/composables/supervisorQueueMetrics.js'
const NOW = Date.parse('2026-10-06T12:00:30Z')
const traffic = (time, overrides = {}) => ({ bucket: `2026-10-06T${time}Z`, queueName: 'orders', pushPerSecond: 8, popPerSecond: 10, ackFailed: 2, ...overrides })
const backlog = (time, pending) => ({ bucket: `2026-10-06T${time}Z`, pending })

test('rates use the latest complete bucket and backlog keeps its actual sample timestamps', () => {
  const result = supervisorQueueMetrics({ bucketMinutes: 1, series: [traffic('12:00:00', { ackFailed: 900 }), traffic('11:58:00'), traffic('11:59:00', { popPerSecond: 12 })],
    backlog: [backlog('12:00:00', 12), backlog('11:05:00', 40), backlog('11:06:00', 30)] }, 'orders', NOW)
  assert.equal(result.pop, 12)
  assert.equal(result.push, 8)
  assert.equal(result.ackFailures, 4)
  assert.equal(result.pendingDelta, -28)
  assert.equal(result.pending, 12)
  assert.deepEqual(result.points.map(point => point.pending), [40, 30, 12])
  assert.equal(result.points[2].at - result.points[1].at, 54 * 60_000)
})

test('empty, missing and partial data remain unknown instead of healthy zeroes', () => {
  const empty = supervisorQueueMetrics({ series: [], backlog: [] }, 'orders', NOW)
  for (const field of ['push', 'pop', 'ackFailures', 'pending', 'pendingDelta']) assert.equal(empty[field], null)
  const partial = supervisorQueueMetrics({ series: [traffic('11:58:00', { ackFailed: undefined, popPerSecond: null })], backlog: [backlog('11:58:00', 3)] }, 'orders', NOW)
  assert.equal(partial.ackFailures, null)
  assert.equal(partial.pop, null)
  assert.equal(partial.pendingDelta, null)
  assert.equal(partial.pending, 3)
  const zero = supervisorQueueMetrics({ series: [traffic('11:58:00', { ackFailed: 0, popPerSecond: 0 })] }, 'orders', NOW)
  assert.equal(zero.ackFailures, 0)
  assert.equal(zero.pop, 0)
})

test('the response must be scoped to the requested queue and contain unambiguous time buckets', () => {
  for (const series of [[traffic('11:58:00', { queueName: 'another-queue' })], [traffic('11:58:00', { queueName: undefined })], [traffic('11:58:00'), traffic('11:58:00')], [{ bucket: 'invalid' }]]) {
    assert.throws(() => supervisorQueueMetrics({ series }, 'orders', NOW), /Inconsistent/)
  }
  assert.throws(() => supervisorQueueMetrics({ series: [], backlog: null }, 'orders', NOW), /Invalid/)
  assert.equal(supervisorQueueMetrics({ series: [traffic('10:58:00')] }, 'orders', NOW).push, null)
})
