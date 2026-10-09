import { test } from 'node:test'
import assert from 'node:assert/strict'
import { supervisorActivity, createSupervisorActivityReader } from '../src/composables/supervisorActivity.js'
const NOW = Date.parse('2026-10-08T12:00:30Z')
const row = (time, extra = {}) => ({ bucket: `2026-10-08T${time}Z`, queueName: 'orders', pushPerSecond: 2, popPerSecond: 1, ackFailed: 0, ...extra })
const flush = () => new Promise(resolve => setImmediate(resolve))

test('activity plots regular minute buckets, preserves gaps and excludes both partial edges', () => {
  const result = supervisorActivity({ bucketMinutes: 1, series: [row('11:00:00'), row('11:02:00'), row('11:04:00'), row('11:59:00'), row('12:00:00', { ackFailed: 50 })], backlog: [{ bucket: '2026-10-08T11:02:00Z', pending: 0 }, { bucket: '2026-10-08T12:00:00Z', pending: 20 }] }, 'orders', NOW)
  assert.equal(result.points.length, 60)
  assert.equal(result.points[0].incoming, null)
  assert.equal(result.points[2].incoming, 120)
  assert.equal(result.points[3].incoming, null)
  assert.equal(result.points[4].delivered, 60)
  assert.equal(result.incoming, 120)
  assert.equal(result.delivered, 60)
  assert.equal(result.trafficSamples, 3)
  assert.equal(result.ackFailures, 0)
  assert.equal(result.points[2].pending, 0)
  assert.equal(result.backlogSamples, 1)
  assert.equal(result.pending, 20)
  assert.equal(result.pendingAt, Date.parse('2026-10-08T12:00:00Z'))
  assert.equal(result.pendingDelta, 20)
})

test('old traffic is not presented as a current rate and absence is distinct from measured zero', () => {
  const sparse = supervisorActivity({ series: [row('11:10:00')] }, 'orders', NOW)
  assert.equal(sparse.incoming, null)
  assert.equal(sparse.delivered, null)
  assert.equal(sparse.trafficSamples, 1)
  const empty = supervisorActivity({ series: [], backlog: [] }, 'orders', NOW)
  for (const key of ['incoming', 'delivered', 'pending', 'pendingAt', 'ackFailures']) assert.equal(empty[key], null)
  assert.equal(empty.trafficSamples, 0)
  const zero = supervisorActivity({ series: [row('11:59:00', { pushPerSecond: 0, popPerSecond: 0 })] }, 'orders', NOW)
  assert.equal(zero.incoming, 0)
  assert.equal(zero.delivered, 0)
  assert.equal(zero.trafficSamples, 1)
})

test('activity refuses mixed queues, duplicate buckets and invalid bucket widths', () => {
  for (const data of [{ series: [row('11:59:00', { queueName: 'other' })] }, { series: [row('11:59:00'), row('11:59:00')] }, { series: [], bucketMinutes: 0 }, { series: [], bucketMinutes: 7 }]) {
    assert.throws(() => supervisorActivity(data, 'orders', NOW))
  }
})

test('visible cards share requests per queue and concurrency stays bounded', async () => {
  const calls = [], resolves = []
  const reader = createSupervisorActivityReader(queue => { calls.push(queue); return new Promise(resolve => resolves.push(resolve)) }, 2)
  const a = reader.load('a'), same = reader.load('a'), b = reader.load('b'), c = reader.load('c')
  assert.equal(a, same)
  await flush()
  assert.deepEqual(calls, ['a', 'b'])
  resolves[0]('first'); await flush()
  assert.deepEqual(calls, ['a', 'b', 'c'])
  resolves[1]('second'); resolves[2]('third')
  assert.deepEqual(await Promise.all([a, b, c]), ['first', 'second', 'third'])
  assert.equal(reader.load('a'), a)
  reader.clear()
})

test('source switches abort active reads, discard queued reads and reject late old-cluster responses', async () => {
  const calls = [], pending = []
  const reader = createSupervisorActivityReader((queue, signal) => { calls.push(queue); return new Promise(resolve => pending.push({ signal, resolve })) }, 1)
  const a = reader.load('old-active'), b = reader.load('old-queued')
  const settled = Promise.allSettled([a, b])
  await flush()
  reader.clear()
  assert.equal(pending[0].signal.aborted, true)
  const fresh = reader.load('new')
  pending[0].resolve('obsolete'); await flush()
  assert.deepEqual(calls, ['old-active', 'new'])
  pending[1].resolve('current')
  assert.equal(await fresh, 'current')
  for (const result of await settled) { assert.equal(result.status, 'rejected'); assert.equal(result.reason.name, 'AbortError') }
})

test('a refresh before scheduled reads run never invokes the old source', async () => {
  const calls = []
  const reader = createSupervisorActivityReader(async queue => { calls.push(queue); return queue })
  const old = reader.load('old')
  const failure = assert.rejects(old, { name: 'AbortError' })
  reader.clear()
  const fresh = reader.load('fresh')
  await failure
  assert.equal(await fresh, 'fresh')
  assert.deepEqual(calls, ['fresh'])
})
