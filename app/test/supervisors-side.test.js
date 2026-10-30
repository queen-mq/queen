// The Supervisors page: its list of applications (by name, one line each,
// marked by the page's own verdict) and the pools it shows on the right, one
// row per pool across replicas, marked by the rules the page already applies
// to an instance and to a pool — stale, stopped, the pool's finding.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { STATUS_FORMAT, decodeSupervisorSlot, supervisorObservation, supervisorPart } from '../src/composables/supervisorStatus.js'
import { supervisorGroups } from '../src/composables/supervisorGroups.js'
import {
  POOL_FILTERS, applicationList, applicationPools, applicationVerdict, poolKey, poolTone, showOptions,
  supervisorApplications,
} from '../src/composables/supervisorsSide.js'

// The same publication fixtures as supervisor-status.test.js.
const NOW = 1_800_000_000
const ID = 'a'.repeat(32)
const WRITE = 'b'.repeat(32)
const SLOT = `orders-production/${ID}`
const pool = (overrides = {}) => ({ supervisor: 'main', queue: 'orders', running: 4, desired: 4, draining: 0,
  depth: 200, depth_available: true, ready: true, healthy: true, restart_state: 'closed', restart_failures: 0,
  process_cost_per_worker: 2, reserved_processes: 8, renewal_helpers_reserved: 4, ...overrides })
const document = (overrides = {}) => ({ schema: 'queen.supervisor.status/v1', instance_id: ID, hostname: 'worker-1', pid: 42,
  updated_at_epoch: NOW - 5, state: 'running', paused: false, stopping: false, engine: 'php', ready: true, capacity_satisfied: true,
  configuration: { process_limit: 32, heartbeat_timeout: 30 }, draining: 0,
  pool_status: [pool()], ...overrides })
function wire(doc, slot = SLOT) {
  const bytes = Buffer.from(JSON.stringify(doc))
  return [
    { key: `${slot}/head`, value: { format: STATUS_FORMAT, write: WRITE, chunks: 1, bytes: bytes.length }, expired: false },
    { key: `${slot}/chunk/0000`, value: { write: WRITE, index: 0, data: bytes.toString('base64') }, expired: false },
  ]
}
const observe = (overrides = {}, unconfirmed = false) => supervisorObservation(
  decodeSupervisorSlot(wire(document(overrides)), supervisorPart(`${SLOT}/head`)), NOW * 1000, unconfirmed)
const replica = (row, id, group = row.group) => ({ ...row, instance: id, slot: `${group}/${id}`, group, hostname: `worker-${id}` })

test('the list is every application by name, one line each, with its mark and instance count', () => {
  const groups = supervisorGroups([
    replica(observe(), 'a', 'zeta-production'),
    replica(observe(), 'b', 'zeta-production'),
    replica(observe({ updated_at_epoch: NOW - 31 }), 'c', 'late-production'),
    replica(observe({ state: 'stopped' }), 'd', 'gone-production'),
    replica(observe({ pool_status: [pool({ running: 0 })] }), 'e', 'alpha-production'),
  ])
  // supervisorGroups puts the ones that need you first; the list is by name.
  assert.deepEqual(applicationList(groups).map((a) => [a.name, a.mark, a.count]), [
    ['alpha-production', 'bad', 1],
    ['gone-production', 'idle', 1],
    ['late-production', 'warn', 1],
    ['zeta-production', null, 2],
  ])
  assert.equal(applicationList(groups).find((a) => a.name === 'late-production').why, 'Heartbeat overdue')
  assert.equal(applicationList(groups).find((a) => a.name === 'zeta-production').why, 'Reporting normally')
})

test('the list search names an application, a host or a pool, and keeps whole applications', () => {
  const groups = supervisorGroups([
    replica(observe({ pool_status: [pool(), pool({ supervisor: 'mailer', queue: 'mail' })] }), 'a'),
    replica(observe(), 'b', 'billing-production'),
  ])
  assert.deepEqual(applicationList(groups, { search: 'MAILER' }).map((a) => a.name), ['orders-production'])
  assert.deepEqual(applicationList(groups, { search: 'worker-b' }).map((a) => a.name), ['billing-production'])
  assert.deepEqual(applicationList(groups, { search: 'billing' }).map((a) => a.name), ['billing-production'])
  assert.deepEqual(applicationList(groups, { search: 'nothing-like-it' }), [])
  // In a queue's context, only the applications serving it.
  assert.deepEqual(applicationList(groups, { queue: 'mail' }).map((a) => a.name), ['orders-production'])
  assert.deepEqual(applicationList([]), [])
})

test('the mark is the page verdict without the partial-view overlay', () => {
  const healthy = replica(observe(), 'a')
  const [partial] = supervisorGroups([healthy], { partial: true })
  // The page's card says "Partial view"; the list does not mark every row for it.
  assert.equal(partial.tone, 'warn')
  assert.deepEqual(applicationVerdict(partial), { mark: null, why: 'Reporting normally' })
  // A paused instance beside a healthy one is a warning, as supervisorGroups says.
  const paused = replica(observe({ state: 'paused' }), 'b')
  const [mixed] = supervisorGroups([healthy, paused])
  assert.equal(mixed.tone, 'warn')
  assert.deepEqual(applicationVerdict(mixed), { mark: 'warn', why: 'Some instances are not running' })
  // Alone, a paused instance is not running: a grey ring, not an alarm.
  const [alone] = supervisorGroups([paused])
  assert.equal(applicationVerdict(alone).mark, 'idle')
  // A failed read leaves every instance unconfirmed: amber.
  const [unread] = supervisorGroups([replica(observe({}, true), 'c')])
  assert.equal(applicationVerdict(unread).mark, 'warn')
  assert.deepEqual(applicationVerdict({ rows: [] }), { mark: null, why: '' })
})

test('a pool is one row however many replicas report it, with their workers added up', () => {
  const [app] = supervisorGroups([replica(observe(), 'a'), replica(observe(), 'b')])
  const pools = applicationPools(app)
  assert.equal(pools.length, 1)
  assert.deepEqual(
    [pools[0].name, pools[0].queue, pools[0].tone, pools[0].mark, pools[0].running, pools[0].desired, pools[0].replicas],
    ['main', 'orders', 'good', null, 8, 8, 2],
  )
  assert.equal(pools[0].key, poolKey({ name: 'main', queue: 'orders' }))
  assert.equal(pools[0].finding, 'At desired capacity')
  assert.deepEqual(pools[0].notes, [])
})

test('a stale replica marks its pools amber and is the one to open', () => {
  const fresh = replica(observe(), 'a')
  const overdue = replica(observe({ updated_at_epoch: NOW - 31 }), 'b')
  assert.equal(poolTone(overdue, overdue.pools[0]), 'warn')
  const [app] = supervisorGroups([fresh, overdue])
  const [p] = applicationPools(app)
  assert.equal(p.tone, 'warn')
  assert.equal(p.mark, 'warn')
  assert.equal(p.stale, 1)
  assert.equal(p.open, overdue.slot)
  assert.equal(p.finding, 'Heartbeat overdue')
  // Only the fresh running replica counts toward the workers.
  assert.equal(p.running, 4)
})

test("the pool's own finding sets the mark on a fresh running instance", () => {
  const critical = replica(observe({ pool_status: [pool(), pool({ supervisor: 'extra', queue: 'mail', running: 0 })] }), 'a')
  const [app] = supervisorGroups([critical])
  const pools = applicationPools(app)
  assert.deepEqual(pools.map((p) => [p.name, p.tone, p.mark]), [['extra', 'bad', 'bad'], ['main', 'good', null]])
  assert.equal(pools[0].finding, 'Pending work, no workers')
})

test('a pool only stopped instances report is idle, unmarked, and says so', () => {
  const stopped = replica(observe({ state: 'stopped' }), 'a')
  assert.equal(stopped.terminal, true)
  const [app] = supervisorGroups([stopped])
  const [p] = applicationPools(app)
  assert.deepEqual([p.tone, p.mark, p.finding, p.stopped], ['idle', null, 'Stopped', 1])
  // Beside a running replica it is that replica's pool, with the stop said.
  const [mixed] = supervisorGroups([replica(observe(), 'b'), stopped])
  const [q] = applicationPools(mixed)
  assert.equal(q.tone, 'good')
  assert.deepEqual(q.notes, ['1 stopped'])
})

test('draining is words, not a mark, and the draining filter keeps only those pools', () => {
  const row = replica(observe({ draining: 1, pool_status: [pool(), pool({ supervisor: 'slow', queue: 'mail', draining: 1 })] }), 'a')
  const [app] = supervisorGroups([row])
  const all = applicationPools(app)
  const slow = all.find((p) => p.name === 'slow')
  assert.deepEqual([slow.tone, slow.mark, slow.finding, slow.draining], ['good', null, 'Workers draining', 1])
  assert.deepEqual(applicationPools(app, { pools: 'draining' }).map((p) => p.name), ['slow'])
  assert.deepEqual(applicationPools(app, { pools: 'attention' }), [])
})

test('the queue context keeps only the pools serving that queue', () => {
  const row = replica(observe({ pool_status: [pool(), pool({ supervisor: 'mailer', queue: 'mail' })] }), 'a')
  const [app] = supervisorGroups([row])
  assert.deepEqual(applicationPools(app, { queue: 'mail' }).map((p) => p.name), ['mailer'])
})

test('the right side lists applications by search, Show and Pools filter, never by the pick', () => {
  const groups = supervisorGroups([
    replica(observe(), 'a'),
    replica(observe({ draining: 1, pool_status: [pool({ draining: 1 })] }), 'b', 'busy-production'),
    replica(observe({ updated_at_epoch: NOW - 31 }), 'c', 'late-production'),
  ])
  assert.deepEqual(supervisorApplications(groups).map((g) => g.name).sort(), ['busy-production', 'late-production', 'orders-production'])
  assert.deepEqual(supervisorApplications(groups, { pools: 'draining' }).map((g) => g.name), ['busy-production'])
  assert.deepEqual(supervisorApplications(groups, { filter: 'stale' }).map((g) => g.name), ['late-production'])
  assert.deepEqual(supervisorApplications(groups, { pools: 'attention' }).map((g) => g.name), ['late-production'])
  assert.deepEqual(supervisorApplications(groups, { queue: 'nope' }), [])
})

test('the Show choices: the four words, one per engine, and a remembered engine kept', () => {
  assert.deepEqual(showOptions(['php', 'node']).map((o) => o.value), ['all', 'attention', 'stale', 'stopped', 'engine:php', 'engine:node'])
  assert.equal(showOptions(['php'])[4].label, 'PHP engine')
  // An engine named in the address that no instance reports any more.
  assert.deepEqual(showOptions([], 'engine:go').map((o) => o.value), ['all', 'attention', 'stale', 'stopped', 'engine:go'])
  assert.deepEqual(POOL_FILTERS.map((o) => o.value), ['all', 'attention', 'draining'])
})
