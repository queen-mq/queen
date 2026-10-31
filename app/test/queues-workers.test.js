// The Workers view of the Queues section (console/queues/workersView.js): the
// reports worker processes publish into Queen KV, read through the old
// Supervisors page's own path (readSupervisorPage -> supervisorObservations
// -> supervisorGroups), turned into one row per pool under its application.
//
// The fixture is a stage-shaped deployment written in the publishers' wire
// format (head + base64 chunks, as RemoteStatusDocument.php, remote_status.rs
// and the SDKs' supervision write it), on queues stage really has:
//   pmsintool-stage  Laravel, Rust engine: 2 pods, one short a worker on default
//   smartpms-stage   Laravel, PHP engine: notifications' restart circuit open,
//                    mail draining, and the stopped pod of the last rollout
//   traceflow-stage  JS SDK: 3 consumers of tf.traces, one overdue
//   channel-stage    Go SDK: two consumer groups, healthy
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'

import { STATUS_FORMAT, readSupervisorPage, supervisorObservations } from '../src/composables/supervisorStatus.js'
import { supervisorGroups } from '../src/composables/supervisorGroups.js'
import {
  NAMESPACE, POOL_ROWS, appDetail, breakable, appRows, appSummary, instanceDetail, instanceState, listCall, poolDetail, poolRows,
  segmentCounts, sourceError, span, workersVerdict,
} from '../src/console/queues/workersView.js'

const NOW = 1_793_456_000 // seconds
// Instance ids as the publishers make them: 32 lowercase hex digits.
const hex = (seed) => createHash('sha256').update(String(seed)).digest('hex').slice(0, 32)
let writes = 0
const writeId = () => (++writes).toString(16).padStart(32, '0')

function wire(doc, slot) {
  const bytes = Buffer.from(JSON.stringify(doc))
  const write = writeId()
  const rows = [{ key: `${slot}/head`, value: { format: STATUS_FORMAT, write, chunks: 1, bytes: bytes.length }, expired: false }]
  rows.push({ key: `${slot}/chunk/0000`, value: { write, index: 0, data: bytes.toString('base64') }, expired: false })
  return rows
}

// A Laravel supervisor's report (supervisor.status/v1), its budget made to add up.
function supervisor({ id, host, engine = 'php', age = 5, state = 'running', pools, limit = 24, cost = 1, configs = [], ready = true }) {
  const status = pools.map((p) => {
    const running = p.running ?? p.desired; const draining = p.draining ?? 0
    return {
      supervisor: p.name, queue: p.queue, running, desired: p.desired, draining, depth: p.depth ?? 0, depth_available: true,
      ready: true, healthy: (p.restart ?? 'closed') === 'closed', restart_state: p.restart ?? 'closed', restart_failures: p.failures ?? 0,
      restart_in_seconds: p.retryIn ?? null, process_cost_per_worker: cost, reserved_processes: (running + draining) * cost,
      renewal_helpers_reserved: (running + draining) * (cost - 1), replicas: p.replicas ?? 2, pids: p.pids ?? [],
    }
  })
  const active = status.reduce((n, p) => n + p.running, 0)
  const draining = status.reduce((n, p) => n + p.draining, 0)
  const helpers = status.reduce((n, p) => n + p.renewal_helpers_reserved, 0)
  return {
    schema: 'queen.supervisor.status/v1', instance_id: id, hostname: host, pid: 4211, engine, engine_version: engine === 'rust' ? '0.3.2' : '8.3.12',
    client_version: '2.7.0', state, paused: false, stopping: false, ready, capacity_satisfied: ready,
    updated_at_epoch: NOW - age, started_at_epoch: NOW - 273_600, uptime_seconds: 273_600 - age,
    configuration: { process_limit: limit, heartbeat_timeout: 30, supervisors: configs }, draining,
    process_budget: { limit, used: active + draining + helpers, available: limit - active - draining - helpers, active_worker_processes: active, draining_worker_processes: draining, renewal_helpers_reserved: helpers },
    pool_status: status,
  }
}
// An SDK consumer's report (consumer.status/v1), as client-js / client-go publish it.
function consumer({ id, host, engine, model, queue, group, concurrency = 4, running = concurrency, busy = 1, completed = 0, failed = 0, age = 6, state = 'running' }) {
  return {
    schema: 'queen.consumer.status/v1', instance_id: id, engine, execution_model: model, hostname: host, pid: 77, state,
    updated_at_epoch: NOW - age, started_at_epoch: NOW - 7200, uptime_seconds: 7200 - age, configuration: { heartbeat_timeout: 30 },
    pool_status: [{ name: 'consumer', queue, namespace: null, task: null, consumer_group: group, desired: concurrency, running, busy, completed, failed,
      last_completed_at_epoch: NOW - age - 1, oldest_inflight_seconds: busy ? 2 : null }],
  }
}
const cfg = (name, queue, extra = {}) => ({ name, queues: [queue], connection: 'queen', consumer_group: 'pmsintool', balance: 'auto', strategy: 'time',
  min_processes: 1, max_processes: 8, timeout: 120, retry_after: 180, lease_renewal: true, tries: 3, memory: 256, ...extra })

const ID = { pmsA: hex('pms-a'), pmsB: hex('pms-b'), smA: hex('sm-a'), smOld: hex('sm-old'), tf0: hex('tf-0'), tf1: hex('tf-1'), tf2: hex('tf-2'), chA: hex('ch-a'), chB: hex('ch-b'), chC: hex('ch-c') }
const DOCS = [
  ['pmsintool-stage', ID.pmsA, supervisor({ id: ID.pmsA, host: 'pmsintool-worker-5c8d9b7f4-k2xlq', engine: 'rust', cost: 2, pools: [
    { name: 'default', queue: 'pmsintool.default', desired: 4, pids: [101, 102, 103, 104] }, { name: 'chargebee', queue: 'pmsintool.chargebee', desired: 2 }],
    configs: [cfg('default', 'pmsintool.default'), cfg('chargebee', 'pmsintool.chargebee', { max_processes: 2 })] })],
  ['pmsintool-stage', ID.pmsB, supervisor({ id: ID.pmsB, host: 'pmsintool-worker-5c8d9b7f4-r7m2n', engine: 'rust', cost: 2, pools: [
    { name: 'default', queue: 'pmsintool.default', desired: 4, running: 3 }, { name: 'chargebee', queue: 'pmsintool.chargebee', desired: 2 }],
    configs: [cfg('default', 'pmsintool.default'), cfg('chargebee', 'pmsintool.chargebee', { max_processes: 2 })], ready: false })],
  ['smartpms-stage', ID.smA, supervisor({ id: ID.smA, host: 'smartpms-worker-6f7c4d-9qz4t', limit: 16, ready: false, pools: [
    { name: 'mail', queue: 'smartpms.mail', desired: 2, draining: 1 }, { name: 'iot', queue: 'smartpms.iot', desired: 2 },
    { name: 'notifications', queue: 'smartpms.notifications', desired: 2, running: 0, depth: 3, restart: 'open', failures: 5, retryIn: 30 }] })],
  ['smartpms-stage', ID.smOld, supervisor({ id: ID.smOld, host: 'smartpms-worker-58b4c9-x1x2c', limit: 16, state: 'stopped', age: 140, pools: [
    { name: 'mail', queue: 'smartpms.mail', desired: 2, running: 0 }] })],
  ['traceflow-stage', ID.tf0, consumer({ id: ID.tf0, host: 'tf-archiver-0', engine: 'js', model: 'async-tasks', queue: 'tf.traces', group: 'tf-archiver', completed: 51_204 })],
  ['traceflow-stage', ID.tf1, consumer({ id: ID.tf1, host: 'tf-archiver-1', engine: 'js', model: 'async-tasks', queue: 'tf.traces', group: 'tf-archiver', completed: 50_877 })],
  ['traceflow-stage', ID.tf2, consumer({ id: ID.tf2, host: 'tf-archiver-2', engine: 'js', model: 'async-tasks', queue: 'tf.traces', group: 'tf-archiver', completed: 49_310, age: 48 })],
  ['channel-stage', ID.chA, consumer({ id: ID.chA, host: 'channel-go-7b9d-2kq8m', engine: 'go', model: 'goroutines', queue: 'channel.metric_sample', group: 'channel.metric_sample.aggregator', completed: 9_870 })],
  ['channel-stage', ID.chB, consumer({ id: ID.chB, host: 'channel-go-7b9d-2kq8m', engine: 'go', model: 'goroutines', queue: 'channel.opsync_fetch', group: 'channel.opsync_fetch.worker', concurrency: 2, completed: 12_040, failed: 2_154 })],
  ['channel-stage', ID.chC, consumer({ id: ID.chC, host: 'channel-go-7b9d-5tnc4', engine: 'go', model: 'goroutines', queue: 'channel.opsync_fetch', group: 'channel.opsync_fetch.worker', concurrency: 2, completed: 11_877, failed: 2_101 })],
]
const byteOrder = (a, b) => Buffer.compare(Buffer.from(a.key), Buffer.from(b.key))
const ROWS = DOCS.flatMap(([group, id, doc]) => wire(doc, `${group}/${id}`)).sort(byteOrder)

// The view's read: one page of the KV listing, decoded by the old page's reader.
async function readAll(rows = ROWS, { group = '' } = {}) {
  const asked = []
  const list = async (body) => {
    asked.push(body)
    const keep = [...rows].sort(byteOrder).filter((r) => (!body.prefix || r.key.startsWith(body.prefix)) && (!body.after || Buffer.compare(Buffer.from(r.key), Buffer.from(body.after)) > 0))
    const page = keep.slice(0, body.limit)
    return { rows: page, truncated: keep.length > body.limit, nextAfter: keep.length > body.limit ? page.at(-1).key : null }
  }
  const { entries } = await readSupervisorPage(list, { namespace: NAMESPACE, group })
  const observed = supervisorObservations(entries, NOW * 1000)
  return { asked, rows: observed, groups: supervisorGroups(observed) }
}
const app = (groups, name) => groups.find((g) => g.name === name)

test('the read is the old page\'s one bounded page, and the view states the same call', async () => {
  const { asked, rows } = await readAll()
  assert.equal(rows.length, 10)
  assert.deepEqual(asked, [{ namespace: 'queen-supervisor', limit: 100, includeExpired: true }])
  assert.deepEqual(listCall(), { method: 'POST', path: '/api/v1/resources/kv/list', body: asked[0] })
  const one = await readAll(ROWS, { group: 'smartpms-stage' })
  assert.deepEqual(one.asked[0], { namespace: 'queen-supervisor', limit: 100, includeExpired: true, prefix: 'smartpms-stage/' })
  assert.deepEqual(listCall({ group: 'smartpms-stage', after: 'smartpms-stage/x' }).body, { ...one.asked[0], after: 'smartpms-stage/x' })
  assert.equal(one.groups.length, 1)
  assert.equal(sourceError({ namespace: '', group: '' }), 'Name the KV namespace the workers write to.')
  assert.match(sourceError({ namespace: 'queen-supervisor', group: 'coordination' }), /reserved/)
  assert.equal(sourceError({ namespace: 'queen-supervisor', group: 'orders-production' }), '')
})

test('a pool is one row across the pods that run it, its numbers from those that report now, by name', async () => {
  const { groups } = await readAll()
  const pools = poolRows(app(groups, 'pmsintool-stage'))
  assert.deepEqual(pools.map((p) => [p.name, p.queueName, p.word, p.tone, p.running, p.desired, p.reporting, p.ongoing]), [
    ['chargebee', 'pmsintool.chargebee', 'At target', '', 4, 4, 2, 2],
    ['default', 'pmsintool.default', 'Below target', 'amber', 7, 8, 2, 2],
  ])
  // The pod short of a worker is the one the row opens.
  assert.equal(pools[1].lead, `pmsintool-stage/${ID.pmsB}`)
  const s = appSummary(app(groups, 'pmsintool-stage'))
  assert.deepEqual([s.kindText, s.instances, s.tone, s.heard], ['Laravel, Rust engine', '2 instances', '', 'heard within 5 s'])
  // A pod not confirming readiness alone is a fact about that pod: pmsB's pool already says it.
  assert.deepEqual(s.issues, [])
})

test('an SDK consumer is named by its consumer group, one row per group and queue', async () => {
  const { groups } = await readAll()
  const pools = poolRows(app(groups, 'channel-stage'))
  assert.deepEqual(pools.map((p) => [p.name, p.rawName, p.group, p.queueName, `${p.running} / ${p.desired}`, p.ongoing]), [
    ['channel.metric_sample.aggregator', 'consumer', 'channel.metric_sample.aggregator', 'channel.metric_sample', '4 / 4', 1],
    ['channel.opsync_fetch.worker', 'consumer', 'channel.opsync_fetch.worker', 'channel.opsync_fetch', '4 / 4', 2],
  ])
  assert.equal(appSummary(app(groups, 'channel-stage')).kindText, 'Go SDK')
  // Two consumers on one host are two instances, told apart by their id.
  assert.equal(appSummary(app(groups, 'channel-stage')).instances, '3 instances')
})

test('an instance that stopped reporting marks its application once, not the pools it runs', async () => {
  const { groups } = await readAll()
  const tf = app(groups, 'traceflow-stage')
  const s = appSummary(tf)
  assert.deepEqual([s.tone, s.issues, s.instances, s.heard], ['amber', ['1 not reporting'], '3 instances', 'heard within 6 s'])
  const [pool] = poolRows(tf)
  assert.deepEqual([pool.name, pool.word, pool.tone, pool.running, pool.desired, pool.reporting, pool.ongoing, pool.silent], ['tf-archiver', 'At target', '', 8, 8, 2, 3, 1])
  const late = tf.rows.find((r) => r.hostname === 'tf-archiver-2')
  assert.deepEqual(instanceState(late), { kind: 'silent', word: 'Heartbeat overdue', tone: 'amber', issue: true, silent: true })
})

test('a restart circuit is red, draining is words, the stopped pod of a rollout is a fact of its application', async () => {
  const { groups } = await readAll()
  const sm = app(groups, 'smartpms-stage')
  const pools = poolRows(sm)
  assert.deepEqual(pools.map((p) => [p.name, p.word, p.tone, p.note, p.stopped]), [
    ['iot', 'At target', '', '', 0],
    ['mail', 'Draining', '', '', 1],
    ['notifications', 'Restart circuit open', 'red', '', 0],
  ])
  assert.match(pools[2].next, /exit logs/)
  const s = appSummary(sm)
  assert.deepEqual([s.kindText, s.instances, s.tone, s.issues], ['Laravel, PHP engine', '1 instance, 1 stopped', '', []])
})

test('the segments count rows before the filter, and the filter names anything a row holds', async () => {
  const { groups } = await readAll()
  assert.deepEqual(segmentCounts(groups), { all: 8, needs: 3, draining: 1, silent: 1, stopped: 1 })
  // needs: notifications (red), default (amber) and tf-archiver, whose application has a silent pod
  const needs = appRows(groups, { seg: 'needs' })
  assert.deepEqual(needs.map((a) => [a.name, a.pools.map((p) => p.name)]), [
    ['pmsintool-stage', ['default']], ['smartpms-stage', ['notifications']], ['traceflow-stage', ['tf-archiver']],
  ])
  const names = (opts) => appRows(groups, opts).flatMap((a) => a.pools.map((p) => `${a.name}:${p.name}`))
  assert.deepEqual(names({ find: 'r7m2n' }), ['pmsintool-stage:chargebee', 'pmsintool-stage:default'])
  assert.deepEqual(names({ find: 'opsync' }), ['channel-stage:channel.opsync_fetch.worker'])
  assert.deepEqual(names({ find: 'python' }), [])
  assert.deepEqual(names({ find: 'go' }), ['channel-stage:channel.metric_sample.aggregator', 'channel-stage:channel.opsync_fetch.worker'])
  // A match on the application keeps all of it.
  assert.deepEqual(names({ find: 'smartpms-stage' }), ['smartpms-stage:iot', 'smartpms-stage:mail', 'smartpms-stage:notifications'])
  assert.deepEqual(names({ seg: 'draining' }), ['smartpms-stage:mail'])
  assert.deepEqual(names({ seg: 'stopped' }), ['smartpms-stage:mail'])
  // Applications by name, whatever their state.
  assert.deepEqual(appRows(groups).map((a) => a.name), ['channel-stage', 'pmsintool-stage', 'smartpms-stage', 'traceflow-stage'])
})

test('?queue= keeps the pools that read that queue, and says when none does', async () => {
  const { groups } = await readAll()
  const rows = appRows(groups, { queue: 'channel.opsync_fetch' })
  assert.deepEqual(rows.map((a) => [a.name, a.pools.map((p) => p.name)]), [['channel-stage', ['channel.opsync_fetch.worker']]])
  assert.deepEqual(segmentCounts(groups, { queue: 'smartpms.mail' }), { all: 1, needs: 0, draining: 1, silent: 0, stopped: 1 })
  assert.deepEqual(workersVerdict({ groups, loaded: true, queue: 'nobody.reads.this' }), { kind: 'empty', queue: 'nobody.reads.this' })
  const v = workersVerdict({ groups, loaded: true, queue: 'pmsintool.default' })
  assert.deepEqual([v.kind, v.names], ['issues', ['pmsintool-stage']])
})

test('a row keeps its place when its pool goes wrong or comes back', async () => {
  const { groups } = await readAll()
  const before = appRows(groups).flatMap((a) => a.pools.map((p) => p.id))
  const healed = DOCS.map(([g, id, doc]) => (id === ID.smA
    ? [g, id, supervisor({ id, host: doc.hostname, limit: 16, pools: [{ name: 'mail', queue: 'smartpms.mail', desired: 2 }, { name: 'iot', queue: 'smartpms.iot', desired: 2 }, { name: 'notifications', queue: 'smartpms.notifications', desired: 2 }] })]
    : [g, id, doc]))
  const after = await readAll(healed.flatMap(([g, id, doc]) => wire(doc, `${g}/${id}`)).sort(byteOrder))
  assert.deepEqual(appRows(after.groups).flatMap((a) => a.pools.map((p) => p.id)), before)
  assert.equal(poolRows(app(after.groups, 'smartpms-stage')).find((p) => p.name === 'notifications').word, 'At target')
})

test('the line on top: what needs you by name, red first, else how many work', async () => {
  const { groups } = await readAll()
  assert.deepEqual(workersVerdict({ loaded: false }), { kind: 'reading' })
  assert.deepEqual(workersVerdict({ loaded: false, failure: 'timeout' }), { kind: 'failed', text: 'timeout' })
  assert.deepEqual(workersVerdict({ loaded: false, refusal: 'gated' }), { kind: 'refused', refusal: 'gated' })
  assert.deepEqual(workersVerdict({ groups: [], loaded: true }), { kind: 'empty', queue: '' })
  assert.deepEqual(workersVerdict({ groups, loaded: true }), {
    kind: 'issues', tone: 'red', names: ['smartpms-stage', 'pmsintool-stage'], more: 1, count: 3, others: 1,
  })
  const fine = groups.filter((g) => g.name === 'channel-stage')
  assert.deepEqual(workersVerdict({ groups: fine, loaded: true }), { kind: 'fine', tail: '8 workers on 2 pools, in 1 application.' })
  const stopped = (await readAll(wire(DOCS[3][2], `smartpms-stage/${ID.smOld}`))).groups
  assert.deepEqual(workersVerdict({ groups: stopped, loaded: true }), { kind: 'stopped', apps: 1 })
})

test('an unreadable report gets a row of its own and marks its application', async () => {
  const torn = { key: `pmsintool-stage/${hex('torn')}/head`, value: { format: STATUS_FORMAT, write: writeId(), chunks: 1, bytes: 900 }, expired: false }
  const { groups } = await readAll([...ROWS, torn].sort(byteOrder))
  const pms = appRows(groups).find((a) => a.name === 'pmsintool-stage')
  assert.deepEqual(pms.orphans.map((o) => [o.word, o.tone, o.silent]), [['Report unreadable', '', true]])
  assert.match(pms.orphans[0].next, /Incomplete status/)
  assert.deepEqual([pms.tone, pms.issues, pms.instances], ['amber', ['1 not reporting'], '3 instances'])
  assert.equal(appRows(groups, { seg: 'silent' }).find((a) => a.name === 'pmsintool-stage').orphans.length, 1)
})

test('a big application shows a first part of its pools and says how many more', async () => {
  const many = Array.from({ length: 15 }, (_, i) => ({ name: `pool-${String(i).padStart(2, '0')}`, queue: `bulk.q${i}`, desired: 1 }))
  const { groups } = await readAll(wire(supervisor({ id: hex('bulk'), host: 'bulk-1', limit: 64, pools: many }), `bulk-stage/${hex('bulk')}`))
  const [cut] = appRows(groups)
  assert.equal(cut.pools.length, POOL_ROWS - 2)
  assert.equal(cut.hidden, 15 - (POOL_ROWS - 2))
  assert.equal(appRows(groups, { expanded: new Set(['bulk-stage']) })[0].pools.length, 15)
  assert.equal(appRows(groups, { find: 'pool' })[0].pools.length, 15)
})

test('the right side: an application, a pool and an instance in words', async () => {
  const { groups } = await readAll()
  const pms = app(groups, 'pmsintool-stage')
  const a = appDetail(pms)
  assert.equal(a.kind, 'app')
  assert.equal(a.kindText, 'Laravel, Rust engine')
  assert.deepEqual(a.facts.map((f) => [f.k, f.v]), [
    ['Workers', '11 running of 12 wanted'], ['Draining', '0 workers'], ['Free slots', '26 process slots the shared budgets can still fit'],
  ])
  assert.deepEqual(a.instances.map((i) => [i.host, i.word, i.tone, i.workers, i.subId, i.sub]), [
    ['pmsintool-worker-5c8d9b7f4-k2xlq', 'At target', '', '6 / 6', `…${ID.pmsA.slice(-8)}`, 'Rust · heard 5 s ago'],
    ['pmsintool-worker-5c8d9b7f4-r7m2n', 'Below target', 'amber', '5 / 6', `…${ID.pmsB.slice(-8)}`, 'Rust · heard 5 s ago'],
  ])

  const p = poolDetail(pms, poolRows(pms).find((x) => x.name === 'default').id)
  assert.deepEqual(p.hosts.map((h) => [h.host, h.word, h.workers, h.subId, h.sub]), [
    ['pmsintool-worker-5c8d9b7f4-k2xlq', 'At target', '4 / 4', `…${ID.pmsA.slice(-8)}`, 'heard 5 s ago'],
    ['pmsintool-worker-5c8d9b7f4-r7m2n', 'Below target', '3 / 4', `…${ID.pmsB.slice(-8)}`, 'heard 5 s ago'],
  ])
  assert.deepEqual(p.settings.map((f) => `${f.k}: ${f.v}`), [
    'Consumer group: pmsintool', 'Connection: queen', 'Scaling: auto, time strategy', 'Workers: 1 to 8 per instance',
    'Per worker: 2 processes, with its lease renewal helper', 'Replicas: 2 coordinated', 'Job timeout: 120 s',
    'Lease: 180 s, renewed while a job runs', 'Attempts: 3', 'Memory: 256 MB per worker',
  ])

  const i = instanceDetail(pms, `pmsintool-stage/${ID.pmsA}`)
  assert.equal(i.kicker, 'Laravel supervisor, Rust engine')
  assert.equal(i.short, `…${ID.pmsA.slice(-8)}`)
  const facts = Object.fromEntries(i.facts.map((f) => [f.k, f.v]))
  assert.match(facts.Heartbeat, /^\d\d:\d\d:\d\d, 5 s before the read$/)
  assert.equal(facts['Up for'].split(',')[0], '3 d 4 h')
  assert.equal(facts.Processes, '12 of 24 slots used, 12 free; 6 hold lease renewal helpers')
  assert.equal(facts.Versions, 'engine 0.3.2, client 2.7.0')
  assert.equal(facts.Readiness, 'ready')
  assert.equal(facts.Workers, '6 running of 6 wanted')
  assert.equal(instanceDetail(pms, `pmsintool-stage/${ID.pmsB}`).facts.find((f) => f.k === 'Readiness').v, 'not ready')
  assert.deepEqual(i.pools.map((x) => [x.name, x.subId, x.sub, x.word]), [['chargebee', 'pmsintool.chargebee', '', 'At target'], ['default', 'pmsintool.default', '', 'At target']])
  assert.deepEqual(i.pools.find((x) => x.name === 'default').pids, [101, 102, 103, 104])
  assert.equal(i.publication.find((f) => f.k === 'Key').v, `pmsintool-stage/${ID.pmsA}/head`)

  const ch = app(groups, 'channel-stage')
  const c = instanceDetail(ch, `channel-stage/${ID.chB}`)
  assert.equal(c.kicker, 'Go consumer')
  assert.deepEqual(c.handler.map((f) => `${f.k}: ${f.v}`), ['Handler calls: 12,040 finished, 2,154 failed, since it started', `Last finished: ${c.handler[1].v}`, 'Oldest running: 2 s'])
  const cf = Object.fromEntries(c.facts.map((f) => [f.k, f]))
  assert.deepEqual([cf.Group.v, cf.Queue.v, cf.Queue.to, cf['Runs as'].v, cf['Pools to check'], cf.Readiness], ['channel.opsync_fetch.worker', 'channel.opsync_fetch', 'channel.opsync_fetch', 'goroutines', undefined, undefined])
  assert.equal(c.onePool, true)

  // A consumer's numbers on its row: busy handlers and failed calls, only when not zero.
  assert.deepEqual(poolDetail(ch, poolRows(ch).find((x) => x.queueName === 'channel.opsync_fetch').id).hosts.map((h) => h.sub.split(' · ').slice(1)), [
    ['1 busy', '2,154 failed calls'], ['1 busy', '2,101 failed calls'],
  ])
  const sm = app(groups, 'smartpms-stage')
  assert.deepEqual(instanceDetail(sm, `smartpms-stage/${ID.smA}`).pools.map((x) => [x.subId, x.sub]), [['smartpms.iot', ''], ['smartpms.mail', '1 draining'], ['smartpms.notifications', '3 waiting']])
  const stopped = instanceDetail(sm, `smartpms-stage/${ID.smOld}`)
  assert.deepEqual([stopped.word, stopped.last.startsWith('Its final report')], ['Stopped', true])
  assert.deepEqual(poolDetail(sm, poolRows(sm).find((x) => x.name === 'notifications').id).failures, [{ host: 'smartpms-worker-6f7c4d-9qz4t', failures: 5, retryIn: 30 }])
  assert.equal(span(273_600), '3 d 4 h')
  assert.deepEqual(breakable('channel.opsync_fetch.worker'), ['channel.', 'opsync_', 'fetch.', 'worker'])
  assert.deepEqual(breakable('pmsintool-worker-5c8d9b7f4-k2xlq'), ['pmsintool-', 'worker-', '5c8d9b7f4-', 'k2xlq'])
})
