// The Overview page's three views (Overview, Queues, Server), its fixed sub
// sidebar and the numbers each view draws: composables/overviewViews.js.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { brokerStatus } from '../src/composables/useBrokerStatus.js'
import {
  attentionList, attentionWhy, busiestQueues, groupsSummary, inRateUnit, lagText, nodeLines,
  nodeRows, OVERVIEW_VIEWS, overviewVerdict, overviewView, perSecond, queueTotals, queuesIn, replicaSeries,
  replicaTimeline, rollupQueueOps, sideGroups, systemRedirect, trafficWindow, viewAlias, viewQuery, windowSeconds, worstMark,
} from '../src/composables/overviewViews.js'

const q = (name, namespace, messages = {}, extra = {}) => ({ name, namespace, partitions: 4, messages: { pending: 0, ...messages }, ...extra })
const g = (queueName, maxTimeLag, extra = {}) => ({ name: `${queueName}-reader`, queueName, state: 'Stable', maxTimeLag, partitionsWithLag: 0, ...extra })

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

test('the view: the Summary by default; every view by its key', () => {
  assert.equal(overviewView('nodes'), 'summary', 'the cell views are System\'s now')
  assert.equal(overviewView(undefined), 'summary')
  assert.equal(overviewView(''), 'summary')
  assert.equal(overviewView('anything'), 'summary')
  assert.equal(overviewView(['issues']), 'summary', 'a repeated ?view= is not a pick')
  for (const key of ['issues', 'throughput', 'backlog', 'groups', 'dead-letters']) assert.equal(overviewView(key), key)
})

test('the Cell row is a link to System, and the old cell addresses go there', () => {
  assert.equal(overviewView('cell'), 'summary')
  for (const [from, to] of [['server', 'overview'], ['cell', 'overview'], ['nodes', 'nodes'], ['resources', 'compute'], ['replication', 'raft']]) {
    assert.equal(systemRedirect(from), to)
  }
  assert.equal(systemRedirect('backlog'), null)
  const op = sideGroups({}, true)
  const cell = op[0].rows.find((r) => r.key === 'cell')
  assert.equal(cell.href, '/system')
  assert.equal(sideGroups({}, false)[0].rows.some((r) => r.key === 'cell'), false)
})

test('worstMark: only warn and bad are marks', () => {
  assert.equal(worstMark([]), null)
  assert.equal(worstMark(['ok', 'mute', '', null, 'idle']), null)
  assert.equal(worstMark(['ok', 'warn']), 'warn')
  assert.equal(worstMark(['warn', 'bad', 'ok']), 'bad')
})

// ---------------------------------------------------------------------------
// The verdict
// ---------------------------------------------------------------------------

test('verdict: healthy only when both readings are in and nothing needs you', () => {
  assert.deepEqual(overviewVerdict({ known: true }), { sev: 'ok', word: 'Healthy', headline: 'All queues are healthy' })
  assert.deepEqual(overviewVerdict({ loading: true }), { sev: 'unknown', word: 'Checking', headline: 'Checking your queues…' })
  assert.deepEqual(overviewVerdict({ known: false }), { sev: 'unknown', word: 'Unknown', headline: 'Status unknown' })
})

test('verdict: the queues, worst first, then the tenant, then the cell', () => {
  const lag = (name, sev) => ({ name, sev })
  assert.equal(overviewVerdict({ known: true, issues: [lag('orders', 'bad')] }).headline, 'orders is falling behind')
  assert.equal(overviewVerdict({ known: true, issues: [lag('a', 'bad'), lag('b', 'bad'), lag('c', 'warn')] }).headline, '2 queues are falling behind')
  assert.equal(overviewVerdict({ known: true, issues: [lag('a', 'warn')] }).headline, '1 queue needs attention')
  assert.equal(overviewVerdict({ known: true, issues: [lag('a', 'warn'), lag('b', 'warn')] }).headline, '2 queues need attention')
  const acks = { key: 'acks', sev: 'warn', name: 'Ack failures', tenant: true }
  assert.deepEqual(overviewVerdict({ known: true, issues: [acks] }), { sev: 'warn', word: 'Degraded', headline: 'Ack failures need attention' })
  const disk = { sev: 'warn', title: "queen-2's disk is nearly full" }
  assert.deepEqual(overviewVerdict({ known: true, cellIssues: [disk] }), { sev: 'warn', word: 'Degraded', headline: "queen-2's disk is nearly full" })
  assert.equal(overviewVerdict({ known: true, cellIssues: [disk, disk] }).headline, '2 problems in the cell')
})

test('verdict: a failing cell leads, even while the queues cannot be read', () => {
  const quorum = { sev: 'bad', title: 'Quorum lost' }
  assert.deepEqual(overviewVerdict({ known: false, cellIssues: [quorum] }), { sev: 'bad', word: 'Failing', headline: 'Quorum lost' })
  assert.equal(overviewVerdict({ known: true, issues: [{ name: 'a', sev: 'bad' }], cellIssues: [quorum] }).headline, 'Quorum lost')
})

// ---------------------------------------------------------------------------
// Words
// ---------------------------------------------------------------------------

test('lagText: an age in plain units; unknown is a dash, not zero', () => {
  assert.equal(lagText(null), '—')
  assert.equal(lagText(undefined), '—')
  assert.equal(lagText(0), '0s')
  assert.equal(lagText(42.4), '42s')
  assert.equal(lagText(312), '5m 12s')
  assert.equal(lagText(300), '5m')
  assert.equal(lagText(7440), '2h 4m')
  assert.equal(lagText(90000), '1d 1h')
})

test('perSecond: a rate for a column that names the unit', () => {
  assert.equal(perSecond(null), '—')
  assert.equal(perSecond(0), '0')
  assert.equal(perSecond(0.0667), '0.07')
  assert.equal(perSecond(0.0014), '<0.01', 'a trickle is not zero')
  assert.equal(perSecond(12.46), '12.5')
  assert.equal(perSecond(340.4), '340')
  assert.equal(perSecond(1234), '1.2k')
})

test('attentionWhy: the reason in a few words', () => {
  assert.equal(attentionWhy({ reason: 'lag', lag: 312 }), 'A consumer group is behind')
  assert.equal(attentionWhy({ reason: 'noReader', deadOnly: false }), 'No consumer group reads it')
  assert.equal(attentionWhy({ reason: 'noReader', deadOnly: true }), 'Queue mode has never consumed from it')
  assert.equal(attentionWhy(null), '')
})

// ---------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------

const listing = [
  q('orders', 'shop', { pending: 10, processing: 2, deadLetter: 1 }, { retainedBytes: 1000 }),
  q('refunds', 'shop', { pending: 5, deadLetter: 0 }, { retainedBytes: 500 }),
  q('mail', '', { pending: 0 }),
  q('legacy', undefined, { pending: 3 }),
]

test('queuesIn: null is every namespace, an empty string the default one', () => {
  assert.deepEqual(queuesIn(listing, null).map((x) => x.name), ['orders', 'refunds', 'mail', 'legacy'])
  assert.deepEqual(queuesIn(listing, 'shop').map((x) => x.name), ['orders', 'refunds'])
  assert.deepEqual(queuesIn(listing, '').map((x) => x.name), ['mail', 'legacy'])
  assert.deepEqual(queuesIn(listing, 'nope'), [])
  assert.deepEqual(queuesIn([null, { namespace: 'shop' }], 'shop'), [], 'a row without a name is not a queue')
})

test('queueTotals: sums of what the queues report; unreported is null, no queues is zero', () => {
  assert.deepEqual(queueTotals(queuesIn(listing, 'shop')), {
    queues: 2, partitions: 8, pending: 15, processing: 2, deadLetter: 1, retainedBytes: 1500,
  })
  const quiet = queueTotals([q('mail', '', { pending: 0 })])
  assert.equal(quiet.retainedBytes, null, 'no queue said how much it stores')
  assert.equal(quiet.deadLetter, null)
  assert.equal(quiet.pending, 0, 'a reported 0 stays 0')
  assert.deepEqual(queueTotals([]), { queues: 0, partitions: 0, pending: 0, processing: 0, deadLetter: 0, retainedBytes: 0 })
})

const ops = [
  { bucket: '2026-10-30T10:01:00Z', queueName: 'orders', pushPerSecond: 2, popPerSecond: 1, ackPerSecond: 1, pushMessages: 120, popMessages: 60, ackSuccess: 58, ackFailed: 2, avgLagMs: 100, maxLagMs: 400, parkedCount: 1 },
  { bucket: '2026-10-30T10:00:00Z', queueName: 'orders', pushPerSecond: 1, popPerSecond: 1, pushMessages: 60, popMessages: 60, avgLagMs: 50, maxLagMs: 90 },
  // No pops: its lag is no measurement and must not pull the bucket's down.
  { bucket: '2026-10-30T10:01:00Z', queueName: 'refunds', pushPerSecond: 0.5, pushMessages: 30, popMessages: 0, avgLagMs: 0, maxLagMs: null },
  { bucket: '2026-10-30T10:01:00Z', queueName: 'mail', pushPerSecond: 4, popPerSecond: 3, pushMessages: 240, popMessages: 180, avgLagMs: 400, maxLagMs: 900, fetchMessages: 6, fetchPerSecond: 0.1 },
  // A deleted queue: in the tenant's traffic, in no namespace.
  { bucket: '2026-10-30T10:00:00Z', queueName: 'gone', pushPerSecond: 9, pushMessages: 540 },
]

test('rollupQueueOps: every row for the tenant, oldest bucket first', () => {
  const rows = rollupQueueOps(ops)
  assert.deepEqual(rows.map((r) => r.bucket), ['2026-10-30T10:00:00Z', '2026-10-30T10:01:00Z'])
  assert.equal(rows[0].pushPerSecond, 10)
  assert.equal(rows[1].pushPerSecond, 6.5)
  assert.equal(rows[1].fetchMessages, 6)
  assert.equal(rows[1].parkedTotal, 1)
  // (100·60 + 400·180) / 240: refunds popped nothing and is left out.
  assert.equal(rows[1].avgLagMs, 325)
  assert.equal(rows[1].maxLagMs, 900)
  assert.equal(rows[0].avgLagMs, 50)
})

test('rollupQueueOps: a namespace is the rows of its queues only', () => {
  const rows = rollupQueueOps(ops, new Set(['orders', 'refunds']))
  assert.equal(rows.length, 2)
  assert.equal(rows[0].pushPerSecond, 1, 'the deleted queue is in no namespace')
  assert.equal(rows[1].pushPerSecond, 2.5)
  assert.equal(rows[1].ackFailed, 2)
  assert.equal(rows[1].avgLagMs, 100)
  assert.deepEqual(rollupQueueOps(ops, new Set()), [])
  assert.deepEqual(rollupQueueOps(null), [])
})

test('windowSeconds: the span of a time range, or null', () => {
  assert.equal(windowSeconds({ from: '2026-10-30T10:00:00Z', to: '2026-10-30T11:00:00Z' }), 3600)
  assert.equal(windowSeconds({ from: 'x', to: '2026-10-30T11:00:00Z' }), null)
  assert.equal(windowSeconds({ from: '2026-10-30T11:00:00Z', to: '2026-10-30T10:00:00Z' }), null)
  assert.equal(windowSeconds(undefined), null)
})

test('busiestQueues: most messages moved first, per second over the window, quiet queues left out', () => {
  const rows = busiestQueues([...ops, { bucket: '2026-10-30T10:00:00Z', queueName: 'idle', pushMessages: 0, popMessages: 0 }],
    new Set(['orders', 'refunds', 'mail', 'idle']), { seconds: 60 })
  assert.deepEqual(rows.map((r) => r.name), ['mail', 'orders', 'refunds'])
  assert.equal(rows[0].pushPerSecond, 4)
  assert.equal(rows[0].popPerSecond, 3)
  assert.equal(rows[1].pushMessages, 180)
  assert.equal(busiestQueues(ops, null, { limit: 1 })[0].name, 'gone')
  assert.equal(busiestQueues(ops, null)[0].pushPerSecond, null, 'no window, no rate')
})

test('attentionList: failing first, then the longest lag, then the most pending', () => {
  const queues = [q('a', 'x', { pending: 5 }), q('b', 'x', { pending: 50 }), q('c', 'x', { pending: 1 }), q('d', 'x', { pending: 9 })]
  const groups = [g('a', 120), g('b', 120), g('c', 4000)]
  // d has messages and no reader at all.
  const rows = attentionList(queues, groups)
  assert.deepEqual(rows.map((r) => [r.name, r.sev]), [['c', 'bad'], ['b', 'warn'], ['a', 'warn'], ['d', 'warn']])
  assert.equal(rows[3].reason, 'noReader')
})

test('groupsSummary: the groups of a namespace, the ones behind first, inactive Kafka groups last', () => {
  const groups = [
    g('orders', 10),
    g('orders', 120, { name: 'billing' }),
    g('refunds', 900000, { name: 'old-kafka', kind: 'kafka', inactiveSeconds: 99999 }),
    g('refunds', 4000, { name: 'slow' }),
    g('mail', 9000, { name: 'elsewhere' }),
  ]
  const s = groupsSummary(groups, new Set(['orders', 'refunds']))
  assert.deepEqual(s.rows.map((r) => r.name), ['slow', 'billing', 'orders-reader', 'old-kafka'])
  assert.equal(s.total, 4)
  assert.equal(s.behind, 2, 'an inactive Kafka group is not behind, it is gone')
  assert.equal(s.sev, 'bad')
  assert.equal(groupsSummary(groups, null).total, 5)
  assert.deepEqual(groupsSummary([], null), { rows: [], total: 0, behind: 0, sev: null })
})

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

const GiB = 1024 ** 3
const member = (nodeId, over = {}) => ({
  nodeId,
  role: 'voter',
  state: 'follower',
  reachable: true,
  hostname: `queen-${nodeId}`,
  term: 3,
  lagEntries: 0,
  heartbeatAgeMs: 20,
  host: {
    cpuPct: 50, cpus: 4, rssBytes: 2 * GiB, memLimitBytes: 8 * GiB,
    disk: { totalBytes: 100 * GiB, usedBytes: 41 * GiB, usedPct: 41, gate: true, highPct: 85, lowPct: 80, writesRefused: false },
  },
  traffic: {
    windowSeconds: 10,
    network: null,
    disk: { readBytesPerSec: 0, writtenBytesPerSec: 13504 },
    raft: { sentBytesPerSec: 660, receivedBytesPerSec: 7002, snapshot: { sentBytesPerSec: 0, receivedBytesPerSec: 0 } },
  },
  ...over,
})

test('nodeRows: one row per node, in node-id order, a mark only on a node that needs you', () => {
  const status = brokerStatus({
    leaderId: 1,
    term: 3,
    members: [
      member(2, { host: { ...member(2).host, disk: { ...member(2).host.disk, usedPct: 82 } } }),
      member(1, { state: 'leader', heartbeatAgeMs: null }),
      member(3, { reachable: false, state: 'unreachable', host: null, traffic: null }),
    ],
  })
  const rows = nodeRows(status)
  assert.deepEqual(rows.map((r) => r.name), ['queen-1', 'queen-2', 'queen-3'])
  assert.deepEqual(rows.map((r) => r.mark), [null, 'warn', 'bad'])
  assert.deepEqual(rows.map((r) => r.leader), [true, false, false])
  assert.equal(rows[0].role, 'Leader')
  assert.equal(rows[2].role, 'Unreachable')
  assert.equal(rows[2].roleTone, 'bad')
  assert.equal(rows[0].cpu.text, '13%')
  assert.equal(rows[0].mem.text, '25%')
  assert.deepEqual(rows[1].disk, { text: '82%', tone: 'warn' })
  assert.equal(rows[0].raft, '660 B/s / 6.84 KB/s')
  assert.equal(rows[0].net, '—', 'macOS reports no network: unknown, not zero')
  assert.equal(rows[0].write, '13.2 KB/s')
  assert.equal(rows[2].cpu.text, '—')
  assert.equal(rows[2].write, '—')
  assert.equal(trafficWindow(status), 10)
  assert.deepEqual(nodeRows(null), [])
  assert.equal(trafficWindow(null), null)
})

test('nodeRows: a follower behind says how far', () => {
  const status = brokerStatus({ leaderId: 1, term: 3, members: [member(1, { state: 'leader' }), member(2, { lagEntries: 12345 })] })
  assert.equal(nodeRows(status)[1].role, 'Follower · 12,345 behind')
})

const point = (timestamp, cpu, rss, read, written) => ({
  timestamp,
  metrics: {
    cpu: { user_us: { avg: cpu, max: cpu * 2, last: cpu } },
    memory: { rss_bytes: { avg: rss } },
    disk: { read_bytes_per_sec: { avg: read }, written_bytes_per_sec: { avg: written } },
    network: { received_bytes_per_sec: { avg: null }, sent_bytes_per_sec: { avg: null } },
  },
})
const replicas = [
  { hostname: 'queen-1', timeSeries: [point('2026-10-30T10:01:00Z', 600, 100 * 2 ** 20, 0, 2048), point('2026-10-30T10:00:00Z', 500, 90 * 2 ** 20, 0, 1024)] },
  // Started a minute later, and missed nothing since.
  { hostname: 'queen-2', timeSeries: [point('2026-10-30T10:01:00Z', 300, 80 * 2 ** 20, 512, 4 * 1024 * 1024)] },
]

test('replicaTimeline: the union of every replica\'s timestamps, oldest first', () => {
  assert.deepEqual(replicaTimeline(replicas), ['2026-10-30T10:00:00Z', '2026-10-30T10:01:00Z'])
  assert.deepEqual(replicaTimeline(null), [])
})

test('replicaSeries: read by timestamp, a gap where the replica sent nothing', () => {
  const timeline = replicaTimeline(replicas)
  assert.deepEqual(replicaSeries(replicas[1], timeline, ['cpu', 'user_us'], { scale: (v) => v / 100 }), [null, 3])
  assert.deepEqual(replicaSeries(replicas[0], timeline, ['cpu', 'user_us'], { agg: 'max' }), [1000, 1200])
  assert.deepEqual(replicaSeries(replicas[0], timeline, ['network', 'received_bytes_per_sec']), [null, null])
})

test('nodeLines: one line per node, the second spec dashed; nothing reported is no data', () => {
  const cpu = nodeLines(replicas, [{ path: ['cpu', 'user_us'] }], { scale: (v) => v / 100, suffix: ' · user' })
  assert.deepEqual(cpu.datasets.map((d) => d.label), ['queen-1 · user', 'queen-2 · user'])
  assert.deepEqual(cpu.datasets.map((d) => d.node), [0, 1])
  assert.deepEqual(cpu.datasets[0].data, [5, 6])
  assert.equal(cpu.hasData, true)
  const disk = nodeLines(replicas, [{ path: ['disk', 'read_bytes_per_sec'], short: 'read' }, { path: ['disk', 'written_bytes_per_sec'], short: 'write' }])
  assert.deepEqual(disk.datasets.map((d) => [d.label, d.node, d.dashed]), [
    ['queen-1 · read', 0, false], ['queen-1 · write', 0, true], ['queen-2 · read', 1, false], ['queen-2 · write', 1, true],
  ])
  const net = nodeLines(replicas, [{ path: ['network', 'received_bytes_per_sec'], short: 'in' }, { path: ['network', 'sent_bytes_per_sec'], short: 'out' }])
  assert.equal(net.hasData, false)
  assert.equal(nodeLines([], [{ path: ['cpu', 'user_us'] }]).hasData, false)
})

test('inRateUnit: the unit the largest value reads best in, named in every label', () => {
  const disk = inRateUnit(nodeLines(replicas, [{ path: ['disk', 'read_bytes_per_sec'], short: 'read' }, { path: ['disk', 'written_bytes_per_sec'], short: 'write' }]))
  assert.equal(disk.unit.label, 'MB/s')
  assert.equal(disk.datasets[3].label, 'queen-2 · write (MB/s)')
  assert.deepEqual(disk.datasets[3].data, [null, 4])
  assert.deepEqual(disk.datasets[1].data, [0, 0])
})
