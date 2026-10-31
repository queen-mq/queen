// The Cluster page's models (src/console/cluster/clusterViews.js), held
// against payloads shaped like stage's answers of 2026-10-31 (2.7.0-beta.2,
// three voters, queen-mq-v2-2 leading).

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { brokerStatus, cellIssues } from '../src/composables/useBrokerStatus.js'
import { clusterView } from '../src/composables/useRaftCluster.js'
import {
  chartsOf, duration, linkModel, niceTop, nodeDetail, nodeRows,
  pct, ratePair, segments, sharedGate, size, verdictOf, versionsOf, hostLabel,
} from '../src/console/cluster/clusterViews.js'

const GiB = 1024 ** 3
const host = (over = {}) => ({
  anonBytes: 199245824, rssBytes: 916279296, memLimitBytes: 8 * GiB, cpuPct: 23.78, cpuWindowSeconds: 60, cpus: 8,
  disk: { gate: true, highPct: 85, lowPct: 80, totalBytes: 52521566208, usedBytes: 1010446336, usedPct: 1.92, writesRefused: false },
  ...over,
})
const traffic = () => ({
  windowSeconds: 60,
  disk: { readBytesPerSec: 0, writtenBytesPerSec: 385453 },
  network: { receivedBytesPerSec: 260280, sentBytesPerSec: 279088 },
  raft: {
    receivedBytesPerSec: 224626, sentBytesPerSec: 237579,
    replication: { receivedBytesPerSec: 223716, sentBytesPerSec: 14270 },
    forward: { receivedBytesPerSec: 487, sentBytesPerSec: 3387 },
    snapshot: { receivedBytesPerSec: 0, sentBytesPerSec: 0 },
    link: { receivedBytesPerSec: 0, sentBytesPerSec: 0 },
    control: { receivedBytesPerSec: 423, sentBytesPerSec: 219922 },
  },
})
const member = (id, over = {}) => ({
  nodeId: id, hostname: `queen-mq-v2-${id - 1}`, role: 'voter', state: id === 3 ? 'leader' : 'follower', reachable: true,
  httpAddr: `queen-mq-v2-${id - 1}.queen-mq-v2-headless.queen.svc.cluster.local:6632`,
  raftAddr: `queen-mq-v2-${id - 1}.queen-mq-v2-headless.queen.svc.cluster.local:7400`,
  term: 123, appliedIndex: 120022615, committedIndex: 120022615, durableIndex: 120022601, matchIndex: 120022615,
  lagEntries: id === 3 ? null : 0, heartbeatAgeMs: id === 3 ? null : 56, inflight: 0,
  log: { bytes: 199426690, files: 526 },
  store: { mapBytes: 64 * GiB, mapUsedPct: 0.99, maxReaders: 1024, readersInUse: 21, usedBytes: 680468480 },
  host: host(), traffic: traffic(), uptimeSeconds: 59721, version: '2.7.0-beta.2', local: id === 3,
  ...over,
})
const stage = (over = {}) => ({ clusterId: 'raft-1147a5d2', leaderId: 3, term: 123, self: 3, members: [member(1), member(2), member(3)], ...over })
const read = (payload) => {
  const status = brokerStatus(payload)
  return { status, view: clusterView(payload), issues: cellIssues(status) }
}

test('the words for numbers: the chip\'s sizes and shares, spans, rate pairs', () => {
  assert.equal(size(199245824), '190 MB')
  assert.equal(size(8 * GiB), '8 GB')
  assert.equal(size(1180872704), '1.1 GB')
  assert.equal(size(null), '–')
  assert.equal(pct(0.0297), '3%')
  assert.equal(pct(0.004), '<1%')
  assert.equal(pct(0), '0%')
  assert.equal(duration(59721), '16 h 35 min')
  assert.equal(duration(42), '42 s')
  assert.equal(duration(61190), '17 h')        // 16 h 59 min 50 s
  assert.equal(duration(3590), '1 h')          // 59 min 50 s
  assert.equal(duration(3 * 86400 + 23.9 * 3600), '4 d')
  assert.equal(duration(90061), '1 d 1 h')
  assert.equal(ratePair(260280, 279088), '254 / 273 KB/s')
  assert.equal(ratePair(449570, 599324), '439 / 585 KB/s')
  assert.equal(ratePair(14270, 3387), '13.9 / 3.31 KB/s')
  assert.equal(ratePair(0, 1300), '0 / 1.27 KB/s')
  assert.equal(ratePair(null, null), '–')
})

test('a row per node in node-id order, the leader said in words, every reading from the members', () => {
  const { status, view } = read(stage({ members: [member(3), member(1), member(2)] }))
  const rows = nodeRows(status, view)
  assert.deepEqual(rows.map((r) => r.name), ['queen-mq-v2-0', 'queen-mq-v2-1', 'queen-mq-v2-2'])
  assert.deepEqual(rows.map((r) => r.role), ['Follower', 'Follower', 'Leader'])
  const f = rows[0]
  assert.equal(f.up, '16 h 35 min')
  assert.equal(f.cpu, '3%')                 // 23.78% of one core over 8 cores
  assert.equal(f.memory, '190 MB of 8 GB')  // held (anon) of the limit, as the chip
  assert.equal(f.disk.text, '2%')
  assert.equal(f.disk.gate, 85)
  assert.equal(f.behind, '0')
  assert.equal(f.heard, '56 ms ago')
  assert.equal(f.network, '254 / 273 KB/s')
  assert.equal(f.raft, '219 / 232 KB/s')
  const l = rows[2]
  assert.equal(l.behind, '–')
  assert.equal(l.heard, '–')
  assert.equal(sharedGate(status), 85)
})

test('a node nobody reached is red, with its error, and no readings', () => {
  const down = { nodeId: 2, role: 'voter', reachable: false, state: 'unreachable', raftAddr: 'q2:7400', httpAddr: 'q2:6632', error: 'connection refused' }
  const { status, view, issues } = read(stage({ members: [member(1), down, member(3)] }))
  const rows = nodeRows(status, view)
  assert.equal(rows[1].role, 'Unreachable')
  assert.equal(rows[1].roleTone, 'red')
  assert.equal(rows[1].error, 'connection refused')
  const v = verdictOf({ status, view, issues })
  assert.equal(v.tone, 'amber')
  assert.equal(v.text, '1 of 3 voters unreachable.')
  assert.match(v.sub, /^Quorum holds with 2 up and 2 needed — one more loss stops writes\.$/)
})

test('the verdict when all is well: who votes and who leads, and what one loss does', () => {
  const { status, view, issues } = read(stage())
  assert.deepEqual(issues, [])
  const v = verdictOf({ status, view, issues })
  assert.equal(v.tone, '')
  assert.equal(v.badge, 'Healthy')
  assert.equal(v.text, 'All 3 nodes are voting; `queen-mq-v2-2` leads.')
  assert.equal(v.sub, 'Writes go on with one node down.')
})

test('the verdict while reading, when the route is missing, when the read failed', () => {
  assert.equal(verdictOf({ status: null }).text, 'Reading the nodes…')
  assert.match(verdictOf({ status: null, absent: true }).text, /does not report its nodes/)
  const err = Object.assign(new Error('Service unavailable'), { status: 503, code: 'no_leader' })
  const v = verdictOf({ status: null, error: err })
  assert.equal(v.tone, 'red')
  assert.match(v.text, /^The broker did not answer: /)
})

test('several issues: one headline, each issue in the list, worst first', () => {
  const busy = member(1, { host: host({ cpuPct: 790 }) })
  const behind = member(2, { lagEntries: 250000 })
  const { status, view, issues } = read(stage({ members: [busy, behind, member(3)] }))
  const v = verdictOf({ status, view, issues })
  assert.equal(v.text, `${issues.length} problems in the cell.`)
  assert.equal(v.list.length, issues.length)
  assert.ok(v.list.every((i) => i.tone === 'amber' || i.tone === 'red'))
})

test('versions: one is said once, several are listed', () => {
  assert.deepEqual(versionsOf(clusterView(stage())), { list: ['2.7.0-beta.2'], mixed: false, text: '2.7.0-beta.2' })
  const v = versionsOf(clusterView(stage({ members: [member(1, { version: '2.7.0-beta.1' }), member(2), member(3)] })))
  assert.equal(v.mixed, true)
  assert.equal(v.text, '2.7.0-beta.1 · 2.7.0-beta.2')
})

test('one node in the drawer: addresses, log and store, the disk by what fills it, each Raft channel', () => {
  const { status, view } = read(stage())
  const d = nodeDetail(status, view, 'queen-mq-v2-0', { now: 100_000_000, when: (t) => `at ${t}` })
  assert.equal(d.nodeId, 1)
  assert.deepEqual(d.facts.map((f) => f.k), ['Role', 'Version', 'Started', 'HTTP', 'Raft'])
  assert.equal(d.facts[0].v, 'Follower in term 123')
  assert.equal(d.facts[2].v, `at ${100_000_000 - 59721 * 1000}`)
  assert.equal(d.log[0].v, '120,022,615')
  assert.equal(d.log[0].sub, '14 not durable yet')
  assert.equal(d.log.find((x) => x.k === 'Log').v, '190 MB in 526 files')
  assert.equal(d.log.find((x) => x.k === 'Store').v, '0.99% of its 64 GB map')
  assert.equal(d.disk.line, '964 MB of 49 GB used')
  assert.equal(d.disk.gate, 'Writes stop at 85% and start again under 80%.')
  assert.deepEqual(d.disk.parts.map((p) => p.label), ['Raft log', 'Store', 'Everything else'])
  assert.equal(d.disk.io, '0 B/s read, 376 KB/s written')
  assert.equal(d.machine[0].v, '24% of one core, of the 8 it may use')
  assert.equal(d.machine[1].v, '874 MB')
  assert.deepEqual(d.channels.map((c) => c.label), ['Replication', 'Forwarding', 'Snapshots', 'Standby link', 'Control'])
  assert.equal(d.channels[2].idle, true)
  assert.equal(d.window, 'last 60 s')
  assert.equal(nodeDetail(status, view, 'nope'), null)
})

test('niceTop: a round number a little above the largest value', () => {
  assert.equal(niceTop(4.8, 10), 10)
  assert.equal(niceTop(11), 12)
  assert.equal(niceTop(1.13), 1.2)
  assert.equal(niceTop(0, 10), 10)
  assert.equal(niceTop(0), 1)
  assert.equal(niceTop(585), 800)
  assert.equal(niceTop(100), 120)
})

const point = (ts, over = {}) => ({
  timestamp: ts,
  metrics: {
    cpu: { user_us: { avg: 1610 }, system_us: { avg: 1283 } },
    memory: { rss_bytes: { avg: 927510528 } },
    disk: { used_bytes: { avg: 1011499008 }, total_bytes: { avg: 52521566208 } },
    network: { received_bytes_per_sec: { avg: 305453 }, sent_bytes_per_sec: { avg: 170340 } },
    raft_traffic: { received_bytes_per_sec: { avg: 245482 }, sent_bytes_per_sec: { avg: 75005 } },
    ...over,
  },
})

test('the charts: a line per node, styled by node order, CPU as a share of its cores', () => {
  const { status } = read(stage())
  const from = Date.parse('2026-10-31T07:13:00Z'); const to = Date.parse('2026-10-31T08:13:00Z')
  const metrics = {
    bucketMinutes: 1, nodesMissing: [],
    replicas: ['queen-mq-v2-2', 'queen-mq-v2-0', 'queen-mq-v2-1'].map((h) => ({ hostname: h, timeSeries: [point('2026-10-31T07:14:00.000Z'), point('2026-10-31T07:15:00.000Z')] })),
  }
  const m = chartsOf(metrics, status, { from, to })
  assert.deepEqual(m.legend.map((l) => [l.name, l.ink, l.dashed]), [
    ['queen-mq-v2-0', 'in', false], ['queen-mq-v2-1', 'out', false], ['queen-mq-v2-2', 'in', true],
  ])
  assert.deepEqual(m.charts.map((c) => c.id), ['cpu', 'memory', 'disk', 'network', 'raft'])
  const cpu = m.charts[0]
  assert.equal(cpu.sub, 'share of its cores')
  assert.ok(Math.abs(cpu.series[0].points[0][1] - 28.93 / 8) < 1e-9)
  assert.equal(cpu.series[0].points[0][0], Date.parse('2026-10-31T07:14:30.000Z'))  // the middle of its minute
  assert.equal(cpu.topText, '0 to 10%')
  const series = m.charts[0].series.find((s) => s.id === 'queen-mq-v2-2')
  assert.equal(series.dashed, true)
  assert.equal(m.charts[1].topText, '0 to 1 GB')
  assert.equal(m.charts[2].topText, '0 to 10%')
  assert.equal(m.charts[3].topText, '0 to 500 KB/s')
  assert.ok(m.charts.every((c) => c.has))
})

test('the charts: % of one core when a node does not say its cores and the others differ; a node that did not answer is named', () => {
  const p = stage({ members: [member(1, { host: host({ cpus: null }) }), member(2, { host: host({ cpus: 4 }) }), member(3)] })
  const { status } = read(p)
  const metrics = { bucketMinutes: 1, nodesMissing: [2], replicas: [{ hostname: 'queen-mq-v2-0', timeSeries: [point('2026-10-31T07:14:00.000Z', { network: {} })] }] }
  const m = chartsOf(metrics, status, {})
  assert.equal(m.charts[0].sub, '% of one core')
  assert.ok(Math.abs(m.charts[0].series[0].points[0][1] - 28.93) < 1e-9)
  assert.deepEqual(m.missing, ['queen-mq-v2-1'])
  assert.equal(m.charts.find((c) => c.id === 'network').has, false)
  assert.deepEqual(m.legend.filter((l) => l.quiet).map((l) => l.name), ['queen-mq-v2-1', 'queen-mq-v2-2'])
})

test('the standby link, as each role says it', () => {
  assert.equal(linkModel({ data: { role: 'primary', leader: true } }).text, 'No standby reads this cluster, and it follows no other.')
  assert.equal(linkModel({ data: { role: 'primary', leader: true } }).canPromote, false)
  const reading = linkModel({ data: { role: 'primary', readers: [{ name: 'queen-dr', after: 120022000, idleMs: 2000 }], holding: true } }, { selfApplied: 120022615 })
  assert.equal(reading.text, "`queen-dr` reads this cluster's log: `queen-dr` last said where it was 2 s ago, 615 entries behind.")
  assert.equal(reading.canPromote, false)
  const standby = linkModel({ data: { role: 'standby', source: 'prod-a:6632', follower: { state: 'following', lagEntries: 12, lagMs: 400, lastAnswerMs: 80 } } })
  assert.equal(standby.text, 'This cluster is a standby of `prod-a:6632`, 12 entries behind (400 ms). The source answered 80 ms ago.')
  assert.equal(standby.canPromote, true)
  assert.equal(linkModel({ data: { role: 'standby', source: 's', follower: { state: 'halted', error: 'gap in the log' } } }).tone, 'red')
  assert.equal(linkModel({}).text, 'Reading the link…')
  const blocked = Object.assign(new Error('blocked'), { status: 404, code: 'route_blocked' })
  assert.match(linkModel({ error: blocked }).text, /^queen-proxy does not pass/)
  const missing = Object.assign(new Error('not found'), { status: 404, code: 'no_such_route' })
  assert.equal(linkModel({ error: missing }).text, 'This broker has no standby link: it answers 404.')
})

test('a sentence with a call in backticks is split for the template', () => {
  assert.deepEqual(segments('It answers `GET /a` with 404.'), [{ t: 'It answers ', mono: false }, { t: 'GET /a', mono: true }, { t: ' with 404.', mono: false }])
  assert.deepEqual(segments('plain'), [{ t: 'plain', mono: false }])
})

test('a node nobody reached is named by the host its address names, in the table, the drawer and the charts', () => {
  assert.equal(hostLabel('queen-mq-v2-1.queen-mq-v2-headless.queen.svc.cluster.local:6632'), 'queen-mq-v2-1')
  assert.equal(hostLabel('10.0.0.12:6632'), null)
  assert.equal(hostLabel('localhost:6633'), null)
  assert.equal(hostLabel('[::1]:6632'), null)
  const down = { nodeId: 2, role: 'voter', reachable: false, state: 'unreachable', error: 'connection refused',
    raftAddr: 'queen-mq-v2-1.queen-mq-v2-headless.queen.svc.cluster.local:7400', httpAddr: 'queen-mq-v2-1.queen-mq-v2-headless.queen.svc.cluster.local:6632' }
  const p = stage({ members: [member(1), down, member(3)] })
  const { status, view } = read(p)
  assert.equal(status.nodes[1].name, 'node-2')   // brokerStatus's name, which the band's chip shows
  assert.deepEqual(nodeRows(status, view).map((r) => r.name), ['queen-mq-v2-0', 'queen-mq-v2-1', 'queen-mq-v2-2'])
  assert.equal(nodeDetail(status, view, 'queen-mq-v2-1').nodeId, 2)
  assert.equal(nodeDetail(status, view, 'node-2').name, 'queen-mq-v2-1')
  const metrics = { bucketMinutes: 1, nodesMissing: [2], replicas: ['queen-mq-v2-0', 'queen-mq-v2-1', 'queen-mq-v2-2'].map((h) => ({ hostname: h, timeSeries: [point('2026-10-31T07:14:00.000Z')] })) }
  const m = chartsOf(metrics, status, { view })
  assert.deepEqual(m.legend.map((l) => l.name), ['queen-mq-v2-0', 'queen-mq-v2-1', 'queen-mq-v2-2'])
  assert.equal(m.charts[0].sub, 'share of its cores')   // the 8 cores the others say
  assert.deepEqual(m.missing, ['queen-mq-v2-1'])
})
