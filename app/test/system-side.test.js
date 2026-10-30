// System's list and tabs: the cell's nodes and the marks of the two tabs,
// read from GET /api/v1/raft/members payloads through brokerStatus.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { brokerStatus } from '../src/composables/useBrokerStatus.js'
import {
  attention, nodeEntries, nodeMark, nodeReplicas, nodeRole, tabMarks, worstMark,
} from '../src/composables/systemSide.js'

const GiB = 1024 ** 3

const host = (over = {}) => ({
  cpuPct: 150,
  cpuWindowSeconds: 60,
  cpus: 4,
  rssBytes: 2 * GiB,
  memLimitBytes: 8 * GiB,
  disk: { totalBytes: 100 * GiB, usedBytes: 41 * GiB, usedPct: 41, gate: true, highPct: 85, lowPct: 80, writesRefused: false },
  ...over,
})

const member = (nodeId, over = {}) => ({
  nodeId,
  role: 'voter',
  state: 'follower',
  reachable: true,
  hostname: `queen-${nodeId}`,
  term: 14,
  lagEntries: 0,
  heartbeatAgeMs: 12,
  host: host(),
  ...over,
})

const cell = (overs = {}) => brokerStatus({
  leaderId: 1,
  term: 14,
  members: [
    member(1, { state: 'leader', lagEntries: null, heartbeatAgeMs: null, ...(overs[1] || {}) }),
    member(2, overs[2] || {}),
    member(3, overs[3] || {}),
  ],
})

test('the worst severity wins, and only attention is a mark', () => {
  assert.equal(worstMark([]), 'ok')
  assert.equal(worstMark(['', null, undefined]), 'ok')
  assert.equal(worstMark(['', 'warn']), 'warn')
  assert.equal(worstMark(['warn', 'bad', '']), 'bad')
  assert.equal(worstMark(['mute', 'ok']), 'ok')
  assert.deepEqual(['ok', 'idle', '', null, 'warn', 'bad'].map(attention), ['', '', '', '', 'warn', 'bad'])
})

test('a healthy cell: no node marked, each with its role, in node-id order', () => {
  const entries = nodeEntries(cell())
  assert.deepEqual(entries.map((e) => e.name), ['queen-1', 'queen-2', 'queen-3'])
  assert.deepEqual(entries.map((e) => e.index), [0, 1, 2])
  assert.deepEqual(entries.map((e) => e.mark), ['', '', ''])
  assert.deepEqual(entries.map((e) => e.role), ['Leader', 'Follower', 'Follower'])
  assert.deepEqual(entries.map((e) => e.why), ['', '', ''])
})

test('a node is marked by the worst of its cpu, memory, disk, lag and heartbeat', () => {
  const s = cell({
    2: { host: host({ cpuPct: 380 }) },               // 95% of its cores: warn
    3: { host: host({ rssBytes: 7.5 * GiB }) },       // 7.5 of 8 GiB: bad
  })
  const [one, two, three] = nodeEntries(s)
  assert.equal(one.mark, '')
  assert.equal(two.mark, 'warn')
  assert.equal(three.mark, 'bad')
  assert.match(two.why, /CPU 95%/)
  assert.match(three.why, /Memory 94%/)
})

test('a node nobody reached is bad, says so, and has no readings to judge', () => {
  const s = cell({ 3: { state: 'unreachable', reachable: false, hostname: null, host: undefined } })
  const three = nodeEntries(s)[2]
  assert.equal(three.name, 'node-3')
  assert.equal(three.mark, 'bad')
  assert.equal(three.role, 'Unreachable')
  assert.equal(three.why, 'No answer from this member')
  assert.equal(nodeMark(s.nodes[2]), 'bad')
})

test('a follower behind says by how much, in its why', () => {
  const two = nodeEntries(cell({ 2: { lagEntries: 1234 } }))[1]
  assert.equal(two.role, 'Follower')
  assert.equal(two.why, '1,234 entries behind the leader')
})

test('a learner says so, whatever its state', () => {
  const s = cell({ 3: { role: 'learner', state: 'follower' } })
  assert.equal(nodeRole(s.nodes[2]), 'Learner')
  assert.equal(nodeEntries(s)[2].role, 'Learner')
})

test('no members read: no nodes, and the tabs carry no mark', () => {
  assert.deepEqual(nodeEntries(null), [])
  assert.deepEqual(tabMarks(null), { resources: { mark: '', why: '' }, raft: { mark: '', why: '' } })
})

test('a healthy cell marks neither tab', () => {
  const t = tabMarks(cell())
  assert.equal(t.resources.mark, '')
  assert.equal(t.raft.mark, '')
})

test('the tabs carry the cell\'s issues: resources the machines, raft the log', () => {
  const busy = tabMarks(cell({ 2: { host: host({ cpuPct: 380 }) } }))
  assert.equal(busy.resources.mark, 'warn')
  assert.match(busy.resources.why, /queen-2 is busy/)
  assert.equal(busy.raft.mark, '')

  // The quorum's own words (clusterAlert) when a voter is down.
  const down = tabMarks(cell({ 3: { state: 'unreachable', reachable: false, hostname: null, host: undefined } }))
  assert.equal(down.raft.mark, 'warn')
  assert.match(down.raft.why, /1 of 3 voters unreachable/)
  assert.equal(down.resources.mark, '')
})

test('with a node picked, the tabs carry that node\'s issues only', () => {
  const s = cell({
    2: { host: host({ cpuPct: 380 }) },
    3: { state: 'unreachable', reachable: false, hostname: null, host: undefined },
  })
  assert.deepEqual(tabMarks(s, 'queen-1'), { resources: { mark: '', why: '' }, raft: { mark: '', why: '' } })
  assert.equal(tabMarks(s, 'queen-2').resources.mark, 'warn')
  assert.equal(tabMarks(s, 'queen-2').resources.why, 'CPU 95% of the cores it may use')
  assert.equal(tabMarks(s, 'queen-2').raft.mark, '')
  const gone = tabMarks(s, 'node-3')
  assert.equal(gone.raft.mark, 'bad')
  assert.equal(gone.raft.why, 'node-3 does not answer')
  assert.equal(gone.resources.mark, '')
  // A name the members do not hold.
  assert.equal(tabMarks(s, 'node-9').raft.mark, '')
})

test('an issue several nodes share names one of them, picked, in its own words', () => {
  const full = host({ disk: { ...host().disk, usedPct: 82 } }) // past the low line, under the gate
  const s = cell({ 1: { host: full }, 2: { host: full }, 3: { host: full } })
  assert.match(tabMarks(s).resources.why, /3 nodes' disks are nearly full/)
  assert.equal(tabMarks(s, 'queen-2').resources.mark, 'warn')
  assert.doesNotMatch(tabMarks(s, 'queen-2').resources.why, /3 nodes/)
  assert.match(tabMarks(s, 'queen-2').resources.why, /Disk 82% used/)
})

test('a single node is the leader and carries no raft mark', () => {
  const s = brokerStatus({ leaderId: 1, term: 3, members: [member(1, { state: 'leader', lagEntries: null, heartbeatAgeMs: null })] })
  assert.deepEqual(nodeEntries(s).map((e) => [e.role, e.mark]), [['Leader', '']])
  assert.equal(tabMarks(s).raft.mark, '')
})

test('a node picks its own metric replicas by hostname; none picked is all of them', () => {
  const replicas = [
    { hostname: 'queen-1', port: 6632, timeSeries: [{}, {}] },
    { hostname: 'queen-2', port: 6632, timeSeries: [{}] },
    { hostname: 'queen-2', port: 6633, timeSeries: [{}, {}, {}] },
  ]
  assert.equal(nodeReplicas(replicas, null).length, 3)
  assert.deepEqual(nodeReplicas(replicas, 'queen-2').map((r) => r.port), [6632, 6633])
  assert.deepEqual(nodeReplicas(replicas, 'node-9'), [])
  assert.deepEqual(nodeReplicas(undefined, 'queen-1'), [])
})
