// The Raft source of System, held against the broker contract's own payloads:
// GET /api/v1/raft/members and GET /api/v1/raft/status.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { createApiClient } from '../src/api/httpClient.js'
import {
  clusterAlert,
  clusterView,
  describeRaftFailure,
  formatIndex,
  formatMapPct,
  isNoLeaderError,
  memberRow,
  nodeView,
  quorumHealth,
  stateChip,
} from '../src/composables/useRaftCluster.js'

// One member as the contract spells it; `over` changes what a case is about.
const member = (nodeId, over = {}) => ({
  nodeId,
  role: 'voter',
  state: 'follower',
  local: false,
  reachable: true,
  raftAddr: `127.0.0.1:${7400 + nodeId}`,
  httpAddr: `127.0.0.1:${6630 + nodeId}`,
  hostname: `node-${nodeId}`,
  term: 2,
  lastLogIndex: 58972,
  committedIndex: 58972,
  appliedIndex: 58972,
  durableIndex: 58960,
  matchIndex: 58972,
  lagEntries: 0,
  heartbeatAgeMs: 12,
  inflight: 0,
  log: { files: 708, bytes: 65559235 },
  store: { mapBytes: 68719476736, usedBytes: 26378240, mapUsedPct: 0.038, readersInUse: 8, maxReaders: 1024 },
  version: '1.6.0',
  uptimeSeconds: 2972,
  error: null,
  ...over,
})

// "For an unreachable member only nodeId/role/raftAddr/httpAddr/reachable:false/error are set."
const down = (nodeId, error = 'connect: connection refused', role = 'voter') => ({
  nodeId,
  role,
  state: null,
  local: false,
  reachable: false,
  raftAddr: `127.0.0.1:${7400 + nodeId}`,
  httpAddr: `127.0.0.1:${6630 + nodeId}`,
  hostname: null,
  term: null,
  lastLogIndex: null,
  committedIndex: null,
  appliedIndex: null,
  durableIndex: null,
  matchIndex: null,
  lagEntries: null,
  heartbeatAgeMs: null,
  inflight: null,
  log: null,
  store: null,
  version: null,
  uptimeSeconds: null,
  error,
})

// The contract example: node 1 leads, node 2 answered.
const members = (list, over = {}) => ({
  engine: 'raft', clusterId: 'raft-3f9a2c1e', leaderId: 1, term: 2, self: 2, members: list, ...over,
})
const healthy = () => members([
  member(3),
  member(1, { state: 'leader', lagEntries: 0, heartbeatAgeMs: 12 }),
  member(2, { local: true }),
])

// ---------------------------------------------------------------------------
// The healthy cluster
// ---------------------------------------------------------------------------

test('three voters up: ordered by node id, the leader marked, this node marked, no alert', () => {
  const view = clusterView(healthy())
  assert.equal(view.clusterId, 'raft-3f9a2c1e')
  assert.equal(view.term, 2)
  assert.equal(view.self, 2)
  assert.deepEqual(view.rows.map((r) => r.nodeId), [1, 2, 3])
  assert.equal(view.leader.nodeId, 1)
  assert.deepEqual(view.rows[0].chip, { cls: 'chip-ok', label: 'leader' })
  assert.deepEqual(view.rows[1].chip, { cls: 'chip-mute', label: 'follower' })
  assert.deepEqual(view.rows.map((r) => r.isSelf), [false, true, false])
  assert.deepEqual(view.quorum, {
    voters: 3, up: 3, down: 0, learners: 0, majority: 2, hasQuorum: true, spare: 1, severity: '',
  })
  assert.equal(view.singleNode, false)
  assert.equal(clusterAlert(view), null)
})

test("lag and heartbeat are the leader's view of followers: the leader's own row has neither", () => {
  const [leader, follower] = clusterView(healthy()).rows
  assert.equal(leader.lag, null)
  assert.equal(leader.heartbeatMs, null)
  assert.equal(follower.lag, 0)
  assert.equal(follower.heartbeatMs, 12)
  assert.equal(follower.lagSeverity, '')
  assert.equal(follower.heartbeatSeverity, '')
  // The figures arrive as sent, unrounded.
  assert.equal(follower.applied, 58972)
  assert.equal(follower.durable, 58960)
  assert.equal(follower.logBytes, 65559235)
  assert.equal(follower.mapPct, 0.038)
  assert.equal(follower.mapSeverity, '')
})

test('lag severity follows openraft, heartbeat severity the election timer', () => {
  const row = (over) => memberRow(member(2, over), { leaderId: 1, self: 2 })
  assert.equal(row({ lagEntries: 4_999 }).lagSeverity, '')
  assert.equal(row({ lagEntries: 5_000 }).lagSeverity, 'warn')
  assert.equal(row({ lagEntries: 9_000_000 }).lagSeverity, 'warn')
  assert.equal(row({ lagEntries: null }).lag, null)
  assert.equal(row({ heartbeatAgeMs: 650 }).heartbeatSeverity, 'warn')
  assert.equal(row({ heartbeatAgeMs: 2_400 }).heartbeatSeverity, 'bad')
  assert.equal(row({ store: { mapUsedPct: 86 } }).mapSeverity, 'bad')
})

// ---------------------------------------------------------------------------
// Unreachable members and the quorum
// ---------------------------------------------------------------------------

test('an unreachable member row carries its error and no figure at all', () => {
  const row = memberRow(down(3), { leaderId: 1, self: 2 })
  assert.equal(row.unreachable, true)
  assert.equal(row.state, 'unreachable')
  assert.deepEqual(row.chip, { cls: 'chip-bad', label: 'unreachable' })
  assert.equal(row.error, 'connect: connection refused')
  assert.equal(row.raftAddr, '127.0.0.1:7403')
  assert.equal(row.httpAddr, '127.0.0.1:6633')
  for (const k of ['term', 'applied', 'committed', 'durable', 'lag', 'heartbeatMs', 'logBytes', 'logFiles', 'mapPct']) {
    assert.equal(row[k], null, k)
  }
  // Unknown is not zero, and it has no colour.
  assert.equal(row.lagSeverity, '')
  assert.equal(row.heartbeatSeverity, '')
  assert.equal(row.mapSeverity, '')
  // The state may also arrive spelled out.
  assert.equal(memberRow({ ...down(3), state: 'unreachable', reachable: undefined }).unreachable, true)
})

test('one voter of three down: quorum holds, with nothing to spare', () => {
  const view = clusterView(members([member(1, { state: 'leader' }), member(2, { local: true }), down(3)]))
  assert.equal(view.quorum.up, 2)
  assert.equal(view.quorum.down, 1)
  assert.equal(view.quorum.hasQuorum, true)
  assert.equal(view.quorum.spare, 0)
  assert.equal(view.quorum.severity, 'warn')
  const alert = clusterAlert(view)
  assert.equal(alert.level, 'warn')
  assert.equal(alert.title, '1 of 3 voters unreachable')
  assert.match(alert.detail, /one more loss stops writes/)
})

test('a majority of voters down: quorum lost, and it says so first', () => {
  const view = clusterView(members([member(2, { local: true, state: 'candidate' }), down(1), down(3)], { leaderId: null }))
  assert.equal(view.quorum.up, 1)
  assert.equal(view.quorum.hasQuorum, false)
  assert.equal(view.quorum.severity, 'bad')
  const alert = clusterAlert(view)
  assert.equal(alert.level, 'bad')
  assert.equal(alert.title, 'Quorum lost')
  assert.match(alert.detail, /^1 of 3 voters reachable, 2 needed/)
  assert.deepEqual(view.rows[1].chip, { cls: 'chip-warn', label: 'candidate' })
})

test('five voters with one down can still lose one more', () => {
  const view = clusterView(members([
    member(1, { state: 'leader' }), member(2), member(3), member(4), down(5),
  ]))
  assert.equal(view.quorum.majority, 3)
  assert.equal(view.quorum.spare, 1)
  assert.match(clusterAlert(view).detail, /1 more can fail before writes stop/)
})

test('learners never vote, so a learner down moves no quorum', () => {
  const view = clusterView(members([
    member(1, { state: 'leader' }), member(2), member(3), down(4, 'timeout', 'learner'),
  ]))
  assert.equal(view.quorum.voters, 3)
  assert.equal(view.quorum.learners, 1)
  assert.equal(view.quorum.severity, '')
  assert.equal(clusterAlert(view), null)
  assert.equal(view.rows[3].role, 'learner')
  assert.equal(view.rows[3].unreachable, true)
})

test('a voter shut down answers, but does not count toward a majority', () => {
  const view = clusterView(members([member(1, { state: 'leader' }), member(2), member(3, { state: 'shutdown' })]))
  assert.equal(view.quorum.up, 2)
  assert.deepEqual(view.rows[2].chip, { cls: 'chip-bad', label: 'shutdown' })
  // It answered, so its figures are real — only its vote is gone.
  assert.equal(view.rows[2].unreachable, false)
  assert.equal(view.rows[2].applied, 58972)
})

test('a quorum with no leader is an election running', () => {
  const view = clusterView(members(
    [member(1), member(2, { local: true, state: 'candidate' }), member(3)],
    { leaderId: null, term: 3 },
  ))
  assert.equal(view.leader, null)
  const alert = clusterAlert(view)
  assert.equal(alert.level, 'warn')
  assert.equal(alert.title, 'No leader')
})

test('a single node is its own quorum, with nothing to replicate', () => {
  const view = clusterView(members([member(1, { state: 'leader', local: true })], { self: 1 }))
  assert.equal(view.singleNode, true)
  assert.equal(view.quorum.voters, 1)
  assert.equal(view.quorum.majority, 1)
  assert.equal(view.quorum.spare, 0)
  assert.equal(clusterAlert(view), null)
})

test('what is not a members payload is not a cluster', () => {
  assert.equal(clusterView(null), null)
  assert.equal(clusterView({}), null)
  assert.equal(clusterView({ members: 'three' }), null)
  assert.equal(clusterAlert(null), null)
  assert.deepEqual(quorumHealth(null), {
    voters: 0, up: 0, down: 0, learners: 0, majority: null, hasQuorum: null, spare: 0, severity: '',
  })
  assert.deepEqual(stateChip('joining'), { cls: 'chip-mute', label: 'joining' })
  assert.deepEqual(stateChip(null), { cls: 'chip-mute', label: 'unknown' })
})

// ---------------------------------------------------------------------------
// The Replicated log card (GET /api/v1/raft/status)
// ---------------------------------------------------------------------------

const status = (over = {}) => ({
  ...member(2, { local: true }),
  clusterId: 'raft-3f9a2c1e', leaderId: 1, self: 2, voters: 3, singleNode: false,
  ...over,
})

test('the Replicated log card: this node, where it stands, one tone', () => {
  const node = nodeView(status())
  assert.equal(node.nodeId, 2)
  assert.equal(node.hostname, 'node-2')
  assert.deepEqual(node.chip, { cls: 'chip-mute', label: 'follower' })
  assert.equal(node.leaderNote, 'leader is node 1')
  assert.equal(node.clusterNote, '3 voters in the cluster')
  assert.equal(node.applied, 58972)
  assert.equal(node.committed, 58972)
  assert.equal(node.durable, 58960)
  assert.equal(node.inflight, 0)
  assert.equal(node.logFiles, 708)
  assert.equal(node.mapBytes, 68719476736)
  assert.equal(node.severity, '')
})

test('the card warns without a leader and at the store gate, and says single node plainly', () => {
  const leaderless = nodeView(status({ leaderId: null, state: 'candidate' }))
  assert.equal(leaderless.leaderNote, 'no leader known')
  assert.equal(leaderless.leaderSeverity, 'warn')
  assert.equal(leaderless.severity, 'warn')

  assert.equal(nodeView(status({ store: { mapUsedPct: 85.2 } })).severity, 'bad')

  const single = nodeView(status({ nodeId: 1, self: 1, leaderId: 1, state: 'leader', voters: 1, singleNode: true }))
  assert.equal(single.leaderNote, 'this node leads')
  assert.equal(single.clusterNote, 'single node, not replicated')
  assert.deepEqual(single.chip, { cls: 'chip-ok', label: 'leader' })
  assert.equal(nodeView(null), null)
})

// ---------------------------------------------------------------------------
// Formatting and failures
// ---------------------------------------------------------------------------

test('log indexes never abbreviate; a small map share stays visible', () => {
  assert.equal(formatIndex(58_972), '58,972')
  // formatNumber would print 123M for all three columns.
  assert.equal(formatIndex(123_456_789), '123,456,789')
  assert.equal(formatIndex(0), '0')
  assert.equal(formatIndex(null), '—')
  assert.equal(formatIndex('x'), '—')

  assert.equal(formatMapPct(0.038), '0.04%')
  assert.equal(formatMapPct(0.004), '<0.01%')
  assert.equal(formatMapPct(0), '0%')
  assert.equal(formatMapPct(12.345), '12.3%')
  assert.equal(formatMapPct(85), '85.0%')
  assert.equal(formatMapPct(null), '—')
})

test('503 no_leader reads as a cluster state; anything else as the failure it is', async () => {
  const fail = async (body, status) => {
    const client = createApiClient({
      apiBaseUrl: 'https://queen.test',
      fetch: async () => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } }),
    })
    try { await client.get('/api/v1/raft/members') } catch (err) { return err }
    throw new Error('the call was expected to fail')
  }
  // server/src/handlers/raft.rs err_response(RsmError::NoLeader).
  const noLeader = await fail({ error: 'no leader', code: 'no_leader' }, 503)
  assert.equal(isNoLeaderError(noLeader), true)
  assert.match(describeRaftFailure(noLeader), /^no leader is elected/)

  const internal = await fail({ error: 'store read failed', code: 'internal' }, 500)
  assert.equal(isNoLeaderError(internal), false)
  assert.equal(describeRaftFailure(internal), 'Server error (HTTP 500)')
})
