// The Overview's broker strip, held against GET /api/v1/raft/members payloads
// whose members carry `host` (server/src/rsm/facade/real/phase2/admin.rs).

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { brokerStatus, cellIssues, hostReading } from '../src/composables/useBrokerStatus.js'

const GiB = 1024 ** 3

const host = (over = {}) => ({
  cpuPct: 150,           // one and a half cores busy
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
  hostname: `node-${nodeId}`,
  term: 14,
  lagEntries: 0,
  heartbeatAgeMs: 12,
  host: host(),
  ...over,
})

test('a single node: shares of what it may use, and its own write gate', () => {
  const s = brokerStatus({ leaderId: 1, term: 3, self: 1, members: [member(1, { state: 'leader', lagEntries: 0, heartbeatAgeMs: null })] })
  assert.equal(s.nodes.length, 1)
  assert.equal(s.cpu.top.share, 150 / 400)
  assert.equal(s.cpu.top.cpus, 4)
  assert.equal(s.mem.top.share, 0.25)
  assert.equal(s.disk.top.usedPct, 41)
  assert.equal(s.disk.top.highPct, 85)
  assert.equal(s.sev, '')
  assert.equal(s.raft.single, true)
  assert.equal(s.raft.leader, 'node-1')
  assert.equal(s.raft.term, 3)
})

test('the fullest node speaks for a measure; every node gets a meter', () => {
  const s = brokerStatus({
    leaderId: 1,
    term: 14,
    members: [
      member(3, { host: host({ cpuPct: 50 }) }),
      member(1, { state: 'leader', lagEntries: null }),
      member(2, { host: host({ cpuPct: 380, rssBytes: 7.5 * GiB }) }),
    ],
  })
  assert.deepEqual(s.nodes.map((n) => n.name), ['node-1', 'node-2', 'node-3'])
  assert.equal(s.cpu.top.name, 'node-2')
  assert.equal(s.cpu.sev, 'warn')                 // 95% of its cores: saturated
  assert.equal(s.mem.sev, 'bad')                  // 7.5 of 8 GiB
  assert.deepEqual(s.cpu.per.map((p) => p.share), [150 / 400, 380 / 400, 50 / 400])
  assert.equal(s.raft.single, false)
  assert.equal(s.raft.voters, 3)
  assert.equal(s.raft.up, 3)
  assert.equal(s.sev, 'bad')
})

test('memory is what the process holds when the broker says so, else its resident set', () => {
  // 2.0.1 reports anonBytes: the resident set less the file pages it maps (the
  // store's LMDB file), which the kernel drops before it kills anything.
  const now = hostReading(member(1, { host: host({ rssBytes: 7.5 * GiB, anonBytes: 2 * GiB }) }))
  assert.equal(now.mem.held, 2 * GiB)
  assert.equal(now.mem.share, 0.25)
  assert.equal(now.mem.sev, '')
  const older = hostReading(member(1, { host: host({ rssBytes: 7.5 * GiB }) }))
  assert.equal(older.mem.held, 7.5 * GiB)
  assert.equal(older.mem.sev, 'bad')
})

test('an unreachable member is unknown, not idle, and costs the quorum a voter', () => {
  const s = brokerStatus({
    leaderId: 1,
    term: 14,
    members: [
      member(1, { state: 'leader' }),
      member(2),
      { nodeId: 3, role: 'voter', state: 'unreachable', reachable: false, hostname: null, error: 'timeout' },
    ],
  })
  const three = s.nodes[2]
  assert.equal(three.unreachable, true)
  assert.equal(three.cpu.share, null)
  assert.equal(three.disk.usedPct, null)
  assert.equal(s.cpu.per.length, 3)
  assert.equal(s.raft.up, 2)
  assert.equal(s.raft.sev, 'warn')
})

test('a closed disk gate is red even below its line', () => {
  const r = hostReading({ host: host({ disk: { usedPct: 82, gate: true, highPct: 85, lowPct: 80, writesRefused: true } }) })
  assert.equal(r.disk.sev, 'bad')
  assert.equal(hostReading({ host: host({ disk: { usedPct: 82, gate: true, highPct: 85, lowPct: 80 } }) }).disk.sev, 'warn')
})

test('a broker older than `host` has readings of nothing', () => {
  const r = hostReading({ nodeId: 1, state: 'leader' })
  assert.equal(r.cpu.share, null)
  assert.equal(r.mem.share, null)
  assert.equal(r.disk.share, null)
  assert.equal(r.disk.sev, '')
  assert.equal(brokerStatus(null), null)
  assert.equal(brokerStatus({ members: 'nope' }), null)
})

// ---------------------------------------------------------------------------
// The cell's issues on the Overview: the top bar's lines, plus the snapshot.
// ---------------------------------------------------------------------------

const MB = 1024 * 1024
const traffic = (over = {}) => ({
  windowSeconds: 60,
  raft: {
    sentBytesPerSec: 0,
    receivedBytesPerSec: 0,
    replication: { sentBytesPerSec: 0, receivedBytesPerSec: 0 },
    snapshot: { sentBytesPerSec: 0, receivedBytesPerSec: 0 },
    ...over,
  },
})
const three = (overs = {}) => ({
  leaderId: 1,
  term: 14,
  members: [
    member(1, { state: 'leader', lagEntries: null, heartbeatAgeMs: null, traffic: traffic(), ...(overs[1] || {}) }),
    member(2, { traffic: traffic(), ...(overs[2] || {}) }),
    member(3, { traffic: traffic(), ...(overs[3] || {}) }),
  ],
})
const issuesOf = (payload) => cellIssues(brokerStatus(payload))

test('a healthy cell has no issues, and the wire is every node sent, summed', () => {
  const s = brokerStatus(three({
    1: { traffic: traffic({ sentBytesPerSec: 12 * MB }) },
    2: { traffic: traffic({ sentBytesPerSec: 1 * MB }) },
  }))
  assert.deepEqual(cellIssues(s), [])
  assert.equal(s.raft.wire, 13 * MB)
  // No node reports traffic (an older build): the wire is unknown, not 0.
  assert.equal(brokerStatus({ leaderId: 1, members: [member(1, { state: 'leader' })] }).raft.wire, null)
  assert.deepEqual(cellIssues(null), [])
})

test('a snapshot names the node it goes to, or the sender when that node says nothing', () => {
  const to3 = issuesOf(three({
    1: { traffic: traffic({ snapshot: { sentBytesPerSec: 85 * MB, receivedBytesPerSec: 0 } }) },
    3: { traffic: traffic({ snapshot: { sentBytesPerSec: 0, receivedBytesPerSec: 85 * MB } }) },
  }))
  assert.equal(to3.length, 1)
  assert.equal(to3[0].sev, 'warn')
  assert.equal(to3[0].name, 'node-3')
  assert.equal(to3[0].title, 'node-3 is being sent a snapshot')
  assert.match(to3[0].why, /85 MB\/s over the last 60 s/)
  assert.equal(to3[0].to, '/system')
  const fromLeader = issuesOf(three({
    1: { traffic: traffic({ snapshot: { sentBytesPerSec: 40 * MB, receivedBytesPerSec: 0 } }) },
    3: { traffic: undefined },
  }))
  assert.equal(fromLeader.length, 1)
  assert.equal(fromLeader[0].title, 'node-1 is sending a snapshot')
})

test('a follower behind or silent, by the same lines as the Raft table', () => {
  const behind = issuesOf(three({ 2: { lagEntries: 12000 } }))
  assert.deepEqual(behind.map((i) => [i.name, i.sev, i.why]), [['node-2', 'warn', '12,000 log entries behind']])
  const silent = issuesOf(three({ 3: { heartbeatAgeMs: 2600 } }))
  assert.deepEqual(silent.map((i) => [i.title, i.sev, i.why]), [['node-3 stopped answering the leader', 'bad', 'The leader last heard from it 2.6 s ago']])
  assert.equal(issuesOf(three({ 3: { heartbeatAgeMs: 700 } }))[0].sev, 'warn')
})

test('quorum: one voter down is amber, a lost majority red, and a down node is not its own issue', () => {
  const down = (id) => ({ state: 'unreachable', reachable: false, host: undefined, traffic: undefined, error: 'connect refused' })
  const one = issuesOf(three({ 3: down(3) }))
  assert.equal(one.length, 1)
  assert.equal(one[0].name, 'Raft')
  assert.equal(one[0].sev, 'warn')
  const lost = issuesOf(three({ 2: down(2), 3: down(3) }))
  assert.equal(lost[0].sev, 'bad')
  assert.equal(lost[0].title, 'Quorum lost')
})

test('disk, memory and CPU past their lines, worst first', () => {
  const list = issuesOf(three({
    1: { host: host({ cpuPct: 390 }) },
    2: { host: host({ disk: { totalBytes: 100 * GiB, usedBytes: 86 * GiB, usedPct: 86, gate: true, highPct: 85, lowPct: 80, writesRefused: true } }) },
    3: { host: host({ rssBytes: 7.4 * GiB }) },
  }))
  assert.equal(list[0].sev, 'bad')
  assert.equal(list[0].title, 'node-2 refuses writes: its disk is full')
  assert.match(list[0].why, /writes answer 507/)
  const titles = list.map((i) => i.title)
  assert.ok(titles.includes('node-1 is busy'))
  assert.ok(titles.includes('node-3 is short of memory'))
  // Every rank after the first is no worse than the one before it.
  const rank = { bad: 2, warn: 1 }
  list.reduce((prev, i) => { assert.ok(rank[i.sev] <= rank[prev.sev]); return i })
})

test('a problem several nodes share is one row naming them, not one per node', () => {
  const nearlyFull = { totalBytes: 100 * GiB, usedBytes: 84 * GiB, usedPct: 84, gate: true, highPct: 85, lowPct: 80, writesRefused: false }
  const list = issuesOf(three({
    1: { host: host({ disk: nearlyFull }) },
    2: { host: host({ disk: nearlyFull }) },
    3: { host: host({ disk: nearlyFull }) },
  }))
  assert.equal(list.length, 1)
  assert.equal(list[0].name, 'node-1, node-2, node-3')
  assert.equal(list[0].title, "3 nodes' disks are nearly full")
  assert.match(list[0].why, /Disk 84% used/)
  // Different words (84% and 86%) stay two rows: they are not the same reading.
  const apart = issuesOf(three({
    1: { host: host({ disk: nearlyFull }) },
    2: { host: host({ disk: { ...nearlyFull, usedBytes: 86 * GiB, usedPct: 86, writesRefused: true } }) },
  }))
  assert.equal(apart.length, 2)
  assert.equal(apart[0].title, 'node-2 refuses writes: its disk is full')
})
