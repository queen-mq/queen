// The Overview's broker strip, held against GET /api/v1/raft/members payloads
// whose members carry `host` (server/src/rsm/facade/real/phase2/admin.rs).

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { brokerStatus, hostReading } from '../src/composables/useBrokerStatus.js'

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
