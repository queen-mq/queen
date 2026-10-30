// System's traffic figures, held against the broker contract's own payloads:
// the `traffic` block of GET /api/v1/raft/members.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  RAFT_CHANNELS, formatRate, inUnit, memberTraffic, rateUnit, trafficTitle,
} from '../src/composables/useTraffic.js'

const MB = 1024 * 1024

test('a rate reads in the unit its size needs, unknown stays a dash', () => {
  assert.equal(formatRate(0), '0 B/s')
  assert.equal(formatRate(812), '812 B/s')
  assert.equal(formatRate(12.4 * 1024), '12.4 KB/s')
  assert.equal(formatRate(3.214 * MB), '3.21 MB/s')
  assert.equal(formatRate(250 * MB), '250 MB/s')
  assert.equal(formatRate(2 * 1024 * MB), '2 GB/s')
  assert.equal(formatRate(null), '—')
  assert.equal(formatRate(undefined), '—')
  assert.equal(formatRate('nope'), '—')
})

test('a chart takes the unit of its largest value and keeps its gaps', () => {
  assert.equal(rateUnit(0).label, 'B/s')
  assert.equal(rateUnit(null).label, 'B/s')
  assert.equal(rateUnit(900).label, 'B/s')
  assert.equal(rateUnit(5 * 1024).label, 'KB/s')
  assert.equal(rateUnit(40 * MB).label, 'MB/s')
  const mbs = rateUnit(40 * MB)
  assert.equal(inUnit(1.5 * MB, mbs), 1.5)
  assert.equal(inUnit(1234567, mbs), 1.18)
  assert.equal(inUnit(null, mbs), null)
})

// A member's block as local_node_json spells it (admin.rs, dashboard/mod.rs).
const block = {
  windowSeconds: 60,
  raft: {
    sentBytesPerSec: 92 * MB,
    receivedBytesPerSec: 1.5 * MB,
    replication: { sentBytesPerSec: 90 * MB, receivedBytesPerSec: 1024 },
    forward: { sentBytesPerSec: 0, receivedBytesPerSec: 1.4 * MB },
    snapshot: { sentBytesPerSec: 2 * MB, receivedBytesPerSec: 0 },
    link: { sentBytesPerSec: 0, receivedBytesPerSec: 0 },
    control: { sentBytesPerSec: 300, receivedBytesPerSec: 200 },
  },
  network: { receivedBytesPerSec: 20 * MB, sentBytesPerSec: 95 * MB },
  disk: { readBytesPerSec: 0, writtenBytesPerSec: 30 * MB },
}

test("a member's traffic block becomes its Raft table cell", () => {
  const t = memberTraffic({ nodeId: 1, traffic: block })
  assert.equal(t.sent, 92 * MB)
  assert.equal(t.received, 1.5 * MB)
  assert.equal(t.windowSeconds, 60)
  assert.deepEqual(t.channels.map((c) => c.key), RAFT_CHANNELS.map((c) => c.key))
  assert.equal(t.channels[0].sent, 90 * MB)
  assert.equal(t.channels[1].received, 1.4 * MB)
})

test('a member that says nothing has no cell, never zeros', () => {
  assert.equal(memberTraffic({ nodeId: 2 }), null)
  assert.equal(memberTraffic({ nodeId: 2, traffic: null }), null)
  assert.equal(memberTraffic({ nodeId: 2, state: 'unreachable', reachable: false }), null)
  assert.equal(memberTraffic(null), null)
  // A channel a newer broker adds, or one an older one lacks: null, not 0.
  const partial = memberTraffic({ traffic: { raft: { sentBytesPerSec: 5 } } })
  assert.equal(partial.received, null)
  assert.equal(partial.channels[0].sent, null)
})

test('the tooltip names the window and only the channels that moved bytes', () => {
  const title = trafficTitle(memberTraffic({ traffic: block }))
  assert.match(title, /^Raft port, last 60 s:/)
  assert.match(title, /Replication: 90 MB\/s sent, 1 KB\/s received/)
  assert.match(title, /Snapshots: 2 MB\/s sent, 0 B\/s received/)
  assert.doesNotMatch(title, /Standby link/)
  const idle = {
    windowSeconds: 60,
    raft: Object.fromEntries(RAFT_CHANNELS.map((c) => [c.key, { sentBytesPerSec: 0, receivedBytesPerSec: 0 }])),
  }
  assert.equal(trafficTitle(memberTraffic({ traffic: idle })), 'Nothing crossed the Raft port (last 60 s)')
  assert.equal(trafficTitle(null), '')
})
