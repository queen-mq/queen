// The colour policy, pinned.
//
// Every rule in src/composables/useSeverity.js exists to answer one question —
// "is this number a problem?" — and the two cases that started the rewrite are
// the first two assertions below: 90 failed acks in an hour on a cell doing
// ~14 msg/s is 0.18% of the work and must be plain ink, while the same 90
// against 900 acks is 9% and must be visible. Anything that moves a threshold
// has to move a line here too, which is the point: the numbers are a product
// decision, not an implementation detail.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  THRESHOLDS,
  ackFailureSeverity,
  backlogSeverity,
  consumerGroupSeverity,
  dlqGrowthSeverity,
  eventLoopSeverity,
  keepUpSeverity,
  lagMsSeverity,
  laggingPartitionsSeverity,
  lossSeverity,
  numTone,
  pendingDriftSeverity,
  quorumSeverity,
  raftHeartbeatSeverity,
  raftLagSeverity,
  storeMapSeverity,
  timeLagSeverity,
} from '../src/composables/useSeverity.js'

// ---------------------------------------------------------------------------
// Ack failures — the case the policy was written for
// ---------------------------------------------------------------------------

test('a healthy hour with a handful of ack failures is not amber', () => {
  // The production screenshot: 90 failed acks in an hour, ~14 msg/s of real
  // work behind them. The old rule was `ackFailed > 0 → warn`.
  assert.equal(ackFailureSeverity({ failed: 90, succeeded: 50_000 }), '')
  assert.equal(ackFailureSeverity({ failed: 0, succeeded: 50_000 }), '')
  assert.equal(ackFailureSeverity({ failed: 1, succeeded: 50_000 }), '')
})

test('the same 90 failures against 900 acks is attention', () => {
  // 9% of the work failing is a real signal — and it is 90 messages, so it is
  // attention, not an incident: red needs both a bad rate and real volume.
  assert.equal(ackFailureSeverity({ failed: 90, succeeded: 900 }), 'warn')
})

test('ack failures escalate to red on rate AND volume', () => {
  // 10% of 50 000 acks failing: bad rate, and 5 000 messages behind it.
  assert.equal(ackFailureSeverity({ failed: 5_000, succeeded: 45_000 }), 'bad')
  // Same 10%, but only 60 messages: attention.
  assert.equal(ackFailureSeverity({ failed: 60, succeeded: 540 }), 'warn')
  // Exactly at the two lines.
  assert.equal(ackFailureSeverity({ failed: 500, succeeded: 9_500 }), 'bad')
  assert.equal(ackFailureSeverity({ failed: 100, succeeded: 9_900 }), 'warn')
  assert.equal(ackFailureSeverity({ failed: 99, succeeded: 9_901 }), '')
})

test('ack failures fall back to absolutes only without a denominator', () => {
  // The window reported failures but no ack total at all.
  assert.equal(ackFailureSeverity({ failed: 90 }), '')
  assert.equal(ackFailureSeverity({ failed: 100 }), 'warn')
  assert.equal(ackFailureSeverity({ failed: 1_000 }), 'bad')
  // Too few failures to read a rate off: 3 out of 40 acks is three failures,
  // not a 7.5% failure rate.
  assert.equal(ackFailureSeverity({ failed: 3, succeeded: 37 }), '')
  // But a thin window with a solid rate IS called: 40% of the acks in the
  // window failed, and refusing to say so would be the opposite mistake.
  assert.equal(ackFailureSeverity({ failed: 64, succeeded: 96 }), 'warn')
  // `attempts` may be passed directly when the caller already has the total.
  assert.equal(ackFailureSeverity({ failed: 90, attempts: 50_090 }), '')
})

test('ack severity is total for missing and malformed inputs', () => {
  assert.equal(ackFailureSeverity(), '')
  assert.equal(ackFailureSeverity({}), '')
  assert.equal(ackFailureSeverity({ failed: null, succeeded: null }), '')
  assert.equal(ackFailureSeverity({ failed: undefined }), '')
  assert.equal(ackFailureSeverity({ failed: 'x', succeeded: 'y' }), '')
})

// ---------------------------------------------------------------------------
// Dead letters
// ---------------------------------------------------------------------------

test('a dead-letter queue that merely exists is neutral', () => {
  // Depth is not growth: nobody purges a DLQ, so `> 0` never goes out again.
  assert.equal(dlqGrowthSeverity({ added: 0, attempts: 50_000 }), '')
  assert.equal(dlqGrowthSeverity({ added: null, attempts: 50_000 }), '')
  assert.equal(dlqGrowthSeverity({}), '')
})

test('a dead-letter queue that grew notably in the window is attention', () => {
  assert.equal(dlqGrowthSeverity({ added: 900, attempts: 50_000 }), 'warn')
  assert.equal(dlqGrowthSeverity({ added: 90, attempts: 50_000 }), '')     // 0.18%
  assert.equal(dlqGrowthSeverity({ added: 5, attempts: 100 }), '')         // under the floor
  assert.equal(dlqGrowthSeverity({ added: 500 }), 'warn')                  // no denominator
  assert.equal(dlqGrowthSeverity({ added: 40 }), '')                       // no denominator
})

test('dead letters never go red', () => {
  // A dead letter is a message the system has already parked safely.
  assert.equal(dlqGrowthSeverity({ added: 1e6, attempts: 1e6 }), 'warn')
})

// ---------------------------------------------------------------------------
// Backlog and drift — counts turned into verdicts by a rate
// ---------------------------------------------------------------------------

test('a pending count is judged in seconds of work, not in messages', () => {
  // 50 000 pending at 12k/s drains in four seconds.
  assert.equal(backlogSeverity({ pending: 50_000, drainPerSec: 12_000 }), '')
  // The same 50 000 at 20/s is 40 minutes of work.
  assert.equal(backlogSeverity({ pending: 50_000, drainPerSec: 20 }), 'bad')
  assert.equal(backlogSeverity({ pending: 12_000, drainPerSec: 20 }), 'warn')  // 10 min
  assert.equal(backlogSeverity({ pending: 2_000, drainPerSec: 20 }), '')       // 100 s
})

test('a backlog with no measured drain has no verdict here', () => {
  // Nothing is draining — that is the Time lag row's story (an age), not a
  // count's, and painting both would double-report one fact.
  assert.equal(backlogSeverity({ pending: 1e6, drainPerSec: 0 }), '')
  assert.equal(backlogSeverity({ pending: 1e6, drainPerSec: null }), '')
  assert.equal(backlogSeverity({ pending: 50, drainPerSec: 0.01 }), '')  // under the floor
  assert.equal(backlogSeverity(), '')
})

test('push-vs-ack drift is a share of what arrived', () => {
  // +1 000 against 500 000 pushed is 0.2%: the window closed mid-flight.
  assert.equal(pendingDriftSeverity({ delta: 1_000, pushed: 500_000 }), '')
  assert.equal(pendingDriftSeverity({ delta: 60_000, pushed: 500_000 }), 'warn')  // 12%
  assert.equal(pendingDriftSeverity({ delta: 300_000, pushed: 500_000 }), 'bad')  // 60%
  // Catching up is a positive state and keeps its green.
  assert.equal(pendingDriftSeverity({ delta: -5_000, pushed: 500_000 }), 'ok')
  // Noise floor, both directions.
  assert.equal(pendingDriftSeverity({ delta: 40, pushed: 100 }), '')
  assert.equal(pendingDriftSeverity({ delta: -40, pushed: 100 }), '')
  assert.equal(pendingDriftSeverity({ delta: null, pushed: 100 }), '')
})

// ---------------------------------------------------------------------------
// Measures that are already proportional — kept, because they describe real
// degradation at any scale
// ---------------------------------------------------------------------------

test('message age keeps its thresholds: a duration is scale-free', () => {
  assert.equal(timeLagSeverity(null), 'mute')
  assert.equal(timeLagSeverity(0), 'mute')
  assert.equal(timeLagSeverity(5), 'ok')
  assert.equal(timeLagSeverity(59), 'ok')
  assert.equal(timeLagSeverity(60), 'warn')
  assert.equal(timeLagSeverity(299), 'warn')
  assert.equal(timeLagSeverity(300), 'bad')
})

test('queue lag in ms ranks fresh / quiet / late / stalled', () => {
  assert.equal(lagMsSeverity(null), 'mute')
  assert.equal(lagMsSeverity(400), 'ok')
  assert.equal(lagMsSeverity(5_000), 'mute')
  assert.equal(lagMsSeverity(30_000), 'warn')
  assert.equal(lagMsSeverity(90_000), 'bad')
})

test('event loop lag is host degradation, and stays', () => {
  assert.equal(eventLoopSeverity(0), '')
  assert.equal(eventLoopSeverity(12), '')
  assert.equal(eventLoopSeverity(50), 'warn')
  assert.equal(eventLoopSeverity(100), 'bad')
})

// ---------------------------------------------------------------------------
// Keeping up, loss, consumer groups
// ---------------------------------------------------------------------------

test('a queue level to within sampling jitter is not "elevated"', () => {
  // The old rule called 0.84 elevated. On a level queue that is the bucket
  // boundary, not the queue.
  assert.equal(keepUpSeverity({ pop: 84, push: 100 }), 'mute')
  assert.equal(keepUpSeverity({ pop: 100, push: 100 }), 'ok')
  assert.equal(keepUpSeverity({ pop: 70, push: 100 }), 'warn')
  assert.equal(keepUpSeverity({ pop: 20, push: 100 }), 'bad')
  // Too little traffic to judge at all.
  assert.equal(keepUpSeverity({ pop: 0.2, push: 0.5 }), 'mute')
  assert.equal(keepUpSeverity({ pop: 0, push: 0 }), 'mute')
  assert.equal(keepUpSeverity({ pop: 40, push: 0 }), 'ok')
})

test('eviction is attention only once it is a real share of what was read', () => {
  assert.equal(lossSeverity({ dropped: 0, delivered: 1_000 }), '')
  assert.equal(lossSeverity({ dropped: 2, delivered: 100_000 }), '')
  assert.equal(lossSeverity({ dropped: 400, delivered: 1_000 }), 'warn')
  assert.equal(lossSeverity({ dropped: 5 }), 'warn')   // no denominator: loss is loss
  assert.equal(lossSeverity({}), '')
})

test('a consumer group that is behind is a real state and keeps its colour', () => {
  assert.equal(consumerGroupSeverity({ state: 'Stable', maxTimeLag: 0 }), 'ok')
  assert.equal(consumerGroupSeverity({ state: 'Dead', maxTimeLag: 9_000 }), 'mute')
  assert.equal(consumerGroupSeverity({ state: 'Lagging', maxTimeLag: 10 }), 'warn')
  assert.equal(consumerGroupSeverity({ state: 'Lagging', maxTimeLag: 400 }), 'bad')
  assert.equal(consumerGroupSeverity({ state: 'Stable', maxTimeLag: 120 }), 'warn')
})

test('one partition momentarily behind is what working looks like', () => {
  assert.equal(laggingPartitionsSeverity({ behind: 1, total: 64 }), 'mute')
  assert.equal(laggingPartitionsSeverity({ behind: 20, total: 64 }), 'warn')
  assert.equal(laggingPartitionsSeverity({ behind: 60, total: 64 }), 'bad')
  assert.equal(laggingPartitionsSeverity({ behind: 3, total: null }), 'mute')
  assert.equal(laggingPartitionsSeverity({ behind: 0, total: 64 }), 'mute')
})

// ---------------------------------------------------------------------------
// Raft mode — every line is the broker's own, not one invented here
// ---------------------------------------------------------------------------

test("a follower's lag is judged on openraft's own lagging line, and never red", () => {
  assert.equal(THRESHOLDS.raftLagWarnEntries, 5_000)   // openraft replication_lag_threshold
  assert.equal(raftLagSeverity(0), '')
  assert.equal(raftLagSeverity(64), '')                // one AppendEntries payload in flight
  assert.equal(raftLagSeverity(4_999), '')
  assert.equal(raftLagSeverity(5_000), 'warn')
  // Redundancy reduced, not an outage: that is the quorum's call to make.
  assert.equal(raftLagSeverity(50_000_000), 'warn')
  assert.equal(raftLagSeverity(null), '')
  assert.equal(raftLagSeverity(undefined), '')
})

test('heartbeat age is a duration against the cluster timers', () => {
  // 100 ms heartbeats, a 1-2 s election timeout (replicator/raft raft_config).
  assert.equal(raftHeartbeatSeverity(12), '')
  assert.equal(raftHeartbeatSeverity(499), '')
  assert.equal(raftHeartbeatSeverity(500), 'warn')
  assert.equal(raftHeartbeatSeverity(1_999), 'warn')
  assert.equal(raftHeartbeatSeverity(2_000), 'bad')
  assert.equal(raftHeartbeatSeverity(null), '')
})

test("the store map follows the broker's refusal gate", () => {
  // MAP_LOW_PCT / MAP_HIGH_PCT in server/src/rsm/store/mod.rs.
  assert.equal(THRESHOLDS.storeMapWarnPct, 80)
  assert.equal(THRESHOLDS.storeMapBadPct, 85)
  assert.equal(storeMapSeverity(0.038), '')            // a 64 GiB map, 25 MB used
  assert.equal(storeMapSeverity(79.9), '')
  assert.equal(storeMapSeverity(80), 'warn')
  assert.equal(storeMapSeverity(85), 'bad')            // pushes answer 507 storage_full
  assert.equal(storeMapSeverity(null), '')
})

test('a quorum is broken only below a majority of voters', () => {
  assert.equal(quorumSeverity({ up: 3, voters: 3 }), '')
  assert.equal(quorumSeverity({ up: 2, voters: 3 }), 'warn')
  assert.equal(quorumSeverity({ up: 1, voters: 3 }), 'bad')
  assert.equal(quorumSeverity({ up: 3, voters: 5 }), 'warn')
  assert.equal(quorumSeverity({ up: 2, voters: 5 }), 'bad')
  assert.equal(quorumSeverity({ up: 1, voters: 1 }), '')
  assert.equal(quorumSeverity({ up: 0, voters: 1 }), 'bad')
  // Two voters have no failure to spare by construction: one down is broken.
  assert.equal(quorumSeverity({ up: 1, voters: 2 }), 'bad')
  assert.equal(quorumSeverity({ up: 0, voters: 0 }), '')
  assert.equal(quorumSeverity({}), '')
})

// ---------------------------------------------------------------------------
// Tone plumbing
// ---------------------------------------------------------------------------

test('only attention and failure repaint a number', () => {
  assert.equal(numTone('warn'), 'warn')
  assert.equal(numTone('bad'), 'bad')
  assert.equal(numTone('ok'), '')
  assert.equal(numTone('mute'), '')
  assert.equal(numTone(''), '')
  assert.equal(numTone(undefined), '')
})

test('thresholds are frozen so a call site cannot edit the policy', () => {
  assert.equal(Object.isFrozen(THRESHOLDS), true)
  assert.equal(THRESHOLDS.ackWarnRate, 0.01)
  assert.equal(THRESHOLDS.ackBadRate, 0.05)
})
