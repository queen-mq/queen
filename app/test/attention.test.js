// "What needs you", pinned. The Overview, the sidebar, Queues and Consumer
// groups all read this one rule; a change here moves all four at once.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  byLagInactiveLast, groupAttention, groupPresence, groupState, queueAttention, summarize,
} from '../src/composables/useAttention.js'
import { THRESHOLDS } from '../src/composables/useSeverity.js'

const q = (name, pending) => ({ name, messages: { pending } })
const g = (queueName, maxTimeLag, state = 'Stable') => ({ name: `${queueName}-g`, queueName, maxTimeLag, state })
// A group read through the Kafka facade, as the broker lists it once the
// facade reports presence: `inactiveSeconds` 0 while it is there.
const kafka = (queueName, maxTimeLag, inactiveSeconds, more = {}) => ({
  name: `${queueName}-kafka`, queueName, maxTimeLag, kind: 'kafka',
  state: inactiveSeconds === 0 ? 'Stable' : 'Empty',
  liveMembers: inactiveSeconds === 0 ? 3 : 0,
  lastSeenAt: inactiveSeconds === 0 ? null : '2026-10-19T09:00:00.000Z',
  inactiveSeconds,
  ...more,
})
const queueMode = (queueName, maxTimeLag, state = 'Dead') => ({ name: '__QUEUE_MODE__', queueName, maxTimeLag, state })

test('a group is judged by the age of what it has not read', () => {
  assert.equal(groupAttention(g('a', 0)), 'ok')
  assert.equal(groupAttention(g('a', 59)), 'ok')
  assert.equal(groupAttention(g('a', 60)), 'warn')
  assert.equal(groupAttention(g('a', 300)), 'bad')
})

test("a queue with its own lines is judged by them, and its neighbours are not", () => {
  const slow = { lagWarnSeconds: 600, lagBadSeconds: 1800 }
  const linesFor = (queue) => (queue === 'batch' ? slow : undefined)
  assert.equal(groupAttention(g('batch', 400, 'Lagging'), linesFor), 'ok')
  assert.equal(groupAttention(g('batch', 700), linesFor), 'warn')
  assert.equal(groupAttention(g('live', 400), linesFor), 'bad')
  const out = queueAttention([q('batch', 10), q('live', 10)], [g('batch', 400), g('live', 400)], linesFor)
  assert.deepEqual(out.map((i) => [i.name, i.sev]), [['live', 'bad']])
  // Handed straight to Array.map, the index must not be read as a lookup.
  assert.deepEqual([g('a', 0), g('a', 90)].map(groupAttention), ['ok', 'warn'])
})

test('a queue nobody is expected to read is not flagged for having no reader', () => {
  const linesFor = (queue) => (queue === 'archive' ? { lagWarnSeconds: 60, lagBadSeconds: 300, noReaderOk: true } : undefined)
  const out = queueAttention([q('archive', 83), q('orders', 83)], [], linesFor)
  assert.deepEqual(out.map((i) => [i.name, i.reason]), [['orders', 'noReader']])
  // The flag is about readers only: a group that IS there and is behind still counts.
  const late = queueAttention([q('archive', 83)], [g('archive', 400)], linesFor)
  assert.deepEqual(late.map((i) => [i.name, i.sev, i.reason]), [['archive', 'bad', 'lag']])
})

test('a group that has never consumed is not an alarm', () => {
  assert.equal(groupAttention(g('a', 90_000, 'Dead')), 'mute')
})

test('a queue takes the worst of its live groups', () => {
  const out = queueAttention([q('a', 10)], [g('a', 5), g('a', 120), g('a', 400)])
  assert.deepEqual(out.map((i) => [i.name, i.sev, i.reason, i.lag]), [['a', 'bad', 'lag', 400]])
})

test('messages with nobody to read them are attention, not failure', () => {
  const out = queueAttention([q('a', 83)], [])
  assert.deepEqual(out.map((i) => [i.name, i.sev, i.reason, i.deadOnly]), [['a', 'warn', 'noReader', false]])
})

test('a named group that has never consumed tells its own story, not the queue', () => {
  // A subscription somebody made: its row says "never consumed", and the
  // queue's pending is that group's backlog, not unread messages.
  assert.deepEqual(queueAttention([q('a', 5)], [g('a', 90_000, 'Dead')]), [])
  assert.deepEqual(queueAttention([q('a', 5)], [queueMode('a', 90_000), g('a', 90_000, 'Dead')]), [])
})

test('a queue whose only reader is a queue-mode cursor that never consumed has no reader', () => {
  const out = queueAttention([q('a', 5)], [queueMode('a', 90_000)])
  assert.deepEqual(out.map((i) => [i.sev, i.reason, i.deadOnly]), [['warn', 'noReader', true]])
  // A queue-mode cursor that has consumed is a reader like any group.
  assert.deepEqual(queueAttention([q('a', 5)], [queueMode('a', 2, 'Stable')]), [])
})

test('a queue Kafka clients created is not unread for having no group yet', () => {
  const out = queueAttention([{ ...q('topic', 83), kafkaTopic: true }, q('orders', 83)], [])
  assert.deepEqual(out.map((i) => [i.name, i.reason]), [['orders', 'noReader']])
})

test('"no reader expected" still lets a native queue off', () => {
  const linesFor = (queue) => (queue === 'audit' ? { ...THRESHOLDS, noReaderOk: true } : THRESHOLDS)
  assert.deepEqual(queueAttention([q('audit', 83)], [], linesFor), [])
  assert.deepEqual(queueAttention([q('audit', 83)], [queueMode('audit', 9_000)], linesFor), [])
})

// ---------------------------------------------------------------------------
// Kafka groups: presence, not cursors
// ---------------------------------------------------------------------------

test('a live Kafka group is a reader, judged by its lag like any group', () => {
  // The case that started it: consumed through the facade, two seconds behind,
  // listed by the old broker as Dead and flagged "No reader · 83 pending".
  const live = kafka('orders', 2, 0)
  assert.equal(groupAttention(live), 'ok')
  assert.equal(groupState(live), 'ok')
  assert.deepEqual(queueAttention([q('orders', 83)], [live]), [])
  assert.equal(groupAttention(kafka('orders', 400, 0, { state: 'Lagging' })), 'bad')
  // A client that assigns its own partitions commits offsets with no members: live.
  assert.equal(groupAttention(kafka('orders', 2, 0, { liveMembers: 0 })), 'ok')
})

test('a Kafka group with no live member is judged by its lag through the grace', () => {
  // Ten minutes without a member: a restart, a rebalance, a crash not yet called.
  const away = kafka('orders', 400, 600)
  assert.equal(groupAttention(away), 'bad')
  assert.equal(groupState(away), 'bad')
  const out = queueAttention([q('orders', 83)], [away])
  assert.deepEqual(out.map((i) => [i.name, i.sev, i.reason, i.lag]), [['orders', 'bad', 'lag', 400]])
  // Not behind, and nobody there: not "Stable".
  assert.equal(groupAttention(kafka('orders', 5, 600)), 'ok')
  assert.equal(groupState(kafka('orders', 5, 600)), 'empty')
})

test('past the grace a Kafka group is inactive: mute, and its queue raises nothing', () => {
  const gone = kafka('orders', 90_000, THRESHOLDS.inactiveAfterSeconds)
  assert.equal(groupAttention(gone), 'mute')
  assert.equal(groupState(gone), 'inactive')
  assert.deepEqual(queueAttention([q('orders', 83)], [gone]), [])
  // Beside a live group, the queue is that group's: neither its verdict nor its lag.
  const out = queueAttention([q('orders', 83)], [gone, kafka('orders', 90, 0)])
  assert.deepEqual(out.map((i) => [i.sev, i.reason, i.lag]), [['warn', 'lag', 90]])
})

test("a queue's own grace is the one its Kafka groups are judged by", () => {
  const linesFor = (queue) => (queue === 'nightly' ? { ...THRESHOLDS, inactiveAfterSeconds: 3600 } : THRESHOLDS)
  // Twenty minutes away, two hours behind.
  assert.equal(groupAttention(kafka('nightly', 7200, 1200), linesFor), 'bad')
  assert.equal(groupAttention(kafka('orders', 7200, 1200), linesFor), 'mute')
  const out = queueAttention([q('nightly', 9), q('orders', 9)],
    [kafka('nightly', 7200, 1200), kafka('orders', 7200, 1200)], linesFor)
  assert.deepEqual(out.map((i) => [i.name, i.sev]), [['nightly', 'bad']])
})

test('a Kafka group with nothing to date it by is inactive, whatever its state', () => {
  // No presence at all (an older facade, presence off): the old listing.
  const unknown = kafka('orders', 9_000, null, { state: 'Dead', liveMembers: null, lastSeenAt: null })
  assert.equal(groupAttention(unknown), 'mute')
  assert.equal(groupState(unknown), 'inactive')
  // Alive, but its members no longer subscribe to this queue.
  const left = kafka('orders', 9_000, null, { state: 'Empty', liveMembers: 0, lastSeenAt: null })
  assert.equal(groupAttention(left), 'mute')
  // Neither is a queue with no reader: the group is named.
  assert.deepEqual(queueAttention([q('orders', 83)], [unknown]), [])
})

test("an inactive Kafka group's presence is said, on the broker's clock", () => {
  assert.equal(groupPresence(kafka('orders', 2, 0)), '')
  assert.equal(groupPresence(g('orders', 2)), '')
  assert.equal(groupPresence(kafka('orders', 5, 240)), 'last seen 4m ago')
  assert.equal(groupPresence(kafka('orders', 5, 10_800)), 'last seen 3h ago')
  assert.equal(groupPresence(kafka('orders', 5, null, { state: 'Empty', liveMembers: 0, lastSeenAt: null })),
    'not subscribed by its members')
  assert.equal(groupPresence(kafka('orders', 5, null, { state: 'Dead', liveMembers: null, lastSeenAt: null })),
    'no member data')
})

test('an empty queue with no reader, and a queue kept up with, need nothing', () => {
  assert.deepEqual(queueAttention([q('empty', 0), q('fine', 40)], [g('fine', 2)]), [])
})

test('a missing pending count is not a backlog', () => {
  assert.deepEqual(queueAttention([{ name: 'a' }], []), [])
})

test('the input order is kept, so a caller can sort by depth first', () => {
  const out = queueAttention([q('b', 9), q('a', 1)], [])
  assert.deepEqual(out.map((i) => i.name), ['b', 'a'])
})

test('a list by lag puts the inactive Kafka groups last, each part still by lag', () => {
  const gone = { ...kafka('orders', 90_000, THRESHOLDS.inactiveAfterSeconds), name: 'gone' }
  const undated = { ...kafka('audit', 50_000, null, { state: 'Dead', liveMembers: null }), name: 'undated' }
  const behind = { ...g('orders', 400), name: 'behind' }
  const away = { ...kafka('orders', 120, 600), name: 'away' } // inside the grace: still judged
  const fine = { ...kafka('audit', 2, 0), name: 'fine' }
  const tie = { ...g('audit', 2), name: 'tie' }
  const input = [gone, fine, undated, behind, tie, away]
  const sorted = byLagInactiveLast(input)
  assert.deepEqual(sorted.map((x) => x.name), ['behind', 'away', 'fine', 'tie', 'gone', 'undated'])
  assert.deepEqual(input.map((x) => x.name), ['gone', 'fine', 'undated', 'behind', 'tie', 'away'], 'the input is not reordered')
  // A queue's own grace decides which part a group is in.
  const short = (queue) => (queue === 'orders' ? { ...THRESHOLDS, inactiveAfterSeconds: 300 } : undefined)
  assert.deepEqual(byLagInactiveLast([away, behind], short).map((x) => x.name), ['behind', 'away'])
  assert.deepEqual(byLagInactiveLast([fine, away], short).map((x) => x.name), ['fine', 'away'])
  assert.deepEqual(byLagInactiveLast(undefined), [])
})

test('summarize reports the worst and counts only what needs you', () => {
  assert.deepEqual(summarize(['ok', 'warn', 'mute', 'bad', 'warn']), { sev: 'bad', count: 3 })
  assert.deepEqual(summarize(['ok', 'mute']), { sev: null, count: 0 })
})
