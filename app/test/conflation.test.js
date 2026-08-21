// Conflation rules for the console (PLAN_CONFLATION §5.4).
//
// The webapp had no test suite at all: `app/package.json` declared dev / build /
// preview and nothing else. This file starts one in the shape the rest of the
// repo already uses for JavaScript — `node --test` with `node:assert/strict`,
// like clients/client-js/test-v2/*-unit — and adds no dependency to do it.
//
// What is covered is the decision each of the three touched surfaces makes, not
// the markup around it: whether the conflation badge appears, and whether the
// "High pending depth" advisory is allowed to go quiet. Both are pure functions
// in @/composables/useConflation precisely so they can be asserted here instead
// of only by eye.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  isConflating,
  groupKey,
  conflatingKeys,
  partitionConflates,
  isGroupLagging,
  laggingGroupsVerdict,
  pendingDepthAdvisory,
  PENDING_DEPTH_FLOOR,
} from '../src/composables/useConflation.js'

// ---------------------------------------------------------------------------
// The badge
// ---------------------------------------------------------------------------

test('badge: only an explicit true conflates', () => {
  assert.equal(isConflating({ conflation: true }), true)
  assert.equal(isConflating({ conflation: false }), false)
  // An older broker omits the field entirely; a group listing that predates
  // §2.6 must read as "not conflating", never as unknown-so-probably-yes.
  assert.equal(isConflating({}), false)
  assert.equal(isConflating(null), false)
  assert.equal(isConflating(undefined), false)
  // Truthy is not true: 'false', 1 and 'yes' are all wire accidents.
  assert.equal(isConflating({ conflation: 'true' }), false)
  assert.equal(isConflating({ conflation: 1 }), false)
})

test('badge: the group identity is (name, queue), not the name', () => {
  // conflation is declared per (group, queue): the same group may conflate on
  // one queue and not on another, and the badge must follow the pair.
  const groups = [
    { name: 'recompute', queueName: 'entities', conflation: true },
    { name: 'recompute', queueName: 'audit', conflation: false },
  ]
  const keys = conflatingKeys(groups)
  assert.equal(keys.size, 1)
  assert.ok(keys.has(groupKey('recompute', 'entities')))
  assert.ok(!keys.has(groupKey('recompute', 'audit')))
})

test('badge: the key separator cannot be forged out of a name', () => {
  // `a@b` on `c` and `a` on `b@c` are different groups. With '@' as the
  // separator they would share one key and one badge.
  assert.notEqual(groupKey('a@b', 'c'), groupKey('a', 'b@c'))
})

test('badge: a lagging-partition row is labelled from the group listing', () => {
  // The lagging payload (get_lagging_partitions_v1) carries no conflation field
  // and snake_case names, so the table joins it to the listing the page holds.
  const keys = conflatingKeys([
    { name: 'recompute', queueName: 'entities', conflation: true },
  ])
  assert.equal(
    partitionConflates({ consumer_group: 'recompute', queue_name: 'entities' }, keys),
    true,
  )
  assert.equal(
    partitionConflates({ consumer_group: 'recompute', queue_name: 'other' }, keys),
    false,
  )
  assert.equal(
    partitionConflates({ consumer_group: 'workers', queue_name: 'entities' }, keys),
    false,
  )
  // No listing yet (or a failed one) labels nothing — it does not label
  // everything.
  assert.equal(
    partitionConflates({ consumer_group: 'recompute', queue_name: 'entities' }, new Set()),
    false,
  )
})

test('badge: queue-mode groups are ordinary rows here', () => {
  // The synthetic __QUEUE_MODE__ group is a group like any other as far as a
  // delivery policy goes; nothing about the badge special-cases the name.
  const keys = conflatingKeys([
    { name: '__QUEUE_MODE__', queueName: 'entities', conflation: true },
  ])
  assert.equal(
    partitionConflates({ consumer_group: '__QUEUE_MODE__', queue_name: 'entities' }, keys),
    true,
  )
})

// ---------------------------------------------------------------------------
// The advisory
// ---------------------------------------------------------------------------

const conflating = (over = {}) =>
  ({ name: 'recompute', queueName: 'q', conflation: true, partitionsWithLag: 12, ...over })
const plain = (over = {}) =>
  ({ name: 'workers', queueName: 'q', conflation: false, partitionsWithLag: 3, ...over })

test('advisory: lag verdict over the groups on one queue', () => {
  assert.equal(laggingGroupsVerdict([conflating()]), 'conflating')
  assert.equal(laggingGroupsVerdict([conflating(), plain()]), 'mixed')
  assert.equal(laggingGroupsVerdict([plain()]), 'mixed')
  // Nothing behind: pending, whatever it is, is not conflation's doing.
  assert.equal(laggingGroupsVerdict([]), 'none')
  assert.equal(
    laggingGroupsVerdict([conflating({ partitionsWithLag: 0, maxTimeLag: 0 })]),
    'none',
  )
  // A group that is NOT behind cannot license silence for one that is.
  assert.equal(
    laggingGroupsVerdict([conflating({ partitionsWithLag: 0, maxTimeLag: 0 }), plain()]),
    'mixed',
  )
})

test('advisory: unknown and unavailable are their own answers', () => {
  assert.equal(laggingGroupsVerdict(null), 'unknown')
  assert.equal(laggingGroupsVerdict(undefined), 'unknown')
  // A failed lookup is not an empty one, even when stale rows are still held.
  assert.equal(laggingGroupsVerdict([conflating()], { failed: true }), 'unavailable')
})

test('advisory: lagging is the inclusive rule', () => {
  assert.equal(isGroupLagging({ state: 'Lagging' }), true)
  assert.equal(isGroupLagging({ maxTimeLag: 1 }), true)
  assert.equal(isGroupLagging({ partitionsWithLag: 1 }), true)
  assert.equal(isGroupLagging({ state: 'Stable', maxTimeLag: 0, partitionsWithLag: 0 }), false)
  assert.equal(isGroupLagging(null), false)
})

test('advisory: suppressed only when every lagging group conflates', () => {
  const deep = PENDING_DEPTH_FLOOR + 1
  assert.equal(pendingDepthAdvisory(deep, 'conflating'), 'log-depth')
  assert.equal(pendingDepthAdvisory(4_000_000, 'conflating'), 'log-depth')
  // One non-conflating group behind and the queue is an incident again.
  assert.equal(pendingDepthAdvisory(deep, 'mixed'), 'fire')
})

test('advisory: not knowing never buys silence', () => {
  const deep = PENDING_DEPTH_FLOOR + 1
  // In flight: hold, so the warning is not shown and then retracted.
  assert.equal(pendingDepthAdvisory(deep, 'unknown'), 'hold')
  // Failed: the advisory fires exactly as it did before this feature.
  assert.equal(pendingDepthAdvisory(deep, 'unavailable'), 'fire')
  // Deep with nobody behind: still an incident, and not conflation's to excuse.
  assert.equal(pendingDepthAdvisory(deep, 'none'), 'fire')
})

test('advisory: the floor is unchanged for every verdict', () => {
  for (const verdict of ['conflating', 'mixed', 'none', 'unknown', 'unavailable']) {
    assert.equal(pendingDepthAdvisory(0, verdict), 'none')
    assert.equal(pendingDepthAdvisory(PENDING_DEPTH_FLOOR, verdict), 'none')
    assert.equal(pendingDepthAdvisory(null, verdict), 'none')
    assert.equal(pendingDepthAdvisory(undefined, verdict), 'none')
  }
  assert.equal(PENDING_DEPTH_FLOOR, 10000)
})

// ---------------------------------------------------------------------------
// End to end over one payload shape, the way the page sees it
// ---------------------------------------------------------------------------

test('advisory: the §5.3 example queue reads as healthy', () => {
  // "A conflating queue at pending: 4 000 000, effectivePending: 12 is healthy;
  // the same numbers on a non-conflating group are an incident." (§5.3)
  const listing = [
    { name: 'recompute', queueName: 'entities', conflation: true,
      partitionsWithLag: 12, totalLag: 4_000_000, maxTimeLag: 4, state: 'Stable' },
    { name: 'audit', queueName: 'other-queue', conflation: false,
      partitionsWithLag: 900, totalLag: 4_000_000, maxTimeLag: 9000, state: 'Lagging' },
  ]
  const onQueue = listing.filter(g => g.queueName === 'entities')
  assert.equal(pendingDepthAdvisory(4_000_000, laggingGroupsVerdict(onQueue)), 'log-depth')

  // The very same numbers on the queue next door, where nothing conflates.
  const next = listing.filter(g => g.queueName === 'other-queue')
  assert.equal(pendingDepthAdvisory(4_000_000, laggingGroupsVerdict(next)), 'fire')
})
