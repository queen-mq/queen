import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  describeDrops,
  dropTotal,
  ephemeralRows,
  isQueueGone,
  matchesSearch,
  partitionRows,
  tierWord,
} from '../src/composables/useEphemeralSide.js'

// The Ephemeral sub sidebar's pure half: every queue by name with its depth,
// a list a 2 s poll cannot reorder.

const q = (name, depth) => ({ name, depth })

test('matchesSearch: anywhere in the name, case-insensitive, trimmed', () => {
  assert.equal(matchesSearch('rpc.reply.42', ''), true)
  assert.equal(matchesSearch('rpc.reply.42', ' REPLY '), true)
  assert.equal(matchesSearch('rpc.reply.42', 'inbox'), false)
  assert.equal(matchesSearch(null, 'x'), false)
  assert.equal(matchesSearch('a', null), true)
})

test('ephemeralRows: by name, whatever the depth', () => {
  const rows = ephemeralRows([q('b', 0), q('deep', 90), q('a', 0), q('shallow', 3), q('x', null)])
  assert.deepEqual(rows.map((r) => r.name), ['a', 'b', 'deep', 'shallow', 'x'])
})

test('ephemeralRows: search narrows, junk ignored', () => {
  assert.deepEqual(ephemeralRows([q('rpc.a', 1), q('inbox.b', 0), null, q('', 2)], { search: 'rpc' }).map((r) => r.name), ['rpc.a'])
  assert.deepEqual(ephemeralRows(null), [])
})

test('dropTotal: the three counters together; none reported is no figure, never a zero', () => {
  assert.equal(dropTotal({ bounds: 2, ttl: 0, retry: 1 }), 3)
  assert.equal(dropTotal({ bounds: null, ttl: 4, retry: null }), 4)
  assert.equal(dropTotal({ bounds: null, ttl: null, retry: null }), null)
  assert.equal(dropTotal(undefined), null)
})

test('describeDrops names every counter, a missing one as a dash', () => {
  assert.equal(describeDrops({ bounds: 1200, ttl: 0, retry: null }), 'bounds 1,200 · ttl 0 · retry —')
})

test('tierWord: only an explicit answer is a tier', () => {
  assert.equal(tierWord(true), 'Declared')
  assert.equal(tierWord(false), 'Implicit')
  assert.equal(tierWord(null), '—')
})

test('partitionRows: the depth route\'s partitions by name, unreported figures kept null', () => {
  // As GET /api/v1/ephemeral/queues/:queue/depth answers it.
  const payload = {
    queue: 'inbox-demo', tier: 'implicit', pending: 4, bytes: 28,
    partitions: [{ partition: 'b', pending: 1, bytes: 7 }, { partition: 'Default', pending: 3, bytes: 21 }, { partition: 'c' }, null],
    groups: [],
  }
  assert.deepEqual(partitionRows(payload), [
    { name: 'b', depth: 1, bytes: 7 },
    { name: 'c', depth: null, bytes: null },
    { name: 'Default', depth: 3, bytes: 21 },
  ])
  assert.equal(partitionRows({ partitions: 2 }), null, 'a count is not a list')
  assert.equal(partitionRows(null), null)
})

test('isQueueGone: a missing queue is gone, a missing route is not', () => {
  assert.equal(isQueueGone({ status: 404, code: 'ephemeral_queue_not_found' }), true)
  assert.equal(isQueueGone({ status: 404, code: null }), false, 'no route: the class itself is absent')
  assert.equal(isQueueGone({ status: 503, code: 'ephemeral_queue_not_found' }), false)
  assert.equal(isQueueGone(null), false)
})
