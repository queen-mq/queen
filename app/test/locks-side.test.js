import { test } from 'node:test'
import assert from 'node:assert/strict'

import { permitsOf } from '../src/composables/useLocks.js'
import {
  SIDE_READ_LIMIT,
  describeHeld,
  filterEntries,
  lockEntryOf,
  lockPrefixes,
  lockTotals,
  wholeSet,
} from '../src/composables/useLocksSide.js'

// The Locks sub sidebar's pure half: a fixed filter list of lock name
// prefixes, counted over every held permit, never over one filtered page.

const row = (key, owner = 'host:1:abc') => ({
  key,
  value: owner === null ? {} : { owner },
  version: 7,
  updatedAt: '2026-10-30T10:00:00Z',
  expiresAt: '2026-10-30T10:01:00Z',
})

test('lockEntryOf: the prefix up to the first ":", or the whole name', () => {
  assert.equal(lockEntryOf('sync:orders'), 'sync:')
  assert.equal(lockEntryOf('sync:orders:eu'), 'sync:')
  assert.equal(lockEntryOf('leader'), 'leader')
  assert.equal(lockEntryOf(':lead'), ':')
  assert.equal(lockEntryOf(undefined), null)
  assert.equal(lockEntryOf(''), null)
})

test('lockPrefixes: by name, never by count; locks and semaphores counted apart', () => {
  const permits = permitsOf([
    row('sync:a#0'),
    row('sync:b#0'),
    row('sync:pool#0'),
    row('sync:pool#1'),
    row('sync:pool#2'),
    row('job:nightly#0'),
    row('leader#0'),
  ])
  const entries = lockPrefixes(permits)
  assert.deepEqual(entries.map((e) => e.prefix), ['job:', 'leader', 'sync:'])
  const sync = entries[2]
  assert.equal(sync.locks, 2)
  assert.equal(sync.semaphores, 1)
  assert.equal(sync.semaphorePermits, 3)
  assert.equal(sync.permits, 5, 'the number the list shows: the rows its filter shows')
  assert.equal(describeHeld(sync), '2 locks · 1 semaphore (3 permits)')
  assert.equal(describeHeld(entries[0]), '1 lock')
})

test('lockPrefixes: a row that is not a permit is an other row, not a lock', () => {
  const entries = lockPrefixes(permitsOf([row('sync:stray'), row('sync:x#0', null)]))
  assert.equal(entries.length, 1)
  assert.equal(entries[0].other, 2)
  assert.equal(entries[0].locks, 0)
  assert.equal(entries[0].permits, 2)
  assert.equal(describeHeld(entries[0]), '2 other rows')
})

test('lockPrefixes: byte order; junk ignored', () => {
  const entries = lockPrefixes(permitsOf([row('b:1#0'), row('a:1#0'), row('B:1#0')]))
  assert.deepEqual(entries.map((e) => e.prefix), ['B:', 'a:', 'b:'])
  assert.deepEqual(lockPrefixes(null), [])
  assert.deepEqual(lockPrefixes([null, {}]), [])
})

test('lockTotals: the "All held locks" row is the entries summed', () => {
  const entries = lockPrefixes(permitsOf([row('sync:a#0'), row('sync:p#0'), row('sync:p#1'), row('job:n#0'), row('x:stray')]))
  assert.deepEqual(lockTotals(entries), { locks: 2, semaphores: 1, semaphorePermits: 2, other: 1, permits: 5 })
  assert.deepEqual(lockTotals(null), { locks: 0, semaphores: 0, semaphorePermits: 0, other: 0, permits: 0 })
})

test('wholeSet: only the first unfiltered page with nothing after it is every held permit', () => {
  const permits = permitsOf([row('sync:a#0')])
  assert.equal(wholeSet({ prefix: '', page: 1, more: false, permits }), permits)
  assert.equal(wholeSet({ prefix: 'sync:', page: 1, more: false, permits }), null, 'a filtered page holds one prefix')
  assert.equal(wholeSet({ prefix: '', page: 2, more: false, permits }), null, 'a later page lacks the first')
  assert.equal(wholeSet({ prefix: '', page: 1, more: true, permits }), null, 'the walk goes on')
  assert.equal(wholeSet(null), null)
  assert.deepEqual(wholeSet({ prefix: '', page: 1, more: false, permits: [] }), [], 'no lock held is a whole set too')
})

test('filterEntries: the prefix or a lock name under it, case-insensitive, trimmed', () => {
  const entries = lockPrefixes(permitsOf([row('sync:feratel#0'), row('sync:booking#0'), row('job:b#0'), row('Leader#0')]))
  assert.deepEqual(entries.find((e) => e.prefix === 'sync:').names, ['sync:booking', 'sync:feratel'])
  assert.deepEqual(filterEntries(entries, ' LEAD ').map((e) => e.prefix), ['Leader'])
  assert.deepEqual(filterEntries(entries, 'feratel').map((e) => e.prefix), ['sync:'], 'a name finds its prefix')
  assert.equal(filterEntries(entries, '').length, 3)
  assert.deepEqual(filterEntries(null, 'x'), [])
})

test('the list reads at most one broker page of permits', () => {
  assert.equal(SIDE_READ_LIMIT, 1000)
})
