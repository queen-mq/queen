import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  LOCKS_NAMESPACE,
  describeLocksEnd,
  formatAgo,
  formatExpiresIn,
  formatHeldFor,
  formatSpan,
  guardOf,
  lifetimeSeconds,
  locksListBody,
  permitOf,
  permitsOf,
  releaseBody,
  releaseCommand,
  splitPermitKey,
} from '../src/composables/useLocks.js'

// The Locks page's pure half. A permit is a KV row (server/src/locks.rs), so
// everything here is how a row of the console's KV listing reads as one, and
// what the page may say about it.

// A row as `POST /api/v1/resources/kv/list` answers it, six fractional digits
// trimmed of their zeros as the broker prints them.
const row = (over = {}) => ({
  key: 'daily-report#0',
  value: { owner: 'cron-7' },
  version: 210,
  expiresAt: '2026-10-08T09:37:22.87144+00:00',
  updatedAt: '2026-10-08T09:35:22.87144+00:00',
  expired: false,
  ...over,
})
const NOW = Date.parse('2026-10-08T09:35:30.871Z')

// ---------------------------------------------------------------------------
// The request body
// ---------------------------------------------------------------------------

test('the page asks for the lock namespace, live permits only', () => {
  assert.equal(LOCKS_NAMESPACE, 'queen-locks')
  assert.deepEqual(locksListBody({ limit: 100 }), {
    namespace: 'queen-locks', includeExpired: false, limit: 100,
  })
  // The opposite of the KV page, on purpose: a permit past its expiry is held
  // by nobody, and "who holds this now" is the only question here.
  for (const body of [locksListBody(), locksListBody({ prefix: 'sync:', after: 'sync:a#0', limit: 50 })]) {
    assert.equal(body.includeExpired, false)
  }
})

test('a prefix and a cursor travel only when they exist', () => {
  const first = locksListBody({ prefix: '', after: null })
  assert.equal('prefix' in first, false)
  assert.equal('after' in first, false)
  const next = locksListBody({ prefix: 'sync:', after: 'sync:crm#0' })
  assert.equal(next.prefix, 'sync:')
  assert.equal(next.after, 'sync:crm#0')
})

// ---------------------------------------------------------------------------
// A key is a name and a slot
// ---------------------------------------------------------------------------

test('the slot is what follows the last #', () => {
  assert.deepEqual(splitPermitKey('daily-report#0'), { name: 'daily-report', slot: 0 })
  assert.deepEqual(splitPermitKey('gpu#1023'), { name: 'gpu', slot: 1023 })
  assert.deepEqual(splitPermitKey('order/9137:sync#12'), { name: 'order/9137:sync', slot: 12 })
})

test('a key the broker would not have written is not a permit', () => {
  // The broker writes the plain decimal and nothing looser, and a name is
  // never empty: these are somebody's hand-written rows.
  for (const key of ['daily-report', 'gpu#', 'gpu#x', 'gpu#07', 'gpu#+7', 'gpu#-1', '#0', 'gpu#1 ', '', null, 7]) {
    assert.equal(splitPermitKey(key), null, String(key))
  }
})

// ---------------------------------------------------------------------------
// A row is a permit
// ---------------------------------------------------------------------------

test('a permit never renewed was taken when it was written', () => {
  const p = permitOf(row())
  assert.equal(p.name, 'daily-report')
  assert.equal(p.slot, 0)
  assert.equal(p.owner, 'cron-7')
  assert.equal(p.token, 210)
  assert.equal(p.since, '2026-10-08T09:35:22.87144+00:00')
  assert.equal(p.renewedAt, p.since)
  assert.equal(p.foreign, false)
})

test('a renewed permit keeps the acquire in its value', () => {
  // The first renewal copies the acquire's time into the value; every later
  // one carries it. `updatedAt` then says only when the holder last renewed.
  const p = permitOf(row({ value: { owner: 'cron-7', since: '2026-10-08T03:21:07.5+00:00' } }))
  assert.equal(p.since, '2026-10-08T03:21:07.5+00:00')
  assert.equal(p.renewedAt, '2026-10-08T09:35:22.87144+00:00')
  assert.equal(formatHeldFor(p.since, NOW), '6h 14m')
  assert.equal(formatAgo(p.renewedAt, NOW), '8s ago')
})

test('a permit without an owner is held all the same', () => {
  const p = permitOf(row({ value: { owner: null } }))
  assert.equal(p.owner, null)
  assert.equal(p.foreign, false)
})

test('a row somebody wrote by hand is listed as a row, not as a lock', () => {
  // It occupies its key, so hiding it would hide the reason an acquire fails.
  const noSlot = permitOf(row({ key: 'scratch' }))
  assert.equal(noSlot.foreign, true)
  assert.equal(noSlot.name, 'scratch')
  assert.equal(noSlot.slot, null)
  const noOwner = permitOf(row({ value: 'written by hand' }))
  assert.equal(noOwner.foreign, true)
  assert.equal(noOwner.owner, null)
  // And the locks route cannot address either of them.
  assert.equal(releaseBody(noSlot), null)
  assert.equal(releaseBody(noOwner), null)
})

test('a page tells a semaphore by what it can see of it', () => {
  const page = permitsOf([
    row({ key: 'daily-report#0' }),
    row({ key: 'gpu#0', value: { owner: 'a' } }),
    row({ key: 'gpu#2', value: { owner: 'b' } }),
    row({ key: 'pool#3', value: { owner: 'c' } }),
  ])
  assert.deepEqual(page.map((p) => [p.name, p.slot, p.shared]), [
    ['daily-report', 0, false],
    ['gpu', 0, true],
    ['gpu', 2, true],
    ['pool', 3, true],
  ])
  assert.deepEqual(permitsOf(null), [])
  assert.deepEqual(permitsOf([null, {}, { key: 7 }]), [])
})

// ---------------------------------------------------------------------------
// Time, against the load instant
// ---------------------------------------------------------------------------

test('a span has two units at most, the larger first', () => {
  assert.equal(formatSpan(0), '0s')
  assert.equal(formatSpan(47_000), '47s')
  assert.equal(formatSpan(60_000), '1m')
  assert.equal(formatSpan(725_000), '12m 5s')
  assert.equal(formatSpan(2 * 3_600_000 + 14 * 60_000 + 9_000), '2h 14m')
  assert.equal(formatSpan(3 * 86_400_000 + 4 * 3_600_000), '3d 4h')
  assert.equal(formatSpan(2 * 86_400_000), '2d')
  assert.equal(formatSpan(-1), '—')
  assert.equal(formatSpan(NaN), '—')
})

test('every relative cell is rendered against the instant it is given', () => {
  const p = permitOf(row())
  assert.equal(formatHeldFor(p.since, NOW), '8s')
  assert.equal(formatAgo(p.renewedAt, NOW), '8s ago')
  assert.equal(formatExpiresIn(p.expiresAt, NOW), 'in 1m 52s')
  // A clock a little behind the broker's must not print a negative span.
  assert.equal(formatHeldFor(p.since, NOW - 60_000), '0s')
  assert.equal(formatAgo(p.renewedAt, NOW - 60_000), 'just now')
  assert.equal(formatExpiresIn(p.expiresAt, NOW + 600_000), 'now')
  for (const bad of [null, undefined, '', 'not a time']) {
    assert.equal(formatHeldFor(bad, NOW), '—')
    assert.equal(formatAgo(bad, NOW), '—')
    assert.equal(formatExpiresIn(bad, NOW), '—')
  }
})

test('the lifetime is the distance from the last write to its expiry', () => {
  assert.equal(lifetimeSeconds(permitOf(row())), 120)
  assert.equal(lifetimeSeconds(permitOf(row({ expiresAt: null }))), null)
  assert.equal(lifetimeSeconds(null), null)
})

// ---------------------------------------------------------------------------
// The machine behind a row
// ---------------------------------------------------------------------------

test('the guard is the check of the permit’s row at its token', () => {
  assert.deepEqual(guardOf(permitOf(row())), {
    op: 'check', ns: 'queen-locks', key: 'daily-report#0', expect: 210, required: true,
  })
  assert.equal(guardOf(permitOf(row({ version: undefined }))), null)
})

test('the release names the lease period on screen, and the slot of a semaphore', () => {
  // The token is the one the page read: a holder that renewed since answers
  // `lost` and keeps its lock.
  assert.deepEqual(releaseBody(permitOf(row())), {
    operations: [{ op: 'release', name: 'daily-report', token: 210 }],
  })
  assert.deepEqual(releaseBody(permitOf(row({ key: 'gpu#3' }))), {
    operations: [{ op: 'release', name: 'gpu', token: 210, slot: 3 }],
  })
})

test('the curl line survives a quote in a lock’s name', () => {
  const line = releaseCommand(permitOf(row({ key: "bob's-job#0" })), 'https://queen.example')
  assert.equal(
    line,
    `curl -s -X POST https://queen.example/api/v1/locks -H 'content-type: application/json' ` +
      `-d '{"operations":[{"op":"release","name":"bob'\\''s-job","token":210}]}'`,
  )
  assert.equal(releaseCommand(permitOf(row({ key: 'scratch' }))), null)
})

test('the foot of the page says where the walk ends', () => {
  assert.equal(describeLocksEnd({ rowCount: 3, truncated: false }), 'every held lock')
  assert.equal(describeLocksEnd({ rowCount: 3, truncated: false, prefixed: true }), 'end of the prefix')
  assert.equal(describeLocksEnd({ rowCount: 3, truncated: true }), '')
  assert.equal(describeLocksEnd({ rowCount: 0 }), '')
})
