import { test } from 'node:test'
import assert from 'node:assert/strict'

import { formatTimestamp, formatTimestampUtc } from '../src/format.js'

test('console timestamps use the compact browser-local format', () => {
  assert.equal(
    formatTimestamp('2026-09-03T14:03:06Z', 'en-US', { timeZone: 'Europe/Rome' }),
    'Sep 3, 2026, 04:03:06 PM',
  )
  assert.equal(formatTimestampUtc('2026-09-03T14:03:06Z'), 'UTC: 2026-09-03T14:03:06.000Z')
})

test('console timestamp formatter handles missing and invalid values', () => {
  assert.equal(formatTimestamp(null), '—')
  assert.equal(formatTimestamp('not-a-date'), 'not-a-date')
  assert.equal(formatTimestampUtc('not-a-date'), '')
})
