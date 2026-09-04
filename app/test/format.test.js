import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  formatTimestamp, formatTimestampRange, formatTimestampRangeUtc,
  formatTimestampTime, formatTimestampUtc,
} from '../src/composables/useFormat.js'

const instant = '2026-09-03T14:03:06.000Z'
const options = { timeZone: 'Europe/Rome' }

test('absolute timestamps use one compact browser-local format', () => {
  assert.equal(
    formatTimestamp(instant, 'en-US', options),
    'Sep 3, 2026, 04:03:06 PM',
  )
  assert.equal(
    formatTimestampTime(instant, 'en-US', options),
    '04:03:06 PM',
  )
})

test('timestamp ranges apply the same contract to both endpoints', () => {
  const range = formatTimestampRange(
    instant,
    '2026-09-03T15:03:06.000Z',
    'en-US',
    options,
  )
  assert.match(range, /04:03:06 PM →/)
  assert.match(range, /05:03:06 PM$/)
  assert.equal(
    formatTimestampRangeUtc(instant, '2026-09-03T15:03:06.000Z'),
    'UTC: 2026-09-03T14:03:06.000Z → 2026-09-03T15:03:06.000Z',
  )
})

test('UTC hover title preserves the exact instant', () => {
  assert.equal(formatTimestampUtc(instant), 'UTC: 2026-09-03T14:03:06.000Z')
})

test('timestamp formatting degrades safely for missing and invalid values', () => {
  assert.equal(formatTimestamp(null), '—')
  assert.equal(formatTimestamp('not-a-date'), 'not-a-date')
  assert.equal(formatTimestampUtc('not-a-date'), '')
})
