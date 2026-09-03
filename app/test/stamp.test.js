import { test } from 'node:test'
import assert from 'node:assert/strict'

import { formatTimeWithZone, stamp } from '../src/composables/useStamp.js'

const instant = new Date('2026-09-03T11:11:17Z')

const panel = (failed, lastUpdated = instant) => ({
  failed: { value: failed },
  lastUpdated: { value: lastUpdated },
})

test('time formatting follows the effective browser timezone', () => {
  assert.equal(
    formatTimeWithZone(instant, 'en-US', { timeZone: 'UTC' }),
    '11:11:17 AM',
  )
  assert.equal(
    formatTimeWithZone(instant, 'en-US', { timeZone: 'Europe/Rome' }),
    '01:11:17 PM',
  )
})

test('freshness stamps stay compact for fresh and stale data', () => {
  const options = { timeZone: 'UTC' }

  assert.equal(stamp(panel(false), 'en-US', options), 'as of 11:11:17 AM')
  assert.equal(
    stamp(panel(true), 'en-US', options),
    'stale · last good 11:11:17 AM',
  )
})

test('freshness stamps still distinguish never loaded data', () => {
  assert.equal(stamp(panel(false, null)), '')
  assert.equal(stamp(panel(true, null)), 'unavailable')
})
