import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  DEFAULT_TIME_RANGE, TIME_RANGES, EXTENDED_TIME_RANGES, SYSTEM_TIME_RANGES,
  LAG_THRESHOLDS, USAGE_TIME_RANGES, rangeMinutes, rangeSeconds, relativeTimeRange,
} from '../../shared/timeRanges.mjs'

test('all picker families include the same hourly presets, including 12h', () => {
  assert.deepEqual(TIME_RANGES.map(range => range.value), ['1h', '6h', '12h', '24h'])
  for (const family of [EXTENDED_TIME_RANGES, SYSTEM_TIME_RANGES, LAG_THRESHOLDS, USAGE_TIME_RANGES]) {
    for (const range of TIME_RANGES) assert.ok(family.includes(range))
    assert.equal(new Set(family.map(range => range.value)).size, family.length)
    assert.ok(Object.isFrozen(family))
  }
  const halfDay = TIME_RANGES.find(range => range.value === '12h')
  assert.equal(halfDay.minutes, 720)
  assert.equal(halfDay.seconds, 43200)
  assert.equal(halfDay.hours, 12)
  assert.ok(Object.isFrozen(halfDay))
})

test('relative ranges and API units also support non-preset investigation links', () => {
  const to = new Date('2026-10-19T12:00:00.000Z')
  for (const [value, minutes] of [['15m', 15], ['90m', 90], ['12h', 720], ['24h', 1440], ['7d', 10080]]) {
    assert.equal(rangeMinutes(value), minutes)
    assert.equal(rangeSeconds(value), minutes * 60)
    const window = relativeTimeRange(value, to)
    assert.equal(window.to.getTime() - window.from.getTime(), minutes * 60_000)
    assert.equal(window.to.toISOString(), '2026-10-19T12:00:00.000Z')
  }
})

test('malformed and nonpositive durations fall back to the default hour', () => {
  const defaultMinutes = rangeMinutes(DEFAULT_TIME_RANGE)
  assert.equal(defaultMinutes, 60)
  for (const value of [undefined, null, '', 'custom', '12', '12w', '-12h', '0m', '1.5h', `${'9'.repeat(400)}h`]) {
    assert.equal(rangeMinutes(value), defaultMinutes)
    assert.equal(rangeSeconds(value), 3600)
  }
})

test('12h means twelve elapsed hours across the daylight-saving transition', () => {
  const { from, to } = relativeTimeRange('12h', new Date('2026-10-25T08:00:00+01:00'))
  assert.equal(from.toISOString(), '2026-10-24T19:00:00.000Z')
  assert.equal(to.getTime() - from.getTime(), 43_200_000)
})

test('console usage presets stay within the API whole-hour limits', () => {
  assert.deepEqual(USAGE_TIME_RANGES.map(range => range.hours), [1, 6, 12, 24, 72, 168])
  for (const range of USAGE_TIME_RANGES) {
    assert.ok(Number.isInteger(range.hours) && range.hours >= 1 && range.hours <= 168)
  }
})
