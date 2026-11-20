// The time window's From and To (console/windowTime.js): a time alone is
// today, a date and a time is that day, in local time. Until 2026-11-20 the
// band took times of today only, so a list of messages could not be read
// for any other day without editing the address.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { atText, parseAt, rangeOf } from '../src/console/windowTime.js'

// 2026-11-20 08:30:00 local time.
const NOW = new Date(2026, 10, 20, 8, 30, 0).getTime()
const at = (y, mo, d, h, mi, s = 0) => new Date(y, mo - 1, d, h, mi, s).getTime()

test('parseAt: a time alone is today', () => {
  assert.equal(parseAt('14:05', NOW), at(2026, 11, 20, 14, 5))
  assert.equal(parseAt(' 9.40 ', NOW), at(2026, 11, 20, 9, 40))
  assert.equal(parseAt('07:40:56', NOW), at(2026, 11, 20, 7, 40, 56))
})

test('parseAt: a date and a time is that day, in another month or year too', () => {
  assert.equal(parseAt('2026-11-19 14:05', NOW), at(2026, 11, 19, 14, 5))
  assert.equal(parseAt('2026-11-19T14:05:30', NOW), at(2026, 11, 19, 14, 5, 30))
  assert.equal(parseAt('2026-10-31 23:59', NOW), at(2026, 10, 31, 23, 59))
  assert.equal(parseAt('2025-12-31 00:00', NOW), at(2025, 12, 31, 0, 0))
  assert.equal(parseAt('2026-2-3 4:05', NOW), at(2026, 2, 3, 4, 5))
})

test('parseAt: what is not a time is null', () => {
  for (const s of ['', 'now', '24:00', '14:60', '14:05:60', '2026-02-30 10:00', '2026-13-01 10:00', '19 14:05', '2026-11-19']) {
    assert.equal(parseAt(s, NOW), null, s)
  }
})

test('atText: what From and To show, read back to the same moment', () => {
  assert.equal(atText(at(2026, 11, 19, 14, 5)), '2026-11-19 14:05')
  assert.equal(atText(at(2026, 1, 2, 3, 4, 5)), '2026-01-02 03:04:05')
  for (const ms of [at(2026, 11, 19, 14, 5), at(2026, 1, 2, 3, 4, 5), NOW]) assert.equal(parseAt(atText(ms), NOW), ms)
})

test('rangeOf: a window on another day, and a To past now cut at now', () => {
  assert.deepEqual(rangeOf('2026-11-19 14:00', '2026-11-19 15:00', NOW), { from: at(2026, 11, 19, 14, 0), to: at(2026, 11, 19, 15, 0) })
  assert.deepEqual(rangeOf('2026-11-19 22:00', '01:00', NOW), { from: at(2026, 11, 19, 22, 0), to: at(2026, 11, 20, 1, 0) })
  assert.deepEqual(rangeOf('08:00', '09:00', NOW), { from: at(2026, 11, 20, 8, 0), to: NOW })
})

test('rangeOf: each refusal says why', () => {
  assert.match(rangeOf('yesterday', '10:00', NOW).error[0], /14:05/)
  assert.match(rangeOf('09:00', '10:00', NOW).error[0], /to come/)
  assert.match(rangeOf('08:00', '07:00', NOW).error[0], /earlier than To/)
  assert.match(rangeOf('08:00', '08:00:30', NOW).error[0], /a minute/)
})
