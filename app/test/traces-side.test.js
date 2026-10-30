// Traces' list: GET /api/v1/traces/names, listed by name (a stable order)
// and narrowed by the search box.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { TRACE_NAMES_LIMIT, readTraceNames, traceNameList } from '../src/composables/tracesSide.js'

const payload = {
  trace_names: [
    { trace_name: 'booking-42', trace_count: 7, message_count: 3, last_seen: '2026-10-30T08:00:00.000Z' },
    { trace_name: 'order-9137', trace_count: 2, message_count: 1, last_seen: '2026-10-30T10:00:00.000Z' },
    { trace_name: 'rates-sync', trace_count: 40, message_count: 12, last_seen: null },
    { trace_name: '', trace_count: 1 },
    null,
  ],
  total: 5,
  pagination: { limit: TRACE_NAMES_LIMIT, offset: 0 },
}

test('the names are read with their counts; a blank or broken row is skipped', () => {
  const { rows, total } = readTraceNames(payload)
  assert.deepEqual(rows.map((r) => r.name), ['booking-42', 'order-9137', 'rates-sync'])
  assert.equal(rows[0].traces, 7)
  assert.equal(rows[0].messages, 3)
  assert.equal(rows[0].lastSeen, Date.parse('2026-10-30T08:00:00.000Z'))
  assert.equal(rows[2].lastSeen, null)
  assert.equal(total, 5)
})

test('an answer with no list is not "no names"', () => {
  assert.equal(readTraceNames(null), null)
  assert.equal(readTraceNames({ error: 'x' }), null)
  assert.deepEqual(readTraceNames({ trace_names: [] }), { rows: [], total: 0 })
  // A total under the rows it came with is not believed.
  assert.equal(readTraceNames({ trace_names: payload.trace_names, total: 1 }).total, 3)
})

test('the list is by name, whenever each name was last seen', () => {
  const { rows } = readTraceNames(payload)
  const list = traceNameList(rows)
  assert.deepEqual(list.rows.map((r) => r.name), ['booking-42', 'order-9137', 'rates-sync'])
  // The same order from any input order: a refresh never moves a row.
  assert.deepEqual(traceNameList([...rows].reverse()).rows.map((r) => r.name), ['booking-42', 'order-9137', 'rates-sync'])
  assert.equal(list.ask, null)
  assert.equal(list.open, null)
})

test('what is typed narrows the list, and a name it does not hold can be searched', () => {
  const { rows } = readTraceNames(payload)
  const some = traceNameList(rows, { term: ' OR ' })
  assert.deepEqual(some.rows.map((r) => r.name), ['order-9137'])
  assert.equal(some.ask, 'OR')
  // An exact name on the list is its row, not a second "search" row.
  assert.equal(traceNameList(rows, { term: 'booking-42' }).ask, null)
  assert.deepEqual(traceNameList(rows, { term: 'nothing-like-it' }).rows, [])
})

test('the name open on the page is said even when the list does not hold it', () => {
  const { rows } = readTraceNames(payload)
  assert.equal(traceNameList(rows, { current: 'booking-42' }).open, null)
  const elsewhere = traceNameList(rows, { current: 'older-name', term: 'older-name' })
  assert.equal(elsewhere.open, 'older-name')
  assert.equal(elsewhere.ask, null)
  assert.equal(traceNameList(undefined, { current: 'x' }).open, 'x')
})
