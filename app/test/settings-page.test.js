// Settings › Alerts (console/settings/alertLines.js): lines as typed, the
// draft over the stored document, what a save stores and what it moves on
// the Overview.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  GROUPS, WORDS, lineErrors, lineText, nextDocument, overviewMoves, parseLine, parts, putCall,
  queueNow, readDraft, sideText, tenantGroups,
} from '../src/console/settings/alertLines.js'
import { LINES, LINE_META, QUEUE_FLAGS, QUEUE_LINES } from '../src/composables/settingsDoc.js'
import { THRESHOLDS } from '../src/composables/useSeverity.js'

const M = LINE_META
// The stage document of 2026-10-31: two queues with lines of their own, no tenant lines.
const STAGE = { queues: { 'tf.traces': { lagBadSeconds: 501, lagWarnSeconds: 500 }, 'wl-4': { noReaderOk: true } } }

test('every line of the catalogue has words, under one heading, in the catalogue order', () => {
  for (const meta of LINES) {
    assert.ok(WORDS[meta.key]?.what && WORDS[meta.key]?.label && WORDS[meta.key]?.lead, meta.key)
    assert.ok(GROUPS.some((g) => g.id === WORDS[meta.key].group), meta.key)
  }
  for (const f of QUEUE_FLAGS) assert.ok(WORDS[f.key]?.label, f.key)
  assert.deepEqual(tenantGroups().flatMap((g) => g.lines.map((m) => m.key)), LINES.map((m) => m.key))
})

test('a line is typed as a person says it, and written back the same way', () => {
  assert.equal(parseLine(M.lagWarnSeconds, '90'), 90)
  assert.equal(parseLine(M.lagWarnSeconds, '90 s'), 90)
  assert.equal(parseLine(M.lagWarnSeconds, '2 min'), 120)
  assert.equal(parseLine(M.lagWarnSeconds, '1.5 h'), 5400)
  assert.equal(parseLine(M.lagWarnSeconds, '  10MIN '), 600)
  assert.equal(parseLine(M.lagWarnSeconds, ''), null)
  assert.ok(Number.isNaN(parseLine(M.lagWarnSeconds, '2.5 s')))
  assert.ok(Number.isNaN(parseLine(M.lagWarnSeconds, '5 weeks')))
  assert.equal(parseLine(M.ackWarnRate, '5%'), 0.05)
  assert.equal(parseLine(M.ackWarnRate, '0.5'), 0.005)
  assert.ok(Number.isNaN(parseLine(M.ackWarnRate, '5 min')))
  assert.deepEqual([60, 300, 500, 3600, 5400].map((v) => lineText(M.lagWarnSeconds, v)), ['1 min', '5 min', '500 s', '1 h', '90 min'])
  assert.equal(lineText(M.memWarnShare, 0.8), '80%')
  assert.equal(lineText(M.ackWarnRate, undefined), '')
  for (const v of [1, 59, 60, 90, 3600, 86400]) assert.equal(parseLine(M.lagBadSeconds, lineText(M.lagBadSeconds, v)), v)
})

test('untouched lines show what is stored; a blank inherits; only real changes are rows', () => {
  const none = readDraft(STAGE, { tenant: {}, queues: {} })
  assert.deepEqual(none.rows, [])
  assert.equal(none.bad, 0)
  assert.deepEqual(none.queues.map((q) => q.name), ['tf.traces', 'wl-4'])
  // Typing the stored value again is no change.
  const same = readDraft(STAGE, { tenant: {}, queues: { 'tf.traces': { lagWarnSeconds: '500 s' } } })
  assert.deepEqual(same.rows, [])
  const r = readDraft(STAGE, {
    tenant: { lagWarnSeconds: '2 min' },
    queues: { 'tf.traces': { lagWarnSeconds: '' }, 'wl-4': { noReaderOk: false }, 'orders.created': { backlogWarnSeconds: '10 min' } },
  })
  assert.equal(r.bad, 0)
  assert.deepEqual(r.rows.map((x) => [x.scope, x.key, x.before, x.after]), [
    ['', 'lagWarnSeconds', undefined, 120],
    ['orders.created', 'backlogWarnSeconds', undefined, 600],
    ['tf.traces', 'lagWarnSeconds', 500, undefined],
    ['wl-4', 'noReaderOk', true, false],
  ])
  // A queue's blank inherits the tenant's line as the draft makes it (2 min, not the built-in 1 min).
  assert.deepEqual(r.rows.map((x) => [sideText(x, 'before', r), sideText(x, 'after', r)]), [
    ['built-in 1 min', '2 min'],
    ['the tenant’s 5 min', '10 min'],
    ['500 s', 'the tenant’s 2 min'],
    ['yes', 'no'],
  ])
})

test('a line has to be a line, and attention comes before failing', () => {
  assert.deepEqual(lineErrors(THRESHOLDS, { lagWarnSeconds: NaN }), { lagWarnSeconds: 'Write a number and its unit, like 90 s, 5 min or 2 h.' })
  assert.deepEqual(lineErrors(THRESHOLDS, { ackWarnRate: 1.5 }), { ackWarnRate: 'Above 0%, up to 100%.' })
  assert.deepEqual(lineErrors(THRESHOLDS, { lagWarnSeconds: 600 }), { lagWarnSeconds: 'Has to be under the failing line, 5 min.' })
  assert.deepEqual(lineErrors(THRESHOLDS, { lagBadSeconds: 30 }), { lagBadSeconds: 'Has to be over the line that needs you, 1 min.' })
  // A queue's line is judged against the tenant's lines in force.
  const r = readDraft(STAGE, { tenant: {}, queues: { 'tf.traces': { lagWarnSeconds: '10 min' } } })
  assert.equal(r.bad, 1)
  assert.match(r.queues[0].errors.lagWarnSeconds, /under the failing line, 501 s/)
  assert.deepEqual(r.rows, [])
  // The clash is said where the change was made: removing tf.traces' failing
  // line would leave the tenant's 5 min under its own 500 s.
  const removed = readDraft(STAGE, { tenant: {}, queues: { 'tf.traces': { lagBadSeconds: '' } } })
  assert.deepEqual(removed.queues[0].errors, { lagBadSeconds: 'Removing it leaves the failing line at 5 min, under the line that needs you, 500 s.' })
  assert.equal(removed.bad, 1)
})

test('a save rewrites only the scopes it changes, and keeps what this console does not know', () => {
  const raw = { ...STAGE, alerter: { chat: 42 }, defaults: { lagWarnSeconds: 90, futureLine: 7 } }
  const r = readDraft(raw, { tenant: {}, queues: { 'wl-4': { noReaderOk: false }, 'tf.traces': { lagWarnSeconds: '400 s' } } })
  const next = nextDocument(raw, r)
  assert.deepEqual(next.alerter, { chat: 42 })
  assert.deepEqual(next.defaults, { lagWarnSeconds: 90, futureLine: 7 })
  assert.deepEqual(next.queues, { 'tf.traces': { lagBadSeconds: 501, lagWarnSeconds: 400 } })
  // Clearing a tenant line drops it; an unknown field there stays.
  const r2 = readDraft(raw, { tenant: { lagWarnSeconds: '' }, queues: {} })
  assert.deepEqual(nextDocument(raw, r2).defaults, { futureLine: 7 })
  // No document yet: the first save makes one.
  const r3 = readDraft(null, { tenant: { ackWarnRate: '2%' }, queues: {} })
  assert.deepEqual(nextDocument(null, r3), { defaults: { ackWarnRate: 0.02 } })
})

test('the PUT is shown whole, unless the document is too long to read', () => {
  const short = putCall({ value: STAGE, forever: true, expect: 43262018 })
  assert.equal(short.long, false)
  assert.equal(short.call, `PUT /api/v1/kv/queen.console/settings ${JSON.stringify({ value: STAGE, forever: true, expect: 43262018 })}`)
  const big = { queues: Object.fromEntries(Array.from({ length: 60 }, (_, i) => [`queue-${i}`, { lagWarnSeconds: 120 }])) }
  const long = putCall({ value: big, forever: true, expect: 7 })
  assert.equal(long.long, true)
  assert.equal(long.call, 'PUT /api/v1/kv/queen.console/settings {"value":{…},"forever":true,"expect":7}')
})

const queue = (name, pending = 10) => ({ name, messages: { pending } })
const group = (queueName, maxTimeLag, name = 'workers') => ({ name, queueName, maxTimeLag, state: 'Stable', totalLag: 5 })

test('what a save moves on the Overview, judged by the one rule', () => {
  const queues = [queue('orders'), queue('mail'), queue('tf.traces')]
  const groups = [group('orders', 120), group('mail', 400), group('tf.traces', 30)]
  // Built-in: 1 min needs you, 5 min fails. orders (2 min) amber, mail (6 min 40 s) red.
  const raise = { defaults: { lagWarnSeconds: 180, lagBadSeconds: 600 } }
  assert.deepEqual(overviewMoves(queues, groups, null, null, raise), [
    '`orders` leaves the Overview.',
    '`mail` turns from red to amber.',
  ])
  const own = { queues: { 'tf.traces': { lagWarnSeconds: 20, lagBadSeconds: 40 } } }
  assert.deepEqual(overviewMoves(queues, groups, null, null, own), [
    '`tf.traces` goes on the Overview: its slowest group is 30 s behind.',
  ])
  assert.deepEqual(overviewMoves([], groups, null, null, own), [])
  assert.deepEqual(parts('`a` and `b`.'), [{ t: 'a', mono: true }, { t: ' and ', mono: false }, { t: 'b', mono: true }, { t: '.', mono: false }])
})

test('where one queue stands now, by its lines', () => {
  const lines = () => THRESHOLDS
  assert.deepEqual(queueNow(null, [], lines, null), { text: '', tone: '' })
  assert.deepEqual(queueNow(queue('orders'), [group('orders', 120)], lines, null), { text: 'slowest group 2 min behind', tone: 'amber' })
  assert.deepEqual(queueNow(queue('orders'), [group('orders', 0)], lines, null), { text: 'every group caught up', tone: '' })
  assert.deepEqual(queueNow(queue('orders', 0), [], lines, null), { text: 'no group reads it', tone: '' })
  assert.deepEqual(queueNow(queue('orders', 3), [], () => ({ ...THRESHOLDS, noReaderOk: true }), null), { text: '3 waiting messages, no group reads it', tone: '' })
  assert.deepEqual(queueNow(queue('orders', 1), [], lines, null), { text: 'nothing reads its 1 waiting message', tone: 'amber' })
  // Every per-queue line is one the tenant table has too.
  for (const m of QUEUE_LINES) assert.ok(LINES.includes(m))
})
