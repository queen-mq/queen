// Settings' list and the section it opens: the tenant's lines, then each
// queue with its own, said as they are in force; and the "Add a queue" menu.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { LINES, QUEUE_FLAGS, QUEUE_LINES, formatSpan, readSettings, resolveLines } from '../src/composables/settingsDoc.js'
import {
  matchEntries, movedCount, ownCount, ownQueueEntries, queueChoices, queueLinesInForce, tenantLinesInForce,
} from '../src/composables/settingsSide.js'
import { THRESHOLDS } from '../src/composables/useSeverity.js'

const doc = readSettings({
  defaults: { lagWarnSeconds: 300 },
  queues: {
    'reports.nightly': { lagWarnSeconds: 900, lagBadSeconds: 3600 },
    'audit.archive': { noReaderOk: true },
  },
})

test('the tenant moved as many lines as its document sets', () => {
  assert.equal(movedCount(doc.defaults), 1)
  assert.equal(movedCount({}), 0)
  assert.equal(movedCount(undefined), 0)
  assert.equal(movedCount({ lagWarnSeconds: null, noSuchLine: 4 }), 0)
})

test('queues with their own settings, by name, with what they have of their own', () => {
  const entries = ownQueueEntries(doc.queues, new Set(['reports.nightly']))
  assert.deepEqual(entries.map((e) => e.name), ['audit.archive', 'reports.nightly'])
  assert.deepEqual(entries[0].summary, ['no reader expected'])
  assert.deepEqual(entries[1].summary, ['Consumer lag 15m / 1h'])
  assert.equal(entries[0].exists, false)
  assert.equal(entries[1].exists, true)
})

test('while the queues are not read, no queue is said not to exist', () => {
  assert.ok(ownQueueEntries(doc.queues, null).every((e) => e.exists))
  assert.deepEqual(ownQueueEntries(undefined), [])
})

test('the list search keeps the names that hold the term, case-blind', () => {
  const entries = ownQueueEntries(doc.queues)
  assert.deepEqual(matchEntries(entries, 'NIGHT').map((e) => e.name), ['reports.nightly'])
  assert.equal(matchEntries(entries, '  ').length, 2)
  assert.deepEqual(matchEntries(entries, 'zzz'), [])
  assert.deepEqual(matchEntries(undefined, 'a'), [])
})

test('a queue section: its own lines where it has them, the tenant\'s otherwise', () => {
  const tenant = resolveLines(THRESHOLDS, doc)
  const view = queueLinesInForce(doc.queues.get('reports.nightly'), tenant)
  assert.equal(view.hasOwn, true)
  assert.deepEqual(view.groups.flatMap((g) => g.rows).map((r) => r.key), QUEUE_LINES.map((m) => m.key))
  const lag = view.groups[0].rows
  assert.deepEqual(lag.slice(0, 2).map((r) => [r.key, r.own, r.text]), [
    ['lagWarnSeconds', true, '15m'],
    ['lagBadSeconds', true, '1h'],
  ])
  // The tenant's line, from the document's defaults over the built-in table.
  const ack = view.groups.find((g) => g.name === 'Ack failures').rows[0]
  assert.equal(ack.own, false)
  assert.ok(ack.text.endsWith('%'))
  assert.deepEqual(view.flags.map((f) => [f.key, f.on]), [['noReaderOk', false]])
  assert.equal(ownCount(doc.queues.get('reports.nightly')), 2)
})

test('a queue with only a flag has its own; one with nothing is all the tenant\'s', () => {
  const tenant = resolveLines(THRESHOLDS, doc)
  const flagged = queueLinesInForce(doc.queues.get('audit.archive'), tenant)
  assert.equal(flagged.hasOwn, true)
  assert.equal(flagged.flags[0].on, true)
  assert.ok(flagged.groups.every((g) => g.rows.every((r) => !r.own)))
  assert.equal(flagged.groups[0].rows[0].text, '5m') // the tenant's 300 s
  assert.equal(ownCount(doc.queues.get('audit.archive')), QUEUE_FLAGS.length)

  const none = queueLinesInForce(undefined, tenant)
  assert.equal(none.hasOwn, false)
  assert.equal(ownCount(undefined), 0)
  assert.equal(queueLinesInForce(undefined, undefined).groups[0].rows[0].text, '—')
})

test('the tenant\'s lines for a reader: the document\'s where it moves one, built-in elsewhere', () => {
  const view = tenantLinesInForce(doc.defaults, THRESHOLDS)
  assert.equal(view.moved, 1)
  const rows = view.groups.flatMap((g) => g.rows)
  assert.deepEqual(rows.map((r) => r.key), LINES.map((m) => m.key))
  assert.deepEqual([rows[0].key, rows[0].own, rows[0].text], ['lagWarnSeconds', true, '5m'])
  assert.equal(rows[1].own, false)
  assert.equal(rows[1].text, formatSpan(THRESHOLDS.lagBadSeconds))
  assert.ok(rows.find((r) => r.key === 'memWarnShare').text.endsWith('%'))
  assert.equal(tenantLinesInForce(undefined, undefined).groups[0].rows[0].text, '—')
})

test('add a queue: the queues with nothing of their own, by name, and a typed new name', () => {
  const names = ['orders', 'audit.archive', 'billing', 'orders', 'reports.nightly', '', null]
  const all = queueChoices(names, doc.queues)
  assert.deepEqual(all.rows, ['billing', 'orders'])
  assert.equal(all.more, 0)
  assert.equal(all.typed, '')
  assert.equal(all.own, '')

  assert.deepEqual(queueChoices(names, doc.queues, 'ORD').rows, ['orders'])
  // A name no queue has yet may be given its lines before its first push.
  const fresh = queueChoices(names, doc.queues, ' payments ')
  assert.deepEqual(fresh.rows, [])
  assert.equal(fresh.typed, 'payments')
  // An exact name that is offered is not offered twice.
  assert.equal(queueChoices(names, doc.queues, 'orders').typed, '')
  // A queue that already has its own is opened, not added.
  const taken = queueChoices(names, doc.queues, 'audit.archive')
  assert.equal(taken.typed, '')
  assert.equal(taken.own, 'audit.archive')

  const capped = queueChoices(['a', 'b', 'c'], new Set(), '', 2)
  assert.deepEqual(capped.rows, ['a', 'b'])
  assert.equal(capped.more, 1)
  assert.deepEqual(queueChoices(undefined, undefined), { rows: [], more: 0, typed: '', own: '' })
})
