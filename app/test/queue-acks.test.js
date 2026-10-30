// A queue whose acks fail needs you: the one rule (useAttention) judges each
// queue's last hour (queueOps, from analytics/workload by queue), so the
// verdict, the issues, the queue list and a queue's Alerts tab agree. Stage,
// 2026-10-30: channel.opsync_fetch failed 2,132 of 2,132 acks in the hour,
// the tenant's whole 14% — and no queue carried a mark.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { overviewVerdict, attentionWhy } from '../src/composables/overviewViews.js'
import { queueAlerts } from '../src/composables/queueAlerts.js'
import { acksText, opsByQueue } from '../src/composables/queueOps.js'
import { buildQueueTriage, filterQueueTriage } from '../src/composables/queueTriage.js'
import { ackAttention, queueAttention } from '../src/composables/useAttention.js'
import { THRESHOLDS } from '../src/composables/useSeverity.js'

const q = (name, pending = 0, deadLetter = 0) => ({ name, messages: { pending, deadLetter } })
const group = (queueName, maxTimeLag, name = 'workers') => ({ name, queueName, state: 'Stable', maxTimeLag })

// The workload answer's shape, cut to what is read.
const payload = {
  rows: [
    { key: 'channel.opsync_fetch', now: { deadLetter: 8624 }, window: { ackFailed: 2132, ackSuccess: 0, popMessages: 2130, pushMessages: 355 } },
    { key: 'pmsintool.default', now: { deadLetter: 3 }, window: { ackFailed: 7, ackSuccess: 24 } },
    { key: 'orders', now: { deadLetter: 0 }, window: { ackFailed: 40, ackSuccess: 1960 } },
    { key: 'quiet', now: {}, window: {} },
  ],
}
const ops = opsByQueue(payload)

test('opsByQueue keeps each queue\'s hour and its dead letters, zero for what is missing', () => {
  assert.deepEqual(ops.get('channel.opsync_fetch'), { ackFailed: 2132, ackSuccess: 0, popMessages: 2130, pushMessages: 355, deadLetter: 8624 })
  assert.deepEqual(ops.get('quiet'), { ackFailed: 0, ackSuccess: 0, popMessages: 0, pushMessages: 0, deadLetter: null })
  assert.equal(opsByQueue(null).size, 0)
  assert.equal(opsByQueue({ rows: [{ window: {} }] }).size, 0, 'a row without a key is skipped')
})

test('acksText says the hour in words', () => {
  assert.equal(acksText(ops.get('channel.opsync_fetch')), '2,132 of 2,132 acks failed (100%)')
  assert.equal(acksText(ops.get('orders')), '40 of 2,000 acks failed (2%)')
  assert.equal(acksText(ops.get('quiet')), 'no ack failed')
  assert.equal(acksText(null), '')
})

test('a queue failing its acks needs you, by the ack lines; a few failures are not a rate', () => {
  assert.equal(ackAttention('channel.opsync_fetch', undefined, ops).sev, 'bad')
  assert.equal(ackAttention('orders', undefined, ops).sev, 'warn', '2% is past the 1% line, under the 500 failures red needs')
  assert.equal(ackAttention('pmsintool.default', undefined, ops).sev, '', '7 failures are under the floor of 10')
  assert.equal(ackAttention('channel.opsync_fetch', undefined, null).sev, '', 'without the hour, acks are not judged')

  const out = queueAttention([q('channel.opsync_fetch', 1, 8624), q('pmsintool.default'), q('orders')], [
    group('channel.opsync_fetch', 2), group('orders', 1), group('pmsintool.default', 1),
  ], undefined, ops)
  assert.deepEqual(out.map((a) => [a.name, a.sev, a.reason]), [
    ['channel.opsync_fetch', 'bad', 'acks'],
    ['orders', 'warn', 'acks'],
  ])
  assert.equal(out[0].ops.deadLetter, 8624)
})

test('a queue behind and failing acks takes the worse; lag on a tie', () => {
  const behind = group('orders', 400) // past 5 min: bad
  assert.deepEqual(queueAttention([q('orders')], [behind], undefined, ops).map((a) => [a.sev, a.reason]), [['bad', 'lag']])
  const slightly = group('orders', 90) // past 1 min: warn, ties the acks' warn
  assert.deepEqual(queueAttention([q('orders')], [slightly], undefined, ops).map((a) => [a.sev, a.reason]), [['warn', 'lag']])
  const fine = group('channel.opsync_fetch', 2)
  assert.deepEqual(queueAttention([q('channel.opsync_fetch')], [fine], undefined, ops).map((a) => [a.sev, a.reason]), [['bad', 'acks']])
})

test('a queue\'s own ack lines judge it', () => {
  const linesFor = (name) => (name === 'orders' ? { ...THRESHOLDS, ackWarnRate: 0.05 } : THRESHOLDS)
  assert.equal(ackAttention('orders', linesFor, ops).sev, '', '2% is under this queue\'s 5% line')
})

test('the Alerts tab lists failing acks, also on a queue that is behind', () => {
  const alerts = queueAlerts(q('orders'), [group('orders', 400)], undefined, ops)
  assert.deepEqual(alerts.map((a) => [a.key, a.sev, a.tab]), [['lag:workers', 'bad', 'consumers'], ['acks', 'warn', 'failed']])
  const [acks] = queueAlerts(q('channel.opsync_fetch'), [group('channel.opsync_fetch', 2)], undefined, ops)
  assert.equal(acks.title, 'Its acks are failing')
  assert.equal(acks.detail, '2,132 of 2,132 acks failed (100%) in the last hour; 8,624 in dead letter.')
})

test('the verdict names the queue failing its acks', () => {
  const issues = queueAttention([q('channel.opsync_fetch')], [group('channel.opsync_fetch', 2)], undefined, ops)
  const v = overviewVerdict({ known: true, issues })
  assert.deepEqual(v, { sev: 'bad', word: 'Failing', headline: 'channel.opsync_fetch is failing its acks' })
  assert.equal(attentionWhy(issues[0]), '2,132 of 2,132 acks failed (100%) in the last hour')

  const mixed = [{ name: 'a', sev: 'bad', reason: 'acks' }, { name: 'b', sev: 'bad', reason: 'lag' }]
  assert.equal(overviewVerdict({ known: true, issues: mixed }).headline, '2 queues are failing')
  const two = [{ name: 'a', sev: 'bad', reason: 'acks' }, { name: 'b', sev: 'bad', reason: 'acks' }]
  assert.equal(overviewVerdict({ known: true, issues: two }).headline, '2 queues are failing their acks')
})

test('the triage list carries the hour and filters failing acks', () => {
  const rows = buildQueueTriage([q('channel.opsync_fetch', 1), q('orders'), q('calm')], [group('calm', 1)], new Map(), undefined, ops)
  assert.deepEqual(rows.slice(0, 2).map((r) => [r.name, r.sev, r.reason]), [['channel.opsync_fetch', 'bad', 'acks'], ['orders', 'warn', 'acks']])
  assert.equal(rows[0].ops.ackFailed, 2132)
  assert.deepEqual(filterQueueTriage(rows, '', 'acks').map((r) => r.name), ['channel.opsync_fetch', 'orders'])
})
