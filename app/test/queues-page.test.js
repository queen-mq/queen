// The Queues page of the new console (console/queues/queuesView.js): each
// queue's row from the shared stores' answers, its namespace, the segments,
// the order a header click froze, the line on top and the New queue draft.
// Shapes cut from stage on 2026-10-31: 123 queues in 9 namespaces, with
// channel.opsync_fetch failing every ack and tf.traces 100 s behind.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  HOUR_S, inSegment, namespaceGroups, newQueueDraft, nextSort, perSecond, queueRows, queuesVerdict,
  readerOf, segmentCounts, snapshotOrder, sparkValues, stateOf, hasNews,
} from '../src/console/queues/queuesView.js'
import { opsByQueue, seriesByQueue } from '../src/composables/queueOps.js'

const q = (name, namespace, { pending = 0, deadLetter = 0, partitions = 1, task = '' } = {}) =>
  ({ name, namespace, task, partitions, messages: { pending, deadLetter } })
const g = (queueName, name, maxTimeLag = 0, state = 'Stable') => ({ queueName, name, maxTimeLag, state, kind: 'queen' })
const minutes = (n, v) => Array.from({ length: n }, () => v)

const QUEUES = [
  q('channel.opsync_fetch', 'channel', { pending: 1, deadLetter: 8639, partitions: 3 }),
  q('channel.metric_sample', 'channel', { partitions: 3610 }),
  q('tf.traces', 'traceflow', { partitions: 2 }),
  q('tf.events', 'traceflow', { pending: 600, partitions: 2163 }),
  q('queen-alerts.probe', '', { partitions: 0 }),
  q('smartpms.iot', 'smartpms', { deadLetter: 54, partitions: 64, task: 'iot' }),
  q('orphan', 'smartpms', { pending: 12, partitions: 4 }),
]
const GROUPS = [
  g('channel.opsync_fetch', 'channel.opsync_fetch.worker', 2),
  g('channel.metric_sample', '__QUEUE_MODE__'),
  g('tf.traces', 'tf-archiver', 400),
  g('tf.events', 'tf-indexer', 20),
  g('smartpms.iot', 'iot-a'), g('smartpms.iot', 'iot-b'),
]
const WORKLOAD = {
  rows: [
    { key: 'channel.opsync_fetch', now: { deadLetter: 8639 }, window: { ackFailed: 2154, ackSuccess: 0, popMessages: 2154, pushMessages: 361 }, series: { push: [null, ...minutes(60, 6)], pop: [null, ...minutes(60, 36)], ackFailed: [null, ...minutes(60, 36)] } },
    { key: 'tf.events', now: { deadLetter: 0 }, window: { ackFailed: 0, ackSuccess: 8651, popMessages: 8650, pushMessages: 8663 }, series: { push: [null, ...minutes(60, 100)], pop: [null, ...minutes(60, 160)], ackFailed: [null, ...minutes(60, 0)] } },
    { key: 'tf.traces', now: { deadLetter: 0 }, window: { ackFailed: 0, ackSuccess: 2100, popMessages: 2100, pushMessages: 2160 }, series: { push: [null, ...minutes(60, 40)], pop: [null, ...minutes(60, 30)], ackFailed: [null, ...minutes(60, 0)] } },
    { key: 'channel.metric_sample', now: {}, window: { pushMessages: 0, popMessages: 0, ackSuccess: 0, ackFailed: 0 }, series: { push: [null, ...minutes(60, 0)], pop: [null, ...minutes(60, 0)], ackFailed: [] } },
  ],
}
const ops = opsByQueue(WORKLOAD)
const series = seriesByQueue(WORKLOAD)
const rows = queueRows({ queues: QUEUES, groups: GROUPS, ops, series })
const row = (name) => rows.find((r) => r.name === name)

test('a row carries what the list shows, from the stores alone', () => {
  const r = row('channel.opsync_fetch')
  assert.equal(r.ns, 'channel')
  assert.equal(r.pending, 1)
  assert.equal(r.dead, 8639)
  assert.equal(r.partitions, 3)
  assert.equal(r.named, 1)
  assert.equal(r.inHour, 361)
  // Acked is the hour's acks that succeeded, not its pops: this queue pops 2,154 and acks none.
  assert.equal(r.ackHour, 0)
  assert.equal(r.lag, 2)
  assert.deepEqual(r.reader, { label: 'channel.opsync_fetch.worker', mono: true })
  assert.equal(row('channel.metric_sample').named, 0)
  assert.equal(row('channel.metric_sample').queueMode, true)
  assert.equal(row('queen-alerts.probe').inHour, null, 'no workload row: not measured, not zero')
  assert.equal(row('queen-alerts.probe').ns, '')
})

test('the one rule marks the queues that need you, and the state says why', () => {
  assert.equal(row('channel.opsync_fetch').sev, 'bad')
  assert.deepEqual(row('channel.opsync_fetch').state, { kind: 'acks', word: 'Failing', tone: 'red' })
  // 400 s behind (past the 5 min line), and 40 a minute arrive while 30 leave: not draining.
  assert.equal(row('tf.traces').sev, 'bad')
  assert.equal(row('tf.traces').state.word, 'Not draining')
  // Nothing set up to read it, with messages waiting.
  assert.equal(row('orphan').state.word, 'No reader')
  assert.equal(row('orphan').state.tone, 'amber')
  // Behind by 20 s and draining faster than it fills: catching up, not flagged.
  const ev = row('tf.events')
  assert.equal(ev.sev, null)
  assert.equal(ev.state.kind, 'catching')
  assert.equal(Math.round(ev.state.eta), 600) // 600 waiting, (160 - 100) a minute = 1 a second
  assert.equal(row('channel.metric_sample').state, null)
})

test('stateOf: behind while draining is "Behind"; no traffic to judge is no claim', () => {
  assert.equal(stateOf({ issue: { reason: 'lag', sev: 'warn' }, inS: 1, outS: 2 }).word, 'Behind')
  assert.equal(stateOf({ issue: { reason: 'lag', sev: 'warn' } }).word, 'Behind')
  assert.equal(stateOf({ pending: 5 }), null)
  assert.equal(stateOf({ pending: 5, inS: 2, outS: 1 }), null)
})

test('perSecond and sparkValues leave out the minute still filling', () => {
  assert.equal(perSecond([null, 60, 60, 60, 60, 60, 6000]), 1)
  assert.equal(perSecond([null]), null)
  assert.deepEqual(sparkValues([null, null, 3, null, 5, 99]), [3, 0, 5])
  assert.deepEqual(sparkValues([]), [])
})

test('readerOf names the group that stands for a queue', () => {
  const qm = g('x', '__QUEUE_MODE__')
  assert.deepEqual(readerOf([qm], [qm]), { label: 'queue mode', mono: false })
  assert.deepEqual(readerOf([g('x', 'a'), g('x', 'b')], []), { label: '2 groups', mono: false })
  assert.deepEqual(readerOf([g('x', 'a', 5), g('x', 'b', 50)], [g('x', 'a', 5), g('x', 'b', 50)]), { label: 'b', mono: true })
})

test('segments: needs you, waiting, dead letters, with counts over every queue', () => {
  assert.deepEqual(segmentCounts(rows, 2), { all: 7, needs: 3, waiting: 3, dead: 2, eph: 2 })
  assert.equal(inSegment(row('smartpms.iot'), 'dead'), true)
  assert.equal(inSegment(row('smartpms.iot'), 'waiting'), false)
  assert.equal(inSegment(row('smartpms.iot'), 'all'), true)
})

test('namespaces by name, the default one last; a filter hides the empty ones', () => {
  const all = namespaceGroups(rows)
  assert.deepEqual(all.map((x) => x.ns), ['channel', 'smartpms', 'traceflow', ''])
  assert.deepEqual(all[0].rows.map((r) => r.name), ['channel.metric_sample', 'channel.opsync_fetch'])
  assert.deepEqual(all[0].counts, { failing: 1, behind: 0, noReader: 0, catching: 0 })
  // A term matches a name, a namespace or a task (navigation.queueMatches).
  const byTask = namespaceGroups(rows, { find: 'iot' })
  assert.deepEqual(byTask.map((x) => [x.ns, x.rows.length, x.total]), [['smartpms', 1, 2]])
  const needs = namespaceGroups(rows, { seg: 'needs' })
  assert.deepEqual(needs.map((x) => x.ns), ['channel', 'smartpms', 'traceflow'])
  assert.equal(hasNews(all[0]), true)
  assert.equal(hasNews(all[3]), false)
})

test('a header click freezes the order: the numbers move, the rows do not', () => {
  assert.deepEqual(nextSort(null, 'waiting'), { key: 'waiting', dir: 'desc' })
  assert.deepEqual(nextSort({ key: 'waiting', dir: 'desc' }, 'waiting'), { key: 'waiting', dir: 'asc' })
  assert.equal(nextSort({ key: 'waiting', dir: 'asc' }, 'waiting'), null)
  assert.deepEqual(nextSort({ key: 'waiting', dir: 'asc' }, 'dead'), { key: 'dead', dir: 'desc' })

  const rank = snapshotOrder(rows, { key: 'waiting', dir: 'desc' })
  const before = namespaceGroups(rows, { rank }).find((x) => x.ns === 'traceflow').rows.map((r) => r.name)
  assert.deepEqual(before, ['tf.events', 'tf.traces'])
  // A refresh: tf.traces now holds far more. Same rank, same order.
  const later = rows.map((r) => (r.name === 'tf.traces' ? { ...r, pending: 99_999 } : r))
  const after = namespaceGroups(later, { rank }).find((x) => x.ns === 'traceflow').rows.map((r) => r.name)
  assert.deepEqual(after, before)
  // A queue the snapshot never saw goes after the ones it did.
  const added = [...later, { ...row('tf.events'), name: 'tf.aaa' }]
  assert.deepEqual(namespaceGroups(added, { rank }).find((x) => x.ns === 'traceflow').rows.map((r) => r.name), ['tf.events', 'tf.traces', 'tf.aaa'])
  // Unknown values go last, whichever the direction.
  const byIn = snapshotOrder(rows, { key: 'in', dir: 'asc' })
  assert.equal(byIn.get('queen-alerts.probe') > byIn.get('channel.metric_sample'), true)
  assert.equal(snapshotOrder(rows, { key: 'nope', dir: 'asc' }), null)
})

test('the line on top: worst first, then the tenant acks, then all fine', () => {
  assert.deepEqual(queuesVerdict({ rows, loaded: false }), { kind: 'loading' })
  assert.deepEqual(queuesVerdict({ rows, loaded: true, groupsKnown: false }), { kind: 'unknown' })
  const v = queuesVerdict({ rows, loaded: true, groupsKnown: true, ops })
  assert.equal(v.kind, 'issues')
  assert.equal(v.tone, 'red')
  assert.equal(v.count, 3)
  assert.deepEqual(v.names, ['channel.opsync_fetch', 'tf.traces'])
  assert.equal(v.more, 1)
  const calm = rows.map((r) => ({ ...r, sev: null }))
  assert.deepEqual(queuesVerdict({ rows: calm, loaded: true, groupsKnown: true, ops: null }), { kind: 'fine' })
  // Acks failing across queues with no single one past its line: the Overview's tenant issue.
  const spread = new Map([['a', { ackFailed: 30, ackSuccess: 70 }], ['b', { ackFailed: 30, ackSuccess: 70 }]])
  const t = queuesVerdict({ rows: calm, loaded: true, groupsKnown: true, ops: spread })
  assert.equal(t.kind, 'tenant')
  assert.equal(t.failed, 60)
  assert.equal(t.total, 200)
})

test('the New queue draft: the call it makes, and what it refuses', () => {
  const known = new Map(QUEUES.map((x) => [x.name, x]))
  const empty = newQueueDraft({ name: '', leaseTime: '300', retryLimit: '3' }, known)
  assert.equal(empty.ok, false)
  assert.equal(empty.nameError, 'A queue needs a name.')

  const d = newQueueDraft({ name: 'orders.refunded', namespace: ' orders ', task: '', leaseTime: '60', retryLimit: '5' }, known)
  assert.equal(d.ok, true)
  assert.deepEqual(d.body, { queue: 'orders.refunded', options: { namespace: 'orders', leaseTime: 60, retryLimit: 5 }, mode: 'merge' })

  // A cleared field takes the broker's default, said on the wire.
  const blank = newQueueDraft({ name: 'x', leaseTime: '', retryLimit: '' }, known)
  assert.deepEqual(blank.body.options, { leaseTime: 300, retryLimit: 3 })

  // A name the cluster has: /configure would merge into it, so it is refused here.
  const taken = newQueueDraft({ name: 'smartpms.iot', leaseTime: '300', retryLimit: '3' }, known)
  assert.equal(taken.ok, false)
  assert.equal(taken.exists, 'smartpms.iot')
  assert.equal(taken.existsIn, 'smartpms')

  const bad = newQueueDraft({ name: 'y', leaseTime: 'soon', retryLimit: '-1' }, known)
  assert.equal(bad.ok, false)
  assert.match(bad.errors.leaseTime, /whole number/)
  assert.match(bad.errors.retryLimit, /negative/)
})

test('rates come from the hour', () => {
  assert.equal(HOUR_S, 3600)
  assert.equal(Math.round((row('tf.events').inHour / HOUR_S) * 100) / 100, 2.41)
})

test('newQueueDraft sends every other option only when it differs from the default', () => {
  const known = new Map()
  const d = newQueueDraft({
    name: 'orders.refunded', leaseTime: '300', retryLimit: '3',
    dedupWindowSeconds: '60', encryptionEnabled: true, priority: '0', retryDelay: '',
  }, known)
  assert.equal(d.ok, true)
  assert.deepEqual(d.extra, { dedupWindowSeconds: 60, encryptionEnabled: true })
  assert.equal(d.body.options.dedupWindowSeconds, 60)
  assert.equal(d.body.options.encryptionEnabled, true)
  assert.equal('priority' in d.body.options, false, 'at its default: not sent')
  assert.equal('retryDelay' in d.body.options, false, 'cleared: the default, not sent')
  const bad = newQueueDraft({ name: 'x', leaseTime: '300', retryLimit: '3', dedupWindowSeconds: '1.5' }, known)
  assert.equal(bad.ok, false)
  assert.ok(bad.errors.dedupWindowSeconds)
})
