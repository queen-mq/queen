// The Workload page's models (src/console/workload/workloadViews.js and
// workloadDeeper.js), held against payloads shaped like stage's answers of
// 2026-10-31 (2.7.0-beta.2, 123 queues in 9 namespaces). The page writes
// nothing: its one request is the GET readParams builds.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { enrichRows, trimOpenBucket } from '../src/composables/useWorkload.js'
import {
  bytes, compact, compareWindow, compareWords, countsModel, delta, findingsOf, fmt, groupsModel, inOrder,
  mapModel, marksOf, parts, pct, rankRows, rate, readCall, readParams, rowName, scopeName, scopeOf, span,
  stableKeys, tableRows, trimHeadBucket, verdictOf, wait, windowWords, workCharts,
} from '../src/console/workload/workloadViews.js'
import {
  baselineModel, bucketWords, bytesModel, costModel, deadModel, deeperNotes, livenessModel, livenessSum,
  retentionModel, weekModel,
} from '../src/console/workload/workloadDeeper.js'

// ---- payloads in stage's shape ------------------------------------------------------

const MIN = 60_000
const T0 = Date.parse('2026-10-31T09:05:00Z')
const iso = (ms) => new Date(ms).toISOString().replace('.000Z', 'Z')
const bucketsOf = (n, start = T0, width = MIN) => Array.from({ length: n }, (_, i) => iso(start + i * width))
const win = (o = {}) => ({
  pushMessages: 0, pushRequests: 0, popMessages: 0, fetchMessages: 0, popEmpty: 0, ackRequests: 0,
  ackSuccess: 0, ackFailed: 0, transactions: 0, conflated: 0, partitionsCreated: 0, partitionsDeleted: 0,
  parkedAvg: 0, avgLagMs: null, maxLagMs: null, ...o,
})
const now = (o = {}) => ({
  pending: 0, processing: 0, deadLetter: 0, retainedBytes: 0, partitions: 0, groups: 0,
  queuesWithoutGroup: 0, pendingWithoutGroup: 0, queuesTouched: 0, queuesActive: 0, ...o,
})
const series = (n, pop = []) => ({
  push: new Array(n).fill(null), pop: Array.from({ length: n }, (_, i) => (pop[i] === undefined ? null : pop[i])),
  fetch: new Array(n).fill(null), popEmpty: new Array(n).fill(null), ackFailed: new Array(n).fill(null),
  parked: new Array(n).fill(null), avgLagMs: new Array(n).fill(null), maxLagMs: new Array(n).fill(null),
})
const row = (key, o = {}, n = 4, pop = []) => ({ key, queues: 1, window: win(o.window), now: now(o.now), series: series(n, pop) })

/** The namespace read of the last hour, as stage answered it (cut to its shape). */
function nsPayload() {
  const n = 4
  return {
    bucketMinutes: 1,
    groupBy: 'namespace',
    timeRange: { from: '2026-10-31T09:05:18.000Z', to: '2026-10-31T09:09:18.000Z' },
    buckets: bucketsOf(n),
    nodesMissing: [],
    rows: [
      row('traceflow', { window: { popMessages: 10889, pushMessages: 10888, ackSuccess: 10889, popEmpty: 11000, avgLagMs: 9, maxLagMs: 127 }, now: { pending: 15, processing: 15, partitions: 5778, groups: 6, queuesTouched: 6, queuesActive: 2, retainedBytes: 10_000_000 } }, n, [null, 180, 190, 200]),
      row('channel', { window: { popMessages: 3772, pushMessages: 1961, ackSuccess: 1604, ackFailed: 2157, popEmpty: 27000, avgLagMs: 2100, maxLagMs: 120020 }, now: { deadLetter: 8639, partitions: 76, groups: 20, queuesTouched: 8, queuesActive: 5, retainedBytes: 24_000_000 } }, n, [null, 60, 62, 64]),
      row('pmsintool', { window: { popMessages: 31, pushMessages: 17, ackSuccess: 24, ackFailed: 7, popEmpty: 8350, avgLagMs: 1500, maxLagMs: 4200 }, now: { deadLetter: 161, partitions: 65, groups: 7, queuesTouched: 7, retainedBytes: 980_000 } }, n, [null, 0, 31, 0]),
      row('gate', { window: { popEmpty: 29062 }, now: { partitions: 8, groups: 19, queuesTouched: 19 } }, n),
      row('', { window: {}, now: { partitions: 64, queuesTouched: 0 } }, n),
    ].map((r) => ({ ...r, queues: { traceflow: 6, channel: 16, pmsintool: 7, gate: 19, '': 2 }[r.key] })),
    tenant: {
      queues: 123,
      window: win({ pushMessages: 12901, popMessages: 14772, ackSuccess: 12624, ackFailed: 2164, popEmpty: 395569, avgLagMs: 541, maxLagMs: 120020 }),
      now: now({ pending: 15, processing: 15, deadLetter: 8903, retainedBytes: 184539006, partitions: 6920, groups: 144 }),
      series: series(n, [null, 240, 283, 264]),
    },
  }
}
const nsRows = (payload = nsPayload(), groups = new Map(), meta = new Map()) => enrichRows(payload, groups, meta, 'namespace')

// ---- words for numbers ------------------------------------------------------------------

test('numbers in words: en-US integers, shares that never round to 100% while one failed, 1024-based bytes', () => {
  assert.equal(fmt(14772), '14,772')
  assert.equal(fmt(null), '–')
  assert.equal(pct(0.733), '73%')
  assert.equal(pct(0.036), '3.6%')
  assert.equal(pct(0.0004), '<0.1%')
  assert.equal(pct(2127 / 2128), '99.9%')
  assert.equal(pct(1), '100%')
  assert.equal(pct(0), '0%')
  assert.equal(bytes(184539006), '176 MB')
  assert.equal(bytes(240), '240 B')
  assert.equal(compact(8650), '8.7k')
  assert.equal(compact(395569), '396k')
  assert.equal(rate(4.88), '4.88')
  assert.equal(rate(0.03), '0.03')
  assert.equal(rate(1234.4), '1,234')
  assert.equal(span(38), '38 s')
  assert.equal(span(125 * 60), '2 h 5 min')
  assert.equal(wait(541), '541 ms')
  assert.equal(wait(5125), '5.1 s')
  assert.equal(wait(120020), '2 min')
  assert.equal(delta({ pct: 0.12 }), '+12%')
  assert.equal(delta({ pct: -0.034 }), '−3.4%')
  assert.equal(delta({ pct: null }), '–')
  assert.deepEqual(parts('`channel` holds 3'), [{ t: 'channel', mono: true }, { t: ' holds 3', mono: false }])
})

// ---- the scope and its one request ---------------------------------------------------------

test('the scope: a pick is groupBy=queue with the namespace (or task) as a filter, and the empty group is a real pick', () => {
  const iso2 = { from: '2026-10-31T09:05:18.000Z', to: '2026-10-31T10:05:18.000Z' }
  const root = scopeOf('namespace', null)
  assert.equal(root.level, 'namespace')
  assert.deepEqual(readParams(root, iso2), { ...iso2, groupBy: 'namespace' })
  assert.equal(readCall(root), 'GET /api/v1/analytics/workload?groupBy=namespace')

  const channel = scopeOf('namespace', 'channel')
  assert.equal(channel.level, 'queue')
  assert.deepEqual(readParams(channel, iso2), { ...iso2, groupBy: 'queue', namespace: 'channel' })
  assert.equal(readCall(channel), 'GET /api/v1/analytics/workload?groupBy=queue&namespace=channel')

  const none = scopeOf('task', '')
  assert.deepEqual(readParams(none, iso2), { ...iso2, groupBy: 'queue', task: '' })
  assert.equal(scopeName(none), 'No task')
  assert.notEqual(none.sig, scopeOf('task', null).sig)

  // grouped by queue, the list still names namespaces: a pick narrows by namespace
  const q = scopeOf('queue', 'smartpms')
  assert.equal(q.listBy, 'namespace')
  assert.deepEqual(readParams(q, iso2), { ...iso2, groupBy: 'queue', namespace: 'smartpms' })
  assert.equal(scopeName(scopeOf('queue', null)), 'All queues')
  assert.equal(scopeOf('bogus', null).by, 'namespace')
})

test('the window in words, and the one compared with moved back by its span, a day or a week', () => {
  const live = { id: '1h', live: true, custom: false, from: T0, to: T0 + 60 * MIN, minutes: 60, long: 'last hour, live', label: 'Live' }
  assert.deepEqual(windowWords(live), { label: 'Last hour', inThe: 'in the last hour' })
  assert.equal(compareWords('previous', live), 'the hour before')
  assert.equal(compareWords('previous', { ...live, minutes: 360 }), 'the 6 hours before')
  assert.equal(compareWords('yesterday', live), 'the same time yesterday')
  assert.equal(compareWords('previous', { ...live, custom: true, minutes: 37 }), 'the 37 min before')
  assert.equal(compareWords('off', live), '')
  const prev = compareWindow(live, 'previous')
  assert.equal(prev.to, T0)
  assert.equal(prev.shift, 60 * MIN)
  assert.equal(compareWindow(live, 'lastWeek').shift, 7 * 24 * 60 * MIN)
  assert.equal(compareWindow(live, 'off'), null)
})

// ---- rows never re-sort by themselves -----------------------------------------------------

test('the order: busiest first when a scope is read, then kept; a new row joins at the end, a new scope ranks again', () => {
  const rows = nsRows()
  const first = stableKeys(null, rows, 'a')
  assert.deepEqual(first.keys, ['traceflow', 'channel', 'pmsintool', 'gate', ''])
  // a refresh in which channel overtakes traceflow moves nothing
  const flipped = rows.map((r) => (r.key === 'channel' ? { ...r, reads: 99999 } : r)).concat([{ key: 'smartchat', reads: 50000, window: win() }])
  const again = stableKeys(first, flipped, 'a')
  assert.deepEqual(again.keys, ['traceflow', 'channel', 'pmsintool', 'gate', '', 'smartchat'])
  assert.deepEqual(stableKeys(first, flipped, 'b').keys.slice(0, 3), ['channel', 'smartchat', 'traceflow'])
  assert.deepEqual(inOrder(flipped, again.keys).map((r) => r.key).at(-1), 'smartchat')
  assert.deepEqual(rankRows([{ key: '', reads: 0 }, { key: 'b', reads: 0 }, { key: 'a', reads: 0 }]).map((r) => r.key), ['a', 'b', ''])
})

test('the two cut buckets: the open one on the right and the one the window began inside of on the left', () => {
  const p = nsPayload()
  const cut = trimHeadBucket(p)
  assert.equal(cut.buckets.length, 3)
  assert.equal(cut.buckets[0], iso(T0 + MIN))
  assert.deepEqual(cut.tenant.series.pop, [240, 283, 264])
  assert.deepEqual(cut.rows[0].series.pop, [180, 190, 200])
  assert.equal(cut.tenant.window.popMessages, 14772, 'the broker\'s totals keep it')
  // a window that began on a bucket's edge loses nothing
  const aligned = { ...p, timeRange: { from: iso(T0), to: p.timeRange.to } }
  assert.equal(trimHeadBucket(aligned), aligned)
  // with the open tail (useWorkload trimOpenBucket) the two compose
  const both = trimHeadBucket(trimOpenBucket(p, T0 + 3.5 * MIN))
  assert.deepEqual(both.buckets, [iso(T0 + MIN), iso(T0 + 2 * MIN)])
})

// ---- the counts --------------------------------------------------------------------------------

test('the counts: ack failures judged as a share past a floor (red on stage), deltas against the window compared with', () => {
  const t = nsPayload().tenant
  const c = countsModel(t, win({ pushMessages: 13100, popMessages: 14772, ackSuccess: 12700, ackFailed: 2200, popEmpty: 400000 }), { seconds: 3600 })
  const by = (list, k) => list.find((x) => x.k === k)
  assert.equal(by(c.now, 'queues').v, '123')
  assert.equal(by(c.now, 'retained').v, '176 MB')
  assert.equal(by(c.now, 'pending').tone, '', '15 pending at 3.5 acks a second is seconds of work')
  assert.equal(by(c.window, 'ackf').tone, 'red', '2,164 of 14,788 acks failed: 14.6%, past 500 failures')
  assert.equal(by(c.window, 'push').delta, '−1.5%')
  assert.equal(by(c.window, 'pop').delta, '0%')
  assert.equal(by(c.window, 'fill').v, '3.6%')
  assert.equal(by(c.window, 'fetch'), undefined, 'no Kafka fetches: no Kafka count')
  const k = countsModel({ ...t, window: { ...t.window, fetchMessages: 40 } }, null, { seconds: 3600 })
  assert.equal(by(k.window, 'fetch').v, '40')
  assert.equal(by(k.window, 'fetch').delta, '', 'no comparison, no change')
  const few = countsModel({ ...t, window: win({ ackSuccess: 24, ackFailed: 7 }) }, null, { seconds: 3600 })
  assert.equal(by(few.window, 'ackf').tone, '', 'seven failures are under the floor of ten')
})

// ---- what needs you ------------------------------------------------------------------------------

test('what needs you: the console\'s one ack rule, a minute\'s wait for a pop, and dead-letter depth as a note', () => {
  const rows = nsRows()
  const keys = stableKeys(null, rows, 'x').keys
  const { needs, notes } = findingsOf(rows, keys, { inThe: 'in the last hour' })
  assert.deepEqual(needs.map((f) => [f.key, f.kind, f.tone]), [['channel', 'acks', 'red'], ['channel', 'slow', 'amber']])
  assert.equal(needs[0].text, '`channel`: 2,157 of 3,761 acks failed in the last hour.')
  assert.equal(needs[1].text, 'A message on `channel` waited 2 min for a pop.')
  // pmsintool: 7 of 31 acks failed, under the floor: nothing, as on the Overview
  assert.ok(!needs.some((f) => f.key === 'pmsintool'))
  assert.equal(marksOf(needs).get('channel'), 'red')
  // the dead letters are history: a note, never a mark
  const dlq = notes.find((n) => n.kind === 'dlq')
  assert.equal(dlq.text, '`channel` holds 8,639 dead letters.')
  assert.equal(dlq.tone, '')
  // empty polls fold into one line for every row they fit
  const empty = notes.filter((n) => n.kind === 'empty')
  assert.equal(empty.length, 1)
  assert.match(empty[0].text, /^`pmsintool` and `gate` answered almost every poll with nothing\.$/)
  assert.match(empty[0].evidence, /^37,412 empty polls for 31 messages; no long poll waits on them\.$/)
  // nothing pushed, read or polled and no group: a cleanup note, said in words for the empty group
  const idle = notes.find((n) => n.kind === 'idle')
  assert.match(idle.text, /^The queues without a namespace: nothing pushed or read in the last hour, and no consumer group\.$/)
})

test('what needs you: no reader and a group behind need the consumer groups, judged by each queue\'s lines', () => {
  const p = nsPayload()
  p.rows[1].now.pendingWithoutGroup = 40
  p.rows[1].now.pending = 40
  p.rows[1].now.queuesWithoutGroup = 1
  const meta = new Map([
    ['channel.opsync_fetch', { namespace: 'channel' }],
    ['channel.ops_task', { namespace: 'channel' }],
  ])
  const groups = new Map([['channel.opsync_fetch', [{
    name: 'channel.opsync_fetch.worker', queueName: 'channel.opsync_fetch', kind: 'queen', state: 'Lagging',
    maxTimeLag: 400, partitionsWithLag: 1, partitionCursors: 3, members: 3, totalLag: 12,
  }]]])
  const rows = nsRows(p, groups, meta)
  const keys = stableKeys(null, rows, 'x').keys
  const { needs } = findingsOf(rows, keys, { inThe: 'in the last hour' })
  const behind = needs.find((f) => f.kind === 'behind')
  assert.equal(behind.tone, 'red', '400 s is past the 300 s line')
  assert.equal(behind.text, '`channel.opsync_fetch.worker` on `channel.opsync_fetch` is 7 min behind.')
  assert.equal(behind.evidence, '1 of 3 partitions behind, 12 messages waiting.')
  const noReader = needs.find((f) => f.kind === 'noReader')
  assert.equal(noReader.tone, 'amber')
  assert.equal(noReader.text, '`channel` holds 40 messages on queues no consumer group reads.')
  // a queue's own lines: 400 s is fine where the queue may run 10 minutes behind
  const lenient = () => ({ lagWarnSeconds: 600, lagBadSeconds: 1800 })
  assert.ok(!findingsOf(nsRows(p, groups, meta), keys, { linesFor: lenient }).needs.some((f) => f.kind === 'behind'))
  // without the groups those two rules say nothing at all
  const off = findingsOf(rows, keys, { hasGroups: false }).needs
  assert.ok(!off.some((f) => f.kind === 'behind' || f.kind === 'noReader'))
})

test('the verdict: one finding said whole, several counted, none says who works; the scope is named', () => {
  const rows = nsRows()
  const keys = stableKeys(null, rows, 'x').keys
  const scope = scopeOf('namespace', null)
  const { needs } = findingsOf(rows, keys, {})
  const many = verdictOf({ loaded: true, needs, rows, keys, scope })
  assert.deepEqual([many.tone, many.badge, many.text, many.list.length], ['red', 'Failing', '2 things need you.', 2])
  const one = verdictOf({ loaded: true, needs: needs.slice(1), rows, keys, scope })
  assert.deepEqual([one.tone, one.badge, one.list.length], ['amber', 'Slow pops', 0])
  assert.equal(one.sub, 'On average a message waited 2.1 s.')
  const fine = verdictOf({ loaded: true, needs: [], rows, keys, scope })
  assert.equal(fine.text, 'Nothing is stuck. The busiest are `traceflow` and `channel`.')
  const quiet = verdictOf({ loaded: true, needs: [], rows: [rows[3]], keys, scope: scopeOf('namespace', ''), inThe: 'in the last hour' })
  assert.equal(quiet.text, 'Nothing was pushed or read in the queues without a namespace in the last hour.')
  assert.equal(verdictOf({ loaded: true, needs: needs.concat(needs), rows, keys, scope: scopeOf('task', 'core') }).text, '4 things need you in `core`.')
  assert.equal(verdictOf({ loaded: false, error: 'HTTP 503' }).text, 'The broker did not answer: HTTP 503.')
  assert.equal(verdictOf({ loaded: false }).loading, true)
})

// ---- the charts, the table, the map -----------------------------------------------------------------

test('work over time: a second, gaps drawn as 0, the busiest few and the rest as one, the compared window moved over this one', () => {
  const p = trimHeadBucket(nsPayload())
  const rows = nsRows(p)
  const keys = stableKeys(null, rows, 'm', (r) => r.window.popMessages).keys
  const prevP = trimHeadBucket(nsPayload())
  const m = workCharts(rows, keys, {
    buckets: p.buckets, bucketMinutes: 1, measure: 'pop', scopeLabel: 'All namespaces', compareLabel: 'the hour before', limit: 2,
    prev: { rows: nsRows(prevP), buckets: prevP.buckets.map((b) => iso(Date.parse(b) - 60 * MIN)), shift: 60 * MIN },
  })
  assert.equal(m.series[0].label, 'All namespaces')
  assert.deepEqual(m.series[0].points[0], [T0 + MIN + 30_000, 240 / 60])
  assert.equal(m.series[1].label, 'The hour before')
  assert.equal(m.series[1].dashed, true)
  assert.deepEqual(m.series[1].points.map((x) => x[0]), m.series[0].points.map((x) => x[0]), 'moved over this window')
  assert.deepEqual(m.small.map((s) => s.label), ['traceflow', 'channel', 'pmsintool'], 'one left over is named, not "1 others"')
  assert.equal(m.small[2].points[0][1], 0, 'a minute without a row is 0')
  assert.equal(m.quiet, 2)
  assert.equal(m.peak, 283 / 60)
  const five = workCharts(rows, keys, { buckets: p.buckets, bucketMinutes: 1, measure: 'pop', limit: 1 })
  assert.equal(five.small[1].label, '2 others')
  assert.deepEqual(five.small[1].names, ['channel', 'pmsintool'])
  const gone = workCharts(rows, keys, { buckets: p.buckets, measure: 'pop', compareLabel: 'the same time last week', prev: { rows: [], buckets: [], shift: 1 } })
  assert.equal(gone.series.length, 1)
  assert.equal(gone.prevNote, 'Nothing was recorded the same time last week, so it is not drawn.')
  const parked = workCharts(rows, keys, { buckets: p.buckets, measure: 'parked' })
  assert.equal(parked.measure.rate, false)
})

test('the table: every cell of every tab, in the kept order, coloured only where a rule says so', () => {
  const p = nsPayload()
  const rows = nsRows(p)
  const keys = stableKeys(null, rows, 'x').keys
  const { needs } = findingsOf(rows, keys, {})
  const prev = new Map(rows.map((r) => [r.key, { ...r, reads: r.key === 'channel' ? 2000 : r.reads }]))
  const t = tableRows(rows, keys, { prev, prevTenantReads: 14000, tenantNow: p.tenant.now, marks: marksOf(needs) })
  const ch = t.find((r) => r.key === 'channel')
  assert.equal(ch.mark, 'red')
  assert.equal(ch.share.text, '26%')
  assert.equal(Math.round(ch.share.prev * 1000) / 1000, 0.143)
  assert.equal(ch.ack.tone, 'red')
  assert.equal(ch.ack.sub, '2,157 failed')
  assert.equal(ch.lag.text, '2.1 s → 2 min')
  assert.equal(ch.lag.tone, 'amber')
  assert.equal(ch.dlq.link, null, 'a namespace links to its queues, not to one queue\'s dead letters')
  assert.equal(ch.oldest.text, 'no group', 'no groups known for it')
  const pms = t.find((r) => r.key === 'pmsintool')
  assert.equal(pms.ack.tone, '', 'under the floor')
  assert.equal(pms.fill.text, '0.4%')
  const empty = t.find((r) => r.key === '')
  assert.deepEqual([empty.name, empty.mono], ['No namespace', false])
  assert.equal(empty.delivered.dim, true)
  assert.equal(empty.churn.text, '0')
  const tf = t.find((r) => r.key === 'traceflow')
  assert.equal(tf.partitions.sub, '963 a queue')
  assert.equal(tf.pending.sub, '15 in flight')
  // without the consumer groups no one knows how far behind a row is
  assert.equal(tableRows(rows, keys, { hasGroups: false })[0].oldest.text, '–')
  // a queue row links to its page and its dead letters
  const q = enrichRows({ ...p, rows: [{ ...p.rows[1], key: 'channel.opsync_fetch' }] }, new Map(), new Map(), 'queue')
  const qt = tableRows(q, ['channel.opsync_fetch'], {})[0]
  assert.equal(qt.to, '/queues/channel.opsync_fetch')
  assert.deepEqual(qt.dlq.link, { path: '/dlq', query: { queue: 'channel.opsync_fetch' } })
  assert.deepEqual(rowName(q[0], 'namespace'), { text: 'channel.opsync_fetch', mono: true })
})

test('the activity map: square-root grey, a minute with no row outlined, the rows that delivered nothing named once', () => {
  const p = trimHeadBucket(nsPayload())
  const rows = nsRows(p)
  const m = mapModel(rows, stableKeys(null, rows, 'x').keys, { buckets: p.buckets, bucketMinutes: 1 })
  assert.equal(m.max, 200)
  assert.deepEqual(m.rows.map((r) => r.name), ['traceflow', 'channel', 'pmsintool'])
  assert.equal(m.rows[0].cells[2].p, 100)
  assert.equal(m.rows[1].cells[0].p, Math.round(Math.sqrt(60 / 200) * 100))
  assert.equal(m.rows[2].cells[0].na, false, 'a row with 0 is measured')
  assert.deepEqual(m.idle, ['gate', 'No namespace'])
})

test('one queue\'s groups: worst first, the Kafka groups nobody runs last, and the order kept while it is open', () => {
  const g = (name, o = {}) => ({ name, queueName: 'q', kind: 'queen', state: 'Stable', maxTimeLag: 0, partitionsWithLag: 0, partitionCursors: 3, totalLag: 0, ...o })
  const list = [
    g('a'),
    g('kafka-old', { kind: 'kafka', state: 'Dead', inactiveSeconds: 7200, maxTimeLag: 90000 }),
    g('b', { state: 'Lagging', maxTimeLag: 120, partitionsWithLag: 2, totalLag: 30 }),
    g('__QUEUE_MODE__', { state: 'Dead' }),
  ]
  const m = groupsModel(list)
  assert.deepEqual(m.rows.map((r) => r.key), ['b', 'a', '__QUEUE_MODE__', 'kafka-old'])
  assert.deepEqual([m.rows[0].mark, m.rows[0].state, m.rows[0].oldest, m.rows[0].behind], ['amber', 'behind', '2 min', '2 of 3'])
  assert.deepEqual([m.rows[2].name, m.rows[2].mono, m.rows[2].state], ['queue mode', false, 'stopped'])
  assert.equal(m.rows[3].state, 'not running')
  assert.match(m.rows[3].presence, /^last seen/)
  assert.equal(m.summary, '4 groups: 1 behind, 1 stopped, 1 not running.')
  // a's lag grows past b's on a refresh: the open list does not move
  const later = groupsModel(list.map((x) => (x.name === 'a' ? { ...x, maxTimeLag: 500 } : x)), undefined, m.keys)
  assert.deepEqual(later.rows.map((r) => r.key), ['b', 'a', '__QUEUE_MODE__', 'kafka-old'])
  assert.equal(groupsModel([g('only')]).summary, 'One group, keeping up.')
})

// ---- the deeper layer ----------------------------------------------------------------------------------

const H = 60 * MIN
const week = (hours, start, popAt) => {
  const buckets = bucketsOf(hours, start, H)
  const pop = buckets.map((b, i) => popAt(i, b))
  return {
    bucketMinutes: 60, groupBy: 'namespace', buckets, timeRange: { from: buckets[0], to: iso(start + hours * H) },
    tenant: { queues: 2, window: win(), now: now(), series: { ...series(hours), pop } },
    rows: [{ key: 'channel', queues: 1, window: win(), now: now(), series: { ...series(hours), pop: pop.map((v) => (v === null ? null : v / 2)) } }],
  }
}

test('the week: one hour a cell, UTC, the open bucket left out, and what was not recorded said', () => {
  const start = Date.parse('2026-10-30T09:00:00Z') // a Friday
  const p = week(26, start, (i) => (i === 7 ? 21061 : 14000))
  const w = weekModel(p, null)
  assert.equal(w.has, true)
  assert.equal(w.rows[0].day, 'Mon')
  const fri = w.rows.find((r) => r.day === 'Fri')
  assert.equal(fri.cells[16].p, 100)
  assert.equal(fri.cells[8].na, true, 'not recorded')
  assert.match(w.stats, /The busiest hour was Friday at 16:00 UTC, with 21,061/)
  assert.equal(weekModel(p, 'channel').has, true)
  assert.equal(weekModel(p, 'gate').has, false)
  assert.equal(weekModel(null).has, false)
})

test('is this hour normal: the tenant first, amber at twice or half, nothing to compare without an earlier day', () => {
  const start = Date.parse('2026-10-28T10:00:00Z')
  const p = week(3 * 24 + 1, start, (i, b) => (b.endsWith('T09:00:00Z') ? (b.startsWith('2026-10-31') ? 300 : 100) : 50))
  const b = baselineModel(p)
  assert.equal(b.label, 'Sat 09:00 UTC')
  assert.equal(b.rows[0].name, 'Every queue')
  assert.equal(b.rows[0].ratio.text, '3×')
  assert.equal(b.rows[0].ratio.tone, 'amber')
  assert.equal(b.rows[1].mono, true)
  const short = baselineModel(week(25, Date.parse('2026-10-30T09:00:00Z'), () => 100))
  assert.equal(short.rows[0].ratio.text, 'no earlier day')
  assert.equal(short.rows[0].bar, null)
})

const effPayload = () => ({
  bucketMinutes: 15, groupBy: 'queue', buckets: bucketsOf(4, T0, 15 * MIN), timeRange: { from: iso(T0), to: iso(T0 + 60 * MIN) },
  tenant: { queues: 3, window: win(), now: now(), series: series(4) },
  rows: [
    { key: 'channel.opsync_fetch', window: win({ pushMessages: 355, pushRequests: 1, popMessages: 2128, ackSuccess: 0, ackFailed: 2128, ackRequests: 2128, partitionsCreated: 0, partitionsDeleted: 0 }), now: now({ deadLetter: 8639, retainedBytes: 3_100_000 }), series: { ...series(4), maxLagMs: [5000, 4000, null, 3000] } },
    { key: 'channel.metric_rollup', window: win({ pushMessages: 492, pushRequests: 492, popMessages: 501, ackSuccess: 488, ackRequests: 488 }), now: now({ retainedBytes: 3_600_000 }), series: { ...series(4), maxLagMs: Array(4).fill(120020) } },
    { key: 'tf.events', window: win({ pushMessages: 0, transactions: 900, popMessages: 8570, ackSuccess: 0, partitionsCreated: 52513, partitionsDeleted: 52587 }), now: now({ retainedBytes: 6_000_000 }), series: series(4) },
  ],
})

test('what one push costs: amber for redeliveries and a wait that keeps happening, never twice for acks', () => {
  const groups = new Map([['channel.opsync_fetch', [{ name: 'w' }]], ['channel.metric_rollup', [{ name: 'r' }]], ['tf.events', [{ name: 'e' }]]])
  const c = costModel(effPayload(), groups)
  assert.deepEqual(c.rows.map((r) => r.key), ['tf.events', 'channel.opsync_fetch', 'channel.metric_rollup'])
  const fetch = c.rows[1]
  assert.deepEqual([fetch.fanout.text, fetch.fanout.tone], ['5.99×', 'amber'])
  assert.deepEqual([fetch.acks.text, fetch.acks.tone], ['0%', undefined])
  assert.equal(fetch.batch.push, '355')
  assert.equal(fetch.budget.text, 'all under 10 s')
  const tf = c.rows[0]
  assert.deepEqual([tf.fanout.text, tf.fanout.dim], ['fed by transactions', true])
  assert.equal(tf.acks.sub, 'acked in transactions')
  // a chronic wait: under 20 busy buckets is not yet a pattern
  assert.equal(c.rows[2].budget.text, '100% over a minute')
  assert.equal(c.rows[2].budget.tone, '')
  assert.equal(bucketWords(15), 'quarter hours')
  const only = costModel(effPayload(), groups, (q) => q.startsWith('channel.'))
  assert.equal(only.rows.length, 2)
})

test('where messages go: retention by queue or "cannot split", partitions from one counter, dead letters with their cause', () => {
  assert.deepEqual(retentionModel({ totals: {} }), { ungrouped: true, rows: [] })
  const ret = retentionModel({ rows: [
    { key: 'tf.events', totals: { retentionMsgs: 5886, completedRetentionMsgs: 0, evictionMsgs: 0 } },
    { key: 'q.lost', totals: { retentionMsgs: 1, completedRetentionMsgs: 0, evictionMsgs: 12 } },
    { key: 'q.none', totals: { retentionMsgs: 0, completedRetentionMsgs: 0, evictionMsgs: 0 } },
  ] })
  assert.deepEqual(ret.rows.map((r) => [r.key, r.tone]), [['tf.events', ''], ['q.lost', 'amber']])

  const eff = costModel(effPayload(), new Map()).eff
  const live = { rows: [
    { queue: 'tf.events', partitions: 3611, live24h: 2164, created24h: 2164 },
    { queue: 'smartpms.mail', partitions: 64, live24h: 64, created24h: 0 },
  ] }
  const l = livenessModel(live, eff)
  assert.deepEqual([l[0].created, l[0].deleted, l[0].liveText], ['52,513', '52,587', '60%'])
  assert.deepEqual([l[1].created, l[1].deleted], ['0', '–'], 'no 24 h row: the route\'s survivors, no deletions')
  const sum = livenessSum(live, () => true, eff)
  assert.deepEqual([sum.total, sum.written, sum.churn.queue, sum.churn.created24h], [3675, 2228, 'tf.events', 52513])

  const sigs = new Map([['channel.opsync_fetch', { data: { queue: 'channel.opsync_fetch', sample: 150, rowsNow: 8639, signatures: [{ text: 'fetch: hotel_id not configured', share: 1, n: 150 }] } }]])
  const d = deadModel(eff, sigs)
  assert.equal(d.length, 1)
  assert.deepEqual([d[0].dead, d[0].cause.text, d[0].shareText], ['8,639', 'fetch: hotel_id not configured', '100% of the newest 150'])
  assert.deepEqual(d[0].to, { path: '/dlq', query: { queue: 'channel.opsync_fetch' } })
  assert.equal(deadModel(eff, new Map())[0].cause.text, 'Reading its causes…')
  assert.equal(deadModel(eff, new Map([['channel.opsync_fetch', { error: new Error('x') }]]))[0].cause.text, 'Its causes could not be read.')

  const b = bytesModel(eff, [{ name: 'tf.events', segments: { messages: 8650 } }, { name: 'channel.metric_rollup', messages: { total: 0 } }])
  assert.deepEqual(b.map((r) => [r.key, r.retained, r.each]), [['tf.events', '5.7 MB', '694 B']])
})

test('what the deeper layer adds up to: loss by policy and a single error in amber, what the counters cannot attribute in grey', () => {
  const eff = costModel(effPayload(), new Map()).eff.map((e) => (e.queue === 'tf.events' ? { ...e, pop: 8570 } : e))
  const notes = deeperNotes({
    eff,
    retention: [{ key: 'q.lost', evicted: 12, age: 1 }],
    sigs: [
      { queue: 'a', rowsNow: 8639, sample: 150, avgBytes: 240, signatures: [{ text: 'x', share: 1 }] },
      { queue: 'b', rowsNow: 161, sample: 150, avgBytes: 200_000, signatures: [{ text: 'y', share: 1 }] },
    ],
    liveness: { total: 6920, written: 5336, churn: { queue: 'tf.events', created24h: 52476 } },
  })
  assert.deepEqual(notes.map((n) => n.tone), ['amber', 'amber', 'amber', '', ''])
  assert.equal(notes[0].text, '`q.lost` dropped messages nobody had read: they outlived its max wait.')
  assert.equal(notes[1].text, 'The dead letters of `a` and `b` are each mostly one error: fixing it clears most of them.')
  assert.match(notes[2].text, /^Dead letters of 195 KB each on `b`/)
  assert.equal(notes[3].text, '`tf.events` is fed through transactions: its pushes are not counted, so reads per push cannot be measured.')
  assert.equal(notes[4].text, '5,336 of 6,920 partitions were written to in the last 24 hours.')
  assert.deepEqual(deeperNotes(), [])
})
