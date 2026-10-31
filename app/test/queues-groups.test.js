// The Consumer groups view of the Queues section (console/queues/groupsView.js):
// each (group, queue) row from the shared stores' answers, what needs you by
// the app's one rule, how fast what waits drains, the namespaces, the order a
// header click froze, the line on top, the partitions behind, and the calls
// the actions send. Shapes cut from stage on 2026-10-31: 74 rows of 57 groups
// over 55 queues, smartpms on 14 queues, channel.opsync_fetch failing every
// ack, tf.traces read by tf-archiver alone, smartchat.router.outgoing by 10.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  addSamples, behindRows, deleteCall, deleteOutcome, groupLabel, groupRows, groupsVerdict, hasNews, keepPlaces,
  matches, namespaceGroups, nextSort, parseWhen, partitionCall, pickedRow, rateTrend, restartOf, rowKey, sampleTrend,
  seekCall, seekOutcome, segmentCounts, sendCall, snapshotOrder, statusOf, whenText, QUEUE_MODE,
} from '../src/console/queues/groupsView.js'
import { opsByQueue, seriesByQueue } from '../src/composables/queueOps.js'

const q = (name, namespace, partitions = 1) => ({ name, namespace, partitions, messages: { pending: 0, deadLetter: 0 } })
const g = (queueName, name, { lag = 0, waiting = 0, behind = 0, members = 1, state = 'Stable', kind = 'queen', ...more } = {}) => ({
  name, queueName, maxTimeLag: lag, totalLag: waiting, partitionsWithLag: behind, members, partitionCursors: members,
  state, kind, conflation: false, liveMembers: null, lastSeenAt: null, inactiveSeconds: null,
  subscriptionMode: 'new', subscriptionTimestamp: '2026-09-29T07:20:38.542282Z', subscriptionCreatedAt: '2026-09-29T07:20:38.542282Z',
  topics: [queueName], ...more,
})
const minutes = (n, v) => Array.from({ length: n }, () => v)
const row = (series) => ({ push: [null, ...series.push], pop: [null, ...series.pop], ackFailed: [null, ...minutes(series.push.length, 0)] })

const QUEUES = [
  q('channel.opsync_fetch', 'channel', 3),
  q('channel.metric_sample', 'channel', 24),
  q('tf.traces', 'traceflow', 2163),
  q('smartchat.router.outgoing', 'smartchat', 72),
  q('smartchat.router.history', 'smartchat', 72),
  q('smartpms.iot', 'smartpms', 64),
  q('smartpms.sync', 'smartpms', 64),
  q('loose', '', 1),
]
const GROUPS = [
  g('channel.opsync_fetch', 'channel.opsync_fetch.worker', { lag: 2, waiting: 1, behind: 1, members: 3, conflation: true }),
  g('channel.metric_sample', QUEUE_MODE, { members: 24, subscriptionMode: null, subscriptionTimestamp: null, subscriptionCreatedAt: null }),
  g('tf.traces', 'tf-archiver', { lag: 89, waiting: 54, behind: 54, members: 2163 }),
  g('smartchat.router.outgoing', 'email-outbox', { members: 72 }),
  g('smartchat.router.outgoing', 'push-notification-sender', { lag: 400, waiting: 900, behind: 12, members: 72 }),
  g('smartchat.router.history', QUEUE_MODE, { members: 72, state: 'Dead', subscriptionMode: null }),
  g('smartpms.iot', 'smartpms', { members: 64, subscriptionMode: 'all', subscriptionTimestamp: '1970-01-01T00:00:00.000000Z' }),
  g('smartpms.sync', 'smartpms', { lag: 30, waiting: 20, behind: 3, members: 64 }),
  g('loose', 'kafka-reader', { kind: 'kafka', lag: 4000, waiting: 70, state: 'Empty', liveMembers: 0, inactiveSeconds: 7200, lastSeenAt: '2026-10-31T08:00:00.000Z' }),
]
// The hour by queue: opsync_fetch pops each message six times (every ack
// fails); tf.traces reads 3 a minute faster than it fills; router.outgoing's
// pops are its two groups' together.
const WORKLOAD = {
  rows: [
    { key: 'channel.opsync_fetch', window: { ackFailed: 2118, ackSuccess: 0, popMessages: 2119, pushMessages: 354 }, series: row({ push: minutes(60, 6), pop: minutes(60, 36) }) },
    { key: 'tf.traces', window: { ackFailed: 0, ackSuccess: 2132, popMessages: 2137, pushMessages: 2132 }, series: row({ push: minutes(60, 36), pop: minutes(60, 39) }) },
    { key: 'smartchat.router.outgoing', window: { ackFailed: 0, ackSuccess: 40, popMessages: 40, pushMessages: 20 }, series: row({ push: minutes(60, 2), pop: minutes(60, 4) }) },
    { key: 'smartpms.sync', window: { ackFailed: 0, ackSuccess: 600, popMessages: 600, pushMessages: 620 }, series: row({ push: minutes(60, 12), pop: minutes(60, 10) }) },
  ],
}
const NOW = Date.parse('2026-10-31T11:00:00Z')
const build = (more = {}) => groupRows({
  groups: GROUPS, queues: QUEUES, ops: opsByQueue(WORKLOAD), series: seriesByQueue(WORKLOAD), now: NOW, ...more,
})
const byKey = (rows, name, queue) => rows.find((r) => r.key === rowKey(name, queue))

test('every row of the listing becomes a row, under its queue’s namespace', () => {
  const rows = build()
  assert.equal(rows.length, GROUPS.length)
  assert.equal(byKey(rows, 'tf-archiver', 'tf.traces').ns, 'traceflow')
  assert.equal(byKey(rows, 'kafka-reader', 'loose').ns, '')
  const qm = byKey(rows, QUEUE_MODE, 'channel.metric_sample')
  assert.equal(qm.label, 'queue mode')
  assert.equal(qm.queueMode, true)
  assert.equal(groupLabel('smartpms'), 'smartpms')
  // A group on several queues is a row on each.
  assert.equal(rows.filter((r) => r.name === 'smartpms').length, 2)
  // Not read yet is no rows, not an empty cluster.
  assert.deepEqual(groupRows({ groups: null, queues: QUEUES }), [])
})

test('what needs you is the one rule: past the lag lines, or reading a queue whose acks fail', () => {
  const rows = build()
  const fetch = byKey(rows, 'channel.opsync_fetch.worker', 'channel.opsync_fetch')
  assert.deepEqual([fetch.need.sev, fetch.need.reason, fetch.need.shared], ['bad', 'acks', false])
  assert.equal(fetch.tone, 'red')
  const tf = byKey(rows, 'tf-archiver', 'tf.traces')
  assert.deepEqual([tf.need.sev, tf.need.reason, tf.tone], ['warn', 'lag', 'amber'])
  assert.equal(byKey(rows, 'push-notification-sender', 'smartchat.router.outgoing').need.sev, 'bad')
  // Under its line (30 s < 60 s): fine. Never consumed, and a Kafka group gone
  // two hours: mute, never flagged.
  assert.equal(byKey(rows, 'smartpms', 'smartpms.sync').need, null)
  assert.equal(byKey(rows, QUEUE_MODE, 'smartchat.router.history').need, null)
  assert.equal(byKey(rows, QUEUE_MODE, 'smartchat.router.history').state, 'mute')
  assert.equal(byKey(rows, 'kafka-reader', 'loose').need, null)
  assert.equal(byKey(rows, 'kafka-reader', 'loose').state, 'inactive')
  assert.deepEqual(segmentCounts(rows), { all: 9, needs: 3 })
})

test('a queue failing its acks flags every live reader, and says the count is shared', () => {
  const groups = [...GROUPS, g('channel.opsync_fetch', 'second-reader', { members: 3 })]
  const rows = build({ groups })
  for (const name of ['channel.opsync_fetch.worker', 'second-reader']) {
    const r = byKey(rows, name, 'channel.opsync_fetch')
    assert.equal(r.need.reason, 'acks')
    assert.equal(r.need.shared, true)
  }
})

test('a drain is judged from the queue’s minutes only for its one reader', () => {
  const rows = build()
  // tf.traces: 39 read and 36 arrive a minute → 0.05/s faster: 54 clear in 1,080 s.
  const tf = byKey(rows, 'tf-archiver', 'tf.traces')
  assert.equal(tf.trend.kind, 'clearing')
  assert.equal(tf.trend.from, 'rates')
  assert.equal(Math.round(tf.trend.eta), 1080)
  // smartpms.sync: 10 read, 12 arrive: growing.
  assert.equal(byKey(rows, 'smartpms', 'smartpms.sync').trend.kind, 'growing')
  // Two groups share router.outgoing's pops: no minutes for either, and no
  // read of its own yet.
  assert.equal(byKey(rows, 'push-notification-sender', 'smartchat.router.outgoing').trend.kind, 'unknown')
  assert.equal(byKey(rows, 'email-outbox', 'smartchat.router.outgoing').trend.kind, 'caught')
  // A conflating group retires a partition per delivery: never by the pops.
  assert.equal(byKey(rows, 'channel.opsync_fetch.worker', 'channel.opsync_fetch').trend.kind, 'unknown')
  // Mute groups are not judged at all.
  assert.equal(byKey(rows, 'kafka-reader', 'loose').trend.kind, 'off')
})

test('rateTrend: within 5% is level; nothing moving with a backlog is stalled', () => {
  assert.equal(rateTrend(0, 1, 1).kind, 'caught')
  assert.equal(rateTrend(null, 1, 1).kind, 'unknown')
  assert.equal(rateTrend(10, null, 1).kind, 'unknown')
  assert.equal(rateTrend(10, 1, 1.04).kind, 'level')
  assert.equal(rateTrend(10, 1, 0.96).kind, 'level')
  assert.equal(rateTrend(10, 0, 0).kind, 'stalled')
  assert.deepEqual(rateTrend(100, 1, 2), { kind: 'clearing', eta: 100, from: 'rates' })
  assert.equal(rateTrend(100, 2, 1).kind, 'growing')
})

test('a shared queue’s group is judged from this page’s reads of its waiting count', () => {
  const samples = new Map()
  const at = (s) => NOW - s * 1000
  const read = (s, waiting) => addSamples(samples, GROUPS.map((x) => (x.name === 'push-notification-sender' ? { ...x, totalLag: waiting } : x)), at(s))
  read(120, 1200); read(90, 1100); read(60, 1000); read(0, 900)
  const k = rowKey('push-notification-sender', 'smartchat.router.outgoing')
  assert.equal(samples.get(k).length, 4)
  // 300 fewer in 120 s: 2.5 a second, 900 left → 360 s.
  const t = sampleTrend(samples.get(k), NOW)
  assert.equal(t.kind, 'clearing')
  assert.equal(Math.round(t.eta), 360)
  const rows = build({ samples, groups: GROUPS.map((x) => (x.name === 'push-notification-sender' ? { ...x, totalLag: 900 } : x)) })
  assert.equal(byKey(rows, 'push-notification-sender', 'smartchat.router.outgoing').trend.from, 'samples')
  // The same read twice is one sample; a row gone from the listing is forgotten.
  read(0, 900)
  assert.equal(samples.get(k).length, 4)
  addSamples(samples, GROUPS.filter((x) => x.name !== 'push-notification-sender'), NOW + 1000)
  assert.equal(samples.has(k), false)
})

test('sampleTrend: needs two reads 50 s apart, a recent last one, and ignores a change of one', () => {
  assert.equal(sampleTrend([], NOW).kind, 'unknown')
  assert.equal(sampleTrend([[NOW - 30_000, 10], [NOW, 5]], NOW).kind, 'unknown')
  // The last read is four minutes old: it says nothing of now.
  assert.equal(sampleTrend([[NOW - 300_000, 10], [NOW - 240_000, 5]], NOW).kind, 'unknown')
  assert.equal(sampleTrend([[NOW - 60_000, 10], [NOW, 11]], NOW).kind, 'level')
  assert.equal(sampleTrend([[NOW - 60_000, 10], [NOW, 20]], NOW).kind, 'growing')
  assert.equal(sampleTrend([[NOW - 60_000, 10], [NOW, 0]], NOW).kind, 'caught')
  // Only the last five minutes count: a read from ten minutes ago is not the start.
  assert.equal(sampleTrend([[NOW - 600_000, 1000], [NOW - 60_000, 10], [NOW, 10]], NOW).kind, 'level')
})

test('statusOf says the verdict and the drain in one cell', () => {
  const rows = build()
  assert.deepEqual(statusOf(byKey(rows, 'channel.opsync_fetch.worker', 'channel.opsync_fetch')), { kind: 'failing', tone: 'red' })
  assert.equal(statusOf(byKey(rows, 'tf-archiver', 'tf.traces')).kind, 'clearing')
  assert.deepEqual(statusOf(byKey(rows, 'smartpms', 'smartpms.sync')), { kind: 'growing', tone: '' })
  assert.deepEqual(statusOf(byKey(rows, 'push-notification-sender', 'smartchat.router.outgoing')), { kind: 'behind', tone: 'red' })
  assert.equal(statusOf(byKey(rows, 'email-outbox', 'smartchat.router.outgoing')).kind, 'caught')
  assert.equal(statusOf(byKey(rows, QUEUE_MODE, 'smartchat.router.history')).kind, 'never')
  assert.equal(statusOf(byKey(rows, 'kafka-reader', 'loose')).kind, 'off')
  // Flagged and level is not draining, in its tone; level and fine is steady.
  const flagged = { ...byKey(rows, 'tf-archiver', 'tf.traces'), trend: { kind: 'level' } }
  assert.deepEqual(statusOf(flagged), { kind: 'growing', tone: 'amber' })
  assert.deepEqual(statusOf({ ...flagged, need: null, tone: '' }), { kind: 'steady', tone: '' })
})

test('namespaces as the Queues list draws them, the default one last, with what a closed one hides', () => {
  const rows = build()
  const ns = namespaceGroups(rows)
  assert.deepEqual(ns.map((x) => x.ns), ['channel', 'smartchat', 'smartpms', 'traceflow', ''])
  const channel = ns.find((x) => x.ns === 'channel')
  assert.deepEqual(channel.counts, { failing: 1, behind: 0 })
  assert.equal(hasNews(channel), true)
  assert.equal(hasNews(ns.find((x) => x.ns === 'smartpms')), false)
  // Inside one: group, then queue; queue mode under q.
  assert.deepEqual(ns.find((x) => x.ns === 'smartchat').rows.map((r) => r.label), ['email-outbox', 'push-notification-sender', 'queue mode'])
  // Narrowed, an empty namespace goes; the filter finds groups, queues, namespaces and "queue mode".
  assert.deepEqual(namespaceGroups(rows, { seg: 'needs' }).map((x) => x.ns), ['channel', 'smartchat', 'traceflow'])
  assert.deepEqual(namespaceGroups(rows, { find: 'queue mode' }).map((x) => x.rows.length), [1, 1])
  assert.deepEqual(namespaceGroups(rows, { find: 'smartpms.sync' }).flatMap((x) => x.rows.map((r) => r.queue)), ['smartpms.sync'])
  assert.equal(matches(byKey(rows, 'tf-archiver', 'tf.traces'), 'TRACEFLOW'), true)
})

test('a header click freezes an order; a refresh only updates numbers', () => {
  const rows = build()
  const s = nextSort(null, 'oldest')
  assert.deepEqual(s, { key: 'oldest', dir: 'desc' })
  const rank = snapshotOrder(rows, s)
  const smartchat = (rs) => namespaceGroups(rs, { rank }).find((x) => x.ns === 'smartchat').rows.map((r) => r.label)
  assert.deepEqual(smartchat(rows), ['push-notification-sender', 'email-outbox', 'queue mode'])
  // email-outbox falls far behind later: it keeps its place until the next click.
  const later = rows.map((r) => (r.name === 'email-outbox' ? { ...r, lag: 9999 } : r))
  assert.deepEqual(smartchat(later), ['push-notification-sender', 'email-outbox', 'queue mode'])
  assert.equal(snapshotOrder(rows, { key: 'nope', dir: 'desc' }), null)
})

test('the line on top: worst first, a group named once, how many more', () => {
  const rows = build()
  const v = groupsVerdict({ rows, loaded: true })
  assert.equal(v.kind, 'issues')
  assert.equal(v.tone, 'red')
  assert.deepEqual(v.names.map((n) => n.name), ['push-notification-sender', 'channel.opsync_fetch.worker'])
  assert.equal(v.more, 1)
  assert.equal(v.count, 3)
  assert.equal(v.only, false)
  assert.deepEqual(groupsVerdict({ loaded: false }), { kind: 'loading' })
  assert.deepEqual(groupsVerdict({ loaded: false, failed: true }), { kind: 'failed' })
  assert.deepEqual(groupsVerdict({ rows: [], loaded: true }), { kind: 'none' })
  assert.deepEqual(groupsVerdict({ rows: rows.filter((r) => !r.need), loaded: true }), { kind: 'fine' })
})

test('?group= opens a group where it is worst; ?gqueue= picks one of its queues', () => {
  const groups = [...GROUPS, g('smartpms.iot', 'smartpms', { lag: 900, waiting: 5, members: 64 })].filter((x, i, all) => all.findIndex((y) => y.name === x.name && y.queueName === x.queueName) === i)
  const rows = build({ groups: groups.map((x) => (x.name === 'smartpms' && x.queueName === 'smartpms.iot' ? { ...x, maxTimeLag: 900 } : x)) })
  assert.equal(pickedRow(rows, 'smartpms').queue, 'smartpms.iot')
  assert.equal(pickedRow(rows, 'smartpms', 'smartpms.sync').queue, 'smartpms.sync')
  assert.equal(pickedRow(rows, 'smartpms', 'nope').queue, 'smartpms.iot')
  assert.equal(pickedRow(rows, 'nobody'), null)
  assert.equal(pickedRow(rows, ''), null)
})

test('partitions behind: past the chosen age, one group’s on one queue, in the broker’s order', () => {
  const lagging = [
    { consumer_group: 'tf-archiver', queue_name: 'tf.traces', partition_name: 'p-1', worker_id: 'w-1', offset_lag: 1, time_lag_seconds: 89, oldest_unconsumed_at: '2026-10-31T10:58:31.000Z', kind: 'queen' },
    { consumer_group: QUEUE_MODE, queue_name: 'channel.metric_sample', partition_name: 'p-2', worker_id: null, offset_lag: 3, time_lag_seconds: 70, kind: 'queen' },
    { consumer_group: 'tf-archiver', queue_name: 'tf.traces', partition_name: 'p-3', worker_id: null, offset_lag: 2, time_lag_seconds: 41, kind: 'queen' },
    { consumer_group: 'tf-archiver', queue_name: 'tf.traces', partition_name: '', time_lag_seconds: 900 },
  ]
  assert.deepEqual(behindRows(lagging, { min: 60 }).map((p) => p.partition), ['p-1', 'p-2'])
  assert.deepEqual(behindRows(lagging, { group: 'tf-archiver', queue: 'tf.traces' }).map((p) => [p.partition, p.waiting, p.lag]), [['p-1', 1, 89], ['p-3', 2, 41]])
  assert.equal(behindRows(lagging)[1].label, 'queue mode')
  assert.deepEqual(behindRows(null), [])
})

test('keepPlaces: a new read never moves a row; one that caught up stays, as gone', () => {
  const a = { key: 'a', lag: 90 }; const b = { key: 'b', lag: 80 }; const c = { key: 'c', lag: 95 }
  const first = keepPlaces(null, [a, b])
  assert.deepEqual(first.list.map((r) => r.key), ['a', 'b'])
  const next = keepPlaces(first, [c, { ...b, lag: 120 }])
  assert.deepEqual(next.list.map((r) => [r.key, r.gone, r.lag]), [['a', true, 90], ['b', false, 120], ['c', false, 95]])
})

test('the calls: the method, path and body each action sends, names encoded', () => {
  assert.deepEqual(seekCall('tf-archiver', 'tf.traces'), {
    kind: 'seek', method: 'POST', path: '/api/v1/consumer-groups/tf-archiver/queues/tf.traces/seek',
    body: { toEnd: true }, group: 'tf-archiver', queue: 'tf.traces',
    text: 'POST /api/v1/consumer-groups/tf-archiver/queues/tf.traces/seek {"toEnd":true}',
  })
  const at = Date.parse('2026-10-31T09:30:00.000Z')
  assert.deepEqual(seekCall('smartpms', 'smartpms.sync', at).body, { timestamp: '2026-10-31T09:30:00.000Z' })
  // A name is caller text: '/', '#' and spaces must not address another group.
  assert.equal(seekCall('a/b#c', 'q 1').path, '/api/v1/consumer-groups/a%2Fb%23c/queues/q%201/seek')
  const p = partitionCall(QUEUE_MODE, 'channel.metric_sample', 'conn:28/x')
  assert.equal(p.path, '/api/v1/consumer-groups/__QUEUE_MODE__/queues/channel.metric_sample/partitions/conn%3A28%2Fx/seek')
  assert.deepEqual(p.body, { toEnd: true })
  assert.equal(p.method, 'POST')
  const d1 = deleteCall('smartpms', 'smartpms.sync', false)
  assert.deepEqual([d1.kind, d1.method, d1.path, d1.params], ['deleteForQueue', 'DELETE', '/api/v1/consumer-groups/smartpms/queues/smartpms.sync', { deleteMetadata: false }])
  assert.equal(d1.text, 'DELETE /api/v1/consumer-groups/smartpms/queues/smartpms.sync?deleteMetadata=false')
  const d2 = deleteCall('smartpms', null)
  assert.deepEqual([d2.kind, d2.path, d2.deleteMetadata, d2.text], ['delete', '/api/v1/consumer-groups/smartpms', true, 'DELETE /api/v1/consumer-groups/smartpms?deleteMetadata=true'])
})

test('sendCall hands the same parts to the API layer the dialog showed', async () => {
  const sent = []
  const api = {
    seek: (...a) => { sent.push(['seek', ...a]); return Promise.resolve({ data: { success: true, partitionsUpdated: 3 } }) },
    seekPartition: (...a) => { sent.push(['seekPartition', ...a]); return Promise.resolve({ data: {} }) },
    deleteForQueue: (...a) => { sent.push(['deleteForQueue', ...a]); return Promise.resolve({ data: {} }) },
    delete: (...a) => { sent.push(['delete', ...a]); return Promise.resolve({ data: {} }) },
  }
  const cfg = { quiet: true }
  await sendCall(api, seekCall('g', 'q'), cfg)
  await sendCall(api, seekCall('g', 'q', 0), cfg)
  await sendCall(api, partitionCall('g', 'q', 'p'), cfg)
  await sendCall(api, deleteCall('g', 'q', false), cfg)
  await sendCall(api, deleteCall('g', null, true), cfg)
  assert.deepEqual(sent, [
    ['seek', 'g', 'q', { toEnd: true }, cfg],
    ['seek', 'g', 'q', { timestamp: '1970-01-01T00:00:00.000Z' }, cfg],
    ['seekPartition', 'g', 'q', 'p', cfg],
    ['deleteForQueue', 'g', 'q', false, cfg],
    ['delete', 'g', true, cfg],
  ])
  await assert.rejects(sendCall(api, { kind: 'purge' }))
})

test('an answer that did nothing is not a success', () => {
  assert.deepEqual(seekOutcome({ success: true, partitionsUpdated: 2163 }), { ok: true, moved: 2163, problem: null })
  assert.equal(seekOutcome({ success: true, partitionsUpdated: 0 }).ok, false)
  assert.equal(seekOutcome({ success: false, error: 'Partition not found', partitionsUpdated: 0 }).problem, 'Partition not found')
  assert.equal(seekOutcome(null).ok, false)
  assert.deepEqual(deleteOutcome({ success: true, deletedPartitions: 64, metadataDeleted: true }), { ok: true, deleted: 64, metadata: true, problem: null })
  assert.equal(deleteOutcome({ success: true, deletedPartitions: 0, metadataDeleted: true }).ok, false)
  assert.equal(deleteOutcome('').ok, false)
})

test('where a deleted group starts again, by its subscription', () => {
  const rows = build()
  assert.deepEqual(restartOf(byKey(rows, QUEUE_MODE, 'channel.metric_sample'), true), { kind: 'queueMode' })
  assert.deepEqual(restartOf(byKey(rows, 'tf-archiver', 'tf.traces'), false), { kind: 'fresh' })
  assert.deepEqual(restartOf(byKey(rows, 'smartpms', 'smartpms.iot'), true), { kind: 'all' })
  assert.deepEqual(restartOf(byKey(rows, 'tf-archiver', 'tf.traces'), true), { kind: 'new', since: Date.parse('2026-09-29T07:20:38.542Z') })
  const ts = { ...byKey(rows, 'tf-archiver', 'tf.traces'), sub: { mode: 'timestamp', at: '2026-09-25T13:24:41.572000Z', since: null } }
  assert.deepEqual(restartOf(ts, true), { kind: 'timestamp', since: Date.parse('2026-09-25T13:24:41.572Z') })
})

test('a time typed for a seek, local, and back', () => {
  const base = new Date(2026, 9, 31, 11, 5).getTime()
  assert.equal(parseWhen('2026-10-31 09:30', base), new Date(2026, 9, 31, 9, 30).getTime())
  assert.equal(parseWhen('2026-10-31T09:30:15', base), new Date(2026, 9, 31, 9, 30, 15).getTime())
  assert.equal(parseWhen('9:30', base), new Date(2026, 9, 31, 9, 30).getTime())
  assert.equal(parseWhen('2026-02-31 09:30', base), null)
  assert.equal(parseWhen('24:00', base), null)
  assert.equal(parseWhen('yesterday', base), null)
  assert.equal(whenText(new Date(2026, 0, 2, 3, 4).getTime()), '2026-01-02 03:04')
  assert.equal(parseWhen(whenText(base), base), base)
})
