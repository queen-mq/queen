// The Queues section's Messages view and its ephemeral queue detail
// (console/queues/messagesView.js, console/queues/ephemeralView.js): the
// address, the read, the list that holds still, the picked message's story,
// and the requests behind every write, pinned here because stage is read-only.
// Shapes cut from stage on 2026-10-31: channel.opsync_fetch dead-letters every
// poll after 5 retries; tf.traces is leased by tf-archiver; smartchat queues
// are stored encrypted and the broker decrypts them; gate.reply.channel-go is
// a declared ephemeral queue with two partitions on the node that answered.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  MAX_OFFSET, PAGE, callText, copyOf, curlOf, deleteAnswer, deleteMessageRequest, emptyParts, groupStates, groupsDiffer,
  listCall, listParams, mergeRefresh, messageRows, messageState, older, payloadOf, readFilters, readPick, relaxOf,
  traceSummary, windowWords, withFilters,
} from '../src/console/queues/messagesView.js'
import {
  MAX_ROWS, deleteAnswer as ephDeleteAnswer, deleteRequest, droppedOf, dur, groupRows, heldWords, listRows, lossRows, nextSort,
  optionsOf, resetAnswer, resetRequest, size, snapshotOrder, survival,
} from '../src/console/queues/ephemeralView.js'

const PID = '01a110ff-f581-7000-a4ca-82b9b6a13c70'
const TXN = 'poll-28cade7a-0726-5913-b537-0059620cf2bf-reviews-1793441032073'
const row = (over = {}) => ({
  id: '01a1916a-678b-7002-80c5-42abba3ba376',
  transactionId: TXN,
  partitionId: PID,
  queue: 'channel.opsync_fetch',
  partition: 'conn:28cade7a-0726-5913-b537-0059620cf2bf:reviews',
  createdAt: '2026-10-31T10:03:52.076Z',
  offset: 215326,
  status: 'pending',
  busStatus: { consumedBy: 0, totalGroups: 1 },
  payload: { source: 'channel', data: { portalKey: 'booking-com' } },
  isEncrypted: false,
  payloadAvailable: true,
  ...over,
})

// ---- the address ----

test('readFilters: the m-prefixed fields, an unknown status read as All, arrays as their first', () => {
  assert.deepEqual(readFilters({ mq: ' channel.opsync_fetch ', mstatus: 'dead_letter', mpart: 'Default', view: 'messages' }), {
    queue: 'channel.opsync_fetch', namespace: '', task: '', partition: 'Default', status: 'dead_letter',
  })
  assert.equal(readFilters({ mstatus: 'failed' }).status, '', 'not a status the route takes')
  assert.equal(readFilters({ mns: ['channel', 'gate'] }).namespace, 'channel')
  assert.deepEqual(readFilters({}), { queue: '', namespace: '', task: '', partition: '', status: '' })
})

test('withFilters: other fields kept, empty ones dropped, the pick cleared', () => {
  const q = { view: 'messages', range: '6h', mq: 'tf.events', mpid: PID, mtx: TXN }
  assert.deepEqual(withFilters(q, { status: 'pending', queue: '' }), { view: 'messages', range: '6h', mstatus: 'pending' })
  assert.deepEqual(withFilters({ view: 'messages' }, { namespace: 'channel', bogus: 'x' }), { view: 'messages', mns: 'channel' })
})

test('readPick: both halves of the address, the transactionId as typed', () => {
  assert.deepEqual(readPick({ mpid: PID, mtx: TXN }), { pid: PID, txn: TXN })
  assert.deepEqual(readPick({ mpid: PID, mtx: ' x ' }), { pid: PID, txn: ' x ' })
  assert.equal(readPick({ mpid: PID }), null)
})

// ---- the read ----

test('listParams and listCall: filters, the band window, the page; times readable in the line', () => {
  const p = listParams({ queue: 'channel.opsync_fetch', status: 'dead_letter', partition: '' }, { from: '2026-10-31T09:05:00.000Z', to: '2026-10-31T10:05:00.000Z' }, 40)
  assert.deepEqual(p, { queue: 'channel.opsync_fetch', status: 'dead_letter', from: '2026-10-31T09:05:00.000Z', to: '2026-10-31T10:05:00.000Z', limit: PAGE, offset: 40 })
  assert.equal(listCall(p), 'GET /api/v1/messages?queue=channel.opsync_fetch&status=dead_letter&from=2026-10-31T09:05:00.000Z&to=2026-10-31T10:05:00.000Z&limit=20&offset=40')
  assert.equal(listParams({}, {}, 0).offset, undefined, 'page 1 sends no offset')
  assert.equal(listCall({ queue: 'a&b c' }), 'GET /api/v1/messages?queue=a%26b%20c')
})

test('windowWords: presets and a fixed from-to', () => {
  assert.equal(windowWords({ id: '1h', live: true }), 'pushed in the last hour')
  assert.equal(windowWords({ id: '7d' }), 'pushed in the last 7 days')
  assert.equal(windowWords({ id: 'custom', custom: true, label: '09:00 – 10:00' }), 'pushed from 09:00 to 10:00')
})

test('messageRows: words, the fan-out count only while groups are at it, unaddressable rows kept', () => {
  const [a, b, c, d] = messageRows([
    row(),
    row({ id: 'x2', status: 'processing', busStatus: { consumedBy: 3, totalGroups: 10 } }),
    row({ id: 'x3', status: 'completed', busStatus: { consumedBy: 10, totalGroups: 10 } }),
    row({ id: null, transactionId: null, offset: 7 }),
  ])
  assert.equal(a.word, 'waiting')
  assert.equal(a.read, '', 'one group: its word says it')
  assert.equal(a.at, Date.parse('2026-10-31T10:03:52.076Z'))
  assert.equal(b.word, 'leased')
  assert.equal(b.read, '3 of 10 read')
  assert.equal(c.read, '', 'done means all of them')
  assert.equal(d.addressable, false)
  assert.equal(d.key, `${PID}\u0000\u00007`)
  assert.deepEqual(messageRows(null), [])
})

test('mergeRefresh: the list holds still, statuses update in place, arrivals are counted', () => {
  const shown = messageRows([row({ id: 'm3', createdAt: '2026-10-31T10:00:03Z' }), row({ id: 'm2', createdAt: '2026-10-31T10:00:02Z' }), row({ id: 'm1', createdAt: '2026-10-31T10:00:01Z' })])
  const same = messageRows([row({ id: 'm3', createdAt: '2026-10-31T10:00:03Z', status: 'completed' }), row({ id: 'm2', createdAt: '2026-10-31T10:00:02Z' }), row({ id: 'm1', createdAt: '2026-10-31T10:00:01Z' })])
  const a = mergeRefresh(shown, same)
  assert.equal(a.newer, 0)
  assert.equal(a.rows[0].word, 'done', 'the same rows: replaced')

  const moved = messageRows([row({ id: 'm5', createdAt: '2026-10-31T10:00:05Z' }), row({ id: 'm4', createdAt: '2026-10-31T10:00:04Z' }), row({ id: 'm3', createdAt: '2026-10-31T10:00:03Z', status: 'dead_letter' })])
  const b = mergeRefresh(shown, moved)
  assert.equal(b.newer, 2, 'two arrived since')
  assert.deepEqual(b.rows.map((r) => r.key), ['m3', 'm2', 'm1'], 'nothing moved')
  assert.equal(b.rows[0].word, 'dead letter', 'a row still on the page takes its fresh status')
  assert.equal(b.rows[1].word, 'waiting', 'a row that fell off keeps the last it said')

  assert.deepEqual(mergeRefresh([], moved), { rows: moved, newer: 0 }, 'nothing on screen: the fresh page')
})

test('older: a full page goes on, a cut page goes where the broker says, 10,000 is the floor', () => {
  assert.deepEqual(older({ offset: 0, count: 20 }), { can: true, next: 20, deep: false })
  assert.deepEqual(older({ offset: 40, count: 7 }), { can: false, next: 47, deep: false }, 'a short page is the end')
  assert.deepEqual(older({ offset: 0, count: 5, pagination: { hasMore: true, nextOffset: 5 } }), { can: true, next: 5, deep: false }, 'cut by its byte budget')
  assert.deepEqual(older({ offset: MAX_OFFSET - 10, count: 20 }), { can: false, next: MAX_OFFSET + 10, deep: true })
})

test('emptyParts and relaxOf: what is empty, and the one filter to let go of first', () => {
  const words = (f, o) => emptyParts(f, o).map((p) => p.t).join('')
  assert.equal(words({ status: 'processing', queue: 'channel.opsync_fetch' }, { when: 'pushed in the last 15 minutes' }), 'No leased messages in channel.opsync_fetch pushed in the last 15 minutes.')
  assert.equal(words({}, { offset: 20, when: 'pushed in the last hour' }), 'No older messages pushed in the last hour.')
  assert.equal(words({ namespace: 'channel', partition: 'Default' }), 'No messages in namespace channel, partition Default, pushed in the last hour.')
  assert.ok(emptyParts({ queue: 'q' }).some((p) => p.mono && p.t === 'q'), 'the queue in mono')
  assert.deepEqual(relaxOf({ status: 'dead_letter', queue: 'q' }), { patch: { status: '' }, label: 'Show every status' })
  assert.deepEqual(relaxOf({ partition: 'p', queue: 'q' }).patch, { partition: '' })
  assert.deepEqual(relaxOf({ task: 'long' }).patch, { queue: '', namespace: '', task: '' })
  assert.equal(relaxOf({}), null)
})

// ---- the picked message ----

const NOW = Date.parse('2026-10-31T10:04:00Z')
const time = (ms) => new Date(ms).toISOString().slice(11, 19)

test('groupStates: done, leased (a live lease on a group not past it) or waiting; queue mode last', () => {
  const g = groupStates({ consumerGroups: [
    { name: '__QUEUE_MODE__', consumed: false, leaseExpiresAt: null },
    { name: 'tf-archiver', consumed: false, leaseExpiresAt: '2026-10-31T10:07:00Z' },
    { name: 'audit', consumed: false, leaseExpiresAt: '2026-10-31T10:00:00Z' },
    { name: 'billing', consumed: true, leaseExpiresAt: '2026-10-31T10:07:00Z' },
  ] }, NOW)
  assert.deepEqual(g.map((x) => [x.label, x.state]), [['audit', 'waiting'], ['billing', 'done'], ['tf-archiver', 'leased'], ['queue mode', 'waiting']])
  assert.equal(g[2].until, Date.parse('2026-10-31T10:07:00Z'))
  assert.equal(groupStates({ status: 'pending' }), null, 'the list row has no groups: not known yet')
  assert.equal(groupsDiffer(g), true)
  assert.equal(groupsDiffer([{ state: 'done' }, { state: 'done' }]), false)
})

test('messageState: one sentence per status, names in mono', () => {
  const dead = messageState({ status: 'dead_letter', retryCount: 5, errorMessage: 'fetch: resolve booking review cursor scope' }, [])
  assert.equal(dead.line, 'Dead letter, after 5 retries.')
  assert.equal(dead.error, 'fetch: resolve booking review cursor scope')

  const leased = messageState({ status: 'processing' }, [{ name: 'tf-archiver', label: 'tf-archiver', state: 'leased', until: Date.parse('2026-10-31T10:25:08Z') }], time)
  assert.equal(leased.line, 'Leased: a worker of tf-archiver holds its partition until 10:25:08.')
  assert.ok(leased.parts.some((p) => p.mono && p.t === 'tf-archiver'))

  const groups = (states) => states.map(([name, state]) => ({ name, label: name, state }))
  assert.equal(messageState({ status: 'completed' }, groups([['a', 'done'], ['b', 'done']])).line, 'Done: all 2 groups read it.')
  assert.equal(messageState({ status: 'completed' }, groups([['streams.tf-traces', 'done']])).line, 'Done: streams.tf-traces read it.')
  assert.equal(messageState({ status: 'completed' }, groups([['__QUEUE_MODE__', 'done']])).line, 'Done: a worker took it and acked it.')
  assert.equal(messageState({ status: 'pending' }, groups([['a', 'waiting'], ['b', 'done'], ['c', 'waiting']])).line, 'Waiting for a and c.')
  assert.equal(messageState({ status: 'pending' }, groups([['a', 'waiting'], ['b', 'waiting'], ['c', 'waiting'], ['d', 'waiting']])).line, 'Waiting for a, b and 2 more.')
  assert.equal(messageState({ status: 'pending' }, null).line, 'Waiting: no reader is past it yet.', 'before the detail is read')
  assert.equal(messageState(null), null)
})

test('payloadOf: decrypted, sealed, gone, a Kafka record, plain JSON', () => {
  assert.deepEqual(payloadOf(row({ isEncrypted: true, payload: { from: { tenantId: '9593ead8' } } })), { kind: 'json', value: { from: { tenantId: '9593ead8' } }, record: null, encrypted: true })
  assert.equal(payloadOf(row({ isEncrypted: true, payload: { encrypted: 'q83v', iv: 'AAEC', authTag: 'BAUG' } })).kind, 'sealed', 'no key on the broker that answered')
  assert.equal(payloadOf(row({ payloadAvailable: false })).kind, 'gone')
  const b64 = (s) => Buffer.from(s, 'utf8').toString('base64')
  const k = payloadOf(row({ payload: { k: b64('order-17'), v: b64('{"total":12}'), t: 1793441032075 } }))
  assert.equal(k.kind, 'kafka')
  assert.equal(k.record.key.text, 'order-17')
  assert.deepEqual(k.record.value.json, { total: 12 })
  assert.equal(payloadOf(row({ payload: 'plain text' })).kind, 'json')
  assert.equal(payloadOf(null).kind, 'none')
})

test('copyOf and curlOf: the same queue, partition and payload, and a shell-safe line', () => {
  assert.deepEqual(copyOf(row()), { queue: 'channel.opsync_fetch', partition: 'conn:28cade7a-0726-5913-b537-0059620cf2bf:reviews', payload: { source: 'channel', data: { portalKey: 'booking-com' } } })
  assert.equal(copyOf(row({ payloadAvailable: false })), null)
  assert.equal(copyOf(row({ payload: { encrypted: 'q83v', iv: 'AAEC', authTag: 'BAUG' } })), null)
  assert.equal(copyOf(row({ partition: '' })).partition, 'Default')

  const line = curlOf({ origin: 'https://queen.example', queue: 'q', partition: 'p', payload: { note: "it's" } })
  assert.equal(line, `curl -X POST 'https://queen.example/api/v1/push' -H 'Content-Type: application/json' -d '{"items":[{"queue":"q","partition":"p","payload":{"note":"it'\\''s"}}]}'`)
  assert.match(curlOf({ origin: '', queue: 'q', payload: 1, key: true }), /-H "Authorization: Bearer \$QUEEN_API_KEY" -d /)
})

test('traceSummary: how many events, and the names they carry', () => {
  const t = traceSummary({ total: 3, traces: [{ names: ['order-17'] }, { names: ['order-17', 'booking-9'] }, { trace_names: ['booking-9'] }] })
  assert.equal(t.total, 3)
  assert.deepEqual(t.names, [{ name: 'booking-9', n: 2 }, { name: 'order-17', n: 2 }])
  assert.deepEqual(traceSummary({ traces: [], total: 0 }), { total: 0, names: [] })
})

// ---- the writes ----

test('deleting a message: one DELETE to its dead-letter address, both halves encoded', () => {
  const req = deleteMessageRequest(PID, TXN)
  assert.deepEqual(req, { method: 'DELETE', path: `/api/v1/messages/${PID}/${TXN}` })
  assert.equal(callText(req), `DELETE /api/v1/messages/${PID}/${TXN}`)
  assert.equal(deleteMessageRequest('p/1', 'a#b?c').path, '/api/v1/messages/p%2F1/a%23b%3Fc', 'a / or # would address another message')
  assert.deepEqual(deleteAnswer({ success: true, partitionId: PID, transactionId: TXN, deleted: 2 }), { ok: true, n: 2, why: '' })
  assert.deepEqual(deleteAnswer({ success: false, message: 'No dead-letter row for this address.' }), { ok: false, n: 0, why: 'No dead-letter row for this address.' })
})

test('resetting and deleting an ephemeral queue: the calls the dialogs show', () => {
  const reset = resetRequest('gate.reply.channel-go')
  assert.deepEqual(reset, { method: 'POST', path: '/api/v1/ephemeral/reset', body: { queue: 'gate.reply.channel-go' } })
  assert.equal(callText(reset), 'POST /api/v1/ephemeral/reset {"queue":"gate.reply.channel-go"}')
  const del = deleteRequest('gate.reply.channel-go')
  assert.deepEqual(del, { method: 'DELETE', path: '/api/v1/ephemeral/queue/gate.reply.channel-go' })
  assert.equal(callText(del), 'DELETE /api/v1/ephemeral/queue/gate.reply.channel-go')
  assert.equal(deleteRequest('inbox/42#a').path, '/api/v1/ephemeral/queue/inbox%2F42%23a')
})

test('the ephemeral answers, said as what they are: one node\'s count, a configuration gone or not', () => {
  assert.equal(resetAnswer({ queue: 'q', dropped: 12 }), 'The node that answered dropped 12 messages; every other node drops its own.')
  assert.equal(resetAnswer({ queue: 'q', dropped: 1 }), 'The node that answered dropped 1 message; every other node drops its own.')
  assert.match(resetAnswer({}), /Every node drops/)
  assert.match(ephDeleteAnswer({ queue: 'q', deleted: true, declared: true }), /stored configuration is gone/)
  assert.match(ephDeleteAnswer({ queue: 'q', deleted: false, declared: false }), /no stored configuration/)
  assert.equal(heldWords(0), 'the node that answered holds none right now')
  assert.equal(heldWords(1200), 'the node that answered holds 1,200 messages')
  assert.match(heldWords(null), /did not say/)
})

// ---- one ephemeral queue ----

const LIST = { queues: [
  { queue: 'channel.smartchat.rpc', tier: 'declared', depth: 0, bytes: 0, partitions: 0, groups: 1, drops: { bounds: 0, ttl: 0, retry: 0 }, options: { maxBytes: 16777216, maxLength: 1000, policy: 'reject', ttlSeconds: 60, leaseSeconds: 30, retryLimit: 5 } },
  { queue: 'gate.reply.channel-go', tier: 'declared', depth: 0, bytes: 0, partitions: 2, groups: 5, drops: { bounds: 0, ttl: 0, retry: 0 }, options: { maxBytes: 16777216, maxLength: 10000, policy: 'dropOldest', ttlSeconds: 600, leaseSeconds: 30, retryLimit: 1 } },
], count: 2, cellBytes: 4096 }
const DEPTH = { queue: 'gate.reply.channel-go', group: null, tier: 'declared', pending: 0, partitionsPending: 0, bytes: 0,
  partitions: [{ partition: 'ciaochannel-go-588d6bf885-97zvk.server', pending: 0, bytes: 0 }, { partition: 'ciaochannel-go-worker-949588546-h7nzn.worker', pending: 0, bytes: 0 }],
  groups: [{ group: 'ciaochannel-go-worker-949588546-h7nzn.worker', pending: 3, skipped: 2 }, { group: '__QUEUE_MODE__', pending: 0, skipped: 0 }] }

test('groupRows and optionsOf: the depth route\'s groups, the list\'s options', () => {
  assert.deepEqual(groupRows(DEPTH), [
    { name: 'ciaochannel-go-worker-949588546-h7nzn.worker', label: 'ciaochannel-go-worker-949588546-h7nzn.worker', pending: 3, skipped: 2 },
    { name: '__QUEUE_MODE__', label: 'queue mode', pending: 0, skipped: 0 },
  ])
  assert.equal(groupRows({}), null, 'not reported is not none')
  assert.equal(optionsOf(LIST, 'gate.reply.channel-go').policy, 'dropOldest')
  assert.equal(optionsOf(LIST, 'nope'), null)
})

test('lossRows: each cause with its rule from the effective options', () => {
  const [bounds, ttl, retry] = lossRows({ bounds: 4, ttl: 0, retry: 1 }, optionsOf(LIST, 'gate.reply.channel-go'))
  assert.equal(bounds.rule, 'It holds 10,000 messages or 16 MB; past that, the oldest are dropped.')
  assert.equal(bounds.n, 4)
  assert.equal(ttl.rule, 'A message waits at most 10 min.')
  assert.equal(retry.rule, 'Delivered again when its 30 s lease runs out or a worker fails it, once; then that group drops it.')
  const rpc = lossRows({}, optionsOf(LIST, 'channel.smartchat.rpc'))
  assert.equal(rpc[0].rule, 'It holds 1,000 messages or 16 MB; past that, a push is refused and nothing is dropped.')
  assert.equal(rpc[1].rule, 'A message waits at most 1 min.')
  assert.match(rpc[2].rule, /up to 5 times/)
  assert.equal(rpc[0].n, null, 'a count not reported stays unsaid')
  assert.equal(lossRows({}, { ttlSeconds: 0, retryLimit: 0 })[1].rule, 'No age limit.')
  assert.equal(lossRows({}, null)[0].rule, '', 'options not read: no rule')
})

test('survival: what a deploy, a crash and a full stop do, by tier', () => {
  const declared = survival(true)
  assert.deepEqual(declared.map((s) => [s.when, s.kept]), [['A deploy, node by node', true], ['A node crash', false], ['Every node stopping at once', false], ['Its configuration', true]])
  assert.deepEqual(survival(false).at(-1), { when: 'The queue itself', kept: false, why: 'Nothing is stored: a later push or pop makes it again, empty.' })
  assert.equal(survival(null).length, 3, 'a tier not reported is not guessed')
})

test('the ephemeral list: filtered, in the order a click froze, cut at 200 and said so', () => {
  const qs = [
    { name: 'inbox.c', declared: false, depth: 5, bytes: 10, drops: { bounds: 1, ttl: 0, retry: 0 } },
    { name: 'inbox.a', declared: false, depth: 9, bytes: 99, drops: {} },
    { name: 'gate.reply.channel-go', declared: true, depth: 0, bytes: 0, drops: { bounds: 0, ttl: 0, retry: 0 } },
    { name: 'inbox.b', declared: false, depth: null, bytes: 1, drops: { bounds: 2, ttl: 3, retry: null } },
  ]
  assert.deepEqual(listRows(qs).rows.map((q) => q.name), ['gate.reply.channel-go', 'inbox.a', 'inbox.b', 'inbox.c'], 'by name')
  assert.deepEqual(listRows(qs, { find: 'INBOX', tier: 'implicit' }).rows.map((q) => q.name), ['inbox.a', 'inbox.b', 'inbox.c'])
  assert.deepEqual(listRows(qs, { tier: 'declared' }).rows.map((q) => q.name), ['gate.reply.channel-go'])

  const s = nextSort(null, 'depth')
  assert.deepEqual(s, { key: 'depth', dir: 'desc' })
  assert.deepEqual(nextSort(s, 'depth'), { key: 'depth', dir: 'asc' })
  assert.equal(nextSort({ key: 'depth', dir: 'asc' }, 'depth'), null, 'third click: back to names')
  const rank = snapshotOrder(qs, s)
  assert.deepEqual(listRows(qs, { rank }).rows.map((q) => q.name), ['inbox.a', 'inbox.c', 'gate.reply.channel-go', 'inbox.b'], 'unknown depth last')
  const later = [...qs.map((q) => ({ ...q, depth: q.name === 'inbox.c' ? 500 : q.depth })), { name: 'inbox.0', declared: false, depth: 1000 }]
  assert.deepEqual(listRows(later, { rank }).rows.map((q) => q.name), ['inbox.a', 'inbox.c', 'gate.reply.channel-go', 'inbox.b', 'inbox.0'], 'a refresh moves nothing; a new queue goes last')
  assert.deepEqual(listRows(qs, { rank: snapshotOrder(qs, { key: 'dropped', dir: 'desc' }) }).rows.map((q) => q.name)[0], 'inbox.b')

  const many = Array.from({ length: 450 }, (_, i) => ({ name: `inbox.${String(i).padStart(3, '0')}`, declared: false }))
  const cut = listRows(many)
  assert.equal(cut.rows.length, MAX_ROWS)
  assert.equal(cut.matched, 450)
  assert.equal(droppedOf({ drops: { bounds: 2, ttl: null, retry: 1 } }), 3)
  assert.equal(droppedOf({ drops: {} }), null, 'none reported is not zero')
})

test('size and dur: the console\'s short words', () => {
  assert.equal(size(16777216), '16 MB')
  assert.equal(size(1536), '1.5 KB')
  assert.equal(size(0), '0 B')
  assert.equal(size(null), '–')
  assert.equal(dur(30), '30 s')
  assert.equal(dur(600), '10 min')
  assert.equal(dur(90), '1 min 30 s')
  assert.equal(dur(7200), '2 h')
})
