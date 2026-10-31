// The Dead letters page, pinned: the broker's error fold ported exactly (so a
// row finds its cause even when two causes share their head), the counts per
// cause, what still happens, the list's fixed order and the page's one line.
// Stage, 2026-10-31: 8,903 dead letters in 4 queues, 8 causes; channel.opsync_fetch
// held 8,639 of one cause, failing every 10 s; smartpms.channel-sync had five
// causes, two of them "POST …/rate-plans [<n>]: …".

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  apportion, breakable, buildCauses, countedLine, countText, errorSignature, fenced, fileName,
  isLive, LIVE_MS, markdownHead, mergeSeen, rankNew, retriesWords, roughly, rowsOf, summaryLine, summaryParts,
} from '../src/console/deadletters/dlCauses.js'

const NOW = Date.parse('2026-10-31T08:11:00Z')
const OPSYNC = 'fetch: resolve booking review cursor scope: bookingcom: hotel_id not configured on connection (authDetails.hotelId / externalId)'
const ISE = 'POST https://channel-stage.smartness.com/api/v1/rate-plans [500]: {"statusCode":500,"code":"INTERNAL_SERVER_ERROR","error":"Internal Server Error","message":"Internal Server Error"}'
const PARENT = 'POST https://channel-stage.smartness.com/api/v1/rate-plans [404]: {"statusCode":404,"code":"RATE_PLAN_PARENT_NOT_FOUND","error":"Not Found","message":"Parent rate plan 4471 not found"}'
const SMTP = 'Expected response code "250" but got code "530", with message "530 5.7.0 Authentication required".'

test('errorSignature folds as the broker does (admin.rs error_signature and its tests)', () => {
  assert.equal(errorSignature('  '), '(no message)')
  assert.equal(errorSignature(null), '(no message)')
  assert.equal(
    errorSignature('order 01a0d2c7-e81f-702c-8734-c88401672347 failed at 2026-09-24T09:57:29.004Z after 3 tries'),
    'order <id> failed at <date> after <n> tries',
  )
  assert.equal(errorSignature('hash deadbeefdeadbeefdeadbeef\n\tretry   7'), 'hash <id> retry <n>')
  assert.equal(errorSignature('v2 abc123 x1'), 'v2 abc123 x1', 'a word with letters is not a number')
  assert.equal(errorSignature('x'.repeat(200)).length, 120)
  assert.equal(errorSignature('due 2026-11-14 and 2026-10-31T08:10:36.901Z'), 'due <date> and <date>')
  assert.equal(errorSignature('id 12345678901234567890'), 'id <id>', 'twenty digits are hex: an id, not a number')
})

test('the stage errors fold to the stage signatures', () => {
  assert.equal(errorSignature(OPSYNC), OPSYNC.slice(0, 120), 'cut at 120 characters')
  assert.equal(errorSignature(ISE), 'POST https://channel-stage.smartness.com/api/v1/rate-plans [<n>]: {"statusCode":<n>,"code":"INTERNAL_SERVER_ERROR","erro')
  assert.equal(errorSignature(SMTP), 'Expected response code "<n>" but got code "<n>", with message "<n> <n>.<n>.<n> Authentication required".')
  assert.notEqual(errorSignature(ISE), errorSignature(PARENT), 'two causes with one head stay two')
})

test('rowsOf takes exactly the rows of a cause, and the rest takes the others', () => {
  const rows = [{ errorMessage: ISE }, { errorMessage: PARENT }, { errorMessage: ISE }, { errorMessage: 'timeout after 30 s' }]
  const known = [errorSignature(ISE), errorSignature(PARENT)]
  assert.equal(rowsOf({ text: errorSignature(PARENT), known }, rows).length, 1)
  assert.equal(rowsOf({ text: errorSignature(ISE), known }, rows).length, 2)
  assert.deepEqual(rowsOf({ rest: true, text: '', known }, rows), [rows[3]])
  assert.equal(rowsOf({ unknown: true }, rows).length, 4, 'causes not read: every row is a sample')
  assert.deepEqual(rowsOf(null, rows), [])
})

test('apportion splits a whole in whole numbers that add up', () => {
  assert.deepEqual(apportion(49, [21, 15, 10, 2, 1]), [21, 15, 10, 2, 1])
  assert.deepEqual(apportion(10, [1, 1, 1]), [4, 3, 3])
  assert.deepEqual(apportion(8639, [200]), [8639])
  assert.deepEqual(apportion(0, [3, 1]), [0, 0])
  assert.deepEqual(apportion(5, [0, 0]), [0, 0])
  const big = apportion(8639, [120, 60, 20])
  assert.equal(big.reduce((a, b) => a + b, 0), 8639)
})

// The stage answers, cut to what is read.
const sigOpsync = { queue: 'channel.opsync_fetch', rowsNow: 8639, sample: 200, newest: '2026-10-31T08:10:36.901Z', oldest: '2026-10-31T07:37:27.010Z', groups: ['channel.opsync_fetch.worker'], retryCounts: [5], signatures: [{ n: 200, share: 1, text: OPSYNC.slice(0, 120) }] }
const sigSync = { queue: 'smartpms.channel-sync', rowsNow: 3, sample: 3, newest: '2026-10-30T16:11:55.767Z', oldest: '2026-10-30T11:00:12.205Z', groups: ['smartpms'], retryCounts: [0], signatures: [{ n: 2, share: 0.667, text: errorSignature(ISE) }, { n: 1, share: 0.333, text: errorSignature(PARENT) }] }
const syncRows = [
  { id: 'a', errorMessage: ISE, failedAt: '2026-10-30T16:11:55.767Z', consumerGroup: 'smartpms', retryCount: 0 },
  { id: 'b', errorMessage: PARENT, failedAt: '2026-10-30T12:15:30.973Z', consumerGroup: 'smartpms', retryCount: 0 },
  { id: 'c', errorMessage: ISE, failedAt: '2026-10-30T11:00:12.205Z', consumerGroup: 'smartpms', retryCount: 0 },
]
const queues = [{ name: 'channel.opsync_fetch', dead: 8639 }, { name: 'smartpms.channel-sync', dead: 3 }]
const sigs = new Map([['channel.opsync_fetch', { data: sigOpsync }], ['smartpms.channel-sync', { data: sigSync }]])
const read = new Map([['smartpms.channel-sync', { messages: syncRows }]])

test('buildCauses: one cause per signature, counted against the queue', () => {
  const cs = buildCauses(queues, sigs, read)
  assert.equal(cs.length, 3)
  const opsync = cs.find((c) => c.queue === 'channel.opsync_fetch')
  assert.equal(opsync.count, 8639, 'all of the newest 200: the whole queue, scaled')
  assert.equal(opsync.exact, false, '200 of 8,639 were read')
  assert.equal(opsync.cut, true)
  assert.equal(opsync.single, true)
  assert.equal(opsync.newest, Date.parse('2026-10-31T08:10:36.901Z'), 'one cause: the sample says when')
  assert.deepEqual(opsync.retries, [5])
  const [ise, parent] = cs.filter((c) => c.queue === 'smartpms.channel-sync')
  assert.equal(ise.count, 2); assert.equal(parent.count, 1)
  assert.equal(ise.exact, true, 'every row was read and the queue has not moved')
  assert.equal(ise.newest, Date.parse('2026-10-30T16:11:55.767Z'), 'several causes: each its own newest row')
  assert.equal(parent.newest, Date.parse('2026-10-30T12:15:30.973Z'))
  assert.equal(parent.full, PARENT, 'the words in full come from its newest row')
  assert.equal(countText(opsync), '≈ 8,639')
  assert.equal(countText(ise), '2')
})

test('buildCauses: the kinds past the top 8 are one "rest" row; a failed read is said', () => {
  const s = { ...sigSync, rowsNow: 10, sample: 10, signatures: [{ n: 7, share: 0.7, text: errorSignature(ISE) }] }
  const cs = buildCauses([{ name: 'q', dead: 10 }], new Map([['q', { data: s }]]), new Map())
  assert.equal(cs.length, 2)
  assert.equal(cs[1].rest, true); assert.equal(cs[1].count, 3); assert.equal(cs[1].text, '')
  const bad = buildCauses([{ name: 'q', dead: 10 }], new Map([['q', { data: null, error: 'Not found' }]]), new Map())
  assert.equal(bad[0].unknown, true); assert.equal(bad[0].count, 10)
  assert.deepEqual(buildCauses([{ name: 'q', dead: 10 }], new Map(), new Map()), [], 'not read yet: nothing')
})

test('the count is scaled (≈) as soon as the queue moved past the sample it came from', () => {
  const cs = buildCauses([{ name: 'smartpms.channel-sync', dead: 5 }], sigs, read)
  assert.equal(cs[0].exact, false)
  assert.equal(cs[0].count + cs[1].count, 5, 'the causes of a queue add up to the queue')
})

test('still happening: the newest within 10 minutes', () => {
  assert.equal(isLive({ newest: NOW - 20_000 }, NOW), true)
  assert.equal(isLive({ newest: NOW - LIVE_MS - 1 }, NOW), false)
  assert.equal(isLive({ newest: null }, NOW), false)
  assert.equal(isLive({ newest: NOW, gone: true }, NOW), false)
})

test('rankNew: still happening first, then the most recent; a ranked cause keeps its place', () => {
  const cs = buildCauses(queues, sigs, read)
  const r1 = rankNew(new Map(), cs, NOW)
  const order = [...cs].sort((a, b) => r1.get(a.key) - r1.get(b.key)).map((c) => c.count)
  assert.deepEqual(order, [8639, 2, 1])
  const later = { ...cs[2], key: 'q\u0000new', newest: NOW }
  const r2 = rankNew(r1, [...cs, later], NOW)
  assert.equal(r2.get(cs[0].key), r1.get(cs[0].key), 'old ranks unchanged')
  assert.equal(r2.get(later.key), 3, 'a new cause goes after the others, even a live one')
})

test('mergeSeen keeps a vanished cause as none left, or as out of the sample', () => {
  const cs = buildCauses(queues, sigs, read)
  const seen = new Map(cs.map((c) => [c.key, c]))
  const empty = mergeSeen(seen, [], sigs, new Set(['channel.opsync_fetch']))
  const sync = [...empty.values()].filter((c) => c.queue === 'smartpms.channel-sync')
  assert.ok(sync.every((c) => c.gone && c.count === 0), 'its queue has none: none left')
  const opsync = empty.get(cs[0].key)
  assert.equal(opsync.dropped, true, 'still dead letters, older ones unread: not counted, not gone')
  assert.equal(opsync.count, null)
  const scoped = mergeSeen(seen, [], sigs, new Set(), new Set(['smartpms.channel-sync']))
  assert.equal(scoped.get(cs[0].key).gone, undefined, 'a queue this read did not cover stays as it was')
})

test('the one line at the top', () => {
  const cs = buildCauses(queues, sigs, read)
  assert.equal(summaryLine(cs, { total: 8642, queues: 2, now: NOW }), '8,642 dead letters in 2 queues, 3 causes. One is still happening.')
  assert.equal(summaryLine(cs, { total: 8642, queues: 2, now: NOW + LIVE_MS }), '8,642 dead letters in 2 queues, 3 causes. All have stopped.')
  const one = cs.filter((c) => c.queue === 'channel.opsync_fetch')
  assert.equal(summaryLine(one, { total: 8639, queue: 'channel.opsync_fetch', now: NOW }), '8,639 dead letters in channel.opsync_fetch, all of one cause. It is still happening.')
  assert.deepEqual(summaryParts(one, { total: 8639, queue: 'channel.opsync_fetch', now: NOW })[2], { t: 'channel.opsync_fetch', mono: true }, 'the queue is mono')
  assert.equal(summaryLine([], { total: 0, queues: 0 }), 'No dead letters.')
  assert.equal(summaryLine([], { total: 0, queue: 'tf.audit-history' }), 'No dead letters in tf.audit-history.')
  assert.equal(summaryLine([{ unknown: true, key: 'q' }], { total: 10, queues: 1 }), '10 dead letters in 1 queue. Their causes could not be read.')
})

test('how a count was made, in words', () => {
  const [opsync, ise] = buildCauses(queues, sigs, read)
  assert.equal(countedLine(opsync), 'Its newest 200 dead letters all failed this way; the 8,439 older ones were not read, so the count is scaled from these.')
  assert.equal(countedLine(ise), 'One of 2 causes among its 3 dead letters, all read.')
  assert.equal(countedLine({ ...ise, single: true, causes: 1 }), 'Every dead letter of this queue failed this way.')
  assert.equal(countedLine({ ...opsync, n: 120 }), '120 of its newest 200 dead letters failed this way; the 8,439 older ones were not read, so the count is scaled from these.')
  assert.equal(countedLine({ ...ise, gone: true }), '')
})

test('small words', () => {
  assert.equal(retriesWords([5]), 'after 5 retries')
  assert.equal(retriesWords([1]), 'after 1 retry')
  assert.equal(retriesWords([0]), 'with no retry')
  assert.equal(retriesWords([0, 3]), 'after 0 to 3 retries')
  assert.equal(retriesWords([]), '')
  assert.equal(roughly(3300), '55 min')
  assert.equal(roughly(3590), '1 h', 'never "60 min"')
  assert.equal(roughly(7 * 3600 + 600), '7 h')
  assert.equal(roughly(3 * 86_400), '3 d')
  assert.deepEqual(breakable('POST https://a.b/api/v1 [<n>]: {"x":1,"y":2}'), ['POST https://a.b/', 'api/', 'v1 [<n>]: {"x":1,', '"y":2}'], 'never inside "//"')
  assert.equal(breakable(OPSYNC).join(''), OPSYNC, 'the parts are the text')
})

test('files: names safe everywhere, Markdown fences that hold any error', () => {
  assert.equal(fileName('channel.opsync_fetch', 'json', Date.parse('2026-10-31T09:00:00')), 'dead-letters-channel.opsync_fetch-2026-10-31.json')
  assert.equal(fileName('a/b c', 'md', Date.parse('2026-10-31T09:00:00')), 'dead-letters-a_b_c-2026-10-31.md')
  assert.equal(fenced('x ``` y'), '````text\nx ``` y\n````')
  const head = markdownHead({ queue: 'q', text: 'boom <n>' }, 3, { where: 'stage', at: Date.parse('2026-10-31T08:00:00Z') })
  assert.match(head, /^# Dead letters of q\n/)
  assert.match(head, /3 dead letters with one cause/)
  assert.match(head, /```text\nboom <n>\n```/)
})
