// Feed streams on the Streams page (src/console/streams/feedStreams.js): the
// keys a feed stream keeps in `queen-feed`, its lock, and the row the page
// draws from them.
import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  FEED_NAMESPACE, behindCell, committedText, feedAfter, feedKeyOf, feedKeysToRead, feedListBody, feedLockName, feedNeedsText,
  feedStreamRows, feedWhy, lockQueryOf, ownerParts, readsOf, runsOnText, scanFeedKeys, shardState, sinkOf, stateSavedText,
  stepWords, stoppedAt, whereText, readsQueue, withFeedReaders, feedQueueState,
} from '../src/console/streams/feedStreams.js'

const NOW = Date.parse('2026-11-07T12:00:00.000Z')
const iso = (msAgo) => new Date(NOW - msAgo).toISOString()
const permit = (name, owner = 'qf-run-1:4242:a1b2c3', sinceAgo = 600_000) => ({
  name, owner, slot: 0, since: iso(sinceAgo), renewedAt: iso(1000), expiresAt: new Date(NOW + 14_000).toISOString(), foreign: false,
})

test('feedKeyOf: what each key of queen-feed is', () => {
  assert.deepEqual(feedKeyOf('orders-it'), { type: 'position', queryId: 'orders-it', shard: null, id: 'orders-it' })
  assert.deepEqual(feedKeyOf('orders-it#shard2of4'), { type: 'position', queryId: 'orders-it', shard: { index: 2, count: 4 }, id: 'orders-it#shard2of4' })
  assert.deepEqual(feedKeyOf('orders-it#about'), { type: 'about', queryId: 'orders-it' })
  assert.deepEqual(feedKeyOf('orders-it#layout'), { type: 'layout', queryId: 'orders-it' })
  assert.deepEqual(feedKeyOf('orders-it#shard0of4#snap#k3x#12'), { type: 'chunk', id: 'orders-it#shard0of4' })
  assert.deepEqual(feedKeyOf('orders-it#snap#k3x#0'), { type: 'chunk', id: 'orders-it' })
  assert.equal(feedKeyOf('orders-it#3'), null, 'state of the old bucket format')
  assert.equal(feedKeyOf('orders-it#shard4of4'), null, 'no shard 4 of 4')
  assert.equal(feedKeyOf('orders-it#shard01of4'), null)
  assert.equal(feedKeyOf('#about'), null)
  assert.equal(feedKeyOf(''), null)
  assert.equal(feedKeyOf(null), null)
})

test('feedListBody: keys only, a full page, the cursor when there is one', () => {
  assert.deepEqual(feedListBody(), { namespace: FEED_NAMESPACE, keysOnly: true, limit: 1000 })
  assert.deepEqual(feedListBody('a#about'), { namespace: FEED_NAMESPACE, keysOnly: true, limit: 1000, after: 'a#about' })
})

test('feedAfter: a page that ends in chunks jumps past the rest of them', () => {
  assert.equal(feedAfter([{ key: 'a' }, { key: 'a#snap#s1#0' }], 'a#snap#s1#0'), 'a#snap$')
  assert.ok('a#snap#zzzz#999' < 'a#snap$' && 'a#snap$' < 'a-eu', '$ sorts after every #snap# key, before the next stream')
  assert.ok('q#shard0of2#snap$' < 'q#shard1of2')
  assert.equal(feedAfter([{ key: 'a' }, { key: 'b#about' }], 'b#about'), 'b#about')
})

test('scanFeedKeys: pages, chunk runs skipped, and an inexact answer past the page budget', async () => {
  const all = ['a', 'a#about', ...Array.from({ length: 2500 }, (_, i) => `a#snap#s1#${String(i).padStart(5, '0')}`), 'b#layout', 'b#shard0of2', 'b#shard1of2']
    .sort()
  const asked = []
  const listPage = async (body) => {
    asked.push(body.after ?? null)
    const from = body.after ? all.filter((k) => k > body.after) : all
    const rows = from.slice(0, body.limit).map((key, i) => ({ key, version: i + 1, updatedAt: iso(0) }))
    return { rows, truncated: from.length > body.limit, nextAfter: from.length > body.limit ? rows[rows.length - 1].key : null }
  }
  const { keys, exact } = await scanFeedKeys(listPage)
  assert.equal(exact, true)
  assert.deepEqual([...keys.keys()], ['a', 'a#about', 'b#layout', 'b#shard0of2', 'b#shard1of2'])
  assert.deepEqual(asked, [null, 'a#snap$'], 'two pages, not four: the chunk run is skipped at once')
  assert.deepEqual(feedKeysToRead(keys), ['a', 'a#about', 'b#layout', 'b#shard0of2', 'b#shard1of2'])
  const short = await scanFeedKeys(async () => ({ rows: [{ key: 'x' }], truncated: true, nextAfter: 'x' }), { maxPages: 3 })
  assert.equal(short.exact, false, 'a cursor that does not move stops the scan')
})

test('feedLockName and ownerParts: the lock a runner holds, and who holds it', () => {
  assert.equal(feedLockName('orders-it'), 'queen-feed/orders-it')
  assert.equal(feedLockName('orders-it', { index: 1, count: 4 }), 'queen-feed/orders-it/shard-1-of-4')
  assert.deepEqual(ownerParts('qf-run-1:4242:a1b2c3d4e5f6'), { host: 'qf-run-1', pid: 4242 })
  assert.deepEqual(ownerParts('my-own-owner'), { host: 'my-own-owner', pid: null })
  assert.deepEqual(ownerParts(null), { host: null, pid: null })
})

test('shardState: a runner that holds the lock, by its record', () => {
  const lock = permit('queen-feed/s')
  const caught = shardState({ record: { position: 812, updatedAt: iso(2000), caughtUp: true, readTo: NOW - 90_000 }, permit: lock, now: NOW })
  assert.equal(caught.state, 'running')
  assert.equal(caught.behindMs, 0, 'caughtUp wins over an old readTo: the broker clock moves only with writes')
  assert.deepEqual([caught.holder.host, caught.holder.pid], ['qf-run-1', 4242])
  const behind = shardState({ record: { updatedAt: iso(1000), caughtUp: false, readTo: NOW - 1000 - 125_000 }, permit: lock, now: NOW })
  assert.equal(behind.behindMs, 125_000)
  const close = shardState({ record: { updatedAt: iso(1000), caughtUp: false, readTo: NOW - 1000 - 900 }, permit: lock, now: NOW })
  assert.equal(close.behindMs, 0, 'under 2 s is not behind')
  const old = shardState({ record: { updatedAt: iso(1000), clock: NOW - 1000 - 6000 }, permit: lock, now: NOW })
  assert.equal(old.behindMs, 0, 'an older runner: its clock lags one position commit on a quiet feed')
  const unknown = shardState({ record: { updatedAt: iso(1000) }, permit: lock, now: NOW })
  assert.equal(unknown.behindMs, null)
  assert.equal(shardState({ permit: permit('queen-feed/s', undefined, 5000), now: NOW }).state, 'starting')
})

test('shardState: a lock held with no write for a while is a runner that does not get on', () => {
  const stale = shardState({ record: { updatedAt: iso(5 * 60_000) }, permit: permit('queen-feed/s'), now: NOW })
  assert.equal(stale.state, 'quiet')
  assert.equal(stale.quietMs, 5 * 60_000)
  const takeover = shardState({ record: { updatedAt: iso(3 * 3600_000) }, permit: permit('queen-feed/s', undefined, 10_000), now: NOW })
  assert.equal(takeover.state, 'running', 'a runner that took over 10 s ago has not had its turn yet')
  const slow = shardState({ record: { updatedAt: iso(100_000) }, permit: permit('queen-feed/s'), about: { positionCommitMillis: 60_000 }, now: NOW })
  assert.equal(slow.state, 'running', 'three of its own position commits: 3 min')
  const first = shardState({ permit: permit('queen-feed/s', undefined, 5 * 60_000), now: NOW })
  assert.deepEqual([first.state, first.quietMs], ['quiet', 5 * 60_000], 'a lock held for 5 min with no first commit')
  assert.equal(shardState({ permit: permit('queen-feed/s', undefined, 20_000), now: NOW }).state, 'starting')
})

test('shardState: no lock, by what the record says', () => {
  const failed = shardState({ record: { stopped: { reason: 'poison', error: 'bad order' }, updatedAt: iso(60_000) }, now: NOW })
  assert.equal(failed.state, 'failed')
  assert.equal(failed.stopped.error, 'bad order')
  assert.equal(shardState({ record: { stopped: { reason: 'stop' } }, now: NOW }).state, 'stopped')
  assert.equal(shardState({ record: { updatedAt: iso(60_000) }, now: NOW }).state, 'gone')
  assert.equal(shardState({ now: NOW }).state, 'never')
  assert.equal(shardState({ record: { updatedAt: iso(3000), caughtUp: true }, about: { exclusive: false }, now: NOW }).state, 'running',
    'exclusive: false takes no lock: a fresh record is a runner')
  const expired = { ...permit('queen-feed/s'), expiresAt: iso(1) }
  assert.equal(shardState({ record: { updatedAt: iso(60_000) }, permit: expired, now: NOW }).state, 'gone')
})

const about = {
  v: 1,
  queryId: 'unanswered',
  hash: '3fa9c2e1d0b74a55',
  client: 'client-js 2.7.0',
  reads: { queues: ['smartchat.incoming', 'smartchat.outgoing'], partitions: null, where: [{ field: 'data.channel', op: '=', value: 'whatsapp' }] },
  steps: [
    { kind: 'ops', ops: ['map'] },
    { kind: 'window', name: 'perMinute', clock: 'feed', window: { kind: 'window-tumbling', seconds: 60, gracePeriod: 0 }, reducer: { kind: 'aggregate', fields: ['count'] } },
    { kind: 'pattern', name: 'unanswered', clock: 'event', config: { kind: 'pattern', within: 900_000, timeout: true } },
  ],
  sink: { kind: 'queues', queues: ['alerts.unanswered'] },
  onPoison: { queue: 'feed-dlq' },
  shards: { count: 3, by: 'partition' },
  exclusive: true,
  positionCommitMillis: 5000,
}

test('readsOf, sinkOf, whereText: what the description says it reads and writes', () => {
  const r = readsOf(about)
  assert.equal(r.text, 'smartchat.incoming, smartchat.outgoing')
  assert.deepEqual(r.where, ['data.channel = "whatsapp"'])
  assert.equal(readsOf({ reads: { queues: null } }).text, 'every queue')
  assert.equal(readsOf(null), null)
  assert.deepEqual(sinkOf(about), { kind: 'queues', queues: ['alerts.unanswered'], text: 'alerts.unanswered' })
  assert.equal(sinkOf({ sink: { kind: 'route' } }).text, 'queues a function picks')
  assert.equal(whereText({ field: 'queue', op: 'exists' }), 'queue exists')
})

test('stepWords: each kind of step, with what it holds', () => {
  assert.deepEqual(stepWords(about.steps[1], { perMinute: 231 }), {
    kind: 'window', name: 'perMinute', label: 'window', detail: 'tumbling 1 min · aggregate count', clock: 'feed clock', open: 231, held: null,
  })
  const p = stepWords(about.steps[2], { unanswered: 912 }, { unanswered: 3 })
  assert.deepEqual([p.label, p.detail, p.clock, p.open, p.held], ['pattern', 'within 15 min, with a deadline', 'event time', 912, 3])
  assert.equal(stepWords({ kind: 'gather', name: 'runs', config: { parts: ['invoice', 'payment'], within: 3600_000 } }).detail, 'of invoice, payment, within 1 h')
  assert.equal(stepWords({ kind: 'gather', name: 'runs', config: { count: true, within: 60_000 } }).detail, 'the count from its messages, within 1 min')
  assert.equal(stepWords({ kind: 'join', name: 'j', config: { parts: ['a', 'b', 'c'], within: 300_000 } }).detail, 'of a, b, c, within 5 min')
  assert.equal(stepWords({ kind: 'table', name: 'settings', config: { queues: ['settings'] } }).detail, 'of settings')
  assert.equal(stepWords({ kind: 'window', name: 's', window: { kind: 'window-session', gap: 30, gracePeriod: 5 }, reducer: { kind: 'reduce' } }).detail,
    'session, gap 30 s, grace 5 s · reduce')
  assert.deepEqual(stepWords({ kind: 'ops', ops: ['where', 'map'], where: [{ field: 'data.n', op: '>', value: 3 }] }).detail, 'data.n > 3')
})

test('feedStreamRows: a stream in three shards on two hosts, one behind', () => {
  const values = new Map([
    ['unanswered#about', about],
    ['unanswered#layout', { shards: 3, shardBy: 'partition' }],
    ['unanswered#shard0of3', { position: 900, updatedAt: iso(1000), caughtUp: true, open: { perMinute: 100, unanswered: 300 }, epoch: 0, snapshot: { bytes: 2_000_000, layers: [{}, {}], clock: NOW - 38_000 } }],
    ['unanswered#shard1of3', { position: 880, updatedAt: iso(2000), caughtUp: false, readTo: NOW - 2000 - 40_000, open: { perMinute: 120, unanswered: 312 }, held: { unanswered: 3 }, snapshot: { bytes: 2_400_000, layers: [{}], clock: NOW - 50_000 } }],
    ['unanswered#shard2of3', { position: 905, updatedAt: iso(1500), caughtUp: true, open: { perMinute: 11, unanswered: 300 }, skips: ['e\u001f1'] }],
  ])
  const keys = new Map([...values.keys(), 'unanswered#shard0of3#snap#s1#0'].map((k) => [k, {}]))
  const permits = [
    permit('queen-feed/unanswered/shard-0-of-3', 'qf-run-1:4242:aa'),
    permit('queen-feed/unanswered/shard-1-of-3', 'qf-run-1:4251:bb'),
    permit('queen-feed/unanswered/shard-2-of-3', 'qf-run-2:3107:cc'),
    { name: 'queen-feed/other', owner: 'x:1:ff', slot: 0, foreign: false, expiresAt: new Date(NOW + 9000).toISOString() },
  ]
  const rows = feedStreamRows({ keys, values, permits, now: NOW })
  assert.deepEqual(rows.map((r) => r.name), ['other', 'unanswered'], 'a lock alone is a stream too')
  const row = rows.find((r) => r.name === 'unanswered')
  assert.equal(row.kind, 'feed')
  assert.equal(row.name, 'unanswered')
  assert.equal(row.shardCount, 3)
  assert.equal(row.status, 'running')
  assert.equal(row.needs, '')
  assert.equal(row.behindMs, 40_000, 'the furthest behind of the shards')
  assert.deepEqual(behindCell(row), { text: '40 s', tone: '' })
  assert.equal(row.open, 1143)
  assert.deepEqual(row.openBy, { perMinute: 231, unanswered: 912 })
  assert.deepEqual(row.heldBy, { unanswered: 3 })
  assert.equal(row.source, 'smartchat.incoming, smartchat.outgoing')
  assert.equal(row.sinkText, 'alerts.unanswered')
  assert.equal(row.deadLetters, 'feed-dlq')
  assert.equal(runsOnText(row), '3 processes on 2 hosts')
  assert.equal(row.skipped, 1)
  assert.equal(stateSavedText(row), '4.2 MB in 3 layers, as of 50 s ago')
  assert.equal(feedWhy(row), null)
})

test('feedStreamRows: shards in one process are one process; a state not written yet says where a restart reads from', () => {
  const values = new Map([
    ['g#layout', { shards: 2 }],
    ['g#shard0of2', { position: 40, updatedAt: iso(500), caughtUp: true, open: { w: 3 }, snapshot: { bytes: 0, chunks: 0, position: 7, clock: NOW - 60_000 } }],
    ['g#shard1of2', { position: 41, updatedAt: iso(9000), caughtUp: true, open: { w: 1 }, snapshot: { bytes: 0, chunks: 0, position: 9, clock: NOW - 60_000 } }],
  ])
  const permits = [permit('queen-feed/g/shard-0-of-2', 'box:77:aaaa'), permit('queen-feed/g/shard-1-of-2', 'box:77:bbbb')]
  const [row] = feedStreamRows({ keys: new Map([...values.keys()].map((k) => [k, {}])), values, permits, now: NOW })
  assert.equal(runsOnText(row), 'box, pid 77, all 2 shards', 'one lock per shard, each its own owner: still one process')
  assert.equal(stateSavedText(row), 'none written yet: a restart reads the feed again from position 7')
  assert.equal(committedText(row), 'just now, the latest of 2 shards')
  const empty = feedStreamRows({ keys: new Map([['e', {}]]), values: new Map([['e', { position: 3, updatedAt: iso(120_000), caughtUp: true, open: { w: 0 }, snapshot: { bytes: 0, position: 3 } }]]), now: NOW })[0]
  assert.equal(stateSavedText(empty), 'nothing to keep')
  assert.equal(committedText(empty), 'position 3, 2 min ago')
})

test('lockQueryOf, feedStreamRows: a runner that has not committed yet is found by its lock', () => {
  assert.deepEqual(lockQueryOf('queen-feed/a/b'), { queryId: 'a/b', shard: null }, 'a queryId may hold a slash')
  assert.deepEqual(lockQueryOf('queen-feed/q/shard-1-of-4'), { queryId: 'q', shard: { index: 1, count: 4 } })
  assert.equal(lockQueryOf('queen-feed/'), null)
  assert.equal(lockQueryOf('jobs/nightly'), null)
  const rows = feedStreamRows({ keys: new Map(), values: new Map(), permits: [permit('queen-feed/slow', undefined, 5 * 60_000)], now: NOW })
  assert.deepEqual(rows.map((r) => [r.name, r.status, r.needs]), [['slow', 'quiet', 'amber']])
  assert.equal(feedWhy(rows[0]).text, 'It holds its lock and has written nothing for 5 min, on qf-run-1 (pid 4242).')
})

test('feedStreamRows: a stream without a description still shows, from its record and its lock', () => {
  const values = new Map([['copy', { position: 12, updatedAt: iso(3000) }]])
  const [row] = feedStreamRows({ keys: new Map([['copy', {}]]), values, permits: [permit('queen-feed/copy')], now: NOW })
  assert.equal(row.status, 'running')
  assert.equal(row.source, null)
  assert.equal(row.reads, null)
  assert.equal(runsOnText(row), 'qf-run-1, pid 4242')
  assert.equal(row.position, 12)
})

test('feedWhy: a stream stopped on a message says on which, and what to do', () => {
  const values = new Map([['pay', { position: 812, updatedAt: iso(720_000), stopped: { reason: 'poison', at: iso(720_000), error: 'bad order', attempts: 4, position: 812, queue: 'payments', partition: 'p-77', offset: 41 } }]])
  const [row] = feedStreamRows({ keys: new Map([['pay', {}]]), values, now: NOW })
  assert.equal(row.status, 'failed')
  assert.equal(row.needs, 'amber')
  assert.deepEqual(behindCell(row), { text: 'stopped', tone: 'amber' })
  assert.equal(feedNeedsText(row), 'stopped on a message it cannot process.')
  const why = feedWhy(row)
  assert.equal(why.tone, 'amber')
  assert.equal(why.text, 'It stopped 12 min ago at the message at position 812 (payments/p-77, offset 41) after 4 tries: bad order.')
  assert.match(why.hint, /onPoison: 'skip'/)
  assert.equal(stoppedAt({ step: 'perMinute', result: 'window', key: 'c1' }), 'the window of step perMinute, key "c1"')
})

test('feedWhy: no runner, no writes, and part of the shards stopped', () => {
  const gone = feedStreamRows({ keys: new Map([['g', {}]]), values: new Map([['g', { updatedAt: iso(3600_000) }]]), now: NOW })[0]
  assert.equal(gone.status, 'gone')
  assert.equal(feedNeedsText(gone), 'has no runner.')
  assert.equal(feedWhy(gone).text, 'Nothing has run it for 1 h: its lock is free, and its last commit does not say it stopped.')
  const quiet = feedStreamRows({ keys: new Map([['q', {}]]), values: new Map([['q', { updatedAt: iso(600_000) }]]), permits: [permit('queen-feed/q')], now: NOW })[0]
  assert.equal(quiet.status, 'quiet')
  assert.equal(feedWhy(quiet).text, 'It holds its lock and has written nothing for 10 min, on qf-run-1 (pid 4242).')
  const values = new Map([
    ['p#layout', { shards: 2 }],
    ['p#shard0of2', { updatedAt: iso(1000), caughtUp: true }],
    ['p#shard1of2', { updatedAt: iso(9000), stopped: { reason: 'stop', at: iso(9000) } }],
  ])
  const partial = feedStreamRows({ keys: new Map([...values.keys()].map((k) => [k, {}])), values, permits: [permit('queen-feed/p/shard-0-of-2')], now: NOW })[0]
  assert.equal(partial.status, 'partial')
  assert.equal(partial.needs, '')
  assert.equal(feedWhy(partial).text, '1 of 2 shards were stopped by their process; the others run.')
  const stopped = feedStreamRows({ keys: new Map([['s', {}]]), values: new Map([['s', { position: 5, updatedAt: iso(180_000), stopped: { reason: 'stop', at: iso(180_000) } }]]), now: NOW })[0]
  assert.equal(stopped.status, 'stopped')
  assert.equal(stopped.needs, '')
  assert.deepEqual(feedWhy(stopped), { tone: '', text: 'Its process stopped it 3 min ago. It goes on from its position when it starts again.', hint: null })
  const never = feedStreamRows({ keys: new Map([['n#about', {}]]), values: new Map([['n#about', about]]), now: NOW })[0]
  assert.equal(never.status, 'never')
  assert.equal(never.shardCount, 3, 'the description says how many shards')
})

test('readsQueue and withFeedReaders: which queues a feed stream reads', () => {
  assert.equal(readsQueue({ queues: ['*'] }, 'feed-3'), true)
  assert.equal(readsQueue({ queues: null }, 'anything'), true, 'no queues named: every queue')
  assert.equal(readsQueue({ queues: ['pay*', 'orders'] }, 'payments'), true)
  assert.equal(readsQueue({ queues: ['pay*', 'orders'] }, 'orders.copy'), false)
  assert.equal(readsQueue({ queues: ['orders'], tables: ['settings'] }, 'settings'), true, 'a table join reads its queue')
  assert.equal(readsQueue(null, 'orders'), false)
  const streams = [
    { name: 'copy', status: 'running', needs: '', reads: { queues: ['orders'] }, about: { reads: { queues: ['orders'] } } },
    { name: 'all', status: 'gone', needs: 'amber', reads: { queues: ['*'] }, about: { reads: { queues: ['*'] } } },
    { name: 'old', status: 'running', needs: '', reads: null, about: null },
  ]
  const queues = [{ name: 'orders' }, { name: 'audit' }]
  const out = withFeedReaders(queues, streams)
  assert.deepEqual(out[0].feedReaders, [{ name: 'copy', status: 'running', needs: '' }, { name: 'all', status: 'gone', needs: 'amber' }])
  assert.deepEqual(out[1].feedReaders.map((r) => r.name), ['all'])
  assert.equal(queues[0].feedReaders, undefined, 'the given rows are not changed')
  assert.equal(withFeedReaders(queues, []), queues, 'no stream: the same list')
  assert.equal(withFeedReaders(queues, [streams[2]]), queues, 'a stream without a description reads nothing known')
})

test('feedQueueState: what a queue read by feed streams says', () => {
  assert.equal(feedQueueState([]), null)
  const run = feedQueueState([{ name: 'copy', status: 'running' }], 115_000)
  assert.deepEqual([run.kind, run.word, run.tone], ['feed', 'Feed stream', ''])
  assert.equal(run.title, 'Read by the feed stream copy. It reads without a consumer group, so nothing here is acked: the queue holds 115,000 messages until retention removes them.')
  assert.deepEqual([feedQueueState([{ name: 'a', status: 'stopped' }]).word, feedQueueState([{ name: 'a', status: 'stopped' }]).tone], ['Feed stopped', ''])
  const down = feedQueueState([{ name: 'a', status: 'gone' }, { name: 'b', status: 'failed' }], 3)
  assert.deepEqual([down.word, down.tone], ['Feed down', 'amber'])
  assert.match(down.title, /^Read by 2 feed streams \(a, b\), and nothing runs them now: see Streams\./)
  assert.equal(feedQueueState([{ name: 'a', status: 'gone' }, { name: 'b', status: 'running' }]).word, 'Feed stream', 'one running is enough')
  const empty = feedQueueState([{ name: 'a', status: 'gone' }], 0)
  assert.deepEqual([empty.kind, empty.word, empty.tone], ['feed', 'Feed down', ''], 'down with nothing held: a fact, not a need')
})
