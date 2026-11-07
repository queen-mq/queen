import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { FeedReader, whereCondition, sinceToIso, conditionHolds, normalizeWhere } from '../../client-v2/feed/Feed.js'
import { Stream } from '../../client-v2/streams/Stream.js'
import { feedOutputId, outputPrefix, POSITION_NAMESPACE } from '../../client-v2/streams/runtime/FeedRunner.js'
import { fakeBroker, fakeQueen, until } from './fakeBroker.js'

/** An HTTP client that answers from a list and records what it was asked. */
function fakeHttp(answers, name = 'main') {
  const calls = []
  return {
    name,
    calls,
    async post(path, body, timeout) {
      calls.push({ path, body: JSON.parse(JSON.stringify(body)), timeout })
      const next = answers.shift()
      if (!next) throw new Error(`${name}: no answer left`)
      if (next instanceof Error) throw next
      return next
    },
    async destroy() {}
  }
}

const entry = (position, n, extra = {}) => ({
  id: `m${n}`, kind: 'push', offset: n, partition: extra.partition ?? 'c1', partitionId: 'pid-1',
  payload: { n, ...(extra.payload || {}) }, position, queue: extra.queue ?? 'orders',
  transactionId: `t${n}`, ts: '2026-11-06T09:00:00.000000Z'
})
const page = (entries, next, caughtUp = true, applied = next - 1) =>
  ({ applied, caughtUp, entries, next, resolvedTs: '2026-11-06T09:00:01.000000Z' })

describe('where conditions', () => {
  it('builds and checks conditions', () => {
    assert.deepEqual(whereCondition('data.country', '=', 'IT'), { field: 'data.country', op: '=', value: 'IT' })
    assert.deepEqual(whereCondition('data.vip', 'exists'), { field: 'data.vip', op: 'exists' })
    assert.throws(() => whereCondition('x', '~', 1), /unknown op/)
    assert.throws(() => whereCondition('x', 'in', 1), /needs an array/)
    assert.deepEqual(normalizeWhere(['data.a', '=', 1]), [{ field: 'data.a', op: '=', value: 1 }])
    assert.deepEqual(normalizeWhere([['data.a', '=', 1], { field: 'queue', op: '!=', value: 'q' }]).length, 2)
  })

  it('evaluates like the broker on a message', () => {
    const msg = { queue: 'orders', partition: 'booking-9', data: { country: 'IT', total: 40, tags: ['vip'] } }
    assert.equal(conditionHolds(whereCondition('data.country', '=', 'IT'), msg), true)
    assert.equal(conditionHolds(whereCondition('data.total', '>', 100), msg), false)
    assert.equal(conditionHolds(whereCondition('data.tags.0', '=', 'vip'), msg), true)
    assert.equal(conditionHolds(whereCondition('partition', 'prefix', 'booking-'), msg), true)
    assert.equal(conditionHolds(whereCondition('data.country', 'in', ['FR', 'IT']), msg), true)
    // A missing field holds for no op, `!=` included.
    assert.equal(conditionHolds(whereCondition('data.missing', '!=', 'x'), msg), false)
  })

  it('reads since as a duration, a date or a time', () => {
    const now = Date.parse('2026-11-06T10:00:00Z')
    assert.equal(sinceToIso('1d', now), '2026-11-05T10:00:00.000Z')
    assert.equal(sinceToIso('15m', now), '2026-11-06T09:45:00.000Z')
    assert.equal(sinceToIso(new Date(now)), '2026-11-06T10:00:00.000Z')
    assert.equal(sinceToIso('2026-11-01T00:00:00Z'), '2026-11-01T00:00:00.000Z')
    assert.throws(() => sinceToIso('yesterday'), /since must be/)
  })
})

describe('FeedReader', () => {
  it('sends the read, moves to next, and stops once caught up when not following', async () => {
    const http = fakeHttp([
      page([entry(5, 1), entry(6, 2)], 7, false, 9),
      page([entry(9, 3)], 10, true)
    ])
    const reader = new FeedReader(http, () => { throw new Error('no follower') }, 'http', {
      queues: 'orders*', where: [['data.country', '=', 'IT']], from: 'earliest', follow: false
    })
    const seen = []
    for await (const e of reader) seen.push(e.data.n)
    assert.deepEqual(seen, [1, 2, 3])
    assert.equal(reader.position, 10)
    assert.equal(http.calls[0].path, '/api/v1/feed')
    assert.deepEqual(http.calls[0].body.queues, ['orders*'])
    assert.deepEqual(http.calls[0].body.where, [{ field: 'data.country', op: '=', value: 'IT' }])
    assert.equal(http.calls[0].body.from, 'earliest')
    assert.equal(http.calls[0].body.maxWaitMs, 0, 'follow:false never long-polls')
    assert.equal(http.calls[1].body.from, 7)
  })

  it('hands entries out in Queen message shape', async () => {
    const http = fakeHttp([page([entry(5, 1)], 6)])
    const reader = new FeedReader(http, () => null, 'http', { from: 1 })
    const { entries } = await reader.read()
    const e = entries[0]
    assert.deepEqual(e.data, { n: 1 })
    assert.equal(e.createdAt, e.ts)
    assert.equal(e.position, 5)
    assert.equal(e.partition, 'c1')
  })

  it('a leader that answers 421 sends the reader to the followers it names', async () => {
    const refused = Object.assign(new Error('feed on leader'), {
      status: 421, code: 'feed_on_leader',
      body: { code: 'feed_on_leader', followers: [{ nodeId: 2, httpAddr: '10.0.0.2:6632' }, { nodeId: 3, httpAddr: '10.0.0.3:6632' }] }
    })
    const main = fakeHttp([refused])
    const opened = []
    const followers = {
      'https://10.0.0.2:6632': fakeHttp([page([entry(5, 1)], 6), page([entry(6, 2)], 7)], 'f2'),
      'https://10.0.0.3:6632': fakeHttp([], 'f3')
    }
    const reader = new FeedReader(main, (url) => { opened.push(url); return followers[url] }, 'https', { from: 1 })
    const first = await reader.read()
    assert.deepEqual(first.entries.map((e) => e.data.n), [1])
    assert.deepEqual(opened, ['https://10.0.0.2:6632', 'https://10.0.0.3:6632'])
    // The next read goes straight to the follower.
    const second = await reader.read()
    assert.deepEqual(second.entries.map((e) => e.data.n), [2])
    assert.equal(main.calls.length, 1)
    assert.equal(followers['https://10.0.0.2:6632'].calls.length, 2)
  })

  it('a follower that does not answer passes the read to the next node', async () => {
    const down = Object.assign(new Error('ECONNREFUSED'), { code: 'ECONNREFUSED' })
    const main = fakeHttp([page([entry(5, 1)], 6)])
    const f1 = fakeHttp([down], 'f1')
    const reader = new FeedReader(main, () => f1, 'http', { from: 1, followerUrls: ['http://10.0.0.9:6632'] })
    const p = await reader.read()
    assert.deepEqual(p.entries.map((e) => e.data.n), [1])
    assert.equal(f1.calls.length, 1)
    assert.equal(main.calls.length, 1, 'then the configured URL')
  })
})

const fast = (o = {}) => ({ waitMillis: 20, commitEveryMillis: 0, ...o })
const realReads = (b) => b.feedReads.filter((r) => !(r.from === 'now' && r.maxRecords === 1 && r.maxWaitMs === 0))

describe('Stream.fromFeed runner', () => {
  it('pushes the outputs and the new position in one transaction, with ids made from the entry', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push([
      { queue: 'orders', partition: 'c1', data: { n: 1, country: 'IT' } },
      { queue: 'orders', partition: 'c1', data: { n: 2, country: 'IT' } }
    ])
    b.push({ queue: 'orders', partition: 'c1', data: { n: 3, country: 'IT' } })
    b.push({ queue: 'orders', partition: 'c2', data: { n: 4, country: 'FR' } })
    const runner = await Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
      .where('data.country', '=', 'IT')
      .filter((m) => m.data.n !== 2)
      .map((m) => ({ order: m.data.n }))
      .to(queen.queue('orders-it'))
      .run({ queryId: 'orders-it', ...fast() })
    await until(() => b.messages('orders-it').length >= 2)
    await runner.stop()

    const out = b.messages('orders-it')
    assert.deepEqual(out.map((e) => e.payload), [{ order: 1 }, { order: 3 }])
    assert.equal(out[0].partition, 'c1', 'the source partition by default')
    assert.equal(out[0].position, out[1].position, 'one commit')
    assert.equal(out[0].transactionId, feedOutputId('orders-it', b.messages('orders')[0], 0))
    assert.ok(out[0].transactionId.startsWith(outputPrefix('orders-it')))
    const txn = b.txns.find((t) => t.items.length > 0)
    const pos = txn.kv.find((op) => op.key === 'orders-it')
    assert.equal(pos.op, 'putIfAbsent', 'a new query creates its position')
    assert.equal(pos.required, true)
    assert.ok(txn.kv.some((op) => op.op === 'check' && op.ns === 'queen-locks'), 'the commit is guarded by the lock')
    assert.ok(b.kvGet(POSITION_NAMESPACE, 'orders-it').value.position > 4)
    // The broker did the where; the query's own outputs are left out of the read.
    const [first] = realReads(b)
    assert.deepEqual(first.queues, ['orders'])
    assert.deepEqual(first.where, [
      { field: 'data.country', op: '=', value: 'IT' },
      { field: 'transactionId', op: '!prefix', value: outputPrefix('orders-it') }
    ])
    assert.equal(first.from, 'earliest')
  })

  it('a stored position is where it starts, and later commits fence on its version', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    for (let n = 1; n <= 3; n++) b.push({ queue: 'orders', partition: 'c1', data: { n } })
    const v = b.kvPut(POSITION_NAMESPACE, 'copy', { position: 3, queryId: 'copy' })
    const runner = await Stream.fromFeed(queen).to(queen.queue('copy')).run({ queryId: 'copy', ...fast() })
    await until(() => b.messages('copy').length >= 1)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 4 } })
    await until(() => b.messages('copy').length >= 2)
    await runner.stop()
    assert.deepEqual(b.messages('copy').map((e) => e.payload.n), [3, 4], 'from position 3 on')
    assert.equal(realReads(b)[0].from, 3)
    const commits = b.txns.filter((t) => t.items.length > 0)
    assert.equal(commits[0].kv.find((op) => op.key === 'copy').expect, v)
    assert.equal(commits[1].kv.find((op) => op.key === 'copy').expect, commits[0].written.copy, 'the version the first commit answered')
  })

  it('a lost commit carries on from the position that won, writing nothing of its own', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    let winner = null
    b.injectNext({
      before: (broker) => {
        // Another process committed first: the position moved past n=2.
        broker.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
        winner = broker.applied + 1
        broker.kvPut(POSITION_NAMESPACE, 'copy', { position: winner, queryId: 'copy' })
      }
    })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .to(queen.queue('copy')).run({ queryId: 'copy', ...fast({ exclusive: false }) })
    await until(() => runner.metrics().conflicts >= 1)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 3 } })
    await until(() => b.messages('copy').length >= 1)
    await runner.stop()
    assert.deepEqual(b.messages('copy').map((e) => e.payload.n), [3], 'n=1 lost to the winner, n=2 was the winner\'s')
    assert.ok(realReads(b).some((r) => r.from === winner), 'read again from the winner')
  })

  it('a commit refused as duplicate (another runner pushed these outputs) reads on from the stored position', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    const src = b.messages('orders')[0]
    let winner = null
    b.injectNext({
      before: (broker) => {
        broker.ids.add(`copy\u0000c1\u0000${feedOutputId('race', src, 0)}`)
        winner = broker.applied + 1
        broker.kvPut(POSITION_NAMESPACE, 'race', { position: winner, queryId: 'race' })
      }
    })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .to(queen.queue('copy')).run({ queryId: 'race', ...fast({ exclusive: false }) })
    await until(() => runner.metrics().conflicts >= 1)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
    await until(() => b.messages('copy').length >= 1)
    await runner.stop()
    assert.deepEqual(b.messages('copy').map((e) => e.payload.n), [2])
    assert.ok(realReads(b).some((r) => r.from === winner), 'read on from the winner, not the same page again')
    assert.equal(runner.metrics().errors, 0, 'a lost race is not an error')
  })

  it('a broker without the feed fails run() with the reason, instead of retrying in silence', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    queen.feed = (options) => new FeedReader({
      async post() { throw Object.assign(new Error('HTTP 404: Not Found'), { status: 404 }) }
    }, () => null, 'http', options)
    await assert.rejects(
      Stream.fromFeed(queen).to(queen.queue('copy')).run({ queryId: 'old-broker' }),
      (err) => err.code === 'no_feed_route' && /no POST \/api\/v1\/feed/.test(err.message)
    )
  })

  it('a cycle error with no onError hook is said on the console', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    b.injectNext({ error: Object.assign(new Error('HTTP 503: Service Unavailable'), { status: 503 }) })
    const said = []
    const original = console.error
    console.error = (...args) => said.push(args.join(' '))
    try {
      const runner = await Stream.fromFeed(queen, { from: 'earliest' }).to(queen.queue('copy')).run({ queryId: 'loud', ...fast() })
      await until(() => b.messages('copy').length >= 1, 5000)
      await runner.stop()
    } finally {
      console.error = original
    }
    assert.ok(said.some((s) => s.includes('[queen feed] loud') && s.includes('503') && s.includes('(retrying)')), said.join('\n'))
    assert.equal(b.messages('copy').length, 1, 'the retry wrote it once')
  })

  it('refuses a chain it cannot run on a feed', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    await assert.rejects(
      Stream.fromFeed(queen).gate(() => true).to(queen.queue('x')).run({ queryId: 'g' }),
      /gate\(\) runs on queue sources only/
    )
    await assert.rejects(Stream.fromFeed(queen).map((m) => m).run({ queryId: 'nosink' }), /end the chain/)
    await assert.rejects(Stream.fromFeed(queen).to(queen.queue('x')).run({ queryId: 'a#b' }), /without '#'/)
    await assert.rejects(
      Stream.fromFeed(queen).to(queen.queue('x')).run({ queryId: 'p', onPoison: 42 }),
      /onPoison is 'stop', 'skip' or a queue/
    )
  })
})
