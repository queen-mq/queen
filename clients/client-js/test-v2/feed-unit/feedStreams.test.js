// The five things a feed stream does beyond a stateless copy: a message that
// keeps failing (poison), one runner per queryId (standby), routing to many
// queues, windows on the feed's clock, and patterns/joins across queues.
// Each runs the client's own Kv, Lock, TransactionBuilder and FeedReader
// against the broker in memory of fakeBroker.js.
import { describe, it, afterEach } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { outputPrefix, POSITION_NAMESPACE } from '../../client-v2/streams/runtime/FeedRunner.js'
import { fakeBroker, fakeQueen, until } from './fakeBroker.js'

const T0 = Date.parse('2026-11-06T09:00:00.000Z')
const fast = (o = {}) => ({ waitMillis: 20, commitEveryMillis: 0, retryDelayMillis: 1, ...o })
const payloads = (b, q) => b.messages(q).map((e) => e.payload)
const at = (b, ms) => { b.nowMs = T0 + ms }
const quiet = (o = {}) => ({ onError: () => {}, ...o })

// Every runner a test starts is stopped after it, passed or failed.
const started = []
const _run = Stream.prototype.run
Stream.prototype.run = async function (opts) {
  const runner = await _run.call(this, opts)
  started.push(runner)
  return runner
}
afterEach(async () => {
  while (started.length) await started.pop().stop().catch(() => {})
})

describe('feed streams: poison messages', () => {
  const orders = (b, n) => {
    for (let i = 1; i <= n; i++) b.push({ queue: 'orders', partition: `c${i}`, data: { n: i } })
  }

  it("stops at a message that keeps failing, by default, with what came before committed", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 5)
    let calls = 0
    const errors = []
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => {
        if (m.data.n === 3) { calls++; throw new Error('bad order') }
        return { n: m.data.n }
      })
      .to(queen.queue('out'))
      .run({ queryId: 'stop', ...fast({ maxRecords: 2 }), onError: (err, ctx) => errors.push({ err, ctx }) })
    await runner.done
    assert.deepEqual(payloads(b, 'out'), [{ n: 1 }, { n: 2 }])
    assert.equal(calls, 4, 'the first try and 3 retries')
    const third = b.messages('orders')[2]
    assert.equal(b.kvGet(POSITION_NAMESPACE, 'stop').value.position, third.position, 'the stored position is the message')
    const m = runner.metrics()
    assert.equal(m.role, 'stopped')
    assert.equal(m.stopped.reason, 'poison')
    assert.equal(m.stopped.position, third.position)
    const stop = errors.find((e) => e.ctx.action === 'stopped')
    assert.ok(stop, 'onError hears it')
    assert.match(stop.err.message, /position \d+ \(orders\/c3, offset 1\) failed 4 times: bad order/)
    assert.equal(stop.err.code, 'feed_poison')
    await runner.stop()
  })

  it("'skip' leaves the message out and carries on", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 5)
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => { if (m.data.n === 3) throw new Error('bad order'); return { n: m.data.n } })
      .to(queen.queue('out'))
      .run({ queryId: 'skip', ...fast(), ...quiet({ onPoison: 'skip' }) })
    await until(() => b.messages('out').length >= 4)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out').map((p) => p.n), [1, 2, 4, 5])
    assert.equal(runner.metrics().skipped, 1)
    assert.equal(runner.metrics().poisoned, 1)
  })

  it('a dead-letter queue gets the message and the error, in the commit of the others', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 4)
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => { if (m.data.n === 2) throw new TypeError('no price'); return { n: m.data.n } })
      .to(queen.queue('out'))
      .run({ queryId: 'dlq', ...fast(), ...quiet({ onPoison: queen.queue('out-dlq'), retries: 1 }) })
    await until(() => b.messages('out').length >= 3)
    await runner.stop()
    const [dead] = b.messages('out-dlq')
    assert.ok(dead, 'dead-lettered')
    assert.equal(dead.partition, 'c2', 'in the partition it came from')
    assert.deepEqual(dead.payload.entry.data, { n: 2 })
    assert.equal(dead.payload.entry.queue, 'orders')
    assert.deepEqual(dead.payload.error, { name: 'TypeError', message: 'no price' })
    assert.equal(dead.payload.attempts, 2)
    assert.equal(dead.payload.queryId, 'dlq')
    assert.ok(dead.transactionId.startsWith(outputPrefix('dlq')), 'the query never reads it back')
    assert.equal(dead.position, b.messages('out')[0].position, 'one commit with the outputs')
    assert.equal(runner.metrics().deadLettered, 1)
  })

  it('a function that fails, then passes, is not poison', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 3)
    let tries = 0
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => {
        if (m.data.n === 2 && ++tries < 3) throw new Error('flaky')
        return { n: m.data.n }
      })
      .to(queen.queue('out'))
      .run({ queryId: 'flaky', ...fast() })
    await until(() => b.messages('out').length >= 3)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out').map((p) => p.n), [1, 2, 3])
    assert.equal(runner.metrics().retries, 2)
    assert.equal(runner.metrics().poisoned, 0)
  })

  it('a message tried again counts its results once in metrics().emitted; a skipped one counts none', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 3)
    let failed = false
    const calls = []
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .flatMap((m) => [m.data.n, m.data.n * 10])
      .foreach((v) => {
        calls.push(v)
        if (v === 20 && !failed) { failed = true; throw new Error('once') }
        if (v === 30) throw new Error('always')
      })
      .run({ queryId: 'emitted-once', ...fast(), ...quiet({ onPoison: 'skip' }) })
    await until(() => runner.metrics().skipped >= 1)
    await runner.stop()
    assert.deepEqual(calls.slice(0, 5), [1, 10, 2, 20, 2], 'message 2 runs again from its first result')
    assert.equal(runner.metrics().emitted, 4, 'the four results of messages 1 and 2, once each')
    assert.equal(runner.metrics().skipped, 1)
  })

  it('a retried message starts again from the state before it, even after an in-place change', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 1; i <= 3; i++) b.push({ queue: 'orders', partition: 'c1', data: { n: i } })
    let failed = false
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .windowTumbling({ seconds: 60 })
      .reduce((acc, v) => {
        acc.sum += v.n
        acc.count += 1
        if (v.n === 2 && !failed) { failed = true; throw new Error('once') }
        return acc
      }, { sum: 0, count: 0 })
      .to(queen.queue('sums'))
      .run({ queryId: 'undo', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 3)
    at(b, 61000)
    b.push({ queue: 'other', partition: 'x', data: {} })
    await until(() => b.messages('sums').length >= 1)
    await runner.stop()
    assert.deepEqual(payloads(b, 'sums'), [{ sum: 6, count: 3 }], 'the failed try left nothing behind')
  })

  it('a window result whose sink function keeps failing is skipped, and the stream goes on', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'bad', data: { n: 1 } })
    b.push({ queue: 'orders', partition: 'good', data: { n: 2 } })
    const seen = []
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1 })
      .foreach((value, ctx) => {
        if (ctx.key === 'bad') throw new Error('downstream refused')
        seen.push({ key: ctx.key, ...value })
      })
      .run({ queryId: 'emit-skip', ...fast({ now: () => b.nowMs }), ...quiet({ onPoison: 'skip' }) })
    await until(() => runner.metrics().entriesRead >= 2)
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'good', data: { n: 3 } })
    await until(() => seen.length >= 1 && runner.metrics().skipped >= 1)
    await runner.stop()
    assert.deepEqual(seen, [{ key: 'good', count: 1 }])
  })
})

describe('feed streams: one runner per queryId', () => {
  it('a second process on the queryId waits: each message runs .foreach() once', async () => {
    const b = fakeBroker()
    const qa = fakeQueen(b, 'a')
    const qb = fakeQueen(b, 'b')
    for (let i = 1; i <= 40; i++) b.push({ queue: 'orders', partition: `c${i % 5}`, data: { n: i } })
    const calls = []
    const effect = (q, who) => Stream.fromFeed(q, { from: 'earliest' })
      .foreach((v) => { calls.push({ who, n: v.n }) })
    const ra = await effect(qa, 'a').run({ queryId: 'fx', ...fast({ maxRecords: 7 }) })
    const rb = await effect(qb, 'b').run({ queryId: 'fx', ...fast({ maxRecords: 7 }) })
    await until(() => calls.length >= 40)
    await new Promise((r) => setTimeout(r, 100))
    assert.equal(ra.metrics().role, 'active')
    assert.equal(rb.metrics().role, 'standby')
    assert.equal(calls.length, 40)
    assert.deepEqual([...new Set(calls.map((c) => c.who))], ['a'])
    await ra.stop()
    await rb.stop()
  })

  it('when the active one stops, the standby takes over from the stored position', async () => {
    const b = fakeBroker()
    const qa = fakeQueen(b, 'a')
    const qb = fakeQueen(b, 'b')
    for (let i = 1; i <= 5; i++) b.push({ queue: 'orders', partition: 'c1', data: { n: i } })
    const copy = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] }).to(q.queue('copy'))
    const ra = await copy(qa).run({ queryId: 'copy', ...fast({ lockTtl: '2s' }) })
    const rb = await copy(qb).run({ queryId: 'copy', ...fast({ lockTtl: '2s' }) })
    await until(() => b.messages('copy').length >= 5)
    await ra.stop()
    for (let i = 6; i <= 8; i++) b.push({ queue: 'orders', partition: 'c1', data: { n: i } })
    await until(() => b.messages('copy').length >= 8, 6000, 'the standby to copy 6..8')
    await rb.stop()
    assert.deepEqual(payloads(b, 'copy').map((p) => p.n), [1, 2, 3, 4, 5, 6, 7, 8], 'each once, in order')
    assert.equal(rb.metrics().takeovers, 1)
  })

  it('a process that loses its lock cannot commit, and the other one takes over', async () => {
    const b = fakeBroker()
    const qa = fakeQueen(b, 'a')
    const qb = fakeQueen(b, 'b')
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    const copy = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] }).to(q.queue('copy'))
    const ra = await copy(qa).run({ queryId: 'cut', ...fast({ lockTtl: '1s' }), ...quiet() })
    const rb = await copy(qb).run({ queryId: 'cut', ...fast({ lockTtl: '1s' }), ...quiet() })
    await until(() => b.messages('copy').length >= 1)
    // a's process is cut off (a partition, or a long pause): its lock expires.
    qa.http.cut = true
    b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
    await until(() => b.messages('copy').length >= 2, 8000, 'b to take over')
    qa.http.cut = false
    b.push({ queue: 'orders', partition: 'c1', data: { n: 3 } })
    await until(() => b.messages('copy').length >= 3, 4000)
    await new Promise((r) => setTimeout(r, 200))
    assert.equal(ra.metrics().role, 'standby', 'a found its lock gone')
    assert.equal(rb.metrics().role, 'active')
    assert.deepEqual(payloads(b, 'copy').map((p) => p.n), [1, 2, 3])
    await ra.stop()
    await rb.stop()
  })

  it('exclusive: false runs both, and the position still decides who commits', async () => {
    const b = fakeBroker()
    const qa = fakeQueen(b, 'a')
    const qb = fakeQueen(b, 'b')
    for (let i = 1; i <= 30; i++) b.push({ queue: 'orders', partition: `c${i % 3}`, data: { n: i } })
    const copy = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] }).to(q.queue('copy'))
    const ra = await copy(qa).run({ queryId: 'both', ...fast({ exclusive: false, maxRecords: 4 }) })
    const rb = await copy(qb).run({ queryId: 'both', ...fast({ exclusive: false, maxRecords: 4 }) })
    await until(() => b.messages('copy').length >= 30)
    await new Promise((r) => setTimeout(r, 100))
    await ra.stop()
    await rb.stop()
    const ns = payloads(b, 'copy').map((p) => p.n)
    assert.equal(ns.length, 30)
    assert.equal(new Set(ns).size, 30, 'no message twice')
    assert.equal(ra.metrics().role, 'stopped')
  })
})

describe('feed streams: routing', () => {
  it('a function picks the queue per message, and every output commits with the position', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'o1', data: { id: 1, country: 'IT' } })
    b.push({ queue: 'orders', partition: 'o2', data: { id: 2, country: 'FR' } })
    b.push({ queue: 'orders', partition: 'o3', data: { id: 3, country: 'IT' } })
    b.push({ queue: 'orders', partition: 'o4', data: { id: 4 } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders*'] })
      .to((order) => (order.country ? `orders-${order.country}` : null))
      .run({ queryId: 'router', ...fast() })
    await until(() => b.messages('orders-IT').length >= 2 && b.messages('orders-FR').length >= 1)
    await new Promise((r) => setTimeout(r, 100))
    await runner.stop()
    assert.deepEqual(payloads(b, 'orders-IT').map((p) => p.id), [1, 3])
    assert.deepEqual(payloads(b, 'orders-FR').map((p) => p.id), [2])
    const commit = b.txns.find((t) => t.items.length > 0)
    assert.equal(commit.items.length, 3, 'one transaction for both queues')
    assert.ok(commit.kv.some((op) => op.key === 'router'), 'with the position')
    // orders* matches its own outputs, and it never reads them back.
    assert.equal(runner.metrics().entriesRead, 4)
    assert.equal(b.messages('orders-IT').length, 2)
  })

  it('a list sends each record to every queue; a function may answer a partition or several targets', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'o1', data: { id: 1, total: 500 } })
    b.push({ queue: 'orders', partition: 'o2', data: { id: 2, total: 5 } })
    const all = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .to([queen.queue('audit'), queen.queue('bi')])
      .run({ queryId: 'fanout', ...fast() })
    const picked = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .to((o) => [
        { queue: 'by-size', partition: o.total > 100 ? 'big' : 'small' },
        o.total > 100 ? queen.queue('review') : null
      ])
      .run({ queryId: 'picker', ...fast() })
    await until(() => b.messages('bi').length >= 2 && b.messages('by-size').length >= 2 && b.messages('review').length >= 1)
    await all.stop()
    await picked.stop()
    assert.deepEqual(payloads(b, 'audit').map((p) => p.id), [1, 2])
    assert.deepEqual(payloads(b, 'bi').map((p) => p.id), [1, 2])
    assert.deepEqual(b.messages('by-size').map((e) => [e.payload.id, e.partition]), [[1, 'big'], [2, 'small']])
    assert.deepEqual(payloads(b, 'review').map((p) => p.id), [1])
    assert.equal(b.messages('review')[0].partition, 'o1', 'the source partition by default')
  })

  it('a queue source refuses several queues and functions, and so do pattern and join', async () => {
    const qb = { name: 'src', pop: async () => [], ack: async () => {}, push: async () => {} }
    await assert.rejects(Stream.from(qb).to((v) => 'x').run({ queryId: 'r1' }), /Stream.fromFeed only/)
    await assert.rejects(Stream.from(qb).to([{ name: 'a' }, { name: 'b' }]).run({ queryId: 'r2' }), /Stream.fromFeed only/)
    await assert.rejects(
      Stream.from(qb).pattern({ start: () => true, then: () => true, within: '1m' }).to({ name: 'x' }).run({ queryId: 'r3' }),
      /Stream.fromFeed only/
    )
  })
})

describe('feed streams: windows on the feed clock', () => {
  it('a tumbling window counts per entity across queues and closes when the clock passes its end', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000); b.push({ queue: 'orders', partition: 'customer-1', data: { total: 10 } })
    at(b, 2000); b.push({ queue: 'payments', partition: 'customer-1', data: { total: 10 } })
    at(b, 3000); b.push({ queue: 'orders', partition: 'customer-2', data: { total: 7 } })
    const results = []
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders', 'payments'] })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1, sum: (v) => v.total, avg: (v) => v.total })
      .map((acc, ctx) => ({ ...acc, key: ctx.key, window: ctx.windowKey }))
      .to(queen.queue('per-minute'))
      .run({ queryId: 'per-minute', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 3)
    assert.equal(b.messages('per-minute').length, 0, 'open until the minute is over')
    at(b, 61000); b.push({ queue: 'orders', partition: 'customer-3', data: { total: 1 } })
    await until(() => b.messages('per-minute').length >= 2)
    await runner.stop()
    const out = b.messages('per-minute')
    assert.deepEqual(out.map((e) => e.payload), [
      { count: 2, sum: 20, avg: 10, key: 'customer-1', window: '2026-11-06T09:00:00.000Z' },
      { count: 1, sum: 7, avg: 7, key: 'customer-2', window: '2026-11-06T09:00:00.000Z' }
    ], 'no __avg_ running sums in the result')
    assert.deepEqual(out.map((e) => e.partition), ['customer-1', 'customer-2'], 'a result goes to its key\'s partition')
    for (const e of out) results.push(e)
    assert.ok(out[0].transactionId.startsWith(outputPrefix('per-minute')))
    const stored = b.kvKeys(POSITION_NAMESPACE, 'per-minute#snap#')
    assert.ok(stored.length >= 1, 'a stop writes a snapshot: the open window of customer-3 is in it')
    assert.ok(b.txns.every((t) => t.kv.length <= 64), 'every commit within the 64 KV ops of a transaction')
  })

  it('a window on a quiet feed closes on time: the runner moves the broker clock with a write', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 5000); b.push({ queue: 'orders', partition: 'c1', data: {} })
    at(b, 6000); b.push({ queue: 'orders', partition: 'c1', data: {} })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1 })
      .to(queen.queue('counts'))
      .run({ queryId: 'quiet', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 2)
    // Nothing more is written: only time passes.
    at(b, 59500)
    await new Promise((r) => setTimeout(r, 100))
    assert.equal(b.messages('counts').length, 0, 'not before its end')
    at(b, 60000)
    await until(() => b.messages('counts').length >= 1, 3000, 'the window to close with no message after it')
    await runner.stop()
    assert.deepEqual(payloads(b, 'counts'), [{ count: 2 }])
    assert.ok(runner.metrics().ticks >= 1)
  })

  it('a restart carries the open windows on from KV, without counting anything twice', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 0; i < 3; i++) b.push({ queue: 'orders', partition: 'c1', data: { n: i } })
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest' })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1 })
      .to(q.queue('counts'))
    const first = await chain(queen).run({ queryId: 'restart', ...fast({ now: () => b.nowMs }) })
    await until(() => first.metrics().entriesRead >= 3)
    await first.stop()
    at(b, 2000)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 3 } })
    const second = await chain(fakeQueen(b, 'second')).run({ queryId: 'restart', ...fast({ now: () => b.nowMs }) })
    await until(() => second.metrics().entriesRead >= 1)
    at(b, 61000)
    b.push({ queue: 'other', partition: 'x', data: {} })
    await until(() => b.messages('counts').length >= 1)
    await second.stop()
    assert.deepEqual(payloads(b, 'counts'), [{ count: 4 }])
  })

  it('sliding windows count each message in every window it falls in', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 10000); b.push({ queue: 'q', partition: 'k', data: {} })
    at(b, 40000); b.push({ queue: 'q', partition: 'k', data: {} })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['q'] })
      .windowSliding({ size: 60, slide: 30 })
      .aggregate({ count: () => 1 })
      .map((acc, ctx) => ({ start: ctx.windowKey.slice(11, 19), count: acc.count }))
      .to(queen.queue('slides'))
      .run({ queryId: 'slides', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 2)
    at(b, 200000); b.push({ queue: 'other', partition: 'x', data: {} })
    await until(() => b.messages('slides').length >= 3)
    await runner.stop()
    assert.deepEqual(payloads(b, 'slides'), [
      { start: '08:59:30', count: 1 },
      { start: '09:00:00', count: 2 },
      { start: '09:00:30', count: 1 }
    ])
  })

  it('a session closes when a message comes past the gap, and on the clock when none does', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 0); b.push({ queue: 'clicks', partition: 'u1', data: {} })
    at(b, 5000); b.push({ queue: 'clicks', partition: 'u1', data: {} })
    at(b, 30000); b.push({ queue: 'clicks', partition: 'u1', data: {} })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['clicks'] })
      .windowSession({ gap: 10 })
      .aggregate({ count: () => 1 })
      .map((acc, ctx) => ({ count: acc.count, from: ctx.windowStart - T0, to: ctx.windowEnd - T0 }))
      .to(queen.queue('sessions'))
      .run({ queryId: 'sessions', ...fast({ now: () => b.nowMs }) })
    await until(() => b.messages('sessions').length >= 1)
    assert.deepEqual(payloads(b, 'sessions'), [{ count: 2, from: 0, to: 15000 }])
    at(b, 41000)
    await until(() => b.messages('sessions').length >= 2, 3000, 'the second session to close on the clock')
    await runner.stop()
    assert.deepEqual(payloads(b, 'sessions')[1], { count: 1, from: 30000, to: 40000 })
  })

  it('cron windows follow the wall clock, and keyBy groups by any field', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 59000); b.push({ queue: 'sales', partition: 's1', data: { country: 'IT', amount: 5 } })
    at(b, 61000); b.push({ queue: 'sales', partition: 's2', data: { country: 'IT', amount: 7 } })
    at(b, 62000); b.push({ queue: 'sales', partition: 's3', data: { country: 'FR', amount: 1 } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['sales'] })
      .keyBy((m) => m.data.country)
      .windowCron({ every: 'minute' })
      .aggregate({ sum: (v) => v.amount })
      .map((acc, ctx) => ({ country: ctx.key, minute: ctx.windowKey.slice(11, 16), sum: acc.sum }))
      .to(queen.queue('per-country'))
      .run({ queryId: 'cron', ...fast({ now: () => b.nowMs }) })
    await until(() => b.messages('per-country').length >= 1)
    at(b, 125000); b.push({ queue: 'other', partition: 'x', data: {} })
    await until(() => b.messages('per-country').length >= 3)
    await runner.stop()
    assert.deepEqual(payloads(b, 'per-country'), [
      { country: 'IT', minute: '09:00', sum: 5 },
      { country: 'FR', minute: '09:01', sum: 1 },
      { country: 'IT', minute: '09:01', sum: 7 }
    ], 'windows that end together close in key order')
  })

  it('event-time windows close on the watermark and drop what comes too late', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const ev = (sec) => new Date(T0 + sec * 1000).toISOString()
    b.push({ queue: 'readings', partition: 'sensor', data: { at: ev(10), v: 1 } })
    b.push({ queue: 'readings', partition: 'sensor', data: { at: ev(50), v: 2 } })
    b.push({ queue: 'readings', partition: 'sensor', data: { at: ev(70), v: 3 } })
    b.push({ queue: 'readings', partition: 'sensor', data: { at: ev(20), v: 4 } })
    b.push({ queue: 'readings', partition: 'sensor', data: { at: ev(130), v: 5 } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['readings'] })
      .windowTumbling({ seconds: 60, eventTime: (m) => m.data.at, allowedLateness: 5 })
      .aggregate({ sum: (v) => v.v })
      .map((acc, ctx) => ({ minute: ctx.windowKey.slice(11, 16), sum: acc.sum }))
      .to(queen.queue('sums'))
      .run({ queryId: 'event-time', ...fast() })
    await until(() => b.messages('sums').length >= 2)
    await runner.stop()
    assert.deepEqual(payloads(b, 'sums'), [{ minute: '09:00', sum: 3 }, { minute: '09:01', sum: 3 }])
    assert.equal(runner.metrics().late, 1, 'the reading of 09:00:20 came after the watermark passed it')
  })

  it('a chain that no longer fits the stored state is refused; reset starts it over', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const first = await Stream.fromFeed(queen, { from: 'earliest' })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 }).to(queen.queue('counts'))
      .run({ queryId: 'chain', ...fast({ now: () => b.nowMs }) })
    await until(() => first.metrics().stateKeys >= 1)
    await first.stop()
    assert.ok(b.kvKeys(POSITION_NAMESPACE, 'chain#snap#').length >= 1, 'a stop writes a snapshot')
    const changed = () => Stream.fromFeed(queen, { from: 'earliest' })
      .windowTumbling({ seconds: 30 }).aggregate({ count: () => 1 }).to(queen.queue('counts'))
    await assert.rejects(changed().run({ queryId: 'chain', ...fast() }), (err) => err.code === 'feed_chain_changed')
    const again = await changed().run({ queryId: 'chain', ...fast({ reset: 'v2', now: () => b.nowMs }) })
    await until(() => again.metrics().entriesRead >= 1)
    await again.stop()
    const stored = b.kvGet(POSITION_NAMESPACE, 'chain').value
    assert.equal(stored.epoch, 1)
    assert.equal(stored.resetToken, 'v2')
    // The same reset string again carries on, it does not start over.
    const third = await changed().run({ queryId: 'chain', ...fast({ reset: 'v2', now: () => b.nowMs }) })
    await new Promise((r) => setTimeout(r, 80))
    await third.stop()
    assert.equal(b.kvGet(POSITION_NAMESPACE, 'chain').value.epoch, 1)
  })
})

describe('feed streams: patterns and joins', () => {
  const pattern = (queen, extra = {}) => Stream.fromFeed(queen, { from: 'earliest', queues: ['orders', 'payments'] })
    .pattern({
      start: (m) => m.queue === 'orders',
      then: (m) => m.queue === 'payments',
      within: '15m',
      onMatch: (order, payment) => ({ order: order.data.id, paid: payment.data.amount }),
      onTimeout: (order) => ({ order: order.data.id, remind: true }),
      ...extra
    })
    .to(queen.queue('followups'))

  it('an order then its payment within 15 minutes is a match, across two queues', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 0); b.push({ queue: 'orders', partition: 'order-1', data: { id: 1 } })
    at(b, 60000); b.push({ queue: 'payments', partition: 'order-1', data: { amount: 30 } })
    at(b, 70000); b.push({ queue: 'orders', partition: 'order-3', data: { id: 3 } })
    at(b, 970000); b.push({ queue: 'payments', partition: 'order-3', data: { amount: 5 } })
    at(b, 971000); b.push({ queue: 'payments', partition: 'order-9', data: { amount: 1 } })
    const runner = await pattern(queen).run({ queryId: 'paid', ...fast({ now: () => b.nowMs }) })
    await until(() => b.messages('followups').length >= 2)
    await new Promise((r) => setTimeout(r, 50))
    await runner.stop()
    assert.deepEqual(payloads(b, 'followups'), [{ order: 1, paid: 30 }, { order: 3, paid: 5 }],
      'exactly 15 minutes is still in time; a payment with no order waits for nothing')
    assert.equal(b.messages('followups')[0].partition, 'order-1')
  })

  it('an order with no payment in time sends the reminder at the deadline, on a quiet feed', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 0); b.push({ queue: 'orders', partition: 'order-2', data: { id: 2 } })
    const runner = await pattern(queen).run({ queryId: 'remind', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 1)
    at(b, 900000)
    await new Promise((r) => setTimeout(r, 100))
    assert.equal(b.messages('followups').length, 0, 'not at 15 minutes yet')
    at(b, 901000)
    await until(() => b.messages('followups').length >= 1, 3000, 'the reminder')
    // A payment after the deadline is no match any more.
    at(b, 950000); b.push({ queue: 'payments', partition: 'order-2', data: { amount: 9 } })
    await new Promise((r) => setTimeout(r, 100))
    await runner.stop()
    assert.deepEqual(payloads(b, 'followups'), [{ order: 2, remind: true }])
  })

  it("onRepeat: 'restart' counts the deadline from the latest start", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 0); b.push({ queue: 'orders', partition: 'cart-1', data: { id: 'a' } })
    at(b, 600000); b.push({ queue: 'orders', partition: 'cart-1', data: { id: 'b' } })
    const runner = await pattern(queen, { onRepeat: 'restart' }).run({ queryId: 'cart', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 2)
    at(b, 1000000)
    await new Promise((r) => setTimeout(r, 150))
    assert.equal(b.messages('followups').length, 0, 'the second start moved the deadline to 25 minutes')
    at(b, 1501000)
    await until(() => b.messages('followups').length >= 1)
    await runner.stop()
    assert.deepEqual(payloads(b, 'followups'), [{ order: 'b', remind: true }])
  })

  it('a join pairs two messages of an entity in either order, and times out the one left alone', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 0); b.push({ queue: 'shipments', partition: 'order-1', data: { carrier: 'DHL' } })
    at(b, 1000); b.push({ queue: 'orders', partition: 'order-1', data: { id: 1 } })
    at(b, 2000); b.push({ queue: 'orders', partition: 'order-2', data: { id: 2 } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders', 'shipments'] })
      .join({
        left: (m) => m.queue === 'orders',
        right: (m) => m.queue === 'shipments',
        within: '1h',
        select: (order, shipment) => ({ order: order.data.id, carrier: shipment.data.carrier }),
        onTimeout: (m, side) => ({ unshipped: m.data.id, side })
      })
      .to(queen.queue('joined'))
      .run({ queryId: 'join', ...fast({ now: () => b.nowMs }) })
    await until(() => b.messages('joined').length >= 1)
    at(b, 2000 + 3601000)
    await until(() => b.messages('joined').length >= 2, 3000, 'the timeout of order 2')
    await runner.stop()
    assert.deepEqual(payloads(b, 'joined'), [{ order: 1, carrier: 'DHL' }, { unshipped: 2, side: 'left' }])
  })

  it('after a flatMap, the key sees each item: one message can open several waits', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 0); b.push({ queue: 'sync', partition: 'conv-1', data: { changes: [{ messageId: 'm1' }, { messageId: 'm2' }, { other: 1 }] } })
    at(b, 10); b.push({ queue: 'delivered', partition: 'x', data: { message: { id: 'm2' } } })
    at(b, 15); b.push({ queue: 'other', partition: 'x', data: {} })
    at(b, 20); b.push({ queue: 'delivered', partition: 'x', data: { message: { id: 'm1' } } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['sync', 'delivered', 'other'] })
      .flatMap((m) => (m.queue === 'sync' ? m.data.changes.filter((c) => c.messageId) : [m.data]))
      .pattern({
        start: (m) => m.queue === 'sync',
        then: (m) => m.queue === 'delivered',
        // Asked only of sync and delivered messages: 'other' has no id at all.
        key: (m, v) => (m.queue === 'sync' ? v.messageId : v.message.id),
        within: '1m',
        onMatch: (s, d) => ({ id: d.data.message.id })
      })
      .to(queen.queue('ok'))
      .run({ queryId: 'flat-key', ...fast({ now: () => b.nowMs }) })
    await until(() => b.messages('ok').length >= 2)
    await runner.stop()
    assert.deepEqual(payloads(b, 'ok').map((p) => p.id).sort(), ['m1', 'm2'])
  })

  it('the entity can be a field instead of the partition', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 0); b.push({ queue: 'orders', partition: 'p-a', data: { id: 'X1' } })
    at(b, 1000); b.push({ queue: 'payments', partition: 'p-b', data: { order: 'X1', amount: 3 } })
    const runner = await pattern(queen, { key: (m) => m.data.id ?? m.data.order }).run({ queryId: 'by-field', ...fast({ now: () => b.nowMs }) })
    await until(() => b.messages('followups').length >= 1)
    await runner.stop()
    assert.deepEqual(payloads(b, 'followups'), [{ order: 'X1', paid: 3 }])
    assert.equal(b.messages('followups')[0].partition, 'X1')
  })
})

describe('feed streams: commits that go wrong', () => {
  it('a commit whose answer was lost is not written twice (stateless)', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    for (let i = 1; i <= 3; i++) b.push({ queue: 'orders', partition: 'c1', data: { n: i } })
    // The broker applies the commit, and the answer never comes back.
    b.injectNext({ when: 'after', error: Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }) })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .to(queen.queue('copy'))
      .run({ queryId: 'lost-answer', ...fast(), ...quiet() })
    await until(() => runner.metrics().conflicts >= 1, 4000, 'the retry to find the commit stored')
    b.push({ queue: 'orders', partition: 'c1', data: { n: 4 } })
    await until(() => b.messages('copy').length >= 4)
    await runner.stop()
    assert.deepEqual(payloads(b, 'copy').map((p) => p.n), [1, 2, 3, 4])
  })

  it('a commit whose answer was lost does not count twice (windows)', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 1; i <= 3; i++) b.push({ queue: 'orders', partition: 'c1', data: { n: i } })
    b.injectNext({ when: 'after', error: Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }) })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1 })
      .to(queen.queue('counts'))
      .run({ queryId: 'lost-window', ...fast({ now: () => b.nowMs }), ...quiet() })
    await until(() => runner.metrics().conflicts >= 1, 4000)
    at(b, 2000); b.push({ queue: 'orders', partition: 'c1', data: { n: 4 } })
    at(b, 61000); b.push({ queue: 'orders', partition: 'c2', data: { n: 5 } })
    await until(() => b.messages('counts').length >= 1)
    await runner.stop()
    assert.deepEqual(payloads(b, 'counts'), [{ count: 4 }])
  })

  it('a page with more outputs than maxPendingOutputs commits between positions', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    for (let i = 1; i <= 7; i++) b.push({ queue: 'orders', partition: `c${i}`, data: { n: i } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .to(queen.queue('copy'))
      .run({ queryId: 'split', ...fast({ maxPendingOutputs: 2 }) })
    await until(() => b.messages('copy').length >= 7)
    await runner.stop()
    assert.deepEqual(payloads(b, 'copy').map((p) => p.n), [1, 2, 3, 4, 5, 6, 7])
    const commits = b.txns.filter((t) => t.items.length > 0)
    assert.ok(commits.length >= 4, `${commits.length} commits`)
    assert.ok(commits.every((t) => t.items.length <= 2))
  })

  it('a process whose lock was taken can commit nothing more, and stands by', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .to(queen.queue('copy'))
      .run({ queryId: 'stolen', ...fast(), ...quiet() })
    await until(() => b.messages('copy').length >= 1)
    // The lock expired on the broker and somebody else holds it now.
    b.locks.set('queen-feed/stolen', { owner: 'someone-else', token: 9999, expiresAt: Date.now() + 60000 })
    b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
    await until(() => runner.metrics().role === 'standby', 3000)
    await new Promise((r) => setTimeout(r, 100))
    assert.deepEqual(payloads(b, 'copy').map((p) => p.n), [1], 'n=2 is the new holder\'s to copy')
    await runner.stop()
  })
})
