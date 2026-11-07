// .foreachBatch(fn, { maxItems, maxWaitMillis }) on a feed stream: the results
// in batches, and the position saved only once fn returns. Each test runs the
// client's own runner against the broker in memory of fakeBroker.js.
import { describe, it, afterEach } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { feedOutputId, outputPrefix, POSITION_NAMESPACE as NS } from '../../client-v2/streams/runtime/FeedRunner.js'
import { fakeBroker, fakeQueen, until } from './fakeBroker.js'

const T0 = Date.parse('2026-11-06T09:00:00.000Z')
const fast = (o = {}) => ({ waitMillis: 20, commitEveryMillis: 0, retryDelayMillis: 1, ...o })
const at = (b, ms) => { b.nowMs = T0 + ms }
const quiet = (o = {}) => ({ onError: () => {}, ...o })
const wait = (ms) => new Promise((r) => setTimeout(r, ms))
// A process that dies: its client stops answering, so it can neither commit nor keep its lock.
const crash = (q) => { q.http.cut = true }
const stored = (b, id) => b.kvGet(NS, id)?.value ?? null
const orders = (b, from, to) => {
  for (let i = from; i <= to; i++) b.push({ queue: 'orders', partition: `c${i}`, data: { n: i } })
}
const ns = (items) => items.map((it) => it.value.n)
// The record's position is the next one to read: past `e` once e is committed. (The lock and
// each commit take positions of their own, so the positions are read from the log.)
const past = (b, id, e) => (stored(b, id)?.position ?? 0) > e.position
const lastOrder = (b) => b.messages('orders').at(-1)

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

/** A batch function that records each call, and runs `then(items, call)` (to throw, wait, crash). */
function recorder(then = null) {
  const calls = []
  const fn = async (items) => {
    calls.push(items)
    if (then) await then(items, calls.length)
  }
  return { calls, fn }
}

describe('foreachBatch: batches and the position', () => {
  it('one batch per read: each item has the value, the ctx foreach gets, and the id of the output; the position moves after fn returns', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 5)
    let release
    const gate = new Promise((r) => { release = r })
    let seenWhileOpen = 'unset'
    const rec = recorder(async () => {
      seenWhileOpen = stored(b, 'per-read')
      await gate
    })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(rec.fn)
      .run({ queryId: 'per-read', ...fast() })
    await until(() => rec.calls.length === 1)
    await wait(50)
    assert.equal(seenWhileOpen, null, 'nothing committed while fn runs')
    assert.equal(stored(b, 'per-read'), null, 'still nothing committed')
    release()
    await until(() => past(b, 'per-read', lastOrder(b)))
    const [items] = rec.calls
    assert.deepEqual(ns(items), [1, 2, 3, 4, 5], 'the whole read, in log order')
    const msgs = b.messages('orders')
    items.forEach((it, i) => {
      const e = msgs[i]
      assert.deepEqual(Object.keys(it).sort(), ['ctx', 'id', 'value'])
      assert.deepEqual(it.ctx, { position: e.position, queue: 'orders', partition: e.partition, partitionId: e.partitionId, offset: e.offset, transactionId: e.transactionId })
      assert.equal(it.id, feedOutputId('per-read', e, 0, 0, 'per-read'), 'the id a .to() output of it would have')
    })
    const m = runner.metrics()
    assert.equal(m.batches, 1)
    assert.equal(m.effects, 5, 'effects count the items delivered')
    assert.equal(m.emitted, 5)
    assert.equal(m.pendingBatch, 0)
    assert.equal(rec.calls.length, 1)
  })

  it('maxItems: a read of 7 goes in calls of 3, 3 and 1, each followed by its commit', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 7)
    const positions = []
    const rec = recorder(() => { positions.push(stored(b, 'max-items')?.position ?? null) })
    await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(rec.fn, { maxItems: 3 })
      .run({ queryId: 'max-items', ...fast() })
    await until(() => past(b, 'max-items', lastOrder(b)))
    assert.deepEqual(rec.calls.map(ns), [[1, 2, 3], [4, 5, 6], [7]])
    const msgs = b.messages('orders')
    assert.deepEqual(positions, [null, msgs[3].position, msgs[6].position], 'each call sees the commit of the one before, and no more')
  })

  it('maxItems also splits what one message makes (a flatMap): every call holds maxItems at most, the commit comes after the last', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 5 } })
    const positions = []
    const rec = recorder(() => { positions.push(stored(b, 'split-one')?.position ?? null) })
    await Stream.fromFeed(queen, { from: 'earliest' })
      .flatMap((m) => Array.from({ length: m.data.n }, (_, i) => ({ n: i })))
      .foreachBatch(rec.fn, { maxItems: 2 })
      .run({ queryId: 'split-one', ...fast() })
    await until(() => past(b, 'split-one', lastOrder(b)))
    assert.deepEqual(rec.calls.map(ns), [[0, 1], [2, 3], [4]])
    assert.deepEqual(positions, [null, null, null], 'one message is one position: it commits once, after its last item')
    const e = b.messages('orders')[0]
    assert.deepEqual(rec.calls.flat().map((it) => it.id), [0, 1, 2, 3, 4].map((i) => feedOutputId('split-one', e, i, 0, 'split-one')))
  })

  it('maxWaitMillis: one batch across reads, nothing committed while it waits; full, it goes at once', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 0)
    orders(b, 1, 2)
    const rec = recorder()
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(rec.fn, { maxItems: 5, maxWaitMillis: 1000 })
      .run({ queryId: 'linger', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().pendingBatch === 2)
    at(b, 400)
    orders(b, 3, 3)
    await until(() => runner.metrics().pendingBatch === 3)
    await wait(80)
    assert.equal(rec.calls.length, 0, 'it waits for more')
    assert.equal(stored(b, 'linger'), null, 'and nothing commits past the items that wait')
    at(b, 1000)
    await until(() => rec.calls.length === 1)
    assert.deepEqual(ns(rec.calls[0]), [1, 2, 3], 'one batch of the two reads')
    await until(() => past(b, 'linger', lastOrder(b)))
    // Five at once fill the batch: it goes without waiting.
    orders(b, 4, 8)
    await until(() => rec.calls.length === 2)
    assert.deepEqual(ns(rec.calls[1]), [4, 5, 6, 7, 8])
    await until(() => past(b, 'linger', lastOrder(b)))
  })
})

describe('foreachBatch: stops and crashes', () => {
  it('stop() delivers the batch that waits, then commits with the stop mark', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 3)
    const rec = recorder()
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(rec.fn, { maxWaitMillis: 3600000 })
      .run({ queryId: 'stop-flush', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().pendingBatch === 3)
    assert.equal(rec.calls.length, 0)
    await runner.stop()
    assert.deepEqual(rec.calls.map(ns), [[1, 2, 3]])
    const rec2 = stored(b, 'stop-flush')
    assert.ok(past(b, 'stop-flush', lastOrder(b)), 'committed past the batch')
    assert.equal(rec2.stopped.reason, 'stop')
  })

  it('a batch that fails at stop() is tried once and left for the next start, which gets the same items', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 2)
    const first = recorder((items) => { if (items[0].value.n === 3) throw new Error('down') })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(first.fn, { maxWaitMillis: 3600000, maxItems: 2 })
      .run({ queryId: 'stop-fail', ...fast({ now: () => b.nowMs }), ...quiet({ retries: 3 }) })
    await until(() => past(b, 'stop-fail', lastOrder(b)))
    orders(b, 3, 3)
    await until(() => runner.metrics().pendingBatch === 1)
    await runner.stop()
    assert.deepEqual(first.calls.map(ns), [[1, 2], [3]], 'one try at the stop: no retries')
    assert.ok(!past(b, 'stop-fail', lastOrder(b)), 'the item is not committed')
    assert.equal(stored(b, 'stop-fail').stopped.reason, 'stop')
    const again = recorder()
    const second = await Stream.fromFeed(fakeQueen(b, 'again'), { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(again.fn)
      .run({ queryId: 'stop-fail', ...fast() })
    await until(() => again.calls.length === 1)
    assert.deepEqual(ns(again.calls[0]), [3])
    assert.equal(again.calls[0][0].id, first.calls[1][0].id, 'the same id: an upsert by it writes once')
    await second.stop()
  })

  it('a crash before fn returns delivers the batch again with the same ids; a replay delivers nothing', async () => {
    const b = fakeBroker()
    const qa = fakeQueen(b, 'a')
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    b.push({ queue: 'orders', partition: 'c2', data: {} })
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const perMinute = (q, rec) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1 })
      .foreachBatch(rec.fn)
    // A delivers minute 0 and commits it; minute 1 closes in a batch it never commits.
    let cut = false
    const a = recorder((items) => {
      if (items.some((it) => it.ctx.windowStart === T0 + 60000)) { crash(qa); cut = true }
    })
    const opts = { ...fast({ now: () => b.nowMs, lockTtl: '1s', snapshotEveryMillis: 3600000 }), ...quiet() }
    await perMinute(qa, a).run({ queryId: 'batch-crash', ...opts })
    await until(() => a.calls.length === 1 && past(b, 'batch-crash', lastOrder(b)))
    assert.deepEqual(a.calls[0].map((it) => [it.ctx.key, it.value.count]), [['c1', 1], ['c2', 1]], 'minute 0, one item per key')
    at(b, 121000)
    b.push({ queue: 'orders', partition: 'c3', data: {} })
    await until(() => cut)
    const lost = a.calls[1]
    assert.deepEqual(lost.map((it) => [it.ctx.key, it.value.count]), [['c1', 1]])
    const bb = recorder()
    const second = await perMinute(fakeQueen(b, 'b'), bb).run({ queryId: 'batch-crash', ...opts })
    await until(() => second.metrics().role === 'active', 6000, 'the takeover')
    await until(() => bb.calls.length >= 1)
    assert.ok(second.metrics().replays >= 1, 'it read the feed again from the snapshot')
    assert.deepEqual(bb.calls[0].map((it) => [it.ctx.key, it.value.count]), [['c1', 1]], 'minute 1 again, and minute 0 not: the replay sends nothing')
    assert.deepEqual(bb.calls[0].map((it) => it.id), lost.map((it) => it.id), 'the same ids as the batch that never committed')
    assert.ok(lost.every((it) => it.id.startsWith(outputPrefix('batch-crash'))))
    await until(() => past(b, 'batch-crash', lastOrder(b)))
  })
})

describe('foreachBatch: failures and the poison policy', () => {
  it('a batch that fails, then passes, is tried again whole: not poison', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 3)
    const rec = recorder((items, call) => { if (call <= 2) throw new Error('flaky') })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(rec.fn)
      .run({ queryId: 'batch-flaky', ...fast() })
    await until(() => past(b, 'batch-flaky', lastOrder(b)))
    assert.deepEqual(rec.calls.map(ns), [[1, 2, 3], [1, 2, 3], [1, 2, 3]])
    assert.deepEqual(rec.calls[2].map((it) => it.id), rec.calls[0].map((it) => it.id))
    const m = runner.metrics()
    assert.equal(m.retries, 2)
    assert.equal(m.poisoned, 0)
    assert.equal(m.batches, 1)
    assert.equal(m.effects, 3)
  })

  it("'skip': tried whole, then item by item; the item that fails is left out, the others are delivered", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 4)
    const errors = []
    const rec = recorder((items) => { if (items.some((it) => it.value.n === 3)) throw new Error('bad row') })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(rec.fn)
      .run({ queryId: 'batch-skip', ...fast(), onPoison: 'skip', retries: 1, onError: (err, ctx) => errors.push({ err, ctx }) })
    await until(() => past(b, 'batch-skip', lastOrder(b)))
    assert.deepEqual(rec.calls.map(ns), [[1, 2, 3, 4], [1, 2, 3, 4], [1], [2], [3], [3], [4]])
    const m = runner.metrics()
    assert.equal(m.skipped, 1)
    assert.equal(m.poisoned, 1)
    assert.equal(m.effects, 3, 'items 1, 2 and 4')
    assert.equal(m.batches, 3)
    const third = b.messages('orders')[2]
    const skip = errors.find((e) => e.ctx.action === 'skipped')
    assert.ok(skip)
    assert.equal(skip.ctx.phase, 'poison')
    assert.equal(skip.ctx.position, third.position)
    assert.equal(skip.ctx.partition, 'c3')
    assert.match(skip.err.message, /an item of the message at position \d+ \(orders\/c3, offset 1\) failed 2 times in foreachBatch and was skipped: bad row/)
  })

  it('a dead-letter queue gets the item, in the commit that moves the position past the batch', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 3)
    const rec = recorder((items) => { if (items.some((it) => it.value.n === 2)) throw new TypeError('no price') })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(rec.fn)
      .run({ queryId: 'batch-dlq', ...fast(), ...quiet({ onPoison: queen.queue('batch-dlq-q'), retries: 0 }) })
    await until(() => past(b, 'batch-dlq', lastOrder(b)))
    assert.deepEqual(rec.calls.map(ns), [[1, 2, 3], [1], [2], [3]])
    const [dead] = b.messages('batch-dlq-q')
    assert.ok(dead, 'dead-lettered')
    assert.equal(dead.partition, 'c2', 'in the partition it came from')
    const second = b.messages('orders')[1]
    assert.deepEqual(dead.payload, {
      queryId: 'batch-dlq',
      attempts: 1,
      error: { name: 'TypeError', message: 'no price' },
      item: { value: { n: 2 }, ctx: rec.calls[0][1].ctx, id: feedOutputId('batch-dlq', second, 0, 0, 'batch-dlq') }
    })
    assert.ok(dead.transactionId.startsWith(outputPrefix('batch-dlq')), 'the query never reads it back')
    const commit = b.txns.find((t) => t.items.some((it) => it.queue === 'batch-dlq-q'))
    assert.ok(commit.kv.some((op) => op.key === 'batch-dlq' && op.value?.position > lastOrder(b).position), 'with the position past the batch, in one transaction')
    assert.equal(runner.metrics().deadLettered, 1)
  })

  it("'stop': nothing of the batch commits; the record names the item, and a restart gets the whole batch again", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 2)
    const errors = []
    const rec = recorder((items) => { if (items.some((it) => it.value.n === 4)) throw new Error('bad row') })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(rec.fn)
      .run({ queryId: 'batch-stop', ...fast(), retries: 1, onError: (err, ctx) => errors.push({ err, ctx }) })
    await until(() => past(b, 'batch-stop', lastOrder(b)))
    const before = stored(b, 'batch-stop').position
    orders(b, 3, 5)
    await runner.done
    assert.deepEqual(rec.calls.map(ns), [[1, 2], [3, 4, 5], [3, 4, 5], [3], [4], [4]], 'whole twice, then one by one up to the item')
    const fourth = b.messages('orders')[3]
    const rec2 = stored(b, 'batch-stop')
    assert.equal(rec2.position, before, 'the last commit: before the batch')
    assert.equal(rec2.stopped.reason, 'poison')
    assert.equal(rec2.stopped.position, fourth.position, 'the item that failed')
    assert.equal(rec2.stopped.partition, 'c4')
    const m = runner.metrics()
    assert.equal(m.role, 'stopped')
    assert.equal(m.stopped.reason, 'poison')
    assert.equal(m.stopped.position, fourth.position)
    const stop = errors.find((e) => e.ctx.action === 'stopped')
    assert.equal(stop.err.code, 'feed_poison')
    assert.match(stop.err.message, /an item of the message at position \d+ \(orders\/c4, offset 1\) failed 2 times in foreachBatch: bad row\. The stream stopped before its batch/)
    const again = recorder()
    await Stream.fromFeed(fakeQueen(b, 'fixed'), { from: 'earliest' })
      .map((m) => ({ n: m.data.n }))
      .foreachBatch(again.fn)
      .run({ queryId: 'batch-stop', ...fast() })
    await until(() => past(b, 'batch-stop', lastOrder(b)))
    assert.deepEqual(again.calls.map(ns), [[3, 4, 5]])
    assert.deepEqual(again.calls[0].map((it) => it.id), rec.calls[1].map((it) => it.id))
  })

  it("a message that stops the stream ('stop') comes after the batch that waits: the batch goes first, the commit stops at the message", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 4)
    const rec = recorder()
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => { if (m.data.n === 3) throw new Error('bad order'); return { n: m.data.n } })
      .foreachBatch(rec.fn)
      .run({ queryId: 'msg-stop', ...fast(), ...quiet() })
    await runner.done
    assert.deepEqual(rec.calls.map(ns), [[1, 2]])
    const third = b.messages('orders')[2]
    const r = stored(b, 'msg-stop')
    assert.equal(r.position, third.position, 'stopped at the message')
    assert.equal(r.stopped.reason, 'poison')
    assert.equal(runner.metrics().effects, 2)
  })
})

describe('foreachBatch: windows and closes', () => {
  it('nothing commits while a window result waits in the batch, not even a tick for the window due next; the commit after fn moves the clock', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'c2', data: {} })
    const rec = recorder()
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1 })
      .foreachBatch(rec.fn, { maxWaitMillis: 100000 })
      .run({ queryId: 'batch-tick', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().pendingBatch === 1)
    const commits = runner.metrics().commits
    // Minute 1 is due by now on this clock: a stream without a waiting batch would tick.
    at(b, 125000)
    await wait(150)
    assert.equal(runner.metrics().ticks, 0, 'no tick while an item waits')
    assert.equal(runner.metrics().commits, commits, 'no commit')
    assert.equal(rec.calls.length, 0)
    at(b, 161000)
    await until(() => rec.calls.length === 1)
    assert.deepEqual(rec.calls[0].map((it) => [it.ctx.key, it.value.count]), [['c1', 1]])
    // The commit after it moved the broker's clock: minute 1 closes into the next batch.
    await until(() => runner.metrics().pendingBatch === 1)
    at(b, 262000)
    await until(() => rec.calls.length === 2)
    assert.deepEqual(rec.calls[1].map((it) => [it.ctx.key, it.value.count]), [['c2', 1]])
    assert.ok(rec.calls.flat().every((it) => it.id.startsWith(outputPrefix('batch-tick')) && it.id.length === outputPrefix('batch-tick').length + 40))
  })

  it('a close of many windows at once goes in batches of maxItems, with commits in the middle of the close', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    const parts = 2500
    for (let i = 0; i < parts; i++) b.push({ queue: 'orders', partition: `p${i}`, data: {} })
    const rec = recorder()
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1 })
      .foreachBatch(rec.fn, { maxItems: 1000 })
      .run({ queryId: 'batch-mass', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= parts && runner.metrics().pendingBatch === 0 && runner.metrics().commits >= 1, 10000)
    at(b, 61000)
    b.push({ queue: 'other', partition: 'x', data: {} })
    await until(() => rec.calls.flat().length >= parts, 10000)
    assert.deepEqual(rec.calls.map((c) => c.length), [1000, 1000, 500])
    assert.ok(runner.metrics().closeCommits >= 1, 'a commit in the middle of the close')
    assert.equal(new Set(rec.calls.flat().map((it) => it.id)).size, parts, 'every result its own id')
  })
})

describe('foreachBatch: the chain', () => {
  it('runs on Stream.fromFeed only', async () => {
    const queen = { queue: (n) => ({ name: n }) }
    const s = Stream.from(queen.queue('orders')).foreachBatch(async () => {})
    await assert.rejects(() => s.run({ queryId: 'q' }), /\.foreachBatch\(\) runs on Stream\.fromFeed only/)
  })

  it('checks its function and options', async () => {
    const queen = fakeQueen(fakeBroker())
    const s = Stream.fromFeed(queen, {})
    assert.throws(() => s.foreachBatch('no'), /fn is a function/)
    assert.throws(() => s.foreachBatch(() => {}, { maxItems: 0 }), /maxItems is a whole number from 1/)
    assert.throws(() => s.foreachBatch(() => {}, { maxItems: 1.5 }), /maxItems is a whole number from 1/)
    assert.throws(() => s.foreachBatch(() => {}, { maxWaitMillis: -1 }), /maxWaitMillis is a whole number from 0/)
    assert.throws(() => s.foreachBatch(() => {}, { maxWait: 10 }), /unknown option "maxWait"/)
    await assert.rejects(() => s.foreachBatch(() => {}).map((x) => x).run({ queryId: 'x' }), /must be the last in the chain/)
  })

  it('the description of the stream names the sink and its options', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1, 1)
    await Stream.fromFeed(queen, { from: 'earliest' })
      .foreachBatch(async () => {}, { maxItems: 50, maxWaitMillis: 250 })
      .run({ queryId: 'batch-about', ...fast() })
    await until(() => b.kvGet(NS, 'batch-about#about'))
    assert.deepEqual(b.kvGet(NS, 'batch-about#about').value.sink, { kind: 'foreachBatch', maxItems: 50, maxWaitMillis: 250 })
  })
})
