// What a feed stream leaves in KV for a reader outside its process (the
// console's Streams page): a description of the stream (`<queryId>#about`),
// and in the position record the feed time it read up to, whether it was
// caught up, a heartbeat on a quiet feed, what each step holds, and why it
// stopped. Same broker in memory as the other feed tests (fakeBroker.js).
import { describe, it, afterEach } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { POSITION_NAMESPACE as NS } from '../../client-v2/streams/runtime/FeedRunner.js'
import { fakeBroker, fakeQueen, until } from './fakeBroker.js'

const T0 = Date.parse('2026-11-06T09:00:00.000Z')
const fast = (o = {}) => ({ waitMillis: 20, commitEveryMillis: 0, retryDelayMillis: 1, ...o })
const at = (b, ms) => { b.nowMs = T0 + ms }
const quiet = (o = {}) => ({ onError: () => {}, ...o })
const wait = (ms) => new Promise((r) => setTimeout(r, ms))
const record = (b, id) => b.kvGet(NS, id)?.value

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

describe('feed records: the description', () => {
  const chain = (queen, seconds = 60) => Stream.fromFeed(queen, { from: 'earliest', queues: ['orders', 'pay*'] })
    .where('data.country', '=', 'IT')
    .map((m) => m.data)
    .windowTumbling({ seconds })
    .aggregate({ count: () => 1, sum: (v) => v.total })
    .name('perCustomer')
    .to(queen.queue('totals'))

  it('says what the stream reads, its steps, where results go and what a failing message does', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { country: 'IT', total: 1 } })
    await chain(queen).run({ queryId: 'about', ...fast(), onPoison: queen.queue('feed-dlq') })
    await until(() => record(b, 'about#about'), 3000, 'the first commit')
    const about = record(b, 'about#about')
    assert.equal(about.v, 1)
    assert.equal(about.queryId, 'about')
    assert.match(about.client, /^client-js \d+\.\d+\.\d+/)
    assert.match(about.hash, /^[0-9a-f]{16}$/)
    assert.ok(b.txns.some((t) => t.kv.some((op) => op.key === 'about#about') && t.kv.some((op) => op.key === 'about')),
      'in a commit of the position')
    assert.deepEqual(about.reads, {
      queues: ['orders', 'pay*'],
      partitions: null,
      where: [{ field: 'data.country', op: '=', value: 'IT' }]
    }, 'a where before the first stateful step is the broker\'s filter')
    assert.deepEqual(about.steps.map((s) => s.kind), ['ops', 'window'])
    assert.deepEqual(about.steps[0], { kind: 'ops', ops: ['map'] })
    const w = about.steps[1]
    assert.equal(w.name, 'perCustomer')
    assert.equal(w.clock, 'feed')
    assert.equal(w.window.kind, 'window-tumbling')
    assert.equal(w.window.seconds, 60)
    assert.deepEqual(w.reducer, { kind: 'aggregate', fields: ['count', 'sum'] })
    assert.deepEqual(about.sink, { kind: 'queues', queues: ['totals'] })
    assert.deepEqual(about.onPoison, { queue: 'feed-dlq' })
    assert.equal(about.shards, null)
    assert.equal(about.exclusive, true)
  })

  it('is written once per activation; the next deploy\'s chain replaces it', async () => {
    const b = fakeBroker()
    b.push({ queue: 'orders', partition: 'c1', data: { country: 'IT', total: 1 } })
    const first = await chain(fakeQueen(b, 'a')).run({ queryId: 'again', ...fast() })
    await until(() => record(b, 'again#about'), 3000, 'the first commit')
    const v1 = b.kvGet(NS, 'again#about').version
    b.push({ queue: 'orders', partition: 'c2', data: { country: 'IT', total: 1 } })
    await until(() => b.kvGet(NS, 'again').value.position > b.messages('orders')[1].position)
    assert.equal(b.kvGet(NS, 'again#about').version, v1, 'not at every commit')
    await first.stop()
    await chain(fakeQueen(b, 'c'), 10).run({ queryId: 'again', ...fast({ reset: 'v2' }) })
    await until(() => b.kvGet(NS, 'again#about').version > v1, 3000, 'the new deploy\'s first commit')
    assert.equal(record(b, 'again#about').steps[1].window.seconds, 10)
  })

  it('names a pattern, a gather, a table and their clocks; a function sink is a route; shards are one record', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { at: new Date(T0).toISOString() } })
    await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders', 'payments'] })
      .joinTable({ queue: 'customers', on: (m) => m.partition, select: (v, row) => ({ ...v, tier: row?.tier ?? null }) })
      .name('customers')
      .pattern({
        start: (m) => m.queue === 'orders',
        then: (m) => m.queue === 'payments',
        within: '15m',
        eventTime: (m) => m.data.at,
        onTimeout: (s) => ({ unpaid: s.partition })
      })
      .name('unpaid')
      .gather({ steps: { unpaid: (m) => m.data.unpaid !== undefined }, key: (m) => m.data.unpaid, within: '1h', eventTime: false })
      .name('alerts')
      .to((v) => (v.unpaid ? 'alerts' : null))
      .run({ queryId: 'kinds', ...fast(), shards: 2, onPoison: 'skip' })
    await until(() => record(b, 'kinds#about'), 3000, 'a shard\'s first commit')
    const about = record(b, 'kinds#about')
    assert.deepEqual(about.steps.map((s) => [s.kind, s.name]), [['table', 'customers'], ['pattern', 'unpaid'], ['gather', 'alerts']])
    assert.deepEqual(about.steps[0].config.queues, ['customers'])
    assert.equal(about.steps[0].clock, undefined, 'a table runs on no clock')
    assert.equal(about.steps[1].clock, 'event')
    assert.equal(about.steps[1].config.within, 15 * 60000)
    assert.equal(about.steps[2].clock, 'feed', 'eventTime: false, after a step on event time')
    assert.deepEqual(about.steps[2].config.parts, ['unpaid'])
    assert.deepEqual(about.reads.tables, ['customers'])
    assert.deepEqual(about.sink, { kind: 'route' })
    assert.equal(about.onPoison, 'skip')
    assert.deepEqual(about.shards, { count: 2, by: 'partition' })
    assert.deepEqual(b.kvKeys(NS, 'kinds#about'), ['kinds#about'], 'one description for every shard')
  })
})

describe('feed records: the position record', () => {
  it('says the feed time it read up to, and that it was caught up', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000); b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    at(b, 2000); b.push({ queue: 'orders', partition: 'c2', data: { n: 2 } })
    const last = b.messages('orders')[1]
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => m.data)
      .to(queen.queue('out'))
      .run({ queryId: 'read-to', ...fast({ now: () => b.nowMs }) })
    await until(() => b.messages('out').length >= 2 && record(b, 'read-to')?.caughtUp === true)
    const r = record(b, 'read-to')
    assert.ok(r.readTo >= Date.parse(last.ts || last.createdAt), 'at or past the last message read')
    assert.equal(r.caughtUp, true)
    assert.equal(r.open, undefined, 'a chain with no state counts nothing')
    assert.equal(r.stopped, undefined)
    await runner.stop()
  })

  it('a quiet, caught-up stream keeps its record fresh: its own writes move the log, the position follows', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000); b.push({ queue: 'orders', partition: 'c1', data: {} })
    await Stream.fromFeed(queen, { from: 'earliest' })
      .to(queen.queue('out'))
      .run({ queryId: 'fresh', ...fast({ now: () => b.nowMs, positionCommitMillis: 5000 }) })
    await until(() => b.messages('out').length >= 1 && record(b, 'fresh')?.caughtUp === true)
    const v = b.kvGet(NS, 'fresh').version
    at(b, 3000)
    await wait(100)
    assert.equal(b.kvGet(NS, 'fresh').version, v, 'not before positionCommitMillis')
    at(b, 7000)
    await until(() => b.kvGet(NS, 'fresh').version > v, 3000, 'the next position commit')
    const r = record(b, 'fresh')
    assert.equal(r.caughtUp, true)
    assert.equal(r.updatedAt, new Date(T0 + 7000).toISOString())
    // The broker's clock moves only with writes: a quiet read resolves to the
    // last write's time, so readTo can trail the wall clock; caughtUp says it is not behind.
    assert.ok(r.readTo >= T0 + 1000 && r.readTo <= T0 + 7000, `readTo ${r.readTo - T0} ms`)
    assert.equal(record(b, 'fresh#about').positionCommitMillis, 5000, 'the description says how often to expect it')
  })

  it('counts what each step holds: open windows, and messages an event-time step holds', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const ev = (min) => new Date(Date.parse('2025-10-01T10:00:00.000Z') + min * 60000).toISOString()
    at(b, 1000)
    for (const c of ['c1', 'c2', 'c3']) b.push({ queue: 'orders', partition: c, data: { total: 1, at: ev(0) } })
    b.push({ queue: 'orders', partition: 'c1', data: { total: 1, at: ev(30) } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .windowTumbling({ seconds: 60 })
      .aggregate({ count: () => 1 })
      .name('perMinute')
      .pattern({
        start: (m) => true,
        then: (m) => false,
        within: '1h',
        eventTime: () => ev(0),
        allowedLateness: '1h',
        onTimeout: (s) => s
      })
      .name('later')
      .to(queen.queue('out'))
      .run({ queryId: 'counts', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 4)
    await until(() => record(b, 'counts')?.open?.perMinute === 3)
    const r = record(b, 'counts')
    assert.deepEqual(Object.keys(r.open), ['perMinute', 'later'])
    assert.equal(r.held, undefined, 'nothing reached the event-time step yet: no window closed')
  })

  it('counts the messages an event-time step holds for their turn', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const ev = (min) => new Date(Date.parse('2025-10-01T10:00:00.000Z') + min * 60000).toISOString()
    at(b, 1000)
    b.push({ queue: 'issues', partition: 'r1', data: { n: 1, at: ev(0) } })
    b.push({ queue: 'issues', partition: 'r1', data: { n: 2, at: ev(30) } })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['issues', 'comments'] })
      .pattern({
        start: (m) => m.queue === 'issues',
        then: (m) => m.queue === 'comments',
        key: (m) => `${m.partition}#${m.data.n}`,
        within: '1h',
        eventTime: (m) => m.data.at,
        allowedLateness: '20m',
        onTimeout: (s) => ({ unanswered: s.data.n })
      })
      .name('unanswered')
      .to(queen.queue('out'))
      .run({ queryId: 'held', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().steps[0].held === 1)
    await until(() => record(b, 'held')?.held?.unanswered === 1, 3000, 'the commit after it')
    const r = record(b, 'held')
    assert.equal(r.open.unanswered, 1, 'issue 1 waits for its answer; issue 2 waits for the watermark')
  })
})

describe('feed records: why it stopped', () => {
  const orders = (b, n) => {
    for (let i = 1; i <= n; i++) b.push({ queue: 'orders', partition: `c${i}`, data: { n: i } })
  }

  it('a message that keeps failing: the reason, the attempts and the message, in the commit of the others', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 4)
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map((m) => { if (m.data.n === 3) throw new Error('bad order'); return m.data })
      .to(queen.queue('out'))
      .run({ queryId: 'poison', ...fast({ maxRecords: 2 }), ...quiet() })
    await runner.done
    const third = b.messages('orders')[2]
    const r = record(b, 'poison')
    assert.equal(r.position, third.position)
    assert.equal(r.stopped.reason, 'poison')
    assert.equal(r.stopped.error, 'bad order')
    assert.equal(r.stopped.attempts, 4)
    assert.equal(r.stopped.position, third.position)
    assert.equal(r.stopped.queue, 'orders')
    assert.equal(r.stopped.partition, 'c3')
    assert.equal(r.stopped.offset, third.offset)
    assert.ok(Date.parse(r.stopped.at) > 0)
  })

  it('a message that fails first, with nothing else to commit: the record still says so', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 2)
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map(() => { throw new Error('no parser') })
      .to(queen.queue('out'))
      .run({ queryId: 'first', ...fast(), ...quiet() })
    await runner.done
    const r = record(b, 'first')
    assert.equal(r.position, b.messages('orders')[0].position)
    assert.equal(r.stopped.reason, 'poison')
    assert.equal(r.stopped.error, 'no parser')
  })

  it('a long error is cut', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    orders(b, 1)
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .map(() => { throw new Error('x'.repeat(5000)) })
      .to(queen.queue('out'))
      .run({ queryId: 'long', ...fast(), ...quiet() })
    await runner.done
    assert.ok(record(b, 'long').stopped.error.length <= 501)
  })

  it('a stop asked by its process says so, and the next commit of a new run drops it', async () => {
    const b = fakeBroker()
    orders(b, 2)
    const qa = fakeQueen(b, 'a')
    const first = await Stream.fromFeed(qa, { from: 'earliest' }).to(qa.queue('out')).run({ queryId: 'asked', ...fast() })
    await until(() => b.messages('out').length >= 2)
    await first.stop()
    assert.equal(record(b, 'asked').stopped.reason, 'stop')
    const qb = fakeQueen(b, 'b')
    const second = await Stream.fromFeed(qb, { from: 'earliest' }).to(qb.queue('out')).run({ queryId: 'asked', ...fast() })
    orders(b, 1)
    await until(() => b.messages('out').length >= 3)
    await until(() => record(b, 'asked').stopped === undefined, 3000, 'the new run\'s commit')
    await second.stop()
  })

  it('a stream stopped before its first read writes no record', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const runner = await Stream.fromFeed(queen, { from: 'earliest' }).to(queen.queue('out')).run({ queryId: 'early', ...fast() })
    await runner.stop()
    const r = record(b, 'early')
    assert.ok(r === undefined || Number.isInteger(r.position), `a position or nothing, got ${JSON.stringify(r)}`)
  })

  it('a standby that stops writes nothing', async () => {
    const b = fakeBroker()
    orders(b, 1)
    const qa = fakeQueen(b, 'a')
    const active = await Stream.fromFeed(qa, { from: 'earliest' }).to(qa.queue('out')).run({ queryId: 'pair', ...fast() })
    await until(() => b.messages('out').length >= 1 && record(b, 'pair')?.caughtUp === true)
    const qb = fakeQueen(b, 'b')
    const standby = await Stream.fromFeed(qb, { from: 'earliest' }).to(qb.queue('out')).run({ queryId: 'pair', ...fast() })
    assert.equal(standby.metrics().role, 'standby')
    const v = b.kvGet(NS, 'pair').version
    await standby.stop()
    assert.equal(b.kvGet(NS, 'pair').version, v)
    assert.equal(record(b, 'pair').stopped, undefined)
    await active.stop()
  })
})
