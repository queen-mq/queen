// Feed streams on event time: steps whose clock is the time the messages
// carry, not the time the log committed them. History pushed all at once,
// messages out of order, a quiet feed, steps chained on event time, and a
// crash anywhere, against the broker in memory of fakeBroker.js.
import { describe, it, afterEach } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { POSITION_NAMESPACE as NS } from '../../client-v2/streams/runtime/FeedRunner.js'
import { fakeBroker, fakeQueen, until } from './fakeBroker.js'

const T0 = Date.parse('2026-11-06T09:00:00.000Z')
// The history: a day long before the feed's clock.
const H0 = Date.parse('2025-10-01T10:00:00.000Z')
const ev = (min, sec = 0) => new Date(H0 + min * 60000 + sec * 1000).toISOString()
const fast = (o = {}) => ({ waitMillis: 20, commitEveryMillis: 0, retryDelayMillis: 1, ...o })
const payloads = (b, q) => b.messages(q).map((e) => e.payload)
const at = (b, ms) => { b.nowMs = T0 + ms }
const quiet = (o = {}) => ({ onError: () => {}, ...o })
const wait = (ms) => new Promise((r) => setTimeout(r, ms))
// A process that dies: its client stops answering, so it can neither commit nor keep its lock.
const crash = (q) => { q.http.cut = true }

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

// An issue opened, and the first comment on it within the hour.
const issue = (b, repo, n, min) => b.push({ queue: 'issues', partition: repo, data: { n, at: ev(min) } })
const comment = (b, repo, n, min) => b.push({ queue: 'comments', partition: repo, data: { n, at: ev(min) } })
const unanswered = (q, opts = {}) => Stream.fromFeed(q, { from: 'earliest', queues: ['issues', 'comments'] })
  .pattern({
    start: (m) => m.queue === 'issues',
    then: (m) => m.queue === 'comments',
    key: (m) => `${m.partition}#${m.data.n}`,
    within: '1h',
    eventTime: (m) => m.data.at,
    onMatch: (s, t) => ({ issue: `${s.partition}#${s.data.n}`, answered: t.data.at.slice(11, 16) }),
    onTimeout: (s) => ({ issue: `${s.partition}#${s.data.n}`, unanswered: true }),
    ...opts
  })
  .name('unanswered')
  .to(q.queue('out'))

describe('feed event time: deadlines on the messages\' own times', () => {
  it('history pushed all at once: a pattern times out by the event times, not the commit times', async () => {
    const load = (b) => {
      issue(b, 'r1', 1, 0)
      issue(b, 'r1', 2, 10)
      issue(b, 'r2', 1, 20)
      comment(b, 'r1', 1, 30)
      comment(b, 'r2', 1, 85) // 65 minutes after its issue
      issue(b, 'r3', 1, 120)
    }
    const b = fakeBroker()
    const queen = fakeQueen(b)
    load(b)
    const runner = await unanswered(queen).run({ queryId: 'history', ...fast() })
    await until(() => b.messages('out').length >= 3)
    await wait(30)
    assert.deepEqual(payloads(b, 'out'), [
      { issue: 'r1#1', answered: '10:30' },
      { issue: 'r1#2', unanswered: true },
      { issue: 'r2#1', unanswered: true }
    ])
    const step = runner.metrics().steps[0]
    assert.equal(step.clock, 'event')
    assert.equal(step.watermark, ev(120), 'the watermark is the latest event time')
    assert.equal(step.open, 1, 'r3#1 waits: its hour has not passed')
    assert.equal(runner.metrics().watermark, ev(120))

    // The same history on the feed's clock: everything is within the hour.
    const f = fakeBroker()
    const fq = fakeQueen(f)
    load(f)
    await Stream.fromFeed(fq, { from: 'earliest', queues: ['issues', 'comments'] })
      .pattern({
        start: (m) => m.queue === 'issues',
        then: (m) => m.queue === 'comments',
        key: (m) => `${m.partition}#${m.data.n}`,
        within: '1h',
        onMatch: (s) => ({ issue: `${s.partition}#${s.data.n}` })
      })
      .to(fq.queue('out'))
      .run({ queryId: 'history-feed', ...fast() })
    await until(() => f.messages('out').length >= 2)
    assert.deepEqual(payloads(f, 'out'), [{ issue: 'r1#1' }, { issue: 'r2#1' }], 'the 65-minute comment matches on commit times')
  })

  it('a message out of order by less than allowedLateness takes its place; one later than that is late', async () => {
    const load = (b) => {
      issue(b, 'r1', 1, 0)
      issue(b, 'r9', 1, 45)
      comment(b, 'r1', 1, 30) // 15 minutes out of order
      issue(b, 'r9', 2, 120)
    }
    const run = async (opts, queryId) => {
      const b = fakeBroker()
      const queen = fakeQueen(b)
      load(b)
      const runner = await unanswered(queen, opts).run({ queryId, ...fast() })
      await until(() => runner.metrics().entriesRead >= 4)
      await wait(60)
      return { out: payloads(b, 'out'), metrics: runner.metrics() }
    }
    const held = await run({ allowedLateness: '20m' }, 'lateness')
    assert.deepEqual(held.out, [{ issue: 'r1#1', answered: '10:30' }], 'the comment waited for its place')
    assert.equal(held.metrics.late, 0)
    assert.equal(held.metrics.steps[0].held, 1, 'r9#2 (12:00) waits for the watermark (11:40)')

    const strict = await run({}, 'strict')
    assert.deepEqual(strict.out, [{ issue: 'r1#1', unanswered: true }, { issue: 'r9#1', unanswered: true }],
      'without lateness the comment came after the watermark: dropped, so its issue times out')
    assert.equal(strict.metrics.late, 1)

    const include = await run({ onLate: 'include' }, 'include')
    assert.deepEqual(include.out, [{ issue: 'r1#1', answered: '10:30' }, { issue: 'r9#1', unanswered: true }],
      "onLate: 'include' takes the late comment at once")
    assert.equal(include.metrics.late, 1)
  })

  it('a join on event time pairs the sides in either order, and its deadline counts on event time', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'payments', partition: 'o1', data: { at: ev(5) } })
    b.push({ queue: 'orders', partition: 'o1', data: { at: ev(0) } })
    b.push({ queue: 'orders', partition: 'o2', data: { at: ev(10) } })
    b.push({ queue: 'orders', partition: 'o3', data: { at: ev(40) } })
    await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders', 'payments'] })
      .join({
        left: (m) => m.queue === 'orders',
        right: (m) => m.queue === 'payments',
        within: '15m',
        eventTime: (m) => m.data.at,
        allowedLateness: '5m',
        select: (l, r) => ({ order: l.partition, paidAfterSec: (Date.parse(r.data.at) - Date.parse(l.data.at)) / 1000 }),
        onTimeout: (m, side) => ({ order: m.partition, missing: side === 'left' ? 'payment' : 'order' })
      })
      .to(queen.queue('out'))
      .run({ queryId: 'join', ...fast() })
    await until(() => b.messages('out').length >= 2)
    await wait(30)
    assert.deepEqual(payloads(b, 'out'), [
      { order: 'o1', paidAfterSec: 300 },
      { order: 'o2', missing: 'payment' }
    ])
  })

  it('a gather on event time counts its deadline from the earliest part, whatever order the parts come in', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const part = (queue, run, min) => b.push({ queue, partition: 'x', data: { run, at: ev(min) } })
    part('c', 1, 9)
    part('a', 1, 0)
    part('b', 1, 5)
    part('a', 2, 20)
    part('b', 2, 25)
    part('c', 2, 31) // 11 minutes after its run's first part
    part('a', 'zz', 50)
    await Stream.fromFeed(queen, { from: 'earliest', queues: ['a', 'b', 'c'] })
      .gather({
        steps: ['a', 'b', 'c'],
        key: (m) => String(m.data.run),
        within: '10m',
        eventTime: (m) => m.data.at,
        allowedLateness: '15m',
        onComplete: (got, run) => ({ run, done: true, first: got.a.data.at.slice(11, 16) }),
        onTimeout: (got, missing, run) => ({ run, missing })
      })
      .to(queen.queue('out'))
      .run({ queryId: 'gather', ...fast() })
    await until(() => b.messages('out').length >= 2)
    await wait(30)
    assert.deepEqual(payloads(b, 'out'), [
      { run: '1', done: true, first: '10:00' },
      { run: '2', missing: ['c'] }
    ])
  })

  it('a session on event time takes its messages in event-time order', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    for (const min of [0, 20, 10]) b.push({ queue: 'clicks', partition: 'p', data: { user: 'u', at: ev(min) } })
    b.push({ queue: 'clicks', partition: 'p', data: { user: 'v', at: ev(120) } })
    await Stream.fromFeed(queen, { from: 'earliest', queues: ['clicks'] })
      .keyBy((m) => m.data.user)
      .windowSession({ gap: 900, eventTime: (m) => m.data.at, allowedLateness: 1800 })
      .aggregate({ clicks: () => 1 })
      .map((v, ctx) => ({ user: ctx.key, clicks: v.clicks, from: new Date(ctx.windowStart).toISOString().slice(11, 16), to: new Date(ctx.windowEnd).toISOString().slice(11, 16) }))
      .to(queen.queue('sessions'))
      .run({ queryId: 'sessions', ...fast() })
    await until(() => b.messages('sessions').length >= 1)
    await wait(30)
    // In the order they came, 10:20 would end the 10:00 session (past the
    // 15-minute gap) before 10:10 joins them.
    assert.deepEqual(payloads(b, 'sessions'), [{ user: 'u', clicks: 3, from: '10:00', to: '10:35' }])
  })
})

describe('feed sessions: the gap', () => {
  it('a message exactly gap after the last one is still in the session, with or without grace, on either clock', async () => {
    const sessions = async (opts, eventTime) => {
      const b = fakeBroker()
      const queen = fakeQueen(b)
      for (const sec of [0, 10, 21]) {
        at(b, sec * 1000)
        b.push({ queue: 'clicks', partition: 'u1', data: { at: ev(0, sec) } })
      }
      at(b, 60000)
      b.push({ queue: 'clicks', partition: 'other', data: { at: ev(1) } })
      const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['clicks'] })
        .windowSession({ gap: 10, ...opts, ...(eventTime ? { eventTime: (m) => m.data.at } : {}) })
        .aggregate({ count: () => 1 })
        .map((acc, ctx) => ({ user: ctx.key, count: acc.count }))
        .to(queen.queue('sessions'))
        .run({ queryId: 'gap', ...fast({ now: () => b.nowMs }) })
      await until(() => b.messages('sessions').length >= 2)
      await runner.stop()
      return payloads(b, 'sessions').filter((p) => p.user === 'u1')
    }
    // 0 and 10 are 10 s apart: one session; 21 is 11 s after 10: the next.
    const want = [{ user: 'u1', count: 2 }, { user: 'u1', count: 1 }]
    assert.deepEqual(await sessions({}, false), want, "feed's clock, no grace")
    assert.deepEqual(await sessions({ gracePeriod: 2 }, false), want, "feed's clock, grace")
    assert.deepEqual(await sessions({}, true), want, 'event time, no grace')
    assert.deepEqual(await sessions({ gracePeriod: 2 }, true), want, 'event time, grace')
  })
})

describe('feed event time: chained steps', () => {
  const pushes = (b) => {
    for (const min of [5, 10, 15, 60, 90, 105, 180, 190, 200, 210]) b.push({ queue: 'pushes', partition: 'r1', data: { at: ev(min) } })
    b.push({ queue: 'pushes', partition: 'r2', data: { at: ev(310) } })
  }
  const busyHours = (q, patternOpts = {}) => Stream.fromFeed(q, { from: 'earliest', queues: ['pushes'] })
    .windowTumbling({ seconds: 3600, eventTime: (m) => m.data.at })
    .aggregate({ pushes: () => 1 })
    .map((v, ctx) => ({ ...v, hour: ctx.windowKey.slice(11, 13) }))
    .pattern({
      start: (m) => m.data.pushes >= 3,
      then: (m) => m.data.pushes >= 3,
      within: '90m',
      onMatch: (a, c) => ({ repo: a.partition, busy: [a.data.hour, c.data.hour] }),
      onTimeout: (a) => ({ repo: a.partition, alone: a.data.hour }),
      ...patternOpts
    })
    .name('busy')
    .to(q.queue('out'))

  it("a step after a step on event time runs on the results' event times (a window's last millisecond)", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    pushes(b)
    const runner = await busyHours(queen).run({ queryId: 'chain', ...fast() })
    await until(() => b.messages('out').length >= 2)
    await wait(30)
    // Hours 10 and 11 are within 90 minutes (10:59:59.999 to 11:59:59.999);
    // hour 13 is not, so hour 11's wait times out first.
    assert.deepEqual(payloads(b, 'out'), [
      { repo: 'r1', busy: ['10', '11'] },
      { repo: 'r1', alone: '11' }
    ])
    const steps = runner.metrics().steps
    assert.deepEqual(steps.map((s) => s.clock), ['event', 'event'])
    // The pattern follows the window step's watermark (15:10: r2's push): that
    // step can send nothing older (r2's hour 15 is open until 16:00).
    assert.equal(steps[1].watermark, ev(310), "the watermark of the window step: nothing older can come from it")
  })

  it('eventTime: false keeps a later step on the feed\'s clock', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    pushes(b)
    const runner = await busyHours(queen, { eventTime: false }).run({ queryId: 'chain-feed', ...fast() })
    await until(() => b.messages('out').length >= 2)
    await wait(30)
    assert.deepEqual(payloads(b, 'out'), [
      { repo: 'r1', busy: ['10', '11'] },
      { repo: 'r1', busy: ['11', '13'] }
    ], 'the results came within 90 minutes of each other on the feed\'s clock')
    assert.deepEqual(runner.metrics().steps.map((s) => s.clock), ['event', 'feed'])
  })
})

describe('feed event time: a quiet feed', () => {
  it('without messages the watermark stays; with idle it moves on with the feed\'s clock', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    b.push({ queue: 'readings', partition: 's1', data: { at: ev(0, 10), v: 1 } })
    b.push({ queue: 'readings', partition: 's1', data: { at: ev(0, 50), v: 2 } })
    const sums = (idle) => Stream.fromFeed(queen, { from: 'earliest', queues: ['readings'] })
      .windowTumbling({ seconds: 60, eventTime: (m) => m.data.at, ...(idle ? { idle } : {}) })
      .aggregate({ sum: (v) => v.v })
      .map((acc, ctx) => ({ minute: ctx.windowKey.slice(11, 16), sum: acc.sum }))
    const still = await sums(null).to(queen.queue('still')).run({ queryId: 'no-idle', ...fast({ now: () => b.nowMs }) })
    const runner = await sums('30s').to(queen.queue('sums')).run({ queryId: 'idle', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 2 && still.metrics().entriesRead >= 2)
    // 35 s after the last message: the watermark moved 5 s past 10:00:50.
    at(b, 36000)
    await wait(200)
    assert.equal(b.messages('sums').length, 0)
    // 41 s after: past 10:01:00, the minute closes.
    at(b, 42000)
    await until(() => b.messages('sums').length >= 1, 3000)
    assert.deepEqual(payloads(b, 'sums'), [{ minute: '10:00', sum: 3 }])
    assert.equal(runner.metrics().steps[0].watermark, ev(1, 1), 'at 42 s: 10:00:50 + 11 s')
    at(b, 600000)
    await wait(200)
    assert.equal(b.messages('still').length, 0, 'without idle only a message moves the watermark')
  })

  it('a finished import releases what it held and passes its last deadlines at the pace of the feed\'s clock', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    b.push({ queue: 'issues', partition: 'r1', data: { n: 1, at: ev(0, 0) } })
    b.push({ queue: 'issues', partition: 'r1', data: { n: 2, at: ev(0, 5) } })
    b.push({ queue: 'comments', partition: 'r1', data: { n: 1, at: ev(0, 12) } })
    const runner = await unanswered(queen, { within: '20s', idle: '30s', allowedLateness: '10s' })
      .run({ queryId: 'tail', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 3)
    await wait(50)
    // The watermark is at 10:00:02: r1#2 (10:00:05) and the comment (10:00:12) wait for it.
    assert.deepEqual(payloads(b, 'out'), [])
    assert.equal(runner.metrics().steps[0].held, 2)
    // 30 s of quiet, then event time goes on with the feed's clock: the
    // comment's turn comes 40 s after the last message (in time for r1#1).
    at(b, 1000 + 45000)
    await until(() => b.messages('out').length >= 1, 3000)
    await wait(50)
    assert.deepEqual(payloads(b, 'out'), [{ issue: 'r1#1', answered: '10:00' }])
    // r1#2 is due at 10:00:25.001: 53 s after.
    at(b, 1000 + 54000)
    await until(() => b.messages('out').length >= 2, 3000)
    assert.deepEqual(payloads(b, 'out'), [{ issue: 'r1#1', answered: '10:00' }, { issue: 'r1#2', unanswered: true }])
  })
})

describe('feed event time: restarts', () => {
  it('a stop and a start keep the held messages and the watermark', async () => {
    const b = fakeBroker()
    issue(b, 'r1', 1, 0)
    issue(b, 'r2', 1, 10)
    const first = await unanswered(fakeQueen(b, 'a'), { allowedLateness: '30m' }).run({ queryId: 'restart', ...fast() })
    await until(() => first.metrics().entriesRead >= 2)
    assert.equal(first.metrics().steps[0].held, 2)
    await first.stop()
    comment(b, 'r1', 1, 50)
    issue(b, 'r3', 1, 200)
    const second = await unanswered(fakeQueen(b, 'b'), { allowedLateness: '30m' }).run({ queryId: 'restart', ...fast() })
    await until(() => b.messages('out').length >= 2)
    await wait(30)
    assert.deepEqual(payloads(b, 'out'), [{ issue: 'r1#1', answered: '10:50' }, { issue: 'r2#1', unanswered: true }])
    assert.equal(second.metrics().replays, 0, 'the stop wrote a snapshot: nothing to read again')
  })

  it('a replay closes again what idle closed, at the same feed time, and sends it once', async () => {
    const b = fakeBroker()
    const opts = { queryId: 'idle-replay', ...fast({ now: () => b.nowMs, lockTtl: '1s', snapshotEveryMillis: 3600000 }), ...quiet() }
    const chain = (q) => unanswered(q, { within: '1m', idle: '30s' })
    at(b, 1000)
    b.push({ queue: 'issues', partition: 'r1', data: { n: 1, at: ev(0, 0) } })
    b.push({ queue: 'issues', partition: 'r2', data: { n: 1, at: ev(0, 30) } })
    const qa = fakeQueen(b, 'a')
    const a = await chain(qa).run(opts)
    await until(() => a.metrics().entriesRead >= 2)
    // Quiet: r1#1 (due 10:01:00.001) passes 61.001 s after the last message.
    at(b, 1000 + 62000)
    await until(() => b.messages('out').length >= 1, 3000)
    // r4#1 keeps the state from going empty (an empty state would move the snapshot here).
    b.push({ queue: 'issues', partition: 'r4', data: { n: 1, at: ev(1, 15) } })
    b.push({ queue: 'comments', partition: 'r2', data: { n: 1, at: ev(1, 20) } })
    await until(() => b.messages('out').length >= 2, 3000)
    const end = b.messages('comments').pop().position
    await until(() => (b.kvGet(NS, 'idle-replay')?.value?.position ?? 0) > end)
    assert.equal(b.kvGet(NS, 'idle-replay').value.snapshot.position, 1, 'no snapshot since the start')
    crash(qa)
    // The next runner reads it all again: the timeout (idle) and the match
    // come back in the state, and are not sent again.
    const second = await chain(fakeQueen(b, 'b')).run(opts)
    await until(() => second.metrics().role === 'active', 6000)
    assert.ok(second.metrics().replayEntries >= 4, `replayed ${second.metrics().replayEntries}`)
    at(b, 1000 + 70000)
    b.push({ queue: 'issues', partition: 'r3', data: { n: 1, at: ev(2, 0) } })
    at(b, 1000 + 200000)
    await until(() => b.messages('out').length >= 4, 4000)
    await wait(50)
    assert.deepEqual(payloads(b, 'out'), [
      { issue: 'r1#1', unanswered: true },
      { issue: 'r2#1', answered: '10:01' },
      { issue: 'r4#1', unanswered: true },
      { issue: 'r3#1', unanswered: true }
    ])
  })

  it('out-of-order history, crashed at random points, sends what one uninterrupted run sends', async () => {
    let seed = 11
    // The LCG in 32-bit integers: in doubles the product loses its low bits.
    const rnd = () => { seed = (Math.imul(seed, 1103515245) + 12345) & 0x7fffffff; return seed / 2147483648 }
    const msgs = []
    let e = 0
    for (let i = 0; i < 150; i++) {
      e += Math.floor(rnd() * 90)
      const jitter = Math.floor((rnd() - 0.5) * 240) // up to 2 minutes either way
      msgs.push({ i, repo: `r${Math.floor(rnd() * 6)}`, open: rnd() < 0.5, sec: Math.max(0, e + jitter) })
    }
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['issues', 'comments'] })
      .pattern({
        start: (m) => m.queue === 'issues',
        then: (m) => m.queue === 'comments',
        within: '10m',
        eventTime: (m) => m.data.at,
        allowedLateness: '3m',
        idle: '30s',
        onMatch: (s, t) => ({ repo: s.partition, s: s.data.i, t: t.data.i }),
        onTimeout: (s) => ({ repo: s.partition, s: s.data.i, timeout: true })
      })
      .keyBy((v) => v.repo)
      .windowTumbling({ seconds: 1800 })
      .aggregate({ results: () => 1 })
      .map((v, ctx) => ({ repo: ctx.key, half: ctx.windowKey.slice(11, 16), results: v.results }))
      .to(q.queue('out'))
    // Per repo (the sink's partition), in order: one commit groups its pushes
    // by partition, and where the commits fall differs with the crashes.
    const byRepo = (b) => {
      const m = {}
      for (const e of b.messages('out')) (m[e.partition] ||= []).push(e.payload)
      return m
    }
    // Returns the position of the last message.
    const load = (b) => {
      let last = 0
      for (const m of msgs) {
        at(b, m.i * 1000)
        last = b.push({ queue: m.open ? 'issues' : 'comments', partition: m.repo, data: { i: m.i, at: new Date(H0 + m.sec * 1000).toISOString() } })
      }
      return last
    }
    // The import ends; the feed's clock goes on for 80 minutes, which closes
    // every wait and window (idle), the same way on both runs.
    const tail = async (b, runner) => {
      for (let k = 1; k <= 8; k++) {
        at(b, msgs.length * 1000 + k * 600000)
        await wait(60)
      }
      await until(() => {
        const m = runner.metrics()
        return m.open === 0 && m.steps.every((st) => !st.held) && m.pendingOutputs === 0
      }, 8000, 'every wait and window to close')
      await wait(100)
    }

    const ref = fakeBroker()
    const refEnd = load(ref)
    const r = await chain(fakeQueen(ref)).run({ queryId: 'rand-et', ...fast({ now: () => ref.nowMs }) })
    await until(() => (ref.kvGet(NS, 'rand-et')?.value?.position ?? 0) > refEnd, 6000)
    await tail(ref, r)
    await r.stop()
    const expected = byRepo(ref)
    assert.ok(Object.keys(expected).length === 6 && payloads(ref, 'out').length >= 12, `results: ${payloads(ref, 'out').length}`)

    const b = fakeBroker()
    const end = load(b)
    const opts = () => ({ queryId: 'rand-et', ...fast({ now: () => b.nowMs, lockTtl: '1s', maxRecords: 7, snapshotMaxEntries: 20 }), ...quiet() })
    let q = fakeQueen(b, 'p0')
    let runner = await chain(q).run(opts())
    for (let k = 1; k <= 3; k++) {
      const target = Math.floor(msgs.length * k / 4)
      await until(() => (b.kvGet(NS, 'rand-et')?.value?.position ?? 0) >= target, 6000)
      crash(q)
      q = fakeQueen(b, `p${k}`)
      runner = await chain(q).run(opts())
      await until(() => runner.metrics().role === 'active', 6000)
    }
    await until(() => (b.kvGet(NS, 'rand-et')?.value?.position ?? 0) > end, 6000)
    await tail(b, runner)
    await runner.stop()
    assert.deepEqual(byRepo(b), expected)
  })
})

describe('feed event time: failures and checks', () => {
  it('a held message whose step keeps failing meets the poison policy as the message it is', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    issue(b, 'bad', 1, 0)
    issue(b, 'good', 1, 0)
    comment(b, 'bad', 1, 5)
    comment(b, 'good', 1, 6)
    issue(b, 'later', 1, 30)
    const runner = await unanswered(queen, {
      allowedLateness: '10m',
      onMatch: (s) => {
        if (s.partition === 'bad') throw new Error('boom')
        return { issue: `${s.partition}#${s.data.n}`, answered: true }
      }
    }).run({ queryId: 'poison-held', ...fast({ onPoison: queen.queue('dlq') }), ...quiet() })
    await until(() => b.messages('out').length >= 1 && b.messages('dlq').length >= 1)
    await wait(30)
    assert.deepEqual(payloads(b, 'out'), [{ issue: 'good#1', answered: true }])
    const [record] = payloads(b, 'dlq')
    assert.equal(record.entry.queue, 'comments', 'the comment that made the failing match')
    assert.equal(record.entry.partition, 'bad')
    assert.equal(record.error.message, 'boom')
    assert.equal(runner.metrics().deadLettered, 1)
  })

  it('a live read lists what a step keeps, not the messages it holds for order', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    issue(b, 'r1', 1, 0)
    issue(b, 'r2', 1, 5)
    const runner = await unanswered(queen, { allowedLateness: '1h' }).run({ queryId: 'live', ...fast() })
    await until(() => runner.metrics().entriesRead >= 2)
    assert.deepEqual(runner.state('unanswered').list(), { items: [], truncated: false, total: 0 })
    assert.equal(runner.metrics().steps[0].held, 2)
    issue(b, 'r3', 1, 62)
    await until(() => runner.metrics().steps[0].open === 1)
    const list = runner.state('unanswered').list()
    assert.equal(list.total, 1)
    assert.equal(list.items[0].key, 'r1#1')
    assert.equal(runner.state('unanswered').get('r1#1').found, true)
  })

  it('refuses idle, allowedLateness and onLate on the feed\'s clock, idle on a queue source, and an eventTime that is not a function', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const pattern = (extra) => Stream.fromFeed(queen, { from: 'earliest' })
      .pattern({ start: () => true, then: () => true, within: '1m', ...extra })
      .to(queen.queue('out'))
    await assert.rejects(pattern({ idle: '30s' }).run({ queryId: 'v1', ...fast() }), /idle moves a watermark on/)
    await assert.rejects(pattern({ allowedLateness: '1m' }).run({ queryId: 'v2', ...fast() }), /allowedLateness and onLate are for event time/)
    await assert.rejects(pattern({ onLate: 'include' }).run({ queryId: 'v3', ...fast() }), /allowedLateness and onLate are for event time/)
    const window = (extra) => Stream.fromFeed(queen, { from: 'earliest' })
      .windowTumbling({ seconds: 60, ...extra }).aggregate({ n: () => 1 }).to(queen.queue('out'))
    await assert.rejects(window({ idle: 30 }).run({ queryId: 'v4', ...fast() }), /idle moves a watermark on/)
    await assert.rejects(window({ allowedLateness: 30 }).run({ queryId: 'v6', ...fast() }), /allowedLateness and onLate are for event time: the tumbling window/)
    await assert.rejects(window({ onLate: 'include' }).run({ queryId: 'v7', ...fast() }), /allowedLateness and onLate are for event time: the tumbling window/)
    await assert.rejects(Stream.fromFeed(queen, { from: 'earliest' })
      .windowSession({ gap: 60, allowedLateness: '1m' }).aggregate({ n: () => 1 }).to(queen.queue('out'))
      .run({ queryId: 'v8', ...fast() }), /allowedLateness and onLate are for event time: the session window/)
    // The plain values pass: no lateness, late messages dropped.
    const plain = await window({ allowedLateness: 0, onLate: 'drop' }).run({ queryId: 'v9', ...fast() })
    await plain.stop()
    await assert.rejects(Stream.from({ name: 'src' }).windowTumbling({ seconds: 60, idle: 30 }).aggregate({ n: () => 1 })
      .to({ name: 'out' }).run({ queryId: 'v5' }), /runs on Stream.fromFeed only/)
    assert.throws(() => pattern({ eventTime: 'data.at' }), /eventTime is a function/)
    assert.throws(() => pattern({ eventTime: (m) => m.data.at, idle: 0 }), /idle is a duration above zero/)
  })
})

describe('feed event time: what the code review found', () => {
  it('a step after a step on event time follows its watermark: a quiet feed sends nothing the step before still has as late', async () => {
    // The pattern's timeout of issue 1 (10:10 of the feed's clock = of event time) and the
    // match of issue 2 both count in the hour, though idle moved the pattern on in between.
    const b = fakeBroker()
    const q = fakeQueen(b)
    const push = (ms, queue, n) => { at(b, ms); b.push({ queue, partition: 'r1', data: { n, at: new Date(T0 + ms).toISOString() } }) }
    push(0, 'issues', 1)
    push(300000, 'issues', 2)
    push(400000, 'comments', 2)
    const runner = await Stream.fromFeed(q, { from: 'earliest', queues: ['issues', 'comments'] })
      .pattern({
        start: (m) => m.queue === 'issues', then: (m) => m.queue === 'comments',
        key: (m) => `${m.partition}#${m.data.n}`, within: '10m',
        eventTime: (m) => m.data.at, idle: '1m',
        onMatch: (s) => ({ issue: s.data.n, answered: true }), onTimeout: (s) => ({ issue: s.data.n, answered: false })
      })
      .keyBy(() => 'r1')
      .windowTumbling({ seconds: 3600 })
      .aggregate({ results: () => 1 })
      .to(q.queue('hourly'))
      .run({ queryId: 'review-h1', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 3)
    for (const ms of [500000, 700000, 1200000, 3000000, 4000000, 5000000, 6000000]) { at(b, ms); await wait(60) }
    await until(() => b.messages('hourly').length >= 1, 3000)
    await wait(50)
    assert.equal(runner.metrics().late, 0)
    assert.deepEqual(payloads(b, 'hourly'), [{ results: 2 }])
  })

  it('a window after a window: every reading counted once, with idle on the first only', async () => {
    const b = fakeBroker()
    const q = fakeQueen(b)
    // A message every 20 s of the feed's clock; event time at a third of that pace.
    for (let k = 0; k < 60; k++) {
      at(b, k * 20000)
      b.push({ queue: 'r', partition: 's', data: { at: new Date(H0 + Math.floor(k * 20000 / 3)).toISOString() } })
    }
    const runner = await Stream.fromFeed(q, { from: 'earliest', queues: ['r'] })
      .windowTumbling({ seconds: 60, eventTime: (m) => m.data.at, idle: '30s' })
      .aggregate({ n: () => 1 })
      .windowTumbling({ seconds: 3600 })
      .aggregate({ n: (v) => v.n })
      .to(q.queue('out'))
      .run({ queryId: 'review-h1b', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 60)
    for (let k = 1; k <= 8; k++) { at(b, 60 * 20000 + k * 900000); await wait(60) }
    await until(() => b.messages('out').length >= 1, 3000)
    await wait(50)
    assert.equal(runner.metrics().late, 0)
    assert.deepEqual(payloads(b, 'out'), [{ n: 60 }])
  })

  it('idle goes on the first step on event time only', async () => {
    const b = fakeBroker()
    const q = fakeQueen(b)
    const first = () => Stream.fromFeed(q, { from: 'earliest' }).windowTumbling({ seconds: 60, eventTime: (m) => m.data.at }).aggregate({ n: () => 1 })
    await assert.rejects(first().windowTumbling({ seconds: 3600, idle: 30 }).aggregate({ n: (v) => v.n }).to(q.queue('out'))
      .run({ queryId: 'v-fed-idle', ...fast() }), /follows its watermark: put idle on the first step/)
    await assert.rejects(first().pattern({ start: () => true, then: () => true, within: '1m', eventTime: (m) => m.ts, idle: '30s' }).to(q.queue('out'))
      .run({ queryId: 'v-fed-own', ...fast() }), /results of the step before it/)
  })

  it('a held message whose unit failed leaves nothing behind that releases the next one early', async () => {
    const run = async (withBad) => {
      const b = fakeBroker()
      const q = fakeQueen(b)
      // One transaction: an item of k1 is held, then an item without a kind fails the unit
      // (skipped); k2's part a at 10:20 is held next, under the same number.
      const e1 = { queue: 'ev', partition: 'p', data: { at: ev(0), items: withBad ? [{ kind: 'a', id: 'k1' }, { id: 'bad' }] : [] } }
      b.push([e1, { queue: 'ev', partition: 'p', data: { at: ev(20), items: [{ kind: 'a', id: 'k2' }] } }])
      b.push({ queue: 'ev', partition: 'p', data: { at: ev(31), items: [{ kind: 'a', id: 'k3' }] } })
      b.push({ queue: 'ev', partition: 'p', data: { at: ev(10), items: [{ kind: 'b', id: 'k2' }] } })
      b.push({ queue: 'ev', partition: 'p', data: { at: ev(200), items: [{ kind: 'a', id: 'k9' }] } })
      const runner = await Stream.fromFeed(q, { from: 'earliest', queues: ['ev'] })
        .flatMap((m) => m.data.items)
        .gather({
          steps: { a: (m, v) => v.kind === 'a', b: (m, v) => v.kind.startsWith('b') },
          key: (m, v) => v.id, within: '5m', eventTime: (m) => m.data.at, allowedLateness: '30m',
          onComplete: (got, key) => ({ key, done: true }), onTimeout: (got, missing, key) => ({ key, missing })
        })
        .to(q.queue('out'))
        .run({ queryId: 'review-h2', ...fast({ onPoison: 'skip' }), ...quiet() })
      await until(() => runner.metrics().entriesRead >= 5)
      await wait(80)
      return { out: payloads(b, 'out').filter((p) => p.key === 'k2'), skipped: runner.metrics().skipped }
    }
    const want = [{ key: 'k2', missing: ['a'] }, { key: 'k2', missing: ['b'] }]
    assert.deepEqual((await run(false)).out, want)
    const bad = await run(true)
    assert.equal(bad.skipped, 1)
    assert.deepEqual(bad.out, want)
  })

  it('two held items of one message that both fail go to the dead-letter queue, each once', async () => {
    const b = fakeBroker()
    const q = fakeQueen(b)
    b.push({ queue: 'issues', partition: 'r1', data: { ids: ['a', 'b'], bad: true, at: ev(0) } })
    b.push({ queue: 'comments', partition: 'r1', data: { ids: ['a', 'b'], at: ev(5) } })
    b.push({ queue: 'issues', partition: 'r1', data: { ids: ['c'], at: ev(30) } })
    const runner = await Stream.fromFeed(q, { from: 'earliest', queues: ['issues', 'comments'] })
      .flatMap((m) => m.data.ids.map((id) => ({ id })))
      .pattern({
        start: (m) => m.queue === 'issues', then: (m) => m.queue === 'comments',
        key: (m, v) => v.id, within: '1h', eventTime: (m) => m.data.at,
        onMatch: (s) => { if (s.data.bad) throw new Error('boom'); return { ok: true } }
      })
      .to(q.queue('out'))
      .run({ queryId: 'review-h3', ...fast({ onPoison: q.queue('dlq') }), ...quiet() })
    await until(() => b.messages('dlq').length >= 2, 3000)
    await wait(80)
    assert.equal(b.messages('dlq').length, 2)
    assert.equal(new Set(b.messages('dlq').map((e) => e.transactionId)).size, 2)
    assert.ok(runner.metrics().conflicts <= 1, `conflicts: ${runner.metrics().conflicts}`)
  })

  it('an Invalid Date meets the poison policy; a window takes allowedLateness as a duration too', async () => {
    const b = fakeBroker()
    const q = fakeQueen(b)
    b.push({ queue: 'issues', partition: 'r1', data: { n: 1, at: ev(0) } })
    b.push({ queue: 'issues', partition: 'r1', data: { n: 2 } })
    b.push({ queue: 'comments', partition: 'r1', data: { n: 1, at: ev(5) } })
    for (let k = 3; k < 10; k++) b.push({ queue: 'issues', partition: 'r1', data: { n: k, at: ev(60 * k) } })
    const runner = await unanswered(q, { eventTime: (m) => new Date(m.data.at) })
      .run({ queryId: 'review-m1', ...fast({ onPoison: 'skip' }), ...quiet() })
    await until(() => runner.metrics().entriesRead >= 10)
    await until(() => b.messages('out').length >= 3)
    assert.equal(runner.metrics().skipped, 1)
    assert.deepEqual(payloads(b, 'out').slice(0, 2), [{ issue: 'r1#1', answered: '10:05' }, { issue: 'r1#3', unanswered: true }])

    const w = fakeBroker()
    const wq = fakeQueen(w)
    for (const min of [0, 1, 2, 30, 31, 90]) w.push({ queue: 'r', partition: 's', data: { at: ev(min) } })
    await Stream.fromFeed(wq, { from: 'earliest', queues: ['r'] })
      .windowTumbling({ seconds: 600, eventTime: (m) => m.data.at, allowedLateness: '5m' })
      .aggregate({ n: () => 1 })
      .to(wq.queue('out'))
      .run({ queryId: 'review-m1b', ...fast() })
    await until(() => w.messages('out').length >= 2)
    assert.deepEqual(payloads(w, 'out'), [{ n: 3 }, { n: 2 }])
  })

  it('a message a pattern ignores needs no time and moves no watermark', async () => {
    const b = fakeBroker()
    const q = fakeQueen(b)
    b.push({ queue: 'issues', partition: 'r1', data: { n: 1, at: ev(0) } })
    b.push({ queue: 'audit', partition: 'x', data: { who: 'bot' } }) // no time at all
    b.push({ queue: 'audit', partition: 'x', data: { at: ev(60 * 24 * 365) } }) // a year later
    b.push({ queue: 'comments', partition: 'r1', data: { n: 1, at: ev(5) } })
    b.push({ queue: 'issues', partition: 'r2', data: { n: 1, at: ev(30) } })
    const runner = await Stream.fromFeed(q, { from: 'earliest' })
      .pattern({
        start: (m) => m.queue === 'issues', then: (m) => m.queue === 'comments',
        key: (m) => `${m.partition}#${m.data.n}`, within: '1h', allowedLateness: '10m',
        eventTime: (m) => m.data.at,
        onMatch: (s) => ({ answered: s.data.n }), onTimeout: (s) => ({ unanswered: s.data.n })
      })
      .to(q.queue('out'))
      .run({ queryId: 'review-m2', ...fast(), ...quiet() })
    await until(() => runner.metrics().entriesRead >= 5)
    await until(() => b.messages('out').length >= 1)
    await wait(50)
    assert.equal(runner.metrics().stopped, null)
    assert.equal(runner.metrics().late, 0)
    assert.deepEqual(payloads(b, 'out'), [{ answered: 1 }])
  })

  it('the chain hash says which steps take the event time of the step before; eventTime: false on a first step changes nothing', () => {
    const q = fakeQueen(fakeBroker())
    const hash = (s) => s.to(q.queue('out'))._compileFeed().chainHash
    const win = () => Stream.fromFeed(q).windowTumbling({ seconds: 60, eventTime: (m) => m.data.at }).aggregate({ n: () => 1 })
    const pat = (o = {}) => ({ start: () => true, then: () => true, within: '1m', ...o })
    assert.notEqual(hash(win().pattern(pat())), hash(win().pattern(pat({ eventTime: false }))), 'inherited event time vs the feed clock')
    const plain = () => Stream.fromFeed(q).windowTumbling({ seconds: 60 }).aggregate({ n: () => 1 })
    assert.equal(hash(plain()), hash(Stream.fromFeed(q).windowTumbling({ seconds: 60, eventTime: false }).aggregate({ n: () => 1 })))
  })
})
