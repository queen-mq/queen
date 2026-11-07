// What a feed stream does since it keeps two positions instead of its state:
// a restart reads the feed again from the last snapshot; any number of
// stateful steps in one stream; joins of any number of sides; gathers; table
// joins; shards; and its live state, read in process and over Queen.
// Each runs the client's own Kv, Lock, TransactionBuilder, Ephemeral and
// FeedReader against the broker in memory of fakeBroker.js.
import { describe, it, afterEach } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { POSITION_NAMESPACE as NS, outputPrefix } from '../../client-v2/streams/runtime/FeedRunner.js'
import { shardOf, fnv1a32 } from '../../client-v2/streams/runtime/feed/state.js'
import { encodeSnapshot, decodeSnapshot } from '../../client-v2/streams/runtime/feed/snapshot.js'
import { fakeBroker, fakeQueen, until } from './fakeBroker.js'

const T0 = Date.parse('2026-11-06T09:00:00.000Z')
const MIN0 = Math.floor(T0 / 60000) * 60000
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

const perMinute = (q, opts = {}) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'], ...opts })
  .windowTumbling({ seconds: 60 })
  .aggregate({ count: () => 1 })
  .name('perMinute')
  .to(q.queue('counts'))

describe('feed state: two positions and a replay', () => {
  it('a commit writes the position only: no state, while windows stay open', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 0; i < 3; i++) b.push({ queue: 'orders', partition: 'c1', data: { n: i } })
    const runner = await perMinute(queen).run({ queryId: 'pos-only', ...fast({ now: () => b.nowMs }) })
    await until(() => (b.kvGet(NS, 'pos-only')?.value?.position ?? 0) > 3)
    const stored = b.kvGet(NS, 'pos-only').value
    assert.equal(stored.snapshot.chunks, 0, 'no chunks: the state is not written')
    assert.equal(stored.snapshot.position, 1, "the snapshot is the stream's start, where the state was empty")
    assert.deepEqual(b.kvKeys(NS, 'pos-only#').filter((k) => k !== 'pos-only#about'), [], 'nothing but the position record (and the description)')
    assert.equal(runner.metrics().stateKeys, 1, 'the open window is in memory')
  })

  it('after a crash the next runner reads again from the snapshot: nothing lost, nothing sent twice', async () => {
    const b = fakeBroker()
    const qa = fakeQueen(b, 'a')
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    // No snapshot due in this test: the next runner must read the feed again.
    const a = await perMinute(qa).run({ queryId: 'crash', ...fast({ now: () => b.nowMs, lockTtl: '1s', snapshotEveryMillis: 3600000 }), ...quiet() })
    await until(() => b.messages('counts').length >= 1 && (b.kvGet(NS, 'crash')?.value?.position ?? 0) > 3)
    assert.deepEqual(payloads(b, 'counts'), [{ count: 2 }], 'minute 0 closed before the crash')
    crash(qa)
    const commitsAtCrash = a.metrics().commits
    at(b, 62000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const qb = fakeQueen(b, 'b')
    const second = await perMinute(qb).run({ queryId: 'crash', ...fast({ now: () => b.nowMs, lockTtl: '1s', snapshotEveryMillis: 3600000 }), ...quiet() })
    await until(() => second.metrics().role === 'active', 6000, 'the takeover')
    assert.ok(second.metrics().replays >= 1, 'it read the feed again')
    assert.ok(second.metrics().replayEntries >= 3)
    at(b, 121000)
    b.push({ queue: 'orders', partition: 'other', data: {} })
    await until(() => b.messages('counts').length >= 2)
    await second.stop()
    assert.deepEqual(payloads(b, 'counts'), [{ count: 2 }, { count: 2 }], 'minute 0 once, minute 1 whole')
    assert.equal(a.metrics().commits, commitsAtCrash, 'the crashed process committed nothing more')
  })

  it('a snapshot is a full layer, then deltas of what changed; only named layers stay in KV, and a restart loads them and the tail', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 0; i < 20; i++) b.push({ queue: 'orders', partition: `c${i}`, data: {} })
    const runner = await perMinute(queen).run({ queryId: 'snapdue', ...fast({ now: () => b.nowMs, snapshotMaxEntries: 2 }) })
    await until(() => runner.metrics().snapshots >= 1 && runner.metrics().entriesRead >= 20)
    const first = b.kvGet(NS, 'snapdue').value.snapshot
    assert.equal(first.layers.length, 1)
    assert.ok(first.layers[0].full, 'the first snapshot is a full layer')
    // Two more messages for c0: one key changed, so the next snapshot is a delta of it on the full layer.
    for (let k = 0; k < 2; k++) b.push({ queue: 'orders', partition: 'c0', data: {} })
    await until(() => (b.kvGet(NS, 'snapdue').value.snapshot.layers || []).length >= 2)
    const second = b.kvGet(NS, 'snapdue').value.snapshot
    assert.equal(second.layers[0].sid, first.layers[0].sid, 'the full layer is still the one under it')
    assert.ok(!second.layers[1].full, 'a delta')
    assert.ok(second.layers[1].bytes < second.layers[0].bytes / 5, `the delta holds the changed key only (${second.layers[1].bytes} B vs ${second.layers[0].bytes} B)`)
    await runner.stop()
    const stored = b.kvGet(NS, 'snapdue').value
    const named = new Set(stored.snapshot.layers.map((l) => l.sid))
    const keys = b.kvKeys(NS, 'snapdue#snap#')
    assert.ok(keys.length > 0 && keys.every((k) => named.has(k.slice('snapdue#snap#'.length, k.lastIndexOf('#')))), 'only the chunks of named layers are in KV')
    const again = await perMinute(fakeQueen(b, 'again')).run({ queryId: 'snapdue', ...fast({ now: () => b.nowMs }) })
    assert.equal(again.metrics().stateKeys, 20, 'the twenty open windows came back')
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'late', data: {} })
    await until(() => b.messages('counts').length >= 20)
    await again.stop()
    const counts = Object.fromEntries(b.messages('counts').map((e) => [e.partition, e.payload.count]))
    const want = Object.fromEntries(Array.from({ length: 20 }, (_, i) => [`c${i}`, i === 0 ? 3 : 1]))
    assert.deepEqual(counts, want)
  })

  it('a state that did not change moves the snapshot with the position, writing no chunk', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const runner = await perMinute(queen, { queues: ['orders', 'noise'] })
      .run({ queryId: 'moved', ...fast({ now: () => b.nowMs, snapshotMaxEntries: 1, positionCommitMillis: 0 }) })
    await until(() => runner.metrics().snapshots >= 1)
    const sid = b.kvGet(NS, 'moved').value.snapshot.id
    const snapshots = runner.metrics().snapshots
    for (let i = 0; i < 3; i++) b.push({ queue: 'other-queue', partition: 'x', data: {} })
    const end = b.applied + 1
    await until(() => (b.kvGet(NS, 'moved')?.value?.position ?? 0) >= end)
    const stored = b.kvGet(NS, 'moved').value
    assert.equal(stored.snapshot.id, sid, 'the same chunks')
    assert.equal(stored.snapshot.position, stored.position, 'named at the new position')
    assert.equal(runner.metrics().snapshots, snapshots, 'nothing written')
  })

  it('an empty state is a snapshot of no chunks, and the old chunks go', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const runner = await perMinute(queen).run({ queryId: 'empty', ...fast({ now: () => b.nowMs, snapshotMaxEntries: 1 }) })
    await until(() => runner.metrics().snapshots >= 1 && b.kvKeys(NS, 'empty#snap#').length >= 1)
    at(b, 61000)
    await until(() => b.messages('counts').length >= 1, 3000, 'the window to close on the clock')
    await until(() => b.kvGet(NS, 'empty').value.snapshot.chunks === 0 && b.kvKeys(NS, 'empty#snap#').length === 0, 3000)
    assert.equal(runner.metrics().stateKeys, 0)
  })

  it('a state of thousands of keys in many chunks comes back whole after a crash', async () => {
    const b = fakeBroker()
    const qa = fakeQueen(b, 'a')
    at(b, 1000)
    const N = 1500
    for (let i = 0; i < N; i += 50) b.push(Array.from({ length: 50 }, (_, j) => ({ queue: 'orders', partition: `k${i + j}`, data: { pad: 'x'.repeat(40) } })))
    const a = await perMinute(qa).run({ queryId: 'big', ...fast({ now: () => b.nowMs, lockTtl: '1s', snapshotMaxEntries: 100, snapshotChunkChars: 2000 }), ...quiet() })
    await until(() => a.metrics().entriesRead >= N && a.metrics().snapshots >= 1)
    await until(() => b.kvGet(NS, 'big').value.snapshot.chunks > 3, 3000, 'a snapshot of several chunks')
    crash(qa)
    const qb = fakeQueen(b, 'b')
    const second = await perMinute(qb).run({ queryId: 'big', ...fast({ now: () => b.nowMs, lockTtl: '1s' }), ...quiet() })
    await until(() => second.metrics().role === 'active', 6000)
    // The snapshot, the replay after it, and whatever was not committed yet, read live.
    await until(() => second.metrics().stateKeys === N, 6000, 'every key back')
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'flush', data: {} })
    await until(() => b.messages('counts').length >= N, 6000)
    await second.stop()
    const counts = b.messages('counts')
    assert.equal(counts.length, N, 'one result per key, none twice')
    assert.equal(new Set(counts.map((e) => e.partition)).size, N)
    assert.ok(counts.every((e) => e.payload.count === 1))
  })

  it('a record of the bucket format is read once and carried on as snapshots', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    const pos = b.push({ queue: 'orders', partition: 'c1', data: {} })
    const chainHash = perMinute(queen)._compileFeed().chainHash
    const sk = `w\u001f${MIN0}\u001fc1`
    b.kvPut(NS, 'legacy', { position: pos + 1, queryId: 'legacy', epoch: 0, buckets: 2, hash: chainHash, clock: T0 + 1000 })
    b.kvPut(NS, 'legacy#0', { e: { [sk]: { key: 'c1', start: MIN0, end: MIN0 + 60000, due: MIN0 + 60000, acc: { count: 5 } } } })
    b.kvPut(NS, 'legacy#1', { e: {} })
    const runner = await perMinute(queen).run({ queryId: 'legacy', ...fast({ now: () => b.nowMs }) })
    at(b, 2000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    await until(() => (b.kvGet(NS, 'legacy')?.value?.snapshot?.chunks ?? 0) >= 1)
    await until(() => b.kvKeys(NS, 'legacy#').every((k) => k.startsWith('legacy#snap#') || k === 'legacy#about'), 2000, 'the buckets to go')
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'c2', data: {} })
    await until(() => b.messages('counts').length >= 1)
    await runner.stop()
    assert.deepEqual(payloads(b, 'counts')[0], { count: 6 }, 'the 5 from the buckets and the new one')
  })

  it('snapshots encode and decode', () => {
    const dump = { steps: [{ name: 's', kind: 'window', entries: [['a', { x: 'é'.repeat(3000) }], ['b', [1, 2, null]]] }] }
    const { chunks, bytes } = encodeSnapshot(dump, 1024)
    assert.ok(chunks.length >= 1)
    assert.ok(bytes > 3000)
    assert.deepEqual(decodeSnapshot(chunks).steps, dump.steps)
  })
})

describe('feed state: many stateful steps', () => {
  it("a window's results feed a second window", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    b.push({ queue: 'orders', partition: 'c2', data: {} })
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 }).name('perKey')
      .keyBy(() => 'all')
      .windowTumbling({ seconds: 120 }).aggregate({ total: (v) => v.count, keys: () => 1 }).name('total')
      .to(queen.queue('totals'))
      .run({ queryId: 'two-windows', ...fast({ now: () => b.nowMs }) })
    at(b, 121000)
    b.push({ queue: 'orders', partition: 'c3', data: {} })
    await until(() => b.messages('totals').length >= 1)
    await runner.stop()
    assert.deepEqual(payloads(b, 'totals'), [{ total: 3, keys: 2 }])
    assert.equal(b.messages('totals')[0].partition, 'all')
    assert.deepEqual(runner.metrics().steps.map((s) => s.name), ['perKey', 'total'])
  })

  it("a pattern's matches feed a window, and a window's results feed a pattern", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000); b.push({ queue: 'orders', partition: 'o1', data: {} })
    at(b, 2000); b.push({ queue: 'payments', partition: 'o1', data: {} })
    at(b, 3000); b.push({ queue: 'orders', partition: 'o2', data: {} })
    at(b, 4000); b.push({ queue: 'payments', partition: 'o2', data: {} })
    at(b, 5000); b.push({ queue: 'orders', partition: 'o3', data: {} })
    const paid = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders', 'payments'] })
      .pattern({ start: (m) => m.queue === 'orders', then: (m) => m.queue === 'payments', within: '15m', onMatch: (o) => ({ order: o.partition }) })
      .keyBy(() => 'all')
      .windowTumbling({ seconds: 60 }).aggregate({ paid: () => 1 })
      .to(queen.queue('paid-per-minute'))
      .run({ queryId: 'pattern-window', ...fast({ now: () => b.nowMs }) })
    // A burst per tenant, then calm: a window's results are the pattern's messages.
    const bursts = await Stream.fromFeed(queen, { from: 'earliest', queues: ['clicks'] })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 })
      .pattern({
        start: (m) => m.data.count >= 2,
        then: (m) => m.data.count < 2,
        within: '5m',
        onMatch: (start, calm) => ({ tenant: start.partition, from: start.data.count, to: calm.data.count })
      })
      .to(queen.queue('calmed'))
      .run({ queryId: 'window-pattern', ...fast({ now: () => b.nowMs }) })
    at(b, 6000); b.push({ queue: 'clicks', partition: 't1', data: {} })
    at(b, 7000); b.push({ queue: 'clicks', partition: 't1', data: {} })
    at(b, 61000); b.push({ queue: 'clicks', partition: 't1', data: {} })
    at(b, 121000); b.push({ queue: 'clicks', partition: 'x', data: {} })
    await until(() => b.messages('paid-per-minute').length >= 1 && b.messages('calmed').length >= 1)
    await paid.stop()
    await bursts.stop()
    assert.deepEqual(payloads(b, 'paid-per-minute'), [{ paid: 2 }])
    assert.deepEqual(payloads(b, 'calmed'), [{ tenant: 't1', from: 2, to: 1 }])
  })

  it('a chain of a table join, a three-sided join and a window replays after a crash to the same results', async () => {
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders', 'payments', 'shipments'] })
      .joinTable({ queue: 'carriers', on: (m) => m.data.carrier, select: (v, row) => ({ ...v, speed: row?.speed ?? 'unknown' }) })
      .join({
        sides: { order: 'orders', paid: 'payments', sent: 'shipments' },
        within: '10m',
        // A join keeps the message's payload; keep the table-joined value instead.
        keep: (m, value) => value,
        select: (got, key) => ({ order: key, speed: got.sent.data.speed })
      })
      .keyBy(() => 'all')
      .windowTumbling({ seconds: 60 }).aggregate({ done: () => 1, fast: (v) => (v.speed === 'fast' ? 1 : 0) })
      .to(q.queue('done-per-minute'))
    const load = (b) => {
      at(b, 500); b.push({ queue: 'carriers', partition: 'dhl', data: { speed: 'fast' } })
      for (let i = 0; i < 6; i++) {
        at(b, 1000 + i * 100)
        b.push({ queue: 'shipments', partition: `o${i}`, data: { carrier: i % 2 ? 'dhl' : 'ups' } })
        b.push({ queue: 'orders', partition: `o${i}`, data: {} })
        if (i !== 5) b.push({ queue: 'payments', partition: `o${i}`, data: {} })
      }
    }
    // Once without a crash, for the expected results.
    const ref = fakeBroker()
    load(ref)
    const r = await chain(fakeQueen(ref)).run({ queryId: 'steps', ...fast({ now: () => ref.nowMs }) })
    at(ref, 61000); ref.push({ queue: 'orders', partition: 'flush', data: {} })
    await until(() => ref.messages('done-per-minute').length >= 1)
    await r.stop()
    assert.deepEqual(payloads(ref, 'done-per-minute'), [{ done: 5, fast: 2 }])
    // Again, with a crash in the middle.
    const b = fakeBroker()
    load(b)
    const qa = fakeQueen(b, 'a')
    const a = await chain(qa).run({ queryId: 'steps', ...fast({ now: () => b.nowMs, lockTtl: '1s', maxRecords: 4 }), ...quiet() })
    await until(() => a.metrics().entriesRead >= 8)
    crash(qa)
    const second = await chain(fakeQueen(b, 'b')).run({ queryId: 'steps', ...fast({ now: () => b.nowMs, lockTtl: '1s' }), ...quiet() })
    await until(() => second.metrics().role === 'active', 6000)
    at(b, 61000); b.push({ queue: 'orders', partition: 'flush', data: {} })
    await until(() => b.messages('done-per-minute').length >= 1)
    await wait(50)
    await second.stop()
    assert.deepEqual(payloads(b, 'done-per-minute'), payloads(ref, 'done-per-minute'))
  })
})

describe('feed state: stopping', () => {
  it('stop() cancels a long poll in flight, and still commits and snapshots what was done', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 0; i < 3; i++) b.push({ queue: 'orders', partition: `c${i}`, data: {} })
    const runner = await perMinute(queen).run({ queryId: 'prompt-stop', ...fast({ now: () => b.nowMs, waitMillis: 20000, commitEveryMillis: 60000 }) })
    await until(() => runner.metrics().entriesRead >= 3)
    await wait(50)
    const t0 = Date.now()
    await runner.stop()
    assert.ok(Date.now() - t0 < 1000, `stopped in ${Date.now() - t0} ms, not after the 20 s poll`)
    const stored = b.kvGet(NS, 'prompt-stop').value
    assert.ok(stored.snapshot.chunks >= 1, 'the open windows went into a snapshot')
    assert.ok(stored.position > 3)
  })
})

describe('feed state: steps and failures', () => {
  it('a later step whose function keeps failing on a result skips that result; the rest goes on', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (const p of ['c1', 'c2', 'c2']) b.push({ queue: 'orders', partition: p, data: {} })
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 })
      .keyBy((v) => { if (v.count === 1) throw new Error('a single order is refused'); return 'all' })
      .windowTumbling({ seconds: 120 }).aggregate({ sum: (v) => v.count })
      .to(queen.queue('out'))
      .run({ queryId: 'poison-step', ...fast({ now: () => b.nowMs, onPoison: 'skip' }), ...quiet() })
    // Minute 0 closes at 61 s, into the second window's first two minutes, which close at 121 s.
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'x', data: {} })
    at(b, 121000)
    b.push({ queue: 'orders', partition: 'x', data: {} })
    await until(() => b.messages('out').length >= 1)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out'), [{ sum: 2 }], 'c1 (one order) failed and was skipped; c2 counted')
    assert.ok(runner.metrics().skipped >= 1)
  })

  it("a table with initial: 'now' starts empty", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'settings', partition: 'p1', data: { currency: 'EUR' } })
    const runner = await Stream.fromFeed(queen, { queues: ['bookings'] })
      .joinTable({ queue: 'settings', initial: 'now', on: (m) => m.data.property, select: (v, row) => ({ id: v.id, currency: row?.currency ?? null }) })
      .to(queen.queue('out'))
      .run({ queryId: 'table-now', ...fast() })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 1, property: 'p1' } })
    b.push({ queue: 'settings', partition: 'p1', data: { currency: 'CHF' } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 2, property: 'p1' } })
    await until(() => b.messages('out').length >= 2)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out'), [{ id: 1, currency: null }, { id: 2, currency: 'CHF' }])
  })

  it('names steps, refuses a name in the wrong place or twice, and the feed-only steps on a queue source', async () => {
    const q = fakeQueen(fakeBroker())
    const s = Stream.fromFeed(q).windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 }).name('a')
    assert.equal(s._compileFeed().steps.find((x) => x.kind === 'window').name, 'a')
    assert.throws(() => Stream.fromFeed(q).name('a')._compileFeed(), /stateful step right before/)
    assert.throws(() => s.map((v) => v).name('b')._compileFeed(), /stateful step right before/)
    assert.throws(() => s.windowTumbling({ seconds: 60 }).aggregate({ n: () => 1 }).name('a').to(q.queue('x'))._compileFeed(), /named "a"/)
    assert.throws(() => Stream.fromFeed(q).name('bad name'), /a step name/)
    const queueSource = Stream.from({ name: 'orders', pop() {}, ack() {}, push() {} })
    await assert.rejects(queueSource.gather({ steps: ['a'], within: '1m' }).to(q.queue('x')).run({ queryId: 'qs' }), /Stream.fromFeed only/)
  })
})

describe('feed state: joins of many sides and gathers', () => {
  it('a three-sided join pairs the sides in any order, and names the missing side at the deadline', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .join({
        sides: ['orders', 'payments', 'shipments'],
        within: '15m',
        select: (got, key) => ({ order: key, sides: Object.keys(got).sort() }),
        onTimeout: (got, missing, key) => ({ order: key, missing })
      })
      .to(queen.queue('out'))
      .run({ queryId: 'join3', ...fast({ now: () => b.nowMs }) })
    at(b, 1000); b.push({ queue: 'shipments', partition: 'o1', data: {} })
    at(b, 2000); b.push({ queue: 'payments', partition: 'o1', data: {} })
    at(b, 3000); b.push({ queue: 'orders', partition: 'o2', data: {} })
    at(b, 4000); b.push({ queue: 'orders', partition: 'o1', data: {} })
    await until(() => b.messages('out').length >= 1)
    assert.deepEqual(payloads(b, 'out'), [{ order: 'o1', sides: ['orders', 'payments', 'shipments'] }])
    at(b, 3000 + 15 * 60000 + 1)
    await until(() => b.messages('out').length >= 2, 3000, 'the deadline of o2')
    await runner.stop()
    assert.deepEqual(payloads(b, 'out')[1], { order: 'o2', missing: ['payments', 'shipments'] })
  })

  it('a gather of named steps completes once; a late duplicate opens nothing', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const runner = await Stream.fromFeed(queen, { from: 'earliest' })
      .gather({
        steps: { paid: (m) => m.queue === 'payments', stocked: (m) => m.queue === 'stock' },
        key: (m) => m.data.order,
        within: '10m',
        onComplete: (got, key) => ({ order: key, ready: true }),
        onTimeout: (got, missing, key) => ({ order: key, missing })
      })
      .name('runs')
      .to(queen.queue('out'))
      .run({ queryId: 'gather-steps', ...fast({ now: () => b.nowMs }) })
    at(b, 1000); b.push({ queue: 'payments', partition: 'p', data: { order: 'A' } })
    at(b, 2000); b.push({ queue: 'stock', partition: 's', data: { order: 'A' } })
    await until(() => b.messages('out').length >= 1)
    assert.deepEqual(runner.state('runs').get('A').value.done, true, 'remembered until its deadline')
    at(b, 3000); b.push({ queue: 'payments', partition: 'p', data: { order: 'A' } })
    at(b, 1000 + 10 * 60000 + 1000)
    await wait(150)
    b.push({ queue: 'stock', partition: 's', data: { order: 'B' } })
    at(b, 1000 + 10 * 60000 + 2000 + 10 * 60000)
    await until(() => b.messages('out').length >= 2, 3000, 'the deadline of B')
    await runner.stop()
    assert.deepEqual(payloads(b, 'out'), [{ order: 'A', ready: true }, { order: 'B', missing: ['paid'] }])
  })

  it('a gather by count waits for distinct parts, and says how many are missing', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['parts'] })
      .gather({
        count: (m) => m.data.of,
        key: (m) => m.data.batch,
        within: '5m',
        onComplete: (got, key) => ({ batch: key, parts: got.map((p) => p.data.n).sort() }),
        onTimeout: (got, missing, key) => ({ batch: key, missing })
      })
      .to(queen.queue('out'))
      .run({ queryId: 'gather-count', ...fast({ now: () => b.nowMs }) })
    at(b, 1000)
    b.push({ queue: 'parts', partition: 'x', data: { batch: 'b1', of: 3, n: 1 }, transactionId: 'b1-1' })
    b.push({ queue: 'parts', partition: 'x', data: { batch: 'b1', of: 3, n: 1 }, transactionId: 'b1-1' })
    b.push({ queue: 'parts', partition: 'y', data: { batch: 'b1', of: 3, n: 2 }, transactionId: 'b1-2' })
    b.push({ queue: 'parts', partition: 'x', data: { batch: 'b2', of: 2, n: 1 }, transactionId: 'b2-1' })
    b.push({ queue: 'parts', partition: 'z', data: { batch: 'b1', of: 3, n: 3 }, transactionId: 'b1-3' })
    await until(() => b.messages('out').length >= 1)
    assert.deepEqual(payloads(b, 'out')[0], { batch: 'b1', parts: [1, 2, 3] }, 'the duplicate counted once')
    at(b, 1000 + 5 * 60000 + 1)
    await until(() => b.messages('out').length >= 2, 3000)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out')[1], { batch: 'b2', missing: 1 })
  })

  it('refuses a join of one side and a gather with both steps and count', () => {
    const q = fakeQueen(fakeBroker())
    assert.throws(() => Stream.fromFeed(q).join({ sides: ['a'], within: '1m' }), /at least two/)
    assert.throws(() => Stream.fromFeed(q).gather({ steps: ['a'], count: () => 1, within: '1m' }), /one of the two/)
    assert.throws(() => Stream.fromFeed(q).gather({ within: '1m' }), /one of the two/)
  })
})

describe('feed state: tables', () => {
  const bookings = (q, opts = {}) => Stream.fromFeed(q, { from: 'earliest', queues: ['bookings'], ...opts.source })
    .joinTable({ queue: 'settings', on: (m) => m.data.property, select: (v, row) => ({ id: v.id, currency: row ? row.currency : null }), ...opts.table })
    .to(q.queue('out'))

  it('a message sees the table as of its own position: updates before it, none after', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'settings', partition: 'p1', data: { currency: 'EUR' } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 1, property: 'p1' } })
    b.push({ queue: 'settings', partition: 'p1', data: { currency: 'CHF' } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 2, property: 'p1' } })
    // A change and a booking in one transaction: the change counts first.
    b.push([
      { queue: 'bookings', partition: 'x', data: { id: 3, property: 'p1' } },
      { queue: 'settings', partition: 'p1', data: { currency: 'GBP' } }
    ])
    b.push({ queue: 'settings', partition: 'p1', data: null })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 4, property: 'p1' } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 5, property: 'p2' } })
    const runner = await bookings(queen).run({ queryId: 'table', ...fast() })
    await until(() => b.messages('out').length >= 5)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out'), [
      { id: 1, currency: 'EUR' },
      { id: 2, currency: 'CHF' },
      { id: 3, currency: 'GBP' },
      { id: 4, currency: null },
      { id: 5, currency: null }
    ])
  })

  it("a new stream first reads the table queue from its beginning, and missing: 'drop' drops", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'settings', partition: 'p1', data: { currency: 'EUR' } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 0, property: 'p1' } })
    const runner = await bookings(queen, { source: { from: 'now' }, table: { missing: 'drop' } }).run({ queryId: 'prelude', ...fast() })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 1, property: 'p1' } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 2, property: 'nope' } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 3, property: 'p1' } })
    await until(() => b.messages('out').length >= 2)
    await wait(50)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out'), [{ id: 1, currency: 'EUR' }, { id: 3, currency: 'EUR' }], 'a row from before the start; the unknown property dropped; id 0 is before the start')
    assert.ok(runner.metrics().tableRows >= 1)
  })

  it("a where of the stream does not filter the table's rows, and the table survives a restart", async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'settings', partition: 'p1', data: { currency: 'EUR' } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 1, property: 'p1', vip: true } })
    b.push({ queue: 'bookings', partition: 'x', data: { id: 2, property: 'p1', vip: false } })
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['bookings'] })
      .where('data.vip', '=', true)
      .joinTable({ queue: 'settings', on: (m) => m.data.property, select: (v, row) => ({ id: v.id, currency: row?.currency ?? null }) })
      .to(q.queue('out'))
    const first = await chain(queen).run({ queryId: 'table-where', ...fast() })
    await until(() => b.messages('out').length >= 1)
    await first.stop()
    b.push({ queue: 'bookings', partition: 'x', data: { id: 3, property: 'p1', vip: true } })
    const second = await chain(fakeQueen(b, 'second')).run({ queryId: 'table-where', ...fast() })
    await until(() => b.messages('out').length >= 2)
    await second.stop()
    assert.deepEqual(payloads(b, 'out'), [{ id: 1, currency: 'EUR' }, { id: 3, currency: 'EUR' }])
    assert.ok(!b.feedReads.some((r) => (r.where || []).some((c) => c.field === 'data.vip')), 'the where ran here, not on the broker')
  })
})

describe('feed state: shards', () => {
  it('the same hash as the broker: FNV-1a over UTF-8', () => {
    assert.equal(fnv1a32(''), 0x811c9dc5)
    assert.equal(fnv1a32('a'), 0xe40c292c)
    assert.equal(fnv1a32('foobar'), 0xbf9cf968)
    assert.equal(fnv1a32('é'), fnv1a32(Buffer.from([0xc3, 0xa9]).toString('utf8')))
  })

  it('shards: every partition is read by one shard, each with its own position, lock and filter on the broker', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    for (let i = 0; i < 30; i++) for (let j = 0; j < 2; j++) b.push({ queue: 'orders', partition: `c${i}`, data: { i, j } })
    const group = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .map((m) => ({ ...m.data }))
      .to(queen.queue('copy'))
      .run({ queryId: 'sharded', shards: 3, ...fast() })
    await until(() => b.messages('copy').length >= 60)
    await wait(50)
    await group.stop()
    const copy = b.messages('copy')
    assert.equal(copy.length, 60, 'each message once')
    assert.equal(new Set(copy.map((e) => e.transactionId)).size, 60)
    assert.ok(copy.every((e) => e.transactionId.startsWith(outputPrefix('sharded'))), 'one prefix for every shard')
    for (let s = 0; s < 3; s++) {
      assert.ok(b.kvGet(NS, `sharded#shard${s}of3`).value.position > 1, `shard ${s} has a position`)
      assert.ok(b.feedReads.some((r) => r.shard && r.shard.count === 3 && r.shard.index === s), `shard ${s} reads its slice`)
    }
    assert.equal(b.kvGet(NS, 'sharded#layout').value.shards, 3)
    const m = group.metrics()
    assert.equal(m.shards, 3)
    assert.equal(m.outputs, 60)
    // Each shard took the partitions the hash gives it.
    for (const e of copy) assert.equal(shardOf(e.partition, 3), shardOf(`c${e.payload.i}`, 3))
  })

  it('shardBy a key: windows per tenant come out right across shards', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 0; i < 40; i++) b.push({ queue: 'clicks', partition: `p${i % 7}`, data: { tenant: `t${i % 5}` } })
    const group = await Stream.fromFeed(queen, { from: 'earliest', queues: ['clicks'] })
      .keyBy((m) => m.data.tenant)
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 })
      .map((v, ctx) => ({ tenant: ctx.key, count: v.count }))
      .to(queen.queue('per-tenant'))
      .run({ queryId: 'by-tenant', shards: 4, shardBy: (m) => m.data.tenant, ...fast({ now: () => b.nowMs }) })
    at(b, 61000)
    b.push({ queue: 'clicks', partition: 'p0', data: { tenant: 'flush' } })
    await until(() => b.messages('per-tenant').length >= 5)
    await group.stop()
    const out = payloads(b, 'per-tenant').sort((x, y) => (x.tenant < y.tenant ? -1 : 1))
    assert.deepEqual(out, [0, 1, 2, 3, 4].map((t) => ({ tenant: `t${t}`, count: 8 })))
    assert.ok(!b.feedReads.some((r) => r.shard), 'a key shard is filtered here')
  })

  it('a different shard count is refused; a reset starts the shards over', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const chain = () => Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] }).to(queen.queue('copy'))
    const g = await chain().run({ queryId: 'layout', shards: 3, ...fast() })
    await until(() => b.messages('copy').length >= 1)
    await g.stop()
    await assert.rejects(chain().run({ queryId: 'layout', shards: 2, ...fast() }), (err) => err.code === 'feed_layout_changed')
    await assert.rejects(chain().run({ queryId: 'layout', ...fast() }), (err) => err.code === 'feed_layout_changed')
    const again = await chain().run({ queryId: 'layout', shards: 2, reset: 'two', ...fast() })
    await again.stop()
    assert.equal(b.kvGet(NS, 'layout#layout').value.shards, 2)
  })

  it('a second process waits on every shard and takes them over when the first stops', async () => {
    const b = fakeBroker()
    for (let i = 0; i < 12; i++) b.push({ queue: 'orders', partition: `c${i}`, data: { n: i } })
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] }).to(q.queue('copy'))
    const qa = fakeQueen(b, 'a')
    const qb = fakeQueen(b, 'b')
    const a = await chain(qa).run({ queryId: 'standby-shards', shards: 3, ...fast() })
    const bb = await chain(qb).run({ queryId: 'standby-shards', shards: 3, ...fast() })
    await until(() => b.messages('copy').length >= 12)
    assert.equal(bb.metrics().running, 0, 'all standby')
    await a.stop()
    for (let i = 12; i < 24; i++) b.push({ queue: 'orders', partition: `c${i}`, data: { n: i } })
    await until(() => b.messages('copy').length >= 24, 6000)
    await bb.stop()
    assert.deepEqual(payloads(b, 'copy').map((p) => p.n).sort((x, y) => x - y), Array.from({ length: 24 }, (_, i) => i))
  })

  it('every shard sees the whole table', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'settings', partition: 'p1', data: { currency: 'EUR' } })
    for (let i = 0; i < 20; i++) b.push({ queue: 'bookings', partition: `b${i}`, data: { id: i, property: 'p1' } })
    const group = await Stream.fromFeed(queen, { from: 'earliest', queues: ['bookings'] })
      .joinTable({ queue: 'settings', on: (m) => m.data.property, select: (v, row) => ({ id: v.id, currency: row?.currency }) })
      .to(queen.queue('out'))
      .run({ queryId: 'shard-table', shards: 3, ...fast() })
    await until(() => b.messages('out').length >= 20)
    await group.stop()
    assert.ok(payloads(b, 'out').every((p) => p.currency === 'EUR'))
    assert.ok(b.feedReads.some((r) => r.shard && JSON.stringify(r.shard.queues) === '["bookings"]'), 'the shard splits the stream queue only')
  })
})

describe('feed state: live reads', () => {
  it('in the process: a step by name, one key or a list', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (const p of ['c1', 'c1', 'c2']) b.push({ queue: 'orders', partition: p, data: {} })
    const runner = await perMinute(queen).run({ queryId: 'live', ...fast({ now: () => b.nowMs }) })
    await until(() => runner.metrics().entriesRead >= 3)
    const one = runner.state('perMinute').get('c1')
    assert.equal(one.found, true)
    assert.deepEqual(one.value.windows.map((w) => w.value), [{ count: 2 }])
    assert.equal(runner.state('perMinute').list().items.length, 2)
    assert.equal(runner.state('perMinute').get('nope').found, false)
    assert.match(runner.state('nope').get('c1').error, /no step named/)
  })

  it('from outside, over ephemeral queues, on one runner and on shards', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b, 'runner', { ephemeral: true })
    at(b, 1000)
    for (let i = 0; i < 9; i++) b.push({ queue: 'orders', partition: `c${i % 3}`, data: {} })
    const runner = await perMinute(queen).run({ queryId: 'view-one', ...fast({ now: () => b.nowMs, serveWaitMillis: 100 }) })
    await until(() => runner.metrics().entriesRead >= 9)
    const asker = fakeQueen(b, 'asker', { ephemeral: true })
    const view = Stream.view(asker, 'view-one', { timeoutMillis: 2000 })
    const got = await view.get('perMinute', 'c1')
    assert.equal(got.found, true)
    assert.equal(got.value.windows[0].value.count, 3)
    assert.equal((await view.list('perMinute')).items.length, 3)
    const [status] = await view.status()
    assert.equal(status.role, 'active')
    assert.ok(status.metrics.entriesRead >= 9)

    const group = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 }).name('perMinute')
      .to(queen.queue('counts2'))
      .run({ queryId: 'view-shards', shards: 3, ...fast({ now: () => b.nowMs, serveWaitMillis: 100 }) })
    await until(() => group.metrics().entriesRead >= 9)
    const sharded = Stream.view(asker, 'view-shards', { timeoutMillis: 2000 })
    for (const k of ['c0', 'c1', 'c2']) {
      const r = await sharded.get('perMinute', k)
      assert.equal(r.found, true, k)
      assert.equal(r.shard, shardOf(k, 3), `${k} answered by its shard`)
    }
    assert.equal((await sharded.list('perMinute')).items.length, 3)
    assert.equal((await sharded.status()).length, 3)
  })

  it('a view of a stream nobody runs times out with the reason', async () => {
    const asker = fakeQueen(fakeBroker(), 'asker', { ephemeral: true })
    await assert.rejects(Stream.view(asker, 'nobody', { timeoutMillis: 150 }).get('s', 'k'), (err) => err.code === 'feed_view_timeout')
  })
})

describe('feed state: what the code review found', () => {
  it("a later step's deadline that passed on a tick is closed before an earlier step's result reaches it, on a replay too", async () => {
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['events'] })
      .windowTumbling({ seconds: 10 }).aggregate({ count: () => 1 })
      .keyBy(() => 'all')
      .join({ left: (m) => m.ctx.key === 'k1', right: (m) => m.ctx.key === 'k2', within: '5s', onTimeout: (m, side) => ({ side, key: m.ctx.key }) })
      .to(q.queue('out'))
    const scenario = async (crashIt) => {
      const b = fakeBroker()
      let q = fakeQueen(b, 'a')
      const opts = () => ({ queryId: 'xstep', ...fast({ now: () => b.nowMs, lockTtl: '1s', snapshotEveryMillis: 3600000 }), ...quiet() })
      at(b, 1000); b.push({ queue: 'events', partition: 'k1', data: {} })
      at(b, 11000); b.push({ queue: 'events', partition: 'k2', data: {} })
      let r = await chain(q).run(opts())
      await until(() => r.metrics().entriesRead >= 2 && r.metrics().steps[1].open === 1)
      at(b, 16000) // quiet: a tick, and step 2's deadline (15 s) passes alone
      await until(() => b.messages('out').length >= 1, 3000, 'the left timeout')
      at(b, 21000) // a tick: k2's window closes at 20 s, its result waits in step 2
      await until(() => r.metrics().steps[1].open === 1 && r.metrics().steps[0].open === 0, 3000)
      await until(() => (b.kvGet(NS, 'xstep')?.value?.clock ?? 0) >= T0 + 21000, 3000)
      if (crashIt) {
        crash(q)
        q = fakeQueen(b, 'b')
        r = await chain(q).run(opts())
        await until(() => r.metrics().role === 'active', 6000)
        assert.ok(r.metrics().replays >= 1)
      }
      at(b, 26000); b.push({ queue: 'events', partition: 'z', data: {} })
      await until(() => b.messages('out').length >= 2, 3000, 'the right timeout')
      await wait(50)
      return payloads(b, 'out')
    }
    const live = await scenario(false)
    assert.deepEqual(live, [{ side: 'left', key: 'k1' }, { side: 'right', key: 'k2' }])
    assert.deepEqual(await scenario(true), live)
  })

  it('a takeover after a stand-down keeps the state it loaded (the start-up snapshot does not come back)', async () => {
    const b = fakeBroker()
    at(b, 1000)
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 })
      .to(q.queue('counts'))
    const opts = () => ({ queryId: 'arm', ...fast({ now: () => b.nowMs, lockTtl: '1s' }), ...quiet() })
    const qa = fakeQueen(b, 'a')
    const orig = qa.http.post.bind(qa.http)
    let feedCalls = 0
    qa.http.post = async (path, body, ...rest) => {
      if (path === '/api/v1/feed' && ++feedCalls === 2) qa.http.cut = true
      return orig(path, body, ...rest)
    }
    const a = await chain(qa).run(opts())
    await until(() => a.metrics().role === 'standby', 15000, 'a stands down')
    for (let i = 0; i < 3; i++) b.push({ queue: 'orders', partition: `c${i}`, data: {} })
    const bb = await chain(fakeQueen(b, 'b')).run(opts())
    await until(() => bb.metrics().role === 'active' && bb.metrics().stateKeys === 3, 15000)
    await bb.stop()
    qa.http.cut = false
    await until(() => a.metrics().role === 'active', 20000, 'a takes over')
    await until(() => a.metrics().commits >= 1, 5000)
    await a.stop()
    const c = await chain(fakeQueen(b, 'c')).run(opts())
    assert.equal(c.metrics().stateKeys, 3)
  })

  for (const queues of [['acme.*'], ['acme.bookings', 'acme.settings']]) {
    it(`shards never split a table queue the stream's queues also name (${JSON.stringify(queues)})`, async () => {
      let bk = 0; while (shardOf(`b${bk}`, 2) !== 0) bk++
      let pr = 0; while (shardOf(`p${pr}`, 2) !== 1) pr++
      const b = fakeBroker()
      b.push({ queue: 'acme.settings', partition: `p${pr}`, data: { currency: 'EUR' } })
      b.push({ queue: 'acme.bookings', partition: `b${bk}`, data: { id: 1, property: `p${pr}` } })
      const q = fakeQueen(b)
      const g = await Stream.fromFeed(q, { from: 'earliest', queues })
        .joinTable({ queue: 'acme.settings', on: (m) => m.data.property, select: (v, row) => ({ id: v.id, currency: row ? row.currency : null }) })
        .to(q.queue('out'))
        .run({ queryId: `t-${queues.length}`, shards: 2, ...fast() })
      await until(() => b.messages('out').length >= 1)
      await wait(50)
      await g.stop()
      assert.deepEqual(payloads(b, 'out'), [{ id: 1, currency: 'EUR' }])
    })
  }

  it('a process still on the old layout commits nothing once a reset changed it, and stops', async () => {
    const b = fakeBroker()
    for (let i = 0; i < 6; i++) b.push({ queue: 'orders', partition: `c${i}`, data: { n: i } })
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] }).to(q.queue('copy'))
    // An idle shard commits its moved position now and then: then it meets the new layout too.
    const old = await chain(fakeQueen(b, 'old')).run({ queryId: 'relayout', shards: 2, ...fast({ positionCommitMillis: 50 }), ...quiet() })
    await until(() => b.messages('copy').length >= 6)
    const fresh = await chain(fakeQueen(b, 'new')).run({ queryId: 'relayout', shards: 3, reset: 'three', ...fast(), ...quiet() })
    b.push({ queue: 'orders', partition: 'c9', data: { n: 9 } })
    await until(() => old.metrics().perShard.every((m) => m.role === 'stopped'), 4000, 'the old shards to stop')
    await until(() => b.messages('copy').some((e) => e.payload.n === 9))
    await wait(100)
    await fresh.stop()
    const nines = payloads(b, 'copy').filter((p) => p.n === 9)
    assert.equal(nines.length, 1, 'the new message once: only the new layout committed it')
    assert.ok(old.metrics().perShard.some((m) => m.stopped && /layout/.test(m.stopped.error)))
  })

  it('a reset string applies to a process that took over as a standby', async () => {
    const b = fakeBroker()
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const first = await perMinute(fakeQueen(b, 'v1')).run({ queryId: 'reset-takeover', ...fast({ now: () => b.nowMs, reset: 'v1' }) })
    await until(() => first.metrics().stateKeys === 1)
    const second = await perMinute(fakeQueen(b, 'v2')).run({ queryId: 'reset-takeover', ...fast({ now: () => b.nowMs, reset: 'v2' }) })
    assert.equal(second.metrics().role, 'standby')
    await first.stop()
    await until(() => second.metrics().role === 'active', 6000)
    await until(() => b.kvGet(NS, 'reset-takeover').value.resetToken === 'v2', 3000)
    const stored = b.kvGet(NS, 'reset-takeover').value
    assert.equal(stored.epoch, 1, 'started over once')
  })

  it('a replay leaves out the messages the run before skipped, even when they would pass now', async () => {
    const b = fakeBroker()
    at(b, 1000)
    for (let i = 0; i < 4; i++) b.push({ queue: 'orders', partition: 'c1', data: { n: i } })
    let failing = true
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] })
      .map((m) => { if (failing && m.data.n === 2) throw new Error('a flaky service'); return m.data })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 })
      .to(q.queue('counts'))
    const opts = () => ({ queryId: 'skips', ...fast({ now: () => b.nowMs, lockTtl: '1s', onPoison: 'skip', snapshotEveryMillis: 3600000 }), ...quiet() })
    const qa = fakeQueen(b, 'a')
    const a = await chain(qa).run(opts())
    await until(() => a.metrics().skipped === 1 && (b.kvGet(NS, 'skips')?.value?.position ?? 0) > 4)
    assert.equal(b.kvGet(NS, 'skips').value.skips.length, 1, 'the skip is kept with the position')
    crash(qa)
    failing = false
    const second = await chain(fakeQueen(b, 'b')).run(opts())
    await until(() => second.metrics().role === 'active', 6000)
    assert.ok(second.metrics().replaySkipped >= 1)
    at(b, 61000); b.push({ queue: 'orders', partition: 'x', data: { n: 9 } })
    await until(() => b.messages('counts').length >= 1)
    await second.stop()
    assert.deepEqual(payloads(b, 'counts')[0], { count: 3 }, 'as the first run counted: the skipped message stays out')
  })

  it('a shardBy that throws meets the poison policy in shard 0 only', async () => {
    const b = fakeBroker()
    for (let i = 0; i < 6; i++) b.push({ queue: 'orders', partition: `c${i}`, data: { n: i } })
    const dlq = []
    const g = await Stream.fromFeed(fakeQueen(b), { from: 'earliest', queues: ['orders'] })
      .to(fakeQueen(b).queue('copy'))
      .run({ queryId: 'bad-shard-key', shards: 3, shardBy: (m) => { if (m.data.n === 4) throw new Error('no key'); return `k${m.data.n}` },
        ...fast({ onPoison: 'skip', onError: (e, ctx) => { if (ctx.action === 'skipped') dlq.push(ctx.shard?.index) } }) })
    await until(() => b.messages('copy').length >= 5)
    await wait(100)
    await g.stop()
    assert.deepEqual(payloads(b, 'copy').map((p) => p.n).sort(), [0, 1, 2, 3, 5])
    assert.deepEqual(dlq, [0], 'one skip, in shard 0')
  })

  it('since without from starts at the first message since then', async () => {
    const b = fakeBroker()
    at(b, 1000); b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    at(b, 5000); b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
    const r = await Stream.fromFeed(fakeQueen(b), { queues: ['orders'], since: new Date(T0 + 3000) })
      .to(fakeQueen(b).queue('copy'))
      .run({ queryId: 'since', ...fast() })
    await until(() => b.messages('copy').length >= 1)
    await wait(50)
    await r.stop()
    assert.deepEqual(payloads(b, 'copy').map((p) => p.n), [2])
  })

  it('a gather by count of results from a window counts each window once', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const runner = await Stream.fromFeed(queen, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 })
      .gather({ count: () => 2, key: (m) => m.partition, within: '10m', onComplete: (got, key) => ({ key, minutes: got.map((g) => g.data.count) }) })
      .to(queen.queue('out'))
      .run({ queryId: 'gather-windows', ...fast({ now: () => b.nowMs }) })
    at(b, 1000); b.push({ queue: 'orders', partition: 'c1', data: {} })
    at(b, 61000); b.push({ queue: 'orders', partition: 'c1', data: {} })
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    at(b, 121000); b.push({ queue: 'orders', partition: 'flush', data: {} })
    await until(() => b.messages('out').length >= 1)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out'), [{ key: 'c1', minutes: [1, 2] }])
  })
})

describe('feed state: a crash anywhere gives the same results', () => {
  it('a result reaches the next step at the time its window closed, however late a replay sees it', async () => {
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 })
      .keyBy(() => 'all')
      .windowTumbling({ seconds: 60 }).aggregate({ windows: () => 1 })
      .map((v, ctx) => ({ ...v, minute: ctx.windowKey }))
      .to(q.queue('out'))
    const b = fakeBroker()
    const qa = fakeQueen(b, 'a')
    at(b, 1000)
    b.push({ queue: 'orders', partition: 'c1', data: {} })
    const opts = (q) => ({ queryId: 'late-close', ...fast({ now: () => b.nowMs, lockTtl: '1s', snapshotEveryMillis: 3600000 }), ...quiet() })
    const a = await chain(qa).run(opts(qa))
    await until(() => a.metrics().entriesRead >= 1)
    // A quiet feed: this runner closes minute 0 on the clock (a tick), at about 61 s.
    at(b, 61000)
    await until(() => a.metrics().steps[1].open === 1, 3000, 'the first result in the second step')
    at(b, 130000)
    b.push({ queue: 'orders', partition: 'x', data: {} })
    await until(() => b.messages('out').length >= 1)
    await until(() => (b.kvGet(NS, 'late-close')?.value?.position ?? 0) > b.messages('orders').pop().position)
    crash(qa)
    // The next runner replays from the start: it sees minute 0 close only at 130 s.
    const second = await chain(fakeQueen(b, 'b')).run(opts(null))
    await until(() => second.metrics().role === 'active', 6000)
    at(b, 250000)
    b.push({ queue: 'orders', partition: 'y', data: {} })
    await until(() => b.messages('out').length >= 2, 4000)
    await wait(50)
    await second.stop()
    // c1's minute closed at 60 s (into the second step's 09:01), x's at 180 s
    // (into 09:03). A replay that dated c1's result at 130 s, when it saw the
    // close, would put it in 09:02 and send it a second time.
    assert.deepEqual(payloads(b, 'out').map((p) => p.minute), [
      new Date(MIN0 + 60000).toISOString(),
      new Date(MIN0 + 180000).toISOString()
    ], 'each result once, in the minute its window closed')
  })

  it('two windows chained, crashed at random points, send what one uninterrupted run sends', async () => {
    let seed = 7
    const rnd = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648 }
    const msgs = []
    let t = 0
    for (let i = 0; i < 160; i++) {
      t += Math.floor(rnd() * 4000)
      msgs.push({ t, partition: `p${Math.floor(rnd() * 9)}`, v: Math.floor(rnd() * 10) })
    }
    const chain = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['events'] })
      .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1, sum: (v) => v.v })
      .keyBy(() => 'all')
      .windowTumbling({ seconds: 180 }).aggregate({ windows: () => 1, total: (v) => v.sum })
      .to(q.queue('out'))
    const load = (b) => { for (const m of msgs) { at(b, m.t); b.push({ queue: 'events', partition: m.partition, data: { v: m.v } }) } }
    // Two messages far later close every window of both steps (the second
    // step's last results arrive with the first one); returns the last position.
    const flush = (b) => {
      at(b, t + 400000); b.push({ queue: 'events', partition: 'flush', data: { v: 0 } })
      at(b, t + 800000); return b.push({ queue: 'events', partition: 'flush', data: { v: 0 } })
    }

    const ref = fakeBroker()
    load(ref)
    const r = await chain(fakeQueen(ref)).run({ queryId: 'rand', ...fast({ now: () => ref.nowMs }) })
    const refEnd = flush(ref)
    await until(() => (ref.kvGet(NS, 'rand')?.value?.position ?? 0) > refEnd, 6000)
    await wait(100)
    await r.stop()
    const expected = payloads(ref, 'out')
    assert.ok(expected.length >= 3)

    const b = fakeBroker()
    load(b)
    let q = fakeQueen(b, 'p0')
    let runner = await chain(q).run({ queryId: 'rand', ...fast({ now: () => b.nowMs, lockTtl: '1s', maxRecords: 7, snapshotMaxEntries: 25 }), ...quiet() })
    for (let k = 1; k <= 3; k++) {
      const target = Math.floor(msgs.length * k / 4)
      await until(() => (b.kvGet(NS, 'rand')?.value?.position ?? 0) >= target, 6000)
      crash(q)
      q = fakeQueen(b, `p${k}`)
      runner = await chain(q).run({ queryId: 'rand', ...fast({ now: () => b.nowMs, lockTtl: '1s', maxRecords: 7, snapshotMaxEntries: 25 }), ...quiet() })
      await until(() => runner.metrics().role === 'active', 6000)
    }
    const end = flush(b)
    await until(() => (b.kvGet(NS, 'rand')?.value?.position ?? 0) > end && b.messages('out').length >= expected.length, 8000)
    await wait(100)
    await runner.stop()
    assert.deepEqual(payloads(b, 'out'), expected)
  })
})
