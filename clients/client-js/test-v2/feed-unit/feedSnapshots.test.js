// Snapshots in layers and pieces, and counts kept as the state changes:
// a snapshot costs what changed, no string ever holds the whole state, the
// process keeps turning while a big layer is written, and a metric costs the
// same at any size.
import { describe, it, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import crypto from 'node:crypto'

import { Stream } from '../../client-v2/streams/Stream.js'
import { POSITION_NAMESPACE as NS } from '../../client-v2/streams/runtime/FeedRunner.js'
import { FeedState } from '../../client-v2/streams/runtime/feed/state.js'
import { Pipeline } from '../../client-v2/streams/runtime/feed/pipeline.js'
import {
  writeLayer, readLayer, encodeSnapshot, layersOf, chunkKey, newSnapshotId
} from '../../client-v2/streams/runtime/feed/snapshot.js'
import { fakeBroker, fakeQueen, until } from './fakeBroker.js'

const T0 = Date.parse('2026-11-06T09:00:00.000Z')
const fast = (o = {}) => ({ waitMillis: 20, commitEveryMillis: 0, retryDelayMillis: 1, ...o })
const at = (b, ms) => { b.nowMs = T0 + ms }

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

const sidOf = (prefix, key) => key.slice(prefix.length, key.lastIndexOf('#'))

describe('feed state: counts kept as it changes', () => {
  it('keeps its class counts through sets, deletes, rollbacks and loads in pieces', () => {
    const classify = (k, v) => (k.startsWith('b') ? 'held' : v && !v.done ? 'open' : null)
    const st = new FeedState()
    st.classifyWith(classify)
    const scan = (c) => {
      let n = 0
      for (const [k, v] of st.entries()) if (classify(k, v) === c) n++
      return n
    }
    let seed = 7
    const rnd = (n) => (seed = (seed * 1103515245 + 12345) % 2147483648) % n
    for (let round = 0; round < 300; round++) {
      const mark = st.mark()
      for (let i = 0; i < 20; i++) {
        const k = `${rnd(2) ? 'b' : 'c'}${rnd(30)}`
        if (rnd(3) === 0) st.delete(k)
        else st.set(k, { done: rnd(4) === 0 })
      }
      if (rnd(2)) st.rollbackTo(mark)
      else st.committed()
      assert.equal(st.count('open'), scan('open'), `open, round ${round}`)
      assert.equal(st.count('held'), scan('held'), `held, round ${round}`)
    }
    const entries = [...st.entries()]
    st.loadBegin()
    for (const [k, v] of entries) st.loadSet(k, v)
    st.loadDelete(entries[0][0])
    st.loaded()
    assert.equal(st.count('open'), scan('open'))
    assert.equal(st.count('held'), scan('held'))
    assert.equal(st.touchedSinceSnapshot, 0, 'a load leaves nothing to write')
    st.wipe()
    assert.equal(st.count('open'), 0)
  })

  it("a pipeline's metrics cost the same with 200,000 open windows", async () => {
    const q = fakeQueen(fakeBroker())
    const { steps } = perMinute(q)._compileFeed()
    const p = new Pipeline(steps)
    const N = 200000
    p.loadBegin()
    const set = []
    for (let i = 0; i < N; i++) set.push([`w\u001f0\u001fk${i}`, { key: `k${i}`, start: 0, end: 60000, due: 60000, acc: { count: 1 } }])
    p.loadPiece(0, 'window', set, [])
    await p.loadEnd(null)
    const t0 = performance.now()
    let open = 0
    for (let i = 0; i < 2000; i++) {
      open = p.open
      p.stepMetrics(0)
    }
    const ms = performance.now() - t0
    assert.equal(open, N)
    assert.ok(ms < 300, `2,000 reads of the metrics took ${ms.toFixed(0)} ms`)
  })
})

describe('feed state: millions due at once', () => {
  const loaded = async (n, due) => {
    const q = fakeQueen(fakeBroker())
    const { steps } = perMinute(q)._compileFeed()
    const p = new Pipeline(steps)
    p.loadBegin()
    const set = []
    for (let i = 0; i < n; i++) set.push([`w\u001f0\u001fk${String(i).padStart(7, '0')}`, { key: `k${i}`, start: 0, end: due, due, acc: { count: 1 } }])
    p.loadPiece(0, 'window', set, [])
    await p.loadEnd(null)
    return p
  }
  const gaps = () => {
    const g = { max: 0, last: performance.now() }
    g.timer = setInterval(() => { const now = performance.now(); g.max = Math.max(g.max, now - g.last); g.last = now }, 5)
    return g
  }

  it('100,000 windows that end at one time close in batches, in key order, with the event loop turning', async () => {
    const p = await loaded(100000, 60000)
    const order = []
    const g = gaps()
    const r = await p.advance(60000, async (em) => { order.push(em.key); return 'ok' })
    clearInterval(g.timer)
    assert.equal(r, 'ok')
    assert.equal(order.length, 100000)
    assert.equal(p.open, 0)
    // One order every time, whatever the batches: the heap's (due, state key), here k0, k1, k2, ...
    assert.ok(order.every((k, i) => k === `k${i}`), 'the closes keep the heap order across batches')
    assert.ok(g.max < 200, `the event loop stood still for ${g.max.toFixed(0)} ms`)
  })

  it('a loaded state of 500,000 entries is indexed again in batches', async () => {
    const g = gaps()
    const p = await loaded(500000, 60000)
    clearInterval(g.timer)
    assert.equal(p.open, 500000)
    assert.ok(g.max < 400, `the event loop stood still for ${g.max.toFixed(0)} ms`)
  })
})

describe('feed state: a long close commits in pieces', () => {
  const crash = (q) => { q.http.cut = true }
  const quiet = (o = {}) => ({ onError: () => {}, ...o })

  it('12,000 windows that end at one time commit in pieces, and each result goes out once', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    const N = 12000
    for (let i = 0; i < N; i++) b.push({ queue: 'orders', partition: `c${i}`, data: {} })
    const runner = await perMinute(queen).run({ queryId: 'burst', ...fast({ now: () => b.nowMs, maxPendingOutputs: 1000, snapshotEveryMillis: 3600000 }) })
    await until(() => runner.metrics().entriesRead >= N, 15000)
    at(b, 61000)
    b.push({ queue: 'orders', partition: 'late', data: {} })
    await until(() => b.messages('counts').length >= N, 30000, 'every window closed')
    await runner.stop()
    const keys = b.messages('counts').map((e) => e.partition)
    assert.equal(keys.length, N)
    assert.equal(new Set(keys).size, N, 'no result twice')
    // A commit comes after a batch of closes (2,000) once 1,000 results wait: six for 12,000.
    assert.ok(runner.metrics().closeCommits >= 5, `commits in the middle of the close (${runner.metrics().closeCommits})`)
    assert.equal(b.kvGet(NS, 'burst').value.units, N, 'the record counts the closes')
  })

  for (const [what, snapshotEveryMillis] of [['read again from the start', 3600000], ['snapshots in the middle of the close', 1]]) {
    it(`a crash in the middle of a long close (${what}): the next process closes the rest, each result once`, async () => {
      const b = fakeBroker()
      const qa = fakeQueen(b, 'a')
      at(b, 1000)
      const N = 20000
      for (let i = 0; i < N; i++) b.push({ queue: 'orders', partition: `c${i}`, data: {} })
      const opts = () => fast({ now: () => b.nowMs, lockTtl: '1s', maxPendingOutputs: 400, snapshotEveryMillis })
      const a = await perMinute(qa).run({ queryId: 'burstcrash', ...opts(), ...quiet() })
      await until(() => a.metrics().entriesRead >= N, 15000)
      at(b, 61000)
      b.push({ queue: 'orders', partition: 'late', data: {} })
      await until(() => a.metrics().closeCommits >= 3, 15000, 'commits in the middle of the close')
      crash(qa)
      await new Promise((r) => setTimeout(r, 50))
      const stored = b.kvGet(NS, 'burstcrash').value
      assert.ok(stored.units > 0 && stored.units < N, `the record names how far the close got (${stored.units} of ${N})`)
      const sent = b.messages('counts').length
      assert.equal(sent, stored.units, 'what is committed is what the record counts')
      const qb = fakeQueen(b, 'b')
      const second = await perMinute(qb).run({ queryId: 'burstcrash', ...opts(), ...quiet() })
      await until(() => second.metrics().role === 'active', 8000, 'the takeover')
      await until(() => b.messages('counts').length >= N, 30000, 'the rest of the close')
      await second.stop()
      const keys = b.messages('counts').map((e) => e.partition)
      assert.equal(keys.length, N, 'every window once')
      assert.equal(new Set(keys).size, N, 'no result twice')
      assert.equal(b.kvGet(NS, 'burstcrash').value.units, N)
    })
  }
})

describe('feed state: snapshots in pieces and layers', () => {
  it('a big layer is written in pieces while the event loop keeps turning, and reads back whole', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const N = 200000
    function * entries() {
      for (let i = 0; i < N; i++) yield [`w\u001f0\u001fk${i}`, { key: `k${i}`, start: 0, end: 60000, due: 60000, acc: { count: i % 7 } }]
    }
    let maxGap = 0
    let last = performance.now()
    const timer = setInterval(() => {
      const now = performance.now()
      maxGap = Math.max(maxGap, now - last)
      last = now
    }, 5)
    const w = await writeLayer({ queen, ns: NS, id: 'big', sid: 's1', lock: null, steps: [{ kind: 'window', entries: entries() }] })
    clearInterval(timer)
    assert.equal(w.entries, N)
    assert.ok(w.chunks > 10, `many chunks (${w.chunks})`)
    assert.ok(maxGap < 200, `the event loop stood still for ${maxGap.toFixed(0)} ms while the layer was written`)
    const got = new Map()
    let pieces = 0
    await readLayer({
      queen, ns: NS, id: 'big', sid: 's1', count: w.chunks,
      onPiece: (pc) => { pieces++; for (const [k, v] of pc.set) got.set(k, v) }
    })
    assert.ok(pieces > 10, `read back piece by piece (${pieces})`)
    assert.equal(got.size, N)
    assert.deepEqual(got.get('w\u001f0\u001fk12345'), { key: 'k12345', start: 0, end: 60000, due: 60000, acc: { count: 12345 % 7 } })
  })

  it('a piece bigger than one KV value goes on in the next one; deletes are read back', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    // Hex of hashes: deflate saves about half of it, so the one piece spans several chunks.
    const big = Array.from({ length: 64 }, (_, i) => crypto.createHash('sha256').update(String(i)).digest('hex')).join('')
    const list = [['a', { big, n: 1 }], ['gone', undefined], ['b', { n: 2 }]]
    const w = await writeLayer({ queen, ns: NS, id: 'small', sid: 's1', lock: null, steps: [{ kind: 'pattern', entries: list }], chunkChars: 1024, pieceBytes: 1 << 20 })
    assert.ok(w.chunks >= 2, `the piece spans several chunks (${w.chunks})`)
    for (let n = 0; n < w.chunks; n++) {
      const v = b.kvGet(NS, chunkKey('small', 's1', n)).value
      assert.ok(v.d.length <= 1024)
      assert.equal(v.m, n < w.chunks - 1 ? 1 : undefined, n < w.chunks - 1 ? `chunk ${n} says more follows` : 'the last chunk ends the piece')
    }
    const seen = []
    await readLayer({ queen, ns: NS, id: 'small', sid: 's1', count: w.chunks, onPiece: (pc) => seen.push(pc) })
    assert.equal(seen.length, 1)
    assert.deepEqual(seen[0].set, [['a', { big, n: 1 }], ['b', { n: 2 }]])
    assert.deepEqual(seen[0].del, ['gone'])
    assert.equal(seen[0].k, 'pattern')
  })

  it('the piece limit counts UTF-8 bytes, not characters', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    // 200 characters, 600 bytes in UTF-8: counted in characters, a piece would hold three times the limit.
    const text = '漢字'.repeat(100)
    const list = Array.from({ length: 12 }, (_, i) => [`k${i}`, { text }])
    const room = 2048
    const w = await writeLayer({ queen, ns: NS, id: 'utf8', sid: 's1', lock: null, steps: [{ kind: 'window', entries: list }], pieceBytes: room })
    const pieces = []
    await readLayer({ queen, ns: NS, id: 'utf8', sid: 's1', count: w.chunks, onPiece: (pc) => pieces.push(pc) })
    assert.deepEqual(pieces.flatMap((pc) => pc.set), list)
    assert.ok(pieces.length >= 3, `${pieces.length} pieces`)
    for (const pc of pieces) {
      // A piece ends at the entry that takes it to the limit: the entries before that one stay under it.
      const before = pc.set.slice(0, -1).reduce((n, e) => n + Buffer.byteLength(JSON.stringify(e)), 0)
      assert.ok(before < room, `${before} bytes before the last entry of a piece (limit ${room})`)
    }
  })

  it('a wait that matched after the full layer is a delete in the next delta, and does not come back', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 0; i < 20; i++) b.push({ queue: 'orders', partition: `o${i}`, data: { start: true } })
    const waits = (q) => Stream.fromFeed(q, { from: 'earliest', queues: ['orders'] })
      .pattern({ start: (m) => m.data.start === true, then: (m) => m.data.paid === true, within: '1h', onMatch: (s) => ({ paid: s.partition }) })
      .to(q.queue('paid'))
    const runner = await waits(queen).run({ queryId: 'dels', ...fast({ now: () => b.nowMs, snapshotMaxEntries: 1 }) })
    await until(() => runner.metrics().entriesRead >= 20 && runner.metrics().snapshots >= 1)
    const base = b.kvGet(NS, 'dels').value.snapshot.layers[0]
    b.push({ queue: 'orders', partition: 'o3', data: { paid: true } })
    b.push({ queue: 'orders', partition: 'o7', data: { paid: true } })
    await until(() => b.messages('paid').length >= 2 && (b.kvGet(NS, 'dels').value.snapshot.layers || []).length >= 2)
    await runner.stop()
    const snap = b.kvGet(NS, 'dels').value.snapshot
    assert.equal(snap.layers[0].sid, base.sid, 'the full layer stays under the deltas')
    const deleted = []
    for (const l of snap.layers.slice(1)) {
      await readLayer({ queen, ns: NS, id: 'dels', sid: l.sid, count: l.chunks, onPiece: (pc) => deleted.push(...pc.del) })
    }
    assert.equal(deleted.filter((k) => k.endsWith('o3') || k.endsWith('o7')).length, 2, `the two matched waits are deletes (${JSON.stringify(deleted)})`)
    const again = await waits(fakeQueen(b, 'again')).run({ queryId: 'dels', ...fast({ now: () => b.nowMs }) })
    assert.equal(again.metrics().open, 18, 'eighteen waits came back, not the two that matched')
    b.push({ queue: 'orders', partition: 'o3', data: { paid: true } })
    b.push({ queue: 'orders', partition: 'o4', data: { paid: true } })
    await until(() => b.messages('paid').length >= 3)
    await again.stop()
    assert.deepEqual(b.messages('paid').map((e) => e.payload.paid).sort(), ['o3', 'o4', 'o7'], 'o3 matched once, o4 matched now')
  })

  it('when the deltas weigh as much as the full layer, a full layer replaces them and their chunks go', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 0; i < 4; i++) b.push({ queue: 'orders', partition: `c${i}`, data: {} })
    const runner = await perMinute(queen).run({ queryId: 'compact', ...fast({ now: () => b.nowMs, snapshotMaxEntries: 1 }) })
    await until(() => runner.metrics().snapshots >= 1 && runner.metrics().entriesRead >= 4)
    const firstBase = b.kvGet(NS, 'compact').value.snapshot.layers[0].sid
    let sawDeltas = 0
    for (let i = 4; i < 40; i++) {
      b.push({ queue: 'orders', partition: `c${i}`, data: {} })
      await until(() => runner.metrics().entriesRead >= i + 1)
      sawDeltas = Math.max(sawDeltas, (b.kvGet(NS, 'compact').value.snapshot.layers || []).length - 1)
    }
    await until(() => {
      const s = b.kvGet(NS, 'compact').value.snapshot
      return s.layers[0].sid !== firstBase && s.layers[0].full
    }, 3000, 'a new full layer')
    await runner.stop()
    assert.ok(sawDeltas >= 1, 'deltas came before it')
    assert.ok(runner.metrics().snapshotsFull >= 2, 'the first full layer and at least one more')
    const snap = b.kvGet(NS, 'compact').value.snapshot
    const named = new Set(snap.layers.map((l) => l.sid))
    const keys = b.kvKeys(NS, 'compact#snap#')
    assert.ok(keys.every((k) => named.has(sidOf('compact#snap#', k))), 'the replaced layers are gone from KV')
    assert.ok(!keys.some((k) => sidOf('compact#snap#', k) === firstBase), 'the first full layer is gone')
    const again = await perMinute(fakeQueen(b, 'again')).run({ queryId: 'compact', ...fast({ now: () => b.nowMs }) })
    assert.equal(again.metrics().stateKeys, 40, 'all forty windows came back')
  })

  it('a snapshot of format 1 (one JSON) still loads, and the next one is a full layer of format 2', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    at(b, 1000)
    for (let i = 0; i < 3; i++) b.push({ queue: 'orders', partition: `c${i}`, data: {} })
    const first = await perMinute(queen).run({ queryId: 'old', ...fast({ now: () => b.nowMs }) })
    await until(() => first.metrics().entriesRead >= 3)
    await first.stop()
    // Rewrite the stored snapshot the way a runner of format 1 wrote it.
    const rec = b.kvGet(NS, 'old').value
    const entries = []
    for (const l of layersOf(rec.snapshot)) {
      await readLayer({ queen, ns: NS, id: 'old', sid: l.sid, count: l.chunks, onPiece: (pc) => entries.push(...pc.set) })
    }
    for (const k of b.kvKeys(NS, 'old#snap#')) b.kv.delete(`${NS}\u0000${k}`)
    const { chunks, bytes } = encodeSnapshot({ steps: [{ name: 'perMinute', kind: 'window', entries }] })
    const sid = newSnapshotId()
    chunks.forEach((d, n) => b.kvPut(NS, chunkKey('old', sid, n), { d }))
    b.kvPut(NS, 'old', { ...rec, snapshot: { id: sid, chunks: chunks.length, bytes, position: rec.snapshot.position, clock: rec.snapshot.clock, metas: rec.snapshot.metas } })
    const again = await perMinute(fakeQueen(b, 'again')).run({ queryId: 'old', ...fast({ now: () => b.nowMs, snapshotMaxEntries: 1 }) })
    assert.equal(again.metrics().stateKeys, 3, 'the three windows of the old snapshot came back')
    b.push({ queue: 'orders', partition: 'c9', data: {} })
    await until(() => (b.kvGet(NS, 'old').value.snapshot.layers || []).length === 1)
    await again.stop()
    const snap = b.kvGet(NS, 'old').value.snapshot
    assert.ok(snap.layers[0].full, 'a full layer of format 2')
    assert.ok(!b.kvKeys(NS, 'old#snap#').some((k) => sidOf('old#snap#', k) === sid), 'the old chunks are gone')
  })
})
