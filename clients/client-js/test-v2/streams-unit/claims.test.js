/**
 * Closing quiet windows under a claim (broker 2.7, issue #153).
 *
 *   - The window operators say when the earliest window still open closes:
 *     the Runner claims the partition then.
 *   - Against the fake streams server: a window a stopped runner left open is
 *     found by the next runner's sweep, claimed, and closed in a cycle that
 *     acks the claim's empty batch; a busy partition is tried again later;
 *     without the claim routes (an older broker) and for event-time windows
 *     the old unleased flush runs.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { WindowTumblingOperator } from '../../client-v2/streams/operators/WindowTumblingOperator.js'
import { WindowSlidingOperator } from '../../client-v2/streams/operators/WindowSlidingOperator.js'
import { WindowSessionOperator } from '../../client-v2/streams/operators/WindowSessionOperator.js'
import { ReduceOperator, stateKeyFor } from '../../client-v2/streams/operators/ReduceOperator.js'
import { createFakeStreamsServer, createFakeSource, fakeMessage } from './fakeServer.js'

const iso = ms => new Date(ms).toISOString()
const counter = () => new ReduceOperator((acc, _v) => acc + 1, 0)

async function annotate(window, times, key = 'k') {
  const out = []
  for (const t of times) {
    out.push(...await window.apply({ msg: { createdAt: iso(t), data: 1 }, key, value: 1 }))
  }
  return out
}

describe('next close — tumbling / sliding (ReduceOperator.run)', () => {
  it('is the earliest open window end plus grace, then the next, then none', async () => {
    const w = new WindowTumblingOperator({ seconds: 10, gracePeriod: 2 })
    const envs = await annotate(w, [5_000, 15_000])
    const run = clock => counter().run({
      envelopes: envs, loadedState: new Map(), streamTimeMs: clock,
      operatorTag: w.operatorTag, gracePeriodMs: w.gracePeriodMs
    })

    let out = await run(9_000)
    assert.equal(out.emits.length, 0)
    assert.equal(out.nextCloseMs, 12_000, 'first window: end 10 s + 2 s grace')

    // The first closed: the second is next.
    out = await run(12_000)
    assert.equal(out.emits.length, 1)
    assert.equal(out.nextCloseMs, 22_000)

    // Both closed: nothing to come back for.
    out = await run(22_000)
    assert.equal(out.emits.length, 2)
    assert.equal(out.nextCloseMs, null)
  })

  it('counts a stored window the batch did not touch', async () => {
    const loaded = new Map([[
      stateKeyFor('tumb:10', iso(10_000), 'k'),
      { acc: 1, windowStart: 10_000, windowEnd: 20_000 }
    ]])
    // No events at all: the stored window alone says when to come back.
    let out = await counter().run({
      envelopes: [], loadedState: loaded, streamTimeMs: 15_000, operatorTag: 'tumb:10', gracePeriodMs: 0
    })
    assert.equal(out.emits.length, 0)
    assert.equal(out.nextCloseMs, 20_000)

    // A later window gets an event: the stored one still closes first.
    const w = new WindowTumblingOperator({ seconds: 10 })
    out = await counter().run({
      envelopes: await annotate(w, [25_000]), loadedState: loaded, streamTimeMs: 19_000,
      operatorTag: 'tumb:10', gracePeriodMs: 0
    })
    assert.equal(out.nextCloseMs, 20_000)
  })

  it('takes the earliest of the sliding windows an event falls in', async () => {
    const w = new WindowSlidingOperator({ size: 10, slide: 5 })
    const envs = await annotate(w, [7_000])   // [0, 10 s) and [5 s, 15 s)
    const run = clock => counter().run({
      envelopes: envs, loadedState: new Map(), streamTimeMs: clock, operatorTag: w.operatorTag, gracePeriodMs: 0
    })
    let out = await run(7_000)
    assert.equal(out.nextCloseMs, 10_000)
    out = await run(10_000)
    assert.equal(out.emits.length, 1)
    assert.equal(out.nextCloseMs, 15_000)
  })
})

describe('next close — session (WindowSessionOperator.runSession)', () => {
  const runSession = (w, envelopes, loadedState, nowMs) => w.runSession({
    envelopes, loadedState, reducerFn: (acc, _v) => acc + 1, reducerInitial: () => 0, nowMs
  })

  it('is the last event plus gap and grace, and none once it closes', async () => {
    const w = new WindowSessionOperator({ gap: 30 })
    const envs = await annotate(w, [0, 10_000], 'u1')
    let out = await runSession(w, envs, new Map(), 11_000)
    assert.equal(out.emits.length, 0)
    assert.equal(out.nextCloseMs, 40_000)

    // The idle close of the stored session: nothing left open after it.
    const stored = new Map([[w.openSessionStateKey('u1'), { acc: 2, sessionStart: 0, lastEventTime: 10_000 }]])
    out = await runSession(w, [], stored, 39_999)
    assert.equal(out.emits.length, 0)
    assert.equal(out.nextCloseMs, 40_000)
    out = await runSession(w, [], stored, 40_000)
    assert.equal(out.emits.length, 1)
    assert.equal(out.nextCloseMs, null)
  })

  it('adds the grace, and takes the earliest of several keys', async () => {
    const w = new WindowSessionOperator({ gap: 30, gracePeriod: 5 })
    const envs = [
      ...await annotate(w, [1_000], 'a'),
      ...await annotate(w, [4_000], 'b')
    ]
    const out = await runSession(w, envs, new Map(), 4_000)
    assert.equal(out.nextCloseMs, 1_000 + 30_000 + 5_000)
  })
})

// ---------------------------------------------------------------- Runner

const PART_A = '101'

function windowedStream(source, { eventTime } = {}) {
  return Stream
    .from(source)
    .windowTumbling({ seconds: 1, idleFlushMs: 100, ...(eventTime ? { eventTime } : {}) })
    .aggregate({ count: () => 1 })
    .to({ queueName: 'sink' })
}

async function waitUntil(fn, timeoutMs = 5000) {
  const start = Date.now()
  while (!fn() && Date.now() - start < timeoutMs) {
    await new Promise(r => setTimeout(r, 20))
  }
  return fn()
}

const closingCycle = c => c.ack && c.ack.count === 0 && (c.push_items || []).length > 0

describe('Runner — closes under a claim (fake server)', () => {
  it('a window left open by a stopped runner is swept, claimed and closed with a count-0 ack', async () => {
    const server = await createFakeStreamsServer({ claims: { names: { [PART_A]: 'cust-1' } } })
    try {
      // Runner 1 folds one event, and stops while its 1 s window is open
      // (stamped a second ahead, so the window cannot end before the stop).
      const first = await windowedStream(createFakeSource('src', [[
        fakeMessage({ partitionId: PART_A, partitionName: 'cust-1', data: { n: 1 }, createdAt: iso(Date.now() + 1000) })
      ]])).run({ queryId: 'claim-restart', url: server.url })
      assert.ok(await waitUntil(() => server.recorded.cycles.some(c => c.ack && c.ack.count === 1)), 'runner 1 committed its cycle')
      await first.stop()
      assert.equal(server.recorded.cycles.filter(closingCycle).length, 0, 'runner 1 closed nothing')

      // Runner 2, same query, no new events.
      const second = await windowedStream(createFakeSource('src', [])).run({ queryId: 'claim-restart', url: server.url })
      const closed = await waitUntil(() => server.recorded.cycles.find(closingCycle), 5000)
      await second.stop()
      assert.ok(closed, 'runner 2 closed the window')

      // Its first sweep took every partition holding state.
      const sweeps = server.recorded.statePartitions
      assert.ok(sweeps.length >= 2, 'each runner sweeps at start')
      assert.equal(sweeps[sweeps.length - 1].untouched_for_ms, undefined)

      const claim = server.recorded.claims.find(c => c.leaseId === closed.ack.leaseId)
      assert.ok(claim, 'the close names the lease of a claim')
      assert.deepEqual(claim.request.partition_ids, [PART_A])
      assert.equal(claim.request.consumer_group, 'streams.claim-restart')
      assert.equal(claim.request.lease_seconds, 30)

      assert.equal(closed.partition_id, PART_A)
      assert.equal(closed.release_lease, true)
      assert.deepEqual(closed.ack, { transactionId: '', leaseId: claim.leaseId, status: 'completed', count: 0 })
      assert.equal(closed.push_items.length, 1)
      assert.equal(closed.push_items[0].partition, 'cust-1')
      assert.deepEqual(closed.push_items[0].payload, { count: 1 })
      assert.deepEqual(closed.state_ops.map(o => o.type), ['delete'])
      assert.equal(server.recorded.cycles.filter(closingCycle).length, 1, 'closed once')
      assert.equal(second.metrics().flushCyclesTotal, 1)
    } finally {
      await server.close()
    }
  })

  it('a busy partition is claimed again no sooner than a second later', async () => {
    let calls = 0
    const server = await createFakeStreamsServer({
      claims: { status: () => (++calls === 1 ? 'busy' : 'claimed') }
    })
    try {
      // The window is already past its end: due at the first tick.
      const old = Date.now() - 5_000
      const handle = await windowedStream(createFakeSource('src', [[
        fakeMessage({ partitionId: PART_A, partitionName: 'cust-1', data: { n: 1 }, createdAt: iso(old) })
      ]])).run({ queryId: 'claim-busy', url: server.url })
      const closed = await waitUntil(() => server.recorded.cycles.find(closingCycle), 5000)
      await handle.stop()
      assert.ok(closed, 'closed once the partition was free')
      const claims = server.recorded.claims.filter(c => c.request.partition_ids.includes(PART_A))
      assert.ok(claims.length >= 2)
      assert.ok(claims[1].at - claims[0].at >= 900, `retried after ${claims[1].at - claims[0].at} ms`)
      // The claim answered no name: the one the cycle saw is used.
      assert.equal(closed.push_items[0].partition, 'cust-1')
    } finally {
      await server.close()
    }
  })

  it('falls back to the unleased flush on a broker without the claim routes', async () => {
    const server = await createFakeStreamsServer()   // the claim routes answer 404
    try {
      const handle = await windowedStream(createFakeSource('src', [[
        fakeMessage({ partitionId: PART_A, partitionName: 'cust-1', data: { n: 1 }, createdAt: iso(Date.now()) })
      ]])).run({ queryId: 'claim-old-broker', url: server.url })
      const flushed = await waitUntil(() => server.recorded.cycles.find(c => c.ack === null && c.push_items.length > 0), 5000)
      await handle.stop()
      assert.ok(flushed, 'the old flush closed the window, with no ack')
      assert.equal(handle._claims, 'no')
    } finally {
      await server.close()
    }
  })

  it('event-time windows keep the old flush and never claim', async () => {
    const server = await createFakeStreamsServer({ claims: {} })
    try {
      const handle = await windowedStream(createFakeSource('src', [[
        fakeMessage({ partitionId: PART_A, partitionName: 'cust-1', data: { ts: 1_000 } })
      ]]), { eventTime: m => m.data.ts }).run({ queryId: 'claim-event-time', url: server.url })
      assert.ok(await waitUntil(() => server.recorded.cycles.length === 1), 'the cycle committed')
      await new Promise(r => setTimeout(r, 300))
      await handle.stop()
      assert.equal(server.recorded.statePartitions.length, 0)
      assert.equal(server.recorded.claims.length, 0)
    } finally {
      await server.close()
    }
  })
})
