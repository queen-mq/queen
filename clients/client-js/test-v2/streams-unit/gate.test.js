/**
 * .gate() settles SOURCE MESSAGES, not envelopes.
 *
 * The gate's partial ack is an offset commit: a 2.x broker advances the
 * cursor `ack.count` messages from the head of the leased batch and keeps the
 * lease (release_lease=false). A pre-stage breaks the one-to-one map between
 * messages and the envelopes the gate sees -- `.filter()` leaves a message
 * with none, `.flatMap()` with several -- and the runner counted envelopes:
 *
 *   flatMap x2, message 2 denied   count 2 = messages 1 AND 2 acked; the
 *                                  denied message was never redelivered
 *                                  (reproduced live on 2.0.0-beta.6: 4 of 6
 *                                  sink items, message 2 lost)
 *   filter drops every message     nothing committed; the batch came back on
 *                                  every lease expiry, forever
 *
 * Same fix as the Rust client (clients/client-rust/src/streams/runner.rs,
 * gate_cycle): a message is settled only when every envelope it produced is
 * allowed, and what a denied message did to the state is rolled back.
 */

import { describe, it, before, after } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { createFakeStreamsServer, createFakeSource, fakeMessage } from './fakeServer.js'

const PART = '00000000-0000-0000-0000-00000000ga7e'

const batch = (...ns) => ns.map((n, i) => fakeMessage({
  partitionId: PART,
  partitionName: 'tenant-1',
  data: { n },
  createdAt: `2026-10-02T10:00:0${i}.000Z`
}))

const quiet = { info() {}, warn() {}, error() {}, debug() {} }

// Run until `cycles` cycles were committed (or the time is up), then stop.
async function runFor(server, stream, { cycles = 1, timeoutMs = 1500, queryId }) {
  const baseline = server.recorded.cycles.length
  const handle = await stream.run({ queryId, url: server.url, logger: quiet })
  const start = Date.now()
  while (server.recorded.cycles.length - baseline < cycles && Date.now() - start < timeoutMs) {
    await new Promise(r => setTimeout(r, 20))
  }
  await handle.stop()
  return { handle, cycles: server.recorded.cycles.slice(baseline) }
}

describe('.gate() — the partial ack counts source messages', () => {
  let server
  before(async () => { server = await createFakeStreamsServer() })
  after(async () => { await server.close() })

  it('flatMap before the gate: a denied message is not acked with its predecessor', async () => {
    const msgs = batch(1, 2, 3)
    const stream = Stream.from(createFakeSource('src', [msgs]))
      .flatMap(m => [m.data, m.data])
      .gate(v => v.n !== 2)
      .to({ _queueName: 'sink' })

    const { cycles } = await runFor(server, stream, { queryId: 'gate.flatmap' })
    assert.equal(cycles.length, 1)
    const c = cycles[0]
    assert.equal(c.ack.count, 1, 'one MESSAGE settled; two envelopes acked messages 1 and 2')
    assert.equal(c.ack.transactionId, msgs[0].transactionId)
    assert.equal(c.release_lease, false, 'the denied tail stays leased, in order')
    assert.deepEqual(c.push_items.map(p => p.payload.n), [1, 1], 'only the settled message reaches the sink')
  })

  it('flatMap before the gate: a message half allowed is not settled at all', async () => {
    let seen = 0
    const stream = Stream.from(createFakeSource('src', [batch(1, 2)]))
      .flatMap(m => [m.data, m.data])
      .gate(() => ++seen === 1)     // the first value of message 1 passes, its second does not

    const { cycles } = await runFor(server, stream, { queryId: 'gate.half', timeoutMs: 400 })
    assert.equal(cycles.length, 0, 'nothing settled, nothing committed: the lease brings both back')
  })

  it('filter before the gate: dropped messages are settled with the prefix', async () => {
    const msgs = batch(1, 2, 3)
    const stream = Stream.from(createFakeSource('src', [msgs]))
      .filter(m => m.data.n === 3)
      .gate(() => true)
      .to({ _queueName: 'sink' })

    const { cycles } = await runFor(server, stream, { queryId: 'gate.filter' })
    assert.equal(cycles.length, 1)
    assert.equal(cycles[0].ack.count, 3, 'all three messages are done; one envelope is not one message')
    assert.equal(cycles[0].ack.transactionId, msgs[2].transactionId)
    assert.equal(cycles[0].release_lease, true)
    assert.deepEqual(cycles[0].push_items.map(p => p.payload.n), [3])
  })

  it('filter that drops the whole batch: the batch is acked, not held forever', async () => {
    const msgs = batch(1, 2)
    const stream = Stream.from(createFakeSource('src', [msgs]))
      .filter(() => false)
      .gate(() => true)
      .to({ _queueName: 'sink' })

    const { cycles } = await runFor(server, stream, { queryId: 'gate.allfiltered' })
    assert.equal(cycles.length, 1, 'a cycle is committed even though the gate saw nothing')
    assert.equal(cycles[0].ack.count, 2)
    assert.equal(cycles[0].release_lease, true)
    assert.equal(cycles[0].push_items.length, 0)
    assert.equal(cycles[0].state_ops.length, 0)
  })

  it('filter, then a deny: the count stops before the denied message', async () => {
    const msgs = batch(1, 2, 3, 4)
    const stream = Stream.from(createFakeSource('src', [msgs]))
      .filter(m => m.data.n !== 2)      // message 2 produces nothing
      .gate(v => v.n !== 3)             // message 3 is denied

    const { cycles } = await runFor(server, stream, { queryId: 'gate.filterdeny' })
    assert.equal(cycles.length, 1)
    assert.equal(cycles[0].ack.count, 2, 'messages 1 and 2 settled; the gate allowed one envelope')
    assert.equal(cycles[0].ack.transactionId, msgs[1].transactionId)
    assert.equal(cycles[0].release_lease, false)
  })

  it('a denied first message still commits nothing', async () => {
    const stream = Stream.from(createFakeSource('src', [batch(1, 2)]))
      .gate(() => false)
    const { cycles, handle } = await runFor(server, stream, { queryId: 'gate.firstdeny', timeoutMs: 400 })
    assert.equal(cycles.length, 0)
    assert.equal(handle.metrics().gateDenialsTotal, 2, 'denials are counted in messages')
  })
})

describe('.gate() — a denied message did not happen', () => {
  let server
  before(async () => { server = await createFakeStreamsServer() })
  after(async () => { await server.close() })

  it('what a denied message did to the state is not persisted', async () => {
    // A counter the gate bumps BEFORE deciding: message 1 is allowed at 1,
    // message 2 bumps it to 2 and is denied. The state that commits is 1.
    const stream = Stream.from(createFakeSource('src', [batch(1, 2)]))
      .gate((v, ctx) => {
        ctx.state.count = (ctx.state.count || 0) + 1
        return v.n === 1
      })

    const { cycles } = await runFor(server, stream, { queryId: 'gate.rollback' })
    assert.equal(cycles.length, 1)
    assert.equal(cycles[0].ack.count, 1)
    assert.deepEqual(cycles[0].state_ops, [{ type: 'upsert', key: PART, value: { count: 1 } }])
  })

  it('a key only a denied message touched is not written at all', async () => {
    const stream = Stream.from(createFakeSource('src', [batch(1, 2)]))
      .keyBy(m => `k${m.data.n}`)
      .gate((v, ctx) => {
        ctx.state.seen = true
        return v.n === 1
      })

    const { cycles } = await runFor(server, stream, { queryId: 'gate.untouched' })
    assert.equal(cycles.length, 1)
    assert.deepEqual(cycles[0].state_ops.map(op => op.key), ['k1'])
  })
})

describe('.gate() — the prefix is the broker\'s, in offset order', () => {
  let server
  before(async () => { server = await createFakeStreamsServer() })
  after(async () => { await server.close() })

  it('orders by offset when the broker sends one, not by (createdAt, id)', async () => {
    // Two producers pushed in the same millisecond: same createdAt, and the
    // message the broker stored FIRST carries the higher id. Sorting by
    // (createdAt, id) put it second, so a deny on it acked it anyway.
    const createdAt = '2026-10-02T10:00:00.000Z'
    const first = { ...fakeMessage({ partitionId: PART, data: { n: 'first' }, createdAt }), id: 'msg-z', offset: 10 }
    const second = { ...fakeMessage({ partitionId: PART, data: { n: 'second' }, createdAt }), id: 'msg-a', offset: 11 }
    const stream = Stream.from(createFakeSource('src', [[second, first]]))
      .gate(v => v.n === 'first')

    const { cycles } = await runFor(server, stream, { queryId: 'gate.offsetorder' })
    assert.equal(cycles.length, 1)
    assert.equal(cycles[0].ack.count, 1)
    assert.equal(cycles[0].ack.transactionId, first.transactionId, 'the settled prefix is offset 10, the one the broker acks')
    assert.equal(cycles[0].release_lease, false)
  })
})
