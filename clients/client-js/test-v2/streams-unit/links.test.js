/**
 * Auto-link on a queue source (PLAN_ROUTING.md §7, the routing contract §6):
 * a `.to()` whose record is the popped message unchanged sends cycle push
 * items with `link: { offset, transactionId }` (the cycle's own partition
 * holds the message), so the sink holds the same message.
 *
 * What is at stake: only an unchanged record links (a map, a flatMap, a
 * window make new ones, which are pushed); `{ link: false }` pushes; the
 * payload still travels beside the link, because a broker older than links
 * ignores the field and would otherwise store an empty message; and a
 * cluster that cannot link yet (400 cluster_version) turns links off, so the
 * messages that come back are pushed.
 */

import { describe, it, before, after } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { createFakeStreamsServer, createFakeSource, fakeMessage } from './fakeServer.js'

const PART = '7'

const batch = (...ns) => ns.map((n, i) => ({
  ...fakeMessage({ partitionId: PART, partitionName: 'order-1', data: { n, kind: n % 2 ? 'odd' : 'even' }, createdAt: `2026-11-12T10:00:0${i}.000Z` }),
  offset: 40 + i
}))

const logs = () => {
  const warned = []
  return { warned, logger: { info() {}, warn(msg) { warned.push(msg) }, error() {}, debug() {} } }
}

// Run until `cycles` cycles were committed (or the time is up), then stop.
async function runFor(server, stream, { cycles = 1, timeoutMs = 1500, queryId, logger = logs().logger }) {
  const baseline = server.recorded.cycles.length
  const handle = await stream.run({ queryId, url: server.url, logger })
  const start = Date.now()
  while (server.recorded.cycles.length - baseline < cycles && Date.now() - start < timeoutMs) {
    await new Promise(r => setTimeout(r, 20))
  }
  await handle.stop()
  return { handle, cycles: server.recorded.cycles.slice(baseline) }
}

describe('queue-source .to() — links', () => {
  let server
  before(async () => { server = await createFakeStreamsServer() })
  after(async () => { await server.close() })

  it('a where/filter chain links each message it keeps, by offset and transactionId', async () => {
    const msgs = batch(1, 2, 3)
    const stream = Stream.from(createFakeSource('orders', [msgs]))
      .where('data.kind', '=', 'odd')
      .filter(m => m.data.n > 0)
      .to({ _queueName: 'orders-odd' })

    const { cycles: [c] } = await runFor(server, stream, { queryId: 'links.filter' })
    assert.equal(c.partition_id, PART)
    assert.deepEqual(c.push_items, [
      { queue: 'orders-odd', partition: 'order-1', payload: { n: 1, kind: 'odd' }, link: { offset: 40, transactionId: msgs[0].transactionId } },
      { queue: 'orders-odd', partition: 'order-1', payload: { n: 3, kind: 'odd' }, link: { offset: 42, transactionId: msgs[2].transactionId } }
    ], 'the payload beside the link: a broker older than links stores it, as before')
    assert.equal(c.ack.count, 3, 'every message of the batch is settled, linked or filtered out')
  })

  it('a keyBy and a gate change no record: still links', async () => {
    const msgs = batch(1, 2)
    const stream = Stream.from(createFakeSource('orders', [msgs]))
      .keyBy(m => m.data.kind)
      .gate(() => true)
      .to({ _queueName: 'orders-gated' })

    const { cycles: [c] } = await runFor(server, stream, { queryId: 'links.gate' })
    assert.deepEqual(c.push_items.map(p => p.link), [
      { offset: 40, transactionId: msgs[0].transactionId },
      { offset: 41, transactionId: msgs[1].transactionId }
    ])
  })

  it('a map, a flatMap or a window makes new records: pushed, no link', async () => {
    const mapped = Stream.from(createFakeSource('orders', [batch(1)])).map(m => ({ ...m.data, seen: true })).to({ _queueName: 'out' })
    const flat = Stream.from(createFakeSource('orders', [batch(1)])).flatMap(m => [m.data]).to({ _queueName: 'out' })
    for (const [queryId, stream] of [['links.map', mapped], ['links.flatmap', flat]]) {
      const { cycles: [c] } = await runFor(server, stream, { queryId })
      assert.equal(c.push_items.length, 1, queryId)
      assert.equal('link' in c.push_items[0], false, queryId)
    }
  })

  it('{ link: false } pushes copies', async () => {
    const stream = Stream.from(createFakeSource('orders', [batch(1)])).to({ _queueName: 'copy' }, { link: false })
    const { cycles: [c] } = await runFor(server, stream, { queryId: 'links.off' })
    assert.deepEqual(c.push_items, [{ queue: 'copy', partition: 'order-1', payload: { n: 1, kind: 'odd' } }])
  })

  it('a message without an offset (a pop of an older broker) is pushed', async () => {
    const [msg] = batch(1)
    delete msg.offset
    const stream = Stream.from(createFakeSource('orders', [[msg]])).to({ _queueName: 'copy' })
    const { cycles: [c] } = await runFor(server, stream, { queryId: 'links.no-offset' })
    assert.equal('link' in c.push_items[0], false)
  })
})

describe('queue-source .to() — a cluster that cannot link yet', () => {
  let server
  before(async () => { server = await createFakeStreamsServer({ refuseLinks: true }) })
  after(async () => { await server.close() })

  it('turns links off once, and pushes the messages that come back', async () => {
    const msgs = batch(1)
    // The refused cycle applied nothing: the lease brings the message back.
    const source = createFakeSource('orders', [msgs, msgs])
    const { warned, logger } = logs()
    const errors = []
    const handle = await Stream.from(source).to({ _queueName: 'copy' })
      .run({ queryId: 'links.refused', url: server.url, logger, onError: (err) => errors.push(err) })
    const start = Date.now()
    while (server.recorded.cycles.length < 2 && Date.now() - start < 3000) await new Promise(r => setTimeout(r, 20))
    await handle.stop()

    const [refused, pushed] = server.recorded.cycles
    assert.ok(refused.push_items[0].link, 'the first cycle linked')
    assert.equal(errors[0].status, 400)
    assert.equal(errors[0].body.code, 'cluster_version')
    assert.deepEqual(pushed.push_items, [{ queue: 'copy', partition: 'order-1', payload: { n: 1, kind: 'odd' } }])
    assert.equal(warned.filter(w => /links refused/.test(w)).length, 1, 'said once')
  })
})

describe('queue-source .to() — a linked message gone at its source', () => {
  let server
  before(async () => { server = await createFakeStreamsServer({ refuseLinksOnce: 'source_gone' }) })
  after(async () => { await server.close() })

  it('pushes that cycle again, and links the ones after it', async () => {
    const first = batch(1)
    const later = batch(3)
    // The refused cycle's message comes back with its lease, then a new one.
    const source = createFakeSource('orders', [first, first, later])
    const { warned, logger } = logs()
    const handle = await Stream.from(source).to({ _queueName: 'copy' })
      .run({ queryId: 'links.once', url: server.url, logger, onError: () => {} })
    const start = Date.now()
    while (server.recorded.cycles.length < 3 && Date.now() - start < 3000) await new Promise(r => setTimeout(r, 20))
    await handle.stop()

    const [refused, retried, next] = server.recorded.cycles
    assert.ok(refused.push_items[0].link, 'linked, and refused: source_gone')
    assert.equal('link' in retried.push_items[0], false, 'that cycle again, as a push')
    assert.deepEqual(next.push_items[0].link, { offset: 40, transactionId: later[0].transactionId }, 'the next one links')
    assert.equal(warned.length, 0, 'nothing to say: links stay on')
  })
})
