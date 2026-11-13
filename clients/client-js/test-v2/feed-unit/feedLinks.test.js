/**
 * Streams auto-link (PLAN_ROUTING.md §7, the routing contract §6): a
 * `.to(queue)` on a feed source whose records are their source messages
 * unchanged -- only where/filter/keyBy before it -- commits `link` operations
 * instead of pushes of copies; anything a map, flatMap or stateful step made
 * is still a push, and `.to(q, { link: false })` pushes.
 *
 * The feed shows a linked message in its target too, read through to its
 * source and under the source's transactionId, which a stream's filter on its
 * own prefix does not catch. So a stream links only into queues it does not
 * read, and pushes copies into the ones it does: it never reads back its own
 * outputs, while a stream on another stream's target sees them.
 *
 * Against fakeBroker.js, which resolves a link to its message (same id and
 * transactionId, its payload not sent again), commits a second link of one
 * message into one partition as a no-op, and shows links in the feed.
 */
import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import { feedOutputId, outputPrefix } from '../../client-v2/streams/runtime/FeedRunner.js'
import { fakeBroker, fakeQueen, until } from './fakeBroker.js'

const fast = (o = {}) => ({ waitMillis: 20, commitEveryMillis: 0, ...o })
const wait = (ms) => new Promise((r) => setTimeout(r, ms))
// Generous: these run beside the whole unit suite.
const settle = (cond, what) => until(cond, 10000, what)
/** The operations of every committed transaction that wrote messages. */
const operations = (b) => b.txns.filter((t) => t.items.length > 0).flatMap((t) => t.items)
const linked = (b) => operations(b).filter((it) => it.link)

/**
 * Run `fn` with a `start(stream, opts)` that runs a stream and keeps it, and
 * stop every stream it started, however `fn` ends: a runner left running would
 * keep the test process alive.
 */
async function withStreams(fn) {
  const runners = []
  const start = async (stream, opts) => {
    const runner = await stream.run(opts)
    runners.push(runner)
    return runner
  }
  try {
    return await fn(start)
  } finally {
    await Promise.all(runners.map((r) => r.stop().catch(() => {})))
  }
}

describe('Stream.fromFeed .to() — links', () => {
  it('a chain of where and filter links its messages: the same message, its payload not sent again', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    const sent = []
    const post = queen.http.post
    queen.http.post = async (path, body, ...rest) => {
      if (path === '/api/v1/transaction') sent.push(JSON.parse(JSON.stringify(body)))
      return post(path, body, ...rest)
    }
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1, country: 'IT' }, headers: { type: 'order.paid' } })
    b.push({ queue: 'orders', partition: 'c1', data: { n: 2, country: 'IT' } })
    b.push({ queue: 'orders', partition: 'c2', data: { n: 3, country: 'FR' } })
    await withStreams(async (start) => {
      const runner = await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
        .where('data.country', '=', 'IT')
        .filter((m) => m.data.n > 0)
        .keyBy((m) => m.data.country)
        .to(queen.queue('orders-it')), { queryId: 'orders-it', ...fast() })
      await settle(() => b.messages('orders-it').length >= 2)
      await runner.stop()
    })

    const source = b.messages('orders')
    const out = b.messages('orders-it')
    assert.deepEqual(out.map((e) => e.payload.n), [1, 2])
    assert.deepEqual(out.map((e) => e.transactionId), [source[0].transactionId, source[1].transactionId], 'the source\'s transactionId')
    assert.deepEqual(out.map((e) => e.id), [source[0].id, source[1].id], 'the source\'s id')
    assert.deepEqual(out[0].headers, { type: 'order.paid' }, 'read through to its message, headers too')
    assert.equal(out[0].partition, 'c1', 'the source partition by default')

    const ops = sent.flatMap((body) => body.operations)
    assert.deepEqual(ops.map((op) => op.type), ['link', 'link'], 'one link per message, no push')
    assert.deepEqual(ops[0], {
      type: 'link', queue: 'orders-it', partition: 'c1',
      from: { queue: 'orders', partition: 'c1', offset: source[0].offset, transactionId: source[0].transactionId }
    })
    assert.ok(!('payload' in ops[0]) && !('items' in ops[0]))
  })

  it('a map makes a new record: pushed, with ids made from the entry, as before', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    await withStreams(async (start) => {
      await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
        .map((m) => m.data)
        .to(queen.queue('copy')), { queryId: 'mapped', ...fast() })
      await settle(() => b.messages('copy').length >= 1)
    })

    assert.equal(linked(b).length, 0)
    const [out] = b.messages('copy')
    assert.equal(out.transactionId, feedOutputId('mapped', b.messages('orders')[0], 0))
  })

  it('a stateful step makes new records: pushed', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.nowMs = b.lastNowMs = Date.parse('2026-11-12T10:00:00.000Z')
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    await withStreams(async (start) => {
      await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
        .windowTumbling({ seconds: 60 }).aggregate({ count: () => 1 })
        .to(queen.queue('per-minute')), { queryId: 'counted', ...fast({ now: () => b.nowMs }) })
      b.nowMs += 61000
      b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
      await settle(() => b.messages('per-minute').length >= 1)
    })

    assert.equal(linked(b).length, 0)
    assert.ok(b.messages('per-minute')[0].transactionId.startsWith(outputPrefix('counted')))
  })

  it('{ link: false } pushes copies', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    await withStreams(async (start) => {
      await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
        .to(queen.queue('copy'), { link: false }), { queryId: 'copies', ...fast() })
      await settle(() => b.messages('copy').length >= 1)
    })

    assert.equal(linked(b).length, 0)
    assert.equal(b.messages('copy')[0].transactionId, feedOutputId('copies', b.messages('orders')[0], 0))
  })

  it('a function that routes still links, each to its queue and partition, in one commit', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push([
      { queue: 'orders', partition: 'c1', data: { n: 1, country: 'IT' } },
      { queue: 'orders', partition: 'c2', data: { n: 2, country: 'FR' } }
    ])
    await withStreams(async (start) => {
      await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
        .to((order) => [{ queue: `orders-${order.country}`, partition: 'all' }, 'audit']), { queryId: 'router', ...fast() })
      await settle(() => b.messages('audit').length >= 2)
    })

    assert.deepEqual(b.messages('orders-IT').map((e) => [e.partition, e.payload.n]), [['all', 1]])
    assert.deepEqual(b.messages('orders-FR').map((e) => [e.partition, e.payload.n]), [['all', 2]])
    assert.deepEqual(b.messages('audit').map((e) => [e.partition, e.payload.n]), [['c1', 1], ['c2', 2]])
    assert.equal(linked(b).length, 4)
  })

  it('a message its target holds already (another query linked it there) is a no-op: the commit goes through', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    const [first] = b.messages('orders')
    // Another query linked the same message into copy/c1 first.
    await queen.transaction().queue('copy').partition('c1').link([first]).commit()
    let runner
    await withStreams(async (start) => {
      runner = await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
        .to(queen.queue('copy')), { queryId: 'second', ...fast() })
      await settle(() => b.txns.some((t) => t.items.some((it) => it.duplicate)))
      b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
      await settle(() => b.messages('copy').length >= 2)
      await wait(30)
    })

    assert.deepEqual(b.messages('copy').map((e) => [e.payload.n, e.transactionId]), [
      [1, first.transactionId], [2, b.messages('orders')[1].transactionId]
    ], 'each message once in copy/c1')
    assert.equal(runner.metrics().conflicts, 0, 'no rollback, no reading the same entries again')
  })

  // As the client's errors carry them: an HTTP refusal with its status and
  // body, a rolled-back transaction with its `.reason`. Decided by the code.
  const rolledBack = (reason) => Object.assign(new Error(`${reason}: refused`), { reason })
  for (const [label, error] of [
    ['a broker older than links (400 bad_request: push and ack only)', Object.assign(
      new Error('the broker refused the bundle'),
      { status: 400, body: { success: false, reason: 'bad_request', error: 'push and ack only', results: [] } })],
    ['a cluster not on catalogue version 8 (cluster_version)', rolledBack('cluster_version')],
    ['a credential that may not read the source (scope_denied)', rolledBack('scope_denied')],
    ['a target that is a reading queue (reading_queue)', rolledBack('reading_queue')]
  ]) {
    it(`${label}: the stream pushes copies from then on, and says so once`, async () => {
      const b = fakeBroker()
      const queen = fakeQueen(b)
      b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
      b.injectNext({ error })
      const warned = []
      const original = console.warn
      console.warn = (...args) => warned.push(args.join(' '))
      let runner
      try {
        await withStreams(async (start) => {
          runner = await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
            .to(queen.queue('copy')), { queryId: 'old', ...fast() })
          await settle(() => b.messages('copy').length >= 1)
          b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
          await settle(() => b.messages('copy').length >= 2)
        })
      } finally {
        console.warn = original
      }

      const copy = b.messages('copy')
      assert.deepEqual(copy.map((e) => e.payload.n), [1, 2])
      assert.ok(copy.every((e) => e.transactionId.startsWith(outputPrefix('old'))), 'pushed, ids made from the entries')
      assert.equal(linked(b).length, 0)
      assert.equal(warned.filter((w) => w.includes('links refused')).length, 1, warned.join('\n'))
      assert.equal(runner.metrics().errors, 0, 'not an error: the stream goes on as before links')
    })
  }

  for (const reason of ['source_gone', 'source_mismatch']) {
    it(`a linked message ${reason === 'source_gone' ? 'gone' : 'changed'} at its source (${reason}): that commit goes as pushes, the next ones link`, async () => {
      const b = fakeBroker()
      const queen = fakeQueen(b)
      b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
      b.injectNext({ error: rolledBack(reason) })
      const warned = []
      const original = console.warn
      console.warn = (...args) => warned.push(args.join(' '))
      try {
        await withStreams(async (start) => {
          await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
            .to(queen.queue('copy')), { queryId: 'once', ...fast() })
          await settle(() => b.messages('copy').length >= 1)
          b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
          await settle(() => b.messages('copy').length >= 2)
        })
      } finally {
        console.warn = original
      }

      const [pushed, link] = b.messages('copy')
      const source = b.messages('orders')
      assert.equal(pushed.transactionId, feedOutputId('once', source[0], 0), 'the refused commit, as a push')
      assert.equal(link.transactionId, source[1].transactionId, 'the next message is linked')
      assert.equal(warned.length, 0, 'nothing to say: links stay on')
    })
  }
})

describe('Stream.fromFeed .to() — links and the queues a stream reads', () => {
  it('a stream on every queue pushes: it reads its target, and never reads back its outputs', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    await withStreams(async (start) => {
      await start(Stream.fromFeed(queen, { from: 'earliest' })
        .to(queen.queue('copy')), { queryId: 'everything', ...fast() })
      await settle(() => b.messages('copy').length >= 1)
      b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
      await settle(() => b.messages('copy').length >= 2)
      await wait(60)
    })

    assert.equal(linked(b).length, 0, 'it reads copy: a link there would come back under the source\'s transactionId')
    assert.deepEqual(b.messages('copy').map((e) => e.payload.n), [1, 2], 'each once, no echo')
    assert.ok(b.messages('copy').every((e) => e.transactionId.startsWith(outputPrefix('everything'))))
  })

  it('links into the queues it does not read, and pushes into the ones it does', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    await withStreams(async (start) => {
      await start(Stream.fromFeed(queen, { queues: ['orders', 'cop*'], from: 'earliest' })
        .where('queue', '=', 'orders')
        .to([queen.queue('copy'), queen.queue('audit')]), { queryId: 'both', ...fast() })
      await settle(() => b.messages('copy').length >= 1 && b.messages('audit').length >= 1)
      await wait(60)
    })

    const [source] = b.messages('orders')
    assert.deepEqual(b.messages('audit').map((e) => [e.transactionId, !!e.link]), [[source.transactionId, true]], 'audit is not read: linked')
    assert.deepEqual(b.messages('copy').map((e) => [e.transactionId, !!e.link]), [[feedOutputId('both', source, 0), false]], 'copy matches cop*: pushed')
  })

  it('a self-reading stream never reads its own output back: orders* routed into a queue named from the message\'s', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 } })
    await withStreams(async (start) => {
      await start(Stream.fromFeed(queen, { queues: ['orders*'], from: 'earliest' })
        .to((order, msg) => `${msg.queue}-copy`), { queryId: 'self', ...fast() })
      await settle(() => b.messages('orders-copy').length >= 1)
      b.push({ queue: 'orders', partition: 'c1', data: { n: 2 } })
      await settle(() => b.messages('orders-copy').length >= 2)
      // Time for an echo to show, were there one.
      await wait(150)
    })

    assert.deepEqual(b.messages('orders-copy').map((e) => e.payload.n), [1, 2], 'each output once')
    assert.equal(b.messages('orders-copy-copy').length, 0, 'no output read back and routed again')
    assert.equal(linked(b).length, 0, 'orders-copy matches orders*: pushed, under ids it leaves out of its reads')
  })

  it('a feed stream on another stream\'s link target sees the linked outputs, read through to their source', async () => {
    const b = fakeBroker()
    const queen = fakeQueen(b)
    b.push({ queue: 'orders', partition: 'c1', data: { n: 1 }, headers: { type: 'order.paid' } })
    const seen = []
    await withStreams(async (start) => {
      await start(Stream.fromFeed(queen, { queues: ['orders'], from: 'earliest' })
        .to(queen.queue('orders-paid')), { queryId: 'first', ...fast() })
      await start(Stream.fromFeed(queen, { queues: ['orders-paid'], from: 'earliest' })
        .where('headers.type', '=', 'order.paid')
        .foreach((value, ctx) => { seen.push({ value, ...ctx }) }), { queryId: 'second', ...fast() })
      await settle(() => seen.length >= 1)
    })

    const [source] = b.messages('orders')
    assert.equal(linked(b).length, 1, 'the first stream linked')
    assert.deepEqual(seen.map((s) => [s.queue, s.transactionId, s.value.n]), [['orders-paid', source.transactionId, 1]])
  })
})
