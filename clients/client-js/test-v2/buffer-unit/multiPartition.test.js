/**
 * A push across partitions, one buffer per queue, the client-wide bound, the
 * event loop's turn, and items the broker refuses (2026-11-06).
 *
 * Found with a producer that pushed buffered messages to 100 queues x 1,000
 * partitions in a `while (true)` loop: 100,000 buffers each with its own
 * bound, and adds that resolved at once, so Node never ran a flush timer and
 * ran out of memory in ten seconds without sending a request. No broker here:
 * the sink is a fake with a post() the test drives.
 */
import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { QueueBuilder, itemPartition } from '../../client-v2/builders/QueueBuilder.js'
import { TransactionBuilder } from '../../client-v2/builders/TransactionBuilder.js'
import { BufferManager, groupByPartition, refusedItems, MAX_REFUSED_ATTEMPTS } from '../../client-v2/buffer/BufferManager.js'

const tick = (ms = 0) => new Promise((resolve) => setTimeout(resolve, ms))
async function until(predicate, what, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await tick(2)
  }
  assert.fail(`timed out waiting for: ${what}`)
}

/** Records every POST body and answers `queued` for each item. */
function recordingSink(answer = (items) => items.map(() => ({ status: 'queued' }))) {
  const posts = []
  return {
    posts,
    async post(path, body) {
      posts.push({ path, body: JSON.parse(JSON.stringify(body)) })
      return answer(body.items || [], posts.length - 1)
    }
  }
}

describe('push: a partition per item', () => {
  it('one push carries every item to its own partition', async () => {
    const sink = recordingSink()
    const builder = new QueueBuilder(null, sink, new BufferManager(sink), 'orders')
    await builder.push([
      { partition: 'hotel-1', data: { n: 1 } },
      { partition: 'hotel-2', data: { n: 2 } },
      { data: { n: 3 } }
    ])
    assert.equal(sink.posts.length, 1, 'one request')
    assert.deepEqual(sink.posts[0].body.items.map((i) => i.partition), ['hotel-1', 'hotel-2', 'Default'])
    assert.deepEqual(sink.posts[0].body.items.map((i) => i.payload.n), [1, 2, 3])
  })

  it('an item without a partition takes the builder\'s', async () => {
    const sink = recordingSink()
    const builder = new QueueBuilder(null, sink, new BufferManager(sink), 'orders').partition('eu')
    await builder.push([{ data: { n: 1 } }, { partition: 'us', data: { n: 2 } }])
    assert.deepEqual(sink.posts[0].body.items.map((i) => i.partition), ['eu', 'us'])
  })

  it('a bare object is its own payload: its partition field does not route it', async () => {
    const sink = recordingSink()
    const builder = new QueueBuilder(null, sink, new BufferManager(sink), 'orders')
    await builder.push({ partition: 'not-a-route', amount: 5 })
    const [item] = sink.posts[0].body.items
    assert.equal(item.partition, 'Default')
    assert.deepEqual(item.payload, { partition: 'not-a-route', amount: 5 })
  })

  it('reads a numeric partition as its string and refuses anything else', () => {
    assert.equal(itemPartition({ partition: 42, data: 1 }, true, 'Default'), '42')
    assert.throws(() => itemPartition({ partition: {}, data: 1 }, true, 'Default'), /non-empty string/)
    assert.throws(() => itemPartition({ partition: '', data: 1 }, true, 'Default'), /non-empty string/)
  })

  it('a transaction\'s push takes an item\'s partition too', async () => {
    const bodies = []
    const http = { async post(path, body) { bodies.push(body); return { transactionId: 't', success: true, results: [] } } }
    await new TransactionBuilder(http)
      .queue('orders').push([{ partition: 'a', data: { n: 1 } }, { data: { n: 2 } }])
      .commit()
    const items = bodies[0].operations[0].items
    assert.equal(items[0].partition, 'a')
    assert.equal('partition' in items[1], false, 'none named: none sent, as before')
  })
})

describe('buffer: one per queue', () => {
  it('buffered pushes to many partitions of a queue share one buffer and leave as one push, grouped', async () => {
    const sink = recordingSink()
    const manager = new BufferManager(sink)
    const q = () => new QueueBuilder(null, sink, manager, 'orders').buffer({ messageCount: 6, timeMillis: 60000 })
    await q().partition('a').push({ data: { n: 1 } })
    await q().partition('b').push({ data: { n: 2 } })
    await q().partition('a').push({ data: { n: 3 } })
    await q().partition('c').push({ data: { n: 4 } })
    assert.equal(manager.getStats().activeBuffers, 1, 'one buffer for the queue, not one per partition')
    await q().push([{ partition: 'b', data: { n: 5 } }, { partition: 'a', data: { n: 6 } }])
    await until(() => sink.posts.length === 1, 'the count threshold')
    const sent = sink.posts[0].body.items
    // Each partition's items together, in each partition's own order.
    assert.deepEqual(sent.map((i) => `${i.partition}${i.payload.n}`), ['a1', 'a3', 'a6', 'b2', 'b5', 'c4'])
    await until(() => manager.getStats().totalBufferedMessages === 0, 'the buffer to empty')
  })

  it('groupByPartition keeps a single-partition batch as it is', () => {
    const batch = [{ partition: 'x', payload: 1 }, { partition: 'x', payload: 2 }]
    assert.equal(groupByPartition(batch), batch)
  })
})

describe('buffer: the whole client is bounded', () => {
  it('past bufferMaxMessages a buffered push waits, whatever the number of queues', async () => {
    const inflight = []
    const http = {
      post(path, body) {
        return new Promise((resolve) => inflight.push(() => resolve(body.items.map(() => ({ status: 'queued' })))))
      }
    }
    const manager = new BufferManager(http, { maxMessages: 5 })
    const opts = { messageCount: 100, timeMillis: 60000 }
    for (let i = 0; i < 5; i++) {
      await manager.addMessage(`q${i}`, { queue: `q${i}`, partition: 'p', payload: i, transactionId: `t${i}` }, opts)
    }
    let sixth = false
    const pending = manager.addMessage('q9', { queue: 'q9', partition: 'p', payload: 9, transactionId: 't9' }, opts).then(() => { sixth = true })
    await until(() => inflight.length === 5, 'the bound to make every buffer drain')
    await tick(20)
    assert.equal(sixth, false, 'the sixth add waits while five are buffered')
    inflight.shift()()
    await pending
    assert.equal(sixth, true, 'one batch landed: room for the sixth')
    while (inflight.length) inflight.shift()()
    manager.cleanup()
  })
})

describe('buffer: the event loop gets its turn', () => {
  it('a while-true producer of buffered pushes lets the time-based flush run', async () => {
    const sink = recordingSink()
    const manager = new BufferManager(sink, { maxMessages: 10_000_000 })
    // A count threshold the loop never reaches: only the timer can flush.
    const opts = { messageCount: 1_000_000, timeMillis: 20 }
    const began = Date.now()
    let n = 0
    while (sink.posts.length === 0 && Date.now() - began < 3000) {
      await manager.addMessage('orders', { queue: 'orders', partition: `p${n % 1000}`, payload: n, transactionId: `t${n}` }, opts)
      n++
    }
    assert.ok(sink.posts.length > 0, `no flush ran during ${n} adds in ${Date.now() - began} ms: the loop starved the timers`)
    manager.cleanup()
  })
})

describe('buffer: items the broker refuses', () => {
  it('a refused item goes back and is sent again; what landed is not', async () => {
    const sink = recordingSink((items, i) => i === 0
      ? items.map((it) => (it.payload === 2 ? { status: 'error', error: 'partition is being deleted' } : { status: 'queued' }))
      : items.map(() => ({ status: 'queued' })))
    const manager = new BufferManager(sink)
    const opts = { messageCount: 3, timeMillis: 60000, retryDelayMillis: 1 }
    for (const n of [1, 2, 3]) {
      await manager.addMessage('orders', { queue: 'orders', partition: 'p', payload: n, transactionId: `t${n}` }, opts)
    }
    await until(() => sink.posts.length === 2, 'the retry of the refused item')
    assert.deepEqual(sink.posts[1].body.items.map((i) => i.payload), [2], 'only the refused one, same transaction id')
    assert.equal(sink.posts[1].body.items[0].transactionId, 't2')
    await until(() => manager.getStats().totalBufferedMessages === 0, 'the buffer to empty')
    assert.equal(manager.getStats().droppedMessages, 0)
  })

  it('after MAX_REFUSED_ATTEMPTS sends the item is dropped and reported', async () => {
    const sink = recordingSink((items) => items.map(() => ({ status: 'error', error: 'payload too large' })))
    const reported = []
    const manager = new BufferManager(sink, { onError: (items, err) => reported.push({ items, err }) })
    await manager.addMessage('orders', { queue: 'orders', partition: 'p', payload: 1, transactionId: 't1' }, { messageCount: 1, timeMillis: 60000, retryDelayMillis: 1 })
    await until(() => reported.length === 1, 'the drop to be reported')
    assert.equal(sink.posts.length, MAX_REFUSED_ATTEMPTS)
    assert.equal(reported[0].items[0].transactionId, 't1')
    assert.match(reported[0].err.message, /refused by the broker 5 times and dropped: payload too large/)
    assert.equal(manager.getStats().droppedMessages, 1)
    await until(() => manager.getStats().totalBufferedMessages === 0, 'nothing left buffered')
  })

  it('reads refusals by position; a missing or counted answer is handled', () => {
    const sent = [{ n: 1 }, { n: 2 }, { n: 3 }]
    assert.deepEqual(refusedItems(sent, [{ status: 'queued' }, { status: 'duplicate' }, { status: 'error', error: 'x' }]).map((r) => r.item.n), [3])
    assert.deepEqual(refusedItems(sent, []), [], 'no answer per item: landed, as before')
    assert.equal(refusedItems(sent, { count: 3, queued: 2, duplicate: 0, error: 1 }).length, 3, 'counts only: all sent again')
    assert.deepEqual(refusedItems(sent, null), [])
  })
})
