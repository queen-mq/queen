/**
 * `link` in transactions (PLAN_ROUTING.md §7, the routing contract §5):
 * `.queue(q).partition(p).link([msg], { headers })` forwards a message the
 * worker holds, without its payload.
 *
 * What is at stake: one `{type:'link', queue, partition, from, headers}`
 * operation per message, with `from` naming the message the way the broker
 * finds it (a popped message by partitionId, offset and transactionId, plus
 * the group it was popped with; a feed entry, whose partitionId is a UUID, by
 * queue and partition), in the order the caller added the operations; and a
 * binary transaction never moves a link ahead of a push it came after.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { linkSource } from '../../client-v2/builders/TransactionBuilder.js'
import { withWireServer } from '../wire-unit/_wireServer.js'

// A message as a 2.x pop answers it.
const POPPED = {
  id: '0190aaaa-0000-7000-8000-000000000001', transactionId: 'order-1-paid', data: { total: 900 },
  partitionId: '123', partition: 'order-1', leaseId: 'lease-1', consumerGroup: 'billing', offset: 42
}
// A feed entry (feed/Feed.js toEntry): its partitionId is the partition's UUID.
const ENTRY = {
  position: 9, kind: 'push', queue: 'orders', partition: 'order-2', partitionId: '0190bbbb-0000-7000-8000-000000000002',
  offset: 7, id: '0190aaaa-0000-7000-8000-000000000002', transactionId: 'order-2-paid', data: { total: 5 }
}

const committed = (results = []) => () => ({ body: JSON.stringify({ transactionId: 'tx', success: true, results }) })
const transactions = (hits) => hits.filter(h => h.method === 'POST' && h.url === '/api/v1/transaction')
const operationsOf = (hit) => JSON.parse(hit.raw.toString()).operations

async function withQueen(config, answer, run) {
  await withWireServer(answer, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, retryAttempts: 1, ...config })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

describe('link — the operation', () => {
  it('names a popped message by partitionId, offset, transactionId and its group, with the link\'s headers', async () => {
    await withQueen({}, committed(), async (queen, hits) => {
      await queen.transaction()
        .ack(POPPED)
        .queue('manual-review').partition('order-1')
        .link([POPPED], { headers: { 'review-reason': 'amount' } })
        .commit()

      const ops = operationsOf(transactions(hits)[0])
      assert.deepEqual(ops.map(op => op.type), ['ack', 'link'])
      assert.deepEqual(ops[1], {
        type: 'link',
        queue: 'manual-review',
        partition: 'order-1',
        from: { partitionId: '123', offset: 42, transactionId: 'order-1-paid', consumerGroup: 'billing' },
        headers: { 'review-reason': 'amount' }
      })
      assert.equal('payload' in ops[1] || 'data' in ops[1], false, 'the payload is not sent again')
    })
  })

  it('names a feed entry by its queue and partition, with no group', async () => {
    await withQueen({}, committed(), async (queen, hits) => {
      await queen.transaction().queue('audit').partition('a1').link(ENTRY).commit()

      const [op] = operationsOf(transactions(hits)[0])
      assert.deepEqual(op, {
        type: 'link', queue: 'audit', partition: 'a1',
        from: { queue: 'orders', partition: 'order-2', offset: 7, transactionId: 'order-2-paid' }
      })
    })
  })

  it('one operation per message; without .partition(p) each keeps its partition\'s name', async () => {
    await withQueen({}, committed(), async (queen, hits) => {
      const other = { ...POPPED, transactionId: 'order-3-paid', partition: 'order-3', offset: 43 }
      await queen.transaction().queue('next-step').link([POPPED, other]).commit()

      const ops = operationsOf(transactions(hits)[0])
      assert.deepEqual(ops.map(op => [op.type, op.queue, op.partition, op.from.transactionId]), [
        ['link', 'next-step', 'order-1', 'order-1-paid'],
        ['link', 'next-step', 'order-3', 'order-3-paid']
      ])
      assert.equal('headers' in ops[0], false, 'no headers: none sent')
    })
  })

  it('a queue-mode message names no group; { consumerGroup } overrides the message\'s', () => {
    assert.deepEqual(linkSource({ ...POPPED, consumerGroup: '__QUEUE_MODE__' }), { partitionId: '123', offset: 42, transactionId: 'order-1-paid' })
    assert.equal(linkSource(POPPED).consumerGroup, undefined, 'linkSource names the group it is given')
    assert.equal(linkSource(POPPED, 'billing').consumerGroup, 'billing')
    // A partitionId that is a number, an offset that is digits: as an older answer may give them.
    assert.deepEqual(linkSource({ ...POPPED, partitionId: 123, offset: '42' }), { partitionId: '123', offset: 42, transactionId: 'order-1-paid' })
  })

  it('takes the group from { consumerGroup } before the message\'s', async () => {
    await withQueen({}, committed(), async (queen, hits) => {
      await queen.transaction().queue('q').link([POPPED], { consumerGroup: 'audit' }).commit()
      assert.equal(operationsOf(transactions(hits)[0])[0].from.consumerGroup, 'audit')
    })
  })

  it('refuses, before any request, a message it cannot name, and bad headers', async () => {
    await withQueen({}, committed(), async (queen, hits) => {
      const link = (msg, opts) => queen.transaction().queue('q').partition('p').link([msg], opts)
      assert.throws(() => link({ ...POPPED, offset: undefined }), /link: the message has no offset/)
      assert.throws(() => link({ ...POPPED, offset: -1 }), /no offset/)
      assert.throws(() => link({ ...POPPED, transactionId: '' }), /link: the message has no transactionId/)
      assert.throws(() => link({ ...ENTRY, queue: undefined }), /its partitionId as a pop returns it, or its queue and partition/)
      assert.throws(() => link('order-1-paid'), /link: pass messages as a pop or the feed returned them/)
      assert.throws(() => link(POPPED, { headers: { 'queen-x': 'y' } }), /^Error: link: header name "queen-x"/)
      assert.equal(hits.length, 0)
    })
  })

  it('keeps the order of the operations: an ack, a link, a push, a link', async () => {
    await withQueen({}, committed(), async (queen, hits) => {
      await queen.transaction()
        .ack(POPPED)
        .queue('a').partition('p').link([POPPED])
        .queue('a').partition('p').push([{ data: { note: 1 }, transactionId: 'n1' }])
        .queue('b').link([ENTRY])
        .commit()

      // A push names its queue on its items.
      const queueOf = (op) => op.queue ?? (op.items ? op.items[0].queue : '')
      assert.deepEqual(operationsOf(transactions(hits)[0]).map(op => `${op.type}:${queueOf(op) ?? ''}`), ['ack:', 'link:a', 'push:a', 'link:b'])
    })
  })

  it('a link the broker cannot make rolls the transaction back: commit() throws with the reason', async () => {
    const gone = () => ({ body: JSON.stringify({ transactionId: 'tx', success: false, reason: 'source_gone', error: 'source_gone: offset 42 of orders is not retained', results: [] }) })
    await withQueen({}, gone, async (queen) => {
      await assert.rejects(
        queen.transaction().ack(POPPED).queue('manual-review').link([POPPED]).commit(),
        (err) => err.reason === 'source_gone' && /not retained/.test(err.message)
      )
    })
  })
})

describe('link — with binaryPush and pushCompression', () => {
  const framed = { binaryPush: true, pushCompression: true }

  it('a link before the pushes stays in the JSON head, the pushes go as frames', async () => {
    await withQueen(framed, committed([{ index: 0 }, { index: 1 }]), async (queen, hits) => {
      await queen.transaction()
        .queue('a').partition('p').link([POPPED])
        .queue('a').partition('p').push([{ data: { n: 1 }, transactionId: 'n1' }])
        .commit()

      const raw = transactions(hits)[0].raw
      assert.equal(raw.subarray(0, 4).toString('latin1'), 'QTB1')
      const head = JSON.parse(raw.subarray(9, 9 + raw.readUInt32LE(5)).toString())
      assert.deepEqual(head.operations.map(op => op.type), ['link'])
    })
  })

  it('a link after a push keeps the bundle JSON: the broker writes the head before the frames', async () => {
    await withQueen(framed, committed(), async (queen, hits) => {
      await queen.transaction()
        .queue('a').partition('p').push([{ data: { n: 1 }, transactionId: 'n1' }])
        .queue('a').partition('p').link([POPPED])
        .commit()

      const hit = transactions(hits)[0]
      assert.equal(hit.headers['content-type'], 'application/json')
      assert.deepEqual(operationsOf(hit).map(op => op.type), ['push', 'link'])
    })
  })
})
