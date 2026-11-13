/**
 * Filtered consumer groups (PLAN_ROUTING.md §4, the routing contract §3):
 * `.group(g).where(...)` sends `where` on every pop of the group.
 *
 * What is at stake: the parameter is the same on every pop (the broker fixes
 * the group's where at its first one and refuses a different one), in both
 * pop parameter builders (QueueBuilder.pop and ConsumerManager#buildParams);
 * conditions travel as the JSON list and one string as itself; a where the
 * broker refuses is raised, never swallowed into an empty pop; and a pop
 * without a where is byte for byte what it was.
 *
 * Same style as pop-unit/popDefaults.test.js: a real node:http server playing
 * a canned plan, and every assertion is about what crossed the socket.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen, FILTERED_GROUPS_UNSUPPORTED } from '../../client-v2/index.js'
import { withPlanServer, ok } from '../kv-unit/_planServer.js'

const message = (n) => ({
  id: `0190aaaa-0000-7000-8000-00000000000${n}`, transactionId: `t${n}`, data: { n }, partitionId: '7',
  partition: 'p1', leaseId: 'lease-1', consumerGroup: 'billing', offset: n
})
// A filtered pop's answer echoes `"filtered": true`, empty ones too.
const popBody = (messages = [message(1)]) => ok({ success: true, messages, partitionsClaimed: 1, filtered: true })
const empty = ok({ success: true, messages: [], filtered: true })
const acked = ok([{ index: 0, transactionId: 't1', success: true }])

const pops = (hits) => hits.filter(h => h.method === 'GET' && h.url.startsWith('/api/v1/pop'))
function query(url) {
  const i = url.indexOf('?')
  return new URLSearchParams(i < 0 ? '' : url.slice(i + 1))
}

async function withQueen(plan, defaultResponse, run) {
  await withPlanServer(plan, defaultResponse, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, retryAttempts: 1 })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

describe('where — on the pop', () => {
  it('sends the conditions of several .where() calls as one JSON list, ANDed', async () => {
    await withQueen([popBody()], empty, async (queen, hits) => {
      await queen.queue('orders').group('billing')
        .where('data.kind', 'in', ['order.paid', 'order.refunded'])
        .where('headers.region', '=', 'eu')
        .where('data.vip', 'exists')
        .pop()

      const q = query(pops(hits)[0].url)
      assert.equal(q.get('consumerGroup'), 'billing')
      assert.deepEqual(JSON.parse(q.get('where')), [
        { field: 'data.kind', op: 'in', value: ['order.paid', 'order.refunded'] },
        { field: 'headers.region', op: '=', value: 'eu' },
        { field: 'data.vip', op: 'exists' }
      ])
    })
  })

  it('sends one string as it is; two strings are ANDed', async () => {
    await withQueen([popBody(), popBody()], empty, async (queen, hits) => {
      await queen.queue('orders').group('billing').where("data.kind in ('a', 'b') and data.total > 100").pop()
      await queen.queue('orders').group('billing').where("data.kind = 'a'").where(' headers.type = \'x\' ').pop()

      assert.equal(query(pops(hits)[0].url).get('where'), "data.kind in ('a', 'b') and data.total > 100")
      assert.equal(query(pops(hits)[1].url).get('where'), "data.kind = 'a' and headers.type = 'x'")
    })
  })

  it('takes a list of conditions in one call, as the feed\'s where option does', async () => {
    await withQueen([popBody()], empty, async (queen, hits) => {
      await queen.queue('orders').group('billing').where([['data.kind', '=', 'a'], { field: 'partition', op: 'prefix', value: 'eu-' }]).pop()

      assert.deepEqual(JSON.parse(query(pops(hits)[0].url).get('where')), [
        { field: 'data.kind', op: '=', value: 'a' },
        { field: 'partition', op: 'prefix', value: 'eu-' }
      ])
    })
  })

  it('goes on the pinned partition route too', async () => {
    await withQueen([popBody()], empty, async (queen, hits) => {
      await queen.queue('orders').partition('p1').group('billing').where('data.kind', '=', 'a').pop()

      const hit = pops(hits)[0]
      assert.ok(hit.url.startsWith('/api/v1/pop/queue/orders/partition/p1?'), hit.url)
      assert.ok(query(hit.url).has('where'))
    })
  })

  it('a pop without a where sends no where parameter', async () => {
    await withQueen([popBody()], empty, async (queen, hits) => {
      await queen.queue('orders').group('billing').pop()
      assert.equal(query(pops(hits)[0].url).has('where'), false)
    })
  })

  it('checks the conditions as Streams does, and refuses the two forms together', () => {
    const queen = new Queen({ url: 'http://127.0.0.1:1', handleSignals: false })
    const g = () => queen.queue('orders').group('billing')
    assert.throws(() => g().where('data.kind', '~', 'a'), /unknown op "~"/)
    assert.throws(() => g().where('data.kind', 'in', 'a'), /in on data.kind needs an array/)
    assert.throws(() => g().where('data.kind', '=', 'a').where("data.total > 1"), /\.where\(field, op, value\) calls or one string, not both/)
    assert.throws(() => g().where("data.total > 1").where('data.kind', '=', 'a'), /not both/)
    assert.throws(() => g().where('   '), /an empty string/)
    return queen.close()
  })

  it('refuses, before any request, a where with no group or no queue name', async () => {
    await withQueen([], empty, async (queen, hits) => {
      await assert.rejects(queen.queue('orders').where('data.kind', '=', 'a').pop(), /where filters a consumer group: name one with \.group\(name\)/)
      await assert.rejects(queen.queue().namespace('ns').group('g').where('data.kind', '=', 'a').pop(), /a filtered group reads a named queue/)
      assert.throws(() => queen.queue('orders').where('data.kind', '=', 'a').consume(() => {}), /name one with \.group\(name\)/)
      assert.equal(hits.length, 0, 'no request was sent')
    })
  })

  it('pop() raises the broker\'s refusal of a where instead of answering []', async () => {
    const mismatch = { status: 400, body: { code: 'where_mismatch', error: "group billing on orders filters data.kind = 'a': change it with POST /api/v1/consumer-groups/billing/subscription" } }
    const version = { status: 409, body: { code: 'cluster_version', error: 'filtered groups need every node on catalogue version 8' } }
    await withQueen([mismatch, version, mismatch], empty, async (queen) => {
      await assert.rejects(
        queen.queue('orders').group('billing').where('data.kind', '=', 'b').pop(),
        (err) => err.status === 400 && err.code === 'where_mismatch' && /filters data\.kind = 'a'/.test(err.message)
      )
      await assert.rejects(
        queen.queue('orders').group('billing').where('data.kind', '=', 'b').pop(),
        (err) => err.status === 409 && err.code === 'cluster_version'
      )
      // Unchanged for a pop with no where: a 400 is still an empty answer.
      assert.deepEqual(await queen.queue('orders').group('billing').pop(), [])
    })
  })
})

describe('where — a broker that does not filter (no "filtered" echo)', () => {
  const unsupported = (err) => err.code === FILTERED_GROUPS_UNSUPPORTED && /does not filter consumer groups/.test(err.message)
  const unfiltered = ok({ success: true, messages: [message(1)], partitionsClaimed: 1 })

  it('pop() raises on an old broker\'s bodiless 204, the first thing an idle queue shows', async () => {
    await withQueen([{ status: 204 }], empty, async (queen) => {
      await assert.rejects(queen.queue('orders').group('billing').where('data.kind', '=', 'a').pop(), unsupported)
    })
  })

  it('pop() raises on an answer without the echo, before handing out its messages', async () => {
    await withQueen([unfiltered], empty, async (queen) => {
      await assert.rejects(queen.queue('orders').group('billing').where("data.kind = 'a'").popResult(), unsupported)
    })
  })

  it('consume() stops on the first answer without the echo, and no handler runs', async () => {
    for (const answer of [{ status: 204 }, unfiltered]) {
      await withQueen([answer], empty, async (queen, hits) => {
        let handled = 0
        await assert.rejects(
          async () => { await queen.queue('orders').group('billing').where('data.kind', '=', 'a').consume(async () => { handled++ }) },
          unsupported
        )
        assert.equal(handled, 0)
        assert.equal(pops(hits).length, 1)
      })
    }
  })

  it('a pop without a where reads a 204 or an answer with no echo as always', async () => {
    await withQueen([{ status: 204 }, ok({ success: true, messages: [message(1)] })], empty, async (queen) => {
      assert.deepEqual(await queen.queue('orders').group('billing').pop(), [])
      assert.equal((await queen.queue('orders').group('billing').pop()).length, 1)
    })
  })
})

describe('where — on consume()', () => {
  it('sends the same where on every pop of the group', async () => {
    await withQueen([popBody([message(1)]), acked, popBody([message(2)]), acked], empty, async (queen, hits) => {
      const seen = []
      await queen.queue('orders').group('billing').where('data.kind', '=', 'a').each().limit(2)
        .consume(async (m) => { seen.push(m.transactionId) })

      assert.deepEqual(seen, ['t1', 't2'])
      const wheres = pops(hits).map(h => query(h.url).get('where'))
      assert.equal(wheres.length, 2)
      assert.deepEqual(wheres, [wheres[0], wheres[0]], 'one value on every pop')
      assert.deepEqual(JSON.parse(wheres[0]), [{ field: 'data.kind', op: '=', value: 'a' }])
    })
  })

  it('stops on a where_mismatch: the next pop would be refused the same way', async () => {
    const mismatch = { status: 400, body: { code: 'where_mismatch', error: 'a different where' } }
    await withQueen([mismatch], empty, async (queen, hits) => {
      await assert.rejects(
        async () => { await queen.queue('orders').group('billing').where("data.kind = 'b'").consume(async () => {}) },
        (err) => err.code === 'where_mismatch'
      )
      assert.equal(pops(hits).length, 1)
    })
  })
})

describe('where — admin.updateConsumerGroupWhere', () => {
  it('posts { queue, where } to the group\'s subscription route', async () => {
    await withQueen([ok({ success: true, consumerGroup: 'billing', queueName: 'orders', where: 'set' })], ok({ success: true }), async (queen, hits) => {
      const res = await queen.admin.updateConsumerGroupWhere('billing', 'orders', "data.kind = 'order.paid'")
      await queen.admin.updateConsumerGroupWhere('bill ing', 'orders', [['data.kind', 'in', ['a', 'b']]])
      await queen.admin.updateConsumerGroupWhere('billing', 'orders', ['data.total', '>', 100])

      assert.equal(res.success, true)
      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/consumer-groups/billing/subscription')
      assert.deepEqual(hits[0].body, { queue: 'orders', where: "data.kind = 'order.paid'" })
      assert.equal(hits[1].url, '/api/v1/consumer-groups/bill%20ing/subscription')
      assert.deepEqual(hits[1].body, { queue: 'orders', where: [{ field: 'data.kind', op: 'in', value: ['a', 'b'] }] })
      assert.deepEqual(hits[2].body.where, [{ field: 'data.total', op: '>', value: 100 }])
    })
  })

  it('refuses no where, and passes the broker\'s 404 for a group that does not filter', async () => {
    const notFiltering = { status: 404, body: { success: false, error: 'group billing does not filter orders' } }
    await withQueen([notFiltering], ok({}), async (queen, hits) => {
      await assert.rejects(queen.admin.updateConsumerGroupWhere('billing', 'orders', null), /give the new where/)
      assert.equal(hits.length, 0)
      await assert.rejects(queen.admin.updateConsumerGroupWhere('billing', 'orders', 'data.kind = \'x\''), (err) => err.status === 404)
    })
  })
})
