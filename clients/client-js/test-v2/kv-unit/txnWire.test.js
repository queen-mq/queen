/**
 * Transaction wire contract for the kv and timers riders
 * (PLAN_KV_TIMERS.md §6.3, §8.2, §8.3, §10.4).
 *
 * THE ONE SHAPE RULE, AND WHY IT IS NOT NEGOTIABLE. `kv` and `timers` are
 * TOP-LEVEL fields of the request body, beside `operations` and never inside
 * it. The reason is a silent failure in another language that this client's
 * shape has to respect anyway, because the broker parses one wire for all
 * seven: two Go struct fields carrying the same JSON key at the same level are
 * BOTH DROPPED by encoding/json, with no error. A bundle would go out with
 * zero kv ops, the broker would commit a transaction with no gate, and the
 * putIfAbsent the bundle existed for would simply never have happened.
 *
 * BYTE-IDENTITY WHEN THE RIDERS ARE ABSENT. A transaction that carries neither
 * array must produce exactly today's body -- no `kv: []`, no `timers: null`.
 * Anything else is a wire change for every broker that has not been upgraded.
 *
 * AND THE ONE BEHAVIOUR CHANGE THIS FEATURE FORCES ON `commit()`: a lost
 * `required` precondition RETURNS `{success:false, reason:'kv_precondition'}`,
 * it does not throw. It is the expected outcome of every legitimate
 * redelivery -- the idempotency marker doing its job -- and a throw would put
 * the single most frequent outcome of the product inside every caller's
 * catch-and-retry. Everything else that fails still throws.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { withPlanServer, ok } from './_planServer.js'

const NS = 'test-txn-ns'
const QUEUE = 'test-txn-q'

const MSG = { transactionId: 'aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa', partitionId: 'pppppppp-pppp-7ppp-8ppp-pppppppppppp', leaseId: 'llllllll-llll-7lll-8lll-llllllllllll' }

const success = (results = []) => ok({ transactionId: 'txn-1', success: true, results })

async function withQueen(plan, run) {
  await withPlanServer(plan, success(), async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

const b64 = (obj) => Buffer.from(JSON.stringify(obj), 'utf8').toString('base64')

describe('Transaction wire — where the riders live', () => {
  it('a bundle with no riders is byte-for-byte what it is today', async () => {
    await withQueen([success()], async (queen, hits) => {
      await queen.transaction().ack(MSG).commit()
      assert.deepEqual(Object.keys(hits[0].body).sort(), ['operations', 'requiredLeases'],
        'no kv:[] and no timers:null — a bundle without riders must not change shape')
    })
  })

  it('kv is a TOP-LEVEL array, never an element of operations', async () => {
    await withQueen([success()], async (queen, hits) => {
      await queen.transaction()
        .ack(MSG)
        .kv.putIfAbsent(NS, 'marker', true, { ttlSeconds: 3600 })
        .commit()

      const body = hits[0].body
      assert.ok(Array.isArray(body.kv))
      assert.deepEqual(body.kv, [{ op: 'putIfAbsent', ns: NS, key: 'marker', value: true, ttlSeconds: 3600 }])
      assert.equal(body.operations.length, 1)
      assert.equal(body.operations[0].type, 'ack')
      for (const op of body.operations) {
        assert.ok(op.type !== 'kv', 'an op typed `kv` inside operations is a named 400 from the broker, by design')
      }
    })
  })

  it('timers is the other top-level array, and carries the same op shape as the route', async () => {
    await withQueen([success()], async (queen, hits) => {
      await queen.transaction()
        .ack(MSG)
        .timer(QUEUE).key('k').delayMs(60_000).payload({ a: 1 }).txn('tx-9').schedule()
        .commit()

      assert.deepEqual(hits[0].body.timers, [{
        op: 'schedule', queue: QUEUE, timerKey: 'k', delayMs: 60000, txn: 'tx-9', payload: b64({ a: 1 })
      }])
    })
  })

  it('a timer cancel inside a bundle is an op in the timers array', async () => {
    await withQueen([success()], async (queen, hits) => {
      await queen.transaction()
        .ack(MSG)
        .timer(QUEUE).key('k').txn('tx-9').cancel()
        .commit()

      assert.deepEqual(hits[0].body.timers, [{ op: 'cancel', queue: QUEUE, timerKey: 'k', txn: 'tx-9' }])
    })
  })

  it('once() is the gate: putIfAbsent with required:true', async () => {
    await withQueen([success()], async (queen, hits) => {
      await queen.transaction().ack(MSG).once(NS, 'order-9f1', { ttlSeconds: 86400 }).commit()
      assert.deepEqual(hits[0].body.kv, [{
        op: 'putIfAbsent', ns: NS, key: 'order-9f1', value: true, ttlSeconds: 86400, required: true
      }])
    })
  })

  it('until on a rider is resolved at COMMIT time, not when the op is queued', async () => {
    await withQueen([success()], async (queen, hits) => {
      const tx = queen.transaction().ack(MSG).kv.put(NS, 'k', 1, { until: new Date(Date.now() + 60_000) })
      await new Promise(r => setTimeout(r, 1100))
      await tx.commit()
      const sent = hits[0].body.kv[0]
      assert.ok(!('until' in sent))
      assert.ok(sent.ttlSeconds <= 59, `a delta computed at build time would still say 60; got ${sent.ttlSeconds}`)
      assert.ok(sent.ttlSeconds >= 58)
    })
  })

  it('getPrefix is refused before the request, with the surface that does allow it named', async () => {
    await withQueen([], async (queen, hits) => {
      assert.throws(() => queen.transaction().kv.getPrefix(NS, 'p'), (e) => {
        assert.match(e.message, /\/api\/v1\/kv/)
        return true
      })
      assert.equal(hits.length, 0)
    })
  })

  it('a kv-only bundle is legal and commits without operations', async () => {
    await withQueen([success()], async (queen, hits) => {
      await queen.transaction().kv.put(NS, 'k', 1, { ttlSeconds: 60 }).commit()
      assert.deepEqual(hits[0].body.operations, [])
      assert.equal(hits[0].body.kv.length, 1)
    })
  })

  it('an entirely empty transaction still refuses to commit', async () => {
    await withQueen([], async (queen, hits) => {
      await assert.rejects(() => queen.transaction().commit(), (e) => {
        assert.match(e.message, /no operations/i)
        return true
      })
      assert.equal(hits.length, 0)
    })
  })
})

describe('Transaction wire — the verdict that must not throw', () => {
  const precondition = ok({
    transactionId: 'txn-1',
    success: false,
    ok: false,
    reason: 'kv_precondition',
    error: 'kv_precondition_failed',
    failedIndex: 1,
    kvReason: 'exists',
    version: 90101,
    value: { owner: 'first-delivery' },
    results: []
  })

  it('commit() RETURNS on a lost precondition', async () => {
    await withQueen([precondition], async (queen) => {
      const res = await queen.transaction()
        .ack(MSG)
        .once(NS, 'order-9f1', { ttlSeconds: 86400 })
        .commit()

      assert.equal(res.success, false)
      assert.equal(res.reason, 'kv_precondition')
      assert.equal(res.failedIndex, 1, 'the index is in the FLAT space of results[]')
      assert.equal(res.kvReason, 'exists')
      assert.equal(res.version, 90101)
      assert.deepEqual(res.value, { owner: 'first-delivery' })
    })
  })

  it('commit() still THROWS on every other failure', async () => {
    const rejected = ok({ transactionId: 'txn-1', success: false, reason: 'ack_rejected', error: 'QTXN invalid or expired lease', results: [] })
    await withQueen([rejected], async (queen) => {
      await assert.rejects(() => queen.transaction().ack(MSG).commit(), (e) => {
        assert.match(e.message, /QTXN/)
        assert.equal(e.reason, 'ack_rejected', 'the closed-taxonomy reason travels on the error, so nobody matches the message')
        return true
      })
    })
  })

  it('commit() throws on a duplicate and on a misalignment, with the reason attached', async () => {
    const dup = ok({ transactionId: 'txn-1', success: false, reason: 'duplicate', error: 'QDUP duplicate transaction', results: [] })
    const bad = ok({ transactionId: 'txn-1', success: false, reason: 'misaligned', error: 'QTXN rider results missing', results: [] })
    await withQueen([dup, bad], async (queen) => {
      await assert.rejects(() => queen.transaction().ack(MSG).commit(), (e) => { assert.equal(e.reason, 'duplicate'); return true })
      await assert.rejects(() => queen.transaction().ack(MSG).commit(), (e) => { assert.equal(e.reason, 'misaligned'); return true })
    })
  })

  it('a committed bundle hands back the flat results, riders included', async () => {
    const results = [
      { index: 0, type: 'ack', success: true },
      { index: 1, type: 'kv', opIndex: 0, op: 'put', applied: true, key: 'k', version: 4 },
      { index: 2, type: 'timer', opIndex: 0, ok: true, status: 'scheduled', timerKey: 'k' }
    ]
    await withQueen([success(results)], async (queen) => {
      const res = await queen.transaction()
        .ack(MSG)
        .kv.put(NS, 'k', 1, { ttlSeconds: 60 })
        .timer(QUEUE).key('k').delayMs(1).payload({}).txn('t').schedule()
        .commit()

      assert.equal(res.success, true)
      assert.equal(res.results[1].type, 'kv')
      assert.equal(res.results[1].applied, true)
      assert.equal(res.results[2].status, 'scheduled')
    })
  })
})
