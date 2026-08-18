/**
 * Timer wire contract (PLAN_KV_TIMERS.md §4, §6.2, §8.1, §9.6).
 *
 * What these tests exist to pin, in order of how much a mistake costs:
 *
 *   1. CANCEL GOES TO ITS OWN ROUTE. `DELETE /api/v1/timers/:queue/*timerKey`
 *      is the one route the proxy is forbidden to block (§9.6). A cancel sent
 *      inside `POST /api/v1/timers` inherits the schedule's authorization, so
 *      an SDK that "simplified" the two into one call would 403 the cancels of
 *      a tenant that is over quota -- while the fire never stops by itself, so
 *      that tenant keeps producing messages it cannot stop.
 *   2. delayMs, never delaySeconds and never an absolute instant (§4.2,
 *      §20.6). One clock, Postgres's. `deliverAt` in an op is a 22023.
 *   3. The payload is base64 (§6.2), and the SDK encodes it, so a caller never
 *      has to know.
 *   4. `txn` is mandatory on a schedule and is echoed on `absent`, which is
 *      what makes "was it already delivered?" answerable without a second API
 *      (§4.4, §20.2).
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { withPlanServer, ok } from './_planServer.js'

const QUEUE = 'test-timers-q'

async function withQueen(plan, run) {
  await withPlanServer(plan, ok({ results: [{ ok: true, status: 'scheduled' }] }), async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

const scheduled = (extra = {}) => ok({
  results: [{ ok: true, status: 'scheduled', queue: QUEUE, timerKey: 'k', txn: 't', messageId: 'm', deliverAt: '2026-08-17T00:00:30.000000Z', ...extra }]
})

const b64 = (obj) => Buffer.from(JSON.stringify(obj), 'utf8').toString('base64')

describe('Timer wire — schedule', () => {
  it('sends one schedule op with delayMs and a base64 payload', async () => {
    await withQueen([scheduled()], async (queen, hits) => {
      const res = await queen.timer(QUEUE)
        .key('order-9f1')
        .delayMs(30_000)
        .payload({ orderId: '9f1' })
        .txn('11111111-1111-7111-8111-111111111111')
        .schedule()

      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/timers')
      assert.deepEqual(hits[0].body, {
        operations: [{
          op: 'schedule',
          queue: QUEUE,
          timerKey: 'order-9f1',
          delayMs: 30000,
          txn: '11111111-1111-7111-8111-111111111111',
          payload: b64({ orderId: '9f1' })
        }]
      })
      assert.equal(res.status, 'scheduled')
      assert.equal(res.messageId, 'm')
    })
  })

  it('mints a txn when the caller does not supply one, and never omits it', async () => {
    await withQueen([scheduled()], async (queen, hits) => {
      await queen.timer(QUEUE).key('k').delayMs(1).payload({}).schedule()
      const op = hits[0].body.operations[0]
      assert.equal(typeof op.txn, 'string')
      assert.ok(op.txn.length >= 32, 'txn is mandatory on a schedule (§20.2)')
    })
  })

  it('carries an explicit partition and omits it otherwise', async () => {
    await withQueen([scheduled(), scheduled()], async (queen, hits) => {
      await queen.timer(QUEUE).key('k').partition('eu').delayMs(1).payload({}).txn('t').schedule()
      assert.deepEqual(hits[0].body.operations[0], {
        op: 'schedule', queue: QUEUE, timerKey: 'k', partition: 'eu', delayMs: 1, txn: 't', payload: b64({})
      })

      await queen.timer(QUEUE).key('k').delayMs(1).payload({}).txn('t').schedule()
      assert.ok(!('partition' in hits[1].body.operations[0]))
    })
  })

  it('converts the `delay` duration string to milliseconds', async () => {
    await withQueen([scheduled(), scheduled(), scheduled()], async (queen, hits) => {
      await queen.timer(QUEUE).key('k').delay('250ms').payload({}).txn('t').schedule()
      await queen.timer(QUEUE).key('k').delay('30s').payload({}).txn('t').schedule()
      await queen.timer(QUEUE).key('k').delay('2h').payload({}).txn('t').schedule()
      assert.equal(hits[0].body.operations[0].delayMs, 250, 'a 250ms retry backoff is why this wire is in ms (§20.6)')
      assert.equal(hits[1].body.operations[0].delayMs, 30000)
      assert.equal(hits[2].body.operations[0].delayMs, 7200000)
    })
  })

  it('sends raw bytes as-is when given a Buffer, and JSON otherwise', async () => {
    await withQueen([scheduled()], async (queen, hits) => {
      await queen.timer(QUEUE).key('k').delayMs(1).payload(Buffer.from([1, 2, 3])).txn('t').schedule()
      assert.equal(hits[0].body.operations[0].payload, Buffer.from([1, 2, 3]).toString('base64'))
    })
  })

  it('refuses the server-owned spellings instead of letting the broker 400', async () => {
    await withQueen([], async (queen, hits) => {
      const t = queen.timer(QUEUE).key('k').payload({}).txn('t')
      assert.equal(typeof t.delaySeconds, 'undefined', 'delaySeconds is not expressible on this wire (§20.6)')
      assert.equal(typeof t.at, 'undefined', 'an absolute instant is not expressible either (§4.2)')
      await assert.rejects(() => queen.timer(QUEUE).key('k').payload({}).schedule(),
        (e) => { assert.match(e.message, /delay/i); return true })
      await assert.rejects(() => queen.timer(QUEUE).delayMs(1).payload({}).schedule(),
        (e) => { assert.match(e.message, /key/i); return true })
      await assert.rejects(() => queen.timer(QUEUE).key('k').delayMs(1).schedule(),
        (e) => { assert.match(e.message, /payload/i); return true })
      assert.equal(hits.length, 0)
    })
  })
})

describe('Timer wire — cancel, peek, list', () => {
  it('cancel uses DELETE on its OWN route, the one that is never blockable', async () => {
    const answer = ok({ ok: true, status: 'cancelled', queue: QUEUE, timerKey: 'order/9f1', txn: 't' })
    await withQueen([answer], async (queen, hits) => {
      const res = await queen.timer(QUEUE).key('order/9f1').cancel()
      assert.equal(hits[0].method, 'DELETE', 'a cancel inside POST /api/v1/timers inherits the schedule ladder (§9.6)')
      assert.equal(hits[0].url, `/api/v1/timers/${QUEUE}/order%2F9f1`)
      assert.equal(hits[0].raw, '', 'the cancel route carries no body')
      assert.equal(res.status, 'cancelled')
    })
  })

  it('cancel echoes the expected txn as the one query parameter it reads', async () => {
    const absent = ok({ ok: false, status: 'absent', queue: QUEUE, timerKey: 'k', txn: 'tx-1' })
    await withQueen([absent], async (queen, hits) => {
      const res = await queen.timer(QUEUE).key('k').txn('tx-1').cancel()
      assert.equal(hits[0].url, `/api/v1/timers/${QUEUE}/k?txn=tx-1`)
      assert.equal(res.ok, false, 'absent carries ok:false — it MAY mean already delivered (§4.4)')
      assert.equal(res.status, 'absent')
      assert.equal(res.txn, 'tx-1', 'the txn comes back so the log can be checked without a second API')
    })
  })

  it('peek reads one key and reports a miss as found:false with HTTP 200', async () => {
    await withQueen([ok({ found: false, queue: QUEUE, timerKey: 'k' })], async (queen, hits) => {
      const res = await queen.timer(QUEUE).key('k').peek()
      assert.equal(hits[0].method, 'GET')
      assert.equal(hits[0].url, `/api/v1/timers/${QUEUE}/k`)
      assert.equal(res.found, false)
    })
  })

  it('list is keyset over one queue, and the queue is mandatory', async () => {
    await withQueen([ok({ rows: [], truncated: false, nextAfter: null }), ok({ rows: [], truncated: false, nextAfter: null })], async (queen, hits) => {
      await queen.timer(QUEUE).list()
      assert.equal(hits[0].url, `/api/v1/timers/${QUEUE}`)

      await queen.timer(QUEUE).list({ after: 'k1', limit: 10 })
      assert.equal(hits[1].url, `/api/v1/timers/${QUEUE}?after=k1&limit=10`)

      assert.throws(() => queen.timer(''), (e) => { assert.match(e.message, /queue/i); return true })
    })
  })

  it('peek and list do not need a payload, a delay or a txn', async () => {
    await withQueen([ok({ found: false })], async (queen) => {
      await queen.timer(QUEUE).key('k').peek()
    })
  })
})
