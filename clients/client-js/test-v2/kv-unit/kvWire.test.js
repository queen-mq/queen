/**
 * KV wire contract (PLAN_KV_TIMERS.md §5, §6.1, §8.1).
 *
 * These tests assert the EXACT JSON body of every KV operation against a
 * scripted plan server. They are the only thing that catches a wrong wire
 * shape before production: the broker's stored procedure has a closed
 * taxonomy, so a misspelled field is not a compile error anywhere, it is a
 * 400 on somebody's first idempotency marker.
 *
 * deepEqual, not "contains": an EXTRA field is as much a contract break as a
 * missing one. `expect` sent as `null`, a `ttlMillis` invented by a client, a
 * `tenant` echoed from a caller -- all of them are refused by the broker, and
 * all of them would ship silently if these assertions were subset checks.
 *
 * The rules pinned here that live in the CLIENT and nowhere else:
 *   * ttlSeconds is canonical; `ttl` (a duration STRING) and `until` (an
 *     instant) are sugar converted at SEND time, rounded UP to the second
 *     (§20.1: a TTL rounded down can expire a marker before the window it had
 *     to cover).
 *   * an explicitly undefined/null `expect` is a client-side bug, never a
 *     silent downgrade to upsert (§5.3).
 *   * `get` returns the ROW: `found` is separate from `value`, because
 *     'null'::jsonb is a legal value and {found:true,value:null} and
 *     {found:false} are different things (§5.5).
 *   * every write returns a WriteResult object, which is ALWAYS truthy --
 *     `if (await kv.delete(...))` is always true, so `applied` is the field
 *     (§10.4).
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { withPlanServer, kvResults } from './_planServer.js'

const NS = 'test-kv-ns'

/** Run `fn(kv, hits)` against a plan server, closing the client afterwards. */
async function withKv(plan, run, clientOptions = {}) {
  await withPlanServer(plan, kvResults({ index: 0, op: 'get', found: false, key: 'k' }), async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, ...clientOptions })
    try {
      await run(queen.kv, hits, queen)
    } finally {
      await queen.close()
    }
  })
}

/** The single-op body every path in this file must produce. */
function oneOp(op) {
  return { operations: [op] }
}

describe('KV wire — reads', () => {
  it('get sends one `get` op to POST /api/v1/kv and returns the row', async () => {
    const row = { index: 0, op: 'get', found: true, key: 'k', value: { a: 1 }, version: 91, expiresAt: '2026-08-18T00:00:00.000000Z', updatedAt: '2026-08-17T00:00:00.000000Z' }
    await withKv([kvResults(row)], async (kv, hits) => {
      const got = await kv.get(NS, 'k')

      assert.equal(hits.length, 1)
      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/kv')
      assert.deepEqual(hits[0].body, oneOp({ op: 'get', ns: NS, key: 'k' }))

      assert.equal(got.found, true)
      assert.deepEqual(got.value, { a: 1 })
      assert.equal(got.version, 91)
      assert.equal(got.expiresAt, '2026-08-18T00:00:00.000000Z')
    })
  })

  it('get never collapses {found:true,value:null} into a miss', async () => {
    await withKv([kvResults({ index: 0, op: 'get', found: true, key: 'k', value: null, version: 5 })], async (kv) => {
      const got = await kv.get(NS, 'k')
      assert.equal(got.found, true, 'a JSON null is a legal VALUE, not an absence')
      assert.equal(got.value, null)
    })
  })

  it('get reports a miss as found:false with no value', async () => {
    await withKv([kvResults({ index: 0, op: 'get', found: false, key: 'k' })], async (kv) => {
      const got = await kv.get(NS, 'k')
      assert.equal(got.found, false)
      assert.equal(got.value, undefined)
    })
  })

  it('getMany sends the keys array and returns rows plus explicit missing', async () => {
    const answer = { index: 0, op: 'getMany', rows: [{ key: 'a', value: 1, version: 2 }], missing: ['b'], truncated: false }
    await withKv([kvResults(answer)], async (kv, hits) => {
      const res = await kv.getMany(NS, ['a', 'b'])
      assert.deepEqual(hits[0].body, oneOp({ op: 'getMany', ns: NS, keys: ['a', 'b'] }))
      assert.deepEqual(res.missing, ['b'], 'absence is a datum, never computed by difference')
      assert.equal(res.rows.length, 1)
      assert.equal(res.truncated, false)
    })
  })

  it('getPrefix goes in the POST body — never a query string', async () => {
    const answer = { index: 0, op: 'getPrefix', rows: [], truncated: false, nextAfter: null }
    await withKv([kvResults(answer)], async (kv, hits) => {
      await kv.getPrefix(NS, 'quota:acme:', { limit: 50, after: 'quota:acme:a', keysOnly: true })
      assert.equal(hits[0].url, '/api/v1/kv', 'a prefix in a URL is recorded by every access log on the way in (§5.5)')
      assert.deepEqual(hits[0].body, oneOp({
        op: 'getPrefix', ns: NS, prefix: 'quota:acme:', limit: 50, after: 'quota:acme:a', keysOnly: true
      }))
    })
  })

  it('getPrefix omits the optional fields it was not given', async () => {
    const answer = { index: 0, op: 'getPrefix', rows: [], truncated: false, nextAfter: null }
    await withKv([kvResults(answer)], async (kv, hits) => {
      await kv.getPrefix(NS, 'p')
      assert.deepEqual(hits[0].body, oneOp({ op: 'getPrefix', ns: NS, prefix: 'p' }))
    })
  })

  it('listAll walks the keyset cursor and stops when the page is not truncated', async () => {
    const page1 = kvResults({ index: 0, op: 'getPrefix', rows: [{ key: 'p:1', value: 1 }, { key: 'p:2', value: 2 }], truncated: true, nextAfter: 'p:2' })
    const page2 = kvResults({ index: 0, op: 'getPrefix', rows: [{ key: 'p:3', value: 3 }], truncated: false, nextAfter: null })
    await withKv([page1, page2], async (kv, hits) => {
      const keys = []
      for await (const row of kv.listAll(NS, 'p:')) keys.push(row.key)

      assert.deepEqual(keys, ['p:1', 'p:2', 'p:3'])
      assert.equal(hits.length, 2, 'exactly one call per page')
      assert.deepEqual(hits[0].body, oneOp({ op: 'getPrefix', ns: NS, prefix: 'p:' }))
      assert.deepEqual(hits[1].body, oneOp({ op: 'getPrefix', ns: NS, prefix: 'p:', after: 'p:2' }),
        'the second page carries the exclusive cursor, not an offset')
    })
  })
})

describe('KV wire — writes and expiry', () => {
  const applied = (op, extra = {}) => kvResults({ index: 0, op, applied: true, key: 'k', value: { v: 1 }, version: 12, ...extra })

  it('put sends ttlSeconds verbatim', async () => {
    await withKv([applied('put')], async (kv, hits) => {
      const res = await kv.put(NS, 'k', { v: 1 }, { ttlSeconds: 3600 })
      assert.deepEqual(hits[0].body, oneOp({ op: 'put', ns: NS, key: 'k', value: { v: 1 }, ttlSeconds: 3600 }))
      assert.equal(res.applied, true)
      assert.equal(res.version, 12)
    })
  })

  it('put converts the `ttl` duration string to whole seconds, rounding UP', async () => {
    await withKv([applied('put'), applied('put'), applied('put')], async (kv, hits) => {
      await kv.put(NS, 'k', 1, { ttl: '24h' })
      await kv.put(NS, 'k', 1, { ttl: '1h30m' })
      await kv.put(NS, 'k', 1, { ttl: '1500ms' })
      assert.equal(hits[0].body.operations[0].ttlSeconds, 86400)
      assert.equal(hits[1].body.operations[0].ttlSeconds, 5400)
      assert.equal(hits[2].body.operations[0].ttlSeconds, 2, 'rounded UP: a TTL rounded down expires before the window it had to cover')
      for (const h of hits) {
        assert.ok(!('ttl' in h.body.operations[0]), 'the sugar never reaches the wire')
        assert.ok(!('ttlMillis' in h.body.operations[0]), 'ttlMillis does not exist in this product (§20.1)')
      }
    })
  })

  it('put converts `until` into a delta at SEND time, rounding UP', async () => {
    await withKv([applied('put')], async (kv, hits) => {
      await kv.put(NS, 'k', 1, { until: new Date(Date.now() + 10_500) })
      const sent = hits[0].body.operations[0]
      assert.ok(!('until' in sent), 'an absolute instant is never on the wire')
      assert.ok(sent.ttlSeconds === 11 || sent.ttlSeconds === 10, `unexpected ttlSeconds ${sent.ttlSeconds}`)
      assert.ok(sent.ttlSeconds >= 10)
    })
  })

  it('put rejects an `until` already in the past instead of sending a dead TTL', async () => {
    await withKv([], async (kv, hits) => {
      await assert.rejects(
        () => kv.put(NS, 'k', 1, { until: new Date(Date.now() - 1000) }),
        (e) => { assert.match(e.message, /until/); return true }
      )
      assert.equal(hits.length, 0, 'nothing may reach the broker')
    })
  })

  it('put sends forever:true when asked, and refuses forever:false', async () => {
    await withKv([applied('put')], async (kv, hits) => {
      await kv.put(NS, 'k', 1, { forever: true })
      assert.deepEqual(hits[0].body, oneOp({ op: 'put', ns: NS, key: 'k', value: 1, forever: true }))
      await assert.rejects(() => kv.put(NS, 'k', 1, { forever: false }))
    })
  })

  it('put refuses two spellings of the same expiry rather than picking one', async () => {
    await withKv([], async (kv, hits) => {
      await assert.rejects(() => kv.put(NS, 'k', 1, { ttlSeconds: 60, ttl: '1m' }))
      await assert.rejects(() => kv.put(NS, 'k', 1, { ttlSeconds: 60, until: new Date(Date.now() + 1000) }))
      assert.equal(hits.length, 0)
    })
  })

  it('put with no expiry at all still goes to the broker, which owns the rule', async () => {
    // §5.1: the "exactly one of ttlSeconds and forever" rule lives in
    // kv_apply_v1 so that all seven clients and the embedded broker inherit
    // it. The SDK must not re-implement it, or the two can disagree.
    await withKv([kvResults({ index: 0, op: 'put', applied: false })], async (kv, hits) => {
      await kv.put(NS, 'k', 1, {})
      assert.deepEqual(hits[0].body, oneOp({ op: 'put', ns: NS, key: 'k', value: 1 }))
    })
  })

  it('put carries expect and required exactly as given', async () => {
    await withKv([applied('put'), applied('put')], async (kv, hits) => {
      await kv.put(NS, 'k', 1, { ttlSeconds: 60, expect: 90101 })
      await kv.put(NS, 'k', 1, { ttlSeconds: 60, expect: 0, required: true })
      assert.deepEqual(hits[0].body, oneOp({ op: 'put', ns: NS, key: 'k', value: 1, ttlSeconds: 60, expect: 90101 }))
      assert.deepEqual(hits[1].body, oneOp({ op: 'put', ns: NS, key: 'k', value: 1, ttlSeconds: 60, expect: 0, required: true }))
    })
  })

  it('an explicitly undefined or null expect is a client-side bug, not an upsert', async () => {
    await withKv([], async (kv, hits) => {
      await assert.rejects(() => kv.put(NS, 'k', 1, { ttlSeconds: 60, expect: undefined }))
      await assert.rejects(() => kv.put(NS, 'k', 1, { ttlSeconds: 60, expect: null }))
      assert.equal(hits.length, 0, 'a silent downgrade to upsert is the failure this rule exists to prevent')
    })
  })

  it('putIfAbsent has its own op name and refuses a contradictory expect', async () => {
    await withKv([applied('put')], async (kv, hits) => {
      await kv.putIfAbsent(NS, 'k', { v: 1 }, { ttlSeconds: 60 })
      assert.deepEqual(hits[0].body, oneOp({ op: 'putIfAbsent', ns: NS, key: 'k', value: { v: 1 }, ttlSeconds: 60 }))
      await assert.rejects(() => kv.putIfAbsent(NS, 'k', 1, { ttlSeconds: 60, expect: 7 }))
    })
  })

  it('putIfAbsent hands the loser the winner value and version, with applied:false', async () => {
    const loser = kvResults({ index: 0, op: 'put', applied: false, reason: 'exists', key: 'k', value: { owner: 'a' }, version: 90101 })
    await withKv([loser], async (kv) => {
      const res = await kv.putIfAbsent(NS, 'k', { owner: 'b' }, { ttlSeconds: 60 })
      assert.equal(res.applied, false)
      assert.equal(res.reason, 'exists')
      assert.deepEqual(res.value, { owner: 'a' }, 'the loser must not need a second round trip')
      assert.equal(res.version, 90101)
    })
  })

  it('delete returns a WriteResult and carries an optional expect', async () => {
    const gone = kvResults({ index: 0, op: 'delete', applied: true, key: 'k', value: null, version: 13 })
    await withKv([gone, gone], async (kv, hits) => {
      const res = await kv.delete(NS, 'k')
      assert.deepEqual(hits[0].body, oneOp({ op: 'delete', ns: NS, key: 'k' }))
      assert.equal(res.applied, true)
      assert.equal(typeof res, 'object', 'a WriteResult is ALWAYS truthy: `if (await kv.delete(...))` is a bug')

      await kv.delete(NS, 'k', { expect: 13, required: true })
      assert.deepEqual(hits[1].body, oneOp({ op: 'delete', ns: NS, key: 'k', expect: 13, required: true }))
    })
  })

  it('incr sends delta, min and max, and defaults delta to 1', async () => {
    const bumped = kvResults({ index: 0, op: 'incr', applied: true, key: 'k', value: 3, version: 4 })
    await withKv([bumped, bumped], async (kv, hits) => {
      await kv.incr(NS, 'k', 2, { ttlSeconds: 60, max: 100, min: 0 })
      assert.deepEqual(hits[0].body, oneOp({ op: 'incr', ns: NS, key: 'k', delta: 2, ttlSeconds: 60, min: 0, max: 100 }))

      await kv.incr(NS, 'k', undefined, { ttl: '1m' })
      assert.deepEqual(hits[1].body, oneOp({ op: 'incr', ns: NS, key: 'k', delta: 1, ttlSeconds: 60 }))
    })
  })

  it('incr refuses an expect instead of letting the broker raise', async () => {
    await withKv([], async (kv, hits) => {
      await assert.rejects(() => kv.incr(NS, 'k', 1, { ttlSeconds: 60, expect: 3 }))
      assert.equal(hits.length, 0)
    })
  })

  it('incr raises rather than hand back a counter JSON.parse already broke', async () => {
    // §5.4: past 2^53 JSON.parse loses precision SILENTLY. A rate limiter that
    // reads a wrong count is worse than one that fails.
    const huge = kvResults({ index: 0, op: 'incr', applied: true, key: 'k', value: 9007199254740993, version: 4 })
    await withKv([huge], async (kv) => {
      await assert.rejects(() => kv.incr(NS, 'k', 1, { ttlSeconds: 60 }), (e) => {
        assert.match(e.message, /precision|safe integer/i)
        return true
      })
    })
  })

  it('incr with max reports the refusal as applied:false — the admission decision', async () => {
    const refused = kvResults({ index: 0, op: 'incr', applied: false, reason: 'limit', key: 'k', value: 100, version: 9 })
    await withKv([refused], async (kv) => {
      const res = await kv.incr(NS, 'k', 1, { ttlSeconds: 60, max: 100 })
      assert.equal(res.applied, false, 'with max, `applied` IS the admission decision')
      assert.equal(res.reason, 'limit')
      assert.equal(res.value, 100, 'the current value, never the would-be one')
    })
  })

  it('once is putIfAbsent with a default marker value, and reports who won', async () => {
    const won = kvResults({ index: 0, op: 'put', applied: true, key: 'k', value: true, version: 1 })
    const lost = kvResults({ index: 0, op: 'put', applied: false, reason: 'exists', key: 'k', value: true, version: 1 })
    await withKv([won, lost], async (kv, hits) => {
      const first = await kv.once(NS, 'k', { ttlSeconds: 3600 })
      assert.deepEqual(hits[0].body, oneOp({ op: 'putIfAbsent', ns: NS, key: 'k', value: true, ttlSeconds: 3600 }))
      assert.equal(first.won, true)

      const second = await kv.once(NS, 'k', { ttlSeconds: 3600 })
      assert.equal(second.won, false)
      assert.equal(second.value, true)
    })
  })
})

describe('KV wire — envelopes and errors', () => {
  it('a short results array fails loudly instead of being misattributed', async () => {
    await withKv([{ status: 200, body: { results: [] } }], async (kv) => {
      await assert.rejects(() => kv.get(NS, 'k'), (e) => {
        assert.match(e.message, /result/i)
        return true
      })
    })
  })

  it('a lost `required` precondition comes back as a verdict, not a throw', async () => {
    // §8.3: HTTP 200 with ok:false. It is the expected outcome of every
    // legitimate redelivery and must not enter a retry policy.
    const verdict = {
      status: 200,
      body: { ok: false, reason: 'kv_precondition', failedIndex: 0, kvReason: 'exists', version: 90101, value: { owner: 'a' } }
    }
    await withKv([verdict], async (kv) => {
      const res = await kv.putIfAbsent(NS, 'k', 1, { ttlSeconds: 60, required: true })
      assert.equal(res.applied, false)
      assert.equal(res.precondition, true)
      assert.equal(res.reason, 'exists')
      assert.deepEqual(res.value, { owner: 'a' })
      assert.equal(res.version, 90101)
    })
  })

  it('the KV error envelope reaches the caller as `.code`, never as prose to match', async () => {
    // The KV routes put the CODE in `error` (§9.5's closed taxonomy); the HTTP
    // layer maps `body.error` onto `.message`. The SDK restores it into
    // `.code` so callers branch on a code here exactly as they do on a proxy
    // 403, and never on a message.
    //
    // The fixture used to be `404 {error:'not_found', reason:'kv_not_enabled'}`,
    // the answer of a cell built without the KV boot flag. That flag is gone and
    // nothing in the broker can answer 404 for a KV surface any more, so the
    // fixture is the one refusal that IS still reachable: the operator's runtime
    // kill switch, 503 + Retry-After. Handling it is not feature detection --
    // it is a paused surface that will come back, and note that the client
    // treats it as exactly that: with the default `retryAttempts: 3` this 503 is
    // retried with backoff like any other 5xx. `retryAttempts: 1` here so the
    // plan is one entry and the assertion is about the envelope, not the policy.
    const paused = {
      status: 503,
      headers: { 'retry-after': '1' },
      body: { error: 'kv_disabled', reason: 'kv_disabled' }
    }
    await withKv([paused], async (kv, hits) => {
      await assert.rejects(() => kv.get(NS, 'k'), (e) => {
        assert.equal(e.status, 503)
        assert.equal(e.code, 'kv_disabled')
        return true
      })
      assert.equal(hits.length, 1)
    }, { retryAttempts: 1 })
  })

  it('refuses a missing namespace or key before spending a request', async () => {
    await withKv([], async (kv, hits) => {
      await assert.rejects(() => kv.get('', 'k'))
      await assert.rejects(() => kv.get(NS, ''))
      await assert.rejects(() => kv.put(NS, undefined, 1, { ttlSeconds: 1 }))
      assert.equal(hits.length, 0)
    })
  })
})
