/**
 * Locks: the wire contract of `POST /api/v1/locks`, the `check` KV op, and
 * what the lock handle does around them.
 *
 * Same method as kvWire.test.js: a scripted plan server and deepEqual on the
 * EXACT request body, because an extra field is as much a contract break as a
 * missing one.
 *
 * What lives in the CLIENT and nowhere else, and is pinned here:
 *   * `acquire()` resolves a BOOLEAN (every other verdict of this SDK is an
 *     object, and objects are truthy);
 *   * the handle always sends its owner, which is what makes a retry of a
 *     call whose answer was lost come back with the same permit;
 *   * a renew's NEW token replaces the old one for the guard and the release;
 *   * a guard that lost to the handle's own renewal is sent again with the
 *     new token, and one that lost to another holder is returned and marks
 *     the lock lost;
 *   * a transaction that asked for a guard never goes out without one.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen, Lock, LOCK_NOT_HELD } from '../../client-v2/index.js'
import { createServer } from 'node:http'

import { withPlanServer, ok, kvResults } from './_planServer.js'

const guardOf = (name, slot, token) => ({
  op: 'check', ns: 'queen-locks', key: `${name}#${slot}`, expect: token, required: true
})
const granted = (name, token, extra = {}) => ok({
  results: [{
    index: 0, op: 'acquire', name, acquired: true, slot: 0, token, owner: 'o',
    guard: guardOf(name, 0, token), ...extra
  }]
})
const refused = (name, reason = 'held') => ok({
  results: [{ index: 0, op: 'acquire', name, acquired: false, reason, holders: [{ slot: 0, owner: 'other' }] }]
})
const renewed = (name, token, slot = 0) => ok({
  results: [{ index: 0, op: 'renew', name, renewed: true, slot, token, guard: guardOf(name, slot, token) }]
})
const lostRenew = (name) => ok({
  results: [{ index: 0, op: 'renew', name, renewed: false, reason: 'lost', slot: 0, holders: [{ slot: 0, owner: 'other' }] }]
})
const released = (name, yes = true) => ok({
  results: [yes
    ? { index: 0, op: 'release', name, released: true, slot: 0 }
    : { index: 0, op: 'release', name, released: false, reason: 'lost', slot: 0, holders: [] }]
})

/** Run `fn(queen, hits)` against a plan server, closing the client afterwards. */
async function withQueen(plan, run) {
  await withPlanServer(plan, ok({ results: [] }), async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

const opOf = (hit) => hit.body.operations[0]

/**
 * A server that answers by what it is asked, for the tests where the ORDER of
 * two calls in flight is the point (a background renew beside a release, a
 * commit beside a renew) and a positional plan cannot say it.
 * `answer({ url, body }, reply)` calls `reply(json)` now or later.
 */
async function withAnsweringServer(answer, run) {
  const hits = []
  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      const hit = { method: req.method, url: req.url, body: raw ? JSON.parse(raw) : null }
      hits.push(hit)
      answer(hit, (json) => {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(json))
      })
    })
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const queen = new Queen({ url: `http://127.0.0.1:${server.address().port}`, handleSignals: false })
  try {
    await run(queen, hits)
  } finally {
    await queen.close()
    await new Promise(resolve => server.close(resolve))
  }
}

describe('KV wire — check', () => {
  it('check sends its expect and nothing else, and answers a WriteResult', async () => {
    const plan = [
      kvResults({ index: 0, op: 'check', applied: true, key: 'k', version: 7 }),
      kvResults({ index: 0, op: 'check', applied: false, reason: 'version', key: 'k', value: { n: 2 }, version: 9 })
    ]
    await withQueen(plan, async (queen, hits) => {
      const held = await queen.kv.check('orders', 'k', { expect: 7 })
      assert.deepEqual(hits[0].body, { operations: [{ op: 'check', ns: 'orders', key: 'k', expect: 7 }] })
      assert.equal(hits[0].url, '/api/v1/kv')
      assert.equal(held.applied, true)
      assert.equal('value' in held, false, 'a held check hands back no value')

      const stale = await queen.kv.check('orders', 'k', { expect: 7, required: true })
      assert.deepEqual(opOf(hits[1]), { op: 'check', ns: 'orders', key: 'k', expect: 7, required: true })
      assert.equal(stale.applied, false)
      assert.equal(stale.reason, 'version')
      assert.equal(stale.version, 9)
    })
  })

  it('expect: 0 asks for absence, and a check with no expect is refused before the request', async () => {
    await withQueen([kvResults({ index: 0, op: 'check', applied: true, key: 'k', version: 0 })], async (queen, hits) => {
      await queen.kv.check('orders', 'k', { expect: 0 })
      assert.deepEqual(opOf(hits[0]), { op: 'check', ns: 'orders', key: 'k', expect: 0 })
      await assert.rejects(queen.kv.check('orders', 'k'), /check needs `expect`/)
      await assert.rejects(queen.kv.check('orders', 'k', { expect: null }), /`expect` was written but has no value/)
      // It writes nothing, so it takes no lifetime: none is sent even if given.
      assert.equal(hits.length, 1)
    })
  })

  it('check rides a transaction in the kv array, built by the same code', async () => {
    await withQueen([ok({ success: true, results: [] })], async (queen, hits) => {
      await queen.transaction()
        .kv.check('queen-locks', 'job#0', { expect: 41, required: true })
        .kv.put('work', 'state', { n: 1 }, { forever: true })
        .commit()
      assert.equal(hits[0].url, '/api/v1/transaction')
      assert.deepEqual(hits[0].body.kv, [
        { op: 'check', ns: 'queen-locks', key: 'job#0', expect: 41, required: true },
        { op: 'put', ns: 'work', key: 'state', value: { n: 1 }, forever: true }
      ])
    })
  })
})

describe('Locks wire — the four operations', () => {
  it('acquire, renew, release and get send exactly their fields', async () => {
    const plan = [
      granted('daily-report', 100),
      renewed('daily-report', 101),
      released('daily-report'),
      ok({ results: [{ index: 0, op: 'get', name: 'daily-report', held: false, holders: [] }] })
    ]
    await withQueen(plan, async (queen, hits) => {
      const a = await queen.locks.acquire('daily-report', { ttl: '30s', owner: 'o' })
      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/locks')
      assert.deepEqual(hits[0].body, {
        operations: [{ op: 'acquire', name: 'daily-report', ttlSeconds: 30, owner: 'o' }]
      })
      assert.equal(a.acquired, true)
      assert.equal(a.token, 100)
      assert.deepEqual(a.guard, guardOf('daily-report', 0, 100))

      const r = await queen.locks.renew('daily-report', { token: 100, ttlSeconds: 45, owner: 'o' })
      assert.deepEqual(opOf(hits[1]), { op: 'renew', name: 'daily-report', token: 100, ttlSeconds: 45, owner: 'o' })
      assert.equal(r.token, 101, 'a renew answers a NEW token')

      const d = await queen.locks.release('daily-report', { token: 101 })
      assert.deepEqual(opOf(hits[2]), { op: 'release', name: 'daily-report', token: 101 })
      assert.equal(d.released, true)

      const g = await queen.locks.get('daily-report')
      assert.deepEqual(opOf(hits[3]), { op: 'get', name: 'daily-report' })
      assert.equal(g.held, false)
    })
  })

  it('a semaphore sends its limit and its slot; a lock sends neither', async () => {
    const plan = [
      ok({ results: [{ index: 0, op: 'acquire', name: 'gpu', acquired: true, slot: 2, token: 7, owner: 'o', guard: guardOf('gpu', 2, 7) }] }),
      renewed('gpu', 8, 2),
      ok({ results: [{ index: 0, op: 'release', name: 'gpu', released: true, slot: 2 }] })
    ]
    await withQueen(plan, async (queen, hits) => {
      await queen.locks.acquire('gpu', { ttlSeconds: 60, owner: 'o', limit: 4 })
      assert.deepEqual(opOf(hits[0]), { op: 'acquire', name: 'gpu', ttlSeconds: 60, owner: 'o', limit: 4 })
      await queen.locks.renew('gpu', { token: 7, slot: 2, ttlSeconds: 60 })
      assert.deepEqual(opOf(hits[1]), { op: 'renew', name: 'gpu', token: 7, ttlSeconds: 60, slot: 2 })
      await queen.locks.release('gpu', { token: 8, slot: 2 })
      assert.deepEqual(opOf(hits[2]), { op: 'release', name: 'gpu', token: 8, slot: 2 })
    })
  })

  it('batch carries several locks in one call and insists on aligned results', async () => {
    const two = ok({
      results: [
        { index: 0, op: 'get', name: 'a', held: false, holders: [] },
        { index: 1, op: 'get', name: 'b', held: false, holders: [] }
      ]
    })
    await withQueen([two, ok({ results: [{ index: 0, op: 'get', name: 'a', held: false, holders: [] }] })], async (queen, hits) => {
      const ops = [{ op: 'get', name: 'a' }, { op: 'get', name: 'b' }]
      const got = await queen.locks.batch(ops)
      assert.deepEqual(hits[0].body, { operations: ops })
      assert.equal(got.length, 2)
      await assert.rejects(queen.locks.batch(ops), /expected \{"results":\[\.\.\.\]\} with 2 element/)
    })
  })

  it('what the broker would refuse is refused before the request', async () => {
    await withQueen([], async (queen, hits) => {
      const locks = queen.locks
      await assert.rejects(locks.acquire('a', {}), /a lifetime is required/)
      await assert.rejects(locks.acquire('a', { forever: true }), /there is no `forever`/)
      await assert.rejects(locks.acquire('a', { ttl: '30s', ttlSeconds: 30 }), /declared twice/)
      await assert.rejects(locks.acquire('a', { ttlSeconds: 1.5 }), /a lifetime is required/)
      await assert.rejects(locks.acquire('a#b', { ttl: '1s' }), /without '#'/)
      await assert.rejects(locks.acquire('', { ttl: '1s' }), /a name is a non-empty string/)
      await assert.rejects(locks.acquire('a', { ttl: '1s', limit: 0 }), /limit is a whole number/)
      await assert.rejects(locks.acquire('a', { ttl: '1s', owner: '' }), /owner is a non-empty string/)
      await assert.rejects(locks.renew('a', { ttl: '1s' }), /renew needs the token/)
      await assert.rejects(locks.release('a', {}), /release needs the token/)
      assert.throws(() => queen.lock('a', { ttl: '1s', limit: 3 }), /queen\.semaphore/)
      assert.throws(() => queen.lock('a', {}), /a lifetime is required/)
      assert.throws(() => queen.lock('a', { ttl: '3s', renewEvery: '3s' }), /renewEvery must be shorter/)
      assert.equal(hits.length, 0)
    })
  })

  it('a ttl is rounded UP to the second, never down', async () => {
    await withQueen([granted('a', 1)], async (queen, hits) => {
      await queen.locks.acquire('a', { ttl: '1500ms' })
      assert.equal(opOf(hits[0]).ttlSeconds, 2)
    })
  })
})

describe('Lock handle', () => {
  it('acquire resolves a boolean, and the handle always names its owner', async () => {
    await withQueen([refused('job'), granted('job', 100), released('job')], async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      assert.ok(lock instanceof Lock)
      assert.equal(lock.held, false)
      assert.equal(lock.token, null)

      assert.equal(await lock.acquire(), false, 'held by somebody else is `false`, not a truthy object')
      const sent = opOf(hits[0])
      assert.equal(sent.owner, lock.owner)
      assert.match(sent.owner, /^.+:\d+:[0-9a-f]{12}$/, 'host:pid:random, unique per handle')
      assert.deepEqual(Object.keys(sent).sort(), ['name', 'op', 'owner', 'ttlSeconds'])

      assert.equal(await lock.acquire(), true)
      assert.equal(opOf(hits[1]).owner, lock.owner, 'the same owner on the retry')
      assert.equal(lock.held, true)
      assert.equal(lock.token, 100)
      assert.equal(lock.slot, 0)
      assert.deepEqual(lock.guard(), guardOf('job', 0, 100))
      assert.ok(lock.validUntil > Date.now() + 25_000 && lock.validUntil <= Date.now() + 30_000)
      assert.equal(await lock.acquire(), true, 'already held: no call')
      assert.equal(hits.length, 2)

      assert.equal(await lock.release(), true)
      assert.deepEqual(opOf(hits[2]), { op: 'release', name: 'job', token: 100, slot: 0 })
      assert.equal(lock.held, false)
      assert.equal(lock.signal.aborted, false, 'a release is not a loss')
      assert.throws(() => lock.guard(), (e) => e.code === LOCK_NOT_HELD)
    })
  })

  it('two handles of one lock have two owners', async () => {
    await withQueen([], async (queen) => {
      const a = queen.lock('job', { ttl: '30s' })
      const b = queen.lock('job', { ttl: '30s' })
      assert.notEqual(a.owner, b.owner)
      assert.equal(queen.lock('job', { ttl: '30s', owner: 'cron-7' }).owner, 'cron-7')
    })
  })

  it('acquire with a wait comes back until the permit is free', async () => {
    await withQueen([refused('job'), refused('job', 'contended'), granted('job', 5), released('job')], async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false, retryMinMs: 5, retryMaxMs: 10 })
      assert.equal(await lock.acquire({ wait: '5s' }), true)
      assert.equal(hits.length, 3)
      await lock.release()
    })
  })

  it('a wait that runs out resolves false, and a signal gives it up early', async () => {
    // The plan server's default answer is not a lock result: script every attempt.
    const always = Array.from({ length: 50 }, () => refused('job'))
    await withQueen(always, async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', retryMinMs: 5, retryMaxMs: 10 })
      const t0 = Date.now()
      assert.equal(await lock.acquire({ wait: '60ms' }), false)
      assert.ok(Date.now() - t0 >= 50 && hits.length >= 2)

      const ac = new AbortController()
      setTimeout(() => ac.abort(), 20)
      const t1 = Date.now()
      assert.equal(await lock.acquire({ wait: '10s', signal: ac.signal }), false)
      assert.ok(Date.now() - t1 < 2000, 'the signal ended the wait')
    })
  })

  it('a renew puts the NEW token in the guard and in the release', async () => {
    await withQueen([granted('job', 100), renewed('job', 101), released('job')], async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      await lock.acquire()
      assert.equal(await lock.renew(), true)
      assert.deepEqual(opOf(hits[1]), { op: 'renew', name: 'job', token: 100, ttlSeconds: 30, slot: 0, owner: lock.owner })
      assert.equal(lock.token, 101)
      assert.deepEqual(lock.guard(), guardOf('job', 0, 101))
      await lock.release()
      assert.equal(opOf(hits[2]).token, 101)
    })
  })

  it('renews in the background, every third of the lifetime unless told otherwise', async () => {
    let token = 1
    const answer = (hit, reply) => {
      const op = opOf(hit)
      if (op.op === 'acquire') reply(granted('job', token).body)
      else if (op.op === 'renew') reply(renewed('job', ++token).body)
      else reply(released('job').body)
    }
    await withAnsweringServer(answer, async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '2s', renewEvery: '40ms' })
      await lock.acquire()
      await new Promise(r => setTimeout(r, 150))
      const renews = hits.map(opOf).filter(op => op.op === 'renew')
      assert.ok(renews.length >= 2, `renewed in the background (${renews.length} renews)`)
      assert.deepEqual(renews.slice(0, 2).map(op => op.token), [1, 2], 'each renew carries the token of the one before')
      assert.equal(lock.held, true)
      assert.equal(lock.token, token)
      await lock.release()
      assert.equal(opOf(hits.at(-1)).op, 'release')
      assert.equal(opOf(hits.at(-1)).token, token, 'released with the last token')
      const calls = hits.length
      await new Promise(r => setTimeout(r, 100))
      assert.equal(hits.length, calls, 'a released lock renews no more')
    })
  })

  it('a renew the broker refuses is a loss: held false, signal aborted, onLost called', async () => {
    await withQueen([granted('job', 100), lostRenew('job')], async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      const seen = []
      lock.onLost(reason => seen.push(reason))
      await lock.acquire()
      const signal = lock.signal
      assert.equal(await lock.renew(), false)
      assert.equal(lock.held, false)
      assert.equal(lock.token, null)
      assert.equal(signal.aborted, true)
      assert.equal(signal.reason.code, LOCK_NOT_HELD)
      assert.deepEqual(seen, ['renew'])
      assert.equal(await lock.release(), false, 'nothing to give back, and no call')
      assert.equal(hits.length, 2)
    })
  })

  it('a lifetime that runs out with no renew is a loss on this machine too', async () => {
    await withQueen([granted('job', 100)], async (queen) => {
      const lock = queen.lock('job', { ttl: '1s', autoRenew: false })
      const seen = []
      lock.onLost(reason => seen.push(reason))
      await lock.acquire()
      assert.equal(lock.held, true)
      await new Promise(r => setTimeout(r, 1100))
      assert.equal(lock.held, false, 'past its deadline a handle does not claim to hold')
      assert.deepEqual(seen, ['expired'])
      assert.equal(lock.signal.aborted, true)
    })
  })

  it('run acquires, hands the lock to fn, and releases whatever fn does', async () => {
    const plan = [granted('job', 100), released('job'), refused('job'), granted('job', 200), released('job')]
    await withQueen(plan, async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      const out = await lock.run(async (l) => {
        assert.equal(l.held, true)
        return 42
      })
      assert.deepEqual(out, { acquired: true, value: 42 })
      assert.equal(opOf(hits[1]).op, 'release')

      assert.deepEqual(await lock.run(() => { throw new Error('never runs') }), { acquired: false })

      await assert.rejects(lock.run(() => { throw new Error('boom') }), /boom/)
      assert.equal(opOf(hits[4]).op, 'release', 'released although fn threw')
      assert.equal(lock.held, false)
    })
  })

  it('close() gives back what the handles still hold', async () => {
    await withPlanServer([granted('job', 100), released('job')], ok({ results: [] }), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      const lock = queen.lock('job', { ttl: '30s' })
      await lock.acquire()
      await queen.close()
      assert.equal(hits.length, 2)
      assert.deepEqual(opOf(hits[1]), { op: 'release', name: 'job', token: 100, slot: 0 })
      assert.equal(lock.held, false)
    })
  })

  it('a semaphore handle is one permit of several', async () => {
    const plan = [
      ok({ results: [{ index: 0, op: 'acquire', name: 'gpu', acquired: true, slot: 3, token: 9, owner: 'o', guard: guardOf('gpu', 3, 9) }] }),
      ok({ results: [{ index: 0, op: 'release', name: 'gpu', released: true, slot: 3 }] })
    ]
    await withQueen(plan, async (queen, hits) => {
      const permit = queen.semaphore('gpu', 4, { ttl: '1m', autoRenew: false })
      assert.equal(permit.limit, 4)
      assert.equal(await permit.acquire(), true)
      assert.equal(opOf(hits[0]).limit, 4)
      assert.equal(permit.slot, 3)
      assert.deepEqual(permit.guard(), guardOf('gpu', 3, 9))
      await permit.release()
      assert.deepEqual(opOf(hits[1]), { op: 'release', name: 'gpu', token: 9, slot: 3 })
    })
  })
})

describe('Transaction wire — guard(lock)', () => {
  const committed = ok({ success: true, results: [] })
  const lostTo = (failedIndex, kvReason, value, version) => ok({
    success: false, reason: 'kv_precondition', error: 'QKV', results: [], ok: false,
    failedIndex, kvReason, value, version
  })

  it('the guard is the first op of the kv rider, at the token held when commit sends', async () => {
    await withQueen([granted('job', 100), renewed('job', 101), committed, released('job')], async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      await lock.acquire()
      const txn = queen.transaction()
        .guard(lock)
        .queue('reports').push([{ data: { n: 1 }, transactionId: 't1' }])
        .kv.put('work', 'state', { n: 1 }, { forever: true })
      await lock.renew()                    // after the guard was asked for, before commit
      const res = await txn.commit()
      assert.equal(res.success, true)
      assert.equal(hits[2].url, '/api/v1/transaction')
      assert.deepEqual(hits[2].body.kv, [
        guardOf('job', 0, 101),
        { op: 'put', ns: 'work', key: 'state', value: { n: 1 }, forever: true }
      ])
      await lock.release()
    })
  })

  it('a guard that lost to the lock\'s own renewal is sent again with the new token', async () => {
    // The race, in the order that makes it: the commit is sent with token
    // 100; the lock's renewal is sent and APPLIED while the commit is on its
    // way; the broker then judges the commit against token 101. So the first
    // commit is held until the renew has arrived, and answered after it.
    let heldCommit = null
    let commits = 0
    const answer = (hit, reply) => {
      if (hit.url === '/api/v1/transaction') {
        commits++
        if (commits === 1) heldCommit = reply
        else reply({ success: true, results: [] })
        return
      }
      const op = opOf(hit)
      if (op.op === 'acquire') return reply(granted('job', 100).body)
      if (op.op === 'release') return reply(released('job').body)
      // The renew: answered first, then the commit it overtook.
      reply(renewed('job', 101).body)
      // Two pushed items come first in the flat index space: the guard is 2.
      heldCommit(lostTo(2, 'version', { owner: op.owner }, 101).body)
    }
    await withAnsweringServer(answer, async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      await lock.acquire()
      const commit = queen.transaction()
        .guard(lock)
        .queue('reports').push([{ data: 1, transactionId: 'a' }, { data: 2, transactionId: 'b' }])
        .commit()
      while (!heldCommit) await new Promise(r => setTimeout(r, 2))
      await lock.renew()
      const res = await commit
      assert.equal(res.success, true, 'the step committed on the second send')
      const sends = hits.filter(h => h.url === '/api/v1/transaction')
      assert.equal(sends.length, 2)
      assert.equal(sends[0].body.kv[0].expect, 100)
      assert.equal(sends[1].body.kv[0].expect, 101)
      assert.deepEqual(sends[1].body.operations, sends[0].body.operations, 'the same step, not a second one')
      assert.equal(lock.held, true)
      assert.equal(lock.token, 101)
    })
  })

  it('a guard that lost to another holder is the verdict, and the lock reports lost', async () => {
    const plan = [granted('job', 100), lostTo(0, 'version', { owner: 'somebody-else' }, 250)]
    await withQueen(plan, async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      const seen = []
      lock.onLost(r => seen.push(r))
      await lock.acquire()
      const res = await queen.transaction().guard(lock).kv.put('w', 'k', 1, { forever: true }).commit()
      assert.equal(res.success, false, 'returned, not thrown — like once()')
      assert.equal(res.reason, 'kv_precondition')
      assert.equal(hits.filter(h => h.url === '/api/v1/transaction').length, 1, 'not sent again')
      assert.equal(lock.held, false)
      assert.deepEqual(seen, ['guard'])
    })
  })

  it('an expired permit is a lost guard too', async () => {
    await withQueen([granted('job', 100), lostTo(0, 'absent', null, 0)], async (queen) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      await lock.acquire()
      const res = await queen.transaction().guard(lock).kv.put('w', 'k', 1, { forever: true }).commit()
      assert.equal(res.success, false)
      assert.equal(lock.held, false)
    })
  })

  it('a precondition that is not the guard\'s leaves the lock alone', async () => {
    // kv = [guard, once]: flat index 1 is the once marker.
    await withQueen([granted('job', 100), lostTo(1, 'exists', true, 77), released('job')], async (queen) => {
      const lock = queen.lock('job', { ttl: '30s', autoRenew: false })
      await lock.acquire()
      const res = await queen.transaction().guard(lock).once('idem', 'order-1', { ttl: '1h' }).commit()
      assert.equal(res.success, false)
      assert.equal(lock.held, true, 'the marker lost, not the lock')
      await lock.release()
    })
  })

  it('a step that asked for a guard never goes out without one', async () => {
    await withQueen([], async (queen, hits) => {
      const lock = queen.lock('job', { ttl: '30s' })
      const txn = queen.transaction().guard(lock).kv.put('w', 'k', 1, { forever: true })
      await assert.rejects(txn.commit(), (e) => e.code === LOCK_NOT_HELD)
      assert.equal(hits.length, 0)
      assert.throws(() => queen.transaction().guard({}), /guard\(\) takes a lock/)
    })
  })
})
