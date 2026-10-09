/**
 * Locks integration suite: `queen.lock`, `queen.semaphore`, `queen.locks`,
 * `kv.check` and `transaction().guard(lock)` against a real broker.
 *
 * Repeatable for the reason kv.js gives: every run starts on a fresh broker.
 * Every lock here is named under `test-` and carries a lifetime, so a run that
 * goes wrong leaves nothing that does not expire by itself.
 *
 * What only a real broker can show, and what these are for:
 *   * one holder: two handles, one permit;
 *   * the retry of a call whose answer was lost comes back with the SAME
 *     permit (the owner);
 *   * an expired lock goes to the next handle with a HIGHER token, and the
 *     old handle's guarded step commits nothing — the message is not pushed;
 *   * a guarded step survives the lock's own renewal;
 *   * a semaphore never grants more than its limit to a crowd.
 */

import { sleep } from './_kvtimers.js'

const fail = (message) => ({ success: false, message })

/** A broker alone raises its cluster version to 5 on its first tick; `check` needs it. */
async function checksAreServed(client) {
  for (let i = 0; i < 100; i++) {
    try {
      await client.kv.check('test-locks-probe', 'k', { expect: 0 })
      return
    } catch (error) {
      if (error.status !== 503) throw error
      await sleep(100)
    }
  }
  throw new Error('the broker never served a check (cluster version below 5?)')
}

async function drain(client, queue) {
  const seen = []
  for (;;) {
    const msgs = await client.queue(queue).batch(50).wait(false).pop()
    if (!msgs || msgs.length === 0) return seen
    seen.push(...msgs.map(m => m.data))
    await client.ack(msgs, true)
  }
}

export async function lockHasOneHolderAndAGuardedStepCommits(client) {
  await checksAreServed(client)
  const name = 'test-lock/one-holder'
  const a = client.lock(name, { ttl: '30s' })
  const b = client.lock(name, { ttl: '30s' })
  try {
    if ((await a.acquire()) !== true) return fail('the first handle did not acquire')
    if ((await b.acquire()) !== false) return fail('a second handle acquired a held lock')
    if (a.token === null || a.slot !== 0) return fail(`the holder has no token or slot: ${a.token} ${a.slot}`)

    const who = await client.locks.get(name)
    if (who.held !== true || who.holders.length !== 1 || who.holders[0].owner !== a.owner ||
        who.holders[0].token !== a.token || !who.holders[0].expiresAt) {
      return fail(`get does not show the holder: ${JSON.stringify(who)}`)
    }

    // The permit is a KV row and nothing else.
    const row = await client.kv.get('queen-locks', `${name}#0`)
    if (row.found !== true || row.version !== a.token || row.value.owner !== a.owner) {
      return fail(`the permit's row is not what the lock says: ${JSON.stringify(row)}`)
    }

    const queue = 'test-lock-one-holder'
    const res = await client.transaction()
      .guard(a)
      .queue(queue).push([{ data: { step: 1 } }])
      .commit()
    if (res.success !== true) return fail(`the holder's guarded step did not commit: ${JSON.stringify(res)}`)
    const got = await drain(client, queue)
    if (got.length !== 1 || got[0].step !== 1) return fail(`expected the one guarded message, got ${JSON.stringify(got)}`)

    if ((await a.release()) !== true) return fail('release did not remove the permit')
    if ((await b.acquire()) !== true) return fail('the lock was not free after its release')
    return { success: true, message: 'one holder, get and the KV row agree, a guarded step commits, release frees it' }
  } finally {
    await a.release().catch(() => {})
    await b.release().catch(() => {})
  }
}

export async function lockRetryWithTheSameOwnerIsTheSamePermit(client) {
  const name = 'test-lock/retry-owner'
  const owner = `test-owner-${Date.now()}`
  // What a client does when the answer to its acquire never arrived: the same
  // call again.
  const first = await client.locks.acquire(name, { ttl: '30s', owner })
  const again = await client.locks.acquire(name, { ttl: '30s', owner })
  if (first.acquired !== true || again.acquired !== true) {
    return fail(`both attempts must answer the permit: ${JSON.stringify([first, again])}`)
  }
  if (again.already !== true || again.token !== first.token) {
    return fail(`the retry must answer the SAME permit, marked already: ${JSON.stringify(again)}`)
  }
  // The same for a renew: its answer is lost, the old token is sent again.
  const renewed = await client.locks.renew(name, { token: first.token, ttl: '30s', owner })
  const resent = await client.locks.renew(name, { token: first.token, ttl: '30s', owner })
  if (renewed.renewed !== true || resent.renewed !== true || !(resent.token > renewed.token)) {
    return fail(`a renew sent again by its owner must be carried through: ${JSON.stringify([renewed, resent])}`)
  }
  // Somebody else's stale token is not.
  const stranger = await client.locks.renew(name, { token: first.token, ttl: '30s', owner: 'somebody-else' })
  if (stranger.renewed !== false || stranger.reason !== 'lost' || stranger.holders[0].owner !== owner) {
    return fail(`a stranger's stale token must be lost: ${JSON.stringify(stranger)}`)
  }
  const done = await client.locks.release(name, { token: resent.token })
  return {
    success: done.released === true,
    message: 'acquire and renew sent again by their owner answer the permit; a stranger\'s stale token is lost'
  }
}

export async function lockExpiredIsTakenOverAndTheOldHolderCommitsNothing(client) {
  await checksAreServed(client)
  const name = 'test-lock/expiry'
  const queue = 'test-lock-expiry'
  // The old holder does not renew: it is "paused" for longer than its lease.
  const old = client.lock(name, { ttl: '1s', autoRenew: false })
  const next = client.lock(name, { ttl: '30s' })
  try {
    if (!(await old.acquire())) return fail('the first holder did not acquire')
    const oldToken = old.token
    const staleGuard = old.guard()
    await sleep(1300)

    if (!(await next.acquire())) return fail('an expired lock was not free for the next holder')
    if (!(next.token > oldToken)) return fail(`the next holder's token ${next.token} is not above ${oldToken}`)

    // The old holder wakes up and sends the step it was about to send.
    const stale = await client.transaction()
      .kv.check(staleGuard.ns, staleGuard.key, { expect: staleGuard.expect, required: true })
      .queue(queue).push([{ data: { from: 'old' } }])
      .commit()
    if (stale.success !== false || stale.reason !== 'kv_precondition') {
      return fail(`the old holder's step must roll back: ${JSON.stringify(stale)}`)
    }
    if (old.held !== false) return fail('a handle past its lifetime must not claim to hold')

    const ok = await client.transaction().guard(next).queue(queue).push([{ data: { from: 'next' } }]).commit()
    if (ok.success !== true) return fail(`the new holder's step did not commit: ${JSON.stringify(ok)}`)

    const got = await drain(client, queue)
    if (got.length !== 1 || got[0].from !== 'next') {
      return fail(`only the new holder's message may exist, got ${JSON.stringify(got)}`)
    }
    return { success: true, message: 'takeover with a higher token; the old holder\'s guarded step pushed nothing' }
  } finally {
    await next.release().catch(() => {})
  }
}

export async function lockGuardedStepSurvivesItsOwnRenewal(client) {
  await checksAreServed(client)
  const name = 'test-lock/renew-under-load'
  const queue = 'test-lock-renew'
  // A short lease renewed every 100 ms, and steps committed as fast as they
  // go: some of them are judged after a renew moved the token.
  const lock = client.lock(name, { ttl: '2s', renewEvery: '100ms' })
  try {
    if (!(await lock.acquire())) return fail('did not acquire')
    const first = lock.token
    let committed = 0
    const end = Date.now() + 1500
    while (Date.now() < end) {
      const res = await client.transaction()
        .guard(lock)
        .queue(queue).push([{ data: { n: committed } }])
        .commit()
      if (res.success !== true) return fail(`step ${committed} did not commit while the lock was held: ${JSON.stringify(res)}`)
      committed++
    }
    if (!(lock.token > first)) return fail('the lock never renewed during the run; the test proved nothing')
    if (lock.held !== true) return fail('the lock was lost under its own renewal')
    const got = await drain(client, queue)
    if (got.length !== committed) return fail(`${committed} steps committed, ${got.length} messages exist`)
    return { success: true, message: `${committed} guarded steps across ${lock.token - first > 0 ? 'several' : 'no'} renewals, each committed once` }
  } finally {
    await lock.release().catch(() => {})
  }
}

export async function lockRunHoldsForTheWorkAndWaitsItsTurn(client) {
  const name = 'test-lock/run'
  const order = []
  const work = (id, ms) => async () => {
    order.push(`${id}:in`)
    await sleep(ms)
    order.push(`${id}:out`)
    return id
  }
  const a = client.lock(name, { ttl: '10s' })
  const b = client.lock(name, { ttl: '10s' })
  const [ra, rb] = await Promise.all([
    a.run(work('a', 300), { wait: '10s' }),
    b.run(work('b', 50), { wait: '10s' })
  ])
  if (!ra.acquired || !rb.acquired) return fail(`both must get their turn: ${JSON.stringify([ra, rb])}`)
  // Never interleaved: in, out, in, out.
  const interleaved = order[0].split(':')[0] !== order[1].split(':')[0]
  if (interleaved || order.length !== 4) return fail(`the two runs overlapped: ${order.join(' ')}`)
  const none = await client.lock(name, { ttl: '10s' }).run(async () => {
    return client.lock(name, { ttl: '10s' }).run(async () => 'inner')
  })
  if (none.value.acquired !== false) return fail('a second handle ran inside the first one\'s hold')
  const free = await client.locks.get(name)
  return {
    success: free.held === false,
    message: `two runs took turns (${order.join(' ')}), and the lock is free afterwards`
  }
}

export async function semaphoreNeverGrantsMoreThanItsLimit(client) {
  const name = 'test-sem/crowd'
  const LIMIT = 3
  const permits = Array.from({ length: 12 }, () => client.semaphore(name, LIMIT, { ttl: '30s' }))
  try {
    const got = await Promise.all(permits.map(p => p.acquire()))
    let holders = permits.filter((_, i) => got[i])
    if (holders.length < 1 || holders.length > LIMIT) return fail(`${holders.length} permits granted of ${LIMIT}`)
    // A crowd can leave a permit free (each gives up after a few lost races):
    // one at a time, it fills.
    for (const p of permits) {
      if (holders.length === LIMIT) break
      if (!p.held && await p.acquire()) holders.push(p)
    }
    if (holders.length !== LIMIT) return fail(`the semaphore did not fill: ${holders.length} of ${LIMIT}`)
    const slots = new Set(holders.map(p => p.slot))
    if (slots.size !== LIMIT) return fail(`two holders share a slot: ${[...slots]}`)

    const extra = client.semaphore(name, LIMIT, { ttl: '30s' })
    if (await extra.acquire()) return fail('a permit past the limit was granted')
    const who = await client.locks.get(name)
    if (who.holders.length !== LIMIT) return fail(`get shows ${who.holders.length} holders of ${LIMIT}`)

    // One leaves, the waiter gets exactly that slot.
    const freed = holders[0].slot
    const waiting = extra.acquire({ wait: '5s' })
    await sleep(200)
    await holders[0].release()
    if (!(await waiting)) return fail('a waiter did not get the freed permit')
    if (extra.slot !== freed) return fail(`the waiter got slot ${extra.slot}, not the freed ${freed}`)
    await extra.release()
    return { success: true, message: `a crowd of 12 on ${LIMIT} permits: never more than ${LIMIT}, a freed slot goes to the waiter` }
  } finally {
    await Promise.all(permits.map(p => p.release().catch(() => {})))
  }
}

export async function kvCheckLooksAndGatesWithoutWriting(client) {
  await checksAreServed(client)
  const ns = 'test-kv-check'
  const put = await client.kv.put(ns, 'state', { n: 1 }, { ttl: '10m' })
  const held = await client.kv.check(ns, 'state', { expect: put.version })
  if (held.applied !== true || held.version !== put.version || 'value' in held) {
    return fail(`a held check answers its version and no value: ${JSON.stringify(held)}`)
  }
  const stale = await client.kv.check(ns, 'state', { expect: put.version + 1000 })
  if (stale.applied !== false || stale.reason !== 'version' || stale.value.n !== 1) {
    return fail(`a lost check answers what a reader sees: ${JSON.stringify(stale)}`)
  }
  const absent = await client.kv.check(ns, 'nobody', { expect: 0 })
  if (absent.applied !== true) return fail(`expect: 0 on an absent key must hold: ${JSON.stringify(absent)}`)
  const row = await client.kv.get(ns, 'state')
  if (row.version !== put.version) return fail('a check moved the version it read')

  // The gate: a batch-wide precondition on a key the batch does not write.
  const gated = await client.kv.check(ns, 'state', { expect: put.version + 1000, required: true })
  if (gated.applied !== false || gated.precondition !== true) {
    return fail(`a required check that loses is a precondition verdict: ${JSON.stringify(gated)}`)
  }
  return { success: true, message: 'check holds, loses with the current row, takes expect:0, and never writes' }
}
