/**
 * KV integration suite (PLAN_KV_TIMERS.md §5, §8.3).
 *
 * Every test here is repeatable ONLY because run.js purges `queen.kv` for the
 * `test-%` namespaces before the run (§10.4): without that purge a
 * `putIfAbsent` test is green on its first execution and red forever after,
 * and an `incr` test accumulates between runs. If you add a test here, its
 * namespace must start with `test-` or it will be the one that rots.
 *
 * And `forever` is BANNED in this file. A test that goes wrong with a TTL
 * leaves state that expires by itself; a test that goes wrong with `forever`
 * leaves immortal state in a shared database.
 */

import { KV_NS, sleep } from './_kvtimers.js'

export async function kvPutGetDelete(client) {
  const key = 'basic/put-get-delete'
  const written = await client.kv.put(KV_NS, key, { hello: 'world', n: 1 }, { ttl: '10m' })
  if (written.applied !== true) {
    return { success: false, message: `put did not apply: ${JSON.stringify(written)}` }
  }

  const row = await client.kv.get(KV_NS, key)
  if (row.found !== true || row.value.hello !== 'world' || row.version !== written.version) {
    return { success: false, message: `get returned ${JSON.stringify(row)}` }
  }
  if (!row.expiresAt) {
    return { success: false, message: 'a key written with a TTL must report expiresAt' }
  }

  const removed = await client.kv.delete(KV_NS, key)
  const after = await client.kv.get(KV_NS, key)

  return {
    success: removed.applied === true && after.found === false,
    message: 'put, get (row with version and expiresAt), delete, get-miss'
  }
}

export async function kvNullIsAValueNotAnAbsence(client) {
  const key = 'basic/null-value'
  await client.kv.put(KV_NS, key, null, { ttl: '10m' })
  const row = await client.kv.get(KV_NS, key)

  return {
    success: row.found === true && row.value === null,
    message: row.found === true && row.value === null
      ? 'a JSON null round-trips as {found:true, value:null}'
      : `found and value were collapsed: ${JSON.stringify(row)}`
  }
}

export async function kvPutIfAbsentIsExactlyOneWinner(client) {
  const key = 'marker/exactly-once'
  const first = await client.kv.putIfAbsent(KV_NS, key, { owner: 'first' }, { ttl: '10m' })
  const second = await client.kv.putIfAbsent(KV_NS, key, { owner: 'second' }, { ttl: '10m' })

  const ok = first.applied === true
    && second.applied === false
    && second.reason === 'exists'
    && second.value && second.value.owner === 'first'
    && second.version === first.version

  return {
    success: ok,
    message: ok
      ? 'exactly one winner, and the loser is handed the winner value without a second round trip'
      : `first=${JSON.stringify(first)} second=${JSON.stringify(second)}`
  }
}

/**
 * §5.3's headline repair: `expect: N > 0` on an absent key must be a PURE
 * UPDATE and create NOTHING. In the naive shape it falls into the INSERT
 * branch and creates the row -- which in a saga fires the compensating command
 * that `expect` existed to prevent.
 */
export async function kvExpectOnAbsentKeyCreatesNothing(client) {
  const key = 'fence/absent-key'
  const res = await client.kv.put(KV_NS, key, { compensate: true }, { ttl: '10m', expect: 90101 })
  const row = await client.kv.get(KV_NS, key)

  const ok = res.applied === false && res.reason === 'absent' && row.found === false
  return {
    success: ok,
    message: ok
      ? 'expect:N>0 on an absent key applied nothing and created nothing'
      : `res=${JSON.stringify(res)} row=${JSON.stringify(row)}`
  }
}

export async function kvExpectFencesAStaleWriter(client) {
  const key = 'fence/stale-writer'
  const v1 = await client.kv.put(KV_NS, key, { step: 1 }, { ttl: '10m' })
  const v2 = await client.kv.put(KV_NS, key, { step: 2 }, { ttl: '10m', expect: v1.version })
  // The stale holder still carries v1's version: its write must lose.
  const stale = await client.kv.put(KV_NS, key, { step: 'stale' }, { ttl: '10m', expect: v1.version })
  const row = await client.kv.get(KV_NS, key)

  const ok = v2.applied === true && stale.applied === false && stale.reason === 'version' && row.value.step === 2
  return {
    success: ok,
    message: ok
      ? 'a fenced write from a stale holder loses with reason "version"'
      : `v2=${JSON.stringify(v2)} stale=${JSON.stringify(stale)} row=${JSON.stringify(row)}`
  }
}

/**
 * §5.4: with `max`, `applied` IS the admission decision -- and the first call
 * of a window is guarded too, which is repair 2 (the naive INSERT branch has
 * no WHERE, so `max:5, delta:10` would admit 10 on the first call and blow the
 * quota exactly when a limiter is being attacked).
 */
export async function kvIncrIsTheAdmissionDecision(client) {
  const key = 'quota/window'
  const a = await client.kv.incr(KV_NS, key, 1, { ttl: '10m', max: 2 })
  const b = await client.kv.incr(KV_NS, key, 1, { ttl: '10m', max: 2 })
  const c = await client.kv.incr(KV_NS, key, 1, { ttl: '10m', max: 2 })

  const firstCallGuarded = await client.kv.incr(KV_NS, 'quota/first-call', 10, { ttl: '10m', max: 5 })

  const ok = a.applied === true && a.value === 1
    && b.applied === true && b.value === 2
    && c.applied === false && c.reason === 'limit' && c.value === 2
    && firstCallGuarded.applied === false && firstCallGuarded.reason === 'limit'

  return {
    success: ok,
    message: ok
      ? 'incr admits up to max, refuses beyond it without saturating, and guards the first call of the window'
      : `a=${JSON.stringify(a)} b=${JSON.stringify(b)} c=${JSON.stringify(c)} first=${JSON.stringify(firstCallGuarded)}`
  }
}

/**
 * §5.4, repair 3: an expired non-numeric row must not poison the counter. Left
 * unrepaired, every request of that customer is refused with `reason:'type'`
 * until the sweeper prunes -- a reason no client handles as "retry".
 */
export async function kvIncrTreatsAnExpiredRowAsZero(client) {
  const key = 'quota/expired-initializer'
  await client.kv.put(KV_NS, key, { count: 0 }, { ttlSeconds: 1 })
  await sleep(1500)
  const res = await client.kv.incr(KV_NS, key, 1, { ttl: '10m', max: 5 })

  return {
    success: res.applied === true && res.value === 1,
    message: res.applied === true
      ? 'an expired non-numeric row counts as zero and starts a new window'
      : `incr over an expired row answered ${JSON.stringify(res)}`
  }
}

export async function kvGetManyReportsMissingExplicitly(client) {
  await client.kv.put(KV_NS, 'many/a', 1, { ttl: '10m' })
  await client.kv.put(KV_NS, 'many/b', 2, { ttl: '10m' })
  const res = await client.kv.getMany(KV_NS, ['many/a', 'many/b', 'many/nope'])

  const keys = res.rows.map(r => r.key).sort()
  const ok = keys.length === 2 && keys[0] === 'many/a' && keys[1] === 'many/b'
    && res.missing.length === 1 && res.missing[0] === 'many/nope'

  return {
    success: ok,
    message: ok ? 'getMany returns rows plus an explicit missing list' : JSON.stringify(res)
  }
}

export async function kvGetPrefixPagesAndListAllWalksThem(client) {
  for (const i of [1, 2, 3, 4, 5]) {
    await client.kv.put(KV_NS, `scan/item-${i}`, { i }, { ttl: '10m' })
  }
  // A neighbour that must NOT be caught by the prefix.
  await client.kv.put(KV_NS, 'scanX/other', { i: 0 }, { ttl: '10m' })

  const page = await client.kv.getPrefix(KV_NS, 'scan/', { limit: 2 })
  if (page.rows.length !== 2 || page.truncated !== true || !page.nextAfter) {
    return { success: false, message: `first page: ${JSON.stringify(page)}` }
  }

  const seen = []
  for await (const row of client.kv.listAll(KV_NS, 'scan/')) seen.push(row.key)

  const ok = seen.length === 5 && seen.every(k => k.startsWith('scan/'))
  return {
    success: ok,
    message: ok
      ? 'getPrefix pages on a keyset cursor and listAll walks every page'
      : `listAll saw ${JSON.stringify(seen)}`
  }
}

export async function kvExpiredKeyIsNeverReturned(client) {
  const key = 'ttl/short'
  await client.kv.put(KV_NS, key, { v: 1 }, { ttlSeconds: 1 })
  const alive = await client.kv.get(KV_NS, key)
  await sleep(1500)
  const dead = await client.kv.get(KV_NS, key)
  // And it must not count as existing either, well before the sweeper prunes.
  const resurrect = await client.kv.putIfAbsent(KV_NS, key, { v: 2 }, { ttl: '10m' })

  const ok = alive.found === true && dead.found === false && resurrect.applied === true
  return {
    success: ok,
    message: ok
      ? 'an expired key is never returned and never counts as existing, sweeper or not'
      : `alive=${JSON.stringify(alive)} dead=${JSON.stringify(dead)} resurrect=${JSON.stringify(resurrect)}`
  }
}

export async function kvOnceReportsTheWinner(client) {
  const key = 'once/order-9f1'
  const first = await client.kv.once(KV_NS, key, { ttl: '10m' })
  const second = await client.kv.once(KV_NS, key, { ttl: '10m' })

  return {
    success: first.won === true && second.won === false,
    message: first.won === true && second.won === false
      ? 'once() wins once and loses forever after, inside the TTL'
      : `first=${JSON.stringify(first)} second=${JSON.stringify(second)}`
  }
}

/**
 * THE IDIOM THE WHOLE FEATURE EXISTS FOR (§8.3, §10.2).
 *
 * A bundle that pushes and marks itself done in one transaction. On a
 * redelivery the marker already exists, the transaction ABORTS, and `commit()`
 * RETURNS `{success:false, reason:'kv_precondition'}` instead of throwing --
 * because a lost precondition is the expected outcome of every legitimate
 * redelivery and must not enter a retry policy. The push must not have landed.
 */
export async function kvTransactionGateBlocksTheSecondDelivery(client) {
  const queue = await client.queue('test-kv-gate-out').create()
  if (!queue.configured) return { success: false, message: 'Queue not created' }

  const orderId = 'order-gate-1'
  const first = await client.transaction()
    .queue('test-kv-gate-out')
    .push([{ data: { orderId, attempt: 1 } }])
    .once(KV_NS, `gate/${orderId}`, { ttl: '10m' })
    .commit()

  if (first.success !== true) {
    return { success: false, message: `the first delivery should commit: ${JSON.stringify(first)}` }
  }

  const second = await client.transaction()
    .queue('test-kv-gate-out')
    .push([{ data: { orderId, attempt: 2 } }])
    .once(KV_NS, `gate/${orderId}`, { ttl: '10m' })
    .commit()

  if (second.success !== false || second.reason !== 'kv_precondition') {
    return { success: false, message: `the redelivery should be gated, got ${JSON.stringify(second)}` }
  }
  if (second.kvReason !== 'exists' || typeof second.failedIndex !== 'number') {
    return { success: false, message: `the verdict must carry kvReason and failedIndex: ${JSON.stringify(second)}` }
  }

  const delivered = await client.queue('test-kv-gate-out').batch(10).wait(false).pop()
  const attempts = delivered.map(m => m.data.attempt)

  const ok = attempts.length === 1 && attempts[0] === 1
  return {
    success: ok,
    message: ok
      ? 'the gate rolled back the second bundle whole: one message, not two, and commit() returned the verdict'
      : `queue received attempts ${JSON.stringify(attempts)} (expected exactly [1])`
  }
}
