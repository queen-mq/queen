// Which engine the cell runs — the one verdict raft mode hangs on.
//
// The failed-/health cases go through the real HTTP client, as in
// gated-verdict.test.js: both engines answer 503 WITH a health body (raft
// while no leader is known, Postgres while its database is down), and the
// verdict has to survive the trip through ApiError.body. A hand-made
// `{body}` would pass while the app's own errors fell through to "unknown".

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { createApiClient } from '../src/api/httpClient.js'
import {
  ENGINE_POSTGRES,
  ENGINE_RAFT,
  createEngineStore,
  engineFromHealth,
  engineFromHealthError,
  storageSourceLabel,
} from '../src/composables/useEngine.js'

// What the two brokers send (server/src/handlers/raft.rs handle_health,
// server/src/handlers/status.rs handle_health).
const RAFT_HEALTHY = {
  status: 'healthy', engine: 'raft', version: '1.6.0',
  raft: { role: 'leader', leader: true, term: 2, applied: 58939, commit: 58939, lag: 0, storageReady: true },
}
const RAFT_SETTLING = {
  status: 'settling', engine: 'raft', version: '1.6.0',
  raft: { role: 'follower', leader: false, term: 3, applied: 58939, commit: 58990, lag: 2400, storageReady: true },
}
const PG_HEALTHY = { status: 'healthy', database: 'connected', engine: 'segments-rust', version: '1.5.1' }
const PG_DOWN = { status: 'unhealthy', database: 'disconnected', engine: 'segments-rust', version: '1.5.1' }

const json = (body, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })

/** GET /health through the real client; resolves the error it rejects with. */
const failedHealth = async (respond) => {
  const client = createApiClient({ apiBaseUrl: 'https://queen.test', fetch: async () => respond() })
  try {
    await client.get('/health')
  } catch (err) {
    return err
  }
  throw new Error('the call was expected to fail')
}

/** A /health the test settles by hand. */
const deferred = () => {
  let resolve
  let reject
  const promise = new Promise((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}

// ---------------------------------------------------------------------------
// The verdict a body carries
// ---------------------------------------------------------------------------

test('only a body that says raft is raft; every other health answer is Postgres', () => {
  assert.equal(engineFromHealth(RAFT_HEALTHY), ENGINE_RAFT)
  assert.equal(engineFromHealth(RAFT_SETTLING), ENGINE_RAFT)
  assert.equal(engineFromHealth(PG_HEALTHY), ENGINE_POSTGRES)
  assert.equal(engineFromHealth(PG_DOWN), ENGINE_POSTGRES)
  // A broker older than the engine field still answers a status.
  assert.equal(engineFromHealth({ status: 'ok' }), ENGINE_POSTGRES)
})

test('what is not a health answer decides nothing', () => {
  assert.equal(engineFromHealth(null), null)
  assert.equal(engineFromHealth(undefined), null)
  assert.equal(engineFromHealth('<!doctype html><title>Queen</title>'), null)
  assert.equal(engineFromHealth([]), null)
  assert.equal(engineFromHealth({}), null)
})

test('a failed /health still names its engine when it carries a body', async () => {
  // Raft while no leader is known: 503 settling.
  const settling = await failedHealth(() => json(RAFT_SETTLING, 503))
  assert.equal(settling.status, 503)
  assert.equal(engineFromHealthError(settling), ENGINE_RAFT)

  // Postgres with its database down: 503 unhealthy.
  const pgDown = await failedHealth(() => json(PG_DOWN, 503))
  assert.equal(engineFromHealthError(pgDown), ENGINE_POSTGRES)

  // Nothing answered, or the SPA fallback did: no verdict either way.
  const offline = await failedHealth(() => { throw new TypeError('Failed to fetch') })
  assert.equal(offline.status, 0)
  assert.equal(engineFromHealthError(offline), null)
  const fallback = await failedHealth(() =>
    new Response('<!doctype html>', { status: 200, headers: { 'content-type': 'text/html' } }))
  assert.equal(fallback.code, 'not_an_api_response')
  assert.equal(engineFromHealthError(fallback), null)
  assert.equal(engineFromHealthError(null), null)
})

test('the storage toggle keeps "Postgres stats" unless the engine is raft', () => {
  assert.equal(storageSourceLabel(ENGINE_RAFT), 'Raft')
  assert.equal(storageSourceLabel(ENGINE_POSTGRES), 'Postgres stats')
  // Not known yet (or /health unreachable): the page renders as it always has.
  assert.equal(storageSourceLabel(null), 'Postgres stats')
  assert.equal(storageSourceLabel(undefined), 'Postgres stats')
})

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

test('one /health answers every concurrent caller and records the verdict', async () => {
  let calls = 0
  const d = deferred()
  const store = createEngineStore({ fetchHealth: () => { calls++; return d.promise } })
  assert.equal(store.engine.value, null)
  assert.equal(store.isRaft.value, false)

  const a = store.loadHealth()
  const b = store.loadHealth()
  const c = store.ensure()
  d.resolve({ data: RAFT_HEALTHY })
  assert.deepEqual((await a).data, RAFT_HEALTHY)
  assert.deepEqual((await b).data, RAFT_HEALTHY)
  assert.equal(await c, ENGINE_RAFT)
  assert.equal(calls, 1)
  assert.equal(store.isRaft.value, true)
  assert.equal(store.settled.value, true)
})

test('once a cluster has answered, asking again costs nothing', async () => {
  let calls = 0
  const store = createEngineStore({ fetchHealth: async () => { calls++; return { data: PG_HEALTHY } } })
  assert.equal(await store.ensure(), ENGINE_POSTGRES)
  assert.equal(await store.ensure(), ENGINE_POSTGRES)
  assert.equal(calls, 1)
  // The shell's own poll still goes out: loadHealth is the call, not a cache.
  await store.loadHealth()
  assert.equal(calls, 2)
})

test('a 503 settling is still raft, and the shell still sees the failure', async () => {
  const err = await failedHealth(() => json(RAFT_SETTLING, 503))
  const store = createEngineStore({ fetchHealth: async () => { throw err } })
  await assert.rejects(store.loadHealth(), err)
  assert.equal(store.engine.value, ENGINE_RAFT)
})

test('an unreachable /health settles without inventing a verdict, and keeps the one it had', async () => {
  const offline = await failedHealth(() => { throw new TypeError('Failed to fetch') })

  const fresh = createEngineStore({ fetchHealth: async () => { throw offline } })
  assert.equal(await fresh.ensure(), null)
  assert.equal(fresh.settled.value, true)
  assert.equal(fresh.isRaft.value, false)

  // A raft cell does not become Postgres because one poll failed.
  let fail = false
  const store = createEngineStore({
    fetchHealth: async () => { if (fail) throw offline; return { data: RAFT_HEALTHY } },
  })
  await store.loadHealth()
  fail = true
  await assert.rejects(store.loadHealth())
  assert.equal(store.engine.value, ENGINE_RAFT)
})

test('an answer that lands after a cluster switch belongs to the old cell', async () => {
  let epoch = 0
  const d = deferred()
  const store = createEngineStore({ fetchHealth: () => d.promise, getEpoch: () => epoch })
  const pending = store.loadHealth()
  epoch = 1
  store.reset()
  d.resolve({ data: RAFT_HEALTHY })
  await pending
  assert.equal(store.engine.value, null)
  assert.equal(store.settled.value, false)
})

test('reset forgets the cell, and the next ask goes to the network', async () => {
  let calls = 0
  let body = RAFT_HEALTHY
  const store = createEngineStore({ fetchHealth: async () => { calls++; return { data: body } } })
  assert.equal(await store.ensure(), ENGINE_RAFT)
  store.reset()
  assert.equal(store.engine.value, null)
  assert.equal(store.isRaft.value, false)
  body = PG_HEALTHY
  assert.equal(await store.ensure(), ENGINE_POSTGRES)
  assert.equal(calls, 2)
})

test('a remembered verdict paints first, the live answer wins and is remembered', async () => {
  const kept = []
  const d = deferred()
  const store = createEngineStore({
    fetchHealth: () => d.promise,
    recall: () => ENGINE_RAFT,
    remember: (v) => kept.push(v),
  })
  const pending = store.ensure()
  // Before /health answers, the reload already renders raft.
  assert.equal(store.isRaft.value, true)
  assert.equal(store.settled.value, false)
  // The cell was migrated back: the live answer overrules the memory.
  d.resolve({ data: PG_HEALTHY })
  assert.equal(await pending, ENGINE_POSTGRES)
  assert.equal(store.isRaft.value, false)
  assert.deepEqual(kept, [ENGINE_POSTGRES])
})

test('junk in storage, or storage that throws, is no memory at all', async () => {
  const junk = createEngineStore({ fetchHealth: () => new Promise(() => {}), recall: () => 'mysql' })
  junk.ensure()
  assert.equal(junk.engine.value, null)

  const broken = createEngineStore({
    fetchHealth: async () => ({ data: RAFT_HEALTHY }),
    recall: () => { throw new Error('SecurityError') },
    remember: () => { throw new Error('QuotaExceededError') },
  })
  assert.equal(await broken.ensure(), ENGINE_RAFT)
})
