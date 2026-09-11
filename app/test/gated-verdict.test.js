import { test } from 'node:test'
import assert from 'node:assert/strict'

import { createApiClient } from '../src/api/httpClient.js'
import { ApiError, isGatedVerdictError } from '../src/api/errors.js'
import { describeVerdict, gatedVerdict, refusalCode } from '../src/composables/useGatedVerdict.js'

// The errors are built the way the app builds them — through the real HTTP
// client, from the real envelopes — because the whole point of this helper is
// that the two components do NOT answer in the same shape:
//
//   proxy/src/errors.rs json_error   {"error": <message>, "code": <code>}
//   server handlers/timers.rs err()  {"error": <code>, "reason": …}
//
// A verdict asserted against a hand-made `{code}` object would pass while the
// broker's own refusals fell through to 'transient' in production.
const failWith = async (body, { status, contentType = 'application/json', path = '/api/v1/timers/orders' } = {}) => {
  const client = createApiClient({
    apiBaseUrl: 'https://queen.test',
    fetch: async () =>
      new Response(typeof body === 'string' ? body : JSON.stringify(body), {
        status,
        headers: { 'content-type': contentType },
      }),
  })
  try {
    await client.get(path)
  } catch (err) {
    return err
  }
  throw new Error('the call was expected to fail')
}

test('the route is not here: 404, route_blocked, and the SPA fallback', async () => {
  const plain404 = await failWith({ error: 'not found' }, { status: 404 })
  assert.ok(plain404 instanceof ApiError)
  assert.equal(gatedVerdict(plain404), 'absent')

  const blocked = await failWith({ error: 'route not available', code: 'route_blocked' }, { status: 404 })
  assert.equal(blocked.code, 'route_blocked')
  assert.equal(gatedVerdict(blocked), 'absent')

  // Broker-direct against a build older than the route: the SPA fallback
  // answers 200 text/html, which the client turns into not_an_api_response.
  const fallback = await failWith('<!doctype html><title>Queen</title>', { status: 200, contentType: 'text/html' })
  assert.equal(fallback.code, 'not_an_api_response')
  assert.equal(gatedVerdict(fallback), 'absent')
})

test('403 feature_gated is the plan, from either envelope', async () => {
  const fromProxy = await failWith({ error: 'not in your plan', code: 'feature_gated' }, { status: 403 })
  assert.equal(fromProxy.code, 'feature_gated')
  assert.equal(gatedVerdict(fromProxy), 'gated')

  // The broker's own gate (switches.rs → handlers/timers.rs err()) names the
  // code in `error` and carries no `code` field at all.
  const fromBroker = await failWith({ error: 'feature_gated', reason: 'feature_gated' }, { status: 403 })
  assert.equal(fromBroker.code, null)
  assert.equal(refusalCode(fromBroker), 'feature_gated')
  assert.equal(gatedVerdict(fromBroker), 'gated')
})

test('a bare 403 is a permission answer, not a feature gate', async () => {
  const forbidden = await failWith({ error: 'forbidden', code: 'forbidden' }, { status: 403 })
  assert.equal(gatedVerdict(forbidden), 'transient')
})

test('503 with a switch code is the cell, not the caller', async () => {
  for (const code of [
    'kv_disabled', 'timers_disabled', 'ephemeral_disabled',
    'kv_unavailable', 'timers_unavailable', 'ephemeral_unavailable',
  ]) {
    const paused = await failWith({ error: code, reason: 'switch' }, { status: 503 })
    assert.equal(gatedVerdict(paused), 'paused', code)
  }
  // THE ONE THE TIMERS PAGE ACTUALLY MEETS. handlers/timers.rs unavailable()
  // mints `timers_unavailable` — not the ladder's family code — for a pool
  // exhaustion, a statement timeout or a dead connection, and it is the ONLY
  // 503 the four routes this dashboard calls can produce: switches.rs pins
  // rung 1 to true for TimerRead/TimerCancel and quota.rs allows both, so
  // `timers_disabled` is reachable only on POST /api/v1/timers.
  const exhausted = await failWith({ error: 'timers_unavailable', reason: 'timers_pool_exhausted' }, { status: 503 })
  assert.equal(gatedVerdict(exhausted), 'paused')
  assert.equal(refusalCode(exhausted), 'timers_unavailable')

  // handlers/kv.rs answers the same conditions with its own code.
  const kvExhausted = await failWith({ error: 'kv_unavailable', reason: 'kv_pool_exhausted' }, { status: 503 })
  assert.equal(gatedVerdict(kvExhausted), 'paused')
})

test('everything else is transient and keeps the last-good rows', async () => {
  const rateLimited = await failWith({ error: 'slow down', code: 'rate_limited' }, { status: 429 })
  assert.equal(gatedVerdict(rateLimited), 'transient')

  const serverFault = await failWith({ error: 'boom' }, { status: 500 })
  assert.equal(gatedVerdict(serverFault), 'transient')

  const badRequest = await failWith({ error: 'timers_bad_request', reason: 'timers_count_prefix_required' }, { status: 400 })
  assert.equal(gatedVerdict(badRequest), 'transient')

  assert.equal(gatedVerdict(null), 'transient')
  assert.equal(gatedVerdict(new Error('offline')), 'transient')
})

test('the copy names the family and says whose fault it is', () => {
  assert.equal(describeVerdict('absent', 'timers').title, 'Timers are not available on this broker')
  assert.equal(describeVerdict('gated', 'kv').title, 'The KV browser is not enabled for this cluster')
  assert.match(describeVerdict('paused', 'timers').title, /not being served right now/)
  assert.match(describeVerdict('paused', 'timers').detail, /503/)
  assert.match(describeVerdict('transient', 'timers').title, /Cannot load timers/)
  // An unregistered family still reads as English rather than as "undefined".
  assert.equal(describeVerdict('absent', 'dlq signatures').title, 'dlq signatures is not available on this broker')
})

// ---------------------------------------------------------------------------
// The same two states, as the transport sees them
// ---------------------------------------------------------------------------

test('the verdicts a page renders itself are exactly what the client keeps quiet', async () => {
  // api/httpClient.js cannot import this module — it is the transport, and the
  // verdict names and the copy are about the screen — so the PREDICATE lives in
  // api/errors.js and both sides read it. This test is the seam: whatever
  // `gatedVerdict` calls 'gated' or 'paused', `isGatedVerdictError` must agree
  // with, or a page renders a quiet card and the toast fires anyway.
  const cases = [
    [{ error: 'not in your plan', code: 'feature_gated' }, 403],
    [{ error: 'feature_gated', reason: 'feature_gated' }, 403],
    [{ error: 'kv_disabled', reason: 'switch' }, 503],
    [{ error: 'timers_unavailable', reason: 'timers_pool_exhausted' }, 503],
    [{ error: 'ephemeral_unavailable', reason: 'switch' }, 503],
  ]
  for (const [body, status] of cases) {
    const err = await failWith(body, { status })
    assert.ok(['gated', 'paused'].includes(gatedVerdict(err)), JSON.stringify(body))
    assert.equal(isGatedVerdictError(err), true, JSON.stringify(body))
  }

  // 'absent' is NOT one of them: isMissingRouteError already covers it, and the
  // route-support store remembers it for the whole cluster epoch.
  assert.equal(isGatedVerdictError(await failWith({ error: 'not found' }, { status: 404 })), false)

  // A transient failure is a failure: it must reach the global surface.
  assert.equal(isGatedVerdictError(await failWith({ error: 'boom' }, { status: 500 })), false)
  assert.equal(isGatedVerdictError(await failWith({ error: 'slow down', code: 'rate_limited' }, { status: 429 })), false)
  assert.equal(isGatedVerdictError(await failWith({ error: 'forbidden', code: 'forbidden' }, { status: 403 })), false)
  assert.equal(isGatedVerdictError(null), false)
})
