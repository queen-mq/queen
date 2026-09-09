import assert from 'node:assert/strict'
import { test } from 'node:test'

import { createRouteSupport, isMissingRoute } from '../src/composables/useRouteSupport.js'

const notFound = () => Object.assign(new Error('Not Found'), { status: 404 })
const boom = () => Object.assign(new Error('boom'), { status: 500 })

test('isMissingRoute: the three stable answers, nothing else', () => {
  assert.equal(isMissingRoute({ status: 404 }), true)
  assert.equal(isMissingRoute({ status: 404, code: 'route_blocked' }), true)
  assert.equal(isMissingRoute({ code: 'not_an_api_response' }), true)
  assert.equal(isMissingRoute({ status: 500 }), false)
  assert.equal(isMissingRoute({ status: 403, code: 'feature_gated' }), false)
  assert.equal(isMissingRoute(null), false)
})

test('guard: a 404 is remembered and the next ask rejects without a request', async () => {
  const rs = createRouteSupport(() => 0)
  const err = notFound()
  let calls = 0
  const call = rs.guard('dlq-signatures', async () => { calls++; throw err })
  await assert.rejects(call({ queue: 'a' }), err)
  await assert.rejects(call({ queue: 'b' }), err)
  await assert.rejects(call({ queue: 'c' }), err)
  assert.equal(calls, 1)
  assert.equal(rs.missing('dlq-signatures'), err)
  assert.equal(rs.missing('partition-liveness'), null)
})

test('guard: a transient failure is not a verdict, and a success passes through', async () => {
  const rs = createRouteSupport(() => 0)
  let calls = 0
  const call = rs.guard('workload', async (x) => {
    calls++
    if (calls === 1) throw boom()
    return { data: x }
  })
  await assert.rejects(call(1))
  assert.equal(rs.missing('workload'), null)
  assert.deepEqual(await call(2), { data: 2 })
  assert.equal(calls, 2)
})

test('guard: verdicts are per cluster epoch, so a switch re-asks', async () => {
  let epoch = 0
  const rs = createRouteSupport(() => epoch)
  let calls = 0
  const call = rs.guard('workload', async () => { calls++; throw notFound() })
  await assert.rejects(call())
  await assert.rejects(call())
  assert.equal(calls, 1)
  epoch = 1
  await assert.rejects(call())
  assert.equal(calls, 2)
  epoch = 0
  await assert.rejects(call())
  assert.equal(calls, 2, 'the old epoch keeps its verdict')
})

test('forget: an explicit re-probe asks again', async () => {
  const rs = createRouteSupport(() => 0)
  let calls = 0
  const call = rs.guard('workload', async () => { calls++; throw notFound() })
  await assert.rejects(call())
  rs.forget('workload')
  assert.equal(rs.missing('workload'), null)
  await assert.rejects(call())
  assert.equal(calls, 2)
})
