import assert from 'node:assert/strict'
import { test } from 'node:test'

import { MAX_CHARS, render, thin } from '../src/answer.mjs'
import { MASKED, clip, createGuard, redactText } from '../src/guard.mjs'

const row = () => ({
  transactionId: 't1',
  data: { orderId: 7, guest: { name: 'Ada', email: 'ada@example.test' }, rooms: [{ guest: 'x', pin: '1234' }, { guest: 'y', pin: '5678' }] },
  payload: { orderId: 7, guest: { name: 'Ada', email: 'ada@example.test' } },
  headers: { Authorization: 'Bearer abc', type: 'paid' },
  errorMessage: 'card declined for ada@example.test on order 91234',
})

test('bodies and headers are hidden by default', () => {
  const out = createGuard().message(row())
  assert.equal(out.data, undefined)
  assert.equal(out.payload, undefined)
  assert.equal(out.headers, undefined)
  assert.equal(out.transactionId, 't1')
  assert.ok(!JSON.stringify(out).includes('ada@example.test'))
  assert.equal(out.errorMessage, 'card declined for <email> on order <n>')
})

test('turned on, bodies show and listed fields stay hidden', () => {
  const g = createGuard({ readMessages: true, mask: ['data.guest.email', 'data.rooms.pin', 'headers.authorization'] })
  const out = g.message(row())
  assert.equal(out.data.guest.name, 'Ada')
  assert.equal(out.data.guest.email, MASKED)
  assert.deepEqual(out.data.rooms, [{ guest: 'x', pin: MASKED }, { guest: 'y', pin: MASKED }])
  assert.equal(out.headers.Authorization, MASKED)
  assert.equal(out.headers.type, 'paid')
  assert.equal(out.payload, undefined, 'the body is not sent twice, where the mask could miss one')
})

test('a mask with a star covers every key at that level', () => {
  const g = createGuard({ readMessages: true, mask: ['data.guest.*'] })
  assert.deepEqual(g.message(row()).data.guest, { name: MASKED, email: MASKED })
})

test('a KV value follows the same rule', () => {
  assert.equal(createGuard().value({ a: 1 }), undefined)
  const g = createGuard({ readMessages: true, mask: ['data.guest.email'] })
  assert.deepEqual(g.value({ guest: { email: 'x', name: 'y' } }), { guest: { email: MASKED, name: 'y' } })
})

test('a search cannot probe a masked field', () => {
  const g = createGuard({ readMessages: true, mask: ['data.guest.email'] })
  assert.equal(g.refusesField('data.guest.email'), true)
  assert.equal(g.refusesField('data.guest'), true, 'its parent holds it')
  assert.equal(g.refusesField('data'), true)
  assert.equal(g.refusesField('data.guest.email.domain'), true)
  assert.equal(g.refusesField('data.guest.name'), false)
  assert.equal(g.refusesField('data.country'), false)
})

test('error text loses what usually identifies someone', () => {
  assert.equal(redactText('token=abc123 failed'), 'token=<hidden> failed')
  assert.equal(redactText('Authorization: Bearer abc.def'), 'Authorization: Bearer <hidden>')
  assert.equal(redactText('password: "hunter2"'), 'password: "<hidden>"')
  assert.equal(redactText('id 3f2504e0-4f89-11d3-9a0c-0305e82c3301'), 'id <id>')
  assert.equal(redactText('invalid token format'), 'invalid token format', 'plain words stay')
  assert.equal(redactText('retry 3 of 5'), 'retry 3 of 5', 'small numbers stay')
})

test('a huge string is cut', () => {
  const out = clip({ a: 'x'.repeat(5000) })
  assert.ok(out.a.length < 2100)
  assert.ok(out.a.endsWith('more characters]'))
})

test('an answer fits, and says when it was cut', () => {
  const big = { rows: Array.from({ length: 5000 }, (_, i) => ({ i, text: 'x'.repeat(40) })), keep: 'yes' }
  const text = render(big)
  assert.ok(text.length <= MAX_CHARS + 400, `${text.length} chars`)
  const out = JSON.parse(text)
  assert.equal(out.keep, 'yes')
  assert.ok(out.rows.length < 5000)
  assert.equal(out.cut.lists[0].path, 'rows')
  assert.equal(out.cut.lists[0].had, 5000)
  assert.equal(big.rows.length, 5000, 'the input is not changed')
})

test('an answer that fits is left alone', () => {
  assert.deepEqual(JSON.parse(render({ a: [1, 2, 3] })), { a: [1, 2, 3] })
})

test('thin keeps the first and the last point', () => {
  const points = Array.from({ length: 100 }, (_, i) => i)
  const out = thin(points, 10)
  assert.equal(out.length, 10)
  assert.equal(out[0], 0)
  assert.equal(out.at(-1), 99)
  assert.deepEqual(thin([1, 2], 10), [1, 2])
})
