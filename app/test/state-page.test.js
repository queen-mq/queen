import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  VALUE_CAP, bytes, span, inOrAgo, fires, looksBinary, valueView, sizeFlag, keyRows, expiresText, flagLine,
  summaryLine, lockRows, lockTotals, leaseLeft, timerRows, parseJson, valueChanges, lifetimeOf, editPlan,
  putPlan, deletePlan, writeAnswer,
} from '../src/console/state/stateViews.js'
import { permitsOf } from '../src/composables/useLocks.js'

// The State page's pure half: its words, the large-value line, and the bodies
// and calls its dialogs show before anything runs.

const T = Date.parse('2026-10-31T09:00:00Z')
const iso = (ms) => new Date(ms).toISOString().replace('Z', '000Z') // the broker's six digits

test('span rounds once, so a unit never reads 60 or 24', () => {
  assert.equal(span(42), '42 s')
  assert.equal(span(89), '1 min')
  assert.equal(span(3599), '1 h')
  assert.equal(span(2 * 3600 - 20), '2 h')
  assert.equal(span(2 * 86400 - 30), '2 d')
  assert.equal(span(86400 + 5 * 3600), '1 d 5 h')
  assert.equal(span(3600 + 25 * 60), '1 h 25 min')
})

test('relative times say in, ago and late, and now near zero', () => {
  assert.equal(inOrAgo(T + 4 * 60_000, T), 'in 4 min')
  assert.equal(inOrAgo(T - 4 * 60_000, T), '4 min ago')
  assert.equal(inOrAgo(T + 400, T), 'now')
  assert.equal(fires(T - 90_000, T), '2 min late')
  assert.equal(fires(null, T), '')
})

test('bytes is the console rule, 1024-based', () => {
  assert.equal(bytes(164556), '161 KB')
  assert.equal(bytes(52770), '52 KB')
  assert.equal(bytes(2002), '2 KB')
  assert.equal(bytes(89), '89 B')
  assert.equal(bytes(null), '–')
})

test('looksBinary: base64 of bytes yes; ids, words and base64 of text no', () => {
  const bin = Buffer.from(Array.from({ length: 300 }, (_, i) => (i * 97 + 13) % 256)).toString('base64')
  assert.equal(looksBinary(bin), true)
  assert.equal(looksBinary('0199a000f1ee70008000000000000000000000000000000000000000000000000000'), false)
  assert.equal(looksBinary('traceflow keeps one document per trace here while it is open; the compactor'), false)
  assert.equal(looksBinary(Buffer.from('a plain sentence long enough to pass the length rule, and then some more').toString('base64')), false)
})

test('valueView tells json, text and binary apart', () => {
  assert.equal(valueView({ key: 'a', value: { plan: 'pro' } }).kind, 'json')
  assert.equal(valueView({ key: 'a', value: { plan: 'pro' } }).text, '{\n  "plan": "pro"\n}')
  assert.equal(valueView({ key: 'a', value: 'hello' }).kind, 'text')
  assert.equal(valueView({ key: 'a', value: 'hello' }).text, 'hello')
  assert.equal(valueView({ key: 'a', value: null }).kind, 'json')
  assert.equal(valueView({ key: 'a', value: null }).size, 4) // a stored null is four bytes
  assert.equal(valueView({ key: 'a' }).kind, 'none')
})

test('a value is flagged past three quarters of the limit; amber only when it never expires', () => {
  assert.equal(sizeFlag(VALUE_CAP * 0.74), null)
  assert.equal(sizeFlag(VALUE_CAP * 0.8).tone, 'amber')
  assert.equal(sizeFlag(VALUE_CAP * 0.8, { expires: true }).tone, '')
  const rows = keyRows({ rows: [
    { key: 'big', value: 'x'.repeat(52_000), version: 3, expiresAt: null, updatedAt: iso(T) },
    { key: 'big-ttl', value: 'y'.repeat(50_000), version: 4, expiresAt: iso(T + 3600_000), updatedAt: iso(T) },
    { key: 'small', value: 1, version: 5, expiresAt: null, updatedAt: iso(T) },
  ] })
  const line = flagLine(rows)
  assert.equal(line.tone, 'amber')
  assert.equal(line.badge, '2 values near 64 KB')
  assert.equal(flagLine(rows.slice(1)).tone, '')
  assert.match(flagLine(rows.slice(1)).text, /It expires, so it goes by itself/)
  assert.equal(flagLine(rows.slice(2)), null)
})

test('the Expires cell is the broker verdict first, then the time left', () => {
  const [a, b, c] = keyRows({ rows: [
    { key: 'a', value: 1, version: 1, expiresAt: iso(T - 1000), updatedAt: iso(T - 9000), expired: true },
    { key: 'b', value: 1, version: 1, expiresAt: iso(T + 7200_000), updatedAt: iso(T) },
    { key: 'c', value: 1, version: 1, expiresAt: null, updatedAt: iso(T) },
  ] })
  assert.equal(expiresText(a, T), 'expired')
  assert.equal(expiresText(b, T), 'in 2 h')
  assert.equal(expiresText(c, T), '')
})

test('the summary says each total once, in words, and the empty states', () => {
  const kv = { keys: 809, bytes: 161_809, namespaces: 13 }
  assert.equal(
    summaryLine({ kv, locks: { locks: 0, semaphores: 0 }, timers: { count: 1, queues: 1, more: false } }),
    '809 keys in 13 namespaces, 158 KB in all. No locks are held, and 1 timer waits in 1 queue.',
  )
  assert.equal(
    summaryLine({ kv, locks: { locks: 2, semaphores: 1 }, timers: { count: 0, queues: 0 } }),
    '809 keys in 13 namespaces, 158 KB in all. 2 locks and 1 semaphore are held, and no timers are waiting.',
  )
  assert.equal(
    summaryLine({ kv, locks: { locks: 0, semaphores: 0 }, timers: { reading: true, total: 123 } }),
    '809 keys in 13 namespaces, 158 KB in all. No locks are held. Reading the timers of 123 queues…',
  )
  assert.match(summaryLine({ kv: { keys: 0, bytes: 0, namespaces: 0 }, locks: null, timers: null }), /^Nothing is kept in KV yet\. Reading the locks and the timers…$/)
})

test('a family the broker does not serve leaves the summary instead of reading forever', () => {
  const kv = { keys: 9, bytes: 2048, namespaces: 2 }
  assert.equal(summaryLine({ kv, locks: { locks: 1, semaphores: 0 }, timers: { unavailable: true } }), '9 keys in 2 namespaces, 2 KB in all. 1 lock is held.')
  assert.equal(
    summaryLine({ kv: { unavailable: true }, locks: { unavailable: true }, timers: { count: 2, queues: 1 } }),
    'KV is not available on this broker. 2 timers wait in 1 queue.',
  )
  assert.equal(summaryLine({ kv: { unavailable: true }, locks: { unavailable: true }, timers: { unavailable: true } }), 'KV is not available on this broker.')
})

test('permits become one row per lock name; a semaphore has a holder per permit', () => {
  const at = (s) => iso(T + s * 1000)
  const permits = permitsOf([
    { key: 'daily-report#0', value: { owner: 'report-1' }, version: 10, expiresAt: at(840), updatedAt: at(-360) },
    { key: 'pms-api#0', value: { owner: 'pms-1' }, version: 11, expiresAt: at(240), updatedAt: at(-360) },
    { key: 'pms-api#2', value: { owner: 'pms-2' }, version: 12, expiresAt: at(360), updatedAt: at(-300) },
    { key: 'stray', value: 'not a permit', version: 13, expiresAt: null, updatedAt: at(-10) },
  ])
  const { rows, other } = lockRows(permits)
  assert.deepEqual(rows.map((r) => [r.name, r.kind, r.holders.length]), [['daily-report', 'lock', 1], ['pms-api', 'semaphore', 2]])
  assert.equal(other, 1)
  assert.deepEqual(lockTotals(rows), { locks: 1, semaphores: 1 })
  assert.equal(leaseLeft(rows[1].endsMs, T), '4 min') // the permit that ends first
  assert.equal(leaseLeft(T - 1, T), 'ending')
  assert.equal(rows[0].holders[0].lifetime, 1200)
})

test('timers of every queue merge into one list, the next to fire first', () => {
  const t = (key, s) => ({ timerKey: key, partition: 'p', deliverAt: iso(T + s * 1000), attempts: 0, claimed: false })
  const r = timerRows([
    { queue: 'emails.send', rows: [t('b', 600), t('a', 60)], truncated: true },
    { queue: 'payments.capture', rows: [t('c', 30)] },
    { queue: 'broken', error: 'Server error (HTTP 503)' },
    { queue: 'empty', rows: [] },
  ])
  assert.deepEqual(r.rows.map((x) => `${x.queue}/${x.key}`), ['payments.capture/c', 'emails.send/a', 'emails.send/b'])
  assert.equal(r.count, 3)
  assert.equal(r.queues, 2)
  assert.equal(r.more, true)
  assert.deepEqual(r.failed, [{ queue: 'broken', error: 'Server error (HTTP 503)' }])
  assert.equal(r.perQueue.get('emails.send'), 2)
})

test('parseJson answers in the parser words, without its prefix', () => {
  assert.equal(parseJson('').error, 'it is empty')
  assert.equal(parseJson('{"a":1}').compact, '{"a":1}')
  assert.equal(parseJson('{a:1}').ok, false)
})

test('an edit diff of two objects lists the fields that change', () => {
  assert.deepEqual(
    valueChanges({ plan: 'pro', seats: 12, owner: 'x' }, { plan: 'business', seats: 12, since: '2026' }),
    [
      { k: 'owner', from: '"x"', to: '–', field: true },
      { k: 'plan', from: '"pro"', to: '"business"', field: true },
      { k: 'since', from: '–', to: '"2026"', field: true },
    ],
  )
  assert.equal(valueChanges(1, 2)[0].k, 'Value')
})

test('an edit keeps the lifetime and sends the version it was read at', () => {
  const row = { key: 'acct-0093/plan', value: { plan: 'pro' }, version: 331, expiresAt: null, updatedAt: iso(T) }
  const p = editPlan({ ns: 'accounts', row, text: '{"plan":"business"}' })
  assert.equal(p.ok, true)
  assert.deepEqual(p.body, { value: { plan: 'business' }, forever: true, expect: 331 })
  assert.equal(p.call, 'PUT /api/v1/kv/accounts/acct-0093%2Fplan {"value":{"plan":"business"},"forever":true,"expect":331}')
  assert.equal(editPlan({ ns: 'accounts', row, text: '{"plan":"pro"}' }).same, true)
  assert.equal(editPlan({ ns: 'accounts', row, text: '{"plan":' }).ok, false)
  const ttl = { ...row, expiresAt: iso(T + 7200_000) }
  assert.deepEqual(editPlan({ ns: 'accounts', row: ttl, text: '1', ifUnchanged: false }).body, { value: 1, ttlSeconds: 7200 })
  assert.equal(lifetimeOf(ttl), 7200)
  const text = { ...row, value: 'hello' }
  assert.deepEqual(editPlan({ ns: 'n', row: text, text: 'hi there', asText: true }).body.value, 'hi there')
  assert.match(editPlan({ ns: 'n', row, text: JSON.stringify('x'.repeat(VALUE_CAP)) }).error, /over 64 KB/)
})

test('put: only if absent is expect 0; the namespace rule is the broker one', () => {
  const p = putPlan({ ns: 'orders', key: 'order/9f1', text: '{"status":"paid"}', expiry: '1d' })
  assert.equal(p.ok, true)
  assert.deepEqual(p.body, { value: { status: 'paid' }, ttlSeconds: 86400, expect: 0 })
  assert.equal(p.call, 'PUT /api/v1/kv/orders/order%2F9f1 {"value":{"status":"paid"},"ttlSeconds":86400,"expect":0}')
  assert.match(p.effect, /removes it 1 day after the write/)
  assert.deepEqual(putPlan({ ns: 'orders', key: 'k', text: '1', absent: false }).body, { value: 1, forever: true })
  assert.equal(putPlan({ ns: 'Orders', key: 'k', text: '1' }).ok, false)
  assert.equal(putPlan({ ns: 'orders', key: '', text: '1' }).problems.length, 0) // empty is not wrong yet
  assert.equal(putPlan({ ns: 'orders', key: 'k', text: '' }).ok, false)
})

test('delete carries the version it was read at, except for an expired row', () => {
  assert.deepEqual(deletePlan({ ns: 'accounts', row: { key: 'acct-0093/plan', version: 331 } }), {
    body: { expect: 331 },
    call: 'DELETE /api/v1/kv/accounts/acct-0093%2Fplan {"expect":331}',
  })
  assert.equal(deletePlan({ ns: 'a', row: { key: 'k', version: 2, expired: true } }).body, null)
})

test('the broker answer reads as what is stored now', () => {
  assert.equal(writeAnswer('put', { applied: true, version: 332 }, { key: 'k', ns: 'n' }).text, 'Saved k')
  assert.equal(writeAnswer('put', { applied: false, reason: 'exists', version: 9 }, { key: 'k' }).text, 'Not written: k exists')
  assert.equal(writeAnswer('delete', { applied: false, reason: 'version', version: 9 }, { key: 'k' }).text, 'Not deleted: k changed since you opened it')
  assert.equal(writeAnswer('delete', { applied: true }, { key: 'k', ns: 'n' }).sub, 'The broker answered 200: it is gone from n.')
  assert.equal(writeAnswer('put', {}, { key: 'k' }).tone, 'red')
})
