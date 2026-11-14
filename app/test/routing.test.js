// Routing in the console (PLAN_ROUTING.md §10): the `where` language as the
// broker reads it (console/queues/whereText.js), a queue's route in words, a
// filtered group's where, the configure call of a reading queue, its preview
// on a source's last messages, the 409 a source's readers answer, and a
// message's headers (console/queues/routeView.js, messagesView.js).
//
// The wire this file pins (the routing worktree, 2026-11-12):
//   server/src/rsm/route/cond.rs                  the where parser and evaluator
//   server/src/rsm/facade/real/phase2.rs          route_json, apply_route_options,
//                                                 READER_KEEPS, api_delete_queue_routed
//   server/src/rsm/route/mod.rs                   HIDDEN_SEP, partition_by_valid

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { holds, parseWhere, readField, whereHolds, whereOf } from '../src/console/queues/whereText.js'
import {
  HIDDEN_SEP, PREVIEW_LIMIT, READER_KEEPS, alsoIn, behindText, deleteQueueCall, filterWords, filteredGroupsOf, fromLabel,
  groupFilterOf, hasReadersOf, holdLines, keepWindow, parseSources, partitionFieldValid, previewOf, previewParams, readByLine,
  readersParts, readingOptions, readsLine, routeOf, shownGroups, shownQueueName, splitHidden,
} from '../src/console/queues/routeView.js'
import { newQueueDraft, queueRows } from '../src/console/queues/queuesView.js'
import { behindRows, groupRows, sendCall, whereCall, whereOutcome } from '../src/console/queues/groupsView.js'
import { headerRows } from '../src/console/queues/messagesView.js'
import { queueAttention } from '../src/composables/useAttention.js'

const text = (parts) => (parts || []).map((p) => p.t).join('')
const must = (s) => {
  const r = parseWhere(s)
  assert.equal(r.error, null, `${s}: ${r.error}`)
  return r.conds
}
const msg = (data, extra = {}) => ({ queue: 'orders', partition: 'o-1', transactionId: 't1', data, payload: data, headers: null, ...extra })

// ---- the where language -------------------------------------------------------

test('where: the string form reads every op, and writes back as the broker writes it', () => {
  const s = "data.kind in ('order.paid', 'order.refunded') and data.total > 100 and headers.type prefix 'order.'"
  const conds = must(s)
  assert.deepEqual(conds.map((c) => [c.field, c.op, c.value]), [
    ['data.kind', 'in', ['order.paid', 'order.refunded']],
    ['data.total', '>', 100],
    ['headers.type', 'prefix', 'order.'],
  ])
  // The list form, objects or triples, says the same thing in the same words.
  assert.equal(whereOf([
    { field: 'data.kind', op: 'in', value: ['order.paid', 'order.refunded'] },
    { field: 'data.total', op: '>', value: 100 },
    ['headers.type', 'prefix', 'order.'],
  ]), s)
  assert.equal(whereOf(`  ${s} `), s)
  assert.equal(whereOf(null), '')
  for (const [w, op] of [
    ['data.x = 1', '='], ['data.x == 1', '='], ['data.x != 1', '!='], ['data.x <> 1', '!='],
    ['data.x < 1', '<'], ['data.x <= 1', '<='], ['data.x > 1', '>'], ['data.x >= 1', '>='],
    ['data.x exists', 'exists'], ["data.x prefix 'a'", 'prefix'], ["data.x !prefix 'a'", '!prefix'],
    ['data.x IN (1, 2)', 'in'], ['data.x in [1, 2]', 'in'],
  ]) assert.equal(must(w)[0].op, op, w)
  assert.deepEqual(must(''), [], 'no where: every message')
  assert.deepEqual(must("data.a = 'it''s' and data.b = true AND data.c = null and data.d = -1.5e2").map((c) => c.value), ["it's", true, null, -150])
  assert.deepEqual(must('data.x in ()')[0].value, [])
})

test('where: what the broker refuses, refused in its words', () => {
  const err = (s) => parseWhere(s).error
  assert.match(err("data.kind = 'a' or data.kind = 'b'"), /only AND/)
  assert.match(err("dat.kind = 'a'"), /unknown field "dat.kind"/)
  assert.match(err('offset > 3'), /known only once a message is stored/)
  assert.match(err("headers.Type = 'a'"), /bad header name "Type"/)
  assert.match(err('data..x = 1'), /bad field path/)
  assert.match(err(`data.${Array.from({ length: 17 }, () => 'a').join('.')} = 1`), /bad field path/)
  assert.match(err("data.x = 'open"), /not closed/)
  assert.match(err('data.x in 1'), /needs \(value, value, \.\.\.\)/)
  assert.match(err('data.x in (1,)'), /a value, at token/)
  assert.match(err('data.x prefix 1'), /needs a string/)
  assert.match(err('data.x > true'), /needs a number or a string/)
  assert.match(err('data.x = 01'), /not a number/)
  assert.match(err('data.x ~ 1'), /unexpected "~"/)
  assert.match(err('data.x !like 1'), /unexpected '!'/)
  assert.match(err('data.x'), /needs an operator/)
  assert.match(err('data.x ='), /needs a value/)
  assert.match(err('data.x = 1 data.y = 2'), /expected 'and'/)
  assert.match(err('= 1'), /starts with a field/)
  assert.equal(parseWhere('data.x = 1').conds.length, 1)
  assert.equal(parseWhere("data.x = 'a'").error, null)
})

test('where: a field the message lacks matches no op, != included', () => {
  const m = msg({ kind: 'order.paid', total: 120, tags: ['vip', 'it'], customer: { country: 'IT' }, empty: null },
    { headers: { type: 'order.paid' } })
  const yes = (s) => assert.equal(whereHolds(must(s), m), true, s)
  const no = (s) => assert.equal(whereHolds(must(s), m), false, s)
  yes("data.kind = 'order.paid'")
  yes("data.kind in ('order.paid', 'x')")
  yes('data.total > 100 and data.total <= 120')
  yes('data.total = 120.0')
  yes("data.customer.country = 'IT'")
  yes("data.tags.0 = 'vip'")
  yes('data.empty exists')
  yes("headers.type prefix 'order.'")
  yes("data.kind !prefix 'refund'")
  yes("queue = 'orders' and partition = 'o-1' and transactionId = 't1'")
  yes("data.kind > 'order.a'")
  no("data.kind != 'order.paid'")
  no("data.missing != 'x'")
  no('data.missing exists')
  no("headers.missing = 'x'")
  no("headers.missing !prefix 'x'")
  no("data.total = '120'")
  no("data.total prefix '1'")
  no("data.total > 'a'")
  no('data.tags.x exists')
  // No condition: every message.
  assert.equal(whereHolds([], m), true)
  assert.equal(holds(must('data.kind exists')[0], null), false)
  // A payload that is not an object reads `data` itself.
  assert.equal(whereHolds(must("data = 'hello'"), msg('hello')), true)
  assert.equal(whereHolds(must("data.x = 'hello'"), msg('hello')), false)
})

// ---- the route object -----------------------------------------------------------

const ROUTE_IT = {
  from: [{ queue: 'orders', where: "data.country = 'IT'", conditions: [{ field: 'data.country', op: '=', value: 'IT' }] }],
  partitionBy: 'same', group: null, catchingUp: false, readers: [], holders: [],
}
const ROUTE_ORDERS = {
  from: [], partitionBy: 'same', group: null, catchingUp: false,
  readers: [{ queue: 'orders-it' }, { queue: 'orders', group: 'billing' }],
  holders: [{ queue: 'orders-it' }, { queue: 'orders', group: 'billing' }, { queue: 'manual-review' }],
}

test('routeOf: a reader, a source, and a queue with no route', () => {
  assert.equal(routeOf({ name: 'plain' }), null)
  assert.equal(routeOf(null), null)
  const r = routeOf({ name: 'orders-it', route: ROUTE_IT })
  assert.equal(r.reading, true)
  assert.deepEqual(r.from, [{ queue: 'orders', where: "data.country = 'IT'" }])
  assert.equal(r.partitionBy, 'same')
  const s = routeOf({ name: 'orders', route: ROUTE_ORDERS })
  assert.equal(s.reading, false)
  const none = { where: '', oldestAt: null, holdsRetention: false }
  assert.deepEqual(s.readers, [
    { queue: 'orders-it', group: null, ...none },
    { queue: 'orders', group: 'billing', ...none },
  ])
  // A hidden queue's name, raw, is its queue and its group; a repeat is one.
  const h = routeOf({ route: { readers: [`orders${HIDDEN_SEP}billing`, { queue: `orders${HIDDEN_SEP}billing` }] } })
  assert.deepEqual(h.readers, [{ queue: 'orders', group: 'billing', ...none }])
  // A where only in its list form still reads as text.
  assert.equal(routeOf({ route: { from: [{ queue: 'a', conditions: [['data.x', '=', 1]] }] } }).from[0].where, 'data.x = 1')
  assert.equal(routeOf({ route: { from: ['a'], partitionBy: 'data.customerId' } }).partitionBy, 'data.customerId')
})

test('the Queues list says "from" on a reading queue, and nothing on the others', () => {
  assert.equal(fromLabel(routeOf({ route: ROUTE_IT })), 'from orders')
  assert.equal(fromLabel(routeOf({ route: { from: ['orders', 'refunds'] } })), 'from orders and refunds')
  assert.equal(fromLabel(routeOf({ route: { from: ['a', 'b', 'c'] } })), 'from a and 2 more')
  assert.equal(fromLabel(routeOf({ route: ROUTE_ORDERS })), '')
  assert.equal(fromLabel(null), '')
  const rows = queueRows({ queues: [{ name: 'orders-it', route: ROUTE_IT }, { name: 'orders', route: ROUTE_ORDERS }, { name: 'plain' }], groups: [] })
  assert.deepEqual(rows.map((r) => r.from), ['from orders', '', ''])
})

test('a reading queue\'s line: what it reads, what it keeps, how it is keyed, whether it catches up', () => {
  const route = routeOf({ route: ROUTE_IT })
  assert.equal(text(readsLine(route, { options: { retentionEnabled: true, retentionSeconds: 86400, maxWaitTimeSeconds: 0 } })),
    "Reads orders where data.country = 'IT' · keeps what orders keeps (1 d)")
  const parts = readsLine(route)
  assert.deepEqual(parts[1], { t: 'orders', mono: true, to: '/queues/orders' })
  assert.deepEqual(parts[3], { t: "data.country = 'IT'", mono: true })
  assert.equal(text(parts), "Reads orders where data.country = 'IT' · keeps what orders keeps")
  // Several sources with one where say it once; with their own, each says its own.
  assert.equal(text(readsLine(routeOf({ route: { from: [{ queue: 'a', where: 'data.x = 1' }, { queue: 'b', where: 'data.x = 1' }] } }))),
    'Reads a and b where data.x = 1 · keeps what its sources keep')
  assert.equal(text(readsLine(routeOf({ route: { from: [{ queue: 'a', where: 'data.x = 1' }, { queue: 'b', where: '' }, { queue: 'c', where: 'data.y = 2' }], partitionBy: 'data.customerId', catchingUp: true } }))),
    'Reads a where data.x = 1, b and c where data.y = 2 · keeps what its sources keep · partitioned by data.customerId · catching up')
  assert.equal(text(readsLine(routeOf({ route: { from: ['orders'] } }))), 'Reads orders · keeps what orders keeps')
  assert.equal(readsLine(routeOf({ route: ROUTE_ORDERS })), null)
  assert.equal(readsLine(null), null)
})

test('keepWindow: the shorter of the retention window and the TTL, whatever is read', () => {
  assert.equal(keepWindow({ retentionEnabled: true, retentionSeconds: 86400, maxWaitTimeSeconds: 3600 }), 3600)
  assert.equal(keepWindow({ retentionEnabled: false, retentionSeconds: 86400 }), null, 'retention off deletes nothing')
  assert.equal(keepWindow({ retentionEnabled: true, completedRetentionSeconds: 600 }), null, 'completed retention waits for its readers')
  assert.equal(keepWindow({ maxWaitTimeSeconds: 120 }), 120)
  assert.equal(keepWindow(null), null)
})

test('a source\'s line: its reading queues with their where, its filtered groups, and where else its messages are', () => {
  const route = routeOf({ route: ROUTE_ORDERS })
  const queues = [{ name: 'orders-it', route: ROUTE_IT }, { name: 'orders', route: ROUTE_ORDERS }]
  const parts = readByLine(route, { queue: 'orders', queues })
  assert.equal(text(parts), "Read by orders-it (where data.country = 'IT') and billing (filtered group) · also in manual-review")
  // A filtered group opens on its queue's page, on its row.
  assert.deepEqual(parts.find((p) => p.t === 'billing').to, { path: '/queues/orders', query: { group: 'billing' } })
  // A group row that says its where is quoted.
  const groups = [{ name: 'billing', queueName: 'orders', where: "data.kind = 'paid'" }]
  assert.equal(text(readByLine(route, { queue: 'orders', queues, groups })),
    "Read by orders-it (where data.country = 'IT') and billing (filtered group, where data.kind = 'paid') · also in manual-review")
  // Holders only: a queue its messages were linked into.
  assert.equal(text(readByLine(routeOf({ route: { holders: [{ queue: 'review' }] } }), { queue: 'orders' })), 'Its messages are also in review')
  // Past four readers, how many more.
  const many = routeOf({ route: { readers: ['a', 'b', 'c', 'd', 'e', 'f'].map((queue) => ({ queue })) } })
  assert.equal(text(readByLine(many, { queue: 'orders' })), 'Read by a, b, c, d and 2 more')
  assert.equal(readByLine(routeOf({ route: ROUTE_IT }), { queue: 'orders-it' }), null)
  assert.equal(readByLine(null), null)
  // What is on partitions none of them takes (the broker's messages.unread).
  assert.equal(text(readByLine(many, { queue: 'orders', unread: 1200 })), 'Read by a, b, c, d and 2 more · 1,200 on partitions none of them takes')
  assert.equal(text(readByLine(many, { queue: 'orders', unread: 0 })), 'Read by a, b, c, d and 2 more')
})

// ---- filtered groups ---------------------------------------------------------------

test('a filtered group is shown on its queue, never under its hidden queue', () => {
  const hidden = `orders${HIDDEN_SEP}billing`
  assert.deepEqual(splitHidden(hidden), { queue: 'orders', group: 'billing' })
  assert.equal(splitHidden('orders'), null)
  assert.equal(shownQueueName(hidden), 'orders')
  assert.equal(shownQueueName('orders'), 'orders')
  const listing = [
    { name: 'billing', queueName: hidden, topics: [hidden], totalLag: 3, maxTimeLag: 40, state: 'Stable' },
    { name: 'audit', queueName: 'orders', topics: ['orders'], totalLag: 0, maxTimeLag: 0, state: 'Stable' },
  ]
  const shown = shownGroups(listing)
  assert.deepEqual(shown[0], { ...listing[0], queueName: 'orders', topics: ['orders'], filtered: true })
  assert.equal(shown[1], listing[1], 'a plain row is kept as it came')
  assert.equal(shownGroups(null), null)
  const rows = groupRows({ groups: shown, queues: [{ name: 'orders', route: ROUTE_ORDERS, partitions: 2 }] })
  assert.deepEqual(rows.map((r) => [r.name, r.queue, r.filter]), [['billing', 'orders', { where: '', known: false }], ['audit', 'orders', null]])
  // The partitions behind of its hidden queue are on its queue too.
  const behind = behindRows([{ partition_name: 'o-1', queue_name: hidden, consumer_group: 'billing', time_lag_seconds: 50, offset_lag: 3 }], { group: 'billing', queue: 'orders' })
  assert.equal(behind.length, 1)
  assert.equal(behind[0].queue, 'orders')
})

test('groupFilterOf: the row\'s own where, else its queue\'s route, else none', () => {
  // As the broker sends a filtered group's row: `filtered` and `where`, an empty where too.
  assert.deepEqual(groupFilterOf({ name: 'billing', queueName: 'orders', filtered: true, where: "data.kind = 'paid'" }), { where: "data.kind = 'paid'", known: true })
  assert.deepEqual(groupFilterOf({ name: 'billing', queueName: 'orders', filtered: true, where: '' }), { where: '', known: true })
  assert.deepEqual(groupFilterOf({ name: 'billing', queueName: 'orders', where: [['data.kind', '=', 'paid']] }), { where: "data.kind = 'paid'", known: true })
  // An empty where on a row that is not filtered is no filter.
  assert.equal(groupFilterOf({ name: 'audit', queueName: 'orders', where: '' }), null)
  // From the queue's route: its reader entry leaves out an empty where, so none there is not known.
  assert.deepEqual(groupFilterOf({ name: 'billing', queueName: 'orders' }, { name: 'orders', route: ROUTE_ORDERS }), { where: '', known: false })
  assert.deepEqual(groupFilterOf({ name: 'billing', queueName: 'orders', filtered: true }), { where: '', known: false })
  assert.deepEqual(groupFilterOf({ name: 'billing', queueName: 'orders' }, { route: { readers: [{ queue: 'orders', group: 'billing', where: 'data.x = 1' }] } }), { where: 'data.x = 1', known: true })
  assert.equal(groupFilterOf({ name: 'audit', queueName: 'orders' }, { name: 'orders', route: ROUTE_ORDERS }), null)
  assert.equal(groupFilterOf(null), null)
  assert.equal(filterWords({ where: 'data.x = 1', known: true }), '')
  assert.equal(filterWords({ where: '', known: true }), 'every message: its where is empty')
  assert.equal(filterWords({ where: '', known: false }), 'filtered; this listing does not say by what')
  assert.equal(filterWords(null), '')
})

test('a filtered group\'s new where: the call, its answer, and the API it goes through', async () => {
  const c = whereCall('billing', 'orders', "  data.kind = 'paid' ")
  assert.deepEqual(c.body, { queue: 'orders', where: "data.kind = 'paid'" })
  assert.equal(c.text, 'POST /api/v1/consumer-groups/billing/subscription {"queue":"orders","where":"data.kind = \'paid\'"}')
  assert.equal(whereCall('a/b', 'q', '').path, '/api/v1/consumer-groups/a%2Fb/subscription')
  assert.deepEqual(whereCall('g', 'q', null).body, { queue: 'q', where: '' }, 'empty: every message')
  assert.deepEqual(whereOutcome({ success: true, consumerGroup: 'billing', queueName: 'orders', where: 'set' }), { ok: true, problem: null })
  assert.deepEqual(whereOutcome({ success: false, error: 'group billing does not filter orders' }), { ok: false, problem: 'group billing does not filter orders' })
  assert.equal(whereOutcome(null).ok, false)
  const sent = []
  await sendCall({ subscription: (...args) => { sent.push(args); return Promise.resolve({ data: { success: true } }) } }, c, { quiet: true })
  assert.deepEqual(sent, [['billing', { queue: 'orders', where: "data.kind = 'paid'" }, { quiet: true }]])
})

// ---- one queue's read: catch-up, ages, holders --------------------------------------

const NOW = Date.parse('2026-11-12T12:00:00Z')
const ago = (s) => new Date(NOW - s * 1000).toISOString()

test('catching up says how far: messages read of those its sources kept', () => {
  const catching = { ...ROUTE_IT, catchingUp: true }
  const route = routeOf({ route: { ...catching, catchUp: { read: 4210, total: 12000 } } })
  assert.deepEqual(route.catchUp, { read: 4210, total: 12000 })
  assert.match(text(readsLine(route)), / · catching up: 4,210 of 12,000$/)
  // The list says only that it does.
  assert.match(text(readsLine(routeOf({ route: catching }))), / · catching up$/)
  assert.equal(routeOf({ route: { ...ROUTE_IT, catchUp: { read: 'x' } } }).catchUp, null)
  assert.deepEqual(routeOf({ route: { ...ROUTE_IT, catchUp: { read: 13, total: 12 } } }).catchUp, { read: 12, total: 12 }, 'never past the total')
})

test('a reader\'s lag is the age of its oldest unconsumed message; none behind says nothing', () => {
  assert.equal(behindText(NOW - 120_000, NOW), '2 min behind')
  assert.equal(behindText(null, NOW), '')
  const route = routeOf({
    route: {
      ...ROUTE_ORDERS,
      readers: [
        { queue: 'orders-it', where: "data.country = 'IT'", oldestUnconsumedAt: ago(120) },
        { queue: 'orders', group: 'billing', where: "data.kind = 'paid'", oldestUnconsumedAt: null },
      ],
    },
  })
  assert.equal(route.readers[0].oldestAt, NOW - 120_000)
  assert.equal(text(readByLine(route, { queue: 'orders', now: NOW })),
    "Read by orders-it (where data.country = 'IT', 2 min behind) and billing (filtered group, where data.kind = 'paid') · also in manual-review")
})

test('a holder turns amber once it holds back what the source\'s completed retention would delete', () => {
  const holders = [
    { queue: 'orders-it', oldestUnconsumedAt: ago(30), holdsRetention: true },
    { queue: 'manual-review', oldestUnconsumedAt: ago(3 * 3600), holdsRetention: true },
    { queue: 'orders', group: 'billing', oldestUnconsumedAt: ago(7200), holdsRetention: true },
    { queue: 'archive', oldestUnconsumedAt: null, holdsRetention: false },
  ]
  const route = routeOf({ route: { ...ROUTE_ORDERS, holders } })
  const keeps = { retentionEnabled: true, completedRetentionSeconds: 3600 }
  const lines = holdLines(route, { queue: 'orders', options: keeps, now: NOW })
  assert.deepEqual(lines.map((l) => text(l.parts)), [
    'manual-review holds back the retention of orders: its oldest unconsumed message is 3 h old.',
    'billing (filtered group) holds back the retention of orders: its oldest unconsumed message is 2 h old.',
  ], 'oldest first; orders-it, 30 s behind, holds nothing back yet')
  assert.deepEqual(lines[1].parts[0].to, { path: '/queues/orders', query: { group: 'billing' } })
  // Without completed retention the source deletes by time alone: no holder holds anything back.
  assert.deepEqual(holdLines(route, { queue: 'orders', options: { retentionEnabled: true, retentionSeconds: 60 }, now: NOW }), [])
  assert.deepEqual(holdLines(route, { queue: 'orders', options: { retentionEnabled: false, completedRetentionSeconds: 60 }, now: NOW }), [])
  assert.deepEqual(holdLines(null, { options: keeps }), [])
})

// ---- a source's message: where else it shows -----------------------------------------

test('also in: the readers whose where takes the message, judged as the broker judges a push', () => {
  const queues = [
    {
      name: 'orders',
      route: {
        ...ROUTE_ORDERS,
        readers: [
          { queue: 'orders-it', where: "data.country = 'IT'" },
          { queue: 'orders-by-customer' },
          { queue: 'orders-all' },
          { queue: 'orders', group: 'billing', where: "headers.type = 'order.paid'" },
          { queue: 'orders', group: 'audit' },
          { queue: 'orders-fr', where: "data.country = 'FR'" },
        ],
      },
    },
    { name: 'orders-it', route: ROUTE_IT },
    // Keyed by a field: a message without it is not routed. Its where comes from its own row.
    { name: 'orders-by-customer', route: { from: [{ queue: 'orders', where: 'data.total > 100' }], partitionBy: 'data.customerId' } },
    { name: 'orders-all', route: { from: ['orders'] } },
  ]
  const m = msg({ country: 'IT', total: 120 }, { headers: { type: 'order.paid' } })
  const a = alsoIn(m, { queues })
  assert.deepEqual(a.queues, ['orders-it', 'orders-all'], 'orders-by-customer: no customerId; orders-fr: not FR')
  assert.deepEqual(a.groups, ['billing', 'audit'])
  assert.equal(text(a.parts), 'Also in orders-it and orders-all, and for groups billing and audit.')
  assert.deepEqual(a.parts[2], { t: 'orders-it', mono: true, to: '/queues/orders-it' })
  const keyed = alsoIn(msg({ country: 'DE', total: 500, customerId: 'c-1' }), { queues })
  assert.deepEqual(keyed.queues, ['orders-by-customer', 'orders-all'])
  assert.equal(text(alsoIn(msg({ country: 'DE' }), { queues: [{ name: 'orders', route: { readers: [{ queue: 'orders', group: 'audit' }] } }] }).parts), 'Also for group audit.')
  // No reader takes it, or the queue has none, or a reader's where is one this console cannot read.
  assert.equal(alsoIn(msg({ country: 'DE' }), { queues: [{ name: 'orders', route: { readers: [{ queue: 'orders-it', where: "data.country = 'IT'" }] } }] }), null)
  assert.equal(alsoIn(msg({ country: 'IT' }), { queues: [{ name: 'orders' }] }), null)
  assert.equal(alsoIn(msg({ country: 'IT' }), { queues: [{ name: 'orders', route: { readers: [{ queue: 'x', where: 'data.x ~ 1' }] } }] }), null)
  assert.equal(alsoIn(null, { queues }), null)
})

test('readField reads a field as a condition does', () => {
  const m = msg({ customerId: 'c-1', n: 0 }, { headers: { customer: 'c-2' } })
  assert.equal(readField('data.customerId', m), 'c-1')
  assert.equal(readField('data.n', m), 0)
  assert.equal(readField('headers.customer', m), 'c-2')
  assert.equal(readField('data.missing', m), undefined)
  assert.equal(readField('headers.missing', m), undefined)
  assert.equal(readField('nonsense', m), undefined)
  assert.equal(readField('data.x', null), undefined)
})

test('a source that reading queues read is not a queue without a reader', () => {
  const queues = [
    { name: 'orders', messages: { pending: 40 }, route: ROUTE_ORDERS },
    { name: 'lonely', messages: { pending: 40 } },
  ]
  assert.deepEqual(queueAttention(queues, []).map((i) => [i.name, i.reason]), [['lonely', 'noReader']])
})

// ---- New queue: a queue that reads other queues ---------------------------------------

const KNOWN = new Map([['orders', { namespace: 'shop' }], ['refunds', { namespace: 'shop' }]])
const READ = { reads: true, from: 'orders', where: '', partitionMode: 'same', partitionField: '', start: 'now' }

test('the New queue form, reading: the configure call it sends', () => {
  const d = newQueueDraft({ name: 'orders-it', leaseTime: '30', retryLimit: '5', ...READ, where: " data.country = 'IT' " }, KNOWN)
  assert.equal(d.ok, true, JSON.stringify(d.errors))
  assert.deepEqual(d.body, {
    queue: 'orders-it',
    options: { from: 'orders', where: "data.country = 'IT'", partitionBy: 'same', start: 'now', leaseTime: 30, retryLimit: 5 },
    mode: 'merge',
  })
  assert.deepEqual(d.reading.sources, ['orders'])
  // Several sources go as a list; a field re-keys it; all starts with what they keep.
  const many = newQueueDraft({ name: 'm', leaseTime: '300', retryLimit: '3', ...READ, from: 'orders, refunds, orders', partitionMode: 'field', partitionField: 'data.customerId', start: 'all' }, KNOWN)
  assert.equal(many.ok, true)
  assert.deepEqual(many.body.options, { from: ['orders', 'refunds'], partitionBy: 'data.customerId', start: 'all', leaseTime: 300, retryLimit: 3 })
  // Not reading: none of it is sent, whatever the fields still hold.
  const plain = newQueueDraft({ name: 'p', leaseTime: '300', retryLimit: '3', ...READ, reads: false }, KNOWN)
  assert.deepEqual(plain.body.options, { leaseTime: 300, retryLimit: 3 })
  assert.equal(plain.reading, null)
})

test('a reading queue keeps what its sources keep: the options it refuses are never sent', () => {
  const form = {
    name: 'orders-it', leaseTime: '300', retryLimit: '3', ...READ,
    retentionEnabled: true, retentionSeconds: '600', dedupWindowSeconds: '60', encryptionEnabled: true, retentionSinkHold: 'lake', priority: '2',
  }
  const d = newQueueDraft(form, KNOWN)
  assert.equal(d.ok, true)
  assert.deepEqual(d.extra, { priority: 2 })
  for (const k of READER_KEEPS) assert.equal(k in d.body.options, false, k)
  // The same form as an ordinary queue sends them.
  const plain = newQueueDraft({ ...form, reads: false }, KNOWN)
  assert.equal(plain.body.options.retentionSeconds, 600)
  assert.equal(plain.body.options.encryptionEnabled, true)
})

test('the New queue form, reading: what the broker would refuse, refused before the call', () => {
  const draft = (patch) => newQueueDraft({ name: 'orders-it', leaseTime: '300', retryLimit: '3', ...READ, ...patch }, KNOWN)
  assert.equal(draft({ from: ' , ' }).errors.from, 'Name the queues it reads.')
  assert.equal(draft({ from: 'orders, orders-it' }).errors.from, 'orders-it cannot read itself.')
  assert.equal(draft({ from: 'orderz' }).errors.from, 'orderz is not a queue on this cluster: a reading queue reads queues that exist.')
  assert.match(draft({ from: 'a, b' }).errors.from, /^a and b are not queues/)
  assert.match(draft({ from: `orders${HIDDEN_SEP}billing` }).errors.from, /filtered group/)
  assert.match(draft({ where: "data.kind = 'a' or data.kind = 'b'" }).errors.where, /only AND/)
  assert.match(draft({ partitionMode: 'field', partitionField: 'customerId' }).errors.partitionBy, /data\.<path> or headers\.<name>/)
  for (const patch of [{ from: '' }, { where: 'nope' }, { partitionMode: 'field', partitionField: '' }]) assert.equal(draft(patch).ok, false, JSON.stringify(patch))
  // Before the list is read nothing is known to be missing: the broker answers.
  assert.equal(readingOptions({ from: 'anything' }, { name: 'x', known: null }).errors.from, undefined)
})

test('parseSources and partitionFieldValid follow the broker', () => {
  assert.deepEqual(parseSources(' orders ,refunds,, orders , my queue'), ['orders', 'refunds', 'my queue'])
  assert.deepEqual(parseSources(undefined), [])
  for (const f of ['data.customerId', 'data.a.b', 'headers.customer', 'headers.x-y_z.1']) assert.equal(partitionFieldValid(f), true, f)
  for (const f of ['', 'same', 'customerId', 'data.', 'data..x', 'headers.Customer', 'headers.', `data.${Array.from({ length: 17 }, () => 'a').join('.')}`]) {
    assert.equal(partitionFieldValid(f), false, f)
  }
})

test('the preview: the where on the sources\' last messages', () => {
  const conds = must("data.country = 'IT'")
  const it = (n) => Array.from({ length: n }, (_, i) => msg({ country: i % 4 === 0 ? 'IT' : 'FR' }))
  assert.equal(previewOf({ sources: ['orders'], conds: [] }), null, 'no where: nothing to try')
  assert.equal(previewOf({ sources: [], conds }), null)
  assert.equal(previewOf({ sources: ['orders'], conds, samples: new Map() }).kind, 'reading')
  const full = previewOf({ sources: ['orders'], conds, samples: new Map([['orders', { messages: it(PREVIEW_LIMIT) }]]) })
  assert.deepEqual([full.kind, full.n, full.of], ['count', 50, 200])
  assert.equal(full.text, '50 of the last 200 messages of orders match.')
  const few = previewOf({ sources: ['orders', 'refunds'], conds, samples: new Map([['orders', { messages: it(8) }], ['refunds', { messages: [] }]]) })
  assert.equal(few.text, '2 of the 8 messages orders and refunds got in the last 7 days match.')
  // A page the broker cut by its byte budget is "the last" of more, not all of them.
  assert.equal(previewOf({ sources: ['orders'], conds, samples: new Map([['orders', { messages: it(8), cut: true }]]) }).text,
    '2 of the last 8 messages of orders match.')
  assert.equal(previewOf({ sources: ['orders'], conds, samples: new Map([['orders', { messages: [] }]]) }).text,
    'orders has no message from the last 7 days to try it on.')
  assert.equal(previewOf({ sources: ['orders'], conds, samples: new Map([['orders', { error: 'Rate limited' }]]) }).kind, 'error')
  const p = previewParams('orders', Date.parse('2026-11-12T10:00:00Z'))
  assert.deepEqual(p, { queue: 'orders', limit: PREVIEW_LIMIT, from: '2026-11-05T10:00:00.000Z' })
})

// ---- deleting a source -------------------------------------------------------------------

test('a delete its reading queues refuse: the readers named, and the force call', () => {
  assert.deepEqual(deleteQueueCall('a/b'), { path: '/api/v1/resources/queues/a%2Fb', params: undefined, text: 'DELETE /api/v1/resources/queues/a%2Fb' })
  assert.deepEqual(deleteQueueCall('orders', { force: true }), {
    path: '/api/v1/resources/queues/orders', params: { force: true }, text: 'DELETE /api/v1/resources/queues/orders?force=true',
  })
  // As httpClient hands it over: the envelope's code, and the envelope as the body.
  const body = { deleted: false, code: 'has_readers', error: 'orders is read by orders-it, orders-fr: delete them first, or delete with force=true (their links then read as gone)', readers: ['orders-it', 'orders-fr'] }
  const r = hasReadersOf({ status: 409, code: 'has_readers', body })
  assert.deepEqual(r.readers.map((x) => x.queue), ['orders-it', 'orders-fr'])
  assert.equal(r.said, body.error)
  assert.equal(text(readersParts(r.readers)), 'orders-it and orders-fr')
  assert.ok(hasReadersOf({ status: 409, code: null, body }), 'the code read from the body when the envelope had none')
  assert.equal(hasReadersOf({ status: 409, code: 'conflict', body: { code: 'conflict' } }), null)
  assert.equal(hasReadersOf({ status: 400, body }), null)
  assert.equal(hasReadersOf(null), null)
  assert.deepEqual(filteredGroupsOf(routeOf({ route: ROUTE_ORDERS })).map((g) => g.group), ['billing'])
  assert.deepEqual(filteredGroupsOf(null), [])
})

// ---- headers -----------------------------------------------------------------------------

test('a message\'s headers, by name, and none when it has none', () => {
  assert.deepEqual(headerRows({ headers: { type: 'order.paid', 'content-type': 'application/json', traceparent: '00-4bf9-01' } }), [
    { name: 'content-type', value: 'application/json' },
    { name: 'traceparent', value: '00-4bf9-01' },
    { name: 'type', value: 'order.paid' },
  ])
  assert.deepEqual(headerRows({ headers: null }), [])
  assert.deepEqual(headerRows({ headers: {} }), [])
  assert.deepEqual(headerRows({}), [])
  assert.deepEqual(headerRows(null), [])
  assert.deepEqual(headerRows({ headers: ['x'] }), [])
})
