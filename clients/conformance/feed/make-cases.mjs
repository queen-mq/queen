#!/usr/bin/env node
/**
 * Writes cases/*.json: the feed-stream conformance cases (README.md, section 2).
 * Edit here, run `node make-cases.mjs`, then regenerate expected/ with
 * `node harness.mjs --sdk js --write`.
 */
import { writeFileSync, mkdirSync, readdirSync, unlinkSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))
const T0 = Date.parse('2026-11-06T09:00:00.000Z')
const at = (sec) => new Date(T0 + Math.round(sec * 1000)).toISOString()

// ---- expressions ----
const g = (p) => ({ $get: p })
const eq = (a, b) => ({ $eq: [a, b] })
const isQueue = (name) => eq(g('0.queue'), name)
const iff = (c, a, b) => ({ $if: [c, a, b] })
const str = (e) => ({ $str: e })
const add = (...e) => ({ $add: e })
const keys = (e) => ({ $keys: e })
const thr = (msg) => ({ $throw: msg })
const TRUE = true

// ---- script steps ----
const push = (queue, partition, data, transactionId) => ({ queue, partition, data, transactionId })
const tx = (sec, ...pushes) => ({ at: at(sec), push: pushes })
const txs = (...list) => ({ tx: list })
const start = (name, extra = {}) => ({ start: name, ...extra })
const idle = (name, extra = {}) => ({ idle: name, ...extra })
const stop = (name) => ({ stop: name })
const kill = (name) => ({ kill: name })
const clock = (sec) => ({ clock: at(sec) })
const state = (name, step, key, as) => ({ state: name, step, key, as })
const list = (name, step, limit, as) => ({ list: name, step, limit, as })
const view = (op, extra, as) => ({ view: { op, ...extra }, as })
const waitDone = (name) => ({ wait: name, for: 'done' })
const waitRole = (name, role) => ({ waitRole: name, role })
const metrics = (name, as, keysList) => ({ metrics: name, as, ...(keysList ? { keys: keysList } : {}) })

const cases = []
function add_(c) {
  if (!c.run) c.run = {}
  c.run.queryId = c.run.queryId || `c-${c.name}`
  c.mode = c.mode || 'fake'
  cases.push(c)
}

// =========================================================================
// Stateless: where, map, flatMap, routing, foreach
// =========================================================================

add_({
  name: 'route-where-map',
  about: 'stateless: a where pushed to the broker, map, one sink; a restart reads on without duplicates',
  mode: 'both',
  source: { queues: ['c-orders'], from: 'earliest' },
  chain: [
    { op: 'where', args: ['data.country', '=', 'IT'] },
    { op: 'map', fn: { id: g('0.data.id'), total: g('0.data.total'), from: g('0.partition') } },
    { op: 'to', target: 'c-orders-it' }
  ],
  script: [
    txs(
      tx(1, push('c-orders', 'p1', { id: 1, country: 'IT', total: 10 }, 'o1')),
      tx(2, push('c-orders', 'p2', { id: 2, country: 'DE', total: 20 }, 'o2')),
      tx(3, push('c-orders', 'p1', { id: 3, country: 'IT', total: 30 }, 'o3'), push('c-orders', 'p3', { id: 4, country: 'IT', total: 40 }, 'o4'))
    ),
    start('A'), idle('A'), stop('A'),
    txs(tx(4, push('c-orders', 'p2', { id: 5, country: 'IT', total: 50 }, 'o5'))),
    start('B'), idle('B'), stop('B')
  ]
})

add_({
  name: 'route-fn-flatmap',
  about: 'flatMap makes several records per message; a .to() function picks a queue, {queue, partition}, a list, or nothing',
  mode: 'both',
  source: { queues: ['c-baskets'], from: 'earliest' },
  chain: [
    { op: 'flatMap', fn: g('0.data.items') },
    {
      op: 'to',
      target: {
        fn: iff(eq(g('0.kind'), 'a'),
          { queue: 'c-out-a', partition: str(g('0.id')) },
          iff(eq(g('0.kind'), 'b'), ['c-out-b', { queue: 'c-out-all', partition: 'x' }], null))
      }
    }
  ],
  script: [
    txs(
      tx(1, push('c-baskets', 'p1', { items: [{ kind: 'a', id: 1 }, { kind: 'b', id: 2 }, { kind: 'none', id: 3 }] }, 'b1')),
      tx(2, push('c-baskets', 'p2', { items: [{ kind: 'b', id: 4 }, { kind: 'a', id: 5.5 }] }, 'b2')),
      tx(3, push('c-baskets', 'p2', { items: [] }, 'b3'))
    ),
    start('A'), idle('A'), stop('A')
  ]
})

add_({
  name: 'route-list-partition',
  about: 'a list of queues gets every record; the sink partition option is a function of the value',
  mode: 'both',
  source: { queues: ['c-events'], from: 'earliest' },
  chain: [
    { op: 'map', fn: { region: g('0.data.region'), n: g('0.data.n') } },
    { op: 'to', target: ['c-copy-1', 'c-copy-2'], partition: { fn: g('0.region') } }
  ],
  script: [
    txs(
      tx(1, push('c-events', 'p1', { region: 'eu', n: 1 }, 'e1'), push('c-events', 'p2', { region: 'us', n: 2 }, 'e2')),
      tx(2, push('c-events', 'p1', { region: 'eu', n: 3 }, 'e3'))
    ),
    start('A'), idle('A'), stop('A')
  ]
})

add_({
  name: 'where-ops',
  about: 'where with > prefix in exists != on the broker, then a local filter; missing fields hold for no op',
  mode: 'both',
  source: { queues: ['c-sales'], from: 'earliest' },
  chain: [
    { op: 'where', args: ['data.total', '>', 10] },
    { op: 'where', args: ['partition', 'prefix', 'eu-'] },
    { op: 'where', args: ['data.kind', 'in', ['x', 'y']] },
    { op: 'where', args: ['data.tags', 'exists'] },
    { op: 'where', args: ['data.status', '!=', 'void'] },
    { op: 'filter', fn: eq(g('0.data.vip'), true) },
    { op: 'map', fn: { id: g('0.data.id') } },
    { op: 'to', target: 'c-where-out' }
  ],
  script: [
    txs(
      tx(1,
        push('c-sales', 'eu-1', { id: 1, total: 11, kind: 'x', tags: [], status: 'ok', vip: true }, 's1'),
        push('c-sales', 'eu-1', { id: 2, total: 10, kind: 'x', tags: [], status: 'ok', vip: true }, 's2'),
        push('c-sales', 'us-1', { id: 3, total: 50, kind: 'x', tags: [], status: 'ok', vip: true }, 's3'),
        push('c-sales', 'eu-2', { id: 4, total: 50, kind: 'z', tags: [], status: 'ok', vip: true }, 's4')),
      tx(2,
        push('c-sales', 'eu-2', { id: 5, total: 50, kind: 'y', status: 'ok', vip: true }, 's5'),
        push('c-sales', 'eu-2', { id: 6, total: 50, kind: 'y', tags: ['a'], vip: true }, 's6'),
        push('c-sales', 'eu-3', { id: 7, total: 50, kind: 'y', tags: ['a'], status: 'void', vip: true }, 's7'),
        push('c-sales', 'eu-3', { id: 8, total: 99.5, kind: 'y', tags: ['a'], status: 'ok', vip: false }, 's8'),
        push('c-sales', 'eu-3', { id: 9, total: 99.5, kind: 'y', tags: { a: 1 }, status: 'ok', vip: true }, 's9'))
    ),
    start('A'), idle('A'), stop('A')
  ]
})

add_({
  name: 'foreach-ctx',
  about: 'foreach gets the value and the message context, once per message, in log order',
  mode: 'both',
  source: { queues: ['c-ticks'], from: 'earliest' },
  chain: [
    { op: 'map', fn: { n: g('0.data.n'), q: g('0.queue') } },
    { op: 'foreach' }
  ],
  compareMetrics: ['emitted', 'effects', 'outputs'],
  script: [
    txs(
      tx(1, push('c-ticks', 'p1', { n: 1 }, 't1')),
      tx(2, push('c-ticks', 'p2', { n: 2 }, 't2'), push('c-ticks', 'p1', { n: 3 }, 't3'))
    ),
    start('A'), idle('A'), stop('A')
  ]
})

// =========================================================================
// Windows on the feed's clock
// =========================================================================

add_({
  name: 'window-tumbling',
  about: 'a 60 s tumbling window with aggregate per partition across two queues, closed when the feed clock passes its end',
  source: { queues: ['c-orders', 'c-refunds'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1, sum: g('0.amount'), max: g('0.amount'), min: g('0.amount'), avg: g('0.amount') } },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowKey'), end: g('1.windowEnd'), value: g('0') } },
    { op: 'to', target: 'c-minute-totals' }
  ],
  script: [
    txs(
      tx(5, push('c-orders', 'u1', { amount: 10 }, 'a1')),
      tx(15, push('c-refunds', 'u1', { amount: -4 }, 'a2')),
      tx(20, push('c-orders', 'u2', { amount: 7 }, 'a3')),
      tx(59.999, push('c-orders', 'u1', { amount: 3 }, 'a4'))
    ),
    start('A'), idle('A'),
    state('A', 'step1', 'u1', 'u1-open'),
    clock(60.5), idle('A'),
    txs(tx(70, push('c-orders', 'u1', { amount: 100 }, 'a5'), push('c-orders', 'u3', { note: 'no amount' }, 'a6'))),
    idle('A'),
    list('A', 'step1', 10, 'open-list'),
    stop('A')
  ]
})

add_({
  name: 'window-sliding',
  about: 'sliding windows (60 s every 30 s): each message counts in every window it falls in; reduce with an initial value',
  source: { queues: ['c-pay'], from: 'earliest' },
  chain: [
    { op: 'windowSliding', opts: { size: 60, slide: 30 } },
    { op: 'reduce', fn: add(g('0'), g('1.amount')), initial: 0 },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowStart'), end: g('1.windowEnd'), sum: g('0') } },
    { op: 'to', target: 'c-sliding-out' }
  ],
  script: [
    txs(
      tx(10, push('c-pay', 'k', { amount: 1 }, 'p1')),
      tx(40, push('c-pay', 'k', { amount: 2 }, 'p2')),
      tx(70, push('c-pay', 'k', { amount: 4 }, 'p3'))
    ),
    start('A'), idle('A'),
    clock(121), idle('A'), stop('A')
  ]
})

add_({
  name: 'window-session',
  about: 'sessions with a 20 s gap per user: a message exactly the gap later stays in the session; quiet sessions close on the clock',
  source: { queues: ['c-clicks'], from: 'earliest' },
  chain: [
    { op: 'keyBy', fn: g('0.data.user') },
    { op: 'windowSession', opts: { gap: 20 } },
    { op: 'aggregate', fields: { count: 1, sum: g('0.n') } },
    { op: 'map', fn: { user: g('1.key'), start: g('1.windowStart'), end: g('1.windowEnd'), value: g('0') } },
    { op: 'to', target: 'c-sessions' }
  ],
  script: [
    txs(
      tx(0, push('c-clicks', 'p1', { user: 'u1', n: 1 }, 'c1')),
      tx(5, push('c-clicks', 'p2', { user: 'u2', n: 10 }, 'c2')),
      tx(20, push('c-clicks', 'p1', { user: 'u1', n: 2 }, 'c3')),
      tx(45, push('c-clicks', 'p3', { user: 'u1', n: 4 }, 'c4'))
    ),
    start('A'), idle('A'),
    clock(90), idle('A'), stop('A')
  ]
})

add_({
  name: 'window-cron',
  about: 'cron windows on minute boundaries keyed by a field; an object accumulator',
  source: { queues: ['c-temps'], from: 'earliest' },
  chain: [
    { op: 'keyBy', fn: g('0.data.sensor') },
    { op: 'windowCron', opts: { every: 'minute' } },
    { op: 'reduce', fn: { n: add(g('0.n'), 1), last: g('1.v') }, initial: { n: 0 } },
    { op: 'map', fn: { sensor: g('1.key'), minute: g('1.windowKey'), acc: g('0') } },
    { op: 'to', target: 'c-cron-out' }
  ],
  script: [
    txs(
      tx(10, push('c-temps', 'a', { sensor: 's1', v: 20 }, 't1')),
      tx(30, push('c-temps', 'b', { sensor: 's1', v: 21 }, 't2')),
      tx(50, push('c-temps', 'a', { sensor: 's2', v: 5 }, 't3')),
      tx(65, push('c-temps', 'a', { sensor: 's1', v: 22 }, 't4'))
    ),
    start('A'), idle('A'),
    clock(125), idle('A'), stop('A')
  ]
})

add_({
  name: 'window-grace',
  about: 'gracePeriod delays the close past the window end',
  source: { queues: ['c-hits'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 30, gracePeriod: 15 } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowKey'), count: g('0.count') } },
    { op: 'to', target: 'c-grace-out' }
  ],
  script: [
    txs(tx(10, push('c-hits', 'h', {}, 'h1')), tx(35, push('c-hits', 'h', {}, 'h2'))),
    start('A'), idle('A'),
    clock(44), idle('A'),
    metrics('A', 'before-grace', ['outputs']),
    clock(46), idle('A'),
    clock(76), idle('A'), stop('A')
  ]
})

add_({
  name: 'window-restart-stop',
  about: 'a stop with open windows writes a snapshot; the next process loads it and closes the windows with every message counted once',
  source: { queues: ['c-visits'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1, sum: g('0.n') } },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowKey'), value: g('0') } },
    { op: 'to', target: 'c-visits-out' }
  ],
  script: [
    txs(tx(5, push('c-visits', 'v1', { n: 1 }, 'v1')), tx(10, push('c-visits', 'v2', { n: 2 }, 'v2'))),
    start('A'), idle('A'), stop('A'),
    txs(tx(20, push('c-visits', 'v1', { n: 4 }, 'v3'))),
    start('B'), idle('B'),
    clock(61), idle('B'), stop('B')
  ]
})

add_({
  name: 'window-restart-kill',
  about: 'a process killed with open windows: the next one loads the last snapshot and reads the feed again from its position, sending nothing twice',
  source: { queues: ['c-visits'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1, sum: g('0.n') } },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowKey'), value: g('0') } },
    { op: 'to', target: 'c-visits-out' }
  ],
  compareMetrics: [],
  script: [
    txs(tx(5, push('c-visits', 'v1', { n: 1 }, 'v1')), tx(10, push('c-visits', 'v2', { n: 2 }, 'v2'))),
    start('A'), idle('A'),
    txs(tx(30, push('c-visits', 'v1', { n: 8 }, 'v4'))),
    idle('A'), kill('A'),
    txs(tx(40, push('c-visits', 'v2', { n: 16 }, 'v5'))),
    start('B'), idle('B', { timeoutMs: 30000 }),
    metrics('B', 'b-replayed', ['replays']),
    clock(61), idle('B'), stop('B')
  ]
})

add_({
  name: 'window-snapshot-layers',
  about: 'a snapshot at every commit: a full layer, then deltas; the next process loads every layer in order',
  source: { queues: ['c-visits'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1, sum: g('0.n') } },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowKey'), value: g('0') } },
    { op: 'to', target: 'c-visits-out' }
  ],
  timing: { snapshotEveryMillis: 0 },
  script: [
    txs(tx(1, push('c-visits', 'a', { n: 1 }, 'v1'))), start('A'), idle('A'),
    txs(tx(2, push('c-visits', 'b', { n: 2 }, 'v2'))), idle('A'),
    txs(tx(3, push('c-visits', 'a', { n: 4 }, 'v3'))), idle('A'),
    txs(tx(4, push('c-visits', 'c', { n: 8 }, 'v4'))), idle('A'),
    stop('A'),
    start('B'), idle('B'),
    txs(tx(5, push('c-visits', 'c', { n: 16 }, 'v5'))), idle('B'),
    clock(61), idle('B'), stop('B')
  ]
})

add_({
  name: 'window-mass-close',
  about: '3,000 windows that end at one time: the close commits in pieces (maxPendingOutputs 500), each result once, the record counts the closes',
  source: { queues: ['c-burst'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'to', target: 'c-burst-out' }
  ],
  timing: { maxPendingOutputs: 500 },
  compareMetrics: ['emitted', 'outputs'],
  script: [
    txs(...Array.from({ length: 30 }, (_, i) => tx(1 + i, ...Array.from({ length: 100 }, (_, j) => push('c-burst', `k${i * 100 + j}`, {}, `m${i}-${j}`))))),
    start('A'), idle('A'),
    clock(61), idle('A', { timeoutMs: 60000 }), stop('A')
  ]
})

// =========================================================================
// Patterns and joins
// =========================================================================

const followups = (extra = {}) => ({
  source: { queues: ['c-orders', 'c-payments'], from: 'earliest' },
  chain: [
    {
      op: 'pattern',
      opts: {
        start: isQueue('c-orders'),
        then: isQueue('c-payments'),
        within: '15m',
        onMatch: { order: g('0.data.id'), paid: g('1.data.amount'), at: g('1.ts') },
        onTimeout: { order: g('0.data.id'), remind: true },
        ...extra
      }
    },
    { op: 'to', target: 'c-followups' }
  ]
})

add_({
  name: 'pattern-match-timeout',
  about: 'an order then its payment within 15 minutes is a match across two queues; an order alone times out on a quiet feed; a second start while one waits is ignored (keep)',
  ...followups(),
  script: [
    txs(
      tx(0, push('c-orders', 'order-1', { id: 1 }, 'o1')),
      tx(60, push('c-orders', 'order-2', { id: 2 }, 'o2')),
      tx(120, push('c-orders', 'order-1', { id: 11 }, 'o1b')),
      tx(300, push('c-payments', 'order-1', { amount: 30 }, 'p1')),
      tx(400, push('c-payments', 'order-9', { amount: 1 }, 'p9'))
    ),
    start('A'), idle('A'),
    state('A', 'step1', 'order-2', 'waiting-2'),
    clock(60 + 900), idle('A'),
    clock(60 + 901), idle('A'), stop('A')
  ]
})

add_({
  name: 'pattern-onrepeat-restart',
  about: "onRepeat 'restart': a new start replaces the waiting one, and the deadline counts from it; a then exactly `within` after is in time",
  ...followups({ onRepeat: 'restart' }),
  script: [
    txs(
      tx(0, push('c-orders', 'order-1', { id: 1 }, 'o1')),
      tx(600, push('c-orders', 'order-1', { id: 2 }, 'o2')),
      tx(1500, push('c-payments', 'order-1', { amount: 5 }, 'p1')),
      tx(1600, push('c-orders', 'order-3', { id: 3 }, 'o3'))
    ),
    start('A'), idle('A'),
    clock(1600 + 900), idle('A'),
    txs(tx(1600 + 900, push('c-payments', 'order-3', { amount: 7 }, 'p3'))),
    idle('A'), stop('A')
  ]
})

add_({
  name: 'pattern-flatmap-key',
  about: 'after a flatMap the key sees each item: one order waits once per item; key(msg, value) picks the entity',
  source: { queues: ['c-orders', 'c-stock'], from: 'earliest' },
  chain: [
    { op: 'flatMap', fn: iff(isQueue('c-orders'), g('0.data.items'), [g('0.data')]) },
    {
      op: 'pattern',
      opts: {
        start: isQueue('c-orders'),
        then: isQueue('c-stock'),
        key: g('1.sku'),
        within: '10m',
        onMatch: { sku: g('1.data.sku'), order: g('0.data.id'), qty: g('1.data.qty') },
        onTimeout: { order: g('0.data.id'), noStock: true }
      }
    },
    { op: 'to', target: 'c-stock-out' }
  ],
  script: [
    txs(
      tx(0, push('c-orders', 'o-1', { id: 1, items: [{ sku: 'A' }, { sku: 'B' }] }, 'o1')),
      tx(30, push('c-stock', 'w1', { sku: 'B', qty: 3 }, 's1')),
      tx(40, push('c-stock', 'w1', { sku: 'Z', qty: 1 }, 's2'))
    ),
    start('A'), idle('A'),
    clock(601), idle('A'), stop('A')
  ]
})

add_({
  name: 'pattern-keep',
  about: "keep(msg, value) keeps only a projection of the waiting message: in the state, in the snapshot and in what onMatch sees; .name() names the step for live reads",
  source: { queues: ['c-orders', 'c-payments'], from: 'earliest' },
  chain: [
    {
      op: 'pattern',
      opts: {
        start: isQueue('c-orders'),
        then: isQueue('c-payments'),
        within: '15m',
        keep: { id: g('0.data.id') },
        onMatch: { start: g('0.data'), then: g('1.data') }
      }
    },
    { op: 'name', name: 'waits' },
    { op: 'to', target: 'c-keep-out' }
  ],
  script: [
    txs(
      tx(0, push('c-orders', 'order-1', { id: 1, big: 'x'.repeat(50), more: [1, 2, 3] }, 'o1')),
      tx(1, push('c-orders', 'order-2', { id: 2, big: 'y'.repeat(50) }, 'o2')),
      tx(2, push('c-payments', 'order-1', { amount: 9, note: 'kept' }, 'p1'))
    ),
    start('A'), idle('A'),
    state('A', 'waits', 'order-2', 'waits-2'),
    list('A', 'waits', 10, 'waits-list'),
    stop('A')
  ]
})

add_({
  name: 'pattern-default-output',
  about: 'a pattern without onMatch sends { key, start, then } with the two messages as the step keeps them; a timeout without onTimeout sends nothing',
  source: { queues: ['c-a', 'c-b'], from: 'earliest' },
  chain: [
    { op: 'pattern', opts: { start: isQueue('c-a'), then: isQueue('c-b'), within: '1m' } },
    { op: 'to', target: 'c-default-out' }
  ],
  script: [
    txs(
      tx(0, push('c-a', 'e1', { v: 1 }, 'a1'), push('c-a', 'e2', { v: 2 }, 'a2')),
      tx(10, push('c-b', 'e1', { v: 3 }, 'b1'))
    ),
    start('A'), idle('A'),
    clock(61), idle('A'), stop('A')
  ]
})

add_({
  name: 'join-two-sides',
  about: 'a join pairs a left and a right in either order within 2 days; one left alone times out with its side',
  source: { queues: ['c-orders', 'c-shipments'], from: 'earliest' },
  chain: [
    {
      op: 'join',
      opts: {
        left: isQueue('c-orders'),
        right: isQueue('c-shipments'),
        within: '2d',
        select: { order: g('0.data.id'), carrier: g('1.data.carrier') },
        onTimeout: { key: g('0.partition'), side: g('1') }
      }
    },
    { op: 'to', target: 'c-joined' }
  ],
  script: [
    txs(
      tx(0, push('c-shipments', 'o-1', { carrier: 'dhl' }, 's1')),
      tx(10, push('c-orders', 'o-1', { id: 1 }, 'o1')),
      tx(20, push('c-orders', 'o-2', { id: 2 }, 'o2')),
      tx(30, push('c-shipments', 'o-3', { carrier: 'ups' }, 's3'))
    ),
    start('A'), idle('A'),
    clock(2 * 86400 + 31), idle('A'), stop('A')
  ]
})

add_({
  name: 'join-many-sides',
  about: "a join of three sides (two by queue name, one by a test) sends select(got, key) when the last is in; a deadline sends onTimeout(got, missing, key); onRepeat 'latest' replaces a side",
  source: { queues: ['c-orders', 'c-payments', 'c-shipments'], from: 'earliest' },
  chain: [
    {
      op: 'join',
      opts: {
        sides: { order: 'c-orders', paid: 'c-payments', sent: isQueue('c-shipments') },
        within: '1h',
        onRepeat: 'latest',
        select: { key: g('1'), parts: keys(g('0')), amount: g('0.paid.data.amount'), carrier: g('0.sent.data.carrier') },
        onTimeout: { key: g('2'), missing: g('1'), got: keys(g('0')) }
      }
    },
    { op: 'to', target: 'c-three' }
  ],
  script: [
    txs(
      tx(0, push('c-orders', 'o-1', { id: 1 }, 'o1')),
      tx(10, push('c-payments', 'o-1', { amount: 5 }, 'p1')),
      tx(20, push('c-payments', 'o-1', { amount: 6 }, 'p1b')),
      tx(30, push('c-shipments', 'o-1', { carrier: 'gls' }, 's1')),
      tx(40, push('c-orders', 'o-2', { id: 2 }, 'o2')),
      tx(50, push('c-shipments', 'o-2', { carrier: 'dpd' }, 's2'))
    ),
    start('A'), idle('A'),
    clock(40 + 3601), idle('A'), stop('A')
  ]
})

add_({
  name: 'gather-steps',
  about: 'gather by steps: onComplete when every step is in; a finished run is remembered, so a late duplicate opens nothing; a run missing a step times out with the missing names',
  source: { queues: ['c-pay', 'c-stock', 'c-ship'], from: 'earliest' },
  chain: [
    {
      op: 'gather',
      opts: {
        steps: ['c-pay', 'c-stock', 'c-ship'],
        key: g('0.data.orderId'),
        within: '5m',
        onComplete: { order: g('1'), got: keys(g('0')) },
        onTimeout: { order: g('2'), missing: g('1'), got: keys(g('0')) }
      }
    },
    { op: 'name', name: 'runs' },
    { op: 'to', target: 'c-gathered' }
  ],
  script: [
    txs(
      tx(0, push('c-pay', 'x', { orderId: 'r1' }, 'g1')),
      tx(10, push('c-stock', 'y', { orderId: 'r1' }, 'g2')),
      tx(20, push('c-pay', 'x', { orderId: 'r2' }, 'g3')),
      tx(30, push('c-ship', 'z', { orderId: 'r1' }, 'g4')),
      tx(40, push('c-pay', 'w', { orderId: 'r1' }, 'g5')),
      tx(50, push('c-stock', 'y', { orderId: 'r2' }, 'g6'))
    ),
    start('A'), idle('A'),
    state('A', 'runs', 'r1', 'r1-done'),
    state('A', 'runs', 'r2', 'r2-open'),
    clock(321), idle('A'), stop('A')
  ]
})

add_({
  name: 'gather-count',
  about: 'gather by count: a part says how many parts its run has (the largest count wins); complete at that many; a deadline says how many are missing (null when no part said)',
  source: { queues: ['c-parts'], from: 'earliest' },
  chain: [
    {
      op: 'gather',
      opts: {
        count: g('0.data.parts'),
        key: g('0.data.batch'),
        within: '10m',
        onComplete: { batch: g('1'), n: { $len: g('0') } },
        onTimeout: { batch: g('2'), missing: g('1'), n: { $len: g('0') } }
      }
    },
    { op: 'to', target: 'c-batches' }
  ],
  script: [
    txs(
      tx(0, push('c-parts', 'a', { batch: 'b1', parts: 3 }, 'x1')),
      tx(5, push('c-parts', 'b', { batch: 'b1' }, 'x2')),
      tx(6, push('c-parts', 'a', { batch: 'b2', parts: 4 }, 'x3')),
      tx(7, push('c-parts', 'c', { batch: 'b1' }, 'x4')),
      tx(8, push('c-parts', 'b', { batch: 'b2', parts: 2 }, 'x5')),
      tx(9, push('c-parts', 'c', { batch: 'b3' }, 'x6'))
    ),
    start('A'), idle('A'),
    clock(611), idle('A'), stop('A')
  ]
})

// =========================================================================
// Table joins
// =========================================================================

add_({
  name: 'join-table',
  about: "joinTable: a new stream first reads the table queue from its start (initial 'all'); a message sees the table as of its position; null deletes a row; missing 'keep'",
  source: { queues: ['c-bookings'] },
  chain: [
    { op: 'joinTable', opts: { queue: 'c-settings', on: g('0.data.propertyId'), select: { booking: g('0.id'), currency: g('1.currency') } } },
    { op: 'name', name: 'settings' },
    { op: 'to', target: 'c-priced' }
  ],
  script: [
    txs(
      tx(0, push('c-settings', 'prop-1', { currency: 'EUR' }, 'r1')),
      tx(1, push('c-settings', 'prop-2', { currency: 'USD' }, 'r2'))
    ),
    start('A'),
    txs(
      tx(10, push('c-bookings', 'b', { id: 1, propertyId: 'prop-1' }, 'k1')),
      tx(11, push('c-settings', 'prop-1', { currency: 'GBP' }, 'r3')),
      tx(12, push('c-bookings', 'b', { id: 2, propertyId: 'prop-1' }, 'k2')),
      tx(13, push('c-settings', 'prop-2', null, 'r4')),
      tx(14, push('c-bookings', 'b', { id: 3, propertyId: 'prop-2' }, 'k3')),
      tx(15, push('c-bookings', 'b', { id: 4, propertyId: 'prop-3' }, 'k4'))
    ),
    idle('A'),
    state('A', 'settings', 'prop-1', 'row-1'),
    list('A', 'settings', 10, 'rows'),
    stop('A')
  ]
})

add_({
  name: 'join-table-drop-same-tx',
  about: "missing 'drop' leaves out a message with no row; a row in the same transaction as a message counts before it",
  source: { queues: ['c-bookings'], from: 'earliest' },
  chain: [
    { op: 'joinTable', opts: { queue: 'c-settings', on: g('0.data.propertyId'), missing: 'drop', select: { booking: g('0.id'), currency: g('1.currency'), pos: g('2.position') } } },
    { op: 'to', target: 'c-priced' }
  ],
  script: [
    txs(
      tx(0, push('c-bookings', 'b', { id: 1, propertyId: 'prop-9' }, 'k1'), push('c-settings', 'prop-9', { currency: 'CHF' }, 'r1')),
      tx(1, push('c-bookings', 'b', { id: 2, propertyId: 'prop-0' }, 'k2'))
    ),
    start('A'), idle('A'), stop('A')
  ]
})

// =========================================================================
// Event time
// =========================================================================

const etWindow = (opts) => ({
  source: { queues: ['c-readings'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60, eventTime: g('0.data.at'), ...opts } },
    { op: 'aggregate', fields: { count: 1, sum: g('0.v') } },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowKey'), value: g('0') } },
    { op: 'to', target: 'c-et-out' }
  ]
})
const reading = (sec, eventSec, v, id) => tx(sec, push('c-readings', 'r', { at: at(eventSec), v }, id))

add_({
  name: 'et-window-lateness',
  about: 'event-time windows close on the watermark (latest event time - allowedLateness); an event older than the watermark is late and dropped',
  mode: 'both',
  ...etWindow({ allowedLateness: '30s' }),
  compareMetrics: ['emitted', 'outputs', 'late'],
  script: [
    txs(
      reading(1, 10, 1, 'e1'),
      reading(2, 80, 2, 'e2'),
      reading(3, 40, 4, 'e3'),
      reading(4, 55, 8, 'e4'),
      reading(5, 100, 16, 'e5'),
      reading(6, 200, 32, 'e6')
    ),
    start('A'), idle('A'), stop('A')
  ]
})

add_({
  name: 'et-window-include',
  about: "onLate 'include' folds a late event in: into a window still open, or a closed window opened again and sent again",
  ...etWindow({ allowedLateness: 10, onLate: 'include' }),
  compareMetrics: ['emitted', 'outputs', 'late'],
  script: [
    txs(
      reading(1, 10, 1, 'e1'),
      reading(2, 75, 2, 'e2'),
      reading(3, 30, 4, 'e3'),
      reading(4, 140, 8, 'e4'),
      reading(5, 20, 16, 'e5'),
      reading(6, 300, 32, 'e6')
    ),
    start('A'), idle('A'), stop('A')
  ]
})

add_({
  name: 'et-pattern-order',
  about: 'a pattern on event time takes its messages in event-time order: a then that arrives before its start, within allowedLateness, still matches; held messages are state, carried by the snapshot to the next process',
  mode: 'both',
  source: { queues: ['c-a', 'c-b'], from: 'earliest' },
  chain: [
    {
      op: 'pattern',
      opts: {
        start: isQueue('c-a'),
        then: isQueue('c-b'),
        within: '5m',
        eventTime: g('0.data.at'),
        allowedLateness: '1m',
        onMatch: { key: g('0.partition'), a: g('0.data.n'), b: g('1.data.n') },
        onTimeout: { key: g('0.partition'), a: g('0.data.n'), timeout: true }
      }
    },
    { op: 'to', target: 'c-et-pattern-out' }
  ],
  script: [
    txs(
      tx(1, push('c-b', 'e1', { at: at(30), n: 'b1' }, 'b1')),
      tx(2, push('c-a', 'e1', { at: at(10), n: 'a1' }, 'a1')),
      tx(3, push('c-a', 'e2', { at: at(40), n: 'a2' }, 'a2'))
    ),
    start('A'), idle('A'), stop('A'),
    txs(
      tx(4, push('c-a', 'e3', { at: at(100), n: 'a3' }, 'a3')),
      tx(5, push('c-b', 'e3', { at: at(200), n: 'b3' }, 'b3')),
      tx(6, push('c-b', 'zz', { at: at(900), n: 'flush' }, 'bz'))
    ),
    start('B'), idle('B'), stop('B')
  ]
})

// Moves between processes (and, with --cross, between SDKs): one stops or dies with open windows, the next goes on.
const etTakeover = (name, about, end) => add_({
  name,
  about,
  mode: 'both',
  ...etWindow({}),
  compareMetrics: [],
  script: [
    txs(reading(1, 10, 1, 'e1'), reading(2, 20, 2, 'e2'), reading(3, 70, 4, 'e3')),
    start('A'), idle('A'), end,
    txs(reading(4, 80, 8, 'e4'), reading(5, 130, 16, 'e5'), reading(6, 200, 32, 'e6'), reading(7, 400, 64, 'e7')),
    start('B'), idle('B', { timeoutMs: 30000 }), stop('B')
  ]
})
etTakeover('et-takeover-stop', 'event-time windows across a stop: the next process (another SDK with --cross) loads the snapshot the first wrote and closes every window once', stop('A'))
etTakeover('et-takeover-kill', 'event-time windows across a SIGKILL: the next process (another SDK with --cross) takes the lock, replays from the last snapshot and closes every window once', kill('A'))

add_({
  name: 'et-idle',
  about: 'idle: after a quiet minute on the feed clock the watermark moves on with it, so the last window closes with no new message',
  ...etWindow({ idle: '1m' }),
  script: [
    txs(reading(1, 5, 1, 'e1'), reading(2, 20, 2, 'e2')),
    start('A'), idle('A'),
    metrics('A', 'before-idle', ['outputs']),
    clock(2 + 60 + 41), idle('A'),
    stop('A')
  ]
})

add_({
  name: 'et-chain-two-windows',
  about: 'a window after an event-time window runs on event time too (the results carry their window end) and follows its watermark',
  source: { queues: ['c-readings'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60, eventTime: g('0.data.at') } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'windowTumbling', opts: { seconds: 180 } },
    { op: 'aggregate', fields: { max: g('0.count'), sum: g('0.count') } },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowKey'), value: g('0') } },
    { op: 'to', target: 'c-et-chain-out' }
  ],
  script: [
    txs(
      reading(1, 10, 1, 'e1'), reading(2, 20, 1, 'e2'), reading(3, 70, 1, 'e3'),
      reading(4, 130, 1, 'e4'), reading(5, 135, 1, 'e5'), reading(6, 136, 1, 'e6'),
      reading(7, 200, 1, 'e7'), reading(8, 400, 1, 'e8')
    ),
    start('A'), idle('A'), stop('A')
  ]
})

// =========================================================================
// Several stateful steps
// =========================================================================

add_({
  name: 'two-windows-feed',
  about: 'a window of window results on the feed clock: each result reaches the next step at the time its window closed',
  source: { queues: ['c-hits'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'windowTumbling', opts: { seconds: 300 } },
    { op: 'aggregate', fields: { max: g('0.count'), sum: g('0.count') } },
    { op: 'map', fn: { key: g('1.key'), start: g('1.windowKey'), value: g('0') } },
    { op: 'to', target: 'c-five-min' }
  ],
  script: [
    txs(tx(10, push('c-hits', 'h', {}, 'h1')), tx(20, push('c-hits', 'h', {}, 'h2')), tx(70, push('c-hits', 'h', {}, 'h3')), tx(250, push('c-hits', 'h', {}, 'h4'))),
    start('A'), idle('A'),
    clock(301), idle('A'), stop('A')
  ]
})

add_({
  name: 'window-then-pattern',
  about: 'a pattern over window results: two busy minutes in a row for one key',
  source: { queues: ['c-hits'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'filter', fn: { $gte: [g('0.count'), 2] } },
    {
      op: 'pattern',
      opts: {
        start: TRUE,
        then: TRUE,
        within: '90s',
        onMatch: { key: g('0.partition'), first: g('0.data.count'), second: g('1.data.count'), firstCtx: g('0.ctx.windowKey') }
      }
    },
    { op: 'to', target: 'c-busy' }
  ],
  script: [
    txs(
      tx(1, push('c-hits', 'h', {}, 'h1')), tx(2, push('c-hits', 'h', {}, 'h2')),
      tx(61, push('c-hits', 'h', {}, 'h3')), tx(62, push('c-hits', 'h', {}, 'h4')), tx(63, push('c-hits', 'h', {}, 'h5')),
      tx(121, push('c-hits', 'k', {}, 'k1'))
    ),
    start('A'), idle('A'),
    clock(181), idle('A'), stop('A')
  ]
})

// =========================================================================
// A message that keeps failing
// =========================================================================

const failing = iff(g('0.data.bad'), thr('bad message'), { n: g('0.data.n') })

add_({
  name: 'poison-stop',
  about: "onPoison 'stop' (the default): what came before the failing message commits, the stream stops there; a new process stops there again",
  source: { queues: ['c-in'], from: 'earliest' },
  chain: [{ op: 'map', fn: failing }, { op: 'to', target: 'c-out' }],
  run: { retries: 1 },
  script: [
    txs(
      tx(1, push('c-in', 'p', { n: 1 }, 'i1')),
      tx(2, push('c-in', 'p', { n: 2 }, 'i2')),
      tx(3, push('c-in', 'p', { bad: true }, 'i3')),
      tx(4, push('c-in', 'p', { n: 4 }, 'i4'))
    ),
    start('A'), waitDone('A'),
    start('B'), waitDone('B')
  ]
})

add_({
  name: 'poison-skip',
  about: "onPoison 'skip' leaves the message out; a stateful stream remembers the skip, so a replay leaves out the same one",
  source: { queues: ['c-in'], from: 'earliest' },
  chain: [
    { op: 'map', fn: failing },
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1, sum: g('0.n') } },
    { op: 'map', fn: { key: g('1.key'), value: g('0') } },
    { op: 'to', target: 'c-out' }
  ],
  run: { onPoison: 'skip', retries: 0 },
  compareMetrics: [],
  script: [
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1')), tx(2, push('c-in', 'p', { bad: true }, 'i2')), tx(3, push('c-in', 'p', { n: 3 }, 'i3'))),
    start('A'), idle('A'), metrics('A', 'a', ['skipped', 'poisoned']), kill('A'),
    start('B'), idle('B', { timeoutMs: 30000 }), metrics('B', 'b', ['replaySkipped']),
    clock(61), idle('B'), stop('B')
  ]
})

add_({
  name: 'poison-dlq',
  about: 'a dead-letter queue gets the failing message with the error and the attempts, in the commit of the others',
  source: { queues: ['c-in'], from: 'earliest' },
  chain: [{ op: 'map', fn: failing }, { op: 'to', target: 'c-out' }],
  run: { onPoison: 'c-dlq', retries: 2 },
  script: [
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1')), tx(2, push('c-in', 'q', { bad: true, why: 'x' }, 'i2')), tx(3, push('c-in', 'p', { n: 3 }, 'i3'))),
    start('A'), idle('A'), stop('A')
  ]
})

add_({
  name: 'poison-flaky',
  about: 'a function that fails twice and then passes is retried, not poison',
  source: { queues: ['c-in'], from: 'earliest' },
  chain: [{ op: 'map', fn: { $flaky: { times: 2, key: g('0.transactionId'), then: { n: g('0.data.n') } } } }, { op: 'to', target: 'c-out' }],
  run: { retries: 3 },
  compareMetrics: ['outputs', 'poisoned', 'retries'],
  script: [
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1')), tx(2, push('c-in', 'p', { n: 2 }, 'i2'))),
    start('A'), idle('A'), stop('A')
  ]
})

add_({
  name: 'poison-result-dlq',
  about: 'a window result whose next function keeps failing goes to the dead-letter queue as a result, and the stream goes on',
  source: { queues: ['c-in'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'map', fn: iff(eq(g('0.count'), 2), thr('two is bad'), { key: g('1.key'), count: g('0.count') }) },
    { op: 'to', target: 'c-out' }
  ],
  run: { onPoison: 'c-dlq', retries: 0 },
  script: [
    txs(tx(1, push('c-in', 'a', {}, 'i1')), tx(2, push('c-in', 'b', {}, 'i2')), tx(3, push('c-in', 'b', {}, 'i3'))),
    start('A'), idle('A'),
    clock(61), idle('A'), stop('A')
  ]
})

// =========================================================================
// One runner per queryId; reset; layout; shards; lost answers
// =========================================================================

const copy = { source: { queues: ['c-in'], from: 'earliest' }, chain: [{ op: 'map', fn: { n: g('0.data.n') } }, { op: 'to', target: 'c-out' }] }

add_({
  name: 'standby-stop-takeover',
  about: 'a second process on the queryId waits; when the first stops, it takes over from the stored position; every message goes out once',
  ...copy,
  compareMetrics: [],
  script: [
    start('A'), start('B'),
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1')), tx(2, push('c-in', 'p', { n: 2 }, 'i2'))),
    idle('A'), stop('A'),
    waitRole('B', 'active'),
    txs(tx(3, push('c-in', 'p', { n: 3 }, 'i3'))),
    idle('B'), stop('B')
  ]
})

add_({
  name: 'standby-kill-takeover',
  about: 'the active process dies; the standby takes over once the lock expires, from the stored position',
  ...copy,
  compareMetrics: [],
  script: [
    start('A'), start('B'),
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1'))),
    idle('A'), kill('A'),
    waitRole('B', 'active'),
    txs(tx(2, push('c-in', 'p', { n: 2 }, 'i2'))),
    idle('B'), stop('B')
  ]
})

add_({
  name: 'exclusive-false',
  about: 'exclusive: false runs two processes at once; the position version still decides which one commits: every output once',
  ...copy,
  run: { exclusive: false },
  compareMetrics: [],
  script: [
    start('A'), start('B'),
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1')), tx(2, push('c-in', 'q', { n: 2 }, 'i2')), tx(3, push('c-in', 'p', { n: 3 }, 'i3'))),
    idle('A'), stop('A'), stop('B')
  ]
})

add_({
  name: 'reset-epoch',
  about: 'reset: true starts over at every start: a new epoch, from the source start again, with outputs of their own',
  ...copy,
  script: [
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1'))),
    start('A'), idle('A'), stop('A'),
    start('B', { run: { reset: true } }), idle('B'), stop('B')
  ]
})

add_({
  name: 'reset-token',
  about: 'reset: "<token>" starts over once per token: the next start with the same token goes on',
  ...copy,
  script: [
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1'))),
    start('A'), idle('A'), stop('A'),
    start('B', { run: { reset: 'v2' } }), idle('B'), stop('B'),
    txs(tx(2, push('c-in', 'p', { n: 2 }, 'i2'))),
    start('C', { run: { reset: 'v2' } }), idle('C'), stop('C')
  ]
})

const perMinute = (seconds) => [
  { op: 'windowTumbling', opts: { seconds } },
  { op: 'aggregate', fields: { count: 1 } },
  { op: 'to', target: 'c-out' }
]

add_({
  name: 'chain-changed',
  about: 'a chain whose stateful steps changed is refused while the stored state was kept by the old chain; reset starts it over',
  source: { queues: ['c-in'], from: 'earliest' },
  chain: perMinute(60),
  script: [
    txs(tx(1, push('c-in', 'p', {}, 'i1'))),
    start('A'), idle('A'), stop('A'),
    start('B', { chain: perMinute(120), mayFail: true }),
    start('C', { chain: perMinute(120), run: { reset: true } }), idle('C'),
    clock(121), idle('C'), stop('C')
  ]
})

const sharded = { source: { queues: ['c-in'], from: 'earliest' }, chain: [{ op: 'map', fn: { n: g('0.data.n'), p: g('0.partition') } }, { op: 'to', target: 'c-out' }] }
const sixPartitions = txs(...['p0', 'p1', 'p2', 'p3', 'p4', 'p5'].map((p, i) => tx(i + 1, push('c-in', p, { n: i }, `i${i}`))))

add_({
  name: 'shards-group',
  about: 'run({ shards: 2 }): two shards in one process, each its partitions (FNV-1a), its own position record, one layout record',
  ...sharded,
  compareMetrics: [],
  script: [
    sixPartitions,
    start('A', { run: { shards: 2 } }),
    idle('A', { keys: ['$q#shard0of2', '$q#shard1of2'] }),
    stop('A')
  ]
})

add_({
  name: 'shards-processes',
  about: 'shard { index, count } in two processes: the same split as one process with shards',
  ...sharded,
  compareMetrics: [],
  script: [
    sixPartitions,
    start('A', { run: { shard: { index: 0, count: 2 } } }),
    start('B', { run: { shard: { index: 1, count: 2 } } }),
    idle('A', { keys: ['$q#shard0of2', '$q#shard1of2'] }),
    stop('A'), stop('B')
  ]
})

add_({
  name: 'shards-shardby',
  about: 'shardBy: a function of the message picks the shard (filtered in the process); a window keyed by the same field',
  source: { queues: ['c-in'], from: 'earliest' },
  chain: [
    { op: 'keyBy', fn: g('0.data.user') },
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'map', fn: { user: g('1.key'), count: g('0.count') } },
    { op: 'to', target: 'c-out' }
  ],
  run: { shardBy: g('0.data.user') },
  compareMetrics: [],
  script: [
    txs(...['ann', 'bob', 'cy', 'dee', 'ann', 'bob'].map((u, i) => tx(i + 1, push('c-in', `p${i % 2}`, { user: u }, `i${i}`)))),
    start('A', { run: { shards: 2 } }),
    idle('A', { keys: ['$q#shard0of2', '$q#shard1of2'] }),
    clock(61),
    idle('A', { keys: ['$q#shard0of2', '$q#shard1of2'] }),
    stop('A')
  ]
})

add_({
  name: 'shard-layout-refused',
  about: 'a query that ran whole is refused in shards (keys would move with their state) unless reset',
  ...sharded,
  compareMetrics: [],
  script: [
    txs(tx(1, push('c-in', 'p0', { n: 1 }, 'i1'))),
    start('A'), idle('A'), stop('A'),
    start('B', { run: { shards: 2 }, mayFail: true }),
    start('C', { run: { shards: 2, reset: true } }),
    idle('C', { keys: ['$q#shard0of2', '$q#shard1of2'] }), stop('C')
  ]
})

add_({
  name: 'lost-answer',
  about: 'a commit that applied but whose answer was lost is not written twice',
  ...copy,
  compareMetrics: [],
  script: [
    start('A'),
    { inject: { route: '/api/v1/transaction', when: 'after', status: 503, times: 1 } },
    txs(tx(1, push('c-in', 'p', { n: 1 }, 'i1')), tx(2, push('c-in', 'p', { n: 2 }, 'i2'))),
    idle('A', { timeoutMs: 30000 }),
    txs(tx(3, push('c-in', 'p', { n: 3 }, 'i3'))),
    idle('A'), stop('A')
  ]
})

add_({
  name: 'lost-answer-window',
  about: 'a stateful commit whose answer was lost: the window still counts each message once',
  source: { queues: ['c-in'], from: 'earliest' },
  chain: perMinute(60),
  compareMetrics: [],
  script: [
    start('A'),
    { inject: { route: '/api/v1/transaction', when: 'after', status: 503, times: 1 } },
    txs(tx(1, push('c-in', 'p', {}, 'i1')), tx(2, push('c-in', 'p', {}, 'i2'))),
    idle('A', { timeoutMs: 30000 }),
    clock(61), idle('A'), stop('A')
  ]
})

// =========================================================================
// Live state
// =========================================================================

add_({
  name: 'view-live',
  about: 'Stream.view asks the running process over ephemeral queues: get, list, status',
  source: { queues: ['c-visits'], from: 'earliest' },
  chain: [
    { op: 'keyBy', fn: g('0.data.user') },
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'name', name: 'perUser' },
    { op: 'to', target: 'c-out' }
  ],
  script: [
    txs(tx(1, push('c-visits', 'p', { user: 'ann' }, 'v1')), tx(2, push('c-visits', 'p', { user: 'ann' }, 'v2')), tx(3, push('c-visits', 'q', { user: 'bob' }, 'v3'))),
    start('A'), idle('A'),
    view('get', { step: 'perUser', key: 'ann' }, 'view-ann'),
    view('get', { step: 'perUser', key: 'nobody' }, 'view-nobody'),
    view('list', { step: 'perUser', limit: 10 }, 'view-list'),
    view('get', { step: 'noSuchStep', key: 'ann' }, 'view-bad-step'),
    view('status', {}, 'view-status'),
    stop('A')
  ]
})

// =========================================================================
// foreachBatch: the results in batches, the position after the call
// =========================================================================

const or = (...e) => ({ $or: e })
const gt = (a, b) => ({ $gt: [a, b] })
const len = (e) => ({ $len: e })
const pause = (ms) => ({ sleep: ms })
// A batch function that fails for every batch of more than one item, and for item n alone:
// tried whole, then item by item, the poison policy meets item n only.
const failsOn = (n) => iff(or(gt(len(g('0')), 1), eq(g('0.0.value.n'), n)), thr('bad row'), null)
const BATCH_METRICS = ['emitted', 'effects', 'batches', 'outputs', 'retries', 'poisoned', 'skipped', 'deadLettered', 'pendingBatch']
const numbers = { source: { queues: ['c-in'], from: 'earliest' } }
const toN = { op: 'map', fn: { n: g('0.data.n') } }
// Transactions for numbers from..to, one second apart from `sec`, in two partitions.
const ins = (from, to, sec = from) => Array.from({ length: to - from + 1 }, (_, i) =>
  tx(sec + i, push('c-in', `p${(from + i) % 2}`, { n: from + i }, `i${from + i}`)))

add_({
  name: 'batch-per-read',
  about: 'foreachBatch: the results of one read in one call; each item has the value, the ctx foreach gets and the id its output would have; the position commits after the call',
  mode: 'both',
  ...numbers,
  chain: [toN, { op: 'foreachBatch' }],
  // Live, a follower that lags can split the first read: the calls compare on the fake broker only.
  compareMetrics: ['emitted', 'effects', 'outputs', 'pendingBatch'],
  script: [txs(...ins(1, 4)), start('A'), idle('A'), stop('A')]
})

add_({
  name: 'batch-max-items',
  about: 'foreachBatch maxItems: a read of 7 goes in calls of 3, 3 and 1, each followed by its commit',
  ...numbers,
  chain: [toN, { op: 'foreachBatch', opts: { maxItems: 3 } }],
  compareMetrics: BATCH_METRICS,
  script: [txs(...ins(1, 7)), start('A'), idle('A'), metrics('A', 'a', ['batches', 'effects', 'commits']), stop('A')]
})

add_({
  name: 'batch-flatmap-split',
  about: 'foreachBatch maxItems splits what one message makes: calls of at most maxItems, one commit after the last',
  ...numbers,
  chain: [
    { op: 'flatMap', fn: [{ n: g('0.data.n'), i: 0 }, { n: g('0.data.n'), i: 1 }, { n: g('0.data.n'), i: 2 }] },
    { op: 'foreachBatch', opts: { maxItems: 2 } }
  ],
  compareMetrics: BATCH_METRICS,
  script: [txs(...ins(1, 1)), start('A'), idle('A'), metrics('A', 'a', ['batches', 'effects', 'commits']), stop('A')]
})

add_({
  name: 'batch-linger',
  about: 'foreachBatch maxWaitMillis: the items of two reads in one call; nothing commits while they wait',
  ...numbers,
  chain: [toN, { op: 'foreachBatch', opts: { maxItems: 10, maxWaitMillis: 1500 } }],
  compareMetrics: BATCH_METRICS,
  script: [
    txs(...ins(1, 2)), start('A'), pause(300), metrics('A', 'waiting', ['pendingBatch', 'batches', 'commits']),
    txs(...ins(3, 4, 10)), pause(300), metrics('A', 'still', ['pendingBatch', 'batches', 'commits']),
    idle('A'), stop('A')
  ]
})

add_({
  name: 'batch-retry',
  about: 'foreachBatch: a batch that fails twice, then passes, is tried again whole, with the same items and ids: not poison',
  ...numbers,
  chain: [toN, { op: 'foreachBatch', fn: { $flaky: { times: 2, key: 'batch', then: null } } }],
  run: { retries: 3 },
  compareMetrics: BATCH_METRICS,
  script: [txs(...ins(1, 3)), start('A'), idle('A'), stop('A')]
})

add_({
  name: 'batch-poison-skip',
  about: "foreachBatch onPoison 'skip': tried whole, then item by item; the item that fails is left out, the others are delivered",
  ...numbers,
  chain: [toN, { op: 'foreachBatch', fn: failsOn(3) }],
  run: { onPoison: 'skip', retries: 1 },
  compareMetrics: BATCH_METRICS,
  script: [txs(...ins(1, 4)), start('A'), idle('A'), stop('A')]
})

add_({
  name: 'batch-poison-dlq',
  about: 'foreachBatch with a dead-letter queue: the item that fails goes there (value, ctx, id, error), in the commit past the batch',
  ...numbers,
  chain: [toN, { op: 'foreachBatch', fn: failsOn(2) }],
  run: { onPoison: 'c-dlq', retries: 0 },
  compareMetrics: BATCH_METRICS,
  script: [txs(...ins(1, 3)), start('A'), idle('A'), stop('A')]
})

add_({
  name: 'batch-poison-stop',
  about: "foreachBatch onPoison 'stop': nothing of the batch commits and the record names the item; a new process gets the whole batch again and stops there again",
  ...numbers,
  chain: [toN, { op: 'foreachBatch', fn: failsOn(3), opts: { maxItems: 2 } }],
  run: { retries: 1 },
  compareMetrics: BATCH_METRICS,
  script: [txs(...ins(1, 4)), start('A'), waitDone('A'), start('B'), waitDone('B')]
})

add_({
  name: 'batch-message-stop',
  about: 'foreachBatch: a message that stops the stream comes after the batch that waits: the batch goes first, the commit stops at the message',
  ...numbers,
  chain: [{ op: 'map', fn: iff(eq(g('0.data.n'), 3), thr('bad message'), { n: g('0.data.n') }) }, { op: 'foreachBatch' }],
  run: { retries: 0 },
  compareMetrics: BATCH_METRICS,
  script: [txs(...ins(1, 4)), start('A'), waitDone('A')]
})

add_({
  name: 'batch-window',
  about: 'foreachBatch after a window: the results of one close in one call, with ids of their own; the commit after the call moves the clock',
  source: { queues: ['c-visits'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1, sum: g('0.n') } },
    { op: 'foreachBatch' }
  ],
  compareMetrics: BATCH_METRICS,
  script: [
    txs(tx(5, push('c-visits', 'v1', { n: 1 }, 'v1')), tx(10, push('c-visits', 'v2', { n: 2 }, 'v2')), tx(20, push('c-visits', 'v1', { n: 4 }, 'v3'))),
    start('A'), idle('A'),
    clock(61), idle('A'),
    txs(tx(70, push('c-visits', 'v1', { n: 8 }, 'v4'))),
    clock(121), idle('A'), stop('A')
  ]
})

add_({
  name: 'batch-restart-kill',
  about: 'foreachBatch: a process killed with a batch that waits; the next one delivers it, with the same ids, at its stop',
  ...numbers,
  chain: [toN, { op: 'foreachBatch', opts: { maxItems: 2, maxWaitMillis: 600000 } }],
  run: { lockTtl: '1s' },
  compareMetrics: BATCH_METRICS,
  script: [
    txs(...ins(1, 2)), start('A'), idle('A'),
    txs(...ins(3, 3, 10)), pause(300), metrics('A', 'a-waiting', ['pendingBatch', 'batches']),
    kill('A'),
    start('B'), waitRole('B', 'active'), pause(300), metrics('B', 'b-waiting', ['pendingBatch', 'batches']),
    stop('B')
  ]
})

add_({
  name: 'batch-window-restart',
  about: 'foreachBatch after a window, killed after a close it delivered with the next window open: the next process reads the feed again from the snapshot and delivers nothing of that close, then the next one',
  source: { queues: ['c-visits'], from: 'earliest' },
  chain: [
    { op: 'windowTumbling', opts: { seconds: 60 } },
    { op: 'aggregate', fields: { count: 1 } },
    { op: 'foreachBatch', opts: { maxItems: 1 } }
  ],
  run: { lockTtl: '1s', snapshotEveryMillis: 3600000 },
  compareMetrics: BATCH_METRICS,
  script: [
    txs(tx(5, push('c-visits', 'v1', {}, 'v1')), tx(65, push('c-visits', 'v1', {}, 'v2'))),
    start('A'), idle('A'), kill('A'),
    start('B'), idle('B', { timeoutMs: 30000 }),
    metrics('B', 'b-replayed', ['replays']),
    clock(121), idle('B'), stop('B')
  ]
})

// ---- write ----
const dir = join(HERE, 'cases')
mkdirSync(dir, { recursive: true })
for (const f of readdirSync(dir)) if (f.endsWith('.json')) unlinkSync(join(dir, f))
for (const c of cases) writeFileSync(join(dir, `${c.name}.json`), `${JSON.stringify(c, null, 2)}\n`)
console.log(`${cases.length} cases written`)
