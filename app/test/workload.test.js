import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import {
  deeperFindings, efficiency, enrichRows, findings, flowSeries, heatCells, lagBudget,
  rollupFromQueueOps, sameHourBaseline, severity, weeklyProfile,
} from '../src/composables/useWorkload.js'

const fixture = (name) =>
  JSON.parse(readFileSync(fileURLToPath(new URL(`./fixtures/${name}`, import.meta.url)), 'utf8'))

const NS = fixture('workload.namespace.1h.json')
const Q = fixture('workload.queue.smartchat.1h.json')
const CG = fixture('consumer_groups.json')
const QUEUES = fixture('queues.json')
const WEEK = fixture('workload.namespace.7d.json')
const Q24 = fixture('workload.queue.24h.json')
const OPS = fixture('queue_ops.small.json')

const rowsOf = (payload) => (Array.isArray(payload) ? payload : payload.rows || payload.data || payload)

const groupsByQueue = () => {
  const m = new Map()
  for (const g of rowsOf(CG)) {
    if (!m.has(g.queueName)) m.set(g.queueName, [])
    m.get(g.queueName).push(g)
  }
  return m
}

const queueMeta = () => {
  const m = new Map()
  for (const q of rowsOf(QUEUES)) m.set(q.name, { namespace: q.namespace, task: q.task })
  return m
}

test('share is against the tenant and sums to about 1 over the namespace rows', () => {
  const rows = enrichRows(NS, groupsByQueue(), queueMeta(), 'namespace')
  assert.ok(rows.length > 1, 'fixture should carry several namespaces')
  const total = rows.reduce((s, r) => s + r.share, 0)
  // The rows partition the tenant's queues, so the shares partition its pops.
  assert.ok(Math.abs(total - 1) < 0.01, `shares summed to ${total}`)
  const pops = rows.reduce((s, r) => s + r.window.popMessages, 0)
  assert.equal(pops, NS.tenant.window.popMessages)
})

test('null handling: no pops means no lag, few acks means no ack ratio', () => {
  const rows = enrichRows(NS, groupsByQueue(), queueMeta(), 'namespace')
  for (const r of rows) {
    if (r.window.popMessages === 0) {
      assert.equal(r.window.avgLagMs, null, `${r.key} reported lag without pops`)
      assert.equal(r.window.maxLagMs, null, `${r.key} reported max lag without pops`)
    }
    if (r.acks < 5) assert.equal(r.ackOk, null, `${r.key} claimed an ack ratio from ${r.acks} acks`)
    if (r.window.popMessages + r.window.popEmpty < 5) assert.equal(r.fill, null)
    // A row whose queues have no consumer group knows nothing about waiting.
    if (r.groupsN === 0) assert.equal(r.oldest, null)
  }
})

test('the no-consumer-group rule fires for smartads, and is suspended without the groups list', () => {
  const rows = enrichRows(NS, groupsByQueue(), queueMeta(), 'namespace')
  const smartads = rows.find((r) => r.key === 'smartads')
  assert.ok(smartads, 'fixture should carry a smartads namespace')
  assert.ok(smartads.now.pendingWithoutGroup > 0)
  assert.equal(smartads.sev, 'bad')

  const f = findings(rows, 'namespace', 'last 1h')
  const hit = f.find((x) => x.key === 'smartads' && x.text.includes('no consumer group'))
  assert.ok(hit, 'expected a finding about pending messages with no consumer group')
  assert.equal(hit.sev, 'bad')
  assert.match(hit.text, /pending messages on queues with no consumer group$/)

  // Consumer groups unavailable: the rule cannot tell "no group" from
  // "we could not read the groups", so it must not fire.
  const blind = findings(rows, 'namespace', 'last 1h', { hasGroups: false })
  assert.equal(blind.filter((x) => x.text.includes('no consumer group')).length, 0)
})

test('severity precedence: the worst rule wins, quiet rows are not alarming', () => {
  const base = { window: { pushMessages: 0, popMessages: 0, popEmpty: 0, ackFailed: 0, parkedAvg: 0, maxLagMs: null }, now: { pendingWithoutGroup: 0, deadLetter: 0 }, acks: 0, ackOk: null, oldest: null }
  const w = (o) => ({ ...base, ...o, window: { ...base.window, ...(o.window || {}) }, now: { ...base.now, ...(o.now || {}) } })

  assert.equal(severity(w({})), 'mute')
  assert.equal(severity(w({ window: { popEmpty: 40 } })), 'ice')
  assert.equal(severity(w({ window: { popMessages: 10 } })), 'ok')
  assert.equal(severity(w({ now: { deadLetter: 1000 } })), 'warn')
  assert.equal(severity(w({ oldest: 61 })), 'warn')
  assert.equal(severity(w({ oldest: 300 })), 'bad')
  // A pending backlog nobody can consume outranks everything else, including
  // a row that would otherwise read as healthy.
  assert.equal(severity(w({ window: { popMessages: 1e6 }, now: { pendingWithoutGroup: 1 } })), 'bad')
  // 19 acks is still not enough to call a failure rate.
  assert.equal(severity(w({ acks: 19, ackOk: 0.1, window: { popMessages: 5 } })), 'ok')
  assert.equal(severity(w({ acks: 20, ackOk: 0.6 })), 'warn')
  assert.equal(severity(w({ acks: 20, ackOk: 0.4 })), 'bad')
})

test('findings come back worst first', () => {
  const rows = enrichRows(NS, groupsByQueue(), queueMeta(), 'namespace')
  const f = findings(rows, 'namespace', 'last 1h')
  assert.ok(f.length > 0)
  const rank = { bad: 3, warn: 2, ok: 1, ice: 0.5, mute: 0 }
  for (let i = 1; i < f.length; i++) {
    assert.ok(rank[f[i - 1].sev] >= rank[f[i].sev], `finding ${i} outranks the one before it`)
  }
  assert.equal(f[0].sev, 'bad')
})

test('flowSeries: at most five series, Other last, gaps preserved', () => {
  const rows = enrichRows(Q, groupsByQueue(), queueMeta(), 'queue')
  assert.ok(rows.length > 5, 'the queue fixture should have more than five rows')
  const s = flowSeries(rows, Q.buckets, 'pop')
  assert.equal(s.length, 5)
  assert.match(s[4].label, /^Other \(\d+\)$/)
  assert.equal(s[4].key, null)
  for (const one of s) assert.equal(one.data.length, Q.buckets.length)
  // A bucket where every folded row is null must stay null, not become 0.
  const rest = [...rows].sort((a, b) => b.window.popMessages - a.window.popMessages).slice(4)
  for (let i = 0; i < Q.buckets.length; i++) {
    if (rest.every((r) => r.series.pop[i] === null)) assert.equal(s[4].data[i], null)
  }
  // The top four are the four biggest by deliveries, in order.
  const top = [...rows].sort((a, b) => b.window.popMessages - a.window.popMessages).slice(0, 4)
  assert.deepEqual(s.slice(0, 4).map((x) => x.label), top.map((r) => r.name))
})

test('heatCells: sqrt scale, 100% on the busiest cell, null stays null', () => {
  const rows = enrichRows(NS, groupsByQueue(), queueMeta(), 'namespace')
  const heat = heatCells(rows)
  assert.equal(heat.rows.length, rows.length)
  let sawMax = false
  for (const r of heat.rows) {
    assert.equal(r.values.length, NS.buckets.length)
    for (let i = 0; i < r.values.length; i++) {
      const c = r.values[i]
      if (c.value === null) { assert.equal(c.percent, null); continue }
      assert.ok(c.percent >= 0 && c.percent <= 100)
      if (c.value === heat.max && heat.max > 0) { assert.equal(c.percent, 100); sawMax = true }
      // sqrt, not linear: a cell at a quarter of the max reads at half ink.
      assert.equal(c.percent, heat.max ? Math.round(Math.sqrt(c.value / heat.max) * 100) : 0)
    }
  }
  assert.ok(sawMax, 'the busiest cell should be in the grid')
})

test('lagBudget counts only buckets that actually delivered', () => {
  const rows = enrichRows(Q, groupsByQueue(), queueMeta(), 'queue')
  const r = rows.find((x) => x.series.maxLagMs.some((v) => v !== null))
  const b = lagBudget(r)
  assert.equal(b.buckets, r.series.maxLagMs.filter((v) => v !== null).length)
  assert.equal(b.under10s + b.from10to60 + b.over60s, b.buckets)
})

// ---------------------------------------------------------------------------
// The deeper layer
// ---------------------------------------------------------------------------

test('weeklyProfile: 7x24 UTC cells, no bucket stays null, totals match the series', () => {
  const w = weeklyProfile(WEEK)
  assert.equal(w.tenant.length, 7)
  for (const day of w.tenant) assert.equal(day.length, 24)

  // Every non-null tenant bucket lands in exactly one cell, so the grid sums
  // to the window's deliveries.
  const gridTotal = w.tenant.flat().reduce((a, b) => a + (b || 0), 0)
  const seriesTotal = WEEK.tenant.series.pop.reduce((a, b) => a + (b || 0), 0)
  assert.equal(gridTotal, seriesTotal)

  // Cell placement is UTC and additive.
  const expect = new Map()
  WEEK.buckets.forEach((b, i) => {
    const v = WEEK.tenant.series.pop[i]
    if (v === null) return
    const d = new Date(b)
    const k = `${d.getUTCDay()}:${d.getUTCHours()}`
    expect.set(k, (expect.get(k) || 0) + v)
  })
  for (let d = 0; d < 7; d++) {
    for (let h = 0; h < 24; h++) {
      assert.equal(w.tenant[d][h], expect.has(`${d}:${h}`) ? expect.get(`${d}:${h}`) : null,
        `cell ${d}:${h}`)
    }
  }

  // One matrix per row, and the per-row totals agree with the payload window.
  for (const r of WEEK.rows) {
    assert.ok(w.byKey.has(r.key), `no matrix for ${r.key}`)
    assert.equal(w.totals.get(r.key), r.series.pop.reduce((a, b) => a + (b || 0), 0))
  }
})

test('sameHourBaseline: the last COMPLETE bucket against the same hour on earlier days', () => {
  const b = sameHourBaseline(WEEK)
  // Never the last bucket — that one is still filling.
  assert.equal(b.index, WEEK.buckets.length - 2)
  assert.equal(b.hour, WEEK.buckets[b.index])
  assert.equal(b.rows[0].key, '*')
  assert.equal(b.rows.length, WEEK.rows.length + 1)

  const at = new Date(b.hour)
  const peers = []
  WEEK.buckets.forEach((x, i) => {
    if (i >= b.index) return
    const d = new Date(x)
    if (d.getUTCHours() === at.getUTCHours() && d.getUTCDate() !== at.getUTCDate()) peers.push(i)
  })
  assert.ok(peers.length >= 5, 'a 7d window should carry several earlier days')

  const tenant = b.rows[0]
  const vals = peers.map((i) => WEEK.tenant.series.pop[i]).filter((v) => v !== null)
  const mean = vals.reduce((a, x) => a + x, 0) / vals.length
  assert.equal(tenant.days, vals.length)
  assert.equal(tenant.current, WEEK.tenant.series.pop[b.index])
  assert.equal(tenant.mean, Math.round(mean))
  assert.equal(tenant.min, Math.min(...vals))
  assert.equal(tenant.max, Math.max(...vals))
  assert.equal(tenant.ratio, Math.round((tenant.current / mean) * 100) / 100)

  // No earlier day means no baseline — not a ratio of 1.
  const one = sameHourBaseline({ buckets: WEEK.buckets.slice(0, 2), rows: [], tenant: { series: { pop: [1, 2] } } })
  assert.equal(one.rows[0].mean, null)
  assert.equal(one.rows[0].ratio, null)
  assert.equal(one.rows[0].z, null)
  assert.equal(one.rows[0].days, 0)
})

test('efficiency: every ratio is null when its denominator is zero', () => {
  const eff = efficiency(Q24)
  assert.equal(eff.length, Q24.rows.length)
  for (let i = 1; i < eff.length; i++) assert.ok(eff[i - 1].pop >= eff[i].pop, 'sorted by deliveries')

  const byQueue = new Map(Q24.rows.map((r) => [r.key, r]))
  let sawTrxFed = false
  for (const e of eff) {
    const w = byQueue.get(e.queue).window
    if (w.pushMessages === 0) {
      assert.equal(e.fanout, null, `${e.queue} claimed a fan-out with no pushes`)
      if (w.transactions > 0 && w.popMessages > 0) sawTrxFed = true
    } else {
      assert.equal(e.fanout, Math.round((w.popMessages / w.pushMessages) * 100) / 100)
    }
    if (w.popMessages === 0) assert.equal(e.ackPerDelivery, null)
    if (w.pushRequests === 0) assert.equal(e.pushBatch, null)
    if (w.ackRequests === 0) assert.equal(e.ackBatch, null)
    assert.deepEqual(e.lagBudget, lagBudget(byQueue.get(e.queue)))
  }
  assert.ok(sawTrxFed, 'the 24h fixture should carry a transaction-fed queue')

  // Group counts come from the consumer-group list when it is up.
  const withGroups = efficiency(Q24, groupsByQueue())
  const one = withGroups.find((e) => e.groups > 0)
  assert.ok(one, 'at least one queue should have a consumer group')
})

test('deeperFindings: the two collector rules are consolidated into one line each', () => {
  const eff = efficiency(Q24)
  const f = deeperFindings({ efficiency: eff })
  const fed = f.filter((x) => x.text.includes('/transaction'))
  assert.equal(fed.length, 1, 'transaction-fed queues must be ONE finding')
  const n = eff.filter((e) => e.push === 0 && e.trx > 0 && e.pop >= 1000).length
  assert.ok(n > 1, 'the fixture should carry several transaction-fed queues')
  assert.match(fed[0].text, new RegExp(`^${n} queues are fed through /transaction`))

  const elsewhere = f.filter((x) => x.text.includes('acked by the counters'))
  assert.ok(elsewhere.length <= 1, 'acked-elsewhere must be at most ONE finding')

  const rank = { bad: 3, warn: 2, ok: 1, ice: 0.5, mute: 0 }
  for (let i = 1; i < f.length; i++) assert.ok(rank[f[i - 1].sev] >= rank[f[i].sev])
})

test('deeperFindings: a missing endpoint contributes no rule, not a clean bill', () => {
  // Nothing loaded at all: no findings, and in particular no "0 evicted".
  assert.deepEqual(deeperFindings({}), [])
  const f = deeperFindings({
    retention: [{ key: 'q.a', eviction: 12, retention: 3 }],
    dlq: [{ queue: 'q.b', rowsNow: 400, sample: 200, avgBytes: 250_000, signatures: [{ text: 'boom <id>', n: 180, share: 0.9 }] }],
    baseline: { rows: [{ key: '*', ratio: 9 }, { key: 'ns', ratio: 3.2, current: 320, mean: 100, min: 80, max: 120, days: 6 }] },
    partitions: [{ queue: 'q.a', partitions: 100, live24h: 10, created24h: 40 }],
  })
  assert.equal(f.filter((x) => x.text.includes('evicted')).length, 1)
  assert.equal(f.filter((x) => x.text.includes('mostly one error')).length, 1)
  assert.equal(f.filter((x) => x.text.startsWith('Dead-letter rows of up to')).length, 1)
  // The tenant row is not a namespace: it must not produce a "3× its usual" line.
  const ratio = f.filter((x) => x.text.includes('its usual deliveries'))
  assert.equal(ratio.length, 1)
  assert.match(ratio[0].text, /^ns: 3.2×/)
  assert.equal(f[f.length - 1].sev, 'mute')
  assert.match(f[f.length - 1].text, /^10 of 100 partitions were touched/)
})

// ---------------------------------------------------------------------------
// The client-side fallback: the same payload, rolled up from queue-ops
// ---------------------------------------------------------------------------

test('rollupFromQueueOps: known sums, per-queue parked average, pop-weighted lag', () => {
  const p = rollupFromQueueOps({ ...OPS, groupBy: 'namespace' })
  assert.equal(p.groupBy, 'namespace')
  assert.equal(p.bucketMinutes, 15)
  assert.equal(p.computedClientSide, true)
  assert.deepEqual(p.buckets, [
    '2026-09-09T08:00:00Z', '2026-09-09T08:15:00Z', '2026-09-09T08:30:00Z',
  ])

  const t = p.tenant
  assert.equal(t.queues, 3, 'gamma.q3 has no metrics row but is still a queue')
  assert.equal(t.window.pushMessages, 45)
  assert.equal(t.window.popMessages, 70)
  assert.equal(t.window.popEmpty, 56)
  assert.equal(t.window.ackSuccess, 68)
  assert.equal(t.window.ackFailed, 2)
  assert.equal(t.window.transactions, 8)
  assert.equal(t.window.partitionsCreated, 1)
  assert.equal(t.window.partitionsDeleted, 2)
  // pop-weighted: (100*20 + 200*10 + 50*40) / 70
  assert.equal(t.window.avgLagMs, Math.round(6000 / 70))
  assert.equal(t.window.maxLagMs, 60000)
  // parked is a gauge: AVG per queue ((4+6)/2 and (2+10)/2), summed.
  assert.equal(t.window.parkedAvg, 11)

  // `now` joins status/queues and resources/queues; the queue with no group
  // carries its pending into pendingWithoutGroup.
  assert.equal(t.now.pending, 107)
  assert.equal(t.now.processing, 5)
  assert.equal(t.now.deadLetter, 3)
  assert.equal(t.now.partitions, 7)
  assert.equal(t.now.retainedBytes, 7000)
  assert.equal(t.now.groups, 3)
  assert.equal(t.now.queuesWithoutGroup, 1)
  assert.equal(t.now.pendingWithoutGroup, 7)
  assert.equal(t.now.queuesTouched, 2)
  assert.equal(t.now.queuesActive, 2)

  const alpha = p.rows.find((r) => r.key === 'alpha')
  const beta = p.rows.find((r) => r.key === 'beta')
  const gamma = p.rows.find((r) => r.key === 'gamma')
  assert.equal(alpha.window.pushMessages, 40)
  assert.equal(alpha.window.popMessages, 30)
  assert.equal(alpha.window.avgLagMs, Math.round(4000 / 30))
  assert.equal(alpha.window.maxLagMs, 900)
  assert.equal(alpha.window.parkedAvg, 5)
  assert.equal(beta.window.parkedAvg, 6)
  assert.equal(beta.window.avgLagMs, 50)
  // A bucket with no row for the group is null, a row that reported zero is 0.
  assert.deepEqual(alpha.series.pop, [20, 10, null])
  assert.deepEqual(beta.series.pop, [0, null, 40])
  assert.deepEqual(beta.series.maxLagMs, [null, null, 60000])
  assert.deepEqual(alpha.series.parked, [4, 6, null])
  // The untouched queue is a row of its own with a window of zeros — and its
  // pending has nobody to consume it.
  assert.equal(gamma.window.popMessages, 0)
  assert.equal(gamma.now.queuesTouched, 0)
  assert.equal(gamma.now.pendingWithoutGroup, 7)
  // Rows partition the tenant.
  assert.equal(p.rows.reduce((s, r) => s + r.window.popMessages, 0), t.window.popMessages)
  assert.equal(p.rows.reduce((s, r) => s + r.queues, 0), t.queues)
})

test('rollupFromQueueOps: groupBy=queue carries namespace/task, filters narrow the rows only', () => {
  const p = rollupFromQueueOps({ ...OPS, groupBy: 'queue', namespace: 'alpha' })
  assert.equal(p.rows.length, 1)
  assert.equal(p.rows[0].key, 'alpha.q1')
  assert.equal(p.rows[0].namespace, 'alpha')
  assert.equal(p.rows[0].task, 'ingest')
  // The tenant total ignores the filters, so a share is still of the tenant.
  assert.equal(p.tenant.window.popMessages, 70)
  assert.equal(p.rows[0].window.popMessages, 30)

  // The rolled-up payload is the contract's shape: everything downstream works.
  const rows = enrichRows(p, groupsByQueue(), queueMeta(), 'queue')
  assert.equal(rows[0].share, 30 / 70)
  assert.equal(rows[0].acks, 30)
  assert.equal(rows[0].ackOk, 28 / 30)
  assert.equal(heatCells(rows).max, 20)
  assert.deepEqual(lagBudget(p.rows[0]), { buckets: 2, under10s: 2, from10to60: 0, over60s: 0 })
})

import { comparisonRange, totalSeries, windowDeltas } from '../src/composables/useWorkload.js'
import { CATEGORY_SLOTS, categorySlot, resetCategorySlots } from '../src/composables/useCategoryColors.js'

test('comparisonRange: previous = same length before; yesterday / lastWeek = same clock window', () => {
  const from = new Date('2026-09-09T09:00:00Z')
  const to = new Date('2026-09-09T10:00:00Z')
  const prev = comparisonRange({ from, to }, 'previous')
  assert.equal(prev.from.toISOString(), '2026-09-09T08:00:00.000Z')
  assert.equal(prev.to.toISOString(), '2026-09-09T09:00:00.000Z')
  const y = comparisonRange({ from, to }, 'yesterday')
  assert.equal(y.from.toISOString(), '2026-09-08T09:00:00.000Z')
  assert.equal(y.to.toISOString(), '2026-09-08T10:00:00.000Z')
  const w = comparisonRange({ from, to }, 'lastWeek')
  assert.equal(w.from.toISOString(), '2026-09-02T09:00:00.000Z')
})

test('windowDeltas: abs and pct per counter, pct null on a zero base, null without a window', () => {
  const d = windowDeltas(
    { pushMessages: 120, popMessages: 90, ackSuccess: 80, ackFailed: 4, popEmpty: 0 },
    { pushMessages: 100, popMessages: 100, ackSuccess: 80, ackFailed: 0, popEmpty: 50 },
  )
  assert.deepEqual(d.pushMessages, { abs: 20, pct: 0.2, prev: 100 })
  assert.deepEqual(d.popMessages, { abs: -10, pct: -0.1, prev: 100 })
  assert.deepEqual(d.ackFailed, { abs: 4, pct: null, prev: 0 })
  assert.deepEqual(d.popEmpty, { abs: -50, pct: -1, prev: 50 })
  assert.equal(windowDeltas(null, {}), null)
  assert.equal(windowDeltas({}, null), null)
})

test('totalSeries: sums per bucket and keeps a bucket null only when every row is null', () => {
  const rows = [
    { series: { pop: [1, null, 3] } },
    { series: { pop: [null, null, 4] } },
  ]
  assert.deepEqual(totalSeries(rows, [0, 1, 2], 'pop'), [1, null, 7])
  assert.deepEqual(totalSeries(rows, [0, 1, 2], 'push'), [null, null, null])
})

test('categorySlot: first sight assigns in order, per kind, grey past the palette, reset forgets', () => {
  resetCategorySlots('t')
  assert.equal(categorySlot('t', 'smartchat'), 0)
  assert.equal(categorySlot('t', 'channel'), 1)
  assert.equal(categorySlot('t', 'smartchat'), 0, 'a refresh keeps the colour')
  for (let i = 2; i < CATEGORY_SLOTS; i++) categorySlot('t', `k${i}`)
  assert.equal(categorySlot('t', 'sixth'), null)
  assert.equal(categorySlot('u', 'sixth'), 0, 'another kind starts over')
  resetCategorySlots('t')
  assert.equal(categorySlot('t', 'channel'), 0)
})

import { trimOpenBucket } from '../src/composables/useWorkload.js'

test('trimOpenBucket: drops the bucket that contains now, keeps a closed one, leaves nulls alone', () => {
  const p = {
    bucketMinutes: 1,
    buckets: ['2026-09-09T10:00:00Z', '2026-09-09T10:01:00Z', '2026-09-09T10:02:00Z'],
    rows: [{ key: 'a', series: { pop: [1, 2, 0], push: [null, 1, 0] } }],
    tenant: { series: { pop: [1, 2, 0] } },
  }
  const open = trimOpenBucket(p, new Date('2026-09-09T10:02:30Z').getTime())
  assert.deepEqual(open.buckets, p.buckets.slice(0, 2))
  assert.deepEqual(open.rows[0].series, { pop: [1, 2], push: [null, 1] })
  assert.deepEqual(open.tenant.series, { pop: [1, 2] })
  assert.equal(trimOpenBucket(p, new Date('2026-09-09T10:03:00Z').getTime()), p, 'closed: untouched')
  assert.equal(trimOpenBucket(null), null)
})
