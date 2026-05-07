/**
 * Tumbling windows — fixed-size, non-overlapping. Tests cover:
 *   - basic windowed sum + sink emit
 *   - per-partition state isolation (3 customers, no cross-bleed)
 *   - all aggregate fields (count, sum, min, max, avg)
 *   - .foreach(value, ctx) receives window/partition metadata
 *   - gracePeriod keeps a window open past its end
 *   - idleFlushMs closes ripe windows on quiet partitions
 */

import { Stream } from '../../../../streams/index.js'
import {
  STREAMS_URL, mkName, sleep, pushSpread, drainUntil, drainSink,
  expect, summarise
} from './_helpers.js'

// ----------------------------------------------------------------------------
// Basic — single partition, deterministic amounts, expect closed-window sums.
// ----------------------------------------------------------------------------

export async function tumblingBasicWindowSum(client) {
  const src = mkName('tumblingBasicWindowSum', 'src')
  const sink = mkName('tumblingBasicWindowSum', 'sink')
  const queryId = mkName('tumblingBasicWindowSum', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // 8 messages over ~8 seconds, 1 per second.
  const itemPushPromise = (async () => {
    for (let i = 0; i < 8; i++) {
      await client.queue(src).partition('one').push([{ data: { amount: i + 1 } }])
      await sleep(1100)
    }
  })()

  const handle = await Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 2, idleFlushMs: 800 })
    .aggregate({ count: () => 1, sum: m => m.amount })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  await itemPushPromise
  // Wait until the total count across all emits = 8 (idle flush must
  // close every window, including the trailing one).
  const emits = await drainUntil(client, sink, {
    until: out => out.reduce((a, m) => a + (m.data.count || 0), 0) >= 8,
    timeoutMs: 15000
  })
  await handle.stop()

  // Sum across all emits must equal 1+2+...+8 = 36.
  const totalSum = emits.reduce((a, m) => a + (m.data.sum || 0), 0)
  const totalCount = emits.reduce((a, m) => a + (m.data.count || 0), 0)
  return summarise('tumblingBasicWindowSum', [
    expect(emits.length, '>=', 3, 'at least 3 windows closed'),
    expect(totalSum, '===', 36, 'sum across all windows = 1..8'),
    expect(totalCount, '===', 8, 'count across all windows = 8'),
    expect(handle.metrics().errorsTotal, '===', 0, 'no runner errors')
  ])
}

// ----------------------------------------------------------------------------
// Per-partition isolation — 3 customers, deterministic values, verify no
// cross-partition contamination of aggregates.
// ----------------------------------------------------------------------------

export async function tumblingPerPartitionIsolation(client) {
  const src = mkName('tumblingPerPartitionIsolation', 'src')
  const sink = mkName('tumblingPerPartitionIsolation', 'sink')
  const queryId = mkName('tumblingPerPartitionIsolation', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // 3 customers × 6 messages each. Each customer has a unique amount range.
  const CUSTOMERS = ['cust-A', 'cust-B', 'cust-C']
  const itemPushPromise = (async () => {
    for (let i = 0; i < 6; i++) {
      for (let cIdx = 0; cIdx < CUSTOMERS.length; cIdx++) {
        const cust = CUSTOMERS[cIdx]
        // cust-A: amounts 100..105, cust-B: 200..205, cust-C: 300..305.
        const amount = (cIdx + 1) * 100 + i
        await client.queue(src).partition(cust).push([{ data: { customer: cust, amount } }])
      }
      await sleep(1200)
    }
  })()

  const handle = await Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 2, idleFlushMs: 1000 })
    .aggregate({ count: () => 1, sum: m => m.amount })
    .map((agg, ctx) => ({ partition: ctx.partition, ...agg }))
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, maxPartitions: 4, reset: true })

  await itemPushPromise
  // Wait until total count across all emits = 18 (3 customers × 6 events).
  const emits = await drainUntil(client, sink, {
    until: out => out.reduce((a, m) => a + (m.data.count || 0), 0) >= 18,
    timeoutMs: 18000
  })
  await handle.stop()

  // Every emit's payload should have a partition matching exactly one
  // customer. Sums are constrained to the customer's range.
  let crossBleed = 0
  let perCustSum = { 'cust-A': 0, 'cust-B': 0, 'cust-C': 0 }
  let perCustCount = { 'cust-A': 0, 'cust-B': 0, 'cust-C': 0 }

  for (const m of emits) {
    const p = m.data.partition
    if (!perCustSum.hasOwnProperty(p)) {
      crossBleed++
      continue
    }
    perCustSum[p] += m.data.sum
    perCustCount[p] += m.data.count
    // Defensive sanity: every amount in this customer's window must fall
    // in the customer's range — if it doesn't, two partitions' state was
    // mixed. We can only check totals though since the sink doesn't emit
    // individual amounts.
  }
  return summarise('tumblingPerPartitionIsolation', [
    expect(crossBleed, '===', 0, 'no emits with unknown partition (cross-bleed)'),
    expect(perCustCount['cust-A'], '===', 6, 'cust-A total count'),
    expect(perCustCount['cust-B'], '===', 6, 'cust-B total count'),
    expect(perCustCount['cust-C'], '===', 6, 'cust-C total count'),
    // cust-A: 100+101+102+103+104+105 = 615
    expect(perCustSum['cust-A'], '===', 615, 'cust-A total sum'),
    // cust-B: 200+201+202+203+204+205 = 1215
    expect(perCustSum['cust-B'], '===', 1215, 'cust-B total sum'),
    // cust-C: 300+301+302+303+304+305 = 1815
    expect(perCustSum['cust-C'], '===', 1815, 'cust-C total sum')
  ])
}

// ----------------------------------------------------------------------------
// All aggregate stats — count, sum, min, max, avg.
// ----------------------------------------------------------------------------

export async function tumblingAggregateAllStats(client) {
  const src = mkName('tumblingAggregateAllStats', 'src')
  const sink = mkName('tumblingAggregateAllStats', 'sink')
  const queryId = mkName('tumblingAggregateAllStats', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // Push 5 values into the SAME window (all within 1 second), then idle
  // flush will close it. Sum = 50, count = 5, avg = 10, min = 2, max = 30.
  const values = [10, 5, 30, 2, 3]
  for (const v of values) {
    await client.queue(src).partition('p').push([{ data: { v } }])
  }

  const handle = await Stream
    .from(client.queue(src))
    // Use a small window so idle flush closes it within the test timeout.
    .windowTumbling({ seconds: 3, idleFlushMs: 500 })
    .aggregate({
      count: () => 1,
      sum:   m => m.v,
      min:   m => m.v,
      max:   m => m.v,
      avg:   m => m.v
    })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  // Wait for idle flush to fire and emit the closed window (3s + grace + slack).
  const emits = await drainUntil(client, sink, {
    until: out => out.length >= 1, timeoutMs: 10000
  })
  await handle.stop()

  if (emits.length === 0) return { success: false, message: 'no emit observed' }
  const v = emits[0].data
  return summarise('tumblingAggregateAllStats', [
    expect(v.count, '===', 5, 'count'),
    expect(v.sum,   '===', 50, 'sum'),
    expect(v.min,   '===', 2, 'min'),
    expect(v.max,   '===', 30, 'max'),
    expect(v.avg,   '===', 10, 'avg')
  ])
}

// ----------------------------------------------------------------------------
// gracePeriod — events that arrive after windowEnd but within grace must
// still land in the original window's accumulator.
// ----------------------------------------------------------------------------

export async function tumblingGracePeriodDelaysClose(client) {
  const src = mkName('tumblingGracePeriodDelaysClose', 'src')
  const sink = mkName('tumblingGracePeriodDelaysClose', 'sink')
  const queryId = mkName('tumblingGracePeriodDelaysClose', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // Push 3 events as a single batch so they all land in the same
  // 2-second window (Queen-stamped createdAt resolves to the same ms).
  // Then verify:
  //   - During the grace period, the window stays OPEN (no emit yet)
  //   - After grace expires, idle flush closes it with sum=6 (1+2+3)
  await client.queue(src).partition('p').push([
    { data: { v: 1 } },
    { data: { v: 2 } },
    { data: { v: 3 } }
  ])

  const handle = await Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 2, gracePeriod: 4, idleFlushMs: 500 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  // After ~3s the window's end is past, but grace=4s means it should NOT
  // be closed yet (close at ~ windowStart + 2 + 4 = 6s after first event).
  await sleep(3000)
  const earlyDrain = await drainSink(client, sink, { timeoutMs: 500 })
  // Wait long enough for grace + idle flush to close.
  await sleep(4500)
  const lateDrain = await drainSink(client, sink, { timeoutMs: 500 })

  await handle.stop()

  return summarise('tumblingGracePeriodDelaysClose', [
    expect(earlyDrain.length, '===', 0, 'no emit during grace period'),
    expect(lateDrain.length, '>=', 1, 'window closed after grace expired'),
    expect(lateDrain[0]?.data?.sum, '===', 6, 'closed window sum (1+2+3)'),
    expect(lateDrain[0]?.data?.count, '===', 3, 'closed window count')
  ])
}

// ----------------------------------------------------------------------------
// idleFlushMs closes ripe windows even when no new traffic arrives — the
// canonical "v0.2 correctness fix" test.
// ----------------------------------------------------------------------------

export async function tumblingIdleFlushClosesQuietPartitions(client) {
  const src = mkName('tumblingIdleFlushClosesQuietPartitions', 'src')
  const sink = mkName('tumblingIdleFlushClosesQuietPartitions', 'sink')
  const queryId = mkName('tumblingIdleFlushClosesQuietPartitions', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // Push 3 messages then go silent — without idle flush, the window would
  // stay open forever. With a 700ms flush cadence and 1-second windows,
  // the flush should close the window within ~2 seconds of the last push.
  for (let i = 0; i < 3; i++) {
    await client.queue(src).partition('quiet').push([{ data: { v: 7 } }])
  }

  const handle = await Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 1, idleFlushMs: 700 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  // Wait for idle flush to fire.
  const emits = await drainUntil(client, sink, {
    until: out => out.length >= 1, timeoutMs: 8000
  })
  await handle.stop()

  return summarise('tumblingIdleFlushClosesQuietPartitions', [
    expect(emits.length, '>=', 1, 'idle flush emitted closed window'),
    expect(emits[0]?.data?.count, '===', 3, 'closed window count'),
    expect(emits[0]?.data?.sum, '===', 21, 'closed window sum'),
    expect(handle.metrics().flushCyclesTotal, '>=', 1, 'flushCyclesTotal advanced')
  ])
}

// ----------------------------------------------------------------------------
// .foreach(value, ctx) — verify ctx is populated with window + partition info.
// ----------------------------------------------------------------------------

export async function tumblingForeachCtxHasWindowAndPartition(client) {
  const src = mkName('tumblingForeachCtxHasWindowAndPartition', 'src')
  const queryId = mkName('tumblingForeachCtxHasWindowAndPartition', 'q')

  await client.queue(src).create()
  for (let i = 0; i < 3; i++) {
    await client.queue(src).partition('myKey').push([{ data: { v: i } }])
  }

  const captured = []
  const handle = await Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 1, idleFlushMs: 700 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .foreach((value, ctx) => { captured.push({ value, ctx }) })
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  const start = Date.now()
  while (captured.length < 1 && Date.now() - start < 8000) await sleep(100)
  await handle.stop()

  if (captured.length === 0) return { success: false, message: 'foreach not invoked' }
  const c = captured[0]
  return summarise('tumblingForeachCtxHasWindowAndPartition', [
    expect(c.ctx.partition, '===', 'myKey', 'ctx.partition'),
    expect(typeof c.ctx.partitionId, '===', 'string', 'ctx.partitionId is string'),
    expect(typeof c.ctx.windowKey, '===', 'string', 'ctx.windowKey is ISO string'),
    expect(typeof c.ctx.windowStart, '===', 'number', 'ctx.windowStart ms'),
    expect(typeof c.ctx.windowEnd, '===', 'number', 'ctx.windowEnd ms'),
    expect(c.value.count, '===', 3, 'value.count')
  ])
}
