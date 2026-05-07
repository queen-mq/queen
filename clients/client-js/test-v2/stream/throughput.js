/**
 * High-throughput / multi-partition tests.
 *
 * These tests push thousands of messages across many partitions and verify
 * (a) the engine processes them all, (b) the resulting aggregates reflect
 * the true input, and (c) per-partition isolation holds at scale.
 */

import { Stream } from '../../../../streams/index.js'
import {
  STREAMS_URL, mkName, sleep, drainSink, drainUntil,
  expect, summarise
} from './_helpers.js'

// ----------------------------------------------------------------------------
// 5,000 messages across 20 partitions, processed under a tumbling window.
// (Kept under 10k to keep the suite under ~30s; bump if running standalone.)
// ----------------------------------------------------------------------------

export async function throughput5kAcross20Partitions(client) {
  const src = mkName('throughput5kAcross20Partitions', 'src')
  const sink = mkName('throughput5kAcross20Partitions', 'sink')
  const queryId = mkName('throughput5kAcross20Partitions', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  const NUM_PARTITIONS = 20
  const PER_PARTITION = 250
  const BATCH = 100

  // Push deterministically: every partition gets values 1..PER_PARTITION
  // so each partition's true sum is PER_PARTITION*(PER_PARTITION+1)/2.
  const pushStart = Date.now()
  for (let p = 0; p < NUM_PARTITIONS; p++) {
    const partition = `p-${p}`
    for (let off = 0; off < PER_PARTITION; off += BATCH) {
      const items = []
      for (let i = off; i < off + BATCH && i < PER_PARTITION; i++) {
        items.push({ data: { v: i + 1 } })
      }
      await client.queue(src).partition(partition).push(items)
    }
  }
  const pushMs = Date.now() - pushStart

  // Tumbling 5s so idle flush actually closes windows within the test
  // timeout. All push() calls above complete in < 5s, so each
  // partition's events fit in one window and we get exactly 20 emits.
  const handle = await Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 5, idleFlushMs: 1000 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .map((agg, ctx) => ({ partition: ctx.partition, ...agg }))
    .to(client.queue(sink))
    .run({
      queryId,
      url: STREAMS_URL,
      batchSize: 200,
      maxPartitions: 8,
      reset: true
    })

  // Wait until we have at least 20 partition emits — possibly more if
  // pushes spanned a window boundary, in which case we sum them per
  // partition below.
  const emits = await drainUntil(client, sink, {
    until: out => {
      const seen = new Set()
      for (const m of out) seen.add(m.data.partition)
      return seen.size >= NUM_PARTITIONS &&
             out.reduce((a, m) => a + (m.data.count || 0), 0) >= NUM_PARTITIONS * PER_PARTITION
    },
    timeoutMs: 60000
  })
  await handle.stop()

  const expectedSumPer = (PER_PARTITION * (PER_PARTITION + 1)) / 2
  const totalsByPart = new Map()
  for (const m of emits) {
    const p = m.data.partition
    if (!totalsByPart.has(p)) totalsByPart.set(p, { count: 0, sum: 0 })
    const t = totalsByPart.get(p)
    t.count += m.data.count
    t.sum   += m.data.sum
  }

  // Every partition's totals must match exactly.
  let mismatched = 0
  let missing = 0
  for (let p = 0; p < NUM_PARTITIONS; p++) {
    const t = totalsByPart.get(`p-${p}`)
    if (!t) { missing++; continue }
    if (t.count !== PER_PARTITION || t.sum !== expectedSumPer) mismatched++
  }

  return summarise('throughput5kAcross20Partitions', [
    expect(missing, '===', 0, 'no partitions missed'),
    expect(mismatched, '===', 0, 'all partitions have exact count + sum'),
    expect(handle.metrics().errorsTotal, '===', 0, 'no runner errors'),
    expect(pushMs, '<', 15000, 'push completed reasonably fast')
  ])
}

// ----------------------------------------------------------------------------
// Many small windows / many cycles. Verifies the runner doesn't drop
// messages under sustained pop pressure.
// ----------------------------------------------------------------------------

export async function throughputManyWindowsSinglePartition(client) {
  const src = mkName('throughputManyWindowsSinglePartition', 'src')
  const sink = mkName('throughputManyWindowsSinglePartition', 'sink')
  const queryId = mkName('throughputManyWindowsSinglePartition', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  const N = 200
  // Push 200 messages quickly.
  for (let off = 0; off < N; off += 50) {
    const items = []
    for (let i = off; i < off + 50 && i < N; i++) {
      items.push({ data: { v: i + 1 } })
    }
    await client.queue(src).partition('p').push(items)
  }

  const handle = await Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 1, idleFlushMs: 600 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  // Wait for all 200 messages to be reflected in emits.
  const emits = await drainUntil(client, sink, {
    until: out => {
      const total = out.reduce((a, m) => a + (m.data.count || 0), 0)
      return total >= N
    },
    timeoutMs: 30000
  })
  await handle.stop()

  const totalCount = emits.reduce((a, m) => a + (m.data.count || 0), 0)
  const totalSum = emits.reduce((a, m) => a + (m.data.sum || 0), 0)
  const expectedSum = (N * (N + 1)) / 2
  return summarise('throughputManyWindowsSinglePartition', [
    expect(totalCount, '===', N, 'all messages counted'),
    expect(totalSum, '===', expectedSum, 'sum matches 1..N'),
    expect(handle.metrics().errorsTotal, '===', 0, 'no errors')
  ])
}
