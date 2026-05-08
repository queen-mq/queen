/**
 * Combined / end-to-end pipeline scenarios.
 *
 * These exercise multi-stage pipelines (pre-stateless + window + aggregate
 * + post-stateless + sink) and concurrent streams that share a Queen
 * instance but operate on independent queues.
 */

import { Stream } from '../../client-v2/index.js'
import {
  STREAMS_URL, mkName, sleep, drainUntil,
  expect, summarise
} from './_helpers.js'

// ----------------------------------------------------------------------------
// Full pipeline: pre-stage filter + map → keyBy → window → aggregate →
// post-stage map + filter → foreach.
// ----------------------------------------------------------------------------

export async function combinedFullPipeline(client) {
  const src = mkName('combinedFullPipeline', 'src')
  const queryId = mkName('combinedFullPipeline', 'q')

  await client.queue(src).create()

  // Push 9 events + 9 heartbeats per user, all back-to-back so they land
  // in the same 5-second window. Single-batch push per partition.
  for (let u = 0; u < 3; u++) {
    const items = []
    for (let i = 0; i < 3; i++) {
      items.push({ data: { type: 'order', user: u, amount: 100 + u * 100 + i } })
      items.push({ data: { type: 'heartbeat' } })
    }
    await client.queue(src).partition(`user-${u}`).push(items)
  }

  const captured = []
  const handle = await Stream
    .from(client.queue(src))
    // Pre-reducer stateless: drop heartbeats, project the payload.
    .filter(m => m.data.type === 'order')
    .map(m => ({ user: m.data.user, amount: m.data.amount }))
    // Tumbling 5s so the test's idle flush actually closes windows
    // within the (also short) test wait. All 3 per-user events fit in
    // one window because we push them quickly back-to-back.
    .windowTumbling({ seconds: 5, idleFlushMs: 800 })
    .aggregate({ count: () => 1, sum: m => m.amount })
    // Post-reducer: re-project with partition info.
    .map((agg, ctx) => ({
      user: ctx.partition,
      windowKey: ctx.windowKey,
      ...agg
    }))
    .filter(v => v.count > 0)
    .foreach((value) => { captured.push(value) })
    .run({
      queryId,
      url: STREAMS_URL,
      batchSize: 100,
      maxPartitions: 4,
      reset: true
    })

  // Wait long enough for windowEnd to pass + idle flush to close all 3
  // user windows. windowTumbling(5s) + idleFlushMs(800) means worst case
  // ~6s.
  const start = Date.now()
  while (captured.length < 3 && Date.now() - start < 15000) await sleep(100)
  await handle.stop()

  const byUser = new Map()
  for (const c of captured) byUser.set(c.user, c)
  return summarise('combinedFullPipeline', [
    expect(byUser.size, '===', 3, 'three users emitted'),
    expect(byUser.get('user-0')?.count, '===', 3, 'user-0 count'),
    expect(byUser.get('user-0')?.sum, '===', 303, 'user-0 sum (100+101+102)'),
    expect(byUser.get('user-1')?.count, '===', 3, 'user-1 count'),
    expect(byUser.get('user-1')?.sum, '===', 603, 'user-1 sum (200+201+202)'),
    expect(byUser.get('user-2')?.count, '===', 3, 'user-2 count'),
    expect(byUser.get('user-2')?.sum, '===', 903, 'user-2 sum (300+301+302)')
  ])
}

// ----------------------------------------------------------------------------
// Two streams running concurrently against the same source queue, with
// distinct queryIds. Each gets its own consumer-group cursor and they
// must see all messages independently.
// ----------------------------------------------------------------------------

export async function combinedTwoConcurrentStreams(client) {
  const src = mkName('combinedTwoConcurrentStreams', 'src')
  const sinkA = mkName('combinedTwoConcurrentStreams-a', 'sink')
  const sinkB = mkName('combinedTwoConcurrentStreams-b', 'sink')
  const qA = mkName('combinedTwoConcurrentStreams-a', 'q')
  const qB = mkName('combinedTwoConcurrentStreams-b', 'q')

  await client.queue(src).create()
  await client.queue(sinkA).create()
  await client.queue(sinkB).create()

  // Push 12 events into a single partition.
  const items = []
  for (let i = 0; i < 12; i++) items.push({ data: { v: i + 1 } })
  await client.queue(src).partition('p').push(items)

  // Two streams: A computes a sum, B computes a count. Window size is
  // small enough that idle flush closes within the test wait.
  const streamA = Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 3, idleFlushMs: 600 })
    .aggregate({ sum: m => m.v })
    .to(client.queue(sinkA))
  const streamB = Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 3, idleFlushMs: 600 })
    .aggregate({ count: () => 1 })
    .to(client.queue(sinkB))

  const [handleA, handleB] = await Promise.all([
    streamA.run({ queryId: qA, url: STREAMS_URL, batchSize: 50, reset: true }),
    streamB.run({ queryId: qB, url: STREAMS_URL, batchSize: 50, reset: true })
  ])

  // Wait until the FULL totals are present on each sink. Each stream sees
  // all 12 events (different consumer groups), so sumA must reach 78 and
  // countB must reach 12 across all closed windows.
  const [emitsA, emitsB] = await Promise.all([
    drainUntil(client, sinkA, {
      until: out => out.reduce((a, m) => a + (m.data.sum || 0), 0) >= 78,
      timeoutMs: 12000
    }),
    drainUntil(client, sinkB, {
      until: out => out.reduce((a, m) => a + (m.data.count || 0), 0) >= 12,
      timeoutMs: 12000
    })
  ])
  await handleA.stop()
  await handleB.stop()

  const sumA = emitsA.reduce((a, m) => a + (m.data.sum || 0), 0)
  const countB = emitsB.reduce((a, m) => a + (m.data.count || 0), 0)
  return summarise('combinedTwoConcurrentStreams', [
    expect(emitsA.length, '>=', 1, 'stream A emitted'),
    expect(emitsB.length, '>=', 1, 'stream B emitted'),
    expect(sumA,   '===', 78, 'stream A sum 1..12'),
    expect(countB, '===', 12, 'stream B count')
  ])
}
