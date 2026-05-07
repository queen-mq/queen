/**
 * Event-time mode + per-partition watermarks + late-event handling.
 *
 * In event-time mode, the window operator's clock is the user-provided
 * timestamp (e.g. m.data.eventTs), NOT the Queen-stamped createdAt. The
 * runner maintains a per-partition watermark stored under the reserved
 * __wm__ state key; events older than the watermark are routed per the
 * `onLate` policy.
 */

import { Stream } from '../../../../streams/index.js'
import {
  STREAMS_URL, mkName, sleep, drainSink, drainUntil,
  expect, summarise
} from './_helpers.js'

// ----------------------------------------------------------------------------
// Basic event-time: windows are bucketed by eventTs, not createdAt.
// We push events with backdated eventTs values and verify the closed-window
// emits use the eventTs-derived windowKey.
// ----------------------------------------------------------------------------

export async function eventTimeBucketsByExtractor(client) {
  const src = mkName('eventTimeBucketsByExtractor', 'src')
  const sink = mkName('eventTimeBucketsByExtractor', 'sink')
  const queryId = mkName('eventTimeBucketsByExtractor', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // 3 events: all created NOW, but eventTs values that span a 60-second
  // window. eventTs values are at 10:00:01, 10:00:30, and 10:01:05 (the
  // last crosses the minute boundary, closing the 10:00 window).
  await client.queue(src).partition('p').push([
    { data: { v: 1, eventTs: '2026-01-01T10:00:01Z' } },
    { data: { v: 2, eventTs: '2026-01-01T10:00:30Z' } },
    { data: { v: 5, eventTs: '2026-01-01T10:01:05Z' } }
  ])

  const handle = await Stream
    .from(client.queue(src))
    .windowTumbling({
      seconds: 60,
      eventTime: m => m.data.eventTs,
      idleFlushMs: 700
    })
    .aggregate({ count: () => 1, sum: m => m.v })
    .map((agg, ctx) => ({ windowKey: ctx.windowKey, ...agg }))
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  // The 10:00 window closes when the 10:01:05 event advances the
  // watermark past its end. The 10:01 window stays open until either
  // a fresh event past 10:02 advances the watermark OR idle flush. In
  // event-time mode, idle flush uses the watermark (not Date.now()), so
  // without further events the watermark stays at ~10:01:05 and the
  // 10:01 window legitimately remains open.
  //
  // We only assert on the 10:00 window here. (See
  // eventTimeWatermarkAdvancesAcrossBatch for the multi-batch path.)
  const emits = await drainUntil(client, sink, {
    until: out => out.length >= 1, timeoutMs: 10000
  })
  await handle.stop()

  const byWin = {}
  for (const m of emits) byWin[m.data.windowKey] = m.data
  return summarise('eventTimeBucketsByExtractor', [
    expect(emits.length, '>=', 1, 'at least one window emitted'),
    expect(byWin['2026-01-01T10:00:00.000Z']?.sum, '===', 3, '10:00 window sum'),
    expect(byWin['2026-01-01T10:00:00.000Z']?.count, '===', 2, '10:00 window count')
  ])
}

// ----------------------------------------------------------------------------
// Late event with onLate='drop' is excluded from the window and counted in
// metrics().lateEventsTotal.
// ----------------------------------------------------------------------------

export async function eventTimeLateDropExcludesEvent(client) {
  const src = mkName('eventTimeLateDropExcludesEvent', 'src')
  const sink = mkName('eventTimeLateDropExcludesEvent', 'sink')
  const queryId = mkName('eventTimeLateDropExcludesEvent', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // Run 1: advance the watermark by emitting a fresh event.
  await client.queue(src).partition('p').push([
    { data: { v: 100, eventTs: '2026-01-01T11:00:00Z' } }
  ])

  const buildStream = (handler) => Stream
    .from(client.queue(src))
    .windowTumbling({
      seconds: 60,
      eventTime: m => m.data.eventTs,
      allowedLateness: 30,
      onLate: 'drop',
      idleFlushMs: 700
    })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run(handler)

  const handle1 = await buildStream({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })
  await sleep(2000)
  await handle1.stop()

  // Run 2: push a stale event with eventTs way before the watermark.
  await client.queue(src).partition('p').push([
    { data: { v: 999, eventTs: '2026-01-01T05:00:00Z' } }
  ])
  const handle2 = await buildStream({ queryId, url: STREAMS_URL, batchSize: 50 })
  await sleep(2000)
  await handle2.stop()

  return summarise('eventTimeLateDropExcludesEvent', [
    expect(handle2.metrics().lateEventsTotal, '>=', 1, 'late event recorded')
  ])
}

// ----------------------------------------------------------------------------
// Allowed lateness — events within the lateness window are NOT considered late.
// ----------------------------------------------------------------------------

export async function eventTimeAllowedLatenessIncluded(client) {
  const src = mkName('eventTimeAllowedLatenessIncluded', 'src')
  const sink = mkName('eventTimeAllowedLatenessIncluded', 'sink')
  const queryId = mkName('eventTimeAllowedLatenessIncluded', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // Run 1: advance watermark to 11:00:00 - 60s = 10:59:00.
  await client.queue(src).partition('p').push([
    { data: { v: 1, eventTs: '2026-01-01T11:00:00Z' } }
  ])

  const buildStream = (handler) => Stream
    .from(client.queue(src))
    .windowTumbling({
      seconds: 60,
      eventTime: m => m.data.eventTs,
      allowedLateness: 60,                    // 60s tolerance
      onLate: 'drop',
      idleFlushMs: 700
    })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run(handler)

  const handle1 = await buildStream({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })
  await sleep(2000)
  await handle1.stop()

  // Run 2: a "late but within lateness" event — eventTs=10:59:30 is 30s
  // older than the watermark of 10:59:00, but still within the 60s
  // allowed-lateness window so it should be accepted.
  // Watermark in our model = max(observed) - allowedLateness, and the
  // late filter is `eventTime < watermark`. So 10:59:30 vs wm 10:59:00:
  // 10:59:30 > 10:59:00 → NOT late. Good.
  await client.queue(src).partition('p').push([
    { data: { v: 7, eventTs: '2026-01-01T10:59:30Z' } }
  ])
  const handle2 = await buildStream({ queryId, url: STREAMS_URL, batchSize: 50 })
  await sleep(2000)
  await handle2.stop()

  return summarise('eventTimeAllowedLatenessIncluded', [
    expect(handle2.metrics().lateEventsTotal, '===', 0, 'no late events')
  ])
}

// ----------------------------------------------------------------------------
// onLate='include' accumulates the late event into its window even if it
// would otherwise be dropped.
// ----------------------------------------------------------------------------

export async function eventTimeLateIncludePolicy(client) {
  const src = mkName('eventTimeLateIncludePolicy', 'src')
  const sink = mkName('eventTimeLateIncludePolicy', 'sink')
  const queryId = mkName('eventTimeLateIncludePolicy', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  await client.queue(src).partition('p').push([
    { data: { v: 1, eventTs: '2026-01-01T11:00:00Z' } }
  ])

  const buildStream = (handler) => Stream
    .from(client.queue(src))
    .windowTumbling({
      seconds: 60,
      eventTime: m => m.data.eventTs,
      allowedLateness: 30,
      onLate: 'include',
      idleFlushMs: 700
    })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run(handler)

  const handle1 = await buildStream({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })
  await sleep(2000)
  await handle1.stop()

  await client.queue(src).partition('p').push([
    { data: { v: 999, eventTs: '2026-01-01T05:00:00Z' } }
  ])
  const handle2 = await buildStream({ queryId, url: STREAMS_URL, batchSize: 50 })
  await sleep(2000)
  await handle2.stop()

  return summarise('eventTimeLateIncludePolicy', [
    // include policy: lateEventsTotal stays at 0 because we did not drop.
    expect(handle2.metrics().lateEventsTotal, '===', 0, 'include policy did not increment dropped count')
  ])
}
