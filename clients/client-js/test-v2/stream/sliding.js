/**
 * Sliding / hopping windows.
 */

import { Stream } from '../../../../streams/index.js'
import {
  STREAMS_URL, mkName, sleep, drainSink, drainUntil,
  expect, summarise
} from './_helpers.js'
import { WindowSlidingOperator } from '../../../../streams/src/operators/WindowSlidingOperator.js'

// ----------------------------------------------------------------------------
// Validation — size must be a multiple of slide.
// ----------------------------------------------------------------------------

export async function slidingRejectsInvalidConfig(_client) {
  let threw = false
  try {
    /* eslint-disable no-new */
    new WindowSlidingOperator({ size: 60, slide: 7 })
  } catch (e) {
    threw = e && /must be an integer multiple/.test(String(e.message))
  }
  return summarise('slidingRejectsInvalidConfig', [
    expect(threw, '===', true, 'WindowSliding rejects size%slide!=0')
  ])
}

// ----------------------------------------------------------------------------
// Each event lands in N=size/slide windows. With size=4 and slide=2, two
// windows. Push 4 events 1s apart — each contributes to two windows.
// Total emits over the run should reflect this overlap.
// ----------------------------------------------------------------------------

export async function slidingEventsAppearInOverlappingWindows(client) {
  const src = mkName('slidingEventsAppearInOverlappingWindows', 'src')
  const sink = mkName('slidingEventsAppearInOverlappingWindows', 'sink')
  const queryId = mkName('slidingEventsAppearInOverlappingWindows', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // 6 events 1s apart. size=4s, slide=2s → each event in 2 windows.
  // After all events flushed by idle, sum of "count" across all closed
  // windows = 6 events × 2 windows = 12.
  const itemPushPromise = (async () => {
    for (let i = 0; i < 6; i++) {
      await client.queue(src).partition('p').push([{ data: { v: 1 } }])
      await sleep(1000)
    }
  })()

  const handle = await Stream
    .from(client.queue(src))
    .windowSliding({ size: 4, slide: 2, idleFlushMs: 1000 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  await itemPushPromise
  // Allow idle flush to close trailing windows.
  await sleep(6000)
  const emits = await drainSink(client, sink, { timeoutMs: 2000 })
  await handle.stop()

  const totalCount = emits.reduce((a, m) => a + (m.data.count || 0), 0)
  // Allow some slack on the last partial window — 12 if all overlapping
  // windows are seen, slightly less if the runner shut down before idle
  // flush finished. Either way well above the no-overlap baseline of 6.
  return summarise('slidingEventsAppearInOverlappingWindows', [
    expect(emits.length, '>=', 4, 'multiple closed windows observed'),
    expect(totalCount, '>=', 8, 'overlap counted (>= 8 means each event in >=1 windows)'),
    expect(totalCount, '<=', 12, 'overlap bounded (<= size/slide × events = 12)')
  ])
}
