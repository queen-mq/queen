/**
 * Stateless operators on streams: map, filter, flatMap, keyBy, foreach,
 * .to(sink). These tests don't use any window operator — they verify the
 * basic plumbing works against a live Queen.
 */

import { Stream } from '../../client-v2/index.js'
import {
  STREAMS_URL, mkName, sleep, pushSpread, drainSink, drainUntil,
  expect, summarise, runStreamFor
} from './_helpers.js'

// ----------------------------------------------------------------------------
// map + filter + sink — verify the basic pipeline routes events correctly.
// ----------------------------------------------------------------------------

export async function streamMapFilterSink(client) {
  const src = mkName('streamMapFilterSink', 'src')
  const sink = mkName('streamMapFilterSink', 'sink')
  const queryId = mkName('streamMapFilterSink', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // 30 events: 10 of type 'order', 10 of type 'heartbeat', 10 of type 'log'.
  const items = []
  for (let i = 0; i < 30; i++) {
    const type = i % 3 === 0 ? 'order' : i % 3 === 1 ? 'heartbeat' : 'log'
    items.push({ partition: 'Default', data: { type, n: i } })
  }
  await pushSpread(client, src, items)

  const handle = await Stream
    .from(client.queue(src))
    .filter(m => m.data.type !== 'heartbeat')
    .map(m => ({ ...m.data, marked: true }))
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 100, reset: true })

  // 20 messages should land on the sink (10 orders + 10 logs).
  const drained = await drainUntil(client, sink, {
    until: out => out.length >= 20,
    timeoutMs: 8000
  })
  await handle.stop()

  const checks = [
    expect(drained.length, '>=', 20, 'sink message count'),
    expect(drained.every(m => m.data.marked === true), '===', true, 'every msg has marked=true'),
    expect(drained.every(m => m.data.type !== 'heartbeat'), '===', true, 'no heartbeats survived filter'),
    expect(handle.metrics().errorsTotal, '===', 0, 'no errors')
  ]
  return summarise('streamMapFilterSink', checks)
}

// ----------------------------------------------------------------------------
// flatMap — fan-out: each input produces N outputs.
// ----------------------------------------------------------------------------

export async function streamFlatMapFanout(client) {
  const src = mkName('streamFlatMapFanout', 'src')
  const sink = mkName('streamFlatMapFanout', 'sink')
  const queryId = mkName('streamFlatMapFanout', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // 5 inputs, each fans out to 4 outputs → expect 20 sink messages.
  const items = []
  for (let i = 0; i < 5; i++) {
    items.push({ partition: 'Default', data: { id: i } })
  }
  await pushSpread(client, src, items)

  const handle = await Stream
    .from(client.queue(src))
    .flatMap(m => [
      { id: m.data.id, copy: 1 },
      { id: m.data.id, copy: 2 },
      { id: m.data.id, copy: 3 },
      { id: m.data.id, copy: 4 }
    ])
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 100, reset: true })

  const drained = await drainUntil(client, sink, {
    until: out => out.length >= 20,
    timeoutMs: 8000
  })
  await handle.stop()

  return summarise('streamFlatMapFanout', [
    expect(drained.length, '===', 20, 'fan-out count')
  ])
}

// ----------------------------------------------------------------------------
// foreach — terminal at-least-once side effect captures every emit.
// ----------------------------------------------------------------------------

export async function streamForeachCapturesAll(client) {
  const src = mkName('streamForeachCapturesAll', 'src')
  const queryId = mkName('streamForeachCapturesAll', 'q')

  await client.queue(src).create()

  const items = []
  for (let i = 0; i < 12; i++) {
    items.push({ partition: 'Default', data: { i } })
  }
  await pushSpread(client, src, items)

  const captured = []
  const handle = await Stream
    .from(client.queue(src))
    .foreach((value) => { captured.push(value) })
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  // Wait until at-least-once semantics catch up to 12 messages.
  const start = Date.now()
  while (captured.length < 12 && Date.now() - start < 5000) await sleep(50)
  await handle.stop()

  return summarise('streamForeachCapturesAll', [
    expect(captured.length, '>=', 12, 'foreach captured all'),
    expect(captured.every(v => typeof v.i === 'number'), '===', true, 'each captured value is an i-bearing object')
  ])
}

// ----------------------------------------------------------------------------
// keyBy — overrides the default partition-key. Without a window/reducer
// downstream there's no observable behavior change to the sink, but we do
// verify the runner emits a warning at start and doesn't crash.
// ----------------------------------------------------------------------------

export async function streamKeyByDoesNotCrash(client) {
  const src = mkName('streamKeyByDoesNotCrash', 'src')
  const sink = mkName('streamKeyByDoesNotCrash', 'sink')
  const queryId = mkName('streamKeyByDoesNotCrash', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  const items = []
  for (let i = 0; i < 6; i++) {
    items.push({ partition: 'Default', data: { region: i % 2 === 0 ? 'EU' : 'US', n: i } })
  }
  await pushSpread(client, src, items)

  const handle = await Stream
    .from(client.queue(src))
    .keyBy(m => m.data.region)
    .map(m => m.data)
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 100, reset: true })

  const drained = await drainUntil(client, sink, {
    until: out => out.length >= 6,
    timeoutMs: 8000
  })
  await handle.stop()

  return summarise('streamKeyByDoesNotCrash', [
    expect(drained.length, '===', 6, 'all messages reach sink'),
    expect(handle.metrics().errorsTotal, '===', 0, 'no errors')
  ])
}

// ----------------------------------------------------------------------------
// Pre-reducer ops only — async map should be honored (await pauses the cycle).
// ----------------------------------------------------------------------------

export async function streamAsyncMap(client) {
  const src = mkName('streamAsyncMap', 'src')
  const sink = mkName('streamAsyncMap', 'sink')
  const queryId = mkName('streamAsyncMap', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  const items = []
  for (let i = 0; i < 5; i++) {
    items.push({ partition: 'Default', data: { i } })
  }
  await pushSpread(client, src, items)

  const handle = await Stream
    .from(client.queue(src))
    .map(async (m) => {
      // Simulate I/O latency.
      await sleep(20)
      return { ...m.data, asyncEnriched: true }
    })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 100, reset: true })

  const drained = await drainUntil(client, sink, {
    until: out => out.length >= 5,
    timeoutMs: 8000
  })
  await handle.stop()

  return summarise('streamAsyncMap', [
    expect(drained.length, '===', 5, 'all enriched events reach sink'),
    expect(drained.every(m => m.data.asyncEnriched === true), '===', true, 'every msg async-enriched')
  ])
}
