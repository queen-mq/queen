/**
 * Crash recovery + config_hash safety.
 *
 * Tests in this file exercise the registration / mismatch / reset paths
 * and verify that resuming a stopped runner picks up where it left off.
 */

import { Stream } from '../../client-v2/index.js'
import {
  STREAMS_URL, mkName, sleep, drainSink, drainUntil,
  expect, summarise
} from './_helpers.js'

// ----------------------------------------------------------------------------
// config_hash mismatch is rejected with a clear error unless reset:true.
// ----------------------------------------------------------------------------

export async function recoveryConfigHashMismatchRejects(client) {
  const src = mkName('recoveryConfigHashMismatchRejects', 'src')
  const queryId = mkName('recoveryConfigHashMismatchRejects', 'q')

  await client.queue(src).create()

  // Run 1: register with one chain shape.
  const handle1 = await Stream
    .from(client.queue(src))
    .map(m => m.data)
    .to({ _queueName: 'never' })
    .run({ queryId, url: STREAMS_URL, reset: true })
  await handle1.stop()

  // Run 2: same queryId, DIFFERENT chain shape (added .filter). Without
  // reset:true, the register call must throw with a clear error.
  let threw = false
  let errMsg = ''
  try {
    const handle2 = await Stream
      .from(client.queue(src))
      .map(m => m.data)
      .filter(_ => true)                          // changed shape!
      .to({ _queueName: 'never' })
      .run({ queryId, url: STREAMS_URL })          // NO reset
    await handle2.stop()
  } catch (e) {
    threw = true
    errMsg = e && e.message ? e.message : String(e)
  }
  return summarise('recoveryConfigHashMismatchRejects', [
    expect(threw, '===', true, 'mismatch threw'),
    expect(/config_hash mismatch/i.test(errMsg), '===', true, 'error mentions config_hash mismatch')
  ])
}

// ----------------------------------------------------------------------------
// reset:true wipes state so a redeployed shape can run cleanly.
// ----------------------------------------------------------------------------

export async function recoveryResetTrueAcceptsNewShape(client) {
  const src = mkName('recoveryResetTrueAcceptsNewShape', 'src')
  const queryId = mkName('recoveryResetTrueAcceptsNewShape', 'q')

  await client.queue(src).create()

  // Register original shape.
  const handle1 = await Stream
    .from(client.queue(src))
    .map(m => m.data)
    .to({ _queueName: 'never' })
    .run({ queryId, url: STREAMS_URL, reset: true })
  await handle1.stop()

  // Different shape with reset:true → should NOT throw.
  let threw = false
  try {
    const handle2 = await Stream
      .from(client.queue(src))
      .map(m => m.data)
      .filter(_ => true)
      .to({ _queueName: 'never' })
      .run({ queryId, url: STREAMS_URL, reset: true })
    await handle2.stop()
  } catch (e) {
    threw = true
  }

  return summarise('recoveryResetTrueAcceptsNewShape', [
    expect(threw, '===', false, 'reset:true accepted')
  ])
}

// ----------------------------------------------------------------------------
// Mid-stream stop and resume — the second runner picks up the source cursor
// AND the watermarked window state. We push some data, run to flush, stop,
// push more, run again, and verify the second run only sees the new events.
// ----------------------------------------------------------------------------

export async function recoveryMidStreamResume(client) {
  const src = mkName('recoveryMidStreamResume', 'src')
  const sink = mkName('recoveryMidStreamResume', 'sink')
  const queryId = mkName('recoveryMidStreamResume', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  const buildStream = (handler) => Stream
    .from(client.queue(src))
    .windowTumbling({ seconds: 1, idleFlushMs: 500 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run(handler)

  // Run 1: push 5 events, let them flush.
  for (let i = 0; i < 5; i++) {
    await client.queue(src).partition('p').push([{ data: { v: 1 } }])
  }
  const handle1 = await buildStream({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })
  // Wait for at least one closed-window emit.
  await drainUntil(client, sink, { until: out => out.length >= 1, timeoutMs: 5000 })
  await handle1.stop()

  const m1 = handle1.metrics()

  // Push 5 more events while no runner is alive.
  for (let i = 0; i < 5; i++) {
    await client.queue(src).partition('p').push([{ data: { v: 1 } }])
  }

  // Run 2: same queryId, no reset.
  const handle2 = await buildStream({ queryId, url: STREAMS_URL, batchSize: 50 })
  await sleep(3000)
  await handle2.stop()
  const m2 = handle2.metrics()

  // The second run should have processed roughly 5 messages — the "new"
  // ones — and not the original 5 (those were already acked).
  return summarise('recoveryMidStreamResume', [
    expect(m1.messagesTotal, '>=', 5, 'run 1 saw events'),
    expect(m2.messagesTotal, '>=', 5, 'run 2 picked up new events'),
    expect(m2.messagesTotal, '<=', 8, 'run 2 did NOT re-process all of run 1')
  ])
}
