/**
 * Session windows — per-key activity-based.
 */

import { Stream } from '../../../../streams/index.js'
import {
  STREAMS_URL, mkName, sleep, drainSink, drainUntil,
  expect, summarise
} from './_helpers.js'

// ----------------------------------------------------------------------------
// A session extends as long as events keep arriving within the gap.
// Two events close together, long pause, two more events → 2 sessions.
// ----------------------------------------------------------------------------

export async function sessionGapClosesAndStartsNew(client) {
  const src = mkName('sessionGapClosesAndStartsNew', 'src')
  const sink = mkName('sessionGapClosesAndStartsNew', 'sink')
  const queryId = mkName('sessionGapClosesAndStartsNew', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // Push 2 events 500ms apart, sleep 4s (> 2s gap), push 2 more events.
  // Expect: 2 closed sessions.
  const pushPromise = (async () => {
    await client.queue(src).partition('user-1').push([{ data: { v: 1 } }])
    await sleep(500)
    await client.queue(src).partition('user-1').push([{ data: { v: 2 } }])
    await sleep(4000)
    await client.queue(src).partition('user-1').push([{ data: { v: 3 } }])
    await sleep(500)
    await client.queue(src).partition('user-1').push([{ data: { v: 4 } }])
  })()

  const handle = await Stream
    .from(client.queue(src))
    .windowSession({ gap: 2, idleFlushMs: 500 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  await pushPromise
  // Wait for idle flush to close the second (still-open) session.
  await sleep(5000)
  const emits = await drainSink(client, sink, { timeoutMs: 2000 })
  await handle.stop()

  const counts = emits.map(m => m.data.count).sort((a, b) => a - b)
  const sums = emits.map(m => m.data.sum).sort((a, b) => a - b)
  return summarise('sessionGapClosesAndStartsNew', [
    expect(emits.length, '===', 2, 'two sessions emitted'),
    expect(counts[0], '===', 2, 'session 1 count'),
    expect(counts[1], '===', 2, 'session 2 count'),
    expect(sums[0],   '===', 3, 'session 1 sum (1+2)'),
    expect(sums[1],   '===', 7, 'session 2 sum (3+4)')
  ])
}

// ----------------------------------------------------------------------------
// Idle flush — pushes some events then goes silent. The session must be
// closed by the idle-flush sweep within `gap + flush` after the last event.
// ----------------------------------------------------------------------------

export async function sessionIdleFlushClosesQuietSession(client) {
  const src = mkName('sessionIdleFlushClosesQuietSession', 'src')
  const sink = mkName('sessionIdleFlushClosesQuietSession', 'sink')
  const queryId = mkName('sessionIdleFlushClosesQuietSession', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  for (let i = 0; i < 3; i++) {
    await client.queue(src).partition('user-quiet').push([{ data: { v: i + 1 } }])
    await sleep(200)
  }

  const handle = await Stream
    .from(client.queue(src))
    .windowSession({ gap: 1, idleFlushMs: 500 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  // Wait for gap + flush to elapse.
  const emits = await drainUntil(client, sink, {
    until: out => out.length >= 1, timeoutMs: 8000
  })
  await handle.stop()

  return summarise('sessionIdleFlushClosesQuietSession', [
    expect(emits.length, '===', 1, 'one session emitted'),
    expect(emits[0]?.data?.count, '===', 3, 'session count'),
    expect(emits[0]?.data?.sum, '===', 6, 'session sum (1+2+3)'),
    expect(handle.metrics().flushCyclesTotal, '>=', 1, 'flush fired')
  ])
}

// ----------------------------------------------------------------------------
// Multiple keys — each key has its own independent session in the same
// partition.
// ----------------------------------------------------------------------------

export async function sessionMultipleKeysIndependent(client) {
  const src = mkName('sessionMultipleKeysIndependent', 'src')
  const sink = mkName('sessionMultipleKeysIndependent', 'sink')
  const queryId = mkName('sessionMultipleKeysIndependent', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // Two users, three events each, all within one session per user.
  for (let i = 0; i < 3; i++) {
    await client.queue(src).partition('shared').push([
      { data: { user: 'alice', v: i + 1 } },
      { data: { user: 'bob',   v: (i + 1) * 10 } }
    ])
    await sleep(200)
  }

  const handle = await Stream
    .from(client.queue(src))
    .keyBy(m => m.data.user)
    .windowSession({ gap: 1, idleFlushMs: 500 })
    .aggregate({ count: () => 1, sum: m => m.v })
    .map((v, ctx) => ({ user: ctx.key, ...v }))
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  const emits = await drainUntil(client, sink, {
    until: out => out.length >= 2, timeoutMs: 8000
  })
  await handle.stop()

  const byUser = {}
  for (const m of emits) byUser[m.data.user] = m.data
  return summarise('sessionMultipleKeysIndependent', [
    expect(Object.keys(byUser).length, '===', 2, 'two distinct users emitted'),
    expect(byUser['alice']?.count, '===', 3, 'alice count'),
    expect(byUser['alice']?.sum,   '===', 6, 'alice sum (1+2+3)'),
    expect(byUser['bob']?.count,   '===', 3, 'bob count'),
    expect(byUser['bob']?.sum,     '===', 60, 'bob sum (10+20+30)')
  ])
}
