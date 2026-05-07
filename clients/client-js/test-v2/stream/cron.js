/**
 * Cron / wall-clock-aligned windows. v0.2 ships the `every:` shorthand:
 *   'second' | 'minute' | 'hour' | 'day' | 'week'
 *
 * Boundaries are anchored to UTC. Day = midnight UTC; week = Monday 00:00
 * UTC. Tests here exercise live behaviour (in-PG state) for the second
 * granularity (fast enough to test in seconds rather than hours).
 */

import { Stream } from '../../../../streams/index.js'
import { WindowCronOperator } from '../../../../streams/src/operators/WindowCronOperator.js'
import {
  STREAMS_URL, mkName, sleep, drainUntil,
  expect, summarise
} from './_helpers.js'

// ----------------------------------------------------------------------------
// Validation — invalid 'every' rejected.
// ----------------------------------------------------------------------------

export async function cronRejectsInvalidEvery(_client) {
  let threw = false
  try {
    /* eslint-disable no-new */
    new WindowCronOperator({ every: 'fortnight' })
  } catch (e) {
    threw = e && /every must be/.test(String(e.message))
  }
  return summarise('cronRejectsInvalidEvery', [
    expect(threw, '===', true, 'unknown every: rejected')
  ])
}

// ----------------------------------------------------------------------------
// Annotation — minute / hour / day / week boundaries are floored as expected.
// ----------------------------------------------------------------------------

export async function cronBoundariesAlignToUTC(_client) {
  const cases = [
    { every: 'minute', createdAt: '2026-01-01T10:00:35.123Z', expected: '2026-01-01T10:00:00.000Z' },
    { every: 'hour',   createdAt: '2026-01-01T10:30:35.000Z', expected: '2026-01-01T10:00:00.000Z' },
    { every: 'day',    createdAt: '2026-01-01T22:30:35.000Z', expected: '2026-01-01T00:00:00.000Z' },
    // 2026-01-07 is a Wednesday → Monday Jan 5.
    { every: 'week',   createdAt: '2026-01-07T12:00:00.000Z', expected: '2026-01-05T00:00:00.000Z' }
  ]
  const checks = []
  for (const c of cases) {
    const op = new WindowCronOperator({ every: c.every })
    const out = await op.apply({
      msg: { createdAt: c.createdAt, data: {} },
      key: 'p1', value: {}
    })
    checks.push(expect(out[0].windowKey, '===', c.expected, `every=${c.every}`))
  }
  return summarise('cronBoundariesAlignToUTC', checks)
}

// ----------------------------------------------------------------------------
// Live: every: 'second' on a real Queen instance. Push some events spanning
// 3 second boundaries, expect at least 2 closed windows.
// ----------------------------------------------------------------------------

export async function cronEverySecondLive(client) {
  const src = mkName('cronEverySecondLive', 'src')
  const sink = mkName('cronEverySecondLive', 'sink')
  const queryId = mkName('cronEverySecondLive', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  // Push one event per ~700ms for ~5 seconds → spans ~7 second boundaries.
  const pushPromise = (async () => {
    for (let i = 0; i < 7; i++) {
      await client.queue(src).partition('p').push([{ data: { v: 1 } }])
      await sleep(700)
    }
  })()

  const handle = await Stream
    .from(client.queue(src))
    .windowCron({ every: 'second', idleFlushMs: 500 })
    .aggregate({ count: () => 1 })
    .to(client.queue(sink))
    .run({ queryId, url: STREAMS_URL, batchSize: 50, reset: true })

  await pushPromise
  const emits = await drainUntil(client, sink, {
    until: out => out.length >= 4, timeoutMs: 10000
  })
  await handle.stop()

  // We pushed 7 events spanning multiple 1-second windows; expect at least 4
  // closed windows after idle flush.
  return summarise('cronEverySecondLive', [
    expect(emits.length, '>=', 4, 'multiple 1-second windows closed'),
    expect(emits.every(m => typeof m.data.count === 'number'), '===', true, 'every emit has count')
  ])
}
