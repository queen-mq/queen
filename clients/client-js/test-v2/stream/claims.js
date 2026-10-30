/**
 * Closing windows under a claim (broker 2.7, issue #153).
 *
 * A processing-time window closes when its time comes even if its partition
 * gets no more messages: the runner claims the partition (a lease with an
 * empty batch) and closes the window in a cycle that acks that batch. Before
 * claims, the idle flush only knew the partitions its own process had popped
 * in the last five minutes, so a window open at a restart never closed, and
 * two runners could emit one window twice.
 */

import { Stream } from '../../client-v2/index.js'
import {
  STREAMS_URL, mkName, sleep, drainUntil,
  expect, summarise
} from './_helpers.js'

function countsByPartition(messages) {
  const by = new Map()
  for (const m of messages) {
    if (!by.has(m.partition)) by.set(m.partition, [])
    by.get(m.partition).push(m.data?.count)
  }
  return by
}

// ----------------------------------------------------------------------------
// A session open when its runner stops closes on the next runner, with no new
// event on its partition.
// ----------------------------------------------------------------------------

export async function claimSessionOpenAtRestartClosesWithoutNewEvent(client) {
  const src = mkName('claimSessionOpenAtRestartClosesWithoutNewEvent', 'src')
  const sink = mkName('claimSessionOpenAtRestartClosesWithoutNewEvent', 'sink')
  const queryId = mkName('claimSessionOpenAtRestartClosesWithoutNewEvent', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  const build = () => Stream
    .from(client.queue(src))
    .windowSession({ gap: 3, idleFlushMs: 250 })
    .aggregate({ count: () => 1 })
    .to(client.queue(sink))
  const opts = { queryId, url: STREAMS_URL, maxWaitMillis: 200, subscriptionMode: 'all' }

  // Two events of trace-a, then a stop while its 3 s session is open.
  const first = await build().run({ ...opts, reset: true })
  for (let i = 0; i < 2; i++) {
    await client.queue(src).partition('trace-a').push([{ data: { n: 1 } }])
    await sleep(100)
  }
  await sleep(1200)
  const folded = first.metrics().messagesTotal
  await first.stop()

  // Runner 2, same query. trace-b is the control: it gets an event now.
  const second = await build().run(opts)
  await client.queue(src).partition('trace-b').push([{ data: { n: 1 } }])

  const group = `drain-claim-restart-${Date.now()}`
  const out = await drainUntil(client, sink, {
    group,
    timeoutMs: 15_000,
    until: m => m.some(x => x.partition === 'trace-a') && m.some(x => x.partition === 'trace-b')
  })
  // Nothing more comes for either partition.
  await sleep(2000)
  const late = await drainUntil(client, sink, { group, timeoutMs: 1000, until: () => false })
  await second.stop()

  const by = countsByPartition([...out, ...late])
  return summarise('claimSessionOpenAtRestartClosesWithoutNewEvent', [
    expect(folded, '===', 2, 'runner 1 folded both events'),
    expect(JSON.stringify(by.get('trace-a') || []), '===', '[2]', "trace-a's session closes once, with both events"),
    expect(JSON.stringify(by.get('trace-b') || []), '===', '[1]', "trace-b's session closes once"),
    expect(by.size, '===', 2, 'emits on trace-a and trace-b only')
  ])
}

// ----------------------------------------------------------------------------
// Two runners on one query close every session exactly once: each close runs
// under its partition's lease, and the broker refuses a close whose lease is
// gone.
// ----------------------------------------------------------------------------

export async function claimTwoRunnersCloseEachSessionOnce(client) {
  const src = mkName('claimTwoRunnersCloseEachSessionOnce', 'src')
  const sink = mkName('claimTwoRunnersCloseEachSessionOnce', 'sink')
  const queryId = mkName('claimTwoRunnersCloseEachSessionOnce', 'q')

  await client.queue(src).create()
  await client.queue(sink).create()

  const build = () => Stream
    .from(client.queue(src))
    .windowSession({ gap: 2, idleFlushMs: 100 })
    .aggregate({ count: () => 1 })
    .to(client.queue(sink))
  const opts = { queryId, url: STREAMS_URL, maxWaitMillis: 100, maxPartitions: 2, subscriptionMode: 'all' }
  const a = await build().run({ ...opts, reset: true })
  const b = await build().run(opts)

  const PARTITIONS = 20
  for (let round = 0; round < 2; round++) {
    for (let p = 0; p < PARTITIONS; p++) {
      await client.queue(src).partition(`s-${p}`).push([{ data: { round } }])
    }
    await sleep(300)
  }

  const group = `drain-claim-two-${Date.now()}`
  const out = await drainUntil(client, sink, {
    group, timeoutMs: 20_000, until: m => m.length >= PARTITIONS
  })
  // Anything emitted twice would arrive within a few flush ticks.
  await sleep(2000)
  const late = await drainUntil(client, sink, { group, timeoutMs: 1000, until: () => false })
  await a.stop()
  await b.stop()

  const by = countsByPartition([...out, ...late])
  const wrong = [...by.entries()].filter(([, counts]) => JSON.stringify(counts) !== '[2]')
  return summarise('claimTwoRunnersCloseEachSessionOnce', [
    expect(by.size, '===', PARTITIONS, 'every partition emitted'),
    expect(JSON.stringify(wrong), '===', '[]', 'partitions not emitted exactly once with count 2'),
    expect(a.metrics().messagesTotal + b.metrics().messagesTotal, '===', 2 * PARTITIONS, 'each event folded once')
  ])
}
