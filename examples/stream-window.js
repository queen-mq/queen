/**
 * Minimal sliding window example.
 *
 * Pushes 6 events 1 second apart into a single partition. The stream computes
 * a sliding window of size=4s, slide=2s — so windows overlap and each event
 * lands in size/slide = 2 windows.
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node examples/stream-window.js
 */

import { Queen, Stream } from 'queen-mq'

const url = process.env.QUEEN_URL || 'http://localhost:6632'
const q = new Queen({ url })

const SRC  = 'window-src'
const SINK = 'window-sink'

await q.queue(SRC).delete()
await q.queue(SINK).delete()
await q.queue(SRC).create()
await q.queue(SINK).create()

// 1) Producer: 6 events, 1s apart, all into the same partition.
;(async () => {
  for (let i = 0; i < 6; i++) {
    await q.queue(SRC).partition('p1').push([{ data: { v: i + 1 } }])
    await new Promise(r => setTimeout(r, 1000))
  }
})()

// 2) Sliding window: size=4s, slide=2s ⇒ each event in 2 overlapping windows.
const stream = await Stream
  .from(q.queue(SRC))
  .windowSliding({ size: 4, slide: 2, idleFlushMs: 1000 })
  .reduce(
    (acc, msg) => ({
      count: acc.count + 1,
      sum:   acc.sum + (msg.v ?? 0),
      items: [...acc.items, msg]
    }),
    { count: 0, sum: 0, items: [] }
  )
  .foreach((window, ctx) => {
    console.log('window', window, ctx)
  })
  .run({
    queryId:       'examples.stream-window',
    url,
    batchSize:     50,
    maxPartitions: 1,
    reset:         true
  })


// 4) Let everything drain (events take 6s to produce + idle flush trails).
await new Promise(r => setTimeout(r, 15_000))
console.log('metrics:', stream.metrics())
await stream.stop()
await q.close()
