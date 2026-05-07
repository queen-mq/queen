/**
 * Example 04 — In-window dedup.
 *
 * Source `webhooks.raw` may contain duplicate `eventId`s within a 5-minute
 * window. We dedupe per partition and emit a single record per unique
 * eventId per window onto `webhooks.unique`.
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node examples/04-in-window-dedup.js
 */

import { Queen } from 'queen-mq'
import { Stream } from '@queenmq/streams'

const url = process.env.QUEEN_URL || 'http://localhost:6632'
const q = new Queen(url)

const handle = await Stream
  .from(q.queue('webhooks.raw'))
  .windowTumbling({ seconds: 300 })
  .reduce(
    (seen, m) => {
      const id = m.data && m.data.eventId
      if (!id) return seen
      if (!seen.includes(id)) seen.push(id)
      return seen
    },
    /* initial seen = */ []
  )
  .flatMap(seen => seen.map(eventId => ({ eventId })))
  .to(q.queue('webhooks.unique'))
  .run({
    queryId: 'examples.webhooks.dedup_5min',
    url
  })

await new Promise(r => setTimeout(r, 10 * 60_000))
console.log('Stats:', handle.metrics())
await handle.stop()
await q.close()
