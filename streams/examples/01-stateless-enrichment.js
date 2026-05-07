/**
 * Example 01 — Stateless enrichment.
 *
 * Read events from `events.raw`, enrich each one with derived fields, drop
 * heartbeats, and emit to `events.enriched`. No state, no windowing — just
 * a transactional outbox-style pipeline running in your Node process.
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node examples/01-stateless-enrichment.js
 */

import { Queen } from 'queen-mq'
import { Stream } from '@queenmq/streams'

const url = process.env.QUEEN_URL || 'http://localhost:6632'
const q = new Queen(url)

console.log('Starting stateless enrichment stream...')

const handle = await Stream
  .from(q.queue('events.raw'))
  .filter(m => m.data && m.data.type !== 'heartbeat')
  .map(m => ({
    ...m.data,
    receivedAt: m.createdAt,
    upperType: typeof m.data.type === 'string' ? m.data.type.toUpperCase() : null
  }))
  .to(q.queue('events.enriched'))
  .run({
    queryId: 'examples.events.enrich',
    url,
    batchSize: 200,
    maxPartitions: 4
  })

// Run for a minute, then drain.
await new Promise(r => setTimeout(r, 60_000))
console.log('Stopping...', handle.metrics())
await handle.stop()
await q.close()
