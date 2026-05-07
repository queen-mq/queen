/**
 * Example 05 — Per-entity hourly profile build.
 *
 * Demonstrates the "per-entity stateful pipeline" shape: source events are
 * partitioned by customerId, each customer's events arrive FIFO ordered,
 * we accumulate an hourly profile per customer, and on hour boundary we
 * write the profile to a database via .foreach (at-least-once).
 *
 * If you need exactly-once external effects, prefer .to(queue) and have a
 * separate worker drain the sink with a transactional ack into your DB.
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node examples/05-per-entity-hourly-profile.js
 */

import { Queen } from 'queen-mq'
import { Stream } from '@queenmq/streams'

const url = process.env.QUEEN_URL || 'http://localhost:6632'
const q = new Queen(url)

// Stand-in for your business DB write. In a real app this would be a
// pg.Client, a knex connection, or an HTTP call.
async function writeProfile(profile) {
  console.log('[example05] writing profile for', profile.customerId, '=>', JSON.stringify(profile))
  // await myPg.query('INSERT INTO customer_profiles ...', [...])
}

function mergeProfile(acc, m) {
  acc.customerId = m.data.customerId
  acc.events = (acc.events || 0) + 1
  acc.lastSeen = m.createdAt || new Date().toISOString()
  if (m.data.amount) {
    acc.totalAmount = (acc.totalAmount || 0) + m.data.amount
  }
  if (Array.isArray(m.data.tags)) {
    acc.tags = Array.from(new Set([...(acc.tags || []), ...m.data.tags]))
  }
  return acc
}

const handle = await Stream
  .from(q.queue('account.events'))
  // Default key = source partition_id (== customerId in this layout).
  .windowTumbling({ seconds: 3600 })
  .reduce(mergeProfile, { customerId: null, events: 0, totalAmount: 0, tags: [] })
  .foreach(profile => writeProfile(profile))
  .run({
    queryId: 'examples.customer.hourly_profile',
    url,
    batchSize: 100,
    maxPartitions: 4
  })

await new Promise(r => setTimeout(r, 60 * 60_000))
console.log('Stats:', handle.metrics())
await handle.stop()
await q.close()
