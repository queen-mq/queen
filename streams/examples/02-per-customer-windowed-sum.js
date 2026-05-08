/**
 * Example 02 — Per-customer windowed sum.
 *
 * Source `orders` is partitioned by customerId on push. The streaming query
 * computes a 1-minute tumbling sum of `amount` per customer and pushes the
 * result onto `orders.totals_per_customer_per_min`.
 *
 * Why this leverages Queen's partitioning:
 *   - Each customer's events are FIFO ordered (Queen partition guarantee).
 *   - Each customer's window state lives in a single (query_id,
 *     partition_id, windowKey) row, written by exactly one worker (the
 *     one holding the partition lease).
 *   - More partitions × more workers = linear scaling, no central coordinator.
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node examples/02-per-customer-windowed-sum.js
 */

import { Queen, Stream } from 'queen-mq'
const url = process.env.QUEEN_URL || 'http://localhost:6632'
const q = new Queen(url)

const handle = await Stream
  .from(q.queue('orders'))
  // No .keyBy() needed — default key is partition_id (== customerId).
  .windowTumbling({ seconds: 60 })
  .aggregate({
    count: () => 1,
    sum:   m => m.data.amount,
    avg:   m => m.data.amount,
    max:   m => m.data.amount
  })
  .to(q.queue('orders.totals_per_customer_per_min'))
  .run({
    queryId: 'examples.orders.per_customer_per_min',
    url,
    batchSize: 200,
    maxPartitions: 4
  })

await new Promise(r => setTimeout(r, 5 * 60_000))
console.log('Stats:', handle.metrics())
await handle.stop()
await q.close()
