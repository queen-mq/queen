/**
 * Compile-only checks of the Stream SDK's declarations, written the way the
 * stream integration suite (test-v2/stream/*.js) and the README use it.
 * Never executed: `tsc -p types-test` type-checks it.
 */

import type { FeedStreamHandle, Message, QueueStreamHandle, StreamContext } from '../client-v2/index.js'
import { type Queen, Stream, slidingWindowGate, tokenBucketGate } from '../client-v2/index.js'

interface Order {
  orderId: number
  amount: number
  country: string
}

const STREAMS_URL = 'http://localhost:6632'

export async function queueSource(client: Queen): Promise<void> {
  // map/filter see the source message; the value flows to the sink
  const handle = await Stream.from(client.queue<Order>('orders'))
    .filter((m) => m.data.amount > 10)
    .map((m) => ({ id: m.data.orderId, total: m.data.amount }))
    .to(client.queue('orders-big'))
    .run({
      queryId: 'orders-big',
      url: STREAMS_URL,
      batchSize: 50,
      reset: true,
      subscriptionMode: 'all'
    })
  const typed: QueueStreamHandle = handle
  void typed.metrics().messagesTotal
  await handle.stop()

  // windows: reduce folds the value, foreach gets the aggregate and the window
  const sums: number[] = []
  const windowed = await Stream.from(client.queue<Order>('orders'))
    .map((m) => m.data.amount)
    .windowTumbling({ seconds: 1, gracePeriod: 0, idleFlushMs: 500 })
    .reduce((acc, amount) => acc + amount, 0)
    .foreach((total, ctx: StreamContext) => {
      sums.push(total)
      void [ctx.windowStart, ctx.partitionId]
    })
    .run({ queryId: 'sums', url: STREAMS_URL })
  await windowed.stop()

  // aggregate: one numeric field per extractor
  await Stream.from(client.queue<Order>('orders'))
    .keyBy((m) => m.data.country)
    .windowSliding({ size: 10, slide: 5 })
    .aggregate({
      count: () => 1,
      sum: (o) => o.amount,
      min: (o) => o.amount,
      max: (o) => o.amount,
      avg: (o) => o.amount
    })
    .map((agg) => ({ count: agg.count, min: agg.min ?? 0 }))
    .foreach((v) => void v.count)
    .run({ queryId: 'agg', url: STREAMS_URL })

  await Stream.from(client.queue<Order>('orders'))
    .windowSession({
      gap: 30,
      eventTime: (m) => m.createdAt,
      allowedLateness: '5m',
      onLate: 'drop'
    })
    .reduce((acc: Order[], o) => [...acc, o], [])
    .foreach((orders) => void orders.length)
    .run({ queryId: 'sessions', url: STREAMS_URL })

  // a map that may return undefined: reduce then folds the message itself,
  // so its value is `number | Message<Order>` (only `number` with
  // strictNullChecks off, where the annotation below is still accepted)
  void Stream.from(client.queue<Order>('orders'))
    .map((m) => (m.data.amount > 0 ? m.data.amount : undefined))
    .windowTumbling({ seconds: 1 })
    .reduce((acc: number, v: number | Message<Order>) => acc + (typeof v === 'number' ? v : v.data.amount), 0)

  await Stream.from(client.queue('ticks'))
    .windowCron({ every: 'minute' })
    .aggregate({ count: () => 1 })
    .to(client.queue('per-minute'))
    .run({ queryId: 'cron', url: STREAMS_URL })

  // gates
  await Stream.from(client.queue<Order>('requests'))
    .gate((req, ctx) => {
      ctx.state['tokens'] ??= 10
      void [req.amount, ctx.streamTimeMs, ctx.partitionId]
      return ctx.state['tokens'] > 0
    })
    .to(client.queue('allowed'))
    .run({ queryId: 'gate', url: STREAMS_URL, maxPartitions: 1 })
  void Stream.from(client.queue<Order>('requests')).gate(
    tokenBucketGate<Order>({
      capacity: 10,
      refillPerSec: 5,
      costFn: (o) => o.amount
    })
  )
  void Stream.from(client.queue('requests')).gate(slidingWindowGate({ limit: 100, windowSec: 60 }))

  // the runner passes subscriptionMode to the queue builder: the same modes
  void Stream.from(client.queue('orders')).run({
    queryId: 'new-only',
    url: STREAMS_URL,
    subscriptionMode: 'new-only'
  })
  // @ts-expect-error a queue source needs the broker url
  void Stream.from(client.queue('orders')).run({ queryId: 'no-url' })
  // @ts-expect-error not a cron unit
  void Stream.from(client.queue('ticks')).windowCron({ every: 'fortnight' })
}

export async function feedSource(client: Queen): Promise<void> {
  const handle = await Stream.fromFeed<Order>(client, { queues: ['orders'] })
    .where('data.country', '=', 'IT')
    .filter((m) => m.data.amount > 100)
    .to(client.queue('orders-it'))
    .run({ queryId: 'orders-it', from: 'earliest' })
  const typed: FeedStreamHandle = handle
  void typed.state('perTenant').get('tenant-42')
  await handle.stop()

  await Stream.fromFeed(client, { queues: ['orders', 'payments'] })
    .pattern({
      start: (m) => m.queue === 'orders',
      then: (m) => m.queue === 'payments',
      within: '15m',
      onMatch: (order, payment) => ({
        order: order.data.id,
        paid: payment.data.at
      }),
      onTimeout: (order) => ({ order: order.data.id, remind: true })
    })
    .to((value: unknown) => (value ? 'order-followups' : null))
    .run({ queryId: 'followups', shards: 2 })

  await Stream.fromFeed(client, { queues: ['orders', 'shipments'] })
    .join({
      left: (m) => m.queue === 'orders',
      right: (m) => m.queue === 'shipments',
      within: '2d'
    })
    .foreachBatch(async (items) => void items.map((it) => it.id), {
      maxItems: 500,
      maxWaitMillis: 1000
    })
    .run({ queryId: 'joined' })

  const view = Stream.view(client, 'reply-time')
  const got = await view.get('perTenant', 'tenant-42')
  if (got.found) void got.value
  void (await view.list('perTenant', { limit: 50 })).items
}
