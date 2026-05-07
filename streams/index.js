/**
 * @queenmq/streams — fat-client streaming engine on top of Queen MQ.
 *
 * Public surface:
 *   import { Stream } from '@queenmq/streams'
 *
 *   await Stream
 *     .from(queen.queue('orders'))
 *     .windowTumbling({ seconds: 60 })
 *     .aggregate({ sum: m => m.data.amount })
 *     .to(queen.queue('orders.totals_per_minute'))
 *     .run({ queryId: 'orders.per_minute_sum' })
 */

export { Stream } from './src/Stream.js'
