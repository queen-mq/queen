/**
 * The README's "TypeScript Support" example, kept compiling. Never executed.
 */

import type { Message, QueueConfig } from '../client-v2/index.js'
import { Queen, Stream } from '../client-v2/index.js'

export async function readmeExample(): Promise<Message<OrderData>[]> {
  const queen = new Queen('http://localhost:6632')

  const config: QueueConfig = { leaseTime: 30, retryLimit: 5 }
  await queen.queue('orders').config(config).create()

  const orders = queen.queue<OrderData>('orders')
  await orders.push([{ data: { orderId: 9137, amount: 99.5 } }])
  const messages: Message<OrderData>[] = await orders.pop()

  await orders
    .group('billing')
    .each()
    .consume(async (message) => {
      console.log(message.data.amount)
    })

  await Stream.from(orders)
    .map((m) => m.data.amount)
    .windowTumbling({ seconds: 60 })
    .reduce((total, amount) => total + amount, 0)
    .foreach((total, ctx) => console.log(ctx.windowStart, total))
    .run({ queryId: 'order-totals', url: 'http://localhost:6632' })

  return messages
}

interface OrderData {
  orderId: number
  amount: number
}
