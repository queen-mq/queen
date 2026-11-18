/**
 * Checks that only hold with strictNullChecks on: `tsconfig.json` compiles
 * this file, `tsconfig.loose.json` excludes it. Never executed.
 */

import type { Message } from '../client-v2/index.js'
import { type Queen, Stream } from '../client-v2/index.js'

interface Order {
  amount: number
}

/** Fails to compile unless `A` and `B` are the same type. */
type Equal<A, B> = (<X>() => X extends A ? 1 : 2) extends <X>() => X extends B ? 1 : 2 ? true : false
function assertType<_T extends true>(): void {}

export function reduceFoldsTheMessageForAnUndefinedValue(client: Queen): void {
  const maybe = Stream.from(client.queue<Order>('orders'))
    .map((m) => (m.data.amount > 0 ? m.data.amount : undefined))
    .windowTumbling({ seconds: 1 })

  // an undefined value folds the message itself
  void maybe.reduce((acc, v) => {
    assertType<Equal<typeof v, number | Message<Order>>>()
    return acc + (typeof v === 'number' ? v : v.data.amount)
  }, 0)

  // a value that is never undefined folds as it is
  void Stream.from(client.queue<Order>('orders'))
    .map((m) => m.data.amount)
    .windowTumbling({ seconds: 1 })
    .reduce((acc, v) => {
      assertType<Equal<typeof v, number>>()
      return acc + v
    }, 0)
}
