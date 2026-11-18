/**
 * Compile-only checks of the broker client's declarations, written the way
 * the integration suite (test-v2/*.js) and the README use the client.
 * Never executed: `tsc -p types-test` type-checks it. `@ts-expect-error`
 * lines prove a type is not silently `any`.
 */

import type {
  AckBatchResult,
  AckSingleResult,
  BufferedPushResult,
  FeedEntry,
  KvRow,
  KvWriteResult,
  Message,
  PushItemResult,
  QueueConfig,
  QueueStreamHandle,
  RenewResult,
  TransactionResult
} from '../client-v2/index.js'
import {
  Admin,
  CLIENT_DEFAULTS,
  CLIENT_TOKEN_PREFIX,
  CONFLATION_UNSUPPORTED,
  EPHEMERAL_QUEUE_NOT_FOUND,
  EPHEMERAL_UNSUPPORTED,
  expiresAtMs,
  FILTERED_GROUPS_UNSUPPORTED,
  LOCK_NOT_HELD,
  type Lock,
  QUEUE_DEFAULTS,
  Queen,
  READING_QUEUES_UNSUPPORTED,
  sinceToIso,
  TokenSource,
  whereCondition
} from '../client-v2/index.js'

interface OrderData {
  orderId: number
  amount: number
}

/** Fails to compile unless `A` and `B` are the same type. */
type Equal<A, B> = (<X>() => X extends A ? 1 : 2) extends <X>() => X extends B ? 1 : 2 ? true : false
function assertType<_T extends true>(): void {}

export async function construction(): Promise<void> {
  const a = new Queen('http://localhost:6632')
  const b = new Queen(['http://localhost:6632', 'http://localhost:6633'])
  const c = new Queen({
    urls: ['http://localhost:6632', 'http://localhost:6633'],
    loadBalancingStrategy: 'round-robin',
    bearerToken: async () => 'qt_token',
    handleSignals: false,
    retry429: { maxAttempts: 3 },
    logger: console
  })
  // @ts-expect-error not a strategy
  void new Queen({ url: 'http://x', loadBalancingStrategy: 'random' })
  // the README's TypeScript example
  const queen: Queen = new Queen('http://localhost:6632')
  const messages: Message<OrderData>[] = await queen.queue('orders').pop()
  void messages
  await Promise.all([a.close(), b.close(), c.close()])
}

export async function queues(client: Queen): Promise<void> {
  const config: QueueConfig = {
    leaseTime: 30,
    retryLimit: 5,
    retentionSeconds: null
  }
  const created = await client.queue('test-queue-v2').config(config).create()
  if (!created.configured) throw new Error('not configured')

  // a reading queue
  await client
    .queue('orders-it')
    .config({
      from: [{ queue: 'orders', where: [['data.country', '=', 'IT']] }],
      partitionBy: 'same',
      start: 'now'
    })
    .create()

  // @ts-expect-error unknown queue option
  void client.queue('q').config({ leaseTimeSeconds: 30 })

  const del = await client.queue('test-queue-v2').delete({ force: true })
  void del.deleted

  // onError turns a rejection into a value
  const op = await client
    .queue('q')
    .create()
    .onError(() => {})
  if ('success' in op && op.success === false) void op.error
}

export async function push(client: Queen): Promise<void> {
  const res = await client
    .queue('orders')
    .partition('customer-42')
    .push([{ data: { orderId: 9137, amount: 99.5 } }])
  assertType<Equal<typeof res, PushItemResult[]>>()
  if (res[0]?.status !== 'queued') throw new Error('not queued')

  // typed payloads, envelopes and bare objects
  const typed = client.queue<OrderData>('orders')
  await typed.push({
    data: { orderId: 1, amount: 2 },
    partition: 'p1',
    transactionId: 'tx-1'
  })
  await typed.push([
    { orderId: 1, amount: 2 },
    { payload: { orderId: 3, amount: 4 }, headers: { region: 'eu' } }
  ])
  // @ts-expect-error amount must be a number
  await typed.push({ data: { orderId: 1, amount: 'two' } })

  // buffered pushes resolve the buffer's count
  const buffered = await client
    .queue('q')
    .buffer({ messageCount: 100, timeMillis: 1000 })
    .push([{ data: {} }])
  assertType<Equal<typeof buffered, BufferedPushResult>>()

  // callbacks
  const withCallbacks = await client
    .queue('q')
    .push([{ data: { a: 1 } }])
    .onSuccess((items) => void items.length)
    .onDuplicate((items) => void items[0]?.transactionId)
    .onError((items, error) => void [items.length, error.results])
  assertType<Equal<typeof withCallbacks, PushItemResult[] | null>>()
}

export async function popAndAck(client: Queen): Promise<void> {
  const messages = await client.queue<OrderData>('orders').group('billing').batch(10).wait(false).pop()
  for (const m of messages) {
    const amount: number = m.data.amount
    void amount
    void [m.transactionId, m.partitionId, m.leaseId, m.consumerGroup, m.createdAt]
  }
  const { messages: popped, autopilot } = await client.queue('events').group('w').popResult()
  if (autopilot) void [autopilot.partitions, autopilot.batch, autopilot.waitMillis]

  const first = popped[0]
  if (!first) return
  const single: AckSingleResult = await client.ack(first, true)
  const nack: AckSingleResult = await client.ack(first, false, {
    error: 'reason'
  })
  const retry: AckSingleResult = await client.ack(first, 'retry', {
    group: 'workers'
  })
  const batch: AckBatchResult = await client.ack(popped, true)
  const mixed: AckBatchResult = await client.ack([
    { ...first, _status: 'completed' },
    { ...first, _status: 'failed', _error: 'bad' }
  ])
  void [single.success, nack, retry, batch.results, mixed]
  // @ts-expect-error not an ack status
  await client.ack(first, 'done')
  // @ts-expect-error a bare transactionId has no partitionId: the ack always fails
  await client.ack('txn-1', true)
  // @ts-expect-error an ack names its partition
  await client.ack({ transactionId: 'txn-1' }, true)
  await client.ack({ id: 'txn-1', partitionId: '42' }, true)

  const renewed: RenewResult = await client.renew(first)
  const byId: RenewResult = await client.renew('lease-id')
  // a list resolves one object, not an array, when no item has a lease id
  const renewedAll = await client.renew(popped)
  assertType<Equal<typeof renewedAll, RenewResult[] | RenewResult>>()
  const renewedCount = Array.isArray(renewedAll) ? renewedAll.filter((r) => r.success).length : 0
  void [renewed.success, renewedCount, byId.newExpiresAt]

  // filtered groups
  await client
    .queue('orders')
    .group('billing')
    .where('data.kind', 'in', ['order.paid'])
    .where('headers.region', '=', 'eu')
    .pop()
  await client.queue('orders').group('billing').where("data.kind = 'order.paid'").pop()
  // @ts-expect-error not an operator
  void client.queue('orders').group('billing').where('data.kind', '~', 'x')

  // subscription modes, 'new-only' as client-v2/README.md writes it
  await client
    .queue('q')
    .group('my-group')
    .subscriptionMode('new-only')
    .consume(async (msg) => {
      /* new only */ void msg
    })
  void client.queue('q').group('g').subscriptionMode('new')
  void client.queue('q').group('g').subscriptionMode('all')
  // @ts-expect-error not a subscription mode
  void client.queue('q').group('g').subscriptionMode('latest')
}

export async function consume(client: Queen): Promise<void> {
  // without .each() the handler gets the popped array
  await client
    .queue<OrderData>('tasks')
    .concurrency(10)
    .batch(20)
    .consume(async (msgs) => {
      for (const m of msgs) void m.data.orderId
    })

  // with .each() it gets one message, also after .each() in the chain
  await client
    .queue<OrderData>('tasks')
    .group('workers')
    .each()
    .autoAck(false)
    .consume(async (message) => {
      void message.data.amount
      await client.ack(message, true)
      if (message.trace)
        await message.trace({
          traceName: ['a', 'b'],
          eventType: 'info',
          data: { ok: true }
        })
    })

  await client
    .queue<OrderData>('tasks')
    // @ts-expect-error without .each() the handler gets an array
    .consume(async (message: Message<OrderData>) => void message)

  const stop = new AbortController()
  const consuming = Promise.resolve(
    client
      .queue('tasks')
      .group('workers')
      .each()
      .consume(async () => {}, { signal: stop.signal })
  )
  stop.abort()
  await consuming

  await client
    .queue('tasks')
    .each()
    .consume(async () => {})
    .onError(async (message, error) => {
      await client.ack(message, false, {
        error: error instanceof Error ? error.message : String(error)
      })
    })
}

export async function transactions(client: Queen, msg: Message<OrderData>, lock: Lock): Promise<void> {
  const res: TransactionResult = await client
    .transaction()
    .ack(msg)
    .queue('test-queue-v2-complete-next')
    .push([{ data: { message: 'Next', count: msg.data.orderId + 1 } }])
    .queue('manual-review')
    .partition(msg.partition)
    .link([msg], { headers: { 'review-reason': 'amount' } })
    .kv.put('saga', 'saga-1', { step: 'charged' }, { ttl: '24h' })
    .kv.incr('counters', 'orders', 1, { max: 1000, ttlSeconds: 3600 })
    .once('dedup', msg.transactionId, { ttl: '24h' })
    .timer('reminders')
    .key('order-1')
    .delay('30m')
    .payload({ orderId: 1 })
    .schedule()
    .guard(lock)
    .commit()
  if (res.success === false && res.reason === 'kv_precondition') return

  void client
    .transaction()
    // @ts-expect-error an expect on putIfAbsent is a contradiction
    .kv.putIfAbsent('ns', 'k', 1, { ttl: '1h', expect: 0 })
  // @ts-expect-error check needs expect
  void client.transaction().kv.check('ns', 'k', {})
}

export async function kvLocksTimers(client: Queen): Promise<void> {
  const row: KvRow<{ step: string }> = await client.kv.get('saga', 'saga-1')
  if (row.found) void row.value?.step
  const write: KvWriteResult = await client.kv.put('saga', 'saga-1', { step: 'paid' }, { ttl: '24h', expect: 3 })
  if (!write.applied) void write.reason
  const { won } = await client.kv.once('idem', 'order-1', { ttl: '24h' })
  void won
  const page = await client.kv.getPrefix('quota', 'acme:', {
    limit: 100,
    keysOnly: true
  })
  void page.nextAfter
  for await (const r of client.kv.listAll('quota', 'acme:')) void r.key
  const checked = await client.kv.check('orders', '9137', { expect: 3 })
  void checked.applied

  const lock = client.lock('daily-report', { ttl: '30s' })
  if (await lock.acquire({ wait: '10s' })) {
    void [lock.token, lock.held, lock.signal.aborted]
    await lock.release()
  }
  const gpu = client.semaphore('gpu', 4, { ttl: '2m' })
  const ran = await gpu.run(async () => 42, { wait: '30s' })
  if (ran.acquired) {
    const value: number = ran.value
    void value
  }
  lock.onLost((reason) => void reason)
  const holders = await client.locks.get('daily-report')
  void holders.holders.map((h) => h.owner)
  // @ts-expect-error a lock has one permit
  void client.lock('l', { ttl: '30s', limit: 2 })
  // @ts-expect-error a lock needs a lifetime
  void client.lock('l', { autoRenew: false })
  // @ts-expect-error one lifetime, not two
  void client.lock('l', { ttl: '30s', ttlSeconds: 30 })
  void client.lock('l', { ttlSeconds: 30, owner: 'worker-1' })

  // a KV write needs exactly one expiry
  await client.kv.put('saga', 's', { step: 'x' }, { forever: true })
  await client.kv.put('saga', 's', { step: 'x' }, { until: new Date() })
  // @ts-expect-error a KV write needs an expiry
  await client.kv.put('saga', 's', { step: 'x' }, {})
  // @ts-expect-error a KV write needs an expiry
  await client.kv.once('idem', 'order-1', { value: 1 })
  // @ts-expect-error one expiry, not two
  await client.kv.putIfAbsent('m', 'k', true, { ttl: '1h', forever: true })
  // @ts-expect-error incr writes too: its TTL applies when it creates the key
  await client.kv.incr('counters', 'orders', 1, { max: 10 })
  await client.kv.incr('counters', 'orders', undefined, { ttlSeconds: 60 })
  await client.kv.delete('saga', 's')
  void client
    .transaction()
    // @ts-expect-error the same rule inside a transaction
    .kv.put('saga', 's', 1, { required: true })

  const scheduled = await client.timer('orders').key('order-1').delay('30m').payload({ orderId: 1 }).schedule()
  void [scheduled.ok, scheduled.status]
  const cancelled = await client.timer('orders').key('order-1').cancel()
  void cancelled.status
  const pending = await client.timer('orders').list({ limit: 50 })
  void pending.rows
}

export async function feedReadsAndMore(client: Queen): Promise<void> {
  for await (const e of client.feed<OrderData>({
    partitions: ['booking-9137'],
    since: '1d',
    follow: false
  })) {
    const entry: FeedEntry<OrderData> = e
    void [entry.position, entry.queue, entry.data.amount]
  }
  const reader = client.feed({
    queues: ['orders'],
    where: [['data.country', '=', 'IT']],
    from: 'earliest'
  })
  const page = await reader.read({ waitMillis: 0 })
  void [page.next, page.caughtUp]
  await reader.close()

  const read = await client
    .queue<OrderData>('orders')
    .fetch(['customer-42', 'customer-7'], { offsets: { 'customer-42': 0 } })
  const records = read.get('customer-42')?.records ?? []
  void records.map((r) => r.payload.amount)
  const { partitions, safeTime } = await client.queue('orders').changedSince(new Date())
  void [partitions.map((p) => p.name), safeTime]

  const dlq = await client.queue('q').dlq('consumer-group').limit(10).from('2025-01-01').get()
  void [dlq.total, dlq.messages]

  const eph = await client.ephemeral.pop<{ hello: string }>('inbox:7', {
    wait: true,
    group: 'workers'
  })
  void eph.messages.map((m) => m.payload.hello)
  await client.ephemeral.ack('inbox:7', eph.messages, { group: 'workers' })
  await client.ephemeral.push('presence', [{ user: 'a', typing: true }], {
    partition: 'room-7'
  })
  // @ts-expect-error an option ephemeral queues do not know
  await client.ephemeral.configure('q', { ttlSecond: 5 })

  const depth = await client.admin.getQueueDepth('orders', 'billing')
  void depth.partitions.map((p) => p.pending)
  await client.admin.seekConsumerGroup('audit', 'orders', {
    timestamp: new Date().toISOString()
  })
  const overview = await client.admin.getOverview()
  // @ts-expect-error undocumented admin answers are unknown: narrow them first
  void overview.queues
  void (client.admin instanceof Admin)

  const { token } = await client.tokens.mint({
    sub: 'dev-8f3a',
    ttlSeconds: 3600
  })
  void token
}

export function constants(): void {
  const codes: string[] = [
    LOCK_NOT_HELD,
    EPHEMERAL_UNSUPPORTED,
    EPHEMERAL_QUEUE_NOT_FOUND,
    CONFLATION_UNSUPPORTED,
    FILTERED_GROUPS_UNSUPPORTED,
    READING_QUEUES_UNSUPPORTED,
    CLIENT_TOKEN_PREFIX
  ]
  assertType<Equal<typeof LOCK_NOT_HELD, 'LOCK_NOT_HELD'>>()
  void [codes, CLIENT_DEFAULTS.timeoutMillis, QUEUE_DEFAULTS.leaseTime, expiresAtMs('qt_x')]
  void new TokenSource('key').configured
  void whereCondition('data.total', '>', 100)
  void sinceToIso('15m')
}

export type { QueueStreamHandle }
