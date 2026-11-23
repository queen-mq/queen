// Real state for the live test: queues in every condition the tools must tell
// apart, written through the broker's own routes. Run it against a throwaway
// cluster only:
//
//   node test/live/seed.mjs http://127.0.0.1:17632
//
// Every name starts with `dbg`, and the payloads hold made-up people, so the
// masking checks have something to hide.

const OPTIONS = {
  leaseTime: 600,
  retryLimit: 1,
  priority: 0,
  delayedProcessing: 0,
  windowBuffer: 0,
  maxSize: 0,
  retentionSeconds: 0,
  completedRetentionSeconds: 0,
  encryptionEnabled: false,
}

async function call(base, method, path, body) {
  const res = await fetch(base + path, {
    method,
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const text = await res.text()
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${text.slice(0, 300)}`)
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

const configure = (base, queue, task, options = {}) =>
  call(base, 'POST', '/api/v1/configure', { queue, namespace: 'dbg', task, options: { ...OPTIONS, ...options } })

function push(base, queue, rows) {
  return call(base, 'POST', '/api/v1/push', {
    items: rows.map((r) => ({ queue, partition: r.partition, transactionId: r.id, payload: r.payload })),
  })
}

async function pop(base, queue, group, batch = 10) {
  const q = new URLSearchParams({ batch: String(batch), wait: 'false', consumerGroup: group, subscriptionMode: 'all' })
  const answer = await call(base, 'GET', `/api/v1/pop/queue/${encodeURIComponent(queue)}?${q}`)
  return answer?.messages || []
}

const ack = (base, m, group, status, error = null) =>
  call(base, 'POST', '/api/v1/ack', {
    transactionId: m.transactionId,
    partitionId: m.partitionId,
    leaseId: m.leaseId,
    consumerGroup: group,
    status,
    error,
  })

const person = (n) => ({
  orderId: 9000 + n,
  country: n % 2 ? 'IT' : 'FR',
  guest: { name: `Test Guest ${n}`, email: `guest${n}@example.test` },
})

/** Every message of a queue a group can still take, acked `completed`. */
async function drain(base, queue, group) {
  let taken = 0
  for (let round = 0; round < 50; round += 1) {
    const batch = await pop(base, queue, group, 50)
    if (!batch.length) return taken
    for (const m of batch) await ack(base, m, group, 'completed')
    taken += batch.length
  }
  return taken
}

export async function seed(base) {
  // A healthy group next to one that stopped after its first message.
  await configure(base, 'dbg.orders', 'orders')
  await push(base, 'dbg.orders', Array.from({ length: 24 }, (_, i) => ({
    partition: `customer-${(i % 4) + 1}`,
    id: `order-${i}`,
    payload: person(i),
  })))
  const billing = await drain(base, 'dbg.orders', 'billing')
  const [first] = await pop(base, 'dbg.orders', 'shipping', 1)
  await ack(base, first, 'shipping', 'completed')

  // Every message of this one fails until it is dead-lettered.
  await configure(base, 'dbg.payments', 'payments')
  await push(base, 'dbg.payments', Array.from({ length: 6 }, (_, i) => ({
    partition: `merchant-${(i % 2) + 1}`,
    id: `payment-${i}`,
    payload: person(100 + i),
  })))
  for (let round = 0; round < 60; round += 1) {
    const batch = await pop(base, 'dbg.payments', 'charger', 10)
    for (const m of batch) {
      const n = Number(m.transactionId.split('-')[1])
      await ack(base, m, 'charger', 'failed', n % 2 ? `card declined for order ${9100 + n}` : 'gateway timeout after 30000 ms')
    }
    const now = await call(base, 'GET', '/api/v1/status/queues/dbg.payments')
    if (now?.totals?.messages?.deadLetter === 6) break
    // A failed message comes back after the queue's retryDelay (1 s).
    await new Promise((resolve) => setTimeout(resolve, 250))
  }

  // A worker took a lease and went silent.
  await configure(base, 'dbg.stuck', 'stuck')
  await push(base, 'dbg.stuck', Array.from({ length: 3 }, (_, i) => ({
    partition: 'only',
    id: `stuck-${i}`,
    payload: person(200 + i),
  })))
  const held = await pop(base, 'dbg.stuck', 'worker-a', 1)

  // Messages nobody is set up to read.
  await configure(base, 'dbg.unread', 'unread')
  await push(base, 'dbg.unread', Array.from({ length: 4 }, (_, i) => ({
    partition: 'p',
    id: `unread-${i}`,
    payload: person(300 + i),
  })))

  // App state.
  await call(base, 'POST', '/api/v1/kv', {
    operations: [
      { op: 'put', ns: 'dbg', key: 'customer/1', value: { step: 'paid', guest: { email: 'guest1@example.test' } }, forever: true },
      { op: 'put', ns: 'dbg', key: 'customer/2', value: { step: 'new' }, ttlSeconds: 3600 },
    ],
  })
  await call(base, 'POST', '/api/v1/timers', {
    operations: [{
      op: 'schedule',
      queue: 'dbg.orders',
      timerKey: 'reminder-1',
      partition: 'customer-1',
      delayMs: 3_600_000,
      txn: 'timer-reminder-1',
      payload: Buffer.from(JSON.stringify({ remind: 'guest1@example.test' })).toString('base64'),
    }],
  })
  // One operation per lock name in a call: the pool's two holders are two calls.
  await call(base, 'POST', '/api/v1/locks', {
    operations: [
      { op: 'acquire', name: 'dbg-report', ttlSeconds: 600, owner: 'worker-7' },
      { op: 'acquire', name: 'dbg-pool', ttlSeconds: 600, owner: 'worker-1', limit: 3 },
    ],
  })
  await call(base, 'POST', '/api/v1/locks', {
    operations: [{ op: 'acquire', name: 'dbg-pool', ttlSeconds: 600, owner: 'worker-2', limit: 3 }],
  })

  // One business flow, recorded the way the SDKs record it.
  await call(base, 'POST', '/api/v1/traces', {
    transactionId: first.transactionId,
    partitionId: first.partitionId,
    consumerGroup: 'shipping',
    traceNames: ['order-flow-1'],
    eventType: 'info',
    data: { text: 'label printed', guest: { email: 'guest0@example.test' } },
  })

  return { billing, traced: first, held: held[0] }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const base = process.argv[2]
  if (!base) throw new Error('usage: node test/live/seed.mjs <broker url>')
  const out = await seed(base)
  console.log(JSON.stringify({ billing: out.billing, traced: out.traced.transactionId, held: out.held?.transactionId }))
}
