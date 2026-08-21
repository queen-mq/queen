// Conflation — last-value delivery for a consumer group (PLAN_CONFLATION).
//
// The workload: one partition is one logical key ("recompute entity X"), the
// producer is faster than the handler, and only the NEWEST request per key is
// worth executing. Without conflation the consumer grinds through every stale
// request. With it, a pop of a partition returns that partition's newest visible
// message and commits everything behind it when the handler acks.
//
// Three things this example is trying to show, in order:
//   1. what the consumer actually receives under a backlog;
//   2. that conflation is a property of the GROUP, so a second group on the same
//      queue can still read everything;
//   3. the guarantee that makes it safe — after the last push, the handler runs
//      at least once more.
//
// Run a broker on :6632, then:  node examples/35-conflation.js

import { Queen } from '../clients/client-js/client-v2/index.js'

const queen = new Queen('http://localhost:6632')
const queueName = `example-conflation-${Date.now()}`

await queen.queue(queueName).create()

// Two "entities" = two partitions. One partition is one logical key: that is the
// contract conflation is defined against, not something the broker infers.
const KEYS = ['entity-a', 'entity-b']

console.log(`Pushing 50 recompute requests per key into ${queueName}...`)
for (const key of KEYS) {
  // The partition is set on the BUILDER, not per item: one builder chain writes
  // one lane, which is the same shape the ordering guarantee is defined on.
  const items = []
  for (let i = 1; i <= 50; i++) items.push({ data: { key, version: i } })
  await queen.queue(queueName).partition(key).push(items)
}
console.log('Pushed 100 messages across 2 partitions\n')

// =============================================================================
// 1. The conflating consumer
// =============================================================================
// `.conflation()` is declared on the consume/pop builder and persisted the first
// time this group registers on the queue. From then on the STORED value wins for
// every consumer of the group, so this is a decision about the group, not about
// this call. `subscriptionMode('all')` makes the backlog visible in the first
// place; conflation then collapses it.
//
// partitions(64) matters: `partitions` defaults to 1 and a conflating pop
// yields at most one message per partition, so the partition cap — not `batch` —
// is what sizes a round trip. 64 is the ceiling the broker enforces.
console.log('=== Conflating group: expect 1 message per key, the newest ===')
const seen = []
await queen.queue(queueName)
  .group('recompute')
  .conflation()
  .subscriptionMode('all')
  .partitions(64)
  .wait(false)
  .limit(2)
  .each()
  .consume(async (message) => {
    seen.push(message.data)
    console.log(`  handled ${message.data.key} version ${message.data.version}`)
  })

console.log(`Handler ran ${seen.length} times for 100 messages`)
console.log(`Versions handled: ${seen.map((d) => d.version).join(', ')} (expected 50, 50)\n`)

// =============================================================================
// 2. Another group on the same queue reads everything
// =============================================================================
// This is why conflation is per group and not per queue: `audit` never declared
// it, so its own cursor still walks every offset.
console.log('=== Non-conflating group on the SAME queue ===')
let auditCount = 0
await queen.queue(queueName)
  .group('audit')
  .subscriptionMode('all')
  .batch(200)
  .partitions(64)
  .wait(false)
  .limit(100)
  .each()
  .consume(async () => { auditCount++ })

console.log(`audit received ${auditCount} messages (expected 100)\n`)

// =============================================================================
// 3. The guarantee
// =============================================================================
// "After the last push to a partition, at least one run of that partition's
// handler STARTS after that push commits." The broker never commits past an
// offset it did not observe at pop time, so anything that became visible after
// the claim is still pending and the next pop delivers it.
console.log('=== The guarantee: the last push is always handled ===')
await queen.queue(queueName).partition('entity-a').push(
  Array.from({ length: 25 }, (_, i) => ({ data: { key: 'entity-a', version: 100 + i } }))
)
const after = []
await queen.queue(queueName)
  .group('recompute')
  .conflation()
  .partitions(64)
  .wait(false)
  .limit(1)
  .each()
  .consume(async (message) => { after.push(message.data.version) })

console.log(`  handled version ${after[0]} of the 25 just pushed (expected 124, the newest)\n`)

// =============================================================================
// 4. Reading the backlog of a conflating group
// =============================================================================
// `pending` is LOG depth: positions still to retire, most of which conflation
// will skip. `effectivePending` is WORK depth: the handler runs that remain. On a
// conflating group these are wildly different numbers, and only the second one is
// an incident when it grows. Build a real backlog first, or the two agree at zero
// and the point is lost.
console.log('=== Depth: log depth vs work depth ===')
for (const key of KEYS) {
  const items = []
  for (let i = 1; i <= 500; i++) items.push({ data: { key, version: 1000 + i } })
  await queen.queue(queueName).partition(key).push(items)
}
const depth = await queen.admin.getQueueDepth(queueName, 'recompute')
console.log(`  conflation:                     ${depth.conflation}`)
console.log(`  pending (log depth):            ${depth.pending}`)
console.log(`  partitionsPending:              ${depth.partitionsPending}`)
console.log(`  effectivePending (work depth):  ${depth.effectivePending}`)
console.log('  -> a thousand pending positions, two handler runs to do\n')

// =============================================================================
// 5. What conflation refuses, and why
// =============================================================================
// Both of these are consumer bugs whose silent form is unfixable in production,
// so the broker answers 400 rather than warning. Shown at the wire because that
// is where the contract lives (and because this SDK's pop() cannot express
// autoAck=true: it reads `true` as the consume default and sends nothing).
console.log('=== Refusals (HTTP 400) ===')
for (const [label, qs] of [
  ['no consumer group', 'conflation=true'],
  ['with autoAck', `consumerGroup=recompute&conflation=true&autoAck=true`]
]) {
  const res = await fetch(`http://localhost:6632/api/v1/pop/queue/${queueName}?${qs}`)
  const body = await res.json()
  console.log(`  ${label.padEnd(18)} -> ${res.status}: ${body.error.slice(0, 80)}...`)
}

// A consumer that asks for conflation and does not get it does NOT quietly
// process the backlog one message at a time: every SDK raises on the first
// response that carries no conflation echo, including an empty one. Against a
// broker older than 1.1.0 the calls above would throw
// "conflation was requested but this broker did not apply it".

await queen.queue(queueName).delete()
await queen.close()
console.log('\nDone.')
