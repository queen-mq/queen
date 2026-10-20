/**
 * Reads by offset (queue.fetch) and partition discovery (queue.changedSince)
 * against a live broker.
 *
 * Run: node run.js fetchReadsEveryRecordByOffset
 *      node run.js changedSinceListsTheWrittenPartitions
 */

const QUEUE_FETCH = 'test-queue-v2-reads-fetch'
const QUEUE_CHANGED = 'test-queue-v2-reads-changed'

export async function fetchReadsEveryRecordByOffset(client) {
  // 1,200 partitions: more than one fetch call (1000 entries each).
  const partitions = Array.from({ length: 1200 }, (_, i) => `entity-${i}`)
  const perPartition = 3
  for (let n = 0; n < perPartition; n++) {
    const items = partitions.map(p => ({ partition: p, transactionId: `${p}-${n}`, data: { p, n } }))
    for (let i = 0; i < items.length; i += 500) {
      const chunk = items.slice(i, i + 500)
      // One push per partition group keeps the per-partition order of n.
      await Promise.all(chunk.map(x => client.queue(QUEUE_FETCH).partition(x.partition).push([{ transactionId: x.transactionId, data: x.data }])))
    }
  }

  const read = await client.queue(QUEUE_FETCH).fetch(partitions)
  if (read.size !== partitions.length) {
    return { success: false, message: `fetch answered ${read.size} partitions for ${partitions.length}` }
  }
  for (const p of partitions) {
    const part = read.get(p)
    const got = part.records.map(r => `${r.offset}:${r.payload.p}:${r.payload.n}:${r.transactionId}`)
    const want = Array.from({ length: perPartition }, (_, n) => `${n}:${p}:${n}:${p}-${n}`)
    if (got.join(',') !== want.join(',')) {
      return { success: false, message: `${p}: read ${got.join(',')}, expected ${want.join(',')}` }
    }
    if (part.highWatermark !== perPartition || part.nextOffset !== perPartition || part.logStartOffset !== 0) {
      return { success: false, message: `${p}: bounds ${JSON.stringify(part)}` }
    }
  }

  // A later read from nextOffset answers only what came after.
  await client.queue(QUEUE_FETCH).partition('entity-7').push([{ transactionId: 'entity-7-late', data: { late: true } }])
  const later = await client.queue(QUEUE_FETCH).fetch(['entity-7', 'entity-8'], {
    offsets: { 'entity-7': read.get('entity-7').nextOffset, 'entity-8': read.get('entity-8').nextOffset }
  })
  const late = later.get('entity-7').records
  if (late.length !== 1 || late[0].offset !== perPartition || late[0].payload.late !== true) {
    return { success: false, message: `read from nextOffset answered ${JSON.stringify(late)}` }
  }
  if (later.get('entity-8').records.length !== 0) {
    return { success: false, message: 'a partition with nothing new answered records' }
  }

  // The reads took no lease and moved no cursor: a pop still gets the message.
  const popped = await client.queue(QUEUE_FETCH).partition('entity-9').batch(10).wait(false).pop()
  if (popped.length !== perPartition) {
    return { success: false, message: `a pop after the fetch got ${popped.length} messages, expected ${perPartition}` }
  }

  const missing = await client.queue('test-queue-v2-reads-does-not-exist').fetch(['x'])
  if (missing.get('x').records.length !== 0) {
    return { success: false, message: 'a queue that does not exist answered records' }
  }
  return { success: true, message: `read ${partitions.length} partitions by offset` }
}

export async function changedSinceListsTheWrittenPartitions(client) {
  const before = new Date(Date.now() - 60_000)
  const names = Array.from({ length: 1100 }, (_, i) => `c-${i}`)
  for (let i = 0; i < names.length; i += 100) {
    await Promise.all(names.slice(i, i + 100).map(p => client.queue(QUEUE_CHANGED).partition(p).push([{ data: { p } }])))
  }

  // 1,100 partitions: two pages.
  const first = await client.queue(QUEUE_CHANGED).changedSince(before)
  const listed = new Set(first.partitions.map(p => p.name))
  const missing = names.filter(n => !listed.has(n))
  if (missing.length > 0) {
    return { success: false, message: `changedSince missed ${missing.length} partitions, e.g. ${missing.slice(0, 3)}` }
  }
  if (first.partitions.length !== names.length || typeof first.safeTime !== 'string') {
    return { success: false, message: `changedSince answered ${first.partitions.length} partitions, safeTime ${first.safeTime}` }
  }
  const one = first.partitions.find(p => p.name === 'c-5')
  if (one.lastOffset !== 0 || one.logStart !== 0 || typeof one.lastWriteAt !== 'string' || typeof one.id !== 'string') {
    return { success: false, message: `partition fields ${JSON.stringify(one)}` }
  }

  // From the safeTime of that pass, a write to one partition lists it, and
  // nothing written before that safeTime comes back.
  await client.queue(QUEUE_CHANGED).partition('c-42').push([{ data: { again: true } }])
  const next = await client.queue(QUEUE_CHANGED).changedSince(first.safeTime)
  if (!next.partitions.some(p => p.name === 'c-42' && p.lastOffset === 1)) {
    return { success: false, message: `c-42 not listed after its write: ${JSON.stringify(next.partitions.slice(0, 5))}` }
  }
  if (next.partitions.some(p => p.lastWriteAt < first.safeTime)) {
    return { success: false, message: 'a partition written before safeTime was listed' }
  }

  const all = await client.queue(QUEUE_CHANGED).changedSince()
  if (all.partitions.length !== names.length) {
    return { success: false, message: `changedSince() without since answered ${all.partitions.length}` }
  }
  const none = await client.queue('test-queue-v2-reads-does-not-exist').changedSince(before)
  if (none.partitions.length !== 0) {
    return { success: false, message: 'a queue that does not exist listed partitions' }
  }
  return { success: true, message: `listed ${names.length} partitions over two pages` }
}
