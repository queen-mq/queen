import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { computed, ref } from 'vue'
import { compileScript, parse } from '@vue/compiler-sfc'

// Exercise the page's reactive counts and requests without mounting its charts.
const descriptor = parse(readFileSync(new URL('../src/views/QueueDetail.vue', import.meta.url), 'utf8')).descriptor
const names = ['partitions', 'partitionCount', 'totalMessages', 'consumedTotal', 'partitionData', 'partitionRows', 'partitionSeeds', 'fetchPartitions', 'fetchQueueDetail']
const declarations = compileScript(descriptor, { id: 'queue-detail-test' }).scriptSetupAst
  .filter(n => n.type === 'VariableDeclaration' && names.includes(n.declarations[0].id.name))
assert.equal(declarations.length, names.length)
const source = declarations.map(n => descriptor.scriptSetup.content.slice(n.start, n.end)).join('\n')

function page(data, overrides = {}) {
  const requests = []
  const deps = {
    computed, ref, MAX_SEEDS: 610, toNum: n => n == null ? null : Number(n),
    statusData: ref(data), queueName: ref('traces'), queueSev: ref('ok'),
    detailError: ref(null), detailStale: ref(null),
    analytics: { getQueueDetail: async (name, params) => { requests.push({ name, params }); return { data } } },
    getPartitions: async params => { requests.push(params); return { data: { partitions: [{ partition: 'one', pending: 2, lagSeconds: 3 }] } } },
    ...overrides,
  }
  const state = new Function(...Object.keys(deps), `${source}\nreturn { ${names.join(',')} }`)(...Object.values(deps))
  return { ...deps, ...state, requests }
}

const summary = (count, statsAge = 0) => ({
  statsAge, partitions: [], totals: { partitions: count, consumed: 12, messages: { total: 9, pending: 2, processing: 1, completed: 7, deadLetter: 0 } },
})

test('large queue loads a summary and draws aggregate seeds without a partition scan', async () => {
  const p = page(summary(100001, 5))
  await p.fetchQueueDetail()
  assert.deepEqual(p.requests, [{ name: 'traces', params: { includePartitions: false } }])
  assert.equal(p.partitionCount.value, 100001)
  assert.equal(p.consumedTotal.value, 12)
  assert.equal(p.totalMessages.value.total, 9)
  assert.equal(p.partitionRows.value, null)
  assert.deepEqual(p.partitionSeeds.value, [{ name: 'traces', partitions: 100001, pending: 2, sev: 'ok' }])
})

test('small queues load actual partition seeds; growth discards their old rows', async () => {
  const p = page(summary(610))
  await p.fetchQueueDetail()
  assert.deepEqual(p.requests[1], { queue: 'traces', limit: 610 })
  assert.deepEqual(p.partitionSeeds.value, [{ name: 'one', partitions: 1, pending: 2, lag: 3, sev: 'ok' }])
  p.statusData.value = summary(611)
  await p.fetchPartitions()
  assert.equal(p.requests.length, 2)
  assert.equal(p.partitionRows.value, null)
})

test('cold samples are unknown, and old brokers retain their detailed totals', async () => {
  const p = page(summary(null, null))
  await p.fetchQueueDetail()
  assert.equal(p.requests.length, 1)
  assert.equal(p.partitionCount.value, null)
  assert.equal(p.consumedTotal.value, null)
  assert.equal(p.totalMessages.value.total, null)
  assert.equal(p.totalMessages.value.pending, 2)
  p.statusData.value = { partitions: [{ messages: { total: 5, pending: 1 }, cursor: { totalConsumed: 4 } }] }
  assert.equal(p.partitionCount.value, 1)
  assert.equal(p.consumedTotal.value, 4)
  assert.equal(p.totalMessages.value.total, 5)
  p.statusData.value = summary(0)
  p.statusData.value.totals.consumed = 0
  assert.equal(p.partitionCount.value, 0)
  assert.equal(p.consumedTotal.value, 0)
})

test('a queue change ignores the previous queue response and its partition scan', async () => {
  let resolve
  const p = page(null, { analytics: { getQueueDetail: () => new Promise(r => { resolve = r }) } })
  const pending = p.fetchQueueDetail()
  p.queueName.value = 'other'
  resolve({ data: summary(1) })
  await pending
  assert.equal(p.statusData.value, null)
  assert.deepEqual(p.requests, [])
})
