import { test } from 'node:test'
import assert from 'node:assert/strict'
import { ConsumerManager } from '../../client-v2/consumer/ConsumerManager.js'
import { Supervision } from '../../client-v2/consumer/Supervision.js'

const options = { queue: 'orders', concurrency: 2, timeoutMillis: 30000, limit: 1, batch: 1, each: true, autoAck: true, wait: false }
const decode = body => {
  assert.equal(body.operations.length, 2)
  const [head, chunk] = body.operations
  assert.equal(head.ns, 'queen-supervisor')
  assert.equal(head.ttlSeconds, 60)
  assert.equal(chunk.ttlSeconds, 60)
  assert.equal(head.value.write, chunk.value.write)
  const bytes = Buffer.from(chunk.value.data, 'base64')
  assert.equal(bytes.length, head.value.bytes)
  return JSON.parse(bytes)
}
function rig(fail = false) {
  const docs = [], acks = []
  const http = { get: async () => ({ messages: [{ transactionId: 't', partitionId: 'p', data: { secret: 42 } }] }),
    post: async (path, body) => { assert.equal(path, '/api/v1/kv'); docs.push(decode(body)); if (fail) throw new Error('offline'); return { results: [{ applied: true }, { applied: true }] } } }
  const queen = { ack: async (_, success) => { acks.push(success); return { success: true } } }
  return { docs, acks, http, manager: new ConsumerManager(http, queen) }
}
test('default and explicit off create no publications and preserve acknowledgement', async () => {
  for (const supervision of [undefined, false]) {
    const r = rig(); await r.manager.start(async () => {}, { ...options, supervision })
    assert.equal(r.docs.length, 0); assert.deepEqual(r.acks, [true, true])
  }
})
test('enabled consumers count actual exits, handler failures and final state without changing ACKs', async () => {
  const r = rig()
  let n = 0
  await r.manager.start(async () => { if (++n === 1) throw new Error('private error') }, { ...options, supervision: { group: 'billing-production' } })
  const last = r.docs.at(-1)
  assert.equal(last.state, 'stopped'); assert.equal(last.pool_status[0].running, 0)
  assert.equal(last.pool_status[0].busy, 0); assert.equal(last.pool_status[0].completed, 1)
  assert.equal(last.pool_status[0].failed, 1); assert.deepEqual(r.acks.sort(), [false, true])
  assert.equal(JSON.stringify(r.docs).includes('private error'), false)
  assert.equal(JSON.stringify(r.docs).includes('secret'), false)
})
test('publication errors do not affect consumption; instances are unique', async () => {
  const r = rig(true)
  await r.manager.start(async () => {}, { ...options, supervision: { group: 'billing' } })
  const first = r.docs[0].instance_id
  await r.manager.start(async () => {}, { ...options, supervision: { group: 'billing' } })
  assert.notEqual(first, r.docs.at(-1).instance_id); assert.equal(r.acks.length, 4)
})
test('busy handlers remain observable and publication is serialized', async () => {
  const r = rig(); const reporter = new Supervision(r.http, { group: 'billing' }, options)
  let release
  const work = reporter.wrap(() => new Promise(resolve => { release = resolve }))()
  reporter.running = 1
  await Promise.all([reporter.publish(), reporter.publish()])
  assert.equal(r.docs.length, 1); assert.equal(r.docs[0].pool_status[0].busy, 1)
  assert.equal(r.docs[0].pool_status[0].completed, 0)
  release(); await work
  assert.equal(reporter.document().pool_status[0].completed, 1)
  assert.equal(reporter.document().pool_status[0].oldest_inflight_seconds, null)
})
test('invalid opt-in groups fail before polling', async () => {
  for (const group of ['', 'coordination', 'a/b', 'billing\n', undefined]) {
    const r = rig(); await assert.rejects(r.manager.start(async () => {}, { ...options, supervision: { group } }), /supervision.group/)
    assert.equal(r.docs.length, 0)
  }
})

test('real HTTP publishing preserves authentication and has a total deadline', async () => {
  const { createServer } = await import('node:http')
  const { Queen } = await import('../../client-v2/index.js')
  let kvCalls = 0, polls = 0
  const docs = []
  const server = createServer((req, res) => {
    if (req.url === '/api/v1/kv') {
      kvCalls++
      assert.equal(req.headers.authorization, 'Bearer test-token')
      let bytes = ''
      req.on('data', chunk => { bytes += chunk })
      req.on('end', () => { docs.push(decode(JSON.parse(bytes))) })
      return // Deliberately never answer. Consumption and shutdown stay bounded.
    }
    polls++
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify({ messages: [{ transactionId: 't', partitionId: 'p', data: {} }] }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const queen = new Queen({ url: `http://127.0.0.1:${server.address().port}`, bearerToken: 'test-token', handleSignals: false })
  const start = performance.now()
  try {
    await queen.queue('orders').supervision({ group: 'wire' }).wait(false).autoAck(false).limit(1).consume(async () => {})
    assert.equal(polls, 1); assert.equal(kvCalls, 2)
    assert.equal(docs.at(-1).state, 'stopped')
    assert.ok(performance.now() - start < 6000, 'two publications are bounded by two seconds each')
  } finally {
    await queen.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve))
  }
})
