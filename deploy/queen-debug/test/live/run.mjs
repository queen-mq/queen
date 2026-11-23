// The server, as a real process, against a real three-node cluster that this
// script starts, breaks on purpose and deletes. Needs Docker.
//
//   node test/live/run.mjs
//
// It refuses to start if anything already answers on its ports.

import assert from 'node:assert/strict'
import { execFileSync, spawn } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { createInterface } from 'node:readline'
import { fileURLToPath } from 'node:url'

import { seed } from './seed.mjs'

const COMPOSE = fileURLToPath(new URL('./compose.yaml', import.meta.url))
const BIN = fileURLToPath(new URL('../../bin/queen-debug.mjs', import.meta.url))
const PORTS = { 'queen-1': 17632, 'queen-2': 17633, 'queen-3': 17634 }
const urlOf = (n) => `http://127.0.0.1:${PORTS[n]}`
const BROKERS = Object.keys(PORTS).map(urlOf).join(',')
const HTTP_PORT = 17700
const env = { ...process.env, QUEEN_RAFT_TOKEN: randomBytes(32).toString('hex') }

const compose = (...args) => execFileSync('docker', ['compose', '-f', COMPOSE, ...args], { env, stdio: ['ignore', 'pipe', 'pipe'] }).toString()
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

let passed = 0
async function check(name, fn) {
  const started = Date.now()
  await fn()
  passed += 1
  console.log(`  ok  ${name} (${Date.now() - started} ms)`)
}

async function until(what, fn, timeoutMs = 60_000) {
  const end = Date.now() + timeoutMs
  let last
  while (Date.now() < end) {
    try {
      const value = await fn()
      if (value) return value
    } catch (e) {
      last = e
    }
    await sleep(300)
  }
  throw new Error(`timed out waiting for ${what}${last ? `: ${last.message}` : ''}`)
}

const health = async (node) => (await fetch(`${urlOf(node)}/health`, { signal: AbortSignal.timeout(1500) })).status

// ---- the two transports ----------------------------------------------------

function startHttp(extra = [], port = HTTP_PORT) {
  const child = spawn(process.execPath, [BIN, '--transport', 'http', '--listen', `127.0.0.1:${port}`, '--brokers', BROKERS, ...extra], { stdio: ['ignore', 'ignore', 'pipe'] })
  const logs = []
  createInterface({ input: child.stderr }).on('line', (l) => logs.push(l))
  let id = 0
  const rpc = async (method, params) => {
    const res = await fetch(`http://127.0.0.1:${port}/mcp`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream' },
      body: JSON.stringify({ jsonrpc: '2.0', id: (id += 1), method, params }),
    })
    assert.equal(res.status, 200)
    return res.json()
  }
  return { child, logs, rpc, call: toolCaller(rpc), port }
}

function startStdio(extra = []) {
  const child = spawn(process.execPath, [BIN, '--brokers', BROKERS, ...extra], { stdio: ['pipe', 'pipe', 'pipe'] })
  const waiting = new Map()
  createInterface({ input: child.stdout }).on('line', (line) => {
    const m = JSON.parse(line)
    waiting.get(m.id)?.(m)
    waiting.delete(m.id)
  })
  child.stderr.resume()
  let id = 0
  const rpc = (method, params) => new Promise((resolve) => {
    id += 1
    waiting.set(id, resolve)
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`)
  })
  return { child, rpc, call: toolCaller(rpc) }
}

function toolCaller(rpc) {
  return async (name, args = {}) => {
    const r = await rpc('tools/call', { name, arguments: args })
    assert.ok(!r.error, `${name}: ${JSON.stringify(r.error)}`)
    const json = JSON.parse(r.result.content[0].text)
    assert.ok(!r.result.isError, `${name}: ${r.result.content[0].text}`)
    return json
  }
}

// What a read must never change: each group's cursor and each queue's counts.
async function positions() {
  const groups = await (await fetch(`${urlOf('queen-1')}/api/v1/consumer-groups`)).json()
  const queues = await (await fetch(`${urlOf('queen-1')}/api/v1/resources/queues`)).json()
  const details = {}
  for (const g of groups) {
    const d = await (await fetch(`${urlOf('queen-1')}/api/v1/consumer-groups/${g.name}`)).json()
    details[g.name] = Object.fromEntries(Object.entries(d).map(([q, v]) => [q, v.partitions.map((p) => [p.partition, p.totalConsumed, p.offsetLag, p.leaseActive, p.workerId])]))
  }
  return { details, queues: queues.queues.map((q) => [q.name, q.messages]).sort() }
}

async function everyRead(c, seeded) {
  return [
    await c.call('diagnose'),
    await c.call('why_stuck', { queue: 'dbg.orders' }),
    await c.call('get_message', { transactionId: 'payment-3', queue: 'dbg.payments' }),
    await c.call('node_detail', { node: 'queen-1' }),
    await c.call('node_logs', { node: 'queen-1' }),
    await c.call('cluster_metrics', { minutes: 10 }),
    await c.call('list_queues'),
    await c.call('queue_detail', { queue: 'dbg.stuck' }),
    await c.call('queue_history', { queue: 'dbg.orders', minutes: 10 }),
    await c.call('list_groups'),
    await c.call('group_detail', { group: 'worker-a' }),
    await c.call('list_messages', { queue: 'dbg.stuck' }),
    await c.call('fetch_messages', { queue: 'dbg.stuck', partition: 'only' }),
    await c.call('find_messages', { transactionId: seeded.held.transactionId, queue: 'dbg.stuck' }),
    await c.call('follow_trace', { name: 'order-flow-1' }),
    await c.call('dlq_summary'),
    await c.call('list_dlq', { queue: 'dbg.payments' }),
    await c.call('read_kv', { namespace: 'dbg', key: 'customer/1' }),
    await c.call('list_timers', { queue: 'dbg.orders' }),
    await c.call('list_locks'),
    await c.call('stream_state'),
    await c.call('connector_status'),
  ]
}

async function main() {
  for (const port of [...Object.values(PORTS), HTTP_PORT]) {
    const taken = await fetch(`http://127.0.0.1:${port}/`, { signal: AbortSignal.timeout(500) }).then(() => true, (e) => e?.cause?.code !== 'ECONNREFUSED')
    if (taken) throw new Error(`port ${port} is in use: this test only runs on a free machine`)
  }

  console.log('Starting three nodes')
  compose('up', '-d')
  await until('a healthy cluster', async () => (await Promise.all(Object.keys(PORTS).map(health))).every((s) => s === 200))
  const seeded = await seed(urlOf('queen-1'))
  console.log('Waiting for the first metrics minute')
  await until('metrics', async () => {
    const r = await (await fetch(`${urlOf('queen-1')}/api/v1/analytics/queue-ops?queue=dbg.payments`)).json()
    return r.series?.some((p) => p.ackFailed > 0)
  }, 90_000)

  const http = startHttp()
  const stdio = startStdio()
  const bodies = startHttp(['--read-messages', '--mask', 'data.guest.email'], HTTP_PORT + 1)
  const probe = startHttp(['--allow-probe'], HTTP_PORT + 2)
  await until('the server', async () => (await fetch(`http://127.0.0.1:${HTTP_PORT}/healthz`)).ok)
  await until('the second server', async () => (await fetch(`http://127.0.0.1:${HTTP_PORT + 1}/healthz`)).ok)
  await until('the third server', async () => (await fetch(`http://127.0.0.1:${HTTP_PORT + 2}/healthz`)).ok)

  try {
    console.log('A healthy cluster')
    await check('both transports speak MCP', async () => {
      for (const c of [http, stdio]) {
        const r = await c.rpc('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'live', version: '0' } })
        assert.equal(r.result.serverInfo.name, 'queen-debug')
        assert.equal((await c.rpc('tools/list', {})).result.tools.length, 22)
      }
    })
    await check('every read leaves every cursor, lease and count as it was', async () => {
      const before = await positions()
      await everyRead(http, seeded)
      await everyRead(stdio, seeded)
      assert.deepEqual(await positions(), before)
    })
    await check('diagnose finds each seeded problem', async () => {
      const d = await http.call('diagnose')
      assert.equal(d.verdict, 'degraded')
      assert.deepEqual(d.nodes.map((n) => n.name).sort(), ['queen-1', 'queen-2', 'queen-3'])
      assert.ok(d.nodes.every((n) => n.ready))
      const about = (q) => d.findings.find((f) => f.queue === q)?.code
      assert.equal(about('dbg.unread'), 'queue_noReader')
      assert.equal(about('dbg.payments'), 'queue_acks')
    })
    await check('why_stuck names the silent worker', async () => {
      const r = await http.call('why_stuck', { queue: 'dbg.stuck' })
      assert.equal(r.causes[0].code, 'worker_holds_lease')
      assert.ok(r.causes[0].evidence.includes(seeded.held.leaseId), `${seeded.held.leaseId} not in: ${r.causes[0].evidence}`)
    })
    await check('why_stuck tells no reader from failing acks', async () => {
      assert.equal((await http.call('why_stuck', { queue: 'dbg.unread' })).causes[0].code, 'no_reader')
      assert.equal((await http.call('why_stuck', { queue: 'dbg.payments' })).causes[0].code, 'acks_failing')
    })
    await check('a dead letter is explained', async () => {
      const m = await http.call('get_message', { transactionId: 'payment-3' })
      assert.equal(m.message.status, 'dead_letter')
      assert.equal(m.message.retryCount, 1)
      const s = await http.call('dlq_summary', { queue: 'dbg.payments' })
      assert.deepEqual(s.queues[0].errors.map((e) => e.error).sort(), ['card declined for order <n>', 'gateway timeout after <n> ms'])
    })
    await check('search finds by payload field and by id', async () => {
      const byField = await http.call('find_messages', { where: "data.orderId = 9103", limit: 5 })
      assert.equal(byField.count, 1)
      assert.equal(byField.messages[0].transactionId, 'payment-3')
      const byId = await http.call('find_messages', { transactionId: 'payment-3', where: "data.country = 'IT'" })
      assert.equal(byId.count, 1)
    })
    await check('bodies are hidden by default', async () => {
      const text = JSON.stringify(await everyRead(http, seeded))
      assert.ok(!/example\.test|Test Guest/.test(text))
    })
    await check('turned on, bodies show and the mask holds', async () => {
      const m = await bodies.call('get_message', { transactionId: 'payment-3', queue: 'dbg.payments' })
      assert.equal(m.message.data.guest.name, 'Test Guest 103')
      assert.equal(m.message.data.guest.email, '[masked]')
      const text = JSON.stringify(await everyRead(bodies, seeded))
      assert.match(text, /Test Guest/)
      assert.ok(!/guest\d+@example\.test/.test(text), 'a masked email got out')
    })
    await check('locks, timers and KV read right', async () => {
      const l = await http.call('list_locks', { prefix: 'dbg-' })
      assert.deepEqual([...new Set(l.locks.map((x) => x.name))].sort(), ['dbg-pool', 'dbg-report'])
      assert.equal(l.locks.find((x) => x.name === 'dbg-report').owner, 'worker-7')
      assert.ok(l.locks.filter((x) => x.name === 'dbg-pool').every((x) => x.semaphore))
      const t = await bodies.call('list_timers', { queue: 'dbg.orders', timerKey: 'reminder-1' })
      assert.equal(t.timer.partition, 'customer-1')
      assert.deepEqual(t.timer.payload, { remind: 'guest1@example.test' })
      const k = await bodies.call('read_kv', { namespace: 'dbg', key: 'customer/1' })
      assert.equal(k.value.guest.email, '[masked]')
      assert.equal(k.value.step, 'paid')
    })
    await check('the probe commits, when turned on', async () => {
      const r = await probe.call('write_probe')
      assert.equal(r.commits, true)
      assert.equal(r.results.length, 2)
    })

    // The hard case: the node left is the last leader, which the broker does
    // not let serve the log search.
    const roles = await Promise.all(Object.keys(PORTS).map(async (n) => [n, (await (await fetch(`${urlOf(n)}/health`)).json()).raft.role]))
    const leader = roles.find(([, role]) => role === 'leader')[0]
    const [firstDown, secondDown] = Object.keys(PORTS).filter((n) => n !== leader)

    console.log(`One node down (${firstDown})`)
    compose('stop', '-t', '2', firstDown)
    await until('the others to notice', async () => {
      const d = await http.call('diagnose')
      return d.findings.some((f) => f.code === 'node_down' && f.node === firstDown) ? d : null
    })
    await check('diagnose: degraded, names the node', async () => {
      const d = await http.call('diagnose')
      assert.equal(d.verdict, 'degraded')
      assert.equal(d.nodes.length, 3)
      assert.equal(d.nodes.find((n) => n.name === firstDown).state, 'unreachable')
    })
    await check('reads and the probe still work', async () => {
      assert.equal((await http.call('list_dlq', { queue: 'dbg.payments' })).total, 6)
      assert.equal((await http.call('read_kv', { namespace: 'dbg', key: 'customer/1' })).found, true)
      assert.equal((await probe.call('write_probe')).commits, true)
    })

    console.log(`A majority down, the last leader (${leader}) left alone`)
    compose('stop', '-t', '2', secondDown)
    await until('the survivor to lose its quorum', async () => (await health(leader)) === 503, 30_000)
    await check('diagnose: critical, no quorum, the runbook', async () => {
      const d = await http.call('diagnose')
      assert.equal(d.verdict, 'critical')
      assert.equal(d.findings[0].code, 'no_quorum')
      assert.match(d.findings[0].runbook.url, /a-majority-is-down/)
      assert.equal(d.nodes.length, 3)
    })
    await check('messages, DLQ, groups and queues still read, and say whose view it is', async () => {
      const dlq = await http.call('list_dlq', { queue: 'dbg.payments' })
      assert.equal(dlq.total, 6)
      assert.equal(dlq.source.ready, false)
      assert.match(dlq.notes.join(' '), /without a quorum/)
      assert.equal((await http.call('get_message', { transactionId: 'payment-3', queue: 'dbg.payments' })).message.status, 'dead_letter')
      assert.ok((await http.call('list_messages', { queue: 'dbg.stuck' })).count > 0)
      assert.ok((await http.call('fetch_messages', { queue: 'dbg.stuck', partition: 'only' })).count > 0)
      assert.equal((await http.call('find_messages', { transactionId: 'payment-3' })).count, 1)
      assert.equal((await http.call('find_messages', { where: 'data.orderId = 9103', queue: 'dbg.pay*' })).messages[0].transactionId, 'payment-3')
      assert.ok((await http.call('list_queues')).queues.length >= 4)
      assert.ok((await http.call('list_groups')).groups.length >= 4)
      assert.ok((await http.call('group_detail', { group: 'worker-a' })).queues.length)
      assert.equal((await http.call('dlq_summary', { queue: 'dbg.payments' })).queues[0].deadLetters, 6)
    })
    await check('why_stuck puts the missing quorum first', async () => {
      assert.equal((await http.call('why_stuck', { queue: 'dbg.stuck' })).causes[0].code, 'no_quorum')
    })
    await check('state tools say they need a quorum, at once', async () => {
      const started = Date.now()
      for (const [n, a] of [['read_kv', {}], ['list_timers', { queue: 'dbg.orders' }], ['list_locks', {}], ['stream_state', {}]]) {
        assert.equal((await http.call(n, a)).unavailable, 'needs_quorum', n)
      }
      assert.ok(Date.now() - started < 5_000, `took ${Date.now() - started} ms`)
    })
    await check('the probe says it does not commit', async () => {
      const r = await probe.call('write_probe')
      assert.equal(r.commits, false)
    })

    console.log('Every node down')
    compose('stop', '-t', '2', leader)
    await check('diagnose still answers', async () => {
      const d = await http.call('diagnose')
      assert.equal(d.verdict, 'critical')
      assert.equal(d.findings[0].code, 'no_broker_answers')
    })

    console.log('Back up')
    compose('start', 'queen-1', 'queen-2', 'queen-3')
    await until('a healthy cluster again', async () => (await Promise.all(Object.keys(PORTS).map((n) => health(n).catch(() => 0)))).every((s) => s === 200), 90_000)
    await check('diagnose: no node problem left', async () => {
      const d = await until('a clean view', async () => {
        const v = await http.call('diagnose')
        return v.nodes.every((n) => n.ready) ? v : null
      })
      assert.notEqual(d.verdict, 'critical')
      assert.ok(!d.findings.some((f) => f.node))
    })
    await check('the call log holds no value anybody searched for', async () => {
      const text = http.logs.join('\n')
      assert.match(text, /"tool":"find_messages"/)
      assert.ok(!text.includes('9103'))
      assert.ok(!text.includes('payment-3'))
    })
  } finally {
    for (const c of [http, stdio, bodies, probe]) c.child.kill()
  }
  console.log(`\n${passed} checks passed`)
}

let failed = false
try {
  await main()
} catch (e) {
  failed = true
  console.error(`\nFAILED: ${e.stack ?? e}`)
} finally {
  try {
    if (failed) console.error(compose('ps', '--all').slice(0, 1200))
    compose('down', '-v', '-t', '2')
  } catch (e) {
    console.error(`could not remove the test cluster: ${e.message}`)
    failed = true
  }
}
process.exit(failed ? 1 : 0)
