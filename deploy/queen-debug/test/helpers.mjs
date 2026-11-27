// Replays real broker answers (test/fixtures/cluster.json, see live/record.mjs)
// and records every request a tool makes.

import { readFileSync } from 'node:fs'

import { loadConfig } from '../src/config.mjs'
import { buildServer } from '../src/server.mjs'

export const BROKERS = 'http://127.0.0.1:17632,http://127.0.0.1:17633,http://127.0.0.1:17634'

/** One call of every tool, with arguments that hit the seeded state. */
export const SAMPLE_CALLS = [
  ['diagnose', {}],
  ['why_stuck', { queue: 'dbg.stuck' }],
  ['why_stuck', { queue: 'dbg.orders', group: 'shipping' }],
  ['why_stuck', { queue: 'dbg.unread' }],
  ['why_stuck', { queue: 'dbg.payments' }],
  ['get_message', { transactionId: 'payment-3', queue: 'dbg.payments' }],
  ['get_message', { transactionId: 'order-0' }],
  ['node_detail', { node: 'queen-1' }],
  ['node_logs', { node: 'queen-1' }],
  ['cluster_metrics', { minutes: 30 }],
  ['write_probe', {}],
  ['list_queues', {}],
  ['queue_detail', { queue: 'dbg.orders' }],
  ['queue_history', { queue: 'dbg.payments' }],
  ['queue_history', {}],
  ['list_groups', {}],
  ['group_detail', { group: 'worker-a' }],
  ['list_messages', { queue: 'dbg.orders', limit: 5 }],
  ['fetch_messages', { queue: 'dbg.orders', partition: 'customer-1', limit: 3 }],
  ['find_messages', { where: "data.country = 'IT'", queue: 'dbg.pay*', limit: 3 }],
  ['follow_trace', {}],
  ['follow_trace', { name: 'order-flow-1' }],
  ['dlq_summary', {}],
  ['dlq_summary', { queue: 'dbg.payments' }],
  ['list_dlq', { queue: 'dbg.payments', limit: 3 }],
  ['read_kv', {}],
  ['read_kv', { namespace: 'dbg' }],
  ['read_kv', { namespace: 'dbg', key: 'customer/1' }],
  ['list_timers', { queue: 'dbg.orders' }],
  ['list_timers', { queue: 'dbg.orders', timerKey: 'reminder-1' }],
  ['list_locks', {}],
  ['stream_state', {}],
  ['connector_status', {}],
]

const VOLATILE = new Set(['from', 'to'])

/** A request's identity, without the clock in its time window. */
export function keyOf(url, method, body) {
  const q = [...url.searchParams.entries()].filter(([k]) => !VOLATILE.has(k)).sort().map(([k, v]) => `${k}=${v}`).join('&')
  return `${url.host} ${method} ${url.pathname}${q ? `?${q}` : ''}${body ? ` ${body}` : ''}`
}

export const loadFixture = () => JSON.parse(readFileSync(new URL('./fixtures/cluster.json', import.meta.url), 'utf8'))

// Pods get their own addresses in the tests with Kubernetes: each answers what
// the recorded node at that position answered.
const POD_HOSTS = { '127.0.0.1:6632': '127.0.0.1:17632', '127.0.0.2:6632': '127.0.0.1:17633', '127.0.0.3:6632': '127.0.0.1:17634' }

export function replayFetch(fixture, requests = [], overrides = () => null) {
  return async (url, init = {}) => {
    const u = new URL(url)
    const method = init.method ?? 'GET'
    requests.push({ method, pathname: u.pathname, search: u.search, body: init.body, headers: init.headers, redirect: init.redirect })
    const forced = overrides(u, method)
    if (forced?.down) throw new TypeError('fetch failed')
    if (forced) return new Response(forced.body ?? null, { status: forced.status ?? 200, headers: { 'content-type': 'application/json' } })
    const recorded = new URL(u)
    if (POD_HOSTS[u.host]) recorded.host = POD_HOSTS[u.host]
    const key = keyOf(recorded, method, init.body)
    const anyNode = key.split(' ').slice(1).join(' ')
    const hit = fixture[key]
      // The same read from another node: any node of the cluster may answer.
      ?? Object.entries(fixture).find(([k]) => k.split(' ').slice(1).join(' ') === anyNode)?.[1]
    if (!hit) return new Response('{"error":"not in the fixture"}', { status: 404, headers: { 'content-type': 'application/json' } })
    return new Response(hit.body, { status: hit.status, headers: { 'content-type': hit.type } })
  }
}

export function serverOn(fixture, { flags = [], requests = [], overrides, kube = null } = {}) {
  // With pods, the brokers are found through Kubernetes, as in a real install.
  const config = kube
    ? loadConfig(['--namespace', 'queen', '--selector', 'run=queen', ...flags], { KUBERNETES_SERVICE_HOST: '10.0.0.1' })
    : loadConfig(['--brokers', BROKERS, ...flags], {})
  const logs = []
  const built = buildServer(config, { fetchImpl: replayFetch(fixture, requests, overrides), log: (e) => logs.push(e), kube })
  let id = 0
  const rpc = (method, params) => built.protocol.dispatch({ jsonrpc: '2.0', id: (id += 1), method, params })
  const call = async (name, args = {}) => {
    const reply = await rpc('tools/call', { name, arguments: args })
    if (reply.error) return { rpcError: reply.error }
    return { isError: Boolean(reply.result.isError), text: reply.result.content[0].text, json: JSON.parse(reply.result.content[0].text) }
  }
  return { ...built, rpc, call, requests, logs }
}
