/**
 * Minimal fake of the /streams/v1/* endpoints, plus a fake QueueBuilder
 * that mimics queen-mq's pop() semantics. Used by the cycle/runner tests.
 */

import { createServer } from 'node:http'
import { randomUUID } from 'node:crypto'

export function createFakeStreamsServer({ stateSeeds } = {}) {
  const recorded = {
    registers: [],
    cycles: [],
    stateGets: []
  }
  const queries = new Map()        // name -> { id, config_hash }
  // state: `${queryId}|${partitionId}|${key}` -> value
  const state = stateSeeds instanceof Map
    ? new Map(stateSeeds)
    : (stateSeeds && typeof stateSeeds === 'object'
        ? new Map(Object.entries(stateSeeds))
        : new Map())

  const server = createServer((req, res) => {
    let body = ''
    req.on('data', chunk => { body += chunk })
    req.on('end', () => {
      let parsed = {}
      try { parsed = body ? JSON.parse(body) : {} } catch { parsed = {} }

      const send = (status, payload) => {
        res.writeHead(status, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(payload))
      }

      if (req.method === 'POST' && req.url === '/streams/v1/queries') {
        recorded.registers.push(parsed)
        const existing = queries.get(parsed.name)
        if (existing) {
          if (existing.config_hash !== parsed.config_hash && !parsed.reset) {
            return send(409, { success: false, error: 'config_hash mismatch' })
          }
          if (parsed.reset) {
            for (const k of state.keys()) {
              if (k.startsWith(existing.id + '|')) state.delete(k)
            }
            existing.config_hash = parsed.config_hash
          }
          return send(200, {
            success: true,
            query_id: existing.id,
            name: parsed.name,
            config_hash: parsed.config_hash,
            fresh: false,
            reset: !!parsed.reset
          })
        }
        const id = randomUUID()
        queries.set(parsed.name, { id, config_hash: parsed.config_hash })
        return send(200, {
          success: true,
          query_id: id,
          name: parsed.name,
          config_hash: parsed.config_hash,
          fresh: true,
          reset: false
        })
      }

      if (req.method === 'POST' && req.url === '/streams/v1/state/get') {
        recorded.stateGets.push(parsed)
        const rows = []
        const prefix = `${parsed.query_id}|${parsed.partition_id}|`
        const keys = Array.isArray(parsed.keys) && parsed.keys.length > 0
          ? new Set(parsed.keys)
          : null
        for (const [k, v] of state.entries()) {
          if (!k.startsWith(prefix)) continue
          const stateKey = k.slice(prefix.length)
          if (keys && !keys.has(stateKey)) continue
          rows.push({ key: stateKey, value: v, updated_at: new Date().toISOString() })
        }
        return send(200, { success: true, rows })
      }

      if (req.method === 'POST' && req.url === '/streams/v1/cycle') {
        recorded.cycles.push(parsed)
        const queryId = parsed.query_id
        const partitionId = parsed.partition_id
        // Apply state ops
        for (const op of parsed.state_ops || []) {
          const k = `${queryId}|${partitionId}|${op.key}`
          if (op.type === 'upsert') state.set(k, op.value)
          else if (op.type === 'delete') state.delete(k)
        }
        return send(200, {
          success: true,
          query_id: queryId,
          partition_id: partitionId,
          state_ops_applied: (parsed.state_ops || []).length,
          push_results: (parsed.push_items || []).map(p => ({
            queueName: p.queue,
            messageId: randomUUID(),
            transactionId: p.transactionId || randomUUID()
          })),
          ack_result: parsed.ack ? { success: true, lease_released: true, dlq: false } : null
        })
      }

      send(404, { error: 'not found' })
    })
  })

  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address()
      resolve({
        url: `http://127.0.0.1:${addr.port}`,
        recorded,
        state,
        async close() {
          await new Promise(r => server.close(r))
        }
      })
    })
  })
}

/**
 * A fake QueueBuilder that mimics the methods Runner.js uses on the source:
 *   .batch(n).wait(b).timeoutMillis(ms).group(g).partitions(n).subscriptionMode(s).subscriptionFrom(t).pop()
 *
 * Construct with a list of "rounds" — each round is the array of messages
 * that the next pop() call returns. Once all rounds are exhausted, pop()
 * returns [] forever (idle).
 */
export function createFakeSource(name, rounds) {
  const queue = [...rounds]
  const builder = {
    _queueName: name,
    queueName: name
  }
  for (const m of ['batch', 'wait', 'timeoutMillis', 'group', 'partitions', 'subscriptionMode', 'subscriptionFrom']) {
    builder[m] = () => builder
  }
  builder.pop = async () => {
    if (queue.length === 0) return []
    return queue.shift()
  }
  return builder
}

let __nextMsgIdx = 0
export function fakeMessage({ partitionId, partitionName = 'Default', data, createdAt, transactionId }) {
  __nextMsgIdx++
  return {
    id: `msg-${__nextMsgIdx}`,
    transactionId: transactionId || `tx-${__nextMsgIdx}`,
    partitionId,
    partition: partitionName,
    leaseId: 'fake-lease',
    consumerGroup: 'streams.test',
    data,
    createdAt: createdAt || new Date().toISOString()
  }
}
