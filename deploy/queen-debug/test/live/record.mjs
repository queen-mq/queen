// Records what a real, seeded cluster answers, for the unit tests to replay.
// Run after seed.mjs, against a throwaway three-node cluster:
//
//   node test/live/record.mjs http://127.0.0.1:17632,http://127.0.0.1:17633,http://127.0.0.1:17634
//
// The payloads are the seed's made-up people.

import { writeFileSync } from 'node:fs'

import { loadConfig } from '../../src/config.mjs'
import { buildServer } from '../../src/server.mjs'
import { keyOf, SAMPLE_CALLS } from '../helpers.mjs'

const brokers = process.argv[2]
if (!brokers) throw new Error('usage: node test/live/record.mjs <broker urls>')

const records = {}
const fetchImpl = async (url, init = {}) => {
  const res = await fetch(url, init)
  const body = await res.clone().text()
  records[keyOf(new URL(url), init.method ?? 'GET', init.body)] = {
    status: res.status,
    type: res.headers.get('content-type') ?? 'application/json',
    body: body.length > 200_000 ? body.slice(0, 200_000) : body,
  }
  return res
}

const config = loadConfig(['--brokers', brokers, '--read-messages', '--allow-probe'], {})
const { protocol } = buildServer(config, { fetchImpl })
let id = 0
for (const [name, args] of SAMPLE_CALLS) {
  const reply = await protocol.dispatch({ jsonrpc: '2.0', id: (id += 1), method: 'tools/call', params: { name, arguments: args } })
  if (reply.error) throw new Error(`${name}: ${reply.error.message}`)
  if (reply.result.isError) throw new Error(`${name}: ${reply.result.content[0].text}`)
}
const file = new URL('../fixtures/cluster.json', import.meta.url)
writeFileSync(file, `${JSON.stringify(records)}\n`)
console.log(`${Object.keys(records).length} answers recorded`)
