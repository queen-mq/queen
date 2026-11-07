// The streams, killed with SIGKILL partway and started again, then checked.
//
//   GH_IDLE=off node killtest.mjs [--at 0.4]
//
// Starts streams.mjs, waits until the slowest stream has committed `at` of
// the way through the pushed events, kills the process (no stop, no last
// commit), starts a new one, waits until every stream is past the end,
// stops it, and runs check.mjs: the results must be exactly the batch ones,
// each sent once.
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { client, QUERY, Q, log } from './common.mjs'

const args = process.argv.slice(2)
const at = args.includes('--at') ? Number(args[args.indexOf('--at') + 1]) : 0.4
const here = path.dirname(fileURLToPath(import.meta.url))
const queen = client()

const probe = queen.feed({ queues: Object.values(Q), from: 'now', waitMillis: 0 })
const end = (await probe.probe()).next
const startProbe = queen.feed({ queues: Object.values(Q), from: 'earliest', waitMillis: 0 })
const first = (await startProbe.read({ waitMillis: 0 })).entries[0]?.position ?? 0
await probe.close?.()
await startProbe.close?.()

async function slowest() {
  let min = Infinity
  for (const id of Object.values(QUERY)) {
    const layout = await queen.kv.get('queen-feed', `${id}#layout`)
    const n = layout && layout.found ? layout.value?.shards ?? 1 : 1
    const rows = n > 1
      ? await Promise.all(Array.from({ length: n }, (_, s) => queen.kv.get('queen-feed', `${id}#shard${s}of${n}`)))
      : [await queen.kv.get('queen-feed', id)]
    for (const r of rows) min = Math.min(min, r && r.found && Number.isInteger(r.value?.position) ? r.value.position : 0)
  }
  return min
}
const run = (name) => {
  const c = spawn(process.execPath, [path.join(here, 'streams.mjs'), ...args.filter((a, i) => a !== '--at' && args[i - 1] !== '--at')], { stdio: ['ignore', 'pipe', 'inherit'], env: process.env })
  c.stdout.on('data', (d) => process.stdout.write(`[${name}] ${d}`))
  return c
}
const until = async (cond, what, ms = 1800000) => {
  const t0 = Date.now()
  while (!(await cond())) {
    if (Date.now() - t0 > ms) throw new Error(`timed out waiting for ${what}`)
    await new Promise((r) => setTimeout(r, 250))
  }
}

const t0 = Date.now()
const target = first + Math.floor((end - first) * at)
log(`positions ${first}..${end}: killing once every stream is past ${target}`)
const a = run('first')
await until(async () => (await slowest()) >= target, 'the first process to get there')
a.kill('SIGKILL')
const killedAt = Date.now()
log(`SIGKILL after ${((killedAt - t0) / 1000).toFixed(1)} s`)
const b = run('second')
await until(async () => (await slowest()) >= end, 'the second process to finish')
log(`second process past the end ${((Date.now() - killedAt) / 1000).toFixed(1)} s after the kill (lock takeover, snapshot load and replay included); total ${((Date.now() - t0) / 1000).toFixed(1)} s`)
b.kill('SIGTERM')
await new Promise((r) => b.once('exit', r))
const check = spawn(process.execPath, [path.join(here, 'check.mjs')], { stdio: 'inherit', env: process.env })
check.once('exit', (code) => process.exit(code ?? 1))
