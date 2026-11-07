// Wait until every stream has committed past the last pushed event; prints how long it took.
//   node wait.mjs [timeout seconds = 600]
import { client, QUERY, Q, log } from './common.mjs'
const queen = client()
const timeout = Number(process.argv[2] ?? 600) * 1000
const started = Date.now()
const probe = queen.feed({ queues: Object.values(Q), from: 'now', waitMillis: 0 })
const end = (await probe.probe()).next
await probe.close?.()
const positions = async () => {
  const out = {}
  for (const [name, id] of Object.entries(QUERY)) {
    // A sharded stream keeps one record per shard (its layout says how many): the slowest counts.
    const layout = await queen.kv.get('queen-feed', `${id}#layout`)
    const n = layout && layout.found ? layout.value?.shards ?? 1 : 1
    const rows = n > 1
      ? await Promise.all(Array.from({ length: n }, (_, s) => queen.kv.get('queen-feed', `${id}#shard${s}of${n}`)))
      : [await queen.kv.get('queen-feed', id)]
    const ps = rows.filter((r) => r && r.found && Number.isInteger(r.value?.position)).map((r) => r.value.position)
    out[name] = ps.length ? Math.min(...ps) : 0
  }
  return out
}
for (;;) {
  const p = await positions()
  if (Object.values(p).every((x) => x >= end)) {
    log(`all streams past position ${end} after ${((Date.now() - started) / 1000).toFixed(1)} s`)
    process.exit(0)
  }
  if (Date.now() - started > timeout) {
    log(`timed out: ${JSON.stringify(p)} of ${end}`)
    process.exit(1)
  }
  await new Promise((r) => setTimeout(r, 500))
}
