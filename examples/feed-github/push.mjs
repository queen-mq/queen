// Push the prepared events into Queen, in the archive's order.
//
//   node push.mjs                 as fast as it goes (a backfill)
//   node push.mjs --speed 600     at 600x the day's own pace (an hour every 6 s)
//   node push.mjs --limit 200000  the first 200,000 events only
//
// One transaction per batch: up to 2,000 events covering at most 30 s of
// the day. The feed gives the messages of one transaction grouped by
// partition, so a batch must cover less time than the streams'
// allowedLateness (2 minutes) or its own reordering would make messages late.
// Each message's transaction id is GitHub's event id: pushing again writes
// nothing twice.
import fs from 'node:fs'
import path from 'node:path'
import readline from 'node:readline'
import { DATA, client, queueOf, partitionOf, log } from './common.mjs'

const args = process.argv.slice(2)
const opt = (name, dflt) => {
  const i = args.indexOf(`--${name}`)
  return i === -1 ? dflt : Number(args[i + 1])
}
const speed = opt('speed', 0)
const limit = opt('limit', Infinity)
const MAX_BATCH = 2000
const MAX_SPAN_MS = 30000

const queen = client()
const file = path.join(DATA, 'events.ndjson')
if (!fs.existsSync(file)) {
  console.error(`no ${file}: run ./fetch.sh and node prepare.mjs first`)
  process.exit(1)
}

let batch = []
let batchStart = null
let pushed = 0
let duplicates = 0
let firstEventAt = null
const started = Date.now()
let lastReport = started

async function send() {
  if (!batch.length) return
  if (speed > 0) {
    // The day's clock runs `speed` times faster than this one.
    const due = started + (Date.parse(batch[0].t) - firstEventAt) / speed
    const wait = due - Date.now()
    if (wait > 0) await new Promise((r) => setTimeout(r, wait))
  }
  const groups = new Map()
  for (const e of batch) {
    const q = queueOf(e.q)
    if (!groups.has(q)) groups.set(q, [])
    const { q: _q, id, ...data } = e
    groups.get(q).push({ partition: partitionOf(e), transactionId: `gh-${id}`, data })
  }
  const tx = queen.transaction()
  for (const [q, items] of groups) tx.queue(q).push(items)
  const res = await tx.commit()
  if (res && res.success === false) throw new Error(`transaction refused: ${res.reason} ${res.error ?? ''}`)
  for (const r of res?.results ?? []) {
    if (r?.type !== 'push') continue
    if (r.status === 'duplicate') duplicates++
    else if (r.status && r.status !== 'queued') throw new Error(`push refused: ${JSON.stringify(r)}`)
  }
  pushed += batch.length
  batch = []
  batchStart = null
  const now = Date.now()
  if (now - lastReport >= 5000) {
    lastReport = now
    log(`${pushed} pushed (${Math.round(pushed / ((now - started) / 1000))}/s), day at ${groupsTime()}`)
  }
}
let lastT = null
const groupsTime = () => lastT

const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity })
let n = 0
for await (const line of rl) {
  if (!line) continue
  if (n++ >= limit) break
  const e = JSON.parse(line)
  const t = Date.parse(e.t)
  if (firstEventAt === null) firstEventAt = t
  lastT = e.t
  if (batch.length && (batch.length >= MAX_BATCH || Math.abs(t - batchStart) > MAX_SPAN_MS)) await send()
  if (!batch.length) batchStart = t
  batch.push(e)
}
await send()
const secs = (Date.now() - started) / 1000
log(`done: ${pushed} events in ${secs.toFixed(1)} s (${Math.round(pushed / secs)}/s)${duplicates ? `, ${duplicates} already there` : ''}`)
process.exit(0)
