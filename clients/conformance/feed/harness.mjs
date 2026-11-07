#!/usr/bin/env node
/**
 * Runs the feed-stream conformance cases against one SDK's runner and
 * compares what it did with what the JS SDK did (expected/). See README.md.
 *
 *   node harness.mjs --sdk js --write                 regenerate expected/ (JS is the reference)
 *   node harness.mjs --sdk python [--case <name>]     compare
 *   node harness.mjs --sdk go --keep                  keep the result files (results/<sdk>/)
 *   node harness.mjs --sdk rust --live http://127.0.0.1:17872 [--cross js,rust]
 *
 * Fake mode starts fake-broker.mjs in this process. Live mode pushes through
 * the JS client to a real broker, skips `clock` steps and runs only the cases
 * marked live/both.
 */
import { readFileSync, writeFileSync, readdirSync, mkdirSync, existsSync } from 'node:fs'
import { spawn, spawnSync } from 'node:child_process'
import { createHash, randomBytes } from 'node:crypto'
import { inflateRawSync } from 'node:zlib'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { serve } from './fake-broker.mjs'

const HERE = dirname(fileURLToPath(import.meta.url))
const REPO = resolve(HERE, '../../..')
const NS = 'queen-feed'
const sha = (s) => createHash('sha256').update(s).digest('hex')

function arg(name, fallback = undefined) {
  const i = process.argv.indexOf(`--${name}`)
  return i > 0 && i + 1 < process.argv.length ? process.argv[i + 1] : fallback
}
const flag = (name) => process.argv.includes(`--${name}`)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// ---- ids, as the runner makes them (FeedRunner.js) --------------------------------

export function outputPrefix(queryId) {
  return `fd-${sha(`queen-feed\n${queryId}`).slice(0, 8)}-`
}

function shardScope(queryId, shard) {
  return shard ? `${queryId}#shard${shard.index}of${shard.count}` : queryId
}

// ---- runners ------------------------------------------------------------------

function runnerSpec(sdk) {
  const all = JSON.parse(readFileSync(join(HERE, 'runners.json'), 'utf8'))
  const spec = all[sdk]
  if (!spec) throw new Error(`runners.json has no ${sdk}`)
  return spec
}

function buildRunner(sdk) {
  const spec = runnerSpec(sdk)
  if (!spec.build) return
  const cwd = resolve(REPO, spec.cwd || '.')
  const r = spawnSync(spec.build[0], spec.build.slice(1), { cwd, stdio: ['ignore', 'inherit', 'inherit'], env: { ...process.env, ...(spec.env || {}) } })
  if (r.status !== 0) throw new Error(`building the ${sdk} runner failed (${spec.build.join(' ')})`)
}

/** One runner process: JSON lines out, commands in. */
class RunnerProc {
  constructor(sdk, name, args, log) {
    const spec = runnerSpec(sdk)
    const cwd = resolve(REPO, spec.cwd || '.')
    this.name = name
    this.sdk = sdk
    this.lines = []
    this.waiters = []
    this.exited = null
    this.log = log
    this.p = spawn(spec.run[0], [...spec.run.slice(1), ...args], { cwd, stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, ...(spec.env || {}) } })
    let buf = ''
    this.p.stdout.on('data', (d) => {
      buf += d.toString('utf8')
      let i
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i).trim()
        buf = buf.slice(i + 1)
        if (!line) continue
        let ev
        try { ev = JSON.parse(line) } catch (_) { ev = { event: 'garbage', line } }
        this.lines.push(ev)
        this.log.push({ runner: name, ...ev })
        this.#check()
      }
    })
    this.stderr = ''
    this.p.stderr.on('data', (d) => { this.stderr += d.toString('utf8'); if (this.stderr.length > 200000) this.stderr = this.stderr.slice(-100000) })
    this.p.stdin.on('error', () => {})
    this.p.on('exit', (code, signal) => { this.exited = { code, signal }; this.#check() })
  }

  #check() {
    for (const w of [...this.waiters]) {
      const hit = this.lines.find((l, i) => i >= w.from && w.pred(l))
      if (hit || (this.exited && w.orExit)) {
        this.waiters = this.waiters.filter((x) => x !== w)
        w.resolve(hit || null)
      }
    }
  }

  /** The next line (after `from`) that matches, or null when the process exits first. */
  waitFor(pred, ms, what, from = 0) {
    return new Promise((resolve, reject) => {
      const w = { pred, from, resolve, orExit: true }
      this.waiters.push(w)
      this.#check()
      setTimeout(() => {
        if (this.waiters.includes(w)) {
          this.waiters = this.waiters.filter((x) => x !== w)
          reject(new Error(`runner ${this.name} (${this.sdk}): timed out after ${ms} ms waiting for ${what}\n--- stderr tail ---\n${this.stderr.slice(-3000)}`))
        }
      }, ms)
    })
  }

  send(cmd) {
    if (!this.exited) this.p.stdin.write(`${cmd}\n`)
  }

  async ask(cmd, event, ms = 10000) {
    const from = this.lines.length
    this.send(cmd)
    const got = await this.waitFor((l) => l.event === event, ms, `${event} after ${cmd}`, from)
    if (!got) throw new Error(`runner ${this.name} exited before answering ${cmd}\n${this.stderr.slice(-3000)}`)
    return got
  }

  kill() {
    if (!this.exited) this.p.kill('SIGKILL')
  }
}

function runOnce(sdk, args, ms = 20000) {
  return new Promise((done, reject) => {
    const spec = runnerSpec(sdk)
    const p = spawn(spec.run[0], [...spec.run.slice(1), ...args], { cwd: resolve(REPO, spec.cwd || '.'), stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, ...(spec.env || {}) } })
    let out = ''
    let err = ''
    p.stdout.on('data', (d) => { out += d })
    p.stderr.on('data', (d) => { err += d })
    const t = setTimeout(() => { p.kill('SIGKILL'); reject(new Error(`${sdk} view timed out\n${err.slice(-2000)}`)) }, ms)
    p.on('exit', () => {
      clearTimeout(t)
      const lines = out.split('\n').filter(Boolean).map((l) => { try { return JSON.parse(l) } catch (_) { return { event: 'garbage', line: l } } })
      done(lines.find((l) => l.event === 'view' || l.event === 'failed') || { event: 'failed', message: `no answer; stderr: ${err.slice(-500)}` })
    })
  })
}

// ---- brokers --------------------------------------------------------------------

class FakeBroker {
  constructor(url) { this.url = url; this.live = false }
  async call(method, path, body) {
    const r = await fetch(`${this.url}${path}`, { method, headers: { 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) })
    const text = await r.text()
    const json = text ? JSON.parse(text) : null
    if (!r.ok) throw new Error(`fake broker ${path}: ${r.status} ${text}`)
    return json
  }
  reset(start) { return this.call('POST', '/_control/reset', start ? { start } : {}) }
  tx(txs) { return this.call('POST', '/_control/tx', { txs }) }
  clock(at) { return this.call('POST', '/_control/clock', { at }) }
  kvPut(ns, key, value) { return this.call('POST', '/_control/kv', { ns, key, value }) }
  inject(spec) { return this.call('POST', '/_control/inject', spec) }
  dump() { return this.call('GET', '/_control/dump') }
  progress(key) { return this.call('GET', `/_control/progress?ns=${encodeURIComponent(NS)}&key=${encodeURIComponent(key)}`) }
}

class LiveBroker {
  constructor(url) {
    this.url = url
    this.live = true
  }

  async init() {
    const { Queen } = await import(process.env.FEED_JS_SDK || '../../client-js/client-v2/index.js')
    this.queen = new Queen({ url: this.url, handleSignals: false })
  }

  async tx(txs) {
    const positions = []
    for (const t of txs) {
      const tb = this.queen.transaction()
      const groups = new Map()
      for (const p of t.push) {
        const k = `${p.queue}\u0000${p.partition}`
        if (!groups.has(k)) groups.set(k, { queue: p.queue, partition: p.partition, items: [] })
        groups.get(k).items.push({ data: p.data, transactionId: p.transactionId })
      }
      for (const g of groups.values()) tb.queue(g.queue).partition(g.partition).push(g.items)
      const res = await tb.commit()
      if (res && res.success === false) throw new Error(`live push refused: ${res.reason} ${res.error || ''}`)
      positions.push(null)
    }
    // Where the log is now: idle waits until the runner has read past it.
    this.mark = (await this.queen.feed({ from: 'now' }).probe()).next
    return { positions }
  }

  async kv(key) {
    const row = await this.queen.kv.get(NS, key)
    return row && row.found ? row : null
  }
}

// ---- the result -----------------------------------------------------------------

/** Every layer the record's snapshot names, applied in order: per step, the entries by key. */
function decodeState(kvRows, id, record) {
  const snap = record && record.snapshot
  if (!snap) return null
  const layers = Array.isArray(snap.layers)
    ? snap.layers
    : snap.id && snap.chunks > 0 ? [{ sid: snap.id, chunks: snap.chunks, v: 1 }] : []
  const kv = new Map(kvRows.filter((r) => r.ns === NS).map((r) => [r.key, r.value]))
  const steps = []
  const chunkOf = (sid, n) => kv.get(`${id}#snap#${sid}#${n}`)
  for (const l of layers) {
    if (l.v === 1) {
      let b64 = ''
      for (let n = 0; n < l.chunks; n++) b64 += chunkOf(l.sid, n)?.d ?? ''
      const dump = JSON.parse(inflateRawSync(Buffer.from(b64, 'base64')).toString('utf8'))
      dump.steps.forEach((st, i) => { steps[i] = { kind: st.kind, entries: new Map(st.entries) } })
      continue
    }
    let parts = []
    for (let n = 0; n < l.chunks; n++) {
      const v = chunkOf(l.sid, n)
      if (!v) throw new Error(`snapshot ${l.sid} of ${id}: chunk ${n} is missing`)
      parts.push(v.d)
      if (v.m) continue
      const piece = JSON.parse(inflateRawSync(Buffer.from(parts.join(''), 'base64')).toString('utf8'))
      parts = []
      if (piece.v !== 2) throw new Error(`snapshot ${l.sid}: piece format ${piece.v}`)
      if (!steps[piece.s]) steps[piece.s] = { kind: piece.k, entries: new Map() }
      for (const [k, v] of piece.set || []) steps[piece.s].entries.set(k, v)
      for (const k of piece.del || []) steps[piece.s].entries.delete(k)
    }
    if (parts.length) throw new Error(`snapshot ${l.sid}: the last chunk says more follows`)
  }
  return Array.from({ length: steps.length }, (_, i) => steps[i]
    ? { kind: steps[i].kind, entries: [...steps[i].entries.entries()].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)) }
    : null)
}

/**
 * Positions and broker ids in a value, as script places: a script message's
 * position is `S<step>.<tx>`, any other one `R` (a runner's own write).
 */
function makeNormalizer(places, log, applied = null) {
  const pid = new Map()
  for (const e of log) pid.set(e.partitionId, `pid:${e.queue}/${e.partition}`)
  const msgId = new Map()
  for (const e of log) msgId.set(e.id, `id:${places.get(e.position) ?? 'R'}/${e.queue}/${e.partition}/${e.offset}`)
  const pos = (p) => (Number.isInteger(p) ? (places.get(p) ?? (applied !== null && p === applied + 1 ? 'end' : p > 0 ? 'R' : p)) : p)
  const walk = (v, key) => {
    if (Array.isArray(v)) return v.map((x) => walk(x, null))
    if (v && typeof v === 'object') {
      const out = {}
      for (const [k, x] of Object.entries(v)) {
        if ((k === 'position' || k === 'p') && Number.isInteger(x)) out[k] = pos(x)
        else if (k === 'partitionId' && typeof x === 'string') out[k] = pid.get(x) ?? x
        else if (k === 'id' && typeof x === 'string' && msgId.has(x)) out[k] = msgId.get(x)
        else out[k] = walk(x, k)
      }
      return out
    }
    return v
  }
  return { walk, pos }
}

function resultOf(c, dump, ctx) {
  const queryId = c.run.queryId
  const prefix = outputPrefix(queryId)
  const places = ctx.places
  const norm = makeNormalizer(places, dump.log, dump.applied)
  // Every id a message output can have: per source message, per shard scope, per epoch, per index.
  const scopes = new Set([queryId])
  const shardCounts = new Set()
  for (const st of [c.run, ...(c.script || []).filter((s) => s.start).map((s) => s.run || {})]) {
    if (st.shard) scopes.add(shardScope(queryId, st.shard))
    if (st.shards) shardCounts.add(st.shards)
  }
  for (const n of shardCounts) for (let i = 0; i < n; i++) scopes.add(shardScope(queryId, { index: i, count: n }))
  const idFrom = new Map()
  for (const e of dump.log) {
    const place = places.get(e.position)
    if (!place) continue
    for (const scope of scopes) {
      for (let epoch = 0; epoch < 4; epoch++) {
        for (let i = 0; i < 8; i++) {
          const h = sha(`${scope}\n${epoch}\n${e.position}\n${e.queue}\n${e.partition}\n${e.offset}\n${i}`)
          idFrom.set(`${prefix}${h.slice(0, 40)}`, `${place}/${e.queue}/${e.partition}#${i}`)
        }
        const d = sha(`${scope}\n${epoch}\ndlq\n${e.position}\n${e.queue}\n${e.partition}\n${e.offset}`)
        idFrom.set(`${prefix}${d.slice(0, 40)}`, `dlq:${place}/${e.queue}/${e.partition}`)
      }
    }
  }
  const problems = []
  // foreachBatch items: a message's result has the id its output would have (a place); a
  // close's result an id of its own, named by when it first came (emit#1, emit#2, ...), so
  // the same result delivered again shows the same name. A dead-lettered item's message id
  // is made from the item's id.
  const emitNames = new Map()
  const itemId = (id) => {
    if (idFrom.has(id)) return idFrom.get(id)
    if (!emitNames.has(id)) {
      if (typeof id !== 'string' || !id.startsWith(prefix) || !/^[0-9a-f]{40}$/.test(id.slice(prefix.length))) problems.push(`item id ${id}: not ${prefix} + 40 hex`)
      emitNames.set(id, `emit#${emitNames.size + 1}`)
    }
    return emitNames.get(id)
  }
  const batches = ctx.events.filter((e) => e.event === 'foreachBatch').map((e) => ({
    runner: e.runner,
    items: (e.items || []).map((it) => ({ value: norm.walk(it.value), ctx: norm.walk(it.ctx), id: itemId(it.id), _raw: it.id }))
  }))
  const dlqItem = new Map()
  for (const b of batches) {
    for (const it of b.items) {
      for (const scope of scopes) {
        for (let epoch = 0; epoch < 4; epoch++) dlqItem.set(`${prefix}${sha(`${scope}\n${epoch}\ndlq-item\n${it._raw}`).slice(0, 40)}`, `dlq-item:${it.id}`)
      }
      delete it._raw
    }
  }
  const outputs = []
  const seen = new Set()
  for (const e of dump.log) {
    if (typeof e.transactionId !== 'string' || !e.transactionId.startsWith(prefix)) continue
    if (seen.has(e.transactionId)) problems.push(`output id ${e.transactionId} twice`)
    seen.add(e.transactionId)
    let from = idFrom.get(e.transactionId) ?? dlqItem.get(e.transactionId)
    if (!from) {
      if (!/^[0-9a-f]{40}$/.test(e.transactionId.slice(prefix.length))) problems.push(`output id ${e.transactionId}: not ${prefix} + 40 hex`)
      from = 'emit'
    }
    const data = norm.walk(e.payload)
    if (data && typeof data === 'object' && data.queryId && data.error && typeof data.error === 'object') delete data.error.name
    if (data && typeof data === 'object' && data.queryId && data.item && typeof data.item.id === 'string') data.item.id = itemId(data.item.id)
    outputs.push({ queue: e.queue, partition: e.partition, data, from })
  }
  // Order inside one partition is the stream's order; across partitions it is how a commit grouped them.
  outputs.forEach((o, i) => { o._i = i })
  outputs.sort((a, b) => (a.queue < b.queue ? -1 : a.queue > b.queue ? 1 : a.partition < b.partition ? -1 : a.partition > b.partition ? 1 : a._i - b._i))
  for (const o of outputs) delete o._i
  // The position records: the query's, or each shard's.
  const records = {}
  const states = {}
  for (const row of dump.kv) {
    if (row.ns !== NS || row.key.includes('#snap#')) continue
    if (row.key !== queryId && !row.key.startsWith(`${queryId}#shard`)) continue
    const v = row.value || {}
    records[row.key] = {
      position: norm.pos(v.position),
      epoch: v.epoch,
      hash: v.hash,
      clock: v.clock,
      units: v.units,
      skips: Array.isArray(v.skips) ? v.skips.map((k) => String(k).replace(/^e\u001f(\d+)/, (m, n) => `e\u001f${norm.pos(Number(n))}`)) : v.skips,
      shard: v.shard,
      resetToken: v.resetToken,
      readTo: v.readTo,
      caughtUp: v.caughtUp,
      open: v.open,
      held: v.held,
      // When it stopped and the error's text are the process's own: the reason and the place compare.
      stopped: v.stopped ? normStopped(v.stopped, norm) : undefined,
      snapshotPosition: v.snapshot ? norm.pos(v.snapshot.position) : undefined,
      snapshotUnits: v.snapshot ? v.snapshot.units : undefined,
      metas: v.snapshot ? v.snapshot.metas : undefined
    }
    try {
      const st = decodeState(dump.kv, row.key, v)
      states[row.key] = st ? norm.walk(st) : null
    } catch (err) {
      problems.push(`state of ${row.key}: ${err.message}`)
    }
  }
  const layout = dump.kv.find((r) => r.ns === NS && r.key === `${queryId}#layout`)
  const about = dump.kv.find((r) => r.ns === NS && r.key === `${queryId}#about`)
  return {
    outputs,
    records,
    states,
    layout: layout ? { shards: layout.value.shards, shardBy: layout.value.shardBy, resetToken: layout.value.resetToken } : null,
    about: about ? normAbout(about.value) : null,
    foreach: ctx.events.filter((e) => e.event === 'foreach').map((e) => ({ runner: e.runner, value: norm.walk(e.value), ctx: norm.walk(e.ctx) })),
    // Every call, in order (a retry is a call): only in a case that has some, so the others' golden files stay as they are.
    ...(batches.length ? { foreachBatch: batches } : {}),
    errors: ctx.events.filter((e) => e.event === 'error' && (c.compareErrors || e.phase === 'poison')).map((e) => ({ runner: e.runner, phase: e.phase, action: e.action ?? null, code: e.code ?? null })),
    failed: ctx.events.filter((e) => e.event === 'failed').map((e) => ({ runner: e.runner, code: e.code ?? null })),
    recorded: norm.walk(ctx.recorded),
    problems
  }
}

/** `stopped` in a position record, without its time and its error's text. */
function normStopped(st, norm) {
  const o = { ...st }
  delete o.at
  if (o.error !== undefined) o.error = true
  if (Number.isInteger(o.position)) o.position = norm.pos(o.position)
  return o
}

/** `<queryId>#about`, without the client's name and version (each SDK says its own). */
function normAbout(v) {
  if (!v || typeof v !== 'object') return v
  const o = { ...v }
  delete o.client
  return o
}

// ---- comparing --------------------------------------------------------------------

function same(a, b) {
  // JSON has no undefined: a missing value and null are one.
  if (a === undefined) a = null
  if (b === undefined) b = null
  if (typeof a === 'number' && typeof b === 'number') return a === b || Math.abs(a - b) < 1e-9 * Math.max(1, Math.abs(a))
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return a === b
  if (Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a)) return a.length === b.length && a.every((x, i) => same(x, b[i]))
  const ka = Object.keys(a).filter((k) => a[k] !== undefined && a[k] !== null)
  const kb = Object.keys(b).filter((k) => b[k] !== undefined && b[k] !== null)
  if (ka.length !== kb.length) return false
  return ka.every((k) => same(a[k], b[k]))
}

/** The differences between two results, as readable lines. */
function diff(exp, got, path = '') {
  const out = []
  if (same(exp, got)) return out
  if (Array.isArray(exp) && Array.isArray(got)) {
    const n = Math.max(exp.length, got.length)
    for (let i = 0; i < n && out.length < 12; i++) {
      if (i >= exp.length) out.push(`${path}[${i}]: extra ${JSON.stringify(got[i]).slice(0, 300)}`)
      else if (i >= got.length) out.push(`${path}[${i}]: missing ${JSON.stringify(exp[i]).slice(0, 300)}`)
      else out.push(...diff(exp[i], got[i], `${path}[${i}]`))
    }
    if (exp.length !== got.length) out.push(`${path}: ${exp.length} expected, ${got.length} got`)
    return out
  }
  if (exp && got && typeof exp === 'object' && typeof got === 'object') {
    const keys = new Set([...Object.keys(exp), ...Object.keys(got)])
    for (const k of keys) {
      if (out.length >= 12) break
      out.push(...diff(exp[k] ?? null, got[k] ?? null, path ? `${path}.${k}` : k))
    }
    return out
  }
  return [`${path}: expected ${JSON.stringify(exp)?.slice(0, 300)}, got ${JSON.stringify(got)?.slice(0, 300)}`]
}

const DEFAULT_METRICS = ['emitted', 'outputs', 'effects', 'poisoned', 'skipped', 'deadLettered', 'late', 'replaySkipped']

function pickMetrics(m, keys) {
  const out = {}
  for (const k of keys) out[k] = m ? m[k] ?? null : null
  return out
}

/** A list answer's items in key order: the order a process keeps its state in is its own. */
function sortItems(answer) {
  if (!answer || typeof answer !== 'object' || !Array.isArray(answer.items)) return answer
  const k = (x) => JSON.stringify(x && x.key !== undefined ? x.key : x)
  return { ...answer, items: [...answer.items].sort((a, b) => (k(a) < k(b) ? -1 : k(a) > k(b) ? 1 : 0)) }
}

// ---- running one case -------------------------------------------------------------

const TIMING = {
  commitEveryMillis: 0,
  positionCommitMillis: 0,
  waitMillis: 300,
  lockTtl: '5s',
  retryDelayMillis: 5,
  snapshotEveryMillis: 3600000
}

async function cyclesOf(runner) {
  if (!runner || runner.exited) return 0
  const m = await runner.ask('metrics', 'metrics')
  return Number(m.metrics.cycles) || 0
}

/**
 * Idle: every record at the end of the log, nothing moved for a while, and the
 * runner read the feed twice since this step began (so it saw the latest
 * messages and the latest clock, not only a record a process before it left).
 */
async function idle(broker, c, runner, name, opts = {}) {
  const queryId = c.run.queryId
  const quietMs = opts.quietMs ?? 400
  const deadline = Date.now() + (opts.timeoutMs ?? 45000)
  const keys = opts.keys || [queryId]
  const from = await cyclesOf(runner)
  for (;;) {
    if (runner && runner.exited) throw new Error(`runner ${name} exited while waiting for idle: ${JSON.stringify(runner.exited)}\n${runner.stderr.slice(-3000)}`)
    if ((await cyclesOf(runner)) < from + 2) {
      if (Date.now() > deadline) throw new Error(`idle: runner ${name} read nothing in time (cycles stayed under ${from + 2})\n${runner ? runner.stderr.slice(-3000) : ''}`)
      await sleep(50)
      continue
    }
    if (broker.live) {
      // Live: idle = every record at the end of the log and no change for a while.
      // Live: other writers share the broker, so the end is where the log was after this case's last push.
      const rows = await Promise.all(keys.map((k) => broker.kv(k)))
      const at = rows.map((r) => (r ? `${r.value.position}:${JSON.stringify(r.value.snapshot ?? null)}` : 'none')).join('|')
      const done = rows.every((r) => r && r.value.position >= (broker.mark ?? 0))
      // What came before the mark is committed with the position past it.
      if (done) return
      void at
    } else {
      const ps = await Promise.all(keys.map((k) => broker.progress(k)))
      const done = ps.every((p) => p.record && p.record.position === p.applied + 1)
      if (done && ps[0].idleMs >= quietMs) return
    }
    if (Date.now() > deadline) {
      const last = broker.live ? '' : JSON.stringify(await broker.progress(keys[0]))
      throw new Error(`idle: runner ${name} did not settle in time; ${last}\n${runner ? runner.stderr.slice(-3000) : ''}`)
    }
    await sleep(50)
  }
}

async function runCase(sdkFor, c, { broker, liveUrl, write }) {
  const events = []
  const recorded = {}
  const runners = new Map()
  const places = new Map()
  const url = broker.url
  const caseFile = join(HERE, 'cases', `${c.name}.json`)
  let q = c
  if (broker.live) {
    // Live: a fresh queryId and fresh queue names, so runs never meet.
    const tag = randomBytes(3).toString('hex')
    const text = JSON.stringify(c).replaceAll('"c-', `"lv${tag}-c-`)
    q = JSON.parse(text)
    q._liveTag = tag
  }
  const liveCase = broker.live ? join(HERE, '.live', `${q.name}-${q._liveTag}.json`) : caseFile
  if (broker.live) {
    mkdirSync(join(HERE, '.live'), { recursive: true })
    writeFileSync(liveCase, JSON.stringify(q))
  }
  if (!broker.live) await broker.reset(q.start)
  const queryId = q.run.queryId
  let stepNo = 0
  try {
    for (const st of q.script) {
      stepNo++
      if (st.tx) {
        const r = await broker.tx(st.tx)
        r.positions.forEach((p, i) => { if (p !== null) places.set(p, `S${stepNo}.${i + 1}`) })
      } else if (st.clock) {
        if (!broker.live) await broker.clock(st.clock)
      } else if (st.start) {
        const sdk = st.sdk ? sdkFor(st.sdk) : sdkFor(null)
        const runOpts = { ...TIMING, ...(q.timing || {}), ...(st.run || {}) }
        let file = liveCase
        if (st.chain || st.source) {
          mkdirSync(join(HERE, '.live'), { recursive: true })
          file = join(HERE, '.live', `${q.name}-${st.start}-${randomBytes(3).toString('hex')}.json`)
          writeFileSync(file, JSON.stringify({ ...q, chain: st.chain || q.chain, source: st.source || q.source }))
        }
        const r = new RunnerProc(sdk, st.start, ['run', '--url', st.url || url, '--case', file, '--runner', st.start, '--run', JSON.stringify(runOpts)], events)
        runners.set(st.start, r)
        const got = await r.waitFor((l) => l.event === 'started' || l.event === 'failed', 30000, 'started')
        if (!got) throw new Error(`runner ${st.start} (${sdk}) exited before it started: ${JSON.stringify(r.exited)}\n${r.stderr.slice(-3000)}`)
        if (got.event === 'failed' && !st.mayFail) throw new Error(`runner ${st.start} (${sdk}) failed to start: ${got.message}`)
      } else if (st.idle) {
        const keys = st.keys ? st.keys.map((k) => k.replace('$q', queryId)) : [queryId]
        await idle(broker, q, runners.get(st.idle), st.idle, { keys, quietMs: st.quietMs, timeoutMs: st.timeoutMs })
      } else if (st.stop) {
        const r = runners.get(st.stop)
        const got = await r.ask('stop', 'stopped', 30000)
        recorded[`${st.stop}.stopped`] = pickMetrics(got.metrics, q.compareMetrics || DEFAULT_METRICS)
      } else if (st.kill) {
        runners.get(st.kill).kill()
        await sleep(50)
      } else if (st.wait) {
        const r = runners.get(st.wait)
        const got = await r.waitFor((l) => l.event === st.for, st.timeoutMs ?? 20000, st.for)
        if (got && got.metrics) recorded[`${st.wait}.${st.for}`] = pickMetrics(got.metrics, q.compareMetrics || DEFAULT_METRICS)
      } else if (st.waitRole) {
        const r = runners.get(st.waitRole)
        const deadline = Date.now() + (st.timeoutMs ?? 20000)
        for (;;) {
          const m = await r.ask('metrics', 'metrics')
          if (m.metrics.role === st.role) break
          if (Date.now() > deadline) throw new Error(`runner ${st.waitRole} never became ${st.role} (is ${m.metrics.role})`)
          await sleep(100)
        }
      } else if (st.metrics) {
        const m = await runners.get(st.metrics).ask('metrics', 'metrics')
        recorded[st.as] = pickMetrics(m.metrics, st.keys || q.compareMetrics || DEFAULT_METRICS)
      } else if (st.state) {
        const a = await runners.get(st.state).ask(`state ${st.step} ${st.key}`, 'state')
        recorded[st.as] = a.answer && a.answer.error ? { error: true, found: a.answer.found } : a.answer
      } else if (st.list) {
        const a = await runners.get(st.list).ask(`list ${st.step} ${st.limit ?? 100}`, 'state')
        recorded[st.as] = sortItems(a.answer)
      } else if (st.view) {
        const sdk = st.sdk ? sdkFor(st.sdk) : sdkFor(null)
        const v = st.view
        const args = ['view', '--url', url, '--query-id', queryId, '--op', v.op]
        if (v.step) args.push('--step', v.step)
        if (v.key !== undefined) args.push('--key', String(v.key))
        if (v.limit) args.push('--limit', String(v.limit))
        if (v.shards) args.push('--shards', String(v.shards))
        const a = await runOnce(sdk, args)
        // A refusal's text is the SDK's own: only that it was refused compares.
        let answer = a.event === 'view' ? a.answer : { failed: true }
        answer = sortItems(answer)
        if (v.op === 'status' && Array.isArray(answer)) answer = answer.map((x) => ({ shard: x.shard, shards: x.shards, role: x.role })).sort((x, y) => x.shard - y.shard)
        recorded[st.as] = answer
      } else if (st.kvPut) {
        if (!broker.live) await broker.kvPut(NS, st.kvPut.key.replace('$q', queryId), st.kvPut.value)
      } else if (st.inject) {
        if (!broker.live) await broker.inject(st.inject)
      } else if (st.sleep) {
        await sleep(st.sleep)
      } else {
        throw new Error(`script step ${stepNo}: unknown ${JSON.stringify(st)}`)
      }
    }
  } finally {
    for (const r of runners.values()) {
      if (!r.exited) {
        r.send('stop')
        await r.waitFor(() => false, 3000, 'exit').catch(() => {})
        r.kill()
      }
    }
  }
  if (broker.live) return liveResult(q, broker, events, recorded)
  const dump = await broker.dump()
  return resultOf(q, dump, { places, events, recorded })
}

/** Live: outputs read back from the broker's feed; ids checked for shape and uniqueness only. */
async function liveResult(q, broker, events, recorded) {
  const prefix = outputPrefix(q.run.queryId)
  // Values name the run's own queues (lv<tag>-c-...): back to the case's names.
  const untag = (v) => (v === undefined ? v : JSON.parse(JSON.stringify(v).replaceAll(`lv${q._liveTag}-`, '')))
  const reader = broker.queen.feed({ from: 'earliest', where: [{ field: 'transactionId', op: 'prefix', value: prefix }], waitMillis: 0 })
  const outputs = []
  const problems = []
  const seen = new Set()
  for (;;) {
    const page = await reader.read({ waitMillis: 0 })
    for (const e of page.entries) {
      if (seen.has(e.transactionId)) problems.push(`output id ${e.transactionId} twice`)
      seen.add(e.transactionId)
      outputs.push({ queue: e.queue.replace(`lv${q._liveTag}-`, ''), partition: e.partition, data: untag(e.data) })
    }
    if (page.caughtUp) break
  }
  await reader.close()
  const row = await broker.kv(q.run.queryId)
  const about = await broker.kv(`${q.run.queryId}#about`)
  return {
    outputs: outputs.sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1)),
    record: row ? { epoch: row.value.epoch, hash: row.value.hash, caughtUp: row.value.caughtUp, open: row.value.open, held: row.value.held } : null,
    about: about ? untag(normAbout({ ...about.value, queryId: undefined })) : null,
    foreach: events.filter((e) => e.event === 'foreach').map((e) => untag(e.value)).sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1)),
    // Live, how reads split the items into calls is the broker's timing: the items only, sorted.
    ...(events.some((e) => e.event === 'foreachBatch')
      ? { foreachBatch: events.filter((e) => e.event === 'foreachBatch').flatMap((e) => (e.items || []).map((it) => untag(it.value))).sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1)) }
      : {}),
    errors: events.filter((e) => e.event === 'error').map((e) => ({ phase: e.phase, action: e.action ?? null })),
    recorded: untag(recorded),
    problems
  }
}

// ---- main ---------------------------------------------------------------------------

async function main() {
  const sdk = arg('sdk', 'js')
  const only = arg('case')
  const write = flag('write')
  const live = arg('live')
  const cross = arg('cross') ? arg('cross').split(',') : null
  const cases = readdirSync(join(HERE, 'cases')).filter((f) => f.endsWith('.json')).sort()
    .map((f) => JSON.parse(readFileSync(join(HERE, 'cases', f), 'utf8')))
    .filter((c) => !only || only.split(',').includes(c.name))
    .filter((c) => (live ? (c.mode === 'live' || c.mode === 'both') : c.mode !== 'live'))
  if (!cases.length) throw new Error('no case matches')
  const sdks = new Set([sdk, ...(cross || [])])
  for (const s of sdks) buildRunner(s)

  let broker
  let fake = null
  if (live) {
    broker = new LiveBroker(live)
    await broker.init()
  } else {
    fake = await serve({ port: 0, quiet: !flag('verbose') })
    broker = new FakeBroker(`http://127.0.0.1:${fake.port}`)
  }

  const outDir = join(HERE, 'results', live ? `${sdk}-live` : sdk)
  if (flag('keep')) mkdirSync(outDir, { recursive: true })
  let failed = 0
  for (const c of cases) {
    const started = Date.now()
    // A case may name the SDK of each runner (cross runs); `--cross a,b` rotates them.
    let n = 0
    const sdkFor = (named) => {
      if (named && sdks.has(named)) return named
      if (cross) return cross[(n++) % cross.length]
      return sdk
    }
    let result
    try {
      result = await runCase(sdkFor, c, { broker, write })
    } catch (err) {
      failed++
      console.log(`FAIL ${c.name} (${sdk}${live ? ', live' : ''}): ${err.message}`)
      continue
    }
    if (flag('keep')) writeFileSync(join(outDir, `${c.name}.json`), JSON.stringify(result, null, 2))
    const expFile = join(HERE, 'expected', `${c.name}${live ? '.live' : ''}.json`)
    if (write) {
      if (result.problems.length) {
        failed++
        console.log(`FAIL ${c.name}: the reference run has problems: ${result.problems.join('; ')}`)
        continue
      }
      writeFileSync(expFile, `${JSON.stringify(result, null, 2)}\n`)
      console.log(`wrote ${c.name} (${result.outputs.length} outputs, ${Date.now() - started} ms)`)
      continue
    }
    if (!existsSync(expFile)) {
      failed++
      console.log(`FAIL ${c.name}: no expected/${c.name}${live ? '.live' : ''}.json (run the js harness with --write)`)
      continue
    }
    const exp = JSON.parse(readFileSync(expFile, 'utf8'))
    const lines = [...result.problems.map((p) => `problem: ${p}`), ...diff(exp, result)]
    if (lines.length) {
      failed++
      console.log(`FAIL ${c.name} (${[...sdks].join('+')}${live ? ', live' : ''}):\n  ${lines.slice(0, 20).join('\n  ')}`)
    } else {
      console.log(`ok   ${c.name} (${[...sdks].join('+')}${live ? ', live' : ''}, ${result.outputs.length} outputs, ${Date.now() - started} ms)`)
    }
  }
  if (fake) fake.server.close()
  console.log(`${cases.length - failed}/${cases.length} cases pass`)
  process.exit(failed ? 1 : 0)
}

main().catch((err) => {
  console.error(err)
  process.exit(2)
})
