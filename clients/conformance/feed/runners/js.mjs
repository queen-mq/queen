#!/usr/bin/env node
/**
 * The JS SDK's conformance runner (README.md, sections 1 and 3): builds a
 * feed stream from a case file with the JS SDK and runs it, speaking JSON
 * lines on stdout and taking commands on stdin.
 *
 *   node runners/js.mjs run  --url <url> --case <case.json> [--run '<json>']
 *   node runners/js.mjs view --url <url> --query-id <id> --op get|list|status [--step s] [--key k] [--limit n] [--shards n] [--timeout-ms n]
 *
 * The other SDKs' runners do the same with their own SDK. This file is also
 * the reference for the expression language.
 */
import { readFileSync } from 'node:fs'
import { createInterface } from 'node:readline'
// FEED_JS_SDK: another copy of the JS SDK's client-v2/index.js (a frozen one while the main
// checkout's is being edited); by default the repository's own.
const { Queen, Stream } = await import(process.env.FEED_JS_SDK || '../../../client-js/client-v2/index.js')

// ---- output -----------------------------------------------------------------

function say(obj) {
  process.stdout.write(`${JSON.stringify(obj, (k, v) => (v === undefined ? undefined : v))}\n`)
}

function arg(name, fallback = undefined) {
  const i = process.argv.indexOf(`--${name}`)
  return i > 0 && i + 1 < process.argv.length ? process.argv[i + 1] : fallback
}

// ---- expressions ------------------------------------------------------------

const flaky = new Map()

function truthy(v) {
  return !(v === false || v === 0 || v === '' || v === null || v === undefined || Number.isNaN(v))
}

function deepEq(a, b) {
  if (a === undefined || b === undefined) return a === b
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return a === b
  if (Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a)) return a.length === b.length && a.every((x, i) => deepEq(x, b[i]))
  const ka = Object.keys(a).filter((k) => a[k] !== undefined)
  const kb = Object.keys(b).filter((k) => b[k] !== undefined)
  return ka.length === kb.length && ka.every((k) => Object.prototype.hasOwnProperty.call(b, k) && deepEq(a[k], b[k]))
}

function getPath(args, path) {
  const segs = String(path).split('.')
  let cur = args[Number(segs[0])]
  for (let i = 1; i < segs.length; i++) {
    if (cur === null || cur === undefined || typeof cur !== 'object') return undefined
    cur = Array.isArray(cur) ? cur[Number(segs[i])] : cur[segs[i]]
  }
  return cur
}

function cmp(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0
  if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0
  return null
}

export function evaluate(expr, args) {
  if (expr === null || typeof expr !== 'object') return expr
  if (Array.isArray(expr)) return expr.map((e) => { const v = evaluate(e, args); return v === undefined ? null : v })
  const keys = Object.keys(expr)
  const op = keys.find((k) => k.startsWith('$'))
  if (op === undefined) {
    const out = {}
    for (const k of keys) {
      const v = evaluate(expr[k], args)
      if (v !== undefined) out[k] = v
    }
    return out
  }
  if (keys.length !== 1) throw new Error(`expression: ${op} stands alone in its object`)
  const a = expr[op]
  const ev = (e) => evaluate(e, args)
  switch (op) {
    case '$get': return getPath(args, a)
    case '$eq': return deepEq(ev(a[0]), ev(a[1]))
    case '$ne': return !deepEq(ev(a[0]), ev(a[1]))
    case '$lt': { const c = cmp(ev(a[0]), ev(a[1])); return c !== null && c < 0 }
    case '$lte': { const c = cmp(ev(a[0]), ev(a[1])); return c !== null && c <= 0 }
    case '$gt': { const c = cmp(ev(a[0]), ev(a[1])); return c !== null && c > 0 }
    case '$gte': { const c = cmp(ev(a[0]), ev(a[1])); return c !== null && c >= 0 }
    case '$and': { for (const e of a) if (!truthy(ev(e))) return false; return true }
    case '$or': { for (const e of a) if (truthy(ev(e))) return true; return false }
    case '$not': return !truthy(ev(a))
    case '$if': return truthy(ev(a[0])) ? ev(a[1]) : ev(a[2])
    case '$add': { let s = 0; for (const e of a) { const v = ev(e); if (typeof v === 'number') s += v } return s }
    case '$mod': return ev(a[0]) % ev(a[1])
    case '$str': return String(ev(a))
    case '$concat': return a.map((e) => { const v = ev(e); return v === undefined ? '' : String(v) }).join('')
    case '$len': { const v = ev(a); return Array.isArray(v) || typeof v === 'string' ? v.length : 0 }
    case '$keys': { const v = ev(a); return v && typeof v === 'object' && !Array.isArray(v) ? Object.keys(v).filter((k) => v[k] !== undefined).sort() : [] }
    case '$missing': return undefined
    case '$throw': throw new Error(String(a))
    case '$flaky': {
      const key = String(ev(a.key))
      const n = flaky.get(key) || 0
      if (n < a.times) {
        flaky.set(key, n + 1)
        throw new Error(`flaky ${key} ${n + 1}`)
      }
      return ev(a.then)
    }
  }
  throw new Error(`expression: unknown operator ${op}`)
}

const fn = (expr) => (...args) => evaluate(expr, args)

// ---- the chain --------------------------------------------------------------

function withFns(opts, names) {
  const out = { ...opts }
  for (const n of names) {
    if (out[n] === undefined) continue
    if (n === 'eventTime' && out[n] === false) continue
    out[n] = fn(out[n])
  }
  return out
}

/** sides / steps: a queue name stays a name; anything else is a test. */
function parts(spec) {
  if (Array.isArray(spec)) return spec
  const out = {}
  for (const [k, v] of Object.entries(spec)) out[k] = typeof v === 'string' ? v : fn(v)
  return out
}

const WINDOW_FNS = ['eventTime']

function build(queen, c) {
  let s = Stream.fromFeed(queen, { ...(c.source || {}) })
  for (const st of c.chain) {
    switch (st.op) {
      case 'where': s = st.args.length === 2 ? s.where(st.args[0], st.args[1]) : s.where(st.args[0], st.args[1], st.args[2]); break
      case 'filter': s = s.filter(fn(st.fn)); break
      case 'map': s = s.map(fn(st.fn)); break
      case 'flatMap': s = s.flatMap(fn(st.fn)); break
      case 'keyBy': s = s.keyBy(fn(st.fn)); break
      case 'windowTumbling': s = s.windowTumbling(withFns(st.opts, WINDOW_FNS)); break
      case 'windowSliding': s = s.windowSliding(withFns(st.opts, WINDOW_FNS)); break
      case 'windowSession': s = s.windowSession(withFns(st.opts, WINDOW_FNS)); break
      case 'windowCron': s = s.windowCron(withFns(st.opts, WINDOW_FNS)); break
      case 'reduce': s = 'initial' in st ? s.reduce(fn(st.fn), st.initial) : s.reduce(fn(st.fn)); break
      case 'aggregate': {
        const ex = {}
        for (const [k, e] of Object.entries(st.fields)) ex[k] = fn(e)
        s = s.aggregate(ex)
        break
      }
      case 'pattern': s = s.pattern(withFns(st.opts, ['start', 'then', 'key', 'onMatch', 'onTimeout', 'keep', 'eventTime'])); break
      case 'join': {
        if (st.opts.sides !== undefined) {
          const o = withFns(st.opts, ['key', 'select', 'onTimeout', 'keep', 'eventTime'])
          o.sides = parts(st.opts.sides)
          s = s.join(o)
        } else {
          s = s.join(withFns(st.opts, ['left', 'right', 'key', 'select', 'onTimeout', 'keep', 'eventTime']))
        }
        break
      }
      case 'gather': {
        const o = withFns(st.opts, ['count', 'key', 'onComplete', 'onTimeout', 'keep', 'eventTime'])
        if (st.opts.steps !== undefined) o.steps = parts(st.opts.steps)
        s = s.gather(o)
        break
      }
      case 'joinTable': s = s.joinTable(withFns(st.opts, ['on', 'key', 'value', 'select'])); break
      case 'name': s = s.name(st.name); break
      case 'to': {
        const t = st.target
        const target = typeof t === 'string'
          ? queen.queue(t)
          : Array.isArray(t)
            ? t.map((q) => queen.queue(q))
            : fn(t.fn)
        const opts = {}
        if (typeof st.partition === 'string') opts.partition = st.partition
        else if (st.partition && st.partition.fn !== undefined) opts.partition = fn(st.partition.fn)
        s = s.to(target, opts)
        break
      }
      case 'foreach': {
        const e = st.fn
        s = s.foreach(async (value, ctx) => {
          say({ event: 'foreach', value, ctx })
          return e === undefined ? undefined : evaluate(e, [value, ctx])
        })
        break
      }
      default:
        throw new Error(`case: unknown chain op ${st.op}`)
    }
  }
  return s
}

function runOptions(c, overrides) {
  const o = { ...(c.run || {}), ...(overrides || {}) }
  if (o.shardBy !== undefined && o.shardBy !== 'partition') o.shardBy = fn(o.shardBy)
  o.onError = (err, ctx) => say({ event: 'error', message: err && err.message, phase: ctx && ctx.phase, action: ctx && ctx.action, code: err && err.code })
  return o
}

// ---- main -------------------------------------------------------------------

async function main() {
  const mode = process.argv[2]
  const url = arg('url')
  if (!url) throw new Error('--url is required')
  const queen = new Queen({ url, handleSignals: false })

  if (mode === 'view') {
    const view = Stream.view(queen, arg('query-id'), {
      shards: arg('shards') ? Number(arg('shards')) : undefined,
      timeoutMillis: arg('timeout-ms') ? Number(arg('timeout-ms')) : 3000
    })
    const op = arg('op')
    let answer
    try {
      if (op === 'get') answer = await view.get(arg('step'), arg('key'))
      else if (op === 'list') answer = await view.list(arg('step'), { limit: Number(arg('limit', '100')) })
      else answer = await view.status()
      say({ event: 'view', answer })
    } catch (err) {
      say({ event: 'failed', message: err.message, code: err.code })
    }
    await queen.close?.().catch?.(() => {})
    process.exit(0)
  }

  if (mode !== 'run') throw new Error('mode is run or view')
  const c = JSON.parse(readFileSync(arg('case'), 'utf8'))
  const overrides = arg('run') ? JSON.parse(arg('run')) : {}
  let runner
  try {
    runner = await build(queen, c).run(runOptions(c, overrides))
  } catch (err) {
    say({ event: 'failed', message: err.message, code: err.code })
    process.exit(2)
  }
  const role = runner.metrics().role ?? (runner.metrics().running ? 'active' : 'standby')
  say({ event: 'started', role })

  let ending = false
  const end = async (event) => {
    if (ending) return
    ending = true
    if (event === 'stopped') await runner.stop()
    say({ event, metrics: runner.metrics() })
    process.exit(0)
  }
  Promise.resolve(runner.done).then(() => end('done'), () => end('done'))

  const rl = createInterface({ input: process.stdin })
  rl.on('line', (line) => {
    const [cmd, ...rest] = line.trim().split(/\s+/)
    if (cmd === 'stop') end('stopped')
    else if (cmd === 'metrics') say({ event: 'metrics', metrics: runner.metrics() })
    else if (cmd === 'state') say({ event: 'state', answer: runner.state(rest[0]).get(rest.slice(1).join(' ')) })
    else if (cmd === 'list') say({ event: 'state', answer: runner.state(rest[0]).list(Number(rest[1] || 100)) })
  })
  rl.on('close', () => end('stopped'))
}

main().catch((err) => {
  say({ event: 'failed', message: err && err.message ? err.message : String(err) })
  process.exit(2)
})
