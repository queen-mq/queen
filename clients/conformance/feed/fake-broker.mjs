#!/usr/bin/env node
/**
 * A broker in memory, over HTTP, for the feed-stream conformance cases: it
 * speaks the five wires a feed stream uses (`POST /api/v1/feed`, `/api/v1/kv`,
 * `/api/v1/transaction`, `/api/v1/locks`, `/api/v1/ephemeral/*`), as the real
 * broker answers them, so every SDK runs its own client code against it.
 *
 *   node fake-broker.mjs [--port 0]      prints {"port":N} on stdout once listening
 *
 * Rules (the ones that keep a case deterministic across SDKs):
 * - The clock is virtual. `nowMs` is the broker's wall clock, moved only by
 *   the control routes. A write is stamped max(nowMs, the last stamp). A
 *   caught-up read resolves to the last stamp; a read that is not caught up
 *   resolves to the time of the last entry it covers (as feed.rs does).
 * - Only transactions that push take a log position (on the real broker every
 *   write does; a stream never depends on that). So a KV-only commit, a lock
 *   call or a snapshot chunk never shifts the positions of the case's own
 *   messages, whatever an SDK's commit timing.
 * - Control transactions (`/_control/tx`) are applied all at once before any
 *   reader wakes.
 * Locks expire on the real clock.
 *
 * Control routes (JSON in, JSON out):
 *   POST /_control/reset {start?}                 empty broker; clock at `start` (ISO)
 *   POST /_control/tx {txs:[{at, push:[{queue, partition, data, transactionId}]}]}
 *                                                  -> {positions:[...]} (one per tx)
 *   POST /_control/clock {at}                      the clock moves to `at` (never back); readers wake
 *   POST /_control/kv {ns, key, value}             another process's KV write
 *   POST /_control/inject {route, when, status, body?, times?}
 *                                                  the next `times` calls of `route` answer
 *                                                  `status` (`when`: 'before' the call applies, or
 *                                                  'after': applied, answer lost)
 *   GET  /_control/dump                            everything: log, kv, locks, stats
 *   GET  /_control/progress?ns=&key=               {applied, record, lastProgressAt, nowMs, idleMs}
 */
import http from 'node:http'

const iso = (ms) => new Date(ms).toISOString()
const kvKey = (ns, key) => `${ns}\u0000${key}`
const LOCK_NS = 'queen-locks'
const MAX_WAIT_MS = 30000

function glob(pattern) {
  const re = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')
  return new RegExp(`^${re}$`)
}

// ---- where, as the broker evaluates it (feed.rs) -------------------------------

const OPS = new Set(['=', '==', '!=', '<', '<=', '>', '>=', 'in', 'exists', 'prefix', '!prefix'])

function jsonEq(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return a === b
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return a === b
  return JSON.stringify(a) === JSON.stringify(b)
}

function compare(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0
  if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0
  return null
}

function fieldValue(field, msg) {
  switch (field) {
    case 'queue': return msg.queue
    case 'partition': return msg.partition
    case 'transactionId': return msg.transactionId
    case 'offset': return msg.offset
    case 'position': return msg.position
    case 'data': return msg.data
  }
  if (!field.startsWith('data.')) return undefined
  let cur = msg.data
  for (const key of field.slice(5).split('.')) {
    if (cur === null || typeof cur !== 'object') return undefined
    cur = Array.isArray(cur) ? cur[Number(key)] : cur[key]
    if (cur === undefined) return undefined
  }
  return cur
}

function holds(cond, msg) {
  const v = fieldValue(cond.field, msg)
  if (v === undefined) return false
  switch (cond.op) {
    case 'exists': return true
    case '=': case '==': return jsonEq(v, cond.value)
    case '!=': return !jsonEq(v, cond.value)
    case 'in': return Array.isArray(cond.value) && cond.value.some((x) => jsonEq(v, x))
    case 'prefix': return typeof v === 'string' && typeof cond.value === 'string' && v.startsWith(cond.value)
    case '!prefix': return typeof v === 'string' && typeof cond.value === 'string' && !v.startsWith(cond.value)
    case '<': { const c = compare(v, cond.value); return c !== null && c < 0 }
    case '<=': { const c = compare(v, cond.value); return c !== null && c <= 0 }
    case '>': { const c = compare(v, cond.value); return c !== null && c > 0 }
    case '>=': { const c = compare(v, cond.value); return c !== null && c >= 0 }
  }
  return false
}

/** FNV-1a 32 over UTF-8 (feed.rs `fnv1a32`). */
export function fnv1a32(text) {
  const bytes = Buffer.from(String(text), 'utf8')
  let h = 0x811c9dc5
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i]
    h = Math.imul(h, 0x01000193)
  }
  return h >>> 0
}

class HttpError extends Error {
  constructor(status, body) {
    super(body.error || `HTTP ${status}`)
    this.status = status
    this.body = body
  }
}

const bad = (msg, code = 'bad_request') => new HttpError(400, { error: msg, code })

// ---- the broker -------------------------------------------------------------

export function newBroker({ start = Date.parse('2026-11-06T09:00:00.000Z') } = {}) {
  const b = {
    nowMs: start,
    lastNowMs: start,
    applied: 0,
    log: [],
    kv: new Map(),
    kvVersion: 100,
    ids: new Set(),
    offsets: new Map(),
    pids: new Map(),
    locks: new Map(),
    tokens: 0,
    waiters: [],
    ephemeral: new Map(),
    ephemeralWaiters: [],
    ephemeralSeq: 0,
    injected: [],
    lastProgressAt: Date.now(),
    stats: { requests: {}, unknown: {}, txns: 0, kvOnlyTxns: 0, pushTxns: 0, lockCalls: 0, feedReads: 0 }
  }

  const stampTime = () => {
    b.lastNowMs = Math.max(b.nowMs, b.lastNowMs)
    return b.lastNowMs
  }
  const stampPosition = () => {
    b.applied++
    return { position: b.applied, t: stampTime() }
  }
  const progress = () => { b.lastProgressAt = Date.now() }
  b.wake = () => {
    const w = b.waiters
    b.waiters = []
    for (const fn of w) fn()
  }

  function appendPushes(items, position, t) {
    const results = []
    for (const it of items) {
      const partition = it.partition ?? 'Default'
      const pk = `${it.queue}\u0000${partition}`
      if (!b.pids.has(pk)) b.pids.set(pk, `pid-${b.pids.size + 1}`)
      const offset = (b.offsets.get(pk) || 0) + 1
      b.offsets.set(pk, offset)
      b.ids.add(`${pk}\u0000${it.transactionId}`)
      b.log.push({
        position, kind: 'push', queue: it.queue, partition, partitionId: b.pids.get(pk), offset,
        id: `m-${position}-${it.queue}-${partition}-${offset}`, transactionId: it.transactionId, payload: it.payload ?? null, ts: iso(t)
      })
      results.push({ index: results.length, type: 'push', success: true, transactionId: it.transactionId, queueName: it.queue, messageId: `m-${position}-${it.queue}-${partition}-${offset}` })
    }
    return results
  }

  // -- control --

  b.controlTx = (txs) => {
    const positions = []
    for (const tx of txs || []) {
      if (tx.at) b.nowMs = Math.max(b.nowMs, Date.parse(tx.at))
      const items = (tx.push || []).map((x, i) => ({
        queue: x.queue, partition: x.partition, payload: x.data,
        transactionId: x.transactionId ?? `in-${b.applied + 1}-${i}`
      }))
      if (!items.length) throw bad('a control tx pushes at least one message')
      const { position, t } = stampPosition()
      appendPushes(items, position, t)
      positions.push(position)
    }
    progress()
    b.wake()
    return { positions, applied: b.applied }
  }

  b.controlClock = (at) => {
    const t = Date.parse(at)
    if (!Number.isFinite(t)) throw bad(`clock: ${JSON.stringify(at)} is not a time`)
    b.nowMs = Math.max(b.nowMs, t)
    b.lastNowMs = Math.max(b.lastNowMs, b.nowMs)
    b.wake()
    return { nowMs: b.nowMs, lastNowMs: b.lastNowMs }
  }

  b.controlKv = ({ ns, key, value }) => {
    stampTime()
    const version = ++b.kvVersion
    b.kv.set(kvKey(ns, key), { value: JSON.parse(JSON.stringify(value)), version, updatedAt: iso(b.lastNowMs) })
    progress()
    return { version }
  }

  b.dump = () => ({
    nowMs: b.nowMs,
    lastNowMs: b.lastNowMs,
    applied: b.applied,
    log: b.log,
    kv: [...b.kv.entries()].map(([k, row]) => {
      const i = k.indexOf('\u0000')
      return { ns: k.slice(0, i), key: k.slice(i + 1), value: row.value, version: row.version }
    }),
    locks: [...b.locks.entries()].map(([name, row]) => ({ name, owner: row.owner, token: row.token, live: row.expiresAt > Date.now() })),
    stats: b.stats
  })

  b.progressOf = (ns, key) => {
    const row = ns && key ? b.kv.get(kvKey(ns, key)) : null
    return {
      applied: b.applied,
      record: row ? row.value : null,
      version: row ? row.version : 0,
      lastProgressAt: b.lastProgressAt,
      idleMs: Date.now() - b.lastProgressAt,
      nowMs: b.nowMs
    }
  }

  // -- the feed --

  const FEED_FIELDS = new Set(['from', 'since', 'queues', 'partitions', 'kinds', 'where', 'maxRecords', 'maxBytes', 'maxWaitMs', 'shard'])

  function checkFeedBody(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('feed: the body is an object')
    for (const k of Object.keys(body)) {
      if (!FEED_FIELDS.has(k)) throw bad(`feed: unknown field \`${k}\``)
    }
    for (const c of body.where || []) {
      if (!c || typeof c.field !== 'string' || !OPS.has(c.op)) throw bad(`feed: bad where condition ${JSON.stringify(c)}`)
      for (const k of Object.keys(c)) if (!['field', 'op', 'value'].includes(k)) throw bad(`feed: where has unknown field \`${k}\``)
    }
    if (body.shard !== undefined && body.shard !== null) {
      const s = body.shard
      if (!Number.isInteger(s.count) || s.count < 1 || s.count > 4096 || !Number.isInteger(s.index) || s.index < 0 || s.index >= s.count) {
        throw bad(`feed: bad shard ${JSON.stringify(s)}`)
      }
      for (const k of Object.keys(s)) if (!['count', 'index', 'queues'].includes(k)) throw bad(`feed: shard has unknown field \`${k}\``)
    }
    if (body.maxRecords !== undefined && (!Number.isInteger(body.maxRecords) || body.maxRecords < 1 || body.maxRecords > 10000)) {
      throw bad('feed: maxRecords is 1 to 10000')
    }
    if (body.maxWaitMs !== undefined && (!Number.isInteger(body.maxWaitMs) || body.maxWaitMs < 0)) throw bad('feed: maxWaitMs is a whole number from 0')
    const from = body.from
    if (!(from === undefined || from === null || from === 'now' || from === 'earliest' || (Number.isInteger(from) && from >= 0))) {
      throw bad(`feed: from is a position, 'now' or 'earliest', got ${JSON.stringify(from)}`)
    }
  }

  b.read = async (body, again = false) => {
    if (!again) {
      checkFeedBody(body)
      b.stats.feedReads++
    }
    let from = body.from
    if (from === undefined || from === null || from === 'now') from = b.applied + 1
    else if (from === 'earliest') from = 1
    from = Number(from)
    const queues = body.queues ? (Array.isArray(body.queues) ? body.queues : [body.queues]).map(glob) : null
    const parts = body.partitions ? (Array.isArray(body.partitions) ? body.partitions : [body.partitions]) : null
    const where = body.where || []
    const since = body.since ? Date.parse(body.since) : null
    const max = body.maxRecords ?? 1000
    const shard = body.shard || null
    const shardQueues = shard && shard.queues ? shard.queues.map(glob) : null
    const out = []
    let next = b.applied + 1
    for (const e of b.log) {
      if (e.position < from) continue
      if (out.length >= max && e.position !== out[out.length - 1].position) {
        next = e.position
        break
      }
      if (queues && !queues.some((re) => re.test(e.queue))) continue
      if (parts && !parts.includes(e.partition)) continue
      if (since !== null && Date.parse(e.ts) < since) continue
      if (shard && (!shardQueues || shardQueues.some((re) => re.test(e.queue))) && fnv1a32(e.partition) % shard.count !== shard.index) continue
      const msg = { queue: e.queue, partition: e.partition, transactionId: e.transactionId, offset: e.offset, position: e.position, data: e.payload }
      if (!where.every((c) => holds(c, msg))) continue
      out.push(e)
    }
    const caughtUp = next > b.applied
    const wait = Math.min(body.maxWaitMs || 0, MAX_WAIT_MS)
    if (out.length === 0 && caughtUp && wait > 0 && !again) {
      await new Promise((resolve) => {
        const timer = setTimeout(() => { b.waiters = b.waiters.filter((w) => w !== done); resolve() }, wait)
        function done() { clearTimeout(timer); resolve() }
        b.waiters.push(done)
      })
      return b.read({ ...body, from }, true)
    }
    let lastTs = null
    for (const e of b.log) {
      if (e.position >= next) break
      lastTs = e.ts
    }
    return {
      applied: b.applied,
      caughtUp,
      entries: out.map((e) => ({ ...e })),
      next,
      resolvedTs: iso(caughtUp ? b.lastNowMs : Date.parse(lastTs ?? iso(b.lastNowMs)))
    }
  }

  // -- KV --

  function rowOut(key, row) {
    return { expiresAt: null, key, value: row.value, version: row.version, updatedAt: row.updatedAt }
  }

  b.kvCall = (operations) => {
    if (!Array.isArray(operations)) throw bad('body must be an array of operations, or {"operations":[...]}')
    const results = operations.map((op, index) => {
      if (op.op === 'get') {
        const row = b.kv.get(kvKey(op.ns, op.key))
        return row
          ? { index, op: 'get', key: op.key, found: true, value: row.value, version: row.version, updatedAt: row.updatedAt }
          : { index, op: 'get', key: op.key, found: false }
      }
      if (op.op === 'getMany') {
        const rows = []
        const missing = []
        for (const k of op.keys || []) {
          const row = b.kv.get(kvKey(op.ns, k))
          if (row) rows.push(rowOut(k, row))
          else missing.push(k)
        }
        return { index, op: 'getMany', rows, missing, truncated: false }
      }
      if (op.op === 'getPrefix') {
        const prefix = op.prefix ?? ''
        const keys = [...b.kv.keys()]
          .filter((k) => k.startsWith(`${op.ns}\u0000${prefix}`))
          .map((k) => k.slice(op.ns.length + 1))
          .sort()
          .filter((k) => op.after === undefined || op.after === null || k > op.after)
        const limit = Math.min(op.limit ?? 100, 1000)
        const page = keys.slice(0, limit)
        const rows = page.map((k) => {
          const row = b.kv.get(kvKey(op.ns, k))
          return op.keysOnly ? { expiresAt: null, key: k, updatedAt: row.updatedAt, version: row.version } : rowOut(k, row)
        })
        const truncated = keys.length > limit
        return { index, op: 'getPrefix', rows, truncated, nextAfter: truncated ? page[page.length - 1] : null }
      }
      if (['put', 'putIfAbsent', 'delete', 'check'].includes(op.op)) {
        // A write on the KV route is a transaction of its own.
        const res = b.transaction({ kv: [op] })
        if (res.success === false) return { index, op: op.op, key: op.key, applied: false, reason: res.kvReason ?? res.reason, version: res.version ?? null }
        const r = res.results.find((x) => x.type === 'kv') || {}
        return { index, op: op.op, key: op.key, applied: true, version: r.version }
      }
      throw bad(`kv: op ${JSON.stringify(op.op)} is not served by the fake broker`)
    })
    return { results }
  }

  // -- locks --

  function lockRow(name) {
    const row = b.locks.get(name)
    return row && row.expiresAt > Date.now() ? row : null
  }
  const guardOf = (name, token) => ({ op: 'check', ns: LOCK_NS, key: `${name}#0`, expect: token, required: true })

  b.locksCall = (operations) => {
    if (!Array.isArray(operations)) throw bad('locks: the body is {"operations":[...]}')
    b.stats.lockCalls++
    const results = operations.map((op, index) => {
      const name = op.name
      if (typeof name !== 'string' || !name) throw bad('locks: every op names a lock')
      const row = lockRow(name)
      const ttl = Number(op.ttlSeconds)
      if (op.op === 'acquire') {
        if (!(ttl > 0)) throw bad('locks: acquire needs ttlSeconds')
        if (row && row.owner !== op.owner) {
          return { index, op: 'acquire', name, acquired: false, reason: 'held', holders: [{ slot: 0, owner: row.owner }] }
        }
        stampTime()
        if (row && row.owner === op.owner) {
          row.expiresAt = Date.now() + ttl * 1000
          return { index, op: 'acquire', name, acquired: true, slot: 0, token: row.token, owner: op.owner ?? null, guard: guardOf(name, row.token), already: true }
        }
        const token = ++b.tokens
        b.locks.set(name, { owner: op.owner ?? null, token, expiresAt: Date.now() + ttl * 1000 })
        return { index, op: 'acquire', name, acquired: true, slot: 0, token, owner: op.owner ?? null, guard: guardOf(name, token) }
      }
      if (op.op === 'renew') {
        if (!row || row.token !== op.token) {
          return { index, op: 'renew', name, renewed: false, reason: 'lost', slot: 0, holders: row ? [{ slot: 0, owner: row.owner }] : [] }
        }
        stampTime()
        const token = ++b.tokens
        b.locks.set(name, { owner: row.owner, token, expiresAt: Date.now() + (ttl > 0 ? ttl : 15) * 1000 })
        return { index, op: 'renew', name, renewed: true, slot: 0, token, guard: guardOf(name, token) }
      }
      if (op.op === 'release') {
        if (!row || row.token !== op.token) {
          return { index, op: 'release', name, released: false, reason: 'lost', slot: 0, holders: row ? [{ slot: 0, owner: row.owner }] : [] }
        }
        stampTime()
        b.locks.delete(name)
        return { index, op: 'release', name, released: true, slot: 0 }
      }
      if (op.op === 'get') {
        return { index, op: 'get', name, held: !!row, holders: row ? [{ slot: 0, owner: row.owner, token: row.token }] : [] }
      }
      throw bad(`locks: op ${JSON.stringify(op.op)} is not served by the fake broker`)
    })
    return { results }
  }

  // -- transactions --

  b.transaction = (body) => {
    b.stats.txns++
    const items = []
    for (const op of body.operations || []) {
      if (op.type === 'push') {
        for (const it of op.items || []) {
          items.push({ queue: it.queue ?? op.queue, partition: it.partition ?? op.partition, transactionId: it.transactionId, payload: it.payload ?? it.data })
        }
      } else if (op.type !== undefined) {
        throw bad(`transaction: operation type ${JSON.stringify(op.type)} is not served by the fake broker`)
      }
    }
    const kv = body.kv || []
    // Duplicates first, as the broker answers them.
    const seen = new Set()
    for (const it of items) {
      if (typeof it.queue !== 'string' || !it.queue) throw bad('transaction: a push names its queue')
      const k = `${it.queue}\u0000${it.partition ?? 'Default'}\u0000${it.transactionId}`
      if (it.transactionId && (b.ids.has(k) || seen.has(k))) {
        return { success: false, reason: 'duplicate', error: `QDUP a pushed message to ${it.queue}/${it.partition ?? 'Default'} is a duplicate; the transaction rolled back`, results: [], transactionId: `txn-${b.stats.txns}` }
      }
      seen.add(k)
    }
    if (kv.length > 64) return { success: false, reason: 'bad_request', error: `kv_too_many_ops: ${kv.length} ops in one call, the ceiling is 64` }
    for (let i = 0; i < kv.length; i++) {
      const op = kv[i]
      const fail = (kvReason, row) => ({
        success: false, ok: false, reason: 'kv_precondition', failedIndex: items.length + i, kvReason,
        version: row ? row.version : null, value: row ? row.value : null,
        error: 'QKV a required KV precondition failed; the transaction rolled back', results: [], transactionId: `txn-${b.stats.txns}`
      })
      const required = op.required !== false
      if (op.op === 'check' && op.ns === LOCK_NS) {
        const name = String(op.key).replace(/#\d+$/, '')
        const row = lockRow(name)
        if (!row) return fail('missing', null)
        if (row.token !== op.expect) return fail('version', { version: row.token, value: { owner: row.owner } })
        continue
      }
      const row = b.kv.get(kvKey(op.ns, op.key))
      if (op.op === 'check') {
        if (op.expect === 0 && row && required) return fail('exists', row)
        if (op.expect !== 0 && (!row || row.version !== op.expect) && required) return fail(row ? 'version' : 'missing', row)
        continue
      }
      if (op.op === 'putIfAbsent' && row && required) return fail('exists', row)
      if (op.op === 'put' && op.expect !== undefined && op.expect !== null && required) {
        if (!row) return fail('missing', null)
        if (row.version !== op.expect) return fail('version', row)
      }
      if ((op.op === 'put' || op.op === 'putIfAbsent') && !op.forever && !op.ttlSeconds && !op.ttl && !op.expiresAt) {
        return { success: false, reason: 'bad_request', error: 'kv_expiry_not_specified' }
      }
      if (!['put', 'putIfAbsent', 'delete', 'check'].includes(op.op)) throw bad(`transaction: kv op ${JSON.stringify(op.op)} is not served by the fake broker`)
    }
    let position = null
    let t
    if (items.length) {
      const s = stampPosition()
      position = s.position
      t = s.t
      b.stats.pushTxns++
    } else {
      t = stampTime()
      b.stats.kvOnlyTxns++
    }
    const results = items.length ? appendPushes(items, position, t) : []
    let moved = items.length > 0
    for (let i = 0; i < kv.length; i++) {
      const op = kv[i]
      if (op.op === 'check') {
        results.push({ index: items.length + i, opIndex: i, type: 'kv', op: 'check', key: op.key, applied: true })
        continue
      }
      const k = kvKey(op.ns, op.key)
      const row = b.kv.get(k)
      if (op.op === 'delete') {
        b.kv.delete(k)
        if (row) moved = true
        results.push({ index: items.length + i, opIndex: i, type: 'kv', op: 'delete', key: op.key, applied: !!row })
        continue
      }
      if (op.op === 'putIfAbsent' && row) {
        results.push({ index: items.length + i, opIndex: i, type: 'kv', op: op.op, key: op.key, applied: false, version: row.version })
        continue
      }
      const version = ++b.kvVersion
      const value = JSON.parse(JSON.stringify(op.value ?? null))
      // Progress, for the harness's idle test: anything but a position record whose position stayed.
      if (!row || !samePosition(row.value, value)) moved = true
      b.kv.set(k, { value, version, updatedAt: iso(t) })
      results.push({ index: items.length + i, opIndex: i, type: 'kv', op: op.op, key: op.key, value, version, applied: true })
    }
    if (moved) progress()
    b.wake()
    return { success: true, transactionId: `txn-${b.stats.txns}`, results }
  }

  function samePosition(a, c) {
    if (!a || !c || typeof a !== 'object' || typeof c !== 'object') return false
    if (!('position' in a) || !('queryId' in a)) return false
    const strip = (v) => {
      const o = { ...v }
      delete o.updatedAt
      return JSON.stringify(o)
    }
    return strip(a) === strip(c)
  }

  // -- ephemeral --

  const ephWake = () => {
    const w = b.ephemeralWaiters
    b.ephemeralWaiters = []
    for (const fn of w) fn()
  }
  b.ephemeralPush = (body) => {
    if (!body || typeof body.queue !== 'string') throw bad('ephemeral push: the body names a queue')
    const q = b.ephemeral.get(body.queue) || []
    for (const m of body.messages || []) {
      q.push({
        id: `e-${++b.ephemeralSeq}`,
        partition: m.partition ?? body.partition ?? 'Default',
        payload: JSON.parse(JSON.stringify(m.payload ?? m.data ?? null)),
        attempts: 1
      })
    }
    b.ephemeral.set(body.queue, q)
    ephWake()
    return { queue: body.queue, pushed: (body.messages || []).length }
  }
  b.ephemeralPop = async (params) => {
    const queue = params.get('queue')
    const batch = Math.max(1, Number(params.get('batch') || 1))
    const wait = params.get('wait') === 'true'
    const timeout = Math.min(Number(params.get('timeout') || 0), MAX_WAIT_MS)
    const take = () => {
      const q = b.ephemeral.get(queue) || []
      const out = q.splice(0, batch)
      return { queue, messages: out }
    }
    let got = take()
    if (got.messages.length || !wait || timeout <= 0) return got
    await new Promise((resolve) => {
      const timer = setTimeout(() => { b.ephemeralWaiters = b.ephemeralWaiters.filter((w) => w !== done); resolve() }, timeout)
      function done() { clearTimeout(timer); resolve() }
      b.ephemeralWaiters.push(done)
    })
    got = take()
    return got
  }

  return b
}

// ---- HTTP -------------------------------------------------------------------

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    req.on('data', (c) => chunks.push(c))
    req.on('end', () => {
      const text = Buffer.concat(chunks).toString('utf8')
      if (!text) return resolve(null)
      try { resolve(JSON.parse(text)) } catch (err) { reject(bad(`the body is not JSON: ${err.message}`)) }
    })
    req.on('error', reject)
  })
}

export function serve({ port = 0, host = '127.0.0.1', quiet = false } = {}) {
  let b = newBroker()
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x')
    const path = url.pathname
    const send = (status, body) => {
      const text = JSON.stringify(body)
      res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) })
      res.end(text)
    }
    const route = `${req.method} ${path.startsWith('/api/v1/ephemeral/queue/') ? '/api/v1/ephemeral/queue/:q' : path}`
    try {
      const body = req.method === 'GET' || req.method === 'DELETE' ? null : await readBody(req)
      if (path.startsWith('/_control/')) {
        if (route === 'POST /_control/reset') { b = newBroker(body && body.start ? { start: Date.parse(body.start) } : {}); return send(200, { ok: true }) }
        if (route === 'POST /_control/tx') return send(200, b.controlTx(body && body.txs))
        if (route === 'POST /_control/clock') return send(200, b.controlClock(body && body.at))
        if (route === 'POST /_control/kv') return send(200, b.controlKv(body || {}))
        if (route === 'POST /_control/inject') { b.injected.push({ times: 1, when: 'before', status: 503, ...body }); return send(200, { ok: true }) }
        if (route === 'GET /_control/dump') return send(200, b.dump())
        if (route === 'GET /_control/progress') return send(200, b.progressOf(url.searchParams.get('ns'), url.searchParams.get('key')))
        return send(404, { error: `no control route ${route}` })
      }
      b.stats.requests[route] = (b.stats.requests[route] || 0) + 1
      // An injected failure for this route: before it applies, or after (the answer is lost).
      const inj = b.injected.find((x) => x.route === path && x.times > 0)
      if (inj) inj.times--
      if (inj && inj.when !== 'after') return send(inj.status, inj.body || { error: 'injected failure', code: 'injected' })
      let answer
      if (route === 'POST /api/v1/feed') answer = await b.read(body || {})
      else if (route === 'POST /api/v1/kv') answer = b.kvCall(Array.isArray(body) ? body : body && body.operations)
      else if (route === 'POST /api/v1/transaction') answer = b.transaction(body || {})
      else if (route === 'POST /api/v1/locks') answer = b.locksCall(Array.isArray(body) ? body : body && body.operations)
      else if (route === 'POST /api/v1/ephemeral/push') { answer = b.ephemeralPush(body); return send(201, { pushed: answer.pushed }) }
      else if (route === 'GET /api/v1/ephemeral/pop') answer = await b.ephemeralPop(url.searchParams)
      else if (route === 'DELETE /api/v1/ephemeral/queue/:q') {
        b.ephemeral.delete(decodeURIComponent(path.slice('/api/v1/ephemeral/queue/'.length)))
        answer = { deleted: true }
      } else if (route === 'GET /health' || route === 'GET /api/v1/health') {
        answer = { status: 'healthy', engine: 'fake', version: '2.7.1' }
      } else {
        b.stats.unknown[route] = (b.stats.unknown[route] || 0) + 1
        if (!quiet) process.stderr.write(`[fake-broker] no route ${route}\n`)
        return send(404, { error: `the fake broker has no ${route}`, code: 'no_such_route' })
      }
      if (inj && inj.when === 'after') return send(inj.status, inj.body || { error: 'injected failure after apply', code: 'injected' })
      return send(200, answer)
    } catch (err) {
      if (err instanceof HttpError) return send(err.status, err.body)
      if (!quiet) process.stderr.write(`[fake-broker] ${route}: ${err && err.stack ? err.stack : err}\n`)
      return send(500, { error: String(err && err.message ? err.message : err), code: 'internal' })
    }
  })
  return new Promise((resolve) => {
    server.listen(port, host, () => resolve({ server, port: server.address().port, broker: () => b }))
  })
}

const isMain = process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href
if (isMain) {
  const i = process.argv.indexOf('--port')
  const port = i > 0 ? Number(process.argv[i + 1]) : 0
  const { port: bound } = await serve({ port, quiet: process.argv.includes('--quiet') })
  process.stdout.write(`${JSON.stringify({ port: bound })}\n`)
}
