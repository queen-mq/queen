/**
 * A broker in memory that speaks the four wires a feed stream uses --
 * `POST /api/v1/feed`, `/api/v1/kv`, `/api/v1/transaction`, `/api/v1/locks` --
 * so the tests run the client's own Kv, Lock, TransactionBuilder and
 * FeedReader against it.
 *
 * Every write is one log position, as on the broker: a transaction (its
 * pushes share the position), a lock call. Only pushes are feed entries. The
 * clock is virtual: `broker.nowMs` is the wall clock, which the test moves; a
 * write is stamped `max(nowMs, the last write's time)`, and a caught-up read
 * resolves to the last write's time, so the clock moves only with writes, as
 * on the broker. Locks expire on the real clock (the Lock handle keeps its
 * deadline on it).
 */
import { conditionHolds } from '../../client-v2/feed/Feed.js'
import { TransactionBuilder } from '../../client-v2/builders/TransactionBuilder.js'
import { Kv } from '../../client-v2/kv/Kv.js'
import { Locks, Lock } from '../../client-v2/locks/Locks.js'
import { FeedReader } from '../../client-v2/feed/Feed.js'
import { Ephemeral } from '../../client-v2/ephemeral/Ephemeral.js'
import { shardOf } from '../../client-v2/streams/runtime/feed/state.js'

const iso = (ms) => new Date(ms).toISOString()

function glob(pattern) {
  const re = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')
  return new RegExp(`^${re}$`)
}

export function fakeBroker({ start = Date.parse('2026-11-06T09:00:00.000Z') } = {}) {
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
    txns: [],
    feedReads: [],
    injected: [],
    clients: [],
    ephemeral: new Map(),
    ephemeralWaiters: [],
    ephemeralSeq: 0
  }

  const stamp = () => {
    b.applied++
    b.lastNowMs = Math.max(b.nowMs, b.lastNowMs)
    return { position: b.applied, t: b.lastNowMs }
  }
  const wake = () => {
    const w = b.waiters
    b.waiters = []
    for (const fn of w) fn()
  }
  const kvKey = (ns, key) => `${ns}\u0000${key}`

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
        id: `m-${position}-${offset}`, transactionId: it.transactionId, payload: it.payload, ts: iso(t)
      })
      results.push({ type: 'push', status: 'queued', transactionId: it.transactionId })
    }
    return results
  }

  /** A producer's push: one position for all of `items` ({ queue, partition, data, transactionId? }). */
  b.push = (items) => {
    const list = Array.isArray(items) ? items : [items]
    const { position, t } = stamp()
    appendPushes(list.map((x, i) => ({
      queue: x.queue, partition: x.partition, payload: x.data,
      transactionId: x.transactionId ?? `in-${position}-${i}`
    })), position, t)
    wake()
    return position
  }

  /** Entries of a queue, as stored. */
  b.messages = (queue) => b.log.filter((e) => e.queue === queue)

  b.kvGet = (ns, key) => b.kv.get(kvKey(ns, key))

  /** Write a KV row as another process's commit would (one position). */
  b.kvPut = (ns, key, value) => {
    stamp()
    const version = ++b.kvVersion
    b.kv.set(kvKey(ns, key), { value: JSON.parse(JSON.stringify(value)), version })
    return version
  }

  /** The KV keys of a namespace that start with `prefix`. */
  b.kvKeys = (ns, prefix = '') => [...b.kv.keys()]
    .filter((k) => k.startsWith(`${ns}\u0000${prefix}`))
    .map((k) => k.slice(ns.length + 1))

  b.read = async (body, again = false) => {
    if (!again) b.feedReads.push(JSON.parse(JSON.stringify(body)))
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
      if (shard && (!shardQueues || shardQueues.some((re) => re.test(e.queue))) && shardOf(e.partition, shard.count) !== shard.index) continue
      const msg = { queue: e.queue, partition: e.partition, transactionId: e.transactionId, offset: e.offset, position: e.position, data: e.payload }
      if (!where.every((c) => conditionHolds(c, msg))) continue
      out.push(e)
    }
    const caughtUp = next > b.applied
    if (out.length === 0 && caughtUp && (body.maxWaitMs || 0) > 0 && !again) {
      await new Promise((resolve) => {
        const timer = setTimeout(() => { b.waiters = b.waiters.filter((w) => w !== done); resolve() }, body.maxWaitMs)
        function done() { clearTimeout(timer); resolve() }
        b.waiters.push(done)
      })
      return b.read({ ...body, from }, true)
    }
    const last = b.log.filter((e) => e.position < next).pop()
    return {
      applied: b.applied,
      caughtUp,
      entries: out.map((e) => ({ ...e })),
      next,
      resolvedTs: iso(caughtUp ? b.lastNowMs : Date.parse(last ? last.ts : iso(b.lastNowMs)))
    }
  }

  b.kvCall = (operations) => {
    const results = operations.map((op, index) => {
      if (op.op === 'get') {
        const row = b.kv.get(kvKey(op.ns, op.key))
        return row ? { index, op: 'get', key: op.key, found: true, value: row.value, version: row.version } : { index, op: 'get', key: op.key, found: false }
      }
      if (op.op === 'getMany') {
        const rows = []
        const missing = []
        for (const k of op.keys) {
          const row = b.kv.get(kvKey(op.ns, k))
          if (row) rows.push({ key: k, value: row.value, version: row.version })
          else missing.push(k)
        }
        return { index, op: 'getMany', rows, missing, truncated: false }
      }
      if (op.op === 'getPrefix') {
        const keys = b.kvKeys(op.ns, op.prefix).sort().filter((k) => op.after === undefined || op.after === null || k > op.after)
        const limit = Math.min(op.limit ?? 100, 1000)
        const page = keys.slice(0, limit)
        const rows = page.map((k) => {
          const row = b.kv.get(kvKey(op.ns, k))
          return op.keysOnly ? { key: k, version: row.version } : { key: k, value: row.value, version: row.version }
        })
        const truncated = keys.length > limit
        return { index, op: 'getPrefix', rows, truncated, nextAfter: truncated ? page[page.length - 1] : null }
      }
      throw new Error(`fake broker: kv op ${op.op} not served`)
    })
    return { results }
  }

  function lockRow(name) {
    const row = b.locks.get(name)
    return row && row.expiresAt > Date.now() ? row : null
  }

  b.locksCall = (operations) => {
    const results = operations.map((op) => {
      const row = lockRow(op.name)
      if (op.op === 'acquire') {
        if (row && row.owner !== op.owner) return { acquired: false, reason: 'held', holders: [{ owner: row.owner, token: row.token }] }
        stamp()
        const token = ++b.tokens
        b.locks.set(op.name, { owner: op.owner, token, expiresAt: Date.now() + op.ttlSeconds * 1000 })
        return { acquired: true, slot: 0, token, owner: op.owner, guard: { op: 'check', ns: 'queen-locks', key: op.name, expect: token, required: true } }
      }
      if (op.op === 'renew') {
        if (!row || row.token !== op.token) return { renewed: false, reason: 'lost', holders: [] }
        stamp()
        const token = ++b.tokens
        b.locks.set(op.name, { owner: row.owner, token, expiresAt: Date.now() + op.ttlSeconds * 1000 })
        return { renewed: true, slot: 0, token, guard: { op: 'check', ns: 'queen-locks', key: op.name, expect: token, required: true } }
      }
      if (op.op === 'release') {
        if (!row || row.token !== op.token) return { released: false, reason: 'lost' }
        stamp()
        b.locks.delete(op.name)
        return { released: true }
      }
      if (op.op === 'get') return { held: !!row, holders: row ? [{ owner: row.owner, token: row.token }] : [] }
      throw new Error(`fake broker: lock op ${op.op} not served`)
    })
    return { results }
  }

  /**
   * Something for the next transaction: `before(broker)` runs first (another
   * process's commit, say); `error` is thrown instead of the answer, before
   * the transaction applies (`when: 'before'`) or after it (a lost answer).
   */
  b.injectNext = (spec) => b.injected.push(spec)

  b.transaction = (body) => {
    const items = []
    for (const op of body.operations || []) if (op.type === 'push') items.push(...op.items)
    const kv = body.kv || []
    const injected = b.injected.shift()
    if (injected && injected.before) injected.before(b)
    if (injected && injected.error && injected.when !== 'after') throw injected.error
    // Duplicates first, as the broker answers them.
    const seen = new Set()
    for (const it of items) {
      const k = `${it.queue}\u0000${it.partition ?? 'Default'}\u0000${it.transactionId}`
      if (b.ids.has(k) || seen.has(k)) {
        return { success: false, reason: 'duplicate', error: 'QDUP a pushed message is a duplicate; the transaction rolled back', results: [] }
      }
      seen.add(k)
    }
    if (kv.length > 64) return { success: false, reason: 'bad_request', error: `kv_too_many_ops: ${kv.length} ops in one call, the ceiling is 64` }
    for (let i = 0; i < kv.length; i++) {
      const op = kv[i]
      const fail = (kvReason, row) => ({
        success: false, reason: 'kv_precondition', failedIndex: items.length + i, kvReason,
        version: row ? row.version : null, value: row ? row.value : null,
        error: 'QKV a required KV precondition failed; the transaction rolled back', results: []
      })
      if (op.op === 'check' && op.ns === 'queen-locks') {
        const row = lockRow(op.key)
        if (!row) return fail('missing', null)
        if (row.token !== op.expect) return fail('version', { version: row.token, value: { owner: row.owner } })
        continue
      }
      const row = b.kv.get(kvKey(op.ns, op.key))
      if (op.op === 'check') {
        if (op.expect === 0 && row) return fail('exists', row)
        if (op.expect !== 0 && (!row || row.version !== op.expect)) return fail(row ? 'version' : 'missing', row)
        continue
      }
      if (op.op === 'putIfAbsent' && row) return fail('exists', row)
      if (op.op === 'put' && op.expect !== undefined) {
        if (!row) return fail('missing', null)
        if (row.version !== op.expect) return fail('version', row)
      }
      if (op.op === 'put' && !op.forever && !op.ttlSeconds) {
        return { success: false, reason: 'bad_request', error: 'kv_expiry_not_specified' }
      }
    }
    const { position, t } = stamp()
    const results = appendPushes(items.map((it) => ({ ...it, partition: it.partition ?? 'Default' })), position, t)
    const written = {}
    for (let i = 0; i < kv.length; i++) {
      const op = kv[i]
      if (op.op === 'check') {
        results.push({ index: items.length + i, type: 'kv', op: 'check', key: op.key })
        continue
      }
      const k = kvKey(op.ns, op.key)
      if (op.op === 'delete') {
        b.kv.delete(k)
        results.push({ index: items.length + i, type: 'kv', op: 'delete', key: op.key, applied: true })
        continue
      }
      const version = ++b.kvVersion
      b.kv.set(k, { value: JSON.parse(JSON.stringify(op.value)), version })
      written[op.key] = version
      results.push({ index: items.length + i, type: 'kv', op: op.op, key: op.key, version, applied: true })
    }
    b.txns.push({ position, items, kv, written })
    wake()
    if (injected && injected.error && injected.when === 'after') throw injected.error
    return { success: true, transactionId: `txn-${position}`, results }
  }

  // Ephemeral queues, in memory: push, and a pop that commits on delivery
  // (the only kind the feed's live-state reads use), with a long poll.
  const ephWake = () => {
    const w = b.ephemeralWaiters
    b.ephemeralWaiters = []
    for (const fn of w) fn()
  }
  b.ephemeralPush = (body) => {
    const q = b.ephemeral.get(body.queue) || []
    for (const m of body.messages || []) q.push({ id: `e-${++b.ephemeralSeq}`, partition: body.partition ?? 'Default', payload: JSON.parse(JSON.stringify(m.payload ?? null)), attempts: 0 })
    b.ephemeral.set(body.queue, q)
    ephWake()
    return { pushed: (body.messages || []).length }
  }
  b.ephemeralPop = async (params) => {
    const queue = params.get('queue')
    const batch = Number(params.get('batch') || 1)
    const wait = params.get('wait') === 'true'
    const timeout = Number(params.get('timeout') || 0)
    const take = () => {
      const q = b.ephemeral.get(queue) || []
      const out = q.splice(0, batch)
      return { queue, messages: out }
    }
    let got = take()
    if (got.messages.length || !wait) return got
    await new Promise((resolve) => {
      const timer = setTimeout(() => { b.ephemeralWaiters = b.ephemeralWaiters.filter((w) => w !== done); resolve() }, timeout)
      function done() { clearTimeout(timer); resolve() }
      b.ephemeralWaiters.push(done)
    })
    got = take()
    return got
  }

  /** An HTTP client on the broker, as one process would have; `cut` makes it unreachable. */
  b.client = (name = 'client') => {
    const c = {
      name,
      cut: false,
      async post(path, body, timeoutMillis, affinityKey, retryKind, signal) {
        if (c.cut) throw Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' })
        await new Promise((r) => setImmediate(r))
        const copy = JSON.parse(JSON.stringify(body))
        if (path === '/api/v1/feed') {
          if (!signal) return b.read(copy)
          // A caller's signal closes the request, as the client's HttpClient does.
          const aborted = () => Object.assign(new Error('the caller aborted the request'), { aborted: true })
          if (signal.aborted) throw aborted()
          return Promise.race([b.read(copy), new Promise((_, reject) => signal.addEventListener('abort', () => reject(aborted()), { once: true }))])
        }
        if (path === '/api/v1/kv') return b.kvCall(copy.operations)
        if (path === '/api/v1/transaction') return b.transaction(copy)
        if (path === '/api/v1/locks') return b.locksCall(copy.operations)
        if (path === '/api/v1/ephemeral/push') return b.ephemeralPush(copy)
        throw new Error(`fake broker: no ${path}`)
      },
      async get(path) {
        if (c.cut) throw Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' })
        await new Promise((r) => setImmediate(r))
        const [route, query] = path.split('?')
        if (route === '/api/v1/ephemeral/pop') return b.ephemeralPop(new URLSearchParams(query))
        throw new Error(`fake broker: no GET ${route}`)
      },
      async delete(path) {
        const m = /^\/api\/v1\/ephemeral\/queue\/(.+)$/.exec(path)
        if (m) { b.ephemeral.delete(decodeURIComponent(m[1])); return { deleted: true } }
        throw new Error(`fake broker: no DELETE ${path}`)
      },
      async destroy() {}
    }
    b.clients.push(c)
    return c
  }

  return b
}

/**
 * A Queen client stand-in made of the client's own parts, on one fake HTTP
 * client. `ephemeral: true` gives it the ephemeral queues too (live-state
 * reads); without, a runner serves no state.
 */
export function fakeQueen(broker, name = 'queen', { ephemeral = false } = {}) {
  const http = broker.client(name)
  const locks = new Locks(http)
  const q = {
    http,
    kv: new Kv(http),
    transaction: () => new TransactionBuilder(http),
    lock: (n, opts) => new Lock(locks, n, opts),
    feed: (options) => new FeedReader(http, () => http, 'http', options),
    queue: (n) => ({ name: n })
  }
  if (ephemeral) q.ephemeral = new Ephemeral(http)
  return q
}

export const until = async (cond, ms = 3000, what = 'condition') => {
  const end = Date.now() + ms
  while (!(await cond())) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`)
    await new Promise((r) => setTimeout(r, 5))
  }
}
