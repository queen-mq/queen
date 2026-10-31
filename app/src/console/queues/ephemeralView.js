// One ephemeral queue in the Queues section (?show=eph&name=), as data: its
// groups and partitions from its depth route, what it drops and why, what
// survives what, and the two writes (reset, delete) with their answers.
//
// Pure: no Vue, no '@/' imports (test/queues-messages.test.js).
//
// What the broker says, as read in server/src/handlers/ephemeral.rs and
// server/src/ephemeral.rs:
//   - GET /api/v1/ephemeral/queues and .../queues/:queue/depth answer from the
//     memory of the node that answered: each node counts only the partitions it
//     holds, and no route says which node holds which partition
//   - depth: {pending, bytes, partitions: [{partition, pending, bytes}],
//     groups: [{group, pending, skipped}]}; 404 ephemeral_queue_not_found when
//     the queue is not on that node
//   - the list's options are the effective configuration: maxLength, maxBytes,
//     policy (reject | dropOldest), ttlSeconds (0: no age limit),
//     leaseSeconds, retryLimit
//   - reset answers {queue, dropped}: what THAT node dropped; every other node
//     drops its own, fire and forget
//   - delete answers {queue, deleted, declared}: deleted is whether a stored
//     configuration went; every node drops its messages either way
//   - a node that stops (SIGTERM) hands its partitions to the next owner; a
//     crash loses what the node held; a declared queue's configuration is a row
//     of the log, an implicit queue is collected once empty and idle
import { callText } from './messagesView.js'

export { callText }
export const QUEUE_MODE = '__QUEUE_MODE__'

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const fmt = (n) => (n == null || !Number.isFinite(Number(n)) ? '–' : Math.round(Number(n)).toLocaleString('en-US'))
const plural = (n, one, many) => `${fmt(n)} ${n === 1 ? one : many}`

/** Bytes in words: 16 MB, 1.5 KB. */
export function size(b) {
  const v0 = num(b)
  if (v0 == null) return '–'
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let v = v0; let i = 0
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++ }
  return `${v < 10 && i > 0 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${u[i]}`
}

/** Seconds in words: 30 s, 10 min, 2 h. */
export function dur(seconds) {
  const s = Math.max(0, Math.round(num(seconds) || 0))
  if (s < 60) return `${s} s`
  if (s < 3600) return s % 60 ? `${Math.floor(s / 60)} min ${s % 60} s` : `${s / 60} min`
  const h = Math.floor(s / 3600); const m = Math.round((s % 3600) / 60)
  return m ? `${h} h ${m} min` : `${h} h`
}

// ---- the list -----------------------------------------------------------------

/** Thousands of request/reply inboxes are this class's normal: the list draws at most this many. */
export const MAX_ROWS = 200

/** Every message a queue let go unread: the three counters, or null when none was reported. */
export function droppedOf(q) {
  const parts = [q?.drops?.bounds, q?.drops?.ttl, q?.drops?.retry].map(num)
  return parts.every((v) => v == null) ? null : parts.reduce((s, v) => s + (v || 0), 0)
}

/** The columns a header click sorts by. */
export const EPH_SORTS = {
  depth: { label: 'waiting', get: (q) => q.depth },
  bytes: { label: 'memory', get: (q) => q.bytes },
  dropped: { label: 'dropped', get: (q) => droppedOf(q) },
}

/** A header click: largest first, then smallest first, then back to names. */
export function nextSort(current, key) {
  if (!current || current.key !== key) return { key, dir: 'desc' }
  if (current.dir === 'desc') return { key, dir: 'asc' }
  return null
}

/** The order as it stood at the click (name -> rank): a refresh never moves a row. Unknown values last. */
export function snapshotOrder(queues = [], { key, dir } = {}) {
  const get = EPH_SORTS[key]?.get
  if (!get) return null
  const sign = dir === 'asc' ? 1 : -1
  const list = [...queues].sort((a, b) => {
    const va = num(get(a)); const vb = num(get(b))
    if (va == null && vb == null) return a.name.localeCompare(b.name)
    if (va == null) return 1
    if (vb == null) return -1
    return sign * (va - vb) || a.name.localeCompare(b.name)
  })
  return new Map(list.map((q, i) => [q.name, i]))
}

/**
 * The rows to draw: narrowed by the page's filter and by tier, in the frozen
 * order (or by name; a queue that appeared since the click goes last), at most
 * `max`. → { rows, matched }
 */
export function listRows(queues = [], { find = '', tier = '', rank = null, max = MAX_ROWS } = {}) {
  const t = find.trim().toLowerCase()
  const matched = (Array.isArray(queues) ? queues : [])
    .filter((q) => q && typeof q.name === 'string' && q.name)
    .filter((q) => !t || q.name.toLowerCase().includes(t))
    .filter((q) => !tier || (tier === 'declared' ? q.declared === true : q.declared === false))
    .sort((a, b) => {
      if (rank) {
        const ra = rank.has(a.name) ? rank.get(a.name) : Infinity
        const rb = rank.has(b.name) ? rank.get(b.name) : Infinity
        if (ra !== rb) return ra - rb
      }
      return a.name.localeCompare(b.name)
    })
  return { rows: matched.slice(0, max), matched: matched.length }
}

// ---- the requests -----------------------------------------------------------

/** Drop every message, void the leases, move every group to the end. The queue stays. */
export const resetRequest = (queue) => ({ method: 'POST', path: '/api/v1/ephemeral/reset', body: { queue } })

/** Drop every message and forget the queue, with its stored configuration if it has one. */
export const deleteRequest = (queue) => ({ method: 'DELETE', path: `/api/v1/ephemeral/queue/${encodeURIComponent(queue)}` })

/** The toast after a reset: the broker's own count, said as the one node's. */
export function resetAnswer(data) {
  const n = num(data?.dropped)
  if (n == null) return 'The broker answered 200. Every node drops its messages of it.'
  return `The node that answered dropped ${plural(n, 'message', 'messages')}; every other node drops its own.`
}

/** The toast after a delete. */
export function deleteAnswer(data) {
  return data?.deleted === true || data?.declared === true
    ? 'The broker answered 200: its stored configuration is gone, and every node drops its messages.'
    : 'The broker answered 200: every node drops its messages. It had no stored configuration.'
}

/** "the node that answered holds 12" — the one count a dialog can honestly give. */
export function heldWords(depth) {
  const n = num(depth)
  if (n == null) return 'the node that answered did not say how many it holds'
  if (n === 0) return 'the node that answered holds none right now'
  return `the node that answered holds ${plural(n, 'message', 'messages')}`
}

// ---- what it says ---------------------------------------------------------

/** The depth route's groups as rows, by name. */
export function groupRows(depth) {
  const raw = Array.isArray(depth?.groups) ? depth.groups : null
  if (!raw) return null
  return raw
    .filter((g) => g && typeof g === 'object')
    .map((g) => {
      const name = String(g.group ?? g.name ?? '')
      return { name, label: !name || name === QUEUE_MODE ? 'queue mode' : name, pending: num(g.pending ?? g.lag), skipped: num(g.skipped) }
    })
    .sort((a, b) => a.label.localeCompare(b.label))
}

/** The queue's options from the list's raw rows, or null. */
export function optionsOf(list, name) {
  const rows = Array.isArray(list?.queues) ? list.queues : Array.isArray(list) ? list : []
  const row = rows.find((q) => q && (q.queue ?? q.name) === name)
  return row && row.options && typeof row.options === 'object' ? row.options : null
}

/**
 * The three ways a message leaves unread, each with its rule (from the
 * effective options) and how many went that way.
 * → [{ key, label, rule, n }]
 */
export function lossRows(drops = {}, options = null) {
  const o = options || {}
  const maxLength = num(o.maxLength); const maxBytes = num(o.maxBytes)
  const ttl = num(o.ttlSeconds); const lease = num(o.leaseSeconds); const retries = num(o.retryLimit)
  let bounds = ''
  if (maxLength != null || maxBytes != null) {
    const cap = [maxLength != null ? plural(maxLength, 'message', 'messages') : '', maxBytes != null ? size(maxBytes) : ''].filter(Boolean).join(' or ')
    bounds = o.policy === 'dropOldest'
      ? `It holds ${cap}; past that, the oldest are dropped.`
      : o.policy === 'reject'
        ? `It holds ${cap}; past that, a push is refused and nothing is dropped.`
        : `It holds ${cap}.`
  }
  let age = ''
  if (ttl != null) age = ttl > 0 ? `A message waits at most ${dur(ttl)}.` : 'No age limit.'
  // A lease that runs out and a failed ack both spend a try; a group that has
  // spent them all drops the message for itself (ephemeral.rs sweep_leases).
  let retry = ''
  if (retries != null) {
    const leaseText = lease != null ? `its ${dur(lease)} lease` : 'its lease'
    retry = retries > 0
      ? `Delivered again when ${leaseText} runs out or a worker fails it, ${retries === 1 ? 'once' : `up to ${fmt(retries)} times`}; then that group drops it.`
      : `When ${leaseText} runs out or a worker fails it, that group drops it.`
  }
  return [
    { key: 'bounds', label: 'Too many', rule: bounds, n: num(drops?.bounds) },
    { key: 'ttl', label: 'Too old', rule: age, n: num(drops?.ttl) },
    { key: 'retry', label: 'Out of tries', rule: retry, n: num(drops?.retry) },
  ]
}

/** What survives what, for this queue. → [{ when, kept, why }] */
export function survival(declared) {
  return [
    { when: 'A deploy, node by node', kept: true, why: 'A node that stops hands its partitions to the next one.' },
    { when: 'A node crash', kept: false, why: 'What that node held is gone: nothing is on disk.' },
    { when: 'Every node stopping at once', kept: false, why: 'No node is left to take them. A one-node broker is this case on every restart.' },
    declared === true
      ? { when: 'Its configuration', kept: true, why: 'It is stored in the log, so the queue comes back, empty.' }
      : declared === false
        ? { when: 'The queue itself', kept: false, why: 'Nothing is stored: a later push or pop makes it again, empty.' }
        : null,
  ].filter(Boolean)
}
