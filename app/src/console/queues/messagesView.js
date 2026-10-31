// The Messages view of the Queues section, as data: its filters from the
// address (?mq= ?mns= ?mtask= ?mpart= ?mstatus=, the pick ?mpid= ?mtx=), the
// query GET /api/v1/messages gets with the band's window, its rows in the
// console's words, what a refresh may change (the list holds still: a row's
// status updates in place, newer messages are counted until you ask for
// them), the picked message's story, and the requests its actions send.
//
// Pure: no Vue, no '@/' imports (test/queues-messages.test.js).
//
// The broker's route, as read in server/src/rsm/facade/real/phase2/reads.rs
// (api_messages, api_message, message_status):
//   - newest first, in [from, to) (to rounded up to the next minute), paged by
//     limit/offset; an offset past 10,000 answers 400 (QUEEN_MESSAGES_MAX_OFFSET)
//   - `total` is the count of THIS page, not of every match
//   - a page cut by its byte budget says `pagination.hasMore` and `nextOffset`
//   - a status is read against the partition's cursors: completed when every
//     group's cursor (or queue mode's) is past it, processing when any lease on
//     the partition is live, dead_letter when a dead-letter row holds it
import { kafkaRecordOf } from '../../composables/kafkaRecord.js'
import { isEncryptedEnvelope } from '../../composables/usePushVerdict.js'

export const PAGE = 20
export const MAX_OFFSET = 10_000
export const QUEUE_MODE = '__QUEUE_MODE__'

export const STATUSES = [
  { value: '', label: 'All' },
  { value: 'pending', label: 'Waiting' },
  { value: 'processing', label: 'Leased' },
  { value: 'completed', label: 'Done' },
  { value: 'dead_letter', label: 'Dead letters' },
]
const STATUS_VALUES = STATUSES.map((s) => s.value).filter(Boolean)
export const WORD = { pending: 'waiting', processing: 'leased', completed: 'done', dead_letter: 'dead letter', failed: 'failed' }
const NOUN = { '': 'messages', pending: 'waiting messages', processing: 'leased messages', completed: 'done messages', dead_letter: 'dead letters' }

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const fmt = (n) => (n == null || !Number.isFinite(Number(n)) ? '–' : Math.round(Number(n)).toLocaleString('en-US'))
const one = (v) => (Array.isArray(v) ? v[0] : v)
const text = (v) => (typeof one(v) === 'string' ? one(v).trim() : '')
export const groupWord = (name) => (name === QUEUE_MODE ? 'queue mode' : name)

// ---- the address ----------------------------------------------------------

/** The filters as the address holds them; an unknown status reads as All. */
export function readFilters(query = {}) {
  const status = text(query.mstatus)
  return {
    queue: text(query.mq),
    namespace: text(query.mns),
    task: text(query.mtask),
    partition: text(query.mpart),
    status: STATUS_VALUES.includes(status) ? status : '',
  }
}

/** The pick (?mpid= ?mtx=): the detail route's address, or null. */
export function readPick(query = {}) {
  const pid = text(query.mpid)
  const txn = typeof one(query.mtx) === 'string' ? one(query.mtx) : ''
  return pid && txn ? { pid, txn } : null
}

const FIELD = { queue: 'mq', namespace: 'mns', task: 'mtask', partition: 'mpart', status: 'mstatus' }

/**
 * The next address after a filter change: the other fields kept, empty ones
 * dropped, and the pick cleared, since the list it was picked from is gone.
 */
export function withFilters(query = {}, patch = {}) {
  const next = { ...query }
  for (const [k, v] of Object.entries(patch)) {
    const key = FIELD[k]
    if (!key) continue
    const s = typeof v === 'string' ? v.trim() : ''
    if (s) next[key] = s
    else delete next[key]
  }
  delete next.mpid
  delete next.mtx
  return next
}

// ---- the read -------------------------------------------------------------

/** The query of GET /api/v1/messages: the filters, the window, the page. */
export function listParams(filters = {}, window = {}, offset = 0, limit = PAGE) {
  const p = {}
  for (const k of ['queue', 'namespace', 'task', 'partition', 'status']) if (filters[k]) p[k] = filters[k]
  if (window.from) p.from = window.from
  if (window.to) p.to = window.to
  p.limit = limit
  if (offset > 0) p.offset = offset
  return p
}

/** The call as the read line shows it: encoded, but a time's colons left readable. */
export function listCall(params = {}) {
  const q = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v).replace(/%3A/gi, ':')}`).join('&')
  return `GET /api/v1/messages${q ? `?${q}` : ''}`
}

/** The window in words, for a sentence ("pushed in the last hour"). */
export function windowWords(win = {}) {
  if (win.custom) return `pushed from ${String(win.label || '').replace(' – ', ' to ')}`
  return {
    '15m': 'pushed in the last 15 minutes',
    '6h': 'pushed in the last 6 hours',
    '24h': 'pushed in the last 24 hours',
    '7d': 'pushed in the last 7 days',
  }[win.id] || 'pushed in the last hour'
}

/** The page's messages as rows. */
export function messageRows(messages) {
  return (Array.isArray(messages) ? messages : []).filter((m) => m && typeof m === 'object').map((m, i) => {
    const at = Date.parse(m.createdAt)
    const total = num(m.busStatus?.totalGroups)
    const raw = m.busStatus?.consumedBy
    const read = Array.isArray(raw) ? raw.length : num(raw)
    return {
      key: m.id || `${m.partitionId || ''}\u0000${m.transactionId || ''}\u0000${m.offset ?? i}`,
      pid: m.partitionId || '',
      txn: m.transactionId || '',
      queue: m.queue || '',
      partition: m.partition || '',
      at: Number.isFinite(at) ? at : null,
      status: m.status || '',
      word: WORD[m.status] || m.status || '–',
      // Only while groups that fan out are still at it: one group's word, or
      // "done", already says it.
      read: (m.status === 'pending' || m.status === 'processing') && total != null && total > 1 && read != null ? `${fmt(read)} of ${fmt(total)} read` : '',
      addressable: Boolean(m.partitionId && m.transactionId),
      raw: m,
    }
  })
}

/**
 * What a refresh may change on screen. The rows stay where they are and keep
 * their order; a row the fresh page still holds takes its fresh status. Rows
 * newer than the newest one shown are counted, not inserted.
 * → { rows, newer }
 */
export function mergeRefresh(current = [], fresh = []) {
  if (!current.length) return { rows: fresh, newer: 0 }
  if (current.length === fresh.length && current.every((r, i) => r.key === fresh[i].key)) return { rows: fresh, newer: 0 }
  const byKey = new Map(fresh.map((r) => [r.key, r]))
  const shown = new Set(current.map((r) => r.key))
  const head = current[0].at ?? -Infinity
  return {
    rows: current.map((r) => byKey.get(r.key) || r),
    newer: fresh.filter((r) => !shown.has(r.key) && (r.at ?? -Infinity) > head).length,
  }
}

/** Where "Older" goes, and whether it may: a page cut by bytes says where the next starts. */
export function older({ offset = 0, count = 0, pagination = null, limit = PAGE } = {}) {
  const cut = pagination?.hasMore === true && Number.isInteger(pagination?.nextOffset)
  const more = cut || count >= limit
  const next = cut ? pagination.nextOffset : offset + count
  const deep = more && next > MAX_OFFSET
  return { can: more && !deep, next, deep }
}

/** The sentence of an empty page, as parts ({ t, mono }). */
export function emptyParts(filters = {}, { offset = 0, when = 'pushed in the last hour' } = {}) {
  const out = [{ t: offset > 0 ? `No older ${NOUN[filters.status || ''] || 'messages'}` : `No ${NOUN[filters.status || ''] || 'messages'}` }]
  const add = (lead, value) => { out.push({ t: lead }); out.push({ t: value, mono: true }) }
  if (filters.queue) add(' in ', filters.queue)
  else if (filters.namespace) add(' in namespace ', filters.namespace)
  else if (filters.task) add(' with task ', filters.task)
  if (filters.partition) add(filters.queue || filters.namespace || filters.task ? ', partition ' : ' in partition ', filters.partition)
  out.push({ t: `${filters.partition ? ',' : ''} ${when}.` })
  return out
}

// ---- the picked message ---------------------------------------------------

/**
 * Each group of the partition and what it did with the message, from the
 * detail route's consumerGroups ({name, committed, consumed, leaseExpiresAt}).
 * A lease is the partition's, not the message's: a live one on a group that has
 * not passed the message means a worker of that group holds it now.
 * → [{ name, label, state: 'done'|'leased'|'waiting', until }] | null (not read yet)
 */
export function groupStates(m, now = Date.now()) {
  if (!m || !Array.isArray(m.consumerGroups)) return null
  return m.consumerGroups
    .filter((g) => g && typeof g.name === 'string')
    .map((g) => {
      const until = Date.parse(g.leaseExpiresAt)
      const live = Number.isFinite(until) && until > now
      const state = g.consumed === true ? 'done' : live ? 'leased' : 'waiting'
      return { name: g.name, label: groupWord(g.name), state, until: state === 'leased' ? until : null }
    })
    .sort((a, b) => (a.name === QUEUE_MODE) - (b.name === QUEUE_MODE) || a.label.localeCompare(b.label))
}

// "a, b and c" as parts, each name in mono; past three, "a, b and 4 more".
function andParts(names) {
  const shown = names.length <= 3 ? names : names.slice(0, 2)
  const out = []
  shown.forEach((n, i) => {
    if (i > 0) out.push({ t: i === shown.length - 1 && names.length <= 3 ? ' and ' : ', ' })
    out.push({ t: n, mono: true })
  })
  if (names.length > 3) out.push({ t: ` and ${fmt(names.length - 2)} more` })
  return out
}

/**
 * The message's state as one sentence that starts with its word, in parts
 * ({ t, mono }: a group's name is mono). `groups` is groupStates (null until
 * the detail is read); `time` words a lease's end.
 * → { word, parts, line, error }
 */
export function messageState(m, groups = null, time = (ms) => new Date(ms).toISOString()) {
  if (!m) return null
  const named = (groups || []).filter((g) => g.name !== QUEUE_MODE)
  const say = (word, parts, error = '') => ({ word, parts, line: parts.map((p) => p.t).join(''), error })
  switch (m.status) {
    case 'dead_letter': {
      const n = num(m.retryCount)
      return say('Dead letter', [{ t: n ? `Dead letter, after ${fmt(n)} ${n === 1 ? 'retry' : 'retries'}.` : 'Dead letter.' }],
        typeof m.errorMessage === 'string' && m.errorMessage ? m.errorMessage : '')
    }
    case 'completed':
      if (!groups) return say('Done', [{ t: 'Done: every reader is past it.' }])
      if (!named.length) return say('Done', [{ t: 'Done: a worker took it and acked it.' }])
      return say('Done', named.length === 1
        ? [{ t: 'Done: ' }, { t: named[0].label, mono: true }, { t: ' read it.' }]
        : [{ t: `Done: all ${fmt(named.length)} groups read it.` }])
    case 'processing': {
      const g = (groups || []).find((x) => x.state === 'leased')
      if (!g) return say('Leased', [{ t: 'Leased: a worker holds its partition now.' }])
      return say('Leased', g.name === QUEUE_MODE
        ? [{ t: `Leased: a worker holds its partition until ${time(g.until)}.` }]
        : [{ t: 'Leased: a worker of ' }, { t: g.label, mono: true }, { t: ` holds its partition until ${time(g.until)}.` }])
    }
    case 'pending': {
      if (!groups) return say('Waiting', [{ t: 'Waiting: no reader is past it yet.' }])
      const waiting = named.filter((g) => g.state !== 'done').map((g) => g.label)
      if (!waiting.length) return say('Waiting', [{ t: 'Waiting: no worker has taken it yet.' }])
      return say('Waiting', [{ t: 'Waiting for ' }, ...andParts(waiting), { t: '.' }])
    }
    default:
      return say(m.status || 'Unknown', m.status ? [{ t: `${m.status}.` }] : [])
  }
}

/** Whether the groups' own words add anything: not when they all say the same. */
export const groupsDiffer = (groups) => Array.isArray(groups) && new Set(groups.map((g) => g.state)).size > 1

/** The one filter an empty page should let go of first: the status, the partition, then the queue. */
export function relaxOf(filters = {}) {
  if (filters.status) return { patch: { status: '' }, label: 'Show every status' }
  if (filters.partition) return { patch: { partition: '' }, label: 'Show every partition' }
  if (filters.queue || filters.namespace || filters.task) return { patch: { queue: '', namespace: '', task: '' }, label: 'Show every queue' }
  return null
}

/**
 * The payload and the form it can be shown in:
 *   gone    the broker no longer stores it (retention)
 *   sealed  stored encrypted and the broker that answered has no key for it:
 *           what came back is the {encrypted, iv, authTag} envelope
 *   kafka   a record the Kafka facade wrote ({k, v, h, t}, base64 inside)
 *   json    any other JSON value; `encrypted` when it is stored encrypted and
 *           the broker decrypted it for this read
 */
export function payloadOf(m) {
  if (!m) return { kind: 'none' }
  if (m.payloadAvailable === false) return { kind: 'gone' }
  const value = m.payload !== undefined ? m.payload : m.data
  if (isEncryptedEnvelope(value)) return { kind: 'sealed' }
  const record = kafkaRecordOf(value)
  return { kind: record ? 'kafka' : 'json', value, record, encrypted: m.isEncrypted === true }
}

/** What "Push a copy" opens on: the same queue, partition and payload, a new transactionId. */
export function copyOf(m) {
  const p = payloadOf(m)
  if (p.kind !== 'json' && p.kind !== 'kafka') return null
  if (!m.queue) return null
  return { queue: m.queue, partition: m.partition || 'Default', payload: p.value }
}

const sq = (s) => `'${String(s).replace(/'/g, "'\\''")}'`

/**
 * The same payload to the same partition, as a curl line. Behind the proxy
 * every call needs a key, so the line carries the header with a variable.
 */
export function curlOf({ origin = '', queue, partition, payload, key = false } = {}) {
  const item = { queue, partition: partition || 'Default', payload }
  const parts = ['curl -X POST', sq(`${origin}/api/v1/push`), '-H', sq('Content-Type: application/json')]
  if (key) parts.push('-H', '"Authorization: Bearer $QUEEN_API_KEY"')
  parts.push('-d', sq(JSON.stringify({ items: [item] })))
  return parts.join(' ')
}

/** The traces of one message (GET /api/v1/traces/{pid}/{txn}): how many, and their names. */
export function traceSummary(answer) {
  const list = Array.isArray(answer?.traces) ? answer.traces : Array.isArray(answer?.events) ? answer.events : []
  const names = new Map()
  for (const t of list) {
    for (const n of Array.isArray(t?.names) ? t.names : Array.isArray(t?.trace_names) ? t.trace_names : []) {
      if (typeof n === 'string' && n) names.set(n, (names.get(n) || 0) + 1)
    }
  }
  const total = num(answer?.total)
  return {
    total: total ?? list.length,
    names: [...names.entries()].map(([name, n]) => ({ name, n })).sort((a, b) => b.n - a.n || a.name.localeCompare(b.name)),
  }
}

// ---- the writes -------------------------------------------------------------

/** The text a dialog shows for a request: method, path, body. */
export function callText(req) {
  return `${req.method} ${req.path}${req.body !== undefined ? ` ${JSON.stringify(req.body)}` : ''}`
}

/**
 * Delete one message's dead-letter rows (one per group that dead-lettered it).
 * The broker deletes only dead letters: a live message is in the log until its
 * retention ends, and the route answers it 404 (admin.rs message_delete_miss).
 */
export function deleteMessageRequest(partitionId, transactionId) {
  return { method: 'DELETE', path: `/api/v1/messages/${encodeURIComponent(partitionId)}/${encodeURIComponent(transactionId)}` }
}

/** The broker's answer to the delete: 200 {success, deleted}. → { ok, n, why } */
export function deleteAnswer(data) {
  if (data?.success === true) return { ok: true, n: num(data.deleted) || 1, why: '' }
  return { ok: false, n: 0, why: data?.message || data?.error || 'The broker did not delete it.' }
}
