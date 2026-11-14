// Routing in the console (PLAN_ROUTING.md §10), as data: a queue's part in a
// route as the broker sends it, the words the Queues list and a queue's page
// say about it, a filtered group's where, the options that make a new queue
// read other queues, the preview of a where on a source's last messages, and
// the delete that a source's readers refuse.
//
// The wire (server/src/rsm/facade/real/phase2.rs route_json, route_json_read),
// on the rows of GET /api/v1/resources/queues, on one queue and on a
// configure answer:
//   route: { from: [{queue, where, conditions}], partitionBy: 'same' | 'data.…' | 'headers.…',
//            group, catchingUp, readers: [{queue, where?} | {queue, group, where?}], holders: [...] }
// and on one queue's read only: each reader's and holder's `oldestUnconsumedAt`
// (ISO or null), each holder's `holdsRetention`, and while it catches up
// `catchUp: {read, total}`. A reader `{queue}` is a reading queue;
// `{queue, group}` is a filtered group of that queue. A filtered group reads
// through a hidden queue, `<queue>\u0001<group>`, that the console never
// shows: an answer that names one is read as its queue and its group.
//
// Pure: no Vue, no '@/' imports (test/routing.test.js).
import { span } from '../words.js'
import { parseWhere, readField, whereHolds, whereOf } from './whereText.js'

/** The separator of a filtered group's hidden queue (server/src/rsm/route/mod.rs HIDDEN_SEP). */
export const HIDDEN_SEP = '\u0001'

/** `{ queue, group }` of a hidden queue's name; null for any other name. */
export function splitHidden(name) {
  if (typeof name !== 'string') return null
  const i = name.indexOf(HIDDEN_SEP)
  return i < 0 ? null : { queue: name.slice(0, i), group: name.slice(i + 1) }
}
export const isHiddenQueue = (name) => typeof name === 'string' && name.includes(HIDDEN_SEP)

/** The options a reading queue refuses (400 reading_queue): it keeps what its sources keep (phase2.rs READER_KEEPS). */
export const READER_KEEPS = Object.freeze([
  'retentionEnabled', 'retentionSeconds', 'completedRetentionSeconds', 'maxWaitTimeSeconds',
  'dedupWindowSeconds', 'encryptionEnabled', 'maxSize', 'retentionSinkHold', 'retentionSinkHoldMaxSeconds',
])

const queuePath = (name) => `/queues/${encodeURIComponent(name)}`
const fmt = (n) => Math.round(Number(n)).toLocaleString('en-US')

// ---- the route object -----------------------------------------------------------

/**
 * One reader or holder, from `{queue}`, `{queue, group}`, a name, or a hidden
 * queue's name: `{ queue, group, where, oldestAt, holdsRetention }`, `oldestAt`
 * the ms of the oldest message it has not consumed (null: none, or not said).
 */
function readerEntry(x) {
  const raw = typeof x === 'string' ? { queue: x } : x
  if (!raw || typeof raw.queue !== 'string' || !raw.queue) return null
  const hidden = splitHidden(raw.queue)
  const group = hidden ? hidden.group : typeof raw.group === 'string' && raw.group ? raw.group : null
  const at = Date.parse(raw.oldestUnconsumedAt)
  return {
    queue: hidden ? hidden.queue : raw.queue,
    group,
    where: whereOf(raw.where ?? raw.conditions),
    oldestAt: Number.isFinite(at) ? at : null,
    holdsRetention: raw.holdsRetention === true,
  }
}
const entryKey = (r) => `${r.queue}\u0000${r.group || ''}`

function readerList(list) {
  const seen = new Set()
  const out = []
  for (const x of Array.isArray(list) ? list : []) {
    const r = readerEntry(x)
    if (!r || seen.has(entryKey(r))) continue
    seen.add(entryKey(r))
    out.push(r)
  }
  return out
}

/**
 * A queue's route (`route` on a queue row, on one queue's detail, or on a
 * configure answer), checked; null when the queue takes no part in one.
 *   { from: [{ queue, where }], reading, partitionBy, catchingUp, catchUp, readers, holders }
 * `reading`: it reads other queues. `where` is the string form, '' for none.
 * `catchUp`: { read, total } messages while it catches up, when one queue's
 * read said so; else null.
 */
export function routeOf(q) {
  const r = q && typeof q === 'object' ? q.route : null
  if (!r || typeof r !== 'object') return null
  const from = []
  for (const s of Array.isArray(r.from) ? r.from : []) {
    const name = typeof s === 'string' ? s : s?.queue
    if (typeof name !== 'string' || !name) continue
    from.push({ queue: name, where: typeof s === 'string' ? '' : whereOf(typeof s.where === 'string' ? s.where : s.conditions) })
  }
  const pb = typeof r.partitionBy === 'string' && r.partitionBy && r.partitionBy !== 'same' ? r.partitionBy : 'same'
  const read = Number(r.catchUp?.read)
  const total = Number(r.catchUp?.total)
  return {
    from,
    reading: from.length > 0,
    partitionBy: pb,
    catchingUp: r.catchingUp === true,
    catchUp: Number.isFinite(read) && Number.isFinite(total) && total >= 0 ? { read: Math.min(read, total), total } : null,
    readers: readerList(r.readers),
    holders: readerList(r.holders),
  }
}

/** The Queues list's quiet label of a reading queue: "from orders", "from orders and refunds", "from orders and 2 more". */
export function fromLabel(route) {
  const names = route?.reading ? route.from.map((s) => s.queue) : []
  if (!names.length) return ''
  if (names.length === 1) return `from ${names[0]}`
  if (names.length === 2) return `from ${names[0]} and ${names[1]}`
  return `from ${names[0]} and ${names.length - 1} more`
}

// ---- a queue's page: what it reads, and who reads it --------------------------------
// Lines as parts, { t, mono?, to? }: a part with `to` is a link.

const queueRef = (name) => ({ t: name, mono: true, to: queuePath(name) })
const groupRef = (queue, group) => ({ t: group, mono: true, to: { path: queuePath(queue), query: { group } } })

/** "a", "a and b", "a, b and c"; past `max`, "a, b, c, d and 3 more". Each item is a list of parts. */
function listInto(parts, items, max) {
  const shown = items.length > max ? items.slice(0, max) : items
  shown.forEach((item, i) => {
    if (i > 0) parts.push({ t: i === shown.length - 1 && items.length <= max ? ' and ' : ', ' })
    parts.push(...item)
  })
  if (items.length > max) parts.push({ t: ` and ${fmt(items.length - max)} more` })
}

/**
 * How long a reading queue's messages last whether read or not, in seconds:
 * the shorter of its retention window and its message TTL, which the broker
 * copies from its sources (route/mod.rs keep_like). null when neither is set:
 * then a message lasts until it is read, or for as long as its source keeps it.
 */
export function keepWindow(options) {
  const o = options && typeof options === 'object' ? options : {}
  const set = []
  if (o.retentionEnabled === true && Number(o.retentionSeconds) > 0) set.push(Number(o.retentionSeconds))
  if (Number(o.maxWaitTimeSeconds) > 0) set.push(Number(o.maxWaitTimeSeconds))
  return set.length ? Math.min(...set) : null
}

/**
 * A reading queue's one line: "Reads orders where data.country = 'IT' ·
 * keeps what orders keeps (1 d)", "· partitioned by data.customerId" when it
 * re-keys, "· catching up: 4,210 of 12,000" while it reads its sources'
 * history (its messages read, of those the sources kept when it started; one
 * queue's read says how far, the list only that it does). null for a queue
 * that reads no other. `options` is the queue's own (its keep settings are
 * its sources').
 */
export function readsLine(route, { options = null } = {}) {
  if (!route?.reading) return null
  const parts = [{ t: 'Reads ' }]
  const wheres = new Set(route.from.map((s) => s.where))
  const shared = wheres.size === 1 ? [...wheres][0] : null
  listInto(parts, route.from.map((s) => (shared === null && s.where
    ? [queueRef(s.queue), { t: ' where ' }, { t: s.where, mono: true }]
    : [queueRef(s.queue)])), Infinity)
  if (shared) parts.push({ t: ' where ' }, { t: shared, mono: true })
  parts.push({ t: ' · keeps what ' })
  if (route.from.length === 1) parts.push(queueRef(route.from[0].queue), { t: ' keeps' })
  else parts.push({ t: 'its sources keep' })
  const keep = keepWindow(options)
  if (keep) parts.push({ t: ` (${span(keep)})` })
  if (route.partitionBy !== 'same') parts.push({ t: ' · partitioned by ' }, { t: route.partitionBy, mono: true })
  if (route.catchingUp) {
    parts.push({ t: route.catchUp ? ` · catching up: ${fmt(route.catchUp.read)} of ${fmt(route.catchUp.total)}` : ' · catching up' })
  }
  return parts
}

/** How far behind a reader is, from the oldest message it has not consumed: "2 min behind", or '' (none, or not said). */
export function behindText(oldestAt, now = Date.now()) {
  if (!Number.isFinite(oldestAt)) return ''
  return `${span(Math.max(0, (now - oldestAt) / 1000))} behind`
}

/**
 * A source's one line: "Read by orders-it (where …, 2 min behind) and
 * billing (filtered group, where …)", then "· also in manual-review" for the
 * queues that hold links to its messages without reading it. A reader's
 * where is its entry's; else a reading queue's own row in `queues` (its
 * `route.from` entry for this queue), a filtered group's row in `groups`. How
 * far behind a reader is comes from one queue's read (`oldestUnconsumedAt`);
 * none behind says nothing. `unread` (the queue's `messages.unread`) ends it
 * with what is on partitions that none of them takes. null when nothing reads
 * it.
 */
export function readByLine(route, { queue = '', queues = [], groups = [], max = 4, now = Date.now(), unread = 0 } = {}) {
  if (!route) return null
  const readers = route.readers
  const known = new Set(readers.map(entryKey))
  const also = route.holders.filter((h) => !known.has(entryKey(h)))
  if (!readers.length && !also.length) return null
  const rowOf = new Map((Array.isArray(queues) ? queues : []).filter((x) => x?.name).map((x) => [x.name, x]))
  // "(filtered group, where …, 2 min behind)": what is known of a reader, in one parenthesis.
  const about = (lead, where, behind) => {
    const bits = []
    if (lead) bits.push([{ t: lead }])
    if (where) bits.push([{ t: 'where ' }, { t: where, mono: true }])
    if (behind) bits.push([{ t: behind }])
    if (!bits.length) return []
    return [{ t: ' (' }, ...bits.flatMap((b, i) => (i ? [{ t: ', ' }, ...b] : b)), { t: ')' }]
  }
  const parts = []
  if (readers.length) {
    parts.push({ t: 'Read by ' })
    listInto(parts, readers.map((r) => {
      const behind = behindText(r.oldestAt, now)
      if (r.group) {
        const g = (Array.isArray(groups) ? groups : []).find((x) => x?.name === r.group && x.queueName === r.queue)
        return [groupRef(r.queue, r.group), ...about('filtered group', r.where || whereOf(g?.where), behind)]
      }
      const own = routeOf(rowOf.get(r.queue))?.from.find((s) => s.queue === queue)
      return [queueRef(r.queue), ...about('', r.where || own?.where, behind)]
    }), max)
  }
  if (also.length) {
    parts.push({ t: readers.length ? ' · also in ' : 'Its messages are also in ' })
    listInto(parts, also.map((h) => [h.group ? groupRef(h.queue, h.group) : queueRef(h.queue)]), max)
  }
  const left = Number(unread)
  if (readers.length && Number.isFinite(left) && left > 0) {
    parts.push({ t: ` · ${fmt(left)} on partitions none of them takes` })
  }
  return parts
}

/**
 * The holders that hold back a source's retention, one amber line each on
 * its page, oldest first. A holder counts as one more consumer group of the
 * source (PLAN_ROUTING.md §3.5): the source's completed retention keeps what
 * a holder has not consumed. The broker says a holder holds it
 * (`holdsRetention`) whenever it has anything unconsumed; it holds something
 * back only once that is older than the source's completed retention, the
 * age the source would delete it at otherwise, and only that is worth a look.
 * Without completed retention the source deletes by time alone, or nothing,
 * and no holder holds anything back.
 *   options  the source's own options
 * → [{ age, parts }] ([] when none)
 */
export function holdLines(route, { queue = '', options = null, now = Date.now() } = {}) {
  const o = options && typeof options === 'object' ? options : {}
  const keep = o.retentionEnabled === true && Number(o.completedRetentionSeconds) > 0 ? Number(o.completedRetentionSeconds) : null
  if (!route || keep == null) return []
  return route.holders
    .filter((h) => h.holdsRetention && Number.isFinite(h.oldestAt) && (now - h.oldestAt) / 1000 > keep)
    .sort((a, b) => a.oldestAt - b.oldestAt)
    .map((h) => {
      const age = (now - h.oldestAt) / 1000
      return {
        age,
        parts: [
          h.group ? groupRef(h.queue, h.group) : queueRef(h.queue),
          { t: `${h.group ? ' (filtered group)' : ''} holds back the retention of ` },
          { t: queue, mono: true },
          { t: `: its oldest unconsumed message is ${span(age)} old.` },
        ],
      }
    })
}

/**
 * Where else a message of a source shows (PLAN_ROUTING.md §10): the reading
 * queues and filtered groups whose where takes it, judged as the broker
 * judges a push (whereText, on the message's queue, partition, transactionId,
 * data and headers). A reading queue keyed by a field takes only a message
 * that has the field. It says what each where takes, not when each reader
 * began: one made later, from new messages on, does not hold an older one.
 *   m       the message (GET /api/v1/messages/…)
 *   queues  the queue list's rows: the source's `route`, and each reader's own
 * → { queues, groups, parts } | null (no reader takes it)
 */
export function alsoIn(m, { queues = [] } = {}) {
  if (!m || typeof m !== 'object' || typeof m.queue !== 'string') return null
  const rowOf = new Map((Array.isArray(queues) ? queues : []).filter((x) => x?.name).map((x) => [x.name, x]))
  const route = routeOf(rowOf.get(m.queue))
  if (!route?.readers.length) return null
  const inQueues = []
  const forGroups = []
  for (const r of route.readers) {
    const own = r.group ? null : routeOf(rowOf.get(r.queue))
    const parsed = parseWhere(r.where || own?.from.find((s) => s.queue === m.queue)?.where || '')
    if (!parsed.conds || !whereHolds(parsed.conds, m)) continue
    if (own && own.partitionBy !== 'same' && readField(own.partitionBy, m) === undefined) continue
    if (r.group) forGroups.push(r.group)
    else inQueues.push(r.queue)
  }
  if (!inQueues.length && !forGroups.length) return null
  const parts = [{ t: 'Also ' }]
  if (inQueues.length) {
    parts.push({ t: 'in ' })
    listInto(parts, inQueues.map((q) => [queueRef(q)]), 4)
  }
  if (forGroups.length) {
    parts.push({ t: `${inQueues.length ? ', and ' : ''}for ${forGroups.length === 1 ? 'group' : 'groups'} ` })
    listInto(parts, forGroups.map((g) => [groupRef(m.queue, g)]), 4)
  }
  parts.push({ t: '.' })
  return { queues: inQueues, groups: forGroups, parts }
}

// ---- consumer groups ---------------------------------------------------------------

/**
 * The groups listing (GET /api/v1/consumer-groups) with every filtered group
 * on its queue: a row that names a hidden queue reads as that queue, with
 * `filtered: true`. Other rows are returned as they came.
 */
export function shownGroups(list) {
  if (!Array.isArray(list)) return list
  return list.map((g) => {
    const h = splitHidden(g?.queueName)
    return h ? { ...g, queueName: h.queue, topics: [h.queue], filtered: true } : g
  })
}

/**
 * A group's filter: `{ where, known }`, or null for a group that reads all of
 * its queue. `where` is the string form; `known` says an answer gave it ('' is
 * then an empty where: every message). From the group's row (`filtered` and
 * `where`, as the broker sends them), else from its queue's route, whose
 * readers name it (`where` left out when empty, so '' there is not known).
 */
export function groupFilterOf(g, queueRow = null) {
  if (!g || typeof g !== 'object') return null
  const said = typeof g.where === 'string' || Array.isArray(g.where)
  if (said && (g.filtered === true || whereOf(g.where))) return { where: whereOf(g.where), known: true }
  const entry = routeOf(queueRow)?.readers.find((r) => r.group === g.name && r.queue === g.queueName)
  if (g.filtered === true || entry) return { where: entry?.where || '', known: !!entry?.where }
  return null
}

/** A filter with no where text to show, in words: '' when it has one (show the where itself). */
export function filterWords(filter) {
  if (!filter || filter.where) return ''
  return filter.known ? 'every message: its where is empty' : 'filtered; this listing does not say by what'
}

/** The partitions-behind answer (GET /api/v1/consumer-groups/lagging) with each hidden queue read as its queue. */
export function shownQueueName(name) {
  const h = splitHidden(name)
  return h ? h.queue : name
}

// ---- New queue: reading other queues -------------------------------------------------

/** "orders, refunds" → ['orders', 'refunds']: commas only (a name may hold a space), trimmed, once each. */
export function parseSources(text) {
  const out = []
  for (const s of String(text ?? '').split(',')) {
    const name = s.trim()
    if (name && !out.includes(name)) out.push(name)
  }
  return out
}

/** A partitionBy field the broker takes: `data.<path>` or `headers.<name>` (route/mod.rs partition_by_valid). */
export function partitionFieldValid(field) {
  const s = String(field ?? '')
  if (s.startsWith('headers.')) return /^[a-z0-9._-]{1,64}$/.test(s.slice('headers.'.length))
  if (!s.startsWith('data.')) return false
  const path = s.slice('data.'.length).split('.')
  return !path.some((p) => p === '') && path.length <= 16
}

const andNames = (names) => (names.length === 1 ? names[0] : `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`)

/**
 * The options that make a new queue read other queues, from the form:
 *   form   { from: 'orders, refunds', where, partitionMode: 'same'|'field', partitionField, start: 'now'|'all' }
 *   name   the new queue's name; known: anything with .has by name (queuesStore.queueMeta)
 * → { sources, where, conds, partitionBy, start, errors, options }
 * `options` go into the configure call: `from` (one name, or the list),
 * `where` only when there is one, `partitionBy` and `start` always, so the
 * call on screen says what the queue does. `errors` are the refusals the
 * broker would answer (route_loop, source_not_found, bad_where, a bad
 * partitionBy), keyed from, where and partitionBy.
 */
export function readingOptions(form = {}, { name = '', known = null } = {}) {
  const sources = parseSources(form.from)
  const errors = {}
  const unknown = typeof known?.has === 'function' ? sources.filter((s) => !known.has(s)) : []
  if (!sources.length) errors.from = 'Name the queues it reads.'
  else if (name && sources.includes(name)) errors.from = `${name} cannot read itself.`
  else if (sources.some(isHiddenQueue)) errors.from = 'A filtered group’s queue is not one to read.'
  else if (unknown.length) {
    errors.from = `${andNames(unknown)} ${unknown.length === 1 ? 'is not a queue' : 'are not queues'} on this cluster: a reading queue reads queues that exist.`
  }
  const where = String(form.where ?? '').trim()
  const parsed = parseWhere(where)
  if (parsed.error) errors.where = parsed.error
  const byField = form.partitionMode === 'field'
  const field = String(form.partitionField ?? '').trim()
  if (byField && !partitionFieldValid(field)) errors.partitionBy = 'A field: data.<path> or headers.<name>, like data.customerId.'
  const start = form.start === 'all' ? 'all' : 'now'
  const options = { from: sources.length === 1 ? sources[0] : sources }
  if (where) options.where = where
  options.partitionBy = byField ? field : 'same'
  options.start = start
  return { sources, where, conds: parsed.conds, partitionBy: options.partitionBy, start, errors, options }
}

// ---- the preview: the where on the sources' last messages ------------------------------

/** How many of each source's newest messages the preview reads, and over how many days. */
export const PREVIEW_LIMIT = 200
export const PREVIEW_DAYS = 7

/** The read behind the preview: GET /api/v1/messages for one source, its newest messages first. */
export function previewParams(source, now = Date.now()) {
  return { queue: source, limit: PREVIEW_LIMIT, from: new Date(now - PREVIEW_DAYS * 86_400_000).toISOString() }
}

/**
 * "37 of the last 200 messages of orders match": the where tried on the
 * sources' newest messages, as the broker would judge each one at push time.
 *   samples  Map source → { messages, cut? } | { error } (undefined: not read yet);
 *            `cut`: the broker ended the page early (its byte budget), so
 *            there were more messages than it holds
 * → { kind: 'reading'|'error'|'empty'|'count', n?, of?, text } | null (no where to try)
 */
export function previewOf({ sources = [], conds = null, samples = null } = {}) {
  if (!sources.length || !Array.isArray(conds) || !conds.length) return null
  const names = andNames(sources)
  const got = sources.map((s) => (typeof samples?.get === 'function' ? samples.get(s) : undefined))
  if (got.some((x) => !x)) return { kind: 'reading', text: `Trying it on the last messages of ${names}…` }
  const bad = got.find((x) => x.error)
  if (bad) return { kind: 'error', text: `The last messages of ${names} could not be read (${bad.error}), so it is not tried.` }
  const all = got.flatMap((x) => (Array.isArray(x.messages) ? x.messages : []))
  if (!all.length) {
    return { kind: 'empty', text: `${names} ${sources.length === 1 ? 'has' : 'have'} no message from the last ${PREVIEW_DAYS} days to try it on.` }
  }
  const n = all.filter((m) => whereHolds(conds, m)).length
  const full = got.every((x) => x.cut === true || (x.messages || []).length >= PREVIEW_LIMIT)
  const text = full
    ? `${fmt(n)} of the last ${fmt(all.length)} messages of ${names} match.`
    : `${fmt(n)} of the ${fmt(all.length)} messages ${names} got in the last ${PREVIEW_DAYS} days match.`
  return { kind: 'count', n, of: all.length, text }
}

// ---- deleting a queue that others read ---------------------------------------------------

/** The delete of a queue, as sent and as shown: `force` deletes a source its reading queues read. */
export function deleteQueueCall(name, { force = false } = {}) {
  const path = `/api/v1/resources/queues/${encodeURIComponent(name)}`
  return force
    ? { path, params: { force: true }, text: `DELETE ${path}?force=true` }
    : { path, params: undefined, text: `DELETE ${path}` }
}

/**
 * The broker's 409 for a queue reading queues read (phase2.rs
 * api_delete_queue_routed): `{ readers: [{ queue, group }], said }`, or null
 * for any other answer. Its readers are the reading queues; a filtered
 * group's hidden queue never stops a delete, it goes with the queue.
 */
export function hasReadersOf(err) {
  if (!err || err.status !== 409) return null
  const body = err.body && typeof err.body === 'object' ? err.body : null
  const code = typeof err.code === 'string' && err.code ? err.code : body?.code
  if (code !== 'has_readers') return null
  return { readers: readerList(body?.readers), said: typeof body?.error === 'string' ? body.error : '' }
}

/** The readers a refusal names, as parts: "orders-it and orders-fr". */
export function readersParts(readers = [], { max = 4 } = {}) {
  const parts = []
  listInto(parts, readers.map((r) => [r.group ? groupRef(r.queue, r.group) : queueRef(r.queue)]), max)
  return parts
}

/** The filtered groups of a queue, from its route: they go when it goes. */
export const filteredGroupsOf = (route) => (route?.readers || []).filter((r) => r.group)
