// The Ephemeral page's sub sidebar and head — PURE functions, no Vue, no DOM,
// so `npm test` can assert them without a browser.
//
// THE LIST IS EVERY EPHEMERAL QUEUE, BY NAME, WITH ITS DEPTH: the rows of
// `GET /api/v1/ephemeral/queues` (the answering node's own map walk:
// name, tier, depth, bytes, partitions, groups, drops, options), normalized by
// stores/ephemeralStore.js. A fixed list: a click shows that queue on the
// right and moves the highlight, and the order never follows the depth, so a
// 2 s poll cannot move rows under the pointer.
//
// NO MARKS. The payload carries no verdict, and its drops are the queue's
// policy at work (bounds, ttl, retry exhaustion): a count of them says nothing
// without knowing what the queue was asked to hold, so a non-zero count is
// shown in ink, never in amber.
//
// The search matches the way the page's table does (matchesSearch is shared by
// both), so the column and the table always show the same queues.

/** Case-insensitive, anywhere in the name; an empty search matches everything. */
export function matchesSearch(name, search = '') {
  const term = typeof search === 'string' ? search.trim().toLowerCase() : ''
  if (!term) return true
  return typeof name === 'string' && name.toLowerCase().includes(term)
}

const byName = (a, b) => a.name.localeCompare(b.name)

/** Rows (the store's normalized queues) → the list: by name, narrowed by the search, junk left out. */
export function ephemeralRows(rows, { search = '' } = {}) {
  return (Array.isArray(rows) ? rows : [])
    .filter((q) => q && typeof q.name === 'string' && q.name !== '' && matchesSearch(q.name, search))
    .sort(byName)
}

/**
 * Every message a queue let go without it being consumed — bounds, ttl and
 * retry exhaustion together — or null when the broker reported none of the
 * three ('—', never a zero). A counter it did not report adds nothing.
 */
export function dropTotal(drops) {
  const parts = [drops?.bounds, drops?.ttl, drops?.retry].filter((v) => Number.isFinite(v))
  return parts.length ? parts.reduce((s, v) => s + v, 0) : null
}

/** `bounds 2 · ttl 0 · retry 1`, '—' for a counter the broker did not report. */
export function describeDrops(drops) {
  const n = (v) => (Number.isFinite(v) ? v.toLocaleString('en-US') : '—')
  return `bounds ${n(drops?.bounds)} · ttl ${n(drops?.ttl)} · retry ${n(drops?.retry)}`
}

/** Explicit true / explicit false only: a payload that carries neither is not evidence for either. */
export function tierWord(declared) {
  if (declared === true) return 'Declared'
  if (declared === false) return 'Implicit'
  return '—'
}

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

/**
 * One queue's partitions from its depth route
 * (`partitions: [{partition, pending, bytes}]`, `pending` being that
 * partition's share of the ring), by name; null when the payload carries no
 * list. A figure the broker did not report stays null.
 */
export function partitionRows(payload) {
  const raw = payload && typeof payload === 'object' ? payload.partitions : null
  if (!Array.isArray(raw)) return null
  return raw
    .filter((p) => p && typeof p === 'object')
    .map((p) => ({ name: String(p.partition ?? p.name ?? ''), depth: num(p.pending ?? p.depth), bytes: num(p.bytes) }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

/**
 * The depth route's refusal for a queue that is not there (deleted, collected
 * once empty and idle, or held by another node): the queue is gone, the
 * family is not — never a reason to call the whole class unavailable.
 */
export function isQueueGone(err) {
  return Boolean(err) && err.status === 404 && err.code === 'ephemeral_queue_not_found'
}
