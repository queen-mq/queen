// The Workload page's models, from what the broker answers. Pure: no Vue, no
// '@/', so test/workload-page.test.js holds them against payloads shaped like
// stage's answers of 2026-10-31.
//
//   GET /api/v1/analytics/workload   the window, by namespace, task or queue;
//                                    a pick is groupBy=queue with the
//                                    namespace (or task) as a filter
//   GET /api/v1/consumer-groups      the groups' lag: the workload read counts
//                                    groups and never says how far behind
//   GET /api/v1/resources/queues     queue -> namespace and task
//
// The rules are the old page's (composables/useWorkload.js enrichRows and
// findings): the same lines and floors, said in the console's words. A
// reading nobody gave is '–', never a 0.

import { comparisonRange, fetchedOf, totalSeries, windowDeltas } from '../../composables/useWorkload.js'
import { ackFailureSeverity, backlogSeverity, THRESHOLDS } from '../../composables/useSeverity.js'
import { byLagInactiveLast, groupAttention, groupPresence, groupState } from '../../composables/useAttention.js'

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const cap = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1) : s)

// ---- words for numbers ------------------------------------------------------

/** An integer in en-US; '–' for nothing. */
export const fmt = (v) => (num(v) === null ? '–' : Math.round(num(v)).toLocaleString('en-US'))

/** "3 queues", "1 queue". */
export const plural = (n, one, many = `${one}s`) => `${fmt(n)} ${num(n) === 1 ? one : many}`

/** A share 0..1: "74%", "3.6%", "99.9%", "<0.1%"; never 100% while some is missing. */
export function pct(share) {
  const s = num(share)
  if (s === null) return '–'
  const p = s * 100
  if (p === 0) return '0%'
  if (p < 0.1) return '<0.1%'
  if (p < 10) return `${Number(p.toFixed(1))}%`
  if (p >= 99.5 && p < 100) return `${Math.floor(p * 10) / 10}%`
  return `${Math.round(p)}%`
}

/** 1024-based, as the band's cluster chip says it: "176 MB", "1.1 GB", "240 B". */
export function bytes(b) {
  const v0 = num(b)
  if (v0 === null) return '–'
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let v = v0
  let i = 0
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++ }
  return `${v < 10 && i > 0 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${u[i]}`
}

/** A count in few characters: "940", "1.2k", "12k", "3.4M". */
export function compact(n) {
  const x = num(n)
  if (x === null) return '–'
  const a = Math.abs(x)
  if (a >= 1e6) return `${Number((x / 1e6).toFixed(1))}M`
  if (a >= 1e4) return `${Math.round(x / 1e3)}k`
  if (a >= 1e3) return `${Number((x / 1e3).toFixed(1))}k`
  return String(Math.round(x))
}

/** A rate a second, as the Overview says it: "1,234", "12.5", "0.42", "0". */
export function rate(v) {
  const x = num(v)
  if (x === null) return '–'
  const a = Math.abs(x)
  if (a >= 100) return Math.round(x).toLocaleString('en-US')
  if (a >= 10) return x.toFixed(1)
  if (a === 0) return '0'
  if (a < 0.01) return '<0.01'
  return x.toFixed(2).replace(/0$/, '')
}

/** "38 s", "12 min", "2 h 5 min", "3 d 4 h": rounded once, then split. */
export function span(seconds) {
  const s = Math.max(0, Math.round(num(seconds) ?? 0))
  if (s < 60) return `${s} s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min`
  if (m < 24 * 60) {
    const h = Math.floor(m / 60)
    const mm = m % 60
    return mm ? `${h} h ${mm} min` : `${h} h`
  }
  const hours = Math.round(s / 3600)
  const d = Math.floor(hours / 24)
  const hh = hours % 24
  return hh ? `${d} d ${hh} h` : `${d} d`
}

/** A wait in milliseconds: "540 ms", "5.1 s", "42 s", "2 min". */
export function wait(ms) {
  const x = num(ms)
  if (x === null) return '–'
  if (x < 1000) return `${Math.round(x)} ms`
  if (x < 10_000) return `${(x / 1000).toFixed(1).replace(/\.0$/, '')} s`
  return span(x / 1000)
}

/** A signed change against the window compared with: "+12%", "−3.4%", '–' without a base. */
export function delta(d) {
  const p = num(d?.pct)
  if (p === null) return '–'
  if (Math.abs(p) < 0.0005) return '0%'
  return `${p > 0 ? '+' : '−'}${pct(Math.abs(p))}`
}

/** "`traceflow` is busy" → [{ t: 'traceflow', mono: true }, { t: ' is busy', mono: false }]. */
export function parts(text) {
  return String(text ?? '').split('`').map((t, i) => ({ t, mono: i % 2 === 1 })).filter((p) => p.t)
}

/** A verdict word to the console's colour: bad is red, warn is amber, the rest is ink. */
export const toneOf = (sev) => (sev === 'bad' ? 'red' : sev === 'warn' ? 'amber' : '')

// ---- the scope: group by, and the one picked ----------------------------------

export const GROUP_BY = [
  { value: 'namespace', label: 'Namespace' },
  { value: 'task', label: 'Task' },
  { value: 'queue', label: 'Queue' },
]

/**
 * What the rows are. `by` is the control; `key` the namespace (or task) picked
 * in the side list, null for all of them ('' is the real group of the queues
 * without one). Grouped by queue, the side list still lists namespaces: a pick
 * narrows the queues to one of them.
 *
 * @returns {{ by, listBy, key, level, sig }}
 *   `level` is what one row is: the group, or a queue once one is picked.
 */
export function scopeOf(by, key) {
  const b = GROUP_BY.some((g) => g.value === by) ? by : 'namespace'
  const listBy = b === 'task' ? 'task' : 'namespace'
  const k = key === null || key === undefined ? null : String(key)
  return { by: b, listBy, key: k, level: k === null ? b : 'queue', sig: `${b}|${k === null ? '\u0000' : k}` }
}

/** The empty key's name: the real group of the queues without one. */
export const emptyName = (listBy) => (listBy === 'task' ? 'No task' : 'No namespace')
/** The list's first row, and the page's word for no pick. */
export const rootName = (by) => (by === 'task' ? 'All tasks' : by === 'queue' ? 'All queues' : 'All namespaces')
/** What the scope is called: "All namespaces", "channel", "No namespace". */
export const scopeName = (scope) => (scope.key === null ? rootName(scope.by) : (scope.key || emptyName(scope.listBy)))
/** A row's name on screen, and whether it is an id (mono). */
export function rowName(row, listBy) {
  if (row.level === 'queue') return { text: String(row.key), mono: true }
  return row.key ? { text: String(row.key), mono: true } : { text: emptyName(listBy), mono: false }
}
/** A row's name inside a sentence; the empty group is said in words. */
function said(row, listBy, start = false) {
  if (row.level === 'queue' || row.key) return `\`${row.key}\``
  const t = listBy === 'task' ? 'the queues without a task' : 'the queues without a namespace'
  return start ? cap(t) : t
}

/** The read's parameters for a scope over a window ({ from, to } ISO). */
export function readParams(scope, iso) {
  const p = { from: iso.from, to: iso.to, groupBy: scope.level }
  if (scope.key !== null) p[scope.listBy] = scope.key
  return p
}

/** The read as the page says it: "GET /api/v1/analytics/workload?groupBy=queue&namespace=channel". */
export function readCall(scope) {
  const q = [`groupBy=${scope.level}`]
  if (scope.key !== null) q.push(`${scope.listBy}=${encodeURIComponent(scope.key)}`)
  return `GET /api/v1/analytics/workload?${q.join('&')}`
}

// ---- the window, and the one it is compared with --------------------------------

const SPAN_WORDS = { 15: '15 minutes', 60: 'hour', 360: '6 hours', 1440: 'day', 10080: 'week' }

/** The window in words: { label: 'Last hour', inThe: 'in the last hour' }. */
export function windowWords(win) {
  if (!win) return { label: '', inThe: '' }
  if (win.custom) return { label: win.label, inThe: `from ${String(win.label).replace(' – ', ' to ')}` }
  const long = String(win.long || '').replace(/, live$/, '')
  return { label: cap(long), inThe: `in the ${long}` }
}

export const COMPARE = ['previous', 'yesterday', 'lastWeek', 'off']

/** The window compared with, in words: "the hour before", "the same time yesterday"; '' when off. */
export function compareWords(mode, win) {
  if (mode === 'off') return ''
  if (mode === 'yesterday') return 'the same time yesterday'
  if (mode === 'lastWeek') return 'the same time last week'
  if (win && !win.custom && SPAN_WORDS[win.minutes]) return `the ${SPAN_WORDS[win.minutes]} before`
  return `the ${span((win?.minutes || 60) * 60)} before`
}

/**
 * The window compared with, in ms and ISO, and how far back it is (`shift`):
 * a point of it, moved forward by `shift`, lies over the same moment of this
 * window. null when off.
 */
export function compareWindow(win, mode) {
  if (!win || !COMPARE.includes(mode) || mode === 'off') return null
  const r = comparisonRange({ from: win.from, to: win.to }, mode)
  const from = r.from.getTime()
  return { from, to: r.to.getTime(), shift: win.from - from, iso: { from: r.from.toISOString(), to: r.to.toISOString() } }
}

/**
 * Drop the first bucket when it began before the window did. The window
 * starts mid-minute, so its first bucket is cut: at one-minute buckets the
 * broker has no row for it at all, at five it holds part of one. Drawn, it is
 * a ramp up from 0 on the left of every line, the twin of the open bucket on
 * the right (useWorkload trimOpenBucket). The window's totals are the
 * broker's own and keep it.
 */
export function trimHeadBucket(payload) {
  const b = payload?.buckets
  const from = Date.parse(payload?.timeRange?.from)
  if (!Array.isArray(b) || b.length < 2 || !Number.isFinite(from)) return payload
  const first = Date.parse(b[0])
  if (!Number.isFinite(first) || first >= from) return payload
  const cut = (arr) => (Array.isArray(arr) ? arr.slice(1) : arr)
  const cutSeries = (series) => Object.fromEntries(Object.entries(series || {}).map(([k, v]) => [k, cut(v)]))
  return {
    ...payload,
    buckets: cut(b),
    rows: (payload.rows || []).map((r) => ({ ...r, series: cutSeries(r.series) })),
    tenant: payload.tenant?.series ? { ...payload.tenant, series: cutSeries(payload.tenant.series) } : payload.tenant,
  }
}

// ---- the order: rows never re-sort by themselves ---------------------------------

const keyOrder = (a, b) => (a.key === '' ? 1 : b.key === '' ? -1 : String(a.key).localeCompare(String(b.key)))

/** Busiest first: reads, then pushes, then the name (the empty group last). */
export function rankRows(rows, value = (r) => r.reads) {
  return [...(rows || [])].sort((a, b) => ((num(value(b)) || 0) - (num(value(a)) || 0))
    || ((b.window?.pushMessages || 0) - (a.window?.pushMessages || 0))
    || keyOrder(a, b))
}

/**
 * The rows' order, kept while the scope (`sig`) stays: a refresh moves no row,
 * a row that appears joins at the end, one that goes leaves its place. A new
 * scope ranks again.
 *
 * @returns {{ sig: string, keys: string[] }}
 */
export function stableKeys(prev, rows, sig, value) {
  const list = rows || []
  if (!prev || prev.sig !== sig) return { sig, keys: rankRows(list, value).map((r) => r.key) }
  const have = new Set(list.map((r) => r.key))
  const kept = prev.keys.filter((k) => have.has(k))
  const seen = new Set(kept)
  const added = rankRows(list.filter((r) => !seen.has(r.key)), value).map((r) => r.key)
  return { sig, keys: kept.concat(added) }
}

/** The rows in the order of `keys`; a row the order does not know goes last. */
export function inOrder(rows, keys) {
  const at = new Map((keys || []).map((k, i) => [k, i]))
  return [...(rows || [])].sort((a, b) => (at.get(a.key) ?? 1e9) - (at.get(b.key) ?? 1e9))
}

// ---- the counts: the tenant now, and over the window --------------------------------

/** Kafka clients read by fetch: a read, not a delivery. Said wherever the number is. */
export const FETCHED_TITLE = 'Records Kafka clients fetched through the broker\'s Kafka facade. A fetch is a read, not a delivery: '
  + 'it takes no lease and nothing acks it, a client that seeks back reads again, and two Kafka consumer groups '
  + 'reading one topic count every record twice.'

/**
 * The counts strip: what the tenant holds now, and what the window added up
 * to, each with its change against the window compared with. Pending is
 * judged as seconds of work at the window's ack rate, and ack failures as a
 * share of the acks (useSeverity): a raw count is not a verdict.
 *
 * @param {object|null} tenant tenantTotals(payload)
 * @param {object|null} prevWindow the compared window's tenant.window
 * @param {{ seconds?: number, fetched?: boolean }} [o] the window's length; whether to show Kafka fetches
 */
export function countsModel(tenant, prevWindow, { seconds = null, fetched = false } = {}) {
  if (!tenant) return null
  const n = tenant.now || {}
  const w = tenant.window || {}
  const d = prevWindow ? windowDeltas(w, prevWindow) : null
  const drain = seconds ? (num(w.ackSuccess) || 0) / seconds : null
  const pendingTone = toneOf(backlogSeverity({ pending: n.pending, drainPerSec: drain }))
  const ackTone = toneOf(ackFailureSeverity({ failed: w.ackFailed, succeeded: w.ackSuccess }))
  const polls = (num(w.popMessages) || 0) + (num(w.popEmpty) || 0)
  const fill = polls >= 5 ? (num(w.popMessages) || 0) / polls : null
  const at = (k) => (d ? delta(d[k]) : '')
  const showFetch = fetched || fetchedOf(w) > 0 || fetchedOf(prevWindow) > 0
  return {
    now: [
      { k: 'queues', label: 'Queues', v: fmt(tenant.queues) },
      { k: 'groups', label: 'Consumer groups', v: fmt(n.groups) },
      { k: 'partitions', label: 'Partitions', v: fmt(n.partitions) },
      {
        k: 'pending', label: 'Pending', v: fmt(n.pending), tone: pendingTone,
        title: pendingTone ? 'At the window\'s ack rate this backlog is more work than the lines allow' : '',
      },
      { k: 'processing', label: 'In flight', v: fmt(n.processing), title: 'Leased to a worker and not acked yet' },
      { k: 'dlq', label: 'In DLQ', v: fmt(n.deadLetter), title: 'Dead letters held now; nothing purges them' },
      { k: 'retained', label: 'Retained', v: bytes(n.retainedBytes), title: 'On disk, for every queue' },
    ],
    window: [
      { k: 'push', label: 'Pushed', v: fmt(w.pushMessages), delta: at('pushMessages') },
      { k: 'pop', label: 'Delivered', v: fmt(w.popMessages), delta: at('popMessages') },
      showFetch ? { k: 'fetch', label: 'Fetched by Kafka', v: fmt(fetchedOf(w)), delta: at('fetchMessages'), title: FETCHED_TITLE } : null,
      { k: 'ack', label: 'Acked', v: fmt(w.ackSuccess), delta: at('ackSuccess') },
      {
        k: 'ackf', label: 'Ack failures', v: fmt(w.ackFailed), delta: at('ackFailed'), tone: ackTone,
        title: ackTone ? `${pct((num(w.ackFailed) || 0) / Math.max(1, (num(w.ackFailed) || 0) + (num(w.ackSuccess) || 0)))} of the acks failed` : '',
      },
      { k: 'empty', label: 'Empty polls', v: fmt(w.popEmpty), delta: at('popEmpty'), title: 'Polls that found nothing to deliver' },
      { k: 'fill', label: 'Fill', v: pct(fill), title: 'Of the polls, the share that brought a message' },
    ].filter(Boolean),
  }
}

// ---- work over time -------------------------------------------------------------------

/** The measures the charts can draw, and the window counter each sums to. */
export const MEASURES = [
  { id: 'pop', label: 'Delivered', unit: 'deliveries a second', total: 'popMessages', rate: true },
  { id: 'push', label: 'Pushed', unit: 'pushes a second', total: 'pushMessages', rate: true },
  { id: 'fetch', label: 'Fetched by Kafka', unit: 'records fetched by Kafka clients a second', total: 'fetchMessages', rate: true, kafka: true },
  { id: 'popEmpty', label: 'Empty polls', unit: 'empty polls a second', total: 'popEmpty', rate: true },
  { id: 'ackFailed', label: 'Ack failures', unit: 'ack failures a second', total: 'ackFailed', rate: true },
  { id: 'parked', label: 'Parked polls', unit: 'long polls parked, on average', total: 'parkedAvg', rate: false },
]
export const measureOf = (id) => MEASURES.find((m) => m.id === id) || MEASURES[0]
/** What one row did of a measure over the window. */
export const measureTotal = (row, id) => num(row?.window?.[measureOf(id).total]) || 0

/**
 * The charts of one measure: the scope's total (with the window compared
 * with, dashed, moved over this one) and one small chart for each of the
 * busiest rows, the rest summed in one "others". A rate is a second; a
 * bucket with no metrics row is drawn as 0, since a queue writes one only for
 * a minute something happened. A compared window that recorded nothing at
 * all is not drawn as a flat line: `prevNote` says so instead.
 *
 * @param {Array<object>} rows enriched rows of this window
 * @param {string[]} keys their order for this measure (stableKeys by measureTotal)
 */
export function workCharts(rows, keys, {
  buckets = [], bucketMinutes = 1, measure = 'pop', prev = null, scopeLabel = '', compareLabel = '', limit = 5, listBy = 'namespace',
} = {}) {
  const m = measureOf(measure)
  const width = Math.max(1, num(bucketMinutes) || 1) * 60_000
  const div = m.rate ? width / 1000 : 1
  const timesOf = (bs, shift = 0) => (bs || []).map((b) => Date.parse(b) + width / 2 + shift)
  const at = timesOf(buckets)
  const line = (vals, times = at) => times.map((t, i) => [t, (vals[i] ?? 0) / div])
  const peakOf = (points) => points.reduce((mx, p) => Math.max(mx, p[1] || 0), 0)
  const nothing = (vals) => !vals.some((v) => v !== null && v !== undefined)

  const total = { id: 'total', label: scopeLabel, ink: 'in', area: true, points: line(totalSeries(rows || [], buckets, m.id)) }
  const series = [total]
  let prevNote = ''
  if (prev) {
    const pv = totalSeries(prev.rows || [], prev.buckets || [], m.id)
    if (nothing(pv)) prevNote = `Nothing was recorded ${compareLabel}, so it is not drawn.`
    else series.push({ id: 'prev', label: cap(compareLabel), ink: 'out', dashed: true, points: line(pv, timesOf(prev.buckets, prev.shift || 0)) })
  }

  const busy = inOrder(rows, keys).filter((r) => measureTotal(r, m.id) > 0)
  const top = busy.slice(0, limit)
  const rest = busy.slice(limit)
  const one = (r) => {
    const points = line(r.series?.[m.id] || [])
    const name = rowName(r, listBy)
    return { key: r.key, label: name.text, mono: name.mono, row: r, points, peak: peakOf(points) }
  }
  const small = top.map(one)
  if (rest.length === 1) small.push(one(rest[0]))
  else if (rest.length > 1) {
    const points = line(totalSeries(rest, buckets, m.id))
    small.push({ key: '\u0000others', label: `${rest.length} others`, mono: false, others: true, names: rest.map((r) => rowName(r, listBy).text), points, peak: peakOf(points) })
  }
  return { measure: m, series, peak: peakOf(total.points), small, quiet: (rows || []).length - busy.length, prevNote }
}

// ---- what needs you -----------------------------------------------------------------------

const lagLines = (linesFor, q) => (typeof linesFor === 'function' && linesFor(q)) || THRESHOLDS
const lagVerdict = (s, t) => (s >= t.lagBadSeconds ? 'bad' : s >= t.lagWarnSeconds ? 'warn' : null)
const groupLabel = (name) => (name === '__QUEUE_MODE__' ? 'queue mode' : name)
const TONE_RANK = { red: 3, amber: 2, '': 1 }

/**
 * A row's acks, by the console's one rule (useSeverity ackFailureSeverity, as
 * the Overview, the Queues list and this page's counts judge them): a share of
 * the acks failing, past a floor of failures. A queue is judged by its own
 * lines; a namespace or a task by the tenant's.
 */
export function ackToneOf(row, linesFor) {
  const w = row?.window || {}
  const lines = row?.level === 'queue' && typeof linesFor === 'function' ? linesFor(row.key) : undefined
  return toneOf(ackFailureSeverity({ failed: w.ackFailed, succeeded: w.ackSuccess }, lines))
}

/** "`a`, `b`, `c` and 3 more" — names said once, the rest counted. */
function some(names, max = 4) {
  const shown = names.slice(0, max)
  const more = names.length - shown.length
  if (more > 0) return `${shown.join(', ')} and ${more} more`
  if (shown.length <= 1) return shown.join('')
  return `${shown.slice(0, -1).join(', ')} and ${shown[shown.length - 1]}`
}

/**
 * What needs you in the rows, and what is worth a look. The old page's rules
 * (useWorkload findings), with the console's colour policy:
 *   no reader     messages no consumer group reads (amber, as useAttention says it)
 *   behind        a group past its queue's lag lines (amber, red)
 *   failing       acks failing as a share, past a floor (ackToneOf: amber, red)
 *   slow pops     a message waited a minute or more for its pop (amber)
 * and, in grey, since none is news: a dead-letter queue past 1,000 (its depth
 * is history, nothing purges it), polls that mostly find nothing, and groups
 * where nothing happened at all. A note that fits several rows is one line.
 * Without the consumer groups the rules that need them stay off (`hasGroups`):
 * a queue with no group looks just like one whose groups did not load.
 *
 * @returns {{ needs: Array<object>, notes: Array<object> }} needs worst first;
 *   each { key, tone, badge, text, evidence, row, kind, dlq? }
 */
export function findingsOf(rows, keys, { listBy = 'namespace', hasGroups = true, linesFor, inThe = 'in the window' } = {}) {
  const needs = []
  const notes = []
  const empty = []
  const idle = []
  for (const r of inOrder(rows, keys)) {
    const w = r.window || {}
    const n = r.now || {}
    const base = { key: r.key, row: r }
    const who = said(r, listBy, true)
    const queue = r.level === 'queue'
    const many = !queue && !r.key
    if (hasGroups && n.pendingWithoutGroup > 0 && !r.noGroupExpected) {
      needs.push({
        ...base, kind: 'noReader', tone: 'amber', badge: 'No reader',
        text: queue
          ? `${who} holds ${fmt(n.pendingWithoutGroup)} messages, and no consumer group reads it.`
          : `${who} ${many ? 'hold' : 'holds'} ${fmt(n.pendingWithoutGroup)} messages on queues no consumer group reads.`,
        evidence: queue ? '' : `${fmt(n.queuesWithoutGroup)} of ${plural(r.queues, 'queue')} have no group; pick it to see which.`,
      })
    }
    if (hasGroups) {
      for (const { group: g, queue: q } of r.laggingGroups || []) {
        const v = lagVerdict(num(g.maxTimeLag) || 0, lagLines(linesFor, q))
        if (!v) continue
        const cursors = num(g.partitionCursors ?? g.members)
        needs.push({
          ...base, kind: 'behind', tone: toneOf(v), badge: 'Behind', queue: q,
          text: `\`${groupLabel(g.name)}\` on \`${q}\` is ${span(g.maxTimeLag)} behind.`,
          evidence: `${fmt(g.partitionsWithLag)} of ${plural(cursors, 'partition')} behind, ${plural(g.totalLag || 0, 'message')} waiting.`,
        })
      }
    }
    const ackTone = ackToneOf(r, linesFor)
    if (ackTone) {
      needs.push({
        ...base, kind: 'acks', tone: ackTone, badge: 'Failing', dlq: queue ? r.key : null,
        text: `${who}: ${fmt(w.ackFailed)} of ${fmt(r.acks)} acks failed ${inThe}.`,
        evidence: !w.ackSuccess ? 'Not one ack succeeded.' : `${pct(r.ackOk)} of the acks succeeded.`,
      })
    }
    if (num(w.maxLagMs) !== null && w.maxLagMs >= 60_000) {
      needs.push({
        ...base, kind: 'slow', tone: 'amber', badge: 'Slow pops',
        text: `A message on ${said(r, listBy)} waited ${wait(w.maxLagMs)} for a pop.`,
        evidence: `On average a message waited ${wait(w.avgLagMs)}.`,
      })
    }
    if (n.deadLetter >= 1000) {
      notes.push({
        ...base, kind: 'dlq', tone: '', dlq: queue ? r.key : null,
        text: `${who} ${many ? 'hold' : 'holds'} ${fmt(n.deadLetter)} dead letters.`,
        evidence: queue ? 'Their causes are on the Dead letters page.' : `Across ${plural(r.queues, 'queue')}.`,
      })
    }
    if (w.popEmpty >= 1000 && r.fill !== null && r.fill < 0.05) empty.push(r)
    if (hasGroups && n.queuesTouched === 0 && r.groupsN === 0) idle.push(r)
  }
  if (empty.length) {
    const sum = (k) => empty.reduce((a, r) => a + (num(r.window?.[k]) || 0), 0)
    const parked = sum('parkedAvg')
    const one = empty.length === 1
    notes.push({
      key: one ? empty[0].key : null, row: one ? empty[0] : null, kind: 'empty', tone: '',
      text: one
        ? `${said(empty[0], listBy, true)} answered ${fmt(sum('popEmpty'))} polls with nothing, and ${fmt(sum('popMessages'))} with a message.`
        : `${cap(some(empty.map((r) => said(r, listBy))))} answered almost every poll with nothing.`,
      evidence: `${one ? '' : `${fmt(sum('popEmpty'))} empty polls for ${plural(sum('popMessages'), 'message')}; `}`
        + (parked >= 0.5 ? `${fmt(parked)} long polls wait on ${one ? 'it' : 'them'} on average.` : `no long poll waits on ${one ? 'it' : 'them'}.`),
    })
  }
  if (idle.length) {
    const one = idle.length === 1
    const queues = idle.reduce((a, r) => a + (r.level === 'queue' ? 1 : (num(r.queues) || 0)), 0)
    notes.push({
      key: one ? idle[0].key : null, row: one ? idle[0] : null, kind: 'idle', tone: '',
      text: `${cap(some(idle.map((r) => said(r, listBy))))}: nothing pushed or read ${inThe}, and no consumer group.`,
      evidence: idle[0].level === 'queue' ? 'Candidates for cleanup.' : `${plural(queues, 'queue')} in all; candidates for cleanup.`,
    })
  }
  const worst = needs.map((f, i) => ({ f, i }))
    .sort((a, b) => (TONE_RANK[b.f.tone] - TONE_RANK[a.f.tone]) || (a.i - b.i))
    .map((x) => x.f)
  return { needs: worst, notes }
}

/** Each row's worst mark: key → 'red' | 'amber'. */
export function marksOf(needs) {
  const out = new Map()
  for (const f of needs || []) {
    const had = out.get(f.key) || ''
    if (TONE_RANK[f.tone] > TONE_RANK[had]) out.set(f.key, f.tone)
  }
  return out
}

/**
 * The line under the title: is anything stuck, and who does the work.
 *
 * @returns {{ tone, badge, text, sub, list, loading? }}
 */
export function verdictOf({ loaded = false, error = '', needs = [], rows = [], keys = [], scope, inThe = 'in the window' } = {}) {
  if (!loaded) {
    if (error) return { tone: 'red', badge: 'No answer', text: `The broker did not answer: ${error}.`, sub: '', list: [] }
    return { tone: '', badge: '', text: 'Reading the workload…', sub: '', list: [], loading: true }
  }
  const listBy = scope?.listBy || 'namespace'
  const where = scope && scope.key !== null ? ` in ${scope.key ? `\`${scope.key}\`` : `the queues without a ${listBy}`}` : ''
  if (needs.length === 1) {
    const f = needs[0]
    return { tone: f.tone, badge: f.badge, text: f.text, sub: f.evidence, list: [], finding: f }
  }
  if (needs.length > 1) {
    const tone = needs[0].tone
    return { tone, badge: tone === 'red' ? 'Failing' : 'Needs you', text: `${needs.length} things need you${where}.`, sub: '', list: needs }
  }
  const busy = inOrder(rows, keys).filter((r) => r.reads > 0 || (r.window?.pushMessages || 0) > 0)
  if (!busy.length) return { tone: '', badge: 'Quiet', text: `Nothing was pushed or read${where} ${inThe}.`, sub: '', list: [] }
  const top = rankRows(busy).slice(0, 2).map((r) => said(r, listBy))
  const who = top.length === 1 ? `The busiest is ${top[0]}.` : `The busiest are ${top[0]} and ${top[1]}.`
  return { tone: '', badge: 'Flowing', text: `Nothing is stuck${where}. ${who}`, sub: '', list: [] }
}

// ---- the table: one row per group or queue, three sets of columns --------------------

export const TABS = [
  { id: 'work', label: 'Share of the work' },
  { id: 'backlog', label: 'Backlog and efficiency' },
  { id: 'footprint', label: 'Footprint' },
]

/**
 * Every cell the table can show, for every row, in the order of `keys`. The
 * tabs pick columns; the rows and their order stay. Colour only where a rule
 * says so, the same as findingsOf: no reader, acks failing, a group behind by
 * its queue's lines, a minute's wait for a pop.
 *
 * @param {object} o
 * @param {boolean} o.hasGroups the consumer groups loaded: without them, no one knows how far behind a row is
 * @param {Map<string, object>|null} o.prev the compared window's rows by key
 * @param {object|null} o.tenantNow the tenant's `now`, for the footprint bars
 * @param {Map<string, string>} o.marks marksOf(needs)
 */
export function tableRows(rows, keys, { listBy = 'namespace', hasGroups = true, linesFor, prev = null, prevTenantReads = 0, tenantNow = null, marks = new Map() } = {}) {
  const parts = Math.max(1, num(tenantNow?.partitions) || 0)
  const held = Math.max(1, num(tenantNow?.retainedBytes) || 0)
  return inOrder(rows, keys).map((r) => {
    const w = r.window || {}
    const n = r.now || {}
    const queue = r.level === 'queue'
    const name = rowName(r, listBy)
    const p = prev?.get(r.key) || null
    const noReader = hasGroups && n.pendingWithoutGroup > 0 && !r.noGroupExpected
    const ackTone = ackToneOf(r, linesFor)
    let oldest
    if (!hasGroups) oldest = { text: '–', dim: true, title: 'The consumer groups did not load' }
    else if (r.oldest !== null) oldest = { text: r.oldest > 0 ? span(r.oldest) : '0 s', dim: !(r.oldest > 0), tone: toneOf(r.lagSev), title: `${r.groupsLagging} of ${plural(r.groupsN, 'group')} behind${r.groupsInactive ? `; ${r.groupsInactive} not running` : ''}` }
    else if (r.groupsN) oldest = { text: 'not running', dim: true, title: 'Every group of it is a Kafka group nobody runs: their lag is nobody\'s alarm' }
    else oldest = { text: 'no group', dim: true, title: 'No consumer group reads it' }
    const created = num(w.partitionsCreated)
    const deleted = num(w.partitionsDeleted)
    return {
      key: r.key,
      name: name.text,
      mono: name.mono,
      queue,
      to: queue ? `/queues/${encodeURIComponent(r.key)}` : null,
      mark: marks.get(r.key) || '',
      // share of the work, over the window
      share: {
        value: r.share || 0,
        text: pct(r.share || 0),
        prev: p && prevTenantReads > 0 ? p.reads / prevTenantReads : null,
      },
      delivered: { text: fmt(w.popMessages), dim: !w.popMessages },
      pushed: { text: fmt(w.pushMessages), dim: !w.pushMessages },
      fetched: r.fetched > 0 ? fmt(r.fetched) : (r.fetched === 0 ? '0' : '–'),
      // backlog and efficiency
      pending: {
        text: fmt(n.pending),
        dim: !n.pending,
        tone: noReader && (queue || n.pendingWithoutGroup === n.pending) ? 'amber' : '',
        sub: noReader && !queue && n.pendingWithoutGroup !== n.pending
          ? `${fmt(n.pendingWithoutGroup)} with no reader`
          : (n.processing ? `${fmt(n.processing)} in flight` : ''),
        subTone: noReader && !queue && n.pendingWithoutGroup !== n.pending ? 'amber' : '',
        title: noReader ? 'No consumer group reads some of these messages' : '',
      },
      oldest,
      dlq: { text: fmt(n.deadLetter), link: queue && n.deadLetter > 0 ? { path: '/dlq', query: { queue: r.key } } : null, dim: !n.deadLetter },
      fill: r.kafkaOnly
        ? { text: 'Kafka', sub: `${compact(r.fetched)} fetched`, dim: true, title: 'Read by Kafka fetches only: a fetch is no poll, so there is no fill' }
        : r.fill === null
          ? { text: '–', dim: true, title: 'Under 5 polls: too few to say' }
          : { text: pct(r.fill), sub: w.popEmpty ? `${compact(w.popEmpty)} empty` : '', title: `${fmt(w.popMessages)} deliveries, ${fmt(w.popEmpty)} empty polls` },
      ack: r.ackOk === null
        ? { text: '–', dim: true, title: r.acks ? `${plural(r.acks, 'ack')}: under 5, too few to judge` : 'No acks' }
        : { text: pct(r.ackOk), tone: ackTone, sub: w.ackFailed ? `${fmt(w.ackFailed)} failed` : '', title: `${fmt(w.ackSuccess)} of ${fmt(r.acks)} acks succeeded` },
      lag: num(w.maxLagMs) === null
        ? { text: '–', dim: true, title: 'Nothing popped' }
        : { text: `${wait(w.avgLagMs)} → ${wait(w.maxLagMs)}`, tone: w.maxLagMs >= 60_000 ? 'amber' : '', title: 'How long a message waited for its pop: on average, then at worst' },
      // footprint, now (churn over the window)
      queues: queue ? null : fmt(r.queues),
      groups: { text: fmt(n.groups), dim: !n.groups, sub: !queue && n.queuesWithoutGroup ? `${n.queuesWithoutGroup === 1 ? '1 queue has' : `${fmt(n.queuesWithoutGroup)} queues have`} none` : '' },
      partitions: {
        text: fmt(n.partitions),
        bar: Math.min(1, (num(n.partitions) || 0) / parts),
        sub: !queue && r.queues > 1 && n.partitions ? `${compact((num(n.partitions) || 0) / r.queues)} a queue` : '',
      },
      retained: { text: bytes(n.retainedBytes), bar: Math.min(1, (num(n.retainedBytes) || 0) / held) },
      churn: created === null && deleted === null
        ? { text: 'not reported', dim: true }
        : (created || deleted)
          ? { text: `+${fmt(created || 0)} / −${fmt(deleted || 0)}` }
          : { text: '0', dim: true },
    }
  })
}

// ---- the activity map: every row, every bucket, in grey ---------------------------------

/**
 * Deliveries per bucket for each row that delivered something, stronger for
 * more (the square root of the share of the strongest cell, or every row
 * but the busiest is blank). A bucket with no metrics row is outlined, not
 * drawn pale: unmeasured is not quiet. The rows that delivered nothing are
 * named once instead of drawn empty.
 */
export function mapModel(rows, keys, { buckets = [], bucketMinutes = 1, listBy = 'namespace' } = {}) {
  const ordered = inOrder(rows, keys)
  let max = 0
  for (const r of ordered) for (const v of r.series?.pop || []) if (num(v) !== null && v > max) max = v
  const width = Math.max(1, num(bucketMinutes) || 1) * 60_000
  const times = (buckets || []).map((b) => Date.parse(b))
  const drawn = ordered.filter((r) => (r.window?.popMessages || 0) > 0)
  const idle = ordered.filter((r) => !((r.window?.popMessages || 0) > 0))
  return {
    max,
    width,
    times,
    rows: drawn.map((r) => {
      const name = rowName(r, listBy)
      return {
        key: r.key,
        name: name.text,
        mono: name.mono,
        cells: (r.series?.pop || []).map((v) => (num(v) === null
          ? { na: true, v: null, p: 0 }
          : { na: false, v, p: max ? Math.round(Math.sqrt(v / max) * 100) : 0 })),
      }
    }),
    idle: idle.map((r) => rowName(r, listBy).text),
  }
}

// ---- the consumer groups of one queue -------------------------------------------------------

const STATE_WORDS = { Stable: 'keeping up', Lagging: 'behind', Empty: 'no members', Dead: 'stopped', Rebalancing: 'rebalancing' }
function readsFrom(g) {
  const m = g.subscriptionMode
  if (m === 'all') return 'from the start'
  if (m === 'new') return 'new messages only'
  if (m && g.subscriptionTimestamp) return `from ${new Date(g.subscriptionTimestamp).toISOString().slice(0, 16).replace('T', ' ')} UTC`
  return g.name === '__QUEUE_MODE__' ? 'every message, once' : ''
}

/**
 * One queue's consumer groups, worst lag first and the Kafka groups nobody
 * runs last (useAttention byLagInactiveLast), in the order of `keys` once
 * the queue is open, so a refresh moves none of them.
 *
 * @returns {{ summary: string, rows: Array<object>, keys: string[] }}
 */
export function groupsModel(groups, linesFor, keys = null) {
  const ranked = byLagInactiveLast(groups || [], linesFor)
  const order = keys && keys.length ? keys : ranked.map((g) => g.name)
  const at = new Map(order.map((k, i) => [k, i]))
  const list = [...ranked].sort((a, b) => (at.get(a.name) ?? 1e9) - (at.get(b.name) ?? 1e9))
  let behind = 0
  let stopped = 0
  let idle = 0
  const rows = list.map((g) => {
    const st = groupState(g, linesFor)
    const sev = groupAttention(g, linesFor)
    if (st === 'inactive') idle++
    else if (g.state === 'Dead') stopped++
    else if (g.state === 'Lagging' || sev === 'bad' || sev === 'warn') behind++
    const cursors = num(g.partitionCursors ?? g.members)
    return {
      key: g.name,
      name: groupLabel(g.name),
      mono: g.name !== '__QUEUE_MODE__',
      kafka: g.kind === 'kafka',
      mark: sev === 'mute' ? '' : toneOf(sev),
      state: st === 'inactive' ? 'not running' : st === 'empty' ? 'no live member' : (STATE_WORDS[g.state] || String(g.state || '–').toLowerCase()),
      presence: groupPresence(g, linesFor),
      oldest: num(g.maxTimeLag) ? span(g.maxTimeLag) : '0 s',
      waiting: fmt(g.totalLag || 0),
      behind: g.partitionsWithLag ? `${fmt(g.partitionsWithLag)} of ${fmt(cursors)}` : 'none',
      readers: num(g.liveMembers) === null ? '' : plural(g.liveMembers, 'reader'),
      from: readsFrom(g),
      conflation: !!g.conflation,
    }
  })
  const bits = []
  if (behind) bits.push(`${behind} behind`)
  if (stopped) bits.push(`${stopped} stopped`)
  if (idle) bits.push(`${idle} not running`)
  const n = rows.length
  const summary = !n ? '' : bits.length ? `${plural(n, 'group')}: ${bits.join(', ')}.` : n === 1 ? 'One group, keeping up.' : `${plural(n, 'group')}, all keeping up.`
  return { summary, rows, keys: order }
}
