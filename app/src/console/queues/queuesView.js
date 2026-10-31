// The Queues page of the new console, as data: each queue's row, the
// namespace it sits under, the segment it falls in, the order a header click
// froze, the one line on top, and the New queue form's draft. The rule of what
// needs you is the app's one rule (useAttention.queueAttention over the last
// hour), so this page, the Overview and a queue's own page never disagree.
//
// Pure: no Vue, no '@/' imports, so test/queues-page.test.js holds it against
// stage-shaped answers. Numbers stay numbers here; the page words them with
// the console's helpers (fmt, rate, span).
import { queueAttention, groupAttention } from '../../composables/useAttention.js'
import { ackFailureSeverity, SEV_BAD, SEV_WARN } from '../../composables/useSeverity.js'
import { queueMatches } from '../../composables/navigation.js'
import {
  OPTION_DEFAULTS, OPTION_META, buildBody, existingQueueName, queueNameError, readFormValue, validate, EDITABLE_OPTIONS, configDiff, readFormValues,
} from '../../composables/useQueueConfig.js'

export const QUEUE_MODE = '__QUEUE_MODE__'
/** The workload read the shared store makes is one hour, in one-minute buckets. */
export const HOUR_S = 3600

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const RANK = { [SEV_BAD]: 2, [SEV_WARN]: 1 }
export const toneOf = (sev) => (sev === SEV_BAD ? 'red' : sev === SEV_WARN ? 'amber' : '')
const byLag = (a, b) => (num(b.maxTimeLag) || 0) - (num(a.maxTimeLag) || 0)

/** The last `n` complete minutes' mean a second (the Overview's rule): the last minute is still filling. */
export function perSecond(minutes, n = 5) {
  const done = (minutes || []).slice(0, -1).filter((v) => v != null)
  const last = done.slice(-n)
  if (!last.length) return null
  return last.reduce((a, b) => a + b, 0) / last.length / 60
}

/** A minute series for a small line: complete minutes only, leading gaps dropped, other gaps as 0. */
export function sparkValues(minutes) {
  const done = (minutes || []).slice(0, -1)
  const first = done.findIndex((v) => v != null)
  return first < 0 ? [] : done.slice(first).map((v) => (v == null ? 0 : v))
}

/**
 * The group that stands for a queue (the Overview's readerOf): its worst live
 * one by lag, else its one named group, else how many, else queue mode.
 */
export function readerOf(all = [], live = []) {
  const named = all.filter((g) => g.name !== QUEUE_MODE)
  const worst = [...live].sort(byLag)[0]
  if (worst && worst.name !== QUEUE_MODE) return { label: worst.name, mono: true }
  if (named.length === 1) return { label: named[0].name, mono: true }
  if (named.length > 1) return { label: `${named.length} groups`, mono: false }
  return { label: 'queue mode', mono: false }
}

/**
 * What a row's State cell says, or null when there is nothing to say.
 *   { kind: 'acks'|'lag'|'noReader'|'catching', word, tone, eta? }
 * A queue behind whose readers take less than arrives is "Not draining";
 * one draining faster than it fills, with nothing flagged, is "Catching up"
 * (eta: seconds to empty at that pace).
 */
export function stateOf({ issue = null, pending = 0, inS = null, outS = null } = {}) {
  if (issue?.reason === 'acks') return { kind: 'acks', word: 'Failing', tone: toneOf(issue.sev) }
  if (issue?.reason === 'lag') {
    const growing = inS != null && outS != null && outS < inS
    return { kind: 'lag', word: growing ? 'Not draining' : 'Behind', tone: toneOf(issue.sev) }
  }
  if (issue?.reason === 'noReader') return { kind: 'noReader', word: 'No reader', tone: 'amber' }
  if ((pending || 0) > 0 && inS != null && outS != null && outS > inS) {
    return { kind: 'catching', word: 'Catching up', tone: '', eta: pending / (outS - inS) }
  }
  return null
}

/**
 * Every queue as a row, from the shared stores' answers:
 *   queues   GET /api/v1/resources/queues          (queuesStore.queues)
 *   groups   GET /api/v1/consumer-groups           (groupsStore.groups; null = not read)
 *   ops      the hour's totals by queue            (queueOpsStore.ops; Map or null)
 *   series   the hour's minutes by queue           (queueOpsStore.series; Map or null)
 * `inHour` / `ackHour` are the hour's counts (arrived, acked with success);
 * the page divides them by HOUR_S. `lag` is the worst live group's age of
 * its oldest unread message, in seconds.
 */
export function queueRows({ queues = [], groups = null, ops = null, series = null, linesFor } = {}) {
  const byQueue = new Map()
  for (const g of groups || []) {
    if (!g?.queueName) continue
    if (!byQueue.has(g.queueName)) byQueue.set(g.queueName, [])
    byQueue.get(g.queueName).push(g)
  }
  const issues = groups == null ? new Map() : new Map(queueAttention(queues, groups, linesFor, ops).map((i) => [i.name, i]))
  const out = []
  for (const q of queues || []) {
    if (!q?.name) continue
    const all = byQueue.get(q.name) || []
    const live = all.filter((g) => groupAttention(g, linesFor) !== 'mute')
    const worst = [...live].sort(byLag)[0] || null
    const hour = typeof ops?.get === 'function' ? ops.get(q.name) || null : null
    const minutes = typeof series?.get === 'function' ? series.get(q.name) || null : null
    const issue = issues.get(q.name) || null
    const pending = num(q.messages?.pending)
    const inS = minutes ? perSecond(minutes.push) : null
    const outS = minutes ? perSecond(minutes.pop) : null
    const lag = worst ? num(worst.maxTimeLag) : null
    out.push({
      name: q.name,
      ns: typeof q.namespace === 'string' ? q.namespace : '',
      task: typeof q.task === 'string' ? q.task : '',
      pending,
      dead: num(q.messages?.deadLetter),
      partitions: num(q.partitions),
      named: all.filter((g) => g.name !== QUEUE_MODE).length,
      queueMode: all.some((g) => g.name === QUEUE_MODE),
      groupsKnown: groups != null,
      lag: lag && lag > 0 ? lag : null,
      inHour: hour ? num(hour.pushMessages) ?? 0 : null,
      ackHour: hour ? num(hour.ackSuccess) ?? 0 : null,
      failedHour: hour ? num(hour.ackFailed) ?? 0 : null,
      spark: minutes ? sparkValues(minutes.push) : null,
      sev: issue?.sev || null,
      issue,
      state: stateOf({ issue, pending, inS, outS }),
      reader: all.length ? readerOf(all, live) : null,
      inS,
      outS,
    })
  }
  return out
}

// ---- segments ----------------------------------------------------------------

export const SEGMENTS = [
  { value: 'all', label: 'All' },
  { value: 'needs', label: 'Needs you' },
  { value: 'waiting', label: 'Waiting' },
  { value: 'dead', label: 'Has dead letters' },
  { value: 'eph', label: 'Ephemeral' },
]
export const SEGMENT_VALUES = SEGMENTS.map((s) => s.value)

export function inSegment(row, seg) {
  if (seg === 'needs') return !!row.sev
  if (seg === 'waiting') return (row.pending || 0) > 0
  if (seg === 'dead') return (row.dead || 0) > 0
  return true
}

export function segmentCounts(rows = [], ephemeral = null) {
  return {
    all: rows.length,
    needs: rows.filter((r) => r.sev).length,
    waiting: rows.filter((r) => (r.pending || 0) > 0).length,
    dead: rows.filter((r) => (r.dead || 0) > 0).length,
    eph: ephemeral,
  }
}

// ---- order -------------------------------------------------------------------

/** The columns a header click sorts by, and how each reads its row. */
export const SORTS = {
  waiting: { label: 'Waiting', get: (r) => r.pending },
  oldest: { label: 'Oldest', get: (r) => r.lag },
  in: { label: 'In a second', get: (r) => r.inHour },
  acked: { label: 'Acked', get: (r) => r.ackHour },
  dead: { label: 'Dead letters', get: (r) => r.dead },
  partitions: { label: 'Partitions', get: (r) => r.partitions },
}

/** A header click: largest first, then smallest first, then back to names. */
export function nextSort(current, key) {
  if (!current || current.key !== key) return { key, dir: 'desc' }
  if (current.dir === 'desc') return { key, dir: 'asc' }
  return null
}

/**
 * The order of a sort as it stood when the header was clicked: name -> rank.
 * The page keeps it until the next click, so a refresh updates the numbers
 * in place and never moves a row. Unknown values go last either way.
 */
export function snapshotOrder(rows = [], { key, dir } = {}) {
  const get = SORTS[key]?.get
  if (!get) return null
  const sign = dir === 'asc' ? 1 : -1
  const list = [...rows].sort((a, b) => {
    const va = num(get(a)); const vb = num(get(b))
    if (va == null && vb == null) return a.name.localeCompare(b.name)
    if (va == null) return 1
    if (vb == null) return -1
    return sign * (va - vb) || a.name.localeCompare(b.name)
  })
  return new Map(list.map((r, i) => [r.name, i]))
}

const byRank = (rank) => (a, b) => {
  if (rank) {
    const ra = rank.has(a.name) ? rank.get(a.name) : Infinity
    const rb = rank.has(b.name) ? rank.get(b.name) : Infinity
    if (ra !== rb) return ra - rb
  }
  return a.name.localeCompare(b.name)
}

/**
 * The rows by namespace: namespaces by name, the default one ('') last; inside
 * each, the frozen order (`rank`) or names. While a filter or a segment
 * narrows them, a namespace with nothing left is not shown.
 *   [{ ns, rows, total, counts: { failing, behind, noReader, catching } }]
 */
export function namespaceGroups(rows = [], { find = '', seg = 'all', rank = null } = {}) {
  const narrowed = !!String(find || '').trim() || (seg && seg !== 'all')
  const byNs = new Map()
  for (const r of rows) {
    if (!byNs.has(r.ns)) byNs.set(r.ns, [])
    byNs.get(r.ns).push(r)
  }
  const names = [...byNs.keys()].sort((a, b) => (a === '') - (b === '') || a.localeCompare(b))
  const out = []
  for (const ns of names) {
    const mine = byNs.get(ns)
    const shown = mine
      .filter((r) => inSegment(r, seg) && queueMatches({ name: r.name, namespace: r.ns, task: r.task }, find))
      .sort(byRank(rank))
    if (narrowed && !shown.length) continue
    const counts = { failing: 0, behind: 0, noReader: 0, catching: 0 }
    for (const r of shown) {
      const k = r.state?.kind
      if (k === 'acks') counts.failing++
      else if (k === 'lag') counts.behind++
      else if (k === 'noReader') counts.noReader++
      else if (k === 'catching') counts.catching++
    }
    out.push({ ns, rows: shown, total: mine.length, counts, flagged: shown.some((r) => r.sev) })
  }
  return out
}

/** Whether a namespace opens by default: it holds a queue that needs you, or one catching up. */
export const hasNews = (group) => group.rows.some((r) => r.sev || r.state?.kind === 'catching')

// ---- the line on top -----------------------------------------------------------

/**
 * The state of every queue, for one sentence:
 *   { kind: 'loading' }                       nothing read yet
 *   { kind: 'unknown' }                       the consumer groups did not answer
 *   { kind: 'issues', tone, names, more }     queues that need you, worst first
 *   { kind: 'tenant', tone, failed, total }   acks fail across queues, no single one past its line
 *   { kind: 'fine' }                          every queue drains on its own
 * `tenant` is the Overview's own issue (useOverview tenantAcks), so both say it.
 */
export function queuesVerdict({ rows = [], loaded = false, groupsKnown = false, ops = null, lines } = {}) {
  if (!loaded) return { kind: 'loading' }
  if (!groupsKnown) return { kind: 'unknown' }
  const flagged = rows.filter((r) => r.sev)
    .sort((a, b) => (RANK[b.sev] || 0) - (RANK[a.sev] || 0) || (b.pending || 0) - (a.pending || 0) || a.name.localeCompare(b.name))
  if (flagged.length) {
    return {
      kind: 'issues',
      tone: toneOf(flagged[0].sev),
      names: flagged.slice(0, 2).map((r) => r.name),
      more: Math.max(0, flagged.length - 2),
      count: flagged.length,
      only: rows.length === flagged.length,
    }
  }
  if (ops && typeof ops.values === 'function') {
    let failed = 0; let ok = 0
    for (const o of ops.values()) { failed += num(o.ackFailed) || 0; ok += num(o.ackSuccess) || 0 }
    const sev = ackFailureSeverity({ failed, succeeded: ok }, lines)
    if (sev === SEV_BAD || sev === SEV_WARN) return { kind: 'tenant', tone: toneOf(sev), failed, total: failed + ok }
  }
  return { kind: 'fine' }
}

// ---- New queue -------------------------------------------------------------------

/**
 * The New queue form as the call it makes. `/configure` creates and merges in
 * one route: `mode: 'merge'` (the old QueueConfigModal's create) keeps a race
 * with someone creating the same name from resetting that queue, and a name
 * the listing already has is refused here rather than turned into an edit.
 * The form's own fields go on the wire explicitly; every other option takes
 * the broker's default.
 *   form   { name, namespace, task, leaseTime, retryLimit } (strings, as typed)
 *   known  anything with .has / .get by name (queuesStore.queueMeta)
 */
export function newQueueDraft(form = {}, known = null) {
  const raw = typeof form.name === 'string' ? form.name : ''
  const name = raw.trim()
  const nameError = queueNameError(raw)
  const exists = existingQueueName(raw, known)
  const lease = readFormValue(OPTION_META.leaseTime, form.leaseTime)
  const retries = readFormValue(OPTION_META.retryLimit, form.retryLimit)
  // Every other option the form carries: only those that differ from the
  // broker's defaults are sent, so the queue gets the defaults for the rest.
  // A cleared field means "the default", which a new queue already has.
  const others = EDITABLE_OPTIONS.map((o) => o.key).filter((k) => k !== 'leaseTime' && k !== 'retryLimit' && k in form)
  const extra = configDiff(OPTION_DEFAULTS, readFormValues(Object.fromEntries(others.map((k) => [k, form[k]]))))
  for (const k of Object.keys(extra)) if (extra[k] === null) delete extra[k]
  const errors = validate({ leaseTime: lease, retryLimit: retries, ...extra })
  const namespace = String(form.namespace ?? '').trim()
  const task = String(form.task ?? '').trim()
  const options = {}
  if (namespace) options.namespace = namespace
  if (task) options.task = task
  options.leaseTime = lease ?? OPTION_DEFAULTS.leaseTime
  options.retryLimit = retries ?? OPTION_DEFAULTS.retryLimit
  Object.assign(options, extra)
  const ok = !nameError && !exists && !Object.keys(errors).length
  return {
    name,
    namespace,
    task,
    nameError,
    exists,
    existsIn: exists && typeof known?.get === 'function' ? (known.get(exists)?.namespace ?? '') : null,
    errors,
    ok,
    leaseTime: options.leaseTime,
    retryLimit: options.retryLimit,
    extra,
    body: buildBody({ queue: name, options, mode: 'merge' }),
  }
}
