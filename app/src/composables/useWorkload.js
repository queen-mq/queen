// Workload page rules — PURE functions, no Vue, no DOM, no colours.
//
// Everything the Workload view decides about a row (its share of the tenant,
// whether an ack ratio is trustworthy, how old the oldest waiting message is,
// how bad it is, what to say about it) lives here, so `npm test` can assert
// the rules against recorded payloads instead of a rendered page.
//
// Two sources are joined here that the endpoint deliberately does NOT join:
//   * GET /api/v1/consumer-groups — per-queue group state and time lag
//   * the queue list (queueMeta)  — queue -> { namespace, task }
// The workload procedure counts groups but never reports their lag, so the
// "oldest waiting" number and the lagging-group findings only exist once the
// consumer-groups panel has loaded. When it has NOT, `oldest` must stay null
// and the caller must say so rather than render a zero.

/** Severity ladder. Higher wins; `mute` is "nothing to say". */
export const SEV_RANK = { bad: 3, warn: 2, ok: 1, ice: 0.5, mute: 0 }

const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const pad = (n) => String(n).padStart(2, '0')

/**
 * The formatters the mock used, kept together so the view and the findings
 * strings phrase a number the same way. Every one of them renders an em dash
 * for null/undefined: an unknown is never a zero.
 */
export const formatters = {
  n: (n) => (n === null || n === undefined) ? '—' : Math.round(n).toLocaleString('en-US'),
  k: (n) => n === null || n === undefined ? '—'
    : Math.abs(n) >= 1e6 ? (n / 1e6).toFixed(1) + 'M'
      : Math.abs(n) >= 1e4 ? Math.round(n / 1e3) + 'k'
        : Math.abs(n) >= 1e3 ? (n / 1e3).toFixed(1) + 'k' : String(Math.round(n)),
  pct: (p) => (p === null || p === undefined) ? '—'
    : ((p * 100) < 10 ? (p * 100).toFixed(1) : Math.round(p * 100)) + '%',
  ms: (ms) => (ms === null || ms === undefined) ? '—'
    : ms < 1000 ? Math.round(ms) + 'ms'
      : ms < 60000 ? (ms / 1000).toFixed(1) + 's'
        : ms < 3600000 ? (ms / 60000).toFixed(1) + 'm' : (ms / 3600000).toFixed(1) + 'h',
  sec: (s) => (s === null || s === undefined) ? '—'
    : s < 1 ? '0s'
      : s < 60 ? Math.round(s) + 's'
        : s < 3600 ? Math.floor(s / 60) + 'm ' + Math.round(s % 60) + 's'
          : s < 86400 ? Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm'
            : Math.floor(s / 86400) + 'd ' + Math.floor((s % 86400) / 3600) + 'h',
  bytes: (b) => {
    if (b === null || b === undefined) return '—'
    if (b === 0) return '0 B'
    const k = 1024
    const i = Math.min(4, Math.floor(Math.log(b) / Math.log(k)))
    return (b / Math.pow(k, i)).toFixed(i >= 2 ? 1 : 0) + ' ' + ['B', 'KB', 'MB', 'GB', 'TB'][i]
  },
  plural: (n, w) => formatters.n(n) + ' ' + w + (n === 1 ? '' : 's'),
  /** A signed change for a comparison: '+12%', '−3.4%', '—' without a base. */
  delta: (p) => (p === null || p === undefined) ? '—'
    : (p >= 0 ? '+' : '−') + formatters.pct(Math.abs(p)),
  /** Bucket start as UTC `HH:MM`, prefixed with `Mon DD` when the window spans days. */
  bucket: (iso, multiDay) => {
    const d = new Date(iso)
    const hm = pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes())
    return (multiDay ? MON[d.getUTCMonth()] + ' ' + pad(d.getUTCDate()) + ' ' : '') + hm
  },
}

const { n: fmtN, pct: fmtPct, ms: fmtMs, sec: fmtSec, plural } = formatters

/**
 * Group lag for one row, read off the consumer-groups list.
 *
 * @param {Map<string, Array<object>>} groupsByQueue queue name -> group rows
 * @param {(queue: string) => (string|null)} keyOfQueue maps a queue to the row key
 * @param {string} key the row's key
 * @returns {{lagging: number, maxLag: number|null, groups: number, bad: Array<{group: object, queue: string}>}}
 *          `maxLag` is seconds (the list's `maxTimeLag`), null when no group matched.
 */
export function groupLag(groupsByQueue, keyOfQueue, key) {
  let lagging = 0
  let maxLag = null
  let groups = 0
  const bad = []
  for (const [q, gs] of groupsByQueue) {
    if (keyOfQueue(q) !== key) continue
    for (const g of gs) {
      groups++
      const isLag = g.state === 'Lagging' || (g.partitionsWithLag || 0) > 0
      if (isLag) { lagging++; bad.push({ group: g, queue: q }) }
      const t = g.maxTimeLag || 0
      if (maxLag === null || t > maxLag) maxLag = t
    }
  }
  return { lagging, maxLag, groups, bad }
}

/**
 * Severity of one enriched row. Ported verbatim from the mock: the rules are
 * additive and the worst one wins.
 *
 * @param {object} row an object from {@link enrichRows}
 * @returns {'bad'|'warn'|'ok'|'ice'|'mute'}
 */
export function severity(row) {
  const w = row.window
  const n = row.now
  let sev = 'mute'
  const up = (s) => { if (SEV_RANK[s] > SEV_RANK[sev]) sev = s }
  if (n.pendingWithoutGroup > 0) up('bad')
  if (row.oldest !== null && row.oldest >= 300) up('bad')
  else if (row.oldest !== null && row.oldest >= 60) up('warn')
  if (row.acks >= 20 && row.ackOk < 0.5) up('bad')
  else if (row.acks >= 20 && row.ackOk < 0.9) up('warn')
  if (w.maxLagMs !== null && w.maxLagMs >= 60000) up('warn')
  if (n.deadLetter >= 1000) up('warn')
  if (sev === 'mute') {
    if (w.pushMessages + w.popMessages > 0) sev = 'ok'
    else if (w.popEmpty > 0 || w.parkedAvg > 0) sev = 'ice'
  }
  return sev
}

/**
 * Enrich the endpoint's rows with everything the page derives.
 *
 * Derived fields, and what each is honest about:
 *   share   — popMessages against `payload.tenant.window.popMessages`, so the
 *             number means "of the tenant" even when the rows are filtered.
 *   ackOk   — null under 5 acks (a 1-of-1 failure is not a 0% success rate)
 *   fill    — null under 5 polls, else pops / (pops + empty polls)
 *   oldest  — seconds, the MAX `maxTimeLag` over the groups of this row's
 *             queues; null when the row has no group at all (nothing to know)
 *
 * @param {object} payload GET /api/v1/analytics/workload response
 * @param {Map<string, Array<object>>} groupsByQueue queue -> consumer-group rows ('' / empty map when unknown)
 * @param {Map<string, {namespace: string, task: string}>} queueMeta queue -> its namespace/task
 * @param {'namespace'|'task'|'queue'} level how the rows are keyed
 * @returns {Array<object>} the payload rows plus name/level/acks/share/ackOk/fill/oldest/groupsLagging/groupsN/laggingGroups/touched/sev
 */
export function enrichRows(payload, groupsByQueue, queueMeta, level) {
  if (!payload || !Array.isArray(payload.rows)) return []
  const groups = groupsByQueue || new Map()
  const meta = queueMeta || new Map()
  const keyOfQueue = level === 'queue'
    ? (q) => q
    : (q) => {
      const m = meta.get(q)
      return m ? (level === 'task' ? m.task : m.namespace) : null
    }
  const tenantPop = payload.tenant?.window?.popMessages || 0
  const fallbackName = level === 'task' ? '(no task)' : '(no namespace)'
  return payload.rows.map((r) => {
    const w = r.window
    const n = r.now
    const acks = w.ackSuccess + w.ackFailed
    const gl = groupLag(groups, keyOfQueue, r.key)
    const e = {
      ...r,
      name: level === 'queue' ? r.key : (r.key || fallbackName),
      level,
      acks,
      share: tenantPop ? w.popMessages / tenantPop : 0,
      ackOk: acks >= 5 ? w.ackSuccess / acks : null,
      fill: (w.popMessages + w.popEmpty) >= 5 ? w.popMessages / (w.popMessages + w.popEmpty) : null,
      oldest: gl.groups ? gl.maxLag : null,
      groupsLagging: gl.lagging,
      groupsN: gl.groups,
      laggingGroups: gl.bad,
      touched: n.queuesTouched > 0,
    }
    e.sev = severity(e)
    return e
  })
}

/**
 * "What needs attention": one sentence per rule per row, worst first.
 *
 * `hasGroups` suspends the two rules that can only be decided from the
 * consumer-groups list — the no-consumer-group rule and the lagging-group
 * rule — because with that panel down a queue with no group looks exactly
 * like a queue whose groups we could not read.
 *
 * @param {Array<object>} rows output of {@link enrichRows}
 * @param {'namespace'|'task'|'queue'} level
 * @param {string} rangeLabel e.g. 'last 1h', used in the sentences
 * @param {{hasGroups?: boolean}} [options]
 * @returns {Array<{sev: string, key: string, name: string, text: string, evidence: string}>}
 */
export function findings(rows, level, rangeLabel, options = {}) {
  const hasGroups = options.hasGroups !== false
  const out = []
  const sevOf = (s) => s >= 300 ? 'bad' : 'warn'
  for (const r of rows) {
    const w = r.window
    const n = r.now
    const base = { key: r.key, name: r.name, row: r }
    if (hasGroups && n.pendingWithoutGroup > 0) {
      out.push({
        ...base, sev: 'bad',
        text: `${r.name}: ${fmtN(n.pendingWithoutGroup)} pending messages on ${level === 'queue' ? 'a queue' : 'queues'} with no consumer group`,
        evidence: level === 'queue'
          ? `${plural(n.partitions, 'partition')} · pushed ${fmtN(w.pushMessages)} in the window`
          : `${fmtN(n.queuesWithoutGroup)} of ${fmtN(r.queues)} queues have no group · drill in to see which`,
      })
    }
    if (hasGroups) {
      for (const { group: g, queue: q } of r.laggingGroups) {
        if ((g.maxTimeLag || 0) < 60) continue
        out.push({
          ...base, sev: sevOf(g.maxTimeLag),
          text: `${g.name === '__QUEUE_MODE__' ? 'queue mode' : g.name} on ${q} is ${fmtSec(g.maxTimeLag)} behind`,
          evidence: `${fmtN(g.partitionsWithLag)} of ${fmtN(g.members)} partitions lagging · ${fmtN(g.totalLag || 0)} messages`,
        })
      }
    }
    if (r.acks >= 20 && r.ackOk < 0.9) {
      out.push({
        ...base, sev: r.ackOk < 0.5 ? 'bad' : 'warn',
        text: `${r.name}: ${fmtN(w.ackFailed)} of ${fmtN(r.acks)} acks failed (${fmtPct(r.ackOk)} ok)`,
        evidence: `${fmtN(w.popMessages)} delivered · ${fmtN(n.deadLetter)} in DLQ now`,
      })
    }
    if (w.maxLagMs !== null && w.maxLagMs >= 60000) {
      out.push({
        ...base, sev: 'warn',
        text: `${r.name}: a delivery waited ${fmtMs(w.maxLagMs)} before being popped`,
        evidence: `pop-weighted average ${fmtMs(w.avgLagMs)} over ${fmtN(w.popMessages)} deliveries`,
      })
    }
    if (n.deadLetter >= 1000) {
      out.push({
        ...base, sev: 'warn',
        text: `${r.name}: ${fmtN(n.deadLetter)} rows in dead-letter queues`,
        evidence: level === 'queue'
          ? `${plural(n.partitions, 'partition')}`
          : `across ${plural(r.queues, 'queue')} · drill in for the split`,
      })
    }
    if (w.popEmpty >= 1000 && r.fill !== null && r.fill < 0.05) {
      out.push({
        ...base, sev: 'ice',
        text: `${r.name}: ${fmtN(w.popEmpty)} empty polls for ${fmtN(w.popMessages)} deliveries`,
        evidence: `${fmtN(Math.round(w.parkedAvg))} long-polls parked on average · ${fmtN(r.groupsN)} groups · fill ${fmtPct(r.fill)}`,
      })
    }
    if (n.queuesTouched === 0 && r.groupsN === 0 && hasGroups) {
      out.push({
        ...base, sev: 'mute',
        text: `${r.name}: ${plural(n.queues, 'queue')}, nothing pushed, popped or polled in the ${rangeLabel}, no consumer group`,
        evidence: 'cleanup candidates',
      })
    }
  }
  out.sort((a, b) => SEV_RANK[b.sev] - SEV_RANK[a.sev])
  return out
}

/**
 * Stacked-area series for "Work over time": the four biggest rows by
 * deliveries, then one `Other (n)` series that sums the rest. A bucket with
 * no row stays null in every series (a gap, never a zero); `Other` is only
 * non-null where at least one of its rows had a value.
 *
 * @param {Array<object>} rows output of {@link enrichRows}
 * @param {Array<string>} buckets payload.buckets
 * @param {'push'|'pop'|'popEmpty'|'ackFailed'|'parked'} metric series key
 * @returns {Array<{label: string, data: Array<number|null>, index: number, key: string|null}>}
 *          at most 5 entries; `index` is the palette slot, `key` is null for `Other`.
 */
export function flowSeries(rows, buckets, metric) {
  const sorted = [...rows].sort((a, b) => b.window.popMessages - a.window.popMessages)
  const top = sorted.slice(0, 4)
  const rest = sorted.slice(4)
  const series = top.map((r, i) => ({
    label: r.name, key: r.key, index: i, data: (r.series[metric] || []).map((v) => v),
  }))
  if (rest.length) {
    series.push({
      label: `Other (${rest.length})`,
      key: null,
      index: 4,
      data: (buckets || []).map((_, i) => rest.reduce((s, r) => {
        const v = r.series[metric][i]
        return v === null || v === undefined ? s : (s === null ? v : s + v)
      }, null)),
    })
  }
  return series
}

/**
 * Activity map cells: one row per group, one cell per bucket, sorted by
 * deliveries. `percent` is sqrt-scaled against the busiest cell on screen —
 * linear scaling makes every row but the top one black.
 *
 * @param {Array<object>} rows output of {@link enrichRows}
 * @returns {{max: number, rows: Array<{key: string, name: string, total: number,
 *           values: Array<{value: number|null, percent: number|null}>}>}}
 */
/**
 * Drop the last bucket while it is still open. Metrics land on minute
 * boundaries, so the bucket that contains `now` is always partial: drawn, it
 * is a phantom drop to zero on the right edge of every series, and it steals
 * the edge labels. Returns the payload itself when nothing needs trimming.
 */
export function trimOpenBucket(payload, now = Date.now()) {
  const b = payload?.buckets
  if (!Array.isArray(b) || !b.length) return payload
  const width = (payload.bucketMinutes || 1) * 60_000
  const last = new Date(b[b.length - 1]).getTime()
  if (!Number.isFinite(last) || last + width <= now) return payload
  const cut = (arr) => (Array.isArray(arr) ? arr.slice(0, b.length - 1) : arr)
  const cutSeries = (series) => Object.fromEntries(Object.entries(series || {}).map(([k, v]) => [k, cut(v)]))
  return {
    ...payload,
    buckets: cut(b),
    rows: (payload.rows || []).map((r) => ({ ...r, series: cutSeries(r.series) })),
    tenant: payload.tenant ? { ...payload.tenant, series: cutSeries(payload.tenant.series) } : payload.tenant,
  }
}

export function heatCells(rows) {
  const sorted = [...rows].sort((a, b) => b.window.popMessages - a.window.popMessages)
  let max = 0
  for (const r of sorted) for (const v of (r.series.pop || [])) if (v !== null && v > max) max = v
  return {
    max,
    rows: sorted.map((r) => ({
      key: r.key,
      name: r.name,
      total: r.window.popMessages,
      values: (r.series.pop || []).map((v) => ({
        value: v === null || v === undefined ? null : v,
        percent: v === null || v === undefined ? null : (max ? Math.round(Math.sqrt(v / max) * 100) : 0),
      })),
    })),
  }
}

/**
 * How the row spent its active buckets, by worst lag at pop. Only buckets
 * with a delivery count (a non-null maxLagMs) count as active.
 *
 * @param {object} row an enriched row
 * @returns {{buckets: number, under10s: number, from10to60: number, over60s: number}}
 */
// ---------------------------------------------------------------------------
// Comparing two windows
// ---------------------------------------------------------------------------

/**
 * The window to compare against. `previous` is the same length immediately
 * before; `yesterday` and `lastWeek` are the same clock window one day and
 * seven days earlier, which is what a daily or a weekly cycle wants.
 */
export function comparisonRange({ from, to }, mode) {
  const f = new Date(from).getTime()
  const t = new Date(to).getTime()
  const back = mode === 'yesterday' ? 86_400_000 : mode === 'lastWeek' ? 7 * 86_400_000 : (t - f)
  return { from: new Date(f - back), to: new Date(t - back) }
}

const WINDOW_KEYS = ['pushMessages', 'popMessages', 'ackSuccess', 'ackFailed', 'popEmpty']

/**
 * Per-counter change of a window against its comparison window. `pct` is
 * null when the base is zero: "+∞%" is not a number anyone can act on, and
 * the absolute change is right there.
 */
export function windowDeltas(current, previous, keys = WINDOW_KEYS) {
  if (!current || !previous) return null
  const out = {}
  for (const k of keys) {
    const a = current[k]
    const b = previous[k]
    const ok = Number.isFinite(a) && Number.isFinite(b)
    out[k] = { abs: ok ? a - b : null, pct: ok && b > 0 ? (a - b) / b : null, prev: ok ? b : null }
  }
  return out
}

/** One series per bucket: the sum over rows, null where every row is null. */
export function totalSeries(rows, buckets, metric) {
  return (buckets || []).map((_, i) => rows.reduce((s, r) => {
    const v = (r.series[metric] || [])[i]
    return v === null || v === undefined ? s : (s === null ? v : s + v)
  }, null))
}

export function lagBudget(row) {
  const out = { buckets: 0, under10s: 0, from10to60: 0, over60s: 0 }
  for (const v of (row.series?.maxLagMs || [])) {
    if (v === null || v === undefined) continue
    out.buckets++
    if (v >= 60000) out.over60s++
    else if (v >= 10000) out.from10to60++
    else out.under10s++
  }
  return out
}

// ===========================================================================
// THE DEEPER LAYER
//
// Everything below answers a question the panels above cannot: not "who is
// busy" but "is this normal", "what does one push cost", "why are these rows
// in the DLQ", "where are the bytes". Same discipline as the rest of this
// file: pure, no Vue, and an unknown is null — never a zero.
//
// The inputs are the SAME workload payload (WORKLOAD_CONTRACT.md), fetched
// over two extra windows: 7d at hourly buckets for the weekly profile and the
// same-hour baseline, 24h at queue level for the efficiency ratios. Nothing
// here reads a raw metrics row.
// ===========================================================================

const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
const round = (v, d) => (v === null || v === undefined || !Number.isFinite(v)
  ? null : Math.round(v * 10 ** d) / 10 ** d)

/** Empty 7x24 matrix of nulls — a cell stays null until a bucket lands in it. */
const emptyWeek = () => Array.from({ length: 7 }, () => new Array(24).fill(null))

/**
 * Day-of-week x hour-of-day profile of deliveries, in UTC.
 *
 * Every bucket of a 7d payload is dropped into its (weekday, hour) cell and
 * summed; a cell no bucket reached stays null, so an hour the window never
 * covered is drawn as "no bucket", not as a quiet hour. UTC on purpose: the
 * buckets are UTC and a local-time grid would smear a spike across two cells
 * for half the year.
 *
 * @param {object} payload7d a workload response over ~7 days, hourly buckets
 * @returns {{days: Array<string>, tenant: Array<Array<number|null>>,
 *            byKey: Map<string, Array<Array<number|null>>>, totals: Map<string, number>}}
 */
export function weeklyProfile(payload7d) {
  const out = { days: [], tenant: emptyWeek(), byKey: new Map(), totals: new Map() }
  if (!payload7d || !Array.isArray(payload7d.buckets)) return out
  const buckets = payload7d.buckets
  const days = new Set()
  const add = (m, dow, h, v) => { m[dow][h] = (m[dow][h] === null ? 0 : m[dow][h]) + v }

  const stamps = buckets.map((b) => {
    const d = new Date(b)
    days.add(b.slice(0, 10))
    return { dow: d.getUTCDay(), h: d.getUTCHours() }
  })
  out.days = [...days].sort()

  const tenantPop = payload7d.tenant?.series?.pop || []
  for (let i = 0; i < stamps.length; i++) {
    const v = tenantPop[i]
    if (v === null || v === undefined) continue
    add(out.tenant, stamps[i].dow, stamps[i].h, v)
  }
  for (const r of (payload7d.rows || [])) {
    const m = emptyWeek()
    let total = 0
    const pop = r.series?.pop || []
    for (let i = 0; i < stamps.length; i++) {
      const v = pop[i]
      if (v === null || v === undefined) continue
      add(m, stamps[i].dow, stamps[i].h, v)
      total += v
    }
    out.byKey.set(r.key, m)
    out.totals.set(r.key, total)
  }
  return out
}

/**
 * "Is this hour normal": the last COMPLETE bucket against the same hour on
 * every earlier day of the window.
 *
 * The final bucket of a live window is partial — it is still filling — so
 * comparing it to whole hours always reads as a collapse. The bucket before
 * it is the newest one that can be trusted, and the comparison set is every
 * bucket at the same UTC hour on an earlier calendar day (matched on the
 * timestamp, not by index arithmetic, so a payload with sub-hourly buckets
 * still lines up).
 *
 * `ratio`, `mean`, `min`, `max` and `z` are null when there is no earlier day
 * to compare with — one day of history is not a baseline.
 *
 * @param {object} payload7d a workload response over ~7 days, hourly buckets
 * @returns {{hour: string|null, index: number, label: string,
 *            rows: Array<{key: string, current: number|null, mean: number|null,
 *                         min: number|null, max: number|null, ratio: number|null,
 *                         z: number|null, days: number}>}}
 *          `rows[0].key` is `'*'` — the tenant.
 */
export function sameHourBaseline(payload7d) {
  const empty = { hour: null, index: -1, label: '', rows: [] }
  if (!payload7d || !Array.isArray(payload7d.buckets) || payload7d.buckets.length < 2) return empty
  const buckets = payload7d.buckets
  // The last bucket is still filling; the one before it is the newest whole one.
  const index = buckets.length - 2
  const hour = buckets[index]
  const at = new Date(hour)
  const peers = []
  for (let i = 0; i < index; i++) {
    const d = new Date(buckets[i])
    if (d.getUTCHours() === at.getUTCHours() && d.getUTCDate() !== at.getUTCDate()) peers.push(i)
  }

  const stat = (key, series) => {
    const current = series[index] ?? null
    const vals = peers.map((i) => series[i]).filter((v) => v !== null && v !== undefined)
    if (!vals.length) {
      return { key, current, mean: null, min: null, max: null, ratio: null, z: null, days: 0 }
    }
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length
    const sd = Math.sqrt(vals.reduce((a, b) => a + (b - mean) ** 2, 0) / vals.length)
    return {
      key,
      current,
      mean: Math.round(mean),
      min: Math.min(...vals),
      max: Math.max(...vals),
      ratio: current === null ? null : (mean ? round(current / mean, 2) : null),
      z: current === null || vals.length < 2 || !sd ? null : round((current - mean) / sd, 1),
      days: vals.length,
    }
  }

  const rows = [stat('*', payload7d.tenant?.series?.pop || [])]
  for (const r of (payload7d.rows || [])) rows.push(stat(r.key, r.series?.pop || []))
  return {
    hour,
    index,
    label: `${DOW[at.getUTCDay()]} ${pad(at.getUTCHours())}:00 UTC`,
    rows,
  }
}

/**
 * What one queue's traffic costs, from the 24h queue-level payload.
 *
 * Every ratio has a denominator that can be zero and a meaning that dies with
 * it, so each is null rather than 0:
 *   fanout         deliveries per pushed message — null with 0 pushes, which
 *                  is the /transaction case (the collector bumps
 *                  transaction_count and attributes no push)
 *   ackPerDelivery acks the counters attribute, per delivery — an ack made
 *                  inside /transaction is NOT counted here, so a low value
 *                  can mean "acked elsewhere", not "not acked"
 *   pushBatch      messages per push request; ackBatch, per ack request
 *   emptyPerDelivery empty polls paid per delivery
 *
 * @param {object} payload24h workload response, groupBy=queue, ~24h
 * @param {Map<string, Array<object>>} [groupsByQueue] queue -> consumer-group rows
 * @returns {Array<object>} one row per queue, deliveries desc
 */
export function efficiency(payload24h, groupsByQueue) {
  if (!payload24h || !Array.isArray(payload24h.rows)) return []
  const gs = groupsByQueue || new Map()
  return payload24h.rows.map((r) => {
    const w = r.window
    const pop = w.popMessages
    const ack = w.ackSuccess
    const acks = w.ackSuccess + w.ackFailed
    const known = gs.size ? (gs.get(r.key) || []).length : (r.now?.groups ?? 0)
    return {
      queue: r.key,
      namespace: r.namespace ?? null,
      push: w.pushMessages,
      pushReq: w.pushRequests,
      pop,
      popEmpty: w.popEmpty,
      ack,
      ackf: w.ackFailed,
      ackReq: w.ackRequests,
      trx: w.transactions,
      // The window's own partition churn — the liveness route reports
      // creations but deliberately no deletions, so this is where a delete
      // count can come from at all.
      partitionsCreated: w.partitionsCreated,
      partitionsDeleted: w.partitionsDeleted,
      groups: known,
      pending: r.now?.pending ?? null,
      deadLetter: r.now?.deadLetter ?? null,
      retainedBytes: r.now?.retainedBytes ?? null,
      partitions: r.now?.partitions ?? null,
      fanout: w.pushMessages > 0 ? round(pop / w.pushMessages, 2) : null,
      ackPerDelivery: pop > 0 ? round(ack / pop, 3) : null,
      pushBatch: w.pushRequests > 0 ? round(w.pushMessages / w.pushRequests, 1) : null,
      ackBatch: w.ackRequests > 0 ? round(acks / w.ackRequests, 1) : null,
      emptyPerDelivery: pop > 0 ? round(w.popEmpty / pop, 2) : null,
      lagBudget: lagBudget(r),
    }
  }).sort((a, b) => b.pop - a.pop)
}

/**
 * The deeper "what needs attention": the rules that need the second layer.
 *
 * Two of them are DELIBERATELY consolidated into one line each — the
 * transaction-fed queues and the acked-elsewhere queues — because both are
 * one fact about how the collector attributes work, and printing it 30 times
 * turns a list into a wall nobody reads.
 *
 * Every input is optional: a card whose endpoint 404s on this broker simply
 * contributes no rule, which is not the same as contributing a clean bill.
 *
 * @param {{efficiency?: Array<object>, baseline?: object, dlq?: Array<object>,
 *          retention?: Array<object>, partitions?: Array<object>}} input
 * @returns {Array<{sev: string, text: string, evidence: string}>} worst first
 */
export function deeperFindings(input = {}) {
  const eff = input.efficiency || []
  const dlq = input.dlq || []
  const retention = input.retention || []
  const partitions = input.partitions || []
  const baseline = input.baseline || { rows: [] }
  const byQueue = new Map(eff.map((e) => [e.queue, e]))
  const f = []

  // Loss by policy: a message dropped for age was never delivered to anyone.
  for (const r of retention) {
    if (!(r.eviction > 0)) continue
    const e = byQueue.get(r.key) || {}
    f.push({
      sev: 'warn',
      text: `${r.key}: ${fmtN(r.eviction)} messages evicted as older than the queue's max wait`,
      evidence: `${plural(e.groups || 0, 'consumer group')} on the queue · ${fmtN(r.retention)} age-retained · loss by policy`,
    })
  }

  const fed = eff.filter((e) => e.push === 0 && e.trx > 0 && e.pop >= 1000).sort((a, b) => b.pop - a.pop)
  if (fed.length) {
    f.push({
      sev: 'warn',
      text: `${fmtN(fed.length)} queues are fed through /transaction: ${fmtN(fed.reduce((a, e) => a + e.pop, 0))} deliveries with 0 pushes counted`,
      evidence: fed.slice(0, 5).map((e) => `${e.queue} ${fmtK(e.pop)}`).join(' · ')
        + (fed.length > 5 ? ' · …' : '')
        + ' · the collector bumps transaction_count only, pushes are not attributed',
    })
  }

  const elsewhere = eff.filter((e) => (
    e.pop >= 1000 && e.ackPerDelivery !== null && e.ackPerDelivery < 0.5
    && e.fanout !== null && e.fanout <= (e.groups || 1) * 1.25
  )).sort((a, b) => b.pop - a.pop)
  if (elsewhere.length) {
    f.push({
      sev: 'warn',
      text: `${fmtN(elsewhere.length)} queues show under 50% of deliveries acked by the counters`,
      evidence: elsewhere.slice(0, 5).map((e) => `${e.queue} ${fmtPct(e.ackPerDelivery)}`).join(' · ')
        + ' · deliveries ≈ pushes and nothing piles up: the acks happen inside /transaction and are not attributed',
    })
  }

  // Chronic, not incidental: a quarter of the queue's ACTIVE buckets over 60s.
  for (const e of eff) {
    const b = e.lagBudget
    if (!b || b.buckets < 20 || b.over60s / b.buckets < 0.25) continue
    f.push({
      sev: 'warn',
      text: `${e.queue}: a delivery waited over 60s in ${fmtN(b.over60s)} of ${fmtN(b.buckets)} active buckets`,
      evidence: `${fmtN(b.from10to60)} more between 10 and 60s · ${fmtN(e.pop)} deliveries`,
    })
  }

  const one = dlq.filter((d) => (
    d.signatures?.length && d.signatures[0].share >= 0.5 && (d.rowsNow || 0) >= 100
  ))
  if (one.length) {
    f.push({
      sev: 'warn',
      text: `${fmtN(one.length)} dead-letter queues are each mostly one error`,
      evidence: one.map((d) => `${d.queue} ${fmtPct(d.signatures[0].share)} "${d.signatures[0].text.slice(0, 46)}…"`).join(' · '),
    })
  }

  const heavy = dlq.filter((d) => (d.avgBytes || 0) >= 100_000)
  if (heavy.length) {
    f.push({
      sev: 'warn',
      text: `Dead-letter rows of up to ${formatters.bytes(Math.max(...heavy.map((d) => d.avgBytes)))} each on ${plural(heavy.length, 'queue')}`,
      evidence: heavy.map((d) => `${d.queue} ${formatters.bytes(d.avgBytes)} per row over ${fmtN(d.sample)} rows`).join(' · '),
    })
  }

  for (const r of (baseline.rows || [])) {
    if (r.key === '*' || r.ratio === null) continue
    if (r.ratio < 2 && r.ratio > 0.5) continue
    f.push({
      sev: 'warn',
      text: `${r.key || '(no namespace)'}: ${r.ratio}× its usual deliveries for this hour`,
      evidence: `${fmtN(r.current)} vs ${fmtN(r.mean)} (min ${fmtN(r.min)}, max ${fmtN(r.max)}) over ${plural(r.days, 'day')}`,
    })
  }

  if (partitions.length) {
    const tot = partitions.reduce((a, p) => ({ t: a.t + (p.partitions || 0), l: a.l + (p.live24h || 0) }), { t: 0, l: 0 })
    const churn = [...partitions].sort((a, b) => (b.created24h || 0) - (a.created24h || 0))[0]
    f.push({
      sev: 'mute',
      text: `${fmtN(tot.l)} of ${fmtN(tot.t)} partitions were touched in the last 24h`,
      evidence: churn ? `${churn.queue} created ${fmtN(churn.created24h)} partitions in a day` : '',
    })
  }

  f.sort((a, b) => SEV_RANK[b.sev] - SEV_RANK[a.sev])
  return f
}

const { k: fmtK } = formatters

// ===========================================================================
// FALLBACK: the same payload, rolled up in the browser.
//
// /api/v1/analytics/workload does not exist on any deployed broker yet, and a
// page that renders "not available" on every cluster is a page nobody sees.
// So: when the endpoint 404s, the view fetches the reads that DO exist
// (queue-ops for the window, status/queues + resources/queues for `now`, the
// consumer-group list for the group counts) and this function folds them into
// EXACTLY the contract-1 shape, so nothing downstream — enrichRows, findings,
// flowSeries, heatCells, the whole deeper layer — knows the difference.
//
// Two honest differences from the server-side procedure, both narrower rather
// than wider:
//   * `buckets` are the distinct buckets the ROWS carry, not a generate_series
//     over the window: a bucket in which nothing at all happened anywhere does
//     not appear (server-side it would appear, null in every series).
//   * `queues` / `queuesWithoutGroup` count the queues the queue list knows
//     about, so a queue created after the list was cached is missing from the
//     count until the next fetch.
// ===========================================================================

const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : 0)

/**
 * Fold the pre-workload endpoints into a contract-1 workload payload.
 *
 * @param {object} input
 * @param {object|Array} input.ops GET /api/v1/analytics/queue-ops (payload or its `series`)
 * @param {Array<object>} [input.statusQueues] GET /api/v1/status/queues rows (pending/processing/deadLetter/partitions)
 * @param {Array<object>} [input.resourceQueues] GET /api/v1/resources/queues rows (namespace/task/retainedBytes)
 * @param {Array<object>} [input.consumerGroups] GET /api/v1/consumer-groups rows
 * @param {'namespace'|'task'|'queue'} [input.groupBy]
 * @param {string} [input.namespace] / [input.task] / [input.queue] exact-match filters, as the endpoint takes them
 * @param {Date|string} [input.capturedAt]
 * @returns {object} the WORKLOAD_CONTRACT.md payload
 */
export function rollupFromQueueOps(input = {}) {
  const groupBy = input.groupBy || 'namespace'
  const series = Array.isArray(input.ops) ? input.ops : (input.ops?.series || [])
  const status = input.statusQueues || []
  const resource = input.resourceQueues || []
  const cgs = input.consumerGroups || []

  // queue -> namespace/task, and the full queue set: a queue with no metrics
  // row in the window is still a queue of the group (`queues`, and the
  // no-consumer-group rules depend on it).
  const meta = new Map()
  for (const q of resource) {
    if (!q?.name) continue
    meta.set(q.name, { namespace: q.namespace || '', task: q.task || '' })
  }
  const statusByQueue = new Map()
  for (const q of status) {
    const name = q?.name || q?.queueName || q?.queue
    if (!name) continue
    statusByQueue.set(name, q)
    if (!meta.has(name)) meta.set(name, { namespace: q.namespace || '', task: q.task || '' })
  }
  const bytesByQueue = new Map()
  for (const q of resource) if (q?.name) bytesByQueue.set(q.name, num(q.retainedBytes))

  const groupsPerQueue = new Map()
  for (const g of cgs) {
    const q = g?.queueName
    if (!q) continue
    groupsPerQueue.set(q, (groupsPerQueue.get(q) || 0) + 1)
  }

  const keyOf = (queue) => {
    if (groupBy === 'queue') return queue
    const m = meta.get(queue)
    if (!m) return null
    return groupBy === 'task' ? m.task : m.namespace
  }
  const passes = (queue) => {
    const m = meta.get(queue) || { namespace: '', task: '' }
    if (input.queue !== undefined && input.queue !== null && queue !== input.queue) return false
    if (input.namespace !== undefined && input.namespace !== null && m.namespace !== input.namespace) return false
    if (input.task !== undefined && input.task !== null && m.task !== input.task) return false
    return true
  }

  const buckets = [...new Set(series.map((s) => s.bucket))].sort()
  const bucketIndex = new Map(buckets.map((b, i) => [b, i]))

  const blankWindow = () => ({
    pushMessages: 0, pushRequests: 0, popMessages: 0, popEmpty: 0, ackRequests: 0,
    ackSuccess: 0, ackFailed: 0, transactions: 0, conflated: 0,
    partitionsCreated: 0, partitionsDeleted: 0, parkedAvg: 0, avgLagMs: null, maxLagMs: null,
  })
  const blankSeries = () => ({
    push: new Array(buckets.length).fill(null),
    pop: new Array(buckets.length).fill(null),
    popEmpty: new Array(buckets.length).fill(null),
    ackFailed: new Array(buckets.length).fill(null),
    parked: new Array(buckets.length).fill(null),
    avgLagMs: new Array(buckets.length).fill(null),
    maxLagMs: new Array(buckets.length).fill(null),
  })
  const blankNow = () => ({
    pending: 0, processing: 0, deadLetter: 0, retainedBytes: 0, partitions: 0,
    groups: 0, queuesWithoutGroup: 0, pendingWithoutGroup: 0, queuesTouched: 0, queuesActive: 0,
  })
  // Accumulators the payload does not carry: the pop-weighted lag numerator,
  // the per-queue parked averages, and per (group, bucket) lag weights.
  const mk = () => ({
    window: blankWindow(), series: blankSeries(), now: blankNow(),
    queues: new Set(), touched: new Set(), active: new Map(),
    lagNum: 0, lagDen: 0, parked: new Map(),
    bucketLag: new Map(), bucketParked: new Map(),
  })

  const groups = new Map()
  const tenant = mk()
  const groupOf = (key) => {
    if (!groups.has(key)) groups.set(key, mk())
    return groups.get(key)
  }

  // Every queue of the tenant belongs to the tenant totals; only the ones that
  // pass the filters belong to a row (exactly as the endpoint scopes them).
  for (const [queue] of meta) {
    tenant.queues.add(queue)
    if (!passes(queue)) continue
    const key = keyOf(queue)
    if (key === null) continue
    groupOf(key).queues.add(queue)
  }

  const addRow = (acc, row) => {
    const w = acc.window
    const i = bucketIndex.get(row.bucket)
    w.pushMessages += num(row.pushMessages)
    w.pushRequests += num(row.pushRequests)
    w.popMessages += num(row.popMessages)
    w.popEmpty += num(row.popEmpty)
    w.ackRequests += num(row.ackRequests)
    w.ackSuccess += num(row.ackSuccess)
    w.ackFailed += num(row.ackFailed)
    w.transactions += num(row.transactions)
    w.partitionsCreated += num(row.partitionsCreated)
    w.partitionsDeleted += num(row.partitionsDeleted)
    acc.touched.add(row.queueName)
    acc.active.set(row.queueName,
      (acc.active.get(row.queueName) || 0) + num(row.pushMessages) + num(row.popMessages))
    const parked = acc.parked.get(row.queueName) || { sum: 0, n: 0 }
    parked.sum += num(row.parkedCount)
    parked.n += 1
    acc.parked.set(row.queueName, parked)
    if (i === undefined) return
    const s = acc.series
    s.push[i] = num(s.push[i]) + num(row.pushMessages)
    s.pop[i] = num(s.pop[i]) + num(row.popMessages)
    s.popEmpty[i] = num(s.popEmpty[i]) + num(row.popEmpty)
    s.ackFailed[i] = num(s.ackFailed[i]) + num(row.ackFailed)
    // parked is a gauge: per-queue average within the bucket, summed across
    // queues — one row per (queue, bucket) here, so the average is the value.
    s.parked[i] = num(s.parked[i]) + num(row.parkedCount)
    const pops = num(row.popMessages)
    if (pops > 0 && row.avgLagMs !== null && row.avgLagMs !== undefined) {
      acc.lagNum += Number(row.avgLagMs) * pops
      acc.lagDen += pops
      const b = acc.bucketLag.get(i) || { num: 0, den: 0, max: null }
      b.num += Number(row.avgLagMs) * pops
      b.den += pops
      const mx = row.maxLagMs === null || row.maxLagMs === undefined ? null : Number(row.maxLagMs)
      if (mx !== null && (b.max === null || mx > b.max)) b.max = mx
      acc.bucketLag.set(i, b)
      if (mx !== null && (w.maxLagMs === null || mx > w.maxLagMs)) w.maxLagMs = mx
    }
  }

  for (const row of series) {
    if (!row?.queueName) continue
    addRow(tenant, row)
    if (!passes(row.queueName)) continue
    const key = keyOf(row.queueName)
    if (key === null) continue
    addRow(groupOf(key), row)
  }

  const finish = (acc) => {
    const w = acc.window
    w.avgLagMs = acc.lagDen ? Math.round(acc.lagNum / acc.lagDen) : null
    // parkedAvg: per-queue AVG over ITS rows, then SUM across queues.
    let parkedAvg = 0
    for (const p of acc.parked.values()) parkedAvg += p.n ? p.sum / p.n : 0
    w.parkedAvg = Math.round(parkedAvg * 100) / 100
    for (const [i, b] of acc.bucketLag) {
      acc.series.avgLagMs[i] = b.den ? Math.round(b.num / b.den) : null
      acc.series.maxLagMs[i] = b.max
    }
    const now = acc.now
    for (const queue of acc.queues) {
      const st = statusByQueue.get(queue) || {}
      const m = st.messages || {}
      now.pending += num(m.pending ?? st.pending)
      now.processing += num(m.processing ?? st.processing)
      now.deadLetter += num(m.deadLetter ?? m.dead_letter ?? st.deadLetter)
      now.partitions += num(st.partitions ?? st.partitionCount)
      now.retainedBytes += num(bytesByQueue.get(queue))
      const g = groupsPerQueue.get(queue) || 0
      now.groups += g
      if (g === 0) {
        now.queuesWithoutGroup += 1
        now.pendingWithoutGroup += num(m.pending ?? st.pending)
      }
    }
    now.queuesTouched = [...acc.touched].filter((q) => acc.queues.has(q)).length
    now.queuesActive = [...acc.active].filter(([q, v]) => v > 0 && acc.queues.has(q)).length
    return { queues: acc.queues.size, window: w, series: acc.series, now }
  }

  const rows = [...groups.entries()].map(([key, acc]) => {
    const done = finish(acc)
    const row = { key, ...done }
    if (groupBy === 'queue') {
      const m = meta.get(key) || { namespace: '', task: '' }
      return { key, namespace: m.namespace, task: m.task, ...done }
    }
    return row
  }).sort((a, b) => b.window.popMessages - a.window.popMessages)

  const capturedAt = input.capturedAt ? new Date(input.capturedAt) : new Date()
  const from = input.ops?.timeRange?.from || (buckets.length ? buckets[0] : capturedAt.toISOString())
  const to = input.ops?.timeRange?.to || capturedAt.toISOString()
  return {
    timeRange: { from, to },
    bucketMinutes: input.ops?.bucketMinutes ?? 0,
    groupBy,
    buckets,
    rows,
    tenant: finish(tenant),
    computedClientSide: true,
  }
}
