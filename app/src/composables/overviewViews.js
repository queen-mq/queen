// The Overview page (views/Dashboard.vue) and its nine views, picked in its
// sub sidebar and kept in the address as ?view= (OVERVIEW_VIEWS):
//
//   Summary (no ?view=), Issues        the answer, and everything that needs you
//   Cell                               the cell compact: its verdict, its issues
//                                      and each node now (System has the rest);
//                                      a live operator's only
//   Throughput, Backlog, Consumer      the tenant's queues, one namespace or
//   groups, Dead letters               all of them
//
// Everything here is a pure function over the payloads the page already
// reads. No Vue, no '@/': test/overview-views.test.js pins it.

import { inNamespace } from './navigation.js'
import { acksText } from './queueOps.js'
import { byLagInactiveLast, groupAttention, queueAttention } from './useAttention.js'
import { formatRate, inUnit, rateUnit } from './useTraffic.js'

const RANK = { bad: 2, warn: 1 }

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const count = (v) => Math.round(v).toLocaleString('en-US')
const capital = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1) : s)

/** The worst of several severities as a mark: 'bad', 'warn', or null when nothing needs you. */
export function worstMark(sevs) {
  let out = null
  for (const s of sevs || []) if ((RANK[s] || 0) > (RANK[out] || 0)) out = s
  return out
}

// ---------------------------------------------------------------------------
// The views
// ---------------------------------------------------------------------------

/**
 * The Overview's views, in the order of its sub sidebar, under three groups:
 * the answer first (Summary, Issues), then the tenant's queues, then the cell
 * under them (a live operator's only).
 */
export const OVERVIEW_VIEWS = Object.freeze([
  { key: 'summary', name: 'Summary', group: '', icon: 'gauge' },
  { key: 'issues', name: 'Issues', group: '', icon: 'inbox' },
  // The cell under the queues, compact (System holds its charts and its log):
  // a live operator's only.
  { key: 'cell', name: 'Cell', group: '', operator: true, icon: 'server' },
  { key: 'throughput', name: 'Throughput', group: 'Queues', icon: 'activity' },
  { key: 'backlog', name: 'Backlog', group: 'Queues', icon: 'hourglass' },
  { key: 'groups', name: 'Consumer groups', group: 'Queues', icon: 'users-round' },
  { key: 'dead-letters', name: 'Dead letters', group: 'Queues', icon: 'archive-x' },
])
const BY_KEY = new Map(OVERVIEW_VIEWS.map((v) => [v.key, v]))

/** The views a namespace narrows: the four about the tenant's queues. */
export const NAMESPACED_VIEWS = Object.freeze(['throughput', 'backlog', 'groups', 'dead-letters'])

// Older addresses: the views before 2026-10-30 night, and the sections
// before them.
const ALIASES = { overview: 'summary', queues: 'throughput', consumers: 'groups' }

// The cell views the Overview had for a day, and the sections before them:
// their content is System's now, and an old address goes there.
const TO_SYSTEM = { server: 'overview', nodes: 'overview', resources: 'compute', replication: 'raft' }

/** The System view an old Overview address goes to, or null. */
export function systemRedirect(value) {
  return typeof value === 'string' && TO_SYSTEM[value] ? TO_SYSTEM[value] : null
}

/**
 * ?view= → the view shown. An old name lands on the view that holds its
 * content now. An operator's view asked by anyone else, and anything
 * unknown, is the Summary.
 */
export function overviewView(value, canOperate = false) {
  const v = typeof value === 'string' ? (ALIASES[value] || value) : ''
  const known = BY_KEY.get(v)
  return known && (!known.operator || canOperate) ? v : 'summary'
}

/** The ?view= an old address is rewritten to, or null when it is current. */
export function viewAlias(value) {
  return typeof value === 'string' && ALIASES[value] ? ALIASES[value] : null
}

/** The ?view= of a view: none for the Summary, the page's landing. */
export const viewQuery = (key) => (key === 'summary' ? undefined : key)

// ---------------------------------------------------------------------------
// The verdict: the Overview's title, its mark and its word
// ---------------------------------------------------------------------------

/** One cell issue says itself; several are counted. */
const cellHeadline = (list) => (list.length === 1 ? list[0].title : `${list.length} problems in the cell`)

/**
 * The verdict over what needs you. It never says "healthy" while the queues
 * or the groups could not be read: that is unknown. A failing cell is a fact
 * even then, and it leads: every queue stands on it.
 *
 * @param {object} o
 * @param {boolean} o.loading  the queues or the groups are being read for the first time
 * @param {boolean} o.known    both were read, and the last read of each succeeded
 * @param {Array} o.issues     the tenant's: `{ sev, name }` per queue, `{ sev, tenant: true }` for ack failures
 * @param {Array} o.cellIssues the cell's (useBrokerStatus cellIssues): `{ sev, title }`
 * @returns {{ sev: 'ok'|'warn'|'bad'|'unknown', word: string, headline: string }}
 */
export function overviewVerdict({ loading = false, known = false, issues = [], cellIssues = [] } = {}) {
  const cellBad = cellIssues.filter((i) => i.sev === 'bad')
  let sev
  if (cellBad.length) sev = 'bad'
  else if (!known) sev = 'unknown'
  else {
    const all = [...cellIssues, ...issues]
    sev = all.some((i) => i.sev === 'bad') ? 'bad' : all.length ? 'warn' : 'ok'
  }
  const word = { ok: 'Healthy', warn: 'Degraded', bad: 'Failing' }[sev] || (loading ? 'Checking' : 'Unknown')

  let headline
  const queueIssues = issues.filter((i) => !i.tenant)
  const bad = queueIssues.filter((i) => i.sev === 'bad')
  if (cellBad.length) headline = cellHeadline(cellBad)
  else if (loading) headline = 'Checking your queues…'
  else if (!known) headline = 'Status unknown'
  else if (bad.length === 1) headline = `${bad[0].name} ${bad[0].reason === 'acks' ? 'is failing its acks' : 'is falling behind'}`
  else if (bad.length > 1) headline = bad.every((i) => i.reason === 'acks') ? `${bad.length} queues are failing their acks`
    : bad.every((i) => i.reason !== 'acks') ? `${bad.length} queues are falling behind` : `${bad.length} queues are failing`
  else if (queueIssues.length) headline = `${queueIssues.length} ${queueIssues.length === 1 ? 'queue needs' : 'queues need'} attention`
  else if (issues.length) headline = 'Ack failures need attention'
  else if (cellIssues.length) headline = cellHeadline(cellIssues)
  else headline = 'All queues are healthy'
  return { sev, word, headline }
}

// ---------------------------------------------------------------------------
// The sub sidebar: the nine views under their groups, a fixed list
// ---------------------------------------------------------------------------

/**
 * The list, group by group: `[{ label, rows: [{ key, name, mark, num, title }] }]`.
 * `status` gives each view its row's state, `{ mark, num, title }`, from the
 * numbers the page already has; a view it does not mention shows '—'. `mark`
 * is kept only when it is 'warn' or 'bad'. The cell's group is a live
 * operator's only.
 *
 * @param {Record<string, {mark?: string|null, num?: string, title?: string}>} status
 * @param {boolean} canOperate
 */
export function sideGroups(status = {}, canOperate = false) {
  const groups = []
  for (const v of OVERVIEW_VIEWS) {
    if (v.operator && !canOperate) continue
    let g = groups[groups.length - 1]
    if (!g || g.label !== v.group) { g = { label: v.group, rows: [] }; groups.push(g) }
    const st = status[v.key] || {}
    g.rows.push({
      key: v.key,
      name: v.name,
      icon: v.icon || null,
      mark: worstMark([st.mark]),
      num: st.num === undefined || st.num === null || st.num === '' ? '—' : String(st.num),
      title: st.title || '',
    })
  }
  return groups
}

// ---------------------------------------------------------------------------
// Numbers in words
// ---------------------------------------------------------------------------

/** An age in seconds: "0s", "45s", "5m 12s", "2h 4m", "3d 1h"; '—' when unknown. */
export function lagText(seconds) {
  const s = num(seconds)
  if (s === null) return '—'
  if (s === 0) return '0s'
  if (s < 60) return `${Math.round(s)}s`
  if (s < 3600) {
    const m = Math.floor(s / 60)
    const r = Math.round(s % 60)
    return r ? `${m}m ${r}s` : `${m}m`
  }
  if (s < 86400) {
    const h = Math.floor(s / 3600)
    const m = Math.floor((s % 3600) / 60)
    return m ? `${h}h ${m}m` : `${h}h`
  }
  const d = Math.floor(s / 86400)
  const h = Math.floor((s % 86400) / 3600)
  return h ? `${d}d ${h}h` : `${d}d`
}

/** Messages per second for a column that names the unit: "0", "<0.01", "0.07", "12.5", "340", "1.2k"; '—' when unknown. */
export function perSecond(v) {
  const n = num(v)
  if (n === null) return '—'
  if (n === 0) return '0'
  // A trickle is not nothing: thirty messages over six hours still moved.
  if (n < 0.01) return '<0.01'
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`
  if (n >= 100) return String(Math.round(n))
  if (n >= 10) return n.toFixed(1)
  return n.toFixed(2)
}

/** Why a queue needs you, in a few words (queueAttention's reasons); the lag itself is a column of its own. */
export function attentionWhy(a) {
  if (!a) return ''
  if (a.reason === 'lag') return 'A consumer group is behind'
  if (a.reason === 'acks') return a.ops ? `${acksText(a.ops)} in the last hour` : 'Its acks are failing'
  return a.deadOnly ? 'Queue mode has never consumed from it' : 'No consumer group reads it'
}

// ---------------------------------------------------------------------------
// Queues: one namespace, or all of them
// ---------------------------------------------------------------------------

/** The queues of a namespace (null: every namespace; '' is the default one), in the listing's order. */
export function queuesIn(queues, namespace = null) {
  return (queues || []).filter((q) => q?.name && inNamespace(q, namespace))
}

/**
 * The head's totals over a set of queues. A field no queue reported is null
 * ('—'), never 0; no queues at all is 0 of everything.
 */
export function queueTotals(queues) {
  const list = (queues || []).filter((q) => q?.name)
  const sum = (pick) => {
    let s = list.length ? null : 0
    for (const q of list) {
      const v = num(pick(q))
      if (v !== null) s = (s ?? 0) + v
    }
    return s
  }
  return {
    queues: list.length,
    partitions: sum((q) => q.partitions),
    pending: sum((q) => q.messages?.pending),
    processing: sum((q) => q.messages?.processing),
    deadLetter: sum((q) => q.messages?.deadLetter),
    retainedBytes: sum((q) => q.retainedBytes ?? q.messages?.retainedBytes),
  }
}

/**
 * queue-ops `series` (one row per queue and bucket) → one row per bucket,
 * oldest first, over the queues in `names` (a Set; null: every row, the
 * tenant's whole traffic). Rates and counts are summed; the lag is the
 * pop-weighted mean, from the queues that popped in the bucket only: a queue
 * with no pops has no measurement, and its 0 would report a stalled backlog
 * as no lag.
 */
export function rollupQueueOps(series, names = null) {
  const byBucket = new Map()
  for (const row of series || []) {
    if (!row?.bucket) continue
    if (names && !names.has(row.queueName)) continue
    let b = byBucket.get(row.bucket)
    if (!b) {
      b = {
        bucket: row.bucket,
        pushPerSecond: 0, popPerSecond: 0, ackPerSecond: 0,
        // A Kafka fetch, kept apart only to chart the Kafka share.
        fetchPerSecond: 0, fetchMessages: 0,
        pushMessages: 0, popMessages: 0, ackSuccess: 0, ackFailed: 0, popEmpty: 0,
        partitionsCreated: 0, partitionsDeleted: 0,
        // A long-poll lives on one (queue, partition, worker): summing the
        // queues' gauges is a fair total.
        parkedTotal: 0,
        lagWeighted: 0, lagPops: 0, maxLagMs: null,
      }
      byBucket.set(row.bucket, b)
    }
    b.pushPerSecond += num(row.pushPerSecond) || 0
    b.popPerSecond += num(row.popPerSecond) || 0
    b.ackPerSecond += num(row.ackPerSecond) || 0
    b.fetchPerSecond += num(row.fetchPerSecond) || 0
    b.fetchMessages += num(row.fetchMessages) || 0
    b.pushMessages += num(row.pushMessages) || 0
    b.popMessages += num(row.popMessages) || 0
    b.ackSuccess += num(row.ackSuccess) || 0
    b.ackFailed += num(row.ackFailed) || 0
    b.popEmpty += num(row.popEmpty) || 0
    b.partitionsCreated += num(row.partitionsCreated) || 0
    b.partitionsDeleted += num(row.partitionsDeleted) || 0
    b.parkedTotal += num(row.parkedCount) || 0
    const pops = num(row.popMessages) || 0
    if (pops > 0) {
      b.lagWeighted += (num(row.avgLagMs) || 0) * pops
      b.lagPops += pops
      const mx = num(row.maxLagMs)
      if (mx !== null) b.maxLagMs = b.maxLagMs === null ? mx : Math.max(b.maxLagMs, mx)
    }
  }
  const rows = [...byBucket.values()].sort((a, b) => a.bucket.localeCompare(b.bucket))
  for (const b of rows) b.avgLagMs = b.lagPops > 0 ? b.lagWeighted / b.lagPops : null
  return rows
}

/** The seconds a `timeRange: { from, to }` spans; null when it does not say. */
export function windowSeconds(timeRange) {
  const from = Date.parse(timeRange?.from || '')
  const to = Date.parse(timeRange?.to || '')
  return Number.isFinite(from) && Number.isFinite(to) && to > from ? (to - from) / 1000 : null
}

/**
 * The busiest queues of the window, from queue-ops `series`: the messages
 * pushed and popped, and per second over the window. Only the queues in
 * `names` (a Set; null: every one), only the ones that moved anything,
 * most messages first.
 */
export function busiestQueues(series, names = null, { seconds = null, limit = 8 } = {}) {
  const byQueue = new Map()
  for (const row of series || []) {
    const name = row?.queueName
    if (!name || (names && !names.has(name))) continue
    const q = byQueue.get(name) || { name, pushMessages: 0, popMessages: 0 }
    q.pushMessages += num(row.pushMessages) || 0
    q.popMessages += num(row.popMessages) || 0
    byQueue.set(name, q)
  }
  const s = num(seconds)
  return [...byQueue.values()]
    .filter((q) => q.pushMessages + q.popMessages > 0)
    .map((q) => ({
      ...q,
      pushPerSecond: s ? q.pushMessages / s : null,
      popPerSecond: s ? q.popMessages / s : null,
    }))
    .sort((a, b) => (b.pushMessages + b.popMessages) - (a.pushMessages + a.popMessages) || a.name.localeCompare(b.name))
    .slice(0, limit)
}

/**
 * Where the backlog is: the queues with the most messages waiting, deepest
 * first, at most `limit`, as `[{ name, namespace, pending, lag, sev }]`.
 * `lag` is the oldest wait among the queue's live groups in seconds (an
 * inactive Kafka group's lag only grows, so it is left out; null when no
 * group reports one); `sev` is the queue's mark by the one rule, from
 * `sevByQueue`, or null. A queue with nothing waiting is not listed.
 */
export function deepestQueues(queues, groups, sevByQueue = new Map(), limit = 8, linesFor) {
  const lagOf = new Map()
  for (const g of groups || []) {
    const lag = num(g?.maxTimeLag)
    if (!g?.queueName || lag === null || groupAttention(g, linesFor) === 'mute') continue
    lagOf.set(g.queueName, Math.max(lagOf.get(g.queueName) ?? 0, lag))
  }
  return (queues || [])
    .filter((q) => q?.name)
    .map((q) => ({
      name: q.name,
      namespace: q.namespace || '',
      pending: num(q.messages?.pending) || 0,
      lag: lagOf.has(q.name) ? lagOf.get(q.name) : null,
      sev: sevByQueue?.get?.(q.name) || null,
    }))
    .filter((q) => q.pending > 0)
    .sort((a, b) => b.pending - a.pending || a.name.localeCompare(b.name))
    .slice(0, limit)
}

/**
 * The queues that need you (queueAttention, the app's one rule): failing
 * first, then the longest lag, then the most pending.
 */
export function attentionList(queues, groups, linesFor, ops = null) {
  return queueAttention(queues, groups, linesFor, ops)
    .sort((a, b) => (RANK[b.sev] || 0) - (RANK[a.sev] || 0)
      || (b.lag || 0) - (a.lag || 0)
      || (b.pending || 0) - (a.pending || 0)
      || a.name.localeCompare(b.name))
}

/**
 * The consumer groups on the queues in `names` (a Set; null: every group),
 * the ones behind first and inactive Kafka groups last (byLagInactiveLast),
 * with how many need you by the groups' own rule (groupAttention).
 */
export function groupsSummary(groups, names = null, linesFor) {
  const list = (groups || []).filter((g) => g?.queueName && (!names || names.has(g.queueName)))
  const sevs = list.map((g) => groupAttention(g, linesFor))
  return {
    rows: byLagInactiveLast(list, linesFor),
    total: list.length,
    behind: sevs.filter((s) => s === 'warn' || s === 'bad').length,
    sev: worstMark(sevs),
  }
}

// ---------------------------------------------------------------------------
// Server: the cell's nodes
// ---------------------------------------------------------------------------

const share = (x) => (x === null || x === undefined ? '—' : x > 0 && x < 0.01 ? '<1%' : `${Math.round(x * 100)}%`)
const tone = (sev) => (sev === 'warn' || sev === 'bad' ? sev : '')

/**
 * brokerStatus(...) → the nodes table, one row per node in node-id order.
 * A node nobody reached is bad; else its mark is the worst of its own
 * readings (null when nothing needs you). Shares are of what the node may
 * use; rates are bytes per second over the collector's last interval.
 */
export function nodeRows(status) {
  if (!status || !Array.isArray(status.nodes)) return []
  return status.nodes.map((n) => ({
    name: n.name,
    leader: Boolean(n.isLeader),
    mark: n.unreachable ? 'bad' : worstMark([n.cpu?.sev, n.mem?.sev, n.disk?.sev, n.lagSeverity, n.heartbeatSeverity]),
    role: n.unreachable ? 'Unreachable' : `${capital(n.state || '—')}${n.lag ? ` · ${count(n.lag)} behind` : ''}`,
    roleTone: n.unreachable ? 'bad' : tone(worstMark([n.lagSeverity, n.heartbeatSeverity])),
    cpu: { text: share(n.cpu?.share), tone: tone(n.cpu?.sev) },
    mem: { text: share(n.mem?.share), tone: tone(n.mem?.sev) },
    disk: { text: n.disk?.usedPct === null || n.disk?.usedPct === undefined ? '—' : `${Math.round(n.disk.usedPct)}%`, tone: tone(n.disk?.sev) },
    raft: n.traffic ? `${formatRate(n.traffic.sent)} / ${formatRate(n.traffic.received)}` : '—',
    net: n.traffic?.network ? `${formatRate(n.traffic.network.received)} / ${formatRate(n.traffic.network.sent)}` : '—',
    write: n.traffic?.disk ? formatRate(n.traffic.disk.written) : '—',
  }))
}

/** The collector interval the nodes' rates cover, in seconds; null when none says. */
export function trafficWindow(status) {
  for (const n of status?.nodes || []) {
    const w = num(n.traffic?.windowSeconds)
    if (w) return w
  }
  return null
}

// ---------------------------------------------------------------------------
// Server: per-node charts over time, from GET /api/v1/analytics/system-metrics
// (`replicas[].timeSeries[].metrics`, every leaf `{avg, min, max, last}`), as
// the System page draws its "Per server" view.
//
// Replicas do not share a bucket grid: one may start later, restart or miss a
// tick. The timeline is the UNION of every replica's timestamps, and each line
// is read BY timestamp, so a shorter series is a gap in its line rather than a
// shift onto another replica's clock.
// ---------------------------------------------------------------------------

/** Every timestamp any replica reported, oldest first. */
export function replicaTimeline(replicas) {
  const seen = new Set()
  for (const r of replicas || []) {
    for (const p of r?.timeSeries || []) if (p?.timestamp) seen.add(p.timestamp)
  }
  return [...seen].sort()
}

const leaf = (metrics, path) => {
  let node = metrics
  for (const key of path) {
    node = node?.[key]
    if (node === undefined || node === null) return null
  }
  return node
}

/** One replica's `agg` of `path` on the timeline, scaled; null where it reported none. */
export function replicaSeries(replica, timeline, path, { agg = 'avg', scale = (v) => v } = {}) {
  const byTs = new Map()
  for (const p of replica?.timeSeries || []) if (p?.timestamp) byTs.set(p.timestamp, p)
  return (timeline || []).map((ts) => {
    const v = num(leaf(byTs.get(ts)?.metrics, path)?.[agg])
    return v === null ? null : scale(v)
  })
}

/**
 * One line per node, and per spec of it when there are two (in and out,
 * read and write): `{ timeline, hasData, datasets: [{ label, node, dashed,
 * data }] }`. `node` is the replica's index, for its colour; the second spec
 * of a node is `dashed`. A spec is `{ path, short }`; `suffix` names a lone
 * spec in every label ("queen-1 · user").
 */
export function nodeLines(replicas, specs, { agg = 'avg', scale = (v) => v, suffix = '' } = {}) {
  const list = (replicas || []).filter(Boolean)
  const timeline = replicaTimeline(list)
  const datasets = list.flatMap((r, i) => specs.map((spec, j) => ({
    label: `${r.hostname || `node ${i + 1}`}${specs.length > 1 ? ` · ${spec.short}` : suffix}`,
    node: i,
    dashed: j > 0,
    data: replicaSeries(r, timeline, spec.path, { agg, scale }),
  })))
  return { timeline, datasets, hasData: datasets.some((d) => d.data.some((v) => v !== null)) }
}

/**
 * nodeLines in bytes per second, put in the unit its largest value reads
 * best in (useTraffic), and the unit named in every label.
 */
export function inRateUnit(lines) {
  let max = 0
  for (const d of lines.datasets) for (const v of d.data) if (v !== null && v > max) max = v
  const unit = rateUnit(max)
  return {
    ...lines,
    unit,
    datasets: lines.datasets.map((d) => ({ ...d, label: `${d.label} (${unit.label})`, data: d.data.map((v) => inUnit(v, unit)) })),
  }
}
