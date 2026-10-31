// The Consumer groups view of the Queues section, as data: every row the
// broker lists (one group on one queue), the namespace it sits under, whether
// it needs you, how fast what waits for it drains, the order a header click
// froze, the line on top, and the calls its actions send.
//
// What needs you is the app's one rule (useAttention): a group past its
// queue's lag lines (groupAttention), or a reader of a queue whose acks fail
// (ackAttention, the half of queueAttention a group can own). So a queue the
// Queues list flags for its lag or its acks has its groups flagged here, and
// a queue flagged because nobody reads it has no group to flag.
//
// Pure: no Vue, no '@/' imports, so test/queues-groups.test.js holds it
// against stage-shaped answers. Numbers stay numbers here; the view words
// them with the console's helpers (fmt, span).
import { ackAttention, groupAttention, groupPresence, groupState } from '../../composables/useAttention.js'
import { groupActivity, SEV_BAD, SEV_WARN } from '../../composables/useSeverity.js'
import { isConflating } from '../../composables/useConflation.js'
import { nextSort, perSecond, QUEUE_MODE, toneOf } from './queuesView.js'
import { LAG_THRESHOLDS } from '../../../../shared/timeRanges.mjs'
import { acksText } from '../../composables/queueOps.js'
import { span } from '../words.js'

export { nextSort, QUEUE_MODE }

/** The ages the partitions-behind panel offers (the old page's choices), in seconds; the first is the default. */
export const AGES = LAG_THRESHOLDS.map((t) => t.seconds)

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const RANK = { [SEV_BAD]: 2, [SEV_WARN]: 1 }
const linesOf = (linesFor, queue) => (typeof linesFor === 'function' ? linesFor(queue) : undefined)

/** One row's identity: a group name is caller text, so NUL keeps `a` on `b·c` apart from `a·b` on `c`. */
export const rowKey = (name, queue) => `${queue ?? ''}\u0000${name ?? ''}`

/** The name a person reads, as the queue page writes it: the groupless pop is "queue mode". */
export const groupLabel = (name) => (name === QUEUE_MODE ? 'queue mode' : String(name ?? ''))

// ---- how fast what waits drains ---------------------------------------------

/** The window a drain is judged over (the Queues list's five minutes), and the least of it worth judging. */
export const TREND_WINDOW_MS = 5 * 60_000
export const TREND_MIN_MS = 50_000
/** Within 5% of its pace a flow is level: it neither clears nor grows. */
const LEVEL_SHARE = 0.05

/**
 * From the queue's own minutes (arrived, read), as the Queues list and the
 * Overview judge a queue: `{ kind, eta?, from }`, kind one of
 *   'caught'    nothing waits
 *   'clearing'  it reads faster than messages arrive; `eta` seconds to empty
 *   'growing'   messages arrive faster than it reads
 *   'level'     within 5% of each other: what waits stays
 *   'stalled'   nothing arrives and nothing is read, yet something waits
 *   'unknown'   no minutes to judge by
 */
export function rateTrend(waiting, inS, outS) {
  const w = num(waiting)
  if (w === null) return { kind: 'unknown' }
  if (w <= 0) return { kind: 'caught' }
  if (inS == null || outS == null) return { kind: 'unknown' }
  const pace = Math.max(inS, outS)
  if (!(pace > 0)) return { kind: 'stalled', from: 'rates' }
  const net = outS - inS
  if (net > pace * LEVEL_SHARE) return { kind: 'clearing', eta: w / net, from: 'rates' }
  if (net < -pace * LEVEL_SHARE) return { kind: 'growing', from: 'rates' }
  return { kind: 'level', from: 'rates' }
}

/**
 * From this page's own reads of the group's waiting count, `[[ms, waiting]]`
 * oldest first: how it moved over the last five minutes, judged once two
 * reads are at least 50 s apart. The same kinds as rateTrend; a change of
 * one message, or of 2% of what waits, is level.
 */
export function sampleTrend(list, now = Date.now()) {
  const pts = Array.isArray(list) ? list : []
  if (pts.length < 2) return { kind: 'unknown' }
  const [t1, w1] = pts[pts.length - 1]
  // A last read older than three minutes says nothing of now.
  if (now - t1 > 3 * 60_000) return { kind: 'unknown' }
  if (w1 <= 0) return { kind: 'caught' }
  const first = pts.find(([t]) => t >= t1 - TREND_WINDOW_MS && t1 - t >= TREND_MIN_MS)
  if (!first) return { kind: 'unknown' }
  const [t0, w0] = first
  const shrank = w0 - w1
  const band = Math.max(1, 0.02 * Math.max(w0, w1))
  if (shrank > band) return { kind: 'clearing', eta: w1 / (shrank / ((t1 - t0) / 1000)), from: 'samples' }
  if (shrank < -band) return { kind: 'growing', from: 'samples' }
  return { kind: 'level', from: 'samples' }
}

/**
 * Keep the listing's waiting counts, read at `at`, in `samples` (a Map,
 * rowKey → [[ms, waiting]], oldest first): six minutes of them. A row the
 * listing no longer has is forgotten. Returns the same Map.
 */
export function addSamples(samples, groups, at) {
  if (!samples || !Array.isArray(groups) || !Number.isFinite(at) || at <= 0) return samples
  const keep = at - TREND_WINDOW_MS - 60_000
  const seen = new Set()
  for (const g of groups) {
    const w = num(g?.totalLag)
    if (!g?.queueName || g.name == null || w === null) continue
    const k = rowKey(g.name, g.queueName)
    seen.add(k)
    const list = samples.get(k) || []
    if (!list.length || list[list.length - 1][0] < at) list.push([at, w])
    while (list.length && list[0][0] < keep) list.shift()
    samples.set(k, list)
  }
  for (const k of [...samples.keys()]) if (!seen.has(k)) samples.delete(k)
  return samples
}

// ---- the rows ----------------------------------------------------------------

/**
 * Every row of the listing, with what the view says about it:
 *   groups   GET /api/v1/consumer-groups      (groupsStore.groups; null = not read)
 *   queues   GET /api/v1/resources/queues     (queuesStore.queues): each queue's namespace
 *   ops      the hour's totals by queue        (queueOpsStore.ops): its acks
 *   series   the hour's minutes by queue       (queueOpsStore.series): its pace
 *   samples  this page's reads of each row's waiting count (addSamples)
 *
 * The pace of a queue is every group's together (a pop delivers to one group),
 * so a drain is judged from the queue's minutes only for its one reader. A
 * conflating group retires a whole partition with one delivery and a Kafka
 * group fetches instead of popping, so theirs, like a shared queue's, is
 * judged from their own waiting count over this page's reads.
 */
export function groupRows({ groups = null, queues = [], ops = null, series = null, samples = null, linesFor, now = Date.now() } = {}) {
  if (!Array.isArray(groups)) return []
  const meta = new Map()
  for (const q of queues || []) if (q?.name) meta.set(q.name, q)
  const onQueue = new Map()
  for (const g of groups) {
    if (!g?.queueName) continue
    if (!onQueue.has(g.queueName)) onQueue.set(g.queueName, [])
    onQueue.get(g.queueName).push(g)
  }
  const out = []
  for (const g of groups) {
    if (!g?.queueName || g.name == null) continue
    const q = g.queueName
    const lines = linesOf(linesFor, q)
    const sev = groupAttention(g, linesFor)
    const live = sev !== 'mute'
    const all = onQueue.get(q) || []
    const liveReaders = all.filter((x) => groupAttention(x, linesFor) !== 'mute').length
    let need = sev === SEV_BAD || sev === SEV_WARN ? { sev, reason: 'lag' } : null
    const { sev: ackSev, hour } = ackAttention(q, linesFor, ops)
    if (live && (RANK[ackSev] || 0) > (RANK[need?.sev] || 0)) need = { sev: ackSev, reason: 'acks', shared: liveReaders > 1, hour }
    const waiting = num(g.totalLag)
    const conflates = isConflating(g)
    const kafka = g.kind === 'kafka'
    const minutes = typeof series?.get === 'function' ? series.get(q) || null : null
    let trend
    if (!live) trend = { kind: 'off' }
    else if (all.length === 1 && !conflates && !kafka && minutes) trend = rateTrend(waiting, perSecond(minutes.push), perSecond(minutes.pop))
    else if (waiting !== null && waiting <= 0) trend = { kind: 'caught' }
    else trend = sampleTrend(samples?.get?.(rowKey(g.name, q)), now)
    const m = meta.get(q)
    out.push({
      key: rowKey(g.name, q),
      name: g.name,
      label: groupLabel(g.name),
      queueMode: g.name === QUEUE_MODE,
      queue: q,
      ns: typeof m?.namespace === 'string' ? m.namespace : '',
      kafka,
      conflates,
      sev,
      state: groupState(g, linesFor),
      need,
      tone: toneOf(need?.sev),
      waiting,
      lag: num(g.maxTimeLag),
      behind: num(g.partitionsWithLag),
      members: num(g.partitionCursors ?? g.members),
      // A seek moves the group on every partition of the queue, cursor or not.
      partitions: num(m?.partitions),
      trend,
      readers: all.length,
      lines: lines ? { warn: num(lines.lagWarnSeconds), bad: num(lines.lagBadSeconds), grace: num(lines.inactiveAfterSeconds) } : null,
      sub: { mode: g.subscriptionMode ?? null, at: g.subscriptionTimestamp ?? null, since: g.subscriptionCreatedAt ?? null },
      presence: kafka
        ? {
          activity: groupActivity(g, lines),
          members: num(g.liveMembers),
          lastSeenAt: g.lastSeenAt || null,
          away: num(g.inactiveSeconds),
          said: groupPresence(g, linesFor),
        }
        : null,
    })
  }
  return out
}

/**
 * What a row's state cell says, as a kind the view words:
 *   failing   its queue's acks fail (tone)
 *   never     a native group that has never consumed (the broker's Dead)
 *   off       a Kafka group gone past its grace: not running
 *   empty     a Kafka group with no live member, inside the grace, lag fine
 *   caught    nothing waits
 *   clearing  it drains: `eta` seconds at this pace
 *   growing   it does not drain (tone when it needs you)
 *   steady    something waits, within its lines, and the pace keeps it there
 *   behind    past its lines, and how it moves is not known yet (tone)
 */
export function statusOf(row) {
  if (!row) return { kind: 'steady', tone: '' }
  if (row.need?.reason === 'acks') return { kind: 'failing', tone: row.tone }
  if (row.state === 'mute') return { kind: 'never', tone: '' }
  if (row.state === 'inactive') return { kind: 'off', tone: '' }
  if (row.state === 'empty') return { kind: 'empty', tone: '' }
  const flagged = !!row.need
  const tone = flagged ? row.tone : ''
  switch (row.trend?.kind) {
    case 'caught': return { kind: 'caught', tone: '' }
    case 'clearing': return { kind: 'clearing', eta: row.trend.eta, tone: '' }
    case 'growing':
    case 'stalled': return { kind: 'growing', tone }
    case 'level': return flagged ? { kind: 'growing', tone } : { kind: 'steady', tone: '' }
    default: return flagged ? { kind: 'behind', tone } : { kind: 'steady', tone: '' }
  }
}

// The words of a state (statusOf). A group that keeps up says nothing: its
// dash says nothing waits, as on the Queues list; words are for what is worth
// a look. Shared by the Consumer groups view and a queue's Groups tab.
export const STATE_WORDS = Object.freeze({
  failing: 'Failing', never: 'Never consumed', off: 'Not running', empty: 'No members',
  caught: '', growing: 'Not draining', steady: '', behind: 'Behind',
})
export const QUIET_STATES = new Set(['never', 'off', 'empty'])
export function stateWords(st) {
  if (st?.kind === 'clearing') return st.eta < 60 ? 'Clears in under a minute' : `Clears in about ${span(st.eta)}`
  return STATE_WORDS[st?.kind] || ''
}
const sentence = (s) => s.charAt(0).toUpperCase() + s.slice(1)
export function stateTitle(r, st) {
  if (st.kind === 'failing') return `${sentence(acksText(r.need.hour))} on ${r.queue} in the last hour.`
  if (st.kind === 'off' || st.kind === 'empty') {
    const away = r.presence?.away
    return away != null ? `Last seen ${span(away)} ago.` : 'No member data to date it by.'
  }
  if (st.kind === 'never') return 'It has a cursor here, but it has never consumed.'
  const from = r.trend?.from
  if (from === 'rates') return `At the pace of ${r.queue} over the last five minutes, which only this group reads.`
  if (from === 'samples') return 'From how its waiting count moved over this view’s last reads.'
  if (st.kind === 'behind') return 'How it moves is known after a minute of reads.'
  return ''
}

// ---- the list: segments, filter, namespaces, order -----------------------------

export const SEGMENTS = [
  { value: 'all', label: 'All' },
  { value: 'needs', label: 'Needs you' },
]
export const SEGMENT_VALUES = SEGMENTS.map((s) => s.value)
export const inSegment = (row, seg) => (seg === 'needs' ? !!row.need : true)
export const segmentCounts = (rows = []) => ({ all: rows.length, needs: rows.filter((r) => r.need).length })

/** The filter box: a group's name ("queue mode" finds the groupless pop), its queue or its namespace. */
export function matches(row, term) {
  const t = String(term ?? '').trim().toLowerCase()
  if (!t) return true
  return [row.label, row.name, row.queue, row.ns].some((v) => typeof v === 'string' && v.toLowerCase().includes(t))
}

const byName = (a, b) => a.label.localeCompare(b.label) || a.queue.localeCompare(b.queue)

/** The columns a header click sorts by. */
export const SORTS = {
  oldest: { label: 'Oldest unread', get: (r) => r.lag },
}

/**
 * The order of a sort as it stood at the click: rowKey → rank. The view keeps
 * it until the next click, so a refresh updates numbers in place and never
 * moves a row. Unknown values go last either way.
 */
export function snapshotOrder(rows = [], { key, dir } = {}) {
  const get = SORTS[key]?.get
  if (!get) return null
  const sign = dir === 'asc' ? 1 : -1
  const list = [...rows].sort((a, b) => {
    const va = num(get(a)); const vb = num(get(b))
    if (va == null && vb == null) return byName(a, b)
    if (va == null) return 1
    if (vb == null) return -1
    return sign * (va - vb) || byName(a, b)
  })
  return new Map(list.map((r, i) => [r.key, i]))
}

const byRank = (rank) => (a, b) => {
  if (rank) {
    const ra = rank.has(a.key) ? rank.get(a.key) : Infinity
    const rb = rank.has(b.key) ? rank.get(b.key) : Infinity
    if (ra !== rb) return ra - rb
  }
  return byName(a, b)
}

/**
 * The rows by their queue's namespace, as the Queues list draws its queues:
 * namespaces by name, the default one ('') last; inside each, the frozen
 * order (`rank`) or group then queue. While a filter or a segment narrows
 * them, a namespace with nothing left is not shown.
 *   [{ ns, rows, total, counts: { failing, behind }, flagged }]
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
    const shown = mine.filter((r) => inSegment(r, seg) && matches(r, find)).sort(byRank(rank))
    if (narrowed && !shown.length) continue
    const counts = { failing: 0, behind: 0 }
    for (const r of shown) {
      if (r.need?.reason === 'acks') counts.failing++
      else if (r.need) counts.behind++
    }
    out.push({ ns, rows: shown, total: mine.length, counts, flagged: shown.some((r) => r.need) })
  }
  return out
}

/** Whether a namespace opens by default: it holds a group that needs you. */
export const hasNews = (ns) => ns.rows.some((r) => r.need)

// ---- the line on top -------------------------------------------------------------

const worstFirst = (a, b) => (RANK[b.need?.sev] || 0) - (RANK[a.need?.sev] || 0)
  || (a.need?.reason === 'lag' ? 0 : 1) - (b.need?.reason === 'lag' ? 0 : 1)
  || (b.lag || 0) - (a.lag || 0)
  || byName(a, b)

/**
 * The state of every group, for one sentence:
 *   { kind: 'loading' }                          nothing read yet
 *   { kind: 'failed' }                           the listing did not answer, and none is held
 *   { kind: 'none' }                             no group has read a queue
 *   { kind: 'fine' }                             every group keeps up
 *   { kind: 'issues', tone, names, more, count, only }
 *     names: the first two groups that need you, worst first ({ name, label, queue, queues })
 * A group flagged on several queues is one name, with how many.
 */
export function groupsVerdict({ rows = [], loaded = false, failed = false } = {}) {
  if (!loaded) return { kind: failed ? 'failed' : 'loading' }
  if (!rows.length) return { kind: 'none' }
  const flagged = rows.filter((r) => r.need).sort(worstFirst)
  if (!flagged.length) return { kind: 'fine' }
  const seen = new Map()
  for (const r of flagged) {
    if (!seen.has(r.name)) seen.set(r.name, { name: r.name, label: r.label, mono: !r.queueMode, queue: r.queue, queues: 0 })
    seen.get(r.name).queues++
  }
  const names = [...seen.values()]
  return {
    kind: 'issues',
    tone: toneOf(flagged[0].need.sev),
    names: names.slice(0, 2),
    more: Math.max(0, names.length - 2),
    count: names.length,
    only: rows.every((r) => r.need),
  }
}

// ---- the pick ----------------------------------------------------------------------

/** The rows of a group, worst first: its first is where the view opens it without a queue. */
export const rowsOf = (rows, name) => rows.filter((r) => r.name === name).sort(worstFirst)

/** The picked row: the group on the queue asked for, else the group where it is worst. */
export function pickedRow(rows = [], group = '', queue = '') {
  if (!group) return null
  const mine = rowsOf(rows, group)
  return mine.find((r) => r.queue === queue) || mine[0] || null
}

// ---- partitions behind (GET /api/v1/consumer-groups/lagging) -----------------------

/** The lagging route's own cap: the broker answers at most this many partitions, the furthest behind. */
export const LAGGING_LIMIT = 1000

/**
 * The partitions of the lagging answer (snake_case, oldest first) behind more
 * than `min` seconds, one group's on one queue with `group` and `queue`:
 *   [{ key, group, label, queueMode, queue, partition, worker, waiting, lag, oldestAt, kafka }]
 */
export function behindRows(data, { min = 0, group = null, queue = null } = {}) {
  const out = []
  for (const p of Array.isArray(data) ? data : []) {
    if (!p?.partition_name || !p.queue_name || p.consumer_group == null) continue
    const lag = num(p.time_lag_seconds)
    if (lag === null || lag <= min) continue
    if (group != null && p.consumer_group !== group) continue
    if (queue != null && p.queue_name !== queue) continue
    out.push({
      key: `${p.queue_name}\u0000${p.consumer_group}\u0000${p.partition_name}`,
      group: p.consumer_group,
      label: groupLabel(p.consumer_group),
      queueMode: p.consumer_group === QUEUE_MODE,
      queue: p.queue_name,
      partition: p.partition_name,
      worker: p.worker_id || null,
      waiting: num(p.offset_lag),
      lag,
      oldestAt: p.oldest_unconsumed_at || null,
      kafka: p.kind === 'kafka',
    })
  }
  return out
}

/**
 * Rows in the place they were first shown. A new read updates the numbers of
 * those still there, adds new ones at the end, and keeps one that left as
 * `gone` (caught up since), so nothing moves under a hand about to click.
 *   prev  { order: [key], rows: Map } | null (a fresh table: the read's own order)
 */
export function keepPlaces(prev, fresh = []) {
  const order = prev ? [...prev.order] : []
  const rows = new Map()
  const now = new Map(fresh.map((r) => [r.key, r]))
  for (const k of order) {
    const r = now.get(k)
    rows.set(k, r ? { ...r, gone: false } : { ...prev.rows.get(k), gone: true })
  }
  for (const r of fresh) {
    if (rows.has(r.key)) continue
    order.push(r.key)
    rows.set(r.key, { ...r, gone: false })
  }
  return { order, rows, list: order.map((k) => rows.get(k)) }
}

// ---- actions: the calls, sent through the app's API layer ---------------------------

const enc = encodeURIComponent
const BASE = '/api/v1/consumer-groups'

/**
 * Move a group's cursor on every partition of one queue: to the newest
 * message (`at` null: what waits is skipped), or to just before the first
 * message pushed at or after `at` (ms): from there on it is delivered again.
 */
export function seekCall(group, queue, at = null) {
  const body = at == null ? { toEnd: true } : { timestamp: new Date(at).toISOString() }
  const path = `${BASE}/${enc(group)}/queues/${enc(queue)}/seek`
  return { kind: 'seek', method: 'POST', path, body, group, queue, text: `POST ${path} ${JSON.stringify(body)}` }
}

/** Skip what waits in one partition: its cursor to the partition's newest message. */
export function partitionCall(group, queue, partition) {
  const body = { toEnd: true }
  const path = `${BASE}/${enc(group)}/queues/${enc(queue)}/partitions/${enc(partition)}/seek`
  return { kind: 'seekPartition', method: 'POST', path, body, group, queue, partition, text: `POST ${path} ${JSON.stringify(body)}` }
}

/**
 * Delete a group's cursors on one queue (`queue`), or on every queue (null);
 * `deleteMetadata` also deletes its subscription there.
 */
export function deleteCall(group, queue = null, deleteMetadata = true) {
  const meta = deleteMetadata !== false
  const path = queue ? `${BASE}/${enc(group)}/queues/${enc(queue)}` : `${BASE}/${enc(group)}`
  return {
    kind: queue ? 'deleteForQueue' : 'delete',
    method: 'DELETE',
    path,
    params: { deleteMetadata: meta },
    group,
    queue: queue || null,
    deleteMetadata: meta,
    text: `DELETE ${path}?deleteMetadata=${meta}`,
  }
}

/**
 * Send a call through `api` (`consumers` of '@/api'), whose functions build
 * the same method, path and body from the same parts: what runs is what the
 * dialog showed.
 */
export function sendCall(api, call, config = { quiet: true }) {
  switch (call?.kind) {
    case 'seek': return api.seek(call.group, call.queue, call.body, config)
    case 'seekPartition': return api.seekPartition(call.group, call.queue, call.partition, config)
    case 'deleteForQueue': return api.deleteForQueue(call.group, call.queue, call.deleteMetadata, config)
    case 'delete': return api.delete(call.group, call.deleteMetadata, config)
    default: return Promise.reject(new Error(`No such call: ${call?.kind}`))
  }
}

/**
 * What a seek's answer says. A 2xx that moved no partition did nothing, and
 * saying "done" there would be a lie (the old page's rule).
 *   { ok, moved, problem }
 */
export function seekOutcome(data) {
  if (!data || typeof data !== 'object') return { ok: false, moved: null, problem: 'The broker returned no result.' }
  const moved = num(data.partitionsUpdated)
  if (data.success === false) return { ok: false, moved, problem: data.error || 'The broker refused the seek.' }
  if (moved === 0) return { ok: false, moved, problem: 'No partition matched, so the cursor did not move.' }
  return { ok: true, moved, problem: null }
}

/** What a delete's answer says: `{ ok, deleted, metadata, problem }`; zero cursors deleted is nothing done. */
export function deleteOutcome(data) {
  if (!data || typeof data !== 'object') return { ok: false, deleted: null, metadata: false, problem: 'The broker returned no result.' }
  const deleted = num(data.deletedPartitions)
  const metadata = data.metadataDeleted === true
  if (data.success === false) return { ok: false, deleted, metadata, problem: data.error || 'The broker refused the delete.' }
  if (deleted === 0) return { ok: false, deleted, metadata, problem: 'Nothing was deleted: no cursor of this group matched.' }
  return { ok: true, deleted, metadata, problem: null }
}

/**
 * Where a group starts again after its cursors are deleted, if its workers pop
 * once more (server: queue mode always seeds from the start; a group with its
 * subscription kept seeds from it; one without registers afresh from what its
 * workers ask, `new` unless the broker's DEFAULT_SUBSCRIPTION_MODE says all):
 *   { kind: 'queueMode' }                     every message the queue keeps, again
 *   { kind: 'fresh' }                         a new group: by default only new messages
 *   { kind: 'all' }                           its kept subscription: from the start
 *   { kind: 'new', since }                    from when it first subscribed
 *   { kind: 'timestamp', since }              from its subscription's time
 */
export function restartOf(row, keepSubscription) {
  if (!row || row.queueMode) return { kind: 'queueMode' }
  if (!keepSubscription || !row.sub?.mode) return { kind: 'fresh' }
  if (row.sub.mode === 'all') return { kind: 'all' }
  if (row.sub.mode === 'timestamp') return { kind: 'timestamp', since: Date.parse(row.sub.at) || null }
  return { kind: 'new', since: Date.parse(row.sub.since || row.sub.at) || null }
}

// ---- a time typed for a seek ----------------------------------------------------------

const two = (n) => String(n).padStart(2, '0')

/** "2026-10-31 09:30" in local time: how the seek's field shows a time. */
export function whenText(ms) {
  const d = new Date(ms)
  return `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())} ${two(d.getHours())}:${two(d.getMinutes())}`
}

/**
 * A time typed in the seek's field, local: "2026-10-31 09:30" (a T works for
 * the space, seconds may follow), or "09:30" for today. null when it is not one.
 */
export function parseWhen(text, now = Date.now()) {
  const s = String(text ?? '').trim()
  let m = /^(\d{4})-(\d{1,2})-(\d{1,2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(s)
  if (m) {
    const [y, mo, d, h, mi, se] = [m[1], m[2], m[3], m[4], m[5], m[6] || '0'].map(Number)
    if (mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 || se > 59) return null
    const t = new Date(y, mo - 1, d, h, mi, se)
    // 2026-02-31 rolls into March: not a date anyone meant.
    if (t.getMonth() !== mo - 1 || t.getDate() !== d) return null
    return t.getTime()
  }
  m = /^(\d{1,2})[:.](\d{2})$/.exec(s)
  if (m) {
    const h = Number(m[1]); const mi = Number(m[2])
    if (h > 23 || mi > 59) return null
    const t = new Date(now)
    t.setHours(h, mi, 0, 0)
    return t.getTime()
  }
  return null
}
