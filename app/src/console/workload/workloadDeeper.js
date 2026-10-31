// The deeper layer of the Workload page: not "who is busy" but "is this
// normal", "what does one push cost", "where do messages go", "where are the
// bytes". Read when someone asks for it, never on the 30 s tick: the 7-day
// read is the heaviest the page makes. Pure, like workloadViews.js.
//
//   GET /api/v1/analytics/workload            7 days by namespace (or task), hourly buckets
//   GET /api/v1/analytics/workload            the last 24 hours by queue
//   GET /api/v1/analytics/retention           the window, by queue
//   GET /api/v1/analytics/partition-liveness  partitions, and how many were written to
//   GET /api/v1/analytics/dlq-signatures      why one queue's dead letters failed
//
// The rules are the old page's (useWorkload sameHourBaseline, efficiency,
// deeperFindings), said in the console's words. Hours here are UTC: the
// buckets are, and a week that crosses a clock change would smear a daily job
// over two local hours.

import { efficiency, sameHourBaseline, trimOpenBucket, weeklyProfile } from '../../composables/useWorkload.js'
import { bytes, emptyName, fmt, pct, plural } from './workloadViews.js'

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const pad = (n) => String(n).padStart(2, '0')
const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
const DOW_LONG = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
const MONDAY_FIRST = [1, 2, 3, 4, 5, 6, 0]
const all = () => true

/** The width of a bucket in words, for "in 30 of 96 quarter hours". */
export function bucketWords(minutes) {
  const m = num(minutes) || 0
  if (m === 15) return 'quarter hours'
  if (m === 60) return 'hours'
  if (m === 1) return 'minutes'
  return `${m}-minute spans`
}

// ---- the week -----------------------------------------------------------------------

/**
 * Deliveries by day of the week and hour of the day, UTC, for the tenant
 * (key null) or one group. The read's open bucket is left out, so each cell
 * holds one hour. Darker is more, as on the activity map.
 *
 * @returns {{ has: boolean, rows: Array<{ day, cells }>, days: number, stats: string }}
 */
export function weekModel(payload7d, key = null) {
  const wk = weeklyProfile(trimOpenBucket(payload7d))
  const m = key === null ? wk.tenant : (wk.byKey.get(key) || null)
  if (!payload7d || !m) return { has: false, rows: [], days: wk.days.length, stats: '' }
  let max = 0
  for (const day of m) for (const v of day) if (v !== null && v > max) max = v
  const rows = MONDAY_FIRST.map((d) => ({
    day: DOW[d],
    cells: m[d].map((v, h) => (v === null
      ? { na: true, v: null, p: 0, h }
      : { na: false, v, p: max ? Math.round(Math.sqrt(v / max) * 100) : 0, h })),
  }))
  const flat = m.flat().filter((v) => v !== null)
  let stats = ''
  if (!flat.length) stats = 'No hour of the last 7 days was recorded.'
  else if (!max) stats = 'Nothing was delivered in the last 7 days.'
  else {
    const avg = flat.reduce((a, b) => a + b, 0) / flat.length
    let bd = 0
    let bh = 0
    m.forEach((row, d) => row.forEach((v, h) => { if (v === max) { bd = d; bh = h } }))
    const times = avg ? Math.round((max / avg) * 10) / 10 : null
    stats = `${fmt(avg)} deliveries an hour on average. The busiest hour was ${DOW_LONG[bd]} at ${pad(bh)}:00 UTC, with ${fmt(max)}`
      + (times ? `, ${times} times the average.` : '.')
  }
  return { has: true, rows, days: wk.days.length, stats }
}

// ---- is this hour normal ---------------------------------------------------------------

/**
 * The last complete hour against the same hour on the earlier days of the
 * week (useWorkload sameHourBaseline), the tenant first. A group with nothing
 * then and nothing now is left out: 0 against 0 says nothing. Amber at twice
 * or half its usual, the old page's line.
 *
 * @returns {{ label: string, rows: Array<object>, days: number }}
 */
export function baselineModel(payload7d, { listBy = 'namespace' } = {}) {
  const b = sameHourBaseline(payload7d)
  if (!b.rows.length) return { label: '', rows: [], days: 0 }
  const kept = b.rows.filter((r) => r.key === '*' || (num(r.current) || 0) > 0 || (num(r.mean) || 0) > 0)
  const top = Math.max(2.6, ...kept.map((r) => num(r.ratio) || 0)) * 1.1
  return {
    label: b.label,
    days: b.rows[0]?.days || 0,
    top,
    rows: kept.map((r) => {
      const ratio = num(r.ratio)
      const odd = ratio !== null && (ratio >= 2 || ratio <= 0.5)
      return {
        key: r.key,
        tenant: r.key === '*',
        name: r.key === '*' ? 'Every queue' : (r.key || emptyName(listBy)),
        mono: r.key !== '*' && !!r.key,
        current: fmt(r.current),
        usual: r.mean === null ? '–' : fmt(r.mean),
        min: fmt(r.min),
        max: fmt(r.max),
        ratio: ratio === null ? { text: r.mean === null ? 'no earlier day' : '–', dim: true } : { text: `${ratio}×`, tone: odd ? 'amber' : '' },
        bar: ratio === null ? null : Math.min(1, ratio / top),
        days: r.days,
      }
    }),
  }
}

// ---- what one push costs ------------------------------------------------------------------

/**
 * The queues read the most over the last 24 hours, as the old page's four
 * cost charts in one table: reads per pushed message, acks per delivery,
 * messages per request, and the share of busy buckets a message waited a
 * while for its pop. `keep` narrows them to the queues of the pick. Amber
 * where a push costs more than it should: more reads than its groups account
 * for (redeliveries), and a minute's wait that keeps happening. Whether acks
 * fail is the rows' Ack ok, by the console's one ack rule, so it is not
 * coloured twice here.
 *
 * @returns {{ rows: Array<object>, eff: Array<object>, bucketMinutes: number }}
 */
export function costModel(payload24h, groupsByQueue, keep = all, limit = 12) {
  const eff = efficiency(payload24h, groupsByQueue).filter((e) => keep(e.queue))
  const read = eff.filter((e) => e.pop + e.fetch > 0).slice(0, limit)
  return {
    eff,
    bucketMinutes: num(payload24h?.bucketMinutes) || 0,
    rows: read.map((e) => {
      const b = e.lagBudget || { buckets: 0, under10s: 0, from10to60: 0, over60s: 0 }
      const share = (k) => (b.buckets ? b[k] / b.buckets : 0)
      const chronic = b.buckets >= 20 && share('over60s') >= 0.25
      const inTrx = e.ack === 0 && e.trx > 0
      return {
        key: e.queue,
        fanout: e.fanout === null
          ? { text: e.trx > 0 ? 'fed by transactions' : '–', dim: true, title: e.trx > 0 ? 'Pushed inside transactions, which the counters do not attribute to a queue' : 'Nothing pushed' }
          : { text: `${e.fanout}×`, tone: e.groups > 0 && e.fanout > e.groups * 1.25 ? 'amber' : '', sub: plural(e.groups, 'group'), title: `${fmt(e.pop + e.fetch)} reads for ${fmt(e.push)} pushes` },
        acks: e.ackPerDelivery === null
          ? { text: e.pop === 0 && e.fetch > 0 ? 'Kafka, no acks' : '–', dim: true }
          : { text: pct(Math.min(1, e.ackPerDelivery)), sub: inTrx ? 'acked in transactions' : '', title: `${fmt(e.ack)} acked of ${fmt(e.pop)} delivered` },
        batch: {
          push: e.pushBatch === null ? '–' : String(e.pushBatch),
          ack: e.ackBatch === null ? '–' : String(e.ackBatch),
          title: `${fmt(e.push)} pushed in ${plural(e.pushReq, 'request')}; ${fmt(e.ack + e.ackf)} acks in ${plural(e.ackReq, 'request')}`,
        },
        budget: b.buckets
          ? {
            under: share('under10s'), mid: share('from10to60'), over: share('over60s'),
            text: b.over60s ? `${pct(share('over60s'))} over a minute` : b.from10to60 ? `${pct(share('from10to60'))} over 10 s` : 'all under 10 s',
            tone: chronic ? 'amber' : '',
            title: `Of ${fmt(b.buckets)} buckets with deliveries: ${fmt(b.under10s)} under 10 s, ${fmt(b.from10to60)} from 10 s to a minute, ${fmt(b.over60s)} over a minute`,
          }
          : null,
      }
    }),
  }
}

// ---- where messages go -----------------------------------------------------------------------

/**
 * What retention and eviction removed in the window, by queue, the most first.
 * Evicted is loss by policy (older than the queue's max wait, never
 * delivered), the only amber here. An older broker ignores groupBy and answers
 * the totals alone: that is "cannot split", not "nothing happened".
 *
 * @returns {{ ungrouped: boolean, rows: Array<object> }}
 */
export function retentionModel(ret, keep = all, limit = 10) {
  if (!ret) return { ungrouped: false, rows: [] }
  if (!Array.isArray(ret.rows)) return { ungrouped: true, rows: [] }
  const rows = ret.rows
    .map((r) => ({
      key: r.key || '',
      age: num(r.totals?.retentionMsgs) || 0,
      done: num(r.totals?.completedRetentionMsgs) || 0,
      evicted: num(r.totals?.evictionMsgs) || 0,
    }))
    .filter((r) => r.key && keep(r.key) && r.age + r.done + r.evicted > 0)
    .sort((a, b) => (b.age + b.done + b.evicted) - (a.age + a.done + a.evicted))
    .slice(0, limit)
  const top = Math.max(1, ...rows.map((r) => r.age + r.done + r.evicted))
  return {
    ungrouped: false,
    rows: rows.map((r) => ({
      ...r,
      ageText: fmt(r.age),
      doneText: fmt(r.done),
      evictedText: fmt(r.evicted),
      tone: r.evicted > 0 ? 'amber' : '',
      bar: { age: r.age / top, done: r.done / top, evicted: r.evicted / top },
    })),
  }
}

/**
 * The queues with the most partitions, and how many of them were written to
 * in the last 24 hours. Deletions are the 24 h workload read's own counter:
 * the liveness route reports creations and deliberately no deletions.
 */
export function livenessModel(live, eff = [], keep = all, limit = 10) {
  const list = Array.isArray(live?.rows) ? live.rows : []
  const churn = new Map((eff || []).map((e) => [e.queue, e]))
  return list
    .filter((r) => r?.queue && keep(r.queue))
    .sort((a, b) => (num(b.partitions) || 0) - (num(a.partitions) || 0))
    .slice(0, limit)
    .map((r) => {
      const total = num(r.partitions) || 0
      const c = churn.get(r.queue) || null
      return {
        key: r.queue,
        partitions: fmt(total),
        live: total ? (num(r.live24h) || 0) / total : 0,
        liveText: total ? pct((num(r.live24h) || 0) / total) : '–',
        written: fmt(r.live24h),
        // Created and deleted from one counter, the 24 h read's: the liveness
        // route counts only the partitions created that are still there.
        created: fmt(c && num(c.partitionsCreated) !== null ? c.partitionsCreated : r.created24h),
        deleted: c && num(c.partitionsDeleted) !== null ? fmt(c.partitionsDeleted) : '–',
      }
    })
}

/**
 * The queues holding the most dead letters, each with its most common cause
 * from a sample of the newest (dlq-signatures). The causes themselves are the
 * Dead letters page's: each row links there.
 *
 * @param {Map<string, {data?: object, error?: object}>} sigs queue → its signatures read
 */
export function deadModel(eff = [], sigs = new Map(), keep = all, limit = 6) {
  return (eff || [])
    .filter((e) => keep(e.queue) && (num(e.deadLetter) || 0) > 0)
    .sort((a, b) => b.deadLetter - a.deadLetter)
    .slice(0, limit)
    .map((e) => {
      const s = sigs.get(e.queue)
      const top = s?.data?.signatures?.[0] || null
      let cause
      if (!s) cause = { text: 'Reading its causes…', dim: true }
      else if (s.error) cause = { text: 'Its causes could not be read.', dim: true }
      else if (!top) cause = { text: 'No error message in the sample.', dim: true }
      else cause = { text: top.text, mono: true, share: num(top.share), sample: num(s.data.sample) || 0, n: num(top.n) || 0 }
      return {
        key: e.queue,
        dead: fmt(e.deadLetter),
        cause,
        shareText: cause.share === undefined || cause.share === null ? '' : `${pct(cause.share)} of the newest ${fmt(cause.sample)}`,
        to: { path: '/dlq', query: { queue: e.queue } },
      }
    })
}

/**
 * The queues that retain the most bytes, and what one message costs: the
 * bytes are the 24 h read's `now`, the message count the queue list's, the
 * only place it exists.
 */
export function bytesModel(eff = [], queues = [], keep = all, limit = 10) {
  const msgs = new Map()
  for (const q of queues || []) {
    const n = num(q?.segments?.messages ?? q?.messages?.total)
    if (q?.name && n !== null && n > 0) msgs.set(q.name, n)
  }
  const rows = (eff || [])
    .filter((e) => keep(e.queue) && (num(e.retainedBytes) || 0) > 0 && msgs.has(e.queue))
    .sort((a, b) => b.retainedBytes - a.retainedBytes)
    .slice(0, limit)
  const top = Math.max(1, ...rows.map((e) => e.retainedBytes))
  return rows.map((e) => ({
    key: e.queue,
    retained: bytes(e.retainedBytes),
    bar: e.retainedBytes / top,
    messages: fmt(msgs.get(e.queue)),
    each: bytes(e.retainedBytes / msgs.get(e.queue)),
  }))
}

// ---- what the deeper layer adds up to ---------------------------------------------------------

/**
 * The old page's deeper findings, in words: loss by policy, chronic waits,
 * dead-letter queues that are mostly one error or heavy rows, an hour far
 * from its usual (amber); and, in grey, what the counters cannot attribute
 * (pushes and acks made inside transactions) and how many partitions are
 * alive. The numbers stay in the tables above; these say what they mean.
 *
 * @returns {Array<{ tone: ''|'amber', text: string, evidence: string }>} amber first
 */
export function deeperNotes({ eff = [], bucketMinutes = 15, baseline = null, sigs = [], retention = [], liveness = null, listBy = 'namespace' } = {}) {
  const out = []
  const byQueue = new Map((eff || []).map((e) => [e.queue, e]))

  for (const r of retention || []) {
    if (!(r.evicted > 0)) continue
    const e = byQueue.get(r.key)
    out.push({
      tone: 'amber',
      text: `\`${r.key}\` dropped messages nobody had read: they outlived its max wait.`,
      evidence: e ? `It has ${plural(e.groups || 0, 'consumer group')}. Loss by policy, not by fault.` : 'Loss by policy, not by fault.',
    })
  }

  for (const e of eff || []) {
    const b = e.lagBudget
    if (!b || b.buckets < 20 || b.over60s / b.buckets < 0.25) continue
    out.push({
      tone: 'amber',
      text: `On \`${e.queue}\` a message waits over a minute for its pop in a quarter or more of the busy ${bucketWords(bucketMinutes)}.`,
      evidence: 'Not a spike: it keeps happening through the day.',
    })
  }

  // One line for every queue it fits: the causes themselves are listed above.
  const one = (sigs || []).filter((d) => d?.signatures?.length && d.signatures[0].share >= 0.5 && (d.rowsNow || 0) >= 100)
  if (one.length) {
    const names = one.map((d) => `\`${d.queue}\``)
    out.push({
      tone: 'amber',
      text: one.length === 1
        ? `${names[0]}'s dead letters are mostly one error: fixing it clears most of them.`
        : `The dead letters of ${names.slice(0, -1).join(', ')} and ${names[names.length - 1]} are each mostly one error: fixing it clears most of them.`,
      evidence: '',
    })
  }

  const heavy = (sigs || []).filter((d) => (d?.avgBytes || 0) >= 100_000)
  if (heavy.length) {
    out.push({
      tone: 'amber',
      text: `Dead letters of ${bytes(Math.max(...heavy.map((d) => d.avgBytes)))} each on ${heavy.length === 1 ? `\`${heavy[0].queue}\`` : plural(heavy.length, 'queue')}: they fill the disk faster than the messages that failed.`,
      evidence: heavy.length > 1 ? heavy.map((d) => `${d.queue} ${bytes(d.avgBytes)}`).join(' · ') : `Over the newest ${fmt(heavy[0].sample)} rows.`,
    })
  }

  for (const r of baseline?.rows || []) {
    if (r.tenant || r.ratio?.tone !== 'amber') continue
    const name = r.mono ? `\`${r.key}\`` : (r.key ? r.name : `The queues without a ${listBy}`)
    const none = r.ratio.text === '0×'
    out.push({
      tone: 'amber',
      text: none
        ? `${name} delivered nothing at ${baseline.label}, an hour that is usually busy.`
        : `${name} had ${r.ratio.text.replace('×', ' times')} its usual deliveries at ${baseline.label}.`,
      evidence: r.min === r.max ? `${r.usual} at that hour on the earlier days.` : `Between ${r.min} and ${r.max} at that hour on the earlier days.`,
    })
  }

  const fed = (eff || []).filter((e) => e.push === 0 && e.trx > 0 && e.pop >= 1000).sort((a, b) => b.pop - a.pop)
  if (fed.length) {
    out.push({
      tone: '',
      text: fed.length === 1
        ? `\`${fed[0].queue}\` is fed through transactions: its pushes are not counted, so reads per push cannot be measured.`
        : `${fmt(fed.length)} queues are fed through transactions: their pushes are not counted, so reads per push cannot be measured.`,
      evidence: fed.length > 1 ? fed.slice(0, 5).map((e) => e.queue).join(' · ') + (fed.length > 5 ? ' · …' : '') : '',
    })
  }

  const elsewhere = (eff || []).filter((e) => e.pop >= 1000 && e.ackPerDelivery !== null && e.ackPerDelivery < 0.5
    && e.fanout !== null && e.fanout <= (e.groups || 1) * 1.25).sort((a, b) => b.pop - a.pop)
  if (elsewhere.length) {
    out.push({
      tone: '',
      text: `${elsewhere.length === 1 ? `\`${elsewhere[0].queue}\` acks` : `${fmt(elsewhere.length)} queues ack`} under half of what ${elsewhere.length === 1 ? 'it delivers' : 'they deliver'}, yet nothing piles up: the acks happen inside transactions, which the counters do not attribute.`,
      evidence: elsewhere.length > 1 ? elsewhere.slice(0, 5).map((e) => e.queue).join(' · ') : '',
    })
  }

  if (liveness && liveness.total > 0) {
    out.push({
      tone: '',
      text: `${fmt(liveness.written)} of ${plural(liveness.total, 'partition')} were written to in the last 24 hours.`,
      evidence: liveness.churn ? `\`${liveness.churn.queue}\` alone created ${fmt(liveness.churn.created24h)} in that day.` : '',
    })
  }

  return out.map((f, i) => ({ f, i })).sort((a, b) => ((b.f.tone ? 1 : 0) - (a.f.tone ? 1 : 0)) || (a.i - b.i)).map((x) => x.f)
}

/**
 * The liveness read's sum over the queues kept, and the queue that created the
 * most partitions in the day: by the 24 h read's counter when it has the
 * queue, else by the liveness route's own (the ones still there).
 */
export function livenessSum(live, keep = all, eff = []) {
  const list = (Array.isArray(live?.rows) ? live.rows : []).filter((r) => r?.queue && keep(r.queue))
  if (!list.length) return null
  const counted = new Map((eff || []).map((e) => [e.queue, num(e.partitionsCreated)]))
  const created = (r) => counted.get(r.queue) ?? num(r.created24h) ?? 0
  const total = list.reduce((a, r) => a + (num(r.partitions) || 0), 0)
  const written = list.reduce((a, r) => a + (num(r.live24h) || 0), 0)
  const top = [...list].sort((a, b) => created(b) - created(a))[0]
  return { total, written, churn: top && created(top) > 0 ? { queue: top.queue, created24h: created(top) } : null }
}

