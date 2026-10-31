// The Overview's reads and words. One rule of what needs you, the app's own
// (useAttention.queueAttention over the last hour, and the cell's issues),
// so the new page and the old one never disagree. Then, for each thing that
// needs you, the few facts that say why: the cause of its dead letters, how
// its waiting messages are spread over its partitions, its last hour.
//
// Reads, every 30 s (the app's ticker, paused while the tab is hidden):
//   stores: resources/queues, consumer-groups, workload?groupBy=queue,
//           the alert lines, ephemeral queues (shared with every page)
//   GET /api/v1/resources/overview
//   GET /api/v1/analytics/queue-ops?from&to      (the window's traffic)
// and at most once a minute for each thing that needs you:
//   GET /api/v1/analytics/dlq-signatures?queue=  (failing acks)
//   GET /api/v1/resources/partitions?queue=      (behind)
//   GET /api/v1/analytics/queue-ops?queue=&from&to (behind: waiting, last hour)
import { computed, ref, shallowRef, watch } from 'vue'
import { resources, system, queues as queuesApi, kv } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { queueAttention, groupAttention } from '@/composables/useAttention'
import { rollupQueueOps } from '@/composables/overviewViews'
import { trimIncompleteBuckets } from '@/composables/useApi'
import { ackFailureSeverity, SEV_BAD, SEV_WARN } from '@/composables/useSeverity'
import { acksText } from '@/composables/queueOps'
import { rate, span } from './words.js'
import { currentEpoch } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { useEphemeralStore } from '@/stores/ephemeralStore'
import { nowMs, clock } from './useWindow'

const QUEUE_MODE = '__QUEUE_MODE__'
const MAX_CARDS = 3
const MAX_BEHIND = 3

// ---- words ----
const fmt = (n) => (n == null || !Number.isFinite(Number(n)) ? '–' : Math.round(Number(n)).toLocaleString('en-US'))
export { rate, span }
function bytes(b) {
  if (b == null) return '–'
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let v = Number(b); let i = 0
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++ }
  return `${v < 10 && i > 0 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${u[i]}`
}
const tone = (sev) => (sev === SEV_BAD ? 'red' : sev === SEV_WARN ? 'amber' : '')
const RANK = { [SEV_BAD]: 2, [SEV_WARN]: 1 }
const clip = (s, n = 120) => (s && s.length > n ? s.slice(0, n - 1).trimEnd() + '…' : s || '')
const enc = encodeURIComponent
const toMs = (b) => (typeof b === 'number' ? (b < 1e12 ? b * 1000 : b) : Date.parse(b))

// The last `n` complete minutes' mean of a per-minute count, as a rate a second.
function perSecond(minutes, n = 5) {
  const done = (minutes || []).slice(0, -1).filter((v) => v != null)
  const last = done.slice(-n)
  if (!last.length) return null
  return last.reduce((a, b) => a + b, 0) / last.length / 60
}

// The first minute of the run of non-zero minutes that ends now (a gap of
// up to two quiet minutes keeps the run going). null: nothing now.
function runStart(minutes, buckets) {
  const done = (minutes || []).slice(0, -1)
  let i = done.length - 1
  let quiet = 0
  let start = null
  while (i >= 0) {
    if ((done[i] || 0) > 0) { start = i; quiet = 0 } else if (start !== null && ++quiet > 2) break
    else if (start === null && done.length - 1 - i > 2) return null
    i--
  }
  if (start === null) return null
  // The run reaches back to the hour's first measured minute: it is older.
  const first = done.findIndex((v) => v != null)
  return { ms: toMs(buckets[start]), sinceStart: start <= Math.max(0, first) + 1 }
}

export function useOverview(win, frame) {
  const qs = useQueuesStore()
  const gs = useGroupsStore()
  const os = useQueueOpsStore()
  const ss = useSettingsStore()
  const es = useEphemeralStore()

  const overview = shallowRef(null)
  const ops = shallowRef(null)           // the window's queue-ops answer
  const opsKey = ref('')
  const failed = ref(null)               // a read that failed, in words
  const loading = ref(true)
  const kvNamespaces = ref(null)
  const extras = shallowRef(new Map())   // 'sig:q' | 'parts:q' | 'wait:q' -> { at, data }

  async function refresh() {
    const epoch = currentEpoch()
    loading.value = true
    const w = win.value
    const key = w.custom ? `${w.from}-${w.to}` : `${w.id}`
    const fetchOps = !ops.value || opsKey.value !== key || !w.custom
    const jobs = [
      qs.fetchQueues({ ttlMs: 25_000 }),
      gs.fetchGroups(),
      os.fetchOps(),
      ss.fetchSettings?.(),
      es.fetchQueues?.(),
      resources.getOverview({ quiet: true }).then((r) => { if (epoch === currentEpoch()) overview.value = r.data }),
      fetchOps
        ? system.getQueueOps({ from: w.iso.from, to: w.iso.to }, { quiet: true }).then((r) => {
          if (epoch !== currentEpoch()) return
          ops.value = r.data; opsKey.value = key
        })
        : null,
    ]
    if (kvNamespaces.value === null) {
      jobs.push(kv.namespaces({ quiet: true, probe: true }).then((r) => { kvNamespaces.value = (r.data?.namespaces || []).length }).catch(() => {}))
    }
    const done = await Promise.allSettled(jobs)
    if (epoch !== currentEpoch()) return
    const bad = done.find((d) => d.status === 'rejected')
    failed.value = bad ? describeApiError(bad.reason) : (qs.error.value ? describeApiError(qs.error.value) : null)
    loading.value = false
    fetchExtras(epoch)
  }
  useAutoRefresh(refresh)
  refresh()
  // A new window: its traffic only.
  watch(() => (win.value.custom ? `${win.value.from}-${win.value.to}` : win.value.id), () => refresh())

  // ---- what needs you ----
  const linesFor = (name) => (typeof ss.linesFor === 'function' ? ss.linesFor(name) : undefined)
  const groupsOf = computed(() => {
    const m = new Map()
    for (const g of gs.groups.value || []) {
      if (!g?.queueName) continue
      if (!m.has(g.queueName)) m.set(g.queueName, [])
      m.get(g.queueName).push(g)
    }
    return m
  })
  const queueRows = computed(() => qs.queues.value || [])
  const queueByName = computed(() => new Map(queueRows.value.map((q) => [q.name, q])))

  const queueIssues = computed(() => {
    if (!queueRows.value.length || gs.groups.value == null) return []
    const byDepth = [...queueRows.value].sort((a, b) => (b.messages?.pending || 0) - (a.messages?.pending || 0))
    return queueAttention(byDepth, gs.groups.value, linesFor, os.ops.value)
  })
  const tenantAcks = computed(() => {
    if (queueIssues.value.some((i) => i.reason === 'acks') || !os.ops.value) return null
    let f = 0; let s = 0; let k = 0
    for (const o of os.ops.value.values()) { f += o.ackFailed || 0; s += o.ackSuccess || 0; if (o.ackFailed) k++ }
    const sev = ackFailureSeverity({ failed: f, succeeded: s }, ss.lines?.value)
    return sev ? { sev, failed: f, succeeded: s, queues: k } : null
  })
  const cellIssues = computed(() => (frame?.cell?.value?.issues || []))

  const allIssues = computed(() => {
    const out = []
    for (const c of cellIssues.value) out.push({ kind: 'cell', sev: c.sev, key: c.key, issue: c })
    for (const q of queueIssues.value) out.push({ kind: 'queue', sev: q.sev, key: 'q:' + q.name, issue: q })
    if (tenantAcks.value) out.push({ kind: 'tenant', sev: tenantAcks.value.sev, key: 'acks', issue: tenantAcks.value })
    const order = { cell: 0, queue: 1, tenant: 2 }
    return out.sort((a, b) => (RANK[b.sev] || 0) - (RANK[a.sev] || 0) || order[a.kind] - order[b.kind])
  })

  function extra(kind, name) {
    const e = extras.value.get(`${kind}:${name}`)
    return e ? e.data : null
  }
  async function fetchExtras(epoch) {
    const wanted = allIssues.value.slice(0, MAX_CARDS).filter((i) => i.kind === 'queue')
    const now = Date.now()
    const next = new Map(extras.value)
    const jobs = []
    const want = (k, fn) => {
      const e = next.get(k)
      if (e && now - e.at < 60_000) return
      jobs.push(fn().then((data) => { next.set(k, { at: Date.now(), data }) }).catch(() => { next.set(k, { at: Date.now(), data: null }) }))
    }
    for (const { issue } of wanted) {
      const name = issue.name
      if (issue.reason === 'acks') {
        want(`sig:${name}`, () => system.getDlqSignatures({ queue: name }, { quiet: true, probe: true }).then((r) => r.data))
      } else if (issue.reason === 'lag') {
        want(`parts:${name}`, () => queuesApi.partitions({ queue: name, limit: 50 }, { quiet: true, probe: true }).then((r) => r.data))
        const to = new Date(); const from = new Date(to.getTime() - 3600_000)
        want(`wait:${name}`, () => system.getQueueOps({ queue: name, from: from.toISOString(), to: to.toISOString() }, { quiet: true }).then((r) => r.data?.backlog || []))
      }
    }
    if (!jobs.length) return
    await Promise.allSettled(jobs)
    if (epoch !== currentEpoch()) return
    extras.value = next
  }

  // The group that stands for a queue: its worst live one by lag, else the
  // first named one, else queue mode.
  function readerOf(name) {
    const all = groupsOf.value.get(name) || []
    const named = all.filter((g) => g.name !== QUEUE_MODE)
    const live = all.filter((g) => groupAttention(g, linesFor) !== 'mute')
    const worst = [...live].sort((a, b) => (b.maxTimeLag || 0) - (a.maxTimeLag || 0))[0]
    if (worst && worst.name !== QUEUE_MODE) return { label: worst.name, mono: true, group: worst }
    if (named.length === 1) return { label: named[0].name, mono: true, group: named[0] }
    if (named.length > 1) return { label: `${named.length} groups`, mono: false, group: null }
    return { label: 'queue mode', mono: false, group: worst || null }
  }

  function card(entry) {
    const now = nowMs.value
    if (entry.kind === 'cell') {
      const c = entry.issue
      return {
        key: entry.key, tone: tone(c.sev), badge: 'Cell', heading: c.words, sentence: c.why,
        detail: 'It is the broker under every tenant of this cell.',
        actions: [{ label: 'Open Cluster', to: '/cluster' }],
      }
    }
    if (entry.kind === 'tenant') {
      const t = entry.issue
      const total = t.failed + t.succeeded
      return {
        key: entry.key, tone: tone(t.sev), badge: 'Failing', heading: 'Acks across your queues',
        sentence: `${fmt(t.failed)} of ${fmt(total)} acks failed in the last hour${total ? ` (${Math.round((t.failed / total) * 1000) / 10}%)` : ''}, over ${t.queues} queues.`,
        detail: 'No single queue explains it: look at the dead letters by cause.',
        actions: [{ label: 'Open dead letters', to: '/dlq' }],
      }
    }
    const a = entry.issue
    const name = a.name
    const q = queueByName.value.get(name) || {}
    const reader = readerOf(name)
    const minutes = os.series.value?.get(name) || null
    const buckets = os.buckets.value || []
    const base = {
      key: entry.key, tone: tone(a.sev), queue: name, link: `/queues/${enc(name)}`,
      group: reader.label, groupMono: reader.mono,
    }
    if (a.reason === 'acks') {
      const sig = extra('sig', name)
      const dead = q.messages?.deadLetter
      let detail = ''
      if (sig?.signatures?.length) {
        const top = sig.signatures[0]
        detail = sig.signatures.length === 1
          ? `${fmt(dead ?? sig.rowsNow)} dead letters, all one cause: “${clip(top.text)}”.`
          : `${fmt(dead ?? sig.rowsNow)} dead letters, ${sig.signatures.length} causes; most of them: “${clip(top.text, 90)}”.`
        const newest = sig.newest ? Date.parse(sig.newest) : null
        if (newest) {
          detail += now - newest < 10 * 60_000
            ? ` Still happening, the last one ${span((now - newest) / 1000)} ago, so a replay now would fail the same way.`
            : ` The last one came at ${clock(newest)}.`
        }
      } else if (dead) {
        detail = `${fmt(dead)} dead letters wait in it.`
      }
      const run = minutes ? runStart(minutes.ackFailed, buckets) : null
      return {
        ...base, badge: 'Failing',
        sentence: a.ops ? `${acksText(a.ops)[0].toUpperCase()}${acksText(a.ops).slice(1)} in the last hour.` : 'Its acks are failing.',
        detail,
        since: run ? (run.sinceStart ? 'all of the last hour' : `since ${clock(run.ms)} · ${span((now - run.ms) / 1000)}`) : '',
        spark: minutes ? { kind: 'bars', values: minutes.ackFailed.slice(0, -1).map((v) => v ?? 0), caption: 'failed acks a minute, last hour' } : null,
        actions: [
          { label: 'Open its dead letters', to: { path: '/dlq', query: { queue: name } } },
          { label: 'Open the queue', to: `/queues/${enc(name)}` },
        ],
      }
    }
    if (a.reason === 'lag') {
      const inS = minutes ? perSecond(minutes.push) : null
      const outS = minutes ? perSecond(minutes.pop) : null
      const pending = a.pending || q.messages?.pending || 0
      const growing = inS != null && outS != null && outS < inS
      let sentence = `${fmt(pending)} message${pending === 1 ? '' : 's'} waiting; the oldest has waited ${span(a.lag)}.`
      if (inS != null && outS != null) {
        sentence += growing
          ? ` ${rate(inS)} a second arrive and ${reader.mono ? reader.label : 'its readers'} take ${rate(outS)}, so the pile grows.`
          : ` It drains ${rate(outS - inS)} a second faster than it fills.`
      }
      let detail = ''
      const parts = extra('parts', name)
      const list = parts?.partitions || []
      const total = q.partitions || parts?.walked || list.length
      if (list.length && pending > 0 && total >= 5) {
        const top = list.slice(0, 3).reduce((s, p) => s + (p.pending || 0), 0)
        const share = top / Math.max(pending, list.reduce((s, p) => s + (p.pending || 0), 0))
        if (share >= 0.6) detail = `${Math.min(3, list.length)} of ${fmt(total)} partitions hold ${Math.round(share * 100)}% of them: those entities are stuck, and more workers will not help.`
        else detail = `They are spread over ${fmt(total)} partitions: more workers would help.`
      }
      const wait = extra('wait', name)
      const values = (wait || []).map((r) => (r.pending == null ? null : Number(r.pending)))
      return {
        ...base, badge: growing ? 'Not draining' : 'Behind', sentence, detail,
        since: a.lag ? `oldest since ${clock(now - a.lag * 1000)}` : '',
        spark: values.length > 1 ? { kind: 'line', values, caption: 'waiting, last hour' } : null,
        actions: [
          { label: 'Open the queue', to: `/queues/${enc(name)}` },
          { label: 'Its partitions', to: { path: `/queues/${enc(name)}`, query: { tab: 'partitions' } } },
        ],
      }
    }
    // noReader
    return {
      ...base, badge: 'No reader', group: '',
      sentence: `${fmt(a.pending)} message${a.pending === 1 ? '' : 's'} waiting, and no consumer group reads it.`,
      detail: a.deadOnly ? 'Queue mode has a cursor here, but it has never consumed.' : 'Start its workers, or mark it as read by nobody in its alert lines.',
      spark: minutes ? { kind: 'line', values: minutes.push.slice(0, -1).map((v) => v ?? 0), caption: 'arriving a minute, last hour' } : null,
      actions: [
        { label: 'Open the queue', to: `/queues/${enc(name)}` },
        { label: 'Alert lines', to: { path: '/settings', query: { queue: name } } },
      ],
    }
  }

  const needs = computed(() => allIssues.value.slice(0, MAX_CARDS).map(card))
  const moreNeeds = computed(() => Math.max(0, allIssues.value.length - MAX_CARDS))
  const moreLink = computed(() => ({ path: '/queues' }))

  // ---- behind, and draining on their own ----
  const behindAll = computed(() => {
    const flagged = new Set(queueIssues.value.map((i) => i.name))
    const out = []
    for (const q of queueRows.value) {
      const pending = q.messages?.pending || 0
      if (pending <= 0 || flagged.has(q.name)) continue
      const m = os.series.value?.get(q.name)
      const inS = m ? perSecond(m.push) : null
      const outS = m ? perSecond(m.pop) : null
      if (inS == null || outS == null || outS <= inS) continue
      const eta = pending / (outS - inS)
      const r = readerOf(q.name)
      out.push({
        key: q.name, queue: q.name, link: `/queues/${enc(q.name)}`, group: r.label, groupMono: r.mono, pending,
        waiting: `${fmt(pending)} waiting`,
        eta: eta < 60 ? 'clears in under a minute' : `clears in about ${span(eta)}`,
      })
    }
    return out.sort((a, b) => b.pending - a.pending)
  })
  const behind = computed(() => behindAll.value.slice(0, MAX_BEHIND))

  // ---- the window's traffic ----
  const hero = computed(() => {
    const d = ops.value
    const bm = d?.bucketMinutes || 1
    // A bucket the window cuts reads low (its count over the whole bucket):
    // keep only the buckets wholly inside it.
    const lo = win.value.from - 1000
    const hi = win.value.to + 1000
    const rows = d
      ? trimIncompleteBuckets(rollupQueueOps(d.series), { bucketMinutes: bm }).filter((r) => {
        const t = Date.parse(r.bucket)
        return t >= lo && t + bm * 60_000 <= hi
      })
      : []
    const inPts = rows.map((r) => [Date.parse(r.bucket) + (bm * 60_000) / 2, r.pushPerSecond])
    const ackPts = rows.map((r) => [Date.parse(r.bucket) + (bm * 60_000) / 2, r.ackSuccess / (bm * 60)])
    const avg = (pts) => (pts.length ? pts.reduce((s, p) => s + (p[1] || 0), 0) / pts.length : null)
    const created = rows.reduce((s, r) => s + (r.partitionsCreated || 0), 0)
    const deleted = rows.reduce((s, r) => s + (r.partitionsDeleted || 0), 0)
    return {
      series: [
        { id: 'in', label: 'In', ink: 'in', area: true, points: inPts },
        { id: 'acked', label: 'Acked', ink: 'out', points: ackPts },
      ],
      legend: [
        { id: 'in', label: 'In a second', ink: 'var(--ink-in)', value: rate(avg(inPts)) },
        { id: 'acked', label: 'Acked', ink: 'var(--ink-out)', value: rate(avg(ackPts)) },
      ],
      partitions: { created, deleted },
      aria: `Messages arriving and acked each second, ${win.value.long}.`,
      empty: d != null && !rows.length,
    }
  })

  // ---- what the cluster holds ----
  const inventory = computed(() => {
    const o = overview.value || {}
    const qrows = queueRows.value
    const groups = gs.groups.value || []
    const behindGroups = groups.filter((g) => {
      const s = groupAttention(g, linesFor)
      return s === 'warn' || s === 'bad'
    }).length
    const nss = new Set(qrows.map((q) => q.namespace).filter((n) => typeof n === 'string' && n))
    const retained = qrows.reduce((s, q) => s + (Number(q.retainedBytes) || 0), 0)
    const dlqQueues = qrows.filter((q) => (q.messages?.deadLetter || 0) > 0).length
    const p = hero.value.partitions
    const rows = [
      { id: 'queues', label: 'Queues', value: fmt(qrows.length || o.queues), sub: `${nss.size} namespace${nss.size === 1 ? '' : 's'}`, to: '/queues' },
      { id: 'partitions', label: 'Partitions', value: fmt(o.partitions), sub: p.created || p.deleted ? `+${fmt(p.created)} −${fmt(p.deleted)}` : '', title: `Made and removed, ${win.value.long}`, to: '/queues' },
      { id: 'groups', label: 'Consumer groups', value: fmt(groups.length), sub: behindGroups ? `${behindGroups} behind` : 'none behind', to: { path: '/queues', query: { view: 'groups' } } },
      { id: 'stored', label: 'Messages stored', value: fmt(o.messages?.total), sub: retained ? bytes(retained) : '', to: { path: '/queues', query: { view: 'messages' } } },
      { id: 'dlq', label: 'Dead letters', value: fmt(o.messages?.deadLetter), sub: dlqQueues ? `in ${dlqQueues} queue${dlqQueues === 1 ? '' : 's'}` : '', to: '/dlq' },
      { id: 'ephemeral', label: 'Ephemeral queues', value: es.count?.value == null ? '–' : fmt(es.count.value), sub: es.totals?.value?.depth != null ? `${fmt(es.totals.value.depth)} waiting` : '', to: { path: '/queues', query: { show: 'eph' } } },
      { id: 'kv', label: 'KV keys', value: fmt(qs.kvRows.value), sub: qs.kvBytes.value != null ? bytes(qs.kvBytes.value) : '', title: kvNamespaces.value ? `In ${kvNamespaces.value} KV namespaces` : '', to: '/state' },
      { id: 'timers', label: 'Timers', value: qs.timerRows.value ? fmt(qs.timerRows.value) : '0', sub: '', to: { path: '/state', query: { view: 'timers' } } },
    ]
    return rows
  })

  // ---- the verdict ----
  const ready = computed(() => queueRows.value.length > 0 && gs.groups.value != null)
  const verdict = computed(() => {
    const n = allIssues.value.length
    const worst = allIssues.value[0]
    const st = frame?.cell?.value?.status
    const title = n === 0 ? 'Nothing needs you' : n === 1 ? '1 thing needs you' : `${n} things need you`
    let sentence = n ? 'Everything else is flowing' : 'Everything is flowing'
    if (st && !cellIssues.value.length) {
      const voters = st.raft.voters || st.nodes.length
      const up = st.raft.up ?? st.nodes.length
      sentence += st.raft.single ? ', on one node.' : up === voters ? `, and all ${voters} nodes are voting.` : `, with ${up} of ${voters} nodes voting.`
    } else sentence += '.'
    return { title, sentence, tone: worst ? tone(worst.sev) : '', count: n }
  })

  const error = computed(() => (failed.value && !ready.value ? `The broker did not answer: ${failed.value}` : null))

  return {
    loading, ready, error, verdict, needs, moreNeeds, moreLink, behind,
    moreBehind: computed(() => Math.max(0, behindAll.value.length - MAX_BEHIND)),
    hero, inventory, refresh,
  }
}
