// One queue: is it all right, and if not, why. The same rule as the Overview
// (useAttention.queueAttention over the last hour), then the facts behind it.
//
// Reads, every 30 s (the app's ticker):
//   stores: resources/queues, consumer-groups, workload?groupBy=queue, alert lines
//   GET /api/v1/analytics/queue-ops?queue=&from&to    its traffic and waiting, the window
//   GET /api/v1/resources/partitions?queue=&limit=500 the partitions holding the most
//   GET /api/v1/analytics/dlq-signatures?queue=       why its dead letters failed (when it has some)
// The tabs read their own lists (messages, dead letters, timers, options).
import { computed, ref, shallowRef, watch } from 'vue'
import { queues as queuesApi, system } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { queueAttention, groupAttention } from '@/composables/useAttention'
import { rollupQueueOps } from '@/composables/overviewViews'
import { trimIncompleteBuckets } from '@/composables/useApi'
import { acksText } from '@/composables/queueOps'
import { SEV_BAD, SEV_WARN } from '@/composables/useSeverity'
import { currentEpoch } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { rate, span } from './useOverview'

export const QUEUE_MODE = '__QUEUE_MODE__'
export const fmt = (n) => (n == null || !Number.isFinite(Number(n)) ? '–' : Math.round(Number(n)).toLocaleString('en-US'))
export const tone = (sev) => (sev === SEV_BAD ? 'red' : sev === SEV_WARN ? 'amber' : '')
export const groupLabel = (name) => (name === QUEUE_MODE ? 'queue mode' : name)

function perSecond(minutes, n = 5) {
  const done = (minutes || []).slice(0, -1).filter((v) => v != null)
  const last = done.slice(-n)
  if (!last.length) return null
  return last.reduce((a, b) => a + b, 0) / last.length / 60
}

export function useQueuePage(name, win) {
  const qs = useQueuesStore()
  const gs = useGroupsStore()
  const os = useQueueOpsStore()
  const ss = useSettingsStore()
  const linesFor = (q) => (typeof ss.linesFor === 'function' ? ss.linesFor(q) : undefined)

  const ops = shallowRef(null)
  const opsKey = ref('')
  const parts = shallowRef(null)
  const sig = shallowRef(null)
  const sigAt = ref(0)
  const loading = ref(true)
  const failed = ref(null)
  const missing = ref(false)

  const row = computed(() => (qs.queues.value || []).find((q) => q.name === name.value) || null)
  const groups = computed(() => (gs.groups.value || []).filter((g) => g.queueName === name.value))
  const hour = computed(() => os.ops.value?.get(name.value) || null)
  const minutes = computed(() => os.series.value?.get(name.value) || null)
  const issue = computed(() => {
    if (!row.value || gs.groups.value == null) return null
    return queueAttention([row.value], gs.groups.value, linesFor, os.ops.value)[0] || null
  })

  async function refresh() {
    const epoch = currentEpoch()
    const q = name.value
    const w = win.value
    loading.value = true
    const key = `${q}|${w.custom ? `${w.from}-${w.to}` : w.id}`
    const jobs = [
      qs.fetchQueues({ ttlMs: 25_000 }),
      gs.fetchGroups(),
      os.fetchOps(),
      ss.fetchSettings?.(),
      (!ops.value || opsKey.value !== key || !w.custom)
        ? system.getQueueOps({ queue: q, from: w.iso.from, to: w.iso.to }, { quiet: true }).then((r) => {
          if (epoch !== currentEpoch() || q !== name.value) return
          ops.value = r.data; opsKey.value = key
        })
        : null,
      queuesApi.partitions({ queue: q, limit: 500 }, { quiet: true, probe: true }).then((r) => {
        if (epoch === currentEpoch() && q === name.value) parts.value = r.data
      }),
    ]
    const done = await Promise.allSettled(jobs)
    if (epoch !== currentEpoch() || q !== name.value) return
    const bad = done.find((d) => d.status === 'rejected')
    failed.value = bad ? describeApiError(bad.reason) : null
    // Not in the listing: ask for it by name, so a deleted or mistyped queue says so.
    if (!row.value && qs.queues.value?.length) {
      try { await queuesApi.get(q, { quiet: true, probe: true }); missing.value = false } catch (e) { missing.value = e?.status === 404 || e?.response?.status === 404 || /404|not found/i.test(String(e?.message)) }
    } else missing.value = false
    // The causes of its dead letters, at most once a minute.
    if ((row.value?.messages?.deadLetter || 0) > 0 && Date.now() - sigAt.value > 60_000) {
      sigAt.value = Date.now()
      system.getDlqSignatures({ queue: q }, { quiet: true, probe: true }).then((r) => { if (q === name.value) sig.value = r.data }).catch(() => {})
    }
    loading.value = false
  }
  useAutoRefresh(refresh)
  refresh()
  watch(name, () => { ops.value = null; parts.value = null; sig.value = null; sigAt.value = 0; refresh() })
  watch(() => (win.value.custom ? `${win.value.from}-${win.value.to}` : win.value.id), () => refresh())

  // ---- the window: what arrived, what was acked, what waited ----
  const traffic = computed(() => {
    const d = ops.value
    const bm = d?.bucketMinutes || 1
    const lo = win.value.from - 1000
    const hi = win.value.to + 1000
    const inside = (b) => { const t = Date.parse(b); return t >= lo && t + bm * 60_000 <= hi }
    const rows = d ? trimIncompleteBuckets(rollupQueueOps(d.series, new Set([name.value])), { bucketMinutes: bm }).filter((r) => inside(r.bucket)) : []
    const mid = (b) => Date.parse(b) + (bm * 60_000) / 2
    const inPts = rows.map((r) => [mid(r.bucket), r.pushPerSecond])
    const ackPts = rows.map((r) => [mid(r.bucket), r.ackSuccess / (bm * 60)])
    const failPts = rows.map((r) => [mid(r.bucket), r.ackFailed / (bm * 60)])
    const anyFail = failPts.some((p) => p[1] > 0)
    const backlog = (d?.backlog || []).filter((b) => inside(b.bucket)).map((b) => [mid(b.bucket), b.pending == null ? null : Number(b.pending)])
    const avg = (pts) => (pts.length ? pts.reduce((s, p) => s + (p[1] || 0), 0) / pts.length : null)
    const series = [
      { id: 'in', label: 'In', ink: 'in', area: true, points: inPts },
      { id: 'acked', label: 'Acked', ink: 'out', points: ackPts },
    ]
    if (anyFail) series.push({ id: 'failed', label: 'Failed', ink: 'out', dashed: true, points: failPts })
    const legend = [
      { id: 'in', label: 'In a second', ink: 'var(--ink-in)', value: rate(avg(inPts)) },
      { id: 'acked', label: 'Acked', ink: 'var(--ink-out)', value: rate(avg(ackPts)) },
    ]
    if (anyFail) legend.push({ id: 'failed', label: 'Failed', ink: 'var(--ink-out)', dashed: true, value: rate(avg(failPts)) })
    const lastWait = backlog.length ? backlog[backlog.length - 1][1] : null
    return {
      series, legend,
      waiting: [{ id: 'waiting', label: 'Waiting', ink: 'in', points: backlog }],
      waitingNow: lastWait,
      ready: d != null,
    }
  })

  // ---- the verdict, in one line ----
  const worstGroup = computed(() => {
    const live = groups.value.filter((g) => groupAttention(g, linesFor) !== 'mute')
    return [...live].sort((a, b) => (b.maxTimeLag || 0) - (a.maxTimeLag || 0))[0] || null
  })
  const verdict = computed(() => {
    const r = row.value
    if (!r) return null
    const pending = r.messages?.pending || 0
    const i = issue.value
    const inS = perSecond(minutes.value?.push)
    const outS = perSecond(minutes.value?.pop)
    if (i?.reason === 'acks') {
      return { tone: tone(i.sev), badge: 'Failing', text: `${acksText(i.ops)} in the last hour.` }
    }
    if (i?.reason === 'lag') {
      const g = worstGroup.value
      const growing = inS != null && outS != null && outS < inS
      return {
        tone: tone(i.sev), badge: growing ? 'Not draining' : 'Behind',
        text: `${g ? groupLabel(g.name) : 'A group'} is ${span(i.lag)} behind, with ${fmt(pending)} waiting.`,
      }
    }
    if (i?.reason === 'noReader') {
      return { tone: 'amber', badge: 'No reader', text: `${fmt(pending)} waiting, and no consumer group reads it.` }
    }
    if (pending > 0 && inS != null && outS != null && outS > inS) {
      const eta = pending / (outS - inS)
      return { tone: '', badge: 'Draining', text: `${fmt(pending)} waiting, ${eta < 60 ? 'clears in under a minute' : `clears in about ${span(eta)}`}.` }
    }
    const h = hour.value
    if (!h || (!h.pushMessages && !h.popMessages)) return { tone: '', badge: 'Quiet', text: 'Nothing arrived or left in the last hour.' }
    return { tone: '', badge: 'Flowing', text: 'Every group keeps up.' }
  })
  const caughtUp = computed(() => {
    const i = issue.value
    const named = groups.value.filter((g) => (g.totalLag || 0) === 0 && groupAttention(g, linesFor) !== 'mute')
    if (!i || !named.length || named.length === groups.value.length) return ''
    const names = named.slice(0, 3).map((g) => groupLabel(g.name))
    const more = named.length > 3 ? ` and ${named.length - 3} more` : ''
    return `${names.join(', ')}${more} ${named.length === 1 ? 'is' : 'are'} caught up.`
  })

  // ---- why it needs you ----
  const why = computed(() => {
    const i = issue.value
    if (!i) return null
    const r = row.value || {}
    if (i.reason === 'acks') {
      const s = sig.value
      const newest = s?.newest ? Date.parse(s.newest) : null
      const still = newest && Date.now() - newest < 10 * 60_000
      return {
        kind: 'acks', tone: tone(i.sev),
        title: 'Its acks are failing',
        causes: (s?.signatures || []).slice(0, 4).map((c) => ({ text: c.text, n: c.n, share: c.share })),
        sample: s?.sample || null,
        dead: r.messages?.deadLetter ?? s?.rowsNow ?? null,
        line: newest
          ? (still
            ? `Still happening: the last one failed ${Date.now() - newest < 5000 ? 'just now' : `${span((Date.now() - newest) / 1000)} ago`}, so a replay now would fail the same way.`
            : `The last one failed at ${new Date(newest).toLocaleTimeString()}.`)
          : '',
        groups: s?.groups || [],
      }
    }
    if (i.reason === 'lag') {
      const list = parts.value?.partitions || []
      const pending = r.messages?.pending || list.reduce((s, p) => s + (p.pending || 0), 0)
      const total = r.partitions || parts.value?.walked || list.length
      const top = list.slice(0, 3)
      const topSum = top.reduce((s, p) => s + (p.pending || 0), 0)
      const share = pending ? Math.min(1, topSum / pending) : 0
      const stuck = share >= 0.6 && total >= 5
      return {
        kind: 'lag', tone: tone(i.sev),
        title: `${worstGroup.value ? groupLabel(worstGroup.value.name) : 'A group'} is behind`,
        share, topCount: top.length, topSum, total, othersSum: Math.max(0, pending - topSum),
        line: stuck
          ? `${top.length} of ${fmt(total)} partitions hold ${Math.round(share * 100)}% of what waits. Those entities are stuck on their oldest message: more workers will not help.`
          : `What waits is spread over ${fmt(total)} partitions: more workers, or faster ones, would help.`,
        rows: list.slice(0, 5),
      }
    }
    return {
      kind: 'noReader', tone: 'amber', title: 'Nobody reads it',
      line: i.deadOnly
        ? 'Queue mode has a cursor here, but it has never consumed. Start its workers, or say in its alert lines that nobody reads it.'
        : 'No consumer group reads this queue. Start its workers, or say in its alert lines that nobody reads it.',
    }
  })

  const facts = computed(() => {
    const r = row.value
    if (!r) return []
    const o = r.options || {}
    const out = []
    if (r.namespace) out.push(r.namespace)
    out.push(`${fmt(r.partitions)} partition${r.partitions === 1 ? '' : 's'}`)
    const named = groups.value.filter((g) => g.name !== QUEUE_MODE).length
    if (named) out.push(`${named} group${named === 1 ? '' : 's'}`)
    else if (groups.value.length) out.push('queue mode')
    if (o.leaseTime != null) out.push(`lease ${o.leaseTime} s`)
    if (r.kafkaTopic) out.push('Kafka topic')
    return out
  })

  return {
    row, groups, hour, minutes, issue, verdict, caughtUp, why, facts, traffic, parts, sig,
    loading, failed, missing, refresh, linesFor,
  }
}
