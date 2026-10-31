// The Workload page, in the new console: who does the work, how much, and
// what is stuck, for the tenant, by namespace, task or queue. The old page's
// reads and rules (views/Workload.vue, composables/useWorkload.js), in the
// console's frame and words.
//
// Reads, on the app's 30 s ticker:
//   GET /api/v1/analytics/workload   the window at the scope's level (a pick is
//                                    groupBy=queue with it as a filter), and the
//                                    window compared with, at most every 5 min
//   stores: resources/queues, consumer-groups, the hour's queue-ops (the side
//   list's marks) and the alert lines
// When someone asks (the deeper layer), never on the ticker:
//   the same read over 7 days by namespace (or task) and 24 hours by queue,
//   GET /api/v1/analytics/retention, partition-liveness and dlq-signatures
// A broker without /analytics/workload answers through the same payload,
// rolled up in this browser from queue-ops, status/queues, the queue list and
// the groups (useWorkload rollupFromQueueOps); the page says so.
import { computed, onUnmounted, ref, shallowRef, watch } from 'vue'
import { analytics, system } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { enrichRows, fetchedOf, readsOf, rollupFromQueueOps, tenantTotals, trimOpenBucket } from '@/composables/useWorkload'
import { missingNodes } from '@/composables/usePartial'
import { workloadList } from '@/composables/workloadSide'
import { currentEpoch, onClusterChange } from '@/stores/identity'
import { isMissingRoute, routeSupport } from '@/stores/routeSupport'
import { useQueuesStore } from '@/stores/queuesStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import {
  compareWindow, compareWords, countsModel, findingsOf, groupsModel, mapModel, marksOf, measureTotal,
  readParams, scopeName, stableKeys, tableRows, trimHeadBucket, verdictOf, windowWords, workCharts,
} from './workload/workloadViews'
import {
  baselineModel, bytesModel, costModel, deadModel, deeperNotes, livenessModel, livenessSum, retentionModel, weekModel,
} from './workload/workloadDeeper'

// The first call is also the probe: nothing negotiates a version, and a
// missing route is a fact about the broker, remembered per cluster.
const getWorkload = routeSupport.guard('workload', (params, config) => system.getWorkload(params, { ...config, probe: true }))
const getSignatures = routeSupport.guard('dlq-signatures', (params, config) => system.getDlqSignatures(params, { ...config, probe: true }))
const getLiveness = routeSupport.guard('partition-liveness', (params, config) => system.getPartitionLiveness(params, { ...config, probe: true }))

const QUIET = { quiet: true }
const DAY = 86_400_000
const listOf = (d) => (Array.isArray(d) ? d : (d?.queues || d?.rows || d?.data || []))

export const FALLBACK_TITLE = 'This broker has no /api/v1/analytics/workload: the page rolled these numbers up from '
  + 'queue-ops, status/queues, resources/queues and the consumer-group list.'

/**
 * @param {object} o
 * @param {import('vue').ComputedRef} o.win      useWindow().win
 * @param {import('vue').ComputedRef} o.scope    scopeOf(by, key)
 * @param {import('vue').ComputedRef} o.measure  what the charts draw
 * @param {import('vue').ComputedRef} o.compare  previous | yesterday | lastWeek | off
 * @param {import('vue').ComputedRef} o.deep     the deeper layer was asked for
 * @param {import('vue').ComputedRef} o.picked   the queue whose consumer groups are open
 * @param {import('vue').Ref<string>} o.search   the side list's filter
 */
export function useWorkloadPage({ win, scope, measure, compare, deep, picked, search }) {
  const qs = useQueuesStore()
  const gs = useGroupsStore()
  const os = useQueueOpsStore()
  const ss = useSettingsStore()
  const linesFor = (q) => (typeof ss.linesFor === 'function' ? ss.linesFor(q) : undefined)
  const winKey = () => (win.value.custom ? `${win.value.from}-${win.value.to}` : win.value.id)

  // ---- the window, at the scope ----------------------------------------------------
  const fallback = ref(routeSupport.missing('workload') !== null)
  const payload = shallowRef(null)
  const shownAt = shallowRef({ sig: '', win: '', scope: null, at: 0 }) // what the payload on screen was read for
  const error = ref(null)
  let seq = 0

  async function rollup(params) {
    const [ops, status] = await Promise.all([
      system.getQueueOps({ from: params.from, to: params.to }, QUIET),
      analytics.getQueues({ limit: 500 }, QUIET).catch(() => ({ data: [] })),
      qs.fetchQueues({ ttlMs: 25_000 }).catch(() => null),
      gs.fetchGroups().catch(() => null),
    ])
    return rollupFromQueueOps({
      ops: ops.data,
      statusQueues: listOf(status.data),
      resourceQueues: qs.queues.value || [],
      consumerGroups: gs.groups.value || [],
      groupBy: params.groupBy,
      namespace: params.namespace,
      task: params.task,
      queue: params.queue,
      capturedAt: new Date(),
    })
  }
  // Until the route has answered once, the reads that start together (the
  // window, the one compared with, the deeper layer) wait for the first: on
  // an older broker that is one 404, not four.
  let decided = fallback.value
  let probe = null
  async function read(params) {
    if (!decided && probe) await probe.catch(() => {})
    if (fallback.value) return rollup(params)
    const ask = getWorkload(params, QUIET)
    if (!decided) probe = ask
    try {
      const data = (await ask).data
      decided = true
      return data
    } catch (e) {
      if (!isMissingRoute(e)) throw e
      // An older broker: switch over and ask once more. dlq-signatures and
      // partition-liveness shipped with /workload, so it lacks them too.
      decided = true
      fallback.value = true
      routeSupport.remember('dlq-signatures', e)
      routeSupport.remember('partition-liveness', e)
      return rollup(params)
    } finally {
      if (probe === ask) probe = null
    }
  }

  async function readNow() {
    const s = scope.value
    const w = win.value
    const key = winKey()
    const my = ++seq
    const epoch = currentEpoch()
    try {
      const data = await read(readParams(s, w.iso))
      if (my !== seq || epoch !== currentEpoch()) return
      payload.value = data
      shownAt.value = { sig: s.sig, win: key, scope: s, at: Date.now() }
      error.value = null
    } catch (e) {
      if (my !== seq || epoch !== currentEpoch()) return
      error.value = e
    }
  }

  // ---- the window compared with ------------------------------------------------------
  // The same read, shifted (useWorkload comparisonRange): its totals are the
  // counts' changes, its rows the ticks on the share bars, its total the
  // dashed line. A day or a week back is not a cheap read, so it is asked
  // again only when the scope or the window changes, or every 5 minutes.
  const prev = shallowRef(null) // { data, sig, shift } | { data: null, sig, error }
  let prevSig = ''
  let prevAt = 0
  let prevSeq = 0
  async function readPrev(force = false) {
    const mode = compare.value
    if (mode === 'off') { prevSeq++; prev.value = null; prevSig = ''; return }
    const s = scope.value
    const cw = compareWindow(win.value, mode)
    if (!cw) return
    const sig = `${mode}|${s.sig}|${winKey()}`
    if (!force && sig === prevSig && Date.now() - prevAt < 300_000) return
    prevSig = sig
    prevAt = Date.now()
    const my = ++prevSeq
    const epoch = currentEpoch()
    try {
      const data = await read(readParams(s, cw.iso))
      if (my !== prevSeq || epoch !== currentEpoch()) return
      prev.value = { data, sig, shift: cw.shift }
    } catch (e) {
      if (my !== prevSeq || epoch !== currentEpoch()) return
      prev.value = { data: null, sig, error: e }
      prevSig = ''
    }
  }

  function refreshStores() {
    qs.fetchQueues({ ttlMs: 25_000 }).catch(() => {})
    gs.fetchGroups().catch(() => {})
    os.fetchOps().catch(() => {})
    Promise.resolve(ss.fetchSettings?.()).catch(() => {})
  }
  function refresh() {
    refreshStores()
    readNow()
    readPrev()
  }
  useAutoRefresh(refresh)
  refresh()
  watch(() => scope.value.sig, () => { readNow(); readPrev(true) })
  watch(winKey, () => { readNow(); readPrev(true); if (deep.value) readRetention() })
  watch(compare, () => readPrev(true))

  // ---- what the rows are -----------------------------------------------------------------
  const hasGroups = computed(() => Array.isArray(gs.groups.value) && !gs.error.value)
  const groupsError = computed(() => (gs.error.value ? describeApiError(gs.error.value) : ''))
  const groupsByQueue = computed(() => {
    const m = new Map()
    for (const g of gs.groups.value || []) {
      if (!g?.queueName) continue
      if (!m.has(g.queueName)) m.set(g.queueName, [])
      m.get(g.queueName).push(g)
    }
    return m
  })
  const queueMeta = computed(() => {
    const m = new Map()
    for (const [name, q] of qs.queueMeta.value) m.set(name, { namespace: q.namespace, task: q.task, kafkaTopic: q.kafkaTopic === true })
    return m
  })

  const shownScope = computed(() => shownAt.value.scope || scope.value)
  const listBy = computed(() => shownScope.value.listBy)
  // A pick or a window the rows on screen were not read for: they stay, dimmed,
  // until the answer lands, and say nothing about the new one meanwhile.
  const stale = computed(() => !!payload.value && (shownAt.value.sig !== scope.value.sig || shownAt.value.win !== winKey()))
  // Both cut buckets dropped: the open one on the right, and the one the
  // window began inside of on the left.
  const shown = computed(() => trimHeadBucket(trimOpenBucket(payload.value)))
  const tenant = computed(() => tenantTotals(shown.value))
  const buckets = computed(() => shown.value?.buckets || [])
  const bucketMinutes = computed(() => shown.value?.bucketMinutes || 1)
  const rows = computed(() => enrichRows(shown.value, groupsByQueue.value, queueMeta.value, shownScope.value.level, linesFor))

  let rowOrder = null
  const order = computed(() => {
    rowOrder = stableKeys(rowOrder, rows.value, `${shownAt.value.sig}|${shownAt.value.win}`)
    return rowOrder.keys
  })

  const prevShown = computed(() => {
    const p = prev.value
    if (!p || !p.data || p.sig !== `${compare.value}|${shownAt.value.sig}|${shownAt.value.win}`) return null
    return { payload: trimHeadBucket(trimOpenBucket(p.data)), shift: p.shift }
  })
  const prevRows = computed(() => (prevShown.value
    ? enrichRows(prevShown.value.payload, groupsByQueue.value, queueMeta.value, shownScope.value.level, linesFor)
    : []))
  const prevByKey = computed(() => new Map(prevRows.value.map((r) => [r.key, r])))
  const prevTenant = computed(() => (prevShown.value ? tenantTotals(prevShown.value.payload) : null))
  const prevError = computed(() => (prev.value?.error ? describeApiError(prev.value.error) : ''))

  // ---- words ------------------------------------------------------------------------------
  const words = computed(() => windowWords(win.value))
  const compareLabel = computed(() => compareWords(compare.value, win.value))
  const fetchSeen = computed(() => fetchedOf(tenant.value?.window) > 0 || fetchedOf(prevTenant.value?.window) > 0)
  const measureNow = computed(() => (measure.value === 'fetch' && !fetchSeen.value ? 'pop' : measure.value))

  // ---- what needs you -----------------------------------------------------------------------
  const found = computed(() => findingsOf(rows.value, order.value, {
    listBy: listBy.value, hasGroups: hasGroups.value, linesFor, inThe: words.value.inThe,
  }))
  const marks = computed(() => marksOf(found.value.needs))
  const errorText = computed(() => (error.value ? describeApiError(error.value) : ''))
  const verdict = computed(() => {
    if (stale.value) {
      const s = scope.value
      const what = s.key === null ? scopeName(s).toLowerCase() : s.key ? `\`${s.key}\`` : `the queues without a ${s.listBy}`
      return { tone: '', badge: '', text: `Reading ${what}…`, sub: '', list: [], loading: true }
    }
    return verdictOf({
      loaded: !!payload.value, error: errorText.value, needs: found.value.needs,
      rows: rows.value, keys: order.value, scope: shownScope.value, inThe: words.value.inThe,
    })
  })

  // ---- the parts -----------------------------------------------------------------------------
  const counts = computed(() => countsModel(tenant.value, prevTenant.value?.window || null, {
    seconds: Math.max(1, (win.value.to - win.value.from) / 1000),
  }))

  let measureOrder = null
  const measureKeys = computed(() => {
    const m = measureNow.value
    measureOrder = stableKeys(measureOrder, rows.value, `${shownAt.value.sig}|${shownAt.value.win}|${m}`, (r) => measureTotal(r, m))
    return measureOrder.keys
  })
  const charts = computed(() => workCharts(rows.value, measureKeys.value, {
    buckets: buckets.value,
    bucketMinutes: bucketMinutes.value,
    measure: measureNow.value,
    prev: prevShown.value ? { rows: prevRows.value, buckets: prevShown.value.payload?.buckets || [], shift: prevShown.value.shift } : null,
    scopeLabel: scopeName(shownScope.value),
    compareLabel: compareLabel.value,
    listBy: listBy.value,
  }))

  const table = computed(() => tableRows(rows.value, order.value, {
    listBy: listBy.value,
    hasGroups: hasGroups.value,
    linesFor,
    prev: prevShown.value ? prevByKey.value : null,
    prevTenantReads: readsOf(prevTenant.value?.window),
    tenantNow: tenant.value?.now || null,
    marks: marks.value,
  }))
  const anyFetched = computed(() => rows.value.some((r) => r.fetched > 0))

  const map = computed(() => mapModel(rows.value, order.value, {
    buckets: buckets.value, bucketMinutes: bucketMinutes.value, listBy: listBy.value,
  }))

  const partial = computed(() => missingNodes(shown.value, prevShown.value?.payload))

  // ---- the side list: every namespace (or task), fixed, a click picks one ----------------------
  const list = computed(() => workloadList({
    queues: qs.queues.value || [],
    groups: hasGroups.value ? gs.groups.value : null,
    groupBy: scope.value.listBy,
    focus: scope.value.key,
    keys: shownScope.value.level === shownScope.value.listBy && shownScope.value.by === scope.value.by ? rows.value.map((r) => r.key ?? '') : [],
    search: search?.value || '',
    linesFor,
    ops: os.ops.value,
  }))
  const listed = computed(() => qs.lastFetched.value > 0 || (qs.queues.value || []).length > 0)
  const listError = computed(() => (qs.error.value && !listed.value ? describeApiError(qs.error.value) : ''))

  // ---- the consumer groups of the queue picked -------------------------------------------------
  let groupKeys = { queue: '', keys: [] }
  const pickedRow = computed(() => (picked.value ? rows.value.find((r) => r.key === picked.value) || null : null))
  const groups = computed(() => {
    const q = picked.value
    if (!q) return null
    const m = groupsModel(groupsByQueue.value.get(q) || [], linesFor, groupKeys.queue === q ? groupKeys.keys : null)
    groupKeys = { queue: q, keys: m.keys }
    return m
  })

  // ---- the deeper layer, when asked --------------------------------------------------------------
  const slot = () => ({ data: shallowRef(null), error: ref(null), loading: ref(false) })
  const dWeek = slot()
  const dDay = slot()
  const dRet = slot()
  const dLive = slot()
  const sigs = shallowRef(new Map())
  const deepAt = ref(0)
  let deepSeq = 0

  async function load(s, fn) {
    const epoch = currentEpoch()
    const my = deepSeq
    s.loading.value = true
    try {
      const d = await fn()
      if (epoch !== currentEpoch() || my !== deepSeq) return
      s.data.value = d
      s.error.value = null
    } catch (e) {
      if (epoch !== currentEpoch() || my !== deepSeq) return
      s.error.value = e
    } finally {
      if (my === deepSeq) s.loading.value = false
    }
  }
  const isoAt = (ms) => new Date(ms).toISOString()
  function readWeek() {
    const by = scope.value.listBy
    const now = Date.now()
    return load(dWeek, async () => ({ by, payload: await read({ from: isoAt(now - 7 * DAY), to: isoAt(now), groupBy: by }) }))
  }
  function readDay() {
    const now = Date.now()
    return load(dDay, () => read({ from: isoAt(now - DAY), to: isoAt(now), groupBy: 'queue' }))
  }
  // The deeper routes shipped with /workload: until it has answered once,
  // they wait for it, so an older broker is asked about none of them.
  const probed = () => (!decided && probe ? probe.catch(() => {}) : null)
  function readRetention() {
    const w = win.value
    return load(dRet, async () => { await probed(); return (await system.getRetention({ from: w.iso.from, to: w.iso.to, groupBy: 'queue' }, QUIET)).data })
  }
  function readLive() {
    return load(dLive, async () => { await probed(); return (await getLiveness({ limit: 500 }, QUIET)).data })
  }

  const keep = computed(() => {
    const s = scope.value
    if (s.key === null) return () => true
    const meta = queueMeta.value
    return (q) => (meta.get(q)?.[s.listBy] ?? '') === s.key
  })
  const cost = computed(() => costModel(dDay.data.value, groupsByQueue.value, keep.value))
  const dead = computed(() => deadModel(cost.value.eff, sigs.value, keep.value))
  const deadQueues = computed(() => dead.value.map((r) => r.key).join('\n'))

  async function readSigs() {
    const want = dead.value.map((r) => r.key).filter((q) => !sigs.value.has(q))
    if (!want.length) return
    const epoch = currentEpoch()
    const my = deepSeq
    const got = await Promise.allSettled(want.map((queue) => getSignatures({ queue, limit: 150 }, QUIET)))
    if (epoch !== currentEpoch() || my !== deepSeq) return
    const next = new Map(sigs.value)
    got.forEach((r, i) => next.set(want[i], r.status === 'fulfilled' ? { data: r.value.data } : { error: r.reason }))
    sigs.value = next
  }

  function readDeeper() {
    deepSeq++
    sigs.value = new Map()
    deepAt.value = Date.now()
    readWeek()
    readDay().then(readSigs)
    readRetention()
    readLive()
  }
  watch(deep, (on) => { if (on && !deepAt.value) readDeeper() }, { immediate: true })
  watch(() => scope.value.listBy, () => { if (deep.value && deepAt.value) readWeek() })
  watch(deadQueues, () => { if (deep.value && deepAt.value) readSigs() })

  function resetDeeper() {
    deepSeq++
    for (const s of [dWeek, dDay, dRet, dLive]) { s.data.value = null; s.error.value = null; s.loading.value = false }
    sigs.value = new Map()
    deepAt.value = 0
  }

  const weekRead = computed(() => (dWeek.data.value && dWeek.data.value.by === scope.value.listBy ? dWeek.data.value.payload : null))
  const deeper = computed(() => {
    const weekP = weekRead.value
    const s = scope.value
    const baseline = baselineModel(weekP, { listBy: s.listBy })
    const retention = retentionModel(dRet.data.value, keep.value)
    const liveness = livenessModel(dLive.data.value, cost.value.eff, keep.value)
    const recorded = (weekP?.tenant?.series?.pop || []).filter((v) => v !== null && v !== undefined).length
    const sigList = dead.value.map((r) => sigs.value.get(r.key)?.data).filter(Boolean)
    return {
      at: deepAt.value,
      loading: dWeek.loading.value || dDay.loading.value || dRet.loading.value || dLive.loading.value,
      week: weekModel(weekP, s.key),
      weekState: state(dWeek, weekP),
      recorded,
      baseline,
      cost: cost.value,
      costState: state(dDay, dDay.data.value),
      retention,
      retentionState: state(dRet, dRet.data.value),
      liveness,
      livenessState: state(dLive, dLive.data.value),
      dead: dead.value,
      bytes: bytesModel(cost.value.eff, qs.queues.value || [], keep.value),
      notes: deeperNotes({
        eff: cost.value.eff, bucketMinutes: cost.value.bucketMinutes, baseline, sigs: sigList,
        retention: retention.rows, liveness: livenessSum(dLive.data.value, keep.value, cost.value.eff), listBy: s.listBy,
      }),
      partial: missingNodes(weekP, dDay.data.value),
    }
  })
  /** A panel's state in words: '' when it has its read. */
  function state(s, data) {
    if (data) return ''
    if (s.error.value) return isMissingRoute(s.error.value) ? 'Not available on this broker.' : `The broker did not answer: ${describeApiError(s.error.value)}.`
    return 'Reading…'
  }

  const off = onClusterChange(() => {
    seq++
    prevSeq++
    payload.value = null
    shownAt.value = { sig: '', win: '', scope: null, at: 0 }
    error.value = null
    prev.value = null
    prevSig = ''
    rowOrder = null
    measureOrder = null
    fallback.value = routeSupport.missing('workload') !== null
    decided = fallback.value
    probe = null
    resetDeeper()
  })
  onUnmounted(off)

  return {
    // reads
    fallback, payload, error, errorText, stale, shownScope, shownAt, readAt: computed(() => shownAt.value.at),
    hasGroups, groupsError, prevError, prevDrawn: computed(() => !!prevShown.value), partial,
    // the parts
    rows, order, verdict, found, counts, charts, table, anyFetched, map, fetchSeen, measureNow,
    words, compareLabel, list, listed, listError, groups, pickedRow, bucketMinutes,
    // the deeper layer
    deeper, readDeeper,
    linesFor, refresh,
    lines: computed(() => ss.lines?.value || null),
  }
}
