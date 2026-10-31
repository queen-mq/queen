// The State page: the tenant's KV keys by namespace, the locks and semaphores
// held, and the timers of every queue. Its words and rules are in
// ./state/stateViews.js (pure); this file reads and writes.
//
// Reads, on the app's 30 s ticker:
//   GET  /api/v1/resources/kv/namespaces          the namespaces, each with its exact key count
//   POST /api/v1/resources/kv/list                the page of keys on screen, 50 by name. A POST
//                                                 because its cursor is a key, and a key in a URL
//                                                 lands in every access log on the way (§5.5)
//   POST /api/v1/resources/kv/list {queen-locks}  the permits held, and only when that namespace
//                                                 has rows: no namespace, no lock, no call
//   GET  /api/v1/timers/:queue?limit=100          one call per queue, four at a time; every tick
//                                                 on the Timers view, every tenth elsewhere. The
//                                                 queue listing's `timerRows` is always 0 on a 2.x
//                                                 broker (it does not measure timers), so this is
//                                                 the only way to know
// Writes, each shown first in a QcDialog with its call:
//   PUT /api/v1/kv/:ns/:key   DELETE /api/v1/kv/:ns/:key   DELETE /api/v1/timers/:queue/:key
import { computed, ref, shallowRef, watch } from 'vue'
import client from '@/api/client'
import { kv as kvApi, timers as timersApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { gatedVerdict } from '@/composables/useGatedVerdict'
import { kvListBody, namespaceOptions, kvRefusalText, sweeperUsageIsMeasured } from '@/composables/useKvView'
import { namespaceTotal } from '@/composables/useKvSide'
import { LOCKS_NAMESPACE, locksListBody, permitsOf } from '@/composables/useLocks'
import { timerListParams } from '@/composables/useTimers'
import { currentEpoch, useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { routeSupport } from '@/stores/routeSupport'
import { PAGE, kvPath, lockRows, lockTotals, timerRows } from './state/stateViews'

const TIMER_LIMIT = 100
const SWEEP_MAX = 500
const SWEEP_AT_ONCE = 4
const SWEEP_EVERY = 10

const byteOrder = (a, b) => (a < b ? -1 : a > b ? 1 : 0)

// Both KV reads are one route family, as on the old pages: a cell that lacks
// one lacks the other, and routeSupport remembers a 404 for the cluster epoch.
const listKv = routeSupport.guard('kv', (body, config) => kvApi.list(body, { ...config, probe: true, quiet: true }))
const listNs = routeSupport.guard('kv', (config) => kvApi.namespaces({ ...config, probe: true, quiet: true }))
const listTimers = routeSupport.guard('timers', (q, params, config) => timersApi.list(q, params, { ...config, probe: true, quiet: true }))
const peekCall = routeSupport.guard('timers', (q, k, config) => timersApi.peek(q, k, { ...config, probe: true, quiet: true }))

/** The broker's own sentence for a KV refusal, else the shared one. */
export const refusal = (e) => kvRefusalText(e) || describeApiError(e)

export function useStatePage({ view, nsAsked }) {
  const qs = useQueuesStore()
  const { epoch, can } = useIdentity()
  const canWrite = computed(() => can('produce') || can('consume'))

  // ---- the namespaces ------------------------------------------------------
  const nsData = shallowRef(null) // [{namespace, keys}], the broker's byte order
  const nsError = ref(null)
  const kvVerdict = ref(null)     // absent | gated | paused: KV is not served here
  async function loadNamespaces() {
    const at = currentEpoch()
    try {
      const r = await listNs()
      if (at !== currentEpoch()) return
      nsData.value = namespaceOptions(r.data)
      nsError.value = null
      if (kvVerdict.value === 'paused') kvVerdict.value = null
    } catch (e) {
      if (at !== currentEpoch()) return
      const v = gatedVerdict(e)
      if (v !== 'transient') kvVerdict.value = v
      nsError.value = refusal(e)
    }
  }
  /** The namespace on the right: the one in the address, else the first by name. */
  const ns = computed(() => nsAsked.value || nsData.value?.[0]?.namespace || '')
  const listed = computed(() => !nsData.value || nsData.value.some((o) => o.namespace === ns.value))

  // ---- the page of keys ----------------------------------------------------
  const prefix = ref('')
  const after = ref(null)
  const pageNo = ref(0)
  const keys = shallowRef(null)
  const keysError = ref(null)
  const keysLoading = ref(false)
  const askKey = computed(() => JSON.stringify([ns.value, prefix.value, after.value]))
  let keysSeq = 0
  async function loadKeys() {
    if (view.value !== 'keys' || !ns.value) return
    if (kvVerdict.value && kvVerdict.value !== 'paused') return
    const at = currentEpoch()
    const seq = ++keysSeq
    const ask = askKey.value
    const body = kvListBody({ namespace: ns.value, prefix: prefix.value, after: after.value, limit: PAGE })
    keysLoading.value = true
    try {
      const r = await listKv(body)
      if (at !== currentEpoch() || seq !== keysSeq) return
      keys.value = { ask, body, rows: r.data?.rows || [], truncated: r.data?.truncated === true, nextAfter: r.data?.nextAfter ?? null, bytes: r.data?.bytes ?? null, at: Date.now() }
      keysError.value = null
      if (kvVerdict.value === 'paused') kvVerdict.value = null
    } catch (e) {
      if (at !== currentEpoch() || seq !== keysSeq) return
      const v = gatedVerdict(e)
      if (v !== 'transient') kvVerdict.value = v
      keysError.value = refusal(e)
    } finally {
      if (seq === keysSeq) keysLoading.value = false
    }
  }
  /** The rows on screen, only under the query they answer. */
  const keysPage = computed(() => (keys.value && keys.value.ask === askKey.value ? keys.value : null))
  function nextPage() {
    const p = keysPage.value
    if (!p?.truncated || !p.nextAfter) return
    after.value = p.nextAfter
    pageNo.value += 1
  }
  function firstPage() { after.value = null; pageNo.value = 0 }
  // A new namespace opens whole, and a new prefix starts a new walk; both are
  // flushed before the load below, so it asks once.
  watch(ns, () => { prefix.value = ''; after.value = null; pageNo.value = 0 }, { flush: 'sync' })
  watch(prefix, () => { after.value = null; pageNo.value = 0 }, { flush: 'sync' })
  watch(askKey, () => loadKeys())

  // ---- locks and semaphores ------------------------------------------------
  const locks = shallowRef(null) // {permits, truncated, absent}
  const locksError = ref(null)
  async function loadLocks() {
    if (kvVerdict.value && kvVerdict.value !== 'paused') return
    if (!nsData.value) return
    const at = currentEpoch()
    // No row in queen-locks: nothing is held, and there is nothing to ask.
    if (!nsData.value.some((o) => o.namespace === LOCKS_NAMESPACE && (o.keys == null || o.keys > 0))) {
      locks.value = { permits: [], truncated: false, absent: true }
      locksError.value = null
      return
    }
    try {
      const r = await listKv(locksListBody({ limit: 1000 }))
      if (at !== currentEpoch()) return
      locks.value = { permits: permitsOf(r.data?.rows), truncated: r.data?.truncated === true, absent: false }
      locksError.value = null
    } catch (e) {
      if (at !== currentEpoch()) return
      const v = gatedVerdict(e)
      if (v !== 'transient') kvVerdict.value = v
      locksError.value = refusal(e)
    }
  }
  const lockList = computed(() => (locks.value ? lockRows(locks.value.permits) : null))

  // ---- timers: one read per queue -------------------------------------------
  const sweep = shallowRef(null) // the last complete sweep {results, at, total, read}
  const sweeping = ref(false)
  const sweepDone = ref(0)
  const sweepTotal = ref(0)
  const timersVerdict = ref(null)
  let sweepEpoch = -1
  async function sweepTimers() {
    const at = currentEpoch()
    if (sweeping.value && sweepEpoch === at) return
    if (timersVerdict.value && timersVerdict.value !== 'paused') return
    if (!qs.lastFetched.value) return // the queue listing first
    const names = [...new Set((qs.queues.value || []).map((q) => q.name || q.queue).filter(Boolean))].sort(byteOrder)
    const list = names.slice(0, SWEEP_MAX)
    sweepEpoch = at
    sweeping.value = true
    sweepDone.value = 0
    sweepTotal.value = list.length
    const results = []
    let stop = null
    let i = 0
    const worker = async () => {
      while (i < list.length && !stop && at === currentEpoch()) {
        const q = list[i++]
        try {
          const r = await listTimers(q, timerListParams(undefined, TIMER_LIMIT))
          results.push({ queue: q, rows: r.data?.rows || [], truncated: r.data?.truncated === true })
        } catch (e) {
          const v = gatedVerdict(e)
          // The family is not here (or not in the plan): one answer for every queue.
          if (v === 'absent' || v === 'gated') stop = v
          else results.push({ queue: q, error: describeApiError(e) })
        }
        sweepDone.value += 1
      }
    }
    await Promise.all(Array.from({ length: Math.min(SWEEP_AT_ONCE, list.length) }, worker))
    if (at !== currentEpoch()) return
    sweeping.value = false
    if (stop) { timersVerdict.value = stop; return }
    timersVerdict.value = null
    sweep.value = { results, at: Date.now(), total: names.length, read: list.length }
  }
  const timerList = computed(() => (sweep.value ? timerRows(sweep.value.results) : null))

  /** One queue again (after a cancel), patched into the last sweep. */
  async function rereadQueue(q) {
    const at = currentEpoch()
    try {
      const r = await listTimers(q, timerListParams(undefined, TIMER_LIMIT))
      if (at !== currentEpoch() || !sweep.value) return
      const results = sweep.value.results.filter((x) => x.queue !== q)
      results.push({ queue: q, rows: r.data?.rows || [], truncated: r.data?.truncated === true })
      sweep.value = { ...sweep.value, results }
    } catch { /* the next sweep says */ }
  }

  async function peekTimer(queue, key) {
    const r = await peekCall(queue, key)
    return r.data
  }

  // ---- the summary's figures -------------------------------------------------
  const kvOff = computed(() => kvVerdict.value === 'absent' || kvVerdict.value === 'gated')
  const kvTotals = computed(() => {
    if (kvOff.value) return { unavailable: true }
    const opts = nsData.value
    if (!opts && qs.kvRows.value == null) return null
    // The listing's root is the source (kvRows, kvBytes); the namespace list is
    // the witness that a cached zero is not a measurement (useKvView.js).
    const measured = sweeperUsageIsMeasured(qs.kvRows.value, opts || [])
    const keysN = measured ? qs.kvRows.value : namespaceTotal(opts || [])
    return { keys: keysN, bytes: measured ? qs.kvBytes.value : null, namespaces: opts ? opts.length : null }
  })
  const lockSum = computed(() => (kvOff.value ? { unavailable: true } : lockList.value ? lockTotals(lockList.value.rows) : null))
  const timerSum = computed(() => {
    if (timersVerdict.value === 'absent' || timersVerdict.value === 'gated') return { unavailable: true }
    const t = timerList.value
    if (!t) return { reading: true, total: sweepTotal.value || (qs.queues.value || []).length }
    return { count: t.count, queues: t.queues, more: t.more, reading: false }
  })

  // ---- writes ------------------------------------------------------------
  const putKey = (nsName, key, body) => kvApi.put(nsName, key, body, { quiet: true })
  const deleteKey = (nsName, key, body) => client.delete(kvPath(nsName, key), body ? { data: body, quiet: true } : { quiet: true })
  const cancelTimer = (queue, key) => timersApi.cancel(queue, key, { quiet: true })
  async function afterKvWrite() {
    qs.invalidate?.()
    await loadNamespaces()
    await loadKeys()
    loadLocks()
    qs.fetchQueues({ force: true }).catch(() => {})
  }

  // ---- the ticker, the views, a cluster switch ---------------------------------
  let ticks = 0
  async function tick() {
    ticks += 1
    await loadNamespaces()
    loadKeys()
    loadLocks()
    if (view.value === 'timers' || ticks % SWEEP_EVERY === 0) sweepTimers()
  }
  useAutoRefresh(tick)
  // The first page of keys: now when the address names the namespace, else
  // when the namespaces land and the first one becomes `ns` (the watch above).
  if (ns.value) loadKeys()
  loadNamespaces().then(() => loadLocks())
  watch(() => qs.lastFetched.value, (t) => { if (t && !sweep.value) sweepTimers() }, { immediate: true })
  watch(view, (v) => {
    if (v === 'keys') loadKeys()
    if (v === 'locks') loadLocks()
    if (v === 'timers' && (!sweep.value || Date.now() - sweep.value.at > 30_000)) sweepTimers()
  })
  watch(epoch, () => {
    nsData.value = null; nsError.value = null; kvVerdict.value = null
    keys.value = null; keysError.value = null; after.value = null; pageNo.value = 0; prefix.value = ''
    locks.value = null; locksError.value = null
    sweep.value = null; sweeping.value = false; timersVerdict.value = null
  })

  return {
    canWrite,
    nsData, nsError, kvVerdict, ns, listed, loadNamespaces,
    prefix, pageNo, keysPage, keysError, keysLoading, nextPage, firstPage, loadKeys,
    locks, locksError, lockList, loadLocks,
    sweep, sweeping, sweepDone, sweepTotal, timersVerdict, timerList, sweepTimers, rereadQueue, peekTimer,
    kvTotals, lockSum, timerSum,
    putKey, deleteKey, cancelTimer, afterKvWrite,
  }
}
