// The Dead letters page: why the tenant's dead letters failed, cause by cause,
// across its queues (or one queue's, with ?queue=), and the picked cause's
// newest rows.
//
// Reads, on the app's 30 s ticker:
//   store: GET /api/v1/resources/queues            which queues hold dead letters
//   GET /api/v1/analytics/dlq-signatures?queue=    each of them, at most once a minute
//   GET /api/v1/dlq?queue=&limit=                  a queue with several causes, to tell
//                                                  them apart (at most once a minute),
//                                                  and the picked cause's queue, its samples
// Replay, discard and download read the rows they act on again, just before.
import { computed, onScopeDispose, ref, shallowRef, watch } from 'vue'
import { dlq as dlqApi, system } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { currentEpoch, onClusterChange } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { buildCauses, mergeSeen, rankNew, rowsOf, SAMPLE } from './deadletters/dlCauses'

const SIG_MS = 60_000 // causes: at most once a minute, as the queue page reads them
const ROWS_MS = 30_000 // the picked queue's samples
export const SAMPLES = 8 // shown for a cause

/** Run `fn` over `items`, `n` at a time. */
async function each(items, fn, n = 4) {
  const queue = [...items]
  const run = async () => { while (queue.length) await fn(queue.shift()) }
  await Promise.all(Array.from({ length: Math.min(n, queue.length) }, run))
}

export function useDeadLetters(focus) {
  const qs = useQueuesStore()

  const sigs = shallowRef(new Map()) // queue → { data, error, at }
  const read = shallowRef(new Map()) // queue → { messages, total, limit, at, error }
  const seen = shallowRef(new Map()) // key → cause, every cause seen this visit
  const ranks = shallowRef(new Map()) // key → its place in the list, given once
  const ready = ref(false)
  const failed = ref(null)
  const pickedKey = ref(null)
  const inflight = new Map()
  let epochAt = currentEpoch()

  function reset() {
    sigs.value = new Map(); read.value = new Map(); seen.value = new Map(); ranks.value = new Map()
    ready.value = false; failed.value = null; pickedKey.value = null
    inflight.clear()
    epochAt = currentEpoch()
  }
  onScopeDispose(onClusterChange(reset))

  const put = (store, k, v) => { const m = new Map(store.value); m.set(k, v); store.value = m }

  // Every queue of the listing, and the ones holding dead letters, by name.
  const listing = computed(() => (qs.queues.value || []).map((q) => ({
    name: q.name,
    dead: Number(q.messages?.deadLetter) || 0,
    pending: Number(q.messages?.pending) || 0,
  })))
  const deadQueues = computed(() => listing.value.filter((q) => q.dead > 0).sort((a, b) => a.name.localeCompare(b.name)))
  const shownQueues = computed(() => (focus.value ? deadQueues.value.filter((q) => q.name === focus.value) : deadQueues.value))
  const queueRow = (name) => listing.value.find((q) => q.name === name) || null
  const total = computed(() => shownQueues.value.reduce((a, q) => a + q.dead, 0))

  // ---- the list: every cause seen, in the place it was first given ----
  const causes = computed(() => {
    const r = ranks.value
    return [...seen.value.values()]
      .filter((c) => !focus.value || c.queue === focus.value)
      .sort((a, b) => (r.get(a.key) ?? 1e9) - (r.get(b.key) ?? 1e9))
  })
  const picked = computed(() => causes.value.find((c) => c.key === pickedKey.value)
    || causes.value.find((c) => !c.gone)
    || causes.value[0]
    || null)
  function pick(key) { pickedKey.value = key }

  // How many rows a queue needs read: none for one cause (the sample says
  // when), enough to tell several apart (the sample's rows).
  function kindsOf(name) {
    const d = sigs.value.get(name)?.data
    if (!d) return 0
    const sum = (d.signatures || []).reduce((a, s) => a + (Number(s.n) || 0), 0)
    return (d.signatures || []).length + ((Number(d.sample) || 0) > sum ? 1 : 0)
  }
  function rowsNeeded(name, forSamples) {
    const d = sigs.value.get(name)?.data
    if (kindsOf(name) > 1) return Math.min(SAMPLE, Math.max(SAMPLES, Number(d?.sample) || SAMPLES))
    return forSamples ? SAMPLES : 0
  }

  async function readSig(name, force, epoch) {
    const cur = sigs.value.get(name)
    if (!force && cur?.at && Date.now() - cur.at < SIG_MS) return
    try {
      const r = await system.getDlqSignatures({ queue: name }, { quiet: true, probe: true })
      if (epoch === currentEpoch()) put(sigs, name, { data: r.data, error: null, at: Date.now() })
    } catch (e) {
      if (epoch === currentEpoch()) put(sigs, name, { data: cur?.data || null, error: describeApiError(e), at: Date.now() })
    }
  }
  function readRows(name, limit, maxAge, force, epoch) {
    if (!limit) return Promise.resolve()
    const cur = read.value.get(name)
    if (!force && cur && cur.limit >= limit && Date.now() - cur.at < maxAge) return Promise.resolve()
    const key = `${name}|${limit}`
    if (inflight.has(key)) return inflight.get(key)
    const p = (async () => {
      try {
        const r = await dlqApi.list({ queue: name, limit }, { quiet: true })
        if (epoch !== currentEpoch()) return
        put(read, name, { messages: r.data?.messages || [], total: r.data?.total ?? null, limit, at: Date.now(), error: null })
      } catch (e) {
        if (epoch === currentEpoch()) put(read, name, { ...(cur || { messages: [], limit: 0 }), at: Date.now(), error: describeApiError(e) })
      }
    })().finally(() => inflight.delete(key))
    inflight.set(key, p)
    return p
  }

  function rebuild() {
    const fresh = buildCauses(shownQueues.value, sigs.value, read.value)
    const dead = new Set(deadQueues.value.map((q) => q.name))
    // One queue's read says nothing about the others; every queue's read does.
    const scope = focus.value ? new Set([focus.value]) : null
    seen.value = mergeSeen(seen.value, fresh, sigs.value, dead, scope)
    ranks.value = rankNew(ranks.value, [...seen.value.values()], Date.now())
  }
  const haveAll = () => shownQueues.value.every((q) => sigs.value.has(q.name))

  // One read at a time; a call during one runs again after it (forced if any
  // caller forced), so a new ?queue= or an action is never answered by a read
  // that started before it.
  let running = null
  let again = null // null | 'soft' | 'force'
  function refresh(force = false) {
    if (epochAt !== currentEpoch()) reset()
    if (running) {
      again = force || again === 'force' ? 'force' : 'soft'
      return running
    }
    const epoch = currentEpoch()
    const p = (async () => {
      await qs.fetchQueues(force ? { force: true } : { ttlMs: 25_000 })
      if (epoch !== currentEpoch()) return
      failed.value = qs.error.value && !qs.queues.value?.length ? describeApiError(qs.error.value) : null
      const list = shownQueues.value
      await each(list, (q) => readSig(q.name, force, epoch))
      if (epoch !== currentEpoch()) return
      const multi = list.filter((q) => kindsOf(q.name) > 1)
      await each(multi, (q) => readRows(q.name, rowsNeeded(q.name, false), SIG_MS, force, epoch))
      if (epoch !== currentEpoch()) return
      rebuild()
      if (haveAll()) ready.value = true
      const c = picked.value
      if (c && !c.gone) {
        await readRows(c.queue, rowsNeeded(c.queue, true), ROWS_MS, force, epoch)
        if (epoch === currentEpoch()) rebuild()
      }
    })().finally(() => {
      running = null
      if (again) { const f = again === 'force'; again = null; refresh(f) }
    })
    running = p
    return p
  }
  useAutoRefresh(() => refresh())
  refresh()

  // Another queue, or all of them: a new view, ranked afresh, without a blank
  // when its causes were read already.
  watch(focus, () => {
    pickedKey.value = null
    ranks.value = new Map()
    if (haveAll()) rebuild()
    else ready.value = false
    refresh()
  })
  // A newly picked cause reads its queue's newest rows now, not on the next tick.
  watch(() => picked.value?.queue, (q) => {
    if (!q || !ready.value) return
    const epoch = currentEpoch()
    readRows(q, rowsNeeded(q, true), ROWS_MS, false, epoch).then(() => { if (epoch === currentEpoch()) rebuild() })
  })

  const samples = computed(() => {
    const c = picked.value
    if (!c) return []
    return rowsOf(c, read.value.get(c.queue)?.messages || []).slice(0, SAMPLES)
  })
  const samplesRead = computed(() => {
    const c = picked.value
    const r = c ? read.value.get(c.queue) : null
    return r ? { limit: r.limit, error: r.error, at: r.at } : null
  })

  /** The rows of `cause` among its queue's newest 200, read now: what an action acts on. */
  async function freshRows(cause) {
    const r = await dlqApi.list({ queue: cause.queue, limit: SAMPLE }, { quiet: true })
    const rows = r.data?.messages || []
    return { rows: rowsOf(cause, rows), total: r.data?.total ?? null }
  }

  return {
    ready, failed, causes, picked, pick, samples, samplesRead,
    deadQueues, shownQueues, queueRow, total, refresh, freshRows,
  }
}
