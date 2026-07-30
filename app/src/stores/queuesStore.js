// Shared queues store — module-level singleton.
//
// Multiple views (Queues, Consumers, Dashboard, …) all need the queue list
// and its derived data (namespaces, tasks, name → meta map). Fetching it
// once and sharing the result across views makes the typical session
// dramatically lighter on the network: switching between Queues and
// Consumers never refetches when the data is fresh, and the namespace
// filter on Consumers piggybacks on what Queues just loaded.
//
// Three things this store does that a per-view fetch can't:
//
//   1. TTL caching. fetchQueues() short-circuits if data was loaded less
//      than `ttlMs` ago. Force refresh with { force: true }.
//   2. In-flight de-duplication. If two views mount at the same time
//      and both call fetchQueues(), only one HTTP request is issued; the
//      second caller awaits the same promise.
//   3. Derived computeds (queueMeta, namespaces, tasks) live on the store
//      so each view doesn't have to roll its own.
//
// TENANT KEYING IS A CORRECTNESS REQUIREMENT, NOT AN OPTIMISATION. A single
// process-global cache with no tenant key serves the previous tenant's queues
// under the new tenant's name after a cluster switch — a cross-tenant lie in
// the UI even though the backend scoped every call correctly. So: reset on
// switch, and discard any response that was issued for a cluster we have
// since left.
//
// API:
//   const { queues, loading, error, lastFetched, fetchQueues, queueMeta,
//           namespaces, tasks, isFresh, invalidate, reset } = useQueuesStore()
//
//   await fetchQueues()                // use cache if fresh, else fetch
//   await fetchQueues({ force: true }) // always fetch (e.g. after mutation)
//   invalidate()                       // mark cache stale; next fetch refreshes
import { ref, computed } from 'vue'

import { queues as queuesApi } from '@/api'
import { currentEpoch, onClusterChange } from '@/stores/identity'

// Default cache TTL — long enough that incidental view-switching is free,
// short enough that a stale list doesn't deceive the operator. Nothing
// force-refreshes it in the background; a view that needs live data registers
// with useAutoRefresh and passes { force: true }.
const DEFAULT_TTL_MS = 30_000

// ---------------------------------------------------------------------------
// Module-level singletons. Every useQueuesStore() call shares these refs.
// ---------------------------------------------------------------------------
const queues = ref([])
const loading = ref(false)
const error = ref(null)
const lastFetched = ref(0)
let inflight = null  // pending Promise, used for de-duplication
let inflightEpoch = -1

// ---------------------------------------------------------------------------
// Fetch with TTL + de-duplication.
// ---------------------------------------------------------------------------
const fetchQueues = async ({ force = false, ttlMs = DEFAULT_TTL_MS } = {}) => {
  const fresh = queues.value.length > 0 && Date.now() - lastFetched.value < ttlMs
  if (!force && fresh) return queues.value
  if (inflight && inflightEpoch === currentEpoch()) return inflight

  loading.value = true
  error.value = null
  const epochAtStart = currentEpoch()
  inflightEpoch = epochAtStart

  inflight = (async () => {
    try {
      const r = await queuesApi.list()
      // The switch happened while this was in flight: these rows belong to
      // the cluster we left. Dropping them is the whole point of the epoch.
      if (epochAtStart !== currentEpoch()) return queues.value
      const all = r.data?.queues || r.data || []
      // Filter out queues with empty/whitespace names — these are stale
      // entries from the partition_lookup table that should be cleaned
      // up server-side but show up in the API response today.
      queues.value = all.filter(q => q.name && q.name.trim() !== '')
      lastFetched.value = Date.now()
      return queues.value
    } catch (err) {
      // Keep last-good data so the UI doesn't blank out — but `error` and
      // `lastFetched` are exported so a view can dim the grid and say how old
      // this is. Stale data presented as live is the failure mode here.
      error.value = err
      return queues.value
    } finally {
      loading.value = false
      inflight = null
    }
  })()
  return inflight
}

const invalidate = () => { lastFetched.value = 0 }

/** Full wipe: nothing from the previous cluster survives, in-flight included. */
const reset = () => {
  inflight = null
  inflightEpoch = -1
  queues.value = []
  error.value = null
  lastFetched.value = 0
  loading.value = false
}

onClusterChange(reset)

// ---------------------------------------------------------------------------
// Derived data — computed once on the module level so every view sees the
// same Maps without re-deriving on each render.
// ---------------------------------------------------------------------------
const queueMeta = computed(() => {
  const m = new Map()
  for (const q of queues.value) m.set(q.name, q)
  return m
})

const namespaces = computed(() => {
  const set = new Set(
    queues.value.map(q => q.namespace).filter(ns => ns !== undefined)
  )
  return [...set].sort()
})

const tasks = computed(() => {
  const set = new Set(
    queues.value.map(q => q.task).filter(t => t !== undefined)
  )
  return [...set].sort()
})

const isFresh = computed(() =>
  queues.value.length > 0 && Date.now() - lastFetched.value < DEFAULT_TTL_MS
)

/** Last load failed and what is on screen predates it. */
const isStale = computed(() => error.value !== null && lastFetched.value > 0)

// ---------------------------------------------------------------------------
// Public composable. The same singleton refs are returned to every caller,
// so reactivity propagates across views naturally.
// ---------------------------------------------------------------------------
export function useQueuesStore() {
  return {
    queues,
    loading,
    error,
    lastFetched,
    isFresh,
    isStale,
    queueMeta,
    namespaces,
    tasks,
    fetchQueues,
    invalidate,
    reset,
  }
}
