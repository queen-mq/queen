// The Workers view's reads: the old Supervisors page's (views/Supervisors.vue),
// without its drawing.
//
//   POST /api/v1/resources/kv/list  {namespace: 'queen-supervisor', limit: 100,
//        includeExpired: true, prefix?: '<group>/', after?}
//     One bounded page per call; readSupervisorPage keeps whole slots on a page
//     and leaves a torn trailing one for the next. A refresh re-reads as many
//     pages as were loaded, never more, so opening the view on a big deployment
//     is not an unbounded scan; "Read more" asks for the next page. A POST
//     because its cursor is a key (api/index.js); the broker and the proxy class
//     it a read.
//   The shared stores the Queues section already refreshes on the same tick:
//     the queue listing (what waits) and the last hour per queue (what came).
//
// On the app's 30 s ticker (useAutoRefresh). A stable refusal (the route is not
// served, the plan has no KV, the cell answers 503 for the family) stops the
// ticker's reads until "Check again".
import { computed, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { kv } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { gatedVerdict } from '@/composables/useGatedVerdict'
import { kvRefusalText } from '@/composables/useKvView'
import { readSupervisorPage, supervisorObservations } from '@/composables/supervisorStatus'
import { supervisorGroups } from '@/composables/supervisorGroups'
import { useIdentity } from '@/stores/identity'
import { routeSupport } from '@/stores/routeSupport'
import { NAMESPACE } from './workersView'

export function useWorkers() {
  const { epoch } = useIdentity()
  const namespace = ref(NAMESPACE)
  const group = ref('')
  const entries = shallowRef([])
  const after = ref(null)
  const readAt = ref(null)
  const loading = ref(false)
  const error = shallowRef(null)
  const verdict = ref(null)
  // quiet: the view says a failed read in its own line, so the old toast host stays silent.
  const list = routeSupport.guard('kv', async (body, config) => (await kv.list(body, { ...config, quiet: true })).data)

  let controller = null
  let sequence = 0
  let pagesLoaded = 1
  async function read(append = false) {
    const turn = ++sequence
    const askedEpoch = epoch.value
    controller?.abort()
    controller = new AbortController()
    const signal = controller.signal
    loading.value = true
    try {
      let cursor = append ? after.value : null
      const next = new Map(append ? entries.value.map((row) => [row.slot, row]) : [])
      const calls = append ? 1 : pagesLoaded
      let count = 0
      for (; count < calls; count++) {
        const result = await readSupervisorPage(list, { namespace: namespace.value, group: group.value, after: cursor, signal })
        if (turn !== sequence || askedEpoch !== epoch.value) return
        for (const entry of result.entries) next.set(entry.slot, entry)
        cursor = result.after
        if (!cursor) { count++; break }
      }
      entries.value = [...next.values()]
      after.value = cursor
      pagesLoaded = append ? pagesLoaded + count : count
      readAt.value = Date.now()
      error.value = null
      verdict.value = null
    } catch (failure) {
      if (turn === sequence && askedEpoch === epoch.value && !signal.aborted) {
        error.value = failure
        verdict.value = gatedVerdict(failure)
      }
    } finally {
      if (turn === sequence) loading.value = false
    }
  }
  function refresh(explicit = false) {
    if (loading.value || (verdict.value && verdict.value !== 'transient' && !explicit)) return
    read()
  }
  function readMore() { if (!loading.value && after.value) read(true) }
  function checkAgain() { routeSupport.forget('kv'); verdict.value = null; refresh(true) }
  function clear() {
    sequence++
    controller?.abort()
    entries.value = []; after.value = null; readAt.value = null
    error.value = null; verdict.value = null; pagesLoaded = 1; loading.value = false
  }
  /** Read another namespace, or one publication group of it ('' for every group). */
  function setSource(ns, g) {
    clear()
    namespace.value = ns
    group.value = g
    read()
  }
  watch(epoch, () => { clear(); read() })
  read()
  useAutoRefresh(() => refresh())
  onBeforeUnmount(() => { sequence++; controller?.abort() })

  // Ages are at the read, by this browser's clock, as on the old page. After a
  // failed refresh the last read stays as it was, and the view says so once.
  const rows = computed(() => supervisorObservations(entries.value, readAt.value || Date.now(), false))
  const groups = computed(() => supervisorGroups(rows.value, { partial: Boolean(after.value) }))
  const loaded = computed(() => readAt.value != null)
  const failure = computed(() => (error.value ? kvRefusalText(error.value) || describeApiError(error.value) : null))
  const stable = computed(() => (verdict.value && verdict.value !== 'transient' ? verdict.value : null))

  return {
    namespace, group, readAt, loading, loaded, error, failure, stable, partial: computed(() => Boolean(after.value)),
    rows, groups, refresh, readMore, checkAgain, setSource,
  }
}
