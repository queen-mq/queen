// Shared consumer-groups store — module-level singleton, the twin of
// queuesStore. The sidebar's marks, the Queues list's verdicts and a queue
// page's colour all read "what needs you" through composables/useAttention,
// which needs the tenant's consumer groups.
//
// THE READ IS EXPENSIVE. GET /api/v1/consumer-groups walks every queue,
// partition and cursor on the broker (reads.rs cursor_lags) and reads segment
// files for groups with a backlog: on a 1M-partition queue it is a full scan.
// So there is ONE listing for the whole app: the pages that already read it
// (the Overview, Consumer groups) publish() what they got, and a reader only
// fetches when nobody has for a whole TTL.
//
// Same rules as queuesStore: TTL + in-flight de-duplication, and TENANT KEYING
// IS A CORRECTNESS REQUIREMENT — reset on a cluster switch, and a response
// issued for a cluster we have since left is dropped.
//
//   const { groups, error, lastFetched, fetchGroups, publish } = useGroupsStore()
//   await fetchGroups()          // only if nobody read it within the TTL
//   publish(response.data)       // a page that read it anyway
import { ref } from 'vue'

import { consumers as consumersApi } from '@/api'
import { currentEpoch, onClusterChange } from '@/stores/identity'

const DEFAULT_TTL_MS = 60_000

const groups = ref(null) // null = not read yet; [] = read, none
const error = ref(null)
const lastFetched = ref(0)
let inflight = null
let inflightEpoch = -1

const fetchGroups = async ({ force = false, ttlMs = DEFAULT_TTL_MS } = {}) => {
  const fresh = groups.value !== null && Date.now() - lastFetched.value < ttlMs
  if (!force && fresh) return groups.value
  if (inflight && inflightEpoch === currentEpoch()) return inflight

  const epochAtStart = currentEpoch()
  inflightEpoch = epochAtStart
  inflight = (async () => {
    try {
      const d = (await consumersApi.list()).data
      if (epochAtStart !== currentEpoch()) return groups.value
      groups.value = Array.isArray(d) ? d : (d?.consumer_groups || [])
      error.value = null
      lastFetched.value = Date.now()
      return groups.value
    } catch (err) {
      if (epochAtStart !== currentEpoch()) return groups.value
      // The last good list stays, but `error` says it is not current: a reader
      // must treat the verdicts as unknown rather than as all-clear.
      error.value = err
      return groups.value
    } finally {
      inflight = null
    }
  })()
  return inflight
}

/** A page that read the listing itself hands it over, so nobody reads it twice. */
const publish = (payload) => {
  groups.value = Array.isArray(payload) ? payload : (payload?.consumer_groups || [])
  error.value = null
  lastFetched.value = Date.now()
}

const reset = () => {
  inflight = null
  inflightEpoch = -1
  groups.value = null
  error.value = null
  lastFetched.value = 0
}

onClusterChange(reset)

export function useGroupsStore() {
  return { groups, error, lastFetched, fetchGroups, publish, reset }
}
