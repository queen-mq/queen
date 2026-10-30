// The last hour's work per queue — its acks, failed and not, and its dead
// letters — for the app's one rule of what needs you (composables/
// useAttention): a queue whose acks fail is a queue that needs you, and the
// queue listing carries no window to tell.
//
// One read for the whole app, GET /api/v1/analytics/workload?groupBy=queue
// (the hour, one-minute buckets). It answers a series per queue; only each
// queue's totals are kept. Same rules as groupsStore: TTL, in-flight
// de-duplication, and tenant keying (reset on a cluster switch, a response
// for a cluster we have since left is dropped).
//
//   const { ops, fetchOps } = useQueueOpsStore()
//   await fetchOps()             // only if nobody read it within the TTL
//   ops.value?.get('orders')     // { ackFailed, ackSuccess, popMessages, … } | undefined
//
// A broker without the route (older than the Workload page) answers 404:
// `ops` stays null and the rule judges queues without their acks.
import { ref, shallowRef } from 'vue'

import { system } from '@/api'
import { opsByQueue } from '@/composables/queueOps'
import { currentEpoch, onClusterChange } from '@/stores/identity'

const DEFAULT_TTL_MS = 30_000

const ops = shallowRef(null) // null = not read (or not served); Map name → totals
const error = ref(null)
const lastFetched = ref(0)
let inflight = null
let inflightEpoch = -1

const fetchOps = async ({ force = false, ttlMs = DEFAULT_TTL_MS } = {}) => {
  const fresh = lastFetched.value > 0 && Date.now() - lastFetched.value < ttlMs
  if (!force && fresh) return ops.value
  if (inflight && inflightEpoch === currentEpoch()) return inflight

  const epochAtStart = currentEpoch()
  inflightEpoch = epochAtStart
  inflight = (async () => {
    try {
      const d = (await system.getWorkload({ groupBy: 'queue' }, { probe: true, quiet: true })).data
      if (epochAtStart !== currentEpoch()) return ops.value
      ops.value = opsByQueue(d)
      error.value = null
      return ops.value
    } catch (err) {
      if (epochAtStart !== currentEpoch()) return ops.value
      // The last good totals stay; a 404 (no route on this broker) leaves null.
      error.value = err
      return ops.value
    } finally {
      // A failed read also waits out the TTL: it is not retried on every tick.
      lastFetched.value = Date.now()
      inflight = null
    }
  })()
  return inflight
}

const reset = () => {
  inflight = null
  inflightEpoch = -1
  ops.value = null
  error.value = null
  lastFetched.value = 0
}

onClusterChange(reset)

export function useQueueOpsStore() {
  return { ops, error, lastFetched, fetchOps, reset }
}
