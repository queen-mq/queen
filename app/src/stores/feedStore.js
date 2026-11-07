// Shared feed streams store — module-level singleton, like queuesStore.
//
// What every feed stream (Stream.fromFeed) keeps in KV, read once for the
// whole console. The Streams page lists the streams; every page with a queue
// list asks which feed streams read a queue (queuesStore joins this in as
// `feedReaders`), because a feed stream reads without a consumer group, and
// without it a queue it reads looks like one nobody reads.
//
// Reads (console/streams/feedStreams.js says what each key holds):
//   POST /api/v1/resources/kv/list {queen-feed, keysOnly}   the keys, a page of
//        1,000 at a time, a snapshot's chunks skipped in one jump
//   GET  /api/v1/kv/queen-feed/<key>   each record, description and layout,
//        again only when its version moved
//   POST /api/v1/resources/kv/list {queen-locks, prefix queen-feed/}   who runs them
// A broker without the KV listing, or a tenant whose plan has no KV, answers
// 404/403: no feed stream can run there, and the list is empty.
//
// TTL-cached and de-duplicated like queuesStore; reset on a cluster switch
// (the same cross-tenant reason queuesStore states).
//
// API:
//   const { rows, exact, error, lastFetched, fetchFeed } = useFeedStore()
//   await fetchFeed()                 // the cache when fresh
//   await fetchFeed({ force: true })  // always read
import { ref, shallowRef } from 'vue'

import { kv as kvApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { locksListBody, permitsOf } from '@/composables/useLocks'
import { readSinceRefresh } from '@/composables/useRefresh'
import { isMissingRoute } from '@/stores/routeSupport'
import { currentEpoch, onClusterChange } from '@/stores/identity'
import {
  FEED_LOCK_PREFIX, FEED_NAMESPACE, feedKeysToRead, feedStreamRows, scanFeedKeys,
} from '@/console/streams/feedStreams'

const DEFAULT_TTL_MS = 30_000
// Values read at once: a stream in many shards is one GET per shard.
const READS_AT_ONCE = 6

const rows = shallowRef([])
const exact = ref(true)
const error = ref(null)
const lastFetched = ref(0)
// Each key's value by the version it was read at.
const values = new Map()
let inflight = null
let inflightEpoch = -1

async function inBatches(items, n, fn) {
  for (let i = 0; i < items.length; i += n) await Promise.all(items.slice(i, i + n).map(fn))
}

async function load(epoch) {
  const quiet = { quiet: true, probe: true }
  try {
    const { keys, exact: scanned } = await scanFeedKeys(async (body) => (await kvApi.list(body, quiet)).data)
    if (epoch !== currentEpoch()) return
    // The descriptions and layouts move rarely, the records at every commit.
    const stale = feedKeysToRead(keys).filter((k) => {
      const v = keys.get(k)?.version
      return v == null || values.get(k)?.version !== v
    })
    await inBatches(stale, READS_AT_ONCE, async (k) => {
      const row = (await kvApi.get(FEED_NAMESPACE, k, quiet)).data
      if (row && row.found) values.set(k, { version: row.version ?? keys.get(k)?.version ?? null, value: row.value })
      else values.delete(k)
    })
    for (const k of [...values.keys()]) if (!keys.has(k)) values.delete(k)
    // Always asked: a runner that has not committed yet has its lock and no key.
    const permits = permitsOf((await kvApi.list(locksListBody({ prefix: FEED_LOCK_PREFIX, limit: 1000 }), quiet)).data?.rows)
    if (epoch !== currentEpoch()) return
    const vals = new Map([...values.entries()].map(([k, v]) => [k, v.value]))
    rows.value = feedStreamRows({ keys, values: vals, permits, now: Date.now() })
    exact.value = scanned
    error.value = null
    lastFetched.value = Date.now()
  } catch (e) {
    if (epoch !== currentEpoch()) return
    const status = e?.status ?? e?.response?.status
    if (isMissingRoute(e) || status === 404 || status === 405 || status === 403) {
      rows.value = []
      exact.value = true
      error.value = null
      lastFetched.value = Date.now()
    } else {
      error.value = describeApiError(e)
    }
  }
}

function fetchFeed({ force = false, ttlMs = DEFAULT_TTL_MS } = {}) {
  const fresh = lastFetched.value > 0 && Date.now() - lastFetched.value < ttlMs && readSinceRefresh(lastFetched.value)
  if (!force && fresh) return Promise.resolve(rows.value)
  if (inflight && inflightEpoch === currentEpoch()) return inflight
  const epoch = currentEpoch()
  inflightEpoch = epoch
  inflight = load(epoch)
    .then(() => rows.value)
    .finally(() => { inflight = null })
  return inflight
}

/** Full wipe on a cluster switch: another cluster's streams under this one's name would be a lie. */
function reset() {
  inflight = null
  inflightEpoch = -1
  rows.value = []
  exact.value = true
  error.value = null
  lastFetched.value = 0
  values.clear()
}

onClusterChange(reset)

export function useFeedStore() {
  return { rows, exact, error, lastFetched, fetchFeed, reset }
}
