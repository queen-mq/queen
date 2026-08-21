// Shared ephemeral-queues store — module-level singleton, the queuesStore
// pattern (tenant-keyed by epoch, in-flight de-duplication, last-good rows kept
// on a failed refresh) plus the one thing a brand-new route family needs:
//
//   A SUPPORT VERDICT. /api/v1/ephemeral/* is new in 1.1 and nothing negotiates
//   a version, so the first call is also the probe. Three answers are STABLE —
//   asking again cannot change them — and a 2s poller that keeps asking turns
//   each of them into a toast every few seconds:
//
//     404                  the broker has no such route (older than 1.1)
//     404 route_blocked    the proxy does not classify the family
//     not_an_api_response  broker-direct: the SPA fallback answered the GET
//                          (api/client.js turns that 200 text/html into this)
//     403 feature_gated    the cluster's plan does not grant the class (§5.1)
//
//   On any of those the store stops asking and exposes the verdict, so the view
//   renders ONE quiet state instead of a failing poll. Everything else — 5xx,
//   offline, 429 — is transient: last-good rows stay, the global surface has
//   already said so, and the poller backs off for a cooldown rather than
//   hammering a cluster that is already unhappy.
//
// Two views read this: the Ephemeral view (force-refreshed on its own 2s
// ticker) and the cross-link chip on Queues (TTL-cached, no extra traffic).
// Keeping the probe in one place is what makes the chip free and keeps the
// old-broker answer from being discovered twice.
//
// TENANT KEYING IS A CORRECTNESS REQUIREMENT, exactly as in queuesStore: a
// cluster switch can also mean a different cell running a different broker
// version, so the verdict resets with the rows.
//
// API:
//   const { queues, loading, error, support, fetchQueues, fetchDepth } = useEphemeralStore()
//
//   await fetchQueues()                 // TTL-cached (the chip)
//   await fetchQueues({ force: true })  // always ask (the view's 2s poll)
//   await fetchQueues({ probe: true })  // clear a stable verdict and re-ask
import { computed, ref } from 'vue'

import { ephemeral as ephemeralApi } from '@/api'
import { toNum } from '@/composables/useApi'
import { currentEpoch, onClusterChange } from '@/stores/identity'

const DEFAULT_TTL_MS = 30_000
// After a TRANSIENT failure the 2s poll would re-fail on the same cadence.
// One failure is news; thirty a minute is noise, so back off and let the
// inline banner carry the state until the cooldown expires.
const COOLDOWN_MS = 10_000

// 'unknown'  — never asked, or the answer has not landed yet
// 'live'     — the family answered; ephemeral queues exist on this cell
// 'absent'   — the broker/proxy does not expose the family (older than 1.1)
// 'gated'    — exposed, but this cluster's plan does not grant it (403)
const support = ref('unknown')

const queues = ref([])
const loading = ref(false)
const error = ref(null)
const lastFetched = ref(0)
let inflight = null
let inflightEpoch = -1
let cooldownUntil = 0

// ---------------------------------------------------------------------------
// Payload → rows.
//
// The status endpoints report gauges (§6): depth is the RING LENGTH, lag is
// head − cursor, and both are per (queue, group). Nothing here invents a
// figure: an absent field becomes null via toNum and every surface renders
// null as '—', because on this class "the broker did not report it" and "it is
// zero" are different facts and only one of them is good news.
// ---------------------------------------------------------------------------

/** `__QUEUE_MODE__` / no group at all is the groupless queue-mode pop (§1.5). */
const groupLabel = (name) =>
  !name || name === '__QUEUE_MODE__' ? '(queue mode)' : String(name)

function normalizeGroup(raw, queueHead) {
  const g = raw && typeof raw === 'object' ? raw : {}
  const cursor = toNum(g.cursor ?? g.offset)
  const head = toNum(g.head) ?? queueHead
  // Prefer the broker's own lag; derive head − cursor only when both halves
  // are present. Never clamp a missing half to zero.
  const reported = toNum(g.lag)
  const lag = reported !== null
    ? reported
    : (head !== null && cursor !== null ? Math.max(0, head - cursor) : null)
  return {
    name: groupLabel(g.group ?? g.name),
    cursor,
    lag,
    // Ring eviction can move the floor past a parked cursor; the skip is
    // counted per group (§3.2). Rendered only when the broker reports it.
    skipped: toNum(g.skipped),
  }
}

function normalizeGroups(raw, queueHead) {
  if (Array.isArray(raw)) return raw.map((g) => normalizeGroup(g, queueHead))
  // Map form: { "workers": { cursor: 12 } } or the bare-cursor shorthand.
  if (raw && typeof raw === 'object') {
    return Object.entries(raw).map(([name, g]) =>
      normalizeGroup(
        typeof g === 'object' && g !== null ? { ...g, group: name } : { group: name, cursor: g },
        queueHead,
      )
    )
  }
  // A bare count (or nothing): countable, not expandable.
  return null
}

/**
 * One row of the status payload, in the console's vocabulary.
 *
 * Field aliases are deliberate and narrow — the wire is fixed by §3.1's
 * vocabulary, not by a schema this app can import, so each field accepts the
 * plan's word plus at most one obvious spelling. Anything else stays null.
 */
export function normalizeEphemeralQueue(raw, fallbackName = null) {
  const r = raw && typeof raw === 'object' ? raw : {}
  const head = toNum(r.head)
  const groups = normalizeGroups(r.groups, head)
  const groupCount = groups
    ? groups.length
    : toNum(typeof r.groups === 'number' ? r.groups : r.groupCount)
  return {
    name: String(r.queue ?? r.name ?? fallbackName ?? ''),
    // EXPLICIT true / EXPLICIT false only. A payload that carries neither is
    // not evidence for either tier (§1.1), and the badge renders nothing
    // rather than guess which one this queue is.
    declared: r.declared === true ? true : r.declared === false ? false : null,
    depth: toNum(r.depth ?? r.length),
    bytes: toNum(r.bytes ?? r.sizeBytes),
    head,
    partitions: Array.isArray(r.partitions) ? r.partitions.length : toNum(r.partitions),
    // Single-owner placement in v1 (§3.7): which broker holds the ring. Absent
    // on a single-broker cell, where the question has one answer.
    owner: r.owner ?? r.ownerBroker ?? r.ownerServerId ?? null,
    groups,
    groupCount,
    // Three ways a message leaves without being consumed (§1.6, §1.7, §1.3).
    drops: {
      bounds: toNum(r.drops?.bounds ?? r.droppedBounds),
      ttl: toNum(r.drops?.ttl ?? r.droppedTtl),
      retry: toNum(r.drops?.retry ?? r.droppedRetry),
    },
  }
}

/**
 * The stable verdict an error carries, or null when it is transient.
 * A stable verdict is rendered; a transient one is retried after a cooldown.
 */
function verdictFor(err) {
  if (!err) return null
  if (err.status === 404) return 'absent'
  if (err.code === 'not_an_api_response') return 'absent'
  if (err.status === 403 && err.code === 'feature_gated') return 'gated'
  return null
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

/**
 * Load the tenant's ephemeral queues.
 *
 *   force  — ignore the TTL (the view's 2s poll)
 *   probe  — also clear a stable verdict and a cooldown, so an operator can
 *            re-ask after upgrading the broker ("Check again")
 */
const fetchQueues = async ({ force = false, probe = false, ttlMs = DEFAULT_TTL_MS } = {}) => {
  if (probe) {
    support.value = 'unknown'
    cooldownUntil = 0
    error.value = null
  }
  // Stable verdicts are not re-asked: that is the whole anti-toast-loop rule.
  if (support.value === 'absent' || support.value === 'gated') return queues.value
  if (!probe && cooldownUntil > Date.now()) return queues.value

  const fresh = lastFetched.value > 0 && Date.now() - lastFetched.value < ttlMs
  if (!force && !probe && fresh) return queues.value
  if (inflight && inflightEpoch === currentEpoch()) return inflight

  loading.value = true
  const epochAtStart = currentEpoch()
  inflightEpoch = epochAtStart

  inflight = (async () => {
    try {
      const r = await ephemeralApi.queues()
      // Switched cluster mid-flight: these rows are the tenant we left.
      if (epochAtStart !== currentEpoch()) return queues.value
      const rows = r.data?.queues || (Array.isArray(r.data) ? r.data : [])
      queues.value = rows.map((row) => normalizeEphemeralQueue(row))
      lastFetched.value = Date.now()
      error.value = null
      support.value = 'live'
      cooldownUntil = 0
      return queues.value
    } catch (err) {
      if (epochAtStart !== currentEpoch()) return queues.value
      const verdict = verdictFor(err)
      if (verdict) {
        // Not a failure of this cell — a fact about it. No rows can exist.
        support.value = verdict
        queues.value = []
        error.value = err
        return queues.value
      }
      // Transient: keep last-good rows, say how old they are, and stop asking
      // for a while. The failure is already on the global surface.
      error.value = err
      cooldownUntil = Date.now() + (err?.retryAfter ? err.retryAfter * 1000 : COOLDOWN_MS)
      return queues.value
    } finally {
      loading.value = false
      inflight = null
    }
  })()
  return inflight
}

/**
 * One queue's gauges, straight from the per-queue status route — the authority
 * for its per-group cursors while a row is expanded. Resolves to a normalized
 * row, or null when the call failed (the caller renders that inline; a stable
 * verdict is recorded so the family stops being polled at all).
 */
const fetchDepth = async (name, config) => {
  try {
    const r = await ephemeralApi.depth(name, config)
    return normalizeEphemeralQueue(r.data, name)
  } catch (err) {
    const verdict = verdictFor(err)
    if (verdict) support.value = verdict
    return null
  }
}

const invalidate = () => { lastFetched.value = 0 }

/** Full wipe on a cluster switch — the verdict too: a different cell can be a
 *  different broker version, so "absent here" says nothing about there. */
const reset = () => {
  inflight = null
  inflightEpoch = -1
  cooldownUntil = 0
  queues.value = []
  error.value = null
  lastFetched.value = 0
  loading.value = false
  support.value = 'unknown'
}

onClusterChange(reset)

// ---------------------------------------------------------------------------
// Derived
// ---------------------------------------------------------------------------

/** The family answered at least once on this cluster. */
const available = computed(() => support.value === 'live')
const count = computed(() => queues.value.length)
/** Last load failed and what is on screen predates it. */
const isStale = computed(() => error.value !== null && support.value === 'live' && lastFetched.value > 0)

const totals = computed(() => {
  // A list that loaded empty really is nothing in RAM — that zero is a fact.
  // With rows on screen, a total stays null until at least one row reported
  // the figure, so "the broker does not report bytes" cannot read as "0 B".
  if (queues.value.length === 0) return { depth: 0, bytes: 0, drops: 0 }
  let depth = null
  let bytes = null
  let drops = null
  for (const q of queues.value) {
    if (q.depth !== null) depth = (depth ?? 0) + q.depth
    if (q.bytes !== null) bytes = (bytes ?? 0) + q.bytes
    for (const d of [q.drops.bounds, q.drops.ttl, q.drops.retry]) {
      if (d !== null) drops = (drops ?? 0) + d
    }
  }
  return { depth, bytes, drops }
})

export function useEphemeralStore() {
  return {
    queues,
    loading,
    error,
    lastFetched,
    support,
    available,
    isStale,
    count,
    totals,
    fetchQueues,
    fetchDepth,
    invalidate,
    reset,
  }
}
