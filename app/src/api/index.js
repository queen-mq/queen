import client from './client'
import { timerAddr, timerQueueAddr } from './timerPath'

// ===========================================================================
// TWO SURFACES, AND THE CALL SITE MUST SAY WHICH.
//
//  * Everything below except `operator` is TENANT-SCOPED: the proxy injects
//    the tenant of the acting cluster, so the answer is "for this tenant".
//  * `operator` is CELL-LEVEL: unscopable by nature (host CPU, PG internals,
//    cell maintenance, the file buffer). It answers 200 only for a live
//    operator and 404 `route_blocked` for everyone else
//    (proxy/src/routes.rs is_operator_route). A view that renders one of
//    those numbers MUST label it cell-level, or a tenant reads a cell figure
//    as their own.
//
// Routes the proxy blocks for EVERY principal — /api/v1/stats/refresh,
// /internal/*, discovery GET /api/v1/pop, bare /metrics,
// /api/v1/system/shared-state — are not declared here at all. Declaring one
// only produces a 404 the UI has to explain away.
// ===========================================================================

// ============================================
// RESOURCES API (tenant-scoped)
// ============================================
export const resources = {
  getOverview: (config) => client.get('/api/v1/resources/overview', config),
  getNamespaces: (config) => client.get('/api/v1/resources/namespaces', config),
  getTasks: (config) => client.get('/api/v1/resources/tasks', config),
}

// ============================================
// QUEUES API (tenant-scoped)
// ============================================
// A queue name is arbitrary caller text — the push path creates whatever name it
// is given, and `/configure` accepts even '' — so it is ENCODED into every path
// that addresses one. Unencoded, a name carrying '#', '?' or '/' silently
// addresses a DIFFERENT queue (`/queues/a#b` resolves to `/queues/a`), which is
// worse than a failure: the editor would prefill from one queue and save to
// another.
export const queues = {
  list: (params, config) => client.get('/api/v1/resources/queues', { params, ...config }),
  get: (name, config) => client.get(`/api/v1/resources/queues/${encodeURIComponent(name)}`, config),
  delete: (name, config) =>
    client.delete(`/api/v1/resources/queues/${encodeURIComponent(name)}`, config),
  /**
   * Create or reconfigure a queue. `{queue, namespace?, task?, options:{...}, mode?}`.
   * `mode` is 'merge' (default: an option the body does not mention keeps its
   * current value, `null` restores the default) or 'replace' (every option is
   * reset to its default unless given, the declarative form `queenctl apply`
   * uses). PLAN_DASHBOARD_ACTIONS.md §2.2. QueueAdmin at the proxy.
   */
  configure: (body, config) => client.post('/api/v1/configure', body, config),
}

// ============================================
// EPHEMERAL QUEUES API (tenant-scoped)
// ============================================
// The RAM storage class (EPHEMERAL_QUEUES.md §3.1) — its own route family, and
// its own vocabulary. What these answer is NOT what `queues` above answers: an
// ephemeral queue has a ring depth and no pending, no retained bytes, no DLQ
// and no PG-derived lag, because none of those concepts has a referent when the
// contents survive nothing (§1.2). A view that borrows a durable column here is
// inventing a number.
//
// The two status routes read in-process gauges and touch no database (§6), so —
// unlike the durable meter, whose 1s poll is load-bearing on PG — they can be
// polled at 1-2s at zero cost anywhere.
//
// OLD BROKER / OLD PROXY. The family is new in 1.1 and nothing negotiates a
// version: a broker without the routes 404s, a proxy that does not classify
// them answers 404 {"code":"route_blocked"}, and a broker-direct build falls
// through to the SPA fallback, which client.js already converts into
// `not_an_api_response`. All three mean "not exposed here" — a state to render,
// never a failure to retry (stores/ephemeralStore.js owns that verdict).
export const ephemeralQueues = (config) => client.get('/api/v1/ephemeral/queues', config)

/** One queue's gauges. Queue names are arbitrary caller text — encode them (see `addr`). */
export const ephemeralDepth = (queue, config) =>
  client.get(`/api/v1/ephemeral/queues/${encodeURIComponent(queue)}/depth`, config)

/**
 * Drop every message, void the leases, rewind every group cursor. 200 {dropped}.
 * Legal only because of the §1.2 loss contract — there is nothing to recover.
 * The queue name travels in the BODY here, not the path (§3.1).
 */
export const ephemeralReset = (queue, config) =>
  client.post('/api/v1/ephemeral/reset', { queue }, config)

/** Contents AND the declared configuration (§1.1). Nothing survives it. */
export const ephemeralDelete = (queue, config) =>
  client.delete(`/api/v1/ephemeral/queue/${encodeURIComponent(queue)}`, config)

/** Namespace form for call sites that read better grouped — the same four functions. */
export const ephemeral = {
  queues: ephemeralQueues,
  depth: ephemeralDepth,
  reset: ephemeralReset,
  delete: ephemeralDelete,
}

// ============================================
// MESSAGES API (tenant-scoped)
// ============================================
// A transaction id is arbitrary caller-supplied text (server/src/handlers/data.rs
// validates nothing): unencoded, one containing `/` builds a 4-segment path that
// matches no route and lands on a fallback, i.e. a call that reads as success
// while addressing something else entirely.
const addr = (partitionId, transactionId) =>
  `/api/v1/messages/${encodeURIComponent(partitionId)}/${encodeURIComponent(transactionId)}`

export const messages = {
  list: (params, config) => client.get('/api/v1/messages', { params, ...config }),
  get: (partitionId, transactionId, config) => client.get(addr(partitionId, transactionId), config),
  delete: (partitionId, transactionId, config) =>
    client.delete(addr(partitionId, transactionId), config),
  push: (data, config) => client.post('/api/v1/push', data, config),

  // No retry here on purpose. The broker's POST .../retry addresses a dead
  // letter by (partition, transaction id), which can name one row per consumer
  // group; the dashboard replays by DLQ row id instead (`dlq.replay`), so the
  // control acts on exactly the row on screen. No move-to-DLQ: no such route.
}

// ============================================
// TRACES API (tenant-scoped)
// ============================================
export const traces = {
  getByName: (traceName, params, config) =>
    client.get(`/api/v1/traces/by-name/${encodeURIComponent(traceName)}`, { params, ...config }),
  /** Trace names that actually exist for this tenant — the only honest source of suggestions. */
  getAvailableNames: (params, config) =>
    client.get('/api/v1/traces/names', { params, ...config }),
}

// ============================================
// ANALYTICS API (tenant-scoped)
// ============================================
export const analytics = {
  getQueues: (params, config) => client.get('/api/v1/status/queues', { params, ...config }),
  // Encoded for the same reason `queues.get` is: this is the read Queue Detail
  // renders its numbers from, and a name with a '#' in it would quietly show
  // another queue's.
  getQueueDetail: (name, params, config) =>
    client.get(`/api/v1/status/queues/${encodeURIComponent(name)}`, { params, ...config }),
}

// ============================================
// CONSUMER GROUPS API (tenant-scoped)
// ============================================
export const consumers = {
  list: (config) => client.get('/api/v1/consumer-groups', config),
  get: (name, config) => client.get(`/api/v1/consumer-groups/${encodeURIComponent(name)}`, config),
  getLagging: (minLagSeconds, config) =>
    client.get('/api/v1/consumer-groups/lagging', { params: { minLagSeconds }, ...config }),
  delete: (name, deleteMetadata = true, config) =>
    client.delete(`/api/v1/consumer-groups/${encodeURIComponent(name)}`, {
      params: { deleteMetadata }, ...config
    }),
  deleteForQueue: (name, queueName, deleteMetadata = true, config) =>
    client.delete(`/api/v1/consumer-groups/${encodeURIComponent(name)}/queues/${encodeURIComponent(queueName)}`, {
      params: { deleteMetadata }, ...config
    }),
  seek: (name, queue, options, config) =>
    client.post(`/api/v1/consumer-groups/${encodeURIComponent(name)}/queues/${encodeURIComponent(queue)}/seek`, options, config),
  // `{toEnd:true}` EXPLICIT, never a null body. The broker's per-partition handler
  // defaults an *empty* body to toEnd, but this client sets a default
  // `Content-Type: application/json`, so a JSON client stringifies a null
  // payload into the 4 literal bytes `null` — non-empty, and not a struct, so
  // parse_seek answers 400 {"error":"bad body"} and the button never worked.
  seekPartition: (name, queue, partition, config) =>
    client.post(`/api/v1/consumer-groups/${encodeURIComponent(name)}/queues/${encodeURIComponent(queue)}/partitions/${encodeURIComponent(partition)}/seek`, { toEnd: true }, config),
}

// ============================================
// DEAD LETTER QUEUE API (tenant-scoped)
// ============================================
export const dlq = {
  list: (params, config) => client.get('/api/v1/dlq', { params, ...config }),
  /** Purge every DLQ entry matching an exact queue and optional consumer group. */
  purge: (params, config) => client.delete('/api/v1/dlq', { params, ...config }),
  /** Purge one DLQ entry. 200 {success:false} when nothing matched — check it. */
  delete: (partitionId, transactionId, config) =>
    client.delete(addr(partitionId, transactionId), config),
  /**
   * Replay ONE dead-letter row, addressed by its row id (the `id` the list
   * returns), on the broker's move primitive: lock + push + delete in one
   * transaction, deterministic transaction id `dlq:<id>`, so a second click
   * answers 404 `gone` instead of pushing twice (PLAN_DASHBOARD_ACTIONS.md
   * §2.3). Body `{}` replays to the origin queue/partition; `{queue, partition}`
   * moves it elsewhere in the same tenant. QueueAdmin at the proxy, and
   * push-blocked like a push because it grows retained bytes.
   */
  replay: (id, body = {}, config) =>
    client.post(`/api/v1/dlq/${encodeURIComponent(id)}/replay`, body, config),
}

// ============================================
// KV BROWSER API (tenant-scoped, read-only)
// ============================================
// The console's view of the KV store (PLAN_DASHBOARD_ACTIONS.md §2.5). NOT the
// batch route `POST /api/v1/kv`: that one is Gated(Kv, Mixed) at the proxy, so a
// Viewer cannot call it at all, it is metered as a KV batch, and `getPrefix`
// requires a prefix. These two live under /api/v1/resources, which the proxy
// classifies Read by prefix and method-agnostically — no feature gate, no quota.
//
// `list` is a POST because the cursor is a KEY. A key in a query string
// (`?after=wh.deliver:promotion-publication:b15f…`) is exactly the leak
// PLAN_KV_TIMERS.md §5.5 forbids, through four components' access logs.
//
// Keyset paging: `{rows, truncated, nextAfter}`. `after` is an EXCLUSIVE cursor
// (the last key of the previous page), `limit` is clamped 1..1000 by the SP and
// never rejected, and a byte budget can end a page early — `truncated` tells the
// truth either way. Page 135 of 27k costs the same one index-range read as page 1.
//
// NEW ROUTES: a broker older than these answers 404 — a state to render, not a
// failure to retry (stores/routeSupport.js owns that verdict).
export const kv = {
  /** `[{namespace, keys}]` for the acting tenant, exact and expired-inclusive.
   *  Θ(keys of the tenant): an index-only scan of the primary key where the
   *  tenant is a selective slice, a sequential scan where it is most of the
   *  table. Milliseconds at the tens of thousands of keys a cell holds. */
  namespaces: (config) => client.get('/api/v1/resources/kv/namespaces', config),
  /** `{namespace, prefix?, after?, limit?, keysOnly?, includeExpired?}` → `{rows, truncated, nextAfter}`. */
  list: (body, config) => client.post('/api/v1/resources/kv/list', body, config),
}

// ============================================
// TIMERS API (tenant-scoped)
// ============================================
// The scheduled-message family (server/src/handlers/timers.rs). Every route
// exists since 1.2; the dashboard simply never called them
// (PLAN_DASHBOARD_ACTIONS.md §2.6). Gated(Timers, Read|Open) at the proxy, so a
// cluster whose plan lacks the feature answers 403 feature_gated — a state to
// render once, never to poll.
//
// THE OPERATOR'S SWITCH DOES NOT REACH THESE FOUR. switches.rs pins rung 1 to
// `true` for `Surface::TimerRead` / `TimerCancel` and quota.rs hands them
// `Verdict::Allow` (§9.6: a read that answered 503 would stop a caller finding
// out whether a timer it can no longer cancel is still pending, and the stop
// button must not switch itself off). So `timers_disabled` is reachable only on
// `POST /api/v1/timers`, which nothing here calls, and the only 503 these
// routes can mint is the handler's own `timers_unavailable` — a pool
// exhaustion, a statement timeout or a dead connection.
//
// The key encoder lives in ./timerPath.js so `node --test` can reach it; see
// that file for why the escaping is load-bearing and not uniform across the
// clients.
export const timers = {
  /** Keyset page: `{after?, limit?}` → `{rows, truncated, nextAfter}`; `after` is exclusive on timerKey. */
  list: (queue, params, config) => client.get(timerQueueAddr(queue), { params, ...config }),
  /** Exact count under a NON-EMPTY prefix (the SP refuses a whole-queue count) → `{count}`. */
  count: (queue, prefix, config) =>
    client.get(timerQueueAddr(queue), { params: { mode: 'count', prefix }, ...config }),
  /** One timer as stored: `{found:false}` is HTTP 200. `payload` is base64 of the
   *  bytes AS STORED; `payloadZstd` and `encrypted` are booleans describing them
   *  (025_log_timers.sql log_timers_peek_v1), and encryption is outermost. */
  peek: (queue, timerKey, config) => client.get(timerAddr(queue, timerKey), config),
  /** Cancel one timer; `{params:{txn}}` echoes the caller's txn back on `absent`.
   *  Render the SP's verdict verbatim: `cancelled` | `too_late` | `absent`, all
   *  HTTP 200, and `absent` leaves no tombstone. */
  cancel: (queue, timerKey, config) => client.delete(timerAddr(queue, timerKey), config),
}

// ============================================
// SYSTEM API (tenant-scoped)
// ============================================
export const system = {
  // Cell health, not tenant health: it reports the broker behind the acting
  // cluster. Label it as such wherever it is rendered.
  getHealth: (config) => client.get('/health', config),
  // Per-queue time series — tenant-scoped broker-side since Track B2.
  getQueueLag: (params, config) => client.get('/api/v1/analytics/queue-lag', { params, ...config }),
  getQueueOps: (params, config) => client.get('/api/v1/analytics/queue-ops', { params, ...config }),
  getQueueParkedReplicas: (params, config) =>
    client.get('/api/v1/analytics/queue-parked-replicas', { params, ...config }),
  getRetention: (params, config) => client.get('/api/v1/analytics/retention', { params, ...config }),
  // Who is doing the work, grouped by namespace / task / queue. TENANT-SCOPED
  // like the rest of this object: the procedure filters every queue by the
  // tenant the proxy injects, and `tenant` in the payload is this tenant's
  // total, not the cell's — so a share computed against it is honest.
  getWorkload: (params, config) => client.get('/api/v1/analytics/workload', { params, ...config }),
  // The deeper layer of the Workload page. TENANT-SCOPED like the rest of this
  // object — every queue is filtered by the tenant the proxy injects — and all
  // three live under /api/v1/analytics/, so the proxy classifies them Read by
  // prefix and no proxy change was needed to expose them.
  //
  // NEW ROUTES: a broker older than these answers 404, which is a state to
  // render ("not available on this broker"), not a failure to retry. Nothing
  // negotiates a version, so the first call is also the probe.
  /** Why rows are in one queue's DLQ: folded error signatures over a sample. `queue` is required. */
  getDlqSignatures: (params, config) =>
    client.get('/api/v1/analytics/dlq-signatures', { params, ...config }),
  /** Partitions per queue and how many were written to in the last 1h / 24h / 7d. */
  getPartitionLiveness: (params, config) =>
    client.get('/api/v1/analytics/partition-liveness', { params, ...config }),
}

// ============================================
// OPERATOR API — CELL-LEVEL. 200 only when /auth/me says operator_live;
// 404 {"code":"route_blocked"} for every other principal.
// Never render one of these numbers without saying it covers the whole cell.
// ============================================
export const operator = {
  /** Users and cluster grants for every tenant represented on the acting cell. */
  listUsers: (config) => client.get('/api/operator/users', config),
  createUser: (body, config) => client.post('/api/operator/users', body, config),
  updateUser: (userId, body, config) =>
    client.patch(`/api/operator/users/${encodeURIComponent(userId)}`, body, config),
  setUserRole: (userId, clusterId, role, config) =>
    client.put(`/api/operator/users/${encodeURIComponent(userId)}/roles/${encodeURIComponent(clusterId)}`, { role }, config),
  removeUserRole: (userId, clusterId, config) =>
    client.delete(`/api/operator/users/${encodeURIComponent(userId)}/roles/${encodeURIComponent(clusterId)}`, config),
  /** Cell-wide broker status (every tenant on this cell). */
  getStatus: (params, config) => client.get('/api/v1/status', { params, ...config }),
  /** Disk file-buffer state for the cell. */
  getBuffers: (params, config) => client.get('/api/v1/status/buffers', { params, ...config }),
  getSystemMetrics: (params, config) =>
    client.get('/api/v1/analytics/system-metrics', { params, ...config }),
  getWorkerMetrics: (params, config) =>
    client.get('/api/v1/analytics/worker-metrics', { params, ...config }),
  getPostgresStats: (config) => client.get('/api/v1/analytics/postgres-stats', config),
  /**
   * The two maintenance kill switches, both cell-wide (every tenant on the
   * cell, not just yours). GET reads a flag, POST flips it.
   *
   * `getMaintenance` reports BOTH flags — `maintenanceMode` and
   * `popMaintenanceMode` — so the header banners need only this one call;
   * `setPopMaintenance` is the pop switch's write half.
   */
  getMaintenance: (config) => client.get('/api/v1/system/maintenance', config),
  setMaintenance: (enabled, config) =>
    client.post('/api/v1/system/maintenance', { enabled }, config),
  setPopMaintenance: (enabled, config) =>
    client.post('/api/v1/system/maintenance/pop', { enabled }, config),
  getPrometheus: (config) =>
    client.get('/metrics/prometheus', { responseType: 'text', ...config }),
}

export default {
  resources,
  queues,
  ephemeral,
  messages,
  traces,
  analytics,
  consumers,
  dlq,
  kv,
  timers,
  system,
  operator,
}

export { ApiError, describeApiError } from './errors'
