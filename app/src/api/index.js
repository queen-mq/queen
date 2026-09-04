import client from './client'

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
export const queues = {
  list: (params, config) => client.get('/api/v1/resources/queues', { params, ...config }),
  get: (name, config) => client.get(`/api/v1/resources/queues/${name}`, config),
  delete: (name, config) => client.delete(`/api/v1/resources/queues/${name}`, config),
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

  // NO retry / move-to-DLQ here: the broker registers only GET and DELETE on
  // /api/v1/messages/:pid/:txid (server/src/main.rs). A POST fell through to the
  // static fallback, which answers 200 text/html to any method, so the caller
  // reported a success that never happened. Restore these only together with the
  // routes — a control that cannot verify its own outcome does not ship.
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
  getQueueDetail: (name, params, config) =>
    client.get(`/api/v1/status/queues/${name}`, { params, ...config }),
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
  // `Content-Type: application/json`, so axios' transformRequest stringifies a null
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
  // No replay: the broker has no re-push route for a DLQ snapshot. See the note
  // in `messages` — this used to POST a path that does not exist.
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
}

// ============================================
// OPERATOR API — CELL-LEVEL. 200 only when /auth/me says operator_live;
// 404 {"code":"route_blocked"} for every other principal.
// Never render one of these numbers without saying it covers the whole cell.
// ============================================
export const operator = {
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
  system,
  operator,
}

export { ApiError, describeApiError } from './errors'
