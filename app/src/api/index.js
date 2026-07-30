import client from './client'

// ===========================================================================
// TWO SURFACES, AND THE CALL SITE MUST SAY WHICH.
//
//  * Everything below except `operator` is TENANT-SCOPED: the proxy injects
//    the tenant of the acting cluster, so the answer is "for this tenant".
//  * `operator` is CELL-LEVEL: unscopable by nature (host CPU, PG internals,
//    cell maintenance, the file buffer). It answers 200 only for a live
//    operator and 404 `route_blocked` for everyone else
//    (queen_proxy/src/routes.rs is_operator_route). A view that renders one of
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
  seekPartition: (name, queue, partition, config) =>
    client.post(`/api/v1/consumer-groups/${encodeURIComponent(name)}/queues/${encodeURIComponent(queue)}/partitions/${encodeURIComponent(partition)}/seek`, null, config),
}

// ============================================
// DEAD LETTER QUEUE API (tenant-scoped)
// ============================================
export const dlq = {
  list: (params, config) => client.get('/api/v1/dlq', { params, ...config }),
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
  messages,
  traces,
  analytics,
  consumers,
  dlq,
  system,
  operator,
}

export { ApiError, describeApiError } from './errors'
