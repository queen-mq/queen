import client from './client'

// ============================================
// RESOURCES API
// ============================================
export const resources = {
  getOverview: () => client.get('/api/v1/resources/overview'),
  getNamespaces: () => client.get('/api/v1/resources/namespaces'),
  getTasks: () => client.get('/api/v1/resources/tasks'),
}

// ============================================
// QUEUES API
// ============================================
export const queues = {
  list: (params) => client.get('/api/v1/resources/queues', { params }),
  get: (name) => client.get(`/api/v1/resources/queues/${name}`),
  delete: (name) => client.delete(`/api/v1/resources/queues/${name}`),
  clear: (name, partition) => {
    const params = partition ? { partition } : {}
    return client.delete(`/api/v1/queues/${name}/clear`, { params })
  },
  configure: (data) => client.post('/api/v1/configure', data),
  getPartitions: (params) => client.get('/api/v1/resources/partitions', { params }),
}

// ============================================
// MESSAGES API
// ============================================
export const messages = {
  list: (params) => client.get('/api/v1/messages', { params }),
  get: (partitionId, transactionId) => 
    client.get(`/api/v1/messages/${partitionId}/${transactionId}`),
  delete: (partitionId, transactionId) => 
    client.delete(`/api/v1/messages/${partitionId}/${transactionId}`),
  retry: (partitionId, transactionId) => 
    client.post(`/api/v1/messages/${partitionId}/${transactionId}/retry`),
  moveToDLQ: (partitionId, transactionId) => 
    client.post(`/api/v1/messages/${partitionId}/${transactionId}/dlq`),
  push: (data) => client.post('/api/v1/push', data),
  pop: (queue, partition, params) => {
    if (partition) {
      return client.get(`/api/v1/pop/queue/${queue}/partition/${partition}`, { params })
    }
    return client.get(`/api/v1/pop/queue/${queue}`, { params })
  },
  ack: (data) => client.post('/api/v1/ack', data),
}

// ============================================
// TRACES API
// ============================================
export const traces = {
  getByName: (traceName, params) => 
    client.get(`/api/v1/traces/by-name/${encodeURIComponent(traceName)}`, { params }),
  getAvailableNames: (params) => 
    client.get('/api/v1/traces/names', { params }),
  getForMessage: (partitionId, transactionId) => 
    client.get(`/api/v1/traces/${partitionId}/${transactionId}`),
}

// ============================================
// ANALYTICS API
// ============================================
export const analytics = {
  getStatus: (params) => client.get('/api/v1/status', { params }),
  getQueues: (params) => client.get('/api/v1/status/queues', { params }),
  getQueueDetail: (name, params) => client.get(`/api/v1/status/queues/${name}`, { params }),
  getAnalytics: (params) => client.get('/api/v1/status/analytics', { params }),
}

// ============================================
// CONSUMER GROUPS API
// ============================================
export const consumers = {
  list: () => client.get('/api/v1/consumer-groups'),
  refreshStats: () => client.post('/api/v1/stats/refresh'),
  get: (name) => client.get(`/api/v1/consumer-groups/${encodeURIComponent(name)}`),
  getLagging: (minLagSeconds) => 
    client.get(`/api/v1/consumer-groups/lagging`, { params: { minLagSeconds } }),
  delete: (name, deleteMetadata = true) => 
    client.delete(`/api/v1/consumer-groups/${encodeURIComponent(name)}`, { 
      params: { deleteMetadata } 
    }),
  deleteForQueue: (name, queueName, deleteMetadata = true) => 
    client.delete(`/api/v1/consumer-groups/${encodeURIComponent(name)}/queues/${encodeURIComponent(queueName)}`, { 
      params: { deleteMetadata } 
    }),
  seek: (name, queue, options) => 
    client.post(`/api/v1/consumer-groups/${encodeURIComponent(name)}/queues/${encodeURIComponent(queue)}/seek`, options),
  seekPartition: (name, queue, partition) => 
    client.post(`/api/v1/consumer-groups/${encodeURIComponent(name)}/queues/${encodeURIComponent(queue)}/partitions/${encodeURIComponent(partition)}/seek`),
}

// ============================================
// DEAD LETTER QUEUE API
// ============================================
export const dlq = {
  list: (params) => client.get('/api/v1/dlq', { params }),
  getStatus: (params) => client.get('/api/v1/status', { params }),
  retry: (partitionId, transactionId) =>
    client.post(`/api/v1/messages/${partitionId}/${transactionId}/retry`),
  delete: (partitionId, transactionId) =>
    client.delete(`/api/v1/messages/${partitionId}/${transactionId}`),
}

// ============================================
// SYSTEM API
// ============================================
export const system = {
  getHealth: () => client.get('/health'),
  getMetrics: () => client.get('/metrics'),
  // Push Maintenance Mode
  getMaintenance: () => client.get('/api/v1/system/maintenance'),
  setMaintenance: (enabled) => client.post('/api/v1/system/maintenance', { enabled }),
  // Pop Maintenance Mode
  getPopMaintenance: () => client.get('/api/v1/system/maintenance/pop'),
  setPopMaintenance: (enabled) => client.post('/api/v1/system/maintenance/pop', { enabled }),
  // System Metrics
  getSystemMetrics: (params) => client.get('/api/v1/analytics/system-metrics', { params }),
  getWorkerMetrics: (params) => client.get('/api/v1/analytics/worker-metrics', { params }),
  // Per-Queue Lag & Pop Throughput
  getQueueLag: (params) => client.get('/api/v1/analytics/queue-lag', { params }),
  // Per-Queue Ops (push / pop / ack / trx + partition lifecycle)
  getQueueOps: (params) => client.get('/api/v1/analytics/queue-ops', { params }),
  // Per-replica parked breakdown (Parked tab "individual replicas" toggle)
  getQueueParkedReplicas: (params) => client.get('/api/v1/analytics/queue-parked-replicas', { params }),
  // Retention / Eviction time series
  getRetention: (params) => client.get('/api/v1/analytics/retention', { params }),
  // PostgreSQL Stats
  getPostgresStats: () => client.get('/api/v1/analytics/postgres-stats'),
}

// Export all APIs
export default {
  resources,
  queues,
  messages,
  traces,
  analytics,
  consumers,
  dlq,
  system,
}

