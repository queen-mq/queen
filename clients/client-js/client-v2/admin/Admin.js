/**
 * Admin API for Queen - Administrative and observability endpoints
 * These APIs are typically used by dashboards and admin tools, not regular applications
 */

import * as logger from '../utils/logger.js'

export class Admin {
  #httpClient

  constructor(httpClient) {
    this.#httpClient = httpClient
  }

  // ===========================
  // Resources API
  // ===========================

  /**
   * Get system overview with queue counts, message stats, etc.
   * @returns {Promise<object>}
   */
  async getOverview() {
    logger.log('Admin.getOverview', {})
    return this.#httpClient.get('/api/v1/resources/overview')
  }

  /**
   * Get all namespaces
   * @returns {Promise<object>}
   */
  async getNamespaces() {
    logger.log('Admin.getNamespaces', {})
    return this.#httpClient.get('/api/v1/resources/namespaces')
  }

  /**
   * Get all tasks
   * @returns {Promise<object>}
   */
  async getTasks() {
    logger.log('Admin.getTasks', {})
    return this.#httpClient.get('/api/v1/resources/tasks')
  }

  // ===========================
  // Queues API
  // ===========================

  /**
   * List all queues
   * @param {object} params - Query parameters (limit, offset, search, etc.)
   * @returns {Promise<object>}
   */
  async listQueues(params = {}) {
    logger.log('Admin.listQueues', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/resources/queues${queryString}`)
  }

  /**
   * Get queue details
   * @param {string} name - Queue name
   * @returns {Promise<object>}
   */
  async getQueue(name) {
    logger.log('Admin.getQueue', { name })
    return this.#httpClient.get(`/api/v1/resources/queues/${encodeURIComponent(name)}`)
  }

  /**
   * Per-partition backlog for a queue — the cheap sibling of getQueue:
   * watermark arithmetic only, no segments, no timestamps. Shape:
   * {queue, group, pending, partitionsPending, conflation, effectivePending,
   *  partitions: [{partition, pending}]}.
   * Omitting group gives queue-level pending under the same worst-cursor
   * precedence the dashboard publishes; a named group is that group's own
   * backlog per partition. Requires broker >= 1.0.4 — an older broker
   * answers 404 no_such_route, so fall back to getQueue there.
   *
   * The three fields added in 1.1.0 (PLAN_CONFLATION §2.5/§5.3):
   *   - `partitionsPending`: how many partitions owe work (pending > 0). Useful
   *     for every group; it is what queenctl used to compute client-side.
   *   - `conflation`: the group's stored last-value delivery policy.
   *   - `effectivePending`: WORK depth — handler invocations still owed. For a
   *     conflating group that is `partitionsPending` (one call per partition,
   *     newest message only); otherwise it equals `pending`.
   *
   * Read them together: for a conflating group `pending` is LOG depth (log
   * positions still to retire), so `pending: 4000000, effectivePending: 12` is
   * healthy — the same two numbers on a non-conflating group are an incident.
   * Absent on brokers older than 1.1.0.
   * @param {string} name - Queue name
   * @param {string|null} [group] - Consumer group (optional)
   * @returns {Promise<object>}
   */
  async getQueueDepth(name, group = null) {
    logger.log('Admin.getQueueDepth', { name, group })
    const queryString = group ? `?group=${encodeURIComponent(group)}` : ''
    return this.#httpClient.get(`/api/v1/resources/queues/${encodeURIComponent(name)}/depth${queryString}`)
  }

  /**
   * Clear all messages from a queue
   * @param {string} name - Queue name
   * @param {string} [partition] - Optional partition to clear
   * @returns {Promise<object>}
   */
  async clearQueue(name, partition = null) {
    logger.log('Admin.clearQueue', { name, partition })
    const queryString = partition ? `?partition=${encodeURIComponent(partition)}` : ''
    return this.#httpClient.delete(`/api/v1/queues/${encodeURIComponent(name)}/clear${queryString}`)
  }

  /**
   * Get all partitions
   * @param {object} params - Query parameters (queue, limit, etc.)
   * @returns {Promise<object>}
   */
  async getPartitions(params = {}) {
    logger.log('Admin.getPartitions', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/resources/partitions${queryString}`)
  }

  // ===========================
  // Messages API
  // ===========================

  /**
   * List messages with filters
   * @param {object} params - Query parameters (queue, partition, status, limit, offset, etc.)
   * @returns {Promise<object>}
   */
  async listMessages(params = {}) {
    logger.log('Admin.listMessages', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/messages${queryString}`)
  }

  /**
   * Get a specific message
   * @param {string} partitionId - Partition ID
   * @param {string} transactionId - Transaction ID
   * @returns {Promise<object>}
   */
  async getMessage(partitionId, transactionId) {
    logger.log('Admin.getMessage', { partitionId, transactionId })
    return this.#httpClient.get(`/api/v1/messages/${partitionId}/${transactionId}`)
  }

  /**
   * Delete a specific message
   * @param {string} partitionId - Partition ID
   * @param {string} transactionId - Transaction ID
   * @returns {Promise<object>}
   */
  async deleteMessage(partitionId, transactionId) {
    logger.log('Admin.deleteMessage', { partitionId, transactionId })
    return this.#httpClient.delete(`/api/v1/messages/${partitionId}/${transactionId}`)
  }

  /**
   * Retry a failed message
   * @param {string} partitionId - Partition ID
   * @param {string} transactionId - Transaction ID
   * @returns {Promise<object>}
   */
  async retryMessage(partitionId, transactionId) {
    logger.log('Admin.retryMessage', { partitionId, transactionId })
    return this.#httpClient.post(`/api/v1/messages/${partitionId}/${transactionId}/retry`, {})
  }

  /**
   * Move a message to the Dead Letter Queue
   * @param {string} partitionId - Partition ID
   * @param {string} transactionId - Transaction ID
   * @returns {Promise<object>}
   */
  async moveMessageToDLQ(partitionId, transactionId) {
    logger.log('Admin.moveMessageToDLQ', { partitionId, transactionId })
    return this.#httpClient.post(`/api/v1/messages/${partitionId}/${transactionId}/dlq`, {})
  }

  // ===========================
  // Traces API
  // ===========================

  /**
   * Get traces by name
   * @param {string} traceName - Trace name to search for
   * @param {object} params - Query parameters (limit, offset, from, to, etc.)
   * @returns {Promise<object>}
   */
  async getTracesByName(traceName, params = {}) {
    logger.log('Admin.getTracesByName', { traceName, params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/traces/by-name/${encodeURIComponent(traceName)}${queryString}`)
  }

  /**
   * Get available trace names
   * @param {object} params - Query parameters (limit, search, etc.)
   * @returns {Promise<object>}
   */
  async getTraceNames(params = {}) {
    logger.log('Admin.getTraceNames', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/traces/names${queryString}`)
  }

  /**
   * Get traces for a specific message
   * @param {string} partitionId - Partition ID
   * @param {string} transactionId - Transaction ID
   * @returns {Promise<object>}
   */
  async getTracesForMessage(partitionId, transactionId) {
    logger.log('Admin.getTracesForMessage', { partitionId, transactionId })
    return this.#httpClient.get(`/api/v1/traces/${partitionId}/${transactionId}`)
  }

  // ===========================
  // Analytics/Status API
  // ===========================

  /**
   * Get system status
   * @param {object} params - Query parameters
   * @returns {Promise<object>}
   */
  async getStatus(params = {}) {
    logger.log('Admin.getStatus', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/status${queryString}`)
  }

  /**
   * Get queue statistics
   * @param {object} params - Query parameters (limit, offset, etc.)
   * @returns {Promise<object>}
   */
  async getQueueStats(params = {}) {
    logger.log('Admin.getQueueStats', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/status/queues${queryString}`)
  }

  /**
   * Get detailed statistics for a specific queue
   * @param {string} name - Queue name
   * @param {object} params - Query parameters
   * @returns {Promise<object>}
   */
  async getQueueDetail(name, params = {}) {
    logger.log('Admin.getQueueDetail', { name, params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/status/queues/${encodeURIComponent(name)}${queryString}`)
  }

  /**
   * Get analytics data
   * @param {object} params - Query parameters (from, to, interval, etc.)
   * @returns {Promise<object>}
   */
  async getAnalytics(params = {}) {
    logger.log('Admin.getAnalytics', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/status/analytics${queryString}`)
  }

  // ===========================
  // Consumer Groups API
  // ===========================

  /**
   * List all consumer groups
   * @returns {Promise<object>}
   */
  async listConsumerGroups() {
    logger.log('Admin.listConsumerGroups', {})
    return this.#httpClient.get('/api/v1/consumer-groups')
  }

  /**
   * Refresh consumer group statistics
   * @returns {Promise<object>}
   */
  async refreshConsumerStats() {
    logger.log('Admin.refreshConsumerStats', {})
    return this.#httpClient.post('/api/v1/stats/refresh', {})
  }

  /**
   * Get consumer group details
   * @param {string} name - Consumer group name
   * @returns {Promise<object>}
   */
  async getConsumerGroup(name) {
    logger.log('Admin.getConsumerGroup', { name })
    return this.#httpClient.get(`/api/v1/consumer-groups/${encodeURIComponent(name)}`)
  }

  /**
   * Get lagging consumer groups
   * @param {number} [minLagSeconds=60] - Minimum lag in seconds to be considered lagging
   * @returns {Promise<object>}
   */
  async getLaggingConsumers(minLagSeconds = 60) {
    logger.log('Admin.getLaggingConsumers', { minLagSeconds })
    return this.#httpClient.get(`/api/v1/consumer-groups/lagging?minLagSeconds=${minLagSeconds}`)
  }

  /**
   * Delete a consumer group for a specific queue
   * @param {string} consumerGroup - Consumer group name
   * @param {string} queueName - Queue name
   * @param {boolean} [deleteMetadata=true] - Whether to delete subscription metadata
   * @returns {Promise<object>}
   */
  async deleteConsumerGroupForQueue(consumerGroup, queueName, deleteMetadata = true) {
    logger.log('Admin.deleteConsumerGroupForQueue', { consumerGroup, queueName, deleteMetadata })
    const url = `/api/v1/consumer-groups/${encodeURIComponent(consumerGroup)}/queues/${encodeURIComponent(queueName)}?deleteMetadata=${deleteMetadata}`
    return this.#httpClient.delete(url)
  }

  /**
   * Seek consumer group offset for a queue
   * @param {string} consumerGroup - Consumer group name
   * @param {string} queueName - Queue name
   * @param {object} options - Seek options (timestamp, position, etc.)
   * @returns {Promise<object>}
   */
  async seekConsumerGroup(consumerGroup, queueName, options = {}) {
    logger.log('Admin.seekConsumerGroup', { consumerGroup, queueName, options })
    const url = `/api/v1/consumer-groups/${encodeURIComponent(consumerGroup)}/queues/${encodeURIComponent(queueName)}/seek`
    return this.#httpClient.post(url, options)
  }

  // ===========================
  // System API
  // ===========================

  /**
   * Health check
   * @returns {Promise<object>}
   */
  async health() {
    logger.log('Admin.health', {})
    return this.#httpClient.get('/health')
  }

  /**
   * Get Prometheus metrics
   * @returns {Promise<string>} Raw metrics text
   */
  async metrics() {
    logger.log('Admin.metrics', {})
    return this.#httpClient.get('/metrics')
  }

  /**
   * Get push maintenance mode status
   * @returns {Promise<object>}
   */
  async getMaintenanceMode() {
    logger.log('Admin.getMaintenanceMode', {})
    return this.#httpClient.get('/api/v1/system/maintenance')
  }

  /**
   * Set push maintenance mode
   * @param {boolean} enabled - Enable or disable maintenance mode
   * @returns {Promise<object>}
   */
  async setMaintenanceMode(enabled) {
    logger.log('Admin.setMaintenanceMode', { enabled })
    return this.#httpClient.post('/api/v1/system/maintenance', { enabled })
  }

  /**
   * Get pop maintenance mode status
   * @returns {Promise<object>}
   */
  async getPopMaintenanceMode() {
    logger.log('Admin.getPopMaintenanceMode', {})
    return this.#httpClient.get('/api/v1/system/maintenance/pop')
  }

  /**
   * Set pop maintenance mode
   * @param {boolean} enabled - Enable or disable pop maintenance mode
   * @returns {Promise<object>}
   */
  async setPopMaintenanceMode(enabled) {
    logger.log('Admin.setPopMaintenanceMode', { enabled })
    return this.#httpClient.post('/api/v1/system/maintenance/pop', { enabled })
  }

  /**
   * Get system metrics (CPU, memory, connections, etc.)
   * @param {object} params - Query parameters (from, to, etc.)
   * @returns {Promise<object>}
   */
  async getSystemMetrics(params = {}) {
    logger.log('Admin.getSystemMetrics', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/analytics/system-metrics${queryString}`)
  }

  /**
   * Get worker metrics
   * @param {object} params - Query parameters
   * @returns {Promise<object>}
   */
  async getWorkerMetrics(params = {}) {
    logger.log('Admin.getWorkerMetrics', { params })
    const queryString = this.#buildQueryString(params)
    return this.#httpClient.get(`/api/v1/analytics/worker-metrics${queryString}`)
  }

  /**
   * Get PostgreSQL statistics
   * @returns {Promise<object>}
   */
  async getPostgresStats() {
    logger.log('Admin.getPostgresStats', {})
    return this.#httpClient.get('/api/v1/analytics/postgres-stats')
  }

  // ===========================
  // Helper Methods
  // ===========================

  #buildQueryString(params) {
    const searchParams = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null) {
        searchParams.append(key, String(value))
      }
    }
    const queryString = searchParams.toString()
    return queryString ? `?${queryString}` : ''
  }
}

