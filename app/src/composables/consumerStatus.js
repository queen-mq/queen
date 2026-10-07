// Consumer tasks share a process. Never infer process budgets, broker readiness,
// successful acknowledgements, or restart guarantees from their observations.
const count = value => Number.isSafeInteger(value) && value >= 0 ? value : null
const text = value => typeof value === 'string' && value.length > 0 && value.length <= 255 && !/[\x00-\x1f\x7f]/.test(value) ? value : null

export function consumerObservation(entry, base, now, unconfirmed) {
  const raw = entry.document
  const epoch = count(raw.updated_at_epoch)
  const timeout = count(raw.configuration.heartbeat_timeout)
  const pools = raw.pool_status.map(pool => {
    if (!pool || typeof pool !== 'object') return null
    const running = count(pool.running), desired = count(pool.desired), busy = count(pool.busy)
    const completed = count(pool.completed), failed = count(pool.failed)
    const last = count(pool.last_completed_at_epoch), oldest = count(pool.oldest_inflight_seconds)
    if (!text(pool.name) || running === null || desired === null || desired < 1 || desired > 4096 || running > desired
      || busy === null || busy > running || completed === null || failed === null
      || (pool.queue !== null && !text(pool.queue)) || !text(pool.consumer_group)
      || (pool.last_completed_at_epoch !== null && (last === null || last > epoch + 5))
      || (busy > 0 ? oldest === null : pool.oldest_inflight_seconds !== null)) return null
    const missing = running < desired
    return {
      name: pool.name, queue: pool.queue || [text(pool.namespace), text(pool.task)].filter(Boolean).join(' / ') || 'All queues',
      queueName: pool.queue, running, desired, busy, completed, failed, lastCompleted: last ? last * 1000 : null,
      oldest, depth: null, draining: null, readiness: null, capacity: !missing,
      configuration: { consumerGroup: pool.consumer_group }, pids: [], failures: null,
      label: missing ? 'Consumer capacity reduced' : 'Consumers running', severity: missing ? 'warn' : '', priority: missing ? 3 : 0,
      next: missing ? 'Check consumer exits and application logs. The client does not restart stopped tasks.'
        : 'Compare handler completions and in-flight duration over time. A heartbeat alone does not prove progress.',
    }
  })
  const valid = pools.every(Boolean) && pools.length > 0 && new Set(pools.filter(Boolean).map(p => p.name)).size === pools.length
  const model = ['async-tasks', 'goroutines', 'threads', 'cooperative'].includes(raw.execution_model) ? raw.execution_model : null
  const row = { ...base, consumer: true, executionModel: model, hostname: text(raw.hostname), engine: text(raw.engine),
    instance: raw.instance_id, pid: count(raw.pid), state: ['starting', 'running', 'stopping', 'stopped'].includes(raw.state) ? raw.state : null,
    updatedAt: epoch ? epoch * 1000 : null, age: epoch ? Math.floor(now / 1000) - epoch : null,
    timeout: timeout > 0 && timeout <= 86400 ? timeout : null,
    startedAt: count(raw.started_at_epoch) > 0 && raw.started_at_epoch <= epoch + 5 ? raw.started_at_epoch * 1000 : null,
    uptime: count(raw.uptime_seconds), pools: pools.filter(Boolean),
    queueCount: valid ? new Set(pools.map(p => p.queueName).filter(Boolean)).size : null,
    poolCount: valid ? pools.length : null,
    workers: valid ? pools.reduce((n, p) => n + p.running, 0) : null,
    desired: valid ? pools.reduce((n, p) => n + p.desired, 0) : null,
    missingWorkers: valid ? pools.reduce((n, p) => n + p.desired - p.running, 0) : null,
  }
  if (unconfirmed) return { ...row, label: 'Current state unconfirmed', next: 'The latest read failed. These values come from the last successful reading.' }
  if (entry.expired) return { ...row, label: 'Publication expired', next: 'The publication expired. Check the application and its publisher.' }
  if (row.age === null || row.age < -5 || row.timeout === null) return { ...row, label: 'Heartbeat unconfirmed', next: 'Check the heartbeat timestamp and host clocks.' }
  if (row.age > row.timeout) return { ...row, label: 'Heartbeat overdue', next: 'Check the application and publisher. Current consumer health is unknown.' }
  row.fresh = true
  if (!valid || !model || !row.pid || !row.state || !row.engine) return { ...row, label: 'Incomplete telemetry', next: 'The publication does not provide a complete consumer observation.' }
  if (row.state !== 'running') return { ...row, label: { starting: 'Starting', stopping: 'Stopping', stopped: 'Stopped' }[row.state], next: 'This is the last reported consumer state.', severity: '', priority: 1 }
  row.capacity = row.missingWorkers === 0
  row.affectedPools = pools.filter(p => p.severity).length
  const issue = pools.find(p => p.severity) || pools[0]
  return { ...row, label: issue.label, next: issue.next, severity: issue.severity, priority: issue.priority }
}
