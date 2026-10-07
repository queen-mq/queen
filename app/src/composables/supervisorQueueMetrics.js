const number = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null

// Queue-level observations from a deliberate, one-queue read. These never
// become per-instance throughput, even when a supervisor has only one pool.
export function supervisorQueueMetrics(data, queue, now = Date.now()) {
  if (!data || !Array.isArray(data.series) || (data.backlog !== undefined && !Array.isArray(data.backlog))) throw new Error('Invalid queue metrics response')
  const width = Number.isInteger(data.bucketMinutes) && data.bucketMinutes > 0 ? data.bucketMinutes * 60_000 : 60_000
  const since = now - 3_600_000
  const rows = (input, complete) => {
    const seen = new Set()
    return input.map(row => {
      const time = typeof row?.bucket === 'string' ? Date.parse(row.bucket) : NaN
      if (!Number.isFinite(time) || (complete && row.queueName !== queue) || seen.has(time)) throw new Error('Inconsistent queue metrics response')
      seen.add(time)
      return { ...row, time }
    }).filter(row => row.time >= since && row.time + (complete ? width : 0) <= now).sort((a, b) => a.time - b.time)
  }
  const history = rows(data.series, true)
  const backlog = rows(data.backlog || [], false)
  const latest = history.at(-1)
  const failures = history.map(row => number(row.ackFailed))
  const pending = backlog.map(row => number(row.pending))
  const completePending = pending.length > 1 && pending.every(value => value !== null)
  return {
    push: number(latest?.pushPerSecond), pop: number(latest?.popPerSecond),
    ackFailures: failures.length && failures.every(value => value !== null) ? failures.reduce((a, b) => a + b, 0) : null,
    bucket: latest?.time ?? null,
    pendingDelta: completePending ? pending.at(-1) - pending[0] : null,
    pending: pending.at(-1) ?? null,
    points: completePending ? backlog.map(row => ({ at: row.time, pending: row.pending })) : [],
  }
}
