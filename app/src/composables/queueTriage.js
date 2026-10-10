import { groupAttention, queueAttention } from './useAttention.js'

const count = (value) => {
  if (value === null || value === undefined || value === '' || typeof value === 'boolean') return null
  const n = Number(value)
  return Number.isFinite(n) && n >= 0 ? n : null
}

// Two actual queue-list readings, never a push-minus-pop estimate. The caller
// resets this on failure and scope change. Long gaps are not a recent trend.
export function observePending(previous, queues, at) {
  const current = { at, counts: new Map(queues.filter(q => q?.name).map(q => [q.name, count(q.messages?.pending)])) }
  const elapsed = previous ? at - previous.at : 0
  const comparable = elapsed > 0 && elapsed <= 120_000
  const delta = new Map()
  if (comparable) {
    for (const [name, pending] of current.counts) {
      const before = previous.counts.get(name)
      if (pending !== null && before !== null && before !== undefined) delta.set(name, pending - before)
    }
  }
  return { current, delta, elapsed: comparable ? elapsed : null }
}

export function buildQueueTriage(queues = [], groups = [], deltas = new Map(), linesFor) {
  const issues = new Map(queueAttention(queues, groups, linesFor).map(i => [i.name, i]))
  const byQueue = new Map()
  for (const g of groups) {
    if (!byQueue.has(g.queueName)) byQueue.set(g.queueName, [])
    byQueue.get(g.queueName).push(g)
  }
  return queues.filter(q => q?.name).map(q => {
    const issue = issues.get(q.name)
    const all = byQueue.get(q.name) || []
    const affected = all.filter(g => ['bad', 'warn'].includes(groupAttention(g, linesFor)))
      .sort((a, b) => (count(b.maxTimeLag) || 0) - (count(a.maxTimeLag) || 0))
    const live = all.filter(g => groupAttention(g, linesFor) !== 'mute')
    const lags = live.map(g => count(g.maxTimeLag)).filter(n => n !== null)
    return {
      name: q.name, namespace: q.namespace || '', pending: count(q.messages?.pending),
      processing: count(q.messages?.processing),
      sev: issue?.sev || null, reason: issue?.reason || null, deadOnly: issue?.deadOnly || false,
      lag: lags.length ? lags.reduce((a, b) => Math.max(a, b)) : null,
      delta: deltas.has(q.name) ? deltas.get(q.name) : null,
      affected, groupCount: all.length, readerCount: live.length,
    }
  }).sort((a, b) => {
    const rank = { bad: 2, warn: 1 }
    return (rank[b.sev] || 0) - (rank[a.sev] || 0)
      || (b.lag ?? -1) - (a.lag ?? -1)
      || (b.pending ?? -1) - (a.pending ?? -1)
      || a.name.localeCompare(b.name)
  })
}

export function filterQueueTriage(rows, search = '', filter = 'attention') {
  const term = search.trim().toLocaleLowerCase()
  return rows.filter(row => {
    if (term && !`${row.name} ${row.namespace}`.toLocaleLowerCase().includes(term)) return false
    if (filter === 'attention') return row.sev !== null
    if (filter === 'bad') return row.sev === 'bad'
    if (filter === 'noReader') return row.reason === 'noReader'
    if (filter === 'growing') return row.delta !== null && row.delta > 0
    return true
  })
}
