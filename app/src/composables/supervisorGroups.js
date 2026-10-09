// Group only within the current source. Publisher language and hostname are
// metadata; the publication group is the application's deployment identity.
export function supervisorTone(row) {
  if (!row.fresh) return 'warn'
  if (row.severity === 'bad') return 'bad'
  if (row.severity) return 'warn'
  if (row.state !== 'running') return 'idle'
  return row.affectedPools === 0 ? 'good' : 'warn'
}

const rank = { bad: 3, warn: 2, idle: 1, good: 0 }
const sum = (rows, field) => rows.every(row => Number.isSafeInteger(row[field]))
  ? rows.reduce((total, row) => total + row[field], 0) : null

export function supervisorGroups(rows, { partial = false } = {}) {
  const groups = new Map()
  for (const row of rows) {
    if (!groups.has(row.group)) groups.set(row.group, [])
    groups.get(row.group).push(row)
  }
  return [...groups].map(([name, members]) => {
    const instances = [...members].sort((a, b) => rank[supervisorTone(b)] - rank[supervisorTone(a)]
      || b.priority - a.priority || a.slot.localeCompare(b.slot))
    const counts = { good: 0, warn: 0, bad: 0, idle: 0 }
    for (const row of instances) counts[supervisorTone(row)]++
    let tone = supervisorTone(instances[0])
    // A stopped replica alongside running replicas must not disappear inside a
    // green total. An entirely paused/stopped group has a neutral lifecycle state.
    if (tone === 'idle' && counts.good) tone = 'warn'
    if (partial && tone !== 'bad') tone = 'warn'
    const current = instances.every(row => row.fresh && row.state === 'running')
    const completePools = instances.every(row => row.poolCount !== null)
    const completeQueues = instances.every(row => row.queueCount !== null)
    const queues = instances.flatMap(row => row.pools.map(pool => row.consumer ? pool.queueName : pool.queue).filter(Boolean))
    const ages = instances.map(row => row.age)
    const states = new Set(instances.map(row => row.state))
    const pools = instances.flatMap(row => row.pools)
    const consumerOnly = instances.every(row => row.consumer)
    const processOnly = instances.every(row => !row.consumer)
    const queueRows = new Map()
    for (const row of instances) for (const pool of row.pools) {
      const name = row.consumer ? pool.queueName : pool.queue
      if (!name) continue
      if (!queueRows.has(name)) queueRows.set(name, [])
      queueRows.get(name).push(pool)
    }
    const queueSummaries = [...queueRows].map(([name, allocations]) => ({
      name, running: current && completePools ? sum(allocations, 'running') : null,
      desired: current && completePools ? sum(allocations, 'desired') : null,
      tone: !current || !completePools ? 'warn' : allocations.some(pool => pool.severity === 'bad') ? 'bad'
        : allocations.some(pool => pool.severity) ? 'warn' : 'good',
    })).sort((a, b) => rank[b.tone] - rank[a.tone] || a.name.localeCompare(b.name))
    return {
      name, rows: instances, count: instances.length, counts, tone, partial,
      label: tone === 'bad' ? 'Critical issue' : tone === 'good' ? 'No issues reported'
        : partial ? 'Partial view' : tone === 'warn' ? 'Needs attention'
          : states.size === 1 ? instances[0].label : 'Not running',
      detail: tone === 'good' ? 'All loaded instances reporting normally'
        : partial && tone !== 'bad' ? 'More publications available · totals may be incomplete'
          : instances[0].label,
      engines: [...new Set(instances.map(row => row.engine).filter(Boolean))].sort(),
      workers: current ? sum(instances, 'workers') : null,
      desired: current ? sum(instances, 'desired') : null,
      // A surplus on one replica cannot cancel a shortfall on another.
      missingWorkers: current ? sum(instances, 'missingWorkers') : null,
      queueCount: completeQueues ? new Set(queues).size : null,
      queues: queueSummaries, consumerOnly, processOnly,
      poolCount: sum(instances, 'poolCount'),
      affectedPools: current ? sum(instances, 'affectedPools') : null,
      busy: current && completePools && consumerOnly ? sum(pools, 'busy') : null,
      handlerFailures: current && completePools && consumerOnly ? sum(pools, 'failed') : null,
      draining: current && processOnly ? sum(instances, 'draining') : null,
      headroom: current && processOnly && instances.every(row => row.budget) ? sum(instances.map(row => row.budget), 'available') : null,
      scopedConsumers: instances.some(row => row.consumer && row.pools.some(pool => !pool.queueName)),
      oldestAge: ages.every(age => age !== null && age >= -5) ? Math.max(0, ...ages) : null,
    }
  }).sort((a, b) => rank[b.tone] - rank[a.tone] || a.name.localeCompare(b.name))
}

// Filters select whole groups. Searching for a healthy host must never hide its
// unhealthy siblings or silently turn a partial sum into an application total.
export function filterSupervisorGroups(groups, { search = '', filter = 'all', group = null } = {}) {
  const term = search.trim().toLocaleLowerCase()
  return groups.filter(item => {
    if (group !== null && item.name !== group) return false
    return item.rows.some(row => {
      if (term && ![row.group, row.hostname, row.instance, ...row.pools.map(pool => `${pool.name} ${pool.queue}`)]
        .join(' ').toLocaleLowerCase().includes(term)) return false
      return filter === 'all' || (filter === 'attention' && ['bad', 'warn'].includes(item.tone))
        || (filter === 'stale' && !row.fresh) || filter === `engine:${row.engine}`
    })
  })
}
