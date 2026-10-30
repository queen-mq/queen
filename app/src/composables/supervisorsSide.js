// The Supervisors page, as data: its list of applications and the pools it
// shows on the right.
//
// The list (applicationList) is every application by name, one line each:
// a mark only where the page's own verdict asks for one (applicationVerdict)
//   bad / warn  an instance needs you, or its heartbeat is overdue (stale)
//   idle        every instance stopped, or none is running: a grey ring
//   (none)      reporting normally
// and how many instances publish under it. A click picks the application;
// its pools are shown in its card on the right, never under it in the list.
// The partial-view overlay supervisorGroups puts on every tone is left out of
// the marks: "more publications are available" is said once, in the header.
//
// A pool's mark comes from the instances that report it, by the rules the
// page already applies to an instance (supervisorTone) and to a pool
// (poolFinding, the pool's `severity`):
//   stopped   every instance reporting it has stopped: idle (a final report)
//   stale     an ongoing instance whose heartbeat is overdue, expired or
//             unreadable: amber, its health is unconfirmed
//   finding   a fresh running instance: the pool's own finding (amber or
//             coral); a draining pool is not a problem, only words
// The worst ongoing instance sets the mark and opens when the pool is clicked.
//
// Pure: no Vue, no '@/' imports, so test/supervisors-side.test.js pins it.
import { filterSupervisorGroups, supervisorTone } from './supervisorGroups.js'

const RANK = { bad: 3, warn: 2, idle: 1, good: 0 }

/** The drawer's key for a pool (Supervisors.vue poolKey). */
export const poolKey = (pool) => JSON.stringify([pool?.name, pool?.queue])

/** The Pools filter of the tools row, and the drawer's pool table. */
export const POOL_FILTERS = [
  { value: 'all', label: 'All' },
  { value: 'attention', label: 'Need attention' },
  { value: 'draining', label: 'Draining' },
]

/**
 * The Show filter of the tools row (filterSupervisorGroups' `filter`), one
 * choice per engine the loaded instances report. A value the address holds
 * that no engine answers any more stays a choice, so the field never goes
 * blank.
 */
export function showOptions(engines = [], current = 'all') {
  const out = [
    { value: 'all', label: 'All' },
    { value: 'attention', label: 'Need attention' },
    { value: 'stale', label: 'Stale' },
    { value: 'stopped', label: 'Stopped' },
    ...(engines || []).map((e) => ({ value: `engine:${e}`, label: `${String(e).toUpperCase()} engine` })),
  ]
  if (current && !out.some((o) => o.value === current) && String(current).startsWith('engine:')) {
    out.push({ value: current, label: `${String(current).slice(7).toUpperCase()} engine` })
  }
  return out
}

const queueOf = (row, pool) => (row.consumer ? pool.queueName : pool.queue)
const worst = (list) => list.reduce((a, b) => (RANK[b.tone] > RANK[a.tone] ? b : a))
const markOf = (tone) => (tone === 'bad' || tone === 'warn' || tone === 'idle' ? tone : null)

/**
 * One application's mark and the words behind it, by supervisorGroups' rule
 * without its partial-view overlay: the worst ongoing instance's tone, an
 * instance that is not running beside healthy ones a warning, every
 * instance stopped a grey ring. `{ mark: 'bad'|'warn'|'idle'|null, why }`.
 */
export function applicationVerdict(group) {
  const rows = Array.isArray(group?.rows) ? group.rows : []
  if (!rows.length) return { mark: null, why: '' }
  const ongoing = rows.filter((r) => !r.terminal)
  if (!ongoing.length) return { mark: 'idle', why: rows.length === 1 ? 'Stopped' : 'Every instance stopped' }
  const lead = ongoing.map((row) => ({ row, tone: supervisorTone(row) })).reduce((a, b) => (RANK[b.tone] > RANK[a.tone] ? b : a))
  if (lead.tone === 'idle' && rows.some((r) => supervisorTone(r) === 'good')) {
    return { mark: 'warn', why: 'Some instances are not running' }
  }
  if (lead.tone === 'good') return { mark: null, why: 'Reporting normally' }
  return { mark: markOf(lead.tone), why: lead.row.label || '' }
}

/**
 * The list: the applications the search names (an application, a host or a
 * pool — filterSupervisorGroups, which keeps whole applications), by name,
 * as `{ name, mark, why, count }`. `queue` keeps the ones serving it.
 */
export function applicationList(groups = [], { search = '', queue = '' } = {}) {
  const all = Array.isArray(groups) ? groups : []
  const scoped = queue ? all.filter((g) => (g.queues || []).some((q) => q.name === queue)) : all
  return filterSupervisorGroups(scoped, { search: String(search || '') })
    .map((g) => ({ name: g.name, count: g.count, ...applicationVerdict(g) }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

/** One instance's verdict on one of its pools ('good'|'warn'|'bad'|'idle'). */
export function poolTone(row, pool) {
  if (row.terminal) return 'idle'
  if (!row.fresh) return 'warn'
  if (row.state !== 'running') return row.severity === 'bad' ? 'bad' : row.severity ? 'warn' : 'idle'
  // Running, but the publication could not say which pools are affected.
  if (row.affectedPools === null) return 'warn'
  return pool.severity === 'bad' ? 'bad' : pool.severity ? 'warn' : 'good'
}

/**
 * An application's pools, worst first, each as
 *   { key, name, queue, tone, mark, finding, notes, running, desired,
 *     draining, stale, stopped, replicas, open }
 * `mark` is 'bad' | 'warn' only where the pool needs you. `open` is the slot
 * of the instance to show for it. `running` / `desired` add up the instances
 * with a recent running heartbeat, and are null when none has, or one did not
 * report them. `queue` keeps only the pools serving that queue (the page's
 * queue context); `pools` is the Pools filter.
 */
export function applicationPools(group, { queue = '', pools = 'all' } = {}) {
  const byKey = new Map()
  for (const row of group?.rows || []) {
    for (const pool of row.pools || []) {
      if (queue && queueOf(row, pool) !== queue) continue
      const key = poolKey(pool)
      if (!byKey.has(key)) byKey.set(key, { key, name: pool.name, queue: pool.queue, holders: [] })
      byKey.get(key).holders.push({ row, pool, tone: poolTone(row, pool) })
    }
  }
  const out = []
  for (const { key, name, queue: poolQueue, holders } of byKey.values()) {
    const ongoing = holders.filter((h) => !h.row.terminal)
    const lead = worst(ongoing.length ? ongoing : holders)
    const tone = ongoing.length ? lead.tone : 'idle'
    const reporting = ongoing.filter((h) => h.row.fresh && h.row.state === 'running')
    const sum = (field) => (reporting.length && reporting.every((h) => Number.isSafeInteger(h.pool[field]))
      ? reporting.reduce((n, h) => n + h.pool[field], 0) : null)
    const drainingHolders = ongoing.filter((h) => h.pool.draining > 0)
    const draining = drainingHolders.reduce((n, h) => n + h.pool.draining, 0)
    if (pools === 'attention' && tone !== 'bad' && tone !== 'warn') continue
    if (pools === 'draining' && !draining) continue
    const stale = ongoing.filter((h) => !h.row.fresh).length
    const stopped = holders.length - ongoing.length
    // The words of whatever set the mark: the instance's when it is the
    // instance that is in doubt, else the pool's finding.
    const finding = !ongoing.length ? 'Stopped'
      : !lead.row.fresh || lead.row.state !== 'running' || lead.row.affectedPools === null ? lead.row.label
        : lead.pool.label
    const notes = []
    if (draining && finding !== 'Workers draining') notes.push(`${draining} draining`)
    if (stale && lead.row.fresh) notes.push(`${stale} stale`)
    if (stopped && ongoing.length) notes.push(`${stopped} stopped`)
    const opener = pools === 'draining' && drainingHolders.length ? worst(drainingHolders) : lead
    out.push({
      key, name, queue: poolQueue, tone, mark: tone === 'bad' || tone === 'warn' ? tone : null,
      finding, notes, running: sum('running'), desired: sum('desired'), draining, stale, stopped,
      replicas: holders.length, open: opener.row.slot,
    })
  }
  return out.sort((a, b) => RANK[b.tone] - RANK[a.tone] || String(a.queue).localeCompare(String(b.queue)) || String(a.name).localeCompare(String(b.name)))
}

/**
 * The applications the right side shows: supervisorGroups' groups that serve
 * `queue` (when the page is in a queue's context), pass the search and the
 * Show filter (filterSupervisorGroups), and — unless the Pools filter is
 * 'all' — have a pool it keeps. Not narrowed to the one picked: that is the
 * page's, by name.
 */
export function supervisorApplications(groups = [], { search = '', filter = 'all', queue = '', pools = 'all' } = {}) {
  const scoped = queue ? groups.filter((g) => g.queues.some((q) => q.name === queue)) : groups
  const listed = filterSupervisorGroups(scoped, { search, filter })
  return pools === 'all' ? listed : listed.filter((g) => applicationPools(g, { queue, pools }).length > 0)
}
