// The Workers view of the Queues section (/queues?view=workers), as data.
//
// Worker processes publish their status into Queen KV: a Laravel supervisor
// (PHP or Rust engine) with QUEEN_SUPERVISOR_REMOTE_STATUS=true, and an SDK
// consumer whose consume has supervision on. Every instance writes its own
// slot, <group>/<instance>/head plus chunks, in the queen-supervisor namespace,
// at each heartbeat and with a TTL. The old Supervisors page's composables read
// and judge those reports (supervisorStatus, supervisorGroups, supervisorsSide);
// this file turns their answer into the view's rows and words:
//   - one row per pool (a pool name, the queue it reads, and for an SDK
//     consumer its consumer group) across the instances that run it, under its
//     application (the publication group);
//   - a pool's state comes from the instances that report now. An instance that
//     does not (overdue, expired, unreadable, incomplete, paused) marks its
//     application once, not every pool it runs: one colour per fact;
//   - rows stay in name order, so a refresh never moves one;
//   - the line on top, the segments, and what the right side shows.
//
// Pure: no Vue, no '@/' imports, so test/queues-workers.test.js holds it.
import { supervisorGroupPrefix } from '../../composables/supervisorStatus.js'

export const NAMESPACE = 'queen-supervisor'
export const QUEUE_MODE = '__QUEUE_MODE__'
/** A big application shows this many pools before "Show all". */
export const POOL_ROWS = 12

export const SEGMENTS = [
  { value: 'all', label: 'All' },
  { value: 'needs', label: 'Need you' },
  { value: 'draining', label: 'Draining' },
  { value: 'silent', label: 'Not reporting' },
  { value: 'stopped', label: 'Stopped' },
]
export const SEGMENT_VALUES = SEGMENTS.map((s) => s.value)

// ---- words ---------------------------------------------------------------------

export const fmt = (n) => (n == null || !Number.isFinite(Number(n)) ? '–' : Math.round(Number(n)).toLocaleString('en-US'))
const plural = (n, one, many = `${one}s`) => (n === 1 ? one : many)
const two = (n) => String(n).padStart(2, '0')
/** 14:02:09, the browser's clock. */
export function hms(ms) {
  if (!Number.isFinite(ms)) return '–'
  const d = new Date(ms)
  return `${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}`
}
/** 4 s, 38 min, 2 h 5 min, 3 d 4 h (the console's span). */
export function span(seconds) {
  if (seconds == null || !Number.isFinite(seconds)) return '–'
  const s = Math.max(0, Math.round(seconds))
  if (s < 60) return `${s} s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min`
  if (m < 24 * 60) { const h = Math.floor(m / 60); const mm = m % 60; return mm ? `${h} h ${mm} min` : `${h} h` }
  const hours = Math.round(s / 3600)
  const d = Math.floor(hours / 24); const hh = hours % 24
  return hh ? `${d} d ${hh} h` : `${d} d`
}
const DAY = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short' })
const dayTime = (ms) => `${DAY.format(new Date(ms))} ${hms(ms).slice(0, 5)}`
const list = (xs) => (xs.length < 2 ? xs.join('') : `${xs.slice(0, -1).join(', ')} and ${xs.at(-1)}`)

/** A long name in pieces after . - _ / : so it wraps at a joint, never mid-word. */
export const breakable = (text) => String(text ?? '').split(/(?<=[._\-/:])/)

const ENGINES = { php: 'PHP', rust: 'Rust', js: 'JS', go: 'Go', python: 'Python', cpp: 'C++', cli: 'CLI' }
export const engineName = (e) => (e ? ENGINES[e] || String(e).toUpperCase() : '')
const MODELS = { 'async-tasks': 'async tasks', goroutines: 'goroutines', threads: 'threads', cooperative: 'cooperative tasks' }

// The old page's labels (supervisorStatus, consumerStatus), in the console's words.
const WORDS = {
  'At desired capacity': 'At target',
  'Consumers running': 'At target',
  'Below desired capacity': 'Below target',
  'Consumer capacity reduced': 'Below target',
  'No process headroom': 'No free process slot',
  'Pending work, no workers': 'Work waits, no worker',
  'Incomplete telemetry': 'Incomplete report',
  'Readiness not confirmed': 'Not confirmed ready',
  'Workers draining': 'Draining',
  'Idle · zero target': 'Target is zero',
  'Publication expired': 'Report expired',
  'Heartbeat unconfirmed': 'Heartbeat time unclear',
  'Status unavailable': 'Report unreadable',
  'Current state unconfirmed': 'Unconfirmed',
  'State unknown': 'State unknown',
}
export const say = (label) => WORDS[label] || label || '–'
export const toneOf = (severity) => (severity === 'bad' ? 'red' : severity === 'warn' ? 'amber' : '')
const SEV = { bad: 2, warn: 1 }
const TONE = { red: 2, amber: 1 }
export const groupLabel = (g) => (g === QUEUE_MODE ? 'queue mode' : g)
const byText = (a, b) => String(a).localeCompare(String(b))
const host = (row) => row.hostname || row.group || row.slot
const hostOrder = (a, b) => byText(host(a), host(b)) || byText(a.slot, b.slot)

/** "Laravel, Rust engine" | "Go SDK" | "Laravel and JS SDK": who publishes for an application. */
export function kindText(rows = []) {
  const sup = [...new Set(rows.filter((r) => !r.consumer && r.engine).map((r) => engineName(r.engine)))].sort()
  const sdk = [...new Set(rows.filter((r) => r.consumer && r.engine).map((r) => engineName(r.engine)))].sort()
  if (sdk.length && !sup.length) return `${list(sdk)} SDK`
  if (sup.length && !sdk.length) return `Laravel, ${list(sup)} ${plural(sup.length, 'engine')}`
  if (sup.length && sdk.length) return `Laravel and ${list(sdk)} SDK`
  return 'engine not reported'
}

// ---- an instance -------------------------------------------------------------------

/** A complete report from a running instance: its pools' numbers are current. */
export const reportsNow = (row) => !!row && row.fresh && row.state === 'running' && row.affectedPools !== null && !row.terminal

/**
 * What an instance's report says about the instance itself:
 *   { kind, word, tone, issue, silent }
 * `tone` is the instance's own mark in a list of instances (its worst pool
 * when it runs); `issue` is a fact about the instance, which its application
 * carries once; `silent` means it does not report now.
 */
export function instanceState(row) {
  if (!row) return { kind: 'unknown', word: '–', tone: '', issue: false, silent: false }
  if (row.terminal) return { kind: 'stopped', word: 'Stopped', tone: '', issue: false, silent: false }
  if (!row.fresh) return { kind: 'silent', word: say(row.label), tone: 'amber', issue: true, silent: true }
  if (row.state === 'running') {
    if (row.affectedPools === null) return { kind: 'incomplete', word: say(row.label), tone: 'amber', issue: true, silent: true }
    const worst = (row.pools || []).reduce((a, p) => ((SEV[p.severity] || 0) > (SEV[a?.severity] || 0) ? p : a), null)
    if (worst?.severity) return { kind: 'running', word: say(worst.label), tone: toneOf(worst.severity), issue: false, silent: false }
    if (row.severity) return { kind: 'unready', word: say(row.label), tone: 'amber', issue: true, silent: false }
    return { kind: 'running', word: say(row.label), tone: '', issue: false, silent: false }
  }
  if (row.state === 'paused') return { kind: 'paused', word: 'Paused', tone: 'amber', issue: true, silent: false }
  if (!row.state) return { kind: 'unknown', word: say(row.label), tone: 'amber', issue: true, silent: false }
  return { kind: row.state, word: say(row.label), tone: '', issue: false, silent: false }
}

// ---- pools ---------------------------------------------------------------------

/** The queue a pool reads: a queue name, or null for a consumer reading a whole namespace or task. */
const queueNameOf = (row, pool) => (row.consumer ? pool.queueName || null : pool.queue)
const groupOf = (row, pool) => (row.consumer ? pool.configuration?.consumerGroup || null : null)
/** A pool's id in the address (?wo=), the same across reads. */
export const poolId = (app, pool, consumerGroup = null) =>
  `pool:${app}/${pool.name}/${pool.queue}${consumerGroup ? `/${consumerGroup}` : ''}`
export const appId = (app) => `app:${app}`
export const instanceId = (slot) => `host:${slot}`

/**
 * An application's pools, one row each across the instances that run it, by
 * name then queue. `queue` keeps the pools reading that queue.
 *   { id, app, name, rawName, consumer, group, queue, queueName, tone, word,
 *     next, note, running, desired, reporting, ongoing, silent, stopped,
 *     draining, lead, holders }
 * `running` / `desired` add up the instances that report now (null when none
 * does, or one left a number out). `lead` is the slot to open for it.
 */
export function poolRows(group, { queue = '' } = {}) {
  const app = group?.name ?? ''
  const byId = new Map()
  for (const row of [...(group?.rows || [])].sort(hostOrder)) {
    for (const pool of row.pools || []) {
      const queueName = queueNameOf(row, pool)
      if (queue && queueName !== queue) continue
      const cg = groupOf(row, pool)
      const id = poolId(app, pool, cg)
      if (!byId.has(id)) {
        byId.set(id, {
          id, app, rawName: pool.name, consumer: !!row.consumer, group: cg,
          name: row.consumer && cg ? groupLabel(cg) : pool.name,
          queue: pool.queue, queueName, holders: [],
        })
      }
      byId.get(id).holders.push({ row, pool })
    }
  }
  const out = []
  for (const p of byId.values()) {
    const ongoing = p.holders.filter((h) => !h.row.terminal)
    const reporting = ongoing.filter((h) => reportsNow(h.row))
    const sum = (field) => (reporting.length && reporting.every((h) => Number.isSafeInteger(h.pool[field]))
      ? reporting.reduce((n, h) => n + h.pool[field], 0) : null)
    const draining = ongoing.reduce((n, h) => n + (Number.isSafeInteger(h.pool.draining) ? h.pool.draining : 0), 0)
    let tone = ''; let word; let next; let lead
    if (reporting.length) {
      const worst = reporting.reduce((a, h) => ((SEV[h.pool.severity] || 0) > (SEV[a.pool.severity] || 0) ? h : a), reporting[0])
      lead = worst
      tone = toneOf(worst.pool.severity)
      word = say(worst.pool.label)
      next = worst.pool.next
    } else if (ongoing.length) {
      // No instance reports it now: its application says why, once.
      lead = ongoing[0]
      const s = instanceState(lead.row)
      word = s.silent ? 'Not reporting' : s.word
      next = lead.row.next
    } else {
      lead = p.holders[0]
      word = 'Stopped'
      next = lead.row.next
    }
    out.push({
      ...p, tone, word, next,
      note: draining && word !== 'Draining' ? `${fmt(draining)} draining` : '',
      running: sum('running'), desired: sum('desired'),
      reporting: reporting.length, ongoing: ongoing.length,
      silent: ongoing.length - reporting.length, stopped: p.holders.length - ongoing.length,
      draining, lead: lead.row.slot,
    })
  }
  return out.sort((a, b) => byText(a.name, b.name) || byText(a.queue, b.queue) || byText(a.id, b.id))
}

// ---- applications ----------------------------------------------------------------

function heardText(rows) {
  const ongoing = rows.filter((r) => !r.terminal)
  const now = ongoing.filter(reportsNow).map((r) => r.age).filter((a) => a != null)
  if (now.length) return `heard within ${span(Math.max(0, ...now))}`
  const ages = ongoing.map((r) => r.age).filter((a) => a != null && a >= -5)
  if (ages.length) return `last heard ${span(Math.min(...ages))} ago`
  return ''
}

/** One application's own line: the facts about its instances, and its mark when one of them needs you. */
export function appSummary(group) {
  const rows = group?.rows || []
  const ongoing = rows.filter((r) => !r.terminal)
  const states = ongoing.map(instanceState)
  const count = (pred) => states.filter(pred).length
  const silent = count((s) => s.silent)
  const paused = count((s) => s.kind === 'paused')
  const unknown = count((s) => s.kind === 'unknown')
  const unready = count((s) => s.kind === 'unready')
  const issues = []
  if (silent) issues.push(`${fmt(silent)} not reporting`)
  if (paused) issues.push(`${fmt(paused)} paused`)
  if (unready) issues.push(`${fmt(unready)} not confirmed ready`)
  if (unknown) issues.push(`${fmt(unknown)} in an unknown state`)
  const stopped = rows.length - ongoing.length
  const instances = ongoing.length
    ? `${fmt(ongoing.length)} ${plural(ongoing.length, 'instance')}${stopped ? `, ${fmt(stopped)} stopped` : ''}`
    : `${fmt(stopped)} stopped ${plural(stopped, 'instance')}`
  return {
    name: group?.name ?? '',
    kindText: kindText(rows),
    tone: issues.length ? 'amber' : '',
    issues, instances, heard: heardText(rows),
    ongoing: ongoing.length, stopped, silent,
  }
}

/** Instances that report no pool at all (an unreadable or incomplete report): they get a row of their own. */
export function orphanRows(group) {
  return [...(group?.rows || [])].filter((r) => !(r.pools || []).length).sort(hostOrder).map((r) => {
    const s = instanceState(r)
    return {
      id: instanceId(r.slot), slot: r.slot, host: host(r), word: s.word, tone: s.issue ? '' : s.tone,
      silent: s.silent, terminal: r.terminal, issue: s.issue, next: r.next,
    }
  })
}

const has = (hay, term) => hay.filter(Boolean).join(' ').toLocaleLowerCase().includes(term)
const instHay = (r) => [r.hostname, r.instance, r.engine, engineName(r.engine)]

/** Whether a pool row is in a segment; `app` is its application's summary. */
export function inSegment(p, seg, app) {
  if (seg === 'needs') return !!p.tone || !!app?.tone
  if (seg === 'draining') return p.draining > 0
  if (seg === 'silent') return p.silent > 0
  if (seg === 'stopped') return p.stopped > 0
  return true
}
const orphanIn = (o, seg) => (seg === 'needs' ? o.issue || !!o.tone : seg === 'silent' ? o.silent : seg === 'stopped' ? o.terminal : seg !== 'draining')

/**
 * The table: every application by name with its pools (and its instances
 * that report no pool), after the queue, the filter and the segment.
 *   [{ ...appSummary, pools, orphans, total, hidden }]
 * `find` matches an application, a host, an instance id, an engine, a pool,
 * a queue or a consumer group; a match on the application keeps all of it.
 * `expanded` names the applications showing all their pools.
 */
export function appRows(groups = [], { queue = '', find = '', seg = 'all', expanded = null } = {}) {
  const term = String(find || '').trim().toLocaleLowerCase()
  const narrowed = !!term || (seg && seg !== 'all')
  const out = []
  for (const g of [...groups].sort((a, b) => byText(a.name, b.name))) {
    const app = appSummary(g)
    const all = poolRows(g, { queue })
    const orphansAll = queue ? [] : orphanRows(g)
    if (queue && !all.length) continue
    const whole = !term || has([g.name], term)
    const pools = all.filter((p) => inSegment(p, seg, app) && (whole || has([p.name, p.rawName, p.queue, p.group, ...p.holders.flatMap((h) => instHay(h.row))], term)))
    const orphans = orphansAll.filter((o) => orphanIn(o, seg) && (whole || has(instHay((g.rows || []).find((r) => r.slot === o.slot) || {}), term)))
    if (narrowed && !pools.length && !orphans.length) continue
    const cut = !narrowed && pools.length > POOL_ROWS && !(expanded && expanded.has?.(g.name))
    out.push({ ...app, pools: cut ? pools.slice(0, POOL_ROWS - 2) : pools, orphans, total: all.length, hidden: cut ? pools.length - (POOL_ROWS - 2) : 0 })
  }
  return out
}

/** How many rows each segment holds, before the filter (the Queues tab's rule). */
export function segmentCounts(groups = [], { queue = '' } = {}) {
  const counts = Object.fromEntries(SEGMENT_VALUES.map((v) => [v, 0]))
  for (const g of groups) {
    const app = appSummary(g)
    const pools = poolRows(g, { queue })
    const orphans = queue ? [] : orphanRows(g)
    for (const v of SEGMENT_VALUES) {
      counts[v] += pools.filter((p) => inSegment(p, v, app)).length + orphans.filter((o) => orphanIn(o, v)).length
    }
  }
  return counts
}

// ---- the line on top ---------------------------------------------------------------

/**
 * One line: is any worker pool wrong, and which.
 *   { kind: 'reading'|'failed'|'refused'|'empty'|'issues'|'fine'|'calm'|'stopped', ... }
 * `failure` is the first read's error text when nothing was ever read;
 * `refusal` a stable verdict (absent | gated | paused) of the KV listing.
 */
export function workersVerdict({ groups = [], loaded = false, failure = null, refusal = null, queue = '' } = {}) {
  if (refusal) return { kind: 'refused', refusal }
  if (!loaded) return failure ? { kind: 'failed', text: failure } : { kind: 'reading' }
  const scoped = queue ? groups.filter((g) => poolRows(g, { queue }).length) : groups
  if (!scoped.length) return { kind: 'empty', queue }
  const apps = scoped.map((g) => {
    const s = appSummary(g)
    const pools = poolRows(g, { queue })
    const worst = pools.reduce((t, p) => Math.max(t, TONE[p.tone] || 0), TONE[s.tone] || 0)
    return { name: g.name, worst, summary: s, pools }
  })
  const flagged = apps.filter((a) => a.worst > 0).sort((a, b) => b.worst - a.worst || byText(a.name, b.name))
  const pools = apps.flatMap((a) => a.pools)
  const running = pools.reduce((n, p) => n + (p.running || 0), 0)
  if (flagged.length) {
    return {
      kind: 'issues', tone: flagged[0].worst === 2 ? 'red' : 'amber',
      names: flagged.slice(0, 2).map((a) => a.name), more: Math.max(0, flagged.length - 2),
      count: flagged.length, others: apps.length - flagged.length,
    }
  }
  if (!apps.some((a) => a.summary.ongoing)) return { kind: 'stopped', apps: apps.length }
  const settled = pools.every((p) => p.reporting && ['At target', 'Target is zero', 'Draining'].includes(p.word))
  const tail = `${fmt(running)} ${plural(running, 'worker')} on ${fmt(pools.length)} ${plural(pools.length, 'pool')}, in ${fmt(apps.length)} ${plural(apps.length, 'application')}.`
  if (settled) return { kind: 'fine', tail }
  // Nothing needs you, but not every pool is at its target yet: say which, by state.
  const moving = new Map()
  for (const p of pools) {
    if (p.reporting && ['At target', 'Target is zero', 'Draining'].includes(p.word)) continue
    moving.set(p.word, (moving.get(p.word) || 0) + 1)
  }
  return { kind: 'calm', tail, why: list([...moving].map(([w, n]) => `${fmt(n)} ${w.toLowerCase()}`)) }
}

// ---- the right side -----------------------------------------------------------------

const yesNo = (v, yes, no, unknown) => (v === true ? yes : v === false ? no : unknown)
const secs = (v) => (v == null ? '–' : `${fmt(v)} s`)

const shortId = (row) => (row.instance ? `…${row.instance.slice(-8)}` : '')
const heardAgo = (row) => (row.age == null ? '' : row.age < -5 ? 'heard ahead of this clock' : `heard ${span(Math.max(0, row.age))} ago`)
/** What a pool's numbers on one instance add, only where they are not zero. */
function poolExtras(row, pool) {
  const out = []
  if (row.consumer) {
    if (pool.busy) out.push(`${fmt(pool.busy)} busy`)
    if (pool.failed) out.push(`${fmt(pool.failed)} failed ${plural(pool.failed, 'call')}`)
  } else {
    if (pool.depth) out.push(`${fmt(pool.depth)} waiting`)
    if (pool.draining) out.push(`${fmt(pool.draining)} draining`)
  }
  return out
}

/** The heartbeat in words: "14:02:09, 4 s before the read". */
export function heartbeatText(row) {
  if (!row?.updatedAt) return 'not in the report'
  if (row.age != null && row.age < -5) return `${hms(row.updatedAt)}, ahead of this browser's clock`
  return `${hms(row.updatedAt)}, ${span(Math.max(0, row.age ?? 0))} before the read`
}

/** The picked application: its figures and its instances. */
export function appDetail(group) {
  if (!group) return null
  const s = appSummary(group)
  const facts = []
  const of = group.reportingCount && group.reportingCount !== s.ongoing ? `, from ${fmt(group.reportingCount)} of ${fmt(s.ongoing)} instances` : ''
  facts.push({ k: 'Workers', v: group.workers == null ? 'not known: no instance reports now' : `${fmt(group.workers)} running of ${fmt(group.desired)} wanted${of}` })
  // Which pools need you is on the left, one mark per pool.
  if (group.consumerOnly) {
    if (group.busy != null) facts.push({ k: 'Busy now', v: `${fmt(group.busy)} ${plural(group.busy, 'handler')}` })
    if (group.handlerFailures != null) facts.push({ k: 'Handler failures', v: `${fmt(group.handlerFailures)}, since each instance started` })
  } else if (group.processOnly) {
    if (group.draining != null) facts.push({ k: 'Draining', v: `${fmt(group.draining)} ${plural(group.draining, 'worker')}` })
    if (group.headroom != null) facts.push({ k: 'Free slots', v: `${fmt(group.headroom)} ${plural(group.headroom, 'process slot')} the shared budgets can still fit` })
  }
  const notes = []
  if (group.partial) notes.push('More reports are stored than were read: this counts the instances read so far.')
  if (group.scopedConsumers) notes.push('Some consumers read a whole namespace or task, not one queue.')
  // By host, whatever their state: an instance that stops keeps its place.
  const instances = [...(group.rows || [])].sort(hostOrder).map((r) => {
    const st = instanceState(r)
    return {
      id: instanceId(r.slot), slot: r.slot, host: host(r), word: st.word, tone: st.tone, terminal: r.terminal,
      subId: shortId(r), sub: [engineName(r.engine), heardAgo(r)].filter(Boolean).join(' · '),
      workers: r.workers == null ? '–' : `${fmt(r.workers)} / ${fmt(r.desired)}`,
    }
  })
  return { ...s, kind: 'app', id: appId(group.name), name: group.name, facts, notes, instances }
}

function settingsOf(pool, consumer) {
  const c = pool?.configuration
  if (consumer) return c?.consumerGroup ? [{ k: 'Consumer group', v: groupLabel(c.consumerGroup), mono: c.consumerGroup !== QUEUE_MODE }] : []
  if (!c) return []
  const out = []
  if (c.consumerGroup) out.push({ k: 'Consumer group', v: c.consumerGroup, mono: true })
  if (c.connection) out.push({ k: 'Connection', v: c.connection, mono: true })
  if (c.balance || c.strategy) out.push({ k: 'Scaling', v: [c.balance, c.strategy && `${c.strategy} strategy`].filter(Boolean).join(', ') })
  if (c.min != null || c.max != null) out.push({ k: 'Workers', v: `${fmt(c.min)} to ${fmt(c.max)} per instance` })
  if (pool.cost != null) out.push({ k: 'Per worker', v: `${fmt(pool.cost)} ${plural(pool.cost, 'process', 'processes')}${pool.cost > 1 ? ', with its lease renewal helper' : ''}` })
  if (pool.replicas != null) out.push({ k: 'Replicas', v: `${fmt(pool.replicas)} coordinated` })
  if (c.timeout != null) out.push({ k: 'Job timeout', v: secs(c.timeout) })
  if (c.lease != null || c.leaseRenewal != null) out.push({ k: 'Lease', v: `${secs(c.lease)}${yesNo(c.leaseRenewal, ', renewed while a job runs', ', not renewed', '')}` })
  if (c.tries != null) out.push({ k: 'Attempts', v: c.tries === 0 ? 'unlimited' : fmt(c.tries) })
  if (c.memoryLimit != null) out.push({ k: 'Memory', v: `${fmt(c.memoryLimit)} MB per worker` })
  return out
}

/** The picked pool: what to check, the instances that run it, its settings. */
export function poolDetail(group, id) {
  if (!group) return null
  const p = poolRows(group).find((x) => x.id === id)
  if (!p) return null
  const hosts = p.holders.slice().sort((a, b) => hostOrder(a.row, b.row)).map(({ row, pool }) => {
    const now = reportsNow(row)
    const st = instanceState(row)
    return {
      id: instanceId(row.slot), slot: row.slot, host: host(row),
      word: now ? say(pool.label) : st.word, tone: now ? toneOf(pool.severity) : st.tone,
      workers: pool.running == null ? '–' : `${fmt(pool.running)} / ${fmt(pool.desired)}`,
      // Laravel: the queue depth it measured and its draining workers; an SDK consumer: busy handlers and failed calls.
      subId: shortId(row), sub: [heardAgo(row), ...poolExtras(row, pool)].filter(Boolean).join(' · '),
      terminal: row.terminal, now,
    }
  })
  const lead = p.holders.find((h) => h.row.slot === p.lead) || p.holders[0]
  const failures = p.holders.filter((h) => h.pool.failures).map((h) => ({ host: host(h.row), failures: h.pool.failures, retryIn: h.pool.retryIn }))
  return {
    kind: 'pool', id: p.id, app: group.name, appId: appId(group.name), name: p.name, rawName: p.rawName,
    consumer: p.consumer, queue: p.queue, queueName: p.queueName, tone: p.tone, word: p.word, next: p.next,
    note: p.note, hosts, settings: settingsOf(lead?.pool, p.consumer), failures,
  }
}

/** The picked instance: its report in full. */
export function instanceDetail(group, slot) {
  const row = (group?.rows || []).find((r) => r.slot === slot)
  if (!row) return null
  const st = instanceState(row)
  const facts = [{ k: 'Heartbeat', v: heartbeatText(row) }]
  if (row.timeout) facts.push({ k: 'Timeout', v: `${fmt(row.timeout)} s: a report older than that is overdue` })
  if (row.uptime != null) facts.push({ k: 'Up for', v: `${span(row.uptime)}${row.startedAt ? `, since ${dayTime(row.startedAt)}` : ''}` })
  if (row.pid) facts.push({ k: row.consumer ? 'Process' : 'Master', v: `PID ${row.pid}`, mono: true })
  const versions = [row.engineVersion && `engine ${row.engineVersion}`, row.clientVersion && `client ${row.clientVersion}`].filter(Boolean)
  if (versions.length) facts.push({ k: 'Versions', v: versions.join(', '), mono: true })
  const one = row.consumer && row.pools?.length === 1 ? row.pools[0] : null
  if (one) {
    const g = groupOf(row, one)
    if (g) facts.push({ k: 'Group', v: groupLabel(g), mono: g !== QUEUE_MODE })
    facts.push({ k: 'Queue', v: one.queue, mono: true, to: one.queueName || '' })
  }
  if (row.workers != null) {
    // "below target" only where surplus elsewhere hides it: one pool short while another runs over.
    const hidden = row.missingWorkers && row.missingWorkers !== row.desired - row.workers
    const extra = [hidden ? `${fmt(row.missingWorkers)} below target in their pools` : '', !row.consumer && row.draining ? `${fmt(row.draining)} draining` : ''].filter(Boolean)
    facts.push({ k: 'Workers', v: `${fmt(row.workers)} running of ${fmt(row.desired)} wanted${extra.length ? `, ${extra.join(', ')}` : ''}` })
  }
  if (!row.consumer && row.affectedPools != null) facts.push({ k: 'Pools to check', v: `${fmt(row.affectedPools)} of ${fmt(row.poolCount)}` })
  if (!row.consumer && st.kind === 'running') facts.push({ k: 'Readiness', v: yesNo(row.readiness, 'ready', 'not ready', 'not confirmed by its report') })
  if (row.consumer) {
    if (row.executionModel) facts.push({ k: 'Runs as', v: MODELS[row.executionModel] || row.executionModel })
  } else if (row.budget) {
    const b = row.budget
    facts.push({ k: 'Processes', v: `${fmt(b.used)} of ${fmt(b.limit)} slots used, ${fmt(b.available)} free${b.helpers ? `; ${fmt(b.helpers)} hold lease renewal helpers` : ''}` })
  }
  const pools = [...(row.pools || [])].sort((a, b) => byText(a.name, b.name) || byText(a.queue, b.queue)).map((pool) => {
    const now = reportsNow(row)
    return {
      id: poolId(row.group, pool, groupOf(row, pool)), name: row.consumer && groupOf(row, pool) ? groupLabel(groupOf(row, pool)) : pool.name,
      queue: pool.queue, queueName: queueNameOf(row, pool),
      word: now ? say(pool.label) : row.terminal ? 'Stopped' : 'Last reported', tone: now ? toneOf(pool.severity) : '',
      workers: pool.running == null ? '–' : `${fmt(pool.running)} / ${fmt(pool.desired)}`,
      subId: pool.queue, sub: poolExtras(row, pool).join(' · '),
      pids: pool.pids || [], failures: pool.failures || 0, retryIn: pool.retryIn,
    }
  })
  const handler = row.consumer && row.pools?.length === 1 ? (() => {
    const p = row.pools[0]
    return [
      { k: 'Handler calls', v: `${fmt(p.completed)} finished, ${fmt(p.failed)} failed, since it started` },
      { k: 'Last finished', v: p.lastCompleted ? hms(p.lastCompleted) : 'none yet' },
      { k: 'Oldest running', v: p.busy ? span(p.oldest) : 'nothing in flight' },
    ]
  })() : []
  return {
    kind: 'host', id: instanceId(row.slot), slot: row.slot, app: row.group, appId: appId(row.group),
    title: host(row), consumer: !!row.consumer,
    kicker: row.consumer ? `${engineName(row.engine) || 'SDK'} consumer` : `Laravel supervisor${row.engine ? `, ${engineName(row.engine)} engine` : ''}`,
    short: shortId(row),
    word: st.word, tone: st.tone, next: row.next, onePool: !!one,
    last: row.terminal ? 'Its final report: this instance has stopped and sends no more heartbeats.'
      : !row.fresh ? 'These are the values of its last report; how it is doing now is not known.' : '',
    facts, pools, handler,
    publication: [
      { k: 'Group', v: `${row.group}${row.legacy ? ' (the old key layout)' : ''}`, mono: true },
      { k: 'Instance', v: row.instance || 'not in the key', mono: true },
      { k: 'Key', v: `${row.slot}/head`, mono: true },
    ],
  }
}

// ---- the source ---------------------------------------------------------------------

/** The read the view makes, as the call it sends: `POST /api/v1/resources/kv/list` and its body. */
export function listCall({ namespace = NAMESPACE, group = '', after = null } = {}) {
  const prefix = supervisorGroupPrefix(group)
  const body = { namespace, limit: 100, includeExpired: true }
  if (prefix) body.prefix = prefix
  if (after) body.after = after
  return { method: 'POST', path: '/api/v1/resources/kv/list', body }
}

/** Why a source cannot be read, or '' when it can. */
export function sourceError({ namespace = '', group = '' } = {}) {
  const ns = String(namespace || '').trim()
  if (!ns) return 'Name the KV namespace the workers write to.'
  if (ns.length > 64) return 'A KV namespace has at most 64 characters.'
  try { supervisorGroupPrefix(String(group || '').trim()) } catch (e) { return e.message }
  return ''
}
