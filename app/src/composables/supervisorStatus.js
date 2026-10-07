// Language-independent contract, currently published by RemoteStatusDocument.php
// and supervisor/src/remote_status.rs. Decode a slot from ONE KV page only.
export const STATUS_FORMAT = 'queen.supervisor.remote-status/v1'
export const validSupervisorGroup = group => typeof group === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$/.test(group) && group !== 'coordination'
export const supervisorGroupPrefix = group => {
  if (group === '') return ''
  if (!validSupervisorGroup(group)) throw new Error('Use 1–255 letters, digits, dots, underscores or hyphens, starting with a letter or digit. “coordination” is reserved.')
  return `${group}/`
}
const MAX_BYTES = 1_048_576
const MAX_CHUNKS = 24
const ID = /^[0-9a-f]{16,128}$/
const WRITE = /^[0-9a-f]{32}$/
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value)
const integer = (value, max = Number.MAX_SAFE_INTEGER) => Number.isSafeInteger(value) && value >= 0 && value <= max ? value : null
const text = (value, max = 255) => typeof value === 'string' && value.length > 0 && value.length <= max && !/[\x00-\x1f\x7f]/.test(value) ? value : null
const sumKnown = values => values.some(v => v === null) ? null : values.reduce((a, b) => a + b, 0)

export function supervisorPart(key) {
  if (typeof key !== 'string' || key.startsWith('coordination/')) return null
  const match = /^(.*)\/(head|chunk\/[0-9]{4})$/.exec(key)
  if (!match || !text(match[1], 1024)) return null
  const slot = match[1]
  const slash = slot.lastIndexOf('/')
  const instance = slot.slice(slash + 1)
  const perInstance = slash > 0 && ID.test(instance)
  const group = perInstance ? slot.slice(0, slash) : slot
  // Keep a legacy key intact: never merge foo/bar into the canonical group foo.
  return { slot, part: match[2], group, instance: perInstance ? instance : null, legacy: !perInstance || !validSupervisorGroup(group) }
}

export function decodeSupervisorSlot(rows, address) {
  const values = new Map(rows.map(row => [row.key, row]))
  const headRow = values.get(`${address.slot}/head`)
  const head = headRow?.value
  const unavailable = reason => ({ ...address, document: null, reason, expired: headRow?.expired === true })
  if (typeof headRow?.expired !== 'boolean' || !object(head) || typeof head.write !== 'string' || !WRITE.test(head.write) || !Number.isInteger(head.chunks) || head.chunks < 1 || head.chunks > MAX_CHUNKS
    || !Number.isInteger(head.bytes) || head.bytes < 1 || head.bytes > MAX_BYTES) return unavailable('Incomplete or invalid status document')
  if (head.format !== STATUS_FORMAT) return unavailable('Unsupported supervisor status format')
  const bytes = new Uint8Array(head.bytes)
  let offset = 0
  let expired = headRow.expired === true
  try {
    for (let index = 0; index < head.chunks; index++) {
      const row = values.get(`${address.slot}/chunk/${String(index).padStart(4, '0')}`)
      const chunk = row?.value
      if (typeof row?.expired !== 'boolean' || !object(chunk) || chunk.write !== head.write || chunk.index !== index || typeof chunk.data !== 'string'
        || chunk.data.length > 60_000 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(chunk.data)) return unavailable('Incomplete status: waiting for a consistent heartbeat')
      const slice = atob(chunk.data)
      if (offset + slice.length > head.bytes) return unavailable('Invalid status length')
      for (let i = 0; i < slice.length; i++) bytes[offset++] = slice.charCodeAt(i)
      expired ||= row.expired === true
    }
    if (offset !== head.bytes) return unavailable('Incomplete status document')
    const document = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes))
    if (!object(document) || document.schema !== 'queen.supervisor.status/v1' || typeof document.instance_id !== 'string' || !ID.test(document.instance_id)
      || (address.instance && address.instance !== document.instance_id) || !Array.isArray(document.pool_status)
      || document.pool_status.length > 256 || !object(document.configuration)) return unavailable('Invalid supervisor identity or schema')
    return { ...address, document, expired, reason: null }
  } catch {
    return unavailable('Invalid supervisor status encoding')
  }
}

/** One bounded scan. A trailing slot is re-read on the next page instead of
 * joining chunks from independent snapshots. The cursor stays in the body. */
export async function readSupervisorPage(list, { namespace, group = '', after = null, signal } = {}) {
  const prefix = supervisorGroupPrefix(group)
  const body = { namespace, limit: 100, includeExpired: true }
  if (prefix) body.prefix = prefix
  if (after) body.after = after
  const data = await list(body, { signal, probe: true })
  if (!Array.isArray(data?.rows) || typeof data.truncated !== 'boolean'
    || data.rows.some(row => !object(row) || typeof row.key !== 'string' || (prefix && !row.key.startsWith(prefix)))) throw new Error('Invalid KV listing response')
  const keys = data.rows.map(row => row.key)
  // Byte order is the broker's keyset contract. Keys are UTF-8, not locale sorted.
  const compare = (a, b) => {
    const encoder = new TextEncoder(), aa = encoder.encode(a), bb = encoder.encode(b)
    for (let i = 0; i < Math.min(aa.length, bb.length); i++) if (aa[i] !== bb[i]) return aa[i] - bb[i]
    return aa.length - bb.length
  }
  if (keys.some((key, i) => (i ? compare(key, keys[i - 1]) <= 0 : after && compare(key, after) <= 0))
    || (data.truncated && (keys.length === 0 || data.nextAfter !== keys.at(-1)))) throw new Error('Invalid KV pagination response')
  let keep = keys.length
  if (data.truncated) {
    const lastSlot = supervisorPart(keys.at(-1))?.slot
    if (lastSlot) while (keep > 0 && supervisorPart(keys[keep - 1])?.slot === lastSlot) keep--
    // A malformed slot larger than a page must not trap the cursor forever.
    if (keep === 0) keep = keys.length
  }
  const slots = new Map()
  for (const row of data.rows.slice(0, keep)) {
    const address = supervisorPart(row.key)
    if (!address || (group && address.group !== group)) continue
    if (!slots.has(address.slot)) slots.set(address.slot, { address, rows: [] })
    slots.get(address.slot).rows.push(row)
  }
  const entries = []
  for (const { address, rows } of slots.values()) {
    const head = rows.find(row => row.key === `${address.slot}/head`)
    // Ignore unrelated KV values and the replica-coordination keys. Recognize
    // broken chunks too, so a torn status cannot become a healthy empty list.
    const recognized = typeof head?.value?.format === 'string' && head.value.format.startsWith('queen.supervisor.remote-status/')
      || (!head && rows.some(row => typeof row.value?.write === 'string' && WRITE.test(row.value.write) && Number.isInteger(row.value?.index)))
    if (recognized) entries.push(decodeSupervisorSlot(rows, address))
  }
  return { entries, after: data.truncated ? keys[keep - 1] : null }
}

const boolean = value => typeof value === 'boolean' ? value : null

function poolConfigurations(raw) {
  const configs = new Map()
  if (!Array.isArray(raw) || raw.length > 256) return configs
  for (const config of raw) {
    if (!object(config) || !text(config.name)) continue
    configs.set(config.name, configs.has(config.name) ? null : config)
  }
  return configs
}

function normalizeConfiguration(config, queue) {
  if (!object(config) || !Array.isArray(config.queues) || !config.queues.includes(queue)) return null
  let min = integer(config.min_processes, 4096), max = integer(config.max_processes, 4096)
  if (min !== null && max !== null && min > max) min = max = null
  return {
    connection: text(config.connection), consumerGroup: text(config.consumer_group),
    balance: text(config.balance, 64), strategy: text(config.strategy, 64), min, max,
    timeout: integer(config.timeout, 31_536_000), lease: integer(config.retry_after, 31_536_000),
    leaseRenewal: boolean(config.lease_renewal), tries: integer(config.tries), memoryLimit: integer(config.memory),
  }
}

function normalizePool(raw, limit, configs) {
  if (!object(raw) || !text(raw.supervisor ?? raw.name) || !text(raw.queue)) return null
  const running = integer(raw.running ?? raw.processes, limit)
  const desired = integer(raw.desired, limit)
  const draining = integer(raw.draining, limit)
  const depth = raw.depth_available === true ? integer(raw.depth) : null
  const failures = integer(raw.restart_failures)
  const restart = ['closed', 'backoff', 'open', 'probe'].includes(raw.restart_state) ? raw.restart_state : null
  let cost = integer(raw.process_cost_per_worker, limit)
  const reserved = integer(raw.reserved_processes, limit)
  const helpers = integer(raw.renewal_helpers_reserved, limit)
  if (!cost || running === null || draining === null || reserved !== (running + draining) * cost || helpers !== (running + draining) * (cost - 1)) cost = null
  return {
    name: raw.supervisor ?? raw.name, queue: raw.queue, running, desired, draining, depth,
    restart, failures, retryIn: integer(raw.restart_in_seconds), cost, helpers: cost ? helpers : null,
    reserved: cost ? reserved : null,
    configuration: normalizeConfiguration(configs.get(raw.supervisor ?? raw.name), raw.queue),
    readiness: boolean(raw.ready) !== null && running !== null && desired !== null && draining !== null && depth !== null ? raw.ready && (desired === 0 || running > 0) : null,
    capacity: boolean(raw.capacity_satisfied) !== null && running !== null && desired !== null ? raw.capacity_satisfied && running >= desired : null,
    replicas: integer(raw.replicas, 10_000) || null,
    ready: raw.ready === true && running !== null && desired !== null && draining !== null && depth !== null && (desired === 0 || running > 0),
    healthy: raw.healthy === true && restart === 'closed' && failures === 0,
    pids: Array.isArray(raw.pids) ? raw.pids.filter(pid => integer(pid) > 0).slice(0, 512) : [],
  }
}

function normalizeBudget(raw, configuredLimit, pools, reportedDraining) {
  if (!object(raw) || !configuredLimit || pools.length === 0) return null
  const names = ['limit', 'used', 'available', 'active_worker_processes', 'draining_worker_processes', 'renewal_helpers_reserved']
  const values = names.map(name => integer(raw[name], 4096))
  if (values.some(v => v === null)) return null
  const [limit, used, available, active, draining, helpers] = values
  if (limit !== configuredLimit || used > limit || available !== limit - used || used !== active + draining + helpers
    || active !== sumKnown(pools.map(p => p.running)) || draining !== sumKnown(pools.map(p => p.draining))
    || draining !== reportedDraining || helpers !== sumKnown(pools.map(p => p.helpers))) return null
  return { limit, used, available, active, draining, helpers }
}

export function poolFinding(pool, budget) {
  const result = (label, next, severity = '', priority = 0) => ({ label, next, severity, priority })
  if (['open', 'backoff', 'probe'].includes(pool.restart)) return result(
    { open: 'Restart circuit open', backoff: 'Restart backoff', probe: 'Restart probe' }[pool.restart],
    'Check worker exit logs and restart failures before adding capacity.', pool.restart === 'open' ? 'bad' : 'warn', pool.restart === 'open' ? 5 : 4)
  if (pool.running !== null && pool.desired !== null) {
    if (pool.running === 0 && pool.depth > 0) return result('Pending work, no workers', pool.desired === 0
      ? 'The target is zero. Check balancing settings and configured limits.' : 'Workers are requested but none are running. Check startup and worker exit logs.', 'bad', 5)
    if (pool.running < pool.desired) return budget && pool.cost !== null && budget.available < pool.cost
      ? result('No process headroom', 'The shared budget cannot fit another worker. Review draining processes and the process limit.', 'warn', 3)
      : result('Below desired capacity', 'Check the next heartbeat for convergence, then worker startup logs if the gap persists.', 'warn', 3)
  }
  if (pool.running === null || pool.desired === null || pool.depth === null) return result('Incomplete telemetry', 'Check the supervisor status publication and broker connectivity.', 'warn', 2)
  if (!pool.ready || !pool.healthy) return result('Readiness not confirmed', 'Check worker health and the next heartbeat; matching counts alone do not confirm readiness.', 'warn', 2)
  if (pool.draining > 0) return result('Workers draining', 'Allow in-flight jobs to finish. Check duration and timeouts if draining persists.', '', 1)
  return result(pool.desired === 0 ? 'Idle · zero target' : 'At desired capacity', pool.depth > 0
    ? 'Compare backlog over time before deciding whether more capacity is needed.' : 'No capacity issue observed in this heartbeat.')
}

export function supervisorObservation(entry, now = Date.now(), unconfirmed = false) {
  const base = { slot: entry.slot, group: entry.group, legacy: entry.legacy, instance: entry.instance, pools: [], workers: null, desired: null, budget: null,
    queueCount: null, poolCount: null, affectedPools: null, draining: null, missingWorkers: null,
    readiness: null, capacity: null, pid: null, startedAt: null, uptime: null, engineVersion: null, clientVersion: null,
    hostname: null, engine: null, state: null, updatedAt: null, age: null, timeout: null, fresh: false, severity: 'warn', priority: 4 }
  if (!entry.document) return { ...base, label: 'Status unavailable', next: entry.reason }
  const raw = entry.document, config = raw.configuration
  const limit = integer(config.process_limit, 4096) || null
  const timeout = integer(config.heartbeat_timeout, 86_400) || null
  const epoch = integer(raw.updated_at_epoch) || null
  const age = epoch ? Math.floor(now / 1000) - epoch : null
  const configs = poolConfigurations(config.supervisors)
  const pools = raw.pool_status.map(pool => normalizePool(pool, limit ?? 4096, configs))
  const complete = pools.every(Boolean) && new Set(pools.filter(Boolean).map(p => `${p.name}\0${p.queue}`)).size === pools.length
  const validPools = pools.filter(Boolean)
  const budget = complete ? normalizeBudget(raw.process_budget, limit, validPools, integer(raw.draining, 4096)) : null
  const row = { ...base, hostname: text(raw.hostname), instance: raw.instance_id,
    engine: text(raw.engine, 64),
    state: ['starting', 'running', 'paused', 'terminating', 'stopped'].includes(raw.state) ? raw.state : null,
    updatedAt: epoch ? epoch * 1000 : null, age, timeout, budget,
    pid: integer(raw.pid) || null,
    startedAt: integer(raw.started_at_epoch) > 0 && epoch !== null && raw.started_at_epoch <= epoch + 5 ? raw.started_at_epoch * 1000 : null,
    uptime: integer(raw.uptime_seconds), engineVersion: text(raw.engine_version, 64), clientVersion: text(raw.client_version, 64),
    queueCount: complete ? new Set(validPools.map(pool => pool.queue)).size : null,
    poolCount: complete ? pools.length : null,
    draining: complete && sumKnown(validPools.map(pool => pool.draining)) === integer(raw.draining, 4096) ? raw.draining : null,
    workers: complete && pools.length ? sumKnown(validPools.map(p => p.running)) : null,
    desired: complete && pools.length ? sumKnown(validPools.map(p => p.desired)) : null,
    // Surplus in one pool cannot fill another pool's missing allocation.
    missingWorkers: complete && pools.length ? sumKnown(validPools.map(pool => pool.running === null || pool.desired === null ? null : Math.max(0, pool.desired - pool.running))) : null,
    pools: validPools.map(pool => ({ ...pool, ...poolFinding(pool, budget) })).sort((a, b) => b.priority - a.priority || a.queue.localeCompare(b.queue)),
  }
  if (unconfirmed) return { ...row, label: 'Current state unconfirmed', next: 'The latest read failed. These values come from the last successful reading.' }
  if (entry.expired) return { ...row, label: 'Publication expired', next: 'The KV record has expired. Check the supervisor and its remote-status publisher.' }
  if (age === null || age < -5 || timeout === null) return { ...row, label: 'Heartbeat unconfirmed', next: 'Check the heartbeat timestamp, timing configuration and host clocks.' }
  if (age > timeout) return { ...row, label: 'Heartbeat overdue', next: 'Check the master process and remote-status publication. Current worker health is unknown.' }
  row.fresh = true
  if (row.state !== 'running') return { ...row, label: { paused: 'Paused', starting: 'Starting', terminating: 'Terminating', stopped: 'Stopped' }[row.state] || 'State unknown',
    priority: row.state ? 1 : 4, severity: row.state ? '' : 'warn', next: 'This is the last reported state. Manage the supervisor from its own host.' }
  if (!complete || pools.length === 0 || !limit || integer(raw.pid) === null || raw.pid < 1 || raw.paused !== false || raw.stopping !== false) return { ...row, label: 'Incomplete telemetry', next: 'The publication does not provide a complete running supervisor status.' }
  row.affectedPools = row.pools.filter(pool => pool.severity).length
  row.readiness = boolean(raw.ready) === false ? false : raw.ready === true && row.pools.every(pool => pool.readiness !== null) ? row.pools.every(pool => pool.readiness) : null
  row.capacity = boolean(raw.capacity_satisfied) === false ? false : raw.capacity_satisfied === true && row.pools.every(pool => pool.running !== null && pool.desired !== null) ? row.pools.every(pool => pool.running >= pool.desired) : null
  const issue = row.pools.find(pool => pool.severity)
  if (issue) return { ...row, label: issue.label, next: issue.next, severity: issue.severity, priority: issue.priority }
  if (raw.ready !== true || raw.capacity_satisfied !== true) return { ...row, label: 'Readiness not confirmed', next: 'Check the master process and the next heartbeat.' }
  return { ...row, label: 'At desired capacity', next: 'No pool capacity issue observed in this heartbeat.', severity: '', priority: 0 }
}

// A rolling upgrade can leave the legacy and per-instance slots alongside
// each other until TTL expiry. Count that process generation only once.
export function supervisorObservations(entries, now = Date.now(), unconfirmed = false) {
  const unique = new Map()
  const rank = value => [value.expired ? 0 : 1, integer(value.document?.updated_at_epoch) || 0, value.instance ? 1 : 0]
  for (const entry of entries) {
    const key = entry.document ? `${entry.group}\0${entry.document.instance_id}` : entry.slot
    const prior = unique.get(key)
    const candidateRank = rank(entry), previousRank = prior ? rank(prior) : []
    const difference = candidateRank.findIndex((n, i) => n !== previousRank[i])
    if (!prior || (difference >= 0 && candidateRank[difference] > previousRank[difference])) unique.set(key, entry)
  }
  return [...unique.values()].map(entry => supervisorObservation(entry, now, unconfirmed))
    .sort((a, b) => b.priority - a.priority || a.group.localeCompare(b.group) || a.slot.localeCompare(b.slot))
}
