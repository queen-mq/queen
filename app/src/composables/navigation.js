// Route contracts shared by links and their destination pages.
export const textParam = value => typeof value === 'string' ? value : ''

export function safeReturnTo(value) {
  const path = textParam(value)
  // Only dashboard destinations; never a protocol-relative or external URL.
  return /^\/(?:\?(?:.*)|$|(?:queues(?:\/[^?]*)?|messages|dlq|timers|consumers|supervisors|workload|traces|kv|locks|ephemeral|settings|members|keys|users|system)(?:\?.*)?)$/.test(path)
    && !/[\\\r\n]/.test(path) ? path : ''
}

// The queue a page is about: in the path on the queue's own pages
// (/queues/:queueName, /queues/:queueName/alerts), in ?queue= on its tabs.
export function queueOf(route) {
  return textParam(route?.params?.queueName) || textParam(route?.query?.queue)
}

export function contextQuery(route) {
  const query = {}
  for (const key of ['range', 'from', 'to']) {
    if (textParam(route.query?.[key])) query[key] = route.query[key]
  }
  const origin = safeReturnTo(route.query?.returnTo) || (route.name === 'QueueDetail' ? '/queues' : safeReturnTo(route.fullPath))
  if (origin) query.returnTo = origin
  return query
}

export function queueLocation(queue, route, view = 'overview', extra = {}) {
  const paths = { messages: '/messages', failed: '/dlq', scheduled: '/timers', consumers: '/consumers', supervisors: '/supervisors' }
  const own = `/queues/${encodeURIComponent(queue)}`
  return {
    path: paths[view] || (view === 'alerts' ? `${own}/alerts` : own),
    query: { ...contextQuery(route), ...(paths[view] ? { queue } : {}), ...extra },
  }
}

// The Queues section: everything about one queue, as tabs beside the queue
// list. `all` is where a tab lives with no queue picked; a tab without one
// needs a queue and is hidden until one is picked.
export const QUEUE_TABS = [
  { view: 'overview', label: 'Overview', names: ['Queues', 'QueueDetail'], all: '/queues' },
  { view: 'messages', label: 'Messages', names: ['Messages'], all: '/messages' },
  { view: 'consumers', label: 'Consumer groups', names: ['Consumers'], all: '/consumers' },
  { view: 'supervisors', label: 'Supervisors', names: ['Supervisors'] },
  { view: 'failed', label: 'Dead letter', names: ['DeadLetter'], all: '/dlq' },
  { view: 'scheduled', label: 'Timers', names: ['Timers'] },
  { view: 'alerts', label: 'Alerts', names: ['QueueAlerts'] },
]

// The tab this page is in the Queues section, or '' when it is not part of
// it. Consumer groups and Supervisors are tabs only while a queue is picked;
// without one they are pages of their own.
export function queueTabOf(route) {
  const tab = route?.meta?.queueTab
  if (!tab) return ''
  return route.meta.queueTabNeedsQueue && !queueOf(route) ? '' : tab
}

// Where a tab goes: for the picked queue, or across every queue. A tab that
// needs a queue falls back to the queue list's own page.
// Only the investigation window crosses tabs: no return path, since the
// section's own list and breadcrumb are the way back.
export function queueTabLocation(view, queue, route, extra = {}) {
  const query = {}
  for (const key of ['range', 'from', 'to']) {
    if (textParam(route?.query?.[key])) query[key] = route.query[key]
  }
  if (queue) {
    const at = queueLocation(queue, { query: {} }, view, extra)
    return { path: at.path, query: { ...query, ...at.query } }
  }
  const tab = QUEUE_TABS.find((t) => t.view === view)
  return { path: tab?.all || '/queues', query: { ...query, ...extra } }
}

// One rule for the queue list and the Queues table: a term matches a queue's
// name, its namespace or its task.
export function queueMatches(queue, term) {
  const t = textParam(term).trim().toLowerCase()
  if (!t) return true
  return [queue?.name, queue?.namespace, queue?.task].some((v) => typeof v === 'string' && v.toLowerCase().includes(t))
}

/** A queue in the namespace picked in the list; null (or undefined) means every namespace. */
export function inNamespace(queue, namespace) {
  if (namespace === null || namespace === undefined) return true
  return (typeof queue?.namespace === 'string' ? queue.namespace : '') === namespace
}

const SEV_RANK = { bad: 2, warn: 1 }

/**
 * The namespaces of a queue listing, by name, each with how many queues it
 * holds and the worst mark among them: `[{ name, count, sev }]`. `sevOf(queue)`
 * gives a queue's mark ('bad' | 'warn' | null); without it there are none.
 * '' is the default namespace, a real one.
 */
export function namespaceCounts(queues = [], sevOf = null) {
  const byName = new Map()
  for (const q of queues || []) {
    if (!q?.name) continue
    const ns = typeof q.namespace === 'string' ? q.namespace : ''
    const row = byName.get(ns) || { name: ns, count: 0, sev: null }
    row.count += 1
    const sev = typeof sevOf === 'function' ? sevOf(q) : null
    if ((SEV_RANK[sev] || 0) > (SEV_RANK[row.sev] || 0)) row.sev = sev
    byName.set(ns, row)
  }
  return [...byName.values()].sort((a, b) => a.name.localeCompare(b.name))
}

// The queue list's own filters, kept in the address: the search (?find=) and
// the namespace (?namespace=; present and empty is the default namespace).
// They ride along on every pick and every tab, so the list stays as it was
// filtered.
export function listCarry(route) {
  const out = {}
  const find = textParam(route?.query?.find)
  if (find) out.find = find
  if (typeof route?.query?.namespace === 'string') out.namespace = route.query.namespace
  return out
}

export const consumerLocation = (group, route) => queueLocation(group.queueName || '', route, 'consumers', { group: group.name })

export function readRouteValue(raw, fallback) {
  if (Array.isArray(fallback)) return (Array.isArray(raw) ? raw : typeof raw === 'string' ? [raw] : fallback).filter(value => typeof value === 'string')
  if (typeof raw !== 'string') return fallback
  if (typeof fallback === 'number') {
    const number = Number(raw)
    return Number.isSafeInteger(number) && number >= (fallback > 0 ? 1 : 0) ? number : fallback
  }
  if (typeof fallback === 'boolean') return raw === 'true'
  return raw
}

export function validWindow(from, to) {
  const start = Date.parse(textParam(from)), end = Date.parse(textParam(to))
  return Number.isFinite(start) && Number.isFinite(end) && start < end
    ? { from: new Date(start), to: new Date(end) } : null
}

export function windowQuery(window) {
  return { from: window.from.toISOString(), to: window.to.toISOString() }
}
