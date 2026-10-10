// Route contracts shared by links and their destination pages.
export const textParam = value => typeof value === 'string' ? value : ''

export function safeReturnTo(value) {
  const path = textParam(value)
  // Only dashboard destinations; never a protocol-relative or external URL.
  return /^\/(?:\?(?:.*)|$|(?:queues(?:\/[^?]*)?|messages|dlq|timers|consumers|supervisors|analytics|operations|workload|traces|kv|locks|ephemeral|members|keys|users|system)(?:\?.*)?)$/.test(path)
    && !/[\\\r\n]/.test(path) ? path : ''
}

export function queueOf(route) {
  return route.name === 'QueueDetail' ? textParam(route.params.queueName) : textParam(route.query.queue)
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
  const paths = { messages: '/messages', failed: '/dlq', scheduled: '/timers', consumers: '/consumers', metrics: '/analytics', supervisors: '/supervisors' }
  return {
    path: paths[view] || `/queues/${encodeURIComponent(queue)}`,
    query: { ...contextQuery(route), ...(paths[view] ? { queue } : {}), ...extra },
  }
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
