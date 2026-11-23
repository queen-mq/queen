// The only broker routes this server can call. Every request passes through
// `isAllowed` at the last moment, on the URL as it will be sent, so a bug in a
// tool cannot turn into a write: a broker port often has no authentication.
//
// Nothing here pops, acks, seeks, retries, deletes or configures. The one
// write, the probe, exists only when the operator turned it on.

const READS = [
  'GET /health',
  'GET /metrics/prometheus',

  'GET /api/v1/raft/status',
  'GET /api/v1/raft/members',

  'GET /api/v1/status',
  'GET /api/v1/status/queues',
  'GET /api/v1/status/queues/:queue',

  'GET /api/v1/resources/overview',
  'GET /api/v1/resources/queues',
  'GET /api/v1/resources/partitions',
  'GET /api/v1/resources/kv/namespaces',
  'POST /api/v1/resources/kv/list',

  'GET /api/v1/consumer-groups',
  'GET /api/v1/consumer-groups/lagging',
  'GET /api/v1/consumer-groups/:group',

  'GET /api/v1/messages',
  'GET /api/v1/messages/:partition/:transaction',
  'GET /api/v1/dlq',
  'POST /api/v1/fetch',
  'POST /api/v1/fetch/offsets',
  'POST /api/v1/partitions/changed',
  'POST /api/v1/feed',

  'GET /api/v1/traces/names',
  'GET /api/v1/traces/by-name/:name',
  'GET /api/v1/traces/:partition/:transaction',

  'GET /api/v1/analytics/dlq-signatures',
  'GET /api/v1/analytics/queue-lag',
  'GET /api/v1/analytics/queue-ops',
  'GET /api/v1/analytics/queue-parked-replicas',
  'GET /api/v1/analytics/retention',
  'GET /api/v1/analytics/system-metrics',
  'GET /api/v1/analytics/worker-metrics',
  'GET /api/v1/analytics/workload',

  'GET /api/v1/kv/:ns/*key',
  'GET /api/v1/timers/:queue',
  'GET /api/v1/timers/:queue/*key',
  'GET /api/v1/ephemeral/queues',
  'GET /api/v1/ephemeral/queues/:queue/depth',
  'GET /api/v1/connectors',
  'GET /streams/v1/queries',
  'POST /streams/v1/state/partitions',
]

export const PROBE_NAMESPACE = 'queen-debug'
export const PROBE_KEY = 'probe'
const PROBE = `PUT /api/v1/kv/${PROBE_NAMESPACE}/${PROBE_KEY}`

function compile(route) {
  const [method, path] = route.split(' ')
  const pattern = path
    .split('/')
    .map((part) => {
      if (part.startsWith(':')) return '[^/]+'
      if (part.startsWith('*')) return '[^/]+(?:/[^/]+)*'
      return part.replace(/[.+?^${}()|[\]\\]/g, '\\$&')
    })
    .join('/')
  return { method, re: new RegExp(`^${pattern}$`) }
}

const compiledReads = READS.map(compile)
const compiledProbe = compile(PROBE)

// A dot segment in any spelling. `URL` collapses `..` and `%2e%2e` before the
// check, so this only catches what reached the path some other way.
const DOT_SEGMENT = /(^|\/)(?:\.|%2e){1,2}(?=\/|$)/i

export function isAllowed(method, pathname, { allowProbe = false } = {}) {
  if (typeof pathname !== 'string' || DOT_SEGMENT.test(pathname)) return false
  const verb = String(method).toUpperCase()
  if (allowProbe && verb === compiledProbe.method && compiledProbe.re.test(pathname)) return true
  return compiledReads.some((r) => r.method === verb && r.re.test(pathname))
}

/** One path segment from a caller's value. Refuses what a URL would reinterpret. */
export function segment(value, label = 'value') {
  const text = String(value ?? '')
  if (text === '' || text === '.' || text === '..') throw new RangeError(`${label} is not a valid name`)
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(text)) throw new RangeError(`${label} holds a control character`)
  return encodeURIComponent(text)
}

/** A key that may hold slashes: each part is one segment, so none can climb. */
export function keyPath(value, label = 'key') {
  return String(value ?? '').split('/').map((part) => segment(part, label)).join('/')
}

/** `path?query`, leaving out what the caller did not set. */
export function withQuery(path, params = {}) {
  const query = new URLSearchParams()
  for (const [name, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') query.set(name, String(value))
  }
  const text = query.toString()
  return text ? `${path}?${text}` : path
}
