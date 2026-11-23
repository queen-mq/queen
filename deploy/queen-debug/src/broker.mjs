// One HTTP call to one broker, through the route list. No redirects, a deadline
// and a size limit: a sick node must not hang or fill this server.

import { isAllowed } from './routes.mjs'

const MAX_BODY_BYTES = 32 * 1024 * 1024
export const DEFAULT_TIMEOUT_MS = 8_000

export class BrokerError extends Error {
  constructor(kind, message, { status = null, node = null, code = null } = {}) {
    super(message)
    this.name = 'BrokerError'
    this.kind = kind
    this.status = status
    this.node = node
    this.code = code
  }

  /** Another node may answer this one. */
  get retryable() {
    return ['unreachable', 'timeout', 'unavailable', 'broker_error', 'wrong_node'].includes(this.kind)
  }
}

async function readLimited(res) {
  if (!res.body) return ''
  const reader = res.body.getReader()
  const chunks = []
  let total = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    total += value.byteLength
    if (total > MAX_BODY_BYTES) {
      await reader.cancel().catch(() => {})
      throw new BrokerError('too_large', 'the broker answered more than this server reads at once: ask for fewer rows')
    }
    chunks.push(value)
  }
  return Buffer.concat(chunks.map((c) => Buffer.from(c))).toString('utf8')
}

function parse(text) {
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

function errorFor(status, data, nodeName, label) {
  const code = typeof data === 'object' && data ? (data.code ?? data.error ?? null) : null
  const said = typeof data === 'object' && data ? (data.detail ?? data.message ?? data.error ?? '') : ''
  const opts = { status, node: nodeName, code: typeof code === 'string' ? code : null }
  const tail = said ? `: ${String(said).slice(0, 200)}` : ''
  if (status === 401 || status === 403) return new BrokerError('no_access', `${label} was refused${tail}`, opts)
  if (status === 404) return new BrokerError('not_found', `${label} found nothing${tail}`, opts)
  if (status === 421) return new BrokerError('wrong_node', `${label} must be served by another node${tail}`, opts)
  if (status === 503 || status === 504) return new BrokerError('unavailable', `${label} is unavailable${tail}`, opts)
  if (status >= 500) return new BrokerError('broker_error', `${label} failed with ${status}${tail}`, opts)
  return new BrokerError('bad_request', `${label} was rejected with ${status}${tail}`, opts)
}

export function createBrokerCall({ token = null, allowProbe = false, fetchImpl = fetch } = {}) {
  return async function call(node, method, target, { body, timeoutMs = DEFAULT_TIMEOUT_MS, relay = false, accept = [] } = {}) {
    const base = new URL(node.url)
    const url = new URL(target, base)
    const label = `${method} ${url.pathname}`
    // Checked here, on the URL about to be sent, whatever built it.
    if (url.origin !== base.origin || !isAllowed(method, url.pathname, { allowProbe })) {
      throw new BrokerError('refused', `${label} is not on this server's list of routes`, { node: node.name })
    }
    const headers = { accept: 'application/json' }
    if (token) headers.authorization = `Bearer ${token}`
    if (body !== undefined) headers['content-type'] = 'application/json'
    if (relay) headers['x-queen-feed-relay'] = '1'

    let res
    let text
    try {
      res = await fetchImpl(url, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        redirect: 'manual',
        signal: AbortSignal.timeout(timeoutMs),
      })
      text = await readLimited(res)
    } catch (e) {
      if (e instanceof BrokerError) throw e
      if (e?.name === 'TimeoutError' || e?.name === 'AbortError') {
        throw new BrokerError('timeout', `${label} did not answer within ${timeoutMs} ms`, { node: node.name })
      }
      throw new BrokerError('unreachable', `${label} could not reach ${node.name}`, { node: node.name })
    }
    const data = parse(text)
    if (res.status >= 300 && res.status < 400) {
      throw new BrokerError('bad_request', `${label} answered a redirect, which this server does not follow`, { status: res.status, node: node.name })
    }
    if (!res.ok && !accept.includes(res.status)) throw errorFor(res.status, data, node.name, label)
    return { status: res.status, data }
  }
}
