import axios from 'axios'

import { ApiError, CODE_ROUTE_BLOCKED } from './errors'
import { actingClusterHeader, redirectToLogin } from '@/stores/identity'
import { reportApiFailure, reportApiSuccess } from '@/stores/ui'

// Same origin under the proxy; VITE_API_BASE_URL only exists for the
// broker-direct debugging mode (see vite.config.js).
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || ''

const client = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  // The httpOnly `queen_session` cookie authenticates /api/v1/* and
  // /api/console/* directly. No token dance, no Authorization header.
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json'
  }
})

// Paths that answer JSON, never HTML. `/metrics/prometheus` is deliberately
// out: it is the one gateway surface whose success body is text.
const isJsonApiPath = (url = '') => url.startsWith('/api/') || url === '/health'

/** Paths whose cluster attribution is set by the act-as header. */
// `/api/console/*` is here for the SHARED-HOST console (one URL fronting many
// clusters), where the Host names no cluster and the header is the only thing
// that can — proxy/src/acting.rs `resolve_route_console`. Everywhere else the
// console deliberately IGNORES the header (it is read only on a host listed in
// QUEEN_PROXY_SHARED_HOSTS), so sending it changes nothing on a per-cluster
// hostname or broker-direct.
const isClusterScoped = (url = '') =>
  url.startsWith('/api/v1/') || url.startsWith('/api/console/') ||
  url.startsWith('/api/operator/') || url === '/health'

// ---------------------------------------------------------------------------
// Request: one place, and only one, attaches the acting cluster.
// ---------------------------------------------------------------------------
client.interceptors.request.use(
  (config) => {
    if (isClusterScoped(config.url || '')) {
      const act = actingClusterHeader()
      if (act) config.headers[act.name] = act.value
    }
    return config
  },
  (error) => Promise.reject(error)
)

// ---------------------------------------------------------------------------
// Response: preserve status + the proxy's {code, error} envelope, report the
// failure to the global surface, and bounce a 401 to the login page.
// ---------------------------------------------------------------------------

function fail(err) {
  reportApiFailure(err)
  return Promise.reject(err)
}

function retryAfterSeconds(headers) {
  const raw = headers?.['retry-after'] ?? headers?.['Retry-After']
  const n = Number(raw)
  return Number.isFinite(n) ? n : null
}

client.interceptors.response.use(
  (response) => {
    // A 200 that is not JSON on an API path means the request fell through to
    // a static/SPA fallback — i.e. the endpoint does not exist. Left alone,
    // axios resolves and the caller reports success for a call that never
    // happened, which is the phantom-endpoint lie. Reject it instead.
    const url = response.config?.url || ''
    const contentType = String(response.headers?.['content-type'] || '')
    if (isJsonApiPath(url) && contentType && !contentType.includes('json')) {
      return fail(new ApiError('Endpoint does not exist on this broker', {
        status: response.status,
        code: 'not_an_api_response',
        path: url,
      }))
    }
    reportApiSuccess()
    return response
  },
  (error) => {
    // An aborted request is a caller decision (unmount, tenant switch), not a
    // failure: it must not paint a banner.
    if (axios.isCancel?.(error) || error?.code === 'ERR_CANCELED') {
      return Promise.reject(error)
    }

    const res = error.response
    const status = res?.status ?? 0
    const body = res?.data
    const envelope = body && typeof body === 'object' ? body : null
    const path = error.config?.url || null
    const apiErr = new ApiError(
      envelope?.error || envelope?.code || error.message || 'Network error',
      {
        status,
        code: envelope?.code || null,
        retryAfter: retryAfterSeconds(res?.headers),
        body,
        path,
      }
    )

    // Contract: API paths never redirect, they 401. Reacting to that is the
    // SPA's job, and this is the single place it happens.
    if (status === 401) {
      redirectToLogin()
      // Navigating away: never settle, so no view flashes an error state on
      // its way off screen.
      return new Promise(() => {})
    }

    return fail(apiErr)
  }
)

export { ApiError, CODE_ROUTE_BLOCKED }
export default client
