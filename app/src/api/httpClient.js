import ky, { isHTTPError } from 'ky'

import { ApiError } from './errors.js'

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

function retryAfterSeconds(headers) {
  const raw = headers?.get?.('retry-after')
  if (raw === null || raw === undefined || raw === '') return null
  const n = Number(raw)
  return Number.isFinite(n) ? n : null
}

function resolveUrl(path, apiBaseUrl) {
  if (!apiBaseUrl) return path
  return `${apiBaseUrl.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`
}

async function responseData(response, responseType) {
  const body = await response.text()
  if (responseType === 'text' || body === '') return body

  // Keep a non-JSON success body as text. API routes are rejected earlier when
  // their declared content type proves that the request fell through to the SPA.
  try {
    return JSON.parse(body)
  } catch {
    return body
  }
}

function errorData(error) {
  if (!isHTTPError(error) || typeof error.data !== 'string') {
    return isHTTPError(error) ? error.data : null
  }
  try {
    return JSON.parse(error.data)
  } catch {
    return error.data
  }
}

/**
 * Build the browser client around injectable side effects. The injection keeps
 * the transport contract testable without a running broker or a browser.
 */
export function createApiClient({
  apiBaseUrl = '',
  getActingClusterHeader = () => null,
  redirectToLogin = () => {},
  reportApiFailure = () => {},
  reportApiSuccess = () => {},
  fetch: fetchImplementation,
} = {}) {
  // Keep the request semantics from the previous transport. In particular,
  // Ky retries GET, PUT, and DELETE by default; console actions must happen
  // once unless a caller explicitly starts a new request.
  const transport = ky.create({
    timeout: 30000,
    retry: 0,
    // The httpOnly `queen_session` cookie authenticates /api/v1/* and
    // /api/console/* directly. `include` also covers broker-direct debugging
    // when VITE_API_BASE_URL points at a different origin.
    credentials: 'include',
    ...(fetchImplementation ? { fetch: fetchImplementation } : {}),
    hooks: {
      beforeRequest: [
        ({ request }) => {
          const path = new URL(request.url).pathname
          if (!isClusterScoped(path)) return
          const act = getActingClusterHeader()
          if (act) request.headers.set(act.name, act.value)
        },
      ],
    },
  })

  function fail(err) {
    reportApiFailure(err)
    return Promise.reject(err)
  }

  /**
   * Preserve the existing `{data, status, headers, config}` response shape, so
   * swapping transports cannot silently change every view and store at once.
   */
  async function request(method, path, config = {}, json) {
    const {
      params,
      responseType,
      data: configBody,
      ...requestOptions
    } = config || {}

    const options = {
      ...requestOptions,
      method,
      retry: 0,
      credentials: 'include',
    }
    if (params !== undefined) options.searchParams = params

    const body = json === undefined ? configBody : json
    if (body !== undefined) options.json = body

    try {
      const response = await transport(resolveUrl(path, apiBaseUrl), options)
      const contentType = String(response.headers.get('content-type') || '')

      // A 200 that is not JSON on an API path means the request fell through
      // to a static/SPA fallback — i.e. the endpoint does not exist.
      if (isJsonApiPath(path) && contentType && !contentType.includes('json')) {
        return fail(new ApiError('Endpoint does not exist on this broker', {
          status: response.status,
          code: 'not_an_api_response',
          path,
        }))
      }

      const data = await responseData(response, responseType)
      reportApiSuccess()
      return {
        data,
        status: response.status,
        headers: response.headers,
        config: { url: path },
      }
    } catch (error) {
      // An aborted request is a caller decision (unmount, tenant switch), not
      // a failure: it must not paint a banner.
      if (config?.signal?.aborted || error?.name === 'AbortError') throw error

      // Response validation already produced the public error contract; only
      // report it once.
      if (error instanceof ApiError) return fail(error)

      const response = isHTTPError(error) ? error.response : null
      const status = response?.status ?? 0
      const body = errorData(error)
      const envelope = body && typeof body === 'object' ? body : null
      const apiErr = new ApiError(
        envelope?.error || envelope?.code || error?.message || 'Network error',
        {
          status,
          code: envelope?.code || null,
          retryAfter: retryAfterSeconds(response?.headers),
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
  }

  return {
    get: (path, config) => request('get', path, config),
    delete: (path, config) => request('delete', path, config),
    post: (path, data, config) => request('post', path, config, data),
    put: (path, data, config) => request('put', path, config, data),
    patch: (path, data, config) => request('patch', path, config, data),
  }
}
