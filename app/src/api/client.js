import { createApiClient } from './httpClient'
import { ApiError, CODE_ROUTE_BLOCKED } from './errors'
import { actingClusterHeader, redirectToLogin } from '@/stores/identity'
import { reportApiFailure, reportApiSuccess } from '@/stores/ui'

// Same origin under the proxy; VITE_API_BASE_URL only exists for the
// broker-direct debugging mode (see vite.config.js).
const client = createApiClient({
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL || '',
  getActingClusterHeader: actingClusterHeader,
  redirectToLogin,
  reportApiFailure,
  reportApiSuccess,
})

export { ApiError, CODE_ROUTE_BLOCKED }
export default client
