// Connectors (queen-mq/queen#155): every PostgreSQL source and sink of the
// tenant, read-only.
//
// Reads, every 30 s (the app's ticker):
//   GET /api/v1/connectors   the documents (redacted: never a password), the
//                            answering node's status of each, a source's
//                            pointer and lease (server/src/handlers/connectors.rs)
// A broker built without the `pg` feature, or older than the route, answers
// 404; behind queen-proxy the route needs the admin role (it names a
// database and its login), so another role reads 403.
import { computed, ref, shallowRef } from 'vue'
import client from '@/api/client'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { isMissingRoute } from '@/stores/routeSupport'
import { currentEpoch } from '@/stores/identity'
import { rowOf, verdictOf, detailOf } from './connectors/connectorsView'

export function useConnectorsPage() {
  const list = shallowRef(null) // the entries as the broker answered them
  const absent = ref(false)
  const refused = ref(false)
  const error = ref(null)
  const loading = ref(true)
  const readAt = ref(0)

  async function refresh() {
    const epoch = currentEpoch()
    loading.value = true
    try {
      const r = await client.get('/api/v1/connectors', { quiet: true, probe: true })
      if (epoch !== currentEpoch()) return
      list.value = Array.isArray(r.data?.connectors) ? r.data.connectors : []
      absent.value = false
      refused.value = false
      error.value = null
      readAt.value = Date.now()
    } catch (e) {
      if (epoch !== currentEpoch()) return
      const status = e?.status ?? e?.response?.status
      if (isMissingRoute(e) || status === 404 || status === 405) { absent.value = true; list.value = null; error.value = null }
      else if (status === 403) { refused.value = true; list.value = null; error.value = null }
      else error.value = describeApiError(e)
    } finally { loading.value = false }
  }
  useAutoRefresh(refresh)
  refresh()

  const rows = computed(() => (list.value || []).map((c) => rowOf(c)).sort((a, b) => a.name.localeCompare(b.name)))
  const verdict = computed(() => verdictOf(rows.value))
  const detail = (name) => detailOf((list.value || []).find((c) => c.name === name) || null)
  const raw = (name) => (list.value || []).find((c) => c.name === name) || null

  return { rows, verdict, detail, raw, absent, refused, error, loading, readAt, refresh }
}
