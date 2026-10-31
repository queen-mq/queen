// The Cluster page: the cell under every tenant, for operators. Who votes and
// who leads, each node now, each node over the window, and what an operator
// can do to the cell today.
//
// Reads (every source is cell-level):
//   GET /api/v1/raft/members              the frame's read (the band's chip draws it too)
//   GET /api/v1/analytics/system-metrics  the window, a point a minute per node
//   GET /api/v1/system/link               the standby link
// on the app's 30 s ticker; the metrics again only when a minute has passed
// or the window moved.
import { computed, onUnmounted, ref, shallowRef, watch } from 'vue'
import client from '@/api/client'
import { operator } from '@/api'
import { useApi } from '@/composables/useApi'
import { useAutoRefresh } from '@/composables/useRefresh'
import { brokerStatus, cellIssues } from '@/composables/useBrokerStatus'
import { clusterView } from '@/composables/useRaftCluster'
import { currentEpoch, onClusterChange, useIdentity } from '@/stores/identity'
import { isMissingRoute, routeSupport } from '@/stores/routeSupport'
import { stamp } from './useWindow'
import {
  chartsOf, linkModel, nodeDetail, nodeRows, sharedGate, verdictOf, versionsOf,
} from './cluster/clusterViews'

const getLink = routeSupport.guard('system-link', (config) => client.get('/api/v1/system/link', config))

export function useClusterPage(frame, win) {
  const id = useIdentity()
  const canOperate = computed(() => id.can('operator'))

  // ---- the members: the frame's read once it hands its payload over ----
  // (useConsoleFrame cell.payload); until then this page reads them itself.
  const cellOf = () => frame?.cell?.value || {}
  const shared = 'payload' in cellOf()
  const own = shared
    ? null
    : useApi(routeSupport.guard('raft-members', (config) => operator.getRaftMembers({ ...config, probe: true })), { immediate: false })
  const payload = computed(() => (shared ? cellOf().payload : own.data.value) || null)
  const membersError = computed(() => (shared ? cellOf().error : own.error.value) || null)
  const status = computed(() => (shared ? cellOf().status : brokerStatus(payload.value)) || null)
  const view = computed(() => clusterView(payload.value))
  const issues = computed(() => cellIssues(status.value))
  const absent = computed(() => !payload.value && isMissingRoute(membersError.value))

  // ---- the window: every node's samples ----
  const metrics = shallowRef(null)
  const metricsError = ref(null)
  let metricsKey = ''
  let metricsAt = 0
  let metricsSeq = 0
  const winKey = () => (win.value.custom ? `${win.value.from}-${win.value.to}` : win.value.id)
  async function readMetrics(force = false) {
    const w = win.value
    const key = winKey()
    if (!force && metrics.value && key === metricsKey && (w.custom || Date.now() - metricsAt < 55_000)) return
    const seq = ++metricsSeq
    const epoch = currentEpoch()
    try {
      const r = await operator.getSystemMetrics({ from: w.iso.from, to: w.iso.to }, { quiet: true })
      if (seq !== metricsSeq || epoch !== currentEpoch()) return
      metrics.value = r.data
      metricsKey = key
      metricsAt = Date.now()
      metricsError.value = null
    } catch (e) {
      if (seq !== metricsSeq || epoch !== currentEpoch()) return
      metricsError.value = e
    }
  }

  // ---- the standby link ----
  const link = shallowRef(null)
  const linkError = ref(null)
  async function readOps() {
    const epoch = currentEpoch()
    const [l] = await Promise.allSettled([getLink({ quiet: true, probe: true })])
    if (epoch !== currentEpoch()) return
    if (l.status === 'fulfilled') { link.value = l.value.data; linkError.value = null } else { linkError.value = l.reason; if (isMissingRoute(l.reason)) link.value = null }
  }

  function refresh() {
    if (!canOperate.value) return
    own?.refresh()
    readMetrics()
    readOps()
  }
  /** After an action: everything again, now. */
  function reread() {
    if (!canOperate.value) return
    if (own) own.refresh()
    else frame?.refreshCell?.()
    readMetrics(true)
    readOps()
  }
  useAutoRefresh(refresh)
  refresh()
  watch(winKey, () => readMetrics(true))
  watch(canOperate, (live) => { if (live) refresh() })
  const off = onClusterChange(() => {
    metrics.value = null; metricsError.value = null; metricsKey = ''; metricsAt = 0
    link.value = null; linkError.value = null
  })
  onUnmounted(off)

  // ---- what the page draws ----
  const rows = computed(() => nodeRows(status.value, view.value))
  const versions = computed(() => versionsOf(view.value))
  const gate = computed(() => sharedGate(status.value))
  const columns = computed(() => ({
    version: versions.value.mixed,
    network: rows.value.some((r) => r.network),
    raft: rows.value.some((r) => r.raft),
  }))
  const verdict = computed(() => verdictOf({
    status: status.value, view: view.value, issues: issues.value, error: membersError.value, absent: absent.value,
  }))
  const charts = computed(() => chartsOf(metrics.value, status.value, { from: win.value.from, to: win.value.to, view: view.value }))
  const selfApplied = computed(() => view.value?.rows.find((r) => r.isSelf)?.applied ?? null)
  const linkRow = computed(() => linkModel({ data: link.value, error: linkError.value }, { selfApplied: selfApplied.value }))
  const detail = (name) => nodeDetail(status.value, view.value, name, { now: Date.now(), when: (t) => stamp(t, true) })

  return {
    canOperate, shared, payload, status, view, issues, absent, membersError,
    rows, versions, gate, columns, verdict, detail,
    metrics, metricsError, charts,
    link, linkRow,
    refresh, reread,
  }
}
