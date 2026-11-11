// The reads behind the Overview's three panels (console/overviewPanels.js says
// what each one shows). Nearly all of it is already read for the page:
//   where the traffic is   the queue rows: queuesStore, groupsStore and
//                          queueOpsStore, the stores the frame ticks
//   the cluster underneath the top bar's own read of the cell (useConsoleFrame)
//   streams and connectors the Streams and Connectors pages' own composables,
//                          so their words are the pages' words:
//     GET /streams/v1/queries  the queue streams' registry (+ the feed store)
//     GET /api/v1/connectors   the connectors (404 without them, 403 below admin)
// every 30 s, on the app's ticker.
import { computed } from 'vue'
import { clusterView } from '@/composables/useRaftCluster'
import { useQueuesStore } from '@/stores/queuesStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { queueRows } from './queues/queuesView'
import { clusterNodes, insideBroker, namespaceTraffic } from './overviewPanels'
import { useStreamsPage } from './useStreamsPage'
import { useConnectorsPage } from './useConnectorsPage'

export function useOverviewPanels(frame) {
  const qs = useQueuesStore()
  const gs = useGroupsStore()
  const os = useQueueOpsStore()
  const ss = useSettingsStore()
  const linesFor = (q) => (typeof ss.linesFor === 'function' ? ss.linesFor(q) : undefined)

  const traffic = computed(() => namespaceTraffic(queueRows({
    queues: qs.queues.value, groups: gs.groups.value, ops: os.ops.value, series: os.series.value, linesFor,
  })))

  const nodes = computed(() => {
    const c = frame?.cell?.value
    return c?.status ? clusterNodes(c.status, clusterView(c.payload)) : null
  })

  const streams = useStreamsPage()
  const connectors = useConnectorsPage()
  const inside = computed(() => {
    const rows = streams.rows.value || []
    // Connectors not read yet, not in this broker, or not for this role: no line.
    const conn = connectors.absent.value || connectors.refused.value || !connectors.readAt.value ? null : connectors.rows.value
    const out = insideBroker({
      feedRows: rows.filter((r) => r.kind === 'feed'),
      queueStreams: rows.filter((r) => r.kind === 'queue'),
      connectorRows: conn,
    })
    return {
      ...out,
      streamsVerdict: streams.verdict.value,
      connectorsVerdict: conn && conn.length ? connectors.verdict.value : null,
    }
  })

  return { traffic, nodes, inside }
}
