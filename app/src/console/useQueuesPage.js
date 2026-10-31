// The Queues page: every queue by namespace, what needs you, the filter and
// segment (in the address: ?find=, ?show=), the order a header click froze,
// and which namespaces are open. No read per row: everything comes from the
// shared stores the frame already ticks, every 30 s (the app's ticker):
//   GET /api/v1/resources/queues                  queuesStore
//   GET /api/v1/consumer-groups                   groupsStore (25 s here: this page shows the verdicts)
//   GET /api/v1/analytics/workload?groupBy=queue  queueOpsStore: the hour by queue, by minute
//   GET /api/v1/ephemeral/queues                  ephemeralStore
//   the alert lines                               settingsStore
import { computed, reactive, ref, shallowRef, watch } from 'vue'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { onClusterChange } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { useEphemeralStore } from '@/stores/ephemeralStore'
import {
  SEGMENTS, SEGMENT_VALUES, hasNews, namespaceGroups, nextSort, queueRows, queuesVerdict,
  segmentCounts, snapshotOrder,
} from './queues/queuesView'

// Kept for the session (module scope): back from a queue, the namespaces are
// open and the order is as you left them, and the queue you opened is lit.
const opened = reactive(new Map()) // ns -> open, the first default or your click
const sort = ref(null)             // { key, dir, at } | null (by name)
const rank = shallowRef(null)      // name -> position, frozen at the click
const visited = ref('')
onClusterChange(() => { opened.clear(); sort.value = null; rank.value = null; visited.value = '' })

export function useQueuesPage() {
  const qs = useQueuesStore()
  const gs = useGroupsStore()
  const os = useQueueOpsStore()
  const ss = useSettingsStore()
  const es = useEphemeralStore()
  const linesFor = (q) => (typeof ss.linesFor === 'function' ? ss.linesFor(q) : undefined)

  const find = ref('')
  const show = ref('all')
  useRouteState({ find, show })
  // An unknown ?show= reads as All, and the switch says so.
  watch(show, (v) => { if (!SEGMENT_VALUES.includes(v)) show.value = 'all' }, { immediate: true })
  const seg = computed(() => (SEGMENT_VALUES.includes(show.value) ? show.value : 'all'))
  const narrowed = computed(() => !!find.value.trim() || seg.value !== 'all')

  function refresh() {
    return Promise.allSettled([
      qs.fetchQueues({ ttlMs: 25_000 }),
      gs.fetchGroups({ ttlMs: 25_000 }),
      os.fetchOps(),
      ss.fetchSettings?.(),
      es.fetchQueues?.(),
    ])
  }
  useAutoRefresh(refresh)
  refresh()

  const loaded = computed(() => qs.lastFetched.value > 0 || qs.queues.value.length > 0)
  const rows = computed(() => queueRows({
    queues: qs.queues.value, groups: gs.groups.value, ops: os.ops.value, series: os.series.value, linesFor,
  }))
  const groups = computed(() => namespaceGroups(rows.value, { find: find.value, seg: seg.value, rank: rank.value }))
  // Verdicts are known once the groups are read and the hour was asked for.
  const judged = computed(() => gs.groups.value != null && os.lastFetched.value > 0)

  // ---- open namespaces ----
  // A namespace opens by default when it holds a queue that needs you or is
  // catching up, decided the first time it is seen with its verdicts known;
  // after that only a click opens or closes it. A refresh never does.
  watch([groups, judged], () => {
    if (!judged.value || narrowed.value) return
    for (const g of groups.value) if (!opened.has(g.ns)) opened.set(g.ns, hasNews(g))
  }, { immediate: true })
  // While a filter or a segment narrows the list, every namespace left is open,
  // and closing one lasts until the filter changes.
  const fopen = reactive(new Map())
  watch([find, seg], () => fopen.clear())
  const isOpen = (g) => (narrowed.value
    ? (fopen.has(g.ns) ? fopen.get(g.ns) : true)
    : (opened.has(g.ns) ? opened.get(g.ns) : false))
  function toggle(g) { (narrowed.value ? fopen : opened).set(g.ns, !isOpen(g)) }

  // ---- order ----
  function sortBy(key) {
    const next = nextSort(sort.value, key)
    sort.value = next ? { ...next, at: Date.now() } : null
    rank.value = next ? snapshotOrder(rows.value, next) : null
  }

  // ---- what the header and the segments say ----
  const ephCount = computed(() => (es.support.value === 'live' ? es.count.value : es.support.value === 'unknown' ? null : 0))
  const counts = computed(() => segmentCounts(rows.value, ephCount.value))
  const segments = computed(() => SEGMENTS.map((s) => ({
    ...s,
    count: !loaded.value || (s.value === 'needs' && !judged.value) || counts.value[s.value] == null
      ? null
      : counts.value[s.value].toLocaleString('en-US'),
  })))
  const facts = computed(() => {
    const list = qs.queues.value || []
    if (!list.length) return []
    const parts = list.reduce((s, q) => s + (Number(q.partitions) || 0), 0)
    const nss = new Set(list.map((q) => q.namespace).filter((n) => typeof n === 'string' && n))
    const out = [`${parts.toLocaleString('en-US')} partition${parts === 1 ? '' : 's'}`]
    const g = gs.groups.value
    if (g) out.push(`${g.length.toLocaleString('en-US')} consumer group${g.length === 1 ? '' : 's'}`)
    out.push(`${nss.size} namespace${nss.size === 1 ? '' : 's'}`)
    return out
  })
  const verdict = computed(() => queuesVerdict({
    rows: rows.value, loaded: loaded.value, groupsKnown: judged.value, ops: os.ops.value, lines: ss.lines?.value,
  }))

  // ---- what could not be read ----
  const listError = computed(() => (qs.error.value && !loaded.value ? describeApiError(qs.error.value) : null))
  const stale = computed(() => (qs.isStale.value ? { error: describeApiError(qs.error.value), at: qs.lastFetched.value } : null))
  const groupsError = computed(() => (gs.groups.value == null && gs.error.value ? describeApiError(gs.error.value) : null))
  const noTraffic = computed(() => (os.ops.value == null && os.error.value ? describeApiError(os.error.value) : null))

  // The New queue form's suggestions: what the cluster already uses, with how many queues.
  const labels = (key) => computed(() => {
    const m = new Map()
    for (const q of qs.queues.value || []) {
      const v = typeof q[key] === 'string' ? q[key] : ''
      if (v) m.set(v, (m.get(v) || 0) + 1)
    }
    return [...m.entries()].map(([value, count]) => ({ value, count })).sort((a, b) => a.value.localeCompare(b.value))
  })
  const namespaces = labels('namespace')
  const tasks = labels('task')

  /** After a create: show the new queue, lit, with its namespace open. */
  function reveal(name, ns) {
    if (narrowed.value) { find.value = ''; show.value = 'all' }
    opened.set(ns || '', true)
    visited.value = name
    qs.invalidate()
    qs.fetchQueues({ force: true })
  }

  return {
    find, show, seg, narrowed, segments, facts, verdict, rows, groups, judged, loaded,
    isOpen, toggle, sort, sortBy, visited, reveal,
    listError, stale, groupsError, noTraffic,
    ephemeral: es, namespaces, tasks, refresh,
  }
}
