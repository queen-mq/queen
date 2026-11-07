// Streams: each registered stream, what it reads and writes, whether it keeps
// up, and its windows past their end that never closed. Two kinds: a queue
// stream (Stream.from a queue: the broker's registry) and a feed stream
// (Stream.fromFeed: what its runner keeps in KV, console/streams/feedStreams.js).
//
// Reads, every 30 s (the app's ticker):
//   GET /streams/v1/queries   the registry and what each stream's state holds
//                             (a broker from 2.7.0; an older one answers 404)
//   stores: consumer-groups (a runner reads its source as `streams.<name>`),
//           workload by queue (the source's and the sink's last hour)
//   feedStore: the feed streams' records, descriptions and locks in KV
//              (stores/feedStore.js says which reads), the one copy every page shares
// Without the list the page still finds the streams from their groups, and
// says what it cannot see.
import { computed, ref, shallowRef } from 'vue'
import client from '@/api/client'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { groupAttention } from '@/composables/useAttention'
import { isMissingRoute } from '@/stores/routeSupport'
import { currentEpoch } from '@/stores/identity'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { useFeedStore } from '@/stores/feedStore'
import { tone } from './useQueuePage'
import { perSecond } from './words'
import { feedNeedsText, matchQueues } from './streams/feedStreams'

const PREFIX = 'streams.'

export { perSecond }

/** The arrivals a second of the queues `patterns` names (null: every queue), summed; null when none is known. */
function ratesOf(patterns, series) {
  if (!series) return null
  let total = null
  const names = patterns == null ? [...series.keys()] : matchQueues(patterns, series.keys())
  for (const q of names) {
    const v = perSecond(series.get(q)?.push)
    if (v != null) total = (total ?? 0) + v
  }
  return total
}

/** One stream as the page draws it, from its registry row (or its group alone). */
export function streamRow({ query = null, group = null, linesFor, series, now = Date.now() }) {
  const name = query ? query.name : String(group?.name || '').slice(PREFIX.length)
  const source = query ? query.source_queue : group?.queueName || ''
  const sink = query ? query.sink_queue || null : null
  const state = query?.state || null
  const sev = group ? groupAttention(group, linesFor) : ''
  const lagTone = sev === 'mute' ? '' : tone(sev)
  const overdue = state?.overdue || 0
  const inS = perSecond(series?.get(source)?.push)
  const outS = sink ? perSecond(series?.get(sink)?.push) : null
  const lastWrite = state?.newest_write ? Date.parse(state.newest_write) : null
  const oldestOverdue = state?.oldest_overdue_end ? Date.parse(state.oldest_overdue_end) : null
  // What needs you: windows that never closed will not close by themselves
  // (amber); a group behind by the alert lines is the group's own rule.
  const needs = overdue > 0 ? 'amber' : lagTone
  return {
    kind: 'queue',
    key: name,
    name,
    source,
    sink,
    group: group || null,
    groupName: group?.name || (query ? `${PREFIX}${name}` : ''),
    queryId: query?.query_id || null,
    hash: query?.config_hash || null,
    created: query?.created_at ? Date.parse(query.created_at) : null,
    updated: query?.updated_at ? Date.parse(query.updated_at) : null,
    lag: group?.maxTimeLag || 0,
    waiting: group?.totalLag || 0,
    partitionsBehind: group?.partitionsWithLag || 0,
    lagTone,
    inS,
    outS,
    state,
    windows: state ? state.windows : null,
    overdue,
    oldestOverdue,
    lastWrite,
    exact: state ? state.exact !== false : true,
    needs,
    derived: !query,
    now,
  }
}

export function useStreamsPage() {
  const gs = useGroupsStore()
  const os = useQueueOpsStore()
  const ss = useSettingsStore()
  const linesFor = (q) => (typeof ss.linesFor === 'function' ? ss.linesFor(q) : undefined)

  const data = shallowRef(null)
  const absent = ref(false)
  const off = ref(false)
  const error = ref(null)
  const loading = ref(true)
  // Feed streams: the shared store, read again at every refresh of this page.
  const fs = useFeedStore()

  async function refresh() {
    const epoch = currentEpoch()
    loading.value = true
    gs.fetchGroups().catch(() => {})
    os.fetchOps().catch(() => {})
    ss.fetchSettings?.()?.catch?.(() => {})
    const feedRead = fs.fetchFeed({ force: true }).catch(() => {})
    try {
      const r = await client.get('/streams/v1/queries', { quiet: true, probe: true })
      if (epoch !== currentEpoch()) return
      data.value = r.data
      absent.value = false
      error.value = null
    } catch (e) {
      if (epoch !== currentEpoch()) return
      const status = e?.status ?? e?.response?.status
      if (isMissingRoute(e) || status === 404 || status === 405) { absent.value = true; data.value = null; error.value = null; off.value = false }
      // queen-proxy refuses the streams family to a tenant without streams.
      else if (status === 403) { off.value = true; data.value = null; error.value = null }
      else error.value = describeApiError(e)
    } finally {
      await feedRead
      loading.value = false
    }
  }

  useAutoRefresh(refresh)
  refresh()

  const rows = computed(() => {
    const groups = gs.groups.value || []
    const series = os.series.value
    const byName = new Map(groups.filter((g) => String(g.name).startsWith(PREFIX)).map((g) => [g.name, g]))
    const queries = data.value?.queries
    const list = queries
      ? queries.map((q) => streamRow({ query: q, group: byName.get(`${PREFIX}${q.name}`) || byName.get(`${PREFIX}${q.query_id}`) || null, linesFor, series }))
      : [...byName.values()].map((g) => streamRow({ group: g, linesFor, series }))
    for (const f of fs.rows.value || []) {
      list.push({
        ...f,
        // Unknown without a description; `queues` null there means every queue.
        inS: f.reads ? ratesOf(f.reads.queues, series) : null,
        outS: f.sink?.kind === 'queues' ? ratesOf(f.sink.queues, series) : null,
        exact: true,
      })
    }
    return list.sort((a, b) => a.name.localeCompare(b.name) || (a.kind < b.kind ? -1 : 1))
  })

  const verdict = computed(() => {
    const rs = rows.value
    if (!rs.length) return null
    const needs = rs.filter((r) => r.needs)
    if (!needs.length) {
      // Without the list the windows are not seen, so say only what is.
      const windows = rs.some((r) => r.kind === 'queue' && r.state) ? ', and every window closes' : ''
      return { tone: '', text: rs.length === 1 ? `It keeps up${windows}.` : `Every stream keeps up${windows}.` }
    }
    const worst = needs.find((r) => r.needs === 'red') || needs[0]
    const names = needs.slice(0, 2).map((r) => r.name)
    let one
    if (worst.kind === 'feed') one = feedNeedsText(worst)
    else one = worst.overdue ? `has ${worst.overdue} window${worst.overdue === 1 ? '' : 's'} past their end that never closed.` : 'is behind.'
    return {
      tone: worst.needs,
      names,
      text: needs.length === 1 ? one : `${needs.length > 2 ? `and ${needs.length - 2} more ` : ''}need you.`,
    }
  })

  return {
    rows, verdict, absent, off, error, loading, refresh,
    feedError: fs.error,
    feedExact: computed(() => fs.exact.value !== false),
    overdueAfterMs: computed(() => data.value?.overdue_after_ms ?? 300_000),
  }
}
