// Streams: each registered stream, what it reads and writes, whether it keeps
// up, and its windows past their end that never closed.
//
// Reads, every 30 s (the app's ticker):
//   GET /streams/v1/queries   the registry and what each stream's state holds
//                             (a broker from 2.7.0; an older one answers 404)
//   stores: consumer-groups (a runner reads its source as `streams.<name>`),
//           workload by queue (the source's and the sink's last hour)
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
import { tone } from './useQueuePage'

const PREFIX = 'streams.'

export function perSecond(minutes, n = 5) {
  const done = (minutes || []).slice(0, -1).filter((v) => v != null)
  const last = done.slice(-n)
  if (!last.length) return null
  return last.reduce((a, b) => a + b, 0) / last.length / 60
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

  async function refresh() {
    const epoch = currentEpoch()
    loading.value = true
    gs.fetchGroups().catch(() => {})
    os.fetchOps().catch(() => {})
    ss.fetchSettings?.()?.catch?.(() => {})
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
    } finally { loading.value = false }
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
    return list.sort((a, b) => a.name.localeCompare(b.name))
  })

  const verdict = computed(() => {
    const rs = rows.value
    if (!rs.length) return null
    const needs = rs.filter((r) => r.needs)
    if (!needs.length) {
      // Without the list the windows are not seen, so say only what is.
      const windows = rs.some((r) => r.state) ? ', and every window closes' : ''
      return { tone: '', text: rs.length === 1 ? `It keeps up${windows}.` : `Every stream keeps up${windows}.` }
    }
    const worst = needs.find((r) => r.needs === 'red') || needs[0]
    const names = needs.slice(0, 2).map((r) => r.name)
    return {
      tone: worst.needs,
      names,
      text: needs.length === 1
        ? (worst.overdue ? `has ${worst.overdue} window${worst.overdue === 1 ? '' : 's'} past their end that never closed.` : 'is behind.')
        : `${needs.length > 2 ? `and ${needs.length - 2} more ` : ''}need you.`,
    }
  })

  return {
    rows, verdict, absent, off, error, loading, refresh,
    overdueAfterMs: computed(() => data.value?.overdue_after_ms ?? 300_000),
  }
}
