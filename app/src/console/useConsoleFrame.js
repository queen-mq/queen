// What the console's frame shows around every page: who you are, the cluster
// and its cell (for the band's chip), the clusters to switch to, and what ⌘K
// can find. It reads the stores the old shell reads, so both agree.
import { computed } from 'vue'
import { operator } from '@/api'
import { useApi } from '@/composables/useApi'
import { useAutoRefresh } from '@/composables/useRefresh'
import { brokerStatus, cellIssues } from '@/composables/useBrokerStatus'
import { formatRate } from '@/composables/useTraffic'
import { SEV_BAD, SEV_WARN } from '@/composables/useSeverity'
import { useIdentity } from '@/stores/identity'
import { routeSupport } from '@/stores/routeSupport'
import { useQueuesStore } from '@/stores/queuesStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { queueAttention } from '@/composables/useAttention'

export const PATHS = {
  queues: '/queues',
  streams: '/streams',
  dlq: '/dlq',
  workload: '/workload',
  entities: '/entities',
  state: '/state',
  cluster: '/cluster',
  settings: '/settings',
}

const tone = (sev) => (sev === SEV_BAD ? 'red' : sev === SEV_WARN ? 'amber' : '')

const pct = (share) => (share == null ? '–' : share > 0 && share < 0.01 ? '<1%' : `${Math.round(share * 100)}%`)
function size(b) {
  if (b == null) return '–'
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let v = b; let i = 0
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++ }
  return `${v < 10 && i > 0 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${u[i]}`
}

// The chip's few words: the worst cell problem, short.
function shortWords(issue) {
  const t = issue.title || ''
  if (/quorum/i.test(t) || /quorum/i.test(issue.why || '')) return issue.sev === SEV_BAD ? 'no quorum' : 'quorum at risk'
  if (/refuses writes/.test(t)) return 'writes refused'
  if (/disk/i.test(t)) return (issue.why.match(/Disk (\d+%)/) || [])[1] ? `disk ${(issue.why.match(/Disk (\d+%)/) || [])[1]}` : 'disk nearly full'
  if (/memory/i.test(t)) return 'memory short'
  if (/behind/i.test(t)) return 'a node is behind'
  if (/answering/i.test(t)) return 'a node is silent'
  if (/snapshot/i.test(t)) return 'snapshot moving'
  if (/busy/i.test(t)) return 'CPU busy'
  return t
}

export function useConsoleFrame() {
  const id = useIdentity()

  const user = computed(() => {
    const email = id.email.value
    const name = email ? email.replace(/@.*/, '') : (id.standalone.value ? 'broker-direct' : 'you')
    return { name, email, role: id.role.value }
  })

  // Behind the proxy: the acting cluster's slug. Straight to a broker (dev,
  // or auth off) the one cluster is "local", so the cell names it.
  const cluster = computed(() => {
    const c = id.actingCluster.value
    if (id.standalone.value) return { name: id.actingCellSlug.value || c?.slug || 'broker', id: c?.id }
    return { name: c?.slug || c?.name || 'no cluster', id: c?.id }
  })
  const clusters = computed(() => {
    if (id.standalone.value) return []
    const on = id.actingCluster.value?.id
    return (id.clusters.value || []).map((c) => ({ id: c.id, label: c.slug || c.name || c.id, on: c.id === on }))
  })
  function switchCluster(clusterId) { id.setActingCluster(clusterId) }

  // ---- the cell, for operators (it covers every tenant on it) ----
  const members = useApi(
    routeSupport.guard('raft-members', (config) => operator.getRaftMembers({ ...config, probe: true })),
    { immediate: false },
  )
  const refreshCell = () => (id.can('operator') ? members.refresh() : undefined)
  refreshCell()
  useAutoRefresh(refreshCell)

  const status = computed(() => brokerStatus(members.data.value))
  const cell = computed(() => {
    const st = status.value
    // The raw read too, so a page (Cluster) can use it instead of reading again.
    const read = { payload: members.data.value, error: members.error.value }
    if (!st) return { mark: null, metrics: [], summary: id.can('operator') ? 'Reading its nodes…' : 'Its nodes are shown to operators.', nodes: [], issues: [], status: null, ...read }
    const issues = cellIssues(st)
    const worst = issues[0]
    const voters = st.raft.voters || st.nodes.length
    const up = st.raft.up ?? st.nodes.filter((n) => !n.unreachable).length
    const leader = st.raft.leader
    const summary = st.raft.single
      ? 'one node'
      : `${leader ? leader + ' leads, ' : 'no leader, '}${up === voters ? `all ${voters} voting` : `${up} of ${voters} voting`}`
    const nodes = st.nodes.map((n) => ({
      id: n.nodeId ?? n.name,
      name: n.name,
      leader: n.isLeader,
      cpu: n.unreachable ? '–' : pct(n.cpu.share),
      cpuTone: tone(n.cpu.sev),
      memory: n.unreachable ? '–' : (n.mem.held != null && n.mem.limit ? `${size(n.mem.held)} of ${size(n.mem.limit)}` : pct(n.mem.share)),
      memoryTone: tone(n.mem.sev),
      disk: n.unreachable ? '–' : (n.disk.usedPct == null ? '–' : `${Math.round(n.disk.usedPct)}%`),
      diskTone: tone(n.disk.sev),
      behind: n.unreachable ? 'unreachable' : n.isLeader ? '–' : (n.lag == null ? '–' : n.lag.toLocaleString('en-US')),
      behindTone: n.unreachable ? 'red' : tone(n.lagSeverity),
    }))
    // The band's numbers, without opening the panel: CPU, memory and disk of
    // the node that runs out first, and every node's network in and out.
    const metrics = []
    const c = st.cpu.top; const m = st.mem.top; const d = st.disk.top
    if (c) metrics.push({ id: 'cpu', label: 'CPU', value: pct(c.share), tone: tone(st.cpu.sev), title: `CPU: ${c.name} is the busiest node, ${pct(c.share)} of the ${c.cpus} cores it may use` })
    if (m) metrics.push({ id: 'mem', label: 'Mem', value: pct(m.share), tone: tone(st.mem.sev), title: `Memory: ${m.name} holds the most, ${size(m.held)} of ${size(m.limit)}` })
    if (d && d.usedPct != null) metrics.push({ id: 'disk', label: 'Disk', value: `${Math.round(d.usedPct)}%`, tone: tone(st.disk.sev), title: `Disk: ${d.name} is the fullest, ${Math.round(d.usedPct)}% used${d.gate && d.highPct != null ? `; writes stop at ${d.highPct}%` : ''}` })
    const net = st.nodes.reduce((sum, n) => (n.traffic?.network ? (sum ?? 0) + (n.traffic.network.received || 0) + (n.traffic.network.sent || 0) : sum), null)
    if (net != null) metrics.push({ id: 'net', label: 'Net', value: formatRate(net), tone: '', title: 'Network: every node, in and out, a second' })
    else if (st.raft.wire != null) metrics.push({ id: 'net', label: 'Raft', value: formatRate(st.raft.wire), tone: '', title: 'The Raft port, every node, a second (this broker does not measure its network)' })
    // A problem a number already shows in colour needs no words beside it.
    const covered = worst && /:(cpu|memory|disk)(,|$)|^cell:(cpu|memory|disk):/.test(worst.key)
    return {
      mark: worst ? { tone: tone(worst.sev), words: covered ? '' : shortWords(worst) } : null,
      metrics,
      summary,
      nodes,
      issues: issues.map((i) => ({ tone: tone(i.sev), words: i.title, why: i.why, key: i.key, sev: i.sev })),
      status: st,
      ...read,
    }
  })

  // ---- the shared stores: the frame ticks them, as the old sidebar did ----
  const qs = useQueuesStore()
  const gs = useGroupsStore()
  const os = useQueueOpsStore()
  const ss = useSettingsStore()
  const tickStores = () => {
    qs.fetchQueues({ ttlMs: 25_000 }).catch(() => {})
    gs.fetchGroups().catch(() => {})
    os.fetchOps().catch(() => {})
    ss.fetchSettings?.().catch?.(() => {})
  }
  tickStores()
  useAutoRefresh(tickStores)

  // What needs you, for the rail's mark on every page: the Overview's rule.
  const attention = computed(() => {
    if (!qs.queues.value?.length || gs.groups.value == null) return null
    const linesFor = (q) => (typeof ss.linesFor === 'function' ? ss.linesFor(q) : undefined)
    const issues = queueAttention(qs.queues.value, gs.groups.value, linesFor, os.ops.value)
    const cellSevs = (cell.value.issues || []).map((i) => i.sev)
    const sevs = [...issues.map((i) => i.sev), ...cellSevs]
    if (!sevs.length) return null
    return { count: sevs.length, tone: sevs.includes(SEV_BAD) ? 'red' : 'amber' }
  })

  // ---- ⌘K ----
  const PAGES = [
    { label: 'Overview', to: '/' },
    { label: 'Queues', to: '/queues' },
    { label: 'Consumer groups', sub: 'Queues', to: { path: '/queues', query: { view: 'groups' } } },
    { label: 'Messages', sub: 'Queues', to: { path: '/queues', query: { view: 'messages' } } },
    { label: 'Dead letters', to: '/dlq' },
    { label: 'Streams', to: '/streams' },
    { label: 'Workload', to: '/workload' },
    { label: 'Ephemeral queues', sub: 'Queues', to: { path: '/queues', query: { show: 'eph' } } },
    { label: 'Workers', sub: 'supervisor pools, Queues', to: { path: '/queues', query: { view: 'workers' } } },
    { label: 'KV keys', sub: 'State', to: '/state' },
    { label: 'Locks', sub: 'State', to: { path: '/state', query: { view: 'locks' } } },
    { label: 'Timers', sub: 'State', to: { path: '/state', query: { view: 'timers' } } },
    { label: 'Entities', sub: 'traced timelines', to: '/entities' },
    { label: 'Cluster', to: '/cluster' },
    { label: 'State', sub: 'keys, locks, timers', to: '/state' },
    { label: 'Access', sub: 'members and API keys', to: '/settings?view=access' },
    { label: 'Settings', to: '/settings' },
  ]
  const searchItems = computed(() => {
    const out = PAGES.map((p) => ({ kind: 'page', ...p }))
    for (const q of qs.queues.value || []) {
      const name = q.name || q.queue
      if (name) out.push({ kind: 'queue', label: name, sub: q.namespace || '', to: `/queues/${encodeURIComponent(name)}` })
    }
    const seen = new Set()
    for (const g of gs.groups.value || []) {
      const name = g.name || g.consumerGroup
      if (!name || name === '__QUEUE_MODE__' || seen.has(name)) continue
      seen.add(name)
      out.push({ kind: 'group', label: name, sub: g.queueName || g.queue || '', to: { path: '/queues', query: { view: 'groups', group: name } } })
    }
    return out
  })

  return {
    paths: PATHS,
    user,
    canSignOut: computed(() => !id.standalone.value),
    signOut: () => id.logout(),
    cluster,
    clusters,
    switchCluster,
    cell,
    refreshCell,
    attention,
    canOperate: computed(() => id.can('operator')),
    searchItems,
  }
}
