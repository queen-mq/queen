<template>
  <div class="view-container">

    <!-- CELL-LEVEL PAGE. Every source below is an operator route the proxy
         answers 200 for only when /auth/me says operator_live; the numbers
         cover the whole cell, every tenant on it. Say that on screen — a cell
         figure read as the acting tenant's is the exact lie this page could
         tell. -->
    <div v-if="!canOperate" class="card">
      <div class="card-body">
        <div class="panel-err">
          This page is cell-level and only a live operator may open it. Nothing
          here is scoped to <strong>{{ actingTenantSlug || 'your tenant' }}</strong>.
        </div>
      </div>
    </div>

    <template v-else>
      <!-- SCOPE STRIP. Cell variant: same container as every other view's
           strip, drawn in the scope hue because the SCOPE differs — not
           because anything is wrong. (It was amber until the colour policy:
           this page opened with a warning chip on a healthy cell.) Built from
           useIdentity(), so it renders while loading, on a failed fetch and on
           an empty page. -->
      <div class="scope-strip scope-strip-cell">
        <span class="chip chip-scope"><span class="dot"></span>cell · operator</span>
        <span class="scope-text">
          host resources and the replicated log for
          <strong>cell {{ actingCellSlug || 'unknown' }}</strong>
          <span class="scope-sep">·</span>
          shared by every tenant on it, not scoped to {{ actingTenantSlug || 'your tenant' }}
        </span>
      </div>

      <!-- PAGE BANNERS. The Source switch decides which fetch the whole page is
           made of, so either failure is a fact about the page. A single panel's
           failure stays inside that panel as .panel-err. -->
      <div v-if="dataSource === 'system' && metrics.failed.value" class="status-banner banner-bad view-banner">
        <span :title="formatTimestampUtc(metrics.lastUpdated.value)"><strong>Could not load server metrics</strong> · {{ describeApiError(metrics.error.value) }}<template v-if="systemData"> · showing the last samples that loaded{{ metrics.lastUpdated.value ? ` (as of ${formatTimestamp(metrics.lastUpdated.value)})` : '' }}</template></span>
      </div>
      <!-- A route this broker does not serve is not a failure: RaftCluster
           says so quietly, in place of the view. -->
      <div v-if="dataSource === 'storage' && raftMembers.failed.value && !raftMembersAbsent" class="status-banner banner-bad view-banner">
        <span :title="formatTimestampUtc(raftMembers.lastUpdated.value)"><strong>Could not load the Raft cluster</strong> · {{ describeRaftFailure(raftMembers.error.value) }}<template v-if="raftMembers.data.value"> · showing the last members that loaded{{ raftMembers.lastUpdated.value ? ` (as of ${formatTimestamp(raftMembers.lastUpdated.value)})` : '' }}</template></span>
      </div>

      <!-- =================== FILTERS =================== -->
      <div class="card filters">
        <div class="card-body filter-rows">

          <div class="filter-row">
            <div v-if="dataSource === 'system'" class="filter-field">
              <span class="label-xs">Range</span>
              <div class="seg">
                <button
                  v-for="range in timeRanges"
                  :key="range.value"
                  :class="{ on: timeRange === range.value && !customMode }"
                  @click="selectQuickRange(range.value)"
                >{{ range.label }}</button>
                <button :class="{ on: customMode }" @click="toggleCustomMode">Custom</button>
              </div>
            </div>

            <div class="filter-field">
              <span class="label-xs">Source</span>
              <div class="seg">
                <button :class="{ on: dataSource === 'system' }" @click="selectSource('system')">Server resources</button>
                <!-- The storage the cell runs on: the Raft cluster. -->
                <button :class="{ on: dataSource === 'storage' }" @click="selectSource('storage')">Raft</button>
              </div>
            </div>
          </div>

          <div v-if="dataSource === 'system'" class="filter-row">
            <div class="filter-field">
              <span class="label-xs">View</span>
              <div class="seg">
                <button :class="{ on: viewMode === 'individual' }" @click="viewMode = 'individual'">Per server</button>
                <button :class="{ on: viewMode === 'aggregate' }" @click="viewMode = 'aggregate'">Aggregate</button>
              </div>
            </div>
            <div class="filter-field">
              <span class="label-xs">Metric</span>
              <div class="seg">
                <button
                  v-for="agg in aggregationTypes"
                  :key="agg.value"
                  :class="{ on: aggregationType === agg.value }"
                  @click="aggregationType = agg.value"
                >{{ agg.label }}</button>
              </div>
            </div>
            <span class="filter-hint">
              {{ viewMode === 'aggregate'
                ? `summed across ${replicaCountLabel}; a bucket where a replica sent no sample sums only those that did`
                : 'one line per broker replica; gaps are buckets that replica never reported' }}
            </span>
          </div>

          <div v-if="dataSource === 'system' && customMode" class="filter-row filter-row-sep">
            <div class="filter-field">
              <span class="label-xs">From</span>
              <input v-model="customFrom" type="datetime-local" class="input" :title="formatTimestampUtc(customFrom)" />
            </div>
            <div class="filter-field">
              <span class="label-xs">To</span>
              <input v-model="customTo" type="datetime-local" class="input" :title="formatTimestampUtc(customTo)" />
            </div>
            <button class="btn btn-primary" :disabled="!customRangeValid" @click="applyCustomRange">Apply</button>
            <span v-if="customError" class="filter-invalid">{{ customError }}</span>
          </div>
        </div>
      </div>

      <!-- =================== REPLICATED LOG ===================
           The page's summary block, shown under either source: what the node
           that answered holds is its share of the log. -->
      <div class="card" :class="{ 'card-alarm': raftAlarm }" style="margin-bottom:16px;">
        <div class="card-header">
          <h3>Replicated log</h3>
          <span v-if="node && node.nodeId !== null" class="card-sub">
            node {{ node.nodeId }}{{ node.hostname ? ` · ${node.hostname}` : '' }}
          </span>
          <span class="chip chip-mute">cell-level</span>
          <span class="muted">{{ stamp(raftStatus) }}</span>
        </div>
        <div class="card-body">
          <div v-if="raftStatusAbsent" class="panel-na">
            This broker does not report its replicated log — <code>GET /api/v1/raft/status</code> is not served here.
          </div>
          <template v-else>
            <div v-if="raftStatus.failed.value" class="panel-err">
              Log state unavailable — {{ describeRaftFailure(raftStatus.error.value) }}.
              The figures below are unknown, not zero.
            </div>
            <div class="stat-grid stat-grid-6">
              <div class="stat">
                <div class="stat-label">Role</div>
                <div class="stat-value">
                  <span v-if="!node?.state" class="font-mono">—</span>
                  <span v-else class="chip" :class="node.chip.cls"><span class="dot"></span>{{ node.chip.label }}</span>
                </div>
                <div class="stat-foot">{{ node?.clusterNote || '—' }}</div>
              </div>
              <div class="stat">
                <div class="stat-label">Term</div>
                <div class="stat-value font-mono">{{ metric(node?.term) }}</div>
                <div class="stat-foot">
                  <span v-if="node" :class="{ 'num warn': node.leaderSeverity === 'warn' }">{{ node.leaderNote }}</span>
                  <span v-else>—</span>
                </div>
              </div>
              <!-- Three indexes, one tile: applied is this node's own, and the
                   two under it are what it trails — each on its own line, so
                   a nine-digit index never wraps mid-sentence. -->
              <div class="stat">
                <div class="stat-label">Applied</div>
                <div class="stat-value font-mono">{{ formatIndex(node?.applied) }}</div>
                <div class="stat-foot">committed <span class="font-mono tabular-nums">{{ formatIndex(node?.committed) }}</span></div>
                <div class="stat-foot">durable <span class="font-mono tabular-nums">{{ formatIndex(node?.durable) }}</span></div>
              </div>
              <div class="stat">
                <div class="stat-label">Inflight</div>
                <div class="stat-value font-mono">{{ metric(node?.inflight) }}</div>
                <div class="stat-foot">appended, not yet applied</div>
              </div>
              <div class="stat">
                <div class="stat-label">Log</div>
                <div class="stat-value font-mono">{{ bytes(node?.logBytes) }}</div>
                <div class="stat-foot">{{ metric(node?.logFiles) }} files</div>
              </div>
              <div class="stat">
                <div class="stat-label">Store map</div>
                <div class="stat-value font-mono num" :class="numTone(node?.mapSeverity)">{{ formatMapPct(node?.mapPct) }}</div>
                <div class="stat-foot">{{ bytes(node?.mapUsed) }} of {{ bytes(node?.mapBytes) }}</div>
              </div>
            </div>
          </template>
        </div>
      </div>

      <!-- =================== SERVER RESOURCES =================== -->
      <template v-if="dataSource === 'system'">
        <div v-if="metricsFirstLoad" class="sys-grid-2">
          <div v-for="i in 4" :key="i" class="card">
            <div class="card-body"><div class="skeleton" style="height:192px;" /></div>
          </div>
        </div>

        <template v-else>
          <template v-if="systemData">
            <div class="sys-grid-2" style="margin-bottom:16px;">
              <div class="card">
                <div class="card-header">
                  <h3>CPU usage</h3>
                  <span class="card-sub">{{ replicaCountLabel }}</span>
                  <span class="chip chip-mute">cell-level</span>
                  <span class="muted">{{ stamp(metrics) }}</span>
                </div>
                <div class="card-body">
                  <BaseChart
                    v-if="cpuChart.labels.length"
                    type="line" :data="cpuChart" :options="cpuOptions" height="240px"
                  />
                  <div v-else class="panel-msg">No CPU samples in this range.</div>
                </div>
              </div>

              <div class="card">
                <div class="card-header">
                  <h3>Memory usage</h3>
                  <span class="chip chip-mute">cell-level</span>
                  <span class="muted">{{ stamp(metrics) }}</span>
                </div>
                <div class="card-body">
                  <BaseChart
                    v-if="memoryChart.labels.length"
                    type="line" :data="memoryChart" :options="memoryOptions" height="240px"
                  />
                  <div v-else class="panel-msg">No memory samples in this range.</div>
                </div>
              </div>
            </div>

            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Broker workers</h3>
                <span class="chip chip-mute">cell-level</span>
                <span class="muted">{{ stamp(status) }}</span>
              </div>
              <div class="card-body">
                <div v-if="status.failed.value" class="panel-err">
                  {{ describeApiError(status.error.value) }}
                </div>
                <div v-else-if="!workers.length" class="panel-msg">
                  No worker reported in the last two minutes.
                </div>
                <div v-else class="sys-workers">
                  <div v-for="w in workers" :key="`${w.hostname}:${w.workerId}`" class="sys-worker">
                    <span class="sys-worker-host font-mono">{{ w.hostname }}</span>
                    <span class="chip" :class="workerChip(w).cls">
                      <span class="dot"></span>{{ workerChip(w).label }}
                    </span>
                    <span class="sys-worker-meta font-mono">
                      loop {{ msOrDash(w.avgEventLoopLagMs) }} avg · {{ msOrDash(w.maxEventLoopLagMs) }} peak
                    </span>
                    <span class="sys-worker-meta font-mono">
                      {{ metric(toNum(w.messagesProcessed)) }} msg / 2 min
                    </span>
                  </div>
                  <!-- No DB-errors figure: nothing increments that counter,
                       which is why the Dashboard and Queue Operations dropped
                       their series too. -->
                  <p class="sys-note">
                    Ack failures since broker start (cell-wide):
                    <span class="font-mono">{{ metric(lifetimeAckFailed) }}</span>
                  </p>
                </div>
              </div>
            </div>

            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Cell summary</h3>
                <span class="chip chip-mute">cell-level</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <div class="stat-grid stat-grid-5">
                  <div class="stat">
                    <div class="stat-label">Replicas</div>
                    <div class="stat-value font-mono">{{ metric(toNum(systemData.replicaCount)) }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Data points</div>
                    <div class="stat-value font-mono">{{ metric(toNum(systemData.pointCount)) }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Bucket size</div>
                    <div class="stat-value font-mono">{{ formatBucketSize(systemData.bucketMinutes) }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">CPU</div>
                    <div class="stat-value font-mono">{{ pct(latest.cpuUser) }}</div>
                    <div class="stat-foot">{{ acrossLabel }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Memory</div>
                    <div class="stat-value font-mono">{{ mb(latest.rss) }}</div>
                    <div class="stat-foot">{{ acrossLabel }}</div>
                  </div>
                </div>
              </div>
            </div>

            <div v-if="replicas.length" class="card">
              <div class="card-header">
                <h3>Server details</h3>
                <span class="card-sub">last sample per replica</span>
                <span class="chip chip-mute">cell-level</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <div class="sys-scroll">
                  <table class="t">
                    <thead>
                      <tr>
                        <th>Hostname</th>
                        <th class="right">Port</th>
                        <th class="right">CPU (user)</th>
                        <th class="right">CPU (sys)</th>
                        <th class="right">Memory</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="replica in replicas" :key="`${replica.hostname}:${replica.port}`">
                        <td style="font-weight:500;">{{ replica.hostname }}</td>
                        <td class="right font-mono tabular-nums">{{ replica.port }}</td>
                        <td class="right font-mono tabular-nums">{{ pct(cpuOf(replica, 'user_us')) }}</td>
                        <td class="right font-mono tabular-nums">{{ pct(cpuOf(replica, 'system_us')) }}</td>
                        <td class="right font-mono tabular-nums">{{ mb(lastOf(replica, ['memory', 'rss_bytes'])) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </template>
        </template>
      </template>

      <!-- =================== RAFT =================== -->
      <RaftCluster
        v-else
        :members="raftMembers.data.value"
        :loading="raftMembers.loading.value && !raftMembers.data.value"
        :absent="raftMembersAbsent"
        :stamp="stamp(raftMembers)"
        @recheck="recheckRaft"
      />
    </template>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue'

import BaseChart from '@/components/BaseChart.vue'
import RaftCluster from '@/components/RaftCluster.vue'
import { describeApiError, operator } from '@/api'
import { formatBytes, formatNumber, toNum, useApi } from '@/composables/useApi'
import { chartColor } from '@/composables/useChartTheme'
import {
  formatChartLabel, formatDateTimeLocal, formatTimestamp, formatTimestampUtc,
  isMultiDay, validateRange,
} from '@/composables/useFormat'
import {
  describeRaftFailure, formatIndex, formatMapPct, nodeView,
} from '@/composables/useRaftCluster'
import { useRefresh } from '@/composables/useRefresh'
import { numTone } from '@/composables/useSeverity'
import { stamp } from '@/composables/useStamp'
import { useIdentity } from '@/stores/identity'
import { isMissingRoute, routeSupport } from '@/stores/routeSupport'

// CELL-LEVEL PAGE — every source is an operator route (queen_proxy
// is_operator_route): /api/v1/analytics/system-metrics, the bare
// /api/v1/status, /api/v1/raft/status and /api/v1/raft/members. None of them
// is tenant-scopable: host CPU and a replicated log belong to the cell. The
// route already declares requires:'operator'; this guard also stops the calls
// if the operator session stops being live while the page is open.
const { can, actingTenantSlug, actingCellSlug } = useIdentity()
const canOperate = computed(() => can('operator'))

// 'system' | 'storage'. The second option is the cell's storage: the Raft
// cluster.
const dataSource = ref('system')
const viewMode = ref('aggregate')
const aggregationType = ref('avg')
const timeRange = ref(60)
const customMode = ref(false)
const customFrom = ref('')
const customTo = ref('')
const appliedCustom = ref(null)

const timeRanges = [
  { label: '15m', value: 15 },
  { label: '1h', value: 60 },
  { label: '6h', value: 360 },
  { label: '24h', value: 1440 },
]

const aggregationTypes = [
  { label: 'Average', value: 'avg' },
  { label: 'Maximum', value: 'max' },
  { label: 'Minimum', value: 'min' },
]

// ---------------------------------------------------------------------------
// Range
// ---------------------------------------------------------------------------
function currentRange() {
  if (customMode.value && appliedCustom.value) return appliedCustom.value
  const to = new Date()
  const from = new Date(to.getTime() - timeRange.value * 60_000)
  return { from, to }
}

// Live, not on-click: an invalid range explains itself as it is typed instead
// of leaving the user with a button that does nothing when pressed.
const customError = computed(() => validateRange(customFrom.value, customTo.value).error || '')
const customRangeValid = computed(() => !customError.value)

const selectQuickRange = (value) => {
  customMode.value = false
  timeRange.value = value
  metrics.refresh()
}

const toggleCustomMode = () => {
  customMode.value = !customMode.value
  if (customMode.value) {
    const now = new Date()
    customTo.value = formatDateTimeLocal(now)
    customFrom.value = formatDateTimeLocal(new Date(now.getTime() - timeRange.value * 60_000))
  } else {
    appliedCustom.value = null
    metrics.refresh()
  }
}

const applyCustomRange = () => {
  const parsed = validateRange(customFrom.value, customTo.value)
  if (parsed.error) return
  appliedCustom.value = { from: parsed.from, to: parsed.to }
  metrics.refresh()
}

// ---------------------------------------------------------------------------
// Fetchers — each panel keeps its own error, so a dead Raft tab cannot leave
// the resources tab rendering minutes-old numbers as if they were live.
// ---------------------------------------------------------------------------
const metrics = useApi((config) => {
  const { from, to } = currentRange()
  return operator.getSystemMetrics({ from: from.toISOString(), to: to.toISOString() }, config)
}, { immediate: false })

const status = useApi((config) => operator.getStatus(undefined, config), { immediate: false })

// The raft routes. A broker without them answers 404 and a proxy that does
// not classify them 404 route_blocked — a state to render, not a failure to
// retry — so each is guarded (asked once per cluster epoch) and a probe (no
// toast for that answer).
const raftStatus = useApi(
  routeSupport.guard('raft-status', (config) => operator.getRaftStatus({ ...config, probe: true })),
  { immediate: false },
)
const raftMembers = useApi(
  routeSupport.guard('raft-members', (config) => operator.getRaftMembers({ ...config, probe: true })),
  { immediate: false },
)

const fetchData = () => {
  if (!canOperate.value) return
  // The summary block, under either source.
  raftStatus.refresh()
  if (dataSource.value === 'storage') {
    raftMembers.refresh()
  } else {
    metrics.refresh()
    status.refresh()
  }
}

const selectSource = (src) => {
  dataSource.value = src
  fetchData()
}

useRefresh(fetchData)
watch(canOperate, (live) => { if (live) fetchData() }, { immediate: true })

/** "Check again" on the quiet state: forget the verdict for the family, ask once more. */
const recheckRaft = () => {
  routeSupport.forget('raft-members')
  routeSupport.forget('raft-status')
  raftMembers.refresh()
  raftStatus.refresh()
}

// ---------------------------------------------------------------------------
// Panel state
// ---------------------------------------------------------------------------
const systemData = computed(() => metrics.data.value)
const metricsFirstLoad = computed(() => metrics.loading.value && !metrics.data.value)

/** A number we hold, or an em dash — never a 0 standing in for "unknown". */
const metric = (v) => (v === null || v === undefined ? '—' : formatNumber(v))
const bytes = (v) => (v === null || v === undefined ? '—' : formatBytes(v))
const pct = (v) => (v === null || v === undefined ? '—' : `${(v / 100).toFixed(1)}%`)
const mb = (v) => (v === null || v === undefined ? '—' : `${Math.round(v / 1024 / 1024)} MB`)
const msOrDash = (v) => {
  const n = toNum(v)
  return n === null ? '—' : `${Math.round(n)}ms`
}

const formatBucketSize = (minutes) => {
  const n = toNum(minutes)
  if (!n) return '1 min'
  if (n < 60) return `${n} min`
  const hours = Math.floor(n / 60)
  const rest = n % 60
  return rest === 0 ? `${hours}h` : `${hours}h ${rest}m`
}

// ---------------------------------------------------------------------------
// Replicated log — the node that answered /api/v1/raft/status
// ---------------------------------------------------------------------------
const node = computed(() => nodeView(raftStatus.data.value))
const raftAlarm = computed(() => (node.value?.severity || '') !== '')
const raftStatusAbsent = computed(() => isMissingRoute(raftStatus.error.value))
const raftMembersAbsent = computed(() => isMissingRoute(raftMembers.error.value))

// ---------------------------------------------------------------------------
// Brokers (bare /api/v1/status — workers seen in the last two minutes)
// ---------------------------------------------------------------------------
const workers = computed(() => status.data.value?.workers || [])
const lifetimeAckFailed = computed(() => toNum(status.data.value?.errors?.ackFailed))

// The chip states what the payload says, not what we hope. Only the event-loop
// gauges are actually written by this broker (min_free_slots / db_connections /
// max_job_queue_size never are), so nothing else is rendered here.
const workerChip = (w) => {
  const peak = toNum(w.maxEventLoopLagMs)
  const avg = toNum(w.avgEventLoopLagMs)
  if (peak === null && avg === null) return { cls: 'chip-mute', label: 'no lag data' }
  if ((peak ?? 0) > 500 || (avg ?? 0) > 200) return { cls: 'chip-bad', label: 'event loop stalling' }
  if ((peak ?? 0) > 100 || (avg ?? 0) > 50) return { cls: 'chip-warn', label: 'event loop busy' }
  return { cls: 'chip-ok', label: 'responsive' }
}

// ---------------------------------------------------------------------------
// Replica time series
//
// Replicas do not share a bucket grid: one may start later, restart, or miss a
// collector tick. Labels therefore come from the UNION of every replica's
// timestamps and each series is indexed BY timestamp, so a shorter series is a
// gap in the line rather than a silent shift onto another replica's clock.
// ---------------------------------------------------------------------------
const replicas = computed(() => systemData.value?.replicas || [])
const replicaCountLabel = computed(() => {
  const n = replicas.value.length
  return `${n} replica${n === 1 ? '' : 's'}`
})
const acrossLabel = computed(() =>
  replicas.value.length > 1 ? `summed across ${replicaCountLabel.value}` : 'this replica',
)

const timeline = computed(() => {
  const seen = new Set()
  for (const r of replicas.value) {
    for (const point of r.timeSeries || []) {
      if (point?.timestamp) seen.add(point.timestamp)
    }
  }
  return [...seen].sort()
})

const chartLabels = computed(() => {
  const multiDay = isMultiDay(timeline.value)
  return timeline.value.map(ts => formatChartLabel(new Date(ts), multiDay))
})

const pointsByTimestamp = (replica) => {
  const map = new Map()
  for (const point of replica.timeSeries || []) {
    if (point?.timestamp) map.set(point.timestamp, point)
  }
  return map
}

const leaf = (metricsObj, path) => {
  let node = metricsObj
  for (const key of path) {
    node = node?.[key]
    if (node === undefined || node === null) return null
  }
  return node
}

/** One replica's values aligned to the union timeline; null where it has none. */
const replicaSeries = (replica, path, scale = v => v) => {
  const byTs = pointsByTimestamp(replica)
  return timeline.value.map(ts => {
    const node = leaf(byTs.get(ts)?.metrics, path)
    const v = toNum(node?.[aggregationType.value])
    return v === null ? null : scale(v)
  })
}

/** Sum across replicas per bucket; null only when no replica reported it. */
const summedSeries = (path, scale = v => v) => {
  const perReplica = replicas.value.map(r => replicaSeries(r, path, scale))
  return timeline.value.map((_, i) => {
    let sum = null
    for (const series of perReplica) {
      const v = series[i]
      if (v !== null) sum = (sum || 0) + v
    }
    return sum
  })
}

const CPU_SCALE = v => v / 100
const MB_SCALE = v => Math.round(v / 1024 / 1024)

const buildChart = (specs, { perReplicaPath = null, perReplicaScale = v => v, perReplicaSuffix = '' } = {}) => {
  if (!replicas.value.length || !timeline.value.length) return { labels: [], datasets: [] }
  if (viewMode.value === 'individual' && perReplicaPath) {
    return {
      labels: chartLabels.value,
      datasets: replicas.value.map((replica, i) => ({
        label: `${replica.hostname}${perReplicaSuffix}`,
        data: replicaSeries(replica, perReplicaPath, perReplicaScale),
        borderColor: chartColor(i).line,
        fill: false,
        tension: 0,
      })),
    }
  }
  return {
    labels: chartLabels.value,
    datasets: specs.map((spec, i) => ({
      label: spec.label,
      data: summedSeries(spec.path, spec.scale),
      borderColor: chartColor(i).line,
      backgroundColor: chartColor(i).fill,
      fill: true,
      tension: 0,
    })),
  }
}

const cpuChart = computed(() => buildChart(
  [
    { label: 'User CPU (%)', path: ['cpu', 'user_us'], scale: CPU_SCALE },
    { label: 'System CPU (%)', path: ['cpu', 'system_us'], scale: CPU_SCALE },
  ],
  { perReplicaPath: ['cpu', 'user_us'], perReplicaScale: CPU_SCALE, perReplicaSuffix: ' · user' },
))

const memoryChart = computed(() => buildChart(
  [{ label: 'RSS (MB)', path: ['memory', 'rss_bytes'], scale: MB_SCALE }],
  { perReplicaPath: ['memory', 'rss_bytes'], perReplicaScale: MB_SCALE },
))

// ---------------------------------------------------------------------------
// Latest sample. Reduced ACROSS replicas — the old card printed replicas[0]
// beside a "Replicas: N" counter, which reads as a cell figure and is not one.
// ---------------------------------------------------------------------------
const lastOf = (replica, path) => {
  const series = replica?.timeSeries || []
  for (let i = series.length - 1; i >= 0; i--) {
    const v = toNum(leaf(series[i]?.metrics, path)?.last)
    if (v !== null) return v
  }
  return null
}

const cpuOf = (replica, key) => lastOf(replica, ['cpu', key])

const sumLatest = (path) => {
  let sum = null
  for (const replica of replicas.value) {
    const v = lastOf(replica, path)
    if (v !== null) sum = (sum || 0) + v
  }
  return sum
}

const latest = computed(() => ({
  cpuUser: sumLatest(['cpu', 'user_us']),
  rss: sumLatest(['memory', 'rss_bytes']),
}))

// Chart options
const cpuOptions = {
  plugins: { legend: { display: true, position: 'top', labels: { usePointStyle: true, padding: 14 } } },
  scales: {
    y: {
      title: { display: true, text: 'CPU %', font: { size: 11 } },
      ticks: { callback: (value) => `${Number(value).toFixed(1)}%` },
    },
  },
}
const memoryOptions = {
  plugins: { legend: { display: true, position: 'top', labels: { usePointStyle: true, padding: 14 } } },
  scales: { y: { title: { display: true, text: 'Memory (MB)', font: { size: 11 } } } },
}
</script>

<style scoped>
/* Everything shared with the other nine views now lives in style.css:
   .scope-strip*, the .filter-* card family, .card-sub, .stat-grid*, .view-banner,
   .panel-err and .panel-msg*. What is left below is System's own layout. */

/* One step louder than the scope strip on purpose — the replicated log is
   actually in trouble here, so it keeps its own alpha rather than collapsing
   to --warn-bd. */
.card-alarm { border-color: color-mix(in srgb, var(--warn-400) 35%, transparent); }

.sys-note { margin-top: 10px; font-size: 11.5px; color: var(--text-low); }
.sys-scroll { overflow-x: auto; }
.right { text-align: right; }

/* The panel pair. Not a stat grid: it lays out CARDS, at the 16px block
   rhythm, so it keeps its own rule (as Analytics' .an-grid-2 does). */
.sys-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }

.sys-workers { display: flex; flex-direction: column; gap: 8px; }
.sys-worker {
  display: flex; align-items: center; flex-wrap: wrap; gap: 10px;
  padding: 8px 10px; border: 1px solid var(--bd); border-radius: var(--r-card);
}
.sys-worker-host { font-size: 12px; color: var(--text-hi); font-weight: 500; }
.sys-worker-meta { font-size: 11px; color: var(--text-mid); }

@media (max-width: 1100px) {
  .sys-grid-2 { grid-template-columns: 1fr; }
}
</style>
