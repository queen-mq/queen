<template>
  <!--
    System: the cell under every tenant, an operator's page. Seven views in a
    fixed sub sidebar under three groups, kept in the address as ?view=
    (composables/systemViews.js); the node is the header's picker (?node=),
    on the views it narrows. Each row of the list carries the one number of
    its view, coloured only where that number is the trouble.
  -->
  <SubSidebar label="System sections" :current="currentName">
    <template #side>
      <nav class="sys-side" aria-label="System views">
        <div v-for="g in side" :key="g.label || 'top'" class="ss-group">
          <div v-if="g.label" class="ss-label">{{ g.label }}</div>
          <router-link
            v-for="r in g.rows"
            :key="r.key"
            :to="viewTo(r.key)"
            class="ss-row"
            :class="{ on: view === r.key }"
            :aria-current="view === r.key ? 'page' : undefined"
            :title="r.title || undefined"
          >
            <Icon v-if="r.icon" :name="r.icon" :size="16" class="ss-icon" />
            <span class="ss-text"><span class="ss-name">{{ r.name }}</span></span>
            <span class="ss-num" :class="[r.tone, { 'sys-side-word': r.key === 'overview' }]">{{ r.num }}<span v-if="r.tone" class="sr-only"> · {{ r.tone === 'bad' ? 'failing' : 'needs attention' }}</span></span>
          </router-link>
        </div>
      </nav>
    </template>

    <PaneHead :title="currentName" :facts="canOperate ? facts : []">
      <template v-if="canOperate && viewInfo.picker" #actions>
        <UiSelect v-model="focusNode" label="Node" :options="nodeOptions" aria-label="Node" />
      </template>
    </PaneHead>

    <div class="view-container">
      <div v-if="!canOperate" class="card">
        <div class="empty-state">
          <Icon name="shield-check" :size="28" class="empty-state-icon" />
          <p :title="`Nothing here is scoped to ${actingTenantSlug || 'your tenant'}: it covers every tenant on the cell`">Only a live operator can open this page</p>
        </div>
      </div>


      <!-- =================== OVERVIEW: the cell at a glance =================== -->
      <div v-else-if="view === 'overview'" class="sys-stack">
        <CellStatus
          :cell="cell"
          :issues="issueLinks"
          :note="nodesNote"
          full
          :members="memberRows"
          :node-to="nodeTo"
          :stamp-text="stamp(raftMembers)"
        />

        <!-- Each node over the range: four charts, each opening its view. -->
        <div class="sys-ov-head">
          <span class="sys-ov-label">Each node over time</span>
          <UiSelect v-model="rangeChoice" label="Range" :options="shortRangeOptions" aria-label="Time range" size="sm" />
        </div>
        <div v-if="metrics.failed.value && !systemData" class="card"><div class="card-body"><div class="panel-err">{{ describeApiError(metrics.error.value) }}</div></div></div>
        <div v-else class="sys-grid-2">
          <div v-for="c in overviewCharts" :key="c.key" class="card">
            <div class="card-header">
              <router-link class="sys-chart-title" :to="viewTo(c.view)">{{ c.title }}<Icon name="chevron-right" :size="14" /></router-link>
              <span class="card-sub">{{ c.sub }}</span>
            </div>
            <div class="card-body">
              <div v-if="metricsFirstLoad" class="skeleton" style="height:200px;" />
              <BaseChart v-else-if="c.chart.labels.length" type="line" :data="c.chart" :options="c.options" height="200px" />
              <div v-else class="panel-msg">{{ c.none }}</div>
            </div>
          </div>
        </div>
      </div>

      <!-- =================== RAFT: the members, and what crosses the Raft port =================== -->
      <div v-else-if="view === 'raft'" class="sys-stack">
        <div v-if="raftMembers.failed.value && !raftMembersAbsent" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(raftMembers.lastUpdated.value)"><strong>Could not load the Raft cluster</strong> · {{ describeRaftFailure(raftMembers.error.value) }}<template v-if="raftMembers.data.value"> · showing the last members{{ raftMembers.lastUpdated.value ? ` (${formatTimestamp(raftMembers.lastUpdated.value)})` : '' }}</template></span>
        </div>

        <RaftCluster
          :members="raftMembers.data.value"
          :loading="raftMembers.loading.value && !raftMembers.data.value"
          :absent="raftMembersAbsent"
          :stamp="stamp(raftMembers)"
          :focus-index="focusIndex"
          @recheck="recheckRaft"
        />

        <div v-if="metrics.failed.value" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(metrics.lastUpdated.value)"><strong>Could not load server metrics</strong> · {{ describeApiError(metrics.error.value) }}<template v-if="systemData"> · showing the last samples{{ metrics.lastUpdated.value ? ` (${formatTimestamp(metrics.lastUpdated.value)})` : '' }}</template></span>
        </div>
        <div v-if="focusNode && systemData && !replicas.length" class="status-banner banner-info view-banner">
          <span><strong>{{ focusNode }}</strong> sent no samples in this range.</span>
        </div>

        <PageTools>
          <span class="sys-range" :title="bucketTitle">
            <UiSelect v-model="rangeChoice" label="Range" :options="rangeOptions" aria-label="Time range" />
          </span>
          <template #view>
            <div class="tool-seg" :title="viewTitle">
              <span class="tool-label">View</span>
              <div class="seg" role="group" aria-label="View">
                <button :class="{ on: viewMode === 'individual' }" :aria-pressed="viewMode === 'individual'" @click="viewMode = 'individual'">Per server</button>
                <button :class="{ on: viewMode === 'aggregate' }" :aria-pressed="viewMode === 'aggregate'" @click="viewMode = 'aggregate'">Aggregate</button>
              </div>
            </div>
            <UiSelect v-model="aggregationType" label="Metric" :options="aggregationTypes" aria-label="Value of each point" />
          </template>
        </PageTools>

        <div v-if="customMode" class="page-tools">
          <label class="tool-field">
            <span class="tool-label">From</span>
            <input v-model="customFrom" type="datetime-local" class="input" :title="formatTimestampUtc(customFrom)" />
          </label>
          <label class="tool-field">
            <span class="tool-label">To</span>
            <input v-model="customTo" type="datetime-local" class="input" :title="formatTimestampUtc(customTo)" />
          </label>
          <button class="btn btn-primary" :disabled="!customRangeValid" @click="applyCustomRange">Apply</button>
          <span v-if="customError" class="tool-note is-bad">{{ customError }}</span>
        </div>

        <div v-if="metricsFirstLoad" class="sys-stack">
          <div class="sys-grid-2">
            <div v-for="i in 2" :key="i" class="card"><div class="card-body"><div class="skeleton" style="height:240px;" /></div></div>
          </div>
        </div>
        <template v-else-if="systemData">
          <div class="card">
            <div class="card-header" :title="raftTrafficHelp">
              <h3>Raft traffic</h3>
              <span class="card-sub">{{ viewMode === 'individual' ? 'solid sent · dashed received' : 'bytes on the wire, by channel' }}</span>
              <span class="muted">{{ stamp(metrics) }}</span>
            </div>
            <div class="card-body">
              <div v-if="cell?.raft.single" class="panel-msg">One node, one copy of the log: nothing crosses the Raft port</div>
              <BaseChart v-else-if="raftTrafficChart.hasData" type="line" :data="raftTrafficChart" :options="raftTrafficOptions" height="240px" />
              <div v-else class="panel-msg" title="Older releases do not record it">No Raft traffic samples in this range</div>
            </div>
          </div>

        </template>
      </div>

      <!-- =================== STORAGE: the log and the store on each node =================== -->
      <div v-else-if="view === 'storage'" class="sys-stack">
        <div v-if="raftMembers.failed.value && !raftMembersAbsent" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(raftMembers.lastUpdated.value)"><strong>Could not load the Raft cluster</strong> · {{ describeRaftFailure(raftMembers.error.value) }}<template v-if="raftMembers.data.value"> · showing the last members{{ raftMembers.lastUpdated.value ? ` (${formatTimestamp(raftMembers.lastUpdated.value)})` : '' }}</template></span>
        </div>

        <!-- What grows on each node's disk, over the range: the log, and how
             full the store's map is. -->
        <div class="sys-ov-head">
          <span class="sys-ov-label">Each node over time</span>
          <UiSelect v-model="rangeChoice" label="Range" :options="shortRangeOptions" aria-label="Time range" size="sm" />
        </div>
        <div class="sys-grid-2">
          <div v-for="c in storageCharts" :key="c.key" class="card">
            <div class="card-header">
              <h3>{{ c.title }}</h3>
              <span class="card-sub">{{ c.sub }}</span>
            </div>
            <div class="card-body">
              <div v-if="metricsFirstLoad" class="skeleton" style="height:220px;" />
              <BaseChart v-else-if="c.chart.labels.length" type="line" :data="c.chart" :options="c.options" height="220px" />
              <div v-else class="panel-msg">{{ c.none }}</div>
            </div>
          </div>
        </div>

        <section class="card" aria-labelledby="sys-store-title">
          <div class="card-header">
            <h3 id="sys-store-title">On each node</h3>
            <span class="muted">{{ stamp(raftMembers) }}</span>
          </div>
          <div v-if="!memberRows.length" class="sys-quiet">{{ nodesNote }}</div>
          <div v-else class="table-container">
            <table class="t sys-table">
              <thead>
                <tr>
                  <th>Node</th>
                  <th class="num">Log</th>
                  <th class="num">Files</th>
                  <th class="num" title="Share of the store's map in use; from 85% the broker refuses new writes">Store map</th>
                  <th class="num">Used of the map</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="m in memberRows" :key="m.name">
                  <td><span class="sys-host">{{ m.name }}</span></td>
                  <td class="num">{{ m.log }}</td>
                  <td class="num">{{ m.files }}</td>
                  <td class="num" :class="m.mapTone">{{ m.map }}</td>
                  <td class="num">{{ m.mapUsed }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
        <div v-if="!raftStatusAbsent" class="card" :class="{ 'sys-alarm': raftAlarm }">
          <div class="card-header">
            <h3>Replicated log</h3>
            <span v-if="node && node.nodeId !== null" class="card-sub" title="The node this console's calls reach">as node {{ node.nodeId }}{{ node.hostname ? ` · ${node.hostname}` : '' }} holds it</span>
            <span class="muted">{{ stamp(raftStatus) }}</span>
          </div>
          <div class="card-body">
            <div v-if="raftStatus.failed.value" class="panel-err">
              Log state unavailable · {{ describeRaftFailure(raftStatus.error.value) }}
            </div>
            <div class="stat-grid stat-grid-6">
              <div class="stat">
                <div class="stat-label">Role</div>
                <div class="stat-value">
                  <span v-if="!node?.state">—</span>
                  <span v-else class="sys-state"><span v-if="roleMark" class="g" :class="roleMark" aria-hidden="true" />{{ capital(node.chip.label) }}</span>
                </div>
                <div class="stat-foot">{{ node?.clusterNote || '—' }}</div>
              </div>
              <div class="stat">
                <div class="stat-label">Term</div>
                <div class="stat-value">{{ metric(node?.term) }}</div>
                <div class="stat-foot">
                  <span v-if="node" :class="{ 'num warn': node.leaderSeverity === 'warn' }">{{ node.leaderNote }}</span>
                  <span v-else>—</span>
                </div>
              </div>
              <!-- Three indexes, one tile: applied is this node's own; the two
                   under it are what it trails, each on its own line. -->
              <div class="stat">
                <div class="stat-label">Applied</div>
                <div class="stat-value">{{ formatIndex(node?.applied) }}</div>
                <div class="stat-foot">committed <span class="tabular-nums">{{ formatIndex(node?.committed) }}</span></div>
                <div class="stat-foot">durable <span class="tabular-nums">{{ formatIndex(node?.durable) }}</span></div>
              </div>
              <div class="stat">
                <div class="stat-label">Inflight</div>
                <div class="stat-value">{{ metric(node?.inflight) }}</div>
                <div class="stat-foot">appended, not applied</div>
              </div>
              <div class="stat">
                <div class="stat-label">Log</div>
                <div class="stat-value">{{ bytes(node?.logBytes) }}</div>
                <div class="stat-foot">{{ metric(node?.logFiles) }} files</div>
              </div>
              <div class="stat">
                <div class="stat-label">Store map</div>
                <div class="stat-value num" :class="numTone(node?.mapSeverity)">{{ formatMapPct(node?.mapPct) }}</div>
                <div class="stat-foot">{{ bytes(node?.mapUsed) }} of {{ bytes(node?.mapBytes) }}</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- =================== CPU AND MEMORY, DISK, NETWORK: each node over time =================== -->
      <div v-else class="sys-stack">
        <div v-if="metrics.failed.value" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(metrics.lastUpdated.value)"><strong>Could not load server metrics</strong> · {{ describeApiError(metrics.error.value) }}<template v-if="systemData"> · showing the last samples{{ metrics.lastUpdated.value ? ` (${formatTimestamp(metrics.lastUpdated.value)})` : '' }}</template></span>
        </div>
        <div v-if="focusNode && systemData && !replicas.length" class="status-banner banner-info view-banner">
          <span><strong>{{ focusNode }}</strong> sent no samples in this range.</span>
        </div>

        <PageTools>
          <span class="sys-range" :title="bucketTitle">
            <UiSelect v-model="rangeChoice" label="Range" :options="rangeOptions" aria-label="Time range" />
          </span>
          <template #view>
            <div class="tool-seg" :title="viewTitle">
              <span class="tool-label">View</span>
              <div class="seg" role="group" aria-label="View">
                <button :class="{ on: viewMode === 'individual' }" :aria-pressed="viewMode === 'individual'" @click="viewMode = 'individual'">Per server</button>
                <button :class="{ on: viewMode === 'aggregate' }" :aria-pressed="viewMode === 'aggregate'" @click="viewMode = 'aggregate'">Aggregate</button>
              </div>
            </div>
            <UiSelect v-model="aggregationType" label="Metric" :options="aggregationTypes" aria-label="Value of each point" />
          </template>
        </PageTools>

        <div v-if="customMode" class="page-tools">
          <label class="tool-field">
            <span class="tool-label">From</span>
            <input v-model="customFrom" type="datetime-local" class="input" :title="formatTimestampUtc(customFrom)" />
          </label>
          <label class="tool-field">
            <span class="tool-label">To</span>
            <input v-model="customTo" type="datetime-local" class="input" :title="formatTimestampUtc(customTo)" />
          </label>
          <button class="btn btn-primary" :disabled="!customRangeValid" @click="applyCustomRange">Apply</button>
          <span v-if="customError" class="tool-note is-bad">{{ customError }}</span>
        </div>

        <div v-if="metricsFirstLoad" class="sys-stack">
          <div class="sys-grid-2">
            <div v-for="i in 2" :key="i" class="card"><div class="card-body"><div class="skeleton" style="height:240px;" /></div></div>
          </div>
        </div>
        <template v-else-if="systemData">
          <template v-if="view === 'compute'">
            <div class="sys-grid-2">
            <div class="card">
              <div class="card-header">
                <h3>CPU</h3>
                <span class="card-sub">{{ viewMode === 'individual' ? 'user, per server' : replicaCountLabel }}</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="cpuChart.labels.length" type="line" :data="cpuChart" :options="cpuOptions" height="240px" />
                <div v-else class="panel-msg">No CPU samples in this range</div>
              </div>
            </div>
            <div class="card">
              <div class="card-header">
                <h3>Memory</h3>
                <span class="card-sub">{{ viewMode === 'individual' ? 'resident, per server' : replicaCountLabel }}</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="memoryChart.labels.length" type="line" :data="memoryChart" :options="memoryOptions" height="240px" />
                <div v-else class="panel-msg">No memory samples in this range</div>
              </div>
            </div>
            </div>
          <div class="card">
            <div class="card-header">
              <h3>Event loop</h3>
              <span class="card-sub">every worker on the cell, average and peak, per minute</span>
              <span class="muted">{{ stamp(status) }}</span>
            </div>
            <div class="card-body">
              <div v-if="status.failed.value && !status.data.value" class="panel-err">{{ describeApiError(status.error.value) }}</div>
              <RowChart v-else :series="eventLoopSeries" :labels="eventLoopLabels" :value-format="fmtMs" unit="ms" variant="full" />
            </div>
          </div>
          <div class="card">
            <div class="card-header">
              <h3>Workers</h3>
              <span class="card-sub" title="Cell-wide, since each broker started">{{ metric(lifetimeAckFailed) }} ack failures since start</span>
              <span class="muted">{{ stamp(status) }}</span>
            </div>
            <div
              v-if="batchEfficiency"
              class="sys-batch"
              title="Rows per batch across every tenant on the cell, over the range: the more rows a batch carries, the less each pays for its commit"
            >
              <span class="sys-batch-k">Rows per batch</span>
              <span>push <b>{{ batchEfficiency.push }}</b></span>
              <span>pop <b>{{ batchEfficiency.pop }}</b></span>
              <span>ack <b>{{ batchEfficiency.ack }}</b></span>
            </div>
            <div v-if="status.failed.value" class="card-body"><div class="panel-err">{{ describeApiError(status.error.value) }}</div></div>
            <div v-else-if="!workers.length" class="sys-quiet">{{ focusNode ? `No worker of ${focusNode} reported in the last two minutes` : 'No worker reported in the last two minutes' }}</div>
            <div v-else class="table-container">
              <table class="t sys-table">
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th title="How quickly the worker's event loop answers: under 50 ms average and 100 ms peak it is responsive">Event loop</th>
                    <th class="num">Average</th>
                    <th class="num">Peak</th>
                    <th class="num">Messages, 2 min</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="w in workers" :key="`${w.hostname}:${w.workerId}`">
                    <td><span class="sys-host">{{ w.hostname }}</span><span class="sys-port"> · {{ w.workerId }}</span></td>
                    <td>
                      <span class="sys-state"><span class="sys-mark" aria-hidden="true"><span v-if="workerChip(w).mark" class="g" :class="workerChip(w).mark" /></span>{{ workerChip(w).label }}</span>
                    </td>
                    <td class="num">{{ msOrDash(w.avgEventLoopLagMs) }}</td>
                    <td class="num">{{ msOrDash(w.maxEventLoopLagMs) }}</td>
                    <td class="num">{{ metric(toNum(w.messagesProcessed)) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
          </template>

          <template v-else-if="view === 'disk'">
            <section v-if="cell" class="card" aria-labelledby="sys-used-title">
              <div class="card-header">
                <h3 id="sys-used-title">Used, per node</h3>
                <span class="card-sub">of the data disk; from 99.5% writes stop</span>
              </div>
              <div class="sys-bars">
                <div v-for="d in diskRows" :key="d.name" class="sys-bar">
                  <span class="sys-bar-name">{{ d.name }}</span>
                  <span class="sys-bar-track"><span class="sys-bar-fill" :class="d.tone" :style="{ width: `${d.width}%` }" /></span>
                  <span class="sys-bar-v" :class="d.tone">{{ d.text }}</span>
                </div>
              </div>
            </section>
            <div class="card">
              <div class="card-header" title="How full each node's data disk was, every minute: the same measure as the write gate, which stops writes at 99.5%">
                <h3>Used over time</h3>
                <span class="card-sub">of the data disk, per node</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="diskUsedChart.labels.length" type="line" :data="diskUsedChart" :options="pctOptions" height="240px" />
                <div v-else class="panel-msg" title="Each node records its disk use every minute from 2.7.0-beta.2; a broker before it does not">Not recorded by this broker yet</div>
              </div>
            </div>
            <div class="card">
              <div class="card-header" title="What the broker process read from and wrote to storage. A read the page cache serves is not a disk read.">
                <h3>Disk</h3>
                <span class="card-sub">{{ viewMode === 'individual' ? 'solid read · dashed write' : replicaCountLabel }}</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="diskChart.hasData" type="line" :data="diskChart" :options="diskOptions" height="240px" />
                <div v-else class="panel-msg" title="Older releases do not record it">No disk samples in this range</div>
              </div>
            </div>
          </template>

          <template v-else-if="view === 'network'">
            <div class="card">
              <div class="card-header" title="What each node's network interfaces received and sent: every client, the Raft port and anything else on them. In a container, its own interfaces; loopback and bridges are left out.">
                <h3>Network</h3>
                <span class="card-sub">{{ viewMode === 'individual' ? 'solid in · dashed out' : replicaCountLabel }}</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="networkChart.hasData" type="line" :data="networkChart" :options="networkOptions" height="240px" />
                <div v-else class="panel-msg" title="A broker records the network on Linux only, and older releases not at all">No network samples in this range</div>
              </div>
            </div>
          </template>
        </template>
      </div>
    </div>
  </SubSidebar>
</template>

<script setup>
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import BaseChart from '@/components/BaseChart.vue'
import RowChart from '@/components/RowChart.vue'
import CellStatus from '@/components/CellStatus.vue'
import Icon from '@/components/Icon.vue'
import PageTools from '@/components/PageTools.vue'
import PaneHead from '@/components/PaneHead.vue'
import RaftCluster from '@/components/RaftCluster.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import UiSelect from '@/components/UiSelect.vue'
import { describeApiError, operator } from '@/api'
import { formatBytes, formatDuration, formatNumber, toNum, trimIncompleteBuckets, useApi } from '@/composables/useApi'
import { brokerStatus, cellIssues } from '@/composables/useBrokerStatus'
import { lineColor } from '@/composables/useChartTheme'
import {
  formatChartLabel, formatDateTimeLocal, formatTimestamp, formatTimestampUtc,
  isMultiDay, validateRange,
} from '@/composables/useFormat'
import {
  clusterView, describeRaftFailure, formatIndex, formatMapPct, nodeView,
} from '@/composables/useRaftCluster'
import { useRefresh } from '@/composables/useRefresh'
import { useRouteRange } from '@/composables/useRouteRange'
import { useRouteState } from '@/composables/useRouteState'
import { numTone } from '@/composables/useSeverity'
import { nodeEntries, nodeReplicas } from '@/composables/systemSide'
import {
  cellIssueView, cellVerdict, systemSideGroups, systemView, systemViewAlias, systemViewOf, systemViewQuery,
} from '@/composables/systemViews'
import { RAFT_CHANNELS, formatRate, inUnit, rateUnit } from '@/composables/useTraffic'
import { stamp } from '@/composables/useStamp'
import { useIdentity } from '@/stores/identity'
import { isMissingRoute, routeSupport } from '@/stores/routeSupport'
import { DEFAULT_TIME_RANGE, SYSTEM_TIME_RANGES as timeRanges, relativeTimeRange } from '../../../shared/timeRanges.mjs'

// CELL-LEVEL PAGE — every source is an operator route (queen_proxy
// is_operator_route): /api/v1/analytics/system-metrics, the bare
// /api/v1/status, /api/v1/raft/status and /api/v1/raft/members. None of them
// is tenant-scopable: host CPU and a replicated log belong to the cell. The
// route already declares requires:'operator'; this guard also stops the calls
// if the operator session stops being live while the page is open.
const { can, actingTenantSlug, actingCellSlug } = useIdentity()
const canOperate = computed(() => can('operator'))
const route = useRoute()
const router = useRouter()

// The view (composables/systemViews.js) and the node picked in the header, by
// its name (the members' hostname, which is also the metrics' replica
// hostname: both are the broker's node label); '' for the whole cell. Both in
// the address, so a pick is a link and Back returns. An old ?view=resources
// is rewritten to the view that holds it now.
const viewRaw = ref('')
const focusNode = ref('')
useRouteState({ view: viewRaw, node: focusNode })
const view = computed(() => systemView(viewRaw.value))
watch(() => route.query.view, (v) => {
  const to = systemViewAlias(v)
  if (to && route.name === 'System') router.replace({ query: { ...route.query, view: to } })
}, { immediate: true })
const viewInfo = computed(() => systemViewOf(view.value))
const currentName = computed(() => viewInfo.value.name)
const viewTo = (key) => ({ path: '/system', query: { ...route.query, view: systemViewQuery(key) } })
const nodeTo = (name) => ({ path: '/system', query: { ...route.query, view: 'compute', node: name } })

// A node picked: one line per replica of it. The whole cell: summed.
const viewMode = ref(focusNode.value ? 'individual' : 'aggregate')
watch(focusNode, (name) => { viewMode.value = name ? 'individual' : 'aggregate' })
const aggregationType = ref('avg')
const aggregationTypes = [
  { value: 'avg', label: 'Average' },
  { value: 'max', label: 'Maximum' },
  { value: 'min', label: 'Minimum' },
]

// ---------------------------------------------------------------------------
// Range, in the address (?range=, or ?from=&to= for a window of your own)
// ---------------------------------------------------------------------------
const timeRange = ref(DEFAULT_TIME_RANGE)
const customMode = ref(false)
const customFrom = ref('')
const customTo = ref('')
const appliedCustom = ref(null)
useRouteRange({ range: timeRange, customMode, customFrom, customTo, appliedCustom, reload: () => refreshResources() })

function currentRange() {
  if (customMode.value && appliedCustom.value) return appliedCustom.value
  return relativeTimeRange(timeRange.value)
}

const rangeOptions = [
  ...timeRanges.map((r) => ({ value: r.value, label: r.longLabel })),
  { value: 'custom', label: 'Custom' },
]
const rangeChoice = computed({
  get: () => (customMode.value ? 'custom' : timeRange.value),
  set: (value) => {
    if (value === 'custom') {
      // The form opens on the range on screen; nothing moves until Apply.
      customMode.value = true
      const { from, to } = relativeTimeRange(timeRange.value)
      customTo.value = formatDateTimeLocal(to)
      customFrom.value = formatDateTimeLocal(from)
      return
    }
    customMode.value = false
    appliedCustom.value = null
    timeRange.value = value
    refreshResources()
  },
})

// Live, not on-click: an invalid range explains itself as it is typed instead
// of leaving the user with a button that does nothing when pressed.
const customError = computed(() => validateRange(customFrom.value, customTo.value).error || '')
const customRangeValid = computed(() => !customError.value)
const applyCustomRange = () => {
  const parsed = validateRange(customFrom.value, customTo.value)
  if (parsed.error) return
  appliedCustom.value = { from: parsed.from, to: parsed.to }
  refreshResources()
}

// ---------------------------------------------------------------------------
// Fetchers — each panel keeps its own error, so a dead Raft read cannot leave
// the resources tab rendering minutes-old numbers as if they were live.
// ---------------------------------------------------------------------------
const metrics = useApi((config) => {
  const { from, to } = currentRange()
  return operator.getSystemMetrics({ from: from.toISOString(), to: to.toISOString() }, config)
}, { immediate: false })

// The workers now, and per bucket over the page's range their event loop
// (`throughput`) and the cell's rows per batch (`messages.batchEfficiency`).
const status = useApi((config) => {
  const { from, to } = currentRange()
  return operator.getStatus({ from: from.toISOString(), to: to.toISOString() }, config)
}, { immediate: false })

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

function refreshResources() {
  if (!canOperate.value || !viewInfo.value.charts) return
  metrics.refresh()
  if (view.value === 'compute') status.refresh()
}
const fetchData = () => {
  if (!canOperate.value) return
  // The list's numbers and the node picker, on every view.
  raftStatus.refresh()
  raftMembers.refresh()
  refreshResources()
}

useRefresh(fetchData)
watch(canOperate, (live) => { if (live) fetchData() }, { immediate: true })
watch(view, () => refreshResources())

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
  return n === null ? '—' : `${Math.round(n)} ms`
}

const formatBucketSize = (minutes) => {
  const n = toNum(minutes)
  if (!n) return '1-minute'
  if (n < 60) return `${n}-minute`
  const hours = Math.floor(n / 60)
  const rest = n % 60
  return rest === 0 ? `${hours}-hour` : `${hours}h ${rest}m`
}
const bucketTitle = computed(() => (systemData.value ? `Each point is a ${formatBucketSize(systemData.value.bucketMinutes)} bucket` : undefined))

// ---------------------------------------------------------------------------
// The list, the header and the tabs: the members read above, through
// brokerStatus (the Overview's and the top bar's model of the same payload).
// ---------------------------------------------------------------------------
const cell = computed(() => brokerStatus(raftMembers.data.value))
const sideNodes = computed(() => nodeEntries(cell.value))
const raftMembersAbsent = computed(() => isMissingRoute(raftMembers.error.value))
const raftStatusAbsent = computed(() => isMissingRoute(raftStatus.error.value))
const nodesState = computed(() => {
  if (raftMembersAbsent.value) return 'absent'
  if (sideNodes.value.length) return 'ok'
  if (raftMembers.data.value) return 'empty'
  return raftMembers.failed.value ? 'failed' : 'loading'
})
const nodesError = computed(() => (raftMembers.failed.value ? describeRaftFailure(raftMembers.error.value) : ''))

// The members table draws the same payload's rows in the same order (node id,
// clusterView), so the picked node's row is the one at its index.
const focusIndex = computed(() => sideNodes.value.find((n) => n.name === focusNode.value)?.index ?? -1)
const focusRow = computed(() => (focusIndex.value < 0 ? null : clusterView(raftMembers.data.value)?.rows[focusIndex.value] || null))

const facts = computed(() => {
  if (focusNode.value) {
    const entry = sideNodes.value[focusIndex.value]
    const row = focusRow.value
    if (!entry) return [{ v: 'Not in the members', title: 'The last members read does not hold this node' }]
    if (!row || row.unreachable) return [{ k: 'Role', v: entry.role }]
    const addresses = [row.httpAddr ? `http ${row.httpAddr}` : null, row.raftAddr ? `raft ${row.raftAddr}` : null].filter(Boolean).join(' · ')
    return [
      { k: 'Role', v: entry.role, title: addresses || undefined },
      row.version ? { k: 'Version', v: row.version } : null,
      row.uptimeSeconds !== null ? { k: 'Up', v: formatDuration(row.uptimeSeconds * 1000) } : null,
    ].filter(Boolean)
  }
  const c = cell.value
  return [
    {
      k: 'Cell',
      v: actingCellSlug.value || 'unknown',
      title: `Host resources and the replicated log of this cell, shared by every tenant on it, not scoped to ${actingTenantSlug.value || 'your tenant'}`,
    },
    c ? { k: 'Nodes', v: String(c.nodes.length) } : null,
    c && !c.raft.single ? { k: 'Leader', v: c.raft.leader || 'none', title: c.raft.term !== null ? `Term ${c.raft.term}` : undefined } : null,
    versionFact.value,
  ].filter(Boolean)
})

// The broker's version, as every node reports it in the members read. One
// version is one chip; several (a rolling upgrade, or one left behind) are
// listed, and the tooltip says which node runs which.
const versionFact = computed(() => {
  const rows = clusterView(raftMembers.data.value)?.rows || []
  const names = sideNodes.value
  const byNode = rows
    .map((r, i) => ({ name: names[i]?.name || r.hostname || `node ${r.nodeId}`, version: r.version }))
    .filter((x) => x.version)
  if (!byNode.length) return null
  const versions = [...new Set(byNode.map((x) => x.version))].sort()
  return {
    k: versions.length === 1 ? 'Version' : 'Versions',
    v: versions.join(' · '),
    title: versions.length === 1
      ? `Every node runs Queen ${versions[0]}`
      : `Nodes run different versions: ${byNode.map((x) => `${x.name} ${x.version}`).join(', ')}`,
  }
})

// ---------------------------------------------------------------------------
// The list, the status card and the tables of the Overview, Nodes and
// Storage: the members read, through brokerStatus and clusterView.
// ---------------------------------------------------------------------------
const nodeOptions = computed(() => [
  { value: '', label: 'All nodes' },
  ...sideNodes.value.map((n) => ({ value: n.name, label: n.name })),
])

// Each cell issue opens the view where it is worked on.
const issueList = computed(() => cellIssues(cell.value).map((i) => ({ ...i, view: cellIssueView(i) })))
// Each opens the view it is worked on in (CellStatus draws them in the status card).
const issueLinks = computed(() => issueList.value.map((i) => ({ ...i, to: i.view ? viewTo(i.view) : i.to })))
const verdict = computed(() => cellVerdict(cell.value, issueList.value))
const toneOf = (sev) => (sev === 'warn' || sev === 'bad' ? sev : '')
// A share as a person reads it: a sliver that is not zero says so ("<1%").
const shareText = (top) => {
  const v = toNum(top?.share)
  if (v === null) return '—'
  return v > 0 && v < 0.005 ? '<1%' : `${Math.round(v * 100)}%`
}
// The nodes' network now: what each received and sent, summed over the cell.
const networkNow = computed(() => {
  const nodes = cell.value?.nodes || []
  let sum = null
  for (const n of nodes) {
    const net = n.traffic?.network
    if (!net) continue
    sum = (sum ?? 0) + (toNum(net.received) || 0) + (toNum(net.sent) || 0)
  }
  return sum
})
const nodesNote = computed(() => {
  if (raftMembersAbsent.value) return 'This broker does not report its members'
  if (raftMembers.failed.value) return `Cannot read the members · ${nodesError.value}`
  return 'Reading the members…'
})
// Who each member is, and its log and store: clusterView's rows, by node id.
const memberRows = computed(() => {
  const rows = clusterView(raftMembers.data.value)?.rows || []
  const names = sideNodes.value
  return rows.map((r, i) => ({
    name: names[i]?.name || r.hostname || `node ${r.nodeId}`,
    role: names[i]?.role || (r.unreachable ? 'Unreachable' : capital(r.state || '—')),
    roleTone: r.unreachable ? 'bad' : '',
    version: r.version || '—',
    up: r.uptimeSeconds === null || r.uptimeSeconds === undefined ? '—' : formatDuration(r.uptimeSeconds * 1000),
    http: r.httpAddr || '—',
    raftAddr: r.raftAddr || '—',
    log: bytes(r.logBytes),
    files: metric(r.logFiles),
    map: formatMapPct(r.mapPct),
    mapTone: numTone(r.mapSeverity),
    mapUsed: r.mapUsed === null || r.mapUsed === undefined ? '—' : `${bytes(r.mapUsed)} of ${bytes(r.mapBytes)}`,
  }))
})
// How full each data disk is, for the Disk view's bars.
const diskRows = computed(() => (cell.value?.nodes || [])
  .filter((n) => !focusNode.value || n.name === focusNode.value)
  .map((n) => {
  const pctUsed = toNum(n.disk?.usedPct)
  return {
    name: n.name,
    width: pctUsed === null ? 0 : Math.max(1, Math.min(100, pctUsed)),
    text: pctUsed === null ? '—' : `${Math.round(pctUsed)}%`,
    tone: toneOf(n.disk?.sev),
  }
}))

// The list: every view's one number, coloured only where it is the trouble.
const side = computed(() => {
  const c = cell.value
  const resourceTone = toneOf(c?.cpu.sev) === 'bad' || toneOf(c?.mem.sev) === 'bad' ? 'bad'
    : toneOf(c?.cpu.sev) || toneOf(c?.mem.sev)
  return systemSideGroups({
    overview: c ? { num: verdict.value.word, tone: verdict.value.tone, title: verdict.value.headline } : {},
    raft: c ? {
      num: c.raft.alert ? c.raft.alert.title : c.raft.lagMax ? `${formatNumber(c.raft.lagMax)} behind` : 'In sync',
      tone: toneOf(c.raft.sev),
      title: `Leader ${c.raft.leader || 'none'} · term ${c.raft.term ?? '—'}`,
    } : {},
    storage: memberRows.value.length ? {
      num: bytes(Math.max(...(clusterView(raftMembers.data.value)?.rows || []).map((r) => toNum(r.logBytes) || 0))),
      tone: toneOf(worstSev((clusterView(raftMembers.data.value)?.rows || []).map((r) => r.mapSeverity))),
      title: 'The largest log on a node',
    } : {},
    compute: c ? { num: `CPU ${shareText(c.cpu.top)}`, tone: resourceTone, title: `Memory ${shareText(c.mem.top)} at most` } : {},
    disk: c ? { num: shareText(c.disk.top), tone: toneOf(c.disk.sev), title: c.disk.top ? `The fullest disk: ${c.disk.top.name}` : '' } : {},
    network: networkNow.value === null ? {} : { num: formatRate(networkNow.value), title: 'In and out of every node, summed' },
  })
})
const worstSev = (sevs) => (sevs.includes('bad') ? 'bad' : sevs.includes('warn') ? 'warn' : '')

// ---------------------------------------------------------------------------
// Replicated log — the node that answered /api/v1/raft/status
// ---------------------------------------------------------------------------
const node = computed(() => nodeView(raftStatus.data.value))
const raftAlarm = computed(() => (node.value?.severity || '') !== '')
const roleMark = computed(() => ({ 'chip-warn': 'warn', 'chip-bad': 'bad' }[node.value?.chip?.cls] || ''))

// ---------------------------------------------------------------------------
// Workers (bare /api/v1/status — workers seen in the last two minutes)
// ---------------------------------------------------------------------------
const workers = computed(() => {
  const all = status.data.value?.workers || []
  return focusNode.value ? all.filter((w) => w.hostname === focusNode.value) : all
})
const lifetimeAckFailed = computed(() => toNum(status.data.value?.errors?.ackFailed))

// The workers' event loop over the range: the same read's `throughput`, one
// row a bucket, newest first. Every worker on the cell, whatever node is
// picked: the broker keeps it for the cell, not per node.
const statusHistory = computed(() => {
  const s = status.data.value
  if (!s?.throughput?.length) return []
  return trimIncompleteBuckets([...s.throughput].reverse(), { bucketKey: 'timestamp', bucketMinutes: s.bucketMinutes || 1 })
})
const eventLoopLabels = computed(() => {
  const ts = statusHistory.value.map((x) => x.timestamp)
  const multi = isMultiDay(ts)
  return ts.map((t) => formatChartLabel(new Date(t), multi))
})
const eventLoopSeries = computed(() => {
  const h = statusHistory.value
  if (!h.length) return null
  return [
    { label: 'Average', data: h.map((x) => toNum(x.avgEventLoopLagMs)) },
    { label: 'Peak', data: h.map((x) => toNum(x.maxEventLoopLagMs)) },
  ]
})
const fmtMs = (v) => `${Math.round(v)} ms`

// Rows per batch across every tenant on the cell: the more rows a batch
// carries, the less each pays for its commit.
const batchEfficiency = computed(() => {
  const b = status.data.value?.messages?.batchEfficiency
  if (!b) return null
  const one = (v) => {
    const n = toNum(v)
    return n === null ? '—' : n.toFixed(n < 10 ? 2 : 1)
  }
  return { push: one(b.push), pop: one(b.pop), ack: one(b.ack) }
})

// The state says what the payload says, not what we hope. Only the event-loop
// gauges are actually written by this broker (min_free_slots / db_connections
// / max_job_queue_size never are), so nothing else is rendered here. A mark
// only when it needs you.
const workerChip = (w) => {
  const peak = toNum(w.maxEventLoopLagMs)
  const avg = toNum(w.avgEventLoopLagMs)
  if (peak === null && avg === null) return { mark: '', label: 'No lag data' }
  if ((peak ?? 0) > 500 || (avg ?? 0) > 200) return { mark: 'bad', label: 'Stalling' }
  if ((peak ?? 0) > 100 || (avg ?? 0) > 50) return { mark: 'warn', label: 'Busy' }
  return { mark: '', label: 'Responsive' }
}

// ---------------------------------------------------------------------------
// Replica time series
//
// Replicas do not share a bucket grid: one may start later, restart, or miss a
// collector tick. Labels therefore come from the UNION of every replica's
// timestamps and each series is indexed BY timestamp, so a shorter series is a
// gap in the line rather than a silent shift onto another replica's clock.
// ---------------------------------------------------------------------------
// The picked node's replicas only, or every one: what the charts, the tiles,
// the servers table and the workers are drawn from.
const replicas = computed(() => nodeReplicas(systemData.value?.replicas, focusNode.value || null))
const replicaCountLabel = computed(() => {
  const n = replicas.value.length
  return `${n} replica${n === 1 ? '' : 's'}`
})
const acrossLabel = computed(() =>
  replicas.value.length > 1 ? `summed across ${replicaCountLabel.value}` : focusNode.value || 'this replica',
)
const viewTitle = computed(() => (viewMode.value === 'aggregate'
  ? `Summed across ${replicaCountLabel.value}; a bucket where a replica sent no sample sums only those that did`
  : 'One line per server; a gap is a bucket that server never reported'))

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
  return timeline.value.map((ts) => formatChartLabel(new Date(ts), multiDay))
})

const pointsByTimestamp = (replica) => {
  const map = new Map()
  for (const point of replica.timeSeries || []) {
    if (point?.timestamp) map.set(point.timestamp, point)
  }
  return map
}

const leaf = (metricsObj, path) => {
  let at = metricsObj
  for (const key of path) {
    at = at?.[key]
    if (at === undefined || at === null) return null
  }
  return at
}

/** One replica's values aligned to the union timeline; null where it has none. */
const replicaSeries = (replica, path, scale = (v) => v) => {
  const byTs = pointsByTimestamp(replica)
  return timeline.value.map((ts) => {
    const v = toNum(leaf(byTs.get(ts)?.metrics, path)?.[aggregationType.value])
    return v === null ? null : scale(v)
  })
}

/** Sum across replicas per bucket; null only when no replica reported it. */
const summedSeries = (path, scale = (v) => v) => {
  const perReplica = replicas.value.map((r) => replicaSeries(r, path, scale))
  return timeline.value.map((_, i) => {
    let sum = null
    for (const series of perReplica) {
      const v = series[i]
      if (v !== null) sum = (sum || 0) + v
    }
    return sum
  })
}

const CPU_SCALE = (v) => v / 100
const MB_SCALE = (v) => Math.round(v / 1024 / 1024)

const buildChart = (specs, { perReplicaPath = null, perReplicaScale = (v) => v, perReplicaSuffix = '' } = {}) => {
  if (!replicas.value.length || !timeline.value.length) return { labels: [], datasets: [] }
  if (viewMode.value === 'individual' && perReplicaPath) {
    return {
      labels: chartLabels.value,
      datasets: replicas.value.map((replica, i) => ({
        label: `${replica.hostname}${perReplicaSuffix}`,
        data: replicaSeries(replica, perReplicaPath, perReplicaScale),
        borderColor: lineColor(i).line,
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
      borderColor: lineColor(i).line,
      backgroundColor: lineColor(i).fill,
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
// What the nodes move, in bytes per second (useTraffic.js). Each chart takes
// the unit its largest value reads best in, and names it in every label.
// ---------------------------------------------------------------------------
const largest = (datasets) => {
  let max = 0
  for (const d of datasets) for (const v of d.data) if (v !== null && v > max) max = v
  return max
}

/**
 * Aggregate: one filled line per spec, summed across replicas. Per server:
 * every spec of every replica in the replica's colour, the second dashed.
 */
const buildRateChart = (specs) => {
  const empty = { labels: [], datasets: [], unit: rateUnit(0), hasData: false }
  if (!replicas.value.length || !timeline.value.length) return empty
  const raw = viewMode.value === 'individual'
    ? replicas.value.flatMap((replica, i) => specs.map((spec, j) => ({
      label: `${replica.hostname} · ${spec.short}`,
      data: replicaSeries(replica, spec.path),
      borderColor: lineColor(i).line,
      borderDash: j === 0 ? [] : [5, 4],
      fill: false,
    })))
    : specs.map((spec, i) => ({
      label: spec.label,
      data: summedSeries(spec.path),
      borderColor: lineColor(spec.color ?? i).line,
      backgroundColor: lineColor(spec.color ?? i).fill,
      fill: true,
    }))
  if (!raw.some((d) => d.data.some((v) => v !== null))) return empty
  const unit = rateUnit(largest(raw))
  return {
    labels: chartLabels.value,
    unit,
    hasData: true,
    datasets: raw.map((d) => ({
      ...d,
      label: `${d.label} (${unit.label})`,
      tension: 0,
      data: d.data.map((v) => inUnit(v, unit)),
    })),
  }
}

const networkChart = computed(() => buildRateChart([
  { label: 'Received', short: 'in', path: ['network', 'received_bytes_per_sec'] },
  { label: 'Sent', short: 'out', path: ['network', 'sent_bytes_per_sec'] },
]))

const diskChart = computed(() => buildRateChart([
  { label: 'Read', short: 'read', path: ['disk', 'read_bytes_per_sec'] },
  { label: 'Written', short: 'write', path: ['disk', 'written_bytes_per_sec'] },
]))

// Aggregate: what crossed the wire, by channel. A byte is sent by one node and
// received by another, so the nodes' SENT bytes, summed, count it once. A
// channel that carried nothing in the range stays off the chart; replication
// always shows. Per server: each node's total out and in.
const raftTrafficChart = computed(() => {
  if (viewMode.value === 'individual') {
    return buildRateChart([
      { label: 'Sent', short: 'out', path: ['raft_traffic', 'sent_bytes_per_sec'] },
      { label: 'Received', short: 'in', path: ['raft_traffic', 'received_bytes_per_sec'] },
    ])
  }
  return buildRateChart(RAFT_CHANNELS
    .map((c, i) => ({ key: c.key, label: c.label, color: i, path: ['raft_traffic', c.key, 'sent_bytes_per_sec'] }))
    .filter((s) => s.key === 'replication' || summedSeries(s.path).some((v) => v !== null && v > 0)))
})

const raftTrafficHelp = [
  'Bytes over the Raft RPC port, by what they carry:',
  ...RAFT_CHANNELS.map((c) => `${c.label}: ${c.note}.`),
  'Aggregate sums what each node sent: every byte on the wire once.',
].join('\n')

// A new options object only when a chart's unit changes: BaseChart redraws
// the whole chart for new options, and only re-plots for new data.
const rateOptions = (unitLabel) => ({
  plugins: { legend: { display: true, position: 'top' } },
  scales: { y: { title: { display: true, text: unitLabel, font: { size: 11 } } } },
})
const networkUnit = computed(() => networkChart.value.unit.label)
const diskUnit = computed(() => diskChart.value.unit.label)
const raftTrafficUnit = computed(() => raftTrafficChart.value.unit.label)
const networkOptions = computed(() => rateOptions(networkUnit.value))
const diskOptions = computed(() => rateOptions(diskUnit.value))
const raftTrafficOptions = computed(() => rateOptions(raftTrafficUnit.value))

// ---------------------------------------------------------------------------
// Latest sample. Reduced ACROSS replicas — a card that printed replicas[0]
// beside a replica count would read as a cell figure and not be one.
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
  netIn: sumLatest(['network', 'received_bytes_per_sec']),
  netOut: sumLatest(['network', 'sent_bytes_per_sec']),
  diskRead: sumLatest(['disk', 'read_bytes_per_sec']),
  diskWrite: sumLatest(['disk', 'written_bytes_per_sec']),
  // Every node's sent bytes: what crossed the wire, once.
  raftWire: sumLatest(['raft_traffic', 'sent_bytes_per_sec']),
}))

const tiles = computed(() => {
  const l = latest.value
  const across = acrossLabel.value
  const unreported = 'Not reported: a broker records it on Linux only, and older releases not at all'
  return [
    { k: 'CPU', v: pct(l.cpuUser), title: `User CPU in the last sample, ${across}` },
    { k: 'Memory', v: mb(l.rss), title: `Resident memory in the last sample, ${across}` },
    { k: 'Network in', v: formatRate(l.netIn), title: l.netIn === null ? unreported : `Received in the last sample, ${across}` },
    { k: 'Network out', v: formatRate(l.netOut), title: l.netOut === null ? unreported : `Sent in the last sample, ${across}` },
    { k: 'Disk write', v: formatRate(l.diskWrite), title: l.diskWrite === null ? 'Not reported by this release' : `Written in the last sample, ${across}; ${formatRate(l.diskRead)} read` },
    {
      k: 'Raft port',
      v: formatRate(l.raftWire),
      title: l.raftWire === null ? 'Not reported by this release'
        : focusNode.value ? `What ${focusNode.value} sent on the Raft port in the last sample`
          : 'On the Raft port’s wire in the last sample: every node’s sent bytes, summed',
    },
  ]
})

// ---------------------------------------------------------------------------
// One line per node, over the range: the Overview's four charts, Storage's two
// and the Disk view's use. The Overview is not narrowed by a node picked on
// another view (it has no picker): its lines are every node's.
// ---------------------------------------------------------------------------
const allReplicas = computed(() => nodeReplicas(systemData.value?.replicas, null))
const axisOf = (list) => {
  const seen = new Set()
  for (const r of list) for (const p of r.timeSeries || []) if (p?.timestamp) seen.add(p.timestamp)
  return [...seen].sort()
}
const valuesOn = (replica, axis, value) => {
  const byTs = pointsByTimestamp(replica)
  return axis.map((ts) => value(byTs.get(ts)?.metrics))
}
const at = (metricsObj, path) => toNum(leaf(metricsObj, path)?.[aggregationType.value])
/** `value(metrics)` → a number or null, one line per node of `list`; no labels when nothing was recorded. */
const perNodeChart = (list, value) => {
  const axis = axisOf(list)
  if (!list.length || !axis.length) return { labels: [], datasets: [] }
  const datasets = list.map((r, i) => ({
    label: r.hostname,
    data: valuesOn(r, axis, value),
    borderColor: lineColor(i).line,
    fill: false,
    tension: 0,
  }))
  if (!datasets.some((d) => d.data.some((v) => v !== null))) return { labels: [], datasets: [] }
  const multiDay = isMultiDay(axis)
  return { labels: axis.map((ts) => formatChartLabel(new Date(ts), multiDay)), datasets }
}
// Bytes in the unit the largest value reads best in: MB under a gigabyte.
const bytesChart = (chart) => {
  const max = largest(chart.datasets)
  const unit = max >= 1e9 ? { div: 1e9, label: 'GB' } : { div: 1e6, label: 'MB' }
  return {
    unit,
    labels: chart.labels,
    datasets: chart.datasets.map((d) => ({ ...d, data: d.data.map((v) => (v === null ? null : Math.round((v / unit.div) * 100) / 100)) })),
  }
}
const lineOptions = (title, tick = (v) => v) => ({
  plugins: { legend: { display: true, position: 'top' } },
  scales: { y: { title: { display: true, text: title, font: { size: 11 } }, ticks: { callback: tick } } },
})
const pctOptions = lineOptions('% used', (v) => `${Number(v) < 10 ? Number(v).toFixed(1) : Number(v).toFixed(0)}%`)
const usedPct = (m) => {
  const used = toNum(leaf(m, ['disk', 'used_bytes'])?.last)
  const total = toNum(leaf(m, ['disk', 'total_bytes'])?.last)
  return used === null || !total ? null : Math.round((used / total) * 1000) / 10
}
const diskUsedChart = computed(() => perNodeChart(replicas.value, usedPct))
const logChart = computed(() => bytesChart(perNodeChart(allReplicas.value, (m) => at(m, ['raft', 'log_bytes']))))
// `map_used_pct` is a percentage already (0.99 = 0.99 %), as the table shows it.
const mapChart = computed(() => perNodeChart(allReplicas.value, (m) => {
  const v = at(m, ['raft', 'map_used_pct'])
  return v === null ? null : Math.round(v * 100) / 100
}))

const overviewCharts = computed(() => {
  const all = allReplicas.value
  const diskUsed = perNodeChart(all, usedPct)
  const log = logChart.value
  // The disk's use when the broker records it; before 2.7.0-beta.2, the log,
  // which is most of what fills it.
  const disk = diskUsed.labels.length
    ? { key: 'disk', view: 'disk', title: 'Disk used', sub: 'of the data disk, per node', chart: diskUsed, options: pctOptions, none: 'No disk samples in this range' }
    : { key: 'log', view: 'storage', title: 'Raft log', sub: 'size on disk, per node', chart: log, options: lineOptions(log.unit?.label || 'MB'), none: 'No log samples in this range' }
  return [
    { key: 'cpu', view: 'compute', title: 'CPU', sub: 'user time per node, % of one core', chart: perNodeChart(all, (m) => { const v = at(m, ['cpu', 'user_us']); return v === null ? null : CPU_SCALE(v) }), options: cpuOptions, none: 'No CPU samples in this range' },
    { key: 'memory', view: 'compute', title: 'Memory', sub: 'resident, per node', chart: perNodeChart(all, (m) => { const v = at(m, ['memory', 'rss_bytes']); return v === null ? null : MB_SCALE(v) }), options: memoryOptions, none: 'No memory samples in this range' },
    disk,
    { key: 'network', view: 'network', title: 'Network', sub: 'received and sent, per node', chart: bytesPerSecChart(all), options: networkPerNodeOptions.value, none: 'Not reported: a broker records the network on Linux only' },
  ]
})
// The network per node: what it received and sent, summed, in the unit the
// largest value reads best in.
const networkPerNode = computed(() => {
  const chart = perNodeChart(allReplicas.value, (m) => {
    const rx = at(m, ['network', 'received_bytes_per_sec'])
    const tx = at(m, ['network', 'sent_bytes_per_sec'])
    return rx === null && tx === null ? null : (rx || 0) + (tx || 0)
  })
  const unit = rateUnit(largest(chart.datasets))
  return { unit, labels: chart.labels, datasets: chart.datasets.map((d) => ({ ...d, data: d.data.map((v) => inUnit(v, unit)) })) }
})
const bytesPerSecChart = () => networkPerNode.value
const networkPerNodeOptions = computed(() => lineOptions(networkPerNode.value.unit?.label || 'B/s'))

const storageCharts = computed(() => [
  { key: 'log', title: 'Raft log', sub: 'size on disk, per node', chart: logChart.value, options: lineOptions(logChart.value.unit?.label || 'MB'), none: 'No log samples in this range' },
  { key: 'map', title: 'Store', sub: 'how full its map is, per node', chart: mapChart.value, options: pctOptions, none: 'No store samples in this range' },
])

// The range choices a view without its own tools offers: the presets, not a
// custom window (its form lives on the chart views).
const shortRangeOptions = computed(() => rangeOptions.filter((o) => o.value !== 'custom'))

// Chart options
const cpuOptions = {
  plugins: { legend: { display: true, position: 'top' } },
  scales: {
    y: {
      title: { display: true, text: 'CPU %', font: { size: 11 } },
      ticks: { callback: (value) => `${Number(value).toFixed(1)}%` },
    },
  },
}
const memoryOptions = {
  plugins: { legend: { display: true, position: 'top' } },
  scales: { y: { title: { display: true, text: 'Memory (MB)', font: { size: 11 } } } },
}

const capital = (label) => (label ? label.charAt(0).toUpperCase() + label.slice(1) : label)
</script>

<style scoped>
/* The list: the number is the mark, coral when failing, amber when it needs
   you; the Overview's word is a word, not a number. */
.sys-side { padding-top: 12px; }
.sys-side .ss-num.bad { color: var(--ember-400); }
.sys-side .ss-num.warn { color: var(--warn-400); }
.ss-num.sys-side-word { font-family: inherit; font-size: 12px; }

.sys-mono { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); }
/* Over the charts of the Overview and Storage: what they show, and the range. */
.sys-ov-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-top: 4px; }
.sys-ov-label { font-size: 12px; font-weight: 600; color: var(--text-mid); }
/* A chart's title is the way to its view. */
.sys-chart-title { display: inline-flex; align-items: center; gap: 2px; font-size: 13px; font-weight: 600; color: var(--text-hi); text-decoration: none; }
.sys-chart-title :deep(svg) { color: var(--text-low); transition: transform .12s, color .12s; }
.sys-chart-title:hover :deep(svg) { color: var(--text-hi); transform: translateX(2px); }
/* The cell's rows per batch, a line under the workers' header. */
.sys-batch {
  display: flex; align-items: baseline; flex-wrap: wrap; gap: 4px 18px;
  padding: 10px 16px; border-bottom: 1px solid var(--bd-soft);
  font-size: 12.5px; color: var(--text-mid);
}
.sys-batch b { color: var(--text-hi); font-weight: 600; font-variant-numeric: tabular-nums; }
.sys-batch-k { color: var(--text-low); }

/* How full each data disk is: one bar per node, the number after it. */
.sys-bars { display: flex; flex-direction: column; gap: 10px; padding: 14px 16px 16px; }
.sys-bar { display: grid; grid-template-columns: 110px minmax(0, 1fr) 48px; align-items: center; gap: 12px; font-size: 12.5px; }
.sys-bar-name { color: var(--text-mid); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.sys-bar-track { height: 6px; border-radius: 3px; background: var(--ink-4); overflow: hidden; }
.sys-bar-fill { display: block; height: 100%; border-radius: 3px; background: var(--text-low); }
.sys-bar-fill.warn { background: var(--warn-400); }
.sys-bar-fill.bad { background: var(--ember-400); }
.sys-bar-v { text-align: right; font-variant-numeric: tabular-nums; color: var(--text-hi); }
.sys-bar-v.warn { color: var(--warn-400); }
.sys-bar-v.bad { color: var(--ember-400); }

/* The tab's blocks, a column apart at the 16px rhythm. */
.sys-stack { display: flex; flex-direction: column; gap: 16px; }
/* The panel pair. Not a stat grid: it lays out CARDS. */
.sys-grid-2 { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 16px; }
.sys-range { display: inline-flex; }

/* One step louder than a hairline on purpose — the replicated log is
   actually in trouble here. */
.sys-alarm { border-color: color-mix(in srgb, var(--warn-400) 35%, transparent); }

.sys-quiet { padding: 14px 16px; font-size: 13px; color: var(--text-low); }
.sys-table td { white-space: nowrap; }
.sys-host { font-weight: 500; color: var(--text-hi); }
.sys-port { color: var(--text-low); }
/* "12.4 MB/s / 3.1 MB/s": one cell, two figures, the slash quiet. */
.sys-sep { color: var(--text-low); }
.sys-state { display: inline-flex; align-items: center; gap: 8px; color: var(--text-hi); }
/* The mark's slot, kept when empty so the states line up (the lists' rule). */
.sys-mark { flex: none; width: 10px; display: inline-flex; justify-content: center; }

@media (max-width: 1100px) {
  .sys-grid-2 { grid-template-columns: minmax(0, 1fr); }
}
@media (max-width: 640px) {
  .sys-tiles .count-tile .v { font-size: 17px; }
}
</style>
