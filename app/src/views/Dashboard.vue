<template>
  <div class="view-container">

    <!--
      ========================================================================
      Top bar — range, autorefresh, drilldown chip, last-refresh ticker.
      One slim row, no legend (per-row sparklines are self-evident).
      ========================================================================
    -->
    <div class="dash-bar">
      <div class="seg">
        <button
          v-for="r in timeRanges"
          :key="r.value"
          :class="{ on: selectedRange === r.value }"
          @click="selectedRange = r.value"
        >{{ r.label }}</button>
      </div>

      <div class="dash-live">
        <span class="pulse" />
        <span>live · 30s autorefresh</span>
      </div>

      <div class="dash-bar-right">
        <button
          class="dash-master-toggle"
          @click="toggleAllRows"
          :title="anyExpanded ? 'Collapse every metric chart' : 'Expand every metric into a full chart'"
        >
          <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
            <!-- Two stacked rows + chevrons. Up-pointing chevrons when
                 anything is expanded (so click "lifts up" / collapses);
                 down-pointing when collapsed (click "drops down" / expands). -->
            <path :d="anyExpanded ? 'M3 7l5-3 5 3M3 11l5-3 5 3' : 'M3 5l5 3 5-3M3 9l5 3 5-3'" />
          </svg>
          <span>{{ anyExpanded ? 'Collapse all' : 'Expand all' }}</span>
        </button>
        <span class="dash-refresh-tick">last refresh {{ refreshAgo }}</span>
      </div>
    </div>

    <!--
      ========================================================================
      Counts strip — cluster scope counts on the left, point-in-time perf
      stats on the right. Both are snapshot values (not time series), so
      they don't belong in the metric table below; this strip is the
      designated home for "what's happening right now, no chart needed".
      The pending count is the only number with a threshold tone.
      ========================================================================
    -->
    <div class="counts-strip">
      <div class="counts-group">
        <span class="count-item count-static" title="Messages currently stored for this tenant (retention has already swept the rest)">
          <strong>{{ overviewFailed ? '—' : formatNumber(overview?.messages?.total ?? 0) }}</strong>
          <span>stored</span>
        </span>
        <span class="count-sep">·</span>
        <button class="count-item" @click="$router.push('/queues')" :disabled="loadingQueues">
          <strong>{{ overviewFailed ? '—' : formatNumber(overview?.queues ?? 0) }}</strong>
          <span>queues</span>
        </button>
        <span class="count-sep">·</span>
        <span class="count-item count-static">
          <strong>{{ queuesFailed ? '—' : formatNumber(totalPartitions) }}</strong>
          <span>partitions</span>
        </span>
        <span class="count-sep">·</span>
        <button class="count-item" @click="$router.push('/consumers')" :disabled="loadingConsumers">
          <strong>{{ consumersFailed ? '—' : formatNumber(consumers?.length || 0) }}</strong>
          <span>consumer groups</span>
        </button>
        <span class="count-sep">·</span>
        <span class="count-item count-static">
          <strong class="num" :class="pendingNumClass(overview?.messages?.pending)">
            {{ overviewFailed ? '—' : formatNumber(overview?.messages?.pending ?? 0) }}
          </strong>
          <span>pending</span>
        </span>
        <span class="count-sep">·</span>
        <span class="count-item count-static count-muted">
          <strong>{{ overviewFailed ? '—' : formatNumber(overview?.messages?.completed ?? 0) }}</strong>
          <span>completed</span>
        </span>
      </div>

      <!-- Right group — engine efficiency for the whole CELL, not this tenant:
           it comes from the operator-only /api/v1/status, so it is hidden
           entirely for anyone the proxy would answer 404. -->
      <div
        v-if="can('operator')"
        class="counts-group counts-group-right"
        :title="'Average rows per batch — push / pop / ack, across every tenant on this cell. Higher = healthier engine, less per-commit overhead.'"
      >
        <span class="count-item-label">batch eff <i class="count-scope">cell</i></span>
        <template v-if="statusFailed">
          <span class="count-item count-static count-tight count-muted">
            <strong>—</strong><span class="count-suffix">unavailable</span>
          </span>
        </template>
        <template v-else>
          <span class="count-item count-static count-tight">
            <strong>{{ batchEfficiency.push }}</strong>
            <span class="count-suffix">push</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static count-tight">
            <strong>{{ batchEfficiency.pop }}</strong>
            <span class="count-suffix">pop</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static count-tight">
            <strong>{{ batchEfficiency.ack }}</strong>
            <span class="count-suffix">ack</span>
          </span>
        </template>
      </div>
    </div>

    <!--
      ========================================================================
      Metric table — the heart of the redesign. Nine rows, each carries:
        dot · label · value+unit · context · sparkline.
      Color only fires when a row crosses its threshold; the eye scans the
      dot column and stops on the first non-grey one. Order is by operator
      priority (flow → health → admin), not by code structure.
      ========================================================================
    -->
    <div class="metric-table">
      <div class="metric-head">
        <span></span>
        <span>Metric</span>
        <span class="h-value">Now</span>
        <span>Context</span>
        <span class="h-spark">{{ selectedRange }}</span>
        <span></span>
      </div>

      <!-- Flow -->
      <MetricRow
        label="Throughput"
        :value="throughput.current"
        :unit="throughput.current === '—' ? '' : '/s'"
        :context="throughputContext"
        :series="throughputSeries"
        :labels="chartLabels"
        :value-format="fmtRate"
        expand-unit="msgs / sec"
        :loading="loadingOps"
        :error="opsError"
        tooltip="Push / pop / ack rates for this tenant, summed across its queues (from queue-ops)."
        :expanded="isExpanded('throughput')"
        @toggle-expand="toggleRow('throughput')"
      />
      <MetricRow
        label="Pending Δ"
        :value="pendingDeltaDisplay"
        :unit="pendingDeltaDisplay === '—' ? '' : 'msgs'"
        :context="pendingDeltaContext"
        :sparkline="pendingDeltaSeries"
        :labels="chartLabels"
        :value-format="fmtCount"
        expand-unit="msgs (cumulative)"
        :severity="pendingDeltaSeverity"
        :loading="loadingOps"
        :error="opsError"
        tooltip="Cumulative (push − ack) over the selected window. Positive = falling behind, negative = catching up."
        :expanded="isExpanded('pendingDelta')"
        @toggle-expand="toggleRow('pendingDelta')"
      />
      <!-- Parked + Fill ratio sit between Pending Δ and Time lag because
           together they answer the consumer-side of the flow story:
           who's waiting (Parked) and how often they're actually fed
           (Fill). Time lag below tells you how *late* messages are
           when they finally do get delivered. -->
      <MetricRow
        label="Parked"
        :value="parkedLatest === null ? '—' : formatNumber(Math.round(parkedLatest))"
        :unit="parkedLatest === null ? '' : 'consumers'"
        :context="parkedContext"
        :series="parkedSeriesData"
        :labels="chartLabels"
        :value-format="fmtCount"
        expand-unit="long-polls"
        :loading="loadingOps"
        :error="opsError"
        tooltip="Long-poll consumer connections currently waiting for work, summed across all queues. Approximates idle connected consumers (busy consumers, mid-job, not counted)."
        :expanded="isExpanded('parked')"
        @toggle-expand="toggleRow('parked')"
      />
      <MetricRow
        label="Fill ratio"
        :context="fillContext"
        :series="fillSeriesData"
        :labels="chartLabels"
        :value-format="fmtFillPct"
        expand-unit="%"
        :severity="fillSeverity"
        :loading="loadingOps"
        :error="opsError"
        tooltip="Long-polls returning a message ÷ all long-poll completions, across all queues. Below 30% with traffic = consumers mostly waiting (over-provisioned); near 100% sustained = consumers fully utilized — watch the Time lag row for under-provisioning."
        :expanded="isExpanded('fillRatio')"
        @toggle-expand="toggleRow('fillRatio')"
      >
        <template #value>
          <template v-if="fillLatest === null">
            <span class="num">—</span>
          </template>
          <template v-else>
            <span class="num" :class="fillSeverity">{{ fillLatest.toFixed(1) }}</span><i class="mr-unit">%</i>
          </template>
        </template>
      </MetricRow>
      <MetricRow
        label="Time lag"
        :context="lagContext"
        :series="lagSeriesData"
        :labels="chartLabels"
        :value-format="fmtLagMs"
        expand-unit="ms"
        :severity="lagSeverity"
        :loading="loadingOverview"
        :error="overviewError"
        tooltip="Headline is the tenant's current oldest-message age (avg / max). The chart is pop-sampled per bucket, so it has gaps whenever nothing was consumed — it cannot report the lag of a stalled queue."
        :expanded="isExpanded('timeLag')"
        @toggle-expand="toggleRow('timeLag')"
      >
        <template #value>
          <!-- `0` and "no sample" are different answers. The SP emits NULL when
               it never measured, and that must not read as a healthy zero. -->
          <template v-if="lagAvgSeconds === null && lagMaxSeconds === null">
            <span class="num">—</span>
          </template>
          <template v-else>
            <span class="num" :class="lagNumClass(lagAvgSeconds)">{{ fmtLagSeconds(lagAvgSeconds) }}</span>
            <span class="mr-sep">/</span>
            <span class="num" :class="lagNumClass(lagMaxSeconds)">{{ fmtLagSeconds(lagMaxSeconds) }}</span>
          </template>
        </template>
      </MetricRow>

      <!-- Health -->
      <MetricRow
        label="Errors"
        :value="formatNumber(errorTotal)"
        :context="errorContext"
        :sparkline="errorSeriesData"
        :labels="chartLabels"
        :value-format="fmtCount"
        expand-unit="ack failures"
        :severity="errorSeverity"
        :clickable="errorTotal > 0"
        @click="$router.push('/dlq')"
        :loading="loadingOps"
        :error="opsError"
        tooltip="Ack failures for this tenant across the window. DLQ depth is a current snapshot, not a per-window count, so it is shown separately in the context line."
        :expanded="isExpanded('errors')"
        @toggle-expand="toggleRow('errors')"
      />

      <!-- Admin -->
      <MetricRow
        label="Partitions"
        :context="'created / deleted in window'"
        :series="partitionSeriesData"
        :labels="chartLabels"
        :value-format="fmtCount"
        expand-unit="count"
        :loading="loadingOps"
        :error="opsError"
        :expanded="isExpanded('partitions')"
        @toggle-expand="toggleRow('partitions')"
      >
        <template #value>
          <span class="num" style="color:var(--text-hi);">+{{ formatNumber(partitionCreatedTotal) }}</span>
          <span class="mr-sep">/</span>
          <span class="num mute">−{{ formatNumber(partitionDeletedTotal) }}</span>
        </template>
      </MetricRow>
      <MetricRow
        label="Retention"
        :context="retentionContext"
        :series="retentionSeriesData"
        :labels="retentionLabels"
        :value-format="fmtCount"
        expand-unit="msgs"
        :error="retentionError"
        tooltip="Messages deleted by the retention / eviction workers. The log engine does not write retention_history yet, so an empty series means NOT REPORTED — it is not proof that nothing was deleted."
        :expanded="isExpanded('retention')"
        @toggle-expand="toggleRow('retention')"
      >
        <template #value>
          <span v-if="retentionTotal === null" class="num">—</span>
          <template v-else>
            <span class="num">{{ formatNumber(retentionTotal) }}</span><i class="mr-unit">msgs</i>
          </template>
        </template>
      </MetricRow>

      <!-- =====================================================================
           CELL · OPERATOR. Everything below covers every tenant on this cell,
           and each source is a route the proxy answers 404 for anyone else.
           ===================================================================== -->
      <template v-if="can('operator')">
        <div class="metric-cell-head">
          <span class="metric-cell-tag">CELL · OPERATOR</span>
          <span>host and engine figures for the whole cell — every tenant on it, not just {{ actingTenantSlug || 'this tenant' }}</span>
        </div>
        <MetricRow
          label="Event loop"
          scope="cell"
          :context="eventLoopContext"
          :series="elSeriesData"
          :labels="statusChartLabels"
          :value-format="(v) => v + ' ms'"
          expand-unit="ms"
          :severity="elNumClass(maxEventLoopLag)"
          :loading="loadingStatus"
          :error="statusError"
          :expanded="isExpanded('eventLoop')"
          @toggle-expand="toggleRow('eventLoop')"
        >
          <template #value>
            <template v-if="avgEventLoopLag === null && maxEventLoopLag === null">
              <span class="num">—</span>
            </template>
            <template v-else>
              <span class="num" :class="elNumClass(avgEventLoopLag)">{{ avgEventLoopLag ?? '—' }}<i class="mr-unit">ms</i></span>
              <span class="mr-sep">/</span>
              <span class="num" :class="elNumClass(maxEventLoopLag)">{{ maxEventLoopLag ?? '—' }}<i class="mr-unit">ms</i></span>
            </template>
          </template>
        </MetricRow>
        <MetricRow
          label="Queen CPU"
          scope="cell"
          :context="cpuContext"
          :series="cpuSeriesData"
          :labels="cpuLabels"
          :value-format="(v) => v.toFixed(1) + '%'"
          expand-unit="%"
          :loading="loadingStatus"
          :error="cpuError"
          :expanded="isExpanded('cpu')"
          @toggle-expand="toggleRow('cpu')"
        >
          <template #value>
            <span v-if="cpuLatest === null" class="num">—</span>
            <template v-else>
              <span class="num">{{ cpuLatest.toFixed(1) }}</span><i class="mr-unit">%</i>
            </template>
          </template>
        </MetricRow>
        <MetricRow
          label="DB pool"
          scope="cell"
          :context="poolContext"
          :series="poolSeriesData"
          :labels="statusChartLabels"
          :value-format="(v) => Math.round(v) + ' conns'"
          expand-unit="connections"
          :severity="poolSeverity"
          :loading="loadingStatus"
          :error="statusError"
          :expanded="isExpanded('dbPool')"
          @toggle-expand="toggleRow('dbPool')"
        >
          <template #value>
            <span v-if="!poolLatest" class="num">—</span>
            <template v-else>
              <span class="num" :class="poolSeverity">{{ poolLatest.active ?? '—' }}</span>
              <span class="mr-sep">/</span>
              <span class="num">{{ poolLatest.size ?? '—' }}</span>
              <i class="mr-unit">conns</i>
            </template>
          </template>
        </MetricRow>
      </template>
    </div>

    <!--
      ========================================================================
      Bottom row — two symmetric panels using the same row idiom:
          dot · name · right-side metric  |  meta line below.
      Top queues is sorted by pending depth (the operational priority);
      consumer groups by max time lag (the operational symptom). Click a
      row to drill into its detail page; "see all" footer links the full
      list view.
      ========================================================================
    -->
    <div class="grid-2">
      <div class="card">
        <div class="card-header">
          <h3>Top queues by pending</h3>
          <span class="muted">{{ queuesFailed ? '—' : `${enrichedQueues.length} queues` }}</span>
        </div>

        <div v-if="loadingQueues" class="card-body entity-list">
          <div v-for="i in 6" :key="i" class="skeleton" style="height:48px; border-radius:8px;" />
        </div>

        <div v-else-if="queuesFailed" class="card-body entity-empty entity-failed">
          {{ queuesErrorText }}
        </div>

        <div v-else-if="topPendingQueues.length" class="card-body entity-list">
          <button
            v-for="q in topPendingQueues"
            :key="q.name"
            class="entity-row"
            @click="$router.push(`/queues/${encodeURIComponent(q.name)}`)"
          >
            <div class="entity-head">
              <span class="status-dot" :class="statusDotClass(q._status)" />
              <span class="entity-name">{{ q.name }}</span>
              <span class="entity-right num" :class="lagNumClass(q._lag)">
                {{ q._lag > 0 ? fmtLagSeconds(q._lag) : '—' }}
              </span>
            </div>
            <div class="entity-meta">
              <span class="bar bar-meta">
                <i :class="depthBarClass(q._status)" :style="{ width: q._depthPct + '%' }" />
              </span>
              <span class="meta-text">
                <strong>{{ q._pending === null ? '—' : formatNumber(q._pending) }}</strong> pending
                <span class="meta-sep">·</span>
                {{ q.partitions || 1 }} {{ (q.partitions || 1) === 1 ? 'part' : 'parts' }}
              </span>
            </div>
          </button>
        </div>

        <div v-else class="card-body entity-empty">No queues</div>

        <div class="card-foot">
          <a class="card-foot-link" @click="$router.push('/queues')">See all queues →</a>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Consumer groups by lag</h3>
          <span class="muted">{{ consumersFailed ? '—' : `${consumers.length} total · ${laggingCount} lagging` }}</span>
        </div>

        <div v-if="loadingConsumers" class="card-body entity-list">
          <div v-for="i in 6" :key="i" class="skeleton" style="height:48px; border-radius:8px;" />
        </div>

        <div v-else-if="consumersFailed" class="card-body entity-empty entity-failed">
          {{ consumersErrorText }}
        </div>

        <div v-else-if="sortedConsumers.length" class="card-body entity-list">
          <button
            v-for="g in sortedConsumers.slice(0, 6)"
            :key="g.name + '@' + g.queueName"
            class="entity-row"
            @click="$router.push('/consumers')"
          >
            <div class="entity-head">
              <span class="status-dot" :class="cgDotClass(g)" />
              <span class="entity-name">{{ g.queueName || '?' }}</span>
              <span class="entity-right num" :class="lagNumClass(g.maxTimeLag)">
                {{ (g.maxTimeLag || 0) > 0 ? fmtLagSeconds(g.maxTimeLag) : '—' }}
              </span>
            </div>
            <div class="entity-meta">
              <span class="meta-text">
                <span v-if="g.name === '__QUEUE_MODE__'" class="meta-tag">queue mode</span>
                <span v-else><strong>{{ g.name }}</strong></span>
                <span class="meta-sep">·</span>
                <!-- One row per (partition, group) cursor — NOT a consumer
                     count: one process on a 32-partition queue is 32 rows. -->
                {{ g.members || 0 }} {{ (g.members || 0) === 1 ? 'partition' : 'partitions' }} assigned
                <template v-if="(g.partitionsWithLag || 0) > 0">
                  <span class="meta-sep">·</span>
                  <span class="num warn">{{ g.partitionsWithLag }} lagging</span>
                </template>
              </span>
            </div>
          </button>
        </div>

        <div v-else class="card-body entity-empty">No consumer groups</div>

        <div class="card-foot">
          <a class="card-foot-link" @click="$router.push('/consumers')">See all consumer groups →</a>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import {
  resources,
  queues as queuesApi,
  consumers as consumersApi,
  system as systemApi,
  operator as operatorApi,
  describeApiError,
} from '@/api'
import {
  useApi, formatNumber, toNum, latestFinite, trimIncompleteBuckets,
} from '@/composables/useApi'
import { formatChartLabel } from '@/composables/useFormat'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useIdentity } from '@/stores/identity'
import MetricRow from '@/components/MetricRow.vue'

const { can, actingTenantSlug } = useIdentity()

// ---------------------------------------------------------------------------
// Range
// ---------------------------------------------------------------------------
const selectedRange = ref('1h')
const timeRanges = [
  { label: '1h',  value: '1h',  minutes: 60 },
  { label: '6h',  value: '6h',  minutes: 360 },
  { label: '24h', value: '24h', minutes: 1440 },
]
const getTimeRangeParams = () => {
  const r = timeRanges.find(x => x.value === selectedRange.value) || timeRanges[0]
  const now = new Date()
  return {
    from: new Date(now.getTime() - r.minutes * 60 * 1000).toISOString(),
    to: now.toISOString(),
  }
}

// ---------------------------------------------------------------------------
// SOURCES. Two surfaces, and each row says which one it came from.
//
//   TENANT-SCOPED (the proxy injects the acting cluster's tenant): overview,
//   queues, consumer groups, queue-ops, retention. These answer "for this
//   tenant" and every row built from them is unlabelled.
//
//   CELL-LEVEL: /api/v1/status and /analytics/system-metrics are operator
//   routes — 404 route_blocked for every other principal. They are fetched
//   only when can('operator') and every row built from them carries the
//   `cell` chip, because a cell figure read as a tenant figure is a lie.
//
// Nothing here swallows a failure: `error` drives an inline "unavailable"
// state, so an endpoint we cannot reach never renders as an idle cluster.
// ---------------------------------------------------------------------------
const overviewQ  = useApi((config) => resources.getOverview(config), { immediate: false })
const queuesQ    = useApi((config) => queuesApi.list(undefined, config), { immediate: false })
const consumersQ = useApi((config) => consumersApi.list(config), { immediate: false })
const opsQ       = useApi((config) => systemApi.getQueueOps(getTimeRangeParams(), config), { immediate: false })
const retentionQ = useApi((config) => systemApi.getRetention(getTimeRangeParams(), config), { immediate: false })
const statusQ    = useApi((config) => operatorApi.getStatus(getTimeRangeParams(), config), { immediate: false })
const sysQ       = useApi((config) => operatorApi.getSystemMetrics(getTimeRangeParams(), config), { immediate: false })

const overview = overviewQ.data
const queues = computed(() => {
  const d = queuesQ.data.value
  return d?.queues || (Array.isArray(d) ? d : [])
})
const consumers = computed(() => {
  const d = consumersQ.data.value
  return Array.isArray(d) ? d : (d?.consumer_groups || [])
})

// Skeletons only until the first payload lands; a 30s refresh must not blank
// the page the user is reading.
const firstLoad = (q) => computed(() => q.loading.value && q.data.value === null)
const loadingOverview  = firstLoad(overviewQ)
const loadingQueues    = firstLoad(queuesQ)
const loadingConsumers = firstLoad(consumersQ)
const loadingOps       = firstLoad(opsQ)
const loadingStatus    = firstLoad(statusQ)

const overviewError  = computed(() => overviewQ.error.value)
const opsError       = computed(() => opsQ.error.value)
const retentionError = computed(() => retentionQ.error.value)
const statusError    = computed(() => statusQ.error.value)

const overviewFailed  = computed(() => overviewQ.error.value !== null)
const queuesFailed    = computed(() => queuesQ.error.value !== null)
const consumersFailed = computed(() => consumersQ.error.value !== null)
const statusFailed    = computed(() => statusQ.error.value !== null)

const queuesErrorText    = computed(() => describeApiError(queuesQ.error.value))
const consumersErrorText = computed(() => describeApiError(consumersQ.error.value))

// ---------------------------------------------------------------------------
// Expand state — per-row + master toggle.
// We use a Set keyed by stable row ids (not the human-readable label)
// so future label tweaks don't blow away the user's expanded selection.
// ---------------------------------------------------------------------------
const ALL_ROW_KEYS = [
  'throughput', 'pendingDelta', 'parked', 'fillRatio', 'timeLag',
  'errors', 'partitions', 'retention',
  'eventLoop', 'cpu', 'dbPool',
]
const expandedRows = ref(new Set())
const isExpanded = (key) => expandedRows.value.has(key)
const toggleRow = (key) => {
  const next = new Set(expandedRows.value)
  if (next.has(key)) next.delete(key); else next.add(key)
  expandedRows.value = next
}
const anyExpanded = computed(() => expandedRows.value.size > 0)
const toggleAllRows = () => {
  expandedRows.value = anyExpanded.value ? new Set() : new Set(ALL_ROW_KEYS)
}

// ---------------------------------------------------------------------------
// TENANT history — queue-ops rolled up from one row per (queue, bucket) to one
// row per bucket. This is the source for every unlabelled row on the page.
// ---------------------------------------------------------------------------
const history = computed(() => {
  const payload = opsQ.data.value
  const series = payload?.series || []
  if (!series.length) return []

  const byBucket = new Map()
  for (const row of series) {
    let b = byBucket.get(row.bucket)
    if (!b) {
      b = {
        bucket: row.bucket,
        pushPerSecond: 0, popPerSecond: 0, ackPerSecond: 0,
        pushMessages: 0, popMessages: 0, ackSuccess: 0, ackFailed: 0, popEmpty: 0,
        partitionsCreated: 0, partitionsDeleted: 0,
        // parkedCount is already SUM-ed across workers per (queue, bucket);
        // summing again across queues is a legitimate gauge composition since
        // a long-poll lives on exactly one (queue, partition, worker).
        parkedTotal: 0,
        lagWeighted: 0, lagPops: 0, maxLagMs: null,
      }
      byBucket.set(row.bucket, b)
    }
    b.pushPerSecond += toNum(row.pushPerSecond) || 0
    b.popPerSecond  += toNum(row.popPerSecond)  || 0
    b.ackPerSecond  += toNum(row.ackPerSecond)  || 0
    b.pushMessages  += toNum(row.pushMessages)  || 0
    b.popMessages   += toNum(row.popMessages)   || 0
    b.ackSuccess    += toNum(row.ackSuccess)    || 0
    b.ackFailed     += toNum(row.ackFailed)     || 0
    b.popEmpty      += toNum(row.popEmpty)      || 0
    b.partitionsCreated += toNum(row.partitionsCreated) || 0
    b.partitionsDeleted += toNum(row.partitionsDeleted) || 0
    b.parkedTotal   += toNum(row.parkedCount)   || 0

    // Lag is sampled AT POP. A queue with no pops in the bucket contributes no
    // measurement — folding its 0 in would report a stalled backlog as zero lag.
    const pops = toNum(row.popMessages) || 0
    if (pops > 0) {
      b.lagWeighted += (toNum(row.avgLagMs) || 0) * pops
      b.lagPops += pops
      const mx = toNum(row.maxLagMs)
      if (mx !== null) b.maxLagMs = b.maxLagMs === null ? mx : Math.max(b.maxLagMs, mx)
    }
  }

  const rows = [...byBucket.values()].sort((a, b) => a.bucket.localeCompare(b.bucket))
  for (const b of rows) b.avgLagMs = b.lagPops > 0 ? b.lagWeighted / b.lagPops : null
  // Drop the still-aggregating tail bucket so the right edge of every chart
  // isn't a partial sample reading low.
  return trimIncompleteBuckets(rows, {
    bucketKey: 'bucket',
    bucketMinutes: payload?.bucketMinutes || 1,
  })
})

const multiDay = computed(() => {
  const h = history.value
  if (h.length < 2) return false
  return new Date(h[0].bucket).toDateString() !== new Date(h[h.length - 1].bucket).toDateString()
})
const chartLabels = computed(() =>
  history.value.map(h => formatChartLabel(new Date(h.bucket), multiDay.value))
)

// ---------------------------------------------------------------------------
// Counts strip helpers
// ---------------------------------------------------------------------------
const totalPartitions = computed(() =>
  queues.value.reduce((sum, q) => sum + (q.partitions || 1), 0)
)

// ---------------------------------------------------------------------------
// Throughput row — real push / pop / ack rates for this tenant, summed across
// its queues. NOT the overview's ingestedPerSecond, which under the per-tenant
// path is a retained-message delta and collapses to 0 after a retention sweep.
// ---------------------------------------------------------------------------
const throughput = computed(() => {
  const v = latestFinite(history.value.map(x => x.pushPerSecond))
  return { current: v === null ? '—' : v.toFixed(1) }
})
const throughputSeries = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [
    { label: 'Push', data: h.map(x => toNum(x.pushPerSecond)) },
    { label: 'Pop',  data: h.map(x => toNum(x.popPerSecond)) },
    { label: 'Ack',  data: h.map(x => toNum(x.ackPerSecond)), color: '#4ade80' },
  ]
})
const throughputPeak = computed(() => {
  const finite = history.value.map(x => toNum(x.pushPerSecond)).filter(v => v !== null)
  return finite.length ? Math.max(0, ...finite) : 0
})
const throughputContext = computed(() => {
  if (!history.value.length) return 'no queue-ops buckets in window'
  const peak = throughputPeak.value
  const cur = Number(throughput.value.current) || 0
  if (peak === 0 && cur === 0) return 'idle · no traffic in window'
  if (peak === 0) return `current ${cur.toFixed(1)} /s`
  return `peak ${formatNumber(Math.round(peak))} /s push`
})

// ---------------------------------------------------------------------------
// Pending Δ row — cumulative (push − ack) across the window.
// Positive = falling behind, negative = catching up. When a bucket's source
// values are missing we emit `null` (not 0) so the chart shows a gap; the
// cumulative carries forward so the next valid bucket continues the total.
// ---------------------------------------------------------------------------
const pendingDeltaSeries = computed(() => {
  const h = history.value
  if (!h.length) return []
  let cum = 0
  return h.map(x => {
    const pushed = toNum(x.pushMessages)
    const acked = toNum(x.ackSuccess)
    if (pushed === null && acked === null) return null
    cum += (pushed || 0) - (acked || 0)
    return cum
  })
})
const pendingDeltaLatest = computed(() => latestFinite(pendingDeltaSeries.value))
const pendingDeltaDisplay = computed(() => {
  const v = pendingDeltaLatest.value
  if (v === null) return '—'
  if (v === 0) return '0'
  return (v > 0 ? '+' : '−') + formatNumber(Math.abs(v))
})
const pendingDeltaContext = computed(() => {
  const v = pendingDeltaLatest.value
  if (v === null) return 'no push / ack samples in window'
  if (v === 0) return 'flat · push = ack across window'
  if (v > 0) return 'falling behind · push > ack'
  return 'catching up · ack > push'
})
const pendingDeltaSeverity = computed(() => {
  const v = pendingDeltaLatest.value
  if (v === null) return ''
  if (v > 100000) return 'bad'
  if (v > 1000)   return 'warn'
  if (v < -1000)  return 'ok'
  return ''
})

// ---------------------------------------------------------------------------
// Time lag row.
//
// VALUE  = the tenant overview's lag, which under the log engine falls back to
//          the age of the oldest unconsumed message — so it stays correct with
//          consumers stopped. NULL means the broker has no measurement, and
//          that must render '—', never 0s.
// CHART  = per-bucket pop-sampled lag, with a gap for every bucket that had no
//          pops (see `history` above).
// ---------------------------------------------------------------------------
const lagAvgSeconds = computed(() => toNum(overview.value?.lag?.time?.avg))
const lagMaxSeconds = computed(() => toNum(overview.value?.lag?.time?.max))
const lagSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [
    { label: 'Avg', data: h.map(x => toNum(x.avgLagMs)) },
    { label: 'Max', data: h.map(x => toNum(x.maxLagMs)) },
  ]
})
const lagNumClass = (s) => !s || s === 0 ? '' : s < 60 ? '' : s < 300 ? 'warn' : 'bad'
const lagSeverity = computed(() => lagNumClass(lagMaxSeconds.value || 0))
const lagContext = computed(() => {
  const sampled = history.value.some(x => x.avgLagMs !== null)
  return sampled
    ? 'oldest-message age now · chart is pop-sampled'
    : 'oldest-message age now · no pops in window, chart empty'
})

// ---------------------------------------------------------------------------
// Errors row — ack failures for this tenant across the window, plus the CURRENT
// DLQ depth (a snapshot, so it is named as one rather than added to a window
// sum). `db_errors` is deliberately absent: it is a cell-wide counter the
// broker never increments, and charting a constant zero labelled "DB errors"
// is worse than not charting it.
// ---------------------------------------------------------------------------
const errorSeriesData = computed(() => history.value.map(x => toNum(x.ackFailed)))
const ackFailedTotal = computed(() =>
  history.value.reduce((s, x) => s + (toNum(x.ackFailed) || 0), 0)
)
const dlqDepth = computed(() => toNum(overview.value?.messages?.deadLetter))
const errorTotal = computed(() => ackFailedTotal.value)
const errorContext = computed(() => {
  const dlq = dlqDepth.value
  const dlqText = dlq === null ? 'dlq —' : `dlq ${formatNumber(dlq)} now`
  return `ack ${formatNumber(ackFailedTotal.value)} in window · ${dlqText}`
})
const errorSeverity = computed(() => {
  const ack = ackFailedTotal.value
  const dlq = dlqDepth.value || 0
  if (ack > 100) return 'bad'
  if (ack > 0 || dlq > 0) return 'warn'
  return ''
})

// ---------------------------------------------------------------------------
// Partitions row — admin events from the same queue-ops buckets.
// ---------------------------------------------------------------------------
const partitionSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [
    { label: 'Created', data: h.map(x => toNum(x.partitionsCreated)) },
    { label: 'Deleted', data: h.map(x => toNum(x.partitionsDeleted)) },
  ]
})
const partitionCreatedTotal = computed(() =>
  history.value.reduce((s, x) => s + (toNum(x.partitionsCreated) || 0), 0)
)
const partitionDeletedTotal = computed(() =>
  history.value.reduce((s, x) => s + (toNum(x.partitionsDeleted) || 0), 0)
)

// ---------------------------------------------------------------------------
// Parked row — in-flight long-poll consumer connections, summed across the
// tenant's queues per bucket.
// ---------------------------------------------------------------------------
const parkedSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [{ label: 'Parked', data: h.map(x => toNum(x.parkedTotal)) }]
})
const parkedLatest = computed(() => latestFinite(history.value.map(x => x.parkedTotal)))
const parkedAvg = computed(() => {
  const finite = history.value.map(x => toNum(x.parkedTotal)).filter(v => v !== null)
  if (!finite.length) return 0
  return finite.reduce((s, v) => s + v, 0) / finite.length
})
const parkedContext = computed(() => {
  if (!history.value.length) return '—'
  return `avg ${parkedAvg.value.toFixed(1)} across window · idle long-polls`
})

// ---------------------------------------------------------------------------
// Fill ratio row — popMessages / (popMessages + popEmpty) per bucket.
//   • near 0% with traffic → consumers waiting in vain (over-provisioned)
//   • near 100% sustained  → consumers fully utilized; watch the lag row
// Buckets below the noise floor carry the previous value forward so the line
// stays continuous; the headline uses a real null → '—' so a truly idle window
// is distinguishable from 0% delivery.
// ---------------------------------------------------------------------------
const FILL_NOISE_FLOOR = 5

const fillSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  let last = null
  const data = h.map(x => {
    const pop = toNum(x.popMessages)
    const empty = toNum(x.popEmpty)
    if (pop === null && empty === null) return null
    const total = (pop || 0) + (empty || 0)
    if (total < FILL_NOISE_FLOOR) return last
    last = Math.round(((pop || 0) / total) * 1000) / 10
    return last
  })
  return [{ label: 'Fill', data }]
})

const fillLatest = computed(() => {
  const tail = history.value.slice(-5)
  if (!tail.length) return null
  let pop = 0, empty = 0
  for (const r of tail) {
    pop   += toNum(r.popMessages) || 0
    empty += toNum(r.popEmpty)    || 0
  }
  if (pop + empty < FILL_NOISE_FLOOR) return null
  return Math.round((pop / (pop + empty)) * 1000) / 10
})

const fillContext = computed(() => {
  const v = fillLatest.value
  if (v === null) return 'idle window · no long-poll activity'
  if (v >= 80) return 'high utilization · consumers busy serving'
  if (v < 30)  return 'low utilization · consumers mostly empty'
  return 'balanced · consumer pool sized OK'
})

// Only LOW fill is flagged — high fill is a positive signal on its own, and
// becomes a problem only alongside rising lag, which has its own row.
const fillSeverity = computed(() => {
  const v = fillLatest.value
  if (v === null) return ''
  if (v < 30) return 'warn'
  return ''
})

// ---------------------------------------------------------------------------
// Retention row.
//
// queen.retention_history has no writer under the log engine, so an empty
// series means NOT REPORTED. Rendering that as "0 msgs evicted" would assert
// something the product cannot know while the engine is deleting segments.
// ---------------------------------------------------------------------------
const retentionRows = computed(() => {
  const payload = retentionQ.data.value
  const series = payload?.series || []
  return trimIncompleteBuckets(series, {
    bucketKey: 'bucket',
    bucketMinutes: payload?.bucketMinutes || 1,
  })
})
const retentionSeriesData = computed(() => {
  const rows = retentionRows.value
  if (!rows.length) return null
  return [
    { label: 'Retention', data: rows.map(r => toNum(r.retentionMsgs)) },
    { label: 'Completed', data: rows.map(r => toNum(r.completedRetentionMsgs)) },
    { label: 'Evicted',   data: rows.map(r => toNum(r.evictionMsgs)), color: '#e6b450' },
  ]
})
const retentionLabels = computed(() =>
  retentionRows.value.map(r => formatChartLabel(new Date(r.bucket), false))
)
const retentionTotal = computed(() => {
  const rows = retentionRows.value
  if (!rows.length) return null
  return rows.reduce((s, r) =>
    s +
    (toNum(r.retentionMsgs) || 0) +
    (toNum(r.completedRetentionMsgs) || 0) +
    (toNum(r.evictionMsgs) || 0)
  , 0)
})
const retentionContext = computed(() => {
  if (retentionQ.data.value === null) return 'loading…'
  if (!retentionRows.value.length) {
    return 'not reported · the log engine does not record retention events yet'
  }
  return 'evicted + completed-retention in window'
})

// ===========================================================================
// CELL · OPERATOR sources. Everything below reads /api/v1/status or
// /analytics/system-metrics — cell-wide by nature, operator-only at the proxy.
// ===========================================================================
const statusHistory = computed(() => {
  const s = statusQ.data.value
  if (!s?.throughput?.length) return []
  // status_v3 returns rows newest → oldest; charts read left→right.
  return trimIncompleteBuckets([...s.throughput].reverse(), {
    bucketKey: 'timestamp',
    bucketMinutes: s.bucketMinutes || 1,
  })
})
const statusMultiDay = computed(() => {
  const h = statusHistory.value
  if (h.length < 2) return false
  return new Date(h[0].timestamp).toDateString() !== new Date(h[h.length - 1].timestamp).toDateString()
})
const statusChartLabels = computed(() =>
  statusHistory.value.map(h => formatChartLabel(new Date(h.timestamp), statusMultiDay.value))
)

// --- Event loop (cell) ---
const workerCount = computed(() => statusQ.data.value?.workers?.length || 0)
const avgEventLoopLag = computed(() => {
  const w = statusQ.data.value?.workers
  if (!w?.length) return null
  // Average only over workers that actually reported — a worker missing the
  // field must not pull the cell average towards zero.
  const finite = w.map(x => toNum(x.avgEventLoopLagMs)).filter(v => v !== null)
  if (!finite.length) return null
  return Math.round(finite.reduce((s, x) => s + x, 0) / finite.length)
})
const maxEventLoopLag = computed(() => {
  const w = statusQ.data.value?.workers
  if (!w?.length) return null
  const finite = w.map(x => toNum(x.maxEventLoopLagMs)).filter(v => v !== null)
  return finite.length ? Math.max(...finite) : null
})
const elSeriesData = computed(() => {
  const h = statusHistory.value
  if (!h.length) return null
  return [
    { label: 'Avg', data: h.map(x => toNum(x.avgEventLoopLagMs)) },
    { label: 'Max', data: h.map(x => toNum(x.maxEventLoopLagMs)) },
  ]
})
const elNumClass = (ms) => !ms || ms === 0 ? '' : ms < 50 ? '' : ms < 100 ? 'warn' : 'bad'
const eventLoopContext = computed(() => {
  const n = workerCount.value
  if (!n) return 'no workers reporting on this cell'
  return `${n} worker${n === 1 ? '' : 's'} on this cell · avg / max`
})

// --- Queen CPU (cell) ---
// CPU is cumulative across cores (4 cores pinned = 400%), so it is shown
// without a semantic tone: without a core count a threshold would be a guess.
const hasMultipleReplicas = computed(() => (sysQ.data.value?.replicas || []).length > 1)
const cpuError = computed(() => hasMultipleReplicas.value ? sysQ.error.value : statusQ.error.value)

const combinedCpu = (t) => {
  const user = toNum(t.metrics?.cpu?.user_us?.avg)
  const sys  = toNum(t.metrics?.cpu?.system_us?.avg)
  if (user === null && sys === null) return null
  return ((user || 0) + (sys || 0)) / 100
}

// Single replica → user vs system split. Multi replica → one line per replica
// so fanout imbalance is visible (the value still shows the hottest).
const cpuSeriesData = computed(() => {
  if (hasMultipleReplicas.value) {
    return (sysQ.data.value?.replicas || []).map(r => ({
      label: r.hostname?.substring(0, 12) || 'replica',
      data: (r.timeSeries || []).map(combinedCpu),
    }))
  }
  const h = statusHistory.value
  if (!h.length) return null
  return [
    { label: 'User',   data: h.map(x => toNum(x.queenCpuUserPct)) },
    { label: 'System', data: h.map(x => toNum(x.queenCpuSysPct)) },
  ]
})

// Per-replica buckets aren't necessarily aligned with the status buckets that
// drive the other cell rows, so multi-replica mode carries its own labels.
const cpuLabels = computed(() => {
  if (!hasMultipleReplicas.value) return statusChartLabels.value
  const ts = (sysQ.data.value?.replicas || [])[0]?.timeSeries || []
  if (!ts.length) return []
  const multi = ts.length >= 2 &&
    new Date(ts[0].timestamp).toDateString() !== new Date(ts[ts.length - 1].timestamp).toDateString()
  return ts.map(t => formatChartLabel(new Date(t.timestamp), multi))
})
const cpuLatest = computed(() => {
  if (hasMultipleReplicas.value) {
    let max = null
    for (const r of (sysQ.data.value?.replicas || [])) {
      const v = latestFinite((r.timeSeries || []).map(combinedCpu))
      if (v !== null && (max === null || v > max)) max = v
    }
    return max
  }
  const u = latestFinite(statusHistory.value.map(x => x.queenCpuUserPct))
  const s = latestFinite(statusHistory.value.map(x => x.queenCpuSysPct))
  if (u === null && s === null) return null
  return (u || 0) + (s || 0)
})
const cpuContext = computed(() => {
  if (hasMultipleReplicas.value) {
    return `${(sysQ.data.value?.replicas || []).length} replicas on this cell · hottest shown`
  }
  const u = latestFinite(statusHistory.value.map(x => x.queenCpuUserPct))
  const s = latestFinite(statusHistory.value.map(x => x.queenCpuSysPct))
  if (u === null && s === null) return 'no CPU samples for this cell'
  return `user ${(u || 0).toFixed(0)}% · sys ${(s || 0).toFixed(0)}% · whole cell`
})

// --- DB pool (cell) — saturation = waiters; warn at 80% util, bad at size. ---
const poolLatest = computed(() => {
  const h = statusHistory.value
  if (!h.length) return null
  const active = latestFinite(h.map(x => x.dbPoolActive))
  const idle   = latestFinite(h.map(x => x.dbPoolIdle))
  const size   = latestFinite(h.map(x => x.dbPoolSize))
  if (active === null && idle === null && size === null) return null
  return {
    active: active === null ? null : Math.round(active),
    idle: idle === null ? null : Math.round(idle),
    size: size === null
      ? (active === null && idle === null ? null : Math.round((active || 0) + (idle || 0)))
      : Math.round(size),
  }
})
const poolSeriesData = computed(() => {
  const h = statusHistory.value
  if (!h.length) return null
  return [
    { label: 'Active', data: h.map(x => toNum(x.dbPoolActive)) },
    { label: 'Idle',   data: h.map(x => toNum(x.dbPoolIdle)) },
  ]
})
const poolSeverity = computed(() => {
  const p = poolLatest.value
  if (!p || !p.size || p.active === null) return ''
  if (p.active >= p.size) return 'bad'
  if (p.active / p.size > 0.8) return 'warn'
  return ''
})
const poolContext = computed(() => {
  const p = poolLatest.value
  if (!p) return 'no pool samples for this cell'
  return `idle ${p.idle ?? '—'} · size ${p.size ?? '—'} · whole cell`
})

// --- Batch efficiency (cell) — point-in-time averages. <5 = tiny commits. ---
const batchEfficiency = computed(() => {
  const b = statusQ.data.value?.messages?.batchEfficiency
  const one = (v) => {
    const n = toNum(v)
    return n === null ? '—' : n.toFixed(1)
  }
  return { push: one(b?.push), pop: one(b?.pop), ack: one(b?.ack) }
})

// ---------------------------------------------------------------------------
// Counts strip — pending tone (only number that gets thresholded inline)
// ---------------------------------------------------------------------------
const pendingNumClass = (n) => !n || n < 1000 ? '' : n < 10000 ? 'warn' : 'bad'

// ---------------------------------------------------------------------------
// Bottom panels — Top queues by pending + Consumer groups by lag.
// Both panels share one row idiom (dot · name · right metric · meta line).
// ---------------------------------------------------------------------------

// Per-queue worst-lag rollup, keyed off consumer-group lag (the queue itself
// doesn't carry a lag; it's a property of its consumer groups).
const queueLagMap = computed(() => {
  const m = {}
  for (const c of consumers.value) {
    const q = c.queueName
    if (!q) continue
    const lag = c.maxTimeLag || 0
    if (!m[q] || lag > m[q]) m[q] = lag
  }
  return m
})

const enrichedQueues = computed(() => {
  // `pending` is passed through verbatim: null when the broker did not report
  // one, so the panel can render '—' instead of claiming an empty queue.
  const base = queues.value.map(q => ({ ...q, _pending: toNum(q.messages?.pending) }))
  const maxPending = Math.max(...base.map(q => q._pending ?? 0), 1)
  return base
    .map(q => {
      const lag = queueLagMap.value[q.name] || 0
      const depth = q._pending ?? 0
      const status =
        lag >= 300 || (depth / maxPending) > 0.8 ? 'degraded'
      : lag >= 60  || (depth / maxPending) > 0.5 ? 'watch'
      : 'healthy'
      return {
        ...q,
        _lag: lag,
        _status: status,
        _depthPct: Math.min(100, (depth / maxPending) * 100),
      }
    })
    .sort((a, b) => (b._pending ?? -1) - (a._pending ?? -1))
})

// Top 6 by pending depth — the operational priority for "what's piling up".
const topPendingQueues = computed(() => enrichedQueues.value.slice(0, 6))

const sortedConsumers = computed(() =>
  [...consumers.value].sort((a, b) => (b.maxTimeLag || 0) - (a.maxTimeLag || 0))
)
const laggingCount = computed(() =>
  consumers.value.filter(c => (c.maxTimeLag || 0) >= 60 || (c.partitionsWithLag || 0) > 0).length
)

// Severity → status-dot class mapping. Reused for both panels.
const statusDotClass = (s) =>
  s === 'degraded' ? 'status-dot-danger'
: s === 'watch'    ? 'status-dot-warning'
                   : 'status-dot-success'

const cgStatus = (g) => {
  const lag = g.maxTimeLag || 0
  if (lag >= 300) return 'stuck'
  if (lag >= 60 || (g.partitionsWithLag || 0) > 0) return 'lag'
  return 'healthy'
}
const cgDotClass = (g) => {
  const s = cgStatus(g)
  return s === 'stuck' ? 'status-dot-danger'
       : s === 'lag'   ? 'status-dot-warning'
                       : 'status-dot-success'
}

// Bar fill color follows queue status (same severity vocabulary).
const depthBarClass = (s) => s === 'degraded' ? 'bad' : s === 'watch' ? 'warn' : ''

// ---------------------------------------------------------------------------
// Formatters. The unit is in the name: `fmtLagMs` takes milliseconds,
// `fmtLagSeconds` takes seconds — the two used to share one name across views.
// ---------------------------------------------------------------------------
const fmtRate = (n) => {
  const v = Number(n) || 0
  if (Math.abs(v) >= 1000) return (v / 1000).toFixed(1) + 'k /s'
  if (Math.abs(v) >= 100)  return Math.round(v) + ' /s'
  if (Math.abs(v) >= 10)   return v.toFixed(1) + ' /s'
  return v.toFixed(2) + ' /s'
}
const fmtCount = (n) => {
  const v = Number(n) || 0
  return formatNumber(Math.round(v))
}
const fmtLagMs = (n) => {
  const v = Number(n) || 0
  if (v < 1) return '0'
  if (v < 1000) return Math.round(v) + ' ms'
  if (v < 60000) return (v / 1000).toFixed(1) + ' s'
  return (v / 60000).toFixed(1) + ' m'
}
const fmtLagSeconds = (seconds) => {
  if (seconds === null || seconds === undefined) return '—'
  if (seconds === 0) return '0s'
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60); const s = Math.round(seconds % 60)
    return s ? `${m}m ${s}s` : `${m}m`
  }
  if (seconds < 86400) {
    const h = Math.floor(seconds / 3600); const m = Math.floor((seconds % 3600) / 60)
    return m ? `${h}h ${m}m` : `${h}h`
  }
  const d = Math.floor(seconds / 86400); const h = Math.floor((seconds % 86400) / 3600)
  return h ? `${d}d ${h}h` : `${d}d`
}
// Fill-ratio buckets can be null ("not enough long-poll activity"); render
// those as an em dash so the user can tell it apart from "0% delivered".
const fmtFillPct = (n) => {
  if (n === null || n === undefined) return '—'
  const v = Number(n)
  if (!Number.isFinite(v)) return '—'
  return v.toFixed(1) + '%'
}

// ---------------------------------------------------------------------------
// Last-refresh ticker
// ---------------------------------------------------------------------------
const lastRefreshAt = ref(null)
const nowTick = ref(Date.now())
const refreshAgo = computed(() => {
  if (!lastRefreshAt.value) return '—'
  const sec = Math.max(0, Math.floor((nowTick.value - lastRefreshAt.value) / 1000))
  if (sec < 5) return 'just now'
  if (sec < 60) return `${sec}s ago`
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`
  return `${Math.floor(sec / 3600)}h ago`
})

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------
const fetchAll = async () => {
  const jobs = [
    overviewQ.refresh(), queuesQ.refresh(), consumersQ.refresh(),
    opsQ.refresh(), retentionQ.refresh(),
  ]
  // The two cell-level sources are operator routes: for anyone else the proxy
  // answers 404 route_blocked, so we don't ask and don't render their rows.
  if (can('operator')) jobs.push(statusQ.refresh(), sysQ.refresh())
  await Promise.all(jobs)
  lastRefreshAt.value = Date.now()
}

// One shared ticker for the whole app (paused while the tab is hidden) —
// a private setInterval here would keep spending the tenant's rate budget.
useAutoRefresh(fetchAll)

watch(selectedRange, () => {
  opsQ.refresh()
  retentionQ.refresh()
  if (can('operator')) { statusQ.refresh(); sysQ.refresh() }
})

let tickInterval = null
onMounted(() => {
  fetchAll()
  tickInterval = setInterval(() => { nowTick.value = Date.now() }, 1000)
})
onUnmounted(() => {
  if (tickInterval) clearInterval(tickInterval)
})
</script>

<style scoped>
/* ---------------------------------------------------------------------------
   Top bar
   --------------------------------------------------------------------------- */
.dash-bar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
  flex-wrap: wrap;
}
.dash-live {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 11px;
  font-family: 'JetBrains Mono', monospace;
  color: var(--text-mid);
}
.dash-bar-right {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 12px;
}
.dash-refresh-tick {
  font-size: 11px;
  font-family: 'JetBrains Mono', monospace;
  color: var(--text-low);
}
.dash-master-toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px 4px 8px;
  border-radius: 6px;
  border: 1px solid var(--bd);
  background: var(--ink-2);
  color: var(--text-mid);
  font-size: 11px;
  font-family: 'JetBrains Mono', monospace;
  cursor: pointer;
  transition: color .12s var(--ease), border-color .12s var(--ease), background .12s var(--ease);
}
.dash-master-toggle:hover {
  color: var(--text-hi);
  border-color: var(--bd-hi);
  background: var(--ink-3);
}

/* ---------------------------------------------------------------------------
   Counts strip — cluster scope (left) + point-in-time perf stats (right).
   Splits into two visual groups so the eye reads "what we have" separately
   from "how the engine is performing right now". Both are snapshot stats,
   no time series — they live here so the metric table below can be
   exclusively about charts.
   --------------------------------------------------------------------------- */
.counts-strip {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 14px 24px;
  padding: 12px 16px;
  margin-bottom: 14px;
  border: 1px solid var(--bd);
  background: var(--ink-2);
  border-radius: 8px;
}
.counts-group {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 10px;
}
.counts-group-right {
  /* Slightly muted so the eye reads cluster counts first; perf stats
     are still important but secondary. */
  color: var(--text-low);
}
.count-item-label {
  font-family: 'JetBrains Mono', monospace;
  font-size: 10.5px;
  letter-spacing: .08em;
  text-transform: uppercase;
  color: var(--text-low);
  margin-right: 2px;
}
.count-item {
  display: inline-flex;
  align-items: baseline;
  gap: 6px;
  background: transparent;
  border: none;
  padding: 0;
  color: var(--text-mid);
  font-size: 12.5px;
  cursor: pointer;
  transition: color .12s var(--ease);
}
.count-item.count-static { cursor: default; }
.count-item.count-tight { gap: 4px; }
.count-item:not(:disabled):not(.count-static):hover { color: var(--text-hi); }
.count-item:not(:disabled):not(.count-static):hover strong { color: var(--accent); }
.count-item:disabled { opacity: .5; cursor: default; }
.count-item strong {
  font-family: 'JetBrains Mono', monospace;
  font-variant-numeric: tabular-nums;
  font-size: 14px;
  font-weight: 600;
  color: var(--text-hi);
  letter-spacing: -.005em;
}
.counts-group-right .count-item strong {
  /* Perf stats are secondary, so use medium contrast instead of high.
     The numbers still read clearly but don't compete with the cluster
     scope on the left. */
  color: var(--text-mid);
  font-size: 13px;
}
.count-suffix {
  font-size: 10.5px;
  font-family: 'JetBrains Mono', monospace;
  color: var(--text-low);
  letter-spacing: .04em;
  text-transform: uppercase;
}
.count-muted strong { color: var(--text-mid); }
.count-sep {
  color: var(--bd-hi);
  font-size: 11px;
  user-select: none;
}

/* ---------------------------------------------------------------------------
   Metric table — the heart of the redesign.
   --------------------------------------------------------------------------- */
.metric-table {
  border: 1px solid var(--bd);
  background: var(--ink-2);
  border-radius: 8px;
  overflow: hidden;
  margin-bottom: 14px;
}
.metric-head {
  display: grid;
  grid-template-columns: 14px 150px 160px 1fr 260px 24px;
  gap: 16px;
  padding: 9px 16px;
  font-size: 10.5px;
  font-weight: 600;
  letter-spacing: .08em;
  text-transform: uppercase;
  color: var(--text-low);
  border-bottom: 1px solid var(--bd);
  background: rgba(255, 255, 255, .015);
}
.metric-head .h-value { text-align: right; }
.metric-head .h-spark { text-align: left; font-family: 'JetBrains Mono', monospace; letter-spacing: .04em; text-transform: lowercase; }
@media (max-width: 1100px) {
  .metric-head {
    grid-template-columns: 14px 140px 140px 1fr 180px 24px;
    gap: 12px;
  }
}
@media (max-width: 900px) {
  .metric-head {
    grid-template-columns: 14px 1fr auto 110px 24px;
  }
  .metric-head > :nth-child(4) { display: none; }
}

/* Cell section divider inside the metric table. Same amber vocabulary the
   sidebar's CELL group uses, so "this is not your tenant's number" reads the
   same wherever it appears. */
.metric-cell-head {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 16px;
  font-size: 11px;
  color: var(--text-low);
  border-top: 1px solid var(--bd);
  border-bottom: 1px solid var(--bd);
  background: rgba(230, 180, 80, .05);
}
.metric-cell-tag {
  font-size: 9.5px;
  font-weight: 600;
  letter-spacing: .12em;
  color: var(--warn-400);
  border: 1px solid var(--warn-400);
  border-radius: 3px;
  padding: 1px 5px;
  white-space: nowrap;
}
.count-scope {
  font-style: normal;
  font-size: 9px;
  letter-spacing: .1em;
  text-transform: uppercase;
  color: var(--warn-400);
  border: 1px solid var(--warn-400);
  border-radius: 3px;
  padding: 0 3px;
  margin-left: 4px;
  opacity: .8;
}
.entity-failed {
  color: var(--ember-400);
  font-style: italic;
}

/* The slot-defined value separators — used by compound rows like "avg / max" */
:deep(.mr-sep) {
  color: var(--text-low);
  margin: 0 4px;
  font-weight: 400;
}
:deep(.mr-unit) {
  font-style: normal;
  color: var(--text-low);
  margin-left: 3px;
  font-size: 11px;
  font-weight: 400;
}

/* ---------------------------------------------------------------------------
   Bottom panels — symmetric entity rows.

   One idiom, two panels. Each row is a 2-line card: the head carries
   the leading severity dot, the entity name (left), and the right-side
   numeric metric; the meta line below carries either a depth bar with
   counts (queues) or a tag + counts (consumer groups). The two panels
   render the same .entity-row class so they read as a single visual
   system on the dashboard.
   --------------------------------------------------------------------------- */
.entity-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 10px 6px;
}

.entity-row {
  display: block;
  width: 100%;
  text-align: left;
  border: 1px solid var(--bd);
  border-radius: 8px;
  background: transparent;
  padding: 9px 12px 10px;
  cursor: pointer;
  transition: border-color .12s var(--ease), background .12s var(--ease);
}
:global(.dark) .entity-row { background: rgba(255, 255, 255, .012); }
:global(.light) .entity-row { background: rgba(10, 10, 10, .012); }
.entity-row:hover { border-color: var(--bd-hi); }
:global(.dark) .entity-row:hover { background: rgba(255, 255, 255, .03); }
:global(.light) .entity-row:hover { background: rgba(10, 10, 10, .03); }

.entity-head {
  display: flex;
  align-items: center;
  gap: 8px;
}
.entity-name {
  flex: 1;
  font-size: 13.5px;
  font-weight: 500;
  color: var(--text-hi);
  letter-spacing: -.005em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.entity-right {
  font-family: 'JetBrains Mono', monospace;
  font-variant-numeric: tabular-nums;
  font-size: 12px;
  color: var(--text-mid);
  white-space: nowrap;
  flex-shrink: 0;
}

.entity-meta {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-top: 6px;
  padding-left: 14px;  /* indent under the dot so the meta line aligns with name */
}
.entity-meta .bar-meta {
  flex: 0 1 160px;
  width: 160px;
  height: 4px;
}
.meta-text {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: var(--text-low);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.meta-text strong {
  color: var(--text-mid);
  font-weight: 600;
}
.meta-sep { color: var(--bd-hi); margin: 0 4px; }
.meta-tag {
  display: inline-block;
  padding: 1px 6px;
  border-radius: 99px;
  border: 1px solid var(--bd);
  background: var(--ink-3);
  color: var(--text-mid);
  font-size: 10px;
  font-weight: 500;
  letter-spacing: .02em;
  margin-right: 2px;
}

.entity-empty {
  padding: 32px 16px;
  text-align: center;
  color: var(--text-low);
  font-size: 13px;
}

.card-foot {
  border-top: 1px solid var(--bd);
  padding: 8px 14px;
  display: flex;
  justify-content: flex-end;
}
.card-foot-link {
  font-size: 11.5px;
  color: var(--text-mid);
  cursor: pointer;
  letter-spacing: -.005em;
  transition: color .12s var(--ease);
}
.card-foot-link:hover { color: var(--text-hi); }
</style>
