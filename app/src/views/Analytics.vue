<template>
  <div class="view-container">
    <PerformanceNavigation />

    <PageHead title="Performance">
      <template #sub><span :title="rangeUtcTitle">{{ rangeLabel }}</span></template>
      <template #range>
        <div class="seg" role="group" aria-label="Time range">
          <button
            v-for="r in timeRanges"
            :key="r.value"
            :class="{ on: selectedRange === r.value && !customMode }"
            :aria-pressed="selectedRange === r.value && !customMode ? 'true' : 'false'"
            @click="selectQuickRange(r.value)"
          >{{ r.label }}</button>
          <button :class="{ on: customMode }" :aria-pressed="customMode ? 'true' : 'false'" @click="toggleCustomMode">Custom</button>
        </div>
      </template>
    </PageHead>

    <PageTools>
      <!-- No free entry: the watcher below drops a queue this cluster does not
           list, so accepting a typed name would only make it vanish. -->
      <label class="tool-field" for="an-queue-filter">
        <span class="tool-label">Queue</span>
        <Autocomplete
          id="an-queue-filter"
          v-model="queueFilter"
          :options="queueNames"
          :loading="queuesFirstLoad"
          label="Queue"
          placeholder="All"
        />
      </label>
      <label class="tool-field">
        <span class="tool-label">Namespace</span>
        <select v-model="namespaceFilter" class="input">
          <option value="">All</option>
          <option v-for="ns in namespaceOptions" :key="ns.namespace" :value="ns.namespace">{{ ns.namespace || 'Default' }}</option>
        </select>
      </label>
      <label class="tool-field">
        <span class="tool-label">Task</span>
        <select v-model="taskFilter" class="input">
          <option value="">All</option>
          <option v-for="t in taskOptions" :key="t.task" :value="t.task">{{ t.task || 'Default' }}</option>
        </select>
      </label>
      <template v-if="hasActiveFilter">
        <span v-if="!scopeUnavailable" class="tool-note">{{ scopedQueues.length }} queue{{ scopedQueues.length === 1 ? '' : 's' }} match</span>
        <button class="btn btn-ghost" @click="clearFilters">Clear</button>
      </template>
      <RouterLink v-if="queueFilter" class="btn btn-ghost" :to="queueLocation(queueFilter, route, 'overview', windowQuery(currentRange()))">Inspect queue →</RouterLink>
    </PageTools>

    <!-- A window of your own: the only range that waits for Apply, because a
         half-typed date must not re-scope the page. -->
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

    <!-- A filter the queue list could not be fetched for cannot be applied.
         Say so instead of showing tenant-wide numbers under a filter. -->
    <div v-if="scopeUnavailable" class="panel-err an-block">
      Namespace / task filtering needs the queue list, which failed to load
      ({{ queuesErrorText }}). The panels below are not narrowed to that filter.
    </div>

    <!-- ===================== RANGE-BOUNDED PANELS ===================== -->

    <!-- The range's totals. Everything on this strip follows the range. -->
    <div v-if="opsUnavailable" class="panel-err an-block">{{ opsErrorText }}</div>
    <div v-else class="counts-tiles an-block" aria-label="Totals for the selected range">
      <div class="count-tile"><span class="k">Ingested</span><span class="v">{{ metric(rangeTotals.ingested) }}</span></div>
      <div class="count-tile"><span class="k">Delivered</span><span class="v">{{ metric(rangeTotals.delivered) }}</span></div>
      <div class="count-tile"><span class="k">Acked</span><span class="v">{{ metric(rangeTotals.acked) }}</span></div>
      <div class="count-tile" :title="ackFailTitle">
        <span class="k">Ack failures</span><span class="v" :class="ackFailTone">{{ metric(rangeTotals.ackFailed) }}</span>
      </div>
      <div class="count-tile"><span class="k">Empty polls</span><span class="v">{{ metric(rangeTotals.emptyPolls) }}</span></div>
      <div class="count-tile" title="The worst pop-to-publish delay in the range">
        <span class="k">Peak lag</span>
        <span class="v">{{ rangeTotals.maxLagMs === null ? '—' : formatDuration(rangeTotals.maxLagMs) }}</span>
      </div>
    </div>

    <div class="card" style="margin-bottom:16px;">
      <div class="card-header">
        <h3>Message flow</h3>
        <span class="card-sub">messages per bucket</span>
        <span class="muted">{{ stamp(ops) }}</span>
      </div>
      <div class="card-body">
        <div v-if="opsFirstLoad" class="skeleton" style="height:280px;" />
        <div v-else-if="opsUnavailable" class="panel-err">{{ opsErrorText }}</div>
        <BaseChart
          v-else-if="flowChart.labels.length"
          type="line"
          :data="flowChart"
          :options="flowOptions"
          height="280px"
        />
        <div v-else class="panel-msg">No traffic recorded in this range for the selected scope.</div>
      </div>
    </div>

    <div class="an-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>Top queues by volume</h3>
          <span class="card-sub">ingested and acked</span>
          <span class="muted">{{ stamp(ops) }}</span>
        </div>
        <div class="card-body">
          <div v-if="opsFirstLoad" class="skeleton" style="height:240px;" />
          <div v-else-if="opsUnavailable" class="panel-err">{{ opsErrorText }}</div>
          <BaseChart
            v-else-if="topQueuesChart.labels.length"
            type="bar"
            :data="topQueuesChart"
            :options="barOptions"
            height="240px"
          />
          <div v-else class="panel-msg">No queue moved a message in this range.</div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Backlog right now</h3>
          <span class="card-sub">the range does not apply</span>
          <span class="muted">{{ stamp(queueList) }}</span>
        </div>
        <div class="card-body">
          <div v-if="queuesFailed" class="panel-err">{{ queuesErrorText }}</div>
          <template v-else>
            <div class="stat-grid stat-grid-2">
              <div class="stat">
                <div class="stat-label">Queues</div>
                <div class="stat-value">{{ queuesFirstLoad ? '—' : formatNumber(scopedQueues.length) }}</div>
              </div>
              <div class="stat">
                <div class="stat-label">Messages stored</div>
                <div class="stat-value">{{ metric(backlog.total) }}</div>
              </div>
              <div class="stat">
                <div class="stat-label">Pending</div>
                <div class="stat-value">{{ metric(backlog.pending) }}</div>
              </div>
              <div class="stat">
                <div class="stat-label">Processing</div>
                <div class="stat-value">{{ metric(backlog.processing) }}</div>
              </div>
            </div>
            <!-- Waiting against leased, to scale. Absent when both are zero:
                 an empty bar would be a shape standing for nothing. -->
            <div v-if="backlogSplit" class="an-split">
              <div class="an-split-bar" role="img" :aria-label="backlogSplit.label">
                <span v-if="backlogSplit.pending" class="an-split-p" :style="{ flexGrow: backlogSplit.pending }" />
                <span v-if="backlogSplit.processing" class="an-split-q" :style="{ flexGrow: backlogSplit.processing }" />
              </div>
              <div class="an-split-key">
                <span><i class="an-key-p" />Pending {{ backlogSplit.pendingPct }}</span>
                <span><i class="an-key-q" />Processing {{ backlogSplit.processingPct }}</span>
              </div>
            </div>
            <p v-else-if="!queuesFirstLoad" class="an-note">Nothing pending or in flight right now.</p>
            <!-- The queue reader pages; the totals above cover the page we hold,
                 so say it rather than understate a large tenant silently. -->
            <p v-if="queuePageFull" class="an-note">
              Showing the first {{ QUEUE_PAGE_LIMIT }} queues — this tenant has more, and the
              figures above cover only those {{ QUEUE_PAGE_LIMIT }}.
            </p>
          </template>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { useRoute } from 'vue-router'
import { useRouteState } from '@/composables/useRouteState'
import { useRouteRange, rangeMinutes } from '@/composables/useRouteRange'
import { queueLocation, windowQuery } from '@/composables/navigation'
const route = useRoute()
import { computed, ref, watch } from 'vue'

import BaseChart from '@/components/BaseChart.vue'
import { analytics, describeApiError, resources, system } from '@/api'
import {
  formatDuration, formatNumber, toNum, trimIncompleteBuckets, useApi,
} from '@/composables/useApi'
import { stateColor } from '@/composables/useChartTheme'
import { ackFailureSeverity, numTone } from '@/composables/useSeverity'
import {
  formatChartLabel, formatDateTimeLocal, formatTimestampRange, formatTimestampRangeUtc,
  formatTimestampUtc, isMultiDay, validateRange,
} from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import Autocomplete from '@/components/Autocomplete.vue'
import PerformanceNavigation from '@/components/PerformanceNavigation.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'

// TENANT PAGE. Every source here is tenant-scoped broker-side:
//   /api/v1/analytics/queue-ops  — per (bucket, queue) push/pop/ack in a range
//   /api/v1/status/queues        — the live backlog watermark per queue
//   /api/v1/resources/{namespaces,tasks} — filter options
// The old primary call, bare /api/v1/status, is a cell-level aggregate the
// proxy blocks and the broker never scopes; nothing on this page may come from
// it, so its lifetime "messages/leases/DLQ/workers" panels are gone rather
// than shown as this tenant's.

const QUEUE_PAGE_LIMIT = 500

const timeRanges = [
  { label: '1h', value: '1h' },
  { label: '6h', value: '6h' },
  { label: '24h', value: '24h' },
  { label: '7d', value: '7d' },
]

const selectedRange = ref('1h')
const customMode = ref(false)
const customFrom = ref('')
const customTo = ref('')
// The range the data on screen was actually fetched for — only Apply moves it,
// so a half-typed custom range never silently re-scopes the panels.
const appliedCustom = ref(null)

const queueFilter = ref('')
const namespaceFilter = ref('')
const taskFilter = ref('')
useRouteState({ queue: queueFilter, namespace: namespaceFilter, task: taskFilter })
useRouteRange({ range: selectedRange, customMode, customFrom, customTo, appliedCustom, reload: () => fetchAll() })

// ---------------------------------------------------------------------------
// Range
// ---------------------------------------------------------------------------
const QUICK_MINUTES = { '1h': 60, '6h': 360, '24h': 1440, '7d': 10080 }

function currentRange() {
  if (customMode.value && appliedCustom.value) return appliedCustom.value
  const to = new Date()
  const from = new Date(to.getTime() - (QUICK_MINUTES[selectedRange.value] || rangeMinutes(selectedRange.value)) * 60_000)
  return { from, to }
}

const rangeLabel = computed(() => {
  if (customMode.value && appliedCustom.value) {
    const { from, to } = appliedCustom.value
    return formatTimestampRange(from, to)
  }
  const r = timeRanges.find(t => t.value === selectedRange.value)
  return `last ${r ? r.label : selectedRange.value}`
})
const rangeUtcTitle = computed(() => (
  customMode.value && appliedCustom.value
    ? formatTimestampRangeUtc(appliedCustom.value.from, appliedCustom.value.to)
    : ''
))

// Live, not on-click: an invalid range explains itself as it is typed instead
// of leaving the user with a button that does nothing when pressed.
const customError = computed(() => validateRange(customFrom.value, customTo.value).error || '')
const customRangeValid = computed(() => !customError.value)

const selectQuickRange = (value) => {
  customMode.value = false
  selectedRange.value = value
  fetchAll()
}

const toggleCustomMode = () => {
  customMode.value = !customMode.value
  if (customMode.value) {
    const now = new Date()
    const from = new Date(now.getTime() - (QUICK_MINUTES[selectedRange.value] || rangeMinutes(selectedRange.value)) * 60_000)
    customTo.value = formatDateTimeLocal(now)
    customFrom.value = formatDateTimeLocal(from)
  } else {
    appliedCustom.value = null
    fetchAll()
  }
}

const applyCustomRange = () => {
  const parsed = validateRange(customFrom.value, customTo.value)
  if (parsed.error) return
  appliedCustom.value = { from: parsed.from, to: parsed.to }
  fetchAll()
}

// ---------------------------------------------------------------------------
// Fetchers. Each panel owns its own state so one failure cannot blank the page.
// ---------------------------------------------------------------------------
const ops = useApi((config) => {
  const { from, to } = currentRange()
  const params = { from: from.toISOString(), to: to.toISOString() }
  // queue-ops honours `queue` server-side; namespace/task do not exist on that
  // SP, so they are applied by name against the (server-filtered) queue list.
  if (queueFilter.value) params.queue = queueFilter.value
  return system.getQueueOps(params, config)
})

const queueList = useApi((config) => {
  const params = { limit: QUEUE_PAGE_LIMIT }
  if (namespaceFilter.value) params.namespace = namespaceFilter.value
  if (taskFilter.value) params.task = taskFilter.value
  return analytics.getQueues(params, config)
})

const namespacesApi = useApi((config) => resources.getNamespaces(config))
const tasksApi = useApi((config) => resources.getTasks(config))

const fetchAll = () => {
  ops.refresh()
  queueList.refresh()
  namespacesApi.refresh()
  tasksApi.refresh()
}

useRefresh(fetchAll)

watch([queueFilter, namespaceFilter, taskFilter], () => {
  ops.refresh()
  queueList.refresh()
})

const clearFilters = () => {
  queueFilter.value = ''
  namespaceFilter.value = ''
  taskFilter.value = ''
}

// ---------------------------------------------------------------------------
// Scope
// ---------------------------------------------------------------------------
const queues = computed(() => queueList.data.value?.queues || [])
const queueNames = computed(() => queues.value.map(q => q.name).filter(Boolean).sort())
const namespaceOptions = computed(() => namespacesApi.data.value?.namespaces || [])
const taskOptions = computed(() => tasksApi.data.value?.tasks || [])

const queuePageFull = computed(() => queues.value.length >= QUEUE_PAGE_LIMIT)
const hasActiveFilter = computed(
  () => !!(queueFilter.value || namespaceFilter.value || taskFilter.value)
)
const groupFilterActive = computed(() => !!(namespaceFilter.value || taskFilter.value))
// A namespace/task filter is resolved through the queue list. If that list is
// unavailable the filter cannot be honoured — never quietly widen the scope.
const scopeUnavailable = computed(() => groupFilterActive.value && queueList.failed.value)

/** Queue names the active filter admits, or null when nothing is narrowed. */
const allowedQueueNames = computed(() => {
  if (groupFilterActive.value) {
    const names = queues.value
      .map(q => q.name)
      .filter(n => !queueFilter.value || n === queueFilter.value)
    return new Set(names)
  }
  if (queueFilter.value) return new Set([queueFilter.value])
  return null
})

const scopedQueues = computed(() => {
  const allow = allowedQueueNames.value
  return allow ? queues.value.filter(q => allow.has(q.name)) : queues.value
})

// ---------------------------------------------------------------------------
// Panel state
// ---------------------------------------------------------------------------
const opsFirstLoad = computed(() => ops.loading.value && !ops.data.value)
const queuesFirstLoad = computed(() => queueList.loading.value && !queueList.data.value)
const queuesFailed = computed(() => queueList.failed.value)
const queuesErrorText = computed(() => describeApiError(queueList.error.value))
const opsUnavailable = computed(() => ops.failed.value || scopeUnavailable.value)
const opsErrorText = computed(() => {
  if (ops.failed.value) return describeApiError(ops.error.value)
  return 'Cannot narrow to the selected namespace / task — the queue list is unavailable.'
})

/** A number we hold, or an em dash. Never a 0 standing in for "unknown". */
const metric = (v) => (v === null || v === undefined ? '—' : formatNumber(v))

// ---------------------------------------------------------------------------
// Derived series
// ---------------------------------------------------------------------------
const opsRows = computed(() => {
  const payload = ops.data.value
  if (!payload) return []
  const rows = trimIncompleteBuckets(payload.series || [], {
    bucketKey: 'bucket',
    bucketMinutes: payload.bucketMinutes || 1,
  })
  const allow = allowedQueueNames.value
  return allow ? rows.filter(r => allow.has(r.queueName)) : rows
})

const flowSeries = computed(() => {
  const byBucket = new Map()
  for (const r of opsRows.value) {
    const acc = byBucket.get(r.bucket) || { ingested: 0, delivered: 0, acked: 0 }
    acc.ingested += toNum(r.pushMessages) || 0
    acc.delivered += toNum(r.popMessages) || 0
    acc.acked += toNum(r.ackSuccess) || 0
    byBucket.set(r.bucket, acc)
  }
  return [...byBucket.entries()]
    .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
    .map(([bucket, v]) => ({ bucket, ...v }))
})

const flowChart = computed(() => {
  const rows = flowSeries.value
  if (!rows.length) return { labels: [], datasets: [] }
  const stamps = rows.map(r => r.bucket)
  const multiDay = isMultiDay(stamps)
  return {
    labels: stamps.map(t => formatChartLabel(new Date(t), multiDay)),
    datasets: [
      { label: 'Ingested', data: rows.map(r => r.ingested), fill: true },
      { label: 'Delivered', data: rows.map(r => r.delivered), fill: false },
      { label: 'Acked', data: rows.map(r => r.acked), fill: false },
    ],
  }
})

const rangeTotals = computed(() => {
  const t = { ingested: 0, delivered: 0, acked: 0, ackFailed: 0, emptyPolls: 0, maxLagMs: null }
  if (opsUnavailable.value || !ops.data.value) {
    return { ...t, ingested: null, delivered: null, acked: null, ackFailed: null, emptyPolls: null }
  }
  for (const r of opsRows.value) {
    t.ingested += toNum(r.pushMessages) || 0
    t.delivered += toNum(r.popMessages) || 0
    t.acked += toNum(r.ackSuccess) || 0
    t.ackFailed += toNum(r.ackFailed) || 0
    t.emptyPolls += toNum(r.popEmpty) || 0
    const lag = toNum(r.maxLagMs)
    if (lag !== null) t.maxLagMs = t.maxLagMs === null ? lag : Math.max(t.maxLagMs, lag)
  }
  return t
})

const topQueuesChart = computed(() => {
  const byQueue = new Map()
  for (const r of opsRows.value) {
    const acc = byQueue.get(r.queueName) || { ingested: 0, acked: 0 }
    acc.ingested += toNum(r.pushMessages) || 0
    acc.acked += toNum(r.ackSuccess) || 0
    byQueue.set(r.queueName, acc)
  }
  const top = [...byQueue.entries()]
    .filter(([, v]) => v.ingested > 0 || v.acked > 0)
    .sort((a, b) => (b[1].ingested + b[1].acked) - (a[1].ingested + a[1].acked))
    .slice(0, 8)
  if (!top.length) return { labels: [], datasets: [] }
  return {
    labels: top.map(([name]) => (name.length > 25 ? `${name.slice(0, 25)}…` : name)),
    datasets: [
      {
        label: 'Ingested',
        data: top.map(([, v]) => v.ingested),
        backgroundColor: stateColor('Ingested').fill,
      },
      {
        label: 'Acked',
        data: top.map(([, v]) => v.acked),
        backgroundColor: stateColor('Processed').fill,
      },
    ],
  }
})

const backlog = computed(() => {
  if (queueList.failed.value || !queueList.data.value) {
    return { total: null, pending: null, processing: null }
  }
  return scopedQueues.value.reduce(
    (acc, q) => {
      acc.total += toNum(q.messages?.total) || 0
      acc.pending += toNum(q.messages?.pending) || 0
      acc.processing += toNum(q.messages?.processing) || 0
      return acc
    },
    { total: 0, pending: 0, processing: 0 },
  )
})

// Rounding never claims all or nothing when it is not: 99.9% is ">99%".
const pct = (part, whole) => {
  const v = (part / whole) * 100
  if (v > 0 && v < 1) return '<1%'
  if (v > 99 && v < 100) return '>99%'
  return `${Math.round(v)}%`
}

const backlogSplit = computed(() => {
  const pending = backlog.value.pending || 0
  const processing = backlog.value.processing || 0
  const whole = pending + processing
  if (!whole) return null
  const pendingPct = pct(pending, whole)
  const processingPct = pct(processing, whole)
  return {
    pending,
    processing,
    pendingPct,
    processingPct,
    label: `Pending ${pendingPct}, processing ${processingPct}`,
  }
})

// Amber and coral mean what they mean on the Overview: a share of the window's
// ack attempts, judged by useSeverity, never "more than zero".
const ackFailSev = computed(() => ackFailureSeverity({
  failed: rangeTotals.value.ackFailed,
  succeeded: rangeTotals.value.acked,
}))
const ackFailTone = computed(() => numTone(ackFailSev.value))
const ackFailTitle = computed(() => {
  const f = rangeTotals.value.ackFailed
  const ok = rangeTotals.value.acked
  if (!f || ok === null) return ''
  return `${pct(f, f + ok)} of ack attempts failed in the range`
})

// ---------------------------------------------------------------------------
// Chart options
// ---------------------------------------------------------------------------
const flowOptions = {
  plugins: { legend: { display: true, position: 'top' } },
  scales: { y: { title: { display: true, text: 'Messages per bucket', font: { size: 11 } } } },
}

const barOptions = {
  plugins: { legend: { display: true, position: 'top' } },
  scales: {
    x: { stacked: true },
    y: { stacked: true, beginAtZero: true, title: { display: true, text: 'Messages', font: { size: 11 } } },
  },
}
</script>

<style scoped>
/* Everything shared with the other nine views now lives in style.css:
   .scope-strip*, the .filter-* card family, .stat-grid*, .panel-err and
   .panel-msg. What is left below is Analytics' own layout. */

/* The secondary panel pair. Carries the 16px rhythm to the snapshot card
   below it — separation is always a margin-bottom on the preceding block. */
.an-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }

.an-note { margin: 14px 0 0; font-size: 12px; line-height: 1.5; color: var(--text-low); }
.an-block { margin-bottom: 16px; }

.an-split { margin-top: 18px; display: flex; flex-direction: column; gap: 8px; }
.an-split-bar { display: flex; gap: 2px; height: 6px; }
.an-split-bar span { flex-basis: 0; min-width: 2px; border-radius: 1px; }
.an-split-p, .an-key-p { background: var(--series-1); }
.an-split-q, .an-key-q { background: var(--series-3); }
.an-split-key { display: flex; gap: 16px; flex-wrap: wrap; font-size: 12px; color: var(--text-mid); font-variant-numeric: tabular-nums; }
.an-split-key span { display: inline-flex; align-items: center; gap: 6px; }
.an-split-key i { width: 8px; height: 8px; border-radius: 1px; }

@media (max-width: 1100px) {
  .an-grid-2 { grid-template-columns: 1fr; }
}
</style>
