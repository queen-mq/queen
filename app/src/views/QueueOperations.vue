<template>
  <div class="view-container">

    <!-- Controls: time range picker. The "Source" toggle that used to live
         here disappeared when this view became its own page — the only
         data source is queue-operations, so the toggle had no second
         option to offer. -->
    <div class="card" style="padding:12px 16px; margin-bottom:20px;">
      <div style="display:flex; flex-wrap:wrap; align-items:center; gap:12px 20px;">
        <div style="display:flex; align-items:center; gap:8px; margin-left:auto;">
          <span style="font-size:11px; font-weight:500; color:var(--text-low);">Range</span>
          <div class="seg">
            <button
              v-for="range in timeRanges"
              :key="range.value"
              @click="selectQuickRange(range.value)"
              :class="{ on: timeRange === range.value && !customMode }"
            >{{ range.label }}</button>
            <button
              @click="toggleCustomMode"
              :class="{ on: customMode }"
            >Custom</button>
          </div>
        </div>
      </div>

      <!-- Custom Date/Time Range -->
      <div v-if="customMode" style="display:flex; flex-wrap:wrap; align-items:center; gap:12px; padding-top:12px; margin-top:12px; border-top:1px solid var(--bd);">
        <div style="display:flex; align-items:center; gap:8px;">
          <label style="font-size:12px; font-weight:500; color:var(--text-low); white-space:nowrap;">From:</label>
          <input
            type="datetime-local"
            v-model="customFrom"
            class="input font-mono" style="font-size:13px; padding:6px 10px; width:auto;"
          />
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
          <label style="font-size:12px; font-weight:500; color:var(--text-low); white-space:nowrap;">To:</label>
          <input
            type="datetime-local"
            v-model="customTo"
            class="input font-mono" style="font-size:13px; padding:6px 10px; width:auto;"
          />
        </div>
        <button
          @click="applyCustomRange"
          class="btn btn-primary" style="font-size:12px;"
        >Apply</button>
        <p v-if="rangeError" class="range-error">{{ rangeError }}</p>
      </div>
    </div>

    <!-- Loading skeleton — shown only on first fetch; subsequent refreshes
         leave the previous data on screen for a smooth update. -->
    <div v-if="loading">
      <div style="display:grid; grid-template-columns:repeat(2,1fr); gap:16px;">
        <div v-for="i in 4" :key="i" class="card" style="padding:24px;">
          <div class="skeleton" style="height:192px; width:100%; border-radius:var(--r-card);" />
        </div>
      </div>
    </div>

    <template v-else>
      <!-- Nothing loaded at all. Without this the page rendered blank and the
           user had no way to tell a broken cluster from an empty one. -->
      <div v-if="allFailed" class="card" style="padding:24px;">
        <p style="font-weight:600; margin-bottom:8px; color:var(--ember-400);">
          Cannot load queue operations
        </p>
        <p style="font-size:13px; color:var(--text-mid);">{{ describeApiError(opsError) }}</p>
        <div style="margin-top:16px;">
          <button class="btn btn-ghost" @click="fetchData">Retry</button>
        </div>
      </div>

      <template v-else>
        <!-- ==================================================================
             TENANT. Everything in this block is scoped to the acting cluster's
             tenant by the proxy — these are your queues and nobody else's.
             ================================================================== -->

        <!-- Retention & Eviction Jobs -->
        <div class="card" style="margin-bottom:16px;">
          <div class="card-header">
            <h3>Retention &amp; Eviction Jobs</h3>
            <span class="muted">messages deleted by retention / eviction workers</span>
          </div>
          <div class="card-body">
            <div v-if="retentionError" class="panel-na">{{ describeApiError(retentionError) }}</div>
            <div v-else-if="retentionChartData.labels.length > 0">
              <div style="display:flex; gap:24px; flex-wrap:wrap; margin-bottom:14px;">
                <div class="stat" style="padding:8px 12px;">
                  <div class="stat-label">Retention</div>
                  <div class="stat-value font-mono">{{ formatNumber(retentionTotals?.retentionMsgs || 0) }}</div>
                </div>
                <div class="stat" style="padding:8px 12px;">
                  <div class="stat-label">Completed retention</div>
                  <div class="stat-value font-mono">{{ formatNumber(retentionTotals?.completedRetentionMsgs || 0) }}</div>
                </div>
                <div class="stat" style="padding:8px 12px;">
                  <div class="stat-label">Eviction</div>
                  <div class="stat-value font-mono">{{ formatNumber(retentionTotals?.evictionMsgs || 0) }}</div>
                </div>
                <div class="stat" style="padding:8px 12px;">
                  <div class="stat-label">Events</div>
                  <div class="stat-value font-mono">{{ formatNumber(retentionTotals?.eventCount || 0) }}</div>
                </div>
              </div>
              <BaseChart
                type="bar"
                :data="retentionChartData"
                :options="retentionChartOptions"
                height="240px"
              />
            </div>
            <!-- queen.retention_history has no writer under the log engine, so
                 an empty series means NOT REPORTED. Saying "no events" here
                 would assert nothing was deleted while segments are being
                 swept — the opposite of what is happening. -->
            <div v-else class="panel-na">
              Retention accounting is not recorded by the log engine yet, so this
              cannot be shown. It is not a claim that nothing was deleted.
            </div>
          </div>
        </div>

        <!-- Top Queues leaderboard (window snapshot) -->
        <div class="card" style="margin-bottom:16px;">
          <div class="card-header">
            <h3>Top Queues</h3>
            <span class="muted">averaged across the selected window — what to look at first</span>
          </div>
          <div class="card-body">
            <div v-if="opsError" class="panel-na">{{ describeApiError(opsError) }}</div>
            <div v-else-if="hasTopQueueData" class="top-queues-grid">
              <!-- Push rate -->
              <div>
                <h4 class="label-xs" style="margin-bottom:8px;">By Push/s</h4>
                <table v-if="topQueues.push.length" class="t top-queues">
                  <tbody>
                    <tr v-for="(row, i) in topQueues.push" :key="`push-${row.queue}`">
                      <td class="rank">{{ i + 1 }}</td>
                      <td class="qname" :title="row.queue">{{ row.queue }}</td>
                      <td class="font-mono tabular-nums val">{{ formatRate(row.push) }}</td>
                    </tr>
                  </tbody>
                </table>
                <div v-else class="empty-tile">No push activity</div>
              </div>
              <!-- Pop rate -->
              <div>
                <h4 class="label-xs" style="margin-bottom:8px;">By Pop/s</h4>
                <table v-if="topQueues.pop.length" class="t top-queues">
                  <tbody>
                    <tr v-for="(row, i) in topQueues.pop" :key="`pop-${row.queue}`">
                      <td class="rank">{{ i + 1 }}</td>
                      <td class="qname" :title="row.queue">{{ row.queue }}</td>
                      <td class="font-mono tabular-nums val">{{ formatRate(row.pop) }}</td>
                    </tr>
                  </tbody>
                </table>
                <div v-else class="empty-tile">No pop activity</div>
              </div>
              <!-- Parked (waiting consumers) -->
              <div>
                <h4 class="label-xs" style="margin-bottom:8px;">By Parked (waiting consumers)</h4>
                <table v-if="topQueues.parked.length" class="t top-queues">
                  <tbody>
                    <tr v-for="(row, i) in topQueues.parked" :key="`parked-${row.queue}`">
                      <td class="rank">{{ i + 1 }}</td>
                      <td class="qname" :title="row.queue">{{ row.queue }}</td>
                      <td class="font-mono tabular-nums val">{{ formatParked(row.parked) }}</td>
                    </tr>
                  </tbody>
                </table>
                <div v-else class="empty-tile">No parked long-polls</div>
              </div>
              <!-- Lag -->
              <div>
                <h4 class="label-xs" style="margin-bottom:8px;">By Avg Lag</h4>
                <table v-if="topQueues.lag.length" class="t top-queues">
                  <tbody>
                    <tr v-for="(row, i) in topQueues.lag" :key="`lag-${row.queue}`">
                      <td class="rank">{{ i + 1 }}</td>
                      <td class="qname" :title="row.queue">{{ row.queue }}</td>
                      <td class="font-mono tabular-nums val">{{ formatDurationMs(row.lag) }}</td>
                    </tr>
                  </tbody>
                </table>
                <div v-else class="empty-tile">No measured lag</div>
              </div>
            </div>
            <div v-else class="empty-tile">No queue activity in this range</div>
          </div>
        </div>

        <!-- Per-Queue Metrics -->
        <div class="card" style="margin-bottom:16px;">
          <div class="card-header">
            <h3>Per-Queue Metrics</h3>
          </div>
          <div class="card-body">
            <div v-if="opsError" class="panel-na">{{ describeApiError(opsError) }}</div>
            <template v-else-if="availableQueues.length > 0">
              <div style="display:flex; align-items:center; gap:8px; margin-bottom:12px;">
                <span style="font-size:12px; color:var(--text-low); white-space:nowrap;">Filter queues:</span>
                <MultiSelect
                  v-model="selectedQueues"
                  :options="availableQueues"
                  placeholder="All queues"
                  search-placeholder="Search queues…"
                />
                <span v-if="selectedQueues.length > 0" style="font-size:12px; color:var(--text-low);">
                  {{ selectedQueues.length }} of {{ availableQueues.length }}
                </span>
                <!-- Per-queue view-mode toggle: only meaningful for the Parked
                     tab today (cluster-aggregate vs per-replica). Hidden on
                     other ops to keep the surface tidy. -->
                <div v-if="selectedQueueOp === 'parked'" style="display:flex; align-items:center; gap:8px; margin-left:auto;">
                  <span style="font-size:11px; font-weight:500; color:var(--text-low);">View</span>
                  <div class="seg">
                    <button
                      @click="viewMode = 'aggregate'"
                      :class="{ on: viewMode === 'aggregate' }"
                    >Cluster</button>
                    <button
                      @click="viewMode = 'individual'"
                      :class="{ on: viewMode === 'individual' }"
                    >Per Replica</button>
                  </div>
                </div>
              </div>
              <!-- Op selector: choose which per-queue chart to show -->
              <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-bottom:14px;">
                <span style="font-size:12px; color:var(--text-low); white-space:nowrap;">Op:</span>
                <button
                  v-for="op in queueOpTabs"
                  :key="op.key"
                  @click="selectedQueueOp = op.key"
                  style="display:inline-flex; align-items:center; gap:6px; padding:3px 10px; border-radius:var(--r-pill); font-size:11px; font-weight:500; cursor:pointer; border:1px solid var(--bd-hi); transition:.15s;"
                  :class="selectedQueueOp === op.key ? op.activeClass : 'opacity-50'"
                >
                  <span style="width:6px; height:6px; border-radius:var(--r-pill);" :style="{ background: selectedQueueOp === op.key ? op.activeDot : 'var(--text-faint)' }" />
                  {{ op.label }}
                </button>
              </div>
              <div>
                <h4 class="label-xs" style="margin-bottom:10px;">
                  {{ queueOpActive.label }} by Queue<span v-if="isParkedIndividual"> &amp; Replica</span>
                  <span v-if="queueOpActive.kind === 'rate'" style="color:var(--text-low); font-weight:normal;">(per second)</span>
                  <span v-else-if="queueOpActive.kind === 'rate-signed'" style="color:var(--text-low); font-weight:normal;">(push − pop messages/s; positive = backlog filling, negative = draining)</span>
                  <span v-else-if="queueOpActive.kind === 'percent'" style="color:var(--text-low); font-weight:normal;">(long-polls returning a message ÷ all long-poll completions; gaps = quiet bucket)</span>
                  <span v-else-if="isParkedIndividual" style="color:var(--text-low); font-weight:normal;">(in-flight long-polls, per replica, averaged each minute)</span>
                  <span v-else-if="queueOpActive.kind === 'gauge'" style="color:var(--text-low); font-weight:normal;">(in-flight long-polls, averaged each minute)</span>
                </h4>
                <BaseChart
                  v-if="perQueueChartData.labels.length > 0"
                  :key="`per-queue-ops-${queueOpActive.key}-${isParkedIndividual ? 'individual' : 'aggregate'}-${hasExplicitQueueSelection ? 'legend' : 'nolegend'}`"
                  type="line"
                  :data="perQueueChartData"
                  :options="perQueueThroughputOptions"
                  height="340px"
                />
                <div v-else class="empty-tile">No per-queue data for this op yet</div>
              </div>
              <div style="margin-top:20px;">
                <h4 class="label-xs" style="margin-bottom:10px;">
                  Avg Latency by Queue
                  <span style="color:var(--text-low); font-weight:normal;">(measured at pop; gaps = buckets with no pops)</span>
                </h4>
                <BaseChart
                  v-if="queueLagChartData.labels.length > 0"
                  :key="`per-queue-lag-${hasExplicitQueueSelection ? 'legend' : 'nolegend'}`"
                  type="line"
                  :data="queueLagChartData"
                  :options="perQueueLagOptions"
                  height="340px"
                />
                <div v-else class="empty-tile">No pops in this range, so no latency was measured</div>
              </div>
            </template>
            <div v-else class="empty-tile">
              <p>No per-queue metrics recorded yet.</p>
              <p style="font-size:12px; margin-top:4px;">Per-queue data is collected as soon as the broker flushes its first minute boundary.</p>
            </div>
          </div>
        </div>

        <!-- Partitions per queue -->
        <div class="card" style="margin-bottom:16px;">
          <div class="card-header">
            <h3>Partitions</h3>
            <span class="muted">count &amp; creation / deletion rate per queue</span>
          </div>
          <div class="card-body">
            <div v-if="opsError" class="panel-na">{{ describeApiError(opsError) }}</div>
            <template v-else-if="partitionCountChartData.labels.length > 0 || partitionRateChartData.labels.length > 0">
              <div>
                <h4 class="label-xs" style="margin-bottom:10px;">Partition count by Queue</h4>
                <BaseChart
                  v-if="partitionCountChartData.labels.length > 0"
                  :key="`per-queue-partitions-${hasExplicitQueueSelection ? 'legend' : 'nolegend'}`"
                  type="line"
                  :data="partitionCountChartData"
                  :options="perQueuePartitionCountOptions"
                  height="280px"
                />
                <div v-else class="empty-tile">No partition-count snapshots yet</div>
              </div>
              <div style="margin-top:20px;">
                <h4 class="label-xs" style="margin-bottom:10px;">Partition creation / deletion rate (events per bucket, across all queues)</h4>
                <BaseChart
                  v-if="partitionRateChartData.labels.length > 0"
                  type="bar"
                  :data="partitionRateChartData"
                  :options="partitionRateChartOptions"
                  height="240px"
                />
                <div v-else class="empty-tile">No partition create/delete events in this range</div>
              </div>
            </template>
            <div v-else class="empty-tile">
              <p>No partition metrics recorded yet.</p>
            </div>
          </div>
        </div>

        <!-- ==================================================================
             CELL · OPERATOR. Everything below comes from
             /api/v1/analytics/worker-metrics, which is cell-wide by nature
             (worker lifetime counters across every tenant) and which the proxy
             answers 404 for any non-operator. It is not this tenant's data and
             the header says so.
             ================================================================== -->
        <template v-if="can('operator')">
          <div class="cell-section">
            <span class="cell-tag">CELL · OPERATOR</span>
            <span>
              broker-wide worker figures — every tenant on this cell, not just
              {{ actingTenantSlug || 'this tenant' }}
            </span>
          </div>

          <div v-if="workerError" class="card" style="margin-bottom:16px;">
            <div class="card-body">
              <div class="panel-na">{{ describeApiError(workerError) }}</div>
            </div>
          </div>

          <template v-else>
            <!-- Throughput Chart -->
            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Message Throughput <span class="cell-chip">cell</span></h3>
                <span class="muted">{{ workerData?.pointCount || 0 }} data points</span>
              </div>
              <div class="card-body">
                <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-bottom:14px;">
                  <button
                    v-for="metric in throughputMetrics"
                    :key="metric.key"
                    @click="toggleThroughputMetric(metric.key)"
                    style="display:inline-flex; align-items:center; gap:6px; padding:3px 10px; border-radius:var(--r-pill); font-size:11px; font-weight:500; cursor:pointer; border:1px solid var(--bd-hi); transition:.15s;"
                    :class="selectedThroughputMetrics[metric.key] ? metric.activeClass : 'opacity-50'"
                  >
                    <span style="width:6px; height:6px; border-radius:var(--r-pill);" :style="{ background: selectedThroughputMetrics[metric.key] ? metric.activeDot : 'var(--text-faint)' }" />
                    {{ metric.label }}
                  </button>
                </div>
                <BaseChart
                  v-if="throughputChartData.labels.length > 0"
                  type="line"
                  :data="throughputChartData"
                  :options="throughputChartOptions"
                  height="280px"
                />
                <div v-else class="empty-tile">No throughput data available</div>
              </div>
            </div>

            <!-- Message Latency -->
            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Message Latency <span class="cell-chip">cell</span></h3>
                <span class="muted">time from push to pop</span>
              </div>
              <div class="card-body">
                <BaseChart
                  v-if="latencyChartData.labels.length > 0"
                  type="line"
                  :data="latencyChartData"
                  :options="lagChartOptions"
                  height="240px"
                />
                <div v-else class="empty-tile">No latency data available</div>
              </div>
            </div>

            <!-- Event Loop -->
            <!-- The Connection Pool and Job Queue Depth panels that used to sit
                 beside this one are gone: they charted worker_metrics columns
                 (avg_free_slots / db_connections / avg_job_queue_size) that the
                 Rust broker never writes, so they were flat zeros presented as
                 measurements. Real pool numbers live in system_metrics and are
                 on the Dashboard's DB pool row. -->
            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Event Loop Latency <span class="cell-chip">cell</span></h3>
              </div>
              <div class="card-body">
                <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-bottom:14px;">
                  <button
                    v-for="metric in eventLoopMetrics"
                    :key="metric.key"
                    @click="toggleEventLoopMetric(metric.key)"
                    style="display:inline-flex; align-items:center; gap:6px; padding:3px 10px; border-radius:var(--r-pill); font-size:11px; font-weight:500; cursor:pointer; border:1px solid var(--bd-hi); transition:.15s;"
                    :class="selectedEventLoopMetrics[metric.key] ? metric.activeClass : 'opacity-50'"
                  >
                    <span style="width:6px; height:6px; border-radius:var(--r-pill);" :style="{ background: selectedEventLoopMetrics[metric.key] ? metric.activeDot : 'var(--text-faint)' }" />
                    {{ metric.label }}
                  </button>
                </div>
                <BaseChart
                  v-if="eventLoopChartData.labels.length > 0"
                  type="line"
                  :data="eventLoopChartData"
                  :options="lagChartOptions"
                  height="200px"
                />
                <div v-else class="empty-tile">No event loop data available</div>
              </div>
            </div>

            <!-- Errors Chart -->
            <!-- No "DB errors" series: `db_errors` is declared and written but
                 never incremented anywhere in the broker, so the bar was a
                 constant zero wearing the name of a real failure mode. -->
            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Errors <span class="cell-chip">cell</span></h3>
                <span v-if="totalErrors > 0" class="chip chip-bad" style="margin-left:auto;">{{ totalErrors }} in period</span>
              </div>
              <div class="card-body">
                <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-bottom:14px;">
                  <button
                    v-for="metric in errorMetrics"
                    :key="metric.key"
                    @click="toggleErrorMetric(metric.key)"
                    style="display:inline-flex; align-items:center; gap:6px; padding:3px 10px; border-radius:var(--r-pill); font-size:11px; font-weight:500; cursor:pointer; border:1px solid var(--bd-hi); transition:.15s;"
                    :class="selectedErrorMetrics[metric.key] ? metric.activeClass : 'opacity-50'"
                  >
                    <span style="width:6px; height:6px; border-radius:var(--r-pill);" :style="{ background: selectedErrorMetrics[metric.key] ? metric.activeDot : 'var(--text-faint)' }" />
                    {{ metric.label }}
                  </button>
                </div>
                <BaseChart
                  v-if="errorsChartData.labels.length > 0"
                  type="bar"
                  :data="errorsChartData"
                  :options="errorsChartOptions"
                  height="200px"
                />
                <div v-else class="empty-tile">No error data available</div>
              </div>
            </div>

            <!-- Dead-letter queue rate -->
            <!-- DLQ messages have already failed retries enough times to be
                 moved aside — they're silent business failures (bad payload,
                 downstream crash) and the response is investigation, not infra
                 fix. That's a different on-call workflow than the generic
                 Errors panel above, which is why this chart is separate. -->
            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Dead Letter Queue <span class="cell-chip">cell</span></h3>
                <span v-if="dlqTotal > 0" class="chip chip-bad" style="margin-left:auto;">
                  {{ formatNumber(dlqTotal) }} in period
                </span>
                <span v-else class="muted">messages moved to DLQ</span>
              </div>
              <div class="card-body">
                <BaseChart
                  v-if="dlqChartData.labels.length > 0"
                  type="bar"
                  :data="dlqChartData"
                  :options="dlqChartOptions"
                  height="180px"
                />
                <div v-else class="empty-tile" style="color:var(--ok-500);">
                  No DLQ events on this cell in this period
                </div>
              </div>
            </div>

            <!-- Workers Status Panel -->
            <!-- Free Slots / DB Conn / Job Queue columns removed with the panels
                 above: same never-written worker_metrics columns. -->
            <div v-if="workerData?.workers?.length" class="card">
              <div class="card-header">
                <h3>Workers Status <span class="cell-chip">cell</span></h3>
                <span class="muted">{{ workerData.workers.length }} workers</span>
              </div>
              <div class="card-body">
                <div style="overflow-x:auto;">
                  <table class="t">
                    <thead>
                      <tr>
                        <th>Worker ID</th>
                        <th>Hostname</th>
                        <th style="text-align:right;">Avg EL</th>
                        <th style="text-align:right;">Max EL</th>
                        <th>Last Seen</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr
                        v-for="worker in workerData.workers"
                        :key="worker.workerId"
                      >
                        <td style="font-weight:500;">{{ worker.workerId }}</td>
                        <td style="color:var(--text-mid);">{{ worker.hostname }}</td>
                        <td style="text-align:right;">
                          <span class="font-mono tabular-nums" :class="{ 'val-warn': worker.avgEventLoopLagMs > 100 }">
                            {{ worker.avgEventLoopLagMs }}ms
                          </span>
                        </td>
                        <td style="text-align:right;">
                          <span class="font-mono tabular-nums" :class="{ 'val-bad': worker.maxEventLoopLagMs > 500 }">
                            {{ worker.maxEventLoopLagMs }}ms
                          </span>
                        </td>
                        <td style="font-size:12px; color:var(--text-low);">{{ formatTime(worker.lastSeen) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </template>
        </template>
      </template>
    </template>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, watch } from 'vue'
import { system, operator as operatorApi, describeApiError } from '@/api'
import { toNum, trimIncompleteBuckets, formatNumber } from '@/composables/useApi'
import { formatChartLabel, formatDateTimeLocal, isMultiDay, validateRange } from '@/composables/useFormat'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { chartColor, chartTheme, semanticColors, alpha } from '@/composables/useChartTheme'
import BaseChart from '@/components/BaseChart.vue'
import MultiSelect from '@/components/MultiSelect.vue'

const { can, actingTenantSlug } = useIdentity()
const { notifyWarn } = useToast()

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const loading = ref(true)
const timeRange = ref(60)
const customMode = ref(false)
const customFrom = ref('')
const customTo = ref('')
const rangeError = ref(null)

// CELL-LEVEL. worker-metrics carries broker-wide worker counters with no
// tenant column; the proxy classifies it Operator and answers 404 to everyone
// else, so it is fetched only for a live operator and every panel built from
// it is labelled.
const workerData = ref(null)
const workerError = ref(null)
// TENANT-SCOPED.
const queueOpsData = ref(null)
const queueOpsError = ref(null)
const retentionError = ref(null)
// Per-replica parked breakdown — only fetched when the Parked tab is active
// and the per-queue view is set to 'individual'. Null otherwise so the
// aggregate path is unaffected.
const queueParkedReplicasData = ref(null)
const retentionData = ref(null)

const selectedQueues = ref([])
const selectedQueueOp = ref('pop')
// Cluster-aggregate ('aggregate') vs per-replica ('individual'); only
// affects the Parked tab. Lives at this scope (not inside queueOpTabs)
// so the Parked-tab toggle can drive a refetch via the watcher below.
const viewMode = ref('aggregate')

const timeRanges = [
  { label: '15m', value: 15 },
  { label: '1h', value: 60 },
  { label: '6h', value: 360 },
  { label: '24h', value: 1440 }
]

// ---------------------------------------------------------------------------
// Formatters. `formatDurationMs` says its unit in its name — three different
// `formatDuration`s used to coexist across these views, two taking ms and one
// taking seconds, which is a live foot-gun for anyone moving code between them.
// ---------------------------------------------------------------------------
const formatDurationMs = (ms) => {
  if (ms === undefined || ms === null) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60000).toFixed(1)}m`
}

const formatTime = (timestamp) => {
  if (!timestamp) return '-'
  return new Date(timestamp).toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
}

const formatRate = (v) => {
  const n = Number(v) || 0
  if (n === 0) return '0/s'
  if (n < 10) return `${n.toFixed(2)}/s`
  if (n < 100) return `${n.toFixed(1)}/s`
  return `${Math.round(n)}/s`
}

const formatParked = (v) => {
  const n = Number(v) || 0
  if (n >= 10) return Math.round(n).toString()
  return n.toFixed(2).replace(/\.?0+$/, '') || '0'
}

// ---------------------------------------------------------------------------
// Time range picker
// ---------------------------------------------------------------------------
const selectQuickRange = (value) => {
  customMode.value = false
  rangeError.value = null
  timeRange.value = value
  fetchData()
}

const toggleCustomMode = () => {
  customMode.value = !customMode.value
  rangeError.value = null
  if (customMode.value) {
    const now = new Date()
    const from = new Date(now.getTime() - timeRange.value * 60 * 1000)
    customTo.value = formatDateTimeLocal(now)
    customFrom.value = formatDateTimeLocal(from)
  }
}

const applyCustomRange = () => {
  // Apply used to `return` on a bad range and look like a dead button.
  const range = validateRange(customFrom.value, customTo.value)
  if (range.error) {
    rangeError.value = range.error
    notifyWarn('Range not applied', range.error)
    return
  }
  rangeError.value = null
  fetchData()
}

const timestampsOf = (rows) => (rows || []).map(r => r.timestamp)

// ---------------------------------------------------------------------------
// Throughput / latency / event loop / errors — chip metric toggles
// ---------------------------------------------------------------------------
// `activeDot` is a CSS colour for the inline dot next to each chip, so it is
// written as a var() the same way the inactive fallback (`var(--text-faint)`)
// already is — no literal to drift from the series ramp it is quoting.
const throughputMetrics = [
  { key: 'push', label: 'Push', activeClass: 'chip-mute', activeDot: 'var(--series-1)' },
  { key: 'pop',  label: 'Pop',  activeClass: 'chip-mute', activeDot: 'var(--series-2)' },
  { key: 'ack',  label: 'Ack',  activeClass: 'chip-ok',   activeDot: 'var(--ok-500)' },
]
const selectedThroughputMetrics = reactive({ push: true, pop: true, ack: true })
const toggleThroughputMetric = (key) => { selectedThroughputMetrics[key] = !selectedThroughputMetrics[key] }

const eventLoopMetrics = [
  { key: 'avg', label: 'Avg Event Loop', activeClass: 'chip-mute', activeDot: 'var(--series-1)' },
  { key: 'max', label: 'Max Event Loop', activeClass: 'chip-bad',  activeDot: 'var(--ember-400)' },
]
const selectedEventLoopMetrics = reactive({ avg: true, max: true })
const toggleEventLoopMetric = (key) => { selectedEventLoopMetrics[key] = !selectedEventLoopMetrics[key] }

// No `dbErrors` entry: server/src/metrics.rs declares the counter and
// db.rs writes it, but nothing in the broker ever increments it — the series
// was a guaranteed zero wearing the name of a real failure mode.
const errorMetrics = [
  { key: 'ackFailed', label: 'Ack Failed', activeClass: 'chip-warn', activeDot: 'var(--warn-400)' },
  { key: 'dlq',       label: 'DLQ',        activeClass: 'chip-bad',  activeDot: 'var(--ember-400)' },
]
const selectedErrorMetrics = reactive({ ackFailed: true, dlq: true })
const toggleErrorMetric = (key) => { selectedErrorMetrics[key] = !selectedErrorMetrics[key] }

// ---------------------------------------------------------------------------
// Chart options (each one annotated where its semantics aren't obvious)
// ---------------------------------------------------------------------------
const lagChartOptions = {
  plugins: {
    legend: { display: false },
    tooltip: {
      callbacks: { label: (ctx) => ` ${ctx.dataset.label}: ${formatDurationMs(ctx.parsed.y)}` }
    }
  },
  scales: {
    y: {
      beginAtZero: true,
      min: 0,
      title: { display: true, text: 'Latency', font: { size: 11 } },
      ticks: { callback: (value) => formatDurationMs(value) }
    }
  }
}

const throughputChartOptions = {
  plugins: { legend: { display: false } },
  scales: { y: { title: { display: true, text: 'Operations/s', font: { size: 11 } } } }
}

const errorsChartOptions = {
  plugins: { legend: { display: false } },
  scales: {
    x: { stacked: true },
    y: {
      stacked: true,
      beginAtZero: true,
      min: 0,
      title: { display: true, text: 'Errors', font: { size: 11 } }
    }
  }
}

const dlqChartOptions = {
  plugins: { legend: { display: false } },
  scales: {
    y: {
      beginAtZero: true,
      min: 0,
      title: { display: true, text: 'DLQ messages / bucket', font: { size: 11 } },
      ticks: { precision: 0 }
    }
  }
}

const retentionChartOptions = {
  plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12, padding: 8, font: { size: 11 } } } },
  scales: {
    x: { stacked: true },
    y: {
      stacked: true,
      beginAtZero: true,
      min: 0,
      title: { display: true, text: 'Messages deleted', font: { size: 11 } }
    }
  }
}

// ---------------------------------------------------------------------------
// Per-queue ops tabs — each tab plots one derived counter on the per-queue
// chart. `kind` distinguishes:
//   - 'rate'        → value is per-second; subtitle says "(per second)".
//   - 'rate-signed' → signed per-second delta (push − pop); y-axis crosses zero.
//   - 'count'       → absolute count over the bucket (transactions).
//   - 'gauge'       → snapshot (parked long-polls); no /sec semantics.
//   - 'percent'     → 0–100% derived ratio (fill ratio).
// `field` is either a string property name or a function `(entry) => number`
// for derived metrics.
// ---------------------------------------------------------------------------
const queueOpTabs = [
  { key: 'pop',    label: 'Pop/s',   activeClass: 'chip-mute', activeDot: 'var(--series-2)', field: 'popPerSecond',   yLabel: 'Pops/s',     kind: 'rate'  },
  { key: 'push',   label: 'Push/s',  activeClass: 'chip-mute', activeDot: 'var(--series-1)', field: 'pushPerSecond',  yLabel: 'Pushes/s',   kind: 'rate'  },
  { key: 'ack',    label: 'Ack/s',   activeClass: 'chip-ok',   activeDot: 'var(--ok-500)',   field: 'ackPerSecond',   yLabel: 'Acks/s',     kind: 'rate'  },
  { key: 'empty',  label: 'Empty/s', activeClass: 'chip-mute', activeDot: 'var(--series-4)', field: 'emptyPerSecond', yLabel: 'Empty/s',    kind: 'rate'  },
  // Fill ratio = popMessages / (popMessages + popEmpty). Returns null
  // (= chart gap) when the bucket has too few completions to compute
  // meaningfully — a once-a-minute blip mustn't read as 100%.
  { key: 'fill',   label: 'Fill %',  activeClass: 'chip-ok',   activeDot: 'var(--ok-500)',
    field: (e) => {
      const pop = toNum(e.popMessages)
      const empty = toNum(e.popEmpty)
      if (pop === null && empty === null) return null
      const total = (pop || 0) + (empty || 0)
      if (total < 2) return null
      return Math.round(((pop || 0) / total) * 1000) / 10
    },
    yLabel: 'Fill %', kind: 'percent' },
  // Signed push − pop rate: positive = backlog growing, negative = draining.
  { key: 'delta',  label: 'Push−Pop Δ', activeClass: 'chip-warn', activeDot: 'var(--warn-400)',
    field: (e) => {
      const push = toNum(e.pushPerSecond)
      const pop  = toNum(e.popPerSecond)
      if (push === null && pop === null) return null
      return Math.round(((push || 0) - (pop || 0)) * 100) / 100
    },
    yLabel: 'Push − Pop (msgs/s)', kind: 'rate-signed' },
  { key: 'trx',    label: 'Trx',     activeClass: 'chip-mute', activeDot: 'var(--warn-400)', field: 'transactions',   yLabel: 'Transactions', kind: 'count' },
  // The parked dot was a hand-picked blue — the app's only chip hue with no
  // token behind it. --info-400 is the slot the palette added for exactly
  // this ("FYI", not a health state), and is the same blue to within 2%.
  { key: 'parked', label: 'Parked',  activeClass: 'chip-mute', activeDot: 'var(--info-400)', field: 'parkedCount',    yLabel: 'Parked',     kind: 'gauge' },
]
const queueOpActive = computed(() => queueOpTabs.find(t => t.key === selectedQueueOp.value) || queueOpTabs[0])
const isParkedIndividual = computed(() =>
  selectedQueueOp.value === 'parked' && viewMode.value === 'individual')

// True when the user has explicitly picked queues via the MultiSelect.
// Drives whether the per-queue charts show the legend (with dozens of
// queues a legend would dominate the chart, so we suppress it by default).
const hasExplicitQueueSelection = computed(() => selectedQueues.value.length > 0)

// Tooltip behaviour shared by every per-queue chart. Two cheap rules cut
// the tooltip back to the queues that matter at a given timestamp:
//   1) `filter` drops items whose value is zero — they convey nothing,
//      and the chart line is already at y=0.
//   2) `itemSort` orders biggest-first so most-active queues are on top.
const queueTooltipBase = {
  filter: (item) => Number(item.parsed.y) > 0,
  itemSort: (a, b) => Number(b.parsed.y) - Number(a.parsed.y),
}

const lagTooltip = {
  ...queueTooltipBase,
  callbacks: { label: (ctx) => ` ${ctx.dataset.label}: ${formatDurationMs(ctx.parsed.y)}` }
}

const perQueuePartitionCountOptions = computed(() => ({
  plugins: {
    legend: { display: hasExplicitQueueSelection.value, position: 'top', labels: { boxWidth: 12, padding: 8, font: { size: 11 } } },
    tooltip: queueTooltipBase
  },
  scales: {
    y: {
      beginAtZero: true,
      min: 0,
      title: { display: true, text: 'Partitions', font: { size: 11 } },
      ticks: { precision: 0 }
    }
  }
}))

const partitionRateChartOptions = {
  plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12, padding: 8, font: { size: 11 } } } },
  scales: {
    y: {
      beginAtZero: true,
      title: { display: true, text: 'Events / bucket', font: { size: 11 } },
      ticks: { precision: 0 }
    }
  }
}

// Per-queue chart options. Y-axis behavior per kind:
//   - rate / count / gauge  → starts at 0
//   - rate-signed (delta)   → auto-scaled, crosses zero (positive = filling,
//                             negative = draining); zero gridline emphasized.
//   - percent (fill ratio)  → fixed 0–100 with % suffix on ticks/tooltip.
const perQueueThroughputOptions = computed(() => {
  const kind = queueOpActive.value.kind
  const yScale = {
    title: { display: true, text: queueOpActive.value.yLabel, font: { size: 11 } }
  }
  let tooltipOpts = queueTooltipBase
  if (kind === 'percent') {
    yScale.beginAtZero = true
    yScale.min = 0
    yScale.max = 100
    yScale.ticks = { callback: (value) => `${value}%` }
    // Override the base tooltip's "drop zero" filter — for fill ratio,
    // 0% is itself meaningful (every long-poll empty). Keep big-first sort.
    tooltipOpts = {
      itemSort: queueTooltipBase.itemSort,
      callbacks: { label: (ctx) => ` ${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(1)}%` }
    }
  } else if (kind === 'rate-signed') {
    // Zero is the line that carries the meaning here (filling vs draining), so
    // it gets a bright rule; every other tick falls back to the same grid
    // token BaseChart paints on every other chart — the old 6%-white mix was
    // a hand-rolled duplicate of --bd and drifted the moment the ramp moved.
    yScale.grid = {
      color: (ctx) => ctx.tick.value === 0 ? alpha(chartColor(0).line, 0.35) : chartTheme.grid,
      lineWidth: (ctx) => ctx.tick.value === 0 ? 1.5 : 1
    }
    tooltipOpts = {
      itemSort: queueTooltipBase.itemSort,
      callbacks: {
        label: (ctx) => {
          const v = Number(ctx.parsed.y)
          const sign = v > 0 ? '+' : ''
          return ` ${ctx.dataset.label}: ${sign}${v.toFixed(2)} msgs/s`
        }
      }
    }
  } else {
    yScale.beginAtZero = true
    yScale.min = 0
    if (kind === 'gauge') yScale.ticks = { precision: 0 }
  }
  return {
    plugins: {
      legend: { display: hasExplicitQueueSelection.value, position: 'top', labels: { boxWidth: 12, padding: 8, font: { size: 11 } } },
      tooltip: tooltipOpts
    },
    scales: { y: yScale }
  }
})

const perQueueLagOptions = computed(() => ({
  plugins: {
    legend: { display: hasExplicitQueueSelection.value, position: 'top', labels: { boxWidth: 12, padding: 8, font: { size: 11 } } },
    tooltip: lagTooltip
  },
  scales: {
    y: {
      beginAtZero: true,
      min: 0,
      title: { display: true, text: 'Latency', font: { size: 11 } },
      ticks: { callback: (value) => formatDurationMs(value) }
    }
  }
}))

// ---------------------------------------------------------------------------
// Per-queue colors come from chartColor() / chartPalette in useChartTheme —
// five greys, no red/green/yellow, because those are reserved for status
// semantics (chips, threshold tones). Distinction stays via legend + tooltip
// rather than color symbolism that would mislead.
//
// This file used to carry a verbatim copy of that ramp as a local
// `queueColors` array, which meant the per-queue charts kept painting the old
// blue-cast greys after the palette moved. Deleted; the call sites below now
// index the real one.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Computed: derived metrics + chart datasets
// ---------------------------------------------------------------------------
const totalErrors = computed(() => {
  if (!workerData.value?.timeSeries?.length) return 0
  return workerData.value.timeSeries.reduce((sum, t) => {
    return sum + (toNum(t.ackFailed) || 0) + (toNum(t.dlqCount) || 0)
  }, 0)
})

const throughputChartData = computed(() => {
  if (!workerData.value?.timeSeries?.length) return { labels: [], datasets: [] }

  const ts = [...workerData.value.timeSeries].reverse()
  const multiDay = isMultiDay(timestampsOf(ts))
  const labels = ts.map(t => formatChartLabel(new Date(t.timestamp), multiDay))

  const datasets = []
  if (selectedThroughputMetrics.push) {
    datasets.push({
      label: 'Push/s', data: ts.map(t => toNum(t.pushPerSecond)),
      borderColor: chartColor(0).line, backgroundColor: alpha(chartColor(0).line, 0.12),
      fill: true, tension: 0
    })
  }
  if (selectedThroughputMetrics.pop) {
    datasets.push({
      label: 'Pop/s', data: ts.map(t => toNum(t.popPerSecond)),
      borderColor: chartColor(1).line, backgroundColor: alpha(chartColor(1).line, 0.12),
      fill: true, tension: 0
    })
  }
  if (selectedThroughputMetrics.ack) {
    datasets.push({
      label: 'Ack/s', data: ts.map(t => toNum(t.ackPerSecond)),
      borderColor: chartColor(2).line, backgroundColor: alpha(chartColor(2).line, 0.12),
      fill: true, tension: 0
    })
  }
  return { labels, datasets }
})

const latencyChartData = computed(() => {
  if (!workerData.value?.timeSeries?.length) return { labels: [], datasets: [] }
  const ts = [...workerData.value.timeSeries].reverse()
  const multiDay = isMultiDay(timestampsOf(ts))
  const labels = ts.map(t => formatChartLabel(new Date(t.timestamp), multiDay))
  return {
    labels,
    datasets: [
      { label: 'Avg Lag (ms)', data: ts.map(t => toNum(t.avgLagMs)),
        borderColor: chartColor(0).line, backgroundColor: chartColor(0).fill,
        fill: true, tension: 0 },
      { label: 'Max Lag (ms)', data: ts.map(t => toNum(t.maxLagMs)),
        borderColor: chartColor(1).line, fill: false, tension: 0 }
    ]
  }
})

const eventLoopChartData = computed(() => {
  if (!workerData.value?.timeSeries?.length) return { labels: [], datasets: [] }
  const ts = [...workerData.value.timeSeries].reverse()
  const multiDay = isMultiDay(timestampsOf(ts))
  const labels = ts.map(t => formatChartLabel(new Date(t.timestamp), multiDay))
  const datasets = []
  if (selectedEventLoopMetrics.avg) {
    datasets.push({
      label: 'Avg Event Loop (ms)', data: ts.map(t => toNum(t.avgEventLoopLagMs)),
      borderColor: chartColor(0).line, backgroundColor: alpha(chartColor(0).line, 0.12),
      fill: true, tension: 0
    })
  }
  if (selectedEventLoopMetrics.max) {
    datasets.push({
      label: 'Max Event Loop (ms)', data: ts.map(t => toNum(t.maxEventLoopLagMs)),
      borderColor: chartColor(1).line, backgroundColor: chartColor(1).fill,
      fill: true, tension: 0
    })
  }
  return { labels, datasets }
})

// The Connection Pool and Job Queue Depth datasets are gone with their panels:
// avg_free_slots / db_connections / avg_job_queue_size / max_job_queue_size are
// not in insert_worker_metrics' column list (server/src/db.rs), so they held
// their DDL default of 0 forever. The real pool gauges live in
// queen.system_metrics and are charted by the Dashboard's DB pool row.

const errorsChartData = computed(() => {
  if (!workerData.value?.timeSeries?.length) return { labels: [], datasets: [] }
  const ts = [...workerData.value.timeSeries].reverse()
  const multiDay = isMultiDay(timestampsOf(ts))
  const labels = ts.map(t => formatChartLabel(new Date(t.timestamp), multiDay))
  const datasets = []
  if (selectedErrorMetrics.ackFailed) {
    datasets.push({
      label: 'Ack Failed', data: ts.map(t => toNum(t.ackFailed)),
      backgroundColor: alpha(chartColor(1).line, 0.6), borderColor: chartColor(1).line, borderWidth: 1
    })
  }
  if (selectedErrorMetrics.dlq) {
    datasets.push({
      label: 'DLQ', data: ts.map(t => toNum(t.dlqCount)),
      backgroundColor: alpha(chartColor(0).line, 0.6), borderColor: chartColor(0).line, borderWidth: 1
    })
  }
  return { labels, datasets }
})

// DLQ-only chart. Reuses dlqCount from the worker-metrics time series, but
// gets its own panel because DLQ events have a different operational
// meaning than generic errors (retry-exhausted business failures, not
// infra). Drops bars at zero rather than draw a flat line so spikes in a
// quiet window pop visually.
const dlqChartData = computed(() => {
  if (!workerData.value?.timeSeries?.length) return { labels: [], datasets: [] }
  const ts = [...workerData.value.timeSeries].reverse()
  const data = ts.map(t => toNum(t.dlqCount))
  if (data.every(v => v === null || v === 0)) return { labels: [], datasets: [] }
  const multiDay = isMultiDay(timestampsOf(ts))
  const labels = ts.map(t => formatChartLabel(new Date(t.timestamp), multiDay))
  return {
    labels,
    datasets: [{
      label: 'DLQ', data,
      backgroundColor: alpha(semanticColors.badStrong.line, 0.6),
      borderColor: semanticColors.badStrong.line, borderWidth: 1
    }]
  }
})

const dlqTotal = computed(() => {
  if (!workerData.value?.timeSeries?.length) return 0
  return workerData.value.timeSeries.reduce((sum, t) => sum + (toNum(t.dlqCount) || 0), 0)
})

// Retention / eviction time series
const retentionTotals = computed(() => retentionData.value?.totals || null)
const retentionChartData = computed(() => {
  const rows = retentionData.value?.series || []
  if (!rows.length) return { labels: [], datasets: [] }
  const multiDay = rows.length >= 2 &&
    new Date(rows[0].bucket).toDateString() !== new Date(rows[rows.length-1].bucket).toDateString()
  const labels = rows.map(r => formatChartLabel(new Date(r.bucket), multiDay))
  return {
    labels,
    datasets: [
      { label: 'Retention',           data: rows.map(r => toNum(r.retentionMsgs)),
        backgroundColor: alpha(chartColor(0).line, 0.6), borderColor: chartColor(0).line, borderWidth: 1 },
      { label: 'Completed retention', data: rows.map(r => toNum(r.completedRetentionMsgs)),
        backgroundColor: alpha(chartColor(1).line, 0.6), borderColor: chartColor(1).line, borderWidth: 1 },
      // Eviction is the one series here that carries a warning, not just a
      // third slot in the ramp — it keeps the warn hue.
      { label: 'Eviction',            data: rows.map(r => toNum(r.evictionMsgs)),
        backgroundColor: alpha(semanticColors.warn.line, 0.5), borderColor: semanticColors.warn.line, borderWidth: 1 },
    ]
  }
})

// ---------------------------------------------------------------------------
// Per-queue ops time series helpers — pivots queue_ops data.
// ---------------------------------------------------------------------------
const buildPerQueueOpsChart = (valueAccessor) => {
  const raw = queueOpsData.value?.series || []
  if (!raw.length) return { labels: [], datasets: [] }
  const bucketSet = new Set(raw.map(r => r.bucket))
  const buckets = [...bucketSet].sort()
  const multiDay = buckets.length >= 2 &&
    new Date(buckets[0]).toDateString() !== new Date(buckets[buckets.length - 1]).toDateString()
  const labels = buckets.map(b => formatChartLabel(new Date(b), multiDay))

  const queuesAvailable = [...new Set(raw.map(r => r.queueName))]
  const pick = selectedQueues.value.length > 0
    ? queuesAvailable.filter(q => selectedQueues.value.includes(q))
    : queuesAvailable
  pick.sort()

  const lookup = {}
  raw.forEach(r => { lookup[`${r.queueName}|${r.bucket}`] = r })

  const datasets = pick.map((q, i) => {
    const color = chartColor(i)
    return {
      label: q,
      // valueAccessor may return null to indicate "not enough data this
      // bucket" (e.g. fill ratio with <2 events). Preserve null so
      // Chart.js renders a gap. Likewise: missing (queue, bucket) row →
      // null so quiet queues don't draw a phantom flat line at zero.
      data: buckets.map(b => {
        const entry = lookup[`${q}|${b}`]
        if (!entry) return null
        const v = valueAccessor(entry)
        if (v === null || v === undefined) return null
        const n = Number(v)
        return Number.isFinite(n) ? n : null
      }),
      borderColor: color.line,
      backgroundColor: color.fill,
      fill: false,
      tension: 0,
      spanGaps: true
    }
  })
  return { labels, datasets }
}

const queueOpsRateChartData = computed(() => {
  const field = queueOpActive.value.field
  // `field` may be a string property name on the bucket entry, or a derived
  // accessor function for tabs like Fill % and Push−Pop Δ. We normalize
  // here so buildPerQueueOpsChart only ever sees a function.
  const accessor = typeof field === 'function' ? field : (e => e[field])
  return buildPerQueueOpsChart(accessor)
})

// Per-(queue × replica) breakdown for the Parked tab in individual mode.
// Used as-is when both: Parked tab active AND view mode == individual.
// Each line keyed "<queue> @ <hostname>:<workerId>" so a queue running
// across N replicas yields N lines.
const queueParkedReplicaChartData = computed(() => {
  const raw = queueParkedReplicasData.value?.series || []
  if (!raw.length) return { labels: [], datasets: [] }

  const filterQueues = selectedQueues.value.length > 0
    ? new Set(selectedQueues.value)
    : null
  const filtered = filterQueues
    ? raw.filter(r => filterQueues.has(r.queueName))
    : raw

  const bucketSet = new Set(filtered.map(r => r.bucket))
  const buckets = [...bucketSet].sort()
  const multiDay = buckets.length >= 2 &&
    new Date(buckets[0]).toDateString() !== new Date(buckets[buckets.length - 1]).toDateString()
  const labels = buckets.map(b => formatChartLabel(new Date(b), multiDay))

  const seriesKey = r => `${r.queueName}@${r.hostname}:${r.workerId}`
  const seriesSet = new Set(filtered.map(seriesKey))
  const series = [...seriesSet].sort()

  const lookup = {}
  for (const r of filtered) lookup[`${seriesKey(r)}|${r.bucket}`] = r

  const datasets = series.map((s, i) => {
    const color = chartColor(i)
    return {
      label: s,
      // No entry for this (series, bucket) → null so the chart renders a
      // gap rather than an artificial 0 on the line.
      data: buckets.map(b => {
        const entry = lookup[`${s}|${b}`]
        return entry ? toNum(entry.parkedCount) : null
      }),
      borderColor: color.line,
      backgroundColor: color.fill,
      fill: false,
      tension: 0
    }
  })
  return { labels, datasets }
})

// The chart picks per-replica data only when Parked tab is active *and*
// the per-queue view mode is 'individual'. Everything else uses the
// cluster-aggregated path.
const perQueueChartData = computed(() => {
  if (selectedQueueOp.value === 'parked' && viewMode.value === 'individual') {
    return queueParkedReplicaChartData.value
  }
  return queueOpsRateChartData.value
})

// Partition count snapshot per queue (line chart over time, ~one value per minute)
const partitionCountChartData = computed(() => buildPerQueueOpsChart(e => e.partitionCount))

// Partition create / delete rate across all queues (bar chart summed per bucket)
const partitionRateChartData = computed(() => {
  const raw = queueOpsData.value?.series || []
  if (!raw.length) return { labels: [], datasets: [] }
  const bucketSet = new Set(raw.map(r => r.bucket))
  const buckets = [...bucketSet].sort()
  const multiDay = buckets.length >= 2 &&
    new Date(buckets[0]).toDateString() !== new Date(buckets[buckets.length - 1]).toDateString()
  const labels = buckets.map(b => formatChartLabel(new Date(b), multiDay))

  const created = {}
  const deleted = {}
  buckets.forEach(b => { created[b] = 0; deleted[b] = 0 })
  for (const r of raw) {
    created[r.bucket] = (created[r.bucket] || 0) + (Number(r.partitionsCreated) || 0)
    deleted[r.bucket] = (deleted[r.bucket] || 0) + (Number(r.partitionsDeleted) || 0)
  }

  const hasActivity = buckets.some(b => created[b] > 0 || deleted[b] > 0)
  if (!hasActivity) return { labels: [], datasets: [] }

  return {
    labels,
    datasets: [
      { label: 'Created', data: buckets.map(b => created[b]),
        backgroundColor: alpha(chartColor(0).line, 0.5), borderColor: chartColor(0).line, borderWidth: 1 },
      { label: 'Deleted', data: buckets.map(b => -deleted[b]),
        backgroundColor: alpha(chartColor(1).line, 0.5), borderColor: chartColor(1).line, borderWidth: 1 },
    ]
  }
})

// ---------------------------------------------------------------------------
// Per-queue lag — read off the SAME queue-ops payload as everything else.
//
// The old /api/v1/analytics/queue-lag fetch is gone: get_queue_lag_v1 returns
// raw per-minute rows with no rollup and no LIMIT, so a 7-day custom range
// over 200 queues answered with millions of objects in one response. queue-ops
// already carries avgLagMs / maxLagMs per (queue, bucket) at the range's
// natural rollup, so there is nothing the extra call could add.
// ---------------------------------------------------------------------------
const availableQueues = computed(() => [...(queueOpsData.value?.queues || [])].sort())

// Lag is measured AT POP. A bucket with no pops has no measurement — charting
// its 0 would draw a flat healthy line under a stalled queue.
const queueLagChartData = computed(() => buildPerQueueOpsChart(e => {
  const pops = toNum(e.popMessages) || 0
  return pops > 0 ? toNum(e.avgLagMs) : null
}))

// ---------------------------------------------------------------------------
// Top queues leaderboard
// ---------------------------------------------------------------------------
// Compresses the per-queue spaghetti chart into 4 sortable mini-tables:
// which queues are pushing / popping / parking / lagging the most across
// the selected window. Computed from the same queue-ops series the
// per-queue chart consumes — no extra fetches.
//
// Aggregation rules per metric:
//   - pushPerSecond / popPerSecond  → window average (rate semantics)
//   - parkedCount                   → window average (gauge semantics)
//   - avgLagMs                      → pop-count-weighted average (a queue
//                                     with one slow pop in 60 minutes
//                                     shouldn't dominate the lag chart;
//                                     weighting by pop_count gives the
//                                     "typical message lag" the user feels).
const TOP_QUEUES_LIMIT = 5

const topQueues = computed(() => {
  const series = queueOpsData.value?.series || []
  if (!series.length) return null

  const agg = new Map()
  for (const r of series) {
    let a = agg.get(r.queueName)
    if (!a) {
      a = { queue: r.queueName, n: 0,
            pushSum: 0, popSum: 0, parkedSum: 0,
            lagWeightedSum: 0, popCount: 0 }
      agg.set(r.queueName, a)
    }
    a.n += 1
    a.pushSum   += Number(r.pushPerSecond) || 0
    a.popSum    += Number(r.popPerSecond)  || 0
    a.parkedSum += Number(r.parkedCount)   || 0
    const popMsgs = Number(r.popMessages) || 0
    a.popCount       += popMsgs
    a.lagWeightedSum += (Number(r.avgLagMs) || 0) * popMsgs
  }

  const rows = []
  for (const a of agg.values()) {
    rows.push({
      queue:  a.queue,
      push:   a.n > 0 ? a.pushSum / a.n   : 0,
      pop:    a.n > 0 ? a.popSum  / a.n   : 0,
      parked: a.n > 0 ? a.parkedSum / a.n : 0,
      lag:    a.popCount > 0 ? a.lagWeightedSum / a.popCount : 0,
      popCount: a.popCount
    })
  }

  const top = (key, opts = {}) => {
    const filter = opts.requirePops ? rows.filter(r => r.popCount > 0) : rows
    return [...filter]
      .filter(r => Number(r[key]) > 0)
      .sort((a, b) => b[key] - a[key])
      .slice(0, TOP_QUEUES_LIMIT)
  }

  return {
    push:   top('push'),
    pop:    top('pop'),
    parked: top('parked'),
    // Lag leaderboard only includes queues that actually saw pops in the
    // window — otherwise idle queues with stale max_lag readings poison
    // the ranking.
    lag:    top('lag', { requirePops: true })
  }
})

const hasTopQueueData = computed(() => {
  const t = topQueues.value
  if (!t) return false
  return t.push.length > 0 || t.pop.length > 0 || t.parked.length > 0 || t.lag.length > 0
})

// ---------------------------------------------------------------------------
// Fetcher.
//
// Every call is caught individually and every failure has a home in the
// template. The old version left worker-metrics — the one endpoint the proxy
// blocks for non-operators — unguarded inside the Promise.all, so its 404
// rejected the whole batch and the template, which gated on `workerData` with
// no `v-else`, rendered nothing at all. A blank page is the worst error state
// there is: it is indistinguishable from an idle cluster.
// ---------------------------------------------------------------------------
const fetchData = async () => {
  // Show skeleton only on first fetch; refreshes leave the previous
  // data on screen for a smooth update.
  if (!queueOpsData.value && !workerData.value) loading.value = true

  let from, to
  if (customMode.value && customFrom.value && customTo.value) {
    const range = validateRange(customFrom.value, customTo.value)
    if (range.error) {
      rangeError.value = range.error
      loading.value = false
      return
    }
    from = range.from
    to = range.to
  } else {
    const now = new Date()
    from = new Date(now.getTime() - timeRange.value * 60 * 1000)
    to = now
  }
  const params = { from: from.toISOString(), to: to.toISOString() }

  const wantParkedReplicas = selectedQueueOp.value === 'parked'
    && viewMode.value === 'individual'

  const settle = (p) => p.then(
    res => ({ data: res.data, error: null }),
    err => ({ data: null, error: err }),
  )

  const [workerRes, queueOpsRes, retentionRes, parkedReplicasRes] = await Promise.all([
    // CELL-LEVEL, operator-only. Asking as a tenant only earns a 404.
    can('operator')
      ? settle(operatorApi.getWorkerMetrics(params))
      : Promise.resolve({ data: null, error: null }),
    settle(system.getQueueOps(params)),
    settle(system.getRetention(params)),
    wantParkedReplicas
      ? settle(system.getQueueParkedReplicas(params))
      : Promise.resolve({ data: null, error: null }),
  ])

  // Trim the in-flight current bucket from each time series so the right edge
  // of every chart isn't dragged towards zero by partial samples. Worker /
  // per-queue / parked / retention data share the once-per-minute cadence.
  workerError.value = workerRes.error
  if (workerRes.data) {
    workerData.value = {
      ...workerRes.data,
      timeSeries: trimIncompleteBuckets(workerRes.data.timeSeries || [], {
        bucketKey: 'timestamp',
        bucketMinutes: workerRes.data.bucketMinutes || 1,
      }),
    }
  } else if (workerRes.error) {
    workerData.value = null
  }

  queueOpsError.value = queueOpsRes.error
  if (queueOpsRes.data) {
    queueOpsData.value = {
      ...queueOpsRes.data,
      series: trimIncompleteBuckets(queueOpsRes.data.series || [], {
        bucketKey: 'bucket',
        bucketMinutes: queueOpsRes.data.bucketMinutes || 1,
      }),
    }
  } else if (queueOpsRes.error) {
    queueOpsData.value = null
  }

  retentionError.value = retentionRes.error
  if (retentionRes.data) {
    retentionData.value = {
      ...retentionRes.data,
      series: trimIncompleteBuckets(retentionRes.data.series || [], {
        bucketKey: 'bucket',
        bucketMinutes: retentionRes.data.bucketMinutes || 1,
      }),
    }
  } else if (retentionRes.error) {
    retentionData.value = null
  }

  queueParkedReplicasData.value = parkedReplicasRes.data ? {
    ...parkedReplicasRes.data,
    series: trimIncompleteBuckets(parkedReplicasRes.data.series || [], {
      bucketKey: 'bucket',
      bucketMinutes: parkedReplicasRes.data.bucketMinutes || 1,
    }),
  } : null

  loading.value = false
}

// Failure states the template reads. `opsError` is the tenant half of the page
// and `allFailed` is the only condition under which the page has nothing left
// to show — everything else degrades panel by panel.
const opsError = computed(() => queueOpsError.value)
const allFailed = computed(() =>
  queueOpsError.value !== null && retentionError.value !== null
)

// One shared ticker for the whole app, paused while the tab is hidden: under
// the proxy every poll is rate-limited and metered.
useAutoRefresh(fetchData)
onMounted(fetchData)

// When the user lands on Parked + individual after the initial fetch,
// kick a refetch so the per-replica series shows up without waiting for
// the global refresh tick. Watcher is intentionally narrow: only
// re-runs when the active op or view mode changes meaningfully.
watch([selectedQueueOp, viewMode], ([op, mode], [prevOp, prevMode]) => {
  if (op === prevOp && mode === prevMode) return
  const wasIndividualParked = prevOp === 'parked' && prevMode === 'individual'
  const isIndividualParked  = op === 'parked' && mode === 'individual'
  if (wasIndividualParked !== isIndividualParked) fetchData()
})
</script>

<style scoped>
@media (max-width: 1100px) {
  div[style*="grid-template-columns:repeat(2"] { grid-template-columns: 1fr !important; }
}

/* A panel whose source failed or does not exist. Deliberately not styled as
   an empty state: "nothing to show" and "we could not ask" are different
   answers and must not look the same. */
.panel-na {
  font-size: 12.5px;
  font-style: italic;
  color: var(--text-low);
  text-align: center;
  padding: 20px 12px;
  border: 1px dashed var(--warn-400);
  border-radius: var(--r-card);
}

/* CELL · OPERATOR divider — same amber vocabulary as the sidebar's Cell
   group, so "this is not your tenant's number" reads the same everywhere. */
.cell-section {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding: 10px 14px;
  margin: 24px 0 14px;
  font-size: 12px;
  color: var(--text-low);
  border: 1px solid var(--bd);
  border-left: 3px solid var(--warn-400);
  border-radius: var(--r-card);
  /* Half the --warn-glow tint: this band is a full-width divider, and at the
     glow's .12 it stops reading as a rule and starts reading as a panel. */
  background: color-mix(in srgb, var(--warn-400) 5%, transparent);
}
.cell-tag {
  font-size: 9.5px;
  font-weight: 600;
  letter-spacing: .12em;
  color: var(--warn-400);
  border: 1px solid var(--warn-400);
  border-radius: var(--r-chip);
  padding: 1px 5px;
  white-space: nowrap;
}
.cell-chip {
  font-size: 9px;
  font-weight: 600;
  letter-spacing: .1em;
  text-transform: uppercase;
  color: var(--warn-400);
  border: 1px solid var(--warn-400);
  border-radius: var(--r-chip);
  padding: 0 3px;
  margin-left: 6px;
  vertical-align: 2px;
  opacity: .8;
}

.val-warn { color: var(--warn-400); }
.val-bad { color: var(--ember-400); }

.range-error {
  margin: 10px 0 0;
  font-size: 12px;
  color: var(--warn-400);
}

/* Top Queues leaderboard — 2x2 grid so each mini-table gets enough
   horizontal room to show full queue names without ellipsis. The earlier
   4-across layout looked sleek but truncated everything to "test-q…",
   which defeats the panel's purpose ("which queue do I click into?").
   Collapses to a single column under 640px. */
.top-queues-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 16px 24px;
}
@media (max-width: 640px) {
  .top-queues-grid { grid-template-columns: 1fr; }
}
/* Explicit table layout — without `table-layout: fixed`, the auto
   algorithm interprets `max-width: 0` on the qname cell as "this column
   wants zero width", and the resulting allocation truncates names long
   before the leftover space is exhausted. Fixed layout + explicit
   widths on the rank and value columns lets qname reliably absorb
   everything that's left. */
.top-queues { table-layout: fixed; width: 100%; }
.top-queues td { padding: 4px 8px; font-size: 12px; }
.top-queues td.rank {
  width: 28px; color: var(--text-low); text-align: right;
  font-variant-numeric: tabular-nums; font-size: 11px;
}
.top-queues td.qname {
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  font-family: var(--font-mono, ui-monospace, SFMono-Regular, monospace);
  color: var(--text-mid);
}
.top-queues td.val {
  width: 80px;
  text-align: right; white-space: nowrap; color: var(--text-hi);
}
.empty-tile {
  font-size: 12px; color: var(--text-low); text-align: center;
  padding: 16px 0; border: 1px dashed var(--bd); border-radius: var(--r-card);
}
</style>
