<template>
  <div class="view-container">

    <!-- ======================================================================
         Scope strip — always first, always rendered. It is built from
         useIdentity(), not from anything we fetched, so it still states which
         tenant / cluster / cell these numbers belong to while the page is
         loading and when the fetch failed.
         ====================================================================== -->
    <div class="scope-strip">
      <span class="chip chip-mute">tenant scope</span>
      <span class="scope-text">
        <strong>{{ actingTenantSlug || 'no tenant' }}</strong>
        <span class="scope-sep">/</span>{{ actingClusterSlug || 'no cluster' }}
        <span class="scope-sep">·</span>cell {{ actingCellSlug || 'unknown' }}
      </span>
      <span class="scope-fill"></span>
      <span class="scope-meta">{{ rangeLabel }}</span>
    </div>

    <!-- ======================================================================
         Loading skeleton — first paint only; subsequent refreshes leave the
         previous data on screen so the eye doesn't lose its place. The shape
         mirrors the loaded page block for block (bar · actions · filters ·
         counts · metric table · configuration) so nothing jumps on landing.
         ====================================================================== -->
    <div v-if="firstLoad" style="display:flex; flex-direction:column; gap:14px;">
      <div class="skeleton" style="height:34px; width:60%;" />
      <div class="skeleton" style="height:30px; width:44%;" />
      <div class="skeleton" style="height:56px; width:100%;" />
      <div class="skeleton" style="height:64px; width:100%;" />
      <div class="skeleton" style="height:360px; width:100%;" />
      <div class="skeleton" style="height:200px; width:100%;" />
    </div>

    <!-- Error state. "Queue not found" is reserved for an actual 404: a 500 or
         a dropped connection is a different problem with a different fix, and
         telling the user their queue is gone is the worse of the two lies. -->
    <div v-else-if="detailError" class="card">
      <div class="card-body">
        <!-- Same block as QueueOperations' full-page failure: a card with the
             ember-headlined `.empty-state`, because in this state there is
             nothing else on screen for a banner to sit above. -->
        <div class="empty-state empty-state-failed">
          <h3>{{ detailError.isNotFound ? 'Queue not found' : 'Cannot load this queue' }}</h3>
          <p>
            <template v-if="detailError.isNotFound">
              No queue named <strong class="font-mono">{{ queueName }}</strong> on
              <strong>{{ actingClusterSlug || 'this cluster' }}</strong>.
            </template>
            <template v-else>{{ describeApiError(detailError) }}</template>
          </p>
          <button class="btn btn-ghost" @click="fetchAll">Retry</button>
          <button class="btn btn-ghost" @click="$router.push('/queues')">Back to queues</button>
        </div>
      </div>
    </div>

    <template v-else-if="statusData">
      <!-- ====================================================================
           Detail bar — back · queue · meta. This is page IDENTITY, not a
           control: the breadcrumb cannot carry a queue's namespace, task and
           partition count. The range picker and the live tick that used to
           ride along on the right have moved into the filter card below,
           where every other view keeps them.
           ==================================================================== -->
      <div class="qd-bar">
        <button @click="$router.push('/queues')" class="detail-back" title="Back to queues">
          <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.8">
            <path stroke-linecap="round" stroke-linejoin="round" d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
        </button>

        <span class="detail-name font-mono">{{ queueData?.name || queueName }}</span>

        <span class="detail-meta font-mono">
          <span v-if="queueData?.namespace">{{ queueData.namespace }}</span>
          <span v-if="queueData?.task">· {{ queueData.task }}</span>
          <span>· {{ partitions.length }} partition{{ partitions.length === 1 ? '' : 's' }}</span>
          <span v-if="queueData?.priority != null">· p{{ queueData.priority }}</span>
          <span v-if="queueData?.createdAt">· created {{ formatRelative(queueData.createdAt) }}</span>
        </span>
      </div>

      <!-- Quick actions — second row, separated so the bar above stays
           uncluttered on narrow viewports. -->
      <div class="qd-actions">
        <button class="btn btn-ghost" @click="goMessages">
          <svg width="13" height="13" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.8">
            <path stroke-linecap="round" stroke-linejoin="round" d="M8 10h.01M12 10h.01M16 10h.01M21 12c0 4.418-4.03 8-9 8a9.86 9.86 0 01-4-.8L3 21l1.4-4.2A8.96 8.96 0 013 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
          </svg>
          Browse messages
        </button>
        <button class="btn btn-ghost" @click="goDLQ" :class="{ 'btn-danger': totalMessages.deadLetter > 0 }">
          <svg width="13" height="13" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.8">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
          DLQ
          <span v-if="totalMessages.deadLetter > 0" class="qd-badge qd-badge-bad">{{ formatNumber(totalMessages.deadLetter) }}</span>
        </button>
        <button class="btn btn-ghost" @click="goTraces">
          <svg width="13" height="13" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.8">
            <path stroke-linecap="round" stroke-linejoin="round" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" />
          </svg>
          Traces
        </button>

        <span class="qd-actions-spacer" />

        <!-- DELETE is RouteClass::QueueAdmin at the proxy; anyone else would
             get a 403 from a button that looks perfectly enabled. -->
        <button v-if="can('queueAdmin')" @click="openDeleteModal" class="btn btn-danger">
          <svg width="13" height="13" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.8">
            <path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
          </svg>
          Delete queue
        </button>
      </div>

      <!-- ====================================================================
           Page banners — facts about the whole page. Worst first: the failed
           refresh, then the derived advisories in their own severity order.
           ==================================================================== -->
      <!-- A refresh that failed while data is on screen: the counts and config
           below are the last good ones, not the current ones. -->
      <div v-if="detailStale" class="status-banner banner-bad view-banner">
        <span>
          <strong>Could not load live status</strong> ·
          {{ describeApiError(detailStale) }} · showing the counts and
          configuration that were on screen at {{ lastRefreshText }}.
        </span>
      </div>

      <!-- Derived advisories — only when there's something to flag, so the
           page is quiet on healthy queues. The tone is the severity and is
           carried by the shared banner classes. -->
      <div
        v-for="(b, i) in banners"
        :key="i"
        class="status-banner view-banner"
        :class="`banner-${b.tone}`"
        @click="b.action && b.action()"
      >
        <span><strong>{{ b.title }}</strong> · {{ b.detail }}</span>
        <span class="qd-banner-cta" v-if="b.cta">{{ b.cta }} →</span>
      </div>

      <!-- ====================================================================
           Filter card — the page's only control is the range picker, so it is
           row 1's first field; the live tick closes the row on the right.
           ==================================================================== -->
      <div class="card filters">
        <div class="card-body filter-rows">
          <div class="filter-row">
            <div class="filter-field">
              <span class="label-xs">Range</span>
              <div class="seg">
                <button
                  v-for="r in timeRanges"
                  :key="r.value"
                  :class="{ on: selectedRange === r.value }"
                  @click="selectedRange = r.value"
                >{{ r.label }}</button>
              </div>
            </div>

            <span class="live-tick filter-field-right">
              <span class="pulse" />
              <span>live · {{ refreshAgo }}</span>
            </span>
          </div>
        </div>
      </div>

      <!-- ====================================================================
           Counts strip — point-in-time scope (left) + now-snapshots (right).
           Mirrors Dashboard's counts-strip but scoped to this queue.
           ==================================================================== -->
      <div class="counts-strip">
        <div class="counts-group">
          <!-- These are the totals as of the last good status fetch, exactly
               like the rates on the right — the label was missing, not the
               fact. -->
          <span class="count-item-label">now</span>
          <span class="count-item count-static">
            <strong class="num" :class="pendingNumClass(totalMessages.pending)">{{ formatNumber(totalMessages.pending) }}</strong>
            <span>pending</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static">
            <strong>{{ formatNumber(totalMessages.processing) }}</strong>
            <span>processing</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static count-muted">
            <strong>{{ formatNumber(totalMessages.completed) }}</strong>
            <span>completed</span>
          </span>
          <!-- No "failed" tile: get_queue_detail_v2's log branch hardcodes
               `failed` to 0, so the tile could only ever assert zero while the
               Errors row two blocks down charts the queue's real ack failures. -->
          <span class="count-sep">·</span>
          <button class="count-item" @click="goDLQ">
            <strong class="num" :class="{ bad: totalMessages.deadLetter > 0 }">{{ formatNumber(totalMessages.deadLetter) }}</strong>
            <span>dlq</span>
          </button>
          <span class="count-sep">·</span>
          <span class="count-item count-static count-muted">
            <strong>{{ formatNumber(totalMessages.total) }}</strong>
            <span>total</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static">
            <strong>{{ formatNumber(partitions.length) }}</strong>
            <span>{{ partitions.length === 1 ? 'partition' : 'partitions' }}</span>
          </span>
        </div>

        <div class="counts-group counts-group-right" :title="'Now-rates from queue-ops time series; parked = currently waiting long-poll consumers.'">
          <span class="count-item-label">now</span>
          <span class="count-item count-static count-tight">
            <strong>{{ nowSnapshot.push }}</strong><span class="count-suffix">push/s</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static count-tight">
            <strong>{{ nowSnapshot.pop }}</strong><span class="count-suffix">pop/s</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static count-tight">
            <strong>{{ nowSnapshot.ack }}</strong><span class="count-suffix">ack/s</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static count-tight">
            <strong>{{ nowSnapshot.parked }}</strong><span class="count-suffix">parked</span>
          </span>
        </div>
      </div>

      <!-- ====================================================================
           Metric table — Dashboard idiom, queue-scoped.
           Eight rows tell the story of this queue end-to-end:
             flow (Throughput, Pending Δ, Time lag, Fill ratio)
             health (Errors, Parked)
             admin (Partitions activity, Consumed)
           Each row reuses MetricRow for consistent typography + interactions.
           ==================================================================== -->
      <div class="metric-table">
        <div class="metric-head">
          <span></span>
          <span>Metric</span>
          <span class="h-value">Now</span>
          <span>Context</span>
          <span class="h-spark">{{ selectedRange }}</span>
          <span></span>
        </div>

        <MetricRow
          label="Throughput"
          :value="throughput.current"
          unit="/s"
          :context="throughputContext"
          :series="throughputSeries"
          :labels="chartLabels"
          :value-format="fmtRate"
          expand-unit="msgs / sec"
          :loading="loadingOps"
          :error="opsError"
          :expanded="isExpanded('throughput')"
          @toggle-expand="toggleRow('throughput')"
        />
        <MetricRow
          label="Pending Δ"
          :value="pendingDeltaDisplay"
          unit="msgs"
          :context="pendingDeltaContext"
          :sparkline="pendingDeltaSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          expand-unit="msgs (cumulative)"
          :severity="pendingDeltaSeverity"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Cumulative (push − ack) across the selected window. Positive = queue filling, negative = draining."
          :expanded="isExpanded('pendingDelta')"
          @toggle-expand="toggleRow('pendingDelta')"
        />
        <MetricRow
          label="Time lag"
          :context="lagContext"
          :series="lagSeriesData"
          :labels="chartLabels"
          :value-format="fmtLagMs"
          expand-unit="ms"
          :severity="lagSeverityKey"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Per-bucket avg / max delivery delay, derived from queue lag metrics."
          :expanded="isExpanded('timeLag')"
          @toggle-expand="toggleRow('timeLag')"
        >
          <template #value>
            <template v-if="lagLatest.avg === null && lagLatest.max === null">
              <span class="num">—</span>
            </template>
            <template v-else>
              <span class="num" :class="lagNumClass(lagLatest.avg)">{{ fmtLagShort(lagLatest.avg) }}</span>
              <span class="mr-sep">/</span>
              <span class="num" :class="lagNumClass(lagLatest.max)">{{ fmtLagShort(lagLatest.max) }}</span>
            </template>
          </template>
        </MetricRow>
        <MetricRow
          label="Fill ratio"
          :context="fillContext"
          :series="fillSeriesData"
          :labels="chartLabels"
          :value-format="fmtFillPct"
          expand-unit="%"
          :severity="fillSeverityKey"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Long-polls returning a message ÷ all long-poll completions on this queue. <30% with traffic = consumers mostly empty; ~100% sustained = fully utilized — watch Time lag."
          :expanded="isExpanded('fillRatio')"
          @toggle-expand="toggleRow('fillRatio')"
        >
          <template #value>
            <template v-if="fillLatest === null">
              <span class="num">—</span>
            </template>
            <template v-else>
              <span class="num" :class="fillSeverityKey">{{ fillLatest.toFixed(1) }}</span><i class="mr-unit">%</i>
            </template>
          </template>
        </MetricRow>
        <MetricRow
          label="Errors"
          :value="formatNumber(errorsTotal)"
          :context="errorsContext"
          :sparkline="errorsSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          expand-unit="count"
          :severity="errorsSeverity"
          :loading="loadingOps"
          :error="opsError"
          tooltip="ack failures over the selected window for this queue."
          :expanded="isExpanded('errors')"
          @toggle-expand="toggleRow('errors')"
        />
        <MetricRow
          label="Parked"
          :value="formatNumber(Math.round(parkedLatest))"
          unit="consumers"
          :context="parkedContext"
          :sparkline="parkedSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          expand-unit="long-polls"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Long-poll consumer connections currently waiting on this queue, averaged each minute."
          :expanded="isExpanded('parked')"
          @toggle-expand="toggleRow('parked')"
        />
        <MetricRow
          label="Partitions Δ"
          :context="'created / deleted in window · current ' + partitions.length"
          :series="partitionOpsSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          expand-unit="count"
          :loading="loadingOps"
          :error="opsError"
          :expanded="isExpanded('partitionsOps')"
          @toggle-expand="toggleRow('partitionsOps')"
        >
          <template #value>
            <span class="num" style="color:var(--text-hi);">+{{ formatNumber(partitionCreatedTotal) }}</span>
            <span class="mr-sep">/</span>
            <span class="num mute">−{{ formatNumber(partitionDeletedTotal) }}</span>
          </template>
        </MetricRow>
        <MetricRow
          label="Consumed"
          :value="formatNumber(consumedTotal)"
          unit="msgs"
          :context="consumedContext"
          :sparkline="popSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          expand-unit="msgs (per bucket)"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Lifetime messages consumed across all consumer groups on this queue (from partition cursors); sparkline shows pop rate per bucket."
          :expanded="isExpanded('consumed')"
          @toggle-expand="toggleRow('consumed')"
        />
      </div>

      <!-- ====================================================================
           Configuration card — full queue config + identity chips.
           Always at the bottom: config rarely changes, but is the canonical
           record when something is misbehaving.
           ==================================================================== -->
      <div v-if="queueData?.config" class="card">
        <div class="card-header">
          <h3>Configuration</h3>
          <!-- Subtitle, not freshness: the right edge of a card header is the
               freshness slot everywhere in the app. It is left empty here on
               purpose — this page has no useApi panel to stamp, and
               `lastRefreshAt` is the last refresh ATTEMPT (fetchAll bumps it
               even when the detail fetch failed), so "as of / last good HH:MM"
               would name a moment at which nothing was actually loaded. The
               page's freshness is stated by the live tick instead. -->
          <span class="card-sub">{{ queueData.namespace || '—' }} · {{ queueData.task || '—' }}</span>
        </div>
        <div class="card-body">
          <div class="qd-config-grid">
            <!-- formatDuration(0) is the string '0ms', which is truthy — the
                 old `|| '—'` guard could never fire, so "no TTL" rendered as
                 "0ms" (instant expiry). Test the raw value instead. -->
            <div class="qd-config">
              <span class="label-xs">Lease time</span>
              <span class="qd-config-val font-mono">
                {{ queueData.config.leaseTime ? formatDuration(queueData.config.leaseTime * 1000) : '—' }}
              </span>
              <span class="qd-config-hint">how long a lease is held</span>
            </div>
            <div class="qd-config">
              <span class="label-xs">TTL</span>
              <span class="qd-config-val font-mono">
                {{ queueData.config.ttl ? formatDuration(queueData.config.ttl * 1000) : '∞' }}
              </span>
              <span class="qd-config-hint">message lifetime</span>
            </div>
            <div class="qd-config">
              <span class="label-xs">Max queue size</span>
              <span class="qd-config-val font-mono">{{ queueData.config.maxQueueSize ? formatNumber(queueData.config.maxQueueSize) : '∞' }}</span>
              <span class="qd-config-hint">push back-pressure cap</span>
            </div>
            <div class="qd-config">
              <span class="label-xs">Retry limit</span>
              <span class="qd-config-val font-mono">{{ queueData.config.retryLimit || 0 }}</span>
              <span class="qd-config-hint">attempts before DLQ / drop</span>
            </div>
            <div class="qd-config">
              <span class="label-xs">Retry delay</span>
              <span class="qd-config-val font-mono">{{ queueData.config.retryDelay || 0 }} ms</span>
              <span class="qd-config-hint">backoff between attempts</span>
            </div>
            <div class="qd-config">
              <span class="label-xs">Dead-letter queue</span>
              <span class="qd-config-val">
                <span class="chip" :class="queueData.config.deadLetterQueue ? 'chip-ice' : 'chip-mute'">
                  {{ queueData.config.deadLetterQueue ? 'enabled' : 'disabled' }}
                </span>
              </span>
              <span class="qd-config-hint">where exhausted retries land</span>
            </div>
            <div class="qd-config">
              <span class="label-xs">Priority</span>
              <span class="qd-config-val font-mono">{{ queueData.priority ?? 0 }}</span>
              <span class="qd-config-hint">scheduler priority</span>
            </div>
            <div class="qd-config">
              <span class="label-xs">Created</span>
              <span class="qd-config-val font-mono">{{ formatDate(queueData.createdAt) }}</span>
              <span class="qd-config-hint">{{ formatRelative(queueData.createdAt) }}</span>
            </div>
          </div>
        </div>
      </div>
    </template>

    <!-- ======================================================================
         Modals
         ====================================================================== -->
    <Teleport to="body">
      <div v-if="showDeleteModal" class="modal-backdrop" @click.self="closeDeleteModal">
        <div class="card modal-card">
          <div class="card-header"><h3>Delete queue</h3></div>
          <div class="card-body">
            <div v-if="deleteError" class="panel-err">{{ deleteError }}</div>
            <p style="color:var(--text-mid);">
              Delete <strong>{{ queueName }}</strong>? This permanently removes the queue,
              all partitions ({{ partitions.length }}), and {{ formatNumber(totalMessages.total) }} messages.
              This action cannot be undone.
            </p>
          </div>
          <div class="modal-foot">
            <button @click="closeDeleteModal" class="btn btn-ghost">Cancel</button>
            <button @click="deleteQueue" class="btn btn-danger" :disabled="deleting">
              {{ deleting ? 'Deleting…' : 'Delete queue' }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>

  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { analytics, queues as queuesApi, system as systemApi, describeApiError } from '@/api'
import { formatNumber, formatDuration, toNum, latestFinite, trimIncompleteBuckets } from '@/composables/useApi'
import { formatChartLabel } from '@/composables/useFormat'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { semanticColors } from '@/composables/useChartTheme'
import MetricRow from '@/components/MetricRow.vue'

const route = useRoute()
const router = useRouter()
const queueName = computed(() => route.params.queueName)
const { can, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
const { notifySuccess } = useToast()

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const loading = ref(true)
// The failure that stopped the page from loading, kept as the ApiError so the
// template can tell a real 404 from a 500 or a dropped connection.
const detailError = ref(null)
// A failure that arrived while data was already on screen — the page keeps
// rendering, but says the numbers are the previous load's.
const detailStale = ref(null)

const statusData = ref(null)
const opsData = ref(null)
const opsError = ref(null)

const loadingOps = ref(true)

const showDeleteModal = ref(false)
const deleteError = ref(null)
const deleting = ref(false)

const selectedRange = ref('1h')
const timeRanges = [
  { label: '1h',  value: '1h',  minutes: 60 },
  { label: '6h',  value: '6h',  minutes: 360 },
  { label: '24h', value: '24h', minutes: 1440 },
]
// The resolved window, restated once in the scope strip. There is no custom
// range on this page, so it is always a quick range.
const rangeLabel = computed(() => {
  const r = timeRanges.find(t => t.value === selectedRange.value)
  return `last ${r ? r.label : selectedRange.value}`
})

// Skeletons are first paint only: a refresh must leave the previous page on
// screen. `loading` is only ever true with no statusData, but the guard is
// written out so the rule is visible at the call site.
const firstLoad = computed(() => loading.value && !statusData.value)

const lastRefreshAt = ref(null)
const lastRefreshText = computed(() =>
  lastRefreshAt.value ? new Date(lastRefreshAt.value).toLocaleTimeString() : 'an earlier load'
)
// Live tick, off the app's one shared ticker.
const refreshAgo = useRefreshAgo(lastRefreshAt)

// Per-row expand state — same Set-based pattern as Dashboard so the
// metric table feels familiar from page to page.
const expandedRows = ref(new Set())
const isExpanded = (key) => expandedRows.value.has(key)
const toggleRow = (key) => {
  const next = new Set(expandedRows.value)
  if (next.has(key)) next.delete(key); else next.add(key)
  expandedRows.value = next
}

// ---------------------------------------------------------------------------
// Time range helpers
// ---------------------------------------------------------------------------
const getTimeRangeParams = () => {
  const r = timeRanges.find(x => x.value === selectedRange.value) || timeRanges[0]
  const now = new Date()
  return {
    from: new Date(now.getTime() - r.minutes * 60 * 1000).toISOString(),
    to: now.toISOString(),
    queue: queueName.value,
  }
}

// The queue's identity + config come from /api/v1/status/queues/:name, the
// only endpoint that describes a log queue. The old `get_queue_v2` fallback
// read the rows-engine tables (empty under the log engine) and rendered a
// healthy queue as 0 partitions / 0 messages / no config, so it is gone.
const queueData = computed(() => statusData.value?.queue || null)

// History (oldest → newest). The queue-ops endpoint returns one row per
// (bucket, queueName); since we filter to a single queue, that's
// effectively one row per bucket. We sort by bucket ascending so charts
// read left→right, then drop the in-flight current bucket — workers
// flush on minute boundaries, so the latest bucket otherwise paints a
// phantom drop to zero on the right edge of every chart.
const history = computed(() => {
  const series = opsData.value?.series || []
  const sorted = [...series].sort((a, b) => a.bucket.localeCompare(b.bucket))
  return trimIncompleteBuckets(sorted, {
    bucketKey: 'bucket',
    bucketMinutes: opsData.value?.bucketMinutes || 1,
  })
})
const multiDay = computed(() => {
  const h = history.value
  if (h.length < 2) return false
  return new Date(h[0].bucket).toDateString() !== new Date(h[h.length - 1].bucket).toDateString()
})
const chartLabels = computed(() =>
  history.value.map(r => formatChartLabel(new Date(r.bucket), multiDay.value))
)

// ---------------------------------------------------------------------------
// Partitions (existing logic, retained but enriched)
// ---------------------------------------------------------------------------
const partitions = computed(() => statusData.value?.partitions || [])

const totalMessages = computed(() => {
  const apiTotals = statusData.value?.totals
  if (apiTotals) {
    const msgs = apiTotals.messages || apiTotals
    return {
      total: msgs.total || 0,
      pending: Math.max(0, msgs.pending || 0),
      processing: msgs.processing || 0,
      completed: msgs.completed || 0,
      deadLetter: msgs.deadLetter || 0
    }
  }
  const t = { total: 0, pending: 0, processing: 0, completed: 0, deadLetter: 0 }
  for (const p of partitions.value) {
    const m = p.messages || p.stats || {}
    t.total += m.total || 0
    t.pending += m.pending || 0
    t.processing += m.processing || 0
    t.completed += m.completed || 0
    t.deadLetter += m.deadLetter || 0
  }
  t.pending = Math.max(0, t.pending)
  return t
})

// ---------------------------------------------------------------------------
// Now-snapshot (right side of counts strip) — latest *finite* bucket per
// field. Walking back from the tail means a still-aggregating most-recent
// bucket doesn't make every metric read 0 right after a minute boundary.
// ---------------------------------------------------------------------------
const nowSnapshot = computed(() => {
  const h = history.value
  return {
    push:   fmtRate(latestFinite(h.map(x => x.pushPerSecond)) ?? 0),
    pop:    fmtRate(latestFinite(h.map(x => x.popPerSecond))  ?? 0),
    ack:    fmtRate(latestFinite(h.map(x => x.ackPerSecond))  ?? 0),
    parked: formatNumber(Math.round(latestFinite(h.map(x => x.parkedCount)) ?? 0)),
  }
})

// ---------------------------------------------------------------------------
// Throughput row — push / pop / ack rates per bucket
// ---------------------------------------------------------------------------
const throughput = computed(() => {
  // Prefer pop, fall back to push — and use the latest *finite* values
  // so we don't read 0 from a still-flushing tail bucket.
  const h = history.value
  const pop = latestFinite(h.map(x => x.popPerSecond))
  const push = latestFinite(h.map(x => x.pushPerSecond))
  const v = pop ?? push ?? 0
  return { current: v.toFixed(1) }
})
const throughputSeries = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [
    { label: 'Push', data: h.map(x => toNum(x.pushPerSecond)) },
    { label: 'Pop',  data: h.map(x => toNum(x.popPerSecond)) },
    // Ack is the only semantically-coloured line here (it means "work landed");
    // the colour is resolved from --ok-500, never re-typed.
    { label: 'Ack',  data: h.map(x => toNum(x.ackPerSecond)), color: semanticColors.ok.line },
  ]
})
const throughputPeak = computed(() => {
  const h = history.value
  if (!h.length) return 0
  // Per-bucket max across the three rates, ignoring nulls.
  const perBucket = h.map(x => {
    const vals = [x.pushPerSecond, x.popPerSecond, x.ackPerSecond]
      .map(toNum).filter(v => v !== null)
    return vals.length ? Math.max(...vals) : null
  }).filter(v => v !== null)
  return perBucket.length ? Math.max(0, ...perBucket) : 0
})
const throughputContext = computed(() => {
  if (throughputPeak.value === 0) return 'idle · no traffic in window'
  return `peak ${formatNumber(Math.round(throughputPeak.value))} /s`
})

// ---------------------------------------------------------------------------
// Pending Δ row — cumulative push − pop across the window
// ---------------------------------------------------------------------------
// Cumulative push − pop. When both source values are missing for a bucket
// we emit null (chart gap); otherwise we still accumulate so the line
// continues from the last known total once data resumes.
const pendingDeltaSeries = computed(() => {
  const h = history.value
  if (!h.length) return []
  let cum = 0
  return h.map(x => {
    const push = toNum(x.pushMessages)
    const pop  = toNum(x.popMessages)
    if (push === null && pop === null) return null
    cum += (push || 0) - (pop || 0)
    return cum
  })
})
const pendingDeltaLatest = computed(() => latestFinite(pendingDeltaSeries.value) ?? 0)
const pendingDeltaDisplay = computed(() => {
  const v = pendingDeltaLatest.value
  if (v === 0) return '0'
  return (v > 0 ? '+' : '−') + formatNumber(Math.abs(v))
})
const pendingDeltaContext = computed(() => {
  const v = pendingDeltaLatest.value
  if (v === 0) return 'flat · push = pop across window'
  if (v > 0) return 'queue filling · push > pop'
  return 'queue draining · pop > push'
})
const pendingDeltaSeverity = computed(() => {
  const v = pendingDeltaLatest.value
  if (v > 100000) return 'bad'
  if (v > 1000) return 'warn'
  if (v < -1000) return 'ok'
  return ''
})

// ---------------------------------------------------------------------------
// Time lag row — avg / max from queue-ops series
// ---------------------------------------------------------------------------
// Lag is measured AT POP. get_queue_ops_v1 emits 0 for a bucket with pushes
// but no pops, and 0 read as a measurement is how a backlogged queue with
// stopped consumers reports zero lag. A bucket with no pops has NO SAMPLE.
const lagPerBucket = computed(() =>
  history.value.map(x => {
    const pops = toNum(x.popMessages) || 0
    if (pops <= 0) return { avg: null, max: null }
    return { avg: toNum(x.avgLagMs), max: toNum(x.maxLagMs) }
  })
)
const lagSeriesData = computed(() => {
  const l = lagPerBucket.value
  if (!l.length) return null
  return [
    { label: 'Avg', data: l.map(x => x.avg) },
    { label: 'Max', data: l.map(x => x.max) },
  ]
})
const lagLatest = computed(() => {
  const l = lagPerBucket.value
  if (!l.length) return { avg: null, max: null }
  // Latest *sampled* value for each, so a still-aggregating last bucket
  // doesn't make lag appear to drop.
  return {
    avg: latestFinite(l.map(x => x.avg)),
    max: latestFinite(l.map(x => x.max)),
  }
})
const lagNumClass = (ms) => {
  if (ms === null || ms === undefined) return ''
  if (!ms || ms === 0) return ''
  if (ms < 60_000) return ''
  if (ms < 300_000) return 'warn'
  return 'bad'
}
// MetricRow.severity expects a key like 'warn' / 'bad'; map from the raw ms.
const lagSeverityKey = computed(() => lagNumClass(lagLatest.value.max))
const lagContext = computed(() => {
  const finite = lagPerBucket.value.map(x => x.max).filter(v => v !== null)
  if (!finite.length) return 'no pops in window · lag not measured'
  const peak = Math.max(...finite)
  if (peak === 0) return 'measured, and zero across the window'
  return `peak max ${fmtLagShort(peak)} · avg-bucket lag`
})

// ---------------------------------------------------------------------------
// Fill ratio row — popMessages / (popMessages + popEmpty)
// ---------------------------------------------------------------------------
const FILL_NOISE_FLOOR = 5
const fillSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  // null source data → emit null (real gap)
  // populated but quiet → carry forward last computed value (continuous line)
  // populated and busy  → compute from pop/(pop+empty)
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
  let pop = 0, empty = 0
  for (const r of tail) {
    pop += toNum(r.popMessages) || 0
    empty += toNum(r.popEmpty) || 0
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
const fillSeverityKey = computed(() => {
  const v = fillLatest.value
  if (v === null) return ''
  if (v < 30) return 'warn'
  return ''
})

// ---------------------------------------------------------------------------
// Errors row — ack failed in window for this queue
// ---------------------------------------------------------------------------
const errorsSeries = computed(() => {
  const h = history.value
  if (!h.length) return []
  return h.map(x => toNum(x.ackFailed))
})
const errorsTotal = computed(() =>
  history.value.reduce((s, x) => s + (toNum(x.ackFailed) || 0), 0)
)
const errorsContext = computed(() => {
  const t = errorsTotal.value
  if (!t) return 'no ack failures in window'
  return `${formatNumber(t)} ack failure${t === 1 ? '' : 's'}`
})
const errorsSeverity = computed(() => {
  const t = errorsTotal.value
  if (t > 100) return 'bad'
  if (t > 0) return 'warn'
  return ''
})

// ---------------------------------------------------------------------------
// Parked row — long-poll consumers waiting on this queue
// ---------------------------------------------------------------------------
const parkedSeries = computed(() => history.value.map(x => toNum(x.parkedCount)))
const parkedLatest = computed(() => latestFinite(history.value.map(x => x.parkedCount)) ?? 0)
const parkedAvg = computed(() => {
  const finite = history.value.map(x => toNum(x.parkedCount)).filter(v => v !== null)
  if (!finite.length) return 0
  return finite.reduce((s, v) => s + v, 0) / finite.length
})
const parkedContext = computed(() => {
  if (!history.value.length) return '—'
  return `avg ${parkedAvg.value.toFixed(1)} across window`
})

// ---------------------------------------------------------------------------
// Partitions Δ row — created / deleted in window
// ---------------------------------------------------------------------------
const partitionOpsSeries = computed(() => {
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
// Consumed row — lifetime cursor consumed (total) + per-bucket pop sparkline
// ---------------------------------------------------------------------------
const consumedTotal = computed(() =>
  partitions.value.reduce((s, p) => s + (toNum(p.cursor?.totalConsumed) || 0), 0)
)
const popSeries = computed(() => history.value.map(x => toNum(x.popMessages)))
const consumedContext = computed(() =>
  'lifetime across all consumer groups · sparkline shows pop rate'
)

// ---------------------------------------------------------------------------
// Banner derivation — only the worst signals show, ordered by severity.
// ---------------------------------------------------------------------------
const banners = computed(() => {
  const out = []
  if (totalMessages.value.deadLetter > 0) {
    out.push({
      tone: 'bad',
      title: 'Messages in DLQ',
      detail: `${formatNumber(totalMessages.value.deadLetter)} message${totalMessages.value.deadLetter === 1 ? '' : 's'} require investigation.`,
      cta: 'Open DLQ',
      action: goDLQ,
    })
  }
  if (errorsTotal.value > 0) {
    out.push({
      tone: errorsSeverity.value === 'bad' ? 'bad' : 'warn',
      title: 'Ack failures in window',
      detail: `${formatNumber(errorsTotal.value)} ack failure${errorsTotal.value === 1 ? '' : 's'} across the last ${selectedRange.value}.`,
    })
  }
  if (totalMessages.value.pending > 10000) {
    out.push({
      tone: totalMessages.value.pending > 100000 ? 'bad' : 'warn',
      title: 'High pending depth',
      detail: `${formatNumber(totalMessages.value.pending)} pending — consider scaling consumers or checking lag.`,
    })
  }
  return out
})

// ---------------------------------------------------------------------------
// Numeric tone for top-strip pending
// ---------------------------------------------------------------------------
const pendingNumClass = (n) => {
  if (!n || n < 1000) return ''
  if (n < 10000) return 'warn'
  return 'bad'
}

// ---------------------------------------------------------------------------
// Formatters
// ---------------------------------------------------------------------------
const fmtRate = (n) => {
  const v = Number(n) || 0
  if (Math.abs(v) >= 1000) return (v / 1000).toFixed(1) + 'k'
  if (Math.abs(v) >= 100) return Math.round(v).toString()
  if (Math.abs(v) >= 10) return v.toFixed(1)
  return v.toFixed(2)
}
const fmtCount = (n) => formatNumber(Math.round(Number(n) || 0))
const fmtLagMs = (n) => {
  const v = Number(n) || 0
  if (v < 1) return '0'
  if (v < 1000) return Math.round(v) + ' ms'
  if (v < 60000) return (v / 1000).toFixed(1) + ' s'
  return (v / 60000).toFixed(1) + ' m'
}
function fmtLagShort(ms) {
  if (ms === null || ms === undefined) return '—'
  if (ms === 0) return '0'
  if (ms < 1000) return Math.round(ms) + 'ms'
  if (ms < 60000) return (ms / 1000).toFixed(1) + 's'
  if (ms < 3600000) return (ms / 60000).toFixed(1) + 'm'
  return (ms / 3600000).toFixed(1) + 'h'
}
const fmtFillPct = (n) => {
  if (n === null || n === undefined) return '—'
  const v = Number(n)
  if (!Number.isFinite(v)) return '—'
  return v.toFixed(1) + '%'
}
const formatDate = (ts) => {
  if (!ts) return '—'
  return new Date(ts).toLocaleString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit'
  })
}
const formatRelative = (ts) => {
  if (!ts) return '—'
  const diffMs = Date.now() - new Date(ts).getTime()
  if (diffMs < 0) return 'just now'
  if (diffMs < 60_000) return `${Math.floor(diffMs / 1000)}s ago`
  if (diffMs < 3600_000) return `${Math.floor(diffMs / 60_000)}m ago`
  if (diffMs < 86400_000) return `${Math.floor(diffMs / 3600_000)}h ago`
  if (diffMs < 30 * 86400_000) return `${Math.floor(diffMs / 86400_000)}d ago`
  return formatDate(ts)
}
// ---------------------------------------------------------------------------
// Navigation helpers
// ---------------------------------------------------------------------------
function goMessages() {
  router.push({ path: '/messages', query: { queue: queueName.value } })
}
function goDLQ() {
  router.push({ path: '/dlq', query: { queue: queueName.value } })
}
function goTraces() {
  router.push({ path: '/traces', query: { queue: queueName.value } })
}

// ---------------------------------------------------------------------------
// Fetchers
// ---------------------------------------------------------------------------
// One call describes the queue: /api/v1/status/queues/:name. The old second
// fetch of /resources/queues/:name doubled the request rate of the busiest
// detail page for a fallback that could only ever render an all-zero view.
const fetchQueueDetail = async () => {
  try {
    const r = await analytics.getQueueDetail(queueName.value)
    statusData.value = r.data
    detailError.value = null
    detailStale.value = null
  } catch (err) {
    // Keep whatever is on screen; the banner says the refresh failed. Only a
    // page with nothing to show falls back to the error card.
    if (statusData.value) detailStale.value = err
    else detailError.value = err
  }
}

const fetchOps = async () => {
  if (!opsData.value) loadingOps.value = true
  try {
    const r = await systemApi.getQueueOps(getTimeRangeParams())
    opsData.value = r.data
    opsError.value = null
  } catch (err) {
    // Already announced globally; this drives the per-row "unavailable" state
    // so the metric table never paints an unreachable endpoint as an idle queue.
    opsError.value = err
  } finally {
    loadingOps.value = false
  }
}

const fetchAll = async () => {
  // Two calls only: queue detail (for config + totals) and queue-ops
  // (for the metric table). Consumer-group + lagging-partition scans are
  // expensive at scale and intentionally skipped here — they live on
  // the dedicated /consumers page.
  await Promise.all([fetchQueueDetail(), fetchOps()])
  lastRefreshAt.value = Date.now()
  loading.value = false
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------
const openDeleteModal = () => {
  deleteError.value = null
  showDeleteModal.value = true
}
const closeDeleteModal = () => {
  showDeleteModal.value = false
  deleteError.value = null
}

const deleteQueue = async () => {
  if (deleting.value) return
  const name = queueName.value
  deleting.value = true
  deleteError.value = null
  try {
    const res = await queuesApi.delete(name)
    // delete_queue_v1 answers 200 {deleted:true, existed:false} when there was
    // nothing to delete. Navigating away on that reports a deletion that
    // never happened.
    if (res.data && res.data.existed === false) {
      deleteError.value = `No queue named "${name}" on this cluster — nothing was deleted.`
      return
    }
    closeDeleteModal()
    notifySuccess(`Deleted queue ${name}`)
    router.push('/queues')
  } catch (err) {
    deleteError.value = describeApiError(err)
  } finally {
    deleting.value = false
  }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------
// The shell's shared ticker drives the 30s poll (and pauses it while the tab
// is hidden); a private setInterval here would keep spending the tenant's
// metered request budget off screen.
useAutoRefresh(fetchAll)
watch(selectedRange, fetchOps)
watch(queueName, () => {
  loading.value = true
  detailError.value = null
  detailStale.value = null
  statusData.value = null
  opsData.value = null
  opsError.value = null
  fetchAll()
})

onMounted(fetchAll)
</script>

<style scoped>
/* ---------------------------------------------------------------------------
   Detail bar — back · name · meta. Page identity only: the range picker and
   the live tick now live in the shared filter card below.
   --------------------------------------------------------------------------- */
.qd-bar {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 6px 0 12px;
  border-bottom: 1px solid var(--bd);
  margin-bottom: 12px;
  flex-wrap: wrap;
}

/* Quick-action row */
.qd-actions {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 14px;
  flex-wrap: wrap;
}
.qd-actions-spacer { flex: 1 1 auto; }
.qd-badge {
  display: inline-flex; align-items: center;
  padding: 0 5px; border-radius: var(--r-pill);
  font-size: 10px; font-family: 'JetBrains Mono', monospace;
  margin-left: 4px;
}
.qd-badge-bad {
  background: var(--ember-glow); color: var(--ember-400);
  border: 1px solid color-mix(in srgb, var(--ember-500) 25%, transparent);
}

/* ---------------------------------------------------------------------------
   Advisory banners use the shared .status-banner / .banner-* / .view-banner
   trio. Only the CTA affordance is local: a banner carrying one is a link,
   the rest are statements.
   --------------------------------------------------------------------------- */
.view-banner:has(> .qd-banner-cta) {
  cursor: pointer;
  transition: border-color .12s var(--ease);
}
.view-banner:has(> .qd-banner-cta):hover { border-color: var(--bd-hi); }
.qd-banner-cta {
  margin-left: auto;
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px; color: var(--text-mid);
}

/* Counts strip and metric table are shared idioms — .counts-strip / .count-*
   and .metric-table / .metric-head / .h-spark now live in style.css so this
   page and Dashboard cannot drift apart again. Only the MetricRow internals
   this page reaches into stay here. */
:deep(.mr-sep) { color: var(--text-low); margin: 0 4px; font-weight: 400; }
:deep(.mr-unit) {
  font-style: normal; color: var(--text-low);
  margin-left: 3px; font-size: 11px; font-weight: 400;
}

/* ---------------------------------------------------------------------------
   Configuration grid
   --------------------------------------------------------------------------- */
.qd-config-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 14px 24px;
}
@media (max-width: 1100px) {
  .qd-config-grid { grid-template-columns: repeat(3, 1fr); }
}
@media (max-width: 900px) {
  .qd-config-grid { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 500px) {
  .qd-config-grid { grid-template-columns: 1fr; }
}
.qd-config {
  display: flex; flex-direction: column; gap: 4px;
  padding: 10px 0;
}
.qd-config-val {
  font-size: 14px; font-weight: 600;
  color: var(--text-hi); letter-spacing: -.005em;
}
.qd-config-hint {
  font-size: 10.5px; color: var(--text-low);
  letter-spacing: .04em;
}

/* The delete modal uses the shared shell — .modal-backdrop / .modal-card /
   .modal-foot — and the shared .panel-err for its form error, so it is not a
   fourth private ember box. Nothing modal-shaped is declared here any more. */
</style>
