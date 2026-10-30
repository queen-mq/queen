<template>
  <QueuesLayout :live="statusData ? refreshAgo : ''">
    <!-- PUSH is RouteClass::Produce; EDIT and DELETE are QueueAdmin: the
         buttons mirror the proxy's classes so none is enabled into a 403. -->
    <template v-if="statusData" #actions>
      <button v-if="can('queueAdmin')" class="btn btn-ghost qd-delete" @click="openDeleteModal">Delete</button>
      <button v-if="can('queueAdmin')" class="btn" @click="configOpen = true">Configure</button>
      <button v-if="can('produce')" class="btn btn-primary" @click="pushOpen = true">Push message</button>
    </template>
  <div class="view-container qd">

    <!-- First paint only, shaped like the page: a refresh leaves the previous
         numbers on screen so the eye does not lose its place. -->
    <div v-if="firstLoad" class="qd-skel" aria-hidden="true">
      <div class="skeleton" style="height:64px;" />
      <div class="skeleton" style="height:28px; width:220px; align-self:flex-end;" />
      <div class="skeleton" style="height:240px;" />
      <div class="skeleton" style="height:300px;" />
    </div>

    <!-- "Queue not found" is reserved for an actual 404: a 500 or a dropped
         connection is a different problem with a different fix. -->
    <div v-else-if="detailError" class="card">
      <div class="empty-state empty-state-failed">
        <h3>{{ detailError.isNotFound ? 'Queue not found' : 'Cannot load this queue' }}</h3>
        <p v-if="detailError.isNotFound">No queue named <strong class="font-mono">{{ queueName }}</strong> on {{ actingClusterSlug || 'this cluster' }}.</p>
        <p v-else>{{ describeApiError(detailError) }}</p>
        <button class="btn btn-ghost" @click="fetchAll">Retry</button>
        <button class="btn btn-ghost" @click="router.push(safeReturnTo(route.query.returnTo) || '/queues')">All queues</button>
      </div>
    </div>

    <template v-else-if="statusData">
      <!-- The queue, its facts and its buttons are the section's header line:
           this tab starts with what needs a look, then the counts now, then
           the window's activity under its own range. -->

      <!-- A failed refresh: the numbers below are the previous load's. -->
      <div v-if="detailStale" class="status-banner banner-bad view-banner">
        <span><strong>Could not refresh this queue</strong> · {{ describeApiError(detailStale) }} · the numbers below are from an earlier load</span>
      </div>

      <!-- What needs a look, worst first, each pointing at the tab where it is
           worked on. Quiet when there is nothing. -->
      <ul v-if="notes.length" class="qd-notes" aria-label="Notes on this queue">
        <li v-for="n in notes" :key="n.key" class="qd-note" :class="{ quiet: !n.sev }" :title="n.title || undefined">
          <span class="qd-note-mark" aria-hidden="true"><span v-if="n.sev" class="g" :class="n.sev" /></span>
          <span class="qd-note-text">{{ n.text }}</span>
          <router-link v-if="n.to" :to="n.to" class="btn btn-ghost qd-note-go">{{ n.go }}</router-link>
        </li>
      </ul>

      <!-- Right now: the counts as of the last good status read. Each opens the
           messages it counts. -->
      <div class="qd-now">
        <div class="counts-tiles qd-counts" role="group" aria-label="Messages now">
          <button type="button" class="count-tile" title="Show the pending messages" @click="goMessages('pending')">
            <span class="k">Pending</span>
            <span class="v" :class="pendingNumClass(totalMessages.pending)">{{ formatNumber(totalMessages.pending) }}</span>
          </button>
          <button type="button" class="count-tile" title="Show the messages being processed" @click="goMessages('processing')">
            <span class="k">Processing</span>
            <span class="v">{{ fmtCount(totalMessages.processing) }}</span>
          </button>
          <button type="button" class="count-tile" title="Show the completed messages" @click="goMessages('completed')">
            <span class="k">Completed</span>
            <span class="v">{{ fmtCount(totalMessages.completed) }}</span>
          </button>
          <button type="button" class="count-tile" title="Open the dead letter" @click="goDLQ">
            <span class="k">Dead letter</span>
            <span class="v">{{ formatNumber(totalMessages.deadLetter) }}</span>
          </button>
          <button type="button" class="count-tile" title="Show every message" @click="goMessages()">
            <span class="k">Total</span>
            <span class="v">{{ fmtCount(totalMessages.total) }}</span>
          </button>
        </div>
        <!-- Said only when the counts are not live. -->
        <p v-if="statusData.statsAge === null" class="qd-caption" title="Pending is an estimate until the broker’s first sample is ready">Counts warming up</p>
        <p v-else-if="statusData.statsAge > 0" class="qd-caption" title="The broker samples queue counts; these are from its last sample">Counts sampled {{ formatSpan(statusData.statsAge) }} ago</p>
      </div>

      <!-- The window every tile and the chart below cover. -->
      <div class="qd-sect">
        <h3>Activity</h3>
        <!-- The tiles are queue-ops': a node that did not answer the broker's
             gather took its share out of them (usePartial). -->
        <span v-if="opsPartial" class="chip chip-warn" role="status" :title="PARTIAL_TITLE"><span class="dot" />{{ opsPartial }}</span>
        <!-- A window handed over by another view: none of the presets is on. -->
        <span v-if="linkedWindow" class="tool-note qd-window">{{ rangeLabel }}</span>
        <div class="seg qd-range" role="group" aria-label="Time range">
          <button
            v-for="r in timeRanges"
            :key="r.value"
            :class="{ on: !linkedWindow && selectedRange === r.value }"
            :aria-pressed="!linkedWindow && selectedRange === r.value ? 'true' : 'false'"
            @click="contextFrom = ''; contextTo = ''; selectedRange = r.value"
          >{{ r.label }}</button>
        </div>
      </div>

      <!-- The window's metrics. A tile hands its series to the chart below. -->
      <div class="tile-grid tile-grid-4">
        <MetricTile
          label="Throughput"
          :value="throughput.current"
          :unit="throughput.current === '—' ? '' : '/s push'"
          :context="throughputContext"
          :series="throughputSeries"
          :labels="chartLabels"
          :value-format="fmtRate"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Push, pop and ack per second"
          :selected="selectedMetric === 'throughput'"
          @select="selectedMetric = 'throughput'"
        />
        <MetricTile
          label="Pending Δ"
          :value="pendingDeltaDisplay"
          :unit="pendingDeltaDisplay === '—' ? '' : 'msgs'"
          :context="pendingDeltaContext"
          :sparkline="pendingDeltaSeries"
          :labels="backlogLabels"
          :value-format="fmtCount"
          :severity="pendingDeltaSeverity"
          :loading="loadingOps"
          :error="opsError"
          tooltip="How much the pending count changed across the window, read by the broker once a minute: up is filling, down is draining"
          :selected="selectedMetric === 'pendingDelta'"
          @select="selectedMetric = 'pendingDelta'"
        />
        <MetricTile
          label="Time lag"
          :context="lagContext"
          :series="lagSeriesData"
          :labels="chartLabels"
          :value-format="fmtLagMs"
          :severity="lagSeverityKey"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Average / longest wait before a pop, per bucket"
          :selected="selectedMetric === 'timeLag'"
          @select="selectedMetric = 'timeLag'"
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
        </MetricTile>
        <MetricTile
          label="Fill ratio"
          :context="fillContext"
          :series="fillSeriesData"
          :labels="chartLabels"
          :value-format="fmtFillPct"
          :severity="fillSeverityKey"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Long-polls that came back with a message, of all long-polls on this queue"
          :selected="selectedMetric === 'fillRatio'"
          @select="selectedMetric = 'fillRatio'"
        >
          <template #value>
            <span v-if="fillLatest === null" class="num">—</span>
            <template v-else><span class="num" :class="fillSeverityKey">{{ fillLatest.toFixed(1) }}</span><i class="mr-unit">%</i></template>
          </template>
        </MetricTile>
        <MetricTile
          label="Errors"
          :value="formatNumber(errorsTotal)"
          unit="ack failures"
          :context="errorsContext"
          :sparkline="errorsSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          :severity="errorsSeverity"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Failed acks on this queue across the window"
          :selected="selectedMetric === 'errors'"
          @select="selectedMetric = 'errors'"
        />
        <MetricTile
          label="Parked"
          :value="formatNumber(Math.round(parkedLatest))"
          unit="long-polls"
          :context="parkedContext"
          :sparkline="parkedSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Long-polls waiting on this queue right now"
          :selected="selectedMetric === 'parked'"
          @select="selectedMetric = 'parked'"
        />
        <MetricTile
          label="Partitions Δ"
          context="created / deleted"
          :series="partitionOpsSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Partitions created and deleted across the window"
          :selected="selectedMetric === 'partitionsOps'"
          @select="selectedMetric = 'partitionsOps'"
        >
          <template #value>
            <span class="num">+{{ formatNumber(partitionCreatedTotal) }}</span>
            <span class="mr-sep">/</span>
            <span class="num mute">−{{ formatNumber(partitionDeletedTotal) }}</span>
          </template>
        </MetricTile>
        <MetricTile
          label="Consumed"
          :value="fmtCount(consumedTotal)"
          unit="msgs"
          :context="consumedContext"
          :sparkline="popSeries"
          :labels="chartLabels"
          :value-format="fmtCount"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Messages consumed since the queue was created, every consumer group; the trend is pops per bucket"
          :selected="selectedMetric === 'consumed'"
          @select="selectedMetric = 'consumed'"
        />
      </div>

      <section class="card focus" aria-labelledby="qd-focus-title">
        <div class="card-header">
          <h3 id="qd-focus-title">{{ focus.title }}</h3>
          <span class="card-sub">{{ focus.sub }}</span>
          <span class="muted">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="opsError" class="panel-err">{{ describeApiError(opsError) }}</div>
          <RowChart
            v-else
            :data="focus.sparkline || []"
            :series="focus.series || null"
            :labels="focus.labels || chartLabels"
            :tone="focus.tone || 'mute'"
            :value-format="focus.valueFormat || null"
            :unit="focus.unit"
            variant="full"
          />
        </div>
      </section>

      <!-- The queue's partitions as seeds, beside its configuration. -->
      <div class="qd-bottom">
        <div class="card flower-card">
          <div class="card-header"><h3>Partitions</h3></div>
          <PartitionSunflower
            :mode="partitionRows ? 'partitions' : 'queues'"
            :queue-name="queueName"
            :queues="partitionSeeds"
            :loading="partitionCount === null"
          />
        </div>

        <!-- What the status route reports (six of the queue's options), its
             priority and age, and which alert lines judge it. -->
        <div v-if="queueData?.config" class="card">
          <div class="card-header"><h3>Configuration</h3></div>
          <ul class="qd-settings">
            <!-- formatDuration(0) is the truthy string '0ms', so the raw value
                 is tested: no TTL must not read as instant expiry. -->
            <li>
              <span title="How long a pop holds its messages before they are delivered again">Lease time</span>
              <b>{{ queueData.config.leaseTime ? formatDuration(queueData.config.leaseTime * 1000) : '—' }}</b>
            </li>
            <li>
              <span title="Failed acks before the message is dead-lettered or dropped">Retry limit</span>
              <b>{{ queueData.config.retryLimit || 0 }}</b>
            </li>
            <li>
              <span title="How long a message waits after a failed ack before it comes back">Retry delay</span>
              <b>{{ formatNumber(queueData.config.retryDelay || 0) }} ms</b>
            </li>
            <li>
              <span title="Where a message that exhausts its retries lands">Dead letter</span>
              <b>{{ queueData.config.deadLetterQueue ? 'On' : 'Off' }}</b>
            </li>
            <li>
              <span title="Scheduler priority">Priority</span>
              <b>{{ queueData.priority ?? 0 }}</b>
            </li>
            <!-- The two the broker STORES and never READS (D2, re-confirmed
                 against the raft code 2026-09-26): shown, because they are on
                 the row, but dimmed and saying so. The editor omits them. -->
            <li class="is-inert" title="Stored on the queue and read by no push, pop, ack or maintenance path on this broker">
              <span>TTL</span>
              <b>{{ queueData.config.ttl ? formatDuration(queueData.config.ttl * 1000) : '∞' }}<i>not enforced</i></b>
            </li>
            <li class="is-inert" title="Stored on the queue and read by no push, pop, ack or maintenance path on this broker">
              <span>Max queue size</span>
              <b>{{ queueData.config.maxQueueSize ? formatNumber(queueData.config.maxQueueSize) : '∞' }}<i>not enforced</i></b>
            </li>
            <li>
              <span>Created</span>
              <b :title="formatTimestampUtc(queueData.createdAt)">{{ formatTimestamp(queueData.createdAt) }}<i>{{ formatRelative(queueData.createdAt) }}</i></b>
            </li>
            <!-- Not broker configuration: the lines this console judges the
                 queue by, stated and edited on its Alerts tab. -->
            <li>
              <span title="The lines this queue is judged by (Settings)">Alert lines</span>
              <router-link :to="queueTabLocation('alerts', queueName, route, listCarry(route))" class="qd-link">{{ ownLines ? 'This queue’s own' : 'The tenant’s' }}</router-link>
            </li>
          </ul>
          <p
            v-if="can('queueAdmin')"
            class="qd-config-note"
            title="The status route reports six of a queue’s twenty-one options. Configure reads all of them and offers the eighteen this broker acts on."
          >Configure shows every option.</p>
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
          <div class="card-body qd-form">
            <div v-if="deleteError" class="panel-err">{{ deleteError }}</div>
            <p class="qd-lead">Delete <strong>{{ queueName }}</strong>?</p>
            <p class="qd-help">Its {{ fmtCount(partitionCount) }} partitions and {{ fmtCount(totalMessages.total) }} messages are removed. This cannot be undone.</p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="closeDeleteModal">Cancel</button>
            <button class="btn btn-danger" :disabled="deleting" @click="deleteQueue">{{ deleting ? 'Deleting…' : 'Delete queue' }}</button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- The queue is FIXED: this page is about one queue, and a picker here
         would offer to push somewhere none of the numbers on screen describe. -->
    <PushMessageModal
      :open="pushOpen"
      :queue="queueName"
      queue-fixed
      @close="pushOpen = false"
      @pushed="fetchAll"
    />

    <!-- The editor reads the queue's full configuration itself when it opens;
         a save refetches the page rather than patching anything locally. -->
    <QueueConfigModal
      :open="configOpen"
      mode="edit"
      :queue="queueName"
      @close="configOpen = false"
      @saved="fetchAll"
    />

  </div>
  </QueuesLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useRouteState } from '@/composables/useRouteState'
import { listCarry, queueTabLocation, safeReturnTo, validWindow } from '@/composables/navigation'
import { DEFAULT_TIME_RANGE, TIME_RANGES as timeRanges, relativeTimeRange } from '../../../shared/timeRanges.mjs'
import {
  analytics, queues as queuesApi, system as systemApi,
  consumers as consumersApi, describeApiError,
} from '@/api'
import { formatNumber, formatDuration, toNum, latestFinite, trimIncompleteBuckets, useApi } from '@/composables/useApi'
import { formatChartLabel, formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import {
  laggingGroupsVerdict, pendingDepthAdvisory, PENDING_DEPTH_FLOOR,
} from '@/composables/useConflation'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { missingNodes, partialText, PARTIAL_TITLE } from '@/composables/usePartial'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { queueAlerts } from '@/composables/queueAlerts'
import { formatSpan } from '@/composables/settingsDoc'
import { queueAttention } from '@/composables/useAttention'
import {
  ackFailureSeverity, backlogSeverity, numTone, pendingDriftSeverity, timeLagSeverity,
} from '@/composables/useSeverity'
import MetricTile from '@/components/MetricTile.vue'
import PartitionSunflower, { MAX_SEEDS } from '@/components/PartitionSunflower.vue'
import { routeSupport } from '@/stores/routeSupport'
import RowChart from '@/components/RowChart.vue'
import PushMessageModal from '@/components/PushMessageModal.vue'
import QueueConfigModal from '@/components/QueueConfigModal.vue'
import QueuesLayout from '@/components/QueuesLayout.vue'

const route = useRoute()
const router = useRouter()
const queueName = computed(() => route.params.queueName)
const { can, actingClusterSlug } = useIdentity()
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
const opsPartial = computed(() => (opsError.value ? '' : partialText(missingNodes(opsData.value))))

const loadingOps = ref(true)

const showDeleteModal = ref(false)
const deleteError = ref(null)
const deleting = ref(false)

// The push form, opened on this page's queue. A queued push refetches the page
// rather than incrementing anything locally: the totals and the partition rows
// are the broker's, and guessing the new ones would drift from them.
const pushOpen = ref(false)

// The configuration editor. It fetches the queue's own 21-option echo when it
// opens (/api/v1/resources/queues/:name) rather than reading this page's
// `queueData.config`, which is the status route's six keys — an editor
// prefilled from six and merging twenty-one would be an editor that cannot say
// what the queue is.
const configOpen = ref(false)

const selectedRange = ref(DEFAULT_TIME_RANGE)
const contextFrom = ref(''), contextTo = ref('')
useRouteState({ range: selectedRange, from: contextFrom, to: contextTo })
const linkedWindow = computed(() => validWindow(contextFrom.value, contextTo.value))
// Preserve an investigation window received from another view.
const rangeLabel = computed(() => {
  if (linkedWindow.value) return `${formatTimestamp(linkedWindow.value.from)} → ${formatTimestamp(linkedWindow.value.to)}`
  const r = timeRanges.find(t => t.value === selectedRange.value)
  return `last ${r ? r.label : selectedRange.value}`
})

// Skeletons are first paint only: a refresh must leave the previous page on
// screen. `loading` is only ever true with no statusData, but the guard is
// written out so the rule is visible at the call site.
const firstLoad = computed(() => loading.value && !statusData.value)

const lastRefreshAt = ref(null)
// Live tick, off the app's one shared ticker.
const refreshAgo = useRefreshAgo(lastRefreshAt)

// The tile whose series the focus chart draws — same pattern as the Overview.
const selectedMetric = ref('throughput')

// ---------------------------------------------------------------------------
// Time range helpers
// ---------------------------------------------------------------------------
const getTimeRangeParams = () => {
  const { from, to } = linkedWindow.value || relativeTimeRange(selectedRange.value)
  return {
    from: from.toISOString(),
    to: to.toISOString(),
    queue: queueName.value,
  }
}

// The queue's identity + config come from /api/v1/status/queues/:name, the
// only endpoint that describes a log queue. The old `get_queue_v2` fallback
// read tables that no longer held any queue and rendered a healthy queue as
// 0 partitions / 0 messages / no config, so it is gone.
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
const partitionCount = computed(() => {
  const totals = statusData.value?.totals
  return totals && 'partitions' in totals ? totals.partitions : partitions.value.length
})

const totalMessages = computed(() => {
  const apiTotals = statusData.value?.totals
  if (apiTotals) {
    const msgs = apiTotals.messages || apiTotals
    return {
      total: statusData.value.statsAge === null ? null : msgs.total || 0,
      pending: Math.max(0, msgs.pending || 0),
      processing: statusData.value.statsAge === null ? null : msgs.processing || 0,
      completed: statusData.value.statsAge === null ? null : msgs.completed || 0,
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
// Throughput row — push / pop / ack rates per bucket
// ---------------------------------------------------------------------------
const throughput = computed(() => {
  // The push rate, as the tile's unit says and as the Overview's tile shows:
  // the latest *finite* bucket, so a still-flushing tail does not read 0, and
  // a dash when no bucket measured one.
  const v = latestFinite(history.value.map(x => x.pushPerSecond))
  return { current: v === null ? '—' : v.toFixed(1) }
})
const throughputSeries = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [
    { label: 'Push', data: h.map(x => toNum(x.pushPerSecond)) },
    { label: 'Pop',  data: h.map(x => toNum(x.popPerSecond)) },
    // Ack is the only semantically-coloured line here (it means "work landed");
    // the colour is resolved from --ok-500, never re-typed.
    { label: 'Ack',  data: h.map(x => toNum(x.ackPerSecond)) },
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
  if (throughputPeak.value === 0) return 'no traffic in the window'
  return `peak ${formatNumber(Math.round(throughputPeak.value))} /s`
})

// ---------------------------------------------------------------------------
// Pending Δ row — this queue's backlog, as the broker reads it once a minute
// (queue-ops `backlog` with `queue`: per bucket its last reading of what waits
// and what is in flight). VALUE = how much Pending changed across the window;
// the chart is Pending itself, so it comes back down when the queue drains.
//
// It is NOT push − pop: every consumer group pops each message, a redelivery
// pops it again, and retention and the DLQ take messages nobody popped, so
// summed over a window that difference drifts for ever. (Same source as the
// Overview's Pending Δ, for one queue.)
// ---------------------------------------------------------------------------
const backlog = computed(() => {
  const b = opsData.value?.backlog
  return Array.isArray(b) ? b.filter(x => toNum(x?.pending) !== null) : []
})
const backlogMultiDay = computed(() => {
  const b = backlog.value
  return b.length > 1 && new Date(b[0].bucket).toDateString() !== new Date(b[b.length - 1].bucket).toDateString()
})
const backlogLabels = computed(() =>
  backlog.value.map(b => formatChartLabel(new Date(b.bucket), backlogMultiDay.value))
)
const pendingDeltaSeries = computed(() => backlog.value.map(b => toNum(b.pending)))
const backlogSeries = computed(() => {
  if (!backlog.value.length) return null
  return [
    { label: 'Pending', data: pendingDeltaSeries.value },
    { label: 'In flight', data: backlog.value.map(b => toNum(b.processing)) },
  ]
})
const backlogLast = computed(() => latestFinite(pendingDeltaSeries.value))
// A change needs two readings; a broker that has just started reading has one.
const pendingDeltaLatest = computed(() => {
  const s = pendingDeltaSeries.value
  return s.length > 1 ? s[s.length - 1] - s[0] : null
})
const pendingDeltaDisplay = computed(() => {
  const v = pendingDeltaLatest.value
  if (v === null) return '—'
  if (v === 0) return '0'
  return (v > 0 ? '+' : '−') + formatNumber(Math.abs(v))
})
// Judged as a share of what was pushed into this queue in the same window:
// +1 000 is nothing on a queue that took half a million and a stall on one
// that took two thousand. (Same rule as the Dashboard's Pending Δ row.)
const pushedTotal = computed(() =>
  history.value.reduce((s, x) => s + (toNum(x.pushMessages) || 0), 0)
)
const pendingDeltaSeverity = computed(() =>
  pendingDriftSeverity({ delta: pendingDeltaLatest.value, pushed: pushedTotal.value })
)
const pendingDeltaContext = computed(() => {
  const now = backlogLast.value
  if (now === null) return 'no backlog readings in window'
  const waiting = `${formatNumber(now)} pending now`
  const v = pendingDeltaLatest.value
  if (v === null) return `${waiting} · first reading`
  if (v === 0) return `flat · ${waiting}`
  if (v < 0) return `draining · ${waiting}`
  const sev = pendingDeltaSeverity.value
  return `${sev === 'warn' || sev === 'bad' ? 'falling behind' : 'filling'} · ${waiting}`
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
// An age is already proportional, so it is judged against two lines and
// nothing else: this queue's own when Settings holds some, else the tenant's.
const settingsStore = useSettingsStore()
const queueLines = computed(() => settingsStore.linesFor(queueName.value))
const ownLines = computed(() => settingsStore.settings.value.queues.get(queueName.value) || null)
// A queue Kafka clients created (the queue listing says so; the status route
// this page reads does not, so the shared listing the sidebar keeps is asked).
const queuesStore = useQueuesStore()
const kafkaTopic = computed(() => queueData.value?.kafkaTopic === true
  || queuesStore.queueMeta.value.get(queueName.value)?.kafkaTopic === true)
// The tenant's consumer groups: the shared listing the sidebar keeps (this page
// never pays for a scan of its own), read here and by the queue's verdict.
const groupsStore = useGroupsStore()
const queueOpsStore = useQueueOpsStore()
const lagNumClass = (ms) => (
  ms === null || ms === undefined ? '' : numTone(timeLagSeverity(ms / 1000, queueLines.value))
)
// MetricRow.severity expects a key like 'warn' / 'bad'; map from the raw ms.
const lagSeverityKey = computed(() => lagNumClass(lagLatest.value.max))
const lagContext = computed(() => {
  const finite = lagPerBucket.value.map(x => x.max).filter(v => v !== null)
  if (!finite.length) return 'no pops in the window'
  const peak = Math.max(...finite)
  if (peak === 0) return 'zero across the window'
  return `longest ${fmtLagShort(peak)}`
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
  if (v === null) return 'no long-polls in the window'
  if (v >= 80) return 'high · consumers busy'
  if (v < 30)  return 'low · most long-polls come back empty'
  return 'balanced'
})
// No tone: a low fill ratio means the consumers asked more often than there
// was work, which is what an idle or over-provisioned pool looks like — a
// sizing observation, not a degradation. The context line says it in words.
const fillSeverityKey = computed(() => '')

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
// The denominator is DELIVERIES, not acks: queue-ops reports `ackSuccess` only
// at tenant scope, and every delivered message is a message somebody had to
// ack, so pops are the population the failures are drawn from. Stated in the
// context line so the number is never a share of something unnamed.
const deliveredTotal = computed(() =>
  history.value.reduce((s, x) => s + (toNum(x.popMessages) || 0), 0)
)
const errorsContext = computed(() => {
  const t = errorsTotal.value
  if (!t) return 'none in the window'
  const delivered = deliveredTotal.value
  if (!delivered) return `${formatNumber(t)} ack failure${t === 1 ? '' : 's'}`
  // A redelivery fails again: past the window's deliveries, a share would
  // read over 100%. Every delivery failed is the fact.
  if (t >= delivered) return `${formatNumber(t)} failures, every delivery`
  return `${formatNumber(t)} of ${formatNumber(delivered)} delivered (${((t / delivered) * 100).toFixed(2)}%)`
})
const errorsSeverity = computed(() =>
  ackFailureSeverity({ failed: errorsTotal.value, attempts: deliveredTotal.value }, queueLines.value)
)

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
  return `average ${parkedAvg.value.toFixed(1)}`
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
const consumedTotal = computed(() => statusData.value?.statsAge === null ? null :
  statusData.value?.totals?.consumed ??
    partitions.value.reduce((s, p) => s + (toNum(p.cursor?.totalConsumed) || 0), 0)
)
const popSeries = computed(() => history.value.map(x => toNum(x.popMessages)))
const consumedContext = computed(() => 'since created · trend is pops')

// ---------------------------------------------------------------------------
// Conflation — the one question the page's own two calls cannot answer
// (PLAN_CONFLATION §5.4).
//
// A conflating consumer group is delivered ONE message per partition, the
// newest, and commits past the rest. Its pending count is therefore LOG depth —
// positions still to retire — and not handler runs owed. "High pending depth"
// on a queue whose only lagging groups conflate is not an advisory, it is the
// policy working as declared, and a banner that cries wolf there is a banner
// operators learn to scroll past everywhere else.
//
// The group listing is a tenant-wide scan, which this page deliberately does
// not pay for on every queue (see fetchAll). So it is asked ONLY when the
// advisory would otherwise fire — a queue already deep enough to warn about —
// and never for the shallow queues that are nearly all of them.
// ---------------------------------------------------------------------------
// Sticky until one succeeds, rather than `groupsProbe.failed`: execute() clears
// the error before each retry goes out, so reading that flag directly would
// blink the banner off at the top of every 30s cycle and back on when the call
// failed again.
const probeFailed = ref(false)
const groupsProbe = useApi((config) => consumersApi.list(config), {
  immediate: false,
  onSuccess: () => { probeFailed.value = false },
  onError: () => { probeFailed.value = true },
})

/**
 * The groups on THIS queue, or null while the answer is not in — the listing is
 * tenant-wide, so it stays valid across a queue switch.
 */
const queueGroups = computed(() => {
  const d = groupsProbe.data.value
  if (!d) return null
  const rows = Array.isArray(d) ? d : (d.consumer_groups || [])
  return rows.filter(g => g.queueName === queueName.value)
})

const conflationVerdict = computed(() =>
  laggingGroupsVerdict(queueGroups.value, { failed: probeFailed.value })
)
const depthAdvisory = computed(() =>
  pendingDepthAdvisory(totalMessages.value.pending, conflationVerdict.value)
)

// Which groups are behind moves with the traffic, so the probe rides the
// refresh cycle — but only while the queue is deep enough for the answer to
// change what is on screen.
const probeGroupsIfDeep = () => {
  if (totalMessages.value.pending > PENDING_DEPTH_FLOOR) groupsProbe.refresh()
}

// ---------------------------------------------------------------------------
// Notes — what needs a look on this queue, worst first, one line each, each
// pointing at the tab where it is worked on:
//   alerts       the Alerts tab's verdicts (queueAlerts: the same rule as the
//                queue's mark in the list and the count on the tab)
//   ack failures only when their RATE earned a tone (ackFailureSeverity)
//   depth        the pending-depth advisory, or the fact that it is log depth
//                on a queue whose groups behind all conflate
//   dead letter  a standing to-do list, not an event: never toned
// ---------------------------------------------------------------------------
const listRow = computed(() => (queuesStore.queues.value || []).find(q => q?.name === queueName.value) || null)
const alerts = computed(() => queueAlerts(listRow.value, groupsStore.groups.value || [], settingsStore.linesFor, queueOpsStore.ops.value))
const tabAt = (view) => queueTabLocation(view, queueName.value, route, listCarry(route))
const plural = (n, one, many = `${one}s`) => `${formatNumber(n)} ${n === 1 ? one : many}`
const NOTE_RANK = { bad: 0, warn: 1 }

const notes = computed(() => {
  const out = []
  const a = alerts.value
  if (a.length) {
    out.push({
      key: 'alerts',
      sev: a[0].sev,
      text: `${a[0].title}${a[0].brief ? ` · ${a[0].brief}` : ''}${a.length > 1 ? ` · ${a.length - 1} more` : ''}`,
      title: a[0].detail,
      to: tabAt('alerts'),
      go: 'Alerts',
    })
  }
  // The Alerts line already says the acks fail (the same rule over the last
  // hour): a second line would repeat it.
  if (errorsSeverity.value && !a.some((x) => x.key === 'acks')) {
    const t = errorsTotal.value
    const d = deliveredTotal.value
    const share = !d ? '' : t >= d ? ' · every delivery' : ` · ${((t / d) * 100).toFixed(2)}% of deliveries`
    out.push({ key: 'acks', sev: errorsSeverity.value, text: `${plural(t, 'ack failure')} ${linkedWindow.value ? 'in' : 'in the'} ${rangeLabel.value}${share}` })
  }
  // Four outcomes, not two: 'none' below the floor, 'hold' while the
  // conflation answer is in flight (say nothing rather than flash a warning
  // that is retracted a round trip later), 'log-depth' when the number is real
  // but is not work, and 'fire'. A failed or unasked lookup lands on 'fire':
  // not knowing has never been the same as being fine.
  if (depthAdvisory.value === 'fire') {
    out.push({
      key: 'depth',
      // Attention, never red: a deep queue is a queue with work in it.
      sev: 'warn',
      text: `Deep backlog: ${plural(totalMessages.value.pending, 'message')} waiting`,
      title: `More than ${formatNumber(PENDING_DEPTH_FLOOR)} pending: consider more consumers, or check their lag.`,
      to: tabAt('consumers'),
      go: 'Consumer groups',
    })
  } else if (depthAdvisory.value === 'log-depth') {
    out.push({
      key: 'depth',
      sev: null,
      text: `${formatNumber(totalMessages.value.pending)} pending is log depth: every group behind conflates`,
      title: 'One delivery per partition, the newest: the work left is partitions to visit, not messages.',
      to: tabAt('consumers'),
      go: 'Consumer groups',
    })
  }
  if (totalMessages.value.deadLetter > 0) {
    out.push({
      key: 'dlq',
      sev: null,
      text: `${plural(totalMessages.value.deadLetter, 'message')} in dead letter`,
      to: tabAt('failed'),
      go: 'Dead letter',
    })
  }
  return out.sort((x, y) => (NOTE_RANK[x.sev] ?? 2) - (NOTE_RANK[y.sev] ?? 2))
})

// ---------------------------------------------------------------------------
// Numeric tone for top-strip pending
// ---------------------------------------------------------------------------
// Seconds of work at the rate this queue is actually acking, not a raw depth:
// 50 000 pending drains in four seconds at 12k/s and never drains at 0/s.
const drainPerSec = computed(() => latestFinite(history.value.map(x => x.ackPerSecond)))
const pendingNumClass = (n) => backlogSeverity({ pending: n, drainPerSec: drainPerSec.value }, queueLines.value)

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
const fmtCount = (n) => n === null || n === undefined ? '—' : formatNumber(Math.round(Number(n) || 0))
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
const formatRelative = (ts) => {
  if (!ts) return '—'
  const diffMs = Date.now() - new Date(ts).getTime()
  if (diffMs < 0) return 'just now'
  if (diffMs < 60_000) return `${Math.floor(diffMs / 1000)}s ago`
  if (diffMs < 3600_000) return `${Math.floor(diffMs / 60_000)}m ago`
  if (diffMs < 86400_000) return `${Math.floor(diffMs / 3600_000)}h ago`
  if (diffMs < 30 * 86400_000) return `${Math.floor(diffMs / 86400_000)}d ago`
  return formatTimestamp(ts)
}
// ---------------------------------------------------------------------------
// Navigation helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Focus chart — the tiles hand it their series.
// ---------------------------------------------------------------------------
const focus = computed(() => {
  switch (selectedMetric.value) {
    case 'pendingDelta': return { title: 'Pending', sub: 'waiting and in flight, read by the broker once a minute', series: backlogSeries.value, labels: backlogLabels.value, valueFormat: fmtCount, unit: 'msgs' }
    case 'timeLag': return { title: 'Time lag', sub: 'average and maximum delivery delay per bucket', series: lagSeriesData.value, valueFormat: fmtLagMs, unit: 'ms' }
    case 'fillRatio': return { title: 'Fill ratio', sub: 'long-polls that came back with a message', series: fillSeriesData.value, valueFormat: fmtFillPct, unit: '%' }
    case 'errors': return { title: 'Ack failures', sub: 'failed acks per bucket', sparkline: errorsSeries.value, valueFormat: fmtCount, unit: 'ack failures', tone: errorsSeverity.value || 'mute' }
    case 'parked': return { title: 'Parked', sub: 'long-polls waiting on this queue', sparkline: parkedSeries.value, valueFormat: fmtCount, unit: 'long-polls' }
    case 'partitionsOps': return { title: 'Partitions', sub: 'created and deleted per bucket', series: partitionOpsSeries.value, valueFormat: fmtCount, unit: 'count' }
    case 'consumed': return { title: 'Pops', sub: 'messages popped per bucket, every consumer group', sparkline: popSeries.value, valueFormat: fmtCount, unit: 'msgs per bucket' }
    default: return { title: 'Throughput', sub: 'push, pop and ack per second', series: throughputSeries.value, valueFormat: fmtRate, unit: 'msgs / sec' }
  }
})


// Small queues have one seed per partition; large ones use the queue totals.
// The queue's verdict — shared with the Overview, Queues and the sidebar —
// colours only the partitions that hold work. It reads the shared listing the
// sidebar keeps; this page never pays for a consumer-group scan of its own.
const queueSev = computed(() => {
  const groups = groupsStore.groups.value
  if (groups === null || groupsStore.error.value !== null) return 'ok'
  const here = { name: queueName.value, messages: { pending: totalMessages.value.pending }, kafkaTopic: kafkaTopic.value }
  return queueAttention([here], groups, settingsStore.linesFor, queueOpsStore.ops.value)[0]?.sev || 'ok'
})
// Match the overview: only scan individual partitions while each fits a seed.
const getPartitions = routeSupport.guard('resources-partitions', (params) =>
  queuesApi.partitions(params, { probe: true }))
const partitionData = ref(null)
const partitionRows = computed(() => {
  if (partitionCount.value === null || partitionCount.value > MAX_SEEDS || partitionData.value?.truncated) return null
  return partitionData.value?.partitions ?? null
})
const fetchPartitions = async () => {
  if (partitionCount.value === null || partitionCount.value > MAX_SEEDS) {
    partitionData.value = null
    return
  }
  const name = queueName.value
  try {
    const r = await getPartitions({ queue: name, limit: MAX_SEEDS })
    if (name !== queueName.value) return
    partitionData.value = r.data
  } catch {
    // Keep the last rows; without them the flower uses the queue's totals.
  }
}
const partitionSeeds = computed(() => partitionRows.value
  ? partitionRows.value.map(p => ({
    name: String(p.partition), partitions: 1, pending: p.pending,
    lag: p.lagSeconds, sev: p.pending > 0 ? queueSev.value : 'ok',
  }))
  : [{ name: queueName.value, partitions: partitionCount.value, pending: totalMessages.value.pending, sev: queueSev.value }])

function goMessages(status = '') {
  router.push(queueTabLocation('messages', queueName.value, route, { ...listCarry(route), ...(status ? { status } : {}) }))
}
function goDLQ() {
  router.push(tabAt('failed'))
}

// ---------------------------------------------------------------------------
// Fetchers
// ---------------------------------------------------------------------------
// One call describes the queue: /api/v1/status/queues/:name. The old second
// fetch of /resources/queues/:name doubled the request rate of the busiest
// detail page for a fallback that could only ever render an all-zero view.
const fetchQueueDetail = async () => {
  const name = queueName.value
  try {
    const r = await analytics.getQueueDetail(name, { includePartitions: false })
    if (name !== queueName.value) return
    statusData.value = r.data
    detailError.value = null
    detailStale.value = null
    await fetchPartitions()
  } catch (err) {
    if (name !== queueName.value) return
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
  // Two calls for every queue: queue detail (for config + totals) and queue-ops
  // (for the metric table). Consumer-group + lagging-partition scans are
  // expensive at scale and intentionally skipped here — they live on
  // the dedicated /consumers page.
  //
  // Small queues also load the flower's per-partition rows after the totals.
  // A queue deep enough to warn about also probes conflation, because on a
  // conflating group that warning would be wrong.
  await Promise.all([fetchQueueDetail(), fetchOps()])
  probeGroupsIfDeep()
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
watch([selectedRange, contextFrom, contextTo], fetchOps)
watch(queueName, () => {
  loading.value = true
  detailError.value = null
  detailStale.value = null
  statusData.value = null
  partitionData.value = null
  opsData.value = null
  opsError.value = null
  fetchAll()
})

onMounted(fetchAll)
</script>

<style scoped>
.view-container { display: flex; flex-direction: column; gap: 16px; }
.view-container > .view-banner { margin-bottom: 0; }
.qd-skel { display: flex; flex-direction: column; gap: 16px; }

/* The counts and, when they are not live, how old they are. */
.qd-now { display: flex; flex-direction: column; gap: 8px; }
.qd-caption { margin: 0; align-self: flex-end; font-size: 12px; color: var(--text-low); }

/* The window's head: its name, then its range, which every tile and the chart
   below follow. A step of air above it parts it from the counts. */
.qd-sect { display: flex; align-items: center; gap: 8px 12px; flex-wrap: wrap; min-height: 32px; margin-top: 4px; }
.qd-sect h3 { margin: 0; font-size: 13px; font-weight: 600; color: var(--text-hi); }
.qd-window { margin-left: auto; }
.qd-range { margin-left: auto; }
.qd-window + .qd-range { margin-left: 0; }

.qd-delete { color: var(--text-mid); }
.qd-delete:hover { color: var(--ember-400) !important; }

/* --- Notes: one line each, the mark in front, the tab to go to behind ------- */
.qd-notes {
  list-style: none; margin: 0; padding: 0;
  border: 1px solid var(--bd); border-radius: var(--r-card); background: var(--ink-2);
}
.qd-note {
  display: flex; align-items: center; gap: 12px;
  min-height: 40px; padding: 4px 8px 4px 16px;
  border-top: 1px solid var(--bd-soft); font-size: 13px;
}
.qd-note:first-child { border-top: 0; }
.qd-note-mark { flex: none; width: 10px; display: inline-flex; justify-content: center; }
.qd-note-text { flex: 1; min-width: 0; color: var(--text-hi); overflow-wrap: anywhere; }
.qd-note.quiet .qd-note-text { color: var(--text-mid); }
.qd-note-go { flex: none; }

/* --- The counts: five, one row; two columns on a phone ---------------------- */
.qd-counts { grid-template-columns: repeat(5, minmax(0, 1fr)); }
@media (max-width: 720px) {
  .qd-counts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .qd-counts > .count-tile:last-child { grid-column: span 2; }
}

/* MetricTile internals this page's custom values use. */
:deep(.mr-sep) { color: var(--text-low); margin: 0 4px; font-weight: 400; }
:deep(.mr-unit) { font-style: normal; color: var(--text-low); margin-left: 3px; font-size: 11px; font-weight: 400; }

/* --- The bottom row: the flower beside the configuration -------------------- */
.qd-bottom { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.6fr); gap: 16px; align-items: start; }
.qd-bottom > .card { margin: 0; }
@media (max-width: 1100px) { .qd-bottom { grid-template-columns: minmax(0, 1fr); } }

/* Configuration: the editor's rows without the controls. */
.qd-settings { list-style: none; margin: 0; padding: 0 16px; }
.qd-settings li {
  display: flex; align-items: baseline; justify-content: space-between; gap: 16px;
  min-height: 40px; padding: 8px 0; border-top: 1px solid var(--bd-soft); font-size: 13px;
}
.qd-settings li:first-child { border-top: 0; }
.qd-settings li > span { color: var(--text-mid); }
.qd-settings li > b { font-family: var(--font-mono); font-size: 12px; font-weight: 400; color: var(--text-hi); font-variant-numeric: tabular-nums; text-align: right; }
/* Stored and never read: the figure stays, a step quieter, and says so. */
.qd-settings li.is-inert > b { color: var(--text-low); }
.qd-settings i { margin-left: 8px; font-family: var(--font-sans); font-style: normal; font-size: 12px; font-weight: 400; color: var(--text-low); }
.qd-link { color: var(--text-hi); text-decoration: underline; text-decoration-color: var(--bd-hi); text-underline-offset: 3px; }
.qd-link:hover { text-decoration-color: currentColor; }
.qd-config-note { margin: 0; padding: 12px 16px; border-top: 1px solid var(--bd); font-size: 12px; color: var(--text-low); }

/* --- The delete modal -------------------------------------------------------- */
.qd-form { display: flex; flex-direction: column; gap: 8px; }
.qd-form .panel-err { margin: 0 0 4px; }
.qd-lead { margin: 0; font-size: 13px; color: var(--text-hi); overflow-wrap: anywhere; }
.qd-lead strong { font-weight: 600; }
.qd-help { margin: 0; font-size: 12px; color: var(--text-low); }

@media (max-width: 640px) {
  .qd-note { padding-left: 12px; }
  .qd-settings { padding: 0 12px; }
}
</style>
