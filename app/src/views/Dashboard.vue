<template>
  <div class="view-container dash">

    <!--
      ========================================================================
      Head — the page's name, the live tick and the range, on one line. Whose
      numbers these are is stated once, in the sidebar's tenant row.
      ========================================================================
    -->
    <PageHead title="Overview" :live="refreshAgo">
      <template #range>
        <div class="seg" role="group" aria-label="Time range">
          <button
            v-for="r in timeRanges"
            :key="r.value"
            :class="{ on: selectedRange === r.value }"
            :aria-pressed="selectedRange === r.value ? 'true' : 'false'"
            @click="selectQuickRange(r.value)"
          >{{ r.label }}</button>
        </div>
      </template>
    </PageHead>

    <!--
      ========================================================================
      The answer first: a verdict, the sentence behind it, what needs you,
      and the partitions drawn as a sunflower. Every rule the verdict uses is
      printed under the list — nothing here is decided out of sight.
      ========================================================================
    -->
    <section class="hero" aria-labelledby="dash-headline">
      <div class="hero-left">
        <div>
          <span class="hero-status">
            <span class="g" :class="statusGlyph" aria-hidden="true" />{{ statusWord }}
          </span>
          <h2 id="dash-headline" class="hero-title">{{ headline }}</h2>
          <p class="hero-sum">
            <template v-if="pushNow !== null">{{ formatNumber(queues.length) }} {{ queues.length === 1 ? 'queue takes' : 'queues take' }} in <b>{{ fmtMsgRate(pushNow) }}</b> and deliver <b>{{ fmtMsgRate(popNow) }}</b> to {{ formatNumber(consumers.length) }} consumer {{ consumers.length === 1 ? 'group' : 'groups' }}.</template>
            <template v-else>No traffic samples in the last {{ selectedRange }}.</template>
            <template v-if="pendingNow !== null"> The backlog is <b>{{ formatNumber(pendingNow) }}</b><template v-if="pendingDeltaLatest !== null"> ({{ pendingDeltaDisplay }} {{ pendingDeltaSpan }})</template>.</template>
            <template v-if="lagMaxSeconds !== null"> The oldest message has waited <b>{{ fmtLagSeconds(lagMaxSeconds) }}</b>.</template>
            <template v-if="ackAttempts > 0">{{ ' ' }}<b>{{ ackFailPct }}</b> of acks failed in this window.</template>
          </p>
        </div>

        <div class="card issues">
          <div class="issues-head">
            <b>Open issues</b>
            <span class="issues-count">{{ issuesCountText }}</span>
          </div>
          <ul v-if="issues.length" class="issues-list">
            <li v-for="i in issues" :key="i.key">
              <button class="issue" @click="$router.push(i.to)">
                <span class="g" :class="i.sev" aria-hidden="true" />
                <span class="issue-what">{{ i.name }}</span>
                <span class="issue-why">{{ i.why }}</span>
                <span class="issue-go">Open ›</span>
              </button>
            </li>
          </ul>
          <div v-else class="issues-empty">
            <template v-if="statusKnown">Nothing needs you. Every queue holding messages has a consumer, no group is a minute behind, and ack failures are within limits.</template>
            <template v-else-if="loadingQueues || loadingConsumers">Reading queues and consumer groups…</template>
            <template v-else>Cannot judge: {{ queuesFailed ? queuesErrorText : consumersErrorText }}</template>
          </div>
          <details class="issues-rules">
            <summary>How this is decided</summary>
            <p>
              Worked out in the browser from <code>GET /api/v1/resources/queues</code>,
              <code>GET /api/v1/consumer-groups</code> and this window's
              <code>queue-ops</code>. A queue needs you when a consumer group on it is
              more than 1 minute behind (5 minutes: failing), or when it holds
              messages and no consumer group reads it — a group that has never
              consumed does not count as a reader. Ack failures count when their
              share of the window's acks crosses the product's threshold. The
              sidebar, Queues and Consumer groups use the same rule.
            </p>
          </details>
        </div>
      </div>

      <div class="card flower-card">
        <div class="card-header">
          <h3>Partitions</h3>
          <span class="muted">{{ queuesFailed ? '—' : `${formatNumber(totalPartitions)} across ${formatNumber(queues.length)} queues` }}</span>
        </div>
        <PartitionSunflower :queues="sunflowerQueues" :loading="loadingQueues" :error="queuesQ.error.value" />
      </div>
    </section>

    <!--
      ========================================================================
      Right now — point-in-time counts, as of the last good fetch. The range
      above does not bound these.
      ========================================================================
    -->
    <div class="counts-tiles" role="group" aria-label="Right now">
      <div class="count-tile" title="Messages currently stored for this tenant (retention has already swept the rest)">
        <span class="k">Stored</span>
        <span class="v">{{ overviewFailed ? '—' : formatNumber(overview?.messages?.total ?? 0) }}</span>
      </div>
      <button class="count-tile" @click="$router.push('/queues')">
        <span class="k">Queues</span>
        <span class="v">{{ overviewFailed ? '—' : formatNumber(overview?.queues ?? 0) }}</span>
      </button>
      <div class="count-tile">
        <span class="k">Partitions</span>
        <span class="v">{{ queuesFailed ? '—' : formatNumber(totalPartitions) }}</span>
      </div>
      <button class="count-tile" @click="$router.push('/consumers')">
        <span class="k">Consumer groups</span>
        <span class="v">{{ consumersFailed ? '—' : formatNumber(consumers?.length || 0) }}</span>
      </button>
      <div class="count-tile">
        <span class="k">Pending</span>
        <span class="v num" :class="pendingNumClass(overview?.messages?.pending)">{{ overviewFailed ? '—' : formatNumber(overview?.messages?.pending ?? 0) }}</span>
      </div>
      <div class="count-tile">
        <span class="k">Completed</span>
        <span class="v">{{ overviewFailed ? '—' : formatNumber(overview?.messages?.completed ?? 0) }}</span>
      </div>
    </div>

    <!--
      ========================================================================
      Headline metrics. A tile hands its series to the chart below; it does
      not grow. Same sources, same verdicts as the old metric table.
      ========================================================================
    -->
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
        tooltip="Push / pop / ack rates for this tenant, summed across its queues (from queue-ops)."
        :selected="selectedMetric === 'throughput'"
        @select="selectMetric('throughput')"
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
        tooltip="Messages waiting for a consumer, read by the broker once a minute. The value is how much that changed across the window: up = falling behind, down = catching up."
        :selected="selectedMetric === 'pendingDelta'"
        @select="selectMetric('pendingDelta')"
      />
      <MetricTile
        label="Time lag"
        :context="lagContext"
        :series="lagSeriesData"
        :labels="chartLabels"
        :value-format="fmtLagMs"
        :severity="lagSeverity"
        :loading="loadingOverview"
        :error="overviewError"
        tooltip="Headline is the tenant's current oldest-message age (avg / max). The chart is pop-sampled per bucket, so it has gaps whenever nothing was consumed — it cannot report the lag of a stalled queue."
        :selected="selectedMetric === 'timeLag'"
        @select="selectMetric('timeLag')"
      >
        <template #value>
          <!-- `0` and "no sample" are different answers. -->
          <template v-if="lagAvgSeconds === null && lagMaxSeconds === null">
            <span class="num">—</span>
          </template>
          <template v-else>
            <span class="num" :class="lagNumClass(lagAvgSeconds)">{{ fmtLagSeconds(lagAvgSeconds) }}</span>
            <span class="mr-sep">/</span>
            <span class="num" :class="lagNumClass(lagMaxSeconds)">{{ fmtLagSeconds(lagMaxSeconds) }}</span>
          </template>
        </template>
      </MetricTile>
      <MetricTile
        label="Errors"
        :value="formatNumber(errorTotal)"
        :unit="'ack failures'"
        :context="errorContext"
        :sparkline="errorSeriesData"
        :labels="chartLabels"
        :value-format="fmtCount"
        :severity="errorSeverity"
        :loading="loadingOps"
        :error="opsError"
        tooltip="Ack failures for this tenant across the window. DLQ depth is a current snapshot, not a per-window count, so it is shown in the context line."
        :selected="selectedMetric === 'errors'"
        @select="selectMetric('errors')"
      />
    </div>

    <!-- The chart the tiles hand their series to. -->
    <section class="card focus" aria-labelledby="focus-title">
      <div class="card-header">
        <h3 id="focus-title">{{ focus.title }}</h3>
        <span class="card-sub">{{ focus.sub }}</span>
        <span class="muted">last {{ selectedRange }}</span>
      </div>
      <div class="card-body">
        <div v-if="focus.error" class="panel-err">{{ describeApiError(focus.error) }}</div>
        <RowChart
          v-else
          :data="focus.sparkline || []"
          :series="focus.series || null"
          :labels="focus.labels || []"
          :tone="focus.tone || 'mute'"
          :value-format="focus.valueFormat || null"
          :unit="focus.unit"
          variant="full"
        />
      </div>
    </section>

    <!-- Delivery and housekeeping — this tenant, every queue. -->
    <section class="dash-sect" aria-labelledby="sect-tenant">
      <div class="sect-head">
        <h3 id="sect-tenant">Delivery and housekeeping</h3>
        <span>{{ actingTenantSlug || 'this tenant' }} · every queue</span>
      </div>
      <div class="tile-grid tile-grid-4">
        <MetricTile
          label="Parked"
          :value="parkedLatest === null ? '—' : formatNumber(Math.round(parkedLatest))"
          :unit="parkedLatest === null ? '' : 'long-polls'"
          :context="parkedContext"
          :series="parkedSeriesData"
          :labels="chartLabels"
          :value-format="fmtCount"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Long-poll consumer connections currently waiting for work, summed across all queues."
          :selected="selectedMetric === 'parked'"
          @select="selectMetric('parked')"
        />
        <MetricTile
          label="Fill ratio"
          :context="fillContext"
          :series="fillSeriesData"
          :labels="chartLabels"
          :value-format="fmtFillPct"
          :severity="fillSeverity"
          :loading="loadingOps"
          :error="opsError"
          tooltip="Long-polls returning a message ÷ all long-poll completions, across all queues."
          :selected="selectedMetric === 'fillRatio'"
          @select="selectMetric('fillRatio')"
        >
          <template #value>
            <span v-if="fillLatest === null" class="num">—</span>
            <template v-else><span class="num" :class="fillSeverity">{{ fillLatest.toFixed(1) }}</span><i class="mr-unit">%</i></template>
          </template>
        </MetricTile>
        <MetricTile
          label="Partitions"
          context="created / deleted in window"
          :series="partitionSeriesData"
          :labels="chartLabels"
          :value-format="fmtCount"
          :loading="loadingOps"
          :error="opsError"
          :selected="selectedMetric === 'partitions'"
          @select="selectMetric('partitions')"
        >
          <template #value>
            <span class="num">+{{ formatNumber(partitionCreatedTotal) }}</span>
            <span class="mr-sep">/</span>
            <span class="num mute">−{{ formatNumber(partitionDeletedTotal) }}</span>
          </template>
        </MetricTile>
        <MetricTile
          label="Retention"
          :context="retentionContext"
          :series="retentionSeriesData"
          :labels="retentionLabels"
          :value-format="fmtCount"
          :error="retentionError"
          tooltip="Messages deleted by the retention / eviction workers."
          :selected="selectedMetric === 'retention'"
          @select="selectMetric('retention')"
        >
          <template #value>
            <span v-if="retentionTotal === null" class="num">—</span>
            <template v-else><span class="num">{{ formatNumber(retentionTotal) }}</span><i class="mr-unit">msgs</i></template>
          </template>
        </MetricTile>
      </div>
    </section>

    <!-- The cell — every tenant on it. Operator routes only. -->
    <section v-if="can('operator')" class="dash-sect" aria-labelledby="sect-cell">
      <div class="sect-head">
        <h3 id="sect-cell">Cell</h3>
        <span class="mono">{{ actingCellSlug || 'unknown cell' }}</span>
        <span class="sect-scope">Shared by every tenant on this cell, not just {{ actingTenantSlug || 'this tenant' }}</span>
      </div>
      <div class="tile-grid tile-grid-3">
        <MetricTile
          label="Batch efficiency"
          context="rows per batch: push · pop · ack"
          :error="statusError"
          :loading="loadingStatus"
          :spark="false"
          tooltip="Average rows per batch across every tenant on this cell. Higher = less per-commit overhead."
        >
          <template #value>
            <span class="num">{{ batchEfficiency.push }}</span><span class="mr-sep">·</span><span class="num">{{ batchEfficiency.pop }}</span><span class="mr-sep">·</span><span class="num">{{ batchEfficiency.ack }}</span>
          </template>
        </MetricTile>
        <MetricTile
          label="Event loop"
          :context="eventLoopContext"
          :series="elSeriesData"
          :labels="statusChartLabels"
          :value-format="(v) => v + ' ms'"
          :severity="elNumClass(maxEventLoopLag)"
          :loading="loadingStatus"
          :error="statusError"
          :selected="selectedMetric === 'eventLoop'"
          @select="selectMetric('eventLoop')"
        >
          <template #value>
            <template v-if="avgEventLoopLag === null && maxEventLoopLag === null"><span class="num">—</span></template>
            <template v-else>
              <span class="num" :class="elNumClass(avgEventLoopLag)">{{ avgEventLoopLag ?? '—' }}</span><i class="mr-unit">ms</i>
              <span class="mr-sep">/</span>
              <span class="num" :class="elNumClass(maxEventLoopLag)">{{ maxEventLoopLag ?? '—' }}</span><i class="mr-unit">ms</i>
            </template>
          </template>
        </MetricTile>
        <MetricTile
          label="Queen CPU"
          :context="cpuContext"
          :series="cpuSeriesData"
          :labels="cpuLabels"
          :value-format="(v) => v.toFixed(1) + '%'"
          :loading="loadingStatus"
          :error="cpuError"
          :selected="selectedMetric === 'cpu'"
          @select="selectMetric('cpu')"
        >
          <template #value>
            <span v-if="cpuLatest === null" class="num">—</span>
            <template v-else><span class="num">{{ cpuLatest.toFixed(1) }}</span><i class="mr-unit">%</i></template>
          </template>
        </MetricTile>
      </div>
    </section>

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
          <span class="card-sub">{{ queuesFailed ? '—' : `${enrichedQueues.length} queues` }}</span>
          <span class="muted">{{ stamp(queuesQ) }}</span>
        </div>

        <div v-if="loadingQueues" class="card-body entity-list">
          <div v-for="i in 6" :key="i" class="skeleton" style="height:48px; border-radius:var(--r-card);" />
        </div>

        <div v-else-if="queuesFailed" class="card-body">
          <div class="panel-err">{{ queuesErrorText }}</div>
        </div>

        <div v-else-if="topPendingQueues.length" class="card-body entity-list">
          <button
            v-for="q in topPendingQueues"
            :key="q.name"
            class="entity-row"
            @click="$router.push(`/queues/${encodeURIComponent(q.name)}`)"
          >
            <div class="entity-head">
              <span class="g" :class="queueGlyph(q)" aria-hidden="true" />
              <span class="entity-name">{{ q.name }}</span>
              <span class="entity-right num" :class="lagNumClass(q._lag)">
                {{ q._lag > 0 ? fmtLagSeconds(q._lag) : '—' }}
              </span>
            </div>
            <div class="entity-meta">
              <span class="bar bar-meta">
                <i :class="queueGlyph(q) === 'ok' ? '' : queueGlyph(q)" :style="{ width: q._depthPct + '%' }" />
              </span>
              <span class="meta-text">
                <strong>{{ q._pending === null ? '—' : formatNumber(q._pending) }}</strong> pending
                <span class="meta-sep">·</span>
                {{ formatNumber(q.partitions || 1) }} {{ (q.partitions || 1) === 1 ? 'partition' : 'partitions' }}
              </span>
            </div>
          </button>
        </div>

        <div v-else class="card-body"><div class="panel-msg">No queues</div></div>

        <div class="card-foot">
          <a class="card-foot-link" @click="$router.push('/queues')">All queues ›</a>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Consumer groups by lag</h3>
          <span class="card-sub">{{ consumersFailed ? '—' : `${consumers.length} total · ${laggingCount} lagging` }}</span>
          <span class="muted">{{ stamp(consumersQ) }}</span>
        </div>

        <div v-if="loadingConsumers" class="card-body entity-list">
          <div v-for="i in 6" :key="i" class="skeleton" style="height:48px; border-radius:var(--r-card);" />
        </div>

        <div v-else-if="consumersFailed" class="card-body">
          <div class="panel-err">{{ consumersErrorText }}</div>
        </div>

        <div v-else-if="sortedConsumers.length" class="card-body entity-list">
          <button
            v-for="g in sortedConsumers.slice(0, 6)"
            :key="g.name + '@' + g.queueName"
            class="entity-row"
            @click="$router.push('/consumers')"
          >
            <div class="entity-head">
              <span class="g" :class="groupGlyph(g)" aria-hidden="true" />
              <span class="entity-name">{{ g.queueName || '?' }}</span>
              <!-- A conflating group is delivered ONE message per partition —
                   the newest — so the partitions still to visit are the whole
                   of the work left, and they take the lead figure. -->
              <span
                v-if="isConflating(g)"
                class="entity-right num"
                :class="{ warn: (g.partitionsWithLag || 0) > 0 }"
              >
                {{ (g.partitionsWithLag || 0) > 0 ? `${g.partitionsWithLag} behind` : '—' }}
              </span>
              <span v-else class="entity-right num" :class="lagNumClass(g.maxTimeLag)">
                {{ (g.maxTimeLag || 0) > 0 ? fmtLagSeconds(g.maxTimeLag) : '—' }}
              </span>
            </div>
            <div class="entity-meta">
              <span class="meta-text">
                <span v-if="g.name === '__QUEUE_MODE__'" class="meta-tag">queue mode</span>
                <span v-if="isConflating(g)" class="meta-tag meta-tag-cfl">conflation</span>
                <span v-if="g.name !== '__QUEUE_MODE__'"><strong>{{ g.name }}</strong></span>
                <template v-if="isConflating(g)">
                  <span class="meta-sep">·</span>
                  <span class="num">{{ formatNumber(g.totalLag || 0) }} log lag</span>
                  <span class="meta-sep">·</span>
                  <span class="num" :class="lagNumClass(g.maxTimeLag)">{{ (g.maxTimeLag || 0) > 0 ? fmtLagSeconds(g.maxTimeLag) : '0s' }} time lag</span>
                </template>
                <span class="meta-sep">·</span>
                <!-- One row per (partition, group) cursor — NOT a consumer
                     count: one process on a 32-partition queue is 32 rows. -->
                {{ formatNumber(g.members || 0) }} {{ (g.members || 0) === 1 ? 'partition' : 'partitions' }} assigned
                <template v-if="!isConflating(g) && (g.partitionsWithLag || 0) > 0">
                  <span class="meta-sep">·</span>
                  <span class="num warn">{{ formatNumber(g.partitionsWithLag) }} lagging</span>
                </template>
              </span>
            </div>
          </button>
        </div>

        <div v-else class="card-body"><div class="panel-msg">No consumer groups</div></div>

        <div class="card-foot">
          <a class="card-foot-link" @click="$router.push('/consumers')">All consumer groups ›</a>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
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
import { isConflating } from '@/composables/useConflation'
import { semanticColors } from '@/composables/useChartTheme'
import {
  ackFailureSeverity, backlogSeverity, eventLoopSeverity, numTone,
  pendingDriftSeverity, timeLagSeverity,
} from '@/composables/useSeverity'
import { groupAttention, queueAttention } from '@/composables/useAttention'
import { useGroupsStore } from '@/stores/groupsStore'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { stamp } from '@/composables/useStamp'
import { useIdentity } from '@/stores/identity'
import MetricTile from '@/components/MetricTile.vue'
import PartitionSunflower from '@/components/PartitionSunflower.vue'
import PageHead from '@/components/PageHead.vue'
import RowChart from '@/components/RowChart.vue'

// The scope strip states all three slugs, so it is built from identity and
// never from a fetch — it must survive a failed load and an empty tenant.
const { can, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()

// ---------------------------------------------------------------------------
// Range. Quick ranges only — this view has no Custom mode, so there is no
// custom sub-row and no applied/typed split to keep.
// ---------------------------------------------------------------------------
const selectedRange = ref('1h')
const timeRanges = [
  { label: '1h',  value: '1h',  minutes: 60 },
  { label: '6h',  value: '6h',  minutes: 360 },
  { label: '24h', value: '24h', minutes: 1440 },
]
const selectQuickRange = (value) => { selectedRange.value = value }
// Resolved range, for the scope strip's free slot.
const rangeLabel = computed(() => `last ${selectedRange.value}`)
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
const groupsStore = useGroupsStore()
const overviewQ  = useApi((config) => resources.getOverview(config), { immediate: false })
const queuesQ    = useApi((config) => queuesApi.list(undefined, config), { immediate: false })
// Handed to the shared store, so the sidebar and Queues reuse this read.
const consumersQ = useApi((config) => consumersApi.list(config), { immediate: false, onSuccess: (d) => groupsStore.publish(d) })
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
    { label: 'Ack',  data: h.map(x => toNum(x.ackPerSecond)), color: semanticColors.ok.line },
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
// Pending Δ row — the backlog itself, as the broker reads it once a minute
// (queue-ops `backlog`: per bucket its last reading of what waits and what is
// in flight). VALUE = how much Pending changed across the window; the chart is
// Pending itself, so it comes back down when consumers catch up.
//
// It is NOT push − ack. A message also leaves by a transaction, a stream
// cycle, retention or the DLQ, and every consumer group acks it once: summed
// over a window that difference drifts for ever — on stage it read +436 over
// an hour while the backlog was 0.
// ---------------------------------------------------------------------------
const backlog = computed(() => {
  const b = opsQ.data.value?.backlog
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
// The readings can begin inside the window (a broker that started reading
// recently): the change is then "since" the first reading, not "over" the range.
const pendingDeltaSpan = computed(() => {
  const b = backlog.value
  const from = Date.parse(opsQ.data.value?.timeRange?.from || '')
  if (!b.length || Number.isNaN(from)) return `over ${selectedRange.value}`
  const first = Date.parse(b[0].bucket)
  const step = (opsQ.data.value?.bucketMinutes || 1) * 60_000
  return first - from > 2 * step
    ? `since ${formatChartLabel(new Date(first), backlogMultiDay.value)}`
    : `over ${selectedRange.value}`
})
// The drift is judged against the work that arrived, not against a constant:
// +1 000 is a rounding error on a window that pushed half a million and a
// stall on one that pushed two thousand. useSeverity owns the shares.
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
  if (v < 0) return `catching up · ${waiting}`
  const sev = pendingDeltaSeverity.value
  return `${sev === 'warn' || sev === 'bad' ? 'falling behind' : 'grew'} · ${waiting}`
})

// ---------------------------------------------------------------------------
// Time lag row.
//
// VALUE  = the tenant overview's lag: the age of each queue's oldest
//          unconsumed message — so it stays correct with consumers
//          stopped. NULL means the broker has no measurement, and
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
// An age is proportional by construction — "five minutes late" means the same
// at 14 msg/s and at 140 000 — so this rule survives the rewrite unchanged; it
// just lives in useSeverity now, with the grids that share it.
const lagNumClass = (s) => numTone(timeLagSeverity(s))
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
  const acks = ackSuccessTotal.value + ackFailedTotal.value
  // The rate is the verdict, so the rate is what the line says. Without it the
  // reader cannot tell whether the tone was earned.
  const rate = acks > 0
    ? `${formatNumber(ackFailedTotal.value)} of ${formatNumber(acks)} acks (${((ackFailedTotal.value / acks) * 100).toFixed(2)}%)`
    : `ack ${formatNumber(ackFailedTotal.value)} in window`
  return `${rate} · ${dlqText}`
})
// THE ROW THIS POLICY WAS WRITTEN FOR. The old rule was `ack > 0 || dlq > 0 →
// amber`, so 90 failed acks in an hour on a cell doing ~14 msg/s — 0.18% of
// ~50 000 acks, i.e. a healthy hour — painted the number and its sparkline
// amber, and so did a DLQ that had held the same three messages since March.
//
// Now: the failures are read as a share of the acks ATTEMPTED in the same
// window (success + failed, both already summed here), and the DLQ DEPTH is
// context only. Depth is monotonic — nothing purges it — so it can never go
// back to neutral and is worthless as a signal; DLQ GROWTH would be one, but
// the tenant overview reports a snapshot and the window series carries no
// dead-letter counter, so this row does not claim to know it.
const ackSuccessTotal = computed(() =>
  history.value.reduce((s, x) => s + (toNum(x.ackSuccess) || 0), 0)
)
const errorSeverity = computed(() => ackFailureSeverity({
  failed: ackFailedTotal.value,
  succeeded: ackSuccessTotal.value,
}))

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

// No tone at all. Low fill means the consumers asked more often than there was
// work — which is what an idle or over-provisioned pool looks like, and an
// over-provisioned pool is a sizing observation, not a degradation. The
// context line above already says which band the number is in, in words. High
// fill only matters alongside rising lag, and lag has its own row.
const fillSeverity = computed(() => '')

// ---------------------------------------------------------------------------
// Retention row.
//
// Every retention step is recorded when it applies (rsm/local_metrics.rs), so
// an empty series IS "nothing deleted in the window".
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
    // No status hue: retention deleting messages is retention working. The
    // series is one of three on the same chart and is told apart by the
    // legend, which is what the grey ramp is for.
    { label: 'Evicted',   data: rows.map(r => toNum(r.evictionMsgs)) },
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
  if (!retentionRows.value.length) return 'none in window'
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
// Kept: an event loop that is 100 ms behind is degradation of the broker
// itself, at any message rate.
const elNumClass = (ms) => eventLoopSeverity(ms)
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
// Counts strip — pending tone (the only number thresholded inline).
//
// A depth has no verdict in it: 50 000 pending drains in four seconds at
// 12k/s and never drains at 0/s. The tone is therefore computed in SECONDS OF
// WORK at the ack rate this window actually measured, and stays neutral when
// there is no drain rate to divide by — a stalled queue is the Time lag row's
// story, told as an age, and telling it twice in two colours is not telling it
// better.
// ---------------------------------------------------------------------------
const drainPerSec = computed(() => latestFinite(history.value.map(x => x.ackPerSecond)))
const pendingNumClass = (n) => backlogSeverity({ pending: n, drainPerSec: drainPerSec.value })

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
      // The bar shows the depth RANK — that is what a bar is for. The verdict
      // is never the rank (the biggest queue would always be red); it comes
      // from useAttention, by lag and by whether anyone reads the queue.
      return {
        ...q,
        _lag: lag,
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
// Last-refresh ticker — drives the live tick in the filter card. One ticker
// for the whole app (useRefreshAgo), so the four auto-polling views neither
// each own a timer nor each spell the string their own way.
// ---------------------------------------------------------------------------
const lastRefreshAt = ref(null)
const refreshAgo = useRefreshAgo(lastRefreshAt)


// ---------------------------------------------------------------------------
// Focus chart — the tiles hand it their series; it replaces the old per-row
// expansion. Same data, same formatters, same tones as the tiles.
// ---------------------------------------------------------------------------
const selectedMetric = ref('throughput')
const selectMetric = (key) => { selectedMetric.value = key }
const focus = computed(() => {
  switch (selectedMetric.value) {
    case 'pendingDelta': return { title: 'Pending', sub: 'waiting and in flight, read by the broker once a minute', series: backlogSeries.value, labels: backlogLabels.value, valueFormat: fmtCount, unit: 'msgs', error: opsError.value }
    case 'timeLag': return { title: 'Time lag', sub: 'pop-sampled per bucket, with gaps where nothing was consumed', series: lagSeriesData.value, labels: chartLabels.value, valueFormat: fmtLagMs, unit: 'ms', error: opsError.value }
    case 'errors': return { title: 'Ack failures', sub: 'failed acks per bucket', sparkline: errorSeriesData.value, labels: chartLabels.value, valueFormat: fmtCount, unit: 'ack failures', tone: errorSeverity.value || 'mute', error: opsError.value }
    case 'parked': return { title: 'Parked', sub: 'long-polls waiting for work, summed across queues', series: parkedSeriesData.value, labels: chartLabels.value, valueFormat: fmtCount, unit: 'long-polls', error: opsError.value }
    case 'fillRatio': return { title: 'Fill ratio', sub: 'long-polls that came back with a message', series: fillSeriesData.value, labels: chartLabels.value, valueFormat: fmtFillPct, unit: '%', error: opsError.value }
    case 'partitions': return { title: 'Partitions', sub: 'created and deleted per bucket', series: partitionSeriesData.value, labels: chartLabels.value, valueFormat: fmtCount, unit: 'count', error: opsError.value }
    case 'retention': return { title: 'Retention', sub: 'messages removed by retention and eviction', series: retentionSeriesData.value, labels: retentionLabels.value, valueFormat: fmtCount, unit: 'msgs', error: retentionError.value }
    case 'eventLoop': return { title: 'Event loop', sub: 'scheduling delay, every tenant on this cell', series: elSeriesData.value, labels: statusChartLabels.value, valueFormat: (v) => v + ' ms', unit: 'ms', error: statusError.value }
    case 'cpu': return { title: 'Queen CPU', sub: 'the whole cell', series: cpuSeriesData.value, labels: cpuLabels.value, valueFormat: (v) => v.toFixed(1) + '%', unit: '%', error: cpuError.value }
    default: return { title: 'Throughput', sub: 'push, pop and ack per second, summed across queues', series: throughputSeries.value, labels: chartLabels.value, valueFormat: fmtRate, unit: 'msgs / sec', error: opsError.value }
  }
})

// ---------------------------------------------------------------------------
// Status — what needs you, worked out from data already on this page and
// printed with its rules under the list. It never claims "healthy" when it
// could not read the queues or the groups: that is "status unknown".
//
//   bad   a consumer group on the queue is ≥ 5 min behind
//   warn  a consumer group on the queue is ≥ 1 min behind
//   warn  the queue holds messages and no live consumer group reads it
//   warn/bad  the tenant's ack failures, by ackFailureSeverity (useSeverity)
//
// The first three are composables/useAttention — the sidebar, Queues and
// Consumer groups read the same function, so none of them can disagree.
// ---------------------------------------------------------------------------
const queuePath = (name) => `/queues/${encodeURIComponent(name)}`
const issues = computed(() => {
  if (queuesFailed.value || consumersFailed.value) return []
  const out = []
  // enrichedQueues is sorted by depth; the rule keeps that order.
  for (const a of queueAttention(enrichedQueues.value, consumers.value)) {
    const why = a.reason === 'lag'
      ? `A consumer group is ${fmtLagSeconds(a.lag)} behind · ${formatNumber(a.pending)} pending`
      : `${a.deadOnly ? 'Its consumer group has never read' : 'No consumer group reads it'} · ${formatNumber(a.pending)} waiting`
    out.push({ key: `q:${a.name}`, sev: a.sev, name: a.name, why, to: queuePath(a.name) })
  }
  const es = errorSeverity.value
  if (es === 'warn' || es === 'bad') out.push({ key: 'acks', sev: es, name: 'Ack failures', why: errorContext.value, to: '/dlq', tenant: true })
  const rank = { bad: 2, warn: 1 }
  return out.sort((a, b) => rank[b.sev] - rank[a.sev])
})
const issueSevByQueue = computed(() => new Map(issues.value.filter(i => !i.tenant).map(i => [i.name, i.sev])))
const queueGlyph = (q) => issueSevByQueue.value.get(q.name) || 'ok'
const groupGlyph = (g) => {
  const s = groupAttention(g)
  return s === 'bad' || s === 'warn' ? s : s === 'mute' ? 'idle' : 'ok'
}

const statusKnown = computed(() =>
  !loadingQueues.value && !loadingConsumers.value && !queuesFailed.value && !consumersFailed.value
)
const statusSev = computed(() => {
  if (!statusKnown.value) return 'unknown'
  if (issues.value.some(i => i.sev === 'bad')) return 'bad'
  return issues.value.length ? 'warn' : 'ok'
})
const statusGlyph = computed(() => statusSev.value === 'unknown' ? 'idle' : statusSev.value)
const statusWord = computed(() => {
  const w = { ok: 'Healthy', warn: 'Degraded', bad: 'Failing' }[statusSev.value]
  if (w) return w
  return loadingQueues.value || loadingConsumers.value ? 'Checking' : 'Unknown'
})
const headline = computed(() => {
  if (loadingQueues.value || loadingConsumers.value) return 'Checking your queues…'
  if (!statusKnown.value) return 'Status unknown'
  const qIssues = issues.value.filter(i => !i.tenant)
  const bad = qIssues.filter(i => i.sev === 'bad')
  if (bad.length === 1) return `${bad[0].name} is falling behind`
  if (bad.length > 1) return `${bad.length} queues are falling behind`
  if (qIssues.length) return `${qIssues.length} ${qIssues.length === 1 ? 'queue needs' : 'queues need'} attention`
  if (issues.value.length) return 'Ack failures need attention'
  return 'All queues are healthy'
})
const issuesCountText = computed(() => {
  if (!statusKnown.value) return '—'
  if (!issues.value.length) return 'none'
  const q = issues.value.filter(i => !i.tenant).length
  return `${q} of ${formatNumber(queues.value.length)} queues${issues.value.length > q ? ' · acks' : ''}`
})

// The sentence under the headline: only numbers the page already has.
const pushNow = computed(() => latestFinite(history.value.map(x => x.pushPerSecond)))
const popNow = computed(() => latestFinite(history.value.map(x => x.popPerSecond)))
const pendingNow = computed(() => (overviewFailed.value ? null : toNum(overview.value?.messages?.pending)))
const ackAttempts = computed(() => ackSuccessTotal.value + ackFailedTotal.value)
const ackFailPct = computed(() =>
  ackAttempts.value > 0 ? `${((ackFailedTotal.value / ackAttempts.value) * 100).toFixed(2)}%` : '—'
)
const fmtMsgRate = (n) => fmtRate(n).replace(' /s', ' msg/s')

// The sunflower: one entry per queue; the component decides how many seeds.
const sunflowerQueues = computed(() =>
  enrichedQueues.value.map(q => ({
    name: q.name,
    partitions: q.partitions || 1,
    pending: q._pending ?? 0,
    sev: queueGlyph(q),
  }))
)

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

onMounted(fetchAll)
</script>

<style scoped>
/* ---------------------------------------------------------------------------
   Page layout lives in style.css (OVERVIEW); what is here is the two ranked
   lists at the bottom and the value fragments the tiles' slots render.
   --------------------------------------------------------------------------- */

:deep(.mr-sep) {
  color: var(--text-faint);
  margin: 0 4px;
  font-weight: 400;
}
:deep(.mr-unit) {
  font-style: normal;
  color: var(--text-low);
  margin-left: 4px;
  font-size: 12px;
  font-weight: 400;
}

/* Ranked lists: rows on hairlines inside one card — no box per row. */
.entity-list {
  display: flex;
  flex-direction: column;
  padding: 4px 0;
}
.entity-row {
  display: block;
  width: 100%;
  text-align: left;
  border: 0;
  border-bottom: 1px solid var(--bd-soft);
  background: transparent;
  padding: 10px 16px;
  cursor: pointer;
  color: inherit;
  transition: background .12s var(--ease);
}
.entity-row:last-child { border-bottom: 0; }
.entity-row:hover { background: var(--ink-3); }

.entity-head {
  display: flex;
  align-items: center;
  gap: 10px;
}
.entity-name {
  flex: 1;
  font-size: 13px;
  font-weight: 500;
  color: var(--text-hi);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.entity-right {
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
  margin-top: 5px;
  padding-left: 18px;  /* under the name, past the glyph */
}
.entity-meta .bar-meta {
  flex: 0 1 140px;
  width: 140px;
  height: 4px;
  background: var(--ink-4);
}
.entity-meta .bar-meta i { background: var(--text-faint); }
.entity-meta .bar-meta i.warn { background: var(--warn-400); }
.entity-meta .bar-meta i.bad { background: var(--ember-400); }
.meta-text {
  font-size: 12px;
  color: var(--text-low);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-variant-numeric: tabular-nums;
}
.meta-text strong {
  color: var(--text-mid);
  font-weight: 600;
}
.meta-sep { color: var(--text-faint); margin: 0 4px; }
.meta-tag {
  display: inline-block;
  padding: 0 6px;
  border-radius: var(--r-chip);
  border: 1px solid var(--bd);
  color: var(--text-mid);
  font-size: 11px;
  font-weight: 500;
  margin-right: 4px;
}
.meta-tag-cfl { color: var(--text-hi); }

.card-foot {
  border-top: 1px solid var(--bd);
  padding: 8px 16px;
  display: flex;
  justify-content: flex-end;
}
.card-foot-link {
  font-size: 12px;
  color: var(--text-mid);
  cursor: pointer;
  transition: color .12s var(--ease);
}
.card-foot-link:hover { color: var(--text-hi); }
</style>
