<template>
  <SubSidebar label="Trace names" :current="currentLabel">
  <!-- The tenant's trace names and the search box; a click loads one here. -->
  <template #side>
    <TracesSide
      :rows="traceNames.rows"
      :total="traceNames.total"
      :loaded="traceNames.loaded"
      :failed="traceNames.failed"
      :current="currentTraceName"
      :message="messageTarget"
    />
  </template>
  <div class="view-container">

    <!-- This page does not poll, so it carries no live tick. -->
    <PageHead :title="headTitle">
      <template #title><span :title="headFull || undefined">{{ headTitle }}</span></template>
      <template v-if="facts.length" #sub>
        <span class="tr-facts">
          <span v-for="f in facts" :key="f.k" class="tr-fact" :title="f.title || undefined"><span class="tr-fact-k">{{ f.k }}</span>{{ f.v }}</span>
        </span>
      </template>
      <template v-if="hasTraceTarget" #actions>
        <span v-if="traceData" class="tr-stamp">{{ stamp(tracePanel) }}</span>
        <button type="button" class="btn btn-ghost" @click="clearSearch">Clear</button>
      </template>
    </PageHead>

    <!-- A refresh that failed over rows still on screen says how old they
         are; with nothing on screen the table itself says it. -->
    <div v-if="error && traces.length" class="status-banner banner-bad view-banner tr-banner" role="status">
      <span :title="formatTimestampUtc(tracePanel.lastUpdated.value)">
        <strong>Could not load traces</strong> · {{ error }} · showing the last rows that loaded{{ lastGoodText ? ` (${lastGoodText})` : '' }}
      </span>
    </div>

    <!-- The events of the name (or message) picked -->
    <div v-if="hasTraceTarget" class="card tr-card">
      <div class="tr-table">
        <table class="t">
          <thead>
            <tr>
              <th class="tr-mark-col"><span class="sr-only">Mark</span></th>
              <th>Event</th>
              <th>Time</th>
              <th>Queue</th>
              <th>Partition</th>
              <th>Transaction</th>
              <th>Trace names</th>
              <th>Data</th>
              <th>Worker / group</th>
            </tr>
          </thead>
          <tbody>
            <!-- First paint only: a refresh or a page turn leaves the rows that
                 already loaded on screen. -->
            <template v-if="firstLoad">
              <tr v-for="i in 8" :key="i" class="tr-skeleton">
                <td class="tr-mark-col" />
                <td v-for="(w, j) in [72, 128, 96, 80, 88, 104, 120, 96]" :key="j"><div class="skeleton" :style="{ height: '14px', width: `${w}px` }" /></td>
              </tr>
            </template>

            <!-- A page we could not load is not a page with nothing on it. -->
            <tr v-else-if="error && traces.length === 0" class="tr-state-row">
              <td colspan="9">
                <div class="tr-state" :title="error">
                  <Icon name="circle-x" :size="20" />
                  <span>Could not read the events of <span class="tr-mono">{{ messageTarget?.transactionId || currentTraceName }}</span></span>
                </div>
              </td>
            </tr>

            <tr v-else-if="traces.length === 0" class="tr-state-row">
              <td colspan="9">
                <div class="tr-state">
                  <Icon name="route" :size="20" />
                  <span>{{ messageTarget ? 'No event was recorded for this message' : `No event carries ${currentTraceName}` }}</span>
                </div>
              </td>
            </tr>

            <template v-else>
              <tr
                v-for="trace in traces"
                :key="trace.id"
                class="tr-row"
                tabindex="0"
                @click="viewTrace(trace)"
                @keydown.enter="viewTrace(trace)"
              >
                <td class="tr-mark-col"><span v-if="eventMark(trace.event_type)" class="g" :class="eventMark(trace.event_type)" aria-hidden="true" /></td>
                <td class="tr-event">{{ trace.event_type || '—' }}</td>
                <td class="tr-time" :title="formatTimestampUtc(trace.created_at)">{{ formatTimestamp(trace.created_at) }}</td>
                <td>
                  <span v-if="trace.queue_name" class="tr-queue">{{ trace.queue_name }}</span>
                  <span v-else class="tr-low" title="This trace's partition does not resolve to a queue on this broker, so it cannot name its queue">not recorded</span>
                </td>
                <td class="tr-mid">{{ trace.partition_name || (trace.queue_name ? '—' : 'not recorded') }}</td>
                <td class="tr-mono tr-mid" :title="trace.transaction_id">{{ trace.transaction_id ? `${trace.transaction_id.slice(0, 8)}…` : '—' }}</td>
                <td>
                  <div v-if="trace.trace_names?.length" class="tr-tags">
                    <span v-for="name in trace.trace_names" :key="name" class="tr-tag" :class="{ on: name === currentTraceName }">{{ name }}</span>
                  </div>
                  <span v-else class="tr-low">—</span>
                </td>
                <td class="tr-data">
                  <span v-if="trace.data?.text" :title="trace.data.text">{{ trace.data.text }}</span>
                  <span v-else-if="hasAdditionalData(trace.data)" class="tr-low">JSON</span>
                  <span v-else class="tr-low">—</span>
                </td>
                <td class="tr-worker">
                  <span v-if="trace.worker_id" class="tr-ellipsis" :title="trace.worker_id">{{ trace.worker_id }}</span>
                  <span v-if="hasGroup(trace)" class="tr-low tr-ellipsis">{{ trace.consumer_group }}</span>
                  <span v-if="!trace.worker_id && !hasGroup(trace)" class="tr-low">—</span>
                </td>
              </tr>
            </template>
          </tbody>
        </table>
      </div>

      <!-- The range counts the rows actually drawn. A broker that answered
           every event in one response pages nothing: no pager then. -->
      <div v-if="traces.length && (showPager || !serverPaginates)" class="tr-pager">
        <span v-if="showPager" class="tr-pager-count">
          {{ formatNumber(shownFrom) }}–{{ formatNumber(shownTo) }} of {{ formatNumber(totalTraces) }}
        </span>
        <span v-else class="tr-pager-count" title="This broker returned every event in one response, so there is nothing to page">
          All {{ formatNumber(traces.length) }} events
        </span>
        <div v-if="showPager" class="tr-pager-nav">
          <button type="button" class="btn btn-ghost" :disabled="offset === 0" @click="previousPage">Previous</button>
          <button type="button" class="btn btn-ghost" :disabled="shownTo >= totalTraces" @click="nextPage">Next</button>
        </div>
      </div>
    </div>

    <!-- Nothing picked yet -->
    <div v-else class="card">
      <div class="tr-state tr-state-page">
        <Icon name="route" :size="20" />
        <RouterLink v-if="contextQueue" :to="queueLocation(contextQueue, route, 'messages')">Open a message of {{ contextQueue }} to see its events</RouterLink>
        <!-- With no names to pick (none yet, or the read failed), the search
             box is the way in: it opens a typed name. -->
        <span v-else-if="traceNames.loaded && !traceNames.rows.length">{{ traceNames.failed ? 'Type a trace name in the search, then Enter' : 'No trace recorded yet' }}</span>
        <span v-else>Pick a trace name from the list</span>
      </div>
    </div>

    <DetailDrawer
      :open="Boolean(selectedTrace)"
      :title="selectedTrace ? `${selectedTrace.event_type || 'Trace'} event` : 'Trace event'"
      @close="selectedTrace = null"
    >
      <template #leading>
        <span v-if="selectedTrace && eventMark(selectedTrace.event_type)" class="g" :class="eventMark(selectedTrace.event_type)" aria-hidden="true" />
      </template>

      <template v-if="selectedTrace">
        <div class="tr-detail">
          <dl class="tr-fields">
            <div>
              <dt>Time</dt>
              <dd :title="formatTimestampUtc(selectedTrace.created_at)">{{ formatTimestamp(selectedTrace.created_at) }}</dd>
            </div>
            <div>
              <dt>Queue / partition</dt>
              <dd>{{ selectedTrace.queue_name || '—' }} / {{ selectedTrace.partition_name || '—' }}</dd>
            </div>
            <div>
              <dt>Transaction</dt>
              <dd class="tr-mono tr-break">{{ selectedTrace.transaction_id || '—' }}</dd>
            </div>
            <div v-if="selectedTrace.worker_id">
              <dt>Worker</dt>
              <dd class="tr-mono tr-break">{{ selectedTrace.worker_id }}</dd>
            </div>
            <div v-if="hasGroup(selectedTrace)">
              <dt>Consumer group</dt>
              <dd>{{ selectedTrace.consumer_group }}</dd>
            </div>
          </dl>

          <div v-if="selectedTrace.trace_names?.length">
            <div class="tr-label">Trace names</div>
            <div class="tr-tags">
              <span v-for="name in selectedTrace.trace_names" :key="name" class="tr-tag" :class="{ on: name === currentTraceName }">{{ name }}</span>
            </div>
          </div>

          <div v-if="selectedTrace.data">
            <div class="tr-label">Data</div>
            <p v-if="selectedTrace.data.text" class="trace-text">{{ selectedTrace.data.text }}</p>
            <div v-if="hasAdditionalData(selectedTrace.data)" class="trace-code">
              <pre class="font-mono">{{ formatTraceData(selectedTrace.data) }}</pre>
            </div>
          </div>
        </div>
      </template>

      <template #footer>
        <router-link
          v-if="selectedTrace?.partition_id && selectedTrace?.transaction_id"
          :to="{ path: '/messages', query: { ...contextQuery(route), queue: selectedTrace.queue_name || route.query.queue, partitionId: selectedTrace.partition_id, transactionId: selectedTrace.transaction_id } }"
          class="btn"
          @click="selectedTrace = null"
        >
          Open message
        </router-link>
      </template>
    </DetailDrawer>
  </div>
  </SubSidebar>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { traces as tracesApi, describeApiError } from '@/api'
import DetailDrawer from '@/components/DetailDrawer.vue'
import Icon from '@/components/Icon.vue'
import PageHead from '@/components/PageHead.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import TracesSide from '@/components/TracesSide.vue'
import { contextQuery, queueLocation, readRouteValue, textParam } from '@/composables/navigation'
import { TRACE_NAMES_LIMIT, readTraceNames } from '@/composables/tracesSide'
import { formatNumber, useApi } from '@/composables/useApi'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import { onClusterChange } from '@/stores/identity'

const route = useRoute(), router = useRouter()

// Search state: the name or the message open, from the address.
const currentTraceName = ref('')
const selectedTrace = ref(null)
const offset = ref(0)
const limit = ref(25)
const totalTraces = ref(0)
const messageTarget = computed(() => {
  const partitionId = textParam(route.query.partitionId), transactionId = textParam(route.query.transactionId)
  return partitionId && transactionId ? { partitionId, transactionId } : null
})
const hasTraceTarget = computed(() => Boolean(messageTarget.value || currentTraceName.value))
const contextQueue = computed(() => textParam(route.query.queue))
// Our own verdict on an unusable page, kept apart from the transport error.
const pageError = ref(null)

// useApi owns loading/error, aborts on unmount, and discards a response that
// belongs to a cluster we have since left. The panel object is kept whole so
// the header can stamp its freshness from it.
const tracePanel = useApi(
  (name, params, config) => messageTarget.value
    ? tracesApi.getForMessage(messageTarget.value.partitionId, messageTarget.value.transactionId, params, config)
    : tracesApi.getByName(name, params, config),
  { immediate: false },
)
const {
  data: traceData,
  loading,
  error: apiError,
  execute: executeTraces,
} = tracePanel

const traces = computed(() =>
  Array.isArray(traceData.value?.traces) ? traceData.value.traces : []
)
const error = computed(() =>
  pageError.value || (apiError.value ? describeApiError(apiError.value) : null)
)
/** First paint only — a refresh must not yank loaded rows off the screen. */
const firstLoad = computed(() => loading.value && !traceData.value)
/** When a refresh fails over rows that are still on screen, say how old they are. */
const lastGoodText = computed(() =>
  tracePanel.lastUpdated.value ? formatTimestamp(tracePanel.lastUpdated.value) : ''
)
// Flipped off when a page comes back larger than it was asked for: that broker
// is not applying the limit, so a page range would be fiction.
const serverPaginates = ref(true)

// The list's names: the tenant's real trace names or nothing at all —
// invented examples send every click into "No event carries…". useApi drops
// an answer for a cluster we have since left; a failed read keeps the names
// that loaded before it, and the list says so in one quiet line.
const namesPanel = useApi(
  (config) => tracesApi.getAvailableNames({ limit: TRACE_NAMES_LIMIT }, config),
  { immediate: false },
)
const traceNames = computed(() => {
  const read = readTraceNames(namesPanel.data.value)
  return {
    rows: read?.rows || [],
    total: read?.total || 0,
    loaded: namesPanel.data.value !== null || namesPanel.failed.value,
    // A failed read, or an answer that holds no list: not "no names".
    failed: namesPanel.failed.value || (namesPanel.data.value !== null && read === null),
  }
})

// The header line: the pick, and what its events add up to.
const shortId = (id) => (id && id.length > 12 ? `${id.slice(0, 8)}…` : id)
const headTitle = computed(() => (messageTarget.value ? `Message ${shortId(messageTarget.value.transactionId)}` : currentTraceName.value || 'Traces'))
const headFull = computed(() => (messageTarget.value ? messageTarget.value.transactionId : currentTraceName.value))
const currentLabel = computed(() => (messageTarget.value ? `Message ${shortId(messageTarget.value.transactionId)}` : currentTraceName.value))
const uniqueMessages = computed(() => new Set(traces.value.map(t => t.transaction_id)).size)
const uniqueQueues = computed(() => new Set(traces.value.map(t => t.queue_name).filter(Boolean)).size)
/** A trace whose partition does not resolve to a queue carries no queue name. */
const queueAttributionAvailable = computed(() => traces.value.some(t => t.queue_name))
const facts = computed(() => {
  if (!hasTraceTarget.value || !traces.value.length) return []
  const paged = showPager.value
  return [
    { k: 'Events', v: formatNumber(totalTraces.value || traces.value.length) },
    messageTarget.value ? null : { k: 'Messages', v: formatNumber(uniqueMessages.value), title: paged ? 'On this page of events' : undefined },
    // 0 queues would read as "these events touched no queue". They touched
    // queues the broker could not name for these rows.
    {
      k: 'Queues',
      v: queueAttributionAvailable.value ? formatNumber(uniqueQueues.value) : '—',
      title: queueAttributionAvailable.value ? (paged ? 'On this page of events' : undefined) : 'Not recorded for these events',
    },
  ].filter(Boolean)
})

const shownFrom = computed(() => (traces.value.length ? offset.value + 1 : 0))
const shownTo = computed(() => offset.value + traces.value.length)
const showPager = computed(
  () => serverPaginates.value && (offset.value > 0 || totalTraces.value > traces.value.length)
)

async function fetchTraces() {
  pageError.value = null
  let payload
  try {
    payload = await executeTraces(currentTraceName.value, {
      limit: limit.value,
      offset: offset.value
    })
  } catch {
    // apiError carries it; `error` renders the canonical sentence.
    return
  }

  const rows = payload?.traces
  // A missing array is NOT an empty result: it is a page the broker could not
  // build. Saying "no event" for it denies events that exist.
  if (!Array.isArray(rows)) {
    pageError.value = `The broker returned no result page at offset ${offset.value}.`
    return
  }
  serverPaginates.value = rows.length <= limit.value
  totalTraces.value = Number(payload?.total) || rows.length
}

async function loadPage() {
  if (hasTraceTarget.value) await fetchTraces()
}

// The failure is reported by the shared HTTP client; the list says it in one
// quiet line and the search box still opens a typed name.
const loadTraceNames = () => namesPanel.refresh()
// Another cluster's names are not this one's: useApi forgets them, ask again.
onUnmounted(onClusterChange(loadTraceNames))

function previousPage() {
  if (offset.value > 0) router.replace({ query: { ...route.query, offset: Math.max(0, offset.value - limit.value) || undefined } })
}
function nextPage() {
  if (shownTo.value < totalTraces.value) router.replace({ query: { ...route.query, offset: offset.value + traces.value.length } })
}
function clearSearch() {
  router.replace({ query: { ...contextQuery(route) } })
}

watch(() => [route.query.trace, route.query.partitionId, route.query.transactionId, route.query.offset], () => {
  currentTraceName.value = textParam(route.query.trace)
  tracePanel.abort()
  loading.value = false; apiError.value = null
  offset.value = readRouteValue(route.query.offset, 0)
  selectedTrace.value = null; traceData.value = null; totalTraces.value = 0; pageError.value = null
  if (hasTraceTarget.value) fetchTraces()
}, { immediate: true })

function viewTrace(trace) {
  selectedTrace.value = trace
}

// An event's mark: the app's glyphs, and only where the event says something
// went wrong. Every other event is unmarked.
function eventMark(eventType) {
  const type = String(eventType || '').toLowerCase()
  return type === 'error' ? 'bad' : type === 'warning' || type === 'warn' ? 'warn' : null
}

const hasGroup = (trace) => Boolean(trace?.consumer_group && trace.consumer_group !== '__QUEUE_MODE__')

function hasAdditionalData(data) {
  if (!data || typeof data !== 'object') return false
  const keys = Object.keys(data).filter(k => k !== 'text')
  return keys.length > 0
}

function formatTraceData(data) {
  if (!data || typeof data !== 'object') return ''
  const { text, ...rest } = data
  return JSON.stringify(rest, null, 2)
}

// Refresh: the names in the list, and the events only when one is open.
const refreshCurrentView = async () => {
  await Promise.all([loadTraceNames(), hasTraceTarget.value ? loadPage() : null])
}

// Not useAutoRefresh: this page does not poll, so it shows no live tick. Its
// freshness fact is the stamp in the header.
useRefresh(refreshCurrentView)

onMounted(loadTraceNames)
</script>

<style scoped>
/* The header line's facts: the Queues pilot's chips (QueuesLayout). */
/* The facts drop under the title when the line is full, as the pilot's do. */
.page-head :deep(.page-id) { flex-wrap: wrap; row-gap: 6px; }
.page-head :deep(.page-sub) { overflow: visible; white-space: normal; }
.tr-facts { display: inline-flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.tr-fact {
  display: inline-flex; align-items: baseline; gap: 5px;
  padding: 2px 8px; border-radius: var(--r-chip);
  background: var(--ink-3); border: 1px solid var(--bd);
  font-size: 11.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap;
}
.tr-fact-k { color: var(--text-low); }
.tr-stamp { font-size: 12px; color: var(--text-low); font-variant-numeric: tabular-nums; }
.tr-banner { margin-bottom: 16px; }

/* The events: one table, a quiet header row, rows with room. */
.tr-card { overflow: hidden; }
.tr-table { overflow-x: auto; }
.tr-table .t { width: 100%; min-width: 880px; }
.tr-table .t thead th { background: var(--ink-3); }
.tr-mark-col { width: 28px; padding-right: 0 !important; text-align: center; }
.tr-row { cursor: pointer; }
.tr-row:focus-visible { outline: 2px solid var(--ring); outline-offset: -2px; }
.tr-event { color: var(--text-hi); font-weight: 500; white-space: nowrap; }
.tr-time { color: var(--text-mid); white-space: nowrap; font-variant-numeric: tabular-nums; }
.tr-queue { color: var(--text-hi); font-weight: 500; }
.tr-mid { color: var(--text-mid); }
.tr-low { color: var(--text-low); }
.tr-mono { font-family: var(--font-mono); font-size: 11.5px; }
.tr-break { word-break: break-all; }
.tr-data { max-width: 220px; color: var(--text-mid); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.tr-worker { max-width: 160px; }
.tr-worker > span { display: block; }
.tr-worker > span + span { margin-top: 2px; font-size: 12px; }
.tr-ellipsis { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.tr-tags { display: flex; flex-wrap: wrap; gap: 4px; }
.tr-tag {
  display: inline-flex; align-items: center; padding: 1px 7px; border-radius: var(--r-chip);
  border: 1px solid var(--bd); font-size: 11.5px; color: var(--text-mid); white-space: nowrap;
}
.tr-tag.on { color: var(--text-hi); background: var(--selected); border-color: var(--bd-hi); }

/* An empty or failed table, and the page with nothing picked: one icon and
   one short line. */
.tr-state-row:hover td { background: transparent !important; }
.tr-state {
  display: flex; flex-direction: column; align-items: center; gap: 10px;
  padding: 40px 16px; text-align: center; font-size: 13px; color: var(--text-mid);
}
.tr-state > .icon { color: var(--text-low); }
.tr-state a { color: var(--text-hi); text-decoration: underline; text-underline-offset: 2px; }
.tr-state-page { padding: 56px 16px; }
/* Inside a table wider than a phone, the line stays in view. */
td > .tr-state { position: sticky; left: 0; max-width: min(100%, calc(100vw - 48px)); }

.tr-pager {
  display: flex; align-items: center; justify-content: space-between; gap: 8px 16px; flex-wrap: wrap;
  padding: 10px 14px; border-top: 1px solid var(--bd); font-size: 12px; color: var(--text-low);
}
.tr-pager-count { font-variant-numeric: tabular-nums; }
.tr-pager-nav { display: flex; gap: 4px; }

/* The drawer. */
.tr-detail { display: flex; flex-direction: column; gap: 20px; }
.tr-fields { display: flex; flex-direction: column; gap: 14px; margin: 0; }
.tr-fields dt, .tr-label { margin-bottom: 4px; font-size: 12px; color: var(--text-low); }
.tr-fields dd { margin: 0; font-size: 13px; color: var(--text-hi); }
/* Free text stays one step ABOVE the drawer, the JSON block below it stays
   recessed — the pair has to keep reading apart. */
.trace-text {
  margin: 0 0 12px; font-size: 13px; color: var(--text-hi); white-space: pre-wrap; overflow-wrap: anywhere;
  border: 1px solid var(--bd); border-radius: var(--r-card); padding: 12px;
  background: var(--ink-3);
}
.trace-code {
  border: 1px solid var(--bd); border-radius: var(--r-card);
  padding: 14px; overflow-x: auto;
  background: var(--recessed);
}
.trace-code pre { margin: 0; font-size: 12px; white-space: pre-wrap; color: var(--text-mid); }
</style>
