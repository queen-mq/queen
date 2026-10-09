<template>
  <div class="view-container">

    <!-- This page does not poll, so it carries no live tick. -->
    <PageHead title="Traces" :sub="messageTarget ? `message ${messageTarget.transactionId}` : currentTraceName ? `events named ${currentTraceName}` : ''" />

    <!-- Page banners: the failure first, then the broker-capability caveat. -->
    <div v-if="error" class="status-banner banner-bad view-banner">
      <span :title="formatTimestampUtc(tracePanel.lastUpdated.value)">
        <strong>Could not load traces</strong> · {{ error }}<template v-if="traces.length"> · showing the last rows that loaded{{ lastGoodText ? ` (${lastGoodText})` : '' }}</template>
      </span>
    </div>
    <!-- A broker capability, like the same note on the dead-letter page:
         information, not a condition — so it is not amber. -->
    <div
      v-if="hasTraceTarget && traces.length > 0 && !serverPaginates"
      class="status-banner banner-info view-banner"
    >
      <span>This broker returned every trace in one response — the page controls would page nothing, so they are hidden.</span>
    </div>

    <PageTools>
      <div class="filter-search">
        <svg class="filter-search-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
        </svg>
        <input v-model="searchTraceName" type="text" placeholder="A trace name" class="input" @keyup.enter="searchTraces" />
      </div>
      <button class="btn btn-primary" :disabled="!searchTraceName || loading" @click="searchTraces">Search</button>
      <button v-if="hasTraceTarget" class="btn btn-ghost" @click="clearSearch">Clear</button>
      <!-- Quick examples: real trace names for this tenant, or nothing. -->
      <template v-if="!hasTraceTarget && exampleTraceNames.length > 0">
        <span class="tool-label">Recent</span>
        <div class="pill-row">
          <button
            v-for="example in exampleTraceNames"
            :key="example"
            class="pill"
            @click="searchTraceName = example; searchTraces()"
          >{{ example }}</button>
        </div>
      </template>
    </PageTools>

    <p v-if="route.query.queue && !hasTraceTarget" class="tool-note">
      Open a message to inspect its traces.
      <RouterLink :to="queueLocation(route.query.queue, route, 'messages')">Messages of {{ route.query.queue }} →</RouterLink>
    </p>

    <!-- Summary -->
    <div v-if="hasTraceTarget && traces.length > 0" class="card" style="margin-bottom:16px;">
      <div class="card-header">
        <h3>Trace summary</h3>
        <span class="card-sub">{{ messageTarget ? `events for message ${messageTarget.transactionId}` : `events carrying the trace name ${currentTraceName}` }}</span>
        <span class="muted">{{ stamp(tracePanel) }}</span>
      </div>
      <div class="card-body">
        <div class="stat-grid stat-grid-3">
          <div class="stat">
            <div class="stat-label">Traces found</div>
            <div class="stat-value">{{ totalTraces }}</div>
          </div>
          <div class="stat">
            <div class="stat-label">Unique messages</div>
            <div class="stat-value">{{ uniqueMessages }}</div>
            <div class="stat-foot">on this page</div>
          </div>
          <div class="stat">
            <div class="stat-label">Queues</div>
            <!-- 0 queues would read as "these traces touched no queue". They
                 touched queues the broker could not name for these rows. -->
            <div class="stat-value">{{ queueAttributionAvailable ? uniqueQueues : '—' }}</div>
            <div class="stat-foot">
              {{ queueAttributionAvailable ? 'on this page' : 'queue not recorded for these traces' }}
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Trace events -->
    <div v-if="hasTraceTarget" class="card">
      <div class="card-header">
        <h3>Trace events</h3>
        <span class="muted">{{ stamp(tracePanel) }}</span>
      </div>
      <div style="overflow-x:auto;">
        <table class="t">
          <thead>
            <tr>
              <th>Event</th>
              <th>Time</th>
              <th>Queue</th>
              <th>Partition</th>
              <th>Transaction ID</th>
              <th>Trace Names</th>
              <th>Data</th>
              <th>Worker / Group</th>
            </tr>
          </thead>
          <tbody>
            <!-- First paint only: a refresh or a page turn leaves the rows that
                 already loaded on screen. -->
            <template v-if="firstLoad">
              <tr v-for="i in 8" :key="i">
                <td><div class="skeleton" style="height:16px; width:72px;" /></td>
                <td><div class="skeleton" style="height:16px; width:128px;" /></td>
                <td><div class="skeleton" style="height:16px; width:96px;" /></td>
                <td><div class="skeleton" style="height:16px; width:80px;" /></td>
                <td><div class="skeleton" style="height:16px; width:88px;" /></td>
                <td><div class="skeleton" style="height:16px; width:104px;" /></td>
                <td><div class="skeleton" style="height:16px; width:120px;" /></td>
                <td><div class="skeleton" style="height:16px; width:96px;" /></td>
              </tr>
            </template>

            <!-- A page we could not load is not a page with nothing on it. Rows
                 that did load stay on screen; the banner above says they are
                 the last good ones. -->
            <tr v-else-if="error && traces.length === 0">
              <td colspan="8">
                <div class="empty-state empty-state-failed">
                  <h3>Nothing loaded</h3>
                  <p>Could not read events for <span class="font-mono">{{ messageTarget?.transactionId || currentTraceName }}</span>.</p>
                </div>
              </td>
            </tr>

            <tr v-else-if="traces.length === 0">
              <td colspan="8">
                <div class="empty-state">
                  <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <h3>No traces found</h3>
                  <p>{{ messageTarget ? 'No events were recorded for this message.' : `No events carry the trace name ${currentTraceName}.` }}</p>
                </div>
              </td>
            </tr>

            <template v-else>
              <tr
                v-for="trace in traces"
                :key="trace.id"
                @click="viewTrace(trace)"
                style="cursor:pointer;"
              >
                <!-- Event Type -->
                <td>
                  <div style="display:flex; align-items:center; gap:8px;">
                    <span
                      style="width:8px; height:8px; border-radius:var(--r-pill); flex-shrink:0;"
                      :style="{ background: eventColor(trace.event_type) }"
                    />
                    <span style="font-size:11px; font-weight:600; letter-spacing:.08em; text-transform:uppercase; color:var(--text-hi);">
                      {{ trace.event_type }}
                    </span>
                  </div>
                </td>

                <!-- Time -->
                <td>
                  <span :title="formatTimestampUtc(trace.created_at)" class="tabular-nums" style="font-size:12px; color:var(--text-mid); white-space:nowrap;">
                    {{ formatTimestamp(trace.created_at) }}
                  </span>
                </td>

                <!-- Queue -->
                <td>
                  <span v-if="trace.queue_name" style="font-size:13px; font-weight:500; color:var(--text-hi);">
                    {{ trace.queue_name }}
                  </span>
                  <span
                    v-else
                    class="chip chip-mute"
                    title="This trace's partition does not resolve to a queue on this broker, so it cannot name its queue."
                  >
                    not recorded
                  </span>
                </td>

                <!-- Partition -->
                <td>
                  <span style="font-size:13px; color:var(--text-mid);">
                    {{ trace.partition_name || (trace.queue_name ? '-' : 'not recorded') }}
                  </span>
                </td>

                <!-- Transaction ID -->
                <td>
                  <span class="font-mono" style="font-size:12px; color:var(--text-mid);">
                    {{ trace.transaction_id?.slice(0, 8) }}…
                  </span>
                </td>

                <!-- Trace Names -->
                <td>
                  <div v-if="trace.trace_names?.length > 0" style="display:flex; flex-wrap:wrap; gap:4px;">
                    <span
                      v-for="name in trace.trace_names"
                      :key="name"
                      class="chip"
                      :class="name === currentTraceName ? 'chip-ice' : 'chip-mute'"
                    >
                      {{ name }}
                    </span>
                  </div>
                  <span v-else style="font-size:12px; color:var(--text-low);">-</span>
                </td>

                <!-- Data -->
                <td style="max-width:200px;">
                  <div style="font-size:13px; color:var(--text-mid); overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">
                    <span v-if="trace.data?.text" :title="trace.data.text">{{ trace.data.text }}</span>
                    <span v-else-if="hasAdditionalData(trace.data)" style="color:var(--text-low); font-style:italic;">JSON data</span>
                    <span v-else style="color:var(--text-low);">-</span>
                  </div>
                </td>

                <!-- Worker / Group -->
                <td>
                  <div style="font-size:12px; color:var(--text-mid);">
                    <div v-if="trace.worker_id" style="overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:120px;" :title="trace.worker_id">
                      {{ trace.worker_id }}
                    </div>
                    <div v-if="trace.consumer_group && trace.consumer_group !== '__QUEUE_MODE__'" style="color:var(--text-low); margin-top:2px;">
                      {{ trace.consumer_group }}
                    </div>
                    <span v-if="!trace.worker_id && (!trace.consumer_group || trace.consumer_group === '__QUEUE_MODE__')" style="color:var(--text-low);">-</span>
                  </div>
                </td>
              </tr>
            </template>
          </tbody>
        </table>
      </div>

      <!-- Pagination. The range counts the rows actually rendered — deriving it
           from `limit` claimed a page size the response never had. -->
      <div v-if="showPager && (traces.length > 0 || offset > 0)" class="pager">
        <span class="pager-count">
          Showing <span class="tabular-nums">{{ shownFrom }}</span>–<span class="tabular-nums">{{ shownTo }}</span> of <span class="tabular-nums">{{ totalTraces }}</span>
        </span>
        <div class="pager-nav">
          <button class="btn btn-ghost" :disabled="offset === 0" @click="previousPage">Previous</button>
          <button class="btn btn-ghost" :disabled="shownTo >= totalTraces" @click="nextPage">Next</button>
        </div>
      </div>
    </div>

    <!-- Nothing searched yet -->
    <div v-else class="card">
      <div class="card-body">
        <div class="empty-state">
          <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
            <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
          </svg>
          <h3>Search for traces</h3>
          <p>Enter a trace name above and press Search to view events.</p>
        </div>
      </div>
    </div>

    <DetailDrawer
      :open="Boolean(selectedTrace)"
      :title="selectedTrace ? `${selectedTrace.event_type} trace` : 'Trace detail'"
      @close="selectedTrace = null"
    >
      <template #leading>
        <span
          v-if="selectedTrace"
          style="width:10px; height:10px; border-radius:var(--r-pill); flex-shrink:0;"
          :style="{ background: eventColor(selectedTrace.event_type) }"
        />
      </template>

      <template v-if="selectedTrace">
        <div style="display:flex; flex-direction:column; gap:20px;">
            <!-- Basic Info -->
            <div style="display:flex; flex-direction:column; gap:14px;">
              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Time</label>
                <p :title="formatTimestampUtc(selectedTrace.created_at)" style="font-size:13px; color:var(--text-hi);">{{ formatTimestamp(selectedTrace.created_at) }}</p>
              </div>

              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Queue / partition</label>
                <p style="font-size:13px; font-weight:500; color:var(--text-hi);">
                  {{ selectedTrace.queue_name || '-' }} / {{ selectedTrace.partition_name || '-' }}
                </p>
              </div>

              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Transaction ID</label>
                <p class="font-mono" style="font-size:12px; color:var(--text-mid); word-break:break-all;">{{ selectedTrace.transaction_id }}</p>
              </div>

              <div v-if="selectedTrace.worker_id">
                <label class="label-xs" style="display:block; margin-bottom:6px;">Worker ID</label>
                <p class="font-mono" style="font-size:12px; color:var(--text-mid); word-break:break-all;">{{ selectedTrace.worker_id }}</p>
              </div>

              <div v-if="selectedTrace.consumer_group && selectedTrace.consumer_group !== '__QUEUE_MODE__'">
                <label class="label-xs" style="display:block; margin-bottom:6px;">Consumer group</label>
                <p style="font-size:13px; color:var(--text-mid);">{{ selectedTrace.consumer_group }}</p>
              </div>
            </div>

            <!-- Trace Names -->
            <div v-if="selectedTrace.trace_names?.length > 0">
              <label class="label-xs" style="display:block; margin-bottom:8px;">Trace names</label>
              <div style="display:flex; flex-wrap:wrap; gap:6px;">
                <span
                  v-for="name in selectedTrace.trace_names"
                  :key="name"
                  class="chip"
                  :class="name === currentTraceName ? 'chip-ice' : 'chip-mute'"
                >
                  {{ name }}
                </span>
              </div>
            </div>

            <!-- Trace Data -->
            <div v-if="selectedTrace.data">
              <label class="label-xs" style="display:block; margin-bottom:8px;">Trace data</label>

              <!-- Text content -->
              <div v-if="selectedTrace.data.text" style="margin-bottom:12px;">
                <p class="trace-text">{{ selectedTrace.data.text }}</p>
              </div>

              <!-- JSON data (excluding text) -->
              <div v-if="hasAdditionalData(selectedTrace.data)">
                <div class="trace-code">
                  <pre class="font-mono" style="font-size:12px; white-space:pre-wrap; color:var(--text-mid); margin:0;">{{ formatTraceData(selectedTrace.data) }}</pre>
                </div>
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
          View Full Message
        </router-link>
      </template>
    </DetailDrawer>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { contextQuery, queueLocation, readRouteValue, textParam } from '@/composables/navigation'
const route = useRoute(), router = useRouter()
import { traces as tracesApi, describeApiError } from '@/api'
import { useApi } from '@/composables/useApi'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import DetailDrawer from '@/components/DetailDrawer.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'


// Search state
const searchTraceName = ref('')
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
// Our own verdict on an unusable page, kept apart from the transport error.
const pageError = ref(null)

// useApi owns loading/error, aborts on unmount, and discards a response that
// belongs to a cluster we have since left. The panel object is kept whole so
// the card headers can stamp their freshness from it.
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

// Suggestions are the tenant's real trace names or nothing at all — invented
// examples send every click into "No traces found".
const exampleTraceNames = ref([])

// Computed stats
const uniqueMessages = computed(() => {
  const messageIds = new Set(traces.value.map(t => t.transaction_id))
  return messageIds.size
})

const uniqueQueues = computed(() => {
  const queues = new Set(traces.value.map(t => t.queue_name).filter(Boolean))
  return queues.size
})

/** A trace whose partition does not resolve to a queue carries no queue name. */
const queueAttributionAvailable = computed(() => traces.value.some(t => t.queue_name))

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
  // build. Rendering "No traces found" for it denies traces that exist.
  if (!Array.isArray(rows)) {
    pageError.value = `The broker returned no result page at offset ${offset.value}.`
    return
  }
  serverPaginates.value = rows.length <= limit.value
  totalTraces.value = Number(payload?.total) || rows.length
}

// Search traces by name
async function searchTraces() {
  if (!searchTraceName.value.trim()) return
  await router.push({ path: '/traces', query: { ...contextQuery(route), trace: searchTraceName.value.trim() } })
}

async function loadPage() {
  if (hasTraceTarget.value) await fetchTraces()
}

async function loadExampleTraceNames() {
  try {
    const res = await tracesApi.getAvailableNames({ limit: 8 })
    exampleTraceNames.value = (res.data?.trace_names || [])
      .map(n => n?.trace_name)
      .filter(Boolean)
      .slice(0, 8)
  } catch {
    // No suggestions rather than made-up ones; the failure is already reported
    // by the shared HTTP client and the search box still works.
    exampleTraceNames.value = []
  }
}

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
  searchTraceName.value = currentTraceName.value
  tracePanel.abort()
  loading.value = false; apiError.value = null
  offset.value = readRouteValue(route.query.offset, 0)
  selectedTrace.value = null; traceData.value = null; totalTraces.value = 0; pageError.value = null
  if (hasTraceTarget.value) fetchTraces()
}, { immediate: true })

function viewTrace(trace) {
  selectedTrace.value = trace
}

// Event type colours from the token set, so a trace dot means the same thing
// here as a chip does anywhere else in the product.
function eventColor(eventType) {
  const colors = {
    info: 'var(--ice-400)',
    processing: 'var(--ice-400)',
    step: 'var(--crown-400)',
    error: 'var(--ember-400)',
    warning: 'var(--warn-400)',
  }
  return colors[eventType] || 'var(--text-low)'
}

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

// Refresh function — only refreshes when there's an active search
const refreshCurrentView = async () => {
  if (hasTraceTarget.value) {
    await loadPage()
  }
}

// Not useAutoRefresh: this page does not poll, so it shows no live tick. Its
// freshness fact is the per-card stamp().
useRefresh(refreshCurrentView)

onMounted(loadExampleTraceNames)
</script>

<style scoped>
/* Free text stays one step ABOVE the drawer (it was a white-4% lift), the
   JSON block below it stays recessed — the pair has to keep reading apart. */
.trace-text {
  font-size: 13px; color: var(--text-hi);
  border: 1px solid var(--bd); border-radius: var(--r-card); padding: 12px;
  background: var(--ink-3);
}

.trace-code {
  border: 1px solid var(--bd); border-radius: var(--r-card);
  padding: 14px; overflow-x: auto;
  background: var(--recessed);
}
</style>
