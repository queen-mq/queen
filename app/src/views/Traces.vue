<template>
  <div class="view-container">

    <!-- Page head -->
    <!-- Search Box -->
    <div class="card" style="margin-bottom:16px;">
      <div class="card-body">
        <div style="display:flex; gap:12px; align-items:center;">
          <div style="flex:1; position:relative;">
            <input
              v-model="searchTraceName"
              @keyup.enter="searchTraces"
              type="text"
              placeholder="Enter a trace name"
              class="input"
              style="padding-left:36px;"
            />
            <svg style="width:18px; height:18px; color:var(--text-low); position:absolute; left:10px; top:50%; transform:translateY(-50%);" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
          <button
            @click="searchTraces"
            :disabled="!searchTraceName || loading"
            class="btn btn-primary"
            style="padding:8px 20px;"
          >
            Search
          </button>
          <button
            v-if="currentTraceName"
            @click="clearSearch"
            class="btn btn-ghost"
          >
            Clear
          </button>
        </div>

        <!-- Quick examples: real trace names for this tenant, or nothing. -->
        <div v-if="!currentTraceName && exampleTraceNames.length > 0" style="display:flex; flex-wrap:wrap; align-items:center; gap:8px; margin-top:12px;">
          <span style="font-size:12px; color:var(--text-low);">Recent trace names:</span>
          <button
            v-for="example in exampleTraceNames"
            :key="example"
            @click="searchTraceName = example; searchTraces()"
            class="chip chip-mute"
            style="cursor:pointer; font-size:12px;"
          >
            {{ example }}
          </button>
        </div>
      </div>
    </div>

    <!-- Results Summary -->
    <div v-if="currentTraceName && traces.length > 0" class="card card-accent" style="margin-bottom:16px;">
      <div class="card-body" style="padding:12px 16px;">
        <div style="display:flex; align-items:center; gap:12px;">
          <svg style="width:20px; height:20px; color:var(--text-mid); flex-shrink:0;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <div>
            <p style="font-size:13px; font-weight:600; color:var(--text-hi);">
              Found <span class="font-mono tabular-nums">{{ totalTraces }}</span> trace{{ totalTraces !== 1 ? 's' : '' }} for: <span style="color:var(--text-hi);">{{ currentTraceName }}</span>
            </p>
            <p style="font-size:12px; color:var(--text-mid); margin-top:2px;">
              <span class="font-mono tabular-nums">{{ uniqueMessages }}</span> unique message{{ uniqueMessages !== 1 ? 's' : '' }} ·
              <template v-if="queueAttributionAvailable">
                <span class="font-mono tabular-nums">{{ uniqueQueues }}</span> queue{{ uniqueQueues !== 1 ? 's' : '' }}
              </template>
              <!-- 0 queues would read as "these traces touched no queue". They
                   touched queues the broker could not name for these rows. -->
              <template v-else>queue not recorded for these traces</template>
            </p>
            <p v-if="!serverPaginates" style="font-size:12px; color:var(--warn-400); margin-top:2px;">
              This broker returned every trace in one response — the page controls would page nothing, so they are hidden.
            </p>
            <p style="font-size:11px; color:var(--text-low); margin-top:2px; font-family:'JetBrains Mono',monospace;">{{ scopeLabel }}</p>
          </div>
        </div>
      </div>
    </div>

    <!-- Loading State -->
    <div v-if="loading" class="card">
      <div class="card-body" style="padding:48px 16px; text-align:center;">
        <div class="spinner" style="margin:0 auto 12px;" />
        <p style="font-size:13px; color:var(--text-mid);">Loading traces…</p>
      </div>
    </div>

    <!-- Error State -->
    <div v-else-if="error" class="card" style="border-color:rgba(244,63,94,.25);">
      <div class="card-body">
        <p style="font-size:13px; color:var(--ember-400);">
          <strong>Error:</strong> {{ error }}
        </p>
      </div>
    </div>

    <!-- Traces Table -->
    <div v-else-if="currentTraceName && traces.length > 0" class="card">
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
                    style="width:8px; height:8px; border-radius:99px; flex-shrink:0;"
                    :style="{ background: eventColor(trace.event_type) }"
                  />
                  <span style="font-size:11px; font-weight:600; letter-spacing:.08em; text-transform:uppercase; color:var(--text-hi);">
                    {{ trace.event_type }}
                  </span>
                </div>
              </td>

              <!-- Time -->
              <td>
                <span class="font-mono" style="font-size:12px; color:var(--text-mid); white-space:nowrap;">
                  {{ formatDateTime(trace.created_at) }}
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
                  title="This trace's partition resolves on neither engine, so the broker cannot name its queue."
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
          </tbody>
        </table>
      </div>

      <!-- Pagination. The range counts the rows actually rendered — deriving it
           from `limit` claimed a page size the response never had. -->
      <div v-if="showPager" style="padding:12px 16px; border-top:1px solid var(--bd); display:flex; align-items:center; justify-content:space-between;">
        <p style="font-size:13px; color:var(--text-mid);">
          Showing <span class="font-mono tabular-nums">{{ shownFrom }}</span>–<span class="font-mono tabular-nums">{{ shownTo }}</span> of <span class="font-mono tabular-nums">{{ totalTraces }}</span>
        </p>
        <div style="display:flex; gap:8px;">
          <button
            @click="previousPage"
            :disabled="offset === 0"
            class="btn btn-ghost"
            style="font-size:12px;"
          >
            Previous
          </button>
          <button
            @click="nextPage"
            :disabled="shownTo >= totalTraces"
            class="btn btn-ghost"
            style="font-size:12px;"
          >
            Next
          </button>
        </div>
      </div>
    </div>

    <!-- No Results -->
    <div v-else-if="currentTraceName && traces.length === 0 && !loading" class="card">
      <div class="card-body" style="padding:48px 16px; text-align:center;">
        <svg style="width:56px; height:56px; margin:0 auto 16px; color:var(--text-low);" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        <p style="font-size:16px; font-weight:500; color:var(--text-hi); margin-bottom:4px;">No traces found</p>
        <p style="font-size:13px; color:var(--text-mid);">No traces found for: <span style="font-weight:500;">{{ currentTraceName }}</span></p>
      </div>
    </div>

    <!-- Initial Prompt (Default View) -->
    <div v-else-if="!currentTraceName" class="card">
      <div class="card-body" style="padding:48px 16px; text-align:center;">
        <svg style="width:48px; height:48px; margin:0 auto 12px; color:var(--text-low);" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
        <p style="font-size:14px; font-weight:500; color:var(--text-hi); margin-bottom:4px;">Search for traces</p>
        <p style="font-size:13px; color:var(--text-mid);">Enter a trace name above and press Search to view events.</p>
      </div>
    </div>

    <!-- Trace Detail Panel (teleported to body to avoid transform issues) -->
    <Teleport to="body">
      <div v-if="selectedTrace" class="trace-panel">
        <div style="padding:20px 24px;">
          <!-- Header -->
          <div style="display:flex; align-items:flex-start; justify-content:space-between; margin-bottom:20px; padding-bottom:16px; border-bottom:1px solid var(--bd);">
            <div style="flex:1; min-width:0;">
              <div style="display:flex; align-items:center; gap:10px; margin-bottom:6px;">
                <span
                  style="width:10px; height:10px; border-radius:99px; flex-shrink:0;"
                  :style="{ background: eventColor(selectedTrace.event_type) }"
                />
                <h3 style="font-size:15px; font-weight:700; color:var(--text-hi); text-transform:uppercase; letter-spacing:.06em;">
                  {{ selectedTrace.event_type }} Trace
                </h3>
              </div>
              <p class="font-mono" style="font-size:12px; color:var(--text-low); word-break:break-all;">
                {{ selectedTrace.transaction_id }}
              </p>
            </div>
            <button
              @click="selectedTrace = null"
              class="btn btn-ghost btn-icon"
              style="padding:6px; border-radius:8px;"
            >
              <svg style="width:18px; height:18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          <!-- Content -->
          <div style="display:flex; flex-direction:column; gap:20px;">
            <!-- Basic Info -->
            <div style="display:flex; flex-direction:column; gap:14px;">
              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Time</label>
                <p style="font-size:13px; color:var(--text-hi);">{{ formatDateTime(selectedTrace.created_at) }}</p>
              </div>

              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Queue / Partition</label>
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
                <label class="label-xs" style="display:block; margin-bottom:6px;">Consumer Group</label>
                <p style="font-size:13px; color:var(--text-mid);">{{ selectedTrace.consumer_group }}</p>
              </div>
            </div>

            <!-- Trace Names -->
            <div v-if="selectedTrace.trace_names?.length > 0">
              <label class="label-xs" style="display:block; margin-bottom:8px;">Trace Names</label>
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
              <label class="label-xs" style="display:block; margin-bottom:8px;">Trace Data</label>

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

            <!-- Link to Message -->
            <div style="padding-top:16px; border-top:1px solid var(--bd);">
              <router-link
                :to="`/messages?partitionId=${selectedTrace.partition_id}&transactionId=${selectedTrace.transaction_id}`"
                class="btn"
                style="width:100%; justify-content:center; font-size:12.5px;"
                @click="selectedTrace = null"
              >
                <svg style="width:16px; height:16px; margin-right:6px;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                </svg>
                View Full Message
              </router-link>
            </div>
          </div>
        </div>
      </div>

      <!-- Backdrop -->
      <div
        v-if="selectedTrace"
        class="trace-backdrop"
        @click="selectedTrace = null"
      ></div>
    </Teleport>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { traces as tracesApi, describeApiError } from '@/api'
import { useApi, formatDateTime } from '@/composables/useApi'
import { useRefresh } from '@/composables/useRefresh'
import { useIdentity } from '@/stores/identity'

const { actingTenantSlug, actingClusterSlug } = useIdentity()

// Search state
const searchTraceName = ref('')
const currentTraceName = ref('')
const selectedTrace = ref(null)
const offset = ref(0)
const limit = ref(50)
const totalTraces = ref(0)
// Our own verdict on an unusable page, kept apart from the transport error.
const pageError = ref(null)

// useApi owns loading/error, aborts on unmount, and discards a response that
// belongs to a cluster we have since left.
const {
  data: traceData,
  loading,
  error: apiError,
  execute: executeTraces,
} = useApi(
  (name, params, config) => tracesApi.getByName(name, params, config),
  { immediate: false },
)

const traces = computed(() =>
  Array.isArray(traceData.value?.traces) ? traceData.value.traces : []
)
const error = computed(() =>
  pageError.value || (apiError.value ? describeApiError(apiError.value) : null)
)
// Flipped off when a page comes back larger than it was asked for: that broker
// is not applying the limit, so a page range would be fiction.
const serverPaginates = ref(true)

// Suggestions are the tenant's real trace names or nothing at all — invented
// examples send every click into "No traces found".
const exampleTraceNames = ref([])

const scopeLabel = computed(
  () => `Tenant-scoped · tenant ${actingTenantSlug.value || '—'} · cluster ${actingClusterSlug.value || '—'}`
)

// Computed stats
const uniqueMessages = computed(() => {
  const messageIds = new Set(traces.value.map(t => t.transaction_id))
  return messageIds.size
})

const uniqueQueues = computed(() => {
  const queues = new Set(traces.value.map(t => t.queue_name).filter(Boolean))
  return queues.size
})

/** A trace whose partition resolves on neither engine carries no queue name. */
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

  offset.value = 0
  currentTraceName.value = searchTraceName.value.trim()
  totalTraces.value = 0
  await fetchTraces()
}

// Load page of traces
async function loadPage() {
  if (!currentTraceName.value) return
  await fetchTraces()
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
    // by the interceptor and the search box still works.
    exampleTraceNames.value = []
  }
}

function previousPage() {
  if (offset.value > 0) {
    offset.value = Math.max(0, offset.value - limit.value)
    loadPage()
  }
}

function nextPage() {
  if (shownTo.value >= totalTraces.value) return
  offset.value += traces.value.length || limit.value
  loadPage()
}

function clearSearch() {
  searchTraceName.value = ''
  currentTraceName.value = ''
  traceData.value = null
  totalTraces.value = 0
  offset.value = 0
  pageError.value = null
  apiError.value = null
}

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
  if (currentTraceName.value) {
    await loadPage()
  }
}

useRefresh(refreshCurrentView)

onMounted(loadExampleTraceNames)
</script>

<style scoped>
/* Teleported, but still this component's DOM — scoped styles apply. Split by
   theme instead of hardcoding a dark gradient inline. */
.trace-panel {
  position: fixed; top: 0; right: 0; bottom: 0;
  width: 100%; max-width: 640px; z-index: 50;
  overflow-y: auto;
  border-left: 1px solid var(--bd);
  backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px);
}
html:not(.light) .trace-panel {
  background: linear-gradient(180deg, rgba(20,20,26,.95), rgba(14,14,18,.97));
}
html.light .trace-panel {
  background: rgba(255,255,255,.97);
  box-shadow: -10px 0 40px -10px rgba(0,0,0,.1);
}

.trace-backdrop {
  position: fixed; inset: 0; z-index: 40;
  background: rgba(4,4,6,.55);
  backdrop-filter: blur(6px); -webkit-backdrop-filter: blur(6px);
}

.trace-text {
  font-size: 13px; color: var(--text-hi);
  border: 1px solid var(--bd); border-radius: 10px; padding: 12px;
}
html:not(.light) .trace-text { background: rgba(255,255,255,.04); }
html.light .trace-text { background: var(--paper-1); }

.trace-code {
  border: 1px solid var(--bd); border-radius: 10px;
  padding: 14px; overflow-x: auto;
}
html:not(.light) .trace-code { background: var(--ink-0); }
html.light .trace-code { background: var(--paper-1); }
</style>
