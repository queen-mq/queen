<template>
  <div class="view-container">

    <!-- Scope. These rows are the acting tenant's, on the acting cluster's cell;
         saying so is what keeps a tenant number from being read as a cell number.
         Built from identity, not from the fetch, so it survives a failed load. -->
    <div class="scope-strip">
      <span class="chip chip-mute">tenant scope</span>
      <span class="scope-text">
        <strong>{{ actingTenantSlug || 'no tenant' }}</strong>
        <span class="scope-sep">/</span>{{ actingClusterSlug || 'no cluster' }}
        <span class="scope-sep">·</span>cell {{ actingCellSlug || 'unknown' }}
      </span>
      <span class="scope-fill"></span>
    </div>

    <!-- The list failed. Whatever is in the table below is stale, and saying so
         is the whole point — an empty table would read as "no messages". -->
    <div v-if="listError" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not load messages</strong> · {{ describeApiError(listError) }}<template v-if="messages.length">
          · showing the last rows that loaded{{ lastUpdatedText ? ` (${lastUpdatedText})` : '' }}</template>
      </span>
    </div>

    <!-- The broker answered page N with page N-1's rows: paging further would
         renumber the same messages. -->
    <div v-if="paginationStalled" class="status-banner banner-warn view-banner">
      <span>
        <strong>Pagination is not advancing</strong> ·
        this broker returned the same rows for page {{ currentPage }}. Narrow the time range or the filters instead.
      </span>
    </div>

    <!-- Filters -->
    <div class="card filters">
      <div class="card-body filter-rows">
        <!-- Entity filters -->
        <div class="filter-row">
          <div class="filter-search">
            <svg class="filter-search-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
            </svg>
            <input
              v-model="searchQuery"
              type="text"
              placeholder="Filter loaded rows — transaction or partition ID"
              class="input"
              title="Filters the rows already on this page. It is not a server-side transaction lookup."
            />
          </div>

          <div class="filter-field-col">
            <label class="label-xs">Queue</label>
            <select v-model="filterQueue" class="input">
              <option value="">All Queues</option>
              <option v-for="q in queues" :key="q.name" :value="q.name">
                {{ q.name }}
              </option>
            </select>
          </div>

          <div class="filter-field-col">
            <label class="label-xs">Partition name</label>
            <input
              v-model="filterPartition"
              type="text"
              placeholder="Filter by name..."
              class="input"
            />
          </div>

          <div class="filter-field-col">
            <label class="label-xs">Status</label>
            <select v-model="filterStatus" class="input">
              <option value="">All Status</option>
              <option value="pending">Pending</option>
              <option value="processing">Processing</option>
              <option value="completed">Completed</option>
              <option value="dead_letter">Dead Letter</option>
            </select>
          </div>

          <div class="filter-field-col">
            <label class="label-xs">Limit</label>
            <select v-model="limit" class="input">
              <option :value="50">50 messages</option>
              <option :value="100">100 messages</option>
              <option :value="200">200 messages</option>
              <option :value="500">500 messages</option>
            </select>
          </div>
        </div>

        <!-- Time window and actions. The 1h / 24h / 7d buttons only PREFILL the
             two inputs — nothing is selected until Apply runs — so they stay
             loose buttons under a "quick fill" label instead of becoming a
             range picker with an active state the page has not chosen. -->
        <div class="filter-row">
          <div class="filter-field">
            <span class="label-xs">From</span>
            <input v-model="filterFrom" type="datetime-local" class="input" />
          </div>

          <div class="filter-field">
            <span class="label-xs">To</span>
            <input v-model="filterTo" type="datetime-local" class="input" />
          </div>

          <div class="filter-field">
            <span class="label-xs">Quick fill</span>
            <button class="btn btn-ghost" @click="setTimeRange(1)">1h</button>
            <button class="btn btn-ghost" @click="setTimeRange(24)">24h</button>
            <button class="btn btn-ghost" @click="setTimeRange(168)">7d</button>
          </div>

          <button class="btn btn-primary" @click="applyFilters">Apply</button>
          <button v-if="hasActiveFilters" class="btn btn-ghost" @click="clearFilters">Clear</button>
        </div>
      </div>
    </div>

    <!-- Bus mode. Driven by what the rows actually report as well as by the
         top-level mode: a log-engine queue reports its groups per message. -->
    <div v-if="busGroups > 0" class="card" style="margin-bottom:16px;">
      <div class="card-body">
        <div style="display:flex; align-items:center; gap:8px; font-size:13px;">
          <svg style="width:16px; height:16px; color:var(--text-mid);" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
          </svg>
          <span style="font-weight:600; color:var(--text-hi);">Bus Mode Active</span>
          <span class="chip chip-ice">{{ busGroups }} consumer group(s)</span>
        </div>
      </div>
    </div>

    <!-- Messages list -->
    <div class="card">
      <div class="card-header">
        <h3>Messages</h3>
        <span class="chip chip-mute">{{ formatNumber(messages.length) }} loaded</span>
        <span class="chip chip-mute">page <span class="font-mono tabular-nums">{{ currentPage }}</span></span>
        <span class="muted">{{ stamp(listPanel) }}</span>
      </div>

      <div style="overflow-x:auto;">
        <table class="t">
          <thead>
            <tr>
              <th>Queue</th>
              <th class="hidden xl:table-cell">Partition ID</th>
              <th class="hidden lg:table-cell">Partition</th>
              <th>Transaction ID</th>
              <th style="text-align:right;">Created</th>
              <th style="text-align:right;">Status</th>
            </tr>
          </thead>
          <tbody>
            <template v-if="loading && !messages.length">
              <tr v-for="i in 10" :key="i">
                <td><div class="skeleton" style="height:16px; width:96px;" /></td>
                <td class="hidden xl:table-cell"><div class="skeleton" style="height:16px; width:128px;" /></td>
                <td class="hidden lg:table-cell"><div class="skeleton" style="height:16px; width:48px;" /></td>
                <td><div class="skeleton" style="height:16px; width:160px;" /></td>
                <td><div class="skeleton" style="height:16px; width:112px;" /></td>
                <td><div class="skeleton" style="height:16px; width:80px;" /></td>
              </tr>
            </template>
            <template v-else-if="filteredMessages.length > 0">
              <tr
                v-for="(message, idx) in filteredMessages"
                :key="rowKey(message, idx)"
                :style="isAddressable(message) ? 'cursor:pointer;' : 'cursor:default;'"
                @click="isAddressable(message) && selectMessage(message)"
              >
                <td>
                  <div style="font-size:13px; font-weight:500; color:var(--text-hi);">{{ message.queue }}</div>
                  <div class="lg:hidden" style="font-size:11px; color:var(--text-low); margin-top:2px;">
                    {{ message.partition }}
                  </div>
                </td>
                <td class="hidden xl:table-cell">
                  <div class="font-mono" style="font-size:11px; color:var(--text-mid); word-break:break-all; user-select:all;">
                    {{ message.partitionId }}
                  </div>
                </td>
                <td class="hidden lg:table-cell">
                  <span style="font-size:12px; color:var(--text-mid);">{{ message.partition }}</span>
                </td>
                <td>
                  <!-- The txn id lives inside the segment blob. Once retention
                       drops the segment the broker cannot resolve it: say so
                       instead of rendering an empty, clickable cell. -->
                  <div
                    v-if="isAddressable(message)"
                    class="font-mono"
                    style="font-size:11px; color:var(--text-hi); word-break:break-all; user-select:all;"
                  >
                    {{ message.transactionId }}
                  </div>
                  <span
                    v-else
                    class="chip chip-mute"
                    title="The covering log segment was removed by retention, so the broker can no longer resolve this message's transaction id or payload."
                  >
                    payload expired
                  </span>
                </td>
                <td class="font-mono tabular-nums" style="text-align:right; font-size:12px; color:var(--text-mid); white-space:nowrap;">
                  {{ formatDateTime(message.createdAt) }}
                </td>
                <td style="text-align:right;">
                  <div style="display:flex; flex-direction:column; align-items:flex-end; gap:4px;">
                    <span
                      class="chip"
                      :class="{
                        'chip-ice': message.status === 'pending',
                        'chip-warn': message.status === 'processing',
                        'chip-ok': message.status === 'completed',
                        'chip-bad': message.status === 'dead_letter' || message.status === 'failed'
                      }"
                    >
                      {{ message.status }}
                    </span>
                    <div v-if="message.busStatus && message.busStatus.totalGroups > 0" style="font-size:11px; color:var(--text-low);">
                      {{ message.busStatus.consumedBy }}/{{ message.busStatus.totalGroups }} groups
                    </div>
                  </div>
                </td>
              </tr>
            </template>
            <!-- Never an idle empty state on a failure: that is a load error
                 wearing "no messages" as a disguise. -->
            <tr v-else-if="listError">
              <td colspan="6">
                <div class="empty-state empty-state-failed">
                  <h3>{{ describeApiError(listError) }}</h3>
                  <p>Nothing loaded — this is a failure, not an empty queue.</p>
                </div>
              </td>
            </tr>
            <tr v-else>
              <td colspan="6">
                <div class="empty-state">
                  <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M21.75 6.75v10.5a2.25 2.25 0 01-2.25 2.25h-15a2.25 2.25 0 01-2.25-2.25V6.75m19.5 0A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25m19.5 0v.243a2.25 2.25 0 01-1.07 1.916l-7.5 4.615a2.25 2.25 0 01-2.36 0L3.32 8.91a2.25 2.25 0 01-1.07-1.916V6.75" />
                  </svg>
                  <template v-if="searchQuery && messages.length">
                    <h3>No row matches this filter</h3>
                    <p>No loaded row matches “{{ searchQuery }}” — the filter only searches this page.</p>
                  </template>
                  <template v-else>
                    <h3>No messages found</h3>
                    <p>Try a wider time range, or fewer filters.</p>
                  </template>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Pagination. Not drawn under an empty first page or under a failure:
           Previous/Next below "no messages" offer travel that goes nowhere. -->
      <div v-if="messages.length || currentPage > 1" class="pager">
        <span class="pager-count">Page <span class="font-mono tabular-nums">{{ currentPage }}</span></span>
        <div class="pager-nav">
          <button class="btn btn-ghost" :disabled="currentPage === 1" @click="prevPage">Previous</button>
          <button class="btn btn-ghost" :disabled="!canPageForward" @click="nextPage">Next</button>
        </div>
      </div>
    </div>

    <!-- Message detail panel (teleported to body to avoid transform issues) -->
    <Teleport to="body">
      <!-- Backdrop before the panel, so DOM order matches paint order. -->
      <div v-if="selectedMessage" class="modal-backdrop" @click="closePanel"></div>

      <div v-if="selectedMessage" class="drawer-panel">
        <div class="card-header">
          <h3>Message Details</h3>
          <span v-if="messageDetail?.transactionId" class="card-sub font-mono">{{ messageDetail.transactionId }}</span>
          <button
            @click="closePanel"
            class="btn btn-ghost btn-icon modal-close"
          >
            <svg style="width:18px; height:18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      <div class="card-body">
        <div v-if="detailLoading" style="text-align:center; padding:48px 0;">
          <div class="spinner" style="margin:0 auto 12px;"></div>
          <p style="color:var(--text-low);">Loading details...</p>
        </div>

        <div v-else-if="detailError" class="panel-err">
          {{ detailError }}
        </div>

        <template v-else-if="messageDetail">
          <!-- Status -->
          <div style="margin-bottom:24px;">
            <div style="margin-bottom:20px;">
              <span
                class="chip"
                style="font-size:12px;"
                :class="{
                  'chip-ice': messageDetail.status === 'pending',
                  'chip-warn': messageDetail.status === 'processing',
                  'chip-ok': messageDetail.status === 'completed',
                  'chip-bad': messageDetail.status === 'dead_letter' || messageDetail.status === 'failed'
                }"
              >
                {{ messageDetail.status }}
              </span>
            </div>

            <div style="display:flex; flex-direction:column; gap:16px;">
              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Queue / Partition</label>
                <p style="font-size:13px; font-weight:500; color:var(--text-hi);">
                  {{ messageDetail.queue }} / {{ messageDetail.partition }}
                </p>
              </div>

              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Partition ID</label>
                <p class="font-mono" style="font-size:11px; color:var(--text-mid); word-break:break-all;">{{ messageDetail.partitionId }}</p>
              </div>

              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Transaction ID</label>
                <p class="font-mono" style="font-size:11px; color:var(--text-mid); word-break:break-all;">{{ messageDetail.transactionId }}</p>
              </div>

              <div>
                <label class="label-xs" style="display:block; margin-bottom:6px;">Created</label>
                <p style="font-size:13px; color:var(--text-mid);">{{ formatDateTime(messageDetail.createdAt) }}</p>
              </div>

              <div v-if="messageDetail.traceId">
                <label class="label-xs" style="display:block; margin-bottom:6px;">Trace ID</label>
                <p class="font-mono" style="font-size:11px; color:var(--text-mid); word-break:break-all;">{{ messageDetail.traceId }}</p>
              </div>

              <div v-if="messageDetail.errorMessage">
                <label class="label-xs" style="display:block; margin-bottom:6px;">Error Message</label>
                <p style="font-size:13px; color:var(--ember-400);">{{ messageDetail.errorMessage }}</p>
              </div>

              <div v-if="messageDetail.retryCount">
                <label class="label-xs" style="display:block; margin-bottom:6px;">Retry Count</label>
                <p class="font-mono tabular-nums" style="font-size:13px; color:var(--text-mid);">{{ messageDetail.retryCount }}</p>
              </div>
            </div>
          </div>

          <!-- Queue Config -->
          <div v-if="messageDetail.queueConfig" style="margin-bottom:24px;">
            <h4 style="font-size:13px; font-weight:600; margin-bottom:12px; color:var(--text-hi);">Queue Config</h4>
            <div class="card" style="padding:14px 16px;">
              <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; font-size:13px;">
                <div>
                  <span style="color:var(--text-low);">Lease Time:</span>
                  <span class="font-mono tabular-nums" style="font-weight:500; margin-left:4px; color:var(--text-hi);">{{ messageDetail.queueConfig.leaseTime }}s</span>
                </div>
                <div>
                  <span style="color:var(--text-low);">TTL:</span>
                  <span class="font-mono tabular-nums" style="font-weight:500; margin-left:4px; color:var(--text-hi);">{{ messageDetail.queueConfig.ttl }}s</span>
                </div>
                <div>
                  <span style="color:var(--text-low);">Retry Limit:</span>
                  <span class="font-mono tabular-nums" style="font-weight:500; margin-left:4px; color:var(--text-hi);">{{ messageDetail.queueConfig.retryLimit }}</span>
                </div>
                <div>
                  <span style="color:var(--text-low);">Retry Delay:</span>
                  <span class="font-mono tabular-nums" style="font-weight:500; margin-left:4px; color:var(--text-hi);">{{ messageDetail.queueConfig.retryDelay }}ms</span>
                </div>
              </div>
            </div>
          </div>

          <!-- Payload -->
          <div style="margin-bottom:24px;">
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
              <h4 style="font-size:13px; font-weight:600; color:var(--text-hi);">Payload</h4>
              <button
                v-if="payloadAvailable"
                @click="copyPayload"
                class="btn btn-ghost" style="padding:4px 8px; font-size:11px; gap:4px;"
              >
                <svg v-if="!payloadCopied" style="width:14px; height:14px;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                </svg>
                <svg v-else style="width:14px; height:14px; color:var(--ok-500);" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                </svg>
                {{ payloadCopied ? 'Copied!' : 'Copy' }}
              </button>
            </div>
            <!-- payloadAvailable:false means the segment is gone. An empty
                 payload box would read as "this message carried nothing". -->
            <div v-if="!payloadAvailable" class="card" style="padding:12px 14px;">
              <p style="font-size:13px; color:var(--text-mid);">
                Payload unavailable — the covering log segment was removed by retention.
              </p>
            </div>
            <div v-else class="msg-code">
              <pre class="font-mono" style="font-size:12px; white-space:pre-wrap; margin:0;" v-html="highlightJson(messageDetail.payload)"></pre>
            </div>
          </div>

          <!-- Consumer Groups -->
          <div v-if="messageDetail.consumerGroups && messageDetail.consumerGroups.length > 0" style="margin-bottom:24px;">
            <h4 style="font-size:13px; font-weight:600; margin-bottom:12px; color:var(--text-hi);">Consumer Groups</h4>
            <div style="display:flex; flex-direction:column; gap:8px;">
              <div
                v-for="group in messageDetail.consumerGroups"
                :key="group.name"
                class="card"
                style="display:flex; align-items:center; justify-content:space-between; padding:10px 14px;"
              >
                <span style="font-size:13px; font-weight:500; color:var(--text-hi);">{{ group.name === '__QUEUE_MODE__' ? 'Queue Mode' : group.name }}</span>
                <span
                  class="chip"
                  :class="group.consumed ? 'chip-ok' : 'chip-ice'"
                >
                  {{ group.consumed ? 'Consumed' : 'Pending' }}
                </span>
              </div>
            </div>
          </div>

          <!-- Actions -->
          <div style="display:flex; flex-direction:column; gap:8px; padding-top:8px;">
            <!-- Sits with the button that produced it: this drawer scrolls, and
                 a delete failure hoisted to the top would land off screen. -->
            <div v-if="actionError" class="panel-err">{{ actionError }}</div>

            <div v-if="messageDetail.status === 'completed'" class="card" style="padding:12px 14px; border-color:var(--ok-bd);">
              <div style="display:flex; gap:8px; align-items:center; font-size:13px; color:var(--ok-500);">
                <svg style="width:18px; height:18px; flex-shrink:0;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <p>This message has been successfully consumed and acknowledged.</p>
              </div>
            </div>

            <!-- Only dead-lettered messages are deletable: a live payload lives
                 in an immutable log segment, and the broker answers a delete on
                 one with success:false. Retry / move-to-DLQ are not offered at
                 all — the broker has no route for either. -->
            <button
              v-if="isDeletable && canAdmin"
              @click="deleteMessage"
              :disabled="actionLoading"
              class="btn btn-danger" style="width:100%; justify-content:center;"
            >
              <svg style="width:16px; height:16px;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
              {{ actionLoading ? 'Purging…' : 'Purge dead-letter entry' }}
            </button>

            <p v-else-if="isDeletable" style="font-size:12px; color:var(--text-low);">
              Purging a dead-letter entry needs the admin role on this cluster.
            </p>

            <p v-else style="font-size:12px; color:var(--text-low);">
              Live messages are stored in immutable log segments: they cannot be deleted, retried or
              re-routed from here. Only dead-lettered entries can be purged.
            </p>
          </div>
        </template>
      </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { useRoute } from 'vue-router'
import { messages as messagesApi, queues as queuesApi, describeApiError } from '@/api'
import { useApi, formatNumber, formatDateTime, formatRelativeTime } from '@/composables/useApi'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'

const route = useRoute()
const { can, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
const { notifySuccess } = useToast()

// State
const paginationStalled = ref(false)
let requestedPage = 1
let pageHeadKey = null

const searchQuery = ref('')
const filterQueue = ref('')
const filterPartition = ref('')
const filterStatus = ref('')
const filterFrom = ref('')
const filterTo = ref('')
const limit = ref(100)
const currentPage = ref(1)

const selectedMessage = ref(null)
const messageDetail = ref(null)
const detailLoading = ref(false)
const detailError = ref(null)
const actionLoading = ref(false)
const actionError = ref(null)
const payloadCopied = ref(false)

// The list, its loading/error state and its "as of when" come from one place.
// useApi also aborts on unmount and discards any response that belongs to a
// cluster we have since left, so a late answer cannot land under a new tenant.
// Kept as one object as well as destructured refs: the shared `stamp()` takes
// the whole panel, because "stale · last good HH:MM" needs `failed` next to
// `lastUpdated`.
const listPanel = useApi((params, config) => messagesApi.list(params, config), {
  immediate: false,
  onSuccess: (payload) => {
    const rows = payload?.messages || []
    // Same head row on a later page means the broker ignored the offset: the
    // page counter would climb over identical rows.
    paginationStalled.value = requestedPage > 1 && rows.length > 0 && headKey(rows) === pageHeadKey
    pageHeadKey = headKey(rows)
  },
})

const {
  data: listData,
  loading,
  error: listError,
  lastUpdated,
  execute: executeList,
} = listPanel

const { data: queuesData, refresh: refreshQueues } = useApi(
  (config) => queuesApi.list(undefined, config),
  { immediate: false },
)

const messages = computed(() => listData.value?.messages || [])
const queues = computed(() => queuesData.value?.queues || [])
const queueMode = computed(() => listData.value?.mode || null)

// Permissions come from the identity store only — never from whether a call 403'd.
const canAdmin = computed(() => can('queueAdmin'))

// Computed
const filteredMessages = computed(() => {
  let result = [...messages.value]

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase()
    result = result.filter(m =>
      m.transactionId?.toLowerCase().includes(query) ||
      m.partitionId?.toLowerCase().includes(query)
    )
  }

  return result
})

const hasActiveFilters = computed(() => {
  return searchQuery.value || filterQueue.value || filterPartition.value || filterStatus.value
})

// A log queue reports its groups per message even when the queue-level mode
// probe says nothing, so take whichever evidence exists.
const busGroups = computed(() => {
  const declared = Number(queueMode.value?.busGroupsCount) || 0
  const observed = messages.value.reduce(
    (max, m) => Math.max(max, Number(m.busStatus?.totalGroups) || 0), 0
  )
  return Math.max(declared, observed)
})

const lastUpdatedText = computed(() =>
  lastUpdated.value ? formatRelativeTime(lastUpdated.value) : null
)

// The combined page can carry rows from both engines (up to 2x limit), so a
// short page is the only reliable "there is no more" signal.
const canPageForward = computed(
  () => !paginationStalled.value && messages.value.length >= limit.value
)

const isDeletable = computed(() => messageDetail.value?.status === 'dead_letter')
const payloadAvailable = computed(() => messageDetail.value?.payloadAvailable !== false)

// 047 emits id/transactionId as NULL for log entries and the broker backfills
// them from the frame — which it cannot do once retention removed the segment.
// Such a row is not addressable: no key, no detail fetch.
const isAddressable = (m) => Boolean(m?.partitionId && m?.transactionId)
const rowKey = (m, idx) => m.transactionId || m.txnHash || `${m.partitionId || 'na'}:${idx}`
const headKey = (rows) => (rows.length ? rowKey(rows[0], 0) : null)

// Helper to format Date to datetime-local input format
const formatDateTimeLocal = (date) => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  return `${year}-${month}-${day}T${hours}:${minutes}`
}

// Helper to convert datetime-local to ISO string
const toISOString = (dateTimeLocal) => {
  if (!dateTimeLocal) return ''
  return new Date(dateTimeLocal).toISOString()
}

// Set time range preset
const setTimeRange = (hours) => {
  const now = new Date()
  const from = new Date(now.getTime() - hours * 60 * 60 * 1000)

  filterFrom.value = formatDateTimeLocal(from)
  filterTo.value = formatDateTimeLocal(now)
}

// Clear all filters
const clearFilters = () => {
  searchQuery.value = ''
  filterQueue.value = ''
  filterPartition.value = ''
  filterStatus.value = ''

  // Reset to default last 1 hour
  setTimeRange(1)

  applyFilters()
}

// Methods
const buildParams = () => {
  const params = {
    limit: limit.value,
    offset: (currentPage.value - 1) * limit.value
  }
  if (filterQueue.value) params.queue = filterQueue.value
  if (filterPartition.value) params.partition = filterPartition.value
  if (filterStatus.value) params.status = filterStatus.value
  if (filterFrom.value) params.from = toISOString(filterFrom.value)
  if (filterTo.value) params.to = toISOString(filterTo.value)
  return params
}

// A failed load keeps the rows that are already on screen — the banner says
// they are stale. The failure itself is already on the global surface.
const fetchMessages = () => {
  requestedPage = currentPage.value
  return executeList(buildParams()).catch(() => {})
}

const applyFilters = () => {
  currentPage.value = 1
  fetchMessages()
}

const prevPage = () => {
  if (currentPage.value > 1) {
    currentPage.value--
    fetchMessages()
  }
}

const nextPage = () => {
  if (!canPageForward.value) return
  currentPage.value++
  fetchMessages()
}

const closePanel = () => {
  selectedMessage.value = null
  actionError.value = null
}

const openMessage = async (partitionId, transactionId) => {
  selectedMessage.value = { partitionId, transactionId }
  detailLoading.value = true
  detailError.value = null
  actionError.value = null
  messageDetail.value = null
  payloadCopied.value = false

  try {
    const response = await messagesApi.get(partitionId, transactionId)
    messageDetail.value = response.data
  } catch (err) {
    detailError.value = describeApiError(err)
  } finally {
    detailLoading.value = false
  }
}

const selectMessage = (message) => openMessage(message.partitionId, message.transactionId)

const deleteMessage = async () => {
  const detail = messageDetail.value
  if (!detail || !canAdmin.value) return
  if (!confirm('Purge this dead-letter entry? This action cannot be undone.')) return

  actionLoading.value = true
  actionError.value = null

  try {
    const res = await messagesApi.delete(detail.partitionId, detail.transactionId)
    // 200 with success:false is the broker's "nothing matched" (and its
    // cross-tenant refusal). Closing the panel on that reports a deletion that
    // did not happen.
    if (res.data?.success !== true) {
      actionError.value = res.data?.message || 'The broker did not delete this message.'
      return
    }
    notifySuccess(`Purged dead-letter entry ${detail.transactionId}`)
    closePanel()
    fetchMessages()
  } catch (err) {
    actionError.value = describeApiError(err)
  } finally {
    actionLoading.value = false
  }
}

const formatPayload = (payload) => {
  if (!payload) return 'null'
  if (typeof payload === 'string') {
    try {
      return JSON.stringify(JSON.parse(payload), null, 2)
    } catch {
      return payload
    }
  }
  return JSON.stringify(payload, null, 2)
}

const highlightJson = (payload) => {
  const json = formatPayload(payload)

  // Tokenize and highlight JSON properly
  let result = ''
  let i = 0

  const escapeHtml = (str) => {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  }

  while (i < json.length) {
    const char = json[i]

    // String
    if (char === '"') {
      let str = '"'
      i++
      while (i < json.length && json[i] !== '"') {
        if (json[i] === '\\' && i + 1 < json.length) {
          str += json[i] + json[i + 1]
          i += 2
        } else {
          str += json[i]
          i++
        }
      }
      str += '"'
      i++
      result += `<span style="color:var(--ok-500)">${escapeHtml(str)}</span>`
    }
    // Number
    else if (char === '-' || (char >= '0' && char <= '9')) {
      let num = ''
      while (i < json.length && /[\d.eE+\-]/.test(json[i])) {
        num += json[i]
        i++
      }
      result += `<span style="color:var(--warn-400)">${num}</span>`
    }
    // true, false, null
    else if (json.slice(i, i + 4) === 'true') {
      result += '<span style="color:var(--ice-400)">true</span>'
      i += 4
    }
    else if (json.slice(i, i + 5) === 'false') {
      result += '<span style="color:var(--ice-400)">false</span>'
      i += 5
    }
    else if (json.slice(i, i + 4) === 'null') {
      result += '<span style="color:var(--ice-400)">null</span>'
      i += 4
    }
    // Braces and brackets
    else if (char === '{' || char === '}' || char === '[' || char === ']') {
      result += `<span style="color:var(--text-low)">${char}</span>`
      i++
    }
    // Colon
    else if (char === ':') {
      result += '<span style="color:var(--text-faint)">:</span>'
      i++
    }
    // Everything else (whitespace, commas)
    else {
      result += escapeHtml(char)
      i++
    }
  }

  return result
}

const copyPayload = async () => {
  if (!messageDetail.value?.payload) return

  try {
    const text = formatPayload(messageDetail.value.payload)
    await navigator.clipboard.writeText(text)
    payloadCopied.value = true
    setTimeout(() => {
      payloadCopied.value = false
    }, 2000)
  } catch (err) {
    console.error('Failed to copy:', err)
  }
}

// Register for global refresh
useRefresh(fetchMessages)

// Initialize from query params and set the default time range. This runs in
// setup, not onMounted, so the very first paint is the loading state rather
// than "No messages found" — an empty table before the first request is an
// answer we do not have yet.
if (route.query.queue) {
  filterQueue.value = route.query.queue
}
if (route.query.partition) {
  filterPartition.value = route.query.partition
}
if (route.query.status) {
  filterStatus.value = route.query.status
}
if (!route.query.from && !route.query.to) {
  setTimeRange(1)
}

refreshQueues()
fetchMessages()

// Deep link from Traces ("View Full Message"): the addressed message is not
// necessarily on the default page, so open it directly instead of hoping the
// list happens to contain it.
if (route.query.partitionId && route.query.transactionId) {
  openMessage(route.query.partitionId, route.query.transactionId)
}

// Watch for filter changes (auto-apply on queue/status change)
watch([filterQueue, filterPartition, filterStatus], () => {
  currentPage.value = 1
  fetchMessages()
})
</script>

<style scoped>
/* The drawer shell (`.drawer-panel`) and its scrim (`.modal-backdrop`) are
   shared rules in style.css now — every drawer in the app is the same 640px
   panel over the same scrim. Only the payload box below is this view's own. */

/* Payload: recessed against the drawer, hairline to close the box. */
.msg-code {
  border: 1px solid var(--bd); border-radius: var(--r-card);
  padding: 14px 16px; overflow-x: auto;
  background: var(--recessed);
}
</style>
