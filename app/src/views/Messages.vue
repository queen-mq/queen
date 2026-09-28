<template>
  <div class="view-container">

    <PageHead title="Messages">
      <template #range>
        <div class="seg" role="group" aria-label="Created in">
          <button
            v-for="r in RANGE_PRESETS"
            :key="r.value"
            :class="{ on: rangePreset === r.value }"
            :aria-pressed="rangePreset === r.value ? 'true' : 'false'"
            @click="pickRange(r)"
          >{{ r.value }}</button>
          <button :class="{ on: rangePreset === 'custom' }" @click="rangePreset = 'custom'">Custom</button>
        </div>
      </template>
      <!-- `can('produce')` mirrors the proxy's RouteClass::Produce for
           /api/v1/push, so the button is absent rather than enabled-and-403. -->
      <template v-if="canProduce" #actions>
        <button class="btn" @click="openPush">Push message</button>
      </template>
    </PageHead>

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

    <PageTools>
      <!-- Client-side only: this narrows the rows already on the page, it is
           not a server-side transaction lookup, and the placeholder says so. -->
      <div class="filter-search" title="Filters the rows already on this page. It is not a server-side transaction lookup.">
        <svg class="filter-search-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
        </svg>
        <input v-model="searchQuery" type="text" placeholder="Search loaded rows" class="input" />
      </div>
      <!-- Free entry allowed: `queue` is a server-side parameter here, and a
           deep link can arrive with a queue this cluster's list does not
           carry. -->
      <label class="tool-field" for="msg-queue-filter">
        <span class="tool-label">Queue</span>
        <Autocomplete
          id="msg-queue-filter"
          v-model="filterQueue"
          :options="queueNames"
          :loading="queuesLoading"
          label="Queue"
          placeholder="All"
          allow-custom
        />
      </label>
      <label class="tool-field">
        <span class="tool-label">Partition</span>
        <input v-model="filterPartition" type="text" placeholder="All" class="input" />
      </label>
      <label class="tool-field">
        <span class="tool-label">Status</span>
        <select v-model="filterStatus" class="input">
          <option value="">All</option>
          <option value="pending">Pending</option>
          <option value="processing">Processing</option>
          <option value="completed">Completed</option>
          <option value="dead_letter">Dead letter</option>
        </select>
      </label>
      <button v-if="hasActiveFilters" class="btn btn-ghost" @click="clearFilters">Clear</button>
      <template #view>
        <label class="tool-field">
          <span class="tool-label">Show</span>
          <select v-model="limit" class="input" @change="applyFilters">
            <option :value="50">50</option>
            <option :value="100">100</option>
            <option :value="200">200</option>
            <option :value="500">500</option>
          </select>
        </label>
      </template>
    </PageTools>

    <!-- A window of your own: the one filter that waits for Apply, because
         a half-typed date must not re-scope the list. -->
    <div v-if="rangePreset === 'custom'" class="page-tools">
      <label class="tool-field">
        <span class="tool-label">From</span>
        <input v-model="filterFrom" type="datetime-local" class="input" :title="formatTimestampUtc(filterFrom)" />
      </label>
      <label class="tool-field">
        <span class="tool-label">To</span>
        <input v-model="filterTo" type="datetime-local" class="input" :title="formatTimestampUtc(filterTo)" />
      </label>
      <button class="btn btn-primary" @click="applyFilters">Apply</button>
    </div>

    <!-- Bus mode. Driven by what the rows actually report as well as by the
         top-level mode: a log-engine queue reports its groups per message. -->
    <!-- Messages list -->
    <div class="card">
      <div class="card-header">
        <h3>Messages</h3>
        <span class="card-sub">{{ formatNumber(messages.length) }} loaded · page {{ currentPage }}<template v-if="busGroups > 0"> · bus mode, {{ busGroups }} consumer {{ busGroups === 1 ? 'group' : 'groups' }}</template></span>
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
                <td :title="formatTimestampUtc(message.createdAt)" class="tabular-nums" style="text-align:right; font-size:12px; color:var(--text-mid); white-space:nowrap;">
                  {{ formatTimestamp(message.createdAt) }}
                </td>
                <td style="text-align:right; white-space:nowrap;">
                  <!-- One line: the status as a word (coloured only when it is a
                       failure), and how many groups have it, beside it. -->
                  <span
                    class="msg-status"
                    :class="{ 'is-bad': message.status === 'dead_letter' || message.status === 'failed', 'is-live': message.status === 'pending' || message.status === 'processing' }"
                  >{{ statusLabel(message.status) }}</span>
                  <span v-if="message.busStatus && message.busStatus.totalGroups > 0" class="msg-groups">
                    · {{ message.busStatus.consumedBy }}/{{ message.busStatus.totalGroups }} groups
                  </span>
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
        <span class="pager-count">Page <span class="tabular-nums">{{ currentPage }}</span></span>
        <div class="pager-nav">
          <button class="btn btn-ghost" :disabled="currentPage === 1" @click="prevPage">Previous</button>
          <button class="btn btn-ghost" :disabled="!canPageForward" @click="nextPage">Next</button>
        </div>
      </div>
    </div>

    <DetailDrawer
      :open="Boolean(selectedMessage)"
      title="Message"
      :subtitle="messageDetail?.transactionId || selectedMessage?.transactionId || ''"
      wide
      :split="Boolean(messageDetail)"
      @close="closePanel"
    >
      <div v-if="detailLoading" style="text-align:center; padding:48px 0;">
        <div class="spinner" style="margin:0 auto 12px;"></div>
        <p style="color:var(--text-low);">Loading details...</p>
      </div>

      <div v-else-if="detailError" class="panel-err">
        {{ detailError }}
      </div>

      <template v-else-if="messageDetail">
          <!-- Status and routing match the DLQ drawer: the same facts occupy
               the same positions and identifiers are directly copyable. -->
          <!-- The status as a word, coloured only when it is a failure; the
               retry count is a count beside it. -->
          <div class="detail-status-row">
            <span
              class="msg-status detail-status"
              :class="{ 'is-bad': messageDetail.status === 'dead_letter' || messageDetail.status === 'failed', 'is-live': messageDetail.status === 'pending' || messageDetail.status === 'processing' }"
            >{{ statusLabel(messageDetail.status) }}</span>
            <span v-if="messageDetail.retryCount" class="detail-note">· {{ messageDetail.retryCount }} {{ messageDetail.retryCount === 1 ? 'retry' : 'retries' }}</span>
          </div>

          <div class="detail-fields">
            <DetailField label="Queue" :value="messageDetail.queue" tone="high" />
            <DetailField label="Partition" :value="messageDetail.partition" mono copyable />
            <DetailField label="Partition ID" :value="messageDetail.partitionId" mono copyable />
            <DetailField label="Transaction ID" :value="messageDetail.transactionId" mono copyable />
            <DetailField
              label="Created"
              :value="formatTimestamp(messageDetail.createdAt)"
              :title="formatTimestampUtc(messageDetail.createdAt)"
            />
            <DetailField v-if="messageDetail.traceId" label="Trace ID" :value="messageDetail.traceId" mono copyable />
          </div>

          <DetailField
            v-if="messageDetail.errorMessage"
            class="detail-section"
            label="Error"
            :value="messageDetail.errorMessage"
            mono
            copyable
            boxed
            tone="high"
          />

          <div v-if="messageDetail.queueConfig" class="detail-section">
            <h4 class="detail-section-title">Queue configuration</h4>
            <div class="card detail-config-grid">
              <div><span>Lease time</span><strong>{{ messageDetail.queueConfig.leaseTime }}s</strong></div>
              <div><span>TTL</span><strong>{{ messageDetail.queueConfig.ttl }}s</strong></div>
              <div><span>Retry limit</span><strong>{{ messageDetail.queueConfig.retryLimit }}</strong></div>
              <div><span>Retry delay</span><strong>{{ messageDetail.queueConfig.retryDelay }}ms</strong></div>
            </div>
          </div>

          <!-- Consumer Groups -->
          <div v-if="messageDetail.consumerGroups && messageDetail.consumerGroups.length > 0" class="detail-section">
            <h4 class="detail-section-title">Consumer groups</h4>
            <ul class="detail-list">
              <li v-for="group in messageDetail.consumerGroups" :key="group.name">
                <span>{{ group.name === '__QUEUE_MODE__' ? 'Queue mode' : group.name }}</span>
                <span :class="group.consumed ? 'detail-note' : 'msg-status is-live'">{{ group.consumed ? 'Consumed' : 'Pending' }}</span>
              </li>
            </ul>
          </div>

          <!-- Actions -->
          <div class="detail-actions">
            <!-- Sits with the button that produced it: this drawer scrolls, and
                 a delete failure hoisted to the top would land off screen. -->
            <div v-if="actionError" class="panel-err">{{ actionError }}</div>

            <p v-if="messageDetail.status === 'completed'" class="detail-note">
              Consumed and acknowledged.
            </p>

            <!-- Only dead-lettered messages are deletable: a live payload lives
                 in an immutable log segment, and the broker answers a delete on
                 one with success:false. Retry / move-to-DLQ are not offered at
                 all — the broker has no route for either. -->
            <button
              v-if="isDeletable && canAdmin"
              @click="deleteMessage"
              :disabled="actionLoading"
              class="btn btn-danger"
            >
              {{ actionLoading ? 'Purging…' : 'Purge dead-letter entry' }}
            </button>

            <p v-else-if="isDeletable" style="font-size:12px; color:var(--text-low);">
              Purging a dead-letter entry needs the admin role on this cluster.
            </p>

            <!-- What this drawer CANNOT do, and it has to stay true with "Push
                 a copy" right underneath it: that button opens a form whose
                 queue field is editable, so "re-routed from here" read as a
                 flat contradiction of the control below it. A copy is not a
                 re-route — the original is not moved, not re-delivered and not
                 removed, wherever the copy is pushed — and saying so is what
                 keeps the paragraph honest in both directions. -->
            <p v-else style="font-size:12px; color:var(--text-low);">
              Live messages are stored in immutable log segments: this one cannot be deleted or
              retried from here, and nothing here moves it — a copy pushed to this queue or another
              is a NEW message and leaves this one exactly where it is. Only dead-lettered entries
              can be purged.
            </p>

            <!-- A COPY, and the word retry is never used for it. The broker has
                 no retry for a live message and the one it has for a dead
                 letter is a different route with different hazards; this button
                 opens the push form on this message's queue, partition and
                 payload with an EMPTY transaction id, which is a new message at
                 the tail of the partition. Needs the payload: a message whose
                 covering segment retention removed has nothing to copy. -->
            <template v-if="payloadAvailable && canProduce">
              <p v-if="payloadIsEnvelope" style="font-size:12px; color:var(--text-low);">
                This broker cannot decrypt this message — what it returned is the stored
                <span class="font-mono">{encrypted, iv, authTag}</span> envelope, not the payload.
                A copy would push the envelope itself, so it is not offered here. Push it from a
                broker that carries the encryption key.
              </p>
              <template v-else>
                <button class="btn" @click="openPushCopy">
                  Push a copy
                </button>
                <p style="font-size:12px; color:var(--text-low);">
                  Push a copy: a new transaction id, appended at the tail, not deduplicated against
                  the original.
                </p>
                <!-- On a dead letter the word "replay" is the one an operator
                     expects, and this button is not it: the DLQ row stays, and
                     a second click pushes a second copy. The Dead Letter page
                     has the move primitive that does this exactly once. -->
                <p v-if="isDeletable" style="font-size:12px; color:var(--text-low);">
                  This is a copy, not a replay: the dead-letter entry stays where it is, and a
                  second click pushes a second copy. Use Replay on the Dead Letter page to move
                  the row instead.
                </p>
              </template>
            </template>
          </div>
      </template>

      <template #secondary>
        <div v-if="messageDetail">
          <div class="detail-section-header">
            <label class="label-xs">Payload</label>
            <button
              v-if="payloadAvailable"
              class="btn btn-ghost detail-copy-button"
              @click="copyPayload"
            >
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
          <JsonViewer v-else :value="messageDetail.payload" />
        </div>
      </template>
    </DetailDrawer>

    <!-- One component behind both entry points. `messages-link` is off here:
         this IS the Messages list, and `pushed` puts the queue in the filter
         and refetches, which is the same jump without a navigation that would
         not re-read its own query. -->
    <PushMessageModal
      :open="pushOpen"
      :queue="pushSeed.queue"
      :partition="pushSeed.partition"
      :payload="pushSeed.payload"
      :transaction-id="pushSeed.transactionId"
      :copy="pushSeed.copy"
      :messages-link="false"
      @close="pushOpen = false"
      @pushed="onPushed"
    />
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { useRoute } from 'vue-router'
import { messages as messagesApi, queues as queuesApi, describeApiError } from '@/api'
import { useApi, formatNumber, formatRelativeTime } from '@/composables/useApi'
import { filtersForPushedMessage, isEncryptedEnvelope } from '@/composables/usePushVerdict'
import { formatDateTimeLocal, formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import Autocomplete from '@/components/Autocomplete.vue'
import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import PushMessageModal from '@/components/PushMessageModal.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'

const route = useRoute()
const { can } = useIdentity()
const { notifySuccess, notifyError } = useToast()

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

// What the push form opens on. Held as one object rather than four refs so the
// header's "push into the filtered queue" and the drawer's "copy of this
// message" are two seeds of the same shape, and neither can leak a field of
// the other into the next opening.
const pushOpen = ref(false)
const pushSeed = ref({ queue: '', partition: '', payload: undefined, transactionId: '', copy: false })

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

const {
  data: queuesData,
  loading: queuesLoading,
  refresh: refreshQueues,
} = useApi((config) => queuesApi.list(undefined, config), { immediate: false })

const messages = computed(() => listData.value?.messages || [])
const queues = computed(() => queuesData.value?.queues || [])
const queueNames = computed(() => queues.value.map(q => q.name).filter(Boolean).sort())
const queueMode = computed(() => listData.value?.mode || null)

// Permissions come from the identity store only — never from whether a call 403'd.
const canAdmin = computed(() => can('queueAdmin'))
// POST /api/v1/push is RouteClass::Produce at the proxy: admin or producer.
const canProduce = computed(() => can('produce'))

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

// The broker decrypts a stored envelope before answering — but only with the
// right key configured (encryption.rs `decrypt_payload_bytes`), and hands the
// raw {encrypted,iv,authTag} object over when it has none. Copying THAT would
// push the envelope as a plaintext payload, which on an encrypted queue is
// re-wrapped into an envelope of an envelope. So the copy is refused here, in
// the only place that can tell the difference.
const payloadIsEnvelope = computed(() => isEncryptedEnvelope(messageDetail.value?.payload))

// 047 emits id/transactionId as NULL for log entries and the broker backfills
// them from the frame — which it cannot do once retention removed the segment.
// Such a row is not addressable: no key, no detail fetch.
const isAddressable = (m) => Boolean(m?.partitionId && m?.transactionId)
const rowKey = (m, idx) => m.transactionId || m.txnHash || `${m.partitionId || 'na'}:${idx}`
const headKey = (rows) => (rows.length ? rowKey(rows[0], 0) : null)

// Helper to convert datetime-local to ISO string
const toISOString = (dateTimeLocal) => {
  if (!dateTimeLocal) return ''
  return new Date(dateTimeLocal).toISOString()
}

// The window, as a range like every other page's: a preset applies at once;
// Custom opens the two dates and waits for Apply.
const RANGE_PRESETS = [
  { value: '1h', hours: 1 },
  { value: '24h', hours: 24 },
  { value: '7d', hours: 168 },
]
const rangePreset = ref(route.query.from || route.query.to ? 'custom' : '1h')
const pickRange = (preset) => {
  rangePreset.value = preset.value
  setTimeRange(preset.hours)
  applyFilters()
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
  rangePreset.value = '1h'
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

// ---------------------------------------------------------------------------
// Push
// ---------------------------------------------------------------------------

/** Header button: the queue the list is filtered to, or none if it shows all. */
const openPush = () => {
  pushSeed.value = {
    queue: filterQueue.value,
    partition: '',
    payload: undefined,
    transactionId: '',
    copy: false,
  }
  pushOpen.value = true
}

/**
 * Drawer button: this message's queue, partition and payload — and an EMPTY
 * transaction id, which is what makes it a copy rather than a retry. Reusing
 * the original's id would answer `duplicate` inside the dedup window and write
 * nothing, i.e. a button that looks like it acted and did not.
 */
const openPushCopy = () => {
  const detail = messageDetail.value
  if (!detail) return
  pushSeed.value = {
    queue: detail.queue || '',
    partition: detail.partition || '',
    payload: detail.payload,
    transactionId: '',
    copy: true,
  }
  pushOpen.value = true
}

const onPushed = ({ queue, partition }) => {
  // Land on rows that CAN contain what was just pushed: a refresh that answers
  // a successful push with a table unable to show it reads as a push that did
  // nothing. Which filters would hide the new row is the rule, and it lives in
  // the composable so test/push.test.js holds it — the time window above all,
  // which is pinned at page load and goes stale in under two minutes.
  const next = filtersForPushedMessage(
    {
      to: filterTo.value,
      status: filterStatus.value,
      queue: filterQueue.value,
      partition: filterPartition.value,
    },
    { queue, partition },
    formatDateTimeLocal(new Date()),
  )

  // The three filters below are watched, and their watcher already resets the
  // page and refetches; running applyFilters() as well would fetch twice.
  const watched =
    next.status !== filterStatus.value ||
    next.queue !== filterQueue.value ||
    next.partition !== filterPartition.value

  filterTo.value = next.to
  filterStatus.value = next.status
  filterQueue.value = next.queue
  filterPartition.value = next.partition

  // The row sorts to the head of page 1 (ORDER BY created_at DESC), so a push
  // made from page 3 must not refresh page 3.
  if (!watched) applyFilters()
}

const formatPayload = (payload) => {
  if (payload === null || payload === undefined) return 'null'
  if (typeof payload === 'string') {
    try {
      return JSON.stringify(JSON.parse(payload), null, 2)
    } catch {
      return payload
    }
  }
  return JSON.stringify(payload, null, 2)
}

const copyPayload = async () => {
  if (!messageDetail.value) return

  try {
    const text = formatPayload(messageDetail.value.payload)
    await navigator.clipboard.writeText(text)
    payloadCopied.value = true
    setTimeout(() => {
      payloadCopied.value = false
    }, 2000)
  } catch {
    notifyError('Could not copy to the clipboard', 'Copy failed')
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

// Status words as a person reads them; the wire value stays in the drawer.
const statusLabel = (st) => ({ pending: 'Pending', processing: 'Processing', completed: 'Completed', dead_letter: 'Dead letter', failed: 'Failed' }[st] || st)
</script>

<style scoped>
.msg-status { font-size: 12px; color: var(--text-low); }
.msg-status.is-live { color: var(--text-hi); }
.msg-status.is-bad { color: var(--ember-400); }
.msg-groups { font-size: 12px; color: var(--text-low); }
</style>
