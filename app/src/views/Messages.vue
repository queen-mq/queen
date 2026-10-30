<template>
  <!-- A tab of the Queues section: the queue is picked in the list beside
       (?queue=), every queue when none is. -->
  <QueuesLayout>
    <!-- `can('produce')` mirrors the proxy's RouteClass::Produce for
         /api/v1/push, so the button is absent rather than enabled-and-403. -->
    <template v-if="canProduce" #actions>
      <button class="btn btn-primary" @click="openPush">Push message</button>
    </template>
  <div class="view-container msg">

    <!-- The list failed: the table below is stale (or empty), and says so. -->
    <div v-if="listError" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not load messages</strong> · {{ describeApiError(listError) }}<template v-if="messages.length"> · showing the last rows{{ lastUpdatedText ? ` (${lastUpdatedText})` : '' }}</template>
      </span>
    </div>

    <!-- The broker answered page N with page N-1's rows: paging further would
         renumber the same messages. -->
    <div v-if="paginationStalled" class="status-banner banner-warn view-banner">
      <span><strong>Paging is not advancing</strong> · this broker returned the same rows for page {{ currentPage }}. Narrow the window or the filters.</span>
    </div>

    <PageTools>
      <!-- Client-side: it narrows the rows already on this page; it is not a
           server-side transaction lookup. -->
      <input
        v-model="searchQuery"
        type="search"
        class="input msg-search"
        placeholder="Search this page"
        aria-label="Search the loaded rows by transaction or partition id"
        title="Narrows the rows already loaded on this page. It is not a server-side lookup."
      />
      <label class="tool-field">
        <span class="tool-label">Partition</span>
        <input v-model="filterPartition" type="text" placeholder="All" class="input msg-partition-input" />
      </label>
      <UiSelect v-model="filterStatus" label="Status" :options="STATUS_OPTIONS" />
      <button v-if="hasActiveFilters" class="btn btn-ghost" @click="clearFilters">Clear</button>
      <template #view>
        <div class="seg" role="group" aria-label="Created in">
          <button
            v-for="r in RANGE_PRESETS"
            :key="r.value"
            :class="{ on: rangePreset === r.value }"
            :aria-pressed="rangePreset === r.value ? 'true' : 'false'"
            @click="pickRange(r)"
          >{{ r.value }}</button>
          <button :class="{ on: rangePreset === 'custom' }" :aria-pressed="rangePreset === 'custom' ? 'true' : 'false'" @click="rangePreset = 'custom'">Custom</button>
        </div>
        <UiSelect v-model="limit" label="Show" :options="LIMITS" @change="applyFilters" />
      </template>
    </PageTools>

    <!-- A window of your own: the one filter that waits for Apply, because a
         half-typed date must not re-scope the list. -->
    <div v-if="rangePreset === 'custom'" class="page-tools msg-custom">
      <label class="tool-field">
        <span class="tool-label">From</span>
        <input v-model="filterFrom" type="datetime-local" step="any" class="input" :title="formatTimestampUtc(filterFrom)" />
      </label>
      <label class="tool-field">
        <span class="tool-label">To</span>
        <input v-model="filterTo" type="datetime-local" step="any" class="input" :title="formatTimestampUtc(filterTo)" />
      </label>
      <button class="btn" @click="applyFilters">Apply</button>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Messages</h3>
        <span class="card-sub tabular-nums">{{ formatNumber(messages.length) }} on page {{ currentPage }}<template v-if="busGroups > 0"> · read by {{ busGroups }} consumer {{ busGroups === 1 ? 'group' : 'groups' }}</template></span>
        <span class="muted">{{ stamp(listPanel) }}</span>
      </div>

      <div class="msg-scroll">
        <table class="t msg-table">
          <thead>
            <tr>
              <th v-if="!filterQueue">Queue</th>
              <th>Partition</th>
              <th>Transaction ID</th>
              <!-- GET /api/v1/messages always answers newest first; it takes
                   no sort field or direction. -->
              <th class="r" aria-sort="descending" title="Newest first">Created</th>
              <th class="r">Status</th>
            </tr>
          </thead>
          <tbody>
            <template v-if="loading && !messages.length">
              <tr v-for="i in 8" :key="i" aria-hidden="true">
                <td v-if="!filterQueue"><div class="skeleton" style="height:14px; width:96px;" /></td>
                <td><div class="skeleton" style="height:14px; width:96px;" /></td>
                <td><div class="skeleton" style="height:14px; width:200px;" /></td>
                <td><div class="skeleton" style="height:14px; width:112px; margin-left:auto;" /></td>
                <td><div class="skeleton" style="height:14px; width:80px; margin-left:auto;" /></td>
              </tr>
            </template>
            <template v-else-if="filteredMessages.length > 0">
              <tr
                v-for="(message, idx) in filteredMessages"
                :key="rowKey(message, idx)"
                :class="isAddressable(message) ? 'msg-row' : 'msg-row-dead'"
                :tabindex="isAddressable(message) ? 0 : undefined"
                @click="isAddressable(message) && selectMessage(message)"
                @keydown.enter.prevent="isAddressable(message) && selectMessage(message)"
              >
                <td v-if="!filterQueue"><span class="msg-cut msg-queue" :title="message.queue">{{ message.queue }}</span></td>
                <td><span class="msg-cut msg-partition" :title="message.partitionId ? `${message.partition} · partition ID ${message.partitionId}` : message.partition">{{ message.partition }}</span></td>
                <td>
                  <!-- The txn id lives inside the segment blob: once retention
                       drops the segment the broker cannot resolve it. -->
                  <span v-if="isAddressable(message)" class="msg-cut msg-txn" :title="message.transactionId">{{ message.transactionId }}</span>
                  <span
                    v-else
                    class="msg-expired"
                    title="Retention removed the covering log segment, so the broker can no longer resolve this message's transaction id or payload."
                  >payload expired</span>
                </td>
                <td class="r msg-when" :title="formatTimestampUtc(message.createdAt)">{{ formatTimestampCompact(message.createdAt) }}</td>
                <td class="r msg-state">
                  <MessageStatus :status="message.status" />
                  <span v-if="message.busStatus && message.busStatus.totalGroups > 0" class="msg-groups" title="Consumer groups that have consumed it">· {{ message.busStatus.consumedBy }}/{{ message.busStatus.totalGroups }}</span>
                </td>
              </tr>
            </template>
            <!-- Never an idle empty state on a failure. -->
            <tr v-else-if="listError">
              <td :colspan="columns">
                <div class="empty-state empty-state-failed">
                  <h3>Cannot load messages</h3>
                  <button class="btn btn-ghost" @click="fetchMessages">Retry</button>
                </div>
              </td>
            </tr>
            <tr v-else>
              <td :colspan="columns">
                <div class="empty-state">
                  <Icon name="message-square-text" class="empty-state-icon" :size="28" />
                  <h3 v-if="searchQuery && messages.length">No row on this page matches</h3>
                  <h3 v-else>{{ hasActiveFilters ? 'No message matches' : 'No messages in this window' }}</h3>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Not drawn under an empty first page or a failure: Previous and Next
           there offer travel that goes nowhere. -->
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
      <div v-if="detailLoading" class="msg-loading">
        <div class="spinner" />
        <p>Loading the message…</p>
      </div>

      <div v-else-if="detailError" class="panel-err">{{ detailError }}</div>

      <template v-else-if="messageDetail">
        <!-- Status and routing match the dead-letter drawer: the same facts in
             the same places, identifiers copyable. -->
        <div class="detail-status-row">
          <MessageStatus :status="messageDetail.status" class="detail-status" />
          <span v-if="messageDetail.retryCount" class="detail-note">· {{ messageDetail.retryCount }} {{ messageDetail.retryCount === 1 ? 'retry' : 'retries' }}</span>
        </div>

        <div class="detail-fields">
          <div class="msg-links">
            <RouterLink v-if="messageDetail.queue" class="btn btn-ghost" :to="queueLocation(messageDetail.queue, route)">Open queue</RouterLink>
            <RouterLink class="btn btn-ghost" :to="{ path: '/traces', query: { ...contextQuery(route), queue: messageDetail.queue, partitionId: selectedMessage.partitionId, transactionId: selectedMessage.transactionId } }">Traces</RouterLink>
          </div>
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

        <div v-if="messageDetail.consumerGroups && messageDetail.consumerGroups.length > 0" class="detail-section">
          <h4 class="detail-section-title">Consumer groups</h4>
          <ul class="detail-list">
            <li v-for="group in messageDetail.consumerGroups" :key="group.name">
              <span>{{ group.name === '__QUEUE_MODE__' ? 'Queue mode' : group.name }}</span>
              <span :class="group.consumed ? 'detail-note' : 'msg-pending'">{{ group.consumed ? 'Consumed' : 'Pending' }}</span>
            </li>
          </ul>
        </div>

        <div class="detail-actions">
          <!-- With the button that produced it: this drawer scrolls, and a
               failure hoisted to the top would land off screen. -->
          <div v-if="actionError" class="panel-err">{{ actionError }}</div>

          <p v-if="messageDetail.status === 'completed'" class="detail-note">Consumed and acknowledged.</p>

          <!-- Only dead-lettered messages are deletable: a live payload lives in
               an immutable log segment, and the broker answers a delete on one
               with success:false. Retry and move-to-DLQ have no route at all. -->
          <button
            v-if="isDeletable && canAdmin"
            class="btn btn-danger"
            :disabled="actionLoading"
            @click="purgeAsk = true"
          >{{ actionLoading ? 'Purging…' : 'Purge dead-letter entry' }}</button>
          <p v-else-if="isDeletable" class="detail-note">Purging needs the admin role on this cluster.</p>
          <p
            v-else
            class="detail-note"
            title="Live messages are stored in immutable log segments. A copy pushed to this queue or another is a new message and leaves this one where it is. Only dead-lettered entries can be purged."
          >A live message cannot be deleted or retried from here.</p>

          <!-- A COPY, never called a retry: the push form opens on this
               message's queue, partition and payload with an EMPTY transaction
               id, which is a new message at the tail of the partition. -->
          <template v-if="payloadAvailable && canProduce">
            <p
              v-if="payloadIsEnvelope"
              class="detail-note"
              title="This broker returned the stored {encrypted, iv, authTag} envelope, not the payload: a copy would push the envelope itself. Push it from a broker that carries the encryption key."
            >Encrypted, and this broker cannot decrypt it: no copy.</p>
            <template v-else>
              <button class="btn" @click="openPushCopy">Push a copy</button>
              <p
                class="detail-note"
                :title="isDeletable ? 'The dead-letter entry stays where it is, and a second click pushes a second copy. Replay on the Dead letter tab moves the row instead.' : 'A new transaction id, appended at the tail, not deduplicated against the original.'"
              >{{ isDeletable ? 'A copy, not a replay: the dead-letter entry stays.' : 'A new message, appended at the tail.' }}</p>
            </template>
          </template>
        </div>
      </template>

      <template #secondary>
        <div v-if="messageDetail">
          <div class="detail-section-header">
            <label class="label-xs">Payload</label>
            <div class="payload-tools">
              <!-- Only for a record the Kafka facade wrote: its key, value and
                   headers are stored base64, which reads as noise. -->
              <div v-if="kafkaRecord" class="seg payload-view" role="group" aria-label="Payload view">
                <button
                  :class="{ on: showDecoded }"
                  :aria-pressed="showDecoded ? 'true' : 'false'"
                  @click="payloadView = 'decoded'"
                >Decoded</button>
                <button
                  :class="{ on: !showDecoded }"
                  :aria-pressed="showDecoded ? 'false' : 'true'"
                  @click="payloadView = 'raw'"
                >Raw</button>
              </div>
              <button
                v-if="payloadAvailable"
                class="btn btn-ghost detail-copy-button"
                @click="copyPayload"
              >{{ payloadCopied ? 'Copied' : 'Copy' }}</button>
            </div>
          </div>
          <!-- payloadAvailable:false means the segment is gone: an empty box
               would read as "this message carried nothing". -->
          <p v-if="!payloadAvailable" class="msg-quiet">Payload unavailable: retention removed its log segment.</p>
          <KafkaRecordView v-else-if="showDecoded" :record="kafkaRecord" />
          <JsonViewer v-else :value="messageDetail.payload" />
        </div>
      </template>
    </DetailDrawer>

    <!-- The purge's confirm, over the drawer that asks for it. -->
    <Teleport to="body">
      <div v-if="purgeAsk && messageDetail" class="modal-backdrop msg-over" @click.self="purgeAsk = false">
        <div class="card modal-card">
          <div class="card-header">
            <h3>Purge dead-letter entry</h3>
            <span class="card-sub font-mono msg-cut msg-purge-id">{{ messageDetail.transactionId }}</span>
          </div>
          <div class="card-body msg-form">
            <p class="msg-lead">Purge this dead letter of <strong>{{ messageDetail.queue }}</strong>?</p>
            <p class="msg-help">This cannot be undone.</p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="purgeAsk = false">Cancel</button>
            <button class="btn btn-danger" @click="purgeAsk = false; deleteMessage()">Purge</button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- One component behind both entry points. `messages-link` is off here:
         this IS the Messages list, and `pushed` puts the queue in the filter
         and refetches. -->
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
  </QueuesLayout>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useRouteState, replaceQueryFields } from '@/composables/useRouteState'
import { contextQuery, queueLocation } from '@/composables/navigation'
import { DEFAULT_TIME_RANGE, EXTENDED_TIME_RANGES as RANGE_PRESETS, relativeTimeRange } from '../../../shared/timeRanges.mjs'
import { messages as messagesApi, describeApiError } from '@/api'
import { useApi, formatNumber, formatRelativeTime } from '@/composables/useApi'
import { filtersForPushedMessage, isEncryptedEnvelope } from '@/composables/usePushVerdict'
import { formatDateTimeLocal, formatTimestamp, formatTimestampCompact, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import KafkaRecordView from '@/components/KafkaRecordView.vue'
import { kafkaRecordOf, kafkaRecordPlain } from '@/composables/kafkaRecord'
import PushMessageModal from '@/components/PushMessageModal.vue'
import QueuesLayout from '@/components/QueuesLayout.vue'
import PageTools from '@/components/PageTools.vue'
import MessageStatus from '@/components/MessageStatus.vue'
import Icon from '@/components/Icon.vue'
import UiSelect from '@/components/UiSelect.vue'

const route = useRoute()
const router = useRouter()
const { can } = useIdentity()
const { notifySuccess, notifyError } = useToast()

const STATUS_OPTIONS = [
  { value: '', label: 'All' },
  { value: 'pending', label: 'Pending' },
  { value: 'processing', label: 'Processing' },
  { value: 'completed', label: 'Completed' },
  { value: 'dead_letter', label: 'Dead letter' },
]
const LIMITS = [25, 50, 100, 200, 500]

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
const appliedFrom = ref('')
const appliedTo = ref('')
const limit = ref(25)
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


const messages = computed(() => listData.value?.messages || [])
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

// The Queue column names what the list beside has not picked.
const columns = computed(() => (filterQueue.value ? 4 : 5))

const hasActiveFilters = computed(() => {
  // The queue is the section's pick (the list beside), not a filter to clear.
  return searchQuery.value || filterPartition.value || filterStatus.value
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

// A record the Kafka facade wrote stores its key, value and headers base64
// (protocols/queen-kafka/src/records.rs): shown decoded unless the reader asks
// for the stored form. The choice holds from one message to the next.
const kafkaRecord = computed(() =>
  payloadAvailable.value ? kafkaRecordOf(messageDetail.value?.payload) : null
)
const payloadView = ref('decoded')
const showDecoded = computed(() => Boolean(kafkaRecord.value) && payloadView.value === 'decoded')

// 047 emits id/transactionId as NULL for log entries and the broker backfills
// them from the frame — which it cannot do once retention removed the segment.
// Such a row is not addressable: no key, no detail fetch.
const isAddressable = (m) => Boolean(m?.partitionId && m?.transactionId)
const rowKey = (m, idx) => m.transactionId || m.txnHash || `${m.partitionId || 'na'}:${idx}`
const headKey = (rows) => (rows.length ? rowKey(rows[0], 0) : null)

// Helper to convert datetime-local to ISO string
const toISOString = (dateTimeLocal) => {
  if (!dateTimeLocal || !Number.isFinite(Date.parse(dateTimeLocal))) return ''
  return new Date(dateTimeLocal).toISOString()
}

// The window, as a range like every other page's: a preset applies at once;
// Custom opens the two dates and waits for Apply.
const incomingPreset = () => route.query.from || route.query.to ? 'custom'
  : !route.query.range ? DEFAULT_TIME_RANGE : RANGE_PRESETS.some(r => r.value === route.query.range) ? route.query.range : 'custom'
const rangePreset = ref(incomingPreset())
const pickRange = (preset) => {
  rangePreset.value = preset.value
  setTimeRange(preset.value)
  applyFilters()
}

// Set time range preset
const setTimeRange = (value) => {
  const { from, to } = relativeTimeRange(value)

  filterFrom.value = formatDateTimeLocal(from, true)
  filterTo.value = formatDateTimeLocal(to, true)
  appliedFrom.value = from.toISOString()
  appliedTo.value = to.toISOString()
}

// Clear all filters
const clearFilters = () => {
  searchQuery.value = ''
  filterPartition.value = ''
  filterStatus.value = ''

  // Reset to default last 1 hour
  rangePreset.value = DEFAULT_TIME_RANGE
  setTimeRange(DEFAULT_TIME_RANGE)

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
  if (appliedFrom.value) params.from = appliedFrom.value
  if (appliedTo.value) params.to = appliedTo.value
  return params
}

// A failed load keeps the rows that are already on screen — the banner says
// they are stale. The failure itself is already on the global surface.
const fetchMessages = () => {
  requestedPage = currentPage.value
  return executeList(buildParams()).catch(() => {})
}

const applyFilters = () => {
  appliedFrom.value = toISOString(filterFrom.value)
  appliedTo.value = toISOString(filterTo.value)
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
  replaceQueryFields(router, route, { partitionId: null, transactionId: null })
  selectedMessage.value = null
  actionError.value = null
  purgeAsk.value = false
}

const openMessage = async (partitionId, transactionId) => {
  replaceQueryFields(router, route, { partitionId, transactionId })
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

// Asked in the page's own modal, not the browser's.
const purgeAsk = ref(false)

const deleteMessage = async () => {
  const detail = messageDetail.value
  if (!detail || !canAdmin.value) return

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
  appliedTo.value = toISOString(next.to)
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
    // What is on screen: the decoded record, or the payload as stored.
    const text = showDecoded.value
      ? JSON.stringify(kafkaRecordPlain(kafkaRecord.value), null, 2)
      : formatPayload(messageDetail.value.payload)
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

// Query state is read before the first request, and again for same-page links.
const dateField = field => computed({
  get: () => field.value,
  set: value => { field.value = toISOString(value) },
})
const { restoring: restoringRoute } = useRouteState({
  queue: filterQueue, partition: filterPartition, status: filterStatus,
  from: dateField(appliedFrom), to: dateField(appliedTo), limit, page: currentPage, search: searchQuery,
})
const restoreDateInputs = () => {
  filterFrom.value = appliedFrom.value ? formatDateTimeLocal(new Date(appliedFrom.value), true) : ''
  filterTo.value = appliedTo.value ? formatDateTimeLocal(new Date(appliedTo.value), true) : ''
}
restoreDateInputs()
if (!appliedFrom.value && !appliedTo.value) setTimeRange(route.query.range || DEFAULT_TIME_RANGE)
watch(() => route.query.range, () => {
  if (!route.query.from && !route.query.to) {
    rangePreset.value = incomingPreset()
    setTimeRange(route.query.range || DEFAULT_TIME_RANGE)
    fetchMessages()
  }
})
watch([filterQueue, filterPartition, filterStatus, currentPage, appliedFrom, appliedTo, limit], (values, previous) => {
  if (restoringRoute.value) fetchMessages()
  else if (values.slice(0, 3).some((value, i) => value !== previous[i])) {
    currentPage.value = 1
    fetchMessages()
  }
})
watch([appliedFrom, appliedTo], () => {
  if (restoringRoute.value) {
    restoreDateInputs()
    rangePreset.value = incomingPreset()
  }
})
watch(() => [route.query.partitionId, route.query.transactionId], ([partitionId, transactionId]) => {
  if (typeof partitionId === 'string' && typeof transactionId === 'string') {
    if (selectedMessage.value?.partitionId !== partitionId || selectedMessage.value?.transactionId !== transactionId) openMessage(partitionId, transactionId)
  } else selectedMessage.value = null
}, { immediate: true })
fetchMessages()

</script>

<style scoped>
.msg { display: flex; flex-direction: column; gap: 16px; }
.msg > .page-tools, .msg > .view-banner { margin-bottom: 0; }
.msg-search { flex: 0 1 220px; min-width: 160px; height: 32px; }
.msg-partition-input { width: 120px; }
.msg-custom { margin-top: -8px; }

.msg-scroll { overflow-x: auto; }
.msg-table { min-width: 640px; }
.msg-table th, .msg-table td { white-space: nowrap; }
.msg-table td { height: 40px; font-size: 13px; }
.msg-table .r { text-align: right; }
.msg-row { cursor: pointer; }
.msg-row:focus-visible { outline-offset: -2px; }
.msg-row-dead { cursor: default; }
/* Long names and ids end in an ellipsis; the whole of each is its tooltip. */
.msg-cut { display: inline-block; max-width: 240px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: bottom; }
.msg-queue { font-weight: 500; color: var(--text-hi); }
.msg-partition { color: var(--text-mid); }
.msg-txn { max-width: 340px; font-family: var(--font-mono); font-size: 12px; color: var(--text-hi); }
.msg-expired { font-size: 12px; color: var(--text-low); }
.msg-when { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); font-variant-numeric: tabular-nums; }
.msg-state { font-size: 12px; }
.msg-groups { margin-left: 4px; font-size: 12px; color: var(--text-low); font-variant-numeric: tabular-nums; }

/* --- The drawer ------------------------------------------------------------ */
.msg-loading { padding: 48px 0; text-align: center; font-size: 13px; color: var(--text-low); }
.msg-loading .spinner { margin: 0 auto 12px; }
.msg-links { display: flex; flex-wrap: wrap; gap: 4px; margin: -4px 0 0 -10px; }
.msg-pending { font-size: 12px; color: var(--text-hi); }
.msg-quiet { margin: 0; padding: 12px 0; font-size: 13px; color: var(--text-mid); }
.payload-tools { display: flex; align-items: center; gap: 8px; }

/* The purge confirm opens from the drawer (z 51, scrim 50). */
.msg-over { z-index: 55; }
.msg-purge-id { max-width: 240px; }
.msg-form { display: flex; flex-direction: column; gap: 8px; }
.msg-lead { margin: 0; font-size: 13px; color: var(--text-hi); overflow-wrap: anywhere; }
.msg-lead strong { font-weight: 600; }
.msg-help { margin: 0; font-size: 12px; color: var(--text-low); }
.payload-view button { padding: 2px 8px; font-size: 11px; }

@media (max-width: 640px) {
  .msg-search { flex: 1 1 100%; }
  .msg-custom .tool-field { flex: 1 1 100%; }
  .msg-custom .tool-field .input { flex: 1; }
}
</style>
