<template>
  <div class="view-container">

    <!-- Summary. Every figure here is computed from the DLQ page below — the
         cluster-wide lifetime counter in /api/v1/status is a different fact
         (and the proxy blocks that route for tenants anyway). -->
    <div class="grid-3" style="margin-bottom:8px;">
      <div class="stat">
        <div class="stat-label">Dead-lettered messages</div>
        <div class="stat-value" style="color:var(--ember-400);">{{ known ? formatNumber(messages.length) : '—' }}</div>
        <div class="stat-foot">
          <span v-if="known">on this page · the broker reports no DLQ total</span>
          <span v-else>unknown — the list did not load</span>
        </div>
      </div>
      <div class="stat">
        <div class="stat-label">Affected partitions</div>
        <div class="stat-value">{{ known ? formatNumber(affectedPartitions) : '—' }}</div>
        <div class="stat-foot">
          <span v-if="known">among the messages on this page</span>
          <span v-else>unknown — the list did not load</span>
        </div>
      </div>
      <div class="stat">
        <div class="stat-label">Most frequent error</div>
        <div style="margin-top:8px; font-size:13px; font-family:'JetBrains Mono',monospace; color:var(--ember-400); word-break:break-all; line-height:1.5;">
          {{ known ? (topError || 'No error text recorded') : '—' }}
        </div>
      </div>
    </div>

    <p style="margin-bottom:24px; font-size:11px; color:var(--text-low); font-family:'JetBrains Mono',monospace;" :title="scopeTitle">{{ scopeLabel }}</p>

    <!-- The list failed: say so instead of drawing an empty, healthy-looking page. -->
    <div v-if="error" class="status-banner banner-bad" style="border-radius:var(--r-card); margin-bottom:16px;">
      <span class="pulse-ember" style="width:7px; height:7px; flex-shrink:0;" />
      <span>
        <strong>Could not load the dead-letter queue</strong> · {{ describeApiError(error) }}<template v-if="messages.length">
          · showing the last rows that loaded{{ lastUpdatedText ? ` (${lastUpdatedText})` : '' }}</template>
      </span>
    </div>

    <!-- Top errors breakdown -->
    <div v-if="errorHistogram.length" class="card" style="margin-bottom:24px;">
      <div class="card-header">
        <h3>Errors on this page</h3>
        <span class="muted">{{ errorHistogram.length }} distinct</span>
      </div>
      <div class="card-body" style="display:flex; flex-direction:column; gap:12px;">
        <div v-for="entry in errorHistogram" :key="entry.error" style="display:flex; align-items:center; gap:12px;">
          <span class="font-mono" style="font-size:24px; font-weight:300; color:var(--ember-400); min-width:48px; text-align:right;">{{ entry.count }}</span>
          <div style="flex:1; min-width:0;">
            <div style="font-size:13px; font-family:'JetBrains Mono',monospace; color:var(--text-hi); overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{{ entry.error }}</div>
          </div>
          <div class="bar" style="width:120px; display:block;">
            <i style="background:linear-gradient(90deg, var(--ember-500), var(--ember-600));" :style="{ width: (entry.count / maxErrorCount * 100) + '%' }" />
          </div>
        </div>
      </div>
    </div>

    <!-- Filters. Applied server-side (the endpoint takes queue + consumerGroup),
         so they select from the whole DLQ, not from the loaded page. -->
    <div style="display:flex; align-items:center; gap:12px; margin-bottom:16px; flex-wrap:wrap;">
      <div>
        <label class="label-xs" style="display:block; margin-bottom:6px;">Queue</label>
        <select v-model="filterQueue" class="input" style="width:180px;">
          <option value="">All Queues</option>
          <option v-for="q in queueOptions" :key="q" :value="q">{{ q }}</option>
        </select>
      </div>
      <div>
        <label class="label-xs" style="display:block; margin-bottom:6px;">Consumer group</label>
        <input
          v-model="filterGroup"
          class="input"
          style="width:200px;"
          list="dlq-group-options"
          placeholder="All groups"
          @change="reload"
          @keyup.enter="reload"
        />
        <datalist id="dlq-group-options">
          <option v-for="g in groupSuggestions" :key="g" :value="g" />
        </datalist>
      </div>
      <div>
        <label class="label-xs" style="display:block; margin-bottom:6px;">Page size</label>
        <select v-model.number="pageSize" class="input" style="width:130px;">
          <option :value="50">50</option>
          <option :value="100">100</option>
          <option :value="200">200</option>
        </select>
      </div>
      <div style="margin-left:auto; align-self:flex-end;">
        <span style="font-size:12px; color:var(--text-low); font-family:'JetBrains Mono',monospace;">
          {{ messages.length }} loaded · page {{ page }}
        </span>
      </div>
    </div>

    <!-- Messages table -->
    <div class="card">
      <div v-if="loading && !messages.length" style="padding:24px;">
        <div v-for="i in 6" :key="i" class="skeleton" style="height:44px; margin-bottom:8px; border-radius:var(--r-card);" />
      </div>

      <table v-else-if="messages.length" class="t">
        <thead>
          <tr>
            <th>Message</th>
            <th>Queue</th>
            <th>Consumer</th>
            <th>Error</th>
            <th>Retries</th>
            <th>Failed</th>
            <th v-if="canAdmin" style="text-align:right;">Actions</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="msg in messages"
            :key="msgKey(msg)"
            style="cursor:pointer;"
            :style="selectedMsg && msgKey(selectedMsg) === msgKey(msg) ? 'background:var(--warn-glow)' : ''"
            @click="selectMessage(msg)"
          >
            <td>
              <div style="display:flex; align-items:center; gap:6px;">
                <span class="pulse-ember" style="width:5px; height:5px;" />
                <span class="font-mono" style="font-size:12px;">{{ (msg.transactionId || msg.id || '-').slice(0, 14) }}…</span>
              </div>
            </td>
            <td style="font-weight:500;">{{ msg.queue || '-' }}</td>
            <td class="font-mono" style="font-size:12px; color:var(--text-mid);">{{ msg.consumerGroup || '-' }}</td>
            <td>
              <span class="font-mono" style="font-size:12px; color:var(--ember-400);">{{ truncateError(msg.errorMessage) }}</span>
            </td>
            <td class="font-mono" style="font-size:12px;">{{ msg.retryCount ?? '—' }}</td>
            <td class="font-mono" style="font-size:12px; color:var(--text-mid);">{{ formatRelativeTime(msg.failedAt) }}</td>
            <td v-if="canAdmin" style="text-align:right; white-space:nowrap;" @click.stop>
              <button class="btn btn-danger" style="padding:4px 10px; font-size:11px;" @click="purge(msg)" :disabled="isDeleting(msg)">
                {{ isDeleting(msg) ? '…' : 'Purge' }}
              </button>
              <!-- A purge the broker refused must stay on screen: the row is
                   still here, and so is the reason. -->
              <div v-if="rowError(msg)" style="font-size:11px; color:var(--ember-400); margin-top:4px; max-width:260px; white-space:normal;">
                {{ rowError(msg) }}
              </div>
            </td>
          </tr>
        </tbody>
      </table>

      <div v-else-if="error" style="padding:48px; text-align:center;">
        <p style="font-size:14px; font-weight:500; color:var(--ember-400);">{{ describeApiError(error) }}</p>
        <p style="font-size:13px; margin-top:4px; color:var(--text-low);">
          Nothing loaded — this is a failure, not an empty dead-letter queue.
        </p>
      </div>

      <div v-else style="padding:48px; text-align:center; color:var(--text-low);">
        <svg style="width:40px; height:40px; margin:0 auto 12px; opacity:.4;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.2"><path d="M5 7h14l-1.2 11.2a2 2 0 01-2 1.8H8.2a2 2 0 01-2-1.8L5 7Z"/><path d="M9 4h6v3H9z"/></svg>
        <p style="font-size:14px; font-weight:500;">
          {{ page > 1 ? 'No more dead-lettered messages' : 'Dead letter queue is empty' }}
        </p>
        <p style="font-size:13px; margin-top:4px;">
          {{ hasFilters ? 'No failed messages match these filters.' : 'No failed messages to review.' }}
        </p>
      </div>

      <div style="padding:12px 16px; border-top:1px solid var(--bd); display:flex; align-items:center; justify-content:space-between;">
        <span style="font-size:12px; color:var(--text-low);">
          <template v-if="serverPaginates">Page {{ page }}</template>
          <template v-else>This broker returns the whole dead-letter queue at once — paging is disabled</template>
        </span>
        <div style="display:flex; gap:8px;">
          <button class="btn btn-ghost" style="font-size:12px;" :disabled="page === 1" @click="prevPage">Previous</button>
          <button class="btn btn-ghost" style="font-size:12px;" :disabled="!canPageForward" @click="nextPage">Next</button>
        </div>
      </div>
    </div>

    <!-- Detail panel -->
    <Teleport to="body">
      <div v-if="selectedMsg" class="dlq-panel-backdrop" @click="closeDetail" />
      <div v-if="selectedMsg" class="dlq-panel">
        <div style="padding:20px 24px;">
          <!-- Header -->
          <div style="display:flex; align-items:flex-start; justify-content:space-between; margin-bottom:20px; padding-bottom:16px; border-bottom:1px solid var(--bd);">
            <div style="flex:1; min-width:0;">
              <h3 style="font-size:15px; font-weight:700; margin-bottom:4px; color:var(--text-hi);">DLQ Message Detail</h3>
              <p class="font-mono" style="font-size:11px; color:var(--text-low); word-break:break-all;">
                {{ selectedMsg.transactionId || selectedMsg.id }}
              </p>
            </div>
            <button @click="closeDetail" class="btn btn-ghost btn-icon">
              <svg style="width:18px; height:18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
            </button>
          </div>

          <!-- Status chip -->
          <div style="margin-bottom:20px;">
            <span class="chip chip-bad" style="font-size:12px;">dead_letter</span>
            <span v-if="selectedMsg.retryCount" class="chip chip-warn" style="font-size:12px; margin-left:6px;">
              {{ selectedMsg.retryCount }} retries
            </span>
          </div>

          <!-- Key-value fields -->
          <div class="dlq-kv">
            <div class="dlq-kv-row">
              <label class="label-xs">Queue</label>
              <span style="font-weight:500; color:var(--text-hi);">{{ selectedMsg.queue }}</span>
            </div>
            <div class="dlq-kv-row">
              <label class="label-xs">Partition</label>
              <span class="font-mono" style="font-size:12px; color:var(--text-mid); word-break:break-all;">{{ selectedMsg.partition }}</span>
            </div>
            <div class="dlq-kv-row">
              <label class="label-xs">Partition ID</label>
              <span class="font-mono" style="font-size:12px; color:var(--text-mid); word-break:break-all;">{{ selectedMsg.partitionId }}</span>
            </div>
            <div class="dlq-kv-row">
              <label class="label-xs">Transaction ID</label>
              <span class="font-mono" style="font-size:12px; color:var(--text-mid); word-break:break-all;">{{ selectedMsg.transactionId }}</span>
            </div>
            <div class="dlq-kv-row">
              <label class="label-xs">Consumer group</label>
              <span class="font-mono" style="font-size:12px; color:var(--ice-400);">{{ selectedMsg.consumerGroup }}</span>
            </div>
            <!-- The log engine cannot recover the enqueue time from an opaque
                 blob, so it echoes failed_at as createdAt. Showing the same
                 instant twice under two labels invents a fact. -->
            <div v-if="hasDistinctCreatedAt(selectedMsg)" class="dlq-kv-row">
              <label class="label-xs">Created</label>
              <span style="font-size:13px; color:var(--text-mid);">{{ formatDateTime(selectedMsg.createdAt) }}</span>
            </div>
            <div class="dlq-kv-row">
              <label class="label-xs">Failed at</label>
              <span style="font-size:13px; color:var(--text-mid);">{{ formatDateTime(selectedMsg.failedAt) }}</span>
              <span v-if="!hasDistinctCreatedAt(selectedMsg)" style="font-size:11px; color:var(--text-low);">
                Enqueue time is not recorded for this entry.
              </span>
            </div>
            <div v-if="selectedMsg.errorMessage" class="dlq-kv-row">
              <label class="label-xs">Error</label>
              <span class="font-mono" style="font-size:12px; color:var(--ember-400); word-break:break-all;">{{ selectedMsg.errorMessage }}</span>
            </div>
          </div>

          <!-- Payload -->
          <div v-if="selectedMsg.data" style="margin-top:24px;">
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:8px;">
              <label class="label-xs">Payload</label>
              <button class="btn btn-ghost" style="padding:2px 8px; font-size:11px;" @click="copyPayload">
                {{ copied ? 'Copied!' : (encryptedPayload ? 'Copy envelope' : 'Copy') }}
              </button>
            </div>
            <!-- The DLQ read path does no decryption: what follows is the stored
                 envelope, not the message. Saying "payload" over ciphertext is
                 how a debugger loses an hour. -->
            <div v-if="encryptedPayload" class="status-banner banner-warn" style="border-radius:var(--r-card); margin-bottom:8px;">
              <span>
                <strong>Encrypted envelope</strong> · this queue encrypts payloads and the DLQ endpoint
                returns them as stored. This is ciphertext, not the message body.
              </span>
            </div>
            <pre class="dlq-code">{{ JSON.stringify(selectedMsg.data, null, 2) }}</pre>
          </div>

          <!-- Actions. No "replay": the broker exposes no re-push route for a
               DLQ snapshot, and a button that cannot verify its own outcome is
               worse than no button. -->
          <div v-if="canAdmin" style="display:flex; flex-direction:column; gap:8px; margin-top:24px; padding-top:16px; border-top:1px solid var(--bd);">
            <button class="btn btn-danger" style="width:100%; justify-content:center;" @click="purge(selectedMsg)" :disabled="isDeleting(selectedMsg)">
              {{ isDeleting(selectedMsg) ? 'Purging…' : 'Purge message' }}
            </button>
            <p v-if="rowError(selectedMsg)" style="font-size:12px; color:var(--ember-400);">{{ rowError(selectedMsg) }}</p>
          </div>
          <p v-else style="margin-top:24px; padding-top:16px; border-top:1px solid var(--bd); font-size:12px; color:var(--text-low);">
            Purging needs the admin role on this cluster.
          </p>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { dlq, queues as queuesApi, describeApiError } from '@/api'
import { useApi, formatNumber, formatDateTime, formatRelativeTime } from '@/composables/useApi'
import { useRefresh } from '@/composables/useRefresh'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'

const { can, actingTenantSlug, actingClusterSlug } = useIdentity()
const { notifySuccess, notifyError } = useToast()

const selectedKey = ref(null)
const copied = ref(false)
const filterQueue = ref('')
const filterGroup = ref('')
const page = ref(1)
const pageSize = ref(100)
// Flipped off when a response carries more rows than it was asked for: that
// broker is not applying the limit, so offering "Next" would page nothing.
const serverPaginates = ref(true)
// Per-row transient state, keyed rather than stamped onto the row objects,
// which are replaced wholesale on every refresh.
const deleting = ref(new Set())
const rowErrors = ref(new Map())
// Rows this session verified as purged (success:true). Cleared on every reload,
// so a row that comes back is a row the broker still has.
const purged = ref(new Set())

const msgKey = (msg) => msg.transactionId || msg.id

// The list, its loading/error state and its "as of when" come from one place.
// useApi also aborts on unmount and drops any response belonging to a cluster
// we have since left — the previous tenant's DLQ must never land here.
const {
  data: listData,
  loading,
  error,
  lastUpdated,
  execute: executeList,
} = useApi((params, config) => dlq.list(params, config), {
  immediate: false,
  onSuccess: (payload) => {
    const rows = extractRows(payload)
    serverPaginates.value = rows.length <= pageSize.value
    purged.value = new Set()
    rowErrors.value = new Map()
    // A detail panel over a message that is no longer listed is a stale fact.
    if (selectedKey.value && !rows.some(m => msgKey(m) === selectedKey.value)) {
      selectedKey.value = null
    }
  },
})

const { data: queuesData, refresh: refreshQueues } = useApi(
  (config) => queuesApi.list(undefined, config),
  { immediate: false },
)

const extractRows = (payload) =>
  Array.isArray(payload?.messages) ? payload.messages : (Array.isArray(payload) ? payload : [])

const messages = computed(
  () => extractRows(listData.value).filter(m => !purged.value.has(msgKey(m)))
)
const queueOptions = computed(
  () => (queuesData.value?.queues || []).map(q => q.name).filter(Boolean).sort()
)
const selectedMsg = computed(
  () => messages.value.find(m => msgKey(m) === selectedKey.value) || null
)

const isDeleting = (msg) => deleting.value.has(msgKey(msg))
const rowError = (msg) => rowErrors.value.get(msgKey(msg)) || null

const canAdmin = computed(() => can('queueAdmin'))

const scopeLabel = computed(
  () => `Tenant-scoped · tenant ${actingTenantSlug.value || '—'} · cluster ${actingClusterSlug.value || '—'}`
)
const scopeTitle = 'These are the acting tenant\'s dead-lettered messages on the acting cluster, not the cell total.'

/** False when a failed load left us with nothing: then every figure is unknown, not zero. */
const known = computed(() => !(error.value && messages.value.length === 0))

const affectedPartitions = computed(
  () => new Set(messages.value.map(m => m.partitionId).filter(Boolean)).size
)

const errorHistogram = computed(() => {
  const counts = new Map()
  for (const m of messages.value) {
    const key = m.errorMessage || '(no error text recorded)'
    counts.set(key, (counts.get(key) || 0) + 1)
  }
  return [...counts.entries()]
    .map(([text, count]) => ({ error: text, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8)
})
const topError = computed(() => errorHistogram.value[0]?.error || null)
const maxErrorCount = computed(() => Math.max(...errorHistogram.value.map(e => e.count), 1))

const groupSuggestions = computed(
  () => [...new Set(messages.value.map(m => m.consumerGroup).filter(Boolean))].sort()
)

const hasFilters = computed(() => Boolean(filterQueue.value || filterGroup.value))
const canPageForward = computed(
  () => serverPaginates.value && messages.value.length >= pageSize.value
)
const lastUpdatedText = computed(() =>
  lastUpdated.value ? formatRelativeTime(lastUpdated.value) : null
)

const encryptedPayload = computed(() => isEncryptedEnvelope(selectedMsg.value?.data))

/** The shape server/src/encryption.rs writes; the DLQ read path never unwraps it. */
const isEncryptedEnvelope = (data) =>
  Boolean(data) && typeof data === 'object' &&
  typeof data.encrypted === 'string' &&
  typeof data.iv === 'string' &&
  typeof data.authTag === 'string'

const hasDistinctCreatedAt = (msg) =>
  Boolean(msg.createdAt) && msg.createdAt !== msg.failedAt

const truncateError = (err) => {
  if (!err || err === '-') return '-'
  return err.length > 50 ? err.slice(0, 50) + '…' : err
}

const selectMessage = (msg) => {
  selectedKey.value = selectedKey.value === msgKey(msg) ? null : msgKey(msg)
  copied.value = false
}

const closeDetail = () => { selectedKey.value = null }

const copyPayload = async () => {
  if (!selectedMsg.value?.data) return
  try {
    await navigator.clipboard.writeText(JSON.stringify(selectedMsg.value.data, null, 2))
    copied.value = true
    setTimeout(() => { copied.value = false }, 2000)
  } catch {
    notifyError('Could not copy to the clipboard', 'Copy failed')
  }
}

// A failed load keeps whatever loaded last; the banner above says it is stale.
// The failure itself is already on the global surface (axios interceptor).
const fetchMessages = () => {
  const params = { limit: pageSize.value, offset: (page.value - 1) * pageSize.value }
  if (filterQueue.value) params.queue = filterQueue.value
  if (filterGroup.value) params.consumerGroup = filterGroup.value
  return executeList(params).catch(() => {})
}

const reload = () => {
  page.value = 1
  fetchMessages()
}

const prevPage = () => {
  if (page.value === 1) return
  page.value--
  fetchMessages()
}

const nextPage = () => {
  if (!canPageForward.value) return
  page.value++
  fetchMessages()
}

const purge = async (msg) => {
  if (!canAdmin.value) return
  const key = msgKey(msg)
  if (!confirm(`Purge message ${String(key).slice(0, 16)}…?\n\nThis cannot be undone.`)) return
  deleting.value.add(key)
  rowErrors.value.delete(key)
  try {
    const res = await dlq.delete(msg.partitionId, msg.transactionId)
    // The broker answers 200 {success:false} both when nothing matched and when
    // the ownership gate refused the partition. Dropping the row on that reports
    // a purge that never happened — and the row returns on the next poll.
    if (res.data?.success !== true) {
      const reason = res.data?.message || 'The broker did not purge this message.'
      rowErrors.value.set(key, reason)
      notifyError(reason, 'Purge failed')
      return
    }
    notifySuccess(`Purged ${key}`)
    if (selectedKey.value === key) selectedKey.value = null
    purged.value.add(key)
  } catch (err) {
    rowErrors.value.set(key, describeApiError(err))
  } finally {
    deleting.value.delete(key)
  }
}

// One shared ticker for the whole app (paused while the tab is hidden) instead
// of a private setInterval: under the proxy every poll is metered.
useRefresh(fetchMessages, { auto: true })

watch([filterQueue, pageSize], reload)

// In setup, not onMounted: the first paint must be the loading state, not the
// "dead letter queue is empty" state we have not asked about yet.
refreshQueues()
fetchMessages()
</script>

<style scoped>
.dlq-panel-backdrop {
  position: fixed; inset: 0; z-index: 49;
  background: color-mix(in srgb, var(--ink-0) 40%, transparent);
  backdrop-filter: blur(4px);
}

/* The drawer floats over the page and sits at card level. Its old gradient was
   picked to sit BELOW the old, lighter page; on this one it reads as raised. */
.dlq-panel {
  position: fixed; top: 0; right: 0; bottom: 0;
  width: 100%; max-width: 560px; z-index: 50;
  overflow-y: auto;
  border-left: 1px solid var(--bd);
  background: color-mix(in srgb, var(--card) 97%, transparent);
  backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
}

.dlq-kv { display: flex; flex-direction: column; gap: 16px; }
.dlq-kv-row { display: flex; flex-direction: column; gap: 4px; }

/* Stored envelope / payload: recessed against the drawer. */
.dlq-code {
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px; line-height: 1.6;
  padding: 14px 16px; border-radius: var(--r-card);
  border: 1px solid var(--bd);
  color: var(--text-mid);
  background: var(--recessed);
  white-space: pre; overflow-x: auto;
  max-height: 400px; overflow-y: auto;
}
</style>
