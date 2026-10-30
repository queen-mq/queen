<template>
  <QueuesLayout :live="refreshAgo">
    <template #actions>
      <!-- The throughput and lag columns are queue-ops': a node that did not
           answer the broker's gather took its share out of them (usePartial). -->
      <span v-if="opsPartial" class="chip chip-warn" role="status" :title="`Throughput and lag columns. ${PARTIAL_TITLE}`"><span class="dot" />{{ opsPartial }}</span>
      <button v-if="can('queueAdmin')" class="btn btn-primary" @click="showCreate = true">Create queue</button>
    </template>
  <div class="view-container qs">

    <!-- The store keeps the last good rows on a failed refresh: say how old. -->
    <div v-if="queuesStale" class="status-banner banner-bad view-banner">
      <span :title="formatTimestampUtc(lastFetched)">
        <strong>Could not load the queue list</strong> · {{ describeApiError(queuesError) }} · showing the list from {{ lastFetchedText }}
      </span>
    </div>

    <!-- The search and the namespace are the list's beside (?find=,
         ?namespace=): the list and this table always show the same queues.
         Only the order is the table's. The throughput and lag columns cover
         the last 15 minutes, a window fixed in fetchQueueOps. -->
    <PageTools>
      <span v-if="hasActiveFilter && lastFetched" class="tool-note">{{ headSub }}</span>
      <!-- Those columns come from a second call: when it fails they read '—',
           and this says why — an unreachable metric is not a quiet queue. -->
      <span v-if="opsError" class="tool-note is-bad" :title="describeApiError(opsError)">Throughput and lag unavailable</span>
      <template #view>
        <UiSelect v-model="sortBy" label="Sort" :options="sortOptions" aria-label="Sort the queues" />
      </template>
    </PageTools>

    <QueueHealthGrid
      :queues="filteredQueues"
      :loading="loading"
      :sort-by="sortBy"
      :show-hot="false"
      :can-delete="can('queueAdmin')"
      :attention="attention"
      :attention-unknown="groupsFailed"
      @select="viewQueue"
      @delete="confirmDelete"
    >
      <template #empty>
        <!-- "No queues" is a claim: a failed read says so instead, with a Retry. -->
        <div v-if="queuesError" class="empty-state empty-state-failed">
          <h3>Cannot list queues</h3>
          <p>{{ describeApiError(queuesError) }}</p>
          <button class="btn btn-ghost" @click="refreshAll">Retry</button>
        </div>
        <div v-else class="empty-state">
          <Icon name="layers" class="empty-state-icon" :size="28" />
          <h3>{{ hasActiveFilter ? 'No queue matches' : 'No queues yet' }}</h3>
        </div>
      </template>
    </QueueHealthGrid>

    <!-- Delete confirmation. It stays open on failure and says what happened:
         a closed modal over an unchanged list reads as success. -->
    <Teleport to="body">
      <div v-if="showDeleteModal" class="modal-backdrop" @click.self="closeDeleteModal">
        <div class="card modal-card">
          <div class="card-header"><h3>Delete queue</h3></div>
          <div class="card-body qs-form">
            <div v-if="deleteError" class="panel-err">{{ deleteError }}</div>
            <p class="qs-lead">Delete <strong>{{ queueToDelete?.name }}</strong>?</p>
            <p class="qs-help">The queue and all its messages are removed. This cannot be undone.</p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="closeDeleteModal">Cancel</button>
            <button class="btn btn-danger" :disabled="deleting" @click="deleteQueue">{{ deleting ? 'Deleting…' : 'Delete queue' }}</button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- Create. The modal owns the options and the merge rule; this page only
         refetches once it saved: the new row is the broker's to list. -->
    <QueueConfigModal
      :open="showCreate"
      mode="create"
      @close="showCreate = false"
      @saved="refreshAll"
    />
  </div>
  </QueuesLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useRouteState } from '@/composables/useRouteState'
import { inNamespace, listCarry, queueMatches, queueTabLocation } from '@/composables/navigation'
import { queues as queuesApi, system as systemApi, describeApiError } from '@/api'
import { formatNumber, toNum } from '@/composables/useApi'
import { queueAttention } from '@/composables/useAttention'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { missingNodes, partialText, PARTIAL_TITLE } from '@/composables/usePartial'
import { useToast } from '@/composables/useToast'
import { useGroupsStore } from '@/stores/groupsStore'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'
import QueueConfigModal from '@/components/QueueConfigModal.vue'
import QueueHealthGrid from '@/components/QueueHealthGrid.vue'
import PageTools from '@/components/PageTools.vue'
import QueuesLayout from '@/components/QueuesLayout.vue'
import Icon from '@/components/Icon.vue'
import UiSelect from '@/components/UiSelect.vue'

const router = useRouter()
const route = useRoute()
const { can } = useIdentity()
const { notifySuccess } = useToast()

// Shared store — same singleton the Consumers page reads. Calling
// fetchQueues({ force: true }) on auto-refresh keeps it fresh; navigating
// to Consumers afterwards returns instantly from cache.
const queuesStore = useQueuesStore()
const {
  queues, loading, error: queuesError, isStale: queuesStale, lastFetched,
  fetchQueues: fetchQueuesShared, invalidate,
} = queuesStore

const lastFetchedText = computed(() =>
  lastFetched.value ? formatTimestamp(lastFetched.value) : 'an earlier load'
)

// Live tick, off the shared ticker. `lastFetched` only advances on a
// SUCCESSFUL load, so on a failed refresh this keeps counting up while the
// banner above says why — the tick must never reset itself into claiming
// freshness it does not have.
const refreshAgo = useRefreshAgo(lastFetched)

const opsByQueue = ref(new Map())
const opsError = ref(null)
// The nodes the last good queue-ops read lacked (usePartial).
const opsMissing = ref([])
const opsPartial = computed(() => (opsError.value ? '' : partialText(opsMissing.value)))
// The queue list's filters (QueueList), on the same address fields: the list
// and this table always show the same queues. null = every namespace; '' is
// the default namespace, a real one.
const searchQuery = ref('')
const filterNamespace = ref(null)
const sortBy = ref('health')
useRouteState({ find: searchQuery, namespace: filterNamespace, sort: sortBy })

// Modal state
// The create form. It is not prefilled from anything on this page: a create
// starts from the broker's defaults, which the modal states field by field.
const showCreate = ref(false)
const showDeleteModal = ref(false)
const queueToDelete = ref(null)
const deleteError = ref(null)
const deleting = ref(false)

// The column names, so a sort says which column it orders by.
const sortOptions = [
  { value: 'health', label: 'Worst first' },
  { value: 'avgLagMs', label: 'Max lag' },
  { value: 'density', label: 'Per partition' },
  { value: 'partitions', label: 'Partitions' },
  { value: 'name', label: 'Name' },
]


/**
 * Merge each queue with its latest queue-ops aggregate and derive the
 * fields the grid needs (density, throughput, lag, hotCount).
 *
 *   density   = total / partitions — lifetime messages per partition.
 *               Stays meaningful even when the queue is currently empty
 *               (so a heavily-used queue still reads "loaded").
 *   pushPerSec / popPerSec = average over the last 15m
 *   avgLagMs  = max of maxLagMs across the window (p99-like indicator)
 *   hotCount  = null until backend exposes a hot-count procedure
 */
const enrichedQueues = computed(() => {
  // opsByQueue is empty when the queue-ops call failed. Emitting null (not 0)
  // for the throughput/lag columns is what makes the grid render '—' instead
  // of painting every queue as idle.
  const opsAvailable = !opsError.value
  return queues.value.map(q => {
    const partitions = q.partitions || 0
    const total = q.messages?.total || 0
    const ops = opsByQueue.value.get(q.name)
    return {
      name: q.name,
      namespace: q.namespace,
      task: q.task,
      partitions,
      // `pending` is the broker's number verbatim: null when it did not report
      // one, so the health grid cannot mistake "unknown" for "drained".
      pending: toNum(q.messages?.pending),
      processing: q.messages?.processing || 0,
      total,
      retainedBytes: toNum(q.messages?.retainedBytes ?? q.retainedBytes),
      density: partitions > 0 ? total / partitions : 0,
      pushPerSec: opsAvailable ? (toNum(ops?.pushPerSecond) ?? 0) : null,
      popPerSec: opsAvailable ? (toNum(ops?.popPerSecond) ?? 0) : null,
      // Lag is only sampled at pop: a window with no pops has no measurement,
      // which is not the same as zero lag.
      avgLagMs: opsAvailable ? (ops?.popMessages > 0 ? toNum(ops.maxLagMs) : null) : null,
      hotCount: null,
    }
  })
})

const hasActiveFilter = computed(() => !!searchQuery.value.trim() || filterNamespace.value !== null)

// Filter (sort happens inside QueueHealthGrid)
const filteredQueues = computed(() => {
  let result = enrichedQueues.value

  return result.filter(q => inNamespace(q, filterNamespace.value) && queueMatches(q, searchQuery.value))
})

// Only while the list's search or namespace narrows the table: the header
// already counts every queue.
const headSub = computed(() => {
  const total = queues.value.length
  return `${formatNumber(filteredQueues.value.length)} of ${formatNumber(total)} ${total === 1 ? 'queue' : 'queues'}`
})

// Methods — fetchQueues is now thin shim around the shared store.
// On mount we use the cache (instant if Consumers/Dashboard already loaded
// queues); on auto-refresh we force-bust so we get fresh data.
const fetchQueues = (force = false) => fetchQueuesShared({ force })

/* Pull last 15m of queue-ops and aggregate per queue across the window.
 *
 * We can't just take the most recent 1-min bucket: queues are often bursty,
 * so a quiet bucket right after heavy activity reads as "0 msg/s" even
 * though there's real recent throughput. Instead we sum push/pop messages
 * over the full window and divide by its duration to get an average rate,
 * and take the max of maxLagMs as a p99-like lag indicator.
 *
 * Failure is surfaced, not swallowed: `opsError` blanks the throughput / lag
 * columns to '—' and paints the banner above the grid. */
const fetchQueueOps = async () => {
  try {
    const now = new Date()
    const windowMs = 15 * 60 * 1000
    const from = new Date(now.getTime() - windowMs)
    const r = await systemApi.getQueueOps({
      from: from.toISOString(),
      to: now.toISOString(),
    })
    const series = r.data?.series || []
    const windowSec = windowMs / 1000

    const agg = new Map()
    for (const row of series) {
      const name = row.queueName
      if (!name) continue
      const cur = agg.get(name) || { pushMessages: 0, popMessages: 0, maxLagMs: 0 }
      cur.pushMessages += Number(row.pushMessages) || 0
      cur.popMessages += Number(row.popMessages) || 0
      cur.maxLagMs = Math.max(cur.maxLagMs, Number(row.maxLagMs) || 0)
      agg.set(name, cur)
    }

    const result = new Map()
    for (const [name, a] of agg) {
      result.set(name, {
        pushPerSecond: a.pushMessages / windowSec,
        popPerSecond: a.popMessages / windowSec,
        popMessages: a.popMessages,
        maxLagMs: a.maxLagMs,
      })
    }
    opsByQueue.value = result
    opsMissing.value = missingNodes(r.data)
    opsError.value = null
  } catch (err) {
    // The global surface already announced it; this drives the inline state.
    opsError.value = err
  }
}

// The consumer groups, for the row verdicts: whether anyone reads a queue and
// how far behind, by the rule the Overview and the sidebar use. The shared
// listing (a full scan on the broker — stores/groupsStore), kept fresher here
// because this is the page that shows the verdicts.
const groupsStore = useGroupsStore()
const { linesFor } = useSettingsStore()
const groupsFailed = computed(() => groupsStore.error.value !== null)
const fetchGroups = () => groupsStore.fetchGroups({ ttlMs: 25_000 })
const attention = computed(() => {
  const groups = groupsStore.groups.value
  if (groups === null) return null
  return new Map(queueAttention(queuesStore.queues.value, groups, linesFor).map((a) => [a.name, a]))
})

// Auto-refresh forces fresh queues; mount-time call reuses cache.
const refreshAll = async () => {
  await Promise.all([
    fetchQueues(true),
    fetchQueueOps(),
    fetchGroups(),
  ])
}

const viewQueue = (queue) => {
  // The list's search rides along, so the list beside stays as it was.
  router.push(queueTabLocation('overview', queue.name, route, listCarry(route)))
}

const confirmDelete = (queue) => {
  queueToDelete.value = queue
  deleteError.value = null
  showDeleteModal.value = true
}

const closeDeleteModal = () => {
  showDeleteModal.value = false
  queueToDelete.value = null
  deleteError.value = null
}

const deleteQueue = async () => {
  if (!queueToDelete.value || deleting.value) return
  const name = queueToDelete.value.name
  deleting.value = true
  deleteError.value = null
  try {
    const res = await queuesApi.delete(name)
    // delete_queue_v1 answers 200 {deleted:true, existed:false} for a queue
    // that was not there (or not this tenant's). Treating that as success is
    // how a mistyped name reports "deleted".
    if (res.data && res.data.existed === false) {
      deleteError.value = `No queue named "${name}" on this cluster — nothing was deleted.`
      return
    }
    closeDeleteModal()
    notifySuccess(`Deleted queue ${name}`)
    invalidate()  // mutation happened — bust the cache so other views see it
    refreshAll()
  } catch (err) {
    deleteError.value = describeApiError(err)
  } finally {
    deleting.value = false
  }
}

useAutoRefresh(refreshAll)

// On mount, hit the cache first. If the data is fresh (e.g. user just came
// from /consumers), the queue list shows instantly with zero network.
onMounted(() => {
  fetchQueues()       // cache-respecting
  fetchQueueOps()
  fetchGroups()
})
</script>

<style scoped>
.qs { display: flex; flex-direction: column; gap: 16px; }
.qs > .page-tools, .qs > .view-banner { margin-bottom: 0; }

.qs-form { display: flex; flex-direction: column; gap: 8px; }
.qs-form .panel-err { margin: 0 0 4px; }
.qs-lead { margin: 0; font-size: 13px; color: var(--text-hi); overflow-wrap: anywhere; }
.qs-lead strong { font-weight: 600; }
.qs-help { margin: 0; font-size: 12px; color: var(--text-low); }
</style>
