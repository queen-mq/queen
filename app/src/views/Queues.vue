<template>
  <div class="view-container">

    <PageHead title="Queues" :sub="headSub" :live="refreshAgo">
      <template #actions>
        <button v-if="can('queueAdmin')" class="btn btn-primary" @click="showCreate = true">Create queue</button>
      </template>
    </PageHead>

    <!-- Stale data presented as live is the failure mode: the store keeps the
         last-good rows on a failed refresh, so say how old they are. -->
    <div v-if="queuesStale" class="status-banner banner-bad view-banner">
      <span :title="formatTimestampUtc(lastFetched)">
        <strong>Could not load the queue list</strong> ·
        {{ describeApiError(queuesError) }} · showing the last rows that
        loaded ({{ lastFetchedText }}), which may no longer be true.
      </span>
    </div>
    <!-- Throughput and lag come from a second call. When it fails those columns
         render '—', and this says why — an unreachable metric must never look
         like a quiet queue. -->
    <div v-if="opsError" class="status-banner banner-warn view-banner">
      <span>
        <strong>Throughput and lag unavailable</strong> ·
        {{ describeApiError(opsError) }} — the throughput and lag columns below
        read '—' rather than zero.
      </span>
    </div>

    <!-- No range picker here on purpose: the Throughput and Lag p99 columns
         average over a 15-minute window hard-coded in `fetchQueueOps`, so a
         picker would not drive the fetch. -->
    <PageTools>
      <div class="filter-search">
        <svg class="filter-search-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
        </svg>
        <input v-model="searchQuery" type="text" placeholder="Search queues…" class="input" />
      </div>
      <!-- ALL is `null`, never '': '' is the DEFAULT namespace server-side
           (phase2.rs configured_defaults), so a '' sentinel makes the bucket
           most queues land in the one bucket you cannot select. -->
      <label class="tool-field">
        <span class="tool-label">Namespace</span>
        <select v-model="filterNamespace" class="input">
          <option :value="ALL">All</option>
          <option v-for="ns in namespaces" :key="ns" :value="ns">{{ ns || '(default)' }}</option>
        </select>
      </label>
      <label class="tool-field">
        <span class="tool-label">Task</span>
        <select v-model="filterTask" class="input">
          <option :value="ALL">All</option>
          <option v-for="task in tasks" :key="task" :value="task">{{ task || '(default)' }}</option>
        </select>
      </label>
      <template #view>
        <div class="tool-seg">
          <span class="tool-label">Sort</span>
          <div class="seg">
            <button
              v-for="opt in sortOptions"
              :key="opt.value"
              :class="{ on: sortBy === opt.value }"
              @click="sortBy = opt.value"
            >{{ opt.label }}</button>
          </div>
        </div>
      </template>
    </PageTools>

    <!-- Health grid -->
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
        <!-- "No queues" is a claim. Only make it when the list actually
             loaded — a failed fetch has to say so instead, in ember, with a
             Retry: `cannot list` must never read as `nothing here`. -->
        <div v-if="queuesError" class="empty-state empty-state-failed">
          <h3>Cannot list queues</h3>
          <p>{{ describeApiError(queuesError) }}</p>
          <button class="btn btn-ghost" @click="refreshAll">Retry</button>
        </div>
        <div v-else class="empty-state">
          <!-- Same block, same 40px glyph, as every other empty state in the
               app (Messages · DeadLetter · Traces): stacked rows for a list
               of queues. -->
          <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
            <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5M3.75 17.25h16.5" />
          </svg>
          <h3>No queues found</h3>
          <p>{{ hasActiveFilter ? 'Try adjusting your filters' : 'Create a queue to get started' }}</p>
          <!-- The same control as the header's, offered where the sentence
               above asks for it. Not under a filter, though: the queues that
               exist are simply not in this slice, and creating one more would
               not answer that. -->
          <button v-if="!hasActiveFilter && can('queueAdmin')" class="btn" @click="showCreate = true">
            Create queue
          </button>
        </div>
      </template>
    </QueueHealthGrid>

    <!-- The glyphs the rows use: shape first, colour only for the two states
         that need you. -->
    <div v-if="filteredQueues.length" class="list-legend">
      <span><i class="g ok" aria-hidden="true" />healthy</span>
      <span><i class="g idle" aria-hidden="true" />idle</span>
      <span><i class="g warn" aria-hidden="true" />behind or no reader</span>
      <span><i class="g bad" aria-hidden="true" />falling behind</span>
    </div>

    <!-- Delete confirmation modal -->
    <Teleport to="body">
      <div v-if="showDeleteModal" class="modal-backdrop" @click.self="closeDeleteModal">
        <div class="card modal-card">
          <div class="card-header"><h3>Delete queue</h3></div>
          <div class="card-body">
            <!-- The modal stays open on failure and says what happened. A closed
                 modal plus an unchanged list is indistinguishable from success. -->
            <div v-if="deleteError" class="panel-err">{{ deleteError }}</div>
            <p>
              Are you sure you want to delete <strong>{{ queueToDelete?.name }}</strong>? This will permanently remove the queue and all its messages. This action cannot be undone.
            </p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="closeDeleteModal">Cancel</button>
            <button class="btn btn-danger" :disabled="deleting" @click="deleteQueue">
              {{ deleting ? 'Deleting…' : 'Delete queue' }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- Create. The modal owns the option set and the merge rule; this page
         only says when it is open and refetches once it saved, because the row
         it added belongs to the broker's list and guessing it would drift. -->
    <QueueConfigModal
      :open="showCreate"
      mode="create"
      @close="showCreate = false"
      @saved="refreshAll"
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useRouteState } from '@/composables/useRouteState'
import { queueLocation } from '@/composables/navigation'
import { queues as queuesApi, system as systemApi, describeApiError } from '@/api'
import { formatNumber, toNum } from '@/composables/useApi'
import { queueAttention } from '@/composables/useAttention'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { useToast } from '@/composables/useToast'
import { useGroupsStore } from '@/stores/groupsStore'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import QueueConfigModal from '@/components/QueueConfigModal.vue'
import QueueHealthGrid from '@/components/QueueHealthGrid.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'

const router = useRouter()
const route = useRoute()
const { can } = useIdentity()
const { notifySuccess } = useToast()

// "All" sentinel. It must not be '' — '' is a real, selectable namespace/task
// (the server default), and collapsing the two makes it unfilterable.
const ALL = null

// Shared store — same singleton the Consumers page reads. Calling
// fetchQueues({ force: true }) on auto-refresh keeps it fresh; navigating
// to Consumers afterwards returns instantly from cache.
const queuesStore = useQueuesStore()
const {
  queues, loading, error: queuesError, isStale: queuesStale, lastFetched,
  namespaces: storeNamespaces, tasks: storeTasks,
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
const searchQuery = ref('')
const filterNamespace = ref(ALL)
const filterTask = ref(ALL)
const sortBy = ref('health')
useRouteState({ search: searchQuery, namespace: filterNamespace, task: filterTask, sort: sortBy })

// Modal state
// The create form. It is not prefilled from anything on this page: a create
// starts from the broker's defaults, which the modal states field by field.
const showCreate = ref(false)
const showDeleteModal = ref(false)
const queueToDelete = ref(null)
const deleteError = ref(null)
const deleting = ref(false)

const sortOptions = [
  { value: 'health', label: 'Worst first' },
  { value: 'avgLagMs', label: 'Lag' },
  { value: 'density', label: 'Density' },
  { value: 'partitions', label: 'Partitions' },
  { value: 'name', label: 'Name' },
]

// Re-export the store-derived lists with the names this template already
// uses, so the template doesn't need to change.
const namespaces = storeNamespaces
const tasks = storeTasks

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

const hasActiveFilter = computed(() =>
  !!searchQuery.value || filterNamespace.value !== ALL || filterTask.value !== ALL
)

// Filter (sort happens inside QueueHealthGrid)
const filteredQueues = computed(() => {
  let result = enrichedQueues.value

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase()
    result = result.filter(q => q.name.toLowerCase().includes(query))
  }
  if (filterNamespace.value !== ALL) {
    result = result.filter(q => (q.namespace || '') === filterNamespace.value)
  }
  if (filterTask.value !== ALL) {
    result = result.filter(q => (q.task || '') === filterTask.value)
  }
  return result
})

const headSub = computed(() => {
  if (!lastFetched.value) return ''
  const total = queues.value.length
  const count = `${formatNumber(total)} ${total === 1 ? 'queue' : 'queues'}`
  return hasActiveFilter.value
    ? `Showing ${formatNumber(filteredQueues.value.length)} of ${count}`
    : count
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
const groupsFailed = computed(() => groupsStore.error.value !== null)
const fetchGroups = () => groupsStore.fetchGroups({ ttlMs: 25_000 })
const attention = computed(() => {
  const groups = groupsStore.groups.value
  if (groups === null) return null
  return new Map(queueAttention(queuesStore.queues.value, groups).map((a) => [a.name, a]))
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
  router.push(queueLocation(queue.name, route))
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
/* The head and the tools row are components/PageHead and PageTools, the same
   on every page; the list is QueueHealthGrid. Nothing page-specific is left
   to lay out here. */
</style>
