<template>
  <div class="view-container">

    <!-- Toolbar: search + filters + sort -->
    <div class="qtoolbar">
      <div class="qtoolbar-search">
        <svg class="qtoolbar-search-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
        </svg>
        <input
          v-model="searchQuery"
          type="text"
          placeholder="Search queues..."
          class="input"
        />
      </div>

      <!-- ALL is `null`, never '': '' is the DEFAULT namespace server-side
           (012_configure.sql), so a '' sentinel makes the bucket most queues
           land in the one bucket you cannot select. -->
      <select v-model="filterNamespace" class="input" style="width:160px;">
        <option :value="ALL">All namespaces</option>
        <option v-for="ns in namespaces" :key="ns" :value="ns">
          {{ ns || '(default)' }}
        </option>
      </select>

      <select v-model="filterTask" class="input" style="width:160px;">
        <option :value="ALL">All tasks</option>
        <option v-for="task in tasks" :key="task" :value="task">
          {{ task || '(default)' }}
        </option>
      </select>

      <span style="flex:1;"></span>

      <span class="label-xs" style="color:var(--text-low);">Sort</span>
      <div class="seg">
        <button
          v-for="opt in sortOptions"
          :key="opt.value"
          :class="{ on: sortBy === opt.value }"
          @click="sortBy = opt.value"
        >
          {{ opt.label }}
        </button>
      </div>

      <span class="qhg-legend">
        <span class="ld" style="background:var(--ok-500);"></span> healthy
        <span class="ld" style="background:var(--ice-400);"></span> idle
        <span class="ld" style="background:var(--warn-400);"></span> elevated
        <span class="ld" style="background:var(--ember-400);"></span> falling behind
      </span>
    </div>

    <!-- Stale data presented as live is the failure mode: the store keeps the
         last-good rows on a failed refresh, so say how old they are. -->
    <div v-if="queuesStale" class="status-banner banner-bad view-banner">
      <span>
        <strong>Queue list did not refresh</strong> ·
        {{ describeApiError(queuesError) }} — what is below is from
        {{ lastFetchedText }} and may no longer be true.
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

    <!-- Health grid -->
    <QueueHealthGrid
      :queues="filteredQueues"
      :loading="loading"
      :sort-by="sortBy"
      :show-hot="false"
      :can-delete="can('queueAdmin')"
      @select="viewQueue"
      @delete="confirmDelete"
    >
      <template #empty>
        <!-- "No queues" is a claim. Only make it when the list actually
             loaded — a failed fetch has to say so instead. -->
        <template v-if="queuesError">
          <h3 style="font-size:13px; font-weight:600; color:var(--ember-400); margin:0 0 4px;">
            Cannot list queues
          </h3>
          <p style="font-size:13px; color:var(--text-mid); margin:0 0 12px;">
            {{ describeApiError(queuesError) }}
          </p>
          <button class="btn btn-ghost" @click="refreshAll">Retry</button>
        </template>
        <template v-else>
          <h3 style="font-size:13px; font-weight:600; color:var(--text-hi); margin:0 0 4px;">
            No queues found
          </h3>
          <p style="font-size:13px; color:var(--text-mid); margin:0;">
            {{ hasActiveFilter ? 'Try adjusting your filters' : 'Create a queue to get started' }}
          </p>
        </template>
      </template>
    </QueueHealthGrid>

    <!-- Delete confirmation modal -->
    <div
      v-if="showDeleteModal"
      style="position:fixed; inset:0; z-index:50; display:flex; align-items:center; justify-content:center; padding:16px; background:rgba(0,0,0,.5); backdrop-filter:blur(4px);"
      @click="showDeleteModal = false"
    >
      <div
        class="card animate-slide-up"
        style="padding:24px; width:100%; max-width:420px;"
        @click.stop
      >
        <h3 style="font-size:16px; font-weight:600; color:var(--text-hi); margin-bottom:8px;">
          Delete Queue
        </h3>
        <p style="color:var(--text-mid); margin-bottom:16px;">
          Are you sure you want to delete <strong>{{ queueToDelete?.name }}</strong>? This will permanently remove the queue and all its messages. This action cannot be undone.
        </p>
        <!-- The modal stays open on failure and says what happened. A closed
             modal plus an unchanged list is indistinguishable from success. -->
        <p v-if="deleteError" class="qdel-error">{{ deleteError }}</p>
        <div style="display:flex; align-items:center; justify-content:flex-end; gap:12px;">
          <button @click="closeDeleteModal" class="btn btn-ghost">Cancel</button>
          <button @click="deleteQueue" class="btn btn-danger" :disabled="deleting">
            {{ deleting ? 'Deleting…' : 'Delete Queue' }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { queues as queuesApi, system as systemApi, describeApiError } from '@/api'
import { toNum } from '@/composables/useApi'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import QueueHealthGrid from '@/components/QueueHealthGrid.vue'

const router = useRouter()
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
  lastFetched.value ? new Date(lastFetched.value).toLocaleTimeString() : 'an earlier load'
)

const opsByQueue = ref(new Map())
const opsError = ref(null)
const searchQuery = ref('')
const filterNamespace = ref(ALL)
const filterTask = ref(ALL)
const sortBy = ref('health')

// Modal state
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

// Auto-refresh forces fresh queues; mount-time call reuses cache.
const refreshAll = async () => {
  await Promise.all([fetchQueues(true), fetchQueueOps()])
}

const viewQueue = (queue) => {
  router.push(`/queues/${queue.name}`)
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
})
</script>

<style scoped>
.qtoolbar {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding: 10px 14px;
  margin-bottom: 12px;
  background: var(--ink-2);
  border: 1px solid var(--bd);
  border-radius: var(--r-card);
}

.qtoolbar-search {
  position: relative;
  flex: 1;
  min-width: 220px;
  max-width: 360px;
}
.qtoolbar-search .input { padding-left: 32px; width: 100%; }
.qtoolbar-search-icon {
  position: absolute;
  left: 10px;
  top: 50%;
  transform: translateY(-50%);
  width: 14px;
  height: 14px;
  color: var(--text-low);
  pointer-events: none;
}

.qhg-legend {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  color: var(--text-mid);
  margin-left: 4px;
}
.qhg-legend .ld {
  width: 7px;
  height: 7px;
  border-radius: var(--r-pill);
}

/* Inline banner used inside a view (the shell's own strip is edge-to-edge). */
.view-banner {
  border: 1px solid currentColor;
  border-radius: var(--r-card);
  margin-bottom: 12px;
}

.qdel-error {
  margin: 0 0 16px;
  padding: 8px 10px;
  border-radius: var(--r-control);
  font-size: 12.5px;
  color: var(--ember-400);
  border: 1px solid var(--ember-400);
  background: color-mix(in srgb, var(--ember-500) 8%, transparent);
}

@media (max-width: 880px) {
  .qhg-legend { display: none; }
}
</style>
