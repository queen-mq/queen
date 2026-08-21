<template>
  <div class="view-container">

    <!-- Tenant scope. Consumer groups, lag and cursors below belong to the
         acting tenant on this cluster; the cell is named so neither is read
         as the other. -->
    <div class="scope-strip">
      <span class="chip chip-mute">tenant scope</span>
      <span class="scope-text">
        <strong>{{ actingTenantSlug || 'no tenant' }}</strong>
        <span class="scope-sep">/</span>{{ actingClusterSlug || 'no cluster' }}
        <span class="scope-sep">·</span>cell {{ actingCellSlug || 'unknown' }}
      </span>
      <span class="scope-fill"></span>
      <span v-if="!canAdmin" class="scope-meta">read-only role — cursor and delete actions hidden</span>
    </div>

    <!-- A failed list must not look like an empty one: say the grid below is
         stale (or absent), and how stale. Page-level fact, so it is a page
         banner — the same container Queues uses for the same event, not a
         loose .panel-err, which belongs inside the panel it describes. -->
    <div v-if="groups.failed.value" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not load consumer groups</strong> · {{ groupsErrorText }}<template v-if="consumers.length"> · showing the last rows that loaded{{ groups.lastUpdated.value ? ` (${groups.lastUpdated.value.toLocaleTimeString()})` : '' }}</template>
      </span>
    </div>

    <!--
      ========================================================================
      Filter card — the app-wide control container: a card with no header,
      one row of fields, sort last and right-aligned. This page owns no time
      window, so the row's first field is the search box, not a range picker.
      ========================================================================
    -->
    <div class="card filters">
      <div class="card-body filter-rows">
        <div class="filter-row">
          <div class="filter-search">
            <svg class="filter-search-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
            </svg>
            <input
              v-model="searchQuery"
              type="text"
              placeholder="Search consumer groups..."
              class="input"
            />
          </div>

          <div class="filter-field-col">
            <label class="label-xs">Namespace</label>
            <select v-model="filterNamespace" class="input">
              <option value="">All namespaces</option>
              <option v-for="ns in namespaces" :key="ns" :value="ns">{{ ns || '(empty)' }}</option>
            </select>
          </div>

          <div class="filter-field-col">
            <label class="label-xs">Task</label>
            <select v-model="filterTask" class="input">
              <option value="">All tasks</option>
              <option v-for="t in tasks" :key="t" :value="t">{{ t || '(empty)' }}</option>
            </select>
          </div>

          <!-- No free entry: this filter is applied to the loaded rows, so a
               name outside the list can only produce an empty table. -->
          <div class="filter-field-col">
            <label class="label-xs" for="cg-queue-filter">Queue</label>
            <Autocomplete
              id="cg-queue-filter"
              v-model="filterQueue"
              :options="scopedQueueNames"
              :loading="groupsFirstLoad"
              label="Queue"
              placeholder="All queues"
            />
          </div>

          <label class="filter-check">
            <input v-model="showLaggingOnly" type="checkbox" />
            <span>Lagging only</span>
          </label>

          <div class="filter-field">
            <span class="label-xs">Sort</span>
            <div class="seg">
              <button
                v-for="opt in sortOptions"
                :key="opt.value"
                :class="{ on: sortBy === opt.value }"
                @click="sortBy = opt.value"
              >{{ opt.label }}</button>
            </div>
          </div>

          <span class="qhg-legend">
            <span class="ld" style="background:var(--ok-500);"></span> stable
            <span class="ld" style="background:var(--warn-400);"></span> lagging
            <span class="ld" style="background:var(--ember-400);"></span> stuck
            <span class="ld" style="background:var(--bd-hi);"></span> dead
          </span>
        </div>
      </div>
    </div>

    <!--
      ========================================================================
      Lagging Partitions — placed at the top because it's actionable: the
      operator wants to know "is anything stuck right now?" before they scan
      the per-group list. The badge has THREE states, never two: a count, a
      clean bill of health, and "unavailable" — a failed fetch must never
      render as "none above the threshold".
      ========================================================================
    -->
    <div class="lp-card" :class="{ 'lp-card-warn': laggingPartitions.length > 0 }">
      <button
        class="lp-head"
        :class="{ 'lp-head-open': showLaggingSection }"
        @click="showLaggingSection = !showLaggingSection"
      >
        <svg class="lp-head-chevron" width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M6 4l4 4-4 4" />
        </svg>
        <span class="lp-head-title">Lagging partitions</span>

        <!-- One loading treatment, not four: the Refresh button says it is
             working, the body shows a table-shaped skeleton on first paint,
             and the badge keeps stating the last answer it actually has. -->
        <span
          v-if="lagging.failed.value"
          class="lp-head-badge lp-head-badge-bad"
          :title="laggingErrorText"
        >unavailable</span>
        <span
          v-else-if="laggingLoaded && laggingPartitions.length > 0"
          class="lp-head-badge lp-head-badge-warn"
        >
          {{ laggingPartitions.length }}
          {{ laggingPartitions.length === 1 ? 'partition' : 'partitions' }} above {{ getLagLabel(lagThreshold) }}
        </span>
        <span
          v-else-if="laggingLoaded"
          class="lp-head-badge lp-head-badge-ok"
        >
          none above {{ getLagLabel(lagThreshold) }}
        </span>
        <span v-else class="lp-head-hint">expand to inspect partitions individually</span>

        <span style="flex:1;"></span>

        <span v-if="showLaggingSection" class="lp-head-meta" @click.stop>
          <span class="label-xs">Threshold</span>
          <div class="seg">
            <button
              v-for="preset in lagPresets"
              :key="preset.value"
              :class="{ on: lagThreshold === preset.value }"
              @click="lagThreshold = preset.value; loadLaggingPartitions()"
            >{{ preset.label }}</button>
          </div>
          <button
            class="btn btn-ghost"
            :disabled="laggingLoading"
            @click="loadLaggingPartitions"
          >
            <svg style="width:12px; height:12px;" :class="{ 'animate-spin': laggingLoading }" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            {{ laggingLoading ? 'Loading…' : 'Refresh' }}
          </button>
        </span>
      </button>

      <div v-if="showLaggingSection" class="lp-body">
        <!-- First paint only, and shaped like the table it stands in for: a
             refresh leaves the rows that are already on screen alone. -->
        <div v-if="laggingFirstLoad" class="lp-table-wrap lp-skel">
          <span v-for="i in 3" :key="`lp-skel-${i}`" class="skeleton" style="height:28px;"></span>
        </div>

        <div v-else-if="lagging.failed.value" class="panel-err">
          Lagging partitions could not be loaded — {{ laggingErrorText }}.
          Nothing here means "unknown", not "nothing is behind".
        </div>

        <div v-else-if="laggingPartitions.length > 0" class="lp-table-wrap">
          <table class="t lp-table">
            <thead>
              <tr>
                <th>Group</th>
                <th>Queue</th>
                <th>Partition</th>
                <th>Worker</th>
                <th style="text-align:right;">Offset lag</th>
                <th style="text-align:right;">Time lag</th>
                <th>Oldest unconsumed</th>
                <th v-if="canAdmin" style="text-align:right;">Action</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="p in laggingPartitions" :key="`${p.consumer_group}@${p.queue_name}@${p.partition_name}`">
                <td><span class="lp-cell-strong">{{ p.consumer_group === '__QUEUE_MODE__' ? 'queue mode' : p.consumer_group }}</span></td>
                <td><span class="lp-cell-mid">{{ p.queue_name }}</span></td>
                <td class="font-mono tabular-nums" style="color:var(--text-mid);">{{ p.partition_name }}</td>
                <td>
                  <code class="lp-cell-code">{{ p.worker_id || '—' }}</code>
                </td>
                <td style="text-align:right;">
                  <span class="font-mono tabular-nums num warn">{{ formatNumber(p.offset_lag) }}</span>
                </td>
                <td style="text-align:right;">
                  <span
                    class="font-mono tabular-nums"
                    :class="{ 'num': true, 'warn': (p.time_lag_seconds || 0) > 60, 'bad': (p.time_lag_seconds || 0) > 600 }"
                  >{{ formatDuration((p.time_lag_seconds || 0) * 1000) }}</span>
                </td>
                <td>
                  <span class="font-mono" style="font-size:11.5px; color:var(--text-mid);">{{ formatDateTime(p.oldest_unconsumed_at) }}</span>
                </td>
                <td v-if="canAdmin" style="text-align:right;">
                  <button
                    class="btn btn-ghost"
                    style="font-size:11px; padding:3px 8px;"
                    :disabled="skippingPartition === partitionKey(p)"
                    @click="askSkipPartition(p)"
                  >
                    {{ skippingPartition === partitionKey(p) ? 'Skipping…' : 'Skip to end' }}
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-else class="lp-empty lp-empty-ok">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--ok-500);">
            <path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <span>No partitions lag more than {{ getLagLabel(lagThreshold) }}.</span>
        </div>
      </div>
    </div>

    <!--
      ========================================================================
      Health grid — the main entity list. Click row → detail modal; hover
      reveals per-row actions (view, move-to-now, seek, delete).
      ========================================================================
    -->
    <ConsumerHealthGrid
      :consumers="filteredConsumers"
      :loading="groupsFirstLoad"
      :sort-by="sortBy"
      :can-admin="canAdmin"
      @select="viewConsumer"
      @view="viewConsumer"
      @move-now="askMoveToNow"
      @seek="openSeekModal"
      @delete="confirmDelete"
    >
      <template #empty>
        <!-- "No consumer groups" is a claim. Only make it when the list
             actually loaded — a failed fetch has to say so instead, and
             offer the retry the page otherwise hides. -->
        <div v-if="groups.failed.value" class="empty-state empty-state-failed">
          <h3>Cannot list consumer groups</h3>
          <p>{{ groupsErrorText }}</p>
          <button class="btn btn-ghost" @click="refreshAll">Retry</button>
        </div>
        <div v-else class="empty-state">
          <!-- Same block, same 40px glyph, as every other empty state in the
               app (Queues · Messages · DeadLetter · Traces). -->
          <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
            <path stroke-linecap="round" stroke-linejoin="round" d="M15 19.128a9.38 9.38 0 002.625.372 9.337 9.337 0 004.121-.952 4.125 4.125 0 00-7.533-2.493M15 19.128v-.003c0-1.113-.285-2.16-.786-3.07M15 19.128v.106A12.318 12.318 0 018.624 21c-2.331 0-4.512-.645-6.374-1.766l-.001-.109a6.375 6.375 0 0111.964-3.07M12 6.375a3.375 3.375 0 11-6.75 0 3.375 3.375 0 016.75 0zm8.25 2.25a2.625 2.625 0 11-5.25 0 2.625 2.625 0 015.25 0z" />
          </svg>
          <h3>{{ consumers.length === 0 ? 'No consumer groups found' : 'No groups match your filters' }}</h3>
          <p>
            {{ consumers.length === 0
              ? 'Consumer groups will appear here when clients connect.'
              : 'Try adjusting your search or filter.' }}
          </p>
        </div>
      </template>
    </ConsumerHealthGrid>

    <!-- ===================== Detail modal ===================== -->
    <Teleport to="body">
      <div
        v-if="selectedConsumer"
        class="modal-backdrop"
        @click.self="selectedConsumer = null"
      >
        <!-- Wider than the shared 480px: five tiles at the shared tile size
             is what this modal is for. -->
        <div class="card modal-card" style="max-width:672px;">
          <div class="card-header">
            <h3>{{ selectedConsumer.name === '__QUEUE_MODE__' ? selectedConsumer.queueName : selectedConsumer.name }}</h3>
            <button @click="selectedConsumer = null" class="btn btn-ghost btn-icon modal-close">
              <svg style="width:18px; height:18px;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                <path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
          <div class="card-body">
            <div class="stat-grid stat-grid-5 stat-grid-center" style="margin-bottom:24px;">
              <div class="stat">
                <div class="stat-label">Partitions</div>
                <div class="stat-value font-mono">{{ dash(selectedConsumer.members) }}</div>
              </div>
              <div class="stat">
                <div class="stat-label">State</div>
                <div class="cg-state" :class="stateClass(selectedConsumer)">{{ getStatusText(selectedConsumer) }}</div>
              </div>
              <div class="stat">
                <div class="stat-label">Lag parts</div>
                <div
                  class="stat-value font-mono num"
                  :class="{ warn: (selectedConsumer.partitionsWithLag || 0) > 0 }"
                >{{ dash(selectedConsumer.partitionsWithLag) }}</div>
              </div>
              <div class="stat">
                <!-- Real message-count backlog from the log engine — the number
                     operators actually ask for, and it is returned already. -->
                <div class="stat-label">Backlog</div>
                <div class="stat-value font-mono">{{ dash(selectedConsumer.totalLag) }}</div>
                <div class="stat-foot" style="justify-content:center;">messages behind</div>
              </div>
              <div class="stat">
                <div class="stat-label">Time lag</div>
                <div class="stat-value font-mono">
                  {{ (selectedConsumer.maxTimeLag || 0) > 0 ? formatDuration(selectedConsumer.maxTimeLag * 1000) : '—' }}
                </div>
              </div>
            </div>

            <div style="margin-bottom:16px;">
              <span class="label-xs" style="display:block; margin-bottom:8px;">Queue</span>
              <div class="modal-queue-pill">
                <span style="font-weight:500; color:var(--text-hi);">{{ selectedConsumer.queueName || '—' }}</span>
              </div>
            </div>

            <div v-if="selectedConsumer.topics?.length > 0">
              <span class="label-xs" style="display:block; margin-bottom:12px;">Topics</span>
              <div style="display:flex; flex-direction:column; gap:8px;">
                <div
                  v-for="topic in selectedConsumer.topics"
                  :key="topic"
                  class="modal-queue-pill"
                  style="display:flex; align-items:center; justify-content:space-between;"
                >
                  <span style="font-weight:500; color:var(--text-hi);">{{ topic }}</span>
                  <button
                    v-if="canAdmin"
                    class="btn btn-ghost"
                    @click="openSeekModal({ ...selectedConsumer, queueName: topic })"
                  >Seek</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- ===================== Delete modal ===================== -->
    <Teleport to="body">
      <div
        v-if="consumerToDelete"
        class="modal-backdrop"
        @click.self="closeDeleteModal"
      >
        <div class="card modal-card">
          <div class="card-header"><h3>Delete consumer group</h3></div>
          <div class="card-body">
            <div v-if="deleteError" class="panel-err">{{ deleteError }}</div>

            <p style="color:var(--text-mid); margin-bottom:16px;">
              Are you sure you want to delete <strong>{{ consumerToDelete.name === '__QUEUE_MODE__' ? '(queue mode)' : consumerToDelete.name }}</strong>
              <span v-if="consumerToDelete.queueName"> for queue <strong>{{ consumerToDelete.queueName }}</strong></span>?
            </p>
            <p style="font-size:13px; color:var(--text-low); margin-bottom:16px;">
              This will remove partition consumer state{{ consumerToDelete.queueName ? ' for this queue only' : '' }}.
            </p>

            <label style="display:flex; align-items:center; gap:8px; font-size:13px; color:var(--text-mid); cursor:pointer;">
              <input v-model="deleteMetadata" type="checkbox" style="width:16px; height:16px; accent-color:var(--accent);" />
              Also delete subscription metadata
            </label>
          </div>

          <div class="modal-foot">
            <button @click="closeDeleteModal" class="btn btn-ghost">Cancel</button>
            <button @click="deleteConsumer" :disabled="actionLoading" class="btn btn-danger">
              {{ actionLoading ? 'Deleting…' : 'Delete' }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- ===================== Seek modal ===================== -->
    <Teleport to="body">
      <div
        v-if="showSeekModal"
        class="modal-backdrop"
        @click.self="showSeekModal = false"
      >
        <div class="card modal-card">
          <div class="card-header"><h3>Seek cursor position</h3></div>
          <div class="card-body">
            <div v-if="seekError" class="panel-err">{{ seekError }}</div>

            <p style="color:var(--text-mid); margin-bottom:16px;">
              {{ seekConsumer?.name === '__QUEUE_MODE__' ? '(queue mode)' : seekConsumer?.name }} / {{ seekConsumer?.queueName }}
            </p>

            <div>
              <span class="label-xs" style="display:block; margin-bottom:8px;">Target timestamp</span>
              <input v-model="seekTimestamp" type="datetime-local" class="input" />
              <p style="font-size:12px; color:var(--text-low); margin-top:8px;">
                The cursor will move to the last message at or before this timestamp. Messages after this point will be re-consumed.
              </p>
            </div>
          </div>

          <div class="modal-foot">
            <button @click="showSeekModal = false" class="btn btn-ghost">Cancel</button>
            <button @click="handleSeekToTimestamp" :disabled="actionLoading || !seekTimestamp" class="btn btn-primary">
              {{ actionLoading ? 'Seeking…' : 'Seek to timestamp' }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- ===================== Confirmation modal =====================
         Replaces the native confirm() that used to guard the two cursor-
         skipping actions: same modal system the rest of the page uses, and
         the outcome is reported instead of assumed. -->
    <Teleport to="body">
      <div v-if="pendingConfirm" class="modal-backdrop" @click.self="pendingConfirm = null">
        <div class="card modal-card">
          <div class="card-header"><h3>{{ pendingConfirm.title }}</h3></div>
          <div class="card-body">
            <p style="color:var(--text-mid); margin-bottom:8px;">{{ pendingConfirm.body }}</p>
            <p style="font-size:13px; color:var(--text-low);">{{ pendingConfirm.consequence }}</p>
          </div>

          <div class="modal-foot">
            <button @click="pendingConfirm = null" class="btn btn-ghost">Cancel</button>
            <button @click="runPendingConfirm" :disabled="actionLoading" class="btn btn-primary">
              {{ actionLoading ? 'Working…' : pendingConfirm.confirmLabel }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'

import ConsumerHealthGrid from '@/components/ConsumerHealthGrid.vue'
import { consumers as consumersApi, describeApiError } from '@/api'
import {
  formatDateTime, formatDuration, formatNumber, toNum, useApi,
} from '@/composables/useApi'
import { formatDateTimeLocal } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import Autocomplete from '@/components/Autocomplete.vue'
import { useQueuesStore } from '@/stores/queuesStore'

// TENANT PAGE. /api/v1/consumer-groups* is tenant-scoped broker-side; the
// mutating routes (delete / seek) are RouteClass::QueueAdmin at the proxy, so
// their controls are shown only to a role that may actually use them.
const { can, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
// notifyWarn carries an outcome the interceptor cannot see: a 2xx that did
// nothing (no partition matched, nothing deleted) is a failure to the user.
const { notifySuccess, notifyWarn } = useToast()
const canAdmin = computed(() => can('queueAdmin'))

// Shared queues store. The Queues page populates it; we just consume here.
// queueMeta gives us the queue → { namespace, task } join we need to scope
// consumer rows by namespace/task without a second API call when the data
// is fresh.
const { queueMeta, namespaces, tasks, fetchQueues } = useQueuesStore()

const route = useRoute()

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const searchQuery = ref('')
const showLaggingOnly = ref(false)
const filterNamespace = ref('')
const filterTask = ref('')
const filterQueue = ref('')
const sortBy = ref('health')

const selectedConsumer = ref(null)
const consumerToDelete = ref(null)
const deleteMetadata = ref(true)
const deleteError = ref('')
const actionLoading = ref(false)

const showSeekModal = ref(false)
const seekConsumer = ref(null)
const seekTimestamp = ref('')
const seekError = ref('')

const pendingConfirm = ref(null)

const showLaggingSection = ref(false)
const lagThreshold = ref(3600)
const skippingPartition = ref(null)

const sortOptions = [
  { value: 'health',  label: 'Worst first' },
  { value: 'lag',     label: 'Lag' },
  { value: 'members', label: 'Partitions' },  // sort key kept for back-compat with grid
  { value: 'name',    label: 'Name' },
]

// Wider time-range presets are kept for power users; the defaults
// (1m → 24h) cover everything from "subtle drift" to "stuck for a day".
const lagPresets = [
  { value: 60,    label: '1m'  },
  { value: 300,   label: '5m'  },
  { value: 1800,  label: '30m' },
  { value: 3600,  label: '1h'  },
  { value: 21600, label: '6h'  },
  { value: 86400, label: '24h' },
]

// ---------------------------------------------------------------------------
// Fetchers. Both keep their own error so a failed refresh can never be shown
// as an empty list or as a clean bill of health.
// ---------------------------------------------------------------------------
const groups = useApi((config) => consumersApi.list(config))
const lagging = useApi((config) => consumersApi.getLagging(lagThreshold.value, config))

const consumers = computed(() => {
  const d = groups.data.value
  if (Array.isArray(d)) return d
  return d?.consumer_groups || []
})
const groupsFirstLoad = computed(() => groups.loading.value && !groups.data.value)
const groupsErrorText = computed(() => describeApiError(groups.error.value))

const laggingPartitions = computed(() => {
  const d = lagging.data.value
  return Array.isArray(d) ? d : []
})
const laggingLoading = computed(() => lagging.loading.value)
// First paint only: a refresh must leave the rows that are already on screen
// where they are, so the skeleton is gated on "loading AND nothing yet".
const laggingFirstLoad = computed(() => lagging.loading.value && !lagging.data.value)
const laggingLoaded = computed(() => lagging.lastUpdated.value !== null && !lagging.failed.value)
const laggingErrorText = computed(() => describeApiError(lagging.error.value))

const fetchConsumers = () => groups.refresh()
const loadLaggingPartitions = () => lagging.refresh()

// Pull queues into the shared store (cache-respecting). If another view
// already populated it within the TTL window, this is a no-op.
const ensureQueues = (force = false) => fetchQueues({ force })

// Refresh everything the page shows — including the lagging badge, which used
// to keep stating its mount-time count while the grid beside it updated.
const refreshAll = async () => {
  await Promise.all([fetchConsumers(), ensureQueues(true), loadLaggingPartitions()])
}

// ---------------------------------------------------------------------------
// Computed
// ---------------------------------------------------------------------------
const isLagging = (g) => {
  if (g.state === 'Lagging') return true
  return (g.maxTimeLag || 0) > 0 || (g.partitionsWithLag || 0) > 0
}

// Helper — pull the queue meta (namespace/task) for a consumer row.
// Returns an empty object when the queue isn't in the store yet, so
// undefined-namespace queues match an empty namespace filter.
const metaFor = (c) => queueMeta.value.get(c.queueName) || {}

// "Queue" dropdown is scoped to the active namespace/task filter, so it
// doesn't show every queue in the cluster when the user has narrowed.
// Also limited to queues that actually have consumer groups, so the
// dropdown isn't padded with idle queues.
const scopedQueueNames = computed(() => {
  const seen = new Set()
  for (const c of consumers.value) {
    const meta = metaFor(c)
    if (filterNamespace.value && meta.namespace !== filterNamespace.value) continue
    if (filterTask.value && meta.task !== filterTask.value) continue
    if (c.queueName) seen.add(c.queueName)
  }
  return [...seen].sort()
})

const filteredConsumers = computed(() => {
  let result = [...consumers.value]
  if (searchQuery.value) {
    const q = searchQuery.value.toLowerCase()
    result = result.filter(c =>
      (c.name || '').toLowerCase().includes(q) ||
      (c.queueName || '').toLowerCase().includes(q)
    )
  }
  if (filterNamespace.value) {
    result = result.filter(c => metaFor(c).namespace === filterNamespace.value)
  }
  if (filterTask.value) {
    result = result.filter(c => metaFor(c).task === filterTask.value)
  }
  if (filterQueue.value) {
    result = result.filter(c => c.queueName === filterQueue.value)
  }
  if (showLaggingOnly.value) {
    result = result.filter(isLagging)
  }
  return result
})

// Clear queue filter when it falls out of the namespace/task scope, so
// the user doesn't end up with an empty result set after narrowing.
watch([filterNamespace, filterTask], () => {
  if (filterQueue.value && !scopedQueueNames.value.includes(filterQueue.value)) {
    filterQueue.value = ''
  }
})

// ---------------------------------------------------------------------------
// Presentation helpers
// ---------------------------------------------------------------------------
const getStatusText = (g) => g.state || 'Unknown'
const stateClass = (g) => {
  const s = getStatusText(g)
  if (s === 'Stable') return 'cg-state-ok'
  if (s === 'Lagging') return 'cg-state-warn'
  return 'cg-state-mute'
}
/** A count we hold, or an em dash — a missing value is not a zero. */
const dash = (v) => {
  const n = toNum(v)
  return n === null ? '—' : formatNumber(n)
}
const partitionKey = (p) => `${p.consumer_group}-${p.queue_name}-${p.partition_name}`

const getLagLabel = (seconds) => {
  if (seconds < 3600) {
    const m = Math.round(seconds / 60); return `${m} minute${m !== 1 ? 's' : ''}`
  } else if (seconds < 86400) {
    const h = Math.round(seconds / 3600); return `${h} hour${h !== 1 ? 's' : ''}`
  }
  const d = Math.round(seconds / 86400); return `${d} day${d !== 1 ? 's' : ''}`
}

const viewConsumer = (g) => { selectedConsumer.value = g }

// ---------------------------------------------------------------------------
// Actions
//
// Every one of these returns a body the UI has to READ: the seek SPs report
// `partitionsUpdated`, the deletes report `deletedPartitions`. A 2xx carrying
// zero of either means nothing happened, and saying "done" there is the same
// lie as a success toast on a failed call.
// ---------------------------------------------------------------------------

/** Null when the payload says work was done, else why it wasn't. */
const seekProblem = (data) => {
  if (!data || typeof data !== 'object') return 'The broker returned no result.'
  if (data.success === false) return data.error || 'The broker refused the seek.'
  const n = toNum(data.partitionsUpdated)
  if (n !== null && n === 0) return 'No partition matched — the cursor did not move.'
  return null
}

const confirmDelete = (g) => {
  deleteError.value = ''
  consumerToDelete.value = g
}
const closeDeleteModal = () => {
  consumerToDelete.value = null
  deleteError.value = ''
}

const deleteConsumer = async () => {
  if (!consumerToDelete.value) return
  const target = consumerToDelete.value
  const label = target.name === '__QUEUE_MODE__' ? `queue mode on ${target.queueName}` : target.name
  actionLoading.value = true
  deleteError.value = ''
  try {
    const res = target.queueName
      ? await consumersApi.deleteForQueue(target.name, target.queueName, deleteMetadata.value)
      : await consumersApi.delete(target.name, deleteMetadata.value)
    const deleted = toNum(res.data?.deletedPartitions)
    if (deleted !== null && deleted === 0) {
      // 200 + nothing removed: the group has no state on this tenant. Keep the
      // modal open rather than close it as if the delete had landed.
      deleteError.value = 'Nothing was deleted — no partition state matched this group.'
      return
    }
    consumerToDelete.value = null
    notifySuccess(
      `Deleted ${label}`,
      deleted === null ? null : `${deleted} partition${deleted === 1 ? '' : 's'} cleared`,
    )
    fetchConsumers()
  } catch (err) {
    // The failure is already on the global surface; this keeps the modal open
    // and says why, instead of closing as though it had worked.
    deleteError.value = describeApiError(err)
  } finally {
    actionLoading.value = false
  }
}

const askMoveToNow = (g) => {
  if (!g.queueName) return
  const label = g.name === '__QUEUE_MODE__' ? '(queue mode)' : g.name
  pendingConfirm.value = {
    title: 'Move cursor to now',
    body: `Move the cursor for "${label}" on queue "${g.queueName}" to the latest message?`,
    consequence: 'Every pending message on that queue is skipped and will never be delivered to this group.',
    confirmLabel: 'Move cursor',
    run: () => moveToNow(g, label),
  }
}

const moveToNow = async (g, label) => {
  try {
    const res = await consumersApi.seek(g.name, g.queueName, { toEnd: true })
    const problem = seekProblem(res.data)
    if (problem) {
      notifyWarn(`Cursor not moved for ${label}`, problem)
      return
    }
    notifySuccess(
      `Cursor moved to now for ${label}`,
      `${res.data.partitionsUpdated} partition${res.data.partitionsUpdated === 1 ? '' : 's'} on ${g.queueName}`,
    )
    fetchConsumers()
  } catch {
    // Already announced by the interceptor with the status and reason.
  }
}

const askSkipPartition = (p) => {
  pendingConfirm.value = {
    title: 'Skip partition to end',
    body: `Skip partition "${p.partition_name}" on queue "${p.queue_name}" (group ${p.consumer_group}) to the latest message?`,
    consequence: 'Every pending message on that partition is skipped and will never be delivered to this group.',
    confirmLabel: 'Skip to end',
    run: () => skipPartition(p),
  }
}

const skipPartition = async (p) => {
  skippingPartition.value = partitionKey(p)
  try {
    const res = await consumersApi.seekPartition(p.consumer_group, p.queue_name, p.partition_name)
    const problem = seekProblem(res.data)
    if (problem) {
      notifyWarn(`Partition ${p.partition_name} not skipped`, problem)
      return
    }
    notifySuccess(`Skipped ${p.partition_name} to the end`, `${p.queue_name} · ${p.consumer_group}`)
    await loadLaggingPartitions()
  } catch {
    // Already announced by the interceptor.
  } finally {
    skippingPartition.value = null
  }
}

const runPendingConfirm = async () => {
  const action = pendingConfirm.value
  if (!action) return
  actionLoading.value = true
  try {
    await action.run()
  } finally {
    actionLoading.value = false
    pendingConfirm.value = null
  }
}

const openSeekModal = (g) => {
  seekConsumer.value = g
  seekError.value = ''
  seekTimestamp.value = formatDateTimeLocal(new Date(Date.now() - 60 * 60 * 1000))
  showSeekModal.value = true
}

const handleSeekToTimestamp = async () => {
  if (!seekConsumer.value || !seekTimestamp.value) return
  const target = seekConsumer.value
  const label = target.name === '__QUEUE_MODE__' ? '(queue mode)' : target.name
  actionLoading.value = true
  seekError.value = ''
  try {
    const isoTimestamp = new Date(seekTimestamp.value).toISOString()
    const res = await consumersApi.seek(target.name, target.queueName, { timestamp: isoTimestamp })
    const problem = seekProblem(res.data)
    if (problem) {
      seekError.value = problem
      return
    }
    showSeekModal.value = false
    seekTimestamp.value = ''
    notifySuccess(
      `Cursor moved for ${label}`,
      `${res.data.partitionsUpdated} partition${res.data.partitionsUpdated === 1 ? '' : 's'} on ${target.queueName}`,
    )
    fetchConsumers()
  } catch (err) {
    seekError.value = describeApiError(err)
  } finally {
    actionLoading.value = false
  }
}

// When the user expands the section, refresh the lagging list — even if it's
// already loaded the threshold may have been adjusted while collapsed.
watch(showLaggingSection, (shown) => {
  if (shown) loadLaggingPartitions()
})

useRefresh(refreshAll)

onMounted(() => {
  if (route.query.search) searchQuery.value = route.query.search
  // Reuse the shared queue cache when possible — if the Queues page recently
  // populated it, this returns immediately without a network round-trip.
  // (The consumer and lagging lists are already in flight via useApi.)
  ensureQueues()
})
</script>

<style scoped>
/* The scope strip, the filter card, the ember panel-err, the stat grid and
   the modal shell now live in style.css — this file keeps only what is
   genuinely local to the Consumers page. */
.qhg-legend {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  color: var(--text-mid);
  margin-left: 4px;
}
.qhg-legend .ld { width: 7px; height: 7px; border-radius: var(--r-pill); }
@media (max-width: 880px) {
  .qhg-legend { display: none; }
}

/* Lagging Partitions card — placed at the top, collapsible. Same surface
   tokens as the filter card above it, and the 16px top-level block rhythm.
   When any partition is above threshold, the card carries a subtle warn
   tint along the left edge so the operator sees something is off without
   having to read the badge. */
.lp-card {
  background: var(--ink-2);
  border: 1px solid var(--bd);
  border-radius: var(--r-card);
  overflow: hidden;
  margin-bottom: 16px;
  transition: border-color .15s var(--ease);
}
.lp-card-warn {
  border-color: color-mix(in srgb, var(--warn-400) 35%, transparent);
  box-shadow: inset 3px 0 0 var(--warn-400);
}
.lp-head {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 14px;
  background: transparent;
  border: none;
  border-bottom: 1px solid transparent;
  cursor: pointer;
  text-align: left;
  color: var(--text-mid);
  font-size: 12px;
  transition: background .12s var(--ease), border-color .12s var(--ease);
}
.lp-head:hover { background: var(--ink-4); }
.lp-head-open { border-bottom-color: var(--bd); }
.lp-head-chevron {
  color: var(--text-low);
  transition: transform .15s var(--ease);
  flex-shrink: 0;
}
.lp-head-open .lp-head-chevron { transform: rotate(90deg); }
.lp-head-title {
  font-size: 12.5px;
  font-weight: 600;
  color: var(--text-hi);
  letter-spacing: -.005em;
}

/* Always-visible count badge — warn-toned when partitions are behind,
   muted-ok when none, ember when the answer is unknown. The badge is the
   operator's first signal; the expanded body is the detail. */
.lp-head-badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: var(--r-pill);
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  font-weight: 500;
  border: 1px solid transparent;
  white-space: nowrap;
}
.lp-head-badge-warn {
  color: var(--warn-400);
  background: color-mix(in srgb, var(--warn-400) 10%, transparent);
  border-color: color-mix(in srgb, var(--warn-400) 26%, transparent);
}
.lp-head-badge-ok {
  color: var(--text-low);
  background: var(--ink-3);
  border-color: var(--bd);
}
.lp-head-badge-bad {
  color: var(--ember-400);
  background: var(--ember-glow);
  border-color: var(--ember-bd);
}

.lp-head-hint {
  font-size: 11.5px;
  color: var(--text-low);
}
.lp-head-meta {
  display: inline-flex;
  align-items: center;
  gap: 8px;
}
.lp-body {
  padding: 6px 0;
}
.lp-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 24px;
  color: var(--text-mid);
  font-size: 13px;
}
.lp-empty-ok { color: var(--text-low); }
/* The shared ember box is full-bleed by default; this card's body is
   flush to its edges (the table spans it), so inset it here. */
.lp-body > .panel-err { margin: 12px 14px; }
.lp-table-wrap {
  overflow-x: auto;
}
/* Skeleton shaped like the table it stands in for: three rows at row height,
   inset to the table's own padding. */
.lp-skel {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 14px;
}
.lp-table { width: 100%; }
.lp-cell-strong { color: var(--text-hi); font-weight: 500; }
.lp-cell-mid { color: var(--text-mid); }
.lp-cell-code {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11.5px;
  padding: 1px 6px;
  border-radius: var(--r-chip);
  background: var(--ink-3);
  border: 1px solid var(--bd);
  color: var(--text-mid);
}

/* The State tile shows a word, not a figure — its own scale and its own
   three colours, which is a meaning, not a chrome variant. */
.cg-state { font-size: 16px; font-weight: 600; margin-top: 8px; }
.cg-state-ok { color: var(--ok-500); }
.cg-state-warn { color: var(--warn-400); }
.cg-state-mute { color: var(--text-mid); }

.modal-queue-pill {
  padding: 12px;
  border-radius: var(--r-card);
  border: 1px solid var(--bd);
  background: var(--ink-3);
}
</style>
