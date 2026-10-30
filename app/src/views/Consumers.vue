<template>
  <!-- A tab of the Queues section: the groups of the queue picked in the list
       beside (?queue=), every group when none is (inside the list's
       namespace, if one is picked there). -->
  <QueuesLayout>
    <div class="view-container cg">
      <!-- A failed list must not look like an empty one. -->
      <div v-if="groupsFailed" class="status-banner banner-bad view-banner">
        <span :title="groupsLoaded ? formatTimestampUtc(groupsStore.lastFetched.value) : undefined">
          <strong>Could not load consumer groups</strong> · {{ groupsErrorText }}<template v-if="consumers.length"> · showing the last list</template>
        </span>
      </div>

      <PageTools>
        <input
          v-model="searchQuery"
          type="search"
          class="input cg-search"
          placeholder="Search groups"
          aria-label="Search consumer groups by name or queue"
        />
        <UiCheck v-model="showLaggingOnly" title="Only the groups past their queue’s lag lines (Settings)">Lagging only</UiCheck>
        <span v-if="countLine" class="tool-note">{{ countLine }}</span>
        <template #view>
          <UiSelect v-model="sortBy" label="Sort" :options="SORTS" aria-label="Sort the groups" />
        </template>
      </PageTools>

      <ConsumerHealthGrid
        :consumers="shown"
        :loading="groupsFirstLoad"
        :can-admin="canAdmin"
        :show-queue="!filterQueue"
        :selected-key="selectedKey"
        @select="viewConsumer"
        @delete="confirmDelete"
      >
        <template #empty>
          <!-- "No consumer groups" is a claim: a failed read says so instead. -->
          <div v-if="groupsFailed" class="empty-state empty-state-failed">
            <h3>Cannot list consumer groups</h3>
            <button class="btn btn-ghost" @click="refreshAll">Retry</button>
          </div>
          <div v-else class="empty-state">
            <Icon name="users-round" class="empty-state-icon" :size="28" />
            <h3>{{ emptyLine }}</h3>
          </div>
        </template>
      </ConsumerHealthGrid>

      <div v-if="shown.length" class="list-legend">
        <span><i class="g ok" aria-hidden="true" />stable</span>
        <span><i class="g warn" aria-hidden="true" />lagging</span>
        <span><i class="g bad" aria-hidden="true" />stuck</span>
        <span><i class="g idle" aria-hidden="true" />never consumed, or inactive</span>
      </div>

      <!-- The partitions behind more than a chosen age, across the groups this
           tab shows. Three states, never two: rows, none, or unknown. -->
      <section v-if="scoped.length || lagRows.length || lagging.failed.value" class="card lp" aria-labelledby="lp-title">
        <div class="card-header">
          <h3 id="lp-title">Lagging partitions</h3>
          <span v-if="lagRows.length" class="card-sub tabular-nums">{{ formatNumber(lagRows.length) }}</span>
          <UiSelect
            v-model="lagThreshold"
            class="lp-pick"
            size="sm"
            label="Behind more than"
            :options="lagOptions"
            aria-label="Show partitions behind more than"
          />
        </div>

        <div v-if="laggingFirstLoad" class="lp-skel" aria-hidden="true">
          <span v-for="i in 3" :key="i" class="skeleton" />
        </div>
        <div v-else-if="lagging.failed.value" class="lp-quiet lp-err">
          Unavailable · {{ laggingErrorText }}
        </div>
        <div v-else-if="!lagRows.length" class="lp-quiet">
          No partition is behind more than {{ lagLabel(lagThreshold) }}.
        </div>
        <template v-else>
          <div class="lp-scroll">
            <table class="t lp-table">
              <thead>
                <tr>
                  <th>Group</th>
                  <th v-if="!filterQueue">Queue</th>
                  <th>Partition</th>
                  <th>Worker</th>
                  <th class="r" title="Messages this group has not read in the partition">Offset lag</th>
                  <th class="r">Time lag</th>
                  <th class="r">Oldest unread</th>
                  <th v-if="canAdmin" class="r"><span class="sr-only">Action</span></th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="p in lagRowsShown" :key="`${p.consumer_group}@${p.queue_name}@${p.partition_name}`">
                  <td>
                    <span class="lp-name">{{ groupLabel({ name: p.consumer_group }) }}</span>
                    <!-- The lagging rows carry no policy field; the group listing does. -->
                    <span v-if="rowConflates(p)" class="lp-tag" title="Delivered one message per partition, the newest">conflation</span>
                  </td>
                  <td v-if="!filterQueue" class="lp-mid">{{ p.queue_name }}</td>
                  <td class="lp-mono">{{ p.partition_name }}</td>
                  <td class="lp-mono lp-mid">{{ p.worker_id || '—' }}</td>
                  <!-- On a conflating group this is log lag: positions to retire,
                       not messages owed, so it is stated, never toned. -->
                  <td
                    class="r lp-num"
                    :title="rowConflates(p) ? 'Log lag: positions still to retire. The work left is one message per partition.' : undefined"
                  >{{ formatNumber(p.offset_lag) }}<i v-if="rowConflates(p)" class="lp-unit">log</i></td>
                  <td class="r lp-num" :class="lagTone(p)">{{ formatSpan(toNum(p.time_lag_seconds) || 0) }}</td>
                  <td class="r lp-mid lp-when" :title="formatTimestampUtc(p.oldest_unconsumed_at)">{{ formatTimestampCompact(p.oldest_unconsumed_at) }}</td>
                  <td v-if="canAdmin" class="r">
                    <button
                      class="btn btn-ghost lp-skip"
                      :disabled="skippingPartition === partitionKey(p)"
                      @click="askSkipPartition(p)"
                    >{{ skippingPartition === partitionKey(p) ? 'Skipping…' : 'Skip to end' }}</button>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-if="lagRows.length > LAG_ROWS" class="pager">
            <span class="pager-count tabular-nums">{{ formatNumber(lagRowsShown.length) }} of {{ formatNumber(lagRows.length) }}</span>
            <button class="btn btn-ghost" @click="showAllLag = !showAllLag">{{ showAllLag ? 'Show fewer' : 'Show all' }}</button>
          </div>
        </template>
      </section>
    </div>

    <!-- ===================== The group ===================== -->
    <DetailDrawer
      :open="Boolean(selectedConsumer)"
      :title="selectedConsumer ? groupLabel(selectedConsumer) : 'Consumer group'"
      :subtitle="selectedConsumer?.queueName || ''"
      @close="closeDetail"
    >
      <template v-if="selectedConsumer">
        <!-- The same verdict as the row it was opened from. -->
        <div class="detail-status-row cg-status">
          <span class="g" :class="selectedVerdict.glyph" aria-hidden="true" />
          <span class="detail-status" :class="`is-${selectedVerdict.tone || 'quiet'}`">{{ selectedVerdict.word }}</span>
          <!-- Said here only when the Last seen row below cannot say it. -->
          <span v-if="presence?.said && presence.seen === '—'" class="detail-note">· {{ presence.said }}</span>
          <span v-if="isConflating(selectedConsumer)" class="lp-tag" title="Delivered one message per partition, the newest">conflation</span>
          <span v-if="selectedConsumer.kind === 'kafka'" class="lp-tag" title="Read through the Kafka facade">Kafka</span>
        </div>
        <p v-if="presence?.note" class="cg-note" :title="presence.noteTitle">{{ presence.note }}</p>

        <ul class="detail-list cg-facts">
          <li>
            <span class="detail-note">Queue</span>
            <RouterLink v-if="selectedConsumer.queueName" class="cg-link" :to="queueLocation(selectedConsumer.queueName, route)">{{ selectedConsumer.queueName }}</RouterLink>
            <span v-else>—</span>
          </li>
          <li>
            <span class="detail-note" title="Partitions this group reads">Partitions</span>
            <span class="tabular-nums">{{ dash(selectedConsumer.members) }}</span>
          </li>
          <li>
            <!-- On a conflating group the partitions behind ARE the work: one
                 handler run each, whatever the backlog. -->
            <span class="detail-note" title="Partitions with messages this group has not read yet">Behind</span>
            <span class="tabular-nums">{{ dash(selectedConsumer.partitionsWithLag) }}<i v-if="isConflating(selectedConsumer)" class="cg-unit">handler runs left</i></span>
          </li>
          <li>
            <span class="detail-note" :title="isConflating(selectedConsumer) ? 'Positions still to retire; superseded, not owed' : 'Messages this group has not read'">{{ isConflating(selectedConsumer) ? 'Log lag' : 'Backlog' }}</span>
            <span class="tabular-nums">{{ dash(selectedConsumer.totalLag) }}<i class="cg-unit">{{ isConflating(selectedConsumer) ? 'positions' : 'messages' }}</i></span>
          </li>
          <li>
            <span class="detail-note" title="Age of the oldest message this group has not read">Time lag</span>
            <span class="tabular-nums" :class="selectedLagTone">{{ toNum(selectedConsumer.maxTimeLag) > 0 ? formatSpan(Math.round(toNum(selectedConsumer.maxTimeLag))) : '—' }}</span>
          </li>
          <template v-if="presence">
            <li>
              <span class="detail-note">Live members</span>
              <span class="tabular-nums">{{ presence.members }}</span>
            </li>
            <li>
              <span class="detail-note">Last seen</span>
              <span class="tabular-nums" :title="presence.seenAt ? formatTimestampUtc(presence.seenAt) : undefined">{{ presence.seen }}</span>
            </li>
          </template>
          <li v-for="topic in selectedConsumer.topics || []" :key="topic">
            <span class="detail-note">Topic</span>
            <span class="cg-topic">
              <span class="lp-mono">{{ topic }}</span>
              <button v-if="canAdmin" class="btn btn-ghost" @click="openSeekModal({ ...selectedConsumer, queueName: topic })">Seek</button>
            </span>
          </li>
          <li v-if="selectedConsumer.queueName">
            <span class="detail-note">Supervisors</span>
            <RouterLink class="cg-link" :to="queueLocation(selectedConsumer.queueName, route, 'supervisors')">Open</RouterLink>
          </li>
        </ul>
      </template>

      <template v-if="selectedConsumer" #footer>
        <template v-if="canAdmin">
          <button class="btn btn-danger cg-delete" @click="confirmDelete(selectedConsumer)">Delete</button>
          <template v-if="selectedConsumer.queueName">
            <button class="btn" title="Move the cursor to a point in time" @click="openSeekModal(selectedConsumer)">Seek…</button>
            <button class="btn" title="Skip every pending message for this group" @click="askMoveToNow(selectedConsumer)">Move to now</button>
          </template>
        </template>
        <span v-else class="detail-note cg-readonly">Cursor and delete actions need the admin role.</span>
      </template>
    </DetailDrawer>

    <!-- ===================== Delete ===================== -->
    <Teleport to="body">
      <div v-if="consumerToDelete" class="modal-backdrop cg-over" @click.self="closeDeleteModal">
        <div class="card modal-card">
          <div class="card-header"><h3>Delete consumer group</h3></div>
          <div class="card-body cg-form">
            <div v-if="deleteError" class="panel-err">{{ deleteError }}</div>
            <p class="cg-lead">
              Delete <strong>{{ groupLabel(consumerToDelete) }}</strong><template v-if="consumerToDelete.queueName"> on <strong>{{ consumerToDelete.queueName }}</strong></template>?
            </p>
            <p class="cg-help">Its cursor{{ consumerToDelete.queueName ? ' on this queue' : 's' }} and partition state are removed.</p>
            <UiCheck v-model="deleteMetadata">Also delete its subscription</UiCheck>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="closeDeleteModal">Cancel</button>
            <button class="btn btn-danger" :disabled="actionLoading" @click="deleteConsumer">{{ actionLoading ? 'Deleting…' : 'Delete' }}</button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- ===================== Seek ===================== -->
    <Teleport to="body">
      <div v-if="showSeekModal" class="modal-backdrop cg-over" @click.self="showSeekModal = false">
        <form class="card modal-card" @submit.prevent="handleSeekToTimestamp">
          <div class="card-header">
            <h3>Seek</h3>
            <span class="card-sub">{{ groupLabel(seekConsumer) }} · {{ seekConsumer?.queueName }}</span>
          </div>
          <div class="card-body cg-form">
            <div v-if="seekError" class="panel-err">{{ seekError }}</div>
            <label class="cg-field">
              <span class="cg-field-label">Move the cursor to</span>
              <input v-model="seekTimestamp" type="datetime-local" class="input" :title="formatTimestampUtc(seekTimestamp)" />
            </label>
            <p class="cg-help" title="The cursor moves to the last message at or before this time">Messages after this time are delivered again.</p>
          </div>
          <div class="modal-foot">
            <button type="button" class="btn btn-ghost" @click="showSeekModal = false">Cancel</button>
            <button type="submit" class="btn btn-primary" :disabled="actionLoading || !seekTimestamp">{{ actionLoading ? 'Seeking…' : 'Seek' }}</button>
          </div>
        </form>
      </div>
    </Teleport>

    <!-- ===================== Confirm (move to now, skip a partition) ===================== -->
    <Teleport to="body">
      <div v-if="pendingConfirm" class="modal-backdrop cg-over" @click.self="pendingConfirm = null">
        <div class="card modal-card">
          <div class="card-header"><h3>{{ pendingConfirm.title }}</h3></div>
          <div class="card-body cg-form">
            <p class="cg-lead">{{ pendingConfirm.body }}</p>
            <p class="cg-help">{{ pendingConfirm.consequence }}</p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="pendingConfirm = null">Cancel</button>
            <button class="btn btn-primary" :disabled="actionLoading" @click="runPendingConfirm">{{ actionLoading ? 'Working…' : pendingConfirm.confirmLabel }}</button>
          </div>
        </div>
      </div>
    </Teleport>
  </QueuesLayout>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import { consumers as consumersApi, describeApiError } from '@/api'
import ConsumerHealthGrid from '@/components/ConsumerHealthGrid.vue'
import DetailDrawer from '@/components/DetailDrawer.vue'
import Icon from '@/components/Icon.vue'
import PageTools from '@/components/PageTools.vue'
import QueuesLayout from '@/components/QueuesLayout.vue'
import UiCheck from '@/components/UiCheck.vue'
import UiSelect from '@/components/UiSelect.vue'
import {
  groupCount, groupLabel, sortGroups, verdictOf, visibleGroups, visiblePartitions, SORTS,
} from '@/composables/consumerGroups'
import { queueLocation, queueOf } from '@/composables/navigation'
import { formatSpan } from '@/composables/settingsDoc'
import { formatNumber, toNum, useApi } from '@/composables/useApi'
import { groupPresence } from '@/composables/useAttention'
import { conflatingKeys, isConflating, partitionConflates } from '@/composables/useConflation'
import { formatDateTimeLocal, formatTimestamp, formatTimestampCompact, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { groupActivity, timeLagSeverity } from '@/composables/useSeverity'
import { useToast } from '@/composables/useToast'
import { useGroupsStore } from '@/stores/groupsStore'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { DEFAULT_TIME_RANGE, LAG_THRESHOLDS, rangeSeconds } from '../../../shared/timeRanges.mjs'

// TENANT PAGE. /api/v1/consumer-groups* is tenant-scoped broker-side; delete
// and seek are RouteClass::QueueAdmin at the proxy, so their controls are
// offered only to a role that may use them.
const { can } = useIdentity()
// notifyWarn carries an outcome the HTTP client cannot see: a 2xx that did
// nothing (no partition matched, nothing deleted) is a failure to the user.
const { notifySuccess, notifyWarn } = useToast()
const canAdmin = computed(() => can('queueAdmin'))

const route = useRoute()
const { queueMeta, fetchQueues } = useQueuesStore()
const { linesFor } = useSettingsStore()

// ---------------------------------------------------------------------------
// The scope is the section's: the queue picked in the list, else the list's
// namespace (null = every namespace; '' is the default one). Read, never written.
// ---------------------------------------------------------------------------
const filterQueue = computed(() => queueOf(route))
const filterNamespace = computed(() => (typeof route.query.namespace === 'string' ? route.query.namespace : null))

// The tab's own fields, in the address.
const searchQuery = ref('')
const showLaggingOnly = ref(false)
const sortBy = ref('health')
// The open group: its name, and its queue when no queue is picked (a group's
// name alone can belong to several queues).
const selectedGroup = ref('')
const groupQueue = ref('')
const lagThreshold = ref(rangeSeconds(DEFAULT_TIME_RANGE))
useRouteState({
  search: searchQuery, lagging: showLaggingOnly, sort: sortBy,
  group: selectedGroup, groupQueue, behind: lagThreshold,
})

// ---------------------------------------------------------------------------
// The groups: the app's one listing (stores/groupsStore), which the queue list
// beside keeps fresh, so this tab, the list's marks and the tab counts never
// disagree and nobody pays for the scan twice.
// ---------------------------------------------------------------------------
const groupsStore = useGroupsStore()
const consumers = computed(() => groupsStore.groups.value || [])
const groupsLoaded = computed(() => groupsStore.groups.value !== null)
const groupsFailed = computed(() => groupsStore.error.value !== null)
const groupsFirstLoad = computed(() => !groupsLoaded.value && !groupsFailed.value)
const groupsErrorText = computed(() => describeApiError(groupsStore.error.value))
const fetchConsumers = (force = true) => groupsStore.fetchGroups(force ? { force: true } : { ttlMs: 25_000 })

const metaOf = (q) => queueMeta.value.get(q)
const scoped = computed(() => visibleGroups(consumers.value, {
  queue: filterQueue.value, namespace: filterNamespace.value, metaOf, linesFor,
}))
const shown = computed(() => sortGroups(
  visibleGroups(scoped.value, { search: searchQuery.value, laggingOnly: showLaggingOnly.value, linesFor }),
  sortBy.value, linesFor,
))
const narrowed = computed(() => Boolean(searchQuery.value.trim()) || showLaggingOnly.value)
// The tab's count names the picked queue's groups already; the line speaks
// when it adds something: across every queue, or narrowed.
const countLine = computed(() => (
  groupsLoaded.value && scoped.value.length && (narrowed.value || !filterQueue.value)
    ? groupCount(shown.value.length, scoped.value.length) : ''
))
const emptyLine = computed(() => {
  if (!consumers.value.length) return 'No consumer groups yet'
  if (!scoped.value.length) return filterQueue.value ? 'No consumer group reads this queue yet' : 'No consumer groups in this namespace'
  if (showLaggingOnly.value && !searchQuery.value.trim()) return 'No group is lagging'
  return 'No group matches'
})

// ---------------------------------------------------------------------------
// The open group
// ---------------------------------------------------------------------------
const openQueue = computed(() => filterQueue.value || groupQueue.value)
const selectedConsumer = computed(() => (
  selectedGroup.value
    ? consumers.value.find((g) => g.name === selectedGroup.value && (!openQueue.value || g.queueName === openQueue.value)) || null
    : null
))
const selectedKey = computed(() => {
  const g = selectedConsumer.value
  return g ? `${g.queueName ?? ''}\u0000${g.name ?? ''}` : ''
})
const selectedVerdict = computed(() => verdictOf(selectedConsumer.value, linesFor))
const selectedLagTone = computed(() => {
  const g = selectedConsumer.value
  if (!g || groupActivity(g, linesFor(g.queueName)) === 'inactive') return ''
  const sev = timeLagSeverity(g.maxTimeLag, linesFor(g.queueName))
  return sev === 'warn' || sev === 'bad' ? `is-${sev}` : ''
})
const viewConsumer = (g) => {
  selectedGroup.value = g?.name || ''
  groupQueue.value = filterQueue.value ? '' : g?.queueName || ''
}
const closeDetail = () => {
  selectedGroup.value = ''
  groupQueue.value = ''
}

// A Kafka group's presence as the facade reports it: its live members, when it
// was last seen, and what its state means. Every age is `inactiveSeconds`, on
// the broker's clock, never this browser's.
const presence = computed(() => {
  const g = selectedConsumer.value
  if (g?.kind !== 'kafka') return null
  const lines = linesFor(g.queueName)
  const activity = groupActivity(g, lines)
  const away = toNum(g.inactiveSeconds)
  const members = toNum(g.liveMembers)
  const grace = formatSpan(lines.inactiveAfterSeconds)
  let seen = '—'
  if (activity === 'live') seen = 'now'
  else if (g.lastSeenAt) seen = formatTimestamp(g.lastSeenAt) + (away > 0 ? ` · ${formatSpan(away)} ago` : '')
  let note = ''
  let noteTitle = ''
  if (activity === 'live' && members === 0) {
    note = 'Its client assigns its own partitions.'
    noteTitle = 'Live with no members: its client assigns its own partitions and commits offsets.'
  } else if (activity === 'idle') {
    note = `Judged by its lag until it has been away ${grace}.`
    noteTitle = 'No live member right now, which is also what a restart or a rebalance looks like for a while. Past the grace it is shown as inactive.'
  } else if (activity === 'inactive') {
    note = 'Inactive: not judged by its lag.'
    noteTitle = `${away !== null ? `No live member for longer than ${grace}. ` : ''}An inactive group raises no issue.`
  }
  return {
    members: members === null ? '—' : formatNumber(members),
    seen,
    seenAt: activity === 'live' ? null : g.lastSeenAt || null,
    said: groupPresence(g, linesFor),
    note,
    noteTitle,
  }
})

/** A count we hold, or an em dash: a missing value is not a zero. */
const dash = (v) => {
  const n = toNum(v)
  return n === null ? '—' : formatNumber(n)
}

// ---------------------------------------------------------------------------
// Lagging partitions, behind more than a chosen age
// ---------------------------------------------------------------------------
const LAG_ROWS = 25
const showAllLag = ref(false)
const lagLabel = (seconds) => {
  if (seconds < 3600) { const m = Math.round(seconds / 60); return `${m} minute${m !== 1 ? 's' : ''}` }
  if (seconds < 86400) { const h = Math.round(seconds / 3600); return `${h} hour${h !== 1 ? 's' : ''}` }
  const d = Math.round(seconds / 86400)
  return `${d} day${d !== 1 ? 's' : ''}`
}
const lagOptions = LAG_THRESHOLDS.map((p) => ({ value: p.seconds, label: lagLabel(p.seconds) }))

const lagging = useApi((config) => consumersApi.getLagging(lagThreshold.value, config))
const laggingPartitions = computed(() => (Array.isArray(lagging.data.value) ? lagging.data.value : []))
// First paint only: a refresh leaves the rows on screen where they are.
const laggingFirstLoad = computed(() => lagging.loading.value && !lagging.data.value)
const laggingErrorText = computed(() => describeApiError(lagging.error.value))
const loadLaggingPartitions = () => lagging.refresh()
const lagRows = computed(() => visiblePartitions(laggingPartitions.value, {
  queue: filterQueue.value, namespace: filterNamespace.value, metaOf, search: searchQuery.value,
}))
const lagRowsShown = computed(() => (showAllLag.value ? lagRows.value : lagRows.value.slice(0, LAG_ROWS)))

// Conflation is declared per (group, queue) and only the GROUP listing carries
// it, so the partition rows are labelled from the listing already held.
const conflatingGroupKeys = computed(() => conflatingKeys(consumers.value))
const rowConflates = (p) => partitionConflates(p, conflatingGroupKeys.value)
// An age, judged by its queue's lines (Settings); a conflating group's is
// stated, not toned: on that policy a backlog is the design working.
const lagTone = (p) => {
  if (rowConflates(p)) return ''
  const sev = timeLagSeverity(p.time_lag_seconds, linesFor(p.queue_name))
  return sev === 'warn' || sev === 'bad' ? sev : ''
}
const partitionKey = (p) => `${p.consumer_group}-${p.queue_name}-${p.partition_name}`
const skippingPartition = ref(null)

watch(lagThreshold, () => {
  showAllLag.value = false
  loadLaggingPartitions()
})

// Pull the queue listing (cache-respecting): it scopes groups by namespace.
const ensureQueues = (force = false) => fetchQueues({ force })

const refreshAll = async () => {
  await Promise.all([fetchConsumers(true), ensureQueues(true), loadLaggingPartitions()])
}

// ---------------------------------------------------------------------------
// Actions. Every one returns a body the UI has to READ: the seeks report
// `partitionsUpdated`, the deletes `deletedPartitions`. A 2xx carrying zero of
// either did nothing, and saying "done" there is a lie.
// ---------------------------------------------------------------------------
const consumerToDelete = ref(null)
const deleteMetadata = ref(true)
const deleteError = ref('')
const actionLoading = ref(false)

const showSeekModal = ref(false)
const seekConsumer = ref(null)
const seekTimestamp = ref('')
const seekError = ref('')

const pendingConfirm = ref(null)

/** Null when the payload says work was done, else why it was not. */
const seekProblem = (data) => {
  if (!data || typeof data !== 'object') return 'The broker returned no result.'
  if (data.success === false) return data.error || 'The broker refused the seek.'
  const n = toNum(data.partitionsUpdated)
  if (n !== null && n === 0) return 'No partition matched — the cursor did not move.'
  return null
}
const plural = (n, one) => `${n} ${one}${n === 1 ? '' : 's'}`

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
      // 200 + nothing removed: keep the modal open rather than close it as if
      // the delete had landed.
      deleteError.value = 'Nothing was deleted — no partition state matched this group.'
      return
    }
    consumerToDelete.value = null
    if (selectedConsumer.value && selectedKey.value === `${target.queueName ?? ''}\u0000${target.name ?? ''}`) closeDetail()
    notifySuccess(`Deleted ${label}`, deleted === null ? null : `${plural(deleted, 'partition')} cleared`)
    fetchConsumers()
  } catch (err) {
    // Already on the global surface; this keeps the modal open and says why.
    deleteError.value = describeApiError(err)
  } finally {
    actionLoading.value = false
  }
}

const askMoveToNow = (g) => {
  if (!g.queueName) return
  const label = groupLabel(g)
  pendingConfirm.value = {
    title: 'Move to now',
    body: `Move ${label} on ${g.queueName} to the latest message?`,
    consequence: 'Every pending message is skipped and never delivered to this group.',
    confirmLabel: 'Move to now',
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
    notifySuccess(`Moved ${label} to now`, `${plural(res.data.partitionsUpdated, 'partition')} on ${g.queueName}`)
    fetchConsumers()
  } catch {
    // Already announced by the shared HTTP client with the status and reason.
  }
}

const askSkipPartition = (p) => {
  pendingConfirm.value = {
    title: 'Skip to end',
    body: `Skip partition ${p.partition_name} on ${p.queue_name} to the latest message for ${groupLabel({ name: p.consumer_group })}?`,
    consequence: 'Every pending message in it is skipped and never delivered to this group.',
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
    notifySuccess(`Skipped ${p.partition_name} to the end`, `${p.queue_name} · ${groupLabel({ name: p.consumer_group })}`)
    await loadLaggingPartitions()
  } catch {
    // Already announced by the shared HTTP client.
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
  const label = groupLabel(target)
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
    notifySuccess(`Moved the cursor of ${label}`, `${plural(res.data.partitionsUpdated, 'partition')} on ${target.queueName}`)
    fetchConsumers()
  } catch (err) {
    seekError.value = describeApiError(err)
  } finally {
    actionLoading.value = false
  }
}

useRefresh(refreshAll)

onMounted(() => {
  // Cache-respecting: the list beside has usually just read both.
  ensureQueues()
  fetchConsumers(false)
})
</script>

<style scoped>
.cg { display: flex; flex-direction: column; gap: 16px; }
.cg > .page-tools, .cg > .view-banner { margin-bottom: 0; }
.cg > .list-legend { margin-top: -8px; }
.cg-search { flex: 0 1 240px; min-width: 160px; height: 32px; }

/* --- Lagging partitions ---------------------------------------------------- */
.lp .card-header { flex-wrap: wrap; row-gap: 8px; }
.lp-pick { margin-left: auto; }
.lp-quiet { padding: 12px 16px; font-size: 13px; color: var(--text-low); }
.lp-err { color: var(--ember-400); }
.lp-skel { display: flex; flex-direction: column; gap: 8px; padding: 12px 16px; }
.lp-skel .skeleton { height: 24px; }
.lp-scroll { overflow-x: auto; }
.lp-table { min-width: 720px; }
.lp-table th, .lp-table td { white-space: nowrap; }
.lp-table .r { text-align: right; }
.lp-name { font-weight: 500; color: var(--text-hi); }
.lp-mid { color: var(--text-mid); }
.lp-mono { font-family: var(--font-mono); font-size: 12px; }
.lp-num { font-family: var(--font-mono); font-size: 12px; font-variant-numeric: tabular-nums; color: var(--text-hi); }
.lp-num.warn { color: var(--warn-400); }
.lp-num.bad { color: var(--ember-400); }
.lp-when { font-family: var(--font-mono); font-size: 12px; font-variant-numeric: tabular-nums; }
.lp-table td { height: 40px; font-size: 13px; }
.lp-unit { margin-left: 4px; font-family: var(--font-sans); font-style: normal; font-size: 11px; color: var(--text-low); }
.lp-tag {
  display: inline-block; margin-left: 8px; padding: 0 6px;
  border: 1px solid var(--bd); border-radius: var(--r-chip);
  font-size: 11px; line-height: 17px; color: var(--text-low); vertical-align: 1px;
}
.lp-skip { padding: 3px 8px; font-size: 12px; }

/* --- The group's drawer ------------------------------------------------------ */
.cg-status { flex-wrap: wrap; }
.cg-status .lp-tag { margin-left: 2px; }
.cg-status .detail-status.is-warn { color: var(--warn-400); }
.cg-status .detail-status.is-bad { color: var(--ember-400); }
.cg-note { margin: -8px 0 16px; font-size: 12px; color: var(--text-mid); }
.cg-facts .tabular-nums.is-warn { color: var(--warn-400); }
.cg-facts .tabular-nums.is-bad { color: var(--ember-400); }
.cg-unit { margin-left: 4px; font-style: normal; font-size: 12px; color: var(--text-low); }
.cg-link { color: var(--text-hi); text-decoration: underline; text-decoration-color: var(--bd-hi); text-underline-offset: 3px; }
.cg-link:hover { text-decoration-color: currentColor; }
.cg-topic { display: inline-flex; align-items: center; gap: 8px; }
.cg-delete { margin-right: auto; }
.cg-readonly { margin-right: auto; }

/* --- Modals: over the drawer that opens them (drawer 51, scrim 50) ---------- */
.cg-over { z-index: 55; }
.cg-form { display: flex; flex-direction: column; gap: 12px; }
.cg-form .panel-err { margin: 0; }
.cg-lead { margin: 0; font-size: 13px; color: var(--text-hi); overflow-wrap: anywhere; }
.cg-lead strong { font-weight: 600; }
.cg-help { margin: 0; font-size: 12px; color: var(--text-low); }
.cg-field { display: flex; flex-direction: column; gap: 6px; }
.cg-field-label { font-size: 12px; color: var(--text-mid); }
.cg-field .input { height: 32px; }

@media (max-width: 640px) {
  .cg-search { flex: 1 1 100%; }
}
</style>
