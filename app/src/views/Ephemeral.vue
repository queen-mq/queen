<template>
  <!-- Every ephemeral queue is the list (EphemeralSide): picked on the left
       (?name=), shown on the right; nothing picked, the table of all of them.
       The list's search narrows the table too. -->
  <SubSidebar label="Ephemeral queues" :current="picked || 'All ephemeral queues'">
  <template #side><EphemeralSide v-model:find="find" /></template>
  <div class="view-container eph">

    <PageHead
      :title="picked || 'All ephemeral queues'"
      :live="support === 'live' ? refreshAgo : ''"
      :live-title="POLL_NOTE"
    >
      <!-- The way back to every queue is the top bar's "Ephemeral queues"
           crumb (route meta pick: 'name'), as on every page with a pick. -->
      <template v-if="facts.length" #sub>
        <span class="facts">
          <span v-for="f in facts" :key="f.k" class="fact" :title="f.title"><span class="fact-k">{{ f.k }}</span>{{ f.v }}</span>
        </span>
      </template>
      <!-- §5.1: reset and delete are writes on this family (produce or
           consume keys, any user but a Viewer). -->
      <template v-if="pickedRow && canMutate" #actions>
        <button class="btn" :title="RESET_TITLE" @click="ask('reset', pickedRow)"><Icon name="rotate-ccw" :size="14" />Reset</button>
        <button class="btn" :title="DELETE_TITLE" @click="ask('delete', pickedRow)"><Icon name="trash" :size="14" />Delete</button>
      </template>
    </PageHead>

    <!-- Not exposed here (404, a proxy that does not classify the family, or
         the SPA answering an API path) or not granted (403 feature_gated):
         facts about this cell, not failures of it. One quiet card, and the
         store stops asking so the poll cannot become a toast loop. -->
    <div v-if="support === 'absent' || support === 'gated'" class="card">
      <div class="empty-state">
        <Icon :name="support === 'gated' ? 'lock' : 'zap'" class="empty-state-icon" :size="28" :stroke="1.5" />
        <h3 :title="support === 'gated' ? GATED_TITLE : ABSENT_TITLE">
          {{ support === 'gated' ? 'Ephemeral queues are not enabled for this cluster' : 'This broker does not serve ephemeral queues' }}
        </h3>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <template v-else>
      <!-- The store keeps the last rows that loaded: say how old they are. -->
      <div v-if="isStale" class="status-banner banner-bad view-banner">
        <span :title="formatTimestampUtc(lastFetched)">
          <strong>Could not refresh the ephemeral queues</strong> · {{ describeApiError(error) }} ·
          showing the rows that loaded at {{ lastFetchedText }}, which on a RAM class may already be gone.
        </span>
      </div>

      <!-- ===================== One queue ===================== -->
      <template v-if="picked">
        <div v-if="!loaded && error" class="card">
          <div class="empty-state empty-state-failed">
            <h3>Cannot list ephemeral queues</h3>
            <p>{{ describeApiError(error) }}</p>
            <button class="btn btn-ghost" @click="refreshAll">Retry</button>
          </div>
        </div>
        <div v-else-if="!loaded" class="card">
          <div class="eph-reading"><div v-for="i in 3" :key="i" class="skeleton eph-skeleton" /></div>
        </div>
        <div v-else-if="!pickedRow" class="card">
          <div class="empty-state">
            <Icon name="zap" class="empty-state-icon" :size="28" :stroke="1.5" />
            <p :title="GONE_TITLE">No ephemeral queue named {{ picked }} right now</p>
          </div>
        </div>
        <template v-else>
          <!-- One cursor per group over the ONE ring (§1.5): lag is head −
               cursor, the only lag this class has. -->
          <section class="card">
            <div class="card-header"><h3>Groups</h3></div>
            <div v-if="pickedGroups && pickedGroups.length" class="eph-scroll">
              <table class="t eph-table">
                <thead>
                  <tr>
                    <th>Group</th>
                    <th class="num" :title="CURSOR_TITLE">Cursor</th>
                    <th class="num" :title="LAG_TITLE">Lag</th>
                    <th class="num" :title="SKIPPED_TITLE">Skipped</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="g in pickedGroups" :key="g.name">
                    <td><span class="eph-name">{{ g.name }}</span></td>
                    <td class="num">{{ num(g.cursor) }}</td>
                    <td class="num">{{ num(g.lag) }}</td>
                    <td class="num" :class="skippedClass(g)" :title="g.skipped ? SKIPPED_TITLE : ''">{{ num(g.skipped) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div v-else-if="pickedGroups" class="empty-state eph-empty">
              <Icon name="users" class="empty-state-icon" :size="28" :stroke="1.5" />
              <p>No group has read this queue yet</p>
            </div>
            <div v-else-if="detailState === 'reading'" class="eph-reading"><div v-for="i in 2" :key="i" class="skeleton eph-skeleton" /></div>
            <div v-else class="empty-state eph-empty">
              <Icon name="users" class="empty-state-icon" :size="28" :stroke="1.5" />
              <p :title="detailState === 'failed' ? detailErrorText : ''">
                {{ detailState === 'failed' ? 'Cannot read this queue’s groups' : 'This broker does not report group cursors' }}
              </p>
            </div>
          </section>

          <!-- Only where there is more than one: a single partition is the
               queue itself, already in the head. -->
          <section v-if="pickedPartitions && pickedPartitions.length > 1" class="card">
            <div class="card-header"><h3>Partitions</h3></div>
            <div class="eph-scroll">
              <table class="t eph-table">
                <thead>
                  <tr>
                    <th>Partition</th>
                    <th class="num">Messages</th>
                    <th class="num">Size</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="p in pickedPartitions" :key="p.name">
                    <td><span class="eph-name">{{ p.name }}</span></td>
                    <td class="num">{{ num(p.depth) }}</td>
                    <td class="num">{{ bytes(p.bytes) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </section>
        </template>
      </template>

      <!-- ===================== Every queue ===================== -->
      <template v-else>
        <!-- Thousands of request/reply inboxes are the point of this class,
             so the order is explicit. The search is the list's, beside it. -->
        <PageTools>
          <UiCheck v-model="declaredOnly">Declared only</UiCheck>
          <template #view>
            <div class="tool-seg">
              <span class="tool-label">Sort</span>
              <div class="seg" role="group" aria-label="Sort">
                <button
                  v-for="opt in sortOptions"
                  :key="opt.value"
                  :class="{ on: sortBy === opt.value }"
                  :aria-pressed="sortBy === opt.value ? 'true' : 'false'"
                  @click="sortBy = opt.value"
                >{{ opt.label }}</button>
              </div>
            </div>
          </template>
        </PageTools>

        <div class="card">
          <div v-if="firstLoad || visible.length" class="eph-scroll">
            <table class="t eph-table">
              <thead>
                <tr>
                  <th>Queue</th>
                  <th :title="TIER_TITLE">Tier</th>
                  <th class="num" :title="MESSAGES_TITLE">Messages</th>
                  <th class="num">Size</th>
                  <th class="num">Groups</th>
                  <th class="num" :title="DROPPED_TITLE">Dropped</th>
                  <th v-if="showOwner">Owner</th>
                  <th v-if="canMutate" class="num"><span class="eph-sr">Actions</span></th>
                </tr>
              </thead>
              <tbody>
                <!-- First paint only: a 2 s poll never blanks the rows being read. -->
                <template v-if="firstLoad">
                  <tr v-for="i in 6" :key="`sk-${i}`"><td :colspan="colCount"><div class="skeleton eph-skeleton" /></td></tr>
                </template>
                <template v-else>
                  <tr
                    v-for="row in visible"
                    :key="row.name"
                    class="eph-row"
                    tabindex="0"
                    @click="pick(row.name)"
                    @keydown.enter.self="pick(row.name)"
                  >
                    <td><span class="eph-name" :title="row.name">{{ row.name }}</span></td>
                    <td class="eph-tier" :title="tierTitle(row.declared)">{{ tierWord(row.declared) }}</td>
                    <td class="num">{{ num(row.depth) }}</td>
                    <td class="num">{{ bytes(row.bytes) }}</td>
                    <td class="num">{{ num(row.groupCount) }}</td>
                    <td class="num" :class="{ mute: !dropTotal(row.drops) }" :title="describeDrops(row.drops)">{{ num(dropTotal(row.drops)) }}</td>
                    <td v-if="showOwner"><span class="eph-owner">{{ row.owner || '—' }}</span></td>
                    <td v-if="canMutate" class="num eph-acts" @click.stop>
                      <button class="btn btn-ghost btn-icon" :title="`Reset ${row.name}`" :aria-label="`Reset ${row.name}`" @click="ask('reset', row)"><Icon name="rotate-ccw" :size="14" /></button>
                      <button class="btn btn-ghost btn-icon" :title="`Delete ${row.name}`" :aria-label="`Delete ${row.name}`" @click="ask('delete', row)"><Icon name="trash" :size="14" /></button>
                    </td>
                  </tr>
                </template>
              </tbody>
            </table>
          </div>

          <!-- A failure with nothing loaded is not an empty class. -->
          <div v-else-if="error" class="empty-state empty-state-failed">
            <h3>Cannot list ephemeral queues</h3>
            <p>{{ describeApiError(error) }}</p>
            <button class="btn btn-ghost" @click="refreshAll">Retry</button>
          </div>
          <div v-else-if="rows.length" class="empty-state">
            <Icon name="search" class="empty-state-icon" :size="28" :stroke="1.5" />
            <p>No queue matches</p>
          </div>
          <div v-else class="empty-state">
            <Icon name="zap" class="empty-state-icon" :size="28" :stroke="1.5" />
            <p :title="EMPTY_TITLE">No ephemeral queues yet</p>
          </div>

          <!-- The cap is stated, never silent: a cut list that does not say so
               reads as the whole answer. -->
          <div v-if="sortedRows.length > visible.length" class="pager eph-pager">
            <span class="pager-count">Showing the first {{ formatNumber(visible.length) }} of {{ formatNumber(sortedRows.length) }} · search to narrow</span>
          </div>
        </div>
      </template>
    </template>

    <!-- ===================== Confirm =====================
         One dialog for both verbs, restating the loss with the actual count.
         There is no undo and no dead letter behind this class, so the
         sentence is read before the button is reachable. -->
    <Teleport to="body">
      <div v-if="pending" class="modal-backdrop" @click.self="closeModal">
        <div class="card modal-card">
          <div class="card-header">
            <h3>{{ pending.kind === 'reset' ? 'Reset ephemeral queue' : 'Delete ephemeral queue' }}</h3>
          </div>
          <div class="card-body">
            <!-- The modal stays open on failure and says what happened: a closed
                 modal plus an unchanged list is indistinguishable from success. -->
            <div v-if="actionError" class="panel-err">{{ actionError }}</div>
            <p class="eph-modal-name">{{ pending.row.name }}</p>
            <p class="eph-modal-loss">{{ lossSentence }}</p>
            <p v-if="consequence" class="eph-modal-note">{{ consequence }}</p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="closeModal">Cancel</button>
            <button class="btn btn-danger" :disabled="acting" @click="runPending">
              {{ acting ? 'Working…' : (pending.kind === 'reset' ? 'Reset queue' : 'Delete queue') }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
  </SubSidebar>
</template>

<script setup>
// Ephemeral queues — the RAM storage class (EPHEMERAL_QUEUES.md §5.3).
//
// WHAT THIS PAGE MAY RENDER, AND WHY IT IS SHORTER THAN THE DURABLE ONE.
// The columns are the class's own truth: depth is the ring length, bytes is
// what the ring holds, a group's lag is head − cursor, and a drop is a message
// that left without being consumed (bounds, ttl, or retry exhaustion). There is
// deliberately NO pending, NO retained, NO dead-letter and NO lag derived from
// the durable log here: those concepts have no referent when the contents
// survive nothing (§1.2), and borrowing a durable column would be inventing a
// number.
//
// THE PICKED QUEUE IS READ BY THIS PAGE, NOT BY THE STORE. Its groups and
// partitions come from the per-queue route, which answers 404
// `ephemeral_queue_not_found` for a queue that was deleted or collected —
// and the store's fetchDepth reads every 404 as "this broker has no ephemeral
// queues at all", which would put the whole page in its quiet state over one
// vanished inbox. So the page asks the route itself and reads that answer as
// what it is: the queue is gone.
//
// POLLING. Both status routes read in-process gauges and touch no storage
// (§6), which is why this view carries its own 2s ticker instead of the shell's
// 30s one — the house rule against private intervals exists because a poll
// costs the tenant a metered, rate-limited request that reads the store, and
// here it costs none of the three. The ticker still pauses while the tab is hidden, is
// cleared on unmount, and stops entirely once the family answers a stable
// verdict (see stores/ephemeralStore.js).
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import EphemeralSide from '@/components/EphemeralSide.vue'
import Icon from '@/components/Icon.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import UiCheck from '@/components/UiCheck.vue'
import { ephemeral as ephemeralApi, describeApiError } from '@/api'
import { formatBytes, formatNumber, toNum } from '@/composables/useApi'
import {
  describeDrops, dropTotal, isQueueGone, matchesSearch, partitionRows, tierWord,
} from '@/composables/useEphemeralSide'
import { formatTimestampTime, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { useRouteState } from '@/composables/useRouteState'
import { lossSeverity } from '@/composables/useSeverity'
import { useToast } from '@/composables/useToast'
import { normalizeEphemeralQueue, useEphemeralStore } from '@/stores/ephemeralStore'
import { useIdentity } from '@/stores/identity'

const route = useRoute()
const router = useRouter()
const { can, epoch } = useIdentity()
const { notifySuccess } = useToast()

const store = useEphemeralStore()
const {
  queues: rows, loading, error, lastFetched, support, isStale, totals,
  fetchQueues, invalidate,
} = store

const POLL_MS = 2000
const POLL_NOTE =
  'The status routes read in-process gauges and touch no storage, so this page polls every 2s'
// A tenant can hold thousands of ephemeral inboxes (§1.1). Render a bounded
// slice and say so, rather than paint 5 000 rows every two seconds.
const MAX_ROWS = 200

const STORAGE_TITLE =
  'Contents live in broker RAM and survive nothing: not a restart, not a crash, not a deploy, not an ownership move'
const MESSAGES_TITLE = 'Messages in broker RAM right now: the ring length'
const DROPPED_TITLE = 'Messages that left without being consumed: bounds, ttl and retry exhaustion together'
const TIER_TITLE =
  'Declared: configured with configure, the configuration is durable, the contents are not. ' +
  'Implicit: created by the first push or pop that named it, collected once empty and idle'
const CURSOR_TITLE = 'Where this group has read up to in the ring'
const LAG_TITLE = 'Head − cursor: the messages this group has not read yet'
const SKIPPED_TITLE = 'Messages evicted by ttl or bounds before this group reached them'
const RESET_TITLE = 'Drop every message, void the leases and rewind every group; the queue stays'
const DELETE_TITLE = 'Drop every message and remove the queue'
const GONE_TITLE = 'It was deleted, or collected once empty and idle.'
const EMPTY_TITLE =
  'Ephemeral queues keep their messages in broker RAM and survive nothing. One appears here the moment a ' +
  'push or a pop names it, or when a client declares it with configure.'
const ABSENT_TITLE =
  '/api/v1/ephemeral/* arrived in 1.1. The broker on this cell, or the proxy in front of it, does not serve ' +
  'the family, so there is nothing to list. Durable queues are unaffected.'
const GATED_TITLE =
  'The broker serves the family but this cluster\'s plan does not grant it. An administrator has to enable ' +
  'ephemeral queues before anything can be pushed to or listed from them.'

// The list's search, shared with the table below: local, so typing in the
// list's sheet on a phone does not navigate (which would close the sheet).
const find = ref('')
// The queue picked in the list, in the address.
const picked = ref(null)
useRouteState({ name: picked })
const sortBy = ref('depth')
const declaredOnly = ref(false)
const sortOptions = [
  { value: 'depth', label: 'Messages' },
  { value: 'bytes', label: 'Size' },
  { value: 'name', label: 'Name' },
]

const pending = ref(null)
const actionError = ref(null)
const acting = ref(false)

// ---------------------------------------------------------------------------
// Rendering helpers. null is '—' everywhere: on a class whose whole point is
// that messages disappear, "the broker did not report it" and "it is zero" are
// different facts and only one of them is good news.
// ---------------------------------------------------------------------------
const num = (v) => (v === null || v === undefined ? '—' : formatNumber(v))
const bytes = (v) => (v === null || v === undefined ? '—' : formatBytes(v))
const tierTitle = (declared) => (declared === true
  ? 'Declared with configure: the configuration is durable, the contents are not'
  : declared === false ? 'Created by the first push or pop that named it; collected once empty and idle' : '')

// A ring that drops is a ring doing what it was configured to do: bounds and
// ttl are the POLICY of an ephemeral queue, and a count of drops carries no
// verdict without knowing what the queue was asked to hold. So a non-zero
// count is shown in ink, and never in amber, which would mark every
// long-running ephemeral queue as degraded.
//
// The per-group column is different: `skipped` is what THIS reader missed,
// and it has a denominator — `cursor`, the number of messages it did read.
// Loss becomes attention once it is a real share of that.
const skippedClass = (g) => lossSeverity({ dropped: g.skipped, delivered: g.cursor })

const loaded = computed(() => lastFetched.value > 0 || rows.value.length > 0)
const firstLoad = computed(() => loading.value && lastFetched.value === 0)
const lastFetchedText = computed(() => (lastFetched.value ? formatTimestampTime(lastFetched.value) : 'an earlier load'))
// Fed by the LAST GOOD load, not by the last attempt: a tick that resets on a
// failed poll claims freshness it does not have.
const refreshAgo = useRefreshAgo(lastFetched)

// §5.1: reset / delete are write-authz on this family (produce ‖ consume keys,
// users that are not Viewer) — not the durable queueAdmin. Mirroring the
// proxy's rule here is what keeps "may I show this?" and "will the call be
// authorized?" from drifting apart.
const canMutate = computed(() => can('produce') || can('consume'))

const showOwner = computed(() => rows.value.some((q) => !!q.owner))
const colCount = computed(() => 6 + (showOwner.value ? 1 : 0) + (canMutate.value ? 1 : 0))

const sortedRows = computed(() => {
  const list = rows.value.filter((q) => {
    if (declaredOnly.value && q.declared !== true) return false
    return matchesSearch(q.name, find.value)
  })
  const desc = (a, b) => (b ?? -1) - (a ?? -1)
  return [...list].sort((a, b) => {
    if (sortBy.value === 'name') return a.name.localeCompare(b.name)
    if (sortBy.value === 'bytes') return desc(a.bytes, b.bytes) || a.name.localeCompare(b.name)
    return desc(a.depth, b.depth) || a.name.localeCompare(b.name)
  })
})
const visible = computed(() => sortedRows.value.slice(0, MAX_ROWS))

// A pick is a step the Back button undoes, as a pick in the list is.
const allLocation = computed(() => ({ path: route.path, query: { ...route.query, name: undefined } }))
const pick = (name) => { router.push({ path: route.path, query: { ...route.query, name } }) }

// ---------------------------------------------------------------------------
// The picked queue: its row in the list, and its groups and partitions from
// its own route, re-read on every tick while it is picked.
// ---------------------------------------------------------------------------
const pickedRow = computed(() => (picked.value ? rows.value.find((q) => q.name === picked.value) || null : null))

const detail = ref(null)            // { name, queue (normalized), partitions } of the picked queue
const detailError = ref(null)
const detailState = computed(() => {
  if (detail.value && detail.value.name === picked.value) return 'ready'
  if (detailError.value) return 'failed'
  return 'reading'
})
const detailErrorText = computed(() => (detailError.value ? describeApiError(detailError.value) : ''))

/** The groups: the per-queue route's when it carries them, else the list's,
 *  else null, which renders "not reported", never "none". */
const pickedGroups = computed(() => {
  const d = detail.value && detail.value.name === picked.value ? detail.value.queue : null
  if (d && d.groups) return d.groups
  return pickedRow.value ? pickedRow.value.groups : null
})
const pickedPartitions = computed(() => (detail.value && detail.value.name === picked.value ? detail.value.partitions : null))

const loadDetail = async (name) => {
  const at = epoch.value
  try {
    const r = await ephemeralApi.depth(name, { probe: true })
    if (epoch.value !== at || picked.value !== name) return
    detail.value = { name, queue: normalizeEphemeralQueue(r.data, name), partitions: partitionRows(r.data) }
    detailError.value = null
  } catch (err) {
    if (epoch.value !== at || picked.value !== name) return
    // Gone between the list and this call: the next list read drops the row
    // and the page says so. Anything else stays an inline failure here.
    if (isQueueGone(err)) { detail.value = null; detailError.value = null; return }
    detailError.value = err
  }
}

watch(picked, (name) => {
  detail.value = null
  detailError.value = null
  if (name && pickedRow.value) loadDetail(name)
})
watch(epoch, () => { detail.value = null; detailError.value = null })

// The head's facts: the class's totals with nothing picked, the queue's own
// figures with one — the figures the list and the table show, no read of
// their own.
const facts = computed(() => {
  if (support.value === 'absent' || support.value === 'gated' || !loaded.value) return []
  if (picked.value) {
    const q = pickedRow.value
    if (!q) return []
    const dropped = dropTotal(q.drops)
    return [
      { k: 'Tier', v: tierWord(q.declared), title: tierTitle(q.declared) || STORAGE_TITLE },
      { k: 'Messages', v: num(q.depth), title: `${MESSAGES_TITLE}. ${STORAGE_TITLE}` },
      { k: 'Size', v: bytes(q.bytes), title: 'Bytes the ring holds' },
      q.partitions !== null ? { k: 'Partitions', v: num(q.partitions) } : null,
      { k: 'Dropped', v: num(dropped), title: `${DROPPED_TITLE}: ${describeDrops(q.drops)}` },
      // Only where the broker reports them.
      q.head !== null ? { k: 'Head', v: num(q.head), title: 'The ring head: a group\'s lag is head − cursor' } : null,
      q.owner ? { k: 'Owner', v: String(q.owner), title: 'The broker that holds this queue\'s ring' } : null,
    ].filter(Boolean)
  }
  const t = totals.value
  return [
    { k: 'Storage', v: 'RAM', title: STORAGE_TITLE },
    { k: 'Queues', v: formatNumber(rows.value.length), title: 'Every ephemeral queue on this cell, whatever the filters say' },
    { k: 'Messages', v: num(t.depth), title: MESSAGES_TITLE },
    { k: 'Size', v: bytes(t.bytes), title: 'Bytes every ring holds' },
    { k: 'Dropped', v: num(t.drops), title: DROPPED_TITLE },
  ]
})

// ---------------------------------------------------------------------------
// Loading + the private 2s ticker
// ---------------------------------------------------------------------------
const refreshAll = async () => {
  await fetchQueues({ force: true })
  if (picked.value && pickedRow.value) await loadDetail(picked.value)
}

let ticker = null
const stopTicker = () => {
  if (ticker !== null) { clearInterval(ticker); ticker = null }
}
const startTicker = () => {
  stopTicker()
  ticker = setInterval(() => {
    // Hidden tabs poll nothing, exactly as the shell's own ticker does.
    if (document.hidden) return
    refreshAll()
  }, POLL_MS)
}

// A stable verdict cannot change by asking again — stop the clock rather than
// spin against a broker that will keep answering the same 404.
watch(support, (v) => { if (v === 'absent' || v === 'gated') stopTicker() })

const probeAgain = async () => {
  await fetchQueues({ probe: true })
  if (support.value === 'live') startTicker()
}

// Registered NON-auto: the header's Refresh button and a cluster switch drive
// it, while the cadence above stays this view's own.
useRefresh(refreshAll)

onMounted(() => {
  refreshAll()
  startTicker()
})
onUnmounted(stopTicker)

// ---------------------------------------------------------------------------
// Reset / delete — both destroy contents, so both restate the contract first
// ---------------------------------------------------------------------------
const ask = (kind, row) => {
  pending.value = { kind, row }
  actionError.value = null
}
const closeModal = () => {
  pending.value = null
  actionError.value = null
}

const lossSentence = computed(() => {
  const p = pending.value
  if (!p) return ''
  const n = p.row.depth
  // The count must be the truth or absent — "all null messages" is worse than
  // naming the queue and saying everything on it.
  const what = n === null
    ? 'every message currently on it'
    : `all ${formatNumber(n)} message${n === 1 ? '' : 's'}`
  return p.kind === 'reset'
    ? `This drops ${what} immediately; ephemeral contents are not recoverable.`
    : `This drops ${what} immediately and removes the queue; ephemeral contents are not recoverable.`
})

const consequence = computed(() => {
  const p = pending.value
  if (!p) return ''
  if (p.kind === 'reset') {
    return 'Leases are voided and every group cursor rewinds. The queue itself, and its declared configuration, stay.'
  }
  return p.row.declared === true
    ? 'The declared configuration is deleted too: a later push recreates the queue implicitly, with tenant defaults.'
    : 'A later push or pop with the same name creates the queue again, empty.'
})

const runPending = async () => {
  const p = pending.value
  if (!p || acting.value) return
  const name = p.row.name
  acting.value = true
  actionError.value = null
  try {
    if (p.kind === 'reset') {
      const res = await ephemeralApi.reset(name)
      const dropped = toNum(res.data?.dropped)
      notifySuccess(
        dropped === null
          ? `Reset ${name}`
          : `Reset ${name}: dropped ${formatNumber(dropped)} message${dropped === 1 ? '' : 's'}`
      )
    } else {
      const res = await ephemeralApi.delete(name)
      // An explicit `existed:false` means the name matched nothing. Reporting
      // that as a deletion is how a typo reads as success.
      if (res.data && res.data.existed === false) {
        actionError.value = `No ephemeral queue named "${name}" on this cluster: nothing was deleted.`
        return
      }
      notifySuccess(`Deleted ${name}`)
      // The queue on the right is gone: back to every queue.
      if (picked.value === name) picked.value = null
    }
    closeModal()
    invalidate()
    await refreshAll()
  } catch (err) {
    // Already on the global surface; this drives the inline state.
    actionError.value = describeApiError(err)
  } finally {
    acting.value = false
  }
}
</script>

<style scoped>
/* The head's facts: small chips beside the title, as in the Queues section
   (the same 16px after the title). On a narrow screen they wrap under the
   title rather than being cut. */
.eph :deep(.page-id) { flex-wrap: wrap; gap: 6px 16px; }
.eph :deep(.page-sub) { overflow: visible; white-space: normal; }
.facts { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.fact {
  display: inline-flex; align-items: baseline; gap: 5px;
  padding: 2px 8px; border-radius: var(--r-chip);
  background: var(--ink-3); border: 1px solid var(--bd);
  font-size: 11.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap;
}
.fact-k { color: var(--text-low); }

/* Back to every queue, before the picked queue's name: 8px from it, the
   head's 16px being the title's distance from its facts. */
.eph-back {
  flex: none; width: 28px; height: 28px; margin-right: -8px;
  display: inline-flex; align-items: center; justify-content: center;
  border-radius: var(--r-control); color: var(--text-mid);
}
.eph-back:hover { background: var(--ink-3); color: var(--text-hi); }
.eph-back:focus-visible { outline: 2px solid var(--ring); outline-offset: 1px; }

.view-container.eph > .card + .card,
.view-container.eph > section + section { margin-top: 16px; }

/* Tables scroll inside their card on a narrow screen; one line per row. */
.eph-scroll { overflow-x: auto; }
.eph-table th, .eph-table td { white-space: nowrap; }
.eph-table td { height: 36px; }
.eph-row { cursor: pointer; }
.eph-row:focus-visible { outline: 2px solid var(--ring); outline-offset: -2px; }
.eph-name {
  display: block; max-width: 480px;
  overflow: hidden; text-overflow: ellipsis;
  font-family: var(--font-mono); font-size: 12px; color: var(--text-hi);
}
.eph-tier { color: var(--text-mid); }
.eph-owner { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); }
.eph-table td.num.mute { color: var(--text-low); }
.eph-acts { width: 1%; padding-top: 0; padding-bottom: 0; }
.eph-acts .btn { color: var(--text-low); }
.eph-acts .btn:hover { color: var(--text-hi); }
.eph-acts .btn + .btn { margin-left: 2px; }
.eph-sr {
  position: absolute; width: 1px; height: 1px; overflow: hidden;
  clip: rect(0 0 0 0); white-space: nowrap;
}

.eph-reading { display: flex; flex-direction: column; gap: 8px; padding: 14px 16px; }
.eph-skeleton { height: 16px; }
.eph-empty { padding: 28px 16px; }
.eph-pager .pager-count { font-size: 12px; }

.eph-modal-name { margin-bottom: 10px; font-family: var(--font-mono); font-size: 12.5px; color: var(--text-hi); overflow-wrap: anywhere; }
.eph-modal-loss { margin-bottom: 8px; font-size: 13px; color: var(--text-hi); }
.eph-modal-note { font-size: 12.5px; color: var(--text-mid); }
</style>
