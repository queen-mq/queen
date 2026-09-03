<template>
  <div class="view-container">

    <!-- Tenant scope, built from identity and never from the fetch — same
         strip as Queues, because these queues belong to the acting tenant on
         this cell too. The class chip is not decoration: everything below is
         RAM, and the page has to say so before it says anything else. -->
    <div class="scope-strip">
      <span class="chip chip-mute">tenant scope</span>
      <span class="scope-text">
        <strong>{{ actingTenantSlug || 'no tenant' }}</strong>
        <span class="scope-sep">/</span>{{ actingClusterSlug || 'no cluster' }}
        <span class="scope-sep">·</span>cell {{ actingCellSlug || 'unknown' }}
      </span>
      <span class="scope-fill"></span>
      <router-link to="/queues" class="chip chip-mute" style="text-decoration:none;">durable queues →</router-link>
      <span
        class="chip chip-ice"
        title="Contents live in broker RAM and survive nothing — not a restart, not a crash, not a deploy, not an ownership move"
      >RAM class</span>
    </div>

    <!-- ===================== Not exposed here =====================
         404 (broker older than 1.1), 404 route_blocked (proxy does not
         classify the family), or the SPA fallback answering an API path. All
         three are facts about this cell, not failures of it: one quiet state,
         and the store stops asking so the poll cannot become a toast loop. -->
    <div v-if="support === 'absent'" class="card">
      <div class="empty-state">
        <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 7h16M4 12h16M4 17h16" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M3 3l18 18" />
        </svg>
        <h3>This broker does not expose ephemeral queues</h3>
        <p>
          <code>/api/v1/ephemeral/*</code> arrived in 1.1. The broker on this
          cell — or the proxy in front of it — does not serve the family, so
          there is nothing to list. Durable queues are unaffected.
        </p>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <!-- ===================== Exposed, not granted =====================
         403 feature_gated: the routes exist, this cluster's plan does not
         include the class (§5.1). Also a stable answer — also asked once. -->
    <div v-else-if="support === 'gated'" class="card">
      <div class="empty-state">
        <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <rect x="5" y="11" width="14" height="9" rx="2" />
          <path stroke-linecap="round" d="M8 11V8a4 4 0 018 0v3" />
        </svg>
        <h3>Ephemeral queues are not enabled for this cluster</h3>
        <p>
          The broker serves the family but the plan on this cluster does not
          grant it. An administrator has to enable ephemeral queues before
          anything can be pushed to or listed from them.
        </p>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <template v-else>
      <!-- Stale data presented as live is the failure mode: the store keeps the
           last rows that loaded, so say how old they are and why. -->
      <div v-if="isStale" class="status-banner banner-bad view-banner">
        <span :title="formatTimestampUtc(lastFetched)">
          <strong>Could not refresh the ephemeral queues</strong> ·
          {{ describeApiError(error) }} · showing the last rows that loaded
          ({{ lastFetchedText }}), which on a RAM class may already be gone.
        </span>
      </div>

      <!-- Totals for what is on screen. Every figure here is a live gauge; a
           null anywhere renders '—' rather than collapsing into a zero. -->
      <div class="counts-strip">
        <!-- Class-wide, never filtered: this strip is the tenant's ephemeral
             footprint on the cell, and the table below says separately how many
             of those rows it is currently showing. -->
        <div class="counts-group">
          <span class="count-item count-static" title="Every ephemeral queue this tenant holds on the cell, whatever the filters below say">
            <span class="count-item-label">queues</span>
            <strong>{{ formatNumber(rows.length) }}</strong>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static" title="Ring length across every ephemeral queue">
            <span class="count-item-label">in ram</span>
            <strong>{{ num(totals.depth) }}</strong>
            <span class="count-suffix">msgs</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static">
            <span class="count-item-label">bytes</span>
            <strong>{{ bytes(totals.bytes) }}</strong>
          </span>
          <span class="count-sep">·</span>
          <span
            class="count-item count-static count-muted"
            title="Messages that left without being consumed — bounds, ttl and retry exhaustion together"
          >
            <span class="count-item-label">dropped</span>
            <strong>{{ num(totals.drops) }}</strong>
          </span>
        </div>
        <div class="counts-group counts-group-right">
          <span class="live-tick" :title="POLL_NOTE">
            <span class="pulse" />
            <span>live · {{ refreshAgo }}</span>
          </span>
        </div>
      </div>

      <!-- Filters. High queue cardinality is the point of this class (thousands
           of req/reply inboxes), so search and an explicit order are not a
           convenience here — they are how the page stays readable. -->
      <div class="card filters">
        <div class="card-body filter-rows">
          <div class="filter-row">
            <div class="filter-search">
              <svg class="filter-search-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
              </svg>
              <input v-model="searchQuery" type="text" placeholder="Search ephemeral queues..." class="input" />
            </div>

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

            <div class="filter-field">
              <label class="filter-check">
                <input v-model="declaredOnly" type="checkbox" />
                Declared only
              </label>
            </div>
          </div>
        </div>
      </div>

      <!-- ===================== The table ===================== -->
      <div class="card">
        <div class="card-header">
          <h3>Ephemeral queues</h3>
          <span class="card-sub">depth is the ring — no pending, no retained, no dead letters on this class</span>
          <span class="muted">{{ visible.length }} of {{ formatNumber(rows.length) }}</span>
        </div>

        <div style="overflow-x:auto;">
          <table class="t">
            <thead>
              <tr>
                <th>Queue</th>
                <th>Tier</th>
                <th class="num">Depth</th>
                <th class="num">Bytes</th>
                <th class="num">Groups</th>
                <th>Drops</th>
                <th v-if="showOwner">Owner</th>
                <th v-if="canMutate" style="text-align:right;">Actions</th>
              </tr>
            </thead>
            <tbody>
              <!-- First paint only: a 2s poll must never blank the rows the
                   operator is reading. -->
              <template v-if="firstLoad">
                <tr v-for="i in 6" :key="`sk-${i}`">
                  <td :colspan="colCount"><div class="skeleton" style="height:16px;" /></td>
                </tr>
              </template>

              <template v-else-if="visible.length">
                <template v-for="row in visible" :key="row.name">
                  <tr class="eph-row" @click="toggleRow(row.name)">
                    <td>
                      <div style="display:flex; align-items:center; gap:7px;">
                        <svg
                          class="eph-caret" :class="{ open: isExpanded(row.name) }"
                          width="10" height="10" viewBox="0 0 24 24" fill="none"
                          stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"
                        ><polyline points="9 6 15 12 9 18" /></svg>
                        <span class="font-mono" style="font-size:12px;">{{ row.name }}</span>
                      </div>
                    </td>
                    <td>
                      <!-- Explicit true / explicit false only. A payload that
                           carries neither tells us nothing, and a badge that
                           guesses is worse than no badge. -->
                      <span v-if="row.declared === true" class="chip chip-ice" title="Declared with configure — the configuration is durable, the contents are not">declared</span>
                      <span v-else-if="row.declared === false" class="chip chip-mute" title="Created by the first push or pop that named it; collected once empty and idle">implicit</span>
                      <span v-else class="num mute">—</span>
                    </td>
                    <td class="num">{{ num(row.depth) }}</td>
                    <td class="num">{{ bytes(row.bytes) }}</td>
                    <td class="num">{{ num(row.groupCount) }}</td>
                    <td>
                      <span class="eph-drops">
                        <span :class="dropClass(row.drops.bounds)">{{ num(row.drops.bounds) }}<i>bounds</i></span>
                        <span :class="dropClass(row.drops.ttl)">{{ num(row.drops.ttl) }}<i>ttl</i></span>
                        <span :class="dropClass(row.drops.retry)">{{ num(row.drops.retry) }}<i>retry</i></span>
                      </span>
                    </td>
                    <td v-if="showOwner" class="font-mono" style="font-size:11.5px; color:var(--text-mid);">
                      {{ row.owner || '—' }}
                    </td>
                    <td v-if="canMutate" style="text-align:right; white-space:nowrap;" @click.stop>
                      <button class="btn btn-ghost" style="padding:4px 10px; font-size:11px;" @click="ask('reset', row)">Reset</button>
                      <button class="btn btn-danger" style="padding:4px 10px; font-size:11px; margin-left:6px;" @click="ask('delete', row)">Delete</button>
                    </td>
                  </tr>

                  <!-- Expanded: one cursor per group over the ONE ring (§1.5).
                       lag is head − cursor, and it is the only lag this class
                       has — nothing here is read from Postgres. -->
                  <tr v-if="isExpanded(row.name)">
                    <td :colspan="colCount" style="background:var(--recessed); padding:12px 16px;">
                      <div class="eph-detail-meta">
                        <span v-if="row.partitions !== null"><b>{{ num(row.partitions) }}</b> partition{{ row.partitions === 1 ? '' : 's' }}</span>
                        <span v-if="row.head !== null">ring head <b class="font-mono">{{ num(row.head) }}</b></span>
                        <span v-if="row.owner">owner <b class="font-mono">{{ row.owner }}</b></span>
                        <span class="eph-detail-note">contents survive nothing — this is a snapshot of RAM</span>
                      </div>

                      <table v-if="groupsFor(row) && groupsFor(row).length" class="t eph-groups">
                        <thead>
                          <tr>
                            <th>Group</th>
                            <th class="num">Cursor</th>
                            <th class="num">Lag</th>
                            <th class="num">Skipped</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr v-for="g in groupsFor(row)" :key="g.name">
                            <td class="font-mono" style="font-size:12px;">{{ g.name }}</td>
                            <td class="num">{{ num(g.cursor) }}</td>
                            <td class="num">{{ num(g.lag) }}</td>
                            <td class="num" :class="{ warn: (g.skipped || 0) > 0 }" :title="g.skipped ? 'Messages evicted by ttl or bounds before this group reached them' : ''">
                              {{ num(g.skipped) }}
                            </td>
                          </tr>
                        </tbody>
                      </table>

                      <div v-else-if="groupsFor(row)" class="empty-tile">
                        No group has read this queue yet — the ring has no cursors on it.
                      </div>

                      <div v-else class="panel-na">
                        This broker does not report per-group cursors for ephemeral queues.
                      </div>
                    </td>
                  </tr>
                </template>
              </template>

              <!-- A failure with nothing loaded is not an empty class. -->
              <tr v-else-if="error">
                <td :colspan="colCount">
                  <div class="empty-state empty-state-failed">
                    <h3>Cannot list ephemeral queues</h3>
                    <p>{{ describeApiError(error) }} — nothing loaded, which is not the same as nothing existing.</p>
                    <button class="btn btn-ghost" @click="refreshAll">Retry</button>
                  </div>
                </td>
              </tr>

              <tr v-else-if="rows.length">
                <td :colspan="colCount">
                  <div class="empty-state">
                    <h3>No queue matches these filters</h3>
                    <p>{{ formatNumber(rows.length) }} ephemeral queue{{ rows.length === 1 ? '' : 's' }} are live on this cell.</p>
                  </div>
                </td>
              </tr>

              <!-- Empty, and it loaded: say what this class IS, because an
                   empty table teaches nobody what would fill it. -->
              <tr v-else>
                <td :colspan="colCount">
                  <div class="empty-state">
                    <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                      <path stroke-linecap="round" stroke-linejoin="round" d="M4 7h16M4 12h16M4 17h10" />
                      <circle cx="18" cy="17" r="2.5" />
                    </svg>
                    <h3>No ephemeral queues on this cell</h3>
                    <p>
                      Ephemeral queues keep their messages in broker RAM and
                      survive nothing — not a restart, not a crash, not a deploy,
                      not an ownership move. One appears here the moment a push
                      or a pop names it, or when a client declares it with
                      <code>configure</code>.
                    </p>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- The cap is stated, never silent: a truncated list that does not
             say it is truncated reads as the whole answer. -->
        <div v-if="sortedRows.length > visible.length" class="pager">
          <span class="pager-count">
            Showing the first {{ visible.length }} of {{ formatNumber(sortedRows.length) }} matching queues — narrow with search to see the rest.
          </span>
        </div>
      </div>
    </template>

    <!-- ===================== Confirm =====================
         One dialog for both verbs, and it restates the loss contract with the
         actual count in it. There is no undo and no DLQ behind this class, so
         the sentence has to be read before the button is reachable. -->
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

            <p style="color:var(--text-mid); margin-bottom:10px;">
              <strong class="font-mono">{{ pending.row.name }}</strong>
            </p>
            <p style="color:var(--text-hi); margin-bottom:8px;">{{ lossSentence }}</p>
            <p v-if="consequence" style="font-size:13px; color:var(--text-low);">{{ consequence }}</p>
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
</template>

<script setup>
// Ephemeral queues — the RAM storage class (EPHEMERAL_QUEUES.md §5.3).
//
// WHAT THIS PAGE MAY RENDER, AND WHY IT IS SHORTER THAN THE DURABLE ONE.
// The columns are the class's own truth: depth is the ring length, bytes is
// what the ring holds, a group's lag is head − cursor, and a drop is a message
// that left without being consumed (bounds, ttl, or retry exhaustion). There is
// deliberately NO pending, NO retained, NO dead-letter and NO PG-derived lag
// here: those concepts have no referent when the contents survive nothing
// (§1.2), and borrowing a durable column would be inventing a number.
//
// POLLING. Both status routes read in-process gauges and touch no database
// (§6), which is why this view carries its own 2s ticker instead of the shell's
// 30s one — the house rule against private intervals exists because a poll
// costs the tenant a metered, rate-limited, DB-touching request, and here it
// costs none of the three. The ticker still pauses while the tab is hidden, is
// cleared on unmount, and stops entirely once the family answers a stable
// verdict (see stores/ephemeralStore.js).
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'

import { ephemeral as ephemeralApi, describeApiError } from '@/api'
import { formatBytes, formatNumber, toNum } from '@/composables/useApi'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { useToast } from '@/composables/useToast'
import { useEphemeralStore } from '@/stores/ephemeralStore'
import { useIdentity } from '@/stores/identity'

const { can, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
const { notifySuccess } = useToast()

const store = useEphemeralStore()
const {
  queues: rows, loading, error, lastFetched, support, isStale, totals,
  fetchQueues, fetchDepth, invalidate,
} = store

const POLL_MS = 2000
const POLL_NOTE =
  'The status routes read in-process gauges — no database is touched, so this page polls every 2s'
// A tenant can hold thousands of ephemeral inboxes (§1.1). Render a bounded
// slice and say so, rather than paint 5 000 rows every two seconds.
const MAX_ROWS = 200

const searchQuery = ref('')
const sortBy = ref('depth')
const declaredOnly = ref(false)
const sortOptions = [
  { value: 'depth', label: 'Depth' },
  { value: 'bytes', label: 'Bytes' },
  { value: 'name', label: 'Name' },
]

// Per-row expand state — the Set-based pattern Dashboard and QueueDetail use.
const expanded = ref(new Set())
// name → normalized row from the per-queue depth route, or null when that call
// failed. The list is the table; this is the authority for the open row.
const depthByQueue = ref(new Map())

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
const dropClass = (v) => (v ? 'eph-drop on' : 'eph-drop')

const firstLoad = computed(() => loading.value && lastFetched.value === 0)
const lastFetchedText = computed(() =>
  lastFetched.value ? formatTimestamp(lastFetched.value) : 'an earlier load'
)
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
    if (!searchQuery.value) return true
    return q.name.toLowerCase().includes(searchQuery.value.toLowerCase())
  })
  const desc = (a, b) => (b ?? -1) - (a ?? -1)
  return [...list].sort((a, b) => {
    if (sortBy.value === 'name') return a.name.localeCompare(b.name)
    if (sortBy.value === 'bytes') return desc(a.bytes, b.bytes) || a.name.localeCompare(b.name)
    return desc(a.depth, b.depth) || a.name.localeCompare(b.name)
  })
})
const visible = computed(() => sortedRows.value.slice(0, MAX_ROWS))

// ---------------------------------------------------------------------------
// Expand — per-group cursors for one queue
// ---------------------------------------------------------------------------
const isExpanded = (name) => expanded.value.has(name)

/** The open row's groups: the per-queue route when it carries them, else what
 *  the listing carried, else null — which renders "not reported", not "none". */
const groupsFor = (row) => {
  const detail = depthByQueue.value.get(row.name)
  if (detail && detail.groups) return detail.groups
  return row.groups
}

const loadDepth = async (name) => {
  const detail = await fetchDepth(name)
  const next = new Map(depthByQueue.value)
  next.set(name, detail)
  depthByQueue.value = next
}

const toggleRow = (name) => {
  const next = new Set(expanded.value)
  if (next.has(name)) {
    next.delete(name)
    const map = new Map(depthByQueue.value)
    map.delete(name)
    depthByQueue.value = map
  } else {
    next.add(name)
    loadDepth(name)
  }
  expanded.value = next
}

/** Only the open rows are re-read; a collapsed row costs nothing. */
const refreshExpanded = async () => {
  const names = [...expanded.value].filter((n) => rows.value.some((q) => q.name === n))
  if (!names.length) return
  await Promise.all(names.map(loadDepth))
}

// ---------------------------------------------------------------------------
// Loading + the private 2s ticker
// ---------------------------------------------------------------------------
const refreshAll = async () => {
  await fetchQueues({ force: true })
  await refreshExpanded()
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
    ? 'The declared configuration is deleted too — a later push recreates the queue implicitly, with tenant defaults.'
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
          : `Reset ${name} — dropped ${formatNumber(dropped)} message${dropped === 1 ? '' : 's'}`
      )
    } else {
      const res = await ephemeralApi.delete(name)
      // An explicit `existed:false` means the name matched nothing. Reporting
      // that as a deletion is how a typo reads as success.
      if (res.data && res.data.existed === false) {
        actionError.value = `No ephemeral queue named "${name}" on this cluster — nothing was deleted.`
        return
      }
      const next = new Set(expanded.value)
      next.delete(name)
      expanded.value = next
      notifySuccess(`Deleted ${name}`)
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
/* The caret in the queue cell — the row is the toggle, so the glyph only has
   to say which way it is. */
.eph-caret {
  color: var(--text-faint);
  flex-shrink: 0;
  transition: transform .12s var(--ease), color .12s;
}
.eph-caret.open { transform: rotate(90deg); color: var(--text-mid); }
.eph-row { cursor: pointer; }
.eph-row:hover .eph-caret { color: var(--text-mid); }

/* Three drop counters in one cell: the label rides under the figure so the
   column stays one line wide and no reader has to remember a column order. */
.eph-drops { display: inline-flex; gap: 12px; }
.eph-drop {
  display: inline-flex; flex-direction: column; line-height: 1.15;
  font-family: var(--font-mono); font-variant-numeric: tabular-nums;
  font-size: 12px; color: var(--text-low);
}
.eph-drop i {
  font-style: normal; font-size: 9px; letter-spacing: .06em;
  text-transform: uppercase; color: var(--text-faint);
}
/* Amber only once something has actually been dropped: a zero is not a warning. */
.eph-drop.on { color: var(--warn-400); }

.eph-detail-meta {
  display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 16px;
  font-size: 11.5px; color: var(--text-low); margin-bottom: 10px;
}
.eph-detail-meta b { color: var(--text-hi); font-weight: 600; }
.eph-detail-note { color: var(--text-faint); font-style: italic; }

/* The group table is a table inside a row: no card, no outer border, and a
   head that reads as a sub-head rather than a second page header. */
.eph-groups { background: transparent; }
.eph-groups th { background: transparent; padding: 4px 10px; }
.eph-groups td { padding: 5px 10px; border-bottom: 1px solid var(--bd-soft, var(--bd)); }
.eph-groups tr:hover td { background: transparent; }
</style>
