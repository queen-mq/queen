<template>
  <!-- A tab of the Queues section, for the queue picked in the list beside
       (?queue=): timers are listed one queue at a time. -->
  <QueuesLayout>
  <div class="view-container tm">

    <!-- Not here / not in the plan / not served right now: one quiet card, no
         poll, no retry. composables/useGatedVerdict.js owns the rule and the
         words; stores/routeSupport.js remembers it for the cluster epoch. -->
    <div v-if="quiet" class="card">
      <div class="empty-state">
        <Icon :name="verdict === 'absent' ? 'timer' : verdict === 'gated' ? 'lock' : 'pause'" class="empty-state-icon" :size="28" />
        <h3 :title="quietTitle">{{ quiet.title }}</h3>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <template v-else>
      <!-- The panel keeps the last page that loaded: stale rows must not pass
           for live ones. -->
      <div v-if="isStale" class="status-banner banner-bad view-banner">
        <span>
          <strong>Could not refresh these timers</strong> · {{ describeApiError(listError) }} · showing the page from {{ stamp(listPanel) }}
        </span>
      </div>

      <!-- The queue is the ADDRESS, not a filter: the route is queue-scoped
           because a tenant-wide timer list would be a scan (§4.1). -->
      <PageTools>
        <!-- The exact count. A whole-queue count is REFUSED by the stored
             procedure: an exact aggregate cannot have the list's LIMIT, so it is
             prefix-scoped to stay an index range (handlers/timers.rs). -->
        <label class="tool-field" for="timers-prefix" title="An exact count of the pending timers whose key starts with this. A whole-queue count would be a scan, so the broker asks for a prefix.">
          <span class="tool-label">Key prefix</span>
          <input
            id="timers-prefix"
            v-model="countPrefix"
            class="input tm-prefix"
            placeholder="e.g. retry:"
            @keyup.enter="runCount"
          />
        </label>
        <button
          class="btn btn-ghost"
          :disabled="!queue || !countPrefix || counting"
          :title="countPrefix ? 'Count every pending timer whose key starts with this' : 'The broker refuses a whole-queue count: give a prefix'"
          @click="runCount"
        >{{ counting ? 'Counting…' : 'Count' }}</button>
        <span v-if="countError" class="tool-note is-bad">{{ countError }}</span>
        <span v-else-if="countResult && countResult.count !== null" class="tool-note tabular-nums">{{ formatNumber(countResult.count) }} pending under <code>{{ countResult.prefix }}</code></span>
        <span v-else-if="countResult" class="tool-note">No count for <code>{{ countResult.prefix }}</code></span>
        <template #view>
          <!-- The TENANT's timer count, from the queue listing's root: a sweeper
               snapshot, hence `≈`; shown only when it is a measurement. -->
          <span v-if="tenantTotalMeasured" class="tool-note tabular-nums" :title="TENANT_TOTAL_TITLE">≈ {{ formatNumber(timerRows) }} in all queues</span>
          <UiSelect v-model="limit" label="Show" :options="LIMITS" />
        </template>
      </PageTools>

      <div class="card">
        <div class="card-header">
          <h3>Pending timers</h3>
          <span class="card-sub" title="A timer that has fired leaves no row behind: its message is in the log">not yet delivered</span>
          <span class="muted">{{ stamp(listPanel) }}</span>
        </div>

        <div class="tm-scroll">
          <table class="t tm-table">
            <thead>
              <tr>
                <th>Timer key</th>
                <th>Partition</th>
                <th>Delivers</th>
                <th class="r">Attempts</th>
                <th>Last error</th>
                <th>State</th>
                <th class="r">Created</th>
              </tr>
            </thead>
            <tbody>
              <template v-if="firstLoad">
                <tr v-for="i in 6" :key="`sk-${i}`" aria-hidden="true">
                  <td colspan="7"><div class="skeleton" style="height:14px;" /></td>
                </tr>
              </template>

              <template v-else-if="rows.length">
                <tr
                  v-for="row in rows"
                  :key="row.timerKey"
                  class="tm-row"
                  tabindex="0"
                  @click="openTimer(row)"
                  @keydown.enter.prevent="openTimer(row)"
                >
                  <td>
                    <span class="tm-key">
                      <span class="tm-cut tm-mono" :title="row.timerKey">{{ row.timerKey }}</span>
                      <span v-if="row.encrypted" class="tm-tag" title="Stored as an encrypted envelope: the payload is not readable from this console">encrypted</span>
                    </span>
                  </td>
                  <td><span class="tm-cut tm-mid" :title="row.partition || undefined">{{ row.partition || '—' }}</span></td>
                  <td :title="utcTitle(row.deliverAt)">
                    <span class="tm-due">{{ formatDeliverIn(row.deliverAt, asOf) }}</span>
                    <span class="tm-at">{{ compactStamp(row.deliverAt) }}</span>
                  </td>
                  <td class="r tm-num">{{ row.attempts ?? '—' }}</td>
                  <td>
                    <span v-if="row.lastError" class="tm-cut tm-error" :title="row.lastError">{{ row.lastError }}</span>
                    <span v-else class="tm-low">—</span>
                  </td>
                  <td>
                    <!-- `claimed` is the SP's one definition of "out of your
                         hands": a claim token that has not expired. A row in
                         BACKOFF reads claimed:false and is still cancellable. -->
                    <span v-if="row.claimed" class="tm-state" :title="CLAIMED_TITLE">Claimed</span>
                    <span v-else class="tm-state tm-low" title="No live claim: a cancel can still take it">Pending</span>
                  </td>
                  <td class="r tm-when" :title="utcTitle(row.createdAt)">{{ compactStamp(row.createdAt) }}</td>
                </tr>
              </template>

              <!-- No queue is not an empty queue, and the route cannot answer
                   without one. -->
              <tr v-else-if="!queue">
                <td colspan="7">
                  <div class="empty-state">
                    <Icon name="timer" class="empty-state-icon" :size="28" />
                    <h3>Pick a queue in the list</h3>
                  </div>
                </td>
              </tr>

              <tr v-else-if="listError">
                <td colspan="7">
                  <!-- The banner above speaks only over rows; here the reason
                       has nowhere else to be. -->
                  <div class="empty-state empty-state-failed">
                    <h3>Cannot load timers</h3>
                    <p>{{ describeApiError(listError) }}</p>
                    <button class="btn btn-ghost" @click="reload">Retry</button>
                  </div>
                </td>
              </tr>

              <tr v-else>
                <td colspan="7">
                  <div class="empty-state">
                    <Icon name="timer" class="empty-state-icon" :size="28" />
                    <h3 title="A timer that has fired leaves no row behind: its message is in the log">No pending timers{{ pageNumber > 1 ? ' on this page' : '' }}</h3>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Keyset pager: no page count, because there is no total without a
             count and this is an index range, not an offset. -->
        <div v-if="queue && (rows.length || canPrev)" class="pager">
          <span class="pager-count tabular-nums">
            Page {{ pageNumber }} · {{ rows.length }} {{ rows.length === 1 ? 'row' : 'rows' }}<template v-if="!canNext && !listError"> · the end</template>
          </span>
          <div class="pager-nav">
            <button class="btn btn-ghost" :disabled="!canPrev || loading" @click="goPrev">Previous</button>
            <button class="btn btn-ghost" :disabled="!canNext || loading" @click="goNext">Next</button>
          </div>
        </div>
      </div>
    </template>

    <!-- ===================== Peek ===================== -->
    <DetailDrawer
      :open="Boolean(selected)"
      title="Timer"
      :subtitle="selected?.timerKey || ''"
      wide
      :split="Boolean(peeked && peeked.found !== false)"
      @close="closeDrawer"
    >
      <div v-if="peekLoading" class="tm-loading">
        <div class="spinner" />
        <p>Loading the timer…</p>
      </div>

      <div v-else-if="peekError" class="panel-err">{{ describeApiError(peekError) }}</div>

      <!-- found:false is HTTP 200 and a VERDICT, not an error: the timer is no
           longer pending, which may mean it has already been delivered. -->
      <p
        v-else-if="peeked && peeked.found === false"
        class="tm-quiet"
        :title="`There is no tombstone: the log on ${queue} is the authority.`"
      >No longer pending: it may have been delivered.</p>

      <template v-else-if="peeked">
        <div class="tm-fields">
          <DetailField label="Queue" :value="peeked.queue" mono />
          <DetailField label="Timer key" :value="peeked.timerKey" mono copyable />
          <DetailField label="Partition" :value="peeked.partition || 'Default'" mono />
          <DetailField
            label="Delivers"
            :value="`${formatDeliverIn(peeked.deliverAt, asOf)} · ${localStamp(peeked.deliverAt)}`"
            :title="utcTitle(peeked.deliverAt)"
          />
          <DetailField label="Transaction id" :value="peeked.txn" mono copyable />
          <!-- Promised at SCHEDULE, which makes the delivered frame
               correlatable without a second call. -->
          <DetailField label="Message id" :value="peeked.messageId" mono copyable />
          <DetailField label="Attempts" :value="peeked.attempts ?? '—'" />
          <DetailField label="Last error" :value="peeked.lastError || '—'" :tone="peeked.lastError ? 'danger' : 'mid'" boxed />
          <DetailField label="Producer" :value="peeked.producerSub || '—'" mono />
          <DetailField label="Created" :value="localStamp(peeked.createdAt)" :title="utcTitle(peeked.createdAt)" />
          <DetailField label="Updated" :value="localStamp(peeked.updatedAt)" :title="utcTitle(peeked.updatedAt)" />
          <DetailField
            label="State"
            :value="peeked.claimed ? 'Claimed' : 'Pending'"
            :tone="peeked.claimed ? 'high' : 'mid'"
            :title="peeked.claimed ? CLAIMED_TITLE : 'No live claim: a cancel can still take it'"
          />
        </div>
      </template>

      <template #secondary>
        <div v-if="peeked && peeked.found !== false" class="tm-payload">
          <div class="label-xs tm-payload-head">
            <span>Payload</span>
            <span v-if="payload.state === 'text' || payload.state === 'binary'" class="tm-low">
              {{ formatBytes(payload.bytes) }}<template v-if="peeked.payloadZstd"> · {{ formatBytes(payload.storedBytes) }} stored (zstd)</template>
            </span>
          </div>

          <!-- Peek promises the bytes AS STORED, and encryption happens at
               schedule and is outermost: nothing here could decrypt it. -->
          <p v-if="payload.state === 'encrypted'" class="tm-quiet" title="The broker encrypted this payload when it was scheduled. The consumer receives the decrypted frame.">Encrypted when scheduled: not readable here.</p>
          <!-- Not a failure: a zstd frame can expand four orders of magnitude
               past its stored size, so a preview that decompressed whatever it
               was handed would hang the tab on a payload a producer chose. -->
          <p
            v-else-if="payload.state === 'too_large'"
            class="tm-quiet"
            :title="`Asking for more than this console's ${formatBytes(TIMER_PAYLOAD_BUDGET_BYTES)} decode budget${payload.bytes === null ? '' : `: it declares ${formatBytes(payload.bytes)}`}. The consumer still receives it whole.`"
          >Too large to preview: {{ formatBytes(payload.storedBytes) }} stored.</p>
          <div v-else-if="payload.state === 'error'" class="panel-err">The payload did not decode: {{ payload.error }}.</div>
          <p v-else-if="payload.state === 'binary'" class="tm-quiet">{{ formatBytes(payload.bytes) }} of binary, not UTF-8 text.</p>
          <p v-else-if="payload.state === 'empty'" class="tm-quiet" title="Zero bytes is a legal thing to schedule">Empty payload.</p>
          <JsonViewer v-else-if="payload.state === 'text'" :value="payload.text" />
        </div>
      </template>

      <template #footer>
        <button class="btn btn-ghost" @click="closeDrawer">Close</button>
        <!-- The proxy's rule for a gated write, not the durable queueAdmin:
             every role but Viewer, which `can('produce') || can('consume')`
             mirrors exactly (as views/Ephemeral.vue does). -->
        <button
          v-if="canCancel && peeked && peeked.found !== false"
          class="btn btn-danger"
          @click="askCancel"
        >Cancel timer</button>
      </template>
    </DetailDrawer>

    <!-- ===================== Cancel ===================== -->
    <Teleport to="body">
      <div v-if="pending" class="modal-backdrop timers-cancel-over" @click.self="closeCancel">
        <div class="card modal-card">
          <div class="card-header"><h3>Cancel timer</h3></div>
          <div class="card-body tm-form">
            <div v-if="cancelError" class="panel-err">{{ cancelError }}</div>

            <!-- The verdict, as the stored procedure issued it. `cancelled` is
                 the only one of the three that means the timer will not fire. -->
            <template v-if="cancelVerdict">
              <span class="detail-status" :class="{ 'is-warn': cancelVerdict.tone !== 'ok' }">{{ cancelVerdict.status || 'no status' }}</span>
              <p class="tm-lead">{{ cancelVerdict.sentence }}</p>
            </template>

            <template v-else>
              <p class="tm-lead"><strong class="font-mono">{{ pending.timerKey }}</strong> on <strong class="font-mono">{{ pending.queue }}</strong></p>
              <p class="tm-help" title="A cancelled timer leaves no row and no tombstone.">It will never be delivered, and a cancel cannot be undone.</p>
              <p class="tm-help" title="A broker that has claimed it is delivering it: the cancel answers too_late.">A claimed timer cannot be cancelled.</p>
            </template>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="closeCancel">{{ cancelVerdict ? 'Done' : 'Keep the timer' }}</button>
            <button
              v-if="!cancelVerdict"
              class="btn btn-danger"
              :disabled="cancelling"
              @click="runCancel"
            >{{ cancelling ? 'Cancelling…' : 'Cancel timer' }}</button>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
  </QueuesLayout>
</template>

<script setup>
import { useRouteState } from '@/composables/useRouteState'
// Timers — the scheduled-message family, per queue (PLAN_KV_TIMERS.md §4,
// PLAN_DASHBOARD_ACTIONS.md §2.6).
//
// FOUR ROUTES, ALL OF THEM OLDER THAN THIS PAGE. list (keyset), count (prefix
// only), peek (one key, with the payload) and cancel have existed since 1.2;
// nothing new was added to the broker or the proxy for this view. What the page
// adds is the vocabulary: which numbers are exact, which are a snapshot, and
// which answers are verdicts rather than failures.
//
// NO PRIVATE TICKER, AND NO AUTO-REFRESH. Unlike views/Ephemeral.vue, whose
// gauges are in-process and free, every call here is a read of the replicated
// store on a tenant-scoped, metered, rate-limited route, and a keyset page that re-fetched
// under the reader would move rows while they are being read. The page refreshes
// when the operator asks (the shell's Refresh button) and on a cluster switch —
// and a stable gated verdict short-circuits even that, so a cell that answers
// 404 or 403 is asked exactly once (stores/routeSupport.js + the verdict below).
//
// THREE NUMBERS, THREE PROVENANCES, and the page says which is which:
//   · `≈ N timers pending`  the tenant's whole footprint, from the queue
//                           listing's root — a sweeper snapshot, minutes old
//   · the prefix count      EXACT, for one queue and one key prefix
//   · the page              exactly the rows on screen; the keyset walk has no
//                           total, by construction
import { computed, ref, watch } from 'vue'

import QueuesLayout from '@/components/QueuesLayout.vue'
import PageTools from '@/components/PageTools.vue'
import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import Icon from '@/components/Icon.vue'
import UiSelect from '@/components/UiSelect.vue'
import { timers as timersApi, describeApiError } from '@/api'
import { formatBytes, formatNumber, toNum, useApi } from '@/composables/useApi'
import { formatTimestamp, formatTimestampCompact, formatTimestampUtc } from '@/composables/useFormat'
import { describeVerdict, gatedVerdict } from '@/composables/useGatedVerdict'
import { useKeysetPager } from '@/composables/useKeysetPager'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import {
  TIMER_PAYLOAD_BUDGET_BYTES,
  decodeTimerPayload, describeCancel, formatDeliverIn, moveUnwinds, parseBrokerInstant,
  rowsForQueue, timerListParams, timerUsageIsMeasured,
} from '@/composables/useTimers'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { routeSupport } from '@/stores/routeSupport'

const { can, epoch } = useIdentity()
const { notifySuccess } = useToast()

const TENANT_TOTAL_TITLE =
  'Every pending timer this tenant holds on the cell, from the sweeper\'s cached measurement on the queue listing — minutes old, never a live count'
const CLAIMED_TITLE =
  'A broker holds the lease and is about to deliver this timer. A cancel now answers too_late; the window is bounded by the lease.'
const LIMITS = [25, 50, 100, 250]

// ---------------------------------------------------------------------------
// The queue. It is the ADDRESS, picked in the Queues section's list and kept
// in the URL, so a page of timers is something an operator can paste into a
// ticket.
// ---------------------------------------------------------------------------
const queuesStore = useQueuesStore()
const { timerRows, fetchQueues } = queuesStore

const queue = ref('')
const limit = ref(25)
useRouteState({ queue, limit })

// ---------------------------------------------------------------------------
// The page itself.
//
// `probe: true` says this page RENDERS the stable answers itself: "not on this
// cell" on an older broker, "not in this cluster's plan" on a 403
// feature_gated, and "not being served right now" on a 503 — all three are the
// quiet card above, and a red toast on top of it would report the same fact
// twice. routeSupport.guard remembers the first of them for the cluster epoch,
// so the second page that asks does not even send a request.
// ---------------------------------------------------------------------------
const pager = useKeysetPager()
// Destructured so the template reads them as plain bindings (Vue unwraps a ref
// that is a top-level binding, not one reached through an object).
const { canPrev, canNext, page: pageNumber } = pager
const verdict = ref(null)

const listTimers = routeSupport.guard('timers', (q, params, config) =>
  timersApi.list(q, params, { ...config, probe: true }))
const peekTimer = routeSupport.guard('timers', (q, key, config) =>
  timersApi.peek(q, key, { ...config, probe: true }))
const countTimers = routeSupport.guard('timers', (q, prefix, config) =>
  timersApi.count(q, prefix, { ...config, probe: true }))

const listPanel = useApi(listTimers, { immediate: false })
const peekPanel = useApi(peekTimer, { immediate: false })

// Which queue the rows on screen belong to. Without it, switching queues keeps
// the previous queue's timers on screen under the new queue's name until the
// next page lands — the same class of lie the tenant-keyed stores exist to
// prevent, one scope down.
const loadedQueue = ref(null)

const rows = computed(() => rowsForQueue(loadedQueue.value, queue.value, listPanel.data.value))
const loading = computed(() => listPanel.loading.value)
const listError = computed(() => listPanel.error.value)
const firstLoad = computed(() => loading.value && loadedQueue.value !== queue.value)
const isStale = computed(() => Boolean(listError.value) && rows.value.length > 0)
// Every relative "Delivers" cell is rendered against ONE instant — the load —
// which is what the freshness stamp beside the table claims. A column that
// silently drifted from the stamp above it would be the worse lie.
const asOf = computed(() => listPanel.lastUpdated.value?.getTime() ?? Date.now())

const quiet = computed(() =>
  verdict.value && verdict.value !== 'transient' ? describeVerdict(verdict.value, 'timers') : null
)
// The why, on the quiet card's line. A 503 here is never an operator's switch:
// switches.rs pins rung 1 to `true` for TimerRead / TimerCancel and quota.rs
// allows both (§9.6), so `timers_disabled` is reachable only on POST
// /api/v1/timers, which this page never calls. What it meets is
// handlers/timers.rs `unavailable()`: no leader, a retry, a deadline passed.
const quietTitle = computed(() => {
  if (!quiet.value) return ''
  if (verdict.value === 'absent') return `${quiet.value.detail} The timer routes arrived in 1.2.`
  if (verdict.value === 'paused') return 'No operator switch pauses timer reads or cancels: the cell answers this when the cluster cannot answer (no leader, or a timeout). A retry is the only cure.'
  return quiet.value.detail
})

// A sweeper figure the page on screen contradicts is not a measurement: a cell
// that has not written this tenant's usage row answers `timerRows: 0`, so `≈ 0
// timers pending` over a full page of timers would be certainly wrong. The
// honest rendering of "not measured yet" is silence, not a zero and not a dash.
const tenantTotalMeasured = computed(() => timerUsageIsMeasured(timerRows.value, rows.value))

// A cluster switch can land on a different cell running a different broker, so
// the cursor, the verdict and the open drawer all belong to the cluster we
// left. Checked inside `reload` as well as watched, because the shell fires
// every refresh callback on the same epoch change and the order of the two is
// not something this view should depend on.
let seenEpoch = epoch.value
const syncEpoch = () => {
  if (epoch.value === seenEpoch) return
  seenEpoch = epoch.value
  pager.reset()
  verdict.value = null
  loadedQueue.value = null
  countResult.value = null
  countError.value = null
  closeDrawer()
  closeCancel()
}
watch(epoch, syncEpoch)

// Which request the answer on screen came from. useApi.execute RESOLVES a
// superseded call with the PREVIOUS data instead of throwing — that is how it
// drops an aborted or epoch-stale answer — so the continuation below runs for a
// page that never landed, and would commit `loadedQueue` and a cursor from the
// sequence that was abandoned. Every reload aborts the one before it (a queue
// change, a page size, Next, Prev, the shell's Refresh all reload), so this is
// the common path, not the corner.
//
// It is also the token `movePage` unwinds against: the same counter answers
// "has another load started since?", which is the only way to see a supersede
// that has not written anything yet.
let reqSeq = 0

/** True when THIS call is the one that landed. */
const reload = async () => {
  syncEpoch()
  if (!queue.value) return false
  // A stable verdict cannot change by asking again. The guard would refuse the
  // request anyway; returning here also keeps the panel's error untouched.
  if (verdict.value && verdict.value !== 'transient') return false
  const params = timerListParams(pager.current(), limit.value)
  const asked = queue.value
  const mine = ++reqSeq
  const epochAtStart = epoch.value
  try {
    const data = await listPanel.execute(asked, params)
    if (mine !== reqSeq || asked !== queue.value || epochAtStart !== epoch.value) return false
    verdict.value = null
    loadedQueue.value = asked
    pager.received(data)
    return true
  } catch (err) {
    if (mine !== reqSeq) return false
    verdict.value = gatedVerdict(err)
    return false
  }
}

/**
 * A move whose page never landed is walked back. `next()` has to push the
 * cursor before the request can be sent — it is what produces the cursor to
 * send — so leaving it pushed after a failure prints "Page 3" over page 2's
 * rows and kills Next until the operator presses Previous.
 *
 * The walk is restored ONLY if this move still owns it, and `moveUnwinds` holds
 * both halves of that question: the marks catch whatever has already touched
 * the stack, and `reqSeq` catches a SUPERSEDING LOAD STILL IN FLIGHT — which
 * has touched nothing yet, so the marks say "nothing happened" while a Refresh
 * pressed during a Next is out there fetching the moved cursor. Unwinding under
 * it would relabel its page the moment it lands.
 */
const movePage = async (move) => {
  const before = pager.mark()
  if (!move()) return
  const moved = pager.mark()
  // Read BEFORE the load is started: `reload` takes the next token for itself,
  // so this is "everything that had been started when the move began".
  const seqAtMove = reqSeq
  const landed = await reload()
  if (moveUnwinds({ landed, seqAtMove, seqNow: reqSeq, movedMark: moved, nowMark: pager.mark() })) {
    pager.restore(before)
  }
}
const goNext = () => movePage(pager.next)
const goPrev = () => movePage(pager.prev)

/** Re-ask after an upgrade or after an operator turned the family back on. */
const probeAgain = async () => {
  routeSupport.forget('timers')
  verdict.value = null
  await reload()
}

// The queue and the page size each start a NEW sequence: the cursors on the
// stack address the old one and would silently page through it.
watch(queue, () => {
  pager.reset()
  closeDrawer()
  countResult.value = null
  countError.value = null
  reload()
})
watch(limit, () => { pager.reset(); reload() })

// Registered NON-auto: the header's Refresh button and a cluster switch drive
// this page. See the header comment for why there is no cadence.
useRefresh(reload)

// ---------------------------------------------------------------------------
// Exact count, under a prefix the broker requires
// ---------------------------------------------------------------------------
const countPrefix = ref('')
const countResult = ref(null)
const countError = ref(null)
const counting = ref(false)

const runCount = async () => {
  if (!queue.value || !countPrefix.value || counting.value) return
  counting.value = true
  countError.value = null
  const prefix = countPrefix.value
  const epochAtStart = epoch.value
  try {
    const res = await countTimers(queue.value, prefix)
    if (epochAtStart !== epoch.value) return
    // This number is advertised as EXACT, so a body without a `count` must not
    // become an authoritative zero the broker never said — the same coercion
    // the usage fields are read with toNum to avoid.
    countResult.value = { prefix, count: toNum(res.data?.count) }
  } catch (err) {
    countResult.value = null
    const v = gatedVerdict(err)
    if (v !== 'transient') verdict.value = v
    // A 400 here is the route's own refusal (timers_count_prefix_required and
    // friends), and the sentence that names what to fix is in `detail`, with
    // the code in `reason` — neither of which describeApiError reaches, since
    // it only knows the proxy's `{error, code}` envelope.
    countError.value = err?.body?.detail || err?.body?.reason || describeApiError(err)
  } finally {
    counting.value = false
  }
}

// ---------------------------------------------------------------------------
// Peek — one key, with the payload the list deliberately never carries
// ---------------------------------------------------------------------------
const selected = ref(null)
const peekLoading = computed(() => peekPanel.loading.value)
const peekError = computed(() => peekPanel.error.value)

// The peeked timer, ONLY if it is the one the drawer is titled with. useApi
// never clears `data` when a new call starts, so while the second peek is in
// flight the pane would render the FIRST timer's payload, its size and its zstd
// footnote under the second timer's key — the same lie the loadedQueue guard
// prevents one scope up. The answer identifies itself (`queue` and `timerKey`
// are on both the found and the `found:false` shapes), so no second ref is
// needed to know whose it is.
const peeked = computed(() => {
  const data = peekPanel.data.value
  const key = selected.value?.timerKey
  if (!data || key === undefined) return null
  if (data.timerKey !== undefined && data.timerKey !== key) return null
  if (data.queue !== undefined && data.queue !== queue.value) return null
  return data
})
const payload = computed(() => decodeTimerPayload(peeked.value))

const openTimer = (row) => {
  selected.value = row
  peekPanel.execute(queue.value, row.timerKey).catch((err) => {
    const v = gatedVerdict(err)
    if (v === 'transient') return
    // The family itself answered "not here / not in the plan / not now": the
    // page becomes one quiet card, and a drawer floating over it would be
    // asking about something the page has just said it cannot see.
    verdict.value = v
    closeDrawer()
  })
}

function closeDrawer() {
  selected.value = null
  peekPanel.abort()
}

// ---------------------------------------------------------------------------
// Cancel — gated write, confirmed, and the SP's verdict rendered as given
// ---------------------------------------------------------------------------
const canCancel = computed(() => can('produce') || can('consume'))
const pending = ref(null)
const cancelling = ref(false)
const cancelError = ref(null)
const cancelVerdict = ref(null)

const askCancel = () => {
  if (!selected.value || !queue.value) return
  // The txn the drawer is already showing, carried into the call: the cancel
  // route reads exactly one query parameter and the SP echoes it back on
  // `absent` (025_log_timers.sql: "the expected txn is echoed back so the check
  // needs no second API"). Without it the one verdict where an operator gets
  // hurt says "look for the timer's txn" and cannot name it.
  const txn = peeked.value?.txn || selected.value.txn || null
  pending.value = { queue: queue.value, timerKey: selected.value.timerKey, txn }
  cancelError.value = null
  cancelVerdict.value = null
}

function closeCancel() {
  const done = cancelVerdict.value
  pending.value = null
  cancelError.value = null
  cancelVerdict.value = null
  // The row on screen is a snapshot: after a verdict that removed it (or after
  // discovering it was never there) the page is out of date.
  if (done) {
    closeDrawer()
    reload()
  }
}

const runCancel = async () => {
  const p = pending.value
  if (!p || cancelling.value) return
  cancelling.value = true
  cancelError.value = null
  try {
    // `probe: true` for the same reason as the reads: the catch below turns a
    // gated or paused answer into the page's own quiet card, so the toast
    // would be the second report of it.
    const res = await timersApi.cancel(p.queue, p.timerKey, {
      probe: true,
      ...(p.txn ? { params: { txn: p.txn } } : {}),
    })
    // HTTP 200 with `ok:false` is the normal shape here: `too_late` and
    // `absent` are verdicts about the TIMER, not failures of the call.
    cancelVerdict.value = describeCancel(res.data, p)
    if (cancelVerdict.value.status === 'cancelled') {
      notifySuccess(`Cancelled ${p.timerKey}`, `on ${p.queue}`)
    }
  } catch (err) {
    const v = gatedVerdict(err)
    if (v !== 'transient') verdict.value = v
    cancelError.value = describeApiError(err)
  } finally {
    cancelling.value = false
  }
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------
// BOTH HALVES OF A STAMP GO THROUGH ONE PARSER. The broker writes six
// fractional digits and useFormat's asValidDate is a bare `new Date(value)`, so
// on a runtime that rejects the extra digits the relative half of a cell would
// read "in 4m" beside an absolute half that had fallen back to printing the raw
// string. The local-time value with UTC in the hover title is the app's idiom
// everywhere else and is kept here.
const localStamp = (value) => formatTimestamp(parseBrokerInstant(value))
// The table's short form, the Messages tab's: DD/MM HH:mm:ss.
const compactStamp = (value) => {
  const at = parseBrokerInstant(value)
  return at ? formatTimestampCompact(at) : '—'
}
const utcTitle = (value) => formatTimestampUtc(parseBrokerInstant(value))

// First load. Last, so nothing above can be reached before it is defined.
fetchQueues()
reload()
</script>

<style scoped>
.tm { display: flex; flex-direction: column; gap: 16px; }
.tm > .page-tools, .tm > .view-banner { margin-bottom: 0; }
.tm-prefix { width: 150px; }

.tm-scroll { overflow-x: auto; }
.tm-table { min-width: 760px; }
.tm-table th, .tm-table td { white-space: nowrap; }
.tm-table td { height: 40px; font-size: 13px; }
.tm-table .r { text-align: right; }
.tm-row { cursor: pointer; }
.tm-row:focus-visible { outline-offset: -2px; }
/* Long keys, partitions and errors end in an ellipsis; the whole is the tooltip. */
.tm-cut { display: inline-block; max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: bottom; }
.tm-key { display: inline-flex; align-items: center; gap: 8px; }
.tm-mono { font-family: var(--font-mono); font-size: 12px; color: var(--text-hi); }
.tm-mid { color: var(--text-mid); }
.tm-low { color: var(--text-low); }
.tm-tag { padding: 0 6px; border: 1px solid var(--bd); border-radius: var(--r-chip); font-size: 11px; line-height: 17px; color: var(--text-low); }
.tm-due { font-weight: 500; color: var(--text-hi); }
.tm-at { margin-left: 8px; font-family: var(--font-mono); font-size: 12px; color: var(--text-low); font-variant-numeric: tabular-nums; }
.tm-num { font-family: var(--font-mono); font-size: 12px; font-variant-numeric: tabular-nums; }
.tm-error { max-width: 280px; font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); }
.tm-state { font-size: 12px; color: var(--text-hi); }
.tm-state.tm-low { color: var(--text-low); }
.tm-when { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); font-variant-numeric: tabular-nums; }

/* --- The drawer ------------------------------------------------------------ */
.tm-loading { padding: 48px 0; text-align: center; font-size: 13px; color: var(--text-low); }
.tm-loading .spinner { margin: 0 auto 12px; }
.tm-quiet { margin: 0; padding: 12px 0; font-size: 13px; color: var(--text-mid); }
.tm-fields { display: flex; flex-direction: column; gap: 16px; }
.tm-payload-head { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; margin-bottom: 8px; }

/* --- The cancel confirm ------------------------------------------------------ */
.tm-form { display: flex; flex-direction: column; gap: 8px; }
.tm-form .panel-err { margin: 0 0 4px; }
.tm-lead { margin: 0; font-size: 13px; color: var(--text-hi); overflow-wrap: anywhere; }
.tm-lead strong { font-weight: 600; }
.tm-help { margin: 0; font-size: 12px; color: var(--text-low); }

/* The only way to this confirm is the drawer's footer, which stays open
   behind it: `.drawer-panel` is z-index 51 and the shared scrim 50, so this
   sits above both and under the Autocomplete's menu (60). */
.timers-cancel-over { z-index: 55; }

@media (max-width: 640px) {
  .tm-prefix { width: auto; flex: 1; min-width: 0; }
}
</style>
