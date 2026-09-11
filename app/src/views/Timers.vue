<template>
  <div class="view-container">

    <!-- Tenant scope, from identity and never from the fetch. The timer count
         beside it is the TENANT's, from the queue listing's root, and it is a
         sweeper snapshot — hence `≈`, everywhere it appears. -->
    <div class="scope-strip">
      <span class="chip chip-mute">tenant scope</span>
      <span class="scope-text">
        <strong>{{ actingTenantSlug || 'no tenant' }}</strong>
        <span class="scope-sep">/</span>{{ actingClusterSlug || 'no cluster' }}
        <span class="scope-sep">·</span>cell {{ actingCellSlug || 'unknown' }}
      </span>
      <span class="scope-fill"></span>
      <span v-if="tenantTotalMeasured" class="chip chip-mute" :title="TENANT_TOTAL_TITLE">
        ≈ {{ formatNumber(timerRows) }} timers pending
      </span>
      <span v-if="tenantTotalMeasured && timerBytes !== null" class="scope-meta" :title="TENANT_TOTAL_TITLE">
        ≈ {{ formatBytes(timerBytes) }}
      </span>
      <span class="scope-meta">{{ stamp(listPanel) }}</span>
    </div>

    <!-- ===================== The three quiet states =====================
         Not here / not in the plan / not being served right now. One card, no
         poll, no retry: composables/useGatedVerdict.js owns the rule and the
         wording, and stores/routeSupport.js remembers the first of them for
         the whole cluster epoch so a second page cannot re-ask. -->
    <div v-if="quiet" class="card">
      <div class="empty-state">
        <svg v-if="verdict === 'absent'" class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <circle cx="12" cy="13" r="8" />
          <path stroke-linecap="round" d="M12 9v4l3 2M3 3l18 18" />
        </svg>
        <svg v-else-if="verdict === 'gated'" class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <rect x="5" y="11" width="14" height="9" rx="2" />
          <path stroke-linecap="round" d="M8 11V8a4 4 0 018 0v3" />
        </svg>
        <svg v-else class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <circle cx="12" cy="12" r="9" />
          <path stroke-linecap="round" d="M10 9v6M14 9v6" />
        </svg>
        <h3>{{ quiet.title }}</h3>
        <p>{{ quiet.detail }}</p>
        <p v-if="verdict === 'absent'" style="font-size:13px;">
          <code>/api/v1/timers/*</code> arrived in 1.2.
        </p>
        <!-- The shared copy says a 503 is "an operator's switch, or a broker
             that cannot reach its database". Half of that is unreachable HERE,
             and saying it would send an operator to a switch that was never on
             this path: switches.rs pins rung 1 to `true` for
             Surface::TimerRead / TimerCancel and quota.rs allows both (§9.6 —
             a read that answered 503 would stop a caller finding out whether a
             timer it can no longer cancel is still pending, and the stop button
             must not switch itself off). So `timers_disabled` is reachable only
             on POST /api/v1/timers, which this page never calls, and the 503 it
             does meet is handlers/timers.rs `unavailable()`: a pool exhaustion,
             a statement timeout, a dead connection. The KV page keeps the
             shared wording, where the switch is real. -->
        <p v-else-if="verdict === 'paused'" style="font-size:13px;">
          No operator switch pauses timer reads or cancels — the cell answers
          this when its database is out of reach.
        </p>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <template v-else>
      <!-- Stale rows presented as live is the failure mode this banner exists
           to prevent: the panel keeps the last page that loaded. -->
      <div v-if="isStale" class="status-banner banner-bad view-banner">
        <span>
          <strong>Could not refresh this page of timers</strong> ·
          {{ describeApiError(listError) }} · showing the last page that loaded
          ({{ stamp(listPanel) }}).
        </span>
      </div>

      <!-- Filters. The queue is not a filter, it is the ADDRESS: the route is
           queue-scoped because a tenant-wide timer list would be a scan whose
           call rate is decided by somebody else's web traffic (§4.1). -->
      <div class="card filters">
        <div class="card-body filter-rows">
          <div class="filter-row">
            <div class="filter-field-col filter-field-wide">
              <label class="label-xs" for="timers-queue">Queue</label>
              <Autocomplete
                id="timers-queue"
                v-model="queue"
                :options="queueOptions"
                :loading="queuesLoading"
                label="Queue"
                placeholder="Pick a queue"
                allow-custom
              />
            </div>

            <div class="filter-field-col">
              <label class="label-xs" for="timers-limit">Page size</label>
              <select id="timers-limit" v-model.number="limit" class="input">
                <option :value="50">50</option>
                <option :value="100">100</option>
                <option :value="250">250</option>
              </select>
            </div>

            <!-- The exact count. A whole-queue count is REFUSED by the stored
                 procedure, not by this form: an exact aggregate cannot have
                 the list's LIMIT, so it is prefix-scoped to stay an index
                 range (handlers/timers.rs timer_read_query). -->
            <div class="filter-field-col filter-field-wide">
              <label class="label-xs" for="timers-prefix">Exact count under a prefix</label>
              <div class="timers-count-row">
                <input
                  id="timers-prefix"
                  v-model="countPrefix"
                  class="input"
                  placeholder="prefix — required, e.g. retry:"
                  @keyup.enter="runCount"
                />
                <button
                  class="btn btn-ghost"
                  :disabled="!queue || !countPrefix || counting"
                  :title="countPrefix ? 'Count every pending timer whose key starts with this' : 'The broker refuses a whole-queue count — give a non-empty prefix'"
                  @click="runCount"
                >{{ counting ? 'Counting…' : 'Count' }}</button>
              </div>
            </div>
          </div>
          <!-- Hints and the count's answer live UNDER the row, never inside a
               column: the row aligns its columns at the bottom, so a one-line
               hint beside a two-line one lifted each label and input by a
               different amount and the card read as two rows. -->
          <div class="filter-foot">
            <span v-if="queuesUnavailable" class="filter-hint">
              Queue list unavailable — type a queue name
            </span>
            <span v-else-if="unlistedQueue" class="filter-hint">
              Not in this cluster's queue list — asking the broker anyway
            </span>
            <span v-if="countError" class="filter-hint">{{ countError }}</span>
            <span v-else-if="countResult && countResult.count !== null" class="filter-hint">
              {{ formatNumber(countResult.count) }} pending under
              <code>{{ countResult.prefix }}</code>
            </span>
            <span v-else-if="countResult" class="filter-hint">
              The broker answered no count for <code>{{ countResult.prefix }}</code>
            </span>
            <span v-else class="filter-hint">
              An exact count is prefix-scoped; a whole-queue count would be a scan.
            </span>
          </div>
        </div>
      </div>

      <!-- ===================== The page ===================== -->
      <div class="card">
        <div class="card-header">
          <h3>Pending timers</h3>
          <span class="card-sub">
            scheduled, not yet delivered — a fired timer leaves no row behind
          </span>
          <span class="muted">{{ stamp(listPanel) }}</span>
        </div>

        <div style="overflow-x:auto;">
          <table class="t">
            <thead>
              <tr>
                <th>Timer key</th>
                <th>Partition</th>
                <th>Delivers</th>
                <th class="num">Attempts</th>
                <th>Last error</th>
                <th>State</th>
                <th style="text-align:right;">Created</th>
              </tr>
            </thead>
            <tbody>
              <template v-if="firstLoad">
                <tr v-for="i in 6" :key="`sk-${i}`">
                  <td colspan="7"><div class="skeleton" style="height:16px;" /></td>
                </tr>
              </template>

              <template v-else-if="rows.length">
                <tr
                  v-for="row in rows"
                  :key="row.timerKey"
                  class="timers-row"
                  @click="openTimer(row)"
                >
                  <td>
                    <div class="timers-key">
                      <span class="font-mono" style="font-size:12px;">{{ row.timerKey }}</span>
                      <span v-if="row.encrypted" class="chip chip-mute" title="Stored as an encrypted envelope — the payload is not readable from this console">encrypted</span>
                    </div>
                  </td>
                  <td><span style="font-size:12px; color:var(--text-mid);">{{ row.partition || '—' }}</span></td>
                  <td :title="utcTitle(row.deliverAt)">
                    <div :class="dueClass(row)" style="font-size:12px;">{{ formatDeliverIn(row.deliverAt, asOf) }}</div>
                    <div class="font-mono" style="font-size:11px; color:var(--text-low);">
                      {{ localStamp(row.deliverAt) }}
                    </div>
                  </td>
                  <td class="num" :class="{ warn: (row.attempts || 0) > 0 }">{{ row.attempts ?? '—' }}</td>
                  <td>
                    <span
                      v-if="row.lastError"
                      class="timers-error"
                      :title="row.lastError"
                    >{{ row.lastError }}</span>
                    <span v-else class="num mute">—</span>
                  </td>
                  <td>
                    <!-- `claimed` is the SP's one definition of "out of your
                         hands": a claim token that has not expired. A row in
                         BACKOFF reads claimed:false and is still cancellable,
                         deliberately — so this column says nothing more. -->
                    <span v-if="row.claimed" class="chip chip-warn" :title="CLAIMED_TITLE">claimed</span>
                    <span v-else class="chip chip-mute">pending</span>
                  </td>
                  <td
                    :title="utcTitle(row.createdAt)"
                    class="font-mono tabular-nums"
                    style="text-align:right; font-size:11.5px; color:var(--text-mid); white-space:nowrap;"
                  >{{ localStamp(row.createdAt) }}</td>
                </tr>
              </template>

              <!-- No queue is not an empty queue, and the route cannot answer
                   without one. -->
              <tr v-else-if="!queue">
                <td colspan="7">
                  <div class="empty-state">
                    <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                      <circle cx="12" cy="13" r="8" />
                      <path stroke-linecap="round" d="M12 9v4l3 2M9 2h6" />
                    </svg>
                    <h3>Pick a queue</h3>
                    <p>
                      Timers are listed per queue, never tenant-wide: the key is
                      unique inside one queue, and a whole-tenant listing would be
                      a scan driven by somebody else's traffic.
                    </p>
                  </div>
                </td>
              </tr>

              <tr v-else-if="listError">
                <td colspan="7">
                  <div class="empty-state empty-state-failed">
                    <h3>{{ describeApiError(listError) }}</h3>
                    <p>Nothing loaded — this is a failure, not an empty queue.</p>
                    <button class="btn btn-ghost" @click="reload">Retry</button>
                  </div>
                </td>
              </tr>

              <tr v-else>
                <td colspan="7">
                  <div class="empty-state">
                    <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                      <circle cx="12" cy="13" r="8" />
                      <path stroke-linecap="round" d="M12 9v4l3 2M9 2h6" />
                    </svg>
                    <h3>No pending timers{{ pageNumber > 1 ? ' on this page' : '' }}</h3>
                    <p>
                      Nothing is scheduled on <strong class="font-mono">{{ queue }}</strong> right now.
                      A timer that has fired leaves no row behind — the log is where
                      its message is.
                    </p>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Keyset pager: no page count, because there is no total without a
             count and this is an index range, not an offset. -->
        <div v-if="queue && (rows.length || canPrev)" class="pager">
          <span class="pager-count">
            Page <span class="font-mono tabular-nums">{{ pageNumber }}</span>
            · {{ rows.length }} row{{ rows.length === 1 ? '' : 's' }}
            <template v-if="!canNext && !listError"> · end of the queue</template>
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
      <div v-if="peekLoading" style="text-align:center; padding:48px 0;">
        <div class="spinner" style="margin:0 auto 12px;"></div>
        <p style="color:var(--text-low);">Loading the timer…</p>
      </div>

      <div v-else-if="peekError" class="panel-err">
        {{ describeApiError(peekError) }}
      </div>

      <!-- found:false is HTTP 200 and a VERDICT, not an error: the timer is no
           longer pending, which may mean it has already been delivered. -->
      <div v-else-if="peeked && peeked.found === false" class="empty-tile">
        No longer pending. There is no tombstone, so this may mean it has already
        been delivered — the log on <strong class="font-mono">{{ queue }}</strong> is the authority.
      </div>

      <template v-else-if="peeked">
        <div class="timers-fields">
          <DetailField label="Queue" :value="peeked.queue" mono />
          <DetailField label="Timer key" :value="peeked.timerKey" mono copyable />
          <DetailField label="Partition" :value="peeked.partition || 'Default'" mono />
          <DetailField
            label="Delivers"
            :value="`${formatDeliverIn(peeked.deliverAt, asOf)} · ${localStamp(peeked.deliverAt)}`"
            :title="utcTitle(peeked.deliverAt)"
          />
          <DetailField label="Transaction id" :value="peeked.txn" mono copyable />
          <!-- Promised at SCHEDULE, which is what makes the delivered frame
               correlatable without a second API call. -->
          <DetailField label="Message id" :value="peeked.messageId" mono copyable />
          <DetailField label="Attempts" :value="peeked.attempts ?? '—'" />
          <DetailField label="Last error" :value="peeked.lastError || '—'" :tone="peeked.lastError ? 'danger' : 'mid'" boxed />
          <DetailField label="Producer" :value="peeked.producerSub || '—'" mono />
          <DetailField label="Created" :value="localStamp(peeked.createdAt)" :title="utcTitle(peeked.createdAt)" />
          <DetailField label="Updated" :value="localStamp(peeked.updatedAt)" :title="utcTitle(peeked.updatedAt)" />
          <DetailField
            label="State"
            :value="peeked.claimed ? 'claimed' : 'pending'"
            :tone="peeked.claimed ? 'accent' : 'mid'"
            :title="peeked.claimed ? CLAIMED_TITLE : 'No live claim — a cancel can still take it'"
          />
        </div>
      </template>

      <template #secondary>
        <div v-if="peeked && peeked.found !== false" class="timers-payload">
          <div class="label-xs timers-payload-head">
            <span>Payload</span>
            <span v-if="payload.state === 'text' || payload.state === 'binary'" class="muted">
              {{ formatBytes(payload.bytes) }}
              <template v-if="peeked.payloadZstd"> · {{ formatBytes(payload.storedBytes) }} stored (zstd)</template>
            </span>
          </div>

          <!-- Peek promises the bytes AS STORED, and encryption happens at
               schedule and is outermost: there is nothing this browser could
               decrypt, and pretending otherwise would be a lie about what the
               consumer will receive. -->
          <div v-if="payload.state === 'encrypted'" class="empty-tile">
            <strong>Encrypted envelope.</strong>
            The broker encrypted this payload when it was scheduled, so the console
            cannot show its contents. The consumer receives the decrypted frame.
          </div>
          <!-- Not a failure: the stored bytes are capped at 1 MiB and a zstd
               frame can expand four orders of magnitude past that — or ask the
               decoder to hold a 1.9 GB window for 4 MB of output — so a preview
               that decompressed whatever it was handed would hang the tab on a
               payload a producer chose. "Asking for more than", not "expanding
               past": the size is only named when the frame declared one, and a
               frame refused on its window declared nothing. -->
          <div v-else-if="payload.state === 'too_large'" class="empty-tile">
            <strong>Too large to preview.</strong>
            {{ formatBytes(payload.storedBytes) }} stored, asking for more than this
            console's {{ formatBytes(TIMER_PAYLOAD_BUDGET_BYTES) }} decode budget{{
              payload.bytes === null ? '' : ` — it declares ${formatBytes(payload.bytes)}` }}.
            The consumer still receives it whole.
          </div>
          <div v-else-if="payload.state === 'error'" class="panel-err">
            The payload did not decode — {{ payload.error }}.
          </div>
          <div v-else-if="payload.state === 'binary'" class="empty-tile">
            {{ formatBytes(payload.bytes) }} of binary, not UTF-8 text.
          </div>
          <div v-else-if="payload.state === 'empty'" class="empty-tile">
            Empty payload — zero bytes, which is a legal thing to schedule.
          </div>
          <JsonViewer v-else-if="payload.state === 'text'" :value="payload.text" />
        </div>
      </template>

      <template #footer>
        <button class="btn btn-ghost" @click="closeDrawer">Close</button>
        <!-- The proxy's rule for a gated write, not the durable queueAdmin:
             Gated(Timers, Open) is allowed to every role except Viewer, which
             `can('produce') || can('consume')` mirrors exactly
             (views/Ephemeral.vue does the same for its family). -->
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
          <div class="card-header">
            <h3>Cancel timer</h3>
          </div>
          <div class="card-body">
            <div v-if="cancelError" class="panel-err">{{ cancelError }}</div>

            <!-- The verdict, as the stored procedure issued it. `cancelled` is
                 the only one of the three that means the timer will not fire. -->
            <div v-if="cancelVerdict" class="timers-verdict">
              <span class="chip" :class="cancelVerdict.tone === 'ok' ? 'chip-ok' : 'chip-warn'">
                {{ cancelVerdict.status || 'no status' }}
              </span>
              <p style="color:var(--text-hi); margin-top:8px;">{{ cancelVerdict.sentence }}</p>
            </div>

            <template v-else>
              <p style="color:var(--text-mid); margin-bottom:10px;">
                <strong class="font-mono">{{ pending.timerKey }}</strong>
                on
                <strong class="font-mono">{{ pending.queue }}</strong>
              </p>
              <p style="color:var(--text-hi); margin-bottom:8px;">
                This deletes the scheduled message. It will never be delivered, and
                there is nothing to undo it with — a cancelled timer leaves no row
                and no tombstone.
              </p>
              <p style="font-size:13px; color:var(--text-low);">
                A timer a broker has already claimed cannot be cancelled: that answers
                <code>too_late</code>, and the delivery goes out.
              </p>
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
</template>

<script setup>
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
// gauges are in-process and free, every call here is a Postgres read on a
// tenant-scoped, metered, rate-limited route, and a keyset page that re-fetched
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
import { useRoute, useRouter } from 'vue-router'

import Autocomplete from '@/components/Autocomplete.vue'
import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import { timers as timersApi, describeApiError } from '@/api'
import { formatBytes, formatNumber, toNum, useApi } from '@/composables/useApi'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
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

const route = useRoute()
const router = useRouter()
const { can, epoch, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
const { notifySuccess } = useToast()

const TENANT_TOTAL_TITLE =
  'Every pending timer this tenant holds on the cell, from the sweeper\'s cached measurement on the queue listing — minutes old, never a live count'
const CLAIMED_TITLE =
  'A broker holds the lease and is about to deliver this timer. A cancel now answers too_late; the window is bounded by the lease.'

// ---------------------------------------------------------------------------
// Queue picker. The queue is the ADDRESS, and it lives in the URL so a page of
// timers is something an operator can paste into a ticket.
// ---------------------------------------------------------------------------
const queuesStore = useQueuesStore()
const {
  queues: allQueues, loading: queuesLoading, error: queuesError,
  timerRows, timerBytes, fetchQueues,
} = queuesStore

const queue = ref(typeof route.query.queue === 'string' ? route.query.queue : '')
const limit = ref(100)

const queueOptions = computed(() => allQueues.value.map((q) => q.name).filter(Boolean).sort())
const queuesUnavailable = computed(() => Boolean(queuesError.value) && queueOptions.value.length === 0)
// The picker suggests from /resources/queues; the timers live in their own
// table. A queue that only has timers on it must still be addressable.
const unlistedQueue = computed(
  () => Boolean(queue.value) && queueOptions.value.length > 0 && !queueOptions.value.includes(queue.value)
)

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
watch(queue, (q) => {
  pager.reset()
  closeDrawer()
  countResult.value = null
  countError.value = null
  // The URL carries the queue so the page can be linked to; replace, never
  // push, so Back leaves the page instead of walking the picker's history.
  const current = typeof route.query.queue === 'string' ? route.query.queue : ''
  if (current !== q) {
    router.replace({ query: { ...route.query, queue: q || undefined } })
  }
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
const utcTitle = (value) => formatTimestampUtc(parseBrokerInstant(value))

/**
 * Overdue is not an error — the sweeper runs on a cycle and a claimed row is
 * mid-delivery — but a timer that is a minute late is worth seeing, because
 * that is the shape the 1.5.1 announce defect had.
 */
const dueClass = (row) => {
  const at = parseBrokerInstant(row?.deliverAt)
  if (!at) return 'timers-due'
  return at.getTime() < asOf.value - 60_000 ? 'timers-due warn' : 'timers-due'
}

// First load. Last, so nothing above can be reached before it is defined.
fetchQueues()
reload()
</script>

<style scoped>
/* The queue names and the timer prefixes are long and dotted; the page-size
   select next to them is three digits wide and must not take the same room. */
.filter-field-wide { flex-basis: 260px; max-width: 340px; }

.timers-row { cursor: pointer; }
.timers-key { display: flex; align-items: center; gap: 8px; min-width: 0; }
.timers-due { color: var(--text-hi); font-weight: 500; }
.timers-due.warn { color: var(--warn-400); }

/* One line of the failure, the rest on hover: a stack trace in a table cell
   pushes every other column off the screen. */
.timers-error {
  display: block; max-width: 320px;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  font-size: 11.5px; color: var(--ember-400);
}

.timers-count-row { display: flex; gap: 8px; align-items: center; }
.timers-count-row .input { flex: 1; min-width: 0; }

.timers-fields { display: flex; flex-direction: column; gap: 14px; }

.timers-payload-head {
  display: flex; align-items: baseline; justify-content: space-between;
  gap: 8px; margin-bottom: 6px;
}
.timers-payload-head .muted { color: var(--text-low); font-size: 11px; }

.timers-verdict { margin-bottom: 4px; }

/* The ONLY way to this confirm is the drawer's footer, and askCancel leaves the
   drawer open behind it: `.drawer-panel` is z-index 51 while the shared
   `.modal-backdrop` is 50, so without this the dialog opens BEHIND the panel
   that raised it — invisible, unclickable, and the timer cannot be cancelled at
   all. 55 clears the drawer and still passes under the Autocomplete's teleported
   menu (60), exactly as .dlq-replay-over / .push-over / .qc-over do. */
.timers-cancel-over { z-index: 55; }
</style>
