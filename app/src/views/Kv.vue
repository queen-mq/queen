<template>
  <div class="view-container">

    <!-- Tenant scope, from identity and never from the fetch. Two key counts
         sit beside it and they COUNT DIFFERENT POPULATIONS: `≈ N live keys` is
         the sweeper's five-minute snapshot of the live rows of every namespace,
         `N in <ns>` an exact count of every row of one namespace, expired
         included. Both titles say so; the script header has the why. -->
    <div class="scope-strip">
      <span class="chip chip-mute">tenant scope</span>
      <span class="scope-text">
        <strong>{{ actingTenantSlug || 'no tenant' }}</strong>
        <span class="scope-sep">/</span>{{ actingClusterSlug || 'no cluster' }}
        <span class="scope-sep">·</span>cell {{ actingCellSlug || 'unknown' }}
      </span>
      <span class="scope-fill"></span>
      <span v-if="usageMeasured" class="chip chip-mute" :title="TENANT_TOTAL_TITLE">
        ≈ {{ formatNumber(kvRows) }} live keys
      </span>
      <span v-if="usageMeasured && kvBytes !== null" class="scope-meta" :title="TENANT_BYTES_TITLE">
        ≈ {{ formatBytes(kvBytes) }}
      </span>
      <span v-if="exactCount !== null" class="scope-meta" :title="EXACT_COUNT_TITLE">
        {{ formatNumber(exactCount) }} in {{ namespace }}
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
          <path stroke-linecap="round" d="M14 8a4 4 0 11-4 4M11 12l-7 7v2h3v-2h2v-2h2M3 3l18 18" />
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
        <!-- The store is far older than this browser of it: a cell can serve
             every KV write and read on the wire and still answer 404 here. -->
        <p v-if="verdict === 'absent'" style="font-size:13px;">
          <code>/api/v1/resources/kv/*</code> is newer than the KV store itself —
          the batch route can be alive on this cell while this listing is not.
        </p>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <template v-else>
      <!-- Stale rows presented as live is the failure mode this banner exists
           to prevent: the panel keeps the last page that loaded. -->
      <div v-if="isStale" class="status-banner banner-bad view-banner">
        <span>
          <strong>Could not refresh this page of keys</strong> ·
          {{ listErrorText }} · showing the last page that loaded
          ({{ stamp(listPanel) }}).
        </span>
      </div>

      <!-- Filters. The namespace is not a filter, it is the ADDRESS: KV keys
           are unique inside one namespace and the stored procedure takes it as
           a required argument, so nothing is listable without one. -->
      <div class="card filters">
        <div class="card-body filter-rows">
          <div class="filter-row">
            <div class="filter-field-col filter-field-wide">
              <label class="label-xs" for="kv-namespace">Namespace</label>
              <!-- A select rather than the Autocomplete the timers page uses:
                   the exact key count belongs IN the option, and Autocomplete
                   renders plain strings. A tenant has a handful of namespaces
                   (they are declared, not derived from traffic), so the list
                   stays short by construction. -->
              <select
                v-if="!namespacesUnavailable"
                id="kv-namespace"
                v-model="namespace"
                class="input"
                :disabled="nsLoading && !options.length"
              >
                <option value="">{{ namespacePlaceholder }}</option>
                <option v-for="o in options" :key="o.namespace" :value="o.namespace">{{ o.label }}</option>
              </select>
              <input
                v-else
                id="kv-namespace"
                v-model="namespaceDraft"
                class="input"
                placeholder="namespace"
                spellcheck="false"
                @change="applyNamespaceDraft"
              />
            </div>

            <!-- The prefix replaces "jump to page 47". Keys in this store are
                 structured (`wh.deliver:<tenant>:<id>`), so a prefix is a tight
                 index range — one seek, the same cost as the first page — which
                 is the only navigation a keyset walk can offer. -->
            <div class="filter-field-col filter-field-wide">
              <label class="label-xs" for="kv-prefix">Key prefix</label>
              <input
                id="kv-prefix"
                v-model="prefixDraft"
                class="input"
                placeholder="prefix — e.g. wh.deliver:"
                spellcheck="false"
                :title="prefixHint"
                @keyup.enter="applyPrefixNow"
              />
            </div>

            <div class="filter-field-col">
              <label class="label-xs" for="kv-limit">Page size</label>
              <select id="kv-limit" v-model.number="limit" class="input">
                <option :value="50">50</option>
                <option :value="100">100</option>
                <option :value="250">250</option>
              </select>
            </div>

            <!-- §2.5 D6. Said on the page and not only in the plan: a console
                 that can read application state is expected to be able to edit
                 it, and the absence of an Edit button is not an answer. -->
            <span class="filter-hint filter-field-right" :title="READ_ONLY_TITLE">
              Read-only — this page never writes to the KV store.
            </span>
          </div>
          <!-- Hints live UNDER the row, never inside a column: the row aligns
               its columns at the bottom, so a one-line hint beside a two-line
               one lifted each label and input by a different amount and the
               card read as two rows. Down here a hint can be any length. -->
          <div class="filter-foot">
            <span v-if="namespacesUnavailable" class="filter-hint">
              Namespace list unavailable — type a namespace and press Enter
            </span>
            <span v-else-if="unlistedNamespace" class="filter-hint">
              Not in this tenant's namespace list — asking the broker anyway
            </span>
            <span v-else-if="noNamespaces" class="filter-hint">Nothing written to the store yet.</span>
            <span v-else class="filter-hint">Counts are exact, taken with the list.</span>
            <span class="filter-hint">{{ prefixHint }}</span>
          </div>
        </div>
      </div>

      <!-- ===================== The page =====================
           One request, one page, no client-side slice: DataTable is fed
           exactly what the broker returned and a `pageSize` at least as large
           as the page, so its own offset pager never appears. Walking is the
           keyset strip below it. -->
      <div v-if="showTable" class="kv-sheet">
        <DataTable
          title="Keys"
          :subtitle="stamp(listPanel)"
          :columns="columns"
          :data="pageRows"
          :loading="firstLoad"
          :page-size="tablePageSize"
          clickable
          @row-click="openRow"
        >
          <template #key="{ row }">
            <span class="font-mono kv-key" :title="row.key">{{ row.key }}</span>
          </template>

          <template #size="{ row }">
            <span v-if="row.size !== null" class="num" :title="SIZE_TITLE">{{ formatBytes(row.size) }}</span>
            <span v-else class="num mute">—</span>
          </template>

          <template #version="{ row }">
            <span class="num" :title="VERSION_TITLE">{{ row.version ?? '—' }}</span>
          </template>

          <template #expires="{ row }">
            <div :class="row.expired ? 'kv-expiry warn' : 'kv-expiry'">{{ row.expires }}</div>
            <div v-if="row.expiresAt" class="font-mono kv-sub" :title="formatTimestampUtc(row.expiresAt)">
              {{ formatTimestamp(row.expiresAt) }}
            </div>
          </template>

          <template #updated="{ row }">
            <span class="font-mono kv-sub" :title="formatTimestampUtc(row.updatedAt)">
              {{ formatTimestamp(row.updatedAt) }}
            </span>
          </template>

          <!-- The state cell carries the marker the row is dimmed by; see the
               `:has()` rule in this file's styles. -->
          <template #state="{ row }">
            <span
              class="chip"
              :class="row.state.tone === 'warn' ? 'chip-warn kv-expired' : 'chip-mute'"
              :title="row.state.title"
            >{{ row.state.label }}</span>
            <div v-if="row.state.note" class="kv-sub" :title="row.state.title">{{ row.state.note }}</div>
          </template>
        </DataTable>

        <!-- Keyset pager: no page count, because there is no total without a
             count and this is an index range, not an offset. -->
        <div class="pager kv-pager">
          <span class="pager-count">
            <template v-if="firstLoad">Loading…</template>
            <template v-else>
              Page <span class="font-mono tabular-nums">{{ loadedPage }}</span>
              · {{ pageRows.length }} row{{ pageRows.length === 1 ? '' : 's' }}
              <template v-if="pageBytes !== null">
                · <span :title="PAGE_BYTES_TITLE">{{ formatBytes(pageBytes) }} of values</span>
              </template>
              <template v-if="pageEnd"> · {{ pageEnd }}</template>
            </template>
          </span>
          <div class="pager-nav">
            <button class="btn btn-ghost" :disabled="!canPrev || loading" @click="goPrev">Previous</button>
            <button class="btn btn-ghost" :disabled="!canNext || loading" @click="goNext">Next</button>
          </div>
        </div>
      </div>

      <!-- Nothing to table, for five different reasons that a naive list would
           render as one "no rows": the call failed, no namespace is picked, the
           tenant has no namespaces at all, the prefix matches nothing, or the
           namespace really is empty. Only the first is a failure. -->
      <div v-else class="card">
        <div v-if="listError" class="empty-state empty-state-failed">
          <h3>{{ listErrorText }}</h3>
          <p>Nothing loaded — this is a failure, not an empty namespace.</p>
          <button class="btn btn-ghost" @click="reload">Retry</button>
        </div>

        <div v-else-if="!namespace" class="empty-state">
          <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
            <path stroke-linecap="round" d="M14 8a4 4 0 11-4 4M11 12l-7 7v2h3v-2h2v-2h2" />
          </svg>
          <template v-if="noNamespaces">
            <h3>This tenant has no KV namespaces</h3>
            <p>
              Nothing has been written to the key-value store on this cluster yet.
              A namespace appears here the moment its first key is set.
            </p>
          </template>
          <template v-else>
            <h3>Pick a namespace</h3>
            <p>
              Keys are listed per namespace, never tenant-wide: a key is unique
              inside one namespace, and the counts in the picker are exact.
            </p>
          </template>
        </div>

        <div v-else class="empty-state">
          <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
            <path stroke-linecap="round" d="M14 8a4 4 0 11-4 4M11 12l-7 7v2h3v-2h2v-2h2" />
          </svg>
          <h3>
            No keys<template v-if="appliedPrefix"> under this prefix</template>
            <template v-else-if="loadedPage > 1"> on this page</template>
          </h3>
          <p v-if="appliedPrefix">
            Nothing in <strong class="font-mono">{{ namespace }}</strong> starts with
            <strong class="font-mono">{{ appliedPrefix }}</strong>. A prefix is a byte
            range, not a search: it matches from the first character only, and
            <code>%</code> and <code>_</code> are ordinary characters here.
          </p>
          <p v-else>
            <strong class="font-mono">{{ namespace }}</strong> holds no keys right now —
            not even expired ones, which this page would show.
          </p>
          <button v-if="appliedPrefix" class="btn btn-ghost" @click="clearPrefix">Clear the prefix</button>
          <button v-if="canPrev" class="btn btn-ghost" @click="goPrev">Previous page</button>
        </div>
      </div>
    </template>

    <!-- ===================== One key =====================
         No second request: the list carries the value, which is what the
         stored procedure's 4 MiB page budget buys. The drawer is therefore a
         view of the PAGE's snapshot — it closes whenever a new page is asked
         for, rather than floating over rows it no longer belongs to. -->
    <DetailDrawer
      :open="Boolean(selectedRow)"
      title="Key"
      :subtitle="selectedRow?.key || ''"
      wide
      split
      @close="closeDrawer"
    >
      <div v-if="selectedRow" class="kv-fields">
        <DetailField label="Namespace" :value="loadedNamespace" mono />
        <DetailField label="Key" :value="selectedRow.key" mono copyable />
        <DetailField label="Version" :value="selectedRow.version ?? '—'" :title="VERSION_TITLE" />
        <DetailField
          label="Value size"
          :value="selectedRow.size === null ? '—' : formatBytes(selectedRow.size)"
          :title="SIZE_TITLE"
        />
        <DetailField
          label="Expires"
          :value="selectedRow.expiresAt ? `${selectedRow.expires} · ${formatTimestamp(selectedRow.expiresAt)}` : 'never'"
          :title="selectedRow.expiresAt ? formatTimestampUtc(selectedRow.expiresAt) : 'No TTL — nothing but a delete removes this key'"
        />
        <DetailField
          label="Updated"
          :value="formatTimestamp(selectedRow.updatedAt)"
          :title="formatTimestampUtc(selectedRow.updatedAt)"
        />
        <DetailField
          label="State"
          :value="selectedRow.state.label"
          :tone="selectedRow.state.tone === 'warn' ? 'accent' : 'mid'"
          :title="selectedRow.state.title"
        />
      </div>

      <template #secondary>
        <div class="kv-value">
          <div class="label-xs kv-value-head">
            <span>Value</span>
            <button v-if="hasValue && canCopy" class="btn btn-ghost kv-copy" @click="copyValue">
              {{ copied ? 'Copied!' : 'Copy JSON' }}
            </button>
          </div>
          <!-- A stored JSON `null` is a value four bytes long, so the absence
               of the field — never `value === null` — is what "no value" means
               here. Only a keysOnly page produces it, which this console never
               asks for; the branch exists so a future one cannot render a lie. -->
          <JsonViewer v-if="hasValue" :value="selectedValue" />
          <div v-else class="empty-tile">
            This page carries no value for the key — the listing was asked for keys only.
          </div>
        </div>
      </template>

      <template #footer>
        <span class="kv-foot-note" :title="READ_ONLY_TITLE">Read-only view</span>
        <button class="btn btn-ghost" @click="closeDrawer">Close</button>
      </template>
    </DetailDrawer>
  </div>
</template>

<script setup>
// KV browser — the tenant's key-value store, namespace by namespace
// (PLAN_DASHBOARD_ACTIONS.md §2.5, PLAN_KV_TIMERS.md §5).
//
// TWO ROUTES, BOTH NEW, AND NEITHER IS THE KV API. `GET
// /api/v1/resources/kv/namespaces` and `POST /api/v1/resources/kv/list` live
// under /api/v1/resources, which the proxy classifies Read by prefix and
// method-agnostically: no feature gate, no quota interaction, and — the point
// — visible to a VIEWER. The batch route `POST /api/v1/kv` could answer the
// same questions and cannot be used here: it is Gated(Kv, Mixed), so the proxy
// refuses it to a viewer outright, it is metered as a KV batch, and `getPrefix`
// requires a prefix, which a console opening a namespace does not have (§1.5).
//
// THE LIST IS A POST because its cursor is a KEY. `?after=wh.deliver:
// promotion-publication:b15f6d46…` in a query string is a customer identifier
// in four components' access logs (§5.5), so the cursor travels in the body —
// and for the same reason this page keeps the prefix out of its own URL while
// the namespace, which is a declared name and not data, lives in `?ns=`.
//
// NO PRIVATE TICKER, AND NO AUTO-REFRESH. Every call here is a Postgres read
// on a tenant-scoped, metered, rate-limited route, and a keyset page that
// re-fetched under the reader would move rows while they are being read. The
// page refreshes when the operator asks (the shell's Refresh button) and on a
// cluster switch — and a stable verdict short-circuits even that: a 404 is
// remembered for the whole cluster epoch (stores/routeSupport.js), so no second
// page of the dashboard re-asks it, while a 403 or a 503 stops THIS page's
// calls until it is remounted or "Check again" is pressed.
//
// THREE KEY COUNTS, THREE PROVENANCES, AND TWO DIFFERENT POPULATIONS. The page
// says which is which, because they can disagree and both still be right:
//   · `≈ N live keys` the tenant's footprint across every namespace, from the
//                     queue listing's root. queen.kv_usage_step_v1 counts LIVE
//                     rows only, every five minutes, and above 200k rows it is
//                     a scaled shard sample — hence `≈`, twice over
//   · `N in <ns>`     EXACT, one namespace, a count(*) of EVERY row taken when
//                     the picker was filled — expired-awaiting-sweep included,
//                     so it can exceed the figure beside it
//   · the page        exactly the rows on screen; the keyset walk has no
//                     total, by construction
//
// NO WRITES, ANYWHERE (§2.5 D6). Not because the proxy would refuse them — it
// allows every role but Viewer on the batch route — but because editing
// application state by hand needs guard rails this page does not have (a
// version CAS in the form, an audit trail). The page says so rather than
// leaving an operator to conclude the button is missing.
import { computed, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import DataTable from '@/components/DataTable.vue'
import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import { kv as kvApi, describeApiError } from '@/api'
import { formatBytes, formatNumber, toNum, useApi } from '@/composables/useApi'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { describeVerdict, gatedVerdict } from '@/composables/useGatedVerdict'
import { useKeysetPager } from '@/composables/useKeysetPager'
import {
  describeKvState, describePageEnd, formatExpiry, kvListBody, kvRefusalText,
  namespaceOptions, sweeperUsageIsMeasured, valueBytes,
} from '@/composables/useKvView'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { routeSupport } from '@/stores/routeSupport'

const route = useRoute()
const router = useRouter()
const { epoch, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
const { notifyError } = useToast()

/** How long the prefix box waits for the typing to stop. Every applied prefix
 *  is a metered Postgres read, so the box does not fire per keystroke — and
 *  Enter skips the wait for an operator who already knows what they want. */
const PREFIX_DEBOUNCE_MS = 300

// The tenant figure and the namespace figure do NOT measure the same
// population, and saying so is the whole job of these two titles.
// queen.kv_usage_step_v1 (026_kv_sweeper.sql) counts LIVE rows only, on a
// five-minute cadence, and degrades to a scaled shard sample above 200k rows;
// queen.kv_namespaces_v1 counts every row of the namespace, expired ones
// included, exactly. So the exact number can exceed the approximate one, and an
// operator who is not told why would read that as a bug in one of them.
const TENANT_TOTAL_TITLE =
  'Every LIVE key this tenant holds on the cell, across all namespaces, from the sweeper\'s cached ' +
  'measurement on the queue listing: refreshed about every five minutes, and above 200k rows it is a ' +
  'scaled sample. Expired rows awaiting sweep are NOT in it, so a namespace count below can be larger'
const TENANT_BYTES_TITLE =
  'What the live keys occupy in Postgres — the stored size of each value, after compression and TOAST, ' +
  'from the same five-minute sweeper snapshot. Not comparable with a page\'s byte figure, which measures ' +
  'the JSON text the listing serialized'
const EXACT_COUNT_TITLE =
  'Exact count for this namespace, taken when the picker was filled. It counts every row, expired ones ' +
  'awaiting sweep included — an expired key is not a live key, but it is still an occupied one'
const SIZE_TITLE =
  'The value\'s JSON size, measured in this browser. Postgres counts the same value a byte or two ' +
  'larger — jsonb\'s text form spaces its separators — and the page total below is Postgres\'s count'
const PAGE_BYTES_TITLE =
  'What this page cost to serialize, as Postgres measured it: octet_length(value::text) summed over ' +
  'the rows. A page stops at 4 MiB even when the row limit is not reached'
const VERSION_TITLE =
  'Bumped on every write to this key. It is what a compare-and-set writes against, so a version that ' +
  'moved between two looks means somebody else wrote in between'
const READ_ONLY_TITLE =
  'The console lists and reads keys; it never sets, deletes or expires one. Editing application state ' +
  'by hand needs a version check and an audit trail this page does not have (§2.5 D6)'

// ---------------------------------------------------------------------------
// The tenant's footprint, from the queue listing's root (a sweeper snapshot,
// shared with every other page through the TTL-cached store).
// ---------------------------------------------------------------------------
const queuesStore = useQueuesStore()
const { kvRows, kvBytes, fetchQueues } = queuesStore

// ---------------------------------------------------------------------------
// The query: namespace (the address, in the URL), prefix (a byte range),
// page size. Each of the three starts a NEW sequence — the cursors on the
// pager's stack address the old one and would silently page through it.
// ---------------------------------------------------------------------------
const namespace = ref(typeof route.query.ns === 'string' ? route.query.ns : '')
const namespaceDraft = ref(namespace.value)
const prefixDraft = ref('')
const appliedPrefix = ref('')
const limit = ref(100)

/** What the rows on screen answer. A change here invalidates them, which is
 *  why it is compared rather than assumed: without it, switching namespaces
 *  keeps the previous namespace's keys on screen under the new namespace's
 *  name until the next page lands. */
const queryKey = computed(() => JSON.stringify([namespace.value, appliedPrefix.value, limit.value]))
const loadedKey = ref(null)
const loadedNamespace = ref('')

const pager = useKeysetPager()
// Destructured so the template reads them as plain bindings (Vue unwraps a ref
// that is a top-level binding, not one reached through an object).
const { canPrev, canNext } = pager
/**
 * The page number the ROWS ON SCREEN belong to, which is not always the one the
 * pager has walked to: a Next whose request then fails leaves the cursor stack
 * one deeper than the rows, and a strip reading "Page 2" over page 1's rows
 * would contradict the banner above it saying the refresh failed. Advanced only
 * on a landed page, exactly like `loadedKey`.
 */
const loadedPage = ref(1)
const verdict = ref(null)

/** A new query: the cursors address a sequence that no longer exists. */
const resetQuery = () => {
  pager.reset()
  loadedPage.value = 1
}

// ---------------------------------------------------------------------------
// The two calls.
//
// ONE route-support family for both: they ship together, so a cell that lacks
// one lacks the other, and a second family key would make the page probe twice
// to learn the same fact. `probe: true` says this page RENDERS the stable
// answers itself: "not on this cell" on an older broker, "not in this
// cluster's plan" on a 403 feature_gated, and "not being served right now" on
// the cell's 503 — all three are the quiet card above, and a toast on top of it
// would report the same fact twice (observed with the cell's kv switch off).
// ---------------------------------------------------------------------------
const listKeys = routeSupport.guard('kv', (body, config) =>
  kvApi.list(body, { ...config, probe: true }))
const listNamespaces = routeSupport.guard('kv', (config) =>
  kvApi.namespaces({ ...config, probe: true }))

const listPanel = useApi(listKeys, { immediate: false })
const nsPanel = useApi(listNamespaces, { immediate: false })

const options = computed(() => namespaceOptions(nsPanel.data.value))
const nsLoading = computed(() => nsPanel.loading.value)
/** The list failed and left nothing behind: the picker degrades to free text
 *  rather than to a dead control, exactly as the timers queue picker does. */
const namespacesUnavailable = computed(() => Boolean(nsPanel.error.value) && options.value.length === 0)
const noNamespaces = computed(
  () => !nsLoading.value && !nsPanel.error.value && nsPanel.lastUpdated.value !== null && options.value.length === 0
)
const unlistedNamespace = computed(
  () => Boolean(namespace.value) && options.value.length > 0 && !options.value.some(o => o.namespace === namespace.value)
)
const namespacePlaceholder = computed(() => {
  if (options.value.length) return 'Pick a namespace'
  return nsLoading.value ? 'Loading namespaces…' : 'No namespaces'
})
const exactCount = computed(() => {
  const hit = options.value.find(o => o.namespace === namespace.value)
  return hit ? hit.keys : null
})
// A cell that has not run the sweeper's usage phase yet answers `kvRows: 0`
// rather than omitting the field, so the strip would otherwise print
// `≈ 0 live keys` beside a selector reporting thousands. The selector is the
// witness; the rule lives in useKvView.js with the why.
const usageMeasured = computed(() => sweeperUsageIsMeasured(kvRows.value, options.value))

const rows = computed(() => {
  if (loadedKey.value !== queryKey.value) return []
  const r = listPanel.data.value?.rows
  return Array.isArray(r) ? r : []
})
const loading = computed(() => listPanel.loading.value)
const listError = computed(() => listPanel.error.value)
/** A broker 400 (`kv_bad_namespace` from a typed name) carries its sentence in
 *  `detail` and its code in `reason`, neither of which describeApiError reads:
 *  it only knows the proxy's `{error, code}` envelope. */
const listErrorText = computed(() =>
  (listError.value ? kvRefusalText(listError.value) || describeApiError(listError.value) : ''))
const firstLoad = computed(() => loading.value && loadedKey.value !== queryKey.value)
const isStale = computed(() => Boolean(listError.value) && rows.value.length > 0)
const showTable = computed(() => firstLoad.value || rows.value.length > 0)
// Every relative "Expires" cell is rendered against ONE instant — the load —
// which is what the freshness stamp beside the table claims. A column that
// silently drifted from the stamp above it would be the worse lie.
const asOf = computed(() => listPanel.lastUpdated.value?.getTime() ?? Date.now())

const pageRows = computed(() => rows.value.map((row) => ({
  ...row,
  size: valueBytes(row),
  expires: formatExpiry(row.expiresAt, asOf.value),
  state: describeKvState(row),
})))

// Deliberately not `sortable`. The rows are one page of a walk in byte order,
// and the cursor for the next page is the LAST ROW ON SCREEN: re-ordering them
// in the browser would make "Next continues after the bottom row" false while
// leaving it looking true.
const columns = [
  { key: 'key', label: 'Key' },
  { key: 'size', label: 'Size', align: 'right' },
  { key: 'version', label: 'Version', align: 'right' },
  { key: 'expires', label: 'Expires' },
  { key: 'updated', label: 'Updated' },
  { key: 'state', label: 'State' },
]

/** At least the whole page, so DataTable's own offset pager stays hidden — the
 *  walking is the keyset strip's job. The floor is the skeleton height while
 *  the first page is in flight, when there are no rows to measure. */
const tablePageSize = computed(() => Math.max(pageRows.value.length, 6))
const pageBytes = computed(() => (rows.value.length ? toNum(listPanel.data.value?.bytes) : null))
const pageEnd = computed(() => describePageEnd({
  rowCount: rows.value.length,
  truncated: listPanel.data.value?.truncated === true,
  limit: limit.value,
  prefixed: appliedPrefix.value !== '',
}))

const quiet = computed(() =>
  verdict.value && verdict.value !== 'transient' ? describeVerdict(verdict.value, 'kv') : null
)

// A cluster switch can land on a different cell running a different broker, so
// the cursor, the verdict, the namespace list and the open drawer all belong to
// the cluster we left. Checked inside `reload` as well as watched, because the
// shell fires every refresh callback on the same epoch change and the order of
// the two is not something this view should depend on.
let seenEpoch = epoch.value
const syncEpoch = () => {
  if (epoch.value === seenEpoch) return
  seenEpoch = epoch.value
  resetQuery()
  verdict.value = null
  loadedKey.value = null
  loadedNamespace.value = ''
  closeDrawer()
}
watch(epoch, syncEpoch)

const reload = async () => {
  syncEpoch()
  if (!namespace.value) return
  // A stable verdict cannot change by asking again. The guard would refuse the
  // request anyway; returning here also keeps the panel's error untouched.
  if (verdict.value && verdict.value !== 'transient') return
  // The drawer shows a row from the page this call is about to replace.
  closeDrawer()
  const asked = queryKey.value
  const ns = namespace.value
  const body = kvListBody({
    namespace: ns,
    prefix: appliedPrefix.value,
    after: pager.current(),
    limit: limit.value,
  })
  try {
    const data = await listPanel.execute(body)
    verdict.value = null
    loadedKey.value = asked
    loadedNamespace.value = ns
    loadedPage.value = pager.page.value
    pager.received(data)
  } catch (err) {
    verdict.value = gatedVerdict(err)
  }
}

const loadNamespaces = async () => {
  if (verdict.value && verdict.value !== 'transient') return
  try {
    await nsPanel.execute()
    // Deliberately NOT clearing the verdict on success: the two calls fly in
    // parallel on mount, and a namespace list that lands after the listing was
    // refused would wipe the refusal and put the page back into a state where
    // it asks again. The listing is the authority on whether the family is
    // being served; this call only fills the picker.
    //
    // One namespace is not a choice. More than one and the console must not
    // pick which of a tenant's stores to open — that is the operator's call,
    // and guessing it would put a key listing on screen nobody asked for.
    if (!namespace.value && options.value.length === 1) namespace.value = options.value[0].namespace
  } catch (err) {
    verdict.value = gatedVerdict(err)
  }
}

const goNext = async () => {
  if (!pager.next()) return
  await reload()
}
const goPrev = async () => {
  if (!pager.prev()) return
  await reload()
}

/** Re-ask after an upgrade or after an operator turned the family back on. */
const probeAgain = async () => {
  routeSupport.forget('kv')
  verdict.value = null
  await loadNamespaces()
  await reload()
}

// The namespace lives in the URL so a namespace an operator is looking at is
// something they can paste into a ticket. The PREFIX does not: it is a
// fragment of a key, and the reason the cursor travels in a POST body applies
// to the address bar, the browser history and a screenshot just as well.
watch(namespace, (ns) => {
  resetQuery()
  namespaceDraft.value = ns
  const current = typeof route.query.ns === 'string' ? route.query.ns : ''
  // Replace, never push, so Back leaves the page instead of walking the
  // picker's history.
  if (current !== ns) router.replace({ query: { ...route.query, ns: ns || undefined } })
  reload()
})
watch(appliedPrefix, () => { resetQuery(); reload() })
watch(limit, () => { resetQuery(); reload() })

// ---------------------------------------------------------------------------
// The prefix box. Debounced because each applied prefix is a metered read; NOT
// trimmed, because a space is a legal byte in a key and this is a byte range,
// not a search box (the stored procedure uses starts_with, so `%` and `_` are
// ordinary characters too).
// ---------------------------------------------------------------------------
let prefixTimer = null
const prefixPending = computed(() => prefixDraft.value !== appliedPrefix.value)

watch(prefixDraft, (val) => {
  clearTimeout(prefixTimer)
  prefixTimer = setTimeout(() => { appliedPrefix.value = val }, PREFIX_DEBOUNCE_MS)
})

const applyPrefixNow = () => {
  clearTimeout(prefixTimer)
  appliedPrefix.value = prefixDraft.value
}
const clearPrefix = () => {
  clearTimeout(prefixTimer)
  prefixDraft.value = ''
  appliedPrefix.value = ''
}

const prefixHint = computed(() => (prefixPending.value
  ? 'Applying…'
  : 'A byte range, not a search: it matches from the first character only.'))

/** The free-text fallback, used only when the namespace list did not load. */
const applyNamespaceDraft = () => {
  const typed = namespaceDraft.value.trim()
  if (typed === namespace.value) return
  namespace.value = typed
}

onUnmounted(() => clearTimeout(prefixTimer))

// ---------------------------------------------------------------------------
// One key. The value came with the page, so opening a row costs nothing.
// ---------------------------------------------------------------------------
const selectedRow = ref(null)
const copied = ref(false)
let copyTimer = null

const hasValue = computed(() => Boolean(selectedRow.value) && 'value' in selectedRow.value)
const selectedValue = computed(() => (hasValue.value ? selectedRow.value.value : null))
const canCopy = typeof navigator !== 'undefined' && Boolean(navigator.clipboard)

const openRow = (row) => {
  selectedRow.value = row
  copied.value = false
}

function closeDrawer() {
  selectedRow.value = null
}

const copyValue = async () => {
  if (!hasValue.value) return
  try {
    // Pretty-printed: what is copied is what the drawer shows, and a value
    // pasted into a ticket or a `queenctl kv set` is read by a human first.
    await navigator.clipboard.writeText(JSON.stringify(selectedValue.value, null, 2))
    copied.value = true
    clearTimeout(copyTimer)
    copyTimer = setTimeout(() => { copied.value = false }, 2000)
  } catch {
    notifyError('Could not copy to the clipboard', 'Copy failed')
  }
}

onUnmounted(() => clearTimeout(copyTimer))

// Registered NON-auto: the header's Refresh button and a cluster switch drive
// this page. The namespace counts are refreshed with it — they are exact, and
// an exact number that is an hour old is the one kind of lie this page's whole
// vocabulary exists to prevent.
useRefresh(async () => {
  await loadNamespaces()
  await reload()
})

// First load. Last, so nothing above can be reached before it is defined.
fetchQueues()
loadNamespaces()
reload()
</script>

<style scoped>
/* The namespace names and the key prefixes are long and dotted; the page-size
   select next to them is three digits wide and must not take the same room. */
.filter-field-wide { flex-basis: 260px; max-width: 340px; }

/* DataTable renders its own card and exposes no footer slot, so the keyset
   strip is a second card joined to the first: one border, one rule between
   them, no gap for a shadow to fall into. */
.kv-sheet > :deep(.card) {
  border-bottom-left-radius: 0; border-bottom-right-radius: 0; border-bottom: 0;
}
.kv-pager {
  border: 1px solid var(--bd);
  border-radius: 0 0 var(--r-card) var(--r-card);
  background: var(--ink-2);
}

/* Expired rows, greyed. DataTable takes no per-row class (and it is a shared
   component), so the row reaches its own dimming through the one cell that
   knows: `:has()` selects the <tr> that contains the expired chip, and the
   state cell itself stays at full contrast so the word is still readable. A
   browser that ignores `:has()` loses the dimming and keeps the chip and the
   "awaiting sweep" line — nothing depends on the colour alone. */
.kv-sheet :deep(tbody tr:has(.kv-expired) td) { opacity: 0.55; }
.kv-sheet :deep(tbody tr:has(.kv-expired) td:last-child) { opacity: 1; }

/* Keys run long — `wh.deliver:<tenant>:<uuid>` is 60 characters — and they are
   the column worth the room, so they wrap rather than push the rest off. */
.kv-key { font-size: 12px; word-break: break-all; }
.kv-expiry { font-size: 12px; color: var(--text-hi); font-weight: 500; }
.kv-expiry.warn { color: var(--warn-400); }
.kv-sub { font-size: 11px; color: var(--text-low); white-space: nowrap; }

.kv-fields { display: flex; flex-direction: column; gap: 14px; }

.kv-value-head {
  display: flex; align-items: baseline; justify-content: space-between;
  gap: 8px; margin-bottom: 6px;
}
.kv-copy { padding: 1px 6px; font-size: 10.5px; }

/* The drawer's footer is `justify-content: flex-end`; the note belongs on the
   other side of it, where a write button would have been. */
.kv-foot-note { margin-right: auto; font-size: 11.5px; color: var(--text-low); }
</style>
