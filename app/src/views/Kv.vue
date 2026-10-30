<template>
  <!-- The KV namespaces are the list (KvSide): one picked on the left, its
       keys on the right. The namespace is the address (?ns=); the key prefix
       is a filter of the keys, kept out of the address (see the script). -->
  <SubSidebar label="KV namespaces" :current="namespace || 'All namespaces'">
  <template #side>
    <KvSide
      :namespace="namespace"
      :options="options"
      :settled="nsSettled"
      :error-text="nsErrorText"
      :unavailable="quiet ? quiet.title : ''"
      @open="openNamespace"
    />
  </template>
  <div class="view-container kv">

    <!-- Every count here is exact and includes expired keys awaiting sweep;
         the titles say which read each one came from. -->
    <PageHead :title="namespace || 'All namespaces'">
      <template v-if="facts.length" #sub>
        <span class="facts">
          <span v-for="f in facts" :key="f.k" class="fact" :title="f.title"><span class="fact-k">{{ f.k }}</span>{{ f.v }}</span>
        </span>
      </template>
      <!-- §2.5 D6, said on the page: a console that can read application
           state is expected to edit it, and a missing button is no answer. -->
      <template #actions>
        <span v-if="namespace && stamp(listPanel)" class="kv-quiet" :title="formatTimestampUtc(listPanel.lastUpdated.value)">{{ stamp(listPanel) }}</span>
        <span class="kv-quiet" :title="READ_ONLY_TITLE">Read-only</span>
      </template>
    </PageHead>

    <!-- Not here / not in the plan / not being served right now: one card,
         no poll (composables/useGatedVerdict.js owns the rule and the words;
         stores/routeSupport.js remembers the first for the cluster epoch). -->
    <div v-if="quiet" class="card">
      <div class="empty-state">
        <Icon :name="verdict === 'gated' ? 'lock' : verdict === 'paused' ? 'pause' : 'key-round'" class="empty-state-icon" :size="28" :stroke="1.5" />
        <h3 :title="quietTitle">{{ quiet.title }}</h3>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <template v-else>
      <!-- The page keeps the last rows that loaded: say they are old. -->
      <div v-if="isStale" class="status-banner banner-bad view-banner">
        <span><strong>Could not refresh the keys</strong> · {{ listErrorText }} · showing the page that loaded {{ stampTime }}.</span>
      </div>

      <!-- The prefix replaces "jump to page 47": keys are structured
           (`wh.deliver:<tenant>:<id>`), so a prefix is a tight byte range,
           one seek, the cost of the first page. -->
      <PageTools v-if="namespace">
        <label class="tool-field kv-prefix" for="kv-prefix" :title="prefixHint">
          <span class="tool-label">Prefix</span>
          <input
            id="kv-prefix"
            v-model="prefixDraft"
            type="search"
            class="input"
            placeholder="e.g. wh.deliver:"
            spellcheck="false"
            autocomplete="off"
            @keyup.enter="applyPrefixNow"
          />
        </label>
        <template #view>
          <UiSelect v-model="limit" label="Show" :options="LIMITS" aria-label="Keys per page" />
        </template>
      </PageTools>

      <!-- One request, one page, nothing sliced in the browser. Walking is the
           keyset pager at the foot. Not sortable: the cursor for the next page
           is the LAST ROW ON SCREEN, so a re-ordered page would make "Next
           continues after the bottom row" false while it still looked true. -->
      <div class="card">
        <div v-if="showTable" class="kv-scroll">
          <table class="t kv-table">
            <thead>
              <tr>
                <th>Key</th>
                <th class="num" :title="SIZE_TITLE">Size</th>
                <th class="num" :title="VERSION_TITLE">Version</th>
                <th class="num">Expires</th>
                <th class="num">Updated</th>
              </tr>
            </thead>
            <tbody>
              <template v-if="firstLoad">
                <tr v-for="i in 6" :key="`sk-${i}`"><td colspan="5"><div class="skeleton kv-skeleton" /></td></tr>
              </template>
              <template v-else>
                <tr
                  v-for="row in pageRows"
                  :key="row.key"
                  class="kv-row"
                  :class="{ expired: row.expired === true }"
                  tabindex="0"
                  @click="openRow(row)"
                  @keydown.enter="openRow(row)"
                >
                  <td><span class="kv-key" :title="row.key">{{ row.key }}</span></td>
                  <td class="num" :title="SIZE_TITLE">{{ row.size === null ? '—' : formatBytes(row.size) }}</td>
                  <td class="num">{{ row.version ?? '—' }}</td>
                  <td class="num" :title="row.expired === true ? row.state.title : instantTitle(row.expiresAt)">
                    {{ row.expired === true ? `expired ${row.expires}` : row.expires }}
                  </td>
                  <td class="num" :title="instantTitle(row.updatedAt)">{{ compactStamp(row.updatedAt) }}</td>
                </tr>
              </template>
            </tbody>
          </table>
        </div>

        <!-- Nothing to table, for reasons a bare "no rows" would merge: the
             call failed, no namespace is picked, the tenant has none, the
             prefix matches nothing, or the namespace really is empty. Only
             the first is a failure. -->
        <div v-else-if="listError" class="empty-state empty-state-failed">
          <h3>Cannot read the keys</h3>
          <p>{{ listErrorText }}</p>
          <button class="btn btn-ghost" @click="reload">Retry</button>
        </div>
        <div v-else class="empty-state">
          <Icon name="key-round" class="empty-state-icon" :size="28" :stroke="1.5" />
          <p :title="emptyTitle">{{ emptyLine }}</p>
        </div>

        <!-- Keyset pager: no page count, because there is no total without a
             count and this is an index range, not an offset. -->
        <div v-if="namespace && (pageRows.length || canPrev)" class="pager kv-pager">
          <span class="pager-count">
            <template v-if="firstLoad">Loading…</template>
            <template v-else>
              Page {{ loadedPage }} · {{ exact(pageRows.length) }} {{ pageRows.length === 1 ? 'key' : 'keys' }}<template v-if="pageBytes !== null"> · <span :title="PAGE_BYTES_TITLE">{{ formatBytes(pageBytes) }}</span></template><template v-if="pageEnd"> · {{ pageEnd }}</template>
            </template>
          </span>
          <div class="pager-nav">
            <button class="btn btn-ghost" :disabled="!canPrev || loading" @click="goPrev"><Icon name="chevron-left" :size="14" />Previous</button>
            <button class="btn btn-ghost" :disabled="!canNext || loading" @click="goNext">Next<Icon name="chevron-right" :size="14" /></button>
          </div>
        </div>
      </div>
    </template>

    <!-- One key. No second request: the list carries the value (the 4 MiB
         page budget buys it), so the drawer is a view of the PAGE's snapshot
         and closes whenever a new page is asked for. -->
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
          :value="selectedRow.expiresAt ? `${selectedRow.expires} · ${formatTimestamp(parseBrokerInstant(selectedRow.expiresAt))}` : 'never'"
          :title="selectedRow.expiresAt ? instantTitle(selectedRow.expiresAt) : 'No TTL: nothing but a delete removes this key'"
        />
        <DetailField
          label="Updated"
          :value="formatTimestamp(parseBrokerInstant(selectedRow.updatedAt))"
          :title="instantTitle(selectedRow.updatedAt)"
        />
        <DetailField
          label="State"
          :value="selectedRow.state.note ? `${selectedRow.state.label}, ${selectedRow.state.note}` : selectedRow.state.label"
          tone="high"
          :title="selectedRow.state.title"
        />
      </div>

      <template #secondary>
        <div class="kv-value">
          <div class="label-xs kv-value-head">
            <span>Value</span>
            <button v-if="hasValue && canCopy" class="btn btn-ghost kv-copy" @click="copyValue">
              <Icon :name="copied ? 'check' : 'copy'" :size="12" />{{ copied ? 'Copied' : 'Copy JSON' }}
            </button>
          </div>
          <!-- A stored JSON `null` is a value four bytes long, so the absence
               of the field — never `value === null` — is what "no value" means
               here. Only a keysOnly page produces it, which this console never
               asks for; the branch exists so a future one cannot render a lie. -->
          <JsonViewer v-if="hasValue" :value="selectedValue" />
          <div v-else class="empty-tile">This page carries no value for the key: it was asked for keys only.</div>
        </div>
      </template>

      <template #footer>
        <span class="kv-foot-note" :title="READ_ONLY_TITLE">Read-only</span>
        <button class="btn btn-ghost" @click="closeDrawer">Close</button>
      </template>
    </DetailDrawer>
  </div>
  </SubSidebar>
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
// NO PRIVATE TICKER, AND NO AUTO-REFRESH. Every call here is a read of the
// replicated store on a tenant-scoped, metered, rate-limited route, and a keyset page that
// re-fetched under the reader would move rows while they are being read. The
// page refreshes when the operator asks (the shell's Refresh button) and on a
// cluster switch — and a stable verdict short-circuits even that: a 404 is
// remembered for the whole cluster epoch (stores/routeSupport.js), so no second
// page of the dashboard re-asks it, while a 403 or a 503 stops THIS page's
// calls until it is remounted or "Check again" is pressed.
//
// THE COUNTS, AND WHERE EACH COMES FROM. They are taken at different moments
// and can disagree while both are right, so each chip's title names its read:
//   · Keys (all namespaces)  the namespace listing's exact counts, summed; the
//                            queue listing's `kvRows` only when that fails
//   · Stored                 the queue listing's `kvBytes`: key and value bytes
//                            of every row, counted when that listing was read
//   · Keys (one namespace)   the namespace listing's exact count for it
//   · the page               exactly the rows on screen; the keyset walk has no
//                            total, by construction
// Every one of them includes expired rows awaiting sweep.
//
// NO WRITES, ANYWHERE (§2.5 D6). Not because the proxy would refuse them — it
// allows every role but Viewer on the batch route — but because editing
// application state by hand needs guard rails this page does not have (a
// version CAS in the form, an audit trail). The page says so rather than
// leaving an operator to conclude the button is missing.
import { computed, onUnmounted, ref, watch } from 'vue'

import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import Icon from '@/components/Icon.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import KvSide from '@/components/KvSide.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import UiSelect from '@/components/UiSelect.vue'
import { kv as kvApi, describeApiError } from '@/api'
import { formatBytes, toNum, useApi } from '@/composables/useApi'
import { formatTimestamp, formatTimestampCompact, formatTimestampTime, formatTimestampUtc } from '@/composables/useFormat'
import { describeVerdict, gatedVerdict } from '@/composables/useGatedVerdict'
import { useKeysetPager } from '@/composables/useKeysetPager'
import { exact, namespaceTotal } from '@/composables/useKvSide'
import {
  describeKvState, describePageEnd, formatExpiry, kvListBody, kvRefusalText,
  namespaceOptions, sweeperUsageIsMeasured, valueBytes,
} from '@/composables/useKvView'
import { useRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { stamp } from '@/composables/useStamp'
import { parseBrokerInstant } from '@/composables/useTimers'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { routeSupport } from '@/stores/routeSupport'

const { epoch } = useIdentity()
const { notifyError } = useToast()

/** How long the prefix box waits for the typing to stop. Every applied prefix
 *  is a metered read, so the box does not fire per keystroke — and
 *  Enter skips the wait for an operator who already knows what they want. */
const PREFIX_DEBOUNCE_MS = 300

/** Keys per page; the broker clamps any limit to 1..1000 on its own. */
const LIMITS = [25, 50, 100, 250]

const NAMESPACES_TITLE = 'Namespaces this tenant has, from the namespace list'
const ALL_KEYS_TITLE =
  'Every key of every namespace, the namespace list\'s exact counts added up. Expired keys awaiting ' +
  'sweep are included'
const TENANT_TOTAL_TITLE =
  'Every KV row this tenant stores on the cell, across all namespaces, counted when the queue listing ' +
  'was read. Expired rows awaiting sweep are included'
const TENANT_BYTES_TITLE =
  'The key and value bytes of every KV row this tenant stores on the cell, expired rows included, ' +
  'counted when the queue listing was read. Not comparable with a page\'s byte figure, which counts ' +
  'values only'
const EXACT_COUNT_TITLE =
  'Exact count for this namespace, taken when the namespace list was read. It counts every row, expired ones ' +
  'awaiting sweep included: an expired key is not a live key, but it is still an occupied one'
const SIZE_TITLE =
  'The value\'s JSON size, measured in this browser from the value the broker returned. The page ' +
  'total below is the broker\'s own count of the stored bytes'
const PAGE_BYTES_TITLE =
  'The stored size of the values on this page, as the broker counted it: the bytes of each value\'s ' +
  'JSON text, summed over the rows. A page stops at 4 MiB of values even when the row limit is not reached'
const VERSION_TITLE =
  'Bumped on every write to this key. It is what a compare-and-set writes against, so a version that ' +
  'moved between two looks means somebody else wrote in between'
const READ_ONLY_TITLE =
  'The console lists and reads keys; it never sets, deletes or expires one. Editing application state ' +
  'by hand needs a version check and an audit trail this page does not have (§2.5 D6)'
const ABSENT_NOTE =
  '/api/v1/resources/kv/* is newer than the KV store itself: the batch route can be alive on this cell ' +
  'while this listing is not.'

// ---------------------------------------------------------------------------
// The tenant's footprint, from the queue listing's root (shared with every
// other page through the TTL-cached store).
// ---------------------------------------------------------------------------
const queuesStore = useQueuesStore()
const { kvRows, kvBytes, fetchQueues } = queuesStore

// ---------------------------------------------------------------------------
// The query: namespace (the address, in the URL), prefix (a byte range),
// page size. Each of the three starts a NEW sequence — the cursors on the
// pager's stack address the old one and would silently page through it.
// ---------------------------------------------------------------------------
const namespace = ref('')
const prefixDraft = ref('')
const appliedPrefix = ref('')
const limit = ref(25)
useRouteState({ ns: namespace, limit })

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
/** The namespace list answered or failed at least once on this cluster. */
const nsSettled = computed(() => nsPanel.lastUpdated.value !== null || nsPanel.error.value !== null)
/** The list failed and left nothing behind: the sub sidebar says why and takes
 *  a typed namespace instead, exactly as the timers queue picker does. */
const nsErrorText = computed(() => {
  const err = nsPanel.error.value
  if (!err || options.value.length) return ''
  return kvRefusalText(err) || describeApiError(err)
})
const noNamespaces = computed(
  () => !nsPanel.loading.value && !nsPanel.error.value && nsPanel.lastUpdated.value !== null && options.value.length === 0
)
const exactCount = computed(() => {
  const hit = options.value.find(o => o.namespace === namespace.value)
  return hit ? hit.keys : null
})
// The queue listing can be older than the namespace listing (it is shared
// with every other page), so its figures could otherwise print `0 keys` beside
// a namespace list reporting thousands. That list is the witness; the rule
// lives in useKvView.js with the why.
const usageMeasured = computed(() => sweeperUsageIsMeasured(kvRows.value, options.value))

const facts = computed(() => {
  if (quiet.value) return []
  if (namespace.value) {
    return Number.isFinite(exactCount.value)
      ? [{ k: 'Keys', v: exact(exactCount.value), title: EXACT_COUNT_TITLE }]
      : []
  }
  const out = []
  if (nsPanel.lastUpdated.value !== null) {
    out.push({ k: 'Namespaces', v: exact(options.value.length), title: NAMESPACES_TITLE })
    const total = namespaceTotal(options.value)
    if (total !== null) out.push({ k: 'Keys', v: exact(total), title: ALL_KEYS_TITLE })
    else if (usageMeasured.value) out.push({ k: 'Keys', v: exact(kvRows.value), title: TENANT_TOTAL_TITLE })
  } else if (usageMeasured.value) {
    out.push({ k: 'Keys', v: exact(kvRows.value), title: TENANT_TOTAL_TITLE })
  }
  if (usageMeasured.value && kvBytes.value !== null) out.push({ k: 'Stored', v: formatBytes(kvBytes.value), title: TENANT_BYTES_TITLE })
  return out
})

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
// which is what the freshness stamp in the head claims. A column that
// silently drifted from the stamp above it would be the worse lie.
const asOf = computed(() => listPanel.lastUpdated.value?.getTime() ?? Date.now())
const stampTime = computed(() => (listPanel.lastUpdated.value ? `at ${formatTimestampTime(listPanel.lastUpdated.value)}` : 'earlier'))

const pageRows = computed(() => rows.value.map((row) => ({
  ...row,
  size: valueBytes(row),
  expires: formatExpiry(row.expiresAt, asOf.value),
  state: describeKvState(row),
})))

const pageBytes = computed(() => (rows.value.length ? toNum(listPanel.data.value?.bytes) : null))
const pageEnd = computed(() => describePageEnd({
  rowCount: rows.value.length,
  truncated: listPanel.data.value?.truncated === true,
  limit: limit.value,
  prefixed: appliedPrefix.value !== '',
}))

/** The empty card's one line, and what it leaves out. */
const emptyLine = computed(() => {
  if (!namespace.value) return noNamespaces.value ? 'No KV namespaces yet' : 'Pick a namespace'
  if (loadedKey.value !== queryKey.value) return 'Reading keys…'
  if (appliedPrefix.value) return `No key starts with ${appliedPrefix.value}`
  if (loadedPage.value > 1) return 'No keys on this page'
  return `No keys in ${namespace.value}`
})
const emptyTitle = computed(() => {
  if (!namespace.value) {
    return noNamespaces.value
      ? 'Nothing has been written to the key-value store on this cluster yet. A namespace appears with its first key.'
      : 'Keys are listed per namespace, never tenant-wide: a key is unique inside one namespace.'
  }
  if (appliedPrefix.value) {
    return 'A prefix is a byte range, not a search: it matches from the first character only, and % and _ are ordinary characters here.'
  }
  return 'Not even an expired key, which this page would show.'
})

const quiet = computed(() =>
  verdict.value && verdict.value !== 'transient' ? describeVerdict(verdict.value, 'kv') : null
)
const quietTitle = computed(() => (quiet.value
  ? (verdict.value === 'absent' ? `${quiet.value.detail} ${ABSENT_NOTE}` : quiet.value.detail)
  : ''))

// Both halves of a stamp go through the one parser: the broker prints up to
// six fractional digits.
const compactStamp = (value) => formatTimestampCompact(parseBrokerInstant(value))
const instantTitle = (value) => {
  const at = parseBrokerInstant(value)
  return at ? `${formatTimestamp(at)} · ${formatTimestampUtc(at)}` : ''
}

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
    // being served; this call only fills the namespace list.
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
//
// A prefix belongs to the namespace it was picked in, so a new namespace opens
// whole. `sync`, so the prefix is already cleared when the reload watcher
// below runs: it sees both changes in one flush and asks once.
let prefixTimer = null
watch(namespace, () => {
  if (!prefixDraft.value && !appliedPrefix.value) return
  clearTimeout(prefixTimer)
  prefixDraft.value = ''
  appliedPrefix.value = ''
}, { flush: 'sync' })
watch(appliedPrefix, value => { prefixDraft.value = value })
watch([namespace, appliedPrefix, limit], () => { resetQuery(); reload() })

// ---------------------------------------------------------------------------
// The prefix box. Debounced because each applied prefix is a metered read; NOT
// trimmed, because a space is a legal byte in a key and this is a byte range,
// not a search box (the stored procedure uses starts_with, so `%` and `_` are
// ordinary characters too).
// ---------------------------------------------------------------------------
const prefixPending = computed(() => prefixDraft.value !== appliedPrefix.value)

watch(prefixDraft, (val) => {
  clearTimeout(prefixTimer)
  prefixTimer = setTimeout(() => { appliedPrefix.value = val }, PREFIX_DEBOUNCE_MS)
})

const applyPrefixNow = () => {
  clearTimeout(prefixTimer)
  appliedPrefix.value = prefixDraft.value
}

const prefixHint = computed(() => (prefixPending.value
  ? 'Applying…'
  : 'Keys that start with this. A byte range, not a search: it matches from the first character only.'))

/** A namespace typed in the sub sidebar and opened with Enter, listed or not. */
const openNamespace = (typed) => {
  if (typeof typed === 'string' && typed !== namespace.value) namespace.value = typed
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
/* The head's facts: small chips beside the title, as in the Queues section
   (the same 16px after the title). On a narrow screen they wrap under the
   title rather than being cut. */
.kv :deep(.page-id) { flex-wrap: wrap; gap: 6px 16px; }
.kv :deep(.page-sub) { overflow: visible; white-space: normal; }
.facts { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.fact {
  display: inline-flex; align-items: baseline; gap: 5px;
  padding: 2px 8px; border-radius: var(--r-chip);
  background: var(--ink-3); border: 1px solid var(--bd);
  font-size: 11.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap;
}
.fact-k { color: var(--text-low); }
.kv-quiet { font-size: 12px; color: var(--text-low); white-space: nowrap; font-variant-numeric: tabular-nums; }

/* The prefix field takes the room a long key prefix needs. */
.kv-prefix .input { width: 240px; }

/* The table scrolls inside its card on a narrow screen; one line per row. */
.kv-scroll { overflow-x: auto; }
.kv-table th, .kv-table td { white-space: nowrap; }
.kv-table td { height: 36px; }
.kv-row { cursor: pointer; }
.kv-row:focus-visible { outline: 2px solid var(--ring); outline-offset: -2px; }
/* Keys run long (`wh.deliver:<tenant>:<uuid>`) and are the column worth the
   room: in full up to a point, then cut, the whole of it on hover and in the
   drawer. */
.kv-key {
  display: block; max-width: 640px;
  overflow: hidden; text-overflow: ellipsis;
  font-family: var(--font-mono); font-size: 12px; color: var(--text-hi);
}
/* An expired row awaiting sweep reads quieter, in the low ink that still
   holds its contrast; its Expires cell says the word. */
.kv-row.expired td, .kv-row.expired .kv-key { color: var(--text-low); }
.kv-row.expired td:nth-child(4) { color: var(--text-mid); }
.kv-skeleton { height: 16px; }

.kv-pager { flex-wrap: wrap; gap: 8px 16px; }
.kv-pager .pager-count { font-size: 12px; font-variant-numeric: tabular-nums; }
.kv-pager .btn { gap: 4px; }

.kv-fields { display: flex; flex-direction: column; gap: 14px; }
.kv-value-head {
  display: flex; align-items: baseline; justify-content: space-between;
  gap: 8px; margin-bottom: 6px;
}
.kv-copy { gap: 4px; padding: 1px 6px; font-size: 11px; }

/* The drawer's footer is `justify-content: flex-end`; the note belongs on the
   other side of it, where a write button would have been. */
.kv-foot-note { margin-right: auto; font-size: 12px; color: var(--text-low); }

@media (max-width: 640px) {
  .kv-prefix { flex: 1 1 100%; }
  .kv-prefix .input { width: 100%; }
}
</style>
