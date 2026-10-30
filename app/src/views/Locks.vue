<template>
  <!-- The lock name prefixes are the list (LocksSide), a filter list: picked
       on the left (?prefix=), the permits held under the pick on the right. -->
  <SubSidebar label="Locks" :current="appliedPrefix || 'All held locks'">
  <template #side>
    <LocksSide
      :entries="sideEntries"
      :more="side ? side.more : false"
      :prefix="appliedPrefix"
      :error-text="sideErrorText"
      :unavailable="quiet ? quiet.title : ''"
    />
  </template>
  <div class="view-container locks">

    <PageHead :title="appliedPrefix || 'All held locks'">
      <template v-if="facts.length" #sub>
        <span class="facts">
          <span v-for="f in facts" :key="f.k" class="fact" :title="f.title"><span class="fact-k">{{ f.k }}</span>{{ f.v }}</span>
        </span>
      </template>
      <!-- Said on the page: a console that shows who holds a lock is expected
           to be able to take it away, and a missing button is not an answer. -->
      <template #actions>
        <span v-if="stamp(listPanel)" class="locks-quiet" :title="formatTimestampUtc(listPanel.lastUpdated.value)">{{ stamp(listPanel) }}</span>
        <span class="locks-quiet" :title="READ_ONLY_TITLE">Read-only</span>
      </template>
    </PageHead>

    <!-- Not here / not in the plan / not being served right now. The page
         reads the console's KV listing, so these are that family's answers
         (composables/useGatedVerdict.js owns the rule and the words). -->
    <div v-if="quiet" class="card">
      <div class="empty-state">
        <Icon name="lock" class="empty-state-icon" :size="28" :stroke="1.5" />
        <h3 :title="`${quiet.detail} A lock is a row of the namespace ${LOCKS_NAMESPACE}, read through the KV listing.`">{{ quiet.title }}</h3>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <template v-else>
      <!-- A lock list is only true at the instant it was read: say how old. -->
      <div v-if="isStale" class="status-banner banner-bad view-banner">
        <span><strong>Could not refresh the locks</strong> · {{ listErrorText }} · showing the page that loaded {{ stampTime }}.</span>
      </div>

      <PageTools>
        <label class="tool-field locks-prefix" for="locks-prefix" :title="prefixHint">
          <span class="tool-label">Name starts with</span>
          <input
            id="locks-prefix"
            v-model="prefixDraft"
            type="search"
            class="input"
            placeholder="e.g. sync:"
            spellcheck="false"
            autocomplete="off"
            @keyup.enter="applyPrefixNow"
          />
        </label>
        <template #view>
          <UiSelect v-model="limit" label="Show" :options="LIMITS" aria-label="Permits per page" />
        </template>
      </PageTools>

      <div class="card">
        <div v-if="firstLoad || permits.length" class="locks-scroll">
          <table class="t locks-table">
            <thead>
              <tr>
                <th :title="LOCK_TITLE">Lock</th>
                <th :title="OWNER_TITLE">Holder</th>
                <th class="num" :title="HELD_TITLE">Held for</th>
                <th class="num" :title="RENEWED_TITLE">Renewed</th>
                <th class="num" :title="EXPIRES_TITLE">Expires</th>
              </tr>
            </thead>
            <tbody>
              <template v-if="firstLoad">
                <tr v-for="i in 4" :key="`sk-${i}`"><td colspan="5"><div class="skeleton locks-skeleton" /></td></tr>
              </template>
              <template v-else>
                <tr
                  v-for="p in permits"
                  :key="p.key"
                  class="locks-row"
                  tabindex="0"
                  @click="openPermit(p)"
                  @keydown.enter="openPermit(p)"
                >
                  <td>
                    <span class="locks-name">
                      <span class="locks-lock" :title="p.name">{{ p.name }}</span>
                      <span v-if="p.shared" class="locks-tag" :title="SLOT_TITLE">slot {{ p.slot }}</span>
                      <span v-if="p.foreign" class="locks-tag" :title="FOREIGN_TITLE">not a lock</span>
                    </span>
                  </td>
                  <td>
                    <span v-if="p.owner" class="locks-owner" :title="p.owner">{{ p.owner }}</span>
                    <span v-else class="locks-none" :title="NO_OWNER_TITLE">—</span>
                  </td>
                  <td class="num" :title="`Since ${instantTitle(p.since)}`">{{ formatHeldFor(p.since, asOf) }}</td>
                  <td class="num" :title="instantTitle(p.renewedAt)">{{ formatAgo(p.renewedAt, asOf) }}</td>
                  <td class="num" :title="instantTitle(p.expiresAt)">{{ formatExpiresIn(p.expiresAt, asOf) }}</td>
                </tr>
              </template>
            </tbody>
          </table>
        </div>

        <div v-else-if="listError" class="empty-state empty-state-failed">
          <h3>Cannot read the locks</h3>
          <p>{{ listErrorText }}</p>
          <button class="btn btn-ghost" @click="refreshAll">Retry</button>
        </div>
        <div v-else class="empty-state">
          <Icon name="lock" class="empty-state-icon" :size="28" :stroke="1.5" />
          <p :title="emptyTitle">{{ emptyLine }}</p>
        </div>

        <!-- Keyset pager: no page count, because there is no total without a
             count and this is an index range, not an offset. -->
        <div v-if="permits.length || canPrev" class="pager locks-pager">
          <span class="pager-count">
            Page {{ loadedPage }} · {{ exact(permits.length) }} {{ permits.length === 1 ? 'permit' : 'permits' }}<template v-if="pageEnd"> · {{ pageEnd }}</template>
          </span>
          <div class="pager-nav">
            <button class="btn btn-ghost" :disabled="!canPrev || loading" @click="goPrev"><Icon name="chevron-left" :size="14" />Previous</button>
            <button class="btn btn-ghost" :disabled="!canNext || loading" @click="goNext">Next<Icon name="chevron-right" :size="14" /></button>
          </div>
        </div>
      </div>
    </template>

    <!-- One permit. No second request: the row came with the page, so the
         drawer is a view of the page's snapshot and closes whenever a new
         page is asked for. -->
    <DetailDrawer
      :open="Boolean(selected)"
      :title="selected?.foreign ? 'Row' : 'Lock'"
      :subtitle="selected?.name || ''"
      wide
      split
      @close="closeDrawer"
    >
      <div v-if="selected" class="locks-fields">
        <DetailField label="Name" :value="selected.name" mono copyable />
        <DetailField v-if="selected.shared" label="Slot" :value="selected.slot" :title="SLOT_TITLE" />
        <DetailField
          label="Holder"
          :value="selected.owner || '—'"
          :title="selected.owner ? OWNER_TITLE : NO_OWNER_TITLE"
          mono
          :copyable="Boolean(selected.owner)"
        />
        <DetailField
          label="Held for"
          :value="`${formatHeldFor(selected.since, asOf)} · since ${localStamp(selected.since)}`"
          :title="HELD_TITLE"
        />
        <DetailField
          label="Renewed"
          :value="`${formatAgo(selected.renewedAt, asOf)} · ${localStamp(selected.renewedAt)}`"
          :title="RENEWED_TITLE"
        />
        <DetailField
          label="Expires"
          :value="`${formatExpiresIn(selected.expiresAt, asOf)} · ${localStamp(selected.expiresAt)}`"
          :title="EXPIRES_TITLE"
        />
        <DetailField
          label="Lifetime"
          :value="selectedLifetime === null ? '—' : formatSpan(selectedLifetime * 1000)"
          :title="LIFETIME_TITLE"
        />
        <DetailField label="Token" :value="selected.token ?? '—'" :title="TOKEN_TITLE" copyable />
        <DetailField label="KV row" :value="`${LOCKS_NAMESPACE} / ${selected.key}`" :title="ROW_TITLE" mono />
      </div>

      <template #secondary>
        <div v-if="selected" class="locks-machine">
          <template v-if="selectedGuard">
            <div class="label-xs">Guard</div>
            <p class="locks-note" :title="GUARD_TITLE">What <code>.guard(lock)</code> puts in a transaction.</p>
            <JsonViewer :value="selectedGuard" />
          </template>

          <template v-if="selectedRelease">
            <div class="label-xs locks-machine-head">
              <span>Release by hand</span>
              <button v-if="canCopy" class="btn btn-ghost locks-copy" @click="copyRelease">
                <Icon :name="copied ? 'check' : 'copy'" :size="12" />{{ copied ? 'Copied' : 'Copy as curl' }}
              </button>
            </div>
            <p class="locks-note" :title="RELEASE_TITLE">
              <code>POST /api/v1/locks</code> with this body releases this lease period only.
            </p>
            <JsonViewer :value="selectedRelease" />
          </template>

          <div v-if="selected.foreign" class="empty-tile" :title="FOREIGN_TITLE">
            Not a permit the broker wrote; only the KV routes can remove it.
          </div>
        </div>
      </template>

      <template #footer>
        <router-link class="locks-foot-link" :to="{ path: '/kv', query: { ns: LOCKS_NAMESPACE } }">
          Open the namespace in KV
        </router-link>
        <button class="btn btn-ghost" @click="closeDrawer">Close</button>
      </template>
    </DetailDrawer>
  </div>
  </SubSidebar>
</template>

<script setup>
// Locks — who holds each lock and semaphore permit, since when and until when.
//
// NO ROUTE OF ITS OWN. A permit is one KV row in the namespace `queen-locks`
// (server/src/locks.rs), so this page is the console's KV listing asked for
// that namespace: `POST /api/v1/resources/kv/list`, Read at the proxy and
// read-only at the broker. A Viewer can open it, which the locks route itself
// could not offer: `POST /api/v1/locks` is read-write even for a `get`.
// composables/useLocks.js holds how a row reads as a permit.
//
// TWO READS AT MOST. The table is one keyset page, filtered by a name prefix
// (?prefix=, a byte range). The list beside it counts every held permit, so a
// filter or a page turn never rebuilds it: it takes the table's own page when
// that page is already the whole unfiltered set (the usual case, no extra
// request), and otherwise reads up to 1,000 permits of its own, once per load
// or refresh (composables/useLocksSide.js).
//
// NO PRIVATE TICKER, AND NO AUTO-REFRESH, for the KV page's reasons: every
// call is a read of the replicated store on a metered route, and a keyset page
// that re-fetched under the reader would move rows while they are being read.
// So every relative cell ("held for", "expires in") is rendered against ONE
// instant, the load, which is the instant the stamp in the head names.
//
// NO WRITES. The page takes no lock and releases none. Releasing somebody
// else's lock lets two holders work at once unless the first guards its
// commits, and that call belongs to an operator who has read the holder's
// name, not to a button. What the drawer gives instead is the machine: the
// guard a transaction carries, and the exact release call for the lease
// period on screen.
import { computed, onUnmounted, ref, watch } from 'vue'

import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import Icon from '@/components/Icon.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import LocksSide from '@/components/LocksSide.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import UiSelect from '@/components/UiSelect.vue'
import { kv as kvApi, describeApiError } from '@/api'
import { useApi } from '@/composables/useApi'
import { formatTimestamp, formatTimestampTime, formatTimestampUtc } from '@/composables/useFormat'
import { describeVerdict, gatedVerdict } from '@/composables/useGatedVerdict'
import { useKeysetPager } from '@/composables/useKeysetPager'
import { exact } from '@/composables/useKvSide'
import { kvRefusalText } from '@/composables/useKvView'
import {
  LOCKS_NAMESPACE,
  describeLocksEnd, formatAgo, formatExpiresIn, formatHeldFor, formatSpan,
  guardOf, lifetimeSeconds, locksListBody, permitsOf, releaseBody, releaseCommand,
} from '@/composables/useLocks'
import { SIDE_READ_LIMIT, describeHeld, lockPrefixes, lockTotals, wholeSet } from '@/composables/useLocksSide'
import { useRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { stamp } from '@/composables/useStamp'
import { parseBrokerInstant } from '@/composables/useTimers'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { routeSupport } from '@/stores/routeSupport'

const { epoch } = useIdentity()
const { notifyError } = useToast()

/** How long the filter waits for the typing to stop: each applied prefix is a
 *  metered read. Enter skips the wait. */
const PREFIX_DEBOUNCE_MS = 300

/** Permits per page; the broker clamps any limit to 1..1000 on its own. */
const LIMITS = [25, 50, 100, 250]

const READ_ONLY_TITLE =
  'The console lists locks; it never takes, renews or releases one. A lock that is stuck can be ' +
  'released by hand with the call its row shows'
const LOCK_TITLE =
  'One row per permit: a lock has one, a semaphore one per holder'
const HELD_TITLE =
  'Since the holder took the lock. A renewal does not move it'
const RENEWED_TITLE =
  'When the holder last took or renewed the lock. A handle that renews by itself does so every third ' +
  'of the lifetime'
const EXPIRES_TITLE =
  'When the lock ends if its holder does not renew first. Nobody tells the holder: it may carry on, ' +
  'which is why the work is fenced with the token'
const LIFETIME_TITLE =
  'What the holder asked for at its last acquire or renewal: the distance from that write to its expiry'
const TOKEN_TITLE =
  'The fencing token of this lease period: the version of the lock\'s row. It changes at every renewal, ' +
  'and a later holder always has a higher one'
const SLOT_TITLE =
  'A semaphore has one slot per permit. Each holder has one, and its renew and release name it'
const OWNER_TITLE =
  'The identity the holder gave. The SDKs mint one per handle: host, process id and a random part'
const NO_OWNER_TITLE =
  'Taken without an owner. It is held all the same; a retry of its acquire cannot be told from a stranger'
const FOREIGN_TITLE =
  'In the lock namespace, and not a permit the broker wrote: the key has no slot, or the value no owner. ' +
  'It holds its key all the same'
const ROW_TITLE =
  'The lock is this row and nothing else: its value is the owner, its TTL the lifetime, its version the token'
const GUARD_TITLE =
  'A transaction that carries this in `kv` commits only while this holder still has the lock'
const RELEASE_TITLE =
  'It carries the token on this page, so a holder that has renewed since keeps its lock and the call ' +
  'answers `lost`. The holder is not told: it finds out at its next renewal or guarded commit. Send it ' +
  'with a credential that may write'
const HELD_FACT_TITLE =
  'Permits held: one per lock, one per holder of a semaphore'

// ---------------------------------------------------------------------------
// The query: a name prefix (in the address, ?prefix=) and a page size. Each
// starts a NEW sequence. Lock names are names the applications declare, not
// stored data, so the prefix can sit in the address, as a KV namespace does.
// ---------------------------------------------------------------------------
const prefixDraft = ref('')
const appliedPrefix = ref('')
const limit = ref(25)
useRouteState({ prefix: appliedPrefix, limit })

const queryKey = computed(() => JSON.stringify([appliedPrefix.value, limit.value]))
/** What the rows on screen answer; compared, so a filter that changed does not
 *  leave the previous filter's rows under the new one. */
const loadedKey = ref(null)
const loadedPage = ref(1)
const verdict = ref(null)

const pager = useKeysetPager()
const { canPrev, canNext } = pager

const resetQuery = () => {
  pager.reset()
  loadedPage.value = 1
}

// The KV listing's family in stores/routeSupport.js: a cell that answered 404
// for it is not asked again by this page either.
const listPermits = routeSupport.guard('kv', (body, config) =>
  kvApi.list(body, { ...config, probe: true }))
const listPanel = useApi(listPermits, { immediate: false })
const sidePanel = useApi(listPermits, { immediate: false })

const permits = computed(() => {
  if (loadedKey.value !== queryKey.value) return []
  return permitsOf(listPanel.data.value?.rows)
})
const loading = computed(() => listPanel.loading.value)
const listError = computed(() => listPanel.error.value)
const listErrorText = computed(() =>
  (listError.value ? kvRefusalText(listError.value) || describeApiError(listError.value) : ''))
const firstLoad = computed(() => loading.value && loadedKey.value !== queryKey.value)
const isStale = computed(() => Boolean(listError.value) && permits.value.length > 0)
const asOf = computed(() => listPanel.lastUpdated.value?.getTime() ?? Date.now())
const stampTime = computed(() => (listPanel.lastUpdated.value ? `at ${formatTimestampTime(listPanel.lastUpdated.value)}` : 'earlier'))
const pageEnd = computed(() => describeLocksEnd({
  rowCount: permits.value.length,
  truncated: listPanel.data.value?.truncated === true,
  prefixed: appliedPrefix.value !== '',
}))

const quiet = computed(() =>
  verdict.value && verdict.value !== 'transient' ? describeVerdict(verdict.value, 'kv') : null
)

// ---------------------------------------------------------------------------
// The list: every held permit, `{permits, more}`, or null until read. Taken
// from the table's page when that page is the whole set, else read on its
// own; never rebuilt by a filter or a page turn, only by a load or refresh.
// ---------------------------------------------------------------------------
const side = ref(null)
const sideEntries = computed(() => (side.value ? lockPrefixes(side.value.permits) : null))
const sideErrorText = computed(() => {
  const err = sidePanel.error.value || (side.value ? null : listPanel.error.value)
  return err ? kvRefusalText(err) || describeApiError(err) : ''
})

const loadSide = async () => {
  const at = epoch.value
  try {
    const data = await sidePanel.execute(locksListBody({ limit: SIDE_READ_LIMIT }))
    // Another cluster's permits are not this one's list.
    if (epoch.value !== at) return
    side.value = { permits: permitsOf(data?.rows), more: data?.truncated === true }
  } catch (err) {
    if (epoch.value !== at) return
    const v = gatedVerdict(err)
    if (v !== 'transient') verdict.value = v
  }
}

// The head's facts: the list's counts for what the table shows, so the two
// never disagree; a prefix typed by hand that no row of the list names has
// the page's own count, when the page is all of it.
const facts = computed(() => {
  if (quiet.value) return []
  const entries = sideEntries.value
  let entry = null
  if (entries && !appliedPrefix.value) entry = lockTotals(entries)
  else if (entries) entry = entries.find((e) => e.prefix === appliedPrefix.value) || null
  if (entry) {
    const more = side.value.more && !appliedPrefix.value ? '+' : ''
    const out = [{ k: 'Held', v: `${exact(entry.permits)}${more}`, title: `${HELD_FACT_TITLE}. ${describeHeld(entry) || 'None held'}` }]
    if (entry.semaphores) out.push({ k: 'Semaphores', v: exact(entry.semaphores), title: describeHeld(entry) })
    return out
  }
  if (loadedKey.value === queryKey.value && !listError.value && loadedPage.value === 1 && listPanel.data.value?.truncated !== true) {
    return [{ k: 'Held', v: exact(permits.value.length), title: HELD_FACT_TITLE }]
  }
  return []
})

const emptyLine = computed(() => {
  if (loadedKey.value !== queryKey.value) return 'Reading locks…'
  if (appliedPrefix.value) return `No held lock starts with ${appliedPrefix.value}`
  if (loadedPage.value > 1) return 'No lock on this page'
  return 'No lock is held'
})
const emptyTitle = computed(() => (appliedPrefix.value
  ? 'It matches from the first character only.'
  : 'A lock is here while its holder has it, and leaves when it is released or its lifetime ends. Holders take one with POST /api/v1/locks.'))

// A cluster switch lands on another cell: the cursor, the verdict, the list
// and the open drawer all belong to the cluster we left.
let seenEpoch = epoch.value
const syncEpoch = () => {
  if (epoch.value === seenEpoch) return
  seenEpoch = epoch.value
  resetQuery()
  verdict.value = null
  loadedKey.value = null
  side.value = null
  closeDrawer()
}
watch(epoch, syncEpoch)

/** One page of the table. True when that page was the whole set and so
 *  became the list's too. */
const reload = async () => {
  syncEpoch()
  if (verdict.value && verdict.value !== 'transient') return false
  // The drawer shows a row from the page this call is about to replace.
  closeDrawer()
  const asked = queryKey.value
  const prefix = appliedPrefix.value
  const at = epoch.value
  const body = locksListBody({
    prefix,
    after: pager.current(),
    limit: limit.value,
  })
  try {
    const data = await listPanel.execute(body)
    // Landed after a cluster switch: the next load of the new one decides.
    if (epoch.value !== at) return false
    verdict.value = null
    loadedKey.value = asked
    loadedPage.value = pager.page.value
    pager.received(data)
    const whole = wholeSet({ prefix, page: loadedPage.value, more: data?.truncated === true, permits: permitsOf(data?.rows) })
    if (whole) side.value = { permits: whole, more: false }
    return Boolean(whole)
  } catch (err) {
    if (epoch.value !== at) return false
    verdict.value = gatedVerdict(err)
    return false
  }
}

/** A load or a refresh: the table, then the list if the table could not be it. */
const refreshAll = async () => {
  const whole = await reload()
  if (!whole && !quiet.value) await loadSide()
}

const goNext = async () => {
  if (!pager.next()) return
  await reload()
}
const goPrev = async () => {
  if (!pager.prev()) return
  await reload()
}

const probeAgain = async () => {
  routeSupport.forget('kv')
  verdict.value = null
  await refreshAll()
}

watch([appliedPrefix, limit], () => { resetQuery(); reload() })

// ---------------------------------------------------------------------------
// The "name starts with" field. Debounced, and NOT trimmed: it is a byte range
// over the names, not a search box. A row picked in the list sets the same
// filter through the address.
// ---------------------------------------------------------------------------
let prefixTimer = null
const prefixHint = computed(() => (prefixDraft.value !== appliedPrefix.value
  ? 'Applying…'
  : 'Locks whose name starts with this. A byte range over the names: it matches from the first character only.'))

watch(appliedPrefix, (value) => { prefixDraft.value = value }, { immediate: true })
watch(prefixDraft, (val) => {
  clearTimeout(prefixTimer)
  if (val === appliedPrefix.value) return
  prefixTimer = setTimeout(() => { appliedPrefix.value = val }, PREFIX_DEBOUNCE_MS)
})
const applyPrefixNow = () => {
  clearTimeout(prefixTimer)
  appliedPrefix.value = prefixDraft.value
}
onUnmounted(() => clearTimeout(prefixTimer))

// ---------------------------------------------------------------------------
// One permit, and the machine behind it.
// ---------------------------------------------------------------------------
const selected = ref(null)
const copied = ref(false)
let copyTimer = null
const canCopy = typeof navigator !== 'undefined' && Boolean(navigator.clipboard)

const selectedGuard = computed(() => (selected.value?.foreign ? null : guardOf(selected.value)))
const selectedRelease = computed(() => releaseBody(selected.value))
const selectedLifetime = computed(() => lifetimeSeconds(selected.value))

const openPermit = (permit) => {
  selected.value = permit
  copied.value = false
}

function closeDrawer() {
  selected.value = null
}

const copyRelease = async () => {
  const origin = typeof window !== 'undefined' ? window.location.origin : ''
  const line = releaseCommand(selected.value, origin)
  if (!line) return
  try {
    await navigator.clipboard.writeText(line)
    copied.value = true
    clearTimeout(copyTimer)
    copyTimer = setTimeout(() => { copied.value = false }, 2000)
  } catch {
    notifyError('Could not copy to the clipboard', 'Copy failed')
  }
}
onUnmounted(() => clearTimeout(copyTimer))

// Both halves of a stamp go through the one parser (see views/Timers.vue): the
// broker prints up to six fractional digits.
const localStamp = (value) => formatTimestamp(parseBrokerInstant(value))
const instantTitle = (value) => {
  const at = parseBrokerInstant(value)
  return at ? `${formatTimestamp(at)} · ${formatTimestampUtc(at)}` : ''
}

// Registered NON-auto: the header's Refresh button and a cluster switch drive
// this page.
useRefresh(refreshAll)

// First load. Last, so nothing above can be reached before it is defined.
refreshAll()
</script>

<style scoped>
/* The head's facts: small chips beside the title, as in the Queues section
   (the same 16px after the title). On a narrow screen they wrap under the
   title rather than being cut. */
.locks :deep(.page-id) { flex-wrap: wrap; gap: 6px 16px; }
.locks :deep(.page-sub) { overflow: visible; white-space: normal; }
.facts { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.fact {
  display: inline-flex; align-items: baseline; gap: 5px;
  padding: 2px 8px; border-radius: var(--r-chip);
  background: var(--ink-3); border: 1px solid var(--bd);
  font-size: 11.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap;
}
.fact-k { color: var(--text-low); }
.locks-quiet { font-size: 12px; color: var(--text-low); white-space: nowrap; font-variant-numeric: tabular-nums; }

.locks-prefix .input { width: 200px; }

/* The table scrolls inside its card on a narrow screen; one line per row. */
.locks-scroll { overflow-x: auto; }
.locks-table th, .locks-table td { white-space: nowrap; }
.locks-table td { height: 36px; }
.locks-row { cursor: pointer; }
.locks-row:focus-visible { outline: 2px solid var(--ring); outline-offset: -2px; }
.locks-name { display: inline-flex; align-items: baseline; gap: 8px; min-width: 0; }
.locks-lock { font-family: var(--font-mono); font-size: 12px; color: var(--text-hi); }
.locks-tag { font-size: 12px; color: var(--text-low); }
/* An SDK's owner is `host:pid:random`, and a pod's host name is long: one
   line, the whole of it on hover and in the drawer. */
.locks-owner {
  display: block; max-width: 340px;
  overflow: hidden; text-overflow: ellipsis;
  font-family: var(--font-mono); font-size: 12px; color: var(--text-mid);
}
.locks-none { color: var(--text-low); }
.locks-skeleton { height: 16px; }

.locks-pager { flex-wrap: wrap; gap: 8px 16px; }
.locks-pager .pager-count { font-size: 12px; font-variant-numeric: tabular-nums; }
.locks-pager .btn { gap: 4px; }

.locks-fields { display: flex; flex-direction: column; gap: 14px; }

.locks-machine { display: flex; flex-direction: column; gap: 8px; }
.locks-machine-head {
  display: flex; align-items: baseline; justify-content: space-between;
  gap: 8px; margin-top: 14px;
}
.locks-note { font-size: 12px; color: var(--text-mid); line-height: 1.5; }
.locks-copy { gap: 4px; padding: 1px 6px; font-size: 11px; }

/* The drawer's footer is `justify-content: flex-end`; the link belongs on the
   other side of it. */
.locks-foot-link { margin-right: auto; font-size: 12px; color: var(--text-low); }
.locks-foot-link:hover { color: var(--text-hi); }

@media (max-width: 640px) {
  .locks-prefix { flex: 1 1 100%; }
  .locks-prefix .input { width: 100%; }
}
</style>
