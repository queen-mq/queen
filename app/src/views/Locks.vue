<template>
  <div class="view-container">

    <PageHead title="Locks">
      <template #sub>
        <template v-if="loadedKey === queryKey && !listError">
          {{ formatNumber(permits.length) }} held<template v-if="appliedPrefix"> under this prefix</template><template v-if="canNext || canPrev"> on this page</template>
        </template>
        <template v-if="stamp(listPanel)"><template v-if="loadedKey === queryKey && !listError"> · </template>{{ stamp(listPanel) }}</template>
      </template>
      <!-- Said on the page: a console that shows who holds a lock is expected
           to be able to take it away, and a missing button is not an answer. -->
      <template #actions>
        <span class="tool-note" :title="READ_ONLY_TITLE">Read-only — a lock is taken and released by its holder</span>
      </template>
    </PageHead>

    <!-- ===================== The three quiet states =====================
         Not here / not in the plan / not being served right now. The page reads
         the console's KV listing, so they are that family's answers:
         composables/useGatedVerdict.js owns the rule and the wording. -->
    <div v-if="quiet" class="card">
      <div class="empty-state">
        <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
          <rect x="5" y="11" width="14" height="9" rx="2" />
          <path stroke-linecap="round" d="M8 11V8a4 4 0 018 0v3" />
        </svg>
        <h3>{{ quiet.title }}</h3>
        <p>{{ quiet.detail }}</p>
        <p style="font-size:13px;">
          A lock is a row of the namespace <code>{{ LOCKS_NAMESPACE }}</code>, and this page
          reads it through the KV listing.
        </p>
        <button class="btn btn-ghost" @click="probeAgain">Check again</button>
      </div>
    </div>

    <template v-else>
      <!-- Stale rows presented as live is the failure this banner prevents: a
           lock list is only true at the instant it was read. -->
      <div v-if="isStale" class="status-banner banner-bad view-banner">
        <span>
          <strong>Could not refresh the locks</strong> ·
          {{ listErrorText }} · showing the last page that loaded
          ({{ stamp(listPanel) }}).
        </span>
      </div>

      <PageTools>
        <label class="tool-field" for="locks-prefix" :title="prefixHint">
          <span class="tool-label">Name starts with</span>
          <input
            id="locks-prefix"
            v-model="prefixDraft"
            class="input"
            placeholder="e.g. sync:"
            spellcheck="false"
            @keyup.enter="applyPrefixNow"
          />
        </label>
        <template #view>
          <label class="tool-field" for="locks-limit">
            <span class="tool-label">Show</span>
            <select id="locks-limit" v-model.number="limit" class="input">
              <option :value="25">25</option>
              <option :value="50">50</option>
              <option :value="100">100</option>
              <option :value="250">250</option>
            </select>
          </label>
        </template>
      </PageTools>

      <div class="card">
        <div class="card-header">
          <h3>Held now</h3>
          <span class="card-sub">
            one row per permit — a lock has one, a semaphore one per holder
          </span>
          <span class="muted">{{ stamp(listPanel) }}</span>
        </div>

        <div style="overflow-x:auto;">
          <table class="t">
            <thead>
              <tr>
                <th>Lock</th>
                <th>Holder</th>
                <th :title="HELD_TITLE">Held for</th>
                <th :title="RENEWED_TITLE">Renewed</th>
                <th :title="EXPIRES_TITLE">Expires</th>
              </tr>
            </thead>
            <tbody>
              <template v-if="firstLoad">
                <tr v-for="i in 4" :key="`sk-${i}`">
                  <td colspan="5"><div class="skeleton" style="height:16px;" /></td>
                </tr>
              </template>

              <template v-else-if="permits.length">
                <tr
                  v-for="p in permits"
                  :key="p.key"
                  class="locks-row"
                  @click="openPermit(p)"
                >
                  <td>
                    <div class="locks-name">
                      <span class="font-mono" style="font-size:12px;">{{ p.name }}</span>
                      <span v-if="p.shared" class="chip chip-mute" :title="SLOT_TITLE">slot {{ p.slot }}</span>
                      <span v-if="p.foreign" class="chip chip-mute" :title="FOREIGN_TITLE">not a lock</span>
                    </div>
                  </td>
                  <td>
                    <span v-if="p.owner" class="font-mono locks-owner" :title="p.owner">{{ p.owner }}</span>
                    <span v-else class="mute" :title="NO_OWNER_TITLE">—</span>
                  </td>
                  <td :title="utcTitle(p.since)">
                    <div class="locks-main tabular-nums">{{ formatHeldFor(p.since, asOf) }}</div>
                    <div class="locks-sub tabular-nums">since {{ localStamp(p.since) }}</div>
                  </td>
                  <td :title="utcTitle(p.renewedAt)">
                    <span class="locks-sub tabular-nums">{{ formatAgo(p.renewedAt, asOf) }}</span>
                  </td>
                  <td :title="utcTitle(p.expiresAt)">
                    <div class="locks-main tabular-nums">{{ formatExpiresIn(p.expiresAt, asOf) }}</div>
                    <div class="locks-sub tabular-nums">{{ localStamp(p.expiresAt) }}</div>
                  </td>
                </tr>
              </template>

              <tr v-else-if="listError">
                <td colspan="5">
                  <div class="empty-state empty-state-failed">
                    <h3>{{ listErrorText }}</h3>
                    <p>Nothing loaded — this is a failure, not an empty list.</p>
                    <button class="btn btn-ghost" @click="reload">Retry</button>
                  </div>
                </td>
              </tr>

              <tr v-else>
                <td colspan="5">
                  <div class="empty-state">
                    <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                      <rect x="5" y="11" width="14" height="9" rx="2" />
                      <path stroke-linecap="round" d="M8 11V8a4 4 0 017.5-2" />
                    </svg>
                    <template v-if="appliedPrefix">
                      <h3>No held lock starts with this</h3>
                      <p>
                        Nothing held right now has a name that starts with
                        <strong class="font-mono">{{ appliedPrefix }}</strong>. It matches from
                        the first character only.
                      </p>
                      <button class="btn btn-ghost" @click="clearPrefix">Clear the filter</button>
                    </template>
                    <template v-else>
                      <h3>No lock is held{{ loadedPage > 1 ? ' on this page' : '' }}</h3>
                      <p>
                        Nothing on this cluster holds a lock or a semaphore permit right now.
                        A lock is here while its holder has it, and leaves when it is released
                        or its lifetime ends. Holders take one with
                        <code>POST /api/v1/locks</code>.
                      </p>
                    </template>
                    <button v-if="canPrev" class="btn btn-ghost" @click="goPrev">Previous page</button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Keyset pager: no page count, because there is no total without a
             count and this is an index range, not an offset. -->
        <div v-if="permits.length || canPrev" class="pager">
          <span class="pager-count">
            Page <span class="tabular-nums">{{ loadedPage }}</span>
            · {{ permits.length }} row{{ permits.length === 1 ? '' : 's' }}
            <template v-if="pageEnd"> · {{ pageEnd }}</template>
          </span>
          <div class="pager-nav">
            <button class="btn btn-ghost" :disabled="!canPrev || loading" @click="goPrev">Previous</button>
            <button class="btn btn-ghost" :disabled="!canNext || loading" @click="goNext">Next</button>
          </div>
        </div>
      </div>
    </template>

    <!-- ===================== One permit =====================
         No second request: the row came with the page, so the drawer is a view
         of the page's snapshot and closes whenever a new page is asked for. -->
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
            <p class="locks-note">
              A transaction that carries this in <code>kv</code> commits only while this
              holder still has the lock. It is what <code>.guard(lock)</code> sends.
            </p>
            <JsonViewer :value="selectedGuard" />
          </template>

          <template v-if="selectedRelease">
            <div class="label-xs locks-machine-head">
              <span>Release by hand</span>
              <button v-if="canCopy" class="btn btn-ghost locks-copy" @click="copyRelease">
                {{ copied ? 'Copied!' : 'Copy as curl' }}
              </button>
            </div>
            <p class="locks-note">
              <code>POST /api/v1/locks</code> with the body below releases this lock. It
              carries the token on this page, so a holder that has renewed since keeps its
              lock and the call answers <code>lost</code>. The holder is not told: it finds
              out at its next renewal or guarded commit. Send it with a credential that may
              write.
            </p>
            <JsonViewer :value="selectedRelease" />
          </template>

          <div v-if="selected.foreign" class="empty-tile">
            This row is in the lock namespace and is not a permit the broker wrote: its
            key has no slot, or its value names no owner. It holds its key all the same,
            and only the KV routes can remove it.
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
// NO PRIVATE TICKER, AND NO AUTO-REFRESH, for the KV page's reasons: every
// call is a read of the replicated store on a metered route, and a keyset page
// that re-fetched under the reader would move rows while they are being read.
// So every relative cell ("held for", "expires in") is rendered against ONE
// instant, the load, which is the instant the stamp beside the title names.
//
// NO WRITES. The page takes no lock and releases none. Releasing somebody
// else's lock lets two holders work at once unless the first guards its
// commits, and that call belongs to an operator who has read the holder's
// name, not to a button. What the drawer gives instead is the machine: the
// guard a transaction carries, and the exact release call for the lease
// period on screen.
import { computed, onUnmounted, ref, watch } from 'vue'

import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'
import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import { kv as kvApi, describeApiError } from '@/api'
import { formatNumber, useApi } from '@/composables/useApi'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { describeVerdict, gatedVerdict } from '@/composables/useGatedVerdict'
import { useKeysetPager } from '@/composables/useKeysetPager'
import { kvRefusalText } from '@/composables/useKvView'
import {
  LOCKS_NAMESPACE,
  describeLocksEnd, formatAgo, formatExpiresIn, formatHeldFor, formatSpan,
  guardOf, lifetimeSeconds, locksListBody, permitsOf, releaseBody, releaseCommand,
} from '@/composables/useLocks'
import { useRefresh } from '@/composables/useRefresh'
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

const READ_ONLY_TITLE =
  'The console lists locks; it never takes, renews or releases one. A lock that is stuck can be ' +
  'released by hand with the call its row shows'
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
  'In the lock namespace, and not a permit the broker wrote: the key has no slot, or the value no owner'
const ROW_TITLE =
  'The lock is this row and nothing else: its value is the owner, its TTL the lifetime, its version the token'

// ---------------------------------------------------------------------------
// The query: a name prefix and a page size. Each starts a NEW sequence.
// ---------------------------------------------------------------------------
const prefixDraft = ref('')
const appliedPrefix = ref('')
const limit = ref(25)

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
const pageEnd = computed(() => describeLocksEnd({
  rowCount: permits.value.length,
  truncated: listPanel.data.value?.truncated === true,
  prefixed: appliedPrefix.value !== '',
}))

const quiet = computed(() =>
  verdict.value && verdict.value !== 'transient' ? describeVerdict(verdict.value, 'kv') : null
)

// A cluster switch lands on another cell: the cursor, the verdict and the open
// drawer all belong to the cluster we left.
let seenEpoch = epoch.value
const syncEpoch = () => {
  if (epoch.value === seenEpoch) return
  seenEpoch = epoch.value
  resetQuery()
  verdict.value = null
  loadedKey.value = null
  closeDrawer()
}
watch(epoch, syncEpoch)

const reload = async () => {
  syncEpoch()
  if (verdict.value && verdict.value !== 'transient') return
  // The drawer shows a row from the page this call is about to replace.
  closeDrawer()
  const asked = queryKey.value
  const body = locksListBody({
    prefix: appliedPrefix.value,
    after: pager.current(),
    limit: limit.value,
  })
  try {
    const data = await listPanel.execute(body)
    verdict.value = null
    loadedKey.value = asked
    loadedPage.value = pager.page.value
    pager.received(data)
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

const probeAgain = async () => {
  routeSupport.forget('kv')
  verdict.value = null
  await reload()
}

watch(appliedPrefix, () => { resetQuery(); reload() })
watch(limit, () => { resetQuery(); reload() })

// ---------------------------------------------------------------------------
// The filter. Debounced, and NOT trimmed: it is a byte range over the names,
// not a search box.
// ---------------------------------------------------------------------------
let prefixTimer = null
const prefixHint = computed(() => (prefixDraft.value !== appliedPrefix.value
  ? 'Applying…'
  : 'A byte range over the lock names: it matches from the first character only.'))

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
const utcTitle = (value) => formatTimestampUtc(parseBrokerInstant(value))

// Registered NON-auto: the header's Refresh button and a cluster switch drive
// this page.
useRefresh(reload)

// First load. Last, so nothing above can be reached before it is defined.
reload()
</script>

<style scoped>
.locks-row { cursor: pointer; }
.locks-name { display: flex; align-items: center; gap: 8px; min-width: 0; }

/* An SDK's owner is `host:pid:random`, and a pod's host name is long: one
   line, the whole of it on hover and in the drawer. */
.locks-owner {
  display: block; max-width: 340px;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  font-size: 12px; color: var(--text-mid);
}

.locks-main { font-size: 12px; color: var(--text-hi); font-weight: 500; white-space: nowrap; }
.locks-sub { font-size: 11px; color: var(--text-low); white-space: nowrap; }

.locks-fields { display: flex; flex-direction: column; gap: 14px; }

.locks-machine { display: flex; flex-direction: column; gap: 8px; }
.locks-machine-head {
  display: flex; align-items: baseline; justify-content: space-between;
  gap: 8px; margin-top: 14px;
}
.locks-note { font-size: 12.5px; color: var(--text-mid); line-height: 1.5; }
.locks-copy { padding: 1px 6px; font-size: 10.5px; }

/* The drawer's footer is `justify-content: flex-end`; the link belongs on the
   other side of it. */
.locks-foot-link { margin-right: auto; font-size: 11.5px; color: var(--text-low); }
.locks-foot-link:hover { color: var(--text-hi); }
</style>
