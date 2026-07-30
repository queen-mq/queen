<template>
  <div class="topbar-wrap">
  <header class="topbar">
    <div class="crumbs">
      <span>Queen</span>
      <span class="sep">/</span>
      <span class="here">{{ pageTitle }}</span>
    </div>

    <div class="cmd-search" ref="searchContainer" @click="focusSearch">
      <svg style="width:14px; height:14px; flex-shrink:0;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.8"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
      <input
        ref="searchInput" v-model="searchQuery" type="text"
        placeholder="Jump to a queue, consumer group…"
        class="cmd-input"
        @focus="onSearchFocus" @input="onSearchInput" @blur="onSearchBlur"
        @keydown.enter="handleSearchEnter"
        @keydown.down.prevent="navigateResults(1)"
        @keydown.up.prevent="navigateResults(-1)"
        @keydown.escape="closeSearch"
      />
      <span class="kbd">⌘K</span>

      <div v-if="showResults && searchQuery.length > 0" class="search-dropdown">
        <div v-if="searchLoading" style="padding:12px; text-align:center; color:var(--text-mid); font-size:13px;">Searching…</div>
        <template v-else-if="searchResults.length > 0">
          <div v-for="(r, i) in searchResults" :key="r.id" @mousedown.prevent="selectResult(r)" class="search-item" :class="{ active: i === selectedIndex }">
            <span class="search-type">{{ r.type === 'queue' ? 'Q' : 'CG' }}</span>
            <div style="flex:1; min-width:0;">
              <div style="font-size:13px; font-weight:500; color:var(--text-hi); overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{{ r.name }}</div>
              <div style="font-size:11px; color:var(--text-low);">{{ r.type === 'queue' ? `${r.partitions} partitions` : `${r.queueName} · ${r.members} members` }}</div>
            </div>
          </div>
        </template>
        <div v-else style="padding:12px; text-align:center; color:var(--text-mid); font-size:13px;">No results</div>
      </div>
    </div>

    <button class="top-btn" @click="handleRefresh" :disabled="isRefreshing" title="Refresh">
      <svg style="width:15px; height:15px;" :class="{ 'animate-spin': isRefreshing }" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99"/></svg>
    </button>

    <!-- CELL-level control: only a live operator sees it, and it is labelled so
         nobody reads it as affecting their tenant alone. -->
    <div v-if="isOperator" class="maint-group" title="Cell-level: affects every tenant on this cell">
      <span class="maint-scope">CELL</span>
      <button @click="togglePushMaintenance" :disabled="pushLoading" class="maint-btn" :class="{ on: pushMaintenanceMode }">
        <svg style="width:14px; height:14px;" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M11.42 15.17L17.25 21A2.652 2.652 0 0021 17.25l-5.877-5.877M11.42 15.17l2.496-3.03c.317-.384.74-.626 1.208-.766M11.42 15.17l-4.655 5.653a2.548 2.548 0 11-3.586-3.586l6.837-5.63m5.108-.233c.55-.164 1.163-.188 1.743-.14a4.5 4.5 0 004.486-6.336l-3.276 3.277a3.004 3.004 0 01-2.25-2.25l3.276-3.276a4.5 4.5 0 00-6.336 4.486c.091 1.076-.071 2.264-.904 2.95l-.102.085m-1.745 1.437L5.909 7.5H4.5L2.25 3.75l1.5-1.5L7.5 4.5v1.409l4.26 4.26m-1.745 1.437l1.745-1.437m6.615 8.206L15.75 15.75M4.867 19.125h.008v.008h-.008v-.008z"/></svg>
        Push
        <span v-if="pushMaintenanceMode" class="pulse-amber" style="width:5px; height:5px;" />
      </button>
      <button @click="togglePopMaintenance" :disabled="popLoading" class="maint-btn" :class="{ on: popMaintenanceMode }">
        <svg style="width:14px; height:14px;" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M15.75 5.25v13.5m-7.5-13.5v13.5"/></svg>
        Pop
        <span v-if="popMaintenanceMode" class="pulse-ember" style="width:5px; height:5px;" />
      </button>
    </div>

  </header>

  <!-- Status banners -->
  <div v-if="showBanners" class="status-strip">
    <div v-if="pushMaintenanceMode" class="status-banner banner-warn">
      <span class="pulse-amber" style="width:7px; height:7px; flex-shrink:0;" />
      <span>
        <strong>Push maintenance active</strong> ·
        <span class="font-mono tabular-nums">{{ formatNumber(bufferedMessages) }}</span>
        message{{ bufferedMessages === 1 ? '' : 's' }} buffered on disk
      </span>
    </div>
    <div v-if="popMaintenanceMode" class="status-banner banner-warn">
      <span class="pulse-ember" style="width:7px; height:7px; flex-shrink:0;" />
      <span><strong>Pop maintenance active</strong> · consumers are receiving no messages</span>
    </div>
    <div v-if="failedCount > 0" class="status-banner banner-bad">
      <svg style="width:14px; height:14px; flex-shrink:0;" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
        <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z"/>
      </svg>
      <span>
        <strong>{{ formatNumber(failedCount) }}</strong>
        failed buffered message{{ failedCount === 1 ? '' : 's' }} on disk
        <span v-if="failedMB > 0" style="opacity:.7;">· {{ failedMB.toFixed(1) }} MB</span>
      </span>
    </div>
    <div v-if="bufferedMessages > 0 && !pushMaintenanceMode && !popMaintenanceMode && failedCount === 0" class="status-banner banner-info">
      <svg style="width:14px; height:14px; flex-shrink:0;" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
        <path stroke-linecap="round" stroke-linejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
      </svg>
      <span>
        <span class="font-mono tabular-nums">{{ formatNumber(bufferedMessages) }}</span>
        message{{ bufferedMessages === 1 ? '' : 's' }} still draining from disk buffer
      </span>
    </div>
  </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { queues as queuesApi, consumers as consumersApi, operator } from '@/api'
import { formatNumber } from '@/composables/useApi'
import { useIdentity } from '@/stores/identity'
import { useToast } from '@/composables/useToast'

const route = useRoute()
const router = useRouter()
const emit = defineEmits(['refresh'])

const pageTitle = computed(() => route.meta.title || 'Dashboard')

const searchQuery = ref('')
const showResults = ref(false)
const selectedIndex = ref(0)
const searchContainer = ref(null)
const searchInput = ref(null)
const searchLoading = ref(false)
const searchDataLoaded = ref(false)
const queues = ref([])
const consumers = ref([])

const focusSearch = () => { searchInput.value?.focus() }

const searchResults = computed(() => {
  if (!searchQuery.value) return []
  const q = searchQuery.value.toLowerCase()
  const results = []
  queues.value.filter(x => x.name?.toLowerCase().includes(q)).slice(0, 5).forEach(x => {
    results.push({ id: `q-${x.name}`, type: 'queue', name: x.name, partitions: x.partitions || 1, route: `/queues/${encodeURIComponent(x.name)}` })
  })
  consumers.value.filter(x => x.name?.toLowerCase().includes(q)).slice(0, 5).forEach(x => {
    results.push({ id: `c-${x.name}-${x.queueName}`, type: 'consumer', name: x.name, queueName: x.queueName, members: x.members || 0, route: `/consumers?search=${encodeURIComponent(x.name)}` })
  })
  return results.slice(0, 10)
})

const navigateResults = (dir) => {
  if (!searchResults.value.length) return
  selectedIndex.value = Math.max(0, Math.min(searchResults.value.length - 1, selectedIndex.value + dir))
}
const handleSearchEnter = () => { if (searchResults.value[selectedIndex.value]) selectResult(searchResults.value[selectedIndex.value]) }
const selectResult = (r) => { router.push(r.route); searchQuery.value = ''; showResults.value = false; selectedIndex.value = 0 }
const closeSearch = () => { showResults.value = false; searchQuery.value = '' }
const onSearchFocus = async () => { if (searchQuery.value) showResults.value = true; if (!searchDataLoaded.value) await loadSearchData() }
const onSearchInput = () => { showResults.value = true; if (!searchDataLoaded.value) loadSearchData() }
const onSearchBlur = () => { setTimeout(() => { if (!searchQuery.value) showResults.value = false }, 150) }
watch(searchQuery, () => { selectedIndex.value = 0; if (searchQuery.value) showResults.value = true })

const loadSearchData = async () => {
  if (searchLoading.value) return
  searchLoading.value = true
  try {
    const [qr, cr] = await Promise.all([queuesApi.list(), consumersApi.list()])
    queues.value = Array.isArray(qr.data?.queues || qr.data) ? (qr.data?.queues || qr.data) : []
    consumers.value = Array.isArray(cr.data) ? cr.data : []
    searchDataLoaded.value = true
  } catch { searchDataLoaded.value = true }
  finally { searchLoading.value = false }
}

const handleKeydown = (e) => { if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); focusSearch() } }

const isRefreshing = ref(false)
const handleRefresh = async () => {
  isRefreshing.value = true
  emit('refresh')
  await loadSearchData()
  await loadMaintenanceStatus()
  setTimeout(() => { isRefreshing.value = false }, 500)
}

// Maintenance is a CELL-level control on an operator-only route
// (/api/v1/system/maintenance). Everything below is gated on `can('operator')`:
// a non-operator neither polls it (it would 404 every 30s) nor sees the toggles.
const { can } = useIdentity()
const { notifyError, notifySuccess } = useToast()
const isOperator = computed(() => can('operator'))

const pushMaintenanceMode = ref(false)
const popMaintenanceMode = ref(false)
const bufferedMessages = ref(0)
const failedCount = ref(0)
const failedMB = ref(0)
const pushLoading = ref(false)
const popLoading = ref(false)
// `null` until a successful read: the banners below must never claim "no
// maintenance, 0 buffered" off a FAILED fetch. The refs used to default to
// false/0 with `|| false` coercions on top, so an unreachable (or, for a
// non-operator, 404-ing) endpoint rendered as a healthy idle cell.
const maintenanceKnown = ref(false)
let maintenanceInterval = null

const showBanners = computed(() =>
  maintenanceKnown.value && (
    pushMaintenanceMode.value ||
    popMaintenanceMode.value ||
    failedCount.value > 0 ||
    bufferedMessages.value > 0
  )
)

const loadMaintenanceStatus = async () => {
  if (!isOperator.value) return
  try {
    const r = await operator.getMaintenance()
    pushMaintenanceMode.value = r.data.maintenanceMode === true
    popMaintenanceMode.value = r.data.popMaintenanceMode === true
    bufferedMessages.value = r.data.bufferedMessages ?? 0
    failedCount.value = r.data.bufferStats?.failedCount ?? 0
    failedMB.value = r.data.bufferStats?.failedFiles?.totalMB ?? 0
    maintenanceKnown.value = true
  } catch {
    // The interceptor already surfaced the failure; here we only make sure the
    // banners stop asserting a state we no longer know.
    maintenanceKnown.value = false
  }
}

const togglePushMaintenance = async () => {
  if (pushLoading.value || !isOperator.value) return
  const enable = !pushMaintenanceMode.value
  // This is a CELL-wide switch: it stops pushes for every tenant on this cell,
  // so it stays behind an explicit confirmation that says so.
  if (enable && !confirm('Enable PUSH maintenance on this CELL?\n\nPushes for EVERY tenant on this cell will be routed to the file buffer.')) return
  if (!enable && bufferedMessages.value > 0 && !confirm(`Disable PUSH maintenance?\n\n${bufferedMessages.value} buffered messages will drain.`)) return
  pushLoading.value = true
  try {
    const r = await operator.setMaintenance(enable)
    pushMaintenanceMode.value = r.data.maintenanceMode === true
    bufferedMessages.value = r.data.bufferedMessages ?? 0
    maintenanceKnown.value = true
    notifySuccess(enable ? 'PUSH maintenance enabled on this cell' : 'PUSH maintenance disabled')
  } catch (e) {
    notifyError(e, 'Could not change PUSH maintenance')
    // The cell's real state is now unknown — re-read rather than keep showing
    // the value the click optimistically implied.
    await loadMaintenanceStatus()
  } finally { pushLoading.value = false }
}

// The other half of the same switch. The GET above already reports
// `popMaintenanceMode`, so this state was VISIBLE in the banner long before it
// was operable — the proxy classified the push endpoint as operator-reachable
// and its pop sibling as blocked, which left an operator able to watch pop
// maintenance and unable to leave it. Both are on the operator list now
// (queen_proxy/src/routes.rs is_operator_route).
//
// Pop maintenance is the harsher of the two: pushes under push maintenance are
// spooled to disk and drain later, but a paused pop returns nothing to a
// consumer that is asking, on every tenant on this cell. Hence the blunter
// confirmation.
const togglePopMaintenance = async () => {
  if (popLoading.value || !isOperator.value) return
  const enable = !popMaintenanceMode.value
  if (enable && !confirm('Enable POP maintenance on this CELL?\n\nConsumers for EVERY tenant on this cell will stop receiving messages. Nothing is lost — nothing is delivered until this is turned off.')) return
  popLoading.value = true
  try {
    const r = await operator.setPopMaintenance(enable)
    popMaintenanceMode.value = r.data.popMaintenanceMode === true
    maintenanceKnown.value = true
    notifySuccess(enable ? 'POP maintenance enabled on this cell' : 'POP maintenance disabled')
  } catch (e) {
    notifyError(e, 'Could not change POP maintenance')
    await loadMaintenanceStatus()
  } finally { popLoading.value = false }
}

onMounted(() => {
  document.addEventListener('keydown', handleKeydown)
  loadSearchData()
  loadMaintenanceStatus()
  maintenanceInterval = setInterval(loadMaintenanceStatus, 30000)
})
onUnmounted(() => {
  document.removeEventListener('keydown', handleKeydown)
  if (maintenanceInterval) clearInterval(maintenanceInterval)
})
</script>
