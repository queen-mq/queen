<template>
  <div class="topbar-wrap">
  <header class="topbar">
    <!-- Three parts: the page on the left, the broker in the middle, search
         and the buttons on the right. The two sides share the rest equally,
         so the middle is the bar's centre whatever the page is called. -->
    <div class="topbar-side">
    <!-- The sidebar's width: the full column or its rail of icons. A wide
         screen only; below that the sidebar is a drawer with its own button
         in this same corner. -->
    <button
      class="top-btn nav-toggle"
      @click="toggleSidebar()"
      :title="collapsed ? 'Expand sidebar (⌘\\)' : 'Collapse sidebar (⌘\\)'"
      :aria-label="collapsed ? 'Expand sidebar' : 'Collapse sidebar'"
      :aria-expanded="!collapsed"
    >
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <rect x="3" y="4.5" width="18" height="15" rx="2" />
        <path d="M9 4.5v15" />
      </svg>
    </button>

    <div class="crumbs">
      <template v-if="parentCrumb">
        <router-link class="crumb-link" :to="parentCrumb.to">{{ parentCrumb.label }}</router-link>
        <span class="sep">/</span>
      </template>
      <span class="here">{{ pageTitle }}</span>
    </div>
    </div>

    <BrokerBar />

    <div class="topbar-side topbar-end">
    <div class="cmd-search" :class="{ open: searchOpen }" ref="searchContainer" @click="focusSearch">
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

    <ThemeMenu />

    <button class="top-btn" @click="handleRefresh" :disabled="isRefreshing" title="Refresh">
      <svg style="width:15px; height:15px;" :class="{ 'animate-spin': isRefreshing }" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99"/></svg>
    </button>
    </div>
  </header>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { queues as queuesApi, consumers as consumersApi } from '@/api'
import { collapsed, toggleSidebar, wide } from '@/composables/useSidebar'
import { queueOf, queueLocation, consumerLocation } from '@/composables/navigation'
import { useIdentity } from '@/stores/identity'
import BrokerBar from '@/components/BrokerBar.vue'
import ThemeMenu from '@/components/ThemeMenu.vue'

const route = useRoute()
const router = useRouter()
const emit = defineEmits(['refresh'])

// A detail page names its entity; its parent list is one click back.
const pageTitle = computed(() => {
  if (route.name === 'QueueDetail' && route.params.queueName) return String(route.params.queueName)
  return route.meta.title || 'Overview'
})
const parentCrumb = computed(() =>
  route.name === 'QueueDetail' ? { label: 'Queues', to: '/queues' }
    : queueOf(route) ? { label: queueOf(route), to: queueLocation(queueOf(route), route) } : null
)

const searchQuery = ref('')
const showResults = ref(false)
const selectedIndex = ref(0)
const searchContainer = ref(null)
const searchInput = ref(null)
const searchLoading = ref(false)
const searchDataLoaded = ref(false)
const queues = ref([])
const consumers = ref([])

// The one search box. On a phone it is a magnifier until it is used, so
// opening it has to show the field before it can take the focus.
const searchOpen = ref(false)
const focusSearch = () => {
  searchOpen.value = true
  nextTick(() => searchInput.value?.focus())
}

const searchResults = computed(() => {
  if (!searchQuery.value) return []
  const q = searchQuery.value.toLowerCase()
  const results = []
  queues.value.filter(x => x.name?.toLowerCase().includes(q)).slice(0, 5).forEach(x => {
    results.push({ id: `q-${x.name}`, type: 'queue', name: x.name, partitions: x.partitions || 1, route: queueLocation(x.name, route) })
  })
  consumers.value.filter(x => x.name?.toLowerCase().includes(q)).slice(0, 5).forEach(x => {
    results.push({ id: `c-${x.name}-${x.queueName}`, type: 'consumer', name: x.name, queueName: x.queueName, members: x.members || 0, route: consumerLocation(x, route) })
  })
  return results.slice(0, 10)
})

const navigateResults = (dir) => {
  if (!searchResults.value.length) return
  selectedIndex.value = Math.max(0, Math.min(searchResults.value.length - 1, selectedIndex.value + dir))
}
const handleSearchEnter = () => { if (searchResults.value[selectedIndex.value]) selectResult(searchResults.value[selectedIndex.value]) }
const selectResult = (r) => { router.push(r.route); searchQuery.value = ''; showResults.value = false; selectedIndex.value = 0; searchOpen.value = false; searchInput.value?.blur() }
const closeSearch = () => { showResults.value = false; searchQuery.value = ''; searchOpen.value = false; searchInput.value?.blur() }
const onSearchFocus = async () => { if (searchQuery.value) showResults.value = true; if (!searchDataLoaded.value) await loadSearchData() }
const onSearchInput = () => { showResults.value = true; if (!searchDataLoaded.value) loadSearchData() }
const onSearchBlur = () => { setTimeout(() => { if (!searchQuery.value) { showResults.value = false; searchOpen.value = false } }, 150) }
watch(searchQuery, () => { selectedIndex.value = 0; if (searchQuery.value) showResults.value = true })

const { epoch } = useIdentity()
let searchSequence = 0
const loadSearchData = async () => {
  if (searchLoading.value) return
  searchLoading.value = true
  const sequence = ++searchSequence
  const clusterEpoch = epoch.value
  try {
    const [qr, cr] = await Promise.all([queuesApi.list(), consumersApi.list()])
    if (sequence !== searchSequence || clusterEpoch !== epoch.value) return
    queues.value = Array.isArray(qr.data?.queues || qr.data) ? (qr.data?.queues || qr.data) : []
    consumers.value = Array.isArray(cr.data) ? cr.data : []
    searchDataLoaded.value = true
  } catch { if (sequence === searchSequence) searchDataLoaded.value = false }
  finally { if (sequence === searchSequence) searchLoading.value = false }
}

watch(epoch, () => {
  searchSequence++
  queues.value = []; consumers.value = []
  searchDataLoaded.value = false; searchLoading.value = false
  closeSearch()
  loadSearchData()
})

const handleKeydown = (e) => {
  if (!(e.metaKey || e.ctrlKey)) return
  if (e.key.toLowerCase() === 'k') { e.preventDefault(); focusSearch() }
  else if (e.key === '\\' && wide.value) { e.preventDefault(); toggleSidebar() }
}

const isRefreshing = ref(false)
const handleRefresh = async () => {
  isRefreshing.value = true
  emit('refresh')
  await loadSearchData()
  setTimeout(() => { isRefreshing.value = false }, 500)
}

onMounted(() => {
  document.addEventListener('keydown', handleKeydown)
  loadSearchData()
})
onUnmounted(() => {
  document.removeEventListener('keydown', handleKeydown)
})
</script>
