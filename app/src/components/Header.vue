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

    <!-- Colour scheme. Icon-only, and it shows the scheme you would GET, not
         the one you are in: a sun on the dark surface reads as "go light".
         The label is on the title/aria so the control costs no topbar width,
         which is the scarcest thing up here. -->
    <button
      class="top-btn"
      @click="toggleTheme()"
      :title="isDark ? 'Switch to light' : 'Switch to dark'"
      :aria-label="isDark ? 'Switch to light theme' : 'Switch to dark theme'"
      aria-live="polite"
    >
      <svg v-if="isDark" style="width:15px; height:15px;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <circle cx="12" cy="12" r="4.2"/>
        <path stroke-linecap="round" d="M12 2.6v2.2M12 19.2v2.2M4.22 4.22l1.56 1.56M18.22 18.22l1.56 1.56M2.6 12h2.2M19.2 12h2.2M4.22 19.78l1.56-1.56M18.22 5.78l1.56-1.56"/>
      </svg>
      <svg v-else style="width:15px; height:15px;" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round" d="M20.4 13.9A8.6 8.6 0 1 1 10.1 3.6a6.9 6.9 0 0 0 10.3 10.3z"/>
      </svg>
    </button>

    <button class="top-btn" @click="handleRefresh" :disabled="isRefreshing" title="Refresh">
      <svg style="width:15px; height:15px;" :class="{ 'animate-spin': isRefreshing }" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99"/></svg>
    </button>
  </header>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { queues as queuesApi, consumers as consumersApi } from '@/api'
import { isDark, toggleTheme } from '@/composables/useTheme'

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
