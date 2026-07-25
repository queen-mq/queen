<script setup>
import { ref, onMounted, provide } from 'vue'
import { bootstrapSession, apiFetch } from './api.js'
import Overview from './views/Overview.vue'
import Queues from './views/Queues.vue'
import Keys from './views/Keys.vue'

const TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'queues', label: 'Queues' },
  { id: 'keys', label: 'Keys' },
]

const active = ref('overview')
// Tabs are mounted lazily on first activation (and stay mounted after that,
// toggled with v-show) so e.g. a non-admin viewer never fires the
// admin-only /api/console/keys call just by loading the page.
const activated = ref(['overview'])

function selectTab(id) {
  active.value = id
  if (!activated.value.includes(id)) activated.value.push(id)
}

const ready = ref(false)
const bootError = ref(null)
const overview = ref(null)

async function loadOverview() {
  overview.value = await apiFetch('/api/console/overview')
}

provide('refreshOverview', loadOverview)

onMounted(async () => {
  try {
    await bootstrapSession()
    await loadOverview()
    ready.value = true
  } catch (e) {
    bootError.value = e.message || String(e)
  }
})

function statusLabel(s) {
  switch (s) {
    case 'active':
      return 'Active'
    case 'push_blocked':
      return 'Push blocked'
    case 'suspended':
      return 'Suspended'
    case 'deleting':
      return 'Deleting'
    default:
      return s
  }
}
</script>

<template>
  <header class="topbar">
    <div class="brand">
      <span class="brand-mark">Queen</span>
      <span class="brand-sub">cluster console</span>
    </div>
    <div v-if="overview" class="cluster-badge">
      <span class="slug mono">{{ overview.slug }}</span>
      <span class="status" :class="'status-' + overview.status">{{ statusLabel(overview.status) }}</span>
    </div>
  </header>

  <nav class="tabs">
    <button
      v-for="t in TABS"
      :key="t.id"
      class="tab"
      :class="{ active: active === t.id }"
      @click="selectTab(t.id)"
    >
      {{ t.label }}
    </button>
  </nav>

  <main class="content">
    <div v-if="!ready && !bootError" class="hint">Loading…</div>
    <div v-else-if="bootError" class="banner banner-error">{{ bootError }}</div>
    <template v-else>
      <Overview v-show="active === 'overview'" :overview="overview" />
      <Queues v-if="activated.includes('queues')" v-show="active === 'queues'" />
      <Keys v-if="activated.includes('keys')" v-show="active === 'keys'" />
    </template>
  </main>
</template>
