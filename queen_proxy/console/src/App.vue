<script setup>
import { ref, computed, onMounted, provide } from 'vue'
import { bootstrapSession, apiFetch, logout } from './api.js'
import Overview from './views/Overview.vue'
import Queues from './views/Queues.vue'
import Keys from './views/Keys.vue'
import Members from './views/Members.vue'

// `admin: true` tabs only call endpoints console.rs gates with require_admin,
// so they are hidden from non-admins rather than shown and left to 403.
const TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'queues', label: 'Queues' },
  { id: 'keys', label: 'Keys', admin: true },
  { id: 'members', label: 'Members', admin: true },
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

const visibleTabs = computed(() => TABS.filter((t) => !t.admin || overview.value?.role === 'admin'))

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
    <div class="topbar-right">
      <div v-if="overview" class="cluster-badge">
        <span class="slug mono">{{ overview.slug }}</span>
        <span class="status" :class="'status-' + overview.status">{{ statusLabel(overview.status) }}</span>
      </div>
      <button class="btn btn-ghost btn-sm" @click="logout()">Sign out</button>
    </div>
  </header>

  <nav class="tabs">
    <button
      v-for="t in visibleTabs"
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
      <Queues v-if="activated.includes('queues')" v-show="active === 'queues'" :overview="overview" />
      <Keys v-if="activated.includes('keys')" v-show="active === 'keys'" />
      <Members v-if="activated.includes('members')" v-show="active === 'members'" />
    </template>
  </main>
</template>
