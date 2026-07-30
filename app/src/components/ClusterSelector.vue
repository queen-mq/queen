<template>
  <div class="cluster-select" ref="root">
    <!-- Collapsed rail: the acting cluster still has to be identifiable, or
         the user cannot tell whose numbers are on screen. -->
    <button
      v-if="collapsed"
      class="cluster-chip-mini"
      :title="titleText"
      @click="open = !open"
    >{{ miniLabel }}</button>

    <button v-else class="cluster-chip" :title="titleText" @click="open = !open">
      <div class="cluster-chip-text">
        <div class="cluster-line">
          <span class="cluster-tenant">{{ actingTenantSlug || '—' }}</span>
          <span class="cluster-sep">/</span>
          <span class="cluster-name">{{ actingClusterSlug || 'no cluster' }}</span>
        </div>
        <div class="cluster-sub">
          <!-- TODO(backend): /auth/me clusters[] carries no cell yet, so the
               cell is named only when it is known. Never guess it — a wrong
               cell label is the same lie as a wrong tenant. -->
          <span>cell {{ actingCellSlug || 'unknown' }}</span>
          <span class="cluster-sep">·</span>
          <span>{{ roleLabel }}</span>
        </div>
      </div>
      <svg class="cluster-caret" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
        <path stroke-linecap="round" stroke-linejoin="round" d="m6 9 6 6 6-6" />
      </svg>
    </button>

    <div v-if="open" class="cluster-menu">
      <div class="cluster-menu-head">
        <span>{{ operatorLive ? 'Every cluster on this cell' : 'Your clusters' }}</span>
        <span class="cluster-menu-count">{{ clusters.length }}</span>
      </div>
      <input
        v-if="clusters.length > 8"
        v-model="filter"
        class="cluster-filter"
        type="text"
        placeholder="Filter tenant or cluster…"
      />
      <div class="cluster-menu-list">
        <button
          v-for="c in filtered"
          :key="c.id"
          class="cluster-option"
          :class="{ 'cluster-option-active': c.id === actingCluster?.id }"
          @click="pick(c)"
        >
          <div class="cluster-option-main">
            <span class="cluster-tenant">{{ c.tenant_slug }}</span>
            <span class="cluster-sep">/</span>
            <span class="cluster-name">{{ c.slug }}</span>
          </div>
          <div class="cluster-option-meta">
            <span class="cluster-role">{{ operatorLive ? 'operator' : c.role }}</span>
            <span v-if="c.status && c.status !== 'active'" class="cluster-status">{{ c.status }}</span>
          </div>
        </button>
        <div v-if="!filtered.length" class="cluster-empty">No cluster matches</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'

import { useIdentity } from '@/stores/identity'

defineProps({ collapsed: Boolean })

const {
  clusters, actingCluster, actingClusterSlug, actingTenantSlug, actingCellSlug,
  role, operatorLive, setActingCluster,
} = useIdentity()

const open = ref(false)
const filter = ref('')
const root = ref(null)

const filtered = computed(() => {
  const q = filter.value.trim().toLowerCase()
  if (!q) return clusters.value
  return clusters.value.filter(c =>
    `${c.tenant_slug}/${c.slug}`.toLowerCase().includes(q)
  )
})

const roleLabel = computed(() => {
  // A live operator acts as admin on every cluster; say both, because the
  // rights on screen are not the ones a membership row granted.
  if (operatorLive.value) return 'operator · admin'
  return role.value ? `role ${role.value}` : 'no role'
})

const miniLabel = computed(() =>
  (actingClusterSlug.value || '?').slice(0, 2).toUpperCase()
)

const titleText = computed(() =>
  `tenant ${actingTenantSlug.value || '—'} · cluster ${actingClusterSlug.value || '—'} · cell ${actingCellSlug.value || 'unknown'} · ${roleLabel.value}`
)

const pick = (c) => {
  setActingCluster(c.id)
  open.value = false
  filter.value = ''
}

const onDocClick = (e) => {
  if (open.value && root.value && !root.value.contains(e.target)) open.value = false
}
onMounted(() => document.addEventListener('mousedown', onDocClick))
onUnmounted(() => document.removeEventListener('mousedown', onDocClick))
</script>
