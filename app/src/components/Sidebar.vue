<template>
  <!-- Mobile overlay -->
  <div v-if="mobileOpen" class="sidebar-overlay" @click="mobileOpen = false" />

  <!-- Mobile toggle -->
  <button @click="mobileOpen = !mobileOpen" class="sidebar-mobile-toggle">
    <svg v-if="!mobileOpen" class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5"/></svg>
    <svg v-else class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"/></svg>
  </button>

  <aside class="sidebar" :class="{ 'sidebar-mobile-open': mobileOpen }">
    <div class="sidebar-glass" />

    <div class="sidebar-content">
      <!-- Brand -->
      <div class="brand">
        <div class="brand-mark">
          <img src="/queen-mark.svg" alt="" />
          <span class="brand-health" :class="healthDot" :title="healthTitle" />
        </div>
        <div v-if="!props.collapsed" class="brand-text">
          <span class="brand-word">Queen<b>MQ</b></span>
        </div>
      </div>

      <!-- Acting tenant / cluster. Present on every route because every number
           on every route belongs to it. Standalone (broker-direct) has exactly
           one synthetic cluster and nothing to switch to — no selector. -->
      <ClusterSelector v-if="!standalone" :collapsed="props.collapsed" />

      <!-- Navigation, derived from route meta filtered by what identity
           grants. An entry the user cannot use is never in the DOM. -->
      <nav class="nav-groups">
        <div
          class="nav-group"
          :class="{ 'nav-group-operator': group.operator }"
          v-for="group in navGroups"
          :key="group.label"
        >
          <div class="label-xs nav-group-label" v-if="!props.collapsed">
            <span>{{ group.label }}</span>
            <span v-if="group.operator" class="nav-scope-badge" title="Covers the whole cell, every tenant on it">cell</span>
          </div>
          <router-link
            v-for="item in group.items"
            :key="item.path"
            :to="item.path"
            class="nav-item"
            :class="{ 'nav-item-active': isActive(item.path) }"
            :title="item.scope === 'cell' ? `${item.name} — cell-level, not scoped to your tenant` : item.name"
            @click="closeMobile"
          >
            <component :is="icons[item.icon]" style="width:16px; height:16px;" />
            <span v-if="!props.collapsed">{{ item.name }}</span>
          </router-link>
        </div>
      </nav>

      <!-- Footer -->
      <div class="sidebar-foot">
        <!-- Session pill. Standalone has no session: no email to show and a
             sign-out that could only reload the page — so neither renders. -->
        <div v-if="!props.collapsed && !standalone" class="env-pill">
          <span class="user-email" :title="email || 'signed in'">{{ email || 'signed in' }}</span>
          <button @click="logout" class="env-refresh" title="Sign out">
            <svg style="width:14px; height:14px;" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M15.75 9V5.25A2.25 2.25 0 0013.5 3h-6a2.25 2.25 0 00-2.25 2.25v13.5A2.25 2.25 0 007.5 21h6a2.25 2.25 0 002.25-2.25V15M12 9l-3 3m0 0l3 3m-3-3h12.75"/></svg>
          </button>
        </div>

        <!-- The BROKER's version (cell-level), not the frontend package's:
             the app's own version number says nothing about what is serving
             the data. Unknown stays unknown. -->
        <p v-if="!props.collapsed" class="foot-version" :title="`broker on the acting cell${brokerVersion ? '' : ' — version unavailable'}`">
          broker {{ brokerVersion || 'unknown' }}
        </p>

        <button class="collapse-btn" @click="$emit('toggle-collapse')">
          <svg style="width:14px; height:14px;" :style="props.collapsed ? 'transform:rotate(180deg)' : ''" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.8"><path stroke-linecap="round" stroke-linejoin="round" d="M15 6l-6 6 6 6"/></svg>
        </button>
      </div>
    </div>
  </aside>
</template>

<script setup>
import { ref, computed, watch, h } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import ClusterSelector from '@/components/ClusterSelector.vue'
import { system } from '@/api'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useIdentity } from '@/stores/identity'

const props = defineProps({ collapsed: Boolean })
defineEmits(['toggle-collapse'])

const route = useRoute()
const router = useRouter()
const { email, can, epoch, logout, standalone } = useIdentity()
const mobileOpen = ref(false)

const closeMobile = () => { if (window.innerWidth < 1024) mobileOpen.value = false }
watch(() => route.path, closeMobile)

// ---------------------------------------------------------------------------
// Cell health. Three states, never two: reachable, unreachable, and not yet
// known — an unknown must not render as "offline" any more than as "healthy".
// ---------------------------------------------------------------------------
const health = ref(null)
const healthFailed = ref(false)
const loadingHealth = ref(false)

const refreshHealth = async () => {
  loadingHealth.value = true
  try {
    health.value = (await system.getHealth()).data
    healthFailed.value = false
  } catch {
    // The failure is already on the global surface; here it only has to stop
    // the dot from claiming health it cannot see.
    health.value = null
    healthFailed.value = true
  } finally {
    loadingHealth.value = false
  }
}

const isConnected = computed(() => health.value?.status === 'healthy' || health.value?.status === 'ok')
const healthDot = computed(() => {
  if (!health.value && !healthFailed.value) return 'bg-unknown'
  return isConnected.value ? 'bg-ok' : 'bg-ember'
})
const healthTitle = computed(() => {
  if (loadingHealth.value && !health.value) return 'Checking the cell…'
  if (healthFailed.value) return 'Cell unreachable'
  if (!health.value) return 'Cell status unknown'
  return isConnected.value ? 'Cell healthy' : 'Cell degraded'
})
const brokerVersion = computed(() => health.value?.version || null)

refreshHealth()
useAutoRefresh(refreshHealth)
// A cluster switch can mean a different cell entirely, so the dot must stop
// asserting the old one immediately. The refetch itself comes from the shell,
// which fires every registered refresh callback on the same switch.
watch(epoch, () => { health.value = null; healthFailed.value = false })

const isActive = (path) => path === '/' ? route.path === '/' : route.path.startsWith(path)

// ---------------------------------------------------------------------------
// Nav, straight off the route table. Group order is fixed; the operator group
// is last and marked, because its pages answer for the CELL and not for the
// acting tenant.
// ---------------------------------------------------------------------------
const GROUP_ORDER = ['Overview', 'Routing', 'Observability', 'Cell']
const OPERATOR_GROUP = 'Cell'

const navGroups = computed(() => {
  const groups = new Map()
  for (const r of router.getRoutes()) {
    const nav = r.meta?.nav
    if (!nav) continue
    if (!can(r.meta.requires || 'read')) continue
    if (!groups.has(nav.group)) groups.set(nav.group, [])
    groups.get(nav.group).push({
      name: r.meta.title,
      path: r.path,
      icon: nav.icon,
      order: nav.order ?? 99,
      scope: r.meta.scope || 'tenant',
    })
  }
  return GROUP_ORDER
    .filter(label => groups.has(label))
    .map(label => ({
      label: label === OPERATOR_GROUP ? 'Cell · operator' : label,
      operator: label === OPERATOR_GROUP,
      items: groups.get(label).sort((a, b) => a.order - b.order),
    }))
})

const icons = {
  dashboard: DashboardIcon,
  operations: OperationsIcon,
  queues: QueuesIcon,
  ephemeral: EphemeralIcon,
  consumers: ConsumersIcon,
  messages: MessagesIcon,
  traces: TracesIcon,
  analytics: AnalyticsIcon,
  dlq: DlqIcon,
  system: SystemIcon,
}

function DashboardIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('rect',{x:'3',y:'3',width:'7',height:'9',rx:'1.5'}),h('rect',{x:'14',y:'3',width:'7',height:'5',rx:'1.5'}),h('rect',{x:'14',y:'12',width:'7',height:'9',rx:'1.5'}),h('rect',{x:'3',y:'16',width:'7',height:'5',rx:'1.5'})]) }
function OperationsIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6', 'stroke-linecap':'round', 'stroke-linejoin':'round' }, [h('path',{d:'M3 12h3l2-6 4 12 2.5-7 1.5 4H21'})]) }
function QueuesIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M3 7h18M3 12h18M3 17h18'}),h('circle',{cx:'6',cy:'7',r:'1.2',fill:'currentColor'}),h('circle',{cx:'10',cy:'12',r:'1.2',fill:'currentColor'}),h('circle',{cx:'8',cy:'17',r:'1.2',fill:'currentColor'})]) }
/* Ephemeral: the queue glyph with a bolt through it — same stacked rows, and
   the bolt is the one thing this class is: it lives in RAM and it goes. */
function EphemeralIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6', 'stroke-linecap':'round', 'stroke-linejoin':'round' }, [h('path',{d:'M3 7h9M3 12h6M3 17h8'}),h('path',{d:'M18 3l-4 8h4l-2 10'})]) }
function ConsumersIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('circle',{cx:'9',cy:'8',r:'3'}),h('circle',{cx:'17',cy:'10',r:'2.2'}),h('path',{d:'M3 20c0-3.3 2.7-6 6-6s6 2.7 6 6M15 20c.2-2 1.6-3.5 3.3-3.9'})]) }
function MessagesIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M4 6h16v10a2 2 0 01-2 2H9l-5 4V6Z'}),h('path',{d:'M8 11h8M8 14h5'})]) }
function TracesIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M3 6h6M11 10h8M7 14h10M3 18h6'}),h('circle',{cx:'9',cy:'6',r:'1.6',fill:'currentColor'}),h('circle',{cx:'19',cy:'10',r:'1.6',fill:'currentColor'}),h('circle',{cx:'17',cy:'14',r:'1.6',fill:'currentColor'}),h('circle',{cx:'9',cy:'18',r:'1.6',fill:'currentColor'})]) }
function AnalyticsIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M4 20V10M10 20V4M16 20v-8M22 20H2'})]) }
function SystemIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('rect',{x:'3',y:'4',width:'18',height:'6',rx:'1.6'}),h('rect',{x:'3',y:'14',width:'18',height:'6',rx:'1.6'}),h('circle',{cx:'7',cy:'7',r:'.9',fill:'currentColor'}),h('circle',{cx:'7',cy:'17',r:'.9',fill:'currentColor'})]) }
function DlqIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M5 7h14l-1.2 11.2a2 2 0 01-2 1.8H8.2a2 2 0 01-2-1.8L5 7Z'}),h('path',{d:'M9 4h6v3H9z'})]) }
</script>

<style>
/* Scrim: the page colour at 60%, so it dims what is behind the drawer without
   the faint blue cast the old hand-mixed near-black carried. */
.sidebar-overlay {
  position: fixed; inset: 0;
  background: color-mix(in srgb, var(--ink-0) 60%, transparent);
  backdrop-filter: blur(6px); z-index: 44; display: none;
}
/* Floats over the page, so it has to read RAISED. Its old fill (rgb 20,20,26)
   was picked to sit at the old page level and would now be an ambiguous smear
   over var(--ink-0); --ink-3 is the fill every other bordered control uses. */
.sidebar-mobile-toggle {
  position: fixed; top: 10px; left: 10px; z-index: 50;
  width: 36px; height: 36px; border-radius: var(--r-card);
  background: var(--ink-3);
  border: 1px solid var(--bd-hi); display: none;
  place-items: center; cursor: pointer; color: var(--text-mid);
}

@media (max-width: 700px) {
  .sidebar-overlay { display: block; }
  .sidebar-mobile-toggle { display: grid; }
}
@media (min-width: 701px) {
  .sidebar-overlay { display: none !important; }
  .sidebar-mobile-toggle { display: none !important; }
}

.animate-spin { animation: spin 1s linear infinite; }
</style>
