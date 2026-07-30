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
          <img src="/queen-badge-open.svg" alt="" />
          <span class="brand-health" :class="isConnected ? 'bg-ok' : 'bg-ember'" />
        </div>
        <div v-if="!props.collapsed" class="brand-text">
          <span class="brand-word">Queen<b>MQ</b></span>
        </div>
      </div>

      <!-- Navigation -->
      <nav class="nav-groups">
        <div class="nav-group" v-for="group in navGroups" :key="group.label">
          <div class="label-xs" v-if="!props.collapsed">{{ group.label }}</div>
          <router-link
            v-for="item in group.items"
            :key="item.path"
            :to="item.path"
            class="nav-item"
            :class="{ 'nav-item-active': isActive(item.path) }"
            @click="closeMobile"
          >
            <component :is="item.icon" style="width:16px; height:16px;" />
            <span v-if="!props.collapsed">{{ item.name }}</span>
          </router-link>
        </div>
      </nav>

      <!-- Footer -->
      <div class="sidebar-foot">
        <!-- Proxy user -->
        <div v-if="isProxied && !props.collapsed" class="env-pill">
          <span style="font-size:11px; color:var(--text-mid);">{{ proxyUser?.username || 'User' }}</span>
          <button @click="logout" class="env-refresh" title="Logout">
            <svg style="width:14px; height:14px;" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M15.75 9V5.25A2.25 2.25 0 0013.5 3h-6a2.25 2.25 0 00-2.25 2.25v13.5A2.25 2.25 0 007.5 21h6a2.25 2.25 0 002.25-2.25V15M12 9l-3 3m0 0l3 3m-3-3h12.75"/></svg>
          </button>
        </div>

        <p v-if="!props.collapsed" style="font-size:10.5px; color:var(--text-low); text-align:center; margin-top:4px; font-family:'JetBrains Mono',monospace;">v{{ appVersion }}</p>

        <button class="collapse-btn" @click="$emit('toggle-collapse')">
          <svg style="width:14px; height:14px;" :style="props.collapsed ? 'transform:rotate(180deg)' : ''" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.8"><path stroke-linecap="round" stroke-linejoin="round" d="M15 6l-6 6 6 6"/></svg>
        </button>
      </div>
    </div>
  </aside>
</template>

<script setup>
import { ref, computed, onMounted, watch, h } from 'vue'
import { useRoute } from 'vue-router'
import { system } from '@/api'
import { useProxy } from '@/composables/useProxy'
import { version as appVersion } from '../../package.json'

const props = defineProps({ collapsed: Boolean })
defineEmits(['toggle-collapse'])

const route = useRoute()
const { isProxied, proxyUser, logout } = useProxy()
const mobileOpen = ref(false)

const closeMobile = () => { if (window.innerWidth < 1024) mobileOpen.value = false }
watch(() => route.path, closeMobile)

const health = ref(null)
const loadingHealth = ref(false)
const isConnected = computed(() => health.value?.status === 'healthy' || health.value?.status === 'ok')
const healthStatus = computed(() => {
  if (loadingHealth.value) return { text: 'Checking…' }
  if (!health.value) return { text: 'Offline' }
  if (isConnected.value) return { text: 'Healthy' }
  return { text: 'Degraded' }
})
const refreshHealth = async () => {
  loadingHealth.value = true
  try { health.value = (await system.getHealth()).data } catch { health.value = null }
  finally { loadingHealth.value = false }
}
onMounted(() => { refreshHealth(); setInterval(refreshHealth, 30000) })

const isActive = (path) => path === '/' ? route.path === '/' : route.path.startsWith(path)

const navGroups = [
  { label: 'Overview', items: [
    { name: 'Dashboard', path: '/', icon: DashboardIcon },
    { name: 'Queue Operations', path: '/operations', icon: OperationsIcon },
  ]},
  { label: 'Routing', items: [
    { name: 'Queues', path: '/queues', icon: QueuesIcon },
    { name: 'Consumers', path: '/consumers', icon: ConsumersIcon },
    { name: 'Messages', path: '/messages', icon: MessagesIcon },
  ]},
  { label: 'Observability', items: [
    { name: 'Traces', path: '/traces', icon: TracesIcon },
    { name: 'Analytics', path: '/analytics', icon: AnalyticsIcon },
    { name: 'Dead letter', path: '/dlq', icon: DlqIcon },
  ]},
  { label: 'Infrastructure', items: [
    { name: 'System', path: '/system', icon: SystemIcon },
  ]},
]

function DashboardIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('rect',{x:'3',y:'3',width:'7',height:'9',rx:'1.5'}),h('rect',{x:'14',y:'3',width:'7',height:'5',rx:'1.5'}),h('rect',{x:'14',y:'12',width:'7',height:'9',rx:'1.5'}),h('rect',{x:'3',y:'16',width:'7',height:'5',rx:'1.5'})]) }
function OperationsIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6', 'stroke-linecap':'round', 'stroke-linejoin':'round' }, [h('path',{d:'M3 12h3l2-6 4 12 2.5-7 1.5 4H21'})]) }
function QueuesIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M3 7h18M3 12h18M3 17h18'}),h('circle',{cx:'6',cy:'7',r:'1.2',fill:'currentColor'}),h('circle',{cx:'10',cy:'12',r:'1.2',fill:'currentColor'}),h('circle',{cx:'8',cy:'17',r:'1.2',fill:'currentColor'})]) }
function ConsumersIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('circle',{cx:'9',cy:'8',r:'3'}),h('circle',{cx:'17',cy:'10',r:'2.2'}),h('path',{d:'M3 20c0-3.3 2.7-6 6-6s6 2.7 6 6M15 20c.2-2 1.6-3.5 3.3-3.9'})]) }
function MessagesIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M4 6h16v10a2 2 0 01-2 2H9l-5 4V6Z'}),h('path',{d:'M8 11h8M8 14h5'})]) }
function TracesIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M3 6h6M11 10h8M7 14h10M3 18h6'}),h('circle',{cx:'9',cy:'6',r:'1.6',fill:'currentColor'}),h('circle',{cx:'19',cy:'10',r:'1.6',fill:'currentColor'}),h('circle',{cx:'17',cy:'14',r:'1.6',fill:'currentColor'}),h('circle',{cx:'9',cy:'18',r:'1.6',fill:'currentColor'})]) }
function AnalyticsIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M4 20V10M10 20V4M16 20v-8M22 20H2'})]) }
function SystemIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('rect',{x:'3',y:'4',width:'18',height:'6',rx:'1.6'}),h('rect',{x:'3',y:'14',width:'18',height:'6',rx:'1.6'}),h('circle',{cx:'7',cy:'7',r:'.9',fill:'currentColor'}),h('circle',{cx:'7',cy:'17',r:'.9',fill:'currentColor'})]) }
function DlqIcon(p) { return h('svg', { ...p, fill:'none', viewBox:'0 0 24 24', stroke:'currentColor', 'stroke-width':'1.6' }, [h('path',{d:'M5 7h14l-1.2 11.2a2 2 0 01-2 1.8H8.2a2 2 0 01-2-1.8L5 7Z'}),h('path',{d:'M9 4h6v3H9z'})]) }
</script>

<style>
.sidebar-overlay {
  position: fixed; inset: 0; background: rgba(4,4,6,.6);
  backdrop-filter: blur(6px); z-index: 44; display: none;
}
.sidebar-mobile-toggle {
  position: fixed; top: 10px; left: 10px; z-index: 50;
  width: 36px; height: 36px; border-radius: 9px;
  border: 1px solid var(--bd-hi); display: none;
  place-items: center; cursor: pointer; color: var(--text-mid);
}
html:not(.light) .sidebar-mobile-toggle { background: rgba(20,20,26,.9); }
html.light .sidebar-mobile-toggle { background: rgba(255,255,255,.9); }

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
