<script setup>
// The frame of the new console: a 64 px icon rail (sections, Settings, your
// account) and one band (where you are, search, the cluster, the time
// window, the shortcuts). A page fills the rest.
import './console.css'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import logoRaw from '@/assets/q.svg?raw'
import { useUiStore } from '@/stores/ui'
import QcIcon from './QcIcon.vue'
import QcPopover from './QcPopover.vue'
import QcSearch from './QcSearch.vue'
import QcToasts from './QcToasts.vue'
import TimeChip from './TimeChip.vue'
import LookChip from './LookChip.vue'
import ClusterChip from './ClusterChip.vue'
import { theme, areas } from './useLook'
import { useWindow, clock } from './useWindow'
import { useConsoleFrame } from './useConsoleFrame'

// What the wire did, not a health check: the same flag the old shell shows.
const { offline, offlineSince } = useUiStore()
const offlineAt = computed(() => {
  const t = offlineSince.value ? new Date(offlineSince.value).getTime() : null
  return t && Number.isFinite(t) ? clock(t) : 'the last answer'
})

const props = defineProps({
  path: { type: Array, default: () => [] },    // [{ label, to? }]: where you are
  attention: { type: Object, default: null },  // { tone, count } for the Overview icon's mark
  frame: { type: Object, default: null },      // a page's own useConsoleFrame(), to share its reads
})

const route = useRoute()
const router = useRouter()
const { live } = useWindow()
const frame = props.frame || useConsoleFrame()

const RAIL = [
  [{ id: 'overview', label: 'Overview', key: 'O', to: '/' }],
  [
    { id: 'queues', label: 'Queues', key: 'Q', to: '/queues' },
    { id: 'streams', label: 'Streams', key: 'S', to: frame.paths.streams },
    { id: 'dlq', label: 'Dead letters', key: 'D', to: frame.paths.dlq },
    { id: 'workload', label: 'Workload', key: 'W', to: frame.paths.workload },
  ],
  [
    { id: 'entities', label: 'Entities', key: 'E', to: frame.paths.entities },
    { id: 'state', label: 'State', key: 'K', to: frame.paths.state },
  ],
  [{ id: 'cluster', label: 'Cluster', key: 'C', to: frame.paths.cluster }],
]
const here = computed(() => (route.meta?.console ? route.meta.section || 'overview' : null))
// The Overview's mark on every page: what needs you, by the one rule.
const mark = computed(() => props.attention || frame.attention?.value || null)

const userOpen = ref(false)
const helpOpen = ref(false)
const searchOpen = ref(false)

// ---- keyboard: ⌘K, G then a letter, L, ? ----
let gAt = 0
function typing(e) {
  const t = e.target
  return t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName))
}
function onKey(e) {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); searchOpen.value = !searchOpen.value; return }
  if ((e.metaKey || e.ctrlKey) && e.key === ',') { e.preventDefault(); router.push(frame.paths.settings); return }
  if (e.metaKey || e.ctrlKey || e.altKey || typing(e) || searchOpen.value) return
  const k = e.key.toLowerCase()
  if (gAt && Date.now() - gAt < 1200) {
    gAt = 0
    const item = RAIL.flat().find((i) => i.key.toLowerCase() === k && i.to)
    if (item) { e.preventDefault(); router.push(item.to) }
    return
  }
  if (k === 'g') { gAt = Date.now(); return }
  if (k === 'l') { live(); return }
  if (e.key === '?') { helpOpen.value = !helpOpen.value }
}
onMounted(() => document.addEventListener('keydown', onKey))
onBeforeUnmount(() => document.removeEventListener('keydown', onKey))

// The page behind the console takes its tone, so overscroll shows no seam.
const root = ref(null)
function paintBody() {
  if (!root.value) return
  document.documentElement.style.background = getComputedStyle(root.value).getPropertyValue('--bg')
}
watch([theme, areas], () => requestAnimationFrame(paintBody))
onMounted(paintBody)
onBeforeUnmount(() => { document.documentElement.style.background = '' })

const initials = computed(() => {
  const n = frame.user.value?.name || frame.user.value?.email || ''
  const parts = n.replace(/@.*/, '').split(/[\s._-]+/).filter(Boolean)
  return (parts[0]?.[0] || '?').toUpperCase() + (parts[1]?.[0] || '').toUpperCase()
})
</script>

<template>
  <div ref="root" class="qn qn-shell" :class="[theme, areas === 'tone' ? 'tone' : '']">
    <nav class="qn-rail" aria-label="Sections">
      <RouterLink to="/" class="qn-logo" aria-label="Queen: overview"><span class="qn-logo-mark" v-html="logoRaw" /></RouterLink>
      <template v-for="(group, gi) in RAIL" :key="gi">
        <div v-if="gi" class="qn-sep" />
        <template v-for="item in group" :key="item.id">
          <RouterLink
            v-if="item.to"
            :to="item.to"
            class="qn-ri"
            :class="{ on: here === item.id }"
            :aria-current="here === item.id ? 'page' : undefined"
            :aria-label="item.id === 'overview' && mark?.count ? `Overview, ${mark.count} need you` : item.label"
          >
            <QcIcon :name="item.id" />
            <span v-if="item.id === 'overview' && mark?.tone" class="qn-ri-mark qn-mark" :class="mark.tone" aria-hidden="true" />
            <span class="qn-tip">{{ item.label }} <span class="qn-kbd">G {{ item.key }}</span></span>
          </RouterLink>
          <span v-else class="qn-ri off" :aria-label="`${item.label}, ${item.note}`" tabindex="0">
            <QcIcon :name="item.id" />
            <span class="qn-tip">{{ item.label }} <span class="qn-t3">{{ item.note }}</span></span>
          </span>
        </template>
      </template>
      <div style="flex: 1 1 0" />
      <RouterLink :to="frame.paths.settings" class="qn-ri" :class="{ on: here === 'settings' }" :aria-current="here === 'settings' ? 'page' : undefined" aria-label="Settings">
        <QcIcon name="settings" />
        <span class="qn-tip">Settings <span class="qn-kbd">⌘ ,</span></span>
      </RouterLink>
      <QcPopover v-model:open="userOpen" side="right-end" :width="270" label="Your account">
        <template #trigger="{ toggle }">
          <button type="button" class="qn-ri" aria-haspopup="dialog" :aria-expanded="userOpen" aria-label="Your account" @click="toggle">
            <span class="qn-avatar">
              <template v-if="frame.user.value?.email">{{ initials }}</template>
              <svg v-else width="16" height="16" viewBox="0 0 18 18" aria-hidden="true" style="fill: none; stroke: currentColor; stroke-width: 1.4"><circle cx="9" cy="6.5" r="3" /><path d="M3.5 15.5c.8-3 3-4.5 5.5-4.5s4.7 1.5 5.5 4.5" /></svg>
            </span>
            <span v-if="!userOpen" class="qn-tip">{{ frame.user.value?.name || 'You' }}</span>
          </button>
        </template>
        <div style="padding: 8px 6px 4px">
          <p v-if="frame.user.value?.email" style="font-weight: 500; overflow-wrap: anywhere">{{ frame.user.value.email }}</p>
          <p v-else style="font-weight: 500">Straight to the broker</p>
          <p class="qn-t3" style="font-size: 12px; margin-top: 2px">
            <template v-if="frame.user.value?.role">{{ frame.user.value.role }} on </template><span class="qn-mono">{{ frame.cluster.value.name }}</span>
          </p>
          <div class="qn-popsep" style="margin-top: 12px" />
          <RouterLink :to="frame.paths.settings" class="qn-opt">Settings</RouterLink>
          <button v-if="frame.canSignOut.value" type="button" class="qn-opt" @click="frame.signOut()">Sign out</button>
        </div>
      </QcPopover>
    </nav>

    <div class="qn-body">
      <header class="qn-band">
        <nav aria-label="You are here" class="qn-path">
          <template v-for="(p, i) in path" :key="i">
            <span v-if="i" class="qn-t3" aria-hidden="true">/</span>
            <RouterLink v-if="p.to && i < path.length - 1" :to="p.to" class="qn-t2">{{ p.label }}</RouterLink>
            <span v-else :aria-current="i === path.length - 1 ? 'page' : undefined" :class="p.mono ? 'qn-mono' : ''" style="font-weight: 500">{{ p.label }}</span>
          </template>
        </nav>
        <div style="flex: 1 1 0" />
        <button type="button" class="qn-field qn-find" aria-haspopup="dialog" @click="searchOpen = true">
          <QcIcon name="search" :size="14" />
          <span style="flex: 1 1 0; text-align: left">Search</span>
          <span class="qn-kbd qn-key">⌘K</span>
        </button>
        <ClusterChip
          :name="frame.cluster.value.name"
          :mark="frame.cell.value.mark"
          :summary="frame.cell.value.summary"
          :nodes="frame.cell.value.nodes"
          :issues="frame.cell.value.issues"
          :metrics="frame.cell.value.metrics"
          :clusters="frame.clusters.value"
          @switch="frame.switchCluster"
        />
        <TimeChip />
        <LookChip />
        <QcPopover v-model:open="helpOpen" side="bottom-end" :width="300" label="Keyboard shortcuts" :focus-first="false">
          <template #trigger="{ toggle }">
            <button type="button" class="qn-chip qn-keys" aria-haspopup="dialog" :aria-expanded="helpOpen" aria-label="Keyboard shortcuts" style="width: 32px; justify-content: center; padding: 0" @click="toggle">?</button>
          </template>
          <div class="qn-help">
            <span>Search</span><span class="qn-kbd">⌘ K</span>
            <span>Go to a section</span><span class="qn-kbd">G, then its letter</span>
            <span>Back to live</span><span class="qn-kbd">L</span>
            <span>Settings</span><span class="qn-kbd">⌘ ,</span>
            <span>Close</span><span class="qn-kbd">Esc</span>
          </div>
        </QcPopover>
      </header>
      <p v-if="offline" class="qn-banner" role="alert">
        <span class="qn-mark red" aria-hidden="true" />
        <span><strong style="font-weight: 500">Cannot reach the broker.</strong> Everything here is from before {{ offlineAt }} and may be wrong.</span>
      </p>
      <p v-else-if="!frame.cluster.value.id" class="qn-banner">
        <span class="qn-mark amber" aria-hidden="true" />
        <span>No cluster: this account is a member of none, so there is nothing to show.</span>
      </p>
      <slot name="banner" />
      <main class="qn-page">
        <slot />
      </main>
    </div>

    <QcSearch v-model:open="searchOpen" :items="frame.searchItems.value" />
    <QcToasts />
  </div>
</template>

<style scoped>
/* The old body does not scroll (overflow: hidden): the console scrolls inside itself. */
.qn-shell { display: flex; height: 100vh; overflow: hidden; }
.qn-rail {
  height: 100vh; flex: 0 0 var(--rail-w);
  display: flex; flex-direction: column; align-items: center; gap: 4px;
  padding: 0 0 14px; border-right: 1px solid var(--line); z-index: 31;
}
/* The logo's cell is the band's height, so the mark sits on the band's centre line. */
.qn-logo { display: flex; align-items: center; justify-content: center; align-self: stretch; height: 52px; flex: 0 0 52px; margin-bottom: 10px; color: var(--text); }
.qn-logo:hover { text-decoration: none; }
.qn-logo-mark { display: block; height: 28px; }
.qn-logo-mark :deep(svg) { height: 28px; width: auto; display: block; }
.qn-ri {
  position: relative; display: flex; align-items: center; justify-content: center;
  width: 44px; height: 44px; border-radius: calc(var(--r-ctl) + 2px); color: var(--text2);
}
.qn-ri:hover { background: var(--bg2); color: var(--text); text-decoration: none; }
.qn-ri.on { background: var(--bg3); color: var(--text); }
.qn-ri.off { color: var(--text3); opacity: .55; cursor: default; }
.qn-ri.off:hover { background: transparent; }
.qn-ri-mark { position: absolute; top: 9px; right: 9px; width: 7px; height: 7px; }
.qn-sep { width: 24px; height: 1px; background: var(--line); margin: 6px 0; }
:global(.qn.tone .qn-sep) { background: var(--bg3); }
.qn-tip {
  position: absolute; left: 54px; top: 50%; transform: translateY(-50%);
  display: flex; align-items: center; gap: 10px; white-space: nowrap;
  padding: 5px 8px; border: 1px solid var(--line); border-radius: var(--r-ctl);
  background: var(--pop); color: var(--text); font-size: 12px;
  opacity: 0; pointer-events: none; z-index: 60;
}
.qn-ri:hover .qn-tip, .qn-ri:focus-visible .qn-tip { opacity: 1; }
:global(.qn.tone .qn-tip) { border-color: transparent; }
.qn-avatar { width: 30px; height: 30px; border-radius: 50%; background: var(--bg3); display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 500; color: var(--text2); }
.qn-body { flex: 1 1 0; min-width: 0; height: 100vh; overflow-y: auto; display: flex; flex-direction: column; }
.qn-band {
  position: sticky; top: 0; z-index: 25; background: var(--bg); flex: 0 0 auto;
  display: flex; align-items: center; flex-wrap: wrap; gap: 10px 10px;
  min-height: 52px; padding: 8px 24px; border-bottom: 1px solid var(--line);
}
.qn-path { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; min-width: 0; }
.qn-find { width: 240px; cursor: pointer; }
.qn-find:hover { border-color: var(--text3); }
.qn-key { padding: 1px 5px; border: 1px solid var(--line); border-radius: 3px; }
.qn-help { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px 16px; padding: 8px 8px 6px; }
.qn-page { flex: 1 1 auto; min-width: 0; }
.qn-banner { flex: 0 0 auto; display: flex; align-items: center; gap: 10px; padding: 9px 24px; border-bottom: 1px solid var(--line); background: var(--bg2); }
@media (max-width: 900px) { .qn-find { width: 40px; } .qn-find span { display: none; } }
@media (max-width: 640px) {
  .qn-shell { flex-direction: column; }
  .qn-rail { height: auto; flex: 0 0 auto; flex-direction: row; overflow-x: auto; padding: 6px 8px; border-right: 0; border-bottom: 1px solid var(--line); }
  .qn-body { height: auto; flex: 1 1 0; min-height: 0; }
  .qn-logo { align-self: auto; height: 44px; flex: 0 0 44px; width: 44px; margin: 0 4px 0 0; }
  .qn-sep { width: 1px; height: 24px; margin: 0 6px; }
  .qn-tip { display: none; }
  .qn-band { position: static; padding: 8px 16px; }
  /* keyboard shortcuts: nothing to press on a phone */
  .qn-keys { display: none; }
}
</style>
