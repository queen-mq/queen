<template>
  <!-- Narrow screens: the sidebar is a drawer over the page. -->
  <Transition name="drawer-scrim">
    <div v-if="mobileOpen" class="sidebar-overlay" @click="mobileOpen = false" />
  </Transition>

  <button
    @click="mobileOpen = !mobileOpen"
    class="sidebar-mobile-toggle"
    :aria-label="mobileOpen ? 'Close navigation' : 'Open navigation'"
  >
    <svg v-if="!mobileOpen" class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5"/></svg>
    <svg v-else class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"/></svg>
  </button>

  <aside class="sidebar" :class="{ 'sidebar-mobile-open': mobileOpen, rail }">
    <!-- Brand. As tall as the top bar, so the two share a baseline; no rule
         under it. The wordmark, like Gate's and Traceflow's: the sunflower on
         the q, then the name; the rail keeps the q alone. Petals in the logo's
         yellow, letters in the name's colour (assets/logo.py writes both). -->
    <router-link to="/" class="brand" aria-label="Queen overview" @click="closeMobile">
      <span class="brand-mark" aria-hidden="true" v-html="rail ? mark : wordmark" />
      <span v-if="!rail && brokerVersion" class="brand-ver" :title="`Broker ${brokerVersion}`">{{ shortVersion }}</span>
    </router-link>

    <div class="sidebar-content">
      <!-- Navigation, derived from route meta filtered by what identity
           grants. An entry the user cannot use is never in the DOM. On the
           right of a row that lists things: how many there are — or, when
           something there needs you, the Overview's marks instead. In the
           rail a row is its icon, the most severe mark sits on the icon's
           corner, and a group's label is a short rule of the same height, so
           no icon moves when the sidebar changes width. -->
      <nav class="nav-groups" aria-label="Main">
        <div class="nav-group" v-for="(group, gi) in navGroups" :key="group.label">
          <template v-if="gi > 0">
            <div v-if="rail" class="nav-rule" aria-hidden="true" />
            <div v-else class="nav-label">{{ group.label }}</div>
          </template>
          <router-link
            v-for="item in group.items"
            :key="item.path"
            :to="item.path"
            class="nav-item"
            :class="{ 'nav-item-active': isActive(item.path) }"
            :aria-current="isActive(item.path) ? 'page' : undefined"
            :title="!rail && item.scope === 'cell' ? `${item.name} — covers every tenant on this cell` : undefined"
            @click="closeMobile"
            @mouseenter="showTip($event, item.name, railNote(item))"
            @mouseleave="hideTip"
            @focus="showTip($event, item.name, railNote(item))"
            @blur="hideTip"
          >
            <Icon v-if="RAIL_ICONS[item.icon]" :name="RAIL_ICONS[item.icon]" class="nav-icon" :size="18" :stroke="1.6" />
            <span class="nav-name" :class="{ 'sr-only': rail }">{{ item.name }}</span>
            <span v-if="rail && marks[item.path]" class="g nav-corner" :class="marks[item.path].parts[0].glyph" aria-hidden="true" />
            <span v-else-if="marks[item.path]" class="nav-mark" :title="marks[item.path].title">
              <span v-for="part in marks[item.path].parts" :key="part.glyph" class="nav-mark-part">
                <span class="g" :class="part.glyph" aria-hidden="true" /><span v-if="part.n" aria-hidden="true">{{ part.n }}</span>
              </span>
              <span class="sr-only">{{ marks[item.path].title }}</span>
            </span>
            <span v-else-if="figures[item.path] && !rail" class="nav-fig" :title="figures[item.path].title">{{ figures[item.path].text }}</span>
          </router-link>
        </div>
      </nav>

      <!-- Where you are: whose numbers these are, the cell they live on, and
           how that cell is. A control only when there is something to switch
           to; otherwise words, which must not look like a button. -->
      <!-- The rail has no foot: the cell's health is System's (its icon is
           in the rail) and the top bar's; the drawer keeps who and where. -->
      <div v-if="!rail" class="sidebar-foot">
        <template v-if="!rail">
        <ClusterSelector v-if="switchable" />
        <div v-else class="whoami" :title="whoamiTitle">
          <b>{{ tenantLabel }}<template v-if="!standalone && clusterLabel"><span class="whoami-sep"> / </span><span class="whoami-cluster">{{ clusterLabel }}</span></template></b>
          <span class="whoami-sub">{{ whoamiSub }}</span>
        </div>
        </template>

        <component
          :is="canOperate ? 'router-link' : 'p'"
          v-bind="canOperate ? { to: '/system' } : {}"
          class="cell-line"
          :title="rail ? undefined : healthTitle"
          @click="closeMobile"
          @mouseenter="showTip($event, `${tenantLabel}${clusterLabel ? ` / ${clusterLabel}` : ''}`, `${cellWord}${raftText ? ` · ${raftText}` : ''}`)"
          @mouseleave="hideTip"
        >
          <span class="g" :class="cellGlyph" aria-hidden="true" />
          <span class="cell-words" :class="{ 'sr-only': rail }">
            <b :class="cellTone">{{ cellWord }}</b><template v-if="raftText"> · {{ raftText }}</template><template v-if="raftLagText"> · <span :class="raftLagTone">{{ raftLagText }}</span></template>
          </span>
        </component>
      </div>
    </div>
  </aside>

  <!-- The rail's names, at once and beside the icon. On the body, so the
       column's own scrolling cannot clip it. -->
  <Teleport to="body">
    <div v-if="tip" class="rail-tip" role="tooltip" :style="{ left: `${tip.x}px`, top: `${tip.y}px` }">
      {{ tip.text }}<span v-if="tip.note" class="rail-tip-note">{{ tip.note }}</span>
    </div>
  </Teleport>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import mark from '@/assets/q.svg?raw'
import wordmark from '@/assets/queen.svg?raw'
import ClusterSelector from '@/components/ClusterSelector.vue'
import Icon from '@/components/Icon.vue'
import { system } from '@/api'
import { formatNumber } from '@/composables/useApi'
import { queueTabOf } from '@/composables/navigation'
import { groupAttention, queueAttention, summarize } from '@/composables/useAttention'
import { useAutoRefresh } from '@/composables/useRefresh'
import { raftLagSeverity } from '@/composables/useSeverity'
import { useEphemeralStore } from '@/stores/ephemeralStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { rail } from '@/composables/useSidebar'

const route = useRoute()
const router = useRouter()
const {
  can, epoch, standalone, clusters, operatorLive, role,
  actingTenantSlug, actingClusterSlug, actingCellSlug,
} = useIdentity()
const mobileOpen = ref(false)

// The drawer closes on navigation at every width where it is a drawer.
const DRAWER_MAX = 1100
const closeMobile = () => { if (window.innerWidth <= DRAWER_MAX) mobileOpen.value = false }
watch(() => route.path, closeMobile)

const tenantLabel = computed(() => actingTenantSlug.value || 'no tenant')
const clusterLabel = computed(() => actingClusterSlug.value || '')
const cellLabel = computed(() => actingCellSlug.value || 'unknown cell')
const canOperate = computed(() => can('operator'))

// ---------------------------------------------------------------------------
// Where you are. The picker is a control only when it has somewhere to go: a
// live operator (every cluster on the cell) or a member of more than one.
// ---------------------------------------------------------------------------
const switchable = computed(() => !standalone.value && (operatorLive.value || clusters.value.length > 1))
const roleLabel = computed(() => (role.value ? `role ${role.value}` : 'no role'))
const whoamiSub = computed(() => (
  standalone.value ? `${cellLabel.value} · standalone` : `cell ${cellLabel.value} · ${roleLabel.value}`
))
const whoamiTitle = computed(() => (
  standalone.value
    ? `Tenant ${tenantLabel.value} on cell ${cellLabel.value} — standalone broker, no proxy`
    : `Tenant ${tenantLabel.value} · cluster ${clusterLabel.value || '—'} · cell ${cellLabel.value} · ${roleLabel.value}`
))

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
    // the line from claiming health it cannot see.
    health.value = null
    healthFailed.value = true
  } finally {
    loadingHealth.value = false
  }
}

const isConnected = computed(() => health.value?.status === 'healthy' || health.value?.status === 'ok')
const cellGlyph = computed(() => {
  if (healthFailed.value) return 'bad'
  if (!health.value) return 'idle'
  return isConnected.value ? 'ok' : 'bad'
})
const cellWord = computed(() => {
  if (healthFailed.value) return 'Cell unreachable'
  if (!health.value) return loadingHealth.value ? 'Cell · checking' : 'Cell status unknown'
  return isConnected.value ? 'Cell healthy' : `Cell ${health.value.status || 'degraded'}`
})
const cellTone = computed(() => (cellGlyph.value === 'bad' ? 'is-bad' : ''))
const brokerVersion = computed(() => health.value?.version || null)
// "2.0.0-alpha.4" -> "2.0 alpha"; anything unexpected is shown as it came.
const shortVersion = computed(() => {
  const v = brokerVersion.value
  const m = v && /^(\d+)\.(\d+)\.\d+(?:-([a-z]+))?/i.exec(v)
  return m ? `${m[1]}.${m[2]}${m[3] ? ` ${m[3]}` : ''}` : v
})
const raft = computed(() => (health.value && !healthFailed.value ? health.value.raft || null : null))
const raftText = computed(() => {
  const r = raft.value
  if (!r) return ''
  return `${r.role || 'unknown role'}, term ${r.term ?? '—'}`
})
// Commit lag is said only when there is some: "0 entries" is not news.
const raftLagText = computed(() => {
  const lag = raft.value?.lag
  if (!lag) return ''
  return `${formatNumber(lag)} ${lag === 1 ? 'entry' : 'entries'} behind`
})
const raftLagTone = computed(() => (raftLagSeverity(raft.value?.lag) === 'warn' ? 'is-warn' : ''))
const healthTitle = computed(() => {
  const parts = [`Cell ${cellLabel.value}`]
  if (brokerVersion.value) parts.push(`broker ${brokerVersion.value}`)
  if (raft.value) parts.push(`Raft ${raft.value.role || '—'}, term ${raft.value.term ?? '—'}, commit lag ${raft.value.lag ?? '—'}`)
  if (canOperate.value) parts.push('open System')
  return parts.join(' · ')
})

refreshHealth()
useAutoRefresh(refreshHealth)
// A cluster switch can mean a different cell entirely, so the line must stop
// asserting the old one immediately. The refetch itself comes from the shell,
// which fires every registered refresh callback on the same switch.
watch(epoch, () => { health.value = null; healthFailed.value = false })

// ---------------------------------------------------------------------------
// What is here, and what needs you. Every list comes from a shared store the
// pages use too: the queue listing (which also carries the KV and dead-letter
// totals), the consumer groups and the ephemeral queues. The consumer-group
// read is a full scan on the broker, so the sidebar never adds one within a
// minute of anybody else's (stores/groupsStore). The rule is
// composables/useAttention, the Overview's own, so a mark here always matches
// the page behind it. The lines the rule judges by are the tenant's settings
// (stores/settingsStore); this component is on every page, so its tick is the
// one that keeps them read.
// ---------------------------------------------------------------------------
const queuesStore = useQueuesStore()
const ephemeralStore = useEphemeralStore()
const groupsStore = useGroupsStore()
const queueOpsStore = useQueueOpsStore()
const settingsStore = useSettingsStore()
const groups = groupsStore.groups // null = not read yet
const groupsFailed = computed(() => groupsStore.error.value !== null)

const refreshAttention = () => Promise.allSettled([
  // A tick every 30 s; a list another page read in the last 25 s is reused.
  queuesStore.fetchQueues({ ttlMs: 25_000 }),
  groupsStore.fetchGroups(),
  // The hour of acks per queue, for the same rule (30 s between reads).
  queueOpsStore.fetchOps(),
  // Quiet about a broker without the class: it is a figure, not a check.
  ephemeralStore.fetchQueues(),
  settingsStore.fetchSettings(),
])

refreshAttention()
useAutoRefresh(refreshAttention)

const plural = (n, one, many) => `${formatNumber(n)} ${n === 1 ? one : many}`
const parts = (bad, warn) => [
  bad > 0 && { glyph: 'bad', n: bad },
  warn > 0 && { glyph: 'warn', n: warn },
].filter(Boolean)
// The column is a glance; the page has the exact figure.
const compact = (n) => {
  if (n >= 1e6) return `${(n / 1e6).toFixed(n >= 1e7 ? 0 : 1)}M`
  if (n >= 1e4) return `${Math.round(n / 1e3)}k`
  return formatNumber(n)
}

// Counts of what a row lists, where the count is measured. None for Timers:
// the listing's timer total is not measured on this broker, and a confident
// 0 next to a page of timers is worse than no figure. Zero is not shown.
const figures = computed(() => {
  const out = {}
  const put = (path, n, one, many) => {
    if (n > 0) out[path] = { text: compact(n), title: plural(n, one, many) }
  }
  if (queuesStore.error.value === null && queuesStore.lastFetched.value) {
    const qs = queuesStore.queues.value
    put('/queues', qs.length, 'queue', 'queues')
    put('/dlq', qs.reduce((s, q) => s + (Number(q.messages?.deadLetter) || 0), 0), 'message in dead letter', 'messages in dead letter')
    put('/kv', Number(queuesStore.kvRows.value) || 0, 'key', 'keys')
  }
  if (groups.value && !groupsFailed.value) put('/consumers', groups.value.length, 'consumer group', 'consumer groups')
  if (ephemeralStore.available.value) put('/ephemeral', Number(ephemeralStore.count.value) || 0, 'ephemeral queue', 'ephemeral queues')
  return out
})

const marks = computed(() => {
  const out = {}
  const queuesUnknown = queuesStore.error.value !== null
  if (queuesUnknown || groupsFailed.value) {
    // Not knowing is said as not knowing — a ring, never silence, which would
    // read as "all fine".
    const title = 'Status unknown: the queue or consumer-group list could not be read'
    out['/queues'] = { parts: [{ glyph: 'idle', n: 0 }], title }
    out['/consumers'] = { parts: [{ glyph: 'idle', n: 0 }], title }
    return out
  }
  if (groups.value === null || !queuesStore.lastFetched.value) return out

  const qa = queueAttention(queuesStore.queues.value, groups.value, settingsStore.linesFor, queueOpsStore.ops.value)
  const q = summarize(qa.map((i) => i.sev))
  if (q.sev) {
    const bad = qa.filter((i) => i.sev === 'bad').length
    const behind = qa.filter((i) => i.sev === 'warn' && i.reason === 'lag').length
    const unread = qa.filter((i) => i.reason === 'noReader').length
    const acks = qa.filter((i) => i.sev === 'warn' && i.reason === 'acks').length
    const why = [
      bad && `${bad} failing`,
      behind && `${behind} behind`,
      acks && `${acks} with acks failing`,
      unread && `${unread} with no reader`,
    ].filter(Boolean).join(', ')
    out['/queues'] = { parts: parts(bad, qa.length - bad), title: `${plural(q.count, 'queue needs', 'queues need')} you: ${why}` }
  }

  const gs = groups.value.map((group) => groupAttention(group, settingsStore.linesFor))
  const g = summarize(gs)
  if (g.sev) {
    const bad = gs.filter((s) => s === 'bad').length
    const behind = gs.filter((s) => s === 'warn').length
    const why = [bad && `${bad} falling behind`, behind && `${behind} behind`].filter(Boolean).join(', ')
    out['/consumers'] = { parts: parts(bad, behind), title: `${plural(g.count, 'consumer group is', 'consumer groups are')} behind: ${why}` }
  }
  return out
})

// ---------------------------------------------------------------------------
// The rail. Its rows are icons, so a row's name (and what its count or mark
// says) is shown beside it on hover or focus, without the browser's title
// delay.
// ---------------------------------------------------------------------------
const tip = ref(null)
const showTip = (e, text, note = '') => {
  if (!rail.value) return
  const r = e.currentTarget.getBoundingClientRect()
  tip.value = { text, note, x: Math.round(r.right + 10), y: Math.round(r.top + r.height / 2) }
}
const hideTip = () => { tip.value = null }
watch(rail, hideTip)
watch(() => route.path, hideTip)
const railNote = (item) => marks.value[item.path]?.title || figures.value[item.path]?.title || ''

// A page with no row of its own lights the row that stands for it (Users,
// under Members). Every tab of a queue lights Queues: Consumer groups and
// Supervisors only while they show one queue.
const isActive = (path) => {
  if (queueTabOf(route)) return path === '/queues'
  if (route.meta?.navParent === path) return true
  return path === '/' ? route.path === '/' : route.path.startsWith(path)
}

// ---------------------------------------------------------------------------
// Nav, straight off the route table. Group order is fixed; the operator group
// is last and named for what it covers, because its pages answer for the
// CELL and not for the acting tenant. The first group carries no label.
// ---------------------------------------------------------------------------
const GROUP_ORDER = ['Overview', 'Messaging', 'Workers', 'Analysis', 'Access', 'Cell']
const OPERATOR_GROUP = 'Cell'

const navGroups = computed(() => {
  const groupsByLabel = new Map()
  for (const r of router.getRoutes()) {
    const nav = r.meta?.nav
    if (!nav) continue
    if (!can(r.meta.requires || 'read')) continue
    if (r.meta.proxyOnly && standalone.value) continue
    if (!groupsByLabel.has(nav.group)) groupsByLabel.set(nav.group, [])
    groupsByLabel.get(nav.group).push({
      name: r.meta.title,
      path: r.path,
      icon: nav.icon,
      order: nav.order ?? 99,
      scope: r.meta.scope || 'tenant',
    })
  }
  return GROUP_ORDER
    .filter(label => groupsByLabel.has(label))
    .map(label => ({
      label: label === OPERATOR_GROUP ? 'Cell · every tenant' : label,
      items: groupsByLabel.get(label).sort((a, b) => a.order - b.order),
    }))
})

// ---------------------------------------------------------------------------
// Row icons: Lucide's (components/Icon.vue), one line weight, one ink, chosen
// for what each page holds: a bolt for the class that lives in RAM, a key for
// KV, a padlock for locks, a route for traces, the sliders for the lines.
// ---------------------------------------------------------------------------
const RAIL_ICONS = {
  dashboard: 'layout-dashboard',
  queues: 'layers',
  ephemeral: 'zap',
  kv: 'key-round',
  locks: 'lock',
  supervisors: 'server-cog',
  workload: 'chart-column',
  traces: 'route',
  settings: 'sliders-horizontal',
  system: 'server',
  members: 'users',
  keys: 'id-card',
}
</script>

<style>
.animate-spin { animation: spin 1s linear infinite; }
</style>
