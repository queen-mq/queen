<script setup>
import { ref, computed, onMounted, provide } from 'vue'
import { bootstrapSession, apiFetch, fetchMe, setActCluster, logout } from './api.js'
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

// --- shared-host cluster naming (acting.rs decision z) ----------------------
// On a host in QUEEN_PROXY_SHARED_HOSTS the Host label names no cluster, and a
// session must name one with the act-as header on every call. /auth/me is the
// tell: `acting_cluster: null` means this request resolved to no cluster — the
// selector case — while a per-cluster hostname reports the Host's own cluster,
// and there the header must stay OFF (the data plane honours it on any host,
// so sending it blindly would retarget /api/v1/* calls).
const me = ref(null)
const acting = ref(null) // the picked clusters[] row; null off shared hosts
const needsPick = ref(false)
const pickHint = ref(null)

const selectable = computed(() => me.value?.clusters ?? [])
const showSwitcher = computed(() => acting.value !== null && selectable.value.length > 1)

// The remembered pick survives reloads per-origin, which on a shared host is
// per shared hostname — exactly the scope the choice is valid for.
const CLUSTER_STORE_KEY = 'queen-console-cluster'

function rememberedCluster() {
  try {
    return localStorage.getItem(CLUSTER_STORE_KEY)
  } catch {
    return null // storage blocked (private mode); the picker just shows again
  }
}
function rememberCluster(id) {
  try {
    localStorage.setItem(CLUSTER_STORE_KEY, id)
  } catch {
    // not persisted; the in-memory pick still holds for this page
  }
}
function forgetCluster() {
  try {
    localStorage.removeItem(CLUSTER_STORE_KEY)
  } catch {
    // nothing to forget
  }
}

// Topbar/selector label: the slug, tenant-qualified only when the list holds
// the same slug twice (an operator sees every tenant's clusters).
function labelOf(c) {
  const dup = selectable.value.filter((x) => x.slug === c.slug).length > 1
  return dup ? `${c.tenant_slug}/${c.slug}` : c.slug
}

function useCluster(c) {
  acting.value = c
  rememberCluster(c.id)
  // The uuid, not the slug: resolve_ref takes either, and the id cannot go
  // stale under a cluster rename.
  setActCluster(c.id, me.value?.act_cluster_header)
}

/** Decide whether this session must name a cluster, and with which one.
 * Returns true when boot may proceed, false when the picker must be shown. */
async function resolveCluster() {
  let info = null
  try {
    info = await fetchMe()
  } catch {
    // /auth/me being down must not take per-cluster hosts with it: without an
    // answer, behave exactly as the console always has (no header). A shared
    // host then surfaces its own 403 below, which at least names the problem.
    return true
  }
  me.value = info
  // The Host (or a claim-scoped token) already names the cluster: header off.
  if (info.acting_cluster) return true
  const clusters = info.clusters || []
  if (clusters.length === 0) {
    throw new Error('this account has no clusters; ask an admin to grant one')
  }
  const remembered = rememberedCluster()
  const pick =
    clusters.find((c) => c.id === remembered) || (clusters.length === 1 ? clusters[0] : null)
  if (!pick) {
    needsPick.value = true
    return false
  }
  useCluster(pick)
  return true
}

async function chooseCluster(c) {
  useCluster(c)
  needsPick.value = false
  pickHint.value = null
  try {
    await loadOverview()
    ready.value = true
  } catch (e) {
    bootError.value = e.message || String(e)
  }
}

function switchCluster(event) {
  const id = event.target.value
  if (!id || id === acting.value?.id) return
  rememberCluster(id)
  // Full reload rather than switching in place: tabs stay mounted once
  // activated, so an in-place switch would keep showing another cluster's
  // rows until every view refreshed itself.
  window.location.reload()
}

onMounted(async () => {
  try {
    await bootstrapSession()
    if (!(await resolveCluster())) return // picker shown; boot resumes on pick
    await loadOverview()
    ready.value = true
  } catch (e) {
    // A remembered cluster can die under the session (role revoked, cluster
    // deleted): fall back to the picker instead of parking on a dead 403.
    if (e?.status === 403 && acting.value && selectable.value.length > 1) {
      forgetCluster()
      acting.value = null
      setActCluster(null)
      needsPick.value = true
      pickHint.value = 'The previously selected cluster is no longer available here; pick another.'
      return
    }
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
        <select v-if="showSwitcher" :value="acting.id" @change="switchCluster" title="Switch cluster">
          <option v-for="c in selectable" :key="c.id" :value="c.id">{{ labelOf(c) }}</option>
        </select>
        <span v-else class="slug mono">{{ overview.slug }}</span>
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
    <div v-if="bootError" class="banner banner-error">{{ bootError }}</div>
    <section v-else-if="needsPick" class="card">
      <h2>Choose a cluster</h2>
      <p class="hint">This host serves several clusters; pick the one to open.</p>
      <div v-if="pickHint" class="banner banner-warn"><p>{{ pickHint }}</p></div>
      <table class="list">
        <thead>
          <tr>
            <th>Cluster</th>
            <th>Tenant</th>
            <th>Role</th>
            <th>Status</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="c in selectable" :key="c.id">
            <td class="mono">{{ c.slug }}</td>
            <td class="mono">{{ c.tenant_slug }}</td>
            <td>{{ c.role }}</td>
            <td><span class="status" :class="'status-' + c.status">{{ statusLabel(c.status) }}</span></td>
            <td><button class="btn btn-primary btn-sm" @click="chooseCluster(c)">Open</button></td>
          </tr>
        </tbody>
      </table>
    </section>
    <div v-else-if="!ready" class="hint">Loading…</div>
    <template v-else>
      <Overview v-show="active === 'overview'" :overview="overview" />
      <Queues v-if="activated.includes('queues')" v-show="active === 'queues'" :overview="overview" />
      <Keys v-if="activated.includes('keys')" v-show="active === 'keys'" />
      <Members v-if="activated.includes('members')" v-show="active === 'members'" />
    </template>
  </main>
</template>
