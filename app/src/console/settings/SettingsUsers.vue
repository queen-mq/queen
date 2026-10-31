<script setup>
// Settings › Users and tenants: every account on the cell and the tenants they
// belong to, and an operator's changes to them: add a user, rename one, give,
// change or take away their role on a cluster. Cell-level and a live
// operator's (proxy/src/operator.rs), owned by the proxy: straight to a broker
// there are none, and the view says so in one sentence.
//
// Reads, every 30 s: GET /api/operator/users  { cell, tenants, clusters, users }.
// Writes, built by ./usersActions.js: POST /api/operator/users, PATCH
// /api/operator/users/{user}, PUT and DELETE /api/operator/users/{user}/roles/{cluster}.
// Each first shows what changes and its call (QcDialog); taking access away
// asks for the email; the proxy's answer comes back as a toast.
//
// The list never moves under the hand: a row is ranked once (tenant, then
// email), a change updates it in place, a new user comes in at the end, and a
// filter's rows are decided when the filter changes. The search, the tenant,
// the role and the picked user are the address (?uq=, ?ut=, ?ur=, ?uid=),
// dropped when another Settings page opens.
import { computed, nextTick, onBeforeUnmount, reactive, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { operator } from '@/api'
import { describeApiError } from '@/api/errors'
import { ROLES, userGroups } from '@/composables/accessGroups'
import { useAutoRefresh } from '@/composables/useRefresh'
import { replaceQueryFields, useRouteState } from '@/composables/useRouteState'
import { currentEpoch, useIdentity } from '@/stores/identity'
import QcDialog from '../QcDialog.vue'
import QcIcon from '../QcIcon.vue'
import QcPopover from '../QcPopover.vue'
import QcSeg from '../QcSeg.vue'
import { toast } from '../toasts'
import { span } from '../useOverview'
import { dayMonth, when } from '../useSettingsPage'
import {
  PROVIDERS, accessRows, createPlan, doneWords, failWords, grantsText, inRank, listingOf, onlyAdmin,
  onlyAdminSay, patchCreated, patchName, patchRole, providerLabel, rankNew, removePlan, renamePlan,
  roleCan, roleChoices, roleOn, rolePlan, roleSentence, roleWord, shownSet, whoOf, withShown,
} from './usersActions'

const props = defineProps({ where: { type: String, default: '' } })
const id = useIdentity()
const route = useRoute()
const router = useRouter()
const direct = computed(() => id.standalone.value)
const allowed = computed(() => id.can('operator'))

// ---- the listing: ranked once, patched in place after a change ----
const data = shallowRef(null) // listingOf(GET /api/operator/users)
const failed = ref(null)
const ranks = shallowRef(new Map())
let writes = 0 // a read that started before a change may not land after it
function keep(next) {
  ranks.value = rankNew(ranks.value, next.users)
  data.value = next
}
async function load() {
  if (direct.value || !allowed.value) return
  const epoch = currentEpoch()
  const w = writes
  try {
    const r = await operator.listUsers({ quiet: true })
    if (epoch !== currentEpoch() || w !== writes) return
    keep(listingOf(r?.data))
    failed.value = null
  } catch (e) {
    if (epoch === currentEpoch()) failed.value = describeApiError(e).replace(/[.\s]+$/, '')
  }
}
useAutoRefresh(load)
load()

const cell = computed(() => data.value?.cell?.slug || id.actingCellSlug.value || props.where)
const users = computed(() => data.value?.users || [])
const bySlug = (a, b) => String(a.slug).localeCompare(String(b.slug))
const tenants = computed(() => [...(data.value?.tenants || [])].sort(bySlug))
const clusters = computed(() => data.value?.clusters || [])
const multiTenant = computed(() => tenants.value.length > 1)
const isMe = (u) => !!id.email.value && String(u?.email).toLowerCase() === id.email.value.toLowerCase()
const named = (t) => (t?.name && t.name !== t.slug ? t.name : '')
function ago(v) {
  const t = v ? Date.parse(v) : NaN
  if (!Number.isFinite(t)) return 'never'
  const s = (Date.now() - t) / 1000
  if (s < 60) return 'just now'
  if (s < 7 * 86400) return `${span(s)} ago`
  return dayMonth(t)
}

// ---- the filter and the pick, in the address ----
const uq = ref('')
const ut = ref('')
const ur = ref('')
const uid = ref('')
useRouteState({ uq, ut, ur, uid })
onBeforeUnmount(() => replaceQueryFields(router, route, { uq: null, ut: null, ur: null, uid: null }))

const filter = computed(() => ({ q: uq.value, tenant: ut.value, role: ur.value }))
const shown = shallowRef(null)
watch([users, filter], () => { shown.value = shownSet(users.value, filter.value, shown.value) }, { immediate: true })

const rows = computed(() => inRank(users.value, ranks.value)
  .filter((u) => shown.value?.ids.has(u.id))
  .map((u) => ({ ...u, me: isMe(u), grants: grantsText(u), seen: ago(u.last_login_at) })))
const narrowed = computed(() => !!(uq.value.trim() || ut.value || ur.value))
function showAll() { uq.value = ''; ut.value = ''; ur.value = '' }
// The role filter counts follow the search and the tenant, live.
const roleOptions = computed(() => roleChoices(userGroups(
  users.value.filter((u) => !ut.value || u.tenant_slug === ut.value), tenants.value, { search: uq.value, role: ur.value })))
const tenantOpen = ref(false)
const cols = computed(() => (multiTenant.value
  ? 'minmax(0, 2fr) minmax(0, .7fr) 72px minmax(0, 1.5fr) 92px'
  : 'minmax(0, 2fr) 72px minmax(0, 1.5fr) 92px'))

const picked = computed(() => users.value.find((u) => u.id === uid.value) || null)
const pickedTenant = computed(() => tenants.value.find((t) => t.id === picked.value?.tenant_id) || null)
const access = computed(() => (picked.value ? accessRows(picked.value, clusters.value) : []))
const listEl = ref(null)
const panel = ref(null)
// Stacked (a phone, a narrow window), the user opens under the list: bring it up.
function reveal() {
  nextTick(() => {
    const p = panel.value; const l = listEl.value
    if (p && l && p.getBoundingClientRect().top > l.getBoundingClientRect().top + 24) p.scrollIntoView({ block: 'start', behavior: 'smooth' })
  })
}
function choose(u) {
  uid.value = uid.value === u.id ? '' : u.id
  if (uid.value) reveal()
}

// ---- one dialog at a time ----
const ask = ref(null) // { kind: 'add' } | { kind: 'rename' | 'role' | 'remove', userId, clusterId? }
const busy = ref(false)
watch(id.epoch, () => { data.value = null; ranks.value = new Map(); shown.value = null; ask.value = null; uid.value = ''; load() })
const form = reactive({ tenant: '', cluster: '', name: '', email: '', provider: 'google', password: '', role: 'viewer' })
const renameTo = ref('')
const roleTo = ref('')

const clustersOf = (tenantId) => clusters.value.filter((c) => c.tenant_id === tenantId).sort(bySlug)
function firstCluster(tenantId) {
  const own = clustersOf(tenantId)
  return (own.find((c) => c.id === id.actingCluster.value?.id) || own[0])?.id || ''
}
watch(() => form.tenant, (t) => { if (!clustersOf(t).some((c) => c.id === form.cluster)) form.cluster = firstCluster(t) })

function openAdd() {
  const t = tenants.value.find((x) => x.slug === ut.value)
    || tenants.value.find((x) => x.id === id.actingCluster.value?.tenant_id)
    || tenants.value[0]
  Object.assign(form, { tenant: t?.id || '', name: '', email: '', provider: 'google', password: '', role: 'viewer' })
  form.cluster = firstCluster(form.tenant)
  ask.value = { kind: 'add' }
}
function openRename(u) { renameTo.value = u.name || ''; ask.value = { kind: 'rename', userId: u.id } }
function openRole(u, c) { roleTo.value = roleOn(u, c.id) || 'viewer'; ask.value = { kind: 'role', userId: u.id, clusterId: c.id } }
function openRemove(u, c) { ask.value = { kind: 'remove', userId: u.id, clusterId: c.id } }
function close() {
  if (busy.value) return
  ask.value = null
  form.password = ''
}

const tenantChoices = computed(() => tenants.value.map((t) => ({ value: t.id, label: t.slug })))
const roleSeg = ROLES.map((r) => ({ value: r.name, label: r.name }))

const dlg = computed(() => {
  const a = ask.value
  if (!a || !data.value) return null
  if (a.kind === 'add') {
    const p = createPlan(form, data.value)
    return { kind: 'add', plan: p, title: 'Add a user', where: p.cluster?.slug || '', call: p.call, confirm: 'Add the user', blocked: !p.ok }
  }
  const u = users.value.find((x) => x.id === a.userId)
  if (!u) return null
  if (a.kind === 'rename') {
    const p = renamePlan(u, renameTo.value)
    return { kind: 'rename', plan: p, user: u, title: `Rename ${u.email}?`, where: `cell ${cell.value}`, call: p.call, confirm: 'Rename', blocked: !p.ok }
  }
  const c = clusters.value.find((x) => x.id === a.clusterId)
  if (!c) return null
  const who = whoOf(u)
  if (a.kind === 'role') {
    const p = rolePlan(u, c, roleTo.value, users.value)
    return {
      kind: 'role', plan: p, user: u, cluster: c,
      title: p.from ? `Change the role of ${who} on ${c.slug}?` : `Give ${who} access to ${c.slug}?`,
      where: c.slug, call: p.call, blocked: !p.ok,
      confirm: p.to && p.to !== p.from ? `Make ${who} ${roleWord(p.to)}` : 'Change the role',
    }
  }
  const p = removePlan(u, c, users.value)
  return {
    kind: 'remove', plan: p, user: u, cluster: c, title: `Remove ${who} from ${c.slug}?`,
    where: c.slug, call: p.call, confirm: `Remove from ${c.slug}`, danger: true, typed: u.email, blocked: !p.ok,
  }
})

async function run() {
  const d = dlg.value
  if (!d || busy.value || d.blocked) return
  const req = d.plan.request
  busy.value = true
  try {
    const r = await operator[req.fn](...req.args, { quiet: true })
    const answer = r?.data || {}
    writes++
    if (d.kind === 'add') {
      keep(patchCreated(data.value, answer, d.plan))
      shown.value = withShown(shown.value, String(answer.id))
      uid.value = String(answer.id)
      reveal()
    } else if (d.kind === 'rename') {
      keep(patchName(data.value, d.user.id, answer.name ?? d.plan.name))
    } else {
      keep(patchRole(data.value, d.user.id, d.cluster, d.kind === 'role' ? d.plan.to : ''))
    }
    const w = doneWords(d.kind, { status: r?.status || 200, user: d.user, cluster: d.cluster, plan: d.plan, answer })
    toast(w.text, w.sub)
    ask.value = null
    form.password = ''
    load()
  } catch (e) {
    const status = Number.isFinite(e?.status) ? e.status : 0
    // A 5xx's own sentence ("store unavailable") says more than "Server error".
    const w = failWords(d.kind, status, status >= 500 && e?.message ? e.message : describeApiError(e))
    toast(w.text, w.sub, 'red')
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <div class="qu">
    <!-- straight to a broker: there is no proxy to hold them -->
    <template v-if="direct">
      <h2 class="qu-h">Users and tenants on <span class="qn-mono">{{ where }}</span></h2>
      <p class="qn-t2 qu-calm">Users and tenants are managed by queen-proxy. This console talks to the broker directly, so there are none to show.</p>
    </template>
    <template v-else-if="!allowed">
      <h2 class="qu-h">Users and tenants</h2>
      <p class="qn-t2 qu-calm">These cover every tenant on the cell, so only its operators see them.</p>
    </template>

    <template v-else>
      <p class="qn-t3"><span class="qn-tag qu-cell">cell</span>Cell-level: every tenant on <span class="qn-mono">{{ cell }}</span>. Operators only.</p>
      <p v-if="failed" class="qu-note">The proxy did not answer: {{ failed }}<template v-if="data">. What you see was read before.</template></p>
      <p v-if="!data && !failed" class="qn-t2" style="padding: 16px 0">Reading the accounts…</p>

      <template v-else-if="data">
        <!-- users -->
        <div class="qu-headrow">
          <h2 id="qu-users" class="qu-h">Users</h2>
          <div style="flex: 1 1 0" />
          <button type="button" class="qn-btn" aria-haspopup="dialog" :disabled="!tenants.length" @click="openAdd">Add a user</button>
        </div>

        <div v-if="users.length" class="qu-tools">
          <label class="qn-field qu-find">
            <QcIcon name="search" :size="14" />
            <span class="qn-vh">Find a user by name, email or tenant</span>
            <input v-model="uq" type="text" placeholder="Name, email or tenant" spellcheck="false" autocomplete="off" />
          </label>
          <QcSeg v-model="ur" :options="roleOptions" label="Role on a cluster of this cell" />
          <QcPopover v-if="multiTenant" v-model:open="tenantOpen" side="bottom-start" :width="260" label="Which tenant">
            <template #trigger="{ toggle }">
              <button type="button" class="qn-chip" aria-haspopup="dialog" :aria-expanded="tenantOpen" @click="toggle">
                <span :class="{ 'qn-mono': ut }">{{ ut || 'All tenants' }}</span>
                <QcIcon name="chevron" :size="12" />
              </button>
            </template>
            <template #default="{ close: shut }">
              <button type="button" class="qn-opt" :class="{ on: !ut }" @click="ut = ''; shut()">All tenants</button>
              <div class="qn-popsep" />
              <button v-for="t in tenants" :key="t.id" type="button" class="qn-opt" :class="{ on: ut === t.slug }" @click="ut = t.slug; shut()">
                <span class="qn-mono qu-optname">{{ t.slug }}</span><span v-if="named(t)" class="qn-t3 qu-optname">{{ named(t) }}</span>
              </button>
            </template>
          </QcPopover>
        </div>

        <div class="qu-split">
          <div ref="listEl" class="qu-list">
            <p v-if="!users.length" class="qn-t2" style="padding: 12px 0">No accounts on this cell yet.</p>
            <p v-else-if="!rows.length" class="qn-t2" style="padding: 12px 0">
              No user matches<template v-if="uq.trim()"> “{{ uq.trim() }}”</template>.
              <button v-if="narrowed" type="button" class="qn-link" @click="showAll">Show every user</button>
            </p>
            <div v-else class="qu-scroll">
              <div role="table" aria-labelledby="qu-users" class="qu-table" :style="{ '--cols': cols }">
                <div class="qn-tr head" role="row">
                  <span role="columnheader">User</span>
                  <span v-if="multiTenant" role="columnheader">Tenant</span>
                  <span role="columnheader">Operator</span>
                  <span role="columnheader">Clusters and roles</span>
                  <span role="columnheader">Last sign-in</span>
                </div>
                <div
                  v-for="u in rows" :key="u.id" class="qn-tr click" :class="{ on: uid === u.id }" role="row" tabindex="0"
                  :aria-current="uid === u.id ? 'true' : undefined" @click="choose(u)" @keydown.enter.prevent="choose(u)" @keydown.space.prevent="choose(u)"
                >
                  <span role="cell" class="qu-who" :title="u.name ? `${u.name} · ${u.email}` : u.email">
                    <span class="qu-email">{{ u.email }}<span v-if="u.me" class="qn-t3"> (you)</span></span>
                    <span v-if="u.name" class="qn-t3 qu-uname">{{ u.name }}</span>
                  </span>
                  <span v-if="multiTenant" role="cell" class="qn-mono qn-t2">{{ u.tenant_slug }}</span>
                  <span role="cell" :class="u.is_operator ? '' : 'qn-t3'">{{ u.is_operator ? 'yes' : 'no' }}</span>
                  <span role="cell" :class="u.grants ? '' : 'qn-t3'" :title="u.grants">{{ u.grants || 'no access' }}</span>
                  <span role="cell" class="qn-num" :class="u.last_login_at ? 'qn-t2' : 'qn-t3'">{{ u.seen }}</span>
                </div>
              </div>
            </div>
            <p class="qn-read" style="margin-top: 12px">Read from <span class="qn-mono">GET /api/operator/users</span>, with the tenants below.</p>
          </div>

          <!-- the picked user -->
          <aside v-if="picked" ref="panel" class="qu-detail qn-panel" aria-labelledby="qu-who">
            <div class="qu-dhead">
              <div style="flex: 1 1 0; min-width: 0">
                <h3 id="qu-who" class="qu-dname">{{ picked.name || picked.email }}<span v-if="!picked.name && isMe(picked)" class="qn-t3" style="font-weight: 400"> (you)</span></h3>
                <p v-if="picked.name" class="qn-t2 qu-dmail">{{ picked.email }}<span v-if="isMe(picked)" class="qn-t3"> (you)</span></p>
              </div>
              <button type="button" class="qn-btn3" aria-label="Close the details" style="height: 28px" @click="uid = ''">✕</button>
            </div>
            <dl class="qn-dl" style="margin-top: 14px">
              <dt>Tenant</dt><dd><span class="qn-mono">{{ picked.tenant_slug }}</span><span v-if="named(pickedTenant)" class="qn-t3"> · {{ named(pickedTenant) }}</span></dd>
              <dt>Signs in with</dt><dd>{{ picked.has_local_password ? 'a password' : 'Google or GitHub' }}</dd>
              <dt>Operator</dt><dd>{{ picked.is_operator ? 'yes' : 'no' }}<span class="qn-t3"> · set on the proxy, not here</span></dd>
              <dt>Account made</dt><dd class="qn-num">{{ dayMonth(picked.created_at) }}</dd>
              <dt>Last sign-in</dt><dd class="qn-num" :class="{ 'qn-t3': !picked.last_login_at }">{{ picked.last_login_at ? when(picked.last_login_at) : 'never' }}</dd>
            </dl>
            <div style="margin-top: 14px"><button type="button" class="qn-btn2" aria-haspopup="dialog" @click="openRename(picked)">Rename…</button></div>

            <h4 id="qu-access" class="qn-h2" style="margin-top: 22px">Access on <span class="qn-mono">{{ cell }}</span></h4>
            <div v-if="access.length" role="table" aria-labelledby="qu-access" class="qu-access" style="--cols: minmax(0, 1fr) 112px 104px">
              <div v-for="r in access" :key="r.cluster.id" class="qn-tr" role="row">
                <span role="cell" :title="r.cluster.slug"><span class="qn-mono">{{ r.cluster.slug }}</span><span v-if="r.cluster.status && r.cluster.status !== 'active'" class="qn-t3"> · {{ r.cluster.status }}</span></span>
                <span role="cell">
                  <button type="button" class="qn-btn3 qu-role" :class="{ 'qn-t3': !r.role }" aria-haspopup="dialog" :aria-label="`Role on ${r.cluster.slug}: ${r.role || 'no access'}. Change it`" @click="openRole(picked, r.cluster)">
                    {{ r.role || 'no access' }}<QcIcon name="chevron" :size="12" />
                  </button>
                </span>
                <span role="cell" class="r">
                  <span v-if="r.role && onlyAdmin(users, picked, r.cluster.id)" class="qn-t3 qu-only" title="A cluster keeps at least one admin: make someone else its admin first.">the only admin</span>
                  <button v-else-if="r.role" type="button" class="qn-btn3 red qu-act" aria-haspopup="dialog" :aria-label="`Remove ${picked.email} from ${r.cluster.slug}`" @click="openRemove(picked, r.cluster)">Remove</button>
                </span>
              </div>
            </div>
            <p v-else class="qn-t2" style="margin-top: 8px">{{ picked.tenant_slug }} has no cluster on this cell.</p>
          </aside>
        </div>

        <!-- tenants -->
        <h2 id="qu-tenants" class="qu-h" style="margin-top: 40px">Tenants</h2>
        <div v-if="tenants.length" class="qu-scroll" style="margin-top: 14px">
          <div role="table" aria-labelledby="qu-tenants" class="qu-table" :style="{ '--cols': tenants.some(named) ? 'minmax(0, .8fr) minmax(0, 1fr) minmax(0, 1.4fr) 80px' : 'minmax(0, .8fr) minmax(0, 2.4fr) 80px' }">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Tenant</span><span v-if="tenants.some(named)" role="columnheader">Name</span><span role="columnheader">Clusters on this cell</span><span role="columnheader" class="r">Users</span>
            </div>
            <div v-for="t in tenants" :key="t.id" class="qn-tr" role="row">
              <span role="cell" class="qn-mono">{{ t.slug }}</span>
              <span v-if="tenants.some(named)" role="cell" :class="named(t) ? '' : 'qn-t3'">{{ named(t) || '–' }}</span>
              <span role="cell" class="qn-mono qn-t2" :title="clustersOf(t.id).map((c) => c.slug).join(', ')">{{ clustersOf(t.id).map((c) => c.slug).join(', ') || '–' }}</span>
              <span role="cell" class="r qn-num">{{ users.filter((u) => u.tenant_id === t.id).length.toLocaleString('en-US') }}</span>
            </div>
          </div>
        </div>
        <p v-else class="qn-t2" style="padding: 12px 0">No tenants on this cell.</p>
      </template>
    </template>

    <QcDialog
      :open="!!dlg" :title="dlg?.title || ''" :where="dlg?.where || ''" :call="dlg?.call || ''"
      :confirm-label="dlg?.confirm" :danger="!!dlg?.danger" :typed="dlg?.typed || ''"
      :busy="busy" :blocked="!!dlg?.blocked" @confirm="run" @cancel="close"
    >
      <!-- add a user -->
      <form v-if="dlg?.kind === 'add'" @submit.prevent="run">
        <div class="qu-pair">
          <label>
            <span class="qu-lab">Name</span>
            <span class="qn-field qu-in"><input v-model="form.name" type="text" maxlength="160" autocomplete="off" placeholder="Ada Lovelace" /></span>
          </label>
          <label>
            <span class="qu-lab">Email</span>
            <span class="qn-field qu-in"><input v-model="form.email" type="email" autocomplete="off" spellcheck="false" placeholder="name@example.com" /></span>
          </label>
        </div>
        <div v-if="multiTenant" class="qu-frow">
          <span class="qu-lab">Tenant</span>
          <QcSeg v-model="form.tenant" :options="tenantChoices" label="Tenant" />
        </div>
        <div class="qu-frow">
          <span class="qu-lab">Signs in with</span>
          <QcSeg v-model="form.provider" :options="PROVIDERS" label="Signs in with" />
          <template v-if="form.provider === 'local'">
            <label style="display: block; margin-top: 8px">
              <span class="qn-vh">Password to start with</span>
              <span class="qn-field qu-in"><input v-model="form.password" type="password" maxlength="128" autocomplete="new-password" placeholder="Password to start with" /></span>
            </label>
            <p class="qn-t3 qu-help">At least 12 characters.</p>
          </template>
          <p v-else class="qn-t3 qu-help">Linked at their first verified {{ providerLabel(form.provider) }} sign-in with this address.</p>
        </div>
        <div class="qu-frow">
          <span class="qu-lab">Access to start with</span>
          <div class="qu-start">
            <QcSeg v-model="form.role" :options="roleSeg" label="Role" />
            <span class="qn-t3">on</span>
            <QcSeg v-if="dlg.plan.clusters.length > 1" v-model="form.cluster" :options="dlg.plan.clusters.map((c) => ({ value: c.id, label: c.slug }))" label="Cluster" />
            <span v-else-if="dlg.plan.cluster" class="qn-mono qu-one">{{ dlg.plan.cluster.slug }}</span>
          </div>
          <p class="qn-t3 qu-help">{{ roleSentence(form.role) }}</p>
        </div>
        <p v-if="dlg.plan.say" class="red qu-say">{{ dlg.plan.say }}</p>
        <p v-else-if="dlg.plan.ok" class="qn-t2 qu-say">{{ dlg.plan.email }} joins the tenant {{ dlg.plan.tenant.slug }} as {{ roleWord(form.role) }} on {{ dlg.plan.cluster.slug }}.</p>
        <button type="submit" class="qn-vh" tabindex="-1">Add the user</button>
      </form>

      <!-- rename -->
      <form v-else-if="dlg?.kind === 'rename'" @submit.prevent="run">
        <label style="display: block">
          <span class="qu-lab">Name</span>
          <span class="qn-field qu-in"><input v-model="renameTo" type="text" maxlength="160" autocomplete="off" /></span>
        </label>
        <p v-if="dlg.plan.say" class="red qu-say">{{ dlg.plan.say }}</p>
        <div v-else-if="dlg.plan.ok" class="qu-change qn-num">
          <span class="qn-t2">Name</span>
          <span><span class="qn-t3" :style="dlg.plan.was ? 'text-decoration: line-through' : ''">{{ dlg.plan.was || 'none' }}</span> → {{ dlg.plan.name }}</span>
        </div>
        <p class="qn-t2" style="margin-top: 12px">Their email, how they sign in and their access stay as they are.</p>
        <button type="submit" class="qn-vh" tabindex="-1">Rename</button>
      </form>

      <!-- a role on one cluster -->
      <template v-else-if="dlg?.kind === 'role'">
        <fieldset class="qu-pick">
          <legend class="qn-vh">Role on {{ dlg.cluster.slug }}</legend>
          <button
            v-for="r in ROLES" :key="r.name" type="button" class="qn-opt qu-opt" :aria-pressed="roleTo === r.name"
            :disabled="dlg.plan.guard && r.name !== 'admin'" @click="roleTo = r.name"
          >
            <span style="font-weight: 500">{{ r.name }}<span v-if="r.name === dlg.plan.from" class="qn-t3" style="font-weight: 400"> · now</span></span>
            <span class="qn-t3" style="font-size: 12px">{{ roleCan(r.name) }}</span>
          </button>
        </fieldset>
        <p v-if="dlg.plan.guard" class="qn-t2" style="margin-top: 12px">{{ onlyAdminSay(dlg.user, dlg.cluster) }}</p>
        <div v-else-if="dlg.plan.ok" class="qu-change">
          <span class="qn-t2">Role on <span class="qn-mono">{{ dlg.cluster.slug }}</span></span>
          <span><span class="qn-t3">{{ dlg.plan.from || 'no access' }}</span> → {{ dlg.plan.to }}</span>
        </div>
        <p class="qn-t2" style="margin-top: 12px">{{ dlg.user.email }} keeps their access to other clusters as it is.</p>
      </template>

      <!-- take access away -->
      <template v-else-if="dlg?.kind === 'remove'">
        <div class="qu-change" style="margin-top: 0">
          <span class="qn-t2">Role on <span class="qn-mono">{{ dlg.cluster.slug }}</span></span>
          <span><span class="qn-t3">{{ dlg.plan.from || 'none' }}</span> → no access</span>
        </div>
        <p v-if="dlg.plan.say" class="red qu-say">{{ dlg.plan.say }}</p>
        <p v-else class="qn-t2" style="margin-top: 12px">Their next request to {{ dlg.cluster.slug }} is refused. Their account, their name and their access to other clusters stay.</p>
      </template>
    </QcDialog>
  </div>
</template>

<style scoped>
.qu { min-width: 0; }
.qu-h { font-size: 17px; font-weight: 500; }
.qu-calm { margin-top: 10px; font-size: 14px; max-width: 64ch; }
.qu-cell { font-size: 11px; padding: 0 6px; margin-right: 10px; color: var(--text3); }
.qu-note { margin-top: 12px; padding: 10px 12px; border-radius: var(--r-ctl); background: var(--bg2); color: var(--text2); }
.qu-headrow { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; margin-top: 20px; }
.qu-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 14px; margin-top: 14px; }
.qu-find { width: 260px; max-width: 100%; }
.qu-optname { overflow: hidden; text-overflow: ellipsis; }

/* The list and, once one is picked, the user beside it (under it when narrow). */
.qu-split { display: flex; flex-wrap: wrap; align-items: flex-start; gap: 20px 24px; margin-top: 12px; }
.qu-list { flex: 999 1 520px; min-width: 0; }
.qu-scroll { overflow-x: auto; }
.qu-table { min-width: 520px; }
/* The email, then the name if it fits whole: a name that does not fit wraps to
   a second line, which the cell's one-line height hides. Never "A…". */
.qn-tr > .qu-who { display: flex; flex-wrap: wrap; align-items: baseline; column-gap: 10px; height: 1.45em; }
.qu-email { flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.qu-uname { flex: 0 0 auto; white-space: nowrap; }
.qu-detail { flex: 1 1 340px; min-width: 0; max-width: 420px; padding: 16px 18px; }
.qu-dhead { display: flex; align-items: flex-start; gap: 10px; }
.qu-dname { font-size: 15px; font-weight: 500; overflow-wrap: anywhere; }
.qu-dmail { margin-top: 2px; overflow-wrap: anywhere; }
.qu-access { margin-top: 6px; }
.qu-role { height: 28px; margin-left: -8px; padding: 0 8px; gap: 6px; }
.qu-role:not(.qn-t3) { color: var(--text); }
.qu-act { height: 28px; }
.qu-only { font-size: 12px; }

/* The dialogs: a label over each field, one short line under it at most. */
.qu-lab { display: block; font-size: 12px; color: var(--text3); margin-bottom: 6px; }
.qu-pair { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.2fr); gap: 10px; }
.qu-frow { margin-top: 16px; }
.qu-help { font-size: 12px; margin-top: 6px; }
.qu-start { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; }
.qu-one { font-size: 12.5px; }
.qu-say { font-size: 12.5px; margin-top: 16px; }
.qu-change { display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 6px 16px; margin-top: 14px; padding: 10px 12px; border-radius: var(--r-ctl); background: var(--bg2); overflow-wrap: anywhere; }
.qu-pick { border: 0; margin: 0; padding: 0; min-width: 0; }
.qu-opt { flex-direction: column; align-items: flex-start; justify-content: center; gap: 1px; min-height: 44px; padding: 6px 10px; white-space: normal; }

@media (max-width: 640px) {
  .qu-find { width: 100%; }
}
@media (max-width: 480px) {
  .qu-pair { grid-template-columns: minmax(0, 1fr); }
}
</style>
