<script setup>
// Settings › Access: who may use the cluster (its members and their roles) and
// the API keys apps call it with. The proxy owns both (proxy/src/console.rs,
// every route behind require_admin), so straight to a broker there are none,
// and the page says so in one sentence.
//
// Reads, every 30 s: GET /api/console/members, GET /api/console/keys.
// Every change first shows what changes and its call (QcDialog); removing a
// member or revoking a key asks for the name.
import { computed, reactive, ref, watch } from 'vue'
import { access } from '@/api'
import { describeApiError } from '@/api/errors'
import { ROLES, SCOPES, inScopeOrder } from '@/composables/accessGroups'
import { useAutoRefresh } from '@/composables/useRefresh'
import { currentEpoch, useIdentity } from '@/stores/identity'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { span } from '../useOverview'
import { dayMonth as day } from '../useSettingsPage'

const props = defineProps({ where: { type: String, default: '' } })
const id = useIdentity()
const direct = computed(() => id.standalone.value)
const allowed = computed(() => id.can('clusterAdmin'))

const members = ref(null)
const keys = ref(null)
const failed = ref(null)
async function load() {
  if (direct.value || !allowed.value) return
  const epoch = currentEpoch()
  const [m, k] = await Promise.allSettled([access.listMembers({ quiet: true }), access.listKeys({ quiet: true })])
  if (epoch !== currentEpoch()) return
  if (m.status === 'fulfilled') members.value = Array.isArray(m.value?.data) ? m.value.data : []
  if (k.status === 'fulfilled') keys.value = Array.isArray(k.value?.data) ? k.value.data : []
  const bad = [m, k].find((x) => x.status === 'rejected')
  failed.value = bad ? describeApiError(bad.reason).replace(/[.\s]+$/, '') : null
}
useAutoRefresh(load)
load()
watch(id.epoch, () => { members.value = null; keys.value = null; reveal.value = null; load() })

// ---- words ----
const WHAT = { admin: 'an admin', producer: 'a producer', consumer: 'a consumer', viewer: 'a viewer' }
const THEN = {
  admin: 'can then do everything here, including members and API keys',
  producer: 'can then push messages and read',
  consumer: 'can then pop, acknowledge and read',
  viewer: 'can then only read',
}
const canOf = (role) => (ROLES.find((r) => r.name === role)?.can || '').replace(/^./, (c) => c.toLowerCase())
const shortName = (email) => String(email || '').split('@')[0]
const isMe = (m) => !!id.email.value && String(m.email).toLowerCase() === id.email.value.toLowerCase()
function ago(v) {
  const t = v ? Date.parse(v) : NaN
  if (!Number.isFinite(t)) return 'never'
  const s = (Date.now() - t) / 1000
  if (s < 60) return 'just now'
  if (s < 7 * 86400) return `${span(s)} ago`
  return day(v)
}

const memberRows = computed(() => [...(members.value || [])]
  .sort((a, b) => String(a.email).localeCompare(String(b.email)))
  .map((m) => ({ ...m, me: isMe(m) })))
const keyRows = computed(() => [...(keys.value || [])]
  .sort((a, b) => (!!a.revoked_at - !!b.revoked_at) || String(b.created_at).localeCompare(String(a.created_at)))
  .map((k) => ({ ...k, can: inScopeOrder(k.scopes).join(', '), used: k.last_used_at ? ago(k.last_used_at) : 'never' })))

// ---- one dialog at a time ----
const ask = ref(null) // { kind: 'add' | 'role' | 'remove' | 'key' | 'revoke', ... }
const busy = ref(false)
const form = reactive({ email: '', role: 'viewer', name: '', scopes: ['read'] })
const reveal = ref(null) // { name, key }: a new key's secret, shown this once

function openAdd() { form.email = ''; form.role = 'viewer'; ask.value = { kind: 'add' } }
function openRole(m) { form.role = m.role; ask.value = { kind: 'role', m } }
function openRemove(m) { ask.value = { kind: 'remove', m } }
function openKey() { form.name = ''; form.scopes = ['read']; ask.value = { kind: 'key' } }
function openRevoke(k) { ask.value = { kind: 'revoke', k } }
function toggleScope(s, on) {
  const rest = form.scopes.filter((x) => x !== s)
  form.scopes = on ? inScopeOrder([...rest, s]) : rest
}

const emailOk = (s) => /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(s)
const dialog = computed(() => {
  const a = ask.value
  if (!a) return null
  if (a.kind === 'add') {
    const email = form.email.trim().toLowerCase()
    const dup = email && (members.value || []).some((m) => String(m.email).toLowerCase() === email)
    return {
      title: 'Add a member', confirm: `Add as ${form.role}`,
      call: `POST /api/console/members ${JSON.stringify({ email: email || '…', role: form.role })}`,
      blocked: !emailOk(email) || dup,
      hint: !email ? '' : !emailOk(email) ? 'Write the whole address, like name@example.com.' : dup ? `${email} is already a member: change their role in the list.` : '',
    }
  }
  if (a.kind === 'role') {
    return {
      title: `Change the role of ${shortName(a.m.email)}?`,
      confirm: form.role === a.m.role ? 'Change the role' : `Make ${shortName(a.m.email)} ${WHAT[form.role] || form.role}`,
      call: `POST /api/console/members ${JSON.stringify({ email: a.m.email, role: form.role })}`,
      blocked: form.role === a.m.role,
    }
  }
  if (a.kind === 'remove') {
    return {
      title: `Remove ${a.m.email}?`, confirm: 'Remove', danger: true, typed: a.m.email,
      call: `DELETE /api/console/members ${JSON.stringify({ email: a.m.email })}`,
    }
  }
  if (a.kind === 'key') {
    const name = form.name.trim()
    const dup = name && (keys.value || []).some((k) => !k.revoked_at && k.name === name)
    return {
      title: 'New API key', confirm: 'Create the key',
      call: `POST /api/console/keys ${JSON.stringify({ name: name || '…', scopes: form.scopes })}`,
      blocked: !name || !form.scopes.length || dup,
      hint: dup ? `A key named ${name} is already active.` : !form.scopes.length ? 'Pick at least one thing it can do.' : '',
    }
  }
  return {
    title: `Revoke ${a.k.name}?`, confirm: 'Revoke', danger: true, typed: a.k.name,
    call: `DELETE /api/console/keys/${encodeURIComponent(a.k.id)}`,
  }
})

async function run() {
  const a = ask.value
  if (!a || busy.value || dialog.value?.blocked) return
  busy.value = true
  try {
    if (a.kind === 'add') {
      const email = form.email.trim().toLowerCase()
      const r = await access.grantMember(email, form.role, { quiet: true })
      toast(`${email} is ${WHAT[form.role]} on ${props.where}`, `The proxy answered ${r?.status || 200}.`)
    } else if (a.kind === 'role') {
      const r = await access.grantMember(a.m.email, form.role, { quiet: true })
      toast(`${shortName(a.m.email)} is now ${WHAT[form.role]} on ${props.where}`, `The proxy answered ${r?.status || 200}; nothing else changed.`)
    } else if (a.kind === 'remove') {
      const r = await access.revokeMember(a.m.email, { quiet: true })
      toast(`${a.m.email} removed from ${props.where}`, `The proxy answered ${r?.status || 200}. Their next request is refused.`)
    } else if (a.kind === 'key') {
      const name = form.name.trim()
      const r = await access.createKey({ name, scopes: [...form.scopes] }, { quiet: true })
      reveal.value = { name, key: r?.data?.key || '', id: r?.data?.id || '' }
      toast(`${name} is made on ${props.where}`, `The proxy answered ${r?.status || 200}. Copy its secret now: it shows once.`)
    } else {
      const r = await access.revokeKey(a.k.id, { quiet: true })
      toast(`Revoked ${a.k.name} on ${props.where}`, `The proxy answered ${r?.status || 200}. Requests with it now get 401.`)
    }
    ask.value = null
    await load()
  } catch (e) {
    toast(a.kind === 'key' ? 'Not made' : a.kind === 'revoke' ? 'Not revoked' : 'Not changed', describeApiError(e), 'red')
  } finally {
    busy.value = false
  }
}

const copied = ref(false)
async function copy() {
  try { await navigator.clipboard.writeText(reveal.value?.key || ''); copied.value = true; setTimeout(() => { copied.value = false }, 1500) } catch { /* no clipboard: the secret stays on screen to select */ }
}
const origin = typeof window !== 'undefined' ? window.location.origin : ''
</script>

<template>
  <div class="qac">
    <!-- straight to a broker: there is no proxy to hold them -->
    <template v-if="direct">
      <h2 class="qac-h">Who may use <span class="qn-mono">{{ where }}</span></h2>
      <p class="qn-t2 qac-calm">Members and API keys are managed by queen-proxy. This console talks to the broker directly, so there are none to show.</p>
    </template>

    <template v-else-if="!allowed">
      <h2 class="qac-h">Who may use <span class="qn-mono">{{ where }}</span></h2>
      <p class="qn-t2 qac-calm">Members and API keys are shown to the admins of {{ where }}.</p>
    </template>

    <template v-else>
      <!-- members -->
      <div class="qac-headrow">
        <h2 id="qac-members" class="qac-h">Who may use <span class="qn-mono">{{ where }}</span></h2>
        <div style="flex: 1 1 0" />
        <button type="button" class="qn-btn" aria-haspopup="dialog" @click="openAdd">Add a member</button>
      </div>
      <p v-if="failed" class="qac-note">The proxy did not answer: {{ failed }}<template v-if="members || keys">. What you see was read before.</template></p>
      <p v-if="members === null && !failed" class="qn-t2" style="padding: 16px 0">Reading the members…</p>
      <template v-else-if="members">
        <div v-if="memberRows.length" class="qac-scroll">
          <div role="table" aria-labelledby="qac-members" class="qac-table" style="--cols: minmax(0, 1.6fr) 150px 110px 90px">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Member</span><span role="columnheader">Role</span><span role="columnheader">Since</span><span role="columnheader"><span class="qn-vh">Remove</span></span>
            </div>
            <div v-for="m in memberRows" :key="m.email" class="qn-tr" role="row">
              <span role="cell" :title="m.email">{{ m.email }}<span v-if="m.me" class="qn-t3"> (you)</span></span>
              <span role="cell">
                <button v-if="!m.me" type="button" class="qn-btn3 qac-role" aria-haspopup="dialog" :aria-label="`Role of ${m.email}: ${m.role}. Change it`" @click="openRole(m)">
                  {{ m.role }}
                  <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><polyline points="2,3.5 5,6.5 8,3.5" style="fill: none; stroke: var(--text3); stroke-width: 1.3" /></svg>
                </button>
                <span v-else :title="'Another admin can change your role.'">{{ m.role }}</span>
              </span>
              <span role="cell" class="qn-num qn-t2">{{ day(m.granted_at) }}</span>
              <span role="cell" class="r"><button v-if="!m.me" type="button" class="qn-btn3 qac-act" aria-haspopup="dialog" :aria-label="`Remove ${m.email}`" @click="openRemove(m)">Remove</button></span>
            </div>
          </div>
        </div>
        <p v-else class="qn-t2" style="padding: 12px 0">Nobody is a member of {{ where }} yet.</p>
        <dl class="qac-roles">
          <div v-for="r in ROLES" :key="r.name"><dt>{{ r.name }}</dt><dd class="qn-t3">{{ canOf(r.name) }}</dd></div>
        </dl>
      </template>

      <!-- API keys -->
      <div class="qac-headrow" style="margin-top: 44px">
        <h2 id="qac-keys" class="qac-h">API keys</h2>
        <div style="flex: 1 1 0" />
        <button type="button" class="qn-btn2" aria-haspopup="dialog" @click="openKey">New API key</button>
      </div>

      <section v-if="reveal" class="qn-panel qac-reveal" aria-live="polite">
        <p style="font-weight: 500">The secret of <span class="qn-mono">{{ reveal.name }}</span></p>
        <div class="qac-secret-row">
          <code class="qn-mono qac-secret">{{ reveal.key }}</code>
          <button type="button" class="qn-btn2" @click="copy">{{ copied ? 'Copied' : 'Copy' }}</button>
        </div>
        <p class="qn-t2" style="margin-top: 10px">This is the only time it is shown: the proxy keeps only its hash. Apps send it as a bearer token.</p>
        <p class="qn-call" style="margin-top: 8px">curl -H "Authorization: Bearer {{ reveal.key.slice(0, 10) }}…" {{ origin }}/api/v1/resources/queues</p>
        <div style="display: flex; justify-content: flex-end; margin-top: 12px"><button type="button" class="qn-btn" @click="reveal = null">I have stored it</button></div>
      </section>

      <p v-if="keys === null && !failed" class="qn-t2" style="padding: 16px 0">Reading the keys…</p>
      <template v-else-if="keys">
        <div v-if="keyRows.length" class="qac-scroll">
          <div role="table" aria-labelledby="qac-keys" class="qac-table" style="--cols: minmax(0, 1.2fr) minmax(0, 1fr) 100px 110px 120px">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Name</span><span role="columnheader">Can</span><span role="columnheader">Made</span><span role="columnheader">Last used</span><span role="columnheader" class="r">State</span>
            </div>
            <div v-for="k in keyRows" :key="k.id" class="qn-tr" :class="{ 'qac-gone': k.revoked_at }" role="row">
              <span role="cell" class="qn-mono" :title="k.name">{{ k.name }}</span>
              <span role="cell">{{ k.can }}</span>
              <span role="cell" class="qn-num qn-t2">{{ day(k.created_at) }}</span>
              <span role="cell" class="qn-num" :class="k.last_used_at ? 'qn-t2' : 'qn-t3'">{{ k.used }}</span>
              <span role="cell" class="r">
                <span v-if="k.revoked_at" class="qn-t3">revoked {{ day(k.revoked_at) }}</span>
                <button v-else type="button" class="qn-btn3 qac-act red" aria-haspopup="dialog" :aria-label="`Revoke ${k.name}`" @click="openRevoke(k)">Revoke</button>
              </span>
            </div>
          </div>
        </div>
        <p v-else class="qn-t2" style="padding: 12px 0">No API keys. An app needs one to reach <span class="qn-mono">{{ where }}</span> from outside.</p>
      </template>
      <p class="qn-read" style="margin-top: 12px">A key’s secret shows once, when the key is made. Read from <span class="qn-mono">GET /api/console/members</span> and <span class="qn-mono">GET /api/console/keys</span>.</p>
    </template>

    <QcDialog
      :open="!!dialog" :title="dialog?.title || ''" :where="where" :call="dialog?.call || ''"
      :confirm-label="dialog?.confirm" :danger="!!dialog?.danger" :typed="dialog?.typed || ''"
      :busy="busy" :blocked="!!dialog?.blocked" @confirm="run" @cancel="ask = null"
    >
      <template v-if="ask?.kind === 'add'">
        <form @submit.prevent="run">
          <label style="display: block">
            <span class="qn-t3" style="font-size: 12px">Email</span>
            <span class="qn-field" style="margin-top: 6px"><input v-model="form.email" type="email" autocomplete="off" spellcheck="false" placeholder="name@example.com" /></span>
          </label>
          <p class="qac-hint">{{ dialog.hint }}</p>
          <fieldset class="qac-pick">
            <legend class="qn-t3" style="font-size: 12px">Role</legend>
            <button v-for="r in ROLES" :key="r.name" type="button" class="qn-opt qac-opt" :aria-pressed="form.role === r.name" @click="form.role = r.name">
              <span style="font-weight: 500">{{ r.name }}</span><span class="qn-t3" style="font-size: 12px">{{ canOf(r.name) }}</span>
            </button>
          </fieldset>
          <p class="qn-t2" style="margin-top: 12px">They must have signed in to queen-proxy once: the first sign-in makes the account.</p>
          <button type="submit" class="qn-vh" tabindex="-1">Add</button>
        </form>
      </template>
      <template v-else-if="ask?.kind === 'role'">
        <fieldset class="qac-pick">
          <legend class="qn-vh">Role</legend>
          <button v-for="r in ROLES" :key="r.name" type="button" class="qn-opt qac-opt" :aria-pressed="form.role === r.name" @click="form.role = r.name">
            <span style="font-weight: 500">{{ r.name }}<span v-if="r.name === ask.m.role" class="qn-t3" style="font-weight: 400"> · now</span></span><span class="qn-t3" style="font-size: 12px">{{ canOf(r.name) }}</span>
          </button>
        </fieldset>
        <p v-if="form.role !== ask.m.role" class="qn-t2" style="margin-top: 12px">
          {{ shortName(ask.m.email) }} {{ THEN[form.role] || '' }}<template v-if="ask.m.role === 'admin'">, and no longer change members, keys or settings</template>. Their roles on other clusters stay as they are.
        </p>
      </template>
      <template v-else-if="ask?.kind === 'remove'">
        <p class="qn-t2">{{ ask.m.email }} is {{ WHAT[ask.m.role] || ask.m.role }} here. Their next request to {{ where }} is refused; their account and their roles on other clusters stay.</p>
      </template>
      <template v-else-if="ask?.kind === 'key'">
        <form @submit.prevent="run">
          <label style="display: block">
            <span class="qn-t3" style="font-size: 12px">Name</span>
            <span class="qn-field" style="margin-top: 6px"><input v-model="form.name" class="qn-mono" type="text" maxlength="120" autocomplete="off" spellcheck="false" placeholder="billing-worker" /></span>
          </label>
          <fieldset class="qac-pick">
            <legend class="qn-t3" style="font-size: 12px">What it can do</legend>
            <label v-for="s in SCOPES" :key="s.name" class="qac-check">
              <input type="checkbox" :checked="form.scopes.includes(s.name)" @change="toggleScope(s.name, $event.target.checked)" />
              <span class="qn-mono" style="width: 64px">{{ s.name }}</span><span class="qn-t3" style="font-size: 12px">{{ s.can }}</span>
            </label>
          </fieldset>
          <p class="qac-hint">{{ dialog.hint }}</p>
          <p class="qn-t2">The secret shows once, on this page, when the proxy answers.</p>
          <button type="submit" class="qn-vh" tabindex="-1">Create</button>
        </form>
      </template>
      <template v-else-if="ask?.kind === 'revoke'">
        <p class="qn-t2">It can {{ inScopeOrder(ask.k.scopes).join(', ') }}, and was {{ ask.k.last_used_at ? `last used ${ago(ask.k.last_used_at)}` : 'never used' }}. Apps that send it get 401 from their next request. A revoked key cannot come back; make a new one instead.</p>
      </template>
    </QcDialog>
  </div>
</template>

<style scoped>
.qac { min-width: 0; }
.qac-h { font-size: 17px; font-weight: 500; }
.qac-calm { margin-top: 10px; font-size: 14px; max-width: 64ch; }
.qac-headrow { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.qac-note { margin-top: 12px; padding: 10px 12px; border-radius: var(--r-ctl); background: var(--bg2); color: var(--text2); }
.qac-scroll { overflow-x: auto; margin-top: 16px; }
.qac-table { min-width: 560px; }
.qac-role { height: 28px; margin-left: -8px; padding: 0 8px; color: var(--text); gap: 6px; }
.qac-act { height: 28px; }
.qac-gone > * { color: var(--text3); }
.qac-roles { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px 24px; margin: 14px 0 0; font-size: 12px; }
.qac-roles dt { font-weight: 500; }
.qac-roles dd { margin: 2px 0 0; }
.qac-reveal { margin-top: 16px; }
.qac-secret-row { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 10px; }
.qac-secret { flex: 1 1 260px; min-width: 0; padding: 8px 12px; border-radius: var(--r-ctl); background: var(--bg2); font-size: 13px; overflow-wrap: anywhere; user-select: all; }
.qac-hint { min-height: 18px; margin: 6px 0 4px; font-size: 12px; color: var(--text2); }
.qac-pick { border: 0; margin: 10px 0 0; padding: 0; min-width: 0; }
.qac-pick legend { padding: 0; margin-bottom: 6px; }
.qac-opt { flex-direction: column; align-items: flex-start; justify-content: center; gap: 1px; min-height: 44px; padding: 6px 10px; white-space: normal; }
.qac-check { display: flex; align-items: center; gap: 10px; min-height: 30px; }
.qac-check input { accent-color: var(--text); width: 15px; height: 15px; margin: 0; }
</style>
