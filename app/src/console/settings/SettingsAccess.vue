<script setup>
// Settings › Access: who may use the cluster (its members and their roles),
// the API keys apps call it with, and the clients a scoped key refuses. The
// proxy owns all three (proxy/src/console.rs, every route behind
// require_admin), so straight to a broker there are none, and the page says so
// in one sentence.
//
// A key reaches the whole cluster (its scopes) or only what its grants name
// (a scoped key: some queues, partitions, KV keys or timers). A scoped key's
// usual job is minting client tokens, one per device or user
// (POST /api/v1/tokens); blocking one client refuses that client's tokens.
//
// Reads, every 30 s: GET /api/console/members, /keys and /blocks.
// Every change first shows what changes and its call (QcDialog); removing a
// member, revoking a key or rotating it with no grace asks for the name.
import { computed, reactive, ref, watch } from 'vue'
import { access } from '@/api'
import { describeApiError } from '@/api/errors'
import { ROLES, SCOPES, inScopeOrder } from '@/composables/accessGroups'
import {
  BLOCK_REASON_MAX, DEFAULT_TOKEN_TTL_MAX_S, ROTATE_GRACES, blankRow, blockRows, grantOf, grantProblem, grantText,
  grantsSummary, lengthOf, limitsOf, rowOf, rpsProblem, validSub,
} from '@/composables/scopedKeys'
import { useAutoRefresh } from '@/composables/useRefresh'
import { currentEpoch, useIdentity } from '@/stores/identity'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { span } from '../useOverview'
import { dayMonth as day } from '../useSettingsPage'
import GrantsForm from './GrantsForm.vue'

const props = defineProps({ where: { type: String, default: '' } })
const id = useIdentity()
const direct = computed(() => id.standalone.value)
const allowed = computed(() => id.can('clusterAdmin'))

const members = ref(null)
const keys = ref(null)
const blocks = ref(null) // null until read, and on a proxy older than the block list
const failed = ref(null)
async function load() {
  if (direct.value || !allowed.value) return
  const epoch = currentEpoch()
  const [m, k, b] = await Promise.allSettled([
    access.listMembers({ quiet: true }), access.listKeys({ quiet: true }), access.listBlocks({ quiet: true }),
  ])
  if (epoch !== currentEpoch()) return
  if (m.status === 'fulfilled') members.value = Array.isArray(m.value?.data) ? m.value.data : []
  if (k.status === 'fulfilled') keys.value = Array.isArray(k.value?.data) ? k.value.data : []
  if (b.status === 'fulfilled') blocks.value = Array.isArray(b.value?.data) ? b.value.data : []
  // A proxy without the block list answers it 404: nothing to show, not a fault.
  const bad = [m, k, b].find((x) => x.status === 'rejected' && !(x === b && x.reason?.status === 404))
  failed.value = bad ? describeApiError(bad.reason).replace(/[.\s]+$/, '') : null
}
useAutoRefresh(load)
load()
watch(id.epoch, () => { members.value = null; keys.value = null; blocks.value = null; reveal.value = null; load() })

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
/** What a key can do, in one line: its scopes, or its grants. */
const keyCan = (k) => (k?.scoped ? grantsSummary(k.grants) : inScopeOrder(k?.scopes).join(', '))
const keyRows = computed(() => [...(keys.value || [])]
  .sort((a, b) => (!!a.revoked_at - !!b.revoked_at) || String(b.created_at).localeCompare(String(a.created_at)))
  .map((k) => ({
    ...k,
    can: keyCan(k),
    canAll: k.scoped ? (k.grants || []).map(grantText).join('\n') : '',
    used: k.last_used_at ? ago(k.last_used_at) : 'never',
  })))
const scopedLive = computed(() => keyRows.value.filter((k) => k.scoped && !k.revoked_at))
const blockList = computed(() => blockRows(blocks.value, keys.value))
// The section shows once there is something to block with, or something blocked.
const showBlocks = computed(() => blocks.value !== null && (scopedLive.value.length > 0 || blockList.value.length > 0))

// ---- one dialog at a time ----
const ask = ref(null) // { kind: 'add' | 'role' | 'remove' | 'key' | 'revoke' | 'edit' | 'rotate' | 'block' | 'unblock', ... }
const busy = ref(false)
const form = reactive({
  email: '', role: 'viewer', name: '', scopes: ['read'],
  reach: 'cluster', policy: null, // a new key: the whole cluster, or its grants (GrantsForm)
  grace: 3600, // a rotation: how long the old secret keeps working
  blockKey: '', sub: '', reason: '',
})
const reveal = ref(null) // { name, key, id, mints?, rotated?, until? }: a secret, shown this once

// A scoped key's grants, limits and ceiling as GrantsForm edits them.
let rowId = 1
const policyForm = (k) => ({
  rows: k?.grants?.length ? k.grants.map((g) => ({ ...rowOf(g), id: rowId++ })) : [{ ...blankRow(), id: rowId++ }],
  keyRps: k?.limits?.key_rps != null ? String(k.limits.key_rps) : '',
  subRps: k?.limits?.sub_rps != null ? String(k.limits.sub_rps) : '',
  ttl: Number(k?.token_ttl_max_s) || DEFAULT_TOKEN_TTL_MAX_S,
})
// The body the proxy takes. A burst set through the API stays (limitsOf); the
// default ceiling is left out, which the proxy reads as the same day.
function policyBody(p, before) {
  const body = { grants: p.rows.map(grantOf) }
  const limits = limitsOf(before?.limits, p.keyRps, p.subRps)
  if (Object.keys(limits).length) body.limits = limits
  if (p.ttl !== DEFAULT_TOKEN_TTL_MAX_S) body.token_ttl_max_s = p.ttl
  return body
}
function policyProblem(p) {
  if (!p.rows.length) return 'A scoped key needs at least one grant.'
  for (const r of p.rows) {
    const e = grantProblem(grantOf(r))
    if (e) return e
  }
  return rpsProblem(p.keyRps) || rpsProblem(p.subRps)
}

function openAdd() { form.email = ''; form.role = 'viewer'; ask.value = { kind: 'add' } }
function openRole(m) { form.role = m.role; ask.value = { kind: 'role', m } }
function openRemove(m) { ask.value = { kind: 'remove', m } }
function openKey() { form.name = ''; form.scopes = ['read']; form.reach = 'cluster'; form.policy = policyForm(); ask.value = { kind: 'key' } }
function openRevoke(k) { ask.value = { kind: 'revoke', k } }
function openEdit(k) {
  form.policy = policyForm(k)
  ask.value = { kind: 'edit', k, before: JSON.stringify(policyBody(policyForm(k), k)) }
}
function openRotate(k) { form.grace = 3600; ask.value = { kind: 'rotate', k } }
function openBlock() {
  form.blockKey = scopedLive.value.length === 1 ? scopedLive.value[0].id : ''
  form.sub = ''; form.reason = ''
  ask.value = { kind: 'block' }
}
function openUnblock(b) { ask.value = { kind: 'unblock', b } }
function toggleScope(s, on) {
  const rest = form.scopes.filter((x) => x !== s)
  form.scopes = on ? inScopeOrder([...rest, s]) : rest
}
const keyPath = (keyId) => `/api/console/keys/${encodeURIComponent(keyId)}`

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
    if (form.reach === 'scoped') {
      return {
        title: 'New API key', confirm: 'Create the key',
        call: `POST /api/console/keys ${JSON.stringify({ name: name || '…', ...policyBody(form.policy) })}`,
        blocked: !name || dup || !!policyProblem(form.policy),
        hint: dup ? `A key named ${name} is already active.` : '',
      }
    }
    return {
      title: 'New API key', confirm: 'Create the key',
      call: `POST /api/console/keys ${JSON.stringify({ name: name || '…', scopes: form.scopes })}`,
      blocked: !name || !form.scopes.length || dup,
      hint: dup ? `A key named ${name} is already active.` : !form.scopes.length ? 'Pick at least one thing it can do.' : '',
    }
  }
  if (a.kind === 'edit') {
    const body = policyBody(form.policy, a.k)
    return {
      title: `What ${a.k.name} reaches`, confirm: 'Save',
      call: `PUT ${keyPath(a.k.id)}/policy ${JSON.stringify(body)}`,
      blocked: JSON.stringify(body) === a.before || !!policyProblem(form.policy),
    }
  }
  if (a.kind === 'rotate') {
    const now = form.grace === 0
    return {
      title: `Rotate ${a.k.name}?`, confirm: now ? 'Rotate now' : 'Rotate', danger: now, typed: now ? a.k.name : '',
      call: `POST ${keyPath(a.k.id)}/rotate ${JSON.stringify({ grace_seconds: form.grace })}`,
    }
  }
  if (a.kind === 'block') {
    const k = scopedLive.value.find((x) => x.id === form.blockKey)
    const sub = form.sub.trim()
    const reason = form.reason.trim()
    const again = k && sub && (blocks.value || []).some((b) => b.key === k.id && b.sub === sub)
    return {
      title: 'Block a client', confirm: validSub(sub) ? `Block ${sub}` : 'Block', danger: true,
      call: `POST ${k ? keyPath(k.id) : '/api/console/keys/…'}/blocks ${JSON.stringify(reason ? { sub: sub || '…', reason } : { sub: sub || '…' })}`,
      blocked: !k || !validSub(sub) || [...reason].length > BLOCK_REASON_MAX,
      hint: sub && !validSub(sub) ? 'A client is 1 to 128 letters, digits and . _ - @ +, as its token names it.'
        : again ? `${sub} is already blocked: blocking it again keeps one row, with this reason.` : '',
    }
  }
  if (a.kind === 'unblock') {
    return {
      title: `Unblock ${a.b.sub}?`, confirm: 'Unblock',
      call: `DELETE ${keyPath(a.b.key)}/blocks/${encodeURIComponent(a.b.sub)}`,
    }
  }
  return {
    title: `Revoke ${a.k.name}?`, confirm: 'Revoke', danger: true, typed: a.k.name,
    call: `DELETE ${keyPath(a.k.id)}`,
  }
})

const FAILED = { key: 'Not made', revoke: 'Not revoked', edit: 'Not saved', rotate: 'Not rotated', block: 'Not blocked', unblock: 'Not unblocked' }
/** "until 14:05", "until 12 Nov, 14:05": when a rotation's grace ends. */
function until(ms) {
  const d = new Date(ms)
  const hm = d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })
  return ms - Date.now() < 20 * 3600 * 1000 ? hm : `${day(d.toISOString())}, ${hm}`
}

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
      const scoped = form.reach === 'scoped'
      const body = scoped ? { name, ...policyBody(form.policy) } : { name, scopes: [...form.scopes] }
      const r = await access.createKey(body, { quiet: true })
      // A scoped key cannot list queues: its example is the mint call.
      reveal.value = { name, key: r?.data?.key || '', id: r?.data?.id || '', mints: scoped }
      toast(`${name} is made on ${props.where}`, `The proxy answered ${r?.status || 200}. Copy its secret now: it shows once.`)
    } else if (a.kind === 'edit') {
      const r = await access.setKeyPolicy(a.k.id, policyBody(form.policy, a.k), { quiet: true })
      toast(`${a.k.name} reaches what its grants now say`, `The proxy answered ${r?.status || 200}. The tokens it minted follow within about a second.`)
    } else if (a.kind === 'rotate') {
      const grace = form.grace
      const r = await access.rotateKey(a.k.id, grace, { quiet: true })
      reveal.value = { name: a.k.name, key: r?.data?.key || '', id: a.k.id, mints: !!a.k.scoped, rotated: true, until: grace ? Date.now() + grace * 1000 : 0 }
      toast(`${a.k.name} has a new secret`, `The proxy answered ${r?.status || 200}. ${grace ? `The old one works until ${until(Date.now() + grace * 1000)}.` : 'The old one no longer works.'}`)
    } else if (a.kind === 'block') {
      const k = scopedLive.value.find((x) => x.id === form.blockKey)
      const sub = form.sub.trim()
      const r = await access.blockClient(k.id, sub, form.reason.trim(), { quiet: true })
      toast(`${sub} is blocked on ${props.where}`, `The proxy answered ${r?.status || 200}. Its tokens from ${k.name} are refused within about a second.`)
    } else if (a.kind === 'unblock') {
      const r = await access.unblockClient(a.b.key, a.b.sub, { quiet: true })
      toast(`${a.b.sub} is unblocked on ${props.where}`, `The proxy answered ${r?.status || 200}. Its tokens work again from their next request.`)
    } else {
      const r = await access.revokeKey(a.k.id, { quiet: true })
      toast(`Revoked ${a.k.name} on ${props.where}`, `The proxy answered ${r?.status || 200}. Requests with it${a.k.scoped ? ', and with the tokens it minted,' : ''} now get 401.`)
    }
    ask.value = null
    await load()
  } catch (e) {
    toast(FAILED[a.kind] || 'Not changed', describeApiError(e), 'red')
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
        <p style="font-weight: 500">The {{ reveal.rotated ? 'new ' : '' }}secret of <span class="qn-mono">{{ reveal.name }}</span></p>
        <div class="qac-secret-row">
          <code class="qn-mono qac-secret">{{ reveal.key }}</code>
          <button type="button" class="qn-btn2" @click="copy">{{ copied ? 'Copied' : 'Copy' }}</button>
        </div>
        <p class="qn-t2" style="margin-top: 10px">
          This is the only time it is shown: the proxy keeps only its hash.
          <template v-if="reveal.rotated">{{ reveal.until ? `The old secret works until ${until(reveal.until)}.` : 'The old secret no longer works.' }}</template>
          <template v-else-if="reveal.mints">Your backend mints a token for each device or user with it, and hands the device only its token:</template>
          <template v-else>Apps send it as a bearer token.</template>
        </p>
        <p v-if="reveal.mints" class="qn-call" style="margin-top: 8px">curl -X POST -H "Authorization: Bearer {{ reveal.key.slice(0, 10) }}…" -d '{"sub":"dev-8f3a","ttlSeconds":3600}' {{ origin }}/api/v1/tokens</p>
        <p v-else class="qn-call" style="margin-top: 8px">curl -H "Authorization: Bearer {{ reveal.key.slice(0, 10) }}…" {{ origin }}/api/v1/resources/queues</p>
        <div style="display: flex; justify-content: flex-end; margin-top: 12px"><button type="button" class="qn-btn" @click="reveal = null">I have stored it</button></div>
      </section>

      <p v-if="keys === null && !failed" class="qn-t2" style="padding: 16px 0">Reading the keys…</p>
      <template v-else-if="keys">
        <div v-if="keyRows.length" class="qac-scroll">
          <div role="table" aria-labelledby="qac-keys" class="qac-table qac-keys" style="--cols: minmax(0, 1fr) minmax(0, 1.5fr) 90px 100px 200px">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Name</span><span role="columnheader">Can</span><span role="columnheader">Made</span><span role="columnheader">Last used</span><span role="columnheader" class="r"><span class="qn-vh">Change</span></span>
            </div>
            <div v-for="k in keyRows" :key="k.id" class="qn-tr" :class="{ 'qac-gone': k.revoked_at }" role="row">
              <span role="cell" class="qn-mono" :title="k.name">{{ k.name }}</span>
              <span role="cell" :class="{ 'qn-mono qac-grants': k.scoped }" :title="k.canAll || k.can">{{ k.can }}</span>
              <span role="cell" class="qn-num qn-t2">{{ day(k.created_at) }}</span>
              <span role="cell" class="qn-num" :class="k.last_used_at ? 'qn-t2' : 'qn-t3'">{{ k.used }}</span>
              <span role="cell" class="r qac-acts">
                <span v-if="k.revoked_at" class="qn-t3">revoked {{ day(k.revoked_at) }}</span>
                <template v-else>
                  <button v-if="k.scoped" type="button" class="qn-btn3 qac-act" aria-haspopup="dialog" :aria-label="`Edit what ${k.name} reaches`" @click="openEdit(k)">Edit</button>
                  <button type="button" class="qn-btn3 qac-act" aria-haspopup="dialog" :aria-label="`Rotate the secret of ${k.name}`" @click="openRotate(k)">Rotate</button>
                  <button type="button" class="qn-btn3 qac-act red" aria-haspopup="dialog" :aria-label="`Revoke ${k.name}`" @click="openRevoke(k)">Revoke</button>
                </template>
              </span>
            </div>
          </div>
        </div>
        <p v-else class="qn-t2" style="padding: 12px 0">No API keys. An app needs one to reach <span class="qn-mono">{{ where }}</span> from outside.</p>
      </template>

      <!-- the clients a scoped key refuses -->
      <template v-if="showBlocks">
        <div class="qac-headrow" style="margin-top: 44px">
          <h2 id="qac-blocks" class="qac-h">Blocked clients</h2>
          <div style="flex: 1 1 0" />
          <button v-if="scopedLive.length" type="button" class="qn-btn2" aria-haspopup="dialog" @click="openBlock">Block a client</button>
        </div>
        <div v-if="blockList.length" class="qac-scroll">
          <div role="table" aria-labelledby="qac-blocks" class="qac-table" style="--cols: minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1.4fr) 100px 100px">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Client</span><span role="columnheader">Key</span><span role="columnheader">Reason</span><span role="columnheader">Since</span><span role="columnheader"><span class="qn-vh">Unblock</span></span>
            </div>
            <div v-for="b in blockList" :key="`${b.key}/${b.sub}`" class="qn-tr" role="row">
              <span role="cell" class="qn-mono" :title="b.sub">{{ b.sub }}</span>
              <span role="cell" class="qn-mono" :title="b.keyName">{{ b.keyName }}</span>
              <span role="cell" :class="{ 'qn-t3': !b.reason }" :title="b.reason || ''">{{ b.reason || 'no reason given' }}</span>
              <span role="cell" class="qn-num qn-t2">{{ ago(b.at) }}</span>
              <span role="cell" class="r"><button type="button" class="qn-btn3 qac-act" aria-haspopup="dialog" :aria-label="`Unblock ${b.sub}`" @click="openUnblock(b)">Unblock</button></span>
            </div>
          </div>
        </div>
        <p v-else class="qn-t2" style="padding: 12px 0">No client is blocked. Blocking one refuses the tokens its key minted for it, on every node, within about a second.</p>
      </template>
      <p class="qn-read" style="margin-top: 12px">A key’s secret shows once, when the key is made or rotated. Read from <span class="qn-mono">GET /api/console/members</span>, <span class="qn-mono">/keys</span> and <span class="qn-mono">/blocks</span>.</p>
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
            <span class="qn-field" style="margin-top: 6px"><input v-model="form.name" class="qn-mono" type="text" maxlength="120" autocomplete="off" spellcheck="false" :placeholder="form.reach === 'scoped' ? 'devices' : 'billing-worker'" /></span>
          </label>
          <fieldset class="qac-pick">
            <legend class="qn-t3" style="font-size: 12px">It reaches</legend>
            <div class="qac-reach">
              <button type="button" class="qn-opt qac-opt" :aria-pressed="form.reach === 'cluster'" @click="form.reach = 'cluster'">
                <span style="font-weight: 500">The whole cluster</span><span class="qn-t3" style="font-size: 12px">Every queue, with what you tick: for your own services</span>
              </button>
              <button type="button" class="qn-opt qac-opt" :aria-pressed="form.reach === 'scoped'" @click="form.reach = 'scoped'">
                <span style="font-weight: 500">Only what its grants name</span><span class="qn-t3" style="font-size: 12px">Some queues, partitions, KV keys or timers: mints a token per device or user</span>
              </button>
            </div>
          </fieldset>
          <fieldset v-if="form.reach === 'cluster'" class="qac-pick">
            <legend class="qn-t3" style="font-size: 12px">What it can do</legend>
            <label v-for="s in SCOPES" :key="s.name" class="qac-check">
              <input type="checkbox" :checked="form.scopes.includes(s.name)" @change="toggleScope(s.name, $event.target.checked)" />
              <span class="qn-mono" style="width: 64px">{{ s.name }}</span><span class="qn-t3" style="font-size: 12px">{{ s.can }}</span>
            </label>
          </fieldset>
          <div v-else style="margin-top: 12px"><GrantsForm v-model="form.policy" /></div>
          <p class="qac-hint">{{ dialog.hint }}</p>
          <p class="qn-t2">The secret shows once, on this page, when the proxy answers.</p>
          <button type="submit" class="qn-vh" tabindex="-1">Create</button>
        </form>
      </template>
      <template v-else-if="ask?.kind === 'edit'">
        <GrantsForm v-model="form.policy" />
        <p class="qn-t2" style="margin-top: 12px">The tokens {{ ask.k.name }} minted follow within about a second, on every node: what it no longer grants is refused from their next request.</p>
      </template>
      <template v-else-if="ask?.kind === 'rotate'">
        <p class="qn-t2">A new secret for the same key. What it can do{{ ask.k.scoped ? ', the tokens it minted' : '' }} and its history stay; apps then send the new secret.</p>
        <fieldset class="qac-pick">
          <legend class="qn-t3" style="font-size: 12px">The old secret stops working</legend>
          <div class="qn-seg" role="group" aria-label="The old secret stops working">
            <button v-for="g in ROTATE_GRACES" :key="g.s" type="button" :aria-pressed="form.grace === g.s" @click="form.grace = g.s">{{ g.name }}</button>
          </div>
        </fieldset>
        <p class="qn-t2" style="margin-top: 12px">{{ form.grace === 0 ? 'Apps still sending the old secret get 401 from their next request.' : `Apps have ${lengthOf(form.grace)} to switch; after that the old secret gets 401.` }}</p>
      </template>
      <template v-else-if="ask?.kind === 'block'">
        <form @submit.prevent="run">
          <fieldset v-if="scopedLive.length > 1" class="qac-pick" style="margin-top: 0">
            <legend class="qn-t3" style="font-size: 12px">Its key</legend>
            <button v-for="k in scopedLive" :key="k.id" type="button" class="qn-opt qac-opt" :aria-pressed="form.blockKey === k.id" @click="form.blockKey = k.id">
              <span class="qn-mono" style="font-weight: 500">{{ k.name }}</span><span class="qn-t3 qn-mono qac-grants" style="font-size: 12px">{{ k.can }}</span>
            </button>
          </fieldset>
          <p v-else class="qn-t2">Its key: <span class="qn-mono">{{ scopedLive[0]?.name }}</span></p>
          <label style="display: block; margin-top: 12px">
            <span class="qn-t3" style="font-size: 12px">Client, as its token names it (sub)</span>
            <span class="qn-field" style="margin-top: 6px"><input v-model="form.sub" class="qn-mono" type="text" maxlength="128" autocomplete="off" spellcheck="false" placeholder="dev-8f3a" /></span>
          </label>
          <label style="display: block; margin-top: 10px">
            <span class="qn-t3" style="font-size: 12px">Reason, for the list (optional)</span>
            <span class="qn-field" style="margin-top: 6px"><input v-model="form.reason" type="text" :maxlength="BLOCK_REASON_MAX" autocomplete="off" placeholder="device reported stolen" /></span>
          </label>
          <p class="qac-hint">{{ dialog.hint }}</p>
          <p class="qn-t2">Every token its key minted for this client is refused on every node within about a second, and the key mints no new one for it, until you unblock it.</p>
          <button type="submit" class="qn-vh" tabindex="-1">Block</button>
        </form>
      </template>
      <template v-else-if="ask?.kind === 'unblock'">
        <p class="qn-t2"><span class="qn-mono">{{ ask.b.sub }}</span> was blocked {{ ago(ask.b.at) }}<template v-if="ask.b.reason">: {{ ask.b.reason }}</template>. Its tokens from <span class="qn-mono">{{ ask.b.keyName }}</span> that have not expired work again from their next request, and the key can mint new ones for it.</p>
      </template>
      <template v-else-if="ask?.kind === 'revoke'">
        <p class="qn-t2">It {{ ask.k.scoped ? 'reaches' : 'can' }} {{ keyCan(ask.k) }}, and was {{ ask.k.last_used_at ? `last used ${ago(ask.k.last_used_at)}` : 'never used' }}. Apps that send it{{ ask.k.scoped ? ', and the tokens it minted,' : '' }} get 401 from their next request. A revoked key cannot come back; make a new one instead.</p>
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
.qac-keys { min-width: 680px; }
.qac-grants { font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.qac-acts { display: flex; justify-content: flex-end; gap: 2px; }
.qac-reach { display: grid; gap: 2px; }
</style>
