<template>
  <div class="view-container">

    <PageHead title="Members">
      <template #sub>
        <span>cluster <b>{{ actingClusterSlug || '—' }}</b> · tenant <b>{{ actingTenantSlug || '—' }}</b></span>
      </template>
      <template #actions>
        <button class="btn btn-primary" @click="openGrant">Add member</button>
      </template>
    </PageHead>

    <div v-if="error" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not load members</strong> · {{ describeApiError(error) }}
        <template v-if="loaded"> · showing the last list that loaded</template>
      </span>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Who has access</h3>
        <span class="card-sub">
          {{ members.length }} {{ members.length === 1 ? 'member' : 'members' }} ·
          {{ adminCount }} {{ adminCount === 1 ? 'admin' : 'admins' }}
        </span>
        <span v-if="loading" class="muted">refreshing…</span>
      </div>
      <div class="table-container">
        <table v-if="members.length" class="t members-table">
          <thead>
            <tr>
              <th>Member</th>
              <th>Role</th>
              <th>Since</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="member in members" :key="member.email">
              <td class="members-who">
                <span class="members-email">{{ member.email }}</span>
                <span v-if="isMe(member)" class="members-you">you</span>
              </td>
              <td>
                <select
                  class="input members-role"
                  :value="member.role"
                  :disabled="busy === member.email"
                  :aria-label="`Role of ${member.email}`"
                  @change="changeRole(member, $event)"
                >
                  <option v-for="role in ROLES" :key="role.name" :value="role.name">{{ role.name }}</option>
                </select>
              </td>
              <td class="members-since" :title="formatTimestampUtc(member.granted_at)">
                {{ formatTimestamp(member.granted_at) }}
              </td>
              <td class="right members-act">
                <template v-if="confirming === member.email">
                  <span class="members-confirm">{{ isMe(member) ? 'Remove your own access?' : 'Remove access?' }}</span>
                  <button class="btn btn-danger" :disabled="busy === member.email" @click="revoke(member)">
                    {{ busy === member.email ? 'Removing…' : 'Remove' }}
                  </button>
                  <button class="btn btn-ghost" @click="confirming = null">Keep</button>
                </template>
                <button v-else class="btn btn-ghost" @click="confirming = member.email">Remove</button>
              </td>
            </tr>
          </tbody>
        </table>

        <div v-else-if="loading && !loaded" class="members-loading">
          <div v-for="i in 4" :key="i" class="skeleton" />
        </div>
        <div v-else-if="loaded" class="empty-state">
          <h3>No members on this cluster</h3>
          <p>Grant a role to someone who has signed in once.</p>
        </div>
      </div>
    </div>

    <div class="card members-roles">
      <div class="card-header"><h3>What each role may do</h3></div>
      <dl class="members-role-list">
        <template v-for="role in ROLES" :key="role.name">
          <dt class="font-mono">{{ role.name }}</dt>
          <dd>{{ role.can }}</dd>
        </template>
      </dl>
      <p class="members-help">
        A cluster keeps at least one admin: the proxy refuses a change that would leave none, including your own.
      </p>
    </div>

    <Teleport to="body">
      <div v-if="showGrant" class="modal-backdrop" @click.self="closeGrant">
        <form class="card modal-card" @submit.prevent="grant">
          <div class="card-header"><h3>Add member</h3></div>
          <div class="card-body members-form">
            <div v-if="grantError" class="panel-err">{{ grantError }}</div>
            <label class="members-field">
              <span class="label-xs">Email</span>
              <input
                ref="emailInput"
                v-model.trim="grantForm.email"
                class="input"
                type="email"
                autocomplete="off"
                required
                placeholder="teammate@example.com"
              />
            </label>
            <label class="members-field">
              <span class="label-xs">Role</span>
              <select v-model="grantForm.role" class="input">
                <option v-for="role in ROLES" :key="role.name" :value="role.name">{{ role.name }} — {{ role.can }}</option>
              </select>
            </label>
            <p class="members-help">
              The address must already be a user of tenant <b>{{ actingTenantSlug || '—' }}</b>. Signing in once
              creates it. Granting to an existing member changes their role.
            </p>
          </div>
          <div class="modal-foot">
            <button type="button" class="btn btn-ghost" @click="closeGrant">Cancel</button>
            <button type="submit" class="btn btn-primary" :disabled="granting || !grantForm.email">
              {{ granting ? 'Granting…' : 'Grant access' }}
            </button>
          </div>
        </form>
      </div>
    </Teleport>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'

import { access, describeApiError } from '@/api'
import { useRefresh } from '@/composables/useRefresh'
import { useToast } from '@/composables/useToast'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useIdentity } from '@/stores/identity'
import PageHead from '@/components/PageHead.vue'

// proxy/src/console.rs VALID_ROLES, most privileged first; what each allows is
// the proxy's RouteClass table, mirrored by stores/identity.js CAPABILITIES.
const ROLES = [
  { name: 'admin', can: 'everything, including members and API keys' },
  { name: 'producer', can: 'push messages, and read' },
  { name: 'consumer', can: 'pop and acknowledge messages, and read' },
  { name: 'viewer', can: 'read only' },
]

const { email, epoch, actingClusterSlug, actingTenantSlug } = useIdentity()
const { notifySuccess } = useToast()

const members = ref([])
const loading = ref(false)
const loaded = ref(false)
const error = ref(null)
const busy = ref(null) // email of the row with a call in flight
const confirming = ref(null) // email of the row asking "remove?"

const adminCount = computed(() => members.value.filter(m => m.role === 'admin').length)
const isMe = member => !!email.value && member.email.toLowerCase() === email.value.toLowerCase()

async function load() {
  if (loading.value) return
  loading.value = true
  error.value = null
  try {
    const { data } = await access.listMembers()
    members.value = Array.isArray(data) ? data : []
    loaded.value = true
  } catch (err) {
    error.value = err
  } finally {
    loading.value = false
  }
}

onMounted(load)
useRefresh(load)
// Members belong to the acting cluster: a switch is a different list.
watch(epoch, () => {
  members.value = []
  loaded.value = false
  confirming.value = null
  load()
})

async function changeRole(member, event) {
  const role = event.target.value
  if (role === member.role) return
  busy.value = member.email
  try {
    await access.grantMember(member.email, role)
    notifySuccess(`${member.email} is now ${role}`)
  } catch {
    // The shell's failure toast carries the proxy's reason (a cluster keeps
    // one admin); the reload below puts the refused role back in the select.
  } finally {
    busy.value = null
    await load()
  }
}

async function revoke(member) {
  busy.value = member.email
  try {
    await access.revokeMember(member.email)
    notifySuccess(`Removed ${member.email}`)
    confirming.value = null
  } catch {
    // The shell's failure toast carries the proxy's reason.
  } finally {
    busy.value = null
    await load()
  }
}

const showGrant = ref(false)
const granting = ref(false)
const grantError = ref('')
const grantForm = reactive({ email: '', role: 'viewer' })
const emailInput = ref(null)

function openGrant() {
  grantForm.email = ''
  grantForm.role = 'viewer'
  grantError.value = ''
  showGrant.value = true
  nextTick(() => emailInput.value?.focus())
}

function closeGrant() {
  if (!granting.value) showGrant.value = false
}

async function grant() {
  granting.value = true
  grantError.value = ''
  try {
    await access.grantMember(grantForm.email, grantForm.role)
    notifySuccess(`${grantForm.email} is now ${grantForm.role}`)
    showGrant.value = false
    await load()
  } catch (err) {
    grantError.value = describeApiError(err)
  } finally {
    granting.value = false
  }
}
</script>

<style scoped>
.members-email { font-weight: 550; }
.members-you {
  margin-left: 7px;
  color: var(--text-low);
  font-size: 11px;
}
.members-role { width: 132px; }
.members-since { color: var(--text-mid); white-space: nowrap; }
.members-confirm { margin-right: 8px; color: var(--text-mid); font-size: 12px; }
.members-loading { display: grid; gap: 8px; padding: 16px; }
.members-loading .skeleton { height: 34px; }
.members-roles { margin-top: 12px; }
.members-role-list {
  display: grid;
  grid-template-columns: max-content 1fr;
  gap: 6px 18px;
  margin: 0;
  padding: 4px 16px 0;
  font-size: 12.5px;
}
.members-role-list dt { color: var(--text-hi); }
.members-role-list dd { margin: 0; color: var(--text-mid); }
.members-form { display: grid; gap: 14px; }
.members-field { display: grid; gap: 6px; }
.members-help { margin: 0; padding: 12px 16px 14px; color: var(--text-low); font-size: 11.5px; line-height: 1.45; }
.members-form .members-help { padding: 0; }
.right { text-align: right; white-space: nowrap; }
/* Phone: a row is a small block — who, then role and remove on one line —
   instead of a table wider than the screen. The date is the one to drop. */
@media (max-width: 720px) {
  .members-table thead { display: none; }
  .members-table tr {
    display: grid;
    grid-template-columns: 1fr auto;
    align-items: center;
    gap: 8px 10px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--bd-soft);
  }
  .members-table tr:last-child { border-bottom: 0; }
  .members-table td { display: block; padding: 0; border: 0; }
  .members-table .members-who { grid-column: 1 / -1; overflow-wrap: anywhere; }
  .members-table .members-since { display: none; }
  .members-act { white-space: normal; }
  .members-confirm { display: block; margin: 0 0 6px; }
}
</style>
