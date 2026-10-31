<template>
  <!--
    Access's People table: who may use the acting cluster, and with which
    role. The list beside it (AccessSide) filters it by role (?role=); the
    members are the Access store's, read with the keys.
  -->
  <div>
    <PaneHead :title="title" :facts="facts">
      <template #actions>
        <button class="btn btn-primary" @click="openGrant">Add member</button>
      </template>
    </PaneHead>

    <div class="view-container">
      <div v-if="error" class="status-banner banner-bad view-banner">
        <span>
          <strong>Could not load members</strong> · {{ describeApiError(error) }}<template v-if="loaded"> · showing the last list</template>
        </span>
      </div>

      <div class="card">
        <div v-if="shown.length" class="table-container">
          <table class="t mem-table">
            <thead>
              <tr>
                <th>Member</th>
                <th title="A cluster keeps at least one admin: the proxy refuses a change that would leave none">Role</th>
                <th>Since</th>
                <th><span class="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="member in shown" :key="member.email">
                <td class="mem-who">
                  <span class="mem-email">{{ member.email }}</span>
                  <span v-if="isMe(member)" class="mem-you">you</span>
                </td>
                <td class="mem-role">
                  <UiSelect
                    :model-value="member.role"
                    :options="roleOptions"
                    size="sm"
                    :disabled="busy === member.email"
                    :aria-label="`Role of ${member.email}`"
                    @update:model-value="changeRole(member, $event)"
                  />
                </td>
                <td class="mem-since" :title="formatTimestampUtc(member.granted_at)">{{ day(member.granted_at) }}</td>
                <td class="mem-act">
                  <template v-if="confirming === member.email">
                    <span class="mem-confirm">{{ isMe(member) ? 'Remove your own access?' : 'Remove access?' }}</span>
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
        </div>

        <div v-else-if="loading && !loaded" class="acc-loading">
          <div v-for="i in 4" :key="i" class="skeleton" />
        </div>
        <div v-else-if="members.length" class="empty-state">
          <Icon name="users" :size="28" class="empty-state-icon" />
          <p>No member has this role</p>
          <button class="btn btn-ghost" @click="role = ''">Show all members</button>
        </div>
        <div v-else-if="loaded" class="empty-state">
          <Icon name="users" :size="28" class="empty-state-icon" />
          <p>No members on this cluster</p>
          <button class="btn" @click="openGrant">Add member</button>
        </div>
        <div v-else-if="error" class="empty-state">
          <Icon name="users" :size="28" class="empty-state-icon" />
          <p>Cannot read the members</p>
          <button class="btn btn-ghost" @click="load">Retry</button>
        </div>
      </div>
    </div>

    <Teleport to="body">
      <div v-if="showGrant" class="modal-backdrop" @click.self="closeGrant">
        <form class="card modal-card" @submit.prevent="grant">
          <div class="card-header"><h3>Add member</h3></div>
          <div class="card-body acc-form">
            <div v-if="grantError" class="panel-err">{{ grantError }}</div>
            <label class="acc-field">
              <span class="acc-label">Email</span>
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
            <div class="acc-field">
              <span class="acc-label">Role</span>
              <UiSelect v-model="grantForm.role" :options="roleOptions" block aria-label="Role" />
              <span class="acc-help">{{ roleCan(grantForm.role) }}</span>
            </div>
            <p
              class="acc-help"
              :title="`The address must already be a user of tenant ${actingTenantSlug || 'this tenant'}: signing in once creates it. Granting to someone who is already a member changes their role.`"
            >They must have signed in once.</p>
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
import { computed, nextTick, reactive, ref, watch } from 'vue'

import { access, describeApiError } from '@/api'
import { ROLES, filterMembers, memberPickTitle, roleCan } from '@/composables/accessGroups'
import { useRouteState } from '@/composables/useRouteState'
import { useToast } from '@/composables/useToast'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useAccessStore } from '@/stores/accessStore'
import { useIdentity } from '@/stores/identity'
import Icon from '@/components/Icon.vue'
import PaneHead from '@/components/PaneHead.vue'
import UiSelect from '@/components/UiSelect.vue'

const { email, epoch, actingClusterSlug, actingTenantSlug } = useIdentity()
const { notifySuccess } = useToast()

// The roles as a picker names them; the values are the proxy's.
const roleOptions = ROLES.map(r => ({ value: r.name, label: r.name[0].toUpperCase() + r.name.slice(1) }))

// Read by the Access page with the keys (stores/accessStore), so its list
// counts both; a change here reloads this list.
const { members, membersLoading: loading, membersLoaded: loaded, membersError: error, loadMembers: load } = useAccessStore()
const busy = ref(null) // email of the row with a call in flight
const confirming = ref(null) // email of the row asking "remove?"

// The role picked in the list (AccessSide): the same address field, so the
// list and the table show the same members.
const role = ref('')
useRouteState({ role })
const shown = computed(() => filterMembers(members.value, role.value))
const isMe = member => !!email.value && member.email.toLowerCase() === email.value.toLowerCase()
// A grant is a day; the tooltip keeps the instant.
const day = v => formatTimestamp(v, 'en-US', { hour: undefined, minute: undefined, second: undefined })

const title = computed(() => memberPickTitle(role.value))
const adminCount = computed(() => members.value.filter(m => m.role === 'admin').length)
const facts = computed(() => [
  {
    k: 'Cluster',
    v: actingClusterSlug.value || '—',
    title: `Who may use cluster ${actingClusterSlug.value || '—'} of tenant ${actingTenantSlug.value || '—'}, and with which role`,
  },
  role.value
    ? (roleCan(role.value) ? { v: roleCan(role.value), title: `What the ${role.value} role may do` } : null)
    : (loaded.value ? { k: 'Admins', v: String(adminCount.value), title: 'A cluster keeps at least one admin: the proxy refuses a change that would leave none' } : null),
].filter(Boolean))

// Members belong to the acting cluster: a switch is a different list (the
// page reloads it); a question asked of the old one is dropped.
watch(epoch, () => { confirming.value = null })

async function changeRole(member, next) {
  if (!next || next === member.role) return
  busy.value = member.email
  try {
    await access.grantMember(member.email, next)
    notifySuccess(`${member.email} is now ${next}`)
  } catch {
    // The shell's failure toast carries the proxy's reason (a cluster keeps
    // one admin); the reload below puts the refused role back in the picker.
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
.mem-who { overflow-wrap: anywhere; }
.mem-email { font-weight: 500; color: var(--text-hi); }
.mem-you { margin-left: 8px; font-size: 12px; color: var(--text-low); }
.mem-role { width: 160px; }
.mem-role :deep(.uis) { width: 132px; }
.mem-since { color: var(--text-mid); white-space: nowrap; font-variant-numeric: tabular-nums; }
.mem-act { text-align: right; white-space: nowrap; }
.mem-confirm { margin-right: 8px; font-size: 12px; color: var(--text-mid); }

.acc-loading { display: grid; gap: 8px; padding: 16px; }
.acc-loading .skeleton { height: 34px; }

/* The form: a label over each field, one short line under it at most. */
.acc-form { display: grid; gap: 16px; }
.acc-field { display: grid; gap: 4px; }
.acc-label { font-size: 12px; font-weight: 500; color: var(--text-mid); }
.acc-help { margin: 0; font-size: 12px; line-height: 1.45; color: var(--text-low); }

/* Phone: a row is a small block — who, then role and remove on one line —
   instead of a table wider than the screen. The date is the one to drop. */
@media (max-width: 720px) {
  .mem-table thead { display: none; }
  .mem-table tr {
    display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center;
    gap: 8px 12px; padding: 12px 16px; border-bottom: 1px solid var(--bd-soft);
  }
  .mem-table tr:last-child { border-bottom: 0; }
  .mem-table td { display: block; padding: 0; border: 0; }
  .mem-table .mem-who { grid-column: 1 / -1; }
  .mem-table .mem-role { width: auto; }
  .mem-table .mem-since { display: none; }
  .mem-act { white-space: normal; }
  .mem-confirm { display: block; margin: 0 0 6px; }
}
</style>
