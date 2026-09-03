<template>
  <div class="view-container">
    <div class="scope-strip scope-strip-cell">
      <span class="chip chip-warn"><span class="dot"></span>cell · operator</span>
      <span class="scope-text">
        user accounts and cluster access for <strong>cell {{ cellSlug }}</strong>
        <span class="scope-sep">·</span>
        every tenant represented on this cell
      </span>
    </div>

    <div v-if="error" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not load users</strong> · {{ describeApiError(error) }}
        <template v-if="loaded"> · showing the last list that loaded</template>
      </span>
    </div>

    <div class="users-summary">
      <div class="stat">
        <div class="stat-label">Users</div>
        <div class="stat-value font-mono">{{ users.length }}</div>
        <div class="stat-foot">accounts in cell tenants</div>
      </div>
      <div class="stat">
        <div class="stat-label">Tenants</div>
        <div class="stat-value font-mono">{{ tenants.length }}</div>
        <div class="stat-foot">represented on this cell</div>
      </div>
      <div class="stat">
        <div class="stat-label">Access grants</div>
        <div class="stat-value font-mono">{{ grantCount }}</div>
        <div class="stat-foot">roles on cell clusters</div>
      </div>
    </div>

    <div class="card users-filters">
      <div class="card-body users-toolbar">
        <div class="filter-field-col users-search">
          <label class="label-xs" for="users-search">Search</label>
          <input id="users-search" v-model="search" class="input" placeholder="Name, email or tenant" />
        </div>
        <div class="filter-field-col users-tenant-filter">
          <label class="label-xs" for="users-tenant">Tenant</label>
          <select id="users-tenant" v-model="tenantFilter" class="input">
            <option value="">All tenants</option>
            <option v-for="tenant in tenants" :key="tenant.id" :value="tenant.id">
              {{ tenant.slug }}
            </option>
          </select>
        </div>
        <span class="scope-fill"></span>
        <button class="btn btn-primary" :disabled="!tenants.length" @click="openCreate">
          Add user
        </button>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Cell users</h3>
        <span class="card-sub">{{ filteredUsers.length }} shown</span>
        <span v-if="loading" class="muted">refreshing…</span>
      </div>
      <div class="table-container">
        <table v-if="filteredUsers.length" class="table">
          <thead>
            <tr>
              <th>User</th>
              <th>Tenant</th>
              <th>Sign-in</th>
              <th>Cluster access</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="user in filteredUsers" :key="user.id">
              <td>
                <div class="users-name">{{ user.name || 'Unnamed user' }}</div>
                <div class="users-email">{{ user.email }}</div>
                <div class="users-created">created {{ formatDate(user.created_at) }}</div>
                <div class="users-last-login">
                  {{ user.last_login_at ? `last sign-in ${formatDateTime(user.last_login_at)}` : 'never signed in' }}
                </div>
              </td>
              <td><span class="font-mono">{{ user.tenant_slug }}</span></td>
              <td>
                <span class="chip chip-mute">{{ user.has_local_password ? 'local' : 'OAuth' }}</span>
                <span v-if="user.is_operator" class="chip chip-warn users-operator">operator</span>
              </td>
              <td>
                <div v-if="user.roles.length" class="users-role-list">
                  <span v-for="grant in user.roles" :key="grant.cluster_id" class="chip chip-mute">
                    <span class="font-mono">{{ grant.cluster_slug }}</span> · {{ grant.role }}
                  </span>
                </div>
                <span v-else class="users-no-access">No access on this cell</span>
              </td>
              <td class="right">
                <button class="btn btn-ghost" @click="openEdit(user)">Edit</button>
                <button class="btn btn-ghost" @click="openAccess(user)">Manage access</button>
              </td>
            </tr>
          </tbody>
        </table>

        <div v-else-if="loading && !loaded" class="users-loading">
          <div v-for="i in 5" :key="i" class="skeleton" />
        </div>
        <div v-else class="empty-state">
          <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
            <circle cx="9" cy="8" r="3" />
            <path stroke-linecap="round" d="M3.5 20c0-3.3 2.5-6 5.5-6s5.5 2.7 5.5 6M17 8v6M14 11h6" />
          </svg>
          <h3>{{ users.length ? 'No users match these filters' : 'No users on this cell' }}</h3>
          <p>{{ users.length ? 'Try another email or tenant.' : 'Create the first account and grant its initial cluster role.' }}</p>
          <button v-if="!users.length && tenants.length" class="btn btn-primary" @click="openCreate">Add user</button>
        </div>
      </div>
    </div>

    <Teleport to="body">
      <div v-if="showCreate" class="modal-backdrop" @click.self="closeCreate">
        <form class="card modal-card" @submit.prevent="createUser">
          <div class="card-header"><h3>Add user</h3></div>
          <div class="card-body users-form">
            <div v-if="createError" class="panel-err">{{ createError }}</div>

            <label class="users-field">
              <span class="label-xs">Tenant</span>
              <select v-model="createForm.tenant_id" class="input" required>
                <option v-for="tenant in tenants" :key="tenant.id" :value="tenant.id">
                  {{ tenant.slug }} · {{ tenant.name }}
                </option>
              </select>
            </label>
            <label class="users-field">
              <span class="label-xs">Name</span>
              <input v-model.trim="createForm.name" class="input" maxlength="160" autocomplete="off" required placeholder="Ada Lovelace" />
            </label>
            <label class="users-field">
              <span class="label-xs">Email</span>
              <input v-model.trim="createForm.email" class="input" type="email" autocomplete="off" required placeholder="user@example.com" />
            </label>
            <label class="users-field">
              <span class="label-xs">Sign-in method</span>
              <select v-model="createForm.provider" class="input">
                <option value="google">Google OAuth</option>
                <option value="github">GitHub OAuth</option>
                <option value="local">Local password</option>
              </select>
            </label>
            <label v-if="createForm.provider === 'local'" class="users-field">
              <span class="label-xs">Initial password</span>
              <input v-model="createForm.password" class="input" type="password" minlength="12" maxlength="128" autocomplete="new-password" required />
              <span class="users-help">At least 12 characters.</span>
            </label>
            <div v-else class="users-help users-oauth-note">
              The first verified {{ providerLabel }} login with this email links the identity to this account.
            </div>
            <div class="users-form-grid">
              <label class="users-field">
                <span class="label-xs">Initial cluster</span>
                <select v-model="createForm.cluster_id" class="input" required>
                  <option v-for="cluster in createClusters" :key="cluster.id" :value="cluster.id">
                    {{ cluster.slug }}
                  </option>
                </select>
              </label>
              <label class="users-field">
                <span class="label-xs">Role</span>
                <select v-model="createForm.role" class="input">
                  <option v-for="role in roles" :key="role" :value="role">{{ role }}</option>
                </select>
              </label>
            </div>
          </div>
          <div class="modal-foot">
            <button type="button" class="btn btn-ghost" @click="closeCreate">Cancel</button>
            <button type="submit" class="btn btn-primary" :disabled="creating || !createForm.cluster_id">
              {{ creating ? 'Creating…' : 'Create user' }}
            </button>
          </div>
        </form>
      </div>
    </Teleport>

    <Teleport to="body">
      <div v-if="editedUser" class="modal-backdrop" @click.self="closeEdit">
        <form class="card modal-card" @submit.prevent="updateUser">
          <div class="card-header"><h3>Edit user</h3></div>
          <div class="card-body users-form">
            <div v-if="editError" class="panel-err">{{ editError }}</div>
            <div class="users-modal-email">{{ editedUser.email }} · {{ editedUser.tenant_slug }}</div>
            <label class="users-field">
              <span class="label-xs">Name</span>
              <input v-model.trim="editName" class="input" maxlength="160" autocomplete="off" required />
            </label>
          </div>
          <div class="modal-foot">
            <button type="button" class="btn btn-ghost" @click="closeEdit">Cancel</button>
            <button type="submit" class="btn btn-primary" :disabled="editing || !editName.trim()">
              {{ editing ? 'Saving…' : 'Save name' }}
            </button>
          </div>
        </form>
      </div>
    </Teleport>

    <Teleport to="body">
      <div v-if="managedUser" class="modal-backdrop" @click.self="managedUser = null">
        <div class="card modal-card users-access-modal">
          <div class="card-header">
            <div>
              <h3>Manage cluster access</h3>
              <div class="users-modal-email">{{ managedUser.email }} · {{ managedUser.tenant_slug }}</div>
            </div>
          </div>
          <div class="card-body">
            <div v-if="accessError" class="panel-err">{{ accessError }}</div>
            <div class="users-access-list">
              <div v-for="cluster in managedClusters" :key="cluster.id" class="users-access-row">
                <div class="users-cluster">
                  <span class="font-mono">{{ cluster.slug }}</span>
                  <span class="chip chip-mute">{{ cluster.status }}</span>
                </div>
                <select v-model="roleDrafts[cluster.id]" class="input users-role-select">
                  <option value="">No access</option>
                  <option v-for="role in roles" :key="role" :value="role">{{ role }}</option>
                </select>
                <button
                  class="btn"
                  :class="{ 'btn-danger': !roleDrafts[cluster.id] && currentRole(cluster.id) }"
                  :disabled="savingCluster === cluster.id || roleDrafts[cluster.id] === currentRole(cluster.id)"
                  @click="saveRole(cluster)"
                >
                  {{ savingCluster === cluster.id ? 'Saving…' : roleDrafts[cluster.id] ? 'Save' : 'Remove' }}
                </button>
              </div>
            </div>
            <p class="users-help users-access-note">
              A cluster must keep at least one admin. Removing access here affects only this cell.
            </p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="managedUser = null">Close</button>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'

import { operator, describeApiError } from '@/api'
import { useRefresh } from '@/composables/useRefresh'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'

const roles = ['admin', 'producer', 'consumer', 'viewer']
const { actingCellSlug } = useIdentity()
const { notifySuccess } = useToast()

const users = ref([])
const tenants = ref([])
const clusters = ref([])
const responseCell = ref(null)
const loading = ref(false)
const loaded = ref(false)
const error = ref(null)
const search = ref('')
const tenantFilter = ref('')

const cellSlug = computed(() => responseCell.value?.slug || actingCellSlug.value || 'unknown')
const grantCount = computed(() => users.value.reduce((sum, user) => sum + user.roles.length, 0))
const filteredUsers = computed(() => {
  const query = search.value.trim().toLowerCase()
  return users.value.filter(user => {
    if (tenantFilter.value && user.tenant_id !== tenantFilter.value) return false
    if (!query) return true
    return (user.name || '').toLowerCase().includes(query) ||
      user.email.toLowerCase().includes(query) || user.tenant_slug.toLowerCase().includes(query)
  })
})

async function loadUsers() {
  if (loading.value) return
  loading.value = true
  error.value = null
  try {
    const { data } = await operator.listUsers()
    users.value = data.users || []
    tenants.value = data.tenants || []
    clusters.value = data.clusters || []
    responseCell.value = data.cell || null
    loaded.value = true
    if (tenantFilter.value && !tenants.value.some(t => t.id === tenantFilter.value)) tenantFilter.value = ''
  } catch (err) {
    error.value = err
  } finally {
    loading.value = false
  }
}

onMounted(loadUsers)
useRefresh(loadUsers)

const showCreate = ref(false)
const creating = ref(false)
const createError = ref('')
const createForm = reactive({
  tenant_id: '', name: '', email: '', provider: 'google', password: '', cluster_id: '', role: 'viewer',
})
const createClusters = computed(() => clusters.value.filter(c => c.tenant_id === createForm.tenant_id))
const providerLabel = computed(() => createForm.provider === 'github' ? 'GitHub' : 'Google')

watch(() => createForm.tenant_id, () => {
  if (!createClusters.value.some(c => c.id === createForm.cluster_id)) {
    createForm.cluster_id = createClusters.value[0]?.id || ''
  }
})

function openCreate() {
  const tenant = tenants.value.find(t => t.id === tenantFilter.value) || tenants.value[0]
  createForm.tenant_id = tenant?.id || ''
  createForm.name = ''
  createForm.email = ''
  createForm.provider = 'google'
  createForm.password = ''
  createForm.role = 'viewer'
  createForm.cluster_id = clusters.value.find(c => c.tenant_id === createForm.tenant_id)?.id || ''
  createError.value = ''
  showCreate.value = true
}

function closeCreate() {
  if (creating.value) return
  showCreate.value = false
  createError.value = ''
}

async function createUser() {
  if (creating.value) return
  creating.value = true
  createError.value = ''
  try {
    await operator.createUser({
      tenant_id: createForm.tenant_id,
      name: createForm.name,
      email: createForm.email,
      provider: createForm.provider,
      password: createForm.provider === 'local' ? createForm.password : null,
      cluster_id: createForm.cluster_id,
      role: createForm.role,
    })
    showCreate.value = false
    notifySuccess(`Created ${createForm.email}`)
    await loadUsers()
  } catch (err) {
    createError.value = describeApiError(err)
  } finally {
    creating.value = false
  }
}

const editedUser = ref(null)
const editName = ref('')
const editing = ref(false)
const editError = ref('')

function openEdit(user) {
  editedUser.value = user
  editName.value = user.name || ''
  editError.value = ''
}

function closeEdit() {
  if (editing.value) return
  editedUser.value = null
  editError.value = ''
}

async function updateUser() {
  if (!editedUser.value || editing.value || !editName.value.trim()) return
  const userId = editedUser.value.id
  editing.value = true
  editError.value = ''
  try {
    await operator.updateUser(userId, { name: editName.value })
    notifySuccess(`Updated ${editName.value}`)
    editedUser.value = null
    await loadUsers()
  } catch (err) {
    editError.value = describeApiError(err)
  } finally {
    editing.value = false
  }
}

const managedUser = ref(null)
const roleDrafts = ref({})
const savingCluster = ref(null)
const accessError = ref('')
const managedClusters = computed(() =>
  managedUser.value ? clusters.value.filter(c => c.tenant_id === managedUser.value.tenant_id) : []
)

function openAccess(user) {
  managedUser.value = user
  roleDrafts.value = Object.fromEntries(
    clusters.value
      .filter(c => c.tenant_id === user.tenant_id)
      .map(c => [c.id, user.roles.find(r => r.cluster_id === c.id)?.role || ''])
  )
  accessError.value = ''
}

function currentRole(clusterId) {
  return managedUser.value?.roles.find(role => role.cluster_id === clusterId)?.role || ''
}

async function saveRole(cluster) {
  if (!managedUser.value || savingCluster.value) return
  const userId = managedUser.value.id
  const nextRole = roleDrafts.value[cluster.id]
  savingCluster.value = cluster.id
  accessError.value = ''
  try {
    if (nextRole) await operator.setUserRole(userId, cluster.id, nextRole)
    else await operator.removeUserRole(userId, cluster.id)
    notifySuccess(nextRole ? `Access on ${cluster.slug} set to ${nextRole}` : `Access on ${cluster.slug} removed`)
    await loadUsers()
    const refreshed = users.value.find(user => user.id === userId)
    if (refreshed) openAccess(refreshed)
  } catch (err) {
    accessError.value = describeApiError(err)
    roleDrafts.value[cluster.id] = currentRole(cluster.id)
  } finally {
    savingCluster.value = null
  }
}

function formatDate(value) {
  if (!value) return 'unknown'
  return new Date(value).toLocaleDateString()
}

function formatDateTime(value) {
  if (!value) return 'unknown'
  return new Date(value).toLocaleString()
}
</script>

<style scoped>
.users-summary {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}
.users-filters { margin-bottom: 12px; }
.users-toolbar { display: flex; align-items: end; gap: 10px; }
.users-search { flex: 1 1 320px; max-width: 460px; }
.users-tenant-filter { flex: 0 1 220px; }
.users-name { font-weight: 550; }
.users-email { margin-top: 1px; color: var(--text-mid); font-size: 12px; }
.users-created, .users-last-login, .users-modal-email { margin-top: 2px; color: var(--text-low); font-size: 11px; }
.users-operator { margin-left: 5px; }
.users-role-list { display: flex; flex-wrap: wrap; gap: 5px; }
.users-no-access { color: var(--text-low); font-style: italic; }
.users-loading { display: grid; gap: 8px; padding: 16px; }
.users-loading .skeleton { height: 34px; }
.users-form { display: grid; gap: 14px; }
.users-field { display: grid; gap: 6px; }
.users-form-grid { display: grid; grid-template-columns: 1fr 140px; gap: 10px; }
.users-help { color: var(--text-low); font-size: 11.5px; line-height: 1.45; }
.users-oauth-note { padding: 9px 10px; border: 1px solid var(--bd); border-radius: var(--r-control); background: var(--ink-3); }
.users-access-modal { max-width: 620px; }
.users-access-list { display: grid; gap: 8px; }
.users-access-row {
  display: grid;
  grid-template-columns: minmax(140px, 1fr) 150px 76px;
  gap: 8px;
  align-items: center;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--bd-soft);
}
.users-access-row:last-child { padding-bottom: 0; border-bottom: 0; }
.users-cluster { display: flex; align-items: center; gap: 7px; min-width: 0; }
.users-role-select { min-width: 0; }
.users-access-note { margin-top: 14px; }
.right { text-align: right; }
@media (max-width: 720px) {
  .users-summary { grid-template-columns: 1fr; }
  .users-toolbar { align-items: stretch; flex-direction: column; }
  .users-search, .users-tenant-filter { max-width: none; width: 100%; }
  .users-form-grid { grid-template-columns: 1fr; }
  .users-access-row { grid-template-columns: 1fr 1fr; }
  .users-access-row .btn { grid-column: 2; justify-self: end; }
}
</style>
