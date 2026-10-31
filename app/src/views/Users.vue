<template>
  <SubSidebar label="Users" :current="title">
    <template #side><UsersSide :users="users" :tenants="tenants" :loaded="loaded" :error="error" /></template>

    <PaneHead :title="title" :facts="facts">
      <template #actions>
        <button class="btn btn-primary" :disabled="!tenants.length" @click="openCreate">Add user</button>
      </template>
    </PaneHead>

    <div class="view-container">
      <div v-if="error" class="status-banner banner-bad view-banner">
        <span>
          <strong>Could not load users</strong> · {{ describeApiError(error) }}<template v-if="loaded"> · showing the last list</template>
        </span>
      </div>

      <div class="card">
        <div v-if="filteredUsers.length" class="table-container">
          <table class="t usr-table">
            <thead>
              <tr>
                <th>User</th>
                <th>Tenant</th>
                <th>Sign-in</th>
                <th>Last sign-in</th>
                <th title="Roles are per cluster: the user's role on each cluster of this cell">Cluster access</th>
                <th><span class="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="user in filteredUsers" :key="user.id">
                <td class="usr-who-cell">
                  <div class="usr-who">
                    <span class="usr-name" :class="{ none: !user.name }">{{ user.name || 'Unnamed' }}</span>
                    <span class="usr-email">{{ user.email }}</span>
                  </div>
                </td>
                <td class="usr-tenant">{{ user.tenant_slug }}</td>
                <td class="usr-method">
                  <span class="chip chip-mute">{{ user.has_local_password ? 'local' : 'OAuth' }}</span>
                  <!-- A role, i.e. an identity: the scope's neutral outline, not a warning. -->
                  <span v-if="user.is_operator" class="chip chip-scope">operator</span>
                </td>
                <td class="usr-when" :title="whenTitle(user)">{{ user.last_login_at ? formatRelativeTime(user.last_login_at) : 'never' }}</td>
                <td class="usr-access">
                  <span v-if="user.roles.length" class="usr-grants">
                    <span v-for="g in user.roles" :key="g.cluster_id" class="chip chip-mute">{{ g.cluster_slug }} · {{ g.role }}</span>
                  </span>
                  <span v-else class="usr-none">No access</span>
                </td>
                <td class="usr-act">
                  <button class="btn btn-ghost" @click="openEdit(user)">Rename</button>
                  <button class="btn btn-ghost" @click="openAccess(user)">Access</button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-else-if="loading && !loaded" class="acc-loading">
          <div v-for="i in 5" :key="i" class="skeleton" />
        </div>
        <div v-else-if="users.length" class="empty-state">
          <Icon name="users-round" :size="28" class="empty-state-icon" />
          <p>No user matches</p>
          <button class="btn btn-ghost" @click="showAll">Show all users</button>
        </div>
        <div v-else-if="loaded" class="empty-state">
          <Icon name="users-round" :size="28" class="empty-state-icon" />
          <p>No users on this cell</p>
          <button v-if="tenants.length" class="btn" @click="openCreate">Add user</button>
        </div>
        <div v-else-if="error" class="empty-state">
          <Icon name="users-round" :size="28" class="empty-state-icon" />
          <p>Cannot read the users</p>
          <button class="btn btn-ghost" @click="loadUsers">Retry</button>
        </div>
      </div>
    </div>

    <!-- =================== ADD =================== -->
    <Teleport to="body">
      <div v-if="showCreate" class="modal-backdrop" @click.self="closeCreate">
        <form class="card modal-card" @submit.prevent="createUser">
          <div class="card-header"><h3>Add user</h3></div>
          <div class="card-body acc-form">
            <div v-if="createError" class="panel-err">{{ createError }}</div>
            <div class="acc-field">
              <span class="acc-label">Tenant</span>
              <UiSelect v-model="createForm.tenant_id" :options="tenantOptions" block aria-label="Tenant" />
            </div>
            <label class="acc-field">
              <span class="acc-label">Name</span>
              <input v-model.trim="createForm.name" class="input" maxlength="160" autocomplete="off" required placeholder="Ada Lovelace" />
            </label>
            <label class="acc-field">
              <span class="acc-label">Email</span>
              <input v-model.trim="createForm.email" class="input" type="email" autocomplete="off" required placeholder="user@example.com" />
            </label>
            <div class="acc-field">
              <span class="acc-label">Sign-in</span>
              <UiSelect v-model="createForm.provider" :options="PROVIDERS" block aria-label="Sign-in method" />
              <span v-if="createForm.provider !== 'local'" class="acc-help">Linked on the first verified {{ providerLabel }} sign-in with this email.</span>
            </div>
            <label v-if="createForm.provider === 'local'" class="acc-field">
              <span class="acc-label">Initial password</span>
              <input v-model="createForm.password" class="input" type="password" minlength="12" maxlength="128" autocomplete="new-password" required />
              <span class="acc-help">At least 12 characters.</span>
            </label>
            <div class="usr-pair">
              <div class="acc-field">
                <span class="acc-label">Cluster</span>
                <UiSelect v-model="createForm.cluster_id" :options="createClusterOptions" block placeholder="No cluster" :disabled="!createClusterOptions.length" aria-label="Initial cluster" />
              </div>
              <div class="acc-field">
                <span class="acc-label">Role</span>
                <UiSelect v-model="createForm.role" :options="roleOptions" block aria-label="Role on that cluster" />
              </div>
            </div>
            <span v-if="!createClusterOptions.length" class="acc-help">This tenant has no cluster on this cell.</span>
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

    <!-- =================== RENAME =================== -->
    <Teleport to="body">
      <div v-if="editedUser" class="modal-backdrop" @click.self="closeEdit">
        <form class="card modal-card" @submit.prevent="updateUser">
          <div class="card-header">
            <h3>Rename user</h3>
            <span class="card-sub">{{ editedUser.email }}</span>
          </div>
          <div class="card-body acc-form">
            <div v-if="editError" class="panel-err">{{ editError }}</div>
            <label class="acc-field">
              <span class="acc-label">Name</span>
              <input ref="editInput" v-model.trim="editName" class="input" maxlength="160" autocomplete="off" required />
            </label>
          </div>
          <div class="modal-foot">
            <button type="button" class="btn btn-ghost" @click="closeEdit">Cancel</button>
            <button type="submit" class="btn btn-primary" :disabled="editing || !editName.trim()">
              {{ editing ? 'Saving…' : 'Save' }}
            </button>
          </div>
        </form>
      </div>
    </Teleport>

    <!-- =================== ACCESS =================== -->
    <Teleport to="body">
      <div v-if="managedUser" class="modal-backdrop" @click.self="managedUser = null">
        <div class="card modal-card usr-access-card">
          <div class="card-header">
            <h3>Cluster access</h3>
            <span class="card-sub">{{ managedUser.email }} · {{ managedUser.tenant_slug }}</span>
          </div>
          <div class="card-body">
            <div v-if="accessError" class="panel-err">{{ accessError }}</div>
            <div v-if="managedClusters.length" class="usr-access-list">
              <div v-for="cluster in managedClusters" :key="cluster.id" class="usr-access-row">
                <span class="usr-cluster" :title="cluster.status ? `Cluster ${cluster.slug} · ${cluster.status}` : undefined">
                  <span class="usr-cluster-slug">{{ cluster.slug }}</span>
                  <span v-if="cluster.status" class="usr-cluster-status">{{ cluster.status }}</span>
                </span>
                <UiSelect v-model="roleDrafts[cluster.id]" :options="accessOptions" block size="sm" :aria-label="`Role on ${cluster.slug}`" />
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
            <p v-else class="acc-help">This tenant has no cluster on this cell.</p>
            <p class="acc-help usr-access-note" title="Removing access here affects only this cell">A cluster keeps at least one admin.</p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="managedUser = null">Close</button>
          </div>
        </div>
      </div>
    </Teleport>
  </SubSidebar>
</template>

<script setup>
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'

import { operator, describeApiError } from '@/api'
import { ROLES, filterUsers, userPickTitle } from '@/composables/accessGroups'
import { formatRelativeTime } from '@/composables/useApi'
import { formatTimestamp } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import Icon from '@/components/Icon.vue'
import PaneHead from '@/components/PaneHead.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import UiSelect from '@/components/UiSelect.vue'
import UsersSide from '@/components/UsersSide.vue'

const PROVIDERS = [
  { value: 'google', label: 'Google' },
  { value: 'github', label: 'GitHub' },
  { value: 'local', label: 'Local password' },
]
// The roles as a picker names them; the values are the proxy's.
const roleOptions = ROLES.map(r => ({ value: r.name, label: r.name[0].toUpperCase() + r.name.slice(1) }))
const accessOptions = [{ value: '', label: 'No access' }, ...roleOptions]

const { actingCellSlug } = useIdentity()
const { notifySuccess } = useToast()

const users = ref([])
const tenants = ref([])
const clusters = ref([])
const responseCell = ref(null)
const loading = ref(false)
const loaded = ref(false)
const error = ref(null)
// The search and the pick live in the list (UsersSide), under the same
// address fields, so the list and the table show the same users. A tenant is
// kept by its slug; role is a role held on a cell cluster, or NO_ACCESS.
const search = ref('')
const tenant = ref('')
const role = ref('')
useRouteState({ search, tenant, role })

const cellSlug = computed(() => responseCell.value?.slug || actingCellSlug.value || 'unknown')
const grantCount = computed(() => users.value.reduce((sum, user) => sum + (user.roles?.length || 0), 0))
const filteredUsers = computed(() =>
  filterUsers(users.value, { search: search.value, tenant: tenant.value, role: role.value }))
const title = computed(() => userPickTitle({ tenant: tenant.value, role: role.value }))
const facts = computed(() => [
  { k: 'Cell', v: cellSlug.value, title: 'Every account of every tenant represented on this cell' },
  loaded.value ? { k: 'Tenants', v: String(tenants.value.length) } : null,
  loaded.value ? { k: 'Grants', v: String(grantCount.value), title: 'Roles held on the cell’s clusters, one per user and cluster' } : null,
].filter(Boolean))

function showAll() {
  search.value = ''
  tenant.value = ''
  role.value = ''
}

const whenTitle = user => [
  user.last_login_at ? `Last sign-in ${formatTimestamp(user.last_login_at)}` : 'Never signed in',
  user.created_at ? `created ${formatTimestamp(user.created_at)}` : null,
].filter(Boolean).join(' · ')

async function loadUsers() {
  if (loading.value) return
  loading.value = true
  error.value = null
  try {
    const { data } = await operator.listUsers()
    users.value = (data.users || []).map(u => ({ ...u, roles: Array.isArray(u.roles) ? u.roles : [] }))
    tenants.value = data.tenants || []
    clusters.value = data.clusters || []
    responseCell.value = data.cell || null
    loaded.value = true
  } catch (err) {
    error.value = err
  } finally {
    loading.value = false
  }
}

onMounted(loadUsers)
useRefresh(loadUsers)

// ---------------------------------------------------------------------------
// Add
// ---------------------------------------------------------------------------
const showCreate = ref(false)
const creating = ref(false)
const createError = ref('')
const createForm = reactive({
  tenant_id: '', name: '', email: '', provider: 'google', password: '', cluster_id: '', role: 'viewer',
})
const tenantOptions = computed(() => tenants.value.map(t => ({
  value: t.id,
  label: t.name && t.name !== t.slug ? `${t.slug} · ${t.name}` : t.slug,
})))
const createClusters = computed(() => clusters.value.filter(c => c.tenant_id === createForm.tenant_id))
const createClusterOptions = computed(() => createClusters.value.map(c => ({ value: c.id, label: c.slug })))
const providerLabel = computed(() => PROVIDERS.find(p => p.value === createForm.provider)?.label || '')

watch(() => createForm.tenant_id, () => {
  if (!createClusters.value.some(c => c.id === createForm.cluster_id)) {
    createForm.cluster_id = createClusters.value[0]?.id || ''
  }
})

function openCreate() {
  const picked = tenants.value.find(t => t.slug === tenant.value) || tenants.value[0]
  createForm.tenant_id = picked?.id || ''
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

// ---------------------------------------------------------------------------
// Rename
// ---------------------------------------------------------------------------
const editedUser = ref(null)
const editName = ref('')
const editing = ref(false)
const editError = ref('')
const editInput = ref(null)

function openEdit(user) {
  editedUser.value = user
  editName.value = user.name || ''
  editError.value = ''
  nextTick(() => editInput.value?.focus())
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
    notifySuccess(`Renamed to ${editName.value}`)
    editedUser.value = null
    await loadUsers()
  } catch (err) {
    editError.value = describeApiError(err)
  } finally {
    editing.value = false
  }
}

// ---------------------------------------------------------------------------
// Access, cluster by cluster
// ---------------------------------------------------------------------------
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
  return managedUser.value?.roles.find(r => r.cluster_id === clusterId)?.role || ''
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
</script>

<style scoped>
.usr-who { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
.usr-name { font-weight: 500; color: var(--text-hi); }
.usr-name.none { font-weight: 400; color: var(--text-low); }
.usr-email { font-size: 12px; color: var(--text-mid); overflow-wrap: anywhere; }
.usr-tenant { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); white-space: nowrap; }
.usr-method { white-space: nowrap; }
.usr-method .chip + .chip { margin-left: 4px; }
.usr-when { color: var(--text-mid); white-space: nowrap; font-variant-numeric: tabular-nums; }
.usr-grants { display: flex; flex-wrap: wrap; gap: 4px; }
.usr-none { font-size: 12px; color: var(--text-low); }
.usr-act { text-align: right; white-space: nowrap; }

.acc-loading { display: grid; gap: 8px; padding: 16px; }
.acc-loading .skeleton { height: 34px; }

/* The forms: a label over each field, one short line under it at most. */
.acc-form { display: grid; gap: 16px; }
.acc-field { display: grid; gap: 4px; min-width: 0; }
.acc-label { font-size: 12px; font-weight: 500; color: var(--text-mid); }
.acc-help { margin: 0; font-size: 12px; line-height: 1.45; color: var(--text-low); }
.usr-pair { display: grid; grid-template-columns: minmax(0, 1fr) 150px; gap: 12px; }

/* Access: a row per cluster of the user's tenant. */
.usr-access-card { max-width: 560px; }
.usr-access-list { display: flex; flex-direction: column; }
.usr-access-row {
  display: grid; grid-template-columns: minmax(0, 1fr) 150px 84px; align-items: center; gap: 8px;
  padding: 8px 0; border-top: 1px solid var(--bd-soft);
}
.usr-access-row:first-child { border-top: 0; padding-top: 0; }
.usr-access-row .btn { justify-content: center; }
.usr-cluster { display: flex; align-items: baseline; gap: 8px; min-width: 0; }
.usr-cluster-slug { font-family: var(--font-mono); font-size: 12.5px; color: var(--text-hi); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.usr-cluster-status { font-size: 12px; color: var(--text-low); }
.usr-access-note { margin-top: 12px; }

/* Phone: a row is a small block — who, then tenant and sign-in, then access
   and the buttons — instead of a table wider than the screen. */
@media (max-width: 720px) {
  .usr-table thead { display: none; }
  .usr-table tr {
    display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center;
    gap: 8px 12px; padding: 12px 16px; border-bottom: 1px solid var(--bd-soft);
  }
  .usr-table tr:last-child { border-bottom: 0; }
  .usr-table td { display: block; padding: 0; border: 0; }
  .usr-table .usr-who-cell, .usr-table .usr-access { grid-column: 1 / -1; }
  .usr-table .usr-when { display: none; }
  .usr-act { grid-column: 1 / -1; text-align: left; }
  .usr-pair { grid-template-columns: minmax(0, 1fr); }
  .usr-access-row { grid-template-columns: minmax(0, 1fr) 130px; }
  .usr-access-row .btn { grid-column: 2; }
}
</style>
