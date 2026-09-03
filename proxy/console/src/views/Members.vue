<script setup>
import { ref, onMounted } from 'vue'
import { apiFetch } from '../api.js'
import { formatTimestamp, formatTimestampUtc } from '../format.js'

const members = ref([])
const error = ref(null)
const loading = ref(true)

// Same list console.rs validates against (VALID_ROLES / the cluster_roles
// CHECK constraint). Order is display order, most privileged first.
const ALL_ROLES = ['admin', 'producer', 'consumer', 'viewer']

const newEmail = ref('')
const newRole = ref('viewer')
const granting = ref(false)
const grantError = ref(null)

const confirmingRevoke = ref(null) // email currently in "sure?" state

async function load() {
  loading.value = true
  error.value = null
  try {
    const body = await apiFetch('/api/console/members')
    members.value = Array.isArray(body) ? body : []
  } catch (e) {
    error.value = e.message || String(e)
  } finally {
    loading.value = false
  }
}

onMounted(load)

// Grant is an upsert server-side, so the same call both adds a member and
// changes an existing one's role — the role <select> in the table reuses it.
async function grant(email, role) {
  grantError.value = null
  const address = (email || '').trim()
  if (!address) {
    grantError.value = 'Email is required.'
    return
  }
  granting.value = true
  try {
    await apiFetch('/api/console/members', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: address, role }),
    })
    newEmail.value = ''
    newRole.value = 'viewer'
    await load()
  } catch (e) {
    grantError.value = e.message || String(e)
    await load() // the <select> may be showing a role that was never applied
  } finally {
    granting.value = false
  }
}

async function revoke(email) {
  error.value = null
  try {
    await apiFetch('/api/console/members', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email }),
    })
    confirmingRevoke.value = null
    await load()
  } catch (e) {
    error.value = e.message || String(e)
  }
}

</script>

<template>
  <section>
    <div class="card">
      <h2>Add a member</h2>
      <p class="hint">
        The address must already be a user of this cluster's tenant — the console grants access, it does not create
        accounts.
      </p>
      <div class="form-row">
        <label class="field">
          <span>Email</span>
          <input
            v-model="newEmail"
            type="text"
            placeholder="teammate@example.com"
            @keyup.enter="grant(newEmail, newRole)"
          />
        </label>
        <label class="field">
          <span>Role</span>
          <select v-model="newRole">
            <option v-for="r in ALL_ROLES" :key="r" :value="r">{{ r }}</option>
          </select>
        </label>
        <button class="btn btn-primary" :disabled="granting" @click="grant(newEmail, newRole)">
          {{ granting ? 'Saving…' : 'Grant access' }}
        </button>
      </div>
      <div v-if="grantError" class="banner banner-error">{{ grantError }}</div>
    </div>

    <div class="card">
      <div class="card-head">
        <h2>Members</h2>
        <button class="btn btn-ghost" @click="load">Refresh</button>
      </div>
      <div v-if="loading" class="hint">Loading…</div>
      <div v-else-if="error" class="banner banner-error">{{ error }}</div>
      <div v-else-if="members.length === 0" class="hint">No members yet.</div>
      <table v-else class="list">
        <thead>
          <tr>
            <th>Email</th>
            <th>Role</th>
            <th>Since</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="m in members" :key="m.email">
            <td class="mono">{{ m.email }}</td>
            <td>
              <select :value="m.role" :disabled="granting" @change="grant(m.email, $event.target.value)">
                <option v-for="r in ALL_ROLES" :key="r" :value="r">{{ r }}</option>
              </select>
            </td>
            <td :title="formatTimestampUtc(m.granted_at)">{{ formatTimestamp(m.granted_at) }}</td>
            <td>
              <button v-if="confirmingRevoke !== m.email" class="btn-link danger" @click="confirmingRevoke = m.email">
                Remove
              </button>
              <span v-else class="confirm-inline">
                Sure?
                <button class="btn-link danger" @click="revoke(m.email)">Yes</button>
                <button class="btn-link" @click="confirmingRevoke = null">No</button>
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
