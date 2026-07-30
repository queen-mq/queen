<script setup>
import { ref, onMounted } from 'vue'
import { apiFetch } from '../api.js'

const keys = ref([])
const error = ref(null)
const loading = ref(true)

const ALL_SCOPES = ['produce', 'consume', 'admin', 'read']
const newName = ref('')
const newScopes = ref(new Set(['read']))
const creating = ref(false)
const createError = ref(null)
const justCreated = ref(null) // { id, key }
const copyLabel = ref('Copy')

const confirmingRevoke = ref(null) // id of the key currently in "sure?" state

function toggleScope(s) {
  if (newScopes.value.has(s)) newScopes.value.delete(s)
  else newScopes.value.add(s)
}

async function load() {
  loading.value = true
  error.value = null
  try {
    const body = await apiFetch('/api/console/keys')
    keys.value = Array.isArray(body) ? body : []
  } catch (e) {
    error.value = e.message || String(e)
  } finally {
    loading.value = false
  }
}

onMounted(load)

async function createKey() {
  createError.value = null
  if (!newName.value.trim()) {
    createError.value = 'Name is required.'
    return
  }
  if (newScopes.value.size === 0) {
    createError.value = 'Pick at least one scope.'
    return
  }
  creating.value = true
  try {
    const body = await apiFetch('/api/console/keys', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newName.value.trim(), scopes: Array.from(newScopes.value) }),
    })
    justCreated.value = body
    newName.value = ''
    newScopes.value = new Set(['read'])
    await load()
  } catch (e) {
    createError.value = e.message || String(e)
  } finally {
    creating.value = false
  }
}

async function copyKey() {
  if (!justCreated.value) return
  try {
    await navigator.clipboard.writeText(justCreated.value.key)
    copyLabel.value = 'Copied!'
    setTimeout(() => (copyLabel.value = 'Copy'), 1500)
  } catch {
    copyLabel.value = 'Select & copy manually'
  }
}

async function revoke(id) {
  try {
    await apiFetch(`/api/console/keys/${id}`, { method: 'DELETE' })
    confirmingRevoke.value = null
    await load()
  } catch (e) {
    error.value = e.message || String(e)
  }
}

function fmtDate(s) {
  if (!s) return '—'
  return new Date(s).toLocaleString()
}
</script>

<template>
  <section>
    <div class="card">
      <h2>Create API key</h2>

      <div v-if="justCreated" class="banner banner-warn">
        <p><strong>Copy this key now — it will not be shown again.</strong></p>
        <div class="key-reveal">
          <code class="mono">{{ justCreated.key }}</code>
          <button class="btn btn-ghost" @click="copyKey">{{ copyLabel }}</button>
        </div>
        <button class="btn-link" @click="justCreated = null">Dismiss</button>
      </div>

      <div class="form-row">
        <label class="field">
          <span>Name</span>
          <input v-model="newName" type="text" placeholder="e.g. billing-worker" @keyup.enter="createKey" />
        </label>
        <div class="field">
          <span>Scopes</span>
          <div class="scope-picker">
            <label v-for="s in ALL_SCOPES" :key="s" class="scope-chip">
              <input type="checkbox" :checked="newScopes.has(s)" @change="toggleScope(s)" />
              {{ s }}
            </label>
          </div>
        </div>
        <button class="btn btn-primary" :disabled="creating" @click="createKey">
          {{ creating ? 'Creating…' : 'Create key' }}
        </button>
      </div>
      <div v-if="createError" class="banner banner-error">{{ createError }}</div>
    </div>

    <div class="card">
      <div class="card-head">
        <h2>API keys</h2>
        <button class="btn btn-ghost" @click="load">Refresh</button>
      </div>
      <div v-if="loading" class="hint">Loading…</div>
      <div v-else-if="error" class="banner banner-error">{{ error }}</div>
      <div v-else-if="keys.length === 0" class="hint">No API keys yet.</div>
      <table v-else class="list">
        <thead>
          <tr>
            <th>Name</th>
            <th>Scopes</th>
            <th>Created</th>
            <th>Last used</th>
            <th>Status</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="k in keys" :key="k.id">
            <td>{{ k.name }}</td>
            <td class="mono">{{ k.scopes.join(', ') }}</td>
            <td>{{ fmtDate(k.created_at) }}</td>
            <td>{{ fmtDate(k.last_used_at) }}</td>
            <td>
              <span v-if="k.revoked_at" class="pill pill-off">Revoked</span>
              <span v-else class="pill pill-on">Active</span>
            </td>
            <td>
              <template v-if="!k.revoked_at">
                <button v-if="confirmingRevoke !== k.id" class="btn-link danger" @click="confirmingRevoke = k.id">
                  Revoke
                </button>
                <span v-else class="confirm-inline">
                  Sure?
                  <button class="btn-link danger" @click="revoke(k.id)">Yes</button>
                  <button class="btn-link" @click="confirmingRevoke = null">No</button>
                </span>
              </template>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
