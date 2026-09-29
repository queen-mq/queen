<template>
  <div class="view-container">

    <PageHead title="API keys">
      <template #sub>
        <span>cluster <b>{{ actingClusterSlug || '—' }}</b> · tenant <b>{{ actingTenantSlug || '—' }}</b></span>
      </template>
      <template #actions>
        <button class="btn btn-primary" @click="openCreate">Create key</button>
      </template>
    </PageHead>

    <div v-if="error" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not load API keys</strong> · {{ describeApiError(error) }}
        <template v-if="loaded"> · showing the last list that loaded</template>
      </span>
    </div>

    <div v-if="created" class="card keys-reveal">
      <div class="card-header">
        <h3>Copy the key for {{ created.name }} now</h3>
        <span class="card-sub">It is shown only this once.</span>
      </div>
      <div class="card-body keys-reveal-body">
        <div class="keys-secret-row">
          <code class="font-mono keys-secret">{{ created.key }}</code>
          <button class="btn" @click="copy(created.key)">{{ copied ? 'Copied' : 'Copy' }}</button>
        </div>
        <div class="keys-usage">
          <span class="label-xs">A service sends it on every call</span>
          <code class="font-mono keys-curl">curl -H "Authorization: Bearer {{ created.key.slice(0, 10) }}…" {{ origin }}/api/v1/resources/queues</code>
        </div>
        <div class="keys-reveal-foot">
          <button class="btn btn-ghost" @click="created = null">I have copied it</button>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Keys</h3>
        <span class="card-sub">{{ activeCount }} active<template v-if="revokedCount"> · {{ revokedCount }} revoked</template></span>
        <span v-if="loading" class="muted">refreshing…</span>
      </div>
      <div class="table-container">
        <table v-if="sortedKeys.length" class="t keys-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Scopes</th>
              <th>Created</th>
              <th>Last used</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="key in sortedKeys" :key="key.id" :class="{ 'keys-revoked': key.revoked_at }">
              <td class="keys-who">
                <span class="keys-name">{{ key.name }}</span>
                <span v-if="key.revoked_at" class="chip chip-mute keys-revoked-chip" :title="formatTimestampUtc(key.revoked_at)">
                  revoked {{ formatRelativeTime(key.revoked_at) }}
                </span>
              </td>
              <td class="keys-scopes-cell">
                <span class="keys-scopes">
                  <span v-for="scope in inScopeOrder(key.scopes)" :key="scope" class="chip chip-mute">{{ scope }}</span>
                </span>
              </td>
              <td class="keys-when keys-created" :title="formatTimestampUtc(key.created_at)">{{ formatTimestamp(key.created_at) }}</td>
              <td class="keys-when keys-used" :title="key.last_used_at ? formatTimestampUtc(key.last_used_at) : undefined">
                {{ key.last_used_at ? formatRelativeTime(key.last_used_at) : 'never' }}
              </td>
              <td class="right keys-act">
                <template v-if="!key.revoked_at">
                  <template v-if="confirming === key.id">
                    <span class="keys-confirm">Calls with it fail at once.</span>
                    <button class="btn btn-danger" :disabled="busy === key.id" @click="revoke(key)">
                      {{ busy === key.id ? 'Revoking…' : 'Revoke' }}
                    </button>
                    <button class="btn btn-ghost" @click="confirming = null">Keep</button>
                  </template>
                  <button v-else class="btn btn-ghost" @click="confirming = key.id">Revoke</button>
                </template>
              </td>
            </tr>
          </tbody>
        </table>

        <div v-else-if="loading && !loaded" class="keys-loading">
          <div v-for="i in 3" :key="i" class="skeleton" />
        </div>
        <div v-else-if="loaded" class="empty-state">
          <h3>No API keys on this cluster</h3>
          <p>A service that pushes or pops from outside the cell needs one.</p>
          <button class="btn btn-primary" @click="openCreate">Create key</button>
        </div>
      </div>
    </div>

    <Teleport to="body">
      <div v-if="showCreate" class="modal-backdrop" @click.self="closeCreate">
        <form class="card modal-card" @submit.prevent="create">
          <div class="card-header"><h3>Create API key</h3></div>
          <div class="card-body keys-form">
            <div v-if="createError" class="panel-err">{{ createError }}</div>
            <label class="keys-field">
              <span class="label-xs">Name</span>
              <input
                ref="nameInput"
                v-model.trim="createForm.name"
                class="input"
                maxlength="120"
                autocomplete="off"
                required
                placeholder="billing-worker"
              />
            </label>
            <fieldset class="keys-field keys-scope-set">
              <legend class="label-xs">Scopes</legend>
              <label v-for="scope in SCOPES" :key="scope.name" class="keys-scope">
                <input v-model="createForm.scopes" type="checkbox" :value="scope.name" />
                <span class="font-mono">{{ scope.name }}</span>
                <span class="keys-scope-can">{{ scope.can }}</span>
              </label>
            </fieldset>
          </div>
          <div class="modal-foot">
            <button type="button" class="btn btn-ghost" @click="closeCreate">Cancel</button>
            <button type="submit" class="btn btn-primary" :disabled="creating || !createForm.name || !createForm.scopes.length">
              {{ creating ? 'Creating…' : 'Create key' }}
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
import { formatRelativeTime } from '@/composables/useApi'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import PageHead from '@/components/PageHead.vue'

// proxy/src/console.rs VALID_SCOPES.
const SCOPES = [
  { name: 'read', can: 'list queues, read messages and stats' },
  { name: 'produce', can: 'push messages' },
  { name: 'consume', can: 'pop and acknowledge messages' },
  { name: 'admin', can: 'configure and delete queues' },
]

const { epoch, actingClusterSlug, actingTenantSlug } = useIdentity()
const { notifySuccess } = useToast()
const origin = window.location.origin

const keys = ref([])
const loading = ref(false)
const loaded = ref(false)
const error = ref(null)
const busy = ref(null) // id of the row with a call in flight
const confirming = ref(null) // id of the row asking "revoke?"
const created = ref(null) // { name, key } — held only until dismissed
const copied = ref(false)

// One order on every row, whatever order a key was created with.
const SCOPE_ORDER = SCOPES.map(s => s.name)
const inScopeOrder = scopes => [...(scopes || [])].sort((a, b) => SCOPE_ORDER.indexOf(a) - SCOPE_ORDER.indexOf(b))

const activeCount = computed(() => keys.value.filter(k => !k.revoked_at).length)
const revokedCount = computed(() => keys.value.length - activeCount.value)
// Live keys first, newest first within each half.
const sortedKeys = computed(() => [...keys.value].sort((a, b) =>
  (!!a.revoked_at - !!b.revoked_at) || String(b.created_at).localeCompare(String(a.created_at))))

async function load() {
  if (loading.value) return
  loading.value = true
  error.value = null
  try {
    const { data } = await access.listKeys()
    keys.value = Array.isArray(data) ? data : []
    loaded.value = true
  } catch (err) {
    error.value = err
  } finally {
    loading.value = false
  }
}

onMounted(load)
useRefresh(load)
// Keys belong to the acting cluster: a switch is a different list, and a key
// just revealed for the old one must not linger on the new one.
watch(epoch, () => {
  keys.value = []
  loaded.value = false
  confirming.value = null
  created.value = null
  load()
})

async function revoke(key) {
  busy.value = key.id
  try {
    await access.revokeKey(key.id)
    notifySuccess(`Revoked ${key.name}`)
    confirming.value = null
  } catch {
    // The shell's failure toast carries the proxy's reason.
  } finally {
    busy.value = null
    await load()
  }
}

async function copy(text) {
  try {
    await navigator.clipboard.writeText(text)
    copied.value = true
    setTimeout(() => { copied.value = false }, 1500)
  } catch {
    // No clipboard (insecure context, denied permission): the key stays on
    // screen, selectable, which is all a copy button is for.
  }
}

const showCreate = ref(false)
const creating = ref(false)
const createError = ref('')
const createForm = reactive({ name: '', scopes: ['read'] })
const nameInput = ref(null)

function openCreate() {
  createForm.name = ''
  createForm.scopes = ['read']
  createError.value = ''
  showCreate.value = true
  nextTick(() => nameInput.value?.focus())
}

function closeCreate() {
  if (!creating.value) showCreate.value = false
}

async function create() {
  creating.value = true
  createError.value = ''
  try {
    const { data } = await access.createKey({ name: createForm.name, scopes: [...createForm.scopes] })
    created.value = { name: createForm.name, key: data.key }
    copied.value = false
    showCreate.value = false
    await load()
  } catch (err) {
    createError.value = describeApiError(err)
  } finally {
    creating.value = false
  }
}
</script>

<style scoped>
.keys-name { font-weight: 550; }
.keys-revoked td { color: var(--text-low); }
.keys-revoked .keys-name { font-weight: 450; }
.keys-revoked-chip { margin-left: 7px; }
.keys-scopes { display: flex; flex-wrap: wrap; gap: 5px; }
.keys-when { color: var(--text-mid); white-space: nowrap; }
.keys-confirm { margin-right: 8px; color: var(--text-mid); font-size: 12px; }
.keys-loading { display: grid; gap: 8px; padding: 16px; }
.keys-loading .skeleton { height: 34px; }
.keys-reveal { margin-bottom: 12px; border-color: var(--warn-400); }
.keys-reveal-body { display: grid; gap: 14px; }
.keys-secret-row { display: flex; align-items: center; gap: 10px; }
.keys-secret {
  flex: 1 1 auto;
  min-width: 0;
  padding: 9px 10px;
  border: 1px solid var(--bd);
  border-radius: var(--r-control);
  background: var(--ink-3);
  color: var(--text-hi);
  font-size: 13px;
  overflow-wrap: anywhere;
  user-select: all;
}
.keys-usage { display: grid; gap: 6px; }
.keys-curl { color: var(--text-mid); font-size: 12px; overflow-wrap: anywhere; }
.keys-reveal-foot { display: flex; justify-content: flex-end; }
.keys-form { display: grid; gap: 14px; }
.keys-field { display: grid; gap: 6px; }
.keys-scope-set { margin: 0; padding: 0; border: 0; }
.keys-scope { display: grid; grid-template-columns: auto 72px 1fr; gap: 8px; align-items: center; padding: 3px 0; cursor: pointer; }
.keys-scope .font-mono { font-size: 12.5px; }
.keys-scope-can { color: var(--text-low); font-size: 12px; }
.right { text-align: right; white-space: nowrap; }
/* Phone: a row is a small block — name, scopes, then last use and revoke —
   instead of a table wider than the screen. The creation date is dropped. */
@media (max-width: 720px) {
  .keys-secret-row { flex-direction: column; align-items: stretch; }
  .keys-table thead { display: none; }
  .keys-table tr {
    display: grid;
    grid-template-columns: 1fr auto;
    align-items: center;
    gap: 8px 10px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--bd-soft);
  }
  .keys-table tr:last-child { border-bottom: 0; }
  .keys-table td { display: block; padding: 0; border: 0; }
  .keys-table .keys-who, .keys-table .keys-scopes-cell { grid-column: 1 / -1; }
  .keys-table .keys-created { display: none; }
  .keys-table .keys-used::before { content: 'last used '; }
  .keys-act { white-space: normal; }
  .keys-confirm { display: block; margin: 0 0 6px; }
}
</style>
