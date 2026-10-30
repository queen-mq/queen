<template>
  <SubSidebar label="API keys" :current="title">
    <template #side><ApiKeysSide :keys="keys" :loaded="loaded" :error="error" /></template>

    <PaneHead :title="title" :facts="facts">
      <template #actions>
        <button class="btn btn-primary" @click="openCreate">Create key</button>
      </template>
    </PaneHead>

    <div class="view-container keys">
      <div v-if="error" class="status-banner banner-bad view-banner">
        <span>
          <strong>Could not load API keys</strong> · {{ describeApiError(error) }}<template v-if="loaded"> · showing the last list</template>
        </span>
      </div>

      <!-- A new key, shown this once: the proxy keeps only its hash. -->
      <section v-if="created" class="card keys-reveal" aria-live="polite">
        <div class="card-header">
          <h3>Copy the key for {{ created.name }}</h3>
          <span class="card-sub">shown only this once</span>
        </div>
        <div class="card-body keys-reveal-body">
          <div class="keys-secret-row">
            <code class="keys-secret">{{ created.key }}</code>
            <button class="btn" @click="copy(created.key)">
              <Icon :name="copied ? 'check' : 'copy'" :size="14" />{{ copied ? 'Copied' : 'Copy' }}
            </button>
          </div>
          <code class="keys-curl" title="A service sends the key on every call">curl -H "Authorization: Bearer {{ created.key.slice(0, 10) }}…" {{ origin }}/api/v1/resources/queues</code>
          <div class="keys-reveal-foot">
            <button class="btn btn-ghost" @click="created = null">I have copied it</button>
          </div>
        </div>
      </section>

      <div class="card">
        <div v-if="shownKeys.length" class="table-container">
          <table class="t keys-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Scopes</th>
                <th>Created</th>
                <th>Last used</th>
                <th><span class="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="key in shownKeys" :key="key.id" :class="{ 'keys-revoked': key.revoked_at }">
                <td class="keys-who">
                  <span class="keys-name">{{ key.name }}</span>
                  <span v-if="key.revoked_at" class="keys-gone" :title="formatTimestampUtc(key.revoked_at)">revoked {{ formatRelativeTime(key.revoked_at) }}</span>
                </td>
                <td class="keys-scopes-cell">
                  <span class="keys-scopes">
                    <span v-for="scope in inScopeOrder(key.scopes)" :key="scope" class="chip chip-mute" :title="scopeCan(scope)">{{ scope }}</span>
                  </span>
                </td>
                <td class="keys-when keys-created" :title="formatTimestampUtc(key.created_at)">{{ day(key.created_at) }}</td>
                <td class="keys-when keys-used" :title="key.last_used_at ? formatTimestampUtc(key.last_used_at) : undefined">
                  {{ key.last_used_at ? formatRelativeTime(key.last_used_at) : 'never' }}
                </td>
                <td class="keys-act">
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
        </div>

        <div v-else-if="loading && !loaded" class="acc-loading">
          <div v-for="i in 3" :key="i" class="skeleton" />
        </div>
        <div v-else-if="keys.length" class="empty-state">
          <Icon name="key-round" :size="28" class="empty-state-icon" />
          <p>{{ pick === 'active' ? 'Every key on this cluster is revoked' : 'No key on this cluster is revoked' }}</p>
          <button class="btn btn-ghost" @click="state = ''">Show all keys</button>
        </div>
        <div v-else-if="loaded" class="empty-state">
          <Icon name="key-round" :size="28" class="empty-state-icon" />
          <p title="A service that pushes or pops from outside the cell needs one">No API keys on this cluster</p>
          <button class="btn" @click="openCreate">Create key</button>
        </div>
        <div v-else-if="error" class="empty-state">
          <Icon name="key-round" :size="28" class="empty-state-icon" />
          <p>Cannot read the API keys</p>
          <button class="btn btn-ghost" @click="load">Retry</button>
        </div>
      </div>
    </div>

    <Teleport to="body">
      <div v-if="showCreate" class="modal-backdrop" @click.self="closeCreate">
        <form class="card modal-card" @submit.prevent="create">
          <div class="card-header"><h3>Create API key</h3></div>
          <div class="card-body acc-form">
            <div v-if="createError" class="panel-err">{{ createError }}</div>
            <label class="acc-field">
              <span class="acc-label">Name</span>
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
            <fieldset class="acc-field keys-scope-set">
              <legend class="acc-label">Scopes</legend>
              <UiCheck
                v-for="scope in SCOPES"
                :key="scope.name"
                class="keys-scope"
                :model-value="createForm.scopes.includes(scope.name)"
                @update:model-value="setScope(scope.name, $event)"
              >
                <span class="keys-scope-name">{{ scope.name }}</span>
                <span class="keys-scope-can">{{ scope.can }}</span>
              </UiCheck>
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
  </SubSidebar>
</template>

<script setup>
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'

import { access, describeApiError } from '@/api'
import { SCOPES, filterKeys, inScopeOrder, keyPickTitle, pickKeyState } from '@/composables/accessGroups'
import { formatRelativeTime } from '@/composables/useApi'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import ApiKeysSide from '@/components/ApiKeysSide.vue'
import Icon from '@/components/Icon.vue'
import PaneHead from '@/components/PaneHead.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import UiCheck from '@/components/UiCheck.vue'

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

const scopeCan = name => SCOPES.find(s => s.name === name)?.can || ''
// A creation is a day; the tooltip keeps the instant.
const day = v => formatTimestamp(v, 'en-US', { hour: undefined, minute: undefined, second: undefined })

// Live keys first, newest first within each half.
const sortedKeys = computed(() => [...keys.value].sort((a, b) =>
  (!!a.revoked_at - !!b.revoked_at) || String(b.created_at).localeCompare(String(a.created_at))))
// The state picked in the list (ApiKeysSide): the same address field, so the
// list and the table show the same keys. Anything but active or revoked (the
// proxy has no expiry) reads as all keys.
const state = ref('')
useRouteState({ state })
const pick = computed(() => pickKeyState(state.value))
const shownKeys = computed(() => filterKeys(sortedKeys.value, pick.value))

const title = computed(() => keyPickTitle(state.value))
const neverUsed = computed(() => keys.value.filter(k => !k.revoked_at && !k.last_used_at).length)
const facts = computed(() => [
  {
    k: 'Cluster',
    v: actingClusterSlug.value || '—',
    title: `Credentials services use to call cluster ${actingClusterSlug.value || '—'} of tenant ${actingTenantSlug.value || '—'}`,
  },
  loaded.value && neverUsed.value
    ? { k: 'Never used', v: String(neverUsed.value), title: 'Active keys no call has used yet' }
    : null,
].filter(Boolean))

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

function setScope(name, on) {
  const rest = createForm.scopes.filter(s => s !== name)
  createForm.scopes = on ? inScopeOrder([...rest, name]) : rest
}

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
.keys { display: flex; flex-direction: column; gap: 16px; }
.keys > .status-banner { margin-bottom: 0; }

.keys-name { font-weight: 500; color: var(--text-hi); }
.keys-revoked td { color: var(--text-low); }
.keys-revoked .keys-name { font-weight: 400; color: var(--text-mid); }
.keys-gone { margin-left: 8px; font-size: 12px; color: var(--text-low); }
.keys-scopes { display: flex; flex-wrap: wrap; gap: 4px; }
.keys-when { color: var(--text-mid); white-space: nowrap; font-variant-numeric: tabular-nums; }
.keys-act { text-align: right; white-space: nowrap; }
.keys-confirm { margin-right: 8px; font-size: 12px; color: var(--text-mid); }

/* The one-time secret: a well to select from, the copy beside it. */
.keys-reveal { border-color: var(--bd-hi); }
.keys-reveal-body { display: grid; gap: 12px; }
.keys-secret-row { display: flex; align-items: center; gap: 8px; }
.keys-secret {
  flex: 1 1 auto; min-width: 0;
  padding: 8px 12px; border: 1px solid var(--bd); border-radius: var(--r-control);
  background: var(--ink-3); color: var(--text-hi);
  font-family: var(--font-mono); font-size: 13px; overflow-wrap: anywhere; user-select: all;
}
.keys-curl { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); overflow-wrap: anywhere; }
.keys-reveal-foot { display: flex; justify-content: flex-end; }

.acc-loading { display: grid; gap: 8px; padding: 16px; }
.acc-loading .skeleton { height: 34px; }

/* The form: a label over each field. */
.acc-form { display: grid; gap: 16px; }
.acc-field { display: grid; gap: 4px; }
.acc-label { font-size: 12px; font-weight: 500; color: var(--text-mid); }
.keys-scope-set { margin: 0; padding: 0; border: 0; gap: 2px; }
.keys-scope-set legend { padding: 0; margin-bottom: 4px; }
.keys-scope { padding: 4px 0; }
.keys-scope-name { display: inline-block; width: 64px; font-family: var(--font-mono); font-size: 12.5px; color: var(--text-hi); }
.keys-scope-can { font-size: 12px; color: var(--text-low); }

/* Phone: a row is a small block — name, scopes, then last use and revoke —
   instead of a table wider than the screen. The creation date is dropped. */
@media (max-width: 720px) {
  .keys-secret-row { flex-direction: column; align-items: stretch; }
  .keys-table thead { display: none; }
  .keys-table tr {
    display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center;
    gap: 8px 12px; padding: 12px 16px; border-bottom: 1px solid var(--bd-soft);
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
