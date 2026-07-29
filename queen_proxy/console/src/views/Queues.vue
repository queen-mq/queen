<script setup>
import { ref, computed, onMounted } from 'vue'
import { apiFetch } from '../api.js'

const props = defineProps({ overview: { type: Object, default: null } })

const queues = ref([])
const error = ref(null)
const loading = ref(true)

// `retainedBytes` per (tenant, queue) is what the broker emits since Track B2
// (server/sql/procedures/013_stats.sql), refreshed at its stats cadence — the
// same field src/registry.rs's reconciler sums for the storage quota. A cell
// predating B2 sends no such field, and the column then reads "—" rather than
// claiming zero.
function sizeOf(q) {
  return typeof q.retainedBytes === 'number' ? q.retainedBytes : null
}

// Displayed against the plan cap so a tenant can see which queues are holding
// the bytes. The quota VERDICT is not recomputed here: it is
// overview.storage.over_quota, the reconciler's own decision (with hysteresis),
// which is what actually gates pushes.
const retainedTotal = computed(() => {
  let sum = 0
  let found = false
  for (const q of queues.value) {
    const b = sizeOf(q)
    if (b !== null) {
      sum += b
      found = true
    }
  }
  return found ? sum : null
})

const maxRetained = computed(() => props.overview?.storage?.max_retained_bytes ?? null)

function fmtBytes(n) {
  if (n === null || n === undefined) return '—'
  if (n < 1024) return `${n} B`
  const units = ['KiB', 'MiB', 'GiB', 'TiB']
  let v = n
  let u = -1
  do {
    v /= 1024
    u++
  } while (v >= 1024 && u < units.length - 1)
  return `${v.toFixed(1)} ${units[u]}`
}

async function load() {
  loading.value = true
  error.value = null
  try {
    // Same-origin broker read, proxied by gateway::handle with the SPA's own
    // Bearer session token (task spec: "stesso origin ... con Bearer da
    // /auth/session-token").
    const body = await apiFetch('/api/v1/resources/queues')
    queues.value = Array.isArray(body?.queues) ? body.queues : Array.isArray(body) ? body : []
  } catch (e) {
    error.value = e.message || String(e)
  } finally {
    loading.value = false
  }
}

onMounted(load)
</script>

<template>
  <section class="card">
    <div class="card-head">
      <h2>Queues</h2>
      <button class="btn btn-ghost" @click="load">Refresh</button>
    </div>
    <div v-if="loading" class="hint">Loading…</div>
    <div v-else-if="error" class="banner banner-error">{{ error }}</div>
    <div v-else-if="queues.length === 0" class="hint">No queues yet.</div>
    <template v-else>
      <p class="hint">
        Retained: <strong>{{ fmtBytes(retainedTotal) }}</strong>
        <template v-if="maxRetained"> of {{ fmtBytes(maxRetained) }} allowed by this plan</template>
        <template v-else> (no storage cap on this plan)</template>
        <span v-if="props.overview?.storage?.over_quota" class="pill pill-off">Pushes blocked</span>
      </p>
      <table class="list">
        <thead>
          <tr>
            <th>Name</th>
            <th>Partitions</th>
            <th>Size</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="q in queues" :key="q.name">
            <td class="mono">{{ q.name }}</td>
            <td>{{ q.partitions ?? '—' }}</td>
            <td>{{ fmtBytes(sizeOf(q)) }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>
