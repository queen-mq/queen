<script setup>
import { ref, onMounted } from 'vue'
import { apiFetch } from '../api.js'

const queues = ref([])
const error = ref(null)
const loading = ref(true)

// Same speculative size-field names src/registry.rs's reconciler already
// checks for on the broker's queues response (no field exists yet as of this
// writing — see that file's comment) — kept in sync so the column lights up
// automatically the day the broker adds one, no console change needed.
const SIZE_KEYS = ['retainedBytes', 'bytes', 'sizeBytes', 'diskBytes']

function sizeOf(q) {
  for (const k of SIZE_KEYS) {
    if (typeof q[k] === 'number') return q[k]
  }
  return null
}

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
    <table v-else class="list">
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
  </section>
</template>
