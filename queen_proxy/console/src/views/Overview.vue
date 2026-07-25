<script setup>
import { ref, computed, onMounted } from 'vue'
import { apiFetch } from '../api.js'

defineProps({ overview: { type: Object, default: null } })

const usage = ref([])
const usageError = ref(null)
const usageLoading = ref(true)

async function loadUsage() {
  usageLoading.value = true
  usageError.value = null
  try {
    usage.value = await apiFetch('/api/console/usage?hours=24')
  } catch (e) {
    usageError.value = e.message || String(e)
  } finally {
    usageLoading.value = false
  }
}

onMounted(loadUsage)

// usage_minutes has one row per (minute, op_class) — sum across op classes
// for the per-minute totals the sparkline + counters need.
const byMinute = computed(() => {
  const m = new Map()
  for (const row of usage.value) {
    const cur = m.get(row.minute) || { minute: row.minute, msgs: 0, reqs: 0, bytes_in: 0, bytes_out: 0 }
    cur.msgs += row.msgs
    cur.reqs += row.reqs
    cur.bytes_in += row.bytes_in
    cur.bytes_out += row.bytes_out
    m.set(row.minute, cur)
  }
  return Array.from(m.values()).sort((a, b) => a.minute.localeCompare(b.minute))
})

const totals = computed(() =>
  usage.value.reduce(
    (acc, r) => ({
      reqs: acc.reqs + r.reqs,
      msgs: acc.msgs + r.msgs,
      bytes_in: acc.bytes_in + r.bytes_in,
      bytes_out: acc.bytes_out + r.bytes_out,
    }),
    { reqs: 0, msgs: 0, bytes_in: 0, bytes_out: 0 },
  ),
)

const SPARK_W = 600
const SPARK_H = 80

const sparkPoints = computed(() => {
  const pts = byMinute.value
  if (pts.length === 0) return ''
  const max = Math.max(1, ...pts.map((p) => p.msgs))
  const step = pts.length > 1 ? SPARK_W / (pts.length - 1) : 0
  return pts
    .map((p, i) => {
      const x = pts.length > 1 ? i * step : 0
      const y = SPARK_H - (p.msgs / max) * SPARK_H
      return `${x.toFixed(1)},${y.toFixed(1)}`
    })
    .join(' ')
})

function fmtBytes(n) {
  if (n === null || n === undefined) return 'Unlimited'
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

// Insertion order here is display order.
const LIMIT_LABELS = {
  max_req_per_sec: 'Requests / sec',
  req_burst: 'Request burst',
  max_msgs_per_sec: 'Messages / sec',
  msgs_burst: 'Message burst',
  max_queues: 'Queues',
  max_partitions_per_queue: 'Partitions / queue',
  max_parked_pops: 'Parked consumers',
  max_payload_bytes: 'Max payload size',
  max_batch_items: 'Max batch items',
  max_retained_bytes: 'Retained storage',
  max_retention_seconds: 'Retention',
}
const BYTE_KEYS = new Set(['max_payload_bytes', 'max_retained_bytes'])
const SECONDS_KEYS = new Set(['max_retention_seconds'])

function fmtLimit(key, value) {
  if (value === null || value === undefined) return 'Unlimited'
  if (BYTE_KEYS.has(key)) return fmtBytes(value)
  if (SECONDS_KEYS.has(key)) return `${Math.round(value / 86400)} days`
  return value.toLocaleString()
}
</script>

<template>
  <section>
    <div class="grid-2">
      <div class="card">
        <h2>Plan limits</h2>
        <table class="kv">
          <tbody>
            <tr v-for="(label, key) in LIMIT_LABELS" :key="key">
              <td class="dim">{{ label }}</td>
              <td>{{ overview ? fmtLimit(key, overview.limits[key]) : '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="card">
        <h2>Features</h2>
        <table v-if="overview" class="kv">
          <tbody>
            <tr>
              <td class="dim">Streams</td>
              <td>
                <span :class="['pill', overview.features.streams ? 'pill-on' : '']">
                  {{ overview.features.streams ? 'On' : 'Off' }}
                </span>
              </td>
            </tr>
            <tr>
              <td class="dim">Traces</td>
              <td>
                <span :class="['pill', overview.features.traces ? 'pill-on' : '']">
                  {{ overview.features.traces ? 'On' : 'Off' }}
                </span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h2>Usage — last 24h</h2>
      <div v-if="usageLoading" class="hint">Loading…</div>
      <div v-else-if="usageError" class="banner banner-error">{{ usageError }}</div>
      <template v-else>
        <div class="counters">
          <div class="counter">
            <span class="counter-value">{{ totals.reqs.toLocaleString() }}</span>
            <span class="counter-label">requests</span>
          </div>
          <div class="counter">
            <span class="counter-value">{{ totals.msgs.toLocaleString() }}</span>
            <span class="counter-label">messages</span>
          </div>
          <div class="counter">
            <span class="counter-value">{{ fmtBytes(totals.bytes_in) }}</span>
            <span class="counter-label">in</span>
          </div>
          <div class="counter">
            <span class="counter-value">{{ fmtBytes(totals.bytes_out) }}</span>
            <span class="counter-label">out</span>
          </div>
        </div>
        <svg
          v-if="byMinute.length > 1"
          class="spark"
          :viewBox="`0 0 ${SPARK_W} ${SPARK_H}`"
          preserveAspectRatio="none"
        >
          <polyline :points="sparkPoints" fill="none" stroke="var(--accent)" stroke-width="2" />
        </svg>
        <p v-else class="hint">Not enough data yet for a graph — msgs/min will appear here once traffic flows.</p>
      </template>
    </div>
  </section>
</template>
