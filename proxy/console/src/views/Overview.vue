<script setup>
import { ref, computed, onMounted } from 'vue'
import { apiFetch } from '../api.js'

const props = defineProps({ overview: { type: Object, default: null } })

const usage = ref([])
const usageError = ref(null)
const usageLoading = ref(true)

// The API accepts 1..168 (console.rs clamp_hours); these are the round points
// inside that range, so the picker can never ask for a window that gets
// silently clamped to something else.
const HOUR_CHOICES = [
  { hours: 1, label: 'Last hour' },
  { hours: 6, label: 'Last 6h' },
  { hours: 24, label: 'Last 24h' },
  { hours: 72, label: 'Last 3 days' },
  { hours: 168, label: 'Last 7 days' },
]
const hours = ref(24)

const hoursLabel = computed(() => HOUR_CHOICES.find((c) => c.hours === hours.value)?.label ?? `Last ${hours.value}h`)

async function loadUsage() {
  usageLoading.value = true
  usageError.value = null
  try {
    usage.value = await apiFetch(`/api/console/usage?hours=${hours.value}`)
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

// --- plan + quota ----------------------------------------------------------

// Month-to-date messages against plans.monthly_msgs_quota. Both come from the
// overview response (queen_proxy.cluster_month_msgs), so the figure includes
// today's not-yet-rolled-up minutes.
const monthMsgs = computed(() => props.overview?.usage?.msgs ?? 0)
const monthQuota = computed(() => props.overview?.plan?.monthly_msgs_quota ?? null)
const quotaPct = computed(() => {
  if (!monthQuota.value) return null
  return Math.min(100, (monthMsgs.value / monthQuota.value) * 100)
})

// Live push-block reason as the proxy itself holds it ('storage' |
// 'monthly_quota' | null) — the same flag gateway.rs turns into a 403, not a
// verdict this page recomputes.
const pushBlock = computed(() => props.overview?.push_block ?? null)
const storageBlocked = computed(() => pushBlock.value === 'storage')
const maxRetained = computed(() => props.overview?.storage?.max_retained_bytes ?? null)

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
    <div class="card">
      <div class="card-head">
        <h2>Plan &amp; quota</h2>
        <span v-if="overview" class="pill mono">{{ overview.plan.code }}</span>
      </div>

      <div v-if="storageBlocked" class="banner banner-error">
        <strong>Pushes are being rejected</strong>: retained data is over this plan's
        {{ fmtBytes(maxRetained) }} storage cap. Consumes still work. Delete or drain queues — the Queues tab shows
        retained bytes per queue against the cap.
      </div>
      <div v-else-if="pushBlock === 'monthly_quota'" class="banner banner-error">
        <strong>Pushes are being rejected</strong>: this cluster has used its
        {{ monthQuota ? monthQuota.toLocaleString() : '' }} messages for {{ overview.usage.month }}. Consumes still
        work, and the counter resets at the start of next month.
      </div>
      <div v-else-if="overview && overview.status === 'suspended'" class="banner banner-error">
        <strong>This cluster is suspended</strong>: the whole data plane answers 403 until it is reactivated.
      </div>
      <div v-else-if="overview && overview.status === 'push_blocked'" class="banner banner-warn">
        <p>
          <strong>Pushes are blocked</strong> for this cluster. Consumes still work. No storage or monthly-message
          quota is currently over, so this block came from the control plane (billing or an operator action).
        </p>
      </div>

      <table v-if="overview" class="kv">
        <tbody>
          <tr>
            <td class="dim">Messages this month ({{ overview.usage.month }})</td>
            <td>
              <template v-if="monthQuota">
                {{ monthMsgs.toLocaleString() }} of {{ monthQuota.toLocaleString() }}
                <span v-if="pushBlock === 'monthly_quota'" class="pill pill-off">Quota reached</span>
                <div class="meter" :title="`${quotaPct.toFixed(1)}% of the monthly quota`">
                  <div class="meter-fill" :class="{ 'meter-hot': quotaPct >= 90 }" :style="{ width: quotaPct + '%' }" />
                </div>
              </template>
              <template v-else>{{ monthMsgs.toLocaleString() }} (no monthly quota)</template>
            </td>
          </tr>
          <tr>
            <td class="dim">Retained storage cap</td>
            <td>
              {{ fmtBytes(maxRetained) }}
              <span v-if="storageBlocked" class="pill pill-off">Over quota</span>
              <span v-else-if="maxRetained" class="pill pill-on">Within cap</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

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
      <div class="card-head">
        <h2>Usage — {{ hoursLabel.toLowerCase() }}</h2>
        <select v-model.number="hours" @change="loadUsage">
          <option v-for="c in HOUR_CHOICES" :key="c.hours" :value="c.hours">{{ c.label }}</option>
        </select>
      </div>
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
