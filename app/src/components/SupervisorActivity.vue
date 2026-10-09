<template>
  <!--
    The queue chart and the queues an application reports, side by side. The
    history is the queue's on this cluster, for all of its consumers: the
    heading's title says so, once, instead of small print under every block.
  -->
  <section class="supervisor-activity" aria-label="Queue activity on the current cluster">
    <div class="activity-chart">
      <div class="sect-head">
        <h3>Queue activity</h3>
        <span v-if="queue" class="activity-scope" :title="scopeTitle"><span class="mono">{{ queue }}</span> · last 60 min · all consumers</span>
        <div class="seg activity-mode" role="group" aria-label="Chart metric">
          <button :class="{ on: mode === 'traffic' }" :aria-pressed="mode === 'traffic'" @click="mode = 'traffic'">Traffic</button>
          <button :class="{ on: mode === 'backlog' }" :aria-pressed="mode === 'backlog'" @click="mode = 'backlog'">Backlog</button>
        </div>
      </div>
      <template v-if="queue">
        <div v-if="loading" class="activity-placeholder" role="status">Reading queue history…</div>
        <div v-else-if="error" class="activity-placeholder" role="status"><strong>Queue history unavailable</strong><span>{{ error }}</span></div>
        <template v-else-if="metrics">
          <div class="activity-plot" role="img" :aria-label="chartDescription">
            <BaseChart v-if="hasSamples" :key="mode" :data="chartData" :options="chartOptions" height="96px" />
            <div v-else class="activity-placeholder">No {{ mode === 'traffic' ? 'traffic' : 'backlog' }} samples in this window</div>
          </div>
          <div class="activity-legend">
            <template v-if="mode === 'traffic'"><span><i />Incoming</span><span><i class="dashed" />Delivered</span><span class="unit">messages / min</span></template>
            <template v-else><span><i />Pending messages</span><span class="unit">sampled depth</span></template>
          </div>
        </template>
        <div class="stat-grid stat-grid-3 activity-stats" :title="sampleNote">
          <div class="stat">
            <div class="stat-label">{{ mode === 'traffic' ? 'Incoming / min' : 'Pending' }}</div>
            <div class="stat-value">{{ number(mode === 'traffic' ? metrics?.incoming : metrics?.pending) }}</div>
          </div>
          <div class="stat">
            <div class="stat-label">{{ mode === 'traffic' ? 'Delivered / min' : 'Pending Δ' }}</div>
            <div class="stat-value">{{ mode === 'traffic' ? number(metrics?.delivered) : delta(metrics?.pendingDelta) }}</div>
          </div>
          <div class="stat" title="Ack failures in the complete buckets of this hour">
            <div class="stat-label">Ack failures</div>
            <div class="stat-value" :class="{ warn: metrics?.ackFailures }">{{ number(metrics?.ackFailures) }}</div>
            <div class="stat-foot">this hour</div>
          </div>
        </div>
      </template>
      <div v-else class="activity-placeholder">No named queue reported. Dynamic consumers show their scope in the instance details.</div>
    </div>
    <div class="activity-queues">
      <div class="sect-head">
        <h3>Reported queues</h3>
        <span>{{ queues.length }}</span>
        <RouterLink v-if="queue" class="activity-open" :to="queueLocation(queue, route)">Open queue →</RouterLink>
      </div>
      <div v-if="queues.length" class="queue-table">
        <table class="t">
          <thead><tr><th>Queue</th><th class="right">Workers</th></tr></thead>
          <tbody>
            <tr v-for="item in queues" :key="item.name" :class="{ selected: queue === item.name }">
              <td>
                <button class="row-open" :aria-pressed="queue === item.name" :aria-label="`Show activity for ${item.name}`" :title="item.name" @click="queue = item.name"><span v-if="item.tone === 'warn' || item.tone === 'bad'" class="g" :class="item.tone" aria-hidden="true" />{{ item.name }}</button>
              </td>
              <td class="num right">{{ number(item.running) }} <span class="of">/ {{ number(item.desired) }}</span></td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-else class="queue-list-empty">Queue allocation unavailable</p>
    </div>
  </section>
</template>

<script setup>
import { queueLocation } from '@/composables/navigation'
const route = useRoute()
import { useRoute } from 'vue-router'
import { computed, inject, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import BaseChart from '@/components/BaseChart.vue'
import { supervisorActivityKey } from '@/composables/supervisorActivity'
import { chartTheme } from '@/composables/useChartTheme'
import { describeApiError } from '@/api'
import { useIdentity } from '@/stores/identity'
const props = defineProps({ queues: { type: Array, required: true }, readAt: Number })
const reader = inject(supervisorActivityKey)
const { actingClusterSlug } = useIdentity()
const queue = ref(null), mode = ref('traffic'), metrics = shallowRef(null), loading = ref(false), error = ref('')
let sequence = 0
watch(() => props.queues, queues => {
  if (!queues.some(item => item.name === queue.value)) queue.value = queues[0]?.name || null
}, { immediate: true })
watch([queue, () => props.readAt], async () => {
  const turn = ++sequence
  metrics.value = null; error.value = ''; loading.value = false
  if (!queue.value || !props.readAt) return
  loading.value = true
  try {
    const result = await reader.load(queue.value)
    if (turn === sequence) metrics.value = result
  } catch (failure) {
    if (turn === sequence && failure.name !== 'AbortError') error.value = describeApiError(failure)
  } finally { if (turn === sequence) loading.value = false }
}, { immediate: true })
onBeforeUnmount(() => { sequence++ })
const number = value => value == null ? '—' : value.toLocaleString(undefined, { maximumFractionDigits: 1 })
const delta = value => value == null ? '—' : `${value > 0 ? '+' : ''}${number(value)}`
const time = value => new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
const hasSamples = computed(() => Boolean(mode.value === 'traffic' ? metrics.value?.trafficSamples : metrics.value?.backlogSamples))
// An isolated sample needs a dot: drawing only segments would hide it.
const isolatedPoint = context => context.dataset.data[context.dataIndex] != null && context.dataset.data[context.dataIndex - 1] == null && context.dataset.data[context.dataIndex + 1] == null ? 2 : 0
const chartData = computed(() => ({
  labels: (metrics.value?.points || []).map(point => time(point.at)),
  datasets: mode.value === 'traffic' ? [
    { label: 'Incoming / min', data: metrics.value?.points.map(point => point.incoming), fill: false, pointRadius: isolatedPoint },
    { label: 'Delivered / min', data: metrics.value?.points.map(point => point.delivered), fill: false, borderDash: [4, 3], pointRadius: isolatedPoint },
  ] : [{ label: 'Pending', data: metrics.value?.points.map(point => point.pending), fill: true, pointRadius: isolatedPoint }],
}))
const chartOptions = computed(() => ({ scales: { x: { grid: { display: false }, ticks: { color: chartTheme.tick, maxTicksLimit: 5, maxRotation: 0, font: { size: 9 } } }, y: { ticks: { color: chartTheme.tick, maxTicksLimit: 3, precision: 0, font: { size: 9 } } } } }))
const chartDescription = computed(() => `${mode.value === 'traffic' ? 'Incoming and delivered messages per minute' : 'Pending messages'} over the last hour for ${queue.value}, all consumers on ${actingClusterSlug.value || 'the current cluster'}. Missing samples appear as gaps.`)
const scopeTitle = 'History covers this queue on the current cluster, for all of its consumers. A supervisor may use a different connection.'
const sampleNote = computed(() => {
  if (!metrics.value) return 'Queue metrics are independent of the supervisor heartbeat.'
  if (mode.value === 'backlog') return `Latest depth ${metrics.value.pendingAt ? time(metrics.value.pendingAt) : 'unavailable'} · Δ across available samples`
  const latest = metrics.value.points.at(-1)?.at
  return `Rates ${latest ? `${time(latest)}–${time(latest + metrics.value.minutes * 60_000)}` : 'unavailable'} · Gaps mean no samples`
})
</script>

<style scoped>
.supervisor-activity { display: grid; grid-template-columns: minmax(0, 1.6fr) minmax(220px, 1fr); gap: 24px; padding-top: 14px; min-width: 0; }
.activity-chart, .activity-queues { min-width: 0; }
.sect-head { align-items: center; margin-bottom: 10px; min-height: 26px; }
.activity-scope { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.activity-mode, .activity-open { margin-left: auto; }
.activity-open { font-size: 12px; color: var(--text-mid); text-decoration: none; white-space: nowrap; }
.activity-open:hover { color: var(--text-hi); }
.activity-open:focus-visible, .row-open:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; }
.activity-plot { min-height: 96px; }
.activity-placeholder { min-height: 96px; display: flex; flex-direction: column; justify-content: center; gap: 4px; color: var(--text-low); font-size: 12px; line-height: 1.5; }
.activity-placeholder strong { font-weight: 500; color: var(--text-mid); }
.activity-placeholder span { overflow-wrap: anywhere; }
.activity-legend { display: flex; align-items: center; gap: 14px; margin: 4px 0 12px; color: var(--text-mid); font-size: 11px; }
.activity-legend > span { display: flex; align-items: center; gap: 6px; }
.activity-legend i { width: 12px; border-top: 1.4px solid var(--series-1); }
.activity-legend i.dashed { border-top-style: dashed; border-color: var(--series-2); }
.activity-legend .unit { margin-left: auto; color: var(--text-low); }
.activity-stats { padding-top: 12px; border-top: 1px solid var(--bd); }
.stat-value.warn { color: var(--warn-400); }
.queue-table { max-height: 232px; overflow-y: auto; scrollbar-width: thin; border: 1px solid var(--bd); border-radius: var(--r-card); }
.queue-table .t { width: 100%; }
.queue-table tr.selected td { background: color-mix(in srgb, var(--text-hi) 4%, transparent); }
.right { text-align: right; }
.row-open { display: inline-flex; align-items: center; gap: 8px; max-width: 100%; padding: 0; border: 0; background: none; font: inherit; color: var(--text-mid); text-align: left; cursor: pointer; overflow-wrap: anywhere; }
.row-open:hover, tr.selected .row-open { color: var(--text-hi); }
.of { color: var(--text-low); }
.queue-list-empty { margin: 0; font-size: 12px; color: var(--text-low); }
@media (max-width: 900px) { .supervisor-activity { grid-template-columns: minmax(0, 1fr); gap: 16px; } }
</style>
