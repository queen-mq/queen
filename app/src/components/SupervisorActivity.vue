<template>
  <section class="deployment-activity" aria-label="Queue activity on the current cluster">
    <div class="activity-chart">
      <div class="activity-heading">
        <div><span class="section-label">Queue activity</span><span class="activity-window">Last 60 min</span></div>
        <div class="activity-tabs" aria-label="Chart metric">
          <button :aria-pressed="mode === 'traffic'" @click="mode = 'traffic'">Traffic</button>
          <button :aria-pressed="mode === 'backlog'" @click="mode = 'backlog'">Backlog</button>
        </div>
      </div>
      <template v-if="queue">
        <div class="activity-context"><strong :title="queue">{{ queue }}</strong><span>{{ actingClusterSlug || 'Current cluster' }} · all consumers</span></div>
        <div v-if="loading" class="activity-placeholder" role="status">Reading queue history…</div>
        <div v-else-if="error" class="activity-placeholder activity-error" role="status"><strong>Queue history unavailable</strong><span>{{ error }}</span></div>
        <template v-else-if="metrics">
          <div class="activity-plot" role="img" :aria-label="chartDescription">
            <BaseChart v-if="hasSamples" :key="mode" :data="chartData" :options="chartOptions" height="96px" />
            <div v-else class="activity-placeholder">No {{ mode === 'traffic' ? 'traffic' : 'backlog' }} samples in this window</div>
          </div>
          <div class="activity-legend"><template v-if="mode === 'traffic'"><span><i />Incoming</span><span><i class="dashed" />Delivered</span><small>messages / min</small></template><template v-else><span><i />Pending messages</span><small>sampled depth</small></template></div>
        </template>
        <dl class="activity-readings">
          <div><dt>{{ mode === 'traffic' ? 'Incoming / min' : 'Pending' }}</dt><dd>{{ number(mode === 'traffic' ? metrics?.incoming : metrics?.pending) }}</dd></div>
          <div><dt>{{ mode === 'traffic' ? 'Delivered / min' : 'Pending Δ' }}</dt><dd>{{ mode === 'traffic' ? number(metrics?.delivered) : delta(metrics?.pendingDelta) }}</dd></div>
          <div><dt title="ACK failures observed in the available complete buckets of this hour">ACK failures <span>· sampled hour</span></dt><dd>{{ number(metrics?.ackFailures) }}</dd></div>
        </dl>
        <p class="activity-freshness">{{ sampleNote }}</p>
      </template>
      <div v-else class="activity-placeholder activity-no-queue">No named queue reported.<span>Dynamic consumers expose their scope in instance details.</span></div>
    </div>
    <aside class="activity-queues" aria-label="Reported queues">
      <div class="queue-list-heading"><span class="section-label">Reported queues <small>{{ queues.length }}</small></span><span>Workers</span></div>
      <div class="queue-list" v-if="queues.length">
        <button v-for="item in queues" :key="item.name" class="queue-choice" :class="[`tone-${item.tone}`, { selected: queue === item.name }]" :aria-pressed="queue === item.name" :aria-label="`Show activity for ${item.name}`" @click="queue = item.name">
          <i aria-hidden="true" /><span :title="item.name">{{ item.name }}</span><strong>{{ number(item.running) }}<small> / {{ number(item.desired) }}</small></strong>
        </button>
      </div>
      <p v-else class="queue-list-empty">Queue allocation unavailable</p>
      <RouterLink v-if="queue" class="activity-open" :to="{ name: 'QueueDetail', params: { queueName: queue } }">Explore selected queue <span aria-hidden="true">↗</span></RouterLink>
      <p class="queue-scope">History covers this queue on the current cluster. A supervisor may use a different connection.</p>
    </aside>
  </section>
</template>

<script setup>
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
const sampleNote = computed(() => {
  if (!metrics.value) return 'Queue metrics are independent of the supervisor heartbeat.'
  if (mode.value === 'backlog') return `Latest depth ${metrics.value.pendingAt ? time(metrics.value.pendingAt) : 'unavailable'} · Δ across available samples`
  const latest = metrics.value.points.at(-1)?.at
  return `Rates ${latest ? `${time(latest)}–${time(latest + metrics.value.minutes * 60_000)}` : 'unavailable'} · Gaps mean no samples`
})
</script>

<style scoped>
.deployment-activity { display: grid; grid-template-columns: minmax(0, 1fr) minmax(200px, .65fr); gap: 28px; min-width: 0; }
.activity-chart, .activity-queues { min-width: 0; }
.section-label { font-size: 10px; font-weight: 500; color: var(--text-mid); text-transform: uppercase; letter-spacing: .08em; }
.activity-heading, .activity-heading > div:first-child { display: flex; align-items: center; gap: 12px; }.activity-heading { justify-content: space-between; gap: 8px; }
.activity-window { color: var(--text-low); font-size: 10px; white-space: nowrap; }
.activity-tabs { display: flex; gap: 12px; }.activity-tabs button { border: 0; border-bottom: 1px solid transparent; padding: 3px 0; font: inherit; font-size: 10px; background: none; color: var(--text-low); cursor: pointer; }.activity-tabs button[aria-pressed="true"] { color: var(--text-hi); border-color: var(--text-hi); }
.activity-tabs button:focus-visible, .queue-choice:focus-visible, .activity-open:focus-visible { outline: 2px solid var(--ring); outline-offset: 3px; }
.activity-context { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 4px 12px; margin: 6px 0 8px; font-size: 10px; color: var(--text-low); }.activity-context strong { min-width: 0; font-weight: 400; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 100%; color: var(--text-mid); }
.activity-context > span { font-size: 9px; }
.activity-plot { min-height: 96px; }
.activity-placeholder { height: 130px; display: flex; flex-direction: column; justify-content: center; gap: 8px; color: var(--text-low); font-size: 11px; line-height: 1.6; }
.activity-placeholder strong { font-weight: 500; color: var(--text-mid); }.activity-placeholder span { font-size: 10px; overflow-wrap: anywhere; }.activity-error { height: auto; min-height: 130px; }.activity-no-queue { height: 215px; }.activity-plot .activity-placeholder { height: 96px; }
.activity-legend { display: flex; align-items: center; gap: 15px; margin: 4px 0 10px; color: var(--text-mid); font-size: 9px; }.activity-legend > span { display: flex; align-items: center; gap: 5px; }.activity-legend i { width: 12px; border-top: 1.4px solid var(--series-1); }.activity-legend i.dashed { border-top-style: dashed; border-color: var(--series-2); }.activity-legend > small { margin-left: auto; font-size: 9px; color: var(--text-low); }
.activity-readings { display: grid; grid-template-columns: 1fr 1fr 1.2fr; gap: 12px; padding-top: 9px; border-top: 1px solid var(--bd); margin: 0; }
.activity-readings dt { color: var(--text-low); font-size: 10px; line-height: 1.5; }.activity-readings dt span { font-size: 9px; }.activity-readings dd { margin: 4px 0 0; color: var(--text-hi); font-size: 19px; font-weight: 450; letter-spacing: -.025em; font-variant-numeric: tabular-nums; }
.activity-freshness { color: var(--text-low); font-size: 9px; margin: 8px 0 0; line-height: 1.5; }
.activity-queues { border-left: 1px solid var(--bd); padding-left: 24px; }
.queue-list-heading { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin: 5px 0 14px; }.queue-list-heading .section-label small { margin-left: 5px; font-size: 10px; color: var(--text-low); }.queue-list-heading > span:last-child { color: var(--text-low); font-size: 9px; }
.queue-list { max-height: 166px; overflow-y: auto; scrollbar-width: thin; padding: 3px; margin: -3px; }
.queue-choice { display: flex; align-items: center; gap: 8px; text-align: left; width: 100%; padding: 10px 6px; border: 0; border-bottom: 1px solid var(--bd); font: inherit; background: transparent; color: var(--text-mid); cursor: pointer; }
.queue-choice:hover, .queue-choice.selected { background: color-mix(in srgb, var(--text-hi) 4%, transparent); }.queue-choice.selected > span { color: var(--text-hi); }
.queue-choice > i { flex: none; width: 4px; height: 4px; border-radius: 50%; background: var(--status-color); }.queue-choice > span { font-size: 10px; white-space: nowrap; text-overflow: ellipsis; overflow: hidden; min-width: 0; }.queue-choice strong { flex: none; margin-left: auto; font-size: 11px; font-weight: 450; font-variant-numeric: tabular-nums; }.queue-choice strong small { font-size: 10px; color: var(--text-low); }
.tone-good { --status-color: var(--supervisor-good); }.tone-warn { --status-color: var(--supervisor-warn); }.tone-bad { --status-color: var(--supervisor-bad); }
.activity-open { display: flex; justify-content: space-between; gap: 10px; margin-top: 18px; font-size: 10px; color: var(--text-mid); text-decoration: none; }.activity-open:hover { color: var(--text-hi); }
.queue-scope, .queue-list-empty { color: var(--text-low); font-size: 9px; line-height: 1.6; margin: 10px 0 0; }
@media (max-width: 1200px) { .deployment-activity { gap: 18px; grid-template-columns: minmax(0, 1fr) minmax(170px, .6fr); }.activity-queues { padding-left: 16px; }.activity-heading > div:first-child { gap: 6px; }.activity-window { font-size: 9px; } }
@media (max-width: 640px) { .deployment-activity { grid-template-columns: minmax(0, 1fr); gap: 20px; }.activity-queues { border: 0; padding: 18px 0 0; border-top: 1px solid var(--bd); }.queue-list { max-height: 125px; }.queue-scope { margin-bottom: 2px; }.activity-heading > div:first-child { gap: 12px; } }
</style>
