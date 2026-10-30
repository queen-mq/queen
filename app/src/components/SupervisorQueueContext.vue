<template>
  <!--
    The queue a pool works on, as the cluster sees it: every consumer of it,
    read on request (one queue-ops read), never on the refresh tick.
  -->
  <section class="queue-context" aria-label="Queue metrics on the current cluster">
    <div class="context-head" title="Every consumer of this queue on this cluster. The supervisor's own connection is shown above.">
      <strong>Queue metrics · {{ actingClusterSlug || 'this cluster' }}</strong>
      <RouterLink :to="queueLocation(queue, route)">Open queue<Icon name="arrow-right" :size="13" /></RouterLink>
    </div>
    <div class="context-tools">
      <button type="button" class="btn btn-ghost" :disabled="loading" @click="read">{{ loading ? 'Reading…' : metrics ? 'Read again' : 'Read queue metrics' }}</button>
      <span v-if="error" class="context-error" role="status" :title="error">Queue metrics unavailable</span>
      <span v-else-if="metrics" class="context-stamp" :title="stampTitle">Read at {{ new Date(readAt).toLocaleTimeString() }}</span>
    </div>
    <template v-if="metrics">
      <dl class="context-metrics">
        <div><dt>Push / pop per second</dt><dd>{{ rate(metrics.push) }} / {{ rate(metrics.pop) }}</dd></div>
        <div><dt>Ack failures · 1h</dt><dd>{{ count(metrics.ackFailures) }}</dd></div>
        <div title="Change across the sampled depth of the last hour"><dt>Pending Δ · 1h</dt><dd>{{ metrics.pendingDelta === null ? '—' : `${metrics.pendingDelta > 0 ? '+' : ''}${formatNumber(metrics.pendingDelta)}` }}</dd></div>
      </dl>
      <svg v-if="polyline" class="backlog-trend" viewBox="0 0 600 44" role="img" aria-label="Pending-message samples over the last hour" preserveAspectRatio="none"><polyline :points="polyline" fill="none" stroke="currentColor" stroke-width="1.5" vector-effect="non-scaling-stroke" /></svg>
    </template>
  </section>
</template>

<script setup>
import { computed, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { useRoute } from 'vue-router'

import { system, describeApiError } from '@/api'
import Icon from '@/components/Icon.vue'
import { queueLocation } from '@/composables/navigation'
import { supervisorQueueMetrics } from '@/composables/supervisorQueueMetrics'
import { formatNumber } from '@/composables/useApi'
import { useIdentity } from '@/stores/identity'

const props = defineProps({ queue: { type: String, required: true } })
const route = useRoute()
const { epoch, actingClusterSlug } = useIdentity()
const metrics = shallowRef(null), error = ref(''), loading = ref(false), readAt = ref(null)
let controller, sequence = 0
function clear() { sequence++; controller?.abort(); metrics.value = null; error.value = ''; loading.value = false; readAt.value = null }
watch([() => props.queue, epoch], clear)
onBeforeUnmount(clear)
async function read() {
  clear()
  const turn = sequence, askedEpoch = epoch.value, queue = props.queue, now = Date.now()
  controller = new AbortController()
  loading.value = true
  try {
    const result = await system.getQueueOps({ queue, from: new Date(now - 3_600_000).toISOString(), to: new Date(now).toISOString() }, { signal: controller.signal, probe: true })
    if (turn !== sequence || askedEpoch !== epoch.value) return
    metrics.value = supervisorQueueMetrics(result.data, queue, now)
    readAt.value = Date.now()
  } catch (failure) {
    if (turn === sequence && askedEpoch === epoch.value) error.value = describeApiError(failure)
  } finally { if (turn === sequence) loading.value = false }
}
const count = value => value === null ? '—' : formatNumber(value)
const rate = value => value === null ? '—' : value.toLocaleString(undefined, { maximumFractionDigits: 1 })
const stampTitle = computed(() => {
  const m = metrics.value
  if (!m) return ''
  return `${m.bucket ? `Traffic sample ${new Date(m.bucket).toLocaleTimeString()}` : 'No complete traffic sample'} · ${count(m.pending)} pending in the latest sample`
})
const polyline = computed(() => {
  const points = metrics.value?.points || []
  if (points.length < 2) return ''
  const values = points.map(point => point.pending), min = Math.min(...values), span = Math.max(...values) - min
  const duration = points.at(-1).at - points[0].at
  return points.map(point => `${(point.at - points[0].at) * 600 / duration},${span ? 40 - (point.pending - min) / span * 36 : 22}`).join(' ')
})
</script>

<style scoped>
.queue-context { border-top: 1px solid var(--bd); margin: 16px 0; padding-top: 14px; }
.context-head { display: flex; flex-wrap: wrap; align-items: baseline; justify-content: space-between; gap: 8px 12px; font-size: 12px; }
.context-head strong { font-weight: 600; color: var(--text-hi); }
.context-head a { display: inline-flex; align-items: center; gap: 4px; color: var(--text-mid); text-decoration: none; }
.context-head a:hover { color: var(--text-hi); }
.context-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; margin-top: 8px; }
.context-tools .btn { margin-left: -10px; }
.context-stamp { font-size: 12px; color: var(--text-low); font-variant-numeric: tabular-nums; }
.context-error { font-size: 12px; color: var(--text-mid); }
.context-metrics { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px 16px; margin: 12px 0 0; }
.context-metrics dt { color: var(--text-low); font-size: 12px; }
.context-metrics dd { margin: 4px 0 0; font-size: 16px; font-weight: 600; color: var(--text-hi); font-variant-numeric: tabular-nums; }
.backlog-trend { display: block; width: 100%; height: 44px; color: var(--text-mid); margin: 12px 0 0; }
@media (max-width: 640px) { .context-metrics { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); } }
</style>
