<template>
  <section class="queue-context" aria-label="Queue metrics on the current cluster">
    <div class="context-head"><strong>Queue metrics · {{ actingClusterSlug || 'this cluster' }}</strong><RouterLink :to="queueLocation(queue, route)">Open queue →</RouterLink></div>
    <p>All consumers of this queue on this cluster. The supervisor’s connection is shown above.</p>
    <button class="btn btn-ghost" :disabled="loading" @click="read">{{ loading ? 'Reading…' : metrics ? 'Refresh queue metrics' : 'Load queue metrics' }}</button>
    <p v-if="error" role="status">{{ error }}</p>
    <template v-if="metrics">
      <dl class="context-metrics"><div><dt>Push / pop per second</dt><dd>{{ rate(metrics.push) }} / {{ rate(metrics.pop) }}</dd></div><div><dt>Ack failures · 1h</dt><dd>{{ count(metrics.ackFailures) }}</dd></div><div><dt>Pending Δ · 1h sampled</dt><dd>{{ metrics.pendingDelta === null ? '—' : `${metrics.pendingDelta > 0 ? '+' : ''}${formatNumber(metrics.pendingDelta)}` }}</dd></div></dl>
      <svg v-if="polyline" class="backlog-trend" viewBox="0 0 600 44" role="img" aria-label="Pending-message samples over the last hour" preserveAspectRatio="none"><polyline :points="polyline" fill="none" stroke="currentColor" stroke-width="1.5" vector-effect="non-scaling-stroke" /></svg>
      <p>Read at {{ new Date(readAt).toLocaleTimeString() }} · {{ metrics.bucket ? `traffic sample ${new Date(metrics.bucket).toLocaleTimeString()}` : 'no complete traffic samples' }}. Pending: {{ count(metrics.pending) }} in the latest sample.</p>
    </template>
  </section>
</template>

<script setup>
import { queueLocation } from '@/composables/navigation'
const route = useRoute()
import { useRoute } from 'vue-router'
import { computed, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { system, describeApiError } from '@/api'
import { formatNumber } from '@/composables/useApi'
import { supervisorQueueMetrics } from '@/composables/supervisorQueueMetrics'
import { useIdentity } from '@/stores/identity'
const props = defineProps({ queue: { type: String, required: true } })
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
    if (turn === sequence && askedEpoch === epoch.value) error.value = `Queue metrics unavailable. ${describeApiError(failure)}`
  } finally { if (turn === sequence) loading.value = false }
}
const count = value => value === null ? '—' : formatNumber(value)
const rate = value => value === null ? '—' : value.toLocaleString(undefined, { maximumFractionDigits: 1 })
const polyline = computed(() => {
  const points = metrics.value?.points || []
  if (points.length < 2) return ''
  const values = points.map(point => point.pending), min = Math.min(...values), span = Math.max(...values) - min
  const duration = points.at(-1).at - points[0].at
  return points.map(point => `${(point.at - points[0].at) * 600 / duration},${span ? 40 - (point.pending - min) / span * 36 : 22}`).join(' ')
})
</script>

<style scoped>
.queue-context { border-top: 1px solid var(--bd); border-bottom: 1px solid var(--bd); margin: 18px 0; padding: 16px 0; }
.context-head { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 10px; font-size: 12px; }
.context-head a { color: var(--text-mid); }
.queue-context p { color: var(--text-low); font-size: 11px; line-height: 1.6; margin: 8px 0; }
.context-metrics { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; margin: 16px 0; }
.context-metrics dt { color: var(--text-low); font-size: 11px; }
.context-metrics dd { margin: 6px 0; font-size: 18px; }
.backlog-trend { display: block; width: 100%; height: 44px; color: var(--text-mid); margin: 12px 0; }
</style>
