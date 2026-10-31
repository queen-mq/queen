<script setup>
// Each node over the window: the machine (CPU, memory, disk) on one row, what
// moves (the network, the Raft port) on the next. A line per node, white and
// grey told apart by dash; dragging across any chart zooms the whole console.
import { computed } from 'vue'
import QcChart from '../QcChart.vue'
import { describeApiError } from '@/api/errors'

const props = defineProps({
  model: { type: Object, required: true },   // clusterViews chartsOf
  win: { type: Object, required: true },     // useWindow().win
  loaded: { type: Boolean, default: false },
  error: { type: Object, default: null },
})
const emit = defineEmits(['zoom'])

const INK = { in: 'var(--ink-in)', out: 'var(--ink-out)' }
const missingLine = computed(() => {
  const m = props.model.missing
  if (!m.length) return ''
  return `${m.join(', ')} did not answer this read, so ${m.length === 1 ? 'its lines are' : 'their lines are'} missing.`
})
const every = computed(() => {
  const bm = props.model.bucketMinutes
  return bm === 1 ? 'a point a minute per node' : `a point every ${bm >= 60 && bm % 60 === 0 ? `${bm / 60} h` : `${bm} min`} per node`
})
</script>

<template>
  <section class="cc" aria-labelledby="cc-title">
    <div class="cc-head">
      <h2 id="cc-title" class="cc-title">Each node, {{ win.long }}</h2>
      <div v-if="model.legend.length" class="cc-legend" aria-label="Lines">
        <span v-for="l in model.legend" :key="l.name" class="cc-key">
          <svg width="18" height="4" viewBox="0 0 18 4" aria-hidden="true"><line x1="0" y1="2" x2="18" y2="2" :style="{ stroke: INK[l.ink], strokeWidth: 1.5, strokeDasharray: l.dashed ? '4 3' : 'none' }" /></svg>
          <span><span class="qn-mono">{{ l.name }}</span><span v-if="l.quiet" class="qn-t3">, no samples</span></span>
        </span>
      </div>
    </div>

    <p v-if="error && !loaded" class="qn-t2 cc-state"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ describeApiError(error) }}.</p>
    <p v-else-if="!loaded" class="qn-t2 cc-state">Reading every node's samples…</p>
    <p v-else-if="model.empty" class="qn-t2 cc-state">No node sent samples in this window.</p>
    <template v-else>
      <p v-if="missingLine" class="qn-t2" style="margin-top: 8px">{{ missingLine }}</p>
      <div class="cc-grid">
        <div v-for="c in model.charts" :key="c.id" class="cc-chart" :class="`cc-${c.id}`">
          <div class="cc-chart-head">
            <span class="cc-chart-title">{{ c.title }}</span>
            <span class="qn-t3 cc-sub" :title="c.sub">{{ c.sub }}</span>
            <span v-if="c.has" class="qn-t3 qn-num cc-top">{{ c.topText }}</span>
          </div>
          <QcChart
            v-if="c.has" :series="c.series" :from="win.from" :to="win.to" :height="96" :axis="win.axis"
            :y-max="c.top" :format="c.format" :aria-label="`${c.title}, ${c.sub}, a line per node, ${win.long}`" @zoom="(a, b) => emit('zoom', a, b)"
          />
          <p v-else class="qn-t2 cc-none">{{ c.none }}</p>
        </div>
      </div>
    </template>
    <p class="qn-read" style="margin-top: 14px">Read from <span class="qn-mono">GET /api/v1/analytics/system-metrics</span>, {{ every }}, each the average of its span.</p>
  </section>
</template>

<style scoped>
.cc-head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 22px; }
.cc-title { font-size: 15px; font-weight: 500; }
.cc-legend { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 18px; font-size: 12px; color: var(--text2); }
.cc-key { display: inline-flex; align-items: center; gap: 7px; }
.cc-state { margin-top: 14px; }
.cc-grid { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 26px 32px; margin-top: 16px; }
.cc-chart { grid-column: span 2; min-width: 0; }
.cc-chart.cc-network, .cc-chart.cc-raft { grid-column: span 3; }
.cc-chart-head { display: flex; align-items: baseline; gap: 8px; font-size: 12px; margin-bottom: 8px; white-space: nowrap; }
.cc-chart-title { flex: none; font-weight: 500; color: var(--text); }
.cc-sub { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.cc-top { flex: none; margin-left: auto; font-size: 11px; }
.cc-none { height: 96px; display: flex; align-items: center; font-size: 12.5px; border-bottom: 1px solid var(--line); }
@media (max-width: 900px) { .cc-chart, .cc-chart.cc-network, .cc-chart.cc-raft { grid-column: span 6; } }
</style>
