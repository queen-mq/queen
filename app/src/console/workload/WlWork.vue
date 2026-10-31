<script setup>
// Work over time: the scope's total of one measure (the window compared with
// dashed over it), then a small chart for each of the busiest rows, the rest
// summed in "others". White and grey only; each small chart has its own
// scale, so its peak is written beside its name. Dragging across any chart
// zooms the whole console.
import { computed } from 'vue'
import QcChart from '../QcChart.vue'
import QcSeg from '../QcSeg.vue'
import { MEASURES, rate } from './workloadViews'
import { describeApiError } from '@/api/errors'

const props = defineProps({
  model: { type: Object, required: true },     // workCharts()
  win: { type: Object, required: true },
  measure: { type: String, default: 'pop' },
  fetched: { type: Boolean, default: false },  // the broker counted Kafka fetches in this window
  loaded: { type: Boolean, default: false },
  error: { type: Object, default: null },
  level: { type: String, default: 'namespace' },
  bucketMinutes: { type: Number, default: 1 },
})
const emit = defineEmits(['measure', 'zoom', 'open'])

const options = computed(() => MEASURES.filter((m) => !m.kafka || props.fetched).map((m) => ({ value: m.id, label: m.label })))
const isRate = computed(() => props.model.measure.rate)
const say = (v) => (isRate.value ? `${rate(v)}/s` : rate(v))
const peakText = (v) => (v > 0 ? `peak ${say(v)}` : 'none')
const INK = { in: 'var(--ink-in)', out: 'var(--ink-out)' }
const every = computed(() => {
  const m = props.bucketMinutes
  return m === 1 ? 'a point a minute' : m % 60 === 0 ? `a point every ${m / 60 === 1 ? 'hour' : `${m / 60} h`}` : `a point every ${m} min`
})
const others = (c) => (c.names?.length ? `${c.label}: ${c.names.join(', ')}` : c.label)
</script>

<template>
  <section class="wlw" aria-labelledby="wlw-title">
    <div class="wlw-head">
      <h2 id="wlw-title" class="wlw-title">Work over time, {{ win.long }}</h2>
      <QcSeg :model-value="model.measure.id" :options="options" label="What the charts draw" @update:model-value="emit('measure', $event)" />
    </div>

    <p v-if="error && !loaded" class="qn-t2 wlw-state"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ describeApiError(error) }}.</p>
    <p v-else-if="!loaded" class="qn-t2 wlw-state">Reading the window…</p>
    <template v-else>
      <div class="wlw-total">
        <div class="wlw-legend">
          <span v-for="s in model.series" :key="s.id" class="wlw-key">
            <svg width="18" height="4" viewBox="0 0 18 4" aria-hidden="true"><line x1="0" y1="2" x2="18" y2="2" :style="{ stroke: INK[s.ink], strokeWidth: 1.5, strokeDasharray: s.dashed ? '4 3' : 'none' }" /></svg>
            <span>{{ s.label }}</span>
            <span v-if="s.id === 'total'" class="qn-num qn-t3">{{ peakText(model.peak) }}</span>
          </span>
          <span class="qn-t3 wlw-unit">{{ model.measure.unit }}, from 0</span>
        </div>
        <QcChart
          :series="model.series" :from="win.from" :to="win.to" :height="104" :axis="win.axis" :format="say"
          :aria-label="`${model.measure.unit}, ${win.long}`" @zoom="(a, b) => emit('zoom', a, b)"
        />
        <p v-if="model.prevNote" class="qn-t3 wlw-note">{{ model.prevNote }}</p>
      </div>

      <div v-if="model.small.length" class="wlw-grid">
        <div v-for="c in model.small" :key="c.key" class="wlw-cell">
          <div class="wlw-cell-head">
            <span v-if="c.others" class="qn-t2 wlw-name" :title="others(c)">{{ c.label }}</span>
            <button v-else type="button" class="wlw-name wlw-open" :class="c.mono ? 'qn-mono' : 'qn-t2'" :title="level === 'queue' ? `Its consumer groups` : `Look at its queues`" @click="emit('open', c.key)">{{ c.label }}</button>
            <span class="qn-num qn-t3 wlw-peak">{{ peakText(c.peak) }}</span>
          </div>
          <QcChart
            :series="[{ id: c.key, label: c.label, ink: 'in', points: c.points }]" :from="win.from" :to="win.to" :height="44"
            :axis="win.axis" :format="say" :aria-label="`${c.label}, ${model.measure.unit}`" @zoom="(a, b) => emit('zoom', a, b)"
          />
        </div>
      </div>
      <p v-else class="qn-t2 wlw-state">Nothing in this window: no {{ level === 'queue' ? 'queue' : level }} has any {{ model.measure.label.toLowerCase() }}.</p>
    </template>
    <p class="qn-read wlw-read">
      From the same read, {{ every }}; each small chart has its own scale.<template v-if="model.quiet">{{ ' ' + model.quiet }} {{ level === 'queue' ? (model.quiet === 1 ? 'queue has' : 'queues have') : (model.quiet === 1 ? `${level} has` : `${level === 'task' ? 'tasks' : 'namespaces'} have`) }} none and {{ model.quiet === 1 ? 'is' : 'are' }} not drawn.</template>
    </p>
  </section>
</template>

<style scoped>
.wlw-head { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 10px 22px; }
.wlw-title { font-size: 15px; font-weight: 500; }
.wlw-state { margin-top: 14px; }
.wlw-total { margin-top: 14px; }
.wlw-legend { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 18px; font-size: 12px; color: var(--text2); margin-bottom: 8px; }
.wlw-key { display: inline-flex; align-items: center; gap: 7px; }
.wlw-unit { margin-left: auto; }
.wlw-note { font-size: 12px; margin-top: 6px; }
.wlw-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 20px 32px; margin-top: 22px; }
.wlw-cell { min-width: 0; }
.wlw-cell-head { display: flex; align-items: baseline; gap: 10px; font-size: 12.5px; margin-bottom: 6px; white-space: nowrap; }
.wlw-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; font-weight: 500; color: var(--text); }
.wlw-open { text-align: left; }
.wlw-open:hover { text-decoration: underline; text-underline-offset: 3px; }
.wlw-peak { flex: none; margin-left: auto; font-size: 11.5px; }
.wlw-read { margin-top: 12px; }
@media (max-width: 900px) { .wlw-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 640px) { .wlw-grid { gap: 16px 18px; } .wlw-unit { margin-left: 0; flex-basis: 100%; } }
</style>
