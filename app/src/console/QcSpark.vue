<script setup>
// A small line or bars with no axes: the shape of the last hour.
import { computed } from 'vue'

const props = defineProps({
  values: { type: Array, default: () => [] },   // numbers; null = no data
  kind: { type: String, default: 'line' },      // line | bars
  tone: { type: String, default: 'grey' },      // grey | text | amber | red
  width: { type: Number, default: 160 },
  height: { type: Number, default: 28 },
  label: { type: String, default: '' },
  max: { type: Number, default: 0 },
})

const ink = computed(() => ({ grey: 'var(--text3)', text: 'var(--ink-in)', amber: 'var(--amber)', red: 'var(--red)' }[props.tone] || 'var(--text3)'))
const top = computed(() => {
  const m = props.max || Math.max(0, ...props.values.filter((v) => Number.isFinite(v)))
  return m > 0 ? m : 1
})
const segments = computed(() => {
  const n = props.values.length
  if (n < 2) return []
  const W = props.width; const H = props.height
  const out = []; let cur = []
  props.values.forEach((v, i) => {
    if (!Number.isFinite(v)) { if (cur.length) out.push(cur); cur = []; return }
    const x = (i * W) / (n - 1)
    const y = H - 1 - (Math.max(0, Math.min(v, top.value)) / top.value) * (H - 3)
    cur.push(x.toFixed(1) + ',' + y.toFixed(1))
  })
  if (cur.length) out.push(cur)
  return out.map((s) => s.join(' '))
})
const barPath = computed(() => {
  const n = props.values.length
  if (!n) return ''
  const W = props.width; const H = props.height; const bw = W / n
  let d = ''
  props.values.forEach((v, i) => {
    if (!Number.isFinite(v) || v <= 0) return
    const h = Math.max(1, (Math.min(v, top.value) / top.value) * (H - 2))
    const x = i * bw + bw * 0.15
    d += `M${x.toFixed(1)} ${H} V${(H - h).toFixed(1)} H${(x + bw * 0.7).toFixed(1)} V${H} Z `
  })
  return d
})
</script>

<template>
  <svg :viewBox="`0 0 ${width} ${height}`" :width="width" :height="height" role="img" :aria-label="label" style="max-width: 100%; overflow: visible">
    <line class="qn-baseline" x1="0" :y1="height - 0.5" :x2="width" :y2="height - 0.5" style="stroke: var(--line); stroke-width: 1" />
    <template v-if="kind === 'bars'">
      <path :d="barPath" :style="{ fill: ink }" />
    </template>
    <template v-else>
      <polyline v-for="(p, i) in segments" :key="i" :points="p" :style="{ fill: 'none', stroke: ink, strokeWidth: 1.4, strokeLinejoin: 'round', strokeLinecap: 'round' }" />
    </template>
  </svg>
</template>
