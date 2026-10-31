<script setup>
// A time chart: lines from 0, white and grey, no axes but the times under it.
// Hover reads the values at that minute; dragging across it zooms the whole
// console to that span (the parent calls useWindow().range).
import { computed, ref } from 'vue'
import { stamp } from './useWindow'

const props = defineProps({
  series: { type: Array, default: () => [] }, // [{ id, label, ink: in|out|amber|red, area?, points: [[ms, v|null]] }]
  from: { type: Number, required: true },
  to: { type: Number, required: true },
  height: { type: Number, default: 120 },
  unit: { type: String, default: '' },
  format: { type: Function, default: null },
  brush: { type: Boolean, default: true },
  shade: { type: Array, default: () => [] },  // [{ from, to, tone: amber|red }]
  axis: { type: Array, default: null },       // three labels; else from the span
  ariaLabel: { type: String, default: '' },
  yMax: { type: Number, default: 0 },
  minTop: { type: Number, default: 0 },          // the scale's least top: 1 waiting is not a full chart
})
const emit = defineEmits(['zoom'])

const W = 1000
const INK = { in: 'var(--ink-in)', out: 'var(--ink-out)', amber: 'var(--amber)', red: 'var(--red)' }
const span = computed(() => Math.max(1, props.to - props.from))
const top = computed(() => {
  if (props.yMax > 0) return props.yMax
  let m = 0
  for (const s of props.series) for (const p of s.points) if (Number.isFinite(p[1]) && p[1] > m) m = p[1]
  return Math.max(m > 0 ? m * 1.08 : 1, props.minTop || 0)
})
const X = (t) => ((t - props.from) / span.value) * W
const Y = (v) => props.height - 1 - (Math.max(0, Math.min(v, top.value)) / top.value) * (props.height - 4)

function segmentsOf(points) {
  const out = []; let cur = []
  for (const [t, v] of points) {
    if (t < props.from - span.value * 0.02 || t > props.to + span.value * 0.02) continue
    if (!Number.isFinite(v)) { if (cur.length) out.push(cur); cur = []; continue }
    cur.push([X(t), Y(v)])
  }
  if (cur.length) out.push(cur)
  return out
}
const drawn = computed(() => props.series.map((s) => {
  const segs = segmentsOf(s.points)
  return {
    id: s.id,
    ink: INK[s.ink] || INK.in,
    dashed: !!s.dashed,
    lines: segs.map((seg) => seg.map(([x, y]) => x.toFixed(1) + ',' + y.toFixed(1)).join(' ')),
    areas: s.area ? segs.filter((seg) => seg.length > 1).map((seg) => {
      const pts = seg.map(([x, y]) => x.toFixed(1) + ',' + y.toFixed(1)).join(' ')
      return `${seg[0][0].toFixed(1)},${props.height} ${pts} ${seg[seg.length - 1][0].toFixed(1)},${props.height}`
    }) : [],
  }
}))
const shades = computed(() => props.shade.map((s) => ({
  x: Math.max(0, X(s.from)), w: Math.max(0, Math.min(W, X(s.to)) - Math.max(0, X(s.from))),
  fill: s.tone === 'red' ? 'var(--redbg)' : 'var(--amberbg)',
})))
const labels = computed(() => {
  if (props.axis) return props.axis
  const withDay = span.value >= 86_400_000
  return [stamp(props.from, withDay), stamp(props.from + span.value / 2, withDay), stamp(props.to, withDay)]
})

// ---- hover and brush ----
const hoverF = ref(null)
const drag = ref(null) // { a, b } as fractions
const frac = (e) => {
  const r = e.currentTarget.getBoundingClientRect()
  return Math.max(0, Math.min(1, (e.clientX - r.left) / (r.width || 1)))
}
function down(e) {
  if (!props.brush || e.button !== 0) return
  e.currentTarget.setPointerCapture?.(e.pointerId)
  const f = frac(e)
  drag.value = { a: f, b: f }
}
function move(e) {
  const f = frac(e)
  if (drag.value) drag.value = { a: drag.value.a, b: f }
  else hoverF.value = f
}
function up() {
  const d = drag.value
  drag.value = null
  if (!d) return
  const lo = Math.min(d.a, d.b); const hi = Math.max(d.a, d.b)
  if (hi - lo < 0.02) return
  emit('zoom', props.from + lo * span.value, props.from + hi * span.value)
}
function leave() { if (!drag.value) hoverF.value = null }

const fmt = (v) => {
  if (!Number.isFinite(v)) return '–'
  if (props.format) return props.format(v)
  const a = Math.abs(v)
  const s = a >= 100 ? Math.round(v).toLocaleString() : a >= 10 ? v.toFixed(1) : v.toFixed(2).replace(/\.?0+$/, '') || '0'
  return s + props.unit
}
function nearest(points, t) {
  let lo = 0; let hi = points.length - 1
  if (hi < 0) return null
  while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (points[mid][0] < t) lo = mid; else hi = mid }
  const p = Math.abs(points[lo][0] - t) <= Math.abs(points[hi][0] - t) ? points[lo] : points[hi]
  return Math.abs(p[0] - t) <= span.value / 30 ? p : null
}
const readout = computed(() => {
  if (hoverF.value == null || drag.value) return null
  const t = props.from + hoverF.value * span.value
  const rows = props.series.map((s) => {
    const p = nearest(s.points, t)
    return { id: s.id, label: s.label, ink: INK[s.ink] || INK.in, v: p ? fmt(p[1]) : '–' }
  })
  return { left: hoverF.value * 100, flip: hoverF.value > 0.66, time: stamp(t, span.value >= 86_400_000), rows }
})
const brushStyle = computed(() => {
  const d = drag.value
  if (!d) return null
  return { left: Math.min(d.a, d.b) * 100 + '%', width: Math.abs(d.b - d.a) * 100 + '%' }
})
</script>

<template>
  <div class="qn-chart">
    <div class="qn-chart-plot" :style="{ height: height + 'px' }">
      <svg :viewBox="`0 0 ${W} ${height}`" preserveAspectRatio="none" role="img" :aria-label="ariaLabel" class="qn-chart-svg" :style="{ height: height + 'px' }">
        <rect v-for="(s, i) in shades" :key="'s' + i" :x="s.x" y="0" :width="s.w" :height="height" :style="{ fill: s.fill }" />
        <line class="qn-baseline" x1="0" :y1="height - 0.5" :x2="W" :y2="height - 0.5" style="stroke: var(--line); stroke-width: 1; vector-effect: non-scaling-stroke" />
        <template v-for="d in drawn" :key="d.id">
          <polygon v-for="(a, i) in d.areas" :key="'a' + i" :points="a" style="fill: var(--ink-fill); stroke: none" />
        </template>
        <template v-for="d in drawn" :key="'l' + d.id">
          <polyline
            v-for="(l, i) in d.lines"
            :key="i"
            :points="l"
            :style="{ fill: 'none', stroke: d.ink, strokeWidth: 1.5, strokeLinejoin: 'round', vectorEffect: 'non-scaling-stroke', strokeDasharray: d.dashed ? '4 3' : 'none' }"
          />
        </template>
      </svg>
      <div
        class="qn-chart-hit"
        :class="{ brush }"
        role="presentation"
        @pointerdown="down"
        @pointermove="move"
        @pointerup="up"
        @pointercancel="up"
        @pointerleave="leave"
      >
        <div v-if="brushStyle" class="qn-chart-brush" :style="brushStyle" />
        <template v-if="readout">
          <div class="qn-chart-cursor" :style="{ left: readout.left + '%' }" />
          <div class="qn-chart-read" :class="{ flip: readout.flip }" :style="{ left: readout.left + '%' }">
            <p class="qn-num qn-t3" style="font-size: 11px; margin-bottom: 2px">{{ readout.time }}</p>
            <p v-for="r in readout.rows" :key="r.id" class="qn-num" style="display: flex; align-items: center; gap: 6px; white-space: nowrap">
              <span :style="{ width: '10px', height: '2px', background: r.ink, display: 'inline-block' }" />
              <span class="qn-t2">{{ r.label }}</span><span style="margin-left: auto; padding-left: 10px; font-weight: 500">{{ r.v }}</span>
            </p>
          </div>
        </template>
      </div>
    </div>
    <div class="qn-num qn-t3 qn-chart-axis"><span>{{ labels[0] }}</span><span>{{ labels[1] }}</span><span>{{ labels[2] }}</span></div>
  </div>
</template>

<style scoped>
.qn-chart-plot { position: relative; }
.qn-chart-svg { position: absolute; inset: 0; width: 100%; display: block; overflow: visible; }
.qn-chart-hit { position: absolute; inset: 0; user-select: none; touch-action: pan-y; }
.qn-chart-hit.brush { cursor: crosshair; }
.qn-chart-brush { position: absolute; top: 0; bottom: 0; background: var(--ink-brush); border-left: 1px solid var(--text2); border-right: 1px solid var(--text2); }
.qn-chart-cursor { position: absolute; top: 0; bottom: 0; width: 1px; background: var(--text3); opacity: .6; pointer-events: none; }
.qn-chart-read { position: absolute; top: -6px; transform: translateX(10px); background: var(--pop); border: 1px solid var(--line); border-radius: var(--r-ctl); padding: 6px 8px; font-size: 12px; pointer-events: none; z-index: 5; min-width: 128px; box-shadow: 0 8px 20px rgba(0, 0, 0, .25); }
.qn-chart-read.flip { transform: translateX(calc(-100% - 10px)); }
:global(.qn.tone .qn-chart-read) { border-color: transparent; }
.qn-chart-axis { display: flex; justify-content: space-between; font-size: 11px; margin-top: 6px; }
</style>
