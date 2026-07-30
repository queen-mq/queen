<template>
  <div class="row-chart-wrap">
    <!-- Custom HTML legend, only in full variant when we actually have data
         and at least one labeled series. Built from the same buildChartData
         output Chart.js consumes so the colors and labels stay in sync. -->
    <div
      v-if="variant === 'full' && renderMode === 'chart' && legendItems.length"
      class="row-chart-legend"
    >
      <span
        v-for="item in legendItems"
        :key="item.label"
        class="rcl-item"
      >
        <span class="rcl-swatch" :style="{ background: item.color }" />
        <span class="rcl-label">{{ item.label }}</span>
      </span>
    </div>

    <div class="row-chart" :class="`row-chart-${variant}`" :style="containerStyle">
      <!-- Has-data state: real Chart.js chart. In compact variant, axes are
           hidden and the chart reads as a sparkline; in full variant, axes,
           gridlines, and the HTML legend above are visible for proper
           investigation. -->
      <canvas v-if="renderMode === 'chart'" ref="canvas" class="row-chart-canvas" />

    <!-- Empty/flat fallbacks — only render in compact mode. In full mode,
         we show a centered "no data" placeholder instead, since axes
         around an empty chart look broken. -->
    <template v-else-if="variant === 'compact'">
      <!-- All-zero: solid baseline at the bottom (good news, "all quiet"). -->
      <svg
        v-if="renderMode === 'flat'"
        class="row-chart-fallback"
        :class="`row-chart-${tone}`"
        viewBox="0 0 100 24"
        preserveAspectRatio="none"
        role="img"
        aria-label="no events in window"
      >
        <line class="rcf-flat" x1="0" y1="22" x2="100" y2="22" />
      </svg>

      <!-- No data: dashed line. -->
      <svg
        v-else
        class="row-chart-fallback"
        viewBox="0 0 100 24"
        preserveAspectRatio="none"
        role="img"
        aria-label="no time series"
      >
        <line class="rcf-empty" x1="0" y1="12" x2="100" y2="12" />
      </svg>
    </template>

      <div v-else-if="variant === 'full'" class="row-chart-empty-full">
        {{ renderMode === 'flat' ? 'No events in window' : 'No time series for this metric' }}
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'
import { chartTheme, semanticColors, chartPalette, chartColor, alpha } from '@/composables/useChartTheme'
import {
  Chart, LineController, CategoryScale, LinearScale,
  PointElement, LineElement, Filler, Tooltip,
} from 'chart.js'

Chart.register(
  LineController, CategoryScale, LinearScale,
  PointElement, LineElement, Filler, Tooltip,
)

// Compact, real chart for use inside MetricRow. The visual goal is "looks
// like a sparkline" — no axes, no labels, no legend — but with the
// behavioral benefits of a real chart: hover tooltip with exact values
// and timestamps, multi-series rendering when the metric has more than
// one series to tell.
//
// Three render modes are handled internally so the parent doesn't have to:
//   - 'chart': Chart.js line(+area) with tooltip
//   - 'flat':  data exists but is all-zero across the window  →  solid baseline
//   - 'empty': no time series available                       →  dashed line
const props = defineProps({
  // Single-series shorthand. Used when `series` is not provided.
  data: { type: Array, default: () => [] },
  // Time-axis labels used by the tooltip title (one per data point).
  labels: { type: Array, default: () => [] },
  // Multi-series. When provided, overrides `data`.
  // Shape: [{ label, data, color?, fill? }]
  series: { type: Array, default: null },
  // Severity tone for the single-series line/area color. Ignored when
  // `series` is provided (multi-series uses the grey palette per series).
  tone: { type: String, default: 'mute' }, // 'mute' | 'ok' | 'warn' | 'bad'
  // Optional formatter for tooltip values + (in full variant) y-axis ticks.
  valueFormat: { type: Function, default: null },
  // Display variant:
  //   'compact' — sparkline-like, no axes, ~38px tall (used in MetricRow's
  //               spark column).
  //   'full'    — investigation chart with axes, gridlines, legend, ~210px
  //               tall (used in MetricRow's expanded panel).
  variant: { type: String, default: 'compact' }, // 'compact' | 'full'
  // Optional unit string used as the y-axis title in 'full' variant.
  unit: { type: String, default: '' },
})

const canvas = ref(null)
let chart = null

// ---------------------------------------------------------------------------
// Render-mode selection. Mirrors the old SVG Sparkline contract so empty
// states stay visually distinct from quiet-but-present data.
// ---------------------------------------------------------------------------
// Normalize an input array so that:
//   - null / undefined / NaN  →  null   (rendered as a chart gap)
//   - real numeric values     →  Number (preserved, including 0)
// Critically, the array length is *preserved* so the data stays aligned
// with the labels array. This is the difference between an honest gap
// (the metric had no sample for this bucket) and a misleading zero.
const normalizeArr = (arr) =>
  (arr || []).map(v => {
    if (v === null || v === undefined) return null
    const n = Number(v)
    return Number.isFinite(n) ? n : null
  })

const flatSeries = computed(() => {
  if (props.series && props.series.length) {
    return props.series.map(s => ({ ...s, data: normalizeArr(s.data) }))
  }
  return [{ label: 'Value', data: normalizeArr(props.data) }]
})

const renderMode = computed(() => {
  const ss = flatSeries.value
  // A series has "points" if at least 2 entries are finite (non-null).
  const finiteCount = (s) => s.data.reduce((c, v) => v === null ? c : c + 1, 0)
  const anyHasPoints = ss.some(s => finiteCount(s) >= 2)
  if (!anyHasPoints) return 'empty'
  // Flat = every finite point is zero. We ignore nulls here so a window
  // with one real "0" between gaps still reads as flat (not noisy).
  const anyNonZero = ss.some(s => s.data.some(v => v !== null && v !== 0))
  if (!anyNonZero) return 'flat'
  return 'chart'
})

// ---------------------------------------------------------------------------
// Color resolution — single-series uses the row's severity tone, multi-series
// cycles through the grey palette so the eye treats them as "N comparable
// series" rather than "N severity levels".
// ---------------------------------------------------------------------------
function singleColor() {
  switch (props.tone) {
    case 'ok':   return semanticColors.ok
    case 'warn': return semanticColors.warn
    case 'bad':  return semanticColors.bad
    // 'mute' = no severity to report, so it takes the achromatic ramp rather
    // than a status hue — same convention stateColor() uses for neutral
    // labels. Slot 2 is the ramp's mid grey, the closest token to the
    // hand-typed literal it replaces.
    default:     return chartColor(1)
  }
}

function buildChartData() {
  const ss = flatSeries.value
  // Use the longest series's labels (or fall back to provided / index).
  const longest = ss.reduce((a, b) => (a.data.length >= b.data.length ? a : b), ss[0])
  const labels = props.labels.length === longest.data.length
    ? props.labels
    : longest.data.map((_, i) => i)

  // Single series → severity-toned line + area fill.
  if (ss.length === 1) {
    const c = singleColor()
    return {
      labels,
      datasets: [{
        label: ss[0].label || 'Value',
        data: ss[0].data,
        borderColor: c.line,
        backgroundColor: c.fill,
        borderWidth: 1.4,
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHoverBackgroundColor: c.line,
        pointHoverBorderColor: chartTheme.tooltipBg,
        pointHoverBorderWidth: 1,
        tension: 0.4,
        fill: true,
        // Don't bridge null buckets — render them as actual gaps so the
        // operator can tell "no sample yet" from "real zero".
        spanGaps: false,
      }],
    }
  }

  // Multi-series → only the first dataset gets the area fill; others are
  // line-only so they don't muddy the visual. Per-series colors override
  // the palette when explicitly provided (e.g. ack=green for "healthy
  // completion" sense).
  return {
    labels,
    datasets: ss.map((s, i) => {
      const palette = chartPalette[i % chartPalette.length]
      const line = s.color || palette.line
      // alpha() hands back non-hex input untouched, which is what the old
      // `line + '20'` string concat was hand-rolling (0x20/255 ≈ .125).
      const fill = s.color ? alpha(line, 0.125) : palette.fill
      return {
        label: s.label || `Series ${i + 1}`,
        data: s.data,
        borderColor: line,
        backgroundColor: fill,
        borderWidth: 1.3,
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHoverBackgroundColor: line,
        pointHoverBorderColor: chartTheme.tooltipBg,
        pointHoverBorderWidth: 1,
        tension: 0.4,
        fill: i === 0 || s.fill === true,
        spanGaps: false,
      }
    }),
  }
}

function buildOptions() {
  const fmt = props.valueFormat
  const isFull = props.variant === 'full'
  const gridColor = chartTheme.grid
  const tickColor = chartTheme.tick

  const tooltip = {
    backgroundColor: chartTheme.tooltipBg,
    titleColor:      chartTheme.tooltipText,
    bodyColor:       chartTheme.tooltipBody,
    borderColor:     chartTheme.tooltipBorder,
    borderWidth: 1,
    // --r-chip. Canvas-painted tooltip: Chart.js wants a number here, so it
    // cannot be a var(); keep it equal to the token.
    cornerRadius: 4,
    padding: { top: 6, bottom: 6, left: 10, right: 10 },
    titleFont: { family: chartTheme.fontFamily, size: 10.5, weight: 500 },
    bodyFont:  { family: chartTheme.fontFamily, size: 11 },
    displayColors: true,
    boxWidth: 7, boxHeight: 7, boxPadding: 3,
    callbacks: fmt ? {
      label: (ctx) => `${ctx.dataset.label}: ${fmt(ctx.parsed.y)}`,
    } : undefined,
  }

  // Compact: no axes, no legend, no padding — reads as a sparkline.
  if (!isFull) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { display: false }, tooltip },
      scales: {
        x: { display: false, grid: { display: false }, border: { display: false } },
        y: {
          display: false,
          beginAtZero: true, min: 0,
          grid: { display: false },
          border: { display: false },
        },
      },
      layout: { padding: { top: 2, right: 0, bottom: 2, left: 0 } },
    }
  }

  // Full: visible axes with gridlines, ticks formatted via valueFormat,
  // legend at top-right, slightly stronger line weight. This is the
  // "investigation" view — same data as the row's compact chart, but
  // readable in absolute terms.
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    // Legend is rendered as HTML above the canvas (see template) — Chart.js'
    // built-in legend gets squeezed out at small heights and is hard to
    // style consistently with the rest of the app's typography.
    plugins: {
      legend: { display: false },
      tooltip,
    },
    scales: {
      x: {
        grid: { color: gridColor, drawBorder: false },
        ticks: {
          color: tickColor,
          font: { family: chartTheme.fontFamily, size: 10 },
          maxRotation: 0,
          autoSkipPadding: 24,
        },
        border: { display: false },
      },
      y: {
        beginAtZero: true,
        min: 0,
        grid: { color: gridColor, drawBorder: false },
        ticks: {
          color: tickColor,
          font: { family: chartTheme.fontFamily, size: 10 },
          padding: 8,
          callback: fmt ? (v) => fmt(v) : undefined,
        },
        border: { display: false },
        title: props.unit ? {
          display: true,
          text: props.unit,
          color: tickColor,
          font: { family: chartTheme.fontFamilyUI, size: 10 },
        } : { display: false },
      },
    },
    layout: { padding: { top: 4, right: 8, bottom: 4, left: 4 } },
  }
}

function createChart() {
  if (!canvas.value) return
  if (chart) { chart.destroy(); chart = null }
  chart = new Chart(canvas.value, {
    type: 'line',
    data: buildChartData(),
    options: buildOptions(),
  })
}

// React to data shape, theme, and variant. Variant is special: switching
// between compact and full requires rebuilding options entirely (axes
// appear/disappear) so we always re-init when it changes.
watch([
  () => props.data, () => props.series, () => props.labels,
  () => props.tone, () => props.unit, () => props.variant,
], () => {
  if (renderMode.value !== 'chart') {
    if (chart) { chart.destroy(); chart = null }
    return
  }
  if (chart) {
    chart.data = buildChartData()
    chart.options = buildOptions()
    chart.update('none')
  } else {
    nextTick(createChart)
  }
}, { deep: true })

const containerStyle = computed(() =>
  props.variant === 'full' ? { height: '210px' } : { height: '38px' }
)

// Legend items for the HTML legend rendered above the canvas in full mode.
// Mirrors buildChartData()'s color resolution so swatches stay in sync
// without us having to ask Chart.js for the rendered dataset metadata.
const legendItems = computed(() => {
  const ss = flatSeries.value
  if (ss.length <= 1) return []  // single-series → no legend needed
  return ss.map((s, i) => {
    const palette = chartPalette[i % chartPalette.length]
    return {
      label: s.label || `Series ${i + 1}`,
      color: s.color || palette.line,
    }
  })
})

watch(renderMode, (m) => {
  if (m === 'chart') {
    nextTick(createChart)
  } else if (chart) {
    chart.destroy()
    chart = null
  }
})

onMounted(() => {
  if (renderMode.value === 'chart') nextTick(createChart)
})
onUnmounted(() => { if (chart) { chart.destroy(); chart = null } })
</script>

<style scoped>
.row-chart-wrap {
  width: 100%;
}
.row-chart {
  position: relative;
  width: 100%;
  /* Height is set inline via containerStyle so variant changes don't
     require a CSS class swap. */
}
.row-chart-canvas {
  width: 100% !important;
  height: 100% !important;
  display: block;
}

.row-chart-empty-full {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  color: var(--text-low);
  letter-spacing: -.005em;
}

/* HTML legend — sits above the canvas in full variant. Right-aligned
   so it doesn't compete with the y-axis title on the left. Tiny color
   swatch + label per series. */
.row-chart-legend {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  flex-wrap: wrap;
  gap: 14px;
  padding: 0 4px 6px;
}
.rcl-item {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  color: var(--text-mid);
  font-family: 'Inter', ui-sans-serif, system-ui, sans-serif;
  letter-spacing: -.005em;
}
.rcl-swatch {
  width: 10px;
  height: 10px;
  border-radius: var(--r-chip);
  display: inline-block;
}
.rcl-label {
  white-space: nowrap;
}

/* Empty / flat fallbacks — match the empty-state semantics of the SVG
   Sparkline so the visual language stays consistent. */
.row-chart-fallback {
  width: 100%;
  height: 100%;
  display: block;
}
.rcf-flat,
.rcf-empty {
  stroke: var(--text-low);
  stroke-width: 1;
  vector-effect: non-scaling-stroke;
  opacity: .35;
}
.rcf-empty { stroke-dasharray: 2 3; }

/* Tone variants for the flat-baseline state. The chart-rendered state
   already uses tone-driven colors via singleColor() above. */
.row-chart-ok   .rcf-flat { stroke: var(--ok-500); opacity: .55; }
.row-chart-warn .rcf-flat { stroke: var(--warn-400); opacity: .55; }
.row-chart-bad  .rcf-flat { stroke: var(--ember-400); opacity: .55; }
</style>
