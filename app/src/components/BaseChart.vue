<template>
  <div ref="container" :style="{ height: height || '280px' }" class="relative">
    <canvas ref="canvas" />
  </div>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted, nextTick } from 'vue'
import { useTheme } from '@/composables/useTheme'
import { chartPalette, chartTheme } from '@/composables/useChartTheme'
import {
  Chart, LineController, BarController, DoughnutController,
  CategoryScale, LinearScale, PointElement, LineElement,
  BarElement, ArcElement, Filler, Tooltip, Legend
} from 'chart.js'

Chart.register(
  LineController, BarController, DoughnutController,
  CategoryScale, LinearScale, PointElement, LineElement,
  BarElement, ArcElement, Filler, Tooltip, Legend
)

const props = defineProps({
  type: { type: String, default: 'line' },
  data: { type: Object, required: true },
  options: { type: Object, default: () => ({}) },
  height: { type: String, default: '280px' },
})

const { isDark } = useTheme()
const canvas = ref(null)
const container = ref(null)
let chart = null

// Palette imported from useChartTheme — single source of truth.
const PALETTE = chartPalette

function buildChartData(data) {
  const datasets = (data.datasets || []).map((ds, i) => {
    const c = PALETTE[i % PALETTE.length]
    return {
      ...ds,
      borderColor: ds.borderColor || c.line,
      backgroundColor: ds.fill ? c.fill : (ds.backgroundColor || c.fill),
      borderWidth: ds.borderWidth || 1.4,
      pointRadius: 0,
      pointHoverRadius: 3,
      pointHoverBackgroundColor: ds.borderColor || c.line,
      tension: 0.4,
      fill: ds.fill !== false,
      // Don't bridge null buckets — render them as honest gaps so a
      // still-aggregating tail bucket doesn't draw a phantom drop to 0.
      // Per-dataset override still wins (e.g. `spanGaps: true`).
      spanGaps: ds.spanGaps !== undefined ? ds.spanGaps : false,
    }
  })
  return { labels: data.labels, datasets }
}

function getThemeOptions() {
  const gridColor = isDark.value ? chartTheme.grid : 'rgba(10,10,10,0.06)'
  const tickColor = isDark.value ? chartTheme.tick : '#8a8a86'
  const tooltipBg = isDark.value ? chartTheme.tooltipBg : '#fff'
  const tooltipText = isDark.value ? chartTheme.tooltipText : '#0a0a0a'
  const tooltipBody = isDark.value ? chartTheme.tooltipBody : '#6a6a6a'
  const tooltipBorder = isDark.value ? chartTheme.tooltipBorder : 'rgba(10,10,10,0.08)'

  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: tooltipBg,
        titleColor: tooltipText,
        bodyColor: tooltipBody,
        borderColor: tooltipBorder,
        borderWidth: 1,
        cornerRadius: 4,
        padding: { top: 6, bottom: 6, left: 10, right: 10 },
        titleFont: { family: 'Inter', size: 11.5, weight: 500 },
        bodyFont: { family: 'JetBrains Mono', size: 11 },
        displayColors: true,
        boxWidth: 7, boxHeight: 7, boxPadding: 3,
      },
    },
    scales: props.type === 'line' || props.type === 'bar' ? {
      x: {
        grid: { color: gridColor, drawBorder: false },
        ticks: { color: tickColor, font: { family: 'JetBrains Mono', size: 10 }, maxRotation: 0, autoSkipPadding: 20 },
        border: { display: false },
        // The caller's x options must survive: Chart.js needs `stacked` on the
        // INDEX scale too, or a stacked bar chart silently renders grouped and
        // the y-axis title reads as a total it isn't.
        ...(props.options?.scales?.x || {}),
      },
      y: {
        // `beginAtZero` anchors a non-negative series without clipping. A hard
        // `min: 0` here would hide every negative point (push−pop delta,
        // partition deletions), so signed charts are the caller's to bound.
        beginAtZero: true,
        grid: { color: gridColor, drawBorder: false },
        ticks: { color: tickColor, font: { family: 'JetBrains Mono', size: 10 }, padding: 8 },
        border: { display: false },
        ...(props.options?.scales?.y || {}),
      },
    } : undefined,
  }
}

function createChart() {
  if (!canvas.value) return
  if (chart) { chart.destroy(); chart = null }

  const ctx = canvas.value.getContext('2d')
  const mergedOpts = { ...getThemeOptions() }
  if (props.options?.plugins) mergedOpts.plugins = { ...mergedOpts.plugins, ...props.options.plugins }
  if (props.options?.scales?.y?.title) {
    mergedOpts.scales.y.title = { ...props.options.scales.y.title, color: isDark.value ? chartTheme.axisTitle : '#8a8a86' }
  }

  chart = new Chart(ctx, {
    type: props.type,
    data: buildChartData(props.data),
    options: mergedOpts,
  })
}

watch(() => props.data, () => {
  if (chart) {
    chart.data = buildChartData(props.data)
    chart.update('none')
  } else {
    createChart()
  }
}, { deep: true })

watch(isDark, () => { createChart() })

onMounted(() => { nextTick(createChart) })
onUnmounted(() => { if (chart) { chart.destroy(); chart = null } })
</script>
