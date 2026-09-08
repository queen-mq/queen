// Chart theme — single source of truth for all Chart.js colors.
//
// Philosophy: monochrome by default. Red / green / yellow are reserved for
// status: error / failure / DLQ → red, healthy / success / completed →
// green, warning / threshold-breach → yellow. They are NEVER assigned by
// series index, only by data semantics. Charts that just need to tell N
// series apart use the grey ramp below; the legend / tooltip carries the
// rest of the meaning.
//
// Everything here RESOLVES FROM the active token set in style.css instead of
// restating it. This file used to hold 39 colour literals and zero var()
// references, which meant a palette change repainted the whole app except the
// charts — and the charts paint most of the pixels on Analytics, Dashboard and
// QueueOperations. The literals below are fallbacks for the one case
// getComputedStyle cannot serve (a chart built before the stylesheet
// applies); keep them equal to the dark tokens they mirror.
//
// There are two token sets now (`:root` and `html.light`), so resolution is no
// longer a one-shot: every exported container is REACTIVE and refreshed in
// place when the theme changes, and `themeVersion` ticks so the two components
// that build a Chart.js instance imperatively (BaseChart, RowChart) can
// rebuild it. Nothing here reads `theme` to branch on a colour — the branch
// lives entirely in CSS.

import { reactive, ref, watch } from 'vue'
import { theme } from './useTheme.js'

const FALLBACK = {
  '--text-hi': '#f5f5f5',
  '--text-mid': '#9e9e9e',
  '--text-low': '#808080',
  '--ink-3': '#171717',
  '--bd': '#262626',
  '--bd-hi': '#404040',
  '--ok-500': '#4ade80',
  '--warn-400': '#e6b450',
  '--ember-400': '#fb7185',
  '--ember-500': '#f43f5e',
  '--series-1': '#e6e6e6',
  '--series-2': '#8b8b8b',
  '--series-3': '#6a6a6a',
  '--series-4': '#b8b8b8',
  '--series-5': '#4a4a4a',
}

// Resolved from the live document, then cached. Chart.js option objects are
// rebuilt on every render, so a per-call getComputedStyle would run hundreds
// of times a minute; the cache is invalidated on exactly one event, a theme
// change, which is the only thing that can move these values.
const cache = reactive({ ...FALLBACK })

const readTokens = () => {
  let cs = null
  try {
    cs = typeof getComputedStyle === 'function' ? getComputedStyle(document.documentElement) : null
  } catch {
    cs = null // SSR / detached document
  }
  for (const [name, fb] of Object.entries(FALLBACK)) {
    const v = cs ? cs.getPropertyValue(name).trim() : ''
    cache[name] = v || fb
  }
}

const t = (name) => cache[name]

// Ticks on every theme change. Imperative Chart.js call sites watch this;
// anything inside a computed() re-runs on its own, because `cache` is
// reactive and every accessor below reads through it.
export const themeVersion = ref(0)

// `#rrggbb` → `rgba(r,g,b,a)`. Chart.js fills need an alpha channel and the
// tokens are opaque hex, so the alpha ladder lives here once instead of as
// hand-typed rgba() triples that drift from the token they shadow.
export const alpha = (color, a) => {
  const hex = String(color).trim()
  const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(hex)
  if (!m) return hex // already rgba()/oklch()/named — hand it back untouched
  let h = m[1]
  if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2]
  const n = parseInt(h, 16)
  return `rgba(${(n >> 16) & 255},${(n >> 8) & 255},${n & 255},${a})`
}

// Cycling series palette — all greys, no semantic colors. Use chartColor(i)
// or chartColors[i] for generic N-series charts where index has no health
// meaning (per-replica, per-queue, push/pop/ack split, etc). The order
// alternates light/dark on purpose so ADJACENT indices stay apart: sorted by
// luminance the ramp steps 1.65×–2.10× between neighbours. In light mode the
// ramp inverts (dark greys on a white card) and keeps the same separation.
const SERIES = ['--series-1', '--series-2', '--series-3', '--series-4', '--series-5']

export const chartPalette = reactive(SERIES.map(() => ({ line: '', fill: '' })))

// Semantic colors — call by name when the data itself carries a meaning.
// Do not cycle through these; pick the one that matches what the series
// represents (see stateColor() for label-driven lookup).
export const semanticColors = reactive({
  ok: { line: '', fill: '' },
  warn: { line: '', fill: '' },
  bad: { line: '', fill: '' },
  badStrong: { line: '', fill: '' },
})

// Grid / tick / tooltip / axis colors for Chart.js options. Every value is a
// token, so the chart chrome is literally the same material as the panels
// around it.
export const chartTheme = reactive({
  grid: '',
  tick: '',
  axisTitle: '',
  tooltipBg: '',
  tooltipBorder: '',
  tooltipText: '',
  tooltipBody: '',
  fontFamily: "'JetBrains Mono', ui-monospace, monospace",
  fontFamilyUI: "'Inter', ui-sans-serif, system-ui, sans-serif",
})

// Shortcut palettes for common chart shapes (keep legacy call sites happy).
// Mutated in place, never reassigned: call sites hold the array itself.
export const chartColors = reactive([])
export const chartFills = reactive([])

/**
 * Re-read every token from the document and repaint the exported containers
 * in place. Safe to call at any time; called on boot and on theme change.
 */
export const refreshChartTheme = () => {
  readTokens()

  SERIES.forEach((name, i) => {
    const line = t(name)
    // The darkest slot needs a heavier fill to read at all against the card.
    chartPalette[i].line = line
    chartPalette[i].fill = alpha(line, i === 4 ? 0.18 : 0.1)
  })

  semanticColors.ok.line = t('--ok-500')
  semanticColors.ok.fill = alpha(t('--ok-500'), 0.12)
  semanticColors.warn.line = t('--warn-400')
  semanticColors.warn.fill = alpha(t('--warn-400'), 0.12)
  semanticColors.bad.line = t('--ember-400')
  semanticColors.bad.fill = alpha(t('--ember-400'), 0.12)
  semanticColors.badStrong.line = t('--ember-500')
  semanticColors.badStrong.fill = alpha(t('--ember-500'), 0.18)

  chartTheme.grid = t('--bd')
  chartTheme.tick = t('--text-low')
  chartTheme.axisTitle = t('--text-low')
  chartTheme.tooltipBg = t('--ink-3')
  chartTheme.tooltipBorder = t('--bd-hi')
  chartTheme.tooltipText = t('--text-hi')
  chartTheme.tooltipBody = t('--text-mid')

  chartColors.splice(0, chartColors.length, ...chartPalette.map((c) => c.line))
  chartFills.splice(0, chartFills.length, ...chartPalette.map((c) => c.fill))

  themeVersion.value++
  return themeVersion.value
}

refreshChartTheme()

// The class on <html> is applied synchronously by setTheme() before this
// watcher's callback runs, so getComputedStyle already sees the new set.
watch(theme, () => { refreshChartTheme() })

// Distribution / state color lookup by semantic label.
// Use for doughnut/pie charts where each slice has a named meaning.
// Only labels that genuinely encode status get a colored slot; neutral
// operations (push, pop, ack, ingested, processed, retention, …) all get
// greys so the eye is only drawn to actual problems.
export const stateColor = (label) => {
  const key = String(label || '').toLowerCase()
  const slice = (c, a = 0.75) => ({ line: c, fill: alpha(c, a) })
  if (key.includes('complet') || key === 'ok' || key === 'healthy' || key === 'success' || key === 'stable')
    return slice(t('--ok-500'))
  if (key.includes('dlq') || key.includes('dead') || key.includes('fail') || key === 'error' || key === 'bad' || key === 'stuck')
    return slice(t('--ember-400'))
  if (key === 'warn' || key === 'warning' || key === 'lag' || key === 'lagging' || key === 'evicted' || key === 'eviction')
    return slice(t('--warn-400'))
  if (key === 'pending' || key.includes('queue') || key === 'ingested' || key === 'push' || key.includes('produc'))
    return slice(t('--series-1'), 0.8) // primary grey
  if (key === 'processing' || key === 'processed' || key === 'pop' || key.includes('consum'))
    return slice(t('--series-2'), 0.8) // secondary grey
  return slice(t('--series-3')) // fallback tertiary grey
}

// Convenience accessors used throughout views / Chart.js configs.
export const chartColor = (i) => chartPalette[i % chartPalette.length]

// Helper: build a backgroundColor array for bar/doughnut series by label.
export const seriesBackgrounds = (labels, variant = 'fill') =>
  labels.map((l) => stateColor(l)[variant])

export const seriesBorders = (labels) =>
  labels.map((l) => stateColor(l).line)
