<template>
  <!--
    One metric as a tile: label, value, one line of context, and the same
    compact RowChart MetricRow draws, bleeding to the tile's edges. The API is
    MetricRow's (same props, same slots), with `selected` in place of
    `expanded`: a tile does not grow, it hands its series to the page's focus
    chart. Choosing is the tile's only action; a tile that also navigates
    (`clickable`, e.g. Errors -> Dead letter) says so in its context line.
  -->
  <div
    class="mt"
    :class="{ 'mt-selected': selected, 'mt-failed': !!error, [`mt-sev-${severity}`]: !!severity && !error }"
    role="button"
    tabindex="0"
    :aria-pressed="selected ? 'true' : 'false'"
    :title="tooltip || undefined"
    @click="onClick"
    @keydown.enter.prevent="onClick"
    @keydown.space.prevent="onClick"
  >
    <span class="mt-top">
      <span class="mt-label">{{ label }}<i v-if="scope === 'cell'" class="mt-scope" title="Covers every tenant on this cell">cell</i></span>
      <span v-if="legend.length === 2" class="mt-legend" aria-hidden="true">
        <span v-for="l in legend" :key="l.label"><i :class="`mt-sw mt-sw-${l.slot}`" />{{ l.label.toLowerCase() }}</span>
      </span>
      <span v-else-if="!error && (severity === 'warn' || severity === 'bad')" class="g" :class="severity" aria-hidden="true" />
    </span>

    <span class="mt-value">
      <!-- Unknown is NEVER zero: a failed source says so instead of a value. -->
      <span v-if="error" class="mt-na">unavailable</span>
      <template v-else>
        <slot name="value">
          <span v-if="loading" class="skeleton" style="display:inline-block; width:64px; height:20px; vertical-align:middle;" />
          <template v-else>
            <span class="num" :class="severity">{{ formattedValue }}</span><i v-if="unit" class="mt-unit">{{ unit }}</i>
          </template>
        </slot>
      </template>
    </span>

    <span class="mt-context" :class="{ 'mt-context-err': !!error }">
      <template v-if="error">{{ errorText }}</template>
      <slot v-else name="context">{{ context }}</slot>
    </span>

    <span v-if="spark" class="mt-spark">
      <RowChart
        v-if="!error"
        :data="sparkline"
        :series="series"
        :labels="labels"
        :tone="sparkTone"
        :value-format="valueFormat"
        variant="compact"
      />
    </span>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import RowChart from './RowChart.vue'
import { describeApiError } from '@/api/errors'

const props = defineProps({
  label: { type: String, required: true },
  value: { type: [String, Number], default: '' },
  unit: { type: String, default: '' },
  context: { type: String, default: '' },
  sparkline: { type: Array, default: () => [] },
  series: { type: Array, default: null },
  labels: { type: Array, default: () => [] },
  valueFormat: { type: Function, default: null },
  severity: { type: String, default: '' },
  sparklineTone: { type: String, default: '' },
  loading: { type: Boolean, default: false },
  error: { type: [Object, String], default: null },
  scope: { type: String, default: '' },
  clickable: { type: Boolean, default: false },
  tooltip: { type: String, default: '' },
  selected: { type: Boolean, default: false },
  // A point-in-time figure with no series (batch efficiency) draws no trend
  // rather than an empty one that would read as "no data".
  spark: { type: Boolean, default: true },
})

const emit = defineEmits(['select', 'click'])

const errorText = computed(() =>
  typeof props.error === 'string' ? props.error : describeApiError(props.error)
)

// Same rule as MetricRow: the trend takes the tile's tone, and a tile with no
// verdict draws in the neutral ramp.
const sparkTone = computed(() => props.sparklineTone || (props.severity || 'mute'))

const formattedValue = computed(() => {
  const v = props.value
  if (v === null || v === undefined || v === '') return '—'
  return v
})

// Two lines get a legend in the header; one line needs none, and three or
// more are named in the context line instead (a tile is too narrow).
const legend = computed(() =>
  Array.isArray(props.series) ? props.series.map((s, i) => ({ label: s.label || '', slot: i + 1 })) : []
)

const onClick = () => {
  emit('select')
  if (props.clickable) emit('click')
}
</script>

<style scoped>
.mt {
  position: relative;
  display: flex; flex-direction: column;
  min-width: 0;
  padding: 12px 14px 0;
  background: var(--ink-2);
  border: 1px solid var(--bd);
  border-radius: var(--r-card);
  cursor: pointer;
  overflow: hidden;
  text-align: left;
  transition: border-color .12s var(--ease);
}
.mt:hover { border-color: var(--bd-hi); }
.mt-selected { border-color: var(--text-low); }

.mt-top { display: flex; align-items: center; gap: 8px; min-width: 0; font-size: 12px; color: var(--text-mid); }
.mt-label { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.mt-scope {
  font-style: normal; margin-left: 6px; font-size: 11px; color: var(--text-low);
}
.mt-top .g { margin-left: auto; }
.mt-legend { margin-left: auto; display: inline-flex; gap: 8px; font-size: 11px; color: var(--text-low); white-space: nowrap; }
.mt-legend span { display: inline-flex; align-items: center; gap: 4px; }
.mt-sw { display: inline-block; width: 10px; height: 2px; border-radius: 1px; background: var(--series-1); }
.mt-sw-2 { background: var(--series-2); }

.mt-value {
  margin-top: 6px;
  font-size: 20px; font-weight: 600; line-height: 1.2; letter-spacing: -0.015em;
  font-variant-numeric: tabular-nums; color: var(--text-hi);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.mt-value :deep(.num.warn) { color: var(--warn-400); }
.mt-value :deep(.num.bad) { color: var(--ember-400); }
.mt-unit, .mt-value :deep(.mr-unit) {
  font-style: normal; font-size: 12px; font-weight: 400; color: var(--text-low);
  margin-left: 4px; letter-spacing: 0;
}
.mt-value :deep(.mr-sep) { color: var(--text-faint); margin: 0 4px; font-weight: 400; }
.mt-na { font-size: 13px; font-weight: 500; font-style: italic; color: var(--text-mid); }

.mt-context {
  margin-top: 3px; font-size: 12px; color: var(--text-low);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  font-variant-numeric: tabular-nums;
}
.mt-context-err { color: var(--ember-400); }

.mt-spark { display: block; margin: 10px -14px 0; height: 38px; }
.mt-failed .mt-spark { height: 12px; }
.mt:not(:has(.mt-spark)) { padding-bottom: 14px; }
</style>
