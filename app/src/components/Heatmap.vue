<template>
  <div class="hm">
    <div v-for="row in rows" :key="row.key ?? row.name" class="hm-row">
      <button class="hm-name" :title="row.name" @click="$emit('select', row)">
        <i v-if="row.color" class="hm-swatch" :style="{ background: row.color }"></i>{{ row.name }}
      </button>
      <div class="hm-cells" :style="{ gridTemplateColumns: `repeat(${buckets.length}, 1fr)` }">
        <i
          v-for="(cell, ci) in row.values"
          :key="ci"
          class="hm-cell"
          :class="{ na: cell.value === null }"
          :style="cell.value === null ? null : { background: ink(cell.percent) }"
          :title="tip(row, ci, cell)"
        />
      </div>
      <span class="hm-total">{{ fmt.n(row.total) }}</span>
    </div>

    <div v-if="buckets.length" class="hm-axis">
      <span></span>
      <div class="ax">
        <span>{{ axisLabel(0) }}</span>
        <span>{{ axisLabel(Math.floor(buckets.length / 2)) }}</span>
        <span>{{ axisLabel(buckets.length - 1) }} UTC</span>
      </div>
      <span class="ax hm-axis-end">total</span>
    </div>
  </div>
</template>

<script setup>
import { formatters as fmt } from '@/composables/useWorkload'

// A rows x buckets grid of ink, no chart library: one <i> per cell, shaded by
// mixing --text-hi into the card ground. It is the only panel that can show
// every row at once — a 60-series stacked area cannot — and the cell tooltip
// is the browser's own, so there is nothing to keep in sync with a canvas.
//
// A null cell (no metrics row for that bucket) renders as an outline, NOT as
// the palest shade: an unmeasured bucket must not look like a quiet one.
const props = defineProps({
  /** [{ key?, name, color?, values: [{ value, percent }], total }] — see heatCells(). */
  rows: { type: Array, default: () => [] },
  /** Bucket start timestamps, one per column. */
  buckets: { type: Array, default: () => [] },
  /** Prefix the axis labels with the date (the window spans more than a day). */
  multiDay: { type: Boolean, default: false },
})

defineEmits(['select'])

const ink = (percent) => `color-mix(in srgb, var(--text-hi) ${percent}%, var(--ink-3))`

const axisLabel = (i) => {
  const b = props.buckets[i]
  return b ? fmt.bucket(b, props.multiDay) : ''
}

const tip = (row, ci, cell) => {
  const b = props.buckets[ci]
  const at = b ? `${fmt.bucket(b, props.multiDay)} UTC` : ''
  return cell.value === null
    ? `${row.name} · ${at} · no metrics row`
    : `${row.name} · ${at} · ${fmt.n(cell.value)} delivered`
}
</script>

<style scoped>
.hm { display: flex; flex-direction: column; gap: 2px; }
.hm-row { display: grid; grid-template-columns: 150px 1fr 72px; align-items: center; gap: 8px; }
.hm-name {
  font-size: 11px; color: var(--text-mid); text-align: left; background: none; border: none;
  padding: 0; cursor: pointer; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.hm-name:hover { color: var(--text-hi); }
.hm-swatch { display: inline-block; width: 7px; height: 7px; border-radius: 2px; margin-right: 6px; vertical-align: 1px; }
.hm-cells { display: grid; gap: 1px; height: 14px; }
.hm-cell { display: block; height: 100%; border-radius: 1px; background: var(--ink-3); }
.hm-cell.na { background: transparent; box-shadow: inset 0 0 0 1px var(--bd); }
.hm-total { font-size: 11px; color: var(--text-mid); text-align: right; }
.hm-axis { display: grid; grid-template-columns: 150px 1fr 72px; gap: 8px; margin-top: 4px; }
.hm-axis .ax { display: flex; justify-content: space-between; font-size: 10px; color: var(--text-low); }
.hm-axis-end { justify-content: flex-end; }
</style>
