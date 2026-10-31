<script setup>
// The activity map: every row that delivered something, every bucket of the
// window, in grey, stronger for more (lighter on the dark sheet, darker on
// paper). It is the one view that shows all of them at once, which the
// charts above cannot. A bucket no metrics row covers is an outline:
// unmeasured is not quiet. A name looks inside the group, or opens the
// queue's consumer groups.
import { computed } from 'vue'
import { stamp } from '../useWindow'
import { fmt } from './workloadViews'

const props = defineProps({
  model: { type: Object, required: true },     // mapModel()
  win: { type: Object, required: true },
  level: { type: String, default: 'namespace' },
  picked: { type: String, default: '' },
  bucketMinutes: { type: Number, default: 1 },
})
const emit = defineEmits(['open'])

const per = computed(() => {
  const m = props.bucketMinutes
  return m === 1 ? 'minute' : m === 60 ? 'hour' : m % 60 === 0 ? `${m / 60} hours` : `${m} minutes`
})
const withDay = computed(() => props.win.to - props.win.from >= 86_400_000)
const ink = (p) => `color-mix(in srgb, var(--text) ${Math.max(0, Math.min(100, p))}%, var(--wlm-ground))`
const tip = (r, i, c) => {
  const t = props.model.times[i]
  const at = Number.isFinite(t) ? stamp(t, withDay.value) : ''
  return c.na ? `${r.name}, ${at}: no metrics row` : `${r.name}, ${at}: ${fmt(c.v)} delivered`
}
const idleText = computed(() => {
  const n = props.model.idle
  if (!n.length) return ''
  const word = props.level === 'queue' ? (n.length === 1 ? 'queue' : 'queues') : props.level === 'task' ? (n.length === 1 ? 'task' : 'tasks') : (n.length === 1 ? 'namespace' : 'namespaces')
  if (n.length <= 5) return `Not drawn, since they delivered nothing: ${n.join(', ')}.`
  return `Not drawn: ${fmt(n.length)} ${word} that delivered nothing.`
})
// The window's own three times, as under the charts: the cells span it, less
// the cut buckets at its two ends.
const axis = computed(() => props.win.axis || ['', '', ''])
</script>

<template>
  <section class="wlm" aria-labelledby="wlm-title">
    <div class="wlm-head">
      <h2 id="wlm-title" class="wlm-title">Activity map, {{ win.long }}</h2>
      <span class="qn-t3">deliveries a {{ per }}, the stronger the cell, the more</span>
    </div>
    <p v-if="!model.rows.length" class="qn-t2 wlm-none">Nothing was delivered in this window.</p>
    <div v-else class="wlm-grid" role="img" :aria-label="`Deliveries a ${per} for each of ${model.rows.length} rows, ${win.long}`">
      <div v-for="r in model.rows" :key="r.key" class="wlm-row" :class="{ on: picked === r.key }">
        <button type="button" class="wlm-name" :class="r.mono ? 'qn-mono' : 'qn-t2'" :title="level === 'queue' ? `${r.name}: its consumer groups` : `${r.name}: look at its queues`" @click="emit('open', r.key)">{{ r.name }}</button>
        <div class="wlm-cells" :style="{ gridTemplateColumns: `repeat(${r.cells.length}, minmax(0, 1fr))` }">
          <i v-for="(c, i) in r.cells" :key="i" class="wlm-cell" :class="{ na: c.na }" :style="c.na ? null : { background: ink(c.p) }" :title="tip(r, i, c)" />
        </div>
      </div>
      <div class="wlm-row wlm-axis qn-num qn-t3" aria-hidden="true">
        <span />
        <div class="wlm-ax"><span>{{ axis[0] }}</span><span>{{ axis[1] }}</span><span>{{ axis[2] }}</span></div>
      </div>
    </div>
    <p v-if="idleText" class="qn-t2 wlm-idle">{{ idleText }}</p>
    <p v-if="model.rows.length" class="qn-read wlm-read">From the same read. The strongest cell is {{ fmt(model.max) }} deliveries in a {{ per }}; the shade follows the square root, so a quiet row still shows.</p>
  </section>
</template>

<style scoped>
.wlm { --wlm-ground: var(--bg2); }
:global(.qn.tone .wlm) { --wlm-ground: var(--ctl); }
.wlm-head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 16px; }
.wlm-title { font-size: 15px; font-weight: 500; }
.wlm-head .qn-t3 { font-size: 12px; }
.wlm-none { margin-top: 12px; }
.wlm-grid { display: flex; flex-direction: column; gap: 2px; margin-top: 14px; }
.wlm-row { display: grid; grid-template-columns: 200px minmax(0, 1fr); align-items: center; gap: 12px; }
.wlm-name { font-size: 12.5px; text-align: left; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text); padding: 1px 0; }
.wlm-name.qn-t2 { color: var(--text2); }
.wlm-name:hover { text-decoration: underline; text-underline-offset: 3px; }
.wlm-row.on .wlm-name { font-weight: 500; }
.wlm-cells { display: grid; gap: 1px; height: 14px; }
.wlm-cell { display: block; height: 100%; border-radius: 1px; background: var(--wlm-ground); }
.wlm-cell.na { background: transparent; box-shadow: inset 0 0 0 1px var(--bg3); }
.wlm-axis { margin-top: 6px; font-size: 11px; }
.wlm-ax { display: flex; justify-content: space-between; }
.wlm-idle { font-size: 12.5px; margin-top: 10px; }
.wlm-read { margin-top: 8px; }
@media (max-width: 640px) { .wlm-row { grid-template-columns: 96px minmax(0, 1fr); gap: 8px; } }
</style>
