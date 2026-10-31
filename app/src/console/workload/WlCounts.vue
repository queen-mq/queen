<script setup>
// The tenant's counts, whatever the rows below are narrowed to: what it holds
// now, and what the window added up to, each with its change against the
// window compared with (the popover picks which: the span before, the same
// time yesterday or last week, or none).
import { ref } from 'vue'
import QcPopover from '../QcPopover.vue'
import { COMPARE, compareWords } from './workloadViews'

const props = defineProps({
  counts: { type: Object, default: null },       // countsModel()
  nowLabel: { type: String, default: 'Now' },
  winLabel: { type: String, default: '' },
  compare: { type: String, default: 'previous' },
  compareLabel: { type: String, default: '' },
  win: { type: Object, required: true },
  prevError: { type: String, default: '' },
})
const emit = defineEmits(['compare'])
const open = ref(false)
const cap = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1) : s)
const optLabel = (m) => (m === 'off' ? 'No comparison' : cap(compareWords(m, props.win)))
</script>

<template>
  <section class="wlc qn-panel" aria-label="The tenant's counts">
    <div class="wlc-row">
      <div class="wlc-head"><p>{{ nowLabel }}</p></div>
      <dl class="wlc-cells">
        <div v-for="c in counts.now" :key="c.k" class="wlc-cell" :title="c.title || undefined">
          <dt>{{ c.label }}</dt>
          <dd class="qn-num wlc-v" :class="c.tone">{{ c.v }}</dd>
        </div>
      </dl>
    </div>
    <div class="wlc-sep" />
    <div class="wlc-row">
      <div class="wlc-head">
        <p>{{ winLabel }}</p>
        <QcPopover v-model:open="open" side="bottom-start" :width="250" label="Compare with">
          <template #trigger="{ toggle }">
            <button type="button" class="wlc-vs" aria-haspopup="dialog" :aria-expanded="open" @click="toggle">
              {{ compare === 'off' ? 'Compare with…' : `vs ${compareLabel}` }}
              <svg width="9" height="9" viewBox="0 0 10 10" aria-hidden="true"><polyline points="2,3.5 5,6.5 8,3.5" style="fill: none; stroke: currentColor; stroke-width: 1.3" /></svg>
            </button>
          </template>
          <template #default="{ close }">
            <button v-for="m in COMPARE" :key="m" type="button" class="qn-opt" :class="{ on: compare === m }" @click="emit('compare', m); close()">{{ optLabel(m) }}</button>
          </template>
        </QcPopover>
      </div>
      <dl class="wlc-cells">
        <div v-for="c in counts.window" :key="c.k" class="wlc-cell" :title="c.title || undefined">
          <dt>{{ c.label }}</dt>
          <dd class="wlc-v wlc-vd"><span class="qn-num" :class="c.tone">{{ c.v }}</span><span v-if="c.delta" class="qn-num qn-t3 wlc-d">{{ c.delta }}</span></dd>
        </div>
      </dl>
    </div>
    <p v-if="prevError" class="qn-t2 wlc-err">{{ cap(compareLabel) }} did not load: {{ prevError }}.</p>
  </section>
</template>

<style scoped>
.wlc { padding: 4px 20px; }
.wlc-row { display: grid; grid-template-columns: 132px minmax(0, 1fr); gap: 0 16px; align-items: start; padding: 14px 0; }
.wlc-sep { height: 1px; background: var(--line2); }
:global(.qn.tone .wlc-sep) { background: var(--bg3); }
.wlc-head { min-width: 0; font-size: 13px; font-weight: 500; padding-top: 1px; }
.wlc-vs { display: inline-flex; align-items: center; gap: 5px; margin-top: 4px; font-size: 12px; font-weight: 400; color: var(--text3); text-align: left; }
.wlc-vs:hover { color: var(--text); }
.wlc-cells { display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: 10px 16px; margin: 0; }
.wlc-cell { min-width: 0; }
.wlc-cell dt { font-size: 12px; color: var(--text3); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.wlc-cell dd { margin: 3px 0 0; }
.wlc-v { font-size: 17px; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
/* a change sits after its number, or under it when the cell is too narrow for both */
.wlc-vd { display: flex; flex-wrap: wrap; align-items: baseline; column-gap: 6px; overflow: visible; }
.wlc-vd > .qn-num:first-child { white-space: nowrap; }
.wlc-d { font-size: 11.5px; font-weight: 400; white-space: nowrap; }
.wlc-err { font-size: 12.5px; padding: 0 0 12px; }
@media (max-width: 1100px) {
  .wlc-row { grid-template-columns: minmax(0, 1fr); gap: 8px 0; }
  .wlc-head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 14px; }
  .wlc-vs { margin-top: 0; }
  .wlc-cells { grid-template-columns: repeat(auto-fill, minmax(112px, 1fr)); }
}
@media (max-width: 640px) {
  .wlc { padding: 2px 16px; }
  .wlc-row { padding: 12px 0; }
  .wlc-cells { grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px 12px; }
  .wlc-cell dt { font-size: 11.5px; }
  .wlc-v { font-size: 15px; }
  .wlc-d { flex-basis: 100%; margin-top: 1px; font-size: 11px; }
}
</style>
