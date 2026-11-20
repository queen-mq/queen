<script setup>
// The time window: Live with its clock, the presets, a From and To on any
// day, and "Back to live" while looking at another time. In the band it is
// the page's chip; with `filter` it sits in a list's filters ("pushed last
// hour") and sets the same window, so the band and the list never disagree.
import { computed, ref, watch } from 'vue'
import QcPopover from './QcPopover.vue'
import QcIcon from './QcIcon.vue'
import { useWindow, clock, timeZone } from './useWindow'
import { atText, rangeOf } from './windowTime'
import { toast } from './toasts'

const props = defineProps({ filter: { type: Boolean, default: false } })
const { win, pick, range, live, presets, nowMs } = useWindow()
const open = ref(false)
const from = ref('')
const to = ref('')

// A moving window opens at whole minutes: its 10 s steps are no time anyone typed.
const minute = (ms) => Math.floor(ms / 60_000) * 60_000
watch(open, (o) => {
  if (!o) return
  from.value = atText(win.value.custom ? win.value.from : minute(win.value.from))
  to.value = atText(win.value.custom ? win.value.to : minute(Date.now()))
})

function choose(id, close) { pick(id); close() }
function apply(close) {
  const r = rangeOf(from.value, to.value)
  if (r.error) {
    toast(r.error[0], r.error[1])
    return
  }
  range(r.from, r.to)
  close()
}
const tz = timeZone()
// The filter's words: "pushed" + the window.
const filterText = computed(() => (win.value.custom ? win.value.label : win.value.id === '1h' ? 'last hour' : win.value.long))
</script>

<template>
  <QcPopover v-model:open="open" :side="props.filter ? 'bottom-start' : 'bottom-end'" :width="300" label="Time window">
    <template #trigger="{ toggle }">
      <button v-if="props.filter" type="button" class="qn-chip tc-filter" aria-haspopup="dialog" :aria-expanded="open" aria-label="Pushed in this time window" @click="toggle">
        <span class="qn-t3">pushed</span><span class="qn-num tc-val">{{ filterText }}</span>
        <QcIcon name="chevron" :size="12" />
      </button>
      <button v-else type="button" class="qn-chip" aria-haspopup="dialog" :aria-expanded="open" aria-label="Time window" @click="toggle">
        <template v-if="win.id === '1h'"><span>Live</span><span class="qn-num qn-t3 qn-clock">{{ clock(nowMs, true) }}</span></template>
        <span v-else class="qn-num">{{ win.label }}</span>
        <QcIcon name="chevron" :size="12" />
      </button>
    </template>
    <template #default="{ close }">
      <button
        v-for="p in presets"
        :key="p.id"
        type="button"
        class="qn-opt"
        :aria-pressed="win.id === p.id"
        @click="choose(p.id, close)"
      >
        <span style="flex: 1 1 0">{{ p.label }}</span>
        <span v-if="p.id === '1h'" class="qn-kbd">L</span>
      </button>
      <div class="qn-popsep" />
      <p class="qn-t3 tc-small" style="padding: 4px 10px 6px">From and to</p>
      <div class="tc-at">
        <label class="qn-field"><span class="qn-t3 tc-k">From</span><input v-model="from" class="qn-mono" type="text" spellcheck="false" autocomplete="off" aria-label="From" @keydown.enter="apply(close)" /></label>
        <label class="qn-field"><span class="qn-t3 tc-k">To</span><input v-model="to" class="qn-mono" type="text" spellcheck="false" autocomplete="off" aria-label="To" @keydown.enter="apply(close)" /></label>
      </div>
      <p class="qn-t3 tc-small" style="padding: 6px 10px 0">A time alone, like 14:05, is today.</p>
      <div style="padding: 8px 10px 4px">
        <button type="button" class="qn-btn2" style="width: 100%" @click="apply(close)">Show this time</button>
      </div>
      <p class="qn-t3 tc-small" style="padding: 8px 10px 6px; border-top: 1px solid var(--line2); margin-top: 6px">
        Times in {{ tz }}. Drag across a chart to zoom in. Small lines in lists always show the last hour.
      </p>
    </template>
  </QcPopover>
  <button v-if="!props.filter && win.id !== '1h'" type="button" class="qn-chip" @click="live()">Back to live</button>
</template>

<style scoped>
.tc-small { font-size: 12px; }
.tc-at { display: grid; gap: 6px; padding: 0 10px; }
.tc-k { flex: 0 0 38px; font-size: 12px; }
.tc-filter { gap: 6px; max-width: 100%; }
.tc-val { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
/* a phone: "Live" alone, the band's second row is narrow */
@media (max-width: 640px) { .qn-clock { display: none; } }
</style>
