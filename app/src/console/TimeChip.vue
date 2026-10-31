<script setup>
// The band's time window: Live with its clock, the presets, a from-to for
// today, and "Back to live" while looking at another time.
import { ref, watch } from 'vue'
import QcPopover from './QcPopover.vue'
import QcIcon from './QcIcon.vue'
import { useWindow, clock, todayAt, timeZone } from './useWindow'
import { toast } from './toasts'

const { win, pick, range, live, presets, nowMs } = useWindow()
const open = ref(false)
const from = ref('')
const to = ref('')

watch(open, (o) => {
  if (!o) return
  from.value = clock(win.value.from)
  to.value = win.value.custom ? clock(win.value.to) : clock(Date.now())
})

function choose(id, close) { pick(id); close() }
function apply(close) {
  const a = todayAt(from.value)
  const b = todayAt(to.value)
  if (a == null || b == null || a >= b) {
    toast('Use two times from today, the first one earlier', 'For example 09:00 and 09:40.')
    return
  }
  range(a, Math.min(b, Date.now()))
  close()
}
const tz = timeZone()
</script>

<template>
  <QcPopover v-model:open="open" side="bottom-end" :width="300" label="Time window">
    <template #trigger="{ toggle }">
      <button type="button" class="qn-chip" aria-haspopup="dialog" :aria-expanded="open" aria-label="Time window" @click="toggle">
        <template v-if="win.id === '1h'"><span>Live</span><span class="qn-num qn-t3">{{ clock(nowMs, true) }}</span></template>
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
      <p class="qn-t3" style="font-size: 12px; padding: 4px 10px 6px">From and to, today</p>
      <div style="display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px; padding: 0 10px">
        <label class="qn-field"><span class="qn-vh">From</span><input v-model="from" class="qn-mono" type="text" inputmode="numeric" aria-label="From" @keydown.enter="apply(close)" /></label>
        <label class="qn-field"><span class="qn-vh">To</span><input v-model="to" class="qn-mono" type="text" inputmode="numeric" aria-label="To" @keydown.enter="apply(close)" /></label>
      </div>
      <div style="padding: 8px 10px 4px">
        <button type="button" class="qn-btn2" style="width: 100%" @click="apply(close)">Show this time</button>
      </div>
      <p class="qn-t3" style="font-size: 12px; padding: 8px 10px 6px; border-top: 1px solid var(--line2); margin-top: 6px">
        Times in {{ tz }}. Drag across a chart to zoom in. Small lines in lists always show the last hour.
      </p>
    </template>
  </QcPopover>
  <button v-if="win.id !== '1h'" type="button" class="qn-chip" @click="live()">Back to live</button>
</template>
