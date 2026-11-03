<script setup>
// A panel that opens from a control in the band or the rail. Closes on a
// click outside, on Escape (focus goes back to its control) and when the
// route changes.
//
// The panel is placed on the screen, not inside its control's box: fixed,
// from the control's position, and kept 8 px inside the window. So it never
// opens past an edge (a phone, a control near one), and the phone's rail,
// which scrolls sideways, does not cut it off. It follows its control while
// the page scrolls.
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

const props = defineProps({
  open: { type: Boolean, default: false },
  // where the panel sits against its control
  side: { type: String, default: 'bottom-end' }, // bottom-end | bottom-start | right-end
  width: { type: [Number, String], default: 280 },
  label: { type: String, default: '' },
  focusFirst: { type: Boolean, default: true },
})
const emit = defineEmits(['update:open'])

const root = ref(null)
const panel = ref(null)
const route = useRoute()

function close(refocus = false) {
  emit('update:open', false)
  if (refocus) nextTick(() => root.value?.querySelector('[aria-haspopup]')?.focus?.({ preventScroll: true }))
}
function toggle() { emit('update:open', !props.open) }
function onDoc(e) { if (root.value && !root.value.contains(e.target)) close() }
function onKey(e) {
  if (e.key !== 'Escape') return
  e.stopPropagation()
  close(true)
}
function bind(on) {
  const f = on ? document.addEventListener : document.removeEventListener
  f.call(document, 'pointerdown', onDoc, true)
  f.call(document, 'keydown', onKey, true)
  const w = on ? window.addEventListener : window.removeEventListener
  w.call(window, 'scroll', place, true)
  w.call(window, 'resize', place)
}

// ---- where the panel goes ----
const EDGE = 8 // kept this far inside the window
const pos = ref(null) // { left, top | bottom, width, maxHeight } in window px
function place() {
  if (!props.open || !root.value) return
  const el = root.value.firstElementChild && root.value.firstElementChild !== panel.value ? root.value.firstElementChild : root.value
  const t = el.getBoundingClientRect()
  const vw = document.documentElement.clientWidth || window.innerWidth
  const vh = window.innerHeight
  const asked = typeof props.width === 'number' ? props.width : parseFloat(props.width) || 280
  const width = Math.max(0, Math.min(asked, vw - 2 * EDGE))
  const inside = (x) => Math.max(EDGE, Math.min(x, vw - EDGE - width))
  let p
  if (props.side === 'right-end' && t.right + 10 + width <= vw - EDGE) {
    // beside the control, its bottom on the control's bottom (the rail's account)
    p = { left: t.right + 10, bottom: vh - t.bottom, maxHeight: t.bottom - EDGE }
  } else {
    // under the control; on the right of a rail that is a top bar (a phone), under it too
    const left = props.side === 'bottom-start' ? inside(t.left) : inside(t.right - width)
    const below = vh - (t.bottom + 8) - EDGE
    const above = t.top - 8 - EDGE
    p = below < 200 && above > below
      ? { left, bottom: vh - t.top + 8, maxHeight: above }
      : { left, top: t.bottom + 8, maxHeight: below }
  }
  pos.value = { ...p, width }
}

watch(() => props.open, async (o) => {
  bind(o)
  if (o) place()
  else pos.value = null
  if (o && props.focusFirst) {
    await nextTick()
    panel.value?.querySelector('input, [aria-pressed="true"], button, a[href]')?.focus?.({ preventScroll: true })
  }
}, { immediate: true })
watch(() => route.fullPath, () => { if (props.open) close() })
onMounted(() => { if (props.open) place() })
onBeforeUnmount(() => bind(false))

const style = computed(() => {
  const p = pos.value
  if (!p) return { position: 'fixed', visibility: 'hidden' }
  const s = { position: 'fixed', left: `${p.left}px`, width: `${p.width}px`, maxHeight: `${Math.max(120, p.maxHeight)}px`, overflowY: 'auto' }
  if (p.top != null) s.top = `${p.top}px`
  else s.bottom = `${p.bottom}px`
  return s
})
</script>

<template>
  <div ref="root" class="qn-popwrap">
    <slot name="trigger" :open="open" :toggle="toggle" />
    <div v-if="open" ref="panel" class="qn-pop" role="dialog" :aria-label="label" :style="style">
      <slot :close="close" />
    </div>
  </div>
</template>

<style scoped>
.qn-popwrap { position: relative; }
</style>
