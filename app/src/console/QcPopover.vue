<script setup>
// A panel that opens from a control in the band or the rail. Closes on a
// click outside, on Escape (focus goes back to its control) and when the
// route changes.
import { computed, nextTick, onBeforeUnmount, ref, watch } from 'vue'
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
}
watch(() => props.open, async (o) => {
  bind(o)
  if (o && props.focusFirst) {
    await nextTick()
    panel.value?.querySelector('input, [aria-pressed="true"], button, a[href]')?.focus?.({ preventScroll: true })
  }
}, { immediate: true })
watch(() => route.fullPath, () => { if (props.open) close() })
onBeforeUnmount(() => bind(false))

const style = computed(() => {
  const w = typeof props.width === 'number' ? props.width + 'px' : props.width
  const s = { width: w }
  if (props.side === 'bottom-end') Object.assign(s, { right: 0, top: 'calc(100% + 8px)' })
  else if (props.side === 'bottom-start') Object.assign(s, { left: 0, top: 'calc(100% + 8px)' })
  else if (props.side === 'right-end') Object.assign(s, { left: 'calc(100% + 10px)', bottom: 0 })
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
