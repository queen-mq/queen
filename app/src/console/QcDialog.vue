<script setup>
// A confirm: it names the cluster the action lands on, shows what changes and
// the HTTP call, and asks to type the name first when the action destroys.
import { computed, nextTick, onBeforeUnmount, ref, watch } from 'vue'

const props = defineProps({
  open: { type: Boolean, default: false },
  title: { type: String, required: true },
  where: { type: String, default: '' },         // the cluster: "on stage"
  call: { type: String, default: '' },          // "POST /api/v1/configure {...}"
  confirmLabel: { type: String, default: 'Confirm' },
  danger: { type: Boolean, default: false },
  typed: { type: String, default: '' },         // the name to type before a destructive action
  busy: { type: Boolean, default: false },
  blocked: { type: Boolean, default: false },   // the form is not ready (an invalid payload)
  cancelLabel: { type: String, default: 'Cancel' },
})
const emit = defineEmits(['confirm', 'cancel'])

const box = ref(null)
const text = ref('')
const ready = computed(() => !props.busy && !props.blocked && (!props.typed || text.value.trim() === props.typed))
let before = null

function onKey(e) {
  if (e.key === 'Escape') { e.stopPropagation(); emit('cancel') }
  if (e.key === 'Tab' && box.value) {
    const f = [...box.value.querySelectorAll('input, textarea, select, button:not(:disabled), a[href]')]
    if (!f.length) return
    const first = f[0]; const last = f[f.length - 1]
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus() }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus() }
  }
}
watch(() => props.open, async (o) => {
  if (o) {
    text.value = ''
    before = document.activeElement
    document.addEventListener('keydown', onKey, true)
    await nextTick()
    ;(box.value?.querySelector('input') || box.value?.querySelector('[data-cancel]'))?.focus()
  } else {
    document.removeEventListener('keydown', onKey, true)
    before?.focus?.({ preventScroll: true })
  }
})
onBeforeUnmount(() => document.removeEventListener('keydown', onKey, true))
</script>

<template>
  <Transition name="qn-fade">
    <div v-if="open" class="qn-scrim" @pointerdown.self="emit('cancel')">
      <div ref="box" class="qn-dialog" role="dialog" aria-modal="true" aria-labelledby="qn-dlg-title">
        <div style="display: flex; align-items: center; gap: 10px">
          <h2 id="qn-dlg-title" style="font-size: 16px; font-weight: 600; flex: 1 1 0; min-width: 0">{{ title }}</h2>
          <span v-if="where" class="qn-where">on {{ where }}</span>
        </div>
        <div style="margin-top: 14px"><slot /></div>
        <label v-if="typed" style="display: block; margin-top: 16px">
          <span class="qn-t3" style="font-size: 12px">Type <span class="qn-mono" style="color: var(--text)">{{ typed }}</span> to confirm</span>
          <span class="qn-field" style="margin-top: 6px"><input v-model="text" class="qn-mono" type="text" autocomplete="off" spellcheck="false" /></span>
        </label>
        <p v-if="call" class="qn-call" style="margin-top: 14px">{{ call }}</p>
        <div style="display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 8px; margin-top: 18px">
          <button type="button" class="qn-btn2" data-cancel @click="emit('cancel')">{{ cancelLabel }}</button>
          <button type="button" :class="danger ? 'qn-btn2 danger' : 'qn-btn'" :disabled="!ready" @click="emit('confirm')">
            {{ busy ? 'Sending…' : confirmLabel }}
          </button>
        </div>
      </div>
    </div>
  </Transition>
</template>

<style scoped>
.qn-scrim { position: fixed; inset: 0; z-index: 55; background: var(--scrim); display: flex; align-items: flex-start; justify-content: center; padding: 96px 16px 16px; overflow-y: auto; }
.qn-dialog { width: 520px; max-width: 100%; background: var(--pop); border: 1px solid var(--line); border-radius: calc(var(--r-pop) + 2px); padding: 20px 22px; box-shadow: 0 24px 64px rgba(0, 0, 0, .4); }
:global(.qn.tone .qn-dialog) { border-color: transparent; border-radius: 16px; }
</style>
