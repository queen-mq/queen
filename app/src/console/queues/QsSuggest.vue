<script setup>
// An input with the values the cluster already uses under it (namespaces,
// tasks), each with how many queues carry it. A new value is typed as is.
// Escape closes the list before the dialog around it sees the key.
import { computed, onBeforeUnmount, ref, watch } from 'vue'

const props = defineProps({
  modelValue: { type: String, default: '' },
  options: { type: Array, default: () => [] }, // [{ value, count }]
  id: { type: String, required: true },
  placeholder: { type: String, default: '' },
  describedby: { type: String, default: '' },
})
const emit = defineEmits(['update:modelValue'])

const open = ref(false)
const hi = ref(-1)
const list = computed(() => {
  const t = (props.modelValue || '').trim().toLowerCase()
  return props.options
    .filter((o) => o.value && o.value !== props.modelValue && (!t || o.value.toLowerCase().includes(t)))
    .slice(0, 6)
})
const shown = computed(() => open.value && list.value.length > 0)

function pick(v) { emit('update:modelValue', v); open.value = false; hi.value = -1 }
function onInput(e) { emit('update:modelValue', e.target.value); open.value = true; hi.value = -1 }
function onKey(e) {
  if (e.key === 'ArrowDown') { e.preventDefault(); open.value = true; hi.value = Math.min(list.value.length - 1, hi.value + 1) }
  else if (e.key === 'ArrowUp') { e.preventDefault(); hi.value = Math.max(0, hi.value - 1) }
  else if (e.key === 'Enter' && shown.value && list.value[hi.value]) { e.preventDefault(); pick(list.value[hi.value].value) }
}
// QcDialog closes on Escape from a capture listener on document; one on
// window runs first, so an open list closes alone.
function onEsc(e) {
  if (e.key !== 'Escape' || !shown.value) return
  e.stopPropagation(); e.preventDefault()
  open.value = false
}
watch(shown, (s) => {
  if (s) window.addEventListener('keydown', onEsc, true)
  else window.removeEventListener('keydown', onEsc, true)
})
onBeforeUnmount(() => window.removeEventListener('keydown', onEsc, true))
</script>

<template>
  <div class="qs-sug">
    <span class="qn-field">
      <input
        :id="id"
        :value="modelValue"
        class="qn-mono"
        type="text"
        autocomplete="off"
        spellcheck="false"
        role="combobox"
        aria-autocomplete="list"
        :aria-expanded="shown"
        :aria-controls="`${id}-list`"
        :aria-activedescendant="shown && hi >= 0 ? `${id}-opt-${hi}` : undefined"
        :aria-describedby="describedby || undefined"
        :placeholder="placeholder"
        @input="onInput"
        @focus="open = true"
        @blur="open = false"
        @keydown="onKey"
      />
    </span>
    <div v-if="shown" :id="`${id}-list`" class="qn-pop qs-sug-list" role="listbox">
      <button
        v-for="(o, i) in list"
        :id="`${id}-opt-${i}`"
        :key="o.value"
        type="button"
        class="qn-opt"
        :class="{ hover: i === hi }"
        role="option"
        :aria-selected="i === hi"
        tabindex="-1"
        @mousedown.prevent
        @click="pick(o.value)"
      >
        <span class="qn-mono qs-sug-name">{{ o.value }}</span>
        <span class="qn-num qn-t3" style="font-size: 12px">{{ o.count.toLocaleString('en-US') }} {{ o.count === 1 ? 'queue' : 'queues' }}</span>
      </button>
    </div>
  </div>
</template>

<style scoped>
.qs-sug { position: relative; margin-top: 6px; }
.qs-sug-list { left: 0; right: 0; top: calc(100% + 4px); }
.qs-sug-name { flex: 1 1 0; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
/* the highlighted row is a hover, not a pick */
.qs-sug-list .qn-opt[aria-selected='true'] { background: var(--bg2); font-weight: 400; }
</style>
