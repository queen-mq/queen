<template>
  <!-- A line you may move: what it is on the left, its value on the right, and
       under the value the line it falls back to when the field is blank. -->
  <label class="line-row">
    <span class="line-row-label">{{ meta.label }}</span>
    <span class="line-row-help">
      <span v-if="error" class="line-row-invalid">{{ error }}</span>
      <template v-else>{{ meta.help }}</template>
    </span>
    <span class="line-row-control">
      <LineInput
        :model-value="modelValue"
        :unit="meta.unit"
        :placeholder="toInput(meta, fallback)"
        :label="meta.label"
        :invalid="!!error"
        :disabled="disabled"
        @update:model-value="emit('update:modelValue', $event)"
      />
      <span class="line-row-note">{{ note }}</span>
    </span>
  </label>
</template>

<script setup>
import { computed } from 'vue'

import LineInput from '@/components/LineInput.vue'
import { formatLine, formatSpan, fromInput, toInput } from '@/composables/settingsDoc'

const props = defineProps({
  /** One entry of composables/settingsDoc.js LINES. */
  meta: { type: Object, required: true },
  modelValue: { type: String, default: '' },
  /** The value in force when the field is blank. */
  fallback: { type: Number, required: true },
  /** Whose line that is: 'built-in' on the tenant's form, 'tenant' on a queue's. */
  fallbackName: { type: String, default: 'built-in' },
  error: { type: String, default: '' },
  disabled: { type: Boolean, default: false },
})
const emit = defineEmits(['update:modelValue'])

// Seconds are typed as seconds; past a minute the note says what they come to.
const said = (value) => (props.meta.unit === 's' && value >= 60 ? formatSpan(value) : formatLine(props.meta, value))
const note = computed(() => {
  const typed = fromInput(props.meta, props.modelValue)
  const base = `${props.fallbackName} ${said(props.fallback)}`
  if (typed === null || Number.isNaN(typed)) return base
  return props.meta.unit === 's' && typed >= 60 ? `${formatSpan(typed)} · ${base}` : base
})
</script>

<style scoped>
.line-row {
  display: grid; grid-template-columns: minmax(0, 1fr) auto;
  column-gap: 28px; row-gap: 4px; align-items: start;
  padding: 14px 0; border-top: 1px solid var(--bd-soft);
}
.line-row-label { grid-column: 1; font-size: 13px; font-weight: 500; color: var(--text-hi); }
.line-row-help { grid-column: 1; font-size: 12px; line-height: 1.5; color: var(--text-low); }
.line-row-invalid { color: var(--ember-400); }
.line-row-control {
  grid-column: 2; grid-row: 1 / span 2;
  display: flex; flex-direction: column; align-items: flex-end; gap: 6px;
}
.line-row-note { font-size: 11px; color: var(--text-low); white-space: nowrap; font-variant-numeric: tabular-nums; }
@media (max-width: 560px) {
  .line-row { grid-template-columns: minmax(0, 1fr); }
  .line-row-control { grid-column: 1; grid-row: auto; align-items: flex-start; }
}
</style>
