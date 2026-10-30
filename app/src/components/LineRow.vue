<template>
  <!-- A line you may move, on one row as the pilot's lines read (QueueAlerts):
       what it is on the left, then the line it falls back to while the field
       is blank, then the field. What the line means is its tooltip; a value
       that cannot be saved says why under the name. -->
  <label class="line-row" :title="meta.help">
    <span class="line-row-text">
      <span class="line-row-label">{{ meta.label }}</span>
      <span v-if="error" class="line-row-invalid">{{ error }}</span>
    </span>
    <span class="line-row-note">{{ note }}</span>
    <LineInput
      class="line-row-input"
      :model-value="modelValue"
      :unit="meta.unit"
      :placeholder="toInput(meta, fallback)"
      :label="meta.label"
      :invalid="!!error"
      :disabled="disabled"
      @update:model-value="emit('update:modelValue', $event)"
    />
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
  display: grid; grid-template-columns: minmax(0, 1fr) auto auto;
  column-gap: 16px; align-items: center;
  padding: 8px 0; border-top: 1px solid var(--bd-soft);
}
.line-row-text { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
.line-row-label { font-size: 13px; color: var(--text-hi); }
.line-row-invalid { font-size: 12px; line-height: 1.45; color: var(--ember-400); }
.line-row-note { font-size: 12px; color: var(--text-low); white-space: nowrap; font-variant-numeric: tabular-nums; text-align: right; }
/* Phone: the name on its line, the note and the field under it. */
@media (max-width: 560px) {
  .line-row { grid-template-columns: minmax(0, 1fr) auto; row-gap: 4px; }
  .line-row-text { grid-column: 1 / -1; }
  .line-row-note { text-align: left; }
}
</style>
