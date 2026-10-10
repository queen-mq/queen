<template>
  <!-- One line's value: a number and its unit in one box, the way the queue
       editor draws an option. Blank is a value here — "use the line this one
       sits on" — so the placeholder is that line, not a hint. -->
  <span class="line-input" :class="{ 'line-input-bad': invalid, 'line-input-off': disabled }">
    <input
      :value="modelValue"
      class="input tabular-nums"
      autocomplete="off"
      spellcheck="false"
      :inputmode="unit === 's' ? 'numeric' : 'decimal'"
      :placeholder="placeholder"
      :disabled="disabled"
      :aria-label="label"
      :aria-invalid="invalid ? 'true' : undefined"
      @input="emit('update:modelValue', $event.target.value)"
    />
    <span class="line-input-unit">{{ unit }}</span>
  </span>
</template>

<script setup>
defineProps({
  modelValue: { type: String, default: '' },
  /** 's' or '%' (composables/settingsDoc.js LINES). */
  unit: { type: String, required: true },
  placeholder: { type: String, default: '' },
  label: { type: String, default: '' },
  invalid: { type: Boolean, default: false },
  disabled: { type: Boolean, default: false },
})
const emit = defineEmits(['update:modelValue'])
</script>

<style scoped>
.line-input {
  display: inline-flex; align-items: center; width: 132px; height: 32px; padding-right: 10px;
  border: 1px solid var(--bd-hi); border-radius: var(--r-control); background: var(--ink-3);
}
.line-input:focus-within { box-shadow: 0 0 0 1px var(--ring); }
.line-input .input {
  width: 100%; min-width: 0; height: 30px; padding: 0 0 0 10px; text-align: right;
  border: 0; background: transparent; box-shadow: none;
}
.line-input-unit { margin-left: 6px; min-width: 10px; font-size: 12px; color: var(--text-low); }
.line-input-bad { border-color: var(--ember-400); }
.line-input-off { background: transparent; }
.line-input-off .input { color: var(--text-mid); }
</style>
