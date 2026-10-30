<template>
  <!--
    The app's checkbox: a 16px box drawn in the app's inks, its check in the
    text colour (never yellow: a ticked box is not an alert), the real input
    kept for the keyboard and screen readers.

      <UiCheck v-model="laggingOnly">Lagging only</UiCheck>
  -->
  <label class="uic" :class="{ on: modelValue, disabled }">
    <input
      type="checkbox"
      class="uic-input"
      :checked="modelValue"
      :disabled="disabled"
      @change="emit('update:modelValue', $event.target.checked)"
    />
    <span class="uic-box" aria-hidden="true"><Icon v-if="modelValue" name="check" :size="12" :stroke="2.4" /></span>
    <span v-if="$slots.default" class="uic-text"><slot /></span>
  </label>
</template>

<script setup>
import Icon from '@/components/Icon.vue'

defineProps({
  modelValue: { type: Boolean, default: false },
  disabled: { type: Boolean, default: false },
})
const emit = defineEmits(['update:modelValue'])
</script>

<style>
.uic { display: inline-flex; align-items: center; gap: 8px; cursor: pointer; font-size: 12.5px; color: var(--text-mid); user-select: none; }
.uic.on { color: var(--text-hi); }
.uic.disabled { opacity: .55; cursor: not-allowed; }
.uic-input { position: absolute; opacity: 0; width: 1px; height: 1px; pointer-events: none; }
.uic-box {
  flex: none; width: 16px; height: 16px; display: inline-flex; align-items: center; justify-content: center;
  border: 1px solid var(--bd-hi); border-radius: 4px; background: var(--ink-3); color: var(--ink-0);
  transition: background .12s, border-color .12s;
}
.uic:hover .uic-box { border-color: var(--text-faint); }
.uic.on .uic-box { background: var(--text-hi); border-color: var(--text-hi); }
.uic-input:focus-visible + .uic-box { outline: 2px solid var(--accent); outline-offset: 2px; }
</style>
