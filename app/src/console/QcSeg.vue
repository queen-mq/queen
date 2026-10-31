<script setup>
// A segmented switch: one of a few choices, each a button.
defineProps({
  modelValue: { type: [String, Number, Boolean], default: null },
  options: { type: Array, required: true }, // [{ value, label, count? }]
  label: { type: String, default: '' },
})
defineEmits(['update:modelValue'])
</script>

<template>
  <div class="qn-seg" role="group" :aria-label="label">
    <button
      v-for="o in options"
      :key="String(o.value)"
      type="button"
      :aria-pressed="modelValue === o.value"
      @click="$emit('update:modelValue', o.value)"
    >
      {{ o.label }}<span v-if="o.count != null" class="qn-num qn-t3" style="margin-left: 6px">{{ o.count }}</span>
    </button>
  </div>
</template>
