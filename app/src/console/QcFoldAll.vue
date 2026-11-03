<script setup>
// "Expand all" / "Collapse all" for a list folded by namespace: one click
// opens, or closes, every namespace the list shows. Like a click on one
// namespace, it lasts: a refresh never opens or closes anything.
defineProps({
  open: { type: Boolean, default: false }, // every namespace shown is open
  noun: { type: String, default: 'namespace' },
})
const emit = defineEmits(['set'])
</script>

<template>
  <button type="button" class="qn-btn3 qn-foldall" :aria-label="open ? `Collapse every ${noun}` : `Expand every ${noun}`" @click="emit('set', !open)">
    <svg width="14" height="14" viewBox="0 0 18 18" aria-hidden="true">
      <path v-if="open" d="M5.5 2.5 9 6l3.5-3.5M5.5 15.5 9 12l3.5 3.5" />
      <path v-else d="M5.5 6 9 2.5 12.5 6M5.5 12 9 15.5l3.5-3.5" />
    </svg>
    <span>{{ open ? 'Collapse all' : 'Expand all' }}</span>
  </button>
</template>

<style scoped>
.qn-foldall { height: 28px; padding: 0 8px; gap: 6px; font-size: 12.5px; }
.qn-foldall svg { fill: none; stroke: currentColor; stroke-width: 1.4; stroke-linecap: round; stroke-linejoin: round; }
</style>
