<template>
  <!-- A queue's verdict as a mark and a word: the word carries the colour only
       when a line says so (warn, bad); healthy and idle stay grey. -->
  <span class="queue-status" :class="`queue-status-${tone}`">
    <span class="g" :class="glyph" aria-hidden="true" />
    <span>{{ label }}</span>
  </span>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  label: { type: String, required: true },
  severity: { type: String, default: 'unknown' },
})

const tones = { ok: 'healthy', ice: 'idle', warn: 'warning', bad: 'critical' }
const tone = computed(() => Object.hasOwn(tones, props.severity) ? tones[props.severity] : 'unknown')
const glyph = computed(() => ({ healthy: 'ok', warning: 'warn', critical: 'bad' }[tone.value] || 'idle'))
</script>

<style scoped>
.queue-status {
  display: inline-flex; align-items: center; gap: 8px; min-width: 0;
  font-size: 12px; color: var(--text-low); white-space: nowrap;
}
.queue-status-healthy { color: var(--text-mid); }
.queue-status-warning { color: var(--warn-400); }
.queue-status-critical { color: var(--ember-400); }
</style>
