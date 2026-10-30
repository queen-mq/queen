<template>
  <!-- A message's state as a mark and a word, in greys: a pending message is
       work waiting, not an alarm, and a dead letter is parked history (the
       Dead letter tab is where it is worked on). The shape tells them apart. -->
  <span class="message-status" :class="`message-status-${appearance.tone}`">
    <span class="message-status-mark" aria-hidden="true" />
    {{ appearance.label }}
  </span>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({ status: { type: String, default: '' } })
const states = {
  pending: { label: 'Pending', tone: 'pending' },
  processing: { label: 'Processing', tone: 'processing' },
  completed: { label: 'Completed', tone: 'completed' },
  dead_letter: { label: 'Dead letter', tone: 'failed' },
  failed: { label: 'Failed', tone: 'failed' },
}
const appearance = computed(() => Object.hasOwn(states, props.status)
  ? states[props.status]
  : { label: props.status || 'Unknown', tone: 'unknown' })
</script>

<style scoped>
.message-status { display: inline-flex; align-items: center; gap: 7px; font-size: 12px; color: var(--text-mid); white-space: nowrap; }
.message-status-mark { flex: none; width: 7px; height: 7px; border-radius: 50%; background: var(--text-faint); }
/* Waiting: a ring. In flight: a full dot. Done: a quiet dot. Parked: a square. */
.message-status-pending { color: var(--text-hi); }
.message-status-pending .message-status-mark { background: transparent; border: 1.5px solid var(--text-mid); }
.message-status-processing { color: var(--text-hi); }
.message-status-processing .message-status-mark { background: var(--text-hi); }
.message-status-completed { color: var(--text-low); }
.message-status-failed { color: var(--text-mid); }
.message-status-failed .message-status-mark { border-radius: 1.5px; background: var(--text-low); }
.message-status-unknown { color: var(--text-low); }
.message-status-unknown .message-status-mark { background: transparent; border: 1.5px solid var(--text-faint); }
</style>
