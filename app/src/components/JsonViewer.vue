<template>
  <div class="json-viewer">
    <VueJsonPretty
      :data="normalizedValue"
      :deep="4"
      :collapsed-node-length="20"
      :show-length="true"
      :show-line="true"
      :show-icon="true"
      theme="dark"
    />
  </div>
</template>

<script setup>
import { computed } from 'vue'
import VueJsonPretty from 'vue-json-pretty'
import 'vue-json-pretty/lib/styles.css'

const props = defineProps({
  value: { type: null, required: true },
})

// Message detail historically returned both decoded JSON values and JSON text.
// Parse only valid JSON strings so plain-text payloads remain visibly strings.
const normalizedValue = computed(() => {
  if (typeof props.value !== 'string') return props.value
  try {
    return JSON.parse(props.value)
  } catch {
    return props.value
  }
})
</script>

<style scoped>
.json-viewer {
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px; line-height: 1.6;
  padding: 14px 16px; border-radius: var(--r-card);
  border: 1px solid var(--bd);
  color: var(--text-mid); background: var(--recessed);
  overflow: auto; max-height: 400px;
}

.json-viewer :deep(.vjs-tree) { font-family: 'JetBrains Mono', monospace; font-size: 12px; }
.json-viewer :deep(.vjs-key) { color: var(--ice-400); }
.json-viewer :deep(.vjs-value-string) { color: var(--ok-500); }
.json-viewer :deep(.vjs-value-number),
.json-viewer :deep(.vjs-value-boolean) { color: var(--crown-400); }
.json-viewer :deep(.vjs-value-null),
.json-viewer :deep(.vjs-value-undefined) { color: var(--ember-400); }
.json-viewer :deep(.vjs-comment),
.json-viewer :deep(.vjs-tree-brackets) { color: var(--text-low); }
.json-viewer :deep(.vjs-tree-node.dark:hover) { background: var(--ink-4); }
.json-viewer :deep(.vjs-indent-unit.has-line) { border-left-color: var(--bd-hi); }
</style>
