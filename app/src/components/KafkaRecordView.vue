<template>
  <div class="kafka-record">
    <p class="kr-note">
      Written through the Kafka facade, which stores the key, value and headers base64-encoded.
      Shown decoded; Raw is the stored form.
    </p>

    <div class="kr-grid">
      <label class="label-xs">Key</label>
      <span class="kr-line font-mono" :class="{ 'kr-muted': record.key.kind === 'null' }">
        {{ record.key.kind === 'null' ? 'null — no key' : lineOf(record.key) }}
      </span>

      <label class="label-xs">Time</label>
      <span class="kr-line" :title="timeTitle">{{ timeLine }}</span>

      <label class="label-xs">Headers</label>
      <div v-if="record.headers.length" class="kr-headers">
        <template v-for="(header, i) in record.headers" :key="i">
          <span class="font-mono kr-header-name">{{ header.name }}</span>
          <span class="font-mono kr-line" :class="{ 'kr-muted': header.value.kind === 'null' }">
            {{ lineOf(header.value) }}
          </span>
        </template>
      </div>
      <span v-else class="kr-line kr-muted">none</span>
    </div>

    <div class="kr-value-head">
      <label class="label-xs">Value</label>
      <span class="kr-meta">{{ valueMeta }}</span>
    </div>
    <JsonViewer v-if="record.value.kind === 'json'" :value="record.value.json" />
    <pre v-else-if="record.value.kind === 'binary'" class="kr-box kr-hex">{{ hexDump(record.value.bytes) }}</pre>
    <pre v-else-if="record.value.kind === 'text' && record.value.text" class="kr-box">{{ record.value.text }}</pre>
    <div v-else-if="record.value.kind === 'text'" class="kr-box kr-muted">empty</div>
    <div v-else class="kr-box kr-muted">null — a tombstone: on a compacted topic it deletes its key</div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import JsonViewer from '@/components/JsonViewer.vue'
import { fieldLine, hexDump, HEX_LIMIT } from '@/composables/kafkaRecord'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'

const props = defineProps({
  // A record from `kafkaRecordOf`.
  record: { type: Object, required: true },
})

const bytes = n => `${n.toLocaleString('en-US')} ${n === 1 ? 'byte' : 'bytes'}`

// Keys and header values are short text in practice; an empty one is shown as
// such rather than as nothing.
const lineOf = field => (field.kind === 'text' && field.text === '' ? 'empty' : fieldLine(field))

const timeLine = computed(() => {
  const t = props.record.timestamp
  return t === null || t < 0 ? 'not set — the message’s Created time is the record’s' : formatTimestamp(t)
})
const timeTitle = computed(() => {
  const t = props.record.timestamp
  return t === null || t < 0 ? '' : `${formatTimestampUtc(t)} · ${t} ms`
})

const valueMeta = computed(() => {
  const value = props.record.value
  switch (value.kind) {
    case 'json': return `JSON · ${bytes(value.size)}`
    case 'text': return `text · ${bytes(value.size)}`
    case 'binary':
      return value.size > HEX_LIMIT
        ? `binary, not UTF-8 · ${bytes(value.size)} · first ${bytes(HEX_LIMIT)} shown`
        : `binary, not UTF-8 · ${bytes(value.size)}`
    default: return 'null'
  }
})
</script>

<style scoped>
.kafka-record { display: flex; flex-direction: column; gap: 12px; }
.kr-note { font-size: 11px; color: var(--text-low); }

.kr-grid {
  display: grid; grid-template-columns: 64px minmax(0, 1fr);
  gap: 8px 12px; align-items: baseline;
}
.kr-headers {
  display: grid; grid-template-columns: max-content minmax(0, 1fr);
  gap: 4px 12px; align-items: baseline;
}
.kr-line { font-size: 12px; color: var(--text-hi); word-break: break-all; }
.kr-header-name { font-size: 12px; color: var(--ice-400); }
.kr-muted { color: var(--text-low); }

.kr-value-head { display: flex; align-items: baseline; gap: 10px; margin-top: 4px; }
.kr-meta { font-size: 11px; color: var(--text-low); }

.kr-box {
  margin: 0;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px; line-height: 1.6;
  padding: 14px 16px; border-radius: var(--r-card);
  border: 1px solid var(--bd);
  color: var(--text-mid); background: var(--recessed);
  overflow: auto; max-height: 400px;
  white-space: pre-wrap; word-break: break-all;
}
.kr-box.kr-muted { color: var(--text-low); }
/* A dump is columns: it scrolls rather than wraps. */
.kr-box.kr-hex { white-space: pre; word-break: normal; }
</style>
