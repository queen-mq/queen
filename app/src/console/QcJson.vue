<script setup>
// A JSON value, pretty and easy to read: the data bright, the keys mid-grey,
// braces and commas dim. Greys only, like everything on the console that is
// not a status. Text that is not JSON, or longer than `max`, shows as is.
import { computed } from 'vue'
import { payloadToText } from '@/composables/usePushVerdict'

const props = defineProps({
  value: { type: null, default: undefined },  // any JSON value
  text: { type: String, default: null },      // or text, highlighted when it is JSON
  max: { type: Number, default: 100_000 },     // past this many characters, plain
})

// A string (a key when a colon follows), a literal, a number, or punctuation.
const TOKEN = /("(?:\\u[\da-fA-F]{4}|\\[^u]|[^\\"])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|([{}[\],])/g

const source = computed(() => {
  if (props.text == null) return payloadToText(props.value)
  // Text that parses is shown pretty; anything else as it came.
  try {
    const v = JSON.parse(props.text)
    return v !== null && typeof v === 'object' ? JSON.stringify(v, null, 2) : props.text
  } catch { return props.text }
})
const isJson = computed(() => {
  if (props.text == null) return props.value !== undefined
  try { JSON.parse(props.text); return true } catch { return false }
})
const parts = computed(() => {
  const s = source.value || ''
  if (!isJson.value || s.length > props.max) return [{ c: '', t: s }]
  const out = []
  let last = 0
  let m
  TOKEN.lastIndex = 0
  while ((m = TOKEN.exec(s))) {
    if (m.index > last) out.push({ c: '', t: s.slice(last, m.index) })
    if (m[1] !== undefined) {
      if (m[2]) { out.push({ c: 'k', t: m[1] }); out.push({ c: 'p', t: m[2] }) } else out.push({ c: 's', t: m[1] })
    } else if (m[3]) out.push({ c: 'l', t: m[3] })
    else if (m[4]) out.push({ c: 'n', t: m[4] })
    else if (m[5]) out.push({ c: 'p', t: m[5] })
    last = TOKEN.lastIndex
  }
  if (last < s.length) out.push({ c: '', t: s.slice(last) })
  return out
})
</script>

<template>
  <pre class="qn-pre qn-json"><span v-for="(p, i) in parts" :key="i" :class="p.c">{{ p.t }}</span></pre>
</template>

<style scoped>
.qn-json { color: var(--text3); }
.qn-json .k { color: var(--text2); }
.qn-json .s { color: var(--text); }
.qn-json .n, .qn-json .l { color: var(--text); font-weight: 500; }
.qn-json .p { color: var(--text3); }
</style>
