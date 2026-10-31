<script setup>
// Push one message: partition, transactionId and payload, then the call, then
// the broker's answer for the item (queued, duplicate, or refused).
import { computed, ref, watch } from 'vue'
import { messages as messagesApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { parsePayload, payloadToText, pushVerdict, describePushRefusal } from '@/composables/usePushVerdict'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'

const props = defineProps({
  open: { type: Boolean, default: false },
  queue: { type: String, required: true },
  where: { type: String, default: '' },
  prefill: { type: Object, default: null }, // { partition, payload }
})
const emit = defineEmits(['close', 'pushed'])

const partition = ref('Default')
const txn = ref('')
const text = ref('{\n  \n}')
watch(() => props.open, (o) => {
  if (!o) return
  partition.value = props.prefill?.partition || 'Default'
  txn.value = ''
  text.value = props.prefill ? payloadToText(props.prefill.payload) : '{\n  \n}'
})

const parsed = computed(() => parsePayload(text.value))
const item = computed(() => {
  const it = { queue: props.queue, partition: partition.value.trim() || 'Default', payload: parsed.value.ok ? parsed.value.value : null }
  if (txn.value.trim()) it.transactionId = txn.value.trim()
  return it
})
const call = computed(() => {
  const s = JSON.stringify({ items: [item.value] })
  return `POST /api/v1/push ${s.length > 220 ? s.slice(0, 219) + '…' : s}`
})
const busy = ref(false)
async function push() {
  if (!parsed.value.ok) return
  busy.value = true
  try {
    const r = await messagesApi.push({ items: [item.value] })
    const v = pushVerdict(Array.isArray(r.data) ? r.data : r.data?.results || r.data?.items || [])
    toast(v.title, v.detail, v.kind === 'error' ? 'red' : '')
    if (v.kind !== 'error') { emit('pushed'); emit('close') }
  } catch (e) {
    toast('Not pushed', describePushRefusal?.(e) || describeApiError(e), 'red')
  } finally { busy.value = false }
}
</script>

<template>
  <QcDialog :open="open" :title="`Push a message to ${queue}?`" :where="where" :call="call" confirm-label="Push" :busy="busy" :blocked="!parsed.ok" @confirm="push" @cancel="emit('close')">
    <div style="display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px">
      <label>
        <span class="qn-t3" style="font-size: 12px">Partition</span>
        <span class="qn-field" style="margin-top: 4px"><input v-model="partition" class="qn-mono" type="text" spellcheck="false" /></span>
      </label>
      <label>
        <span class="qn-t3" style="font-size: 12px">transactionId</span>
        <span class="qn-field" style="margin-top: 4px"><input v-model="txn" class="qn-mono" type="text" placeholder="the broker makes one" spellcheck="false" /></span>
      </label>
    </div>
    <label style="display: block; margin-top: 12px">
      <span class="qn-t3" style="font-size: 12px">Payload, any JSON</span>
      <textarea v-model="text" class="qn-textarea" style="margin-top: 4px" spellcheck="false" />
    </label>
    <p v-if="!parsed.ok" class="red" style="font-size: 12px; margin-top: 6px">{{ parsed.message }}</p>
    <p v-else class="qn-t3" style="font-size: 12px; margin-top: 6px">The same transactionId in the same partition within the dedup window is answered duplicate and stored once.</p>
  </QcDialog>
</template>
