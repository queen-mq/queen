<script setup>
// Put a key: its namespace, its name, a JSON value, an expiry, and "only if
// absent" (expect: 0, so an existing key is left alone). The call shows
// before anything runs; the broker's answer comes back as a toast.
import { computed, ref, watch } from 'vue'
import QcDialog from '../QcDialog.vue'
import QcSeg from '../QcSeg.vue'
import { toast } from '../toasts'
import { refusal } from '../useStatePage'
import { EXPIRIES, putPlan, writeAnswer } from './stateViews'

const props = defineProps({
  open: { type: Boolean, default: false },
  s: { type: Object, required: true },
  where: { type: String, default: '' },
})
const emit = defineEmits(['close', 'put'])
const { ns, nsData, putKey, afterKvWrite } = props.s

const putNs = ref('')
const key = ref('')
const text = ref('')
const expiry = ref('never')
const absent = ref(true)
const keyBox = ref(null)
const busy = ref(false)
watch(() => props.open, (o) => {
  if (!o) return
  putNs.value = ns.value || ''
  key.value = ''
  text.value = ''
  expiry.value = 'never'
  absent.value = true
  // The dialog focuses its first field (the namespace); the key is the one to type.
  setTimeout(() => keyBox.value?.focus?.(), 40)
})

const plan = computed(() => putPlan({ ns: putNs.value, key: key.value, text: text.value, expiry: expiry.value, absent: absent.value }))
const options = computed(() => (nsData.value || []).map((o) => o.namespace))

async function put() {
  const p = plan.value
  if (!p.ok) return
  const n = putNs.value.trim()
  const k = key.value
  busy.value = true
  try {
    const r = await putKey(n, k, p.body)
    const a = writeAnswer('put', r.data, { key: k, ns: n, status: r.status || 200 })
    toast(r.data?.applied === true ? `Put ${k} in ${n}` : a.text, a.sub, a.tone)
    await afterKvWrite()
    emit('put', k, n)
  } catch (e) {
    toast('Not put', refusal(e), 'red')
  } finally { busy.value = false }
}
</script>

<template>
  <QcDialog :open="open" title="Put a key" :where="where" :call="plan.call" confirm-label="Put" :busy="busy" :blocked="!plan.ok" @confirm="put" @cancel="emit('close')">
    <div class="stp-names">
      <label>
        <span class="qn-t3" style="font-size: 12px">Namespace</span>
        <span class="qn-field" style="margin-top: 4px"><input v-model="putNs" class="qn-mono" type="text" list="stp-ns" spellcheck="false" autocomplete="off" /></span>
        <datalist id="stp-ns"><option v-for="o in options" :key="o" :value="o" /></datalist>
      </label>
      <label>
        <span class="qn-t3" style="font-size: 12px">Key</span>
        <span class="qn-field" style="margin-top: 4px"><input ref="keyBox" v-model="key" class="qn-mono" type="text" placeholder="order/9f1" spellcheck="false" autocomplete="off" /></span>
      </label>
    </div>
    <label style="display: block; margin-top: 12px">
      <span class="qn-t3" style="font-size: 12px">Value, as JSON</span>
      <textarea v-model="text" class="qn-textarea" style="margin-top: 4px; min-height: 120px" spellcheck="false" placeholder="{ &quot;plan&quot;: &quot;pro&quot; }" />
    </label>
    <p v-for="m in plan.problems" :key="m" class="red" style="font-size: 12px; margin-top: 6px">{{ m }}</p>
    <p v-if="!plan.problems.length" class="qn-t3" style="font-size: 12px; margin-top: 6px">Text goes in quotes: <span class="qn-mono">"hello"</span>.</p>
    <p class="qn-t3" style="font-size: 12px; margin: 14px 0 6px">Expires</p>
    <QcSeg v-model="expiry" :options="EXPIRIES" label="Expires" />
    <label class="stp-check"><input v-model="absent" type="checkbox" /><span>Only if absent</span></label>
    <p class="qn-t2" style="margin-top: 12px">{{ plan.effect }}</p>
  </QcDialog>
</template>

<style scoped>
.stp-names { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.4fr); gap: 10px; }
.stp-check { display: flex; align-items: center; gap: 8px; margin-top: 14px; }
.stp-check input { accent-color: var(--text2); }
@media (max-width: 480px) { .stp-names { grid-template-columns: minmax(0, 1fr); } }
</style>
