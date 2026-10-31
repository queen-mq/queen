<script setup>
// One message of the Messages view, beside the list: its state and who read
// it, where it sits, its payload (a Kafka record decoded, an encrypted or
// expired payload said plainly), its traces, and what you can do with it:
// push a copy, copy it as curl, delete it when it is a dead letter.
import { computed, ref, shallowRef, watch } from 'vue'
import { messages as messagesApi, traces as tracesApi } from '@/api'
import client from '@/api/client'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { fieldLine, hexDump, HEX_LIMIT, kafkaRecordPlain, recordTime } from '@/composables/kafkaRecord'
import { payloadToText } from '@/composables/usePushVerdict'
import { currentEpoch, useIdentity } from '@/stores/identity'
import QcDialog from '../QcDialog.vue'
import QcJson from '../QcJson.vue'
import QcSeg from '../QcSeg.vue'
import { toast } from '../toasts'
import { clock, nowMs } from '../useWindow'
import { span } from '../useOverview'
import { fmt } from '../useQueuePage'
import {
  callText, copyOf, curlOf, deleteAnswer, deleteMessageRequest, groupStates, groupsDiffer, messageState, payloadOf, traceSummary,
} from './messagesView'

const props = defineProps({
  pick: { type: Object, required: true }, // { pid, txn }
  row: { type: Object, default: null },   // the list's row, shown while the detail is read
  where: { type: String, default: '' },
})
const emit = defineEmits(['close', 'copy', 'changed'])
const { can, standalone } = useIdentity()
const deleting = ref(false)
const busy = ref(false)

// ---- the read: GET /api/v1/messages/{pid}/{txn}, and its traces ----
const key = computed(() => `${props.pick.pid}\u0000${props.pick.txn}`)
const detail = shallowRef(null) // { key, data }
const failed = ref(null)        // { gone } | { text }
const traces = shallowRef(null) // { key, data }
let seq = 0
async function load(withTraces = false) {
  const p = props.pick
  const k = key.value
  const my = ++seq
  const at = currentEpoch()
  try {
    const r = await messagesApi.get(p.pid, p.txn, { quiet: true })
    if (my !== seq || at !== currentEpoch()) return
    detail.value = { key: k, data: r.data }
    failed.value = null
  } catch (e) {
    if (my !== seq || at !== currentEpoch()) return
    if (e?.status === 404) { detail.value = null; failed.value = { gone: true } } else failed.value = { text: describeApiError(e) }
  }
  if (!withTraces) return
  try {
    const t = await tracesApi.getForMessage(p.pid, p.txn, { limit: 100 }, { quiet: true })
    if (k === key.value && at === currentEpoch()) traces.value = { key: k, data: t.data }
  } catch { if (k === key.value) traces.value = null }
}
watch(key, () => { detail.value = null; failed.value = null; traces.value = null; deleting.value = false; load(true) }, { immediate: true })
useAutoRefresh(() => load(false))

const m = computed(() => (detail.value?.key === key.value ? detail.value.data : null) || props.row || null)
const read = computed(() => detail.value?.key === key.value)
const time = (ms) => clock(ms, true)
const groups = computed(() => (read.value ? groupStates(m.value, nowMs.value) : null))
const state = computed(() => messageState(m.value, groups.value, time))
const isDead = computed(() => m.value?.status === 'dead_letter')
// Every group by name when there is more than one; each with its own word only
// when they differ (the sentence above already says "all read it").
const shownGroups = computed(() => (groups.value && groups.value.length > 1 ? groups.value : null))
const ownWords = computed(() => groupsDiffer(groups.value))
const groupWords = (g) => (g.state === 'done' ? 'done' : g.state === 'leased' ? `leased until ${time(g.until)}` : 'not yet')
// The detail route carries no offset; the list's row does.
const offset = computed(() => m.value?.offset ?? props.row?.offset ?? null)

const DAY = new Intl.DateTimeFormat(undefined, { day: 'numeric', month: 'short', year: 'numeric' })
const pushedAt = computed(() => {
  const t = Date.parse(m.value?.createdAt)
  if (!Number.isFinite(t)) return null
  const ms = String(new Date(t).getMilliseconds()).padStart(3, '0')
  return { text: `${DAY.format(new Date(t))}, ${clock(t, true)}.${ms}`, ago: `${span((nowMs.value - t) / 1000)} ago`, iso: m.value.createdAt }
})
const ns = computed(() => (typeof m.value?.namespace === 'string' ? m.value.namespace : ''))
const qlink = computed(() => `/queues/${encodeURIComponent(m.value?.queue || '')}`)
const traced = computed(() => (traces.value?.key === key.value ? traceSummary(traces.value.data) : null))

// ---- the payload ----
const payload = computed(() => payloadOf(m.value))
const view = ref('decoded') // holds from one message to the next
const VIEWS = [{ value: 'decoded', label: 'Decoded' }, { value: 'stored', label: 'As stored' }]
const decoded = computed(() => payload.value.kind === 'kafka' && view.value === 'decoded')
const rec = computed(() => payload.value.record)
const bytes = (n) => `${fmt(n)} ${n === 1 ? 'byte' : 'bytes'}`
const valueMeta = computed(() => {
  const v = rec.value?.value
  if (!v) return ''
  if (v.kind === 'json') return `JSON, ${bytes(v.size)}`
  if (v.kind === 'text') return `text, ${bytes(v.size)}`
  if (v.kind === 'binary') return v.size > HEX_LIMIT ? `binary, ${bytes(v.size)}; the first ${bytes(HEX_LIMIT)} shown` : `binary, ${bytes(v.size)}`
  return 'null'
})
const lineOf = (f) => (f.kind === 'text' && f.text === '' ? 'empty' : fieldLine(f))
const recTime = computed(() => {
  const t = rec.value?.timestamp
  const iso = recordTime(t)
  return iso ? { text: `${DAY.format(new Date(t))}, ${clock(t, true)}`, iso } : null
})
async function copyText(text, what) {
  try { await navigator.clipboard.writeText(text); toast(what, '') } catch { toast('Could not copy', 'The browser refused the clipboard.', 'red') }
}
function copyPayload() {
  const p = payload.value
  copyText(decoded.value ? JSON.stringify(kafkaRecordPlain(p.record), null, 2) : payloadToText(p.value), decoded.value ? 'Copied the decoded record' : 'Copied the payload')
}

// ---- what you can do with it ----
const copy = computed(() => copyOf(m.value))
const canProduce = computed(() => can('produce'))
const canAdmin = computed(() => can('queueAdmin'))
function copyCurl() {
  const c = copy.value
  if (!c) return
  copyText(curlOf({ origin: location.origin, ...c, key: !standalone.value }), 'Copied as curl')
}
const note = computed(() => {
  const out = []
  if (payload.value.kind === 'sealed' || payload.value.kind === 'gone') out.push('No copy: the payload cannot be read.')
  else if (!canProduce.value) out.push('Pushing a copy needs a producer role on this cluster.')
  if (isDead.value && !canAdmin.value) out.push('Deleting a dead letter needs the admin role on this cluster.')
  else if (!isDead.value && m.value) out.push('Only a dead letter can be deleted: a live message stays in the log until its retention ends.')
  return out.join(' ')
})

const req = computed(() => (m.value ? deleteMessageRequest(m.value.partitionId || props.pick.pid, m.value.transactionId || props.pick.txn) : null))
async function remove() {
  if (!req.value) return
  busy.value = true
  const txn = props.pick.txn
  try {
    const r = await client.delete(req.value.path, { quiet: true })
    const a = deleteAnswer(r.data)
    if (!a.ok) { toast('Not deleted', a.why, 'red'); return }
    toast(`Dead letter deleted on ${props.where}`, `The broker answered ${r.status || 200}: ${a.n === 1 ? 'its dead-letter row is' : `its ${fmt(a.n)} dead-letter rows are`} gone. The message itself stays in the log until its retention ends.`)
    deleting.value = false
    emit('changed')
    load(false)
  } catch (e) {
    toast('Not deleted', e?.status === 404 ? `No dead-letter row holds ${txn} now: it was replayed or deleted already.` : describeApiError(e), 'red')
  } finally { busy.value = false }
}
</script>

<template>
  <aside class="md qn-panel" aria-label="Message details">
    <div class="md-head">
      <h3 class="qn-mono md-id">{{ pick.txn }}</h3>
      <button type="button" class="qn-btn3 md-x" aria-label="Close the details" @click="emit('close')">✕</button>
    </div>

    <p v-if="failed && failed.gone" class="md-lead">This message is no longer stored: retention removed it, or the address is wrong.</p>
    <p v-else-if="!m && failed" class="md-lead qn-t2"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ failed.text }}</p>
    <p v-else-if="!m" class="md-lead qn-t2">Reading the message…</p>

    <template v-else-if="!(failed && failed.gone)">
      <p class="md-lead"><span v-for="(p, i) in state.parts" :key="i" :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></p>
      <p v-if="state.error" class="qn-mono md-error">{{ state.error }}</p>
      <p v-if="failed" class="qn-t2 md-small"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The last read failed ({{ failed.text }}); this is what it said before.</p>

      <dl class="qn-dl md-facts">
        <dt>Queue</dt>
        <dd><RouterLink :to="qlink" class="qn-mono qn-link">{{ m.queue }}</RouterLink><span v-if="ns" class="qn-t3"> in <span class="qn-mono">{{ ns }}</span></span></dd>
        <dt>Partition</dt>
        <dd class="qn-mono" :title="`partitionId ${m.partitionId || pick.pid}`">{{ m.partition }}</dd>
        <template v-if="pushedAt">
          <dt>Pushed at</dt>
          <dd class="qn-num" :title="pushedAt.iso">{{ pushedAt.text }} <span class="qn-t3">· {{ pushedAt.ago }}</span></dd>
        </template>
        <template v-if="offset != null">
          <dt>Offset</dt>
          <dd class="qn-num">{{ fmt(offset) }}</dd>
        </template>
        <template v-if="shownGroups">
          <dt>Groups</dt>
          <dd>
            <span v-for="g in shownGroups" :key="g.name" class="md-group">
              <span class="qn-mono">{{ g.label }}</span><span v-if="ownWords" class="md-gword" :class="g.state === 'done' ? 'qn-t3' : 'qn-t2'">{{ groupWords(g) }}</span>
            </span>
          </dd>
        </template>
        <template v-if="m.traceId">
          <dt>traceId</dt>
          <dd class="qn-mono">{{ m.traceId }}</dd>
        </template>
        <template v-if="m.producerSub">
          <dt>Pushed by</dt>
          <dd class="qn-mono">{{ m.producerSub }}</dd>
        </template>
        <template v-if="traced && traced.total">
          <dt>Traces</dt>
          <dd>
            <span class="qn-num">{{ fmt(traced.total) }} {{ traced.total === 1 ? 'event' : 'events' }}</span><template v-if="traced.names.length">, as
              <template v-for="(t, i) in traced.names.slice(0, 4)" :key="t.name"><RouterLink :to="{ path: '/entities', query: { name: t.name } }" class="qn-mono qn-link">{{ t.name }}</RouterLink>{{ i < Math.min(4, traced.names.length) - 1 ? ', ' : '' }}</template><template v-if="traced.names.length > 4"> and {{ traced.names.length - 4 }} more</template></template>
          </dd>
        </template>
      </dl>

      <div class="md-payhead">
        <h4 class="qn-h2">Payload</h4>
        <QcSeg v-if="payload.kind === 'kafka'" v-model="view" :options="VIEWS" label="How to show the Kafka record" class="md-seg" />
        <div style="flex: 1 1 0" />
        <button v-if="payload.kind === 'json' || payload.kind === 'kafka'" type="button" class="qn-btn3 md-copy" @click="copyPayload">Copy</button>
      </div>
      <p v-if="payload.kind === 'gone'" class="qn-t2 md-plain">The payload is no longer stored: retention removed it.</p>
      <p v-else-if="payload.kind === 'sealed'" class="qn-t2 md-plain">Stored encrypted, and the broker that answered has no key for it: the console cannot show it.</p>
      <template v-else-if="decoded">
        <p class="qn-t3 md-small">A Kafka record. The facade stores its key, value and headers in base64; here they are decoded.</p>
        <dl class="qn-dl md-rec">
          <dt>Key</dt>
          <dd class="qn-mono" :class="{ 'qn-t3': rec.key.kind === 'null' }">{{ rec.key.kind === 'null' ? 'null, no key' : lineOf(rec.key) }}</dd>
          <dt>Time</dt>
          <dd v-if="recTime" class="qn-num" :title="`${recTime.iso} · ${rec.timestamp} ms`">{{ recTime.text }}</dd>
          <dd v-else class="qn-t3">Not set: the time it was pushed is the record's.</dd>
          <dt>Headers</dt>
          <dd v-if="!rec.headers.length" class="qn-t3">None</dd>
          <dd v-else>
            <span v-for="(h, i) in rec.headers" :key="i" class="md-header"><span class="qn-mono qn-t2">{{ h.name }}</span> <span class="qn-mono" :class="{ 'qn-t3': h.value.kind === 'null' }">{{ lineOf(h.value) }}</span></span>
          </dd>
          <dt>Value</dt>
          <dd class="qn-t2">{{ valueMeta }}</dd>
        </dl>
        <QcJson v-if="rec.value.kind === 'json'" :value="rec.value.json" class="md-body" />
        <pre v-else-if="rec.value.kind === 'binary'" class="qn-pre md-body md-hex">{{ hexDump(rec.value.bytes) }}</pre>
        <pre v-else-if="rec.value.kind === 'text' && rec.value.text" class="qn-pre md-body">{{ rec.value.text }}</pre>
        <p v-else-if="rec.value.kind === 'text'" class="qn-t3 md-plain">Empty.</p>
        <p v-else class="qn-t2 md-plain">null: a tombstone. On a compacted topic it deletes its key.</p>
      </template>
      <template v-else>
        <p v-if="payload.encrypted" class="qn-t3 md-small">Stored encrypted. The broker that answered decrypted it for this read.</p>
        <QcJson :value="payload.value" class="md-body" />
      </template>

      <div class="md-acts">
        <button v-if="canProduce" type="button" class="qn-btn2" :disabled="!copy" @click="emit('copy', copy)">Push a copy…</button>
        <button type="button" class="qn-btn3" :disabled="!copy" @click="copyCurl">Copy as curl</button>
        <div style="flex: 1 1 0" />
        <button v-if="isDead && canAdmin" type="button" class="qn-btn2 danger" aria-haspopup="dialog" @click="deleting = true">Delete…</button>
      </div>
      <p v-if="isDead && canAdmin" class="qn-t3 md-small">Delete removes its dead-letter row. To run it again instead, <RouterLink :to="{ path: '/dlq', query: { queue: m.queue } }" class="qn-link">replay it from Dead letters</RouterLink>.</p>
      <p v-if="note" class="qn-t3 md-small">{{ note }}</p>
    </template>

    <p class="qn-read md-read">Read from <span class="qn-mono">GET /api/v1/messages/{{ encodeURIComponent(pick.pid) }}/{{ encodeURIComponent(pick.txn) }}</span><template v-if="traced && traced.total"> and its traces</template>.</p>

    <QcDialog
      :open="deleting && !!req" title="Delete this dead letter?" :where="where" :call="req ? callText(req) : ''" :typed="pick.txn"
      confirm-label="Delete" danger :busy="busy" @confirm="remove" @cancel="deleting = false"
    >
      <p class="qn-t2">Its dead-letter row goes, for every group that dead-lettered it in <span class="qn-mono" style="color: var(--text)">{{ m?.queue }}</span>. Nothing goes back into the queue, and it cannot be undone.</p>
    </QcDialog>
  </aside>
</template>

<style scoped>
.md { min-width: 0; padding: 16px 18px 18px; }
.md-head { display: flex; align-items: flex-start; gap: 10px; }
.md-id { flex: 1 1 0; min-width: 0; font-size: 13.5px; font-weight: 500; line-height: 1.45; overflow-wrap: anywhere; }
.md-x { flex: 0 0 auto; height: 28px; margin: -3px -6px 0 0; }
.md-lead { font-size: 14px; margin-top: 10px; overflow-wrap: anywhere; }
.md-error { font-size: 12.5px; line-height: 1.5; margin-top: 8px; padding: 10px 12px; background: var(--bg2); border-radius: var(--r-ctl); overflow-wrap: anywhere; }
.md-small { font-size: 12px; margin-top: 8px; }
.md-facts { margin-top: 14px; grid-template-columns: 92px minmax(0, 1fr); }
.md-group { display: block; }
.md-gword { margin-left: 8px; }
.md-header { display: block; overflow-wrap: anywhere; }
.md-payhead { display: flex; align-items: center; gap: 10px; margin-top: 18px; min-height: 30px; }
.md-seg :deep(button) { height: 24px; font-size: 12px; }
.md-copy { height: 28px; }
.md-rec { margin-top: 8px; grid-template-columns: 92px minmax(0, 1fr); }
.md-body { margin-top: 8px; }
.md-hex { white-space: pre; overflow-wrap: normal; overflow-x: auto; }
.md-plain { font-size: 13px; margin-top: 8px; }
.md-acts { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 16px; }
.md-read { margin-top: 14px; overflow-wrap: anywhere; }
</style>
