<script setup>
// The newest messages, 20 at a time, by status; a click opens one beside the
// list, in the same detail as Queues › Messages (console/queues/MessageDetail.vue):
// about half the page, kept in view while the list scrolls, its payload as
// tall as the window lets it be. Stacked under the list below 1000px.
import { computed, nextTick, ref, shallowRef, watch } from 'vue'
import { messages as messagesApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import QcSeg from '../QcSeg.vue'
import MessageDetail from '../queues/MessageDetail.vue'
import { fmt } from '../useQueuePage'

const props = defineProps({ queue: { type: String, required: true }, where: { type: String, default: '' } })
const emit = defineEmits(['copy'])

const STATUSES = [
  { value: '', label: 'All' },
  { value: 'pending', label: 'Waiting' },
  { value: 'processing', label: 'Leased' },
  { value: 'completed', label: 'Done' },
  { value: 'dead_letter', label: 'Dead letters' },
]
const WORD = { pending: 'waiting', processing: 'leased', completed: 'done', dead_letter: 'dead letter', failed: 'failed' }
const status = ref('')
const partition = ref('')
const applied = ref('')
const offset = ref(0)
const LIMIT = 20
const data = shallowRef(null)
const error = ref(null)
const loading = ref(false)
const picked = ref(null)

async function load() {
  loading.value = true
  const params = { queue: props.queue, limit: LIMIT, offset: offset.value }
  if (status.value) params.status = status.value
  if (applied.value) params.partition = applied.value
  try {
    const r = await messagesApi.list(params, { quiet: true })
    data.value = r.data
    error.value = null
  } catch (e) {
    error.value = describeApiError(e)
  } finally {
    loading.value = false
  }
}
useAutoRefresh(load)
load()
watch([status, applied], () => { offset.value = 0; picked.value = null; load() })
watch(offset, load)
watch(() => props.queue, () => { offset.value = 0; picked.value = null; load() })

const rows = computed(() => (data.value?.messages || []).map((m) => ({
  key: m.id || m.transactionId,
  id: m.transactionId,
  part: m.partition,
  at: m.createdAt ? new Date(m.createdAt).toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '–',
  status: WORD[m.status] || m.status || '–',
  read: m.busStatus && m.busStatus.totalGroups ? `${m.busStatus.consumedBy?.length ?? m.busStatus.consumedBy ?? 0} of ${m.busStatus.totalGroups}` : '',
  raw: m,
})))
// The message picked, as it was when picked: on a busy queue it leaves the
// newest 20 within seconds, and the open message must not close with it.
const pickedRaw = shallowRef(null)
watch(picked, (k) => { if (!k) pickedRaw.value = null })
const sel = computed(() => rows.value.find((r) => r.key === picked.value)?.raw || pickedRaw.value)
const pick = computed(() => (sel.value?.partitionId && sel.value?.transactionId ? { pid: sel.value.partitionId, txn: sel.value.transactionId } : null))
// Stacked (a phone, a narrow window), the detail opens under the list: bring it up.
const side = ref(null)
async function choose(key) {
  picked.value = picked.value === key ? null : key
  pickedRaw.value = picked.value ? rows.value.find((r) => r.key === key)?.raw || null : null
  if (picked.value && window.matchMedia?.('(max-width: 1000px)').matches) {
    await nextTick()
    side.value?.scrollIntoView?.({ block: 'start', behavior: 'smooth' })
  }
}
</script>

<template>
  <div class="qm" :class="{ open: !!pick }">
    <div class="qm-list">
      <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px">
        <QcSeg v-model="status" :options="STATUSES" label="Status" />
        <label class="qn-field" style="width: 240px; max-width: 100%">
          <span class="qn-vh">Partition</span>
          <input v-model="partition" class="qn-mono" type="text" placeholder="Partition, then Enter" spellcheck="false" @keydown.enter="applied = partition.trim()" @blur="applied = partition.trim()" />
        </label>
      </div>
      <p v-if="error" class="qn-t2" style="padding: 16px 0"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ error }}</p>
      <p v-else-if="data && !rows.length" class="qn-t2" style="padding: 16px 0">
        No {{ status ? STATUSES.find((s) => s.value === status).label.toLowerCase() : 'messages' }}{{ applied ? ` in ${applied}` : '' }}.
        <button v-if="status || applied" type="button" class="qn-link" @click="status = ''; partition = ''; applied = ''">Show all</button>
      </p>
      <div v-else class="qm-table" role="table" aria-label="Newest messages" style="--cols: minmax(0, 1.5fr) minmax(0, 1.1fr) 150px 96px">
        <div class="qn-tr head" role="row">
          <span role="columnheader">transactionId</span><span role="columnheader">Partition</span>
          <span role="columnheader">Pushed at</span><span role="columnheader">Status</span>
        </div>
        <div
          v-for="r in rows" :key="r.key" class="qn-tr click" :class="{ on: picked === r.key }" role="row"
          tabindex="0" @click="choose(r.key)" @keydown.enter="picked !== r.key && choose(r.key)"
        >
          <span role="cell" class="qn-mono" :title="r.id" style="font-weight: 500">{{ r.id }}</span>
          <span role="cell" class="qn-mono qn-t2" :title="r.part">{{ r.part }}</span>
          <span role="cell" class="qn-num">{{ r.at }}</span>
          <span role="cell" class="qn-t2">{{ r.status }}</span>
        </div>
      </div>
      <div v-if="rows.length" style="display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; margin-top: 12px">
        <span class="qn-num qn-t3">{{ fmt(offset + 1) }}–{{ fmt(offset + rows.length) }}, newest first</span>
        <div style="flex: 1 1 0" />
        <button type="button" class="qn-btn2" :disabled="offset === 0" @click="offset = Math.max(0, offset - LIMIT)">Newer</button>
        <button type="button" class="qn-btn2" :disabled="rows.length < LIMIT" @click="offset += LIMIT">Older</button>
      </div>
      <p class="qn-read" style="margin-top: 12px">Read from <span class="qn-mono">GET /api/v1/messages?queue={{ queue }}&amp;limit=20</span>.</p>
    </div>

    <div v-if="pick" ref="side" class="qm-detail">
      <MessageDetail :pick="pick" :row="sel" :where="where" fill @close="picked = null" @copy="emit('copy', $event)" @changed="load" />
    </div>
  </div>
</template>

<style scoped>
.qm { margin-top: 14px; }
.qm-list { min-width: 0; }
.qm-table { margin-top: 12px; overflow-x: auto; }
/* a message open: the list and the message share the page, the message a little more */
.qm.open { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.15fr); gap: 20px 28px; align-items: start; }
.qm-detail { min-width: 0; scroll-margin-top: 12px; }
@media (min-width: 1001px) {
  /* the message stays in view while the list scrolls */
  .qm-detail { position: sticky; top: 72px; }
}
@media (max-width: 1000px) {
  .qm.open { grid-template-columns: minmax(0, 1fr); }
}
</style>
