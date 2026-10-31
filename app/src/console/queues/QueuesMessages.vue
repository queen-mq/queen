<script setup>
// The newest messages of every queue (or of one queue, a namespace or a task)
// in the band's window, 20 a page; a click opens one beside the list. The list
// holds still while you read it: a refresh updates each row's status in place
// and counts what arrived since, until you ask for it. Its address fields:
// ?mstatus= ?mq= ?mns= ?mtask= ?mpart=, and the pick ?mpid= ?mtx=.
import { computed, nextTick, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { messages as messagesApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { filtersForPushedMessage } from '@/composables/usePushVerdict'
import { currentEpoch, useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import QcSeg from '../QcSeg.vue'
import QueuePush from '../queue/QueuePush.vue'
import { clock, useWindow } from '../useWindow'
import { fmt } from '../useQueuePage'
import MessagesWhere from './MessagesWhere.vue'
import MessageDetail from './MessageDetail.vue'
import {
  PAGE, STATUSES, emptyParts, listCall, listParams, mergeRefresh, messageRows, older, readFilters, readPick,
  relaxOf, windowWords, withFilters,
} from './messagesView'

defineProps({ where: { type: String, default: '' } })
const route = useRoute()
const router = useRouter()
const { can } = useIdentity()
const qs = useQueuesStore()
const { win } = useWindow()

// ---- the address: the filters and the pick ----
const filters = computed(() => readFilters(route.query))
const pick = computed(() => readPick(route.query))
function setFilters(patch, keepPick = false) {
  const next = withFilters(route.query, patch)
  if (keepPick && pick.value) Object.assign(next, { mpid: pick.value.pid, mtx: pick.value.txn })
  router.replace({ query: next })
}
const status = computed({ get: () => filters.value.status, set: (v) => setFilters({ status: v }) })
const partText = ref(filters.value.partition)
watch(() => filters.value.partition, (v) => { partText.value = v })
function applyPartition() {
  const v = partText.value.trim()
  if (v !== filters.value.partition) setFilters({ partition: v })
}
const relax = computed(() => relaxOf(filters.value))

// ---- the read: one page of GET /api/v1/messages ----
const winKey = computed(() => (win.value.custom ? `${win.value.from}-${win.value.to}` : win.value.id))
const offset = ref(0)
const trail = [] // where the newer pages started, for Newer
const rows = shallowRef([])
const pagination = shallowRef(null)
const loaded = ref(false)
const pending = ref(false)
const error = ref(null)
const stale = ref(null) // a refresh failed over the rows on screen: { error, at }
const newer = ref(0)
const readAt = ref(0)
const params = computed(() => listParams(filters.value, win.value.iso, offset.value))
const call = computed(() => listCall(params.value))
let seq = 0
let epoch = currentEpoch()

async function load(merge = false) {
  const my = ++seq
  const at = currentEpoch()
  if (!merge) pending.value = true
  try {
    const r = await messagesApi.list(params.value, { quiet: true })
    if (my !== seq || at !== currentEpoch()) return
    const fresh = messageRows(r.data?.messages)
    if (merge) {
      const m = mergeRefresh(rows.value, fresh)
      rows.value = m.rows
      newer.value = m.newer
    } else {
      rows.value = fresh
      newer.value = 0
    }
    pagination.value = r.data?.pagination || null
    error.value = null
    stale.value = null
    readAt.value = Date.now()
  } catch (e) {
    if (my !== seq || at !== currentEpoch()) return
    const text = describeApiError(e)
    if (merge && rows.value.length) stale.value = { error: text, at: readAt.value }
    else { error.value = text; rows.value = [] }
  } finally {
    if (my === seq) { loaded.value = true; pending.value = false }
  }
}
function first() { offset.value = 0; trail.length = 0; load() }
watch([() => JSON.stringify(filters.value), winKey], first)
load()
useAutoRefresh(() => {
  // A page you asked for is on its way: this tick would merge into the rows it replaces.
  if (epoch === currentEpoch()) return pending.value ? undefined : load(true)
  // Another cluster: nothing on screen is its.
  epoch = currentEpoch()
  rows.value = []
  loaded.value = false
  if (pick.value) router.replace({ query: { ...route.query, mpid: undefined, mtx: undefined } })
  first()
})

// ---- paging: Newer and Older; a newer arrival is counted, not inserted ----
const olderNext = computed(() => older({ offset: offset.value, count: rows.value.length, pagination: pagination.value }))
function goOlder() {
  if (!olderNext.value.can) return
  trail.push(offset.value)
  offset.value = olderNext.value.next
  load()
}
function goNewer() {
  offset.value = trail.length ? trail.pop() : Math.max(0, offset.value - PAGE)
  load()
}
const newerText = computed(() => (newer.value >= PAGE ? `${PAGE} or more newer messages arrived.` : `${fmt(newer.value)} newer ${newer.value === 1 ? 'message' : 'messages'} arrived.`))
const when = computed(() => windowWords(win.value))
const range = computed(() => `${fmt(offset.value + 1)}–${fmt(offset.value + rows.value.length)}, newest first, ${when.value}`)
const empty = computed(() => emptyParts(filters.value, { offset: offset.value, when: when.value }))

// ---- the rows ----
const DAY = new Intl.DateTimeFormat(undefined, { day: 'numeric', month: 'short' })
function atText(ms) {
  if (ms == null) return '–'
  const d = new Date(ms)
  return d.toDateString() === new Date().toDateString() ? clock(ms, true) : `${DAY.format(d)}, ${clock(ms, true)}`
}
const cols = computed(() => (filters.value.queue
  ? '116px minmax(0, 1fr) minmax(0, 1.5fr) 128px'
  : '116px minmax(0, 1.1fr) minmax(0, 1fr) minmax(0, 1.3fr) 128px'))
const statusClass = (r) => (r.status === 'completed' ? 'qn-t3' : r.status === 'dead_letter' ? 'qmv-dead' : 'qn-t2')
const isPicked = (r) => !!pick.value && r.pid === pick.value.pid && r.txn === pick.value.txn

// ---- the pick: the highlight and the right side change, never the list ----
const side = ref(null)
const stacked = () => window.matchMedia?.('(max-width: 1100px)').matches
// Stacked (a phone, a narrow window), the detail opens under the list: once it
// is drawn, bring it up; closing it goes back to its row.
async function choose(r) {
  if (!r.addressable) return
  const off = isPicked(r)
  await router.replace({ query: { ...route.query, mpid: off ? undefined : r.pid, mtx: off ? undefined : r.txn } })
  if (!off && stacked()) { await nextTick(); side.value?.scrollIntoView?.({ block: 'start', behavior: 'smooth' }) }
}
async function closePick() {
  const was = pick.value
  await router.replace({ query: { ...route.query, mpid: undefined, mtx: undefined } })
  if (was && stacked()) {
    await nextTick()
    const k = `${was.pid} ${was.txn}`
    ;[...document.querySelectorAll('.qmv-table [data-k]')].find((e) => e.dataset.k === k)?.scrollIntoView?.({ block: 'center' })
  }
}
const pickedRow = computed(() => (pick.value ? rows.value.find(isPicked)?.raw || null : null))

// ---- push: a message to the queue in the filter, or a copy of the picked one ----
const canProduce = computed(() => can('produce'))
const pushing = ref(false)
const pushQueue = ref('')
const prefill = ref(null)
function openPush(copy = null) {
  pushQueue.value = copy ? copy.queue : filters.value.queue
  prefill.value = copy ? { partition: copy.partition, payload: copy.payload } : null
  if (pushQueue.value) pushing.value = true
}
// A push lands at the head of page 1: go there, and drop a filter that would hide it.
function pushed() {
  const f = filters.value
  const next = filtersForPushedMessage(
    { to: '', status: f.status, queue: f.queue, partition: f.partition },
    { queue: pushQueue.value, partition: prefill.value?.partition || '' },
    '',
  )
  const patch = {}
  for (const k of ['status', 'queue', 'partition']) if (next[k] !== f[k]) patch[k] = next[k]
  if (Object.keys(patch).length) setFilters(patch, true)
  else first()
}
</script>

<template>
  <div class="qmv">
    <div class="qmv-tools">
      <QcSeg v-model="status" :options="STATUSES" label="Status" />
      <MessagesWhere :queue="filters.queue" :namespace="filters.namespace" :task="filters.task" :queues="qs.queues.value" @pick="setFilters" />
      <label class="qn-field qmv-part">
        <span class="qn-vh">Partition</span>
        <input v-model="partText" class="qn-mono" type="text" placeholder="Partition, then Enter" spellcheck="false" autocomplete="off" @keydown.enter="applyPartition" @keydown.esc="partText = filters.partition" @blur="applyPartition" />
      </label>
      <div style="flex: 1 1 0" />
      <span v-if="newer" class="qmv-newer" role="status">{{ newerText }} <button type="button" class="qn-link" @click="first">{{ offset ? 'Back to the newest' : 'Show them' }}</button></span>
      <button v-if="canProduce && filters.queue" type="button" class="qn-btn2" aria-haspopup="dialog" @click="openPush()">Push a message…</button>
    </div>

    <div class="qmv-split">
      <section class="qmv-list" aria-label="Newest messages" :aria-busy="pending">
        <p v-if="stale" class="qn-t2 qmv-note"><span class="qn-mark amber" aria-hidden="true" />The last read failed ({{ stale.error }}); these rows are from {{ clock(stale.at, true) }}.</p>
        <p v-if="error" class="qn-t2 qmv-empty"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ error }}</p>
        <p v-else-if="!loaded" class="qn-t2 qmv-empty">Reading the newest messages…</p>
        <p v-else-if="!rows.length" class="qn-t2 qmv-empty">
          <span v-for="(p, i) in empty" :key="i" :class="{ 'qn-mono': p.mono }">{{ p.t }}</span>
          <button v-if="relax && offset === 0" type="button" class="qn-link" style="margin-left: 8px" @click="setFilters(relax.patch)">{{ relax.label }}</button>
        </p>
        <div v-else class="qmv-scroll" :class="{ wait: pending }">
          <div class="qmv-table" :class="{ one: !!filters.queue }" role="table" aria-label="Newest messages" :style="{ '--cols': cols }">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Pushed at</span>
              <span v-if="!filters.queue" role="columnheader">Queue</span>
              <span role="columnheader" class="qmv-part-c">Partition</span>
              <span role="columnheader" class="qmv-txn-c">transactionId</span>
              <span role="columnheader">Status</span>
            </div>
            <div
              v-for="r in rows" :key="r.key" class="qn-tr" :class="{ click: r.addressable, on: isPicked(r) }" role="row"
              :data-k="`${r.pid} ${r.txn}`" :tabindex="r.addressable ? 0 : undefined" :aria-selected="isPicked(r) ? 'true' : undefined"
              @click="choose(r)" @keydown.enter="choose(r)"
            >
              <span role="cell" class="qn-num" :title="r.raw.createdAt">{{ atText(r.at) }}</span>
              <span v-if="!filters.queue" role="cell" class="qn-mono" :title="r.queue">{{ r.queue }}</span>
              <span role="cell" class="qn-mono qn-t2 qmv-part-c" :title="r.partition">{{ r.partition }}</span>
              <span role="cell" class="qn-mono qmv-txn-c" :class="{ 'qn-t3': !r.txn }" :title="r.txn">{{ r.txn || 'no transactionId' }}</span>
              <span role="cell" :class="statusClass(r)">{{ r.word }}<span v-if="r.read" class="qn-t3 qn-num"> · {{ r.read }}</span></span>
            </div>
          </div>
        </div>

        <div v-if="loaded && !error && (rows.length || offset > 0)" class="qmv-pager">
          <span v-if="rows.length" class="qn-num qn-t3">{{ range }}</span>
          <div style="flex: 1 1 0" />
          <button type="button" class="qn-btn2" :disabled="offset === 0" @click="goNewer">Newer</button>
          <button type="button" class="qn-btn2" :disabled="!olderNext.can" @click="goOlder">Older</button>
        </div>
        <p v-if="olderNext.deep" class="qn-t3 qmv-small">The broker pages no deeper than the newest {{ fmt(10000) }}: narrow the window to read older ones.</p>
        <p class="qn-read qmv-read">Read from <span class="qn-mono">{{ call }}</span>, every 30 s. Done means every reader is past it; leased, that a worker holds its partition now.</p>
      </section>

      <div ref="side" class="qmv-side">
        <MessageDetail v-if="pick" :pick="pick" :row="pickedRow" :where="where" @close="closePick" @copy="openPush" @changed="load(true)" />
        <div v-else-if="rows.length" class="qn-panel qmv-hint">
          <p class="qn-t2">Pick a message to read it: its payload, which groups read it, and what you can do with it.</p>
        </div>
      </div>
    </div>

    <QueuePush :open="pushing" :queue="pushQueue" :where="where" :prefill="prefill" @close="pushing = false" @pushed="pushed" />
  </div>
</template>

<style scoped>
.qmv-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px; margin-top: 22px; }
.qmv-part { width: 220px; max-width: 100%; }
.qmv-newer { font-size: 13px; color: var(--text2); }
.qmv-split { display: grid; grid-template-columns: minmax(0, 1fr) minmax(340px, 440px); gap: 24px 28px; align-items: start; margin-top: 16px; }
.qmv-list { min-width: 0; }
.qmv-note { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
.qmv-empty { padding: 12px 0 4px; }
.qmv-scroll { overflow-x: auto; transition: opacity .12s ease; }
.qmv-scroll.wait { opacity: .55; }
.qmv-table { min-width: 640px; }
.qmv-table.one { min-width: 520px; }
.qmv-dead { color: var(--text); }
.qmv-pager { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; margin-top: 12px; }
.qmv-small { font-size: 12px; margin-top: 8px; }
.qmv-read { margin-top: 12px; overflow-wrap: anywhere; }
/* The detail stays in view while the list scrolls under it. */
.qmv-side { min-width: 0; position: sticky; top: 72px; max-height: calc(100vh - 96px); overflow-y: auto; }
.qmv-hint { padding: 16px 18px; }
@media (prefers-reduced-motion: reduce) { .qmv-scroll { transition: none; } }
/* Narrow: the detail under the list; the hint has no place there. */
@media (max-width: 1100px) {
  .qmv-split { grid-template-columns: minmax(0, 1fr); }
  .qmv-side { position: static; max-height: none; overflow: visible; scroll-margin-top: 16px; }
  .qmv-hint { display: none; }
}
/* A phone: when, which queue (or which message) and its status. */
@media (max-width: 640px) {
  .qmv-part { width: 100%; }
  .qmv-table, .qmv-table.one { min-width: 0; --cols: 64px minmax(0, 1fr) auto !important; }
  .qmv-table .qmv-part-c { display: none; }
  .qmv-table:not(.one) .qmv-txn-c { display: none; }
}
</style>
