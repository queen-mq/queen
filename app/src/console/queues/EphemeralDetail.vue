<script setup>
// One ephemeral queue, beside the list (?show=eph&name=): its partitions and
// groups on the node that answered, what it drops and by which rule, what
// survives what, and the two writes: reset and delete, each shown with its
// call before it runs.
import { computed, ref, shallowRef, watch } from 'vue'
import { ephemeral as ephemeralApi } from '@/api'
import client from '@/api/client'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { isQueueGone, partitionRows } from '@/composables/useEphemeralSide'
import { lossSeverity, SEV_WARN } from '@/composables/useSeverity'
import { currentEpoch, useIdentity } from '@/stores/identity'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { clock } from '../useWindow'
import { fmt } from '../useQueuePage'
import {
  callText, deleteAnswer, deleteRequest, groupRows, heldWords, lossRows, optionsOf, resetAnswer, resetRequest, size,
  survival,
} from './ephemeralView'

const props = defineProps({
  name: { type: String, required: true },
  row: { type: Object, default: null }, // its row in the list (ephemeralStore), or null
  where: { type: String, default: '' },
})
const emit = defineEmits(['close', 'changed', 'deleted'])
const { can } = useIdentity()
const asking = ref(null) // 'reset' | 'delete'
const busy = ref(false)

// ---- the reads: its depth route on every tick, its options on a pick ----
const depth = shallowRef(null)   // { name, data }
const options = shallowRef(null) // { name, data }
const failed = ref(null)
const gone = ref(false)
const readAt = ref(0)
let seq = 0
async function load(withOptions = false) {
  const n = props.name
  const my = ++seq
  const at = currentEpoch()
  try {
    const r = await ephemeralApi.depth(n, { probe: true, quiet: true })
    if (my !== seq || at !== currentEpoch()) return
    depth.value = { name: n, data: r.data }
    failed.value = null
    gone.value = false
    readAt.value = Date.now()
  } catch (e) {
    if (my !== seq || at !== currentEpoch()) return
    if (isQueueGone(e)) { depth.value = null; gone.value = true; failed.value = null } else failed.value = describeApiError(e)
  }
  if (!withOptions) return
  try {
    const l = await ephemeralApi.queues({ probe: true, quiet: true })
    if (n === props.name && at === currentEpoch()) options.value = { name: n, data: optionsOf(l.data, n) }
  } catch { /* the rules stay unsaid; the counts are the list's */ }
}
watch(() => props.name, () => {
  depth.value = null; options.value = null; failed.value = null; gone.value = false; asking.value = null
  load(true)
}, { immediate: true })
useAutoRefresh(() => load(false))

const d = computed(() => (depth.value?.name === props.name ? depth.value.data : null))
const declared = computed(() => (props.row?.declared ?? (d.value?.tier === 'declared' ? true : d.value?.tier === 'implicit' ? false : null)))
const parts = computed(() => (d.value ? partitionRows(d.value) || [] : null))
const groups = computed(() => (d.value ? groupRows(d.value) || [] : null))
const skipTone = (g) => (lossSeverity({ dropped: g.skipped }) === SEV_WARN ? 'amber' : '')
const opts = computed(() => (options.value?.name === props.name ? options.value.data : null))
const loss = computed(() => lossRows(props.row?.drops || {}, opts.value))
const lasts = computed(() => survival(declared.value))
const missing = computed(() => gone.value && !d.value)

// ---- reset and delete ----
// The proxy lets producers and consumers reset and delete (any role but a viewer).
const canWrite = computed(() => can('produce') || can('consume'))
const req = computed(() => (asking.value === 'reset' ? resetRequest(props.name) : asking.value === 'delete' ? deleteRequest(props.name) : null))
const held = computed(() => heldWords(props.row?.depth ?? d.value?.pending))
// The request object the dialog shows is the one sent.
function send(r) {
  const m = r.method.toLowerCase()
  return r.body !== undefined ? client[m](r.path, r.body, { quiet: true }) : client[m](r.path, { quiet: true })
}
async function run() {
  const r = req.value
  const kind = asking.value
  if (!r) return
  busy.value = true
  try {
    const res = await send(r)
    if (kind === 'reset') {
      toast(`${props.name} reset on ${props.where}`, resetAnswer(res.data))
      asking.value = null
      emit('changed')
      load(false)
    } else {
      toast(`${props.name} deleted on ${props.where}`, deleteAnswer(res.data))
      asking.value = null
      emit('deleted')
    }
  } catch (e) {
    toast(kind === 'reset' ? 'Not reset' : 'Not deleted', describeApiError(e), 'red')
  } finally { busy.value = false }
}
</script>

<template>
  <aside class="ed qn-panel" aria-label="Ephemeral queue details">
    <div class="ed-head">
      <h3 class="qn-mono ed-name">{{ name }}</h3>
      <button type="button" class="qn-btn3 ed-x" aria-label="Close the details" @click="emit('close')">✕</button>
    </div>

    <p v-if="missing" class="ed-lead">There is no ephemeral queue named <span class="qn-mono">{{ name }}</span> on the node that answered. It was deleted, or collected once empty and idle.</p>
    <template v-else>
      <h4 class="qn-h2 ed-h ed-first">Partitions</h4>
      <p v-if="failed && !d" class="qn-t2 ed-p"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ failed }}</p>
      <p v-else-if="!parts" class="qn-t2 ed-p">Reading its partitions…</p>
      <p v-else-if="!parts.length" class="qn-t2 ed-p">The node that answered holds none of its partitions now.</p>
      <p v-else-if="parts.length === 1" class="ed-p">One partition, <span class="qn-mono">{{ parts[0].name }}</span>.</p>
      <div v-else class="ed-table" role="table" aria-label="Its partitions" style="--cols: minmax(0, 1fr) 76px 84px">
        <div class="qn-tr head" role="row"><span role="columnheader">Partition</span><span role="columnheader" class="r">Waiting</span><span role="columnheader" class="r">In memory</span></div>
        <div v-for="p in parts" :key="p.name" class="qn-tr" role="row">
          <span role="cell" class="qn-mono wrap ed-cname">{{ p.name }}</span>
          <span role="cell" class="r qn-num" :class="{ 'qn-t3': !p.depth }">{{ fmt(p.depth) }}</span>
          <span role="cell" class="r qn-num" :class="{ 'qn-t3': !p.bytes }">{{ size(p.bytes) }}</span>
        </div>
      </div>
      <p class="qn-t3 ed-small">The broker does not say which node holds each partition.</p>

      <h4 class="qn-h2 ed-h">Consumer groups</h4>
      <p v-if="failed && !d" class="qn-t2 ed-p">Not known: the read failed.</p>
      <p v-else-if="!groups" class="qn-t2 ed-p">Reading its groups…</p>
      <p v-else-if="!groups.length" class="qn-t2 ed-p">No group has a cursor on the node that answered.</p>
      <div v-else class="ed-table" role="table" aria-label="Its consumer groups" style="--cols: minmax(0, 1fr) 92px 72px">
        <div class="qn-tr head" role="row"><span role="columnheader">Group</span><span role="columnheader" class="r">Not read yet</span><span role="columnheader" class="r">Skipped</span></div>
        <div v-for="g in groups" :key="g.name" class="qn-tr" role="row">
          <span role="cell" class="qn-mono wrap ed-cname">{{ g.label }}</span>
          <span role="cell" class="r qn-num" :class="{ 'qn-t3': !g.pending }">{{ fmt(g.pending) }}</span>
          <span role="cell" class="r qn-num ed-skip" :class="skipTone(g) || { 'qn-t3': !g.skipped }">
            <span v-if="skipTone(g)" class="qn-mark amber" aria-hidden="true" />{{ fmt(g.skipped) }}
          </span>
        </div>
      </div>
      <p v-if="groups && groups.some((g) => g.skipped)" class="qn-t3 ed-small">Skipped: dropped before the group read them, by the rules below.</p>
      <p v-if="groups && row && row.groupCount != null && row.groupCount !== groups.length" class="qn-t3 ed-small">
        The list counts every group that ever popped it; only groups with a cursor on the node that answered show here.
      </p>

      <h4 class="qn-h2 ed-h">What it drops</h4>
      <div class="ed-loss" role="table" aria-label="Messages dropped unread, by cause">
        <div v-for="l in loss" :key="l.key" class="ed-lrow" role="row">
          <span role="rowheader" class="ed-lname">{{ l.label }}</span>
          <span role="cell" class="qn-t2 ed-lrule">{{ l.rule || '–' }}</span>
          <span role="cell" class="qn-num ed-ln" :class="{ 'qn-t3': !l.n }">{{ fmt(l.n) }}</span>
        </div>
      </div>

      <h4 class="qn-h2 ed-h">What survives</h4>
      <div class="ed-lasts" role="table" aria-label="What survives what">
        <div v-for="s in lasts" :key="s.when" class="ed-srow" role="row">
          <span role="rowheader" class="ed-when">{{ s.when }}</span>
          <span role="cell" class="ed-kept" :class="s.kept ? '' : 'qn-t2'">{{ s.kept ? 'Kept' : 'Lost' }}</span>
          <span role="cell" class="qn-t2 ed-why">{{ s.why }}</span>
        </div>
      </div>

      <div class="ed-acts">
        <template v-if="canWrite">
          <button type="button" class="qn-btn2" aria-haspopup="dialog" @click="asking = 'reset'">Reset…</button>
          <button type="button" class="qn-btn2 danger" aria-haspopup="dialog" @click="asking = 'delete'">Delete…</button>
        </template>
        <p v-else class="qn-t3 ed-small" style="margin: 0">Reset and delete need a producer or consumer role on this cluster.</p>
      </div>
    </template>

    <p class="qn-read ed-read">
      Read from <span class="qn-mono">GET /api/v1/ephemeral/queues/{{ encodeURIComponent(name) }}/depth</span><template v-if="readAt"> at {{ clock(readAt, true) }}</template>,
      on the node that answered, every 30 s.
    </p>

    <QcDialog
      :open="asking === 'reset'" :title="`Reset ${name}?`" :where="where" :call="req ? callText(req) : ''" :typed="name"
      confirm-label="Reset" danger :busy="busy" @confirm="run" @cancel="asking = null"
    >
      <p class="qn-t2">Every node drops this queue's messages now ({{ held }}), voids every lease and moves every group to the end. The queue and its configuration stay.</p>
      <p class="qn-t2" style="margin-top: 8px">Nothing can bring the messages back.</p>
    </QcDialog>
    <QcDialog
      :open="asking === 'delete'" :title="`Delete ${name}?`" :where="where" :call="req ? callText(req) : ''" :typed="name"
      confirm-label="Delete" danger :busy="busy" @confirm="run" @cancel="asking = null"
    >
      <p class="qn-t2">Every node drops this queue's messages now ({{ held }}) and forgets the queue.</p>
      <p class="qn-t2" style="margin-top: 8px">{{ declared === true
        ? 'Its stored configuration is deleted too: a later push makes it again, with the defaults.'
        : 'A later push or pop makes it again, empty.' }} Nothing can bring the messages back.</p>
    </QcDialog>
  </aside>
</template>

<style scoped>
.ed { min-width: 0; padding: 16px 18px 18px; }
.ed-head { display: flex; align-items: flex-start; gap: 10px; }
.ed-name { flex: 1 1 0; min-width: 0; font-size: 13.5px; font-weight: 500; line-height: 1.45; overflow-wrap: anywhere; }
.ed-x { flex: 0 0 auto; height: 28px; margin: -3px -6px 0 0; }
.ed-lead { font-size: 13px; margin-top: 8px; }
.ed-h { margin-top: 20px; }
.ed-h.ed-first { margin-top: 12px; }
.ed-p { font-size: 13px; margin-top: 8px; overflow-wrap: anywhere; }
.ed-table { margin-top: 6px; }
/* A partition or a group is often a pod name: said whole, on two lines if it must. */
.qn-tr .ed-cname { overflow-wrap: anywhere; padding: 8px 0; line-height: 1.4; }
.ed-small { font-size: 12px; margin-top: 8px; }
.ed-skip { display: inline-flex; align-items: center; justify-content: flex-end; gap: 6px; }
.ed-skip.amber { color: var(--amber); }
.ed-loss, .ed-lasts { margin-top: 8px; display: grid; gap: 10px; }
.ed-lrow { display: grid; grid-template-columns: 92px minmax(0, 1fr) 56px; gap: 12px; align-items: baseline; font-size: 12.5px; }
.ed-lname { color: var(--text); }
.ed-lrule { line-height: 1.45; }
.ed-ln { text-align: right; }
.ed-srow { display: grid; grid-template-columns: 132px 40px minmax(0, 1fr); gap: 12px; align-items: baseline; font-size: 12.5px; }
.ed-kept { font-weight: 500; }
.ed-why { line-height: 1.45; }
.ed-acts { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 20px; }
.ed-read { margin-top: 14px; overflow-wrap: anywhere; }
@media (max-width: 640px) {
  .ed-srow { grid-template-columns: minmax(0, 1fr) 40px; }
  .ed-why { grid-column: 1 / -1; margin-top: -6px; }
}
</style>
