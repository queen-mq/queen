<script setup>
// What the Consumer groups view and a queue's page do to a group, one dialog
// at a time: skip what waits, read again from a time, move one partition (to
// its newest message, or back to a time), delete it on one queue or
// everywhere. Each shows what changes, the cluster and the exact call before
// it runs (groupsView builds the call; sendCall sends those same parts
// through the app's API layer), and the broker's answer comes back in a
// toast. A 2xx that moved or deleted nothing says so: it did nothing.
import { computed, ref, shallowRef, watch } from 'vue'
import { consumers as consumersApi, queues as queuesApi } from '@/api'
import { describeApiError } from '@/api/errors'
import QcDialog from '../QcDialog.vue'
import QcSeg from '../QcSeg.vue'
import { toast } from '../toasts'
import { fmt } from '../useQueuePage'
import { span } from '../useOverview'
import { clock, timeZone } from '../useWindow'
import {
  deleteCall, deleteOutcome, parseWhen, partitionCall, restartOf, seekCall, seekOutcome, sendCall, whenText,
} from './groupsView'

const props = defineProps({
  // { kind: 'skip'|'seek'|'partition'|'delete'|'deleteAll', row, partition?, waiting?, to?, queues?, allMembers?, allWaiting? }
  // A 'partition' act with no partition asks for one; `to` ('end' | 'time') is where it starts.
  act: { type: Object, default: null },
  where: { type: String, default: '' }, // the cluster
})
const emit = defineEmits(['close', 'done'])

const busy = ref(false)
const keep = ref(false) // keep its subscription: off by default, as the old page's box was on
const field = ref('')
const partTo = ref('end') // one partition: 'end' (skip what waits) or 'time'
const partField = ref('') // one partition, when the act named none
const partList = shallowRef([]) // the queue's partitions, for the field's suggestions
watch(() => props.act, (a) => {
  if (!a) return
  keep.value = false
  field.value = whenText(Date.now() - 3600_000)
  partTo.value = a.to === 'time' ? 'time' : 'end'
  partField.value = ''
  partList.value = []
  if (a.kind === 'partition' && !a.partition && a.row?.queue) loadPartitions(a.row.queue)
})
async function loadPartitions(queue) {
  try {
    const r = await queuesApi.partitions({ queue, limit: 500 }, { quiet: true, probe: true })
    if (props.act?.row?.queue !== queue) return
    partList.value = (r.data?.partitions || []).map((p) => p.partition).filter((p) => typeof p === 'string')
  } catch { /* no suggestions: the field still takes a name */ }
}
const PART_TO = [
  { value: 'end', label: 'To its newest message' },
  { value: 'time', label: 'Back to a time' },
]

const a = computed(() => props.act)
const kind = computed(() => a.value?.kind || '')
const row = computed(() => a.value?.row || null)
const label = computed(() => row.value?.label || '')
const queue = computed(() => row.value?.queue || '')
// What a destructive action asks to type: the group's name, or the queue's for queue mode, which has none.
const typed = computed(() => (row.value?.queueMode ? queue.value : row.value?.name || ''))
const plural = (n, one, many) => `${fmt(n)} ${n === 1 ? one : many}`
// A seek moves the group on every partition of the queue, cursor or not.
const across = computed(() => {
  const n = row.value?.partitions ?? row.value?.members
  if (n == null) return 'in every partition'
  return n === 1 ? 'in its one partition' : `in all ${plural(n, 'partition', 'partitions')}`
})
const DAY = (ms) => new Date(ms).toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
const tz = timeZone()

// ---- one partition: the one the act named, or the one typed ----
const part = computed(() => (a.value?.partition ? a.value.partition : partField.value.trim()))
const partProblem = computed(() => (kind.value === 'partition' && !part.value ? 'Name the partition, as its messages carry it.' : ''))

// ---- the time of a seek: the whole queue's, or one partition's ----
const wantsTime = computed(() => kind.value === 'seek' || (kind.value === 'partition' && partTo.value === 'time'))
const at = computed(() => parseWhen(field.value))
const QUICK = [
  { label: '15 min ago', s: 900 },
  { label: '1 h ago', s: 3600 },
  { label: '6 h ago', s: 6 * 3600 },
  { label: '24 h ago', s: 24 * 3600 },
]
const seekProblem = computed(() => {
  if (!wantsTime.value) return ''
  if (at.value == null) return 'Type a date and a time, like 2026-10-31 09:30.'
  if (at.value > Date.now()) return kind.value === 'seek' ? 'That is later than now. To skip everything that waits, use Skip what waits.' : 'That is later than now. To skip what waits there, move it to its newest message.'
  return ''
})
const atWords = computed(() => {
  if (at.value == null) return ''
  const sameDay = new Date(at.value).toDateString() === new Date().toDateString()
  return sameDay ? clock(at.value) : DAY(at.value)
})

// ---- the call each dialog shows, and sends ----
const call = computed(() => {
  const x = a.value
  const r = row.value
  if (!x || !r) return null
  if (x.kind === 'skip') return seekCall(r.name, r.queue)
  if (x.kind === 'seek') return seekCall(r.name, r.queue, at.value ?? Date.now())
  if (x.kind === 'partition') return partitionCall(r.name, r.queue, part.value || '…', partTo.value === 'time' ? (at.value ?? Date.now()) : null)
  if (x.kind === 'delete') return deleteCall(r.name, r.queue, !keep.value)
  if (x.kind === 'deleteAll') return deleteCall(r.name, null, !keep.value)
  return null
})
// Until the time reads as one, the call says where it goes, not a made-up
// instant; until a partition is named, the path shows where it will go.
const callText = computed(() => {
  const c = call.value
  if (!c) return ''
  const text = wantsTime.value && seekProblem.value ? `POST ${c.path} {"timestamp":"…"}` : c.text
  return partProblem.value ? text.replace(encodeURIComponent('…'), '…') : text
})

// ---- what a deleted group does next ----
const SUBS = [
  { value: false, label: 'Delete its subscription too' },
  { value: true, label: 'Keep its subscription' },
]
const restart = computed(() => {
  const r = row.value
  if (!r) return ''
  const w = kind.value === 'deleteAll' ? a.value.allWaiting : r.waiting
  const skipped = w > 0 ? `, so the ${fmt(w)} waiting now ${w === 1 ? 'is' : 'are'} skipped` : ''
  const where = kind.value === 'deleteAll' ? 'a queue' : r.queue
  const s = restartOf(r, keep.value)
  if (s.kind === 'queueMode') return `Queue mode always starts from the oldest message a queue keeps: the next pop of ${r.queue} without a group gets every message it still keeps, again.`
  if (s.kind === 'fresh') return `If its workers pop ${where} again, it starts as a new group, from what they ask: by default only messages pushed after that${skipped}.`
  if (kind.value === 'deleteAll') return 'It keeps its subscription on each queue: if its workers pop one again, it reads from where that subscription starts, and what it read before is delivered again.'
  if (s.kind === 'all') return `It keeps its subscription from the start: if its workers pop ${r.queue} again, every message the queue still keeps is delivered to it again.`
  const since = s.since ? DAY(s.since) : 'its first pop'
  return s.kind === 'timestamp'
    ? `It keeps its subscription from ${since}: if its workers pop ${r.queue} again, everything the queue still keeps from then on is delivered to it again.`
    : `It keeps its subscription, new messages since ${since}: if its workers pop ${r.queue} again, everything the queue still keeps from then on is delivered to it again.`
})

// ---- the dialogs' words ----
const title = computed(() => {
  switch (kind.value) {
    case 'skip': return `Skip what waits for ${label.value}?`
    case 'seek': return `Seek ${label.value} to a time?`
    case 'partition': return partTo.value === 'time' ? `Seek one partition of ${label.value} to a time?` : `Skip one partition for ${label.value}?`
    case 'delete': return `Delete ${label.value} on ${queue.value}?`
    case 'deleteAll': return `Delete ${label.value} everywhere?`
    default: return ''
  }
})
const confirmLabel = computed(() => {
  switch (kind.value) {
    case 'skip': return `Skip on ${props.where}`
    case 'seek': return `Seek on ${props.where}`
    case 'partition': return partTo.value === 'time' ? `Seek on ${props.where}` : `Skip on ${props.where}`
    default: return `Delete on ${props.where}`
  }
})
// Reading again delivers twice, skipping loses: only the second is red.
const danger = computed(() => !wantsTime.value)
const blocked = computed(() => !!seekProblem.value || !!partProblem.value)
// Skipping a whole queue and deleting are typed; one partition is named in full instead.
const typedFor = computed(() => (['skip', 'delete', 'deleteAll'].includes(kind.value) ? typed.value : ''))

function close() {
  if (busy.value) return
  emit('close')
}

async function run() {
  const c = call.value
  const x = a.value
  if (!c || !x || busy.value || blocked.value) return
  busy.value = true
  const who = label.value
  const on = props.where
  try {
    const r = await sendCall(consumersApi, c)
    if (c.kind === 'seek' || c.kind === 'seekPartition') {
      const o = seekOutcome(r?.data)
      if (!o.ok) {
        toast(`Nothing moved on ${on}`, o.problem, 'red')
      } else if (x.kind === 'skip') {
        toast(`Skipped what waited for ${who} on ${on}`, `The broker answered ${r.status || 200}: ${plural(o.moved, 'partition', 'partitions')} of ${c.queue} moved to their newest message.`)
      } else if (x.kind === 'seek') {
        toast(`Moved ${who} to ${atWords.value} on ${on}`, `The broker answered ${r.status || 200}: ${plural(o.moved, 'partition', 'partitions')} of ${c.queue} read again from there.`)
      } else if (c.body?.timestamp) {
        toast(`Moved ${who} to ${atWords.value} in one partition on ${on}`, `The broker answered ${r.status || 200}: ${c.partition} of ${c.queue} is read again from there.`)
      } else {
        toast(`Skipped one partition for ${who} on ${on}`, `The broker answered ${r.status || 200}: ${c.partition} of ${c.queue} moved to its newest message.`)
      }
      emit('done', { call: c, ok: o.ok })
    } else {
      const o = deleteOutcome(r?.data)
      if (!o.ok) {
        toast(`Nothing deleted on ${on}`, o.problem, 'red')
      } else {
        const sub = c.deleteMetadata ? ', and its subscription' : '; its subscription stays'
        toast(
          c.queue ? `Deleted ${who} on ${c.queue}, on ${on}` : `Deleted ${who} everywhere on ${on}`,
          `The broker answered ${r.status || 200}: ${plural(o.deleted, 'cursor', 'cursors')} removed${sub}.`,
        )
      }
      emit('done', { call: c, ok: o.ok })
    }
  } catch (e) {
    toast(c.method === 'DELETE' ? 'Not deleted' : 'Not moved', describeApiError(e), 'red')
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <QcDialog
    :open="!!act"
    :title="title"
    :where="where"
    :call="callText"
    :confirm-label="confirmLabel"
    :danger="danger"
    :typed="typedFor"
    :busy="busy"
    :blocked="blocked"
    @confirm="run"
    @cancel="close"
  >
    <template v-if="act && row">
      <!-- skip what waits: the whole queue -->
      <template v-if="kind === 'skip'">
        <div class="qa-change qn-num">
          <span class="qn-t2">Waiting for <span :class="{ 'qn-mono': !row.queueMode }">{{ label }}</span> in <span class="qn-mono">{{ queue }}</span></span>
          <span>{{ fmt(row.waiting) }} → 0</span>
          <span class="qn-t2">Its cursor {{ across }}</span>
          <span>→ the newest message</span>
        </div>
        <p class="qn-t2 qa-p">They are never delivered to {{ row.queueMode ? 'pops without a group' : label }}; other groups keep theirs. Batches its workers hold now are released.</p>
        <p class="qn-t3 qa-p">To read them later, seek to a time before they arrived, while the queue still keeps them.</p>
      </template>

      <!-- read again from a time -->
      <template v-else-if="kind === 'seek'">
        <label class="qa-when">
          <span class="qn-t2">Read <span class="qn-mono">{{ queue }}</span> again from</span>
          <span class="qn-field qa-field"><input v-model="field" class="qn-mono" type="text" spellcheck="false" autocomplete="off" aria-describedby="qa-tz" /></span>
          <span id="qa-tz" class="qn-t3 qa-tz">{{ tz }}</span>
        </label>
        <div class="qa-quick" role="group" aria-label="Quick times">
          <button v-for="p in QUICK" :key="p.s" type="button" class="qn-btn3" @click="field = whenText(Date.now() - p.s * 1000)">{{ p.label }}</button>
        </div>
        <p v-if="seekProblem" class="qn-t2 qa-p">{{ seekProblem }}</p>
        <template v-else>
          <div class="qa-change qn-num">
            <span class="qn-t2">Its cursor {{ across }}</span>
            <span>→ the last message before {{ atWords }}</span>
          </div>
          <p class="qn-t2 qa-p">Every message pushed from {{ atWords }} on is delivered to {{ row.queueMode ? 'pops without a group' : label }} again, read or not, and anything older that still waits is skipped. Batches its workers hold now are released.</p>
        </template>
      </template>

      <!-- one partition: to its newest message, or back to a time -->
      <template v-else-if="kind === 'partition'">
        <label v-if="!act.partition" class="qa-part">
          <span class="qn-t2">Partition of <span class="qn-mono">{{ queue }}</span></span>
          <span class="qn-field qa-pfield"><input v-model="partField" class="qn-mono" type="text" spellcheck="false" autocomplete="off" list="qa-parts" placeholder="customer-123" /></span>
          <datalist id="qa-parts"><option v-for="p in partList" :key="p" :value="p" /></datalist>
        </label>
        <QcSeg v-model="partTo" :options="PART_TO" label="Move its cursor" class="qa-seg qa-segtop" />
        <template v-if="partTo === 'time'">
          <label class="qa-when qa-p">
            <span class="qn-t2">Read it again from</span>
            <span class="qn-field qa-field"><input v-model="field" class="qn-mono" type="text" spellcheck="false" autocomplete="off" aria-describedby="qa-tz" /></span>
            <span id="qa-tz" class="qn-t3 qa-tz">{{ tz }}</span>
          </label>
          <div class="qa-quick" role="group" aria-label="Quick times">
            <button v-for="p in QUICK" :key="p.s" type="button" class="qn-btn3" @click="field = whenText(Date.now() - p.s * 1000)">{{ p.label }}</button>
          </div>
          <p v-if="seekProblem" class="qn-t2 qa-p">{{ seekProblem }}</p>
          <template v-else>
            <div class="qa-change qn-num">
              <span class="qn-t2">Its cursor in <span class="qn-mono qa-break">{{ part || 'the partition' }}</span></span>
              <span>→ the last message before {{ atWords }}</span>
            </div>
            <p class="qn-t2 qa-p">Every message of that partition pushed from {{ atWords }} on is delivered to {{ row.queueMode ? 'pops without a group' : label }} again, read or not. Its other partitions of <span class="qn-mono">{{ queue }}</span> are not touched.</p>
          </template>
        </template>
        <template v-else>
          <div class="qa-change qn-num qa-p">
            <template v-if="act.waiting != null">
              <span class="qn-t2">Waiting in <span class="qn-mono qa-break">{{ part }}</span></span>
              <span>{{ fmt(act.waiting) }} → 0</span>
            </template>
            <template v-else>
              <span class="qn-t2">Its cursor in <span class="qn-mono qa-break">{{ part || 'the partition' }}</span></span>
              <span>→ its newest message</span>
            </template>
          </div>
          <p class="qn-t2 qa-p">What waits there is never delivered to {{ row.queueMode ? 'pops without a group' : label }}. Its other partitions of <span class="qn-mono">{{ queue }}</span> are not touched.</p>
        </template>
      </template>

      <!-- delete: one queue, or everywhere -->
      <template v-else>
        <div class="qa-change qn-num">
          <template v-if="kind === 'deleteAll'">
            <span class="qn-t2">Its cursors on its {{ fmt(act.queues) }} queues</span>
            <span>{{ fmt(act.allMembers) }} → none</span>
          </template>
          <template v-else>
            <span class="qn-t2">Its cursors on <span class="qn-mono">{{ queue }}</span></span>
            <span>{{ fmt(row.members) }} → none</span>
          </template>
        </div>
        <QcSeg v-if="!row.queueMode" v-model="keep" :options="SUBS" label="Its subscription" class="qa-seg" />
        <p class="qn-t2 qa-p">{{ restart }}</p>
        <p class="qn-t2 qa-p">It cannot be undone.</p>
      </template>
    </template>
  </QcDialog>
</template>

<style scoped>
.qa-change { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px 16px; font-size: 12.5px; }
.qa-change > span:nth-child(even) { text-align: right; white-space: nowrap; }
.qa-p { margin-top: 12px; }
.qa-break { overflow-wrap: anywhere; }
.qa-when { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 10px; }
.qa-field { width: 190px; }
.qa-tz { font-size: 12px; }
.qa-quick { display: flex; flex-wrap: wrap; gap: 2px; margin: 8px 0 14px -8px; }
.qa-quick .qn-btn3 { height: 28px; font-size: 12px; }
.qa-seg { margin-top: 14px; }
.qa-segtop { margin-top: 12px; }
.qa-part { display: flex; flex-direction: column; gap: 6px; }
.qa-pfield { width: 100%; }
</style>
