<script setup>
// With no group picked, the right side holds the partitions behind more than
// a chosen age, across every group (the old page's Lagging partitions), under
// the group and queue they belong to. A group's name opens it; Skip… moves
// one partition to its newest message, after its dialog. Rows keep the place
// they were first read in; one that catches up stays, greyed, until the age
// changes.
import { computed, ref, shallowRef, watch } from 'vue'
import { midCut } from '@/composables/dlqWhy'
import QcPopover from '../QcPopover.vue'
import { fmt } from '../useQueuePage'
import { span } from '../useOverview'
import { clock } from '../useWindow'
import { AGES, behindRows, keepPlaces, LAGGING_LIMIT } from './groupsView'

const props = defineProps({
  lagging: { type: Array, default: null }, // GET /consumer-groups/lagging?minLagSeconds=0, null = not read
  lagError: { type: String, default: '' },
  min: { type: Number, default: 60 }, // seconds
  rowFor: { type: Function, required: true }, // (group, queue) → its row in the list, or null
  canAct: { type: Boolean, default: false },
})
const emit = defineEmits(['update:min', 'pick', 'act'])

const open = ref(false)
const SHOW = 25
const showAll = ref(false)
const placed = shallowRef(null)
watch(() => props.min, () => { placed.value = null; showAll.value = false })
watch([() => props.lagging, () => props.min], () => {
  if (!props.lagging) return
  placed.value = keepPlaces(placed.value, behindRows(props.lagging, { min: props.min }))
}, { immediate: true })
const parts = computed(() => placed.value?.list || [])
const shown = computed(() => (showAll.value ? parts.value : parts.value.slice(0, SHOW)))
const full = computed(() => (props.lagging?.length || 0) >= LAGGING_LIMIT)

// Under the group and queue they belong to, in the order the first of each was read.
const sections = computed(() => {
  const out = []
  const at = new Map()
  for (const p of shown.value) {
    const k = `${p.queue}\u0000${p.group}`
    if (!at.has(k)) {
      const row = props.rowFor(p.group, p.queue)
      at.set(k, out.length)
      out.push({ key: k, group: p.group, label: p.label, queueMode: p.queueMode, queue: p.queue, mark: row?.tone || '', row, parts: [] })
    }
    out[at.get(k)].parts.push(p)
  }
  for (const s of out) s.live = s.parts.filter((p) => !p.gone).length
  return out
})

function skip(s, p) {
  if (s.row) emit('act', { kind: 'partition', row: s.row, partition: p.partition, waiting: p.waiting })
}
</script>

<template>
  <section class="qn-panel cgb" aria-labelledby="cgb-title">
    <div class="cgb-head">
      <h2 id="cgb-title" class="cgb-title">Partitions behind more than</h2>
      <QcPopover v-model:open="open" side="bottom-start" :width="160" label="Behind more than">
        <template #trigger="{ toggle }">
          <button type="button" class="qn-chip cgb-chip" aria-haspopup="dialog" :aria-expanded="open" @click="toggle">
            <span class="qn-num">{{ span(min) }}</span>
            <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><polyline points="2,3.5 5,6.5 8,3.5" style="fill: none; stroke: currentColor; stroke-width: 1.3" /></svg>
          </button>
        </template>
        <template #default="{ close }">
          <button v-for="s in AGES" :key="s" type="button" class="qn-opt qn-num" :aria-pressed="s === min" @click="emit('update:min', s); close()">{{ span(s) }}</button>
        </template>
      </QcPopover>
      <span class="qn-t2">across every group</span>
    </div>

    <p v-if="lagError && !lagging" class="qn-t2 cgb-note"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ lagError }}</p>
    <p v-else-if="!lagging" class="qn-t2 cgb-note">Reading the partitions behind…</p>
    <p v-else-if="!parts.length" class="qn-t2 cgb-note">No partition is behind more than {{ span(min) }}.</p>
    <template v-else>
      <div class="cgb-table" role="table" :aria-label="`Partitions behind more than ${span(min)}`">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Partition</span>
          <span role="columnheader" class="r">Waiting</span>
          <span role="columnheader" class="r">Oldest</span>
          <span role="columnheader" class="cgb-wide">Worker</span>
          <span v-if="canAct" role="columnheader"><span class="qn-vh">Action</span></span>
        </div>
        <div v-for="s in sections" :key="s.key" role="rowgroup">
          <div class="qn-tr cgb-sec" role="row">
            <span role="rowheader" class="cgb-seccell">
              <span class="cgb-markslot" aria-hidden="true"><span v-if="s.mark" class="qn-mark" :class="s.mark" /></span>
              <button type="button" class="cgb-g" :class="s.queueMode ? 'qn-t2' : 'qn-mono'" :title="`Open ${s.label} on ${s.queue}`" @click="emit('pick', s.group, s.queue)">{{ s.label }}</button>
              <span class="qn-t3">on</span>
              <RouterLink :to="`/queues/${encodeURIComponent(s.queue)}`" class="qn-mono cgb-q">{{ s.queue }}</RouterLink>
            </span>
          </div>
          <div v-for="p in s.parts" :key="p.key" class="qn-tr" :class="{ 'cgb-gone': p.gone }" role="row">
            <span role="cell" class="qn-mono cgb-p" :title="p.partition">{{ midCut(p.partition, 44) }}</span>
            <span role="cell" class="r qn-num">{{ p.gone ? '–' : fmt(p.waiting) }}</span>
            <span role="cell" class="r qn-num" :title="p.oldestAt && !p.gone ? `Oldest unread pushed at ${clock(Date.parse(p.oldestAt), true)}` : undefined">{{ p.gone ? 'caught up' : span(p.lag) }}</span>
            <span role="cell" class="qn-mono qn-t2 cgb-w cgb-wide" :title="p.worker || undefined">{{ p.worker ? midCut(p.worker, 14) : '–' }}</span>
            <span v-if="canAct" role="cell" class="r">
              <button v-if="!p.gone && s.row" type="button" class="qn-btn3 cgb-skip" :aria-label="`Skip partition ${p.partition} for ${s.label}`" @click="skip(s, p)">Skip…</button>
            </span>
          </div>
        </div>
      </div>
      <p v-if="parts.length > SHOW" class="cgb-more qn-num qn-t3">
        {{ showAll ? fmt(parts.length) : `${fmt(SHOW)} of ${fmt(parts.length)}` }}
        <button type="button" class="qn-link" @click="showAll = !showAll">{{ showAll ? 'Show fewer' : 'Show all' }}</button>
      </p>
    </template>
    <p class="qn-read cgb-read">
      Read from <span class="qn-mono">GET /api/v1/consumer-groups/lagging?minLagSeconds=0</span>: the broker lists the {{ fmt(LAGGING_LIMIT) }} partitions furthest behind<template v-if="full">, a cut this cluster fills</template>, and these are the ones past {{ span(min) }}.
      Oldest is the age of the oldest message the group has not read there.
    </p>
  </section>
</template>

<style scoped>
.cgb { min-width: 0; }
.cgb-head { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 10px; }
.cgb-title { font-size: 15px; font-weight: 500; }
.cgb-chip { height: 28px; }
.cgb-note { margin-top: 14px; display: flex; align-items: center; }
.cgb-table { margin-top: 14px; --cols: minmax(0, 1fr) 72px 76px minmax(0, 120px) 64px; }
.cgb-table .qn-tr > :first-child { padding-left: 18px; }
/* a group on a queue: the rows under it are its partitions */
.qn-tr.cgb-sec { --cols: minmax(0, 1fr); min-height: 40px; }
.cgb-table .qn-tr.cgb-sec > :first-child { padding-left: 0; }
.cgb-seccell { display: flex; align-items: baseline; gap: 6px; min-width: 0; }
.cgb-markslot { flex: 0 0 8px; display: flex; align-self: center; align-items: center; justify-content: center; margin-right: 4px; }
.cgb-g { font-weight: 500; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 0 1 auto; }
.cgb-g:hover { text-decoration: underline; text-underline-offset: 3px; }
.cgb-q { color: var(--text2); min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 0 1 auto; }
.cgb-p { font-size: 12.5px; }
.cgb-w { font-size: 12px; }
.cgb-skip { height: 26px; font-size: 12px; margin-right: -8px; }
.cgb-gone > * { color: var(--text3); }
.cgb-more { display: flex; align-items: center; gap: 10px; margin-top: 8px; font-size: 12px; }
.cgb-read { margin-top: 12px; overflow-wrap: anywhere; }
@media (max-width: 640px) {
  .cgb.qn-panel { padding: 16px 14px; }
  .cgb-table { --cols: minmax(0, 1fr) 56px 64px 56px; }
  .cgb-table .cgb-wide { display: none; }
}
</style>
