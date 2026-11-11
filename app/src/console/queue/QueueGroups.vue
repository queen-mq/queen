<script setup>
// The groups reading this queue: how far behind each is, and where it reads
// from. State words, marks and the drain estimate are the Consumer groups
// view's (console/queues/groupsView.js), so the two never disagree. The order
// is fixed at first sight, worst first; a group seen later joins at the end.
//
// A row opens the group beside the list (below it on a phone), in the same
// panel as Queues › Consumer groups (QgDetail): its partitions furthest
// behind, and what you can do, with the same dialogs (QgAct): skip what
// waits, seek to a time, move one partition, delete it. ?group= keeps the
// pick in the address. After an action the page reads everything again.
//
// Reads, while a group is open, every 30 s:
//   GET /api/v1/consumer-groups/lagging?minLagSeconds=0  its partitions behind
import { computed, nextTick, ref, shallowRef, watch } from 'vue'
import { consumers as consumersApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { currentEpoch, useIdentity } from '@/stores/identity'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueuesStore } from '@/stores/queuesStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { groupRows, statusOf, stateWords, stateTitle, QUIET_STATES } from '../queues/groupsView'
import QgDetail from '../queues/QgDetail.vue'
import QgAct from '../queues/QgAct.vue'
import { span } from '../words.js'
import { fmt, QUEUE_MODE } from '../useQueuePage'

const props = defineProps({
  groups: { type: Array, default: () => [] },
  linesFor: { type: Function, default: null },
  queue: { type: String, default: '' },
  where: { type: String, default: '' }, // the cluster, for the dialogs
})
const emit = defineEmits(['changed'])
const { can } = useIdentity()
const canAct = computed(() => can('queueAdmin'))
const qs = useQueuesStore()
const gs = useGroupsStore()
const os = useQueueOpsStore()

function readsFrom(g) {
  const m = g.subscriptionMode
  if (m === 'all') return 'the start'
  if (m === 'new') return 'new messages'
  if (m && g.subscriptionTimestamp) return 'from ' + new Date(g.subscriptionTimestamp).toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
  return g.name === QUEUE_MODE ? 'every message' : '–'
}

const list = computed(() => groupRows({ groups: props.groups, queues: qs.queues.value || [], ops: os.ops.value, series: os.series.value, linesFor: props.linesFor }))
const place = new Map()
const rows = computed(() => {
  const raw = new Map(props.groups.map((g) => [g.name, g]))
  const all = [...list.value]
  // first sight: worst first, then by name; afterwards nobody moves
  const fresh = all.filter((r) => !place.has(r.key))
    .sort((a, b) => (b.lag || 0) - (a.lag || 0) || String(a.name).localeCompare(String(b.name)))
  for (const r of fresh) place.set(r.key, place.size)
  return all.sort((a, b) => place.get(a.key) - place.get(b.key)).map((r) => {
    const g = raw.get(r.name) || {}
    const st = statusOf(r)
    return {
      key: r.key,
      group: r.name,
      row: r,
      name: r.label,
      mono: !r.queueMode,
      kafka: r.kafka,
      mark: st.tone || '',
      state: stateWords(st) || '–',
      stateClass: st.tone || (QUIET_STATES.has(st.kind) ? 'qn-t3' : 'qn-t2'),
      stateTitle: stateTitle(r, st) || undefined,
      waiting: fmt(r.waiting || 0),
      oldest: r.lag ? span(r.lag) : '–',
      behind: r.behind ? `${fmt(r.behind)} of ${fmt(r.members)}` : '0',
      readers: g.liveMembers == null ? '–' : fmt(g.liveMembers),
      from: readsFrom(g),
    }
  })
})
// Readers are known for Kafka groups only: the column shows when one has them.
const withReaders = computed(() => rows.value.some((r) => r.readers !== '–'))

// ---- the pick: ?group= opens it beside the list ----
const picked = ref('')
useRouteState({ group: picked })
const pickedRow = computed(() => (picked.value ? list.value.find((r) => r.name === picked.value) || null : null))
const missing = computed(() => !!picked.value && gs.groups.value != null && !pickedRow.value)
// The queues it reads: the panel says so, and offers to delete it everywhere.
const count = computed(() => (pickedRow.value ? (gs.groups.value || []).filter((g) => g.name === pickedRow.value.name).length : 0))
const side = ref(null)
const stacked = () => window.matchMedia?.('(max-width: 1000px)').matches
async function choose(r, e) {
  if (e?.target?.closest?.('a, button')) return
  if (window.getSelection?.()?.toString()) return
  if (picked.value === r.group) return
  picked.value = r.group
  // Stacked, the group opens under the list: bring it up.
  if (stacked()) { await nextTick(); side.value?.scrollIntoView?.({ block: 'start', behavior: 'smooth' }) }
}
function unpick() { picked.value = '' }

const cols = computed(() => {
  if (pickedRow.value || missing.value) return 'minmax(0, 1fr) minmax(0, 150px) 84px'
  return withReaders.value
    ? 'minmax(0, 1.6fr) 150px 100px 100px 120px 80px minmax(0, 1fr)'
    : 'minmax(0, 1.6fr) 150px 100px 100px 120px minmax(0, 1fr)'
})

// ---- its partitions behind, read while it is open ----
const lagging = shallowRef(null) // null = not read
const lagError = ref('')
async function readLagging() {
  if (!picked.value) return
  const epoch = currentEpoch()
  try {
    const r = await consumersApi.getLagging(0, { quiet: true })
    if (epoch !== currentEpoch()) return
    lagging.value = Array.isArray(r.data) ? r.data : []
    lagError.value = ''
  } catch (e) {
    if (epoch === currentEpoch()) lagError.value = describeApiError(e)
  }
}
watch(picked, (v) => { if (v && lagging.value == null) readLagging() }, { immediate: true })
useAutoRefresh(readLagging)

// ---- actions: one dialog at a time; after one, the page reads everything again ----
const pending = ref(null)
function act(a) {
  if (!a?.row) return
  if (a.kind !== 'deleteAll') { pending.value = a; return }
  const everywhere = groupRows({
    groups: (gs.groups.value || []).filter((g) => g.name === a.row.name),
    queues: qs.queues.value || [],
    ops: os.ops.value,
    series: os.series.value,
    linesFor: props.linesFor,
  })
  pending.value = {
    ...a,
    queues: count.value,
    allMembers: everywhere.reduce((s, r) => s + (r.members || 0), 0),
    allWaiting: everywhere.reduce((s, r) => s + (r.waiting || 0), 0),
  }
}
async function acted({ call, ok }) {
  pending.value = null
  emit('changed')
  await gs.fetchGroups?.({ force: true })
  readLagging()
  // Deleted here: the group is no longer on this queue.
  if (ok && call.method === 'DELETE') unpick()
}
</script>

<template>
  <div class="qg" :class="{ split: pickedRow || missing }">
    <div class="qg-list">
      <p v-if="!rows.length" class="qn-t2" style="padding: 18px 0">No consumer group has read this queue yet.</p>
      <div v-else role="table" aria-label="Consumer groups of this queue" class="qg-table" :style="{ '--cols': cols }">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Group</span><span role="columnheader">State</span><span role="columnheader" class="r">Waiting</span>
          <span role="columnheader" class="r qg-more">Oldest</span><span role="columnheader" class="r qg-more">Partitions behind</span>
          <span v-if="withReaders" role="columnheader" class="r qg-more">Readers</span><span role="columnheader" class="qg-more">Reads from</span>
        </div>
        <div
          v-for="r in rows"
          :key="r.key"
          class="qn-tr click"
          :class="{ on: r.group === picked }"
          role="row"
          tabindex="0"
          :aria-current="r.group === picked ? 'true' : undefined"
          @click="choose(r, $event)"
          @keydown.enter.self="choose(r)"
        >
          <span role="cell" style="display: flex; align-items: center; gap: 8px">
            <span v-if="r.mark" class="qn-mark" :class="r.mark" aria-hidden="true" />
            <span :class="r.mono ? 'qn-mono' : 'qn-t2'" style="overflow: hidden; text-overflow: ellipsis">{{ r.name }}</span>
            <span v-if="r.kafka" class="qn-tag" style="font-size: 11px">Kafka</span>
          </span>
          <span role="cell" :class="r.stateClass" :title="r.stateTitle">{{ r.state }}</span>
          <span role="cell" class="r qn-num">{{ r.waiting }}</span>
          <span role="cell" class="r qn-num qg-more">{{ r.oldest }}</span>
          <span role="cell" class="r qn-num qg-more">{{ r.behind }}</span>
          <span v-if="withReaders" role="cell" class="r qn-num qn-t2 qg-more">{{ r.readers }}</span>
          <span role="cell" class="qn-t2 qg-more">{{ r.from }}</span>
        </div>
      </div>
      <p class="qn-read" style="margin-top: 12px">
        Read from <span class="qn-mono">GET /api/v1/consumer-groups</span>, this queue's rows. Oldest is the age of the oldest message a group has not read.
        <template v-if="rows.length && !pickedRow">A group opens beside the list, with its partitions behind and what you can do. </template>
        <RouterLink :to="{ path: '/queues', query: { view: 'groups' } }" class="qn-link">Every consumer group</RouterLink>
      </p>
    </div>

    <div v-if="pickedRow || missing" ref="side" class="qg-side">
      <section v-if="missing" class="qn-panel" style="font-size: 13.5px">
        <p>No consumer group named <span class="qn-mono">{{ picked }}</span> reads {{ queue }}.</p>
        <p style="margin-top: 12px"><button type="button" class="qn-link" @click="unpick">Close</button></p>
      </section>
      <QgDetail
        v-else
        :row="pickedRow"
        :count="count"
        :lagging="lagging"
        :lag-error="lagError"
        :can-act="canAct"
        @act="act"
        @close="unpick"
      />
    </div>

    <QgAct :act="pending" :where="where" @close="pending = null" @done="acted" />
  </div>
</template>

<style scoped>
.qg { margin-top: 6px; }
.qg-list { min-width: 0; overflow-x: auto; }
.qg-table { min-width: 860px; }
/* a group open: the list keeps the group, its state and what waits; the panel has the rest */
.qg.split { display: grid; grid-template-columns: minmax(0, 5fr) minmax(0, 7fr); gap: 24px 28px; align-items: start; }
.qg.split .qg-table { min-width: 0; }
.qg.split .qg-more { display: none; }
.qg-side { min-width: 0; scroll-margin-top: 12px; }
@media (min-width: 1001px) {
  /* the panel stays in view while the page scrolls */
  .qg-side { position: sticky; top: 72px; max-height: calc(100vh - 96px); max-height: calc(100dvh - 96px); overflow-y: auto; }
}
@media (max-width: 1000px) {
  .qg.split { grid-template-columns: minmax(0, 1fr); }
}
/* a phone: the group, its state and what waits; the open group has the rest */
@media (max-width: 640px) {
  .qg-table { min-width: 0; --cols: minmax(0, 1fr) auto 64px !important; }
  .qg-table .qg-more { display: none; }
}
</style>
