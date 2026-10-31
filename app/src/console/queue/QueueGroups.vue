<script setup>
// The groups reading this queue: how far behind each is, and where it reads
// from. State words, marks and the drain estimate are the Consumer groups
// view's (console/queues/groupsView.js), so the two never disagree. The order
// is fixed at first sight, worst first; a group seen later joins at the end.
import { computed } from 'vue'
import { useQueuesStore } from '@/stores/queuesStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { groupRows, statusOf, stateWords, stateTitle, QUIET_STATES } from '../queues/groupsView'
import { span } from '../words.js'
import { fmt, QUEUE_MODE } from '../useQueuePage'

const props = defineProps({
  groups: { type: Array, default: () => [] },
  linesFor: { type: Function, default: null },
})
const qs = useQueuesStore()
const os = useQueueOpsStore()

function readsFrom(g) {
  const m = g.subscriptionMode
  if (m === 'all') return 'the start'
  if (m === 'new') return 'new messages'
  if (m && g.subscriptionTimestamp) return 'from ' + new Date(g.subscriptionTimestamp).toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
  return g.name === QUEUE_MODE ? 'every message' : '–'
}

const place = new Map()
const rows = computed(() => {
  const raw = new Map(props.groups.map((g) => [g.name, g]))
  const list = groupRows({ groups: props.groups, queues: qs.queues.value || [], ops: os.ops.value, series: os.series.value, linesFor: props.linesFor })
  // first sight: worst first, then by name; afterwards nobody moves
  const fresh = list.filter((r) => !place.has(r.key))
    .sort((a, b) => (b.lag || 0) - (a.lag || 0) || String(a.name).localeCompare(String(b.name)))
  for (const r of fresh) place.set(r.key, place.size)
  return list.sort((a, b) => place.get(a.key) - place.get(b.key)).map((r) => {
    const g = raw.get(r.name) || {}
    const st = statusOf(r)
    return {
      key: r.key,
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
const cols = computed(() => (withReaders.value
  ? 'minmax(0, 1.6fr) 150px 100px 100px 120px 80px minmax(0, 1fr)'
  : 'minmax(0, 1.6fr) 150px 100px 100px 120px minmax(0, 1fr)'))
</script>

<template>
  <div class="qg">
    <p v-if="!rows.length" class="qn-t2" style="padding: 18px 0">No consumer group has read this queue yet.</p>
    <div v-else role="table" aria-label="Consumer groups of this queue" :style="{ '--cols': cols }">
      <div class="qn-tr head" role="row">
        <span role="columnheader">Group</span><span role="columnheader">State</span><span role="columnheader" class="r">Waiting</span>
        <span role="columnheader" class="r">Oldest</span><span role="columnheader" class="r">Partitions behind</span>
        <span v-if="withReaders" role="columnheader" class="r">Readers</span><span role="columnheader">Reads from</span>
      </div>
      <div v-for="r in rows" :key="r.key" class="qn-tr" role="row">
        <span role="cell" style="display: flex; align-items: center; gap: 8px">
          <span v-if="r.mark" class="qn-mark" :class="r.mark" aria-hidden="true" />
          <span :class="r.mono ? 'qn-mono' : 'qn-t2'" style="overflow: hidden; text-overflow: ellipsis">{{ r.name }}</span>
          <span v-if="r.kafka" class="qn-tag" style="font-size: 11px">Kafka</span>
        </span>
        <span role="cell" :class="r.stateClass" :title="r.stateTitle">{{ r.state }}</span>
        <span role="cell" class="r qn-num">{{ r.waiting }}</span>
        <span role="cell" class="r qn-num">{{ r.oldest }}</span>
        <span role="cell" class="r qn-num">{{ r.behind }}</span>
        <span v-if="withReaders" role="cell" class="r qn-num qn-t2">{{ r.readers }}</span>
        <span role="cell" class="qn-t2">{{ r.from }}</span>
      </div>
    </div>
    <p class="qn-read" style="margin-top: 12px">
      Read from <span class="qn-mono">GET /api/v1/consumer-groups</span>, this queue's rows. Oldest is the age of the oldest message a group has not read;
      Clears in is this queue's last five minutes when one group reads it alone.
      <RouterLink :to="{ path: '/queues', query: { view: 'groups' } }" class="qn-link">Every consumer group</RouterLink>
    </p>
  </div>
</template>

<style scoped>
.qg { margin-top: 6px; overflow-x: auto; }
.qg [role='table'] { min-width: 860px; }
</style>
