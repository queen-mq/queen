<script setup>
// One group on one queue, beside the list: why it needs you when it does,
// what waits and where it reads from, its partitions furthest behind, and
// what you can do: skip what waits, read again from a time, skip one
// partition, delete it here or everywhere. The list already says how far
// behind it is and whether it drains, so this side does not say it again.
import { computed, ref, shallowRef, watch } from 'vue'
import { acksText } from '@/composables/queueOps'
import { midCut } from '@/composables/dlqWhy'
import QcPopover from '../QcPopover.vue'
import { fmt } from '../useQueuePage'
import { span } from '../useOverview'
import { clock } from '../useWindow'
import { behindRows, keepPlaces, LAGGING_LIMIT } from './groupsView'

const props = defineProps({
  row: { type: Object, required: true },
  count: { type: Number, default: 1 }, // the queues this group reads
  lagging: { type: Array, default: null }, // GET /consumer-groups/lagging?minLagSeconds=0, null = not read
  lagError: { type: String, default: '' },
  canAct: { type: Boolean, default: false },
})
const emit = defineEmits(['act', 'close'])

const r = computed(() => props.row)
const qlink = computed(() => `/queues/${encodeURIComponent(r.value.queue)}`)
const DAY = (ms) => new Date(ms).toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
const plural = (n, one, many) => `${fmt(n)} ${n === 1 ? one : many}`

// ---- why it needs you, when it does, in one sentence ----
const story = computed(() => {
  const x = r.value
  const p = x.presence
  if (x.need?.reason === 'acks') {
    const said = acksText(x.need.hour)
    const shared = x.need.shared ? ` The broker counts acks by queue, and ${x.readers} groups read it: any of them may be the one failing.` : ''
    return { tone: x.tone, text: `${said.charAt(0).toUpperCase()}${said.slice(1)} in the last hour.${shared}`, link: { to: { path: '/dlq', query: { queue: x.queue } }, text: 'Its dead letters' } }
  }
  if (x.need?.reason === 'lag') {
    const line = x.need.sev === 'bad' ? x.lines?.bad : x.lines?.warn
    const which = x.need.sev === 'bad' ? 'second' : 'first'
    return {
      tone: x.tone,
      text: line ? `Behind more than ${span(line)}, the ${which} line of ${x.queue}.` : 'Behind past its queue’s line.',
      link: { to: { path: '/settings', query: { queue: x.queue } }, text: 'Its alert lines' },
    }
  }
  if (x.state === 'mute') return { tone: '', text: x.queueMode ? 'Queue mode has a cursor here, but it has never consumed.' : 'It has a cursor here, but it has never consumed.' }
  if (p?.activity === 'inactive') {
    const seen = p.away != null ? `last seen ${span(p.away)} ago` : 'no member data to date it by'
    return { tone: '', text: `Not running: ${seen}. A group gone past its grace is not judged by its lag.` }
  }
  if (p?.activity === 'idle') {
    const grace = x.lines?.grace
    return { tone: '', text: `No live member right now${p.away != null ? `, last seen ${span(p.away)} ago` : ''}. It is judged by its lag until it has been away ${grace ? span(grace) : 'its grace'}.` }
  }
  if (p?.activity === 'live' && p.members === 0) return { tone: '', text: 'Its client assigns its own partitions.' }
  return null
})

// ---- its facts ----
const readsFrom = computed(() => {
  const x = r.value
  if (x.queueMode) return 'every message, from the oldest kept'
  const s = x.sub || {}
  if (s.mode === 'all') return 'the start'
  if (s.mode === 'timestamp' && Date.parse(s.at)) return `messages from ${DAY(Date.parse(s.at))}`
  if (s.mode === 'new') return Date.parse(s.since) ? `new messages, since ${DAY(Date.parse(s.since))}` : 'new messages'
  return '–'
})
const waitingText = computed(() => {
  const x = r.value
  if (x.waiting == null) return '–'
  if (x.waiting <= 0) return 'nothing'
  if (x.conflates) return `${plural(x.waiting, 'older update', 'older updates')}: it gets only the newest of each partition`
  return plural(x.waiting, 'message', 'messages')
})
const seen = computed(() => {
  const p = r.value.presence
  if (!p) return ''
  if (p.activity === 'live') return 'now'
  const t = Date.parse(p.lastSeenAt)
  if (!t) return '–'
  return `${DAY(t)}${p.away > 0 ? `, ${span(p.away)} ago` : ''}`
})

// ---- its partitions furthest behind: in the place first read, numbers updated ----
const SHOW = 8
const showAll = ref(false)
const placed = shallowRef(null)
watch(() => r.value.key, () => { placed.value = null; showAll.value = false })
watch(
  [() => props.lagging, () => r.value.key],
  () => {
    if (!props.lagging) return
    placed.value = keepPlaces(placed.value, behindRows(props.lagging, { group: r.value.name, queue: r.value.queue }))
  },
  { immediate: true },
)
const parts = computed(() => placed.value?.list || [])
const live = computed(() => parts.value.filter((p) => !p.gone).length)
const shown = computed(() => (showAll.value ? parts.value : parts.value.slice(0, SHOW)))
// The route answers the 1,000 partitions furthest behind across every group:
// a group whose own are past that cut is not "none behind".
const cut = computed(() => (props.lagging?.length || 0) >= LAGGING_LIMIT && (r.value.behind || 0) > live.value)

// ---- what to do ----
const more = ref(false)
function act(kind, extra = {}) { more.value = false; emit('act', { kind, row: r.value, ...extra }) }
</script>

<template>
  <section class="qn-panel cgd" :aria-label="`${r.label} on ${r.queue}`">
    <div class="cgd-head">
      <h2 class="cgd-name" :class="r.queueMode ? 'qn-t2' : 'qn-mono'">{{ r.label }}</h2>
      <span v-if="r.kafka" class="qn-tag cgd-tag" title="Read through the Kafka facade">Kafka</span>
      <span v-if="r.conflates" class="qn-tag cgd-tag" title="A last-value group: it gets only the newest message of each partition">last value</span>
      <div style="flex: 1 1 0" />
      <button type="button" class="qn-btn3 cgd-x" aria-label="Close the group" title="Close" @click="emit('close')">
        <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M2.5 2.5l7 7M9.5 2.5l-7 7" style="fill: none; stroke: currentColor; stroke-width: 1.4; stroke-linecap: round" /></svg>
      </button>
    </div>
    <p class="cgd-on qn-t2">
      on <RouterLink :to="qlink" class="qn-mono cgd-q">{{ r.queue }}</RouterLink><template v-if="count > 1 && !r.queueMode">, one of the {{ fmt(count) }} queues it reads</template>
    </p>

    <p v-if="story" class="cgd-story" :class="story.tone">
      <span v-if="story.tone" class="qn-mark" :class="story.tone" aria-hidden="true" />
      <span class="cgd-storytext">{{ story.text }}<template v-if="story.link">{{ ' ' }}<RouterLink :to="story.link.to" class="qn-link cgd-storylink">{{ story.link.text }}</RouterLink></template></span>
    </p>

    <dl class="qn-dl cgd-facts">
      <dt>Waiting</dt><dd class="qn-num">{{ waitingText }}</dd>
      <dt>Reads from</dt><dd>{{ readsFrom }}</dd>
      <template v-if="r.presence">
        <dt>Live members</dt><dd class="qn-num">{{ r.presence.members == null ? '–' : fmt(r.presence.members) }}</dd>
        <dt>Last seen</dt><dd class="qn-num">{{ seen }}</dd>
      </template>
    </dl>

    <!-- its partitions furthest behind -->
    <div class="cgd-sec">
      <h3 class="qn-h2">Partitions behind</h3>
      <p v-if="lagError && !lagging" class="qn-t2 cgd-note">The broker did not answer: {{ lagError }}</p>
      <p v-else-if="!lagging" class="qn-t2 cgd-note">Reading its partitions…</p>
      <p v-else-if="!parts.length && cut" class="qn-t2 cgd-note">{{ plural(r.behind, 'partition is', 'partitions are') }} behind, but none among the {{ fmt(LAGGING_LIMIT) }} furthest behind across every group, which is all the broker lists.</p>
      <p v-else-if="!parts.length" class="qn-t2 cgd-note">No partition of <span class="qn-mono">{{ r.queue }}</span> is behind for it.</p>
      <template v-else>
        <div class="cgd-parts" role="table" :aria-label="`Partitions of ${r.queue} behind for ${r.label}`">
          <div class="qn-tr head" role="row">
            <span role="columnheader">Partition</span>
            <span role="columnheader" class="r">Waiting</span>
            <span role="columnheader" class="r">Oldest</span>
            <span role="columnheader" class="cgd-wide">Worker</span>
            <span v-if="canAct" role="columnheader"><span class="qn-vh">Action</span></span>
          </div>
          <div v-for="p in shown" :key="p.key" class="qn-tr" :class="{ 'cgd-gone': p.gone }" role="row">
            <span role="cell" class="qn-mono cgd-p" :title="p.partition">{{ midCut(p.partition, 40) }}</span>
            <span role="cell" class="r qn-num">{{ p.gone ? '–' : fmt(p.waiting) }}</span>
            <span role="cell" class="r qn-num" :title="p.oldestAt && !p.gone ? `Oldest unread pushed at ${clock(Date.parse(p.oldestAt), true)}` : undefined">{{ p.gone ? 'caught up' : span(p.lag) }}</span>
            <span role="cell" class="qn-mono qn-t2 cgd-w cgd-wide" :title="p.worker || undefined">{{ p.worker ? midCut(p.worker, 14) : '–' }}</span>
            <span v-if="canAct" role="cell" class="r">
              <button v-if="!p.gone" type="button" class="qn-btn3 cgd-skip" :aria-label="`Skip partition ${p.partition}`" @click="act('partition', { partition: p.partition, waiting: p.waiting })">Skip…</button>
            </span>
          </div>
        </div>
        <p v-if="parts.length > SHOW" class="cgd-more qn-num qn-t3">
          {{ showAll ? fmt(parts.length) : `${fmt(SHOW)} of ${fmt(parts.length)}` }}
          <button type="button" class="qn-link" @click="showAll = !showAll">{{ showAll ? 'Show fewer' : 'Show all' }}</button>
        </p>
      </template>
      <p class="qn-read cgd-read">Read from <span class="qn-mono">GET /api/v1/consumer-groups/lagging?minLagSeconds=0</span>, which lists the {{ fmt(LAGGING_LIMIT) }} partitions furthest behind across every group. Furthest first when first read; a new read updates them in place.</p>
    </div>

    <!-- what to do -->
    <div class="cgd-sec">
      <div v-if="canAct" class="cgd-acts">
        <button type="button" class="qn-btn2" :disabled="!(r.waiting > 0)" @click="act('skip')">Skip what waits…</button>
        <button type="button" class="qn-btn2" @click="act('seek')">Seek to a time…</button>
        <!-- one queue: straight to its dialog; several: here, or everywhere -->
        <QcPopover v-if="count > 1 && !r.queueMode" v-model:open="more" side="bottom-start" :width="260" label="Delete">
          <template #trigger="{ toggle }">
            <button type="button" class="qn-btn2 danger" aria-haspopup="dialog" :aria-expanded="more" @click="toggle">Delete…</button>
          </template>
          <template #default>
            <button type="button" class="qn-opt red" @click="act('delete')">On <span class="qn-mono cgd-optq">{{ r.queue }}</span></button>
            <button type="button" class="qn-opt red" @click="act('deleteAll')">Everywhere, on its {{ fmt(count) }} queues</button>
          </template>
        </QcPopover>
        <button v-else type="button" class="qn-btn2 danger" aria-haspopup="dialog" @click="act('delete')">Delete…</button>
      </div>
      <p v-else class="qn-t2">Moving a group or deleting it needs the admin role on this cluster.</p>
      <p class="cgd-links">
        <RouterLink :to="{ path: '/queues', query: { view: 'workers', queue: r.queue } }" class="qn-link">Its workers</RouterLink>
      </p>
    </div>
  </section>
</template>

<style scoped>
.cgd { min-width: 0; }
.cgd-head { display: flex; align-items: center; gap: 8px; min-width: 0; }
.cgd-name { font-size: 16px; font-weight: 500; min-width: 0; overflow-wrap: anywhere; }
.cgd-tag { font-size: 11px; }
.cgd-x { width: 28px; height: 28px; padding: 0; margin-right: -6px; }
.cgd-on { margin-top: 4px; overflow-wrap: anywhere; }
.cgd-q { color: var(--text); font-weight: 500; }
.cgd-story { display: flex; align-items: baseline; gap: 8px; margin-top: 14px; font-size: 13px; }
.cgd-story .qn-mark { align-self: center; }
.cgd-story.amber .cgd-storytext, .cgd-story.red .cgd-storytext { color: var(--text); }
.cgd-storylink { color: var(--text2); white-space: nowrap; }
.cgd-facts { margin-top: 16px; }
.cgd-sec { margin-top: 20px; padding-top: 16px; border-top: 1px solid var(--line2); }
:global(.qn.tone .cgd .cgd-sec) { border-top-color: var(--bg3); }
.cgd-note { margin-top: 10px; }
.cgd-parts { margin-top: 8px; --cols: minmax(0, 1fr) 72px 76px minmax(0, 120px) 64px; }
.cgd-p { font-size: 12px; }
.cgd-w { font-size: 12px; }
.cgd-skip { height: 26px; font-size: 12px; margin-right: -8px; }
.cgd-gone > * { color: var(--text3); }
.cgd-more { display: flex; align-items: center; gap: 10px; margin-top: 8px; font-size: 12px; }
.cgd-read { margin-top: 10px; overflow-wrap: anywhere; }
.cgd-acts { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.cgd-optq { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.cgd-links { margin-top: 14px; font-size: 12.5px; }
.cgd-links a { color: var(--text2); }
/* a phone keeps the partition and what waits; the worker is in its title */
@media (max-width: 640px) {
  .cgd.qn-panel { padding: 16px 14px; }
  .cgd-parts { --cols: minmax(0, 1fr) 56px 64px 56px; }
  .cgd-parts .cgd-wide { display: none; }
}
</style>
