<script>
// Kept for the session (module scope): back from a queue, the namespaces are
// open as you left them, the order a header click froze still holds, and the
// drains measured from this view's reads are still known.
import { reactive, ref, shallowRef } from 'vue'
import { onClusterChange } from '@/stores/identity'

const opened = reactive(new Map()) // ns -> open: the first default, or your click
const sort = ref(null) // { key, dir, at } | null (by group)
const rank = shallowRef(null) // rowKey -> place, frozen at the click
const samples = new Map() // rowKey -> [[ms, waiting]]: this view's reads, for a drain
onClusterChange(() => { opened.clear(); sort.value = null; rank.value = null; samples.clear() })
</script>

<script setup>
// Every consumer group, in the new console: one line saying which need you,
// a filter and a switch, then each namespace's groups. A row is a group on
// one queue: how far behind it is, whether what waits drains, and how many
// partitions are behind. A row opens the group on the right (below on a
// phone) with its partitions furthest behind and what you can do; with none
// open, the right side holds the partitions behind more than an age, across
// every group. Nothing re-sorts by itself: numbers update in place.
//
// Reads, every 30 s (the app's ticker):
//   stores: GET /api/v1/consumer-groups, /resources/queues,
//           /analytics/workload?groupBy=queue, the alert lines
//   GET /api/v1/consumer-groups/lagging?minLagSeconds=0  the partitions behind
import { computed, nextTick, onScopeDispose, watch } from 'vue'
import { consumers as consumersApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { acksText } from '@/composables/queueOps'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { currentEpoch, useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import QcSeg from '../QcSeg.vue'
import { fmt } from '../useQueuePage'
import { span } from '../useOverview'
import { clock } from '../useWindow'
import {
  AGES, addSamples, groupRows, groupsVerdict, hasNews, namespaceGroups, nextSort, pickedRow, SEGMENTS, SEGMENT_VALUES,
  segmentCounts, snapshotOrder, statusOf, stateWords, stateTitle, QUIET_STATES,
} from './groupsView'
import QgDetail from './QgDetail.vue'
import QgBehind from './QgBehind.vue'
import QgAct from './QgAct.vue'

defineProps({ where: { type: String, default: '' } }) // the cluster, for the dialogs

const { can } = useIdentity()
const canAct = computed(() => can('queueAdmin'))
const qs = useQueuesStore()
const gs = useGroupsStore()
const os = useQueueOpsStore()
const ss = useSettingsStore()
const linesFor = (q) => (typeof ss.linesFor === 'function' ? ss.linesFor(q) : undefined)

// ---- the address: the filter, the switch, the pick and the age (?gq= ?gshow= ?group= ?gqueue= ?gbehind=) ----
const find = ref('')
const show = ref('all')
const group = ref('')
const gqueue = ref('')
const behind = ref(AGES[0])
useRouteState({ gq: find, gshow: show, group, gqueue, gbehind: behind })
// An unknown ?gshow= reads as All, and an unknown age as the first: the switches say so.
watch(show, (v) => { if (!SEGMENT_VALUES.includes(v)) show.value = 'all' }, { immediate: true })
watch(behind, (v) => { if (!AGES.includes(v)) behind.value = AGES[0] }, { immediate: true })
const seg = computed(() => (SEGMENT_VALUES.includes(show.value) ? show.value : 'all'))
const narrowed = computed(() => !!find.value.trim() || seg.value !== 'all')

// ---- reads ----
const lagging = shallowRef(null) // the partitions behind, null = not read
const lagError = ref('')
onScopeDispose(onClusterChange(() => { lagging.value = null; lagError.value = '' }))
async function readLagging() {
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
function refresh(force = false) {
  const fresh = force ? { force: true } : { ttlMs: 25_000 }
  return Promise.allSettled([
    qs.fetchQueues(fresh),
    gs.fetchGroups(fresh),
    os.fetchOps(),
    ss.fetchSettings?.(),
    readLagging(),
  ])
}
useAutoRefresh(() => refresh())
refresh()

// Each read of the listing is a sample of every group's waiting count: how a
// group on a shared queue drains is measured from them.
const sampled = ref(0)
watch(() => gs.lastFetched.value, (t) => {
  if (!t || !gs.groups.value) return
  addSamples(samples, gs.groups.value, t)
  sampled.value = t
}, { immediate: true })

const rows = computed(() => groupRows({
  groups: gs.groups.value,
  queues: qs.queues.value,
  ops: os.ops.value,
  series: os.series.value,
  samples,
  linesFor,
  now: sampled.value || Date.now(),
}))
const loaded = computed(() => gs.groups.value != null)
// What needs you is known once the hour's acks were asked for too.
const judged = computed(() => loaded.value && os.lastFetched.value > 0)
// A row's namespace is its queue's: known once the queue listing is read.
const placed = computed(() => qs.lastFetched.value > 0 || (qs.queues.value || []).length > 0)
const listError = computed(() => (gs.error.value && !loaded.value ? describeApiError(gs.error.value) : ''))
const stale = computed(() => (gs.error.value && loaded.value ? { error: describeApiError(gs.error.value), at: gs.lastFetched.value } : null))

// ---- the line on top ----
const verdict = computed(() => (loaded.value && !judged.value
  ? { kind: 'acks' }
  : groupsVerdict({ rows: rows.value, loaded: loaded.value, failed: !!listError.value })))

// ---- the switch ----
const counts = computed(() => segmentCounts(rows.value))
const segments = computed(() => SEGMENTS.map((s) => ({
  ...s,
  count: !loaded.value || (s.value === 'needs' && !judged.value) ? null : counts.value[s.value].toLocaleString('en-US'),
})))

// ---- namespaces: open by default when they hold a group that needs you; after that, only a click ----
const nsList = computed(() => namespaceGroups(rows.value, { find: find.value, seg: seg.value, rank: rank.value }))
watch([nsList, judged, placed], () => {
  if (!judged.value || !placed.value || narrowed.value) return
  for (const n of nsList.value) if (!opened.has(n.ns)) opened.set(n.ns, hasNews(n))
}, { immediate: true })
// While a filter or the switch narrows the list, every namespace left is open,
// and closing one lasts until the filter changes.
const fopen = reactive(new Map())
watch([find, seg], () => fopen.clear())
const openNs = (ns) => (narrowed.value ? (fopen.has(ns) ? fopen.get(ns) : true) : (opened.has(ns) ? opened.get(ns) : false))
function toggle(ns) { (narrowed.value ? fopen : opened).set(ns, !openNs(ns)) }

// ---- order: a header click sorts inside each namespace, frozen until the next one ----
function sortBy(key) {
  const next = nextSort(sort.value, key)
  sort.value = next ? { ...next, at: Date.now() } : null
  rank.value = next ? snapshotOrder(rows.value, next) : null
}
const arrow = (key) => (sort.value?.key === key ? (sort.value.dir === 'desc' ? '↓' : '↑') : '')
const ariaSort = (key) => (sort.value?.key === key ? (sort.value.dir === 'desc' ? 'descending' : 'ascending') : 'none')
const sortLine = computed(() => {
  const s = sort.value
  if (!s) return 'By group in each namespace. Rows never move while you look.'
  return `Sorted by oldest unread at ${clock(s.at)}, ${s.dir === 'desc' ? 'oldest' : 'newest'} first in each namespace. Click again to change.`
})

// ---- the pick: ?group= opens a group where it is worst, ?gqueue= on one of its queues ----
const picked = computed(() => pickedRow(rows.value, group.value, gqueue.value))
const pickedCount = computed(() => (picked.value ? rows.value.filter((r) => r.name === picked.value.name).length : 0))
const missing = computed(() => !!group.value && loaded.value && !picked.value)
const side = ref(null)
const listEl = ref(null)
let byClick = false
function pick(name, queue = '') { group.value = name; gqueue.value = queue }
function unpick() { group.value = ''; gqueue.value = '' }
function choose(r, e) {
  if (e?.target?.closest?.('a, button')) return
  if (window.getSelection?.()?.toString()) return
  if (picked.value?.key === r.key) return
  byClick = true
  pick(r.name, r.queue)
}
const domKey = (k) => encodeURIComponent(k)
/** Scroll the list (only the list) so the picked row sits in its middle, unless it is in view. */
function revealRow(k) {
  const list = listEl.value
  const el = list?.querySelector(`[data-k="${CSS.escape(domKey(k))}"]`)
  if (!el) return
  const lr = list.getBoundingClientRect()
  const er = el.getBoundingClientRect()
  // Until the page scrolls the list is not stuck yet, and its box runs past the window.
  const bottom = Math.min(lr.bottom, window.innerHeight)
  if (er.top >= lr.top + 48 && er.bottom <= bottom) return
  list.scrollTop += er.top - lr.top - (bottom - lr.top) / 2 + er.height / 2
  // Near the list's end it cannot be centred: the page moves the least it must.
  el.scrollIntoView({ block: 'nearest' })
}
// A group opened from the address, the line on top or the partitions shows
// its row: its namespace opens and the list brings it into view, once the
// namespaces' own default is settled. Stacked (a phone, a narrow window), the
// group opens under the list: that comes up.
watch(() => (picked.value && placed.value && judged.value ? picked.value.key : ''), (k) => {
  const click = byClick
  byClick = false
  if (!k) return
  if (!narrowed.value && !openNs(picked.value.ns)) opened.set(picked.value.ns, true)
  nextTick(() => {
    if (window.matchMedia?.('(max-width: 1000px)').matches) side.value?.scrollIntoView?.({ block: 'start', behavior: 'smooth' })
    else if (!click) revealRow(k)
  })
}, { immediate: true })

// ---- cells ----
function cells(r) {
  const st = statusOf(r)
  return {
    key: r.key,
    row: r,
    label: r.label,
    queueMode: r.queueMode,
    kafka: r.kafka,
    conflates: r.conflates,
    mark: r.tone,
    queue: r.queue,
    to: `/queues/${encodeURIComponent(r.queue)}`,
    behind: r.behind > 0 ? `${fmt(r.behind)} of ${fmt(r.members)} partitions behind` : '',
    oldest: r.lag > 0 ? span(r.lag) : '–',
    oldestDim: !(r.lag > 0) || r.sev === 'mute',
    state: stateWords(st),
    stateClass: st.tone || (QUIET_STATES.has(st.kind) ? 'qn-t3' : 'qn-t2'),
    stateTitle: stateTitle(r, st),
  }
}
// A closed namespace says what it hides.
function summary(n) {
  const toneOf = (reason) => (n.rows.some((r) => r.need?.reason === reason && r.need.sev === 'bad') ? 'red' : 'amber')
  const out = []
  if (n.counts.failing) out.push({ text: `${n.counts.failing} failing`, tone: toneOf('acks') })
  if (n.counts.behind) out.push({ text: `${n.counts.behind} behind`, tone: toneOf('lag') })
  return out
}
const table = computed(() => nsList.value.map((n) => {
  const open = openNs(n.ns)
  const noun = n.total === 1 ? 'group' : 'groups'
  return {
    key: n.ns || '\u0000none',
    ns: n.ns,
    open,
    count: narrowed.value ? `${fmt(n.rows.length)} of ${fmt(n.total)} ${noun}` : `${fmt(n.total)} ${noun}`,
    summary: open ? [] : summary(n),
    rows: open ? n.rows.map(cells) : [],
  }
}))
const emptyText = computed(() => {
  const f = find.value.trim()
  if (f) return `Nothing matches “${f}”${seg.value !== 'all' ? ' here' : ''}.`
  return seg.value === 'needs' ? 'No group needs you.' : 'Nothing to show.'
})
function clearFilter() { find.value = ''; show.value = 'all' }

// ---- the partitions panel finds the row a partition belongs to ----
const rowFor = (name, queue) => rows.value.find((r) => r.name === name && r.queue === queue) || null

// ---- actions: one dialog at a time; after one, everything is read again ----
const pending = ref(null)
function act(a) {
  if (!a?.row) return
  pending.value = a.kind === 'deleteAll'
    ? {
      ...a,
      queues: pickedCount.value,
      allMembers: rows.value.filter((r) => r.name === a.row.name).reduce((s, r) => s + (r.members || 0), 0),
      allWaiting: rows.value.filter((r) => r.name === a.row.name).reduce((s, r) => s + (r.waiting || 0), 0),
    }
    : a
}
async function acted({ call, ok }) {
  pending.value = null
  await refresh(true)
  if (!ok || call.method !== 'DELETE') return
  // The deleted rows are gone: the group opens where it still reads, or closes.
  if (!rows.value.some((r) => r.name === call.group)) unpick()
  else if (call.queue && gqueue.value === call.queue) gqueue.value = ''
}
</script>

<template>
  <div class="cg">
    <!-- which groups need you, in words -->
    <p class="cg-verdict">
      <template v-if="verdict.kind === 'issues'">
        <span class="qn-mark" :class="verdict.tone" aria-hidden="true" />
        <span class="cg-vtext">
          <button type="button" class="cg-vname" :class="{ 'qn-mono': verdict.names[0].mono }" @click="pick(verdict.names[0].name)">{{ verdict.names[0].label }}</button><template v-if="verdict.names[0].queues > 1"> on {{ verdict.names[0].queues }} queues</template>
          <template v-if="verdict.names[1]">{{ verdict.more ? ', ' : ' and ' }}<button type="button" class="cg-vname" :class="{ 'qn-mono': verdict.names[1].mono }" @click="pick(verdict.names[1].name)">{{ verdict.names[1].label }}</button></template>
          <template v-if="verdict.more"> and {{ verdict.more }} more</template>
          {{ verdict.count === 1 ? 'needs' : 'need' }} you.
          <span v-if="!verdict.only" class="qn-t2">Every other group keeps up.</span>
        </span>
      </template>
      <span v-else-if="verdict.kind === 'fine'">Every group keeps up.</span>
      <span v-else-if="verdict.kind === 'none'" class="qn-t2">No consumer group has read a queue yet. One appears with the first pop that names it, and queue mode with the first that names none.</span>
      <span v-else-if="verdict.kind === 'failed'" class="cg-vtext"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ listError }}</span>
      <span v-else-if="verdict.kind === 'acks'" class="qn-t2">Reading the last hour’s acks…</span>
      <span v-else class="qn-t2">Reading the consumer groups…</span>
    </p>

    <template v-if="loaded && rows.length">
      <div class="cg-tools">
        <label class="qn-field cg-find">
          <svg width="14" height="14" viewBox="0 0 18 18" aria-hidden="true" style="fill: none; stroke: currentColor; stroke-width: 1.4"><circle cx="7.5" cy="7.5" r="5" /><line x1="11.3" y1="11.3" x2="15.5" y2="15.5" /></svg>
          <span class="qn-vh">Filter consumer groups</span>
          <input v-model="find" type="text" placeholder="Filter by group, queue or namespace" spellcheck="false" autocomplete="off" @keydown.esc="find = ''" />
        </label>
        <QcSeg v-model="show" :options="segments" label="Show" />
        <span class="qn-t3 cg-sortline">{{ sortLine }}</span>
      </div>
      <p v-if="stale" class="qn-t2 cg-note"><span class="qn-mark amber" aria-hidden="true" />The last read failed ({{ stale.error }}); this is the list from {{ clock(stale.at) }}.</p>

      <div class="cg-split">
        <!-- the list: a click moves the highlight and changes the right side, never the list -->
        <section ref="listEl" class="cg-list" aria-label="Consumer groups">
          <p v-if="!table.length" class="qn-t2 cg-empty">{{ emptyText }} <button type="button" class="qn-link" @click="clearFilter">Show every group</button></p>
          <div v-else class="cg-table" role="table" aria-label="Consumer groups by namespace">
            <div role="rowgroup" class="cg-headgroup">
              <div class="qn-tr head cg-head" role="row">
                <span role="columnheader" class="cg-h1">Group, and the queue it reads</span>
                <span role="columnheader" class="r cg-sorthead" :aria-sort="ariaSort('oldest')">
                  <button type="button" class="cg-sort" :class="{ on: sort && sort.key === 'oldest' }" @click="sortBy('oldest')"><span class="cg-arrow" aria-hidden="true">{{ arrow('oldest') }}</span>Oldest unread</button>
                </span>
              </div>
            </div>
            <div v-for="t in table" :key="t.key" role="rowgroup">
              <div class="qn-tr cg-nsrow" role="row">
                <span role="rowheader" class="cg-nscell">
                  <button type="button" class="cg-ns" :aria-expanded="t.open" @click="toggle(t.ns)">
                    <svg class="cg-chev" :class="{ open: t.open }" width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M4.5 2.5 8 6l-3.5 3.5" /></svg>
                    <span :class="t.ns ? 'qn-mono cg-nsname' : 'cg-nsname cg-none'">{{ t.ns || 'No namespace' }}</span>
                    <span class="qn-t3 qn-num">{{ t.count }}</span>
                    <template v-if="!t.open">
                      <span v-if="!t.summary.length" class="qn-t3">· none needs you</span>
                      <span v-for="s in t.summary" :key="s.text" class="cg-sum qn-num" :class="s.tone"><span class="qn-mark" :class="s.tone" aria-hidden="true" />{{ s.text }}</span>
                    </template>
                  </button>
                </span>
              </div>
              <div
                v-for="r in t.rows"
                :key="r.key"
                class="qn-tr click cg-row"
                :class="{ on: picked && r.key === picked.key }"
                role="row"
                tabindex="0"
                :data-k="domKey(r.key)"
                :aria-current="picked && r.key === picked.key ? 'true' : undefined"
                @click="choose(r.row, $event)"
                @keydown.enter.self="choose(r.row)"
              >
                <span role="rowheader" class="cg-c1">
                  <span class="cg-l1">
                    <span class="cg-markslot" aria-hidden="true"><span v-if="r.mark" class="qn-mark" :class="r.mark" /></span>
                    <span class="cg-name" :class="[r.queueMode ? 'qn-t2' : 'qn-mono', { strong: r.mark }]" :title="r.label">{{ r.label }}</span>
                    <span v-if="r.kafka" class="qn-tag cg-tag" title="Read through the Kafka facade">Kafka</span>
                    <span v-if="r.conflates" class="qn-tag cg-tag" title="A last-value group: it gets only the newest message of each partition">last value</span>
                  </span>
                  <span class="cg-l2">
                    <RouterLink :to="r.to" class="qn-mono cg-q" :title="`Open ${r.queue}`">{{ r.queue }}</RouterLink>
                    <span v-if="r.behind" class="qn-t3 qn-num cg-behind">· {{ r.behind }}</span>
                  </span>
                </span>
                <span role="cell" class="cg-c2">
                  <span class="qn-num cg-old" :class="{ 'qn-t3': r.oldestDim }">{{ r.oldest }}</span>
                  <span class="cg-st" :class="r.stateClass" :title="r.stateTitle || undefined">{{ r.state }}</span>
                </span>
              </div>
            </div>
          </div>
          <p class="qn-read cg-read">
            Read from <span class="qn-mono">GET /api/v1/consumer-groups</span>, <span class="qn-mono">GET /api/v1/resources/queues</span>
            and <span class="qn-mono">GET /api/v1/analytics/workload?groupBy=queue</span>, the last hour. Oldest unread is the age of the
            oldest message a group has not read. Clears in is its queue’s last five minutes when it reads the queue alone, else its
            own waiting count over this view’s reads.
          </p>
        </section>

        <!-- the right side: the picked group, or the partitions behind across every group -->
        <div ref="side" class="cg-side">
          <section v-if="missing" class="qn-panel cg-missing">
            <p>There is no consumer group named <span class="qn-mono">{{ group }}</span> on {{ where }}.</p>
            <p style="margin-top: 12px"><button type="button" class="qn-link" @click="unpick">See the partitions behind across every group</button></p>
          </section>
          <QgDetail
            v-else-if="picked"
            :row="picked"
            :count="pickedCount"
            :lagging="lagging"
            :lag-error="lagError"
            :can-act="canAct"
            @act="act"
            @close="unpick"
          />
          <QgBehind
            v-else
            v-model:min="behind"
            :lagging="lagging"
            :lag-error="lagError"
            :row-for="rowFor"
            :can-act="canAct"
            @pick="pick"
            @act="act"
          />
        </div>
      </div>
    </template>

    <QgAct :act="pending" :where="where" @close="pending = null" @done="acted" />
  </div>
</template>

<style scoped>
.cg { margin-top: 18px; }
.cg-verdict { display: flex; align-items: flex-start; gap: 10px; font-size: 15px; }
/* the mark sits on the first line, however the sentence wraps */
.cg-verdict > .qn-mark { margin-top: 7px; }
.cg-vtext { flex: 1 1 auto; min-width: 0; overflow-wrap: anywhere; }
.cg-vname { font-weight: 500; }
.cg-vname:hover { text-decoration: underline; text-underline-offset: 3px; }
.cg-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px; margin-top: 22px; }
.cg-find { width: 320px; max-width: 100%; }
.cg-sortline { font-size: 12px; margin-left: auto; }
.cg-note { display: flex; align-items: center; gap: 8px; margin-top: 14px; }
.cg-empty { padding: 14px 10px 4px; }

/* the list and the right side; stacked under 1000px */
.cg-split { display: grid; grid-template-columns: minmax(380px, 5fr) minmax(0, 7fr); gap: 28px 32px; align-items: start; margin-top: 16px; }
.cg-list { min-width: 0; }
.cg-side { min-width: 0; scroll-margin-top: 12px; }
@media (min-width: 1001px) {
  /* the list stays in view while the right side scrolls */
  .cg-list { position: sticky; top: 72px; max-height: calc(100vh - 96px); overflow-y: auto; overscroll-behavior: contain; }
  .cg-headgroup { position: sticky; top: 0; z-index: 1; background: var(--bg); }
}

.cg-head { --cols: minmax(0, 1fr) auto; padding: 0 10px; }
.cg-h1 { padding-left: 18px; }
/* a flex cell: an inline button whose first item is the (empty) arrow would sit on its middle, not on the text */
.cg-sorthead { display: flex; align-items: center; justify-content: flex-end; }
.cg-sort { display: inline-flex; align-items: center; gap: 4px; color: var(--text3); font-size: 12px; }
.cg-sort:hover, .cg-sort.on { color: var(--text); }
.cg-arrow { display: inline-block; width: 10px; text-align: center; }

/* a namespace: one row that opens and closes it */
.qn-tr.cg-nsrow { --cols: minmax(0, 1fr); min-height: 40px; border-bottom-color: var(--line); }
.cg-ns { display: flex; align-items: center; gap: 8px; width: 100%; min-height: 40px; padding: 0 8px 0 4px; text-align: left; color: var(--text3); overflow: hidden; white-space: nowrap; }
.cg-ns:hover .cg-nsname { text-decoration: underline; text-underline-offset: 3px; }
.cg-chev { flex: 0 0 auto; width: 12px; height: 12px; margin-right: -2px; fill: none; stroke: var(--text3); stroke-width: 1.4; stroke-linecap: round; stroke-linejoin: round; transition: transform .12s ease; }
.cg-chev.open { transform: rotate(90deg); }
.cg-nsname { color: var(--text); font-weight: 500; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.cg-none { color: var(--text2); }
.cg-sum { display: inline-flex; align-items: center; gap: 6px; }
.cg-sum + .cg-sum::before { content: '·'; color: var(--text3); margin-right: 2px; }

/* a group on a queue: two lines, the name and its queue, then how far behind and whether it drains */
.qn-tr.cg-row { --cols: minmax(0, 1fr) auto; align-items: start; gap: 0 16px; padding: 9px 10px; }
.qn-tr.cg-row > * { overflow: visible; white-space: normal; }
.cg-c1 { display: flex; flex-direction: column; gap: 3px; min-width: 0; }
.cg-l1, .cg-l2 { display: flex; align-items: center; gap: 8px; min-width: 0; white-space: nowrap; }
.cg-l2 { padding-left: 18px; font-size: 12px; }
.cg-markslot { flex: 0 0 8px; display: flex; align-items: center; justify-content: center; }
.cg-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.cg-name.strong { font-weight: 500; }
.cg-tag { flex: none; font-size: 11px; }
.cg-q { flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; color: var(--text3); }
.cg-q:hover { color: var(--text); }
.cg-behind { flex: 0 10 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.cg-c2 { display: flex; flex-direction: column; align-items: flex-end; gap: 3px; text-align: right; }
.cg-old { white-space: nowrap; }
.cg-st { font-size: 12px; white-space: nowrap; }
.cg-row.on .cg-q { color: var(--text2); }
.cg-read { margin-top: 12px; padding: 0 10px; overflow-wrap: anywhere; }
.cg-missing { font-size: 13.5px; }

/* tone: no hairlines, so a namespace is a filled row */
:global(.qn.tone .cg-table .cg-nsrow) { background: var(--bg2); border-radius: var(--r-ctl); margin-top: 6px; }
:global(.qn.tone .cg-table .cg-row) { border-radius: var(--r-ctl); }
@media (prefers-reduced-motion: reduce) { .cg-chev { transition: none; } }
@media (max-width: 1000px) {
  .cg-split { grid-template-columns: minmax(0, 1fr); }
}
@media (max-width: 640px) {
  .cg-find { width: 100%; }
  .cg-sortline { flex-basis: 100%; margin-left: 0; }
  .qn-tr.cg-row { padding: 9px 4px; }
  .cg-head { padding: 0 4px; }
}
</style>
