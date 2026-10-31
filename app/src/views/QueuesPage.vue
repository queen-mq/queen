<script setup>
// Every queue, in the new console: one line saying whether any needs you, a
// filter and a few segments, then each namespace's queues with what waits, how
// old the oldest is, the last hour's traffic, dead letters, partitions and
// groups. A row opens the queue. Nothing re-sorts by itself: the numbers
// update in place. New queue… shows the /configure call before it runs.
import { computed, nextTick, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useIdentity } from '@/stores/identity'
import ConsoleShell from '@/console/ConsoleShell.vue'
import QcSeg from '@/console/QcSeg.vue'
import QcSpark from '@/console/QcSpark.vue'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { useQueuesPage } from '@/console/useQueuesPage'
import { fmt } from '@/console/useQueuePage'
import { rate, span } from '@/console/useOverview'
import { clock } from '@/console/useWindow'
import { acksText } from '@/composables/queueOps'
import { HOUR_S, toneOf } from '@/console/queues/queuesView'
import QueuesNew from '@/console/queues/QueuesNew.vue'
import QueuesEphemeral from '@/console/queues/QueuesEphemeral.vue'
import QueuesGroups from '@/console/queues/QueuesGroups.vue'
import QueuesWorkers from '@/console/queues/QueuesWorkers.vue'
import QueuesMessages from '@/console/queues/QueuesMessages.vue'

const router = useRouter()
const route = useRoute()
const { can } = useIdentity()
const frame = useConsoleFrame()
const where = computed(() => frame.cluster.value.name)
const {
  find, show, seg, narrowed, segments, facts, verdict, rows, groups, judged, loaded,
  isOpen, toggle, sort, sortBy, visited, reveal,
  listError, stale, groupsError, noTraffic, ephemeral, namespaces, tasks,
} = useQueuesPage()

// The section's lists, as tabs (?view=): the queues, every consumer group,
// the workers that publish their pools, and the newest messages of every queue.
// Each view keeps its own address fields; a tab change starts it afresh.
const VIEWS = [
  { id: 'queues', label: 'Queues' },
  { id: 'groups', label: 'Consumer groups' },
  { id: 'workers', label: 'Workers' },
  { id: 'messages', label: 'Messages' },
]
const view = computed(() => (VIEWS.some((v) => v.id === route.query.view) ? route.query.view : 'queues'))
function pickView(id) {
  if (id !== view.value) router.replace({ query: id === 'queues' ? {} : { view: id } })
}
const path = computed(() => (view.value === 'queues'
  ? [{ label: 'Queues' }]
  : [{ label: 'Queues', to: '/queues' }, { label: VIEWS.find((v) => v.id === view.value).label }]))
const canCreate = computed(() => can('queueAdmin'))
const creating = ref(false)
const qlink = (name) => `/queues/${encodeURIComponent(name)}`

// ---- cells ----
const dim = (text) => ({ text, dim: true })
const count = (v) => (v == null ? dim('–') : v === 0 ? dim('0') : { text: fmt(v), dim: false })
function perS(total) {
  if (total == null) return dim('–')
  if (total === 0) return dim('0')
  const v = total / HOUR_S
  return { text: v < 0.01 ? '<0.01' : rate(v), dim: false }
}
function groupsCell(r) {
  if (!r.groupsKnown) return dim('–')
  if (r.named) return { text: fmt(r.named), dim: false }
  return r.queueMode ? dim('queue mode') : dim('0')
}
function stateTitle(r) {
  const s = r.state
  if (!s) return ''
  const who = r.reader?.label || 'a group'
  if (s.kind === 'acks') return `${acksText(r.issue?.ops)} in the last hour.`.replace(/^./, (c) => c.toUpperCase())
  if (s.kind === 'lag') return `${who} is ${span(r.issue?.lag || r.lag || 0)} behind, with ${fmt(r.pending)} waiting.`
  if (s.kind === 'noReader') return `${fmt(r.pending)} waiting, and no consumer group reads it.`
  return `${fmt(r.pending)} waiting; at this pace it clears in ${s.eta < 60 ? 'under a minute' : `about ${span(s.eta)}`}.`
}
const cells = (r) => ({
  name: r.name,
  to: qlink(r.name),
  mark: toneOf(r.sev),
  state: r.state?.word || '',
  stateClass: r.state ? (r.state.tone || 'qn-t2') : '',
  stateTitle: stateTitle(r),
  waiting: count(r.pending),
  oldest: r.lag ? { text: span(r.lag), dim: false } : dim('–'),
  in: perS(r.inHour),
  acked: perS(r.ackHour),
  dead: r.dead,
  partitions: count(r.partitions),
  groups: groupsCell(r),
  spark: r.spark && r.spark.length > 1 && r.inHour > 0 ? { values: r.spark, max: Math.max(6, ...r.spark) } : null,
  sparkLabel: `${fmt(r.inHour)} arrived in the last hour`,
})

// A closed namespace says what it hides.
function summary(g) {
  const toneIn = (kind) => (g.rows.some((r) => r.state?.kind === kind && r.state.tone === 'red') ? 'red' : 'amber')
  const out = []
  if (g.counts.failing) out.push({ text: `${g.counts.failing} failing`, tone: toneIn('acks') })
  if (g.counts.behind) out.push({ text: `${g.counts.behind} behind`, tone: toneIn('lag') })
  if (g.counts.noReader) out.push({ text: `${g.counts.noReader} without a reader`, tone: 'amber' })
  if (g.counts.catching) out.push({ text: `${g.counts.catching} catching up`, tone: '' })
  return out
}
const table = computed(() => groups.value.map((g) => {
  const open = isOpen(g)
  const noun = g.total === 1 ? 'queue' : 'queues'
  return {
    key: g.ns || '\u0000none',
    ns: g.ns,
    g,
    open,
    count: narrowed.value ? `${fmt(g.rows.length)} of ${fmt(g.total)} ${noun}` : `${fmt(g.total)} ${noun}`,
    summary: open ? [] : summary(g),
    rows: open ? g.rows.map(cells) : [],
  }
}))

// ---- the headers: a click sorts inside each namespace, frozen until the next one ----
const HEADS = [
  { key: 'waiting', label: 'Waiting' },
  { key: 'oldest', label: 'Oldest' },
  { key: 'in', label: 'In a second' },
  { key: 'acked', label: 'Acked' },
  { key: 'dead', label: 'Dead letters' },
  { key: 'partitions', label: 'Partitions' },
]
const SORT_WORDS = { waiting: 'waiting', oldest: 'oldest', in: 'arrivals', acked: 'acks', dead: 'dead letters', partitions: 'partitions' }
const arrow = (key) => (sort.value?.key === key ? (sort.value.dir === 'desc' ? '↓' : '↑') : '')
const ariaSort = (key) => (sort.value?.key === key ? (sort.value.dir === 'desc' ? 'descending' : 'ascending') : 'none')
const sortLine = computed(() => {
  if (seg.value === 'eph') return ''
  const s = sort.value
  if (!s) return 'By name in each namespace. Rows never move while you look.'
  return `Sorted by ${SORT_WORDS[s.key]} at ${clock(s.at)}, ${s.dir === 'desc' ? 'largest' : 'smallest'} first in each namespace. Click again to change.`
})

// ---- a row opens its queue; its links open theirs ----
function go(r, e) {
  visited.value = r.name
  if (e.target.closest?.('a, button')) return
  if (window.getSelection?.()?.toString()) return
  if (e.metaKey || e.ctrlKey) { window.open(router.resolve(r.to).href, '_blank', 'noopener'); return }
  router.push(r.to)
}
function clearFilter() { find.value = ''; show.value = 'all' }
const emptyText = computed(() => {
  const f = find.value.trim()
  if (f) return `Nothing matches “${f}”${seg.value !== 'all' ? ' here' : ''}.`
  return { needs: 'No queue needs you.', waiting: 'No queue has messages waiting.', dead: 'No queue has dead letters.' }[seg.value] || 'Nothing to show.'
})
const noNamespace = computed(() => rows.value.filter((r) => !r.ns).length)

// Back from a queue: its row is lit, and brought into view.
onMounted(() => {
  if (!visited.value) return
  nextTick(() => document.querySelector(`[data-q="${CSS.escape(visited.value)}"]`)?.scrollIntoView({ block: 'center' }))
})
function created({ name, ns }) { reveal(name, ns) }
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="qs">
      <header class="qs-head">
        <h1 class="qs-title">Queues</h1>
        <template v-if="view === 'queues'"><span v-for="f in facts" :key="f" class="qn-tag qn-num">{{ f }}</span></template>
        <div style="flex: 1 1 0" />
        <template v-if="view === 'queues'">
          <button v-if="canCreate" type="button" class="qn-btn" aria-haspopup="dialog" @click="creating = true">New queue…</button>
          <span v-else-if="loaded" class="qn-t3" style="font-size: 12px">Creating a queue needs the admin role on this cluster.</span>
        </template>
      </header>

      <nav class="qn-tabs qs-tabs" role="tablist" aria-label="Lists of this section">
        <button v-for="v in VIEWS" :key="v.id" type="button" class="qn-tab" role="tab" :aria-selected="view === v.id" @click="pickView(v.id)">{{ v.label }}</button>
      </nav>

      <QueuesGroups v-if="view === 'groups'" :where="where" />
      <QueuesWorkers v-else-if="view === 'workers'" :where="where" />
      <QueuesMessages v-else-if="view === 'messages'" :where="where" />

      <template v-else>

      <!-- the state of every queue, in words -->
      <p class="qs-verdict">
        <template v-if="verdict.kind === 'issues'">
          <span class="qn-mark" :class="verdict.tone" aria-hidden="true" />
          <span class="qs-vtext">
            <RouterLink :to="qlink(verdict.names[0])" class="qn-mono qs-vname">{{ verdict.names[0] }}</RouterLink>
            <template v-if="verdict.names[1]">{{ verdict.more ? ', ' : ' and ' }}<RouterLink :to="qlink(verdict.names[1])" class="qn-mono qs-vname">{{ verdict.names[1] }}</RouterLink></template>
            <template v-if="verdict.more"> and {{ verdict.more }} more</template>
            {{ verdict.count === 1 ? 'needs' : 'need' }} you.
            <span v-if="!verdict.only" class="qn-t2">Every other queue drains on its own.</span>
          </span>
        </template>
        <template v-else-if="verdict.kind === 'tenant'">
          <span class="qn-mark" :class="verdict.tone" aria-hidden="true" />
          <span class="qs-vtext">No single queue needs you, but {{ fmt(verdict.failed) }} of {{ fmt(verdict.total) }} acks failed in the last hour, across queues.</span>
        </template>
        <span v-else-if="verdict.kind === 'fine'">Every queue drains on its own.</span>
        <span v-else-if="verdict.kind === 'unknown' && groupsError" class="qn-t2">The consumer groups did not answer, so what needs you is not known: {{ groupsError }}.</span>
        <span v-else-if="loaded" class="qn-t2">Reading the consumer groups…</span>
        <span v-else class="qn-t2">Reading the queues…</span>
      </p>

      <div class="qs-tools">
        <label class="qn-field qs-find">
          <svg width="14" height="14" viewBox="0 0 18 18" aria-hidden="true" style="fill: none; stroke: currentColor; stroke-width: 1.4"><circle cx="7.5" cy="7.5" r="5" /><line x1="11.3" y1="11.3" x2="15.5" y2="15.5" /></svg>
          <span class="qn-vh">Filter queues</span>
          <input v-model="find" type="text" placeholder="Filter by name, namespace or task" spellcheck="false" autocomplete="off" @keydown.esc="find = ''" />
        </label>
        <QcSeg v-model="show" :options="segments" label="Show" />
        <span v-if="sortLine" class="qn-t3 qs-sortline">{{ sortLine }}</span>
      </div>

      <QueuesEphemeral v-if="seg === 'eph'" :store="ephemeral" :find="find" :where="where" @clear="find = ''" />

      <template v-else>
        <p v-if="stale" class="qn-t2 qs-note"><span class="qn-mark amber" aria-hidden="true" />The last read failed ({{ stale.error }}); this is the list from {{ clock(stale.at) }}.</p>
        <p v-if="listError" class="qn-t2 qs-empty"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ listError }}</p>
        <p v-else-if="!loaded" class="qn-t2 qs-empty">Reading the queues…</p>
        <p v-else-if="!rows.length" class="qn-t2 qs-empty">No queues yet. The first push to a name makes one<template v-if="canCreate">, or New queue… creates one with its settings</template>.</p>
        <p v-else-if="!table.length" class="qn-t2 qs-empty">{{ emptyText }} <button type="button" class="qn-link" @click="clearFilter">Show every queue</button></p>

        <div v-else class="qs-scroll">
          <div class="qs-table" role="table" aria-label="Queues by namespace">
            <div role="rowgroup">
              <div class="qn-tr head" role="row">
                <span role="columnheader" class="qs-qhead">Queue</span>
                <span role="columnheader">State</span>
                <span v-for="h in HEADS" :key="h.key" role="columnheader" class="r qs-sorthead" :aria-sort="ariaSort(h.key)">
                  <button type="button" class="qs-sort" :class="{ on: sort && sort.key === h.key }" @click="sortBy(h.key)">
                    <span class="qs-arrow" aria-hidden="true">{{ arrow(h.key) }}</span>{{ h.label }}
                  </button>
                </span>
                <span role="columnheader" class="r">Groups</span>
                <span role="columnheader" class="qs-sparkhead">In, last hour</span>
              </div>
            </div>

            <div v-for="t in table" :key="t.key" role="rowgroup">
              <div class="qn-tr qs-nsrow" role="row">
                <span role="rowheader" class="qs-nscell" aria-colspan="10">
                  <button type="button" class="qs-ns" :aria-expanded="t.open" @click="toggle(t.g)">
                    <svg class="qs-chev" :class="{ open: t.open }" width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M4.5 2.5 8 6l-3.5 3.5" /></svg>
                    <span :class="t.ns ? 'qn-mono qs-nsname' : 'qs-nsname qs-none'">{{ t.ns || 'No namespace' }}</span>
                    <span class="qn-t3 qn-num">{{ t.count }}</span>
                    <template v-if="!t.open">
                      <span v-if="!t.summary.length" class="qn-t3">· none needs you</span>
                      <span v-for="s in t.summary" :key="s.text" class="qs-sum qn-num" :class="s.tone || 'qn-t2'">
                        <span v-if="s.tone" class="qn-mark" :class="s.tone" aria-hidden="true" />{{ s.text }}
                      </span>
                    </template>
                  </button>
                </span>
              </div>
              <div
                v-for="r in t.rows"
                :key="r.name"
                class="qn-tr click qs-row"
                :class="{ on: r.name === visited }"
                role="row"
                :data-q="r.name"
                @click="go(r, $event)"
              >
                <span role="rowheader" class="qs-name">
                  <span class="qs-markslot" aria-hidden="true"><span v-if="r.mark" class="qn-mark" :class="r.mark" /></span>
                  <RouterLink :to="r.to" class="qn-mono" :class="{ strong: r.mark }" :title="r.name">{{ r.name }}</RouterLink>
                </span>
                <span role="cell" :class="r.stateClass" :title="r.stateTitle || undefined">{{ r.state }}</span>
                <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.waiting.dim }">{{ r.waiting.text }}</span>
                <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.oldest.dim }">{{ r.oldest.text }}</span>
                <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.in.dim }">{{ r.in.text }}</span>
                <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.acked.dim }">{{ r.acked.text }}</span>
                <span role="cell" class="r qn-num">
                  <RouterLink v-if="r.dead > 0" :to="{ path: '/dlq', query: { queue: r.name } }" class="qs-dlq" :title="`Open the dead letters of ${r.name}`">{{ fmt(r.dead) }}</RouterLink>
                  <span v-else class="qn-t3">{{ r.dead === 0 ? '0' : '–' }}</span>
                </span>
                <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.partitions.dim }">{{ r.partitions.text }}</span>
                <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.groups.dim }">{{ r.groups.text }}</span>
                <span role="cell" class="qs-sparkcell">
                  <QcSpark v-if="r.spark" :values="r.spark.values" :max="r.spark.max" :width="96" :height="20" :label="r.sparkLabel" />
                </span>
              </div>
            </div>
          </div>
        </div>

        <p v-if="noTraffic" class="qn-t2 qs-note"><span class="qn-mark amber" aria-hidden="true" />The last hour's traffic did not load ({{ noTraffic }}), so In, Acked and the lines are empty, and failing acks cannot be seen.</p>
        <p class="qn-read qs-read">
          Read from <span class="qn-mono">GET /api/v1/resources/queues</span>, <span class="qn-mono">GET /api/v1/consumer-groups</span>
          and <span class="qn-mono">GET /api/v1/analytics/workload?groupBy=queue</span>, the last hour. In a second and Acked are that hour's averages;
          Acked counts acks that succeeded. Oldest is the age of the oldest message a group has not read.
        </p>
      </template>
      </template>
    </div>

    <QueuesNew
      :open="creating"
      :where="where"
      :namespaces="namespaces"
      :tasks="tasks"
      :total="rows.length"
      :no-namespace="noNamespace"
      @close="creating = false"
      @created="created"
    />
  </ConsoleShell>
</template>

<style scoped>
.qs { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.qs-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.qs-title { font-size: 22px; font-weight: 500; margin-right: 8px; }
.qs-tabs { margin-top: 18px; }
.qs-verdict { display: flex; align-items: flex-start; gap: 10px; font-size: 15px; margin-top: 18px; }
/* the mark sits on the first line, however the sentence wraps */
.qs-verdict > .qn-mark { margin-top: 7px; }
.qs-vtext { flex: 1 1 auto; min-width: 0; overflow-wrap: anywhere; }
.qs-vname { font-weight: 500; }
.qs-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px; margin-top: 22px; }
.qs-find { width: 320px; max-width: 100%; }
.qs-sortline { font-size: 12px; margin-left: auto; }
.qs-note { display: flex; align-items: center; gap: 8px; margin-top: 14px; }
.qs-empty { padding: 18px 0 4px; }
.qs-read { margin-top: 14px; max-width: 980px; }

/* the table: wide enough for every column, scrolled sideways inside itself below that */
.qs-scroll { margin-top: 16px; overflow-x: auto; }
.qs-table {
  min-width: 1080px;
  --cols: minmax(0, 3.5fr) minmax(96px, 1fr) 72px 72px 84px 66px 88px 78px 84px 100px;
}
.qs-qhead { padding-left: 18px; }
/* a flex cell: an inline button whose first item is the (empty) arrow would sit on its middle, not on the text */
.qs-sorthead { display: flex; align-items: center; justify-content: flex-end; }
.qs-sort { display: inline-flex; align-items: center; gap: 4px; color: var(--text3); font-size: 12px; }
.qs-sort:hover, .qs-sort.on { color: var(--text); }
.qs-arrow { display: inline-block; width: 10px; text-align: center; }
.qs-sparkhead { padding-left: 4px; }

/* a namespace: one row that opens and closes it */
.qn-tr.qs-nsrow { min-height: 40px; border-bottom-color: var(--line); }
.qs-nscell { grid-column: 1 / -1; }
.qs-ns { display: flex; align-items: center; gap: 8px; width: 100%; min-height: 40px; padding-right: 8px; text-align: left; color: var(--text3); }
.qs-ns:hover .qs-nsname { text-decoration: underline; text-underline-offset: 3px; }
.qs-chev { flex: 0 0 auto; width: 12px; height: 12px; margin-right: -2px; fill: none; stroke: var(--text3); stroke-width: 1.4; stroke-linecap: round; stroke-linejoin: round; transition: transform .12s ease; }
.qs-chev.open { transform: rotate(90deg); }
.qs-nsname { color: var(--text); font-weight: 500; }
.qs-none { color: var(--text2); }
.qs-sum { display: inline-flex; align-items: center; gap: 6px; }
.qs-sum + .qs-sum::before { content: '·'; color: var(--text3); margin-right: 2px; }

/* a queue */
.qs-name { display: flex; align-items: center; gap: 10px; }
.qs-markslot { flex: 0 0 8px; display: flex; align-items: center; justify-content: center; }
.qs-name a { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.qs-name a.strong { font-weight: 500; }
.qs-dlq { text-decoration: underline; text-decoration-color: var(--line); text-underline-offset: 3px; }
.qs-dlq:hover { text-decoration-color: currentColor; }
.qs-sparkcell { display: flex; align-items: center; padding-left: 4px; }

/* tone: no hairlines, so a namespace is a filled row */
:global(.qn.tone .qs-table .qs-nsrow) { background: var(--bg2); border-radius: var(--r-ctl); margin-top: 6px; }
:global(.qn.tone .qs-table .qs-ns) { padding-left: 4px; }
:global(.qn.tone .qs-dlq) { text-decoration-color: var(--text3); }
@media (prefers-reduced-motion: reduce) { .qs-chev { transition: none; } }
/* a phone: the name, its state and what waits; the queue's page has the rest */
@media (max-width: 640px) {
  .qs { padding: 18px 16px 40px; }
  .qs-title { font-size: 19px; }
  .qs-find { width: 100%; }
  .qs-sortline { flex-basis: 100%; margin-left: 0; }
  .qs-table { min-width: 0; --cols: minmax(0, 1fr) auto 64px; }
  .qs-table .qn-tr > :nth-child(n+4) { display: none; }
}
</style>
