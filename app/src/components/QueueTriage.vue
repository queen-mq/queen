<template>
  <section class="card triage" aria-labelledby="triage-title">
    <div class="card-header">
      <h3 id="triage-title">{{ filter === 'all' || filter === 'growing' ? 'Queues' : 'Open issues' }}</h3>
      <span class="card-sub">{{ known ? `${formatNumber(attentionCount)} of ${formatNumber(rows.length)} ${rows.length === 1 ? 'queue needs' : 'queues need'} attention` : 'Status unconfirmed' }}</span>
    </div>

    <!-- The cell under every queue, an operator's only: what the top bar
         colours, said as issues. Each opens System, where the node is. -->
    <ul v-if="cellIssues.length" class="triage-list triage-cell" aria-label="Cell issues">
      <li v-for="issue in cellIssues" :key="issue.key">
        <RouterLink class="triage-row" :to="issue.to">
          <span class="g" :class="issue.sev" aria-hidden="true" />
          <span class="triage-name">{{ issue.name }}<i class="cell-chip" title="The cell: shared by every tenant on it">cell</i></span>
          <span class="triage-finding" :class="issue.sev">{{ issue.why }}</span>
          <span class="triage-open" aria-hidden="true">System<Icon name="chevron-right" :size="14" /></span>
        </RouterLink>
      </li>
    </ul>

    <div v-if="!known" class="triage-empty" role="status" :title="errorText ? 'Queue health needs both the queues and the consumer groups' : undefined">
      {{ errorText || 'Reading queues and consumer groups…' }}
    </div>
    <template v-else>
      <!-- A short list (limit) is the Summary's: the first rows that need you,
           no tools, and the way to the full list. -->
      <div v-if="!limit" class="triage-tools">
        <input v-model="search" type="search" placeholder="Search queues or namespaces" class="input triage-search" aria-label="Search queues or namespaces" />
        <UiSelect v-model="filter" label="Show" aria-label="Which queues to show" :options="FILTERS" />
      </div>
      <div v-if="!filtered.length" class="triage-empty">
        {{ rows.length === 0 ? 'No queues yet' : attentionCount === 0 && filter === 'attention' && !search ? 'Nothing needs attention' : 'No queue matches' }}
        <button v-if="search || filter !== 'all'" class="btn btn-ghost" @click="search = ''; filter = 'all'">Show all queues</button>
      </div>
      <ul v-else class="triage-list" aria-label="Queues, worst first, then the longest group lag" title="Worst first, then the longest group lag">
        <li v-for="row in shownRows" :key="row.name" data-triage-row>
          <button class="triage-row" aria-haspopup="dialog" aria-controls="queue-issue-drawer" :aria-expanded="selection?.kind === 'queue' && selection.name === row.name" @click="openQueue(row)">
            <span class="g" :class="row.sev || 'idle'" aria-hidden="true" />
            <span class="triage-name">{{ row.name }}<span class="triage-meta">{{ row.namespace || 'No namespace' }}</span></span>
            <span class="triage-finding" :class="row.sev">{{ finding(row) }}<template v-if="row.reason === 'lag'"> · {{ duration(row.lag) }}</template><template v-else-if="row.reason === 'noReader'"> · {{ number(row.pending) }} pending</template></span>
            <span class="triage-open" aria-hidden="true">Details<Icon name="chevron-right" :size="14" /></span>
          </button>
        </li>
      </ul>
      <RouterLink v-if="limit && moreTo && filtered.length > limit" class="triage-more" :to="moreTo">
        All {{ formatNumber(filtered.length) }} issues<Icon name="chevron-right" :size="14" />
      </RouterLink>
      <div v-if="!limit && pages > 1" class="triage-pagination">
        <span role="status">{{ formatNumber((page - 1) * pageSize + 1) }}–{{ formatNumber(Math.min(page * pageSize, filtered.length)) }} of {{ formatNumber(filtered.length) }} queues</span>
        <div><button class="btn btn-ghost" :disabled="page === 1" @click="page--">Previous</button><button class="btn btn-ghost" :disabled="page >= pages" @click="page++">Next</button></div>
      </div>
    </template>
    <ul v-if="tenantIssues.length" class="triage-list triage-tenant" aria-label="Tenant issues">
      <li v-for="issue in tenantIssues" :key="issue.key">
        <button class="triage-row" aria-haspopup="dialog" aria-controls="queue-issue-drawer" :aria-expanded="selection?.key === issue.key" @click="selection = { kind: 'tenant', key: issue.key, name: issue.name }">
          <span class="g" :class="issue.sev" aria-hidden="true" /><span class="triage-name">{{ issue.name }}</span><span class="triage-finding">{{ issue.why }}</span><span class="triage-open" aria-hidden="true">Details<Icon name="chevron-right" :size="14" /></span>
        </button>
      </li>
    </ul>
    <slot name="rules" />
  </section>

  <Teleport to="body">
    <dialog ref="drawer" id="queue-issue-drawer" class="drawer-panel triage-drawer" aria-labelledby="issue-drawer-title" @cancel.prevent="closeDrawer" @close="onDialogClose" @pointerdown="backdropDown = isOutside($event)" @click="onDrawerClick">
      <div class="card-header triage-drawer-header">
        <h3 id="issue-drawer-title">{{ selection?.kind === 'tenant' ? 'Issue details' : 'Queue details' }}</h3>
        <button class="btn btn-ghost btn-icon modal-close" aria-label="Close issue details" autofocus @click="closeDrawer"><Icon name="x" :size="18" /></button>
      </div>
      <div class="triage-drawer-body">
        <h2 class="triage-drawer-name">{{ selection?.name }}</h2>
        <div v-if="detailUnavailable" class="panel-err" role="status">{{ (selection?.kind === 'tenant' && tenantError) || errorText || 'Refreshing queue and consumer readings…' }} Current health cannot be confirmed.</div>
        <template v-else-if="selected">
          <div class="triage-status"><span class="g" :class="selected.sev || 'idle'" aria-hidden="true" /><strong :class="selected.sev">{{ finding(selected) }}</strong><span>{{ selected.namespace || 'No namespace' }}</span></div>
          <dl class="triage-evidence">
            <div><dt>Pending</dt><dd>{{ number(selected.pending) }}</dd><small>Excludes in-flight work</small></div>
            <div><dt>In flight</dt><dd>{{ number(selected.processing) }}</dd><small>Currently being processed</small></div>
            <div><dt>Pending change</dt><dd>{{ change(selected.delta) }}</dd><small>{{ elapsed ? `Between reads · ${Math.round(elapsed / 1000)}s apart` : 'Waiting for a second reading' }}</small></div>
            <div><dt>Group lag</dt><dd :class="selected.sev">{{ duration(selected.lag) }}</dd><small>Oldest unconfirmed message</small></div>
          </dl>
          <p class="triage-stamp">Queues read at {{ readTime(sampleAt) }} · groups at {{ readTime(groupsAt) }}</p>
          <section class="triage-check" aria-labelledby="issue-next-check">
            <span class="triage-kicker">Next check</span><h4 id="issue-next-check">{{ nextCheck(selected).title }}</h4><p>{{ nextCheck(selected).body }}</p>
          </section>
          <section v-if="selected.affected.length" class="triage-groups" aria-labelledby="issue-groups-title">
            <h4 id="issue-groups-title">Groups behind <span>{{ selected.affected.length }} of {{ selected.groupCount }}</span></h4>
            <ul><li v-for="g in selected.affected.slice(0, 10)" :key="g.name || g.consumerGroup"><span>{{ groupName(g) }}</span><b>{{ duration(g.maxTimeLag) }}</b></li></ul>
            <p v-if="selected.affected.length > 10">Showing 10 of {{ selected.affected.length }} groups. Open consumer groups for the full list.</p>
          </section>
          <p v-if="selected.reason === 'lag' && selected.pending === 0" class="triage-explanation">Zero pending excludes in-flight work. Group lag measures age before confirmation. Inspect in-flight jobs and group progress; these counts come from separate readings.</p>
          <details class="triage-rules"><summary>How this is decided</summary><p>Consumer lag of at least {{ formatSpan(lines.lagWarnSeconds) }} needs attention; at least {{ formatSpan(lines.lagBadSeconds) }} is failing. A queue with lag lines of its own in <router-link to="/settings">Settings</router-link> is judged by those. Pending messages also need attention when nothing is set up to read them: no consumer group on the queue, and no queue-mode consumer that has ever consumed. A queue Kafka clients created is exempt. A Kafka group with no live member is judged by its lag for {{ formatSpan(lines.inactiveAfterSeconds) }}, then it is inactive and not an issue. Queues and Consumer groups use these same rules.</p><p>Pending change compares two queue readings in this browser, at most 2 minutes apart. It resets after a failed queue read or scope change. “—” means there is no comparable reading. An increase alone is not an alert. These readings are independent of the history range.</p></details>
        </template>
        <template v-else-if="selectedTenant">
          <div class="triage-status"><span class="g" :class="selectedTenant.sev" aria-hidden="true" /><strong>Tenant-wide issue</strong></div>
          <p class="triage-explanation">{{ selectedTenant.why }}</p>
          <section class="triage-check"><span class="triage-kicker">Next check</span><h4>Inspect failed acknowledgements</h4><p>Review dead-letter entries and worker errors to identify the affected messages. The failure count covers the selected history window.</p></section>
        </template>
        <p v-else-if="selection?.kind === 'queue'" class="triage-explanation" role="status">This queue is no longer present in the latest reading.</p>
        <p v-else-if="selection" class="triage-explanation" role="status">This issue is no longer reported. Check the overview for the latest status.</p>
      </div>
      <div v-if="!detailUnavailable && (selected || selectedTenant)" class="modal-foot triage-drawer-footer">
        <template v-if="selected">
          <RouterLink class="btn" :to="queueLocation(selected.name, route)">Inspect queue →</RouterLink>
          <RouterLink class="btn btn-ghost" :to="queueLocation(selected.name, route, 'consumers')">Inspect consumer groups →</RouterLink>
        </template>
        <RouterLink v-else class="btn" :to="selectedTenant.to">Inspect failures →</RouterLink>
      </div>
    </dialog>
  </Teleport>
</template>

<script setup>
import { queueLocation } from '@/composables/navigation'
const route = useRoute()
import { useRoute } from 'vue-router'
import { computed, nextTick, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { formatNumber } from '@/composables/useApi'
import { buildQueueTriage, filterQueueTriage, observePending } from '@/composables/queueTriage'
import { formatSpan } from '@/composables/settingsDoc'
import { useSettingsStore } from '@/stores/settingsStore'
import Icon from '@/components/Icon.vue'
import UiSelect from '@/components/UiSelect.vue'

const FILTERS = [
  { value: 'attention', label: 'Needs attention' },
  { value: 'bad', label: 'Falling behind' },
  { value: 'noReader', label: 'No reader' },
  { value: 'growing', label: 'Pending increased' },
  { value: 'all', label: 'All queues' },
]

const props = defineProps({
  queues: { type: Array, default: () => [] }, groups: { type: Array, default: () => [] },
  sampleAt: { default: null }, groupsAt: { default: null },
  queuesError: { type: String, default: '' }, groupsError: { type: String, default: '' },
  tenantIssues: { type: Array, default: () => [] },
  /** The cell's problems (useBrokerStatus cellIssues): rows that open System. */
  cellIssues: { type: Array, default: () => [] },
  tenantError: { type: String, default: '' },
  /** Show only the first rows that need you, without the tools (the Summary). */
  limit: { type: Number, default: 0 },
  /** Where "All N issues" goes when the list is cut. */
  moreTo: { type: [Object, String], default: null },
})
const observation = shallowRef(null)
const queueUnavailable = ref(true), groupsUnavailable = ref(true)
let lastQueueAt = null, lastGroupAt = null
watch([() => props.sampleAt, () => props.queuesError], ([at, error]) => {
  if (error || !at) { queueUnavailable.value = true; observation.value = null; return }
  const timestamp = new Date(at).getTime()
  if (!Number.isFinite(timestamp) || timestamp === lastQueueAt) return
  observation.value = observePending(observation.value?.current, props.queues, timestamp)
  lastQueueAt = timestamp
  queueUnavailable.value = false
}, { immediate: true })
watch([() => props.groupsAt, () => props.groupsError], ([at, error]) => {
  if (error || !at) { groupsUnavailable.value = true; return }
  const timestamp = new Date(at).getTime()
  if (!Number.isFinite(timestamp) || timestamp === lastGroupAt) return
  lastGroupAt = timestamp
  groupsUnavailable.value = false
}, { immediate: true })
const known = computed(() => !queueUnavailable.value && !groupsUnavailable.value)
const errorText = computed(() => props.queuesError || props.groupsError)
const readTime = at => new Date(at).toLocaleTimeString()
const { lines, linesFor } = useSettingsStore()
const rows = computed(() => known.value ? buildQueueTriage(props.queues, props.groups, observation.value?.delta, linesFor) : [])
const elapsed = computed(() => observation.value?.elapsed)
const attentionCount = computed(() => rows.value.filter(r => r.sev).length)
const search = ref(''), filter = ref('attention'), page = ref(1)
const pageSize = 10
const filtered = computed(() => filterQueueTriage(rows.value, search.value, filter.value))
const pages = computed(() => Math.max(1, Math.ceil(filtered.value.length / pageSize)))
watch([search, filter], () => { page.value = 1 })
watch(pages, n => { page.value = Math.min(page.value, n) })
const pageRows = computed(() => filtered.value.slice((page.value - 1) * pageSize, page.value * pageSize))
const shownRows = computed(() => (props.limit ? filtered.value.slice(0, props.limit) : pageRows.value))

// Selection belongs to an identity, not a list position. A heartbeat can move
// or resolve the row; it must never switch the open drawer to another queue.
const selection = shallowRef(null), drawer = ref(null)
const selected = computed(() => selection.value?.kind === 'queue' ? rows.value.find(r => r.name === selection.value.name) : null)
const selectedTenant = computed(() => selection.value?.kind === 'tenant' ? props.tenantIssues.find(i => i.key === selection.value.key) : null)
const detailUnavailable = computed(() => !known.value || (selection.value?.kind === 'tenant' && Boolean(props.tenantError)))
const openQueue = row => { selection.value = { kind: 'queue', name: row.name } }
let closing = false
const closeDrawer = async () => {
  const panel = drawer.value
  if (!panel?.open || closing) return
  closing = true
  // A quick close can interrupt entry. Leave from the current position and
  // scrim opacity instead of jumping to the fully open frame first.
  panel.style.setProperty('--drawer-leave-from', getComputedStyle(panel).transform)
  panel.style.setProperty('--drawer-scrim-from', getComputedStyle(panel, '::backdrop').opacity)
  panel.dataset.drawerMotion = 'leave'
  // Keep the native modal and its content in place until the slide finishes.
  await Promise.allSettled(panel.getAnimations().map(animation => animation.finished))
  if (drawer.value === panel) {
    panel.close()
    selection.value = null
  }
  closing = false
}
watch(selection, async value => {
  await nextTick()
  if (value && selection.value === value && drawer.value && !drawer.value.open) {
    drawer.value.dataset.drawerMotion = 'enter'
    drawer.value.showModal()
  }
  else if (!selection.value) drawer.value?.close()
})
const onDialogClose = () => { if (!drawer.value?.open) selection.value = null }
onBeforeUnmount(() => drawer.value?.close())
let backdropDown = false
function isOutside(event) {
  const rect = drawer.value?.getBoundingClientRect()
  return rect && (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom)
}
function onDrawerClick(event) {
  if (backdropDown && isOutside(event)) closeDrawer()
  backdropDown = false
}
const groupName = g => (g.name || g.consumerGroup) === '__QUEUE_MODE__' ? 'Queue mode' : g.name || g.consumerGroup
const number = n => n === null ? '—' : formatNumber(n)
const change = n => n === null ? '—' : `${n > 0 ? '+' : ''}${formatNumber(n)}`
const duration = n => n === null || n === undefined ? '—' : n < 60 ? `${Math.round(n)}s` : n < 3600 ? `${Math.floor(n / 60)}m ${Math.round(n % 60)}s` : `${Math.floor(n / 3600)}h ${Math.floor((n % 3600) / 60)}m`
const finding = row => row.reason === 'lag' ? 'Consumer lag' : row.reason === 'noReader' ? 'No reader' : row.pending === null ? 'Pending unknown' : 'No alert'
function nextCheck(row) {
  if (row.reason === 'noReader') return { title: 'Check the consumer subscription', body: row.deadOnly ? 'Queue mode has a cursor on this queue, but it has never consumed. Check that a consumer is running on this queue.' : 'No consumer group is registered for this queue. Check the intended subscription and consumer deployment.' }
  if (row.reason === 'lag') return { title: row.pending === 0 ? 'Check unconfirmed work in the lagging groups' : 'Inspect the groups falling behind', body: 'Check their assigned partitions, consumer processes and job failures. Compare queue history before deciding whether more capacity is needed.' }
  if (row.pending === null) return { title: 'Confirm the pending count', body: 'The current reading does not report pending messages. Inspect the queue before drawing a backlog conclusion.' }
  if (row.delta > 0) return { title: 'Check whether the increase persists', body: 'More messages are pending than at the previous read. Open queue history to distinguish a short burst from sustained growth.' }
  return { title: 'No queue alert observed', body: 'Use queue history to inspect throughput and backlog over time. A pending count on its own does not establish a capacity problem.' }
}
</script>

<style scoped>
.triage { overflow: hidden; }
/* The card header's 14px edge, all the way down. */
.triage-tools { display: flex; align-items: center; gap: 8px; padding: 12px 14px; border-bottom: 1px solid var(--bd-soft); }
.triage-search { flex: 1 1 auto; max-width: 320px; height: 32px; }
.triage-list { list-style: none; margin: 0; padding: 0; }
.triage-list li + li { border-top: 1px solid var(--bd-soft); }
.triage-row { display: grid; grid-template-columns: 12px minmax(0, 1fr) auto 64px; align-items: center; gap: 12px; width: 100%; border: 0; background: none; padding: 12px 14px; text-align: left; color: var(--text-hi); font: inherit; cursor: pointer; transition: background .12s var(--ease); }
.triage-row:hover, .triage-row[aria-expanded="true"] { background: var(--ink-3); }
/* Inside the card's clipping edge the focus ring sits within the row. */
.triage-row:focus-visible { outline-offset: -2px; }
.triage-name { min-width: 0; font-size: 13px; overflow-wrap: anywhere; }
.triage-meta { display: block; color: var(--text-low); font-size: 12px; margin-top: 2px; }
.triage-finding { color: var(--text-mid); font-size: 12px; font-variant-numeric: tabular-nums; }
.triage-open { display: inline-flex; align-items: center; justify-content: flex-end; gap: 2px; color: var(--text-low); font-size: 12px; white-space: nowrap; }
.triage-row:hover .triage-open { color: var(--text-hi); }
.triage-more {
  display: flex; align-items: center; justify-content: flex-end; gap: 4px;
  padding: 9px 14px; border-top: 1px solid var(--bd-soft); font-size: 12.5px; color: var(--text-mid);
}
.triage-more:hover { color: var(--text-hi); }
.triage-pagination { display: flex; justify-content: space-between; align-items: center; gap: 8px; padding: 8px 14px; border-top: 1px solid var(--bd-soft); color: var(--text-low); font-size: 12px; }
.triage-pagination > div { display: flex; gap: 8px; }
.triage-pagination button:disabled { opacity: .4; cursor: default; }
.triage-tenant { border-top: 1px solid var(--bd); }
.triage-cell { border-bottom: 1px solid var(--bd); }
/* A cell finding is a sentence, not a count: the name keeps its width and
   the sentence wraps beside it. */
.triage-cell .triage-row { grid-template-columns: 12px max-content minmax(0, 1fr) 64px; }
.triage-cell .triage-finding { text-align: right; }
@media (max-width: 760px) {
  .triage-cell .triage-row { grid-template-columns: 12px minmax(0, 1fr) auto; }
  .triage-cell .triage-finding { text-align: left; grid-column: 2 / -1; }
}
a.triage-row { text-decoration: none; }
.triage-empty { padding: 12px 14px; min-height: 48px; color: var(--text-mid); font-size: 13px; display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; }
.triage-drawer { inset: 0 0 0 auto; margin: 0 0 0 auto; padding: 0; width: min(540px, 100%); max-width: 100%; height: 100dvh; max-height: 100dvh; border: 0; border-left: 1px solid var(--bd); color: var(--text-hi); background: var(--ink-2); overscroll-behavior: contain; }
.triage-drawer[open] { display: flex; flex-direction: column; }
.triage-drawer::backdrop { background: rgb(0 0 0 / .18); }
.triage-drawer-header { flex-shrink: 0; }
.triage-drawer-body { flex: 1; min-height: 0; overflow-y: auto; padding: 24px; overscroll-behavior: contain; }
.triage-drawer-name { margin: 0 0 12px; font-size: 18px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.4; overflow-wrap: anywhere; }
.triage-status { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; color: var(--text-mid); font-size: 12px; }
.triage-status > span:last-child { margin-left: auto; color: var(--text-low); }
.triage-evidence { display: grid; grid-template-columns: 1fr 1fr; gap: 1px; background: var(--bd); border: 1px solid var(--bd); border-radius: var(--r-card); overflow: hidden; margin: 24px 0 12px; }
.triage-evidence > div { background: var(--ink-2); padding: 16px; min-width: 0; }
.triage-evidence dt { font-size: 12px; color: var(--text-mid); }
.triage-evidence dd { margin: 4px 0; font-size: 20px; font-weight: 600; letter-spacing: -0.015em; font-variant-numeric: tabular-nums; }
.triage-evidence small, .triage-stamp { color: var(--text-low); font-size: 12px; line-height: 1.5; }
.triage-stamp { margin: 0; }
.triage-check { margin-top: 24px; }
.triage-kicker { color: var(--text-low); font-size: 12px; }
.triage-check h4 { margin: 4px 0 8px; font-size: 13px; font-weight: 600; }
.triage-check p, .triage-explanation { color: var(--text-mid); font-size: 12px; line-height: 1.7; }
.triage-check p { margin: 0; }
.triage-groups { margin-top: 24px; }
.triage-groups h4 { margin: 0 0 8px; font-size: 13px; font-weight: 600; }
.triage-groups h4 span { float: right; color: var(--text-low); font-weight: 400; }
.triage-groups ul { margin: 0; padding: 0; list-style: none; }
.triage-groups li { display: flex; justify-content: space-between; gap: 12px; padding: 8px 0; border-bottom: 1px solid var(--bd-soft); font-size: 12px; }
.triage-groups li > span { overflow-wrap: anywhere; min-width: 0; }
.triage-groups li > b { flex-shrink: 0; font-weight: 500; font-variant-numeric: tabular-nums; }
.triage-groups p { font-size: 12px; color: var(--text-low); }
.triage-explanation { padding: 12px; margin: 20px 0; background: var(--ink-3); border-radius: var(--r-card); }
.triage-rules { margin-top: 20px; color: var(--text-low); font-size: 12px; line-height: 1.6; }
.triage-rules summary { cursor: pointer; color: var(--text-mid); }
.triage-drawer-footer { flex-shrink: 0; flex-wrap: wrap; justify-content: flex-start; padding: 16px 24px; }
.bad { color: var(--ember-400); }
.warn { color: var(--warn-400); }
@media (max-width: 640px) {
  .triage-tools, .triage-row, .triage-pagination, .triage-empty { padding-left: 12px; padding-right: 12px; }
}
@media (max-width: 760px) {
  .triage-tools { flex-wrap: wrap; }
  .triage-search { flex: 1 1 100%; max-width: none; }
  .triage-row { grid-template-columns: 12px minmax(0, 1fr) auto; gap: 4px 12px; }
  .triage-row > .g { align-self: start; margin-top: 4px; }
  .triage-finding { grid-column: 2; }
  .triage-open { grid-column: 3; grid-row: 1 / span 2; }
}
</style>
