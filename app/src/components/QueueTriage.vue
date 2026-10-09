<template>
  <section class="card triage" aria-labelledby="triage-title">
    <div class="triage-heading">
      <h3 id="triage-title">{{ filter === 'all' || filter === 'growing' ? 'Queues' : 'Open issues' }}</h3>
      <span v-if="known" class="triage-count">{{ formatNumber(attentionCount) }} of {{ formatNumber(rows.length) }} queues need attention</span>
      <span v-else class="triage-count">Status unconfirmed</span>
    </div>

    <div v-if="!known" class="triage-empty" role="status">
      {{ errorText || 'Reading queues and consumer groups…' }}
      <p v-if="errorText">Queue health cannot be assessed until both sources are available.</p>
    </div>
    <template v-else>
      <div class="triage-tools">
        <label class="triage-search"><span class="sr-only">Search queues or namespaces</span><input v-model="search" type="search" placeholder="Search queue or namespace…" class="input" /></label>
        <label><span class="sr-only">Queue triage filter</span><select v-model="filter" class="input" aria-label="Queue triage filter">
          <option value="attention">Needs attention</option><option value="bad">Lag ≥ 5 minutes</option><option value="noReader">No reader</option><option value="growing">Pending increased</option><option value="all">All queues</option>
        </select></label>
        <span class="triage-order">Priority, then group lag</span>
      </div>
      <div v-if="!filtered.length" class="triage-empty">
        {{ rows.length === 0 ? 'No queues in this scope.' : attentionCount === 0 && filter === 'attention' && !search ? 'No queue matches the attention rules.' : 'No queues match these filters.' }}
        <button v-if="search || filter !== 'all'" class="btn btn-ghost" @click="search = ''; filter = 'all'">Show all queues</button>
      </div>
      <ul v-else class="triage-list" aria-label="Queue issues">
        <li v-for="row in pageRows" :key="row.name" data-triage-row>
          <button class="triage-row" aria-haspopup="dialog" aria-controls="queue-issue-drawer" :aria-expanded="selection?.kind === 'queue' && selection.name === row.name" @click="openQueue(row)">
            <span class="g" :class="row.sev || 'idle'" aria-hidden="true" />
            <span class="triage-name">{{ row.name }}<span class="triage-meta">{{ row.namespace || 'No namespace' }}</span></span>
            <span class="triage-finding" :class="row.sev">{{ finding(row) }}<template v-if="row.reason === 'lag'"> · {{ duration(row.lag) }}</template><template v-else-if="row.reason === 'noReader'"> · {{ number(row.pending) }} pending</template></span>
            <span class="triage-open" aria-hidden="true">Details ›</span>
          </button>
        </li>
      </ul>
      <div v-if="pages > 1" class="triage-pagination">
        <span role="status">{{ formatNumber((page - 1) * pageSize + 1) }}–{{ formatNumber(Math.min(page * pageSize, filtered.length)) }} of {{ formatNumber(filtered.length) }} queues</span>
        <div><button class="btn btn-ghost" :disabled="page === 1" @click="page--">Previous</button><button class="btn btn-ghost" :disabled="page >= pages" @click="page++">Next</button></div>
      </div>
    </template>
    <ul v-if="tenantIssues.length" class="triage-list triage-tenant" aria-label="Tenant issues">
      <li v-for="issue in tenantIssues" :key="issue.key">
        <button class="triage-row" aria-haspopup="dialog" aria-controls="queue-issue-drawer" :aria-expanded="selection?.key === issue.key" @click="selection = { kind: 'tenant', key: issue.key, name: issue.name }">
          <span class="g" :class="issue.sev" aria-hidden="true" /><span class="triage-name">{{ issue.name }}</span><span class="triage-finding">{{ issue.why }}</span><span class="triage-open" aria-hidden="true">Details ›</span>
        </button>
      </li>
    </ul>
    <slot name="rules" />
  </section>

  <Teleport to="body">
    <dialog ref="drawer" id="queue-issue-drawer" class="drawer-panel triage-drawer" aria-labelledby="issue-drawer-title" @cancel.prevent="closeDrawer" @close="onDialogClose" @pointerdown="backdropDown = isOutside($event)" @click="onDrawerClick">
      <div class="card-header triage-drawer-header">
        <h3 id="issue-drawer-title">{{ selection?.kind === 'tenant' ? 'Issue details' : 'Queue details' }}</h3>
        <button class="btn btn-ghost btn-icon modal-close" aria-label="Close issue details" autofocus @click="closeDrawer"><svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg></button>
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
          <details class="triage-rules"><summary>How this is decided</summary><p>Consumer lag of at least 1 minute needs attention; at least 5 minutes is failing. Pending messages without a reader also need attention. A group that has never consumed does not count as a reader. Queues and Consumer groups use these same rules.</p><p>Pending change compares two queue readings in this browser, at most 2 minutes apart. It resets after a failed queue read or scope change. “—” means there is no comparable reading. An increase alone is not an alert. These readings are independent of the history range.</p></details>
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

const props = defineProps({
  queues: { type: Array, default: () => [] }, groups: { type: Array, default: () => [] },
  sampleAt: { default: null }, groupsAt: { default: null },
  queuesError: { type: String, default: '' }, groupsError: { type: String, default: '' },
  tenantIssues: { type: Array, default: () => [] },
  tenantError: { type: String, default: '' },
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
const rows = computed(() => known.value ? buildQueueTriage(props.queues, props.groups, observation.value?.delta) : [])
const elapsed = computed(() => observation.value?.elapsed)
const attentionCount = computed(() => rows.value.filter(r => r.sev).length)
const search = ref(''), filter = ref('attention'), page = ref(1)
const pageSize = 10
const filtered = computed(() => filterQueueTriage(rows.value, search.value, filter.value))
const pages = computed(() => Math.max(1, Math.ceil(filtered.value.length / pageSize)))
watch([search, filter], () => { page.value = 1 })
watch(pages, n => { page.value = Math.min(page.value, n) })
const pageRows = computed(() => filtered.value.slice((page.value - 1) * pageSize, page.value * pageSize))

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
  if (row.reason === 'noReader') return { title: 'Check the consumer subscription', body: row.deadOnly ? 'Groups are registered, but none has consumed. Check that a consumer is running with the intended queue and group.' : 'No consumer group is registered for this queue. Check the intended subscription and consumer deployment.' }
  if (row.reason === 'lag') return { title: row.pending === 0 ? 'Check unconfirmed work in the lagging groups' : 'Inspect the groups falling behind', body: 'Check their assigned partitions, consumer processes and job failures. Compare queue history before deciding whether more capacity is needed.' }
  if (row.pending === null) return { title: 'Confirm the pending count', body: 'The current reading does not report pending messages. Inspect the queue before drawing a backlog conclusion.' }
  if (row.delta > 0) return { title: 'Check whether the increase persists', body: 'More messages are pending than at the previous read. Open queue history to distinguish a short burst from sustained growth.' }
  return { title: 'No queue alert observed', body: 'Use queue history to inspect throughput and backlog over time. A pending count on its own does not establish a capacity problem.' }
}
</script>

<style scoped>
.triage { overflow: hidden; }
.triage-heading { display: flex; align-items: center; gap: 12px; padding: 12px 16px; border-bottom: 1px solid var(--bd-soft); }
.triage-heading h3 { margin: 0; font-size: 13px; }
.triage-count { margin-left: auto; color: var(--text-low); font-size: 12px; }
.triage-tools { display: flex; align-items: center; gap: 10px; padding: 10px 16px; border-bottom: 1px solid var(--bd-soft); }
.triage-search { flex: 1; max-width: 330px; }
.triage-tools .input { width: 100%; font-size: 12px; }
.triage-order { margin-left: auto; color: var(--text-low); font-size: 11px; }
.triage-list { list-style: none; margin: 0; padding: 0; }
.triage-list li + li { border-top: 1px solid var(--bd-soft); }
.triage-row { display: grid; grid-template-columns: 12px minmax(0, 1fr) auto 65px; align-items: center; gap: 12px; width: 100%; border: 0; background: none; padding: 12px 16px; text-align: left; color: var(--text-hi); font: inherit; cursor: pointer; }
.triage-row:hover, .triage-row[aria-expanded="true"] { background: var(--ink-3); }
.triage-name { min-width: 0; font-size: 13px; overflow-wrap: anywhere; }
.triage-meta { display: block; color: var(--text-low); font-size: 11px; margin-top: 3px; }
.triage-finding { color: var(--text-mid); font-size: 12px; font-variant-numeric: tabular-nums; }
.triage-open { text-align: right; color: var(--text-low); font-size: 12px; }
.triage-pagination { display: flex; justify-content: space-between; align-items: center; padding: 8px 16px; border-top: 1px solid var(--bd-soft); color: var(--text-low); font-size: 11px; }
.triage-pagination > div { display: flex; gap: 8px; }
.triage .btn { font-size: 11px; }
.triage-pagination button:disabled { opacity: .4; cursor: default; }
.triage-tenant { border-top: 1px solid var(--bd); }
.triage-empty { padding: 16px; color: var(--text-mid); font-size: 12px; display: flex; flex-wrap: wrap; align-items: center; gap: 12px; }
.triage-empty p { margin: 0; }
.triage-drawer { inset: 0 0 0 auto; margin: 0 0 0 auto; padding: 0; width: min(540px, 100%); max-width: 100%; height: 100dvh; max-height: 100dvh; border: 0; border-left: 1px solid var(--bd); color: var(--text-hi); background: var(--ink-2); overscroll-behavior: contain; }
.triage-drawer[open] { display: flex; flex-direction: column; }
.triage-drawer::backdrop { background: rgb(0 0 0 / .18); }
.triage-drawer-header { flex-shrink: 0; }
.triage-drawer-body { flex: 1; min-height: 0; overflow-y: auto; padding: 24px; overscroll-behavior: contain; }
.triage-drawer-name { margin: 0 0 12px; font-size: 20px; font-weight: 550; line-height: 1.4; overflow-wrap: anywhere; }
.triage-status { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; color: var(--text-mid); font-size: 12px; }
.triage-status > span:last-child { margin-left: auto; color: var(--text-low); }
.triage-evidence { display: grid; grid-template-columns: 1fr 1fr; gap: 1px; background: var(--bd); border: 1px solid var(--bd); border-radius: var(--r-card); overflow: hidden; margin: 24px 0 10px; }
.triage-evidence > div { background: var(--ink-2); padding: 16px; min-width: 0; }
.triage-evidence dt { font-size: 12px; color: var(--text-mid); }
.triage-evidence dd { margin: 6px 0; font-size: 25px; font-variant-numeric: tabular-nums; }
.triage-evidence small, .triage-stamp { color: var(--text-low); font-size: 11px; line-height: 1.5; }
.triage-stamp { margin: 0; }
.triage-check { margin-top: 24px; }
.triage-kicker { color: var(--text-low); font-size: 10px; letter-spacing: .08em; text-transform: uppercase; }
.triage-check h4 { margin: 7px 0; font-size: 14px; }
.triage-check p, .triage-explanation { color: var(--text-mid); font-size: 12px; line-height: 1.7; }
.triage-check p { margin: 0; }
.triage-groups { margin-top: 24px; }
.triage-groups h4 { margin: 0 0 10px; font-size: 12px; }
.triage-groups h4 span { float: right; color: var(--text-low); font-weight: 400; }
.triage-groups ul { margin: 0; padding: 0; list-style: none; }
.triage-groups li { display: flex; justify-content: space-between; gap: 12px; padding: 10px 0; border-bottom: 1px solid var(--bd-soft); font-size: 12px; }
.triage-groups li > span { overflow-wrap: anywhere; min-width: 0; }
.triage-groups li > b { flex-shrink: 0; font-weight: 500; font-variant-numeric: tabular-nums; }
.triage-groups p { font-size: 11px; color: var(--text-low); }
.triage-explanation { padding: 12px; margin: 20px 0; background: var(--ink-3); border-radius: var(--r-card); }
.triage-rules { margin-top: 20px; color: var(--text-low); font-size: 11px; line-height: 1.6; }
.triage-rules summary { cursor: pointer; color: var(--text-mid); }
.triage-drawer-footer { flex-shrink: 0; flex-wrap: wrap; justify-content: flex-start; padding: 16px 24px; }
.triage-drawer-footer .btn { font-size: 12px; }
.bad { color: var(--ember-400); }
.warn { color: var(--warn-400); }
@media (max-width: 760px) {
  .triage-heading { align-items: start; flex-wrap: wrap; gap: 6px; }
  .triage-tools { flex-wrap: wrap; }
  .triage-search { flex: 1 1 100%; max-width: none; }
  .triage-order { margin-left: 0; }
  .triage-row { grid-template-columns: 12px minmax(0, 1fr) auto; gap: 4px 10px; }
  .triage-row > .g { align-self: start; margin-top: 5px; }
  .triage-finding { grid-column: 2; }
  .triage-open { grid-column: 3; grid-row: 1 / span 2; }
  .triage-count { margin-left: 0; }
}
</style>
