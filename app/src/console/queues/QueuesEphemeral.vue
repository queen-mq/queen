<script setup>
// The ephemeral queues: in memory on the node that holds each partition,
// outside the raft log. A row picks the queue (?name=) and its detail opens
// beside the list; the list itself never moves. By name, or in the order a
// header click froze; at most 200 rows, said when cut.
import { computed, nextTick, onBeforeUnmount, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { describeApiError } from '@/api/errors'
import { replaceQueryFields } from '@/composables/useRouteState'
import { onClusterChange, useIdentity } from '@/stores/identity'
import QcSeg from '../QcSeg.vue'
import { clock } from '../useWindow'
import EphemeralDetail from './EphemeralDetail.vue'
import { EPH_SORTS, MAX_ROWS, droppedOf, listRows, nextSort, size, snapshotOrder } from './ephemeralView'

const props = defineProps({
  store: { type: Object, required: true }, // useEphemeralStore()
  find: { type: String, default: '' },
  where: { type: String, default: '' },     // the cluster; read from the identity when the page does not pass it
})
const emit = defineEmits(['clear'])
const route = useRoute()
const router = useRouter()
const id = useIdentity()

const fmt = (v) => (v == null ? '–' : Number(v).toLocaleString('en-US'))
const support = computed(() => props.store.support.value)
const all = computed(() => props.store.queues.value || [])

// ---- which rows, in which order ----
// Declared or implicit: offered only when the list holds both.
const tier = ref('')
const tiers = computed(() => {
  const d = all.value.filter((q) => q.declared === true).length
  const i = all.value.filter((q) => q.declared === false).length
  return d && i ? [{ value: '', label: 'Every tier' }, { value: 'declared', label: 'Declared', count: fmt(d) }, { value: 'implicit', label: 'Implicit', count: fmt(i) }] : null
})
const sort = ref(null) // { key, dir, at, rank } frozen at the click
function sortBy(key) {
  const next = nextSort(sort.value, key)
  sort.value = next ? { ...next, at: Date.now(), rank: snapshotOrder(all.value, next) } : null
}
const arrow = (key) => (sort.value?.key === key ? (sort.value.dir === 'desc' ? '↓' : '↑') : '')
const ariaSort = (key) => (sort.value?.key === key ? (sort.value.dir === 'desc' ? 'descending' : 'ascending') : 'none')
const list = computed(() => listRows(all.value, { find: props.find, tier: tiers.value ? tier.value : '', rank: sort.value?.rank || null, max: MAX_ROWS }))
const rows = computed(() => list.value.rows.map((q) => ({
  name: q.name,
  tier: q.declared === true ? 'declared' : q.declared === false ? 'implicit' : '–',
  tierTitle: q.declared === true
    ? 'Declared with configure: the configuration is durable, the contents are not.'
    : q.declared === false ? 'Created by the first push or pop that named it; collected once empty and idle.' : '',
  depth: q.depth,
  bytes: q.bytes,
  partitions: q.partitions,
  groups: q.groupCount,
  dropped: droppedOf(q),
  dropTitle: `Dropped: ${fmt(q.drops?.bounds)} over its bounds, ${fmt(q.drops?.ttl)} past their age, ${fmt(q.drops?.retry)} out of tries.`,
})))
const orderLine = computed(() => {
  const s = sort.value
  const cut = list.value.matched > rows.value.length ? `The first ${fmt(rows.value.length)} of ${fmt(list.value.matched)}` : ''
  if (!s) return cut ? `${cut}, by name; filter to narrow.` : ''
  const how = `by ${EPH_SORTS[s.key].label} as it stood at ${clock(s.at)}, ${s.dir === 'desc' ? 'largest' : 'smallest'} first`
  return cut ? `${cut}, ${how}.` : `Sorted ${how}. Click again to change.`
})
const error = computed(() => (props.store.error.value && !all.value.length && support.value !== 'absent' && support.value !== 'gated'
  ? describeApiError(props.store.error.value) : null))
const stale = computed(() => (props.store.isStale?.value && all.value.length ? { error: describeApiError(props.store.error.value), at: props.store.lastFetched.value } : null))
const checkAgain = () => props.store.fetchQueues?.({ probe: true })

// ---- the pick (?name=): the highlight and the right side change, never the list ----
const picked = computed(() => (typeof route.query.name === 'string' ? route.query.name : ''))
const pickedRow = computed(() => all.value.find((q) => q.name === picked.value) || null)
const at = (name) => ({ path: route.path, query: { ...route.query, name } })
const side = ref(null)
const stacked = () => window.matchMedia?.('(max-width: 1180px)').matches
async function pick(name) {
  const off = name === picked.value
  await router.replace({ path: route.path, query: { ...route.query, name: off ? undefined : name } })
  if (!off && stacked()) { await nextTick(); side.value?.scrollIntoView?.({ block: 'start', behavior: 'smooth' }) }
}
function go(r, e) {
  if (window.getSelection?.()?.toString()) return
  if (e.target.closest?.('a')) {
    // The name is a link for a new tab; a plain click picks, as the row does.
    if (e.metaKey || e.ctrlKey || e.shiftKey) return
    e.preventDefault()
  }
  pick(r.name)
}
async function close() {
  const was = picked.value
  await router.replace({ path: route.path, query: { ...route.query, name: undefined } })
  if (was && stacked()) { await nextTick(); [...document.querySelectorAll('.qse-table [data-q]')].find((e) => e.dataset.q === was)?.scrollIntoView?.({ block: 'center' }) }
}
function refreshList() {
  props.store.invalidate?.()
  props.store.fetchQueues?.({ force: true })
}
function deleted() { close(); refreshList() }
// Leaving the filter (same page, another segment) takes the pick with it. The
// segment's own address change is still queued at this point, and
// replaceQueryFields folds both into one replace. On a page that is already
// another one, the route has moved on and nothing is touched.
onBeforeUnmount(() => { if (route.path === '/queues' && route.query.name) replaceQueryFields(router, route, { name: null }) })
// Another cluster: the picked queue was this one's.
const offCluster = onClusterChange(() => { if (picked.value) replaceQueryFields(router, route, { name: null }) })
onBeforeUnmount(offCluster)

// The cluster the dialogs name: the page's, or the identity's (the shell's rule).
const where = computed(() => {
  if (props.where) return props.where
  const c = id.actingCluster.value
  return id.standalone.value ? (id.actingCellSlug.value || c?.slug || 'broker') : (c?.slug || c?.name || 'no cluster')
})
</script>

<template>
  <div class="qse">
    <p class="qn-t2 qse-lead">Ephemeral queues live in memory on their owner node and are never written to disk. If that node crashes, what waits there is lost. No dead letters, no replay.</p>
    <p v-if="support === 'absent'" class="qn-t2 qse-empty">This broker has no ephemeral queues: it is older than the release that brought them. <button type="button" class="qn-link" @click="checkAgain">Check again</button></p>
    <p v-else-if="support === 'gated'" class="qn-t2 qse-empty">This cluster's plan does not include ephemeral queues. <button type="button" class="qn-link" @click="checkAgain">Check again</button></p>
    <p v-else-if="error" class="qn-t2 qse-empty"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ error }}</p>
    <p v-else-if="support === 'unknown' && !all.length" class="qn-t2 qse-empty">Reading the ephemeral queues…</p>
    <p v-else-if="!all.length && !picked" class="qn-t2 qse-empty">No ephemeral queues here. One appears on its first push, or when an app declares it.</p>
    <div v-else class="qse-split">
      <section class="qse-list" aria-label="Ephemeral queues">
        <p v-if="stale" class="qn-t2 qse-note"><span class="qn-mark amber" aria-hidden="true" />The last read failed ({{ stale.error }}); these rows are from {{ clock(stale.at) }}.</p>
        <QcSeg v-if="tiers" v-model="tier" :options="tiers" label="Tier" class="qse-tiers" />
        <p v-if="!all.length" class="qn-t2 qse-empty">No ephemeral queues here now.</p>
        <p v-else-if="!rows.length" class="qn-t2 qse-empty">
          Nothing matches<template v-if="find.trim()"> “{{ find.trim() }}”</template><template v-if="tiers && tier"> among the {{ tier }} queues</template>.
          <button v-if="find.trim()" type="button" class="qn-link" @click="emit('clear')">Clear the filter</button>
          <button v-else type="button" class="qn-link" @click="tier = ''">Show every tier</button>
        </p>
        <div v-else class="qse-scroll">
          <div class="qse-table" role="table" aria-label="Ephemeral queues" style="--cols: minmax(0, 2.4fr) minmax(64px, 1fr) 84px 92px 82px 64px 84px">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Queue</span><span role="columnheader">Tier</span>
              <span v-for="h in [['depth', 'Waiting'], ['bytes', 'In memory']]" :key="h[0]" role="columnheader" class="r qse-sorthead" :aria-sort="ariaSort(h[0])">
                <button type="button" class="qse-sort" :class="{ on: sort && sort.key === h[0] }" @click="sortBy(h[0])"><span class="qse-arrow" aria-hidden="true">{{ arrow(h[0]) }}</span>{{ h[1] }}</button>
              </span>
              <span role="columnheader" class="r">Partitions</span><span role="columnheader" class="r">Groups</span>
              <span role="columnheader" class="r qse-sorthead" :aria-sort="ariaSort('dropped')">
                <button type="button" class="qse-sort" :class="{ on: sort && sort.key === 'dropped' }" @click="sortBy('dropped')"><span class="qse-arrow" aria-hidden="true">{{ arrow('dropped') }}</span>Dropped</button>
              </span>
            </div>
            <div
              v-for="r in rows" :key="r.name" class="qn-tr click" :class="{ on: r.name === picked }" role="row" :data-q="r.name"
              :aria-selected="r.name === picked ? 'true' : undefined" @click="go(r, $event)"
            >
              <span role="rowheader"><RouterLink :to="at(r.name)" replace class="qn-mono" :class="{ strong: r.name === picked }" :title="r.name">{{ r.name }}</RouterLink></span>
              <span role="cell" class="qn-t2" :title="r.tierTitle || undefined">{{ r.tier }}</span>
              <span role="cell" class="r qn-num" :class="{ 'qn-t3': !r.depth }">{{ fmt(r.depth) }}</span>
              <span role="cell" class="r qn-num" :class="{ 'qn-t3': !r.bytes }">{{ size(r.bytes) }}</span>
              <span role="cell" class="r qn-num" :class="{ 'qn-t3': !r.partitions }">{{ fmt(r.partitions) }}</span>
              <span role="cell" class="r qn-num" :class="{ 'qn-t3': !r.groups }">{{ fmt(r.groups) }}</span>
              <span role="cell" class="r qn-num" :class="{ 'qn-t3': !r.dropped }" :title="r.dropTitle">{{ fmt(r.dropped) }}</span>
            </div>
          </div>
        </div>
        <p v-if="orderLine && rows.length" class="qn-t3 qse-order">{{ orderLine }}</p>
        <p class="qn-read qse-read">
          Read from <span class="qn-mono">GET /api/v1/ephemeral/queues</span>, on the node that answered: each node counts only the partitions it holds.
          Waiting is the length of each ring.
        </p>
      </section>

      <div ref="side" class="qse-side">
        <EphemeralDetail v-if="picked" :name="picked" :row="pickedRow" :where="where" @close="close" @changed="refreshList" @deleted="deleted" />
        <div v-else-if="rows.length" class="qn-panel qse-hint">
          <p class="qn-t2">Pick a queue to see its partitions and groups, what it drops, and what survives a deploy or a crash.</p>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.qse-lead { max-width: 760px; margin-top: 18px; }
.qse-empty { padding: 16px 0 4px; }
.qse-note { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
.qse-tiers { margin-bottom: 10px; }
.qse-split { display: grid; grid-template-columns: minmax(0, 1fr) minmax(340px, 440px); gap: 24px 28px; align-items: start; margin-top: 14px; }
.qse-list { min-width: 0; }
.qse-scroll { overflow-x: auto; }
.qse-table { min-width: 700px; }
.qse-table a { display: block; overflow: hidden; text-overflow: ellipsis; }
.qse-table a.strong { font-weight: 500; }
/* a flex cell: an inline button whose first item is the (empty) arrow would sit on its middle, not on the text */
.qse-sorthead { display: flex; align-items: center; justify-content: flex-end; }
.qse-sort { display: inline-flex; align-items: center; gap: 4px; color: var(--text3); font-size: 12px; }
.qse-sort:hover, .qse-sort.on { color: var(--text); }
.qse-arrow { display: inline-block; width: 10px; text-align: center; }
.qse-order { font-size: 12px; margin-top: 10px; }
.qse-read { margin-top: 12px; }
/* The detail stays in view while the list scrolls under it. */
.qse-side { min-width: 0; position: sticky; top: 72px; max-height: calc(100vh - 96px); overflow-y: auto; }
.qse-hint { padding: 16px 18px; }
@media (max-width: 1180px) {
  .qse-split { grid-template-columns: minmax(0, 1fr); }
  .qse-side { position: static; max-height: none; overflow: visible; scroll-margin-top: 16px; }
  .qse-hint { display: none; }
}
/* A phone: the name, what waits and what it dropped; the detail has the rest. */
@media (max-width: 640px) {
  .qse-table { min-width: 0; --cols: minmax(0, 1fr) 72px 72px !important; }
  .qse-table .qn-tr > :nth-child(2), .qse-table .qn-tr > :nth-child(4), .qse-table .qn-tr > :nth-child(5), .qse-table .qn-tr > :nth-child(6) { display: none; }
}
</style>
