<script setup>
// The Workers view of the Queues section: the worker processes that publish
// their status into Queen KV (Laravel supervisors, SDK consumers with
// supervision on), one row per pool under its application, with its workers
// against their target, the instances that run it and what its queue holds.
// A row opens it on the right; nothing re-sorts by itself.
//   ?wq=     the filter: an application, host, engine, pool, queue or group
//   ?ws=     the segment
//   ?queue=  only the pools that read this queue (old /supervisors?queue= links)
//   ?wo=     what the right side shows: app:<name>, pool:<id>, host:<slot>
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import QcSeg from '@/console/QcSeg.vue'
import QcSpark from '@/console/QcSpark.vue'
import QcPopover from '@/console/QcPopover.vue'
import { useRouteState } from '@/composables/useRouteState'
import { acksText } from '@/composables/queueOps'
import { useQueuesStore } from '@/stores/queuesStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { sparkValues } from './queuesView'
import { useWorkers } from './useWorkers'
import WorkersDetail from './WorkersDetail.vue'
import {
  NAMESPACE, SEGMENTS, SEGMENT_VALUES, appDetail, appId, appRows, fmt, hms, instanceDetail, listCall,
  poolDetail, segmentCounts, sourceError, workersVerdict,
} from './workersView'

defineProps({ where: { type: String, default: '' } })

const w = useWorkers()
const qs = useQueuesStore()
const os = useQueueOpsStore()

// ---- the address ----
const find = ref('')
const seg = ref('all')
const queue = ref('')
const open = ref('')
useRouteState({ wq: find, ws: seg, queue, wo: open })
watch(seg, (v) => { if (!SEGMENT_VALUES.includes(v)) seg.value = 'all' }, { immediate: true })

// ---- the list ----
const expanded = reactive(new Set())
const apps = computed(() => appRows(w.groups.value, { queue: queue.value, find: find.value, seg: seg.value, expanded }))
const counts = computed(() => segmentCounts(w.groups.value, { queue: queue.value }))
const segments = computed(() => SEGMENTS.map((s) => ({ ...s, count: w.loaded.value ? fmt(counts.value[s.value]) : null })))
const verdict = computed(() => workersVerdict({
  groups: w.groups.value, loaded: w.loaded.value, failure: w.failure.value,
  refusal: w.loaded.value ? null : w.stable.value, queue: queue.value,
}))

// What each pool's queue holds and took, from the stores the Queues section refreshes.
const queueRows = computed(() => new Map((qs.queues.value || []).map((q) => [q.name, q])))
function waiting(name) {
  const q = name ? queueRows.value.get(name) : null
  const v = q?.messages?.pending
  return v == null ? { text: '–', dim: true } : { text: fmt(v), dim: v === 0 }
}
function spark(name) {
  if (!name) return null
  const v = sparkValues(os.series.value?.get(name)?.push)
  return v.length > 1 && v.some((x) => x > 0) ? { values: v, max: Math.max(6, ...v), total: v.reduce((a, b) => a + b, 0) } : null
}
const table = computed(() => apps.value.map((a) => ({
  ...a,
  rows: a.pools.map((p) => ({
    ...p,
    workers: p.running == null ? '–' : `${fmt(p.running)} / ${fmt(p.desired)}`,
    instances: p.ongoing ? (p.reporting === p.ongoing ? fmt(p.ongoing) : `${fmt(p.reporting)} of ${fmt(p.ongoing)}`) : '–',
    waiting: waiting(p.queueName),
    spark: spark(p.queueName),
  })),
})))
const cols = computed(() => (queue.value
  ? 'minmax(0, 1.5fr) minmax(0, 1.3fr) 84px 84px'
  : 'minmax(0, 1.55fr) minmax(0, 1.4fr) minmax(0, 1.15fr) 68px 76px 68px 96px'))
const qlink = (name) => `/queues/${encodeURIComponent(name)}`

// ---- the right side ----
const groupByName = computed(() => new Map(w.groups.value.map((g) => [g.name, g])))
const slotGroup = computed(() => new Map(w.rows.value.map((r) => [r.slot, r.group])))
const detail = computed(() => {
  const id = open.value
  if (!id) return null
  if (id.startsWith('app:')) return appDetail(groupByName.value.get(id.slice(4)))
  if (id.startsWith('host:')) {
    const slot = id.slice(5)
    return instanceDetail(groupByName.value.get(slotGroup.value.get(slot)), slot)
  }
  if (id.startsWith('pool:')) {
    for (const g of w.groups.value) {
      if (!id.startsWith(`pool:${g.name}/`)) continue
      const d = poolDetail(g, id)
      if (d) return d
    }
  }
  return null
})
const litApp = computed(() => {
  const id = open.value
  if (id.startsWith('app:')) return id.slice(4)
  if (id.startsWith('host:')) return slotGroup.value.get(id.slice(5)) || ''
  return ''
})
// With ?queue=, its numbers once, on the line that names it (its rows hide them).
const queueNow = computed(() => {
  if (!queue.value) return ''
  const q = queueRows.value.get(queue.value)
  const h = os.ops.value?.get(queue.value)
  const parts = []
  if (q?.messages?.pending != null) parts.push(`${fmt(q.messages.pending)} waiting now`)
  if (h) parts.push(`${fmt(h.pushMessages)} arrived in the last hour`)
  return parts.join(', ')
})
// The pool's queue, the last hour: once here, not on its row.
const queueLine = computed(() => {
  const d = detail.value
  if (d?.kind !== 'pool' || !d.queueName) return null
  const q = queueRows.value.get(d.queueName)
  const h = os.ops.value?.get(d.queueName)
  if (!q && !h) return null
  let line = 'The last hour did not load.'
  if (h && !h.pushMessages && !h.ackSuccess && !h.ackFailed) line = 'Nothing arrived or was acked.'
  else if (h) line = `${fmt(h.pushMessages)} arrived and ${fmt(h.ackSuccess)} were acked; ${acksText(h)}.`
  const dead = q?.messages?.deadLetter || 0
  return { line, dead: dead ? `${fmt(dead)} dead ${dead === 1 ? 'letter' : 'letters'}` : '' }
})
const back = ref(null)
const label = (d) => (d ? d.name || d.title : '')
const panel = ref(null)
function pick(id) {
  back.value = null
  open.value = open.value === id ? '' : id
  reveal()
}
// From the right side to a related one; "back" returns without leaving a way forward.
function follow(id) {
  back.value = back.value?.id === id || !detail.value ? null : { id: detail.value.id, label: label(detail.value) }
  open.value = id
  reveal()
}
function close() { open.value = ''; back.value = null }
// Stacked (a narrow window, a phone), the right side opens under the list: bring it up.
function reveal() {
  if (!open.value || !window.matchMedia?.('(max-width: 1335px)').matches) return
  nextTick(() => panel.value?.$el?.scrollIntoView?.({ block: 'start', behavior: 'smooth' }))
}
function onKey(e) {
  if (e.key !== 'Escape' || !open.value || e.defaultPrevented) return
  if (/^(INPUT|TEXTAREA|SELECT)$/.test(e.target?.tagName || '')) return
  close()
}
onMounted(() => document.addEventListener('keydown', onKey))
onBeforeUnmount(() => document.removeEventListener('keydown', onKey))
function go(id, e) {
  if (e.target.closest?.('a, button') || window.getSelection?.()?.toString()) return
  pick(id)
}
function clearFilter() { find.value = ''; seg.value = 'all' }

// ---- the source: another namespace, or one publication group ----
const sourceOpen = ref(false)
const nsDraft = ref(NAMESPACE)
const groupDraft = ref('')
const srcError = ref('')
watch(sourceOpen, (o) => { if (o) { nsDraft.value = w.namespace.value; groupDraft.value = w.group.value; srcError.value = '' } })
function applySource(done) {
  const ns = nsDraft.value.trim(); const g = groupDraft.value.trim()
  srcError.value = sourceError({ namespace: ns, group: g })
  if (srcError.value) return
  close()
  w.setSource(ns, g)
  done()
}
const call = computed(() => listCall({ namespace: w.namespace.value, group: w.group.value }))
const custom = computed(() => w.namespace.value !== NAMESPACE || !!w.group.value)
const instances = computed(() => w.rows.value.length)
</script>

<template>
  <div class="wk">
    <!-- is any worker wrong -->
    <div class="wk-verdict">
      <template v-if="verdict.kind === 'issues'">
        <span class="qn-mark" :class="verdict.tone" aria-hidden="true" />
        <p class="wk-vtext">
          <button type="button" class="qn-mono wk-vname" @click="pick(appId(verdict.names[0]))">{{ verdict.names[0] }}</button>
          <template v-if="verdict.names[1]">{{ verdict.more ? ', ' : ' and ' }}<button type="button" class="qn-mono wk-vname" @click="pick(appId(verdict.names[1]))">{{ verdict.names[1] }}</button></template>
          <template v-if="verdict.more"> and {{ verdict.more }} more</template>
          {{ verdict.count === 1 ? 'needs' : 'need' }} you.
          <span v-if="verdict.others" class="qn-t2">Nothing else does.</span>
        </p>
      </template>
      <p v-else-if="verdict.kind === 'fine'" class="wk-vtext">Every pool runs at its target. <span class="qn-t2 qn-num">{{ verdict.tail }}</span></p>
      <p v-else-if="verdict.kind === 'calm'" class="wk-vtext">No pool needs you: {{ verdict.why }}. <span class="qn-t2 qn-num">{{ verdict.tail }}</span></p>
      <p v-else-if="verdict.kind === 'stopped'" class="wk-vtext">Every instance that reported here has stopped. <span class="qn-t2">Their last reports stay until they expire.</span></p>
      <template v-else-if="verdict.kind === 'empty' && verdict.queue">
        <p class="wk-vtext">No pool that reports here reads <span class="qn-mono">{{ verdict.queue }}</span>. <button type="button" class="qn-link" @click="queue = ''">See every pool</button></p>
      </template>
      <div v-else-if="verdict.kind === 'empty'" class="wk-vtext">
        <p v-if="custom">
          No worker<template v-if="w.group.value"> of <span class="qn-mono">{{ w.group.value }}</span></template> publishes its status in <span class="qn-mono">{{ w.namespace.value }}</span> yet.
        </p>
        <p v-else>No worker publishes its status here yet.</p>
        <p class="qn-t2 wk-empty">
          Each worker pool would appear here with its workers against their target and how old its last report is.
          A Laravel supervisor publishes it with <span class="qn-mono wk-nb">QUEEN_SUPERVISOR_REMOTE_STATUS=true</span>, an SDK consumer when its consume sets
          <span class="qn-mono wk-nb">supervision</span>, both into the <span class="qn-mono wk-nb">{{ NAMESPACE }}</span> KV namespace.
        </p>
      </div>
      <template v-else-if="verdict.kind === 'refused'">
        <span class="qn-mark amber" aria-hidden="true" />
        <p class="wk-vtext">
          <template v-if="verdict.refusal === 'absent'">This broker does not serve the KV listing, so the workers' reports cannot be read here.</template>
          <template v-else-if="verdict.refusal === 'gated'">This cluster's plan does not include KV, where workers publish their reports.</template>
          <template v-else>The broker answers 503 for KV right now, an operator's switch or a cluster without a leader, so the reports cannot be read.</template>
          <button type="button" class="qn-link" style="margin-left: 6px" @click="w.checkAgain()">Check again</button>
        </p>
      </template>
      <template v-else-if="verdict.kind === 'failed'">
        <span class="qn-mark red" aria-hidden="true" />
        <p class="wk-vtext">The broker did not answer: {{ verdict.text }}{{ /[.!?]$/.test(verdict.text) ? '' : '.' }} <span class="qn-t2">It asks again every 30 s.</span></p>
      </template>
      <p v-else class="wk-vtext qn-t2">Reading the reports workers publish…</p>
    </div>

    <div v-if="w.loaded.value || w.error.value" class="wk-tools">
      <template v-if="w.groups.value.length">
        <label class="qn-field wk-find">
          <svg width="14" height="14" viewBox="0 0 18 18" aria-hidden="true" style="fill: none; stroke: currentColor; stroke-width: 1.4"><circle cx="7.5" cy="7.5" r="5" /><line x1="11.3" y1="11.3" x2="15.5" y2="15.5" /></svg>
          <span class="qn-vh">Filter the pools</span>
          <input v-model="find" type="text" placeholder="Filter by application, host, pool or queue" spellcheck="false" autocomplete="off" @keydown.esc="find = ''" />
        </label>
        <QcSeg v-model="seg" :options="segments" label="Show" />
      </template>
      <div v-if="w.groups.value.length" class="wk-gap" />
      <!-- where the reports are read: the namespace, or one publication group of it -->
      <QcPopover v-model:open="sourceOpen" side="bottom-end" :width="340" label="Where the reports are read">
        <template #trigger="{ toggle }">
          <button type="button" class="qn-chip wk-srcchip" aria-haspopup="dialog" :aria-expanded="sourceOpen" title="The KV namespace the workers publish to: read another, or one publication group" @click="toggle">
            <span class="qn-t3">KV</span>
            <span class="qn-mono wk-srcname">{{ w.namespace.value }}<template v-if="w.group.value"> / {{ w.group.value }}</template></span>
            <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><polyline points="2,3.5 5,6.5 8,3.5" style="fill: none; stroke: currentColor; stroke-width: 1.3" /></svg>
          </button>
        </template>
        <template #default="{ close: done }">
          <form class="wk-srcform" @submit.prevent="applySource(done)">
            <label class="wk-srcl">
              <span class="qn-t3">KV namespace</span>
              <span class="qn-field"><input v-model="nsDraft" class="qn-mono" type="text" maxlength="64" spellcheck="false" autocomplete="off" /></span>
            </label>
            <label class="wk-srcl">
              <span class="qn-t3">Publication group, optional</span>
              <span class="qn-field"><input v-model="groupDraft" class="qn-mono" type="text" maxlength="255" placeholder="Every group" spellcheck="false" autocomplete="off" /></span>
            </label>
            <p class="qn-t3 wk-srchint">Laravel names its group with <span class="qn-mono">remote_status.key</span>; an SDK consumer with its supervision group. Reading one group reads less.</p>
            <p v-if="srcError" class="red wk-srchint" role="alert">{{ srcError }}</p>
            <div class="wk-srcbtns">
              <button v-if="custom" type="button" class="qn-btn3" @click="nsDraft = NAMESPACE; groupDraft = ''">Back to {{ NAMESPACE }}</button>
              <button type="submit" class="qn-btn">Read</button>
            </div>
          </form>
        </template>
      </QcPopover>
    </div>

    <template v-if="w.loaded.value && w.groups.value.length">

      <p v-if="queue" class="wk-qnote">
        Only the pools that read <RouterLink :to="qlink(queue)" class="qn-mono qn-link">{{ queue }}</RouterLink><span v-if="queueNow" class="qn-t2 qn-num">: {{ queueNow }}</span>.
        <button type="button" class="qn-link qn-t2" style="margin-left: 6px" @click="queue = ''">See every pool</button>
      </p>
      <p v-if="w.error.value" class="qn-t2 wk-note">
        <span class="qn-mark amber" aria-hidden="true" />The last read failed ({{ w.failure.value }}); this is what the workers reported at {{ hms(w.readAt.value) }}.
      </p>

      <div class="wk-split" :class="{ open: !!open }">
        <section class="wk-list" aria-label="Worker pools">
          <p v-if="!table.length" class="qn-t2 wk-none">
            Nothing matches{{ find.trim() ? ` “${find.trim()}”` : '' }}{{ seg !== 'all' ? ` in ${SEGMENTS.find((s) => s.value === seg).label}` : '' }}.
            <button type="button" class="qn-link" @click="clearFilter">Show every pool</button>
          </p>
          <div v-else class="wk-scroll">
            <div class="wk-table" :class="{ narrow: !!queue }" role="table" aria-label="Worker pools by application" :style="{ '--cols': cols }">
              <div role="rowgroup">
                <div class="qn-tr head" role="row">
                  <span role="columnheader" class="wk-phead">Pool</span>
                  <span v-if="!queue" role="columnheader">Queue</span>
                  <span role="columnheader">State</span>
                  <span role="columnheader" class="r" title="Running / wanted, added up over the instances that report now">Workers</span>
                  <span role="columnheader" class="r" title="The instances that run it and report now">Instances</span>
                  <span v-if="!queue" role="columnheader" class="r" title="Messages waiting in its queue now">Waiting</span>
                  <span v-if="!queue" role="columnheader" class="wk-sparkhead">In, last hour</span>
                </div>
              </div>

              <div v-for="a in table" :key="a.name" role="rowgroup">
                <div class="qn-tr wk-app" :class="{ on: litApp === a.name }" role="row">
                  <span role="rowheader" class="wk-appcell">
                    <button type="button" class="wk-appbtn" :aria-label="`Open ${a.name}: its figures and its instances`" @click="pick(appId(a.name))">
                      <span class="wk-markslot" aria-hidden="true"><span v-if="a.tone" class="qn-mark" :class="a.tone" /></span>
                      <span class="qn-mono wk-appname">{{ a.name }}</span>
                      <span class="qn-t3 wk-appfact wk-kind">{{ a.kindText }}</span>
                      <span class="qn-t3 wk-appfact qn-num">{{ a.instances }}</span>
                      <span v-for="i in a.issues" :key="i" class="amber wk-appfact qn-num">{{ i }}</span>
                      <span v-if="a.heard" class="qn-t3 wk-appfact wk-heard qn-num">{{ a.heard }}</span>
                    </button>
                  </span>
                </div>

                <div
                  v-for="p in a.rows" :key="p.id" class="qn-tr click wk-row" :class="{ on: open === p.id }" role="row"
                  tabindex="0" @click="go(p.id, $event)" @keydown.enter.prevent="pick(p.id)"
                >
                  <span role="rowheader" class="wk-name">
                    <span class="wk-markslot" aria-hidden="true"><span v-if="p.tone" class="qn-mark" :class="p.tone" /></span>
                    <span class="wk-namecol">
                      <span class="qn-mono wk-pool" :class="{ strong: p.tone }" :title="p.consumer ? `Consumer group ${p.name}, an SDK consume with supervision` : `Pool ${p.name}`">{{ p.name }}</span>
                      <span v-if="!queue" class="qn-mono qn-t3 wk-qsub">{{ p.queue }}</span>
                    </span>
                  </span>
                  <span v-if="!queue" role="cell">
                    <RouterLink v-if="p.queueName" :to="qlink(p.queueName)" class="qn-mono wk-qlink" :title="p.queueName">{{ p.queueName }}</RouterLink>
                    <span v-else class="qn-mono qn-t2" :title="`${p.queue}: every queue of that namespace and task`">{{ p.queue }}</span>
                  </span>
                  <span role="cell" :class="p.tone || 'qn-t2'" :title="p.next || undefined">{{ p.word }}<span v-if="p.note" class="qn-t3"> · {{ p.note }}</span></span>
                  <span role="cell" class="r qn-num" :class="{ 'qn-t3': p.running == null }">{{ p.workers }}</span>
                  <span role="cell" class="r qn-num" :class="{ 'qn-t3': !p.ongoing }">{{ p.instances }}</span>
                  <span v-if="!queue" role="cell" class="r qn-num" :class="{ 'qn-t3': p.waiting.dim }">{{ p.waiting.text }}</span>
                  <span v-if="!queue" role="cell" class="wk-sparkcell">
                    <QcSpark v-if="p.spark" :values="p.spark.values" :max="p.spark.max" :width="92" :height="20" :label="`${fmt(p.spark.total)} arrived in ${p.queueName} in the last hour`" />
                  </span>
                </div>

                <div
                  v-for="o in a.orphans" :key="o.id" class="qn-tr click wk-row wk-orphan" :class="{ on: open === o.id }" role="row"
                  tabindex="0" @click="go(o.id, $event)" @keydown.enter.prevent="pick(o.id)"
                >
                  <span role="cell" class="wk-name wk-span">
                    <span class="wk-markslot" aria-hidden="true"><span v-if="o.tone" class="qn-mark" :class="o.tone" /></span>
                    <span class="qn-mono qn-t2">{{ o.host }}</span>
                    <span :class="o.tone || 'qn-t2'">{{ o.word }}</span>
                    <span class="qn-t3 wk-why">{{ o.next }}</span>
                  </span>
                </div>

                <div v-if="a.hidden" class="qn-tr wk-more" role="row">
                  <span role="cell" class="wk-span"><button type="button" class="qn-link qn-t2" @click="expanded.add(a.name)">Show the other {{ fmt(a.hidden) }} pools of {{ a.name }}</button></span>
                </div>
              </div>
            </div>
          </div>

          <p v-if="w.partial.value" class="qn-t2 wk-note">
            These are the first {{ fmt(instances) }} instances: more reports are stored.
            <button type="button" class="qn-link" :disabled="w.loading.value" @click="w.readMore()">Read more</button>
          </p>
          <p class="qn-read wk-read">
            Read from <span class="qn-mono">{{ call.method }} {{ call.path }}</span>, expired reports included<template v-if="!w.error.value">, at {{ hms(w.readAt.value) }}</template>;
            each queue's hour from <span class="qn-mono">GET /api/v1/analytics/workload?groupBy=queue</span>.
            Workers are running / wanted, on the instances that report now. Rows stay in name order.
          </p>
        </section>

        <WorkersDetail
          v-if="open"
          ref="panel"
          class="wk-detail"
          :d="detail"
          :queue="queueLine"
          :namespace="w.namespace.value"
          :back="back"
          @open="follow"
          @close="close"
        />
      </div>
    </template>
  </div>
</template>

<style scoped>
.wk-verdict { display: flex; align-items: flex-start; gap: 10px; font-size: 15px; margin-top: 18px; }
/* the mark sits on the first line, however the sentence wraps */
.wk-verdict > .qn-mark { margin-top: 7px; }
.wk-vtext { flex: 1 1 auto; min-width: 0; overflow-wrap: anywhere; }
.wk-vname { font-weight: 500; text-decoration: underline; text-decoration-color: var(--line); text-underline-offset: 3px; }
.wk-vname:hover { text-decoration-color: currentColor; }
.wk-empty { margin-top: 8px; max-width: 820px; font-size: 13.5px; line-height: 1.55; }
.wk-nb { white-space: nowrap; }
.wk-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px; margin-top: 22px; }
.wk-find { width: 320px; max-width: 100%; }
.wk-qnote { margin-top: 14px; overflow-wrap: anywhere; }
.wk-note { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 8px; margin-top: 14px; }
.wk-none { padding: 18px 0 4px; }

/* the list, and what is picked on its right; stacked below 1336 px, where the table would be cut */
.wk-split { display: grid; grid-template-columns: minmax(0, 1fr); gap: 24px 28px; margin-top: 16px; align-items: start; }
.wk-split.open { grid-template-columns: minmax(0, 1fr) 440px; }
.wk-list { min-width: 0; }
.wk-detail { position: sticky; top: 64px; max-height: calc(100vh - 80px); overflow-y: auto; }
.wk-scroll { overflow-x: auto; }
.wk-table { min-width: 760px; }
/* one queue: four columns, which would sit far apart across the page */
.wk-table.narrow { min-width: 520px; max-width: 980px; }
.wk-phead { padding-left: 18px; }
.wk-sparkhead { padding-left: 4px; }

/* an application: one row that opens it on the right */
.qn-tr.wk-app { min-height: 42px; border-bottom-color: var(--line); }
.wk-appcell { grid-column: 1 / -1; }
.wk-appbtn { display: flex; flex-wrap: wrap; align-items: center; gap: 2px 0; width: 100%; min-height: 42px; padding: 10px 8px 10px 0; text-align: left; }
.wk-appbtn .wk-markslot { margin-right: 10px; }
.wk-appname { font-weight: 500; color: var(--text); margin-right: 12px; }
.wk-appbtn:hover .wk-appname { text-decoration: underline; text-underline-offset: 3px; }
.wk-appfact { white-space: nowrap; }
.wk-appfact + .wk-appfact::before { content: '·'; color: var(--text3); margin: 0 7px; }
.qn-tr.wk-app.on { background: var(--bg3); }

/* a pool */
.wk-name { display: flex; align-items: center; gap: 10px; }
.wk-markslot { flex: 0 0 8px; display: flex; align-items: center; justify-content: center; }
.wk-namecol { display: flex; flex-direction: column; min-width: 0; }
.wk-pool { overflow: hidden; text-overflow: ellipsis; }
.wk-pool.strong { font-weight: 500; }
.wk-qsub { display: none; font-size: 11.5px; overflow: hidden; text-overflow: ellipsis; }
.wk-qlink { overflow: hidden; text-overflow: ellipsis; }
.wk-sparkcell { display: flex; align-items: center; padding-left: 4px; }
.wk-span { grid-column: 1 / -1; }
.wk-orphan .wk-name { gap: 10px; }
.wk-why { overflow: hidden; text-overflow: ellipsis; }
.wk-more { min-height: 34px; }
.wk-more .wk-span { padding-left: 18px; }

.wk-read { margin-top: 14px; max-width: 980px; }
.wk-gap { flex: 1 1 0; }
.wk-srcchip { max-width: 100%; }
.wk-srcname { overflow: hidden; text-overflow: ellipsis; }
.wk-srcform { display: grid; gap: 10px; padding: 8px 6px 4px; }
.wk-srcl { display: grid; gap: 4px; font-size: 12px; }
.wk-srchint { font-size: 12px; }
.wk-srcbtns { display: flex; justify-content: flex-end; gap: 8px; margin-top: 2px; }

/* tone: no hairlines, so an application is a filled row */
:global(.qn.tone .wk-table .wk-app) { background: var(--bg2); border-radius: var(--r-ctl); margin-top: 6px; }
:global(.qn.tone .wk-table .wk-app.on) { background: var(--bg3); }
/* the filled row insets its mark; every other row moves with it, so names line up */
:global(.qn.tone .wk-appbtn) { padding-left: 6px; }
:global(.qn.tone .wk-table .qn-tr:not(.wk-app)) { padding-left: 6px; }

@media (max-width: 1335px) {
  .wk-split.open { grid-template-columns: minmax(0, 1fr); }
  .wk-detail { position: static; max-height: none; overflow: visible; }
}
/* a phone: the pool with its queue under it, its state and its workers; the right side has the rest */
@media (max-width: 640px) {
  .wk-find { width: 100%; }
  .wk-table, .wk-table.narrow { min-width: 0; --cols: minmax(0, 1fr) minmax(0, 0.9fr) 56px !important; }
  /* every queue: Pool, Queue, State, Workers, … keeps 1, 3, 4; one queue: Pool, State, Workers, Instances keeps 1-3 */
  .wk-table:not(.narrow) .qn-tr.head > :nth-child(2),
  .wk-table:not(.narrow) .qn-tr.wk-row:not(.wk-orphan) > :nth-child(2),
  .wk-table:not(.narrow) .qn-tr.head > :nth-child(n+5),
  .wk-table:not(.narrow) .qn-tr.wk-row:not(.wk-orphan) > :nth-child(n+5),
  .wk-table.narrow .qn-tr.head > :nth-child(n+4),
  .wk-table.narrow .qn-tr.wk-row:not(.wk-orphan) > :nth-child(n+4) { display: none; }
  .wk-qsub { display: block; }
  .wk-why { display: none; }
  /* the application: its name, its instances and what needs you; its engine and heartbeats are on the right */
  .wk-kind, .wk-heard { display: none; }
  .wk-table .qn-tr.wk-row { padding-top: 7px; padding-bottom: 7px; }
  .wk-gap { display: none; }
}
</style>
