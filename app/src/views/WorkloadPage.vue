<script setup>
// Workload, in the new console: who does the work, how much, and what is
// stuck, for the tenant, by namespace, task or queue. The fixed list on the
// left picks one namespace (or task), and the rows become its queues; a
// queue's row opens its consumer groups on the right; the deeper layer is
// read when asked. The address keeps it all (?wby= ?wkey= ?wq= ?wm= ?wt=
// ?wvs= ?wdeep=), with the window as the band sets it (?range=, ?from=&to=).
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ConsoleShell from '@/console/ConsoleShell.vue'
import { useWindow, clock } from '@/console/useWindow'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { useWorkloadPage, FALLBACK_TITLE } from '@/console/useWorkloadPage'
import { COMPARE, MEASURES, TABS, emptyName, parts, readCall, rootName, scopeOf } from '@/console/workload/workloadViews'
import WlSide from '@/console/workload/WlSide.vue'
import WlCounts from '@/console/workload/WlCounts.vue'
import WlWork from '@/console/workload/WlWork.vue'
import WlTable from '@/console/workload/WlTable.vue'
import WlMap from '@/console/workload/WlMap.vue'
import WlGroups from '@/console/workload/WlGroups.vue'
import WlDeeper from '@/console/workload/WlDeeper.vue'

const route = useRoute()
const router = useRouter()
const { win, range } = useWindow()
const frame = useConsoleFrame()

// ---- the address -------------------------------------------------------------------
const one = (v) => (Array.isArray(v) ? v[0] : v)
const arg = (k) => { const v = one(route.query[k]); return typeof v === 'string' ? v : null }
const by = computed(() => (arg('wby') === 'task' || arg('wby') === 'queue' ? arg('wby') : 'namespace'))
const key = computed(() => arg('wkey')) // null: every one; '': the queues without one
const scope = computed(() => scopeOf(by.value, key.value))
const measure = computed(() => (MEASURES.some((m) => m.id === arg('wm')) ? arg('wm') : 'pop'))
const tab = computed(() => (TABS.some((t) => t.id === arg('wt')) ? arg('wt') : 'work'))
const compare = computed(() => (COMPARE.includes(arg('wvs')) ? arg('wvs') : 'previous'))
const deep = computed(() => arg('wdeep') === '1')
const picked = computed(() => (scope.value.level === 'queue' ? arg('wq') || '' : ''))
const search = ref('')

const w = useWorkloadPage({ win, scope, measure, compare, deep, picked, search })

function put(next) { router.replace({ query: { ...route.query, ...next } }) }
function setBy(v) {
  if (v === by.value) return
  search.value = ''
  put({ wby: v === 'namespace' ? undefined : v, wkey: undefined, wq: undefined })
}
function pick(k) { put({ wkey: k === null ? undefined : k, wq: undefined }) }
function openQueue(q) { put({ wq: q && q !== picked.value ? q : undefined }) }
/** A row clicked anywhere on the page: look inside a group, or open a queue's groups. */
function openRow(k) {
  if (w.shownScope.value.level === 'queue') openQueue(k)
  else pick(k)
}
function setMeasure(m) { put({ wm: m === 'pop' ? undefined : m }) }
function setTab(t) { put({ wt: t === 'work' ? undefined : t }) }
function setCompare(m) { put({ wvs: m === 'previous' ? undefined : m }) }
function readDeeper() {
  if (!deep.value) put({ wdeep: '1' })
  else w.readDeeper()
}

// A finding, clicked: where its row is, and its queue's groups when it names one.
function act(f) {
  if (!f?.row) return
  if (f.row.level === 'queue') return openQueue(f.queue || f.key)
  put({ wkey: f.key, wq: f.queue || undefined })
}
const actLabel = (f) => (f.row?.level === 'queue' ? 'Its consumer groups' : 'Look at its queues')

function onKey(e) {
  if (e.key !== 'Escape' || !picked.value || e.defaultPrevented) return
  const t = e.target
  if (t && /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName)) return
  openQueue('')
}
onMounted(() => document.addEventListener('keydown', onKey))
onBeforeUnmount(() => document.removeEventListener('keydown', onKey))

// ---- words --------------------------------------------------------------------------
const pickName = computed(() => (key.value === null ? '' : key.value || emptyName(scope.value.listBy)))
const path = computed(() => (key.value === null
  ? [{ label: 'Workload' }]
  : [{ label: 'Workload', to: { path: '/workload', query: { ...route.query, wkey: undefined, wq: undefined } } }, { label: pickName.value, mono: !!key.value }]))
const v = computed(() => w.verdict.value)
const nowLabel = computed(() => (key.value === null ? 'Now' : 'The tenant, now'))
const winLabel = computed(() => (key.value === null ? w.words.value.label : `The tenant, ${w.words.value.label.toLowerCase()}`))
const partialLine = computed(() => {
  const ids = w.partial.value
  if (!ids.length) return ''
  return ids.length === 1
    ? `Node ${ids[0]} did not answer this read: its traffic is missing from these numbers.`
    : `Nodes ${ids.slice(0, -1).join(', ')} and ${ids[ids.length - 1]} did not answer this read: their traffic is missing from these numbers.`
})
const failedLine = computed(() => (w.error.value && w.payload.value ? `The last read failed (${w.errorText.value}); these numbers are from ${clock(w.readAt.value)}.` : ''))
const loaded = computed(() => !!w.payload.value)
const drawer = computed(() => !!picked.value)
const deadOf = computed(() => w.pickedRow.value?.now?.deadLetter || 0)

// ---- a phone: one picker instead of the list ------------------------------------------
const pickValue = computed(() => (key.value === null ? '*' : `k:${key.value}`))
function pickFrom(value) { pick(value === '*' ? null : value.slice(2)) }
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="wl">
      <header class="wl-head">
        <h1 class="wl-title">Workload</h1>
        <span v-if="w.fallback.value" class="qn-tag" :title="FALLBACK_TITLE">Computed in this browser</span>
      </header>

      <div class="wl-cols">
        <WlSide
          class="wl-side" :by="by" :pick="key" :list="w.list.value" :listed="w.listed.value" :error="w.listError.value"
          :search="search" @by="setBy" @pick="pick" @update:search="search = $event"
        />

        <div class="wl-main">
          <!-- a phone: Group by and one picker -->
          <div class="wl-pickm">
            <div class="qn-seg" role="group" aria-label="Group by">
              <button v-for="o in [{ value: 'namespace', label: 'Namespace' }, { value: 'task', label: 'Task' }, { value: 'queue', label: 'Queue' }]" :key="o.value" type="button" :aria-pressed="by === o.value" @click="setBy(o.value)">{{ o.label }}</button>
            </div>
            <label class="qn-field">
              <span class="qn-t3" style="font-size: 12px">Show</span>
              <select :value="pickValue" aria-label="Which group" @change="pickFrom($event.target.value)">
                <option value="*">{{ rootName(by) }}</option>
                <option v-for="r in w.list.value.rows" :key="r.key" :value="`k:${r.key}`">{{ r.key || emptyName(scope.listBy) }}</option>
              </select>
            </label>
          </div>

          <!-- is anything stuck -->
          <p class="wl-verdict" aria-live="polite">
            <span v-if="v.badge" class="wl-badge" :class="v.tone || 'qn-t2'"><span v-if="v.tone" class="qn-mark" :class="v.tone" aria-hidden="true" />{{ v.badge }}</span>
            <span :class="v.loading ? 'qn-t2' : ''"><template v-for="(p, i) in parts(v.text)" :key="i"><span :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></template></span>
            <span v-if="v.sub" class="qn-t2">{{ v.sub }}</span>
            <button v-if="v.finding?.row" type="button" class="qn-link wl-act" @click="act(v.finding)">{{ actLabel(v.finding) }}</button>
            <RouterLink v-if="v.finding?.dlq" :to="{ path: '/dlq', query: { queue: v.finding.dlq } }" class="qn-link wl-act">Its dead letters</RouterLink>
          </p>
          <section v-if="v.list.length" class="wl-why qn-panel" aria-label="What needs you">
            <ul class="wl-issues">
              <li v-for="(f, i) in v.list" :key="i" class="wl-issue">
                <span class="qn-mark" :class="f.tone" aria-hidden="true" />
                <button type="button" class="wl-itext" :title="actLabel(f)" @click="act(f)">
                  <span><template v-for="(p, k) in parts(f.text)" :key="k"><span :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></template></span>
                  <span v-if="f.evidence" class="qn-t2">{{ ' ' + f.evidence }}</span>
                </button>
                <RouterLink v-if="f.dlq" :to="{ path: '/dlq', query: { queue: f.dlq } }" class="qn-link wl-ilink">Its dead letters</RouterLink>
              </li>
            </ul>
          </section>
          <p v-if="partialLine" class="qn-t2 wl-line">{{ partialLine }}</p>
          <p v-if="failedLine" class="qn-t2 wl-line"><span class="qn-mark red" aria-hidden="true" />{{ failedLine }}</p>
          <p v-if="!w.hasGroups.value && w.groupsError.value && loaded" class="qn-t2 wl-line">
            <span class="qn-mark amber" aria-hidden="true" />The consumer groups did not load ({{ w.groupsError.value }}): what no group reads and who is behind cannot be said.
          </p>

          <div :class="{ 'wl-dim': w.stale.value }">
            <!-- the tenant's counts -->
            <WlCounts
              v-if="w.counts.value" class="wl-sec" :counts="w.counts.value" :now-label="nowLabel" :win-label="winLabel"
              :compare="compare" :compare-label="w.compareLabel.value" :win="win" :prev-error="w.prevError.value" @compare="setCompare"
            />

            <!-- work over time -->
            <WlWork
              class="wl-sec" :model="w.charts.value" :win="win" :measure="w.measureNow.value" :fetched="w.fetchSeen.value"
              :loaded="loaded" :error="w.error.value" :level="w.shownScope.value.level" :bucket-minutes="w.bucketMinutes.value"
              @measure="setMeasure" @zoom="range" @open="openRow"
            />

            <div v-if="loaded" class="wl-lower" :class="{ open: drawer }">
              <!-- the rows -->
              <WlTable
                class="wl-sec wl-table" :rows="w.table.value" :tab="tab" :level="w.shownScope.value.level" :picked="picked"
                :fetched="w.anyFetched.value" :compared="w.prevDrawn.value ? w.compareLabel.value : ''"
                :read-call="readCall(w.shownScope.value)" :in-the="w.words.value.inThe" :has-groups="w.hasGroups.value"
                :groups-error="w.groupsError.value" :compact="drawer" :lines="w.lines.value" :fallback="w.fallback.value" @tab="setTab" @open="openRow"
              />

              <div v-if="drawer" class="wl-drawer">
                <WlGroups :queue="picked" :model="w.groups.value" :has-groups="w.hasGroups.value" :error="w.groupsError.value" :dead="deadOf" @close="openQueue('')" />
              </div>

              <div class="wl-rest">
                <!-- the activity map -->
                <WlMap class="wl-sec" :model="w.map.value" :win="win" :level="w.shownScope.value.level" :picked="picked" :bucket-minutes="w.bucketMinutes.value" @open="openRow" />

                <!-- worth a look -->
                <section v-if="w.found.value.notes.length" class="wl-sec" aria-labelledby="wl-notes-title">
                  <h2 id="wl-notes-title" class="wl-h2">Worth a look</h2>
                  <ul class="wl-notes">
                    <li v-for="(n, i) in w.found.value.notes" :key="i" class="wl-note">
                      <span>
                        <template v-for="(p, k) in parts(n.text)" :key="k"><span :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></template>
                        <span v-if="n.evidence" class="qn-t2">{{ ' ' + n.evidence }}</span>
                      </span>
                      <RouterLink v-if="n.kind === 'dlq' && n.dlq" :to="{ path: '/dlq', query: { queue: n.dlq } }" class="qn-link wl-ilink">See why</RouterLink>
                      <button v-else-if="n.row && n.row.level !== 'queue'" type="button" class="qn-link wl-ilink" @click="act(n)">Look at its queues</button>
                    </li>
                  </ul>
                </section>

                <!-- the deeper layer -->
                <WlDeeper class="wl-sec wl-deeper" :d="w.deeper.value" :open="deep" :scope="scope" :in-the="w.words.value.inThe" :fallback="w.fallback.value" @read="readDeeper" @pick="pick" />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.wl { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.wl-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.wl-title { font-size: 22px; font-weight: 500; margin-right: 8px; }

.wl-cols { display: grid; grid-template-columns: 220px minmax(0, 1fr); gap: 0 40px; margin-top: 20px; align-items: start; }
.wl-side { position: sticky; top: 68px; max-height: calc(100vh - 84px); overflow-y: auto; padding-bottom: 12px; }
.wl-main { min-width: 0; }
.wl-pickm { display: none; flex-direction: column; gap: 10px; margin-bottom: 4px; }
.wl-pickm .qn-seg { align-self: flex-start; }
.wl-pickm select { flex: 1 1 0; min-width: 0; border: 0; outline: 0; background: transparent; color: var(--text); font-size: 13px; }

.wl-verdict { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 10px; font-size: 15px; min-height: 26px; }
.wl-badge { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 500; }
.wl-badge .qn-mark { align-self: center; }
.wl-act { font-size: 13px; }
.wl-why { margin-top: 16px; padding: 12px 18px; }
.wl-issues { display: grid; gap: 2px; }
/* the mark sits on the first line, however the sentence wraps */
.wl-issue { display: grid; grid-template-columns: 8px minmax(0, 1fr) auto; align-items: start; gap: 0 12px; }
.wl-issue > .qn-mark { margin-top: 12px; }
.wl-itext { min-width: 0; padding: 5px 0; text-align: left; line-height: 1.5; }
.wl-issue > .wl-ilink { padding-top: 6px; }
.wl-itext:hover > span:first-child { text-decoration: underline; text-underline-offset: 3px; }
.wl-ilink { flex: none; font-size: 12.5px; }
.wl-line { display: flex; align-items: center; gap: 8px; margin-top: 12px; font-size: 13px; }

.wl-sec { margin-top: 30px; }
.wl-dim { opacity: .45; transition: opacity .15s ease; }
.wl-h2 { font-size: 15px; font-weight: 500; }

/* The rows; a queue picked opens its groups on the right, beside the rows and
   everything under them, and stays in view while the page scrolls. */
.wl-lower { display: grid; grid-template-columns: minmax(0, 1fr); column-gap: 24px; }
.wl-lower.open { grid-template-columns: minmax(0, 1fr) 300px; grid-template-rows: auto 1fr; }
.wl-table { grid-column: 1; grid-row: 1; min-width: 0; }
.wl-rest { grid-column: 1; grid-row: 2; min-width: 0; }
.wl-drawer { grid-column: 2; grid-row: 1 / span 2; align-self: start; position: sticky; top: 68px; margin-top: 30px; max-height: calc(100vh - 84px); overflow-y: auto; }

.wl-notes { display: grid; gap: 8px; margin-top: 10px; }
.wl-note { display: flex; align-items: baseline; gap: 12px; }
.wl-note > span { flex: 1 1 auto; min-width: 0; }
.wl-deeper { margin-top: 44px; }

@media (max-width: 1100px) {
  .wl-lower.open { grid-template-columns: minmax(0, 1fr); grid-template-rows: none; }
  .wl-drawer { grid-column: 1; grid-row: 2; position: static; max-height: none; overflow: visible; margin-top: 18px; }
  .wl-lower.open .wl-rest { grid-row: 3; }
}
@media (max-width: 900px) {
  .wl-cols { grid-template-columns: minmax(0, 1fr); }
  .wl-side { display: none; }
  .wl-pickm { display: flex; margin-bottom: 18px; }
}
@media (max-width: 640px) {
  .wl { padding: 18px 16px 40px; }
  .wl-title { font-size: 19px; }
  .wl-cols { margin-top: 14px; }
  .wl-issue { grid-template-columns: 8px minmax(0, 1fr); }
  .wl-issue > .wl-ilink { grid-column: 2; padding: 0 0 6px; }
}
@media (prefers-reduced-motion: reduce) { .wl-dim { transition: none; } }
</style>
