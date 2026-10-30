<template>
  <!--
    The supervisors that publish their status to this cluster's KV, one card
    per application. The list on the left is every application by name; a
    pick (?group=) narrows the right side to it. With a queue picked
    (?queue=) this is that queue's Supervisors tab, inside the Queues section.
  -->
  <PageFrame label="Applications" :live="liveText" v-bind="frameAttrs">
    <template #side>
      <SupervisorsSide
        v-model:search="search"
        :groups="groups"
        :group="groupFilter"
        :queue="queueFilter"
        :read-at="readAt"
        :error="Boolean(error)"
      />
    </template>
    <!-- In a queue's tab the Queues section's header names the queue. -->
    <template v-if="inQueue && after" #actions>
      <span class="chip chip-warn" role="status" :title="PARTIAL_HELP"><span class="dot" />Partial view</span>
    </template>

    <div class="view-container sv-page">
      <PageHead v-if="!inQueue" :title="headTitle" :live="liveText">
        <template #title><span :title="headTitle">{{ headTitle }}</span></template>
        <template v-if="facts.length" #sub>
          <span class="sv-facts">
            <span v-for="f in facts" :key="f.k" class="sv-fact" :title="f.title || undefined"><span class="sv-fact-k">{{ f.k }}</span>{{ f.v }}</span>
          </span>
        </template>
        <template v-if="after" #actions>
          <span class="chip chip-warn" role="status" :title="PARTIAL_HELP"><span class="dot" />Partial view</span>
        </template>
      </PageHead>

      <PageTools>
        <UiSelect v-model="filter" label="Show" :options="showChoices" aria-label="Applications to show" />
        <UiSelect v-model="poolFilter" label="Pools" :options="POOL_FILTERS" aria-label="Pools to show" />
        <button v-if="filtersSet" type="button" class="btn btn-ghost" @click="clearFilters">Clear</button>
        <span v-if="narrowNote" class="tool-note">{{ narrowNote }}</span>
      </PageTools>

      <div v-if="error" class="status-banner banner-bad view-banner sv-banner" role="status">
        <span :title="readAt ? `Last good read at ${time(readAt)}. Current health is unconfirmed.` : undefined">
          <strong>{{ errorTitle }}</strong> · {{ errorDetail }}<template v-if="readAt"> · showing the read of {{ time(readAt) }}</template>
        </span>
        <button v-if="verdict !== 'transient'" type="button" class="btn btn-ghost" :disabled="loading" @click="checkAgain">Check again</button>
      </div>

      <section class="sv-list" aria-label="Applications">
        <p v-if="!readAt && !error" class="sv-loading" role="status">Reading published supervisor status…</p>
        <div v-else-if="readAt && !rows.length" class="sv-empty">
          <Icon name="server-cog" :size="20" />
          <span :title="EMPTY_HELP">No supervisor publishes here yet</span>
        </div>
        <div v-else-if="readAt && groupFilter !== null && !pickedGroup" class="sv-empty">
          <Icon name="server-cog" :size="20" />
          <span>{{ groupFilter }} no longer publishes here</span>
          <router-link class="btn btn-ghost" :to="allLink">All applications</router-link>
        </div>
        <div v-else-if="readAt && !shown.length" class="sv-empty">
          <Icon name="funnel" :size="20" />
          <span>No application matches {{ searchSet ? 'the search or filters' : 'the filters' }}</span>
          <button type="button" class="btn btn-ghost" @click="clearAll">{{ searchSet ? 'Clear search and filters' : 'Clear filters' }}</button>
        </div>
        <ul v-else-if="shown.length" class="sv-cards">
          <li v-for="group in pageGroups" :key="group.name">
            <SupervisorGroupCard
              :group="group"
              :read-at="readAt"
              :queue="queueFilter"
              :pool-filter="poolFilter"
              @select="selectedSlot = $event"
              @open-pool="openPool"
            />
          </li>
        </ul>
        <div v-if="readAt && (pages > 1 || after)" class="sv-pager">
          <span class="sv-pager-count">{{ pagerText }}</span>
          <div v-if="pages > 1" class="sv-pager-nav">
            <button type="button" class="btn btn-ghost" :disabled="page === 1" @click="page--">Previous</button>
            <span class="sv-pager-page">{{ page }} / {{ pages }}</span>
            <button type="button" class="btn btn-ghost" :disabled="page === pages" @click="page++">Next</button>
          </div>
          <button v-if="after" type="button" class="btn" :disabled="loading" :title="PARTIAL_HELP" @click="loadMore">Load more publications</button>
        </div>
      </section>

      <details class="sv-source" :open="sourceOpen" @toggle="sourceOpen = $event.target.open">
        <summary :title="SOURCE_HELP">
          <span>Source</span>
          <code>{{ namespace }}</code>
          <span v-if="sourceGroup" class="sv-source-group">· {{ sourceGroup }}</span>
          <span class="sv-source-change">Change</span>
        </summary>
        <form class="sv-source-form" @submit.prevent="applySource">
          <label class="sv-field">
            <span>KV namespace</span>
            <input v-model="namespaceDraft" class="input" required maxlength="64" />
          </label>
          <label class="sv-field" title="Matches exactly. Laravel publishers set it through remote_status.key; SDK consumers use their supervision group.">
            <span>Group <span class="sv-optional">optional</span></span>
            <input v-model="groupDraft" class="input" placeholder="Every group" maxlength="255" />
          </label>
          <button class="btn" type="submit">Read</button>
          <p v-if="sourceError" class="sv-source-error" role="alert">{{ sourceError }}</p>
        </form>
      </details>

      <Teleport to="body">
        <dialog
          ref="drawer"
          id="supervisor-drawer"
          class="drawer-panel sd"
          aria-labelledby="supervisor-drawer-title"
          @cancel.prevent="closeDrawer"
          @close="onDialogClose"
          @pointerdown="backdropDown = outside($event)"
          @click="onDrawerClick"
        >
          <div class="card-header sd-head">
            <h3 id="supervisor-drawer-title">Instance</h3>
            <button type="button" class="btn btn-ghost btn-icon modal-close" aria-label="Close instance details" autofocus @click="closeDrawer">
              <Icon name="x" :size="16" />
            </button>
          </div>
          <div class="sd-body">
            <template v-if="selected">
              <h2 class="sd-title">{{ selected.hostname || selected.group }}</h2>
              <p class="sd-sub">{{ selected.group }} · {{ selected.engine?.toUpperCase() || 'Unknown engine' }} · {{ selected.instance ? `instance …${selected.instance.slice(-8)}` : 'instance unavailable' }}</p>
              <p class="sd-status">
                <span class="sd-mark"><span v-if="statusMark(selected)" class="g" :class="statusMark(selected)" aria-hidden="true" /></span>
                <span>{{ selected.label }}</span>
                <template v-if="!selected.consumer">
                  <span class="sd-pill">{{ stateLabel(selected.readiness, 'Ready', 'Not ready', 'Readiness unconfirmed') }}</span>
                  <span class="sd-pill">{{ stateLabel(selected.capacity, 'Target reached', 'Below target', 'Capacity unconfirmed') }}</span>
                </template>
              </p>

              <dl class="sd-evidence">
                <div><dt>Queues</dt><dd>{{ number(selected.queueCount) }}</dd><small>{{ number(selected.poolCount) }} pools</small></div>
                <div><dt>Workers</dt><dd>{{ number(selected.workers) }}<span class="sd-of"> / {{ number(selected.desired) }}</span></dd><small>{{ number(selected.missingWorkers) }} below target<template v-if="!selected.consumer"> · {{ number(selected.draining) }} draining</template></small></div>
                <div><dt>Pools to check</dt><dd :class="{ warn: selected.affectedPools }">{{ number(selected.affectedPools) }}</dd><small>{{ selected.affectedPools === null ? 'unconfirmed' : 'in this heartbeat' }}</small></div>
                <div v-if="selected.consumer"><dt>Execution</dt><dd class="sd-text">{{ selected.executionModel }}</dd><small>tasks share a process</small></div>
                <div v-else><dt>Process budget</dt><dd>{{ selected.budget ? `${selected.budget.used} / ${selected.budget.limit}` : '—' }}</dd><small v-if="selected.budget">{{ selected.budget.available }} free · {{ selected.budget.helpers }} helpers</small><small v-else>not reported</small></div>
              </dl>

              <div class="sd-next">
                <span>Next check</span>
                <p>{{ selected.next }}</p>
              </div>
              <p v-if="selected.terminal" class="sd-last">Final report: this instance has stopped.</p>
              <p v-else-if="!selected.fresh" class="sd-last">Last reported values: current health is unconfirmed.</p>

              <dl class="sd-facts">
                <div><dt>Last heartbeat</dt><dd :title="ageLabel(selected.age)">{{ selected.updatedAt ? time(selected.updatedAt) : '—' }}</dd></div>
                <div><dt>Timeout</dt><dd>{{ selected.timeout ? `${selected.timeout}s` : '—' }}</dd></div>
                <div><dt>Uptime</dt><dd>{{ uptimeLabel(selected.uptime) }}</dd></div>
                <div><dt>{{ selected.consumer ? 'Process PID' : 'Master PID' }}</dt><dd>{{ number(selected.pid) }}</dd></div>
                <div><dt>Engine version</dt><dd>{{ selected.engineVersion || '—' }}</dd></div>
                <div><dt>Client version</dt><dd>{{ selected.clientVersion || '—' }}</dd></div>
                <div v-if="selected.startedAt"><dt>Started</dt><dd>{{ new Date(selected.startedAt).toLocaleString() }}</dd></div>
              </dl>

              <section class="sd-pools" aria-labelledby="supervisor-pools-title">
                <div class="sect-head">
                  <h3 id="supervisor-pools-title">Queues and pools</h3>
                  <span>{{ number(selected.queueCount) }} queues · {{ number(selected.poolCount) }} pools</span>
                </div>
                <div class="sd-pool-tools">
                  <input v-model="poolSearch" type="search" class="input" placeholder="Search queue or pool" aria-label="Search pools" />
                  <span class="sd-pool-note">{{ drawerPoolFilter === 'all' ? 'Issues first' : `Pools: ${poolFilterLabel}` }}</span>
                </div>
                <div class="sd-table">
                  <table class="t">
                    <thead>
                      <tr>
                        <th scope="col">Queue / pool</th>
                        <th scope="col">Finding</th>
                        <th scope="col" class="num" title="Running / desired">Workers</th>
                        <th scope="col" class="num">{{ selected.consumer ? 'Busy' : 'Pending' }}</th>
                        <th scope="col" class="num">{{ selected.consumer ? 'Failures' : 'Draining' }}</th>
                      </tr>
                    </thead>
                    <tbody>
                      <template v-for="pool in shownPools" :key="poolKey(pool)">
                        <tr :class="{ 'sd-open': expandedPool === poolKey(pool) }">
                          <th scope="row">
                            <button
                              type="button"
                              class="sd-toggle"
                              :aria-expanded="expandedPool === poolKey(pool)"
                              :aria-controls="expandedPool === poolKey(pool) ? 'supervisor-pool-detail' : undefined"
                              @click="expandedPool = expandedPool === poolKey(pool) ? null : poolKey(pool)"
                            >
                              <Icon name="chevron-right" class="sd-chevron" :size="14" :stroke="1.8" />
                              <span class="sd-pool-name">{{ pool.queue }}<small>{{ pool.name }}</small></span>
                            </button>
                          </th>
                          <td>
                            <span v-if="canDiagnosePools" class="sd-finding">
                              <span class="sd-mark"><span v-if="pool.severity" class="g" :class="pool.severity === 'bad' ? 'bad' : 'warn'" aria-hidden="true" /></span>{{ pool.label }}
                            </span>
                            <span v-else class="sd-low">{{ selected.state === 'running' ? 'Unconfirmed' : 'Last reported' }}</span>
                          </td>
                          <td class="num">{{ number(pool.running) }} / {{ number(pool.desired) }}</td>
                          <td class="num">{{ number(selected.consumer ? pool.busy : pool.depth) }}</td>
                          <td class="num">{{ number(selected.consumer ? pool.failed : pool.draining) }}</td>
                        </tr>
                        <tr v-if="expandedPool === poolKey(pool)" id="supervisor-pool-detail" class="sd-detail">
                          <td colspan="5">
                            <p v-if="!selected.consumer" class="sd-pills">
                              <span class="sd-pill">{{ canDiagnosePools ? stateLabel(pool.readiness, 'Ready', 'Not ready', 'Readiness not reported') : 'Readiness unconfirmed' }}</span>
                              <span class="sd-pill">{{ canDiagnosePools ? stateLabel(pool.capacity, 'Target reached', 'Below target', 'Capacity not reported') : 'Capacity unconfirmed' }}</span>
                            </p>
                            <p class="sd-line">{{ canDiagnosePools ? pool.next : 'Values from the last publication: current pool health is unconfirmed.' }}</p>
                            <p v-if="pool.failures" class="sd-line">{{ pool.failures }} restart failures<template v-if="pool.retryIn !== null"> · retry in {{ pool.retryIn }}s at publication</template></p>
                            <dl v-if="selected.consumer" class="sd-settings" title="Handler calls, batch calls included, since this consumer started. They do not confirm acknowledgements; a busy handler may be slow or blocked. Process restarts are the application's.">
                              <div><dt>Consumer group</dt><dd>{{ pool.configuration.consumerGroup }}</dd></div>
                              <div><dt>Handler calls ok</dt><dd>{{ number(pool.completed) }}</dd></div>
                              <div><dt>Handler calls failed</dt><dd>{{ number(pool.failed) }}</dd></div>
                              <div><dt>Last handler finished</dt><dd>{{ pool.lastCompleted ? time(pool.lastCompleted) : '—' }}</dd></div>
                              <div><dt>Oldest in flight</dt><dd>{{ seconds(pool.oldest) }}</dd></div>
                            </dl>
                            <dl v-else class="sd-settings" :title="`Pool limits apply to ${pool.name} across its queues. Running / desired is this queue's allocation.`">
                              <div><dt>Consumer group</dt><dd>{{ pool.configuration?.consumerGroup || '—' }}</dd></div>
                              <div><dt>Connection</dt><dd>{{ pool.configuration?.connection || '—' }}</dd></div>
                              <div><dt>Coordinated replicas</dt><dd>{{ number(pool.replicas) }}</dd></div>
                              <div><dt>Scaling / strategy</dt><dd>{{ pool.configuration?.balance || '—' }} / {{ pool.configuration?.strategy || '—' }}</dd></div>
                              <div><dt>Pool min / max</dt><dd>{{ number(pool.configuration?.min ?? null) }} / {{ number(pool.configuration?.max ?? null) }}</dd></div>
                              <div><dt>Processes per worker</dt><dd>{{ number(pool.cost) }}</dd></div>
                              <div><dt>Reserved / helpers</dt><dd>{{ number(pool.reserved) }} / {{ number(pool.helpers) }}</dd></div>
                              <div><dt>Job timeout</dt><dd>{{ seconds(pool.configuration?.timeout) }}</dd></div>
                              <div><dt>Lease</dt><dd>{{ seconds(pool.configuration?.lease) }}</dd></div>
                              <div><dt>Lease renewal</dt><dd>{{ stateLabel(pool.configuration?.leaseRenewal, 'On', 'Off', '—') }}</dd></div>
                              <div><dt>Attempts</dt><dd>{{ pool.configuration?.tries === 0 ? 'Unlimited' : number(pool.configuration?.tries ?? null) }}</dd></div>
                              <div><dt>Memory per worker</dt><dd>{{ pool.configuration?.memoryLimit ? `${formatNumber(pool.configuration.memoryLimit)} MB` : '—' }}</dd></div>
                            </dl>
                            <SupervisorQueueContext v-if="!selected.consumer || pool.queueName" :queue="pool.queue" />
                            <details v-if="pool.pids.length" class="sd-pids"><summary>Worker PIDs ({{ pool.pids.length }})</summary><p class="font-mono">{{ pool.pids.join(', ') }}</p></details>
                          </td>
                        </tr>
                      </template>
                      <tr v-if="!shownPools.length">
                        <td colspan="5" class="sd-none">{{ selected.pools.length ? 'No pool matches' : 'No valid pool telemetry' }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
                <div v-if="poolPages > 1" class="sd-pager">
                  <span>{{ `${(poolPage - 1) * POOLS_PER_PAGE + 1}–${Math.min(poolPage * POOLS_PER_PAGE, poolMatches.length)}` }} of {{ poolMatches.length }} pools</span>
                  <div>
                    <button type="button" class="btn btn-ghost" :disabled="poolPage === 1" @click="poolPage--">Previous</button>
                    <button type="button" class="btn btn-ghost" :disabled="poolPage === poolPages" @click="poolPage++">Next</button>
                  </div>
                </div>
              </section>

              <details class="sd-publication">
                <summary>Publication</summary>
                <dl>
                  <div><dt>Group</dt><dd>{{ selected.group }}<span v-if="selected.legacy"> · legacy key layout</span></dd></div>
                  <div><dt>Instance</dt><dd>{{ selected.instance || 'Unavailable' }}</dd></div>
                  <div><dt>KV namespace</dt><dd>{{ namespace }}</dd></div>
                  <div><dt>Slot</dt><dd>{{ selected.slot }}</dd></div>
                </dl>
              </details>
              <p class="sd-foot" title="Heartbeat age uses the browser clock and expiry is the broker's. Worker logs are not part of this publication, and publishing can fail while local supervision continues.">Pause, continue and stop on the supervisor's own host.</p>
            </template>
            <p v-else class="sd-gone" role="status">This instance is no longer in the loaded publications.</p>
          </div>
        </dialog>
      </Teleport>
    </div>
  </PageFrame>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, provide, ref, shallowRef, watch } from 'vue'
import { useRoute } from 'vue-router'

import { kv, system, describeApiError } from '@/api'
import Icon from '@/components/Icon.vue'
import PageFrame from '@/components/PageFrame.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'
import SupervisorGroupCard from '@/components/SupervisorGroupCard.vue'
import SupervisorQueueContext from '@/components/SupervisorQueueContext.vue'
import SupervisorsSide from '@/components/SupervisorsSide.vue'
import UiSelect from '@/components/UiSelect.vue'
import { queueTabOf } from '@/composables/navigation'
import { createSupervisorActivityReader, supervisorActivity, supervisorActivityKey } from '@/composables/supervisorActivity'
import { supervisorGroups, supervisorTone } from '@/composables/supervisorGroups'
import { POOL_FILTERS, showOptions, supervisorApplications } from '@/composables/supervisorsSide'
import { readSupervisorPage, supervisorObservations, supervisorGroupPrefix } from '@/composables/supervisorStatus'
import { formatNumber } from '@/composables/useApi'
import { describeVerdict, gatedVerdict } from '@/composables/useGatedVerdict'
import { kvRefusalText } from '@/composables/useKvView'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { useRouteState } from '@/composables/useRouteState'
import { useIdentity } from '@/stores/identity'
import { routeSupport } from '@/stores/routeSupport'

const PARTIAL_HELP = 'More publications are available than were read: the figures count the loaded instances only. Load more at the bottom of the page.'
const EMPTY_HELP = 'Set QUEEN_SUPERVISOR_REMOTE_STATUS=true on the application hosts and restart the supervisor; SDK consumers publish here once client supervision is on. Publications expire after their TTL, so an empty list does not prove that nothing runs.'
const SOURCE_HELP = 'Read from the acting cluster\'s KV store. Based on published heartbeats: a recent report does not prove a process still runs.'

// A tab of the picked queue (?queue=) rather than a page of its own.
const route = useRoute()
const inQueue = computed(() => Boolean(queueTabOf(route)))

const { epoch } = useIdentity()
const namespace = ref('queen-supervisor'), sourceGroup = ref('')
const namespaceDraft = ref(namespace.value), groupDraft = ref(''), sourceError = ref(''), sourceOpen = ref(false)
const entries = shallowRef([]), after = ref(null), readAt = ref(null), loading = ref(false), error = shallowRef(null), verdict = ref(null)
const list = routeSupport.guard('kv', async (body, config) => (await kv.list(body, config)).data)
let activityRefusal = null
const activityReader = createSupervisorActivityReader(async (queue, signal) => {
  if (activityRefusal) throw activityRefusal
  // Queued reads still belong to the same refresh window as the aggregate.
  const now = readAt.value
  try {
    const result = await system.getQueueOps({ queue, from: new Date(now - 3_600_000).toISOString(), to: new Date(now).toISOString() }, { signal, probe: true })
    return supervisorActivity(result.data, queue, now)
  } catch (failure) {
    if (!signal.aborted && gatedVerdict(failure) !== 'transient') activityRefusal = failure
    throw failure
  }
})
provide(supervisorActivityKey, activityReader)
watch(readAt, () => activityReader.clear(), { flush: 'sync' })
watch([epoch, namespace, sourceGroup], () => { activityRefusal = null; activityReader.clear() }, { flush: 'sync' })
let controller = null, sequence = 0, pagesLoaded = 1
// Each refresh re-reads the same bounded prefix. More pages are explicit so a
// large deployment cannot turn opening this screen into an unbounded KV scan.
async function read(append = false) {
  const turn = ++sequence, askedEpoch = epoch.value
  controller?.abort()
  controller = new AbortController()
  const signal = controller.signal
  loading.value = true
  try {
    let cursor = append ? after.value : null
    const next = new Map(append ? entries.value.map(row => [row.slot, row]) : [])
    const calls = append ? 1 : pagesLoaded
    let count = 0
    for (; count < calls; count++) {
      const result = await readSupervisorPage(list, { namespace: namespace.value, group: sourceGroup.value, after: cursor, signal })
      if (turn !== sequence || askedEpoch !== epoch.value) return
      for (const entry of result.entries) next.set(entry.slot, entry)
      cursor = result.after
      if (!cursor) { count++; break }
    }
    entries.value = [...next.values()]
    after.value = cursor
    pagesLoaded = append ? pagesLoaded + count : count
    readAt.value = Date.now()
    error.value = null
    verdict.value = null
  } catch (failure) {
    if (turn === sequence && askedEpoch === epoch.value && !signal.aborted) {
      error.value = failure
      verdict.value = gatedVerdict(failure)
    }
  } finally {
    if (turn === sequence) loading.value = false
  }
}
function refresh(explicit = false) {
  if (loading.value || (verdict.value && verdict.value !== 'transient' && !explicit)) return
  if (explicit) activityRefusal = null
  read()
}
function loadMore() { if (!loading.value && after.value) read(true) }
function clearSource() {
  sequence++; controller?.abort(); entries.value = []; after.value = null; readAt.value = null
  selectedSlot.value = null; error.value = null; verdict.value = null; pagesLoaded = 1; page.value = 1; loading.value = false; groupFilter.value = null
}
function applySource() {
  try { supervisorGroupPrefix(groupDraft.value.trim()) }
  catch (failure) { sourceError.value = failure.message; return }
  sourceError.value = ''
  clearSource()
  namespace.value = namespaceDraft.value.trim()
  sourceGroup.value = groupDraft.value.trim()
  groupFilter.value = null
  sourceOpen.value = false
  read()
}
function checkAgain() { routeSupport.forget('kv'); verdict.value = null; refresh(true) }
watch(epoch, () => { clearSource(); read() })
onMounted(() => read())
// The shell's refresh and its one ticker, which is suspended while hidden,
// stop after stable KV refusals; "Check again" in the banner always reads.
useAutoRefresh(() => refresh())
onBeforeUnmount(() => { sequence++; controller?.abort(); activityReader.clear(); drawer.value?.close() })

const rows = computed(() => supervisorObservations(entries.value, readAt.value || Date.now(), Boolean(error.value)))
const stale = computed(() => rows.value.filter(row => !row.fresh && !row.terminal).length)
const stopped = computed(() => rows.value.filter(row => row.terminal).length)
const workers = computed(() => {
  const recent = rows.value.filter(row => row.fresh && row.state === 'running')
  return recent.some(row => row.workers === null) ? null : recent.reduce((n, row) => n + row.workers, 0)
})
const search = ref(''), filter = ref('all'), groupFilter = ref(null), page = ref(1)
const queueFilter = ref('')
// The tools row's Pools filter: the cards (an application with a pool it
// keeps), each card's pools and the drawer's pool table.
const poolFilter = ref('all')
const groups = computed(() => supervisorGroups(rows.value, { partial: Boolean(after.value) }))
const engines = computed(() => [...new Set(rows.value.map(row => row.engine).filter(Boolean))].sort())
const showChoices = computed(() => showOptions(engines.value, filter.value))

// The application picked in the list, and what the right side shows: that
// one (the list's search does not hide a pick), else every application the
// search names; either way through the tools row's two filters.
const pickedGroup = computed(() => (groupFilter.value === null ? null : groups.value.find(g => g.name === groupFilter.value) || null))
const shown = computed(() => {
  const opts = { filter: filter.value, queue: queueFilter.value, pools: poolFilter.value }
  if (groupFilter.value !== null) return pickedGroup.value ? supervisorApplications([pickedGroup.value], opts) : []
  return supervisorApplications(groups.value, { ...opts, search: search.value })
})
const PER_PAGE = 10
const pages = computed(() => Math.max(1, Math.ceil(shown.value.length / PER_PAGE)))
const pageGroups = computed(() => shown.value.slice((page.value - 1) * PER_PAGE, page.value * PER_PAGE))
const filtersSet = computed(() => filter.value !== 'all' || poolFilter.value !== 'all')
// The search is the list's: only the empty state, which names it, clears it too.
const searchSet = computed(() => groupFilter.value === null && Boolean(search.value.trim()))
function clearFilters() { filter.value = 'all'; poolFilter.value = 'all' }
function clearAll() { clearFilters(); if (searchSet.value) search.value = '' }

watch([search, filter, groupFilter, queueFilter, poolFilter], () => { if (!restoringRoute.value) page.value = 1 })
watch(pages, n => { page.value = Math.min(page.value, n) })

const errorTitle = computed(() => verdict.value === 'transient' ? 'Cannot read supervisor publications' : describeVerdict(verdict.value, 'Supervisor discovery').title)
const errorDetail = computed(() => verdict.value === 'transient' ? kvRefusalText(error.value) || describeApiError(error.value) : describeVerdict(verdict.value, 'Supervisor discovery').detail)
const number = value => value === null || value === undefined ? '—' : formatNumber(value)
const metric = value => error.value || !readAt.value ? '—' : number(value)
const stateLabel = (value, yes, no, unknown) => value === true ? yes : value === false ? no : unknown
const seconds = value => value === null || value === undefined ? '—' : `${value}s`
const uptimeLabel = value => value === null ? '—' : value >= 86400 ? `${Math.floor(value / 86400)}d ${Math.floor(value % 86400 / 3600)}h` : value >= 3600 ? `${Math.floor(value / 3600)}h ${Math.floor(value % 3600 / 60)}m` : value >= 60 ? `${Math.floor(value / 60)}m ${value % 60}s` : `${value}s`
const time = value => new Date(value).toLocaleTimeString()
const ageLabel = age => age === null ? 'Age unknown' : age < -5 ? 'Timestamp is in the future' : age < 60 ? `${Math.max(0, age)}s before the read` : age < 3600 ? `${Math.floor(age / 60)}m ${age % 60}s before the read` : `${Math.floor(age / 3600)}h before the read`
// An instance's mark in the drawer: only where it needs you, a ring when it is not running.
const statusMark = row => ({ bad: 'bad', warn: 'warn', idle: 'idle' })[supervisorTone(row)] || null

// The header line: what is picked, a few facts, the live tick.
const refreshAgo = useRefreshAgo(readAt)
const liveText = computed(() => (readAt.value ? refreshAgo.value : ''))
const headTitle = computed(() => groupFilter.value ?? 'All applications')
const frameAttrs = computed(() => (inQueue.value ? {} : { current: groupFilter.value ?? 'All applications' }))
const allLink = computed(() => ({ query: { ...route.query, group: undefined, page: undefined, instance: undefined } }))
const facts = computed(() => {
  if (!readAt.value) return []
  const g = pickedGroup.value
  if (groupFilter.value !== null) {
    if (!g) return []
    return [
      { k: 'Instances', v: formatNumber(g.count) },
      { k: 'Workers', v: `${metric(g.workers)} / ${metric(g.desired)}`, title: 'Running / desired, on instances with a recent running heartbeat' },
      { k: 'Engine', v: g.engines.map(e => e.toUpperCase()).join(' / ') || '—' },
    ]
  }
  return [
    { k: 'Applications', v: formatNumber(groups.value.length) },
    { k: 'Instances', v: formatNumber(rows.value.length) },
    { k: 'Workers', v: metric(workers.value), title: 'Running, on instances with a recent running heartbeat' },
    stopped.value ? { k: 'Stopped', v: formatNumber(stopped.value) } : null,
    stale.value ? { k: 'Stale', v: formatNumber(stale.value), title: 'Instances whose heartbeat is overdue, expired or unreadable' } : null,
  ].filter(Boolean)
})
// Said only when the filters narrow the cards.
const narrowNote = computed(() => {
  if (!readAt.value || groupFilter.value !== null || !(filtersSet.value || search.value.trim())) return ''
  return `${formatNumber(shown.value.length)} of ${formatNumber(groups.value.length)} applications`
})
const pagerText = computed(() => {
  const n = shown.value.length
  if (pages.value <= 1) return `${formatNumber(n)} ${n === 1 ? 'application' : 'applications'} loaded`
  return `${formatNumber((page.value - 1) * PER_PAGE + 1)}–${formatNumber(Math.min(page.value * PER_PAGE, n))} of ${formatNumber(n)} applications`
})

const selectedSlot = ref(null), drawer = ref(null), poolSearch = ref(''), poolPage = ref(1), expandedPool = ref(null)
const { restoring: restoringRoute } = useRouteState({ queue: queueFilter, search, filter, pools: poolFilter, group: groupFilter, page, instance: selectedSlot })
const POOLS_PER_PAGE = 10
const selected = computed(() => rows.value.find(row => row.slot === selectedSlot.value))
const canDiagnosePools = computed(() => selected.value?.affectedPools !== null && selected.value?.fresh && selected.value?.state === 'running' && !error.value)
const poolKey = pool => JSON.stringify([pool.name, pool.queue])
// "Need attention" asks this heartbeat for findings: an instance that cannot
// diagnose its pools (stale, stopped, unconfirmed) shows them all instead.
const drawerPoolFilter = computed(() => poolFilter.value === 'attention' && !canDiagnosePools.value ? 'all' : poolFilter.value)
const poolFilterLabel = computed(() => (POOL_FILTERS.find(o => o.value === drawerPoolFilter.value)?.label || '').toLowerCase())
const poolMatches = computed(() => selected.value?.pools.filter(pool => {
  const term = poolSearch.value.trim().toLocaleLowerCase()
  return `${pool.name} ${pool.queue}`.toLocaleLowerCase().includes(term)
    && (drawerPoolFilter.value === 'all' || (drawerPoolFilter.value === 'attention' && pool.severity) || (drawerPoolFilter.value === 'draining' && pool.draining > 0))
}) || [])
const poolPages = computed(() => Math.max(1, Math.ceil(poolMatches.value.length / POOLS_PER_PAGE)))
const shownPools = computed(() => poolMatches.value.slice((poolPage.value - 1) * POOLS_PER_PAGE, poolPage.value * POOLS_PER_PAGE))
watch([poolSearch, drawerPoolFilter], () => { poolPage.value = 1; expandedPool.value = null })
watch(poolPage, () => { expandedPool.value = null })
watch(poolPages, n => { poolPage.value = Math.min(poolPage.value, n) })
let closing = false
async function closeDrawer() {
  const panel = drawer.value
  if (!panel?.open || closing) return
  closing = true
  panel.style.setProperty('--drawer-leave-from', getComputedStyle(panel).transform)
  panel.style.setProperty('--drawer-scrim-from', getComputedStyle(panel, '::backdrop').opacity)
  panel.dataset.drawerMotion = 'leave'
  await Promise.allSettled(panel.getAnimations().map(animation => animation.finished))
  if (drawer.value === panel) { panel.close(); selectedSlot.value = null }
  closing = false
}
// A pool picked in a card: its instance's drawer, on that pool's page of the
// table, with the pool open. The resets above run first.
let pendingPool = null
function openPool({ slot, key }) {
  if (!slot) return
  if (selectedSlot.value === slot) { focusPool(key); return }
  pendingPool = key
  selectedSlot.value = slot
}
async function focusPool(key) {
  poolSearch.value = ''
  await nextTick()
  const index = poolMatches.value.findIndex(pool => poolKey(pool) === key)
  if (index < 0) return
  poolPage.value = Math.floor(index / POOLS_PER_PAGE) + 1
  await nextTick()
  expandedPool.value = key
  await nextTick()
  document.getElementById('supervisor-pool-detail')?.scrollIntoView({ block: 'nearest' })
}
watch(selectedSlot, async slot => {
  poolSearch.value = ''; poolPage.value = 1; expandedPool.value = null
  await nextTick()
  if (slot && selectedSlot.value === slot && drawer.value && !drawer.value.open) {
    drawer.value.dataset.drawerMotion = 'enter'; drawer.value.showModal()
  } else if (!selectedSlot.value) drawer.value?.close()
  if (pendingPool && slot && selectedSlot.value === slot) { const key = pendingPool; pendingPool = null; focusPool(key) }
  else pendingPool = null
}, { immediate: true })
const onDialogClose = () => { if (!drawer.value?.open) selectedSlot.value = null }
let backdropDown = false
function outside(event) {
  const rect = drawer.value?.getBoundingClientRect()
  return rect && (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom)
}
function onDrawerClick(event) { if (backdropDown && outside(event)) closeDrawer(); backdropDown = false }
</script>

<style scoped>
/* The header line's facts: the Queues pilot's chips (QueuesLayout). */
/* The facts drop under the title when the line is full, as the pilot's do. */
.page-head :deep(.page-id) { flex-wrap: wrap; row-gap: 6px; }
.page-head :deep(.page-sub) { overflow: visible; white-space: normal; }
.sv-facts { display: inline-flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.sv-fact {
  display: inline-flex; align-items: baseline; gap: 5px;
  padding: 2px 8px; border-radius: var(--r-chip);
  background: var(--ink-3); border: 1px solid var(--bd);
  font-size: 11.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap;
}
.sv-fact-k { color: var(--text-low); }

.sv-banner { display: flex; align-items: center; gap: 8px 12px; flex-wrap: wrap; margin-bottom: 16px; }
.sv-banner > span { flex: 1 1 280px; min-width: 0; }

.sv-loading { margin: 0; padding: 24px 0; font-size: 13px; color: var(--text-low); }
.sv-empty {
  display: flex; flex-direction: column; align-items: center; gap: 10px;
  padding: 48px 16px; text-align: center; font-size: 13px; color: var(--text-mid);
  border: 1px solid var(--bd); border-radius: var(--r-card); background: var(--ink-2);
}
.sv-empty > .icon { color: var(--text-low); }
.sv-cards { display: grid; grid-template-columns: minmax(0, 1fr); gap: 16px; list-style: none; padding: 0; margin: 0; }
.sv-cards > li { min-width: 0; }

.sv-pager {
  display: flex; align-items: center; gap: 8px 16px; flex-wrap: wrap;
  margin-top: 16px; font-size: 12px; color: var(--text-low);
}
.sv-pager-count { font-variant-numeric: tabular-nums; }
.sv-pager-nav { display: flex; align-items: center; gap: 4px; margin-left: auto; }
.sv-pager-page { min-width: 48px; text-align: center; font-variant-numeric: tabular-nums; color: var(--text-mid); }
.sv-pager > .btn:last-child { margin-left: auto; }
.sv-pager-nav + .btn { margin-left: 0 !important; }

/* The source: one quiet line under the cards, its form on request. */
.sv-source { margin-top: 20px; font-size: 12px; color: var(--text-low); }
.sv-source > summary {
  display: inline-flex; align-items: center; gap: 6px; flex-wrap: wrap;
  list-style: none; cursor: pointer;
}
.sv-source > summary::-webkit-details-marker { display: none; }
.sv-source code { font-family: var(--font-mono); font-size: 11.5px; color: var(--text-mid); overflow-wrap: anywhere; }
.sv-source-group { color: var(--text-mid); overflow-wrap: anywhere; }
.sv-source-change { color: var(--text-mid); text-decoration: underline; text-underline-offset: 2px; }
.sv-source > summary:hover .sv-source-change { color: var(--text-hi); }
.sv-source[open] .sv-source-change { display: none; }
.sv-source-form { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 12px; max-width: 720px; padding: 12px 0 4px; }
.sv-field { display: flex; flex-direction: column; gap: 6px; flex: 1 1 200px; min-width: 0; font-size: 12px; color: var(--text-mid); }
.sv-field .input { width: 100%; height: 32px; }
.sv-optional { color: var(--text-low); }
.sv-source-error { flex-basis: 100%; margin: 0; font-size: 12px; color: var(--ember-400); }

/* The instance drawer: the page's grammar inside the float. */
.sd { inset: 0 0 0 auto; margin: 0 0 0 auto; padding: 0; width: min(880px, 100%); max-width: 100%; height: 100dvh; max-height: 100dvh; border: 0; border-left: 1px solid var(--bd); color: var(--text-hi); background: var(--ink-2); overscroll-behavior: contain; }
.sd[open] { display: flex; flex-direction: column; }
.sd::backdrop { background: rgb(0 0 0 / .18); }
.sd-head { flex-shrink: 0; }
.sd-body { padding: 20px 24px 28px; overflow-y: auto; flex: 1; min-height: 0; overscroll-behavior: contain; }
.sd-title { margin: 0 0 4px; font-size: 18px; font-weight: 600; letter-spacing: -0.01em; overflow-wrap: anywhere; }
.sd-sub { margin: 0 0 14px; font-size: 12px; color: var(--text-low); overflow-wrap: anywhere; }
.sd-status { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; margin: 0; font-size: 13px; color: var(--text-hi); }
.sd-mark { flex: none; width: 10px; display: inline-flex; justify-content: center; }
.sd-pill { padding: 2px 8px; border: 1px solid var(--bd); border-radius: var(--r-chip); font-size: 11.5px; color: var(--text-mid); }
.sd-pills { display: flex; flex-wrap: wrap; gap: 6px; margin: 0 0 8px; }
.sd-evidence {
  display: grid; grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 16px 0 0; border: 1px solid var(--bd); border-radius: var(--r-card); overflow: hidden;
}
.sd-evidence > div { padding: 12px 14px; min-width: 0; }
.sd-evidence > div + div { border-left: 1px solid var(--bd); }
.sd-evidence dt { font-size: 12px; color: var(--text-low); }
.sd-evidence dd { margin: 4px 0 2px; font-size: 20px; font-weight: 600; letter-spacing: -0.015em; font-variant-numeric: tabular-nums; }
.sd-evidence dd.warn { color: var(--warn-400); }
.sd-evidence dd.sd-text { font-size: 14px; }
.sd-evidence small { font-size: 12px; color: var(--text-low); }
.sd-of { font-size: 12px; font-weight: 500; color: var(--text-mid); }
.sd-next { margin: 16px 0 0; padding: 12px 14px; border-radius: var(--r-card); background: var(--ink-3); }
.sd-next span { font-size: 12px; color: var(--text-low); }
.sd-next p { margin: 4px 0 0; font-size: 13px; line-height: 1.55; }
.sd-last { margin: 10px 0 0; font-size: 12px; color: var(--text-mid); }
.sd-facts { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px 16px; margin: 16px 0 0; }
.sd-facts dt, .sd-settings dt { font-size: 12px; color: var(--text-low); }
.sd-facts dd, .sd-settings dd { margin: 3px 0 0; font-size: 12.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
.sd-pools { margin: 24px 0 0; }
.sd-pools .sect-head { margin-bottom: 10px; }
.sd-pool-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; margin-bottom: 10px; }
.sd-pool-tools .input { flex: 1 1 220px; min-width: 0; height: 32px; }
.sd-pool-note { font-size: 12px; color: var(--text-low); }
.sd-table { overflow-x: auto; border: 1px solid var(--bd); border-radius: var(--r-card); }
.sd-table .t { width: 100%; min-width: 560px; }
.sd-table .t thead th { background: var(--ink-3); }
.sd-table tbody th { font-weight: 500; text-align: left; padding: 9px 12px; }
.sd-toggle { display: flex; align-items: flex-start; gap: 6px; width: 100%; padding: 0; border: 0; background: none; color: var(--text-hi); font: inherit; text-align: left; cursor: pointer; overflow-wrap: anywhere; }
.sd-toggle:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; border-radius: 2px; }
.sd-chevron { flex: none; margin-top: 2px; color: var(--text-low); transition: transform .15s; }
.sd-toggle[aria-expanded="true"] .sd-chevron { transform: rotate(90deg); }
.sd-pool-name small { display: block; margin-top: 2px; font-size: 11.5px; font-weight: 400; color: var(--text-low); }
.sd-finding { display: inline-flex; align-items: center; gap: 8px; color: var(--text-mid); }
.sd-low { color: var(--text-low); }
.sd-open > *, .sd-detail > td { background: var(--ink-3); }
.sd-detail > td { padding: 12px 16px 16px; }
.sd-line { margin: 0 0 8px; font-size: 12px; line-height: 1.55; color: var(--text-mid); }
.sd-settings { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px 16px; margin: 12px 0 0; }
.sd-pids { margin-top: 10px; font-size: 12px; color: var(--text-mid); overflow-wrap: anywhere; }
.sd-pids summary, .sd-publication summary { cursor: pointer; color: var(--text-mid); }
.sd-pids p { margin: 6px 0 0; line-height: 1.7; }
.sd-none { padding: 20px !important; text-align: center; color: var(--text-low); }
.sd-pager { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 8px; margin-top: 8px; font-size: 12px; color: var(--text-low); }
.sd-pager > div { display: flex; gap: 4px; }
.sd-publication { margin-top: 24px; padding-top: 14px; border-top: 1px solid var(--bd); font-size: 12px; color: var(--text-mid); }
.sd-publication dl { margin: 8px 0 0; }
.sd-publication dt { margin-top: 8px; color: var(--text-low); }
.sd-publication dd { margin: 2px 0 0; font-family: var(--font-mono); font-size: 11.5px; overflow-wrap: anywhere; }
.sd-foot { margin: 16px 0 0; font-size: 12px; color: var(--text-low); }
.sd-gone { font-size: 13px; color: var(--text-mid); }

@media (max-width: 760px) {
  .sd-evidence { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); }
  .sd-evidence > div:nth-child(3) { border-left: 0; }
  .sd-evidence > div:nth-child(n+3) { border-top: 1px solid var(--bd); }
  .sd-facts, .sd-settings { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .sd-body { padding: 16px 16px 24px; }
}
@media (max-width: 640px) {
  .sv-pager-nav { margin-left: 0; }
}
@media (prefers-reduced-motion: reduce) { .sd-chevron { transition: none; } }
</style>
