<template>
  <div class="view-container">
    <PageHead title="Supervisors">
      <template #sub>
        <template v-if="readAt && !error"><b>{{ metric(groups.length) }}</b> applications · <b>{{ metric(rows.length) }}</b> instances · <b>{{ metric(workers) }}</b> workers reporting<template v-if="stale"> · {{ stale }} with unconfirmed health</template></template>
        <template v-else>{{ fleetLabel }}</template>
      </template>
      <template #actions>
        <span v-if="readAt && (fleetTone === 'warn' || fleetTone === 'bad')" class="chip" :class="fleetTone === 'bad' ? 'chip-bad' : 'chip-warn'"><span class="dot" />{{ fleetLabel }}</span>
        <span class="supervisor-stamp">{{ loading ? 'Reading…' : readAt ? `Read at ${time(readAt)}` : 'Not read yet' }}</span>
        <button class="btn" :disabled="loading" @click="refresh(true)">Refresh</button>
      </template>
    </PageHead>

    <div v-if="error" class="panel-err" role="status">
      <strong>{{ errorTitle }}</strong><p>{{ errorDetail }}</p>
      <span v-if="readAt">Last successful read: {{ time(readAt) }}. Current health is unconfirmed.</span>
      <button v-if="verdict !== 'transient'" class="btn btn-ghost" :disabled="loading" @click="checkAgain">Check again</button>
    </div>

    <section class="supervisor-list" aria-labelledby="supervisor-list-title">
      <h2 id="supervisor-list-title" class="sr-only">Applications</h2>
      <PageTools>
        <div class="filter-search">
          <svg class="filter-search-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
          </svg>
          <input v-model="search" class="input" type="search" aria-label="Search group, host or queue" placeholder="Search group, host or queue…" />
        </div>
        <label class="tool-field">
          <span class="tool-label">Show</span>
          <select v-model="filter" class="input"><option value="all">All applications</option><option value="attention">Needs attention</option><option value="stale">Stale / unreadable</option><option v-for="engine in engines" :key="engine" :value="`engine:${engine}`">{{ engine }} engine</option></select>
        </label>
        <label class="tool-field">
          <span class="tool-label">Group</span>
          <select v-model="groupFilter" class="input"><option :value="null">All loaded</option><option v-for="group in groups" :key="group.name" :value="group.name">{{ group.name }} · {{ group.count }}</option></select>
        </label>
        <template #view><span class="tool-note">Issues first</span></template>
      </PageTools>
      <div v-if="loading && !readAt" class="supervisor-empty" role="status">Reading published supervisor status…</div>
      <div v-else-if="!rows.length && !error && readAt" class="supervisor-empty">
        <h3>No published supervisors in this source</h3>
        <p>Enable remote status on the application hosts, then restart the supervisor with the updated configuration.</p>
        <code class="supervisor-command">QUEEN_SUPERVISOR_REMOTE_STATUS=true</code>
        <p>Enable optional client supervision to publish SDK consumers here. It is off by default. Check that the publisher connects to this cluster and uses the namespace above.</p>
        <p>Publications expire after their configured TTL. An empty list does not establish that no supervisors are running.</p>
      </div>
      <div v-else-if="!filtered.length && !error" class="supervisor-empty">No loaded applications match these filters.</div>
      <div v-else class="supervisor-overviews">
        <ul class="supervisor-cards">
          <li v-for="group in pageGroups" :key="group.name">
            <SupervisorGroupCard :group="group" :read-at="readAt" @select="selectedSlot = $event" />
          </li>
        </ul>
      </div>
      <div v-if="pages > 1 || after" class="supervisor-pagination">
        <span>{{ formatNumber(filtered.length) }} matching loaded applications</span>
        <div v-if="pages > 1"><button class="btn btn-ghost" :disabled="page === 1" @click="page--">Previous</button><span>{{ page }} / {{ pages }}</span><button class="btn btn-ghost" :disabled="page === pages" @click="page++">Next</button></div>
        <button v-if="after" class="btn" :disabled="loading" @click="loadMore">Load more publications</button>
      </div>
    </section>
    <p v-if="after" class="supervisor-partial" role="status">Partial view: more publications are available. Load more to include the remaining instances in these summaries.</p>
    <footer class="supervisor-footer">
      <p class="supervisor-note">Based on published heartbeats. A recent report does not guarantee that a process is still running.</p>
      <details class="supervisor-source">
        <summary>Source <code>{{ namespace }}</code><span v-if="sourceGroup">· {{ sourceGroup }}</span><span class="muted">Configure source</span></summary>
        <form @submit.prevent="applySource">
          <label>KV namespace<input v-model="namespaceDraft" class="input" required maxlength="64" /></label>
          <label>Group <span class="muted">optional</span><input v-model="groupDraft" class="input" placeholder="e.g. pmsintool" maxlength="255" /></label>
          <button class="btn" type="submit">Read source</button>
          <p v-if="sourceError" class="warn" role="alert">{{ sourceError }}</p>
          <p>Reads the acting cluster’s KV store. A group matches exactly; leave it empty to discover all groups. Laravel publishers set it through <code>remote_status.key</code>; SDK consumers use their supervision group.</p>
        </form>
      </details>
    </footer>

    <Teleport to="body">
      <dialog ref="drawer" id="supervisor-drawer" class="drawer-panel supervisor-drawer" aria-labelledby="supervisor-drawer-title" @cancel.prevent="closeDrawer" @close="onDialogClose" @pointerdown="backdropDown = outside($event)" @click="onDrawerClick">
        <div class="card-header">
          <h3 id="supervisor-drawer-title">Supervisor details</h3>
          <button class="btn btn-ghost btn-icon modal-close" aria-label="Close supervisor details" autofocus @click="closeDrawer">✕</button>
        </div>
        <div class="supervisor-drawer-body">
          <template v-if="selected">
            <h2>{{ selected.hostname || selected.group }}</h2>
            <p class="supervisor-app">{{ selected.group }} · {{ selected.engine?.toUpperCase() || 'Unknown engine' }} · {{ selected.instance ? `Instance …${selected.instance.slice(-8)}` : 'Instance unavailable' }}</p>
            <p class="supervisor-detail-status" :class="selected.severity"><span class="g" :class="selected.severity || 'idle'" aria-hidden="true" />{{ selected.label }}</p>
            <p v-if="!selected.consumer" class="supervisor-readiness"><span>{{ stateLabel(selected.readiness, 'Ready', 'Not ready', 'Readiness unconfirmed') }}</span><span>{{ stateLabel(selected.capacity, 'Target reached', 'Below target', 'Capacity unconfirmed') }}</span></p>
            <dl class="supervisor-evidence">
              <div><dt>Queues</dt><dd>{{ number(selected.queueCount) }}</dd><small>{{ number(selected.poolCount) }} pools</small></div>
              <div><dt>Running / desired</dt><dd>{{ number(selected.workers) }} / {{ number(selected.desired) }}</dd><small>{{ number(selected.missingWorkers) }} below target<template v-if="!selected.consumer"> · {{ number(selected.draining) }} draining</template></small></div>
              <div><dt>Pools needing attention</dt><dd :class="selected.affectedPools ? 'warn' : ''">{{ number(selected.affectedPools) }}</dd><small>{{ selected.affectedPools === null ? 'Health unconfirmed' : 'From this heartbeat' }}</small></div>
              <div v-if="selected.consumer"><dt>Execution model</dt><dd>{{ selected.executionModel }}</dd><small>Consumer tasks share a process</small></div><div v-else><dt>Process budget</dt><dd>{{ selected.budget ? `${selected.budget.used} / ${selected.budget.limit}` : '—' }}</dd><small v-if="selected.budget">{{ selected.budget.available }} available · {{ selected.budget.helpers }} renewal helpers</small><small v-else>Not reported or inconsistent</small></div>
            </dl>
            <p class="supervisor-heartbeat">Last heartbeat {{ selected.updatedAt ? time(selected.updatedAt) : 'unavailable' }} · {{ ageLabel(selected.age) }} · timeout {{ selected.timeout ? `${selected.timeout}s` : 'unavailable' }}</p>
            <dl class="supervisor-runtime"><div><dt>Engine version</dt><dd>{{ selected.engineVersion || 'Not reported' }}</dd></div><div><dt>Client version</dt><dd>{{ selected.clientVersion || 'Not reported' }}</dd></div><div><dt>{{ selected.consumer ? 'Process PID' : 'Master PID' }}</dt><dd>{{ number(selected.pid) }}</dd></div><div><dt>Uptime at heartbeat</dt><dd>{{ uptimeLabel(selected.uptime) }}</dd></div></dl>
            <p v-if="selected.startedAt" class="supervisor-heartbeat">Instance started {{ new Date(selected.startedAt).toLocaleString() }}</p>
            <div class="supervisor-next"><span>Next check</span><p>{{ selected.next }}</p></div>
            <p v-if="!selected.fresh" class="supervisor-last-values">Last reported values. Current worker health is unconfirmed.</p>
            <section class="supervisor-pools" aria-labelledby="supervisor-pools-title">
              <div class="supervisor-pools-heading"><h3 id="supervisor-pools-title">Queues and pools</h3><span>{{ number(selected.queueCount) }} queues · {{ number(selected.poolCount) }} pools</span></div>
              <div class="supervisor-pool-tools">
                <label><span class="sr-only">Search pools</span><input v-model="poolSearch" type="search" class="input" placeholder="Search queue or pool…" /></label>
                <label><span class="sr-only">Filter pools</span><select v-model="poolFilter" class="input"><option value="all">All pools</option><option value="attention" :disabled="!canDiagnosePools">Needs attention</option><option value="draining">Draining workers</option></select></label>
                <span>Issues first</span>
              </div>
              <div class="supervisor-pool-table-wrap">
                <table class="supervisor-pool-table">
                  <thead><tr><th scope="col">Queue / pool</th><th scope="col">Finding</th><th scope="col">Workers <small>running / desired</small></th><th scope="col">{{ selected.consumer ? 'Busy' : 'Pending' }}</th><th scope="col">{{ selected.consumer ? 'Handler failures' : 'Draining' }}</th></tr></thead>
                  <tbody>
                    <template v-for="pool in shownPools" :key="poolKey(pool)">
                      <tr :class="{ 'pool-expanded': expandedPool === poolKey(pool) }">
                        <th scope="row"><button class="supervisor-pool-toggle" :aria-expanded="expandedPool === poolKey(pool)" :aria-controls="expandedPool === poolKey(pool) ? 'supervisor-pool-detail' : undefined" @click="expandedPool = expandedPool === poolKey(pool) ? null : poolKey(pool)"><span aria-hidden="true">{{ expandedPool === poolKey(pool) ? '▾' : '▸' }}</span><span>{{ pool.queue }}<small>{{ pool.name }}</small></span></button></th>
                        <td><span v-if="canDiagnosePools" class="supervisor-pool-finding" :class="pool.severity">{{ pool.label }}</span><span v-else class="muted">{{ selected.state === 'running' ? 'Unconfirmed' : 'Last reported' }}</span></td>
                        <td class="supervisor-pool-count">{{ number(pool.running) }} / {{ number(pool.desired) }}</td><td class="supervisor-pool-count">{{ number(selected.consumer ? pool.busy : pool.depth) }}</td><td class="supervisor-pool-count">{{ number(selected.consumer ? pool.failed : pool.draining) }}</td>
                      </tr>
                      <tr v-if="expandedPool === poolKey(pool)" id="supervisor-pool-detail" class="supervisor-pool-detail"><td colspan="5">
                        <p v-if="!selected.consumer" class="supervisor-readiness"><span>{{ canDiagnosePools ? stateLabel(pool.readiness, 'Ready', 'Not ready', 'Readiness not reported') : 'Readiness unconfirmed' }}</span><span>{{ canDiagnosePools ? stateLabel(pool.capacity, 'Target reached', 'Below target', 'Capacity not reported') : 'Capacity unconfirmed' }}</span></p>
                        <p v-if="canDiagnosePools" class="supervisor-pool-next">{{ pool.next }}</p>
                        <p v-else class="supervisor-pool-next">Values from the last publication. Current pool health is unconfirmed.</p>
                        <p v-if="pool.failures" class="supervisor-restarts">{{ pool.failures }} restart failures<template v-if="pool.retryIn !== null"> · retry in {{ pool.retryIn }}s at publication</template></p>
                        <dl v-if="selected.consumer" class="supervisor-pool-settings">
                          <div><dt>Consumer group</dt><dd>{{ pool.configuration.consumerGroup }}</dd></div>
                          <div><dt>Successful handler calls</dt><dd>{{ number(pool.completed) }}</dd></div>
                          <div><dt>Failed handler calls</dt><dd>{{ number(pool.failed) }}</dd></div>
                          <div><dt>Last handler finished</dt><dd>{{ pool.lastCompleted ? time(pool.lastCompleted) : 'No completion reported' }}</dd></div>
                          <div><dt>Oldest in-flight handler</dt><dd>{{ seconds(pool.oldest) }}</dd></div>
                        </dl>
                        <p v-if="selected.consumer" class="supervisor-pool-next">Counts are handler calls, including batch calls, since this consumer started. They do not confirm acknowledgements. A busy handler may be slow or blocked. Process restarts remain the application's responsibility.</p>
                        <dl v-else class="supervisor-pool-settings">
                          <div><dt>Consumer group</dt><dd>{{ pool.configuration?.consumerGroup || 'Not reported' }}</dd></div>
                          <div><dt>Connection</dt><dd>{{ pool.configuration?.connection || 'Not reported' }}</dd></div>
                          <div><dt>Coordinated replicas</dt><dd>{{ number(pool.replicas) }}</dd></div>
                          <div><dt>Scaling / strategy</dt><dd>{{ pool.configuration?.balance || '—' }} / {{ pool.configuration?.strategy || '—' }}</dd></div>
                          <div><dt>Pool min / max</dt><dd>{{ number(pool.configuration?.min ?? null) }} / {{ number(pool.configuration?.max ?? null) }}</dd></div>
                          <div><dt>Processes per worker</dt><dd>{{ number(pool.cost) }}</dd></div>
                          <div><dt>Reserved / helpers</dt><dd>{{ number(pool.reserved) }} / {{ number(pool.helpers) }}</dd></div>
                          <div><dt>Job timeout</dt><dd>{{ seconds(pool.configuration?.timeout) }}</dd></div>
                          <div><dt>Lease duration</dt><dd>{{ seconds(pool.configuration?.lease) }}</dd></div>
                          <div><dt>Lease renewal</dt><dd>{{ stateLabel(pool.configuration?.leaseRenewal, 'Enabled', 'Disabled', 'Not reported') }}</dd></div>
                          <div><dt>Attempts</dt><dd>{{ pool.configuration?.tries === 0 ? 'Unlimited' : number(pool.configuration?.tries ?? null) }}</dd></div>
                          <div><dt>Memory limit per worker</dt><dd>{{ pool.configuration?.memoryLimit ? `${formatNumber(pool.configuration.memoryLimit)} MB` : 'Not reported' }}</dd></div>
                        </dl>
                        <p v-if="!selected.consumer" class="supervisor-pool-next">Pool limits apply to {{ pool.name }} across its configured queues. Running / desired is this queue’s allocation.</p>
                        <SupervisorQueueContext v-if="!selected.consumer || pool.queueName" :queue="pool.queue" />
                        <details v-if="pool.pids.length" class="supervisor-pids"><summary>Reported worker PIDs ({{ pool.pids.length }})</summary><p class="font-mono">{{ pool.pids.join(', ') }}</p></details>
                        <p v-else-if="!selected.consumer" class="supervisor-pool-next">No worker PIDs reported in this heartbeat.</p>
                      </td></tr>
                    </template>
                    <tr v-if="!shownPools.length"><td colspan="5" class="supervisor-pools-empty">{{ selected.pools.length ? 'No pools match these filters.' : 'No valid pool telemetry is available.' }}</td></tr>
                  </tbody>
                </table>
              </div>
              <div class="supervisor-pool-pagination">
                <span>{{ poolMatches.length ? `${(poolPage - 1) * POOLS_PER_PAGE + 1}–${Math.min(poolPage * POOLS_PER_PAGE, poolMatches.length)}` : '0' }} of {{ poolMatches.length }} pools</span>
                <div><button class="btn btn-ghost" :disabled="poolPage === 1" @click="poolPage--">Previous pools</button><button class="btn btn-ghost" :disabled="poolPage === poolPages" @click="poolPage++">Next pools</button></div>
              </div>
            </section>
            <details class="supervisor-publication"><summary>Publication details</summary><dl><div><dt>Group</dt><dd>{{ selected.group }}<span v-if="selected.legacy"> · legacy key layout</span></dd></div><div><dt>Instance</dt><dd>{{ selected.instance || 'Unavailable' }}</dd></div><div><dt>KV namespace</dt><dd>{{ namespace }}</dd></div><div><dt>Publication slot</dt><dd>{{ selected.slot }}</dd></div></dl><p>Heartbeat age uses the browser clock. Expiry is reported by the broker. Publishing can fail while local supervision continues.</p></details>
            <p class="supervisor-note">Pause, continue and terminate are managed on the supervisor’s host. Worker logs are not part of this publication.</p>
          </template>
          <p v-else role="status">This publication is no longer present in the loaded results. It may have expired or been removed.</p>
        </div>
      </dialog>
    </Teleport>
  </div>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, provide, ref, shallowRef, watch } from 'vue'
import SupervisorQueueContext from '@/components/SupervisorQueueContext.vue'
import SupervisorGroupCard from '@/components/SupervisorGroupCard.vue'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'
import { supervisorGroups, filterSupervisorGroups } from '@/composables/supervisorGroups'
import { createSupervisorActivityReader, supervisorActivity, supervisorActivityKey } from '@/composables/supervisorActivity'
import { kv, system, describeApiError } from '@/api'
import { formatNumber } from '@/composables/useApi'
import { readSupervisorPage, supervisorObservations, supervisorGroupPrefix } from '@/composables/supervisorStatus'
import { describeVerdict, gatedVerdict } from '@/composables/useGatedVerdict'
import { kvRefusalText } from '@/composables/useKvView'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useIdentity } from '@/stores/identity'
import { routeSupport } from '@/stores/routeSupport'

const { epoch } = useIdentity()
const namespace = ref('queen-supervisor'), sourceGroup = ref('')
const namespaceDraft = ref(namespace.value), groupDraft = ref(''), sourceError = ref('')
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
  read()
}
function checkAgain() { routeSupport.forget('kv'); verdict.value = null; refresh(true) }
watch(epoch, () => { clearSource(); read() })
onMounted(() => read())
// The page's own Refresh button always reads. The shell's refresh and its one
// ticker, which is suspended while hidden, stop after stable KV refusals.
useAutoRefresh(() => refresh())
onBeforeUnmount(() => { sequence++; controller?.abort(); activityReader.clear(); drawer.value?.close() })
const rows = computed(() => supervisorObservations(entries.value, readAt.value || Date.now(), Boolean(error.value)))
const attention = computed(() => groups.value.filter(group => ['bad', 'warn'].includes(group.tone)).length)
const stale = computed(() => rows.value.filter(row => !row.fresh).length)
const workers = computed(() => {
  const recent = rows.value.filter(row => row.fresh && row.state === 'running')
  return recent.some(row => row.workers === null) ? null : recent.reduce((n, row) => n + row.workers, 0)
})
const search = ref(''), filter = ref('all'), groupFilter = ref(null), page = ref(1)
const groups = computed(() => supervisorGroups(rows.value, { partial: Boolean(after.value) }))
const fleetTone = computed(() => error.value || after.value ? 'warn' : groups.value.some(group => group.tone === 'bad') ? 'bad' : attention.value ? 'warn' : groups.value.length && groups.value.every(group => group.tone === 'good') ? 'good' : 'idle')
const fleetLabel = computed(() => error.value ? 'Current status unconfirmed' : !readAt.value ? 'Waiting for publications' : after.value ? 'Partial view · more publications available' : attention.value ? `${attention.value} ${attention.value === 1 ? 'application needs' : 'applications need'} attention` : !groups.value.length ? 'No publications loaded' : fleetTone.value === 'good' ? 'No issues reported' : 'Some instances are not running')
const engines = computed(() => [...new Set(rows.value.map(row => row.engine).filter(Boolean))].sort())
const filtered = computed(() => filterSupervisorGroups(groups.value, {
  search: search.value, filter: filter.value, group: groupFilter.value,
}))
const pages = computed(() => Math.max(1, Math.ceil(filtered.value.length / 10)))
const pageGroups = computed(() => filtered.value.slice((page.value - 1) * 10, page.value * 10))

watch([search, filter, groupFilter], () => { page.value = 1 })
watch(pages, n => { page.value = Math.min(page.value, n) })
const errorTitle = computed(() => verdict.value === 'transient' ? 'Cannot read supervisor publications' : describeVerdict(verdict.value, 'Supervisor discovery').title)
const errorDetail = computed(() => verdict.value === 'transient' ? kvRefusalText(error.value) || describeApiError(error.value) : describeVerdict(verdict.value, 'Supervisor discovery').detail)
const number = value => value === null ? '—' : formatNumber(value)
const metric = value => error.value || !readAt.value ? '—' : number(value)
const stateLabel = (value, yes, no, unknown) => value === true ? yes : value === false ? no : unknown
const seconds = value => value === null || value === undefined ? 'Not reported' : `${value}s`
const uptimeLabel = value => value === null ? 'Not reported' : value >= 86400 ? `${Math.floor(value / 86400)}d ${Math.floor(value % 86400 / 3600)}h` : value >= 3600 ? `${Math.floor(value / 3600)}h ${Math.floor(value % 3600 / 60)}m` : value >= 60 ? `${Math.floor(value / 60)}m ${value % 60}s` : `${value}s`
const time = value => new Date(value).toLocaleTimeString()
const ageLabel = age => age === null ? 'Age unknown' : age < -5 ? 'Timestamp is in the future' : age < 60 ? `${Math.max(0, age)}s before read` : age < 3600 ? `${Math.floor(age / 60)}m ${age % 60}s before read` : `${Math.floor(age / 3600)}h before read`

const selectedSlot = ref(null), drawer = ref(null), poolSearch = ref(''), poolFilter = ref('all'), poolPage = ref(1), expandedPool = ref(null)
const POOLS_PER_PAGE = 10
const selected = computed(() => rows.value.find(row => row.slot === selectedSlot.value))
const canDiagnosePools = computed(() => selected.value?.affectedPools !== null && selected.value?.fresh && selected.value?.state === 'running' && !error.value)
const poolKey = pool => JSON.stringify([pool.name, pool.queue])
const poolMatches = computed(() => selected.value?.pools.filter(pool => {
  const term = poolSearch.value.trim().toLocaleLowerCase()
  return `${pool.name} ${pool.queue}`.toLocaleLowerCase().includes(term)
    && (poolFilter.value === 'all' || (poolFilter.value === 'attention' && canDiagnosePools.value && pool.severity) || (poolFilter.value === 'draining' && pool.draining > 0))
}) || [])
const poolPages = computed(() => Math.max(1, Math.ceil(poolMatches.value.length / POOLS_PER_PAGE)))
const shownPools = computed(() => poolMatches.value.slice((poolPage.value - 1) * POOLS_PER_PAGE, poolPage.value * POOLS_PER_PAGE))
watch([poolSearch, poolFilter], () => { poolPage.value = 1; expandedPool.value = null })
watch(poolPage, () => { expandedPool.value = null })
watch(poolPages, n => { poolPage.value = Math.min(poolPage.value, n) })
watch(canDiagnosePools, canDiagnose => { if (!canDiagnose && poolFilter.value === 'attention') poolFilter.value = 'all' })
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
watch(selectedSlot, async slot => {
  poolSearch.value = ''; poolFilter.value = 'all'; poolPage.value = 1; expandedPool.value = null
  await nextTick()
  if (slot && selectedSlot.value === slot && drawer.value && !drawer.value.open) {
    drawer.value.dataset.drawerMotion = 'enter'; drawer.value.showModal()
  } else if (!selectedSlot.value) drawer.value?.close()
})
const onDialogClose = () => { if (!drawer.value?.open) selectedSlot.value = null }
let backdropDown = false
function outside(event) {
  const rect = drawer.value?.getBoundingClientRect()
  return rect && (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom)
}
function onDrawerClick(event) { if (backdropDown && outside(event)) closeDrawer(); backdropDown = false }
</script>

<style scoped>
.supervisor-stamp { font-size: 12px; color: var(--text-low); font-variant-numeric: tabular-nums; }
.supervisor-empty { padding: 32px 0; color: var(--text-mid); font-size: 13px; line-height: 1.7; }
.supervisor-empty h3 { color: var(--text-hi); font-size: 17px; margin: 0 0 10px; }
.supervisor-command { display: inline-block; background: var(--ink-3); border: 1px solid var(--bd); padding: 12px 16px; margin: 8px 0; overflow-wrap: anywhere; }
.supervisor-cards { display: grid; grid-template-columns: minmax(0, 1fr); list-style: none; gap: 12px; padding: 0; margin: 0; }.supervisor-cards > li { min-width: 0; }
.supervisor-partial { color: var(--warn-400); font-size: 12px; margin-top: 14px; }
.supervisor-footer { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 8px 24px; align-items: baseline; padding-top: 16px; }
.supervisor-note { color: var(--text-low); font-size: 12px; line-height: 1.6; margin: 0; max-width: 620px; }
.supervisor-source { color: var(--text-low); font-size: 12px; }.supervisor-source summary { cursor: pointer; overflow-wrap: anywhere; }.supervisor-source summary code { margin-left: 8px; font-size: 11px; }
.supervisor-source summary > .muted { display: none; }.supervisor-source[open] { flex-basis: 100%; }
.supervisor-source form { display: flex; flex-wrap: wrap; align-items: end; gap: 14px; padding: 20px 0; }
.supervisor-source label { flex: 1; min-width: 200px; font-size: 12px; color: var(--text-mid); }.supervisor-source input { display: block; width: 100%; margin-top: 7px; }
.supervisor-source form p { flex-basis: 100%; margin: 0; color: var(--text-low); font-size: 12px; }
.supervisor-readiness { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; color: var(--text-mid); font-size: 11px; }
.supervisor-readiness span { border: 1px solid var(--bd); border-radius: 4px; padding: 4px 7px; }
.supervisor-runtime, .supervisor-pool-settings { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin: 18px 0; }
.supervisor-runtime dt, .supervisor-pool-settings dt { color: var(--text-low); font-size: 11px; }
.supervisor-runtime dd, .supervisor-pool-settings dd { margin: 5px 0 0; color: var(--text-hi); font-size: 12px; overflow-wrap: anywhere; }
.supervisor-pagination { display: flex; gap: 14px; align-items: center; flex-wrap: wrap; padding: 14px 18px; border-top: 1px solid var(--bd); color: var(--text-low); font-size: 12px; }
.supervisor-pagination > div { display: flex; gap: 10px; align-items: center; margin-left: auto; }
.supervisor-pagination > button { margin-left: auto; }
.panel-err { margin-bottom: 18px; }
.panel-err .btn { margin-left: 12px; }
.supervisor-drawer { inset: 0 0 0 auto; margin: 0 0 0 auto; padding: 0; width: min(920px, 100%); max-width: 100%; height: 100dvh; max-height: 100dvh; border: 0; border-left: 1px solid var(--bd); color: var(--text-hi); background: var(--ink-2); overscroll-behavior: contain; }
.supervisor-drawer[open] { display: flex; flex-direction: column; }
.supervisor-drawer::backdrop { background: rgb(0 0 0 / .18); }
.supervisor-drawer > .card-header { flex-shrink: 0; }
.supervisor-drawer-body { padding: 24px; overflow-y: auto; flex: 1; min-height: 0; overscroll-behavior: contain; }
.supervisor-drawer-body h2 { font-size: 23px; font-weight: 550; margin: 0 0 6px; overflow-wrap: anywhere; }
.supervisor-app { margin: 0 0 18px; color: var(--text-low); font-size: 12px; overflow-wrap: anywhere; }
.supervisor-detail-status { display: flex; align-items: center; gap: 8px; font-size: 13px; }
.supervisor-next { background: var(--ink-3); border-radius: var(--r-card); padding: 14px 16px; margin: 18px 0; }
.supervisor-next span { color: var(--text-low); font-size: 10px; text-transform: uppercase; letter-spacing: .06em; }
.supervisor-next p { margin: 6px 0 0; font-size: 13px; line-height: 1.6; }
.supervisor-evidence { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); border: 1px solid var(--bd); border-radius: var(--r-card); margin: 20px 0 12px; }
.supervisor-evidence > div { padding: 16px; min-width: 0; }
.supervisor-evidence > div + div { border-left: 1px solid var(--bd); }
.supervisor-evidence dt { color: var(--text-low); font-size: 11px; }
.supervisor-evidence dd { margin: 8px 0; font-size: 24px; font-variant-numeric: tabular-nums; }
.supervisor-evidence small, .supervisor-heartbeat { font-size: 11px; color: var(--text-low); line-height: 1.6; }
.supervisor-last-values { font-size: 12px; color: var(--warn-400); }
.supervisor-pools { margin: 28px 0; }
.supervisor-pools-heading { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 8px; align-items: baseline; margin-bottom: 14px; }
.supervisor-pools-heading h3 { font-size: 16px; margin: 0; }
.supervisor-pools-heading > span { color: var(--text-low); font-size: 12px; }
.supervisor-pool-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; margin-bottom: 14px; }
.supervisor-pool-tools label:first-child { flex: 1; min-width: 200px; }
.supervisor-pool-tools input { width: 100%; }
.supervisor-pool-tools > span { color: var(--text-low); font-size: 11px; }
.supervisor-pool-table-wrap { overflow-x: auto; border: 1px solid var(--bd); border-radius: var(--r-card); }
.supervisor-pool-table { width: 100%; min-width: 660px; border-collapse: collapse; font-size: 12px; }
.supervisor-pool-table th, .supervisor-pool-table td { padding: 13px 12px; border-bottom: 1px solid var(--bd-soft); vertical-align: middle; }
.supervisor-pool-table thead th { background: var(--ink-3); text-align: left; font-weight: 400; color: var(--text-low); font-size: 11px; }
.supervisor-pool-table thead small { display: block; margin-top: 4px; }
.supervisor-pool-table tbody th { font-weight: 500; width: 29%; }
.supervisor-pool-table td:nth-child(2) { width: 24%; }
.supervisor-pool-table thead th:nth-child(n+3), .supervisor-pool-count { text-align: right; font-variant-numeric: tabular-nums; }
.supervisor-pool-count { white-space: nowrap; }
.supervisor-pool-toggle { display: flex; align-items: start; gap: 8px; border: 0; padding: 0; width: 100%; background: none; color: var(--text-hi); font: inherit; text-align: left; cursor: pointer; overflow-wrap: anywhere; }
.supervisor-pool-toggle > span:first-child { color: var(--text-low); }
.supervisor-pool-toggle small { display: block; margin-top: 5px; font-size: 11px; color: var(--text-low); }
.pool-expanded, .supervisor-pool-detail { background: var(--ink-3); }
.supervisor-pool-detail td { padding: 14px 20px; }
.supervisor-pool-next, .supervisor-restarts { color: var(--text-mid); font-size: 12px; line-height: 1.6; margin: 0 0 8px; }
.supervisor-pids { color: var(--text-mid); font-size: 11px; overflow-wrap: anywhere; }
.supervisor-pids summary, .supervisor-publication summary { cursor: pointer; }
.supervisor-pids p { line-height: 1.8; margin-bottom: 0; }
.supervisor-pools-empty { padding: 24px !important; color: var(--text-low); text-align: center; }
.supervisor-pool-pagination { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 10px; margin-top: 10px; color: var(--text-low); font-size: 12px; }
.supervisor-pool-pagination > div { display: flex; gap: 8px; }
.supervisor-publication { border-top: 1px solid var(--bd); padding-top: 18px; font-size: 12px; color: var(--text-mid); line-height: 1.6; }
.supervisor-publication dt { color: var(--text-low); margin-top: 12px; }
.supervisor-publication dd { margin: 3px 0; font-family: var(--font-mono); font-size: 11px; overflow-wrap: anywhere; }
.bad { color: var(--ember-400); }.warn { color: var(--warn-400); }
@media (max-width: 1100px) {
}
@media (max-width: 760px) {
  .supervisor-runtime, .supervisor-pool-settings { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .supervisor-evidence { grid-template-columns: 1fr 1fr; }.supervisor-evidence > div:nth-child(3) { border-left: 0; }.supervisor-evidence > div:nth-child(n+3) { border-top: 1px solid var(--bd); }
  .supervisor-drawer-body { padding: 20px; }
}
</style>
