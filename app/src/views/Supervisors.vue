<template>
  <div class="view-container supervisors-page">
    <PageHead title="Supervisors" sub="Published worker status">
      <template #actions>
        <span class="supervisor-stamp">{{ loading ? 'Reading…' : readAt ? `Read at ${time(readAt)}` : 'Not read yet' }}</span>
        <button class="btn" :disabled="loading" @click="refresh(true)">Refresh</button>
      </template>
    </PageHead>

    <div class="supervisor-metrics" aria-label="Supervisor metrics from loaded publications">
      <div><span>Instances loaded</span><strong>{{ metric(rows.length) }}</strong><small>{{ groups.length }} groups loaded{{ after ? ' · more available' : '' }}</small></div>
      <div><span>Need attention</span><strong :class="attention ? 'warn' : ''">{{ metric(attention) }}</strong><small>Pool or publication issues</small></div>
      <div><span>Workers reported</span><strong>{{ metric(workers) }}</strong><small>Recent heartbeats only</small></div>
      <div><span>Stale / unreadable</span><strong :class="stale ? 'warn' : ''">{{ metric(stale) }}</strong><small>Current health unconfirmed</small></div>
    </div>

    <details class="card supervisor-source">
      <summary>Source <code>{{ namespace }}</code><span v-if="sourceGroup">· {{ sourceGroup }}</span><span class="muted">Change source</span></summary>
      <form @submit.prevent="applySource">
        <label>KV namespace<input v-model="namespaceDraft" class="input" required maxlength="64" /></label>
        <label>Group <span class="muted">optional</span><input v-model="groupDraft" class="input" placeholder="e.g. pmsintool" maxlength="255" /></label>
        <button class="btn" type="submit">Read source</button>
        <p v-if="sourceError" class="warn" role="alert">{{ sourceError }}</p>
        <p>Reads the acting cluster’s KV store. A group matches exactly; leave it empty to discover all groups. Laravel publishers set it through <code>remote_status.key</code>.</p>
      </form>
    </details>

    <div v-if="error" class="panel-err" role="status">
      <strong>{{ errorTitle }}</strong><p>{{ errorDetail }}</p>
      <span v-if="readAt">Last successful read: {{ time(readAt) }}. Current health is unconfirmed.</span>
      <button v-if="verdict !== 'transient'" class="btn btn-ghost" :disabled="loading" @click="checkAgain">Check again</button>
    </div>

    <section class="card supervisor-list" aria-labelledby="supervisor-list-title">
      <div class="card-header">
        <h3 id="supervisor-list-title">Supervisor instances</h3>
        <span class="muted">Published status · read only</span>
      </div>
      <div class="supervisor-tools">
        <label class="supervisor-search"><span class="sr-only">Search group, host or queue</span><input v-model="search" class="input" type="search" placeholder="Search group, host or queue…" /></label>
        <label><span class="sr-only">Filter supervisors</span><select v-model="filter" class="input"><option value="all">All instances</option><option value="attention">Needs attention</option><option value="stale">Stale / unreadable</option><option v-for="engine in engines" :key="engine" :value="`engine:${engine}`">{{ engine }} engine</option></select></label>
        <label><span class="sr-only">Filter group</span><select v-model="groupFilter" class="input"><option :value="null">All loaded groups</option><option v-for="group in groups" :key="group.name" :value="group.name">{{ group.name }} · {{ group.count }}</option></select></label>
        <span class="muted">Groups with issues first</span>
      </div>
      <div v-if="loading && !readAt" class="supervisor-empty" role="status">Reading published supervisor status…</div>
      <div v-else-if="!rows.length && !error && readAt" class="supervisor-empty">
        <h3>No published supervisors in this source</h3>
        <p>Enable remote status on the application hosts, then restart the supervisor with the updated configuration.</p>
        <code class="supervisor-command">QUEEN_SUPERVISOR_REMOTE_STATUS=true</code>
        <p>Laravel’s PHP and Rust engines already publish this format. Other implementations can use the same versioned contract. Check that the publisher connects to this cluster and uses the namespace above.</p>
        <p>Publications expire after their configured TTL. An empty list does not establish that no supervisors are running.</p>
      </div>
      <div v-else-if="!filtered.length && !error" class="supervisor-empty">No loaded instances match these filters.</div>
      <div v-else class="supervisor-overviews">
        <section v-for="group in pageGroups" :key="group.name" class="supervisor-overview-group" :aria-label="`Group ${group.name}`">
          <div class="supervisor-group"><h4>{{ group.name }}</h4><span>{{ groupCounts.get(group.name) }} loaded instances</span></div>
          <ul class="supervisor-cards">
            <li v-for="row in group.rows" :key="row.slot">
              <button class="supervisor-card" aria-haspopup="dialog" aria-controls="supervisor-drawer" :aria-expanded="selectedSlot === row.slot" @click="selectedSlot = row.slot">
                <span class="supervisor-card-head"><strong>{{ row.hostname || row.group }}</strong><span v-if="row.engine" class="supervisor-engine">{{ row.engine.toUpperCase() }}</span></span>
                <span class="supervisor-instance">{{ row.instance ? `Instance …${row.instance.slice(-8)}` : 'Instance unavailable' }} · {{ row.state || 'unknown state' }}</span>
                <span class="supervisor-finding" :class="row.severity"><span class="g" :class="row.severity || 'idle'" aria-hidden="true" />{{ row.label }}</span>
                <span class="supervisor-card-metrics">
                  <span><small>Queues</small><strong>{{ number(row.queueCount) }}</strong><small>{{ number(row.poolCount) }} pools</small></span>
                  <span><small>Workers</small><strong>{{ number(row.workers) }} <span>/ {{ number(row.desired) }}</span></strong><small>running / desired</small></span>
                  <span><small>Pools needing attention</small><strong :class="row.affectedPools ? 'warn' : ''">{{ number(row.affectedPools) }}</strong><small>{{ row.affectedPools === null ? 'Health unconfirmed' : 'From this heartbeat' }}</small></span>
                </span>
                <span class="supervisor-card-capacity"><span>{{ number(row.missingWorkers) }} below target</span><span>{{ number(row.draining) }} draining</span><span>{{ number(row.budget?.available ?? null) }} process slots free</span></span>
                <span class="supervisor-card-foot"><span>{{ row.age === null ? 'Heartbeat unavailable' : `Heartbeat ${ageLabel(row.age)}` }}</span><span class="supervisor-open" aria-hidden="true">View details →</span></span>
              </button>
            </li>
          </ul>
        </section>
      </div>
      <div v-if="pages > 1 || after" class="supervisor-pagination">
        <span>{{ formatNumber(filtered.length) }} matching loaded instances</span>
        <div v-if="pages > 1"><button class="btn btn-ghost" :disabled="page === 1" @click="page--">Previous</button><span>{{ page }} / {{ pages }}</span><button class="btn btn-ghost" :disabled="page === pages" @click="page++">Next</button></div>
        <button v-if="after" class="btn" :disabled="loading" @click="loadMore">Load more publications</button>
      </div>
    </section>
    <p class="supervisor-note">Status is a snapshot published by each supervisor. Heartbeat freshness does not prove the process is alive. Queue depths may overlap between instances and are not added together.</p>

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
            <p class="supervisor-readiness"><span>{{ stateLabel(selected.readiness, 'Ready', 'Not ready', 'Readiness unconfirmed') }}</span><span>{{ stateLabel(selected.capacity, 'Target reached', 'Below target', 'Capacity unconfirmed') }}</span></p>
            <dl class="supervisor-evidence">
              <div><dt>Queues</dt><dd>{{ number(selected.queueCount) }}</dd><small>{{ number(selected.poolCount) }} pools</small></div>
              <div><dt>Running / desired</dt><dd>{{ number(selected.workers) }} / {{ number(selected.desired) }}</dd><small>{{ number(selected.missingWorkers) }} below target · {{ number(selected.draining) }} draining</small></div>
              <div><dt>Pools needing attention</dt><dd :class="selected.affectedPools ? 'warn' : ''">{{ number(selected.affectedPools) }}</dd><small>{{ selected.affectedPools === null ? 'Health unconfirmed' : 'From this heartbeat' }}</small></div>
              <div><dt>Process budget</dt><dd>{{ selected.budget ? `${selected.budget.used} / ${selected.budget.limit}` : '—' }}</dd><small v-if="selected.budget">{{ selected.budget.available }} available · {{ selected.budget.helpers }} renewal helpers</small><small v-else>Not reported or inconsistent</small></div>
            </dl>
            <p class="supervisor-heartbeat">Last heartbeat {{ selected.updatedAt ? time(selected.updatedAt) : 'unavailable' }} · {{ ageLabel(selected.age) }} · timeout {{ selected.timeout ? `${selected.timeout}s` : 'unavailable' }}</p>
            <dl class="supervisor-runtime"><div><dt>Engine version</dt><dd>{{ selected.engineVersion || 'Not reported' }}</dd></div><div><dt>Client version</dt><dd>{{ selected.clientVersion || 'Not reported' }}</dd></div><div><dt>Master PID</dt><dd>{{ number(selected.pid) }}</dd></div><div><dt>Uptime at heartbeat</dt><dd>{{ uptimeLabel(selected.uptime) }}</dd></div></dl>
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
                  <thead><tr><th scope="col">Queue / pool</th><th scope="col">Finding</th><th scope="col">Workers <small>running / desired</small></th><th scope="col">Pending</th><th scope="col">Draining</th></tr></thead>
                  <tbody>
                    <template v-for="pool in shownPools" :key="poolKey(pool)">
                      <tr :class="{ 'pool-expanded': expandedPool === poolKey(pool) }">
                        <th scope="row"><button class="supervisor-pool-toggle" :aria-expanded="expandedPool === poolKey(pool)" :aria-controls="expandedPool === poolKey(pool) ? 'supervisor-pool-detail' : undefined" @click="expandedPool = expandedPool === poolKey(pool) ? null : poolKey(pool)"><span aria-hidden="true">{{ expandedPool === poolKey(pool) ? '▾' : '▸' }}</span><span>{{ pool.queue }}<small>{{ pool.name }}</small></span></button></th>
                        <td><span v-if="canDiagnosePools" class="supervisor-pool-finding" :class="pool.severity">{{ pool.label }}</span><span v-else class="muted">{{ selected.state === 'running' ? 'Unconfirmed' : 'Last reported' }}</span></td>
                        <td class="supervisor-pool-count">{{ number(pool.running) }} / {{ number(pool.desired) }}</td><td class="supervisor-pool-count">{{ number(pool.depth) }}</td><td class="supervisor-pool-count">{{ number(pool.draining) }}</td>
                      </tr>
                      <tr v-if="expandedPool === poolKey(pool)" id="supervisor-pool-detail" class="supervisor-pool-detail"><td colspan="5">
                        <p class="supervisor-readiness"><span>{{ canDiagnosePools ? stateLabel(pool.readiness, 'Ready', 'Not ready', 'Readiness not reported') : 'Readiness unconfirmed' }}</span><span>{{ canDiagnosePools ? stateLabel(pool.capacity, 'Target reached', 'Below target', 'Capacity not reported') : 'Capacity unconfirmed' }}</span></p>
                        <p v-if="canDiagnosePools" class="supervisor-pool-next">{{ pool.next }}</p>
                        <p v-else class="supervisor-pool-next">Values from the last publication. Current pool health is unconfirmed.</p>
                        <p v-if="pool.failures" class="supervisor-restarts">{{ pool.failures }} restart failures<template v-if="pool.retryIn !== null"> · retry in {{ pool.retryIn }}s at publication</template></p>
                        <dl class="supervisor-pool-settings">
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
                        <p class="supervisor-pool-next">Pool limits apply to {{ pool.name }} across its configured queues. Running / desired is this queue’s allocation.</p>
                        <SupervisorQueueContext :queue="pool.queue" />
                        <details v-if="pool.pids.length" class="supervisor-pids"><summary>Reported worker PIDs ({{ pool.pids.length }})</summary><p class="font-mono">{{ pool.pids.join(', ') }}</p></details>
                        <p v-else class="supervisor-pool-next">No worker PIDs reported in this heartbeat.</p>
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
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue'
import PageHead from '@/components/PageHead.vue'
import SupervisorQueueContext from '@/components/SupervisorQueueContext.vue'
import { kv, describeApiError } from '@/api'
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
onBeforeUnmount(() => { sequence++; controller?.abort(); drawer.value?.close() })
const rows = computed(() => supervisorObservations(entries.value, readAt.value || Date.now(), Boolean(error.value)))
const attention = computed(() => rows.value.filter(row => row.severity).length)
const stale = computed(() => rows.value.filter(row => !row.fresh).length)
const workers = computed(() => {
  const recent = rows.value.filter(row => row.fresh)
  return recent.some(row => row.workers === null) ? null : recent.reduce((n, row) => n + row.workers, 0)
})
const search = ref(''), filter = ref('all'), groupFilter = ref(null), page = ref(1)
const groups = computed(() => {
  const byName = new Map()
  for (const row of rows.value) {
    const group = byName.get(row.group) || { name: row.group, count: 0, priority: 0 }
    group.count++; group.priority = Math.max(group.priority, row.priority)
    byName.set(row.group, group)
  }
  return [...byName.values()].sort((a, b) => b.priority - a.priority || a.name.localeCompare(b.name))
})
const groupCounts = computed(() => new Map(groups.value.map(group => [group.name, group.count])))
const groupOrder = computed(() => new Map(groups.value.map((group, index) => [group.name, index])))
const engines = computed(() => [...new Set(rows.value.map(row => row.engine).filter(Boolean))].sort())
const filtered = computed(() => rows.value.filter(row => {
  if (groupFilter.value !== null && row.group !== groupFilter.value) return false
  const term = search.value.trim().toLocaleLowerCase()
  if (term && ![row.group, row.hostname, row.instance, ...row.pools.map(p => `${p.name} ${p.queue}`)].join(' ').toLocaleLowerCase().includes(term)) return false
  return filter.value === 'all' || (filter.value === 'attention' && row.severity) || (filter.value === 'stale' && !row.fresh) || `engine:${row.engine}` === filter.value
}).sort((a, b) => groupOrder.value.get(a.group) - groupOrder.value.get(b.group) || b.priority - a.priority || a.slot.localeCompare(b.slot)))
const pages = computed(() => Math.max(1, Math.ceil(filtered.value.length / 10)))
const pageRows = computed(() => filtered.value.slice((page.value - 1) * 10, page.value * 10))
const pageGroups = computed(() => {
  const result = []
  for (const row of pageRows.value) {
    if (result.at(-1)?.name !== row.group) result.push({ name: row.group, rows: [] })
    result.at(-1).rows.push(row)
  }
  return result
})
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
.supervisor-stamp, .supervisor-note { color: var(--text-low); font-size: 12px; }
.supervisor-note { line-height: 1.7; margin-top: 16px; }
.supervisor-metrics { display: grid; grid-template-columns: repeat(4, 1fr); border: 1px solid var(--bd); border-radius: var(--r-card); background: var(--ink-2); margin-bottom: 24px; }
.supervisor-metrics > div { padding: 20px 24px; }
.supervisor-metrics > div + div { border-left: 1px solid var(--bd); }
.supervisor-metrics span, .supervisor-metrics small { display: block; color: var(--text-low); font-size: 12px; }
.supervisor-metrics strong { display: block; margin: 8px 0; font-size: 30px; font-weight: 550; font-variant-numeric: tabular-nums; }
.supervisor-source { margin-bottom: 18px; }
.supervisor-source summary { cursor: pointer; padding: 14px 18px; font-size: 12px; overflow-wrap: anywhere; }
.supervisor-source summary code { margin-left: 14px; }
.supervisor-source summary > .muted { float: right; }
.supervisor-source form { display: flex; flex-wrap: wrap; align-items: end; gap: 14px; padding: 8px 18px 18px; }
.supervisor-source label { flex: 1; min-width: 200px; font-size: 12px; color: var(--text-mid); }
.supervisor-source input { display: block; width: 100%; margin-top: 7px; }
.supervisor-source form p { flex-basis: 100%; margin: 0; color: var(--text-low); font-size: 12px; }
.supervisor-tools { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; padding: 14px 18px; border-bottom: 1px solid var(--bd); }
.supervisor-tools > label { min-width: 0; }
.supervisor-tools > label:not(.supervisor-search) { max-width: 260px; }
.supervisor-tools select { max-width: 100%; }
.supervisor-search { flex: 1; max-width: 420px; }
.supervisor-search input { width: 100%; }
.supervisor-tools > span { margin-left: auto; font-size: 12px; }
.supervisor-empty { padding: 30px 24px; color: var(--text-mid); font-size: 13px; line-height: 1.7; }
.supervisor-empty h3 { color: var(--text-hi); font-size: 17px; margin: 0 0 10px; }
.supervisor-command { display: inline-block; background: var(--ink-3); border: 1px solid var(--bd); border-radius: var(--r-control); padding: 12px 16px; margin: 8px 0; overflow-wrap: anywhere; }
.supervisor-overviews { padding: 0 18px 20px; }
.supervisor-group { display: flex; flex-wrap: wrap; gap: 8px 14px; align-items: center; padding: 20px 0 12px; color: var(--text-mid); font-size: 12px; overflow-wrap: anywhere; }
.supervisor-group h4 { margin: 0; font-size: 13px; font-weight: 600; }
.supervisor-group span { color: var(--text-low); }
.supervisor-cards { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); list-style: none; gap: 14px; padding: 0; margin: 0; }
.supervisor-card { display: block; width: 100%; height: 100%; padding: 20px; color: var(--text-hi); border: 1px solid var(--bd); border-radius: var(--r-card); text-align: left; background: var(--ink-2); font: inherit; cursor: pointer; transition: background .15s, border-color .15s; }
.supervisor-card:hover, .supervisor-card[aria-expanded="true"] { background: var(--ink-3); border-color: var(--text-low); }
.supervisor-card-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.supervisor-card-head > strong { font-size: 17px; font-weight: 550; min-width: 0; overflow-wrap: anywhere; }
.supervisor-engine { color: var(--text-low); border: 1px solid var(--bd); border-radius: 4px; padding: 3px 6px; font-size: 10px; overflow-wrap: anywhere; max-width: 40%; }
.supervisor-instance { display: block; margin-top: 6px; font-size: 11px; color: var(--text-low); }
.supervisor-finding { display: flex; align-items: center; gap: 8px; margin-top: 20px; font-size: 12px; }
.supervisor-card-metrics { display: grid; grid-template-columns: .8fr 1fr 1.2fr; gap: 12px; margin-top: 20px; }
.supervisor-card-metrics small { display: block; color: var(--text-low); font-size: 11px; line-height: 1.5; }
.supervisor-card-metrics strong { display: block; margin: 7px 0 3px; font-size: 27px; font-weight: 550; font-variant-numeric: tabular-nums; }
.supervisor-card-metrics strong > span { font-size: 16px; color: var(--text-low); font-weight: 400; }
.supervisor-card-capacity { display: flex; flex-wrap: wrap; gap: 6px 14px; margin-top: 18px; color: var(--text-mid); font-size: 11px; }
.supervisor-readiness { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; color: var(--text-mid); font-size: 11px; }
.supervisor-readiness span { border: 1px solid var(--bd); border-radius: 4px; padding: 4px 7px; }
.supervisor-runtime, .supervisor-pool-settings { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin: 18px 0; }
.supervisor-runtime dt, .supervisor-pool-settings dt { color: var(--text-low); font-size: 11px; }
.supervisor-runtime dd, .supervisor-pool-settings dd { margin: 5px 0 0; color: var(--text-hi); font-size: 12px; overflow-wrap: anywhere; }
.supervisor-card-foot { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 8px; padding-top: 16px; margin-top: 20px; border-top: 1px solid var(--bd-soft); color: var(--text-low); font-size: 11px; }
.supervisor-open { color: var(--text-mid); }
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
@media (max-width: 1000px) {
  .supervisor-cards { grid-template-columns: 1fr; }
}
@media (max-width: 600px) {
  .supervisor-runtime, .supervisor-pool-settings { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .supervisor-metrics, .supervisor-evidence { grid-template-columns: 1fr 1fr; }
  .supervisor-metrics > div { padding: 16px; }
  .supervisor-metrics > div:nth-child(3), .supervisor-evidence > div:nth-child(3) { border-left: 0; }
  .supervisor-metrics > div:nth-child(n+3), .supervisor-evidence > div:nth-child(n+3) { border-top: 1px solid var(--bd); }
  .supervisor-search { flex-basis: 100%; max-width: none; }
  .supervisor-tools > span { display: none; }
  .supervisor-overviews { padding: 0 12px 12px; }
  .supervisor-card { padding: 16px; }
  .supervisor-card-metrics { gap: 8px; grid-template-columns: .75fr 1fr 1.25fr; }
  .supervisor-card-metrics strong { font-size: 23px; }
  .supervisor-source summary > .muted { float: none; display: block; margin-top: 6px; }
  .supervisor-drawer-body { padding: 20px; }
}
</style>
