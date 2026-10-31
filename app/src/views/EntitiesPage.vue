<script setup>
// Entities: what happened to one of them. An entity is anything your workers
// trace by name (an order, an account, a chat): its timeline crosses queues.
// Today it is built from the trace API; a delivery history per message, and
// finding a partition by name across queues, need the broker.
import { computed, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { traces as tracesApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import ConsoleShell from '@/console/ConsoleShell.vue'
import QcSeg from '@/console/QcSeg.vue'
import QcJson from '@/console/QcJson.vue'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { span } from '@/console/useOverview'
import { fmt, groupLabel } from '@/console/useQueuePage'

const route = useRoute()
const router = useRouter()
const frame = useConsoleFrame()
const name = computed(() => (typeof route.query.name === 'string' ? route.query.name : ''))
const path = computed(() => (name.value ? [{ label: 'Entities', to: '/entities' }, { label: name.value, mono: true }] : [{ label: 'Entities' }]))

// ---- the entities the broker has seen, newest first ----
const names = shallowRef(null)
const namesError = ref(null)
const filter = ref('')
async function loadNames() {
  try {
    const r = await tracesApi.getAvailableNames({ limit: 200 }, { quiet: true })
    names.value = r.data
    namesError.value = null
  } catch (e) { namesError.value = describeApiError(e) }
}
const list = computed(() => {
  const rows = (names.value?.trace_names || []).map((n) => ({
    name: n.trace_name, events: n.message_count ?? n.trace_count ?? 0, last: n.last_seen ? Date.parse(n.last_seen) : null,
  }))
  const f = filter.value.trim().toLowerCase()
  return f ? rows.filter((r) => r.name.toLowerCase().includes(f)) : rows
})
const exact = computed(() => {
  const f = filter.value.trim()
  return f && !list.value.some((r) => r.name === f) ? f : ''
})
function pick(n) { sheet.value = false; router.push({ query: { ...route.query, name: n || undefined, q: undefined } }) }

// ---- one entity's timeline ----
const data = shallowRef(null)
const error = ref(null)
const loading = ref(false)
async function loadTimeline() {
  if (!name.value) { data.value = null; return }
  loading.value = true
  const n = name.value
  try {
    const r = await tracesApi.getByName(n, { limit: 200 }, { quiet: true })
    if (n === name.value) { data.value = r.data; error.value = null }
  } catch (e) { error.value = describeApiError(e) } finally { loading.value = false }
}
function refresh() { loadNames(); loadTimeline() }
useAutoRefresh(refresh)
refresh()
watch(name, () => { open.value = null; loadTimeline() })

const queue = computed({
  get: () => (typeof route.query.q === 'string' ? route.query.q : ''),
  set: (v) => router.replace({ query: { ...route.query, q: v || undefined } }),
})
const events = computed(() => {
  const all = data.value?.events || data.value?.traces || []
  return [...all].sort((a, b) => Date.parse(b.createdAt || b.created_at) - Date.parse(a.createdAt || a.created_at))
})
const queues = computed(() => {
  const m = new Map()
  for (const e of events.value) { const q = e.queue_name || '–'; m.set(q, (m.get(q) || 0) + 1) }
  return [...m.entries()].sort((a, b) => b[1] - a[1])
})
const queueOptions = computed(() => [{ value: '', label: 'All', count: events.value.length }, ...queues.value.map(([q, n]) => ({ value: q, label: q, count: n }))])
const shown = computed(() => (queue.value ? events.value.filter((e) => (e.queue_name || '–') === queue.value) : events.value))
const facts = computed(() => {
  const ev = events.value
  if (!ev.length) return ''
  const t = (e) => Date.parse(e.createdAt || e.created_at)
  const first = t(ev[ev.length - 1]); const last = t(ev[0])
  const total = data.value?.total ?? ev.length
  const k = queues.value.length
  return `${fmt(total)} event${total === 1 ? '' : 's'} in ${k} queue${k === 1 ? '' : 's'}, from ${stamp(first)} to ${stamp(last)}${total > ev.length ? `; the newest ${fmt(ev.length)} here` : ''}.`
})
const today = new Date().toDateString()
function stamp(ms) {
  const d = new Date(ms)
  const t = d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  return d.toDateString() === today ? t : d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' }) + ' ' + t
}
const open = ref(null)
const rows = computed(() => shown.value.map((e, i) => ({
  key: (e.traceId || e.trace_id || '') + i,
  at: stamp(Date.parse(e.createdAt || e.created_at)),
  type: e.eventType || e.event_type || 'event',
  bad: /error|fail/i.test(e.eventType || e.event_type || ''),
  queue: e.queue_name || '',
  partition: e.partition_name || '',
  txn: e.transactionId || e.transaction_id || '',
  group: e.consumerGroup || e.consumer_group || '',
  worker: e.worker_id || e.worker || '',
  data: e.data,
  others: (e.trace_names || e.names || []).filter((n) => n !== name.value),
})))
const sheet = ref(false)
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="en">
      <button type="button" class="en-bar qn-btn2" @click="sheet = !sheet">Entities<span v-if="name" class="qn-mono qn-t2" style="margin-left: 8px">· {{ name }}</span></button>
      <aside class="en-side" :class="{ open: sheet }" aria-label="Entities">
        <label class="qn-field">
          <span class="qn-vh">Find an entity</span>
          <input v-model="filter" class="qn-mono" type="text" placeholder="Find or paste an id" spellcheck="false" @keydown.enter="exact && pick(exact)" />
        </label>
        <button v-if="exact" type="button" class="en-item" @click="pick(exact)"><span>Open <span class="qn-mono">{{ exact }}</span></span></button>
        <p v-if="namesError" class="qn-t2" style="padding: 10px 2px">The broker did not answer: {{ namesError }}</p>
        <p v-else-if="names && !list.length && !exact" class="qn-t2" style="padding: 10px 2px">{{ filter ? `Nothing matches “${filter}”.` : 'No entity has been traced yet.' }}</p>
        <nav class="en-list">
          <button v-for="r in list" :key="r.name" type="button" class="en-item" :class="{ on: r.name === name }" :aria-current="r.name === name ? 'true' : undefined" @click="pick(r.name)">
            <span class="qn-mono en-name" :title="r.name">{{ r.name }}</span>
            <span class="qn-num qn-t3 en-meta">{{ fmt(r.events) }}<template v-if="r.last"> · {{ span((Date.now() - r.last) / 1000) }} ago</template></span>
          </button>
        </nav>
        <p v-if="names" class="qn-read" style="margin-top: 10px">The {{ fmt((names.trace_names || []).length) }} traced last<template v-if="names.total > (names.trace_names || []).length">, of {{ fmt(names.total) }}</template>. <span class="qn-mono">GET /api/v1/traces/names</span></p>
      </aside>

      <section class="en-main">
        <template v-if="!name">
          <h1 class="en-title">What happened to one of them?</h1>
          <p class="qn-t2" style="margin-top: 10px; max-width: 640px">An entity is anything your workers trace by name: an order, an account, a chat. Pick one on the left, or paste its id. Its timeline crosses every queue that worked on it, newest first.</p>
          <p class="qn-read" style="margin-top: 14px; max-width: 640px">A worker writes these with the trace call. A delivery the worker did not trace is not here: the broker keeps no delivery history per message yet.</p>
        </template>
        <template v-else>
          <h1 class="qn-mono en-title">{{ name }}</h1>
          <p v-if="facts" class="en-facts">{{ facts }}</p>
          <p v-else-if="loading && !data" class="qn-t2 en-facts">Reading its timeline…</p>
          <p v-else-if="error" class="qn-t2 en-facts">The broker did not answer: {{ error }}</p>
          <p v-else-if="data" class="en-facts">Nothing traced under this name.</p>
          <div v-if="queues.length > 1" style="margin-top: 14px; overflow-x: auto"><QcSeg v-model="queue" :options="queueOptions" label="Queue" /></div>
          <ol v-if="rows.length" class="en-time">
            <li v-for="r in rows" :key="r.key" class="en-ev" :class="{ open: open === r.key }">
              <button type="button" class="en-ev-row" :aria-expanded="open === r.key" @click="open = open === r.key ? null : r.key">
                <span class="qn-num qn-t2 en-at">{{ r.at }}</span>
                <span class="en-type" :class="r.bad ? 'red' : 'qn-t3'"><span v-if="r.bad" class="qn-mark red" aria-hidden="true" />{{ r.type }}</span>
                <span class="en-where">
                  <RouterLink v-if="r.queue" :to="`/queues/${encodeURIComponent(r.queue)}`" class="qn-mono" @click.stop>{{ r.queue }}</RouterLink>
                  <span v-if="r.partition" class="qn-t3" aria-hidden="true"> / </span><span v-if="r.partition" class="qn-mono qn-t2">{{ r.partition }}</span>
                </span>
                <span class="qn-mono qn-t3 en-txn" :title="r.txn">{{ r.txn }}</span>
              </button>
              <div v-if="open === r.key" class="en-more">
                <dl class="qn-dl">
                  <dt>transactionId</dt><dd class="qn-mono">{{ r.txn || '–' }}</dd>
                  <dt>Group</dt><dd class="qn-mono">{{ r.group ? groupLabel(r.group) : '–' }}</dd>
                  <template v-if="r.worker"><dt>Worker</dt><dd class="qn-mono">{{ r.worker }}</dd></template>
                  <template v-if="r.others.length"><dt>Also traced as</dt><dd><button v-for="o in r.others" :key="o" type="button" class="qn-link qn-mono" style="margin-right: 10px" @click="pick(o)">{{ o }}</button></dd></template>
                </dl>
                <QcJson v-if="r.data != null" :value="r.data" style="margin-top: 10px" />
              </div>
            </li>
          </ol>
          <p class="qn-read" style="margin-top: 14px">Read from <span class="qn-mono">GET /api/v1/traces/by-name/{{ name }}</span>, newest first. A delivery the worker did not trace is not here: the broker keeps no delivery history per message yet.</p>
        </template>
      </section>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.en { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; display: grid; grid-template-columns: 300px minmax(0, 1fr); gap: 32px; align-items: start; }
.en-bar { display: none; }
.en-side { position: sticky; top: 78px; max-height: calc(100vh - 110px); display: flex; flex-direction: column; min-height: 0; }
.en-list { margin-top: 10px; overflow-y: auto; min-height: 0; display: flex; flex-direction: column; gap: 2px; }
.en-item { display: flex; align-items: baseline; gap: 10px; width: 100%; text-align: left; padding: 7px 10px; border-radius: var(--r-ctl); color: var(--text2); }
.en-item:hover { background: var(--bg2); color: var(--text); }
.en-item.on { background: var(--bg3); color: var(--text); }
.en-name { flex: 1 1 0; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 12.5px; }
.en-meta { flex: 0 0 auto; font-size: 11.5px; }
.en-title { font-size: 20px; font-weight: 500; overflow-wrap: anywhere; }
.en-facts { margin-top: 10px; font-size: 14.5px; }
.en-time { margin-top: 16px; border-top: 1px solid var(--line); }
.en-ev { border-bottom: 1px solid var(--line2); }
.en-ev-row { display: grid; grid-template-columns: 150px 96px minmax(0, 1.5fr) minmax(0, 1fr); align-items: center; gap: 0 14px; width: 100%; min-height: 40px; text-align: left; padding: 0 6px; }
.en-ev-row:hover, .en-ev.open .en-ev-row { background: var(--bg2); }
.en-ev-row > * { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.en-type { display: inline-flex; align-items: center; gap: 6px; }
.en-more { padding: 12px 6px 16px; }
:global(.qn.tone .en-time) { border-top-color: var(--bg3); }
@media (max-width: 900px) { .en-ev-row { grid-template-columns: 120px 80px minmax(0, 1fr); } .en-txn { display: none; } }
@media (max-width: 640px) {
  .en { grid-template-columns: minmax(0, 1fr); padding: 18px 16px 40px; gap: 14px; }
  .en-bar { display: inline-flex; justify-content: flex-start; }
  .en-side { display: none; position: static; max-height: none; }
  .en-side.open { display: flex; }
}
</style>
