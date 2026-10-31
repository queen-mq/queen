<script setup>
// Streams, in the new console: each stream, what it reads and writes, whether
// it keeps up, and the windows past their end that never closed.
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ConsoleShell from '@/console/ConsoleShell.vue'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { useStreamsPage } from '@/console/useStreamsPage'
import { rate, span } from '@/console/useOverview'
import { fmt } from '@/console/useQueuePage'

const route = useRoute()
const router = useRouter()
const frame = useConsoleFrame()
const s = useStreamsPage()

const picked = computed(() => (typeof route.query.name === 'string' ? route.query.name : ''))
function pick(name) { router.replace({ query: { ...route.query, name: picked.value === name ? undefined : name } }) }
const sel = computed(() => s.rows.value.find((r) => r.name === picked.value) || null)
const path = computed(() => (sel.value ? [{ label: 'Streams', to: '/streams' }, { label: sel.value.name, mono: true }] : [{ label: 'Streams' }]))

const ago = (ms) => (ms ? `${span((Date.now() - ms) / 1000)} ago` : '–')
const when = (ms) => (ms ? new Date(ms).toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }) : '–')
const enc = encodeURIComponent
const overdueMin = computed(() => Math.round(s.overdueAfterMs.value / 60000))
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="st">
      <header class="st-head">
        <h1 class="st-title">Streams</h1>
        <span v-if="s.rows.value.length" class="qn-tag">{{ s.rows.value.length }} stream{{ s.rows.value.length === 1 ? '' : 's' }}</span>
      </header>

      <p v-if="s.verdict.value" class="st-verdict">
        <span v-if="s.verdict.value.tone" class="qn-mark" :class="s.verdict.value.tone" aria-hidden="true" />
        <template v-if="s.verdict.value.names">
          <template v-for="(n, i) in s.verdict.value.names" :key="n"><span v-if="i" class="qn-t2"> and </span><span class="qn-mono">{{ n }}</span></template>
          {{ ' ' + s.verdict.value.text }}
        </template>
        <template v-else>{{ s.verdict.value.text }}</template>
      </p>

      <p v-if="s.absent.value" class="st-note qn-panel">
        This broker does not list its streams: <span class="qn-mono">GET /streams/v1/queries</span> comes with the next build.
        Until then the console finds them from their consumer groups (<span class="qn-mono">streams.&lt;name&gt;</span>): what they read and how far behind they are, but not their sinks or their windows.
      </p>
      <p v-if="s.off.value" class="st-note qn-panel">Streams are not turned on for this tenant. An operator turns them on in its quotas.</p>
      <p v-if="s.error.value" class="qn-t2" style="margin-top: 14px">The broker did not answer: {{ s.error.value }}</p>
      <p v-else-if="!s.loading.value && !s.off.value && !s.rows.value.length" class="st-empty">
        No streams yet. A stream is a query a runner keeps over a queue: windows, sums, joins, written to another queue. It shows here once its runner registers it.
      </p>

      <div v-if="s.rows.value.length" class="st-table" role="table" aria-label="Streams" style="--cols: minmax(0, 1fr) minmax(0, 1.8fr) 96px 92px 92px 92px 100px">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Stream</span><span role="columnheader">Reads → writes</span><span role="columnheader" class="r">Behind</span>
          <span role="columnheader" class="r">In a second</span><span role="columnheader" class="r">Out a second</span>
          <span role="columnheader" class="r">Windows</span><span role="columnheader" class="r">Past their end</span>
        </div>
        <template v-for="r in s.rows.value" :key="r.key">
          <div class="qn-tr click" :class="{ on: picked === r.name }" role="row" tabindex="0" @click="pick(r.name)" @keydown.enter="pick(r.name)">
            <span role="cell" style="display: flex; align-items: center; gap: 8px">
              <span v-if="r.needs" class="qn-mark" :class="r.needs" aria-hidden="true" />
              <span class="qn-mono" style="font-weight: 500; overflow: hidden; text-overflow: ellipsis">{{ r.name }}</span>
            </span>
            <span role="cell" class="qn-mono qn-t2">{{ r.source }}<span v-if="r.sink" class="qn-t3" style="margin: 0 8px">→</span><span v-if="r.sink">{{ r.sink }}</span></span>
            <span role="cell" class="r qn-num" :class="r.lagTone">{{ r.lag ? span(r.lag) : 'caught up' }}</span>
            <span role="cell" class="r qn-num">{{ rate(r.inS) }}</span>
            <span role="cell" class="r qn-num">{{ r.sink ? rate(r.outS) : '–' }}</span>
            <span role="cell" class="r qn-num">{{ r.windows == null ? '–' : fmt(r.windows) + (r.exact ? '' : '+') }}</span>
            <span role="cell" class="r qn-num" :class="r.overdue ? 'amber' : 'qn-t3'">{{ r.windows == null ? '–' : fmt(r.overdue) }}</span>
          </div>
          <section v-if="picked === r.name" class="st-open" :aria-label="`Stream ${r.name}`">
            <p class="st-shape">
              <span class="qn-t3">reads</span>
              <RouterLink :to="`/queues/${enc(r.source)}`" class="qn-mono">{{ r.source }}</RouterLink>
              <span class="qn-t3">as</span> <span class="qn-mono">{{ r.groupName }}</span>
              <template v-if="r.state"><span class="qn-t3">· keeps</span> {{ fmt(r.state.windows) }} window{{ r.state.windows === 1 ? '' : 's' }} <span class="qn-t3">on</span> {{ fmt(r.state.partitions) }} partition{{ r.state.partitions === 1 ? '' : 's' }}</template>
              <template v-if="r.sink"><span class="qn-t3">· writes</span> <RouterLink :to="`/queues/${enc(r.sink)}`" class="qn-mono">{{ r.sink }}</RouterLink></template>
            </p>
            <p v-if="r.overdue" class="st-why amber">
              <span class="qn-mark amber" aria-hidden="true" />
              {{ fmt(r.overdue) }} window{{ r.overdue === 1 ? '' : 's' }} ended more than {{ overdueMin }} min ago and never closed<template v-if="r.oldestOverdue">, the oldest {{ ago(r.oldestOverdue) }}</template>.
              <span class="qn-t2">Their results were never sent. A runner closes a quiet window on its next idle pass; when none does, look at the runner, and at who holds claims on those partitions.</span>
            </p>
            <dl class="qn-dl st-dl">
              <template v-if="r.group">
                <dt>Behind</dt><dd>{{ r.lag ? `${span(r.lag)}, ${fmt(r.waiting)} waiting on ${fmt(r.partitionsBehind)} partition${r.partitionsBehind === 1 ? '' : 's'}` : 'Caught up' }}</dd>
              </template>
              <template v-else><dt>Behind</dt><dd class="qn-t2">Its group has not read yet.</dd></template>
              <template v-if="r.state">
                <dt>State</dt><dd>{{ fmt(r.state.rows) }} row{{ r.state.rows === 1 ? '' : 's' }}<template v-if="!r.exact"> or more (counted up to the read's limit)</template>, the last written {{ ago(r.lastWrite) }}</dd>
              </template>
              <template v-if="r.hash"><dt>Config</dt><dd><span class="qn-mono">{{ r.hash }}</span><span class="qn-t3">, registered {{ when(r.created) }}<template v-if="r.updated && r.updated !== r.created">, changed {{ when(r.updated) }}</template></span></dd></template>
              <template v-if="r.queryId"><dt>Query id</dt><dd class="qn-mono">{{ r.queryId }}</dd></template>
            </dl>
            <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px">
              <RouterLink :to="`/queues/${enc(r.source)}`" class="qn-btn2">Open {{ r.source }}</RouterLink>
              <RouterLink v-if="r.sink" :to="`/queues/${enc(r.sink)}`" class="qn-btn2">Open {{ r.sink }}</RouterLink>
            </div>
          </section>
        </template>
      </div>
      <p class="qn-read" style="margin-top: 12px">
        Read from <span class="qn-mono">GET /streams/v1/queries</span> and the consumer groups. In and out are the source's arrivals and the sink's, the last 5 minutes. A window is past its end once it ended {{ overdueMin }} min ago.
      </p>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.st { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.st-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.st-title { font-size: 22px; font-weight: 500; margin-right: 8px; }
.st-verdict { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 6px; font-size: 15px; margin-top: 14px; }
.st-verdict .qn-mark { margin-right: 4px; }
.st-note { margin-top: 18px; color: var(--text2); max-width: 900px; }
.st-empty { margin-top: 18px; color: var(--text2); max-width: 680px; }
.st-table { margin-top: 22px; overflow-x: auto; }
.st-table [role='row'] { min-width: 920px; }
.st-open { padding: 14px 4px 18px; border-bottom: 1px solid var(--line2); min-width: 920px; }
.st-shape { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 8px; font-size: 14px; }
.st-why { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 8px; margin-top: 12px; max-width: 900px; }
.st-why .qn-mark { align-self: center; }
.st-dl { margin-top: 12px; max-width: 760px; }
@media (max-width: 640px) { .st { padding: 18px 16px 40px; } }
</style>
