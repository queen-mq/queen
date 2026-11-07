<script setup>
// One feed stream, opened under its row on the Streams page: what it reads and
// writes, why it needs you (when it does), its shards, its steps with what
// each holds, and the facts of its records. Everything here comes from KV
// (feedStreams.js); nothing asks the running process.
import { computed } from 'vue'
import { fmt } from '@/console/useQueuePage'
import { span } from '@/console/words'
import { behindCell, committedText, feedWhy, runsOnText, stateSavedText, stepWords } from './feedStreams'

const props = defineProps({ row: { type: Object, required: true } })
const r = computed(() => props.row)
const enc = encodeURIComponent
const exact = (list) => (Array.isArray(list) ? list.filter((q) => !/[*?]/.test(q)) : [])

const why = computed(() => feedWhy(r.value))
const steps = computed(() => (r.value.about?.steps || [])
  .map((s) => stepWords(s, r.value.openBy, r.value.heldBy))
  .filter(Boolean))
const shardCells = computed(() => (r.value.shardCount > 1
  ? r.value.shards.map((s) => ({ ...s, cell: behindCell({ status: s.state, behindMs: s.behindMs }) }))
  : []))
const behind = computed(() => behindCell(r.value))
const committed = computed(() => committedText(r.value))
const sources = computed(() => exact(r.value.reads?.queues))
const sinks = computed(() => (r.value.sink?.kind === 'queues' ? r.value.sink.queues : []))
const runsOn = computed(() => runsOnText(r.value))
const saved = computed(() => stateSavedText(r.value))
const shardWord = (s) => (s.state === 'starting' ? 'starting' : s.state === 'quiet' ? `no writes for ${span(s.quietMs / 1000)}` : null)
</script>

<template>
  <section class="fs" :aria-label="`Stream ${r.name}`">
    <p v-if="r.reads" class="fs-shape">
      <span class="qn-t3">reads the feed:</span>
      <template v-if="r.reads.queues && r.reads.queues.length">
        <template v-for="(q, i) in r.reads.queues" :key="q"><span v-if="i" class="qn-t3">,</span>
          <RouterLink v-if="!/[*?]/.test(q)" :to="`/queues/${enc(q)}`" class="qn-mono">{{ q }}</RouterLink>
          <span v-else class="qn-mono">{{ q }}</span>
        </template>
      </template>
      <span v-else>every queue</span>
      <template v-if="r.reads.where.length"><span class="qn-t3">where</span> <span class="qn-mono">{{ r.reads.where.join(' and ') }}</span></template>
      <template v-if="r.reads.tables.length"><span class="qn-t3">· tables</span> <span class="qn-mono">{{ r.reads.tables.join(', ') }}</span></template>
      <template v-if="r.shardCount > 1"><span class="qn-t3">· in</span> {{ r.shardCount }} shards <span class="qn-t3">by {{ r.about?.shards?.by === 'function' ? 'a key function' : 'partition' }}</span></template>
      <template v-if="r.sink"><span class="qn-t3">· writes</span>
        <template v-if="sinks.length"><template v-for="(q, i) in sinks" :key="q"><span v-if="i" class="qn-t3">,</span> <RouterLink :to="`/queues/${enc(q)}`" class="qn-mono">{{ q }}</RouterLink></template></template>
        <span v-else>{{ r.sink.text }}</span>
      </template>
      <template v-if="r.deadLetters"><span class="qn-t3">· dead letters to</span> <RouterLink :to="`/queues/${enc(r.deadLetters)}`" class="qn-mono">{{ r.deadLetters }}</RouterLink></template>
    </p>
    <p v-else-if="r.lastMs == null" class="fs-shape qn-t2">What it reads and its steps come with its first commit, and it has made none yet.</p>
    <p v-else class="fs-shape qn-t2">
      Its runner does not describe itself (a client older than this console, or another SDK): what it reads and its steps are not known here, its position and its lock are.
    </p>

    <p v-if="why" class="fs-why" :class="why.tone">
      <span v-if="why.tone" class="qn-mark" :class="why.tone" aria-hidden="true" />
      <span>{{ why.text }} <span v-if="why.hint" class="qn-t2">{{ why.hint }}</span></span>
    </p>

    <div v-if="shardCells.length" class="fs-shards" role="list" aria-label="Shards">
      <div v-for="s in shardCells" :key="s.index" class="fs-cell" role="listitem">
        <div class="fs-cell-top"><span class="qn-mono">shard {{ s.index }}</span><span class="qn-num" :class="s.cell.tone">{{ s.cell.text }}</span></div>
        <div class="qn-t3 fs-small">{{ s.holder ? `${s.holder.host ?? s.holder.owner}${s.holder.pid != null ? ` · pid ${s.holder.pid}` : ''}` : 'no runner' }}</div>
        <div class="fs-small">
          <template v-if="s.open != null">{{ fmt(s.open) }} open</template>
          <template v-else-if="shardWord(s)">{{ shardWord(s) }}</template>
          <template v-else-if="s.position != null"><span class="qn-t3">at</span> {{ fmt(s.position) }}</template>
        </div>
      </div>
    </div>

    <ol v-if="steps.length" class="fs-steps" aria-label="Steps">
      <template v-for="(s, i) in steps" :key="i">
        <li v-if="i" class="fs-arrow qn-t3" aria-hidden="true">→</li>
        <li class="fs-step">
          <div><span v-if="s.name" class="qn-mono">{{ s.name }}</span> <span :class="s.name ? 'qn-t3' : 'qn-mono'">{{ s.label }}</span></div>
          <div v-if="s.detail" class="qn-t2 fs-small">{{ s.detail }}</div>
          <div v-if="s.open != null || s.clock" class="fs-small">
            <template v-if="s.open != null">{{ fmt(s.open) }} {{ s.kind === 'table' ? 'rows' : 'open' }}</template><template v-if="s.held"> · {{ fmt(s.held) }} held</template><template v-if="s.clock"><span class="qn-t3"> · {{ s.clock }}</span></template>
          </div>
        </li>
      </template>
      <li v-if="r.sink" class="fs-arrow qn-t3" aria-hidden="true">→</li>
      <li v-if="r.sink" class="fs-step"><span class="qn-t3">to</span> <span class="qn-mono">{{ r.sink.text }}</span></li>
    </ol>

    <dl class="qn-dl fs-dl">
      <dt>Runs on</dt><dd>{{ runsOn ?? 'nothing' }}</dd>
      <dt>Behind</dt><dd :class="behind.tone">{{ behind.text }}<span v-if="r.status === 'running' && r.behindMs" class="qn-t3">, at its last commit</span></dd>
      <dt>Committed</dt><dd>{{ committed }}</dd>
      <template v-if="saved"><dt>State saved</dt><dd>{{ saved }}</dd></template>
      <template v-if="r.skipped"><dt>Left out</dt><dd>{{ fmt(r.skipped) }} message{{ r.skipped === 1 ? '' : 's' }} since its last snapshot <span class="qn-t3">(onPoison 'skip')</span></dd></template>
      <template v-if="r.epoch"><dt>Started over</dt><dd>{{ fmt(r.epoch) }} time{{ r.epoch === 1 ? '' : 's' }} <span class="qn-t3">(a reset)</span></dd></template>
      <template v-if="r.hash"><dt>Config</dt><dd class="qn-mono">{{ r.hash }}</dd></template>
      <template v-if="r.client"><dt>Client</dt><dd>{{ r.client }}</dd></template>
      <dt>Query id</dt><dd class="qn-mono">{{ r.queryId }}</dd>
    </dl>

    <div class="fs-actions">
      <RouterLink v-for="q in sources.slice(0, 3)" :key="`s-${q}`" :to="`/queues/${enc(q)}`" class="qn-btn2">Open {{ q }}</RouterLink>
      <RouterLink v-for="q in sinks.slice(0, 3)" :key="`o-${q}`" :to="`/queues/${enc(q)}`" class="qn-btn2">Open {{ q }}</RouterLink>
      <RouterLink :to="{ path: '/state', query: { ns: 'queen-feed' } }" class="qn-btn2">Its keys</RouterLink>
      <RouterLink :to="{ path: '/state', query: { view: 'locks' } }" class="qn-btn2">Locks</RouterLink>
    </div>
  </section>
</template>

<style scoped>
.fs { padding: 14px 12px 18px; border-bottom: 1px solid var(--line2); min-width: 920px; }
.fs-shape { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 6px; font-size: 14px; max-width: 1100px; }
.fs-why { display: flex; align-items: baseline; gap: 8px; margin-top: 12px; max-width: 900px; }
.fs-why .qn-mark { flex: none; align-self: center; }
.fs-shards { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 8px; margin-top: 14px; max-width: 1100px; }
.fs-cell { background: var(--bg2); border: 1px solid var(--line2); border-radius: var(--r-ctl); padding: 8px 10px; }
.fs-cell-top { display: flex; justify-content: space-between; gap: 8px; }
.fs-small { font-size: 12px; margin-top: 2px; }
.fs-steps { display: flex; flex-wrap: wrap; align-items: stretch; gap: 8px; margin-top: 14px; }
.fs-step { background: var(--bg2); border: 1px solid var(--line2); border-radius: var(--r-ctl); padding: 7px 10px; }
.fs-arrow { align-self: center; }
.fs-dl { margin-top: 14px; max-width: 760px; }
.fs-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
/* tone: filled areas, no hairlines (in scoped CSS the whole selector goes in :global) */
:global(.qn.tone .fs-cell), :global(.qn.tone .fs-step) { background: var(--ctl); border-color: transparent; }
@media (max-width: 640px) {
  .fs { min-width: 0; }
}
</style>
