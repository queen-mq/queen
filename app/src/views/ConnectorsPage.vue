<script setup>
// Connectors, in the new console (queen-mq/queen#155): every PostgreSQL
// source (a table's changes into a queue) and sink (a queue into a table),
// whether it runs, what it moves where and how much, read-only. A row opens
// the connector on the right (?name=): its state, its error and what fixes
// it, its work, and its document as stored (never the password).
import { computed, onBeforeUnmount, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ConsoleShell from '@/console/ConsoleShell.vue'
import QcJson from '@/console/QcJson.vue'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { useConnectorsPage } from '@/console/useConnectorsPage'
import { clock } from '@/console/useWindow'
import { useIdentity } from '@/stores/identity'

const route = useRoute()
const router = useRouter()
const frame = useConsoleFrame()
const { can } = useIdentity()
const c = useConnectorsPage()
const where = computed(() => frame.cluster.value.name)
const admin = computed(() => can('queueAdmin'))

const picked = computed(() => (typeof route.query.name === 'string' ? route.query.name : ''))
function pick(name) { router.replace({ query: { ...route.query, name: name && name !== picked.value ? name : undefined } }) }
const detail = computed(() => (picked.value ? c.detail(picked.value) : null))
const doc = computed(() => {
  const raw = picked.value ? c.raw(picked.value) : null
  if (!raw) return null
  const { status, state, ...stored } = raw
  return stored
})
const path = computed(() => (picked.value ? [{ label: 'Connectors', to: '/connectors' }, { label: picked.value, mono: true }] : [{ label: 'Connectors' }]))
function onKey(e) {
  if (e.key !== 'Escape' || !picked.value || e.defaultPrevented) return
  const t = e.target
  if (t && /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName)) return
  pick('')
}
onMounted(() => document.addEventListener('keydown', onKey))
onBeforeUnmount(() => document.removeEventListener('keydown', onKey))

const sources = computed(() => c.rows.value.filter((r) => r.kind === 'Source').length)
const sinks = computed(() => c.rows.value.length - sources.value)
const retryText = (ms) => (ms == null ? '' : ms < 1000 ? 'It tries again in a moment.' : `It tries again in ${Math.round(ms / 1000)} s.`)
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="cx">
      <header class="cx-head">
        <h1 class="cx-title">Connectors</h1>
        <span v-if="c.rows.value.length" class="qn-tag qn-num">{{ sources }} source{{ sources === 1 ? '' : 's' }}</span>
        <span v-if="c.rows.value.length" class="qn-tag qn-num">{{ sinks }} sink{{ sinks === 1 ? '' : 's' }}</span>
        <span class="qn-tag">PostgreSQL</span>
      </header>

      <template v-if="!admin">
        <p class="cx-verdict qn-t2">Only an admin of <span class="qn-mono">{{ where }}</span> can read its connectors: each one names a database and its login.</p>
      </template>
      <template v-else>
        <p v-if="c.verdict.value" class="cx-verdict" aria-live="polite">
          <span v-if="c.verdict.value.tone" class="qn-mark" :class="c.verdict.value.tone" aria-hidden="true" />
          <span>
            <template v-for="(n, i) in c.verdict.value.names" :key="n"><span v-if="i" class="qn-t2">{{ c.verdict.value.more ? ', ' : ' and ' }}</span><button type="button" class="qn-mono cx-vname" @click="pick(n)">{{ n }}</button></template>
            <template v-if="c.verdict.value.more"> and {{ c.verdict.value.more }} more</template>
            {{ (c.verdict.value.names.length ? ' ' : '') + c.verdict.value.text }}
          </span>
        </p>

        <p v-if="c.absent.value" class="cx-note qn-panel">
          This broker has no connectors: <span class="qn-mono">GET /api/v1/connectors</span> answers 404. A broker built without the
          <span class="qn-mono">pg</span> feature has none; the images on ghcr.io have them.
        </p>
        <p v-else-if="c.refused.value" class="cx-note qn-panel">The proxy refused the read: connectors need the admin role on this cluster.</p>
        <p v-else-if="c.error.value" class="qn-t2" style="margin-top: 14px"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ c.error.value }}</p>
        <p v-else-if="!c.loading.value && !c.rows.value.length" class="cx-empty">
          No connectors yet. A source streams a table’s committed changes into a queue, a sink writes a queue into a table:
          <span class="qn-mono">PUT /api/v1/connectors/&lt;name&gt;</span> creates one
          (<a href="https://queenmq.com/integrations/postgres/" target="_blank" rel="noopener" class="qn-link">how</a>).
        </p>

        <div v-if="c.rows.value.length" class="cx-grid" :class="{ open: !!picked }">
          <section class="cx-list" aria-label="The connectors">
            <div class="cx-scroll">
              <div class="cx-table" role="table" aria-label="Connectors">
                <div class="qn-tr head" role="row">
                  <span role="columnheader">Connector</span>
                  <span role="columnheader">State</span>
                  <span role="columnheader">Moves</span>
                  <span role="columnheader" class="r" title="Messages a source pushed, or a sink applied, since it started on the node that runs it">Moved</span>
                  <span role="columnheader" class="r" title="A source’s WAL the database keeps for its slot: what it has still to read">Slot lag</span>
                  <span role="columnheader" class="r">Last</span>
                </div>
                <div
                  v-for="r in c.rows.value" :key="r.key" class="qn-tr click" :class="{ on: picked === r.name }" role="row"
                  tabindex="0" :aria-selected="picked === r.name" @click="pick(r.name)" @keydown.enter.prevent="pick(r.name)"
                >
                  <span role="cell" class="cx-name">
                    <span class="cx-markslot" aria-hidden="true"><span v-if="r.tone" class="qn-mark" :class="r.tone" /></span>
                    <span style="min-width: 0">
                      <span class="qn-mono cx-n" :title="r.name">{{ r.name }}</span>
                      <span class="qn-t3 cx-kind">{{ r.kind }}</span>
                    </span>
                  </span>
                  <span role="cell" :class="r.tone || (r.phase === 'disabled' || r.phase === 'stopped' ? 'qn-t3' : '')" :title="r.title || undefined">
                    {{ r.word }}<span v-if="r.elsewhere && r.owner" class="qn-t3"> on <span class="qn-mono">{{ r.owner }}</span></span>
                  </span>
                  <span role="cell" class="cx-flows">
                    <span v-for="(f, i) in r.flows.slice(0, 2)" :key="i" class="qn-mono qn-t2 cx-flow">{{ f.from }} <span class="qn-t3">→</span> {{ f.to }}</span>
                    <span v-if="r.flows.length > 2" class="qn-t3 cx-flow">and {{ r.flows.length - 2 }} more tables</span>
                  </span>
                  <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.moved === '–' }" :title="r.movedTitle">{{ r.moved }}</span>
                  <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.behind === '–' }">{{ r.behind }}</span>
                  <span role="cell" class="r qn-num qn-t2">{{ r.last || '–' }}</span>
                </div>
              </div>
            </div>
            <p class="qn-read" style="margin-top: 12px">
              Read from <span class="qn-mono">GET /api/v1/connectors</span>, every 30 s<template v-if="c.readAt.value">, last at {{ clock(c.readAt.value, true) }}</template>.
              Each node runs every connector; a source streams on the node holding its lease, and the others say Standby.
              The counters are the running node’s since it started.
            </p>
          </section>

          <aside v-if="picked" class="cx-drawer qn-panel" :aria-label="`Connector ${picked}`">
            <div class="cx-dhead">
              <div style="flex: 1 1 0; min-width: 0">
                <p class="qn-t3" style="font-size: 12px">{{ detail ? detail.row.kind : 'Connector' }}</p>
                <h3 class="qn-mono cx-dname">{{ picked }}</h3>
              </div>
              <button type="button" class="qn-btn3" aria-label="Close the details" style="height: 28px" @click="pick('')">✕</button>
            </div>
            <p v-if="!detail" class="qn-t2" style="margin-top: 12px">There is no connector named <span class="qn-mono">{{ picked }}</span> in the last read.</p>
            <template v-else>
              <div v-if="detail.error" class="cx-err">
                <p class="red" style="font-weight: 500"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />{{ detail.error.code || 'error' }}</p>
                <p class="cx-errmsg">{{ detail.error.message }}</p>
                <p v-if="detail.retryIn != null" class="qn-t2" style="margin-top: 6px">{{ retryText(detail.retryIn) }}</p>
              </div>
              <dl class="qn-dl cx-dl">
                <template v-for="f in detail.facts" :key="f.k">
                  <dt>{{ f.k }}</dt><dd :class="[f.mono ? 'qn-mono' : '', f.tone || '']">{{ f.v }}</dd>
                </template>
              </dl>
              <h4 class="cx-h">{{ detail.row.kind === 'Source' ? 'Tables into queues' : 'Queue into table' }}</h4>
              <ul class="cx-flowlist">
                <li v-for="(f, i) in detail.flows" :key="i" class="qn-mono">{{ f.from }} <span class="qn-t3">→</span> {{ f.to }}</li>
              </ul>
              <template v-if="detail.work.length">
                <h4 class="cx-h">Its work</h4>
                <dl class="qn-dl cx-dl">
                  <template v-for="f in detail.work" :key="f.k">
                    <dt>{{ f.k }}</dt>
                    <dd><span :class="f.mono ? 'qn-mono' : 'qn-num'">{{ f.v }}</span><span v-if="f.sub" class="qn-t3 cx-sub">{{ f.sub }}</span></dd>
                  </template>
                </dl>
              </template>
              <h4 class="cx-h">Its document</h4>
              <p class="qn-t3" style="font-size: 12px; margin-top: 6px">As stored, without the password. <span class="qn-mono">PUT /api/v1/connectors/{{ picked }}</span> replaces it.</p>
              <QcJson :value="doc" class="cx-doc" />
            </template>
          </aside>
        </div>
      </template>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.cx { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.cx-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.cx-title { font-size: 22px; font-weight: 500; margin-right: 8px; }
.cx-verdict { display: flex; align-items: flex-start; gap: 10px; font-size: 15px; margin-top: 14px; }
.cx-verdict > .qn-mark { margin-top: 7px; }
.cx-vname { font-weight: 500; text-decoration: underline; text-decoration-color: var(--line); text-underline-offset: 3px; }
.cx-note { margin-top: 18px; padding: 14px 18px; }
.cx-empty { margin-top: 18px; color: var(--text2); max-width: 760px; }

.cx-grid { display: grid; grid-template-columns: minmax(0, 1fr); column-gap: 28px; margin-top: 22px; align-items: start; }
.cx-grid.open { grid-template-columns: minmax(0, 1fr) 360px; }
.cx-list { min-width: 0; }
.cx-scroll { overflow-x: auto; }
.cx-table { --cols: minmax(150px, 1.3fr) minmax(110px, .9fr) minmax(180px, 1.8fr) 84px 84px 90px; min-width: 760px; }
.cx-table .qn-tr { gap: 0 14px; min-height: 52px; }
.cx-table .qn-tr.head { min-height: 32px; }
.cx-name { display: flex; align-items: center; gap: 10px; }
.cx-markslot { flex: 0 0 8px; display: flex; align-items: center; justify-content: center; }
.cx-n { display: block; font-weight: 500; overflow: hidden; text-overflow: ellipsis; }
.cx-kind { display: block; font-size: 12px; }
.cx-flows { display: flex; flex-direction: column; gap: 2px; }
.cx-flow { font-size: 12px; overflow: hidden; text-overflow: ellipsis; }

.cx-drawer { position: sticky; top: 68px; max-height: calc(100vh - 84px); overflow-y: auto; padding: 16px 18px 18px; }
.cx-dhead { display: flex; align-items: flex-start; gap: 8px; }
.cx-dname { font-size: 14px; font-weight: 500; margin-top: 2px; overflow-wrap: anywhere; }
.cx-err { margin-top: 14px; padding: 12px 14px; border-radius: var(--r-ctl); background: var(--redbg); }
.cx-errmsg { margin-top: 6px; font-size: 12.5px; overflow-wrap: anywhere; }
.cx-dl { grid-template-columns: 96px minmax(0, 1fr); margin-top: 12px; }
.cx-sub { display: block; font-size: 11.5px; margin-top: 1px; }
.cx-h { font-size: 12px; font-weight: 500; color: var(--text3); margin: 18px 0 0; padding-top: 14px; border-top: 1px solid var(--line2); }
.cx-h + .cx-dl { margin-top: 8px; }
.cx-flowlist { margin-top: 8px; display: grid; gap: 4px; font-size: 12.5px; overflow-wrap: anywhere; }
.cx-doc { margin-top: 8px; max-height: 420px; }
:global(.qn.tone .cx-h) { border-top-color: var(--bg3); }

@media (max-width: 1100px) {
  .cx-grid.open { grid-template-columns: minmax(0, 1fr); }
  .cx-drawer { position: static; max-height: none; overflow: visible; margin-top: 18px; }
}
/* a phone: the connector, its state and what it moved; a tap opens the rest below */
@media (max-width: 640px) {
  .cx { padding: 18px 16px 40px; }
  .cx-title { font-size: 19px; }
  .cx-table { --cols: minmax(0, 1.3fr) minmax(0, 1fr) 64px; min-width: 0; }
  .cx-table .qn-tr > :nth-child(3), .cx-table .qn-tr > :nth-child(n+5) { display: none; }
}
</style>
