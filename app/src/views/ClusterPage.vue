<script setup>
// The cell under every tenant, in the new console: one line saying whether it
// is all right, each node now (a click opens it on the right), each node over
// the window, and what an operator can do to it today.
import { computed, onBeforeUnmount, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ConsoleShell from '@/console/ConsoleShell.vue'
import { useWindow } from '@/console/useWindow'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { useClusterPage } from '@/console/useClusterPage'
import ClusterNodes from '@/console/cluster/ClusterNodes.vue'
import ClusterNode from '@/console/cluster/ClusterNode.vue'
import ClusterCharts from '@/console/cluster/ClusterCharts.vue'
import ClusterOps from '@/console/cluster/ClusterOps.vue'
import { segments } from '@/console/cluster/clusterViews'

const route = useRoute()
const router = useRouter()
const { win, range } = useWindow()
const frame = useConsoleFrame()
const c = useClusterPage(frame, win)
const where = computed(() => frame.cluster.value.name)

// ---- the node on the right, kept in the address (?node=) ----
const picked = computed(() => (typeof route.query.node === 'string' ? route.query.node : ''))
function pick(name) { router.replace({ query: { ...route.query, node: name && name !== picked.value ? name : undefined } }) }
const detail = computed(() => (picked.value ? c.detail(picked.value) : null))
function onKey(e) {
  if (e.key !== 'Escape' || !picked.value || e.defaultPrevented) return
  const t = e.target
  if (t && /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName)) return
  pick('')
}
onMounted(() => document.addEventListener('keydown', onKey))
onBeforeUnmount(() => document.removeEventListener('keydown', onKey))

const gateLine = computed(() => (c.gate.value != null ? ` A node stops taking writes when its disk is ${c.gate.value}% full, the tick on its bar.` : ''))
</script>

<template>
  <ConsoleShell :path="[{ label: 'Cluster' }]" :frame="frame">
    <div class="cp">
      <header class="cp-head">
        <h1 class="cp-title">Cluster</h1>
        <span class="qn-tag">cell-level: every tenant on <span class="qn-mono">{{ where }}</span></span>
        <span v-if="c.versions.value.list.length" class="qn-tag qn-mono" :title="c.versions.value.mixed ? 'The nodes run different versions' : 'Every node runs this version'">{{ c.versions.value.text }}</span>
      </header>

      <template v-if="!c.canOperate.value">
        <p class="cp-verdict qn-t2">Only a live operator can open this page: it covers every tenant on {{ where }}.</p>
      </template>
      <template v-else>
        <!-- is the cell all right -->
        <p class="cp-verdict" aria-live="polite">
          <span v-if="c.verdict.value.badge" class="cp-badge" :class="c.verdict.value.tone || 'qn-t2'">
            <span v-if="c.verdict.value.tone" class="qn-mark" :class="c.verdict.value.tone" aria-hidden="true" />{{ c.verdict.value.badge }}
          </span>
          <span :class="c.verdict.value.loading ? 'qn-t2' : ''"><span v-for="(x, i) in segments(c.verdict.value.text)" :key="i" :class="x.mono ? 'qn-mono cp-id' : ''">{{ x.t }}</span></span>
          <span v-if="c.verdict.value.sub" class="qn-t2">{{ c.verdict.value.sub }}</span>
        </p>
        <section v-if="c.verdict.value.list.length" class="cp-why qn-panel" aria-label="What needs you in the cell">
          <ul class="cp-issues">
            <li v-for="i in c.verdict.value.list" :key="i.key">
              <span class="qn-mark" :class="i.tone" aria-hidden="true" />
              <span style="min-width: 0"><span style="font-weight: 500">{{ i.title }}</span><span v-if="i.why" class="qn-t2"> · {{ i.why }}</span></span>
            </li>
          </ul>
        </section>

        <div class="cp-grid" :class="{ open: !!picked }">
          <!-- each node now -->
          <section class="cp-nodes" aria-label="The nodes">
            <ClusterNodes v-if="c.rows.value.length" :rows="c.rows.value" :columns="c.columns.value" :picked="picked" @pick="pick" />
            <p class="qn-read" style="margin-top: 10px">
              Read from <span class="qn-mono">GET /api/v1/raft/members</span>, the leader's view of every node, every 30 s.{{ gateLine }}
            </p>
          </section>

          <!-- the picked node -->
          <div v-if="picked" class="cp-drawer">
            <ClusterNode :detail="detail" :name="picked" :versions="c.versions.value.mixed" @close="pick('')" />
          </div>

          <!-- each node over the window -->
          <ClusterCharts class="cp-charts" :model="c.charts.value" :win="win" :loaded="!!c.metrics.value" :error="c.metricsError.value" @zoom="range" />

          <!-- what an operator can do -->
          <ClusterOps class="cp-ops" :link="c.linkRow.value" :where="where" @changed="c.reread()" />
        </div>
      </template>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.cp { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.cp-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.cp-title { font-size: 22px; font-weight: 500; margin-right: 8px; }
.cp-verdict { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 10px; font-size: 15px; margin-top: 14px; }
.cp-badge { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 500; }
.cp-badge .qn-mark { align-self: center; }
.cp-id { font-size: 14px; }
.cp-why { margin-top: 18px; padding: 14px 18px; }
.cp-issues { display: grid; gap: 8px; }
.cp-issues li { display: flex; align-items: baseline; gap: 10px; }
.cp-issues .qn-mark { align-self: center; }

/* One column; a picked node opens a second on the right, beside the table,
   the charts and the operations, and stays in view while the page scrolls. */
.cp-grid { display: grid; grid-template-columns: minmax(0, 1fr); column-gap: 28px; margin-top: 22px; }
/* The last row takes what the drawer adds, so nothing above it moves when it opens. */
.cp-grid.open { grid-template-columns: minmax(0, 1fr) 320px; grid-template-rows: auto auto 1fr; }
.cp-nodes { grid-column: 1; grid-row: 1; min-width: 0; }
.cp-drawer { grid-column: 2; grid-row: 1 / span 3; align-self: start; position: sticky; top: 68px; max-height: calc(100vh - 84px); overflow-y: auto; }
.cp-charts { grid-column: 1; grid-row: 2; min-width: 0; margin-top: 36px; }
.cp-ops { grid-column: 1; grid-row: 3; min-width: 0; margin-top: 40px; align-self: start; }

@media (max-width: 1100px) {
  .cp-grid.open { grid-template-columns: minmax(0, 1fr); grid-template-rows: none; }
  .cp-drawer { grid-column: 1; grid-row: 2; position: static; max-height: none; overflow: visible; margin-top: 18px; }
  .cp-charts { grid-row: 3; }
  .cp-ops { grid-row: 4; }
}
@media (max-width: 640px) { .cp { padding: 18px 16px 40px; } .cp-title { font-size: 19px; } }
</style>
