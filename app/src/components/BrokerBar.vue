<template>
  <!--
    The broker, in the top bar of every page: the machine under the cell's
    nodes (CPU, memory, disk) and the replicated log across them, from one
    read, GET /api/v1/raft/members (composables/useBrokerStatus.js). A figure
    is its fullest node's, the one that runs out first; the hairline under it
    is how full. Click for every node.

    Cell-level: every tenant on the cell shares these numbers, so they are an
    operator's only, and the panel names the cell. Colour only when a line is
    crossed (composables/useSeverity.js); the disk's line is the node's own
    write gate.
  -->
  <div v-if="can('operator') && !absent" ref="root" class="bbar">
    <button
      class="bbar-btn"
      :class="{ open }"
      :aria-expanded="open"
      aria-haspopup="dialog"
      :title="open ? '' : buttonTitle"
      @click="open = !open"
    >
      <template v-if="status">
        <span class="bbar-item bbar-full">
          <span class="k">CPU</span><b :class="status.cpu.sev">{{ pct(status.cpu.top?.share ?? null) }}</b>
          <i class="bbar-meter" aria-hidden="true"><i :class="status.cpu.sev" :style="bar(status.cpu.top?.share ?? null)" /></i>
        </span>
        <span class="bbar-item bbar-full">
          <span class="k">Memory</span><b :class="status.mem.sev">{{ size(status.mem.top?.rss ?? null) }}</b>
          <i class="bbar-meter" aria-hidden="true"><i :class="status.mem.sev" :style="bar(status.mem.top?.share ?? null)" /></i>
        </span>
        <span class="bbar-item bbar-full">
          <span class="k">Disk</span><b :class="status.disk.sev">{{ diskPct(status.disk.top) }}</b>
          <i class="bbar-meter" aria-hidden="true">
            <i :class="status.disk.sev" :style="bar(status.disk.top?.share ?? null)" />
            <span v-if="status.disk.top?.gate && status.disk.top.highPct !== null" class="bbar-line" :style="{ left: `${Math.min(100, status.disk.top.highPct)}%` }" />
          </i>
        </span>
        <span class="bbar-item bbar-full">
          <span class="k">Raft</span><b :class="status.raft.sev">{{ raftShort }}</b>
        </span>
        <!-- Narrow screens: one word and the worst mark. -->
        <span class="bbar-item bbar-sum">
          <span class="g" :class="status.sev || 'ok'" aria-hidden="true" /><span class="k">Broker</span>
        </span>
      </template>
      <span v-else class="bbar-item">
        <span class="k">Broker</span><b>{{ loading ? '…' : '—' }}</b>
      </span>
    </button>

    <div v-if="open" class="bbar-panel" role="dialog" aria-label="Broker">
      <div class="bbar-head">
        <span><b>Cell {{ cellLabel }}</b> · shared by every tenant, not just {{ tenantLabel }}</span>
        <router-link to="/system" class="bbar-link" @click="open = false">System ›</router-link>
      </div>
      <div v-if="!status" class="bbar-msg">
        {{ loading ? 'Reading the broker…' : `Broker status unavailable · ${errorText}` }}
      </div>
      <template v-else>
        <div class="bbar-scroll">
          <table class="t bbar-table">
            <thead>
              <tr><th>Node</th><template v-if="measured"><th>CPU</th><th>Memory</th><th>Disk</th></template><th>Raft</th></tr>
            </thead>
            <tbody>
              <tr v-for="n in status.nodes" :key="n.name">
                <td class="bbar-node"><span class="g" :class="nodeGlyph(n)" aria-hidden="true" />{{ n.name }}</td>
                <template v-if="!measured" />
                <template v-else-if="n.unreachable">
                  <td colspan="3" class="bbar-none">unreachable — no reading</td>
                </template>
                <template v-else>
                  <td>
                    <span class="bbar-cell"><b :class="n.cpu.sev">{{ pct(n.cpu.share) }}</b><span>of {{ n.cpu.cpus ?? '?' }} {{ n.cpu.cpus === 1 ? 'core' : 'cores' }}</span></span>
                    <i class="bbar-meter" aria-hidden="true"><i :class="n.cpu.sev" :style="bar(n.cpu.share)" /></i>
                  </td>
                  <td>
                    <span class="bbar-cell"><b :class="n.mem.sev">{{ size(n.mem.rss) }}</b><span>of {{ size(n.mem.limit) }}</span></span>
                    <i class="bbar-meter" aria-hidden="true"><i :class="n.mem.sev" :style="bar(n.mem.share)" /></i>
                  </td>
                  <td>
                    <span class="bbar-cell"><b :class="n.disk.sev">{{ diskPct(n.disk) }}</b><span>{{ diskNote(n.disk) }}</span></span>
                    <i class="bbar-meter" aria-hidden="true">
                      <i :class="n.disk.sev" :style="bar(n.disk.share)" />
                      <span v-if="n.disk.gate && n.disk.highPct !== null" class="bbar-line" :style="{ left: `${Math.min(100, n.disk.highPct)}%` }" />
                    </i>
                  </td>
                </template>
                <td class="bbar-raft">{{ raftOf(n) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p v-if="measured" class="bbar-foot">
          {{ raftLine }}. CPU is the average of the last {{ status.cpu.top?.windowSeconds || 60 }} s, of the cores the node may use;
          memory is against the limit it runs under; the mark on a disk bar is where that node stops taking writes.
        </p>
        <p v-else class="bbar-foot">
          {{ raftLine }}. This broker does not report its CPU, memory or disk; a newer build does.
        </p>
      </template>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import { operator } from '@/api'
import { describeApiError } from '@/api/errors'
import { formatNumber, useApi } from '@/composables/useApi'
import { brokerStatus } from '@/composables/useBrokerStatus'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useIdentity } from '@/stores/identity'
import { isMissingRoute, routeSupport } from '@/stores/routeSupport'

const { can, actingCellSlug, actingTenantSlug } = useIdentity()
const route = useRoute()

// A cluster route, new in 2.0: System guards it under the same name, so an
// older broker's 404 is asked once per cluster and never becomes a toast.
const members = useApi(
  routeSupport.guard('raft-members', (config) => operator.getRaftMembers({ ...config, probe: true })),
  { immediate: false },
)
const refresh = () => (can('operator') ? members.refresh() : undefined)
refresh()
useAutoRefresh(refresh)

const status = computed(() => brokerStatus(members.data.value))
// A broker older than the `host` block reports Raft only: its CPU, memory
// and disk read "—", and the tooltip and the panel say why.
const measured = computed(() => (members.data.value?.members || []).some((m) => m && m.host))
const buttonTitle = computed(() => (measured.value
  ? `Cell ${cellLabel.value}: the broker, shared by every tenant. Click for every node.`
  : `Cell ${cellLabel.value}: this broker reports Raft only, not its CPU, memory or disk; a newer build does. Click for every node.`))
const loading = computed(() => members.loading.value && !members.data.value)
const absent = computed(() => isMissingRoute(members.error.value))
const errorText = computed(() => describeApiError(members.error.value))
const cellLabel = computed(() => actingCellSlug.value || 'unknown')
const tenantLabel = computed(() => actingTenantSlug.value || 'this tenant')

// --- The panel ----------------------------------------------------------------
const open = ref(false)
const root = ref(null)
const onDocClick = (e) => { if (open.value && root.value && !root.value.contains(e.target)) open.value = false }
const onKey = (e) => { if (e.key === 'Escape') open.value = false }
onMounted(() => { document.addEventListener('mousedown', onDocClick); document.addEventListener('keydown', onKey) })
onBeforeUnmount(() => { document.removeEventListener('mousedown', onDocClick); document.removeEventListener('keydown', onKey) })
watch(() => route.path, () => { open.value = false })

// --- Words and numbers --------------------------------------------------------
const pct = (share) => {
  if (share === null) return '—'
  return share > 0 && share < 0.01 ? '<1%' : `${Math.round(share * 100)}%`
}
const bar = (share) => ({ width: share === null ? '0%' : `${Math.min(100, Math.max(0, share * 100))}%` })
// A gate line as configured: 85 stays 85, 99.5 must not read as 100.
const line = (x) => `${Number.isInteger(x) ? x : x.toFixed(1)}%`
// Sizes at a glance: 48 MB, 1.2 GB, 24 GB.
const size = (b) => {
  if (b === null || b === undefined) return '—'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let v = b
  let i = 0
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++ }
  return `${v < 10 && i > 0 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${units[i]}`
}
const diskPct = (d) => {
  if (!d || d.usedPct === null) return '—'
  return d.usedPct < 1 ? '<1%' : `${Math.round(d.usedPct)}%`
}
const diskNote = (d) => {
  if (d.usedPct === null) return 'not measured'
  if (d.refused) return 'writes refused'
  if (d.gate && d.highPct !== null) return `stops at ${line(d.highPct)}`
  return `of ${size(d.totalBytes)}`
}

const raftShort = computed(() => {
  const r = status.value.raft
  if (r.single) return status.value.nodes[0]?.state || '—'
  return `${r.up}/${r.voters}`
})
const raftLine = computed(() => {
  const r = status.value.raft
  const term = r.term === null ? '' : `, term ${formatNumber(r.term)}`
  if (r.single) return `Single node${term}`
  if (!r.hasQuorum) return `${r.up} of ${r.voters} voters answer: no quorum, writes stop`
  if (!r.leader) return `${r.up} of ${r.voters} voters, no leader${term}`
  return `${r.up} of ${r.voters} voters, leader ${r.leader}${term}`
})
const raftOf = (n) => {
  if (n.unreachable) return 'unreachable'
  if (n.lag) return `${n.state} · ${formatNumber(n.lag)} behind`
  return n.state || '—'
}
const nodeGlyph = (n) => {
  if (n.unreachable || n.state === 'shutdown') return 'bad'
  if (n.state === 'candidate') return 'warn'
  if (n.role === 'learner') return 'idle'
  return n.isLeader ? 'leader' : 'ok'
}
</script>
