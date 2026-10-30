<template>
  <!--
    The cell at a glance, for System's Overview and the Overview's Cell view:
    the verdict and the five numbers it stands on, what needs you in the same
    card (each row opens the view it is worked on in), then each node now.

      <CellStatus :cell="brokerStatus(members)" :issues="issues" :note="note" full />

    `full` adds who each node is (version, uptime; its addresses on hover, from
    `members`) and its network and Raft-port traffic (System's Overview);
    without it the table keeps who leads and how loaded each node is, and
    `moreTo` names where the rest is.
  -->
  <section class="card cs-card" aria-label="Status of the cell">
    <div class="cs-status">
      <div class="cs-verdict">
        <span class="cs-word" :class="verdict.tone"><span v-if="verdict.tone" class="g" :class="verdict.tone" aria-hidden="true" />{{ verdict.word }}</span>
        <h2 class="cs-title" :title="verdict.headline">{{ verdict.headline }}</h2>
        <p v-if="line" class="cs-line">{{ line }}</p>
      </div>
      <div v-for="c in cells" :key="c.k" class="cs-kpi" :title="c.title || undefined">
        <span class="cs-kpi-k">{{ c.k }}</span>
        <span class="cs-kpi-v" :class="c.tone">{{ c.v }}</span>
      </div>
    </div>
    <div v-if="!cell" class="cs-quiet">{{ note }}</div>
    <ul v-else-if="issues.length" class="cs-issues" aria-label="What needs you in the cell">
      <li v-for="i in issues" :key="i.key">
        <router-link class="cs-issue" :to="i.to">
          <span class="g" :class="i.sev" aria-hidden="true" />
          <span class="cs-issue-text"><span class="cs-issue-title">{{ i.title }}</span><span v-if="i.why" class="cs-issue-why">{{ i.why }}</span></span>
          <Icon name="chevron-right" :size="14" class="cs-go" />
        </router-link>
      </li>
    </ul>
  </section>

  <section v-if="cell && rows.length" class="card" aria-labelledby="cs-nodes-title">
    <div class="card-header">
      <h3 id="cs-nodes-title">Each node now</h3>
      <span v-if="stampText" class="muted">{{ stampText }}</span>
      <router-link v-if="moreTo" class="cs-more" :to="moreTo">System<Icon name="chevron-right" :size="14" /></router-link>
    </div>
    <div class="table-container">
      <table class="t cs-table">
        <thead>
          <tr>
            <th>Node</th>
            <th>Raft</th>
            <th v-if="full">Version</th>
            <th v-if="full" class="num">Up</th>
            <th class="num" title="Of the cores the node may use">CPU</th>
            <th class="num" title="Of the node's memory limit">Memory</th>
            <th class="num" title="Of the data disk">Disk</th>
            <th v-if="full" class="num">Network in / out</th>
            <th v-if="full" class="num" title="Bytes over the Raft port">Raft out / in</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="n in rows" :key="n.name" :title="addressOf(n.name) || undefined">
            <td>
              <router-link v-if="nodeTo" class="cs-node" :to="nodeTo(n.name)">{{ n.name }}</router-link>
              <span v-else>{{ n.name }}</span>
            </td>
            <td :class="n.roleTone">{{ n.role }}</td>
            <td v-if="full">{{ memberOf(n.name)?.version || '—' }}</td>
            <td v-if="full" class="num">{{ memberOf(n.name)?.up || '—' }}</td>
            <td class="num" :class="n.cpu.tone">{{ n.cpu.text }}</td>
            <td class="num" :class="n.mem.tone">{{ n.mem.text }}</td>
            <td class="num" :class="n.disk.tone">{{ n.disk.text }}</td>
            <td v-if="full" class="num">{{ n.net }}</td>
            <td v-if="full" class="num">{{ n.raft }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup>
import { computed } from 'vue'

import Icon from '@/components/Icon.vue'
import { formatNumber, toNum } from '@/composables/useApi'
import { nodeRows } from '@/composables/overviewViews'
import { cellVerdict } from '@/composables/systemViews'
import { formatRate } from '@/composables/useTraffic'

const props = defineProps({
  /** brokerStatus() of the members read; null while it is unread or failed. */
  cell: { type: Object, default: null },
  /** cellIssues(), each with `to`: the view it is worked on in. */
  issues: { type: Array, default: () => [] },
  /** Said in the card while there is no cell to show. */
  note: { type: String, default: 'Reading the members…' },
  /** System's table: each node's network and Raft-port traffic too. */
  full: { type: Boolean, default: false },
  /** A node's name → where it opens; none draws the names as text. */
  nodeTo: { type: Function, default: null },
  /** Where the rest of the cell is ("System ›" on the table). */
  moreTo: { type: [Object, String], default: null },
  /** When the members were read ("as of …"). */
  stampText: { type: String, default: '' },
  /** The broker's version, said in the verdict's line. */
  version: { type: String, default: '' },
  /** Who each node is, by name: `{ name, version, up, http, raftAddr }` (full). */
  members: { type: Array, default: () => [] },
})

const verdict = computed(() => cellVerdict(props.cell, props.issues))
const line = computed(() => {
  const c = props.cell
  if (!c) return ''
  const parts = [`${c.nodes.length} ${c.nodes.length === 1 ? 'node' : 'nodes'}`]
  if (!c.raft.single) parts.push(`leader ${c.raft.leader || 'none'}`)
  if (c.raft.term !== null && c.raft.term !== undefined) parts.push(`term ${formatNumber(c.raft.term)}`)
  if (props.version) parts.push(props.version)
  return parts.join(' · ')
})

const toneOf = (sev) => (sev === 'warn' || sev === 'bad' ? sev : '')
// A share as a person reads it: a sliver that is not zero says so ("<1%").
const shareText = (top) => {
  const v = toNum(top?.share)
  if (v === null) return '—'
  return v > 0 && v < 0.005 ? '<1%' : `${Math.round(v * 100)}%`
}
// The nodes' network now: what each received and sent, summed over the cell.
const networkNow = computed(() => {
  let sum = null
  for (const n of props.cell?.nodes || []) {
    const net = n.traffic?.network
    if (!net) continue
    sum = (sum ?? 0) + (toNum(net.received) || 0) + (toNum(net.sent) || 0)
  }
  return sum
})
const cells = computed(() => {
  const c = props.cell
  return [
    { k: 'CPU', v: shareText(c?.cpu.top), tone: toneOf(c?.cpu.sev), title: c?.cpu.top ? `The busiest node: ${c.cpu.top.name}` : '' },
    { k: 'Memory', v: shareText(c?.mem.top), tone: toneOf(c?.mem.sev), title: c?.mem.top ? `The fullest node: ${c.mem.top.name}` : '' },
    { k: 'Disk', v: shareText(c?.disk.top), tone: toneOf(c?.disk.sev), title: c?.disk.top ? `The fullest disk: ${c.disk.top.name}; from 99.5% writes stop` : '' },
    { k: 'Network', v: networkNow.value === null ? '—' : formatRate(networkNow.value), title: 'In and out of every node, summed' },
    { k: 'Raft port', v: c?.raft.wire === null || c?.raft.wire === undefined ? '—' : formatRate(c.raft.wire), title: 'Bytes per second on the Raft port, every node' },
  ]
})
const rows = computed(() => nodeRows(props.cell))
const memberBy = computed(() => new Map((props.members || []).map((m) => [m.name, m])))
const memberOf = (name) => memberBy.value.get(name) || null
// A node's addresses, on hover: long, and needed only to reach it by hand.
const addressOf = (name) => {
  const m = memberOf(name)
  if (!m) return ''
  return [m.http && m.http !== '—' ? `HTTP ${m.http}` : '', m.raftAddr && m.raftAddr !== '—' ? `Raft ${m.raftAddr}` : ''].filter(Boolean).join('\n')
}
</script>

<style scoped>
/* The verdict on the left, five number cells beside it, hairlines between;
   what needs you under a hairline in the same card. */
.cs-card { overflow: hidden; }
.cs-status { display: grid; grid-template-columns: minmax(260px, 1.5fr) repeat(5, minmax(0, 1fr)); }
.cs-verdict { min-width: 0; padding: 15px 18px; border-right: 1px solid var(--bd); }
.cs-word { display: inline-flex; align-items: center; gap: 7px; margin-bottom: 6px; font-size: 12px; font-weight: 600; color: var(--text-mid); }
.cs-word.bad { color: var(--ember-400); }
.cs-word.warn { color: var(--warn-400); }
.cs-title { margin: 0; font-size: 16px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.3; color: var(--text-hi); text-wrap: balance; }
.cs-line { margin: 5px 0 0; font-size: 12px; color: var(--text-low); }
.cs-kpi { min-width: 0; padding: 15px 16px; border-right: 1px solid var(--bd); display: flex; flex-direction: column; justify-content: center; gap: 5px; }
.cs-kpi:last-child { border-right: 0; }
.cs-kpi-k { font-size: 11.5px; color: var(--text-low); white-space: nowrap; }
.cs-kpi-v { font-size: 18px; font-weight: 600; letter-spacing: -0.01em; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.cs-kpi-v.bad { color: var(--ember-400); }
.cs-kpi-v.warn { color: var(--warn-400); }
@media (max-width: 1100px) {
  .cs-status { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .cs-verdict { grid-column: 1 / -1; border-right: 0; border-bottom: 1px solid var(--bd); }
  .cs-kpi { border-bottom: 1px solid var(--bd); }
  .cs-kpi:nth-child(4) { border-right: 0; }
  .cs-kpi:nth-child(n+5) { border-bottom: 0; }
}
/* A phone: two cells a row, the fifth across, so no number is cut. */
@media (max-width: 480px) {
  .cs-status { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .cs-kpi, .cs-kpi:nth-child(4), .cs-kpi:nth-child(n+5) { padding: 12px; border-right: 1px solid var(--bd); border-bottom: 1px solid var(--bd); }
  .cs-kpi:nth-child(odd) { border-right: 0; }
  .cs-kpi:last-child { grid-column: 1 / -1; border-right: 0; border-bottom: 0; }
  .cs-kpi-v { font-size: 16px; }
}
.cs-table td:first-child { white-space: nowrap; }
.cs-quiet { padding: 12px 18px; border-top: 1px solid var(--bd); font-size: 13px; color: var(--text-low); }
.cs-issues { list-style: none; margin: 0; padding: 0; border-top: 1px solid var(--bd); }
.cs-issues li + li { border-top: 1px solid var(--bd-soft); }
.cs-issue { display: flex; align-items: center; gap: 12px; padding: 11px 18px; color: inherit; text-decoration: none; }
.cs-issue:hover { background: var(--ink-3); }
.cs-issue:focus-visible { outline-offset: -2px; }
.cs-issue-text { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
.cs-issue-title { font-size: 13px; font-weight: 500; color: var(--text-hi); }
.cs-issue-why { font-size: 12px; color: var(--text-mid); }
.cs-go { color: var(--text-low); flex: none; }
.cs-more { margin-left: auto; display: inline-flex; align-items: center; gap: 2px; font-size: 12.5px; color: var(--text-mid); text-decoration: none; }
.cs-more:hover { color: var(--text-hi); }
.cs-node { color: var(--text-hi); text-decoration: none; }
.cs-node:hover { text-decoration: underline; text-underline-offset: 3px; }
.cs-table td.bad { color: var(--ember-400); }
.cs-table td.warn { color: var(--warn-400); }
</style>
