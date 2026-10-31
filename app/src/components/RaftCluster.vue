<template>
  <!-- NOT HERE. The broker predates the raft routes, or the proxy in front
       does not classify them: a fact about the cell, said once and quietly.
       stores/routeSupport.js remembers it for the cluster epoch, so only
       "Check again" asks a second time. -->
  <div v-if="absent" class="card">
    <div class="empty-state">
      <Icon name="network" :size="28" class="empty-state-icon" />
      <p title="GET /api/v1/raft/members is not served here: the broker predates it, or the proxy in front does not classify it. Nothing else on this page is affected.">
        This broker does not serve the Raft view
      </p>
      <button class="btn btn-ghost" @click="$emit('recheck')">Check again</button>
    </div>
  </div>

  <!-- First paint only: a refresh keeps the rows the operator is reading. -->
  <div v-else-if="loading" class="card">
    <div class="card-body"><div class="skeleton" style="height:144px;" /></div>
  </div>

  <template v-else-if="view">
    <!-- The one thing to know before reading a row: can this cluster still
         elect and commit. Gravest first, one at a time (clusterAlert). -->
    <div
      v-if="alert"
      class="status-banner view-banner"
      :class="alert.level === 'bad' ? 'banner-bad' : 'banner-warn'"
    >
      <span><strong>{{ alert.title }}</strong> · {{ alert.detail }}</span>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Members</h3>
        <span class="card-sub" :title="quorumTitle">{{ quorumLine }}</span>
        <span class="muted">{{ stamp }}</span>
      </div>
      <div v-if="!view.rows.length" class="rc-quiet">No member reported.</div>
      <div v-else class="table-container">
        <table class="t rc-table">
          <thead>
            <tr>
              <th>Node</th>
              <th>State</th>
              <th class="num">Term</th>
              <th class="num" title="The last entry this node has applied to its store">Applied</th>
              <th class="num" title="The last entry a majority of voters holds">Committed</th>
              <th class="num" title="The last entry a durable point of this node's store covers">Durable</th>
              <th class="num" title="Entries the leader holds that this follower has not matched. The leader's view: its own row has none">Lag</th>
              <th class="num" title="Time since the leader last heard from this follower. The leader's view: its own row has none">Heartbeat</th>
              <th class="num" title="What this node sent (out) and received (in) over the Raft port, per second over the metrics collector's last interval; hover a cell for each channel">Traffic</th>
              <th class="num">Log</th>
              <th class="num" title="Share of the store's map in use; from 85% the broker refuses new writes">Store</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="(row, i) in view.rows"
              :key="row.key"
              :ref="(el) => { rowEls[i] = el }"
              :class="{ on: i === focusIndex }"
            >
              <td :title="nodeTitle(row)">
                <div class="rc-node">
                  <span class="rc-node-name">{{ nodeName(row) }}</span>
                  <span v-if="row.isSelf" class="rc-self">this node</span>
                </div>
                <div class="rc-sub">{{ nodeSub(row) }}</div>
                <div v-if="row.error && !row.unreachable" class="rc-err">{{ row.error }}</div>
              </td>
              <td>
                <span class="rc-state">
                  <span class="rc-mark" aria-hidden="true"><span v-if="stateMark(row)" class="g" :class="stateMark(row)" /></span>{{ capital(row.chip.label) }}
                </span>
                <span v-if="row.role === 'learner' && row.state !== 'learner'" class="rc-role">learner</span>
              </td>

              <!-- Nobody could ask it anything: one cell that says so, not a
                   row of dashes that reads like a node with nothing to do. -->
              <td v-if="row.unreachable" :colspan="DATA_COLUMNS" class="rc-down">
                {{ row.error || 'No answer from this member' }}<span v-if="row.raftAddr" class="rc-sub"> · raft {{ row.raftAddr }}</span>
              </td>
              <template v-else>
                <td class="num">{{ count(row.term) }}</td>
                <td class="num">{{ formatIndex(row.applied) }}</td>
                <td class="num">{{ formatIndex(row.committed) }}</td>
                <td class="num">{{ formatIndex(row.durable) }}</td>
                <td class="num" :class="numTone(row.lagSeverity)">{{ count(row.lag) }}</td>
                <td class="num" :class="numTone(row.heartbeatSeverity)">{{ ms(row.heartbeatMs) }}</td>
                <td class="num" :title="trafficTitle(row.traffic)">
                  <template v-if="row.traffic">
                    {{ formatRate(row.traffic.sent) }}<span class="rc-unit"> out</span>
                    <span class="rc-unit"> · </span>{{ formatRate(row.traffic.received) }}<span class="rc-unit"> in</span>
                  </template>
                  <template v-else>—</template>
                </td>
                <td class="num" :title="row.logFiles !== null ? `${count(row.logFiles)} files` : undefined">{{ bytes(row.logBytes) }}</td>
                <td class="num" :class="numTone(row.mapSeverity)" :title="mapTitle(row)">
                  {{ formatMapPct(row.mapPct) }}
                </td>
              </template>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </template>
</template>

<script setup>
import { computed, nextTick, watch } from 'vue'

import Icon from '@/components/Icon.vue'
import { formatBytes, formatDuration, formatNumber } from '@/composables/useApi'
import {
  clusterAlert, clusterView, formatIndex, formatMapPct,
} from '@/composables/useRaftCluster'
import { numTone } from '@/composables/useSeverity'
import { formatRate, trafficTitle } from '@/composables/useTraffic'

// Presentational: System.vue owns the fetch (its Raft tab, refreshed by the
// page's own refresh) and hands over the last good /api/v1/raft/members
// payload plus the state around it.
const props = defineProps({
  /** GET /api/v1/raft/members, the last payload that loaded. */
  members: { type: Object, default: null },
  /** First load in flight, nothing to show yet. */
  loading: { type: Boolean, default: false },
  /** The broker does not serve the route: the quiet state. */
  absent: { type: Boolean, default: false },
  /** Freshness for the card header, from useStamp. */
  stamp: { type: String, default: '' },
  /** The row of the node picked in the page's list (node-id order); -1 for none. */
  focusIndex: { type: Number, default: -1 },
})
defineEmits(['recheck'])

// Every column after Node and State; an unreachable row spans them all.
const DATA_COLUMNS = 9

const view = computed(() => clusterView(props.members))
const alert = computed(() => clusterAlert(view.value))

// The quorum in one line, where the strip used to stand: what it takes to
// elect and commit, and how many can still fail.
const quorumLine = computed(() => {
  const v = view.value
  if (!v) return ''
  const q = v.quorum
  const term = v.term !== null ? ` · term ${count(v.term)}` : ''
  if (v.singleNode) return `single node · not replicated${term}`
  const learners = q.learners ? ` · ${q.learners} learner${q.learners === 1 ? '' : 's'}` : ''
  return `${q.up}/${q.voters} voters · ${q.majority ?? '—'} for quorum · ${q.spare} to spare${learners}${term}`
})
const quorumTitle = computed(() => {
  const v = view.value
  if (!v) return ''
  return [
    v.clusterId ? `Cluster ${v.clusterId}` : null,
    `Leader ${leaderName.value}`,
    v.singleNode
      ? 'One node, one copy of the log: nothing replicates it, so lag and heartbeat do not apply'
      : `Voters that answer and take part, against the voters in the membership; ${v.quorum.majority ?? '—'} are needed to elect a leader and commit a write`,
  ].filter(Boolean).join('\n')
})
const leaderName = computed(() => {
  const v = view.value
  if (v?.leader) return nodeName(v.leader)
  return v?.leaderId !== null && v?.leaderId !== undefined ? `node ${v.leaderId}` : 'none'
})

/** A number we hold, or an em dash — never a 0 standing in for "unknown". */
const count = (v) => (v === null || v === undefined ? '—' : formatNumber(v))
const bytes = (v) => (v === null || v === undefined ? '—' : formatBytes(v))
const ms = (v) => (v === null || v === undefined ? '—' : formatDuration(v))

const nodeName = (row) => row.hostname || (row.nodeId !== null ? `node ${row.nodeId}` : 'unknown node')
const nodeSub = (row) =>
  [row.hostname && row.nodeId !== null ? `node ${row.nodeId}` : null, row.httpAddr]
    .filter(Boolean)
    .join(' · ') || '—'
const nodeTitle = (row) =>
  [
    row.raftAddr ? `raft ${row.raftAddr}` : null,
    row.httpAddr ? `http ${row.httpAddr}` : null,
    row.version ? `v${row.version}` : null,
    row.uptimeSeconds !== null ? `up ${formatDuration(row.uptimeSeconds * 1000)}` : null,
  ].filter(Boolean).join(' · ')
const mapTitle = (row) => {
  const parts = []
  if (row.mapUsed !== null && row.mapBytes !== null) parts.push(`${bytes(row.mapUsed)} of ${bytes(row.mapBytes)} map in use`)
  if (row.readers !== null && row.maxReaders !== null) parts.push(`${count(row.readers)} of ${count(row.maxReaders)} readers`)
  return parts.join(' · ')
}

// A state carries a mark only when it needs you (a candidate, a member down);
// leader and follower are words.
const stateMark = (row) => ({ 'chip-warn': 'warn', 'chip-bad': 'bad' }[row.chip.cls] || '')
const capital = (label) => (label ? label.charAt(0).toUpperCase() + label.slice(1) : label)

// The picked node's row, brought into view when the pick changes or the tab
// opens on one: the table may sit below the fold. Plain, not reactive: the
// rows' elements, set by the template's ref callback.
const rowEls = []
watch(() => props.focusIndex, async (i) => {
  if (i < 0) return
  await nextTick()
  rowEls[i]?.scrollIntoView?.({ block: 'nearest' })
}, { immediate: true })
</script>

<style scoped>
.rc-quiet { padding: 14px 16px; font-size: 13px; color: var(--text-low); }
.rc-table td { white-space: nowrap; }
/* The node picked in the page's list: the list's own selection tone. */
.rc-table tr.on > td, .rc-table tr.on:hover > td { background: var(--selected); --text-low: var(--text-mid); }

.rc-node { display: flex; align-items: baseline; gap: 8px; }
.rc-node-name { font-weight: 500; color: var(--text-hi); }
.rc-self { font-size: 12px; color: var(--text-low); }
.rc-state { display: inline-flex; align-items: center; gap: 8px; color: var(--text-hi); }
/* The mark's slot, kept when empty so the states line up (the lists' rule). */
.rc-mark { flex: none; width: 10px; display: inline-flex; justify-content: center; }
.rc-sub { margin-top: 2px; font-size: 12px; color: var(--text-low); }
.rc-unit { color: var(--text-low); }
.rc-role { margin-left: 6px; font-size: 12px; color: var(--text-low); }
.rc-err { margin-top: 2px; font-size: 12px; color: var(--ember-400); white-space: normal; }
.rc-table td.rc-down { font-size: 12px; color: var(--ember-400); white-space: normal; }
</style>
