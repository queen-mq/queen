<template>
  <!-- NOT HERE. The broker predates the raft routes, or the proxy in front
       does not classify them: a fact about the cell, said once and quietly.
       stores/routeSupport.js remembers it for the cluster epoch, so only
       "Check again" asks a second time. -->
  <div v-if="absent" class="card">
    <div class="empty-state">
      <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
        <circle cx="12" cy="5" r="2.5" />
        <circle cx="5" cy="18" r="2.5" />
        <circle cx="19" cy="18" r="2.5" />
        <path stroke-linecap="round" d="M10.8 7.2L6.2 15.8M13.2 7.2l4.6 8.6M7.5 18h9" />
      </svg>
      <h3>The Raft cluster view is not available on this broker</h3>
      <p>
        <code>GET /api/v1/raft/members</code> is not served here — the broker
        predates it, or the proxy in front does not classify it. Asking again
        cannot change that, and nothing else on this page is affected.
      </p>
      <button class="btn btn-ghost" @click="$emit('recheck')">Check again</button>
    </div>
  </div>

  <!-- First paint only: a refresh keeps the rows the operator is reading. -->
  <template v-else-if="loading">
    <div class="skeleton" style="height:46px; margin-bottom:16px;" />
    <div class="card">
      <div class="card-body"><div class="skeleton" style="height:144px;" /></div>
    </div>
  </template>

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

    <div class="counts-strip rc-strip">
      <div class="counts-group">
        <span class="count-item-label">cluster</span>
        <span class="count-item count-static">
          <strong>{{ view.clusterId || '—' }}</strong>
        </span>
        <!-- Names read label-first ("leader queen-raft-1", "term 7"); the
             count after them keeps the strip's value-first idiom. -->
        <span class="count-sep">·</span>
        <span class="count-item count-static">
          <span>leader</span>
          <strong class="num" :class="numTone(leaderTone)">{{ leaderName }}</strong>
        </span>
        <span class="count-sep">·</span>
        <span class="count-item count-static">
          <span>term</span>
          <strong>{{ count(view.term) }}</strong>
        </span>
        <span class="count-sep">·</span>
        <span
          class="count-item count-static"
          :title="`Voters that answer and take part, against the voters in the membership. ${view.quorum.majority ?? '—'} are needed to elect a leader and commit a write.`"
        >
          <strong class="num" :class="numTone(view.quorum.severity)">{{ view.quorum.up }}/{{ view.quorum.voters }}</strong>
          <span>voters reachable</span>
        </span>
      </div>

      <div class="counts-group counts-group-right">
        <span v-if="view.singleNode" class="count-item count-static count-tight">
          <span class="count-suffix">single node · not replicated</span>
        </span>
        <template v-else>
          <span class="count-item count-static count-tight">
            <strong>{{ view.quorum.majority ?? '—' }}</strong>
            <span class="count-suffix">for quorum</span>
          </span>
          <span class="count-sep">·</span>
          <span class="count-item count-static count-tight">
            <strong>{{ view.quorum.spare }}</strong>
            <span class="count-suffix">to spare</span>
          </span>
          <template v-if="view.quorum.learners">
            <span class="count-sep">·</span>
            <span class="count-item count-static count-tight">
              <strong>{{ view.quorum.learners }}</strong>
              <span class="count-suffix">learner{{ view.quorum.learners === 1 ? '' : 's' }}</span>
            </span>
          </template>
        </template>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Members</h3>
        <span class="card-sub">
          {{ view.rows.length }} member{{ view.rows.length === 1 ? '' : 's' }}<template v-if="view.self !== null"> · as seen by node {{ view.self }}</template>
        </span>
        <span class="chip chip-mute">cell-level</span>
        <span class="muted">{{ stamp }}</span>
      </div>
      <div class="card-body">
        <div v-if="!view.rows.length" class="panel-msg">No member reported.</div>
        <div v-else class="rc-scroll">
          <table class="t rc-table">
            <thead>
              <tr>
                <th>Node</th>
                <th>State</th>
                <th class="right">Term</th>
                <th class="right" title="The last entry this node has applied to its store">Applied</th>
                <th class="right" title="The last entry a majority of voters holds">Committed</th>
                <th class="right" title="The last entry a durable point of this node's store covers">Durable</th>
                <th class="right" title="Entries the leader holds that this follower has not matched">Lag</th>
                <th class="right" title="Time since the leader last heard from this follower">Heartbeat</th>
                <th class="right">Log</th>
                <th class="right" title="Share of the store's map in use; from 85% the broker refuses new writes">Store</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in view.rows" :key="row.key">
                <td :title="nodeTitle(row)">
                  <div class="rc-node">
                    <span class="rc-node-name">{{ nodeName(row) }}</span>
                    <span v-if="row.isSelf" class="chip chip-mute">this node</span>
                  </div>
                  <div class="rc-sub font-mono">{{ nodeSub(row) }}</div>
                  <div v-if="row.error && !row.unreachable" class="rc-err">{{ row.error }}</div>
                </td>
                <td>
                  <span class="chip" :class="row.chip.cls"><span class="dot"></span>{{ row.chip.label }}</span>
                  <span v-if="row.role === 'learner' && row.state !== 'learner'" class="rc-role">learner</span>
                </td>

                <!-- Nobody could ask it anything: one cell that says so, not a
                     row of dashes that reads like a node with nothing to do. -->
                <td v-if="row.unreachable" :colspan="DATA_COLUMNS" class="rc-down">
                  {{ row.error || 'No answer from this member' }}<span v-if="row.raftAddr" class="rc-sub font-mono"> · raft {{ row.raftAddr }}</span>
                </td>
                <template v-else>
                  <td class="right font-mono tabular-nums">{{ count(row.term) }}</td>
                  <td class="right font-mono tabular-nums">{{ formatIndex(row.applied) }}</td>
                  <td class="right font-mono tabular-nums">{{ formatIndex(row.committed) }}</td>
                  <td class="right font-mono tabular-nums">{{ formatIndex(row.durable) }}</td>
                  <td class="right font-mono num" :class="numTone(row.lagSeverity)">{{ count(row.lag) }}</td>
                  <td class="right font-mono num" :class="numTone(row.heartbeatSeverity)">{{ ms(row.heartbeatMs) }}</td>
                  <td class="right font-mono tabular-nums">
                    {{ bytes(row.logBytes) }}<span v-if="row.logFiles !== null" class="rc-sub"> · {{ count(row.logFiles) }} files</span>
                  </td>
                  <td class="right font-mono num" :class="numTone(row.mapSeverity)" :title="mapTitle(row)">
                    {{ formatMapPct(row.mapPct) }}
                  </td>
                </template>
              </tr>
            </tbody>
          </table>
        </div>
        <p class="rc-note">
          <template v-if="view.singleNode">
            One node, one copy of the log: nothing replicates it, so lag and heartbeat do not apply.
          </template>
          <template v-else>
            Lag and heartbeat are the leader's view of each follower; the leader's own row has neither.
          </template>
        </p>
      </div>
    </div>
  </template>
</template>

<script setup>
import { computed } from 'vue'

import { formatBytes, formatDuration, formatNumber } from '@/composables/useApi'
import {
  clusterAlert, clusterView, formatIndex, formatMapPct,
} from '@/composables/useRaftCluster'
import { SEV_BAD, SEV_NONE, SEV_WARN, numTone } from '@/composables/useSeverity'

// Presentational: System.vue owns the fetch (it is one of the page's Source
// options, refreshed by the page's own refresh) and hands over the last good
// /api/v1/raft/members payload plus the state around it.
const props = defineProps({
  /** GET /api/v1/raft/members, the last payload that loaded. */
  members: { type: Object, default: null },
  /** First load in flight, nothing to show yet. */
  loading: { type: Boolean, default: false },
  /** The broker does not serve the route: the quiet state. */
  absent: { type: Boolean, default: false },
  /** Freshness for the card header, from useStamp. */
  stamp: { type: String, default: '' },
})
defineEmits(['recheck'])

// Every column after Node and State; an unreachable row spans them all.
const DATA_COLUMNS = 8

const view = computed(() => clusterView(props.members))
const alert = computed(() => clusterAlert(view.value))

const leaderName = computed(() => {
  const v = view.value
  if (v?.leader) return nodeName(v.leader)
  return v?.leaderId !== null && v?.leaderId !== undefined ? `node ${v.leaderId}` : 'none'
})
const leaderTone = computed(() => {
  const v = view.value
  if (!v || v.leader || v.leaderId !== null) return SEV_NONE
  return v.quorum.hasQuorum === false ? SEV_BAD : SEV_WARN
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
</script>

<style scoped>
/* The strip sits in the page's card column, so it keeps the 16px block rhythm
   rather than the counts strip's own 14px (set for the metric table under it
   on Dashboard and QueueDetail). */
.rc-strip { margin-bottom: 16px; }

.rc-scroll { overflow-x: auto; }
.rc-table td { white-space: nowrap; }
.right { text-align: right; }

.rc-node { display: flex; align-items: center; gap: 6px; }
.rc-node-name { font-weight: 500; color: var(--text-hi); }
.rc-sub { font-size: 11px; color: var(--text-low); }
.rc-role { margin-left: 6px; font-size: 11px; color: var(--text-low); }
.rc-err { margin-top: 2px; font-size: 11px; color: var(--ember-400); white-space: normal; }
.rc-table td.rc-down { font-size: 12px; color: var(--ember-400); white-space: normal; }

/* System's .sys-note, for the one line under the table. */
.rc-note { margin-top: 10px; font-size: 11.5px; color: var(--text-low); }
</style>
