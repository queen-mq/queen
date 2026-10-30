<template>
  <!--
    System: the cell under every tenant, an operator's page. Seven views in a
    fixed sub sidebar under three groups, kept in the address as ?view=
    (composables/systemViews.js); the node is the header's picker (?node=),
    on the views it narrows. Each row of the list carries the one number of
    its view, coloured only where that number is the trouble.
  -->
  <SubSidebar label="System sections" :current="currentName">
    <template #side>
      <nav class="sys-side" aria-label="System views">
        <div v-for="g in side" :key="g.label || 'top'" class="ss-group">
          <div v-if="g.label" class="ss-label">{{ g.label }}</div>
          <router-link
            v-for="r in g.rows"
            :key="r.key"
            :to="viewTo(r.key)"
            class="ss-row"
            :class="{ on: view === r.key }"
            :aria-current="view === r.key ? 'page' : undefined"
            :title="r.title || undefined"
          >
            <Icon v-if="r.icon" :name="r.icon" :size="16" class="ss-icon" />
            <span class="ss-text"><span class="ss-name">{{ r.name }}</span></span>
            <span class="ss-num" :class="[r.tone, { 'sys-side-word': r.key === 'overview' }]">{{ r.num }}<span v-if="r.tone" class="sr-only"> · {{ r.tone === 'bad' ? 'failing' : 'needs attention' }}</span></span>
          </router-link>
        </div>
      </nav>
    </template>

    <PaneHead :title="currentName" :facts="canOperate ? facts : []">
      <template v-if="canOperate && viewInfo.picker" #actions>
        <UiSelect v-model="focusNode" label="Node" :options="nodeOptions" aria-label="Node" />
      </template>
    </PaneHead>

    <div class="view-container">
      <div v-if="!canOperate" class="card">
        <div class="empty-state">
          <Icon name="shield-check" :size="28" class="empty-state-icon" />
          <p :title="`Nothing here is scoped to ${actingTenantSlug || 'your tenant'}: it covers every tenant on the cell`">Only a live operator can open this page</p>
        </div>
      </div>


      <!-- =================== OVERVIEW: the cell at a glance =================== -->
      <div v-else-if="view === 'overview'" class="sys-stack">
        <section class="card sys-status" aria-label="Status of the cell">
          <div class="sys-verdict">
            <span class="sys-verdict-word" :class="verdict.tone"><span v-if="verdict.tone" class="g" :class="verdict.tone" aria-hidden="true" />{{ verdict.word }}</span>
            <h2 class="sys-verdict-title" :title="verdict.headline">{{ verdict.headline }}</h2>
            <p v-if="verdictLine" class="sys-verdict-line">{{ verdictLine }}</p>
          </div>
          <div v-for="c in statusCells" :key="c.k" class="sys-kpi" :title="c.title">
            <span class="sys-kpi-k">{{ c.k }}</span>
            <span class="sys-kpi-v" :class="c.tone">{{ c.v }}</span>
          </div>
        </section>

        <section class="card" aria-labelledby="sys-att-title">
          <div class="card-header"><h3 id="sys-att-title">Needs attention</h3></div>
          <div v-if="!cell" class="sys-quiet">{{ nodesNote }}</div>
          <div v-else-if="!issueList.length" class="sys-quiet">Nothing needs you</div>
          <ul v-else class="sys-issues">
            <li v-for="i in issueList" :key="i.key">
              <router-link class="sys-issue" :to="i.view ? viewTo(i.view) : i.to">
                <span class="g" :class="i.sev" aria-hidden="true" />
                <span class="sys-issue-text"><span class="sys-issue-title">{{ i.title }}</span><span class="sys-issue-why">{{ i.why }}</span></span>
                <Icon name="chevron-right" :size="14" class="sys-issue-go" />
              </router-link>
            </li>
          </ul>
        </section>

        <section v-if="cell" class="card" aria-labelledby="sys-now-title">
          <div class="card-header">
            <h3 id="sys-now-title">Each node now</h3>
            <span class="muted">{{ stamp(raftMembers) }}</span>
          </div>
          <div class="table-container">
            <table class="t sys-table">
              <thead>
                <tr>
                  <th>Node</th>
                  <th>Raft</th>
                  <th class="num" title="Of the cores the node may use">CPU</th>
                  <th class="num" title="Of the node's memory limit">Memory</th>
                  <th class="num" title="Of the data disk">Disk</th>
                  <th class="num">Network in / out</th>
                  <th class="num" title="Bytes over the Raft port">Raft out / in</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="n in nowRows" :key="n.name">
                  <td><router-link class="sys-node-link" :to="nodeTo(n.name)">{{ n.name }}</router-link></td>
                  <td :class="n.roleTone">{{ n.role }}</td>
                  <td class="num" :class="n.cpu.tone">{{ n.cpu.text }}</td>
                  <td class="num" :class="n.mem.tone">{{ n.mem.text }}</td>
                  <td class="num" :class="n.disk.tone">{{ n.disk.text }}</td>
                  <td class="num">{{ n.net }}</td>
                  <td class="num">{{ n.raft }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
      </div>

      <!-- =================== NODES: who they are, and their last sample =================== -->
      <div v-else-if="view === 'nodes'" class="sys-stack">
        <div v-if="raftMembers.failed.value && !raftMembersAbsent" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(raftMembers.lastUpdated.value)"><strong>Could not load the Raft cluster</strong> · {{ describeRaftFailure(raftMembers.error.value) }}<template v-if="raftMembers.data.value"> · showing the last members{{ raftMembers.lastUpdated.value ? ` (${formatTimestamp(raftMembers.lastUpdated.value)})` : '' }}</template></span>
        </div>

        <section class="card" aria-labelledby="sys-nodes-title">
          <div class="card-header">
            <h3 id="sys-nodes-title">Members</h3>
            <span class="muted">{{ stamp(raftMembers) }}</span>
          </div>
          <div v-if="!memberRows.length" class="sys-quiet">{{ nodesNote }}</div>
          <div v-else class="table-container">
            <table class="t sys-table">
              <thead>
                <tr>
                  <th>Node</th>
                  <th>Role</th>
                  <th>Version</th>
                  <th class="num">Up</th>
                  <th>HTTP</th>
                  <th>Raft address</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="m in memberRows" :key="m.name">
                  <td><span class="sys-host">{{ m.name }}</span></td>
                  <td :class="m.roleTone">{{ m.role }}</td>
                  <td>{{ m.version }}</td>
                  <td class="num">{{ m.up }}</td>
                  <td class="sys-mono">{{ m.http }}</td>
                  <td class="sys-mono">{{ m.raftAddr }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
        <div v-if="replicas.length" class="card">
          <div class="card-header">
            <h3>Servers</h3>
            <span class="card-sub">last sample</span>
            <span class="muted">{{ stamp(metrics) }}</span>
          </div>
          <div class="table-container">
            <table class="t sys-table">
              <thead>
                <tr>
                  <th>Server</th>
                  <th class="num">CPU user</th>
                  <th class="num">CPU sys</th>
                  <th class="num">Memory</th>
                  <th class="num">Network in / out</th>
                  <th class="num">Disk read / write</th>
                  <th class="num">Raft out / in</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="replica in replicas" :key="`${replica.hostname}:${replica.port}`">
                  <td><span class="sys-host">{{ replica.hostname }}</span><span class="sys-port">:{{ replica.port }}</span></td>
                  <td class="num">{{ pct(cpuOf(replica, 'user_us')) }}</td>
                  <td class="num">{{ pct(cpuOf(replica, 'system_us')) }}</td>
                  <td class="num">{{ mb(lastOf(replica, ['memory', 'rss_bytes'])) }}</td>
                  <td class="num">{{ formatRate(lastOf(replica, ['network', 'received_bytes_per_sec'])) }}<span class="sys-sep"> / </span>{{ formatRate(lastOf(replica, ['network', 'sent_bytes_per_sec'])) }}</td>
                  <td class="num">{{ formatRate(lastOf(replica, ['disk', 'read_bytes_per_sec'])) }}<span class="sys-sep"> / </span>{{ formatRate(lastOf(replica, ['disk', 'written_bytes_per_sec'])) }}</td>
                  <td class="num">{{ formatRate(lastOf(replica, ['raft_traffic', 'sent_bytes_per_sec'])) }}<span class="sys-sep"> / </span>{{ formatRate(lastOf(replica, ['raft_traffic', 'received_bytes_per_sec'])) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

      </div>

      <!-- =================== RAFT: the members, and what crosses the Raft port =================== -->
      <div v-else-if="view === 'raft'" class="sys-stack">
        <div v-if="raftMembers.failed.value && !raftMembersAbsent" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(raftMembers.lastUpdated.value)"><strong>Could not load the Raft cluster</strong> · {{ describeRaftFailure(raftMembers.error.value) }}<template v-if="raftMembers.data.value"> · showing the last members{{ raftMembers.lastUpdated.value ? ` (${formatTimestamp(raftMembers.lastUpdated.value)})` : '' }}</template></span>
        </div>

        <RaftCluster
          :members="raftMembers.data.value"
          :loading="raftMembers.loading.value && !raftMembers.data.value"
          :absent="raftMembersAbsent"
          :stamp="stamp(raftMembers)"
          :focus-index="focusIndex"
          @recheck="recheckRaft"
        />

        <div v-if="metrics.failed.value" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(metrics.lastUpdated.value)"><strong>Could not load server metrics</strong> · {{ describeApiError(metrics.error.value) }}<template v-if="systemData"> · showing the last samples{{ metrics.lastUpdated.value ? ` (${formatTimestamp(metrics.lastUpdated.value)})` : '' }}</template></span>
        </div>
        <div v-if="focusNode && systemData && !replicas.length" class="status-banner banner-info view-banner">
          <span><strong>{{ focusNode }}</strong> sent no samples in this range.</span>
        </div>

        <PageTools>
          <span class="sys-range" :title="bucketTitle">
            <UiSelect v-model="rangeChoice" label="Range" :options="rangeOptions" aria-label="Time range" />
          </span>
          <template #view>
            <div class="tool-seg" :title="viewTitle">
              <span class="tool-label">View</span>
              <div class="seg" role="group" aria-label="View">
                <button :class="{ on: viewMode === 'individual' }" :aria-pressed="viewMode === 'individual'" @click="viewMode = 'individual'">Per server</button>
                <button :class="{ on: viewMode === 'aggregate' }" :aria-pressed="viewMode === 'aggregate'" @click="viewMode = 'aggregate'">Aggregate</button>
              </div>
            </div>
            <UiSelect v-model="aggregationType" label="Metric" :options="aggregationTypes" aria-label="Value of each point" />
          </template>
        </PageTools>

        <div v-if="customMode" class="page-tools">
          <label class="tool-field">
            <span class="tool-label">From</span>
            <input v-model="customFrom" type="datetime-local" class="input" :title="formatTimestampUtc(customFrom)" />
          </label>
          <label class="tool-field">
            <span class="tool-label">To</span>
            <input v-model="customTo" type="datetime-local" class="input" :title="formatTimestampUtc(customTo)" />
          </label>
          <button class="btn btn-primary" :disabled="!customRangeValid" @click="applyCustomRange">Apply</button>
          <span v-if="customError" class="tool-note is-bad">{{ customError }}</span>
        </div>

        <div v-if="metricsFirstLoad" class="sys-stack">
          <div class="sys-grid-2">
            <div v-for="i in 2" :key="i" class="card"><div class="card-body"><div class="skeleton" style="height:240px;" /></div></div>
          </div>
        </div>
        <template v-else-if="systemData">
          <div class="card">
            <div class="card-header" :title="raftTrafficHelp">
              <h3>Raft traffic</h3>
              <span class="card-sub">{{ viewMode === 'individual' ? 'solid sent · dashed received' : 'bytes on the wire, by channel' }}</span>
              <span class="muted">{{ stamp(metrics) }}</span>
            </div>
            <div class="card-body">
              <div v-if="cell?.raft.single" class="panel-msg">One node, one copy of the log: nothing crosses the Raft port</div>
              <BaseChart v-else-if="raftTrafficChart.hasData" type="line" :data="raftTrafficChart" :options="raftTrafficOptions" height="240px" />
              <div v-else class="panel-msg" title="Older releases do not record it">No Raft traffic samples in this range</div>
            </div>
          </div>

        </template>
      </div>

      <!-- =================== STORAGE: the log and the store on each node =================== -->
      <div v-else-if="view === 'storage'" class="sys-stack">
        <div v-if="raftMembers.failed.value && !raftMembersAbsent" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(raftMembers.lastUpdated.value)"><strong>Could not load the Raft cluster</strong> · {{ describeRaftFailure(raftMembers.error.value) }}<template v-if="raftMembers.data.value"> · showing the last members{{ raftMembers.lastUpdated.value ? ` (${formatTimestamp(raftMembers.lastUpdated.value)})` : '' }}</template></span>
        </div>

        <section class="card" aria-labelledby="sys-store-title">
          <div class="card-header">
            <h3 id="sys-store-title">On each node</h3>
            <span class="muted">{{ stamp(raftMembers) }}</span>
          </div>
          <div v-if="!memberRows.length" class="sys-quiet">{{ nodesNote }}</div>
          <div v-else class="table-container">
            <table class="t sys-table">
              <thead>
                <tr>
                  <th>Node</th>
                  <th class="num">Log</th>
                  <th class="num">Files</th>
                  <th class="num" title="Share of the store's map in use; from 85% the broker refuses new writes">Store map</th>
                  <th class="num">Used of the map</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="m in memberRows" :key="m.name">
                  <td><span class="sys-host">{{ m.name }}</span></td>
                  <td class="num">{{ m.log }}</td>
                  <td class="num">{{ m.files }}</td>
                  <td class="num" :class="m.mapTone">{{ m.map }}</td>
                  <td class="num">{{ m.mapUsed }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
        <div v-if="!raftStatusAbsent" class="card" :class="{ 'sys-alarm': raftAlarm }">
          <div class="card-header">
            <h3>Replicated log</h3>
            <span v-if="node && node.nodeId !== null" class="card-sub" title="The node this console's calls reach">as node {{ node.nodeId }}{{ node.hostname ? ` · ${node.hostname}` : '' }} holds it</span>
            <span class="muted">{{ stamp(raftStatus) }}</span>
          </div>
          <div class="card-body">
            <div v-if="raftStatus.failed.value" class="panel-err">
              Log state unavailable · {{ describeRaftFailure(raftStatus.error.value) }}
            </div>
            <div class="stat-grid stat-grid-6">
              <div class="stat">
                <div class="stat-label">Role</div>
                <div class="stat-value">
                  <span v-if="!node?.state">—</span>
                  <span v-else class="sys-state"><span v-if="roleMark" class="g" :class="roleMark" aria-hidden="true" />{{ capital(node.chip.label) }}</span>
                </div>
                <div class="stat-foot">{{ node?.clusterNote || '—' }}</div>
              </div>
              <div class="stat">
                <div class="stat-label">Term</div>
                <div class="stat-value">{{ metric(node?.term) }}</div>
                <div class="stat-foot">
                  <span v-if="node" :class="{ 'num warn': node.leaderSeverity === 'warn' }">{{ node.leaderNote }}</span>
                  <span v-else>—</span>
                </div>
              </div>
              <!-- Three indexes, one tile: applied is this node's own; the two
                   under it are what it trails, each on its own line. -->
              <div class="stat">
                <div class="stat-label">Applied</div>
                <div class="stat-value">{{ formatIndex(node?.applied) }}</div>
                <div class="stat-foot">committed <span class="tabular-nums">{{ formatIndex(node?.committed) }}</span></div>
                <div class="stat-foot">durable <span class="tabular-nums">{{ formatIndex(node?.durable) }}</span></div>
              </div>
              <div class="stat">
                <div class="stat-label">Inflight</div>
                <div class="stat-value">{{ metric(node?.inflight) }}</div>
                <div class="stat-foot">appended, not applied</div>
              </div>
              <div class="stat">
                <div class="stat-label">Log</div>
                <div class="stat-value">{{ bytes(node?.logBytes) }}</div>
                <div class="stat-foot">{{ metric(node?.logFiles) }} files</div>
              </div>
              <div class="stat">
                <div class="stat-label">Store map</div>
                <div class="stat-value num" :class="numTone(node?.mapSeverity)">{{ formatMapPct(node?.mapPct) }}</div>
                <div class="stat-foot">{{ bytes(node?.mapUsed) }} of {{ bytes(node?.mapBytes) }}</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- =================== CPU AND MEMORY, DISK, NETWORK: each node over time =================== -->
      <div v-else class="sys-stack">
        <div v-if="metrics.failed.value" class="status-banner banner-bad view-banner">
          <span :title="formatTimestampUtc(metrics.lastUpdated.value)"><strong>Could not load server metrics</strong> · {{ describeApiError(metrics.error.value) }}<template v-if="systemData"> · showing the last samples{{ metrics.lastUpdated.value ? ` (${formatTimestamp(metrics.lastUpdated.value)})` : '' }}</template></span>
        </div>
        <div v-if="focusNode && systemData && !replicas.length" class="status-banner banner-info view-banner">
          <span><strong>{{ focusNode }}</strong> sent no samples in this range.</span>
        </div>

        <PageTools>
          <span class="sys-range" :title="bucketTitle">
            <UiSelect v-model="rangeChoice" label="Range" :options="rangeOptions" aria-label="Time range" />
          </span>
          <template #view>
            <div class="tool-seg" :title="viewTitle">
              <span class="tool-label">View</span>
              <div class="seg" role="group" aria-label="View">
                <button :class="{ on: viewMode === 'individual' }" :aria-pressed="viewMode === 'individual'" @click="viewMode = 'individual'">Per server</button>
                <button :class="{ on: viewMode === 'aggregate' }" :aria-pressed="viewMode === 'aggregate'" @click="viewMode = 'aggregate'">Aggregate</button>
              </div>
            </div>
            <UiSelect v-model="aggregationType" label="Metric" :options="aggregationTypes" aria-label="Value of each point" />
          </template>
        </PageTools>

        <div v-if="customMode" class="page-tools">
          <label class="tool-field">
            <span class="tool-label">From</span>
            <input v-model="customFrom" type="datetime-local" class="input" :title="formatTimestampUtc(customFrom)" />
          </label>
          <label class="tool-field">
            <span class="tool-label">To</span>
            <input v-model="customTo" type="datetime-local" class="input" :title="formatTimestampUtc(customTo)" />
          </label>
          <button class="btn btn-primary" :disabled="!customRangeValid" @click="applyCustomRange">Apply</button>
          <span v-if="customError" class="tool-note is-bad">{{ customError }}</span>
        </div>

        <div v-if="metricsFirstLoad" class="sys-stack">
          <div class="sys-grid-2">
            <div v-for="i in 2" :key="i" class="card"><div class="card-body"><div class="skeleton" style="height:240px;" /></div></div>
          </div>
        </div>
        <template v-else-if="systemData">
          <template v-if="view === 'compute'">
            <div class="sys-grid-2">
            <div class="card">
              <div class="card-header">
                <h3>CPU</h3>
                <span class="card-sub">{{ viewMode === 'individual' ? 'user, per server' : replicaCountLabel }}</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="cpuChart.labels.length" type="line" :data="cpuChart" :options="cpuOptions" height="240px" />
                <div v-else class="panel-msg">No CPU samples in this range</div>
              </div>
            </div>
            <div class="card">
              <div class="card-header">
                <h3>Memory</h3>
                <span class="card-sub">{{ viewMode === 'individual' ? 'resident, per server' : replicaCountLabel }}</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="memoryChart.labels.length" type="line" :data="memoryChart" :options="memoryOptions" height="240px" />
                <div v-else class="panel-msg">No memory samples in this range</div>
              </div>
            </div>
            </div>
          <div class="card">
            <div class="card-header">
              <h3>Workers</h3>
              <span class="card-sub" title="Cell-wide, since each broker started">{{ metric(lifetimeAckFailed) }} ack failures since start</span>
              <span class="muted">{{ stamp(status) }}</span>
            </div>
            <div v-if="status.failed.value" class="card-body"><div class="panel-err">{{ describeApiError(status.error.value) }}</div></div>
            <div v-else-if="!workers.length" class="sys-quiet">{{ focusNode ? `No worker of ${focusNode} reported in the last two minutes` : 'No worker reported in the last two minutes' }}</div>
            <div v-else class="table-container">
              <table class="t sys-table">
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th title="How quickly the worker's event loop answers: under 50 ms average and 100 ms peak it is responsive">Event loop</th>
                    <th class="num">Average</th>
                    <th class="num">Peak</th>
                    <th class="num">Messages, 2 min</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="w in workers" :key="`${w.hostname}:${w.workerId}`">
                    <td><span class="sys-host">{{ w.hostname }}</span><span class="sys-port"> · {{ w.workerId }}</span></td>
                    <td>
                      <span class="sys-state"><span class="sys-mark" aria-hidden="true"><span v-if="workerChip(w).mark" class="g" :class="workerChip(w).mark" /></span>{{ workerChip(w).label }}</span>
                    </td>
                    <td class="num">{{ msOrDash(w.avgEventLoopLagMs) }}</td>
                    <td class="num">{{ msOrDash(w.maxEventLoopLagMs) }}</td>
                    <td class="num">{{ metric(toNum(w.messagesProcessed)) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
          </template>

          <template v-else-if="view === 'disk'">
            <section v-if="cell" class="card" aria-labelledby="sys-used-title">
              <div class="card-header">
                <h3 id="sys-used-title">Used, per node</h3>
                <span class="card-sub">of the data disk; from 99.5% writes stop</span>
              </div>
              <div class="sys-bars">
                <div v-for="d in diskRows" :key="d.name" class="sys-bar">
                  <span class="sys-bar-name">{{ d.name }}</span>
                  <span class="sys-bar-track"><span class="sys-bar-fill" :class="d.tone" :style="{ width: `${d.width}%` }" /></span>
                  <span class="sys-bar-v" :class="d.tone">{{ d.text }}</span>
                </div>
              </div>
            </section>
            <div class="card">
              <div class="card-header" title="What the broker process read from and wrote to storage. A read the page cache serves is not a disk read.">
                <h3>Disk</h3>
                <span class="card-sub">{{ viewMode === 'individual' ? 'solid read · dashed write' : replicaCountLabel }}</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="diskChart.hasData" type="line" :data="diskChart" :options="diskOptions" height="240px" />
                <div v-else class="panel-msg" title="Older releases do not record it">No disk samples in this range</div>
              </div>
            </div>
          </template>

          <template v-else-if="view === 'network'">
            <div class="card">
              <div class="card-header" title="What each node's network interfaces received and sent: every client, the Raft port and anything else on them. In a container, its own interfaces; loopback and bridges are left out.">
                <h3>Network</h3>
                <span class="card-sub">{{ viewMode === 'individual' ? 'solid in · dashed out' : replicaCountLabel }}</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <BaseChart v-if="networkChart.hasData" type="line" :data="networkChart" :options="networkOptions" height="240px" />
                <div v-else class="panel-msg" title="A broker records the network on Linux only, and older releases not at all">No network samples in this range</div>
              </div>
            </div>
          </template>
        </template>
      </div>
    </div>
  </SubSidebar>
</template>

<script setup>
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import BaseChart from '@/components/BaseChart.vue'
import Icon from '@/components/Icon.vue'
import PageTools from '@/components/PageTools.vue'
import PaneHead from '@/components/PaneHead.vue'
import RaftCluster from '@/components/RaftCluster.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import UiSelect from '@/components/UiSelect.vue'
import { describeApiError, operator } from '@/api'
import { formatBytes, formatDuration, formatNumber, toNum, useApi } from '@/composables/useApi'
import { brokerStatus, cellIssues } from '@/composables/useBrokerStatus'
import { chartColor } from '@/composables/useChartTheme'
import {
  formatChartLabel, formatDateTimeLocal, formatTimestamp, formatTimestampUtc,
  isMultiDay, validateRange,
} from '@/composables/useFormat'
import {
  clusterView, describeRaftFailure, formatIndex, formatMapPct, nodeView,
} from '@/composables/useRaftCluster'
import { useRefresh } from '@/composables/useRefresh'
import { useRouteRange } from '@/composables/useRouteRange'
import { useRouteState } from '@/composables/useRouteState'
import { numTone } from '@/composables/useSeverity'
import { nodeEntries, nodeReplicas } from '@/composables/systemSide'
import { nodeRows } from '@/composables/overviewViews'
import {
  cellVerdict, systemSideGroups, systemView, systemViewAlias, systemViewOf, systemViewQuery,
} from '@/composables/systemViews'
import { RAFT_CHANNELS, formatRate, inUnit, rateUnit } from '@/composables/useTraffic'
import { stamp } from '@/composables/useStamp'
import { useIdentity } from '@/stores/identity'
import { isMissingRoute, routeSupport } from '@/stores/routeSupport'
import { DEFAULT_TIME_RANGE, SYSTEM_TIME_RANGES as timeRanges, relativeTimeRange } from '../../../shared/timeRanges.mjs'

// CELL-LEVEL PAGE — every source is an operator route (queen_proxy
// is_operator_route): /api/v1/analytics/system-metrics, the bare
// /api/v1/status, /api/v1/raft/status and /api/v1/raft/members. None of them
// is tenant-scopable: host CPU and a replicated log belong to the cell. The
// route already declares requires:'operator'; this guard also stops the calls
// if the operator session stops being live while the page is open.
const { can, actingTenantSlug, actingCellSlug } = useIdentity()
const canOperate = computed(() => can('operator'))
const route = useRoute()
const router = useRouter()

// The view (composables/systemViews.js) and the node picked in the header, by
// its name (the members' hostname, which is also the metrics' replica
// hostname: both are the broker's node label); '' for the whole cell. Both in
// the address, so a pick is a link and Back returns. An old ?view=resources
// is rewritten to the view that holds it now.
const viewRaw = ref('')
const focusNode = ref('')
useRouteState({ view: viewRaw, node: focusNode })
const view = computed(() => systemView(viewRaw.value))
watch(() => route.query.view, (v) => {
  const to = systemViewAlias(v)
  if (to && route.name === 'System') router.replace({ query: { ...route.query, view: to } })
}, { immediate: true })
const viewInfo = computed(() => systemViewOf(view.value))
const currentName = computed(() => viewInfo.value.name)
const viewTo = (key) => ({ path: '/system', query: { ...route.query, view: systemViewQuery(key) } })
const nodeTo = (name) => ({ path: '/system', query: { ...route.query, view: 'compute', node: name } })

// A node picked: one line per replica of it. The whole cell: summed.
const viewMode = ref(focusNode.value ? 'individual' : 'aggregate')
watch(focusNode, (name) => { viewMode.value = name ? 'individual' : 'aggregate' })
const aggregationType = ref('avg')
const aggregationTypes = [
  { value: 'avg', label: 'Average' },
  { value: 'max', label: 'Maximum' },
  { value: 'min', label: 'Minimum' },
]

// ---------------------------------------------------------------------------
// Range, in the address (?range=, or ?from=&to= for a window of your own)
// ---------------------------------------------------------------------------
const timeRange = ref(DEFAULT_TIME_RANGE)
const customMode = ref(false)
const customFrom = ref('')
const customTo = ref('')
const appliedCustom = ref(null)
useRouteRange({ range: timeRange, customMode, customFrom, customTo, appliedCustom, reload: () => refreshResources() })

function currentRange() {
  if (customMode.value && appliedCustom.value) return appliedCustom.value
  return relativeTimeRange(timeRange.value)
}

const rangeOptions = [
  ...timeRanges.map((r) => ({ value: r.value, label: r.longLabel })),
  { value: 'custom', label: 'Custom' },
]
const rangeChoice = computed({
  get: () => (customMode.value ? 'custom' : timeRange.value),
  set: (value) => {
    if (value === 'custom') {
      // The form opens on the range on screen; nothing moves until Apply.
      customMode.value = true
      const { from, to } = relativeTimeRange(timeRange.value)
      customTo.value = formatDateTimeLocal(to)
      customFrom.value = formatDateTimeLocal(from)
      return
    }
    customMode.value = false
    appliedCustom.value = null
    timeRange.value = value
    metrics.refresh()
  },
})

// Live, not on-click: an invalid range explains itself as it is typed instead
// of leaving the user with a button that does nothing when pressed.
const customError = computed(() => validateRange(customFrom.value, customTo.value).error || '')
const customRangeValid = computed(() => !customError.value)
const applyCustomRange = () => {
  const parsed = validateRange(customFrom.value, customTo.value)
  if (parsed.error) return
  appliedCustom.value = { from: parsed.from, to: parsed.to }
  metrics.refresh()
}

// ---------------------------------------------------------------------------
// Fetchers — each panel keeps its own error, so a dead Raft read cannot leave
// the resources tab rendering minutes-old numbers as if they were live.
// ---------------------------------------------------------------------------
const metrics = useApi((config) => {
  const { from, to } = currentRange()
  return operator.getSystemMetrics({ from: from.toISOString(), to: to.toISOString() }, config)
}, { immediate: false })

const status = useApi((config) => operator.getStatus(undefined, config), { immediate: false })

// The raft routes. A broker without them answers 404 and a proxy that does
// not classify them 404 route_blocked — a state to render, not a failure to
// retry — so each is guarded (asked once per cluster epoch) and a probe (no
// toast for that answer).
const raftStatus = useApi(
  routeSupport.guard('raft-status', (config) => operator.getRaftStatus({ ...config, probe: true })),
  { immediate: false },
)
const raftMembers = useApi(
  routeSupport.guard('raft-members', (config) => operator.getRaftMembers({ ...config, probe: true })),
  { immediate: false },
)

function refreshResources() {
  if (!canOperate.value || !viewInfo.value.charts) return
  metrics.refresh()
  if (view.value === 'compute') status.refresh()
}
const fetchData = () => {
  if (!canOperate.value) return
  // The list's numbers and the node picker, on every view.
  raftStatus.refresh()
  raftMembers.refresh()
  refreshResources()
}

useRefresh(fetchData)
watch(canOperate, (live) => { if (live) fetchData() }, { immediate: true })
watch(view, () => refreshResources())

/** "Check again" on the quiet state: forget the verdict for the family, ask once more. */
const recheckRaft = () => {
  routeSupport.forget('raft-members')
  routeSupport.forget('raft-status')
  raftMembers.refresh()
  raftStatus.refresh()
}

// ---------------------------------------------------------------------------
// Panel state
// ---------------------------------------------------------------------------
const systemData = computed(() => metrics.data.value)
const metricsFirstLoad = computed(() => metrics.loading.value && !metrics.data.value)

/** A number we hold, or an em dash — never a 0 standing in for "unknown". */
const metric = (v) => (v === null || v === undefined ? '—' : formatNumber(v))
const bytes = (v) => (v === null || v === undefined ? '—' : formatBytes(v))
const pct = (v) => (v === null || v === undefined ? '—' : `${(v / 100).toFixed(1)}%`)
const mb = (v) => (v === null || v === undefined ? '—' : `${Math.round(v / 1024 / 1024)} MB`)
const msOrDash = (v) => {
  const n = toNum(v)
  return n === null ? '—' : `${Math.round(n)} ms`
}

const formatBucketSize = (minutes) => {
  const n = toNum(minutes)
  if (!n) return '1-minute'
  if (n < 60) return `${n}-minute`
  const hours = Math.floor(n / 60)
  const rest = n % 60
  return rest === 0 ? `${hours}-hour` : `${hours}h ${rest}m`
}
const bucketTitle = computed(() => (systemData.value ? `Each point is a ${formatBucketSize(systemData.value.bucketMinutes)} bucket` : undefined))

// ---------------------------------------------------------------------------
// The list, the header and the tabs: the members read above, through
// brokerStatus (the Overview's and the top bar's model of the same payload).
// ---------------------------------------------------------------------------
const cell = computed(() => brokerStatus(raftMembers.data.value))
const sideNodes = computed(() => nodeEntries(cell.value))
const raftMembersAbsent = computed(() => isMissingRoute(raftMembers.error.value))
const raftStatusAbsent = computed(() => isMissingRoute(raftStatus.error.value))
const nodesState = computed(() => {
  if (raftMembersAbsent.value) return 'absent'
  if (sideNodes.value.length) return 'ok'
  if (raftMembers.data.value) return 'empty'
  return raftMembers.failed.value ? 'failed' : 'loading'
})
const nodesError = computed(() => (raftMembers.failed.value ? describeRaftFailure(raftMembers.error.value) : ''))

// The members table draws the same payload's rows in the same order (node id,
// clusterView), so the picked node's row is the one at its index.
const focusIndex = computed(() => sideNodes.value.find((n) => n.name === focusNode.value)?.index ?? -1)
const focusRow = computed(() => (focusIndex.value < 0 ? null : clusterView(raftMembers.data.value)?.rows[focusIndex.value] || null))

const facts = computed(() => {
  if (focusNode.value) {
    const entry = sideNodes.value[focusIndex.value]
    const row = focusRow.value
    if (!entry) return [{ v: 'Not in the members', title: 'The last members read does not hold this node' }]
    if (!row || row.unreachable) return [{ k: 'Role', v: entry.role }]
    const addresses = [row.httpAddr ? `http ${row.httpAddr}` : null, row.raftAddr ? `raft ${row.raftAddr}` : null].filter(Boolean).join(' · ')
    return [
      { k: 'Role', v: entry.role, title: addresses || undefined },
      row.version ? { k: 'Version', v: row.version } : null,
      row.uptimeSeconds !== null ? { k: 'Up', v: formatDuration(row.uptimeSeconds * 1000) } : null,
    ].filter(Boolean)
  }
  const c = cell.value
  return [
    {
      k: 'Cell',
      v: actingCellSlug.value || 'unknown',
      title: `Host resources and the replicated log of this cell, shared by every tenant on it, not scoped to ${actingTenantSlug.value || 'your tenant'}`,
    },
    c ? { k: 'Nodes', v: String(c.nodes.length) } : null,
    c && !c.raft.single ? { k: 'Leader', v: c.raft.leader || 'none', title: c.raft.term !== null ? `Term ${c.raft.term}` : undefined } : null,
    versionFact.value,
  ].filter(Boolean)
})

// The broker's version, as every node reports it in the members read. One
// version is one chip; several (a rolling upgrade, or one left behind) are
// listed, and the tooltip says which node runs which.
const versionFact = computed(() => {
  const rows = clusterView(raftMembers.data.value)?.rows || []
  const names = sideNodes.value
  const byNode = rows
    .map((r, i) => ({ name: names[i]?.name || r.hostname || `node ${r.nodeId}`, version: r.version }))
    .filter((x) => x.version)
  if (!byNode.length) return null
  const versions = [...new Set(byNode.map((x) => x.version))].sort()
  return {
    k: versions.length === 1 ? 'Version' : 'Versions',
    v: versions.join(' · '),
    title: versions.length === 1
      ? `Every node runs Queen ${versions[0]}`
      : `Nodes run different versions: ${byNode.map((x) => `${x.name} ${x.version}`).join(', ')}`,
  }
})

// ---------------------------------------------------------------------------
// The list, the status card and the tables of the Overview, Nodes and
// Storage: the members read, through brokerStatus and clusterView.
// ---------------------------------------------------------------------------
const nodeOptions = computed(() => [
  { value: '', label: 'All nodes' },
  ...sideNodes.value.map((n) => ({ value: n.name, label: n.name })),
])

// Each cell issue opens the view where it is worked on.
const ISSUE_VIEW = { quorum: 'raft', heartbeat: 'raft', lag: 'raft', snapshot: 'storage', disk: 'disk', memory: 'compute', cpu: 'compute' }
const issueKind = (i) => {
  if (i.kind) return i.kind
  const parts = String(i.key || '').replace(/^cell:/, '').split(':')
  return parts.length === 1 ? parts[0] : parts[1]
}
const issueList = computed(() => cellIssues(cell.value).map((i) => ({ ...i, view: ISSUE_VIEW[issueKind(i)] || null })))
const verdict = computed(() => cellVerdict(cell.value, issueList.value))
const verdictLine = computed(() => {
  const c = cell.value
  if (!c) return ''
  const parts = [`${c.nodes.length} ${c.nodes.length === 1 ? 'node' : 'nodes'}`]
  if (!c.raft.single) parts.push(`leader ${c.raft.leader || 'none'}`)
  if (c.raft.term !== null && c.raft.term !== undefined) parts.push(`term ${formatNumber(c.raft.term)}`)
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
  const nodes = cell.value?.nodes || []
  let sum = null
  for (const n of nodes) {
    const net = n.traffic?.network
    if (!net) continue
    sum = (sum ?? 0) + (toNum(net.received) || 0) + (toNum(net.sent) || 0)
  }
  return sum
})
const statusCells = computed(() => {
  const c = cell.value
  return [
    { k: 'CPU', v: shareText(c?.cpu.top), tone: toneOf(c?.cpu.sev), title: c?.cpu.top ? `The busiest node: ${c.cpu.top.name}` : '' },
    { k: 'Memory', v: shareText(c?.mem.top), tone: toneOf(c?.mem.sev), title: c?.mem.top ? `The fullest node: ${c.mem.top.name}` : '' },
    { k: 'Disk', v: shareText(c?.disk.top), tone: toneOf(c?.disk.sev), title: c?.disk.top ? `The fullest disk: ${c.disk.top.name}; from 99.5% writes stop` : '' },
    { k: 'Network', v: networkNow.value === null ? '—' : formatRate(networkNow.value), title: 'In and out of every node, summed' },
    { k: 'Raft port', v: c?.raft.wire === null || c?.raft.wire === undefined ? '—' : formatRate(c.raft.wire), title: 'Bytes per second on the Raft port, every node' },
  ]
})
const nowRows = computed(() => nodeRows(cell.value))
const nodesNote = computed(() => {
  if (raftMembersAbsent.value) return 'This broker does not report its members'
  if (raftMembers.failed.value) return `Cannot read the members · ${nodesError.value}`
  return 'Reading the members…'
})
// Who each member is, and its log and store: clusterView's rows, by node id.
const memberRows = computed(() => {
  const rows = clusterView(raftMembers.data.value)?.rows || []
  const names = sideNodes.value
  return rows.map((r, i) => ({
    name: names[i]?.name || r.hostname || `node ${r.nodeId}`,
    role: names[i]?.role || (r.unreachable ? 'Unreachable' : capital(r.state || '—')),
    roleTone: r.unreachable ? 'bad' : '',
    version: r.version || '—',
    up: r.uptimeSeconds === null || r.uptimeSeconds === undefined ? '—' : formatDuration(r.uptimeSeconds * 1000),
    http: r.httpAddr || '—',
    raftAddr: r.raftAddr || '—',
    log: bytes(r.logBytes),
    files: metric(r.logFiles),
    map: formatMapPct(r.mapPct),
    mapTone: numTone(r.mapSeverity),
    mapUsed: r.mapUsed === null || r.mapUsed === undefined ? '—' : `${bytes(r.mapUsed)} of ${bytes(r.mapBytes)}`,
  }))
})
// How full each data disk is, for the Disk view's bars.
const diskRows = computed(() => (cell.value?.nodes || [])
  .filter((n) => !focusNode.value || n.name === focusNode.value)
  .map((n) => {
  const pctUsed = toNum(n.disk?.usedPct)
  return {
    name: n.name,
    width: pctUsed === null ? 0 : Math.max(1, Math.min(100, pctUsed)),
    text: pctUsed === null ? '—' : `${Math.round(pctUsed)}%`,
    tone: toneOf(n.disk?.sev),
  }
}))

// The list: every view's one number, coloured only where it is the trouble.
const side = computed(() => {
  const c = cell.value
  const reachable = c ? c.nodes.filter((n) => !n.unreachable).length : 0
  const resourceTone = toneOf(c?.cpu.sev) === 'bad' || toneOf(c?.mem.sev) === 'bad' ? 'bad'
    : toneOf(c?.cpu.sev) || toneOf(c?.mem.sev)
  return systemSideGroups({
    overview: c ? { num: verdict.value.word, tone: verdict.value.tone, title: verdict.value.headline } : {},
    nodes: c ? { num: `${reachable}/${c.nodes.length}`, tone: reachable < c.nodes.length ? 'bad' : '', title: c.raft.leader ? `Leader ${c.raft.leader}` : '' } : {},
    raft: c ? {
      num: c.raft.alert ? c.raft.alert.title : c.raft.lagMax ? `${formatNumber(c.raft.lagMax)} behind` : 'In sync',
      tone: toneOf(c.raft.sev),
      title: `Leader ${c.raft.leader || 'none'} · term ${c.raft.term ?? '—'}`,
    } : {},
    storage: memberRows.value.length ? {
      num: bytes(Math.max(...(clusterView(raftMembers.data.value)?.rows || []).map((r) => toNum(r.logBytes) || 0))),
      tone: toneOf(worstSev((clusterView(raftMembers.data.value)?.rows || []).map((r) => r.mapSeverity))),
      title: 'The largest log on a node',
    } : {},
    compute: c ? { num: `CPU ${shareText(c.cpu.top)}`, tone: resourceTone, title: `Memory ${shareText(c.mem.top)} at most` } : {},
    disk: c ? { num: shareText(c.disk.top), tone: toneOf(c.disk.sev), title: c.disk.top ? `The fullest disk: ${c.disk.top.name}` : '' } : {},
    network: networkNow.value === null ? {} : { num: formatRate(networkNow.value), title: 'In and out of every node, summed' },
  })
})
const worstSev = (sevs) => (sevs.includes('bad') ? 'bad' : sevs.includes('warn') ? 'warn' : '')

// ---------------------------------------------------------------------------
// Replicated log — the node that answered /api/v1/raft/status
// ---------------------------------------------------------------------------
const node = computed(() => nodeView(raftStatus.data.value))
const raftAlarm = computed(() => (node.value?.severity || '') !== '')
const roleMark = computed(() => ({ 'chip-warn': 'warn', 'chip-bad': 'bad' }[node.value?.chip?.cls] || ''))

// ---------------------------------------------------------------------------
// Workers (bare /api/v1/status — workers seen in the last two minutes)
// ---------------------------------------------------------------------------
const workers = computed(() => {
  const all = status.data.value?.workers || []
  return focusNode.value ? all.filter((w) => w.hostname === focusNode.value) : all
})
const lifetimeAckFailed = computed(() => toNum(status.data.value?.errors?.ackFailed))

// The state says what the payload says, not what we hope. Only the event-loop
// gauges are actually written by this broker (min_free_slots / db_connections
// / max_job_queue_size never are), so nothing else is rendered here. A mark
// only when it needs you.
const workerChip = (w) => {
  const peak = toNum(w.maxEventLoopLagMs)
  const avg = toNum(w.avgEventLoopLagMs)
  if (peak === null && avg === null) return { mark: '', label: 'No lag data' }
  if ((peak ?? 0) > 500 || (avg ?? 0) > 200) return { mark: 'bad', label: 'Stalling' }
  if ((peak ?? 0) > 100 || (avg ?? 0) > 50) return { mark: 'warn', label: 'Busy' }
  return { mark: '', label: 'Responsive' }
}

// ---------------------------------------------------------------------------
// Replica time series
//
// Replicas do not share a bucket grid: one may start later, restart, or miss a
// collector tick. Labels therefore come from the UNION of every replica's
// timestamps and each series is indexed BY timestamp, so a shorter series is a
// gap in the line rather than a silent shift onto another replica's clock.
// ---------------------------------------------------------------------------
// The picked node's replicas only, or every one: what the charts, the tiles,
// the servers table and the workers are drawn from.
const replicas = computed(() => nodeReplicas(systemData.value?.replicas, focusNode.value || null))
const replicaCountLabel = computed(() => {
  const n = replicas.value.length
  return `${n} replica${n === 1 ? '' : 's'}`
})
const acrossLabel = computed(() =>
  replicas.value.length > 1 ? `summed across ${replicaCountLabel.value}` : focusNode.value || 'this replica',
)
const viewTitle = computed(() => (viewMode.value === 'aggregate'
  ? `Summed across ${replicaCountLabel.value}; a bucket where a replica sent no sample sums only those that did`
  : 'One line per server; a gap is a bucket that server never reported'))

const timeline = computed(() => {
  const seen = new Set()
  for (const r of replicas.value) {
    for (const point of r.timeSeries || []) {
      if (point?.timestamp) seen.add(point.timestamp)
    }
  }
  return [...seen].sort()
})

const chartLabels = computed(() => {
  const multiDay = isMultiDay(timeline.value)
  return timeline.value.map((ts) => formatChartLabel(new Date(ts), multiDay))
})

const pointsByTimestamp = (replica) => {
  const map = new Map()
  for (const point of replica.timeSeries || []) {
    if (point?.timestamp) map.set(point.timestamp, point)
  }
  return map
}

const leaf = (metricsObj, path) => {
  let at = metricsObj
  for (const key of path) {
    at = at?.[key]
    if (at === undefined || at === null) return null
  }
  return at
}

/** One replica's values aligned to the union timeline; null where it has none. */
const replicaSeries = (replica, path, scale = (v) => v) => {
  const byTs = pointsByTimestamp(replica)
  return timeline.value.map((ts) => {
    const v = toNum(leaf(byTs.get(ts)?.metrics, path)?.[aggregationType.value])
    return v === null ? null : scale(v)
  })
}

/** Sum across replicas per bucket; null only when no replica reported it. */
const summedSeries = (path, scale = (v) => v) => {
  const perReplica = replicas.value.map((r) => replicaSeries(r, path, scale))
  return timeline.value.map((_, i) => {
    let sum = null
    for (const series of perReplica) {
      const v = series[i]
      if (v !== null) sum = (sum || 0) + v
    }
    return sum
  })
}

const CPU_SCALE = (v) => v / 100
const MB_SCALE = (v) => Math.round(v / 1024 / 1024)

const buildChart = (specs, { perReplicaPath = null, perReplicaScale = (v) => v, perReplicaSuffix = '' } = {}) => {
  if (!replicas.value.length || !timeline.value.length) return { labels: [], datasets: [] }
  if (viewMode.value === 'individual' && perReplicaPath) {
    return {
      labels: chartLabels.value,
      datasets: replicas.value.map((replica, i) => ({
        label: `${replica.hostname}${perReplicaSuffix}`,
        data: replicaSeries(replica, perReplicaPath, perReplicaScale),
        borderColor: chartColor(i).line,
        fill: false,
        tension: 0,
      })),
    }
  }
  return {
    labels: chartLabels.value,
    datasets: specs.map((spec, i) => ({
      label: spec.label,
      data: summedSeries(spec.path, spec.scale),
      borderColor: chartColor(i).line,
      backgroundColor: chartColor(i).fill,
      fill: true,
      tension: 0,
    })),
  }
}

const cpuChart = computed(() => buildChart(
  [
    { label: 'User CPU (%)', path: ['cpu', 'user_us'], scale: CPU_SCALE },
    { label: 'System CPU (%)', path: ['cpu', 'system_us'], scale: CPU_SCALE },
  ],
  { perReplicaPath: ['cpu', 'user_us'], perReplicaScale: CPU_SCALE, perReplicaSuffix: ' · user' },
))

const memoryChart = computed(() => buildChart(
  [{ label: 'RSS (MB)', path: ['memory', 'rss_bytes'], scale: MB_SCALE }],
  { perReplicaPath: ['memory', 'rss_bytes'], perReplicaScale: MB_SCALE },
))

// ---------------------------------------------------------------------------
// What the nodes move, in bytes per second (useTraffic.js). Each chart takes
// the unit its largest value reads best in, and names it in every label.
// ---------------------------------------------------------------------------
const largest = (datasets) => {
  let max = 0
  for (const d of datasets) for (const v of d.data) if (v !== null && v > max) max = v
  return max
}

/**
 * Aggregate: one filled line per spec, summed across replicas. Per server:
 * every spec of every replica in the replica's colour, the second dashed.
 */
const buildRateChart = (specs) => {
  const empty = { labels: [], datasets: [], unit: rateUnit(0), hasData: false }
  if (!replicas.value.length || !timeline.value.length) return empty
  const raw = viewMode.value === 'individual'
    ? replicas.value.flatMap((replica, i) => specs.map((spec, j) => ({
      label: `${replica.hostname} · ${spec.short}`,
      data: replicaSeries(replica, spec.path),
      borderColor: chartColor(i).line,
      borderDash: j === 0 ? [] : [5, 4],
      fill: false,
    })))
    : specs.map((spec, i) => ({
      label: spec.label,
      data: summedSeries(spec.path),
      borderColor: chartColor(spec.color ?? i).line,
      backgroundColor: chartColor(spec.color ?? i).fill,
      fill: true,
    }))
  if (!raw.some((d) => d.data.some((v) => v !== null))) return empty
  const unit = rateUnit(largest(raw))
  return {
    labels: chartLabels.value,
    unit,
    hasData: true,
    datasets: raw.map((d) => ({
      ...d,
      label: `${d.label} (${unit.label})`,
      tension: 0,
      data: d.data.map((v) => inUnit(v, unit)),
    })),
  }
}

const networkChart = computed(() => buildRateChart([
  { label: 'Received', short: 'in', path: ['network', 'received_bytes_per_sec'] },
  { label: 'Sent', short: 'out', path: ['network', 'sent_bytes_per_sec'] },
]))

const diskChart = computed(() => buildRateChart([
  { label: 'Read', short: 'read', path: ['disk', 'read_bytes_per_sec'] },
  { label: 'Written', short: 'write', path: ['disk', 'written_bytes_per_sec'] },
]))

// Aggregate: what crossed the wire, by channel. A byte is sent by one node and
// received by another, so the nodes' SENT bytes, summed, count it once. A
// channel that carried nothing in the range stays off the chart; replication
// always shows. Per server: each node's total out and in.
const raftTrafficChart = computed(() => {
  if (viewMode.value === 'individual') {
    return buildRateChart([
      { label: 'Sent', short: 'out', path: ['raft_traffic', 'sent_bytes_per_sec'] },
      { label: 'Received', short: 'in', path: ['raft_traffic', 'received_bytes_per_sec'] },
    ])
  }
  return buildRateChart(RAFT_CHANNELS
    .map((c, i) => ({ key: c.key, label: c.label, color: i, path: ['raft_traffic', c.key, 'sent_bytes_per_sec'] }))
    .filter((s) => s.key === 'replication' || summedSeries(s.path).some((v) => v !== null && v > 0)))
})

const raftTrafficHelp = [
  'Bytes over the Raft RPC port, by what they carry:',
  ...RAFT_CHANNELS.map((c) => `${c.label}: ${c.note}.`),
  'Aggregate sums what each node sent: every byte on the wire once.',
].join('\n')

// A new options object only when a chart's unit changes: BaseChart redraws
// the whole chart for new options, and only re-plots for new data.
const rateOptions = (unitLabel) => ({
  plugins: { legend: { display: true, position: 'top' } },
  scales: { y: { title: { display: true, text: unitLabel, font: { size: 11 } } } },
})
const networkUnit = computed(() => networkChart.value.unit.label)
const diskUnit = computed(() => diskChart.value.unit.label)
const raftTrafficUnit = computed(() => raftTrafficChart.value.unit.label)
const networkOptions = computed(() => rateOptions(networkUnit.value))
const diskOptions = computed(() => rateOptions(diskUnit.value))
const raftTrafficOptions = computed(() => rateOptions(raftTrafficUnit.value))

// ---------------------------------------------------------------------------
// Latest sample. Reduced ACROSS replicas — a card that printed replicas[0]
// beside a replica count would read as a cell figure and not be one.
// ---------------------------------------------------------------------------
const lastOf = (replica, path) => {
  const series = replica?.timeSeries || []
  for (let i = series.length - 1; i >= 0; i--) {
    const v = toNum(leaf(series[i]?.metrics, path)?.last)
    if (v !== null) return v
  }
  return null
}

const cpuOf = (replica, key) => lastOf(replica, ['cpu', key])

const sumLatest = (path) => {
  let sum = null
  for (const replica of replicas.value) {
    const v = lastOf(replica, path)
    if (v !== null) sum = (sum || 0) + v
  }
  return sum
}

const latest = computed(() => ({
  cpuUser: sumLatest(['cpu', 'user_us']),
  rss: sumLatest(['memory', 'rss_bytes']),
  netIn: sumLatest(['network', 'received_bytes_per_sec']),
  netOut: sumLatest(['network', 'sent_bytes_per_sec']),
  diskRead: sumLatest(['disk', 'read_bytes_per_sec']),
  diskWrite: sumLatest(['disk', 'written_bytes_per_sec']),
  // Every node's sent bytes: what crossed the wire, once.
  raftWire: sumLatest(['raft_traffic', 'sent_bytes_per_sec']),
}))

const tiles = computed(() => {
  const l = latest.value
  const across = acrossLabel.value
  const unreported = 'Not reported: a broker records it on Linux only, and older releases not at all'
  return [
    { k: 'CPU', v: pct(l.cpuUser), title: `User CPU in the last sample, ${across}` },
    { k: 'Memory', v: mb(l.rss), title: `Resident memory in the last sample, ${across}` },
    { k: 'Network in', v: formatRate(l.netIn), title: l.netIn === null ? unreported : `Received in the last sample, ${across}` },
    { k: 'Network out', v: formatRate(l.netOut), title: l.netOut === null ? unreported : `Sent in the last sample, ${across}` },
    { k: 'Disk write', v: formatRate(l.diskWrite), title: l.diskWrite === null ? 'Not reported by this release' : `Written in the last sample, ${across}; ${formatRate(l.diskRead)} read` },
    {
      k: 'Raft port',
      v: formatRate(l.raftWire),
      title: l.raftWire === null ? 'Not reported by this release'
        : focusNode.value ? `What ${focusNode.value} sent on the Raft port in the last sample`
          : 'On the Raft port’s wire in the last sample: every node’s sent bytes, summed',
    },
  ]
})

// Chart options
const cpuOptions = {
  plugins: { legend: { display: true, position: 'top' } },
  scales: {
    y: {
      title: { display: true, text: 'CPU %', font: { size: 11 } },
      ticks: { callback: (value) => `${Number(value).toFixed(1)}%` },
    },
  },
}
const memoryOptions = {
  plugins: { legend: { display: true, position: 'top' } },
  scales: { y: { title: { display: true, text: 'Memory (MB)', font: { size: 11 } } } },
}

const capital = (label) => (label ? label.charAt(0).toUpperCase() + label.slice(1) : label)
</script>

<style scoped>
/* The list: the number is the mark, coral when failing, amber when it needs
   you; the Overview's word is a word, not a number. */
.sys-side { padding-top: 12px; }
.sys-side .ss-num.bad { color: var(--ember-400); }
.sys-side .ss-num.warn { color: var(--warn-400); }
.ss-num.sys-side-word { font-family: inherit; font-size: 12px; }

/* The Overview's status card (the Overview page's Summary card, for the
   cell): the verdict, five number cells beside it, hairlines between. */
.sys-status { display: grid; grid-template-columns: minmax(260px, 1.5fr) repeat(5, minmax(0, 1fr)); overflow: hidden; }
.sys-verdict { min-width: 0; padding: 15px 18px; border-right: 1px solid var(--bd); }
.sys-verdict-word { display: inline-flex; align-items: center; gap: 7px; margin-bottom: 6px; font-size: 12px; font-weight: 600; color: var(--text-mid); }
.sys-verdict-word.bad { color: var(--ember-400); }
.sys-verdict-word.warn { color: var(--warn-400); }
.sys-verdict-title { margin: 0; font-size: 16px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.3; color: var(--text-hi); text-wrap: balance; }
.sys-verdict-line { margin: 5px 0 0; font-size: 12px; color: var(--text-low); }
.sys-kpi { min-width: 0; padding: 15px 16px; border-right: 1px solid var(--bd); display: flex; flex-direction: column; justify-content: center; gap: 5px; }
.sys-kpi:last-child { border-right: 0; }
.sys-kpi-k { font-size: 11.5px; color: var(--text-low); white-space: nowrap; }
.sys-kpi-v { font-size: 18px; font-weight: 600; letter-spacing: -0.01em; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.sys-kpi-v.bad { color: var(--ember-400); }
.sys-kpi-v.warn { color: var(--warn-400); }
@media (max-width: 1100px) {
  .sys-status { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .sys-verdict { grid-column: 1 / -1; border-right: 0; border-bottom: 1px solid var(--bd); }
  .sys-kpi { border-bottom: 1px solid var(--bd); }
  .sys-kpi:nth-child(4) { border-right: 0; }
  .sys-kpi:nth-child(n+5) { border-bottom: 0; }
}

/* What needs you in the cell: each row opens the view it is worked on in. */
.sys-issues { list-style: none; margin: 0; padding: 0; }
.sys-issues li + li { border-top: 1px solid var(--bd-soft); }
.sys-issue { display: flex; align-items: center; gap: 12px; padding: 11px 16px; color: inherit; text-decoration: none; }
.sys-issue:hover { background: var(--ink-3); }
.sys-issue-text { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
.sys-issue-title { font-size: 13px; font-weight: 500; color: var(--text-hi); }
.sys-issue-why { font-size: 12px; color: var(--text-mid); }
.sys-issue-go { color: var(--text-low); flex: none; }
.sys-node-link { color: var(--text-hi); text-decoration: none; }
.sys-node-link:hover { text-decoration: underline; text-underline-offset: 3px; }
.sys-mono { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); }

/* How full each data disk is: one bar per node, the number after it. */
.sys-bars { display: flex; flex-direction: column; gap: 10px; padding: 14px 16px 16px; }
.sys-bar { display: grid; grid-template-columns: 110px minmax(0, 1fr) 48px; align-items: center; gap: 12px; font-size: 12.5px; }
.sys-bar-name { color: var(--text-mid); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.sys-bar-track { height: 6px; border-radius: 3px; background: var(--ink-4); overflow: hidden; }
.sys-bar-fill { display: block; height: 100%; border-radius: 3px; background: var(--text-low); }
.sys-bar-fill.warn { background: var(--warn-400); }
.sys-bar-fill.bad { background: var(--ember-400); }
.sys-bar-v { text-align: right; font-variant-numeric: tabular-nums; color: var(--text-hi); }
.sys-bar-v.warn { color: var(--warn-400); }
.sys-bar-v.bad { color: var(--ember-400); }

/* The tab's blocks, a column apart at the 16px rhythm. */
.sys-stack { display: flex; flex-direction: column; gap: 16px; }
/* The panel pair. Not a stat grid: it lays out CARDS. */
.sys-grid-2 { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 16px; }
.sys-range { display: inline-flex; }

/* One step louder than a hairline on purpose — the replicated log is
   actually in trouble here. */
.sys-alarm { border-color: color-mix(in srgb, var(--warn-400) 35%, transparent); }

.sys-quiet { padding: 14px 16px; font-size: 13px; color: var(--text-low); }
.sys-table td { white-space: nowrap; }
.sys-host { font-weight: 500; color: var(--text-hi); }
.sys-port { color: var(--text-low); }
/* "12.4 MB/s / 3.1 MB/s": one cell, two figures, the slash quiet. */
.sys-sep { color: var(--text-low); }
.sys-state { display: inline-flex; align-items: center; gap: 8px; color: var(--text-hi); }
/* The mark's slot, kept when empty so the states line up (the lists' rule). */
.sys-mark { flex: none; width: 10px; display: inline-flex; justify-content: center; }

@media (max-width: 1100px) {
  .sys-grid-2 { grid-template-columns: minmax(0, 1fr); }
}
@media (max-width: 640px) {
  .sys-tiles .count-tile .v { font-size: 17px; }
}
</style>
