<template>
  <div class="view-container">

    <!-- CELL-LEVEL PAGE. Every source below is an operator route the proxy
         answers 200 for only when /auth/me says operator_live; the numbers
         cover the whole cell, every tenant on it. Say that on screen — a cell
         figure read as the acting tenant's is the exact lie this page could
         tell. -->
    <div v-if="!canOperate" class="card">
      <div class="card-body">
        <div class="panel-err">
          This page is cell-level and only a live operator may open it. Nothing
          here is scoped to <strong>{{ actingTenantSlug || 'your tenant' }}</strong>.
        </div>
      </div>
    </div>

    <template v-else>
      <!-- SCOPE STRIP. Cell variant: same container as every other view's
           strip, amber because the scope differs — not because anything is
           wrong. Built from useIdentity(), so it renders while loading, on a
           failed fetch and on an empty page. -->
      <div class="scope-strip scope-strip-cell">
        <span class="chip chip-warn"><span class="dot"></span>cell · operator</span>
        <span class="scope-text">
          host resources, the disk spool and Postgres internals for
          <strong>cell {{ actingCellSlug || 'unknown' }}</strong>
          <span class="scope-sep">·</span>
          shared by every tenant on it, not scoped to {{ actingTenantSlug || 'your tenant' }}
        </span>
      </div>

      <!-- PAGE BANNERS. The Source switch decides which fetch the whole page is
           made of, so either failure is a fact about the page. A single panel's
           failure stays inside that panel as .panel-err. -->
      <div v-if="dataSource === 'system' && metrics.failed.value" class="status-banner banner-bad view-banner">
        <span><strong>Could not load server metrics</strong> · {{ describeApiError(metrics.error.value) }}<template v-if="systemData"> · showing the last samples that loaded{{ metrics.lastUpdated.value ? ` (as of ${metrics.lastUpdated.value.toLocaleTimeString()})` : '' }}</template></span>
      </div>
      <div v-if="dataSource === 'postgres' && pg.failed.value" class="status-banner banner-bad view-banner">
        <span><strong>Could not load Postgres stats</strong> · {{ describeApiError(pg.error.value) }}<template v-if="postgresData"> · showing the last stats that loaded{{ pg.lastUpdated.value ? ` (as of ${pg.lastUpdated.value.toLocaleTimeString()})` : '' }}</template></span>
      </div>

      <!-- =================== FILTERS =================== -->
      <div class="card filters">
        <div class="card-body filter-rows">

          <div class="filter-row">
            <div v-if="dataSource === 'system'" class="filter-field">
              <span class="label-xs">Range</span>
              <div class="seg">
                <button
                  v-for="range in timeRanges"
                  :key="range.value"
                  :class="{ on: timeRange === range.value && !customMode }"
                  @click="selectQuickRange(range.value)"
                >{{ range.label }}</button>
                <button :class="{ on: customMode }" @click="toggleCustomMode">Custom</button>
              </div>
            </div>

            <div class="filter-field">
              <span class="label-xs">Source</span>
              <div class="seg">
                <button :class="{ on: dataSource === 'system' }" @click="selectSource('system')">Server resources</button>
                <button :class="{ on: dataSource === 'postgres' }" @click="selectSource('postgres')">Postgres stats</button>
              </div>
            </div>
          </div>

          <div v-if="dataSource === 'system'" class="filter-row">
            <div class="filter-field">
              <span class="label-xs">View</span>
              <div class="seg">
                <button :class="{ on: viewMode === 'individual' }" @click="viewMode = 'individual'">Per server</button>
                <button :class="{ on: viewMode === 'aggregate' }" @click="viewMode = 'aggregate'">Aggregate</button>
              </div>
            </div>
            <div class="filter-field">
              <span class="label-xs">Metric</span>
              <div class="seg">
                <button
                  v-for="agg in aggregationTypes"
                  :key="agg.value"
                  :class="{ on: aggregationType === agg.value }"
                  @click="aggregationType = agg.value"
                >{{ agg.label }}</button>
              </div>
            </div>
            <span class="filter-hint">
              {{ viewMode === 'aggregate'
                ? `summed across ${replicaCountLabel}; a bucket where a replica sent no sample sums only those that did`
                : 'one line per broker replica; gaps are buckets that replica never reported' }}
            </span>
          </div>

          <div v-if="dataSource === 'system' && customMode" class="filter-row filter-row-sep">
            <div class="filter-field">
              <span class="label-xs">From</span>
              <input v-model="customFrom" type="datetime-local" class="input" />
            </div>
            <div class="filter-field">
              <span class="label-xs">To</span>
              <input v-model="customTo" type="datetime-local" class="input" />
            </div>
            <button class="btn btn-primary" :disabled="!customRangeValid" @click="applyCustomRange">Apply</button>
            <span v-if="customError" class="filter-invalid">{{ customError }}</span>
          </div>
        </div>
      </div>

      <!-- =================== FILE BUFFER (disk spool) ===================
           The page's summary block: the one snapshot that holds whichever
           source is selected. -->
      <div class="card" :class="{ 'card-alarm': spoolAlarm }" style="margin-bottom:16px;">
        <div class="card-header">
          <h3>File buffer</h3>
          <span class="chip chip-mute">cell-level</span>
          <span class="muted">{{ stamp(buffers) }}</span>
        </div>
        <div class="card-body">
          <div v-if="buffers.failed.value" class="panel-err">
            Spool state unavailable — {{ describeApiError(buffers.error.value) }}.
            Pending and failed counts below are unknown, not zero.
          </div>
          <div class="stat-grid stat-grid-3">
            <div class="stat">
              <div class="stat-label">Database</div>
              <div class="stat-value">
                <span v-if="dbHealthy === null" class="font-mono">—</span>
                <span v-else-if="dbHealthy" class="chip chip-ok"><span class="dot"></span>reachable</span>
                <span v-else class="chip chip-bad"><span class="dot"></span>unreachable</span>
              </div>
              <div class="stat-foot">
                {{ dbHealthy === false
                  ? 'the broker is spooling pushes to disk'
                  : 'pushes go straight to Postgres' }}
              </div>
            </div>
            <div class="stat">
              <div class="stat-label">Pending on disk</div>
              <div class="stat-value font-mono num" :class="{ warn: (bufferPending || 0) > 0 }">
                {{ metric(bufferPending) }}
              </div>
              <div class="stat-foot">messages waiting to drain into Postgres</div>
            </div>
            <div class="stat">
              <div class="stat-label">Failed on disk</div>
              <div class="stat-value font-mono num" :class="{ bad: (bufferFailed || 0) > 0 }">
                {{ metric(bufferFailed) }}
              </div>
              <div class="stat-foot">spool files that could not be replayed</div>
            </div>
          </div>
        </div>
      </div>

      <!-- =================== SERVER RESOURCES =================== -->
      <template v-if="dataSource === 'system'">
        <div v-if="metricsFirstLoad" class="sys-grid-2">
          <div v-for="i in 4" :key="i" class="card">
            <div class="card-body"><div class="skeleton" style="height:192px;" /></div>
          </div>
        </div>

        <template v-else>
          <template v-if="systemData">
            <div class="sys-grid-2" style="margin-bottom:16px;">
              <div class="card">
                <div class="card-header">
                  <h3>CPU usage</h3>
                  <span class="card-sub">{{ replicaCountLabel }}</span>
                  <span class="chip chip-mute">cell-level</span>
                  <span class="muted">{{ stamp(metrics) }}</span>
                </div>
                <div class="card-body">
                  <BaseChart
                    v-if="cpuChart.labels.length"
                    type="line" :data="cpuChart" :options="cpuOptions" height="240px"
                  />
                  <div v-else class="panel-msg">No CPU samples in this range.</div>
                </div>
              </div>

              <div class="card">
                <div class="card-header">
                  <h3>Memory usage</h3>
                  <span class="chip chip-mute">cell-level</span>
                  <span class="muted">{{ stamp(metrics) }}</span>
                </div>
                <div class="card-body">
                  <BaseChart
                    v-if="memoryChart.labels.length"
                    type="line" :data="memoryChart" :options="memoryOptions" height="240px"
                  />
                  <div v-else class="panel-msg">No memory samples in this range.</div>
                </div>
              </div>
            </div>

            <div class="sys-grid-2" style="margin-bottom:16px;">
              <div class="card">
                <div class="card-header">
                  <h3>Database pool</h3>
                  <span class="chip chip-mute">cell-level</span>
                  <span class="muted">{{ stamp(metrics) }}</span>
                </div>
                <div class="card-body">
                  <BaseChart
                    v-if="databaseChart.labels.length"
                    type="line" :data="databaseChart" :options="poolOptions" height="200px"
                  />
                  <div v-else class="panel-msg">No pool samples in this range.</div>
                </div>
              </div>

              <div class="card">
                <div class="card-header">
                  <h3>Broker workers</h3>
                  <span class="chip chip-mute">cell-level</span>
                  <span class="muted">{{ stamp(status) }}</span>
                </div>
                <div class="card-body">
                  <div v-if="status.failed.value" class="panel-err">
                    {{ describeApiError(status.error.value) }}
                  </div>
                  <div v-else-if="!workers.length" class="panel-msg">
                    No worker reported in the last two minutes.
                  </div>
                  <div v-else class="sys-workers">
                    <div v-for="w in workers" :key="`${w.hostname}:${w.workerId}`" class="sys-worker">
                      <span class="sys-worker-host font-mono">{{ w.hostname }}</span>
                      <span class="chip" :class="workerChip(w).cls">
                        <span class="dot"></span>{{ workerChip(w).label }}
                      </span>
                      <span class="sys-worker-meta font-mono">
                        loop {{ msOrDash(w.avgEventLoopLagMs) }} avg · {{ msOrDash(w.maxEventLoopLagMs) }} peak
                      </span>
                      <span class="sys-worker-meta font-mono">
                        {{ metric(toNum(w.messagesProcessed)) }} msg / 2 min
                      </span>
                    </div>
                    <p class="sys-note">
                      DB errors since broker start (cell-wide):
                      <span class="font-mono" :class="{ 'color-ember': (lifetimeDbErrors || 0) > 0 }">
                        {{ metric(lifetimeDbErrors) }}
                      </span>
                      · ack failures
                      <span class="font-mono">{{ metric(lifetimeAckFailed) }}</span>
                    </p>
                  </div>
                </div>
              </div>
            </div>

            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Cell summary</h3>
                <span class="chip chip-mute">cell-level</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <div class="stat-grid stat-grid-6">
                  <div class="stat">
                    <div class="stat-label">Replicas</div>
                    <div class="stat-value font-mono">{{ metric(toNum(systemData.replicaCount)) }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Data points</div>
                    <div class="stat-value font-mono">{{ metric(toNum(systemData.pointCount)) }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Bucket size</div>
                    <div class="stat-value font-mono">{{ formatBucketSize(systemData.bucketMinutes) }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">CPU</div>
                    <div class="stat-value font-mono">{{ pct(latest.cpuUser) }}</div>
                    <div class="stat-foot">{{ acrossLabel }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Memory</div>
                    <div class="stat-value font-mono">{{ mb(latest.rss) }}</div>
                    <div class="stat-foot">{{ acrossLabel }}</div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">DB active</div>
                    <div class="stat-value font-mono">{{ metric(latest.dbActive) }}</div>
                    <div class="stat-foot">{{ acrossLabel }}</div>
                  </div>
                </div>
              </div>
            </div>

            <div v-if="replicas.length" class="card">
              <div class="card-header">
                <h3>Server details</h3>
                <span class="card-sub">last sample per replica</span>
                <span class="chip chip-mute">cell-level</span>
                <span class="muted">{{ stamp(metrics) }}</span>
              </div>
              <div class="card-body">
                <div class="sys-scroll">
                  <table class="t">
                    <thead>
                      <tr>
                        <th>Hostname</th>
                        <th class="right">Port</th>
                        <th class="right">CPU (user)</th>
                        <th class="right">CPU (sys)</th>
                        <th class="right">Memory</th>
                        <th class="right">DB pool</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="replica in replicas" :key="`${replica.hostname}:${replica.port}`">
                        <td style="font-weight:500;">{{ replica.hostname }}</td>
                        <td class="right font-mono tabular-nums">{{ replica.port }}</td>
                        <td class="right font-mono tabular-nums">{{ pct(cpuOf(replica, 'user_us')) }}</td>
                        <td class="right font-mono tabular-nums">{{ pct(cpuOf(replica, 'system_us')) }}</td>
                        <td class="right font-mono tabular-nums">{{ mb(lastOf(replica, ['memory', 'rss_bytes'])) }}</td>
                        <td class="right font-mono tabular-nums">
                          {{ metric(lastOf(replica, ['database', 'pool_active'])) }}/{{ metric(lastOf(replica, ['database', 'pool_size'])) }}
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </template>
        </template>
      </template>

      <!-- =================== POSTGRES =================== -->
      <template v-else>
        <div v-if="pgFirstLoad" class="sys-grid-2">
          <div v-for="i in 4" :key="i" class="card">
            <div class="card-body"><div class="skeleton" style="height:192px;" /></div>
          </div>
        </div>

        <template v-else>
          <template v-if="postgresData">
            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Cache performance</h3>
                <span class="card-sub">{{ postgresData.database }}</span>
                <span class="chip chip-mute">cell-level</span>
                <span class="muted">{{ stamp(pg) }}</span>
              </div>
              <div class="card-body">
                <div class="stat-grid stat-grid-4">
                  <div class="stat">
                    <div class="stat-label">Database hit ratio</div>
                    <div class="stat-value font-mono" :class="cacheClass(postgresData.databaseCache?.cacheHitRatio)">
                      {{ ratio(postgresData.databaseCache?.cacheHitRatio) }}
                    </div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Table hit ratio</div>
                    <div class="stat-value font-mono" :class="cacheClass(postgresData.cacheSummary?.tables?.hitRatio)">
                      {{ ratio(postgresData.cacheSummary?.tables?.hitRatio) }}
                    </div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Index hit ratio</div>
                    <div class="stat-value font-mono" :class="cacheClass(postgresData.cacheSummary?.indexes?.hitRatio)">
                      {{ ratio(postgresData.cacheSummary?.indexes?.hitRatio) }}
                    </div>
                  </div>
                  <div class="stat">
                    <div class="stat-label">Shared buffers</div>
                    <div class="stat-value font-mono">
                      {{ postgresData.bufferConfig?.sharedBuffersSize || '—' }}
                    </div>
                  </div>
                </div>
                <p class="sys-note">A ratio is “—” when the object has seen no I/O yet — unknown, not zero.</p>
              </div>
            </div>

            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Table cache stats</h3>
                <span class="card-sub">hit ratios per table in the queen schema</span>
                <span class="muted">{{ stamp(pg) }}</span>
              </div>
              <div class="card-body">
                <div class="sys-scroll">
                  <table class="t">
                    <thead>
                      <tr>
                        <th>Table</th>
                        <th class="right">Disk reads</th>
                        <th class="right">Cache hits</th>
                        <th class="right">Hit ratio</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="table in postgresData.tableCache" :key="table.table">
                        <td class="font-mono" style="font-weight:500;">{{ table.table }}</td>
                        <td class="right font-mono tabular-nums muted-cell">{{ metric(toNum(table.diskReads)) }}</td>
                        <td class="right font-mono tabular-nums muted-cell">{{ metric(toNum(table.cacheHits)) }}</td>
                        <td class="right">
                          <span class="font-mono tabular-nums" :class="cacheClass(table.cacheHitRatio)">
                            {{ ratio(table.cacheHitRatio) }}
                          </span>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Index cache stats</h3>
                <span class="card-sub">top 20 by disk reads</span>
                <span class="muted">{{ stamp(pg) }}</span>
              </div>
              <div class="card-body">
                <div class="sys-scroll">
                  <table class="t">
                    <thead>
                      <tr>
                        <th>Index</th>
                        <th>Table</th>
                        <th class="right">Disk reads</th>
                        <th class="right">Cache hits</th>
                        <th class="right">Hit ratio</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="idx in postgresData.indexCache" :key="idx.index">
                        <td class="font-mono" style="font-size:12px;">{{ idx.index }}</td>
                        <td class="muted-cell">{{ idx.table }}</td>
                        <td class="right font-mono tabular-nums muted-cell">{{ metric(toNum(idx.diskReads)) }}</td>
                        <td class="right font-mono tabular-nums muted-cell">{{ metric(toNum(idx.cacheHits)) }}</td>
                        <td class="right">
                          <span class="font-mono tabular-nums" :class="cacheClass(idx.cacheHitRatio)">
                            {{ ratio(idx.cacheHitRatio) }}
                          </span>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div v-if="postgresData.bufferUsage?.length" class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Buffer cache contents</h3>
                <span class="card-sub">what is cached in shared_buffers</span>
                <span class="muted">{{ stamp(pg) }}</span>
              </div>
              <div class="card-body">
                <div class="sys-scroll">
                  <table class="t">
                    <thead>
                      <tr>
                        <th>Object</th>
                        <th class="right">Buffered size</th>
                        <th class="right">% of cache</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="buf in postgresData.bufferUsage" :key="buf.object">
                        <td class="font-mono" style="font-weight:500;">{{ buf.object }}</td>
                        <td class="right font-mono tabular-nums muted-cell">{{ buf.bufferedSize }}</td>
                        <td class="right">
                          <div class="sys-barcell">
                            <div class="bar" style="width:64px;">
                              <i :style="{ width: `${Math.min(toNum(buf.percentOfCache) || 0, 100)}%` }" />
                            </div>
                            <span class="font-mono tabular-nums sys-barpct">{{ ratio(buf.percentOfCache) }}</span>
                          </div>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Table sizes</h3>
                <span class="card-sub">storage usage per table</span>
                <span class="muted">{{ stamp(pg) }}</span>
              </div>
              <div class="card-body">
                <div class="sys-scroll">
                  <table class="t">
                    <thead>
                      <tr>
                        <th>Table</th>
                        <th class="right">Total size</th>
                        <th class="right">Table size</th>
                        <th class="right">Index size</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="tbl in postgresData.tableSizes" :key="tbl.table">
                        <td class="font-mono" style="font-weight:500;">{{ tbl.table }}</td>
                        <td class="right font-mono tabular-nums" style="font-weight:500;">{{ tbl.totalSize }}</td>
                        <td class="right font-mono tabular-nums muted-cell">{{ tbl.tableSize }}</td>
                        <td class="right font-mono tabular-nums">{{ tbl.indexSize }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div class="sys-grid-2" style="margin-bottom:16px;">
              <div class="card">
                <div class="card-header">
                  <h3>Dead tuples</h3>
                  <span class="card-sub">tables needing vacuum</span>
                  <span v-if="postgresData.deadTuples?.length" class="chip chip-warn">
                    {{ postgresData.deadTuples.length }} tables
                  </span>
                  <span class="muted">{{ stamp(pg) }}</span>
                </div>
                <div class="card-body">
                  <div v-if="postgresData.deadTuples?.length" class="sys-scroll">
                    <table class="t">
                      <thead>
                        <tr>
                          <th>Table</th>
                          <th class="right">Dead</th>
                          <th class="right">Dead %</th>
                          <th>Last vacuum</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr v-for="tbl in postgresData.deadTuples" :key="tbl.table">
                          <td class="font-mono" style="font-size:12px;">{{ tbl.table }}</td>
                          <td class="right font-mono tabular-nums">{{ metric(toNum(tbl.deadTuples)) }}</td>
                          <td class="right">
                            <span class="font-mono tabular-nums" :class="deadClass(tbl.deadPercentage)">
                              {{ ratio(tbl.deadPercentage) }}
                            </span>
                          </td>
                          <td class="sys-age">{{ ageOf(tbl.lastAutovacuum || tbl.lastVacuum) }}</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                  <div v-else class="panel-msg panel-msg-ok">
                    No dead tuples — tables are clean
                  </div>
                </div>
              </div>

              <div class="card">
                <div class="card-header">
                  <h3>HOT update efficiency</h3>
                  <span class="card-sub">higher is better</span>
                  <span class="muted">{{ stamp(pg) }}</span>
                </div>
                <div class="card-body">
                  <div v-if="postgresData.hotUpdates?.length" class="sys-scroll">
                    <table class="t">
                      <thead>
                        <tr>
                          <th>Table</th>
                          <th class="right">Updates</th>
                          <th class="right">HOT %</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr v-for="tbl in postgresData.hotUpdates" :key="tbl.table">
                          <td class="font-mono" style="font-size:12px;">{{ tbl.table }}</td>
                          <td class="right font-mono tabular-nums muted-cell">{{ metric(toNum(tbl.totalUpdates)) }}</td>
                          <td class="right">
                            <span class="font-mono tabular-nums" :class="hotClass(tbl.hotUpdatePercentage)">
                              {{ ratio(tbl.hotUpdatePercentage) }}
                            </span>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                  <div v-else class="panel-msg">No updates recorded yet</div>
                </div>
              </div>
            </div>

            <div class="card" style="margin-bottom:16px;">
              <div class="card-header">
                <h3>Active queries</h3>
                <span class="card-sub">queries running longer than 1s</span>
                <span v-if="postgresData.activeQueries?.length" class="chip chip-bad">
                  {{ postgresData.activeQueries.length }} slow
                </span>
                <span class="muted">{{ stamp(pg) }}</span>
              </div>
              <div class="card-body">
                <p class="sys-note">
                  Every database on this Postgres instance, not only Queen's — a query here
                  may belong to another application sharing the server.
                </p>
                <div v-if="postgresData.activeQueries?.length" class="sys-queries">
                  <div v-for="query in postgresData.activeQueries" :key="query.pid" class="card sys-query">
                    <div class="sys-query-head">
                      <span class="sys-query-pid">PID {{ query.pid }} · {{ query.state }}</span>
                      <span class="font-mono tabular-nums" :class="durationClass(query.duration)">
                        {{ formatDurationSeconds(query.duration) }}
                      </span>
                    </div>
                    <code class="font-mono sys-query-sql">{{ query.query }}</code>
                    <div v-if="query.waitEventType" class="sys-query-wait">
                      Wait: {{ query.waitEventType }} / {{ query.waitEvent }}
                    </div>
                  </div>
                </div>
                <div v-else class="panel-msg panel-msg-ok">No slow queries running</div>
              </div>
            </div>

            <div v-if="postgresData.autovacuumStatus?.length" class="card">
              <div class="card-header">
                <h3>Autovacuum status</h3>
                <span class="chip chip-warn">{{ postgresData.autovacuumStatus.length }} pending</span>
                <span class="muted">{{ stamp(pg) }}</span>
              </div>
              <div class="card-body">
                <div class="sys-scroll">
                  <table class="t">
                    <thead>
                      <tr>
                        <th>Table</th>
                        <th class="right">Dead tuples</th>
                        <th class="right">Vacuum count</th>
                        <th>Last autovacuum</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="tbl in postgresData.autovacuumStatus" :key="tbl.table">
                        <td class="font-mono" style="font-weight:500;">{{ tbl.table }}</td>
                        <td class="right font-mono tabular-nums">{{ metric(toNum(tbl.deadTuples)) }}</td>
                        <td class="right font-mono tabular-nums muted-cell">{{ tbl.autovacuumCount }}</td>
                        <td class="sys-age">{{ ageOf(tbl.lastAutovacuum) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </template>
        </template>
      </template>
    </template>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue'

import BaseChart from '@/components/BaseChart.vue'
import { describeApiError, operator } from '@/api'
import { formatNumber, formatRelativeTime, toNum, useApi } from '@/composables/useApi'
import { chartColor } from '@/composables/useChartTheme'
import { formatChartLabel, formatDateTimeLocal, isMultiDay, validateRange } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import { useIdentity } from '@/stores/identity'

// CELL-LEVEL PAGE — every source is an operator route (queen_proxy
// is_operator_route): /api/v1/analytics/system-metrics, /api/v1/status/buffers,
// /api/v1/analytics/postgres-stats and the bare /api/v1/status. None of them is
// tenant-scopable: host CPU, a disk spool and pg_buffercache belong to the cell.
// The route already declares requires:'operator'; this guard also stops the
// calls if the operator session stops being live while the page is open.
const { can, actingTenantSlug, actingCellSlug } = useIdentity()
const canOperate = computed(() => can('operator'))

const dataSource = ref('system')
const viewMode = ref('aggregate')
const aggregationType = ref('avg')
const timeRange = ref(60)
const customMode = ref(false)
const customFrom = ref('')
const customTo = ref('')
const appliedCustom = ref(null)

const timeRanges = [
  { label: '15m', value: 15 },
  { label: '1h', value: 60 },
  { label: '6h', value: 360 },
  { label: '24h', value: 1440 },
]

const aggregationTypes = [
  { label: 'Average', value: 'avg' },
  { label: 'Maximum', value: 'max' },
  { label: 'Minimum', value: 'min' },
]

// ---------------------------------------------------------------------------
// Range
// ---------------------------------------------------------------------------
function currentRange() {
  if (customMode.value && appliedCustom.value) return appliedCustom.value
  const to = new Date()
  const from = new Date(to.getTime() - timeRange.value * 60_000)
  return { from, to }
}

// Live, not on-click: an invalid range explains itself as it is typed instead
// of leaving the user with a button that does nothing when pressed.
const customError = computed(() => validateRange(customFrom.value, customTo.value).error || '')
const customRangeValid = computed(() => !customError.value)

const selectQuickRange = (value) => {
  customMode.value = false
  timeRange.value = value
  metrics.refresh()
}

const toggleCustomMode = () => {
  customMode.value = !customMode.value
  if (customMode.value) {
    const now = new Date()
    customTo.value = formatDateTimeLocal(now)
    customFrom.value = formatDateTimeLocal(new Date(now.getTime() - timeRange.value * 60_000))
  } else {
    appliedCustom.value = null
    metrics.refresh()
  }
}

const applyCustomRange = () => {
  const parsed = validateRange(customFrom.value, customTo.value)
  if (parsed.error) return
  appliedCustom.value = { from: parsed.from, to: parsed.to }
  metrics.refresh()
}

// ---------------------------------------------------------------------------
// Fetchers — each panel keeps its own error, so a dead Postgres tab cannot
// leave the resources tab rendering minutes-old numbers as if they were live.
// ---------------------------------------------------------------------------
const metrics = useApi((config) => {
  const { from, to } = currentRange()
  return operator.getSystemMetrics({ from: from.toISOString(), to: to.toISOString() }, config)
}, { immediate: false })

const buffers = useApi((config) => operator.getBuffers(undefined, config), { immediate: false })
const status = useApi((config) => operator.getStatus(undefined, config), { immediate: false })
const pg = useApi((config) => operator.getPostgresStats(config), { immediate: false })

const fetchData = () => {
  if (!canOperate.value) return
  buffers.refresh()
  if (dataSource.value === 'postgres') {
    pg.refresh()
  } else {
    metrics.refresh()
    status.refresh()
  }
}

const selectSource = (src) => {
  dataSource.value = src
  fetchData()
}

useRefresh(fetchData)
watch(canOperate, (live) => { if (live) fetchData() }, { immediate: true })

// ---------------------------------------------------------------------------
// Panel state
// ---------------------------------------------------------------------------
const systemData = computed(() => metrics.data.value)
const postgresData = computed(() => pg.data.value)
const metricsFirstLoad = computed(() => metrics.loading.value && !metrics.data.value)
const pgFirstLoad = computed(() => pg.loading.value && !pg.data.value)

/** A number we hold, or an em dash — never a 0 standing in for "unknown". */
const metric = (v) => (v === null || v === undefined ? '—' : formatNumber(v))
const pct = (v) => (v === null || v === undefined ? '—' : `${(v / 100).toFixed(1)}%`)
const mb = (v) => (v === null || v === undefined ? '—' : `${Math.round(v / 1024 / 1024)} MB`)
const ratio = (v) => (v === null || v === undefined ? '—' : `${v}%`)
const msOrDash = (v) => {
  const n = toNum(v)
  return n === null ? '—' : `${Math.round(n)}ms`
}
const ageOf = (ts) => (ts ? formatRelativeTime(ts) : 'Never')

const formatBucketSize = (minutes) => {
  const n = toNum(minutes)
  if (!n) return '1 min'
  if (n < 60) return `${n} min`
  const hours = Math.floor(n / 60)
  const rest = n % 60
  return rest === 0 ? `${hours}h` : `${hours}h ${rest}m`
}

const formatDurationSeconds = (seconds) => {
  const s = toNum(seconds)
  if (s === null) return '—'
  if (s < 60) return `${s.toFixed(1)}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
}

// ---------------------------------------------------------------------------
// File buffer (disk spool)
// ---------------------------------------------------------------------------
const bufferPending = computed(() => toNum(buffers.data.value?.pending))
const bufferFailed = computed(() => toNum(buffers.data.value?.failed))
const dbHealthy = computed(() => {
  const v = buffers.data.value?.dbHealthy
  return v === undefined || v === null ? null : v === true
})
const spoolAlarm = computed(
  () => dbHealthy.value === false || (bufferFailed.value || 0) > 0 || (bufferPending.value || 0) > 0,
)

// ---------------------------------------------------------------------------
// Brokers (bare /api/v1/status — workers seen in the last two minutes)
// ---------------------------------------------------------------------------
const workers = computed(() => status.data.value?.workers || [])
const lifetimeDbErrors = computed(() => toNum(status.data.value?.errors?.dbErrors))
const lifetimeAckFailed = computed(() => toNum(status.data.value?.errors?.ackFailed))

// The chip states what the payload says, not what we hope. Only the event-loop
// gauges are actually written by this broker (min_free_slots / db_connections /
// max_job_queue_size never are), so nothing else is rendered here.
const workerChip = (w) => {
  const peak = toNum(w.maxEventLoopLagMs)
  const avg = toNum(w.avgEventLoopLagMs)
  if (peak === null && avg === null) return { cls: 'chip-mute', label: 'no lag data' }
  if ((peak ?? 0) > 500 || (avg ?? 0) > 200) return { cls: 'chip-bad', label: 'event loop stalling' }
  if ((peak ?? 0) > 100 || (avg ?? 0) > 50) return { cls: 'chip-warn', label: 'event loop busy' }
  return { cls: 'chip-ok', label: 'responsive' }
}

// ---------------------------------------------------------------------------
// Replica time series
//
// Replicas do not share a bucket grid: one may start later, restart, or miss a
// collector tick. Labels therefore come from the UNION of every replica's
// timestamps and each series is indexed BY timestamp, so a shorter series is a
// gap in the line rather than a silent shift onto another replica's clock.
// ---------------------------------------------------------------------------
const replicas = computed(() => systemData.value?.replicas || [])
const replicaCountLabel = computed(() => {
  const n = replicas.value.length
  return `${n} replica${n === 1 ? '' : 's'}`
})
const acrossLabel = computed(() =>
  replicas.value.length > 1 ? `summed across ${replicaCountLabel.value}` : 'this replica',
)

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
  return timeline.value.map(ts => formatChartLabel(new Date(ts), multiDay))
})

const pointsByTimestamp = (replica) => {
  const map = new Map()
  for (const point of replica.timeSeries || []) {
    if (point?.timestamp) map.set(point.timestamp, point)
  }
  return map
}

const leaf = (metricsObj, path) => {
  let node = metricsObj
  for (const key of path) {
    node = node?.[key]
    if (node === undefined || node === null) return null
  }
  return node
}

/** One replica's values aligned to the union timeline; null where it has none. */
const replicaSeries = (replica, path, scale = v => v) => {
  const byTs = pointsByTimestamp(replica)
  return timeline.value.map(ts => {
    const node = leaf(byTs.get(ts)?.metrics, path)
    const v = toNum(node?.[aggregationType.value])
    return v === null ? null : scale(v)
  })
}

/** Sum across replicas per bucket; null only when no replica reported it. */
const summedSeries = (path, scale = v => v) => {
  const perReplica = replicas.value.map(r => replicaSeries(r, path, scale))
  return timeline.value.map((_, i) => {
    let sum = null
    for (const series of perReplica) {
      const v = series[i]
      if (v !== null) sum = (sum || 0) + v
    }
    return sum
  })
}

const CPU_SCALE = v => v / 100
const MB_SCALE = v => Math.round(v / 1024 / 1024)

const buildChart = (specs, { perReplicaPath = null, perReplicaScale = v => v, perReplicaSuffix = '' } = {}) => {
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

const databaseChart = computed(() => buildChart(
  [
    { label: 'Active', path: ['database', 'pool_active'] },
    { label: 'Idle', path: ['database', 'pool_idle'] },
  ],
  { perReplicaPath: ['database', 'pool_active'], perReplicaSuffix: ' · active' },
))

// ---------------------------------------------------------------------------
// Latest sample. Reduced ACROSS replicas — the old card printed replicas[0]
// beside a "Replicas: N" counter, which reads as a cell figure and is not one.
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
  dbActive: sumLatest(['database', 'pool_active']),
}))

// ---------------------------------------------------------------------------
// Postgres classes. A null ratio has no colour — `getCacheRatioClass(0)` would
// paint an idle table red at a "0%" it never reported.
// ---------------------------------------------------------------------------
const cacheClass = (r) => {
  const v = toNum(r)
  if (v === null) return ''
  if (v >= 99) return 'color-ok'
  if (v >= 95) return 'color-ice'
  if (v >= 90) return 'color-crown'
  return 'color-ember'
}

const hotClass = (r) => {
  const v = toNum(r)
  if (v === null) return ''
  if (v >= 95) return 'color-ok'
  if (v >= 80) return 'color-ice'
  if (v >= 50) return 'color-crown'
  return 'color-ember'
}

const deadClass = (r) => {
  const v = toNum(r)
  if (v === null) return ''
  return v > 10 ? 'color-ember' : 'muted-cell'
}

const durationClass = (seconds) => ((toNum(seconds) || 0) > 10 ? 'color-ember' : 'color-crown')

// Chart options
const cpuOptions = {
  plugins: { legend: { display: true, position: 'top', labels: { usePointStyle: true, padding: 14 } } },
  scales: {
    y: {
      title: { display: true, text: 'CPU %', font: { size: 11 } },
      ticks: { callback: (value) => `${Number(value).toFixed(1)}%` },
    },
  },
}
const memoryOptions = {
  plugins: { legend: { display: true, position: 'top', labels: { usePointStyle: true, padding: 14 } } },
  scales: { y: { title: { display: true, text: 'Memory (MB)', font: { size: 11 } } } },
}
const poolOptions = {
  plugins: { legend: { display: true, position: 'top', labels: { usePointStyle: true, padding: 14 } } },
  scales: { y: { title: { display: true, text: 'Connections', font: { size: 11 } } } },
}
</script>

<style scoped>
/* Everything shared with the other nine views now lives in style.css:
   .scope-strip*, the .filter-* card family, .card-sub, .stat-grid*, .view-banner,
   .panel-err and .panel-msg*. What is left below is System's own layout. */

/* One step louder than the scope strip on purpose — the spool is actually in
   trouble here, so it keeps its own alpha rather than collapsing to --warn-bd. */
.card-alarm { border-color: color-mix(in srgb, var(--warn-400) 35%, transparent); }

.sys-note { margin-top: 10px; font-size: 11.5px; color: var(--text-low); }
.sys-scroll { overflow-x: auto; }
.sys-age { font-size: 12px; color: var(--text-low); }
.muted-cell { color: var(--text-mid); }
.right { text-align: right; }

/* The panel pair. Not a stat grid: it lays out CARDS, at the 16px block
   rhythm, so it keeps its own rule (as Analytics' .an-grid-2 does). */
.sys-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }

.sys-workers { display: flex; flex-direction: column; gap: 8px; }
.sys-worker {
  display: flex; align-items: center; flex-wrap: wrap; gap: 10px;
  padding: 8px 10px; border: 1px solid var(--bd); border-radius: var(--r-card);
}
.sys-worker-host { font-size: 12px; color: var(--text-hi); font-weight: 500; }
.sys-worker-meta { font-size: 11px; color: var(--text-mid); }

.sys-barcell { display: flex; align-items: center; justify-content: flex-end; gap: 8px; }
.sys-barpct { font-size: 12px; color: var(--text-mid); width: 52px; text-align: right; }

.sys-queries { display: flex; flex-direction: column; gap: 12px; }
.sys-query { padding: 10px 12px; }
.sys-query-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
.sys-query-pid { font-size: 12px; font-weight: 500; color: var(--text-mid); }
.sys-query-sql { font-size: 12px; color: var(--text-hi); display: block; word-break: break-all; }
.sys-query-wait { margin-top: 8px; font-size: 12px; color: var(--text-low); }

@media (max-width: 1100px) {
  .sys-grid-2 { grid-template-columns: 1fr; }
}
</style>
