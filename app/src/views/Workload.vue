<template>
  <div class="view-container">

    <!-- Scope strip. Every number below is this tenant's; the cell it runs on
         is named too, so a tenant figure is never read as a cell figure. -->
    <div class="scope-strip">
      <span class="chip chip-mute">tenant scope</span>
      <span class="scope-text">
        <strong>{{ actingTenantSlug || 'no tenant' }}</strong>
        <span class="scope-sep">/</span>{{ actingClusterSlug || 'no cluster' }}
        <span class="scope-sep">·</span>cell {{ actingCellSlug || 'unknown' }}
      </span>
      <span class="scope-fill"></span>
      <!-- The fallback is a DIFFERENT provenance, so it is labelled: these
           numbers were rolled up in this browser, not by the broker. -->
      <span v-if="fallbackMode" class="chip chip-mute" :title="FALLBACK_TITLE">computed client-side</span>
      <span class="scope-meta" :title="rangeUtcTitle">{{ rangeLabel }}</span>
      <span class="scope-meta">{{ stamp(workload) }}</span>
    </div>

    <!-- Filters -->
    <div class="card filters">
      <div class="card-body filter-rows">
        <div class="filter-row">
          <div class="filter-field">
            <span class="label-xs">Range</span>
            <div class="seg">
              <button
                v-for="r in timeRanges"
                :key="r.value"
                :class="{ on: selectedRange === r.value && !customMode }"
                @click="selectQuickRange(r.value)"
              >{{ r.label }}</button>
              <button :class="{ on: customMode }" @click="toggleCustomMode">Custom</button>
            </div>
          </div>

          <div class="filter-field">
            <span class="label-xs">Group by</span>
            <div class="seg">
              <button :class="{ on: groupBy === 'namespace' }" @click="selectGroupBy('namespace')">namespace</button>
              <button :class="{ on: groupBy === 'task' }" @click="selectGroupBy('task')">task</button>
            </div>
          </div>

          <div class="filter-field">
            <span class="label-xs">{{ groupBy }}</span>
            <select class="input" :value="focus === null ? '*' : focus" @change="pickFocus($event.target.value)">
              <option value="*">{{ rootCrumb }}</option>
              <option v-for="o in focusOptions" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
          </div>

          <div class="filter-field">
            <span class="label-xs">Compare</span>
            <div class="seg">
              <button
                v-for="c in compareModes"
                :key="c.value"
                :class="{ on: compare === c.value }"
                @click="selectCompare(c.value)"
              >{{ c.label }}</button>
            </div>
          </div>

          <span class="filter-hint">{{ focusHint }}</span>
        </div>

        <div v-if="customMode" class="filter-row filter-row-sep">
          <div class="filter-field">
            <span class="label-xs">From</span>
            <input v-model="customFrom" type="datetime-local" class="input" :title="formatTimestampUtc(customFrom)" />
          </div>
          <div class="filter-field">
            <span class="label-xs">To</span>
            <input v-model="customTo" type="datetime-local" class="input" :title="formatTimestampUtc(customTo)" />
          </div>
          <span v-if="customError" class="filter-hint">{{ customError }}</span>
          <button class="btn btn-primary" :disabled="!customRangeValid" @click="applyCustomRange">Apply</button>
        </div>

        <!-- Focus breadcrumb. Drilling in is a REFETCH with groupBy=queue and
             the namespace/task as a filter, not a client-side slice. -->
        <div class="filter-row filter-row-sep wl-crumbs">
          <button class="crumb" :disabled="focus === null" @click="goRoot">{{ rootCrumb }}</button>
          <template v-if="focus !== null">
            <span class="crumb-sep">›</span>
            <button class="crumb" :disabled="!selectedQueue" @click="selectedQueue = null">{{ focusName }}</button>
          </template>
          <template v-if="selectedQueue">
            <span class="crumb-sep">›</span>
            <button class="crumb" disabled>{{ selectedQueue }}</button>
          </template>
        </div>
      </div>
    </div>

    <!-- Counts. The tenant totals, whatever the rows below are filtered to. -->
    <!-- Counts strip: the Dashboard idiom. Point-in-time totals on the left, the
         window sums on the right; the range picker bounds only the right group. -->
    <div class="counts-strip">
        <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
        <div v-else-if="!tenant" class="empty">Loading…</div>
        <template v-else>
          <div class="counts-group">
            <span class="count-item-label">now</span>
            <span class="count-item count-static"><strong>{{ fmt.n(tenant.queues) }}</strong><span>queues</span></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong>{{ fmt.n(tenant.now.groups) }}</strong><span>consumer groups</span></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong>{{ fmt.n(tenant.now.partitions) }}</strong><span>partitions</span></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong :class="pendingTone">{{ fmt.n(tenant.now.pending) }}</strong><span>pending</span></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong>{{ fmt.n(tenant.now.processing) }}</strong><span>in flight</span></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><!-- Depth, not growth: nothing purges a dead-letter queue, so `> 0` is
                 permanent and says nothing about this window. -->
            <strong>{{ fmt.n(tenant.now.deadLetter) }}</strong><span>in DLQ</span></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong>{{ fmt.bytes(tenant.now.retainedBytes) }}</strong><span>retained</span></span>
          </div>
          <div class="counts-group">
            <span class="count-item-label">{{ rangeLabel }}<template v-if="deltas"> vs {{ compareLabel }}</template></span>
            <span class="count-item count-static"><strong>{{ fmt.n(tenant.window.pushMessages) }}</strong><span>pushed</span><em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.pushMessages.pct) }}</em></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong>{{ fmt.n(tenant.window.popMessages) }}</strong><span>delivered</span><em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.popMessages.pct) }}</em></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong>{{ fmt.n(tenant.window.ackSuccess) }}</strong><span>acked</span><em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.ackSuccess.pct) }}</em></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong :class="ackFailedTone">{{ fmt.n(tenant.window.ackFailed) }}</strong><span>ack failures</span><em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.ackFailed.pct) }}</em></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong>{{ fmt.n(tenant.window.popEmpty) }}</strong><span>empty polls</span><em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.popEmpty.pct) }}</em></span>
            <span class="count-sep">·</span>
            <span class="count-item count-static"><strong>{{ fmt.pct(tenantFill) }}</strong><span>fill</span></span>
          </div>
        </template>
    </div>

    <!-- Work over time -->
    <div class="card">
      <div class="card-header">
        <h3>{{ `Work over time by ${level === 'queue' ? 'queue' : groupBy}` }}</h3>
        <span class="card-sub">stacked, {{ bucketMinutes }} min buckets</span>
        <div class="seg seg-sm">
          <button
            v-for="m in metrics"
            :key="m.value"
            :class="{ on: metric === m.value }"
            @click="metric = m.value"
          >{{ m.label }}</button>
        </div>
        <span class="chip chip-mute">{{ rangeLabel }}</span>
      </div>
      <div class="card-body">
        <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
        <div v-else class="wl-chart" style="height: 280px">
          <canvas :ref="(el) => setCanvas('flow', el)" />
        </div>
      </div>
    </div>

    <!-- Share + activity map -->
    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>{{ `Share of the work by ${level === 'queue' ? 'queue' : groupBy}` }}</h3>
          <span class="card-sub">delivered and pushed, share is of the tenant</span>
          <span class="chip chip-mute">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length * (prevPayload ? 1.5 : 1.15)) }">
            <canvas :ref="(el) => setCanvas('share', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>{{ `Activity map by ${level === 'queue' ? 'queue' : groupBy}` }}</h3>
          <span class="card-sub">deliveries per bucket · darkest = {{ fmt.n(heat.max) }} / {{ bucketMinutes }} min</span>
          <span class="chip chip-mute">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <Heatmap
            v-else
            :rows="heatRows"
            :buckets="buckets"
            :multi-day="multiDay"
            @select="drillByKey"
          />
        </div>
      </div>
    </div>

    <!-- Footprint: what each group IS rather than what it did — partitions,
         queues and consumer groups, retained bytes, partition churn. -->
    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>{{ `Partitions by ${levelWord}` }}</h3>
          <span class="card-sub">live partitions · label = partitions per queue</span>
          <span class="chip chip-mute">now</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('partsBy', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>{{ `Queues and consumer groups by ${levelWord}` }}</h3>
          <span class="card-sub">queues in full colour, the groups attached to them faded</span>
          <span class="chip chip-mute">now</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length * 1.15) }">
            <canvas :ref="(el) => setCanvas('structure', el)" />
          </div>
        </div>
      </div>
    </div>

    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>{{ `Retained bytes by ${levelWord}` }}</h3>
          <span class="card-sub">what is on disk for each</span>
          <span class="chip chip-mute">now</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('retained', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>{{ `Partition churn by ${levelWord}` }}</h3>
          <span class="card-sub">partitions created, then deleted, in the window</span>
          <span class="chip chip-mute">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length * 1.15) }">
            <canvas :ref="(el) => setCanvas('churn', el)" />
          </div>
        </div>
      </div>
    </div>

    <!-- Backlog and efficiency rows: two charts per row so queue names keep
         their width (the label budget follows the card width, see barBase). -->
    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>Pending</h3>
          <span class="card-sub">red = no consumer group</span>
          <span class="chip chip-mute">now</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('pending', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Oldest waiting</h3>
          <span class="card-sub">worst consumer-group time lag</span>
          <span class="chip chip-mute">now</span>
        </div>
        <div class="card-body">
          <!-- This panel is the consumer-groups list, not the workload
               endpoint: with that list down there is no honest bar to draw. -->
          <div v-if="groupsFailed" class="panel-err">{{ groupsErrorText }}</div>
          <div v-else-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('oldest', el)" />
          </div>
        </div>
      </div>

    </div>

    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>In DLQ</h3>
          <span class="card-sub">rows parked in dead-letter</span>
          <span class="chip chip-mute">now</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('dlq', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Fill</h3>
          <span class="card-sub">deliveries per poll · blank under 5 polls</span>
          <span class="chip chip-mute">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('fill', el)" />
          </div>
        </div>
      </div>

    </div>

    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>Ack ok</h3>
          <span class="card-sub">blank under 5 acks</span>
          <span class="chip chip-mute">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('ack', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Lag at pop</h3>
          <span class="card-sub">average → worst, log scale</span>
          <span class="chip chip-mute">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
          <div v-else-if="!rows.length" class="empty">No queue in this scope.</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('lag', el)" />
          </div>
        </div>
      </div>
    </div>

    <!-- Consumer groups of the clicked queue -->
    <div v-if="selectedQueue" class="card">
      <div class="card-header">
        <h3>Consumer groups of {{ selectedQueue }}</h3>
        <span class="card-sub">{{ groupsSub }}</span>
        <span class="chip chip-mute">now</span>
      </div>
      <div class="card-body">
        <div v-if="groupsFailed" class="panel-err">{{ groupsErrorText }}</div>
        <div v-else-if="!queueGroups.length" class="empty">No consumer group has ever attached to this queue.</div>
        <div v-else class="wl-chart" :style="{ height: chartHeight(queueGroups.length) }">
          <canvas :ref="(el) => setCanvas('groups', el)" />
        </div>
      </div>
    </div>


    <!-- ===================================================================
         DEEPER. The layer under the panels above: not "who is busy" but
         "is this normal", "what does one push cost", "why are these rows in
         the DLQ", "where are the bytes". Its fetches are their own — see the
         comment on `fetchDeeper` for why they do NOT ride the refresh tick.
         =================================================================== -->
    <div class="wl-eyebrow">
      <span class="eyebrow-text">Deeper</span>
      <span class="eyebrow-rule"></span>
      <span class="eyebrow-sub">a 7 day profile and a 24 hour queue-level read · loaded on open and on a range or group-by change, not on every refresh</span>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Weekly profile</h3>
        <span class="card-sub">{{ weekSub }}</span>
        <span class="chip chip-mute">last 7d</span>
      </div>
      <div class="card-body">
        <div v-if="deepWeek.failed.value" :class="deeperErrorClass(deepWeek)">{{ deeperErrorText(deepWeek) }}</div>
        <div v-else-if="!week" class="empty">Loading…</div>
        <template v-else>
          <div class="wl-pills">
            <button
              v-for="p in weekPills"
              :key="p.value"
              class="pill"
              :class="{ on: weekPick === p.value }"
              @click="weekPick = p.value"
            >{{ p.label }}</button>
          </div>
          <Heatmap :rows="weekRows" :buckets="HOURS" />
          <p class="wl-note">{{ weekStats }}</p>
        </template>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Is this hour normal</h3>
        <span class="card-sub">{{ baselineSub }}</span>
      </div>
      <div class="card-body">
        <div v-if="deepWeek.failed.value" :class="deeperErrorClass(deepWeek)">{{ deeperErrorText(deepWeek) }}</div>
        <div v-else-if="!baseline.rows.length" class="empty">
          {{ deepWeek.loading.value ? 'Loading…' : 'No deliveries in the last 7 days to compare against.' }}
        </div>
        <div v-else class="wl-chart" :style="{ height: chartHeight(baseline.rows.length) }">
          <canvas :ref="(el) => setCanvas('base', el)" />
        </div>
      </div>
    </div>

    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>Deliveries per pushed message</h3>
          <span class="card-sub">top 12 queues by deliveries</span>
          <span class="chip chip-mute">last 24h</span>
        </div>
        <div class="card-body">
          <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
          <div v-else-if="!topQueues.length" class="empty">{{ deepEff.loading.value ? 'Loading…' : 'No deliveries in the last 24h.' }}</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(topQueues.length) }">
            <canvas :ref="(el) => setCanvas('fan', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Acks per delivery</h3>
          <span class="card-sub">acks inside /transaction are not attributed</span>
          <span class="chip chip-mute">last 24h</span>
        </div>
        <div class="card-body">
          <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
          <div v-else-if="!topQueues.length" class="empty">{{ deepEff.loading.value ? 'Loading…' : 'No deliveries in the last 24h.' }}</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(topQueues.length) }">
            <canvas :ref="(el) => setCanvas('ackd', el)" />
          </div>
        </div>
      </div>

    </div>

    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>Messages per request</h3>
          <span class="card-sub">push and ack batch sizes</span>
          <span class="chip chip-mute">last 24h</span>
        </div>
        <div class="card-body">
          <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
          <div v-else-if="!topQueues.length" class="empty">{{ deepEff.loading.value ? 'Loading…' : 'No deliveries in the last 24h.' }}</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(topQueues.length * 1.15) }">
            <canvas :ref="(el) => setCanvas('batch', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Lag budget</h3>
          <span class="card-sub">share of the queue's active buckets, by worst lag at pop</span>
          <span class="chip chip-mute">last 24h</span>
        </div>
        <div class="card-body">
          <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
          <div v-else-if="!budgetRows.length" class="empty">
            {{ deepEff.loading.value ? 'Loading…' : 'No lag sampled in the last 24h.' }}
          </div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(budgetRows.length) }">
            <canvas :ref="(el) => setCanvas('budget', el)" />
          </div>
        </div>
      </div>

    </div>

    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>Retention and eviction</h3>
          <span class="card-sub">per queue · evicted = older than the max wait, never delivered</span>
          <span class="chip chip-mute">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="retention.failed.value" :class="deeperErrorClass(retention)">{{ deeperErrorText(retention) }}</div>
          <div v-else-if="retentionUngrouped" class="empty">
            Not available on this broker: it answers retention totals without the per-queue split.
          </div>
          <div v-else-if="!retentionRows.length" class="empty">
            {{ retention.loading.value ? 'Loading…' : 'Nothing was retained or evicted in this window.' }}
          </div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(retentionRows.length) }">
            <canvas :ref="(el) => setCanvas('ret', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Partitions alive</h3>
          <span class="card-sub">total vs written to in the last 24h</span>
          <span class="chip chip-mute">now</span>
        </div>
        <div class="card-body">
          <div v-if="partitions.failed.value" :class="deeperErrorClass(partitions)">{{ deeperErrorText(partitions) }}</div>
          <div v-else-if="!partitionRows.length" class="empty">{{ partitions.loading.value ? 'Loading…' : 'No partition reported.' }}</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(partitionRows.length * 1.15) }">
            <canvas :ref="(el) => setCanvas('parts', el)" />
          </div>
        </div>
      </div>
    </div>

    <!-- Folded error messages are long: this card takes the full row. -->
    <div class="card">
      <div class="card-header">
        <h3>Why messages are in the DLQ</h3>
        <span class="card-sub">{{ dlqSub }}</span>
        <span class="chip chip-mute">now</span>
      </div>
      <div class="card-body">
        <div v-if="!dlqPills.length" class="empty">No queue has a dead-letter row right now.</div>
        <template v-else>
          <div class="wl-pills">
            <button
              v-for="p in dlqPills"
              :key="p"
              class="pill"
              :class="{ on: dlqPick === p }"
              @click="pickDlq(p)"
            >{{ shortQueue(p) }}</button>
          </div>
          <div v-if="dlqSignatures.failed.value" :class="deeperErrorClass(dlqSignatures)">{{ deeperErrorText(dlqSignatures) }}</div>
          <div v-else-if="!dlqSigRows.length" class="empty">
            {{ dlqSignatures.loading.value ? 'Loading…' : 'No error message in the sample.' }}
          </div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(dlqSigRows.length) }">
            <canvas :ref="(el) => setCanvas('dlqsig', el)" />
          </div>
        </template>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Where the bytes are</h3>
        <span class="card-sub">retained bytes per message · top 10 by retained</span>
        <span class="chip chip-mute">now</span>
      </div>
      <div class="card-body">
        <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
        <div v-else-if="!byteRows.length" class="empty">
          {{ deepEff.loading.value ? 'Loading…' : 'No queue reports both retained bytes and a message count.' }}
        </div>
        <div v-else class="wl-chart" :style="{ height: chartHeight(byteRows.length) }">
          <canvas :ref="(el) => setCanvas('bytes', el)" />
        </div>
      </div>
    </div>

    <!-- ===================================================================
         FINDINGS, LAST. The panels are the page; these two lists only say in
         words what they already show, so they read after the evidence rather
         than before it. Same rules, same drill-down — moved, not changed.
         =================================================================== -->
    <div class="wl-eyebrow">
      <span class="eyebrow-text">Findings</span>
      <span class="eyebrow-rule"></span>
      <span class="eyebrow-sub">what the panels above add up to · a row in the first list drills into it</span>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>What needs attention</h3>
        <span class="chip chip-mute">{{ rangeLabel }}</span>
      </div>
      <div class="card-body">
        <div v-if="workloadFailed" class="panel-err">{{ workloadErrorText }}</div>
        <template v-else>
          <p class="wl-summary">{{ summary }}</p>
          <div v-if="groupsFailed" class="panel-err">
            Consumer groups unavailable — {{ groupsErrorText }} The waiting and no-group rules are suspended.
          </div>
          <ul class="wl-findings">
            <li v-for="(f, i) in attention" :key="i">
              <button class="finding" :class="`sev-${f.sev}`" @click="drill(f.row)">
                <span class="fdot"></span>
                <span class="ftext">{{ f.text }}</span>
                <span class="fev">{{ f.evidence }}</span>
              </button>
            </li>
            <li v-if="!attention.length" class="empty">Nothing needs attention in this window.</li>
          </ul>
        </template>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>What the deeper layer found</h3>
        <span class="card-sub">rules that need the second read</span>
      </div>
      <div class="card-body">
        <ul class="wl-findings">
          <li v-for="(f, i) in deepAttention" :key="i">
            <div class="finding finding-stack" :class="`sev-${f.sev}`" style="cursor: default">
              <span class="fdot"></span>
              <span class="ftext">{{ f.text }}</span>
              <span class="fev">{{ f.evidence }}</span>
            </div>
          </li>
          <li v-if="!deepAttention.length" class="empty">{{ deepEff.loading.value ? 'Loading…' : 'Nothing to add from the deeper reads.' }}</li>
        </ul>
      </div>
    </div>

  </div>
</template>

<script setup>
import { computed, nextTick, onUnmounted, ref, watch } from 'vue'
import {
  BarController, BarElement, CategoryScale, Chart, Filler, Legend, LineController,
  LineElement, LinearScale, LogarithmicScale, PointElement, Tooltip,
} from 'chart.js'

import Heatmap from '@/components/Heatmap.vue'
import { analytics, consumers, describeApiError, system } from '@/api'
import { useApi } from '@/composables/useApi'
import { categorySlot } from '@/composables/useCategoryColors'
import { alpha, categoryPalette, chartPalette, chartTheme, semanticColors, themeVersion } from '@/composables/useChartTheme'
import { ackFailureSeverity, backlogSeverity } from '@/composables/useSeverity'
import {
  formatDateTimeLocal, formatTimestampRange, formatTimestampRangeUtc, formatTimestampUtc,
  validateRange,
} from '@/composables/useFormat'
import { useAutoRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import {
  comparisonRange, deeperFindings, efficiency, enrichRows, findings, flowSeries, formatters as fmt,
  heatCells, rollupFromQueueOps, sameHourBaseline, totalSeries, trimOpenBucket, weeklyProfile, windowDeltas,
} from '@/composables/useWorkload'
import { onClusterChange, useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { isMissingRoute, routeSupport } from '@/stores/routeSupport'

// TENANT PAGE. Three sources, each with its own panel state:
//   /api/v1/analytics/workload — the whole page's numbers, tenant-scoped
//   /api/v1/consumer-groups    — group lag; the endpoint counts groups but
//                                never reports their lag, so "oldest waiting"
//                                exists only while this list is up
//   the cached queue list      — queue -> namespace/task, which is how a
//                                consumer group is attributed to a row
// The three fail independently and say so independently: a chart card whose
// source is down renders the failure, never a zeroed bar.
Chart.register(
  BarController, BarElement, CategoryScale, Filler, Legend, LineController,
  LineElement, LinearScale, LogarithmicScale, PointElement, Tooltip,
)

const { actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
const queuesStore = useQueuesStore()

// ---------------------------------------------------------------------------
// Chart.js plugins — registered once for the module, drawn from tokens at
// paint time so a theme flip repaints them with everything else.
// ---------------------------------------------------------------------------
const toneColor = (t) => t === 'bad' ? semanticColors.bad.line
  : t === 'warn' ? semanticColors.warn.line
    : t === 'ok' ? semanticColors.ok.line
      : t === 'ice' ? chartPalette[3].line : chartPalette[1].line

/** Value labels past the end of a horizontal bar. */
const endLabels = {
  id: 'endLabels',
  afterDatasetsDraw(chart, _a, opts) {
    if (!opts || !opts.format) return
    const ctx = chart.ctx
    ctx.save()
    ctx.font = `500 10.5px ${chartTheme.fontFamily}`
    ctx.fillStyle = chartTheme.tooltipBody
    ctx.textBaseline = 'middle'
    ctx.textAlign = 'left'
    const ds = chart.data.datasets
    chart.data.labels.forEach((_, i) => {
      let x = chart.chartArea.left
      let y = null
      for (let d = 0; d < ds.length; d++) {
        const meta = chart.getDatasetMeta(d)
        if (!meta || meta.hidden) continue
        const el = meta.data[i]
        if (!el) continue
        if (Number.isFinite(el.x)) x = Math.max(x, el.x)
        y = el.y
      }
      if (y === null || !Number.isFinite(y)) return
      const txt = opts.format(i)
      if (!txt) return
      ctx.fillText(txt, Math.min(x + 6, chart.width - ctx.measureText(txt).width - 2), y)
    })
    ctx.restore()
  },
}

/** Dashed threshold lines on the x axis, with a label above the plot. */
const refLines = {
  id: 'refLines',
  afterDraw(chart, _a, opts) {
    if (!opts || !opts.lines) return
    const ctx = chart.ctx
    const xs = chart.scales.x
    if (!xs) return
    ctx.save()
    ctx.font = `500 9.5px ${chartTheme.fontFamily}`
    // On a linear axis two thresholds can sit a few pixels apart (10s and 60s
    // under a 1h scale): the lines may overlap, the labels must not. Walk them
    // left to right and push a label right of the previous one when needed.
    const lines = opts.lines
      .filter((l) => l.x >= xs.min && l.x <= xs.max)
      .map((l) => ({ ...l, px: xs.getPixelForValue(l.x) }))
      .sort((a, b) => a.px - b.px)
    let lastRight = -Infinity
    for (const l of lines) {
      ctx.strokeStyle = alpha(toneColor(l.tone), 0.7)
      ctx.setLineDash([3, 3])
      ctx.lineWidth = 1
      ctx.beginPath()
      ctx.moveTo(l.px, chart.chartArea.top)
      ctx.lineTo(l.px, chart.chartArea.bottom)
      ctx.stroke()
      ctx.setLineDash([])
      const w = ctx.measureText(l.label).width
      const x = Math.min(Math.max(l.px, lastRight + 6 + w / 2), chart.width - w / 2 - 2)
      lastRight = x + w / 2
      ctx.fillStyle = toneColor(l.tone)
      ctx.textAlign = 'center'
      ctx.fillText(l.label, x, chart.chartArea.top - 4)
    }
    ctx.restore()
  },
}

/** Direct labels at the right edge of a stacked area — cheaper than a legend. */
const edgeLabels = {
  id: 'edgeLabels',
  afterDatasetsDraw(chart, _a, opts) {
    if (!opts || !opts.on) return
    const ctx = chart.ctx
    ctx.save()
    ctx.font = `500 10px ${chartTheme.fontFamily}`
    ctx.textAlign = 'left'
    ctx.textBaseline = 'middle'
    const placed = []
    const n = chart.data.labels.length
    let li = n - 1
    while (li > 0 && !chart.data.datasets.some((ds) => ds.stack !== 'compare' && ds.data[li] > 0)) li--
    // One label per band, wanted at the band's vertical middle on the last
    // bucket. Datasets stack bottom-up, so `below` walks from the baseline.
    let below = chart.chartArea.bottom
    chart.data.datasets.forEach((ds, d) => {
      const meta = chart.getDatasetMeta(d)
      const el = meta.data[li]
      if (meta.hidden || !el || !Number.isFinite(el.y)) return
      if (ds.stack === 'compare') return // the overlay is named by the legend
      const v = ds.data[li]
      const y = (el.y + below) / 2
      below = el.y
      // A band that is empty on the last bucket has no edge to label; the
      // legend still names it.
      if (!(v > 0)) return
      placed.push({ label: ds.label, x: el.x, y })
    })
    // Thin bands crowd their labels together: spread them apart bottom-up,
    // then push the stack back into the chart area top-down if it overflowed.
    const gap = 12
    for (let i = 1; i < placed.length; i++) placed[i].y = Math.min(placed[i].y, placed[i - 1].y - gap)
    const top = chart.chartArea.top + 6
    if (placed.length && placed[placed.length - 1].y < top) {
      placed[placed.length - 1].y = top
      for (let i = placed.length - 2; i >= 0; i--) placed[i].y = Math.max(placed[i].y, placed[i + 1].y + gap)
    }
    ctx.fillStyle = chartTheme.tooltipText
    for (const p of placed) {
      const tw = ctx.measureText(p.label).width
      ctx.fillText(p.label, Math.min(p.x + 5, chart.width - tw - 2), p.y)
    }
    ctx.restore()
  },
}

Chart.register(endLabels, refLines, edgeLabels)

// ---------------------------------------------------------------------------
// Range — the same contract as Analytics, including Custom.
// ---------------------------------------------------------------------------
const timeRanges = [
  { label: '1h', value: '1h' },
  { label: '6h', value: '6h' },
  { label: '24h', value: '24h' },
  { label: '7d', value: '7d' },
]
const QUICK_MINUTES = { '1h': 60, '6h': 360, '24h': 1440, '7d': 10080 }

const selectedRange = ref('1h')
const customMode = ref(false)
const customFrom = ref('')
const customTo = ref('')
const appliedCustom = ref(null)

const groupBy = ref('namespace')
const focus = ref(null)
const selectedQueue = ref(null)
const metric = ref('pop')

const metrics = [
  { label: 'delivered', value: 'pop' },
  { label: 'pushed', value: 'push' },
  { label: 'empty polls', value: 'popEmpty' },
  { label: 'ack failures', value: 'ackFailed' },
  { label: 'parked', value: 'parked' },
]
const METRIC_LABEL = {
  pop: 'deliveries', push: 'pushes', popEmpty: 'empty polls',
  ackFailed: 'ack failures', parked: 'parked long-polls',
}

function currentRange() {
  if (customMode.value && appliedCustom.value) return appliedCustom.value
  const to = new Date()
  const from = new Date(to.getTime() - (QUICK_MINUTES[selectedRange.value] || 60) * 60_000)
  return { from, to }
}

const rangeLabel = computed(() => {
  if (customMode.value && appliedCustom.value) {
    const { from, to } = appliedCustom.value
    return formatTimestampRange(from, to)
  }
  const r = timeRanges.find((t) => t.value === selectedRange.value)
  return `last ${r ? r.label : selectedRange.value}`
})
const rangeUtcTitle = computed(() => (
  customMode.value && appliedCustom.value
    ? formatTimestampRangeUtc(appliedCustom.value.from, appliedCustom.value.to)
    : ''
))
const customError = computed(() => validateRange(customFrom.value, customTo.value).error || '')
const customRangeValid = computed(() => !customError.value)

const selectQuickRange = (value) => {
  customMode.value = false
  selectedRange.value = value
  fetchWorkload()
  fetchDeeper()
}

const toggleCustomMode = () => {
  customMode.value = !customMode.value
  if (customMode.value) {
    const now = new Date()
    const from = new Date(now.getTime() - (QUICK_MINUTES[selectedRange.value] || 60) * 60_000)
    customTo.value = formatDateTimeLocal(now)
    customFrom.value = formatDateTimeLocal(from)
  } else {
    appliedCustom.value = null
    fetchWorkload()
    fetchDeeper()
  }
}

const applyCustomRange = () => {
  const parsed = validateRange(customFrom.value, customTo.value)
  if (parsed.error) return
  appliedCustom.value = { from: parsed.from, to: parsed.to }
  fetchWorkload()
  fetchDeeper()
}

// ---------------------------------------------------------------------------
// Panels
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FALLBACK: this page on a broker that has no /api/v1/analytics/workload.
//
// The endpoint is new and NOTHING negotiates a version, so the first call is
// also the probe. Three answers mean "this broker cannot do it" and asking
// again cannot change them (the ephemeralStore verdict pattern):
//
//   404                  the broker has no such route
//   404 route_blocked    the proxy does not classify it
//   not_an_api_response  broker-direct: the SPA fallback answered the GET
//
// On any of those the page switches, once per cluster, to rolling the payload
// up in the browser from the reads that DO exist — queue-ops for the window,
// status/queues + resources/queues for `now`, the consumer-group list for the
// group counts — and says so with a chip, because the provenance is different
// even though the shape is identical. Everything else (5xx, offline, 429) is
// transient and stays an error: retrying the real endpoint is right there.
const FALLBACK_TITLE = 'This broker has no /api/v1/analytics/workload: the page '
  + 'rolled these numbers up from queue-ops, status/queues, resources/queues '
  + 'and the consumer-group list.'

// The verdict outlives this component: stores/routeSupport remembers it per
// cluster epoch, so coming back to the page does not re-run the probe. The
// call is marked `probe`, so the 404 that establishes it is never a toast: the
// chip on the scope strip is where that fact is shown.
const getWorkload = routeSupport.guard('workload', (params, config) => system.getWorkload(params, { ...config, probe: true }))
const fallbackMode = ref(routeSupport.missing('workload') !== null)

const listOf = (d) => (Array.isArray(d) ? d : (d?.queues || d?.rows || d?.consumerGroups || d?.data || []))

/** The four reads, folded into the contract-1 payload. Shaped like a response. */
async function rollupWorkload(params, config) {
  const [ops, status, cgs] = await Promise.all([
    system.getQueueOps({ from: params.from, to: params.to }, config),
    analytics.getQueues({ limit: 500 }, config).catch(() => ({ data: [] })),
    consumers.list(config).catch(() => ({ data: [] })),
  ])
  await queuesStore.fetchQueues().catch(() => [])
  return {
    data: rollupFromQueueOps({
      ops: ops.data,
      statusQueues: listOf(status.data),
      resourceQueues: queuesStore.queues.value,
      consumerGroups: listOf(cgs.data),
      groupBy: params.groupBy,
      namespace: params.namespace,
      task: params.task,
      queue: params.queue,
      capturedAt: new Date(),
    }),
  }
}

const workload = useApi((params, config) => (
  fallbackMode.value ? rollupWorkload(params, config) : getWorkload(params, config)
), { immediate: false })
const consumerGroups = useApi((config) => consumers.list(config), { immediate: false })

const level = computed(() => (focus.value === null ? groupBy.value : 'queue'))
const levelWord = computed(() => (level.value === 'queue' ? 'queue' : groupBy.value))

// ---------------------------------------------------------------------------
// Comparing with another window. The same read, shifted (comparisonRange),
// through the same fallback; its rows overlay the flow and share charts and
// its tenant totals become the deltas on the counts strip. It is refetched
// when anything that shapes the window changes, and otherwise at most every
// five minutes on the refresh tick: a 7 day comparison is not a cheap read.
// ---------------------------------------------------------------------------
const compareModes = [
  { value: 'off', label: 'off' },
  { value: 'previous', label: 'previous period' },
  { value: 'yesterday', label: 'same time yesterday' },
  { value: 'lastWeek', label: 'same time last week' },
]
const compare = ref('off')
const compareLabel = computed(() => (
  { previous: 'the previous period', yesterday: 'yesterday', lastWeek: 'last week' }[compare.value] || ''
))
const workloadPrev = useApi((params, config) => (
  fallbackMode.value ? rollupWorkload(params, config) : getWorkload(params, config)
), { immediate: false })

function prevParams() {
  const { from, to } = comparisonRange(currentRange(), compare.value)
  const params = { from: from.toISOString(), to: to.toISOString(), groupBy: level.value }
  if (focus.value !== null) params[groupBy.value] = focus.value
  return params
}

let prevSig = ''
let prevFetchedAt = 0
function fetchPrev(force = false) {
  if (compare.value === 'off') {
    workloadPrev.abort()
    workloadPrev.data.value = null
    prevSig = ''
    return
  }
  const sig = JSON.stringify([compare.value, level.value, focus.value, selectedRange.value, customMode.value, appliedCustom.value])
  if (!force && sig === prevSig && Date.now() - prevFetchedAt < 300_000) return
  prevSig = sig
  prevFetchedAt = Date.now()
  workloadPrev.execute(prevParams()).catch(() => {})
}

function selectCompare(v) {
  if (compare.value === v) return
  compare.value = v
  fetchPrev(true)
}

/** Drilling in is a refetch with groupBy=queue plus the row as a filter. */
function workloadParams() {
  const { from, to } = currentRange()
  const params = { from: from.toISOString(), to: to.toISOString(), groupBy: level.value }
  if (focus.value !== null) params[groupBy.value] = focus.value
  return params
}

// The deeper reads wait for the FIRST workload answer instead of riding
// alongside the probe: on a broker without the route they would hit the same
// 404 three times over (three toasts) before the verdict is in.
let deeperPrimed = false

async function fetchWorkload() {
  try {
    await workload.execute(workloadParams())
  } catch (err) {
    // The probe. A missing route is a fact about the broker, so switch the
    // page over and re-ask ONCE; anything else stays the panel's error.
    if (!fallbackMode.value && isMissingRoute(err)) {
      fallbackMode.value = true
      // dlq-signatures and partition-liveness shipped in the same release as
      // /workload, so a broker without it has neither: settle their verdicts
      // here rather than asking twice more (two more toasts, every session).
      routeSupport.remember('dlq-signatures', err)
      routeSupport.remember('partition-liveness', err)
      await workload.execute(workloadParams()).catch(() => {})
    }
  }
  if (!deeperPrimed) {
    deeperPrimed = true
    fetchDeeper()
  }
  fetchPrev()
}

function fetchAll() {
  fetchWorkload()
  consumerGroups.refresh()
  queuesStore.fetchQueues().catch(() => {})
}

fetchAll()
useAutoRefresh(fetchAll)

// A different cluster can be a different cell running a different broker, so
// the verdict resets with it — exactly as the tenant-keyed stores do.
const stopClusterWatch = onClusterChange(() => {
  fallbackMode.value = routeSupport.missing('workload') !== null
  fetchDeeper()
})

const workloadFailed = computed(() => workload.failed.value || !workload.data.value)
const workloadErrorText = computed(() => (
  workload.failed.value ? describeApiError(workload.error.value) : 'No workload data yet.'
))
const groupsFailed = computed(() => consumerGroups.failed.value)
const groupsErrorText = computed(() => describeApiError(consumerGroups.error.value))

// ---------------------------------------------------------------------------
// Derived state
// ---------------------------------------------------------------------------
const groupRows = computed(() => {
  const d = consumerGroups.data.value
  return Array.isArray(d) ? d : (d?.rows || d?.consumerGroups || [])
})

const groupsByQueue = computed(() => {
  const m = new Map()
  for (const g of groupRows.value) {
    if (!m.has(g.queueName)) m.set(g.queueName, [])
    m.get(g.queueName).push(g)
  }
  return m
})

const queueMetaMap = computed(() => {
  const m = new Map()
  for (const [name, q] of queuesStore.queueMeta.value) m.set(name, { namespace: q.namespace, task: q.task })
  return m
})

const payload = computed(() => trimOpenBucket(workload.data.value))
const tenant = computed(() => payload.value?.tenant || null)
const buckets = computed(() => payload.value?.buckets || [])
const bucketMinutes = computed(() => payload.value?.bucketMinutes ?? 0)
const multiDay = computed(() => {
  const b = buckets.value
  return b.length >= 2 && new Date(b[0]).getUTCDate() !== new Date(b[b.length - 1]).getUTCDate()
})

// The rows are read at the level of the payload they came from, not the
// level the filters are already at: between a drill-down and its answer the
// old group rows are still on screen, and reading them as queues would hand
// their keys to the queue colour slots and mislabel every finding.
const levelOf = (p) => (p?.groupBy === 'queue' ? 'queue' : (p?.groupBy || level.value))
const rowsLevel = computed(() => levelOf(payload.value))
const rows = computed(() => enrichRows(payload.value, groupsByQueue.value, queueMetaMap.value, rowsLevel.value))

const prevPayload = computed(() => (compare.value === 'off' ? null : trimOpenBucket(workloadPrev.data.value)))
const prevRows = computed(() => enrichRows(prevPayload.value, groupsByQueue.value, queueMetaMap.value, levelOf(prevPayload.value)))
const prevByKey = computed(() => new Map(prevRows.value.map((r) => [r.key, r])))
const deltas = computed(() => windowDeltas(tenant.value?.window, prevPayload.value?.tenant?.window))

// Entity → colour. The slot is assigned by deliveries rank the first time an
// entity is seen and then kept for the session (useCategoryColors), so a
// refresh that reorders the rows does not repaint them; past the fifth slot
// the bar is grey, and the flow chart folds those into "Other". Queues are
// keyed per focus: the queues of one namespace share nothing with another's,
// and coming back to a namespace finds its queues in their old colours.
const entityColors = computed(() => {
  const m = new Map()
  const kind = rowsLevel.value === 'queue' ? `queue:${groupBy.value}=${focus.value}` : rowsLevel.value
  ;[...rows.value]
    .sort((a, b) => b.window.popMessages - a.window.popMessages)
    .forEach((r) => { m.set(r.key, categorySlot(kind, r.key)) })
  return m
})
const colorOf = (key) => {
  const slot = entityColors.value.get(key)
  return slot === null || slot === undefined ? chartPalette[2].line : categoryPalette[slot].line
}

const tenantFill = computed(() => {
  const w = tenant.value?.window
  if (!w) return null
  return (w.popMessages + w.popEmpty) >= 5 ? w.popMessages / (w.popMessages + w.popEmpty) : null
})

const heat = computed(() => (payload.value ? heatCells(rows.value) : { max: 0, rows: [] }))
const heatRows = computed(() => heat.value.rows.map((r) => ({ ...r, color: colorOf(r.key) })))

const attention = computed(() => findings(
  rows.value, level.value, rangeLabel.value, { hasGroups: !groupsFailed.value },
))

const summary = computed(() => {
  const total = rows.value.reduce((s, r) => s + r.window.popMessages, 0)
  if (!total) return 'Nothing was delivered in this window.'
  const top = [...rows.value]
    .sort((a, b) => b.window.popMessages - a.window.popMessages)
    .filter((r) => r.window.popMessages > 0)
    .slice(0, 3)
  const who = level.value === 'queue' ? `${focusName.value} delivered` : 'the tenant delivered'
  const parts = top.map((r) => `${r.name} ${fmt.pct(r.window.popMessages / total)}`
    + (level.value === 'queue' ? '' : ` on ${fmt.plural(r.now.queuesActive, 'active queue')}`))
  return `In the ${rangeLabel.value} ${who} ${fmt.n(total)} messages: ${parts.join(', ')}.`
})

const rootCrumb = computed(() => (groupBy.value === 'namespace' ? 'all namespaces' : 'all tasks'))
const focusName = computed(() => (
  focus.value || (groupBy.value === 'namespace' ? '(no namespace)' : '(no task)')
))
const focusHint = computed(() => (
  focus.value === null ? `or click a ${groupBy.value} anywhere to drill in`
    : selectedQueue.value ? '' : 'click a queue for its consumer groups'
))

// Every namespace (or task) the tenant has, from the queue list, so a group
// that is quiet in this window can still be picked; the empty key is the
// real group of queues without one and sorts last.
const focusOptions = computed(() => {
  const kind = groupBy.value
  const seen = new Set()
  for (const m of queueMetaMap.value.values()) seen.add((kind === 'task' ? m.task : m.namespace) ?? '')
  for (const r of rows.value) if (r.level !== 'queue') seen.add(r.key ?? '')
  const keys = [...seen].sort((a, b) => (a === '' ? 1 : b === '' ? -1 : a.localeCompare(b)))
  return keys.map((k) => ({ value: k, label: k || (kind === 'task' ? '(no task)' : '(no namespace)') }))
})

function pickFocus(v) {
  if (v === '*') { goRoot(); return }
  if (focus.value === v) return
  focus.value = v
  selectedQueue.value = null
  fetchWorkload()
}

const queueGroups = computed(() => (
  (groupsByQueue.value.get(selectedQueue.value) || [])
    .slice()
    .sort((a, b) => (b.maxTimeLag || 0) - (a.maxTimeLag || 0))
))
const groupsSub = computed(() => {
  const gs = queueGroups.value
  return `${fmt.plural(gs.length, 'group')} · ${gs.filter((g) => g.state === 'Lagging').length} Lagging`
    + ` · ${gs.filter((g) => g.state === 'Dead').length} Dead`
})

const toneClass = (t) => (t ? `wl-${t}` : '')

// ---------------------------------------------------------------------------
// Counts-strip tones.
//
// The strip used to test three counts against constants — pending >= 1000,
// deadLetter > 0, ackFailed > 0 — and a tenant doing ordinary work tripped all
// three. Each is now read against the work the window actually contains.
// ---------------------------------------------------------------------------
const windowSeconds = computed(() => {
  if (customMode.value && appliedCustom.value) {
    const { from, to } = appliedCustom.value
    const s = (new Date(to).getTime() - new Date(from).getTime()) / 1000
    return Number.isFinite(s) && s > 0 ? s : null
  }
  const minutes = QUICK_MINUTES[selectedRange.value]
  return minutes ? minutes * 60 : null
})
// Acks per second across the window: the rate a backlog would drain at.
const tenantDrainPerSec = computed(() => {
  const acked = tenant.value?.window?.ackSuccess
  const seconds = windowSeconds.value
  if (acked === null || acked === undefined || !seconds) return null
  return acked / seconds
})
const pendingTone = computed(() => toneClass(backlogSeverity({
  pending: tenant.value?.now?.pending,
  drainPerSec: tenantDrainPerSec.value,
})))
const ackFailedTone = computed(() => toneClass(ackFailureSeverity({
  failed: tenant.value?.window?.ackFailed,
  succeeded: tenant.value?.window?.ackSuccess,
})))

// ---------------------------------------------------------------------------
// Drill-down
// ---------------------------------------------------------------------------
function drill(row) {
  if (!row) return
  if (row.level === 'queue') selectedQueue.value = row.key
  else { focus.value = row.key; selectedQueue.value = null; fetchWorkload() }
}
const drillByKey = (heatRow) => drill(rows.value.find((r) => r.key === heatRow.key))

function goRoot() {
  focus.value = null
  selectedQueue.value = null
  fetchWorkload()
}

function selectGroupBy(v) {
  if (groupBy.value === v) return
  groupBy.value = v
  focus.value = null
  selectedQueue.value = null
  fetchWorkload()
  fetchDeeper()
}

// ---------------------------------------------------------------------------
// Charts. Imperative like RowChart: these configs need a logarithmic axis,
// per-bar colours and three custom plugins, none of which BaseChart passes
// through. Colours come from useChartTheme only, and every instance is rebuilt
// when `themeVersion` ticks.
// ---------------------------------------------------------------------------
const canvases = {}
const charts = {}

const setCanvas = (id, el) => {
  canvases[id] = el || null
  if (!el && charts[id]) { charts[id].destroy(); delete charts[id] }
}

const chartHeight = (rowCount) => `${Math.max(120, Math.round(rowCount * 30 + 60))}px`

function mount(id, cfg) {
  if (charts[id]) { charts[id].destroy(); delete charts[id] }
  const el = canvases[id]
  if (!el) return
  charts[id] = new Chart(el, cfg)
}

function applyDefaults() {
  Chart.defaults.color = chartTheme.tick
  Chart.defaults.font.family = chartTheme.fontFamily
  Chart.defaults.font.size = 10.5
  Chart.defaults.borderColor = chartTheme.grid
  Chart.defaults.animation = false
  Chart.defaults.maintainAspectRatio = false
  Chart.defaults.plugins.tooltip.backgroundColor = chartTheme.tooltipBg
  Chart.defaults.plugins.tooltip.borderColor = chartTheme.tooltipBorder
  Chart.defaults.plugins.tooltip.borderWidth = 1
  Chart.defaults.plugins.tooltip.titleColor = chartTheme.tooltipText
  Chart.defaults.plugins.tooltip.bodyColor = chartTheme.tooltipBody
  Chart.defaults.plugins.tooltip.padding = 8
  Chart.defaults.plugins.tooltip.cornerRadius = 6
  Chart.defaults.plugins.legend.labels.usePointStyle = true
  Chart.defaults.plugins.legend.labels.boxWidth = 8
  Chart.defaults.plugins.legend.labels.color = chartTheme.tooltipBody
}

// Queue names are long and their distinctive part is the tail, so they are
// truncated from the front. The budget follows the canvas width rather than a
// constant: a fixed 22 characters is what turned channel.metric_sample into
// "…nnel.metric_sample" on a three-column row, and would waste the full-width
// cards. Chart.js binds the scale to `this` in a tick callback, hence the
// regular function.
const CHAR_PX = 6.6 // one 11px monospace glyph
const labelBudget = (chart) => Math.min(64, Math.max(18, Math.floor(((chart && chart.width) || 600) * 0.36 / CHAR_PX)))
const tailTrunc = (l, n) => (l.length > n ? '…' + l.slice(-(n - 1)) : l)
const tickLabel = (labels) => function (v, i) { return tailTrunc(String(labels[i] ?? ''), labelBudget(this.chart)) }

function barBase(labels, onClick) {
  return {
    indexAxis: 'y',
    responsive: true,
    onClick,
    onHover: (e, els) => { e.native.target.style.cursor = els.length && onClick ? 'pointer' : 'default' },
    layout: { padding: { right: 122, top: 10 } },
    scales: {
      x: { grid: { color: chartTheme.grid }, ticks: { maxTicksLimit: 5 }, beginAtZero: true },
      y: {
        grid: { display: false },
        ticks: {
          color: chartTheme.tooltipText,
          font: { size: 11 },
          callback: tickLabel(labels),
        },
      },
    },
    plugins: { legend: { display: false } },
  }
}

const drillOn = (sorted) => (evt, els) => {
  if (!els.length) return
  drill(sorted[els[0].index])
}

function renderFlow() {
  const series = flowSeries(rows.value, buckets.value, metric.value)
  const labels = buckets.value.map((b) => fmt.bucket(b, multiDay.value))
  mount('flow', {
    type: 'line',
    data: {
      labels,
      datasets: series.map((s) => {
        const color = s.key === null ? chartPalette[2].line : colorOf(s.key)
        return {
          label: s.label, data: s.data, borderColor: color, backgroundColor: alpha(color, 0.38),
          fill: true, borderWidth: 1.5, pointRadius: 0, pointHitRadius: 6, tension: 0, spanGaps: false,
        }
      }).concat(prevPayload.value ? [{
        // The comparison window's total, its own stack so it overlays instead
        // of piling onto the areas.
        label: `total, ${compareLabel.value}`, data: totalSeries(prevRows.value, prevPayload.value.buckets, metric.value),
        borderColor: chartTheme.tooltipText, borderDash: [4, 3], borderWidth: 1.25, pointRadius: 0, pointHitRadius: 6,
        fill: false, tension: 0, spanGaps: false, stack: 'compare',
      }] : []),
    },
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      layout: { padding: { right: 118 } },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 10, maxRotation: 0 } },
        y: {
          stacked: true, beginAtZero: true, grid: { color: chartTheme.grid },
          title: { display: true, text: `${METRIC_LABEL[metric.value]} per ${bucketMinutes.value} min bucket`, font: { size: 10 } },
          ticks: { callback: (v) => fmt.k(v) },
        },
      },
      plugins: {
        legend: { display: true, position: 'top', align: 'end' },
        tooltip: {
          itemSort: (a, b) => b.parsed.y - a.parsed.y,
          callbacks: {
            title: (items) => `${items[0].label} UTC`,
            label: (c) => ` ${c.dataset.label}: ${fmt.n(c.parsed.y)}`,
          },
        },
        edgeLabels: { on: true },
      },
    },
  })
}

function renderShare() {
  const sorted = [...rows.value].sort((a, b) => b.window.popMessages - a.window.popMessages)
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, drillOn(sorted))
  options.scales.x.ticks.callback = (v) => fmt.k(v)
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => ` ${c.dataset.label}: ${fmt.n(c.parsed.x)}`
          + (c.datasetIndex === 0 ? ` (${fmt.pct(sorted[c.dataIndex].share)} of the tenant)` : ''),
      },
    },
    endLabels: { format: (i) => fmt.pct(sorted[i].share) },
  }
  mount('share', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'delivered', data: sorted.map((r) => r.window.popMessages), backgroundColor: sorted.map((r) => alpha(colorOf(r.key), 0.9)), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        { label: 'pushed', data: sorted.map((r) => r.window.pushMessages), backgroundColor: sorted.map((r) => alpha(colorOf(r.key), 0.42)), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        ...(prevPayload.value ? [{
          label: `delivered, ${compareLabel.value}`, data: sorted.map((r) => prevByKey.value.get(r.key)?.window.popMessages ?? null),
          backgroundColor: 'transparent', borderColor: chartTheme.tick, borderWidth: 1, borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8,
        }] : []),
      ],
    },
    options,
  })
}

// --- footprint: point-in-time structure, coloured by entity ---------------
function renderPartsBy() {
  const sorted = [...rows.value].sort((a, b) => b.now.partitions - a.now.partitions)
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, drillOn(sorted))
  options.layout.padding.right = 150 // room for "58,492 · 2.8k/queue"
  options.scales.x.ticks.callback = (v) => fmt.k(v)
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = sorted[c.dataIndex]
          return ` ${fmt.n(c.parsed.x)} partitions on ${fmt.plural(r.queues || 0, 'queue')}`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = sorted[i]
        return r.queues ? `${fmt.n(r.now.partitions)} · ${fmt.k(r.now.partitions / r.queues)}/queue` : fmt.n(r.now.partitions)
      },
    },
  }
  mount('partsBy', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'partitions', data: sorted.map((r) => r.now.partitions),
        backgroundColor: sorted.map((r) => alpha(colorOf(r.key), 0.85)), borderRadius: 2, barPercentage: 0.7, categoryPercentage: 0.8,
      }],
    },
    options,
  })
}

function renderStructure() {
  const sorted = [...rows.value].sort((a, b) => ((b.queues || 0) - (a.queues || 0)) || (b.now.groups - a.now.groups))
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, drillOn(sorted))
  options.layout.padding.right = 150 // room for "21 queues · 30 groups"
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => ` ${c.dataset.label}: ${fmt.n(c.parsed.x)}`,
        afterBody: (items) => {
          const r = sorted[items[0].dataIndex]
          return r.now.queuesWithoutGroup ? ` ${fmt.n(r.now.queuesWithoutGroup)} of the queues have no group` : ''
        },
      },
    },
    endLabels: { format: (i) => `${fmt.plural(sorted[i].queues || 0, 'queue')} · ${fmt.plural(sorted[i].now.groups, 'group')}` },
  }
  mount('structure', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'queues', data: sorted.map((r) => r.queues || 0), backgroundColor: sorted.map((r) => alpha(colorOf(r.key), 0.9)), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        { label: 'consumer groups', data: sorted.map((r) => r.now.groups), backgroundColor: sorted.map((r) => alpha(colorOf(r.key), 0.42)), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
      ],
    },
    options,
  })
}

function renderRetained() {
  const sorted = [...rows.value].sort((a, b) => (b.now.retainedBytes || 0) - (a.now.retainedBytes || 0))
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, drillOn(sorted))
  options.scales.x.ticks.callback = (v) => fmt.bytes(v)
  options.plugins = {
    legend: { display: false },
    tooltip: { callbacks: { label: (c) => ` ${fmt.bytes(c.parsed.x)} retained` } },
    endLabels: { format: (i) => fmt.bytes(sorted[i].now.retainedBytes) },
  }
  mount('retained', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'retained', data: sorted.map((r) => r.now.retainedBytes || 0),
        backgroundColor: sorted.map((r) => alpha(colorOf(r.key), 0.85)), borderRadius: 2, barPercentage: 0.7, categoryPercentage: 0.8,
      }],
    },
    options,
  })
}

function renderChurn() {
  const churn = (r) => (r.window.partitionsCreated || 0) + (r.window.partitionsDeleted || 0)
  const sorted = [...rows.value].sort((a, b) => churn(b) - churn(a))
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, drillOn(sorted))
  options.scales.x.ticks.callback = (v) => fmt.k(v)
  options.plugins = {
    legend: { display: false },
    tooltip: { callbacks: { label: (c) => ` ${c.dataset.label}: ${fmt.n(c.parsed.x)}` } },
    endLabels: {
      format: (i) => {
        const w = sorted[i].window
        if (w.partitionsCreated === null && w.partitionsDeleted === null) return 'not reported'
        return `+${fmt.n(w.partitionsCreated || 0)} · −${fmt.n(w.partitionsDeleted || 0)}`
      },
    },
  }
  mount('churn', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'created', data: sorted.map((r) => r.window.partitionsCreated || 0), backgroundColor: sorted.map((r) => alpha(colorOf(r.key), 0.9)), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        { label: 'deleted', data: sorted.map((r) => r.window.partitionsDeleted || 0), backgroundColor: alpha(chartPalette[2].line, 0.7), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
      ],
    },
    options,
  })
}

function renderPending() {
  const sorted = [...rows.value].sort((a, b) => b.now.pending - a.now.pending)
  const labels = sorted.map((r) => r.name)
  const tone = (v) => v >= 10000 ? semanticColors.bad.line : v >= 1000 ? semanticColors.warn.line : chartPalette[1].line
  const options = barBase(labels, drillOn(sorted))
  options.scales.x.stacked = true
  options.scales.y.stacked = true
  options.scales.x.ticks.callback = (v) => fmt.k(v)
  options.plugins = {
    legend: { display: false },
    tooltip: { callbacks: { label: (c) => ` ${c.dataset.label}: ${fmt.n(c.parsed.x)}` } },
    endLabels: {
      format: (i) => {
        const r = sorted[i]
        return r.now.pending
          ? fmt.n(r.now.pending) + (r.now.processing ? ` +${fmt.n(r.now.processing)} in flight` : '')
          : '0'
      },
    },
  }
  mount('pending', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'with a consumer group', data: sorted.map((r) => r.now.pending - r.now.pendingWithoutGroup), backgroundColor: sorted.map((r) => alpha(tone(r.now.pending), 0.85)), borderRadius: 2 },
        { label: 'no consumer group', data: sorted.map((r) => r.now.pendingWithoutGroup), backgroundColor: alpha(semanticColors.bad.line, 0.9), borderRadius: 2 },
      ],
    },
    options,
  })
}

function renderOldest() {
  const sorted = [...rows.value].sort((a, b) => (b.oldest ?? -1) - (a.oldest ?? -1))
  const labels = sorted.map((r) => r.name)
  const tone = (s) => s >= 300 ? 'bad' : s >= 60 ? 'warn' : 'mute'
  const options = barBase(labels, drillOn(sorted))
  options.scales.x.max = Math.max(360, ...sorted.map((r) => r.oldest || 0)) * 1.15
  options.scales.x.ticks.callback = (v) => fmt.sec(v)
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = sorted[c.dataIndex]
          return r.oldest === null ? ' no consumer group'
            : ` ${fmt.sec(r.oldest)} · ${r.groupsLagging} of ${r.groupsN} groups lagging`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = sorted[i]
        if (r.oldest !== null) return fmt.sec(r.oldest)
        return r.now.pending > 0 ? `no group · ${fmt.n(r.now.pending)} pending` : 'no group'
      },
    },
    refLines: { lines: [{ x: 60, label: '60s', tone: 'warn' }, { x: 300, label: '300s', tone: 'bad' }] },
  }
  mount('oldest', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'oldest waiting',
        data: sorted.map((r) => (r.oldest === null ? null : r.oldest)),
        backgroundColor: sorted.map((r) => alpha(toneColor(tone(r.oldest || 0)), 0.85)),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderDlq() {
  const sorted = [...rows.value].sort((a, b) => b.now.deadLetter - a.now.deadLetter)
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, drillOn(sorted))
  options.scales.x.ticks.callback = (v) => fmt.k(v)
  options.plugins = {
    legend: { display: false },
    tooltip: { callbacks: { label: (c) => ` ${fmt.n(c.parsed.x)} rows` } },
    endLabels: { format: (i) => fmt.n(sorted[i].now.deadLetter) },
  }
  mount('dlq', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'in DLQ',
        data: sorted.map((r) => r.now.deadLetter),
        backgroundColor: sorted.map((r) => alpha(r.now.deadLetter > 0 ? semanticColors.warn.line : chartPalette[1].line, 0.85)),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderFill() {
  const sorted = [...rows.value].sort((a, b) => (b.fill ?? -1) - (a.fill ?? -1))
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, null)
  options.scales.x.max = 100
  options.scales.x.ticks.callback = (v) => `${v}%`
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = sorted[c.dataIndex]
          return ` ${fmt.pct(r.fill)} · ${fmt.n(r.window.popMessages)} delivered · ${fmt.n(r.window.popEmpty)} empty polls · ${fmt.n(Math.round(r.window.parkedAvg))} parked`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = sorted[i]
        return r.fill === null ? '—' : `${fmt.pct(r.fill)} · ${fmt.k(r.window.popEmpty)} empty`
      },
    },
  }
  mount('fill', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'fill',
        data: sorted.map((r) => (r.fill === null ? null : r.fill * 100)),
        backgroundColor: alpha(chartPalette[1].line, 0.85), borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderAck() {
  const sorted = [...rows.value].sort((a, b) => (a.ackOk ?? 2) - (b.ackOk ?? 2))
  const labels = sorted.map((r) => r.name)
  const tone = (r) => r.ackOk === null ? 'mute' : r.ackOk < 0.5 ? 'bad' : r.ackOk < 0.9 ? 'warn' : 'mute'
  const options = barBase(labels, null)
  options.scales.x.max = 100
  options.scales.x.ticks.callback = (v) => `${v}%`
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = sorted[c.dataIndex]
          return r.ackOk === null ? ' under 5 acks'
            : ` ${fmt.pct(r.ackOk)} · ${fmt.n(r.window.ackFailed)} of ${fmt.n(r.acks)} failed`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = sorted[i]
        return r.ackOk === null ? `— (${fmt.n(r.acks)} acks)`
          : fmt.pct(r.ackOk) + (r.window.ackFailed ? ` · ${fmt.n(r.window.ackFailed)} failed` : '')
      },
    },
  }
  mount('ack', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'ack ok',
        data: sorted.map((r) => (r.ackOk === null ? null : r.ackOk * 100)),
        backgroundColor: sorted.map((r) => alpha(toneColor(tone(r)), 0.85)),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderLag() {
  const sorted = [...rows.value].sort((a, b) => (b.window.maxLagMs ?? -1) - (a.window.maxLagMs ?? -1))
  const labels = sorted.map((r) => r.name)
  const tone = (ms) => ms >= 60000 ? 'bad' : ms >= 10000 ? 'warn' : 'mute'
  const options = barBase(labels, null)
  options.scales.x = {
    type: 'logarithmic', min: 1,
    max: Math.max(120000, ...sorted.map((r) => r.window.maxLagMs || 0)) * 3,
    grid: { color: chartTheme.grid },
    ticks: {
      maxRotation: 0, autoSkip: false, font: { size: 9.5 },
      callback: (v) => ([1, 10, 100, 1000, 10000, 60000, 600000, 3600000].includes(v) ? fmt.ms(v) : ''),
    },
  }
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = sorted[c.dataIndex]
          return r.window.maxLagMs === null ? ' nothing popped'
            : ` avg ${fmt.ms(r.window.avgLagMs)} → max ${fmt.ms(r.window.maxLagMs)} · ${fmt.n(r.window.popMessages)} pops`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = sorted[i]
        return r.window.maxLagMs === null ? 'no pops'
          : `${fmt.ms(r.window.avgLagMs)} → ${fmt.ms(r.window.maxLagMs)}`
      },
    },
    refLines: { lines: [{ x: 10000, label: '10s', tone: 'warn' }, { x: 60000, label: '60s', tone: 'bad' }] },
  }
  mount('lag', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'lag at pop',
        // A floating bar from the average to the worst: one row, two facts.
        data: sorted.map((r) => (r.window.maxLagMs === null
          ? null
          : [Math.max(1, r.window.avgLagMs || 1), Math.max(2, r.window.maxLagMs)])),
        backgroundColor: sorted.map((r) => alpha(toneColor(tone(r.window.maxLagMs || 0)), 0.8)),
        borderColor: sorted.map((r) => toneColor(tone(r.window.maxLagMs || 0))),
        borderWidth: 1, borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderGroups() {
  const gs = queueGroups.value
  if (!selectedQueue.value || !gs.length) {
    if (charts.groups) { charts.groups.destroy(); delete charts.groups }
    return
  }
  const labels = gs.map((g) => (g.name === '__QUEUE_MODE__' ? 'queue mode' : g.name))
  const tone = (s) => s >= 300 ? 'bad' : s >= 60 ? 'warn' : 'mute'
  const options = barBase(labels, null)
  options.layout.padding.right = 240
  options.scales.x.max = Math.max(360, ...gs.map((g) => g.maxTimeLag || 0)) * 1.15
  options.scales.x.ticks.callback = (v) => fmt.sec(v)
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const g = gs[c.dataIndex]
          return ` ${fmt.sec(g.maxTimeLag || 0)} · ${fmt.n(g.partitionsWithLag)} of ${fmt.n(g.members)} partitions lagging · offset lag ${fmt.n(g.totalLag || 0)}`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const g = gs[i]
        return `${g.maxTimeLag ? fmt.sec(g.maxTimeLag) : '0s'} · ${fmt.n(g.partitionsWithLag)}/${fmt.n(g.members)} lagging`
          + (g.conflation ? ' · conflation' : '') + (g.kind === 'kafka' ? ' · kafka' : '')
          + ` · mode ${g.subscriptionMode || '—'}`
      },
    },
    refLines: { lines: [{ x: 60, label: '60s', tone: 'warn' }, { x: 300, label: '300s', tone: 'bad' }] },
  }
  mount('groups', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'time lag',
        data: gs.map((g) => g.maxTimeLag || 0),
        backgroundColor: gs.map((g) => alpha(toneColor(tone(g.maxTimeLag || 0)), 0.85)),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

// ---------------------------------------------------------------------------
// THE DEEPER LAYER — its own panels, its own fetches, its own stamps.
//
// These do NOT ride useAutoRefresh: the weekly read is a 7 day window at
// hourly buckets and, in fallback mode, that is a queue-ops response of
// several MB. It answers a question that does not change minute to minute
// ("is this hour normal for a Tuesday"), so it is fetched on open and on a
// range or group-by change, and nowhere else.
//
// Each panel fails alone. The three routes of WORKLOAD_CONTRACT_2 are new: on
// an older broker they 404, and that card says "not available on this broker"
// instead of drawing an empty chart, which would read as "nothing happened".
// ---------------------------------------------------------------------------
const HOURS = Array.from({ length: 24 }, (_, h) => `2026-01-01T${String(h).padStart(2, '0')}:00:00Z`)
const DOW_LABEL = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
const DOW_ORDER = [1, 2, 3, 4, 5, 6, 0]

const weekPick = ref('*')
const dlqPick = ref(null)

const deepWeek = useApi((params, config) => (
  fallbackMode.value ? rollupWorkload(params, config) : getWorkload(params, config)
), { immediate: false })
const deepEff = useApi((params, config) => (
  fallbackMode.value ? rollupWorkload(params, config) : getWorkload(params, config)
), { immediate: false })
const retention = useApi((params, config) => system.getRetention(params, config), { immediate: false })
// Neither route has a client-side stand-in, so on an older broker the verdict
// IS the panel; guarded so it is established once per cluster, not once per
// mount or per DLQ pill click (each re-ask would be a toast).
const dlqSignatures = useApi(
  routeSupport.guard('dlq-signatures', (params, config) => system.getDlqSignatures(params, { ...config, probe: true })),
  { immediate: false },
)
const partitions = useApi(
  routeSupport.guard('partition-liveness', (params, config) => system.getPartitionLiveness(params, { ...config, probe: true })),
  { immediate: false },
)

/** A 404 here is an older broker, not a fault — say which, and say it quietly. */
const deeperErrorText = (panel) => (
  isMissingRoute(panel.error.value)
    ? 'Not available on this broker.'
    : describeApiError(panel.error.value)
)
const deeperErrorClass = (panel) => (isMissingRoute(panel.error.value) ? 'empty' : 'panel-err')

function fetchDeeper() {
  const now = new Date()
  const weekFrom = new Date(now.getTime() - 7 * 24 * 60 * 60_000)
  const dayFrom = new Date(now.getTime() - 24 * 60 * 60_000)
  const { from, to } = currentRange()
  deepWeek.execute({ from: weekFrom.toISOString(), to: now.toISOString(), groupBy: groupBy.value }).catch(() => {})
  deepEff.execute({ from: dayFrom.toISOString(), to: now.toISOString(), groupBy: 'queue' }).catch(() => {})
  // groupBy is new on this route too — an older broker just ignores it and
  // answers the ungrouped totals, which leaves `rows` empty rather than wrong.
  retention.execute({ from: from.toISOString(), to: to.toISOString(), groupBy: 'queue' }).catch(() => {})
  partitions.execute({ limit: 10 }).catch(() => {})
}

// --- weekly profile --------------------------------------------------------
const week = computed(() => (deepWeek.data.value ? weeklyProfile(deepWeek.data.value) : null))

const weekPills = computed(() => {
  if (!week.value) return []
  const byTotal = [...week.value.totals.entries()].sort((a, b) => b[1] - a[1])
  return [{ value: '*', label: 'tenant' }].concat(
    byTotal.map(([k]) => ({ value: k, label: k || (groupBy.value === 'task' ? '(no task)' : '(no namespace)') })),
  )
})

const weekMatrix = computed(() => {
  if (!week.value) return null
  return weekPick.value === '*' ? week.value.tenant : (week.value.byKey.get(weekPick.value) || null)
})

/** The day x hour grid, in Heatmap's shape: Monday first, sqrt-scaled ink. */
const weekRows = computed(() => {
  const m = weekMatrix.value
  if (!m) return []
  let max = 0
  for (const day of m) for (const v of day) if (v !== null && v > max) max = v
  return DOW_ORDER.map((d) => ({
    key: String(d),
    name: DOW_LABEL[d],
    total: m[d].reduce((a, b) => a + (b || 0), 0),
    values: m[d].map((v) => ({
      value: v,
      percent: v === null ? null : (max ? Math.round(Math.sqrt(v / max) * 100) : 0),
    })),
  }))
})

const weekSub = computed(() => (
  week.value ? `deliveries per hour of the week, UTC · ${fmt.plural(week.value.days.length, 'calendar day')}` : 'deliveries per hour of the week, UTC'
))

const weekStats = computed(() => {
  const m = weekMatrix.value
  if (!m) return ''
  const flat = m.flat().filter((v) => v !== null)
  if (!flat.length) return 'No hourly bucket in this window.'
  const avg = flat.reduce((a, b) => a + b, 0) / flat.length
  const peak = Math.max(...flat)
  let bd = 0
  let bh = 0
  m.forEach((row, d) => row.forEach((v, h) => { if (v === peak) { bd = d; bh = h } }))
  return `${fmt.n(avg)} per hour on average · peak ${fmt.n(peak)} on ${DOW_LABEL[bd]} `
    + `${String(bh).padStart(2, '0')}:00 UTC (${avg ? (peak / avg).toFixed(1) : '—'}× the average)`
    + ` · ${fmt.plural(flat.length, 'hourly bucket')}`
})

// --- same-hour baseline ----------------------------------------------------
const baseline = computed(() => (
  deepWeek.data.value ? sameHourBaseline(deepWeek.data.value) : { hour: null, index: -1, label: '', rows: [] }
))
const baselineSub = computed(() => (
  baseline.value.rows.length
    ? `${baseline.value.label} against the same hour on the previous ${fmt.plural(baseline.value.rows[0].days, 'day')}`
    : 'the last complete hour against the same hour on the previous days'
))

// --- efficiency ------------------------------------------------------------
const effRows = computed(() => (
  deepEff.data.value ? efficiency(deepEff.data.value, groupsByQueue.value) : []
))
const topQueues = computed(() => effRows.value.filter((e) => e.pop > 0).slice(0, 12))
const budgetRows = computed(() => topQueues.value.filter((e) => e.lagBudget.buckets > 0))

const byteRows = computed(() => {
  // retainedBytes is the workload payload's `now`; the message count comes
  // from the queue list, which is the only place it exists.
  const msgs = new Map()
  for (const q of queuesStore.queues.value) {
    const n = q.segments?.messages ?? q.messages?.total ?? null
    if (n !== null && n > 0) msgs.set(q.name, n)
  }
  return effRows.value
    .filter((e) => (e.retainedBytes || 0) > 0 && msgs.has(e.queue))
    .map((e) => ({ queue: e.queue, retainedBytes: e.retainedBytes, messages: msgs.get(e.queue) }))
    .sort((a, b) => b.retainedBytes - a.retainedBytes)
    .slice(0, 10)
})

// --- retention -------------------------------------------------------------
// An older broker ignores `groupBy` and answers the ungrouped totals: no `rows`
// key at all. That is "cannot split", not "nothing happened".
const retentionUngrouped = computed(() => !!retention.data.value && !Array.isArray(retention.data.value.rows))
const retentionRows = computed(() => {
  const rows = retention.data.value?.rows
  if (!Array.isArray(rows)) return []
  return rows
    .map((r) => ({
      key: r.key || '(no name)',
      retention: r.totals?.retentionMsgs || 0,
      completed: r.totals?.completedRetentionMsgs || 0,
      eviction: r.totals?.evictionMsgs || 0,
    }))
    .filter((r) => r.retention + r.completed + r.eviction > 0)
    .sort((a, b) => (b.retention + b.completed + b.eviction) - (a.retention + a.completed + a.eviction))
    .slice(0, 10)
})

// --- DLQ signatures --------------------------------------------------------
// The pills are the queues that actually have dead-letter rows NOW, read off
// the queue-level payload; one signature fetch per pill, on click.
const dlqPills = computed(() => effRows.value
  .filter((e) => (e.deadLetter || 0) > 0)
  .sort((a, b) => b.deadLetter - a.deadLetter)
  .slice(0, 6)
  .map((e) => e.queue))

const shortQueue = (q) => (q.length > 26 ? '…' + q.slice(-25) : q)

function pickDlq(queue) {
  dlqPick.value = queue
  dlqSignatures.execute({ queue, limit: 150 }).catch(() => {})
}

watch(dlqPills, (pills) => {
  if (!pills.length) { dlqPick.value = null; return }
  if (!dlqPick.value || !pills.includes(dlqPick.value)) pickDlq(pills[0])
})

const dlqSig = computed(() => dlqSignatures.data.value)
const dlqSigRows = computed(() => dlqSig.value?.signatures || [])
const dlqSub = computed(() => {
  const d = dlqSig.value
  if (!d || dlqSignatures.failed.value) return 'folded error messages of the newest rows'
  return `${fmt.n(d.rowsNow)} rows now · sample of the last ${fmt.n(d.sample)} · `
    + `${fmt.bytes(d.avgBytes)} per row · retries ${(d.retryCounts || []).join(' / ') || '—'}`
})

// --- partition liveness ----------------------------------------------------
const partitionRows = computed(() => {
  const rows = partitions.data.value?.rows
  if (!Array.isArray(rows)) return []
  // Churn is the workload window's own counter — the liveness route reports
  // creations but deliberately no deletions.
  const churn = new Map(effRows.value.map((e) => [e.queue, e]))
  return rows.map((r) => {
    const c = churn.get(r.queue) || null
    return { ...r, created: c ? c.partitionsCreated : r.created24h, deleted: c ? c.partitionsDeleted : null }
  })
})

const deepAttention = computed(() => deeperFindings({
  efficiency: effRows.value,
  baseline: baseline.value,
  dlq: dlqSig.value ? [dlqSig.value] : [],
  retention: retentionRows.value,
  partitions: partitionRows.value,
}))

// --- deeper charts. Same idioms as above: barBase + endLabels + refLines,
// colours from useChartTheme only, a null value drawn as a gap. -------------
const narrow = (options, labels) => {
  options.layout.padding.right = 96
  options.scales.y.ticks.callback = tickLabel(labels)
  return options
}

function renderBase() {
  const rows = baseline.value.rows
  if (!rows.length) return
  const labels = rows.map((r) => (r.key === '*' ? 'tenant' : (r.key || '(none)')))
  const tone = (r) => (r.ratio !== null && (r.ratio >= 2 || r.ratio <= 0.5) ? 'warn' : 'mute')
  const options = barBase(labels, null)
  options.scales.x.max = Math.max(2.6, ...rows.map((r) => r.ratio || 0)) * 1.15
  options.scales.x.ticks.callback = (v) => `${Math.round(v * 10) / 10}×`
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = rows[c.dataIndex]
          return r.mean === null ? ' no earlier day to compare with'
            : ` ${fmt.n(r.current)} this hour · usual ${fmt.n(r.mean)} (min ${fmt.n(r.min)}, max ${fmt.n(r.max)})`
              + ` over ${fmt.plural(r.days, 'day')}${r.z !== null ? ` · z ${r.z}` : ''}`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = rows[i]
        return r.ratio === null ? '—' : `${r.ratio}× · ${fmt.k(r.current)} vs ${fmt.k(r.mean)} usual`
      },
    },
    refLines: { lines: [{ x: 1, label: 'usual', tone: 'mute' }] },
  }
  mount('base', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'ratio',
        data: rows.map((r) => r.ratio),
        backgroundColor: rows.map((r) => alpha(toneColor(tone(r)), 0.85)),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderFan() {
  const rows = topQueues.value
  if (!rows.length) return
  const labels = rows.map((r) => r.queue)
  const tone = (r) => (r.fanout !== null && r.groups > 0 && r.fanout > r.groups * 1.25 ? 'warn' : 'mute')
  const options = narrow(barBase(labels, null), labels)
  options.scales.x.max = Math.max(2, ...rows.map((r) => r.fanout || 0)) * 1.2
  options.scales.x.ticks.callback = (v) => `×${v}`
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = rows[c.dataIndex]
          return r.fanout === null
            ? ` ${fmt.n(r.pop)} deliveries, 0 pushes counted, ${fmt.n(r.trx)} transactions`
            : ` ${fmt.n(r.pop)} deliveries for ${fmt.n(r.push)} pushes · ${fmt.plural(r.groups, 'group')}`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = rows[i]
        return r.fanout === null ? `transaction-fed · ${fmt.k(r.trx)} trx`
          : `×${r.fanout} · ${fmt.plural(r.groups, 'group')}`
      },
    },
  }
  mount('fan', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'deliveries per push',
        data: rows.map((r) => r.fanout),
        backgroundColor: rows.map((r) => alpha(toneColor(tone(r)), 0.85)),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderAckd() {
  const rows = topQueues.value
  if (!rows.length) return
  const labels = rows.map((r) => r.queue)
  const tone = (r) => (r.ackPerDelivery !== null && r.ackPerDelivery < 0.9 ? 'warn' : 'mute')
  const options = narrow(barBase(labels, null), labels)
  options.scales.x.max = 100
  options.scales.x.ticks.callback = (v) => `${v}%`
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = rows[c.dataIndex]
          return ` ${fmt.n(r.ack)} acked of ${fmt.n(r.pop)} delivered · acks made inside /transaction are not attributed`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = rows[i]
        if (r.ackPerDelivery === null) return '—'
        return fmt.pct(r.ackPerDelivery)
          + (r.ack === 0 && r.trx > 0 ? ' · acked elsewhere' : '')
      },
    },
  }
  mount('ackd', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'acks per delivery',
        data: rows.map((r) => (r.ackPerDelivery === null ? null : Math.min(100, r.ackPerDelivery * 100))),
        backgroundColor: rows.map((r) => alpha(toneColor(tone(r)), 0.85)),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderBatch() {
  const rows = topQueues.value
  if (!rows.length) return
  const labels = rows.map((r) => r.queue)
  const options = narrow(barBase(labels, null), labels)
  options.scales.x.title = { display: true, text: 'messages per request', font: { size: 10 } }
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = rows[c.dataIndex]
          return c.datasetIndex === 0
            ? ` push: ${fmt.n(r.push)} messages in ${fmt.n(r.pushReq)} requests`
            : ` ack: ${fmt.n(r.ack + r.ackf)} in ${fmt.n(r.ackReq)} requests`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = rows[i]
        return `push ${r.pushBatch ?? '—'} · ack ${r.ackBatch ?? '—'}`
      },
    },
  }
  mount('batch', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'push', data: rows.map((r) => r.pushBatch), backgroundColor: alpha(chartPalette[0].line, 0.85), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        { label: 'ack', data: rows.map((r) => r.ackBatch), backgroundColor: alpha(chartPalette[1].line, 0.85), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
      ],
    },
    options,
  })
}

function renderBudget() {
  const rows = budgetRows.value
  if (!rows.length) return
  const labels = rows.map((r) => r.queue)
  const pctOf = (r, k) => Math.round((r.lagBudget[k] / r.lagBudget.buckets) * 1000) / 10
  const options = narrow(barBase(labels, null), labels)
  options.scales.x.stacked = true
  options.scales.y.stacked = true
  options.scales.x.max = 100
  options.scales.x.ticks.callback = (v) => `${v}%`
  options.plugins = {
    legend: { display: true, position: 'top', align: 'end' },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = rows[c.dataIndex]
          const k = ['under10s', 'from10to60', 'over60s'][c.datasetIndex]
          return ` ${c.dataset.label}: ${fmt.n(r.lagBudget[k])} of ${fmt.n(r.lagBudget.buckets)} active buckets`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const b = rows[i].lagBudget
        return b.over60s ? `${fmt.pct(b.over60s / b.buckets)} over 60s`
          : b.from10to60 ? `${fmt.pct(b.from10to60 / b.buckets)} over 10s` : 'all under 10s'
      },
    },
  }
  mount('budget', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'under 10s', data: rows.map((r) => pctOf(r, 'under10s')), backgroundColor: alpha(chartPalette[1].line, 0.85), borderRadius: 2 },
        { label: '10 to 60s', data: rows.map((r) => pctOf(r, 'from10to60')), backgroundColor: alpha(semanticColors.warn.line, 0.85), borderRadius: 2 },
        { label: '60s and over', data: rows.map((r) => pctOf(r, 'over60s')), backgroundColor: alpha(semanticColors.bad.line, 0.9), borderRadius: 2 },
      ],
    },
    options,
  })
}

function renderRet() {
  const rows = retentionRows.value
  if (!rows.length) return
  const labels = rows.map((r) => r.key)
  const options = narrow(barBase(labels, null), labels)
  options.scales.x.stacked = true
  options.scales.y.stacked = true
  options.scales.x.ticks.callback = (v) => fmt.k(v)
  options.plugins = {
    legend: { display: true, position: 'top', align: 'end' },
    tooltip: { callbacks: { label: (c) => ` ${c.dataset.label}: ${fmt.n(c.parsed.x)}` } },
    endLabels: {
      format: (i) => {
        const r = rows[i]
        return fmt.k(r.retention + r.completed + r.eviction) + (r.eviction ? ` · ${fmt.n(r.eviction)} evicted` : '')
      },
    },
  }
  mount('ret', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'age retention', data: rows.map((r) => r.retention), backgroundColor: alpha(chartPalette[1].line, 0.85), borderRadius: 2 },
        { label: 'completed', data: rows.map((r) => r.completed), backgroundColor: alpha(chartPalette[2].line, 0.85), borderRadius: 2 },
        // Evicted is loss by policy, so it is the only ember on the card.
        { label: 'evicted', data: rows.map((r) => r.eviction), backgroundColor: alpha(semanticColors.bad.line, 0.9), borderRadius: 2 },
      ],
    },
    options,
  })
}

function renderDlqSig() {
  const rows = dlqSigRows.value
  if (!rows.length) return
  const sample = dlqSig.value?.sample || 0
  const labels = rows.map((r) => r.text)
  const options = barBase(labels, null)
  options.scales.x.max = 100
  options.scales.x.ticks.callback = (v) => `${v}%`
  options.scales.y.ticks.font = { size: 10.5 }
  options.scales.y.ticks.callback = (v, i) => (labels[i].length > 72 ? labels[i].slice(0, 71) + '…' : labels[i])
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        title: (items) => rows[items[0].dataIndex].text,
        label: (c) => ` ${fmt.n(rows[c.dataIndex].n)} of ${fmt.n(sample)} sampled`,
      },
    },
    endLabels: { format: (i) => `${fmt.n(rows[i].n)} of ${fmt.n(sample)}` },
  }
  mount('dlqsig', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'share',
        data: rows.map((r) => r.share * 100),
        backgroundColor: alpha(semanticColors.warn.line, 0.85),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

function renderParts() {
  const rows = partitionRows.value
  if (!rows.length) return
  const labels = rows.map((r) => r.queue)
  const options = narrow(barBase(labels, null), labels)
  options.scales.x.ticks.callback = (v) => fmt.k(v)
  options.plugins = {
    legend: { display: true, position: 'top', align: 'end' },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = rows[c.dataIndex]
          return c.datasetIndex === 0
            ? ` ${fmt.n(r.partitions)} partitions · ${fmt.n(r.live7d)} written to in 7d`
            : ` ${fmt.n(r.live24h)} written to in 24h · created ${fmt.n(r.created24h)}`
              + (r.deleted === null ? '' : ` · deleted ${fmt.n(r.deleted)} in the 24h window`)
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = rows[i]
        // Deletions are not on this route: they are the 24h window's own
        // counter, so they are blank for a queue with no metrics row in it.
        return `${fmt.pct(r.partitions ? r.live24h / r.partitions : 0)} live · `
          + `+${fmt.k(r.created)}${r.deleted === null ? '' : ` / −${fmt.k(r.deleted)}`}`
      },
    },
  }
  mount('parts', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'total', data: rows.map((r) => r.partitions), backgroundColor: alpha(chartPalette[2].line, 0.6), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        { label: 'live 24h', data: rows.map((r) => r.live24h), backgroundColor: alpha(chartPalette[0].line, 0.85), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
      ],
    },
    options,
  })
}

function renderBytes() {
  const rows = byteRows.value
  if (!rows.length) return
  const labels = rows.map((r) => r.queue)
  const options = narrow(barBase(labels, null), labels)
  options.scales.x.ticks.callback = (v) => fmt.bytes(v)
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = rows[c.dataIndex]
          return ` ${fmt.bytes(r.retainedBytes)} retained · ${fmt.n(r.messages)} messages`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = rows[i]
        return `${fmt.bytes(r.retainedBytes)} · ${fmt.bytes(r.retainedBytes / r.messages)}/msg`
      },
    },
  }
  mount('bytes', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'bytes per message',
        data: rows.map((r) => r.retainedBytes / r.messages),
        backgroundColor: alpha(chartPalette[1].line, 0.85),
        borderRadius: 2, barPercentage: 0.7,
      }],
    },
    options,
  })
}

async function renderAll() {
  await nextTick()
  applyDefaults()
  if (!payload.value || workload.failed.value) {
    for (const id of Object.keys(charts)) { charts[id].destroy(); delete charts[id] }
    return
  }
  renderFlow()
  renderShare()
  renderPartsBy()
  renderStructure()
  renderRetained()
  renderChurn()
  renderPending()
  if (!groupsFailed.value) renderOldest()
  renderDlq()
  renderFill()
  renderAck()
  renderLag()
  renderGroups()
  renderDeeper()
}

/** The deeper charts. Each one draws only when its own panel has data. */
function renderDeeper() {
  if (!deepWeek.failed.value) renderBase()
  if (!deepEff.failed.value) { renderFan(); renderAckd(); renderBatch(); renderBudget() }
  if (!retention.failed.value) renderRet()
  if (!dlqSignatures.failed.value) renderDlqSig()
  if (!partitions.failed.value) renderParts()
  if (!deepEff.failed.value) renderBytes()
}

watch(
  [rows, prevPayload, metric, themeVersion, selectedQueue, groupsFailed],
  () => { renderAll() },
  { immediate: true },
)

// The deeper panels land on their own schedule, so they repaint on their own
// data rather than waiting for the next workload tick.
watch(
  [baseline, topQueues, budgetRows, retentionRows, dlqSigRows, partitionRows, byteRows, weekRows],
  async () => { await nextTick(); applyDefaults(); renderDeeper() },
)

onUnmounted(() => {
  stopClusterWatch()
  for (const id of Object.keys(charts)) { charts[id].destroy(); delete charts[id] }
})
</script>

<style scoped>
/* Layout rhythm: the same 16px the other views keep between cards. Cards
   inside a grid row take their spacing from the grid, not from themselves. */
.card { margin-bottom: 16px; }
.wl-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
.wl-grid-2 > .card { margin-bottom: 0; }
@media (max-width: 900px) { .wl-grid-2 { grid-template-columns: 1fr; } }

/* Card headers carry a title, a subtitle, one or two chips and the stamp; let
   them wrap on a narrow column instead of clipping. */
.card-header { flex-wrap: wrap; row-gap: 4px; }
.card-header .card-sub { white-space: normal; }
.card-body { padding: 14px 16px 16px; }

.wl-chart { position: relative; width: 100%; }

/* Counts strip tones (the strip itself is global). */
.wl-warn { color: var(--warn-400) !important; }
.count-delta { font-style: normal; font-family: var(--font-mono); font-size: 11px; color: var(--text-low); margin-left: 6px; }
.wl-bad { color: var(--ember-400) !important; }

/* Findings: dot · sentence · evidence, one hairline per row. */
.wl-summary { font-size: 12.5px; color: var(--text-mid); margin: 0 0 10px; line-height: 1.5; }
.wl-findings { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; }
.wl-findings > li { border-bottom: 1px solid var(--bd-soft, var(--bd)); }
.wl-findings > li:last-child { border-bottom: none; }
.finding {
  display: grid; grid-template-columns: 10px minmax(0, 1fr) auto; align-items: baseline; gap: 4px 10px;
  width: 100%; text-align: left; background: none; border: none; cursor: pointer;
  padding: 8px 6px; border-radius: var(--r-chip, 4px); font-family: inherit;
}
/* The evidence sits on the right and never takes more than a third of the row
   from the sentence; on a narrow card it drops under the sentence instead. */
.wl-findings { container-type: inline-size; }
.finding .fev { justify-self: end; text-align: right; max-width: 52ch; }
@container (max-width: 640px) {
  .finding { grid-template-columns: 10px minmax(0, 1fr); }
  .finding .fev { grid-column: 2; justify-self: start; text-align: left; max-width: none; }
}
.finding:hover { background: color-mix(in srgb, var(--text-hi) 2.5%, transparent); }
.fdot { width: 6px; height: 6px; border-radius: 50%; background: var(--bd-hi); position: relative; top: -1px; }
.sev-bad .fdot { background: var(--ember-400); box-shadow: 0 0 8px color-mix(in srgb, var(--ember-500) 55%, transparent); }
.sev-warn .fdot { background: var(--warn-400); }
.sev-ice .fdot { background: var(--ice-400); }
.ftext { font-size: 12.5px; color: var(--text-hi); line-height: 1.45; }
.fev { font-size: 11px; color: var(--text-low); font-variant-numeric: tabular-nums; line-height: 1.45; }
/* The deeper findings carry evidence that is a list (queues, shares), too
   long to sit beside the sentence: stack it under the text instead. */
.finding-stack { grid-template-columns: 10px 1fr; }
.finding-stack .fev { grid-column: 2; justify-self: start; text-align: left; max-width: none; overflow-wrap: anywhere; }
.empty { padding: 22px 0; text-align: center; font-size: 12.5px; color: var(--text-low); }

/* Focus breadcrumb in the filter card. */
.wl-crumbs { display: inline-flex; align-items: center; gap: 6px; font-family: var(--font-mono); font-size: 12px; }
.crumb {
  background: none; border: none; padding: 2px 4px; font-size: 12px; font-family: var(--font-mono);
  color: var(--text-hi); cursor: pointer; border-radius: var(--r-chip, 4px);
  text-decoration: underline; text-decoration-color: var(--text-faint); text-underline-offset: 3px;
}
.crumb:disabled { color: var(--text-mid); cursor: default; text-decoration: none; }
.crumb:not(:disabled):hover { background: var(--ink-3); }
.crumb-sep { color: var(--text-faint); }
.seg-sm button { font-size: 11px; padding: 2px 8px; }

/* Section label between the two layers: the app's uppercase label, a rule, a note. */
.wl-eyebrow { display: flex; align-items: center; gap: 12px; margin: 28px 2px 14px; }
.eyebrow-text {
  font-family: var(--font-mono); font-size: 10.5px; text-transform: uppercase; letter-spacing: .12em;
  color: var(--text-mid); font-weight: 500;
}
.eyebrow-rule { height: 1px; background: var(--bd); flex: 0 0 48px; }
.eyebrow-sub { font-size: 11.5px; color: var(--text-low); }

/* Pills are the app's segmented control, one level smaller. */
.wl-pills {
  display: inline-flex; flex-wrap: wrap; gap: 1px; padding: 2px; margin: 0 0 12px;
  border-radius: var(--r-control); border: 1px solid var(--bd); background: var(--ink-2);
}
.pill {
  background: transparent; border: none; color: var(--text-mid); font-family: inherit;
  font-size: 11.5px; font-weight: 500; padding: 3px 10px; border-radius: var(--r-chip); cursor: pointer;
}
.pill:hover { color: var(--text-hi); }
.pill.on { color: var(--text-hi); background: var(--ink-5); }
.wl-note { font-size: 11.5px; color: var(--text-low); margin: 10px 0 0; }
</style>
