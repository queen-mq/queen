<template>
  <SubSidebar :label="groupBy === 'task' ? 'Tasks' : 'Namespaces'" :current="scopeTitle">
  <!-- The tenant and its namespaces (or tasks), one line each, always in
       sight. A click calls the page's own drill-down, so it refetches exactly
       as a chart click does; a group's queues are on the right. -->
  <template #side>
    <WorkloadSide
      :group-by="groupBy"
      :focus="focus"
      :keys="focusKeys"
      :groups="consumerGroups.data.value ? groupRows : null"
      @group-by="selectGroupBy"
      @root="goRoot"
      @focus="focusNode"
    />
  </template>
  <div class="view-container">

    <PageHead :title="scopeTitle" :live="refreshAgo">
      <template #title><span :title="scopeTitle">{{ scopeTitle }}</span></template>
      <template v-if="headFacts.length" #sub>
        <span class="wl-facts">
          <span v-for="f in headFacts" :key="f.k" class="wl-fact" :title="f.title || undefined">
            <span class="wl-fact-k">{{ f.k }}</span><span class="wl-fact-v">{{ f.v }}</span>
            <button v-if="f.clear" type="button" class="wl-fact-x" :aria-label="f.clearLabel" @click="f.clear"><Icon name="x" :size="12" :stroke="2" /></button>
          </span>
        </span>
      </template>
      <template #range>
        <div class="seg wl-range" role="group" aria-label="Time range">
          <button
            v-for="r in timeRanges"
            :key="r.value"
            type="button"
            :class="{ on: selectedRange === r.value && !customMode }"
            :aria-pressed="selectedRange === r.value && !customMode"
            :title="r.longLabel"
            @click="selectQuickRange(r.value)"
          >{{ r.label }}</button>
          <button type="button" :class="{ on: customMode }" :aria-pressed="customMode" @click="toggleCustomMode">Custom</button>
        </div>
      </template>
      <!-- A node that did not answer the broker's gather took its counts out
           of every number below: said once, here, for the whole page. -->
      <template v-if="partial" #actions>
        <span class="chip chip-warn" role="status" :title="PARTIAL_TITLE"><span class="dot" />{{ partial }}</span>
      </template>
    </PageHead>

    <!-- Group by and the pick are the list's (WorkloadSide). Drilling in is
         still a REFETCH with groupBy=queue and the namespace/task as a
         filter, not a client-side slice. -->
    <PageTools>
      <template #view>
        <UiSelect
          :model-value="compare"
          label="Compare"
          :options="compareModes"
          aria-label="Compare with another window"
          @update:model-value="selectCompare"
        />
      </template>
    </PageTools>

    <!-- A window of your own: the only range that waits for Apply, because a
         half-typed date must not re-scope the page. -->
    <div v-if="customMode" class="page-tools wl-custom">
      <label class="tool-field">
        <span class="tool-label">From</span>
        <input v-model="customFrom" type="datetime-local" class="input" :title="formatTimestampUtc(customFrom)" />
      </label>
      <label class="tool-field">
        <span class="tool-label">To</span>
        <input v-model="customTo" type="datetime-local" class="input" :title="formatTimestampUtc(customTo)" />
      </label>
      <button type="button" class="btn btn-primary" :disabled="!customRangeValid" @click="applyCustomRange">Apply</button>
      <span v-if="customError" class="tool-note is-bad">{{ customError }}</span>
    </div>

    <!-- One line for a failed read, here, rather than in every card: the
         cards keep the last read that loaded, or say they have none. -->
    <div v-if="workload.failed.value" class="status-banner banner-bad view-banner wl-block" role="status">
      <span>
        <strong>Could not load the workload</strong> · {{ workloadErrorText }}<template v-if="loaded && lastLoadedText"> · showing the read of {{ lastLoadedText }}</template>
      </span>
    </div>

    <!-- Counts. The tenant totals, whatever the rows below are filtered to:
         the Overview's count tiles, in two rows. The first is what there is
         right now; the second is what the window added up to, and is the only
         one the range picker moves. -->
    <div v-if="!tenant && !workload.failed.value" class="skeleton wl-block" style="height: 132px" />
    <div v-else-if="tenant" class="wl-counts wl-block">
      <div class="wl-counts-row" role="group" :aria-label="nowHead">
        <span class="wl-counts-head">{{ nowHead }}</span>
        <div class="count-tile"><span class="k">Queues</span><span class="v">{{ fmt.n(tenant.queues) }}</span></div>
        <div class="count-tile"><span class="k">Consumer groups</span><span class="v">{{ fmt.n(tenant.now.groups) }}</span></div>
        <div class="count-tile"><span class="k">Partitions</span><span class="v">{{ fmt.n(tenant.now.partitions) }}</span></div>
        <div class="count-tile"><span class="k">Pending</span><span class="v" :class="pendingTone">{{ fmt.n(tenant.now.pending) }}</span></div>
        <div class="count-tile"><span class="k">In flight</span><span class="v">{{ fmt.n(tenant.now.processing) }}</span></div>
        <!-- Depth, not growth: nothing purges a dead-letter queue, so `> 0` is
             permanent and says nothing about this window. -->
        <div class="count-tile"><span class="k">In DLQ</span><span class="v">{{ fmt.n(tenant.now.deadLetter) }}</span></div>
        <div class="count-tile"><span class="k">Retained</span><span class="v">{{ fmt.bytes(tenant.now.retainedBytes) }}</span></div>
      </div>
      <div class="wl-counts-row" role="group" :aria-label="windowHead">
        <span class="wl-counts-head" :title="rangeUtcTitle || undefined">{{ windowHead }}<template v-if="deltas"> <br /><span class="wl-vs">vs {{ compareLabel }}</span></template></span>
        <div class="count-tile"><span class="k">Pushed</span><span class="v">{{ fmt.n(tenant.window.pushMessages) }}<em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.pushMessages.pct) }}</em></span></div>
        <div class="count-tile"><span class="k">Delivered</span><span class="v">{{ fmt.n(tenant.window.popMessages) }}<em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.popMessages.pct) }}</em></span></div>
        <!-- Its own tile, never added to Delivered: see FETCHED_TITLE. -->
        <div class="count-tile" :title="FETCHED_TITLE"><span class="k">Fetched (Kafka)</span><span class="v">{{ fmt.n(tenant.window.fetchMessages) }}<em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.fetchMessages.pct) }}</em></span></div>
        <div class="count-tile"><span class="k">Acked</span><span class="v">{{ fmt.n(tenant.window.ackSuccess) }}<em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.ackSuccess.pct) }}</em></span></div>
        <div class="count-tile"><span class="k">Ack failures</span><span class="v" :class="ackFailedTone">{{ fmt.n(tenant.window.ackFailed) }}<em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.ackFailed.pct) }}</em></span></div>
        <div class="count-tile"><span class="k">Empty polls</span><span class="v">{{ fmt.n(tenant.window.popEmpty) }}<em v-if="deltas" class="count-delta">{{ fmt.delta(deltas.popEmpty.pct) }}</em></span></div>
        <div class="count-tile" title="Deliveries per poll in the window"><span class="k">Fill</span><span class="v">{{ fmt.pct(tenantFill) }}</span></div>
      </div>
    </div>

    <!-- Work over time -->
    <div class="card">
      <div class="card-header">
        <h3>{{ `Work over time by ${levelWord}` }}</h3>
        <span class="card-sub">stacked, {{ bucketMinutes }}-minute buckets</span>
        <UiSelect v-model="metric" class="wl-card-select" size="sm" :options="metrics" aria-label="Measure" />
        <span class="muted">{{ rangeLabel }}</span>
      </div>
      <div class="card-body">
        <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
        <div v-else class="wl-chart" style="height: 280px">
          <canvas :ref="(el) => setCanvas('flow', el)" />
        </div>
      </div>
    </div>

    <!-- Share + activity map -->
    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>{{ `Share of the work by ${levelWord}` }}</h3>
          <span class="card-sub" :title="`Delivered and fetched (Kafka), with pushed under it; the share is of the tenant's reads. ${FETCHED_TITLE}`">delivered, then pushed</span>
          <span class="muted">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length * (prevPayload ? 1.5 : 1.15)) }">
            <canvas :ref="(el) => setCanvas('share', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>{{ `Activity map by ${levelWord}` }}</h3>
          <span class="card-sub" :title="`The strongest cell is ${fmt.n(heat.max)} deliveries in ${bucketMinutes} minutes`">deliveries per bucket</span>
          <span class="muted">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!heatRows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
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
          <span class="card-sub" title="Live partitions; the label beside a bar adds partitions per queue">live partitions</span>
          <span class="muted">now</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('partsBy', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>{{ `Queues and consumer groups by ${levelWord}` }}</h3>
          <span class="card-sub" title="Queues, with the consumer groups attached to them under">queues, then groups</span>
          <span class="muted">now</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
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
          <span class="card-sub">on disk</span>
          <span class="muted">now</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('retained', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>{{ `Partition churn by ${levelWord}` }}</h3>
          <span class="card-sub" title="Partitions created, then deleted, in the window">created, then deleted</span>
          <span class="muted">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
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
          <span class="card-sub" title="Amber is pending on queues no consumer group reads, unless Settings says none is expected">amber: no consumer group</span>
          <span class="muted">now</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('pending', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Oldest waiting</h3>
          <span class="card-sub" title="The worst consumer-group time lag of each, coloured by that queue's lag lines">worst group lag</span>
          <span class="muted">now</span>
        </div>
        <div class="card-body">
          <!-- This panel is the consumer-groups list, not the workload
               endpoint: with that list down there is no honest bar to draw. -->
          <div v-if="groupsFailed" class="panel-err">{{ groupsErrorText }}</div>
          <div v-else-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
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
          <span class="card-sub">dead-letter rows</span>
          <span class="muted">now</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('dlq', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Fill</h3>
          <span class="card-sub" title="Deliveries per poll; blank under 5 polls. A Kafka consumer fetches and never polls.">deliveries per poll</span>
          <span class="muted">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
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
          <span class="muted">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('ack', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Lag at pop</h3>
          <span class="card-sub" title="From the average to the worst, on a log scale">average to worst</span>
          <span class="muted">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
          <div v-else-if="!rows.length" class="empty"><Icon name="chart-column" :size="16" />No queue in this scope</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(rows.length) }">
            <canvas :ref="(el) => setCanvas('lag', el)" />
          </div>
        </div>
      </div>
    </div>

    <!-- Consumer groups of the clicked queue -->
    <div v-if="selectedQueue" ref="groupsCard" class="card wl-groups-card">
      <div class="card-header">
        <h3 :title="selectedQueue">Consumer groups of {{ selectedQueue }}</h3>
        <span class="card-sub">{{ groupsSub }}</span>
        <div class="wl-card-actions">
          <RouterLink class="btn btn-ghost" :to="queueLocation(selectedQueue, route, 'overview', windowQuery(currentRange()))">Open queue</RouterLink>
          <RouterLink class="btn btn-ghost" :to="queueLocation(selectedQueue, route, 'consumers', windowQuery(currentRange()))">Consumer groups</RouterLink>
          <button type="button" class="btn btn-ghost btn-icon" aria-label="Close the queue" title="Close" @click="selectedQueue = null"><Icon name="x" :size="15" /></button>
        </div>
      </div>
      <div class="card-body">
        <div v-if="groupsFailed" class="panel-err">{{ groupsErrorText }}</div>
        <div v-else-if="!queueGroups.length" class="empty"><Icon name="users-round" :size="16" />No consumer group has ever read this queue</div>
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
      <span class="eyebrow-sub" title="A 7-day profile and a 24-hour queue-level read, loaded on open and on a range or group-by change, not on every refresh">7 days and 24 hours, read on open</span>
      <!-- Its own reads, at their own time: a node missing from them is said
           here, not by the page's chip, which speaks for the panels above. -->
      <span v-if="deeperPartial" class="chip chip-warn" role="status" :title="PARTIAL_TITLE"><span class="dot" />{{ deeperPartial }}</span>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Weekly profile</h3>
        <span class="card-sub">{{ weekSub }}</span>
        <UiSelect v-if="week && weekPills.length > 1" v-model="weekPick" class="wl-card-select" size="sm" label="Show" :options="weekPills" aria-label="Whose week" />
        <span class="muted">last 7d</span>
      </div>
      <div class="card-body">
        <div v-if="deepWeek.failed.value" :class="deeperErrorClass(deepWeek)">{{ deeperErrorText(deepWeek) }}</div>
        <div v-else-if="!week" class="empty">Loading…</div>
        <template v-else>
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
          <Icon v-if="!deepWeek.loading.value" name="chart-column" :size="16" />{{ deepWeek.loading.value ? 'Loading…' : 'No deliveries in the last 7 days to compare' }}
        </div>
        <div v-else class="wl-chart" :style="{ height: chartHeight(baseline.rows.length) }">
          <canvas :ref="(el) => setCanvas('base', el)" />
        </div>
      </div>
    </div>

    <div class="wl-grid-2">
      <div class="card">
        <div class="card-header">
          <h3>Reads per pushed message</h3>
          <span class="card-sub" :title="`Deliveries and Kafka fetches, for the 12 queues with the most reads. ${FETCHED_TITLE}`">top 12 queues by reads</span>
          <span class="muted">last 24h</span>
        </div>
        <div class="card-body">
          <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
          <div v-else-if="!topQueues.length" class="empty"><Icon v-if="!deepEff.loading.value" name="chart-column" :size="16" />{{ deepEff.loading.value ? 'Loading…' : 'No deliveries in the last 24h' }}</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(topQueues.length) }">
            <canvas :ref="(el) => setCanvas('fan', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Acks per delivery</h3>
          <span class="card-sub" title="Acks made inside a /transaction are not attributed to a queue">top 12 queues by reads</span>
          <span class="muted">last 24h</span>
        </div>
        <div class="card-body">
          <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
          <div v-else-if="!topQueues.length" class="empty"><Icon v-if="!deepEff.loading.value" name="chart-column" :size="16" />{{ deepEff.loading.value ? 'Loading…' : 'No deliveries in the last 24h' }}</div>
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
          <span class="muted">last 24h</span>
        </div>
        <div class="card-body">
          <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
          <div v-else-if="!topQueues.length" class="empty"><Icon v-if="!deepEff.loading.value" name="chart-column" :size="16" />{{ deepEff.loading.value ? 'Loading…' : 'No deliveries in the last 24h' }}</div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(topQueues.length * 1.15) }">
            <canvas :ref="(el) => setCanvas('batch', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Lag budget</h3>
          <span class="card-sub" title="The share of each queue's active buckets, by the worst lag at pop in the bucket">active buckets by lag</span>
          <span class="muted">last 24h</span>
        </div>
        <div class="card-body">
          <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
          <div v-else-if="!budgetRows.length" class="empty">
            <Icon v-if="!deepEff.loading.value" name="chart-column" :size="16" />{{ deepEff.loading.value ? 'Loading…' : 'No lag sampled in the last 24h' }}
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
          <span class="card-sub" title="Per queue. Evicted: older than the max wait, never delivered.">per queue</span>
          <span class="muted">{{ rangeLabel }}</span>
        </div>
        <div class="card-body">
          <div v-if="retention.failed.value" :class="deeperErrorClass(retention)">{{ deeperErrorText(retention) }}</div>
          <div v-else-if="retentionUngrouped" class="empty" title="This broker answers retention totals without the per-queue split">
            Not available on this broker
          </div>
          <div v-else-if="!retentionRows.length" class="empty">
            <Icon v-if="!retention.loading.value" name="chart-column" :size="16" />{{ retention.loading.value ? 'Loading…' : 'Nothing retained or evicted in this window' }}
          </div>
          <div v-else class="wl-chart" :style="{ height: chartHeight(retentionRows.length) }">
            <canvas :ref="(el) => setCanvas('ret', el)" />
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <h3>Partitions alive</h3>
          <span class="card-sub" title="Every partition, and those written to in the last 24 hours">total, written in 24h</span>
          <span class="muted">now</span>
        </div>
        <div class="card-body">
          <div v-if="partitions.failed.value" :class="deeperErrorClass(partitions)">{{ deeperErrorText(partitions) }}</div>
          <div v-else-if="!partitionRows.length" class="empty"><Icon v-if="!partitions.loading.value" name="chart-column" :size="16" />{{ partitions.loading.value ? 'Loading…' : 'No partition reported' }}</div>
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
        <span class="card-sub" :title="dlqTitle">{{ dlqSub }}</span>
        <UiSelect v-if="dlqPills.length > 1" :model-value="dlqPick" class="wl-card-select" size="sm" label="Queue" :options="dlqOptions" aria-label="Dead-letter queue" @update:model-value="pickDlq" />
        <span class="muted">now</span>
      </div>
      <div class="card-body">
        <div v-if="!dlqPills.length" class="empty"><Icon name="archive-x" :size="16" />No dead-letter rows right now</div>
        <template v-else>
          <div v-if="dlqSignatures.failed.value" :class="deeperErrorClass(dlqSignatures)">{{ deeperErrorText(dlqSignatures) }}</div>
          <div v-else-if="!dlqSigRows.length" class="empty">
            <Icon v-if="!dlqSignatures.loading.value" name="archive-x" :size="16" />{{ dlqSignatures.loading.value ? 'Loading…' : 'No error message in the sample' }}
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
        <span class="card-sub" title="Retained bytes per message, for the 10 queues that retain the most">bytes per message, top 10</span>
        <span class="muted">now</span>
      </div>
      <div class="card-body">
        <div v-if="deepEff.failed.value" :class="deeperErrorClass(deepEff)">{{ deeperErrorText(deepEff) }}</div>
        <div v-else-if="!byteRows.length" class="empty">
          <Icon v-if="!deepEff.loading.value" name="hard-drive" :size="16" />{{ deepEff.loading.value ? 'Loading…' : 'No queue reports both bytes and a message count' }}
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
      <span class="eyebrow-sub" title="What the panels above add up to; a row in the first list drills into it">what the panels add up to</span>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>What needs attention</h3>
        <span class="muted">{{ rangeLabel }}</span>
      </div>
      <div class="card-body">
        <div v-if="!loaded" class="empty"><Icon v-if="workload.failed.value" name="circle-x" :size="16" />{{ notLoaded }}</div>
        <template v-else>
          <p class="wl-summary">{{ summary }}</p>
          <div v-if="groupsFailed" class="panel-err" :title="groupsErrorText">
            Consumer groups unavailable: the waiting and no-group rules are off.
          </div>
          <ul class="wl-findings">
            <li v-for="(f, i) in attention" :key="i">
              <button type="button" class="finding" :class="`sev-${f.sev}`" @click="drill(f.row)">
                <span class="g" :class="findingGlyph(f.sev)" aria-hidden="true" />
                <span class="ftext">{{ f.text }}</span>
                <span class="fev">{{ f.evidence }}</span>
              </button>
            </li>
            <li v-if="!attention.length" class="empty"><Icon name="circle-check" :size="16" />Nothing needs attention in this window</li>
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
            <div class="finding finding-stack" :class="`sev-${f.sev}`">
              <span class="g" :class="findingGlyph(f.sev)" aria-hidden="true" />
              <span class="ftext">{{ f.text }}</span>
              <span class="fev">{{ f.evidence }}</span>
            </div>
          </li>
          <li v-if="!deepAttention.length" class="empty"><Icon v-if="!deepEff.loading.value" name="circle-check" :size="16" />{{ deepEff.loading.value ? 'Loading…' : 'Nothing to add from the deeper reads' }}</li>
        </ul>
      </div>
    </div>

  </div>
  </SubSidebar>
</template>

<script setup>
import { useRoute } from 'vue-router'
import { useRouteState } from '@/composables/useRouteState'
import { useRouteRange } from '@/composables/useRouteRange'
import { DEFAULT_TIME_RANGE, EXTENDED_TIME_RANGES as timeRanges, rangeSeconds, relativeTimeRange } from '../../../shared/timeRanges.mjs'
import { queueLocation, windowQuery } from '@/composables/navigation'
const route = useRoute()
import { computed, nextTick, onUnmounted, ref, watch } from 'vue'
import {
  BarController, BarElement, CategoryScale, Chart, Filler, Legend, LineController,
  LineElement, LinearScale, LogarithmicScale, PointElement, Tooltip,
} from 'chart.js'

import Heatmap from '@/components/Heatmap.vue'
import Icon from '@/components/Icon.vue'
import UiSelect from '@/components/UiSelect.vue'
import { analytics, consumers, describeApiError, system } from '@/api'
import { useApi } from '@/composables/useApi'
import { alpha, chartPalette, chartTheme, legendLabels, semanticColors, themeVersion } from '@/composables/useChartTheme'
import { ackFailureSeverity, backlogSeverity, groupActivity } from '@/composables/useSeverity'
import { byLagInactiveLast } from '@/composables/useAttention'
import {
  formatDateTimeLocal, formatTimestamp, formatTimestampRange, formatTimestampRangeUtc, formatTimestampUtc,
  validateRange,
} from '@/composables/useFormat'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import PageHead from '@/components/PageHead.vue'
import PageTools from '@/components/PageTools.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import WorkloadSide from '@/components/WorkloadSide.vue'
import { groupName, rootName } from '@/composables/workloadSide'
import { stamp } from '@/composables/useStamp'
import {
  comparisonRange, deeperFindings, efficiency, enrichRows, findings, flowSeries, formatters as fmt,
  heatCells, readsOf, rollupFromQueueOps, sameHourBaseline, tenantTotals, totalSeries, trimOpenBucket,
  weeklyProfile, windowDeltas,
} from '@/composables/useWorkload'
import { missingNodes, partialText, PARTIAL_TITLE } from '@/composables/usePartial'
import { onClusterChange } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { formatSpan } from '@/composables/settingsDoc'
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
const selectedRange = ref(DEFAULT_TIME_RANGE)
const customMode = ref(false)
const customFrom = ref('')
const customTo = ref('')
const appliedCustom = ref(null)
useRouteRange({ range: selectedRange, customMode, customFrom, customTo, appliedCustom, reload: () => { fetchWorkload(); fetchDeeper() } })

const groupBy = ref('namespace')
const focus = ref(null)
const selectedQueue = ref(null)
const metric = ref('pop')
const { restoring: restoringRoute } = useRouteState({ groupBy, focus, selectedQueue, metric })
watch([groupBy, focus], () => { if (restoringRoute.value) fetchWorkload() })

const metrics = [
  { label: 'Delivered', value: 'pop' },
  { label: 'Fetched (Kafka)', value: 'fetch' },
  { label: 'Pushed', value: 'push' },
  { label: 'Empty polls', value: 'popEmpty' },
  { label: 'Ack failures', value: 'ackFailed' },
  { label: 'Parked', value: 'parked' },
]
const METRIC_LABEL = {
  pop: 'deliveries', fetch: 'Kafka-fetched records', push: 'pushes', popEmpty: 'empty polls',
  ackFailed: 'ack failures', parked: 'parked long-polls',
}
// A Kafka consumer reads by fetch, which the broker counts apart from pops
// (useWorkload's header): said wherever the page shows it, because the number
// is honest only with its two caveats.
const FETCHED_TITLE = 'Records Kafka clients fetched through the broker\'s Kafka facade. A fetch is a '
  + 'read, not a delivery: it takes no lease and nothing acks it, a client that seeks back reads '
  + 'again, and two Kafka consumer groups reading one topic count every record twice.'

function currentRange() {
  if (customMode.value && appliedCustom.value) return appliedCustom.value
  return relativeTimeRange(selectedRange.value)
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
    const { from, to } = relativeTimeRange(selectedRange.value)
    customTo.value = formatDateTimeLocal(to)
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
  { value: 'off', label: 'Off' },
  { value: 'previous', label: 'Previous period' },
  { value: 'yesterday', label: 'Same time yesterday' },
  { value: 'lastWeek', label: 'Same time last week' },
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
// The live tick counts from the last load that succeeded.
const refreshAgo = useRefreshAgo(workload.lastUpdated)

// A different cluster can be a different cell running a different broker, so
// the verdict resets with it — exactly as the tenant-keyed stores do.
const stopClusterWatch = onClusterChange(() => {
  fallbackMode.value = routeSupport.missing('workload') !== null
  fetchDeeper()
})

// A read that loaded at least once: the cards keep it through a failed
// refresh, and the banner over them says how old it is.
const loaded = computed(() => Boolean(workload.data.value))
const notLoaded = computed(() => (workload.failed.value ? 'Not loaded' : 'Loading…'))
const workloadErrorText = computed(() => describeApiError(workload.error.value))
const lastLoadedText = computed(() => (workload.lastUpdated.value ? formatTimestamp(workload.lastUpdated.value) : ''))
const groupsFailed = computed(() => consumerGroups.failed.value)
const groupsErrorText = computed(() => describeApiError(consumerGroups.error.value))
// The nodes the broker's gather lacked (usePartial), for this window and the
// one it is compared with: both feed the panels down to the Deeper eyebrow.
// The deeper reads (7d, 24h) are fetched on their own schedule, so they carry
// a chip of their own on that eyebrow (`deeperPartial`, defined with them).
const partial = computed(() => partialText(missingNodes(
  workload.data.value, compare.value === 'off' ? null : workloadPrev.data.value,
)))

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
  for (const [name, q] of queuesStore.queueMeta.value) {
    m.set(name, { namespace: q.namespace, task: q.task, kafkaTopic: q.kafkaTopic === true })
  }
  return m
})

const payload = computed(() => trimOpenBucket(workload.data.value))
// A broker with no queues answers `tenant: {}`: zeros on the counts strip.
const tenant = computed(() => tenantTotals(payload.value))
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
const { lines: tenantLines, linesFor } = useSettingsStore()
// The two lag lines, drawn across a lag chart. "Oldest waiting" draws the
// tenant's: one of its bars may be a queue with lines of its own, and that
// bar's colour follows those, not the two drawn. The chart of one queue's
// groups draws that queue's.
const lagRefLines = (lines) => [
  { x: lines.lagWarnSeconds, label: formatSpan(lines.lagWarnSeconds), tone: 'warn' },
  { x: lines.lagBadSeconds, label: formatSpan(lines.lagBadSeconds), tone: 'bad' },
]
const rows = computed(() => enrichRows(payload.value, groupsByQueue.value, queueMetaMap.value, rowsLevel.value, linesFor))

const prevPayload = computed(() => (compare.value === 'off' ? null : trimOpenBucket(workloadPrev.data.value)))
const prevRows = computed(() => enrichRows(prevPayload.value, groupsByQueue.value, queueMetaMap.value, levelOf(prevPayload.value), linesFor))
const prevByKey = computed(() => new Map(prevRows.value.map((r) => [r.key, r])))
const deltas = computed(() => windowDeltas(tenant.value?.window, prevPayload.value?.tenant?.window))

// Entity → ink. Nothing on this page is coloured by identity: a hue is a
// verdict everywhere else in the console, and five of them on a healthy tenant
// read as five things to look at. The flow chart tells its series apart by
// the grey ramp (and names each one at its right edge); the activity map's
// swatch is that same grey, so a row can be found in the chart above it. The
// four biggest by reads get a step each, in flowSeries' own order; the
// rest share the fifth, as they share "Other". Every bar chart is one ink.
const entityInk = computed(() => {
  const m = new Map()
  ;[...rows.value]
    .sort((a, b) => readsOf(b.window) - readsOf(a.window))
    .slice(0, 4)
    .forEach((r, i) => { m.set(r.key, chartPalette[i].line) })
  return m
})
const colorOf = (key) => entityInk.value.get(key) || chartPalette[4].line
// One measure per row, and two when a row carries a pair.
const barInk = () => alpha(chartPalette[1].line, 0.85)
const barInkLead = () => alpha(chartPalette[0].line, 0.85)
const findingGlyph = (sev) => (sev === 'bad' || sev === 'warn' ? sev : 'ok')

const tenantFill = computed(() => {
  const w = tenant.value?.window
  if (!w) return null
  return (w.popMessages + w.popEmpty) >= 5 ? w.popMessages / (w.popMessages + w.popEmpty) : null
})

const heat = computed(() => (payload.value ? heatCells(rows.value) : { max: 0, rows: [] }))
const heatRows = computed(() => heat.value.rows.map((r) => ({ ...r, color: colorOf(r.key) })))

const attention = computed(() => findings(
  rows.value, level.value, rangeLabel.value, { hasGroups: !groupsFailed.value, linesFor },
))

const summary = computed(() => {
  const total = rows.value.reduce((s, r) => s + r.window.popMessages, 0)
  const fetched = rows.value.reduce((s, r) => s + r.fetched, 0)
  const kafka = fetched ? ` Kafka clients fetched ${fmt.n(fetched)} records besides.` : ''
  if (!total) {
    return fetched
      ? `Nothing was delivered by pop in the ${rangeLabel.value}; Kafka clients fetched ${fmt.n(fetched)} records.`
      : 'Nothing was delivered in this window.'
  }
  const top = [...rows.value]
    .sort((a, b) => b.window.popMessages - a.window.popMessages)
    .filter((r) => r.window.popMessages > 0)
    .slice(0, 3)
  const who = level.value === 'queue' ? `${focusName.value} delivered` : 'the tenant delivered'
  const parts = top.map((r) => `${r.name} ${fmt.pct(r.window.popMessages / total)}`
    + (level.value === 'queue' ? '' : ` on ${fmt.plural(r.now.queuesActive, 'active queue')}`))
  return `In the ${rangeLabel.value} ${who} ${fmt.n(total)} messages: ${parts.join(', ')}.${kafka}`
})

const focusName = computed(() => groupName(focus.value, groupBy.value))

// The header line: the pick (or the tenant), and the facts of that scope from
// the queue list. The counts strip under it is always the tenant's, so it
// says so once a group is picked.
const scopeTitle = computed(() => (focus.value === null ? rootName(groupBy.value) : focusName.value))
const scopeQueues = computed(() => (queuesStore.queues.value || []).filter((q) => q?.name
  && (focus.value === null || ((groupBy.value === 'task' ? q.task : q.namespace) ?? '') === focus.value)))
const headFacts = computed(() => {
  const out = []
  const listed = queuesStore.lastFetched.value > 0 || queuesStore.queues.value.length > 0
  if (listed && focus.value === null) {
    const keys = new Set(scopeQueues.value.map((q) => (groupBy.value === 'task' ? q.task : q.namespace) ?? ''))
    out.push({ k: groupBy.value === 'task' ? 'Tasks' : 'Namespaces', v: fmt.n(keys.size) })
  } else if (listed) {
    const pending = scopeQueues.value.reduce((s, q) => s + (Number(q.messages?.pending) || 0), 0)
    out.push({ k: 'Queues', v: fmt.n(scopeQueues.value.length) }, { k: 'Pending', v: fmt.n(pending) })
  }
  if (selectedQueue.value) {
    out.push({ k: 'Queue', v: selectedQueue.value, title: 'Its consumer groups are under the charts', clear: () => { selectedQueue.value = null }, clearLabel: 'Close the queue' })
  }
  if (customMode.value && appliedCustom.value) out.push({ k: 'Window', v: rangeLabel.value, title: rangeUtcTitle.value })
  if (fallbackMode.value) out.push({ k: 'Computed', v: 'in this browser', title: FALLBACK_TITLE })
  return out
})
const cap = (t) => (t ? t[0].toUpperCase() + t.slice(1) : t)
const nowHead = computed(() => (focus.value === null ? 'Now' : 'Tenant, now'))
const windowHead = computed(() => (focus.value === null ? cap(rangeLabel.value) : `Tenant, ${rangeLabel.value}`))

// The list takes every namespace (or task) from the queue list,
// so a group that is quiet in this window can still be picked; these are the
// keys the window's rows add to it — read only once the rows are at the level
// Group by asks for, not the old level between a switch and its answer.
const focusKeys = computed(() => (
  rowsLevel.value === groupBy.value ? rows.value.map((r) => r.key ?? '') : []
))

// A namespace (or task) clicked in the list: drill into it; clicked again
// with a queue open, back to the group.
function focusNode(v) {
  if (focus.value === v) { selectedQueue.value = null; return }
  focus.value = v
  selectedQueue.value = null
  fetchWorkload()
}

// Worst lag first, inactive Kafka groups last (useAttention byLagInactiveLast).
const queueGroups = computed(() => byLagInactiveLast(groupsByQueue.value.get(selectedQueue.value) || [], linesFor))
// The broker's state words, except for an inactive Kafka group (groupActivity):
// its Dead or Lagging is its cursors' story, and a client that commits offsets
// and takes no lease leaves them nothing to tell. It is counted as inactive.
const groupsSub = computed(() => {
  const lines = linesFor(selectedQueue.value)
  const gs = queueGroups.value
  const inactive = gs.filter((g) => groupActivity(g, lines) === 'inactive')
  const rest = gs.filter((g) => !inactive.includes(g))
  return `${fmt.plural(gs.length, 'group')} · ${rest.filter((g) => g.state === 'Lagging').length} Lagging`
    + ` · ${rest.filter((g) => g.state === 'Dead').length} Dead`
    + (inactive.length ? ` · ${inactive.length} inactive` : '')
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
  return rangeSeconds(selectedRange.value)
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
const groupsCard = ref(null)
function drill(row) {
  if (!row) return
  if (row.level === 'queue') {
    selectedQueue.value = row.key
    // The answer to the click is the card of its consumer groups, under the
    // charts: bring it into view rather than leave it off screen.
    nextTick(() => {
      const reduce = typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
      groupsCard.value?.scrollIntoView({ block: 'nearest', behavior: reduce ? 'auto' : 'smooth' })
    })
  } else { focus.value = row.key; selectedQueue.value = null; fetchWorkload() }
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
  // A phone's card: the room kept for the labels past the bars never takes
  // more than a third of the canvas from the bars themselves.
  const pad = cfg.options?.layout?.padding
  const width = el.parentElement?.clientWidth || 0
  if (pad && typeof pad.right === 'number' && width) pad.right = Math.min(pad.right, Math.round(width * 0.3))
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
  Object.assign(Chart.defaults.plugins.legend.labels, legendLabels())
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
      // Every bar here is a count: whole-number ticks, so an empty window
      // reads 0 and 1 rather than 0 0 0 1 1 1.
      x: { grid: { color: chartTheme.grid }, ticks: { maxTicksLimit: 5, precision: 0 }, beginAtZero: true },
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
        const color = chartPalette[s.index].line
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
          ticks: { callback: (v) => fmt.k(v), precision: 0 },
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
  const sorted = [...rows.value].sort((a, b) => b.reads - a.reads)
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, drillOn(sorted))
  options.scales.x.ticks.callback = (v) => fmt.k(v)
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => ` ${c.dataset.label}: ${fmt.n(c.parsed.x)}`
          + (c.datasetIndex === 0 ? ` (${fmt.pct(sorted[c.dataIndex].share)} of the tenant's reads)` : ''),
        // The fetch bar's two caveats, under it: it is not a delivery.
        afterLabel: (c) => (c.dataset.fetch && c.parsed.x > 0 ? '  no lease, no ack · counted once per Kafka group' : ''),
      },
    },
    endLabels: { format: (i) => fmt.pct(sorted[i].share) },
  }
  mount('share', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'delivered', data: sorted.map((r) => r.window.popMessages), backgroundColor: barInkLead(), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        // Only where the broker counts fetches and a row has some: a tenant
        // without Kafka keeps its two bars.
        ...(sorted.some((r) => r.fetched > 0) ? [{
          label: 'fetched (Kafka)', fetch: true, data: sorted.map((r) => r.fetched), backgroundColor: alpha(chartPalette[0].line, 0.45), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8,
        }] : []),
        { label: 'pushed', data: sorted.map((r) => r.window.pushMessages), backgroundColor: barInk(), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        ...(prevPayload.value ? [{
          label: `delivered + fetched, ${compareLabel.value}`, data: sorted.map((r) => prevByKey.value.get(r.key)?.reads ?? null),
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
        backgroundColor: barInk(), borderRadius: 2, barPercentage: 0.7, categoryPercentage: 0.8,
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
        { label: 'queues', data: sorted.map((r) => r.queues || 0), backgroundColor: barInkLead(), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        { label: 'consumer groups', data: sorted.map((r) => r.now.groups), backgroundColor: barInk(), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
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
        backgroundColor: barInk(), borderRadius: 2, barPercentage: 0.7, categoryPercentage: 0.8,
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
        { label: 'created', data: sorted.map((r) => r.window.partitionsCreated || 0), backgroundColor: barInkLead(), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
        { label: 'deleted', data: sorted.map((r) => r.window.partitionsDeleted || 0), backgroundColor: alpha(chartPalette[2].line, 0.7), borderRadius: 2, barPercentage: 0.82, categoryPercentage: 0.8 },
      ],
    },
    options,
  })
}

function renderPending() {
  const sorted = [...rows.value].sort((a, b) => b.now.pending - a.now.pending)
  const labels = sorted.map((r) => r.name)
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
        { label: 'with a consumer group', data: sorted.map((r) => r.now.pending - r.now.pendingWithoutGroup), backgroundColor: barInk(), borderRadius: 2 },
        // Amber, unless Settings says no reader is expected on those queues.
        { label: 'no consumer group', data: sorted.map((r) => r.now.pendingWithoutGroup), backgroundColor: sorted.map((r) => (r.noGroupExpected ? barInk() : alpha(semanticColors.warn.line, 0.9))), borderRadius: 2 },
      ],
    },
    options,
  })
}

function renderOldest() {
  const sorted = [...rows.value].sort((a, b) => (b.oldest ?? -1) - (a.oldest ?? -1))
  const labels = sorted.map((r) => r.name)
  const options = barBase(labels, drillOn(sorted))
  options.scales.x.max = Math.max(tenantLines.value.lagBadSeconds * 1.2, ...sorted.map((r) => r.oldest || 0)) * 1.15
  options.scales.x.ticks.callback = (v) => fmt.sec(v)
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const r = sorted[c.dataIndex]
          // An inactive Kafka group is counted, and none of the lag (useWorkload groupLag).
          const gone = r.groupsInactive ? ` · ${r.groupsInactive} inactive` : ''
          if (r.oldest === null) return r.groupsN ? ` ${fmt.plural(r.groupsN, 'group')}, all inactive` : ' no consumer group'
          return ` ${fmt.sec(r.oldest)} · ${r.groupsLagging} of ${r.groupsN} groups lagging${gone}`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = sorted[i]
        if (r.oldest !== null) return fmt.sec(r.oldest)
        if (r.groupsN) return 'inactive groups'
        return r.now.pending > 0 ? `no group · ${fmt.n(r.now.pending)} pending` : 'no group'
      },
    },
    refLines: { lines: lagRefLines(tenantLines.value) },
  }
  mount('oldest', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'oldest waiting',
        data: sorted.map((r) => (r.oldest === null ? null : r.oldest)),
        backgroundColor: sorted.map((r) => alpha(toneColor(r.lagSev || 'mute'), 0.85)),
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
        backgroundColor: barInk(),
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
          if (r.kafkaOnly) return ` read by Kafka fetches only · ${fmt.n(r.fetched)} fetched · a fetch is no poll, so there is no fill`
          return ` ${fmt.pct(r.fill)} · ${fmt.n(r.window.popMessages)} delivered · ${fmt.n(r.window.popEmpty)} empty polls · ${fmt.n(Math.round(r.window.parkedAvg))} parked`
        },
      },
    },
    endLabels: {
      format: (i) => {
        const r = sorted[i]
        // Blank is "too few polls"; a topic Kafka clients read is not that.
        if (r.kafkaOnly) return `Kafka · ${fmt.k(r.fetched)} fetched`
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
  const lines = linesFor(selectedQueue.value)
  // An inactive Kafka group keeps its bar, in plain ink, and does not set the
  // scale: its lag grows for as long as nobody runs it, and an axis drawn to
  // it would flatten every group that is behind now. Its bar is cut at the
  // edge; the label beside it says how far behind it is.
  const gone = gs.map((g) => groupActivity(g, lines) === 'inactive')
  const tone = (s, i) => gone[i] ? 'mute' : s >= lines.lagBadSeconds ? 'bad' : s >= lines.lagWarnSeconds ? 'warn' : 'mute'
  const options = barBase(labels, null)
  options.layout.padding.right = 240
  const scaled = gs.filter((_, i) => !gone[i])
  options.scales.x.max = Math.max(lines.lagBadSeconds * 1.2, ...(scaled.length ? scaled : gs).map((g) => g.maxTimeLag || 0)) * 1.15
  options.scales.x.ticks.callback = (v) => fmt.sec(v)
  options.plugins = {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (c) => {
          const g = gs[c.dataIndex]
          return ` ${fmt.sec(g.maxTimeLag || 0)} · ${fmt.n(g.partitionsWithLag)} of ${fmt.n(g.members)} partitions lagging · offset lag ${fmt.n(g.totalLag || 0)}`
            + (gone[c.dataIndex] ? ' · inactive, not judged' : '')
        },
      },
    },
    endLabels: {
      format: (i) => {
        const g = gs[i]
        return `${g.maxTimeLag ? fmt.sec(g.maxTimeLag) : '0s'} · ${fmt.n(g.partitionsWithLag)}/${fmt.n(g.members)} lagging`
          + (g.conflation ? ' · conflation' : '') + (g.kind === 'kafka' ? ' · kafka' : '')
          + (gone[i] ? ' · inactive' : '')
          + ` · mode ${g.subscriptionMode || '—'}`
      },
    },
    refLines: { lines: lagRefLines(lines) },
  }
  mount('groups', {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'time lag',
        data: gs.map((g) => g.maxTimeLag || 0),
        backgroundColor: gs.map((g, i) => alpha(toneColor(tone(g.maxTimeLag || 0, i)), 0.85)),
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
const deeperPartial = computed(() => partialText(missingNodes(deepWeek.data.value, deepEff.data.value)))
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
  return [{ value: '*', label: 'Tenant' }].concat(
    byTotal.map(([k]) => ({ value: k, label: k || (groupBy.value === 'task' ? '(no task)' : '(no namespace)') })),
  )
})

// A pick the new week no longer has (another Group by, a group gone quiet)
// goes back to the tenant rather than leaving the field blank.
watch(weekPills, (pills) => {
  if (pills.length && !pills.some((p) => p.value === weekPick.value)) weekPick.value = '*'
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
// Read by pop or by Kafka fetch: a topic only Kafka clients consume is read.
const topQueues = computed(() => effRows.value.filter((e) => e.pop + e.fetch > 0).slice(0, 12))
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

const dlqOptions = computed(() => dlqPills.value.map((q) => ({ value: q, label: shortQueue(q) })))
const dlqSig = computed(() => dlqSignatures.data.value)
const dlqSigRows = computed(() => dlqSig.value?.signatures || [])
const dlqSub = computed(() => {
  const d = dlqSig.value
  if (!d || dlqSignatures.failed.value) return 'folded error messages'
  return `${fmt.n(d.rowsNow)} rows · newest ${fmt.n(d.sample)} sampled`
})
const dlqTitle = computed(() => {
  const d = dlqSig.value
  const head = `Folded error messages of the newest dead-letter rows${dlqPick.value ? ` of ${dlqPick.value}` : ''}`
  if (!d || dlqSignatures.failed.value) return head
  return `${head}. ${fmt.bytes(d.avgBytes)} per row; retries ${(d.retryCounts || []).join(' / ') || '—'}.`
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
          const reads = r.fetch > 0 ? `${fmt.n(r.pop)} deliveries + ${fmt.n(r.fetch)} Kafka fetches` : `${fmt.n(r.pop)} deliveries`
          return r.fanout === null
            ? ` ${reads}, 0 pushes counted, ${fmt.n(r.trx)} transactions`
            : ` ${reads} for ${fmt.n(r.push)} pushes · ${fmt.plural(r.groups, 'group')}`
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
        label: 'reads per push',
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
        // A Kafka consumer commits offsets; nothing it reads is acked.
        if (r.ackPerDelivery === null) return r.pop === 0 && r.fetch > 0 ? 'Kafka · no acks' : '—'
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
        backgroundColor: barInk(),
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
  if (!payload.value) {
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
/* The header line's facts: the Queues pilot's chips (QueuesLayout). */
/* The facts drop under the title when the line is full, as the pilot's do. */
.page-head :deep(.page-id) { flex-wrap: wrap; row-gap: 6px; }
.page-head :deep(.page-sub) { overflow: visible; white-space: normal; }
.wl-facts { display: inline-flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.wl-fact {
  display: inline-flex; align-items: baseline; gap: 5px; max-width: 100%;
  padding: 2px 8px; border-radius: var(--r-chip);
  background: var(--ink-3); border: 1px solid var(--bd);
  font-size: 11.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap;
  overflow: hidden;
}
.wl-fact-k { color: var(--text-low); }
.wl-fact-v { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.wl-fact-x {
  align-self: center; display: inline-flex; align-items: center; justify-content: center;
  width: 16px; height: 16px; margin: 0 -4px 0 1px; padding: 0;
  border: 0; border-radius: var(--r-chip); background: transparent; color: var(--text-low); cursor: pointer;
}
.wl-fact-x:hover { background: var(--ink-4); color: var(--text-hi); }
.wl-range { flex-wrap: wrap; }

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
/* A control inside a card's header sits before its stamp, on the right. */
.wl-card-select { margin-left: auto; }
.wl-card-select + .muted { margin-left: 0; }
.wl-card-actions { display: flex; align-items: center; gap: 2px; margin-left: auto; flex-wrap: wrap; }
.wl-card-actions .btn { height: 28px; }
.wl-groups-card .card-header h3 { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.wl-custom { margin-top: -6px; }

.wl-chart { position: relative; width: 100%; }

/* Counts: the Overview's count tiles as a two-row ledger, each row named in a
   narrow first cell. One hairline grid, so the two rows share their columns. */
.wl-block { margin-bottom: 16px; }
.wl-counts {
  display: grid; gap: 1px; background: var(--bd);
  border: 1px solid var(--bd); border-radius: var(--r-card); overflow: hidden;
}
.wl-counts-row { display: grid; grid-template-columns: 96px repeat(7, minmax(0, 1fr)); gap: 1px; }
.wl-counts-head {
  display: block; align-self: stretch; padding: 12px 16px; background: var(--ink-2);
  font-size: 12px; font-weight: 500; line-height: 1.4; color: var(--text-mid);
}
.wl-counts .count-tile .v { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.wl-vs { color: var(--text-low); }
.wl-warn { color: var(--warn-400) !important; }
.wl-bad { color: var(--ember-400) !important; }
.count-delta { margin-left: 6px; font-style: normal; font-size: 11px; font-weight: 400; letter-spacing: 0; color: var(--text-low); }
/* Narrower than seven tiles: the row wraps and its tiles grow to fill each
   line, so no line ends in an empty slot. */
@media (max-width: 1100px) {
  .wl-counts-row { display: flex; flex-wrap: wrap; }
  .wl-counts-row > .count-tile { flex: 1 1 150px; }
  .wl-counts-head { flex: 1 1 100%; padding: 8px 16px; }
  .wl-counts-head br { display: none; }
}

/* Findings: glyph · sentence · evidence, one hairline per row. */
.wl-summary { font-size: 13px; color: var(--text-mid); margin: 0 0 10px; line-height: 1.5; }
.wl-findings { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; }
.wl-findings > li { border-bottom: 1px solid var(--bd-soft, var(--bd)); }
.wl-findings > li:last-child { border-bottom: none; }
.finding {
  display: grid; grid-template-columns: 10px minmax(0, 1fr) auto; align-items: center; gap: 4px 10px;
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
.finding:hover { background: var(--ink-3); }
.finding .g { justify-self: center; }
.ftext { font-size: 13px; color: var(--text-hi); line-height: 1.45; }
.fev { font-size: 12px; color: var(--text-low); font-variant-numeric: tabular-nums; line-height: 1.45; }
/* The deeper findings carry evidence that is a list (queues, shares), too
   long to sit beside the sentence: stack it under the text instead. */
.finding-stack { grid-template-columns: 10px 1fr; cursor: default; }
.finding-stack:hover { background: none; }
.finding-stack .fev { grid-column: 2; justify-self: start; text-align: left; max-width: none; overflow-wrap: anywhere; }
.empty {
  display: flex; align-items: center; justify-content: center; gap: 8px;
  padding: 24px 0; text-align: center; font-size: 13px; color: var(--text-low);
}
.empty > .icon { flex: none; color: var(--text-low); }

/* Section label between the two layers: the app's uppercase label, a rule, a note. */
.wl-eyebrow { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 12px; margin: 28px 2px 14px; }
.eyebrow-text { font-size: 13px; font-weight: 600; color: var(--text-hi); }
.eyebrow-rule { height: 1px; background: var(--bd); flex: 0 0 48px; }
.eyebrow-sub { font-size: 12px; color: var(--text-low); }

.wl-note { font-size: 12px; color: var(--text-low); margin: 10px 0 0; }
</style>
