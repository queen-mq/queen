<template>
  <!--
    The Overview: nine views in a fixed sub sidebar, under three groups, kept
    in the address as ?view= (composables/overviewViews.js OVERVIEW_VIEWS).

      Summary, Issues              the answer first, and everything that needs you
      Queues: Throughput, Backlog, the tenant's queues, one namespace or all
        Consumer groups, Dead letters
      Cell: Nodes, Resources,      the cell under them, a live operator's only
        Replication

    Each row of the list carries the one number of its view, in coral or
    amber where that number is the trouble: no signs, and no echo of one
    problem down the list (Issues counts; Nodes colours only a lost node).
  -->
  <SubSidebar label="Overview sections" :current="currentName">
    <template #side>
      <nav class="ov-side" aria-label="Overview views">
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
            <span class="ss-num" :class="[r.mark, { 'ov-side-word': r.key === 'summary' }]">{{ r.num }}<span v-if="r.mark" class="sr-only"> · {{ r.mark === 'bad' ? 'failing' : 'needs attention' }}</span></span>
          </router-link>
        </div>
      </nav>
    </template>

    <div class="view-container ov">
      <!-- One header line: the title, a few facts, then the view's controls.
           On the Summary the title is the verdict itself, its mark before it
           and its word after. -->
      <header class="ov-head">
        <div class="ov-id">
          <h1 class="ov-title" :title="title">{{ title }}</h1>
        </div>
        <div v-if="facts.length" class="ov-facts">
          <span v-for="f in facts" :key="f.k" class="ov-fact" :title="f.title || undefined"><span class="ov-k">{{ f.k }}</span><span :class="f.tone">{{ f.v }}</span></span>
        </div>
        <div class="ov-actions">
          <!-- The rates are gathered from every node; a node that did not
               answer took its share out of them (usePartial). -->
          <span v-if="partial" class="chip chip-warn" role="status" :title="PARTIAL_TITLE"><span class="dot" />{{ partial }}</span>
          <span class="live-tick" title="This page refreshes itself every 30 seconds"><span class="live-dot" aria-hidden="true" /><span>live · {{ refreshAgo }}</span></span>
          <div v-if="ranged" class="seg" role="group" aria-label="Time range">
            <button
              v-for="r in timeRanges"
              :key="r.value"
              type="button"
              :class="{ on: selectedRange === r.value }"
              :aria-pressed="selectedRange === r.value ? 'true' : 'false'"
              :title="r.longLabel"
              @click="selectedRange = r.value"
            >{{ r.label }}</button>
          </div>
          <div v-if="namespaced" class="ov-ns">
            <NamespacePicker v-model="namespace" :namespaces="namespaceOptions" />
          </div>
        </div>
      </header>

      <!-- =====================================================================
           SUMMARY — the answer, said once: the verdict, the five numbers it
           stands on and, in the same card, what needs you, each row naming
           its queue. Under it the hour in four charts, each opening its view.
           No number here is repeated in another block.
           ===================================================================== -->
      <template v-if="view === 'summary'">
        <section class="card ov-summary" aria-label="Status">
          <div class="ov-status">
            <div class="ov-verdict">
              <span class="ov-verdict-word" :class="verdictTone"><span class="g" :class="verdictGlyph" aria-hidden="true" />{{ verdict.word }}</span>
              <h2 class="ov-verdict-title" :title="verdict.headline">{{ verdict.headline }}</h2>
            </div>
            <div v-for="c in statusCells" :key="c.k" class="ov-kpi" :title="c.title">
              <span class="ov-kpi-k">{{ c.k }}</span>
              <span class="ov-kpi-v" :class="c.tone">{{ c.v }}</span>
            </div>
          </div>
          <QueueTriage
            bare
            :key="epoch" :queues="queues" :groups="consumers" :ops="queueOps"
            :sample-at="queuesQ.lastUpdated.value" :groups-at="consumersQ.lastUpdated.value"
            :queues-error="queuesFailed ? queuesErrorText : ''"
            :groups-error="consumersFailed ? consumersErrorText : ''"
            :tenant-issues="issues.filter(i => i.tenant)"
            :cell-issues="cellIssueList"
            :tenant-error="opsError ? describeApiError(opsError) : ''"
            :limit="5"
            :more-to="viewTo('issues')"
          />
        </section>

        <nav class="ov-inventory" aria-label="What this tenant holds">
          <router-link v-for="t in inventory" :key="t.k" class="ov-inv" :to="t.to" :title="t.title">
            <span class="ov-inv-k">{{ t.k }}</span>
            <span class="ov-inv-v">{{ t.v }}</span>
            <span v-if="t.sub" class="ov-inv-sub" :class="t.tone">{{ t.sub }}</span>
          </router-link>
        </nav>

        <div class="ov-trends">
          <section v-for="t in summaryTrends" :key="t.key" class="card ov-trend" :aria-labelledby="`ov-tr-${t.key}`">
            <div class="card-header">
              <router-link :id="`ov-tr-${t.key}`" class="ov-trend-title" :to="viewTo(t.view)">{{ t.title }}<Icon name="chevron-right" :size="14" /></router-link>
              <span class="card-sub">{{ t.sub }}</span>
            </div>
            <div class="card-body">
              <div v-if="t.error" class="panel-err">{{ describeApiError(t.error) }}</div>
              <div v-else-if="t.loading" class="skeleton ov-chart-skeleton" />
              <RowChart v-else-if="t.data" :data="t.data" :labels="t.labels" :tone="t.tone" :value-format="t.valueFormat" :unit="t.unit" variant="full" />
              <RowChart v-else :series="t.series || null" :labels="t.labels" :value-format="t.valueFormat" :unit="t.unit" variant="full" />
            </div>
          </section>
        </div>
      </template>

      <!-- =====================================================================
           ISSUES — everything that needs you: the cell, the queues, the
           tenant's ack failures, searchable, with how it is decided.
           ===================================================================== -->
      <template v-else-if="view === 'issues'">
        <QueueTriage
          :key="epoch" :queues="queues" :groups="consumers" :ops="queueOps"
          :sample-at="queuesQ.lastUpdated.value" :groups-at="consumersQ.lastUpdated.value"
          :queues-error="queuesFailed ? queuesErrorText : ''"
          :groups-error="consumersFailed ? consumersErrorText : ''"
          :tenant-issues="issues.filter(i => i.tenant)"
          :cell-issues="cellIssueList"
          :tenant-error="opsError ? describeApiError(opsError) : ''"
        >
          <template #rules>
            <details class="issues-rules">
              <summary>How this is decided</summary>
              <p>
                Worked out in the browser from <code>GET /api/v1/resources/queues</code>,
                <code>GET /api/v1/consumer-groups</code> and this window's
                <code>queue-ops</code>. A queue needs you when a consumer group on it is
                more than {{ formatSpan(lines.lagWarnSeconds) }} behind
                ({{ formatSpan(lines.lagBadSeconds) }}: failing), or when it holds
                messages and nothing is set up to read them — no consumer group,
                and no queue-mode consumer that has ever consumed; a queue Kafka
                clients created is exempt. A Kafka group with no live member for
                {{ formatSpan(lines.inactiveAfterSeconds) }} is inactive, not an
                issue. A queue also needs you when its acks fail: their share of
                its last hour's acks crosses
                {{ formatLine(LINE_META.ackWarnRate, lines.ackWarnRate) }}
                (<code>analytics/workload</code>, by queue). The tenant's ack
                failures are an issue of their own only when no queue's acks
                explain them. The
                sidebar, Queues and Consumer groups use the same rule, and the lines
                are the ones in <router-link to="/settings">Settings</router-link>,
                where a queue may have lag lines of its own.
              </p>
            </details>
          </template>
        </QueueTriage>
      </template>

      <!-- =====================================================================
           CELL — the cell under the queues, compact: its verdict and five
           numbers, what needs you in it, and each node now. System has its
           charts, its log and its traffic.
           ===================================================================== -->
      <template v-else-if="view === 'cell'">
        <CellStatus
          :cell="cellStatus"
          :issues="cellIssueLinks"
          :note="nodesNote"
          :node-to="systemNodeTo"
          :more-to="{ path: '/system' }"
          :stamp-text="stamp(membersQ)"
          :version="cellVersion"
        />
      </template>

      <!-- =====================================================================
           THROUGHPUT — what the queues take in and deliver, the busiest, and
           how the long-polls are served.
           ===================================================================== -->
      <template v-else-if="view === 'throughput'">
        <section class="card" aria-labelledby="ov-tp-title">
            <div class="card-header">
              <h3 id="ov-tp-title">Messages per second</h3>
              <span class="card-sub">push, pop and ack</span>
              <span v-if="opsQ.failed.value && opsQ.data.value" class="muted">{{ stamp(opsQ) }}</span>
            </div>
            <div class="card-body">
              <div v-if="cards.throughput.state === 'error'" class="panel-err">{{ describeApiError(cards.throughput.error) }}</div>
              <div v-else-if="cards.throughput.state === 'loading'" class="skeleton ov-chart-skeleton" />
              <RowChart v-else :series="nsThroughputSeries" :labels="nsChartLabels" :value-format="fmtRate" unit="msgs / sec" variant="full" />
            </div>
          </section>

          <section class="card" aria-labelledby="ov-busy-title">
            <div class="card-header">
              <h3 id="ov-busy-title">Busiest</h3>
              <span class="card-sub">last {{ selectedRange }}</span>
            </div>
            <div v-if="cards.busiest.state === 'loading'" class="ov-list" aria-hidden="true">
              <div v-for="i in 3" :key="i" class="ov-lrow ov-skel"><span /><span class="skeleton" /><span /></div>
            </div>
            <div v-else-if="cards.busiest.state === 'error'" class="card-body"><div class="panel-err">{{ describeApiError(cards.busiest.error) }}</div></div>
            <div v-else-if="!busiest.length" class="ov-quiet">No traffic in the last {{ selectedRange }}</div>
            <div v-else class="ov-list ov-cols-busy">
              <div class="ov-lhead" aria-hidden="true">
                <span /><span>Queue</span>
                <span class="ov-r" title="Messages pushed per second, averaged over the window">Push /s</span>
                <span class="ov-r" title="Messages popped per second, averaged over the window">Pop /s</span>
              </div>
              <router-link
                v-for="b in busiest"
                :key="b.name"
                class="ov-lrow"
                :to="queueTabLocation('overview', b.name, route, carry)"
                :title="`${b.name} · ${formatNumber(b.pushMessages)} pushed, ${formatNumber(b.popMessages)} popped in the last ${selectedRange}`"
              >
                <span class="ss-mark" aria-hidden="true"><span v-if="issueSevByQueue.get(b.name)" class="g" :class="issueSevByQueue.get(b.name)" /></span>
                <span class="ov-cell"><span class="ov-name">{{ b.name }}</span></span>
                <span class="ov-n">{{ perSecond(b.pushPerSecond) }}</span>
                <span class="ov-n">{{ perSecond(b.popPerSecond) }}</span>
              </router-link>
            </div>
          </section>
        <div class="tile-grid tile-grid-4 ov-tiles-2">
            <MetricTile
            label="Parked"
            :value="parkedLatest === null ? '—' : formatNumber(Math.round(parkedLatest))"
            :unit="parkedLatest === null ? '' : 'long-polls'"
            :context="parkedContext"
            :series="parkedSeriesData"
            :labels="chartLabels"
            :value-format="fmtCount"
            :loading="loadingOps"
            :error="opsError"
            tooltip="Long-poll consumer connections waiting for work, summed across every queue."
            :selected="false"
          />
          <MetricTile
            label="Fill ratio"
            :context="fillContext"
            :series="fillSeriesData"
            :labels="chartLabels"
            :value-format="fmtFillPct"
            :loading="loadingOps"
            :error="opsError"
            tooltip="Long-polls that came back with a message, of every long-poll that ended, across every queue."
            :selected="false"
          >
            <template #value>
              <span v-if="fillLatest === null" class="num">—</span>
              <template v-else><span class="num">{{ fillLatest.toFixed(1) }}</span><i class="mr-unit">%</i></template>
            </template>
          </MetricTile>
        </div>
      </template>

      <!-- =====================================================================
           BACKLOG — what waits: the backlog over time, the queues behind, the
           oldest message, and what partitions and retention did.
           ===================================================================== -->
      <template v-else-if="view === 'backlog'">
          <section class="card" aria-labelledby="ov-bl-title">
            <div class="card-header">
              <h3 id="ov-bl-title">Messages waiting</h3>
              <span class="card-sub">pending and in flight</span>
            </div>
            <div class="card-body">
              <div
                v-if="nsBacklog.state === 'unsplit'"
                class="ov-chart-note"
                title="The broker keeps the backlog over time for the whole tenant and for each queue, not for a namespace. All namespaces shows the tenant's; a queue's own page shows its."
              >Not kept per namespace</div>
              <div v-else-if="nsBacklog.state === 'error'" class="panel-err">{{ describeApiError(nsBacklog.error) }}</div>
              <div v-else-if="nsBacklog.state === 'loading'" class="skeleton ov-chart-skeleton" />
              <RowChart v-else :series="nsBacklog.series" :labels="nsBacklog.labels" :value-format="fmtCount" unit="msgs" variant="full" />
            </div>
          </section>
          <div class="ov-pair">
          <!-- Where the backlog is: the deepest queues, each with its oldest
               wait and its mark. -->
          <section class="card" aria-labelledby="ov-deep-title">
            <div class="card-header">
              <h3 id="ov-deep-title">Deepest queues</h3>
              <span class="card-sub">the most messages waiting</span>
            </div>
            <div v-if="cards.attention.state === 'loading'" class="ov-list" aria-hidden="true">
              <div v-for="i in 3" :key="i" class="ov-lrow ov-skel"><span /><span class="skeleton" /><span /></div>
            </div>
            <div v-else-if="queuesFailed" class="card-body"><div class="panel-err">{{ queuesErrorText }}</div></div>
            <div v-else-if="!nsDeepest.length" class="ov-quiet">Nothing is waiting</div>
            <div v-else class="ov-list ov-cols-att">
              <div class="ov-lhead" aria-hidden="true"><span /><span>Queue</span><span class="ov-r" title="The oldest message a live consumer group of the queue has not read">Oldest</span><span class="ov-r">Pending</span></div>
              <router-link
                v-for="d in nsDeepest"
                :key="d.name"
                class="ov-lrow"
                :to="queueTabLocation('overview', d.name, route, carry)"
                :title="`${d.name} · ${formatNumber(d.pending)} waiting`"
              >
                <span class="ss-mark" aria-hidden="true"><span v-if="d.sev" class="g" :class="d.sev" /></span>
                <span class="ov-cell">
                  <span class="ov-name">{{ d.name }}</span>
                  <span v-if="d.namespace && namespace === null" class="ov-sub">{{ d.namespace }}</span>
                </span>
                <span class="ov-n" :class="d.lag === null ? '' : lagNumClass(d.lag, d.name)">{{ lagText(d.lag) }}</span>
                <span class="ov-n">{{ formatNumber(d.pending) }}</span>
              </router-link>
            </div>
          </section>

          <section class="card" aria-labelledby="ov-att-title">
            <div class="card-header">
              <h3 id="ov-att-title">Needs attention</h3>
              <span v-if="statusKnown" class="card-sub">{{ formatNumber(nsAttention.length) }} of {{ formatNumber(nsQueues.length) }} {{ nsQueues.length === 1 ? 'queue' : 'queues' }}</span>
              <router-link class="ov-more" :to="queueTabLocation('overview', null, route, carry)">All queues<Icon name="chevron-right" :size="14" /></router-link>
            </div>
            <div v-if="cards.attention.state === 'loading'" class="ov-list" aria-hidden="true">
              <div v-for="i in 3" :key="i" class="ov-lrow ov-skel"><span /><span class="skeleton" /><span /></div>
            </div>
            <div v-else-if="cards.attention.state === 'error'" class="card-body"><div class="panel-err">{{ describeApiError(cards.attention.error) }}</div></div>
            <div v-else-if="!nsAttention.length" class="ov-quiet">Nothing needs you</div>
            <div v-else class="ov-list ov-cols-att">
              <div class="ov-lhead" aria-hidden="true"><span /><span>Queue</span><span class="ov-r">Lag</span><span class="ov-r">Pending</span></div>
              <router-link
                v-for="a in nsAttention.slice(0, LIST)"
                :key="a.name"
                class="ov-lrow"
                :to="queueTabLocation('alerts', a.name, route, carry)"
                :title="`${a.name} · ${attentionWhy(a)}`"
              >
                <span class="ss-mark" aria-hidden="true"><span class="g" :class="a.sev" /></span>
                <span class="ov-cell">
                  <span class="ov-name">{{ a.name }}</span>
                  <span class="ov-sub">{{ attentionWhy(a) }}</span>
                </span>
                <span class="ov-n" :class="a.reason === 'lag' ? a.sev : ''">{{ a.reason === 'lag' ? lagText(a.lag) : '—' }}</span>
                <span class="ov-n">{{ formatNumber(a.pending) }}</span>
              </router-link>
            </div>
          </section>
          </div>

        <section class="card" aria-labelledby="ov-lag-title">
          <div class="card-header">
            <h3 id="ov-lag-title">{{ metricChart('timeLag').title }}</h3>
            <span class="card-sub">{{ metricChart('timeLag').sub }}</span>
          </div>
          <div class="card-body">
            <div v-if="metricChart('timeLag').error" class="panel-err">{{ describeApiError(metricChart('timeLag').error) }}</div>
            <RowChart v-else :series="metricChart('timeLag').series || null" :labels="metricChart('timeLag').labels || []" :value-format="metricChart('timeLag').valueFormat" :unit="metricChart('timeLag').unit" variant="full" />
          </div>
        </section>
        <div class="tile-grid tile-grid-4 ov-tiles-2">
            <MetricTile
            label="Partitions"
            context="created / deleted in window"
            :series="partitionSeriesData"
            :labels="chartLabels"
            :value-format="fmtCount"
            :loading="loadingOps"
            :error="opsError"
            :selected="false"
          >
            <template #value>
              <span class="num">+{{ formatNumber(partitionCreatedTotal) }}</span>
              <span class="mr-sep">/</span>
              <span class="num mute">−{{ formatNumber(partitionDeletedTotal) }}</span>
            </template>
          </MetricTile>
          <MetricTile
            label="Retention"
            :context="retentionContext"
            :series="retentionSeriesData"
            :labels="retentionLabels"
            :value-format="fmtCount"
            :error="retentionError"
            tooltip="Messages deleted by the retention and eviction workers."
            :selected="false"
          >
            <template #value>
              <span v-if="retentionTotal === null" class="num">—</span>
              <template v-else><span class="num">{{ formatNumber(retentionTotal) }}</span><i class="mr-unit">msgs</i></template>
            </template>
          </MetricTile>
        </div>
        <section class="card flower-card ov-flower" aria-labelledby="ov-flower-title">
          <div class="card-header">
            <h3 id="ov-flower-title">Partitions</h3>
            <span class="card-sub">{{ queuesFailed || !queuesRead ? '—' : `${formatNumber(nsPartitions)} across ${formatNumber(nsQueues.length)} ${nsQueues.length === 1 ? 'queue' : 'queues'}` }}</span>
          </div>
          <PartitionSunflower :queues="nsSunflowerQueues" :partitions="namespace === null ? sunflowerPartitions : null" :loading="loadingQueues" :error="queuesQ.error.value" />
        </section>
      </template>

      <!-- =====================================================================
           CONSUMER GROUPS — every group of the namespace, the worst first.
           ===================================================================== -->
      <template v-else-if="view === 'groups'">
        <section class="card" aria-labelledby="ov-groups-title">
          <div class="card-header">
            <h3 id="ov-groups-title">Worst first</h3>
            <span v-if="cards.groups.state === 'ok'" class="card-sub">{{ formatNumber(nsGroups.total) }} {{ nsGroups.total === 1 ? 'group' : 'groups' }}<template v-if="nsGroups.behind"> · {{ formatNumber(nsGroups.behind) }} behind</template></span>
            <router-link class="ov-more" :to="queueTabLocation('consumers', null, route, carry)">All consumer groups<Icon name="chevron-right" :size="14" /></router-link>
          </div>
          <div v-if="cards.groups.state === 'loading'" class="ov-list" aria-hidden="true">
            <div v-for="i in 3" :key="i" class="ov-lrow ov-skel"><span /><span class="skeleton" /><span /></div>
          </div>
          <div v-else-if="cards.groups.state === 'error'" class="card-body"><div class="panel-err">{{ describeApiError(cards.groups.error) }}</div></div>
          <div v-else-if="!groupRowsAll.length" class="ov-quiet">No consumer groups</div>
          <div v-else class="ov-list ov-cols-groups">
            <div class="ov-lhead" aria-hidden="true">
              <span /><span>Group</span>
              <span class="ov-r" title="The oldest message the group has not read">Lag</span>
              <span class="ov-r" title="Partitions with messages the group has not read">Behind</span>
            </div>
            <router-link
              v-for="r in groupRowsAll"
              :key="r.key"
              class="ov-lrow"
              :to="r.to"
              :title="`${r.name} on ${r.queue}`"
            >
              <span class="ss-mark" aria-hidden="true"><span v-if="r.mark" class="g" :class="r.mark" /></span>
              <span class="ov-cell">
                <span class="ov-name">{{ r.name }}</span>
                <span class="ov-sub">{{ r.queue }}<template v-if="r.presence"> · {{ r.presence }}</template></span>
              </span>
              <span class="ov-n" :class="r.mark">{{ r.lag }}</span>
              <span class="ov-n">{{ r.behind }}</span>
            </router-link>
          </div>
        </section>
      </template>

      <!-- =====================================================================
           DEAD LETTERS — the failed acks over the window, and where the dead
           letters are now, queue by queue.
           ===================================================================== -->
      <template v-else-if="view === 'dead-letters'">
        <section class="card" aria-labelledby="ov-err-title">
          <div class="card-header">
            <h3 id="ov-err-title">Ack failures</h3>
            <span class="card-sub">failed acks per bucket, every queue</span>
          </div>
          <div class="card-body">
            <div v-if="opsError" class="panel-err">{{ describeApiError(opsError) }}</div>
            <RowChart v-else :data="errorSeriesData || []" :labels="chartLabels || []" :tone="errorSeverity || 'mute'" :value-format="fmtCount" unit="ack failures" variant="full" />
          </div>
        </section>
        <section class="card" aria-labelledby="ov-dlq-title">
          <div class="card-header">
            <h3 id="ov-dlq-title">By queue</h3>
            <span class="card-sub">now</span>
            <router-link class="ov-more" :to="queueTabLocation('failed', null, route, carry)">All dead letters<Icon name="chevron-right" :size="14" /></router-link>
          </div>
          <div v-if="!queuesRead" class="ov-list" aria-hidden="true">
            <div v-for="i in 3" :key="i" class="ov-lrow ov-skel"><span /><span class="skeleton" /><span /></div>
          </div>
          <div v-else-if="!dlqRows.length" class="ov-quiet">No dead letters</div>
          <div v-else class="ov-list ov-cols-dlq">
            <div class="ov-lhead" aria-hidden="true"><span /><span>Queue</span><span class="ov-r">Dead letters</span></div>
            <router-link
              v-for="d in dlqRows"
              :key="d.name"
              class="ov-lrow"
              :to="queueTabLocation('failed', d.name, route, carry)"
            >
              <span class="ss-mark" aria-hidden="true" />
              <span class="ov-cell">
                <span class="ov-name">{{ d.name }}</span>
                <span v-if="d.namespace" class="ov-sub">{{ d.namespace }}</span>
              </span>
              <span class="ov-n">{{ formatNumber(d.count) }}</span>
            </router-link>
          </div>
        </section>
      </template>

    </div>
  </SubSidebar>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import {
  consumers as consumersApi,
  describeApiError,
  operator as operatorApi,
  queues as queuesApi,
  resources,
  system as systemApi,
} from '@/api'
import CellStatus from '@/components/CellStatus.vue'
import Icon from '@/components/Icon.vue'
import MetricTile from '@/components/MetricTile.vue'
import NamespacePicker from '@/components/NamespacePicker.vue'
import PartitionSunflower, { MAX_SEEDS } from '@/components/PartitionSunflower.vue'
import QueueTriage from '@/components/QueueTriage.vue'
import RowChart from '@/components/RowChart.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import { namespaceCounts, queueTabLocation } from '@/composables/navigation'
import {
  attentionList, attentionWhy, deepestQueues, busiestQueues, groupsSummary, lagText,
  overviewVerdict, overviewView, perSecond, queuesIn, queueTotals, rollupQueueOps,
  NAMESPACED_VIEWS, OVERVIEW_VIEWS, sideGroups, systemRedirect, viewAlias, viewQuery, windowSeconds, worstMark,
} from '@/composables/overviewViews'
import { LINE_META, formatLine, formatSpan } from '@/composables/settingsDoc'
import { groupAttention, groupPresence, queueAttention } from '@/composables/useAttention'
import {
  formatBytes, formatNumber, latestFinite, toNum, trimIncompleteBuckets, useApi,
} from '@/composables/useApi'
import { brokerStatus, cellIssues } from '@/composables/useBrokerStatus'
import { clusterView } from '@/composables/useRaftCluster'
import { cellIssueView } from '@/composables/systemViews'
import { formatChartLabel, isMultiDay } from '@/composables/useFormat'
import { missingNodes, partialText, PARTIAL_TITLE } from '@/composables/usePartial'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import { useRouteState } from '@/composables/useRouteState'
import {
  ackFailureSeverity, backlogSeverity, numTone,
  timeLagSeverity,
} from '@/composables/useSeverity'
import { stamp } from '@/composables/useStamp'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useIdentity } from '@/stores/identity'
import { isMissingRoute, routeSupport } from '@/stores/routeSupport'
import { useSettingsStore } from '@/stores/settingsStore'
import { DEFAULT_TIME_RANGE, TIME_RANGES as timeRanges, relativeTimeRange } from '../../../shared/timeRanges.mjs'

const route = useRoute()
const router = useRouter()
const pageName = route.name
const { can, actingTenantSlug, actingCellSlug, epoch } = useIdentity()
const canOperate = computed(() => can('operator'))
const { lines, linesFor } = useSettingsStore()

// Rows a list card draws; the rest is one click away in the Queues section.
const LIST = 8

// ---------------------------------------------------------------------------
// The view, the range and the namespace: all three in the address, so a link
// opens the page as it was. An old ?view= (consumers, cell) is rewritten to
// the view that holds it now.
// ---------------------------------------------------------------------------
const view = computed(() => overviewView(route.query.view, canOperate.value))
watch(() => route.query.view, (v) => {
  if (route.name !== pageName) return
  // The cell's views are System's now: an old address goes there.
  const sys = systemRedirect(v)
  if (sys) { router.replace({ path: '/system', query: sys === 'overview' ? {} : { view: sys } }); return }
  const to = viewAlias(v)
  if (to) router.replace({ query: { ...route.query, view: to } })
}, { immediate: true })
const viewTo = (key) => ({ path: '/', query: { ...route.query, view: viewQuery(key) } })
const currentName = computed(() => OVERVIEW_VIEWS.find((v) => v.key === view.value)?.name || 'Summary')
// The views drawn over a window take the range; the views a namespace
// narrows take the namespace picker.
const RANGED = new Set(['summary', 'throughput', 'backlog', 'dead-letters'])
const ranged = computed(() => RANGED.has(view.value))
const namespaced = computed(() => NAMESPACED_VIEWS.includes(view.value))

const selectedRange = ref(DEFAULT_TIME_RANGE)
// null is every namespace; '' is the default namespace, a real one.
const namespace = ref(null)
useRouteState({ range: selectedRange, namespace })
const getTimeRangeParams = () => {
  const { from, to } = relativeTimeRange(selectedRange.value)
  return { from: from.toISOString(), to: to.toISOString() }
}

// ---------------------------------------------------------------------------
// SOURCES. Tenant-scoped (the proxy injects the acting cluster's tenant):
// overview, queues, consumer groups, queue-ops, retention. Cell-level, an
// operator's only (404 route_blocked for anyone else, so never asked): the
// raft members, /api/v1/status and system-metrics. Nothing swallows a failure:
// `error` drives an inline "unavailable", never an idle-looking zero.
//
// Each view asks for what it draws. The queues, the groups, queue-ops (the
// ack failures are part of the verdict) and the members are read on every
// view: the sub sidebar's marks need them whichever view is open.
// ---------------------------------------------------------------------------
const groupsStore = useGroupsStore()
// Each queue's last hour (its acks), for the one rule: a queue whose acks
// fail needs you, and the tenant's failure rate is then that queue's.
const queueOpsStore = useQueueOpsStore()
const queueOps = queueOpsStore.ops
const overviewQ = useApi((config) => resources.getOverview(config), { immediate: false })
const queuesQ = useApi((config) => queuesApi.list(undefined, config), { immediate: false })
// Handed to the shared store, so the sidebar and Queues reuse this read.
const consumersQ = useApi((config) => consumersApi.list(config), { immediate: false, onSuccess: (d) => groupsStore.publish(d) })
const opsQ = useApi((config) => systemApi.getQueueOps(getTimeRangeParams(), config), { immediate: false })
const retentionQ = useApi((config) => systemApi.getRetention(getTimeRangeParams(), config), { immediate: false })
// The cell's nodes: the read the top bar makes, guarded the same way, so a
// broker without the route is asked once and stays quiet.
const membersQ = useApi(
  routeSupport.guard('raft-members', (config) => operatorApi.getRaftMembers({ ...config, probe: true })),
  { immediate: false },
)

const overview = overviewQ.data
const queues = computed(() => {
  const d = queuesQ.data.value
  return d?.queues || (Array.isArray(d) ? d : [])
})
const consumers = computed(() => {
  const d = consumersQ.data.value
  return Array.isArray(d) ? d : (d?.consumer_groups || [])
})

// Skeletons until the first payload lands, from before the first request
// starts; a refresh must not blank the page being read.
const firstLoad = (q) => computed(() => q.data.value === null && q.error.value === null)
const loadingOverview = firstLoad(overviewQ)
const loadingQueues = firstLoad(queuesQ)
const loadingConsumers = firstLoad(consumersQ)
const loadingOps = firstLoad(opsQ)

const overviewError = computed(() => overviewQ.error.value)
const opsError = computed(() => opsQ.error.value)
const retentionError = computed(() => retentionQ.error.value)

const overviewFailed = computed(() => overviewQ.error.value !== null)
const queuesFailed = computed(() => queuesQ.error.value !== null)
const consumersFailed = computed(() => consumersQ.error.value !== null)

const queuesRead = computed(() => queuesQ.data.value !== null)
const groupsRead = computed(() => consumersQ.data.value !== null)
const queuesErrorText = computed(() => describeApiError(queuesQ.error.value))
const consumersErrorText = computed(() => describeApiError(consumersQ.error.value))

// A node that did not answer the broker's gather (usePartial): queue-ops
// feeds every view of the Overview.
const partial = computed(() => partialText(missingNodes(opsQ.data.value)))

// ---------------------------------------------------------------------------
// Charts' clock.
// ---------------------------------------------------------------------------
const labelsFor = (stamps) => {
  const multi = isMultiDay(stamps)
  return stamps.map((ts) => formatChartLabel(new Date(ts), multi))
}

// ---------------------------------------------------------------------------
// TENANT history: queue-ops rolled up from one row per (queue, bucket) to one
// row per bucket, the still-filling tail bucket dropped so no chart ends in a
// false drop. The source of every tenant tile.
// ---------------------------------------------------------------------------
const trimmedRollup = (names) => {
  const payload = opsQ.data.value
  return trimIncompleteBuckets(rollupQueueOps(payload?.series, names), {
    bucketKey: 'bucket',
    bucketMinutes: payload?.bucketMinutes || 1,
  })
}
const history = computed(() => trimmedRollup(null))
const chartLabels = computed(() => labelsFor(history.value.map((h) => h.bucket)))

const totalPartitions = computed(() => queues.value.reduce((sum, q) => sum + (q.partitions || 1), 0))

// Throughput: real push / pop / ack rates, summed across the queues. Not the
// overview's ingestedPerSecond, which per tenant is a retained-message delta
// and falls to 0 after a retention sweep.
const throughputOf = (h) => {
  if (!h.length) return null
  return [
    { label: 'Push', data: h.map((x) => toNum(x.pushPerSecond)) },
    { label: 'Pop', data: h.map((x) => toNum(x.popPerSecond)) },
    { label: 'Ack', data: h.map((x) => toNum(x.ackPerSecond)) },
    // Only once a fetch is counted: a tenant without Kafka keeps three lines.
    ...(h.some((x) => x.fetchMessages > 0) ? [{ label: 'Fetch (Kafka)', data: h.map((x) => toNum(x.fetchPerSecond)) }] : []),
  ]
}
const throughputSeries = computed(() => throughputOf(history.value))

// Pending — the backlog itself, as the broker reads it once a minute
// (queue-ops `backlog`: per bucket its last reading of what waits and what is
// in flight). It is NOT push − ack: a message also leaves by a transaction, a
// stream cycle, retention or the DLQ, and every group acks it once, so that
// difference drifts for ever.
const backlogRowsOf = (rows) => (Array.isArray(rows) ? rows.filter((x) => toNum(x?.pending) !== null) : [])
const backlogChartOf = (rows) => ({
  labels: labelsFor(rows.map((b) => b.bucket)),
  series: rows.length
    ? [
        { label: 'Pending', data: rows.map((b) => toNum(b.pending)) },
        { label: 'In flight', data: rows.map((b) => toNum(b.processing)) },
      ]
    : null,
})
const backlog = computed(() => backlogRowsOf(opsQ.data.value?.backlog))
const backlogLabels = computed(() => labelsFor(backlog.value.map((b) => b.bucket)))
const backlogSeries = computed(() => backlogChartOf(backlog.value).series)

// Time lag. The value is the tenant overview's: the age of each queue's oldest
// unconsumed message, right with consumers stopped; null is "no measurement"
// and renders '—', never 0s. The chart is sampled at pop, with a gap for every
// bucket without pops.
const lagAvgSeconds = computed(() => toNum(overview.value?.lag?.time?.avg))
const lagMaxSeconds = computed(() => toNum(overview.value?.lag?.time?.max))
const lagSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [
    { label: 'Avg', data: h.map((x) => toNum(x.avgLagMs)) },
    { label: 'Max', data: h.map((x) => toNum(x.maxLagMs)) },
  ]
})
// An age means the same at any message rate, so it is judged against two
// lines only: the queue's own when a row names its queue, else the tenant's.
const lagNumClass = (s, queue) => numTone(timeLagSeverity(s, queue === undefined ? undefined : linesFor(queue)))
const lagSeverity = computed(() => lagNumClass(lagMaxSeconds.value || 0))

// Errors — ack failures across the window, read as a share of the acks
// attempted (success + failed); the dead letter depth is a snapshot, context
// only: nothing purges it, so it could never go back to neutral.
const errorSeriesData = computed(() => history.value.map((x) => toNum(x.ackFailed)))
const ackFailedTotal = computed(() => history.value.reduce((s, x) => s + (toNum(x.ackFailed) || 0), 0))
const ackSuccessTotal = computed(() => history.value.reduce((s, x) => s + (toNum(x.ackSuccess) || 0), 0))
const dlqDepth = computed(() => toNum(overview.value?.messages?.deadLetter))
const errorContext = computed(() => {
  const dlq = dlqDepth.value
  const dlqText = dlq === null ? 'dlq —' : `dlq ${formatNumber(dlq)} now`
  const acks = ackSuccessTotal.value + ackFailedTotal.value
  // The rate is the verdict, so the rate is what the line says.
  const rate = acks > 0
    ? `${formatNumber(ackFailedTotal.value)} of ${formatNumber(acks)} acks (${((ackFailedTotal.value / acks) * 100).toFixed(2)}%)`
    : `ack ${formatNumber(ackFailedTotal.value)} in window`
  return `${rate} · ${dlqText}`
})
const errorSeverity = computed(() => ackFailureSeverity({ failed: ackFailedTotal.value, succeeded: ackSuccessTotal.value }))

// Partitions created and deleted, from the same buckets.
const partitionSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [
    { label: 'Created', data: h.map((x) => toNum(x.partitionsCreated)) },
    { label: 'Deleted', data: h.map((x) => toNum(x.partitionsDeleted)) },
  ]
})
const partitionCreatedTotal = computed(() => history.value.reduce((s, x) => s + (toNum(x.partitionsCreated) || 0), 0))
const partitionDeletedTotal = computed(() => history.value.reduce((s, x) => s + (toNum(x.partitionsDeleted) || 0), 0))

// Parked — long-polls waiting for work, summed across the queues per bucket.
const parkedSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  return [{ label: 'Parked', data: h.map((x) => toNum(x.parkedTotal)) }]
})
const parkedLatest = computed(() => latestFinite(history.value.map((x) => x.parkedTotal)))
const parkedAvg = computed(() => {
  const finite = history.value.map((x) => toNum(x.parkedTotal)).filter((v) => v !== null)
  return finite.length ? finite.reduce((s, v) => s + v, 0) / finite.length : 0
})
const parkedContext = computed(() => (history.value.length ? `avg ${parkedAvg.value.toFixed(1)} across window · idle long-polls` : '—'))

// Fill ratio — popMessages / (popMessages + popEmpty) per bucket. Buckets
// under the noise floor carry the previous value, so the line stays whole;
// the headline uses a real null, so an idle window is not 0% delivery. No
// tone: low fill is a pool larger than its work, a sizing observation.
const FILL_NOISE_FLOOR = 5
const fillSeriesData = computed(() => {
  const h = history.value
  if (!h.length) return null
  let last = null
  const data = h.map((x) => {
    const pop = toNum(x.popMessages)
    const empty = toNum(x.popEmpty)
    if (pop === null && empty === null) return null
    const total = (pop || 0) + (empty || 0)
    if (total < FILL_NOISE_FLOOR) return last
    last = Math.round(((pop || 0) / total) * 1000) / 10
    return last
  })
  return [{ label: 'Fill', data }]
})
const fillLatest = computed(() => {
  const tail = history.value.slice(-5)
  if (!tail.length) return null
  let pop = 0
  let empty = 0
  for (const r of tail) {
    pop += toNum(r.popMessages) || 0
    empty += toNum(r.popEmpty) || 0
  }
  if (pop + empty < FILL_NOISE_FLOOR) return null
  return Math.round((pop / (pop + empty)) * 1000) / 10
})
const fillContext = computed(() => {
  const v = fillLatest.value
  if (v === null) return 'idle window · no long-poll activity'
  if (v >= 80) return 'high utilization · consumers busy serving'
  if (v < 30) return 'low utilization · consumers mostly empty'
  return 'balanced · consumer pool sized OK'
})

// Retention — every step is recorded when it applies, so an empty series is
// "nothing deleted in the window".
const retentionRows = computed(() => {
  const payload = retentionQ.data.value
  return trimIncompleteBuckets(payload?.series || [], { bucketKey: 'bucket', bucketMinutes: payload?.bucketMinutes || 1 })
})
const retentionSeriesData = computed(() => {
  const rows = retentionRows.value
  if (!rows.length) return null
  return [
    { label: 'Retention', data: rows.map((r) => toNum(r.retentionMsgs)) },
    { label: 'Completed', data: rows.map((r) => toNum(r.completedRetentionMsgs)) },
    // No status hue: retention deleting messages is retention working.
    { label: 'Evicted', data: rows.map((r) => toNum(r.evictionMsgs)) },
  ]
})
const retentionLabels = computed(() => labelsFor(retentionRows.value.map((r) => r.bucket)))
const retentionTotal = computed(() => {
  const rows = retentionRows.value
  if (!rows.length) return null
  return rows.reduce((s, r) => s + (toNum(r.retentionMsgs) || 0) + (toNum(r.completedRetentionMsgs) || 0) + (toNum(r.evictionMsgs) || 0), 0)
})
const retentionContext = computed(() => {
  if (retentionQ.data.value === null) return 'loading…'
  if (!retentionRows.value.length) return 'none in window'
  return 'evicted + completed-retention in window'
})

// Pending tone (the counts strip): a depth has no verdict on its own, so it is
// judged in SECONDS OF WORK at the ack rate this window measured, and stays
// neutral with no drain rate to divide by — a stalled queue is the Time lag
// tile's story.
const drainPerSec = computed(() => latestFinite(history.value.map((x) => x.ackPerSecond)))
const pendingNumClass = (n) => backlogSeverity({ pending: n, drainPerSec: drainPerSec.value })

// ---------------------------------------------------------------------------
// Formatters. The unit is in the name.
// ---------------------------------------------------------------------------
const fmtRate = (n) => {
  const v = Number(n) || 0
  if (Math.abs(v) >= 1000) return `${(v / 1000).toFixed(1)}k /s`
  if (Math.abs(v) >= 100) return `${Math.round(v)} /s`
  if (Math.abs(v) >= 10) return `${v.toFixed(1)} /s`
  return `${v.toFixed(2)} /s`
}
const fmtMsgRate = (n) => fmtRate(n).replace(' /s', ' msg/s')
const fmtCount = (n) => formatNumber(Math.round(Number(n) || 0))
const fmtLagMs = (n) => {
  const v = Number(n) || 0
  if (v < 1) return '0'
  if (v < 1000) return `${Math.round(v)} ms`
  if (v < 60000) return `${(v / 1000).toFixed(1)} s`
  return `${(v / 60000).toFixed(1)} m`
}
const fmtMs = (v) => `${Math.round(Number(v) || 0)} ms`
// A fill bucket can be null ("not enough long-poll activity"): a dash, not 0%.
const fmtFillPct = (n) => {
  if (n === null || n === undefined) return '—'
  const v = Number(n)
  return Number.isFinite(v) ? `${v.toFixed(1)}%` : '—'
}

// The live tick: the last refresh attempt, off the app's one ticker.
const lastRefreshAt = ref(null)
const refreshAgo = useRefreshAgo(lastRefreshAt)

// ---------------------------------------------------------------------------
// The chart the Overview's tiles hand their series to.
// ---------------------------------------------------------------------------
const metricChart = (metric) => {
  switch (metric) {
    case 'pendingDelta': return { title: 'Pending', sub: 'waiting and in flight, read once a minute', series: backlogSeries.value, labels: backlogLabels.value, valueFormat: fmtCount, unit: 'msgs', error: opsError.value }
    case 'timeLag': return { title: 'Time lag', sub: 'sampled at pop, gaps where nothing was consumed', series: lagSeriesData.value, labels: chartLabels.value, valueFormat: fmtLagMs, unit: 'ms', error: opsError.value }
    case 'errors': return { title: 'Ack failures', sub: 'failed acks per bucket', sparkline: errorSeriesData.value, labels: chartLabels.value, valueFormat: fmtCount, unit: 'ack failures', tone: errorSeverity.value || 'mute', error: opsError.value }
    case 'parked': return { title: 'Parked', sub: 'long-polls waiting for work, every queue', series: parkedSeriesData.value, labels: chartLabels.value, valueFormat: fmtCount, unit: 'long-polls', error: opsError.value }
    case 'fillRatio': return { title: 'Fill ratio', sub: 'long-polls that came back with a message', series: fillSeriesData.value, labels: chartLabels.value, valueFormat: fmtFillPct, unit: '%', error: opsError.value }
    case 'partitions': return { title: 'Partitions', sub: 'created and deleted per bucket', series: partitionSeriesData.value, labels: chartLabels.value, valueFormat: fmtCount, unit: 'count', error: opsError.value }
    case 'retention': return { title: 'Retention', sub: 'messages removed by retention and eviction', series: retentionSeriesData.value, labels: retentionLabels.value, valueFormat: fmtCount, unit: 'msgs', error: retentionError.value }
    default: return { title: 'Throughput', sub: 'push, pop, ack and Kafka fetch per second, every queue', series: throughputSeries.value, labels: chartLabels.value, valueFormat: fmtRate, unit: 'msgs / sec', error: opsError.value }
  }
}

// The Summary's hour in four charts, each opening its view. The numbers they
// would repeat are in the status card above them, so they carry none.
const summaryTrends = computed(() => {
  const loading = loadingOps.value
  const acks = metricChart('errors')
  return [
    { key: 'tp', view: 'throughput', ...metricChart('throughput'), loading },
    { key: 'wait', view: 'backlog', ...metricChart('pendingDelta'), loading },
    { key: 'lag', view: 'backlog', ...metricChart('timeLag'), loading },
    { key: 'acks', view: 'dead-letters', ...acks, data: acks.sparkline || [], loading },
  ]
})

// ---------------------------------------------------------------------------
// What needs you, worked out from data already on the page (composables/
// useAttention: the sidebar, Queues and Consumer groups read the same rule),
// plus the tenant's ack failures, which only this page reads. It never says
// "healthy" when it could not read the queues or the groups.
// ---------------------------------------------------------------------------
// The queue listing by depth, as the sunflower and the issues list read it.
const queuesByDepth = computed(() => queues.value
  .map((q) => ({ ...q, _pending: toNum(q.messages?.pending) }))
  .sort((a, b) => (b._pending ?? -1) - (a._pending ?? -1)))

// Known only once both were read and their last reads succeeded: before the
// first answer lands nothing is "healthy".
const statusKnown = computed(() => queuesRead.value && groupsRead.value && !queuesFailed.value && !consumersFailed.value)
const issues = computed(() => {
  if (queuesFailed.value || consumersFailed.value) return []
  const out = queueAttention(queuesByDepth.value, consumers.value, linesFor, queueOps.value)
    .map((a) => ({ key: `q:${a.name}`, sev: a.sev, name: a.name, reason: a.reason, why: attentionWhy(a) }))
  // The tenant's ack failures are an issue of their own only when no queue's
  // own acks explain them: failures spread thin over many queues.
  const es = errorSeverity.value
  const explained = out.some((i) => i.reason === 'acks')
  if ((es === 'warn' || es === 'bad') && !explained) out.push({ key: 'acks', sev: es, name: 'Ack failures', why: errorContext.value, to: '/dlq', tenant: true })
  const rank = { bad: 2, warn: 1 }
  return out.sort((a, b) => rank[b.sev] - rank[a.sev])
})
const issueSevByQueue = computed(() => new Map(issues.value.filter((i) => !i.tenant).map((i) => [i.name, i.sev])))

// The cell's own problems, an operator's only (useBrokerStatus cellIssues, the
// lines the top bar colours by). A failed read adds nothing here.
const cellStatus = computed(() => (canOperate.value ? brokerStatus(membersQ.data.value) : null))
const cellIssueList = computed(() => cellIssues(cellStatus.value).map((i) => ({ ...i, to: viewTo('cell'), go: 'Cell' })))
const cellIssueLinks = computed(() => cellIssues(cellStatus.value).map((i) => {
  const v = cellIssueView(i)
  return { ...i, to: { path: '/system', query: v ? { view: v } : {} } }
}))
// A node opens System's CPU and memory view with that node picked.
const systemNodeTo = (name) => ({ path: '/system', query: { view: 'compute', node: name } })
// The version every node reports (several during a roll, each once).
const cellVersion = computed(() => {
  const rows = clusterView(membersQ.data.value)?.rows || []
  const versions = [...new Set(rows.map((r) => r.version).filter(Boolean))].sort()
  return versions.join(' · ')
})

const verdict = computed(() => overviewVerdict({
  loading: loadingQueues.value || loadingConsumers.value,
  known: statusKnown.value,
  issues: issues.value,
  cellIssues: cellIssueList.value,
}))
const verdictGlyph = computed(() => (verdict.value.sev === 'unknown' ? 'idle' : verdict.value.sev))

// The sub sidebar: every view's mark and one number, from what the page
// already reads, so the list alone says where the trouble is.
const queueIssues = computed(() => issues.value.filter((i) => !i.tenant))
const allIssues = computed(() => [...issues.value, ...(canOperate.value ? cellIssueList.value : [])])
const groupsBehind = computed(() => consumers.value
  .map((g) => groupAttention(g, linesFor))
  .filter((sev) => sev === 'warn' || sev === 'bad'))
// The resource that needs you most; with none, the disk: the one that runs out.
const resourceStatus = computed(() => {
  const s = cellStatus.value
  if (!s) return {}
  const rank = { bad: 2, warn: 1 }
  let pick = null
  for (const [label, r] of [['CPU', s.cpu], ['Memory', s.mem], ['Disk', s.disk]]) {
    if ((rank[r?.sev] || 0) > (rank[pick?.sev] || 0)) pick = { label, sev: r.sev, top: r.top }
  }
  if (!pick) pick = { label: 'Disk', sev: null, top: s.disk?.top }
  const share = toNum(pick.top?.share)
  return {
    mark: pick.sev,
    num: share === null ? '—' : `${pick.label} ${Math.round(share * 100)}%`,
    title: pick.top?.name ? `${pick.label}, the highest: ${pick.top.name}` : '',
  }
})
const side = computed(() => {
  const s = cellStatus.value
  const raft = s?.raft
  const n = allIssues.value.length
  const reachable = s ? s.nodes.filter((x) => !x.unreachable).length : 0
  return sideGroups({
    summary: { mark: verdict.value.sev, num: verdict.value.word, title: verdict.value.headline },
    issues: statusKnown.value || allIssues.value.length
      ? { num: formatNumber(n), title: n ? `${formatNumber(n)} ${n === 1 ? 'thing needs' : 'things need'} you` : 'Nothing needs you' }
      : {},
    throughput: pushNow.value === null ? {} : {
      num: `${perSecond(pushNow.value)}/s`,
      title: `${fmtMsgRate(pushNow.value)} in, ${popNow.value === null ? '—' : fmtMsgRate(popNow.value)} out`,
    },
    backlog: pendingNow.value === null ? {} : {
      mark: worstMark([pendingNumClass(pendingNow.value), lagSeverity.value]),
      num: formatNumber(pendingNow.value),
      title: lagMaxSeconds.value === null ? 'Messages waiting' : `Messages waiting; the oldest for ${lagText(lagMaxSeconds.value)}`,
    },
    groups: groupsRead.value && !consumersFailed.value ? {
      mark: worstMark(groupsBehind.value),
      num: groupsBehind.value.length ? `${formatNumber(groupsBehind.value.length)} behind` : formatNumber(consumers.value.length),
      title: `${formatNumber(consumers.value.length)} consumer ${consumers.value.length === 1 ? 'group' : 'groups'}`,
    } : {},
    'dead-letters': dlqDepth.value === null ? {} : {
      mark: errorSeverity.value,
      num: formatNumber(dlqDepth.value),
      title: errorContext.value || 'Messages in dead letter now',
    },
    // How the cell is: its worst problem said short, else its nodes up.
    cell: s ? (() => {
      const mark = worstMark(cellIssueList.value.map((i) => i.sev))
      const num = reachable < s.nodes.length ? `${reachable}/${s.nodes.length} up`
        : raft?.alert ? raft.alert.title
          : resourceStatus.value.mark ? resourceStatus.value.num
            : cellIssueList.value.length ? `${formatNumber(cellIssueList.value.length)} issues`
              : `${reachable}/${s.nodes.length}`
      return { mark, num, title: cellIssueList.value[0]?.title || 'The cell is healthy' }
    })() : {},
  }, canOperate.value)
})

// The sentence under the verdict: only numbers the page already has.
const pushNow = computed(() => latestFinite(history.value.map((x) => x.pushPerSecond)))
const popNow = computed(() => latestFinite(history.value.map((x) => x.popPerSecond)))
// Only when Kafka clients read in the window: a 0 would say nothing new.
const fetchNow = computed(() => (history.value.some((x) => x.fetchMessages > 0) ? latestFinite(history.value.map((x) => x.fetchPerSecond)) : null))
const pendingNow = computed(() => (overviewFailed.value ? null : toNum(overview.value?.messages?.pending)))
const ackAttempts = computed(() => ackSuccessTotal.value + ackFailedTotal.value)
const ackFailPct = computed(() => (ackAttempts.value > 0 ? `${((ackFailedTotal.value / ackAttempts.value) * 100).toFixed(2)}%` : '—'))

// The sunflower: one entry per queue; the component decides how many seeds.
const sunflowerQueues = computed(() => queuesByDepth.value.map((q) => ({
  name: q.name,
  partitions: q.partitions || 1,
  pending: q._pending ?? 0,
  sev: issueSevByQueue.value.get(q.name) || 'ok',
})))
// One seed per real partition needs /resources/partitions, asked only while
// the flower draws one seed per partition. New in 2.0: an older broker 404s
// once (a probe, no toast), routeSupport remembers, and seeds stay per queue.
const getPartitions = routeSupport.guard('resources-partitions', (params, config) =>
  queuesApi.partitions(params, { ...config, probe: true }))
const partitionsQ = useApi((config) => getPartitions({ limit: MAX_SEEDS }, config), { immediate: false })
const refreshPartitions = () => {
  if (view.value !== 'summary' || queuesFailed.value || totalPartitions.value > MAX_SEEDS) return undefined
  return partitionsQ.refresh()
}
// The Backlog view's sunflower: the namespace shown, its queues' partitions.
const nsSunflowerQueues = computed(() => (namespace.value === null
  ? sunflowerQueues.value
  : sunflowerQueues.value.filter((q) => nsNames.value.has(q.name))))
const nsPartitions = computed(() => nsSunflowerQueues.value.reduce((n, q) => n + (toNum(q.partitions) || 0), 0))
const sunflowerPartitions = computed(() => {
  const d = partitionsQ.data.value
  if (!d || d.truncated || totalPartitions.value > MAX_SEEDS) return null
  return d.partitions || null
})

// ---------------------------------------------------------------------------
// QUEUES — one namespace (or every one), from the listing, the groups and
// queue-ops already read. Throughput is queue-ops' rows of the namespace's
// queues, the same rows /api/v1/status sums for `namespace=`; queue-ops is
// the tenant's route, so it answers every reader and not only an operator.
// ---------------------------------------------------------------------------
const nsQueues = computed(() => queuesIn(queues.value, namespace.value))
const nsNames = computed(() => new Set(nsQueues.value.map((q) => q.name)))
// Each namespace with its count and its worst queue's mark; one picked in the
// address that no queue has any more stays in the menu, at 0.
const namespaceOptions = computed(() => {
  const all = namespaceCounts(queues.value, (q) => issueSevByQueue.value.get(q.name) || null)
  if (namespace.value !== null && !all.some((n) => n.name === namespace.value)) all.push({ name: namespace.value, count: 0, sev: null })
  return all
})
// The namespace rides along into the Queues section, so its list stays filtered.
const carry = computed(() => (namespace.value === null ? {} : { namespace: namespace.value }))

// Every namespace is the tenant's whole traffic, deleted queues included.
const nsHistory = computed(() => (namespace.value === null ? history.value : trimmedRollup(nsNames.value)))
const nsChartLabels = computed(() => labelsFor(nsHistory.value.map((h) => h.bucket)))
const nsThroughputSeries = computed(() => throughputOf(nsHistory.value))

// The backlog over time is kept for the tenant and for each queue: a
// namespace of one queue is that queue's (one more queue-ops read), one of
// several has none to show.
const soleQueue = computed(() => (namespace.value !== null && nsQueues.value.length === 1 ? nsQueues.value[0].name : null))
const soleQ = useApi(async (config) => {
  const queue = soleQueue.value
  const res = await systemApi.getQueueOps({ ...getTimeRangeParams(), queue }, config)
  return { data: { queue, backlog: res.data?.backlog } }
}, { immediate: false })
const refreshSole = () => (view.value === 'backlog' && soleQueue.value ? soleQ.refresh() : undefined)
const nsBacklog = computed(() => {
  if (namespace.value === null) {
    const n = need(opsQ)
    return n.state === 'ok' ? { state: 'chart', ...backlogChartOf(backlog.value) } : n
  }
  const n = need(queuesQ)
  if (n.state !== 'ok') return n
  if (nsQueues.value.length > 1) return { state: 'unsplit' }
  if (!soleQueue.value) return { state: 'chart', labels: [], series: null }
  const d = soleQ.data.value
  if (d?.queue !== soleQueue.value) return soleQ.error.value ? { state: 'error', error: soleQ.error.value } : { state: 'loading' }
  return { state: 'chart', ...backlogChartOf(backlogRowsOf(d.backlog)) }
})

// What a card can draw, from the reads it needs: an error when one failed
// before it ever answered, loading while one has not answered yet. A read that
// answered once keeps its rows through a failed refresh.
function need(...reads) {
  const failed = reads.find((q) => q.data.value === null && q.error.value !== null)
  if (failed) return { state: 'error', error: failed.error.value }
  if (reads.some((q) => q.data.value === null)) return { state: 'loading' }
  return { state: 'ok' }
}
const cards = computed(() => {
  const scoped = namespace.value === null ? [] : [queuesQ]
  // What needs you is never shown from a stale read: unknown is not healthy.
  let attention = { state: 'ok' }
  if (!statusKnown.value) {
    const failed = [queuesQ, consumersQ].find((q) => q.error.value !== null)
    attention = failed ? { state: 'error', error: failed.error.value } : { state: 'loading' }
  }
  return {
    throughput: need(opsQ, ...scoped),
    attention,
    busiest: need(opsQ, queuesQ),
    groups: need(consumersQ, ...scoped),
  }
})

const nsAttention = computed(() => (statusKnown.value ? attentionList(nsQueues.value, consumers.value, linesFor, queueOps.value) : []))
// The Backlog's deepest queues, of the namespace shown.
const nsDeepest = computed(() => deepestQueues(nsQueues.value, consumers.value, issueSevByQueue.value, LIST, linesFor))
const busiest = computed(() => busiestQueues(opsQ.data.value?.series, nsNames.value, {
  seconds: windowSeconds(opsQ.data.value?.timeRange),
  limit: LIST,
}))
const nsGroups = computed(() => groupsSummary(consumers.value, namespace.value === null ? null : nsNames.value, linesFor))
const groupRowFor = (g) => {
  const sev = groupAttention(g, linesFor)
  const mark = sev === 'warn' || sev === 'bad' ? sev : null
  return {
    key: `${g.name}@${g.queueName}`,
    name: g.name === '__QUEUE_MODE__' ? 'Queue mode' : g.name,
    queue: g.queueName,
    presence: groupPresence(g, linesFor),
    mark,
    lag: (toNum(g.maxTimeLag) || 0) > 0 ? lagText(g.maxTimeLag) : '—',
    behind: (toNum(g.partitionsWithLag) || 0) > 0 ? formatNumber(toNum(g.partitionsWithLag)) : '—',
    to: queueTabLocation('consumers', g.queueName, route, { ...carry.value, group: g.name }),
  }
}
const groupRowsAll = computed(() => nsGroups.value.rows.slice(0, 200).map(groupRowFor))

// Dead letters by queue, the namespace's, most first: a snapshot of the queue
// listing, so no request of its own.
const dlqRows = computed(() => nsQueues.value
  .map((q) => ({ name: q.name, namespace: q.namespace || '', count: toNum(q.messages?.deadLetter) || 0 }))
  .filter((d) => d.count > 0)
  .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)))

// The Cell view's note while there is no cell to show.
const nodesNote = computed(() => {
  if (isMissingRoute(membersQ.error.value)) return 'This broker does not list its nodes'
  if (membersQ.error.value) return `Cannot read the nodes · ${describeApiError(membersQ.error.value)}`
  return 'Reading the nodes…'
})

// ---------------------------------------------------------------------------
// The head: the title and the facts of the view.
// ---------------------------------------------------------------------------
// The Summary's status card: the verdict's word and tone, the scope it was
// worked out over, and the five numbers the paragraph used to carry.
const verdictTone = computed(() => (verdict.value.sev === 'bad' || verdict.value.sev === 'warn' ? verdict.value.sev : ''))
// What the tenant holds, under the status card: each count with what it
// means now, and the way to where it is listed.
const inventory = computed(() => {
  const queuesKnown = queuesRead.value && !queuesFailed.value
  const groupsKnown = groupsRead.value && !consumersFailed.value
  const needs = queueIssues.value.length
  const behind = groupsBehind.value.length
  const stored = overviewFailed.value ? null : toNum(overview.value?.messages?.total)
  const bytes = queues.value.reduce((s, q) => s + (toNum(q.retainedBytes) || 0), 0)
  const made = partitionCreatedTotal.value
  const gone = partitionDeletedTotal.value
  return [
    {
      k: 'Queues',
      v: queuesKnown ? formatNumber(queues.value.length) : '—',
      sub: !queuesKnown || !groupsKnown ? '' : needs ? `${formatNumber(needs)} ${needs === 1 ? 'needs' : 'need'} you` : 'none needs you',
      tone: worstMark(queueIssues.value.map((i) => i.sev)) || '',
      to: queueTabLocation('overview', null, route),
      title: 'Open the queues',
    },
    {
      k: 'Partitions',
      v: queuesKnown ? formatNumber(totalPartitions.value) : '—',
      sub: made || gone ? `+${formatNumber(made)} / −${formatNumber(gone)} in ${selectedRange.value}` : `none made or removed in ${selectedRange.value}`,
      to: viewTo('backlog'),
      title: 'Partitions made and removed in the window; Backlog draws them',
    },
    {
      k: 'Consumer groups',
      v: groupsKnown ? formatNumber(consumers.value.length) : '—',
      sub: !groupsKnown ? '' : behind ? `${formatNumber(behind)} behind` : 'none behind',
      tone: worstMark(groupsBehind.value) || '',
      to: viewTo('groups'),
      title: 'Open the consumer groups, the worst first',
    },
    {
      k: 'Messages stored',
      v: stored === null ? '—' : formatNumber(stored),
      sub: bytes ? formatBytes(bytes) : '',
      to: viewTo('backlog'),
      title: 'Every message the queues keep now; Backlog shows what retention removes',
    },
  ]
})
const statusCells = computed(() => [
  {
    k: 'In',
    v: pushNow.value === null ? '—' : `${perSecond(pushNow.value)}/s`,
    title: 'Messages pushed per second, the latest bucket of the window',
  },
  {
    k: 'Out',
    v: popNow.value === null ? '—' : `${perSecond(popNow.value)}/s`,
    title: `Messages delivered per second, the latest bucket${fetchNow.value !== null ? `; Kafka clients fetch ${fmtMsgRate(fetchNow.value)} of it` : ''}`,
  },
  {
    k: 'Pending',
    v: pendingNow.value === null ? '—' : formatNumber(pendingNow.value),
    tone: pendingNow.value === null ? '' : pendingNumClass(pendingNow.value),
    title: 'Waiting to be read now, excluding in-flight work. The tone is how long the work takes at this window\'s ack rate',
  },
  {
    k: 'Oldest',
    v: lagMaxSeconds.value === null ? '—' : lagText(lagMaxSeconds.value),
    tone: lagMaxSeconds.value === null ? '' : lagSeverity.value,
    title: 'How long the oldest message has waited',
  },
  {
    k: 'Failed acks',
    v: ackAttempts.value > 0 ? ackFailPct.value : '—',
    tone: ackAttempts.value > 0 ? errorSeverity.value : '',
    title: ackAttempts.value > 0 ? `${formatNumber(ackFailedTotal.value)} of ${formatNumber(ackAttempts.value)} acks failed in the window` : 'No acks in the window',
  },
])

// Every view is titled by its name; the Summary's verdict is its first card.
const title = computed(() => currentName.value)
const dash = (v) => (v === null || v === undefined ? '—' : formatNumber(v))
const facts = computed(() => {
  const v = view.value
  const t = queuesQ.data.value === null ? null : queueTotals(nsQueues.value)
  const cellFact = () => (actingCellSlug.value
    ? [{ k: 'Cell', v: actingCellSlug.value, title: `Shared by every tenant on this cell, not just ${actingTenantSlug.value || 'this tenant'}` }]
    : [])
  if (v === 'issues') {
    const out = [{ k: 'Queues', v: formatNumber(queueIssues.value.length), tone: worstMark(queueIssues.value.map((i) => i.sev)) || '' }]
    if (canOperate.value) out.push({ k: 'Cell', v: formatNumber(cellIssueList.value.length), tone: worstMark(cellIssueList.value.map((i) => i.sev)) || '' })
    if (issues.value.some((i) => i.tenant)) out.push({ k: 'Ack failures', v: ackFailPct.value, tone: errorSeverity.value || '' })
    return out
  }
  if (v === 'throughput') {
    if (!t) return []
    return [
      { k: 'Queues', v: formatNumber(t.queues) },
      { k: 'In', v: pushNow.value === null ? '—' : fmtMsgRate(pushNow.value) },
      { k: 'Out', v: popNow.value === null ? '—' : fmtMsgRate(popNow.value) },
    ]
  }
  if (v === 'backlog') {
    if (!t) return []
    return [
      { k: 'Queues', v: formatNumber(t.queues) },
      { k: 'Pending', v: dash(t.pending) },
      { k: 'Oldest', v: lagMaxSeconds.value === null ? '—' : lagText(lagMaxSeconds.value), tone: lagSeverity.value || '' },
      { k: 'Stored', v: t.retainedBytes === null ? '—' : formatBytes(t.retainedBytes) },
    ]
  }
  if (v === 'groups') {
    if (consumersQ.data.value === null) return []
    const behind = nsGroups.value.rows.filter((g) => { const sev = groupAttention(g, linesFor); return sev === 'warn' || sev === 'bad' }).length
    return [
      { k: 'Groups', v: formatNumber(nsGroups.value.rows.length) },
      { k: 'Behind', v: formatNumber(behind), tone: behind ? worstMark(groupsBehind.value) || '' : '' },
    ]
  }
  if (v === 'dead-letters') {
    return [
      { k: 'Dead letters', v: t ? dash(t.deadLetter) : '—' },
      { k: 'Failed acks', v: formatNumber(ackFailedTotal.value), tone: errorSeverity.value || '' },
      { k: 'Failure rate', v: ackFailPct.value },
    ]
  }
  return []
})

// ---------------------------------------------------------------------------
// Loading. Each view asks for what it draws; the rest waits for its view.
// ---------------------------------------------------------------------------
const viewJobs = (v) => {
  if (v === 'backlog') return [retentionQ.refresh(), refreshSole()]
  return []
}
const fetchAll = async () => {
  // The list's numbers come from these on every view: the queues, the
  // groups, the window's queue-ops and the tenant's overview.
  const jobs = [
    queuesQ.refresh().then(refreshPartitions),
    consumersQ.refresh(),
    opsQ.refresh(),
    overviewQ.refresh(),
    queueOpsStore.fetchOps(),
    ...viewJobs(view.value),
  ]
  // The members are an operator route: for anyone else the proxy answers 404
  // route_blocked, so they are not asked.
  if (canOperate.value) jobs.push(membersQ.refresh())
  await Promise.all(jobs)
  lastRefreshAt.value = Date.now()
}

// One shared ticker for the whole app (paused while the tab is hidden).
useAutoRefresh(fetchAll)

// A view opened for the first time reads what it draws now, not at the tick.
watch(view, (v) => {
  viewJobs(v)
  if (v === 'summary' && queuesQ.data.value !== null) refreshPartitions()
})
watch(soleQueue, refreshSole)
watch(selectedRange, () => {
  opsQ.refresh()
  viewJobs(view.value)
})

onMounted(fetchAll)
</script>

<style scoped>
/* ---------------------------------------------------------------------------
   The sub sidebar: three rows, nothing above them but the column's air.
   --------------------------------------------------------------------------- */
.ov-side { padding-top: 12px; }
/* The number is the mark: coral when it is failing, amber when it needs you. */
.ov-side .ss-num.bad { color: var(--ember-400); }
.ov-side .ss-num.warn { color: var(--warn-400); }
/* The Overview's word ("Healthy") is a word, not a number. */
.ss-num.ov-side-word { font-family: inherit; font-size: 12px; }

/* ---------------------------------------------------------------------------
   The page: the Queues section's rhythm — the title 18px from the top, cards
   16px apart.
   --------------------------------------------------------------------------- */
.ov { display: flex; flex-direction: column; gap: 16px; }

.ov-head { display: flex; align-items: center; gap: 10px 16px; flex-wrap: wrap; min-height: 32px; margin-top: 4px; }
.ov-id { display: flex; align-items: center; gap: 10px; min-width: 0; max-width: 100%; }
.ov-title {
  margin: 0; min-width: 0;
  font-size: 18px; font-weight: 600; letter-spacing: -0.01em; color: var(--text-hi);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.ov-word { flex: none; font-size: 12px; color: var(--text-low); }

/* The Summary's status card: the verdict on the left, five number cells
   beside it, hairlines between; colour only on a number that is trouble.
   What needs you follows in the same card, under a hairline. */
.ov-summary { overflow: hidden; }
.ov-status {
  display: grid; grid-template-columns: minmax(260px, 1.5fr) repeat(5, minmax(0, 1fr));
}

/* What the tenant holds: four counts in one card, hairlines between, each a
   link to where it is listed; the line under a count says what it means now. */
.ov-inventory {
  display: grid; grid-template-columns: repeat(4, minmax(0, 1fr));
  border: 1px solid var(--bd); border-radius: var(--r-card); background: var(--ink-2); overflow: hidden;
}
.ov-inv {
  display: flex; flex-direction: column; gap: 3px; min-width: 0;
  padding: 13px 16px; border-right: 1px solid var(--bd);
  color: inherit; text-decoration: none; transition: background-color .12s;
}
.ov-inv:last-child { border-right: 0; }
.ov-inv:hover { background: var(--ink-3); }
.ov-inv:focus-visible { outline-offset: -2px; }
.ov-inv-k { font-size: 11.5px; color: var(--text-low); }
.ov-inv-v { font-size: 20px; font-weight: 600; letter-spacing: -0.015em; color: var(--text-hi); font-variant-numeric: tabular-nums; }
.ov-inv-sub { font-size: 12px; color: var(--text-mid); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-variant-numeric: tabular-nums; }
.ov-inv-sub.bad { color: var(--ember-400); }
.ov-inv-sub.warn { color: var(--warn-400); }
@media (max-width: 760px) {
  .ov-inventory { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .ov-inv:nth-child(2) { border-right: 0; }
  .ov-inv:nth-child(-n+2) { border-bottom: 1px solid var(--bd); }
}

/* The hour in four charts, two a row (one on a narrow screen). The title is
   the way to the view. */
.ov-trends { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
@media (max-width: 900px) { .ov-trends { grid-template-columns: minmax(0, 1fr); } }
.ov-trend { min-width: 0; }
.ov-trend-title {
  display: inline-flex; align-items: center; gap: 2px;
  font-size: 13px; font-weight: 600; color: var(--text-hi); text-decoration: none;
}
.ov-trend-title :deep(svg) { color: var(--text-low); transition: transform .12s, color .12s; }
.ov-trend-title:hover :deep(svg) { color: var(--text-hi); transform: translateX(2px); }
.ov-flower { align-self: stretch; }
.ov-verdict { min-width: 0; padding: 15px 18px; border-right: 1px solid var(--bd); }
.ov-verdict-word {
  display: inline-flex; align-items: center; gap: 7px; margin-bottom: 6px;
  font-size: 12px; font-weight: 600; color: var(--text-mid);
}
.ov-verdict-word.bad { color: var(--ember-400); }
.ov-verdict-word.warn { color: var(--warn-400); }
.ov-verdict-title {
  margin: 0; font-size: 16px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.3; color: var(--text-hi);
  text-wrap: balance;
}
.ov-verdict-line { margin: 5px 0 0; font-size: 12px; color: var(--text-low); }
.ov-kpi {
  min-width: 0; padding: 15px 16px; border-right: 1px solid var(--bd);
  display: flex; flex-direction: column; justify-content: center; gap: 5px;
}
.ov-kpi:last-child { border-right: 0; }
.ov-kpi-k { font-size: 11.5px; color: var(--text-low); white-space: nowrap; }
.ov-kpi-v {
  font-size: 18px; font-weight: 600; letter-spacing: -0.01em; color: var(--text-hi);
  font-variant-numeric: tabular-nums; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.ov-kpi-v.bad { color: var(--ember-400); }
.ov-kpi-v.warn { color: var(--warn-400); }

/* Narrow: the verdict on its own row, the cells under it, three a row. */
@media (max-width: 1100px) {
  .ov-status { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .ov-verdict { grid-column: 1 / -1; border-right: 0; border-bottom: 1px solid var(--bd); }
  .ov-kpi { border-bottom: 1px solid var(--bd); }
  .ov-kpi:nth-child(4) { border-right: 0; }
  .ov-kpi:nth-child(n+5) { border-bottom: 0; }
}
@media (max-width: 480px) {
  .ov-kpi { padding: 12px; }
  .ov-kpi-v { font-size: 16px; }
}
.ov-facts { display: flex; align-items: baseline; gap: 4px 16px; flex-wrap: wrap; min-width: 0; }
.ov-fact {
  display: inline-flex; align-items: baseline; gap: 5px;
  font-size: 12.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap;
}
.ov-k { color: var(--text-low); }
.ov-fact .warn { color: var(--warn-400); }
.ov-fact .bad { color: var(--ember-400); }
.ov-actions { margin-left: auto; display: flex; align-items: center; gap: 8px 10px; flex-wrap: wrap; }
.ov-actions > .chip { white-space: normal; }
.ov-ns { width: 220px; max-width: 100%; }

/* ---------------------------------------------------------------------------
   Overview: the shared OVERVIEW blocks (style.css), on the 4px grid.
   --------------------------------------------------------------------------- */
.hero { gap: 16px 24px; }
.hero-left { gap: 16px; }
a.count-tile { text-decoration: none; cursor: pointer; }
a.count-tile:hover { background: var(--ink-3); }
/* Inside a clipping frame the focus ring sits within the element. */
a.count-tile:focus-visible, a.ov-lrow:focus-visible { outline-offset: -2px; }

/* The rules under the issues list, on the card's 14px edge. */
.issues-rules { padding: 8px 14px 10px; }

:deep(.mr-sep) { color: var(--text-faint); margin: 0 4px; font-weight: 400; }
:deep(.mr-unit) { font-style: normal; color: var(--text-low); margin-left: 4px; font-size: 12px; font-weight: 400; }

/* ---------------------------------------------------------------------------
   Cards side by side, one column when the page is narrow.
   --------------------------------------------------------------------------- */
.ov-pair { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; align-items: start; }
/* As tall as the chart that takes their place: RowChart's 210px, and the
   per-node charts' 240px with their legend inside. */
.ov-chart-skeleton { height: 210px; }
.ov-chart-note {
  height: 210px; display: flex; align-items: center; justify-content: center;
  font-size: 12px; color: var(--text-low); text-align: center;
}
.ov-tall .ov-chart-skeleton, .ov-tall .ov-chart-note { height: 240px; }
.ov-quiet { padding: 14px; font-size: 13px; color: var(--text-mid); }
.ov-more {
  margin-left: auto; flex: none;
  display: inline-flex; align-items: center; gap: 2px;
  font-size: 12px; color: var(--text-mid); text-decoration: none; white-space: nowrap;
}
.ov-more:hover { color: var(--text-hi); }
.card-header .muted + .ov-more { margin-left: 12px; }

/* ---------------------------------------------------------------------------
   Lists: the Queues table's rows — a label row, then one link per row on a
   hairline, numbers right-aligned on a common baseline.
   --------------------------------------------------------------------------- */
.ov-list { display: flex; flex-direction: column; }
/* Two list cards side by side (one on a narrow screen). */
.ov-pair { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; align-items: start; }
@media (max-width: 1100px) { .ov-pair { grid-template-columns: minmax(0, 1fr); } }
.ov-lhead, .ov-lrow {
  display: grid; align-items: center; gap: 12px;
  padding: 0 14px;
}
.ov-cols-att .ov-lhead, .ov-cols-att .ov-lrow,
.ov-cols-groups .ov-lhead, .ov-cols-groups .ov-lrow { grid-template-columns: 8px minmax(0, 1fr) 72px 72px; }
.ov-cols-busy .ov-lhead, .ov-cols-busy .ov-lrow { grid-template-columns: 8px minmax(0, 1fr) 64px 64px; }
.ov-cols-issues .ov-lrow { grid-template-columns: 8px minmax(0, 1fr); }
.ov-cols-dlq .ov-lhead, .ov-cols-dlq .ov-lrow { grid-template-columns: 8px minmax(0, 1fr) 96px; }
/* Two tiles side by side: a view's two small trends. */
.ov-tiles-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
@media (max-width: 640px) { .ov-tiles-2 { grid-template-columns: minmax(0, 1fr); } }
.ov-lhead {
  height: 32px; border-bottom: 1px solid var(--bd);
  font-size: 12px; font-weight: 500; color: var(--text-low);
}
.ov-lrow {
  min-height: 40px; padding-top: 6px; padding-bottom: 6px;
  border-bottom: 1px solid var(--bd-soft);
  font-size: 13px; color: var(--text-mid); text-decoration: none;
  transition: background .12s var(--ease);
}
.ov-lrow:last-child { border-bottom: 0; }
a.ov-lrow:hover { background: var(--ink-3); }
.ov-lrow .ss-mark { justify-content: center; }
.ov-r { text-align: right; }
.ov-cell { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
.ov-name { color: var(--text-hi); font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ov-sub { font-size: 12px; color: var(--text-low); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ov-wrap { white-space: normal; overflow-wrap: anywhere; }
.ov-n { text-align: right; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap; }
.ov-n.warn { color: var(--warn-400); }
.ov-n.bad { color: var(--ember-400); }
.ov-lrow.ov-skel { grid-template-columns: 8px minmax(0, 1fr) 64px; }
.ov-skel .skeleton { height: 12px; width: 60%; }

/* ---------------------------------------------------------------------------
   Server: the nodes table scrolls inside its card on a phone.
   --------------------------------------------------------------------------- */
.ov-scroll { overflow-x: auto; }
.ov-nodes { min-width: 760px; }
.ov-nodes th:first-child, .ov-nodes td:first-child { padding-left: 14px; }
.ov-nodes th:last-child, .ov-nodes td:last-child { padding-right: 14px; }
.ov-nodes td { white-space: nowrap; color: var(--text-mid); }
.ov-nodes td.ov-node { color: var(--text-hi); font-weight: 500; }
.ov-node .ss-mark { display: inline-flex; margin-right: 10px; vertical-align: middle; }
.ov-nodes td.num { color: var(--text-hi); }
.ov-nodes td.warn { color: var(--warn-400); }
.ov-nodes td.bad { color: var(--ember-400); }

@media (max-width: 1100px) {
  .ov-pair { grid-template-columns: minmax(0, 1fr); }
}
@media (max-width: 640px) {
  .ov-title { font-size: 17px; }
  /* The verdict is a sentence: on a phone it wraps rather than hides. */
  .ov-title-verdict { white-space: normal; text-wrap: balance; }
  .ov-actions { margin-left: 0; width: 100%; }
  .ov-ns { flex: 1 1 100%; width: auto; }
  .ov-lhead, .ov-lrow { padding-left: 12px; padding-right: 12px; gap: 10px; }
  .ov-quiet { padding: 12px; }
  .issues-rules { padding-left: 12px; padding-right: 12px; }
  .ov-nodes th:first-child, .ov-nodes td:first-child { padding-left: 12px; }
  .ov-nodes th:last-child, .ov-nodes td:last-child { padding-right: 12px; }
}
</style>
