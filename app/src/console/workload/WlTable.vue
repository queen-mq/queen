<script setup>
// One row per namespace, task or queue, in a fixed order (busiest first when
// the scope was read; a refresh moves no row), and three sets of columns:
// the share of the work over the window, backlog and efficiency, and the
// footprint. A click on a group looks inside it (the side list's pick); on a
// queue it opens its consumer groups on the right. A queue's name opens its
// own page.
import { computed } from 'vue'
import { TABS, span } from './workloadViews'

const props = defineProps({
  rows: { type: Array, default: () => [] },     // tableRows()
  tab: { type: String, default: 'work' },
  level: { type: String, default: 'namespace' },
  picked: { type: String, default: '' },
  fetched: { type: Boolean, default: false },   // some row was read by Kafka fetches
  compared: { type: String, default: '' },      // the window compared with, in words; '' when none
  readCall: { type: String, default: '' },
  inThe: { type: String, default: '' },
  hasGroups: { type: Boolean, default: true },
  groupsError: { type: String, default: '' },
  compact: { type: Boolean, default: false },   // a queue's groups are open beside the table
  lines: { type: Object, default: null },       // the tenant's alert lines (settingsStore.lines)
  fallback: { type: Boolean, default: false },  // rolled up in this browser: the broker has no workload route
})
const emit = defineEmits(['tab', 'open'])

const queueLevel = computed(() => props.level === 'queue')
const nameHead = computed(() => (props.level === 'task' ? 'Task' : props.level === 'queue' ? 'Queue' : 'Namespace'))
// Each column's least width fits its longest reading, so a number is never
// cut; under the sum the table scrolls inside itself. Beside a queue's groups
// the sub-readings go (their titles keep them) and the columns tighten.
const tracks = computed(() => {
  const c = props.compact
  if (props.tab === 'backlog') return c ? [[150, 1.4], [70], [88], [66], [66], [66], [124]] : [[180, 2], [110], [104], [84], [124], [124], [150]]
  if (props.tab === 'footprint') {
    return (c ? [[150, 1.4], null, [96], [130, 1], [130, 1], [116]] : [[180, 2], queueLevel.value ? null : [70], [150], [180, 1.3], [180, 1.3], [134]]).filter(Boolean)
  }
  return (c ? [[150, 1.1], [180, 2], [96], [96], props.fetched ? [110] : null] : [[180, 1.1], [240, 2], [110], [110], props.fetched ? [124] : null]).filter(Boolean)
})
const gap = computed(() => (props.compact ? 12 : 14))
const cols = computed(() => tracks.value.map(([min, fr]) => (fr ? `minmax(${min}px, ${fr}fr)` : `${min}px`)).join(' '))
const minWidth = computed(() => tracks.value.reduce((s, [min]) => s + min, 0) + gap.value * (tracks.value.length - 1))
const pctOf = (v) => `${Math.max(0, Math.min(100, (v || 0) * 100))}%`
function open(r) { emit('open', r.key) }
</script>

<template>
  <section class="wlt" aria-label="The work by row">
    <nav class="qn-tabs" role="tablist" aria-label="Which columns">
      <button v-for="t in TABS" :key="t.id" type="button" class="qn-tab" role="tab" :aria-selected="tab === t.id" @click="emit('tab', t.id)">{{ t.label }}</button>
    </nav>

    <p v-if="!rows.length" class="qn-t2 wlt-empty">No {{ nameHead.toLowerCase() }} in this scope.</p>
    <div v-else class="wlt-scroll">
      <div class="wlt-table" :class="[`t-${tab}`, { compact }]" role="table" :aria-label="`${TABS.find((t) => t.id === tab)?.label}, by ${nameHead.toLowerCase()}`" :style="{ '--cols': cols, minWidth: minWidth + 'px', columnGap: gap + 'px' }">
        <div class="qn-tr head" role="row">
          <span role="columnheader" class="wlt-namehead">{{ nameHead }}</span>
          <template v-if="tab === 'work'">
            <span role="columnheader" class="ph" title="Deliveries and Kafka fetches, of the tenant's"><span class="lg">Share of the reads</span><span class="sm">Share</span></span>
            <span role="columnheader" class="r ph">Delivered</span>
            <span role="columnheader" class="r">Pushed</span>
            <span v-if="fetched" role="columnheader" class="r" title="Records Kafka clients fetched: a read, not a delivery">Fetched by Kafka</span>
          </template>
          <template v-else-if="tab === 'backlog'">
            <span role="columnheader" class="r">Pending</span>
            <span role="columnheader" class="r ph" title="The oldest message the slowest live group has not read"><span class="lg">Oldest waiting</span><span class="sm">Oldest</span></span>
            <span role="columnheader" class="r">In DLQ</span>
            <span role="columnheader" class="r" title="Of the polls, the share that brought a message">Fill</span>
            <span role="columnheader" class="r ph" title="Of the acks, the share that succeeded">Ack ok</span>
            <span role="columnheader" class="r" title="How long a message waited for its pop: on average, then at worst">Lag at pop</span>
          </template>
          <template v-else>
            <span v-if="!queueLevel" role="columnheader" class="r">Queues</span>
            <span role="columnheader" class="r">Consumer groups</span>
            <span role="columnheader" class="ph"><span class="lg">Partitions</span><span class="sm r">Partitions</span></span>
            <span role="columnheader" class="ph"><span class="lg">Retained</span><span class="sm r">Retained</span></span>
            <span role="columnheader" class="r" title="Partitions created / deleted in the window">Partition churn</span>
          </template>
        </div>

        <div
          v-for="r in rows" :key="r.key" class="qn-tr click wlt-row" :class="{ on: queueLevel && picked === r.key }" role="row"
          tabindex="0" :aria-selected="queueLevel ? picked === r.key : undefined" @click="open(r)" @keydown.enter.prevent="open(r)"
        >
          <span role="rowheader" class="wlt-name">
            <span class="wlt-markslot" aria-hidden="true"><span v-if="r.mark" class="qn-mark" :class="r.mark" /></span>
            <RouterLink v-if="r.to" :to="r.to" class="qn-mono wlt-link" :class="{ strong: r.mark }" :title="`Open ${r.name}`" @click.stop>{{ r.name }}</RouterLink>
            <span v-else :class="[r.mono ? 'qn-mono' : 'qn-t2', { strong: r.mark }]" :title="r.name">{{ r.name }}</span>
          </span>

          <template v-if="tab === 'work'">
            <span role="cell" class="wlt-share ph">
              <span class="wlt-bar" aria-hidden="true">
                <span class="wlt-fill" :style="{ width: pctOf(r.share.value) }" />
                <i v-if="r.share.prev !== null" class="wlt-tick" :style="{ left: pctOf(r.share.prev) }" />
              </span>
              <span class="qn-num wlt-pct">{{ r.share.text }}</span>
            </span>
            <span role="cell" class="r qn-num ph" :class="{ 'qn-t3': r.delivered.dim }">{{ r.delivered.text }}</span>
            <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.pushed.dim }">{{ r.pushed.text }}</span>
            <span v-if="fetched" role="cell" class="r qn-num" :class="{ 'qn-t3': r.fetched === '0' }">{{ r.fetched }}</span>
          </template>

          <template v-else-if="tab === 'backlog'">
            <span role="cell" class="r qn-num" :title="r.pending.title || undefined">
              <span :class="r.pending.tone || (r.pending.dim ? 'qn-t3' : '')">{{ r.pending.text }}</span><span v-if="r.pending.sub" class="wlt-sub" :class="r.pending.subTone || 'qn-t3'">{{ ' · ' + r.pending.sub }}</span>
            </span>
            <span role="cell" class="r qn-num ph" :class="r.oldest.tone || (r.oldest.dim ? 'qn-t3' : '')" :title="r.oldest.title || undefined">{{ r.oldest.text }}</span>
            <span role="cell" class="r qn-num">
              <RouterLink v-if="r.dlq.link" :to="r.dlq.link" class="wlt-dlq" :title="`The dead letters of ${r.name}`" @click.stop>{{ r.dlq.text }}</RouterLink>
              <span v-else :class="{ 'qn-t3': r.dlq.dim }">{{ r.dlq.text }}</span>
            </span>
            <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.fill.dim }" :title="[r.fill.title, compact ? r.fill.sub : ''].filter(Boolean).join('; ') || undefined">{{ r.fill.text }}<span v-if="r.fill.sub" class="qn-t3 wlt-sub"> · {{ r.fill.sub }}</span></span>
            <span role="cell" class="r qn-num ph" :class="r.ack.tone || (r.ack.dim ? 'qn-t3' : '')" :title="r.ack.title || undefined">{{ r.ack.text }}<span v-if="r.ack.sub" class="wlt-sub" :class="r.ack.tone || 'qn-t3'"> · {{ r.ack.sub }}</span></span>
            <span role="cell" class="r qn-num" :class="r.lag.tone || (r.lag.dim ? 'qn-t3' : '')" :title="r.lag.title || undefined">{{ r.lag.text }}</span>
          </template>

          <template v-else>
            <span v-if="!queueLevel" role="cell" class="r qn-num">{{ r.queues }}</span>
            <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.groups.dim }" :title="r.groups.sub || undefined">{{ r.groups.text }}<span v-if="r.groups.sub" class="qn-t3 wlt-sub"> · {{ r.groups.sub }}</span></span>
            <span role="cell" class="wlt-share ph" :title="r.partitions.sub || undefined">
              <span class="wlt-bar" aria-hidden="true"><span class="wlt-fill" :style="{ width: pctOf(r.partitions.bar) }" /></span>
              <span class="qn-num wlt-pct">{{ r.partitions.text }}<span v-if="r.partitions.sub" class="qn-t3 wlt-sub"> · {{ r.partitions.sub }}</span></span>
            </span>
            <span role="cell" class="wlt-share ph">
              <span class="wlt-bar" aria-hidden="true"><span class="wlt-fill" :style="{ width: pctOf(r.retained.bar) }" /></span>
              <span class="qn-num wlt-pct">{{ r.retained.text }}</span>
            </span>
            <span role="cell" class="r qn-num" :class="{ 'qn-t3': r.churn.dim }">{{ r.churn.text }}</span>
          </template>
        </div>
      </div>
    </div>

    <p v-if="!hasGroups && tab === 'backlog'" class="qn-t2 wlt-note">
      <span class="qn-mark amber" aria-hidden="true" />The consumer groups did not load{{ groupsError ? `: ${groupsError}` : '' }}, so how far behind each row is, and which messages no group reads, cannot be said.
    </p>
    <p class="qn-read wlt-read">
      <template v-if="tab === 'work'">
        <template v-if="fallback">Rolled up in this browser from <span class="qn-mono">GET /api/v1/analytics/queue-ops</span>, <span class="qn-mono">GET /api/v1/status/queues</span>, the queue list and the consumer groups, every 30 s: this broker has no workload route.</template>
        <template v-else>Read from <span class="qn-mono">{{ readCall }}</span>, every 30 s.</template> A share is of the tenant's reads: its deliveries, and the records Kafka clients fetched.<span v-if="compared" class="wlt-tickline"> The tick on a bar is its share {{ compared }}.</span>
      </template>
      <template v-else-if="tab === 'backlog'">
        Oldest waiting is from <span class="qn-mono">GET /api/v1/consumer-groups</span><template v-if="lines?.lagWarnSeconds">: amber from {{ span(lines.lagWarnSeconds) }}, red from {{ span(lines.lagBadSeconds) }}, unless a queue has lines of its own</template>.
        Fill is the share of polls that brought a message; lag at pop is how long a message waited for its pop, on average and at worst, amber from a minute; both {{ inThe }}.
      </template>
      <template v-else>
        Queues, groups, partitions and bytes are now; churn is the partitions created and deleted {{ inThe }}. A bar is a share of the tenant's.
      </template>
    </p>
  </section>
</template>

<style scoped>
.wlt-empty { padding: 18px 0 4px; }
.wlt-scroll { overflow-x: auto; margin-top: 4px; }
.wlt-table .qn-tr { column-gap: inherit; }
.wlt-table .sm { display: none; }
.wlt-table.compact .wlt-sub { display: none; }
.wlt-table .qn-tr:not(.head) { font-size: 13px; }
.wlt-namehead { padding-left: 18px; }
.wlt-name { display: flex; align-items: center; gap: 10px; }
.wlt-markslot { flex: 0 0 8px; display: flex; align-items: center; justify-content: center; }
.wlt-name > :last-child { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.wlt-name .strong { font-weight: 500; }
.wlt-link:hover { text-decoration: underline; }
.wlt-sub { font-size: 12px; }
.wlt-share { display: flex; align-items: center; gap: 10px; }
.wlt-bar { position: relative; flex: 1 1 0; min-width: 48px; max-width: 300px; height: 6px; border-radius: 3px; background: var(--line); }
.wlt-fill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 3px; background: var(--text3); min-width: 0; }
.wlt-tick { position: absolute; top: -3px; bottom: -3px; width: 1.5px; margin-left: -0.75px; background: var(--text); opacity: .7; }
.wlt-pct { flex: 0 0 auto; min-width: 3.4em; overflow: hidden; text-overflow: ellipsis; }
.wlt-dlq { text-decoration: underline; text-decoration-color: var(--line); text-underline-offset: 3px; }
.wlt-dlq:hover { text-decoration-color: currentColor; }
.wlt-note { display: flex; align-items: center; gap: 8px; margin-top: 12px; }
.wlt-read { margin-top: 12px; max-width: 980px; }
:global(.qn.tone .wlt-bar) { background: var(--bg3); }
/* a phone: the name and the two readings of the tab that say the most */
@media (max-width: 640px) {
  .wlt-table { min-width: 0 !important; }
  .wlt-table.t-work { --cols: minmax(0, 1fr) 54px 72px !important; }
  .wlt-table.t-backlog { --cols: minmax(0, 1fr) 76px 58px !important; }
  .wlt-table.t-footprint { --cols: minmax(0, 1fr) 64px 70px !important; }
  .wlt-table .qn-tr > :not(.ph):not(:first-child) { display: none; }
  .wlt-table .wlt-bar, .wlt-table .wlt-sub, .wlt-table .lg { display: none; }
  .wlt-table .sm { display: inline; }
  .wlt-table .wlt-share { justify-content: flex-end; }
  .wlt-namehead { padding-left: 18px; }
  .wlt-tickline { display: none; }
}
:global(.qn.tone .wlt-dlq) { text-decoration-color: var(--text3); }
</style>
