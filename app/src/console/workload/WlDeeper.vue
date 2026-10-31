<script setup>
// The deeper layer, read when you ask: the week by hour, whether this hour
// is normal, what one push costs, where messages go (retention, eviction,
// partitions, dead letters) and where the bytes are, then what it all adds
// up to. Narrowed to the side list's pick, as the rows above are. Hours are
// UTC here: the buckets are, and a week that crosses a clock change would
// smear a daily job over two local hours.
import { computed } from 'vue'
import { clock } from '../useWindow'
import { fmt, parts, scopeName } from './workloadViews'

const props = defineProps({
  d: { type: Object, required: true },       // useWorkloadPage().deeper
  open: { type: Boolean, default: false },
  scope: { type: Object, required: true },   // scopeOf()
  inThe: { type: String, default: '' },
  fallback: { type: Boolean, default: false },  // the broker has no workload route: rolled up here
})
const emit = defineEmits(['read', 'pick'])

const who = computed(() => (props.scope.key === null ? 'every queue' : props.scope.key ? `\`${props.scope.key}\`` : scopeName(props.scope).toLowerCase()))
const HOURS = ['00:00', '06:00', '12:00', '18:00']
const shade = (p) => `color-mix(in srgb, var(--text) ${Math.max(0, Math.min(100, p))}%, var(--wld-ground))`
const cellTip = (day, c) => `${day} ${String(c.h).padStart(2, '0')}:00 UTC: ${c.na ? 'not recorded' : `${fmt(c.v)} delivered`}`
const compared = computed(() => props.d.baseline.rows.some((r) => !r.tenant && r.bar !== null) || props.d.baseline.rows[0]?.bar != null)
const pctOf = (v) => `${Math.max(0, Math.min(100, (v || 0) * 100))}%`
const queueTo = (q) => `/queues/${encodeURIComponent(q)}`
</script>

<template>
  <section class="wld" aria-labelledby="wld-title">
    <div class="wld-head">
      <h2 id="wld-title" class="wld-title">Deeper: the last 7 days and 24 hours</h2>
      <button type="button" class="qn-btn2 wld-btn" :disabled="d.loading" @click="emit('read')">{{ d.loading ? 'Reading…' : d.at ? 'Read again' : 'Read them' }}</button>
      <span v-if="d.at && !d.loading" class="qn-t3 qn-num wld-at">read at {{ clock(d.at) }}</span>
    </div>
    <p class="qn-t2 wld-sub">
      Is this hour normal, what one push costs, where messages go and where the bytes are.
      Read when you ask, not every 30 s: a week is the heaviest read on this page.
    </p>

    <template v-if="open">
      <p v-if="d.partial.length" class="qn-t2 wld-line">
        {{ d.partial.length === 1 ? `Node ${d.partial[0]} did not answer these reads: its traffic is missing from them.` : `Nodes ${d.partial.join(', ')} did not answer these reads: their traffic is missing from them.` }}
      </p>

      <div class="wld-two">
        <!-- the week -->
        <div class="wld-part">
          <h3 class="wld-h"><span>The week, by hour in UTC</span> <span class="qn-t3 wld-for"><template v-for="(p, i) in parts(who)" :key="i"><span :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></template></span></h3>
          <p v-if="d.weekState" class="qn-t2 wld-state">{{ d.weekState }}</p>
          <template v-else-if="d.week.has">
            <div class="wlk" role="img" :aria-label="`Deliveries by day and hour, UTC, for ${who.replace(/`/g, '')}`">
              <div v-for="row in d.week.rows" :key="row.day" class="wlk-row">
                <span class="wlk-day qn-t3">{{ row.day }}</span>
                <div class="wlk-cells">
                  <i v-for="c in row.cells" :key="c.h" class="wlk-cell" :class="{ na: c.na }" :style="c.na ? null : { background: shade(c.p) }" :title="cellTip(row.day, c)" />
                </div>
              </div>
              <div class="wlk-row wlk-axis qn-num qn-t3" aria-hidden="true"><span /><div class="wlk-ax"><span v-for="h in HOURS" :key="h">{{ h }}</span></div></div>
            </div>
            <p class="qn-t2 wld-stat">{{ d.week.stats }}<template v-if="d.recorded && d.recorded < 168"> The broker holds {{ fmt(d.recorded) }} of these 168 hours; the outlined ones were not recorded.</template></p>
          </template>
          <p v-else class="qn-t2 wld-state">Nothing recorded for {{ who.replace(/`/g, '') }} in the last 7 days.</p>
        </div>

        <!-- is this hour normal -->
        <div class="wld-part">
          <h3 class="wld-h">Is this hour normal</h3>
          <p v-if="d.weekState" class="qn-t2 wld-state">{{ d.weekState }}</p>
          <p v-else-if="!d.baseline.rows.length" class="qn-t2 wld-state">There is no complete hour to compare yet.</p>
          <template v-else>
            <p class="qn-t3 wld-note">{{ d.baseline.label }}, against the same hour on the days before it.</p>
            <p v-if="!compared" class="qn-t2 wld-state">No earlier day has this hour recorded, so there is nothing to compare it with yet.</p>
            <div v-else class="wld-scroll">
              <div role="table" aria-label="This hour against the usual" class="wld-table" style="--cols: minmax(120px, 1.4fr) 76px 84px minmax(120px, 1.2fr); min-width: 436px">
                <div class="qn-tr head" role="row"><span role="columnheader">Deliveries</span><span role="columnheader" class="r">This hour</span><span role="columnheader" class="r">Usually</span><span role="columnheader">Against usual</span></div>
                <div
                  v-for="r in d.baseline.rows" :key="r.key" class="qn-tr" :class="{ click: !r.tenant, on: !r.tenant && scope.key === r.key }" role="row"
                  :tabindex="r.tenant ? undefined : 0" @click="!r.tenant && emit('pick', r.key)" @keydown.enter.prevent="!r.tenant && emit('pick', r.key)"
                >
                  <span role="cell" :class="r.mono ? 'qn-mono' : (r.tenant ? '' : 'qn-t2')" :style="r.tenant ? 'font-weight: 500' : ''">{{ r.name }}</span>
                  <span role="cell" class="r qn-num">{{ r.current }}</span>
                  <span role="cell" class="r qn-num" :title="r.min !== '–' ? `Between ${r.min} and ${r.max} on ${r.days} days` : undefined">{{ r.usual }}</span>
                  <span role="cell" class="wld-ratio">
                    <span v-if="r.bar !== null" class="wld-rbar" aria-hidden="true">
                      <span class="wld-rfill" :class="r.ratio.tone" :style="{ width: pctOf(r.bar) }" />
                      <i class="wld-rone" :style="{ left: pctOf(1 / d.baseline.top) }" />
                    </span>
                    <span class="qn-num" :class="r.ratio.tone || (r.ratio.dim ? 'qn-t3' : '')">{{ r.ratio.text }}</span>
                  </span>
                </div>
              </div>
            </div>
            <p v-if="compared" class="qn-read wld-read">The line on a bar is the usual; amber is twice or half of it. A click looks inside a group.</p>
          </template>
        </div>
      </div>

      <!-- what one push costs -->
      <div class="wld-part">
        <h3 class="wld-h">What one push costs, the last 24 hours</h3>
        <p v-if="d.costState" class="qn-t2 wld-state">{{ d.costState }}</p>
        <p v-else-if="!d.cost.rows.length" class="qn-t2 wld-state">Nothing was read in the last 24 hours.</p>
        <template v-else>
          <p class="qn-t3 wld-note">The {{ d.cost.rows.length === 1 ? 'queue' : `${d.cost.rows.length} queues` }} read the most.</p>
          <div class="wld-scroll">
            <div role="table" aria-label="What one push costs" class="wld-table" style="--cols: minmax(190px, 2fr) 132px 160px 132px minmax(200px, 1.6fr); min-width: 880px">
              <div class="qn-tr head" role="row">
                <span role="columnheader">Queue</span>
                <span role="columnheader" class="r" title="Deliveries and Kafka fetches per pushed message">Reads a push</span>
                <span role="columnheader" class="r" title="Acks the counters attribute, per delivery">Acks a delivery</span>
                <span role="columnheader" class="r" title="Messages per push request, then per ack request">Batch, push · ack</span>
                <span role="columnheader" title="Of the buckets with deliveries, how long a message waited for its pop at worst">Waited for a pop</span>
              </div>
              <div v-for="r in d.cost.rows" :key="r.key" class="qn-tr" role="row">
                <span role="cell"><RouterLink :to="queueTo(r.key)" class="qn-mono" :title="r.key">{{ r.key }}</RouterLink></span>
                <span role="cell" class="r qn-num" :class="r.fanout.tone || (r.fanout.dim ? 'qn-t3' : '')" :title="r.fanout.title">{{ r.fanout.text }}<span v-if="r.fanout.sub" class="qn-t3 wld-cs"> · {{ r.fanout.sub }}</span></span>
                <span role="cell" class="r qn-num" :class="r.acks.tone || (r.acks.dim ? 'qn-t3' : '')" :title="r.acks.title">{{ r.acks.text }}<span v-if="r.acks.sub" class="qn-t3 wld-cs"> · {{ r.acks.sub }}</span></span>
                <span role="cell" class="r qn-num" :title="r.batch.title">{{ r.batch.push }} · {{ r.batch.ack }}</span>
                <span role="cell" class="wld-budget" :title="r.budget?.title">
                  <template v-if="r.budget">
                    <span class="wld-stack" aria-hidden="true">
                      <span class="wld-s1" :style="{ width: pctOf(r.budget.mid) }" />
                      <span class="wld-s2" :class="{ amber: r.budget.tone }" :style="{ width: pctOf(r.budget.over) }" />
                    </span>
                    <span class="qn-num" :class="r.budget.tone || (r.budget.over || r.budget.mid ? '' : 'qn-t3')">{{ r.budget.text }}</span>
                  </template>
                  <span v-else class="qn-t3">–</span>
                </span>
              </div>
            </div>
          </div>
          <p class="qn-read wld-read">
            <template v-if="fallback">Rolled up in this browser from <span class="qn-mono">GET /api/v1/analytics/queue-ops</span> over the last 24 hours.</template>
            <template v-else>From <span class="qn-mono">GET /api/v1/analytics/workload?groupBy=queue</span> over the last 24 hours.</template>
            A bar is the share of busy buckets in which a message waited 10 s or more for its pop; its strong end, a minute or more. Acks made inside a transaction are not counted for a queue.
          </p>
        </template>
      </div>

      <div class="wld-two">
        <!-- retention and eviction -->
        <div class="wld-part">
          <h3 class="wld-h">Retention and eviction</h3>
          <p v-if="d.retentionState" class="qn-t2 wld-state">{{ d.retentionState }}</p>
          <p v-else-if="d.retention.ungrouped" class="qn-t2 wld-state">This broker answers retention for the tenant as a whole, not by queue.</p>
          <p v-else-if="!d.retention.rows.length" class="qn-t2 wld-state">Nothing was removed {{ inThe }}.</p>
          <template v-else>
            <p class="qn-t3 wld-note">What was removed {{ inThe }}, the most first.</p>
            <div class="wld-scroll">
              <div role="table" aria-label="Retention and eviction by queue" class="wld-table" style="--cols: minmax(140px, 1.6fr) 76px 88px 76px; min-width: 416px">
                <div class="qn-tr head" role="row">
                  <span role="columnheader">Queue</span>
                  <span role="columnheader" class="r" title="Older than the retention, removed">For age</span>
                  <span role="columnheader" class="r" title="Completed, removed after their completed retention">Completed</span>
                  <span role="columnheader" class="r" title="Older than the max wait, never delivered: loss by policy">Evicted</span>
                </div>
                <div v-for="r in d.retention.rows" :key="r.key" class="qn-tr" role="row">
                  <span role="cell"><RouterLink :to="queueTo(r.key)" class="qn-mono" :title="r.key">{{ r.key }}</RouterLink></span>
                  <span role="cell" class="r qn-num">{{ r.ageText }}</span>
                  <span role="cell" class="r qn-num" :class="{ 'qn-t3': !r.done }">{{ r.doneText }}</span>
                  <span role="cell" class="r qn-num" :class="r.tone || (r.evicted ? '' : 'qn-t3')">{{ r.evictedText }}</span>
                </div>
              </div>
            </div>
            <p class="qn-read wld-read">From <span class="qn-mono">GET /api/v1/analytics/retention?groupBy=queue</span>. Evicted messages were never delivered.</p>
          </template>
        </div>

        <!-- partitions alive -->
        <div class="wld-part">
          <h3 class="wld-h">Partitions alive</h3>
          <p v-if="d.livenessState" class="qn-t2 wld-state">{{ d.livenessState }}</p>
          <p v-else-if="!d.liveness.length" class="qn-t2 wld-state">No partition was reported.</p>
          <template v-else>
            <p class="qn-t3 wld-note">The queues with the most partitions, and how many were written to.</p>
            <div class="wld-scroll">
              <div role="table" aria-label="Partitions alive by queue" class="wld-table" style="--cols: minmax(130px, 1.5fr) 76px minmax(108px, 1fr) 64px 64px; min-width: 490px">
                <div class="qn-tr head" role="row">
                  <span role="columnheader">Queue</span>
                  <span role="columnheader" class="r">Partitions</span>
                  <span role="columnheader" title="Written to in the last 24 hours">Written, 24 h</span>
                  <span role="columnheader" class="r" title="Created in the last 24 hours">Created</span>
                  <span role="columnheader" class="r" title="Deleted in the last 24 hours">Deleted</span>
                </div>
                <div v-for="r in d.liveness" :key="r.key" class="qn-tr" role="row">
                  <span role="cell"><RouterLink :to="queueTo(r.key)" class="qn-mono" :title="r.key">{{ r.key }}</RouterLink></span>
                  <span role="cell" class="r qn-num">{{ r.partitions }}</span>
                  <span role="cell" class="wld-ratio" :title="`${r.written} written to`">
                    <span class="wld-rbar" aria-hidden="true"><span class="wld-rfill" :style="{ width: pctOf(r.live) }" /></span>
                    <span class="qn-num">{{ r.liveText }}</span>
                  </span>
                  <span role="cell" class="r qn-num">{{ r.created }}</span>
                  <span role="cell" class="r qn-num">{{ r.deleted }}</span>
                </div>
              </div>
            </div>
            <p class="qn-read wld-read">From <span class="qn-mono">GET /api/v1/analytics/partition-liveness</span>; deletions from the 24-hour read.</p>
          </template>
        </div>
      </div>

      <div class="wld-two">
        <!-- why messages are in the DLQ -->
        <div class="wld-part">
          <h3 class="wld-h">Why messages are in the DLQ</h3>
          <p v-if="d.costState" class="qn-t2 wld-state">{{ d.costState }}</p>
          <p v-else-if="!d.dead.length" class="qn-t2 wld-state">No dead letters right now.</p>
          <template v-else>
            <p class="qn-t3 wld-note">The most common cause of each queue's newest dead letters.</p>
            <ul class="wld-dead">
              <li v-for="r in d.dead" :key="r.key" class="wld-dl">
                <p class="wld-dlhead">
                  <RouterLink :to="queueTo(r.key)" class="qn-mono wld-dlq" :title="r.key">{{ r.key }}</RouterLink>
                  <span class="qn-num qn-t2">{{ r.dead }} dead letters</span>
                  <RouterLink :to="r.to" class="qn-link wld-why">See why</RouterLink>
                </p>
                <p class="wld-cause" :class="r.cause.mono ? 'qn-mono' : 'qn-t3'" :title="r.cause.text">{{ r.cause.text }}</p>
                <p v-if="r.shareText" class="qn-t3 qn-num wld-cshare">{{ r.shareText }}</p>
              </li>
            </ul>
            <p class="qn-read wld-read">From <span class="qn-mono">GET /api/v1/analytics/dlq-signatures?queue=</span>, the newest 150 of each. Every cause is on Dead letters.</p>
          </template>
        </div>

        <!-- where the bytes are -->
        <div class="wld-part">
          <h3 class="wld-h">Where the bytes are</h3>
          <p v-if="d.costState" class="qn-t2 wld-state">{{ d.costState }}</p>
          <p v-else-if="!d.bytes.length" class="qn-t2 wld-state">No queue reports both its bytes and its messages.</p>
          <template v-else>
            <p class="qn-t3 wld-note">The queues that retain the most, and what one message costs.</p>
            <div class="wld-scroll">
              <div role="table" aria-label="Retained bytes by queue" class="wld-table" style="--cols: minmax(140px, 1.5fr) minmax(120px, 1fr) 76px 76px; min-width: 448px">
                <div class="qn-tr head" role="row">
                  <span role="columnheader">Queue</span>
                  <span role="columnheader">Retained</span>
                  <span role="columnheader" class="r">Messages</span>
                  <span role="columnheader" class="r">A message</span>
                </div>
                <div v-for="r in d.bytes" :key="r.key" class="qn-tr" role="row">
                  <span role="cell"><RouterLink :to="queueTo(r.key)" class="qn-mono" :title="r.key">{{ r.key }}</RouterLink></span>
                  <span role="cell" class="wld-ratio">
                    <span class="wld-rbar" aria-hidden="true"><span class="wld-rfill" :style="{ width: pctOf(r.bar) }" /></span>
                    <span class="qn-num">{{ r.retained }}</span>
                  </span>
                  <span role="cell" class="r qn-num">{{ r.messages }}</span>
                  <span role="cell" class="r qn-num">{{ r.each }}</span>
                </div>
              </div>
            </div>
            <p class="qn-read wld-read">Bytes from the 24-hour read; messages from <span class="qn-mono">GET /api/v1/resources/queues</span>.</p>
          </template>
        </div>
      </div>

      <!-- what it adds up to -->
      <div v-if="d.notes.length" class="wld-part">
        <h3 class="wld-h">What these add up to</h3>
        <ul class="wld-notes">
          <li v-for="(n, i) in d.notes" :key="i">
            <span class="wld-nmark" aria-hidden="true"><span v-if="n.tone" class="qn-mark" :class="n.tone" /></span>
            <span style="min-width: 0">
              <span><template v-for="(p, k) in parts(n.text)" :key="k"><span :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></template></span>
              <span v-if="n.evidence" class="qn-t2 wld-nev" :class="{ 'qn-mono': n.mono }"><template v-for="(p, k) in parts(n.evidence)" :key="k"><span :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></template></span>
            </span>
          </li>
        </ul>
      </div>
    </template>
  </section>
</template>

<style scoped>
.wld { --wld-ground: var(--bg2); container-type: inline-size; }
:global(.qn.tone .wld) { --wld-ground: var(--ctl); }
.wld-head { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 16px; }
.wld-title { font-size: 15px; font-weight: 500; }
.wld-btn { height: 28px; font-size: 12.5px; }
.wld-at { font-size: 12px; }
.wld-sub { margin-top: 6px; max-width: 760px; }
.wld-line { margin-top: 12px; }
.wld-two { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 28px 40px; }
.wld-part { min-width: 0; margin-top: 28px; }
.wld-two > .wld-part { margin-top: 28px; }
.wld-h { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 10px; font-size: 13.5px; font-weight: 500; }
.wld-for { font-weight: 400; font-size: 12.5px; }
.wld-note { font-size: 12px; margin-top: 4px; }
.wld-state { margin-top: 10px; font-size: 12.5px; }
.wld-stat { margin-top: 10px; font-size: 12.5px; max-width: 640px; }
.wld-scroll { overflow-x: auto; margin-top: 8px; }
.wld-table .qn-tr { gap: 0 12px; min-height: 34px; font-size: 12.5px; }
.wld-table .qn-tr.head { min-height: 28px; font-size: 11.5px; }
.wld-cs { font-size: 11.5px; }
.wld-read { margin-top: 10px; }

/* the week: 7 rows of 24 hours */
.wlk { margin-top: 12px; display: flex; flex-direction: column; gap: 2px; max-width: 640px; }
.wlk-row { display: grid; grid-template-columns: 34px minmax(0, 1fr); align-items: center; gap: 8px; }
.wlk-day { font-size: 11.5px; }
.wlk-cells { display: grid; grid-template-columns: repeat(24, minmax(0, 1fr)); gap: 2px; height: 16px; }
.wlk-cell { display: block; height: 100%; border-radius: 2px; background: var(--wld-ground); }
.wlk-cell.na { background: transparent; box-shadow: inset 0 0 0 1px var(--bg3); }
.wlk-axis { margin-top: 4px; font-size: 11px; }
.wlk-ax { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); }

/* a ratio against a whole, or the usual */
.wld-ratio { display: flex; align-items: center; gap: 10px; }
.wld-rbar { position: relative; flex: 1 1 0; min-width: 40px; max-width: 160px; height: 6px; border-radius: 3px; background: var(--line); }
.wld-rfill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 3px; background: var(--text3); }
.wld-rfill.amber { background: var(--amber); }
.wld-rone { position: absolute; top: -3px; bottom: -3px; width: 1.5px; margin-left: -0.75px; background: var(--text); opacity: .7; }

/* how long a message waited: the part over 10 s, then over a minute */
.wld-budget { display: flex; align-items: center; gap: 10px; }
.wld-stack { display: flex; flex: 1 1 0; min-width: 40px; max-width: 140px; height: 6px; border-radius: 3px; overflow: hidden; background: var(--line); }
.wld-s1 { display: block; background: var(--text3); }
.wld-s2 { display: block; background: var(--text); }
.wld-s2.amber { background: var(--amber); }

/* dead letters */
.wld-dead { margin-top: 8px; }
.wld-dl { padding: 10px 0; border-bottom: 1px solid var(--line2); }
.wld-dl:last-child { border-bottom: 0; }
:global(.qn.tone .wld-dl) { border-bottom-color: var(--bg3); }
.wld-dlhead { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 12px; font-size: 13px; }
.wld-dlq { font-weight: 500; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.wld-why { margin-left: auto; font-size: 12.5px; }
.wld-cause { margin-top: 4px; font-size: 12px; color: var(--text2); display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; overflow-wrap: anywhere; }
.wld-cshare { font-size: 11.5px; margin-top: 2px; }

/* notes */
.wld-notes { display: grid; gap: 10px; margin-top: 10px; }
.wld-notes li { display: flex; align-items: flex-start; gap: 10px; }
.wld-nmark { flex: 0 0 8px; display: flex; justify-content: center; margin-top: 6px; }
.wld-nev { display: block; font-size: 12.5px; margin-top: 2px; overflow-wrap: anywhere; }
.wld-nev.qn-mono { font-size: 12px; }

:global(.qn.tone .wld-rbar), :global(.qn.tone .wld-stack) { background: var(--bg3); }
/* two parts side by side only where both get their columns: beside a
   queue's groups, or on a narrow screen, they stack */
@container (max-width: 900px) { .wld-two { grid-template-columns: minmax(0, 1fr); gap: 0; } }
</style>
