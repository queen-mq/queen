<template>
  <div class="qhg" :class="{ 'qhg-no-hot': !showHot }">
    <!-- header row (column labels) -->
    <div class="qhead">
      <div></div>
      <div class="h-name">Queue</div>
      <div class="h-c">Density</div>
      <div v-if="showHot" class="h-c h-hot">Hot</div>
      <div class="h-c">Throughput</div>
      <div class="h-c">Lag p99</div>
      <div class="h-c h-parts">Partitions</div>
      <div class="h-c h-store">Storage</div>
      <div></div>
    </div>

    <!-- skeleton -->
    <template v-if="loading && !queues.length">
      <div v-for="i in 8" :key="`s-${i}`" class="qrow qrow-skeleton">
        <span></span>
        <span class="skeleton" style="height: 12px; width: 60%;"></span>
        <span class="skeleton" style="height: 18px; width: 60px; margin: 0 auto;"></span>
        <span v-if="showHot" class="skeleton" style="height: 18px; width: 60px; margin: 0 auto;"></span>
        <span class="skeleton" style="height: 18px; width: 70px; margin: 0 auto;"></span>
        <span class="skeleton" style="height: 18px; width: 60px; margin: 0 auto;"></span>
        <span class="skeleton" style="height: 18px; width: 60px; margin: 0 auto;"></span>
        <span class="skeleton" style="height: 18px; width: 60px; margin: 0 auto;"></span>
        <span></span>
      </div>
    </template>

    <!-- empty state -->
    <div v-else-if="!queues.length" class="qhg-empty">
      <slot name="empty">
        <p>No queues match your filters.</p>
      </slot>
    </div>

    <!-- rows -->
    <template v-else>
      <div
        v-for="q in displayed"
        :key="q.name"
        class="qrow"
        :class="`sev-${cardSev(q)}`"
        @click="$emit('select', q)"
      >
        <span class="qdot"></span>
        <span class="qname">
          <span class="ns">{{ q._nsPrefix }}</span><span class="nm">{{ q._namePart }}</span>
        </span>
        <span class="cell-c">
          <span class="cc" :class="`sev-${densitySev(q.density)}`">
            {{ densityVal(q.density) }}<i>msg/p</i>
          </span>
        </span>
        <span v-if="showHot" class="cell-c cc-hot">
          <span class="cc" :class="`sev-${hotSev(q.hotCount, q.partitions)}`">
            {{ q.hotCount === null ? '—' : fmt(q.hotCount) }}<i v-if="q.hotCount !== null">hot</i>
          </span>
        </span>
        <span class="cell-c">
          <span class="cc" :class="`sev-${throughputSev(q.popPerSec, q.pushPerSec)}`">
            <span :class="arrowClass(q)" style="margin-right:2px;">{{ arrow(q) }}</span>{{ fmtRate(q.popPerSec) }}<i>/s</i>
          </span>
        </span>
        <span class="cell-c">
          <span class="cc" :class="`sev-${lagSev(q.avgLagMs)}`">
            {{ fmtLag(q.avgLagMs) }}
          </span>
        </span>
        <span class="cell-c cc-parts">
          <span class="cc sev-mute">{{ fmt(q.partitions) }}<i>parts</i></span>
        </span>
        <span class="cell-c cc-store">
          <span class="cc sev-mute" :title="q.retainedBytes == null ? 'Storage not reported for this queue' : undefined">
            {{ fmtBytes(q.retainedBytes) }}
          </span>
        </span>
        <span class="cell-c qactions">
          <button
            v-if="canDelete"
            class="qaction"
            title="Delete queue"
            @click.stop="$emit('delete', q)"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
            </svg>
          </button>
        </span>
      </div>
    </template>
  </div>
</template>

<script setup>
import { computed } from 'vue'

import { formatBytes } from '@/composables/useApi'

const props = defineProps({
  /**
   * Queue rows. Each entry should expose:
   *   { name, namespace, task, partitions, pending, processing, density,
   *     popPerSec, pushPerSec, avgLagMs, hotCount, retainedBytes }
   * Missing throughput/lag fields default to 0. `pending`, `hotCount` and
   * `retainedBytes` may be null — that means NOT REPORTED and renders as '—',
   * which is not the same claim as 0.
   */
  queues: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  /** Sort key. One of: 'health' | 'name' | 'avgLagMs' | 'density' | 'hotCount' | 'partitions' */
  sortBy: { type: String, default: 'health' },
  /** Optional cap on rendered rows (e.g. for a Dashboard widget). */
  limit: { type: Number, default: null },
  /** Render the Hot column. Requires `hotCount` field on each queue (from
   * the queue-hot-counts backend procedure, not yet wired). */
  showHot: { type: Boolean, default: false },
  /**
   * Render the per-row delete affordance. The caller passes can('queueAdmin')
   * — DELETE /api/v1/resources/queues/:name is RouteClass::QueueAdmin at the
   * proxy, so showing the button to anyone else offers a 403 dressed as a
   * dead button.
   */
  canDelete: { type: Boolean, default: false },
})

defineEmits(['select', 'delete'])

/* ---------------- severity rules ---------------- */
const SEV_RANK = { ok: 0, ice: 0, mute: 1, warn: 2, bad: 3 }

/* Density now means TOTAL messages per partition — a queue's lifetime
 * weight, not its backlog. So colors scale with magnitude (cold → cool)
 * but never go warn/bad: a heavy queue isn't "at risk" just because
 * it's processed a lot of data over time. */
function densitySev(d) {
  if (!d) return 'mute'
  if (d < 10) return 'mute'
  return 'ice'
}

function hotSev(h, partitions) {
  if (h === null || h === undefined) return 'mute'
  if (h === 0) return 'mute'
  const pct = partitions ? (h / partitions) * 100 : 0
  if (h < 5 && pct < 0.5) return 'ice'
  if (h < 20 && pct < 2) return 'warn'
  return 'bad'
}

/* Throughput is colored by pop/push ratio — i.e. is the queue keeping up? */
function throughputSev(pop, push) {
  if (!pop && !push) return 'mute'
  const ratio = pop / Math.max(1, push)
  if (ratio >= 1.0) return 'ok'
  if (ratio >= 0.85) return 'mute'
  if (ratio >= 0.65) return 'warn'
  return 'bad'
}

function lagSev(ms) {
  if (!ms) return 'mute'
  if (ms < 1000) return 'ok'
  if (ms < 10_000) return 'mute'
  if (ms < 60_000) return 'warn'
  return 'bad'
}

/* Row-level severity describes whether the queue IS KEEPING UP — not just
 * whether any metric is high. Density and hot count never drive the stripe. */
function cardSev(q) {
  const t = throughputSev(q.popPerSec, q.pushPerSec)
  const l = lagSev(q.avgLagMs)
  if (t === 'bad' || l === 'bad') return 'bad'
  if (t === 'warn' || l === 'warn') return 'warn'
  // Only an explicit 0 proves the queue is drained. `pending == null` means
  // the backend did not report it, and claiming "idle" from an unknown is how
  // a backed-up queue ends up painted the same colour as an empty one.
  if ((q.popPerSec || 0) < 5 && (q.pushPerSec || 0) < 5 && q.pending === 0) return 'ice'
  return 'ok'
}

/* ---------------- formatters ---------------- */
function fmt(n) {
  if (n === null || n === undefined) return '—'
  if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e7 ? 0 : 1) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e4 ? 0 : 1) + 'k'
  return String(n)
}
function fmtRate(n) {
  // null = the throughput source failed or was never sampled. '0' would read
  // as "this queue is quiet", which is a different claim.
  if (n === null || n === undefined) return '—'
  if (!n) return '0'
  if (n >= 1000) return (n / 1000).toFixed(1) + 'k'
  if (n >= 100)  return Math.round(n).toString()
  if (n >= 10)   return n.toFixed(1)
  // Sub-10 rates come from float division (e.g. 2 msgs / 18s = 0.111…),
  // so we always cap precision at 2 decimals — never render the raw float.
  return n.toFixed(2)
}
function fmtBytes(bytes) {
  // Unreported storage is '—'; "0 B" would claim an empty queue.
  if (bytes === null || bytes === undefined) return '—'
  return formatBytes(bytes)
}
function fmtLag(ms) {
  if (ms === null || ms === undefined) return '—'
  if (!ms) return '0'
  if (ms < 1000) return ms + 'ms'
  if (ms < 60_000) return (ms / 1000).toFixed(1) + 's'
  if (ms < 3_600_000) return Math.round(ms / 60000) + 'm'
  return (ms / 3_600_000).toFixed(1) + 'h'
}
function densityVal(d) {
  if (!d) return '0'
  if (d < 1) return d.toFixed(2)
  if (d < 10) return d.toFixed(1)
  if (d < 1000) return Math.round(d)
  return fmt(d)
}
function arrow(q) {
  if (!q.popPerSec && !q.pushPerSec) return '·'
  const ratio = (q.popPerSec || 0) / Math.max(1, q.pushPerSec || 0)
  if (ratio >= 1) return '↑'
  if (ratio >= 0.85) return '→'
  return '↓'
}
function arrowClass(q) {
  if (!q.popPerSec && !q.pushPerSec) return 'arrow-mid'
  const ratio = (q.popPerSec || 0) / Math.max(1, q.pushPerSec || 0)
  if (ratio >= 1) return 'arrow-ok'
  if (ratio >= 0.85) return 'arrow-mid'
  return 'arrow-bad'
}

/* ---------------- sort / limit ---------------- */
const displayed = computed(() => {
  const sorted = [...props.queues].map(q => {
    const dotIdx = q.name.indexOf('.')
    return {
      ...q,
      _nsPrefix: dotIdx >= 0 ? q.name.slice(0, dotIdx + 1) : '',
      _namePart: dotIdx >= 0 ? q.name.slice(dotIdx + 1) : q.name,
    }
  })

  if (props.sortBy === 'health') {
    sorted.sort((a, b) => SEV_RANK[cardSev(b)] - SEV_RANK[cardSev(a)]
                          || (b.avgLagMs || 0) - (a.avgLagMs || 0))
  } else if (props.sortBy === 'name') {
    sorted.sort((a, b) => a.name.localeCompare(b.name))
  } else {
    sorted.sort((a, b) => (b[props.sortBy] || 0) - (a[props.sortBy] || 0))
  }

  return props.limit ? sorted.slice(0, props.limit) : sorted
})
</script>

<style scoped>
.qhg {
  background: var(--ink-2);
  border: 1px solid var(--bd);
  border-radius: 6px;
  overflow: hidden;
}

/* column header */
.qhead {
  display: grid;
  grid-template-columns: 14px minmax(200px, 1fr) 76px 86px 78px 92px 82px 84px 32px;
  gap: 10px;
  align-items: center;
  padding: 0 12px 0 0;
  height: 28px;
  background: rgba(255, 255, 255, .012);
  border-bottom: 1px solid var(--bd);
  font-size: 9.5px;
  letter-spacing: .12em;
  text-transform: uppercase;
  color: var(--text-low);
  font-weight: 500;
}
.qhead .h-name { padding-left: 28px; }
.qhead .h-c { text-align: center; }

/* row */
.qrow {
  position: relative;
  display: grid;
  grid-template-columns: 14px minmax(200px, 1fr) 76px 86px 78px 92px 82px 84px 32px;
  gap: 10px;
  align-items: center;
  padding: 0 12px 0 0;
  height: 26px;
  border-bottom: 1px solid var(--bd);
  cursor: pointer;
  transition: background .12s ease;
  font-size: 11.5px;
}

/* without the Hot column */
.qhg-no-hot .qhead,
.qhg-no-hot .qrow {
  grid-template-columns: 14px minmax(200px, 1fr) 76px 78px 92px 82px 84px 32px;
}
.qrow:last-child { border-bottom: none; }
.qrow:hover { background: rgba(255, 255, 255, .025); }

/* left status stripe */
.qrow::before {
  content: '';
  position: absolute;
  left: 0; top: 0; bottom: 0;
  width: 3px;
  background: var(--bd-hi);
}
.qrow.sev-ok::before { background: var(--ok-500); }
.qrow.sev-mute::before { background: var(--bd-hi); }
.qrow.sev-ice::before { background: var(--ice-400); }
.qrow.sev-warn::before { background: var(--warn-400); }
.qrow.sev-bad::before {
  background: var(--ember-400);
  box-shadow: 1px 0 12px rgba(244, 63, 94, .55);
}
.qrow.sev-bad { background: rgba(244, 63, 94, .030); }
.qrow.sev-warn { background: rgba(230, 180, 80, .020); }

.qrow-skeleton { cursor: default; }
.qrow-skeleton::before { background: var(--bd-hi) !important; box-shadow: none !important; }
.qrow-skeleton:hover { background: transparent; }

/* status dot */
.qdot {
  width: 6px; height: 6px;
  border-radius: 99px;
  background: var(--bd-hi);
  margin-left: 10px;
  position: relative;
  flex-shrink: 0;
}
.qrow.sev-ok .qdot { background: var(--ok-500); }
.qrow.sev-ice .qdot { background: var(--ice-400); }
.qrow.sev-warn .qdot { background: var(--warn-400); }
.qrow.sev-bad .qdot { background: var(--ember-400); }
.qrow.sev-bad .qdot::after {
  content: '';
  position: absolute; inset: -2px;
  border-radius: 99px;
  background: var(--ember-400);
  opacity: .25;
  animation: qhg-pulse 2.4s ease-out infinite;
}
@keyframes qhg-pulse {
  0% { transform: scale(.6); opacity: .35; }
  100% { transform: scale(2.0); opacity: 0; }
}

/* queue name */
.qname {
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-size: 12px;
  font-weight: 500;
  color: var(--text-hi);
  letter-spacing: -0.005em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.qname .ns { color: var(--text-low); font-weight: 400; }
.qname .nm { color: var(--text-hi); }

/* cell wrapper to align chip in column center */
.cell-c { text-align: center; }

/* compact chip */
.cc {
  display: inline-flex;
  align-items: baseline;
  justify-content: center;
  padding: 2px 7px;
  border-radius: 4px;
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-variant-numeric: tabular-nums;
  font-size: 11px;
  font-weight: 600;
  line-height: 1.4;
  border: 1px solid transparent;
  white-space: nowrap;
  min-width: 52px;
  letter-spacing: 0;
}
.cc i {
  font-style: normal;
  opacity: .55;
  font-weight: 500;
  margin-left: 4px;
  font-size: 9.5px;
}
.cc.sev-ice { background: rgba(34, 211, 238, .08); color: var(--ice-400); border-color: rgba(34, 211, 238, .20); }
.cc.sev-mute { background: rgba(255, 255, 255, .025); color: var(--text-mid); border-color: var(--bd); }
.cc.sev-ok { background: rgba(74, 222, 128, .07); color: var(--ok-500); border-color: rgba(74, 222, 128, .20); }
.cc.sev-warn { background: rgba(230, 180, 80, .10); color: var(--warn-400); border-color: rgba(230, 180, 80, .26); }
.cc.sev-bad { background: rgba(244, 63, 94, .12); color: var(--ember-400); border-color: rgba(244, 63, 94, .30); }

.arrow-ok { color: var(--ok-500); }
.arrow-mid { color: var(--text-mid); }
.arrow-bad { color: var(--ember-400); }

/* row action button (hover-only) */
.qactions { opacity: 0; transition: opacity .12s ease; }
.qrow:hover .qactions { opacity: 1; }
.qaction {
  width: 22px; height: 22px;
  display: grid;
  place-items: center;
  background: transparent;
  border: 1px solid transparent;
  border-radius: 4px;
  color: var(--text-low);
  cursor: pointer;
  margin: 0 auto;
}
.qaction:hover {
  color: var(--ember-400);
  border-color: rgba(244, 63, 94, .28);
  background: rgba(244, 63, 94, .06);
}
.qaction svg { width: 12px; height: 12px; }

/* empty state */
.qhg-empty {
  padding: 48px 16px;
  text-align: center;
  color: var(--text-mid);
  font-size: 13px;
}

/* responsive: hide hot/parts at narrow widths */
@media (max-width: 1000px) {
  .qhead, .qrow,
  .qhg-no-hot .qhead, .qhg-no-hot .qrow {
    grid-template-columns: 14px minmax(180px, 1fr) 76px 78px 92px 82px 32px;
  }
  .qrow .cc-hot, .qrow .cc-store,
  .qhead .h-hot, .qhead .h-store { display: none; }
}

@media (max-width: 880px) {
  .qhead, .qrow,
  .qhg-no-hot .qhead, .qhg-no-hot .qrow {
    grid-template-columns: 14px minmax(140px, 1fr) 76px 78px 82px 32px;
  }
  .qrow .cc-hot, .qrow .cc-parts, .qrow .cc-store,
  .qhead .h-hot, .qhead .h-parts, .qhead .h-store { display: none; }
}
</style>
