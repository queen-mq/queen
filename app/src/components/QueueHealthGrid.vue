<template>
  <!--
    The all-queues table: one row per queue, its verdict (useAttention's, the
    rule the list beside and the Overview use) as a mark and a word, then plain
    right-aligned numbers; colour only where a verdict says so. A row opens the
    queue; Delete shows on hover.
  -->
  <div class="qhg" :class="{ 'qhg-no-hot': !showHot }">
    <div class="qhead">
      <span class="qstatus-compact" aria-hidden="true" />
      <span class="h-name">Queue</span>
      <span class="h-status">Status</span>
      <span class="h-c h-density" title="Messages per partition, since the queue was created">Per partition</span>
      <span v-if="showHot" class="h-c h-hot">Hot</span>
      <span class="h-c" title="Pops per second, last 15 minutes">Pop rate</span>
      <span class="h-c" title="Longest wait before a pop, last 15 minutes">Max lag</span>
      <span class="h-c h-parts">Partitions</span>
      <span class="h-c h-store" title="Bytes the queue holds on disk">Stored</span>
      <span aria-hidden="true" />
    </div>

    <template v-if="loading && !queues.length">
      <div v-for="i in 8" :key="`s-${i}`" class="qrow qrow-skeleton" aria-hidden="true">
        <span class="qstatus-compact" />
        <span class="skeleton" style="height: 12px; width: 60%;" />
        <span class="skeleton qstatus" style="height: 12px; width: 70px;" />
        <span class="skeleton h-density" style="height: 12px; width: 50px; margin-left: auto;" />
        <span v-if="showHot" class="skeleton cc-hot" style="height: 12px; width: 50px; margin-left: auto;" />
        <span class="skeleton" style="height: 12px; width: 50px; margin-left: auto;" />
        <span class="skeleton" style="height: 12px; width: 40px; margin-left: auto;" />
        <span class="skeleton cc-parts" style="height: 12px; width: 32px; margin-left: auto;" />
        <span class="skeleton cc-store" style="height: 12px; width: 50px; margin-left: auto;" />
        <span />
      </div>
    </template>

    <div v-else-if="!queues.length">
      <!-- The slot owns the whole block, `.empty-state` padding included. -->
      <slot name="empty">
        <div class="empty-state"><h3>No queue matches</h3></div>
      </slot>
    </div>

    <template v-else>
      <div
        v-for="q in displayed"
        :key="q.name"
        class="qrow"
        role="link"
        tabindex="0"
        :aria-label="`${q.name}: ${verdict(q).word}`"
        @click="$emit('select', q)"
        @keydown.enter.prevent="$emit('select', q)"
      >
        <span class="g qstatus-compact" :class="glyph(cardSev(q))" aria-hidden="true" />
        <span class="qname" :title="q.name">
          <span class="ns">{{ q._nsPrefix }}</span><span class="nm">{{ q._namePart }}</span>
        </span>
        <QueueStatus class="qstatus" :severity="cardSev(q)" :label="verdict(q).word" />
        <span class="cell-c h-density">
          <span class="cc">{{ densityVal(q.density) }}</span>
        </span>
        <span v-if="showHot" class="cell-c cc-hot">
          <span class="cc" :class="`sev-${hotSev(q.hotCount, q.partitions)}`">
            {{ q.hotCount === null ? '—' : fmt(q.hotCount) }}<i v-if="q.hotCount !== null">hot</i>
          </span>
        </span>
        <span class="cell-c">
          <span class="cc" :title="`push ${fmtRate(q.pushPerSec)}/s · pop ${fmtRate(q.popPerSec)}/s`">
            <span class="arrow" :class="arrowClass(q)" aria-hidden="true">{{ arrow(q) }}</span>{{ fmtRate(q.popPerSec) }}<i>/s</i>
          </span>
        </span>
        <span class="cell-c">
          <span class="cc">{{ fmtLag(q.avgLagMs) }}</span>
        </span>
        <span class="cell-c cc-parts">
          <span class="cc">{{ fmt(q.partitions) }}</span>
        </span>
        <span class="cell-c cc-store">
          <span class="cc" :title="q.retainedBytes == null ? 'Storage not reported for this queue' : undefined">{{ fmtBytes(q.retainedBytes) }}</span>
        </span>
        <span class="cell-c qactions">
          <button
            v-if="canDelete"
            type="button"
            class="qaction"
            title="Delete queue"
            :aria-label="`Delete ${q.name}`"
            @click.stop="$emit('delete', q)"
            @keydown.enter.stop
          ><Icon name="trash" :size="14" /></button>
        </span>
      </div>
    </template>
  </div>
</template>

<script setup>
import { computed } from 'vue'

import { formatBytes } from '@/composables/useApi'
import { laggingPartitionsSeverity } from '@/composables/useSeverity'
import Icon from '@/components/Icon.vue'
import QueueStatus from '@/components/QueueStatus.vue'

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
  /**
   * What needs you, by queue name — composables/useAttention's verdicts, the
   * rule the Overview and the sidebar use. null while it is being read.
   */
  attention: { type: Object, default: null },
  /** The consumer groups could not be read, so no row can be judged. */
  attentionUnknown: { type: Boolean, default: false },
})

defineEmits(['select', 'delete'])

/* ---------------- severity rules ----------------
 * The thresholds themselves live in @/composables/useSeverity, with the rest
 * of the app's colour policy and a test file that pins them. What stays here
 * is only the mapping from this grid's columns onto those rules. */
const SEV_RANK = { ok: 0, ice: 0, mute: 1, unknown: 1, warn: 2, bad: 3 }

/* Hot partitions, as a SHARE of the queue's partitions. A count could not be
 * a verdict on its own: 20 hot partitions is most of a 24-partition queue and
 * a rounding error on a 4 000-partition one, and the old rule called both of
 * them bad. */
function hotSev(h, partitions) {
  if (h === null || h === undefined || h === 0) return 'mute'
  return laggingPartitionsSeverity({ behind: h, total: partitions })
}


/* The row's verdict is the app's one rule for "needs you" (useAttention):
 * a consumer group behind, or messages nobody reads — the same verdict the
 * Overview and the sidebar give this queue. The pop-rate and lag cells keep
 * their own tones; they describe a column, not the queue. */
function verdict(q) {
  if (props.attentionUnknown) return { sev: 'unknown', word: 'Unknown' }
  const a = props.attention?.get(q.name)
  if (a?.sev === 'bad') return { sev: 'bad', word: 'Falling behind' }
  if (a?.reason === 'noReader') return { sev: 'warn', word: a.deadOnly ? 'Never read' : 'No reader' }
  if (a?.sev === 'warn') return { sev: 'warn', word: 'Behind' }
  // Only an explicit 0 proves the queue is drained. `pending == null` means
  // the backend did not report it, and claiming "idle" from an unknown is how
  // a backed-up queue ends up painted the same colour as an empty one.
  if ((q.popPerSec || 0) < 5 && (q.pushPerSec || 0) < 5 && q.pending === 0) return { sev: 'ice', word: 'Idle' }
  return { sev: 'ok', word: 'Healthy' }
}
function cardSev(q) {
  return verdict(q).sev
}

/* The row verdict as a shape and a word — the same vocabulary as the page
 * legend and the Overview. Only warn and bad carry a colour. */
function glyph(sev) {
  return sev === 'bad' ? 'bad' : sev === 'warn' ? 'warn' : sev === 'ice' || sev === 'unknown' ? 'idle' : 'ok'
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
/* One card, rows on hairlines, plain right-aligned numbers. Narrow rows keep
   the leading mark when the status column steps out. */
.qhg {
  background: var(--ink-2);
  border: 1px solid var(--bd);
  border-radius: var(--r-card);
  overflow: hidden;
}

.qhead, .qrow {
  display: grid;
  grid-template-columns: minmax(200px, 1fr) 128px 96px 82px 96px 80px 86px 88px 28px;
  gap: 12px;
  align-items: center;
  padding: 0 12px 0 16px;
}
.qhg-no-hot .qhead,
.qhg-no-hot .qrow {
  grid-template-columns: minmax(200px, 1fr) 128px 96px 96px 80px 86px 88px 28px;
}

.qhead {
  height: 36px;
  border-bottom: 1px solid var(--bd);
  font-size: 12px; font-weight: 500; color: var(--text-low);
}
.qhead .h-c { text-align: right; white-space: nowrap; }

.qrow {
  height: var(--row-h, 40px);
  border-bottom: 1px solid var(--bd-soft);
  font-size: 13px;
  cursor: pointer;
  transition: background .12s ease;
}
.qrow:last-child { border-bottom: 0; }
.qrow:hover { background: var(--ink-3); }
.qrow:focus-visible { outline-offset: -2px; }
.qrow .g { justify-self: center; }
.qrow-skeleton { cursor: default; }
.qrow-skeleton:hover { background: transparent; }

/* Queue names are names, not code: sans, with the namespace prefix dimmed. */
.qname { font-weight: 500; color: var(--text-hi); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.qname .ns { color: var(--text-low); font-weight: 400; }
.qname .nm { color: var(--text-hi); }

.qstatus { justify-self: start; }
.qstatus-compact { display: none; }

.cell-c { text-align: right; }
/* Figures as in the list beside: mono, tabular, a step smaller than words. */
.cc { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 12px; color: var(--text-hi); white-space: nowrap; }
.cc i { font-family: var(--font-sans); font-style: normal; font-size: 11px; color: var(--text-low); margin-left: 3px; }
.cc.sev-warn { color: var(--warn-400); }
.cc.sev-bad { color: var(--ember-400); }
.arrow { margin-right: 4px; font-family: var(--font-sans); color: var(--text-low); }
.arrow.arrow-bad { color: var(--text-mid); }

/* The row action shows on hover and on keyboard focus. */
.qactions { display: flex; justify-content: flex-end; }
.qaction {
  width: 26px; height: 26px; display: grid; place-items: center;
  border: 0; border-radius: var(--r-control); background: transparent;
  color: var(--text-low); cursor: pointer; opacity: 0; transition: opacity .12s ease;
}
.qrow:hover .qaction, .qrow:focus-within .qaction, .qaction:focus-visible { opacity: 1; }
.qaction:hover { color: var(--ember-400); background: var(--ink-4); }
@media (hover: none) { .qaction { opacity: 1; } }

/* Narrower: drop the columns a triage does not need first. */
@media (max-width: 1180px) {
  .qhead, .qrow, .qhg-no-hot .qhead, .qhg-no-hot .qrow {
    grid-template-columns: minmax(180px, 1fr) 128px 96px 80px 86px 28px;
  }
  .h-density, .qrow .cc-hot, .qhead .h-hot, .qrow .cc-store, .qhead .h-store { display: none; }
}
/* Phone: the mark says the verdict; name, pop rate and lag keep the room. */
@media (max-width: 760px) {
  .qhead, .qrow, .qhg-no-hot .qhead, .qhg-no-hot .qrow {
    grid-template-columns: 14px minmax(0, 1fr) 76px 64px 28px;
    gap: 8px;
    padding: 0 8px 0 12px;
  }
  .qstatus, .qhead .h-status, .qrow .cc-parts, .qhead .h-parts { display: none; }
  .qstatus-compact { display: inline-block; }
}
</style>
