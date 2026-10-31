<template>
  <!--
    The Consumer groups tab's list: one row per group, in the order the page
    gives (composables/consumerGroups sortGroups). A mark and a word for the
    verdict (useAttention's, as everywhere), then plain right-aligned numbers;
    colour only where a line says so. A click opens the group's detail; the
    one row action, Delete, shows on hover and is also in the detail.
  -->
  <div class="chg">
    <div class="chead">
      <span aria-hidden="true" />
      <span>Group</span>
      <span class="c-status">Status</span>
      <span class="c-num c-parts" title="Partitions this group reads">Partitions</span>
      <span class="c-num" title="Partitions with messages this group has not read yet">Behind</span>
      <span class="c-num" title="Age of the oldest message this group has not read">Time lag</span>
      <span class="c-act" aria-hidden="true" />
    </div>

    <template v-if="loading && !consumers.length">
      <div v-for="i in 6" :key="`s-${i}`" class="crow crow-skeleton" aria-hidden="true">
        <span />
        <span class="skeleton" style="height: 12px; width: 55%;" />
        <span class="skeleton c-status" style="height: 12px; width: 64px;" />
        <span class="skeleton c-parts" style="height: 12px; width: 32px; justify-self: end;" />
        <span class="skeleton" style="height: 12px; width: 32px; justify-self: end;" />
        <span class="skeleton" style="height: 12px; width: 48px; justify-self: end;" />
        <span class="c-act" />
      </div>
    </template>

    <div v-else-if="!consumers.length">
      <!-- The slot owns the whole block, `.empty-state` padding included. -->
      <slot name="empty" />
    </div>

    <template v-else>
      <div
        v-for="g in consumers"
        :key="keyOf(g)"
        class="crow"
        :class="{ on: keyOf(g) === selectedKey }"
        tabindex="0"
        :aria-label="`${groupLabel(g)} on ${g.queueName}: ${verdictOf(g, linesFor).word}`"
        @click="$emit('select', g)"
        @keydown.enter.prevent="$emit('select', g)"
      >
        <span class="g" :class="verdictOf(g, linesFor).glyph" aria-hidden="true" />
        <span class="cname" :title="showQueue ? `${groupLabel(g)} on ${g.queueName}` : groupLabel(g)">
          <span class="nm">{{ groupLabel(g) }}</span>
          <span v-if="showQueue && g.queueName" class="ns">{{ g.queueName }}</span>
          <!-- A delivery policy and a client kind ride the name: they say how
               to read the numbers beside it, they are not numbers. -->
          <span v-if="isConflating(g)" class="ctag" title="Delivered one message per partition, the newest: what it is behind is partitions to visit, not messages">conflation</span>
          <span v-if="g.kind === 'kafka'" class="ctag" title="Read through the Kafka facade">Kafka</span>
        </span>
        <span class="cstatus c-status" :class="verdictOf(g, linesFor).tone" :title="presence(g) || undefined">{{ verdictOf(g, linesFor).word }}</span>
        <span class="cnum c-parts">{{ count(g.members) }}</span>
        <span class="cnum" :class="lagPartsSev(g)">{{ toNum(g.partitionsWithLag) > 0 ? count(g.partitionsWithLag) : '—' }}</span>
        <span class="cnum" :class="timeLagSev(g)">{{ toNum(g.maxTimeLag) > 0 ? fmtLag(g.maxTimeLag) : '—' }}</span>
        <span class="c-act">
          <button
            v-if="canAdmin"
            type="button"
            class="caction"
            title="Delete consumer group"
            :aria-label="`Delete ${groupLabel(g)} on ${g.queueName}`"
            @click.stop="$emit('delete', g)"
            @keydown.enter.stop
          ><Icon name="trash" :size="14" /></button>
        </span>
      </div>
    </template>
  </div>
</template>

<script setup>
import Icon from '@/components/Icon.vue'
import { groupLabel, verdictOf } from '@/composables/consumerGroups'
import { formatSpan } from '@/composables/settingsDoc'
import { toNum } from '@/composables/useApi'
import { groupPresence } from '@/composables/useAttention'
import { isConflating } from '@/composables/useConflation'
import { groupActivity, laggingPartitionsSeverity, timeLagSeverity } from '@/composables/useSeverity'
import { useSettingsStore } from '@/stores/settingsStore'

defineProps({
  /**
   * Consumer-group rows, already in the page's order:
   *   { name, queueName, members, maxTimeLag, partitionsWithLag, state,
   *     conflation, kind, liveMembers, lastSeenAt, inactiveSeconds }
   * `members` is the partitions the group reads; `kind === 'kafka'` is a
   * group read through the Kafka facade.
   */
  consumers: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  /** The proxy classifies delete as QueueAdmin: anyone else sees no button. */
  canAdmin: { type: Boolean, default: false },
  /** Name each row's queue: off on a queue's own tab, where the header does. */
  showQueue: { type: Boolean, default: true },
  /** The row whose detail is open (keyOf), drawn selected. */
  selectedKey: { type: String, default: '' },
})

defineEmits(['select', 'delete'])

// A group's lines are its queue's (Settings).
const { linesFor } = useSettingsStore()

const keyOf = (g) => `${g?.queueName ?? ''}\u0000${g?.name ?? ''}`

function presence(g) {
  const said = groupPresence(g, linesFor)
  return said && said.charAt(0).toUpperCase() + said.slice(1)
}

// An inactive Kafka group is not judged by its figures: its lag grows for as
// long as nobody runs it, and painting it would be the alarm its verdict has
// just declined to raise.
const inactive = (g) => groupActivity(g, linesFor(g.queueName)) === 'inactive'
const timeLagSev = (g) => (inactive(g) ? '' : timeLagSeverity(g.maxTimeLag, linesFor(g.queueName)))
const lagPartsSev = (g) => (inactive(g) ? '' : laggingPartitionsSeverity({ behind: g.partitionsWithLag, total: g.members }))

/** A count we hold, or a dash: a missing value is not a zero. */
function count(v) {
  const n = toNum(v)
  if (n === null) return '—'
  if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e7 ? 0 : 1) + 'M'
  if (n >= 1e4) return (n / 1e3).toFixed(0) + 'k'
  return n.toLocaleString('en-US')
}
const fmtLag = (seconds) => formatSpan(Math.round(toNum(seconds) || 0))
</script>

<style scoped>
.chg {
  background: var(--ink-2);
  border: 1px solid var(--bd);
  border-radius: var(--r-card);
  overflow: hidden;
}

.chead, .crow {
  display: grid;
  grid-template-columns: 14px minmax(180px, 1fr) 120px 88px 80px 96px 28px;
  gap: 12px;
  align-items: center;
  padding: 0 12px 0 16px;
}
.chead {
  height: 36px;
  border-bottom: 1px solid var(--bd);
  font-size: 12px; font-weight: 500; color: var(--text-low);
}
.c-num { text-align: right; }

.crow {
  height: var(--row-h, 40px);
  border-bottom: 1px solid var(--bd-soft);
  font-size: 13px;
  cursor: pointer;
  transition: background .12s ease;
}
.crow:last-child { border-bottom: 0; }
.crow:hover { background: var(--ink-3); }
.crow.on, .crow.on:hover { background: var(--selected); --text-low: var(--text-mid); }
.crow:focus-visible { outline-offset: -2px; }
.crow .g { justify-self: center; }
.crow-skeleton { cursor: default; }
.crow-skeleton:hover { background: transparent; }

.cname { display: flex; align-items: baseline; gap: 8px; min-width: 0; overflow: hidden; white-space: nowrap; }
.nm { font-weight: 500; color: var(--text-hi); overflow: hidden; text-overflow: ellipsis; flex: 0 1 auto; min-width: 0; }
.ns { font-size: 12px; color: var(--text-low); overflow: hidden; text-overflow: ellipsis; flex: 0 1 auto; min-width: 0; }
.ns::before { content: '· '; color: var(--text-faint); }
.ctag {
  flex: none; padding: 0 6px; border: 1px solid var(--bd); border-radius: var(--r-chip);
  font-size: 11px; line-height: 17px; color: var(--text-low);
}

.cstatus { font-size: 12px; color: var(--text-mid); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.cstatus.warn { color: var(--warn-400); }
.cstatus.bad { color: var(--ember-400); }

/* Figures as in the queue list: mono, tabular, a step smaller than words. */
.cnum { text-align: right; font-family: var(--font-mono); font-size: 12px; font-variant-numeric: tabular-nums; color: var(--text-hi); white-space: nowrap; }
.cnum.warn { color: var(--warn-400); }
.cnum.bad { color: var(--ember-400); }

/* The row action shows on hover and on keyboard focus. */
.c-act { display: flex; justify-content: flex-end; }
.caction {
  width: 26px; height: 26px; display: grid; place-items: center;
  border: 0; border-radius: var(--r-control); background: transparent;
  color: var(--text-low); cursor: pointer; opacity: 0; transition: opacity .12s ease;
}
.crow:hover .caction, .crow:focus-within .caction, .caction:focus-visible { opacity: 1; }
.caction:hover { color: var(--ember-400); background: var(--ink-4); }
@media (hover: none) { .caction { opacity: 1; } }

/* Narrower: the partition count goes first. */
@media (max-width: 1180px) {
  .chead, .crow { grid-template-columns: 14px minmax(160px, 1fr) 112px 72px 88px 28px; }
  .c-parts { display: none; }
}
/* Phone: the mark says the verdict; the status word and the row action
   (also in the detail) step out so the name keeps its room. */
@media (max-width: 760px) {
  .chead, .crow { grid-template-columns: 14px minmax(0, 1fr) 60px 72px; gap: 8px; padding: 0 12px; }
  .c-status, .c-act { display: none; }
}
</style>
