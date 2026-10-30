<template>
  <!--
    One application: every instance that publishes under one group. A card
    with its header line, flat figures, its pools (one row per pool across
    replicas, supervisorsSide.applicationPools; a row opens the instance
    behind its mark), the queues it reports and their traffic, and its
    instances. Healthy is unmarked; a mark and a chip only where it needs you.
  -->
  <article class="card sv-app">
    <header class="card-header">
      <span v-if="verdict.mark" class="g" :class="verdict.mark" aria-hidden="true" />
      <h3 :title="group.name">{{ group.name }}</h3>
      <span class="card-sub">{{ engineLabel }} · {{ instanceLabel }}</span>
      <span
        v-if="verdict.mark === 'bad' || verdict.mark === 'warn'"
        class="chip sv-verdict"
        :class="verdict.mark === 'bad' ? 'chip-bad' : 'chip-warn'"
        :title="group.detail"
      ><span class="dot" />{{ verdict.why }}</span>
      <span v-else-if="verdict.mark === 'idle'" class="sv-verdict sv-quiet-word" :title="group.detail">{{ verdict.why }}</span>
    </header>

    <div class="card-body">
      <div class="stat-grid sv-stats">
        <div class="stat" title="Running / desired, on instances with a recent running heartbeat">
          <div class="stat-label">Workers</div>
          <div class="stat-value" :class="{ warn: group.missingWorkers }">{{ number(group.workers) }}<small>/ {{ number(group.desired) }}</small></div>
          <div class="stat-foot">{{ group.workers === null ? 'unconfirmed' : group.missingWorkers ? `${number(group.missingWorkers)} below target` : 'running / desired' }}</div>
        </div>
        <div class="stat" title="Pools with a finding, of the pools reported">
          <div class="stat-label">Pools to check</div>
          <div class="stat-value" :class="{ warn: group.affectedPools }">{{ number(group.affectedPools) }}<small>/ {{ number(group.poolCount) }}</small></div>
        </div>
        <template v-if="group.consumerOnly">
          <div class="stat">
            <div class="stat-label">Busy workers</div>
            <div class="stat-value">{{ number(group.busy) }}</div>
          </div>
          <div class="stat" title="Handler failures across reporting instances, counted since each instance started">
            <div class="stat-label">Handler failures</div>
            <div class="stat-value">{{ number(group.handlerFailures) }}</div>
            <div class="stat-foot">since start</div>
          </div>
        </template>
        <template v-else-if="group.processOnly">
          <div class="stat">
            <div class="stat-label">Draining</div>
            <div class="stat-value">{{ number(group.draining) }}</div>
          </div>
          <div class="stat" title="Process slots the shared budget can still fit">
            <div class="stat-label">Free process slots</div>
            <div class="stat-value">{{ number(group.headroom) }}</div>
          </div>
        </template>
        <div class="stat" :title="group.reportingCount ? 'The oldest heartbeat of the instances reporting now, by the browser clock' : 'The oldest heartbeat of the ongoing instances, by the browser clock'">
          <div class="stat-label">Oldest heartbeat</div>
          <div class="stat-value">{{ group.oldestAge === null ? '—' : age(group.oldestAge) }}</div>
          <div class="stat-foot">before this read</div>
        </div>
      </div>
      <p v-if="note" class="sv-note" :title="noteTitle">{{ note }}</p>

      <section class="sv-pools" :aria-label="`Pools of ${group.name}`">
        <div class="sect-head">
          <h3>Pools</h3>
          <span class="sv-count">{{ formatNumber(pools.length) }}</span>
        </div>
        <p v-if="!pools.length" class="sv-quiet">{{ poolFilter === 'all' ? 'No pool reported' : 'No pool matches the filter' }}</p>
        <div v-else class="sv-table">
          <table class="t">
            <thead>
              <tr>
                <th class="sv-mark-col"><span class="sr-only">Mark</span></th>
                <th>Pool</th>
                <th class="sv-queue-col">Queue</th>
                <th>Finding</th>
                <th class="num" title="Running / desired, on instances with a recent running heartbeat">Workers</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="p in shownPools" :key="p.key" class="sv-pool" @click="openPool(p)">
                <td class="sv-mark-col"><span v-if="p.mark" class="g" :class="p.mark" aria-hidden="true" /></td>
                <td class="sv-name-cell">
                  <button
                    type="button"
                    class="sv-open"
                    aria-haspopup="dialog"
                    aria-controls="supervisor-drawer"
                    :title="`${p.name} on ${p.queue} · reported by ${p.replicas} ${p.replicas === 1 ? 'instance' : 'instances'}; opens the one behind its mark`"
                    @click.stop="openPool(p)"
                  >{{ p.name }}</button>
                  <span class="sv-queue-sub">{{ p.queue }}</span>
                </td>
                <td class="sv-queue-col sv-mono">{{ p.queue }}</td>
                <td class="sv-finding">{{ p.finding }}<span v-if="p.notes.length" class="sv-notes"> · {{ p.notes.join(' · ') }}</span></td>
                <td class="num">{{ p.running === null || p.desired === null ? '—' : `${formatNumber(p.running)} / ${formatNumber(p.desired)}` }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <button v-if="pools.length > POOL_ROWS && !allPools" type="button" class="btn btn-ghost sv-more" @click="allPools = true">
          Show all {{ formatNumber(pools.length) }} pools
        </button>
      </section>

      <SupervisorActivity :queues="group.queues" :read-at="readAt" />
    </div>

    <details class="sv-instances">
      <summary>
        <Icon name="chevron-right" class="sv-chevron" :size="14" :stroke="1.8" />
        <span>{{ instanceLabel }}</span>
        <span class="sv-summary">{{ replicaLabel }}</span>
      </summary>
      <div class="sv-table sv-instance-table">
        <table class="t">
          <thead>
            <tr><th>Host</th><th>Instance</th><th>Status</th><th class="num" title="Running / desired">Workers</th><th class="sv-details-col"><span class="sr-only">Open</span></th></tr>
          </thead>
          <tbody>
            <tr v-for="row in group.rows" :key="row.slot">
              <td>
                <button
                  type="button"
                  class="sv-open"
                  aria-haspopup="dialog"
                  aria-controls="supervisor-drawer"
                  :aria-label="`${row.hostname || 'Instance'} · ${row.instance || row.slot} · ${row.label}`"
                  @click="$emit('select', row.slot)"
                >{{ row.hostname || 'Host unavailable' }}</button>
              </td>
              <td class="sv-mono sv-low">{{ row.instance ? `…${row.instance.slice(-8)}` : 'unavailable' }} · {{ row.engine?.toUpperCase() || 'unknown engine' }}</td>
              <td>
                <span class="sv-status">
                  <span class="sv-mark-slot"><span v-if="instanceMark(row)" class="g" :class="instanceMark(row)" aria-hidden="true" /></span>{{ row.label }}
                </span>
              </td>
              <td class="num" :title="row.fresh && row.state === 'running' ? 'Workers reported' : 'Last reported workers'">{{ number(row.workers) }} <span class="sv-low">/ {{ number(row.desired) }}</span></td>
              <td class="num sv-details-col"><button type="button" class="btn btn-ghost sv-details" tabindex="-1" aria-hidden="true" @click="$emit('select', row.slot)">Details</button></td>
            </tr>
          </tbody>
        </table>
      </div>
    </details>
  </article>
</template>

<script setup>
import { computed, ref } from 'vue'

import Icon from '@/components/Icon.vue'
import SupervisorActivity from '@/components/SupervisorActivity.vue'
import { supervisorTone } from '@/composables/supervisorGroups'
import { applicationPools, applicationVerdict } from '@/composables/supervisorsSide'
import { formatNumber } from '@/composables/useApi'

const props = defineProps({
  group: { type: Object, required: true },
  readAt: { type: Number, default: null },
  /** The page's queue context ('' for none): only the pools serving it. */
  queue: { type: String, default: '' },
  /** The tools row's Pools filter: 'all' | 'attention' | 'draining'. */
  poolFilter: { type: String, default: 'all' },
})
const emit = defineEmits(['select', 'open-pool'])

// Pools drawn before "Show all": a large application keeps its card short.
const POOL_ROWS = 8

const tones = ['bad', 'warn', 'good', 'idle']
const toneLabels = { good: 'reporting normally', warn: 'need attention', bad: 'critical', idle: 'not running' }
const number = (value) => (value == null ? '—' : formatNumber(value))
const age = (seconds) => (seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.floor(seconds / 60)}m` : `${Math.floor(seconds / 3600)}h`)
// An instance's status mark: only where it needs you, or a ring when it is not running.
const instanceMark = (row) => ({ bad: 'bad', warn: 'warn', idle: 'idle' })[supervisorTone(row)] || null

const verdict = computed(() => applicationVerdict(props.group))
const engineLabel = computed(() => props.group.engines.map((engine) => engine.toUpperCase()).join(' / ') || 'Unknown engine')
const instanceLabel = computed(() => `${props.group.count} ${props.group.count === 1 ? 'instance' : 'instances'}`)
const replicaLabel = computed(() => tones.filter((tone) => props.group.counts[tone]).map((tone) => `${props.group.counts[tone]} ${toneLabels[tone]}`).join(' · '))

// One short line under the figures, only when they are not the whole story.
const note = computed(() => {
  const g = props.group
  const parts = []
  if (g.reportingCount !== g.count) parts.push(g.reportingCount ? `Figures from ${g.reportingCount} of ${g.count} instances` : 'No instance reporting now')
  if (g.stoppedCount) parts.push(`${g.stoppedCount} stopped`)
  if (g.partial) parts.push('loaded instances only')
  else if (g.scopedConsumers) parts.push('includes consumers with a dynamic scope')
  return parts.join(' · ')
})
const noteTitle = computed(() => (props.group.partial
  ? 'More publications are available than were read: the figures count the loaded instances only.'
  : 'Figures add up the instances with a recent running heartbeat.'))

const pools = computed(() => applicationPools(props.group, { queue: props.queue, pools: props.poolFilter }))
const allPools = ref(false)
const shownPools = computed(() => (allPools.value ? pools.value : pools.value.slice(0, POOL_ROWS)))
const openPool = (p) => emit('open-pool', { slot: p.open, key: p.key })
</script>

<style scoped>
.sv-app { overflow: hidden; }
.card-header { gap: 8px; min-width: 0; }
.card-header h3 { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.card-header .card-sub { white-space: nowrap; }
.sv-verdict { margin-left: auto; max-width: 50%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.sv-quiet-word { font-size: 12px; color: var(--text-low); }

.sv-stats { grid-template-columns: repeat(auto-fit, minmax(128px, 1fr)); }
.stat-value.warn { color: var(--warn-400); }
.sv-note { margin: 12px 0 0; font-size: 12px; color: var(--text-low); }

/* Pools: the card's own table, hairline above it. */
.sv-pools { margin-top: 16px; padding-top: 14px; border-top: 1px solid var(--bd); }
.sv-pools .sect-head { margin-bottom: 8px; }
.sv-count { font-variant-numeric: tabular-nums; }
.sv-quiet { margin: 0; padding: 4px 0 2px; font-size: 12px; color: var(--text-low); }
.sv-table { overflow-x: auto; border: 1px solid var(--bd); border-radius: var(--r-card); }
.sv-table .t { width: 100%; }
.sv-table .t th { background: var(--ink-3); }
.sv-mark-col { width: 28px; padding-right: 0 !important; text-align: center; }
.sv-pool { cursor: pointer; }
.sv-name-cell { min-width: 0; }
.sv-open {
  padding: 0; border: 0; background: none; font: inherit; font-weight: 500; color: var(--text-hi);
  text-align: left; cursor: pointer; overflow-wrap: anywhere;
}
.sv-open:hover { text-decoration: underline; text-underline-offset: 2px; }
.sv-open:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; border-radius: 2px; }
.sv-queue-sub { display: none; }
.sv-mono { font-family: var(--font-mono); font-size: 11.5px; color: var(--text-mid); overflow-wrap: anywhere; }
.sv-finding { color: var(--text-mid); }
.sv-notes { color: var(--text-low); }
.sv-low { color: var(--text-low); }
.sv-more { margin-top: 6px; }

/* Instances: a quiet fold under the card. */
.sv-instances > summary {
  display: flex; align-items: center; gap: 8px; min-height: 36px; padding: 0 14px;
  border-top: 1px solid var(--bd); list-style: none; cursor: pointer; font-size: 12px; color: var(--text-mid);
}
.sv-instances > summary::-webkit-details-marker { display: none; }
.sv-instances > summary:hover { color: var(--text-hi); }
.sv-instances > summary:focus-visible { outline: 2px solid var(--ring); outline-offset: -2px; }
.sv-chevron { flex: none; color: var(--text-low); transition: transform .15s; }
.sv-instances[open] .sv-chevron { transform: rotate(90deg); }
.sv-summary { margin-left: auto; color: var(--text-low); text-align: right; }
.sv-instance-table { border: 0; border-top: 1px solid var(--bd); border-radius: 0; }
.sv-status { display: inline-flex; align-items: center; gap: 8px; }
.sv-mark-slot { flex: none; width: 10px; display: inline-flex; justify-content: center; }
.sv-details { height: 24px; padding: 0 8px; font-size: 11.5px; }

/* A phone: the queue goes under the pool's name instead of a column. */
@media (max-width: 640px) {
  .sv-queue-col { display: none; }
  .sv-queue-sub { display: block; margin-top: 2px; font-family: var(--font-mono); font-size: 11px; color: var(--text-low); overflow-wrap: anywhere; }
  .sv-summary, .sv-details-col { display: none; }
  .sv-verdict { max-width: 100%; margin-left: 0; }
  .card-header { flex-wrap: wrap; }
}
@media (prefers-reduced-motion: reduce) { .sv-chevron { transition: none; } }
</style>
