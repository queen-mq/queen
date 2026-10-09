<template>
  <!--
    One application: every instance that publishes under one group. Built from
    the pieces every other page uses: a card with its header row, flat stats,
    a table. Healthy is grey here as it is on the Overview; colour is kept for
    the application that needs you (the `.g` glyph and the chip).
  -->
  <article class="card supervisor-app">
    <header class="card-header">
      <span class="g" :class="glyph(group.tone)" aria-hidden="true" />
      <h3>{{ group.name }}</h3>
      <span class="card-sub">{{ engineLabel }} · {{ instanceLabel }}</span>
      <span v-if="group.tone === 'bad' || group.tone === 'warn'" class="chip supervisor-finding" :class="group.tone === 'bad' ? 'chip-bad' : 'chip-warn'" :title="group.detail"><span class="dot" />{{ group.label }}</span>
      <span v-else-if="group.tone !== 'good'" class="muted" :title="group.detail">{{ group.label }}</span>
    </header>
    <div class="card-body">
      <div class="stat-grid supervisor-stats">
        <div class="stat">
          <div class="stat-label">Workers</div>
          <div class="stat-value" :class="{ warn: group.missingWorkers }">{{ number(group.workers) }}<small>/ {{ number(group.desired) }}</small></div>
          <div class="stat-foot">{{ group.workers === null ? 'capacity unconfirmed' : group.missingWorkers ? `${number(group.missingWorkers)} below target` : 'running / desired' }}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Pools to check</div>
          <div class="stat-value" :class="{ warn: group.affectedPools }">{{ number(group.affectedPools) }}<small>/ {{ number(group.poolCount) }}</small></div>
        </div>
        <template v-if="group.consumerOnly">
          <div class="stat">
            <div class="stat-label">Busy workers</div>
            <div class="stat-value">{{ number(group.busy) }}</div>
          </div>
          <div class="stat" title="Handler failures across the loaded instances, counted since each instance started">
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
          <div class="stat">
            <div class="stat-label">Free process slots</div>
            <div class="stat-value">{{ number(group.headroom) }}</div>
          </div>
        </template>
        <div class="stat">
          <div class="stat-label">Oldest heartbeat</div>
          <div class="stat-value">{{ group.oldestAge === null ? '—' : age(group.oldestAge) }}</div>
          <div class="stat-foot">before this read</div>
        </div>
      </div>
      <p v-if="group.partial || group.scopedConsumers" class="supervisor-note">{{ group.partial ? 'Loaded instances only: a partial view.' : 'Includes consumers with a dynamic scope.' }}</p>
      <SupervisorActivity :queues="group.queues" :read-at="readAt" />
    </div>
    <details class="supervisor-instances">
      <summary>
        <svg viewBox="0 0 20 20" width="12" height="12" aria-hidden="true"><path d="m8 5 5 5-5 5" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round" /></svg>
        <span>{{ instanceLabel }}</span>
        <span class="instance-summary">{{ replicaLabel }}</span>
      </summary>
      <div class="instance-table">
        <table class="t">
          <thead>
            <tr><th>Host</th><th>Instance</th><th>Status</th><th class="right">Workers</th><th /></tr>
          </thead>
          <tbody>
            <tr v-for="row in group.rows" :key="row.slot">
              <td>
                <button class="row-open" aria-haspopup="dialog" aria-controls="supervisor-drawer" :aria-label="`${row.hostname || 'Instance'} · ${row.instance || row.slot} · ${row.label}`" @click="$emit('select', row.slot)">{{ row.hostname || 'Host unavailable' }}</button>
              </td>
              <td class="instance-id">{{ row.instance ? `…${row.instance.slice(-8)}` : 'unavailable' }} · {{ row.engine?.toUpperCase() || 'unknown engine' }}</td>
              <td><span class="instance-status"><span class="g" :class="glyph(supervisorTone(row))" aria-hidden="true" />{{ row.label }}</span></td>
              <td class="num right" :title="row.fresh && row.state === 'running' ? 'Workers reported' : 'Last reported workers'">{{ number(row.workers) }} <span class="of">/ {{ number(row.desired) }}</span></td>
              <td class="right"><button class="btn btn-ghost row-details" tabindex="-1" aria-hidden="true" @click="$emit('select', row.slot)">Details</button></td>
            </tr>
          </tbody>
        </table>
      </div>
    </details>
  </article>
</template>

<script setup>
import { computed } from 'vue'
import SupervisorActivity from '@/components/SupervisorActivity.vue'
import { supervisorTone } from '@/composables/supervisorGroups'
const props = defineProps({ group: { type: Object, required: true }, readAt: Number })
defineEmits(['select'])
const tones = ['bad', 'warn', 'good', 'idle']
const toneLabels = { good: 'reporting normally', warn: 'need attention', bad: 'critical', idle: 'not running' }
// The app's status glyph: a grey dot for healthy, a shape and a colour for the rest.
const glyph = tone => ({ good: 'ok', warn: 'warn', bad: 'bad' })[tone] || 'idle'
const number = value => value == null ? '—' : value.toLocaleString()
const age = seconds => seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.floor(seconds / 60)}m` : `${Math.floor(seconds / 3600)}h`
const engineLabel = computed(() => props.group.engines.map(engine => engine.toUpperCase()).join(' / ') || 'Unknown engine')
const instanceLabel = computed(() => `${props.group.count} ${props.group.count === 1 ? 'instance' : 'instances'}`)
const replicaLabel = computed(() => tones.filter(tone => props.group.counts[tone]).map(tone => `${props.group.counts[tone]} ${toneLabels[tone]}`).join(' · '))
</script>

<style scoped>
.supervisor-app { overflow: hidden; }
.card-header { gap: 8px; }
.card-header h3 { overflow-wrap: anywhere; }
.supervisor-finding { margin-left: auto; }
.supervisor-stats { grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); padding-bottom: 14px; border-bottom: 1px solid var(--bd); }
.stat-value.warn { color: var(--warn-400); }
.supervisor-note { margin: 10px 0 0; font-size: 12px; color: var(--text-low); }
.supervisor-instances > summary { display: flex; align-items: center; gap: 8px; padding: 8px 14px; border-top: 1px solid var(--bd); list-style: none; cursor: pointer; font-size: 12px; color: var(--text-mid); }
.supervisor-instances > summary::-webkit-details-marker { display: none; }
.supervisor-instances > summary:hover { color: var(--text-hi); }
.supervisor-instances > summary:focus-visible, .row-open:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; }
.supervisor-instances > summary > svg { transition: transform .15s; }
.supervisor-instances[open] > summary > svg { transform: rotate(90deg); }
.instance-summary { margin-left: auto; color: var(--text-low); }
.instance-table { overflow-x: auto; border-top: 1px solid var(--bd); }
.instance-table .t { width: 100%; }
.right { text-align: right; }
.row-open { padding: 0; border: 0; background: none; font: inherit; color: var(--text-hi); text-align: left; cursor: pointer; overflow-wrap: anywhere; }
.row-open:hover { text-decoration: underline; }
.instance-id { color: var(--text-low); font-family: var(--font-mono); font-size: 11px; white-space: nowrap; }
.instance-status { display: inline-flex; align-items: center; gap: 8px; }
.of { color: var(--text-low); }
.row-details { height: 24px; padding: 0 8px; font-size: 11.5px; }
@media (prefers-reduced-motion: reduce) { .supervisor-instances > summary > svg { transition: none; } }
</style>
