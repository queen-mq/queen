<template>
  <article class="deployment" :class="`tone-${group.tone}`">
    <header class="deployment-header">
      <div class="deployment-identity">
        <h3 class="deployment-name">{{ group.name }}</h3>
        <div class="deployment-meta"><span class="engine">{{ group.engines.join(' / ') || 'Unknown engine' }}</span><span aria-hidden="true">/</span><span>{{ group.count }} {{ group.count === 1 ? 'instance' : 'instances' }}</span><span class="instance-marks" :aria-label="replicaLabel"><i v-for="row in group.rows.slice(0, 12)" :key="row.slot" :class="`tone-${supervisorTone(row)}`" :title="`${row.hostname || row.instance} · ${row.label}`" /><small v-if="group.count > 12">+{{ group.count - 12 }}</small></span></div>
      </div>
      <div class="deployment-signal"><strong><span class="signal-dot" aria-hidden="true" />{{ group.label }}</strong><small>{{ group.detail }}</small></div>
    </header>
    <div class="deployment-dashboard">
      <section class="deployment-runtime" aria-label="Supervisor runtime">
        <span class="section-label">Worker capacity</span>
        <p class="worker-capacity">{{ number(group.workers) }}<span> / {{ number(group.desired) }}</span></p>
        <p class="worker-caption" :class="{ shortfall: group.missingWorkers }">{{ group.workers === null ? 'Current capacity unconfirmed' : group.missingWorkers ? `${number(group.missingWorkers)} workers below target` : 'Running / desired' }}</p>
        <dl class="runtime-readings">
          <div><dt>Pools to check</dt><dd :class="{ 'needs-attention': group.affectedPools }">{{ number(group.affectedPools) }} <span>/ {{ number(group.poolCount) }}</span></dd></div>
          <template v-if="group.consumerOnly"><div><dt>Busy workers</dt><dd>{{ number(group.busy) }}</dd></div><div><dt title="Cumulative handler failures across the loaded instances, since each instance started">Handler failures <small>since start</small></dt><dd>{{ number(group.handlerFailures) }}</dd></div></template>
          <template v-else-if="group.processOnly"><div><dt>Draining</dt><dd>{{ number(group.draining) }}</dd></div><div><dt>Free process slots</dt><dd>{{ number(group.headroom) }}</dd></div></template>
          <div><dt>Oldest heartbeat</dt><dd>{{ group.oldestAge === null ? '—' : age(group.oldestAge) }}<small>before read</small></dd></div>
        </dl>
        <p v-if="group.partial || group.scopedConsumers" class="runtime-note">{{ group.partial ? 'Loaded instances only · partial view' : 'Includes consumers with dynamic scope' }}</p>
      </section>
      <SupervisorActivity :queues="group.queues" :read-at="readAt" />
    </div>
    <details class="deployment-details">
      <summary><span class="instance-toggle"><svg viewBox="0 0 20 20" width="13" height="13" aria-hidden="true"><path d="m8 5 5 5-5 5" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round" /></svg>{{ group.count }} {{ group.count === 1 ? 'instance' : 'instances' }} <span>Inspect hosts and pools</span></span><span class="instance-summary">{{ replicaLabel }}</span></summary>
    <div class="deployment-instances">
      <div class="instance-heading"><strong>Instances <span>{{ String(group.count).padStart(2, '0') }}</span></strong><span>Select an instance to inspect its queues and diagnostics</span></div>
      <ul>
        <li v-for="row in group.rows" :key="row.slot">
          <button class="instance-row" :class="`tone-${supervisorTone(row)}`" aria-haspopup="dialog" aria-controls="supervisor-drawer" :aria-label="`${row.hostname || 'Instance'} · ${row.instance || row.slot} · ${row.label}`" @click="$emit('select', row.slot)">
            <span class="instance-name"><strong>{{ row.hostname || 'Host unavailable' }}</strong><small>{{ row.instance ? `…${row.instance.slice(-8)}` : 'Instance unavailable' }} <span aria-hidden="true">/</span> {{ row.engine?.toUpperCase() || 'Unknown engine' }}</small></span>
            <span class="instance-finding"><span class="signal-dot" aria-hidden="true" />{{ row.label }}</span>
            <span class="instance-workers">{{ number(row.workers) }} / {{ number(row.desired) }}<small>{{ row.fresh && row.state === 'running' ? 'workers reported' : 'last reported workers' }}</small></span>
            <span class="instance-open" aria-hidden="true">Inspect <span>↗</span></span>
          </button>
        </li>
      </ul>
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
const number = value => value == null ? '—' : value.toLocaleString()
const age = seconds => seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.floor(seconds / 60)}m` : `${Math.floor(seconds / 3600)}h`
const replicaLabel = computed(() => tones.filter(tone => props.group.counts[tone]).map(tone => `${props.group.counts[tone]} ${toneLabels[tone]}`).join(' · '))
</script>

<style scoped>
.deployment {
  --supervisor-good: #85bba4; --supervisor-warn: var(--warn-400); --supervisor-bad: var(--ember-400); --supervisor-idle: var(--text-low);
  border: 1px solid var(--bd-hi); background: var(--ink-2); border-radius: 3px; overflow: hidden;
}
:global(html.light .deployment) { --supervisor-good: #287a57; }
.tone-good { --status-color: var(--supervisor-good); }.tone-warn { --status-color: var(--supervisor-warn); }.tone-bad { --status-color: var(--supervisor-bad); }.tone-idle { --status-color: var(--supervisor-idle); }
.deployment-header { display: flex; align-items: center; justify-content: space-between; gap: 28px; padding: 16px 24px; border-bottom: 1px solid var(--bd); position: relative; }
.deployment-header::before { content: ''; position: absolute; top: 20px; bottom: 20px; left: -1px; width: 2px; background: var(--status-color); }
.deployment-identity { min-width: 0; }.deployment-name { font-size: 15px; font-weight: 550; line-height: 1.4; letter-spacing: -.015em; color: var(--text-hi); overflow-wrap: anywhere; margin: 0; }
.deployment-meta { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; margin-top: 8px; color: var(--text-low); font-size: 10px; }.engine { text-transform: uppercase; font-family: var(--font-mono); font-size: 9px; max-width: 100%; overflow-wrap: anywhere; }
.instance-marks { display: inline-flex; flex-wrap: wrap; align-items: center; gap: 3px; margin-left: 2px; }.instance-marks i { width: 3px; height: 9px; background: var(--status-color); border-radius: 1px; }.instance-marks small { font-size: 9px; margin-left: 3px; }
.deployment-signal { min-width: 0; text-align: right; }.deployment-signal > strong { display: flex; align-items: center; justify-content: end; gap: 7px; color: var(--status-color); font-size: 11px; font-weight: 500; }.deployment-signal > small { display: block; font-size: 10px; line-height: 1.5; color: var(--text-low); margin-top: 6px; }
.signal-dot { display: inline-block; width: 5px; height: 5px; background: var(--status-color); border-radius: 50%; flex: none; }
.deployment-dashboard { display: grid; grid-template-columns: 175px minmax(0, 1fr); gap: 28px; padding: 18px 24px 16px; }
.deployment-runtime { padding-right: 24px; border-right: 1px solid var(--bd); min-width: 0; }.section-label { font-size: 10px; font-weight: 500; color: var(--text-mid); text-transform: uppercase; letter-spacing: .08em; }
.worker-capacity { margin: 14px 0 5px; font-size: 38px; color: var(--text-hi); font-weight: 400; letter-spacing: -.055em; line-height: 1; font-variant-numeric: tabular-nums; }.worker-capacity > span { color: var(--text-low); font-size: 20px; letter-spacing: -.025em; }
.worker-caption { margin: 0 0 16px; color: var(--text-low); font-size: 10px; line-height: 1.5; }.worker-caption.shortfall { color: var(--status-color); }
.runtime-readings { margin: 0; display: grid; gap: 12px; }.runtime-readings > div { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }.runtime-readings dt { font-size: 10px; color: var(--text-low); }.runtime-readings dd { font-size: 12px; font-variant-numeric: tabular-nums; color: var(--text-hi); margin: 0; text-align: right; }.runtime-readings dd span { color: var(--text-low); font-size: 10px; }.runtime-readings small { font-size: 8px; display: block; color: var(--text-low); margin-top: 2px; }.runtime-readings dd.needs-attention { color: var(--status-color); }
.runtime-note { font-size: 9px; color: var(--text-low); line-height: 1.5; margin: 10px 0 0; }
.deployment-details > summary { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 10px 24px; border-top: 1px solid var(--bd); list-style: none; cursor: pointer; font-size: 10px; color: var(--text-mid); }.deployment-details > summary::-webkit-details-marker { display: none; }.deployment-details > summary:hover { background: color-mix(in srgb, var(--text-hi) 3%, transparent); }.deployment-details > summary:focus-visible, .instance-row:focus-visible { outline: 2px solid var(--ring); outline-offset: -2px; }
.instance-toggle { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; }.instance-toggle > span { color: var(--text-low); margin-left: 6px; }.instance-toggle > svg { transition: transform .15s; }.deployment-details[open] .instance-toggle > svg { transform: rotate(90deg); }.instance-summary { color: var(--status-color); font-size: 9px; text-align: right; }
.deployment-instances { padding: 4px 28px 24px; background: color-mix(in srgb, var(--text-hi) 2%, transparent); }
.instance-heading { display: flex; align-items: baseline; gap: 20px; padding: 14px 0; border-top: 1px solid var(--bd); font-size: 11px; color: var(--text-low); }
.instance-heading > strong { font-weight: 500; font-size: 10px; text-transform: uppercase; letter-spacing: .08em; color: var(--text-mid); }
.instance-heading > strong span { margin-left: 8px; font-family: var(--font-mono); color: var(--text-low); }
.deployment-instances ul { list-style: none; padding: 0 0 0 18px; margin: 0; border-left: 1px solid var(--bd-hi); }
.deployment-instances li { position: relative; }.deployment-instances li::before { content: ''; position: absolute; left: -18px; top: 28px; width: 12px; border-top: 1px solid var(--bd-hi); }
.instance-row { display: grid; grid-template-columns: minmax(130px, 1.7fr) minmax(110px, 1fr) 110px 70px; gap: 18px; align-items: center; width: 100%; padding: 12px 8px; background: transparent; border: 0; color: var(--text-hi); text-align: left; font: inherit; cursor: pointer; }
.instance-row:hover { background: var(--ink-3); }
.instance-name { min-width: 0; }.instance-name strong { display: block; overflow-wrap: anywhere; font-size: 12px; font-weight: 450; }
.instance-name small, .instance-workers small { display: block; font-size: 10px; color: var(--text-low); margin-top: 6px; }.instance-name small span { margin: 0 4px; opacity: .5; }
.instance-finding { display: flex; align-items: center; gap: 7px; color: var(--status-color); font-size: 11px; }
.instance-workers { font-size: 12px; font-variant-numeric: tabular-nums; text-align: right; }.instance-open { font-size: 11px; color: var(--text-low); text-align: right; }.instance-open span { margin-left: 6px; }
@media (max-width: 1200px) { .deployment-dashboard { grid-template-columns: 150px minmax(0, 1fr); gap: 18px; padding: 20px; }.deployment-runtime { padding-right: 18px; }.deployment-header { padding: 18px 20px; }.deployment-signal { max-width: 45%; } }
@media (max-width: 1000px) {
  .deployment-dashboard { grid-template-columns: minmax(0, 1fr); gap: 20px; }.deployment-runtime { display: grid; grid-template-columns: 150px minmax(0, 1fr); column-gap: 24px; border: 0; padding: 0 0 20px; border-bottom: 1px solid var(--bd); }
  .deployment-runtime > .section-label { grid-column: 1; }.worker-capacity { grid-column: 1; margin: 10px 0 5px; }.worker-caption { grid-column: 1; margin: 0; }.runtime-readings { grid-column: 2; grid-row: 1 / 4; display: grid; grid-template-columns: 1fr 1fr; gap: 14px 24px; align-content: center; }.runtime-readings > div { flex-direction: column; gap: 6px; }.runtime-readings dd { font-size: 14px; text-align: left; }.runtime-readings small { display: inline; margin-left: 3px; }.runtime-note { grid-column: 1 / -1; }
}
@media (max-width: 640px) {
  .deployment-header { flex-wrap: wrap; gap: 12px; padding: 18px; }.deployment-signal { max-width: 100%; text-align: left; }.deployment-signal > strong { justify-content: start; }.deployment-name { font-size: 14px; }.deployment-dashboard { padding: 18px; }.deployment-runtime { grid-template-columns: 106px minmax(0, 1fr); gap: 12px 16px; }.worker-capacity { font-size: 34px; margin: 0; }.worker-capacity > span { font-size: 17px; }.runtime-readings { gap: 14px 12px; }.runtime-readings small { display: block; margin-left: 0; }.runtime-readings dd { font-size: 13px; }.runtime-readings > div { gap: 4px; }
  .deployment-details > summary { padding: 12px 16px; }.instance-toggle > span, .instance-summary { display: none; }.deployment-instances { padding: 0 8px 18px; }.instance-heading { flex-wrap: wrap; gap: 6px 16px; }
  .instance-row { grid-template-columns: minmax(0, 1fr) 80px; gap: 10px; }.instance-name { grid-column: 1; }.instance-open { grid-row: 1; grid-column: 2; }.instance-finding { grid-row: 2; grid-column: 1; }.instance-workers { grid-row: 2; grid-column: 2; }
}
@media (prefers-reduced-motion: reduce) { .instance-toggle > svg { transition: none; } }
</style>
