<script setup>
// The right side of the Workers view: the picked application, pool or instance
// (workersView appDetail, poolDetail, instanceDetail). A row in here opens the
// related one in its place; the list on the left only moves its highlight.
// Read-only: a supervisor is paused, continued or stopped on its own host.
import { breakable } from './workersView'

const props = defineProps({
  d: { type: Object, default: null },       // a detail, or null when the last read does not hold it
  queue: { type: Object, default: null },   // a pool's queue, the last hour from the shared stores: { line, dead }
  namespace: { type: String, default: '' },
  back: { type: Object, default: null },    // { id, label }: where this was opened from
})
const emit = defineEmits(['open', 'close'])
const closeable = () => !props.back
const qlink = (name) => `/queues/${encodeURIComponent(name)}`
</script>

<template>
  <aside class="wkd qn-panel" :aria-label="d ? `${d.kind === 'app' ? 'Application' : d.kind === 'pool' ? 'Pool' : 'Instance'} ${d.name || d.title}` : 'Details'">
    <div v-if="back" class="wkd-top">
      <button type="button" class="qn-btn3 wkd-back" :title="`Back to ${back.label}`" @click="emit('open', back.id)">
        <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M7.5 2.5 4 6l3.5 3.5" /></svg>
        <span class="qn-mono">{{ back.label }}</span>
      </button>
      <button type="button" class="qn-btn3 wkd-x" aria-label="Close the details" @click="emit('close')">✕</button>
    </div>

    <!-- what was asked for is not in the last read -->
    <template v-if="!d">
      <div class="wkd-head">
        <p class="qn-t2" style="flex: 1 1 0">It is not in the last read: its report may have expired, or the broker swept it.</p>
        <button v-if="closeable()" type="button" class="qn-btn3 wkd-x" aria-label="Close the details" @click="emit('close')">✕</button>
      </div>
    </template>

    <!-- an application -->
    <template v-else-if="d.kind === 'app'">
      <div class="wkd-head">
        <div class="wkd-id">
          <p class="wkd-kicker">Application · {{ d.kindText }}</p>
          <h3 class="qn-mono wkd-title">{{ d.name }}</h3>
        </div>
        <button v-if="closeable()" type="button" class="qn-btn3 wkd-x" aria-label="Close the details" @click="emit('close')">✕</button>
      </div>
      <dl class="qn-dl wkd-dl">
        <template v-for="f in d.facts" :key="f.k"><dt>{{ f.k }}</dt><dd class="qn-num">{{ f.v }}</dd></template>
      </dl>
      <p v-for="n in d.notes" :key="n" class="qn-t2 wkd-note">{{ n }}</p>
      <h4 class="wkd-h">Its instances</h4>
      <div class="wkd-table" role="table" aria-label="Its instances">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Instance</span><span role="columnheader">State</span>
          <span role="columnheader" class="r" title="Running / wanted">Workers</span>
        </div>
        <div v-for="i in d.instances" :key="i.id" class="qn-tr click" role="row" tabindex="0" @click="emit('open', i.id)" @keydown.enter.prevent="emit('open', i.id)">
          <span role="cell" class="wkd-host"><span class="qn-mono"><template v-for="(c, k) in breakable(i.host)" :key="k">{{ c }}<wbr></template></span><span class="qn-t3 wkd-sub"><span v-if="i.subId" class="qn-mono">{{ i.subId }}</span><template v-if="i.subId && i.sub"> · </template>{{ i.sub }}</span><span class="wkd-stm" :class="i.tone || 'qn-t2'"><span v-if="i.tone" class="qn-mark" :class="i.tone" aria-hidden="true" />{{ i.word }}</span></span>
          <span role="cell" class="wkd-stc" :class="i.tone || (i.terminal ? 'qn-t3' : 'qn-t2')"><span v-if="i.tone" class="qn-mark" :class="i.tone" aria-hidden="true" />{{ i.word }}</span>
          <span role="cell" class="r qn-num">{{ i.workers }}</span>
        </div>
      </div>
    </template>

    <!-- a pool -->
    <template v-else-if="d.kind === 'pool'">
      <div class="wkd-head">
        <div class="wkd-id">
          <p class="wkd-kicker">{{ d.consumer ? 'Consumer group' : 'Pool' }} of <button type="button" class="qn-link qn-mono" @click="emit('open', d.appId)">{{ d.app }}</button></p>
          <h3 class="qn-mono wkd-title">{{ d.name }}</h3>
        </div>
        <button v-if="closeable()" type="button" class="qn-btn3 wkd-x" aria-label="Close the details" @click="emit('close')">✕</button>
      </div>
      <p class="wkd-reads">
        <template v-if="d.queueName">It reads <RouterLink :to="qlink(d.queueName)" class="qn-mono qn-link">{{ d.queueName }}</RouterLink>.</template>
        <template v-else>It reads <span class="qn-mono">{{ d.queue }}</span>, not one named queue.</template>
      </p>
      <p class="wkd-state">
        <span v-if="d.tone" class="qn-mark" :class="d.tone" aria-hidden="true" />
        <span :class="d.tone" style="font-weight: 500">{{ d.word }}</span><span v-if="d.note" class="qn-t2">, {{ d.note }}</span>
      </p>
      <p v-if="d.next" class="qn-t2 wkd-next">{{ d.next }}</p>

      <h4 class="wkd-h">The instances that run it</h4>
      <div class="wkd-table" role="table" aria-label="The instances that run it">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Instance</span><span role="columnheader">State</span>
          <span role="columnheader" class="r" title="Running / wanted">Workers</span>
        </div>
        <div v-for="h in d.hosts" :key="h.id" class="qn-tr click" role="row" tabindex="0" @click="emit('open', h.id)" @keydown.enter.prevent="emit('open', h.id)">
          <span role="cell" class="wkd-host"><span class="qn-mono"><template v-for="(c, k) in breakable(h.host)" :key="k">{{ c }}<wbr></template></span><span class="qn-t3 wkd-sub"><span v-if="h.subId" class="qn-mono">{{ h.subId }}</span><template v-if="h.subId && h.sub"> · </template>{{ h.sub }}</span><span class="wkd-stm" :class="h.tone || 'qn-t2'"><span v-if="h.tone" class="qn-mark" :class="h.tone" aria-hidden="true" />{{ h.word }}</span></span>
          <span role="cell" class="wkd-stc" :class="h.tone || (h.terminal ? 'qn-t3' : 'qn-t2')"><span v-if="h.tone" class="qn-mark" :class="h.tone" aria-hidden="true" />{{ h.word }}</span>
          <span role="cell" class="r qn-num">{{ h.workers }}</span>
        </div>
      </div>
      <p v-for="f in d.failures" :key="f.host" class="qn-t2 wkd-note">
        <span class="qn-mono">{{ f.host }}</span>: {{ f.failures }} restart {{ f.failures === 1 ? 'failure' : 'failures' }} in a row<template v-if="f.retryIn != null">; it was to try again {{ f.retryIn }} s after its report</template>.
      </p>

      <template v-if="d.settings.length">
        <h4 class="wkd-h">Its settings</h4>
        <dl class="qn-dl wkd-dl">
          <template v-for="f in d.settings" :key="f.k"><dt>{{ f.k }}</dt><dd :class="{ 'qn-mono': f.mono }">{{ f.v }}</dd></template>
        </dl>
      </template>

      <template v-if="d.queueName">
        <h4 class="wkd-h">Its queue, the last hour</h4>
        <p v-if="queue" class="qn-num wkd-qline">
          {{ queue.line }}<template v-if="queue.dead"> <RouterLink :to="{ path: '/dlq', query: { queue: d.queueName } }" class="qn-link">{{ queue.dead }}</RouterLink> now.</template>
        </p>
        <p v-else class="qn-t2 wkd-qline">The queue listing does not hold it.</p>
        <p class="wkd-qline" style="margin-top: 6px"><RouterLink :to="qlink(d.queueName)" class="qn-link">Open {{ d.queueName }}</RouterLink><span class="qn-t3">: its traffic and what waits, over any window.</span></p>
      </template>
    </template>

    <!-- an instance -->
    <template v-else>
      <div class="wkd-head">
        <div class="wkd-id">
          <p class="wkd-kicker">{{ d.kicker }}<template v-if="d.short"> · instance <span class="qn-mono">{{ d.short }}</span></template></p>
          <h3 class="qn-mono wkd-title">{{ d.title }}</h3>
        </div>
        <button v-if="closeable()" type="button" class="qn-btn3 wkd-x" aria-label="Close the details" @click="emit('close')">✕</button>
      </div>
      <p class="wkd-reads">Of <button type="button" class="qn-link qn-mono" @click="emit('open', d.appId)">{{ d.app }}</button>.</p>
      <p class="wkd-state">
        <span v-if="d.tone" class="qn-mark" :class="d.tone" aria-hidden="true" />
        <span :class="d.tone" style="font-weight: 500">{{ d.word }}</span>
      </p>
      <p v-if="d.next" class="qn-t2 wkd-next">{{ d.next }}</p>
      <p v-if="d.last" class="qn-t2 wkd-next">{{ d.last }}</p>
      <dl class="qn-dl wkd-dl">
        <template v-for="f in d.facts" :key="f.k">
          <dt>{{ f.k }}</dt>
          <dd :class="{ 'qn-mono': f.mono, 'qn-num': !f.mono }"><RouterLink v-if="f.to" :to="qlink(f.to)" class="qn-link">{{ f.v }}</RouterLink><template v-else>{{ f.v }}</template></dd>
        </template>
      </dl>

      <template v-if="d.pools.length && !d.onePool">
        <h4 class="wkd-h">{{ d.consumer ? 'What it consumes' : 'Its pools' }}</h4>
        <div class="wkd-table" role="table" :aria-label="d.consumer ? 'What it consumes' : 'Its pools'">
          <div class="qn-tr head" role="row">
            <span role="columnheader">{{ d.consumer ? 'Group' : 'Pool' }}, its queue</span><span role="columnheader">State</span>
            <span role="columnheader" class="r" title="Running / wanted">Workers</span>
          </div>
          <div v-for="p in d.pools" :key="p.id" class="qn-tr click" role="row" tabindex="0" @click="emit('open', p.id)" @keydown.enter.prevent="emit('open', p.id)">
            <span role="cell" class="wkd-host"><span class="qn-mono"><template v-for="(c, k) in breakable(p.name)" :key="k">{{ c }}<wbr></template></span><span class="qn-t3 wkd-sub"><span class="qn-mono"><template v-for="(c, k) in breakable(p.subId)" :key="k">{{ c }}<wbr></template></span><template v-if="p.sub"> · {{ p.sub }}</template></span><span class="wkd-stm" :class="p.tone || 'qn-t2'"><span v-if="p.tone" class="qn-mark" :class="p.tone" aria-hidden="true" />{{ p.word }}</span></span>
            <span role="cell" class="wkd-stc" :class="p.tone || 'qn-t2'"><span v-if="p.tone" class="qn-mark" :class="p.tone" aria-hidden="true" />{{ p.word }}</span>
            <span role="cell" class="r qn-num">{{ p.workers }}</span>
          </div>
        </div>
        <p v-for="p in d.pools.filter((x) => x.failures)" :key="`f${p.id}`" class="qn-t2 wkd-note">
          <span class="qn-mono">{{ p.name }}</span>: {{ p.failures }} restart {{ p.failures === 1 ? 'failure' : 'failures' }} in a row<template v-if="p.retryIn != null">; it was to try again {{ p.retryIn }} s after this report</template>.
        </p>
      </template>

      <template v-if="d.handler.length">
        <h4 class="wkd-h">Its handler</h4>
        <dl class="qn-dl wkd-dl">
          <template v-for="f in d.handler" :key="f.k"><dt>{{ f.k }}</dt><dd class="qn-num">{{ f.v }}</dd></template>
        </dl>
        <p class="qn-t3 wkd-note">Counted by the SDK, batch calls included. A finished call is not an ack, and a heartbeat is not progress.</p>
      </template>

      <template v-if="d.pools.some((p) => p.pids.length)">
        <h4 class="wkd-h">Worker PIDs</h4>
        <dl class="qn-dl wkd-dl">
          <template v-for="p in d.pools.filter((x) => x.pids.length)" :key="`p${p.id}`"><dt class="qn-mono">{{ p.name }}</dt><dd class="qn-mono wkd-pids">{{ p.pids.join(' ') }}</dd></template>
        </dl>
      </template>

      <h4 class="wkd-h">Its report</h4>
      <dl class="qn-dl wkd-dl">
        <dt>Namespace</dt><dd class="qn-mono">{{ namespace }}</dd>
        <template v-for="f in d.publication" :key="f.k"><dt>{{ f.k }}</dt><dd :class="{ 'qn-mono': f.mono }">{{ f.v }}</dd></template>
      </dl>
      <p class="qn-read wkd-foot">Pause, continue or stop it on its own host: the console only reads what it publishes. Ages use this browser's clock; expiry is the broker's.</p>
    </template>
  </aside>
</template>

<style scoped>
.wkd { padding: 16px 18px 18px; }
.wkd-top { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin: -4px 0 8px -8px; }
.wkd-back { height: 28px; gap: 4px; font-size: 12px; min-width: 0; }
.wkd-back span { overflow: hidden; text-overflow: ellipsis; }
.wkd-back svg { fill: none; stroke: currentColor; stroke-width: 1.4; stroke-linecap: round; stroke-linejoin: round; }
.wkd-head { display: flex; align-items: flex-start; gap: 8px; }
.wkd-id { flex: 1 1 0; min-width: 0; }
.wkd-x { height: 28px; }
.wkd-kicker { font-size: 12px; color: var(--text3); }
.wkd-title { font-size: 14px; font-weight: 500; margin-top: 2px; overflow-wrap: anywhere; }
.wkd-reads { margin-top: 6px; font-size: 12.5px; color: var(--text2); overflow-wrap: anywhere; }
.wkd-state { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 8px; margin-top: 12px; font-size: 13px; }
.wkd-state .qn-mark { align-self: center; }
.wkd-next { margin-top: 6px; font-size: 12.5px; }
.wkd-dl { grid-template-columns: 96px minmax(0, 1fr); margin-top: 12px; }
.wkd-h { font-size: 12px; font-weight: 500; color: var(--text3); margin: 18px 0 0; padding-top: 14px; border-top: 1px solid var(--line2); }
.wkd-h + .wkd-dl, .wkd-h + .wkd-table, .wkd-h + p { margin-top: 8px; }
.wkd-note { margin-top: 8px; font-size: 12.5px; overflow-wrap: anywhere; }
.wkd-table { --cols: minmax(0, 1fr) 124px 52px; }
.wkd-table .qn-tr { gap: 0 12px; min-height: 40px; font-size: 12.5px; align-items: start; padding: 8px 0; }
.wkd-table .qn-tr.head { min-height: 26px; font-size: 11.5px; padding: 0; align-items: center; }
/* a host is told apart by its end (pod-7d9f-k2xlq): it wraps, never cut */
.wkd-host { display: flex; flex-direction: column; gap: 2px; white-space: normal !important; overflow-wrap: break-word; }
.wkd-sub { font-size: 11.5px; }
.wkd-stc { display: flex; align-items: baseline; gap: 6px; white-space: normal !important; }
.wkd-stc .qn-mark { align-self: center; flex: 0 0 auto; }
.wkd-stm { display: none; }
/* a phone: the name has the width, its state goes under it */
@media (max-width: 640px) {
  .wkd-table { --cols: minmax(0, 1fr) 52px; }
  .wkd-table .qn-tr > :nth-child(2) { display: none; }
  .wkd-stm { display: inline-flex; align-items: center; gap: 6px; margin-top: 2px; font-size: 12.5px; }
}
.wkd-qline { font-size: 12.5px; }
.wkd-pids { font-size: 11.5px; color: var(--text2); line-height: 1.6; }
.wkd-foot { margin-top: 14px; }
:global(.qn.tone .wkd-h) { border-top-color: var(--bg3); }
</style>
