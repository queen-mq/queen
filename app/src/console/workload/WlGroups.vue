<script setup>
// The consumer groups of the queue picked, on the right: how far behind
// each is and how it reads, worst first and the Kafka groups nobody runs
// last (workloadViews groupsModel). The queue's own page has the rest.
defineProps({
  queue: { type: String, required: true },
  model: { type: Object, default: null },        // groupsModel()
  hasGroups: { type: Boolean, default: true },
  error: { type: String, default: '' },
  dead: { type: Number, default: 0 },            // its dead letters now
})
const emit = defineEmits(['close'])
</script>

<template>
  <aside class="wlg qn-panel" :aria-label="`Consumer groups of ${queue}`">
    <div class="wlg-head">
      <div style="flex: 1 1 0; min-width: 0">
        <p class="qn-t3" style="font-size: 12px">Consumer groups of</p>
        <h3 class="qn-mono wlg-name">{{ queue }}</h3>
      </div>
      <button type="button" class="qn-btn3" aria-label="Close the consumer groups" style="height: 28px" @click="emit('close')">✕</button>
    </div>

    <p v-if="!hasGroups && error" class="qn-t2 wlg-sum">The consumer groups did not load: {{ error }}.</p>
    <p v-else-if="!model" class="qn-t2 wlg-sum">Reading the consumer groups…</p>
    <p v-else-if="!model.rows.length" class="qn-t2 wlg-sum">No consumer group has read this queue.</p>
    <template v-else>
      <p class="qn-t2 wlg-sum">{{ model.summary }}</p>
      <ul class="wlg-list">
        <li v-for="g in model.rows" :key="g.key" class="wlg-g">
          <p class="wlg-gname">
            <span v-if="g.mark" class="qn-mark" :class="g.mark" aria-hidden="true" />
            <span :class="g.mono ? 'qn-mono' : 'qn-t2'" class="wlg-gtext" :title="g.name">{{ g.name }}</span>
            <span v-if="g.kafka" class="qn-tag wlg-tag">Kafka</span>
            <span v-if="g.conflation" class="qn-tag wlg-tag" title="Keeps only the newest message of a key">conflation</span>
          </p>
          <dl class="qn-dl wlg-dl">
            <dt>State</dt><dd :class="g.mark">{{ g.state }}<span v-if="g.presence" class="qn-t3">, {{ g.presence }}</span></dd>
            <dt>Oldest</dt><dd class="qn-num" :class="g.mark">{{ g.oldest }}</dd>
            <dt>Waiting</dt><dd class="qn-num">{{ g.waiting }}</dd>
            <dt>Behind</dt><dd class="qn-num">{{ g.behind === 'none' ? 'no partition' : `${g.behind} partitions` }}</dd>
            <template v-if="g.readers"><dt>Readers</dt><dd class="qn-num">{{ g.readers }}</dd></template>
            <template v-if="g.from"><dt>Reads</dt><dd>{{ g.from }}</dd></template>
          </dl>
        </li>
      </ul>
    </template>

    <div class="wlg-actions">
      <RouterLink :to="`/queues/${encodeURIComponent(queue)}`" class="qn-btn2">Open the queue</RouterLink>
      <RouterLink v-if="dead > 0" :to="{ path: '/dlq', query: { queue } }" class="qn-btn3">Its dead letters</RouterLink>
    </div>
    <p class="qn-read wlg-read">Read from <span class="qn-mono">GET /api/v1/consumer-groups</span>, this queue's rows. Oldest is the age of the oldest message a group has not read.</p>
  </aside>
</template>

<style scoped>
.wlg { padding: 16px 18px 18px; }
.wlg-head { display: flex; align-items: flex-start; gap: 8px; }
.wlg-name { font-size: 14px; font-weight: 500; margin-top: 2px; overflow-wrap: anywhere; }
.wlg-sum { margin-top: 10px; font-size: 12.5px; }
.wlg-list { margin-top: 6px; }
.wlg-g { padding: 12px 0; border-bottom: 1px solid var(--line2); }
.wlg-g:last-child { border-bottom: 0; }
:global(.qn.tone .wlg-g) { border-bottom-color: var(--bg3); }
.wlg-gname { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 8px; min-width: 0; font-size: 13px; font-weight: 500; }
.wlg-gtext { min-width: 0; overflow-wrap: anywhere; }
.wlg-tag { font-size: 11px; font-weight: 400; }
.wlg-dl { grid-template-columns: 64px minmax(0, 1fr); gap: 5px 12px; margin-top: 8px; }
.wlg-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
.wlg-actions a { height: 28px; font-size: 12.5px; }
.wlg-read { margin-top: 12px; }
</style>
