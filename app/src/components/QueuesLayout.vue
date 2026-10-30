<template>
  <!--
    The Queues section: everything about a queue in one place. The queue list
    on the left never changes under a click; on the right, one header line
    (the queue, a few facts, the tab's buttons) and the queue's tabs. With no
    queue picked, the tabs that make sense across every queue (Overview,
    Messages, Dead letter) show all of them.
  -->
  <SubSidebar label="Queues" :current="queue || 'All queues'">
    <template #side><QueueList /></template>
    <header class="qs-head">
      <div class="qs-line">
        <h1 class="qs-title" :title="title">{{ title }}</h1>
        <div v-if="facts.length || $slots.facts" class="qs-facts">
          <span v-for="f in facts" :key="f.k" class="qs-fact" :title="f.title || undefined"><span class="qs-k">{{ f.k }}</span>{{ f.v }}</span>
          <slot name="facts" />
        </div>
        <div v-if="live || $slots.actions" class="qs-actions">
          <span v-if="live" class="live-tick" :title="liveTitle"><span class="live-dot" aria-hidden="true" /><span>live · {{ live }}</span></span>
          <slot name="actions" />
        </div>
      </div>
      <nav class="qs-tabs" :aria-label="queue ? `Views of ${queue}` : 'Views of every queue'">
        <router-link
          v-for="t in tabs"
          :key="t.view"
          :to="t.to"
          class="qs-tab"
          :class="{ on: t.view === tab }"
          :aria-current="t.view === tab ? 'page' : undefined"
        >{{ t.label }}<span v-if="t.count" class="qs-count" :class="t.tone" :title="t.countTitle">{{ t.count }}</span></router-link>
      </nav>
    </header>
    <slot />
  </SubSidebar>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'

import QueueList from '@/components/QueueList.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import { QUEUE_TABS, listCarry, queueOf, queueTabLocation, queueTabOf } from '@/composables/navigation'
import { queueAlerts } from '@/composables/queueAlerts'
import { formatBytes, formatNumber, toNum } from '@/composables/useApi'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'

defineProps({
  /** "just now", "12s ago": only on a tab that really polls. */
  live: { type: String, default: '' },
  liveTitle: { type: String, default: 'This page refreshes itself every 30 seconds' },
})

const route = useRoute()
const queuesStore = useQueuesStore()
const groupsStore = useGroupsStore()
const { linesFor } = useSettingsStore()

const queue = computed(() => queueOf(route))
const tab = computed(() => queueTabOf(route) || 'overview')
const row = computed(() => (queue.value ? (queuesStore.queues.value || []).find((q) => q?.name === queue.value) || null : null))
const all = computed(() => queuesStore.queues.value || [])
const loaded = computed(() => queuesStore.lastFetched.value > 0)

const title = computed(() => queue.value || 'All queues')

// The same few facts on every tab, from the queue listing the list beside
// already read: no request of their own.
const sum = (pick) => all.value.reduce((s, q) => s + (toNum(pick(q)) || 0), 0)
const facts = computed(() => {
  if (!loaded.value) return []
  if (!queue.value) {
    return [
      { k: 'Queues', v: formatNumber(all.value.length) },
      { k: 'Pending', v: formatNumber(sum((q) => q.messages?.pending)) },
      { k: 'Stored', v: formatBytes(sum((q) => q.retainedBytes)) },
    ]
  }
  const q = row.value
  if (!q) return []
  return [
    { k: 'Pending', v: formatNumber(toNum(q.messages?.pending) || 0) },
    { k: 'Partitions', v: formatNumber(toNum(q.partitions) || 0) },
    { k: 'Stored', v: formatBytes(toNum(q.retainedBytes) || 0) },
    q.namespace && q.namespace !== q.name ? { k: 'Namespace', v: q.namespace } : null,
    q.task ? { k: 'Task', v: q.task } : null,
  ].filter(Boolean)
})

// The list's search and namespace ride along, so the list stays as it was.
const carry = computed(() => listCarry(route))

const tabs = computed(() => {
  const q = row.value
  const groups = (groupsStore.groups.value || []).filter((g) => !queue.value || g?.queueName === queue.value)
  const alerts = q ? queueAlerts(q, groupsStore.groups.value || [], linesFor) : []
  const dead = queue.value ? toNum(q?.messages?.deadLetter) || 0 : sum((x) => x.messages?.deadLetter)
  const counts = {
    consumers: { count: groups.filter((g) => g.name !== '__QUEUE_MODE__').length },
    failed: { count: dead, countTitle: `${formatNumber(dead)} in dead letter` },
    alerts: {
      count: alerts.length,
      tone: alerts.some((a) => a.sev === 'bad') ? 'bad' : 'warn',
      countTitle: `${alerts.length} ${alerts.length === 1 ? 'thing needs' : 'things need'} you`,
    },
  }
  return QUEUE_TABS
    .filter((t) => queue.value || t.all || t.view === tab.value)
    .map((t) => ({
      ...t,
      to: queueTabLocation(t.view, queue.value, route, carry.value),
      count: counts[t.view]?.count ? formatNumber(counts[t.view].count) : '',
      tone: counts[t.view]?.tone || '',
      countTitle: counts[t.view]?.countTitle || '',
    }))
})
</script>

<style scoped>
.qs-head {
  max-width: 1560px; margin: 0 auto;
  padding: 18px 20px 0;
}
.qs-line { display: flex; align-items: center; gap: 10px 16px; flex-wrap: wrap; min-height: 32px; }
.qs-title {
  margin: 0; min-width: 0; max-width: 100%;
  font-size: 18px; font-weight: 600; letter-spacing: -0.01em; color: var(--text-hi);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.qs-facts { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; min-width: 0; }
.qs-fact {
  display: inline-flex; align-items: baseline; gap: 5px;
  padding: 2px 8px; border-radius: var(--r-chip);
  background: var(--ink-3); border: 1px solid var(--bd);
  font-size: 11.5px; color: var(--text-hi); font-variant-numeric: tabular-nums; white-space: nowrap;
}
.qs-k { color: var(--text-low); }
.qs-actions { margin-left: auto; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }

.qs-tabs {
  display: flex; gap: 2px; flex-wrap: wrap;
  margin-top: 14px; border-bottom: 1px solid var(--bd);
}
.qs-tab {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 8px 12px; margin-bottom: -1px;
  border-bottom: 2px solid transparent;
  font-size: 13px; color: var(--text-mid); text-decoration: none; white-space: nowrap;
}
.qs-tab:hover { color: var(--text-hi); }
.qs-tab.on { color: var(--text-hi); font-weight: 500; border-bottom-color: var(--text-hi); }
.qs-count {
  min-width: 18px; padding: 0 5px; border-radius: var(--r-pill);
  background: var(--ink-4); color: var(--text-mid);
  font-size: 11px; font-weight: 500; line-height: 17px; text-align: center; font-variant-numeric: tabular-nums;
}
.qs-count.warn { background: var(--warn-glow); box-shadow: inset 0 0 0 1px var(--warn-bd); color: var(--warn-400); }
.qs-count.bad { background: var(--ember-glow); box-shadow: inset 0 0 0 1px var(--ember-bd); color: var(--ember-400); }

/* Narrow: the tabs scroll sideways rather than wrap into a second row. */
@media (max-width: 1100px) {
  .qs-tabs { flex-wrap: nowrap; overflow-x: auto; scrollbar-width: none; }
  .qs-tabs::-webkit-scrollbar { display: none; }
}
@media (max-width: 640px) {
  .qs-head { padding: 14px 12px 0; }
  .qs-title { font-size: 17px; }
  .qs-actions { margin-left: 0; width: 100%; }
}
</style>
