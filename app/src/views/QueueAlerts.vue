<template>
  <!--
    A queue's Alerts tab: what needs you on this queue now, by the same rules
    as its mark in the list and the Overview's issues (composables/
    queueAlerts.js), and the lines it is judged by, with their edit form.
  -->
  <QueuesLayout>
    <template v-if="canEdit" #actions>
      <button class="btn" @click="editing = true">Edit lines</button>
    </template>
    <div class="view-container qa">
      <section class="card">
        <div class="card-header"><h3>Now</h3></div>
        <div v-if="!loaded" class="qa-quiet">Reading the queue…</div>
        <div v-else-if="!row" class="qa-quiet">This queue is not in the queue list.</div>
        <ul v-else-if="alerts.length" class="qa-list">
          <li v-for="a in alerts" :key="a.key" class="qa-alert">
            <span class="qa-mark" aria-hidden="true"><span class="g" :class="a.sev" /></span>
            <span class="qa-text">
              <span class="qa-title">{{ a.title }}</span>
              <span class="qa-detail">{{ a.detail }}</span>
            </span>
            <router-link :to="queueTabLocation(a.tab, queue, route)" class="btn btn-ghost">{{ tabLabel(a.tab) }}</router-link>
          </li>
        </ul>
        <div v-else class="qa-quiet">Nothing needs you on this queue.</div>
      </section>

      <section class="card">
        <div class="card-header">
          <h3>Lines</h3>
          <span class="card-sub">{{ view.hasOwn ? 'its own where it has one, the tenant’s elsewhere' : 'the tenant’s' }}</span>
        </div>
        <div v-if="settingsState !== 'ready'" class="qa-quiet">{{ settingsState === 'unread' ? 'Reading the lines…' : 'The tenant’s built-in lines' }}</div>
        <div class="qa-lines">
          <template v-for="group in view.groups" :key="group.name">
            <div class="qa-group">{{ group.name }}</div>
            <div v-for="line in group.rows" :key="line.key" class="qa-line" :title="line.help">
              <span class="qa-label">{{ line.label }}</span>
              <span class="qa-value" :class="{ own: line.own }">{{ line.text }}</span>
              <span class="qa-from">{{ line.own ? 'this queue' : 'tenant' }}</span>
            </div>
          </template>
          <div class="qa-group">Readers</div>
          <div v-for="flag in view.flags" :key="flag.key" class="qa-line" :title="flag.help">
            <span class="qa-label">{{ flag.label }}</span>
            <span class="qa-value" :class="{ own: flag.on }">{{ flag.on ? 'Yes' : 'No' }}</span>
            <span class="qa-from">{{ flag.on ? 'this queue' : 'default' }}</span>
          </div>
        </div>
      </section>
    </div>
    <QueueLinesModal :open="editing" :queue="queue" @close="editing = false" />
  </QueuesLayout>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import QueueLinesModal from '@/components/QueueLinesModal.vue'
import QueuesLayout from '@/components/QueuesLayout.vue'
import { QUEUE_TABS, queueOf, queueTabLocation } from '@/composables/navigation'
import { queueAlerts } from '@/composables/queueAlerts'
import { queueLinesInForce } from '@/composables/settingsSide'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useGroupsStore } from '@/stores/groupsStore'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'

const route = useRoute()
const queuesStore = useQueuesStore()
const groupsStore = useGroupsStore()
const { settings, lines, linesFor, state: settingsState, fetchSettings } = useSettingsStore()
const { can } = useIdentity()

const queue = computed(() => queueOf(route))
const loaded = computed(() => queuesStore.lastFetched.value > 0)
const row = computed(() => (queuesStore.queues.value || []).find((q) => q?.name === queue.value) || null)
const alerts = computed(() => queueAlerts(row.value, groupsStore.groups.value || [], linesFor))
const view = computed(() => queueLinesInForce(settings.value.queues.get(queue.value), lines.value))
const tabLabel = (view) => QUEUE_TABS.find((t) => t.view === view)?.label || 'Open'

// Moving a line is the capability that configures a queue, and only on a
// document that was read.
const canEdit = computed(() => can('queueAdmin') && settingsState.value === 'ready')
const editing = ref(false)

const refresh = () => Promise.all([
  queuesStore.fetchQueues(),
  groupsStore.fetchGroups({ ttlMs: 25_000 }),
  fetchSettings(),
])
onMounted(refresh)
useAutoRefresh(refresh)
</script>

<style scoped>
.qa { display: flex; flex-direction: column; gap: 16px; }
.qa-quiet { padding: 14px 16px; font-size: 13px; color: var(--text-low); }
.qa-list { list-style: none; margin: 0; padding: 0; }
.qa-alert {
  display: flex; align-items: center; gap: 12px;
  padding: 12px 16px; border-top: 1px solid var(--bd-soft);
}
.qa-alert:first-child { border-top: 0; }
.qa-mark { flex: none; width: 10px; display: inline-flex; justify-content: center; }
.qa-text { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
.qa-title { font-size: 13px; font-weight: 500; color: var(--text-hi); }
.qa-detail { font-size: 12px; color: var(--text-mid); }
.qa-lines { padding: 4px 16px 12px; }
.qa-group { padding: 12px 0 4px; font-size: 11px; font-weight: 600; color: var(--text-low); }
.qa-line {
  display: grid; grid-template-columns: minmax(0, 1fr) 90px 80px; align-items: baseline; gap: 12px;
  padding: 6px 0; border-top: 1px solid var(--bd-soft); font-size: 13px;
}
.qa-label { color: var(--text-mid); }
.qa-value { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); text-align: right; font-variant-numeric: tabular-nums; }
.qa-value.own { color: var(--text-hi); font-weight: 500; }
.qa-from { font-size: 11px; color: var(--text-low); }
</style>
