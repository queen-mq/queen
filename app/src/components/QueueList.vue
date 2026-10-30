<template>
  <!--
    The Queues section's list: every queue by name, a mark only on the ones
    that need you (the same rule as the Overview, useAttention), and the
    pending count. It never changes under a click: picking a queue keeps the
    tab you are on and changes the right side. Reads the shared stores, so it
    adds no request of its own while a page beside it has just read them.
  -->
  <div class="ss-tools">
    <input v-model="find" type="search" class="input" placeholder="Search queues" aria-label="Search queues by name, namespace or task" />
    <!-- ALL is null, never '': '' is the default namespace, a real one. -->
    <NamespacePicker v-if="namespaces.length > 1 || namespace !== null" v-model="namespace" :namespaces="namespaces" />
  </div>
  <div v-if="!loaded" class="ss-empty">{{ queuesStore.error.value ? 'Cannot read the queue list' : 'Reading queues…' }}</div>
  <div v-else-if="!rows.length" class="ss-empty">{{ find || namespace !== null ? 'No queue matches' : 'No queues yet' }}</div>
  <nav v-else class="ss-group" aria-label="Queues">
    <router-link
      v-for="q in rows.slice(0, LIMIT)"
      :key="q.name"
      :to="queueTabLocation(tab, q.name, route, carry)"
      class="ss-row"
      :class="{ on: q.name === selected }"
      :aria-current="q.name === selected ? 'page' : undefined"
      :title="q.why ? `${q.name} · ${q.why}` : q.name"
    >
      <span class="ss-mark" aria-hidden="true"><span v-if="q.sev" class="g" :class="q.sev" /></span>
      <span class="ss-text"><span class="ss-name">{{ q.name }}</span></span>
      <span class="ss-num">{{ q.pending === null ? '—' : q.pending ? formatNumber(q.pending) : '' }}</span>
    </router-link>
    <div v-if="rows.length > LIMIT" class="ss-empty">{{ formatNumber(rows.length - LIMIT) }} more · search to narrow</div>
  </nav>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import NamespacePicker from '@/components/NamespacePicker.vue'
import { inNamespace, namespaceCounts, queueMatches, queueOf, queueTabLocation, queueTabOf } from '@/composables/navigation'
import { formatNumber, toNum } from '@/composables/useApi'
import { queueAttention } from '@/composables/useAttention'
import { useAutoRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'

// Rows drawn: a tenant with thousands of queues searches.
const LIMIT = 300

const route = useRoute()
const queuesStore = useQueuesStore()
const groupsStore = useGroupsStore()
const queueOpsStore = useQueueOpsStore()
const { linesFor } = useSettingsStore()

// In the address (?find=, ?namespace=), which the Queues table reads too, so
// the list and the table beside it show the same queues; carried along on a
// pick, so the list stays as it was filtered.
const find = ref('')
const namespace = ref(null)
useRouteState({ find, namespace })
const carry = computed(() => ({
  find: find.value || undefined,
  namespace: namespace.value === null ? undefined : namespace.value,
}))

const tab = computed(() => queueTabOf(route) || 'overview')
const selected = computed(() => queueOf(route) || null)
const loaded = computed(() => queuesStore.lastFetched.value > 0 || queuesStore.queues.value.length > 0)

// The attention verdict of every queue, by name: the list's marks and the
// namespace picker's (a namespace carries its worst queue's).
const attention = computed(() => new Map(
  queueAttention(queuesStore.queues.value || [], groupsStore.groups.value || [], linesFor, queueOpsStore.ops.value).map((a) => [a.name, a]),
))

// Each namespace with its count and mark; one picked in the address that no
// queue has any more stays in the menu (at 0), so the picker never names a
// namespace it cannot show.
const namespaces = computed(() => {
  const all = namespaceCounts(queuesStore.queues.value || [], (q) => attention.value.get(q.name)?.sev || null)
  if (namespace.value !== null && !all.some((n) => n.name === namespace.value)) all.push({ name: namespace.value, count: 0, sev: null })
  return all
})

const rows = computed(() => {
  const queues = queuesStore.queues.value || []
  return queues
    .filter((q) => q?.name && inNamespace(q, namespace.value) && queueMatches(q, find.value))
    .map((q) => {
      const a = attention.value.get(q.name)
      return {
        name: q.name,
        pending: toNum(q.messages?.pending),
        sev: a?.sev || null,
        why: a ? (a.reason === 'lag' ? 'a consumer group is behind' : 'no consumer group reads it') : '',
      }
    })
    // By name only: a list that reorders itself on a refresh moves rows
    // under the pointer. The marks say which ones need you.
    .sort((a, b) => a.name.localeCompare(b.name))
})

const refresh = () => Promise.all([queuesStore.fetchQueues(), groupsStore.fetchGroups({ ttlMs: 25_000 })])
onMounted(refresh)
useAutoRefresh(refresh)
</script>
