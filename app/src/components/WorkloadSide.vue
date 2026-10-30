<template>
  <!--
    The Workload list: the whole tenant, then every namespace — or every
    task, by Group by — one line each: the worst queueAttention mark of its
    queues and their pending messages (workloadSide.workloadList). A click is
    the page's own drill-down (the page's functions, so it refetches exactly
    as a chart click does); its queues are on the right, never listed here.
  -->
  <div class="ss-tools">
    <input
      v-model="search"
      type="search"
      class="input"
      :placeholder="`Search ${plural}`"
      :aria-label="`Search ${plural} or their queues`"
      :title="`A ${groupBy === 'task' ? 'task' : 'namespace'}, or a queue in one`"
    />
    <UiSelect
      :model-value="groupBy"
      label="Group by"
      :options="GROUP_BY"
      aria-label="Group the list by"
      block
      @update:model-value="emit('group-by', $event)"
    />
  </div>
  <nav class="ss-group" :aria-label="`Workload by ${groupBy}`">
    <button
      type="button"
      class="ss-row"
      :class="{ on: list.root.on }"
      :aria-current="list.root.on ? 'true' : undefined"
      :title="loaded ? `The whole tenant · ${formatNumber(list.queues)} ${list.queues === 1 ? 'queue' : 'queues'} in ${formatNumber(list.groups)} ${list.groups === 1 ? groupBy : plural}` : undefined"
      @click="emit('root')"
    >
      <span class="ss-mark wl-all" aria-hidden="true"><Icon name="menu" :size="15" /></span>
      <span class="ss-text"><span class="ss-name">{{ list.root.label }}</span></span>
      <span class="ss-num">{{ loaded ? pending(list.root.pending) : '' }}</span>
    </button>

    <div v-if="!loaded" class="ss-empty">{{ queuesStore.error.value ? 'Cannot read the queue list' : 'Reading queues…' }}</div>
    <div v-else-if="!list.rows.length" class="ss-empty">{{ search.trim() ? 'Nothing matches' : `No ${plural} yet` }}</div>
    <template v-else>
      <button
        v-for="n in list.rows.slice(0, LIMIT)"
        :key="`n:${n.key}`"
        type="button"
        class="ss-row"
        :class="{ on: n.on }"
        :aria-current="n.on ? 'true' : undefined"
        :title="rowTitle(n)"
        @click="emit('focus', n.key)"
      >
        <span class="ss-mark" aria-hidden="true"><span v-if="n.sev" class="g" :class="n.sev" /></span>
        <span class="ss-text"><span class="ss-name">{{ n.label }}</span></span>
        <span class="ss-num">{{ pending(n.pending) }}</span>
      </button>
      <div v-if="list.rows.length > LIMIT" class="ss-empty">{{ formatNumber(list.rows.length - LIMIT) }} more · search to narrow</div>
    </template>
  </nav>
</template>

<script setup>
import { computed, ref } from 'vue'

import Icon from '@/components/Icon.vue'
import UiSelect from '@/components/UiSelect.vue'
import { formatNumber } from '@/composables/useApi'
import { GROUP_BY, workloadList } from '@/composables/workloadSide'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'

const props = defineProps({
  groupBy: { type: String, default: 'namespace' },
  /** The namespace (or task) the page is drilled into; null at the root, '' is the empty one. */
  focus: { type: String, default: null },
  /** Keys the page's workload rows carry, besides the queue list's. */
  keys: { type: Array, default: () => [] },
  /** The page's consumer-group listing; null while it has none to judge by. */
  groups: { type: Array, default: null },
})
const emit = defineEmits(['group-by', 'root', 'focus'])

// Rows drawn: a tenant with thousands of namespaces searches.
const LIMIT = 300

const queuesStore = useQueuesStore()
const { linesFor } = useSettingsStore()
// Not in the address: the page keeps what it shows (?focus=, ?groupBy=).
const search = ref('')

const plural = computed(() => (props.groupBy === 'task' ? 'tasks' : 'namespaces'))
const loaded = computed(() => queuesStore.lastFetched.value > 0 || queuesStore.queues.value.length > 0)
const list = computed(() => workloadList({
  queues: queuesStore.queues.value || [],
  groups: props.groups,
  groupBy: props.groupBy,
  focus: props.focus,
  keys: props.keys,
  search: search.value,
  linesFor,
}))

const pending = (n) => (n === null ? '—' : formatNumber(n))
const rowTitle = (n) => [
  n.label,
  `${formatNumber(n.count)} ${n.count === 1 ? 'queue' : 'queues'}`,
  n.pending === null ? null : `${formatNumber(n.pending)} pending`,
  n.why || null,
].filter(Boolean).join(' · ')
</script>

<style scoped>
/* The "all" row's icon sits in the mark's column, so every name lines up. */
.wl-all { color: var(--text-low); }
</style>
