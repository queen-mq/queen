<script setup>
// The fixed list on the left: Group by, then the whole tenant and every
// namespace (or task), one line each, by name, the empty group last
// (workloadSide.workloadList). A click picks one: the highlight and the right
// side change, never the list. Grouped by queue, the list still names the
// namespaces: a pick narrows the queues to one of them. A group's mark is the
// worst of its queues by the console's one rule (useAttention queueAttention).
import { computed } from 'vue'
import QcSeg from '../QcSeg.vue'
import { GROUP_BY, emptyName, plural, rootName } from './workloadViews'

const props = defineProps({
  by: { type: String, default: 'namespace' },
  pick: { type: String, default: null },        // the key picked; null is the whole tenant
  list: { type: Object, required: true },       // workloadList()
  listed: { type: Boolean, default: false },    // the queue list has loaded once
  error: { type: String, default: '' },
  search: { type: String, default: '' },
})
const emit = defineEmits(['by', 'pick', 'update:search'])

const listBy = computed(() => (props.by === 'task' ? 'task' : 'namespace'))
const word = computed(() => (listBy.value === 'task' ? 'Tasks' : 'Namespaces'))
const many = computed(() => props.list.groups > 10 || !!props.search)
const TONE = { bad: 'red', warn: 'amber' }
const label = (r) => (r.key ? r.key : emptyName(listBy.value))
const title = (r) => [label(r), plural(r.count, 'queue'), r.why || null].filter(Boolean).join(' · ')
</script>

<template>
  <nav class="wls" :aria-label="`Workload by ${listBy}`">
    <QcSeg :model-value="by" :options="GROUP_BY" label="Group by" class="wls-seg" @update:model-value="emit('by', $event)" />

    <label v-if="many" class="qn-field wls-find">
      <svg width="14" height="14" viewBox="0 0 18 18" aria-hidden="true" style="fill: none; stroke: currentColor; stroke-width: 1.4"><circle cx="7.5" cy="7.5" r="5" /><line x1="11.3" y1="11.3" x2="15.5" y2="15.5" /></svg>
      <span class="qn-vh">Filter the {{ word.toLowerCase() }}, or find one by a queue in it</span>
      <input :value="search" type="text" :placeholder="`Filter the ${word.toLowerCase()}`" spellcheck="false" @input="emit('update:search', $event.target.value)" />
    </label>

    <button
      type="button" class="wls-li" :class="{ on: pick === null }" :aria-current="pick === null ? 'true' : undefined"
      :title="listed ? `The whole tenant: ${plural(list.queues, 'queue')} in ${plural(list.groups, listBy)}` : undefined"
      @click="emit('pick', null)"
    >
      <span class="wls-mark" aria-hidden="true" />
      <span class="wls-name">{{ rootName(by) }}</span>
    </button>

    <p class="wls-h">{{ word }}</p>
    <p v-if="!listed" class="wls-note">{{ error ? `The queue list did not load: ${error}.` : 'Reading the queues…' }}</p>
    <p v-else-if="!list.rows.length && search" class="wls-note">Nothing matches “{{ search }}”. <button type="button" class="qn-link" @click="emit('update:search', '')">Clear</button></p>
    <p v-else-if="!list.rows.length" class="wls-note">None yet.</p>
    <button
      v-for="r in list.rows"
      :key="`k:${r.key}`"
      type="button"
      class="wls-li"
      :class="{ on: pick === r.key }"
      :aria-current="pick === r.key ? 'true' : undefined"
      :title="title(r)"
      @click="emit('pick', r.key)"
    >
      <span class="wls-mark" aria-hidden="true"><span v-if="TONE[r.sev]" class="qn-mark" :class="TONE[r.sev]" /></span>
      <span class="wls-name" :class="r.key ? 'qn-mono' : 'qn-t2'">{{ label(r) }}</span>
    </button>
  </nav>
</template>

<style scoped>
.wls { display: flex; flex-direction: column; min-width: 0; }
.wls-seg { align-self: flex-start; margin-bottom: 14px; }
.wls-find { margin-bottom: 12px; }
.wls-h { font-size: 12px; color: var(--text3); padding: 14px 10px 6px; }
.wls-note { font-size: 12.5px; color: var(--text2); padding: 4px 10px; }
.wls-li { display: flex; align-items: center; gap: 10px; width: 100%; min-height: 34px; padding: 0 10px; border-radius: var(--r-ctl); color: var(--text); text-align: left; }
.wls-li:hover { background: var(--bg2); }
.wls-li.on, .wls-li.on:hover { background: var(--bg3); font-weight: 500; }
.wls-mark { flex: 0 0 8px; display: flex; align-items: center; justify-content: center; }
.wls-name { flex: 1 1 0; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
</style>
