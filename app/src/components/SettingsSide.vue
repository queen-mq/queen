<template>
  <!--
    Settings' list: the tenant's lines first, then every queue that has lines
    of its own, by name. Fixed: a click opens that section on the right
    (?for=<queue>, nothing = the lines) and moves the highlight, nothing else;
    a queue is added from the page's header. A search box once the queues no
    longer fit at a glance. Everything comes from the page's reads
    (Settings.vue: the settings document and the queue list).
  -->
  <div v-if="searchable" class="ss-tools">
    <input v-model="find" type="search" class="input" placeholder="Search queues" aria-label="Search the queues with lines of their own" />
  </div>
  <nav class="ss-group" :class="{ 'set-side-top': !searchable }" aria-label="Settings">
    <router-link
      :to="at(null)"
      class="ss-row"
      :class="{ on: !selected }"
      :aria-current="!selected ? 'page' : undefined"
      title="The lines every queue of this tenant is judged by"
    >
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">Lines</span></span>
    </router-link>
  </nav>
  <nav class="ss-group" aria-label="Queues with lines of their own">
    <div class="ss-label">Queues</div>
    <router-link
      v-for="q in shown"
      :key="q.name"
      :to="at(q.name)"
      class="ss-row"
      :class="{ on: q.name === selected }"
      :aria-current="q.name === selected ? 'page' : undefined"
      :title="rowTitle(q)"
    >
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">{{ q.name }}</span></span>
    </router-link>
  </nav>
  <div v-if="state === 'unread'" class="ss-empty">Reading…</div>
  <div v-else-if="!queues.length" class="ss-empty" :title="noneTitle">{{ noneText }}</div>
  <div v-else-if="!shown.length" class="ss-empty">No queue matches</div>
</template>

<script setup>
import { computed, ref } from 'vue'
import { useRoute } from 'vue-router'

import { matchEntries } from '@/composables/settingsSide'

const props = defineProps({
  /** The queue whose section is open; null on the lines. */
  selected: { type: String, default: null },
  /** ownQueueEntries(...): `{ name, summary, exists }`, by name. */
  queues: { type: Array, default: () => [] },
  /** The settings store's state: unread | ready | off | error. */
  state: { type: String, default: 'unread' },
})

// A search box from the length the namespace menu takes one (NamespacePicker).
const SEARCH_FROM = 9

const route = useRoute()

// Unrelated query parameters survive a section change.
const at = (name) => ({ path: route.path, query: { ...route.query, for: name || undefined } })

const find = ref('')
const searchable = computed(() => props.queues.length >= SEARCH_FROM)
const shown = computed(() => (searchable.value ? matchEntries(props.queues, find.value) : props.queues))

const rowTitle = (q) => [
  q.exists ? q.name : `${q.name} · not created yet`,
  q.summary.join(' · '),
].filter(Boolean).join('\n')

const noneText = computed(() => ({ off: 'Not on this cluster', error: 'Cannot read them' }[props.state] || 'None yet'))
const noneTitle = computed(() => ({
  off: 'Queues keep their own lines in KV, which this cluster cannot use',
  error: 'The settings could not be read, so the queues with lines of their own are not known',
}[props.state] || 'Every queue is judged by the tenant’s lines. Add a queue from the header to give it its own.'))
</script>

<style scoped>
/* With no search box above it, the first row starts where the box would. */
.set-side-top { padding-top: 12px; }
</style>
