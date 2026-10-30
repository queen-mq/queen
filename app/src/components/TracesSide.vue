<template>
  <!--
    The Traces list: the tenant's trace names by name, each with how many
    events carry it, and the search box. Typing narrows the list; Enter opens
    the typed name, listed or not. A click loads that name on the right. The
    names come from the page's read (Traces.vue), so this adds no request.
  -->
  <form class="ss-tools" role="search" @submit.prevent="search(typed)">
    <input
      v-model="term"
      type="search"
      class="input"
      placeholder="Search trace names"
      aria-label="Trace name"
      title="Narrows the list; Enter opens the exact name"
      spellcheck="false"
      autocomplete="off"
    />
  </form>

  <nav class="ss-group" aria-label="Trace names">
    <!-- One message's events, opened from Messages. -->
    <span v-if="message" class="ss-row on" aria-current="page" :title="`Every event of message ${message.transactionId}`">
      <span class="ss-mark tr-icon" aria-hidden="true"><Icon name="message-square-text" :size="15" /></span>
      <span class="ss-text"><span class="ss-name tr-mono">{{ message.transactionId }}</span></span>
    </span>
    <!-- The name open on the right when the list does not hold it. -->
    <router-link v-if="list.open" :to="at(list.open)" class="ss-row on" aria-current="page" :title="`${list.open} · not in the list`">
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">{{ list.open }}</span></span>
    </router-link>
    <router-link v-if="list.ask" :to="at(list.ask)" class="ss-row" :title="`Open the events named ${list.ask}`">
      <span class="ss-mark tr-icon" aria-hidden="true"><Icon name="search" :size="15" /></span>
      <span class="ss-text"><span class="ss-name">Open “{{ list.ask }}”</span></span>
    </router-link>

    <div v-if="!loaded" class="ss-empty">Reading trace names…</div>
    <div v-else-if="failed && !rows.length" class="ss-empty" title="The broker did not answer the list of names. A name typed above still opens.">Cannot read trace names</div>
    <div v-else-if="!rows.length" class="ss-empty">No trace names yet</div>
    <div v-else-if="!list.rows.length" class="ss-empty">No listed name matches</div>
    <template v-else>
      <router-link
        v-for="r in list.rows.slice(0, LIMIT)"
        :key="r.name"
        :to="at(r.name)"
        class="ss-row"
        :class="{ on: r.name === current }"
        :aria-current="r.name === current ? 'page' : undefined"
        :title="rowTitle(r)"
      >
        <span class="ss-mark" aria-hidden="true" />
        <span class="ss-text"><span class="ss-name">{{ r.name }}</span></span>
        <span class="ss-num">{{ r.traces === null ? '—' : formatNumber(r.traces) }}</span>
      </router-link>
      <div v-if="hidden > 0" class="ss-empty" title="The broker lists the first names only: type a full name and press Enter to open one past them">
        {{ formatNumber(hidden) }} more · type a full name
      </div>
    </template>
  </nav>
</template>

<script setup>
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import Icon from '@/components/Icon.vue'
import { contextQuery } from '@/composables/navigation'
import { traceNameList } from '@/composables/tracesSide'
import { formatNumber } from '@/composables/useApi'
import { formatTimestamp } from '@/composables/useFormat'

const props = defineProps({
  /** readTraceNames(...).rows: `{ name, traces, messages, lastSeen }`. */
  rows: { type: Array, default: () => [] },
  /** Every name the tenant has; more than `rows` when the list was cut. */
  total: { type: Number, default: 0 },
  /** The names were read (or the read failed) at least once. */
  loaded: { type: Boolean, default: false },
  /** The last read failed. */
  failed: { type: Boolean, default: false },
  /** The trace name open on the right, '' for none. */
  current: { type: String, default: '' },
  /** `{ partitionId, transactionId }` when the page shows one message's events. */
  message: { type: Object, default: null },
})

// Rows drawn; the broker's own cut (TRACE_NAMES_LIMIT) is below it.
const LIMIT = 300

const route = useRoute()
const router = useRouter()

// Not in the address: the page keeps only what it shows (?trace=).
const term = ref('')
const typed = computed(() => term.value.trim())
const list = computed(() => traceNameList(props.rows, { term: term.value, current: props.current }))
const hidden = computed(() => Math.max(0, props.total - props.rows.length) + Math.max(0, list.value.rows.length - LIMIT))

// The name, and the address's context only.
const at = (name) => ({ path: '/traces', query: { ...contextQuery(route), trace: name } })
const search = (name) => {
  if (name) router.push(at(name))
}

const rowTitle = (r) => [
  r.name,
  r.traces === null ? null : `${formatNumber(r.traces)} ${r.traces === 1 ? 'event' : 'events'}`,
  r.messages === null ? null : `${formatNumber(r.messages)} ${r.messages === 1 ? 'message' : 'messages'}`,
  r.lastSeen === null ? null : `last seen ${formatTimestamp(r.lastSeen)}`,
].filter(Boolean).join(' · ')
</script>

<style scoped>
/* An icon in the mark's column, so every name lines up. */
.tr-icon { color: var(--text-low); }
.tr-mono { font-family: var(--font-mono); font-size: 12px; }
</style>
