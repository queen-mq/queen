<template>
  <!--
    The Ephemeral list: every ephemeral queue on this cell, fixed and by name,
    with its depth (messages in broker RAM). A click shows that queue on the
    right (?name=) and moves the highlight; the order never follows the depth,
    so the page's 2 s poll cannot move rows under the pointer. Reads the
    shared store the page polls: no request of its own. The search narrows
    this list and the page's table alike.
  -->
  <div class="ss-tools">
    <input
      :value="find"
      type="search"
      class="input"
      placeholder="Search ephemeral queues"
      aria-label="Search ephemeral queues"
      spellcheck="false"
      @input="emit('update:find', $event.target.value)"
    />
  </div>
  <div v-if="support === 'absent' || support === 'gated'" class="ss-empty">Not available</div>
  <div v-else-if="!loaded" class="ss-empty" :title="error ? describeApiError(error) : ''">{{ error ? 'Cannot read ephemeral queues' : 'Reading queues…' }}</div>
  <div v-else-if="!rows.length" class="ss-empty">{{ find.trim() ? 'No queue matches' : 'No ephemeral queues yet' }}</div>
  <nav v-else class="ss-group" aria-label="Ephemeral queues">
    <router-link
      v-for="q in rows.slice(0, LIMIT)"
      :key="q.name"
      :to="location(q.name)"
      class="ss-row"
      :class="{ on: q.name === selected }"
      :aria-current="q.name === selected ? 'page' : undefined"
      :title="rowTitle(q)"
    >
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">{{ q.name }}</span></span>
      <span class="ss-num">{{ q.depth === null ? '—' : q.depth ? formatNumber(q.depth) : '' }}</span>
    </router-link>
    <div v-if="rows.length > LIMIT" class="ss-empty">{{ formatNumber(rows.length - LIMIT) }} more · search to narrow</div>
  </nav>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'

import { describeApiError } from '@/api'
import { formatBytes, formatNumber } from '@/composables/useApi'
import { dropTotal, ephemeralRows } from '@/composables/useEphemeralSide'
import { useEphemeralStore } from '@/stores/ephemeralStore'

// Rows drawn: a tenant can hold thousands of request/reply inboxes.
const LIMIT = 300

const props = defineProps({
  /** The page's search, shared with its table (v-model:find). */
  find: { type: String, default: '' },
})
const emit = defineEmits(['update:find'])

const route = useRoute()
const { queues, error, lastFetched, support } = useEphemeralStore()

const selected = computed(() => (typeof route.query.name === 'string' && route.query.name) || null)
const location = (name) => ({ path: route.path, query: { ...route.query, name } })

const loaded = computed(() => lastFetched.value > 0 || queues.value.length > 0)
const rows = computed(() => ephemeralRows(queues.value, { search: props.find }))

const rowTitle = (q) => {
  const parts = [q.name]
  parts.push(q.depth === null ? 'depth not reported' : `${formatNumber(q.depth)} ${q.depth === 1 ? 'message' : 'messages'} in RAM`)
  if (q.bytes !== null) parts.push(formatBytes(q.bytes))
  const dropped = dropTotal(q.drops)
  if (dropped) parts.push(`${formatNumber(dropped)} dropped`)
  return parts.join(' · ')
}
</script>
