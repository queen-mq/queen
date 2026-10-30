<template>
  <!--
    The KV list: the tenant's namespaces, fixed and by name, each with its
    exact key count. A click opens the namespace on the right (?ns=) and moves
    the highlight; the key prefix is a filter on the right, not a level here.
    Reads the namespace listing the page has already read: no request of its
    own. The search only narrows the list; Enter opens a name typed, listed or
    not (the way in when the listing cannot be read).
  -->
  <div class="ss-tools">
    <input
      v-model="find"
      type="search"
      class="input"
      placeholder="Search namespaces"
      aria-label="Search namespaces, or type one and press Enter to open it"
      title="Enter opens the namespace typed, listed or not"
      spellcheck="false"
      @keyup.enter="openTyped"
    />
  </div>
  <div v-if="unavailable" class="ss-empty" :title="unavailable">Not available</div>
  <div v-else-if="!rows.length && !settled" class="ss-empty">Reading namespaces…</div>
  <div v-else-if="!rows.length && errorText" class="ss-empty" :title="`${errorText}. Type a namespace above and press Enter to open it.`">Cannot read namespaces</div>
  <div v-else-if="!rows.length" class="ss-empty" :title="find.trim() ? `Enter opens “${find.trim()}”` : ''">{{ find.trim() ? 'No namespace matches' : 'No namespaces yet' }}</div>
  <nav v-else class="ss-group" aria-label="KV namespaces">
    <router-link
      v-for="r in rows.slice(0, SIDE_LIMIT)"
      :key="r.namespace"
      :to="location(r.namespace)"
      class="ss-row"
      :class="{ on: r.namespace === namespace }"
      :aria-current="r.namespace === namespace ? 'page' : undefined"
      :title="rowTitle(r)"
    >
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">{{ r.namespace }}</span></span>
      <span class="ss-num">{{ exact(r.keys) }}</span>
    </router-link>
    <div v-if="rows.length > SIDE_LIMIT" class="ss-empty">{{ exact(rows.length - SIDE_LIMIT) }} more · search to narrow</div>
  </nav>
</template>

<script setup>
import { computed, ref } from 'vue'
import { useRoute } from 'vue-router'

import { SIDE_LIMIT, exact, namespaceRows } from '@/composables/useKvSide'

const props = defineProps({
  /** The namespace open on the right ('' for none). */
  namespace: { type: String, default: '' },
  /** useKvView.namespaceOptions(): `{namespace, keys}`. */
  options: { type: Array, default: () => [] },
  /** The listing has answered or failed: before that an unknown name is not "unlisted". */
  settled: { type: Boolean, default: false },
  /** The listing failed and left nothing behind: why. */
  errorText: { type: String, default: '' },
  /** A stable verdict (not here / not in the plan / not served): its title. */
  unavailable: { type: String, default: '' },
})
const emit = defineEmits(['open'])

const route = useRoute()

// Local, never in the address: a word typed to narrow the list is not a
// namespace until Enter, and an address that changed per keystroke would
// close the list's sheet on a phone.
const find = ref('')
const rows = computed(() => namespaceRows(props.options, { picked: props.namespace, find: find.value, settled: props.settled }))

const location = (ns) => ({ path: route.path, query: { ...route.query, ns } })
const rowTitle = (r) => (r.listed
  ? `${r.namespace} · ${r.keys === null ? 'count not reported' : `${exact(r.keys)} ${r.keys === 1 ? 'key' : 'keys'}`}, expired keys awaiting sweep included`
  : `${r.namespace} · not in the namespace list; the page asks the broker anyway`)

const openTyped = () => {
  const typed = find.value.trim()
  if (typed && typed !== props.namespace) emit('open', typed)
}
</script>
