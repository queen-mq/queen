<template>
  <!--
    API keys' list: every key, then the keys by state, each with its count.
    The proxy answers revoked_at and no expiry, so a key is active or
    revoked. A filter, fixed: a click shows only that state on the page and
    moves the highlight. The pick lives in the address (?state=), which the
    page reads too, and the rows count the list the page has already loaded:
    no request of its own.
  -->
  <div class="ss-group acc-side" role="group" aria-label="API keys by state">
    <button type="button" class="ss-row" :class="{ on: !pick }" :aria-pressed="!pick" @click="state = ''">
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">All keys</span></span>
      <span class="ss-num">{{ loaded ? formatNumber(groups.all) : '—' }}</span>
    </button>
    <button
      v-for="s in groups.states"
      :key="s.state"
      type="button"
      class="ss-row"
      :class="{ on: pick === s.state }"
      :aria-pressed="pick === s.state"
      :title="TITLES[s.state]"
      @click="state = s.state"
    >
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">{{ s.name }}</span></span>
      <span class="ss-num">{{ loaded ? formatNumber(s.count) : '—' }}</span>
    </button>
  </div>
  <div v-if="!loaded" class="ss-empty">{{ error ? 'Cannot read API keys' : 'Reading API keys…' }}</div>
</template>

<script setup>
import { computed, ref } from 'vue'

import { keyGroups, pickKeyState } from '@/composables/accessGroups'
import { formatNumber } from '@/composables/useApi'
import { useRouteState } from '@/composables/useRouteState'

const props = defineProps({
  /** The list the page loaded (proxy list_keys). */
  keys: { type: Array, default: () => [] },
  loaded: { type: Boolean, default: false },
  /** The last load's failure, any shape; only its presence matters here. */
  error: { type: null, default: null },
})

const TITLES = {
  active: 'Keys that work: a key has no expiry date and works until it is revoked',
  revoked: 'Keys that no longer work: a call with one fails',
}

// Under the name the page reads, so a click here filters the table there.
const state = ref('')
useRouteState({ state })
// A state the payload cannot have (?state=expired) reads as all keys.
const pick = computed(() => pickKeyState(state.value))

const groups = computed(() => keyGroups(props.keys))
</script>

<style scoped>
.acc-side { padding-top: 12px; }
</style>
