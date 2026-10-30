<template>
  <!--
    The Locks list: a filter list. "All held locks", then every lock name
    prefix (`job:`, `sync:`) by name, each with the permits held under it.
    A click filters the table on the right (?prefix=) and moves the highlight;
    the rows themselves only change on a refresh. The page reads the permits
    and passes their entries in (composables/useLocksSide.js says how).
  -->
  <div class="ss-tools">
    <input v-model="find" type="search" class="input" placeholder="Search lock names" aria-label="Search lock name prefixes" spellcheck="false" />
  </div>
  <div v-if="unavailable" class="ss-empty" :title="unavailable">Not available</div>
  <nav v-else class="ss-group" aria-label="Held locks by name">
    <router-link
      :to="location('')"
      class="ss-row"
      :class="{ on: !prefix }"
      :aria-current="!prefix ? 'page' : undefined"
      :title="totals ? `Every held permit: ${describeHeld(totals) || 'none'}` : 'Every held permit'"
    >
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">All held locks</span></span>
      <span class="ss-num">{{ totals ? `${exact(totals.permits)}${more ? '+' : ''}` : '—' }}</span>
    </router-link>
    <router-link
      v-for="e in shown.slice(0, SIDE_LIMIT)"
      :key="e.prefix"
      :to="location(e.prefix)"
      class="ss-row"
      :class="{ on: prefix === e.prefix }"
      :aria-current="prefix === e.prefix ? 'page' : undefined"
      :title="`${e.prefix} · ${describeHeld(e)}`"
    >
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">{{ e.prefix }}</span></span>
      <span class="ss-num">{{ exact(e.permits) }}</span>
    </router-link>
    <div v-if="shown.length > SIDE_LIMIT" class="ss-empty">{{ exact(shown.length - SIDE_LIMIT) }} more · search to narrow</div>
  </nav>
  <template v-if="!unavailable">
    <div v-if="!entries && errorText" class="ss-empty" :title="errorText">Cannot read locks</div>
    <div v-else-if="!entries" class="ss-empty">Reading locks…</div>
    <div v-else-if="!entries.length" class="ss-empty">No lock is held</div>
    <div v-else-if="!shown.length" class="ss-empty">No name matches</div>
  </template>
</template>

<script setup>
import { computed, ref } from 'vue'
import { useRoute } from 'vue-router'

import { SIDE_LIMIT, exact } from '@/composables/useKvSide'
import { describeHeld, filterEntries, lockTotals } from '@/composables/useLocksSide'

const props = defineProps({
  /** useLocksSide.lockPrefixes() over every held permit, or null until read. */
  entries: { type: Array, default: null },
  /** More permits are held than the list read: its counts are a floor. */
  more: { type: Boolean, default: false },
  /** The filter on the right ('' for every held lock). */
  prefix: { type: String, default: '' },
  /** The read failed with nothing to show: why. */
  errorText: { type: String, default: '' },
  /** A stable verdict (not here / not in the plan / not served): its title. */
  unavailable: { type: String, default: '' },
})

const route = useRoute()

// Local: narrowing the list is not a filter of the table, and an address that
// changed per keystroke would close the list's sheet on a phone.
const find = ref('')
const shown = computed(() => filterEntries(props.entries, find.value))
const totals = computed(() => (props.entries ? lockTotals(props.entries) : null))

const location = (prefix) => ({ path: route.path, query: { ...route.query, prefix: prefix || undefined } })
</script>
