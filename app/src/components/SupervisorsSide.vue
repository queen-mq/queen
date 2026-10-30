<template>
  <!--
    The Supervisors list: every application by name, one line each — a mark
    where the page's verdict asks for one (stale, stopped, needs attention;
    supervisorsSide.applicationList) and how many instances publish under it.
    A click picks the application (?group=) and changes the right side, where
    its pools are; the list itself never changes under it. The search is the
    page's own address field (?search=), so the cards beside it agree.
  -->
  <div class="ss-tools">
    <input
      v-model="searchModel"
      type="search"
      class="input"
      placeholder="Search applications"
      aria-label="Search applications, hosts or pools"
      title="An application, a host or a pool"
    />
  </div>
  <nav class="ss-group" aria-label="Applications">
    <router-link
      :to="at(null)"
      class="ss-row"
      :class="{ on: group === null }"
      :aria-current="group === null ? 'page' : undefined"
    >
      <span class="ss-mark sv-all" aria-hidden="true"><Icon name="menu" :size="15" /></span>
      <span class="ss-text"><span class="ss-name">All applications</span></span>
      <span class="ss-num">{{ readAt ? formatNumber(groups.length) : '' }}</span>
    </router-link>

    <div v-if="!readAt" class="ss-empty">{{ error ? 'Cannot read supervisors' : 'Reading supervisors…' }}</div>
    <div v-else-if="!groups.length" class="ss-empty">No application publishes here</div>
    <div v-else-if="!rows.length" class="ss-empty">No application matches</div>
    <template v-else>
      <router-link
        v-for="a in rows.slice(0, LIMIT)"
        :key="a.name"
        :to="at(a.name)"
        class="ss-row"
        :class="{ on: a.name === group }"
        :aria-current="a.name === group ? 'page' : undefined"
        :title="`${a.name} · ${a.why} · ${a.count} ${a.count === 1 ? 'instance' : 'instances'}`"
      >
        <span class="ss-mark" aria-hidden="true"><span v-if="a.mark" class="g" :class="a.mark" /></span>
        <span class="ss-text"><span class="ss-name">{{ a.name }}</span></span>
        <span class="ss-num">{{ formatNumber(a.count) }}</span>
      </router-link>
      <div v-if="rows.length > LIMIT" class="ss-empty">{{ formatNumber(rows.length - LIMIT) }} more · search to narrow</div>
    </template>
  </nav>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'

import Icon from '@/components/Icon.vue'
import { applicationList } from '@/composables/supervisorsSide'
import { formatNumber } from '@/composables/useApi'

const props = defineProps({
  /** Every application loaded (supervisorGroups), whatever the filters. */
  groups: { type: Array, default: () => [] },
  /** The application picked (?group=), or null for all of them. */
  group: { type: String, default: null },
  /** The page's queue context ('' for none): only the applications serving it. */
  queue: { type: String, default: '' },
  readAt: { type: Number, default: null },
  /** The last read failed. */
  error: { type: Boolean, default: false },
  /** The page's search (?search=): bound, not copied. */
  search: { type: String, default: '' },
})
const emit = defineEmits(['update:search'])

// Rows drawn: a large fleet searches.
const LIMIT = 300

const route = useRoute()
const searchModel = computed({ get: () => props.search, set: (v) => emit('update:search', v) })
const rows = computed(() => applicationList(props.groups, { search: props.search, queue: props.queue }))

// A pick is the address's ?group=; the search and the tools row's filters ride
// along. The page and the open instance belong to the application left.
const at = (name) => ({ query: { ...route.query, group: name ?? undefined, page: undefined, instance: undefined } })
</script>

<style scoped>
/* The "all" row's icon sits in the mark's column, so every name lines up. */
.sv-all { color: var(--text-low); }
</style>
