<template>
  <!--
    Members' list: every member, then the roles this cluster's members really
    have, each with its count. A filter, fixed: a click shows only that role
    on the page and moves the highlight. The pick lives in the address
    (?role=), which the page reads too, and the rows count the list the page
    has already loaded: no request of its own.
  -->
  <div class="ss-group acc-side" role="group" aria-label="Members by role">
    <button type="button" class="ss-row" :class="{ on: !role }" :aria-pressed="!role" @click="role = ''">
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">All members</span></span>
      <span class="ss-num">{{ loaded ? formatNumber(groups.all) : '—' }}</span>
    </button>
    <button
      v-for="r in groups.roles"
      :key="r.role"
      type="button"
      class="ss-row"
      :class="{ on: role === r.role }"
      :aria-pressed="role === r.role"
      :title="roleCan(r.role) ? `${roleLabel(r.role)} · ${roleCan(r.role)}` : roleLabel(r.role)"
      @click="role = r.role"
    >
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">{{ roleLabel(r.role) }}</span></span>
      <span class="ss-num">{{ formatNumber(r.count) }}</span>
    </button>
  </div>
  <div v-if="!loaded" class="ss-empty">{{ error ? 'Cannot read members' : 'Reading members…' }}</div>
</template>

<script setup>
import { computed, ref } from 'vue'

import { memberGroups, roleCan, roleLabel } from '@/composables/accessGroups'
import { formatNumber } from '@/composables/useApi'
import { useRouteState } from '@/composables/useRouteState'

const props = defineProps({
  /** The list the page loaded (proxy list_members). */
  members: { type: Array, default: () => [] },
  loaded: { type: Boolean, default: false },
  /** The last load's failure, any shape; only its presence matters here. */
  error: { type: null, default: null },
})

// Under the name the page reads, so a click here filters the table there.
const role = ref('')
useRouteState({ role })

const groups = computed(() => memberGroups(props.members, role.value))
</script>

<style scoped>
.acc-side { padding-top: 12px; }
</style>
