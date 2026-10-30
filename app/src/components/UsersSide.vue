<template>
  <!--
    Users' list (operator, whole cell): the search, every user, then every
    tenant on the cell and every role held on its clusters, each with its
    count. A filter, fixed: one pick at a time, so a count is what a click
    shows. The search and the pick live in the address (?search=, ?tenant=,
    ?role=), which the page reads too, and the rows count the list the page
    has already loaded: no request of its own.
  -->
  <div class="ss-tools">
    <input v-model="search" type="search" class="input" placeholder="Search users" aria-label="Search users by name, email or tenant" />
  </div>
  <div class="ss-group" role="group" aria-label="Users">
    <button type="button" class="ss-row" :class="{ on: !tenant && !role }" :aria-pressed="!tenant && !role" @click="pickAll">
      <span class="ss-mark" aria-hidden="true" />
      <span class="ss-text"><span class="ss-name">All users</span></span>
      <span class="ss-num">{{ loaded ? formatNumber(groups.all) : '—' }}</span>
    </button>
  </div>
  <div v-if="!loaded" class="ss-empty">{{ error ? 'Cannot read users' : 'Reading users…' }}</div>
  <template v-else>
    <div v-if="groups.tenants.length" class="ss-group" role="group" aria-label="Users by tenant">
      <div class="ss-label">Tenants</div>
      <button
        v-for="t in groups.tenants"
        :key="t.slug"
        type="button"
        class="ss-row"
        :class="{ on: tenant === t.slug }"
        :aria-pressed="tenant === t.slug"
        :title="t.name ? `${t.slug} · ${t.name}` : t.slug"
        @click="pickTenant(t.slug)"
      >
        <span class="ss-mark" aria-hidden="true" />
        <span class="ss-text"><span class="ss-name">{{ t.slug }}</span></span>
        <span class="ss-num">{{ formatNumber(t.count) }}</span>
      </button>
    </div>
    <div v-if="groups.roles.length" class="ss-group" role="group" aria-label="Users by role on this cell">
      <div class="ss-label" title="Roles are per cluster: a user with two roles counts under both">Roles</div>
      <button
        v-for="r in groups.roles"
        :key="r.role"
        type="button"
        class="ss-row"
        :class="{ on: role === r.role }"
        :aria-pressed="role === r.role"
        :title="r.role === NO_ACCESS ? 'Users with no role on any cluster of this cell' : `Users with the ${r.role} role on at least one cluster of this cell`"
        @click="pickRole(r.role)"
      >
        <span class="ss-mark" aria-hidden="true" />
        <span class="ss-text"><span class="ss-name">{{ r.role === NO_ACCESS ? r.name : roleLabel(r.role) }}</span></span>
        <span class="ss-num">{{ formatNumber(r.count) }}</span>
      </button>
    </div>
  </template>
</template>

<script setup>
import { computed, ref } from 'vue'

import { NO_ACCESS, roleLabel, userGroups } from '@/composables/accessGroups'
import { formatNumber } from '@/composables/useApi'
import { useRouteState } from '@/composables/useRouteState'

const props = defineProps({
  /** The lists the page loaded (proxy operator list_users). */
  users: { type: Array, default: () => [] },
  tenants: { type: Array, default: () => [] },
  loaded: { type: Boolean, default: false },
  /** The last load's failure, any shape; only its presence matters here. */
  error: { type: null, default: null },
})

// Under the names the page reads, so the search and a click here filter the
// table there. A tenant is kept by its slug: readable in the address.
const search = ref('')
const tenant = ref('')
const role = ref('')
useRouteState({ search, tenant, role })

const pickAll = () => { tenant.value = ''; role.value = '' }
const pickTenant = (slug) => { tenant.value = slug; role.value = '' }
const pickRole = (name) => { role.value = name; tenant.value = '' }

const groups = computed(() =>
  userGroups(props.users, props.tenants, { search: search.value, tenant: tenant.value, role: role.value }))
</script>
