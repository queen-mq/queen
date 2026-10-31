<template>
  <!--
    Access: who may use the acting cluster, people and services alike. One
    fixed list (components/AccessSide.vue): People, every member and the roles
    they have; API keys, every key, active and revoked. A pick opens that
    table, filtered, and nothing else moves. Both lists are read together
    (stores/accessStore), so the list counts both whichever table is open.
    Tenant-level, a cluster admin's: the proxy owns these identities
    (proxy/src/console.rs), so a broker-direct build has nothing to show.
  -->
  <SubSidebar label="Access" :current="current">
    <template #side><AccessSide /></template>
    <AccessKeys v-if="view === 'keys'" />
    <AccessMembers v-else />
  </SubSidebar>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'

import AccessKeys from '@/components/AccessKeys.vue'
import AccessMembers from '@/components/AccessMembers.vue'
import AccessSide from '@/components/AccessSide.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import { keyPickTitle, memberPickTitle } from '@/composables/accessGroups'
import { useRefresh } from '@/composables/useRefresh'
import { useRouteState } from '@/composables/useRouteState'
import { useAccessStore } from '@/stores/accessStore'
import { useIdentity } from '@/stores/identity'

// The pick, in the address: ?view=keys opens the keys (people otherwise),
// ?role= and ?state= filter them.
const view = ref('')
const role = ref('')
const state = ref('')
useRouteState({ view, role, state })
// The phone's bar names the table open and its filter.
const current = computed(() => (view.value === 'keys' ? keyPickTitle(state.value) : memberPickTitle(role.value)))

const { loadAll } = useAccessStore()
const { epoch } = useIdentity()
onMounted(loadAll)
useRefresh(loadAll)
// A cluster switch is two different lists (the store emptied them).
watch(epoch, () => { loadAll() })
</script>
