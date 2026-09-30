<template>
  <!--
    Members and Users are the same people at two scopes: who may use the
    acting cluster, and every account on the cell. A live operator is an admin
    on every cluster, so both pages are always theirs; they share one nav row
    (Members) and switch here. Nobody else sees the switch, because Users is an
    operator page.
  -->
  <div v-if="can('operator')" class="seg" role="group" aria-label="Whose accounts">
    <button :class="{ on: route.path === '/members' }" @click="go('/members')">This cluster</button>
    <button :class="{ on: route.path === '/users' }" @click="go('/users')">Every tenant</button>
  </div>
</template>

<script setup>
import { useRoute, useRouter } from 'vue-router'

import { useIdentity } from '@/stores/identity'

const { can } = useIdentity()
const route = useRoute()
const router = useRouter()
const go = (path) => { if (route.path !== path) router.push(path) }
</script>
