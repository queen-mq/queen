<template>
  <!--
    Access's list: who may use the acting cluster, in two groups that never
    move. People: every member, then the roles they really have; API keys:
    every key, then active and revoked. A click opens that table filtered to
    the row and moves the highlight; nothing else changes. The pick lives in
    the address (?view=keys, ?role=, ?state=), which the page reads too, and
    the counts are the lists the page has already loaded.
  -->
  <div class="ss-group acc-side" role="group" aria-label="People by role">
    <div class="ss-label">People</div>
    <button type="button" class="ss-row" :class="{ on: view !== 'keys' && !role }" :aria-pressed="view !== 'keys' && !role" @click="pickPeople('')">
      <Icon name="users" :size="16" class="ss-icon" />
      <span class="ss-text"><span class="ss-name">All members</span></span>
      <span class="ss-num">{{ membersLoaded ? formatNumber(peopleGroups.all) : membersError ? '!' : '—' }}</span>
    </button>
    <button
      v-for="r in peopleGroups.roles"
      :key="r.role"
      type="button"
      class="ss-row acc-sub"
      :class="{ on: view !== 'keys' && role === r.role }"
      :aria-pressed="view !== 'keys' && role === r.role"
      :title="roleCan(r.role) ? `${roleLabel(r.role)} · ${roleCan(r.role)}` : roleLabel(r.role)"
      @click="pickPeople(r.role)"
    >
      <span class="ss-text"><span class="ss-name">{{ roleLabel(r.role) }}</span></span>
      <span class="ss-num">{{ formatNumber(r.count) }}</span>
    </button>
  </div>

  <div class="ss-group" role="group" aria-label="API keys by state">
    <div class="ss-label">API keys</div>
    <button type="button" class="ss-row" :class="{ on: view === 'keys' && !keyPick }" :aria-pressed="view === 'keys' && !keyPick" @click="pickKeys('')">
      <Icon name="id-card" :size="16" class="ss-icon" />
      <span class="ss-text"><span class="ss-name">All keys</span></span>
      <span class="ss-num">{{ keysLoaded ? formatNumber(keyGroupsNow.all) : keysError ? '!' : '—' }}</span>
    </button>
    <button
      v-for="s in keyGroupsNow.states"
      :key="s.state"
      type="button"
      class="ss-row acc-sub"
      :class="{ on: view === 'keys' && keyPick === s.state }"
      :aria-pressed="view === 'keys' && keyPick === s.state"
      :title="KEY_TITLES[s.state]"
      @click="pickKeys(s.state)"
    >
      <span class="ss-text"><span class="ss-name">{{ s.name }}</span></span>
      <span class="ss-num">{{ keysLoaded ? formatNumber(s.count) : '—' }}</span>
    </button>
  </div>
</template>

<script setup>
import { computed, ref } from 'vue'

import Icon from '@/components/Icon.vue'
import { keyGroups, memberGroups, pickKeyState, roleCan, roleLabel } from '@/composables/accessGroups'
import { formatNumber } from '@/composables/useApi'
import { useRouteState } from '@/composables/useRouteState'
import { useAccessStore } from '@/stores/accessStore'

const { members, membersLoaded, membersError, keys, keysLoaded, keysError } = useAccessStore()

const KEY_TITLES = {
  active: 'Keys that work: a key has no expiry date and works until it is revoked',
  revoked: 'Keys that no longer work: a call with one fails',
}

// Under the names the page reads, so a click here opens and filters its table.
const view = ref('')
const role = ref('')
const state = ref('')
useRouteState({ view, role, state })
// A state the payload cannot have (?state=expired) reads as all keys.
const keyPick = computed(() => pickKeyState(state.value))

const peopleGroups = computed(() => memberGroups(members.value, role.value))
const keyGroupsNow = computed(() => keyGroups(keys.value))

// One pick at a time: the other table's filter is dropped with it.
function pickPeople(r) {
  view.value = ''
  state.value = ''
  role.value = r
}
function pickKeys(s) {
  view.value = 'keys'
  role.value = ''
  state.value = s
}
</script>

<style scoped>
.acc-side { padding-top: 12px; }
/* A filter row sits under its group's "All" row, on the text's edge. Not
   .ss-sub: SubSidebar's global .ss-sub is a row's small second line. */
.acc-sub { padding-left: 36px; }
</style>
