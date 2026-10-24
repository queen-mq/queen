<template>
  <!-- The session: who is signed in, and the way out. The last thing on the top
       bar, where an account lives in every console. Standalone has none: no
       email to show, and a sign-out that could only reload the page. -->
  <div v-if="!standalone" ref="root" class="user-picker" @focusout="onFocusOut" @keydown.escape.stop.prevent="closeMenu(true)">
    <button
      ref="trigger"
      type="button"
      class="top-btn user-trigger"
      :class="{ active: open }"
      :title="`Signed in as ${who}`"
      :aria-label="`Account: ${who}`"
      aria-haspopup="menu"
      aria-controls="user-menu"
      :aria-expanded="open"
      @click="open ? closeMenu(true) : openMenu()"
      @keydown.down.prevent="openMenu()"
    >
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <circle cx="12" cy="8.5" r="3.6" />
        <path stroke-linecap="round" d="M4.8 20c.6-3.7 3.6-6 7.2-6s6.6 2.3 7.2 6" />
      </svg>
      <span class="user-name">{{ name }}</span>
    </button>

    <div v-if="open" id="user-menu" class="user-menu" role="menu" aria-label="Account">
      <div class="user-who">
        <span>Signed in as</span>
        <b :title="who">{{ who }}</b>
      </div>
      <button ref="out" type="button" class="user-item" role="menuitem" @click="signOut">Sign out</button>
    </div>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'

import { useIdentity } from '@/stores/identity'

const { email, logout, standalone } = useIdentity()

// A session with no email on it is still a session.
const who = computed(() => email.value || 'signed in')
// On the bar, the part of the address a person goes by; the menu has all of it.
const name = computed(() => (email.value ? email.value.split('@')[0] : 'Account'))

const root = ref(null)
const trigger = ref(null)
const out = ref(null)
const open = ref(false)

async function openMenu() {
  open.value = true
  await nextTick()
  if (open.value) out.value?.focus()
}

function closeMenu(restoreFocus = false) {
  open.value = false
  if (restoreFocus) trigger.value?.focus()
}

function signOut() {
  closeMenu()
  logout()
}

function onFocusOut(event) {
  if (!root.value?.contains(event.relatedTarget)) closeMenu()
}

function onPointerDown(event) {
  if (open.value && !root.value?.contains(event.target)) closeMenu()
}

onMounted(() => document.addEventListener('pointerdown', onPointerDown))
onUnmounted(() => document.removeEventListener('pointerdown', onPointerDown))
</script>
