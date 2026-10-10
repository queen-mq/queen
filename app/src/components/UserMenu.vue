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

<style scoped>
.user-picker { position: relative; flex: none; }
/* The bar's icon button, widened by the name beside the icon. */
.user-trigger {
  width: auto; max-width: 200px; padding: 0 10px 0 8px;
  display: inline-flex; align-items: center; gap: 7px;
  font: inherit; font-size: 13px;
}
.user-trigger.active { color: var(--text-hi); background: var(--ink-4); }
.user-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.user-menu {
  position: absolute; top: calc(100% + 6px); right: 0; z-index: 50;
  min-width: 200px; max-width: 320px; padding: 4px; border: 1px solid var(--bd-hi);
  border-radius: var(--r-card); background: var(--ink-3); box-shadow: var(--shadow-pop);
}
.user-who {
  display: grid; gap: 2px; padding: 8px 10px 9px; margin-bottom: 4px;
  border-bottom: 1px solid var(--bd); font-size: 12px; color: var(--text-low);
}
.user-who b { font-size: 13px; font-weight: 500; color: var(--text-hi); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.user-item {
  display: block; width: 100%; padding: 7px 10px; border: 0; border-radius: var(--r-control);
  background: transparent; color: var(--text-mid); font: inherit; font-size: 13px;
  text-align: left; cursor: pointer; transition: color .12s, background .12s;
}
.user-item:hover, .user-item:focus-visible { color: var(--text-hi); background: var(--ink-4); }
/* A bar with no room for a name keeps the icon: the menu still says who. */
@container (max-width: 1180px) {
  .user-trigger { width: 32px; padding: 0; justify-content: center; }
  .user-name { display: none; }
}
</style>
