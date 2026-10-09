<template>
  <div ref="root" class="theme-picker" @focusout="onFocusOut" @keydown.escape.stop.prevent="closeMenu(true)">
    <button
      ref="trigger"
      type="button"
      class="top-btn theme-trigger"
      :class="{ active: open }"
      :title="`Colour theme: ${currentLabel}`"
      :aria-label="`Colour theme: ${currentLabel}`"
      aria-haspopup="menu"
      aria-controls="theme-menu"
      :aria-expanded="open"
      @click="open ? closeMenu(true) : openMenu()"
      @keydown.down.prevent="openMenu(0)"
      @keydown.up.prevent="openMenu(options.length - 1)"
    >
      <svg v-if="themePreference === 'system'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <rect x="3" y="4" width="18" height="12" rx="2" />
        <path stroke-linecap="round" d="M8 20h8M12 16v4" />
      </svg>
      <svg v-else-if="themePreference === 'light'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <circle cx="12" cy="12" r="4.2" />
        <path stroke-linecap="round" d="M12 2.6v2.2M12 19.2v2.2M4.22 4.22l1.56 1.56M18.22 18.22l1.56 1.56M2.6 12h2.2M19.2 12h2.2M4.22 19.78l1.56-1.56M18.22 5.78l1.56-1.56" />
      </svg>
      <svg v-else viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round" d="M20.4 13.9A8.6 8.6 0 1 1 10.1 3.6a6.9 6.9 0 0 0 10.3 10.3z" />
      </svg>
    </button>

    <div v-if="open" id="theme-menu" ref="menu" class="theme-menu" role="menu" aria-label="Colour theme" @keydown="onMenuKeydown">
      <button
        v-for="(option, index) in options"
        :key="option.value"
        type="button"
        class="theme-option"
        role="menuitemradio"
        :aria-checked="themePreference === option.value"
        :tabindex="index === focusedIndex ? 0 : -1"
        @focus="focusedIndex = index"
        @click="selectTheme(option.value)"
      >
        <span>{{ option.label }}</span>
        <svg v-if="themePreference === option.value" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" d="m5 12 4 4L19 6" />
        </svg>
      </button>
    </div>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import { setTheme, themePreference } from '@/composables/useTheme'

const options = [
  { value: 'system', label: 'System' },
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
]
const currentLabel = computed(() => options.find(option => option.value === themePreference.value)?.label)
const root = ref(null)
const trigger = ref(null)
const menu = ref(null)
const open = ref(false)
const focusedIndex = ref(0)

function focusOption(index) {
  focusedIndex.value = (index + options.length) % options.length
  menu.value?.querySelectorAll('[role="menuitemradio"]')[focusedIndex.value]?.focus()
}

async function openMenu(index = options.findIndex(option => option.value === themePreference.value)) {
  open.value = true
  await nextTick()
  if (open.value) focusOption(index)
}

function closeMenu(restoreFocus = false) {
  open.value = false
  if (restoreFocus) trigger.value?.focus()
}

function selectTheme(value) {
  setTheme(value)
  closeMenu(true)
}

function onMenuKeydown(event) {
  if (event.key === 'Tab') {
    closeMenu(true)
    return
  }
  let index
  if (event.key === 'ArrowDown') index = focusedIndex.value + 1
  else if (event.key === 'ArrowUp') index = focusedIndex.value - 1
  else if (event.key === 'Home') index = 0
  else if (event.key === 'End') index = options.length - 1
  else if (event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
    index = options.findIndex(option => option.label.toLowerCase().startsWith(event.key.toLowerCase()))
    if (index < 0) return
  } else return
  event.preventDefault()
  focusOption(index)
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
.theme-picker { position: relative; flex: none; }
.theme-trigger.active { color: var(--text-hi); background: var(--ink-4); }
.theme-menu {
  position: absolute; top: calc(100% + 6px); right: 0; z-index: 50;
  min-width: 144px; padding: 4px; border: 1px solid var(--bd-hi);
  border-radius: var(--r-card); background: var(--ink-3); box-shadow: var(--shadow-pop);
}
.theme-option {
  display: flex; align-items: center; justify-content: space-between; gap: 16px;
  width: 100%; padding: 7px 10px; border: 0; border-radius: var(--r-control);
  background: transparent; color: var(--text-mid); font: inherit; font-size: 13px;
  text-align: left; cursor: pointer; transition: color .12s, background .12s;
}
.theme-option:hover, .theme-option:focus-visible { color: var(--text-hi); background: var(--ink-4); }
.theme-option[aria-checked="true"] { color: var(--text-hi); }
.theme-option svg { width: 15px; height: 15px; flex: none; color: var(--accent-text); }
</style>
