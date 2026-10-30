<template>
  <!--
    The app's select: a field that names the choice and opens the menu panel
    the theme, account and namespace menus share. Replaces the browser's
    <select>, whose menu is the operating system's and ignores the theme.

      <UiSelect v-model="limit" label="Show" :options="[25, 50, 100]" />
      <UiSelect v-model="status" label="Status" :options="[{ value: '', label: 'All' }, …]" />

    Options are values or { value, label }. The menu is placed on the page
    (teleported), so no card or scrolling column clips it.
  -->
  <div ref="root" class="uis" :class="[`uis-${size}`, { open, block }]">
    <button
      ref="trigger"
      type="button"
      class="uis-field"
      :disabled="disabled"
      aria-haspopup="listbox"
      :aria-expanded="open"
      :aria-label="ariaLabel || label || undefined"
      @click="open ? close(true) : openMenu()"
      @keydown.down.prevent="openMenu()"
      @keydown.up.prevent="openMenu()"
    >
      <span v-if="label" class="uis-label">{{ label }}</span>
      <span class="uis-value">{{ current ? current.label : placeholder }}</span>
      <Icon name="chevron-down" class="uis-chevron" :size="14" :stroke="1.8" />
    </button>
    <Teleport to="body">
      <div
        v-if="open"
        ref="menu"
        class="uis-menu"
        role="listbox"
        :aria-label="ariaLabel || label || 'Options'"
        :style="menuStyle"
        @keydown="onMenuKeydown"
        @keydown.escape.stop.prevent="close(true)"
      >
        <button
          v-for="(o, i) in list"
          :key="String(o.value)"
          type="button"
          role="option"
          class="uis-option"
          :class="{ on: same(o.value, modelValue) }"
          :aria-selected="same(o.value, modelValue)"
          :tabindex="i === focused ? 0 : -1"
          @focus="focused = i"
          @click="pick(o.value)"
        >
          <span class="uis-option-label">{{ o.label }}</span>
          <span v-if="o.hint" class="uis-option-hint">{{ o.hint }}</span>
          <Icon v-if="same(o.value, modelValue)" name="check" class="uis-check" :size="14" :stroke="1.9" />
          <span v-else class="uis-check" />
        </button>
      </div>
    </Teleport>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'

import Icon from '@/components/Icon.vue'

const props = defineProps({
  modelValue: { type: null, default: null },
  /** Values, or { value, label, hint }. */
  options: { type: Array, default: () => [] },
  /** A word inside the field, before the value: "Show", "Status". */
  label: { type: String, default: '' },
  placeholder: { type: String, default: 'Choose' },
  ariaLabel: { type: String, default: '' },
  size: { type: String, default: 'md' },
  /** Fill the width of its container. */
  block: { type: Boolean, default: false },
  disabled: { type: Boolean, default: false },
})
const emit = defineEmits(['update:modelValue', 'change'])

const root = ref(null)
const trigger = ref(null)
const menu = ref(null)
const open = ref(false)
const focused = ref(0)
const menuStyle = ref({})

const list = computed(() => props.options.map((o) => (o !== null && typeof o === 'object' && 'value' in o
  ? { value: o.value, label: String(o.label ?? o.value), hint: o.hint || '' }
  : { value: o, label: String(o), hint: '' })))
// Loose on purpose: an address keeps "25" where the page keeps 25.
const same = (a, b) => a === b || (a !== null && b !== null && a !== undefined && b !== undefined && String(a) === String(b))
const current = computed(() => list.value.find((o) => same(o.value, props.modelValue)) || null)

function place() {
  const r = trigger.value?.getBoundingClientRect()
  if (!r) return
  const width = Math.max(r.width, 160)
  const left = Math.min(r.left, window.innerWidth - width - 8)
  const below = window.innerHeight - r.bottom
  const style = { left: `${Math.max(8, left)}px`, minWidth: `${width}px` }
  if (below < 220 && r.top > below) style.bottom = `${window.innerHeight - r.top + 6}px`
  else style.top = `${r.bottom + 6}px`
  menuStyle.value = style
}

function focusOption(i) {
  const n = list.value.length
  if (!n) return
  focused.value = (i + n) % n
  menu.value?.querySelectorAll('.uis-option')[focused.value]?.focus()
}

async function openMenu() {
  if (props.disabled) return
  place()
  open.value = true
  await nextTick()
  if (open.value) focusOption(Math.max(0, list.value.findIndex((o) => same(o.value, props.modelValue))))
}
function close(restoreFocus = false) {
  open.value = false
  if (restoreFocus) trigger.value?.focus()
}
function pick(value) {
  if (!same(value, props.modelValue)) {
    emit('update:modelValue', value)
    emit('change', value)
  }
  close(true)
}

function onMenuKeydown(event) {
  if (event.key === 'Tab') { close(); return }
  let i
  if (event.key === 'ArrowDown') i = focused.value + 1
  else if (event.key === 'ArrowUp') i = focused.value - 1
  else if (event.key === 'Home') i = 0
  else if (event.key === 'End') i = list.value.length - 1
  else if (event.key.length === 1 && !event.metaKey && !event.ctrlKey && !event.altKey) {
    i = list.value.findIndex((o) => o.label.toLowerCase().startsWith(event.key.toLowerCase()))
    if (i < 0) return
  } else return
  event.preventDefault()
  focusOption(i)
}

function onPointerDown(event) {
  if (!open.value) return
  if (root.value?.contains(event.target) || menu.value?.contains(event.target)) return
  close()
}
// The page moved under the menu: close it. Scrolling the menu's own list is
// not that.
function onViewportChange(event) {
  if (!open.value) return
  if (event?.type === 'scroll' && event.target instanceof Node && menu.value?.contains(event.target)) return
  close()
}
onMounted(() => {
  document.addEventListener('pointerdown', onPointerDown)
  window.addEventListener('resize', onViewportChange)
  window.addEventListener('scroll', onViewportChange, true)
})
onUnmounted(() => {
  document.removeEventListener('pointerdown', onPointerDown)
  window.removeEventListener('resize', onViewportChange)
  window.removeEventListener('scroll', onViewportChange, true)
})
</script>

<style>
/* Global: the menu is teleported to the body, outside this component's scope. */
.uis { position: relative; display: inline-flex; min-width: 0; }
.uis.block { display: flex; width: 100%; }
.uis-field {
  display: inline-flex; align-items: center; gap: 6px; width: 100%; min-width: 0;
  height: 32px; padding: 0 9px 0 10px;
  border: 1px solid var(--bd); border-radius: var(--r-control); background: var(--ink-3);
  color: var(--text-hi); font: inherit; font-size: 12.5px; text-align: left; cursor: pointer;
  transition: border-color .12s;
}
.uis-sm .uis-field { height: 28px; font-size: 12px; padding: 0 7px 0 9px; }
.uis-field:hover:not(:disabled), .uis.open .uis-field { border-color: var(--bd-hi); }
.uis-field:disabled { opacity: .55; cursor: not-allowed; }
.uis-label { color: var(--text-low); flex: none; }
.uis-value { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.uis-chevron { flex: none; color: var(--text-low); transition: transform .12s; }
.uis.open .uis-chevron { transform: rotate(180deg); }

.uis-menu {
  position: fixed; z-index: 120; max-height: 320px; overflow-y: auto;
  padding: 4px; border: 1px solid var(--bd-hi); border-radius: var(--r-card);
  background: var(--ink-3); box-shadow: var(--shadow-pop);
  display: flex; flex-direction: column; gap: 1px;
}
.uis-option {
  display: flex; align-items: center; gap: 10px; width: 100%; min-height: 30px; padding: 0 8px 0 10px;
  border: 0; border-radius: var(--r-control); background: transparent;
  color: var(--text-mid); font: inherit; font-size: 13px; text-align: left; cursor: pointer; white-space: nowrap;
}
.uis-option:hover, .uis-option:focus-visible { background: var(--ink-4); color: var(--text-hi); outline: none; }
.uis-option.on { color: var(--text-hi); }
.uis-option-label { flex: 1; }
.uis-option-hint { font-size: 11.5px; color: var(--text-low); font-family: var(--font-mono); }
.uis-check { flex: none; width: 14px; height: 14px; color: var(--text-hi); }
</style>
