<template>
  <!--
    The queue list's namespace filter: a field that opens the app's menu panel
    (the theme and account menus' look), each namespace with its queue count
    and the worst mark among its queues. null is every namespace; '' is the
    default namespace, a real one. A search box appears once the menu is long.
  -->
  <div ref="root" class="nsp" @focusout="onFocusOut" @keydown.escape.stop.prevent="close(true)">
    <div class="nsp-field" :class="{ open, set: modelValue !== null }">
      <button
        ref="trigger"
        type="button"
        class="nsp-trigger"
        aria-haspopup="listbox"
        :aria-expanded="open"
        :aria-label="`Namespace: ${label}`"
        @click="open ? close(true) : openMenu()"
        @keydown.down.prevent="openMenu()"
      >
        <svg class="nsp-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
          <path stroke-linejoin="round" d="M3.5 7.5a2 2 0 0 1 2-2h4l2 2h7a2 2 0 0 1 2 2v7a2 2 0 0 1-2 2h-13a2 2 0 0 1-2-2z" />
        </svg>
        <span class="nsp-label">{{ label }}</span>
        <svg class="nsp-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" d="m7 10 5 5 5-5" />
        </svg>
      </button>
      <button v-if="modelValue !== null" type="button" class="nsp-clear" aria-label="Show every namespace" title="Every namespace" @click="pick(null)">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
          <path stroke-linecap="round" d="M7 7l10 10M17 7 7 17" />
        </svg>
      </button>
    </div>

    <div v-if="open" class="nsp-menu" @keydown="onMenuKeydown">
      <input
        v-if="searchable"
        ref="findInput"
        v-model="term"
        type="search"
        class="input nsp-find"
        placeholder="Find a namespace"
        aria-label="Find a namespace"
      />
      <div class="nsp-list" role="listbox" aria-label="Namespaces">
        <button
          v-for="(o, i) in shown"
          :key="o.key"
          type="button"
          role="option"
          class="nsp-option"
          :class="{ on: o.value === modelValue }"
          :aria-selected="o.value === modelValue"
          :tabindex="i === focused ? 0 : -1"
          @focus="focused = i"
          @click="pick(o.value)"
        >
          <span class="ss-mark" aria-hidden="true"><span v-if="o.sev" class="g" :class="o.sev" /></span>
          <span class="nsp-name">{{ o.label }}</span>
          <span class="nsp-num">{{ formatNumber(o.count) }}</span>
          <svg class="nsp-check" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
            <path v-if="o.value === modelValue" stroke-linecap="round" stroke-linejoin="round" d="m5 12 4 4L19 6" />
          </svg>
        </button>
        <div v-if="!shown.length" class="nsp-empty">No namespace matches</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'

import { formatNumber } from '@/composables/useApi'

const props = defineProps({
  /** The namespace picked: null for every one, '' for the default one. */
  modelValue: { type: String, default: null },
  /** `[{ name, count, sev }]`, from navigation.namespaceCounts. */
  namespaces: { type: Array, default: () => [] },
})
const emit = defineEmits(['update:modelValue'])

// A search box once the menu no longer fits at a glance.
const SEARCH_FROM = 9

const root = ref(null)
const trigger = ref(null)
const findInput = ref(null)
const open = ref(false)
const term = ref('')
const focused = ref(0)

const nameOf = (ns) => (ns === '' ? '(default)' : ns)
const label = computed(() => (props.modelValue === null ? 'All namespaces' : nameOf(props.modelValue)))

const options = computed(() => {
  const all = props.namespaces.reduce((s, n) => s + (n.count || 0), 0)
  // No mark on "All": each namespace carries its own, and the rail already
  // says that some queue needs you.
  return [
    { key: '\u0000all', value: null, label: 'All namespaces', count: all, sev: null },
    ...props.namespaces.map((n) => ({ key: n.name, value: n.name, label: nameOf(n.name), count: n.count, sev: n.sev || null })),
  ]
})
const searchable = computed(() => options.value.length >= SEARCH_FROM)
const shown = computed(() => {
  const t = term.value.trim().toLowerCase()
  return t ? options.value.filter((o) => o.value !== null && o.label.toLowerCase().includes(t)) : options.value
})
watch(shown, () => { focused.value = 0 })

function optionEls() {
  return root.value?.querySelectorAll('.nsp-option') || []
}
function focusOption(i) {
  const n = shown.value.length
  if (!n) return
  focused.value = (i + n) % n
  optionEls()[focused.value]?.focus()
}

async function openMenu() {
  open.value = true
  term.value = ''
  await nextTick()
  if (!open.value) return
  if (searchable.value) findInput.value?.focus()
  else focusOption(Math.max(0, shown.value.findIndex((o) => o.value === props.modelValue)))
}

function close(restoreFocus = false) {
  open.value = false
  if (restoreFocus) trigger.value?.focus()
}

function pick(value) {
  emit('update:modelValue', value)
  close(true)
}

function onMenuKeydown(event) {
  if (event.key === 'Tab') { close(); return }
  const inFind = event.target === findInput.value
  if (event.key === 'ArrowDown') { event.preventDefault(); focusOption(inFind ? 0 : focused.value + 1) }
  else if (event.key === 'ArrowUp') {
    event.preventDefault()
    if (!inFind && focused.value === 0 && searchable.value) findInput.value?.focus()
    else focusOption(focused.value - 1)
  } else if (event.key === 'Enter' && inFind && shown.value.length) { event.preventDefault(); pick(shown.value[0].value) }
  else if (!inFind && event.key === 'Home') { event.preventDefault(); focusOption(0) }
  else if (!inFind && event.key === 'End') { event.preventDefault(); focusOption(shown.value.length - 1) }
}

function onFocusOut(event) {
  if (open.value && !root.value?.contains(event.relatedTarget)) close()
}
function onPointerDown(event) {
  if (open.value && !root.value?.contains(event.target)) close()
}
onMounted(() => document.addEventListener('pointerdown', onPointerDown))
onUnmounted(() => document.removeEventListener('pointerdown', onPointerDown))
</script>

<style scoped>
.nsp { position: relative; }

/* The field: the search box's twin above it, a button that names the pick. */
.nsp-field {
  display: flex; align-items: center; height: 32px;
  border: 1px solid var(--bd); border-radius: var(--r-control); background: var(--ink-3);
  transition: border-color .12s;
}
.nsp-field:hover, .nsp-field.open { border-color: var(--bd-hi); }
.nsp-trigger {
  flex: 1; min-width: 0; height: 100%;
  display: flex; align-items: center; gap: 8px; padding: 0 10px;
  border: 0; background: transparent; color: var(--text-mid);
  font: inherit; font-size: 12.5px; text-align: left; cursor: pointer;
}
.nsp-field.set .nsp-trigger { color: var(--text-hi); }
.nsp-icon { width: 15px; height: 15px; flex: none; color: var(--text-low); }
.nsp-label { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nsp-chevron { width: 14px; height: 14px; flex: none; color: var(--text-low); transition: transform .12s; }
.nsp-field.open .nsp-chevron { transform: rotate(180deg); }
.nsp-clear {
  flex: none; width: 26px; height: 26px; margin-right: 3px;
  display: inline-flex; align-items: center; justify-content: center;
  border: 0; border-radius: var(--r-chip); background: transparent; color: var(--text-low); cursor: pointer;
}
.nsp-clear:hover { background: var(--ink-4); color: var(--text-hi); }
.nsp-clear svg { width: 13px; height: 13px; }

/* The menu: the theme and account menus' panel, as wide as the list's rows
   under it (4px past the field each side), so none of them shows at its
   edges. */
.nsp-menu {
  position: absolute; top: calc(100% + 6px); left: -4px; right: -4px; z-index: 30;
  padding: 4px; border: 1px solid var(--bd-hi);
  border-radius: var(--r-card); background: var(--ink-3); box-shadow: var(--shadow-pop);
}
.nsp-find { width: 100%; height: 30px; margin-bottom: 4px; font-size: 12.5px; }
.nsp-list { max-height: 300px; overflow-y: auto; display: flex; flex-direction: column; gap: 1px; }
.nsp-option {
  display: flex; align-items: center; gap: 9px;
  width: 100%; min-height: 30px; padding: 0 8px; border: 0; border-radius: var(--r-control);
  background: transparent; color: var(--text-mid); font: inherit; font-size: 13px;
  text-align: left; cursor: pointer;
}
.nsp-option:hover, .nsp-option:focus-visible { background: var(--ink-4); color: var(--text-hi); outline: none; }
.nsp-option.on { color: var(--text-hi); }
.nsp-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nsp-num { font-family: var(--font-mono); font-size: 11.5px; color: var(--text-low); font-variant-numeric: tabular-nums; }
.nsp-check { width: 14px; height: 14px; flex: none; color: var(--text-hi); }
.nsp-empty { padding: 8px 10px; font-size: 12px; color: var(--text-low); }
</style>
