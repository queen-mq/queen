<template>
  <!--
    Settings' "Add a queue": a button in the page's header line that opens the
    app's menu panel (UiSelect's and the namespace picker's look) with a
    search box and the queues that have no lines of their own. A name no
    queue has yet can be typed and taken, so a queue can have its lines
    before its first push; typing the name of one that has its own offers to
    open it instead. A pick emits `pick` (the page opens that queue's form);
    the menu never saves anything.
  -->
  <div class="aqm">
    <button
      ref="trigger"
      type="button"
      class="btn"
      :disabled="disabled"
      aria-haspopup="dialog"
      :aria-expanded="open"
      @click="open ? close(true) : openMenu()"
    >
      <Icon name="plus" :size="14" :stroke="2" />Add a queue
    </button>
    <Teleport to="body">
      <div
        v-if="open"
        ref="menu"
        class="aqm-menu"
        role="dialog"
        aria-label="Add a queue"
        :style="menuStyle"
        @keydown="onKeydown"
      >
        <input
          ref="findInput"
          v-model="term"
          type="search"
          class="input aqm-find"
          placeholder="Find or type a queue name"
          aria-label="Find or type a queue name"
          autocomplete="off"
          spellcheck="false"
        />
        <div class="aqm-list" role="listbox" aria-label="Queues">
          <button
            v-for="(o, i) in options"
            :key="`${o.kind}:${o.name}`"
            type="button"
            role="option"
            class="aqm-option"
            :aria-selected="i === focused"
            :tabindex="i === focused ? 0 : -1"
            :title="o.title || o.name"
            @focus="focused = i"
            @click="choose(o)"
          >
            <span class="aqm-name">{{ o.name }}</span>
            <span v-if="o.note" class="aqm-note">{{ o.note }}</span>
          </button>
          <div v-if="!options.length" class="aqm-empty">{{ emptyText }}</div>
          <div v-else-if="choices.more" class="aqm-empty">{{ formatNumber(choices.more) }} more · type to narrow</div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'

import Icon from '@/components/Icon.vue'
import { queueChoices } from '@/composables/settingsSide'
import { formatNumber } from '@/composables/useApi'

const props = defineProps({
  /** The cluster's queue names. */
  names: { type: Array, default: () => [] },
  /** The queues that already have lines of their own (a Map or a Set). */
  taken: { type: null, default: null },
  disabled: { type: Boolean, default: false },
})
const emit = defineEmits(['pick', 'open'])

const WIDTH = 300

const trigger = ref(null)
const menu = ref(null)
const findInput = ref(null)
const open = ref(false)
const term = ref('')
const focused = ref(-1)
const menuStyle = ref({})

const choices = computed(() => queueChoices(props.names, props.taken, term.value))
const options = computed(() => {
  const c = choices.value
  return [
    ...(c.own ? [{ kind: 'own', name: c.own, note: 'has its own · open', title: `${c.own} has lines of its own: open them` }] : []),
    ...c.rows.map((name) => ({ kind: 'queue', name })),
    ...(c.typed ? [{ kind: 'typed', name: c.typed, note: 'not created yet', title: `No queue is called ${c.typed} yet: its lines will wait for its first push` }] : []),
  ]
})
const emptyText = computed(() => (props.names.length ? 'Every queue has its own lines' : 'No queues yet · type a name'))
watch(term, () => { focused.value = -1 })

function place() {
  const r = trigger.value?.getBoundingClientRect()
  if (!r) return
  const width = Math.min(WIDTH, window.innerWidth - 16)
  const left = Math.min(Math.max(8, r.right - width), window.innerWidth - width - 8)
  const below = window.innerHeight - r.bottom
  const style = { left: `${left}px`, width: `${width}px` }
  if (below < 260 && r.top > below) style.bottom = `${window.innerHeight - r.top + 6}px`
  else style.top = `${r.bottom + 6}px`
  menuStyle.value = style
}

async function openMenu() {
  if (props.disabled) return
  term.value = ''
  focused.value = -1
  place()
  open.value = true
  await nextTick()
  findInput.value?.focus()
}

function close(restoreFocus = false) {
  open.value = false
  if (restoreFocus) trigger.value?.focus()
}

function choose(o) {
  close(true)
  if (o.kind === 'own') emit('open', o.name)
  else emit('pick', o.name)
}

function focusOption(i) {
  const n = options.value.length
  if (!n) return
  focused.value = (i + n) % n
  menu.value?.querySelectorAll('.aqm-option')[focused.value]?.focus()
}

function onKeydown(event) {
  const inFind = event.target === findInput.value
  if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); close(true); return }
  if (event.key === 'Tab') { close(); return }
  if (event.key === 'ArrowDown') { event.preventDefault(); focusOption(inFind ? 0 : focused.value + 1) }
  else if (event.key === 'ArrowUp') {
    event.preventDefault()
    if (!inFind && focused.value <= 0) { focused.value = -1; findInput.value?.focus() } else focusOption(focused.value - 1)
  } else if (event.key === 'Enter' && inFind) {
    event.preventDefault()
    if (options.value.length) choose(options.value[0])
  }
}

function onPointerDown(event) {
  if (!open.value) return
  if (trigger.value?.contains(event.target) || menu.value?.contains(event.target)) return
  close()
}
// The page moved under the menu: it would float away from its button. A
// scroll inside the menu's own list is not one.
function onViewportChange(event) {
  if (!open.value) return
  if (event?.type === 'scroll' && menu.value?.contains(event.target)) return
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

<style scoped>
.aqm { display: inline-flex; }
.aqm .btn { gap: 6px; }

/* The app's menu panel (UiSelect, the namespace picker). Teleported: these
   rules reach it because a scoped style follows the component's own nodes. */
.aqm-menu {
  position: fixed; z-index: 120;
  display: flex; flex-direction: column; gap: 4px;
  padding: 4px; border: 1px solid var(--bd-hi); border-radius: var(--r-card);
  background: var(--ink-3); box-shadow: var(--shadow-pop);
}
.aqm-find { width: 100%; height: 30px; font-size: 12.5px; }
.aqm-list { max-height: 300px; overflow-y: auto; display: flex; flex-direction: column; gap: 1px; }
.aqm-option {
  display: flex; align-items: center; gap: 10px;
  width: 100%; min-height: 30px; padding: 0 10px; border: 0; border-radius: var(--r-control);
  background: transparent; color: var(--text-mid); font: inherit; font-size: 13px;
  text-align: left; cursor: pointer;
}
.aqm-option:hover, .aqm-option:focus-visible { background: var(--ink-4); color: var(--text-hi); outline: none; }
.aqm-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.aqm-note { flex: none; font-size: 12px; color: var(--text-low); }
.aqm-empty { padding: 8px 10px; font-size: 12px; color: var(--text-low); }
</style>
