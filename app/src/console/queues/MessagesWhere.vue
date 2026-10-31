<script setup>
// Which queues the Messages view reads: every queue, one queue, a namespace or
// a task (the broker's queue, namespace and task filters). A chip that opens a
// short list with a search: the namespaces at first, and queues, namespaces
// and tasks that match what you type.
import { computed, ref, watch } from 'vue'
import QcPopover from '../QcPopover.vue'

const props = defineProps({
  queue: { type: String, default: '' },
  namespace: { type: String, default: '' },
  task: { type: String, default: '' },
  queues: { type: Array, default: () => [] }, // the queues listing: [{ name, namespace, task }]
})
const emit = defineEmits(['pick'])

const open = ref(false)
const find = ref('')
// The row Enter picks: none until something is typed, then the first match.
const hi = ref(-1)
watch(open, (o) => { if (o) { find.value = ''; hi.value = -1 } })
watch(find, (t) => { hi.value = t.trim() ? 0 : -1 })

function counted(key) {
  const m = new Map()
  for (const q of props.queues || []) {
    const v = q?.[key]
    if (typeof v === 'string' && v) m.set(v, (m.get(v) || 0) + 1)
  }
  return [...m.entries()].map(([value, n]) => ({ kind: key, value, n })).sort((a, b) => a.value.localeCompare(b.value))
}
const namespaces = computed(() => counted('namespace'))
const tasks = computed(() => counted('task'))
const options = computed(() => {
  const t = find.value.trim().toLowerCase()
  if (!t) return namespaces.value
  const has = (v) => v.toLowerCase().includes(t)
  const first = (v) => (v.toLowerCase().startsWith(t) ? 0 : 1)
  const qs = (props.queues || [])
    .filter((q) => typeof q?.name === 'string' && has(q.name))
    .sort((a, b) => first(a.name) - first(b.name) || a.name.localeCompare(b.name))
    .slice(0, 10)
    .map((q) => ({ kind: 'queue', value: q.name, ns: typeof q.namespace === 'string' ? q.namespace : '' }))
  // A namespace first: a few of them, and each the widest pick.
  return [...namespaces.value.filter((o) => has(o.value)).slice(0, 4), ...qs, ...tasks.value.filter((o) => has(o.value)).slice(0, 4)]
})
const none = computed(() => !props.queue && !props.namespace && !props.task)
const isOn = (o) => (o.kind === 'queue' ? props.queue === o.value : o.kind === 'namespace' ? !props.queue && props.namespace === o.value : !props.queue && !props.namespace && props.task === o.value)

function pick(o, close) {
  emit('pick', o ? { queue: o.kind === 'queue' ? o.value : '', namespace: o.kind === 'namespace' ? o.value : '', task: o.kind === 'task' ? o.value : '' } : { queue: '', namespace: '', task: '' })
  close?.()
}
function onKey(e, close) {
  const n = options.value.length
  if (e.key === 'ArrowDown') { e.preventDefault(); hi.value = n ? Math.min(n - 1, hi.value + 1) : 0 }
  else if (e.key === 'ArrowUp') { e.preventDefault(); hi.value = Math.max(0, hi.value - 1) }
  else if (e.key === 'Enter') {
    e.preventDefault()
    if (options.value[hi.value]) pick(options.value[hi.value], close)
  }
}
const qn = (n) => `${n.toLocaleString('en-US')} ${n === 1 ? 'queue' : 'queues'}`
</script>

<template>
  <QcPopover v-model:open="open" side="bottom-start" :width="380" label="Which queues">
    <template #trigger="{ toggle }">
      <button type="button" class="qn-chip mw-chip" aria-haspopup="dialog" :aria-expanded="open" @click="toggle">
        <template v-if="queue"><span class="qn-mono mw-val">{{ queue }}</span></template>
        <template v-else-if="namespace"><span class="qn-t3">namespace</span><span class="qn-mono mw-val">{{ namespace }}</span></template>
        <template v-else-if="task"><span class="qn-t3">task</span><span class="qn-mono mw-val">{{ task }}</span></template>
        <span v-else>Every queue</span>
        <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><polyline points="2,3.5 5,6.5 8,3.5" style="fill: none; stroke: currentColor; stroke-width: 1.3" /></svg>
      </button>
    </template>
    <template #default="{ close }">
      <label class="qn-field mw-find">
        <span class="qn-vh">Find a queue, a namespace or a task</span>
        <input v-model="find" type="text" placeholder="Find a queue, namespace or task" spellcheck="false" autocomplete="off" @keydown="onKey($event, close)" />
      </label>
      <button type="button" class="qn-opt" :class="{ on: none }" @click="pick(null, close)">Every queue</button>
      <div class="qn-popsep" />
      <p v-if="!find.trim() && options.length" class="mw-head qn-t3">Namespaces</p>
      <div class="mw-list" role="listbox" aria-label="Queues, namespaces and tasks">
        <button
          v-for="(o, i) in options" :key="`${o.kind}:${o.value}`" type="button" class="qn-opt mw-opt"
          :class="{ on: isOn(o), hover: i === hi }" role="option" :aria-selected="isOn(o)" @click="pick(o, close)" @mousemove="hi = i"
        >
          <span class="mw-left">
            <span v-if="o.kind !== 'queue' && find.trim()" class="qn-t3 mw-kind">{{ o.kind }}</span>
            <span class="qn-mono mw-name">{{ o.value }}</span>
          </span>
          <span class="qn-t3 mw-right" :class="{ 'qn-mono': o.kind === 'queue' }">{{ o.kind === 'queue' ? o.ns : qn(o.n) }}</span>
        </button>
      </div>
      <p v-if="find.trim() && !options.length" class="qn-t2 mw-note">No queue, namespace or task has “{{ find.trim() }}” in its name.</p>
      <p v-else-if="!find.trim()" class="qn-t3 mw-note">Type to find a queue or a task.</p>
    </template>
  </QcPopover>
</template>

<style scoped>
.mw-chip { gap: 6px; max-width: 360px; }
.mw-val { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.mw-find { margin-bottom: 6px; }
.mw-head { font-size: 12px; padding: 4px 10px 2px; }
.mw-list { max-height: 300px; overflow-y: auto; }
.mw-opt { display: flex; align-items: center; gap: 12px; width: 100%; }
.mw-left { display: flex; align-items: baseline; gap: 8px; flex: 1 1 0; min-width: 0; }
.mw-kind { flex: 0 0 auto; font-size: 12px; }
.mw-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.mw-right { flex: 0 1 auto; min-width: 0; max-width: 45%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 12px; font-weight: 400; }
.mw-note { font-size: 12px; padding: 6px 10px 2px; }
/* the highlighted row is a hover, not a pick */
.mw-opt.hover:not(.on) { background: var(--bg2); }
</style>
