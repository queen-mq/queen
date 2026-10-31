<script setup>
// ⌘K: find a page, a queue or a consumer group by name. Arrows move, Enter
// opens, Escape closes. Pasting an id (a transactionId, a partition) comes
// with the Entities page.
import { computed, nextTick, onBeforeUnmount, ref, watch } from 'vue'
import { useRouter } from 'vue-router'

const props = defineProps({
  open: { type: Boolean, default: false },
  items: { type: Array, default: () => [] }, // [{ kind, label, sub?, to }]
})
const emit = defineEmits(['update:open'])
const router = useRouter()

const q = ref('')
const at = ref(0)
const input = ref(null)
const list = ref(null)

const KINDS = { page: 'Pages', queue: 'Queues', group: 'Consumer groups' }
const found = computed(() => {
  const t = q.value.trim().toLowerCase()
  const rows = []
  for (const kind of Object.keys(KINDS)) {
    const all = props.items.filter((i) => i.kind === kind)
    const hit = t ? all.filter((i) => (i.label + ' ' + (i.sub || '')).toLowerCase().includes(t)) : (kind === 'page' ? all : all.slice(0, 5))
    hit.slice(0, kind === 'page' ? 12 : 8).forEach((i) => rows.push(i))
  }
  return rows
})
const groups = computed(() => {
  const out = []
  found.value.forEach((r, i) => {
    let g = out.find((x) => x.kind === r.kind)
    if (!g) { g = { kind: r.kind, title: KINDS[r.kind], rows: [] }; out.push(g) }
    g.rows.push({ ...r, i })
  })
  return out
})
const looksLikeId = computed(() => /^[0-9a-f]{8}-[0-9a-f-]{20,}$/i.test(q.value.trim()) || /^[\w.:-]{24,}$/.test(q.value.trim()))

function close() { emit('update:open', false) }
function go(r) { if (!r) return; close(); router.push(r.to) }
function key(e) {
  if (e.key === 'ArrowDown') { e.preventDefault(); at.value = Math.min(found.value.length - 1, at.value + 1); reveal() }
  else if (e.key === 'ArrowUp') { e.preventDefault(); at.value = Math.max(0, at.value - 1); reveal() }
  else if (e.key === 'Enter') { e.preventDefault(); go(found.value[at.value]) }
  else if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); close() }
}
function reveal() { nextTick(() => list.value?.querySelector('[aria-selected="true"]')?.scrollIntoView({ block: 'nearest' })) }
watch(q, () => { at.value = 0 })
watch(() => props.open, async (o) => {
  if (o) { q.value = ''; at.value = 0; await nextTick(); input.value?.focus() }
})
function onDoc(e) { if (props.open && e.target.classList?.contains('qn-search-scrim')) close() }
document.addEventListener('pointerdown', onDoc)
onBeforeUnmount(() => document.removeEventListener('pointerdown', onDoc))
</script>

<template>
  <Transition name="qn-fade">
    <div v-if="open" class="qn-search-scrim">
      <div class="qn-search" role="dialog" aria-modal="true" aria-label="Search">
        <label class="qn-field" style="height: 44px; border-radius: var(--r-pop) var(--r-pop) 0 0; border-width: 0 0 1px 0; background: transparent">
          <svg width="16" height="16" viewBox="0 0 18 18" aria-hidden="true" style="fill: none; stroke: currentColor; stroke-width: 1.4"><circle cx="7.5" cy="7.5" r="5" /><line x1="11.3" y1="11.3" x2="15.5" y2="15.5" /></svg>
          <span class="qn-vh">Search</span>
          <input
            ref="input"
            v-model="q"
            type="text"
            placeholder="Find a page, a queue or a consumer group"
            autocomplete="off"
            spellcheck="false"
            role="combobox"
            aria-expanded="true"
            aria-controls="qn-search-list"
            style="font-size: 14px"
            @keydown="key"
          />
          <span class="qn-kbd">Esc</span>
        </label>
        <div id="qn-search-list" ref="list" class="qn-search-list" role="listbox">
          <p v-if="looksLikeId" class="qn-t2" style="padding: 10px 14px; font-size: 12.5px">Finding a message or an entity by its id comes with the Entities page.</p>
          <p v-if="!found.length && !looksLikeId" class="qn-t2" style="padding: 14px">Nothing matches “{{ q }}”.</p>
          <template v-for="g in groups" :key="g.kind">
            <p class="qn-t3" style="font-size: 11.5px; padding: 10px 14px 4px">{{ g.title }}</p>
            <button
              v-for="r in g.rows"
              :key="r.kind + r.label"
              type="button"
              class="qn-opt"
              role="option"
              :aria-selected="r.i === at"
              :class="{ hover: r.i === at }"
              style="padding: 0 14px; border-radius: 0"
              @mouseenter="at = r.i"
              @click="go(r)"
            >
              <span :class="r.kind === 'page' ? '' : 'qn-mono'" style="flex: 1 1 0; min-width: 0; overflow: hidden; text-overflow: ellipsis">{{ r.label }}</span>
              <span v-if="r.sub" class="qn-t3" style="font-size: 12px">{{ r.sub }}</span>
            </button>
          </template>
        </div>
      </div>
    </div>
  </Transition>
</template>

<style scoped>
.qn-search-scrim { position: fixed; inset: 0; z-index: 45; background: var(--scrim); display: flex; justify-content: center; align-items: flex-start; padding: 12vh 16px 16px; }
.qn-search { width: 600px; max-width: 100%; background: var(--pop); border: 1px solid var(--line); border-radius: var(--r-pop); box-shadow: 0 24px 64px rgba(0, 0, 0, .4); overflow: hidden; }
.qn-search-list { max-height: 52vh; overflow-y: auto; padding-bottom: 6px; }
:global(.qn.tone .qn-search) { border-color: transparent; }
</style>
