<script setup>
// The partitions holding the most, 20 at a time. A partition is an entity:
// its order holds, and one stuck message stops it.
import { computed, ref, watch } from 'vue'
import { span } from '../useOverview'
import { fmt } from '../useQueuePage'

const props = defineProps({
  data: { type: Object, default: null }, // resources/partitions?queue=&limit=500
  total: { type: Number, default: null }, // the queue's partition count
})

const filter = ref('')
const page = ref(0)
const PAGE = 20
watch(filter, () => { page.value = 0 })

const all = computed(() => props.data?.partitions || [])
const shown = computed(() => {
  const f = filter.value.trim().toLowerCase()
  return f ? all.value.filter((p) => String(p.partition).toLowerCase().includes(f)) : all.value
})
const rows = computed(() => shown.value.slice(page.value * PAGE, page.value * PAGE + PAGE))
const range = computed(() => {
  const n = shown.value.length
  if (!n) return ''
  const a = page.value * PAGE + 1
  const b = Math.min(n, a + PAGE - 1)
  return `${fmt(a)}–${fmt(b)} of ${fmt(n)}${filter.value ? ' matching' : ''}`
})
const note = computed(() => {
  const d = props.data
  if (!d) return ''
  const total = props.total ?? d.walked
  return all.value.length < (total || 0)
    ? `The ${fmt(all.value.length)} holding the most of ${fmt(total)}, as the slowest group sees them.`
    : `All ${fmt(all.value.length)}, as the slowest group sees them.`
})
</script>

<template>
  <div class="qpp">
    <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px">
      <label class="qn-field" style="width: 280px; max-width: 100%">
        <svg width="14" height="14" viewBox="0 0 18 18" aria-hidden="true" style="fill: none; stroke: currentColor; stroke-width: 1.4"><circle cx="7.5" cy="7.5" r="5" /><line x1="11.3" y1="11.3" x2="15.5" y2="15.5" /></svg>
        <span class="qn-vh">Filter by partition</span>
        <input v-model="filter" class="qn-mono" type="text" placeholder="Filter by partition" spellcheck="false" />
      </label>
      <span class="qn-read">{{ note }}</span>
    </div>
    <p v-if="!data" class="qn-t2" style="padding: 18px 0">Reading its partitions…</p>
    <p v-else-if="!shown.length" class="qn-t2" style="padding: 18px 0">
      <template v-if="filter">Nothing matches “{{ filter }}”. <button type="button" class="qn-link" @click="filter = ''">Clear the filter</button></template>
      <template v-else>This queue has no partitions yet.</template>
    </p>
    <template v-else>
      <div class="qpp-table" role="table" aria-label="Partitions" style="--cols: minmax(0, 2fr) 110px 100px 110px">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Partition</span><span role="columnheader" class="r">Waiting</span>
          <span role="columnheader" class="r">Leased</span><span role="columnheader" class="r">Oldest</span>
        </div>
        <div v-for="p in rows" :key="p.id || p.partition" class="qn-tr" role="row">
          <span role="cell" class="qn-mono" :title="p.partition">{{ p.partition }}</span>
          <span role="cell" class="r qn-num">{{ fmt(p.pending) }}</span>
          <span role="cell" class="r qn-num qn-t2">{{ fmt(p.processing) }}</span>
          <span role="cell" class="r qn-num" :class="p.lagSeconds ? '' : 'qn-t3'">{{ p.lagSeconds ? span(p.lagSeconds) : '–' }}</span>
        </div>
      </div>
      <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; margin-top: 12px">
        <span class="qn-num qn-t3">{{ range }}</span>
        <div style="flex: 1 1 0" />
        <button type="button" class="qn-btn2" :disabled="page === 0" @click="page--">Previous 20</button>
        <button type="button" class="qn-btn2" :disabled="(page + 1) * PAGE >= shown.length" @click="page++">Next 20</button>
      </div>
    </template>
    <p class="qn-read" style="margin-top: 12px">Read from <span class="qn-mono">GET /api/v1/resources/partitions?queue=…&amp;limit=500</span>. Oldest is the age of the oldest message not read.</p>
  </div>
</template>

<style scoped>
.qpp { margin-top: 14px; }
.qpp-table { margin-top: 12px; overflow-x: auto; }
</style>
