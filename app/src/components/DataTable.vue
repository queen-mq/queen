<template>
  <div class="card">
    <div class="card-header" v-if="title">
      <h3>{{ title }}</h3>
      <span v-if="subtitle" class="muted">{{ subtitle }}</span>
    </div>

    <div v-if="loading" class="dt-skel">
      <div v-for="i in pageSize" :key="i" class="skeleton" />
    </div>

    <table v-else class="t">
      <thead>
        <tr>
          <th
            v-for="col in columns"
            :key="col.key"
            :class="{ 'dt-sort': col.sortable, 'num': col.align === 'right' }"
            @click="col.sortable && toggleSort(col.key)"
          >
            {{ col.label }}
            <svg
              v-if="col.sortable"
              class="dt-sort-ico"
              :class="{ 'is-on': sortKey === col.key, 'is-desc': sortKey === col.key && sortDir === 'desc' }"
              viewBox="0 0 16 16" width="10" height="10" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"
            >
              <polyline points="4 6 8 2 12 6" />
              <polyline points="4 10 8 14 12 10" />
            </svg>
          </th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="(row, idx) in paginatedData"
          :key="idx"
          :class="{ 'dt-clickable': clickable }"
          @click="clickable && $emit('row-click', row)"
        >
          <td
            v-for="col in columns"
            :key="col.key"
            :class="{ 'num': col.align === 'right' }"
          >
            <slot :name="col.key" :value="getNestedValue(row, col.key)" :row="row">
              <span class="font-mono" style="font-size:12px;">{{ getNestedValue(row, col.key) ?? '—' }}</span>
            </slot>
          </td>
        </tr>
        <tr v-if="paginatedData.length === 0">
          <td :colspan="columns.length" class="dt-empty">
            <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24">
              <circle cx="12" cy="12" r="9" /><path d="M8 12h8" />
            </svg>
            No data
          </td>
        </tr>
      </tbody>
    </table>

    <!-- Pagination -->
    <div v-if="totalPages > 1" class="dt-pager">
      <span class="dt-pager-info">
        <span class="mono">{{ (currentPage - 1) * pageSize + 1 }}–{{ Math.min(currentPage * pageSize, sortedData.length) }}</span>
        of <span class="mono">{{ sortedData.length }}</span>
      </span>
      <div class="dt-pager-ctrls">
        <button class="dt-pager-btn" :disabled="currentPage <= 1" @click="currentPage--" title="Previous">
          <svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 6 9 12 15 18" /></svg>
        </button>
        <span class="dt-pager-page mono">{{ currentPage }} / {{ totalPages }}</span>
        <button class="dt-pager-btn" :disabled="currentPage >= totalPages" @click="currentPage++" title="Next">
          <svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 6 15 12 9 18" /></svg>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'

const props = defineProps({
  title: { type: String, default: '' },
  subtitle: { type: String, default: '' },
  columns: { type: Array, required: true },
  data: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  pageSize: { type: Number, default: 10 },
  clickable: { type: Boolean, default: false },
})

defineEmits(['row-click'])

const sortKey = ref(null)
const sortDir = ref('asc')
const currentPage = ref(1)

watch(() => props.data, () => { currentPage.value = 1 })

const toggleSort = (key) => {
  if (sortKey.value === key) { sortDir.value = sortDir.value === 'asc' ? 'desc' : 'asc' }
  else { sortKey.value = key; sortDir.value = 'asc' }
}

const getNestedValue = (obj, path) => {
  return path.split('.').reduce((o, k) => o?.[k], obj)
}

const sortedData = computed(() => {
  if (!sortKey.value) return props.data
  return [...props.data].sort((a, b) => {
    const va = getNestedValue(a, sortKey.value)
    const vb = getNestedValue(b, sortKey.value)
    const cmp = typeof va === 'number' ? va - vb : String(va || '').localeCompare(String(vb || ''))
    return sortDir.value === 'asc' ? cmp : -cmp
  })
})

const totalPages = computed(() => Math.ceil(sortedData.value.length / props.pageSize))

const paginatedData = computed(() => {
  const start = (currentPage.value - 1) * props.pageSize
  return sortedData.value.slice(start, start + props.pageSize)
})
</script>

<style scoped>
.dt-skel {
  padding: 10px 14px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.dt-skel .skeleton {
  height: 28px;
  border-radius: var(--r-control);
}

/* Sortable header */
.dt-sort {
  cursor: pointer;
  user-select: none;
  position: relative;
  padding-right: 22px;
  transition: color 0.1s;
}
.dt-sort:hover { color: var(--text-hi); }
.dt-sort-ico {
  position: absolute;
  right: 8px;
  top: 50%;
  transform: translateY(-50%);
  color: var(--text-faint);
  opacity: 0.6;
  transition: opacity 0.1s, color 0.1s, transform 0.15s;
}
.dt-sort:hover .dt-sort-ico { opacity: 1; color: var(--text-mid); }
.dt-sort-ico.is-on { color: var(--text-hi); opacity: 1; }
.dt-sort-ico.is-on polyline:first-child { stroke: var(--text-hi); }
.dt-sort-ico.is-on:not(.is-desc) polyline:last-child { opacity: 0.3; }
.dt-sort-ico.is-on.is-desc polyline:first-child { opacity: 0.3; }

/* Row hover */
.dt-clickable { cursor: pointer; }

/* Num column alignment */
.t th.num, .t td.num { text-align: right; }

/* Empty state */
.dt-empty {
  text-align: center;
  color: var(--text-low);
  padding: 24px 16px !important;
  font-size: 12px;
}
.dt-empty svg { vertical-align: -2px; margin-right: 6px; color: var(--text-faint); }

/* Pager */
.dt-pager {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 14px;
  border-top: 1px solid var(--bd);
  font-size: 11.5px;
  color: var(--text-low);
}
.dt-pager-info .mono { color: var(--text-hi); }
.dt-pager-ctrls {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}
.dt-pager-page {
  color: var(--text-mid);
  font-size: 11px;
  min-width: 42px;
  text-align: center;
  font-variant-numeric: tabular-nums;
}
.dt-pager-btn {
  width: 22px; height: 22px;
  display: inline-flex; align-items: center; justify-content: center;
  background: transparent;
  border: 1px solid var(--bd);
  border-radius: var(--r-control);
  color: var(--text-mid);
  cursor: pointer;
  transition: color 0.1s, background 0.1s, border-color 0.1s;
}
.dt-pager-btn:hover:not(:disabled) {
  color: var(--text-hi);
  background: var(--ink-4);
  border-color: var(--bd-hi);
}
.dt-pager-btn:disabled { opacity: 0.35; cursor: not-allowed; }
</style>
