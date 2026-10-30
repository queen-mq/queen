<template>
  <!--
    A page with its own sub sidebar: the list of what the page is about on
    the left, the thing picked on the right. The list is fixed: a click moves
    the highlight and changes the right side, never the list itself. It
    lives in the page, under the top bar, so the console shell (shell.css,
    shared with Gate and Traceflow) is untouched.

    Wide: a column that stays put while the page scrolls. Narrow (up to
    1100px): the list is a sheet over the page, opened by a bar at the top of
    the page that names what is picked; a pick, Esc or the scrim closes it.
  -->
  <div class="ss-layout" :class="{ 'ss-sheet-open': sheetOpen }">
    <button type="button" class="ss-mobile-bar" :aria-expanded="sheetOpen" @click="sheetOpen = true">
      <Icon name="list" :size="16" />
      <span class="ss-mobile-label">{{ label }}</span>
      <span v-if="current" class="ss-mobile-current">{{ current }}</span>
      <Icon name="chevron-down" class="ss-mobile-chevron" :size="14" :stroke="1.8" />
    </button>
    <div v-if="sheetOpen" class="ss-scrim" aria-hidden="true" @click="sheetOpen = false" />
    <aside class="ss-side" :aria-label="label" @keydown.escape="sheetOpen = false">
      <div class="ss-sheet-head">
        <span>{{ label }}</span>
        <button type="button" class="ss-sheet-close" aria-label="Close the list" @click="sheetOpen = false">
          <Icon name="x" :size="16" />
        </button>
      </div>
      <slot name="side" />
    </aside>
    <div class="ss-main">
      <slot />
    </div>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import Icon from '@/components/Icon.vue'

defineProps({
  /** The landmark's name, and the narrow screen's bar: "Queues", "KV namespaces". */
  label: { type: String, required: true },
  /** What is picked, for the narrow screen's bar: a queue's name, "All queues". */
  current: { type: String, default: '' },
  /** Kept for callers not yet moved to the fixed list; not drawn. */
  title: { type: String, default: '' },
  meta: { type: String, default: '' },
})

const route = useRoute()
const sheetOpen = ref(false)
// A pick navigates: the sheet has done its job. The list's own filters
// (search, namespace, group by) live in the address too, and typing in them
// must not close the sheet under the finger.
const LIST_FILTERS = new Set(['find', 'namespace', 'search', 'q', 'by', 'groupBy'])
const pickKey = () => {
  const q = Object.keys(route.query).filter((k) => !LIST_FILTERS.has(k)).sort()
    .map((k) => `${k}=${JSON.stringify(route.query[k])}`).join('&')
  return `${route.path}?${q}`
}
watch(pickKey, () => { sheetOpen.value = false })
</script>

<style>
/* Global on purpose: the rows below are the vocabulary every page's list
   uses. One line per row: an attention mark (or its empty slot, so the names
   line up), the name, one number. Every row is a button with a hairline
   outline; the picked one wears the rail's yellow outline and yellow icon,
   with no fill, so "where am I" looks the same at both levels. */
.ss-layout {
  display: grid;
  grid-template-columns: 264px minmax(0, 1fr);
  min-height: 100%;
}
.ss-side {
  position: sticky; top: 0; align-self: start;
  height: calc(100vh - 52px);
  display: flex; flex-direction: column;
  overflow-y: auto;
  background: var(--sub-rail, var(--ink-1));
  border-right: 1px solid var(--bd);
}
.ss-main { min-width: 0; }
.ss-mobile-bar, .ss-sheet-head, .ss-scrim { display: none; }

.ss-tools { display: flex; flex-direction: column; gap: 6px; padding: 12px 12px 8px; }
.ss-tools .input { width: 100%; height: 32px; font-size: 12.5px; }
.ss-pair { display: flex; gap: 6px; }
.ss-pair .input { min-width: 0; }
.ss-group { display: flex; flex-direction: column; gap: 1px; padding: 0 8px 8px; }
.ss-label { padding: 10px 10px 4px; font-size: 11px; font-weight: 500; color: var(--text-low); }
/* A plain row: no frame. Hovered, a grey fill; picked, the rail's yellow
   tint with its name and icon yellow. A frame on every row turns a long list
   into a wall. */
.ss-row {
  display: flex; align-items: center; gap: 10px;
  min-height: 32px; padding: 0 10px; border-radius: 7px;
  border: 1px solid transparent; background: transparent; width: 100%; text-align: left; font: inherit; cursor: pointer;
  color: var(--text-mid); text-decoration: none; font-size: 13px;
  transition: background-color .12s, border-color .12s;
}
.ss-row:hover { background: var(--ink-3); }
.ss-row:hover .ss-name { color: var(--text-hi); }
.ss-row.on, .ss-row.on:hover { background: var(--selected); }
.ss-row.on .ss-name { color: var(--accent-text); font-weight: 500; }
.ss-row:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.ss-mark { flex: none; width: 8px; display: inline-flex; justify-content: center; }
/* A row's icon: the text's quiet grey, the name's ink on hover and when picked.
   Never a colour: a row that is trouble says so with its number. */
.ss-icon { flex: none; color: var(--text-low); }
.ss-row:hover .ss-icon { color: var(--text-hi); }
.ss-row.on .ss-icon { color: var(--accent-text); }
.ss-text { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
.ss-name { color: var(--text-mid); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ss-sub { font-size: 11px; color: var(--text-low); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ss-num { font-family: var(--font-mono); font-size: 11.5px; color: var(--text-low); font-variant-numeric: tabular-nums; }
.ss-row.on .ss-num { color: var(--text-mid); }
.ss-empty { padding: 10px 18px; font-size: 12px; color: var(--text-low); }
.ss-note { margin: auto 0 0; padding: 12px 16px 16px; font-size: 11px; line-height: 1.5; color: var(--text-low); border-top: 1px solid var(--bd); }

/* Narrow: the list is a sheet, opened from a bar at the top of the page. */
@media (max-width: 1100px) {
  /* The bar takes its own height, the page the rest: a short page must not
     stretch the bar's row into a gap under it. */
  .ss-layout { grid-template-columns: minmax(0, 1fr); grid-template-rows: auto 1fr; align-content: start; }
  .ss-mobile-bar {
    display: flex; align-items: center; gap: 8px;
    margin: 12px 12px 0; height: 40px; padding: 0 12px;
    border: 1px solid var(--bd); border-radius: var(--r-control); background: var(--frame, var(--ink-1));
    color: var(--text-mid); font: inherit; font-size: 13px; text-align: left; cursor: pointer;
  }
  .ss-mobile-label { color: var(--text-low); flex: none; }
  .ss-mobile-current { flex: 1; min-width: 0; color: var(--text-hi); font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ss-mobile-chevron { margin-left: auto; color: var(--text-low); flex: none; }
  .ss-side {
    position: fixed; top: 0; bottom: 0; left: 0; z-index: 90;
    width: min(340px, 88vw); height: 100dvh;
    border-right: 1px solid var(--bd-hi);
    transform: translateX(-102%); transition: transform .2s ease;
  }
  .ss-sheet-open .ss-side { transform: none; }
  .ss-scrim { display: block; position: fixed; inset: 0; z-index: 89; background: var(--scrim, rgba(0, 0, 0, .5)); }
  .ss-sheet-head {
    display: flex; align-items: center; justify-content: space-between;
    padding: 14px 12px 4px 18px; font-size: 13px; font-weight: 600; color: var(--text-hi);
  }
  .ss-sheet-close {
    width: 32px; height: 32px; display: inline-flex; align-items: center; justify-content: center;
    border: 0; border-radius: var(--r-control); background: transparent; color: var(--text-mid); cursor: pointer;
  }
  .ss-sheet-close:hover { background: var(--ink-3); color: var(--text-hi); }
}
@media (prefers-reduced-motion: reduce) { .ss-side { transition: none; } }
</style>
