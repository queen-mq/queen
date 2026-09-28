<template>
  <!--
    The one tools row a page may have, only when it filters or re-orders a
    list: what to show on the left (search first, then filters, then
    toggles), how to show it on the right (sort, group, view). Every control
    carries its own label inside it, so nothing floats above the row.
  -->
  <div class="page-tools">
    <div class="tools-filters"><slot /></div>
    <div v-if="$slots.view" class="tools-view"><slot name="view" /></div>
  </div>
</template>
