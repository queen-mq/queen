<script setup>
// The way up from a page one level down, before its title: "Queues /", then
// the title, which names the item. The band shows the name of the app, not
// the path, so this link is the page's way back to its list.
defineProps({
  to: { type: [String, Object], required: true },
  label: { type: String, required: true },
})
</script>

<template>
  <span class="qn-up">
    <RouterLink :to="to" class="qn-t2">{{ label }}</RouterLink>
    <span class="qn-t3" aria-hidden="true">/</span>
  </span>
</template>

<style scoped>
/* The title's size: the path is one line, the parent quieter by colour only. */
.qn-up { display: inline-flex; align-items: baseline; gap: 8px; font-size: var(--qn-title); white-space: nowrap; }
</style>
