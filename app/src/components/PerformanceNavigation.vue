<template>
  <nav class="page-tools" aria-label="Performance views">
    <div class="seg">
      <RouterLink v-for="tab in tabs" :key="tab.path" class="btn btn-ghost" :class="{ on: route.path === tab.path }"
        :aria-current="route.path === tab.path ? 'page' : undefined" :to="{ path: tab.path, query: route.query }">{{ tab.label }}</RouterLink>
    </div>
  </nav>
</template>
<script setup>
import { useRoute } from 'vue-router'
const route = useRoute()
const tabs = [{ path: '/analytics', label: 'Trends' }, { path: '/operations', label: 'Operations' }]
</script>
<style scoped>
.seg a { padding: 3px 10px; min-height: auto; border-radius: var(--r-chip); font-size: 11.5px; }
.seg a.on { color: var(--accent-text); background: var(--selected); }
</style>
