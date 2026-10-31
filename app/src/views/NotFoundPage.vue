<script setup>
// An address no page answers, inside the console's frame.
import { useRoute } from 'vue-router'
import ConsoleShell from '@/console/ConsoleShell.vue'
import { useConsoleFrame } from '@/console/useConsoleFrame'

const route = useRoute()
const frame = useConsoleFrame()
</script>

<template>
  <ConsoleShell :path="[{ label: 'No such page' }]" :frame="frame">
    <div class="nf">
      <h1 class="nf-title">No such page</h1>
      <p class="qn-t2 nf-text"><span class="qn-mono">{{ route.fullPath }}</span> is not a page of this console.</p>
      <RouterLink to="/" class="qn-btn2 nf-back">Open the Overview</RouterLink>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.nf { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.nf-title { font-size: 22px; font-weight: 500; }
.nf-text { margin-top: 12px; font-size: 14.5px; overflow-wrap: anywhere; }
.nf-back { margin-top: 18px; }
@media (max-width: 640px) { .nf { padding: 18px 16px 40px; } }
</style>
