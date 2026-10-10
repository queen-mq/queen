<template>
  <section v-if="queue && supported" class="queue-navigation" aria-label="Queue workspace">
    <div class="queue-navigation-heading">
      <RouterLink v-if="origin" class="queue-return" :to="origin">← {{ originLabel }}</RouterLink>
      <span class="queue-navigation-name">Queue <strong>{{ queue }}</strong></span>
    </div>
    <nav class="queue-navigation-tabs" aria-label="Views of this queue">
      <RouterLink v-for="tab in tabs" :key="tab.view" :to="queueLocation(queue, route, tab.view)"
        :class="{ active: tab.names.includes(route.name) }" :aria-current="tab.names.includes(route.name) ? 'page' : undefined"
      >{{ tab.label }}</RouterLink>
    </nav>
  </section>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { queueOf, queueLocation, safeReturnTo } from '@/composables/navigation'
const route = useRoute(), router = useRouter()
const queue = computed(() => queueOf(route))
const tabs = [
  { view: 'overview', label: 'Overview', names: ['QueueDetail'] },
  { view: 'messages', label: 'Messages', names: ['Messages'] },
  { view: 'failed', label: 'Failed messages', names: ['DeadLetter'] },
  { view: 'scheduled', label: 'Scheduled', names: ['Timers'] },
  { view: 'consumers', label: 'Consumer groups', names: ['Consumers'] },
  { view: 'metrics', label: 'Metrics', names: ['Analytics', 'QueueOperations'] },
  { view: 'supervisors', label: 'Supervisors', names: ['Supervisors'] },
]
const supported = computed(() => tabs.some(tab => tab.names.includes(route.name)) || route.name === 'Traces')
const origin = computed(() => safeReturnTo(route.query.returnTo) || '/queues')
const originLabel = computed(() => `Back to ${router.resolve(origin.value).meta.title || 'queues'}`)
</script>

<style scoped>
.queue-navigation { margin: 16px 20px 0; padding-bottom: 12px; border-bottom: 1px solid var(--bd); }
.queue-navigation-heading { display: flex; align-items: baseline; flex-wrap: wrap; gap: 8px 20px; margin-bottom: 10px; font-size: 12px; }
.queue-return { color: var(--text-mid); }
.queue-navigation-name { color: var(--text-low); overflow-wrap: anywhere; min-width: 0; }
.queue-navigation-name strong { color: var(--text-hi); font-weight: 500; }
.queue-navigation-tabs { display: flex; flex-wrap: wrap; gap: 4px; }
.queue-navigation-tabs a { padding: 6px 10px; border-radius: var(--r-control); font-size: 12px; color: var(--text-mid); }
.queue-navigation-tabs a:hover { background: var(--ink-3); color: var(--text-hi); }
.queue-navigation-tabs a.active { background: var(--ink-3); color: var(--text-hi); box-shadow: inset 0 -2px var(--accent); }
@media (max-width: 640px) { .queue-navigation { margin: 12px 12px 0; } }
</style>
