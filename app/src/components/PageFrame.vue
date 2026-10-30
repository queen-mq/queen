<template>
  <!--
    The frame of a page that is also a tab of a queue (Consumer groups,
    Supervisors). With a queue picked (?queue=) it is that queue's tab: the
    Queues section's list and header, and the page draws no head of its own.
    Without one it is its own page, with its own list on the left.
  -->
  <QueuesLayout v-if="inQueue" :live="live">
    <template v-if="$slots.actions" #actions><slot name="actions" /></template>
    <slot />
  </QueuesLayout>
  <SubSidebar v-else :label="label" :current="current">
    <template #side><slot name="side" /></template>
    <slot />
  </SubSidebar>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'

import QueuesLayout from '@/components/QueuesLayout.vue'
import SubSidebar from '@/components/SubSidebar.vue'
import { queueTabOf } from '@/composables/navigation'

defineProps({
  /** The page's own list, for screen readers. */
  label: { type: String, required: true },
  /** As QueuesLayout's: only on a page that really polls. */
  live: { type: String, default: '' },
  /** What is picked, for the phone's list bar (SubSidebar). */
  current: { type: String, default: '' },
})

const route = useRoute()
const inQueue = computed(() => Boolean(queueTabOf(route)))
</script>
