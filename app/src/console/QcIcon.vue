<script setup>
// The rail's icons, drawn for the console (18 px, stroke 1.4).
defineProps({ name: { type: String, required: true }, size: { type: Number, default: 18 } })
</script>

<template>
  <svg :width="size" :height="size" viewBox="0 0 18 18" aria-hidden="true" class="qn-ico">
    <template v-if="name === 'overview'">
      <path d="M2.5 10.5 L4.6 4 H13.4 L15.5 10.5 V14.5 H2.5 Z" />
      <path d="M2.5 10.5 H6.3 L7.3 12.2 H10.7 L11.7 10.5 H15.5" />
    </template>
    <template v-else-if="name === 'queues'">
      <rect x="3" y="3.5" width="12" height="3" rx="0.8" />
      <rect x="3" y="7.5" width="12" height="3" rx="0.8" />
      <rect x="3" y="11.5" width="12" height="3" rx="0.8" />
    </template>
    <template v-else-if="name === 'streams'">
      <path d="M1.5 7 Q4 4.5 6.5 7 T11.5 7 T16.5 7" />
      <path d="M1.5 12 Q4 9.5 6.5 12 T11.5 12 T16.5 12" />
    </template>
    <template v-else-if="name === 'dlq'">
      <rect x="1.5" y="3" width="12" height="9" rx="1" />
      <polyline points="2,3.8 7.5,8 13,3.8" />
      <line x1="13.2" y1="13.2" x2="16.8" y2="16.8" />
      <line x1="16.8" y1="13.2" x2="13.2" y2="16.8" />
    </template>
    <template v-else-if="name === 'workload'">
      <rect x="2.5" y="9" width="3" height="6" rx="0.6" />
      <rect x="7.5" y="3.5" width="3" height="11.5" rx="0.6" />
      <rect x="12.5" y="6.5" width="3" height="8.5" rx="0.6" />
    </template>
    <template v-else-if="name === 'entities'">
      <line x1="5" y1="3" x2="5" y2="15" />
      <circle cx="5" cy="4.5" r="1.4" class="fill" />
      <circle cx="5" cy="9" r="1.4" class="fill" />
      <circle cx="5" cy="13.5" r="1.4" class="fill" />
      <line x1="8.5" y1="4.5" x2="15" y2="4.5" />
      <line x1="8.5" y1="9" x2="13" y2="9" />
      <line x1="8.5" y1="13.5" x2="14" y2="13.5" />
    </template>
    <template v-else-if="name === 'state'">
      <circle cx="6" cy="9" r="3.2" />
      <polyline points="9.2,9 16,9 16,11.8" />
      <line x1="13.2" y1="9" x2="13.2" y2="11.2" />
    </template>
    <template v-else-if="name === 'cluster'">
      <circle cx="9" cy="4" r="1.9" />
      <circle cx="4" cy="13.5" r="1.9" />
      <circle cx="14" cy="13.5" r="1.9" />
      <line x1="8.1" y1="5.7" x2="4.9" y2="11.8" />
      <line x1="9.9" y1="5.7" x2="13.1" y2="11.8" />
      <line x1="5.9" y1="13.5" x2="12.1" y2="13.5" />
    </template>
    <template v-else-if="name === 'settings'">
      <g transform="scale(1.125)" style="stroke-width: 1.15">
        <polygon points="13.02,6.65 15.13,7.00 15.13,9.00 13.02,9.35 12.50,10.60 13.75,12.33 12.33,13.75 10.60,12.50 9.35,13.02 9.00,15.13 7.00,15.13 6.65,13.02 5.40,12.50 3.67,13.75 2.25,12.33 3.50,10.60 2.98,9.35 0.87,9.00 0.87,7.00 2.98,6.65 3.50,5.40 2.25,3.67 3.67,2.25 5.40,3.50 6.65,2.98 7.00,0.87 9.00,0.87 9.35,2.98 10.60,3.50 12.33,2.25 13.75,3.67 12.50,5.40" />
        <circle cx="8" cy="8" r="2.3" />
      </g>
    </template>
    <template v-else-if="name === 'search'">
      <circle cx="7.5" cy="7.5" r="5" />
      <line x1="11.3" y1="11.3" x2="15.5" y2="15.5" />
    </template>
    <template v-else-if="name === 'chevron'">
      <polyline points="5,7 9,11 13,7" />
    </template>
  </svg>
</template>

<style scoped>
.qn-ico { fill: none; stroke: currentColor; stroke-width: 1.4; stroke-linecap: round; stroke-linejoin: round; flex: 0 0 auto; }
.qn-ico .fill { fill: currentColor; stroke: none; }
</style>
