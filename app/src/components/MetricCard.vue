<template>
  <div class="stat" :class="{ 'cursor-pointer': clickable }" @click="$emit('click')">
    <div class="stat-label">
      <component v-if="icon" :is="icon" class="w-3 h-3" />
      {{ label }}
    </div>

    <div v-if="loading" class="skeleton" style="width:80px; height:36px; margin-top:8px; border-radius:var(--r-card);" />
    <div v-else class="stat-value">
      {{ formattedValue }}<small v-if="unit">{{ unit }}</small>
    </div>

    <div class="stat-foot" v-if="subtext || trend !== null">
      <span v-if="trend !== null" class="trend" :class="trendClass">
        {{ trend > 0 ? '▲' : (trend < 0 ? '▼' : '▬') }} {{ Math.abs(trend) }}%
      </span>
      <span v-if="subtext">{{ subtext }}</span>
    </div>

    <!-- Icon badge (top-right) -->
    <div v-if="icon && !loading" class="stat-icon-badge" :class="iconColorClass">
      <component :is="icon" class="w-5 h-5" />
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { formatNumber } from '@/composables/useApi'

const props = defineProps({
  label: { type: String, required: true },
  value: { type: [Number, String], default: 0 },
  format: { type: String, default: 'number' },
  unit: { type: String, default: null },
  subtext: { type: String, default: null },
  icon: { type: [Object, Function], default: null },
  iconColor: { type: String, default: 'ice' },
  trend: { type: Number, default: null },
  // Which direction is "good" for this metric. Drives whether the trend
  // chip is green (good) or red (bad). Use 'up' for metrics where higher
  // is better (throughput, ack rate), 'down' for metrics where lower is
  // better (errors, pending, lag), or 'neutral' to suppress semantic
  // coloring entirely (always render flat).
  trendIsGoodWhen: {
    type: String,
    default: 'up',
    validator: (v) => ['up', 'down', 'neutral'].includes(v),
  },
  loading: { type: Boolean, default: false },
  clickable: { type: Boolean, default: false },
})

defineEmits(['click'])

const formattedValue = computed(() => {
  if (props.value === null || props.value === undefined) return '-'
  if (props.format === 'raw') return props.value
  return formatNumber(props.value)
})

const iconColorClass = computed(() => {
  const map = {
    ice: 'badge-ice',
    crown: 'badge-crown',
    ember: 'badge-ember',
  }
  return map[props.iconColor] || 'badge-ice'
})

const trendClass = computed(() => {
  if (props.trend === null || props.trend === 0) return 'trend-flat'
  if (props.trendIsGoodWhen === 'neutral') return 'trend-flat'
  const upIsGood = props.trendIsGoodWhen === 'up'
  const isGood = props.trend > 0 ? upIsGood : !upIsGood
  return isGood ? 'trend-up' : 'trend-down'
})
</script>

<style scoped>
/* Monochrome icon chip — no more cyan / amber / pink variants; all KPI
   icons render as neutral grey. The `iconColor` prop is still accepted
   for API compatibility but has no visible effect. Color now lives on
   the data (threshold-based .num classes), not the decoration. */
.stat-icon-badge {
  position: absolute; top: 10px; right: 10px;
  width: 22px; height: 22px; border-radius: var(--r-control);
  display: grid; place-items: center;
  background: var(--ink-3);
  color: var(--text-mid);
  border: 1px solid var(--bd);
}
.stat-icon-badge :deep(svg) { width: 13px; height: 13px; }

/* Kept as no-ops so existing :class bindings don't explode */
.badge-ice, .badge-crown, .badge-ember {
  background: var(--ink-3);
  color: var(--text-mid);
  border-color: var(--bd);
}
</style>
