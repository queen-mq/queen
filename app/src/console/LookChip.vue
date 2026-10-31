<script setup>
// The band's theme switch: the theme (dark, soft, light, the system's) and
// how areas are told apart (lines or tone), remembered in this browser.
import { computed, ref } from 'vue'
import QcPopover from './QcPopover.vue'
import QcSeg from './QcSeg.vue'
import { theme, themePick, areas, THEME_OPTIONS, AREA_OPTIONS } from './useLook'

const open = ref(false)
const label = computed(() => `Theme: ${THEME_OPTIONS.find((o) => o.value === themePick.value)?.label || 'Dark'}, areas by ${areas.value}`)
</script>

<template>
  <QcPopover v-model:open="open" side="bottom-end" :width="300" label="Theme">
    <template #trigger="{ toggle }">
      <button type="button" class="qn-chip qn-look" aria-haspopup="dialog" :aria-expanded="open" :aria-label="label" :title="label" @click="toggle">
        <svg width="16" height="16" viewBox="0 0 18 18" aria-hidden="true" class="qn-look-ico">
          <template v-if="theme === 'light'">
            <circle cx="9" cy="9" r="3.4" />
            <path d="M9 1.8v1.8M9 14.4v1.8M1.8 9h1.8M14.4 9h1.8M3.9 3.9l1.3 1.3M12.8 12.8l1.3 1.3M3.9 14.1l1.3-1.3M12.8 5.2l1.3-1.3" />
          </template>
          <path v-else d="M14.6 11.2A6.3 6.3 0 0 1 6.8 3.4a6.3 6.3 0 1 0 7.8 7.8Z" />
        </svg>
      </button>
    </template>
    <div style="padding: 8px 6px 6px">
      <p class="qn-t3" style="font-size: 12px; margin-bottom: 6px">Theme</p>
      <QcSeg v-model="themePick" :options="THEME_OPTIONS" label="Theme" />
      <p class="qn-t3" style="font-size: 12px; margin: 14px 0 6px">Areas</p>
      <QcSeg v-model="areas" :options="AREA_OPTIONS" label="Areas" />
    </div>
  </QcPopover>
</template>

<style scoped>
.qn-look { width: 32px; justify-content: center; padding: 0; }
.qn-look-ico { fill: none; stroke: currentColor; stroke-width: 1.4; stroke-linecap: round; stroke-linejoin: round; }
</style>
