<template>
  <div class="toast-host" role="status" aria-live="polite">
    <div
      v-for="t in toasts"
      :key="t.id"
      class="toast"
      :class="`toast-${t.level}`"
      @click="dismiss(t.id)"
    >
      <span class="toast-dot" />
      <div class="toast-body">
        <div class="toast-title">
          {{ t.title }}
          <span v-if="t.count > 1" class="toast-count">×{{ t.count }}</span>
        </div>
        <div v-if="t.detail" class="toast-detail">{{ t.detail }}</div>
      </div>
      <button class="toast-close" title="Dismiss" @click.stop="dismiss(t.id)">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path stroke-linecap="round" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  </div>
</template>

<script setup>
import { useUiStore } from '@/stores/ui'

const { toasts, dismiss } = useUiStore()
</script>
