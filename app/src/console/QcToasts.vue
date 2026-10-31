<script setup>
import { toasts, dismiss } from './toasts'
</script>

<template>
  <div class="qn-toasts" aria-live="polite">
    <TransitionGroup name="qn-fade">
      <div v-for="t in toasts" :key="t.id" class="qn-toast" :class="t.tone" role="status">
        <div style="flex: 1 1 0; min-width: 0">
          <p style="font-weight: 500">{{ t.text }}</p>
          <p v-if="t.sub" class="qn-t2" style="font-size: 12px; margin-top: 2px">{{ t.sub }}</p>
        </div>
        <button type="button" class="qn-btn3" aria-label="Close" style="height: 24px; padding: 0 6px" @click="dismiss(t.id)">✕</button>
      </div>
    </TransitionGroup>
  </div>
</template>

<style scoped>
.qn-toasts { position: fixed; left: calc(var(--rail-w) + 24px); bottom: 24px; z-index: 50; display: flex; flex-direction: column; gap: 8px; max-width: 440px; }
.qn-toast { display: flex; align-items: flex-start; gap: 10px; background: var(--pop); border: 1px solid var(--line); border-radius: var(--r-pop); padding: 12px 10px 12px 14px; box-shadow: 0 12px 32px rgba(0, 0, 0, .28); }
.qn-toast.red { border-color: var(--red); }
:global(.qn.tone .qn-toast) { border-color: transparent; }
@media (max-width: 640px) { .qn-toasts { left: 16px; right: 16px; bottom: 16px; } }
</style>
