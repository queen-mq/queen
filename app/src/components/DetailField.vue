<template>
  <div class="detail-field">
    <div v-if="boxed" class="detail-field-header">
      <label class="label-xs">{{ label }}</label>
      <button
        v-if="copyable && canCopy"
        class="btn btn-ghost detail-field-copy"
        :aria-label="`Copy ${label}`"
        @click="copy"
      >
        {{ copied ? 'Copied!' : 'Copy' }}
      </button>
    </div>
    <label v-else class="label-xs">{{ label }}</label>
    <div :class="copyable && !boxed ? 'detail-field-copyable' : 'detail-field-value-wrap'">
      <span
        class="detail-field-value"
        :class="[`detail-field-${tone}`, { 'font-mono': mono, 'detail-field-boxed': boxed }]"
        :title="title"
      >{{ displayValue }}</span>
      <button
        v-if="copyable && !boxed && canCopy"
        class="btn btn-ghost detail-field-copy"
        :aria-label="`Copy ${label}`"
        @click="copy"
      >
        {{ copied ? 'Copied!' : 'Copy' }}
      </button>
    </div>
    <slot></slot>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, ref } from 'vue'
import { useToast } from '@/composables/useToast'

const props = defineProps({
  label: { type: String, required: true },
  value: { type: [String, Number], default: '' },
  mono: { type: Boolean, default: false },
  copyable: { type: Boolean, default: false },
  boxed: { type: Boolean, default: false },
  tone: {
    type: String,
    default: 'mid',
    validator: value => ['high', 'mid', 'accent', 'danger'].includes(value),
  },
  title: { type: String, default: '' },
})

const { notifyError } = useToast()
const copied = ref(false)
let resetTimer = null

const displayValue = computed(() => props.value ?? '')
const canCopy = computed(() => props.value !== null && props.value !== undefined && props.value !== '')

const copy = async () => {
  try {
    await navigator.clipboard.writeText(String(props.value))
    copied.value = true
    clearTimeout(resetTimer)
    resetTimer = setTimeout(() => { copied.value = false }, 2000)
  } catch {
    notifyError('Could not copy to the clipboard', 'Copy failed')
  }
}

onBeforeUnmount(() => clearTimeout(resetTimer))
</script>

<style scoped>
.detail-field { display: flex; flex-direction: column; gap: 4px; }
.detail-field-value-wrap { min-width: 0; }
.detail-field-value {
  display: block; min-width: 0;
  font-size: 13px; word-break: break-word;
}
.detail-field-value.font-mono { font-size: 12px; word-break: break-all; }
.detail-field-high { color: var(--text-hi); font-weight: 500; }
.detail-field-mid { color: var(--text-mid); }
.detail-field-accent { color: var(--ice-400); }
.detail-field-danger { color: var(--ember-400); }

.detail-field-copyable { display: flex; align-items: flex-start; gap: 8px; }
.detail-field-copyable .detail-field-value { flex: 1; }
.detail-field-copy {
  flex-shrink: 0; padding: 1px 6px; font-size: 10.5px;
}
.detail-field-header {
  display: flex; align-items: center; justify-content: space-between; gap: 8px;
  margin-bottom: 4px;
}
.detail-field-boxed {
  max-height: 180px; overflow: auto;
  padding: 14px 16px; border: 1px solid var(--bd); border-radius: var(--r-card);
  background: var(--recessed); line-height: 1.6; white-space: pre-wrap;
  overflow-wrap: anywhere;
}
</style>
