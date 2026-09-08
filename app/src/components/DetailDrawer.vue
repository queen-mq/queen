<template>
  <Teleport to="body">
    <div v-if="open" class="modal-backdrop" @click="emit('close')"></div>

    <aside
      v-if="open"
      class="drawer-panel"
      :class="{ 'detail-drawer-wide': $slots.secondary }"
      role="dialog"
      aria-modal="true"
      :aria-label="title"
    >
      <div class="card-header detail-drawer-header">
        <slot name="leading"></slot>
        <h3>{{ title }}</h3>
        <span v-if="subtitle" class="card-sub font-mono" :title="subtitle">{{ subtitle }}</span>
        <div v-if="$slots.actions" class="detail-drawer-actions">
          <slot name="actions"></slot>
        </div>
        <button
          class="btn btn-ghost btn-icon modal-close"
          :aria-label="`Close ${title}`"
          @click="emit('close')"
        >
          <svg style="width:18px; height:18px;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- A secondary slot is the layout contract: every drawer that supplies
           one gets the same wide, two-column shell without caller flags. -->
      <div class="card-body" :class="{ 'detail-drawer-split': $slots.secondary }">
        <template v-if="$slots.secondary">
          <div class="detail-drawer-primary"><slot></slot></div>
          <div class="detail-drawer-secondary"><slot name="secondary"></slot></div>
        </template>
        <slot v-else></slot>
      </div>

      <div v-if="$slots.footer" class="modal-foot">
        <slot name="footer"></slot>
      </div>
    </aside>
  </Teleport>
</template>

<script setup>
defineProps({
  open: { type: Boolean, required: true },
  title: { type: String, required: true },
  subtitle: { type: String, default: '' },
})

const emit = defineEmits(['close'])
</script>

<style scoped>
.detail-drawer-header h3 { flex-shrink: 0; }

.detail-drawer-header .card-sub {
  flex: 1; min-width: 0;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}

.detail-drawer-actions {
  display: flex; align-items: center; gap: 8px; flex-shrink: 0;
}

.detail-drawer-header .modal-close {
  margin-left: 0; flex-shrink: 0;
}

.detail-drawer-wide { max-width: min(1120px, calc(100vw - 40px)); }
.detail-drawer-split {
  display: grid; grid-template-columns: minmax(300px, .8fr) minmax(420px, 1.2fr);
  gap: 24px; align-items: start;
}
.detail-drawer-primary,
.detail-drawer-secondary { min-width: 0; }
.detail-drawer-secondary { padding-left: 24px; border-left: 1px solid var(--bd); }
.detail-drawer-secondary :deep(.json-viewer) { max-height: calc(100vh - 112px); }

@media (max-width: 640px) {
  .detail-drawer-header .card-sub { display: none; }
  .detail-drawer-actions { margin-left: auto; }
}

@media (max-width: 900px) {
  .detail-drawer-wide { max-width: 640px; }
  .detail-drawer-split { display: block; }
  .detail-drawer-secondary {
    margin-top: 24px; padding-top: 24px; padding-left: 0;
    border-top: 1px solid var(--bd); border-left: 0;
  }
  .detail-drawer-secondary :deep(.json-viewer) { max-height: 400px; }
}
</style>
