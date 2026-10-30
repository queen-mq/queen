<template>
  <!--
    The head of the right column beside a sub sidebar, the Queues section's
    header line (QueuesLayout) for the pages that are not a queue's: what is
    picked in the list, a few facts about it as chips, the buttons on the
    right; tabs under it when the page has views. Settings, System, Members,
    API keys and Users.

      <PaneHead title="All nodes" :facts="[{ k: 'Nodes', v: '3' }]">
        <template #actions><button class="btn btn-primary">Add</button></template>
      </PaneHead>

    A fact is `{ k, v, title }`; without `k` it is one word or two ("Not
    created yet"). A tab is `{ key, label, to, mark, why }`: a router link,
    `mark` 'warn' or 'bad' only when something there needs you.
  -->
  <header class="ph">
    <div class="ph-line">
      <h1 class="ph-title" :title="title">{{ title }}</h1>
      <div v-if="facts.length || $slots.facts" class="ph-facts">
        <span v-for="f in facts" :key="`${f.k || ''}:${f.v}`" class="ph-fact" :title="f.title || undefined">
          <span v-if="f.k" class="ph-k">{{ f.k }}</span>{{ f.v }}
        </span>
        <slot name="facts" />
      </div>
      <div v-if="$slots.actions" class="ph-actions"><slot name="actions" /></div>
    </div>
    <nav v-if="tabs.length" class="ph-tabs" :aria-label="tabsLabel || undefined">
      <router-link
        v-for="t in tabs"
        :key="t.key"
        :to="t.to"
        class="ph-tab"
        :class="{ on: t.key === tab }"
        :aria-current="t.key === tab ? 'page' : undefined"
        :title="t.why || undefined"
      >
        {{ t.label }}
        <template v-if="t.mark">
          <span class="g" :class="t.mark" aria-hidden="true" />
          <span class="sr-only">, needs attention</span>
        </template>
      </router-link>
    </nav>
  </header>
</template>

<script setup>
defineProps({
  /** What is picked in the list beside: "Lines", "All nodes", a queue. */
  title: { type: String, required: true },
  /** `[{ k, v, title }]`: two or three, the same on every tab. */
  facts: { type: Array, default: () => [] },
  /** `[{ key, label, to, mark, why }]`; none for a page with one view. */
  tabs: { type: Array, default: () => [] },
  /** The key of the tab on screen. */
  tab: { type: String, default: '' },
  /** The tabs' landmark name, for screen readers. */
  tabsLabel: { type: String, default: '' },
})
</script>

<style scoped>
/* The Queues section's header line and tabs (QueuesLayout), so a page here
   and a queue's tab start at the same place and read the same way. */
.ph {
  max-width: 1560px; margin: 0 auto;
  padding: 18px 20px 0;
}
.ph-line { display: flex; align-items: center; gap: 10px 16px; flex-wrap: wrap; min-height: 32px; }
.ph-title {
  margin: 0; min-width: 0; max-width: 100%;
  font-size: 18px; font-weight: 600; letter-spacing: -0.01em; color: var(--text-hi);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
/* Facts are words beside the title, not boxes (as the Queues header's). */
.ph-facts { display: flex; align-items: baseline; gap: 4px 16px; flex-wrap: wrap; min-width: 0; }
.ph-fact {
  display: inline-flex; align-items: baseline; gap: 5px; max-width: 100%;
  font-size: 12.5px; color: var(--text-hi); font-variant-numeric: tabular-nums;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.ph-k { color: var(--text-low); }
.ph-actions { margin-left: auto; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }

.ph-tabs {
  display: flex; gap: 2px; flex-wrap: wrap;
  margin-top: 14px; border-bottom: 1px solid var(--bd);
}
.ph-tab {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 8px 12px; margin-bottom: -1px;
  border-bottom: 2px solid transparent;
  font-size: 13px; color: var(--text-mid); text-decoration: none; white-space: nowrap;
}
.ph-tab:hover { color: var(--text-hi); }
.ph-tab.on { color: var(--text-hi); font-weight: 500; border-bottom-color: var(--text-hi); }

/* Narrow: the tabs scroll sideways rather than wrap into a second row. */
@media (max-width: 1100px) {
  .ph-tabs { flex-wrap: nowrap; overflow-x: auto; scrollbar-width: none; }
  .ph-tabs::-webkit-scrollbar { display: none; }
}
@media (max-width: 640px) {
  .ph { padding: 14px 12px 0; }
  .ph-title { font-size: 17px; }
  .ph-actions { margin-left: 0; width: 100%; }
}
</style>
