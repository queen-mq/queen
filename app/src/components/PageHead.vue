<template>
  <!--
    The head of every page, one anatomy everywhere:

      [lead] Title  sub ·············· live · range · switches · actions

    Left: what the page is, and the one fact that qualifies it (a count, the
    window, the cell). Right, always in this order: the live tick (only on a
    page that polls), the time range (only on a page with a window), switches
    that re-shape the whole page, then the buttons — the primary one last.
    Whose data it is is the sidebar's to say, once, not every page's.
  -->
  <header class="page-head">
    <div class="page-id">
      <slot name="lead" />
      <h1><slot name="title">{{ title }}</slot></h1>
      <span v-if="sub || $slots.sub" class="page-sub"><slot name="sub">{{ sub }}</slot></span>
    </div>
    <div v-if="live || $slots.range || $slots.switch || $slots.actions" class="page-controls">
      <span v-if="live" class="live-tick" :title="liveTitle">
        <span class="live-dot" aria-hidden="true" />
        <span>live · {{ live }}</span>
      </span>
      <slot name="range" />
      <slot name="switch" />
      <slot name="actions" />
    </div>
  </header>
</template>

<script setup>
defineProps({
  title: { type: String, required: true },
  /** The one fact beside the title: a count, the window, the cell. */
  sub: { type: String, default: '' },
  /** "just now", "12s ago" — only on a page that really polls. */
  live: { type: String, default: '' },
  liveTitle: { type: String, default: 'This page refreshes itself every 30 seconds' },
})
</script>
