<script setup>
// What is coming, while it comes: flat bars the size of the rows that will
// take their place, so nothing jumps when the answer lands. They show only
// after 300 ms (a quicker answer would only flash them) and they stay still:
// no shimmer, no pulse. The words beside them ("Reading the dead letters…")
// say what is being read; the bars are hidden from screen readers.
defineProps({
  rows: { type: Number, default: 3 },
  // drawn as a card: filled in tone areas, like the cards it stands for
  card: { type: Boolean, default: false },
  widths: { type: Array, default: () => ['92%', '64%', '80%', '56%', '72%'] },
})
</script>

<template>
  <div class="qn-skel" :class="{ 'qn-card': card }" aria-hidden="true">
    <span v-for="i in rows" :key="i" class="qn-skel-bar" :style="{ width: widths[(i - 1) % widths.length] }" />
  </div>
</template>

<style scoped>
.qn-skel { display: grid; gap: 14px; padding: 14px 0; opacity: 0; animation: qn-skel-show 0s 300ms forwards; }
.qn-skel.qn-card { padding: 18px; }
.qn-skel-bar { display: block; height: 10px; border-radius: 5px; background: var(--ctl); }
@keyframes qn-skel-show { to { opacity: 1; } }
</style>
