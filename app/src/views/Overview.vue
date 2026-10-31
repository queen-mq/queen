<script setup>
// The Overview of the new console: is anything waiting on me? One screen:
// the verdict and the cluster's traffic, then what needs you (it will not fix
// itself), what is behind but catching up, and what the cluster holds.
import { computed } from 'vue'
import ConsoleShell from '@/console/ConsoleShell.vue'
import QcChart from '@/console/QcChart.vue'
import QcSpark from '@/console/QcSpark.vue'
import { useWindow, longDate, clock } from '@/console/useWindow'
import { useOverview } from '@/console/useOverview'
import { useConsoleFrame } from '@/console/useConsoleFrame'

const { win, range, nowMs } = useWindow()
// One frame for the band and the page: the cell is read once.
const frame = useConsoleFrame()
const ov = useOverview(win, frame)

const path = [{ label: 'Overview' }]
const attention = computed(() => (ov.needs.value.length ? { tone: ov.verdict.value.tone, count: ov.needs.value.length + ov.moreNeeds.value } : null))
</script>

<template>
  <ConsoleShell :path="path" :attention="attention" :frame="frame">
    <template #banner>
      <p v-if="ov.error.value" class="ov-banner red" role="alert">
        <span class="qn-mark red" aria-hidden="true" /> {{ ov.error.value }}
      </p>
    </template>

    <div class="ov">
      <!-- the verdict, and the cluster's traffic over the window -->
      <section class="ov-hero">
        <div class="ov-verdict">
          <p class="qn-t3" style="font-size: 12px">{{ longDate(nowMs) }}, {{ clock(nowMs) }}</p>
          <h1 class="ov-title">
            <template v-if="ov.loading.value && !ov.ready.value">Looking at the cluster…</template>
            <template v-else>{{ ov.verdict.value.title }}</template>
          </h1>
          <p v-if="ov.ready.value" class="qn-t2 ov-sentence">{{ ov.verdict.value.sentence }}</p>
        </div>
        <div class="ov-traffic qn-card">
          <div class="ov-legend">
            <span v-for="l in ov.hero.value.legend" :key="l.id" class="ov-key">
              <span class="ov-swatch" :style="{ background: l.ink }" aria-hidden="true" />
              {{ l.label }} <span class="qn-num" style="color: var(--text); font-weight: 500; font-size: 13px">{{ l.value }}</span>
            </span>
            <span class="qn-t3" style="margin-left: auto">{{ win.long }}, from 0</span>
          </div>
          <QcChart
            :series="ov.hero.value.series"
            :from="win.from"
            :to="win.to"
            :height="92"
            :axis="win.axis"
            unit="/s"
            :aria-label="ov.hero.value.aria"
            @zoom="range"
          />
        </div>
      </section>

      <div class="ov-cols">
        <div class="ov-main">
          <!-- what needs you -->
          <h2 class="qn-h2">Needs you</h2>
          <p v-if="ov.ready.value && !ov.needs.value.length" class="qn-t2 ov-none">Nothing. Every queue drains on its own, and the cell is fine.</p>
          <article v-for="(n, i) in ov.needs.value" :key="n.key" class="ov-need qn-card" :class="{ extra: i >= 2 }">
            <div class="ov-need-text">
              <h3 class="ov-need-head">
                <span class="ov-badge" :class="n.tone"><span class="qn-mark" :class="n.tone" aria-hidden="true" />{{ n.badge }}</span>
                <span v-if="n.heading">{{ n.heading }}</span>
                <RouterLink v-else-if="n.link" :to="n.link" class="qn-mono">{{ n.queue }}</RouterLink>
                <template v-if="n.group">
                  <span class="qn-t3" aria-hidden="true" style="font-weight: 400">→</span>
                  <span :class="n.groupMono ? 'qn-mono' : 'qn-t2'" style="font-weight: 400">{{ n.group }}</span>
                </template>
              </h3>
              <p class="ov-need-sentence">{{ n.sentence }}</p>
              <p v-if="n.detail" class="qn-t2 ov-need-detail">{{ n.detail }}</p>
              <div class="ov-need-actions">
                <RouterLink v-for="(a, k) in n.actions" :key="k" :to="a.to" :class="k === 0 ? 'qn-btn' : 'qn-btn2'">{{ a.label }}</RouterLink>
              </div>
            </div>
            <div v-if="n.since || (n.spark && n.spark.values.length > 1)" class="ov-need-side">
              <span v-if="n.since" class="qn-num qn-t3" style="font-size: 12px">{{ n.since }}</span>
              <QcSpark v-if="n.spark && n.spark.values.length > 1" :values="n.spark.values" :kind="n.spark.kind" tone="grey" :width="180" :height="34" :label="n.spark.caption" />
              <span v-if="n.spark && n.spark.values.length > 1" class="qn-t3" style="font-size: 11px">{{ n.spark.caption }}</span>
            </div>
          </article>
          <RouterLink v-if="ov.moreNeeds.value > 0" :to="ov.moreLink.value" class="ov-more qn-t2 wide">
            {{ ov.moreNeeds.value }} more need you ›
          </RouterLink>
          <RouterLink v-if="ov.needs.value.length > 2 || ov.moreNeeds.value > 0" :to="ov.moreLink.value" class="ov-more qn-t2 narrow">
            {{ ov.moreNeeds.value + Math.max(0, ov.needs.value.length - 2) }} more need you ›
          </RouterLink>

          <!-- behind, but draining on its own -->
          <h2 class="qn-h2" style="margin-top: 22px">Behind, and catching up on their own</h2>
          <p v-if="ov.ready.value && !ov.behind.value.length" class="qn-t2 ov-none">No queue is behind.</p>
          <div v-else class="ov-behind qn-card">
            <RouterLink v-for="(b, i) in ov.behind.value" :key="b.key" :to="b.link" class="ov-brow qn-row" :class="{ extra: i >= 2 }">
              <span class="ov-bname"><span class="qn-mono">{{ b.queue }}</span> <span class="qn-t3" aria-hidden="true">→</span> <span :class="b.groupMono ? 'qn-mono qn-t2' : 'qn-t2'">{{ b.group }}</span></span>
              <span class="qn-num ov-bwait">{{ b.waiting }}</span>
              <span class="qn-t2 ov-beta">{{ b.eta }}</span>
              <QcSpark v-if="b.spark && b.spark.length > 1" :values="b.spark" :width="140" :height="24" label="Waiting, last hour" />
            </RouterLink>
          </div>
        </div>

        <!-- what the cluster holds -->
        <aside class="ov-sidecol" aria-labelledby="ov-inv-title">
          <h2 id="ov-inv-title" class="qn-h2">In this cluster</h2>
          <div class="ov-side qn-card">
            <ul class="ov-inv">
              <li v-for="r in ov.inventory.value" :key="r.id">
                <RouterLink :to="r.to" class="ov-irow qn-row" :title="r.title || undefined">
                  <span class="qn-t2">{{ r.label }}</span>
                  <span class="qn-num" style="text-align: right; font-weight: 500">{{ r.value }}</span>
                  <span class="qn-num qn-t3 ov-isub">{{ r.sub }}</span>
                </RouterLink>
              </li>
            </ul>
          </div>
        </aside>
      </div>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.ov { max-width: 1400px; margin: 0 auto; padding: 24px 32px 24px; }
.ov-banner { display: flex; align-items: center; gap: 8px; padding: 8px 24px; border-bottom: 1px solid var(--line); }

.ov-hero { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 20px 48px; padding-bottom: 22px; border-bottom: 1px solid var(--line); }
.ov-verdict { flex: 999 1 460px; min-width: 0; }
.ov-title { font-size: 30px; line-height: 1.15; font-weight: 500; letter-spacing: -0.015em; margin-top: 8px; }
.ov-sentence { font-size: 14.5px; margin-top: 10px; max-width: 640px; }
.ov-traffic { flex: 1 1 420px; min-width: 0; max-width: 520px; }
.ov-legend { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 18px; font-size: 12px; color: var(--text3); margin-bottom: 8px; }
.ov-key { display: inline-flex; align-items: center; gap: 6px; }
.ov-swatch { width: 10px; height: 2px; display: inline-block; }

.ov-cols { display: flex; flex-wrap: wrap; gap: 24px 48px; padding-top: 22px; }
.ov-main { flex: 999 1 560px; min-width: 0; }
.ov-sidecol { flex: 1 1 320px; min-width: 0; max-width: 380px; align-self: flex-start; }
.ov-none { padding: 12px 0 4px; }

.ov-need { display: flex; flex-wrap: wrap; gap: 10px 28px; padding: 14px 0 16px; border-bottom: 1px solid var(--line); }
.ov-need-text { flex: 999 1 380px; min-width: 0; }
.ov-need-head { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 10px; font-size: 14px; font-weight: 500; }
.ov-badge { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; }
.ov-badge.amber { color: var(--amber); }
.ov-badge.red { color: var(--red); }
.ov-need-sentence { font-size: 14.5px; line-height: 1.45; margin-top: 6px; }
.ov-need-detail { margin-top: 3px; max-width: 680px; }
.ov-need-side { flex: 0 1 200px; min-width: 0; display: flex; flex-direction: column; align-items: flex-end; gap: 6px; }
.ov-need-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
.ov-need-actions a { height: 28px; padding: 0 10px; font-size: 12.5px; }
.ov-more { display: inline-block; margin-top: 10px; }
.ov-more.narrow { display: none; }

.ov-behind { margin-top: 8px; }
.ov-brow { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 20px; padding: 8px 8px; margin: 0 -8px; border-bottom: 1px solid var(--line2); }
.ov-bname { flex: 1 1 240px; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ov-bwait { flex: 0 0 120px; text-align: right; }
.ov-beta { flex: 0 0 160px; }

.ov-inv { margin-top: 6px; }
.ov-irow { display: grid; grid-template-columns: auto minmax(0, 1fr) minmax(6.5em, auto); align-items: baseline; gap: 12px; padding: 7px 8px; margin: 0 -8px; border-bottom: 1px solid var(--line2); white-space: nowrap; }
.ov-isub { text-align: right; font-size: 12px; }

/* tone: areas are filled, so rows lose their lines. The verdict and the
   traffic are one band; on a wide screen the band and the cards below share
   one grid (a --ov-side right column, --ov-gap apart), so every card edge
   lies on the same lines and the chart starts on the cluster card's edge. */
:global(.qn.tone .ov) { --ov-side: 380px; --ov-gap: 24px; --ov-pad: 20px; }
:global(.qn.tone .ov-hero) { border-bottom: 0; padding: 20px var(--ov-pad); background: var(--card); border-radius: var(--r-card); }
:global(.qn.tone .ov-hero .ov-traffic) { background: transparent; padding: 0; }
:global(.qn.tone .ov-cols) { gap: var(--ov-gap); padding-top: 24px; }
:global(.qn.tone .ov-need) { margin-top: 10px; padding: 16px var(--ov-pad); }
:global(.qn.tone .ov-behind) { margin-top: 10px; padding: 6px var(--ov-pad); }
:global(.qn.tone .ov-side) { margin-top: 10px; padding: 10px var(--ov-pad); }
:global(.qn.tone .ov-side .ov-inv) { margin-top: 0; }
@media (min-width: 1100px) {
  :global(.qn.tone .ov-hero) {
    display: grid; align-items: center;
    grid-template-columns: minmax(0, 1fr) calc(var(--ov-side) - var(--ov-pad));
    column-gap: var(--ov-gap);
  }
  :global(.qn.tone .ov-legend) { column-gap: 14px; }
  :global(.qn.tone .ov-traffic) { max-width: none; }
  :global(.qn.tone .ov-cols) { display: grid; grid-template-columns: minmax(0, 1fr) var(--ov-side); }
  :global(.qn.tone .ov-sidecol) { max-width: none; }
}

/* one screen: on short screens two of each, the rest behind "more" */
@media (max-height: 780px) {
  .extra { display: none !important; }
  .ov-more.wide { display: none; }
  .ov-more.narrow { display: inline-block; }
  .ov { padding-top: 18px; }
  .ov-title { font-size: 26px; }
  .ov-hero { padding-bottom: 16px; }
  .ov-cols { padding-top: 16px; }
  :global(.qn.tone .ov-hero) { padding-top: 16px; padding-bottom: 16px; }
  :global(.qn.tone .ov-cols) { padding-top: 16px; }
}
@media (max-width: 640px) {
  .ov { padding: 18px 16px 32px; }
  .ov-title { font-size: 24px; }
  .ov-sidecol { max-width: none; }
  :global(.qn.tone .ov) { --ov-pad: 16px; }
  .ov-need-side { align-items: flex-start; flex-basis: 100%; }
}
</style>
