<script setup>
// The Overview of the new console: is anything waiting on me? One screen:
// the verdict and the cluster's traffic, then what needs you (it will not fix
// itself), what is behind but catching up, and what the cluster holds. Under
// them, how the system runs: where the traffic is (namespaces by arrivals),
// the cluster underneath (each node), and its streams and connectors.
import { computed, onBeforeUnmount, ref } from 'vue'
import ConsoleShell from '@/console/ConsoleShell.vue'
import QcChart from '@/console/QcChart.vue'
import QcSpark from '@/console/QcSpark.vue'
import { useWindow, longDate, clock } from '@/console/useWindow'
import { useOverview } from '@/console/useOverview'
import { useOverviewPanels } from '@/console/useOverviewPanels'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { rate } from '@/console/words.js'

const { win, range, nowMs } = useWindow()
// One frame for the band and the page: the cell is read once.
const frame = useConsoleFrame()
const ov = useOverview(win, frame)
const more = useOverviewPanels(frame)

const path = [{ label: 'Overview' }]
const attention = computed(() => (ov.needs.value.length ? { tone: ov.verdict.value.tone, count: ov.needs.value.length + ov.moreNeeds.value } : null))
const perS = (v) => (v == null ? '–' : v > 0 && v < 0.01 ? '<0.01' : rate(v))
const plural = (n, one, many) => `${n.toLocaleString('en-US')} ${n === 1 ? one : many}`
// A namespace opens the Queues page filtered to it; queues with none, the whole list.
const nsLink = (ns) => (ns ? { path: '/queues', query: { find: ns } } : { path: '/queues' })
// The pages' own sentence, on hover: "Every stream keeps up.", "pg-in needs you: …".
const sentence = (v) => (v ? `${v.names?.length ? `${v.names.join(', ')} ` : ''}${v.text}` : '')
const streamsTitle = computed(() => sentence(more.inside.value.streamsVerdict))
const connectorsTitle = computed(() => sentence(more.inside.value.connectorsVerdict))
// A small laptop (1280x720): a lower chart, so the page stays one screen.
const shortMq = typeof window !== 'undefined' && window.matchMedia ? window.matchMedia('(max-height: 760px)') : null
const short = ref(!!shortMq?.matches)
const onShort = (e) => { short.value = e.matches }
shortMq?.addEventListener?.('change', onShort)
onBeforeUnmount(() => shortMq?.removeEventListener?.('change', onShort))
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
            :height="short ? 60 : 86"
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
          <h2 class="qn-h2 ov-h2behind">Behind, and catching up on their own<span v-if="ov.ready.value && !ov.behind.value.length" class="ov-hsub">· no queue is behind</span></h2>
          <div v-if="ov.behind.value.length" class="ov-behind qn-card">
            <RouterLink v-for="(b, i) in ov.behind.value" :key="b.key" :to="b.link" class="ov-brow qn-row" :class="{ extra: i >= 2 }">
              <span class="ov-bname"><span class="qn-mono">{{ b.queue }}</span> <span class="qn-t3" aria-hidden="true">→</span> <span :class="b.groupMono ? 'qn-mono qn-t2' : 'qn-t2'">{{ b.group }}</span></span>
              <span class="qn-num ov-bwait">{{ b.waiting }}</span>
              <span class="qn-t2 ov-beta">{{ b.eta }}</span>
              <QcSpark v-if="b.spark && b.spark.length > 1" :values="b.spark" :width="140" :height="24" label="Waiting, last hour" />
            </RouterLink>
          </div>

          <!-- where the traffic is: namespaces by arrivals over the hour -->
          <template v-if="more.traffic.value">
            <div class="ov-h2row ov-h2more">
              <h2 class="qn-h2">Where the traffic is <span class="ov-hsub">namespaces by arrivals, last hour<template v-if="more.traffic.value.more"> · {{ more.traffic.value.more }} more busy</template><template v-if="more.traffic.value.quiet"> · {{ more.traffic.value.quiet }} quiet</template></span></h2>
              <RouterLink to="/queues" class="qn-t3 ov-hlink">Every queue ›</RouterLink>
            </div>
            <p v-if="!more.traffic.value.rows.length" class="qn-t2 ov-none">Nothing arrived in the last hour.</p>
            <div v-else class="ov-tlist qn-card">
              <RouterLink v-for="(t, i) in more.traffic.value.rows" :key="t.ns || '\u0000'" :to="nsLink(t.ns)" class="ov-trow qn-row" :class="{ 'ov-tlast': i >= 3, 'ov-tthird': i === 2 }">
                <span :class="t.ns ? 'qn-mono ov-tname' : 'ov-tname qn-t2'">{{ t.ns || 'No namespace' }}</span>
                <span class="qn-num qn-t3 ov-tq">{{ plural(t.queues, 'queue', 'queues') }}</span>
                <span class="ov-tbar" aria-hidden="true"><span :style="{ width: `${Math.max(2, Math.round(t.share * 100))}%` }" /></span>
                <span class="qn-num ov-trate">{{ perS(t.inS) }}/s</span>
                <span class="qn-num qn-t3 ov-tack">{{ perS(t.ackS) }}/s acked</span>
              </RouterLink>
            </div>
          </template>
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
              <!-- what runs inside the broker: the Streams and Connectors pages' own verdicts -->
              <li v-if="more.inside.value.streams">
                <RouterLink to="/streams" class="ov-irow qn-row" :title="streamsTitle || undefined">
                  <span class="qn-t2">Streams</span>
                  <span class="qn-num" style="text-align: right; font-weight: 500">{{ more.inside.value.streams.total }}</span>
                  <span v-if="more.inside.value.streams.needs.length" class="qn-num ov-isub ov-ineeds" :class="more.inside.value.streamsVerdict?.tone || 'amber'"><span class="qn-mark" :class="more.inside.value.streamsVerdict?.tone || 'amber'" aria-hidden="true" />{{ more.inside.value.streams.needs.length }} {{ more.inside.value.streams.needs.length === 1 ? 'needs' : 'need' }} you</span>
                  <span v-else class="qn-num qn-t3 ov-isub">{{ more.inside.value.streams.total ? `${more.inside.value.streams.feed} feed · ${more.inside.value.streams.queue} queue` : 'none runs' }}</span>
                </RouterLink>
              </li>
              <li v-if="more.inside.value.connectors">
                <RouterLink to="/connectors" class="ov-irow qn-row" :title="connectorsTitle || undefined">
                  <span class="qn-t2">Connectors</span>
                  <span class="qn-num" style="text-align: right; font-weight: 500">{{ more.inside.value.connectors.total }}</span>
                  <span v-if="more.inside.value.connectors.needs.length" class="qn-num ov-isub ov-ineeds" :class="more.inside.value.connectorsVerdict?.tone || 'amber'"><span class="qn-mark" :class="more.inside.value.connectorsVerdict?.tone || 'amber'" aria-hidden="true" />{{ more.inside.value.connectors.needs.length }} {{ more.inside.value.connectors.needs.length === 1 ? 'needs' : 'need' }} you</span>
                  <span v-else class="qn-num qn-t3 ov-isub">{{ more.inside.value.connectors.off ? `${more.inside.value.connectors.off} off` : more.inside.value.connectors.total ? 'all running' : 'none' }}</span>
                </RouterLink>
              </li>
            </ul>
          </div>

          <!-- the cluster underneath: each node, as the Cluster page reads it -->
          <template v-if="more.nodes.value">
            <div class="ov-h2row ov-h2more">
              <h2 class="qn-h2">The cluster underneath</h2>
              <RouterLink to="/cluster" class="qn-t3 ov-hlink">Cluster ›</RouterLink>
            </div>
            <div class="ov-side qn-card">
              <RouterLink v-for="n in more.nodes.value.rows" :key="n.key" to="/cluster" class="ov-nrow qn-row">
                <span class="ov-nname"><span class="qn-mono" :title="n.name">{{ n.name }}</span><span v-if="n.leader || n.roleTone" class="ov-nrole" :class="n.roleTone || 'qn-t2'">{{ n.role.toLowerCase() }}</span></span>
                <span class="qn-num ov-ndisk" :class="n.diskTone"><span class="qn-t3">disk</span> {{ n.disk }}</span>
                <span class="qn-num ov-nup" :class="n.behindTone && n.behind ? n.behindTone : 'qn-t3'">{{ n.behindTone && n.behind ? `${n.behind} behind` : `up ${n.up}` }}</span>
              </RouterLink>
              <p class="qn-t3 ov-nfoot">
                <template v-if="more.nodes.value.versions.mixed">Versions differ: {{ more.nodes.value.versions.text }}.</template>
                <template v-else-if="more.nodes.value.versions.text">All on {{ more.nodes.value.versions.text }}.</template>
                <template v-if="more.nodes.value.gate != null"> Writes stop when a disk passes {{ more.nodes.value.gate }}%.</template>
              </p>
            </div>
          </template>
        </aside>
      </div>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.ov { max-width: 1400px; margin: 0 auto; padding: 24px 32px 16px; }
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

.ov-h2behind { margin-top: 22px; }
.ov-behind { margin-top: 8px; }
.ov-brow { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 20px; padding: 8px 8px; margin: 0 -8px; border-bottom: 1px solid var(--line2); }
.ov-bname { flex: 1 1 240px; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ov-bwait { flex: 0 0 120px; text-align: right; }
.ov-beta { flex: 0 0 160px; }

.ov-inv { margin-top: 6px; }
.ov-irow { display: grid; grid-template-columns: auto minmax(0, 1fr) minmax(6.5em, auto); align-items: baseline; gap: 12px; padding: 7px 8px; margin: 0 -8px; border-bottom: 1px solid var(--line2); white-space: nowrap; }
.ov-isub { text-align: right; font-size: 12px; }

/* the panels under the first screen: how the system runs */
.ov-h2more { margin-top: 22px; }
.ov-h2row { display: flex; align-items: baseline; gap: 12px; }
.ov-h2row .qn-h2 { flex: 1 1 auto; min-width: 0; }
.ov-hlink { font-size: 12px; white-space: nowrap; }
.ov-hlink:hover { color: var(--text); }
.ov-hsub { color: var(--text3); font-weight: 400; margin-left: 6px; }
.ov-tlist { margin-top: 8px; }
.ov-trow { display: grid; grid-template-columns: minmax(0, 1fr) 76px minmax(60px, 1.2fr) 64px 92px; align-items: center; gap: 12px; padding: 6px 8px; margin: 0 -8px; border-bottom: 1px solid var(--line2); white-space: nowrap; }
.ov-tname { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.ov-tq { font-size: 12px; text-align: right; }
.ov-tbar { display: block; height: 6px; border-radius: 3px; background: var(--bg2); overflow: hidden; }
.ov-tbar > span { display: block; height: 100%; border-radius: 3px; background: var(--text3); }
.ov-trate { text-align: right; font-weight: 500; }
.ov-tack { text-align: right; font-size: 12px; }
.ov-ineeds { display: inline-flex; align-items: center; justify-content: flex-end; gap: 6px; }
.ov-ineeds.amber { color: var(--amber); }
.ov-ineeds.red { color: var(--red); }
.ov-nrow { display: grid; grid-template-columns: minmax(0, 1fr) 64px 104px; align-items: baseline; gap: 12px; padding: 6px 8px; margin: 0 -8px; border-bottom: 1px solid var(--line2); white-space: nowrap; }
.ov-nname { display: flex; align-items: baseline; gap: 8px; min-width: 0; overflow: hidden; }
.ov-nname > .qn-mono { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.ov-nrole { flex: none; font-size: 12px; }
.ov-ndisk, .ov-nup { text-align: right; font-size: 12.5px; }
.ov-nup { overflow: hidden; text-overflow: ellipsis; }
.ov-nfoot { font-size: 12px; padding: 8px 0 2px; }
:global(.qn.tone .ov-tlist) { margin-top: 10px; padding: 6px var(--ov-pad) 8px; }
:global(.qn.tone .ov-tbar) { background: var(--well); }
@media (max-width: 640px) {
  .ov-trow { grid-template-columns: minmax(0, 1fr) minmax(40px, 0.8fr) 64px; }
  .ov-tq, .ov-tack { display: none; }
  /* room for the leader's whole name */
  .ov-nrow { grid-template-columns: minmax(0, 1fr) 52px 84px; gap: 8px; }
}

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

/* one screen: under 940px three namespaces; on a laptop two of each (needs,
   behind, namespaces), the rest behind "more", a cause in two lines, tighter rows */
@media (max-height: 940px) { .ov-tlast { display: none !important; } }
@media (max-height: 820px) {
  .extra, .ov-tthird { display: none !important; }
  .ov-more.wide { display: none; }
  .ov-more.narrow { display: inline-block; }
  .ov-need-detail { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
  .ov { padding-top: 18px; }
  .ov-title { font-size: 26px; }
  .ov-hero { padding-bottom: 16px; }
  .ov-cols { padding-top: 16px; }
  .ov-h2more { margin-top: 16px; }
  .ov-irow { padding-top: 5px; padding-bottom: 5px; }
  .ov-nrow, .ov-trow { padding-top: 4px; padding-bottom: 4px; }
  :global(.qn.tone .ov-hero) { padding-top: 16px; padding-bottom: 16px; }
  :global(.qn.tone .ov-cols) { padding-top: 16px; }
}
/* a small laptop: a cause in one line, tighter cards, the nodes without their footer */
@media (max-height: 760px) {
  .ov-need-detail { -webkit-line-clamp: 1; }
  .ov-need-actions { margin-top: 8px; }
  .ov-h2behind { margin-top: 16px; }
  .ov-h2more { margin-top: 12px; }
  .ov-irow { padding-top: 4px; padding-bottom: 4px; }
  .ov-nfoot { display: none; }
  :global(.qn.tone .ov-need) { padding-top: 12px; padding-bottom: 12px; }
}
@media (max-width: 640px) {
  .ov { padding: 18px 16px 32px; }
  .ov-title { font-size: 24px; }
  .ov-sidecol { max-width: none; }
  :global(.qn.tone .ov) { --ov-pad: 16px; }
  .ov-need-side { align-items: flex-start; flex-basis: 100%; }
}
</style>
