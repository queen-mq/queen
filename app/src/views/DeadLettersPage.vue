<script setup>
// Dead letters, in the new console: why the tenant's dead letters failed,
// cause by cause across its queues (one queue's with ?queue=), in a list
// that keeps its order while you look, and the picked cause on the right,
// to read, replay, discard or download.
import { computed, nextTick, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ConsoleShell from '@/console/ConsoleShell.vue'
import QcPopover from '@/console/QcPopover.vue'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { nowMs } from '@/console/useWindow'
import { fmt } from '@/console/useQueuePage'
import { useDeadLetters } from '@/console/useDeadLetters'
import { breakable, countText, isLive, roughly, SAMPLE, summaryParts } from '@/console/deadletters/dlCauses'
import DlCause from '@/console/deadletters/DlCause.vue'

const route = useRoute()
const router = useRouter()
const frame = useConsoleFrame()
const where = computed(() => frame.cluster.value.name)
const focus = computed(() => (typeof route.query.queue === 'string' ? route.query.queue.trim() : ''))
const path = computed(() => (focus.value
  ? [{ label: 'Dead letters', to: '/dlq' }, { label: focus.value, mono: true }]
  : [{ label: 'Dead letters' }]))
const d = useDeadLetters(focus)

const summary = computed(() => summaryParts(d.causes.value, {
  total: d.total.value, queues: d.shownQueues.value.length, queue: focus.value, now: nowMs.value,
}))
const live = (c) => isLive(c, nowMs.value)
const causeText = (c) => (c.unknown ? 'Its causes could not be read' : c.rest ? 'Other causes' : c.text + (c.cut ? '…' : ''))
function lastText(c) {
  if (c.unknown) return 'not read'
  if (c.gone) return 'none left'
  if (c.dropped) return `not in the newest ${SAMPLE}`
  if (c.newest == null) return '–'
  if (live(c)) return 'still happening'
  return `stopped ${roughly((nowMs.value - c.newest) / 1000)} ago`
}
const scaled = computed(() => d.causes.value.some((c) => !c.exact && !c.gone && c.count != null))
// Read whole when the sample held every dead letter of the queue.
const allRead = computed(() => d.causes.value.every((c) => c.unknown || c.sample >= c.rowsNow))

// ---- picking a cause: the highlight and the right side change, never the list ----
const panel = ref(null)
function choose(c) {
  d.pick(c.key)
  // Stacked (a phone, a narrow window), the cause opens under the list: bring it up.
  if (window.matchMedia?.('(max-width: 1000px)').matches) {
    nextTick(() => panel.value?.$el?.scrollIntoView?.({ block: 'start', behavior: 'smooth' }))
  }
}

// ---- one queue, or all of them (?queue=) ----
const pickerOpen = ref(false)
function narrow(name) { router.replace({ query: { ...route.query, queue: name || undefined } }) }
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="dl">
      <header class="dl-head">
        <h1 class="dl-title">Dead letters</h1>
        <div style="flex: 1 1 0" />
        <QcPopover v-if="d.deadQueues.value.length > 1 || focus" v-model:open="pickerOpen" side="bottom-end" :width="340" label="Which queues">
          <template #trigger="{ toggle }">
            <button type="button" class="qn-chip" aria-haspopup="dialog" :aria-expanded="pickerOpen" @click="toggle">
              <span :class="{ 'qn-mono': focus }" class="dl-chipname">{{ focus || 'All queues' }}</span>
              <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><polyline points="2,3.5 5,6.5 8,3.5" style="fill: none; stroke: currentColor; stroke-width: 1.3" /></svg>
            </button>
          </template>
          <template #default="{ close }">
            <button type="button" class="qn-opt" :class="{ on: !focus }" @click="narrow(''); close()">All queues</button>
            <div class="qn-popsep" />
            <button v-for="q in d.deadQueues.value" :key="q.name" type="button" class="qn-opt" :class="{ on: focus === q.name }" @click="narrow(q.name); close()">
              <span class="qn-mono dl-optname">{{ q.name }}</span>
              <span class="qn-num qn-t3">{{ fmt(q.dead) }}</span>
            </button>
          </template>
        </QcPopover>
      </header>

      <p v-if="d.failed.value" class="dl-verdict">The broker did not answer: {{ d.failed.value }}</p>
      <p v-else-if="!d.ready.value" class="dl-verdict qn-t2">Reading the dead letters…</p>
      <template v-else-if="!d.causes.value.length">
        <p class="dl-verdict"><span v-for="(p, i) in summary" :key="i" :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></p>
        <p v-if="focus" style="margin-top: 12px"><RouterLink to="/dlq" class="qn-link">See every queue</RouterLink></p>
      </template>
      <template v-else>
        <p class="dl-verdict"><span v-for="(p, i) in summary" :key="i" :class="{ 'qn-mono': p.mono }">{{ p.t }}</span></p>

        <div class="dl-split">
          <section class="dl-list" aria-label="Causes">
            <div class="qn-tr head dl-lhead" aria-hidden="true"><span>Cause</span><span class="r">Dead letters</span></div>
            <ul>
              <li v-for="c in d.causes.value" :key="c.key">
                <button
                  type="button" class="qn-tr click dl-row"
                  :class="{ on: d.picked.value?.key === c.key, gone: c.gone || c.dropped }"
                  :aria-current="d.picked.value?.key === c.key ? 'true' : undefined"
                  @click="choose(c)"
                >
                  <span class="dl-c1">
                    <span class="qn-mono dl-ctext"><template v-for="(p, i) in breakable(causeText(c))" :key="i">{{ p }}<wbr></template></span>
                    <span v-if="!focus" class="qn-mono dl-cq">{{ c.queue }}</span>
                  </span>
                  <span class="dl-c2">
                    <span class="qn-num dl-cn">{{ countText(c) }}</span>
                    <span class="dl-cw" :class="live(c) ? 'red' : 'qn-t3'"><span v-if="live(c)" class="qn-mark red" aria-hidden="true" />{{ lastText(c) }}</span>
                  </span>
                </button>
              </li>
            </ul>
            <p class="qn-read dl-lread">
              Causes from <span class="qn-mono">GET /api/v1/analytics/dlq-signatures?queue={{ focus || '' }}</span><template v-if="!focus"> for each queue with dead letters</template>, over {{ allRead ? (focus ? 'every one of them' : 'every one') : `up to the newest ${fmt(SAMPLE)} of ${focus ? 'it' : 'each'}` }}<template v-if="scaled">; a count with ≈ is scaled from those</template>.
            </p>
          </section>

          <DlCause
            v-if="d.picked.value"
            ref="panel"
            :cause="d.picked.value"
            :samples="d.samples.value"
            :read="d.samplesRead.value"
            :queue="d.queueRow(d.picked.value.queue)"
            :where="where"
            :fresh-rows="d.freshRows"
            @acted="d.refresh(true)"
          />
        </div>
      </template>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.dl { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.dl-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; min-height: 32px; }
.dl-title { font-size: 22px; font-weight: 500; }
.dl-chipname { max-width: 260px; overflow: hidden; text-overflow: ellipsis; }
.dl-optname { flex: 1 1 0; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.dl-verdict { font-size: 15px; margin-top: 14px; }

.dl-split { display: grid; grid-template-columns: minmax(360px, 5fr) minmax(0, 7fr); gap: 28px 32px; align-items: start; margin-top: 24px; }
/* The list stays in view while the cause beside it scrolls. */
.dl-list { position: sticky; top: 72px; max-height: calc(100vh - 96px); overflow-y: auto; min-width: 0; }
.dl-lhead { --cols: minmax(0, 1fr) auto; padding: 0 10px; position: sticky; top: 0; background: var(--bg); z-index: 1; }
.dl-list .dl-row {
  --cols: minmax(0, 1fr) auto;
  width: 100%; align-items: start; gap: 0 16px; padding: 10px 10px; text-align: left;
}
.dl-list .dl-row > * { white-space: normal; overflow: visible; }
.dl-c1 { display: flex; flex-direction: column; gap: 3px; min-width: 0; }
/* In full: a signature is at most 120 characters, and causes of one queue
   often differ only at the end. */
.dl-ctext { font-size: 12.5px; line-height: 1.45; overflow-wrap: anywhere; }
.dl-cq { font-size: 12px; color: var(--text3); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dl-c2 { display: flex; flex-direction: column; align-items: flex-end; gap: 3px; text-align: right; }
.dl-cn { font-size: 13px; white-space: nowrap; }
.dl-cw { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; white-space: nowrap; }
.dl-cw.red { font-weight: 500; }
.dl-row.on .dl-ctext, .dl-row.on .dl-cn { font-weight: 500; }
.dl-row.on .dl-cq { color: var(--text2); }
.dl-row.gone .dl-ctext, .dl-row.gone .dl-cn { color: var(--text3); }
.dl-lread { margin-top: 10px; padding: 0 10px; overflow-wrap: anywhere; }

@media (max-width: 1000px) {
  .dl-split { grid-template-columns: minmax(0, 1fr); }
  .dl-list { position: static; max-height: none; overflow: visible; }
  .dl-lhead { position: static; }
}
@media (max-width: 640px) {
  .dl { padding: 18px 16px 40px; }
  .dl-title { font-size: 19px; }
}
</style>
