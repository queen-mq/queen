<script setup>
// One queue, in the new console: its name and facts, one line saying whether it
// is all right, the story when it needs you, its traffic over the window, and
// its groups, partitions, messages, dead letters, timers and options.
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { queues as queuesApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import ConsoleShell from '@/console/ConsoleShell.vue'
import QcChart from '@/console/QcChart.vue'
import QcPopover from '@/console/QcPopover.vue'
import QcDialog from '@/console/QcDialog.vue'
import { useWindow } from '@/console/useWindow'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { useQueuePage, fmt } from '@/console/useQueuePage'
import { span } from '@/console/useOverview'
import { toast } from '@/console/toasts'
import QueueGroups from '@/console/queue/QueueGroups.vue'
import QueuePartitions from '@/console/queue/QueuePartitions.vue'
import QueueMessages from '@/console/queue/QueueMessages.vue'
import QueueDead from '@/console/queue/QueueDead.vue'
import QueueScheduled from '@/console/queue/QueueScheduled.vue'
import QueueSettings from '@/console/queue/QueueSettings.vue'
import QueuePush from '@/console/queue/QueuePush.vue'

const route = useRoute()
const router = useRouter()
const { can } = useIdentity()
const qs = useQueuesStore()
const name = computed(() => String(route.params.queueName || ''))
const { win, range } = useWindow()
const frame = useConsoleFrame()
const q = useQueuePage(name, win)
const where = computed(() => frame.cluster.value.name)

const path = computed(() => [{ label: 'Queues', to: '/queues' }, { label: name.value, mono: true }])

// ---- tabs, kept in the address (?tab=) ----
const TABS = ['groups', 'partitions', 'messages', 'dead', 'scheduled', 'settings']
const tab = computed(() => (TABS.includes(route.query.tab) ? route.query.tab : 'groups'))
function pick(t) { router.replace({ query: { ...route.query, tab: t === 'groups' ? undefined : t } }) }
const timerCount = ref(null)
const tabs = computed(() => {
  const r = q.row.value
  return [
    { id: 'groups', label: 'Consumer groups', count: q.groups.value.length || null },
    { id: 'partitions', label: 'Partitions', count: r ? fmt(r.partitions) : null },
    { id: 'messages', label: 'Messages', count: r?.messages?.total != null ? fmt(r.messages.total) : null },
    { id: 'dead', label: 'Dead letters', count: r?.messages?.deadLetter ? fmt(r.messages.deadLetter) : null },
    { id: 'scheduled', label: 'Scheduled', count: timerCount.value || null },
    { id: 'settings', label: 'Settings', count: null },
  ]
})

// ---- actions ----
const pushing = ref(false)
const prefill = ref(null)
function openPush(p = null) { prefill.value = p; pushing.value = true }
const moreOpen = ref(false)
const deleting = ref(false)
const busy = ref(false)
async function remove() {
  busy.value = true
  try {
    const r = await queuesApi.delete(name.value)
    toast(`${name.value} deleted on ${where.value}`, `The broker answered ${r.status || 200}: the queue, its partitions and its messages are gone.`)
    deleting.value = false
    qs.invalidate?.()
    router.push('/queues')
  } catch (e) {
    toast('Not deleted', describeApiError(e), 'red')
  } finally { busy.value = false }
}
const sizeLine = computed(() => {
  const r = q.row.value
  if (!r) return ''
  const m = r.messages || {}
  return `${fmt(r.partitions)} partitions, ${fmt(m.total)} messages (${fmt(m.pending)} waiting) and ${q.groups.value.length} consumer group${q.groups.value.length === 1 ? '' : 's'}`
})
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="qp">
      <!-- a queue that is not there -->
      <template v-if="q.missing.value">
        <h1 class="qn-mono qp-title qn-t2">{{ name }}</h1>
        <p style="font-size: 15px; margin-top: 14px">There is no queue named <span class="qn-mono">{{ name }}</span> on {{ where }}.</p>
        <p style="margin-top: 12px"><RouterLink to="/queues" style="font-weight: 500">Back to Queues</RouterLink></p>
      </template>

      <template v-else>
        <!-- name, facts, actions -->
        <header class="qp-head">
          <h1 class="qn-mono qp-title">{{ name }}</h1>
          <span v-for="f in q.facts.value" :key="f" class="qn-tag">{{ f }}</span>
          <div style="flex: 1 1 0" />
          <button v-if="can('produce')" type="button" class="qn-btn2" @click="openPush()">Push a message</button>
          <QcPopover v-model:open="moreOpen" side="bottom-end" :width="220" label="More actions">
            <template #trigger="{ toggle }">
              <button type="button" class="qn-btn2" aria-label="More actions" aria-haspopup="dialog" :aria-expanded="moreOpen" style="width: 32px; padding: 0" @click="toggle">
                <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><circle cx="3" cy="7" r="1.2" fill="currentColor" /><circle cx="7" cy="7" r="1.2" fill="currentColor" /><circle cx="11" cy="7" r="1.2" fill="currentColor" /></svg>
              </button>
            </template>
            <template #default="{ close }">
              <button type="button" class="qn-opt" @click="pick('settings'); close()">Change its settings</button>
              <RouterLink :to="{ path: '/settings', query: { queue: name } }" class="qn-opt">Its alert lines</RouterLink>
              <RouterLink :to="{ path: '/queues', query: { view: 'workers', queue: name } }" class="qn-opt">The workers that read it</RouterLink>
              <template v-if="can('queueAdmin')">
                <div class="qn-popsep" />
                <button type="button" class="qn-opt red" @click="deleting = true; close()">Delete the queue…</button>
              </template>
            </template>
          </QcPopover>
        </header>

        <!-- is it all right -->
        <p v-if="q.verdict.value" class="qp-verdict">
          <span class="qp-badge" :class="q.verdict.value.tone || 'qn-t2'">
            <span v-if="q.verdict.value.tone" class="qn-mark" :class="q.verdict.value.tone" aria-hidden="true" />{{ q.verdict.value.badge }}
          </span>
          <span>{{ q.verdict.value.text }}</span>
          <span v-if="q.caughtUp.value" class="qn-t2">{{ q.caughtUp.value }}</span>
        </p>
        <p v-else class="qp-verdict qn-t2">Reading {{ name }}…</p>

        <!-- why it needs you -->
        <section v-if="q.why.value" class="qp-why qn-panel" aria-labelledby="qp-why-title">
          <h2 id="qp-why-title" style="font-size: 15px; font-weight: 500">{{ q.why.value.title }}</h2>
          <template v-if="q.why.value.kind === 'acks'">
            <p v-if="q.why.value.causes.length" style="margin-top: 10px">
              {{ fmt(q.why.value.dead) }} dead letters, {{ q.why.value.causes.length === 1 ? 'all one cause' : `${q.why.value.causes.length} causes; the most:` }}
            </p>
            <p v-if="q.why.value.causes.length" class="qn-mono qp-cause">{{ q.why.value.causes[0].text }}</p>
            <p v-if="q.why.value.line" class="qn-t2" style="margin-top: 10px">{{ q.why.value.line }}</p>
            <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px">
              <RouterLink :to="{ path: '/dlq', query: { queue: name } }" class="qn-btn">Open its dead letters</RouterLink>
              <button type="button" class="qn-btn2" @click="pick('dead')">See every cause</button>
            </div>
          </template>
          <template v-else-if="q.why.value.kind === 'lag'">
            <div class="qp-share">
              <div style="flex: 1 1 360px; min-width: 0">
                <div class="qn-share" style="margin-top: 12px" role="img" :aria-label="`${Math.round(q.why.value.share * 100)}% in ${q.why.value.topCount} partitions`">
                  <span :style="{ flex: `0 0 ${Math.max(2, Math.round(q.why.value.share * 100))}%`, background: q.why.value.share >= 0.6 ? 'var(--amber)' : 'var(--text2)' }" />
                  <span style="flex: 1 1 0; background: var(--line)" />
                </div>
                <div class="qn-num qn-t3" style="display: flex; justify-content: space-between; font-size: 12px; margin-top: 8px">
                  <span>{{ q.why.value.topCount }} partitions · {{ fmt(q.why.value.topSum) }} waiting</span>
                  <span>{{ fmt(Math.max(0, q.why.value.total - q.why.value.topCount)) }} others · {{ fmt(q.why.value.othersSum) }}</span>
                </div>
              </div>
              <p class="qn-t2" style="flex: 1 1 360px; min-width: 0">{{ q.why.value.line }}</p>
            </div>
            <div v-if="q.why.value.rows.length" role="table" aria-label="The partitions holding the most" style="--cols: minmax(0, 2fr) 110px 110px; margin-top: 16px">
              <div class="qn-tr head" role="row"><span role="columnheader">Partition, the entity</span><span role="columnheader" class="r">Waiting</span><span role="columnheader" class="r">Oldest</span></div>
              <div v-for="p in q.why.value.rows" :key="p.id || p.partition" class="qn-tr" role="row">
                <span role="cell" class="qn-mono" :title="p.partition" style="font-weight: 500">{{ p.partition }}</span>
                <span role="cell" class="r qn-num">{{ fmt(p.pending) }}</span>
                <span role="cell" class="r qn-num">{{ p.lagSeconds ? span(p.lagSeconds) : '–' }}</span>
              </div>
            </div>
            <p class="qn-read" style="margin-top: 10px">Read from <span class="qn-mono">GET /api/v1/resources/partitions?queue={{ name }}</span>, the partitions holding the most.</p>
          </template>
          <template v-else>
            <p class="qn-t2" style="margin-top: 10px">{{ q.why.value.line }}</p>
            <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px">
              <RouterLink :to="{ path: '/settings', query: { queue: name } }" class="qn-btn2">Its alert lines</RouterLink>
            </div>
          </template>
        </section>

        <!-- its traffic over the window -->
        <section class="qp-charts">
          <div class="qp-chart">
            <div class="qp-legend">
              <span v-for="l in q.traffic.value.legend" :key="l.id" class="qp-key">
                <span class="qp-swatch" :style="{ background: l.dashed ? 'none' : l.ink, borderTop: l.dashed ? `2px dashed ${l.ink}` : 'none' }" aria-hidden="true" />
                {{ l.label }} <span class="qn-num" style="color: var(--text); font-weight: 500">{{ l.value }}</span>
              </span>
              <span class="qn-t3" style="margin-left: auto">{{ win.long }}, from 0</span>
            </div>
            <QcChart :series="q.traffic.value.series" :from="win.from" :to="win.to" :height="120" :axis="win.axis" unit="/s" aria-label="Messages in and acked a second" @zoom="range" />
          </div>
          <div class="qp-chart">
            <div class="qp-legend">
              <span class="qp-key"><span class="qp-swatch" style="background: var(--ink-in)" aria-hidden="true" />Waiting <span class="qn-num" style="color: var(--text); font-weight: 500">{{ fmt(q.row.value?.messages?.pending) }}</span> now</span>
              <span class="qn-t3" style="margin-left: auto">read once a minute</span>
            </div>
            <QcChart :series="q.traffic.value.waiting" :from="win.from" :to="win.to" :height="120" :axis="win.axis" :min-top="10" aria-label="Messages waiting" @zoom="range" />
          </div>
        </section>
        <p v-if="sizeLine" class="qn-read" style="margin-top: 8px">{{ sizeLine }}.</p>

        <!-- the lists -->
        <nav class="qn-tabs" role="tablist" aria-label="Queue views" style="margin-top: 26px">
          <button v-for="t in tabs" :key="t.id" type="button" class="qn-tab" role="tab" :aria-selected="tab === t.id" @click="pick(t.id)">
            {{ t.label }}<span v-if="t.count" class="qn-count">{{ t.count }}</span>
          </button>
        </nav>
        <div role="tabpanel" class="qp-panel">
          <QueueGroups v-if="tab === 'groups'" :groups="q.groups.value" :lines-for="q.linesFor" />
          <QueuePartitions v-else-if="tab === 'partitions'" :data="q.parts.value" :total="q.row.value?.partitions ?? null" />
          <QueueMessages v-else-if="tab === 'messages'" :queue="name" :where="where" @copy="openPush" />
          <QueueDead v-else-if="tab === 'dead'" :queue="name" :count="q.row.value?.messages?.deadLetter || 0" :sig="q.sig.value" />
          <QueueScheduled v-else-if="tab === 'scheduled'" :queue="name" :where="where" @count="timerCount = $event" />
          <QueueSettings v-else-if="tab === 'settings'" :queue="name" :where="where" />
        </div>
      </template>
    </div>

    <QueuePush :open="pushing" :queue="name" :where="where" :prefill="prefill" @close="pushing = false" @pushed="q.refresh()" />
    <QcDialog :open="deleting" :title="`Delete ${name}?`" :where="where" :call="`DELETE /api/v1/resources/queues/${encodeURIComponent(name)}`" :typed="name" confirm-label="Delete" danger :busy="busy" @confirm="remove" @cancel="deleting = false">
      <p class="qn-t2">This removes the queue with its {{ sizeLine }}. It cannot be undone.</p>
    </QcDialog>
  </ConsoleShell>
</template>

<style scoped>
.qp { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.qp-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.qp-title { font-size: 22px; font-weight: 500; margin-right: 8px; overflow-wrap: anywhere; }
.qp-verdict { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 10px; font-size: 15px; margin-top: 14px; }
.qp-badge { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 500; }
.qp-badge .qn-mark { align-self: center; }
.qp-why { margin-top: 20px; }
.qp-cause { font-size: 12.5px; margin-top: 6px; padding: 10px 12px; background: var(--bg2); border-radius: var(--r-ctl); overflow-wrap: anywhere; }
.qp-share { display: flex; flex-wrap: wrap; gap: 14px 40px; align-items: flex-start; }
.qp-charts { display: grid; grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr); gap: 28px; margin-top: 24px; }
.qp-legend { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 18px; font-size: 12px; color: var(--text3); margin-bottom: 8px; }
.qp-key { display: inline-flex; align-items: center; gap: 6px; }
.qp-swatch { width: 12px; height: 2px; display: inline-block; }
.qp-panel { min-height: 180px; }
@media (max-width: 900px) { .qp-charts { grid-template-columns: minmax(0, 1fr); } }
@media (max-width: 640px) { .qp { padding: 18px 16px 40px; } .qp-title { font-size: 19px; } }
</style>
