<script setup>
// Why this queue's dead letters failed (the broker's error signatures), and the
// newest of them. Replay and discard live on the Dead letters page.
import { computed, ref, shallowRef, watch } from 'vue'
import { dlq as dlqApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import QcJson from '../QcJson.vue'
import { span } from '../useOverview'
import { fmt, groupLabel } from '../useQueuePage'

const props = defineProps({
  queue: { type: String, required: true },
  count: { type: Number, default: 0 },
  sig: { type: Object, default: null },
})

const data = shallowRef(null)
const error = ref(null)
const open = ref(null)
async function load() {
  if (!props.count) { data.value = { messages: [], total: 0 }; return }
  try {
    const r = await dlqApi.list({ queue: props.queue, limit: 20 }, { quiet: true })
    data.value = r.data
    error.value = null
  } catch (e) { error.value = describeApiError(e) }
}
useAutoRefresh(load)
load()
watch(() => props.queue, () => { open.value = null; load() })
// The count comes with the queue listing, which may land after this tab opens.
watch(() => props.count, (n, was) => { if (n && !was) load() })

const causes = computed(() => (props.sig?.signatures || []).map((c) => ({ text: c.text, n: c.n, pct: Math.round((c.share || 0) * 100) })))
const rows = computed(() => (data.value?.messages || []).map((m) => ({
  key: m.id,
  at: m.failedAt ? new Date(m.failedAt).toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '–',
  ago: m.failedAt ? span((Date.now() - Date.parse(m.failedAt)) / 1000) + ' ago' : '',
  id: m.transactionId,
  group: m.consumerGroup ? groupLabel(m.consumerGroup) : '–',
  error: m.errorMessage || '–',
  tries: m.retryCount ?? '–',
  raw: m,
})))
const dlqLink = computed(() => ({ path: '/dlq', query: { queue: props.queue } }))
</script>

<template>
  <div class="qd">
    <p v-if="!count" style="font-size: 14.5px; padding-top: 8px">No dead letters on <span class="qn-mono">{{ queue }}</span>.</p>
    <template v-else>
      <div class="qd-head">
        <p style="font-size: 14.5px"><span class="qn-num" style="font-weight: 500">{{ fmt(count) }}</span> dead letters<template v-if="causes.length">, {{ causes.length === 1 ? 'all one cause' : `${causes.length} causes` }}</template>.</p>
        <RouterLink :to="dlqLink" class="qn-btn2">Replay or discard them</RouterLink>
      </div>
      <ul v-if="causes.length" class="qd-causes">
        <li v-for="(c, i) in causes" :key="i">
          <div class="qn-share" style="width: 120px; flex: 0 0 120px; margin-top: 7px" role="img" :aria-label="`${c.pct}% of the newest ${sig?.sample || ''}`">
            <span :style="{ flex: `0 0 ${c.pct}%`, background: 'var(--text2)' }" />
            <span style="flex: 1 1 0; background: var(--line)" />
          </div>
          <span class="qn-num qn-t3" style="flex: 0 0 44px; text-align: right">{{ c.pct }}%</span>
          <span class="qn-mono" style="font-size: 12.5px; overflow-wrap: anywhere">{{ c.text }}</span>
        </li>
      </ul>
      <p v-if="sig" class="qn-read" style="margin-top: 6px">Causes from the newest {{ fmt(sig.sample) }}, read from <span class="qn-mono">GET /api/v1/analytics/dlq-signatures?queue={{ queue }}</span>.</p>

      <h3 class="qn-h2" style="margin-top: 22px">The newest</h3>
      <p v-if="error" class="qn-t2" style="padding: 12px 0">The broker did not answer: {{ error }}</p>
      <div v-else class="qd-table" role="table" aria-label="Newest dead letters" style="--cols: 150px minmax(0, 1.2fr) minmax(0, 0.9fr) minmax(0, 2fr) 56px">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Failed at</span><span role="columnheader">transactionId</span><span role="columnheader">Group</span>
          <span role="columnheader">Error</span><span role="columnheader" class="r">Tries</span>
        </div>
        <template v-for="r in rows" :key="r.key">
          <div class="qn-tr click" :class="{ on: open === r.key }" role="row" tabindex="0" @click="open = open === r.key ? null : r.key" @keydown.enter="open = r.key">
            <span role="cell" class="qn-num" :title="r.ago">{{ r.at }}</span>
            <span role="cell" class="qn-mono" :title="r.id">{{ r.id }}</span>
            <span role="cell" class="qn-mono qn-t2">{{ r.group }}</span>
            <span role="cell" class="qn-t2" :title="r.error">{{ r.error }}</span>
            <span role="cell" class="r qn-num">{{ r.tries }}</span>
          </div>
          <div v-if="open === r.key" class="qd-open">
            <p class="qn-mono" style="font-size: 12.5px; overflow-wrap: anywhere">{{ r.error }}</p>
            <QcJson :value="r.raw.payload ?? r.raw.data" style="margin-top: 10px" />
          </div>
        </template>
      </div>
      <p class="qn-read" style="margin-top: 12px">Read from <span class="qn-mono">GET /api/v1/dlq?queue={{ queue }}&amp;limit=20</span>.</p>
    </template>
  </div>
</template>

<style scoped>
.qd { margin-top: 14px; }
.qd-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px; justify-content: space-between; }
.qd-causes { margin-top: 14px; display: grid; gap: 10px; }
.qd-causes li { display: flex; align-items: flex-start; gap: 12px; }
.qd-table { margin-top: 8px; overflow-x: auto; }
.qd-open { padding: 12px 0 16px; border-bottom: 1px solid var(--line2); }
</style>
