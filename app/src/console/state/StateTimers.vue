<script setup>
import QcJson from '../QcJson.vue'
// The timers of every queue, the next to fire first. There is no tenant-wide
// timer read: it is one GET /api/v1/timers/:queue per queue, each its first
// 100 by key. A click opens one with its payload (a peek); Cancel shows its
// call first.
import { computed, nextTick, ref, watch } from 'vue'
import { timerAddr } from '@/api/timerPath'
import { describeVerdict } from '@/composables/useGatedVerdict'
import { decodeTimerPayload, describeCancel } from '@/composables/useTimers'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { nowMs, clock } from '../useWindow'
import { refusal } from '../useStatePage'
import { bytes, fmt, fires, msOf, ago, clip } from './stateViews'

const props = defineProps({
  s: { type: Object, required: true },
  where: { type: String, default: '' },
})
const { timerList, sweep, sweeping, sweepDone, sweepTotal, timersVerdict, peekTimer, cancelTimer, rereadQueue, canWrite } = props.s
const now = nowMs
const enc = encodeURIComponent

const SHOW = 50
const queue = ref('')
const shown = ref(SHOW)
const picked = ref(null)
watch(queue, () => { shown.value = SHOW; picked.value = null })

const quiet = computed(() => (timersVerdict.value && timersVerdict.value !== 'paused' ? describeVerdict(timersVerdict.value, 'timers') : null))
const all = computed(() => timerList.value?.rows || [])
const list = computed(() => (queue.value ? all.value.filter((t) => t.queue === queue.value) : all.value))
const rows = computed(() => list.value.slice(0, shown.value))
const perQueue = computed(() => [...(timerList.value?.perQueue || new Map())].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
const truncated = computed(() => (sweep.value?.results || []).filter((r) => r.truncated).map((r) => r.queue).sort())
const failed = computed(() => timerList.value?.failed || [])

function at(ms) {
  if (ms == null) return '–'
  const d = new Date(ms)
  return d.toDateString() === new Date(now.value).toDateString()
    ? clock(ms, true)
    : d.toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
}

// ---- one timer, with its payload ----
const drawer = ref(null)
const sel = computed(() => all.value.find((t) => t.id === picked.value) || null)
const peeked = ref(null)
const peekError = ref(null)
let peekSeq = 0
watch(sel, async (t, was) => {
  if (!t || t.id === was?.id) return
  const seq = ++peekSeq
  peeked.value = null
  peekError.value = null
  try {
    const d = await peekTimer(t.queue, t.key)
    if (seq === peekSeq) peeked.value = d
  } catch (e) {
    if (seq === peekSeq) peekError.value = refusal(e)
  }
})
function toggle(id) {
  picked.value = picked.value === id ? null : id
  if (picked.value) nextTick(() => drawer.value?.scrollIntoView?.({ block: 'nearest' }))
}
const cols = computed(() => (sel.value
  ? '150px minmax(0, 1.3fr) minmax(0, 1fr) 48px 84px'
  : '150px minmax(0, 1.4fr) minmax(0, 1fr) minmax(0, 0.9fr) 52px 92px'))
const payload = computed(() => (peeked.value && peeked.value.found !== false ? decodeTimerPayload(peeked.value) : null))
const payloadText = computed(() => {
  const p = payload.value
  if (!p || p.state !== 'text') return ''
  try { const v = JSON.parse(p.text); return v !== null && typeof v === 'object' ? JSON.stringify(v, null, 2) : p.text } catch { return p.text }
})

// ---- cancel ----
const cancelling = ref(null)
const busy = ref(false)
async function cancel() {
  const t = cancelling.value
  if (!t) return
  busy.value = true
  try {
    const r = await cancelTimer(t.queue, t.key)
    const d = describeCancel(r.data, { queue: t.queue, timerKey: t.key })
    toast(d.status === 'cancelled' ? `Cancelled ${t.key}` : `Not cancelled: ${d.status || 'no verdict'}`, `The broker answered ${r.status || 200}. ${d.sentence}`, d.tone === 'ok' ? '' : 'red')
    cancelling.value = null
    if (picked.value === t.id) picked.value = null
    rereadQueue(t.queue)
  } catch (e) {
    toast('Not cancelled', refusal(e), 'red')
  } finally { busy.value = false }
}
</script>

<template>
  <div class="stt">
    <div class="stt-list">
      <div class="stt-head">
        <h2 class="stt-h">Timers</h2>
        <div style="flex: 1 1 0" />
        <label v-if="perQueue.length > 1" class="qn-field stt-pick">
          <span class="qn-t3" style="font-size: 12px; white-space: nowrap">Queue</span>
          <select v-model="queue">
            <option value="">Every queue</option>
            <option v-for="[q, n] in perQueue" :key="q" :value="q">{{ q }} · {{ fmt(n) }}</option>
          </select>
        </label>
      </div>

      <template v-if="quiet">
        <p class="stt-lead">{{ quiet.title }}.</p>
        <p class="qn-t2" style="margin-top: 6px; max-width: 640px">{{ quiet.detail }}</p>
      </template>
      <p v-else-if="!timerList" class="stt-state qn-t2 qn-num">
        <template v-if="sweeping">Reading the timers of {{ fmt(sweepTotal) }} queues, one call each: {{ fmt(sweepDone) }} so far…</template>
        <template v-else>Reading the timers…</template>
      </p>
      <template v-else-if="!all.length">
        <p class="stt-lead">No timers are waiting.</p>
        <p class="qn-t2" style="margin-top: 6px; max-width: 640px">A timer is a message scheduled for later: at its time it fires into its queue, in order with its partition. Steps schedule and cancel them in their transactions.</p>
      </template>

      <template v-else>
        <div class="stt-scroll">
          <div class="stt-table" :class="{ narrow: sel }" role="table" aria-label="Timers, the next to fire first" :style="{ '--cols': cols }">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Fires</span><span role="columnheader">Timer key</span><span role="columnheader">Queue</span>
              <span v-if="!sel" role="columnheader">Partition</span><span role="columnheader" class="r">Tries</span><span role="columnheader"><span class="qn-vh">Actions</span></span>
            </div>
            <div
              v-for="t in rows" :key="t.id" class="qn-tr click" :class="{ on: picked === t.id }" role="row"
              tabindex="0" @click="toggle(t.id)" @keydown.enter="toggle(t.id)"
            >
              <span role="cell" class="qn-num">{{ at(t.atMs) }} <span class="qn-t3">{{ fires(t.atMs, now) }}</span></span>
              <span role="cell" class="qn-mono" :title="t.key" style="font-weight: 500">{{ t.key }}</span>
              <span role="cell" class="qn-mono qn-t2" :title="t.queue">{{ t.queue }}</span>
              <span v-if="!sel" role="cell" class="qn-mono qn-t2" :title="t.partition">{{ t.partition || 'Default' }}</span>
              <span role="cell" class="r qn-num" :class="t.lastError ? 'red' : 'qn-t2'" :title="t.lastError">{{ t.attempts }}</span>
              <span role="cell" style="text-align: right">
                <span v-if="t.claimed" class="qn-t3">firing</span>
                <button v-else-if="canWrite" type="button" class="qn-btn3" style="height: 28px" @click.stop="cancelling = t">Cancel…</button>
              </span>
            </div>
          </div>
        </div>
        <div v-if="list.length > rows.length" style="margin-top: 12px">
          <button type="button" class="qn-btn2" @click="shown += SHOW">Show {{ fmt(Math.min(SHOW, list.length - rows.length)) }} more</button>
        </div>
        <p v-if="truncated.length" class="qn-t2" style="margin-top: 12px">
          <template v-if="truncated.length === 1"><span class="qn-mono">{{ truncated[0] }}</span> holds more than 100: only its first 100 by key are here, so its soonest may be missing.</template>
          <template v-else>{{ fmt(truncated.length) }} queues hold more than 100 each: only their first 100 by key are here, so their soonest may be missing.</template>
        </p>
      </template>

      <p v-if="failed.length" class="qn-t2" style="margin-top: 12px">
        <span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />{{ failed.length === 1 ? 'One queue' : `${fmt(failed.length)} queues` }} did not answer:
        <span class="qn-mono">{{ failed[0].queue }}</span> ({{ clip(failed[0].error, 80) }}){{ failed.length > 1 ? ' and others' : '' }}.
      </p>
      <p class="qn-read" style="margin-top: 12px">
        Read from <span class="qn-mono">GET /api/v1/timers/{queue}</span>, one call per queue<template v-if="sweep">: {{ fmt(sweep.read) }} {{ sweep.read === 1 ? 'queue' : 'queues' }}<template v-if="sweep.read < sweep.total">, the first by name of {{ fmt(sweep.total) }}</template>, the first 100 timers of each by key, {{ ago(sweep.at, now) }}</template>.
      </p>
    </div>

    <aside v-if="sel" ref="drawer" class="stt-drawer qn-panel" :aria-label="`The timer ${sel.key}`">
      <div style="display: flex; align-items: flex-start; gap: 8px">
        <div style="flex: 1 1 0; min-width: 0">
          <p class="qn-t3" style="font-size: 12px">Timer in <RouterLink :to="{ path: `/queues/${enc(sel.queue)}`, query: { tab: 'scheduled' } }" class="qn-mono">{{ sel.queue }}</RouterLink></p>
          <h3 class="qn-mono stt-key">{{ sel.key }}</h3>
        </div>
        <button type="button" class="qn-btn3" aria-label="Close the timer" style="height: 28px; flex: 0 0 auto" @click="picked = null">✕</button>
      </div>
      <dl class="qn-dl" style="margin-top: 14px">
        <dt>Fires</dt><dd class="qn-num">{{ at(sel.atMs) }}, {{ fires(sel.atMs, now) }}</dd>
        <dt>Partition</dt><dd class="qn-mono">{{ sel.partition || 'Default' }}</dd>
        <dt>Tries</dt><dd class="qn-num">{{ sel.attempts }}<span v-if="sel.claimed" class="qn-t3">, firing now</span></dd>
        <template v-if="sel.lastError"><dt>Last error</dt><dd class="red">{{ sel.lastError }}</dd></template>
        <template v-if="peeked && peeked.found !== false">
          <dt>Transaction</dt><dd class="qn-mono">{{ peeked.txn || '–' }}</dd>
          <dt>Scheduled</dt><dd class="qn-num">{{ at(msOf(peeked.createdAt)) }}</dd>
        </template>
      </dl>
      <p class="qn-t3" style="font-size: 12px; margin-top: 14px">Payload<template v-if="payload && (payload.state === 'text' || payload.state === 'binary')">, {{ bytes(payload.bytes) }}<template v-if="peeked.payloadZstd">, {{ bytes(payload.storedBytes) }} stored as zstd</template></template></p>
      <p v-if="peekError" class="qn-t2" style="margin-top: 6px">The broker did not answer: {{ peekError }}</p>
      <p v-else-if="!peeked" class="qn-t2" style="margin-top: 6px">Reading the timer…</p>
      <p v-else-if="peeked.found === false" class="qn-t2" style="margin-top: 6px">No longer waiting: it fired, or someone cancelled it.</p>
      <QcJson v-else-if="payload?.state === 'text'" :text="payloadText" style="margin-top: 6px" />
      <p v-else class="stt-note">
        <template v-if="payload?.state === 'encrypted'">Encrypted when it was scheduled: not readable here.</template>
        <template v-else-if="payload?.state === 'binary'">Binary, not text.</template>
        <template v-else-if="payload?.state === 'empty'">Empty.</template>
        <template v-else-if="payload?.state === 'too_large'">Too large to show: {{ bytes(payload.storedBytes) }} stored.</template>
        <template v-else-if="payload?.state === 'error'">It did not decode: {{ payload.error }}.</template>
        <template v-else>No payload came with it.</template>
      </p>
      <div v-if="canWrite && !sel.claimed" style="margin-top: 16px">
        <button type="button" class="qn-btn2 red" @click="cancelling = sel">Cancel the timer…</button>
      </div>
    </aside>

    <QcDialog
      :open="!!cancelling" :title="`Cancel the timer ${cancelling?.key || ''}?`" :where="where"
      :call="cancelling ? `DELETE ${timerAddr(cancelling.queue, cancelling.key)}` : ''"
      confirm-label="Cancel the timer" cancel-label="Keep it" danger :busy="busy" @confirm="cancel" @cancel="cancelling = null"
    >
      <p class="qn-t2">It will not fire at {{ cancelling ? at(cancelling.atMs) : '' }}, so the message it carries is never pushed into <span class="qn-mono">{{ cancelling?.queue }}</span>. Scheduling the same key again brings it back.</p>
      <p class="qn-t3" style="font-size: 12px; margin-top: 10px">A timer already firing cannot be cancelled: the broker answers too late.</p>
    </QcDialog>
  </div>
</template>

<style scoped>
.stt { display: flex; flex-wrap: wrap; align-items: flex-start; gap: 20px 28px; }
.stt-list { flex: 999 1 520px; min-width: 0; }
.stt-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; min-height: 32px; }
.stt-h { font-size: 18px; font-weight: 500; }
.stt-pick { width: 320px; max-width: 100%; }
.stt-pick select { flex: 1 1 0; min-width: 0; border: 0; outline: 0; background: transparent; color: var(--text); font-size: 12.5px; }
.stt-lead { font-size: 14.5px; margin-top: 14px; }
.stt-state { padding: 18px 0 6px; }
.stt-scroll { margin-top: 12px; overflow-x: auto; }
.stt-table { min-width: 720px; }
.stt-table.narrow { min-width: 520px; }
.stt-drawer { flex: 1 1 340px; min-width: 0; max-width: 440px; padding: 16px 18px; position: sticky; top: 64px; max-height: calc(100vh - 80px); overflow-y: auto; }
.stt-key { font-size: 13.5px; font-weight: 500; margin-top: 2px; overflow-wrap: anywhere; }
.stt-note { margin-top: 6px; padding: 12px 14px; border-radius: var(--r-ctl); background: var(--bg2); color: var(--text2); }
@media (max-width: 900px) { .stt-drawer { position: static; max-height: none; max-width: none; } }
</style>
