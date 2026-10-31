<script setup>
import QcJson from '../QcJson.vue'
// One cause of dead letters: whether it still happens, its words in full,
// where and how it failed, its newest rows (a click shows the payload), and
// what to do with it. Replay and discard first show what changes and the
// call, and read the rows they act on just before; download sends nothing.
import { computed, ref, watch } from 'vue'
import { describeApiError } from '@/api/errors'
import { payloadToText } from '@/composables/usePushVerdict'
import { formatDlqMarkdown } from '@/composables/useDlqMarkdown'
import { midCut } from '@/composables/dlqWhy'
import { useIdentity } from '@/stores/identity'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { clock, nowMs } from '../useWindow'
import { span } from '../useOverview'
import { fmt, groupLabel, QUEUE_MODE } from '../useQueuePage'
import { asJson, breakable, countedLine, countText, fileName, isLive, markdownHead, retriesWords, SAMPLE } from './dlCauses'
import { deleteRows, markdownOf, purgeQueue, replayRows, saveFile } from './dlActions'

const props = defineProps({
  cause: { type: Object, required: true },
  samples: { type: Array, default: () => [] },
  read: { type: Object, default: null }, // { limit, error, at }: the samples' read
  queue: { type: Object, default: null }, // its queue in the listing: { name, dead, pending }
  where: { type: String, default: '' }, // the cluster
  freshRows: { type: Function, required: true },
})
const emit = defineEmits(['acted'])
const { can, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()

const plural = (n, one, many) => `${fmt(n)} ${n === 1 ? one : many}`
const DAY = new Intl.DateTimeFormat(undefined, { weekday: 'short', day: 'numeric' })
const sameDay = (a, b) => new Date(a).toDateString() === new Date(b).toDateString()
const at = (ms, seconds = false) => (sameDay(ms, nowMs.value) ? clock(ms, seconds) : `${DAY.format(new Date(ms))} ${clock(ms, seconds)}`)
// "07:35", "yesterday 16:11", "Thu 29 16:11"
function day(ms) {
  if (sameDay(ms, nowMs.value)) return clock(ms)
  if (sameDay(ms, nowMs.value - 86_400_000)) return `yesterday ${clock(ms)}`
  return `${DAY.format(new Date(ms))} ${clock(ms)}`
}
const when = (ms) => (sameDay(ms, nowMs.value) ? `at ${clock(ms)}` : sameDay(ms, nowMs.value - 86_400_000) ? `yesterday at ${clock(ms)}` : `on ${DAY.format(new Date(ms))} at ${clock(ms)}`)
const ago = (ms) => { const s = (nowMs.value - ms) / 1000; return s < 5 ? 'just now' : `${span(s)} ago` }

// ---- what it is ----
const c = computed(() => props.cause)
const live = computed(() => isLive(c.value, nowMs.value))
const status = computed(() => {
  const x = c.value
  if (x.unknown) return { tone: '', text: `The broker did not say why its queue's dead letters failed: ${x.error}` }
  if (x.gone) return { tone: '', text: 'None left: no dead letter has this cause any more.' }
  if (x.dropped) return { tone: '', text: `No longer among its queue's newest ${SAMPLE} dead letters, so it is not counted now.` }
  if (x.newest == null) return null
  if (live.value) return { tone: 'red', text: `Still happening: the last one failed ${ago(x.newest)}, so a replay now would fail the same way.` }
  return { tone: '', text: `Stopped. The last one failed ${when(x.newest)}.` }
})
const words = computed(() => {
  const x = c.value
  if (x.unknown) return 'Its causes could not be read'
  if (x.rest) return 'Other causes'
  return x.full || (x.text + (x.cut ? '…' : ''))
})
const facts = computed(() => {
  const x = c.value
  const out = []
  const g = x.groups || []
  if (g.length === 1) out.push({ k: g[0] === QUEUE_MODE ? '' : 'group', v: groupLabel(g[0]), mono: g[0] !== QUEUE_MODE })
  else if (g.length > 1) out.push({ k: 'groups', v: g.map(groupLabel).join(', '), mono: true })
  const r = retriesWords(x.retries)
  if (r) out.push({ k: '', v: r })
  // The first is known only when every row was read; within a minute of the last it says nothing.
  if (x.exact && x.oldest && x.newest && x.newest - x.oldest >= 60_000 && !x.gone) out.push({ k: '', v: `since ${day(x.oldest)}` })
  return out
})
const counted = computed(() => (c.value.rest
  ? `The broker names a queue's eight most common causes; these are the rest. ${countedLine(c.value)}`
  : countedLine(c.value)))

// ---- its newest rows ----
const open = ref(null)
watch(() => c.value.key, () => { open.value = null })
const keyOf = (m) => m.id || `${m.transactionId}::${m.consumerGroup}`
// A column only where the rows differ: what they share is said once.
const differ = (f) => new Set(props.samples.map(f)).size > 1
const groupsDiffer = computed(() => differ((m) => m.consumerGroup))
const triesDiffer = computed(() => differ((m) => m.retryCount))
const errorsDiffer = computed(() => differ((m) => String(m.errorMessage ?? '').trim()))
const partsDiffer = computed(() => differ((m) => m.partition))
const onePartition = computed(() => (props.samples.length > 1 && !partsDiffer.value ? props.samples[0].partition || '' : ''))
const showPartition = computed(() => !onePartition.value)
const cols = computed(() => [
  'var(--c1, 118px)', 'minmax(0, 1.5fr)',
  showPartition.value ? 'minmax(0, 1fr)' : null,
  groupsDiffer.value ? 'minmax(0, .8fr)' : null,
  errorsDiffer.value ? 'minmax(0, 1.6fr)' : null,
  triesDiffer.value ? '56px' : null,
].filter(Boolean).join(' '))
// Three columns truncate on a phone; more scroll sideways inside the table.
const minW = computed(() => {
  const n = 2 + [showPartition.value, groupsDiffer.value, errorsDiffer.value, triesDiffer.value].filter(Boolean).length
  return n <= 3 ? '0px' : '580px'
})
const rows = computed(() => props.samples.map((m) => {
  const t = Date.parse(m.failedAt)
  return {
    key: keyOf(m),
    at: Number.isFinite(t) ? at(t, true) : '–',
    ago: Number.isFinite(t) ? ago(t) : '',
    tx: m.transactionId || '',
    partition: m.partition || '',
    group: m.consumerGroup ? groupLabel(m.consumerGroup) : '–',
    error: String(m.errorMessage ?? '').trim() || '(no message)',
    tries: m.retryCount ?? '–',
    raw: m,
  }
}))
const envelope = (d) => !!d && typeof d === 'object' && typeof d.encrypted === 'string' && typeof d.iv === 'string' && typeof d.authTag === 'string'
const context = computed(() => ({ tenant: actingTenantSlug.value, cluster: actingClusterSlug.value || props.where, cell: actingCellSlug.value }))
async function copy(text, what) {
  try { await navigator.clipboard.writeText(text); toast(`Copied ${what}`) } catch { toast('Could not copy to the clipboard', 'The browser refused it.', 'red') }
}
const copyPayload = (m) => copy(payloadToText(m.payload ?? m.data), 'the payload')
const copyMarkdown = (m) => copy(formatDlqMarkdown(m, { ...context.value, encryptedEnvelope: envelope(m.payload ?? m.data) }), 'it as Markdown')

// ---- what to do ----
const canAct = computed(() => can('queueAdmin') && !c.value.gone && !c.value.dropped && !c.value.unknown)
// Every dead letter of the queue is this cause, as far as was read: one purge.
const whole = computed(() => c.value.single && !c.value.rest && !c.value.unknown)
const dead = computed(() => props.queue?.dead ?? c.value.rowsNow ?? 0)
const pending = computed(() => props.queue?.pending ?? 0)
// Its rows among the newest 200: for the queue's only cause, every row there.
const reach = computed(() => Math.min((whole.value ? c.value.count : c.value.n) || 0, SAMPLE))
const partial = computed(() => reach.value < (c.value.count || 0))
const replayLabel = computed(() => (partial.value ? `Replay the newest ${fmt(reach.value)}` : `Replay ${fmt(reach.value)}`))
const discardLabel = computed(() => (whole.value ? `Discard all ${fmt(dead.value)}…` : partial.value ? `Discard the newest ${fmt(reach.value)}…` : `Discard ${fmt(reach.value)}…`))
const advice = computed(() => {
  if (live.value) return ''
  if (c.value.gone || c.value.dropped || c.value.unknown) return ''
  return 'If what caused it is fixed, a replay should go through.'
})

const dlg = ref(null) // 'replay' | 'discard'
const targets = ref(null) // the rows read just now, that the action takes
const reading = ref(false)
const readError = ref(null)
const busy = ref(false)
const progress = ref('')
async function openDlg(kind) {
  dlg.value = kind
  progress.value = ''
  readError.value = null
  if (kind === 'discard' && whole.value) { targets.value = []; return } // one purge: nothing to read first
  targets.value = null
  reading.value = true
  const key = c.value.key
  try {
    const r = await props.freshRows(c.value)
    if (dlg.value === kind && c.value.key === key) targets.value = r.rows
  } catch (e) {
    readError.value = describeApiError(e)
  } finally { reading.value = false }
}
function close() {
  if (busy.value) return
  dlg.value = null
  targets.value = null
}
watch(() => c.value.key, () => { if (!busy.value) close() })
const n = computed(() => (targets.value ? targets.value.length : reach.value))
const after = (x) => (x.exact ? '' : '≈ ') + fmt(Math.max(0, (x.count || 0) - n.value))

const replayTitle = computed(() => `Replay ${plural(n.value, 'dead letter', 'dead letters')}?`)
const replayCall = computed(() => `${fmt(n.value)} × POST /api/v1/dlq/{id}/replay {}`)
const replayConfirm = computed(() => (live.value ? `Replay anyway on ${props.where}` : `Replay ${fmt(n.value)} on ${props.where}`))
async function doReplay() {
  const list = targets.value || []
  if (!list.length || busy.value) return
  busy.value = true
  const x = c.value
  try {
    const r = await replayRows(list, (k, of) => { progress.value = `Replayed ${fmt(k)} of ${fmt(of)}…` })
    const also = []
    if (r.gone) also.push(`${fmt(r.gone)} were already gone`)
    if (r.duplicate) also.push(`${fmt(r.duplicate)} were already in the queue under their replay id, so they stay`)
    if (r.failed) {
      toast(`Replayed ${fmt(r.moved)} of ${fmt(r.asked)} on ${props.where}`, `${r.first}${r.stopped ? ' The rest were not sent.' : ''}`, 'red')
    } else {
      toast(`Replayed ${fmt(r.moved)} on ${props.where}`, `The broker moved ${r.moved === 1 ? 'it' : 'them'} back into ${x.queue}${also.length ? `; ${also.join('; ')}` : ''}.`)
    }
    dlg.value = null
    targets.value = null
    emit('acted')
  } finally { busy.value = false; progress.value = '' }
}

const discardTitle = computed(() => `Discard ${plural(whole.value ? dead.value : n.value, 'dead letter', 'dead letters')}?`)
const discardCall = computed(() => (whole.value
  ? `DELETE /api/v1/dlq?queue=${encodeURIComponent(c.value.queue)}`
  : `${fmt(n.value)} × DELETE /api/v1/messages/{partitionId}/{transactionId}`))
const discardConfirm = computed(() => `Discard ${fmt(whole.value ? dead.value : n.value)} on ${props.where}`)
async function doDiscard() {
  if (busy.value) return
  busy.value = true
  const x = c.value
  try {
    if (whole.value) {
      const k = await purgeQueue(x.queue)
      toast(`Discarded ${plural(k, 'dead letter', 'dead letters')} on ${props.where}`, `The broker answered 200 and deleted ${fmt(k)} from ${x.queue}. Nothing went back into the queue.`)
    } else {
      const r = await deleteRows(targets.value || [], (k, of) => { progress.value = `Discarded ${fmt(k)} of ${fmt(of)}…` })
      if (r.failed) toast(`Discarded ${fmt(r.deleted)} of ${fmt(r.asked)} on ${props.where}`, `${r.first}${r.stopped ? ' The rest were not sent.' : ''}`, 'red')
      else toast(`Discarded ${plural(r.deleted, 'dead letter', 'dead letters')} on ${props.where}`, `The broker deleted them from ${x.queue}${r.missed ? `; ${fmt(r.missed)} were already gone` : ''}. Nothing went back into the queue.`)
    }
    dlg.value = null
    targets.value = null
    emit('acted')
  } catch (e) {
    toast('Not discarded', describeApiError(e), 'red')
  } finally { busy.value = false; progress.value = '' }
}

// ---- download: the newest with this cause, read now; nothing is sent ----
const saving = ref('')
async function download(kind) {
  if (saving.value) return
  saving.value = kind
  const x = c.value
  try {
    const { rows: list } = await props.freshRows(x)
    if (!list.length) { toast('Nothing to download', `No dead letter of ${x.queue} has this cause now.`); return }
    const name = fileName(x.queue, kind === 'json' ? 'json' : 'md')
    if (kind === 'json') saveFile(name, asJson(list))
    else saveFile(name, markdownOf(list, context.value, markdownHead(x, list.length, { where: props.where })) + '\n', 'text/markdown')
    toast(`Downloaded ${plural(list.length, 'dead letter', 'dead letters')} as ${kind === 'json' ? 'JSON' : 'Markdown'}`, `${name}: the newest with this cause.`)
  } catch (e) {
    toast('Not downloaded', describeApiError(e), 'red')
  } finally { saving.value = '' }
}
</script>

<template>
  <section class="dl-cause qn-panel" aria-labelledby="dl-cause-words">
    <p v-if="status" class="dl-status" :class="status.tone || 'qn-t3'">
      <span v-if="status.tone === 'red'" class="qn-mark red" aria-hidden="true" />
      <span>{{ status.text }}</span>
    </p>
    <h2 id="dl-cause-words" class="qn-mono dl-words"><template v-for="(p, i) in breakable(words)" :key="i">{{ p }}<wbr></template></h2>
    <p class="dl-facts qn-t2">
      <RouterLink :to="`/queues/${encodeURIComponent(c.queue)}`" class="qn-mono dl-qlink">{{ c.queue }}</RouterLink>
      <template v-for="f in facts" :key="f.v">
        <span class="qn-t3" aria-hidden="true">·</span>
        <span>{{ f.k ? `${f.k} ` : '' }}<span :class="{ 'qn-mono': f.mono }">{{ f.v }}</span></span>
      </template>
    </p>
    <p v-if="counted" class="qn-t3 dl-counted">{{ counted }}</p>

    <!-- its newest rows -->
    <div class="dl-sec">
      <h3 class="qn-h2">The newest</h3>
      <p v-if="read?.error && !samples.length" class="qn-t2 dl-note">The broker did not answer: {{ read.error }}</p>
      <p v-else-if="!read" class="qn-t2 dl-note">Reading its newest dead letters…</p>
      <p v-else-if="!samples.length" class="qn-t2 dl-note">None of the newest {{ fmt(read.limit) }} dead letters of {{ c.queue }} has this cause now.</p>
      <p v-if="onePartition" class="qn-t2 dl-note">These are all from one partition, <span class="qn-mono dl-onepart">{{ onePartition }}</span>.</p>
      <div v-if="samples.length" class="dl-samples" role="table" aria-label="The newest dead letters with this cause" :style="{ '--cols-wide': cols, '--minw': minW }">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Failed at</span>
          <span role="columnheader">transactionId</span>
          <span v-if="showPartition" role="columnheader" class="dl-wide">Partition</span>
          <span v-if="groupsDiffer" role="columnheader" class="dl-wide">Group</span>
          <span v-if="errorsDiffer" role="columnheader" class="dl-wide">Error</span>
          <span v-if="triesDiffer" role="columnheader" class="r dl-wide">Retries</span>
        </div>
        <template v-for="r in rows" :key="r.key">
          <div
            class="qn-tr click" :class="{ on: open === r.key }" role="row" tabindex="0" :aria-expanded="open === r.key"
            @click="open = open === r.key ? null : r.key" @keydown.enter.prevent="open = open === r.key ? null : r.key"
          >
            <span role="cell" class="qn-num" :title="r.ago">{{ r.at }}</span>
            <span role="cell" class="qn-mono dl-cell" :title="r.tx">{{ midCut(r.tx, showPartition ? 46 : 72) }}</span>
            <span v-if="showPartition" role="cell" class="qn-mono qn-t2 dl-cell dl-wide" :title="r.partition">{{ midCut(r.partition, 30) }}</span>
            <span v-if="groupsDiffer" role="cell" class="qn-mono qn-t2 dl-cell dl-wide" :title="r.group">{{ r.group }}</span>
            <span v-if="errorsDiffer" role="cell" class="qn-mono qn-t2 dl-cell dl-wide" :title="r.error">{{ r.error }}</span>
            <span v-if="triesDiffer" role="cell" class="r qn-num dl-wide">{{ r.tries }}</span>
          </div>
          <div v-if="open === r.key" class="dl-open">
            <dl class="qn-dl">
              <dt>transactionId</dt><dd class="qn-mono">{{ r.tx || '–' }}</dd>
              <template v-if="showPartition"><dt>Partition</dt><dd class="qn-mono">{{ r.partition || '–' }}</dd></template>
              <template v-if="groupsDiffer"><dt>Group</dt><dd class="qn-mono">{{ r.group }}</dd></template>
              <dt>Failed at</dt><dd><span class="qn-mono">{{ r.raw.failedAt || '–' }}</span><span v-if="r.ago" class="qn-t3">, {{ r.ago }}</span></dd>
              <template v-if="triesDiffer"><dt>Retries</dt><dd class="qn-num">{{ r.tries }}</dd></template>
              <template v-if="errorsDiffer || r.error !== words"><dt>Error</dt><dd class="qn-mono">{{ r.error }}</dd></template>
            </dl>
            <p v-if="envelope(r.raw.payload ?? r.raw.data)" class="qn-t3 dl-envelope">Encrypted: this is the stored envelope, not the message.</p>
            <QcJson class="dl-pre" :value="r.raw.payload ?? r.raw.data" />
            <div class="dl-open-acts">
              <button type="button" class="qn-btn3" @click="copyPayload(r.raw)">Copy the payload</button>
              <button type="button" class="qn-btn3" @click="copyMarkdown(r.raw)">Copy as Markdown</button>
            </div>
          </div>
        </template>
      </div>
      <p class="qn-read dl-read">Read from <span class="qn-mono">GET /api/v1/dlq?queue={{ c.queue }}&amp;limit={{ read?.limit || 8 }}</span>, newest first<template v-if="c.causes > 1">, then matched to their cause</template>.</p>
    </div>

    <!-- what to do -->
    <div class="dl-sec dl-do">
      <div class="dl-acts">
        <template v-if="canAct">
          <button type="button" class="qn-btn2" :disabled="!reach" @click="openDlg('replay')">{{ replayLabel }}</button>
          <button type="button" class="qn-btn2 danger" :disabled="!reach && !whole" @click="openDlg('discard')">{{ discardLabel }}</button>
        </template>
        <button type="button" class="qn-btn3" :disabled="!!saving || c.gone" @click="download('json')">{{ saving === 'json' ? 'Reading…' : 'Download JSON' }}</button>
        <button type="button" class="qn-btn3" :disabled="!!saving || c.gone" @click="download('md')">{{ saving === 'md' ? 'Reading…' : 'Download Markdown' }}</button>
      </div>
      <p v-if="!can('queueAdmin')" class="qn-t2 dl-note">Replaying and discarding need the admin role on this cluster.</p>
      <p v-else-if="advice" class="qn-t2 dl-note">{{ advice }}</p>
    </div>

    <!-- replay -->
    <QcDialog :open="dlg === 'replay'" :title="replayTitle" :where="where" :call="replayCall" :confirm-label="replayConfirm" :busy="busy" :blocked="!targets || !targets.length" @confirm="doReplay" @cancel="close">
      <p v-if="reading" class="qn-t2">Reading the dead letters it would replay…</p>
      <p v-else-if="readError" class="qn-t2">The broker did not answer: {{ readError }}</p>
      <p v-else-if="targets && !targets.length" class="qn-t2">None of the newest {{ fmt(SAMPLE) }} dead letters of <span class="qn-mono">{{ c.queue }}</span> has this cause now: there is nothing to replay.</p>
      <template v-else-if="targets">
        <div class="dl-change qn-num">
          <span class="qn-t2">Dead letters with this cause</span><span>{{ countText(c) }} → {{ after(c) }}</span>
          <span class="qn-t2">Waiting in <span class="qn-mono">{{ c.queue }}</span></span><span>{{ fmt(pending) }} → {{ fmt(pending + n) }}</span>
        </div>
        <p class="qn-t2" style="margin-top: 12px">Each goes back into <span class="qn-mono">{{ c.queue }}</span>, in its own partition, as <span class="qn-mono dl-keep">dlq:&lt;row id&gt;</span>: a second replay of the same row writes nothing.<template v-if="partial"> These are the newest {{ fmt(n) }}; the older ones stay dead-lettered.</template></p>
        <p v-if="live" style="margin-top: 10px"><span class="red dl-inline"><span class="qn-mark red" aria-hidden="true" />Still happening,</span> the last one {{ ago(c.newest) }}: a replay now will most likely fail the same way.</p>
        <p v-if="progress" class="qn-num" style="margin-top: 10px">{{ progress }}</p>
      </template>
    </QcDialog>

    <!-- discard -->
    <QcDialog :open="dlg === 'discard'" :title="discardTitle" :where="where" :call="discardCall" :confirm-label="discardConfirm" danger :typed="c.queue" :busy="busy" :blocked="whole ? false : (!targets || !targets.length)" @confirm="doDiscard" @cancel="close">
      <template v-if="whole">
        <div class="dl-change qn-num">
          <span class="qn-t2">Dead letters in <span class="qn-mono">{{ c.queue }}</span></span><span>{{ fmt(dead) }} → 0</span>
          <span class="qn-t2">Waiting in <span class="qn-mono">{{ c.queue }}</span></span><span>{{ fmt(pending) }}, unchanged</span>
        </div>
        <p class="qn-t2" style="margin-top: 12px">Every dead letter of <span class="qn-mono">{{ c.queue }}</span> is deleted, and nothing goes back into the queue. It cannot be undone.</p>
        <p v-if="!c.exact" class="qn-t2" style="margin-top: 8px">Its newest {{ fmt(c.sample) }} all failed this way; the {{ fmt(Math.max(0, dead - c.sample)) }} older ones were not read, and go too.</p>
      </template>
      <template v-else>
        <p v-if="reading" class="qn-t2">Reading the dead letters it would discard…</p>
        <p v-else-if="readError" class="qn-t2">The broker did not answer: {{ readError }}</p>
        <p v-else-if="targets && !targets.length" class="qn-t2">None of the newest {{ fmt(SAMPLE) }} dead letters of <span class="qn-mono">{{ c.queue }}</span> has this cause now: there is nothing to discard.</p>
        <template v-else-if="targets">
          <div class="dl-change qn-num">
            <span class="qn-t2">Dead letters with this cause</span><span>{{ countText(c) }} → {{ after(c) }}</span>
            <span class="qn-t2">Waiting in <span class="qn-mono">{{ c.queue }}</span></span><span>{{ fmt(pending) }}, unchanged</span>
          </div>
          <p class="qn-t2" style="margin-top: 12px">These are deleted, and nothing goes back into <span class="qn-mono">{{ c.queue }}</span>; its dead letters with other causes stay. It cannot be undone.</p>
        </template>
      </template>
      <p v-if="progress" class="qn-num" style="margin-top: 10px">{{ progress }}</p>
    </QcDialog>
  </section>
</template>

<style scoped>
.dl-cause { min-width: 0; }
.dl-status { display: flex; align-items: baseline; gap: 8px; font-size: 12.5px; font-weight: 500; }
.dl-status .qn-mark { align-self: center; }
.dl-status.qn-t3 { font-weight: 400; }
.dl-words { font-size: 15px; font-weight: 500; line-height: 1.45; margin-top: 8px; overflow-wrap: anywhere; white-space: pre-wrap; }
.dl-facts { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 8px; margin-top: 8px; }
.dl-qlink { font-weight: 500; color: var(--text); }
.dl-counted { font-size: 12px; margin-top: 6px; max-width: 72ch; }
.dl-sec { margin-top: 22px; padding-top: 18px; border-top: 1px solid var(--line2); }
:global(.qn.tone .dl-cause .dl-sec) { border-top-color: var(--bg3); }
.dl-note { margin-top: 10px; }
.dl-samples { margin-top: 8px; overflow-x: auto; --cols: var(--cols-wide); }
.dl-samples .qn-tr { min-width: var(--minw, 0px); }
.dl-cell { font-size: 12px; }
.dl-onepart { overflow-wrap: anywhere; }
.dl-open { padding: 12px 0 16px; border-bottom: 1px solid var(--line2); min-width: var(--minw, 0px); }
.dl-envelope { font-size: 12px; margin-top: 10px; }
.dl-pre { margin-top: 12px; }
.dl-open-acts { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 8px; margin-left: -8px; }
.dl-read { margin-top: 10px; overflow-wrap: anywhere; }
.dl-acts { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.dl-keep { white-space: nowrap; }
.dl-change { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px 16px; font-size: 12.5px; }
.dl-change > span:nth-child(even) { text-align: right; white-space: nowrap; }
.dl-inline { display: inline-flex; align-items: center; gap: 6px; font-weight: 500; }
/* Stacked under the list, a picked cause scrolls to the top even before its rows arrive. */
@media (max-width: 1000px) { .dl-cause { min-height: calc(100vh - 140px); scroll-margin-top: 12px; } }
/* A phone keeps when and which; the rest is in the opened row. */
@media (max-width: 640px) {
  .dl-cause.qn-panel { padding: 16px 14px; }
  .dl-samples { --c1: 100px; --cols: var(--c1) minmax(0, 1fr); }
  .dl-samples .qn-tr, .dl-open { min-width: 0; }
  .dl-samples .dl-wide { display: none; }
}
</style>
