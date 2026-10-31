<script setup>
// New queue: its name, namespace and task, lease time and retries, and every
// other option behind "More options"; then what changes and the /configure
// call, before anything runs. Only options that differ from the broker's
// defaults are sent. A name the cluster already has is refused here: the
// route would merge into that queue.
import { computed, ref, watch } from 'vue'
import { queues as queuesApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { describeConfigureRefusal, EDITABLE_OPTIONS, OPTION_DEFAULTS, OPTION_META, formatOptionValue, toFormValue } from '@/composables/useQueueConfig'
import { useQueuesStore } from '@/stores/queuesStore'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { newQueueDraft } from './queuesView'
import QsSuggest from './QsSuggest.vue'

const props = defineProps({
  open: { type: Boolean, default: false },
  where: { type: String, default: '' },
  namespaces: { type: Array, default: () => [] }, // [{ value, count }]
  tasks: { type: Array, default: () => [] },
  total: { type: Number, default: 0 },            // queues on this cluster now
  noNamespace: { type: Number, default: 0 },      // queues with no namespace now
})
const emit = defineEmits(['close', 'created'])
const qs = useQueuesStore()

// Every option but the two the form shows first, in the broker's order.
const MORE = EDITABLE_OPTIONS.filter((o) => o.key !== 'leaseTime' && o.key !== 'retryLimit')
const short = (help) => (help || '').split(/(?<=\.)\s/)[0]
const blank = () => ({
  name: '', namespace: '', task: '',
  leaseTime: String(OPTION_DEFAULTS.leaseTime), retryLimit: String(OPTION_DEFAULTS.retryLimit),
  ...Object.fromEntries(MORE.map((o) => [o.key, toFormValue(o, OPTION_DEFAULTS[o.key])])),
})
const more = ref(false)
const form = ref(blank())
const touched = ref(false)
const busy = ref(false)
watch(() => props.open, (o) => { if (o) { form.value = blank(); touched.value = false; busy.value = false; more.value = false } })
watch(() => form.value.name, (n) => { if (n) touched.value = true })

const d = computed(() => newQueueDraft(form.value, qs.queueMeta.value))
const call = computed(() => `POST /api/v1/configure ${JSON.stringify(d.value.body)}`)
const n = (v) => Number(v).toLocaleString('en-US')

const nameNote = computed(() => {
  const x = d.value
  if (x.exists) return { tone: 'red', taken: x.exists, takenIn: x.existsIn || '', text: x.existsIn ? ' already exists, in ' : ' already exists, with no namespace.' }
  if (x.nameError && touched.value) return { tone: 'red', text: x.nameError }
  return { tone: '', text: 'The name producers and consumers use, like orders.refunded.' }
})
const nsKnown = computed(() => props.namespaces.find((o) => o.value === d.value.namespace) || null)
const nsNote = computed(() => (d.value.namespace && !nsKnown.value
  ? `A new namespace: ${d.value.namespace}.`
  : 'Groups the queue. A worker can pop every queue of a namespace at once.'))
const taskKnown = computed(() => props.tasks.find((o) => o.value === d.value.task) || null)
const taskNote = computed(() => (d.value.task && !taskKnown.value
  ? `A new task: ${d.value.task}.`
  : 'A second label a pop can ask for.'))
const nsBefore = computed(() => (d.value.namespace ? (nsKnown.value?.count || 0) : props.noNamespace))
const extraTags = computed(() => Object.entries(d.value.extra || {}).map(([k, v]) => `${OPTION_META[k].label} ${formatOptionValue(OPTION_META[k], v)}`))
const retries = computed(() => {
  const r = Number(d.value.retryLimit)
  return Number.isFinite(r) ? `${n(r)} ${r === 1 ? 'retry' : 'retries'}` : ''
})

async function create() {
  const x = d.value
  if (!x.ok || busy.value) return
  busy.value = true
  try {
    const r = await queuesApi.configure(x.body)
    const echo = r.data
    // `configured: true` is the broker saying it wrote the row; anything else
    // reached a 200 without configuring.
    if (!echo || echo.configured !== true) {
      toast('Not confirmed', 'The broker answered without confirming the queue, so it may not exist. Look for it before trying again.', 'red')
      return
    }
    toast(`${echo.queue || x.name} created on ${props.where || 'this cluster'}`, 'The broker answered 200. It has no partitions yet: the first push makes one.')
    emit('created', { name: echo.queue || x.name, ns: x.namespace })
    emit('close')
  } catch (e) {
    toast('Not created', describeConfigureRefusal(e) || describeApiError(e), 'red')
  } finally { busy.value = false }
}
</script>

<template>
  <QcDialog
    :open="open"
    title="Create a queue"
    :where="where"
    :call="call"
    :confirm-label="`Create on ${where || 'this cluster'}`"
    :busy="busy"
    :blocked="!d.ok"
    @confirm="create"
    @cancel="emit('close')"
  >
    <div class="qsn-scroll">
    <div class="qsn">
      <label class="qsn-field qsn-wide">
        <span class="qsn-label">Name</span>
        <span class="qn-field" style="margin-top: 6px">
          <input v-model="form.name" class="qn-mono" type="text" autocomplete="off" spellcheck="false" placeholder="orders.refunded" aria-describedby="qsn-name-note" />
        </span>
        <span id="qsn-name-note" class="qsn-note" :class="nameNote.tone">
          <span v-if="nameNote.taken" class="qn-mono">{{ nameNote.taken }}</span>{{ nameNote.text }}<template v-if="nameNote.takenIn"><span class="qn-mono">{{ nameNote.takenIn }}</span>.</template>
          <template v-if="nameNote.taken">{{ ' ' }}<RouterLink :to="{ path: `/queues/${encodeURIComponent(nameNote.taken)}`, query: { tab: 'settings' } }" class="qn-link">Change its settings</RouterLink></template>
        </span>
      </label>

      <div class="qsn-field">
        <label class="qsn-label" for="qsn-ns">Namespace</label>
        <QsSuggest id="qsn-ns" v-model="form.namespace" :options="namespaces" placeholder="none" describedby="qsn-ns-note" />
        <span id="qsn-ns-note" class="qsn-note">{{ nsNote }}</span>
      </div>
      <div class="qsn-field">
        <label class="qsn-label" for="qsn-task">Task</label>
        <QsSuggest id="qsn-task" v-model="form.task" :options="tasks" placeholder="none" describedby="qsn-task-note" />
        <span id="qsn-task-note" class="qsn-note">{{ taskNote }}</span>
      </div>

      <label class="qsn-field">
        <span class="qsn-label">Lease time</span>
        <span class="qn-field" style="margin-top: 6px">
          <input v-model="form.leaseTime" class="qn-num" type="text" inputmode="numeric" autocomplete="off" :placeholder="String(OPTION_DEFAULTS.leaseTime)" aria-describedby="qsn-lease-note" />
          <span>s</span>
        </span>
        <span id="qsn-lease-note" class="qsn-note" :class="{ red: d.errors.leaseTime }">{{ d.errors.leaseTime || 'How long a worker holds a message before another worker gets it.' }}</span>
      </label>
      <label class="qsn-field">
        <span class="qsn-label">Retries</span>
        <span class="qn-field" style="margin-top: 6px">
          <input v-model="form.retryLimit" class="qn-num" type="text" inputmode="numeric" autocomplete="off" :placeholder="String(OPTION_DEFAULTS.retryLimit)" aria-describedby="qsn-retries-note" />
        </span>
        <span id="qsn-retries-note" class="qsn-note" :class="{ red: d.errors.retryLimit }">{{ d.errors.retryLimit || 'Failed tries before a message goes to dead letters.' }}</span>
      </label>
    </div>

    <button type="button" class="qn-btn3 qsn-more" :aria-expanded="more" @click="more = !more">
      {{ more ? 'Fewer options' : `More options (${MORE.length})` }}<span v-if="!more && extraTags.length" class="qn-t3">, {{ extraTags.length }} changed</span>
    </button>
    <div v-if="more" class="qsn-opts">
      <div v-for="o in MORE" :key="o.key" class="qsn-opt">
        <span class="qsn-opt-name">
          <span :id="`qsn-o-${o.key}`">{{ o.label }}</span><span class="qn-mono qn-t3 qsn-key">{{ o.key }}</span>
          <span class="qn-t3 qsn-help">{{ short(o.help) }}</span>
        </span>
        <span class="qsn-opt-input">
          <span v-if="o.type === 'bool'" class="qn-seg" role="group" :aria-labelledby="`qsn-o-${o.key}`">
            <button type="button" :aria-pressed="form[o.key] === true" @click="form[o.key] = true">On</button>
            <button type="button" :aria-pressed="form[o.key] === false" @click="form[o.key] = false">Off</button>
          </span>
          <span v-else class="qn-field" style="width: 128px"><input v-model="form[o.key]" :aria-labelledby="`qsn-o-${o.key}`" :inputmode="o.type === 'int' ? 'numeric' : 'text'" class="qn-num" type="text" autocomplete="off" :placeholder="String(o.default ?? '')" /></span>
          <span class="qn-t2 qsn-unit">{{ o.unit || '' }}</span>
        </span>
        <span v-if="d.errors[o.key]" class="red qsn-err">{{ d.errors[o.key] }}</span>
      </div>
    </div>

    <div v-if="d.exists" class="qsn-what">
      <p class="qsn-label">What changes</p>
      <p class="qn-t2" style="margin-top: 8px">Nothing while the name is taken: on a name that exists, this call would change that queue instead.</p>
    </div>
    <div v-else class="qsn-what">
      <p class="qsn-label">What changes</p>
      <div v-if="d.name && !d.nameError && !d.exists" class="qsn-tags">
        <span class="qn-mono" style="font-weight: 500; margin-right: 4px; overflow-wrap: anywhere">{{ d.name }}</span>
        <span class="qn-tag qn-num">lease {{ d.leaseTime }} s</span>
        <span v-if="retries" class="qn-tag qn-num">{{ retries }}</span>
        <span v-for="t in extraTags" :key="t" class="qn-tag qn-num">{{ t }}</span>
      </div>
      <div class="qsn-counts qn-num">
        <span>Queues {{ d.namespace ? 'in' : 'with no namespace' }} <span v-if="d.namespace" class="qn-mono">{{ d.namespace }}</span></span>
        <span class="qsn-r">{{ n(nsBefore) }} → {{ n(nsBefore + 1) }}</span>
        <span>Queues on <span class="qn-mono">{{ where || 'this cluster' }}</span></span>
        <span class="qsn-r">{{ n(total) }} → {{ n(total + 1) }}</span>
      </div>
      <p class="qn-t2" style="margin-top: 10px">The queue exists at once, with these settings and the broker's defaults for the rest. It has no partitions until the first push makes one.</p>
    </div>
    </div>
  </QcDialog>
</template>

<style scoped>
/* QcDialog's scrim does not scroll: on a short screen the form scrolls inside,
   and the call and the buttons under it stay in view. */
.qsn-scroll { max-height: calc(100vh - 360px); max-height: calc(100dvh - 360px); min-height: 200px; overflow-y: auto; padding: 2px; margin: -2px; }
.qsn { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px 16px; }
.qsn-wide { grid-column: 1 / -1; }
.qsn-field { display: block; min-width: 0; }
.qsn-label { font-size: 12px; color: var(--text3); }
.qsn-note { display: block; font-size: 12px; color: var(--text3); margin-top: 6px; }
.qsn-note.red { color: var(--red); }
.qsn-what { margin-top: 16px; padding-top: 14px; border-top: 1px solid var(--line2); }
.qsn-tags { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 8px; margin-top: 8px; }
.qsn-counts { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 0 12px; margin-top: 8px; font-size: 12.5px; }
.qsn-counts > span { padding: 6px 0; border-bottom: 1px solid var(--line2); }
.qsn-r { text-align: right; }
.qsn-more { margin-top: 14px; padding: 0; }
.qsn-opts { margin-top: 6px; border-top: 1px solid var(--line2); }
.qsn-opt { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center; gap: 4px 18px; padding: 10px 0; border-bottom: 1px solid var(--line2); }
.qsn-opt-name { min-width: 0; }
.qsn-key { font-size: 11.5px; margin-left: 8px; }
.qsn-help { display: block; font-size: 12px; margin-top: 2px; }
.qsn-opt-input { display: inline-flex; align-items: center; gap: 8px; }
.qsn-unit { width: 22px; }
.qsn-err { grid-column: 1 / -1; font-size: 12px; }
:global(.qn.tone .qsn-opts), :global(.qn.tone .qsn-opt) { border-color: var(--bg3); }
:global(.qn.tone .qsn-what) { border-top-color: var(--bg3); }
@media (max-width: 640px) { .qsn { grid-template-columns: minmax(0, 1fr); } }
</style>
