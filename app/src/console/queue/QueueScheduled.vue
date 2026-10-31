<script setup>
// This queue's timers: when each fires, into which partition. A timer fires as
// a real message; cancelling one shows the call first.
import { computed, ref, shallowRef, watch } from 'vue'
import { timers as timersApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { formatDeliverIn, describeCancel } from '@/composables/useTimers'
import { useIdentity } from '@/stores/identity'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { fmt } from '../useQueuePage'

const props = defineProps({
  queue: { type: String, required: true },
  where: { type: String, default: '' },
})
const emit = defineEmits(['count'])
const { can } = useIdentity()

const data = shallowRef(null)
const error = ref(null)
async function load() {
  try {
    const r = await timersApi.list(props.queue, { limit: 50 }, { quiet: true })
    data.value = r.data
    error.value = null
    emit('count', (r.data?.rows || []).length + (r.data?.truncated ? '+' : ''))
  } catch (e) { error.value = describeApiError(e) }
}
useAutoRefresh(load)
load()
watch(() => props.queue, load)

const rows = computed(() => (data.value?.rows || []).map((t) => ({
  key: t.timerKey,
  partition: t.partition || '–',
  at: t.deliverAt ? new Date(t.deliverAt).toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }) : '–',
  rel: t.deliverAt ? formatDeliverIn(t.deliverAt) : '',
  tries: t.attempts ?? 0,
  claimed: !!t.claimed,
  error: t.lastError || '',
})))

const cancelling = ref(null)
const busy = ref(false)
const enc = encodeURIComponent
async function cancel() {
  const key = cancelling.value
  busy.value = true
  try {
    const r = await timersApi.cancel(props.queue, key)
    const d = describeCancel(r.data, { queue: props.queue, timerKey: key })
    toast(d.sentence || 'Cancelled', `The broker answered ${r.status || 200}.`, d.tone === 'ok' ? '' : 'red')
    cancelling.value = null
    load()
  } catch (e) {
    toast('Not cancelled', describeApiError(e), 'red')
  } finally { busy.value = false }
}
</script>

<template>
  <div class="qs">
    <p v-if="error" class="qn-t2" style="padding: 16px 0">The broker did not answer: {{ error }}</p>
    <p v-else-if="data && !rows.length" style="font-size: 14.5px; padding-top: 8px">No timers on <span class="qn-mono">{{ queue }}</span>.</p>
    <div v-else-if="rows.length" class="qs-table" role="table" aria-label="Timers" style="--cols: minmax(0, 1.5fr) minmax(0, 1fr) minmax(0, 1.1fr) 60px 110px">
      <div class="qn-tr head" role="row">
        <span role="columnheader">timerKey</span><span role="columnheader">Partition</span><span role="columnheader">Fires</span>
        <span role="columnheader" class="r">Tries</span><span role="columnheader"><span class="qn-vh">Actions</span></span>
      </div>
      <div v-for="t in rows" :key="t.key" class="qn-tr" role="row">
        <span role="cell" class="qn-mono" :title="t.key">{{ t.key }}</span>
        <span role="cell" class="qn-mono qn-t2" :title="t.partition">{{ t.partition }}</span>
        <span role="cell" class="qn-num">{{ t.at }} <span class="qn-t3">{{ t.rel }}</span></span>
        <span role="cell" class="r qn-num" :class="t.error ? 'red' : ''" :title="t.error">{{ t.tries }}</span>
        <span role="cell" style="text-align: right">
          <span v-if="t.claimed" class="qn-t3">firing</span>
          <button v-else-if="can('produce')" type="button" class="qn-btn3" @click="cancelling = t.key">Cancel…</button>
        </span>
      </div>
    </div>
    <p class="qn-read" style="margin-top: 12px">Read from <span class="qn-mono">GET /api/v1/timers/{{ queue }}</span>, the next 50. A timer fires as a real message into its partition.</p>

    <QcDialog
      :open="!!cancelling"
      :title="`Cancel the timer ${cancelling || ''}?`"
      :where="where"
      :call="cancelling ? `DELETE /api/v1/timers/${enc(queue)}/${enc(cancelling)}` : ''"
      confirm-label="Cancel the timer"
      cancel-label="Keep it"
      danger
      :busy="busy"
      @confirm="cancel"
      @cancel="cancelling = null"
    >
      <p class="qn-t2">It will not fire. A timer already firing cannot be cancelled.</p>
    </QcDialog>
  </div>
</template>

<style scoped>
.qs { margin-top: 14px; }
.qs-table { overflow-x: auto; }
</style>
