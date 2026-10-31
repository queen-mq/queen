<script setup>
// What an operator can do to the cell today, behind a dialog that shows what
// changes, where, and the call before it runs: promote this cluster when it is
// a standby. Restarting a node and moving the leadership have no broker route,
// so they are not offered.
import { computed, ref } from 'vue'
import client from '@/api/client'
import { describeApiError } from '@/api/errors'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { segments } from './clusterViews'

const props = defineProps({
  link: { type: Object, required: true },         // clusterViews linkModel
  where: { type: String, default: '' },           // the cluster's name
})
const emit = defineEmits(['changed'])

const busy = ref(false)

// ---- promote: this cluster stops replaying its source and takes writes ----
const promoting = ref(false)
async function promote() {
  busy.value = true
  try {
    const r = await client.post('/api/v1/system/link/promote', null, { quiet: true })
    promoting.value = false
    toast(`${props.where} takes writes now`, `The broker answered ${r.status || 200}: this cluster is ${r.data?.role || 'promoted'}.`)
    emit('changed')
  } catch (e) {
    toast('Not promoted', describeApiError(e), 'red')
  } finally { busy.value = false }
}

</script>

<template>
  <section class="co" aria-labelledby="co-title">
    <h2 id="co-title" class="co-title">Operations</h2>
    <div class="co-rows">
      <div class="co-row">
        <span class="co-k">Standby</span>
        <span class="co-v" :class="link.tone"><span v-if="link.tone" class="qn-mark" :class="link.tone" aria-hidden="true" /><span><span v-for="(x, i) in segments(link.text)" :key="i" :class="x.mono ? 'qn-mono' : ''">{{ x.t }}</span></span></span>
        <button v-if="link.canPromote" type="button" class="qn-btn2 danger" @click="promoting = true">Promote…</button>
      </div>
    </div>
    <p class="qn-read" style="margin-top: 12px">Read from <span class="qn-mono">GET /api/v1/system/link</span>.</p>

    <QcDialog :open="promoting" :title="`Promote ${where}?`" :where="where" call="POST /api/v1/system/link/promote" :typed="where" confirm-label="Promote" danger :busy="busy" @confirm="promote" @cancel="promoting = false">
      <div class="co-chg">
        <div><span class="co-chg-k qn-mono">{{ where }}</span><span class="co-was">standby</span><span class="qn-t3" aria-hidden="true">→</span><span style="font-weight: 500">takes writes</span></div>
      </div>
      <p style="margin-top: 14px"><span class="qn-mono">{{ where }}</span> stops replaying <span class="qn-mono">{{ link.source }}</span> and takes writes, through any of its nodes. Point the apps at it first.</p>
      <p class="qn-t2" style="margin-top: 6px">Nothing tells <span class="qn-mono">{{ link.source }}</span>. If it still takes writes, two clusters serve and neither sees what the other writes: stop it first.</p>
    </QcDialog>

  </section>
</template>

<style scoped>
.co-title { font-size: 15px; font-weight: 500; }
.co-rows { margin-top: 12px; border-top: 1px solid var(--line); }
.co-row { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 16px; min-height: 48px; padding: 9px 0; border-bottom: 1px solid var(--line2); }
.co-k { flex: 0 0 110px; color: var(--text3); }
.co-v { flex: 1 1 260px; min-width: 0; display: inline-flex; align-items: baseline; gap: 8px; }
.co-v .qn-mark { align-self: center; }
.co-chg { border: 1px solid var(--line); border-radius: var(--r-card); overflow: hidden; }
.co-chg > div { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 12px; padding: 9px 14px; font-size: 12.5px; }
.co-chg > div + div { border-top: 1px solid var(--line2); }
.co-chg-k { flex: 1 1 110px; color: var(--text2); }
.co-was { color: var(--text3); text-decoration: line-through; }
:global(.qn.tone .co-chg) { border-color: transparent; background: var(--bg2); }
@media (max-width: 640px) { .co-k { flex-basis: 100%; } }
</style>
