<script setup>
import QcJson from '../QcJson.vue'
// The keys of one namespace, 50 at a time in name order, with a prefix filter
// (KV has no queries). A click opens the key beside the list: its value, its
// facts, and Edit and Delete, each shown first in a QcDialog with its call.
import { computed, nextTick, onBeforeUnmount, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { describeVerdict } from '@/composables/useGatedVerdict'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'
import { nowMs, clock } from '../useWindow'
import StatePut from './StatePut.vue'
import { refusal } from '../useStatePage'
import {
  PAGE, bytes, fmt, ago, inOrAgo, keyRows, expiresText, flagLine, editPlan, deletePlan, writeAnswer, lifetimeOf, span,
} from './stateViews'

const props = defineProps({
  s: { type: Object, required: true },
  where: { type: String, default: '' },
})
const {
  ns, listed, nsData, kvVerdict, keysPage, keysError, keysLoading, prefix, pageNo, nextPage, firstPage,
  canWrite, putKey, deleteKey, afterKvWrite,
} = props.s
const router = useRouter()
const now = nowMs

// ---- the prefix: applied 300 ms after the typing stops (each one is a read), Enter at once ----
const draft = ref(prefix.value)
let timer = null
watch(draft, (v) => {
  clearTimeout(timer)
  if (v === prefix.value) return
  timer = setTimeout(() => { prefix.value = v }, 300)
})
watch(prefix, (v) => { if (v !== draft.value) draft.value = v })
watch(ns, () => { clearTimeout(timer); picked.value = null })
function applyNow() { clearTimeout(timer); prefix.value = draft.value }
onBeforeUnmount(() => clearTimeout(timer))

// ---- the page ----
const rows = computed(() => keyRows(keysPage.value))
const hasExpiry = computed(() => rows.value.some((r) => r.expiresMs != null || r.expired))
const flag = computed(() => flagLine(rows.value))
const nsCount = computed(() => (nsData.value || []).find((o) => o.namespace === ns.value)?.keys ?? null)
// Where the page is in the walk. Name order is said once, by the line under the
// filter; the namespace's count is said once, in the list on the left.
const rangeText = computed(() => {
  const n = rows.value.length
  if (!n) return ''
  const p = keysPage.value
  const short = p?.truncated && n < PAGE ? ' This page stopped at 4 MB of values.' : ''
  if (pageNo.value === 0 && !p?.truncated) {
    return prefix.value ? `${fmt(n)} ${n === 1 ? 'key starts' : 'keys start'} with “${prefix.value}”.` : ''
  }
  const a = pageNo.value * PAGE + 1
  return `Keys ${fmt(a)}–${fmt(a + n - 1)}${prefix.value ? ` starting with “${prefix.value}”` : ''}.${short}`
})
const quiet = computed(() => (kvVerdict.value && kvVerdict.value !== 'paused' ? describeVerdict(kvVerdict.value, 'kv') : null))

function at(ms) {
  if (ms == null) return '–'
  const d = new Date(ms)
  return d.toDateString() === new Date(now.value).toDateString()
    ? clock(ms, true)
    : d.toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
}

// ---- one key, beside the list ----
const picked = ref(null)
const last = ref(null) // the row as last seen, for a key that leaves the page
const drawer = ref(null)
const showRaw = ref(false)
const sel = computed(() => (picked.value ? rows.value.find((r) => r.key === picked.value) || null : null))
watch(sel, (r) => { if (r) last.value = r })
const shown = computed(() => sel.value || (picked.value && last.value?.key === picked.value ? last.value : null))
const gone = computed(() => Boolean(picked.value && !sel.value && keysPage.value))
function toggle(key) {
  picked.value = picked.value === key ? null : key
  showRaw.value = false
  if (picked.value) nextTick(() => drawer.value?.scrollIntoView?.({ block: 'nearest' }))
}
const cols = computed(() => {
  const base = shown.value ? 'minmax(0, 1fr) 76px 96px' : 'minmax(0, 1.25fr) minmax(0, 1fr) 76px 96px'
  return hasExpiry.value ? `${base} 104px` : base
})
const valueLabel = computed(() => {
  const k = shown.value?.kind
  return k === 'text' ? 'Value, text' : k === 'binary' ? 'Value' : 'Value, JSON'
})
const expiresWords = computed(() => {
  const r = shown.value
  if (!r) return ''
  if (r.expired) return 'expired, waiting for the sweeper'
  if (r.expiresMs == null) return 'never'
  const life = lifetimeOf(r.raw)
  return `${at(r.expiresMs)}, ${inOrAgo(r.expiresMs, now.value)}${life ? ` (a ${span(life)} lifetime)` : ''}`
})
async function copyValue() {
  const r = shown.value
  if (!r) return
  try {
    await navigator.clipboard.writeText(r.kind === 'json' ? r.text : String(r.raw.value))
    toast('Copied the value', `${r.key}, ${bytes(r.size)}.`)
  } catch { toast('Could not copy', 'The browser refused the clipboard.', 'red') }
}

// ---- edit ----
const editing = ref(false)
const editText = ref('')
const ifUnchanged = ref(true)
const editBox = ref(null)
const busy = ref(false)
const asText = computed(() => shown.value?.kind === 'text')
const edit = computed(() => (shown.value
  ? editPlan({ ns: ns.value, row: shown.value.raw, text: editText.value, asText: asText.value, ifUnchanged: ifUnchanged.value })
  : { ok: false, call: '', changes: [], effect: '' }))
function openEdit() {
  const r = shown.value
  if (!r) return
  editText.value = r.text
  ifUnchanged.value = true
  editing.value = true
  // After the dialog has focused its first input (the checkbox), the value.
  setTimeout(() => editBox.value?.focus?.(), 40)
}
async function saveEdit() {
  const r = shown.value
  const plan = edit.value
  if (!r || !plan.ok) return
  busy.value = true
  try {
    const res = await putKey(ns.value, r.key, plan.body)
    const a = writeAnswer('put', res.data, { key: r.key, ns: ns.value, status: res.status || 200 })
    toast(a.text, a.sub, a.tone)
    editing.value = false
    await afterKvWrite()
  } catch (e) {
    toast('Not saved', refusal(e), 'red')
  } finally { busy.value = false }
}

// ---- delete ----
const deleting = ref(false)
const del = computed(() => (shown.value ? deletePlan({ ns: ns.value, row: shown.value.raw }) : { call: '', body: null }))
async function doDelete() {
  const r = shown.value
  if (!r) return
  busy.value = true
  try {
    const res = await deleteKey(ns.value, r.key, del.value.body)
    const a = writeAnswer('delete', res.data, { key: r.key, ns: ns.value, status: res.status || 200 })
    toast(a.text, a.sub, a.tone)
    deleting.value = false
    if (res.data?.applied === true) picked.value = null
    await afterKvWrite()
  } catch (e) {
    toast('Not deleted', refusal(e), 'red')
  } finally { busy.value = false }
}

// ---- put a key ----
const putting = ref(false)
async function onPut(key, putNs) {
  // Show the key just written: its namespace, filtered to its name, open.
  putting.value = false
  if (putNs !== ns.value) await router.replace({ query: { ns: putNs } })
  await nextTick()
  draft.value = key
  prefix.value = key
  picked.value = key
}
</script>

<template>
  <div class="stk">
    <div class="stk-list">
      <div class="stk-head">
        <h2 class="qn-mono stk-ns">{{ ns || 'KV' }}</h2>
        <span v-if="ns && !listed" class="qn-tag">not in the namespace list</span>
        <div style="flex: 1 1 0" />
        <button v-if="canWrite && !quiet" type="button" class="qn-btn2" @click="putting = true">Put a key</button>
      </div>

      <template v-if="quiet">
        <p class="stk-lead">{{ quiet.title }}.</p>
        <p class="qn-t2" style="margin-top: 6px; max-width: 640px">{{ quiet.detail }}</p>
      </template>

      <template v-else-if="nsData && !nsData.length && !ns">
        <p class="stk-lead">Nothing is kept in KV yet.</p>
        <p class="qn-t2" style="margin-top: 6px; max-width: 640px">A namespace appears with its first key. Steps write keys in their transactions; you can also put one here.</p>
      </template>

      <template v-else-if="ns">
        <div class="stk-tools">
          <label class="qn-field stk-prefix">
            <span class="qn-t3" style="font-size: 12px; white-space: nowrap">Keys starting with</span>
            <input v-model="draft" class="qn-mono" type="text" spellcheck="false" autocomplete="off" @keydown.enter="applyNow" />
            <button v-if="draft" type="button" class="qn-btn3" aria-label="Clear the filter" style="height: 24px; padding: 0 4px" @click="draft = ''; applyNow()">✕</button>
          </label>
          <span class="qn-t3">KV has no queries: keys come in name order, and you find them by how they start.</span>
        </div>

        <p v-if="flag" class="stk-flag">
          <span class="stk-badge" :class="flag.tone"><span v-if="flag.tone" class="qn-mark amber" aria-hidden="true" />{{ flag.badge }}</span>
          <span class="qn-t2">{{ flag.text }}</span>
        </p>

        <p v-if="keysError && !rows.length" class="stk-state"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ keysError }}</p>
        <p v-else-if="!keysPage" class="stk-state qn-t2">Reading the keys of <span class="qn-mono">{{ ns }}</span>…</p>
        <p v-else-if="!rows.length && prefix" class="stk-state qn-t2">No key in <span class="qn-mono">{{ ns }}</span> starts with “<span class="qn-mono">{{ prefix }}</span>”. <button type="button" class="qn-link" @click="draft = ''; applyNow()">Clear the filter</button></p>
        <p v-else-if="!rows.length" class="stk-state qn-t2">No keys in <span class="qn-mono">{{ ns }}</span><template v-if="pageNo"> after the last page</template>.</p>

        <template v-else>
          <div class="stk-scroll">
            <div class="stk-table" :class="{ narrow: shown }" role="table" :aria-label="`Keys of ${ns}`" :style="{ '--cols': cols }">
              <div class="qn-tr head" role="row">
                <span role="columnheader">Key</span>
                <span v-if="!shown" role="columnheader">Value</span>
                <span role="columnheader" class="r">Size</span>
                <span role="columnheader" class="r">Updated</span>
                <span v-if="hasExpiry" role="columnheader" class="r">Expires</span>
              </div>
              <div
                v-for="r in rows" :key="r.key"
                class="qn-tr click" :class="{ on: picked === r.key, 'stk-old': r.expired }" role="row"
                tabindex="0" @click="toggle(r.key)" @keydown.enter="toggle(r.key)"
              >
                <span role="cell" class="qn-mono" :title="r.key">{{ r.key }}</span>
                <span v-if="!shown" role="cell" :class="r.kind === 'binary' ? 'qn-t3' : 'qn-mono qn-t2 stk-preview'">{{ r.kind === 'binary' ? 'binary or encrypted' : r.preview }}</span>
                <span role="cell" class="r qn-num" :class="r.flag ? (r.flag.tone || '') : 'qn-t2'" :title="r.flag ? r.flag.long : ''">
                  <span v-if="r.flag && r.flag.tone" class="qn-mark amber stk-cellmark" aria-hidden="true" />{{ bytes(r.size) }}
                </span>
                <span role="cell" class="r qn-num qn-t2" :title="at(r.updatedMs)">{{ ago(r.updatedMs, now) }}</span>
                <span v-if="hasExpiry" role="cell" class="r qn-num qn-t2" :title="r.expiresMs ? at(r.expiresMs) : 'never expires'">{{ expiresText(r, now) || '–' }}</span>
              </div>
            </div>
          </div>
          <p v-if="keysError" class="qn-t2" style="margin-top: 10px">
            <span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The last read failed: {{ keysError }}. These rows are from {{ clock(keysPage.at, true) }}.
          </p>
          <div v-if="rangeText || keysPage?.truncated || pageNo > 0" class="stk-pager">
            <span class="qn-num qn-t3">{{ rangeText }}</span>
            <div style="flex: 1 1 0" />
            <button v-if="pageNo > 0" type="button" class="qn-btn3" @click="firstPage">From the start</button>
            <button v-if="keysPage?.truncated || pageNo > 0" type="button" class="qn-btn2" :disabled="!keysPage?.truncated || keysLoading" @click="nextPage">Next {{ PAGE }}</button>
          </div>
        </template>
        <p class="qn-read" style="margin-top: 12px">Read from <span class="qn-mono">POST /api/v1/resources/kv/list</span>, {{ PAGE }} keys at a time with their values. An expired key stays listed until the sweeper takes it.</p>
      </template>
    </div>

    <aside v-if="shown" ref="drawer" class="stk-drawer qn-panel" :aria-label="`The key ${shown.key}`">
      <div style="display: flex; align-items: flex-start; gap: 8px">
        <div style="flex: 1 1 0; min-width: 0">
          <p class="qn-t3" style="font-size: 12px">Key in <span class="qn-mono">{{ ns }}</span></p>
          <h3 class="qn-mono stk-key">{{ shown.key }}</h3>
        </div>
        <button type="button" class="qn-btn3" aria-label="Close the key" style="height: 28px; flex: 0 0 auto" @click="picked = null">✕</button>
      </div>
      <p v-if="gone" class="qn-t2" style="margin-top: 10px">It is no longer on this page: someone deleted it or changed the page, or the sweeper took it. This is how it was.</p>
      <p v-if="shown.flag" class="stk-flag" style="margin-top: 12px">
        <span class="stk-badge" :class="shown.flag.tone"><span v-if="shown.flag.tone" class="qn-mark amber" aria-hidden="true" />{{ shown.flag.badge }}</span>
        <span class="qn-t2">{{ shown.flag.long }}</span>
      </p>
      <p class="qn-t3" style="font-size: 12px; margin-top: 14px">{{ valueLabel }}</p>
      <p v-if="shown.kind === 'binary' && !showRaw" class="stk-binary">
        Binary or encrypted: {{ bytes(shown.size) }} of base64 text, not shown.
        <button type="button" class="qn-link" @click="showRaw = true">Show the text</button>
      </p>
      <QcJson v-else :text="shown.text" style="margin-top: 6px" />
      <dl class="qn-dl" style="margin-top: 14px">
        <dt>Size</dt><dd class="qn-num" :title="`${fmt(shown.size)} bytes of JSON`">{{ bytes(shown.size) }}</dd>
        <dt>Version</dt><dd class="qn-num">{{ fmt(shown.version) }}</dd>
        <dt>Updated</dt><dd class="qn-num">{{ at(shown.updatedMs) }}, {{ ago(shown.updatedMs, now) }}</dd>
        <dt>Expires</dt><dd class="qn-num">{{ expiresWords }}</dd>
      </dl>
      <div v-if="canWrite" style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 16px">
        <button v-if="shown.kind !== 'binary'" type="button" class="qn-btn2" :disabled="gone" @click="openEdit">Edit…</button>
        <button type="button" class="qn-btn2 red" :disabled="gone" @click="deleting = true">Delete…</button>
        <button type="button" class="qn-btn3" @click="copyValue">Copy the value</button>
      </div>
      <template v-else>
        <div style="margin-top: 16px"><button type="button" class="qn-btn3" style="padding: 0" @click="copyValue">Copy the value</button></div>
        <p class="qn-t3" style="font-size: 12px; margin-top: 8px">Changing a key needs a role that may write; a viewer reads them.</p>
      </template>
    </aside>

    <QcDialog
      :open="editing && !!shown" :title="`Edit ${shown?.key || ''}?`" :where="where" :call="edit.call"
      confirm-label="Save" :busy="busy" :blocked="!edit.ok" @confirm="saveEdit" @cancel="editing = false"
    >
      <p class="qn-t3" style="font-size: 12px">In <span class="qn-mono">{{ ns }}</span>, read at version <span class="qn-num">{{ fmt(shown?.version) }}</span>.</p>
      <label style="display: block; margin-top: 10px">
        <span class="qn-t3" style="font-size: 12px">{{ asText ? 'Value, as text' : 'Value, as JSON' }}</span>
        <textarea ref="editBox" v-model="editText" class="qn-textarea" style="margin-top: 4px" spellcheck="false" />
      </label>
      <p v-if="edit.error" class="red" style="font-size: 12px; margin-top: 6px">{{ edit.error }}</p>
      <p v-else-if="edit.same" class="qn-t3" style="font-size: 12px; margin-top: 6px">Change the value to save it.</p>
      <label class="stk-check"><input v-model="ifUnchanged" type="checkbox" /><span>Only if it has not changed since you opened it</span></label>
      <div v-if="edit.changes.length && !edit.same && !edit.error" class="stk-diff">
        <p v-for="c in edit.changes" :key="c.k" class="stk-diffrow">
          <span :class="c.field ? 'qn-mono qn-t2' : 'qn-t2'" style="flex: 1 1 90px">{{ c.k }}</span>
          <template v-if="!c.note">
            <span class="qn-t3" :class="c.size ? 'qn-num' : 'qn-mono'" style="text-decoration: line-through; overflow-wrap: anywhere">{{ c.from }}</span>
            <span class="qn-t3" aria-hidden="true">→</span>
            <span :class="c.size ? 'qn-num' : 'qn-mono'" style="font-weight: 500; overflow-wrap: anywhere">{{ c.to }}</span>
          </template>
        </p>
      </div>
      <p v-if="edit.effect" class="qn-t2" style="margin-top: 12px">{{ edit.effect }}</p>
    </QcDialog>

    <QcDialog
      :open="deleting && !!shown" :title="`Delete ${shown?.key || ''}?`" :where="where" :call="del.call" :typed="shown?.key || ''"
      confirm-label="Delete" danger :busy="busy" @confirm="doDelete" @cancel="deleting = false"
    >
      <p class="qn-t2">This removes the key and its value from <span class="qn-mono">{{ ns }}</span>. Steps that read it find nothing, and it cannot be undone.</p>
      <div v-if="nsCount != null" class="stk-diff">
        <p class="stk-diffrow">
          <span class="qn-t2" style="flex: 1 1 90px">Keys in <span class="qn-mono">{{ ns }}</span></span>
          <span class="qn-num qn-t3" style="text-decoration: line-through">{{ fmt(nsCount) }}</span>
          <span class="qn-t3" aria-hidden="true">→</span>
          <span class="qn-num" style="font-weight: 500">{{ fmt(Math.max(0, nsCount - 1)) }}</span>
        </p>
      </div>
      <p class="qn-t3" style="font-size: 12px; margin-top: 10px">{{ shown?.expired ? 'It is expired: this removes the row now instead of at the next sweep.' : 'Only at the version you read: if anything wrote it since, nothing is deleted.' }}</p>
    </QcDialog>

    <StatePut :open="putting" :s="s" :where="where" @close="putting = false" @put="onPut" />
  </div>
</template>

<style scoped>
.stk { display: flex; flex-wrap: wrap; align-items: flex-start; gap: 20px 28px; }
.stk-list { flex: 999 1 520px; min-width: 0; }
.stk-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; min-height: 32px; }
.stk-ns { font-size: 18px; font-weight: 500; margin-right: 6px; overflow-wrap: anywhere; }
.stk-lead { font-size: 14.5px; margin-top: 14px; }
.stk-tools { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 16px; margin-top: 16px; }
.stk-prefix { width: 400px; max-width: 100%; }
.stk-flag { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 10px; margin-top: 14px; }
.stk-badge { display: inline-flex; align-items: center; gap: 6px; font-weight: 500; color: var(--text); }
.stk-badge.amber { color: var(--amber); }
.stk-state { padding: 18px 0 6px; }
.stk-scroll { margin-top: 12px; overflow-x: auto; }
.stk-table { min-width: 560px; }
.stk-table.narrow { min-width: 380px; }
.stk-preview { font-size: 12px; }
.stk-old > * { color: var(--text3) !important; }
.stk-cellmark { margin-right: 6px; vertical-align: 1px; }
.stk-pager { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; margin-top: 12px; min-height: 32px; }
.stk-drawer { flex: 1 1 360px; min-width: 0; max-width: 460px; padding: 16px 18px; position: sticky; top: 64px; max-height: calc(100vh - 80px); overflow-y: auto; }
.stk-key { font-size: 13.5px; font-weight: 500; margin-top: 2px; overflow-wrap: anywhere; }
.stk-binary { margin-top: 6px; padding: 12px 14px; border-radius: var(--r-ctl); background: var(--bg2); color: var(--text2); }
.stk-check { display: flex; align-items: center; gap: 8px; margin-top: 12px; }
.stk-check input { accent-color: var(--text2); }
.stk-diff { margin-top: 14px; border: 1px solid var(--line); border-radius: var(--r-card); overflow: hidden; }
.stk-diffrow { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 12px; padding: 9px 14px; font-size: 12.5px; }
.stk-diffrow + .stk-diffrow { border-top: 1px solid var(--line2); }
:global(.qn.tone .stk-diff) { border-color: transparent; background: var(--well); }
@media (max-width: 900px) { .stk-drawer { position: static; max-height: none; max-width: none; } }
</style>
